/**
 * plugin_orphan_sweep.ts — JA-41: OS-level sweep for orphaned hermes-channel
 * plugin processes, and for sessions stuck mid-spawn.
 *
 * Why this exists at all (the gap JA-25's reconcileOrphans cannot close).
 * ---------------------------------------------------------------------
 * reconcileOrphans() discovers candidates EXCLUSIVELY by enumerating
 * $HERMES_HOME/run/channels/*.sock — one socket file per session this pool
 * spawned. Two whole classes of orphan are invisible to that scan, not by a
 * scoping accident but by construction:
 *
 *   (a) The hermes-channel plugin binds a SINGLE, FIXED socket path
 *       ($HERMES_HOME/run/hermes-channel.sock) when no per-session
 *       HERMES_CHANNEL_SOCKET is injected — a different namespace entirely
 *       from the per-session channels dir. Worse, server.ts unlinks and
 *       rebinds that path unconditionally on every startup, so each new
 *       claude session silently steals the socket from whatever plugin
 *       process held it before. The dispossessed process never learns it was
 *       robbed: it keeps running, holding memory, answering nobody. Verified
 *       live on tanuki 2026-07-29 — every ppid=1 `bun server.ts` sampled held
 *       an fd on ~/.hermes/run/hermes-channel.sock whose file had since been
 *       re-created by a newer process, and had `->(none)` on all three stdio
 *       socketpairs (its parent was long gone).
 *
 *   (b) Sessions started by ORDINARY interactive `claude` usage — a human in
 *       a terminal loading the same plugin — were never pool-spawned, so no
 *       socket file in the channels dir ever existed for them, and no Map
 *       entry ever could. reconcileOrphans has nothing to find.
 *
 *   (c) A session stuck EARLY in spawn: spawn() only creates its socket after
 *       waitForSocket() resolves, so an `expect`/`claude` that died or hung
 *       before that point leaves zero sockets and zero children. Invisible to
 *       both the old and the new pool process after a supervisor replace.
 *
 * The only remaining handle on all three is the OS process table. Hence a
 * ps-based scan — deliberately kept as a SEPARATE module from session_pool.ts
 * so it can never alter the already-shipped socket-directory reconcile path.
 *
 * Sweep-only, on purpose (JA-41 phase 1).
 * ---------------------------------------
 * The socket-steal in server.ts is a real bug and fixing it at the source
 * would reduce the orphan RATE, not just clean up after it. That fix is
 * deliberately NOT done here: we sweep first and measure how much
 * accumulation is left, exactly as JA-25 did (sweep, don't fix, then decide).
 * A source fix aimed at the wrong one of the two known mechanisms (socket
 * steal vs. signal propagation through the two-hop `bun run` wrapper) would
 * under-deliver, and we have no evidence yet which dominates.
 *
 * Safety invariants — every one of these is a hard gate, not a heuristic:
 *
 *   1. SAME UID ONLY. Bad Janet runs the identical code under gduquesnay.
 *      His orphans are his instance's to sweep, never ours. Also excludes
 *      unrelated users running superficially similar bun projects (observed
 *      live: jasquier's `bun run --cwd .../claude-channel-slack start`).
 *   2. ppid === 1 ONLY, and re-checked LIVE by exact PID immediately before
 *      the kill — never trusted from the discovery snapshot. A process whose
 *      parent is init has positively been reparented by the kernel: nothing
 *      alive owns it. Anything with a live parent belongs to that parent.
 *   3. IDENTITY RE-CHECKED LIVE at kill time (same discipline as
 *      killOrphanAncestry): the candidate's own command line must still match
 *      the class signature that selected it. Guards the PID-reuse window
 *      between discovery and kill.
 *   4. KILL BY EXACT PID ONLY. Never `pkill -f`, never a name glob — a
 *      command-line match is machine-wide with no instance scoping, which is
 *      precisely how a janet-test sweep could reach into prod.
 *   5. GRACE WINDOW on process age. A just-started process has not had time
 *      to fork its children or be adopted properly; treating "0 children
 *      right now" as proof of stuckness would kill healthy sessions during
 *      boot.
 *
 * Deliberately NOT done: no ancestry walk. When an orphaned `bun run` wrapper
 * is swept, its own child (`bun server.ts`) is reparented to init by the
 * kernel and becomes a first-class candidate on the NEXT tick, on its own
 * merits, re-verified from scratch. Convergence over two ticks costs a minute
 * and buys a much smaller blast radius than killing processes we only reached
 * transitively.
 */

// ---------------------------------------------------------------------------
// Tunables
// ---------------------------------------------------------------------------

/**
 * Path fragment that identifies a directory as a hermes-channel plugin
 * checkout. Matches every deployed copy (source marketplace, the vendored
 * janet-test copy, each instance's own marketplace) and the `-spike` variant,
 * while excluding unrelated bun projects that also happen to run `server.ts`.
 */
export const PLUGIN_DIR_MARKER = "external_plugins/hermes-channel";

/**
 * Minimum process age before a candidate may be swept.
 *
 * Two distinct jobs: it stops a plugin child that was reparented seconds ago
 * (mid-handover, about to be re-adopted or about to exit on its own) from
 * being killed, and it stops a booting `expect`/`claude` from being read as
 * "stuck" merely because it has not forked its children yet. Boot-to-prompt
 * was measured at 1–45s depending on host load (see launcher.exp), so this
 * sits comfortably above the worst observed boot.
 */
export const ORPHAN_GRACE_MS = 120_000;

/** Grace after SIGTERM before checking liveness and escalating. Mirrors EVICT_SIGTERM_GRACE_MS. */
const SWEEP_SIGTERM_GRACE_MS = 500;

/** Grace after the SIGKILL escalation before the final liveness check. Mirrors EVICT_SIGKILL_GRACE_MS. */
const SWEEP_SIGKILL_GRACE_MS = 200;

// ---------------------------------------------------------------------------
// Process snapshot
// ---------------------------------------------------------------------------

export interface ProcessSnapshot {
  pid: number;
  ppid: number;
  uid: number;
  /** Process age in milliseconds, derived from ps `etime`. */
  ageMs: number;
  /** Full command line as reported by ps. */
  command: string;
}

/** Why a candidate was selected — carried through to the log line and the tests. */
export type OrphanClass =
  /** `bun server.ts` / `bun server-v3.ts` running out of a plugin directory. */
  | "plugin-leaf"
  /** `bun run --cwd <plugin dir> ... start` — the plugin's own two-hop wrapper. */
  | "plugin-wrapper"
  /** `expect -f <launcher.exp>` or a `claude_hermes_*` claude with no children. */
  | "stuck-spawn";

export interface OrphanCandidate {
  pid: number;
  klass: OrphanClass;
  /** The command line observed at discovery — re-checked live before the kill. */
  command: string;
  ageMs: number;
}

// ---------------------------------------------------------------------------
// Injectable dependencies — same style as session_pool.ts's PoolDeps
// ---------------------------------------------------------------------------

export interface SweepDeps {
  /** One `ps` pass over the whole process table. */
  listProcesses(): ProcessSnapshot[];
  /** Live parent PID by exact PID, or undefined if the process is gone. */
  getPpid(pid: number): number | undefined;
  /** Live command line by exact PID, or undefined if the process is gone. */
  getCommandLine(pid: number): string | undefined;
  /**
   * Working directory of a process, or undefined if unavailable.
   * The only way to tell a plugin `bun server.ts` (whose argv is the bare,
   * path-free `/opt/homebrew/bin/bun server.ts`) from any other bun project's
   * server.ts. Called ONLY for candidates that already passed the cheap
   * filters, so it never costs an lsof per process on the box.
   */
  getCwd(pid: number): string | undefined;
  /** Kill by exact PID. MUST NOT be implemented as a name-based kill. */
  killPid(pid: number, signal: "SIGTERM" | "SIGKILL"): void;
  /** Liveness probe (signal 0). */
  isPidAlive(pid: number): boolean;
  /** UID of the process running this sweep — the cross-account boundary. */
  ownUid(): number;
}

// ---------------------------------------------------------------------------
// Default (production) implementations
// ---------------------------------------------------------------------------

/**
 * Parse ps `etime` ([[dd-]hh:]mm:ss) into milliseconds.
 * Exported for the tests: the format is fiddly and silently mis-parsing it
 * would disable the grace window without failing anything visibly.
 */
export function parseEtimeMs(etime: string): number {
  const trimmed = etime.trim();
  const match = /^(?:(\d+)-)?(?:(\d+):)?(\d+):(\d+)$/.exec(trimmed);
  if (!match) return 0;
  const days = match[1] ? parseInt(match[1], 10) : 0;
  const hours = match[2] ? parseInt(match[2], 10) : 0;
  const minutes = parseInt(match[3]!, 10);
  const seconds = parseInt(match[4]!, 10);
  return ((days * 24 + hours) * 3600 + minutes * 60 + seconds) * 1000;
}

/** Parse one `ps -axo pid=,ppid=,uid=,etime=,command=` line. */
export function parsePsLine(line: string): ProcessSnapshot | undefined {
  const match = /^\s*(\d+)\s+(\d+)\s+(\d+)\s+(\S+)\s+(.*)$/.exec(line);
  if (!match) return undefined;
  return {
    pid: parseInt(match[1]!, 10),
    ppid: parseInt(match[2]!, 10),
    uid: parseInt(match[3]!, 10),
    ageMs: parseEtimeMs(match[4]!),
    command: match[5]!,
  };
}

function defaultListProcesses(): ProcessSnapshot[] {
  try {
    const result = Bun.spawnSync(
      ["ps", "-axo", "pid=,ppid=,uid=,etime=,command="],
      { stdout: "pipe", stderr: "pipe" }
    );
    return new TextDecoder()
      .decode(result.stdout)
      .split("\n")
      .map(parsePsLine)
      .filter((p): p is ProcessSnapshot => p !== undefined);
  } catch {
    return [];
  }
}

function defaultGetPpid(pid: number): number | undefined {
  try {
    const result = Bun.spawnSync(["ps", "-o", "ppid=", "-p", String(pid)], {
      stdout: "pipe",
      stderr: "pipe",
    });
    const n = parseInt(new TextDecoder().decode(result.stdout).trim(), 10);
    return isNaN(n) ? undefined : n;
  } catch {
    return undefined;
  }
}

function defaultGetCommandLine(pid: number): string | undefined {
  try {
    const result = Bun.spawnSync(["ps", "-o", "command=", "-p", String(pid)], {
      stdout: "pipe",
      stderr: "pipe",
    });
    const out = new TextDecoder().decode(result.stdout).trim();
    return out.length > 0 ? out : undefined;
  } catch {
    return undefined;
  }
}

function defaultGetCwd(pid: number): string | undefined {
  try {
    // -Fn keeps the output machine-parsable: "p<pid>\nfcwd\nn<path>".
    const result = Bun.spawnSync(
      ["lsof", "-a", "-p", String(pid), "-d", "cwd", "-Fn"],
      { stdout: "pipe", stderr: "pipe" }
    );
    const line = new TextDecoder()
      .decode(result.stdout)
      .split("\n")
      .find((l) => l.startsWith("n"));
    return line ? line.slice(1) : undefined;
  } catch {
    return undefined;
  }
}

function defaultKillPid(pid: number, signal: "SIGTERM" | "SIGKILL"): void {
  try {
    process.kill(pid, signal);
    process.stderr.write(`plugin-orphan-sweep: sent ${signal} to pid=${pid}\n`);
  } catch {
    // Already gone — the normal outcome for a process that exited between
    // the verify and the kill.
  }
}

function defaultIsPidAlive(pid: number): boolean {
  try {
    process.kill(pid, 0);
    return true;
  } catch {
    return false;
  }
}

export const PRODUCTION_SWEEP_DEPS: SweepDeps = {
  listProcesses: defaultListProcesses,
  getPpid: defaultGetPpid,
  getCommandLine: defaultGetCommandLine,
  getCwd: defaultGetCwd,
  killPid: defaultKillPid,
  isPidAlive: defaultIsPidAlive,
  ownUid: () => process.getuid?.() ?? -1,
};

// ---------------------------------------------------------------------------
// Command-line signatures
// ---------------------------------------------------------------------------

/**
 * The plugin's leaf process. Its argv is path-free (`/opt/homebrew/bin/bun
 * server.ts`), so this pattern alone proves nothing — it MUST be paired with
 * a cwd check. Kept anchored at the end so `bun server.ts --something` (a
 * shape we do not ship) does not match by accident.
 */
const LEAF_CMDLINE_RE = /(^|\/)bun\s+server(-v\d+)?\.ts$/;

/** The plugin's `bun run --cwd <dir> ... start` wrapper. The dir is right there in argv. */
const WRAPPER_CWD_RE = /(^|\/)bun\s+run\s+--cwd\s+(\S+)/;

/**
 * A pool-spawned claude session. `claude_hermes_` is the pool's own scoped
 * naming convention (claude_hermes_<hash8>) — never a user's interactive
 * session, which carries no --name at all.
 */
const CLAUDE_SESSION_RE = /--name\s+claude_hermes_\S+/;

// ---------------------------------------------------------------------------
// PluginOrphanSweeper
// ---------------------------------------------------------------------------

/**
 * The narrow surface the session pool depends on, so the pool's own tests can
 * inject a fake and never risk touching the real process table.
 */
export interface OrphanSweeperInterface {
  sweep(): Promise<OrphanCandidate[]>;
}

export class PluginOrphanSweeper implements OrphanSweeperInterface {
  private readonly deps: SweepDeps;

  /**
   * A sweep costs one SIGTERM grace period per candidate, so the first sweep
   * after a long accumulation runs for tens of seconds — longer than nothing,
   * but well within the eviction interval. This guard makes the arithmetic
   * irrelevant: a tick that lands while a sweep is still running is dropped
   * rather than allowed to re-discover and re-signal the same PIDs.
   */
  private sweeping = false;

  /**
   * @param launcherExpPath This instance's own launcher.exp. Used as the
   *        `expect` identity signature — it is the one string that is
   *        genuinely per-instance (each deployed ACP copy has its own path),
   *        so a janet-test sweep can never select prod's expect processes.
   * @param graceMs Minimum process age before a candidate is eligible.
   */
  constructor(
    private readonly launcherExpPath: string,
    deps: Partial<SweepDeps> = {},
    private readonly graceMs: number = ORPHAN_GRACE_MS
  ) {
    this.deps = { ...PRODUCTION_SWEEP_DEPS, ...deps };
  }

  /**
   * Discover orphan candidates. PURE DISCOVERY — reads only, kills nothing.
   * Exported separately from sweep() so a census can be taken (and asserted
   * in tests) without any process ever being signalled.
   */
  discover(): OrphanCandidate[] {
    const processes = this.deps.listProcesses();
    const uid = this.deps.ownUid();

    // Child counts for the stuck-mid-spawn test, computed from the same
    // single snapshot so the counts and the candidates cannot disagree.
    const childCount = new Map<number, number>();
    for (const p of processes) {
      childCount.set(p.ppid, (childCount.get(p.ppid) ?? 0) + 1);
    }

    const candidates: OrphanCandidate[] = [];

    for (const p of processes) {
      // Gate 1: never cross-account.
      if (p.uid !== uid) continue;
      // Gate 2: only processes the kernel has positively reparented to init.
      if (p.ppid !== 1) continue;
      // Gate 5: never touch anything younger than the grace window.
      if (p.ageMs < this.graceMs) continue;

      const klass = this.classify(p, childCount.get(p.pid) ?? 0);
      if (klass === undefined) continue;

      candidates.push({
        pid: p.pid,
        klass,
        command: p.command,
        ageMs: p.ageMs,
      });
    }

    return candidates;
  }

  /**
   * Which orphan class, if any, does this process belong to?
   * Returns undefined for everything else — the default is always "leave it
   * alone", including for processes we simply do not recognise.
   */
  private classify(
    p: ProcessSnapshot,
    children: number
  ): OrphanClass | undefined {
    const wrapper = WRAPPER_CWD_RE.exec(p.command);
    if (wrapper && wrapper[2]!.includes(PLUGIN_DIR_MARKER)) {
      return "plugin-wrapper";
    }

    if (LEAF_CMDLINE_RE.test(p.command)) {
      // The argv carries no path — the cwd is the only identity there is.
      const cwd = this.deps.getCwd(p.pid);
      if (cwd !== undefined && cwd.includes(PLUGIN_DIR_MARKER)) {
        return "plugin-leaf";
      }
      return undefined;
    }

    // Stuck mid-spawn: a launcher or a pool-named claude that never got as
    // far as forking anything. With children present it is a normal running
    // session whose supervisor merely died — that subtree is reconcileOrphans'
    // business (it has a socket), not ours.
    const isOurLauncher =
      p.command.includes("expect") && p.command.includes(this.launcherExpPath);
    const isPoolClaude = CLAUDE_SESSION_RE.test(p.command);
    if ((isOurLauncher || isPoolClaude) && children === 0) {
      return "stuck-spawn";
    }

    return undefined;
  }

  /**
   * Discover, verify live, then kill. Returns the PIDs actually signalled.
   *
   * Verification is redone from scratch per candidate immediately before its
   * kill — never carried over from discovery. Between the two, the process
   * may have exited and its PID been reused by something entirely unrelated;
   * ppid and command line are both re-read live to close that window.
   */
  async sweep(): Promise<OrphanCandidate[]> {
    if (this.sweeping) {
      process.stderr.write(
        "plugin-orphan-sweep: previous sweep still running — skipping this tick\n"
      );
      return [];
    }
    this.sweeping = true;
    try {
      return await this.sweepOnce();
    } finally {
      this.sweeping = false;
    }
  }

  private async sweepOnce(): Promise<OrphanCandidate[]> {
    const candidates = this.discover();
    if (candidates.length === 0) return [];

    process.stderr.write(
      `plugin-orphan-sweep: ${candidates.length} candidate(s) — ` +
        candidates.map((c) => `${c.pid}:${c.klass}`).join(",") +
        "\n"
    );

    const swept: OrphanCandidate[] = [];
    for (const candidate of candidates) {
      if (!this.verifyStillOrphan(candidate)) continue;
      await this.killWithGraceEscalation(candidate);
      swept.push(candidate);
    }
    return swept;
  }

  /**
   * Live re-check by exact PID: still reparented to init, and still the same
   * process we selected. Either check failing means "leave it alone" — under-
   * sweeping is always preferred to killing an unverified process.
   */
  private verifyStillOrphan(candidate: OrphanCandidate): boolean {
    const ppid = this.deps.getPpid(candidate.pid);
    if (ppid !== 1) {
      process.stderr.write(
        `plugin-orphan-sweep: skipping pid=${candidate.pid} — ppid is now ` +
          `${ppid ?? "gone"}, no longer a confirmed orphan\n`
      );
      return false;
    }

    const command = this.deps.getCommandLine(candidate.pid);
    if (command === undefined || !this.stillMatchesClass(command, candidate)) {
      process.stderr.write(
        `plugin-orphan-sweep: skipping pid=${candidate.pid} — command line no ` +
          `longer matches class=${candidate.klass} (PID reuse?)\n`
      );
      return false;
    }

    return true;
  }

  /** Does this live command line still match the class the candidate was selected as? */
  private stillMatchesClass(command: string, candidate: OrphanCandidate): boolean {
    switch (candidate.klass) {
      case "plugin-wrapper": {
        const wrapper = WRAPPER_CWD_RE.exec(command);
        return wrapper !== null && wrapper[2]!.includes(PLUGIN_DIR_MARKER);
      }
      case "plugin-leaf":
        // The cwd was already confirmed at discovery; re-confirming identity
        // here means re-confirming the argv shape AND that it is byte-for-byte
        // the same argv, which is what a PID-reuse guard actually needs.
        return LEAF_CMDLINE_RE.test(command) && command === candidate.command;
      case "stuck-spawn":
        return (
          (command.includes(this.launcherExpPath) ||
            CLAUDE_SESSION_RE.test(command)) &&
          command === candidate.command
        );
    }
  }

  /**
   * SIGTERM -> grace -> SIGKILL-if-survived -> grace -> report.
   * Same escalation shape as evict()/killPidWithGraceEscalation, deliberately
   * re-stated here rather than shared, so this new path can never change the
   * behaviour of the already-shipped eviction code. Never falls back to a
   * name-based kill: a candidate surviving SIGKILL is logged and left alone.
   */
  private async killWithGraceEscalation(candidate: OrphanCandidate): Promise<void> {
    const { pid, klass } = candidate;
    this.deps.killPid(pid, "SIGTERM");
    await new Promise<void>((r) => setTimeout(r, SWEEP_SIGTERM_GRACE_MS));
    if (!this.deps.isPidAlive(pid)) return;

    process.stderr.write(
      `plugin-orphan-sweep: pid=${pid} (${klass}) survived SIGTERM — escalating to SIGKILL\n`
    );
    this.deps.killPid(pid, "SIGKILL");
    await new Promise<void>((r) => setTimeout(r, SWEEP_SIGKILL_GRACE_MS));
    if (this.deps.isPidAlive(pid)) {
      process.stderr.write(
        `plugin-orphan-sweep: pid=${pid} (${klass}) survived SIGKILL — giving up on this PID\n`
      );
    }
  }
}

/**
 * Is the sweep enabled? On by default — an orphan sweep that has to be
 * switched on per instance would simply never be on where it matters. Set
 * HERMES_PLUGIN_ORPHAN_SWEEP=0 to disable it for one instance without a
 * redeploy.
 */
export function isSweepEnabled(
  env: Record<string, string | undefined> = process.env
): boolean {
  const raw = env["HERMES_PLUGIN_ORPHAN_SWEEP"];
  return raw !== "0" && raw !== "false";
}
