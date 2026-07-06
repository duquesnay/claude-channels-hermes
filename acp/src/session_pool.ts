/**
 * session_pool.ts — Multi-session pool of persistent claude --channels sessions.
 *
 * Each `session_key` (supplied by Janet via ACP _meta) maps to one long-lived
 * claude --channels process + hermes-channel plugin socket. The pool handles
 * lazy creation, idle eviction, capacity cap, and graceful drain.
 *
 * Design:
 *  - Socket path: $HERMES_HOME/run/channels/<sha256(key)[:16]>.sock
 *    deterministic, short enough for macOS UNIX_MAX_PATH (~104 chars).
 *  - Session name: claude_hermes_<hash8> — scoped kill target for eviction.
 *  - Isolation: HERMES_CHANNEL_SOCKET env injected into the expect launcher
 *    so the plugin bun child inherits it (voie a, proven by spike 2026-06-14).
 *    NO --strict-mcp-config (it suppresses plugins entirely).
 *  - Generation token: nonce regenerated on every (re)spawn. Returned in
 *    PromptResponse._meta.session_generation per turn. Hermes uses it for
 *    DELTA vs CATCH-UP decision.
 *
 * Environment:
 *   HERMES_CHANNELS_IDLE_TTL_MS          idle eviction TTL  (default: 30 min)
 *   HERMES_CHANNELS_MAX_SESSIONS         capacity cap        (default: 10)
 *   HERMES_CHANNELS_MIN_WARM             keep N most-recently-active sessions
 *                                        alive past idle TTL (default: 0)
 *   HERMES_HOME                          base dir            (default: ~/.hermes)
 *   HERMES_CHANNEL_SOCKET                overrides socket path (single-session compat)
 *   HERMES_CLAUDE_NO_ACCOUNT_CONNECTORS  when set (any value), injects
 *                                        ENABLE_CLAUDEAI_MCP_SERVERS=0 into
 *                                        the claude child so account-level
 *                                        claude.ai connectors (Gmail, Notes,
 *                                        Drive, Slack…) are not loaded.
 *                                        Default: unset (connectors load
 *                                        normally — prod ~/.hermes wants them).
 *                                        Verified 2026-06-15: triggers
 *                                        "[claudeai-mcp] Disabled via env var";
 *                                        hermes-channel plugin and subscription
 *                                        auth remain intact.
 */

import { createHash, randomUUID } from "node:crypto";
import { homedir } from "node:os";
import { join, dirname } from "node:path";
import { mkdir, unlink, readdir } from "node:fs/promises";
import { existsSync } from "node:fs";
import {
  isSocketLive,
  waitForSocket,
  sessionDecision,
} from "./supervisor.ts";
import {
  HermesChannelClient,
  type HermesChannelClientInterface,
} from "./hermes_channel_client.ts";

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/** Read TTL at call time so tests can override the env var before constructing the pool. */
function getIdleTtlMs(): number {
  return Number(process.env["HERMES_CHANNELS_IDLE_TTL_MS"] ?? 30 * 60 * 1000);
}

/** Read cap at call time so tests can override the env var before constructing the pool. */
function getMaxSessions(): number {
  return Number(process.env["HERMES_CHANNELS_MAX_SESSIONS"] ?? 10);
}

/**
 * Read MIN_WARM at call time so tests can override the env var.
 * MIN_WARM should be < MAX_SESSIONS. It protects only from idle eviction,
 * not from the hard capacity cap (evictLruIdle).
 */
function getMinWarm(): number {
  return Number(process.env["HERMES_CHANNELS_MIN_WARM"] ?? 0);
}

/** Wait budget for the expect launcher to bind the socket. */
const SOCKET_WAIT_MS = 75_000;

/** Graceful drain deadline before force-kill. */
const DRAIN_TIMEOUT_MS = 30_000;

/** Idle eviction scan interval — use .unref() so it doesn't block test exit. */
const EVICTION_INTERVAL_MS = 60_000;

/**
 * Review item 3 (empirically gauged, not guessed): grace period after
 * SIGTERM before evict() checks isPidAlive(expectPid) and possibly
 * escalates to SIGKILL. Measured against a real spawned session
 * (`scratch/ja25-cascade-probe.ts`): the `expect` process itself died at
 * +53ms after SIGTERM — comfortably inside this window — so escalation to
 * SIGKILL is the EXCEPTION (a genuinely stuck/unresponsive process), not
 * something that fires almost every time. Kept at 500ms rather than
 * shortened or lengthened based on that single measurement; revisit with
 * more data if escalation logs turn out to be frequent in practice.
 */
const EVICT_SIGTERM_GRACE_MS = 500;

/** Grace period after the SIGKILL escalation before the final isPidAlive check. */
const EVICT_SIGKILL_GRACE_MS = 200;

// ---------------------------------------------------------------------------
// Session state
// ---------------------------------------------------------------------------

export interface SessionState {
  socketPath: string;
  /** PID of the expect launcher process (undefined if we didn't spawn it). */
  expectPid: number | undefined;
  client: HermesChannelClientInterface;
  /** Nonce regenerated on every (re)spawn — the generation token. */
  generation: string;
  createdAt: number;
  lastActivityAt: number;
  /** Session name used for scoped kill (claude_hermes_<hash8>). */
  sessionName: string;
}

// ---------------------------------------------------------------------------
// Injectable dependencies — enables unit testing without spawning claude
// ---------------------------------------------------------------------------

export interface PoolDeps {
  /**
   * Spawn the expect launcher; return the PID of the expect process.
   * Receives the environment variables to inject into the subprocess.
   *
   * @param onExit  Gap (c): optional callback invoked with the child's exit
   *                code as soon as it dies, for ANY reason (crash, SIGKILL
   *                from outside, normal exit) — not just when the pool
   *                itself initiated the kill. Lets the pool learn about an
   *                unexpected death in real time instead of only
   *                discovering it reactively on the next getOrCreate() for
   *                the same key (or never, if no such call ever comes).
   */
  spawnLauncher(
    launcherExpPath: string,
    env: Record<string, string>,
    onExit?: (code: number | null) => void
  ): number;

  /**
   * Create a HermesChannelClient for a given socket path.
   * Injected so tests can supply mock clients.
   */
  createClient(socketPath: string): HermesChannelClientInterface;

  /**
   * Kill a process by exact PID with SIGTERM then SIGKILL.
   * MUST target PID, not name glob.
   */
  killPid(pid: number, signal: "SIGTERM" | "SIGKILL"): void;

  /**
   * Kill all processes holding a socket path (lsof -t <path>).
   * Injected for testability; falls back to a real lsof call in prod.
   */
  killSocketHolders(socketPath: string): void;

  /**
   * Current time in ms (injected for deterministic tests).
   */
  now(): number;

  /**
   * Check whether a process is alive (signal 0).
   * Injected so tests with synthetic PIDs can return true/false deterministically.
   */
  isPidAlive(pid: number): boolean;

  /**
   * Last-resort pkill by session name pattern (MUST be the scoped name,
   * e.g. "claude_hermes_<hash8>", never the bare "claude_hermes" generic).
   * Called only if the process is still detectable after killPid + killSocketHolders.
   * Injected so tests can assert the exact pattern and confirm it was called
   * only when necessary (or not called when earlier steps sufficed).
   *
   * ONLY used by evict() (a session this exact instance tracked and
   * spawned). Never used by reconcileOrphans() — see that method's
   * docstring for why (pkill -f matches by argv machine-wide, not scoped
   * to this instance).
   */
  killByName(sessionName: string): void;

  /**
   * List PIDs currently holding an open file descriptor on a socket path
   * (lsof -t <path>). Pure discovery — does NOT kill anything. Used by
   * reconcileOrphans()'s ancestry walk to find the leaf of a live orphaned
   * subtree from its own socket path (the one thing we know for certain
   * belongs to this exact session).
   */
  listSocketHolderPids(socketPath: string): number[];

  /**
   * Parent PID of a process, or undefined if the process no longer
   * exists. Used by the ancestry walk to climb from a discovered leaf PID
   * upward, one exact PID at a time.
   */
  getPpid(pid: number): number | undefined;

  /**
   * Full command line of a process, or undefined if it no longer exists.
   * Used to verify a candidate's identity BEFORE killing it during the
   * ancestry walk — never kill based on PID alone without confirming the
   * argv actually matches this session's own signature.
   */
  getCommandLine(pid: number): string | undefined;
}

// ---------------------------------------------------------------------------
// Default (production) implementations of deps
// ---------------------------------------------------------------------------

function defaultSpawnLauncher(
  launcherExpPath: string,
  env: Record<string, string>,
  onExit?: (code: number | null) => void
): number {
  const child = Bun.spawn(["expect", "-f", launcherExpPath], {
    stdout: "pipe",
    stderr: "pipe",
    stdin: "ignore",
    env: { ...process.env, ...env } as Record<string, string>,
  });

  // Drain output asynchronously to prevent pipe backpressure.
  void drainToStderr(child.stdout, "expect/claude stdout");
  void drainToStderr(child.stderr, "expect/claude stderr");

  // Gap (c): learn about the child's death in real time, whatever the
  // cause — Bun.spawn alone never notifies the pool of this.
  if (onExit) {
    void child.exited.then((code) => onExit(code));
  }

  return child.pid;
}

async function drainToStderr(
  stream: ReadableStream<Uint8Array> | null,
  label: string
): Promise<void> {
  if (!stream) return;
  const reader = stream.getReader();
  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      process.stderr.write(value);
    }
  } catch {
    process.stderr.write(`session-pool: ${label} stream ended\n`);
  }
}

function defaultKillPid(pid: number, signal: "SIGTERM" | "SIGKILL"): void {
  try {
    process.kill(pid, signal);
    process.stderr.write(
      `session-pool: sent ${signal} to pid=${pid}\n`
    );
  } catch {
    // Already gone — normal during eviction.
  }
}

function defaultKillSocketHolders(socketPath: string): void {
  if (!existsSync(socketPath)) return;
  try {
    const result = Bun.spawnSync(["lsof", "-t", socketPath], {
      stdout: "pipe",
      stderr: "pipe",
    });
    const pids = new TextDecoder()
      .decode(result.stdout)
      .trim()
      .split("\n")
      .filter(Boolean);
    for (const pid of pids) {
      const n = parseInt(pid, 10);
      if (!isNaN(n)) {
        defaultKillPid(n, "SIGTERM");
      }
    }
  } catch {
    // lsof unavailable or socket already gone.
  }
}

function defaultKillByName(sessionName: string): void {
  // Scoped last-resort: sessionName MUST be "claude_hermes_<hash8>".
  // Never called with the generic "claude_hermes" pattern.
  Bun.spawnSync(["pkill", "-f", sessionName], { stdout: "pipe", stderr: "pipe" });
  process.stderr.write(`session-pool: pkill -f ${sessionName} (last resort)
`);
}

function defaultIsPidAlive(pid: number): boolean {
  try {
    process.kill(pid, 0);
    return true;
  } catch {
    return false;
  }
}

function defaultListSocketHolderPids(socketPath: string): number[] {
  if (!existsSync(socketPath)) return [];
  try {
    const result = Bun.spawnSync(["lsof", "-t", socketPath], {
      stdout: "pipe",
      stderr: "pipe",
    });
    return new TextDecoder()
      .decode(result.stdout)
      .trim()
      .split("\n")
      .filter(Boolean)
      .map((s) => parseInt(s, 10))
      .filter((n) => !isNaN(n));
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

const PRODUCTION_DEPS: PoolDeps = {
  spawnLauncher: defaultSpawnLauncher,
  createClient: (socketPath) => new HermesChannelClient(socketPath),
  killPid: defaultKillPid,
  killSocketHolders: defaultKillSocketHolders,
  now: () => Date.now(),
  isPidAlive: defaultIsPidAlive,
  killByName: defaultKillByName,
  listSocketHolderPids: defaultListSocketHolderPids,
  getPpid: defaultGetPpid,
  getCommandLine: defaultGetCommandLine,
};

// ---------------------------------------------------------------------------
// Socket path derivation
// ---------------------------------------------------------------------------

/**
 * Base directory for per-session sockets.
 * Uses HERMES_HOME env (default ~/.hermes) so the path never exceeds
 * the macOS UNIX_MAX_PATH of ~104 bytes:
 *   ~/.hermes/run/channels/<16 hex chars>.sock  = ~50 chars (safe).
 */
function channelSocketsDir(): string {
  const hermesHome = process.env["HERMES_HOME"] ?? join(homedir(), ".hermes");
  return join(hermesHome, "run", "channels");
}

/**
 * Derive a deterministic, short socket path from a session key.
 * Uses the first 16 hex chars of sha256(key) — collision-safe enough
 * (2^64 space for a pool capped at 10 sessions).
 */
export function socketPathForKey(key: string): string {
  const hash = createHash("sha256").update(key).digest("hex").slice(0, 16);
  const path = join(channelSocketsDir(), `${hash}.sock`);
  // Defensive assertion — fail loudly rather than silently truncate.
  if (Buffer.byteLength(path, "utf8") >= 104) {
    throw new Error(
      `session-pool: socket path exceeds 104-byte macOS limit: ${path}`
    );
  }
  return path;
}

/**
 * Derive the scoped session name used for the claude --name flag.
 * Uses the first 8 hex chars of sha256(key): claude_hermes_<hash8>.
 * This is the ONLY pattern used for scoped kills — never a generic name.
 */
export function sessionNameForKey(key: string): string {
  const hash = createHash("sha256").update(key).digest("hex").slice(0, 8);
  return `claude_hermes_${hash}`;
}

// ---------------------------------------------------------------------------
// ChannelsSessionPool
// ---------------------------------------------------------------------------

export class ChannelsSessionPool {
  /** Active sessions keyed by session_key. */
  private readonly sessions = new Map<string, SessionState>();

  /**
   * Maps ACP session IDs → session_key.
   * Survives eviction so ACP prompt routing remains valid.
   */
  private readonly acpToKey = new Map<string, string>();

  /**
   * Review item 1 (race fix): socket paths currently being spawned, i.e.
   * reserved BEFORE spawnLauncher() is called and released in a `finally`
   * once the spawn either lands in `sessions` or fails. Closes the window
   * where the socket FILE already exists on disk (the launcher/plugin
   * created it) but `sessions` doesn't have the entry yet (spawn() only
   * calls `sessions.set()` after `waitForSocket()` resolves) — without
   * this, reconcileOrphans() ticking during that window would see a
   * "socket present, not in the Map" session and treat it exactly like a
   * real orphan, killing a session that is still legitimately being born.
   */
  private readonly pendingSpawns = new Set<string>();

  private readonly launcherExpPath: string;
  private readonly deps: PoolDeps;
  private evictionTimer: ReturnType<typeof setInterval> | undefined;

  constructor(launcherExpPath: string, deps: Partial<PoolDeps> = {}) {
    this.launcherExpPath = launcherExpPath;
    this.deps = { ...PRODUCTION_DEPS, ...deps };
  }

  // --------------------------------------------------------------------------
  // ACP session-id ↔ session_key registry
  // --------------------------------------------------------------------------

  /** Register an ACP session ID → session key mapping. */
  registerAcpSession(acpSessionId: string, sessionKey: string): void {
    this.acpToKey.set(acpSessionId, sessionKey);
  }

  /** Look up the session_key for an ACP session ID. */
  sessionKeyForAcp(acpSessionId: string): string | undefined {
    return this.acpToKey.get(acpSessionId);
  }

  /** Remove an ACP session mapping (call on closeSession). */
  unregisterAcpSession(acpSessionId: string): void {
    this.acpToKey.delete(acpSessionId);
  }

  // --------------------------------------------------------------------------
  // getOrCreate
  // --------------------------------------------------------------------------

  /**
   * Return the live SessionState for a key, spawning one if needed.
   *
   * If the session already exists and its expect process is alive (socket
   * present), return it as-is. If stale, clean up and respawn with a new
   * generation nonce. If the pool is full and all sessions are busy, reject
   * the caller immediately with a structured error.
   */
  async getOrCreate(sessionKey: string): Promise<SessionState> {
    const existing = this.sessions.get(sessionKey);
    if (existing) {
      // Gap (d): a socket-type FILE is not proof of a live LISTENER — a
      // process killed without graceful close() (SIGKILL, crash) leaves the
      // file behind. A real connect probe catches that; a plain stat does not.
      const socketPresent = await isSocketLive(existing.socketPath);
      const procPresent =
        existing.expectPid !== undefined
          ? this.isPidAlive(existing.expectPid)
          : false;
      const decision = sessionDecision(socketPresent, procPresent);

      if (decision === "reuse") {
        existing.lastActivityAt = this.deps.now();
        return existing;
      }

      // Stale (socket file but proc gone, or socket gone) — evict then respawn.
      process.stderr.write(
        `session-pool: stale session for key=${sessionKey} (decision=${decision}) — respawning\n`
      );
      await this.evict(sessionKey, false /* not graceful — no proc to drain */);
    }

    // Capacity check: if at MAX_SESSIONS, try to evict an idle session.
    if (this.sessions.size >= getMaxSessions()) {
      const evicted = await this.evictLruIdle();
      if (!evicted) {
        throw new PoolFullError(
          `session-pool: at capacity (${getMaxSessions()} sessions), all busy — refusal`
        );
      }
    }

    return this.spawn(sessionKey);
  }

  // --------------------------------------------------------------------------
  // spawn — internal, creates a new session state
  // --------------------------------------------------------------------------

  private async spawn(sessionKey: string): Promise<SessionState> {
    const socketPath = socketPathForKey(sessionKey);
    const sessionName = sessionNameForKey(sessionKey);
    const generation = randomUUID();

    await mkdir(dirname(socketPath), { recursive: true });

    // Remove stale socket file if present.
    if (existsSync(socketPath)) {
      try {
        await unlink(socketPath);
      } catch {
        // Already gone.
      }
    }

    // When HERMES_CLAUDE_NO_ACCOUNT_CONNECTORS is set, disable account-level
    // claude.ai connectors (Gmail, Notes, Drive, Slack…) in the child claude
    // session by injecting ENABLE_CLAUDEAI_MCP_SERVERS=0.  The supervisor's
    // own env is the authoritative source so janet_test and prod can diverge
    // without touching the launcher or credentials.  Default: unset (prod
    // ~/.hermes keeps connectors enabled).
    const noAccountConnectors =
      Boolean(process.env["HERMES_CLAUDE_NO_ACCOUNT_CONNECTORS"]) &&
      process.env["HERMES_CLAUDE_NO_ACCOUNT_CONNECTORS"] !== "0";

    // Review item 1: reserve the socket path BEFORE spawnLauncher() so it's
    // protected the instant it could possibly appear on disk — released in
    // `finally` however this turns out (success, waitForSocket timeout, or
    // any other throw), so a failed spawn doesn't leave a permanent
    // false-positive "pending" entry blocking reconciliation forever.
    this.pendingSpawns.add(socketPath);
    try {
      const expectPid = this.deps.spawnLauncher(
        this.launcherExpPath,
        {
          HERMES_CHANNEL_SOCKET: socketPath,
          HERMES_SESSION_NAME: sessionName,
          // Persona/sandbox cwd: explicit env wins so the launcher cd's to the
          // instance sandbox (e.g. ~/.janet-test/acp-sandbox) and the claude session
          // loads the right CLAUDE.md + .mcp.json. Falls back to the supervisor cwd.
          HERMES_SESSION_CWD: process.env.HERMES_SESSION_CWD ?? process.cwd(),
          ...(noAccountConnectors && { ENABLE_CLAUDEAI_MCP_SERVERS: "0" }),
        },
        // Gap (c): react the moment this exact child dies, whatever the
        // cause. expectPid/generation are captured by closure — by the
        // time this fires (always asynchronously), both `const`s above
        // have already been assigned, since a real process only exits
        // after this synchronous call returns.
        (code) => this.handleUnexpectedExit(sessionKey, generation, expectPid, code)
      );

      process.stderr.write(
        `session-pool: spawned expect pid=${expectPid} key=${sessionKey} ` +
          `socket=${socketPath} name=${sessionName} gen=${generation}\n`
      );

      await waitForSocket(socketPath, SOCKET_WAIT_MS);

      const client = this.deps.createClient(socketPath);
      const now = this.deps.now();
      const state: SessionState = {
        socketPath,
        expectPid,
        client,
        generation,
        createdAt: now,
        lastActivityAt: now,
        sessionName,
      };

      this.sessions.set(sessionKey, state);
      return state;
    } finally {
      this.pendingSpawns.delete(socketPath);
    }
  }

  // --------------------------------------------------------------------------
  // handleUnexpectedExit — gap (c): reactive cleanup the instant a spawned
  // child dies for ANY reason, without waiting for a future getOrCreate().
  // --------------------------------------------------------------------------

  /**
   * Invoked via the spawnLauncher onExit callback when a session's expect
   * process exits, whatever the cause (crash, external SIGKILL, normal
   * expect completion). Removes the Map entry and cleans up its socket
   * file immediately — this is what makes an abrupt supervisor-side crash
   * of the CHILD (as opposed to the whole pool process) self-heal without
   * waiting for the idle-eviction scan or another request for the same key.
   *
   * Guarded by GENERATION identity (discretionary item A — ABA hardening),
   * not PID. A PID-only guard is vulnerable to OS PID reuse: if this exact
   * key is evicted and respawned before this callback fires, and the new
   * spawn happens to be assigned the SAME PID by the OS (recycled — bun's
   * synthetic PIDs in tests can coincide too), `state.expectPid !==
   * expectPid` would wrongly be false and this would tear down the NEWER,
   * legitimate session. `generation` is a fresh randomUUID per spawn — it
   * can never collide with a prior spawn's, so it is the correct identity
   * to compare, not the OS-recycled PID number. expectPid is passed along
   * only for the log line (still useful to know which PID exited).
   */
  private handleUnexpectedExit(
    sessionKey: string,
    generation: string,
    expectPid: number,
    code: number | null
  ): void {
    const state = this.sessions.get(sessionKey);
    if (!state || state.generation !== generation) return;

    this.sessions.delete(sessionKey);
    process.stderr.write(
      `session-pool: pid=${expectPid} gen=${generation} for key=${sessionKey} exited unexpectedly ` +
        `(code=${code}) — removed from pool without waiting for next request\n`
    );

    try { state.client.close(); } catch { /* ignore */ }

    if (existsSync(state.socketPath)) {
      unlink(state.socketPath).catch(() => { /* ignore — already gone */ });
    }
  }

  // --------------------------------------------------------------------------
  // release — update lastActivityAt after a completed turn
  // --------------------------------------------------------------------------

  release(sessionKey: string): void {
    const state = this.sessions.get(sessionKey);
    if (state) {
      state.lastActivityAt = this.deps.now();
    }
  }

  // --------------------------------------------------------------------------
  // evict — remove a session, optionally draining in-flight requests first
  // --------------------------------------------------------------------------

  /**
   * Evict a session.
   *
   * @param graceful  If true, wait up to DRAIN_TIMEOUT_MS for pendingCount===0
   *                  before killing. Use false for stale/forced evictions.
   *
   * Kill order (scoped, never generic), escalating ONLY as far as needed:
   *   1. SIGTERM to exact expect PID.
   *   2. lsof -t <socket> → SIGTERM each holder (the plugin's bun listener).
   *   3. Verify the exact PID actually died (gap a). If it survived SIGTERM,
   *      escalate to SIGKILL and verify again.
   *   4. Last resort: pkill -f "<sessionName>" (hash-scoped, NOT generic) —
   *      ONLY if the PID is still detectable after the SIGKILL escalation.
   *   NEVER pkill -f "claude_hermes" without the hash suffix.
   *
   * Review item 3 — `claude` is never signaled directly, only `expect`
   * (step 1) and the socket holder (step 2). This is intentional, verified
   * empirically (`scratch/ja25-cascade-probe.ts` against a real spawned
   * session, not assumed): `expect` allocates the pty `claude` runs
   * attached to, so when `expect` dies the kernel sends SIGHUP to that
   * pty's foreground process group — `claude` dies too, via that cascade,
   * WITHOUT evict() ever targeting it directly. Measured: expect died at
   * +53ms, claude followed via the cascade at +2594ms. So by the time
   * evict() returns, `expect` is confirmed dead but `claude` may still be
   * mid-teardown for a couple more seconds in the background — this is
   * fine because the thing that actually matters for socket-path reuse
   * (freeing the socket so a respawn doesn't collide) is handled
   * immediately and directly by step 2 (killSocketHolders), which does not
   * wait on that cascade at all.
   */
  async evict(sessionKey: string, graceful: boolean): Promise<void> {
    const state = this.sessions.get(sessionKey);
    if (!state) return;

    this.sessions.delete(sessionKey);

    if (graceful) {
      await this.drainClient(state);
    }

    // 1. Kill the exact expect PID.
    if (state.expectPid !== undefined) {
      this.deps.killPid(state.expectPid, "SIGTERM");
    }

    // 2. Kill all processes holding the socket (the plugin bun process).
    this.deps.killSocketHolders(state.socketPath);

    // Brief wait for graceful termination.
    await new Promise<void>((r) => setTimeout(r, EVICT_SIGTERM_GRACE_MS));

    // 3. Gap (a): verify the exact PID actually died — never assume a
    // fire-and-forget signal worked. Escalate SIGTERM -> SIGKILL, and only
    // fall back to the name-scoped last resort if the PID survives BOTH.
    let stillAlive =
      state.expectPid !== undefined && this.deps.isPidAlive(state.expectPid);

    if (stillAlive && state.expectPid !== undefined) {
      process.stderr.write(
        `session-pool: pid=${state.expectPid} survived SIGTERM for key=${sessionKey} — escalating to SIGKILL\n`
      );
      this.deps.killPid(state.expectPid, "SIGKILL");
      await new Promise<void>((r) => setTimeout(r, EVICT_SIGKILL_GRACE_MS));
      stillAlive = this.deps.isPidAlive(state.expectPid);
    }

    if (stillAlive) {
      process.stderr.write(
        `session-pool: pid=${state.expectPid} survived SIGKILL for key=${sessionKey} — falling back to killByName\n`
      );
      // Last resort: pkill by scoped session name.
      // sessionName is always "claude_hermes_<hash8>" — never the bare generic.
      // Injected via deps so tests can assert the exact name pattern.
      this.deps.killByName(state.sessionName);
    }

    // Close the client.
    try { state.client.close(); } catch { /* ignore */ }

    // Remove stale socket file.
    if (existsSync(state.socketPath)) {
      try { await unlink(state.socketPath); } catch { /* ignore */ }
    }

    process.stderr.write(
      `session-pool: evicted key=${sessionKey} name=${state.sessionName}\n`
    );
  }

  // --------------------------------------------------------------------------
  // drainClient — wait for in-flight requests to complete
  // --------------------------------------------------------------------------

  private async drainClient(state: SessionState): Promise<void> {
    if (state.client.pendingCount === 0) return;

    process.stderr.write(
      `session-pool: draining ${state.client.pendingCount} pending ` +
        `requests for ${state.sessionName}...\n`
    );

    const deadline = this.deps.now() + DRAIN_TIMEOUT_MS;
    while (state.client.pendingCount > 0 && this.deps.now() < deadline) {
      await new Promise<void>((r) => setTimeout(r, 100));
    }

    if (state.client.pendingCount > 0) {
      process.stderr.write(
        `session-pool: drain timeout for ${state.sessionName} ` +
          `(${state.client.pendingCount} requests still pending)\n`
      );
    }
  }

  // --------------------------------------------------------------------------
  // evictLruIdle — evict the longest-idle session with pendingCount===0
  // --------------------------------------------------------------------------

  private async evictLruIdle(): Promise<boolean> {
    let oldestKey: string | undefined;
    let oldestActivity = Infinity;

    for (const [key, state] of this.sessions) {
      if (
        state.client.pendingCount === 0 &&
        state.lastActivityAt < oldestActivity
      ) {
        oldestActivity = state.lastActivityAt;
        oldestKey = key;
      }
    }

    if (oldestKey === undefined) return false;

    process.stderr.write(
      `session-pool: evicting LRU idle session key=${oldestKey} to make room\n`
    );
    await this.evict(oldestKey, true);
    return true;
  }

  // --------------------------------------------------------------------------
  // Idle eviction loop
  // --------------------------------------------------------------------------

  /**
   * Start the background idle-eviction loop.
   * The interval is .unref()'d so it won't keep the process alive in tests.
   *
   * Gap (b): reconcileOrphans() runs on the SAME tick as scanAndEvictIdle —
   * the in-memory Map scan catches idle sessions THIS process knows about;
   * the OS-level sweep catches sessions it never knew about (survivors of a
   * prior, now-dead, instance of this same pool).
   */
  startIdleEviction(): void {
    if (this.evictionTimer !== undefined) return;
    this.evictionTimer = setInterval(() => {
      void this.scanAndEvictIdle(this.deps.now());
      void this.reconcileOrphans();
    }, EVICTION_INTERVAL_MS);
    this.evictionTimer.unref();
  }

  /**
   * Scan sessions and evict those idle longer than IDLE_TTL_MS.
   * Accepts `now` so tests can inject a deterministic clock.
   */
  async scanAndEvictIdle(now: number): Promise<void> {
    // Compute the warm set: the N most-recently-active sessions are protected
    // from idle eviction. MIN_WARM should be < MAX_SESSIONS.
    const minWarm = getMinWarm();
    const warmSet = new Set<string>();
    if (minWarm > 0) {
      const sorted = [...this.sessions.keys()].sort((a, b) => {
        const aActivity = this.sessions.get(a)!.lastActivityAt;
        const bActivity = this.sessions.get(b)!.lastActivityAt;
        return bActivity - aActivity; // descending: most recent first
      });
      for (const key of sorted.slice(0, minWarm)) {
        warmSet.add(key);
      }
    }

    for (const [key, state] of [...this.sessions]) {
      // Skip sessions in the warm set — protected from idle eviction.
      if (warmSet.has(key)) continue;

      const idleMs = now - state.lastActivityAt;
      if (idleMs >= getIdleTtlMs() && state.client.pendingCount === 0) {
        process.stderr.write(
          `session-pool: idle eviction key=${key} idleMs=${idleMs}\n`
        );
        await this.evict(key, false);
      }
    }
  }

  // --------------------------------------------------------------------------
  // reconcileOrphans — gap (b): OS-level sweep, independent of the Map
  // --------------------------------------------------------------------------

  /**
   * Scan THIS INSTANCE's own socket directory ($HERMES_HOME/run/channels)
   * for ".sock" files not tracked in the in-memory Map, and sweep them.
   *
   * Why this is needed: if the WHOLE pool process crashes (not just one
   * session's child — an abrupt death of the process this class itself
   * runs in), the replacement process starts with an empty Map. Any
   * session the dead process had spawned is now a real orphan — still
   * running, still holding its socket — and completely invisible to
   * scanAndEvictIdle(), which only ever walks `this.sessions`.
   *
   * Safety (deliberately conservative, never a broad process sweep):
   *   - Scoped to THIS instance's own channels dir via HERMES_HOME — can
   *     never reach another instance's sockets (e.g. prod's ~/.hermes).
   *   - NEVER kills by name. An earlier version called `killByName`
   *     (pkill -f "claude_hermes_<hash8>") here — wrong, because `pkill -f`
   *     matches by PROCESS COMMAND LINE, machine-wide, with no reference
   *     to HERMES_HOME or any directory at all. The channels-dir scoping
   *     above only protects the file-enumeration step; it does nothing to
   *     stop a name-based pkill from matching an unrelated process on a
   *     DIFFERENT instance (e.g. prod) that happens to compute the same
   *     hash8 for a different session_key.
   *   - Instead, full subtree cleanup (leaf + its ancestors, e.g.
   *     plugin-bun → claude → expect) is done by `killOrphanAncestry()`:
   *     climbing from the leaf PID (found via an exact lsof lookup on
   *     THIS session's own socket path — the one thing we know for
   *     certain belongs to this exact session) up through `ps`-reported
   *     parent PIDs, killing each by EXACT PID only, with a per-candidate
   *     identity check (its own argv, or its parent's, must match this
   *     session's signature) and a hard stop before ever reaching
   *     something that looks like this instance's own live supervisor.
   *     See that method's docstring for the full guard rationale.
   *
   * Race (review item 1): a session mid-spawn has its socket file created
   * on disk BEFORE `sessions.set()` runs (spawn() only sets the Map entry
   * after `waitForSocket()` resolves) — so a naive Map-only check could
   * mistake an in-flight spawn for an orphan. Guarded two ways, both
   * re-checked LIVE per entry (never a pre-loop snapshot, which itself
   * could go stale while this loop `await`s across iterations):
   *   - `pendingSpawns` — reserved before spawnLauncher() runs, in case the
   *     socket file appears before the Map is ever touched.
   *   - a live scan of `this.sessions` — in case a spawn completes (Map
   *     updated) WHILE this loop is still running.
   */
  async reconcileOrphans(): Promise<void> {
    const dir = channelSocketsDir();

    let entries: string[];
    try {
      entries = await readdir(dir);
    } catch {
      return; // Directory doesn't exist yet — nothing to reconcile.
    }

    for (const entry of entries) {
      if (!entry.endsWith(".sock")) continue;

      const socketPath = join(dir, entry);

      // Live checks — NOT a snapshot taken before the loop (see docstring).
      if (this.pendingSpawns.has(socketPath)) continue; // mid-spawn.
      const isTracked = [...this.sessions.values()].some(
        (s) => s.socketPath === socketPath
      );
      if (isTracked) continue; // known, managed session.

      const hash16 = entry.slice(0, -".sock".length);
      const sessionName = `claude_hermes_${hash16.slice(0, 8)}`;

      const leafPids = this.deps.listSocketHolderPids(socketPath);
      let anyRefused = false;

      if (leafPids.length === 0) {
        process.stderr.write(
          `session-pool: reconcile found untracked socket ${socketPath} ` +
            `(name=${sessionName}) with no live holder — removing stale file\n`
        );
      } else {
        process.stderr.write(
          `session-pool: reconcile found untracked socket ${socketPath} ` +
            `(name=${sessionName}) — walking ancestry from leaf pid(s)=${leafPids.join(",")}\n`
        );
        for (const leafPid of leafPids) {
          const swept = await this.killOrphanAncestry(leafPid, sessionName);
          if (!swept) anyRefused = true;
        }
      }

      // Review item 4 (sibling-supervisor guard): if ANY leaf's ancestry
      // could not be positively confirmed as a true orphan (reparented to
      // PID 1 with no live supervisor anywhere above it), do NOT remove
      // the socket file either — it may still be legitimately in use by a
      // live sibling supervisor's own tracked session. Only ever unlink
      // when we're certain: no live holder at all, or every holder's
      // ancestry was confirmed orphaned and swept.
      if (anyRefused) {
        process.stderr.write(
          `session-pool: reconcile leaving socket ${socketPath} untouched ` +
            `(name=${sessionName}) — could not confirm true orphan, not removing\n`
        );
        continue;
      }

      if (existsSync(socketPath)) {
        try { await unlink(socketPath); } catch { /* ignore — already gone */ }
      }
    }
  }

  // --------------------------------------------------------------------------
  // killOrphanAncestry — full subtree cleanup for reconcileOrphans, exact
  // PID only, never by name.
  // --------------------------------------------------------------------------

  /** Defensive bound on the ancestry walk. Real chain (leaf, plugin-bun wrapper, claude, expect) is 4 hops. */
  private static readonly ANCESTRY_WALK_MAX_HOPS = 6;

  /**
   * Climb from a discovered leaf PID (the exact process holding this
   * session's own socket, per an lsof lookup on its exact path) up
   * through its process ancestry, killing each verified hop by EXACT PID
   * only.
   *
   * THREE STRICTLY SEPARATE PHASES — discover, then verify, then kill.
   * Empirically discovered why this separation is required (not a
   * style preference): killing the leaf can have a side effect where an
   * intermediate process (e.g. the hermes-channel plugin's own bun
   * wrapper, whose job is `bun install && bun server.ts`) naturally exits
   * shortly after ITS OWN child (the leaf) is killed — completely
   * independent of anything this method does to IT directly. An earlier,
   * interleaved version of this walk (discover-a-hop, verify-it,
   * kill-it, repeat) queried each node's ppid/cmdline lazily, one hop at
   * a time — so by the time it reached the wrapper hop, the wrapper had
   * ALREADY died as a side effect of the leaf's own kill (still inside
   * the leaf's SIGTERM grace wait), and `ps` can no longer report a dead
   * PID's ppid/cmdline at all. That made a REAL identity match (the
   * wrapper's true parent, `claude`, genuinely matched `sessionName`)
   * look like a failure, and the walk stopped early — reproduced live
   * against a real spawned session (`scratch/ja25-ancestry-walk-demo.ts`)
   * before this fix. Discovering the whole chain FIRST (pure `ps` reads,
   * no kills in between) means every lookup happens while the chain is
   * still fully intact, so a later kill's side effects can never corrupt
   * an earlier verification decision.
   *
   * Phase 1 (discover): walk from the leaf upward via `getPpid`,
   * recording each PID's command line, until PID 1 (natural top of an
   * orphaned tree), the max hop bound, or this instance's own live
   * supervisor (never even added to the chain — see below).
   *
   * Phase 2 (verify): the leaf (index 0) is pre-verified — it was found
   * via an exact-path lsof match on THIS session's own deterministic
   * socket, so nothing else could hold that fd. Every ancestor beyond the
   * leaf must either match this session's own signature directly (its
   * argv contains `sessionName`, i.e. it's `claude`, or contains this
   * instance's own `launcherExpPath`, i.e. it's `expect`) OR have a
   * PARENT (in the discovered chain, not a live re-query) that matches
   * one of those. That second clause covers the one intermediate hop in
   * the real tree — the plugin's bun wrapper, whose own argv carries no
   * session-specific string. The first node matching neither ends the
   * trusted prefix — under-killing is always preferred over killing an
   * unverified process.
   *
   * Phase 3 (kill): every node in the trusted prefix gets the SAME
   * SIGTERM → grace → SIGKILL-if-survived escalation as evict() (reusing
   * the same named grace constants) via `killPidWithGraceEscalation` — a
   * separate helper from evict()'s own inline sequence, deliberately not
   * shared, so this addition can never alter evict()'s already-shipped,
   * already-tested behavior. Killing a node that already died naturally
   * (e.g. the wrapper, per the mechanism above) is a safe no-op —
   * killPid/isPidAlive already tolerate an absent PID.
   *
   * Review item 4 — sibling-supervisor guard. reconcileOrphans()'s whole
   * premise is "untracked in THIS process's Map == the owning supervisor
   * is dead." That's false when TWO supervisor processes share the same
   * HERMES_HOME (observed live on this box, ordinary multi-session
   * behavior per the Python-side ACP client transport) — if their Maps
   * ever diverge on a key (near-guaranteed: they're separate processes
   * with separate Maps, nothing synchronizes them), instance A's reconcile
   * tick would see instance B's perfectly live, legitimately-owned session
   * as "untracked" and, without this guard, kill it out from under B.
   *
   * The fix: a subtree is trusted as a genuine orphan ONLY when discovery
   * positively reaches PID 1 (the kernel's actual reparent target — the
   * one unambiguous proof nothing alive still owns it). Any other reason
   * discovery stops — hitting a live process that looks like a supervisor
   * (this instance's own, or ANY sibling's: both run the identical
   * acp_entrypoint.ts from the identical path under this same HERMES_HOME,
   * so they're indistinguishable by argv, and there is no need to tell
   * them apart — either way something alive still owns this subtree), the
   * hop bound, or a broken mid-walk lookup — means "not positively
   * confirmed dead," and the ENTIRE subtree is left alone, not just the
   * ancestor where discovery stopped. Returns false in that case so
   * reconcileOrphans() also knows not to remove the socket file.
   *
   * @returns true if the subtree was confirmed a true orphan and swept
   *          (or partially swept, per the identity guard within Phase 2/3);
   *          false if the walk refused to touch it at all.
   */
  private async killOrphanAncestry(
    leafPid: number,
    sessionName: string
  ): Promise<boolean> {
    // --- Phase 1: discover (pure reads, no kills) -------------------------
    const chainPids: number[] = [];
    const chainCmdlines: Array<string | undefined> = [];
    let currentPid: number | undefined = leafPid;
    let confirmedOrphan = false; // only true if we positively reach PID 1

    for (
      let hop = 0;
      hop < ChannelsSessionPool.ANCESTRY_WALK_MAX_HOPS && currentPid !== undefined;
      hop++
    ) {
      const cmdline = this.deps.getCommandLine(currentPid);

      if (this.looksLikeSupervisorCmdline(cmdline)) {
        process.stderr.write(
          `session-pool: reconcile ancestry discovery stopped at pid=${currentPid} ` +
            `(name=${sessionName}) — matches a live supervisor (this instance's own, ` +
            `or a sibling sharing this HERMES_HOME) — NOT a confirmed orphan\n`
        );
        break; // never added to the chain — never a kill candidate.
      }

      chainPids.push(currentPid);
      chainCmdlines.push(cmdline);

      const ppid = this.deps.getPpid(currentPid);
      if (ppid === 1) {
        confirmedOrphan = true; // positively reparented to init — genuine orphan.
        break;
      }
      if (ppid === undefined) break; // process vanished mid-walk — ambiguous, not confirmed.
      currentPid = ppid;
    }

    if (!confirmedOrphan) {
      process.stderr.write(
        `session-pool: reconcile ancestry walk for leaf=${leafPid} (name=${sessionName}) ` +
          `did NOT positively confirm a true orphan (no live supervisor found above it, ` +
          `and reparenting to PID 1 was never observed) — refusing to touch this subtree\n`
      );
      return false;
    }

    // --- Phase 2: verify (pure logic over the static snapshot above) -----
    let trustedCount = 0;
    for (let i = 0; i < chainPids.length; i++) {
      if (i === 0) {
        trustedCount = 1; // leaf: pre-verified by the exact-path lsof lookup that found it.
        continue;
      }
      const selfMatches = this.matchesSignature(chainCmdlines[i], sessionName);
      const parentCmdline = i + 1 < chainCmdlines.length ? chainCmdlines[i + 1] : undefined;
      const parentMatches = this.matchesSignature(parentCmdline, sessionName);
      if (!selfMatches && !parentMatches) {
        process.stderr.write(
          `session-pool: reconcile ancestry walk stopped at pid=${chainPids[i]} — ` +
            `no identity match for name=${sessionName}\n`
        );
        break;
      }
      trustedCount = i + 1;
    }

    // --- Phase 3: kill the verified prefix, leaf first --------------------
    for (let i = 0; i < trustedCount; i++) {
      await this.killPidWithGraceEscalation(
        chainPids[i],
        `reconcile ancestry hop=${i} name=${sessionName}`
      );
    }

    return true;
  }

  /**
   * Does this command line look like a live acp_entrypoint supervisor
   * process — this instance's own, or a sibling's? Both run the identical
   * acp_entrypoint.ts from the identical path under a shared HERMES_HOME,
   * so they are indistinguishable by argv alone, and (per the
   * sibling-supervisor guard above) there is no need to tell them apart —
   * either way, something alive still owns whatever is above this point.
   */
  private looksLikeSupervisorCmdline(cmdline: string | undefined): boolean {
    if (!cmdline) return false;
    const acpEntrypointHint = join(
      dirname(this.launcherExpPath),
      "src",
      "acp_entrypoint.ts"
    );
    return cmdline.includes(acpEntrypointHint);
  }

  /** Does this command line match this session's own signature? */
  private matchesSignature(cmdline: string | undefined, sessionName: string): boolean {
    if (!cmdline) return false;
    return cmdline.includes(sessionName) || cmdline.includes(this.launcherExpPath);
  }

  /**
   * SIGTERM -> grace -> verify -> SIGKILL-if-survived -> grace -> verify.
   * Never falls back to a name-based kill (unlike evict()'s last resort)
   * — if a candidate survives even SIGKILL here, it's logged and left
   * alone rather than escalating to an unscoped primitive.
   */
  private async killPidWithGraceEscalation(
    pid: number,
    context: string
  ): Promise<void> {
    this.deps.killPid(pid, "SIGTERM");
    await new Promise<void>((r) => setTimeout(r, EVICT_SIGTERM_GRACE_MS));
    if (!this.deps.isPidAlive(pid)) return;

    process.stderr.write(
      `session-pool: pid=${pid} survived SIGTERM (${context}) — escalating to SIGKILL\n`
    );
    this.deps.killPid(pid, "SIGKILL");
    await new Promise<void>((r) => setTimeout(r, EVICT_SIGKILL_GRACE_MS));
    if (this.deps.isPidAlive(pid)) {
      process.stderr.write(
        `session-pool: pid=${pid} survived SIGKILL (${context}) — giving up on this PID\n`
      );
    }
  }

  // --------------------------------------------------------------------------
  // shutdown — gracefully evict all sessions (called on SIGTERM)
  // --------------------------------------------------------------------------

  async shutdown(): Promise<void> {
    if (this.evictionTimer !== undefined) {
      clearInterval(this.evictionTimer);
      this.evictionTimer = undefined;
    }

    process.stderr.write(
      `session-pool: shutting down ${this.sessions.size} sessions...\n`
    );

    await Promise.all(
      [...this.sessions.keys()].map((key) => this.evict(key, true))
    );

    process.stderr.write("session-pool: shutdown complete\n");
  }

  // --------------------------------------------------------------------------
  // Helpers
  // --------------------------------------------------------------------------

  private isPidAlive(pid: number): boolean {
    return this.deps.isPidAlive(pid);
  }

  /** Expose sessions count for observability / tests. */
  get size(): number {
    return this.sessions.size;
  }
}

// ---------------------------------------------------------------------------
// PoolFullError — structured refusal when at capacity and all sessions busy
// ---------------------------------------------------------------------------

export class PoolFullError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "PoolFullError";
  }
}
