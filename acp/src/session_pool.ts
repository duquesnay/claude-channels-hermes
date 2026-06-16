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
import { mkdir, unlink } from "node:fs/promises";
import { existsSync } from "node:fs";
import {
  isSocket,
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
   */
  spawnLauncher(
    launcherExpPath: string,
    env: Record<string, string>
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
   */
  killByName(sessionName: string): void;
}

// ---------------------------------------------------------------------------
// Default (production) implementations of deps
// ---------------------------------------------------------------------------

function defaultSpawnLauncher(
  launcherExpPath: string,
  env: Record<string, string>
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

const PRODUCTION_DEPS: PoolDeps = {
  spawnLauncher: defaultSpawnLauncher,
  createClient: (socketPath) => new HermesChannelClient(socketPath),
  killPid: defaultKillPid,
  killSocketHolders: defaultKillSocketHolders,
  now: () => Date.now(),
  isPidAlive: defaultIsPidAlive,
  killByName: defaultKillByName,
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
      const socketPresent = isSocket(existing.socketPath);
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
    const expectPid = this.deps.spawnLauncher(this.launcherExpPath, {
      HERMES_CHANNEL_SOCKET: socketPath,
      HERMES_SESSION_NAME: sessionName,
      // Persona/sandbox cwd: explicit env wins so the launcher cd's to the
      // instance sandbox (e.g. ~/.janet-test/acp-sandbox) and the claude session
      // loads the right CLAUDE.md + .mcp.json. Falls back to the supervisor cwd.
      HERMES_SESSION_CWD: process.env.HERMES_SESSION_CWD ?? process.cwd(),
      ...(noAccountConnectors && { ENABLE_CLAUDEAI_MCP_SERVERS: "0" }),
    });

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
   * Kill order (scoped, never generic):
   *   1. SIGTERM to exact expect PID.
   *   2. lsof -t <socket> → SIGTERM each holder.
   *   3. Last resort: pkill -f "<sessionName>" (hash-scoped, NOT generic).
   *   NEVER pkill -f "claude_hermes" without the hash suffix.
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
    await new Promise<void>((r) => setTimeout(r, 500));

    // 3. Last resort: pkill by scoped session name.
    // sessionName is always "claude_hermes_<hash8>" — never the bare generic.
    // Injected via deps so tests can assert the exact name pattern.
    this.deps.killByName(state.sessionName);

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
   */
  startIdleEviction(): void {
    if (this.evictionTimer !== undefined) return;
    this.evictionTimer = setInterval(
      () => void this.scanAndEvictIdle(this.deps.now()),
      EVICTION_INTERVAL_MS
    );
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
