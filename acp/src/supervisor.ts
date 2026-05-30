/**
 * supervisor.ts — Manages the background claude --channels session.
 *
 * Owns the lifecycle of the persistent claude session that hosts the
 * hermes-channel plugin. The plugin opens the Unix socket server;
 * this supervisor waits for it, then the rest of the ACP stack connects.
 *
 * Design decisions (see spec §Socket path):
 * - Default socket: ~/.hermes/run/hermes-channel.sock
 * - Overridable via HERMES_CHANNEL_SOCKET env on the supervisor side only.
 *   The env does NOT propagate through claude's MCP loader to the plugin,
 *   so the plugin always binds the default path. HERMES_CHANNEL_SOCKET only
 *   controls which path the supervisor/client side connects to.
 * - Single instance: one session, one socket, no per-instance isolation.
 * - No keep-alive/respawn: launchd or the caller is responsible (YAGNI).
 *
 * Process name: claude_hermes_acp (must match launcher.exp --name and shutdown).
 */

import { existsSync, statSync } from "node:fs";
import { unlinkSync } from "node:fs";
import { homedir } from "node:os";
import { join, dirname } from "node:path";
import { mkdir } from "node:fs/promises";

// ---------------------------------------------------------------------------
// Constants — process name is the single source of truth used by launcher.exp,
// reuse detection (pgrep), and shutdown (pkill). Keep in sync.
// ---------------------------------------------------------------------------

/** The --name passed to claude in launcher.exp. MUST match exactly. */
export const CLAUDE_SESSION_NAME = "claude_hermes_acp";

/** Default socket path — where the hermes-channel plugin binds. */
export const DEFAULT_SOCKET_PATH = join(
  homedir(),
  ".hermes",
  "run",
  "hermes-channel.sock"
);

/** How long to wait for the socket to appear (plugin runs bun install first). */
const WAIT_TIMEOUT_MS = 75_000;

/** Poll interval for waitForSocket. */
const POLL_INTERVAL_MS = 500;

// ---------------------------------------------------------------------------
// Pure helpers (unit-testable, no spawn)
// ---------------------------------------------------------------------------

/**
 * Resolve the socket path from the environment or the default.
 * HERMES_CHANNEL_SOCKET is read only on the supervisor side.
 */
export function resolveSocketPath(): string {
  return process.env["HERMES_CHANNEL_SOCKET"] ?? DEFAULT_SOCKET_PATH;
}

/**
 * Test whether a path is a live Unix socket file.
 * Mirrors run_spike.sh's `[ -S "$SOCK" ]` check.
 */
export function isSocket(path: string): boolean {
  try {
    return statSync(path).isFIFO() === false && statSync(path).isSocket();
  } catch {
    return false;
  }
}

/**
 * Test whether the claude_hermes_acp process is running.
 * Uses pgrep -f for name-based detection (same as pkill -f in shutdown).
 */
export function isSessionRunning(): boolean {
  try {
    const result = Bun.spawnSync(["pgrep", "-f", CLAUDE_SESSION_NAME], {
      stdout: "pipe",
      stderr: "pipe",
    });
    return result.exitCode === 0;
  } catch {
    return false;
  }
}

/**
 * Pure session-state decision: given socket presence and proc presence,
 * determine whether to reuse an existing session or launch a new one.
 *
 * @returns "reuse" | "launch" | "stale"
 *   - "reuse": socket up + proc running → connect and use as-is
 *   - "stale": socket file present but proc gone → remove socket then launch
 *   - "launch": socket absent → launch fresh session
 */
export function sessionDecision(
  socketPresent: boolean,
  procPresent: boolean
): "reuse" | "launch" | "stale" {
  if (socketPresent && procPresent) return "reuse";
  if (socketPresent && !procPresent) return "stale";
  return "launch";
}

/**
 * Wait until the socket file appears or timeout is reached.
 * Resolves when the socket is ready, rejects on timeout.
 *
 * @param socketPath  Path to the Unix socket to poll for.
 * @param timeoutMs   Maximum wait time in milliseconds (default: 75 000).
 * @param pollMs      Poll interval in milliseconds (default: 500).
 */
export function waitForSocket(
  socketPath: string,
  timeoutMs: number = WAIT_TIMEOUT_MS,
  pollMs: number = POLL_INTERVAL_MS
): Promise<void> {
  return new Promise<void>((resolve, reject) => {
    const deadline = Date.now() + timeoutMs;

    function check(): void {
      if (isSocket(socketPath)) {
        resolve();
        return;
      }
      if (Date.now() >= deadline) {
        reject(
          new Error(
            `supervisor: socket ${socketPath} did not appear within ${timeoutMs}ms`
          )
        );
        return;
      }
      setTimeout(check, pollMs);
    }

    check();
  });
}

// ---------------------------------------------------------------------------
// Supervisor state
// ---------------------------------------------------------------------------

/** PID of the expect launcher process (undefined if not started by us). */
let expectPid: number | undefined;

// ---------------------------------------------------------------------------
// spawnSession — launch the background claude session via expect
// ---------------------------------------------------------------------------

/**
 * Spawn the expect launcher as a background process.
 * All output from expect/claude goes to stderr to preserve stdout purity.
 * The launcher.exp path is resolved relative to this file's directory.
 */
function spawnSession(launcherExpPath: string): void {
  const child = Bun.spawn(["expect", "-f", launcherExpPath], {
    // All output to stderr — stdout must remain clean NDJSON for Janet.
    stdout: "pipe",
    stderr: "pipe",
    stdin: "ignore",
  });

  expectPid = child.pid;
  process.stderr.write(
    `supervisor: launched expect pid=${expectPid} launcher=${launcherExpPath}\n`
  );

  // Drain expect/claude output to stderr asynchronously (prevent pipe backpressure).
  void pipeToStderr(child.stdout, "expect/claude stdout");
  void pipeToStderr(child.stderr, "expect/claude stderr");
}

/** Drain a ReadableStream to stderr with a prefix (prevents pipe deadlock). */
async function pipeToStderr(
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
    process.stderr.write(`supervisor: ${label} stream ended\n`);
  }
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/**
 * Ensure the claude_hermes_acp session is running and its socket is ready.
 *
 * - If already up (socket + proc): reuse, skip launch.
 * - If socket stale (file present, proc gone): remove then launch.
 * - If nothing: launch fresh.
 *
 * Always waits until the socket is ready before returning.
 *
 * @param socketPath   The socket path to wait for (default: resolveSocketPath()).
 * @param launcherExp  Path to the expect launcher script.
 */
export async function ensureSession(
  socketPath: string = resolveSocketPath(),
  launcherExp: string = join(dirname(new URL(import.meta.url).pathname), "..", "launcher.exp")
): Promise<void> {
  const socketPresent = isSocket(socketPath);
  const procPresent = isSessionRunning();
  const decision = sessionDecision(socketPresent, procPresent);

  process.stderr.write(
    `supervisor: socket=${socketPresent} proc=${procPresent} → ${decision}\n`
  );

  if (decision === "reuse") {
    process.stderr.write(`supervisor: reusing existing session (${CLAUDE_SESSION_NAME})\n`);
    return;
  }

  if (decision === "stale") {
    // Remove the stale socket before launching to avoid connect-to-dead-socket.
    try {
      unlinkSync(socketPath);
      process.stderr.write(`supervisor: removed stale socket ${socketPath}\n`);
    } catch {
      // Already gone — continue.
    }
  }

  // Ensure the socket directory exists before launching (plugin will bind here).
  await mkdir(dirname(socketPath), { recursive: true });

  spawnSession(launcherExp);
  process.stderr.write(
    `supervisor: waiting for socket ${socketPath} (up to ${WAIT_TIMEOUT_MS}ms)...\n`
  );
  await waitForSocket(socketPath);
  process.stderr.write(`supervisor: socket ready\n`);
}

/**
 * Shut down the claude session cleanly.
 *
 * Mirrors run_spike.sh teardown:
 *   1. Kill the expect launcher (SIGTERM).
 *   2. pkill -f claude_hermes_acp (graceful).
 *   3. Kill all lsof holders of the socket.
 *   4. Brief wait.
 *   5. pkill -9 -f claude_hermes_acp (force, survivors only).
 */
export async function shutdown(socketPath: string = resolveSocketPath()): Promise<void> {
  process.stderr.write(`supervisor: shutting down ${CLAUDE_SESSION_NAME}...\n`);

  // 1. Kill the expect launcher if we started it.
  if (expectPid !== undefined) {
    try {
      process.kill(expectPid, "SIGTERM");
      process.stderr.write(`supervisor: sent SIGTERM to expect pid=${expectPid}\n`);
    } catch {
      // Already gone.
    }
    expectPid = undefined;
  }

  // 2. Graceful kill of the claude session by name.
  Bun.spawnSync(["pkill", "-f", CLAUDE_SESSION_NAME], { stdout: "pipe", stderr: "pipe" });

  // 3. Kill all processes holding the socket file (the plugin bun process).
  if (existsSync(socketPath)) {
    try {
      const lsof = Bun.spawnSync(["lsof", "-t", socketPath], {
        stdout: "pipe",
        stderr: "pipe",
      });
      const pids = new TextDecoder().decode(lsof.stdout).trim().split("\n").filter(Boolean);
      for (const pid of pids) {
        try {
          process.kill(parseInt(pid, 10), "SIGTERM");
          process.stderr.write(`supervisor: killed socket holder pid=${pid}\n`);
        } catch {
          // Already gone.
        }
      }
    } catch {
      // lsof not available or socket already gone.
    }
  }

  // 4. Brief wait for graceful termination.
  await new Promise<void>((r) => setTimeout(r, 1000));

  // 5. Force-kill survivors.
  Bun.spawnSync(["pkill", "-9", "-f", CLAUDE_SESSION_NAME], { stdout: "pipe", stderr: "pipe" });

  process.stderr.write(`supervisor: shutdown complete\n`);
}
