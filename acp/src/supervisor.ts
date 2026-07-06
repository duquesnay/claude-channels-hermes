/**
 * supervisor.ts — Pure session-state helpers for the ACP session pool.
 *
 * These are the only exports used by session_pool.ts. The previous single-session
 * lifecycle functions (ensureSession, shutdown, spawnSession) were removed when
 * the pool took over lifecycle management (Workstream 1, 2026-06-15).
 *
 * Remaining exports:
 *   - isSocket / waitForSocket: socket readiness detection
 *   - sessionDecision: pure state machine for reuse vs launch vs stale
 *   - resolveSocketPath / DEFAULT_SOCKET_PATH: single-session compat (test helper)
 */

import { statSync } from "node:fs";
import { createConnection } from "node:net";
import { homedir } from "node:os";
import { join } from "node:path";

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/** Default socket path — kept for backward compat / single-session use. */
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
 * HERMES_CHANNEL_SOCKET is read on the supervisor/pool side only.
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

/** Timeout for a live-listener connect probe (isSocketLive). */
const LIVE_CHECK_TIMEOUT_MS = 1_000;

/**
 * Verify a Unix socket path has an ACTUAL LIVE LISTENER, not just a
 * socket-type file on disk.
 *
 * Gap (d): a process that dies without calling server.close() (SIGKILL,
 * crash) leaves its socket file behind — isSocket() still reports it as
 * "present" (correct file type), but nothing is listening on it anymore.
 * Reusing such a session (sessionDecision would say "reuse" on
 * socketPresent=true) would hand the caller a dead connection. This does a
 * real connect() and treats any failure (ECONNREFUSED, ENOENT, timeout) as
 * "not live" — only a successful connection counts.
 */
export function isSocketLive(
  path: string,
  timeoutMs: number = LIVE_CHECK_TIMEOUT_MS
): Promise<boolean> {
  return new Promise<boolean>((resolve) => {
    if (!isSocket(path)) {
      resolve(false);
      return;
    }

    let settled = false;
    const finish = (result: boolean): void => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      socket.removeAllListeners();
      socket.destroy();
      resolve(result);
    };

    const timer = setTimeout(() => finish(false), timeoutMs);
    const socket = createConnection(path);
    socket.once("connect", () => finish(true));
    socket.once("error", () => finish(false));
  });
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
