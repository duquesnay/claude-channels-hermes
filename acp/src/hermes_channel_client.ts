/**
 * hermes_channel_client.ts — Unix-socket client to a live hermes-channel session.
 *
 * Connects to the hermes-channel IPC socket and forwards prompts using the
 * JSONL multiplexed protocol. ONE shared persistent connection is used for
 * all concurrent requests (multiplexed by request_id).
 *
 * Protocol:
 *   SEND: {"type":"prompt","request_id":"<uuid>","content":"<text>","timeout_ms":<int>}\n
 *   RECV: {"type":"result","request_id":"...","content":"...","duration_ms":<int>}
 *      or {"type":"error","request_id":"...","error":"..."}
 *
 * On socket close or error: all pending requests are rejected and the connection
 * is dropped. The next sendPrompt() call will reconnect automatically.
 */

import { createConnection, type Socket } from "net";
import { homedir } from "os";
import { join } from "path";
import { randomUUID } from "crypto";

// ---------------------------------------------------------------------------
// Public interface — injectable in tests
// ---------------------------------------------------------------------------

export interface HermesChannelClientInterface {
  sendPrompt(content: string, timeoutMs: number): Promise<string>;
  close(): void;
}

// ---------------------------------------------------------------------------
// Pending request entry
// ---------------------------------------------------------------------------

interface PendingEntry {
  resolve: (content: string) => void;
  reject: (err: Error) => void;
  timer: ReturnType<typeof setTimeout>;
}

// ---------------------------------------------------------------------------
// HermesChannelClient
// ---------------------------------------------------------------------------

const DEFAULT_SOCKET_PATH = join(homedir(), ".hermes", "run", "hermes-channel.sock");

function resolveSocketPath(): string {
  return process.env["HERMES_CHANNEL_SOCKET"] ?? DEFAULT_SOCKET_PATH;
}

export class HermesChannelClient implements HermesChannelClientInterface {
  private readonly socketPath: string;
  private socket: Socket | null = null;
  private connectPromise: Promise<void> | null = null;
  private readonly pending = new Map<string, PendingEntry>();
  private readBuffer = "";

  constructor(socketPath?: string) {
    this.socketPath = socketPath ?? resolveSocketPath();
  }

  // --------------------------------------------------------------------------
  // connect — memoized so concurrent first callers share one connect attempt
  // --------------------------------------------------------------------------

  private connect(): Promise<void> {
    if (this.socket !== null) {
      return Promise.resolve();
    }
    if (this.connectPromise !== null) {
      return this.connectPromise;
    }

    this.connectPromise = new Promise<void>((resolve, reject) => {
      const sock = createConnection(this.socketPath);

      sock.once("connect", () => {
        this.socket = sock;
        this.connectPromise = null;
        process.stderr.write(
          `hermes-channel-client: connected to ${this.socketPath}\n`
        );
        resolve();
      });

      sock.once("error", (err) => {
        if (this.socket === null) {
          // Connect-time error
          this.connectPromise = null;
          reject(err);
        }
      });

      sock.on("data", (chunk: Buffer) => {
        this.readBuffer += chunk.toString("utf8");
        let nl: number;
        while ((nl = this.readBuffer.indexOf("\n")) !== -1) {
          const line = this.readBuffer.slice(0, nl).trim();
          this.readBuffer = this.readBuffer.slice(nl + 1);
          if (line) this.handleLine(line);
        }
      });

      sock.on("close", () => {
        process.stderr.write("hermes-channel-client: socket closed by peer\n");
        this.dropConnection(new Error("hermes-channel-client: socket closed before result"));
      });

      sock.on("error", (err) => {
        if (this.socket !== null) {
          // Post-connect socket error
          process.stderr.write(`hermes-channel-client: socket error: ${err}\n`);
          this.dropConnection(
            new Error(`hermes-channel-client: socket error: ${err.message}`)
          );
        }
      });
    });

    return this.connectPromise;
  }

  // --------------------------------------------------------------------------
  // handleLine — parse one JSONL reply and dispatch to pending
  // --------------------------------------------------------------------------

  private handleLine(line: string): void {
    let msg: Record<string, unknown>;
    try {
      msg = JSON.parse(line) as Record<string, unknown>;
    } catch (err) {
      process.stderr.write(
        `hermes-channel-client: JSON parse error: ${err} — line=${line.slice(0, 200)}\n`
      );
      return;
    }

    const requestId = msg["request_id"] as string | undefined;
    if (!requestId) {
      process.stderr.write(
        `hermes-channel-client: reply without request_id (type=${msg["type"]})\n`
      );
      return;
    }

    const entry = this.pending.get(requestId);
    if (!entry) {
      // Unknown or already-resolved request_id — ignore silently (per spec)
      return;
    }

    this.pending.delete(requestId);
    clearTimeout(entry.timer);

    const msgType = msg["type"] as string | undefined;
    if (msgType === "result") {
      entry.resolve(String(msg["content"] ?? ""));
    } else if (msgType === "error") {
      entry.reject(
        new Error(
          `hermes-channel: ${String(msg["error"] ?? "unknown error")}`
        )
      );
    } else {
      entry.reject(
        new Error(`hermes-channel-client: unexpected reply type=${msgType}`)
      );
    }
  }

  // --------------------------------------------------------------------------
  // dropConnection — reject all pending and clear socket state
  // --------------------------------------------------------------------------

  private dropConnection(err: Error): void {
    const sock = this.socket;
    this.socket = null;
    this.connectPromise = null;
    this.readBuffer = "";

    if (sock) {
      try { sock.destroy(); } catch { /* already destroyed */ }
    }

    for (const [, entry] of this.pending) {
      clearTimeout(entry.timer);
      entry.reject(err);
    }
    this.pending.clear();
  }

  // --------------------------------------------------------------------------
  // sendPrompt — public API
  // --------------------------------------------------------------------------

  async sendPrompt(content: string, timeoutMs: number): Promise<string> {
    await this.connect();

    const requestId = randomUUID();

    // Register pending BEFORE writing to the socket (fast echo can arrive immediately)
    const result = await new Promise<string>((resolve, reject) => {
      const timer = setTimeout(() => {
        this.pending.delete(requestId);
        reject(
          new Error(
            `hermes-channel-client: timeout after ${timeoutMs}ms (request_id=${requestId})`
          )
        );
      }, timeoutMs);
      timer.unref();

      this.pending.set(requestId, { resolve, reject, timer });

      const msg =
        JSON.stringify({
          type: "prompt",
          request_id: requestId,
          content,
          timeout_ms: timeoutMs,
        }) + "\n";

      const sock = this.socket!;
      sock.write(msg, "utf8", (err?: Error | null) => {
        if (err) {
          this.pending.delete(requestId);
          clearTimeout(timer);
          reject(
            new Error(`hermes-channel-client: write failed: ${err.message}`)
          );
          this.dropConnection(
            new Error(`hermes-channel-client: write failed: ${err.message}`)
          );
        }
      });
    });

    return result;
  }

  // --------------------------------------------------------------------------
  // close — tear down the connection (use only on global shutdown)
  // --------------------------------------------------------------------------

  close(): void {
    this.dropConnection(new Error("hermes-channel-client: closed"));
  }
}
