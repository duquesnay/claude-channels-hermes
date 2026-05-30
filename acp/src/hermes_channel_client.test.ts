/**
 * Unit tests for hermes_channel_client.ts.
 *
 * Stands up a fake AF_UNIX server inside the test process that speaks the
 * hermes-channel JSONL protocol. No live socket, no claude, no network cost.
 *
 * Test socket at /tmp/hermes-test-<hex>.sock (short path — avoids macOS
 * ENAMETOOLONG limit of ~104 bytes for sun_path).
 */

import { describe, it, expect, beforeEach, afterEach } from "bun:test";
import { createServer, type Server, type Socket } from "net";
import { unlinkSync, existsSync } from "fs";
import { randomBytes } from "crypto";
import { HermesChannelClient } from "./hermes_channel_client.ts";

// ---------------------------------------------------------------------------
// Fake server helpers
// ---------------------------------------------------------------------------

interface FakeServer {
  server: Server;
  socketPath: string;
  /** The last client socket accepted (set after first connection). */
  clientSocket: Socket | null;
  stop(): Promise<void>;
}

/**
 * Start a fake AF_UNIX server that echoes prompts back:
 *   recv: {type:'prompt', request_id, content, timeout_ms}
 *   send: {type:'result', request_id, content:'echo: '+content, duration_ms:0}
 *
 * The handler function can be overridden per test for error/close scenarios.
 */
function startFakeServer(
  handler?: (msg: Record<string, unknown>, sock: Socket) => void
): Promise<FakeServer> {
  const socketPath = `/tmp/hermes-test-${randomBytes(6).toString("hex")}.sock`;

  if (existsSync(socketPath)) {
    try { unlinkSync(socketPath); } catch { /* ignore */ }
  }

  return new Promise((resolve, reject) => {
    let clientSocket: Socket | null = null;

    const server = createServer((conn) => {
      clientSocket = conn;
      let buf = "";

      conn.on("data", (chunk: Buffer) => {
        buf += chunk.toString("utf8");
        let nl: number;
        while ((nl = buf.indexOf("\n")) !== -1) {
          const line = buf.slice(0, nl).trim();
          buf = buf.slice(nl + 1);
          if (!line) continue;
          let msg: Record<string, unknown>;
          try {
            msg = JSON.parse(line) as Record<string, unknown>;
          } catch {
            return;
          }
          if (handler) {
            handler(msg, conn);
          } else {
            // Default: echo
            const requestId = msg["request_id"] as string;
            const content = msg["content"] as string;
            conn.write(
              JSON.stringify({
                type: "result",
                request_id: requestId,
                content: `echo: ${content}`,
                duration_ms: 0,
              }) + "\n"
            );
          }
        }
      });

      conn.on("error", () => { /* ignore in test server */ });
    });

    server.on("error", reject);

    server.listen(socketPath, () => {
      resolve({
        server,
        socketPath,
        get clientSocket() { return clientSocket; },
        stop(): Promise<void> {
          return new Promise((res) => {
            server.close(() => {
              if (existsSync(socketPath)) {
                try { unlinkSync(socketPath); } catch { /* ignore */ }
              }
              res();
            });
            // Force-close any open connections so server.close() completes
            if (clientSocket) {
              try { clientSocket.destroy(); } catch { /* ignore */ }
            }
          });
        },
      });
    });
  });
}

// ---------------------------------------------------------------------------
// Test suite
// ---------------------------------------------------------------------------

describe("HermesChannelClient", () => {
  let fake: FakeServer;
  let client: HermesChannelClient;

  beforeEach(async () => {
    fake = await startFakeServer();
    // Point the client at the test socket via env var
    process.env["HERMES_CHANNEL_SOCKET"] = fake.socketPath;
    client = new HermesChannelClient(fake.socketPath);
  });

  afterEach(async () => {
    client.close();
    await fake.stop();
    delete process.env["HERMES_CHANNEL_SOCKET"];
  });

  // --------------------------------------------------------------------------
  // Happy path
  // --------------------------------------------------------------------------

  it("sendPrompt returns 'echo: <content>' from the fake server", async () => {
    const result = await client.sendPrompt("hello world", 5000);
    expect(result).toBe("echo: hello world");
  });

  it("sendPrompt resolves with empty string when server returns empty content", async () => {
    const emptyServer = await startFakeServer((msg, sock) => {
      const requestId = msg["request_id"] as string;
      sock.write(
        JSON.stringify({ type: "result", request_id: requestId, content: "", duration_ms: 0 }) + "\n"
      );
    });
    const emptyClient = new HermesChannelClient(emptyServer.socketPath);
    try {
      const result = await emptyClient.sendPrompt("anything", 5000);
      expect(result).toBe("");
    } finally {
      emptyClient.close();
      await emptyServer.stop();
    }
  });

  // --------------------------------------------------------------------------
  // request_id mismatch is ignored
  // --------------------------------------------------------------------------

  it("ignores a reply whose request_id does not match any pending request", async () => {
    // Send a mismatched reply first, then the correct one.
    const mismatchServer = await startFakeServer((msg, sock) => {
      const requestId = msg["request_id"] as string;
      const content = msg["content"] as string;
      // First send a reply with a wrong request_id (should be ignored)
      sock.write(
        JSON.stringify({ type: "result", request_id: "does-not-exist", content: "wrong", duration_ms: 0 }) + "\n"
      );
      // Then send the correct reply
      sock.write(
        JSON.stringify({ type: "result", request_id: requestId, content: `echo: ${content}`, duration_ms: 0 }) + "\n"
      );
    });
    const mismatchClient = new HermesChannelClient(mismatchServer.socketPath);
    try {
      const result = await mismatchClient.sendPrompt("test mismatch", 5000);
      expect(result).toBe("echo: test mismatch");
    } finally {
      mismatchClient.close();
      await mismatchServer.stop();
    }
  });

  // --------------------------------------------------------------------------
  // Error reply rejects the promise
  // --------------------------------------------------------------------------

  it("rejects with error message when server sends {type:'error'}", async () => {
    const errorServer = await startFakeServer((msg, sock) => {
      const requestId = msg["request_id"] as string;
      sock.write(
        JSON.stringify({ type: "error", request_id: requestId, error: "something went wrong" }) + "\n"
      );
    });
    const errorClient = new HermesChannelClient(errorServer.socketPath);
    try {
      await expect(errorClient.sendPrompt("trigger error", 5000)).rejects.toThrow("something went wrong");
    } finally {
      errorClient.close();
      await errorServer.stop();
    }
  });

  // --------------------------------------------------------------------------
  // Timeout
  // --------------------------------------------------------------------------

  it("rejects with timeout error when server does not reply within deadline", async () => {
    const silentServer = await startFakeServer((_msg, _sock) => {
      // Intentionally does not reply
    });
    const silentClient = new HermesChannelClient(silentServer.socketPath);
    try {
      await expect(silentClient.sendPrompt("no reply", 50)).rejects.toThrow("timeout");
    } finally {
      silentClient.close();
      await silentServer.stop();
    }
  });

  // --------------------------------------------------------------------------
  // Socket close fails all pending
  // --------------------------------------------------------------------------

  it("rejects all pending when the server closes the socket", async () => {
    const closeServer = await startFakeServer((_msg, sock) => {
      // Close the connection without replying
      sock.destroy();
    });
    const closeClient = new HermesChannelClient(closeServer.socketPath);
    try {
      await expect(closeClient.sendPrompt("will be dropped", 5000)).rejects.toThrow();
    } finally {
      closeClient.close();
      await closeServer.stop();
    }
  });
});
