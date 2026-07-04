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

  // --------------------------------------------------------------------------
  // Progressive streaming (V2) — chunk messages
  // --------------------------------------------------------------------------

  it("chunk messages invoke onChunk callback without resolving the promise early", async () => {
    const chunkServer = await startFakeServer((msg, sock) => {
      const requestId = msg["request_id"] as string;
      // Send two chunks then the final result
      sock.write(JSON.stringify({ type: "chunk", request_id: requestId, content: "Hello" }) + "\n");
      sock.write(JSON.stringify({ type: "chunk", request_id: requestId, content: " world" }) + "\n");
      sock.write(JSON.stringify({ type: "result", request_id: requestId, content: "Hello world", duration_ms: 10 }) + "\n");
    });
    const chunkClient = new HermesChannelClient(chunkServer.socketPath);
    const deltas: string[] = [];
    try {
      const result = await chunkClient.sendPrompt("stream me", 5000, (delta) => {
        deltas.push(delta);
      });
      expect(result).toBe("Hello world");
      expect(deltas).toEqual(["Hello", " world"]);
    } finally {
      chunkClient.close();
      await chunkServer.stop();
    }
  });

  it("chunk messages with no onChunk callback are silently ignored", async () => {
    const chunkServer = await startFakeServer((msg, sock) => {
      const requestId = msg["request_id"] as string;
      sock.write(JSON.stringify({ type: "chunk", request_id: requestId, content: "partial" }) + "\n");
      sock.write(JSON.stringify({ type: "result", request_id: requestId, content: "partial result", duration_ms: 5 }) + "\n");
    });
    const chunkClient = new HermesChannelClient(chunkServer.socketPath);
    try {
      // No onChunk provided — must not throw
      const result = await chunkClient.sendPrompt("no callback", 5000);
      expect(result).toBe("partial result");
    } finally {
      chunkClient.close();
      await chunkServer.stop();
    }
  });

  it("multiple chunk messages accumulate in order before result resolves", async () => {
    const chunkServer = await startFakeServer((msg, sock) => {
      const requestId = msg["request_id"] as string;
      sock.write(JSON.stringify({ type: "chunk", request_id: requestId, content: "A" }) + "\n");
      sock.write(JSON.stringify({ type: "chunk", request_id: requestId, content: "B" }) + "\n");
      sock.write(JSON.stringify({ type: "chunk", request_id: requestId, content: "C" }) + "\n");
      sock.write(JSON.stringify({ type: "result", request_id: requestId, content: "ABC", duration_ms: 1 }) + "\n");
    });
    const chunkClient = new HermesChannelClient(chunkServer.socketPath);
    const received: string[] = [];
    try {
      const result = await chunkClient.sendPrompt("multi-chunk", 5000, (d) => received.push(d));
      expect(received).toEqual(["A", "B", "C"]);
      expect(result).toBe("ABC");
      // pendingCount is 0 after resolution
      expect(chunkClient.pendingCount).toBe(0);
    } finally {
      chunkClient.close();
      await chunkServer.stop();
    }
  });
});

// ---------------------------------------------------------------------------
// Send serialization: two rapid sendPrompt calls must not race
// ---------------------------------------------------------------------------

describe("sendPrompt send serialization", () => {
  it("two rapid sends both register and resolve correctly without racing", async () => {
    // The fake server holds request 1 (no reply immediately) and replies to
    // request 2 first, then request 1. Both promises must resolve with their
    // own correct content regardless of reply order — proving that the send
    // chain does NOT serialize the full turn (only the write step).
    const replies = new Map<string, () => void>();

    const raceServer = await startFakeServer((msg, sock) => {
      const requestId = msg["request_id"] as string;
      const content = msg["content"] as string;
      // Store a trigger for each request so the test can control reply order.
      replies.set(requestId, () => {
        sock.write(
          JSON.stringify({ type: "result", request_id: requestId, content: `echo: ${content}`, duration_ms: 0 }) + "\n"
        );
      });
    });

    const raceClient = new HermesChannelClient(raceServer.socketPath);

    try {
      // Fire both sends without awaiting — they should both be registered.
      const p1 = raceClient.sendPrompt("first", 5000);
      const p2 = raceClient.sendPrompt("second", 5000);

      // Wait briefly for both writes to have been dispatched to the server.
      await new Promise((res) => setTimeout(res, 50));

      // Both requests should now be registered on the server side.
      expect(replies.size).toBe(2);

      // Reply to request 2 first, then request 1.
      const [id1, trigger1] = [...replies.entries()][0]!;
      const [, trigger2] = [...replies.entries()][1]!;
      void id1; // used implicitly via ordering assertion

      trigger2(); // reply to second first
      const result2 = await p2;
      expect(result2).toBe("echo: second");

      trigger1(); // now reply to first
      const result1 = await p1;
      expect(result1).toBe("echo: first");
    } finally {
      raceClient.close();
      await raceServer.stop();
    }
  });
});
