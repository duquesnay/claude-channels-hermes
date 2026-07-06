/**
 * Unit tests for acp_server.ts.
 *
 * Uses a mock ChannelsSessionPool — no live socket, no claude, no network cost.
 * Tests the ACP Agent's behavior including:
 *   - session_key extraction from _meta
 *   - generation token presence in newSession and prompt responses
 *   - pool routing and refusal when pool is full
 *   - error handling (unknown session, client failure)
 *   - progressive streaming: onChunk forwarding + tail reconciliation
 */

import { describe, it, expect, mock } from "bun:test";
import { randomUUID } from "node:crypto";
import { createServer, type Server } from "node:net";
import { unlinkSync, existsSync } from "node:fs";
import { createAgent } from "./acp_server.ts";
import { ChannelsSessionPool, PoolFullError } from "./session_pool.ts";
import type { SessionState, PoolDeps } from "./session_pool.ts";
import type { AgentSideConnection } from "@agentclientprotocol/sdk";

// ---------------------------------------------------------------------------
// Mock helpers
// ---------------------------------------------------------------------------

interface CapturedUpdate {
  sessionId: string;
  update: Record<string, unknown>;
}

function makeMockConnection(): {
  conn: AgentSideConnection;
  updates: CapturedUpdate[];
} {
  const updates: CapturedUpdate[] = [];

  const conn = {
    sessionUpdate: mock(async (params: { sessionId: string; update: Record<string, unknown> }) => {
      updates.push(params as CapturedUpdate);
    }),
    requestPermission: mock(async () => ({ outcome: { outcome: "cancelled" } })),
    signal: new AbortController().signal,
    closed: Promise.resolve(),
  } as unknown as AgentSideConnection;

  return { conn, updates };
}

function makeMockState(overrides: Partial<SessionState> = {}): SessionState {
  return {
    socketPath: "/tmp/fake.sock",
    expectPid: 99000,
    client: {
      sendPrompt: async (_content: string, _timeoutMs: number, _onChunk?: (d: string) => void) => "mock response",
      close: () => {},
      get pendingCount() { return 0; },
    },
    generation: randomUUID(),
    createdAt: Date.now(),
    lastActivityAt: Date.now(),
    sessionName: "claude_hermes_deadbeef",
    ...overrides,
  };
}

/** Build a minimal mock of ChannelsSessionPool. */
function makeMockPool(opts: {
  state?: Partial<SessionState>;
  throwOnGetOrCreate?: Error;
} = {}): ChannelsSessionPool {
  const state = makeMockState(opts.state ?? {});

  const pool = {
    registerAcpSession: mock((_acpId: string, _key: string) => {}),
    unregisterAcpSession: mock((_acpId: string) => {}),
    sessionKeyForAcp: mock((_acpId: string) => "test-session-key"),
    getOrCreate: mock(async (_key: string) => {
      if (opts.throwOnGetOrCreate) throw opts.throwOnGetOrCreate;
      return state;
    }),
    release: mock((_key: string) => {}),
    shutdown: mock(async () => {}),
    startIdleEviction: mock(() => {}),
    size: 0,
  } as unknown as ChannelsSessionPool;

  return pool;
}

// ---------------------------------------------------------------------------
// Real-pool helpers (JA-38 bug b) — makeMockPool()'s sessionKeyForAcp is an
// independent stub with no real state, so it CANNOT reproduce a bug that
// depends on the interaction between unregisterAcpSession and
// sessionKeyForAcp (both read/write the pool's own acpToKey Map). These
// helpers wire a REAL ChannelsSessionPool with fake spawn/kill deps (no real
// claude process, no real socket-holding subprocess) so that interaction is
// exercised for real.
// ---------------------------------------------------------------------------

function startFakeSocket(socketPath: string): Promise<Server> {
  return new Promise((resolve, reject) => {
    const server = createServer(() => {});
    server.on("error", reject);
    server.listen(socketPath, () => resolve(server));
  });
}

function stopFakeSocket(server: Server, socketPath: string): Promise<void> {
  return new Promise((resolve) => {
    server.close(() => {
      if (existsSync(socketPath)) {
        try { unlinkSync(socketPath); } catch { /* ignore */ }
      }
      resolve();
    });
  });
}

function makeRealPool(): { pool: ChannelsSessionPool; cleanup: () => Promise<void> } {
  const fakeServers = new Map<string, Server>();

  const deps: Partial<PoolDeps> = {
    spawnLauncher(_launcherExpPath, env) {
      const socketPath = env["HERMES_CHANNEL_SOCKET"]!;
      void startFakeSocket(socketPath).then((srv) => fakeServers.set(socketPath, srv));
      return 91000 + fakeServers.size;
    },
    createClient(_socketPath) {
      return {
        sendPrompt: async (_c: string, _t: number, _onChunk?: (d: string) => void) => "ok",
        close: () => {},
        get pendingCount() { return 0; },
      };
    },
    killPid(_pid, _signal) {},
    killSocketHolders(_socketPath) {},
    now: () => Date.now(),
    isPidAlive: (_pid) => true,
    killByName(_name) {},
  };

  const pool = new ChannelsSessionPool("/fake/launcher.exp", deps);

  return {
    pool,
    cleanup: async () => {
      for (const [socketPath, server] of fakeServers) {
        await stopFakeSocket(server, socketPath);
      }
    },
  };
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe("createAgent", () => {
  // --------------------------------------------------------------------------
  // initialize
  // --------------------------------------------------------------------------

  it("initialize returns protocolVersion 1", async () => {
    const { conn } = makeMockConnection();
    const pool = makeMockPool();
    const agent = createAgent(conn, pool);

    const result = await agent.initialize({} as any);
    expect(result.protocolVersion).toBe(1);
  });

  it("initialize returns agentCapabilities.loadSession false", async () => {
    const { conn } = makeMockConnection();
    const pool = makeMockPool();
    const agent = createAgent(conn, pool);

    const result = await agent.initialize({} as any);
    expect(result.agentCapabilities?.loadSession).toBe(false);
  });

  // --------------------------------------------------------------------------
  // newSession
  // --------------------------------------------------------------------------

  it("newSession returns a non-empty UUID sessionId", async () => {
    const { conn } = makeMockConnection();
    const pool = makeMockPool();
    const agent = createAgent(conn, pool);

    const result = await agent.newSession({} as any);
    expect(typeof result.sessionId).toBe("string");
    expect(result.sessionId.length).toBeGreaterThan(0);
  });

  it("newSession returns distinct IDs for each call", async () => {
    const { conn } = makeMockConnection();
    const pool = makeMockPool();
    const agent = createAgent(conn, pool);

    const r1 = await agent.newSession({} as any);
    const r2 = await agent.newSession({} as any);
    expect(r1.sessionId).not.toBe(r2.sessionId);
  });

  it("newSession includes _meta.session_generation", async () => {
    const { conn } = makeMockConnection();
    const gen = randomUUID();
    const pool = makeMockPool({ state: { generation: gen } });
    const agent = createAgent(conn, pool);

    const result = (await agent.newSession({} as any)) as any;
    expect(result._meta?.session_generation).toBe(gen);
  });

  it("newSession extracts session_key from _meta[hermes.channels/session_key]", async () => {
    const { conn } = makeMockConnection();
    const pool = makeMockPool();
    const agent = createAgent(conn, pool);

    const sessionKey = "janet-user-123";
    await agent.newSession({ _meta: { "hermes.channels/session_key": sessionKey } } as any);

    // pool.registerAcpSession should have been called with the extracted key
    const registerMock = pool.registerAcpSession as ReturnType<typeof mock>;
    expect(registerMock.mock.calls[0]?.[1]).toBe(sessionKey);
  });

  it("newSession falls back to UUID when _meta is absent", async () => {
    const { conn } = makeMockConnection();
    const pool = makeMockPool();
    const agent = createAgent(conn, pool);

    await agent.newSession({} as any);
    // registerAcpSession called; the key should be a UUID (not undefined)
    const registerMock = pool.registerAcpSession as ReturnType<typeof mock>;
    const key = registerMock.mock.calls[0]?.[1] as string;
    expect(typeof key).toBe("string");
    expect(key.length).toBeGreaterThan(0);
  });

  // --------------------------------------------------------------------------
  // prompt
  // --------------------------------------------------------------------------

  it("prompt returns stopReason end_turn on success", async () => {
    const { conn } = makeMockConnection();
    const pool = makeMockPool();
    const agent = createAgent(conn, pool);

    const { sessionId } = await agent.newSession({} as any);
    const result = await agent.prompt({
      sessionId,
      prompt: [{ type: "text", text: "hi" }],
    } as any);

    expect(result.stopReason).toBe("end_turn");
  });

  it("prompt includes _meta.session_generation on every turn", async () => {
    const { conn } = makeMockConnection();
    const gen = randomUUID();
    const pool = makeMockPool({ state: { generation: gen } });
    const agent = createAgent(conn, pool);

    const { sessionId } = await agent.newSession({} as any);
    const result = (await agent.prompt({
      sessionId,
      prompt: [{ type: "text", text: "hi" }],
    } as any)) as any;

    expect(result._meta?.session_generation).toBe(gen);
  });

  it("prompt emits at least one sessionUpdate with the response text", async () => {
    const { conn, updates } = makeMockConnection();
    const pool = makeMockPool({
      state: {
        client: {
          sendPrompt: async (_c: string, _t: number, _onChunk?: (d: string) => void) => "Hello from hermes",
          close: () => {},
          get pendingCount() { return 0; },
        },
      },
    });
    const agent = createAgent(conn, pool);

    const { sessionId } = await agent.newSession({} as any);
    await agent.prompt({
      sessionId,
      prompt: [{ type: "text", text: "hello" }],
    } as any);

    const chunkUpdates = updates.filter(
      (u) =>
        u.sessionId === sessionId &&
        (u.update as any).sessionUpdate === "agent_message_chunk"
    );
    expect(chunkUpdates.length).toBeGreaterThanOrEqual(1);
  });

  it("prompt extracts and concatenates text from ContentBlock array", async () => {
    const { conn } = makeMockConnection();
    let capturedText = "";
    const pool = makeMockPool({
      state: {
        client: {
          sendPrompt: async (content: string, _t: number, _onChunk?: (d: string) => void) => {
            capturedText = content;
            return "ok";
          },
          close: () => {},
          get pendingCount() { return 0; },
        },
      },
    });
    const agent = createAgent(conn, pool);

    const { sessionId } = await agent.newSession({} as any);
    await agent.prompt({
      sessionId,
      prompt: [
        { type: "text", text: "Hello " },
        { type: "text", text: "world" },
      ],
    } as any);

    expect(capturedText).toBe("Hello world");
  });

  it("prompt returns stopReason refusal when client throws", async () => {
    const { conn } = makeMockConnection();
    const pool = makeMockPool({
      state: {
        client: {
          sendPrompt: async () => { throw new Error("socket error"); },
          close: () => {},
          get pendingCount() { return 0; },
        },
      },
    });
    const agent = createAgent(conn, pool);

    const { sessionId } = await agent.newSession({} as any);
    const result = await agent.prompt({
      sessionId,
      prompt: [{ type: "text", text: "hi" }],
    } as any);

    expect(result.stopReason).toBe("refusal");
  });

  it("prompt returns stopReason refusal when pool is full", async () => {
    const { conn } = makeMockConnection();
    const pool = makeMockPool({ throwOnGetOrCreate: new PoolFullError("full") });
    const agent = createAgent(conn, pool);

    const { sessionId } = await agent.newSession({} as any);
    // Manually fix the session key mapping (the refused-path in newSession handles this)
    // For this test, we simulate prompt on a session whose pool slot is full
    const poolSessionKeyMock = pool.sessionKeyForAcp as ReturnType<typeof mock>;
    poolSessionKeyMock.mockImplementation((_acpId: string) => "test-key");

    const result = await agent.prompt({
      sessionId,
      prompt: [{ type: "text", text: "hi" }],
    } as any);

    expect(result.stopReason).toBe("refusal");
  });

  it("prompt throws when ACP session is unknown", async () => {
    const { conn } = makeMockConnection();
    const pool = makeMockPool();
    (pool.sessionKeyForAcp as ReturnType<typeof mock>).mockImplementation(() => undefined);
    const agent = createAgent(conn, pool);

    await expect(
      agent.prompt({ sessionId: "unknown-session", prompt: [{ type: "text", text: "hi" }] } as any)
    ).rejects.toThrow("not found");
  });

  // --------------------------------------------------------------------------
  // Progressive streaming (V2)
  // --------------------------------------------------------------------------

  it("prompt forwards onChunk deltas as individual agent_message_chunk updates", async () => {
    const { conn, updates } = makeMockConnection();
    const pool = makeMockPool({
      state: {
        client: {
          sendPrompt: async (
            _content: string,
            _timeoutMs: number,
            onChunk?: (delta: string) => void
          ) => {
            // Simulate the plugin calling reply_chunk multiple times with deltas
            if (onChunk) {
              await onChunk("Hello");
              await onChunk(", ");
              await onChunk("world");
            }
            return "Hello, world";
          },
          close: () => {},
          get pendingCount() { return 0; },
        },
      },
    });
    const agent = createAgent(conn, pool);

    const { sessionId } = await agent.newSession({} as any);
    await agent.prompt({
      sessionId,
      prompt: [{ type: "text", text: "stream" }],
    } as any);

    // When chunks fully cover the final text, expect exactly 3 sessionUpdate calls
    // (the 3 delta chunks — no tail emitted because "Hello, world" is fully covered)
    const chunkUpdates = updates.filter(
      (u) =>
        u.sessionId === sessionId &&
        (u.update as any).sessionUpdate === "agent_message_chunk"
    );
    expect(chunkUpdates.length).toBe(3);
    expect((chunkUpdates[0]!.update as any).content.text).toBe("Hello");
    expect((chunkUpdates[1]!.update as any).content.text).toBe(", ");
    expect((chunkUpdates[2]!.update as any).content.text).toBe("world");
  });

  it("prompt emits tail when final text is longer than streamed chunks", async () => {
    const { conn, updates } = makeMockConnection();
    const pool = makeMockPool({
      state: {
        client: {
          sendPrompt: async (
            _content: string,
            _timeoutMs: number,
            onChunk?: (delta: string) => void
          ) => {
            // Only partial chunks streamed (first 5 chars of "Hello world, the rest")
            if (onChunk) {
              await onChunk("Hello");
            }
            return "Hello world, the rest";
          },
          close: () => {},
          get pendingCount() { return 0; },
        },
      },
    });
    const agent = createAgent(conn, pool);

    const { sessionId } = await agent.newSession({} as any);
    await agent.prompt({
      sessionId,
      prompt: [{ type: "text", text: "partial stream" }],
    } as any);

    const chunkUpdates = updates.filter(
      (u) =>
        u.sessionId === sessionId &&
        (u.update as any).sessionUpdate === "agent_message_chunk"
    );
    // Should have 2 updates: the chunk delta + the tail
    expect(chunkUpdates.length).toBe(2);
    expect((chunkUpdates[0]!.update as any).content.text).toBe("Hello");
    expect((chunkUpdates[1]!.update as any).content.text).toBe(" world, the rest");
  });

  it("prompt emits full text as one chunk when no onChunk was called (no-stream fallback)", async () => {
    const { conn, updates } = makeMockConnection();
    const pool = makeMockPool({
      state: {
        client: {
          sendPrompt: async (
            _content: string,
            _timeoutMs: number,
            _onChunk?: (delta: string) => void
          ) => {
            // No chunks emitted — Claude only called reply_open + reply_close
            return "Full reply in one shot";
          },
          close: () => {},
          get pendingCount() { return 0; },
        },
      },
    });
    const agent = createAgent(conn, pool);

    const { sessionId } = await agent.newSession({} as any);
    await agent.prompt({
      sessionId,
      prompt: [{ type: "text", text: "single shot" }],
    } as any);

    const chunkUpdates = updates.filter(
      (u) =>
        u.sessionId === sessionId &&
        (u.update as any).sessionUpdate === "agent_message_chunk"
    );
    // Exactly one update with the full content — same as pre-streaming behavior
    expect(chunkUpdates.length).toBe(1);
    expect((chunkUpdates[0]!.update as any).content.text).toBe("Full reply in one shot");
  });

  // --------------------------------------------------------------------------
  // cancel / closeSession
  // --------------------------------------------------------------------------

  it("cancel does not throw for a known session", async () => {
    const { conn } = makeMockConnection();
    const pool = makeMockPool();
    const agent = createAgent(conn, pool);

    const { sessionId } = await agent.newSession({} as any);
    await expect(agent.cancel({ sessionId } as any)).resolves.toBeUndefined();
  });

  it("closeSession calls pool.unregisterAcpSession", async () => {
    const { conn } = makeMockConnection();
    const pool = makeMockPool();
    const agent = createAgent(conn, pool);

    const { sessionId } = await agent.newSession({} as any);
    await agent.closeSession?.({ sessionId } as any);

    const unregisterMock = pool.unregisterAcpSession as ReturnType<typeof mock>;
    expect(unregisterMock.mock.calls.some((c) => c[0] === sessionId)).toBe(true);
  });

  // --------------------------------------------------------------------------
  // JA-38 (b): a session/cancel notification must NOT break the NEXT prompt
  // on the SAME (still-open) ACP session.
  //
  // Per the ACP SDK docs, closeSession "must cancel any ongoing work (as if
  // session/cancel was called) AND THEN free up any resources" — i.e. cancel
  // is a SUBSET of closeSession's behavior (stop current work), not a
  // superset. The current cancel() calls pool.unregisterAcpSession(), which
  // is the resource-freeing step that belongs to closeSession — it tears
  // down the ACP-session-id -> session_key mapping that routing depends on.
  // A real client (e.g. Janet) sends session/cancel to interrupt one turn
  // and then keeps sending prompts on the SAME sessionId — exactly the
  // reported symptom: the turn AFTER a cancel gets a hard error (observed as
  // "-32603 Internal Error" + empty output at the JSON-RPC layer), even
  // though the session was never closed.
  //
  // Uses a REAL ChannelsSessionPool (not makeMockPool()) because the bug is
  // in the interaction between unregisterAcpSession and sessionKeyForAcp —
  // makeMockPool()'s sessionKeyForAcp is an independent stub that always
  // returns a fixed string regardless of what unregisterAcpSession was
  // called with, so it cannot reproduce this.
  // --------------------------------------------------------------------------

  it("prompt on the same session succeeds after a cancel notification (JA-38 bug b)", async () => {
    const { conn } = makeMockConnection();
    const { pool, cleanup } = makeRealPool();
    const agent = createAgent(conn, pool);

    const { sessionId } = await agent.newSession({
      _meta: { "hermes.channels/session_key": "ja38-cancel-key" },
    } as any);

    // Simulate: a turn starts, the client sends session/cancel while (or
    // shortly after) it's in flight — the agent underneath may ignore it
    // and finish normally (JA-29's documented fallback), but the session
    // itself must remain valid for the NEXT prompt.
    await agent.cancel({ sessionId } as any);

    const result = await agent.prompt({
      sessionId,
      prompt: [{ type: "text", text: "next turn after cancel" }],
    } as any);

    expect(result.stopReason).toBe("end_turn");

    await cleanup();
  });

  // --------------------------------------------------------------------------
  // authenticate
  // --------------------------------------------------------------------------

  it("authenticate returns without error", async () => {
    const { conn } = makeMockConnection();
    const pool = makeMockPool();
    const agent = createAgent(conn, pool);
    const result = await agent.authenticate({} as any);
    expect(result).toBeDefined();
  });
});
