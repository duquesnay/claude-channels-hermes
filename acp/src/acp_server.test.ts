/**
 * Unit tests for acp_server.ts.
 *
 * Uses a mock ChannelsSessionPool — no live socket, no claude, no network cost.
 * Tests the ACP Agent's behavior including:
 *   - session_key extraction from _meta
 *   - generation token presence in newSession and prompt responses
 *   - pool routing and refusal when pool is full
 *   - error handling (unknown session, client failure)
 */

import { describe, it, expect, mock } from "bun:test";
import { randomUUID } from "node:crypto";
import { createAgent } from "./acp_server.ts";
import { ChannelsSessionPool, PoolFullError } from "./session_pool.ts";
import type { SessionState } from "./session_pool.ts";
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
      sendPrompt: async (_content: string, _timeoutMs: number) => "mock response",
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
          sendPrompt: async () => "Hello from hermes",
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
          sendPrompt: async (content: string) => {
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
