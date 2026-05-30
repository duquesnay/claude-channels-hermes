/**
 * Unit tests for acp_server.ts.
 *
 * Uses a mock HermesChannelClient and a spy connection — no live socket,
 * no claude, no network cost.
 */

import { describe, it, expect, mock } from "bun:test";
import { createAgent } from "./acp_server.ts";
import type { HermesChannelClientInterface } from "./hermes_channel_client.ts";
import type { AgentSideConnection } from "@agentclientprotocol/sdk";

// ---------------------------------------------------------------------------
// Mock AgentSideConnection
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

// ---------------------------------------------------------------------------
// Mock HermesChannelClient
// ---------------------------------------------------------------------------

function makeMockClient(
  sendPromptImpl?: (content: string, timeoutMs: number) => Promise<string>
): HermesChannelClientInterface {
  return {
    sendPrompt: sendPromptImpl ?? (async (_content, _timeoutMs) => "mock response"),
    close: () => {},
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
    const client = makeMockClient();
    const agent = createAgent(conn, { client });

    const result = await agent.initialize({} as any);
    expect(result.protocolVersion).toBe(1);
  });

  it("initialize returns agentCapabilities.loadSession false", async () => {
    const { conn } = makeMockConnection();
    const client = makeMockClient();
    const agent = createAgent(conn, { client });

    const result = await agent.initialize({} as any);
    expect(result.agentCapabilities?.loadSession).toBe(false);
  });

  // --------------------------------------------------------------------------
  // newSession
  // --------------------------------------------------------------------------

  it("newSession returns a non-empty UUID sessionId", async () => {
    const { conn } = makeMockConnection();
    const agent = createAgent(conn, { client: makeMockClient() });

    const result = await agent.newSession({} as any);
    expect(typeof result.sessionId).toBe("string");
    expect(result.sessionId.length).toBeGreaterThan(0);
  });

  it("newSession returns distinct IDs for each call", async () => {
    const { conn } = makeMockConnection();
    const agent = createAgent(conn, { client: makeMockClient() });

    const r1 = await agent.newSession({} as any);
    const r2 = await agent.newSession({} as any);
    expect(r1.sessionId).not.toBe(r2.sessionId);
  });

  // --------------------------------------------------------------------------
  // prompt
  // --------------------------------------------------------------------------

  it("prompt sends at least one sessionUpdate and returns stopReason end_turn", async () => {
    const { conn, updates } = makeMockConnection();
    const client = makeMockClient(async () => "Hello from hermes");
    const agent = createAgent(conn, { client });

    const { sessionId } = await agent.newSession({} as any);
    const result = await agent.prompt({
      sessionId,
      prompt: [{ type: "text", text: "hi" }],
    } as any);

    expect(result.stopReason).toBe("end_turn");
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
    const client = makeMockClient(async (content) => {
      capturedText = content;
      return "ok";
    });
    const agent = createAgent(conn, { client });

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

  it("prompt returns stopReason refusal when client rejects", async () => {
    const { conn } = makeMockConnection();
    const client = makeMockClient(async () => {
      throw new Error("socket error");
    });
    const agent = createAgent(conn, { client });

    const { sessionId } = await agent.newSession({} as any);
    const result = await agent.prompt({
      sessionId,
      prompt: [{ type: "text", text: "hi" }],
    } as any);

    expect(result.stopReason).toBe("refusal");
  });

  it("prompt throws when session is unknown", async () => {
    const { conn } = makeMockConnection();
    const agent = createAgent(conn, { client: makeMockClient() });

    await expect(
      agent.prompt({ sessionId: "bad-session", prompt: [{ type: "text", text: "hi" }] } as any)
    ).rejects.toThrow("not found");
  });

  // --------------------------------------------------------------------------
  // cancel — no-op on the shared client
  // --------------------------------------------------------------------------

  it("cancel does not throw for a known session", async () => {
    const { conn } = makeMockConnection();
    const agent = createAgent(conn, { client: makeMockClient() });

    const { sessionId } = await agent.newSession({} as any);
    await expect(agent.cancel({ sessionId } as any)).resolves.toBeUndefined();
  });

  // --------------------------------------------------------------------------
  // authenticate
  // --------------------------------------------------------------------------

  it("authenticate returns without error", async () => {
    const { conn } = makeMockConnection();
    const agent = createAgent(conn, { client: makeMockClient() });
    const result = await agent.authenticate({} as any);
    expect(result).toBeDefined();
  });

  // --------------------------------------------------------------------------
  // closeSession
  // --------------------------------------------------------------------------

  it("closeSession removes the session", async () => {
    const { conn } = makeMockConnection();
    const agent = createAgent(conn, { client: makeMockClient() });

    const { sessionId } = await agent.newSession({} as any);
    await agent.closeSession?.({ sessionId } as any);

    // Prompt on removed session should fail
    await expect(
      agent.prompt({ sessionId, prompt: [{ type: "text", text: "hi" }] } as any)
    ).rejects.toThrow("not found");
  });
});
