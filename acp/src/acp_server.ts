/**
 * acp_server.ts — ACP Agent implementation that bridges to a hermes-channel session.
 *
 * Implements the Agent interface from @agentclientprotocol/sdk for use with
 * AgentSideConnection. A single shared HermesChannelClient is used for all
 * sessions — the live claude --channels session maintains its own transcript.
 * Janet inlines full conversation context per turn, so no per-session subprocess
 * is needed on the backend.
 *
 * Fix C (SDK 0.22.1): `authenticate` is REQUIRED (not optional) on Agent.
 * closeSession is optional in the type but implemented for session hygiene.
 */

import { PROTOCOL_VERSION } from "@agentclientprotocol/sdk";
import type {
  Agent,
  AgentSideConnection,
  InitializeResponse,
  NewSessionResponse,
  PromptResponse,
  AuthenticateResponse,
} from "@agentclientprotocol/sdk";
import type {
  PromptRequest,
  NewSessionRequest,
  InitializeRequest,
  AuthenticateRequest,
  CancelNotification,
  CloseSessionRequest,
} from "@agentclientprotocol/sdk";
import type { HermesChannelClientInterface } from "./hermes_channel_client.ts";

/** Default per-turn timeout in milliseconds. */
const DEFAULT_TURN_TIMEOUT_MS = 120_000;

// ---------------------------------------------------------------------------
// Injectable client interface (for unit testing)
// ---------------------------------------------------------------------------

export interface AgentDeps {
  /** Shared client to the hermes-channel socket. Injected for tests. */
  client: HermesChannelClientInterface;
}

// ---------------------------------------------------------------------------
// ClaudeAgent
// ---------------------------------------------------------------------------

class ClaudeAgent implements Agent {
  private readonly connection: AgentSideConnection;
  private readonly client: HermesChannelClientInterface;
  private readonly sessions = new Set<string>();

  constructor(connection: AgentSideConnection, deps: AgentDeps) {
    this.connection = connection;
    this.client = deps.client;
  }

  // --------------------------------------------------------------------------
  // initialize — negotiate protocol version and capabilities
  // --------------------------------------------------------------------------

  async initialize(_params: InitializeRequest): Promise<InitializeResponse> {
    return {
      protocolVersion: PROTOCOL_VERSION,
      agentCapabilities: {
        loadSession: false,
      },
    };
  }

  // --------------------------------------------------------------------------
  // authenticate — required by SDK 0.22.1 (not optional)
  // --------------------------------------------------------------------------

  async authenticate(_params: AuthenticateRequest): Promise<AuthenticateResponse> {
    // No authentication required — return empty response per spec example.
    return {} as AuthenticateResponse;
  }

  // --------------------------------------------------------------------------
  // newSession — register a UUID session (no per-session backend resource)
  // --------------------------------------------------------------------------

  async newSession(_params: NewSessionRequest): Promise<NewSessionResponse> {
    const sessionId = generateUuid();
    this.sessions.add(sessionId);
    return { sessionId };
  }

  // --------------------------------------------------------------------------
  // prompt — forward to hermes-channel, emit result as agent_message_chunk
  // --------------------------------------------------------------------------

  async prompt(params: PromptRequest): Promise<PromptResponse> {
    const { sessionId, prompt: contentBlocks } = params;
    if (!this.sessions.has(sessionId)) {
      throw new Error(`Session ${sessionId} not found`);
    }

    // Extract text from ContentBlock array
    const promptText = contentBlocks
      .filter((b) => b.type === "text")
      .map((b) => (b as { type: "text"; text: string }).text)
      .join("");

    let content: string;
    try {
      content = await this.client.sendPrompt(promptText, DEFAULT_TURN_TIMEOUT_MS);
    } catch (err) {
      process.stderr.write(
        `hermes-channel-client: prompt failed for session ${sessionId}: ${err}\n`
      );
      return { stopReason: "refusal" };
    }

    // Emit accumulated text as a single agent_message_chunk
    if (content) {
      await this.connection.sessionUpdate({
        sessionId,
        update: {
          sessionUpdate: "agent_message_chunk",
          content: {
            type: "text",
            text: content,
          },
        },
      });
    }

    return { stopReason: "end_turn" };
  }

  // --------------------------------------------------------------------------
  // cancel — no per-session subprocess to kill on the hermes-channel backend
  // --------------------------------------------------------------------------

  async cancel(params: CancelNotification): Promise<void> {
    // The shared client is multiplexed; cancelling one session cannot abort
    // other in-flight requests. Just drop the session record.
    this.sessions.delete(params.sessionId);
  }

  // --------------------------------------------------------------------------
  // closeSession — remove the session record (no backend resource to release)
  // --------------------------------------------------------------------------

  async closeSession(params: CloseSessionRequest): Promise<void> {
    this.sessions.delete(params.sessionId);
  }
}

// ---------------------------------------------------------------------------
// Public factory
// ---------------------------------------------------------------------------

/**
 * Create an Agent instance for the given AgentSideConnection.
 *
 * deps.client is the shared HermesChannelClient — inject a mock in tests.
 *
 * @param conn - The AgentSideConnection from the SDK.
 * @param deps - Injectable dependencies (required: client).
 */
export function createAgent(conn: AgentSideConnection, deps: AgentDeps): Agent {
  return new ClaudeAgent(conn, deps);
}

// ---------------------------------------------------------------------------
// UUID generation (crypto API — no external dependency)
// ---------------------------------------------------------------------------

function generateUuid(): string {
  return Array.from(crypto.getRandomValues(new Uint8Array(16)))
    .map((b, i) => {
      if (i === 6) b = (b & 0x0f) | 0x40;
      if (i === 8) b = (b & 0x3f) | 0x80;
      return b.toString(16).padStart(2, "0");
    })
    .join("")
    .replace(/^(.{8})(.{4})(.{4})(.{4})(.{12})$/, "$1-$2-$3-$4-$5");
}
