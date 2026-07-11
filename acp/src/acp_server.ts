/**
 * acp_server.ts — ACP Agent implementation backed by a ChannelsSessionPool.
 *
 * Each ACP session maps to a `session_key` supplied by Janet in
 * `_meta["hermes.channels/session_key"]`. The pool owns one persistent
 * claude --channels process per session_key.
 *
 * Generation token (linchpin):
 *   - newSession → _meta.session_generation = state.generation (creation nonce)
 *   - prompt → _meta.session_generation = state.generation (current nonce, EVERY turn)
 *   The nonce changes on every pool respawn, letting Hermes detect stale
 *   in-session state and switch from DELTA to CATCH-UP mode.
 *
 * Progressive streaming (V2):
 *   prompt() passes an onChunk callback to sendPrompt that forwards each delta
 *   as an agent_message_chunk session/update immediately as it arrives.
 *   On the final "result", cumulative-length reconciliation is used to avoid
 *   double-delivery: if finalText is longer than what was already streamed as
 *   chunks, the tail is emitted as one more agent_message_chunk. If all content
 *   was already streamed (streamed.length >= finalText.length), no extra emit.
 *   This correctly handles: no chunks (full text emitted once as before),
 *   partial chunks (tail appended), and complete coverage (no duplicate).
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
import { ChannelsSessionPool, PoolFullError } from "./session_pool.ts";
import { randomUUID } from "node:crypto";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";

/** Default per-turn timeout in milliseconds. */
export const DEFAULT_TURN_TIMEOUT_MS = 120_000;

/**
 * Resolve the per-turn timeout from HERMES_CHANNELS_TURN_TIMEOUT_MS.
 *
 * The supervisor turn-timeout must sit BETWEEN the plugin reply_close guard
 * (HERMES_REPLY_CLOSE_GUARD_MS, below) and the gateway deadlines (above) —
 * see ../CONFIGURATION.md "Timeout stack". Invalid or non-positive values
 * fall back to the default so a broken env var can never disable the
 * supervisor timeout.
 */
export function resolveTurnTimeoutMs(
  env: Record<string, string | undefined> = process.env,
): number {
  const raw = env.HERMES_CHANNELS_TURN_TIMEOUT_MS;
  if (raw === undefined || raw.trim() === "") return DEFAULT_TURN_TIMEOUT_MS;
  const parsed = Number(raw);
  if (!Number.isInteger(parsed) || parsed <= 0) {
    process.stderr.write(
      `acp-server: invalid HERMES_CHANNELS_TURN_TIMEOUT_MS=${JSON.stringify(raw)}, using default ${DEFAULT_TURN_TIMEOUT_MS}\n`,
    );
    return DEFAULT_TURN_TIMEOUT_MS;
  }
  return parsed;
}

const TURN_TIMEOUT_MS = resolveTurnTimeoutMs();

/**
 * ACP _meta key used by Janet to supply the session key.
 * Python side: `_meta["hermes.channels/session_key"]`
 */
const SESSION_KEY_META = "hermes.channels/session_key";

// ---------------------------------------------------------------------------
// ClaudeAgent
// ---------------------------------------------------------------------------

class ClaudeAgent implements Agent {
  private readonly connection: AgentSideConnection;
  private readonly pool: ChannelsSessionPool;

  constructor(connection: AgentSideConnection, pool: ChannelsSessionPool) {
    this.connection = connection;
    this.pool = pool;
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
  // authenticate — required by SDK 0.22.1
  // --------------------------------------------------------------------------

  async authenticate(_params: AuthenticateRequest): Promise<AuthenticateResponse> {
    return {} as AuthenticateResponse;
  }

  // --------------------------------------------------------------------------
  // newSession — extract session_key, warm the pool slot, emit generation token
  // --------------------------------------------------------------------------

  async newSession(params: NewSessionRequest): Promise<NewSessionResponse> {
    const acpSessionId = randomUUID();

    // Extract session_key from Janet's _meta; fallback to a UUID for non-Janet clients.
    const meta = (params as Record<string, unknown>)["_meta"] as
      | Record<string, unknown>
      | undefined;
    const sessionKey =
      (meta?.[SESSION_KEY_META] as string | undefined) ?? randomUUID();

    // Register ACP ↔ session_key mapping before getOrCreate so prompt routing
    // works even if getOrCreate throws (mapping will be cleaned on closeSession).
    this.pool.registerAcpSession(acpSessionId, sessionKey);

    let generation: string;
    try {
      const state = await this.pool.getOrCreate(sessionKey);
      generation = state.generation;
    } catch (err) {
      this.pool.unregisterAcpSession(acpSessionId);
      if (err instanceof PoolFullError) {
        // Structured refusal — return a session but indicate stopReason refusal
        // on the next prompt. ACP doesn't have a newSession rejection,
        // so we return a session and let prompt return stopReason:"refusal".
        process.stderr.write(`acp-server: pool full, deferring refusal to prompt for ${acpSessionId}\n`);
        this.pool.registerAcpSession(acpSessionId, `__refused__${acpSessionId}`);
        return { sessionId: acpSessionId };
      }
      throw err;
    }

    process.stderr.write(
      `acp-server: newSession acp=${acpSessionId} key=${sessionKey} gen=${generation}\n`
    );

    return {
      sessionId: acpSessionId,
      // Generation token: consumed by Hermes to detect session respawns.
      // Python: prompt_result["_meta"]["session_generation"]
      _meta: { session_generation: generation },
    } as NewSessionResponse & { _meta: Record<string, unknown> };
  }

  // --------------------------------------------------------------------------
  // prompt — route to pool client, stream chunks, emit generation token
  // --------------------------------------------------------------------------

  async prompt(params: PromptRequest): Promise<PromptResponse> {
    const { sessionId, prompt: contentBlocks } = params;

    const sessionKey = this.pool.sessionKeyForAcp(sessionId);
    if (!sessionKey) {
      throw new Error(`acp-server: ACP session ${sessionId} not found`);
    }

    // Refused sessions (pool full at newSession time).
    if (sessionKey.startsWith("__refused__")) {
      return { stopReason: "refusal" };
    }

    // Re-fetch the state — may have respawned since newSession (generates new nonce).
    let state;
    try {
      state = await this.pool.getOrCreate(sessionKey);
    } catch (err) {
      if (err instanceof PoolFullError) {
        return { stopReason: "refusal" };
      }
      throw err;
    }

    // Extract text from ContentBlock array.
    const promptText = contentBlocks
      .filter((b) => b.type === "text")
      .map((b) => (b as { type: "text"; text: string }).text)
      .join("");

    // Accumulate streamed deltas for length-based tail reconciliation.
    // Each delta forwarded by onChunk is immediately emitted as agent_message_chunk.
    let streamedLength = 0;
    const onChunk = async (delta: string): Promise<void> => {
      streamedLength += delta.length;
      await this.connection.sessionUpdate({
        sessionId,
        update: {
          sessionUpdate: "agent_message_chunk",
          content: {
            type: "text",
            text: delta,
          },
        },
      });
    };

    let content: string;
    try {
      content = await state.client.sendPrompt(promptText, TURN_TIMEOUT_MS, onChunk);
    } catch (err) {
      process.stderr.write(
        `acp-server: prompt failed for session ${sessionId}: ${err}\n`
      );
      return { stopReason: "refusal" } as PromptResponse;
    } finally {
      this.pool.release(sessionKey);
    }

    // Cumulative-length reconciliation: emit the tail of finalText that was not
    // already covered by streamed chunks. This handles three cases:
    //   - no chunks streamed (streamedLength===0): emit full content as before
    //   - partial chunks: emit the remaining suffix
    //   - full coverage (streamedLength >= content.length): no duplicate emit
    // Assumption: the streamed deltas are a strict prefix of finalText (the plugin
    // passes full accumulated text to reply_close, so reply_close content always
    // starts with everything reply_chunk already streamed). If the model rephrases
    // rather than extends, this assumption breaks — but that would be a plugin bug.
    if (content && content.length > streamedLength) {
      const tail = content.slice(streamedLength);
      await this.connection.sessionUpdate({
        sessionId,
        update: {
          sessionUpdate: "agent_message_chunk",
          content: {
            type: "text",
            text: tail,
          },
        },
      });
    }

    // Generation token returned on EVERY turn — Hermes reads this to detect
    // respawns between turns. state.generation is the current nonce.
    return {
      stopReason: "end_turn",
      _meta: { session_generation: state.generation },
    } as PromptResponse & { _meta: Record<string, unknown> };
  }

  // --------------------------------------------------------------------------
  // cancel — stop the CURRENT turn; the session itself stays open (JA-38 b)
  // --------------------------------------------------------------------------

  /**
   * JA-38 (b): session/cancel must NOT tear down the ACP-session -> session_key
   * mapping. Per the ACP SDK docs, closeSession "must cancel any ongoing work
   * (as if session/cancel was called) AND THEN free up any resources" — i.e.
   * cancel is a SUBSET of closeSession's behavior (stop current work), not a
   * superset. This previously called pool.unregisterAcpSession() (copied from
   * closeSession's body since the very first commit, before the pool-based
   * session_key routing existed) — that removed the mapping prompt() depends
   * on, so the NEXT prompt on the same still-open session threw "ACP session
   * ... not found" (observed at the JSON-RPC layer as -32603 Internal Error
   * with empty output), even though the client never closed the session.
   *
   * This is currently a no-op: there is no mechanism yet to interrupt the
   * underlying claude subprocess mid-turn (see JA-38 (a) — investigated
   * separately; a running turn keeps going and completes normally, matching
   * JA-29's already-accepted "agent ignores cancel and completes" fallback).
   * If (a) becomes feasible, this is where the interrupt signal would be sent.
   */
  async cancel(_params: CancelNotification): Promise<void> {
    // Intentionally does not touch pool state — see docstring above.
  }

  // --------------------------------------------------------------------------
  // closeSession — clean up ACP mapping (pool slot retained until idle eviction)
  // --------------------------------------------------------------------------

  async closeSession(params: CloseSessionRequest): Promise<void> {
    this.pool.unregisterAcpSession(params.sessionId);
  }
}

// ---------------------------------------------------------------------------
// Public factory
// ---------------------------------------------------------------------------

/**
 * Create an Agent instance for the given AgentSideConnection.
 *
 * @param conn  The AgentSideConnection from the SDK.
 * @param pool  The shared ChannelsSessionPool (inject a mock in tests).
 */
export function createAgent(
  conn: AgentSideConnection,
  pool: ChannelsSessionPool
): Agent {
  return new ClaudeAgent(conn, pool);
}

/**
 * Resolve the path to launcher.exp relative to this file's directory.
 * Used by acp_entrypoint.ts to create the pool.
 */
export function resolveLauncherExpPath(): string {
  const thisDir = dirname(fileURLToPath(import.meta.url));
  return join(thisDir, "..", "launcher.exp");
}
