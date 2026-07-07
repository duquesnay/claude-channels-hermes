#!/usr/bin/env bun
/**
 * IPC bridge between Hermes daemon and a claude --channels session.
 *
 * Face MCP: stdio MCP server with reply_open/reply_chunk/reply_close tools.
 *   Inbound prompts arrive as notifications/claude/channel so Claude sees them
 *   and calls the reply_* tools to send results back.
 *
 * Face IPC: Unix socket ~/.hermes/run/hermes-channel.sock (0600).
 *   Hermes writes JSONL prompt requests; plugin writes JSONL results back on
 *   the same connection. Multiplexed by request_id UUID.
 *
 * HERMES_CHANNEL_TEST_MODE=1: echo mode — plugin responds to IPC prompts
 *   directly without involving Claude. For smoke-testing the IPC protocol.
 */

import { Server } from '@modelcontextprotocol/sdk/server/index.js'
import { StdioServerTransport } from '@modelcontextprotocol/sdk/server/stdio.js'
import {
  ListToolsRequestSchema,
  CallToolRequestSchema,
} from '@modelcontextprotocol/sdk/types.js'
import { randomUUID } from 'crypto'
import { mkdirSync, chmodSync, existsSync, unlinkSync, appendFileSync } from 'fs'
import { homedir } from 'os'
import { join, dirname } from 'path'
import { createServer as createNetServer, Socket } from 'net'

const TEST_MODE = process.env.HERMES_CHANNEL_TEST_MODE === '1'

if (TEST_MODE) {
  process.stderr.write(`hermes-channel: TEST_MODE active — echo mode, no Claude involvement\n`)
}

// JA-24 instrumentation: session_pool.ts's drainToStderr only forwards this
// process's stderr into the acp_entrypoint's own stderr, which the Python
// acp_client only buffers in-memory (stderr_tail(), surfaced on error paths) —
// it never lands in a file we can grep. So, alongside the existing
// stderr.write calls (kept for continuity / live tailing), also append each
// timing line to a plain file next to this script for reliable retrieval.
const JA24_LOG_PATH = join(__dirname, 'ja24-timing.log')
function ja24Log(line: string): void {
  // No-op when this module is imported rather than run directly (e.g. from
  // bun test's handleReply* unit tests) — keeps test output quiet and avoids
  // polluting the real timing log with synthetic test chat_ids.
  if (!import.meta.main) return
  process.stderr.write(line)
  try {
    appendFileSync(JA24_LOG_PATH, line)
  } catch (err) {
    process.stderr.write(`hermes-channel: JA24 file log write failed: ${err}\n`)
  }
}

const SOCKET_PATH = process.env.HERMES_CHANNEL_SOCKET ?? join(homedir(), '.hermes', 'run', 'hermes-channel.sock')

// ============================================================================
// MCP server
// ============================================================================

const mcp = new Server(
  { name: 'hermes-channel', version: '0.1.0' },
  {
    capabilities: {
      tools: {},
      experimental: { 'claude/channel': {} },
    },
    instructions: [
      'MANDATORY: your text/transcript output is NEVER delivered to the user — it is silently discarded. The ONLY way to send a response is calling the reply_close tool. If your turn ends without calling reply_close, the user receives absolutely nothing and your work is lost.',
      '',
      'This overrides anything your persona or identity instructions say about answering "directly" or "immediately" or being concise — those describe your TONE, not your delivery mechanism. Answering directly means calling reply_close with your direct answer, not skipping the call.',
      '',
      'Inbound prompts arrive as <channel source="hermes" chat_id="..." user="hermes" ts="...">. The chat_id IS the request_id.',
      '',
      'Protocol — decide immediately which of these two cases you are in:',
      '',
      'CASE A — you can answer without any other tool (this is most turns): your VERY FIRST tool call MUST be reply_close(chat_id, text), with your COMPLETE final answer as text. Do not write your answer as plain text/reasoning first, and do not call any other tool first — compose the whole reply directly inside reply_close\'s text argument and call it immediately, as your first action this turn. There is no benefit to thinking out loud before it: that output is discarded anyway, and delaying reply_close is the only way this case can go wrong.',
      '',
      'CASE B — you genuinely need another tool (Read, Bash, Agent, etc.) to produce the answer: use those tools as needed, then call reply_close(chat_id, text) with your complete final response as your LAST tool call.',
      '',
      'MANDATORY: if you launch background/async work (Agent tool, background Bash) and are about to return control while that work keeps running, you MUST still call reply_close before the end of your turn — deliver an interim answer (e.g. "I started X, I will follow up") and report the async results later in a NEW turn when the notification arrives. This is a CASE B turn: the Agent/Bash call is the other tool, reply_close still comes last.',
      'NEVER leave a turn open waiting for async work: the turn times out and the user receives an empty response.',
      '',
      'Advanced (optional): call reply_open(chat_id) first if you want to send progress updates via reply_chunk(handle, text) before the final reply_close(handle, text). Only needed for that case — skip it for the common cases above.',
      '',
      'Before you finish this turn, double check: did you call reply_close? If not, do it now — a plain text reply is never seen by anyone. Only reply_close delivers the result.',
    ].join('\n'),
  },
)

// ============================================================================
// Streams map — handle → routing context
// ============================================================================

interface StreamEntry {
  request_id: string
  hermes_conn: Socket
  startedAt: number
  accumulatedText: string
}

export const streams = new Map<string, StreamEntry>()

// ============================================================================
// Tool definitions
// ============================================================================

mcp.setRequestHandler(ListToolsRequestSchema, async () => ({
  tools: [
    {
      name: 'reply_open',
      description: 'OPTIONAL — only needed if you want to send reply_chunk progress updates before the final reply_close. Starts a reply to an inbound Hermes prompt and returns a handle to use with reply_chunk and reply_close. Skip this and call reply_close(chat_id, text) directly for the common case.',
      inputSchema: {
        type: 'object',
        properties: {
          chat_id: { type: 'string', description: 'The chat_id from the inbound channel message (equals the request_id).' },
        },
        required: ['chat_id'],
      },
    },
    {
      name: 'reply_chunk',
      description: 'Accumulate reply text in progress. Pass FULL accumulated text each time, not a delta. No-op in V1 (stored locally, not relayed). Requires a handle from reply_open.',
      inputSchema: {
        type: 'object',
        properties: {
          handle: { type: 'string', description: 'Handle returned by reply_open.' },
          text: { type: 'string', description: 'Full accumulated text so far.' },
        },
        required: ['handle', 'text'],
      },
    },
    {
      name: 'reply_close',
      description: 'MANDATORY — this is the only way any of your output reaches the user. If you need no other tool to answer, call this FIRST, with your complete answer as text. If you do need other tools first, call this LAST, after them. Call it always, even for the simplest one-line reply; plain text output is discarded and never delivered. Sends the result back to Hermes over IPC. Pass EITHER chat_id (common case — no prior reply_open needed) OR handle (if you called reply_open for progress chunks). Exactly one of the two is required.',
      inputSchema: {
        type: 'object',
        properties: {
          handle: { type: 'string', description: 'Handle returned by reply_open. Use this OR chat_id, not both.' },
          chat_id: { type: 'string', description: 'The chat_id from the inbound channel message. Use this OR handle, not both — this is the common case, skipping reply_open entirely.' },
          text: { type: 'string', description: 'Final reply text. Required when closing by chat_id (no prior reply_chunk to fall back on); optional when closing by handle (falls back to the last reply_chunk text).' },
        },
        required: [],
      },
    },
  ],
}))

// ============================================================================
// Tool handlers
// ============================================================================

// Shared by both reply_close paths (by handle, or directly by chat_id):
// builds the IPC result, logs it, and writes it back to Hermes.
function finalizeReply(request_id: string, conn: Socket, startedAt: number, finalText: string): void {
  const duration_ms = Date.now() - startedAt
  const result = JSON.stringify({
    type: 'result',
    request_id,
    content: finalText,
    duration_ms,
  }) + '\n'
  ja24Log(`hermes-channel: reply_close request_id=${request_id} duration_ms=${duration_ms} ts=${Date.now()}\n`)
  conn.write(result)
}

export async function handleReplyOpen(args: Record<string, unknown>) {
  const { chat_id } = args as { chat_id: string }
  // chat_id doubles as request_id (we routed the notification with request_id as chat_id)
  const pending = pendingByRequestId.get(chat_id)
  if (!pending) {
    throw new Error(`reply_open: no pending IPC request for chat_id=${chat_id}`)
  }
  clearTimeout(pending.guardTimer)
  const handle = randomUUID()
  streams.set(handle, {
    request_id: chat_id,
    hermes_conn: pending.conn,
    startedAt: pending.startedAt,
    accumulatedText: '',
  })
  pendingByRequestId.delete(chat_id)
  // JA-24: ts lets us split inbound->reply_open ("tax" of the first, empty-handed
  // model call) from reply_open->reply_close ("reflection", the actual work) —
  // see the matching ts= on the inbound and reply_close log lines below.
  ja24Log(`hermes-channel: reply_open request_id=${chat_id} handle=${handle} ts=${Date.now()}\n`)
  return { content: [{ type: 'text', text: `handle=${handle}` }] }
}

export async function handleReplyChunk(args: Record<string, unknown>) {
  const { handle, text } = args as { handle: string; text: string }
  const s = streams.get(handle)
  if (!s) throw new Error(`reply_chunk: unknown handle ${handle}`)
  s.accumulatedText = text
  return { content: [{ type: 'text', text: 'queued' }] }
}

export async function handleReplyClose(args: Record<string, unknown>) {
  const { handle, chat_id, text } = args as { handle?: string; chat_id?: string; text?: string }

  if (handle) {
    const s = streams.get(handle)
    if (!s) throw new Error(`reply_close: unknown handle ${handle}`)
    streams.delete(handle)
    finalizeReply(s.request_id, s.hermes_conn, s.startedAt, text ?? s.accumulatedText)
    return { content: [{ type: 'text', text: 'closed' }] }
  }

  if (chat_id) {
    // Fast path (JA-24 collapse): close directly by chat_id, skipping reply_open.
    // Only valid if reply_open hasn't already consumed the pending entry for
    // this chat_id — if it has, the model must use the handle it was given.
    const pending = pendingByRequestId.get(chat_id)
    if (!pending) {
      throw new Error(
        `reply_close: no pending IPC request for chat_id=${chat_id} (already closed, or reply_open already consumed it — use the handle it returned instead)`,
      )
    }
    // JA-24 review finding 1: unlike the handle path (which can fall back to
    // accumulated reply_chunk text), there is no accumulated text on this
    // path — falling back to '' would silently send an empty response to the
    // user (the JA-25 bug class). Reject instead, and do NOT consume the
    // pending entry, so a retry with proper text is still possible.
    if (!text) {
      throw new Error(
        `reply_close: text is required when closing by chat_id (no prior reply_chunk to fall back on) — pass the final response text`,
      )
    }
    clearTimeout(pending.guardTimer)
    pendingByRequestId.delete(chat_id)
    finalizeReply(chat_id, pending.conn, pending.startedAt, text)
    return { content: [{ type: 'text', text: 'closed' }] }
  }

  throw new Error(`reply_close: must pass either 'chat_id' (common case) or 'handle' (after reply_open)`)
}

// ============================================================================
// fallback_close — JA-24 v2 stage 4: Stop-hook last-resort delivery
// ============================================================================
// A Stop hook (a SEPARATE process, spawned by the CLI when the model's turn
// ends, outside the model's own tool-calling loop) is the last line of
// defense against the exact prod failure mode: the model produces real,
// correct text but calls neither reply_open nor reply_close. Where the
// anti-wedge guard only fails the turn cleanly, the Stop hook can extract the
// model's actual answer from the transcript and deliver it here.
//
// This is NOT an MCP tool — the model never calls it, and never should. It
// arrives over the same raw IPC socket as `prompt`/result messages, from the
// hook process's own throwaway connection (distinct from Hermes's long-lived
// one). The PLUGIN arbitrates, not the hook: pendingByRequestId (simple path)
// and streams (advanced reply_open path, keyed by handle but carrying the
// same request_id) are the single source of truth for "already closed?" —
// reusing the exact invariant enforcement reply_close's double-close guard
// and the anti-wedge guard both already rely on. A fallback_close can never
// produce a second result for a request_id a tool call (or the guard)
// already closed.
export function handleFallbackClose(msg: { request_id: string; content?: string }, conn: Socket): void {
  const { request_id, content } = msg
  // Logged unconditionally, before knowing the outcome — proves the hook's
  // message reached the plugin at all, distinct from whether the plugin
  // then delivered it or correctly no-op'd. Without this, a live check like
  // "did fallback_close arrive?" can't tell "the hook never ran" apart from
  // "the hook ran and this was a harmless no-op" (JA-24 v2 spike finding).
  ja24Log(`hermes-channel: fallback_close received request_id=${request_id} has_content=${Boolean(content)} ts=${Date.now()}\n`)
  const ack = (delivered: boolean, error?: string) => {
    conn.write(JSON.stringify({ type: 'fallback_close_ack', request_id, delivered, ...(error ? { error } : {}) }) + '\n')
  }

  const pending = pendingByRequestId.get(request_id)
  if (pending) {
    // Mirrors the JA-24 v2 chat_id-close rule: never silently deliver an
    // empty answer (the JA-25 bug class) — reject and leave the entry armed
    // so a tool call or a corrected fallback retry can still succeed.
    if (!content) {
      ack(false, 'content is required for fallback_close')
      return
    }
    clearTimeout(pending.guardTimer)
    pendingByRequestId.delete(request_id)
    ja24Log(`hermes-channel: fallback_close (simple path) request_id=${request_id} ts=${Date.now()}\n`)
    finalizeReply(request_id, pending.conn, pending.startedAt, content)
    ack(true)
    return
  }

  // Advanced path: reply_open was called (entry moved to `streams`, keyed by
  // handle) but reply_close never followed. streams isn't keyed by
  // request_id, so scan for the matching entry — the pool is capped at a
  // handful of concurrent turns, so this is cheap.
  for (const [handle, s] of streams) {
    if (s.request_id !== request_id) continue
    const finalText = content || s.accumulatedText
    if (!finalText) {
      ack(false, 'content is required for fallback_close (no prior reply_chunk to fall back on)')
      return
    }
    streams.delete(handle)
    ja24Log(`hermes-channel: fallback_close (advanced path) request_id=${request_id} handle=${handle} ts=${Date.now()}\n`)
    finalizeReply(s.request_id, s.hermes_conn, s.startedAt, finalText)
    ack(true)
    return
  }

  // Neither map has it: already delivered by a real tool call, or the
  // anti-wedge guard already fired. Clean no-op — the hook logs and exits.
  ack(false, 'no pending request for this request_id — already closed')
}

mcp.setRequestHandler(CallToolRequestSchema, async req => {
  const args = req.params.arguments as Record<string, unknown>

  if (req.params.name === 'reply_open') return handleReplyOpen(args)
  if (req.params.name === 'reply_chunk') return handleReplyChunk(args)
  if (req.params.name === 'reply_close') return handleReplyClose(args)

  throw new Error(`unknown tool: ${req.params.name}`)
})

// ============================================================================
// Pending IPC requests — before reply_open is called, we track conn here
// keyed by request_id so the tool handler can retrieve it.
// ============================================================================

interface PendingRequest {
  conn: Socket
  startedAt: number
  guardTimer: ReturnType<typeof setTimeout>
}

export const pendingByRequestId = new Map<string, PendingRequest>()

// ============================================================================
// Anti-wedge guard — JA-24 v2 step 3
// ============================================================================
// A weak/distracted model can receive an inbound prompt and call NEITHER
// reply_open NOR reply_close — plain assistant text, zero tool calls (this
// reproduced live on janet-test: a heavy-persona haiku worker, ~40% of
// turns). Without this guard the pending IPC request sits forever: Hermes'
// sendPrompt is still awaiting a `result` line on this connection, and the
// only thing that eventually unblocks it is the much coarser gateway-level
// timeout (JA-27, 210-300s) — a fully wedged turn in the meantime.
//
// This guard fires much sooner (default 90s — comfortably inside a normal
// turn, well past a stuck one) and writes an explicit `error` result so
// sendPrompt fails fast and cleanly instead of wedging.
//
// Read fresh on every call (not cached at module load) so tests can override
// per-case without a separate process — mirrors the existing
// HERMES_CHANNEL_SOCKET / HERMES_CHANNELS_MAX_SESSIONS env-override pattern
// used elsewhere in this codebase (acp/src/session_pool.ts).
export function replyCloseGuardMs(): number {
  return Number(process.env.HERMES_REPLY_CLOSE_GUARD_MS ?? 90_000)
}

// Arms the guard for a freshly-registered pending request. Fires at most
// once: it only acts if the pendingByRequestId entry for this request_id is
// STILL the original, never-opened, never-closed one — reply_open and
// reply_close both delete it (and clear this timer) as part of normal
// handling, so a legitimate close always wins the race, and a late
// reply_open/reply_close after the guard already fired finds no pending
// entry and fails with the same "no pending IPC request" error as any other
// already-closed request (never a second write to the socket).
export function armReplyCloseGuard(request_id: string): ReturnType<typeof setTimeout> {
  return setTimeout(() => {
    const pending = pendingByRequestId.get(request_id)
    if (!pending) return
    pendingByRequestId.delete(request_id)
    const guardMs = replyCloseGuardMs()
    ja24Log(`hermes-channel: reply_close_guard_fired request_id=${request_id} guard_ms=${guardMs} ts=${Date.now()}\n`)
    const errReply = JSON.stringify({
      type: 'error',
      request_id,
      error: `model completed without calling reply_close (no reply_open or reply_close within ${guardMs}ms)`,
    }) + '\n'
    pending.conn.write(errReply)
  }, replyCloseGuardMs())
}

// ============================================================================
// IPC server — listens on SOCKET_PATH for Hermes connections
// ============================================================================

function handleIpcLine(line: string, conn: Socket): void {
  let msg: { type: string; request_id: string; content: string; timeout_ms?: number }
  try {
    msg = JSON.parse(line)
  } catch (err) {
    process.stderr.write(`hermes-channel: IPC JSON parse error: ${err} — line=${line.slice(0, 200)}\n`)
    return
  }

  if (msg.type === 'fallback_close') {
    // From a Stop hook's own throwaway connection, not Hermes's long-lived
    // one — see handleFallbackClose's own doc comment for the full contract.
    handleFallbackClose(msg as unknown as { request_id: string; content?: string }, conn)
    return
  }

  if (msg.type !== 'prompt') {
    process.stderr.write(`hermes-channel: unknown IPC message type=${msg.type}, ignoring\n`)
    return
  }

  const { request_id, content } = msg
  ja24Log(`hermes-channel: inbound request_id=${request_id} content_len=${content.length} ts=${Date.now()}\n`)

  if (TEST_MODE) {
    // Echo directly without Claude
    const reply = JSON.stringify({
      type: 'result',
      request_id,
      content: `echo: ${content}`,
      duration_ms: 0,
    }) + '\n'
    conn.write(reply)
    return
  }

  // Register pending so reply_open can retrieve the connection. Arm the
  // anti-wedge guard (JA-24 v2 step 3) at the same time: if this model turn
  // ends without ever calling reply_open or reply_close, the guard fires and
  // fails the request explicitly instead of leaving it wedged.
  const guardTimer = armReplyCloseGuard(request_id)
  pendingByRequestId.set(request_id, { conn, startedAt: Date.now(), guardTimer })

  // Emit channel notification to Claude
  mcp.notification({
    method: 'notifications/claude/channel',
    params: {
      content,
      meta: {
        chat_id: request_id,
        message_id: request_id,
        user: 'hermes',
        ts: new Date().toISOString(),
      },
    },
  }).catch(err => {
    process.stderr.write(`hermes-channel: failed to deliver notification to Claude: ${err}\n`)
    clearTimeout(guardTimer)
    pendingByRequestId.delete(request_id)
    const errReply = JSON.stringify({
      type: 'error',
      request_id,
      error: `notification delivery failed: ${err}`,
    }) + '\n'
    conn.write(errReply)
  })
}

function startIpcServer(): void {
  mkdirSync(dirname(SOCKET_PATH), { recursive: true })

  if (existsSync(SOCKET_PATH)) {
    try { unlinkSync(SOCKET_PATH) } catch {}
  }

  const server = createNetServer(conn => {
    process.stderr.write(`hermes-channel: IPC client connected\n`)
    let buf = ''

    conn.on('data', chunk => {
      buf += chunk.toString()
      let nl: number
      while ((nl = buf.indexOf('\n')) !== -1) {
        const line = buf.slice(0, nl).trim()
        buf = buf.slice(nl + 1)
        if (line) handleIpcLine(line, conn)
      }
    })

    conn.on('end', () => {
      process.stderr.write(`hermes-channel: IPC client disconnected\n`)
    })

    conn.on('error', err => {
      process.stderr.write(`hermes-channel: IPC conn error: ${err}\n`)
    })
  })

  server.listen(SOCKET_PATH, () => {
    chmodSync(SOCKET_PATH, 0o600)
    process.stderr.write(`hermes-channel: IPC socket listening at ${SOCKET_PATH}\n`)
  })

  server.on('error', err => {
    process.stderr.write(`hermes-channel: IPC server error: ${err}\n`)
    process.exit(1)
  })
}

// ============================================================================
// Boot
// ============================================================================

process.on('unhandledRejection', err => {
  process.stderr.write(`hermes-channel: unhandled rejection: ${err}\n`)
})
process.on('uncaughtException', err => {
  process.stderr.write(`hermes-channel: uncaught exception: ${err}\n`)
  process.exit(1)
})

async function main() {
  await mcp.connect(new StdioServerTransport())
  startIpcServer()
}

// JA-24: guarded so bun test can import this module (for pendingByRequestId /
// streams / handleReply* unit tests) without booting a live stdio MCP
// connection + Unix socket server as a side effect. import.meta.main is only
// true when this file is the process entry point (`bun server.ts`), not when
// imported from a test file.
if (import.meta.main) {
  // JA-24 instrumentation marker: proves a freshly-spawned worker loaded THIS
  // build (vs a stale worker still running pre-edit code from before a
  // restart/eviction). Logged only on real boot, not on module import from tests.
  ja24Log(`hermes-channel: booted JA24-instrumented build pid=${process.pid} ts=${Date.now()}\n`)
  main().catch(err => {
    process.stderr.write(`hermes-channel: boot failed: ${err}\n`)
    process.exit(1)
  })
}
