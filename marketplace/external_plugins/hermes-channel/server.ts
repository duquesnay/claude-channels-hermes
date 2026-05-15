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
import { mkdirSync, chmodSync, existsSync, unlinkSync } from 'fs'
import { homedir } from 'os'
import { join, dirname } from 'path'
import { createServer as createNetServer, Socket } from 'net'

const TEST_MODE = process.env.HERMES_CHANNEL_TEST_MODE === '1'

if (TEST_MODE) {
  process.stderr.write(`hermes-channel: TEST_MODE active — echo mode, no Claude involvement\n`)
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
      'Hermes channel. Inbound prompts arrive as <channel source="hermes" chat_id="..." user="hermes" ts="...">.',
      '',
      'MANDATORY: reply_open MUST be your very first tool call after receiving any inbound message — before any Read, Write, Edit, Bash, or other tool. Never do work first and reply later.',
      '',
      'Response protocol:',
      '  1. reply_open(chat_id) → FIRST call. Returns a handle.',
      '  2. reply_chunk(handle, text) → Optional progress updates. Pass FULL accumulated text each time, not deltas.',
      '  3. reply_close(handle, text?) → Sends the final response back to Hermes. Pass the complete final text here.',
      '',
      'The chat_id IS the request_id — pass it unchanged to reply_open.',
      'Your transcript output never reaches Hermes. Only reply_close delivers the result.',
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

const streams = new Map<string, StreamEntry>()

// ============================================================================
// Tool definitions
// ============================================================================

mcp.setRequestHandler(ListToolsRequestSchema, async () => ({
  tools: [
    {
      name: 'reply_open',
      description: 'Start a reply to an inbound Hermes prompt. Returns a handle to use with reply_chunk and reply_close.',
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
      description: 'Accumulate reply text in progress. Pass FULL accumulated text each time, not a delta. No-op in V1 (stored locally, not relayed).',
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
      description: 'Finalize the reply. Sends the result back to Hermes over IPC and closes the stream.',
      inputSchema: {
        type: 'object',
        properties: {
          handle: { type: 'string' },
          text: { type: 'string', description: 'Optional final text override. If omitted, last reply_chunk text is used.' },
        },
        required: ['handle'],
      },
    },
  ],
}))

// ============================================================================
// Tool handlers
// ============================================================================

mcp.setRequestHandler(CallToolRequestSchema, async req => {
  const args = req.params.arguments as Record<string, unknown>

  if (req.params.name === 'reply_open') {
    const { chat_id } = args as { chat_id: string }
    // chat_id doubles as request_id (we routed the notification with request_id as chat_id)
    const pending = pendingByRequestId.get(chat_id)
    if (!pending) {
      throw new Error(`reply_open: no pending IPC request for chat_id=${chat_id}`)
    }
    const handle = randomUUID()
    streams.set(handle, {
      request_id: chat_id,
      hermes_conn: pending.conn,
      startedAt: pending.startedAt,
      accumulatedText: '',
    })
    pendingByRequestId.delete(chat_id)
    process.stderr.write(`hermes-channel: reply_open request_id=${chat_id} handle=${handle}\n`)
    return { content: [{ type: 'text', text: `handle=${handle}` }] }
  }

  if (req.params.name === 'reply_chunk') {
    const { handle, text } = args as { handle: string; text: string }
    const s = streams.get(handle)
    if (!s) throw new Error(`reply_chunk: unknown handle ${handle}`)
    s.accumulatedText = text
    return { content: [{ type: 'text', text: 'queued' }] }
  }

  if (req.params.name === 'reply_close') {
    const { handle, text } = args as { handle: string; text?: string }
    const s = streams.get(handle)
    if (!s) throw new Error(`reply_close: unknown handle ${handle}`)
    streams.delete(handle)
    const finalText = text ?? s.accumulatedText
    const duration_ms = Date.now() - s.startedAt
    const result = JSON.stringify({
      type: 'result',
      request_id: s.request_id,
      content: finalText,
      duration_ms,
    }) + '\n'
    process.stderr.write(`hermes-channel: reply_close request_id=${s.request_id} duration_ms=${duration_ms}\n`)
    s.hermes_conn.write(result)
    return { content: [{ type: 'text', text: 'closed' }] }
  }

  throw new Error(`unknown tool: ${req.params.name}`)
})

// ============================================================================
// Pending IPC requests — before reply_open is called, we track conn here
// keyed by request_id so the tool handler can retrieve it.
// ============================================================================

interface PendingRequest {
  conn: Socket
  startedAt: number
}

const pendingByRequestId = new Map<string, PendingRequest>()

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

  if (msg.type !== 'prompt') {
    process.stderr.write(`hermes-channel: unknown IPC message type=${msg.type}, ignoring\n`)
    return
  }

  const { request_id, content } = msg
  process.stderr.write(`hermes-channel: inbound request_id=${request_id} content_len=${content.length}\n`)

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

  // Register pending so reply_open can retrieve the connection
  pendingByRequestId.set(request_id, { conn, startedAt: Date.now() })

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

main().catch(err => {
  process.stderr.write(`hermes-channel: boot failed: ${err}\n`)
  process.exit(1)
})
