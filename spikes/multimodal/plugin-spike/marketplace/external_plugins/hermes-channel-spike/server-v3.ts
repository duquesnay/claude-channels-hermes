#!/usr/bin/env bun
/**
 * SPIKE: Multimodal hermes-channel plugin.
 *
 * Extends notifications/claude/channel to carry image content.
 * Three variants controlled by MULTIMODAL_VARIANT env var:
 *
 *   MULTIMODAL_VARIANT=1 (default):
 *     content is an array of MCP-style ImageContent parts:
 *       [{ type: 'text', text }, { type: 'image', data: <b64>, mimeType: 'image/png' }]
 *
 *   MULTIMODAL_VARIANT=2:
 *     Anthropic-style content blocks with nested source:
 *       [{ type: 'text', text }, { type: 'image', source: { type: 'base64', media_type: 'image/png', data } }]
 *
 *   MULTIMODAL_VARIANT=3:
 *     Legacy string content + sidecar content_parts meta field:
 *       { content: '<text>', content_parts: [{ type: 'image', ... }], meta: {...} }
 *
 * Socket: HERMES_CHANNEL_SOCKET env var (default ~/.hermes/run/hermes-channel-spike.sock)
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

const VARIANT = parseInt(process.env.MULTIMODAL_VARIANT ?? '3', 10)
const SOCKET_PATH = process.env.HERMES_CHANNEL_SOCKET ?? join(homedir(), '.hermes', 'run', 'hermes-channel-spike.sock')

process.stderr.write(`hermes-channel-spike: variant=${VARIANT} socket=${SOCKET_PATH}\n`)

// ============================================================================
// MCP server
// ============================================================================

const mcp = new Server(
  { name: 'hermes-channel-spike', version: '0.1.0-spike' },
  {
    capabilities: {
      tools: {},
      experimental: { 'claude/channel': {} },
    },
    instructions: [
      'Hermes channel (multimodal spike). Inbound prompts arrive as <channel source="hermes" chat_id="..." user="hermes" ts="...">.',
      '',
      'MANDATORY: reply_open MUST be your very first tool call after receiving any inbound message.',
      '',
      'Response protocol:',
      '  1. reply_open(chat_id) → FIRST call. Returns a handle.',
      '  2. reply_chunk(handle, text) → Optional progress updates.',
      '  3. reply_close(handle, text?) → Sends the result back. Pass the complete final text here.',
      '',
      'The chat_id IS the request_id — pass it unchanged to reply_open.',
      'Your transcript output never reaches Hermes. Only reply_close delivers the result.',
      '',
      'IMPORTANT: When you receive an inbound message with an image, describe what you see in the image.',
      'Be specific about colors and shapes.',
    ].join('\n'),
  },
)

// ============================================================================
// Streams map
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
      description: 'Start a reply to an inbound Hermes prompt. Returns a handle.',
      inputSchema: {
        type: 'object',
        properties: {
          chat_id: { type: 'string', description: 'The chat_id from the inbound channel message.' },
        },
        required: ['chat_id'],
      },
    },
    {
      name: 'reply_chunk',
      description: 'Accumulate reply text in progress. Pass FULL accumulated text each time, not a delta.',
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
      description: 'Finalize the reply. Sends the result back to Hermes over IPC.',
      inputSchema: {
        type: 'object',
        properties: {
          handle: { type: 'string' },
          text: { type: 'string', description: 'Optional final text override.' },
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
    process.stderr.write(`hermes-channel-spike: reply_open request_id=${chat_id} handle=${handle}\n`)
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
    process.stderr.write(`hermes-channel-spike: reply_close request_id=${s.request_id} duration_ms=${duration_ms}\n`)
    s.hermes_conn.write(result)
    return { content: [{ type: 'text', text: 'closed' }] }
  }

  throw new Error(`unknown tool: ${req.params.name}`)
})

// ============================================================================
// Pending IPC requests
// ============================================================================

interface PendingRequest {
  conn: Socket
  startedAt: number
}

const pendingByRequestId = new Map<string, PendingRequest>()

// ============================================================================
// Multimodal notification builder
// ============================================================================

interface IpcMessage {
  type: string
  request_id: string
  content: string
  image_b64?: string
  image_mime?: string
  timeout_ms?: number
}

function buildNotificationParams(msg: IpcMessage): Record<string, unknown> {
  const meta = {
    chat_id: msg.request_id,
    message_id: msg.request_id,
    user: 'hermes',
    ts: new Date().toISOString(),
  }

  if (!msg.image_b64) {
    // Text-only: plain string content (original behavior)
    return { content: msg.content, meta }
  }

  const imageData = msg.image_b64
  const mimeType = msg.image_mime ?? 'image/png'

  if (VARIANT === 1) {
    // MCP-style ImageContent parts
    // https://spec.modelcontextprotocol.io/specification/2025-03-26/server/utilities/prompts/
    process.stderr.write(`hermes-channel-spike: variant=1 MCP-style ImageContent\n`)
    return {
      content: [
        { type: 'text', text: msg.content },
        { type: 'image', data: imageData, mimeType },
      ],
      meta,
    }
  }

  if (VARIANT === 2) {
    // Anthropic-style content blocks with nested source
    process.stderr.write(`hermes-channel-spike: variant=2 Anthropic-style source blocks\n`)
    return {
      content: [
        { type: 'text', text: msg.content },
        { type: 'image', source: { type: 'base64', media_type: mimeType, data: imageData } },
      ],
      meta,
    }
  }

  // VARIANT === 3: legacy string + sidecar content_parts
  process.stderr.write(`hermes-channel-spike: variant=3 sidecar content_parts\n`)
  return {
    content: msg.content,
    content_parts: [
      { type: 'text', text: msg.content },
      { type: 'image', data: imageData, mimeType },
    ],
    meta,
  }
}

// ============================================================================
// IPC server
// ============================================================================

function handleIpcLine(line: string, conn: Socket): void {
  let msg: IpcMessage
  try {
    msg = JSON.parse(line)
  } catch (err) {
    process.stderr.write(`hermes-channel-spike: IPC JSON parse error: ${err} — line=${line.slice(0, 200)}\n`)
    return
  }

  if (msg.type !== 'prompt') {
    process.stderr.write(`hermes-channel-spike: unknown IPC message type=${msg.type}, ignoring\n`)
    return
  }

  const { request_id } = msg
  const hasImage = !!msg.image_b64
  process.stderr.write(`hermes-channel-spike: inbound request_id=${request_id} has_image=${hasImage} content_len=${msg.content.length}\n`)

  pendingByRequestId.set(request_id, { conn, startedAt: Date.now() })

  const notifParams = buildNotificationParams(msg)

  mcp.notification({
    method: 'notifications/claude/channel',
    params: notifParams,
  }).catch(err => {
    process.stderr.write(`hermes-channel-spike: failed to deliver notification to Claude: ${err}\n`)
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
    process.stderr.write(`hermes-channel-spike: IPC client connected\n`)
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
      process.stderr.write(`hermes-channel-spike: IPC client disconnected\n`)
    })

    conn.on('error', err => {
      process.stderr.write(`hermes-channel-spike: IPC conn error: ${err}\n`)
    })
  })

  server.listen(SOCKET_PATH, () => {
    chmodSync(SOCKET_PATH, 0o600)
    process.stderr.write(`hermes-channel-spike: IPC socket ready at ${SOCKET_PATH}\n`)
  })

  server.on('error', err => {
    process.stderr.write(`hermes-channel-spike: IPC server error: ${err}\n`)
    process.exit(1)
  })
}

// ============================================================================
// Boot
// ============================================================================

process.on('unhandledRejection', err => {
  process.stderr.write(`hermes-channel-spike: unhandled rejection: ${err}\n`)
})
process.on('uncaughtException', err => {
  process.stderr.write(`hermes-channel-spike: uncaught exception: ${err}\n`)
  process.exit(1)
})

async function main() {
  await mcp.connect(new StdioServerTransport())
  startIpcServer()
}

main().catch(err => {
  process.stderr.write(`hermes-channel-spike: boot failed: ${err}\n`)
  process.exit(1)
})
