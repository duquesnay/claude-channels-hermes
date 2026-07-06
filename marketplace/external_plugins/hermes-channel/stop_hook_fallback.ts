#!/usr/bin/env bun
/**
 * Stop hook: last-resort delivery for hermes-channel's inbound turns.
 *
 * JA-24 v2 stage 4. Fires when the model's turn ends (Claude Code's "Stop"
 * lifecycle event), as a SEPARATE process outside the model's own tool-call
 * loop. If the model never called reply_open/reply_close — the residual
 * ~10% failure even after hardening the MCP instructions (JA-24 v2 stages
 * 1-2) and adding the anti-wedge guard (stage 3) — this hook extracts the
 * model's actual final text from the transcript and delivers it via a
 * `fallback_close` message on the SAME IPC socket the plugin already listens
 * on. See handleFallbackClose in server.ts for the delivery contract: the
 * PLUGIN arbitrates (pendingByRequestId / streams are the single source of
 * truth for "already closed?"), so this can never produce a second result
 * for a request_id a real tool call — or the anti-wedge guard — already
 * closed. Calling this a no-op in that case is correct, not an error.
 *
 * Best-effort, side-channel only: this hook must NEVER block or fail the
 * CLI's own Stop lifecycle. It always exits 0, regardless of outcome.
 *
 * Open question from the JA-24 v2 spike (see mission notes): whether a
 * `--channels` worker's transcript actually renders the inbound MCP
 * notification (`notifications/claude/channel`) as a literal
 * `<channel source="hermes" chat_id="...">` user-turn string, the way the
 * MCP server's own `instructions` field describes it to the model. This is
 * ASSUMED here (extraction regex below) but not yet empirically confirmed —
 * flagged for the live verification step before trusting this in prod.
 */

import { readFileSync } from 'fs'
import { createConnection } from 'net'
import { homedir } from 'os'
import { join } from 'path'

interface HookInput {
  transcript_path?: string
  cwd?: string
  session_id?: string
  hook_event_name?: string
}

interface TranscriptEntry {
  type?: string
  message?: {
    role?: string
    content?: string | Array<{ type?: string; text?: string }>
  }
}

function log(line: string): void {
  process.stderr.write(`stop-hook-fallback: ${line}\n`)
}

function extractText(content: TranscriptEntry['message'] extends infer M ? (M extends { content?: infer C } ? C : never) : never): string {
  if (typeof content === 'string') return content
  if (Array.isArray(content)) return content.map(c => c.text ?? '').join('')
  return ''
}

// Finds the CURRENT turn's chat_id: the most recent user-role transcript
// entry whose content contains the hermes-channel inbound tag. Everything
// assistant-authored after that line belongs to this turn — no formal
// per-turn boundary marker is needed beyond that.
export function findCurrentTurn(lines: string[]): { chatId: string; turnStartIndex: number } | null {
  for (let i = lines.length - 1; i >= 0; i--) {
    let obj: TranscriptEntry
    try {
      obj = JSON.parse(lines[i])
    } catch {
      continue
    }
    if (obj.type !== 'user') continue
    const text = extractText(obj.message?.content)
    const match = text.match(/<channel[^>]*\bchat_id="([^"]+)"/)
    if (match) return { chatId: match[1], turnStartIndex: i }
  }
  return null
}

// Concatenates every text content-block from assistant transcript entries
// after turnStartIndex. Each JSONL line is ONE content block (text,
// tool_use, or thinking), chained via parentUuid — NOT one line per full
// API response — so a turn with interleaved thinking/tool_use/text spans
// multiple lines. Taking every text block in order (not just the last line)
// covers that without needing to parse the parentUuid chain.
export function extractFinalText(lines: string[], turnStartIndex: number): string {
  const textParts: string[] = []
  for (let i = turnStartIndex + 1; i < lines.length; i++) {
    let obj: TranscriptEntry
    try {
      obj = JSON.parse(lines[i])
    } catch {
      continue
    }
    if (obj.type !== 'assistant') continue
    const content = obj.message?.content
    if (!Array.isArray(content)) continue
    for (const block of content) {
      if (block.type === 'text' && block.text) textParts.push(block.text)
    }
  }
  return textParts.join('\n\n').trim()
}

function sendFallbackClose(chatId: string, content: string): Promise<void> {
  const socketPath = process.env.HERMES_CHANNEL_SOCKET ?? join(homedir(), '.hermes', 'run', 'hermes-channel.sock')

  return new Promise(resolve => {
    const conn = createConnection(socketPath, () => {
      conn.write(JSON.stringify({ type: 'fallback_close', request_id: chatId, content }) + '\n')
    })

    let buf = ''
    conn.on('data', chunk => {
      buf += chunk.toString()
      const nl = buf.indexOf('\n')
      if (nl === -1) return
      const line = buf.slice(0, nl)
      try {
        const ack = JSON.parse(line)
        if (ack.delivered) {
          log(`chat_id=${chatId}: delivered via Stop-hook fallback (${content.length} chars)`)
        } else {
          log(`chat_id=${chatId}: fallback not needed (${ack.error ?? 'already closed'})`)
        }
      } catch (err) {
        log(`chat_id=${chatId}: could not parse ack: ${err}`)
      }
      conn.end()
      resolve()
    })

    conn.on('error', err => {
      log(`chat_id=${chatId}: socket connection failed (path=${socketPath}): ${err}`)
      resolve()
    })

    // Never let a stuck socket hang the CLI's own Stop lifecycle.
    setTimeout(() => {
      log(`chat_id=${chatId}: ack timeout after 5s — giving up`)
      conn.destroy()
      resolve()
    }, 5000)
  })
}

async function main(): Promise<void> {
  let raw = ''
  for await (const chunk of process.stdin) raw += chunk

  let input: HookInput
  try {
    input = JSON.parse(raw)
  } catch (err) {
    log(`could not parse stdin JSON: ${err}`)
    return
  }

  if (!input.transcript_path) {
    log('no transcript_path in hook input — nothing to do')
    return
  }

  let lines: string[]
  try {
    lines = readFileSync(input.transcript_path, 'utf8').split('\n').filter(Boolean)
  } catch (err) {
    log(`could not read transcript_path=${input.transcript_path}: ${err}`)
    return
  }

  const turn = findCurrentTurn(lines)
  if (!turn) {
    log('no <channel chat_id="..."> tag found in any user turn — not a hermes-channel inbound turn, or the transcript format differs from what the MCP instructions describe; skipping')
    return
  }

  const finalText = extractFinalText(lines, turn.turnStartIndex)
  if (!finalText) {
    log(`chat_id=${turn.chatId}: no assistant text found this turn — likely already closed normally via a tool call, or a genuinely empty turn. Not delivering (the plugin would reject empty content anyway).`)
    return
  }

  await sendFallbackClose(turn.chatId, finalText)
}

// Guarded so bun test can import findCurrentTurn/extractFinalText for unit
// tests without reading real stdin / booting a socket connection as a side
// effect — same pattern as server.ts's import.meta.main guard.
if (import.meta.main) {
  main()
    .catch(err => log(`unhandled error: ${err}`))
    .finally(() => process.exit(0)) // never fail the CLI's own Stop lifecycle
}
