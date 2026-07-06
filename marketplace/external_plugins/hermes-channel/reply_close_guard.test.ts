import { describe, test, expect, beforeEach, afterEach } from 'bun:test'
import {
  pendingByRequestId,
  streams,
  handleReplyOpen,
  handleReplyClose,
  armReplyCloseGuard,
  replyCloseGuardMs,
} from './server.ts'

// JA-24 v2 step 3: anti-wedge net. Prod incident + janet-test repro showed a
// weak (haiku) worker under a heavy persona sometimes calls NEITHER
// reply_open NOR reply_close — plain assistant text, zero tool calls. Without
// this guard, the pending IPC request sits forever: Hermes' sendPrompt is
// still waiting on a `result` line for that request_id, and the only thing
// that eventually unblocks it is the much coarser gateway-level timeout
// (JA-27, 210-300s), during which the turn is fully wedged.
//
// The guard fires HERMES_REPLY_CLOSE_GUARD_MS (default 90s) after an inbound
// prompt is registered, IF NEITHER reply_open NOR reply_close has claimed it
// by then — writing an explicit `error` result so sendPrompt fails fast and
// cleanly instead of wedging. It must never fire after a normal close/open
// (both clear it), and must never cause a second result to be written for
// the same request_id (the pre-existing one-result-per-request_id invariant,
// already enforced by pendingByRequestId being the single source of truth).

function fakeConn() {
  const writes: string[] = []
  return {
    writes,
    write: (data: string) => {
      writes.push(data)
    },
  }
}

// Registers a pending request the same way handleIpcLine does (conn +
// startedAt + an armed guard timer), without needing the full IPC/MCP
// notification machinery — keeps these tests focused on the guard itself.
function registerPending(request_id: string, conn: any) {
  const guardTimer = armReplyCloseGuard(request_id)
  pendingByRequestId.set(request_id, { conn: conn as any, startedAt: Date.now(), guardTimer })
}

const ORIGINAL_GUARD_MS = process.env.HERMES_REPLY_CLOSE_GUARD_MS

beforeEach(() => {
  pendingByRequestId.clear()
  streams.clear()
})

afterEach(() => {
  if (ORIGINAL_GUARD_MS === undefined) delete process.env.HERMES_REPLY_CLOSE_GUARD_MS
  else process.env.HERMES_REPLY_CLOSE_GUARD_MS = ORIGINAL_GUARD_MS
})

describe('replyCloseGuardMs', () => {
  test('defaults to 90000ms when HERMES_REPLY_CLOSE_GUARD_MS is unset', () => {
    delete process.env.HERMES_REPLY_CLOSE_GUARD_MS
    expect(replyCloseGuardMs()).toBe(90_000)
  })

  test('respects HERMES_REPLY_CLOSE_GUARD_MS override', () => {
    process.env.HERMES_REPLY_CLOSE_GUARD_MS = '1234'
    expect(replyCloseGuardMs()).toBe(1234)
  })
})

describe('reply_close anti-wedge guard', () => {
  test('fires and writes an explicit error result if neither reply_open nor reply_close happens in time', async () => {
    process.env.HERMES_REPLY_CLOSE_GUARD_MS = '20'
    const conn = fakeConn()
    registerPending('chat-guard-1', conn)

    await new Promise(r => setTimeout(r, 70))

    expect(conn.writes).toHaveLength(1)
    const payload = JSON.parse(conn.writes[0])
    expect(payload.type).toBe('error')
    expect(payload.request_id).toBe('chat-guard-1')
    expect(payload.error).toMatch(/reply_close/)
    // The entry must be consumed so a late reply_open/reply_close attempt
    // fails cleanly instead of writing a second result.
    expect(pendingByRequestId.has('chat-guard-1')).toBe(false)
  })

  test('does not fire if reply_close(chat_id) happens before the deadline', async () => {
    process.env.HERMES_REPLY_CLOSE_GUARD_MS = '40'
    const conn = fakeConn()
    registerPending('chat-guard-2', conn)

    await handleReplyClose({ chat_id: 'chat-guard-2', text: 'done in time' })
    await new Promise(r => setTimeout(r, 70)) // past the original deadline

    expect(conn.writes).toHaveLength(1) // only the real close, no guard error
    const payload = JSON.parse(conn.writes[0])
    expect(payload.content).toBe('done in time')
  })

  test('does not fire if reply_open happens before the deadline', async () => {
    process.env.HERMES_REPLY_CLOSE_GUARD_MS = '40'
    const conn = fakeConn()
    registerPending('chat-guard-3', conn)

    await handleReplyOpen({ chat_id: 'chat-guard-3' })
    await new Promise(r => setTimeout(r, 70))

    // reply_open itself never writes to the IPC conn, and the guard must not
    // have fired either.
    expect(conn.writes).toHaveLength(0)
  })

  test('a rejected (missing-text) reply_close attempt does not disarm the guard', async () => {
    process.env.HERMES_REPLY_CLOSE_GUARD_MS = '30'
    const conn = fakeConn()
    registerPending('chat-guard-4', conn)

    await expect(handleReplyClose({ chat_id: 'chat-guard-4' })).rejects.toThrow()
    await new Promise(r => setTimeout(r, 60))

    // The pending entry survived the rejected attempt (existing JA-24
    // behavior), so the guard is still the one live timer tracking it and
    // must still fire.
    expect(conn.writes).toHaveLength(1)
    expect(JSON.parse(conn.writes[0]).type).toBe('error')
  })

  test('a retried reply_close after the missing-text rejection disarms the guard', async () => {
    process.env.HERMES_REPLY_CLOSE_GUARD_MS = '40'
    const conn = fakeConn()
    registerPending('chat-guard-5', conn)

    await expect(handleReplyClose({ chat_id: 'chat-guard-5' })).rejects.toThrow()
    await handleReplyClose({ chat_id: 'chat-guard-5', text: 'retried in time' })
    await new Promise(r => setTimeout(r, 70))

    expect(conn.writes).toHaveLength(1)
    expect(JSON.parse(conn.writes[0]).content).toBe('retried in time')
  })

  test('a late reply_close attempt after the guard already fired fails cleanly (no second write)', async () => {
    process.env.HERMES_REPLY_CLOSE_GUARD_MS = '20'
    const conn = fakeConn()
    registerPending('chat-guard-6', conn)

    await new Promise(r => setTimeout(r, 60)) // guard fires, writes the error
    await expect(handleReplyClose({ chat_id: 'chat-guard-6', text: 'too late' })).rejects.toThrow(
      /no pending IPC request/,
    )

    // Still exactly one write total (the guard's error) — never two results
    // for the same request_id.
    expect(conn.writes).toHaveLength(1)
    expect(JSON.parse(conn.writes[0]).type).toBe('error')
  })
})
