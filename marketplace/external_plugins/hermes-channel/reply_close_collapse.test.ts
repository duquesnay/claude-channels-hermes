import { describe, test, expect, beforeEach } from 'bun:test'
import {
  pendingByRequestId,
  streams,
  handleReplyOpen,
  handleReplyChunk,
  handleReplyClose,
} from './server.ts'

// JA-24 step 2: reply_close can close directly by chat_id, skipping reply_open
// (the "tax" measured in step 1 — median ~3-6s of empty first-model-call
// round-trip before any real work starts). reply_open remains available for
// the advanced reply_chunk-progress-updates case (BUG-1 delegation path).

function fakeConn() {
  const writes: string[] = []
  return {
    writes,
    write: (data: string) => {
      writes.push(data)
    },
  }
}

beforeEach(() => {
  pendingByRequestId.clear()
  streams.clear()
})

describe('reply_close by chat_id (collapsed protocol)', () => {
  test('closes directly with chat_id, no prior reply_open', async () => {
    const conn = fakeConn()
    pendingByRequestId.set('chat-1', { conn: conn as any, startedAt: Date.now() - 500 })

    const result = await handleReplyClose({ chat_id: 'chat-1', text: 'final answer' })

    expect(result.content[0].text).toBe('closed')
    expect(conn.writes).toHaveLength(1)
    const payload = JSON.parse(conn.writes[0])
    expect(payload).toMatchObject({
      type: 'result',
      request_id: 'chat-1',
      content: 'final answer',
    })
    expect(payload.duration_ms).toBeGreaterThanOrEqual(0)
    // Pending entry is consumed — a second close (by either path) must fail.
    expect(pendingByRequestId.has('chat-1')).toBe(false)
  })

  test('rejects an unknown chat_id', async () => {
    await expect(handleReplyClose({ chat_id: 'no-such-chat', text: 'x' })).rejects.toThrow(
      /no pending IPC request for chat_id=no-such-chat/,
    )
  })

  test('rejects when neither chat_id nor handle is passed', async () => {
    await expect(handleReplyClose({ text: 'x' })).rejects.toThrow(
      /must pass either 'chat_id'.*or 'handle'/,
    )
  })

  test('rejects a missing text on the chat_id fast path instead of sending a silent empty reply (JA-25 class)', async () => {
    const conn = fakeConn()
    pendingByRequestId.set('chat-2', { conn: conn as any, startedAt: Date.now() })

    await expect(handleReplyClose({ chat_id: 'chat-2' })).rejects.toThrow(
      /text is required when closing by chat_id/,
    )

    // Rejected before consuming the pending entry — a retry with proper text
    // must still be possible, not permanently locked out.
    expect(conn.writes).toHaveLength(0)
    expect(pendingByRequestId.has('chat-2')).toBe(true)
  })

  test('rejects an explicit empty-string text on the chat_id fast path too', async () => {
    const conn = fakeConn()
    pendingByRequestId.set('chat-2b', { conn: conn as any, startedAt: Date.now() })

    await expect(handleReplyClose({ chat_id: 'chat-2b', text: '' })).rejects.toThrow(
      /text is required when closing by chat_id/,
    )
    expect(pendingByRequestId.has('chat-2b')).toBe(true)
  })

  test('retry after the missing-text rejection succeeds', async () => {
    const conn = fakeConn()
    pendingByRequestId.set('chat-2c', { conn: conn as any, startedAt: Date.now() })

    await expect(handleReplyClose({ chat_id: 'chat-2c' })).rejects.toThrow()
    await handleReplyClose({ chat_id: 'chat-2c', text: 'retried answer' })

    const payload = JSON.parse(conn.writes[0])
    expect(payload.content).toBe('retried answer')
    expect(pendingByRequestId.has('chat-2c')).toBe(false)
  })

  test('after reply_open already consumed the chat_id, closing by chat_id fails with a guiding error', async () => {
    const conn = fakeConn()
    pendingByRequestId.set('chat-3', { conn: conn as any, startedAt: Date.now() })

    await handleReplyOpen({ chat_id: 'chat-3' })

    await expect(handleReplyClose({ chat_id: 'chat-3', text: 'too late' })).rejects.toThrow(
      /already closed, or reply_open already consumed it/,
    )
  })
})

describe('reply_open + reply_close by handle (advanced path, still supported)', () => {
  test('reply_open then reply_chunk then reply_close(handle) still works end-to-end', async () => {
    const conn = fakeConn()
    pendingByRequestId.set('chat-4', { conn: conn as any, startedAt: Date.now() - 100 })

    const openResult = await handleReplyOpen({ chat_id: 'chat-4' })
    const handle = openResult.content[0].text.replace('handle=', '')

    await handleReplyChunk({ handle, text: 'partial...' })
    const closeResult = await handleReplyClose({ handle, text: 'final via handle' })

    expect(closeResult.content[0].text).toBe('closed')
    const payload = JSON.parse(conn.writes[0])
    expect(payload).toMatchObject({ request_id: 'chat-4', content: 'final via handle' })
    expect(streams.has(handle)).toBe(false)
  })

  test('reply_close(handle) with no text override falls back to the last reply_chunk text', async () => {
    const conn = fakeConn()
    pendingByRequestId.set('chat-5', { conn: conn as any, startedAt: Date.now() })

    const openResult = await handleReplyOpen({ chat_id: 'chat-5' })
    const handle = openResult.content[0].text.replace('handle=', '')
    await handleReplyChunk({ handle, text: 'accumulated so far' })

    await handleReplyClose({ handle })

    const payload = JSON.parse(conn.writes[0])
    expect(payload.content).toBe('accumulated so far')
  })
})
