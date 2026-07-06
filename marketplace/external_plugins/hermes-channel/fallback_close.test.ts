import { describe, test, expect, beforeEach } from 'bun:test'
import {
  pendingByRequestId,
  streams,
  handleReplyOpen,
  handleReplyClose,
  handleFallbackClose,
  armReplyCloseGuard,
} from './server.ts'

// JA-24 v2 stage 4: Stop-hook fallback. When a model turn ends (Stop hook
// fires) without ever calling reply_open or reply_close — the exact prod
// failure mode, ~10% residual even after hardening — a Stop hook running
// outside the model's own turn extracts the model's last assistant text
// from the transcript and sends it here as a LAST-RESORT delivery path,
// so the user gets the actual answer instead of the anti-wedge guard's
// bare error message.
//
// The plugin, not the hook, arbitrates: it holds pendingByRequestId (and
// streams, for the advanced reply_open path) as the single source of truth
// for "has this request already been closed?" — reusing the exact same
// invariant enforcement as the reply_close double-close guard, so a
// fallback_close can NEVER produce a second result for a request_id that a
// tool call already closed (or that the anti-wedge guard already fired for).

function fakeConn() {
  const writes: string[] = []
  return {
    writes,
    write: (data: string) => {
      writes.push(data)
    },
  }
}

function dummyGuardTimer(): ReturnType<typeof setTimeout> {
  return setTimeout(() => {}, 999_000)
}

beforeEach(() => {
  pendingByRequestId.clear()
  streams.clear()
})

describe('fallback_close (Stop-hook last-resort delivery)', () => {
  test('delivers content when the request is still pending (neither reply_open nor reply_close called)', () => {
    const hermesConn = fakeConn()
    const hookConn = fakeConn()
    pendingByRequestId.set('chat-fb-1', {
      conn: hermesConn as any,
      startedAt: Date.now() - 200,
      guardTimer: dummyGuardTimer(),
    })

    handleFallbackClose({ request_id: 'chat-fb-1', content: 'the answer the model produced' }, hookConn as any)

    // Delivered to HERMES's original connection, not the hook's own.
    expect(hermesConn.writes).toHaveLength(1)
    const payload = JSON.parse(hermesConn.writes[0])
    expect(payload).toMatchObject({
      type: 'result',
      request_id: 'chat-fb-1',
      content: 'the answer the model produced',
    })
    expect(pendingByRequestId.has('chat-fb-1')).toBe(false)

    // Hook gets an ack on its own connection so it can log/exit cleanly.
    expect(hookConn.writes).toHaveLength(1)
    expect(JSON.parse(hookConn.writes[0])).toMatchObject({ type: 'fallback_close_ack', delivered: true })
  })

  test('is a no-op if reply_close already delivered the result (never double-writes)', () => {
    const hermesConn = fakeConn()
    const hookConn = fakeConn()
    pendingByRequestId.set('chat-fb-2', {
      conn: hermesConn as any,
      startedAt: Date.now(),
      guardTimer: dummyGuardTimer(),
    })

    handleReplyClose({ chat_id: 'chat-fb-2', text: 'delivered via tool call' })
    handleFallbackClose({ request_id: 'chat-fb-2', content: 'stale fallback text' }, hookConn as any)

    // Only the real reply_close's write exists — fallback added nothing.
    expect(hermesConn.writes).toHaveLength(1)
    expect(JSON.parse(hermesConn.writes[0]).content).toBe('delivered via tool call')
    expect(JSON.parse(hookConn.writes[0])).toMatchObject({ type: 'fallback_close_ack', delivered: false })
  })

  test('is a no-op if the anti-wedge guard already fired for this request', () => {
    const hermesConn = fakeConn()
    const hookConn = fakeConn()
    // Simulate the guard having already fired: entry removed, error already sent.
    pendingByRequestId.set('chat-fb-3', {
      conn: hermesConn as any,
      startedAt: Date.now(),
      guardTimer: dummyGuardTimer(),
    })
    pendingByRequestId.delete('chat-fb-3') // guard's own cleanup step

    handleFallbackClose({ request_id: 'chat-fb-3', content: 'too late' }, hookConn as any)

    expect(hermesConn.writes).toHaveLength(0)
    expect(JSON.parse(hookConn.writes[0])).toMatchObject({ type: 'fallback_close_ack', delivered: false })
  })

  test('rejects empty content instead of silently delivering an empty answer (JA-25 class)', () => {
    const hermesConn = fakeConn()
    const hookConn = fakeConn()
    pendingByRequestId.set('chat-fb-4', {
      conn: hermesConn as any,
      startedAt: Date.now(),
      guardTimer: dummyGuardTimer(),
    })

    handleFallbackClose({ request_id: 'chat-fb-4', content: '' }, hookConn as any)

    expect(hermesConn.writes).toHaveLength(0)
    // The pending entry must survive so a LEGITIMATE subsequent close (tool
    // call or a corrected fallback retry) can still succeed.
    expect(pendingByRequestId.has('chat-fb-4')).toBe(true)
    const ack = JSON.parse(hookConn.writes[0])
    expect(ack.delivered).toBe(false)
    expect(ack.error).toMatch(/content/)
  })

  test('delivers via the advanced path too — reply_open was called but reply_close never was', () => {
    const hermesConn = fakeConn()
    const hookConn = fakeConn()
    pendingByRequestId.set('chat-fb-5', {
      conn: hermesConn as any,
      startedAt: Date.now() - 300,
      guardTimer: dummyGuardTimer(),
    })

    handleReplyOpen({ chat_id: 'chat-fb-5' }) // moves the entry into `streams`, keyed by handle
    expect(pendingByRequestId.has('chat-fb-5')).toBe(false)
    expect(streams.size).toBe(1)

    handleFallbackClose({ request_id: 'chat-fb-5', content: 'delivered via the advanced-path fallback' }, hookConn as any)

    expect(hermesConn.writes).toHaveLength(1)
    const payload = JSON.parse(hermesConn.writes[0])
    expect(payload).toMatchObject({ request_id: 'chat-fb-5', content: 'delivered via the advanced-path fallback' })
    expect(streams.size).toBe(0)
    expect(JSON.parse(hookConn.writes[0])).toMatchObject({ type: 'fallback_close_ack', delivered: true })
  })

  test('unknown request_id is a clean no-op, not a crash', () => {
    const hookConn = fakeConn()

    handleFallbackClose({ request_id: 'no-such-request', content: 'anything' }, hookConn as any)

    expect(JSON.parse(hookConn.writes[0])).toMatchObject({ type: 'fallback_close_ack', delivered: false })
  })

  test('clears the anti-wedge guard timer on delivery so it never fires after a successful fallback', () => {
    const hermesConn = fakeConn()
    const hookConn = fakeConn()
    process.env.HERMES_REPLY_CLOSE_GUARD_MS = '20'
    const guardTimer = armReplyCloseGuard('chat-fb-6')
    pendingByRequestId.set('chat-fb-6', { conn: hermesConn as any, startedAt: Date.now(), guardTimer })

    handleFallbackClose({ request_id: 'chat-fb-6', content: 'delivered before guard would fire' }, hookConn as any)

    return new Promise(resolve => {
      setTimeout(() => {
        // If the guard had fired too, there would be a SECOND write (the
        // error) on hermesConn — asserting length 1 proves it didn't.
        expect(hermesConn.writes).toHaveLength(1)
        delete process.env.HERMES_REPLY_CLOSE_GUARD_MS
        resolve(undefined)
      }, 50)
    })
  })
})
