import { describe, test, expect, beforeEach } from 'bun:test'
import {
  pendingByRequestId,
  streams,
  handleReplyOpen,
  handleReplyClose,
  handleReplyChunk,
} from './server'

// V2 progressive streaming: reply_chunk relays deltas over the IPC socket as
// {type:"chunk", request_id, content:<delta>} so the supervisor emits
// agent_message_chunk session/update and Hermes renders progress live. The tool
// contract is "pass FULL accumulated text each time" — the plugin diffs against
// what it has already sent and emits only the new suffix (the client expects
// deltas; acp_server reconciles the final tail from the result message).

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

async function openStream(chat_id: string, conn: ReturnType<typeof fakeConn>) {
  pendingByRequestId.set(chat_id, {
    conn: conn as any,
    startedAt: Date.now(),
    guardTimer: dummyGuardTimer(),
  })
  const res = await handleReplyOpen({ chat_id })
  const handle = (res.content[0].text as string).replace('handle=', '')
  return handle
}

beforeEach(() => {
  pendingByRequestId.clear()
  streams.clear()
})

describe('reply_chunk V2 streaming (delta relay over IPC)', () => {
  test('emits a chunk message carrying the delta over the socket', async () => {
    const conn = fakeConn()
    const handle = await openStream('chat-s1', conn)

    await handleReplyChunk({ handle, text: 'Hello' })

    expect(conn.writes).toHaveLength(1)
    const payload = JSON.parse(conn.writes[0])
    expect(payload).toMatchObject({
      type: 'chunk',
      request_id: 'chat-s1',
      content: 'Hello',
    })
  })

  test('successive calls with cumulative text emit only the new suffix', async () => {
    const conn = fakeConn()
    const handle = await openStream('chat-s2', conn)

    await handleReplyChunk({ handle, text: 'Hello' })
    await handleReplyChunk({ handle, text: 'Hello, world' })

    expect(conn.writes).toHaveLength(2)
    expect(JSON.parse(conn.writes[0]).content).toBe('Hello')
    expect(JSON.parse(conn.writes[1]).content).toBe(', world')
  })

  test('a repeated call with no new content writes nothing', async () => {
    const conn = fakeConn()
    const handle = await openStream('chat-s3', conn)

    await handleReplyChunk({ handle, text: 'same' })
    await handleReplyChunk({ handle, text: 'same' })

    expect(conn.writes).toHaveLength(1)
  })

  test('each chunk message is newline-terminated JSONL', async () => {
    const conn = fakeConn()
    const handle = await openStream('chat-s4', conn)

    await handleReplyChunk({ handle, text: 'x' })

    expect(conn.writes[0].endsWith('\n')).toBe(true)
  })

  test('reply_close after streaming still sends the full final result', async () => {
    const conn = fakeConn()
    const handle = await openStream('chat-s5', conn)

    await handleReplyChunk({ handle, text: 'Progress...' })
    await handleReplyClose({ handle, text: 'Progress... done' })

    // last write is the terminal result carrying the complete text
    const last = JSON.parse(conn.writes[conn.writes.length - 1])
    expect(last).toMatchObject({ type: 'result', request_id: 'chat-s5', content: 'Progress... done' })
  })

  test('unknown handle still throws', async () => {
    await expect(handleReplyChunk({ handle: 'nope', text: 'x' })).rejects.toThrow('unknown handle')
  })
})
