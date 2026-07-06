import { describe, test, expect } from 'bun:test'
import { findCurrentTurn, extractFinalText } from './stop_hook_fallback.ts'

// JA-24 v2 stage 4 spike: these fixtures mirror the REAL transcript schema
// confirmed by inspecting a live Claude Code session transcript (JSONL, one
// content-block per line, chained via parentUuid, user content is either a
// plain string or an array of tool_result blocks). The one thing NOT yet
// empirically confirmed (open spike question, see stop_hook_fallback.ts's
// top comment): whether a real --channels worker's transcript renders the
// inbound MCP notification as this exact `<channel ... chat_id="...">`
// string. These tests validate the extraction logic against that assumed
// shape; a live turn is still needed to confirm the assumption itself.

function userLine(content: string) {
  return JSON.stringify({ type: 'user', message: { role: 'user', content } })
}

function assistantTextLine(text: string) {
  return JSON.stringify({ type: 'assistant', message: { role: 'assistant', content: [{ type: 'text', text }] } })
}

function assistantToolUseLine(name: string) {
  return JSON.stringify({ type: 'assistant', message: { role: 'assistant', content: [{ type: 'tool_use', name }] } })
}

function assistantThinkingLine(text: string) {
  return JSON.stringify({ type: 'assistant', message: { role: 'assistant', content: [{ type: 'thinking', text }] } })
}

describe('findCurrentTurn', () => {
  test('finds the chat_id from the most recent hermes-channel inbound tag', () => {
    const lines = [
      userLine('<channel source="hermes" chat_id="old-turn" user="hermes" ts="1">first message</channel>'),
      assistantTextLine('first reply'),
      userLine('<channel source="hermes" chat_id="current-turn" user="hermes" ts="2">second message</channel>'),
    ]

    const result = findCurrentTurn(lines)

    expect(result).not.toBeNull()
    expect(result!.chatId).toBe('current-turn')
    expect(result!.turnStartIndex).toBe(2)
  })

  test('ignores unrelated user turns (plain conversation, tool_result arrays) and returns null if no channel tag exists', () => {
    const lines = [
      userLine('just a normal message, not a channel turn'),
      JSON.stringify({ type: 'user', message: { role: 'user', content: [{ type: 'tool_result', content: 'some tool output' }] } }),
      assistantTextLine('a reply'),
    ]

    expect(findCurrentTurn(lines)).toBeNull()
  })

  test('tolerates malformed JSON lines without throwing', () => {
    const lines = [
      'not valid json at all',
      userLine('<channel source="hermes" chat_id="chat-x" user="hermes" ts="1">hi</channel>'),
    ]

    const result = findCurrentTurn(lines)
    expect(result!.chatId).toBe('chat-x')
  })
})

describe('extractFinalText', () => {
  test('concatenates all text blocks after the turn start, skipping tool_use/thinking blocks', () => {
    const lines = [
      userLine('<channel source="hermes" chat_id="chat-1" user="hermes" ts="1">question</channel>'),
      assistantThinkingLine('let me think about this'),
      assistantToolUseLine('Read'),
      assistantTextLine('Based on what I found, the answer is 42.'),
    ]

    const text = extractFinalText(lines, 0)

    expect(text).toBe('Based on what I found, the answer is 42.')
  })

  test('joins multiple text segments in order when the model produced several', () => {
    const lines = [
      userLine('<channel source="hermes" chat_id="chat-2" user="hermes" ts="1">question</channel>'),
      assistantTextLine('First part of the answer.'),
      assistantToolUseLine('WebSearch'),
      assistantTextLine('Second part, after the search.'),
    ]

    const text = extractFinalText(lines, 0)

    expect(text).toBe('First part of the answer.\n\nSecond part, after the search.')
  })

  test('returns empty string when the turn has no text blocks at all (pure tool-call turn)', () => {
    const lines = [
      userLine('<channel source="hermes" chat_id="chat-3" user="hermes" ts="1">question</channel>'),
      assistantToolUseLine('Bash'),
    ]

    expect(extractFinalText(lines, 0)).toBe('')
  })

  test('only considers assistant lines AFTER turnStartIndex, not earlier turns', () => {
    const lines = [
      assistantTextLine('stale answer from a previous turn — must NOT be picked up'),
      userLine('<channel source="hermes" chat_id="chat-4" user="hermes" ts="1">question</channel>'),
      assistantTextLine('the real current-turn answer'),
    ]

    const text = extractFinalText(lines, 1)

    expect(text).toBe('the real current-turn answer')
  })
})
