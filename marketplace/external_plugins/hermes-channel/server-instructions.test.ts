import { describe, test, expect } from 'bun:test'
import { readFileSync } from 'fs'
import { join } from 'path'

// BUG-1: async delegation (Agent tool / background Bash) launched mid-turn
// left the ACP turn open forever because the instructions never told Claude
// to call reply_close before returning control while background work runs.
// Hermes then times out at 120s and delivers an empty response.
//
// This test asserts the MCP server instructions carry an explicit, imperative
// rule covering that case. It reads the source directly (server.ts is now
// safe to import too, guarded by import.meta.main — see reply_close_collapse
// .test.ts for behavioral tests — but this file stays a pure text assertion
// on the exact instructions string Claude sees, independent of module
// execution).
//
// JA-24: the protocol was collapsed so reply_close(chat_id, text) works
// directly, without a mandatory reply_open first (that first call measured
// at a median ~3-6s "tax" of empty-handed model round-trip before any real
// work — see project memory project_ja24_reply_open_collapse_blocked /
// the JA-24 mission report). reply_open remains available, opt-in, for the
// reply_chunk progress-updates case. The tests below guard both halves of
// that change: the new simplified path is documented, and the old
// mandatory-reply_open-first wording is gone (a regression there would
// silently re-introduce the tax for every turn).

function extractInstructions(): string {
  const source = readFileSync(join(import.meta.dir, 'server.ts'), 'utf8')
  const match = source.match(/instructions:\s*\[([\s\S]*?)\]\.join\('\\n'\)/)
  if (!match) throw new Error('could not locate instructions array in server.ts')
  // Evaluate the array of string literals to get the real joined text.
  // eslint-disable-next-line no-new-func
  const lines = new Function(`return [${match[1]}]`)() as string[]
  return lines.join('\n')
}

describe('hermes-channel MCP instructions', () => {
  test('tells Claude to close the turn before background/async work runs', () => {
    const instructions = extractInstructions()

    expect(instructions).toMatch(/reply_close/)
    expect(instructions.toLowerCase()).toMatch(/background|async/)
    // Must be an imperative rule, not just a description of the tool.
    expect(instructions).toMatch(/MUST/)
    // Must explicitly warn against leaving the turn open / timeout.
    expect(instructions.toLowerCase()).toMatch(/timeout|empty response/)
  })

  test('documents reply_close(chat_id, text) as usable without reply_open first (JA-24 collapse)', () => {
    const instructions = extractInstructions()

    expect(instructions).toMatch(/reply_close\(chat_id/)
    expect(instructions.toLowerCase()).toMatch(/no reply_open call needed|without.*reply_open|reply_open.*optional|optional.*reply_open/)
  })

  test('no longer mandates reply_open as the very first tool call (JA-24 collapse)', () => {
    const instructions = extractInstructions()

    // The old wording forced a reply_open round-trip before ANY other tool
    // call, on every single turn — that round-trip is exactly the "tax"
    // JA-24 measured and removed. If this phrase ever comes back, the tax
    // comes back with it.
    expect(instructions).not.toMatch(/reply_open MUST be your very first tool call/)
  })
})
