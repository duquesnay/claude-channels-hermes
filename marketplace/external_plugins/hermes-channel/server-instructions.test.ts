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

  // JA-24 v2: a prod incident showed a heavy-persona (~8k char CLAUDE.md)
  // haiku worker responding with plain assistant text and ZERO tool calls —
  // it never saw the v1 wording ("Simple protocol: do your work, then call
  // reply_close...") as a hard requirement, just a description. Live
  // repro on janet-test (heavy persona + --model haiku, 10 real turns)
  // reproduced the same skip at ~40% before this hardening. These tests
  // guard the strengthened wording; the actual pass/fail bar is empirical
  // (10/10 real turns calling reply_close), tracked in the mission report,
  // not by these text assertions alone.
  test('states plainly, as the opening MANDATORY line, that text/transcript output is never delivered', () => {
    const instructions = extractInstructions()
    const firstLine = instructions.split('\n').find(l => l.trim().length > 0)

    expect(firstLine).toMatch(/MANDATORY/)
    expect(firstLine!.toLowerCase()).toMatch(/never.*delivered|discarded/)
    expect(firstLine).toMatch(/reply_close/)
  })

  test('reminds the model at the very end of the instructions to confirm reply_close was called', () => {
    const instructions = extractInstructions()
    const lines = instructions.split('\n').filter(l => l.trim().length > 0)
    const lastLine = lines[lines.length - 1]

    // Recency, not just primacy — a weak model skimming a long persona
    // context may only reliably attend to the start and end of the tool
    // instructions block.
    expect(lastLine.toLowerCase()).toMatch(/reply_close/)
  })

  // JA-24 v2 round 2: 20/20 heavy-persona repro turns still skipped at ~20%
  // after the first hardening pass. Hypothesis: a heavy persona (e.g. "answer
  // completely and immediately") reads to a weak model as permission to
  // answer in plain text — a style instruction, not a delivery-mechanism one
  // — and that competes with the MCP instructions above. This test guards an
  // explicit line disarming that conflict.
  test('explicitly overrides persona/identity instructions about answering directly or immediately', () => {
    const instructions = extractInstructions()

    expect(instructions.toLowerCase()).toMatch(/persona|identity/)
    expect(instructions.toLowerCase()).toMatch(/directly|immediately/)
    expect(instructions).toMatch(/reply_close/)
  })
})

describe('hermes-channel MCP tool descriptions', () => {
  function extractToolsSourceBlock(): string {
    const source = readFileSync(join(import.meta.dir, 'server.ts'), 'utf8')
    const start = source.indexOf('mcp.setRequestHandler(ListToolsRequestSchema')
    const end = source.indexOf('// ====', start + 1)
    if (start === -1 || end === -1) throw new Error('could not locate ListTools handler block')
    return source.slice(start, end)
  }

  test('reply_close tool description says it is mandatory and the only delivery mechanism', () => {
    const block = extractToolsSourceBlock()
    const idx = block.indexOf("name: 'reply_close'")
    expect(idx).toBeGreaterThan(-1)
    // reply_close is the last tool defined in the array, so slicing to the
    // end of the block is safe — if that ordering ever changes, this test
    // will need a proper end-of-tool-object boundary instead.
    const replyCloseBlock = block.slice(idx)

    expect(replyCloseBlock.toLowerCase()).toMatch(/mandatory|required/)
    expect(replyCloseBlock.toLowerCase()).toMatch(/only way|discarded|never.*delivered/)
  })
})
