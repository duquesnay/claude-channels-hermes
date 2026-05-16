# Spike B — Inline base64 image in text prompt

**Question**: Can `claude --channels` see an image when its base64 is inlined as a
data URL in the prompt text content?

**Test image**: `test-red.png` — 64x64 px, solid red (R=200 G=30 B=30), 178 bytes, 240 chars b64.

**Success criterion**: Claude replies "red" without any hint beyond the image data.

---

## Protocol used

Socket: `~/.hermes/run/hermes-channel.sock`
Format: `{"type":"prompt","request_id":"<uuid>","content":"<text>"}\n`
Prompt: `"What color is the dominant pixel in this image? Respond with just the color name in English.\n\n<variant>"`

The `content` string is forwarded verbatim to Claude as a `notifications/claude/channel` MCP notification.

---

## Variants tested

| # | Format | Response | Result |
|---|--------|----------|--------|
| 1 | `![image](data:image/png;base64,XXX)` | "white" | FAIL |
| 2 | `data:image/png;base64,XXX` (bare line) | "white" | FAIL |
| 3 | `<data:image/png;base64,XXX>` | "white" | FAIL |
| 4 | `` ```\ndata:image/png;base64,XXX\n``` `` | "white" | FAIL |
| 5 | `Image encoded as base64: data:image/png;base64,XXX` | "white" | FAIL |
| 6 | `XXX` (raw b64 string, no prefix) | "white" | FAIL |

---

## Conclusion

**NEGATIVE — no text format works.**

All 6 variants return "white" (a hallucination), meaning Claude receives the text
but cannot decode/render the embedded image data. The consistent "white" response
(not "red") rules out lucky guessing — it confirms Claude sees opaque text, not
an image.

This is expected: the MCP `notifications/claude/channel` protocol carries a plain
`content: str` field. The Claude API's vision capability requires image data to be
sent as a structured `image` content block with `type: "image"`, `source.type: "base64"`,
`source.media_type`, and `source.data` fields — not embedded in markdown or text.

## Required fix

The `hermes-channel` plugin must be extended to carry multimodal content. Two paths:

**Option A — Structured content in IPC** (preferred):
- Hermes sends `content_blocks: [{type:"image", media_type, data}]` alongside `content: str`
- Plugin passes `image` blocks to Claude via a richer notification format or tool response
- Requires changes to: Hermes shim, IPC schema, plugin notification handling, Claude prompt construction

**Option B — File reference + Read tool**:
- Hermes saves image to a temp file, sends path in prompt
- Claude uses `Read` tool to access the image bytes natively
- Simpler but adds latency and temp file management

## Winner

None. The inline base64-in-text approach is a dead end — do not implement it.
