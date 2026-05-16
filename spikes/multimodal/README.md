# Spike: Multimodal notifications/claude/channel

**Time-boxed:** 2026-05-16  
**Goal:** Can `notifications/claude/channel` carry images (array content) to a `claude --channels` session?  
**Status:** Concluded — definitive negative, images cannot pass through this notification.

---

## Setup

- **Plugin-spike**: `spikes/multimodal/plugin-spike/marketplace/` — spike marketplace with `hermes-channel-spike` plugin
- **Socket**: `~/.hermes/run/hermes-channel-spike.sock` (isolated from live socket)
- **Test client**: `spike_test.py` — sends prompt + image over IPC, reads reply
- **Test image**: `test-red.png` — 64x64 red square, success criterion = Claude says "red" or "rouge"
- **Launch**: `spike-launcher.exp` — expect script with `--dangerously-load-development-channels plugin:hermes-channel-spike@hermes-channel-spike-marketplace`

---

## Variants tested

### Variant 1 — MCP-style content parts (default)

```ts
mcp.notification({
  method: 'notifications/claude/channel',
  params: {
    content: [
      { type: 'text', text: prompt },
      { type: 'image', data: b64, mimeType: 'image/png' },
    ],
    meta: { chat_id, ... }
  }
})
```

**Result: HARD FAIL**

MCP log error:
```
Connection error: Uncaught error in notification handler: $ZodError: [
  {
    "expected": "string",
    "code": "invalid_type",
    "path": ["params", "content"],
    "message": "Invalid input: expected string, received array"
  }
]
```

Claude's MCP client validates `params.content` as `string` using Zod. Sending an array
causes the validation to throw and the STDIO connection to drop (24-28s after connect).
Claude never receives the notification.

### Variant 2 — Anthropic-style content blocks

```ts
params: {
  content: [
    { type: 'text', text: prompt },
    { type: 'image', source: { type: 'base64', media_type: 'image/png', data: b64 } },
  ],
  ...
}
```

**Result: Would HARD FAIL (same reason)**

Same Zod schema on `params.content`. Any array value crashes the connection.
Not fully run but the error is structurally identical.

### Variant 3 — String content + sidecar content_parts

```ts
params: {
  content: prompt_text,          // string — Zod passes
  content_parts: [               // sidecar field — silently ignored
    { type: 'image', data: b64, mimeType: 'image/png' },
  ],
  meta: { chat_id, ... }
}
```

**Result: SOFT FAIL (not tested for images but understood)**

The string content passes Zod validation and Claude receives the text prompt. The
`content_parts` sidecar is not part of the schema — Claude's channel notification handler
reads only `params.content`. Claude would see the text but not the image.

Confirmed: text-only baseline worked perfectly (9.4s round-trip, Claude replied "BASELINE").

---

## Key findings

### 1. notifications/claude/channel enforces content: string via Zod

Claude Code v2.1.141 validates `notifications/claude/channel` params with a Zod schema
that requires `params.content` to be a `string`. Arrays are rejected with a thrown exception
that drops the STDIO connection. This is a **hard protocol boundary**.

MCP log evidence (from `mcp-logs-plugin-hermes-channel-spike-hermes-channel-spike/`):
```
"Connection error: Uncaught error in notification handler: $ZodError: 
  [{ \"expected\": \"string\", \"code\": \"invalid_type\", \"path\": [\"params\",\"content\"] }]"
```

### 2. Text-only path confirmed working

Baseline text-only (no image): Claude received the prompt, called reply_open + reply_close,
returned "BASELINE" in 9.4 seconds. The channel transport itself is healthy.

### 3. claude plugin loader runs from source dir, not cache

For directory marketplaces, `${CLAUDE_PLUGIN_ROOT}` resolves to the SOURCE directory
(the marketplace `source.path`). The plugin cache at
`~/.claude/plugins/cache/<marketplace>/<plugin>/<version>/` is used for github marketplaces.
For local directory marketplaces, Claude runs the plugin from the source.

Exception: bun has its own transpile cache that can serve stale bytecode even when the
source file is modified. `touch`-ing the file may not bust this cache.
**Workaround**: rename the file (e.g., `server-v2.ts`) to guarantee a cache miss.

### 4. .mcp.json env section not working for directory marketplaces

The `env` section in `.mcp.json` did not inject environment variables into the bun process.
Environment variables must be set by another means (e.g., `package.json` start script,
or hardcoded defaults in the server code).

---

## Conclusion

**Images cannot pass through `notifications/claude/channel` today.**

The `params.content` field is `string`-typed in Claude's Zod schema. There is no extension
point in the notification params for binary/image content.

---

## Next step recommendations

### Option A — Image URL workaround (fastest, no protocol change)

Hermes saves the image to a local temp file or serves it over a loopback HTTP server,
then sends a markdown image reference in the text prompt:

```
"Here is the image you requested: ![image](file:///tmp/hermes-image-abc123.png)\n\nWhat color is the shape?"
```

Claude Code renders markdown images in its TUI from file:// URLs (to be verified).
If confirmed, this avoids all protocol changes.

### Option B — Anthropic SDK workaround (bypass plugin transport)

Instead of using `notifications/claude/channel`, Hermes calls the Anthropic API directly
with multimodal content (using prompt caching + image blocks). Results are written back to
Slack as a bot message. This bypasses the `claude --channels` session entirely for image
messages, using the API only when an attachment is detected.

### Option C — Wait for Claude Code multimodal channel support

If Anthropic extends `notifications/claude/channel` to support `content: ContentBlock[]`,
the protocol barrier disappears. The spike plugin is ready to test immediately (Variant 1
is already coded). Subscribe to Claude Code release notes.

### Option D — In-band data URI workaround

Embed image as data URI string in the text content:
```
"data:image/png;base64,<b64data>\n\nWhat color is the shape?"
```

Claude's vision API accepts data URIs in text. Whether Claude Code's channel handler
passes this through to the model vision pipeline is untested. High risk of context bloat.

---

## Artifacts

- `plugin-spike/` — spike plugin (hermes-channel-spike) with 3 variants
- `spike_test.py` — IPC test client
- `spike-launcher.exp` — expect launcher for the spike session
- `test-red.png` — 64x64 red square test image

The plugin is registered as `hermes-channel-spike@hermes-channel-spike-marketplace`
(user scope, will appear in `claude plugin list`). Safe to uninstall when done:
```
claude plugin uninstall hermes-channel-spike@hermes-channel-spike-marketplace
claude plugin marketplace remove hermes-channel-spike-marketplace
```
