# claude-channels-hermes

MCP plugin bridging the Hermes daemon to a `claude --channels` session.

Hermes sends prompts over a Unix socket; the plugin injects them as channel notifications to Claude; Claude calls `reply_open` / `reply_chunk` / `reply_close`; the plugin routes the final reply back to Hermes on the same socket.

## Structure

```
marketplace/
  .claude-plugin/marketplace.json
  external_plugins/hermes-channel/
    server.ts             # plugin entry point
    package.json
    .mcp.json             # MCP server config for plugin loader
    .claude-plugin/plugin.json
```

## Install (one-time)

```bash
claude plugin marketplace add ~/dev/nestor/claude-channels-hermes/marketplace
claude plugin install hermes-channel@claude-channels-hermes
```

## Run locally (smoke test)

```bash
# Terminal 1 — launch Claude with the plugin
claude --dangerously-load-development-channels \
  --channels plugin:hermes-channel@claude-channels-hermes \
  --permission-mode bypassPermissions \
  --allowedTools "mcp__plugin_hermes_channel_hermes_channel__*"

# Terminal 2 — echo mode (no Claude needed)
HERMES_CHANNEL_TEST_MODE=1 bun run marketplace/external_plugins/hermes-channel/server.ts
```

## Smoke test: echo mode (IPC protocol)

```bash
# Start the plugin in echo mode
HERMES_CHANNEL_TEST_MODE=1 bun marketplace/external_plugins/hermes-channel/server.ts &

# Send a test prompt
printf '{"type":"prompt","request_id":"test-001","content":"hello hermes","timeout_ms":30000}\n' \
  | nc -U ~/.hermes/run/hermes-channel.sock

# Expected response:
# {"type":"result","request_id":"test-001","content":"echo: hello hermes","duration_ms":0}
```

## IPC protocol (JSONL, bidirectional on same socket)

Hermes → plugin:
```json
{"type":"prompt","request_id":"<uuid>","content":"<full prompt>","timeout_ms":180000}
```

Plugin → Hermes (success):
```json
{"type":"result","request_id":"<uuid>","content":"<text>","duration_ms":<int>}
```

Plugin → Hermes (error):
```json
{"type":"error","request_id":"<uuid>","error":"<msg>"}
```

## Socket

`~/.hermes/run/hermes-channel.sock` (0600, owner guillaume).
Override with `HERMES_CHANNEL_SOCKET` env var.
