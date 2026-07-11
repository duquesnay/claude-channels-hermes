# Configuration — ACP claude/hermes sessions

How a Hermès instance (Janet, Bad Janet, janet-test) configures the `claude`
sessions the ACP supervisor spawns. Companion to [`../README.md`](../README.md)
(IPC protocol) and `launcher.exp` (the spawn command).

## Spawn chain

```
hermes gateway run                         (launchd service, per-instance HERMES_HOME)
  └─ config.yaml  model.provider: acp_client
       └─ bun run acp/src/acp_entrypoint.ts     (session pool, lazy spawn on message,
            └─ expect -f launcher.exp            terminal.lifetime_seconds recycle)
                 └─ claude --dangerously-load hermes-channel --name claude_hermes_<hash>
                            --model <cfg> [--setting-sources project,local --settings <file>]
                            --allowedTools <lean>
```

The supervisor's **own environment is the authoritative config source**. The same
launcher serves prod / test / twin instances; behaviour diverges purely by env,
never by editing the launcher or the credentials. (Same principle as the
claude.ai-connectors toggle — see memory `janet-prod-claudeai-connectors`.)

## Config surface (the only knobs)

### A. `config.yaml` (at `$HERMES_HOME/config.yaml`)

```yaml
model:
  provider: acp_client                 # spawn claude via the ACP entrypoint
  default: haiku                        # model floor (CLI --model in launcher wins)
  acp_command: /opt/homebrew/bin/bun
  acp_args: [run, <…>/acp/src/acp_entrypoint.ts]
agent:
  max_turns: 60
  reasoning_effort: medium
terminal:
  lifetime_seconds: 300                 # per-session recycle → normal PID churn
plugins:
  hermes-memory-store:                  # ← MEMORY IS CONFIG-DRIVEN (posé par config)
    auto_extract: true
    default_trust: 0.5
    hrr_dim: 1024
    temporal_decay_half_life: 7
```

There is nothing exotic beyond this: memory is just the `hermes-memory-store`
plugin block above; persona is SOUL.md; everything else is env + a settings file.

### B. Supervisor env vars (the isolation layer)

Set by the instance's startup wrapper (prod Janet: `~/.local/bin/janet-startup.sh`)
or directly in the launchd plist `EnvironmentVariables`.

| Var | Role | If unset |
|-----|------|----------|
| `HERMES_HOME` | instance root | `~/.hermes` |
| `HERMES_CLAUDE_SETTINGS_FILE` | isolated claude settings (allow-list, hermes MCP only) | **launcher else-branch: NO isolation, claude reads the `user` `~/.claude`** |
| `HERMES_SESSION_CWD` | session cwd = the persona sandbox | falls back to supervisor cwd (usually `$HOME` → `project` source = `~/.claude`, i.e. the user config again) |
| `HERMES_CLAUDE_ALLOWED_TOOLS` | `--allowedTools` (space-separated string) | launcher default `mcp__…hermes-channel__* Read` |
| `HERMES_CLAUDE_NO_ACCOUNT_CONNECTORS` | when truthy → injects `ENABLE_CLAUDEAI_MCP_SERVERS=0` (kills claude.ai Gmail/Drive/Notes/Slack connectors) | unset = connectors ON (prod default) |

**Critical coupling:** `HERMES_CLAUDE_SETTINGS_FILE` and `HERMES_SESSION_CWD` must be
set *together*. `--setting-sources project,local` (added only in the launcher's
`if` branch) excludes the `user` source, but `project` resolves against the cwd —
so if the cwd is `$HOME`, `project` = `~/.claude` = the polluted user config. The
sandbox cwd is what makes `project` clean.

### C. `$HERMES_HOME/claude-settings.json`

The isolated settings file pointed to by `HERMES_CLAUDE_SETTINGS_FILE`. Controlled
allow-list: hermes + notes MCP, `hermes-channel` plugin, `permissions.defaultMode:
auto`. Deliberately excludes the account's private plugins/connectors.

### D. `$HERMES_HOME/acp-sandbox/` (the persona sandbox)

The `HERMES_SESSION_CWD`. Reference (prod Janet) contains:
- `CLAUDE.md` — "agent identity sandbox": *"Read and embody the identity below
  (SOUL.md)"* — this is how the persona reaches the session.
- `.mcp.json` — the local hermes MCP server for this cwd.
- `.claude/` — project-scope settings for the sandbox.

### E. `$HERMES_HOME/SOUL.md` + `AGENTS.md` + `HEARTBEAT.md`

Persona (SOUL.md) + operational instructions (AGENTS.md) + heartbeat/proactive
(HEARTBEAT.md). Seeded by `hermes_cli/default_soul.py` on first run. **Single
source of truth for identity** — never duplicate the persona into a launcher's
`--append-system-prompt`.

## Reference config vs instance gap

Proven-clean reference = **prod Janet** (`ai.janet.gateway`, guillaume). Its running
env (via `janet-startup.sh`):

```
HERMES_HOME=/Users/guillaume/.hermes
HERMES_CLAUDE_SETTINGS_FILE=/Users/guillaume/.hermes/claude-settings.json
HERMES_SESSION_CWD=/Users/guillaume/.hermes/acp-sandbox
HERMES_CLAUDE_ALLOWED_TOOLS="Bash Skill WebFetch WebSearch Read Edit Write mcp__hermes__* … mcp__plugin_hermes-channel_hermes-channel__*"
ENABLE_CLAUDEAI_MCP_SERVERS=1        # connectors on (NO_ACCOUNT_CONNECTORS commented out)
```

**Bad Janet gap — RESOLVED 2026-07-11.** Was: plist ran `hermes gateway run`
directly with only `HERMES_HOME`, no wrapper, no isolation vars, no
`~/.hermes/acp-sandbox/` on gduquesnay → launcher `else` branch → ACP session read
the polluted `~/.claude` (user). Fix applied:
- Added `HERMES_CLAUDE_SETTINGS_FILE`, `HERMES_SESSION_CWD`, `HERMES_CLAUDE_ALLOWED_TOOLS`
  (mirror prod exact) to `EnvironmentVariables` in `ai.badjanet.gateway.plist`
  (backup `.bak-pre-acp-isolation-20260711`, `plutil -lint` OK).
- Created `/Users/gduquesnay/.hermes/acp-sandbox/` composed from **Bad Janet's own**
  `SOUL.md`+`AGENTS.md` (NOT cloned from prod — prod's sandbox CLAUDE.md carries Good
  Janet's identity + auto-injected memory; cloning would re-leak Janet into Bad Janet).
  Files: `CLAUDE.md` (persona inline + `HERMES-MEMORY` marker), `.mcp.json` (hermes MCP,
  gduquesnay venv), `.claude/settings.local.json`.
- Reloaded via `launchctl bootout` + `bootstrap` (env changes need it, not kickstart).
  Gateway env verified live with gduquesnay isolation paths.

Deferred (one change at a time): `HERMES_CLAUDE_ALLOWED_TOOLS` mirrors prod exactly, so
it lists Janet's personal-world tools (gws, apple-mail, slack-admin) — inert since the
isolated `claude-settings.json` doesn't enable those plugins. Wiring the Mantu toolset
(m365) is a separate tested step: `allowedTools` is auto-approval, not plugin
enablement — m365 must go into the isolated settings' `enabledPlugins` + marketplace.

Pending: empirical verification on the next lazily-spawned Bad Janet session (confirm
the claude cmdline carries `--settings` + `--setting-sources project,local` + cwd=sandbox
and does not read `~/.claude`).

## Prod-safety

Reload/kill scoped by `HERMES_HOME` / `--name claude_hermes_<hash>` only. Never a
broad `pkill -f 'hermes gateway'` (kills prod Janet too). See `~/dev/nestor/CLAUDE.md`.
