#!/usr/bin/env bash
# deploy-manifest.sh — the single list of files each deploy target receives.
#
# Sourced by deploy-acp.sh, deploy-plugin.sh and check-drift.sh. It exists so
# the deploy and the drift check can never disagree about WHAT is deployed: a
# checker carrying its own copy of the file list silently stops covering
# anything a later deploy starts shipping.
#
# Order matters to rsync — includes first, then the catch-all exclude.

ACP_SRC_SUBDIR="acp"
ACP_RSYNC_FILTER=(
  --include='/src/***'
  --include='/launcher.exp'
  --include='/package.json'
  --include='/bun.lock'
  --include='/tsconfig.json'
  --exclude='*'
)

PLUGIN_SRC_SUBDIR="marketplace/external_plugins/hermes-channel"
PLUGIN_RSYNC_FILTER=(
  --include='/server.ts'
  --include='/stop_hook_fallback.ts'
  --include='/.mcp.json'
  --include='/.claude-plugin/***'
  --include='/package.json'
  --include='/bun.lock'
  --include='/*.test.ts'
  --exclude='*'
)
