#!/usr/bin/env bash
# deploy-plugin.sh — sync the hermes-channel plugin to a target plugin dir
# (e.g. the vendored copy inside janet-test, or an instance's marketplace).
#
# Usage: deploy-plugin.sh <target_plugin_dir> [--init]
#
#   <target_plugin_dir>  destination plugin directory. Must already exist
#                        unless --init is given (refusing unknown paths by
#                        default catches typos before they scatter copies).
#   --init               allow creating a brand-new target directory.
#
# What it syncs (file-level, no --delete: runtime artifacts like timing logs
# and the target's node_modules are left untouched):
#   server.ts, stop_hook_fallback.ts, .mcp.json, .claude-plugin/plugin.json,
#   package.json, bun.lock, *.test.ts
# Then writes <target>/VERSION: git describe --always --dirty + deploy date.
#
# Idempotent: re-running with an unchanged source is a no-op apart from the
# VERSION timestamp.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
SRC_DIR="${REPO_ROOT}/marketplace/external_plugins/hermes-channel"

usage() {
  echo "usage: $(basename "$0") <target_plugin_dir> [--init]" >&2
  exit 2
}

TARGET=""
INIT=0
for arg in "$@"; do
  case "$arg" in
    --init) INIT=1 ;;
    -*) usage ;;
    *) [ -n "$TARGET" ] && usage; TARGET="$arg" ;;
  esac
done
[ -n "$TARGET" ] || usage

if [ ! -f "$SRC_DIR/server.ts" ]; then
  echo "error: source $SRC_DIR/server.ts not found — run from the claude-channels-hermes repo" >&2
  exit 1
fi

if [ ! -d "$TARGET" ]; then
  if [ "$INIT" -ne 1 ]; then
    echo "error: target $TARGET does not exist (pass --init to create a new deployment)" >&2
    exit 1
  fi
  mkdir -p "$TARGET"
fi

rsync -a \
  --include='/server.ts' \
  --include='/stop_hook_fallback.ts' \
  --include='/.mcp.json' \
  --include='/.claude-plugin/***' \
  --include='/package.json' \
  --include='/bun.lock' \
  --include='/*.test.ts' \
  --exclude='*' \
  "${SRC_DIR}/" "${TARGET}/"

DESCRIBE="$(git -C "$REPO_ROOT" describe --always --dirty)"
printf '%s %s\n' "$DESCRIBE" "$(date '+%Y-%m-%dT%H:%M:%S%z')" > "${TARGET}/VERSION"

echo "deployed hermes-channel plugin -> $TARGET ($DESCRIBE)"
