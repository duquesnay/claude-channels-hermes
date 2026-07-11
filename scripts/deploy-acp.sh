#!/usr/bin/env bash
# deploy-acp.sh — deploy the ACP supervisor (acp/) to a target instance dir.
#
# Usage: deploy-acp.sh <target_dir> [--init]
#
#   <target_dir>  destination directory (e.g. ~/.janet-test/acp). Must already
#                 exist unless --init is given: deploying to a path that was
#                 never an ACP dir is almost always a typo, so it is refused
#                 by default.
#   --init        allow creating a brand-new target directory.
#
# What it does:
#   1. Backs up the existing target: mv <target> <target>.bak-<timestamp>
#   2. rsyncs the deployable subset of acp/ (src/, launcher.exp, package.json,
#      bun.lock, tsconfig.json) — never node_modules, logs, or test artifacts.
#   3. bun install --cwd <target> (fresh node_modules from the synced lockfile)
#   4. Writes <target>/VERSION: git describe --always --dirty + deploy date.
#
# Idempotent: re-running produces the same target content (each run keeps its
# own timestamped backup). Never touches anything outside <target>*.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
SRC_DIR="${REPO_ROOT}/acp"

usage() {
  echo "usage: $(basename "$0") <target_dir> [--init]" >&2
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

if [ ! -d "$SRC_DIR/src" ]; then
  echo "error: source dir $SRC_DIR/src not found — run from the claude-channels-hermes repo" >&2
  exit 1
fi

if [ ! -d "$TARGET" ]; then
  if [ "$INIT" -ne 1 ]; then
    echo "error: target $TARGET does not exist (pass --init to create a new deployment)" >&2
    exit 1
  fi
  mkdir -p "$TARGET"
else
  BACKUP="${TARGET%/}.bak-$(date +%Y%m%d-%H%M%S)"
  mv "$TARGET" "$BACKUP"
  echo "backup: $BACKUP"
  mkdir -p "$TARGET"
fi

rsync -a \
  --include='/src/***' \
  --include='/launcher.exp' \
  --include='/package.json' \
  --include='/bun.lock' \
  --include='/tsconfig.json' \
  --exclude='*' \
  "${SRC_DIR}/" "${TARGET}/"

bun install --cwd "$TARGET"

DESCRIBE="$(git -C "$REPO_ROOT" describe --always --dirty)"
printf '%s %s\n' "$DESCRIBE" "$(date '+%Y-%m-%dT%H:%M:%S%z')" > "${TARGET}/VERSION"

echo "deployed acp -> $TARGET ($DESCRIBE)"
