#!/usr/bin/env bash
# check-drift.sh — prove every deployed copy is explained by a commit.
#
# Usage: check-drift.sh [--verbose]
#
# The gap this closes (JA-24): the ACP supervisor and the hermes-channel plugin
# are deployed by file-level rsync into directories that are NOT git working
# copies — two of them live inside an unrelated repo, one is a plain directory.
# git therefore cannot tell us whether a target still matches the source. It
# didn't: --permission-mode was hand-edited "auto" -> "dontAsk" directly on the
# live prod launcher.exp on 2026-07-26 and existed in no commit for three days.
# Nothing would have caught it; finding it took a manual hunt through stray
# .bak files. This script is that missing check.
#
# What "explained" means here — the distinction is the whole point:
#
#   OK      target content == source HEAD. Fully reconciled.
#   STALE   target content == the commit named in its own VERSION marker, and
#           that commit is an ancestor of HEAD. Behind, but every byte is
#           accounted for by a commit. Not a failure — deploying is the fix.
#   DRIFT   target content matches NEITHER HEAD nor its VERSION commit. Bytes
#           are running in an instance that exist in no commit anywhere. This
#           is the dontAsk class. Exits non-zero.
#
# Read-only by construction: every rsync runs --dry-run. It is safe to point at
# prod, and it is meant to be — prod is where the drift was.
#
# Exit: 0 = no unexplained drift (STALE/no-marker are reported, not fatal)
#       1 = at least one target DRIFTed or is missing
#       2 = usage / environment error

set -uo pipefail   # deliberately NOT -e: every target must be checked and
                   # reported, not just the ones before the first failure.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
# shellcheck source=deploy-manifest.sh
source "${SCRIPT_DIR}/deploy-manifest.sh"

VERBOSE=0
case "${1:-}" in
  --verbose|-v) VERBOSE=1 ;;
  --help|-h) sed -n '2,30p' "$0"; exit 0 ;;
  "") ;;
  *) echo "usage: $(basename "$0") [--verbose]" >&2; exit 2 ;;
esac

# kind | rw|ro | label | path
# rw/ro is documentation of who may deploy there, not a behaviour switch:
# this script never writes to any of them.
TARGETS=(
  "acp|rw|janet-test ACP supervisor|${HOME}/.janet-test/acp"
  "acp|ro|PROD ACP supervisor|${HOME}/.hermes/acp"
  "plugin|rw|janet-test plugin (jt marketplace)|${HOME}/.janet-test/plugins/claude-channels-hermes-marketplace/external_plugins/hermes-channel"
  "plugin|ro|PROD-loaded plugin (vendored in janet-test repo)|${HOME}/dev/nestor/janet-test/plugins/claude-channels-hermes/marketplace/external_plugins/hermes-channel"
)

WORK="$(mktemp -d "${TMPDIR:-/tmp}/check-drift.XXXXXX")" || exit 2
trap 'rm -rf "$WORK"' EXIT

# materialize <ref> -> echoes a dir holding that commit's tree (cached per ref)
materialize() {
  local ref="$1" sha out
  sha="$(git -C "$REPO_ROOT" rev-parse --verify --quiet "${ref}^{commit}")" || return 1
  out="${WORK}/${sha}"
  if [ ! -d "$out" ]; then
    mkdir -p "$out"
    git -C "$REPO_ROOT" archive "$sha" | tar -x -C "$out" || return 1
  fi
  echo "$out"
}

# content_diff <srcdir> <targetdir> <kind> -> prints one line per differing file
# Compares by CHECKSUM, so a redeploy that only bumps mtimes is not drift, and a
# same-size edit (auto/dontAsk differ in length, but the next one may not) is.
# Files present in the target but not in the manifest are ignored on purpose:
# deploy-plugin.sh runs without --delete and the targets legitimately carry
# node_modules and runtime logs.
content_diff() {
  local src="$1" tgt="$2" kind="$3"
  local out
  case "$kind" in
    acp)    out="$(rsync -rlc --dry-run --itemize-changes --no-times --no-perms --no-owner --no-group \
                     "${ACP_RSYNC_FILTER[@]}" "$src/" "$tgt/" 2>/dev/null)" ;;
    plugin) out="$(rsync -rlc --dry-run --itemize-changes --no-times --no-perms --no-owner --no-group \
                     "${PLUGIN_RSYNC_FILTER[@]}" "$src/" "$tgt/" 2>/dev/null)" ;;
    *) echo "internal: unknown kind $kind" >&2; return 2 ;;
  esac
  # Keep only real content changes: itemize flag 3 is 'c' (checksum differs) or
  # '+' (absent from the target). Anything else is metadata noise.
  echo "$out" | awk '/^>f/ { f = substr($1, 3, 1); if (f == "c" || f == "+") print substr($0, index($0, " ") + 1) }'
}

HEAD_SHA="$(git -C "$REPO_ROOT" rev-parse --short HEAD)" || exit 2
echo "source: ${REPO_ROOT} @ ${HEAD_SHA}"
echo

FAILED=0
for entry in "${TARGETS[@]}"; do
  IFS='|' read -r kind access label path <<< "$entry"
  case "$kind" in
    acp)    subdir="$ACP_SRC_SUBDIR" ;;
    plugin) subdir="$PLUGIN_SRC_SUBDIR" ;;
  esac

  if [ ! -d "$path" ]; then
    printf 'MISSING  %-46s %s\n' "$label" "$path"
    FAILED=1
    continue
  fi

  head_tree="$(materialize "$HEAD_SHA")" || { echo "error: cannot materialize HEAD" >&2; exit 2; }
  vs_head="$(content_diff "${head_tree}/${subdir}" "$path" "$kind")"

  if [ -z "$vs_head" ]; then
    printf 'OK       %-46s %s\n' "$label" "(matches HEAD)"
    [ "$VERBOSE" -eq 1 ] && printf '         %s\n' "$path"
    continue
  fi

  # Differs from HEAD. Is it explained by the commit it claims to be at?
  marker=""
  [ -f "${path}/VERSION" ] && marker="$(awk 'NR==1{print $1}' "${path}/VERSION")"

  if [ -z "$marker" ]; then
    printf 'DRIFT    %-46s %s\n' "$label" "differs from HEAD and carries NO VERSION marker"
    printf '         %s\n' "$path"
    printf '         unexplained files: %s\n' "$(echo "$vs_head" | tr '\n' ' ')"
    FAILED=1
    continue
  fi

  ref_tree="$(materialize "$marker")"
  if [ -z "$ref_tree" ]; then
    printf 'DRIFT    %-46s %s\n' "$label" "VERSION names commit ${marker}, unknown to this repo"
    printf '         %s\n' "$path"
    FAILED=1
    continue
  fi

  vs_ref="$(content_diff "${ref_tree}/${subdir}" "$path" "$kind")"
  behind="$(git -C "$REPO_ROOT" rev-list --count "${marker}..HEAD" 2>/dev/null || echo '?')"

  if [ -z "$vs_ref" ]; then
    printf 'STALE    %-46s %s\n' "$label" "at ${marker}, ${behind} commit(s) behind HEAD — explained, redeploy to reconcile"
    printf '         %s\n' "$path"
    [ "$VERBOSE" -eq 1 ] && printf '         behind on: %s\n' "$(echo "$vs_head" | tr '\n' ' ')"
    continue
  fi

  printf 'DRIFT    %-46s %s\n' "$label" "matches NEITHER HEAD nor its own VERSION (${marker})"
  printf '         %s\n' "$path"
  printf '         files in no commit: %s\n' "$(echo "$vs_ref" | tr '\n' ' ')"
  FAILED=1
done

echo
if [ "$FAILED" -ne 0 ]; then
  echo "FAIL — at least one deployed copy is running bytes that exist in no commit."
  echo "Port it back to source (as its own commit, stating it was found live) BEFORE"
  echo "redeploying: the deploy replaces the target wholesale and would erase it."
  exit 1
fi
echo "PASS — every deployed copy is explained by a commit."
exit 0
