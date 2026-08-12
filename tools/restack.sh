#!/usr/bin/env bash
#
# Maintains the patch stack this fork carries on top of upstream, and regenerates
# the integration branch from it.
#
#   ./tools/restack.sh check       what would move, and whether the merge still applies cleanly
#   ./tools/restack.sh rebase      restack every branch onto its parent
#   ./tools/restack.sh integrate   regenerate the integration branch from the stack
#   ./tools/restack.sh all         rebase, then integrate
#
# The integration branch is REGENERATED, never merged into incrementally: its
# history is disposable, the branches are the source of truth. Dropping a feature
# means deleting one line from MERGE_ORDER below.
#
# rerere replays conflict resolutions you have already made. It replays them
# faithfully, including wrong ones - so a clean run proves nothing until the
# firmware actually builds. 'check' and 'integrate' never touch your branches.

set -euo pipefail

# This script switches branches while it runs, and it does not exist on every
# branch. Re-exec from a copy outside the working tree so bash cannot have the
# file pulled out from under it mid-run.
if [ -z "${RESTACK_DETACHED:-}" ]; then
  _copy=$(mktemp); cat "$0" > "$_copy"
  RESTACK_DETACHED=1 exec bash "$_copy" "$@"
fi

BASE=upstream/dev

# The branch 'integrate' rewrites. Override to rehearse without touching the real
# one:  INTEGRATION=patch_public_test ./tools/restack.sh integrate
INTEGRATION=${INTEGRATION:-patch_public}

# branch : its IMMEDIATE parent. Parents must come before their children.
#
# Immediate, not the root of the chain: each branch is replayed with
# 'git rebase --onto <parent> <old base> <branch>', so only the commits that
# branch actually owns move. That is correct in both cases - upstream moved, or
# you amended a branch in the middle of a chain. Naming the root instead would
# replay the whole chain and silently drop an amendment made to a link above.
# See do_rebase() for why the old bases must be captured before anything moves.
STACK=(
  "feature/core-enhancements    : $BASE"
  "feature/repeater-ping        : feature/core-enhancements"
  "feature/repeater-flood-retry : feature/repeater-ping"
  "feature/rp2040-lowpower      : feature/core-enhancements"
  "feature/lora-ota             : feature/rp2040-lowpower"
  "tools/companion-tools        : feature/core-enhancements"
  "packaging/mlk-defaults       : feature/core-enhancements"
)

# Merge order into the integration branch. Order matters: the two chains must go
# in dependency order, and packaging stays last so its distribution defaults win.
MERGE_ORDER=(
  feature/core-enhancements
  feature/repeater-ping
  feature/repeater-flood-retry
  feature/rp2040-lowpower
  feature/lora-ota
  tools/companion-tools
  packaging/mlk-defaults
)

say()  { printf '\n\033[1m== %s\033[0m\n' "$*"; }
ok()   { printf '  \033[32m%-34s %s\033[0m\n' "$1" "${2:-ok}"; }
bad()  { printf '  \033[31m%-34s %s\033[0m\n' "$1" "${2:-FAILED}"; }

require_clean_tree() {
  if ! git diff --quiet || ! git diff --cached --quiet; then
    echo "working tree has uncommitted changes - commit or stash first" >&2
    exit 1
  fi
}

branch_of() { echo "${1%%:*}" | xargs; }
parent_of() { echo "${1##*:}" | xargs; }

record_refs() {
  say "current tips (recover with: git branch -f <branch> <sha>)"
  for b in "${MERGE_ORDER[@]}"; do
    printf '  %-34s %s\n' "$b" "$(git rev-parse --short "$b")"
  done
}

do_rebase() {
  require_clean_tree
  record_refs
  git fetch upstream --quiet

  # Every old base is computed BEFORE anything moves. This is the whole trick:
  # once a parent has been rebased, 'git rebase <parent> <branch>' picks a merge
  # base that is no longer the parent's previous tip - it falls back further down
  # - and silently replays the PARENT's commits a second time on top of the
  # already-rebased parent. Capturing the bases up front and rebasing with
  # --onto replays only the commits each branch actually owns.
  local -A old_base
  local entry b p
  for entry in "${STACK[@]}"; do
    b=$(branch_of "$entry"); p=$(parent_of "$entry")
    old_base[$b]=$(git merge-base "$p" "$b")
  done

  say "restacking onto $(git rev-parse --short $BASE)"
  for entry in "${STACK[@]}"; do
    b=$(branch_of "$entry"); p=$(parent_of "$entry")
    if [ "$(git rev-parse "${old_base[$b]}")" = "$(git rev-parse "$p")" ]; then
      ok "$b" "already on $p"
      continue
    fi
    # rerere.autoupdate is forced off: it would stage a replayed resolution and
    # let the rebase carry on silently, which is how a stale one gets in.
    if git -c rerere.autoupdate=false rebase --onto "$p" "${old_base[$b]}" "$b" >/dev/null 2>&1; then
      ok "$b" "onto $p"
    else
      bad "$b" "conflict - resolve, then: git rebase --continue"
      echo
      git status --short | grep -E '^(UU|AA|DU|UD)' || true
      exit 1
    fi
  done
}

# Rebuilds the integration branch in a scratch ref, so a failure leaves the real
# branch untouched.
do_integrate() {
  require_clean_tree
  if git rev-parse --verify --quiet "$INTEGRATION" >/dev/null; then
    say "about to REWRITE $INTEGRATION"
    printf '  %s is at %s (%s)\n' "$INTEGRATION" \
      "$(git rev-parse --short "$INTEGRATION")" \
      "$(git log -1 --format=%s "$INTEGRATION" | cut -c1-60)"
    if [ -z "${RESTACK_YES:-}" ]; then
      printf '  continue? [y/N] '
      # /dev/tty when there is one (a task runner may not give us one), else stdin.
      if [ -r /dev/tty ]; then
        read -r answer </dev/tty || answer=n
      else
        read -r answer || answer=n
      fi
      case "$answer" in [yY]*) ;; *) echo "  aborted"; exit 1 ;; esac
    fi
  fi
  local tmp="refs/heads/_restack_tmp"
  git branch -f _restack_tmp "$BASE" >/dev/null
  git checkout --quiet _restack_tmp

  say "merging ${#MERGE_ORDER[@]} branches onto $(git rev-parse --short $BASE)"
  for b in "${MERGE_ORDER[@]}"; do
    if git -c rerere.autoupdate=false merge --no-edit --no-ff "$b" >/dev/null 2>&1; then
      ok "$b"
    else
      bad "$b" "conflict"
      git diff --diff-filter=U --name-only | sed 's/^/    /'
      echo
      echo "resolve, 'git commit', then re-run - or 'git merge --abort' and fix the branch" >&2
      exit 1
    fi
  done

  git branch -f "$INTEGRATION" _restack_tmp
  git checkout --quiet "$INTEGRATION"
  git branch -D _restack_tmp >/dev/null
  say "$INTEGRATION regenerated at $(git rev-parse --short HEAD)"
  echo "  NOTE: this rewrites $INTEGRATION. Branches that descend from it"
  echo "        (mqtt_espnow_multibridge, HTv4LowPower, ...) need re-basing too."
  echo "  NOTE: nothing is pushed, and nothing is built. Build before you trust it."
}

# Read-only: does each branch still apply, and does the whole set still merge?
do_check() {
  git fetch upstream --quiet
  say "behind $BASE"
  for b in "${MERGE_ORDER[@]}"; do
    printf '  %-34s %s commits\n' "$b" "$(git rev-list --count "$b..$BASE")"
  done

  say "merge dry-run onto $(git rev-parse --short $BASE)"
  local head
  head=$(git rev-parse HEAD)
  local worktree
  worktree=$(mktemp -d)
  git worktree add --quiet --detach "$worktree" "$BASE"
  (
    cd "$worktree"
    for b in "${MERGE_ORDER[@]}"; do
      if git merge --no-edit --no-ff "$b" >/dev/null 2>&1; then
        ok "$b"
      else
        bad "$b" "would conflict"
        git diff --diff-filter=U --name-only | sed 's/^/    /'
        git merge --abort
        exit 1
      fi
    done
  ) || { git worktree remove --force "$worktree"; exit 1; }
  git worktree remove --force "$worktree"
  echo
  echo "  clean - but rerere may have replayed a stale resolution. Build before trusting it."
}

case "${1:-check}" in
  check)     do_check ;;
  rebase)    do_rebase ;;
  integrate) do_integrate ;;
  all)       do_rebase; do_integrate ;;
  *)         sed -n '3,18p' "$0"; exit 1 ;;
esac
