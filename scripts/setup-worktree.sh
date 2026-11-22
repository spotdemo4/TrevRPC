#!/usr/bin/env bash
set -euo pipefail

# Sets up a new git worktree of this repository: links .envrc, copies .env, and
# loads the Nix dev shell through direnv. T3 Code (t3.json) and Zed
# (.zed/tasks.json) run it when they create a worktree.
#
# Usage: scripts/setup-worktree.sh [main worktree root]
#
# .envrc is copied from the main worktree (keeping a symlink a symlink), or, when
# the main worktree has none, linked to .envrc.project as CONTRIBUTING.md
# describes. .env is copied only when the main worktree has one.

root="${1:-}"

cd "$(dirname "${BASH_SOURCE[0]}")/.."

if [[ ! -e .envrc && ! -L .envrc ]]; then
  if [[ -n $root && -e $root/.envrc ]]; then
    cp -P "$root/.envrc" .
  else
    ln -s .envrc.project .envrc
  fi
fi

if [[ ! -e .env && -n $root && -e $root/.env ]]; then
  cp -P "$root/.env" .
fi

direnv allow
direnv exec . true
