#!/usr/bin/env bash
set -euo pipefail

assert_clean() {
  test -z "$(git status --porcelain=v1 --untracked-files=all)"
}

start_head="$(git rev-parse HEAD)"
test "$(git ls-remote --heads origin refs/heads/main | cut -f 1)" = "$start_head"
git fetch --prune --prune-tags --tags --force origin

bumper --no-push

# The MsQuic provider has its own version source and may not be handled by the
# repository-wide bumper. Keep that path explicit instead of guessing a
# generated archive update on a runner that cannot build the provider.
if [[ -n "${TREVRPC_PROVIDER_BUMP_COMMAND-}" ]]; then
  TREVRPC_MODULE_DIR="trevrpc-c/provider/msquic" \
    TREVRPC_MODULE_PATH="trev.zip/llc/trevrpc/trevrpc-c/provider/msquic/v2" \
    bash -c "$TREVRPC_PROVIDER_BUMP_COMMAND"
fi
if [[ -n "${TREVRPC_ARTIFACT_UPDATE_COMMAND-}" ]]; then
  TREVRPC_MODULE_DIR="trevrpc-c/provider/msquic" \
    TREVRPC_MODULE_PATH="trev.zip/llc/trevrpc/trevrpc-c/provider/msquic/v2" \
    bash -c "$TREVRPC_ARTIFACT_UPDATE_COMMAND"
fi

final_head="$(git rev-parse HEAD)"
remote_tags="$(mktemp)"
local_tags="$(mktemp)"
trap 'rm -f "$remote_tags" "$local_tags"' EXIT
git ls-remote --refs --tags origin | sort >"$remote_tags"
git for-each-ref --format='%(objectname)%09%(refname)' refs/tags | sort >"$local_tags"
test -z "$(comm -23 "$remote_tags" "$local_tags")"
mapfile -t new_tags < <(comm -13 "$remote_tags" "$local_tags" | cut -f 2 | sed 's#^refs/tags/##')

if [[ "$final_head" == "$start_head" ]]; then
  test "${#new_tags[@]}" -eq 0
  assert_clean
  exit 0
fi

test "${#new_tags[@]}" -gt 0
assert_clean
for tag in "${new_tags[@]}"; do
  test "$(git rev-list -n 1 "$tag")" = "$final_head"
done

nix flake check --option log-lines 100

test "$(git rev-parse HEAD)" = "$final_head"
assert_clean

refspecs=("HEAD:refs/heads/main")
for tag in "${new_tags[@]}"; do
  refspecs+=("refs/tags/$tag:refs/tags/$tag")
done
git push --atomic origin "${refspecs[@]}"
