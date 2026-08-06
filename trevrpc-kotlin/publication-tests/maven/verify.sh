#!/usr/bin/env bash
set -euo pipefail

: "${TREVRPC_STAGING_REPOSITORY:?TREVRPC_STAGING_REPOSITORY must name the staged Maven repository}"

maven_bin=${MAVEN:-mvn}
project_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
local_repository=${MAVEN_LOCAL_REPOSITORY:-$(mktemp -d)}

resolve_trevrpc_version() {
  if [[ -n "${TREVRPC_VERSION:-}" ]]; then echo "$TREVRPC_VERSION"; return; fi
  if [[ -n "${TREVRPC_KOTLIN_VERSION:-}" ]]; then echo "$TREVRPC_KOTLIN_VERSION"; return; fi
  local group_dir="$TREVRPC_STAGING_REPOSITORY/zip/trev/trevrpc"
  if [[ -d "$group_dir" ]]; then
    local versions
    versions=$(find "$group_dir"/core -mindepth 1 -maxdepth 1 -type d -exec basename {} \; 2>/dev/null | tr '\n' ' ')
    # shellcheck disable=SC2206
    local arr=($versions)
    if [[ ${#arr[@]} -eq 1 ]]; then echo "${arr[0]}"; return; fi
  fi
  echo "0.1.1"
}
TREVRPC_VERSION="$(resolve_trevrpc_version)"
export TREVRPC_VERSION
cleanup=false
if [[ -z ${MAVEN_LOCAL_REPOSITORY:-} ]]; then
  cleanup=true
fi
if $cleanup; then
  trap 'rm -rf "$local_repository"' EXIT
fi

"$maven_bin" \
  --batch-mode \
  --errors \
  -Dmaven.repo.local="$local_repository" \
  -Dtrevrpc.repository="file://$TREVRPC_STAGING_REPOSITORY" \
  -Dtrevrpc.version="$TREVRPC_VERSION" \
  -f "$project_dir/pom.xml" \
  clean verify
