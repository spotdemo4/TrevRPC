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
  # Fallback for manual runs: read from main build file
  local fallback
  fallback=$(grep -m1 -E 'version = "[0-9]+\.[0-9]+\.[0-9]+"' "$(dirname "$0")/../../build.gradle.kts" 2>/dev/null | grep -oE '[0-9]+\.[0-9]+\.[0-9]+' || true)
  if [[ -n "$fallback" ]]; then echo "$fallback"; return; fi
  echo "0.1.2"
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

maven_args=(
  --batch-mode
  --errors
  -Dmaven.repo.local="$local_repository"
  -Dtrevrpc.repository="file://$TREVRPC_STAGING_REPOSITORY"
  -Dtrevrpc.version="$TREVRPC_VERSION"
  -f "$project_dir/pom.xml"
)
if [[ -n "${MAVEN_SETTINGS:-}" && -f "$MAVEN_SETTINGS" ]]; then
  maven_args+=(-s "$MAVEN_SETTINGS")
fi
if [[ -n "${TREVRPC_PROTOC_PATH:-}" ]]; then
  [[ "$TREVRPC_PROTOC_PATH" = /* ]] || {
    echo "TREVRPC_PROTOC_PATH must be absolute: $TREVRPC_PROTOC_PATH" >&2
    exit 1
  }
  [[ -f "$TREVRPC_PROTOC_PATH" && -x "$TREVRPC_PROTOC_PATH" ]] || {
    echo "TREVRPC_PROTOC_PATH must name an executable file: $TREVRPC_PROTOC_PATH" >&2
    exit 1
  }
  maven_args+=("-DprotocExecutable=$TREVRPC_PROTOC_PATH")
fi
"$maven_bin" "${maven_args[@]}" clean verify
