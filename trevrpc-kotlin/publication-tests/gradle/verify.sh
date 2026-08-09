#!/usr/bin/env bash
set -euo pipefail

: "${TREVRPC_STAGING_REPOSITORY:?TREVRPC_STAGING_REPOSITORY must name the staged Maven repository}"

gradle_bin=${GRADLE:-gradle}
project_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
work_dir=$(mktemp -d)
trap 'rm -rf "$work_dir"' EXIT
cache_seed=${TREVRPC_GRADLE_CACHE_SEED:-}
read -r -a metadata_modes <<<"${TREVRPC_GRADLE_METADATA_MODES:-gradle pom}"
protoc_args=()
if [[ -n "${TREVRPC_PROTOC_PATH:-}" ]]; then
  [[ "$TREVRPC_PROTOC_PATH" = /* ]] || {
    echo "TREVRPC_PROTOC_PATH must be absolute: $TREVRPC_PROTOC_PATH" >&2
    exit 1
  }
  [[ -f "$TREVRPC_PROTOC_PATH" && -x "$TREVRPC_PROTOC_PATH" ]] || {
    echo "TREVRPC_PROTOC_PATH must name an executable file: $TREVRPC_PROTOC_PATH" >&2
    exit 1
  }
  protoc_args+=("-PtrevrpcProtocPath=$TREVRPC_PROTOC_PATH")
fi

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
for metadata in "${metadata_modes[@]}"; do
  gradle_user_home="$work_dir/gradle-user-home-$metadata"
  mkdir -p "$gradle_user_home"
  gradle_args=()
  if [[ -n "$cache_seed" ]]; then
    test -d "$cache_seed/caches"
    cp -a "$cache_seed/caches" "$gradle_user_home/"
    gradle_args+=(--offline)
  fi
  GRADLE_USER_HOME="$gradle_user_home" \
    "$gradle_bin" \
      "${gradle_args[@]}" \
      "${protoc_args[@]}" \
      --project-dir "$project_dir" \
      --project-cache-dir "$work_dir/project-cache-$metadata" \
      --no-configuration-cache \
      --no-daemon \
      -Ptrevrpc.repository="file://$TREVRPC_STAGING_REPOSITORY" \
      -Ptrevrpc.metadata="$metadata" \
      -PtrevrpcVersion="$TREVRPC_VERSION" \
      -Ptrevrpc.version="$TREVRPC_VERSION" \
      verifyConsumers
done
