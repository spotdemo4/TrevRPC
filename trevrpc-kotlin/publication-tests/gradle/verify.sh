#!/usr/bin/env bash
set -euo pipefail

: "${TREVRPC_STAGING_REPOSITORY:?TREVRPC_STAGING_REPOSITORY must name the staged Maven repository}"

gradle_bin=${GRADLE:-gradle}
project_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
work_dir=$(mktemp -d)
trap 'rm -rf "$work_dir"' EXIT
cache_seed=${TREVRPC_GRADLE_CACHE_SEED:-}
read -r -a metadata_modes <<<"${TREVRPC_GRADLE_METADATA_MODES:-gradle pom}"

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
      --project-dir "$project_dir" \
      --project-cache-dir "$work_dir/project-cache-$metadata" \
      --no-configuration-cache \
      --no-daemon \
      -Ptrevrpc.repository="file://$TREVRPC_STAGING_REPOSITORY" \
      -Ptrevrpc.metadata="$metadata" \
      verifyConsumers
done
