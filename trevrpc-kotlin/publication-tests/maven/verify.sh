#!/usr/bin/env bash
set -euo pipefail

: "${TREVRPC_STAGING_REPOSITORY:?TREVRPC_STAGING_REPOSITORY must name the staged Maven repository}"

maven_bin=${MAVEN:-mvn}
project_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
local_repository=${MAVEN_LOCAL_REPOSITORY:-$(mktemp -d)}
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
  -f "$project_dir/pom.xml" \
  clean verify
