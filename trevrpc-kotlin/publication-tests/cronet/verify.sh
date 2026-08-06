#!/usr/bin/env bash
set -euo pipefail

: "${TREVRPC_STAGING_REPOSITORY:?TREVRPC_STAGING_REPOSITORY must name the staged Maven repository}"

gradle_bin=${GRADLE:-gradle}
maven_bin=${MAVEN:-mvn}
fixture_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
work_dir=$(mktemp -d)
trap 'rm -rf "$work_dir"' EXIT

resolve_trevrpc_version() {
  if [[ -n "${TREVRPC_VERSION:-}" ]]; then
    echo "$TREVRPC_VERSION"
    return
  fi
  if [[ -n "${TREVRPC_KOTLIN_VERSION:-}" ]]; then
    echo "$TREVRPC_KOTLIN_VERSION"
    return
  fi
  # Scan staging repository for the single published version (all modules share it)
  local group_dir="$TREVRPC_STAGING_REPOSITORY/zip/trev/trevrpc"
  if [[ -d "$group_dir" ]]; then
    local versions
    # shellcheck disable=SC2012
    versions=$(ls -1 "$group_dir"/core/ 2>/dev/null | tr '\n' ' ')
    # shellcheck disable=SC2206
    local arr=($versions)
    if [[ ${#arr[@]} -eq 1 ]]; then
      echo "${arr[0]}"
      return
    fi
  fi
  echo "0.1.0"
}

TREVRPC_VERSION="$(resolve_trevrpc_version)"
export TREVRPC_VERSION

python3 - "$TREVRPC_STAGING_REPOSITORY" "$TREVRPC_VERSION" <<'PY'
import pathlib
import struct
import sys
import xml.etree.ElementTree as ET
import zipfile

version = sys.argv[2] if len(sys.argv) >= 3 and sys.argv[2].strip() else "0.1.0"
repository = pathlib.Path(sys.argv[1])
artifact = repository / f"zip/trev/trevrpc/transport-cronet/{version}"
jar = artifact / f"transport-cronet-{version}.jar"
pom = artifact / f"transport-cronet-{version}.pom"

with zipfile.ZipFile(jar) as archive:
    names = archive.namelist()
    assert len(names) == len(set(names)), "Cronet JAR contains duplicate entries"
    assert "org/chromium/net/CronetEngine.class" in names, "CronetEngine is absent from the API JAR"
    assert "META-INF/LICENSE.chromium-cronet" in names, "bundled Cronet classes lack their license"
    majors = {
        struct.unpack(">H", archive.read(name)[6:8])[0]
        for name in names
        if name.endswith(".class")
    }
    assert majors == {61}, f"Cronet JAR must contain only JVM 17 classes, found majors {sorted(majors)}"

root = ET.parse(pom).getroot()
namespace = {"m": "http://maven.apache.org/POM/4.0.0"}
description = root.findtext("m:description", namespaces=namespace) or ""
assert "JVM 17" in description, f"Cronet POM has an untruthful platform description: {description!r}"
dependencies = {
    (
        dependency.findtext("m:groupId", namespaces=namespace),
        dependency.findtext("m:artifactId", namespaces=namespace),
    )
    for dependency in root.findall("m:dependencies/m:dependency", namespace)
}
assert not any(group == "org.chromium.net" for group, _ in dependencies), "Cronet POM exposes an unusable AAR"
licenses = {
    license_element.findtext("m:name", namespaces=namespace)
    for license_element in root.findall("m:licenses/m:license", namespace)
}
assert "Chromium and built-in dependencies" in licenses, "Cronet POM omits bundled class licensing"
PY

GRADLE_USER_HOME="$work_dir/gradle-user-home" \
  "$gradle_bin" \
    --project-dir "$fixture_dir/gradle" \
    --project-cache-dir "$work_dir/gradle-project-cache" \
    --no-configuration-cache \
    --no-daemon \
    -Ptrevrpc.repository="file://$TREVRPC_STAGING_REPOSITORY" \
    -PtrevrpcVersion="$TREVRPC_VERSION" \
    -Ptrevrpc.version="$TREVRPC_VERSION" \
    clean compileKotlin

"$maven_bin" \
  --batch-mode \
  --errors \
  -Dmaven.repo.local="$work_dir/maven-local-repository" \
  -Dtrevrpc.repository="file://$TREVRPC_STAGING_REPOSITORY" \
  -Dtrevrpc.version="$TREVRPC_VERSION" \
  -f "$fixture_dir/maven/pom.xml" \
  clean compile
