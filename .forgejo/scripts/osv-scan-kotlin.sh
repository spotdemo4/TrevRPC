#!/usr/bin/env bash
set -euo pipefail

# Scan only the dependencies that reach users of the published Kotlin modules.
# Their Gradle lockfiles also pin build tooling (Dokka, the Kotlin compiler,
# Android lint) and test classpaths, which never ship in the artifacts, so each
# lockfile is filtered to its compile and runtime classpaths before scanning.

root="${1:-trevrpc-kotlin}"
modules=(core transport-cronet protoc-gen-trevrpc-kotlin)

scan_dir="$(mktemp -d)"
trap 'rm -rf "$scan_dir"' EXIT

for module in "${modules[@]}"; do
  mkdir -p "$scan_dir/$module"
  awk -F= '
    /^#/ || $2 == "" { print; next }
    {
      n = split($2, configurations, ",")
      for (i = 1; i <= n; i++) {
        if (configurations[i] == "compileClasspath" || configurations[i] == "runtimeClasspath") {
          print
          next
        }
      }
    }
  ' "$root/$module/gradle.lockfile" >"$scan_dir/$module/gradle.lockfile"
done

osv-scanner scan source -r "$scan_dir"
