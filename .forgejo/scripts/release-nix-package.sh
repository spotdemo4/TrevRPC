#!/usr/bin/env bash
set -euo pipefail

if [[ "$#" -ne 1 ]]; then
  echo "usage: $0 <trevrpc-go|trevrpc-rust>" >&2
  exit 2
fi

package="$1"
case "$package" in
  trevrpc-go)
    package_output="packages.x86_64-linux.trevrpc-go.x86_64-unknown-linux-musl"
    ;;
  trevrpc-rust)
    package_output="packages.x86_64-linux.trevrpc-rust.x86_64-unknown-linux-musl"
    ;;
  *)
    echo "unsupported release package: $package" >&2
    exit 2
    ;;
esac

ref_name="${GITHUB_REF_NAME-}"
ref="${GITHUB_REF-}"
if [[ ! "$ref_name" =~ ^${package}/v[^/]+$ ]] || [[ "$ref" != "refs/tags/$ref_name" ]]; then
  echo "release requires a $package/v* tag ref; got GITHUB_REF_NAME=$ref_name GITHUB_REF=$ref" >&2
  exit 1
fi

fetch_tags() {
  local attempt
  for attempt in 1 2 3; do
    if git fetch --force --tags origin; then
      return 0
    fi
    echo "tag fetch attempt $attempt of 3 failed" >&2
    if [[ "$attempt" -lt 3 ]]; then
      sleep 5
    fi
  done
  echo "failed to fetch release tags after 3 attempts" >&2
  return 1
}

fetch_tags

head_commit="$(git rev-parse --verify 'HEAD^{commit}')"
tag_commit="$(git rev-parse --verify "refs/tags/$ref_name^{commit}")"
if [[ "$tag_commit" != "$head_commit" ]]; then
  echo "release tag $ref_name does not point at HEAD" >&2
  exit 1
fi

version="$(nix eval --raw ".#$package_output.version")"
if [[ -z "$version" || "$version" =~ [[:space:]/] ]]; then
  echo "invalid Nix version for $package: $version" >&2
  exit 1
fi
expected_tag="$package/v$version"
if [[ "$ref_name" != "$expected_tag" ]]; then
  echo "release tag $ref_name does not match Nix version $version (expected $expected_tag)" >&2
  exit 1
fi

export TAG="$expected_tag"
export PACKAGES="$package_output"
exec flake-release
