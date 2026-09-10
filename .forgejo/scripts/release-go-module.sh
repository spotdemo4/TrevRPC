#!/usr/bin/env bash
set -euo pipefail

# Release a Go module tag by warming the public module proxy and checksum
# database. This deliberately does not build or publish a Nix package: the C
# modules are published by their Git tags and are not Nix release outputs.

usage() {
  echo "usage: $0" >&2
  echo "  GITHUB_REF_NAME=... GITHUB_REF=... $0" >&2
  exit 2
}

ref_name="${GITHUB_REF_NAME-}"
ref="${GITHUB_REF-}"
if [[ -z "$ref_name" || -z "$ref" ]]; then
  usage
fi

if [[ "$ref" != "refs/tags/$ref_name" ]]; then
  echo "Go module release requires a tag ref; got GITHUB_REF_NAME=$ref_name GITHUB_REF=$ref" >&2
  exit 1
fi

# Match the nested provider before the parent prefix. A provider tag is
# derived from MsQuic's version and has the v2 module major suffix; its Go
# module path therefore ends in /v2 while its Git tag prefix does not.
case "$ref_name" in
  trevrpc-c/provider/msquic/v2.*-trevrpc.*)
    if [[ ! "$ref_name" =~ ^trevrpc-c/provider/msquic/v2\.[0-9]+\.[0-9]+-trevrpc\.[0-9]+$ ]]; then
      echo "invalid MsQuic provider module tag: $ref_name" >&2
      exit 1
    fi
    module="trev.zip/llc/trevrpc/trevrpc-c/provider/msquic/v2"
    tag="${ref_name#trevrpc-c/provider/msquic/}"
    python3 .forgejo/scripts/verify-msquic-provider-release.py --tag "$ref_name"
    ;;
  trevrpc-c/v*)
    if [[ ! "$ref_name" =~ ^trevrpc-c/v[0-9]+\.[0-9]+\.[0-9]+([.-][0-9A-Za-z.-]+)?$ ]]; then
      echo "invalid C module tag: $ref_name" >&2
      exit 1
    fi
    module="trev.zip/llc/trevrpc/trevrpc-c"
    tag="${ref_name#trevrpc-c/}"
    ;;
  trevrpc-go/quic-go/v*)
    if [[ ! "$ref_name" =~ ^trevrpc-go/quic-go/v[^/]+$ ]]; then
      echo "invalid QUIC-go module tag: $ref_name" >&2
      exit 1
    fi
    module="trev.zip/llc/trevrpc/trevrpc-go/quic-go"
    tag="${ref_name#trevrpc-go/quic-go/}"
    ;;
  trevrpc-go/v*)
    if [[ ! "$ref_name" =~ ^trevrpc-go/v[^/]+$ ]]; then
      echo "invalid base Go module tag: $ref_name" >&2
      exit 1
    fi
    module="trev.zip/llc/trevrpc/trevrpc-go"
    tag="${ref_name#trevrpc-go/}"
    ;;
  *)
    echo "unsupported Go module release tag: $ref_name" >&2
    exit 1
    ;;
esac

proxy="${GO_PROXY_URL:-https://proxy.golang.org}"
sumdb="${GO_SUMDB_URL:-https://sum.golang.org}"
curl_bin="${CURL_BIN:-curl}"

fetch() {
  local url="$1"
  echo "GET $url"
  "$curl_bin" --fail --show-error --retry 5 --retry-delay 5 --retry-all-errors "$url"
}

module_requirement() {
  local module_file="$1"
  local dependency="$2"
  local name version ignored
  while read -r name version ignored; do
    if [[ "$name" == "require" ]]; then
      name="$version"
      version="$ignored"
    fi
    if [[ "$name" == "$dependency" && "$version" =~ ^v ]]; then
      printf '%s\n' "$version"
      return 0
    fi
  done <"$module_file"
  echo "missing $dependency requirement in $module_file" >&2
  return 1
}

wait_for_module() {
  local dependency="$1"
  local version="$2"
  local url="${proxy%/}/$dependency/@v/$version.info"
  for _ in {1..60}; do
    if "$curl_bin" --fail --silent --show-error "$url" >/dev/null; then
      echo "dependency ready: $dependency@$version"
      return 0
    fi
    sleep 5
  done
  echo "timed out waiting for published dependency $dependency@$version" >&2
  return 1
}

# Tags may be pushed together by the repository bumper, so module publication is
# ordered here by waiting for the exact required sibling modules to reach the
# public proxy before seeding a dependent module.
case "$module" in
  trev.zip/llc/trevrpc/trevrpc-c/provider/msquic/v2)
    wait_for_module \
      trev.zip/llc/trevrpc/trevrpc-c \
      "$(module_requirement trevrpc-c/provider/msquic/go.mod trev.zip/llc/trevrpc/trevrpc-c)"
    ;;
  trev.zip/llc/trevrpc/trevrpc-go)
    wait_for_module \
      trev.zip/llc/trevrpc/trevrpc-c \
      "$(module_requirement trevrpc-go/go.mod trev.zip/llc/trevrpc/trevrpc-c)"
    wait_for_module \
      trev.zip/llc/trevrpc/trevrpc-c/provider/msquic/v2 \
      "$(module_requirement trevrpc-go/go.mod trev.zip/llc/trevrpc/trevrpc-c/provider/msquic/v2)"
    ;;
  trev.zip/llc/trevrpc/trevrpc-go/quic-go)
    wait_for_module \
      trev.zip/llc/trevrpc/trevrpc-go \
      "$(module_requirement trevrpc-go/quic-go/go.mod trev.zip/llc/trevrpc/trevrpc-go)"
    ;;
esac

# The proxy endpoint uses the module path verbatim here because all published
# module paths are lowercase and contain no URL-reserved characters.
fetch "${proxy%/}/$module/@v/$tag.info"
fetch "${proxy%/}/$module/@v/$tag.mod"
fetch "${proxy%/}/$module/@v/$tag.zip"
# Checksum lookup is useful but not required for publication to succeed.
"$curl_bin" --fail --show-error --retry 3 --retry-delay 2 --retry-all-errors \
  "${sumdb%/}/lookup/$module@$tag" || true

echo "Seeded $module@$tag"
