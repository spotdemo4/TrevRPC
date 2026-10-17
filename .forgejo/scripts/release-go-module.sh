#!/usr/bin/env bash
set -euo pipefail

# Release the Go module tag by warming the public module proxy and checksum
# database. Go modules are published by their Git tags alone.

ref_name="${GITHUB_REF_NAME-}"
ref="${GITHUB_REF-}"
if [[ -z "$ref_name" || -z "$ref" ]]; then
  echo "usage: GITHUB_REF_NAME=... GITHUB_REF=... $0" >&2
  exit 2
fi

if [[ "$ref" != "refs/tags/$ref_name" ]]; then
  echo "Go module release requires a tag ref; got GITHUB_REF_NAME=$ref_name GITHUB_REF=$ref" >&2
  exit 1
fi

if [[ ! "$ref_name" =~ ^trevrpc-go/v[^/]+$ ]]; then
  echo "unsupported Go module release tag: $ref_name" >&2
  exit 1
fi
module="trev.zip/llc/trevrpc/trevrpc-go"
tag="${ref_name#trevrpc-go/}"

proxy="${GO_PROXY_URL:-https://proxy.golang.org}"
sumdb="${GO_SUMDB_URL:-https://sum.golang.org}"

fetch() {
  echo "GET $1"
  curl --fail --show-error --retry 5 --retry-delay 5 --retry-all-errors "$@"
}

fetch "${proxy%/}/$module/@v/$tag.info"
fetch "${proxy%/}/$module/@v/$tag.mod"
fetch "${proxy%/}/$module/@v/$tag.zip" --output /dev/null
# Checksum lookup is useful but not required for publication to succeed.
curl --fail --show-error --retry 3 --retry-delay 2 --retry-all-errors \
  "${sumdb%/}/lookup/$module@$tag" || true

echo "Seeded $module@$tag"

# pkg.go.dev only picks new versions up from index.golang.org eventually, which
# can take hours, so request the version the way its "Request" button does and
# wait until it is served. The request answers 408 while processing continues.
pkgsite="${PKGSITE_URL:-https://pkg.go.dev}"
for _ in $(seq 60); do
  page=$(curl --silent --output /dev/null --write-out '%{http_code}' "${pkgsite%/}/$module@$tag" || true)
  if [[ "$page" == 200 ]]; then
    echo "Published $module@$tag to pkg.go.dev"
    exit 0
  fi
  echo "POST ${pkgsite%/}/fetch/$module@$tag"
  curl --silent --output /dev/null --max-time 60 -X POST "${pkgsite%/}/fetch/$module@$tag" || true
  sleep 10
done

# The module is already published through the proxy, so only warn.
echo "::warning::pkg.go.dev did not publish $module@$tag yet; request it at ${pkgsite%/}/$module@$tag"
