#!/usr/bin/env bash
# Generates Gleam code from proto/ into src/ with protoc-gen-trevrpc-gleam.
# PROTOC_GEN_TREVRPC_GLEAM names the plugin; without it, it is built from
# ../.. with rebar3.
set -euo pipefail

here=$(cd "$(dirname "$0")" && pwd)
cd "$here"

plugin=${PROTOC_GEN_TREVRPC_GLEAM:-}
if [ -z "$plugin" ]; then
	(cd ../.. && rebar3 escriptize >&2)
	plugin=$here/../../_build/default/bin/protoc-gen-trevrpc-gleam
fi

# The tests write the bytes they encode to out/, for compare.sh.
rm -rf src out
mkdir -p src out
protoc --plugin=protoc-gen-trevrpc-gleam="$plugin" -I proto --trevrpc-gleam_out=src \
	fixture/v1/common.proto fixture/v1/shapes.proto fixture/v1/enums.proto fixture/v1/error.proto edge.proto
