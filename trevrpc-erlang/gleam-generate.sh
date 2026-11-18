#!/usr/bin/env bash
# Generates the Gleam tests' service code into test/ with protoc-gen-trevrpc-gleam.
# PROTOC_GEN_TREVRPC_GLEAM names the plugin; without it, it is built from
# protoc-gen-trevrpc-gleam with rebar3. TREVRPC_PROTO names
# the directory of trevrpc/options.proto, the repository's proto/ by default.
set -euo pipefail

here=$(cd "$(dirname "$0")" && pwd)
cd "$here"

plugin=${PROTOC_GEN_TREVRPC_GLEAM:-}
if [ -z "$plugin" ]; then
	(cd protoc-gen-trevrpc-gleam && rebar3 escriptize >&2)
	plugin=$here/protoc-gen-trevrpc-gleam/_build/default/bin/protoc-gen-trevrpc-gleam
fi

rm -rf test/greet
protoc --plugin=protoc-gen-trevrpc-gleam="$plugin" -I test/proto -I "${TREVRPC_PROTO:-$here/../proto}" \
	--trevrpc-gleam_out=test greet/v1/greeter.proto
