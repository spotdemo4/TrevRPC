#!/usr/bin/env bash
# Regenerates the interop bindings in src/generated from interop/proto with
# protoc-gen-trevrpc-erlang, or writes them to the directory given instead.
# PROTOC_GEN_TREVRPC_ERLANG names the plugin; without it, the plugin is built
# from ../protoc-gen-trevrpc-erlang with rebar3.
set -euo pipefail

here=$(cd "$(dirname "$0")" && pwd)
root=$here/../..
out=${1:-$here/src/generated}

plugin=${PROTOC_GEN_TREVRPC_ERLANG:-}
if [ -z "$plugin" ]; then
	(cd "$here/../protoc-gen-trevrpc-erlang" && rebar3 escriptize >&2)
	plugin=$here/../protoc-gen-trevrpc-erlang/_build/default/bin/protoc-gen-trevrpc-erlang
fi

# Only what the plugin writes goes: the directory may hold other files.
mkdir -p "$out"
rm -f "$out"/*.erl "$out"/*.hrl
protoc \
	--plugin=protoc-gen-trevrpc-erlang="$plugin" \
	-I "$root/interop/proto" \
	-I "$root/proto" \
	--trevrpc-erlang_out="$out" \
	interop.proto compressed.proto
