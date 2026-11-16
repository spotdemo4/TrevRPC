#!/usr/bin/env bash
# Generates Gleam code from proto/ into src/ with protoc-gen-trevrpc-gleam,
# runs the tests, and checks with protoc that the bytes the Gleam code
# encodes decode to what expected/ says. PROTOC_GEN_TREVRPC_GLEAM names the
# plugin; without it, it is built from ../.. with rebar3.
set -euo pipefail

here=$(cd "$(dirname "$0")" && pwd)
cd "$here"

plugin=${PROTOC_GEN_TREVRPC_GLEAM:-}
if [ -z "$plugin" ]; then
	(cd ../.. && rebar3 escriptize >&2)
	plugin=$here/../../_build/default/bin/protoc-gen-trevrpc-gleam
	ln -sf protoc-gen-trevrpc-erlang "$plugin"
fi

rm -rf src out
mkdir -p src out
protoc --plugin=protoc-gen-trevrpc-gleam="$plugin" -I proto --trevrpc-gleam_out=src \
	fixture/v1/common.proto fixture/v1/shapes.proto fixture/v1/enums.proto fixture/v1/error.proto edge.proto

gleam test

for message in fixture.v1.Shape fixture.v1.Failure Tree Keywords Holder; do
	protoc -I proto --decode="$message" fixture/v1/shapes.proto fixture/v1/error.proto edge.proto \
		<"out/$message.bin" >"out/$message.txt"
	diff -u "expected/$message.txt" "out/$message.txt"
done
echo "protoc decodes what the Gleam code encodes"
