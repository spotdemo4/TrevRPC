#!/usr/bin/env bash
# Checks with protoc that the bytes the tests encoded, in out/, decode to what
# expected/ says.
set -euo pipefail

cd "$(dirname "$0")"

for message in fixture.v1.Shape fixture.v1.Failure Tree Keywords Holder; do
	protoc -I proto --decode="$message" fixture/v1/shapes.proto fixture/v1/error.proto edge.proto \
		<"out/$message.bin" >"out/$message.txt"
	diff -u "expected/$message.txt" "out/$message.txt"
done
echo "protoc decodes what the Gleam code encodes"
