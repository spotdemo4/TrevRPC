#!/usr/bin/env bash
# Regenerates the interop bindings in gen/ from interop/proto with protoc-gen-py and
# protoc-gen-trevrpc-py, or writes them to the directory given instead. Both plugins come from
# PATH, or from ../protoc-gen-trevrpc-py's uv environment without them.
set -euo pipefail

here=$(cd "$(dirname "$0")" && pwd)
root=$here/../..
out=${1:-$here/gen}

if ! command -v protoc-gen-trevrpc-py >/dev/null; then
	PATH=$here/../protoc-gen-trevrpc-py/.venv/bin:$PATH
fi

# Only what the plugins write goes: the directory may hold other files.
rm -rf "$out"
mkdir -p "$out"
protoc \
	-I "$root/interop/proto" \
	-I "$root/proto" \
	--py_out="$out" \
	--trevrpc-py_out="$out" \
	interop.proto compressed.proto unreliable.proto trevrpc/options.proto
