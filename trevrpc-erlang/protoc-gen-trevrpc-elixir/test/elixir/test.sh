#!/usr/bin/env bash
# Generates the code and runs the tests, as the trevrpc-erlang-protoc-gen-elixir
# check does. ERL_LIBS must hold trevrpc and its dependencies; without it,
# they are built from ../../.. with rebar3.
set -euo pipefail

cd "$(dirname "$0")"
if [ -z "${ERL_LIBS:-}" ]; then
	(cd ../../.. && rebar3 compile >&2)
	ERL_LIBS=$(cd ../../../_build/default/lib && pwd)
	export ERL_LIBS
fi
bash generate.sh
mix test
