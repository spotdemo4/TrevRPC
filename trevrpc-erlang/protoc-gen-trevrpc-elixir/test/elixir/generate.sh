#!/usr/bin/env bash
# Generates Elixir code from proto/ into gen/ with
# protoc-gen-trevrpc-elixir. PROTOC_GEN_TREVRPC_ELIXIR names the plugin;
# without it, it is built from ../.. with rebar3. TREVRPC_PROTO names the
# directory of trevrpc/options.proto, the repository's proto/ by default.
set -euo pipefail

here=$(cd "$(dirname "$0")" && pwd)
cd "$here"

plugin=${PROTOC_GEN_TREVRPC_ELIXIR:-}
if [ -z "$plugin" ]; then
	(cd ../.. && rebar3 escriptize >&2)
	plugin=$here/../../_build/default/bin/protoc-gen-trevrpc-elixir
fi

rm -rf gen
mkdir -p gen
protoc --plugin=protoc-gen-trevrpc-elixir="$plugin" -I proto -I "${TREVRPC_PROTO:-$here/../../../../proto}" \
	--trevrpc-elixir_out=module_prefix=Fixture.Rpc:gen fixture/v1/greeter.proto
