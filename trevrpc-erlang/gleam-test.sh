#!/usr/bin/env bash
# Generates the Gleam tests' service and runs the Gleam tests, as the
# trevrpc-erlang-gleam check does. rebar3 eunit runs the Erlang tests.
set -euo pipefail

cd "$(dirname "$0")"
bash gleam-generate.sh
gleam test
