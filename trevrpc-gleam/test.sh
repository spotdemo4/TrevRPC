#!/usr/bin/env bash
# Generates the test service and runs the tests, as the trevrpc-gleam package's
# check does.
set -euo pipefail

cd "$(dirname "$0")"
bash generate.sh
gleam test
