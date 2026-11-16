#!/usr/bin/env bash
# Generates the code, runs the tests, and compares the bytes with protoc, as
# the trevrpc-gleam-protoc-gen check does.
set -euo pipefail

cd "$(dirname "$0")"
bash generate.sh
gleam test
bash compare.sh
