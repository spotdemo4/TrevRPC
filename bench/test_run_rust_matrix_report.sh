#!/usr/bin/env bash

set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
MATRIX_ROOT="$ROOT/bench/rust-matrix"
REPORTER_WAS_SET=${REPORTER+x}
REPORTER=${REPORTER:-$MATRIX_ROOT/target/debug/trevrpc-rust-matrix-report}
TMP_DIR=$(mktemp -d)
trap 'rm -rf "$TMP_DIR"' EXIT

fail() {
    printf 'FAIL: %s\n' "$*" >&2
    exit 1
}

sample() {
    local stack=$1
    local repetition=$2
    local run_id="${stack}-r${repetition}"
    local client_cpu=$((1000000 + repetition * 1000))
    local server_cpu=$((900000 + repetition * 1000))
    jq -cn \
        --arg run_id "$run_id" \
        --arg stack "$stack" \
        --argjson repetition "$repetition" \
        --argjson client_cpu "$client_cpu" \
        --argjson server_cpu "$server_cpu" \
        '{schema_version:1,event:"sample",run_id:$run_id,config_hash:("hash-" + $stack),source_commit:"test",artifact_sha256:"artifact",stack:$stack,application_encoding:"protobuf",workload:"tiny",operation:"unary_closed_loop",repetition:$repetition,concurrency:1,connections:1,warmup_ms:50,measurement_ms:100,elapsed_ns:100000000,drain_ns:0,completed:100,failed:0,throughput_per_s:1000.0,latency_p50_ns:100000,latency_p90_ns:120000,latency_p95_ns:130000,latency_p99_ns:150000,latency_p999_ns:180000,latency_max_ns:200000,application_request_bytes:19,application_response_bytes:19,transport_security_mode:"encrypted",certificate_verification_mode:"private-ca-verified",batching_policy:"one-request-per-lane",network_profile:"loopback",client:{cpu_ns:$client_cpu,rss_before_bytes:1048576,rss_after_bytes:1048576,peak_rss_bytes:2097152,voluntary_context_switches:1,involuntary_context_switches:0},server:{cpu_ns:$server_cpu,rss_before_bytes:1048576,rss_after_bytes:1048576,peak_rss_bytes:3145728,voluntary_context_switches:1,involuntary_context_switches:0},histogram:[{highest_equivalent_ns:100000,count:100}]}'
}

if [[ -z "$REPORTER_WAS_SET" ]]; then
    cargo build --quiet --manifest-path "$MATRIX_ROOT/Cargo.toml" --bin trevrpc-rust-matrix-report
fi
SAMPLES="$TMP_DIR/samples.jsonl"
for repetition in 1 2; do
    sample trevrpc_quinn "$repetition" >>"$SAMPLES"
    sample grpc_tonic_generated "$repetition" >>"$SAMPLES"
done

"$REPORTER" "$SAMPLES" "$TMP_DIR/aggregate.csv" "$TMP_DIR/report.md" 2 1
[[ $(wc -l <"$TMP_DIR/aggregate.csv") == 3 ]] || fail "unexpected aggregate row count"
grep -Fq '`trevrpc_quinn`' "$TMP_DIR/report.md" || fail "missing TrevRPC report row"
grep -Fq '`grpc_tonic_generated`' "$TMP_DIR/report.md" || fail "missing gRPC report row"

sample trevrpc_quinn 1 >"$TMP_DIR/incomplete.jsonl"
sample grpc_tonic_generated 1 >>"$TMP_DIR/incomplete.jsonl"
sample grpc_tonic_generated 2 >>"$TMP_DIR/incomplete.jsonl"
if "$REPORTER" "$TMP_DIR/incomplete.jsonl" "$TMP_DIR/bad.csv" "$TMP_DIR/bad.md" 2 1 \
    >"$TMP_DIR/incomplete.stdout" 2>"$TMP_DIR/incomplete.stderr"; then
    fail "reporter accepted an incomplete cell"
fi
grep -Fq 'has 1 runs, expected 2' "$TMP_DIR/incomplete.stderr" || fail "missing incomplete-cell error"

sample trevrpc_quinn 1 >"$TMP_DIR/missing-stack.jsonl"
if "$REPORTER" "$TMP_DIR/missing-stack.jsonl" "$TMP_DIR/bad.csv" "$TMP_DIR/bad.md" 1 1 \
    >"$TMP_DIR/missing-stack.stdout" 2>"$TMP_DIR/missing-stack.stderr"; then
    fail "reporter accepted a missing comparison stack"
fi
grep -Fq 'missing grpc_tonic_generated concurrency 1 comparison cell' \
    "$TMP_DIR/missing-stack.stderr" || fail "missing comparison-cell error"

jq -c 'if .stack == "grpc_tonic_generated" then .network_profile = "different" else . end' \
    "$SAMPLES" >"$TMP_DIR/mismatch.jsonl"
if "$REPORTER" "$TMP_DIR/mismatch.jsonl" "$TMP_DIR/bad.csv" "$TMP_DIR/bad.md" 2 1 \
    >"$TMP_DIR/mismatch.stdout" 2>"$TMP_DIR/mismatch.stderr"; then
    fail "reporter accepted mismatched matrix settings"
fi
grep -Fq 'does not match matrix-wide immutable configuration' \
    "$TMP_DIR/mismatch.stderr" || fail "missing matrix invariant error"

if "$REPORTER" "$SAMPLES" "$TMP_DIR/bad.csv" "$TMP_DIR/bad.md" 2 1,8 \
    >"$TMP_DIR/concurrency.stdout" 2>"$TMP_DIR/concurrency.stderr"; then
    fail "reporter accepted a missing requested concurrency"
fi
grep -Fq 'do not match expected' "$TMP_DIR/concurrency.stderr" || fail "missing concurrency error"

jq -c 'if .run_id == "trevrpc_quinn-r1" then .completed = 99 else . end' \
    "$SAMPLES" >"$TMP_DIR/corrupt.jsonl"
if "$REPORTER" "$TMP_DIR/corrupt.jsonl" "$TMP_DIR/bad.csv" "$TMP_DIR/bad.md" 2 1 \
    >"$TMP_DIR/corrupt.stdout" 2>"$TMP_DIR/corrupt.stderr"; then
    fail "reporter accepted internally inconsistent measurements"
fi
grep -Fq 'histogram count 100 does not match 99 completions' \
    "$TMP_DIR/corrupt.stderr" || fail "missing measurement invariant error"

jq -c '.schema_version = 2' "$SAMPLES" >"$TMP_DIR/wrong-schema.jsonl"
if "$REPORTER" "$TMP_DIR/wrong-schema.jsonl" "$TMP_DIR/bad.csv" "$TMP_DIR/bad.md" 2 1 \
    >"$TMP_DIR/schema.stdout" 2>"$TMP_DIR/schema.stderr"; then
    fail "reporter accepted an unsupported schema"
fi
grep -Fq 'unsupported sample event' "$TMP_DIR/schema.stderr" || fail "missing schema error"

printf 'Rust matrix report tests passed\n'
