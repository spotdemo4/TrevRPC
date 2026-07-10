#!/usr/bin/env bash

set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
RUNNER="$ROOT/bench/run_rpc_split.sh"
TMP_DIR=$(mktemp -d)
trap 'rm -rf "$TMP_DIR"' EXIT

SAMPLES_HEADER='axis,run,client,server,shape,latency_us,throughput_per_s,iterations,elapsed_s,source,transport_security_mode,certificate_verification_mode,payload_profile,encoded_request_bytes,encoded_response_bytes,serialization_mode,metadata_profile,handshake_inclusion_mode,batching_settings,labels,rust_quinn_ack_threshold,rust_quinn_ack_delay_ms,c_msquic_profile,c_msquic_profile_kind,c_frame_trace,rust_quinn_frame_trace,rust_quinn_qlog,rust_quinn_protocol_trace,rust_tls_keylog,rust_quinn_max_idle_timeout_ms,rust_quinn_keep_alive_ms,rust_quinn_send_window_bytes'
FAILURES_HEADER='axis,run,client,server,source,status,raw_file,rust_quinn_ack_threshold,rust_quinn_ack_delay_ms,c_msquic_profile,c_msquic_profile_kind,batching_settings,c_frame_trace,rust_quinn_frame_trace,rust_quinn_qlog,rust_quinn_protocol_trace,rust_tls_keylog,rust_quinn_max_idle_timeout_ms,rust_quinn_keep_alive_ms,rust_quinn_send_window_bytes,encoded_request_bytes,encoded_response_bytes'
BATCHING='profile=production-representative;js-send-many-batch=16;go-frame-batch=16;rust-frame-batch=32;grpc-batching=library-default'
LABELS='encrypted;tls-skip-verify;tiny;per-message-serialized;none;steady-state-warmed'

fail() {
    printf 'FAIL: %s\n' "$*" >&2
    exit 1
}

assert_contains() {
    local file=$1
    local expected=$2
    if ! grep -Fq -- "$expected" "$file"; then
        printf 'expected %s to contain: %s\n' "$file" "$expected" >&2
        return 1
    fi
}

write_fixture() {
    local out_dir=$1
    mkdir -p "$out_dir"
    printf '%s\n' "$SAMPLES_HEADER" >"$out_dir/rpc-split-samples.csv"
    printf 'server,1,c_msquic,rust_quinn,unary_latency,10.000,100000.000,10,0.100,c-custom,encrypted,tls-skip-verify,tiny,19,19,per-message-serialized,none,steady-state-warmed,%s,%s,library-default,library-default,safe,public-default,disabled,disabled,disabled,disabled,disabled,1234,234,345\n' "$BATCHING" "$LABELS" >>"$out_dir/rpc-split-samples.csv"
    printf '%s\n' "$FAILURES_HEADER" >"$out_dir/rpc-split-failures.csv"
    printf 'server,2,c_msquic,rust_quinn,c-custom,124,%s/raw/failure.txt,library-default,library-default,safe,public-default,%s,disabled,disabled,disabled,disabled,disabled,1234,234,345,19,19\n' "$out_dir" "$BATCHING" >>"$out_dir/rpc-split-failures.csv"
}

run_report() {
    local out_dir=$1
    shift
    env \
        OUT_DIR="$out_dir" \
        TREVRPC_C_FRAME_TRACE=false \
        TREVRPC_RUST_QUINN_FRAME_TRACE=FALSE \
        TREVRPC_RUST_SPLIT_BENCH_QUINN_PROTO_TRACE=off \
        TREVRPC_RUST_SPLIT_BENCH_QUINN_MAX_IDLE_TIMEOUT_MS=1234 \
        TREVRPC_RUST_SPLIT_BENCH_QUINN_KEEP_ALIVE_MS=234 \
        TREVRPC_RUST_SPLIT_BENCH_QUINN_SEND_WINDOW_BYTES=345 \
        "$@" \
        bash "$RUNNER" --report-only
}

expect_mismatch() {
    local name=$1
    local assignment=$2
    local expected=$3
    local out_dir="$TMP_DIR/mismatch-$name"
    write_fixture "$out_dir"
    if run_report "$out_dir" "$assignment" >"$out_dir.stdout" 2>"$out_dir.stderr"; then
        fail "report-only accepted mismatched $name"
    fi
    assert_contains "$out_dir.stderr" "$expected"
}

help_file="$TMP_DIR/help.txt"
bash "$RUNNER" --help >"$help_file"
assert_contains "$help_file" 'TREVRPC_RUST_SPLIT_BENCH_QUINN_SEND_WINDOW_BYTES'
assert_contains "$help_file" 'Enable with 1, true, TRUE, yes, or on'

valid_dir="$TMP_DIR/valid"
write_fixture "$valid_dir"
run_report "$valid_dir" >"$TMP_DIR/valid.stdout" 2>"$TMP_DIR/valid.stderr"
assert_contains "$valid_dir/rpc-split.md" "| Rust Quinn idle timeout | \`1234\` ms |"
assert_contains "$valid_dir/rpc-split.md" "| Rust Quinn keepalive | \`234\` ms |"
assert_contains "$valid_dir/rpc-split.md" "| Rust Quinn send window | \`345\` |"
awk -F, '
    NR == 1 && NF != 36 {
        printf "aggregate header has %d fields, expected 36\n", NF > "/dev/stderr"
        bad = 1
    }
    NR > 1 && (NF != 36 || $18 != "19" || $19 != "19" || $34 != "1234" || $35 != "234" || $36 != "345") {
        print "aggregate row did not preserve encoded-size or Rust transport metadata" > "/dev/stderr"
        bad = 1
    }
    END {
        if (NR != 2) {
            printf "aggregate has %d rows, expected 2\n", NR > "/dev/stderr"
            bad = 1
        }
        exit bad ? 1 : 0
    }
' "$valid_dir/rpc-split.csv"

expect_mismatch idle-timeout TREVRPC_RUST_SPLIT_BENCH_QUINN_MAX_IDLE_TIMEOUT_MS=1235 'Rust Quinn idle timeout'
expect_mismatch keepalive TREVRPC_RUST_SPLIT_BENCH_QUINN_KEEP_ALIVE_MS=235 'Rust Quinn keepalive'
expect_mismatch send-window TREVRPC_RUST_SPLIT_BENCH_QUINN_SEND_WINDOW_BYTES=346 'Rust Quinn send window'
expect_mismatch encoded-request ENCODED_REQUEST_BYTES=20 'encoded request bytes'
expect_mismatch encoded-response ENCODED_RESPONSE_BYTES=20 'encoded response bytes'

legacy_dir="$TMP_DIR/legacy"
mkdir -p "$legacy_dir"
printf '%s\n' 'axis,run,client,server,shape,latency_us,throughput_per_s,iterations,elapsed_s,source,transport_security_mode,certificate_verification_mode,payload_profile,encoded_request_bytes,encoded_response_bytes,serialization_mode,metadata_profile,handshake_inclusion_mode,batching_settings,labels,rust_quinn_ack_threshold,rust_quinn_ack_delay_ms,c_msquic_profile,c_msquic_profile_kind,c_frame_trace,rust_quinn_frame_trace,rust_quinn_qlog,rust_quinn_protocol_trace,rust_tls_keylog' >"$legacy_dir/rpc-split-samples.csv"
printf '%s\n' 'axis,run,client,server,source,status,raw_file,rust_quinn_ack_threshold,rust_quinn_ack_delay_ms,c_msquic_profile,c_msquic_profile_kind,batching_settings,c_frame_trace,rust_quinn_frame_trace,rust_quinn_qlog,rust_quinn_protocol_trace,rust_tls_keylog' >"$legacy_dir/rpc-split-failures.csv"
if run_report "$legacy_dir" >"$TMP_DIR/legacy.stdout" 2>"$TMP_DIR/legacy.stderr"; then
    fail 'report-only accepted the legacy split schema'
fi
assert_contains "$TMP_DIR/legacy.stderr" 'schema mismatch'
assert_contains "$TMP_DIR/legacy.stderr" 'state is never inferred'

assert_contains "$RUNNER" 'RUST_BENCH_ENV_UNSET+=("-u" "TREVRPC_RUST_SPLIT_BENCH_QUINN_SEND_WINDOW_BYTES")'

printf 'Split RPC report-only guardrail tests passed\n'
