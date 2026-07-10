#!/usr/bin/env bash

set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
RUNNER="$ROOT/bench/run_webtransport.sh"
TMP_DIR=$(mktemp -d)
trap 'rm -rf "$TMP_DIR"' EXIT

SAMPLES_HEADER='run,browser,server,shape,latency_us,throughput_per_s,iterations,elapsed_s,source,transport_security_mode,certificate_verification_mode,payload_profile,encoded_request_bytes,encoded_response_bytes,serialization_mode,metadata_profile,handshake_inclusion_mode,batching_settings,labels,c_frame_trace,rust_quinn_qlog,rust_quinn_protocol_trace,rust_tls_keylog,rust_quinn_max_idle_timeout_ms,rust_quinn_keep_alive_ms,browser_congestion_control'
FAILURES_HEADER='run,browser,server,source,status,raw_file,batching_settings,c_frame_trace,rust_quinn_qlog,rust_quinn_protocol_trace,rust_tls_keylog,encoded_request_bytes,encoded_response_bytes,rust_quinn_max_idle_timeout_ms,rust_quinn_keep_alive_ms,browser_congestion_control'
DIAGNOSTIC_BATCHING='profile=diagnostic-instrumented;browser-stream-read-batch=64;browser-stream-write-batch=64;browser-stream-write-batch-bytes=65536;js-native-read-batch=32;js-native-write-batch=16;go-frame-batch=16;rust-frame-batch=32;concurrent-streams=1'
LABELS='encrypted;tls-pinned-server-certificate-hash;tiny;per-message-serialized;none;steady-state-warmed'
CONNECT_LABELS='encrypted;tls-skip-verify;tiny;per-message-serialized;none;steady-state-warmed'

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

assert_not_contains() {
    local file=$1
    local unexpected=$2
    if grep -Fq -- "$unexpected" "$file"; then
        printf 'expected %s not to contain: %s\n' "$file" "$unexpected" >&2
        return 1
    fi
}

write_current_fixture() {
    local out_dir=$1
    mkdir -p "$out_dir"
    printf '%s\n' "$SAMPLES_HEADER" >"$out_dir/webtransport-samples.csv"
    printf '1,chrome,rust_quinn,unary_latency,10.000,100000.000,10,0.100,playwright-chromium,encrypted,tls-pinned-server-certificate-hash,tiny,23,29,per-message-serialized,none,steady-state-warmed,%s,%s,enabled,enabled,enabled,enabled,1234,234,low-latency\n' "$DIAGNOSTIC_BATCHING" "$LABELS" >>"$out_dir/webtransport-samples.csv"
    printf '1,chrome,go_connect,client_stream_latency,N/A,N/A,N/A,N/A,playwright-chromium,encrypted,tls-skip-verify,tiny,23,29,per-message-serialized,none,steady-state-warmed,%s,%s,enabled,enabled,enabled,enabled,1234,234,low-latency\n' "$DIAGNOSTIC_BATCHING" "$CONNECT_LABELS" >>"$out_dir/webtransport-samples.csv"
    printf '%s\n' "$FAILURES_HEADER" >"$out_dir/webtransport-failures.csv"
    printf '2,chrome,rust_quinn,playwright-chromium,124,%s/raw/failure.txt,%s,enabled,enabled,enabled,enabled,23,29,1234,234,low-latency\n' "$out_dir" "$DIAGNOSTIC_BATCHING" >>"$out_dir/webtransport-failures.csv"
}

run_diagnostic_report() {
    local out_dir=$1
    shift
    env \
        OUT_DIR="$out_dir" \
        BENCHMARK_PROFILE=production-representative \
        ENCODED_REQUEST_BYTES=23 \
        ENCODED_RESPONSE_BYTES=29 \
        WEBTRANSPORT_CONGESTION_CONTROL=low-latency \
        TREVRPC_C_FRAME_TRACE=yes \
        TREVRPC_RUST_SPLIT_BENCH_QUINN_MAX_IDLE_TIMEOUT_MS=1234 \
        TREVRPC_RUST_SPLIT_BENCH_QUINN_KEEP_ALIVE_MS=234 \
        TREVRPC_RUST_SPLIT_BENCH_QUINN_QLOG="$out_dir/server.qlog" \
        TREVRPC_RUST_SPLIT_BENCH_QUINN_PROTO_TRACE=on \
        SSLKEYLOGFILE="$out_dir/rust.keys" \
        "$@" \
        bash "$RUNNER" --report-only
}

expect_selected_mismatch() {
    local name=$1
    local assignment=$2
    local expected=$3
    local out_dir="$TMP_DIR/mismatch-$name"
    write_current_fixture "$out_dir"
    if run_diagnostic_report "$out_dir" "$assignment" >"$out_dir.stdout" 2>"$out_dir.stderr"; then
        fail "report-only accepted mismatched $name"
    fi
    assert_contains "$out_dir.stderr" "$expected"
}

help_file="$TMP_DIR/help.txt"
bash "$RUNNER" --help >"$help_file"
assert_contains "$help_file" 'Report-only requires the exact immutable transport and instrumentation schema.'
assert_contains "$help_file" 'TREVRPC_RUST_SPLIT_BENCH_QUINN_QLOG'
assert_not_contains "$help_file" 'TREVRPC_RUST_QUINN_FRAME_TRACE'
assert_not_contains "$help_file" 'WEBTRANSPORT_STREAM_READ_BATCH'
assert_not_contains "$help_file" 'WEBTRANSPORT_DISABLE_STREAM_TIMEOUTS'
env TREVRPC_C_FRAME_TRACE=false TREVRPC_RUST_SPLIT_BENCH_QUINN_PROTO_TRACE=off bash "$RUNNER" --help >"$TMP_DIR/disabled-help.txt"
if env TREVRPC_C_FRAME_TRACE=maybe bash "$RUNNER" --help >"$TMP_DIR/invalid.stdout" 2>"$TMP_DIR/invalid.stderr"; then
    fail 'invalid trace boolean was accepted'
fi
assert_contains "$TMP_DIR/invalid.stderr" 'must be one of 1, true, TRUE, yes, on'

valid_dir="$TMP_DIR/valid"
write_current_fixture "$valid_dir"
run_diagnostic_report "$valid_dir" >"$TMP_DIR/valid.stdout" 2>"$TMP_DIR/valid.stderr"

assert_contains "$valid_dir/webtransport.md" "| Benchmark profile | \`diagnostic-instrumented\` |"
assert_contains "$valid_dir/webtransport.md" "| Rust Quinn qlog | \`enabled\` |"
assert_contains "$valid_dir/webtransport.md" "| RPC stream idle timeout | production default (\`30000\` ms) |"
assert_contains "$valid_dir/webtransport.md" 'immutable sample metadata'
awk -F, '
    NR == 1 && NF != 30 {
        printf "aggregate header has %d fields, expected 30\n", NF > "/dev/stderr"
        bad = 1
    }
    NR > 1 && NF != 30 {
        printf "aggregate row %d has %d fields, expected 30\n", NR, NF > "/dev/stderr"
        bad = 1
    }
    NR > 1 && ($17 != "23" || $18 != "29" || $24 != "enabled" || $25 != "enabled" || $26 != "enabled" || $27 != "enabled" || $28 != "1234" || $29 != "234" || $30 != "low-latency") {
        print "aggregate row did not preserve immutable report settings" > "/dev/stderr"
        bad = 1
    }
    END {
        if (NR != 3) {
            printf "aggregate has %d rows, expected 3\n", NR > "/dev/stderr"
            bad = 1
        }
        exit bad ? 1 : 0
    }
' "$valid_dir/webtransport.csv"

legacy_dir="$TMP_DIR/legacy"
mkdir -p "$legacy_dir"
printf '%s\n' 'run,browser,server,shape,latency_us,throughput_per_s,iterations,elapsed_s,source,transport_security_mode,certificate_verification_mode,payload_profile,encoded_request_bytes,encoded_response_bytes,serialization_mode,metadata_profile,handshake_inclusion_mode,batching_settings,labels,rust_quinn_ack_threshold,rust_quinn_ack_delay_ms,c_frame_trace,rust_quinn_frame_trace,rust_quinn_qlog,rust_quinn_protocol_trace,rust_tls_keylog,rust_quinn_max_idle_timeout_ms,rust_quinn_keep_alive_ms,rust_quinn_send_window_bytes,browser_stream_timeout_disable,browser_congestion_control,c_msquic_profile,c_msquic_profile_kind' >"$legacy_dir/webtransport-samples.csv"
printf '%s\n' 'run,browser,server,source,status,raw_file,rust_quinn_ack_threshold,rust_quinn_ack_delay_ms,batching_settings,c_frame_trace,rust_quinn_frame_trace,rust_quinn_qlog,rust_quinn_protocol_trace,rust_tls_keylog,encoded_request_bytes,encoded_response_bytes,rust_quinn_max_idle_timeout_ms,rust_quinn_keep_alive_ms,rust_quinn_send_window_bytes,browser_stream_timeout_disable,browser_congestion_control,c_msquic_profile,c_msquic_profile_kind' >"$legacy_dir/webtransport-failures.csv"
if OUT_DIR="$legacy_dir" bash "$RUNNER" --report-only >"$TMP_DIR/legacy.stdout" 2>"$TMP_DIR/legacy.stderr"; then
    fail 'report-only accepted the legacy schema'
fi
assert_contains "$TMP_DIR/legacy.stderr" 'schema mismatch'
assert_contains "$TMP_DIR/legacy.stderr" 'state is never inferred'

expect_selected_mismatch encoded-request ENCODED_REQUEST_BYTES=24 'encoded request bytes'
expect_selected_mismatch encoded-response ENCODED_RESPONSE_BYTES=30 'encoded response bytes'
expect_selected_mismatch idle-timeout TREVRPC_RUST_SPLIT_BENCH_QUINN_MAX_IDLE_TIMEOUT_MS=1235 'Rust Quinn idle timeout'
expect_selected_mismatch keepalive TREVRPC_RUST_SPLIT_BENCH_QUINN_KEEP_ALIVE_MS=235 'Rust Quinn keepalive'
expect_selected_mismatch congestion WEBTRANSPORT_CONGESTION_CONTROL=throughput 'browser congestion control'

assert_not_contains "$RUNNER" 'TREVRPC_RUST_QUINN_FRAME_TRACE'
assert_not_contains "$RUNNER" 'TREVRPC_RUST_SPLIT_BENCH_QUINN_SEND_WINDOW_BYTES'
assert_not_contains "$RUNNER" 'TREVRPC_RUST_SPLIT_BENCH_QUINN_ACK_THRESHOLD'
assert_not_contains "$RUNNER" 'TREVRPC_RUST_SPLIT_BENCH_QUINN_ACK_DELAY_MS'
assert_not_contains "$RUNNER" 'TREVRPC_C_MSQUIC_PROFILE'
assert_not_contains "$RUNNER" 'WEBTRANSPORT_SEND_MANY_BATCH'
assert_not_contains "$RUNNER" 'WEBTRANSPORT_STREAM_READ_BATCH'
assert_not_contains "$RUNNER" 'WEBTRANSPORT_STREAM_WRITE_BATCH'
assert_not_contains "$RUNNER" 'WEBTRANSPORT_DISABLE_STREAM_TIMEOUTS'

printf 'WebTransport report-only guardrail tests passed\n'
