#!/usr/bin/env bash

set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
RUNNER="$ROOT/bench/run_webtransport.sh"
TMP_DIR=$(mktemp -d)
trap 'rm -rf "$TMP_DIR"' EXIT

SAMPLES_HEADER='run,browser,server,shape,latency_us,throughput_per_s,iterations,elapsed_s,source,transport_security_mode,certificate_verification_mode,payload_profile,encoded_request_bytes,encoded_response_bytes,serialization_mode,metadata_profile,handshake_inclusion_mode,batching_settings,labels,rust_quinn_ack_threshold,rust_quinn_ack_delay_ms,c_frame_trace,rust_quinn_frame_trace,rust_quinn_qlog,rust_quinn_protocol_trace,rust_tls_keylog,rust_quinn_max_idle_timeout_ms,rust_quinn_keep_alive_ms,rust_quinn_send_window_bytes,browser_stream_timeout_disable,browser_congestion_control,c_msquic_profile,c_msquic_profile_kind'
FAILURES_HEADER='run,browser,server,source,status,raw_file,rust_quinn_ack_threshold,rust_quinn_ack_delay_ms,batching_settings,c_frame_trace,rust_quinn_frame_trace,rust_quinn_qlog,rust_quinn_protocol_trace,rust_tls_keylog,encoded_request_bytes,encoded_response_bytes,rust_quinn_max_idle_timeout_ms,rust_quinn_keep_alive_ms,rust_quinn_send_window_bytes,browser_stream_timeout_disable,browser_congestion_control,c_msquic_profile,c_msquic_profile_kind'
DIAGNOSTIC_BATCHING='profile=diagnostic-interoperability-instrumented;send-many-batch=1;stream-read-batch=64;stream-write-batch=64;stream-write-batch-bytes=65536;concurrent-streams=1'
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

write_current_fixture() {
    local out_dir=$1
    local c_profile=${2:-safe}
    local c_profile_kind=${3:-public-default}
    mkdir -p "$out_dir"
    printf '%s\n' "$SAMPLES_HEADER" >"$out_dir/webtransport-samples.csv"
    printf '1,chrome,rust_quinn,unary_latency,10.000,100000.000,10,0.100,playwright-chromium,encrypted,tls-pinned-server-certificate-hash,tiny,23,29,per-message-serialized,none,steady-state-warmed,%s,%s,1,0,enabled,enabled,enabled,enabled,enabled,1234,234,345,enabled,low-latency,%s,%s\n' "$DIAGNOSTIC_BATCHING" "$LABELS" "$c_profile" "$c_profile_kind" >>"$out_dir/webtransport-samples.csv"
    printf '1,chrome,go_connect,client_stream_latency,N/A,N/A,N/A,N/A,playwright-chromium,encrypted,tls-skip-verify,tiny,23,29,per-message-serialized,none,steady-state-warmed,%s,%s,1,0,enabled,enabled,enabled,enabled,enabled,1234,234,345,enabled,low-latency,%s,%s\n' "$DIAGNOSTIC_BATCHING" "$CONNECT_LABELS" "$c_profile" "$c_profile_kind" >>"$out_dir/webtransport-samples.csv"
    printf '%s\n' "$FAILURES_HEADER" >"$out_dir/webtransport-failures.csv"
    printf '2,chrome,rust_quinn,playwright-chromium,124,%s/raw/failure.txt,1,0,%s,enabled,enabled,enabled,enabled,enabled,23,29,1234,234,345,enabled,low-latency,%s,%s\n' "$out_dir" "$DIAGNOSTIC_BATCHING" "$c_profile" "$c_profile_kind" >>"$out_dir/webtransport-failures.csv"
}

run_diagnostic_report() {
    local out_dir=$1
    shift
    env \
        OUT_DIR="$out_dir" \
        BENCHMARK_PROFILE=production-representative \
        ENCODED_REQUEST_BYTES=23 \
        ENCODED_RESPONSE_BYTES=29 \
        WEBTRANSPORT_DISABLE_STREAM_TIMEOUTS=true \
        WEBTRANSPORT_CONGESTION_CONTROL=low-latency \
        TREVRPC_C_MSQUIC_PROFILE=throughput-1m \
        TREVRPC_C_FRAME_TRACE=yes \
        TREVRPC_RUST_QUINN_FRAME_TRACE=TRUE \
        TREVRPC_RUST_SPLIT_BENCH_QUINN_MAX_IDLE_TIMEOUT_MS=1234 \
        TREVRPC_RUST_SPLIT_BENCH_QUINN_KEEP_ALIVE_MS=234 \
        TREVRPC_RUST_SPLIT_BENCH_QUINN_SEND_WINDOW_BYTES=345 \
        TREVRPC_RUST_SPLIT_BENCH_QUINN_ACK_THRESHOLD=1 \
        TREVRPC_RUST_SPLIT_BENCH_QUINN_ACK_DELAY_MS=0 \
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
env TREVRPC_C_FRAME_TRACE=false TREVRPC_RUST_QUINN_FRAME_TRACE=FALSE TREVRPC_RUST_SPLIT_BENCH_QUINN_PROTO_TRACE=off bash "$RUNNER" --help >"$TMP_DIR/disabled-help.txt"
if env TREVRPC_C_FRAME_TRACE=maybe bash "$RUNNER" --help >"$TMP_DIR/invalid.stdout" 2>"$TMP_DIR/invalid.stderr"; then
    fail 'invalid trace boolean was accepted'
fi
assert_contains "$TMP_DIR/invalid.stderr" 'must be one of 1, true, TRUE, yes, on'

valid_dir="$TMP_DIR/valid"
write_current_fixture "$valid_dir"
run_diagnostic_report "$valid_dir" >"$TMP_DIR/valid.stdout" 2>"$TMP_DIR/valid.stderr"

assert_contains "$valid_dir/webtransport.md" "| Benchmark profile | \`diagnostic-interoperability-instrumented\` |"
assert_contains "$valid_dir/webtransport.md" "| Rust Quinn qlog | \`enabled\` |"
assert_contains "$valid_dir/webtransport.md" "| C MsQuic profile | \`safe\` |"
assert_contains "$valid_dir/webtransport.md" "| Browser stream-timeout disable | \`enabled\` |"
assert_contains "$valid_dir/webtransport.md" 'immutable sample metadata'
awk -F, '
    NR == 1 && NF != 37 {
        printf "aggregate header has %d fields, expected 37\n", NF > "/dev/stderr"
        bad = 1
    }
    NR > 1 && NF != 37 {
        printf "aggregate row %d has %d fields, expected 37\n", NR, NF > "/dev/stderr"
        bad = 1
    }
    NR > 1 && ($17 != "23" || $18 != "29" || $24 != "1" || $25 != "0" || $26 != "enabled" || $27 != "enabled" || $28 != "enabled" || $29 != "enabled" || $30 != "enabled" || $31 != "1234" || $32 != "234" || $33 != "345" || $34 != "enabled" || $35 != "low-latency" || $36 != "safe" || $37 != "public-default") {
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
printf '%s\n' 'run,browser,server,shape,latency_us,throughput_per_s,iterations,elapsed_s,source,transport_security_mode,certificate_verification_mode,payload_profile,encoded_request_bytes,encoded_response_bytes,serialization_mode,metadata_profile,handshake_inclusion_mode,batching_settings,labels' >"$legacy_dir/webtransport-samples.csv"
printf '%s\n' 'run,browser,server,source,status,raw_file' >"$legacy_dir/webtransport-failures.csv"
if OUT_DIR="$legacy_dir" bash "$RUNNER" --report-only >"$TMP_DIR/legacy.stdout" 2>"$TMP_DIR/legacy.stderr"; then
    fail 'report-only accepted the legacy schema'
fi
assert_contains "$TMP_DIR/legacy.stderr" 'schema mismatch'
assert_contains "$TMP_DIR/legacy.stderr" 'state is never inferred'

expect_selected_mismatch encoded-request ENCODED_REQUEST_BYTES=24 'encoded request bytes'
expect_selected_mismatch encoded-response ENCODED_RESPONSE_BYTES=30 'encoded response bytes'
expect_selected_mismatch idle-timeout TREVRPC_RUST_SPLIT_BENCH_QUINN_MAX_IDLE_TIMEOUT_MS=1235 'Rust Quinn idle timeout'
expect_selected_mismatch keepalive TREVRPC_RUST_SPLIT_BENCH_QUINN_KEEP_ALIVE_MS=235 'Rust Quinn keepalive'
expect_selected_mismatch send-window TREVRPC_RUST_SPLIT_BENCH_QUINN_SEND_WINDOW_BYTES=346 'Rust Quinn send window'
expect_selected_mismatch stream-timeout WEBTRANSPORT_DISABLE_STREAM_TIMEOUTS=0 'stream-timeout disable state'
expect_selected_mismatch congestion WEBTRANSPORT_CONGESTION_CONTROL=throughput 'browser congestion control'

c_profile_dir="$TMP_DIR/mismatch-c-profile"
write_current_fixture "$c_profile_dir" throughput-1m public-default
if run_diagnostic_report "$c_profile_dir" >"$c_profile_dir.stdout" 2>"$c_profile_dir.stderr"; then
    fail 'report-only accepted a non-safe recorded C profile'
fi
assert_contains "$c_profile_dir.stderr" 'C MsQuic profile'

c_profile_kind_dir="$TMP_DIR/mismatch-c-profile-kind"
write_current_fixture "$c_profile_kind_dir" safe public-throughput-1m
if run_diagnostic_report "$c_profile_kind_dir" >"$c_profile_kind_dir.stdout" 2>"$c_profile_kind_dir.stderr"; then
    fail 'report-only accepted a mismatched C profile kind'
fi
assert_contains "$c_profile_kind_dir.stderr" 'C MsQuic profile kind'

assert_contains "$RUNNER" 'RUST_BENCH_ENV_UNSET+=("-u" "TREVRPC_RUST_SPLIT_BENCH_QUINN_ACK_THRESHOLD")'
assert_contains "$RUNNER" 'RUST_BENCH_ENV_UNSET+=("-u" "TREVRPC_RUST_SPLIT_BENCH_QUINN_ACK_DELAY_MS")'
assert_contains "$RUNNER" 'RUST_BENCH_ENV_UNSET+=("-u" "TREVRPC_RUST_SPLIT_BENCH_QUINN_SEND_WINDOW_BYTES")'
assert_contains "$RUNNER" "\"TREVRPC_C_MSQUIC_PROFILE=\$C_MSQUIC_PROFILE\""
assert_contains "$RUNNER" "start_server webtransport-rust-server env \"\${RUST_BENCH_ENV_UNSET[@]}\""

printf 'WebTransport report-only guardrail tests passed\n'
