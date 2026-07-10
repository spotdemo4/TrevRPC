#!/usr/bin/env bash

set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
OUT_DIR_WAS_SET=${OUT_DIR+x}
CMAKE_BUILD_DIR_WAS_SET=${CMAKE_BUILD_DIR+x}
OUT_DIR=${OUT_DIR:-target/rpc-split}
case "$OUT_DIR" in
/*) OUT_DIR_ABS="$OUT_DIR" ;;
*) OUT_DIR_ABS="$ROOT/$OUT_DIR" ;;
esac
RAW_DIR="$OUT_DIR/raw"
COMMAND_LOG="$OUT_DIR/commands.txt"
CSV="$OUT_DIR/rpc-split.csv"
SAMPLES_CSV="$OUT_DIR/rpc-split-samples.csv"
FAILURES_CSV="$OUT_DIR/rpc-split-failures.csv"
MARKDOWN="$OUT_DIR/rpc-split.md"

SPLIT_ITERATIONS=${SPLIT_ITERATIONS:-10000}
C_ITERATIONS=${C_ITERATIONS:-$SPLIT_ITERATIONS}
GO_ITERATIONS=${GO_ITERATIONS:-$SPLIT_ITERATIONS}
JS_ITERATIONS=${JS_ITERATIONS:-$SPLIT_ITERATIONS}
RUST_ITERATIONS=${RUST_ITERATIONS:-$SPLIT_ITERATIONS}
SPLIT_RUNS=${SPLIT_RUNS:-3}
SAMPLE_TIMEOUT_SECONDS=${SAMPLE_TIMEOUT_SECONDS:-900}
CMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE:-Release}
CMAKE_BUILD_DIR=${CMAKE_BUILD_DIR:-"$OUT_DIR/c-build"}
SERVER_STARTUP_TIMEOUT_SECONDS=${SERVER_STARTUP_TIMEOUT_SECONDS:-15}
SKIP_BUILD_PREREQUISITES=${SKIP_BUILD_PREREQUISITES:-0}

RUN_CLIENT_AXIS=${RUN_CLIENT_AXIS:-1}
RUN_SERVER_AXIS=${RUN_SERVER_AXIS:-1}
RUN_C_MSQUIC=${RUN_C_MSQUIC:-1}
RUN_GO_QUIC=${RUN_GO_QUIC:-1}
RUN_GO_GRPC=${RUN_GO_GRPC:-1}
RUN_JS_NATIVE=${RUN_JS_NATIVE:-1}
RUN_RUST_QUINN=${RUN_RUST_QUINN:-1}
RUN_RUST_GRPC=${RUN_RUST_GRPC:-1}

PAYLOAD_PROFILE=${PAYLOAD_PROFILE:-tiny}
METADATA_PROFILE=${METADATA_PROFILE:-none}
HANDSHAKE_INCLUSION_MODE=${HANDSHAKE_INCLUSION_MODE:-steady-state-warmed}
BENCHMARK_PROFILE=${BENCHMARK_PROFILE:-production-representative}
SERIALIZATION_MODE=per-message-serialized
RUST_QUINN_MAX_IDLE_TIMEOUT_MS=${TREVRPC_RUST_SPLIT_BENCH_QUINN_MAX_IDLE_TIMEOUT_MS:-600000}
RUST_QUINN_KEEP_ALIVE_MS=${TREVRPC_RUST_SPLIT_BENCH_QUINN_KEEP_ALIVE_MS:-5000}
RUST_QUINN_SEND_WINDOW_BYTES=${TREVRPC_RUST_SPLIT_BENCH_QUINN_SEND_WINDOW_BYTES:-bounded-default}
RUST_QUINN_ACK_THRESHOLD=${TREVRPC_RUST_SPLIT_BENCH_QUINN_ACK_THRESHOLD:-library-default}
RUST_QUINN_ACK_DELAY_MS=${TREVRPC_RUST_SPLIT_BENCH_QUINN_ACK_DELAY_MS:-library-default}
C_FRAME_TRACE=${TREVRPC_C_FRAME_TRACE:-0}
RUST_QUINN_FRAME_TRACE=${TREVRPC_RUST_QUINN_FRAME_TRACE:-0}
RUST_QUINN_PROTOCOL_TRACE=${TREVRPC_RUST_SPLIT_BENCH_QUINN_PROTO_TRACE:-0}
C_FRAME_TRACE_STATE=disabled
RUST_QUINN_FRAME_TRACE_STATE=disabled
RUST_QUINN_QLOG_STATE=disabled
RUST_QUINN_PROTOCOL_TRACE_STATE=disabled
RUST_TLS_KEYLOG_STATE=disabled
if [[ "$C_FRAME_TRACE" == "1" ]]; then C_FRAME_TRACE_STATE=enabled; fi
if [[ "$RUST_QUINN_FRAME_TRACE" == "1" ]]; then RUST_QUINN_FRAME_TRACE_STATE=enabled; fi
if [[ -n "${TREVRPC_RUST_SPLIT_BENCH_QUINN_QLOG:-}" ]]; then RUST_QUINN_QLOG_STATE=enabled; fi
if [[ "$RUST_QUINN_PROTOCOL_TRACE" == "1" ]]; then RUST_QUINN_PROTOCOL_TRACE_STATE=enabled; fi
if [[ -n "${SSLKEYLOGFILE:-}" ]]; then RUST_TLS_KEYLOG_STATE=enabled; fi
RUST_QUINN_ACK_PROFILE=library-default
INSTRUMENTATION_PROFILE=untraced
if [[ "$RUST_QUINN_ACK_THRESHOLD" != "library-default" || "$RUST_QUINN_ACK_DELAY_MS" != "library-default" ]]; then
    RUST_QUINN_ACK_PROFILE=diagnostic-interoperability
    BENCHMARK_PROFILE=diagnostic-interoperability
fi
if [[ "$C_FRAME_TRACE_STATE" == "enabled" || "$RUST_QUINN_FRAME_TRACE_STATE" == "enabled" || "$RUST_QUINN_QLOG_STATE" == "enabled" || "$RUST_QUINN_PROTOCOL_TRACE_STATE" == "enabled" || "$RUST_TLS_KEYLOG_STATE" == "enabled" ]]; then
    INSTRUMENTATION_PROFILE=diagnostic-instrumented
    if [[ "$RUST_QUINN_ACK_PROFILE" == "diagnostic-interoperability" ]]; then
        BENCHMARK_PROFILE=diagnostic-interoperability-instrumented
    else
        BENCHMARK_PROFILE=diagnostic-instrumented
    fi
fi
SPLIT_BATCHING_SETTINGS=${SPLIT_BATCHING_SETTINGS:-profile=$BENCHMARK_PROFILE;js-send-many-batch=${TREVRPC_JS_SEND_MANY_BATCH:-16};go-frame-batch=16;rust-frame-batch=32;grpc-batching=library-default}
if [[ "$SPLIT_BATCHING_SETTINGS" != "profile=$BENCHMARK_PROFILE;"* ]]; then
    printf 'SPLIT_BATCHING_SETTINGS must begin with profile=%s;\n' "$BENCHMARK_PROFILE" >&2
    exit 2
fi
C_MSQUIC_PROFILE=${TREVRPC_C_MSQUIC_PROFILE:-safe}

GO_SPLIT_BENCH="$OUT_DIR_ABS/trevrpc-go-rpc-split-bench"
RUST_SPLIT_BENCH="$ROOT/trevrpc-rust/target/release/examples/rpc_split_bench"

usage() {
    cat <<'EOF'
Usage: bench/run_rpc_split.sh
       bench/run_rpc_split.sh --report-only
       bench/run_rpc_split.sh --smoke

Runs split TrevRPC client/server benchmarks plus Go and Rust gRPC baselines, and writes CSV/Markdown reports.

Report-only requires immutable transport and instrumentation columns from this schema. Older outputs must use their recorded script snapshot or be explicitly migrated; missing state is never inferred.

Environment knobs:
  OUT_DIR                 Output directory. Default: target/rpc-split
  SPLIT_ITERATIONS        Shared fixed iteration count. Default: 10000
  SPLIT_RUNS              Measurement command repetitions. Default: 3
  SAMPLE_TIMEOUT_SECONDS  Per-sample timeout. Default: 900
  SKIP_BUILD_PREREQUISITES
                          Set to 1 only when all benchmark artifacts were built separately.
  C_ITERATIONS            C client iteration count. Default: SPLIT_ITERATIONS
  GO_ITERATIONS           Go client iteration count. Default: SPLIT_ITERATIONS
  JS_ITERATIONS           JavaScript client iteration count. Default: SPLIT_ITERATIONS
  RUST_ITERATIONS         Rust client iteration count. Default: SPLIT_ITERATIONS
  CMAKE_BUILD_TYPE        CMake build type for C benchmarks. Default: Release
  RUN_CLIENT_AXIS         Benchmark clients against reference C servers. Default: 1
  RUN_SERVER_AXIS         Benchmark servers with reference C clients. Default: 1
  RUN_C_MSQUIC            Include C MsQuic transport. Default: 1
  RUN_GO_QUIC             Include Go quic-go transport. Default: 1
  RUN_GO_GRPC             Include Go gRPC TCP baseline. Default: 1
  RUN_JS_NATIVE           Include native JS MsQuic transport. Default: 1
  RUN_RUST_QUINN          Include Rust Quinn transport. Default: 1
  RUN_RUST_GRPC           Include Rust tonic gRPC TCP baseline. Default: 1
  PAYLOAD_PROFILE         Payload profile: tiny, small, medium, large, or mixed. Default: tiny
  METADATA_PROFILE        Metadata profile: none or production. Default: none
  BENCHMARK_PROFILE       Base profile label. ACK tuning or instrumentation forces a diagnostic label.
  TREVRPC_C_MSQUIC_PROFILE
                          C MsQuic profile: safe, receive-1m, or throughput-1m. Default: safe
  TREVRPC_C_FRAME_TRACE   Set to 1 to emit C wire frame trace lines on stderr.
  TREVRPC_RUST_QUINN_FRAME_TRACE
                          Set to 1 to emit Rust Quinn frame/FIN/reset trace lines on stderr.
  TREVRPC_RUST_SPLIT_BENCH_SHAPES
                          Comma-separated Rust client and C server-axis shapes for focused diagnostics.
  TREVRPC_RUST_SPLIT_BENCH_QUINN_ACK_THRESHOLD
                          Diagnostic peer ACK-eliciting threshold or library-default. Default: library-default.
  TREVRPC_RUST_SPLIT_BENCH_QUINN_ACK_DELAY_MS
                          Diagnostic peer ACK delay in ms or library-default. Default: library-default.
  TREVRPC_RUST_SPLIT_BENCH_QUINN_QLOG
                          Rust split server qlog output path for packet diagnostics.
  TREVRPC_RUST_SPLIT_BENCH_QUINN_PROTO_TRACE
                          Set to 1 to emit timestamped Quinn packet/frame protocol traces.
  SSLKEYLOGFILE           Rust split bench writes TLS secrets here when set.

Examples:
  bench/run_rpc_split.sh
  bench/run_rpc_split.sh --report-only
  bench/run_rpc_split.sh --smoke
  SPLIT_ITERATIONS=1000 SPLIT_RUNS=1 bench/run_rpc_split.sh
  RUN_SERVER_AXIS=0 bench/run_rpc_split.sh
EOF
}

REPORT_ONLY=0
SMOKE=0
case "${1:-}" in
"-h" | "--help")
    usage
    exit 0
    ;;
"--report-only")
    REPORT_ONLY=1
    ;;
"--smoke")
    SMOKE=1
    ;;
"") ;;
*)
    usage >&2
    exit 2
    ;;
esac

if [[ "$SMOKE" == "1" ]]; then
    SPLIT_ITERATIONS=10
    C_ITERATIONS=10
    GO_ITERATIONS=10
    JS_ITERATIONS=10
    RUST_ITERATIONS=10
    SPLIT_RUNS=1
    if [[ -z "$OUT_DIR_WAS_SET" ]]; then
        OUT_DIR=target/rpc-split-smoke
    fi
fi

case "$OUT_DIR" in
/*) OUT_DIR_ABS="$OUT_DIR" ;;
*) OUT_DIR_ABS="$ROOT/$OUT_DIR" ;;
esac
RAW_DIR="$OUT_DIR/raw"
COMMAND_LOG="$OUT_DIR/commands.txt"
CSV="$OUT_DIR/rpc-split.csv"
SAMPLES_CSV="$OUT_DIR/rpc-split-samples.csv"
FAILURES_CSV="$OUT_DIR/rpc-split-failures.csv"
MARKDOWN="$OUT_DIR/rpc-split.md"
GO_SPLIT_BENCH="$OUT_DIR_ABS/trevrpc-go-rpc-split-bench"
if [[ -z "$CMAKE_BUILD_DIR_WAS_SET" ]]; then
    CMAKE_BUILD_DIR="$OUT_DIR/c-build"
fi

profile_encoded_bytes() {
    case "$1" in
    tiny) printf '19' ;;
    small) printf '256' ;;
    medium) printf '4096' ;;
    large) printf '65536' ;;
    mixed) printf '19/256/4096/65536' ;;
    *)
        printf 'unsupported PAYLOAD_PROFILE %q\n' "$1" >&2
        exit 2
        ;;
    esac
}

case "$METADATA_PROFILE" in
none | production) ;;
*)
    printf 'unsupported METADATA_PROFILE %q\n' "$METADATA_PROFILE" >&2
    exit 2
    ;;
esac

case "$C_MSQUIC_PROFILE" in
safe)
    C_MSQUIC_PROFILE_KIND=public-default
    C_MSQUIC_STREAM_RECV_WINDOW=unset-msquic-library-default
    C_MSQUIC_CONN_FLOW_CONTROL_WINDOW=unset-msquic-library-default
    C_MSQUIC_SEND_BUFFERING=disabled
    C_MSQUIC_EXECUTION_PROFILE=low-latency
    ;;
receive-1m)
    C_MSQUIC_PROFILE_KIND=benchmark-only-receive-recipe
    C_MSQUIC_STREAM_RECV_WINDOW=1048576
    C_MSQUIC_CONN_FLOW_CONTROL_WINDOW=67108864
    C_MSQUIC_SEND_BUFFERING=disabled
    C_MSQUIC_EXECUTION_PROFILE=low-latency
    ;;
throughput-1m)
    C_MSQUIC_PROFILE_KIND=public-throughput-1m
    C_MSQUIC_STREAM_RECV_WINDOW=1048576
    C_MSQUIC_CONN_FLOW_CONTROL_WINDOW=67108864
    C_MSQUIC_SEND_BUFFERING=enabled
    C_MSQUIC_EXECUTION_PROFILE=max-throughput
    ;;
*)
    printf 'unsupported TREVRPC_C_MSQUIC_PROFILE %q\n' "$C_MSQUIC_PROFILE" >&2
    exit 2
    ;;
esac

C_MSQUIC_PEER_BIDI_STREAMS=128
C_MSQUIC_APP_STREAM_CONCURRENCY=64
C_MSQUIC_MAX_FRAME_SIZE=4194304
C_MSQUIC_MAX_STREAM_BODY_SIZE=16777216
C_MSQUIC_MAX_PENDING_SEND_BYTES=67108864
C_MSQUIC_MAX_PENDING_SEND_COUNT=1024
C_MSQUIC_IDLE_TIMEOUT_MS=600000
C_RPC_STREAM_IDLE_TIMEOUT=disabled
C_WEBTRANSPORT_STREAMS_PER_SESSION=128
C_WEBTRANSPORT_SESSIONS_PER_CONNECTION=16

ENCODED_REQUEST_BYTES=${ENCODED_REQUEST_BYTES:-$(profile_encoded_bytes "$PAYLOAD_PROFILE")}
ENCODED_RESPONSE_BYTES=${ENCODED_RESPONSE_BYTES:-$(profile_encoded_bytes "$PAYLOAD_PROFILE")}
BENCH_ENV=(
    "TREVRPC_BENCH_PAYLOAD_PROFILE=$PAYLOAD_PROFILE"
    "TREVRPC_BENCH_METADATA_PROFILE=$METADATA_PROFILE"
    "TREVRPC_C_MSQUIC_PROFILE=$C_MSQUIC_PROFILE"
)
if [[ -n "${TREVRPC_C_FRAME_TRACE:-}" ]]; then
    BENCH_ENV+=("TREVRPC_C_FRAME_TRACE=$TREVRPC_C_FRAME_TRACE")
fi
RUST_BENCH_ENV=(
    "${BENCH_ENV[@]}"
    "TREVRPC_RUST_SPLIT_BENCH_QUINN_MAX_IDLE_TIMEOUT_MS=$RUST_QUINN_MAX_IDLE_TIMEOUT_MS"
    "TREVRPC_RUST_SPLIT_BENCH_QUINN_KEEP_ALIVE_MS=$RUST_QUINN_KEEP_ALIVE_MS"
)
RUST_BENCH_ENV_UNSET=()
if [[ -n "${TREVRPC_RUST_SPLIT_BENCH_SHAPES:-}" ]]; then
    RUST_BENCH_ENV+=("TREVRPC_RUST_SPLIT_BENCH_SHAPES=$TREVRPC_RUST_SPLIT_BENCH_SHAPES")
fi
if [[ -n "${TREVRPC_RUST_QUINN_FRAME_TRACE:-}" ]]; then
    RUST_BENCH_ENV+=("TREVRPC_RUST_QUINN_FRAME_TRACE=$TREVRPC_RUST_QUINN_FRAME_TRACE")
fi
if [[ -n "${TREVRPC_RUST_SPLIT_BENCH_QUINN_QLOG:-}" ]]; then
    RUST_BENCH_ENV+=("TREVRPC_RUST_SPLIT_BENCH_QUINN_QLOG=$TREVRPC_RUST_SPLIT_BENCH_QUINN_QLOG")
fi
if [[ -n "${TREVRPC_RUST_SPLIT_BENCH_QUINN_PROTO_TRACE:-}" ]]; then
    RUST_BENCH_ENV+=("TREVRPC_RUST_SPLIT_BENCH_QUINN_PROTO_TRACE=$TREVRPC_RUST_SPLIT_BENCH_QUINN_PROTO_TRACE")
fi
if [[ -n "${SSLKEYLOGFILE:-}" ]]; then
    RUST_BENCH_ENV+=("SSLKEYLOGFILE=$SSLKEYLOGFILE")
fi
if [[ "$RUST_QUINN_SEND_WINDOW_BYTES" != "bounded-default" ]]; then
    RUST_BENCH_ENV+=("TREVRPC_RUST_SPLIT_BENCH_QUINN_SEND_WINDOW_BYTES=$RUST_QUINN_SEND_WINDOW_BYTES")
fi
if [[ "$RUST_QUINN_ACK_THRESHOLD" != "library-default" ]]; then
    RUST_BENCH_ENV+=("TREVRPC_RUST_SPLIT_BENCH_QUINN_ACK_THRESHOLD=$RUST_QUINN_ACK_THRESHOLD")
else
    RUST_BENCH_ENV_UNSET+=("-u" "TREVRPC_RUST_SPLIT_BENCH_QUINN_ACK_THRESHOLD")
fi
if [[ "$RUST_QUINN_ACK_DELAY_MS" != "library-default" ]]; then
    RUST_BENCH_ENV+=("TREVRPC_RUST_SPLIT_BENCH_QUINN_ACK_DELAY_MS=$RUST_QUINN_ACK_DELAY_MS")
else
    RUST_BENCH_ENV_UNSET+=("-u" "TREVRPC_RUST_SPLIT_BENCH_QUINN_ACK_DELAY_MS")
fi

initialize_output() {
    mkdir -p "$RAW_DIR"
    : >"$COMMAND_LOG"
    rm -f "$RAW_DIR"/*.txt
    printf 'axis,run,client,server,shape,latency_us,throughput_per_s,iterations,elapsed_s,source,transport_security_mode,certificate_verification_mode,payload_profile,encoded_request_bytes,encoded_response_bytes,serialization_mode,metadata_profile,handshake_inclusion_mode,batching_settings,labels,rust_quinn_ack_threshold,rust_quinn_ack_delay_ms,c_msquic_profile,c_msquic_profile_kind,c_frame_trace,rust_quinn_frame_trace,rust_quinn_qlog,rust_quinn_protocol_trace,rust_tls_keylog\n' >"$SAMPLES_CSV"
    printf 'axis,run,client,server,source,status,raw_file,rust_quinn_ack_threshold,rust_quinn_ack_delay_ms,c_msquic_profile,c_msquic_profile_kind,batching_settings,c_frame_trace,rust_quinn_frame_trace,rust_quinn_qlog,rust_quinn_protocol_trace,rust_tls_keylog\n' >"$FAILURES_CSV"
}

SERVER_PIDS=()
START_SERVER_PORT=""

require_positive_integer() {
    local name=$1
    local value=$2
    if [[ ! "$value" =~ ^[1-9][0-9]*$ ]]; then
        printf '%s must be a positive integer, got %q\n' "$name" "$value" >&2
        exit 2
    fi
}

require_positive_integer SPLIT_RUNS "$SPLIT_RUNS"
require_positive_integer SAMPLE_TIMEOUT_SECONDS "$SAMPLE_TIMEOUT_SECONDS"
require_positive_integer C_ITERATIONS "$C_ITERATIONS"
require_positive_integer GO_ITERATIONS "$GO_ITERATIONS"
require_positive_integer JS_ITERATIONS "$JS_ITERATIONS"
require_positive_integer RUST_ITERATIONS "$RUST_ITERATIONS"

require_boolean() {
    local name=$1
    local value=$2
    if [[ "$value" != "0" && "$value" != "1" ]]; then
        printf '%s must be 0 or 1, got %q\n' "$name" "$value" >&2
        exit 2
    fi
}

require_boolean SKIP_BUILD_PREREQUISITES "$SKIP_BUILD_PREREQUISITES"
require_boolean RUN_CLIENT_AXIS "$RUN_CLIENT_AXIS"
require_boolean RUN_SERVER_AXIS "$RUN_SERVER_AXIS"
require_boolean RUN_C_MSQUIC "$RUN_C_MSQUIC"
require_boolean RUN_GO_QUIC "$RUN_GO_QUIC"
require_boolean RUN_GO_GRPC "$RUN_GO_GRPC"
require_boolean RUN_JS_NATIVE "$RUN_JS_NATIVE"
require_boolean RUN_RUST_QUINN "$RUN_RUST_QUINN"
require_boolean RUN_RUST_GRPC "$RUN_RUST_GRPC"
require_boolean TREVRPC_C_FRAME_TRACE "$C_FRAME_TRACE"
require_boolean TREVRPC_RUST_QUINN_FRAME_TRACE "$RUST_QUINN_FRAME_TRACE"
require_boolean TREVRPC_RUST_SPLIT_BENCH_QUINN_PROTO_TRACE "$RUST_QUINN_PROTOCOL_TRACE"

require_nonnegative_integer_or_library_default() {
    local name=$1
    local value=$2
    if [[ "$value" != "library-default" && ! "$value" =~ ^[0-9]+$ ]]; then
        printf '%s must be a non-negative integer or library-default, got %q\n' "$name" "$value" >&2
        exit 2
    fi
}

require_nonnegative_integer_or_library_default TREVRPC_RUST_SPLIT_BENCH_QUINN_ACK_THRESHOLD "$RUST_QUINN_ACK_THRESHOLD"
require_nonnegative_integer_or_library_default TREVRPC_RUST_SPLIT_BENCH_QUINN_ACK_DELAY_MS "$RUST_QUINN_ACK_DELAY_MS"

quote_command() {
    printf '%q ' "$@"
}

log_command() {
    {
        printf '$ '
        quote_command "$@"
        printf '\n'
    } | tee -a "$COMMAND_LOG"
}

run_and_capture() {
    local name=$1
    shift
    local raw_file="$RAW_DIR/$name.txt"

    log_command "$@"
    "$@" 2>&1 | tee "$raw_file"
}

append_split_csv() {
    local raw_file=$1
    local run=$2
    local axis=$3
    local client=$4
    local server=$5
    local source=$6
    awk -v axis="$axis" -v run="$run" -v client="$client" -v server="$server" -v source="$source" \
        -v payload_profile="$PAYLOAD_PROFILE" \
        -v encoded_request_bytes="$ENCODED_REQUEST_BYTES" \
        -v encoded_response_bytes="$ENCODED_RESPONSE_BYTES" \
        -v metadata_profile="$METADATA_PROFILE" \
        -v handshake_inclusion_mode="$HANDSHAKE_INCLUSION_MODE" \
        -v rust_quinn_ack_threshold="$RUST_QUINN_ACK_THRESHOLD" \
        -v rust_quinn_ack_delay_ms="$RUST_QUINN_ACK_DELAY_MS" \
        -v c_msquic_profile="$C_MSQUIC_PROFILE" \
        -v c_msquic_profile_kind="$C_MSQUIC_PROFILE_KIND" \
        -v c_frame_trace="$C_FRAME_TRACE_STATE" \
        -v rust_quinn_frame_trace="$RUST_QUINN_FRAME_TRACE_STATE" \
        -v rust_quinn_qlog="$RUST_QUINN_QLOG_STATE" \
        -v rust_quinn_protocol_trace="$RUST_QUINN_PROTOCOL_TRACE_STATE" \
        -v rust_tls_keylog="$RUST_TLS_KEYLOG_STATE" \
        -v batching_settings="$SPLIT_BATCHING_SETTINGS" -F '' '
        function transport_security_mode() {
            return "encrypted"
        }
        function certificate_verification_mode() {
            if (axis == "grpc") {
                return "tls-pinned"
            }
            return "tls-skip-verify"
        }
        function serialization_mode(shape) {
            shape = shape
            return "per-message-serialized"
        }
        function labels(shape,    security, cert, serialization) {
            security = transport_security_mode()
            cert = certificate_verification_mode()
            serialization = serialization_mode(shape)
            return security ";" cert ";" payload_profile ";" serialization ";" metadata_profile ";" handshake_inclusion_mode
        }
        function emit(shape, latency_us, throughput, iterations, elapsed) {
            serialization = serialization_mode(shape)
            printf "%s,%s,%s,%s,%s,%.3f,%.3f,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n", \
                axis, run, client, server, shape, latency_us, throughput, iterations, elapsed, source, \
                transport_security_mode(), certificate_verification_mode(), payload_profile, encoded_request_bytes, encoded_response_bytes, \
                serialization, metadata_profile, handshake_inclusion_mode, batching_settings, labels(shape), rust_quinn_ack_threshold, rust_quinn_ack_delay_ms, c_msquic_profile, c_msquic_profile_kind, \
                c_frame_trace, rust_quinn_frame_trace, rust_quinn_qlog, rust_quinn_protocol_trace, rust_tls_keylog
        }
        match($0, /^([^:]+):[[:space:]]+([0-9.]+) us\/op \(([0-9]+) iterations in ([0-9.]+)s\)/, m) {
            latency_us = m[2] + 0
            throughput = latency_us > 0 ? 1000000.0 / latency_us : 0
            emit(m[1], latency_us, throughput, m[3], m[4])
        }
        match($0, /^([^:]+):[[:space:]]+([0-9.]+) (ops\/s|messages\/s) \(([0-9]+) (iterations|messages) in ([0-9.]+)s\)/, m) {
            throughput = m[2] + 0
            latency_us = throughput > 0 ? 1000000.0 / throughput : 0
            emit(m[1], latency_us, throughput, m[4], m[6])
        }
    ' "$raw_file" >>"$SAMPLES_CSV"
}

aggregate_samples_csv() {
    awk -F, '
        function append(values, value) {
            return values == "" ? value : values " " value
        }
        function median(values,    a, n) {
            if (values == "") {
                return ""
            }
            n = split(values, a, " ")
            asort(a)
            if (n % 2 == 1) {
                return a[(n + 1) / 2] + 0
            }
            return (a[n / 2] + a[n / 2 + 1]) / 2
        }
        BEGIN {
            print "axis,client,server,shape,measurements,latency_us_median,latency_us_min,latency_us_max,throughput_per_s_median,throughput_per_s_min,throughput_per_s_max,iterations_per_measurement,elapsed_s_total,source,transport_security_mode,certificate_verification_mode,payload_profile,encoded_request_bytes,encoded_response_bytes,serialization_mode,metadata_profile,handshake_inclusion_mode,batching_settings,labels,rust_quinn_ack_threshold,rust_quinn_ack_delay_ms,c_msquic_profile,c_msquic_profile_kind,c_frame_trace,rust_quinn_frame_trace,rust_quinn_qlog,rust_quinn_protocol_trace,rust_tls_keylog"
        }
        NR == 1 {
            next
        }
        {
            key = $1 SUBSEP $3 SUBSEP $4 SUBSEP $5
            if (!(key in seen)) {
                seen[key] = 1
                order[++order_count] = key
                axis[key] = $1
                client[key] = $3
                server[key] = $4
                shape[key] = $5
                source[key] = $10
                transport_security_mode[key] = $11
                certificate_verification_mode[key] = $12
                payload_profile[key] = $13
                encoded_request_bytes[key] = $14
                encoded_response_bytes[key] = $15
                serialization_mode[key] = $16
                metadata_profile[key] = $17
                handshake_inclusion_mode[key] = $18
                batching_settings[key] = $19
                labels[key] = $20
                rust_quinn_ack_threshold[key] = $21
                rust_quinn_ack_delay_ms[key] = $22
                c_msquic_profile[key] = $23
                c_msquic_profile_kind[key] = $24
                c_frame_trace[key] = $25
                rust_quinn_frame_trace[key] = $26
                rust_quinn_qlog[key] = $27
                rust_quinn_protocol_trace[key] = $28
                rust_tls_keylog[key] = $29
            }
            measurements[key]++
            latencies[key] = append(latencies[key], $6)
            throughputs[key] = append(throughputs[key], $7)
            iterations[key] = append(iterations[key], $8)
            elapsed_total[key] += $9
            if (!(key in latency_min) || $6 + 0 < latency_min[key]) {
                latency_min[key] = $6 + 0
            }
            if (!(key in latency_max) || $6 + 0 > latency_max[key]) {
                latency_max[key] = $6 + 0
            }
            if (!(key in throughput_min) || $7 + 0 < throughput_min[key]) {
                throughput_min[key] = $7 + 0
            }
            if (!(key in throughput_max) || $7 + 0 > throughput_max[key]) {
                throughput_max[key] = $7 + 0
            }
        }
        END {
            for (i = 1; i <= order_count; i++) {
                key = order[i]
                latency = median(latencies[key])
                throughput = median(throughputs[key])
                printf "%s,%s,%s,%s,%d,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.0f,%.3f,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n", \
                    axis[key], client[key], server[key], shape[key], measurements[key], latency, latency_min[key], latency_max[key], throughput, throughput_min[key], throughput_max[key], median(iterations[key]), elapsed_total[key], source[key], \
                    transport_security_mode[key], certificate_verification_mode[key], payload_profile[key], encoded_request_bytes[key], encoded_response_bytes[key], serialization_mode[key], metadata_profile[key], handshake_inclusion_mode[key], batching_settings[key], labels[key], rust_quinn_ack_threshold[key], rust_quinn_ack_delay_ms[key], c_msquic_profile[key], c_msquic_profile_kind[key], \
                    c_frame_trace[key], rust_quinn_frame_trace[key], rust_quinn_qlog[key], rust_quinn_protocol_trace[key], rust_tls_keylog[key]
            }
        }
    ' "$SAMPLES_CSV" >"$CSV"
}

assert_sample_profile_metadata() {
    awk -F, -v rust_quinn_ack_threshold="$RUST_QUINN_ACK_THRESHOLD" \
        -v rust_quinn_ack_delay_ms="$RUST_QUINN_ACK_DELAY_MS" \
        -v c_msquic_profile="$C_MSQUIC_PROFILE" \
        -v c_msquic_profile_kind="$C_MSQUIC_PROFILE_KIND" \
        -v c_frame_trace="$C_FRAME_TRACE_STATE" \
        -v rust_quinn_frame_trace="$RUST_QUINN_FRAME_TRACE_STATE" \
        -v rust_quinn_qlog="$RUST_QUINN_QLOG_STATE" \
        -v rust_quinn_protocol_trace="$RUST_QUINN_PROTOCOL_TRACE_STATE" \
        -v rust_tls_keylog="$RUST_TLS_KEYLOG_STATE" \
        -v batching_settings="$SPLIT_BATCHING_SETTINGS" '
        NR == 1 {
            transport_schema = $21 == "rust_quinn_ack_threshold" && $22 == "rust_quinn_ack_delay_ms" &&
                $23 == "c_msquic_profile" && $24 == "c_msquic_profile_kind"
            instrumentation_schema = $25 == "c_frame_trace" && $26 == "rust_quinn_frame_trace" &&
                $27 == "rust_quinn_qlog" && $28 == "rust_quinn_protocol_trace" && $29 == "rust_tls_keylog"
            if (!transport_schema) {
                printf "legacy sample schema lacks immutable transport metadata; use its recorded script snapshot or explicitly migrate every row\n" > "/dev/stderr"
                bad = 1
            }
            if (!instrumentation_schema) {
                printf "sample schema predates immutable instrumentation metadata; refusing report-only inference or silent relabeling\n" > "/dev/stderr"
                bad = 1
            }
            schema_ok = transport_schema && instrumentation_schema
            next
        }
        schema_ok && $21 != rust_quinn_ack_threshold {
            printf "sample row %d recorded Rust Quinn ACK threshold %s does not match selected %s\n", NR, $21, rust_quinn_ack_threshold > "/dev/stderr"
            bad = 1
        }
        schema_ok && $22 != rust_quinn_ack_delay_ms {
            printf "sample row %d recorded Rust Quinn ACK delay %s does not match selected %s\n", NR, $22, rust_quinn_ack_delay_ms > "/dev/stderr"
            bad = 1
        }
        schema_ok && $23 != c_msquic_profile {
            printf "sample row %d recorded C MsQuic profile %s does not match selected %s\n", NR, $23, c_msquic_profile > "/dev/stderr"
            bad = 1
        }
        schema_ok && $24 != c_msquic_profile_kind {
            printf "sample row %d recorded C MsQuic profile kind %s does not match selected %s\n", NR, $24, c_msquic_profile_kind > "/dev/stderr"
            bad = 1
        }
        schema_ok && $25 != c_frame_trace {
            printf "sample row %d recorded C frame trace state %s does not match selected %s\n", NR, $25, c_frame_trace > "/dev/stderr"
            bad = 1
        }
        schema_ok && $26 != rust_quinn_frame_trace {
            printf "sample row %d recorded Rust Quinn frame trace state %s does not match selected %s\n", NR, $26, rust_quinn_frame_trace > "/dev/stderr"
            bad = 1
        }
        schema_ok && $27 != rust_quinn_qlog {
            printf "sample row %d recorded Rust Quinn qlog state %s does not match selected %s\n", NR, $27, rust_quinn_qlog > "/dev/stderr"
            bad = 1
        }
        schema_ok && $28 != rust_quinn_protocol_trace {
            printf "sample row %d recorded Rust Quinn protocol trace state %s does not match selected %s\n", NR, $28, rust_quinn_protocol_trace > "/dev/stderr"
            bad = 1
        }
        schema_ok && $29 != rust_tls_keylog {
            printf "sample row %d recorded Rust TLS keylog state %s does not match selected %s\n", NR, $29, rust_tls_keylog > "/dev/stderr"
            bad = 1
        }
        schema_ok && $19 != batching_settings {
            printf "sample row %d batching/profile metadata %s does not match selected %s\n", NR, $19, batching_settings > "/dev/stderr"
            bad = 1
        }
        END { exit bad ? 1 : 0 }
    ' "$SAMPLES_CSV"

    awk -F, -v rust_quinn_ack_threshold="$RUST_QUINN_ACK_THRESHOLD" \
        -v rust_quinn_ack_delay_ms="$RUST_QUINN_ACK_DELAY_MS" \
        -v c_msquic_profile="$C_MSQUIC_PROFILE" \
        -v c_msquic_profile_kind="$C_MSQUIC_PROFILE_KIND" \
        -v c_frame_trace="$C_FRAME_TRACE_STATE" \
        -v rust_quinn_frame_trace="$RUST_QUINN_FRAME_TRACE_STATE" \
        -v rust_quinn_qlog="$RUST_QUINN_QLOG_STATE" \
        -v rust_quinn_protocol_trace="$RUST_QUINN_PROTOCOL_TRACE_STATE" \
        -v rust_tls_keylog="$RUST_TLS_KEYLOG_STATE" \
        -v batching_settings="$SPLIT_BATCHING_SETTINGS" '
        NR == 1 {
            transport_schema = $8 == "rust_quinn_ack_threshold" && $9 == "rust_quinn_ack_delay_ms" &&
                $10 == "c_msquic_profile" && $11 == "c_msquic_profile_kind" && $12 == "batching_settings"
            instrumentation_schema = $13 == "c_frame_trace" && $14 == "rust_quinn_frame_trace" &&
                $15 == "rust_quinn_qlog" && $16 == "rust_quinn_protocol_trace" && $17 == "rust_tls_keylog"
            if (!transport_schema) {
                printf "legacy failure schema lacks immutable transport metadata; use its recorded script snapshot or explicitly migrate every row\n" > "/dev/stderr"
                bad = 1
            }
            if (!instrumentation_schema) {
                printf "failure schema predates immutable instrumentation metadata; refusing report-only inference or silent relabeling\n" > "/dev/stderr"
                bad = 1
            }
            schema_ok = transport_schema && instrumentation_schema
            next
        }
        schema_ok && $8 != rust_quinn_ack_threshold {
            printf "failure row %d recorded Rust Quinn ACK threshold %s does not match selected %s\n", NR, $8, rust_quinn_ack_threshold > "/dev/stderr"
            bad = 1
        }
        schema_ok && $9 != rust_quinn_ack_delay_ms {
            printf "failure row %d recorded Rust Quinn ACK delay %s does not match selected %s\n", NR, $9, rust_quinn_ack_delay_ms > "/dev/stderr"
            bad = 1
        }
        schema_ok && $10 != c_msquic_profile {
            printf "failure row %d recorded C MsQuic profile %s does not match selected %s\n", NR, $10, c_msquic_profile > "/dev/stderr"
            bad = 1
        }
        schema_ok && $11 != c_msquic_profile_kind {
            printf "failure row %d recorded C MsQuic profile kind %s does not match selected %s\n", NR, $11, c_msquic_profile_kind > "/dev/stderr"
            bad = 1
        }
        schema_ok && $12 != batching_settings {
            printf "failure row %d batching/profile metadata %s does not match selected %s\n", NR, $12, batching_settings > "/dev/stderr"
            bad = 1
        }
        schema_ok && $13 != c_frame_trace {
            printf "failure row %d recorded C frame trace state %s does not match selected %s\n", NR, $13, c_frame_trace > "/dev/stderr"
            bad = 1
        }
        schema_ok && $14 != rust_quinn_frame_trace {
            printf "failure row %d recorded Rust Quinn frame trace state %s does not match selected %s\n", NR, $14, rust_quinn_frame_trace > "/dev/stderr"
            bad = 1
        }
        schema_ok && $15 != rust_quinn_qlog {
            printf "failure row %d recorded Rust Quinn qlog state %s does not match selected %s\n", NR, $15, rust_quinn_qlog > "/dev/stderr"
            bad = 1
        }
        schema_ok && $16 != rust_quinn_protocol_trace {
            printf "failure row %d recorded Rust Quinn protocol trace state %s does not match selected %s\n", NR, $16, rust_quinn_protocol_trace > "/dev/stderr"
            bad = 1
        }
        schema_ok && $17 != rust_tls_keylog {
            printf "failure row %d recorded Rust TLS keylog state %s does not match selected %s\n", NR, $17, rust_tls_keylog > "/dev/stderr"
            bad = 1
        }
        END { exit bad ? 1 : 0 }
    ' "$FAILURES_CSV"
}

assert_no_plaintext_rows() {
    if awk -F, 'NR > 1 && $15 == "plaintext" { found = 1; exit } END { exit found ? 0 : 1 }' "$CSV"; then
        printf 'split comparison emitted plaintext rows in %s\n' "$CSV" >&2
        exit 2
    fi
}

assert_benchmark_labels() {
    awk -F, -v payload_profile="$PAYLOAD_PROFILE" -v metadata_profile="$METADATA_PROFILE" \
        -v serialization_mode="$SERIALIZATION_MODE" -v handshake_mode="$HANDSHAKE_INCLUSION_MODE" \
        -v rust_quinn_ack_threshold="$RUST_QUINN_ACK_THRESHOLD" -v rust_quinn_ack_delay_ms="$RUST_QUINN_ACK_DELAY_MS" \
        -v c_frame_trace="$C_FRAME_TRACE_STATE" -v rust_quinn_frame_trace="$RUST_QUINN_FRAME_TRACE_STATE" \
        -v rust_quinn_qlog="$RUST_QUINN_QLOG_STATE" -v rust_quinn_protocol_trace="$RUST_QUINN_PROTOCOL_TRACE_STATE" \
        -v rust_tls_keylog="$RUST_TLS_KEYLOG_STATE" '
        NR == 1 { next }
        $15 != "encrypted" {
            printf "row %d has false security label %q\n", NR, $15 > "/dev/stderr"
            bad = 1
        }
        $16 != "tls-pinned" && $16 != "tls-skip-verify" {
            printf "row %d has unexpected certificate verification label %q\n", NR, $16 > "/dev/stderr"
            bad = 1
        }
        $17 != payload_profile {
            printf "row %d payload profile %q does not match selected %q\n", NR, $17, payload_profile > "/dev/stderr"
            bad = 1
        }
        $20 != serialization_mode {
            printf "row %d serialization mode %q does not match selected %q\n", NR, $20, serialization_mode > "/dev/stderr"
            bad = 1
        }
        $21 != metadata_profile {
            printf "row %d metadata profile %q does not match selected %q\n", NR, $21, metadata_profile > "/dev/stderr"
            bad = 1
        }
        $22 != handshake_mode {
            printf "row %d handshake mode %q does not match selected %q\n", NR, $22, handshake_mode > "/dev/stderr"
            bad = 1
        }
        $25 != rust_quinn_ack_threshold {
            printf "row %d recorded Rust Quinn ACK threshold %s does not match selected %s\n", NR, $25, rust_quinn_ack_threshold > "/dev/stderr"
            bad = 1
        }
        $26 != rust_quinn_ack_delay_ms {
            printf "row %d recorded Rust Quinn ACK delay %s does not match selected %s\n", NR, $26, rust_quinn_ack_delay_ms > "/dev/stderr"
            bad = 1
        }
        $29 != c_frame_trace {
            printf "row %d recorded C frame trace state %s does not match selected %s\n", NR, $29, c_frame_trace > "/dev/stderr"
            bad = 1
        }
        $30 != rust_quinn_frame_trace {
            printf "row %d recorded Rust Quinn frame trace state %s does not match selected %s\n", NR, $30, rust_quinn_frame_trace > "/dev/stderr"
            bad = 1
        }
        $31 != rust_quinn_qlog {
            printf "row %d recorded Rust Quinn qlog state %s does not match selected %s\n", NR, $31, rust_quinn_qlog > "/dev/stderr"
            bad = 1
        }
        $32 != rust_quinn_protocol_trace {
            printf "row %d recorded Rust Quinn protocol trace state %s does not match selected %s\n", NR, $32, rust_quinn_protocol_trace > "/dev/stderr"
            bad = 1
        }
        $33 != rust_tls_keylog {
            printf "row %d recorded Rust TLS keylog state %s does not match selected %s\n", NR, $33, rust_tls_keylog > "/dev/stderr"
            bad = 1
        }
        END { exit bad ? 1 : 0 }
    ' "$CSV"
}

write_markdown_report() {
    local generated_at
    generated_at=$(date -u +'%Y-%m-%dT%H:%M:%SZ')

    {
        cat <<EOF
# Split RPC Benchmarks

Generated by \`bench/run_rpc_split.sh\`: $generated_at

## Settings

| Setting | Value |
| --- | --- |
| Output directory | \`$OUT_DIR\` |
| Split runs | \`$SPLIT_RUNS\` |
| Sample timeout | \`$SAMPLE_TIMEOUT_SECONDS\` s |
| C iterations | \`$C_ITERATIONS\` |
| C MsQuic profile | \`$C_MSQUIC_PROFILE\` |
| C MsQuic profile kind | \`$C_MSQUIC_PROFILE_KIND\` |
| C MsQuic stream receive window | \`$C_MSQUIC_STREAM_RECV_WINDOW\` bytes |
| C MsQuic connection flow-control window | \`$C_MSQUIC_CONN_FLOW_CONTROL_WINDOW\` bytes |
| C MsQuic send buffering | \`$C_MSQUIC_SEND_BUFFERING\` |
| C MsQuic execution profile | \`$C_MSQUIC_EXECUTION_PROFILE\` |
| C transport peer bidirectional streams | \`$C_MSQUIC_PEER_BIDI_STREAMS\` |
| C application streams per connection | \`$C_MSQUIC_APP_STREAM_CONCURRENCY\` |
| C WebTransport streams per session | \`$C_WEBTRANSPORT_STREAMS_PER_SESSION\` |
| C WebTransport sessions per connection | \`$C_WEBTRANSPORT_SESSIONS_PER_CONNECTION\` |
| C max frame size | \`$C_MSQUIC_MAX_FRAME_SIZE\` bytes |
| C max cumulative stream body | \`$C_MSQUIC_MAX_STREAM_BODY_SIZE\` bytes |
| C max pending sends | \`$C_MSQUIC_MAX_PENDING_SEND_BYTES\` bytes / \`$C_MSQUIC_MAX_PENDING_SEND_COUNT\` sends per stream |
| C connection idle timeout | \`$C_MSQUIC_IDLE_TIMEOUT_MS\` ms |
| C RPC stream idle timeout | \`$C_RPC_STREAM_IDLE_TIMEOUT\` |
| Go iterations | \`$GO_ITERATIONS\` |
| JS iterations | \`$JS_ITERATIONS\` |
| Rust iterations | \`$RUST_ITERATIONS\` |
| CMake build type | \`$CMAKE_BUILD_TYPE\` |
| Build prerequisites skipped | \`$SKIP_BUILD_PREREQUISITES\` |
| Client axis included | \`$RUN_CLIENT_AXIS\` |
| Server axis included | \`$RUN_SERVER_AXIS\` |
| Go gRPC included | \`$RUN_GO_GRPC\` |
| Rust gRPC included | \`$RUN_RUST_GRPC\` |
| Go gRPC TLS | \`enabled\` |
| Rust tonic TLS | \`enabled\` |
| Benchmark profile | \`$BENCHMARK_PROFILE\` |
| Rust Quinn ACK profile | \`$RUST_QUINN_ACK_PROFILE\` |
| Instrumentation profile | \`$INSTRUMENTATION_PROFILE\` |
| Payload profile | \`$PAYLOAD_PROFILE\` |
| Approx encoded request bytes | \`$ENCODED_REQUEST_BYTES\` |
| Approx encoded response bytes | \`$ENCODED_RESPONSE_BYTES\` |
| Serialization mode | \`$SERIALIZATION_MODE\` |
| Metadata profile | \`$METADATA_PROFILE\` |
| Handshake inclusion mode | \`$HANDSHAKE_INCLUSION_MODE\` |
| Batching settings | \`$SPLIT_BATCHING_SETTINGS\` |
| Rust Quinn idle timeout | \`${RUST_QUINN_MAX_IDLE_TIMEOUT_MS}\` ms |
| Rust Quinn keepalive | \`${RUST_QUINN_KEEP_ALIVE_MS}\` ms |
| Rust Quinn send window | \`${RUST_QUINN_SEND_WINDOW_BYTES}\` |
| Rust Quinn ACK threshold | \`${RUST_QUINN_ACK_THRESHOLD}\` |
| Rust Quinn ACK delay | \`${RUST_QUINN_ACK_DELAY_MS}\` |
| Split shape filter | \`${TREVRPC_RUST_SPLIT_BENCH_SHAPES:-all}\` |
| C frame trace | \`$C_FRAME_TRACE_STATE\` |
| Rust Quinn frame trace | \`$RUST_QUINN_FRAME_TRACE_STATE\` |
| Rust Quinn qlog | \`$RUST_QUINN_QLOG_STATE\` |
| Rust Quinn protocol trace | \`$RUST_QUINN_PROTOCOL_TRACE_STATE\` |
| Rust TLS key log | \`$RUST_TLS_KEYLOG_STATE\` |

## Security and payload model

| Field | Value |
| --- | --- |
| Transport security mode | trevRPC QUIC rows and gRPC baseline rows are encrypted |
| Certificate verification mode | gRPC TLS rows use the generated local certificate as a pinned trust root with hostname verification; trevRPC QUIC split rows use local self-signed certificates with certificate verification skipped in the benchmark clients |
| Payload profile | \`$PAYLOAD_PROFILE\` protobuf message with one string field; default request/reply text is \`TrevRPC benchmark\` |
| Serialization mode | \`$SERIALIZATION_MODE\`; rows serialize and deserialize protobuf messages per operation/message |
| Metadata profile | \`$METADATA_PROFILE\` |
| Handshake inclusion mode | \`$HANDSHAKE_INCLUSION_MODE\`; clients connect and warm before timed samples |
| Batching settings | \`$SPLIT_BATCHING_SETTINGS\` |

## Transport settings

| Row family | Stream limits | Idle timeout and keepalive | Flow control and buffering | ACK and TCP behavior | Notes |
| --- | --- | --- | --- | --- | --- |
| trevRPC C / MsQuic | client and shared server listener peer bidirectional streams \`$C_MSQUIC_PEER_BIDI_STREAMS\`; native application streams per connection \`$C_MSQUIC_APP_STREAM_CONCURRENCY\`; WebTransport streams per session \`$C_WEBTRANSPORT_STREAMS_PER_SESSION\`; sessions per connection \`$C_WEBTRANSPORT_SESSIONS_PER_CONNECTION\` | idle timeout \`${C_MSQUIC_IDLE_TIMEOUT_MS}ms\`; keepalive \`5000ms\`; per-stream RPC idle timeout \`$C_RPC_STREAM_IDLE_TIMEOUT\` | profile \`$C_MSQUIC_PROFILE\` (\`$C_MSQUIC_PROFILE_KIND\`); stream receive window \`$C_MSQUIC_STREAM_RECV_WINDOW\`; connection flow-control window \`$C_MSQUIC_CONN_FLOW_CONTROL_WINDOW\`; send buffering \`$C_MSQUIC_SEND_BUFFERING\`; execution \`$C_MSQUIC_EXECUTION_PROFILE\`; frame/body limits \`$C_MSQUIC_MAX_FRAME_SIZE/$C_MSQUIC_MAX_STREAM_BODY_SIZE\`; pending sends \`$C_MSQUIC_MAX_PENDING_SEND_BYTES/$C_MSQUIC_MAX_PENDING_SEND_COUNT\` | MsQuic ACK behavior is library default; not TCP | Native and WebTransport share one MsQuic listener and therefore the transport peer-stream limit; application admission remains independently capped at \`$C_MSQUIC_APP_STREAM_CONCURRENCY\`. Reference clients use benchmark skip-verify certificates. |
| trevRPC Go / quic-go | server default native stream concurrency \`64\`; split WebTransport server uses \`65535\`; clients disable peer-initiated streams | QUIC idle timeout \`10m\`; keepalive \`5s\`; RPC stream idle timeout disabled for split clients/servers | receive windows are capped from max frame size, stream body size, and concurrency; frame write batch \`16\` | quic-go ACK behavior is library default; not TCP | Raw QUIC rows are transport-compatible with trevRPC C/Rust native rows |
| trevRPC JavaScript / MsQuic | client max streams per session \`128\`; server streams per session \`65535\`; sessions per connection \`16\` | idle timeout \`600000ms\`; RPC stream idle timeout disabled in split path | native read batch \`32\`; write batch \`16\`; MsQuic send buffering follows C native stack defaults | MsQuic ACK behavior is library default; not TCP | Node addon still copies JS buffers into native-owned send buffers before returning |
| trevRPC Rust / Quinn | split native server max concurrent streams per connection \`128\`; max concurrent connections \`512\` | Quinn idle timeout \`${RUST_QUINN_MAX_IDLE_TIMEOUT_MS}ms\`; keepalive \`${RUST_QUINN_KEEP_ALIVE_MS}ms\`; RPC stream idle timeout disabled by default | stream/body limits are runtime-bounded; message frame batch \`32\`; send window \`${RUST_QUINN_SEND_WINDOW_BYTES}\` | ACK threshold \`${RUST_QUINN_ACK_THRESHOLD}\`; ACK delay \`${RUST_QUINN_ACK_DELAY_MS}\`; not TCP | Non-default ACK settings force the report profile to \`diagnostic-interoperability\` and must not be published as production-representative rows |
| gRPC Go / TCP+TLS | HTTP/2/gRPC library defaults | benchmark server/client use process lifetime connection reuse | HTTP/2/gRPC library defaults | TCP_NODELAY not explicitly overridden in the benchmark | Baseline only; not transport-compatible with trevRPC QUIC rows |
| gRPC Rust tonic / TCP+TLS | HTTP/2/tonic library defaults | benchmark server/client use process lifetime connection reuse | HTTP/2/tonic library defaults | TCP_NODELAY enabled on tonic server and client | Baseline only; not transport-compatible with trevRPC QUIC rows |

## Benchmark profile boundary

The sorted tables below are \`$HANDSHAKE_INCLUSION_MODE\` measurements. They exclude client construction, dial/connect, TLS/QUIC handshakes, first-RPC setup, and clean close. Any handshake-inclusive benchmark family must be emitted and published in separate tables.

The \`production-representative\` profile requires structured protobuf messages, per-message serialization/deserialization, encryption, no raw/pre-encoded payload shortcuts, and all diagnostic instrumentation disabled. Throughput-oriented batching is reported in the batching settings; raw, pre-encoded, captured, keylogged, or traced rows must stay out of these tables.

## Environment

| Item | Value |
| --- | --- |
| Kernel | \`$(uname -srmo 2>/dev/null || true)\` |
| CPU | \`$(awk -F: '/model name/ {gsub(/^[ \t]+/, "", $2); print $2; exit}' /proc/cpuinfo 2>/dev/null || true)\` |
| CPU governor | \`$(cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor 2>/dev/null || true)\` |
| Go | \`$(go version 2>/dev/null || true)\` |
| Node | \`$(node --version 2>/dev/null || true)\` |
| Rust | \`$(rustc --version 2>/dev/null || true)\` |
| Cargo | \`$(cargo --version 2>/dev/null || true)\` |
| CMake | \`$(cmake --version 2>/dev/null | head -n 1 || true)\` |

## Results
EOF

        awk -F, '
        function is_throughput_shape(value) {
            return value ~ /_throughput$/
        }
        function compare_metric(i1, v1, i2, v2) {
            if (is_throughput_shape(shape[v1])) {
                if (throughput[v1] > throughput[v2]) {
                    return -1
                }
                if (throughput[v1] < throughput[v2]) {
                    return 1
                }
                return row_order[v1] < row_order[v2] ? -1 : 1
            }
            if (latency[v1] < latency[v2]) {
                return -1
            }
            if (latency[v1] > latency[v2]) {
                return 1
            }
            return row_order[v1] < row_order[v2] ? -1 : 1
        }
        function append(values, value) {
            return values == "" ? value : values " " value
        }
        function framework(value) {
            return value == "grpc" ? "gRPC" : "trevRPC"
        }
        function label_language(value) {
            if (value == "grpc_go") {
                return "go"
            }
            split(value, parts, "_")
            return parts[1]
        }
        function label_implementation(value) {
            if (value == "grpc_go") {
                return "grpc"
            }
            if (value == "go_quic") {
                return "quic-go"
            }
            split(value, parts, "_")
            return substr(value, length(parts[1]) + 2)
        }
        function add_row_to_group(table_axis, id,    group_key) {
            group_key = table_axis SUBSEP shape[id]
            if (!(group_key in seen_group)) {
                seen_group[group_key] = 1
                groups[++group_count] = group_key
                group_axis[group_key] = table_axis
                group_shape[group_key] = shape[id]
            }
            group_rows[group_key] = append(group_rows[group_key], id)
        }
        function print_table_header(axis, shape) {
            printf "\n### `%s` / `%s`\n\n", axis, shape
            if (is_throughput_shape(shape)) {
                if (axis == "client") {
                    print "| Framework | Client language | Client implementation | Median throughput messages/s | Throughput min..max messages/s |"
                    print "| --- | --- | --- | ---: | ---: |"
                } else if (axis == "server") {
                    print "| Framework | Server language | Server implementation | Median throughput messages/s | Throughput min..max messages/s |"
                    print "| --- | --- | --- | ---: | ---: |"
                } else {
                    print "| Framework | Client language | Client implementation | Server language | Server implementation | Median throughput messages/s | Throughput min..max messages/s |"
                    print "| --- | --- | --- | --- | --- | ---: | ---: |"
                }
            } else {
                if (axis == "client") {
                    print "| Framework | Client language | Client implementation | Median latency us/op | Latency min..max us/op |"
                    print "| --- | --- | --- | ---: | ---: |"
                } else if (axis == "server") {
                    print "| Framework | Server language | Server implementation | Median latency us/op | Latency min..max us/op |"
                    print "| --- | --- | --- | ---: | ---: |"
                } else {
                    print "| Framework | Client language | Client implementation | Server language | Server implementation | Median latency us/op | Latency min..max us/op |"
                    print "| --- | --- | --- | --- | --- | ---: | ---: |"
                }
            }
        }
        NR > 1 {
            id = NR - 1
            row_order[id] = id
            axis[id] = $1
            client[id] = $2
            server[id] = $3
            shape[id] = $4
            latency[id] = $6 + 0
            latency_min[id] = $7
            latency_max[id] = $8
            throughput[id] = $9 + 0
            throughput_min[id] = $10 + 0
            throughput_max[id] = $11 + 0
            if (axis[id] == "grpc") {
                add_row_to_group("client", id)
                add_row_to_group("server", id)
            } else {
                add_row_to_group(axis[id], id)
            }
        }
        END {
            for (group_index = 1; group_index <= group_count; group_index++) {
                group_key = groups[group_index]
                row_count = split(group_rows[group_key], rows, " ")
                asort(rows, sorted_rows, "compare_metric")
                print_table_header(group_axis[group_key], group_shape[group_key])
                for (row_index = 1; row_index <= row_count; row_index++) {
                    id = sorted_rows[row_index]
                    if (is_throughput_shape(group_shape[group_key])) {
                        if (group_axis[group_key] == "client") {
                            printf "| `%s` | `%s` | `%s` | %.0f | %.0f..%.0f |\n", framework(axis[id]), label_language(client[id]), label_implementation(client[id]), throughput[id], throughput_min[id], throughput_max[id]
                        } else if (group_axis[group_key] == "server") {
                            printf "| `%s` | `%s` | `%s` | %.0f | %.0f..%.0f |\n", framework(axis[id]), label_language(server[id]), label_implementation(server[id]), throughput[id], throughput_min[id], throughput_max[id]
                        } else {
                            printf "| `%s` | `%s` | `%s` | `%s` | `%s` | %.0f | %.0f..%.0f |\n", framework(axis[id]), label_language(client[id]), label_implementation(client[id]), label_language(server[id]), label_implementation(server[id]), throughput[id], throughput_min[id], throughput_max[id]
                        }
                    } else {
                        if (group_axis[group_key] == "client") {
                            printf "| `%s` | `%s` | `%s` | %.3f | %.3f..%.3f |\n", framework(axis[id]), label_language(client[id]), label_implementation(client[id]), latency[id], latency_min[id], latency_max[id]
                        } else if (group_axis[group_key] == "server") {
                            printf "| `%s` | `%s` | `%s` | %.3f | %.3f..%.3f |\n", framework(axis[id]), label_language(server[id]), label_implementation(server[id]), latency[id], latency_min[id], latency_max[id]
                        } else {
                            printf "| `%s` | `%s` | `%s` | `%s` | `%s` | %.3f | %.3f..%.3f |\n", framework(axis[id]), label_language(client[id]), label_implementation(client[id]), label_language(server[id]), label_implementation(server[id]), latency[id], latency_min[id], latency_max[id]
                        }
                    }
                }
            }
        }' "$CSV"

        local failure_count
        failure_count=$(awk 'END { print (NR > 0 ? NR - 1 : 0) }' "$FAILURES_CSV")
        if ((failure_count > 0)); then
            cat <<EOF

## Failed Samples

Failed or timed-out samples are omitted from the aggregate result tables above.

| Axis | Framework | Run | Client language | Client implementation | Server language | Server implementation | Status | Raw output |
| --- | --- | ---: | --- | --- | --- | --- | ---: | --- |
EOF

            awk -F, -v root_prefix="$ROOT/" '
            function framework(value) {
                return value == "grpc" ? "gRPC" : "trevRPC"
            }
            function label_language(value) {
                if (value == "grpc_go") {
                    return "go"
                }
                split(value, parts, "_")
                return parts[1]
            }
            function label_implementation(value) {
                if (value == "grpc_go") {
                    return "grpc"
                }
                if (value == "go_quic") {
                    return "quic-go"
                }
                split(value, parts, "_")
                return substr(value, length(parts[1]) + 2)
            }
            NR > 1 {
                raw_file = $7
                if (index(raw_file, root_prefix) == 1) {
                    raw_file = substr(raw_file, length(root_prefix) + 1)
                }
                printf "| `%s` | `%s` | %s | `%s` | `%s` | `%s` | `%s` | %s | `%s` |\n", $1, framework($1), $2, label_language($3), label_implementation($3), label_language($4), label_implementation($4), $6, raw_file
            }' "$FAILURES_CSV"
        fi

        cat <<EOF

## Notes

The client axis benchmarks each TrevRPC client transport against a reference C MsQuic server.

The server axis benchmarks each TrevRPC server transport using a reference C client for the matching wire protocol.

Latency rows measure one RPC operation. Stream latency rows use one request and one response message. Stream throughput rows measure messages per second over one open stream.

Latency tables are sorted by median latency ascending. Throughput tables are sorted by median message throughput descending.

Rows with framework \`gRPC\` benchmark gRPC clients and servers split into separate processes over TCP with TLS when the corresponding TLS setting is enabled. They are included in both client and server tables as baselines only and are not transport-compatible with the TrevRPC transport rows.

Raw command output is saved under \`$RAW_DIR\`. Per-measurement normalized rows are saved in \`$SAMPLES_CSV\`. The exact commands are saved in \`$COMMAND_LOG\`.

Rust Quinn ACK settings, the C MsQuic profile, and normalized instrumentation states are stored on every success and failure row. \`--report-only\` requires the selected transport, instrumentation, and batching/profile settings to match that immutable row metadata and refuses to relabel mismatched samples. Schemas predating these fields are rejected; regenerate them with their recorded script snapshot or explicitly migrate every row instead of inferring missing state.
EOF
    } >"$MARKDOWN"
}

wait_for_port() {
    local stdout_file=$1
    local pid=$2
    local deadline=$((SECONDS + SERVER_STARTUP_TIMEOUT_SECONDS))
    local port=""
    while ((SECONDS < deadline)); do
        if ! kill -0 "$pid" 2>/dev/null; then
            printf 'server process %s exited before reporting a port\n' "$pid" >&2
            return 1
        fi
        if [[ -f "$stdout_file" ]]; then
            port=$(awk '/^PORT / {print $2; exit}' "$stdout_file")
            if [[ -n "$port" ]]; then
                printf '%s\n' "$port"
                return 0
            fi
        fi
        sleep 0.1
    done
    printf 'timed out waiting for server port in %s\n' "$stdout_file" >&2
    return 1
}

start_server() {
    local name=$1
    shift
    local stdout_file="$RAW_DIR/$name.stdout.txt"
    local stderr_file="$RAW_DIR/$name.stderr.txt"
    : >"$stdout_file"
    : >"$stderr_file"
    log_command "$@"
    "$@" >"$stdout_file" 2>"$stderr_file" &
    local pid=$!
    SERVER_PIDS+=("$pid")
    START_SERVER_PORT=$(wait_for_port "$stdout_file" "$pid")
}

stop_servers() {
    local pid
    for pid in "${SERVER_PIDS[@]:-}"; do
        if kill -0 "$pid" 2>/dev/null; then
            kill "$pid" 2>/dev/null || true
            wait "$pid" 2>/dev/null || true
        fi
    done
    SERVER_PIDS=()
}

trap stop_servers EXIT

run_split_sample() {
    local name=$1
    local run=$2
    local axis=$3
    local client=$4
    local server=$5
    local source=$6
    shift 6
    if run_and_capture "$name" timeout --kill-after=5s "${SAMPLE_TIMEOUT_SECONDS}s" "$@"; then
        append_split_csv "$RAW_DIR/$name.txt" "$run" "$axis" "$client" "$server" "$source"
    else
        local status=$?
        printf 'sample %s failed with status %d; omitting from aggregate results\n' "$name" "$status" | tee -a "$RAW_DIR/$name.txt" >&2
        printf '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' "$axis" "$run" "$client" "$server" "$source" "$status" "$RAW_DIR/$name.txt" "$RUST_QUINN_ACK_THRESHOLD" "$RUST_QUINN_ACK_DELAY_MS" "$C_MSQUIC_PROFILE" "$C_MSQUIC_PROFILE_KIND" "$SPLIT_BATCHING_SETTINGS" "$C_FRAME_TRACE_STATE" "$RUST_QUINN_FRAME_TRACE_STATE" "$RUST_QUINN_QLOG_STATE" "$RUST_QUINN_PROTOCOL_TRACE_STATE" "$RUST_TLS_KEYLOG_STATE" >>"$FAILURES_CSV"
    fi
}

run_c_split_client_sample() {
    local name=$1
    local run=$2
    local axis=$3
    local client=$4
    local server=$5
    local source=$6
    local c_bench=$7
    local port=$8
    local iterations=$9
    local shapes=${TREVRPC_RUST_SPLIT_BENCH_SHAPES:-}

    if [[ -z "${shapes//[[:space:],]/}" ]]; then
        run_split_sample "$name" "$run" "$axis" "$client" "$server" "$source" env "${BENCH_ENV[@]}" "$c_bench" --split-client msquic 127.0.0.1 "$port" "$iterations"
        return
    fi

    local shape
    local shape_list=()
    IFS=',' read -r -a shape_list <<<"$shapes"
    for shape in "${shape_list[@]}"; do
        shape=${shape//[[:space:]]/}
        if [[ -z "$shape" ]]; then
            continue
        fi
        run_split_sample "$name-$shape" "$run" "$axis" "$client" "$server" "$source" env "${BENCH_ENV[@]}" "$c_bench" --split-client msquic 127.0.0.1 "$port" "$iterations" "$shape"
    done
}

build_prerequisites() {
    rm -f "$CMAKE_BUILD_DIR/msquic-test-cert.pem" "$CMAKE_BUILD_DIR/msquic-test-key.pem"
    run_and_capture c-configure cmake -S trevrpc-c -B "$CMAKE_BUILD_DIR" -DTREVRPC_BUILD_TESTS=OFF -DTREVRPC_BUILD_BENCHMARKS=ON -DCMAKE_BUILD_TYPE="$CMAKE_BUILD_TYPE"
    run_and_capture c-build cmake --build "$CMAKE_BUILD_DIR" --target trevrpc_rpc_comparison_bench

    if [[ "$RUN_GO_QUIC" == "1" || "$RUN_GO_GRPC" == "1" ]]; then
        run_and_capture go-split-build go build -C trevrpc-go -o "$GO_SPLIT_BENCH" ./cmd/trevrpc-rpc-split-bench
    fi
    if [[ "$RUN_JS_NATIVE" == "1" ]]; then
        run_and_capture js-native-build env "CMAKE_BUILD_TYPE=$CMAKE_BUILD_TYPE" npm --prefix trevrpc-js run build:native
    fi
    if [[ "$RUN_RUST_QUINN" == "1" || "$RUN_RUST_GRPC" == "1" ]]; then
        run_and_capture rust-split-build cargo build --manifest-path trevrpc-rust/Cargo.toml --example rpc_split_bench --release
    fi
}

require_prebuilt_artifact() {
    local description=$1
    local path=$2
    local kind=${3:-file}
    if [[ "$kind" == "executable" && ! -x "$path" ]]; then
        printf 'SKIP_BUILD_PREREQUISITES=1 requires prebuilt %s at %s\n' "$description" "$path" >&2
        exit 2
    fi
    if [[ "$kind" == "file" && ! -f "$path" ]]; then
        printf 'SKIP_BUILD_PREREQUISITES=1 requires prebuilt %s at %s\n' "$description" "$path" >&2
        exit 2
    fi
}

validate_prebuilt_artifacts() {
    local native_runtime_enabled=0
    local needs_certificates=0
    local needs_go=0
    local needs_js=0
    local needs_rust=0
    if [[ "$RUN_C_MSQUIC" == "1" || "$RUN_GO_QUIC" == "1" || "$RUN_JS_NATIVE" == "1" || "$RUN_RUST_QUINN" == "1" ]]; then
        native_runtime_enabled=1
    fi
    if [[ "$RUN_CLIENT_AXIS" == "1" && "$native_runtime_enabled" == "0" ]]; then
        printf 'RUN_CLIENT_AXIS=1 requires at least one native runtime to be enabled\n' >&2
        exit 2
    fi
    if [[ "$RUN_SERVER_AXIS" == "1" && "$native_runtime_enabled" == "0" ]]; then
        printf 'RUN_SERVER_AXIS=1 requires at least one native runtime to be enabled\n' >&2
        exit 2
    fi

    if [[ "$RUN_CLIENT_AXIS" == "1" || "$RUN_SERVER_AXIS" == "1" ]]; then
        require_prebuilt_artifact 'C reference benchmark' "$CMAKE_BUILD_DIR/trevrpc_rpc_comparison_bench" executable
    fi
    if [[ "$RUN_CLIENT_AXIS" == "1" || "$RUN_GO_GRPC" == "1" || "$RUN_RUST_GRPC" == "1" ]]; then
        needs_certificates=1
    elif [[ "$RUN_SERVER_AXIS" == "1" ]]; then
        if [[ "$RUN_C_MSQUIC" == "1" || "$RUN_GO_QUIC" == "1" || "$RUN_JS_NATIVE" == "1" ]]; then
            needs_certificates=1
        fi
    fi
    if [[ "$needs_certificates" == "1" ]]; then
        require_prebuilt_artifact 'TLS certificate' "$CMAKE_BUILD_DIR/msquic-test-cert.pem"
        require_prebuilt_artifact 'TLS private key' "$CMAKE_BUILD_DIR/msquic-test-key.pem"
    fi
    if [[ "$RUN_GO_GRPC" == "1" ]]; then
        needs_go=1
    elif [[ "$RUN_GO_QUIC" == "1" ]]; then
        if [[ "$RUN_CLIENT_AXIS" == "1" || "$RUN_SERVER_AXIS" == "1" ]]; then
            needs_go=1
        fi
    fi
    if [[ "$needs_go" == "1" ]]; then
        require_prebuilt_artifact 'Go split benchmark' "$GO_SPLIT_BENCH" executable
    fi
    if [[ "$RUN_JS_NATIVE" == "1" ]]; then
        if [[ "$RUN_CLIENT_AXIS" == "1" || "$RUN_SERVER_AXIS" == "1" ]]; then
            needs_js=1
        fi
    fi
    if [[ "$needs_js" == "1" ]]; then
        require_prebuilt_artifact 'JavaScript native addon' "$ROOT/trevrpc-js/build/native/trevrpc_native.node"
    fi
    if [[ "$RUN_RUST_GRPC" == "1" ]]; then
        needs_rust=1
    elif [[ "$RUN_RUST_QUINN" == "1" ]]; then
        if [[ "$RUN_CLIENT_AXIS" == "1" || "$RUN_SERVER_AXIS" == "1" ]]; then
            needs_rust=1
        fi
    fi
    if [[ "$needs_rust" == "1" ]]; then
        require_prebuilt_artifact 'Rust split benchmark' "$RUST_SPLIT_BENCH" executable
    fi
}

run_client_axis() {
    local c_bench="$CMAKE_BUILD_DIR/trevrpc_rpc_comparison_bench"
    local cert_file="$CMAKE_BUILD_DIR/msquic-test-cert.pem"
    local port
    start_server client-axis-c-server env "${BENCH_ENV[@]}" "$c_bench" --split-serve
    port=$START_SERVER_PORT

    for ((run = 1; run <= SPLIT_RUNS; run++)); do
        if [[ "$RUN_C_MSQUIC" == "1" ]]; then
            run_split_sample "client-c-msquic-run-$run" "$run" client c_msquic c_msquic c-custom env "${BENCH_ENV[@]}" "$c_bench" --split-client msquic 127.0.0.1 "$port" "$C_ITERATIONS"
        fi
        if [[ "$RUN_GO_QUIC" == "1" ]]; then
            run_split_sample "client-go-quic-run-$run" "$run" client go_quic c_msquic go-custom env "${BENCH_ENV[@]}" "$GO_SPLIT_BENCH" -mode client -transport quic -addr "127.0.0.1:$port" -cert "$cert_file" -iterations "$GO_ITERATIONS"
        fi
        if [[ "$RUN_JS_NATIVE" == "1" ]]; then
            run_split_sample "client-js-native-run-$run" "$run" client js_msquic c_msquic js-custom env "${BENCH_ENV[@]}" node trevrpc-js/bench/rpc_split_native.js client 127.0.0.1 "$port" "$JS_ITERATIONS"
        fi
        if [[ "$RUN_RUST_QUINN" == "1" ]]; then
            run_split_sample "client-rust-quinn-run-$run" "$run" client rust_quinn c_msquic rust-custom env "${RUST_BENCH_ENV_UNSET[@]}" "${RUST_BENCH_ENV[@]}" "$RUST_SPLIT_BENCH" client "127.0.0.1:$port" "$cert_file" "$RUST_ITERATIONS"
        fi
    done

    stop_servers
}

run_server_axis() {
    local c_bench="$CMAKE_BUILD_DIR/trevrpc_rpc_comparison_bench"
    local cert_file="$CMAKE_BUILD_DIR/msquic-test-cert.pem"
    local key_file="$CMAKE_BUILD_DIR/msquic-test-key.pem"
    local port
    local run

    if [[ "$RUN_C_MSQUIC" == "1" ]]; then
        start_server server-axis-c-server env "${BENCH_ENV[@]}" "$c_bench" --split-serve
        port=$START_SERVER_PORT
        for ((run = 1; run <= SPLIT_RUNS; run++)); do
            run_c_split_client_sample "server-c-msquic-run-$run" "$run" server c_msquic c_msquic c-custom "$c_bench" "$port" "$C_ITERATIONS"
        done
        stop_servers
    fi

    if [[ "$RUN_GO_QUIC" == "1" ]]; then
        start_server server-axis-go-quic env "${BENCH_ENV[@]}" "$GO_SPLIT_BENCH" -mode server -transport quic -addr 127.0.0.1:0 -cert "$cert_file" -key "$key_file"
        port=$START_SERVER_PORT
        for ((run = 1; run <= SPLIT_RUNS; run++)); do
            run_c_split_client_sample "server-go-quic-run-$run" "$run" server c_msquic go_quic c-custom "$c_bench" "$port" "$C_ITERATIONS"
        done
        stop_servers
    fi

    if [[ "$RUN_RUST_QUINN" == "1" ]]; then
        start_server server-axis-rust-quinn env "${RUST_BENCH_ENV_UNSET[@]}" "${RUST_BENCH_ENV[@]}" "$RUST_SPLIT_BENCH" server 127.0.0.1:0
        port=$START_SERVER_PORT
        for ((run = 1; run <= SPLIT_RUNS; run++)); do
            run_c_split_client_sample "server-rust-quinn-run-$run" "$run" server c_msquic rust_quinn c-custom "$c_bench" "$port" "$C_ITERATIONS"
        done
        stop_servers
    fi

    if [[ "$RUN_JS_NATIVE" == "1" ]]; then
        start_server server-axis-js-native env "${BENCH_ENV[@]}" node trevrpc-js/bench/rpc_split_native.js server "$cert_file" "$key_file"
        port=$START_SERVER_PORT
        for ((run = 1; run <= SPLIT_RUNS; run++)); do
            run_c_split_client_sample "server-js-native-run-$run" "$run" server c_msquic js_msquic c-custom "$c_bench" "$port" "$C_ITERATIONS"
        done
        stop_servers
    fi
}

run_grpc_axis() {
    local run
    local cert_file="$CMAKE_BUILD_DIR/msquic-test-cert.pem"
    local key_file="$CMAKE_BUILD_DIR/msquic-test-key.pem"

    if [[ "$RUN_GO_GRPC" == "1" ]]; then
        start_server grpc-go-server env "${BENCH_ENV[@]}" "$GO_SPLIT_BENCH" -mode server -transport grpc -addr 127.0.0.1:0 -cert "$cert_file" -key "$key_file"
        local port=$START_SERVER_PORT
        for ((run = 1; run <= SPLIT_RUNS; run++)); do
            run_split_sample "grpc-go-run-$run" "$run" grpc grpc_go grpc_go go-custom env "${BENCH_ENV[@]}" "$GO_SPLIT_BENCH" -mode client -transport grpc -cert "$cert_file" -addr "127.0.0.1:$port" -iterations "$GO_ITERATIONS"
        done
        stop_servers
    fi

    if [[ "$RUN_RUST_GRPC" == "1" ]]; then
        start_server grpc-rust-tonic-server env "${BENCH_ENV[@]}" "$RUST_SPLIT_BENCH" grpc-server 127.0.0.1:0 "$cert_file" "$key_file"
        local port=$START_SERVER_PORT
        for ((run = 1; run <= SPLIT_RUNS; run++)); do
            run_split_sample "grpc-rust-tonic-run-$run" "$run" grpc rust_tonic rust_tonic rust-custom env "${BENCH_ENV[@]}" "$RUST_SPLIT_BENCH" grpc-client "127.0.0.1:$port" "$cert_file" "$RUST_ITERATIONS"
        done
        stop_servers
    fi
}

cd "$ROOT"

if [[ "$REPORT_ONLY" == "0" ]]; then
    initialize_output
    if [[ "$SKIP_BUILD_PREREQUISITES" == "1" ]]; then
        validate_prebuilt_artifacts
    else
        build_prerequisites
    fi
    if [[ "$RUN_CLIENT_AXIS" == "1" ]]; then
        run_client_axis
    fi
    if [[ "$RUN_SERVER_AXIS" == "1" ]]; then
        run_server_axis
    fi
    if [[ "$RUN_GO_GRPC" == "1" || "$RUN_RUST_GRPC" == "1" ]]; then
        run_grpc_axis
    fi
fi

assert_sample_profile_metadata
aggregate_samples_csv
assert_no_plaintext_rows
assert_benchmark_labels
write_markdown_report

printf '\nWrote split CSV: %s\n' "$CSV"
printf 'Wrote split per-measurement CSV: %s\n' "$SAMPLES_CSV"
printf 'Wrote split Markdown report: %s\n' "$MARKDOWN"
printf 'Wrote split raw outputs: %s\n' "$RAW_DIR"
