#!/usr/bin/env bash

set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
OUT_DIR_WAS_SET=${OUT_DIR+x}
CMAKE_BUILD_DIR_WAS_SET=${CMAKE_BUILD_DIR+x}
OUT_DIR=${OUT_DIR:-target/webtransport}
case "$OUT_DIR" in
/*) OUT_DIR_ABS="$OUT_DIR" ;;
*) OUT_DIR_ABS="$ROOT/$OUT_DIR" ;;
esac
RAW_DIR="$OUT_DIR/raw"
COMMAND_LOG="$OUT_DIR/commands.txt"
CSV="$OUT_DIR/webtransport.csv"
SAMPLES_CSV="$OUT_DIR/webtransport-samples.csv"
FAILURES_CSV="$OUT_DIR/webtransport-failures.csv"
MARKDOWN="$OUT_DIR/webtransport.md"

WEBTRANSPORT_ITERATIONS=${WEBTRANSPORT_ITERATIONS:-10000}
WEBTRANSPORT_RUNS=${WEBTRANSPORT_RUNS:-3}
SAMPLE_TIMEOUT_SECONDS=${SAMPLE_TIMEOUT_SECONDS:-300}
CMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE:-Release}
CMAKE_BUILD_DIR=${CMAKE_BUILD_DIR:-"$OUT_DIR/c-build"}
SERVER_STARTUP_TIMEOUT_SECONDS=${SERVER_STARTUP_TIMEOUT_SECONDS:-15}

RUN_C=${RUN_C:-1}
RUN_GO=${RUN_GO:-1}
RUN_GO_CONNECT=${RUN_GO_CONNECT:-1}
RUN_RUST=${RUN_RUST:-1}
RUN_JS=${RUN_JS:-${RUN_JS_NATIVE:-1}}

PAYLOAD_PROFILE=${PAYLOAD_PROFILE:-tiny}
METADATA_PROFILE=${METADATA_PROFILE:-none}
HANDSHAKE_INCLUSION_MODE=${HANDSHAKE_INCLUSION_MODE:-steady-state-warmed}
WEBTRANSPORT_BATCHING_SETTINGS=${WEBTRANSPORT_BATCHING_SETTINGS:-send-many-batch=${WEBTRANSPORT_SEND_MANY_BATCH:-1};stream-read-batch=${WEBTRANSPORT_STREAM_READ_BATCH:-default};stream-write-batch=${WEBTRANSPORT_STREAM_WRITE_BATCH:-default};stream-write-batch-bytes=${WEBTRANSPORT_STREAM_WRITE_BATCH_BYTES:-default};concurrent-streams=${WEBTRANSPORT_CONCURRENT_STREAMS:-1}}
SERIALIZATION_MODE=per-message-serialized

GO_SPLIT_BENCH="$OUT_DIR_ABS/trevrpc-go-rpc-split-bench"
RUST_SPLIT_BENCH="$ROOT/trevrpc-rust/target/release/examples/rpc_split_bench"

usage() {
    cat <<'EOF'
Usage: bench/run_webtransport.sh
       bench/run_webtransport.sh --report-only
       bench/run_webtransport.sh --smoke

Runs browser WebTransport benchmarks with a Chromium client driven by Playwright,
then writes normalized CSV/Markdown reports.

Environment knobs:
  OUT_DIR                    Output directory. Default: target/webtransport
  WEBTRANSPORT_ITERATIONS    Fixed iteration count per browser sample. Default: 10000
  WEBTRANSPORT_THROUGHPUT_MESSAGES
                             Message count for browser throughput samples. Default: WEBTRANSPORT_ITERATIONS
  WEBTRANSPORT_RUNS          Measurement command repetitions. Default: 3
  WEBTRANSPORT_DISABLE_STREAM_TIMEOUTS
                             Set to 1 to disable browser stream/deadline timers in TrevRPC calls.
  WEBTRANSPORT_CONGESTION_CONTROL
                             Browser WebTransport congestionControl option: default, low-latency, or throughput.
  PAYLOAD_PROFILE            Payload profile: tiny, small, medium, large, or mixed. Default: tiny
  WEBTRANSPORT_SEND_MANY_BATCH
                             sendMany batch size for browser client-stream and bidi throughput.
  WEBTRANSPORT_STREAM_READ_BATCH
                             Browser transport response frame batch size override.
  WEBTRANSPORT_STREAM_WRITE_BATCH
                             Browser transport request frame batch size override.
  WEBTRANSPORT_STREAM_WRITE_BATCH_BYTES
                             Browser transport request frame byte batch threshold override.
  WEBTRANSPORT_CONCURRENT_STREAMS
                             Concurrent browser streams for additional aggregate throughput rows. Default: 1
  SAMPLE_TIMEOUT_SECONDS     Per-sample timeout. Default: 300
  CMAKE_BUILD_TYPE           CMake build type for C benchmarks/certificates. Default: Release
  RUN_C                      Include trevrpc-c WebTransport server. Default: 1
  RUN_GO                     Include Go WebTransport server. Default: 1
  RUN_GO_CONNECT             Include Go ConnectRPC Fetch baseline. Default: 1
  RUN_RUST                   Include Rust WebTransport server. Default: 1
  RUN_JS                     Include JS WebTransport server. Default: 1
  METADATA_PROFILE           Metadata profile: none or production. Default: none
  TREVRPC_BROWSER_CHROMIUM   Optional Chromium executable path for Playwright.

Examples:
  bench/run_webtransport.sh
  bench/run_webtransport.sh --smoke
  WEBTRANSPORT_ITERATIONS=1000 WEBTRANSPORT_RUNS=1 bench/run_webtransport.sh
  RUN_RUST=0 bench/run_webtransport.sh
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
    WEBTRANSPORT_ITERATIONS=10
    WEBTRANSPORT_RUNS=1
    if [[ -z "$OUT_DIR_WAS_SET" ]]; then
        OUT_DIR=target/webtransport-smoke
    fi
fi

case "$OUT_DIR" in
/*) OUT_DIR_ABS="$OUT_DIR" ;;
*) OUT_DIR_ABS="$ROOT/$OUT_DIR" ;;
esac
RAW_DIR="$OUT_DIR/raw"
COMMAND_LOG="$OUT_DIR/commands.txt"
CSV="$OUT_DIR/webtransport.csv"
SAMPLES_CSV="$OUT_DIR/webtransport-samples.csv"
FAILURES_CSV="$OUT_DIR/webtransport-failures.csv"
MARKDOWN="$OUT_DIR/webtransport.md"
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

ENCODED_REQUEST_BYTES=${ENCODED_REQUEST_BYTES:-$(profile_encoded_bytes "$PAYLOAD_PROFILE")}
ENCODED_RESPONSE_BYTES=${ENCODED_RESPONSE_BYTES:-$(profile_encoded_bytes "$PAYLOAD_PROFILE")}
BENCH_ENV=("TREVRPC_BENCH_PAYLOAD_PROFILE=$PAYLOAD_PROFILE" "TREVRPC_BENCH_METADATA_PROFILE=$METADATA_PROFILE")

initialize_output() {
    mkdir -p "$RAW_DIR"
    : >"$COMMAND_LOG"
    rm -f "$RAW_DIR"/*.txt
    printf 'run,browser,server,shape,latency_us,throughput_per_s,iterations,elapsed_s,source,transport_security_mode,certificate_verification_mode,payload_profile,encoded_request_bytes,encoded_response_bytes,serialization_mode,metadata_profile,handshake_inclusion_mode,batching_settings,labels\n' >"$SAMPLES_CSV"
    printf 'run,browser,server,source,status,raw_file\n' >"$FAILURES_CSV"
}

RPC_SERVER_PIDS=()
STATIC_SERVER_PID=""
START_SERVER_PORT=""
STATIC_PORT=""
STATIC_ORIGIN=""
STATIC_URL=""

require_positive_integer() {
    local name=$1
    local value=$2
    if [[ ! "$value" =~ ^[1-9][0-9]*$ ]]; then
        printf '%s must be a positive integer, got %q\n' "$name" "$value" >&2
        exit 2
    fi
}

require_positive_integer WEBTRANSPORT_ITERATIONS "$WEBTRANSPORT_ITERATIONS"
require_positive_integer WEBTRANSPORT_RUNS "$WEBTRANSPORT_RUNS"
require_positive_integer SAMPLE_TIMEOUT_SECONDS "$SAMPLE_TIMEOUT_SECONDS"

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

append_webtransport_csv() {
    local raw_file=$1
    local run=$2
    local browser=$3
    local server=$4
    local source=$5
    awk -v run="$run" -v browser="$browser" -v server="$server" -v source="$source" \
        -v payload_profile="$PAYLOAD_PROFILE" \
        -v encoded_request_bytes="$ENCODED_REQUEST_BYTES" \
        -v encoded_response_bytes="$ENCODED_RESPONSE_BYTES" \
        -v metadata_profile="$METADATA_PROFILE" \
        -v handshake_inclusion_mode="$HANDSHAKE_INCLUSION_MODE" \
        -v batching_settings="$WEBTRANSPORT_BATCHING_SETTINGS" -F '' '
        function transport_security_mode() {
            return "encrypted"
        }
        function certificate_verification_mode() {
            if (server == "go_connect") {
                return "tls-skip-verify"
            }
            return "tls-pinned-server-certificate-hash"
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
            printf "%s,%s,%s,%s,%.3f,%.3f,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n", \
                run, browser, server, shape, latency_us, throughput, iterations, elapsed, source, \
                transport_security_mode(), certificate_verification_mode(), payload_profile, encoded_request_bytes, encoded_response_bytes, \
                serialization, metadata_profile, handshake_inclusion_mode, batching_settings, labels(shape)
        }
        function emit_unsupported(shape) {
            serialization = serialization_mode(shape)
            printf "%s,%s,%s,%s,N/A,N/A,N/A,N/A,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n", \
                run, browser, server, shape, source, transport_security_mode(), certificate_verification_mode(), payload_profile, \
                encoded_request_bytes, encoded_response_bytes, serialization, metadata_profile, handshake_inclusion_mode, batching_settings, labels(shape)
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
        match($0, /^([^:]+):[[:space:]]+N\/A/, m) {
            emit_unsupported(m[1])
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
            print "browser,server,shape,measurements,latency_us_median,latency_us_min,latency_us_max,throughput_per_s_median,throughput_per_s_min,throughput_per_s_max,iterations_per_measurement,elapsed_s_total,source,transport_security_mode,certificate_verification_mode,payload_profile,encoded_request_bytes,encoded_response_bytes,serialization_mode,metadata_profile,handshake_inclusion_mode,batching_settings,labels"
        }
        NR == 1 {
            next
        }
        {
            key = $2 SUBSEP $3 SUBSEP $4
            if (!(key in seen)) {
                seen[key] = 1
                order[++order_count] = key
                browser[key] = $2
                server[key] = $3
                shape[key] = $4
                source[key] = $9
                transport_security_mode[key] = $10
                certificate_verification_mode[key] = $11
                payload_profile[key] = $12
                encoded_request_bytes[key] = $13
                encoded_response_bytes[key] = $14
                serialization_mode[key] = $15
                metadata_profile[key] = $16
                handshake_inclusion_mode[key] = $17
                batching_settings[key] = $18
                labels[key] = $19
            }
            measurements[key]++
            if ($5 == "N/A" || $6 == "N/A") {
                unsupported[key] = 1
                next
            }
            latencies[key] = append(latencies[key], $5)
            throughputs[key] = append(throughputs[key], $6)
            iterations[key] = append(iterations[key], $7)
            elapsed_total[key] += $8
            if (!(key in latency_min) || $5 + 0 < latency_min[key]) {
                latency_min[key] = $5 + 0
            }
            if (!(key in latency_max) || $5 + 0 > latency_max[key]) {
                latency_max[key] = $5 + 0
            }
            if (!(key in throughput_min) || $6 + 0 < throughput_min[key]) {
                throughput_min[key] = $6 + 0
            }
            if (!(key in throughput_max) || $6 + 0 > throughput_max[key]) {
                throughput_max[key] = $6 + 0
            }
        }
        END {
            for (i = 1; i <= order_count; i++) {
                key = order[i]
                if (unsupported[key] && latencies[key] == "") {
                    printf "%s,%s,%s,%d,N/A,N/A,N/A,N/A,N/A,N/A,N/A,N/A,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n", \
                        browser[key], server[key], shape[key], measurements[key], source[key], \
                        transport_security_mode[key], certificate_verification_mode[key], payload_profile[key], encoded_request_bytes[key], encoded_response_bytes[key], serialization_mode[key], metadata_profile[key], handshake_inclusion_mode[key], batching_settings[key], labels[key]
                    continue
                }
                latency = median(latencies[key])
                throughput = median(throughputs[key])
                printf "%s,%s,%s,%d,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.0f,%.3f,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n", \
                    browser[key], server[key], shape[key], measurements[key], latency, latency_min[key], latency_max[key], throughput, throughput_min[key], throughput_max[key], median(iterations[key]), elapsed_total[key], source[key], \
                    transport_security_mode[key], certificate_verification_mode[key], payload_profile[key], encoded_request_bytes[key], encoded_response_bytes[key], serialization_mode[key], metadata_profile[key], handshake_inclusion_mode[key], batching_settings[key], labels[key]
            }
        }
    ' "$SAMPLES_CSV" >"$CSV"
}

assert_no_plaintext_rows() {
    if awk -F, 'NR > 1 && $14 == "plaintext" { found = 1; exit } END { exit found ? 0 : 1 }' "$CSV"; then
        printf 'WebTransport comparison emitted plaintext rows in %s\n' "$CSV" >&2
        exit 2
    fi
}

assert_benchmark_labels() {
    awk -F, -v payload_profile="$PAYLOAD_PROFILE" -v metadata_profile="$METADATA_PROFILE" \
        -v serialization_mode="$SERIALIZATION_MODE" -v handshake_mode="$HANDSHAKE_INCLUSION_MODE" '
        NR == 1 { next }
        $14 != "encrypted" {
            printf "row %d has false security label %q\n", NR, $14 > "/dev/stderr"
            bad = 1
        }
        $15 != "tls-pinned-server-certificate-hash" && $15 != "tls-skip-verify" {
            printf "row %d has unexpected certificate verification label %q\n", NR, $15 > "/dev/stderr"
            bad = 1
        }
        $16 != payload_profile {
            printf "row %d payload profile %q does not match selected %q\n", NR, $16, payload_profile > "/dev/stderr"
            bad = 1
        }
        $19 != serialization_mode {
            printf "row %d serialization mode %q does not match selected %q\n", NR, $19, serialization_mode > "/dev/stderr"
            bad = 1
        }
        $20 != metadata_profile {
            printf "row %d metadata profile %q does not match selected %q\n", NR, $20, metadata_profile > "/dev/stderr"
            bad = 1
        }
        $21 != handshake_mode {
            printf "row %d handshake mode %q does not match selected %q\n", NR, $21, handshake_mode > "/dev/stderr"
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
# WebTransport Benchmarks

Generated by \`bench/run_webtransport.sh\`: $generated_at

## Settings

| Setting | Value |
| --- | --- |
| Output directory | \`$OUT_DIR\` |
| WebTransport runs | \`$WEBTRANSPORT_RUNS\` |
| Sample timeout | \`$SAMPLE_TIMEOUT_SECONDS\` s |
| Browser iterations | \`$WEBTRANSPORT_ITERATIONS\` |
| Browser throughput messages | \`${WEBTRANSPORT_THROUGHPUT_MESSAGES:-$WEBTRANSPORT_ITERATIONS}\` |
| Browser stream timers disabled | \`${WEBTRANSPORT_DISABLE_STREAM_TIMEOUTS:-0}\` |
| Browser congestion control | \`${WEBTRANSPORT_CONGESTION_CONTROL:-default}\` |
| Browser sendMany batch | \`${WEBTRANSPORT_SEND_MANY_BATCH:-1}\` |
| Browser stream read batch | \`${WEBTRANSPORT_STREAM_READ_BATCH:-default}\` |
| Browser stream write batch | \`${WEBTRANSPORT_STREAM_WRITE_BATCH:-default}\` |
| Browser stream write batch bytes | \`${WEBTRANSPORT_STREAM_WRITE_BATCH_BYTES:-default}\` |
| Browser concurrent streams | \`${WEBTRANSPORT_CONCURRENT_STREAMS:-1}\` |
| CMake build type | \`$CMAKE_BUILD_TYPE\` |
| C server included | \`$RUN_C\` |
| Go server included | \`$RUN_GO\` |
| Go ConnectRPC included | \`$RUN_GO_CONNECT\` |
| Rust server included | \`$RUN_RUST\` |
| JS server included | \`$RUN_JS\` |
| ConnectRPC scheme | \`https\` |
| Payload profile | \`$PAYLOAD_PROFILE\` |
| Approx encoded request bytes | \`$ENCODED_REQUEST_BYTES\` |
| Approx encoded response bytes | \`$ENCODED_RESPONSE_BYTES\` |
| Serialization mode | \`$SERIALIZATION_MODE\` |
| Metadata profile | \`$METADATA_PROFILE\` |
| Handshake inclusion mode | \`$HANDSHAKE_INCLUSION_MODE\` |
| Batching settings | \`$WEBTRANSPORT_BATCHING_SETTINGS\` |

## Security and payload model

| Field | Value |
| --- | --- |
| Transport security mode | WebTransport rows and ConnectRPC baseline rows are encrypted |
| Certificate verification mode | WebTransport rows pin the generated server certificate hash through browser WebTransport APIs; HTTPS ConnectRPC rows use a benchmark-only browser context with certificate verification skipped and are labeled \`tls-skip-verify\` |
| Payload profile | \`$PAYLOAD_PROFILE\`; default request/reply text is \`TrevRPC benchmark\` |
| Serialization mode | \`$SERIALIZATION_MODE\`; rows serialize and deserialize protobuf messages per operation/message |
| Metadata profile | \`$METADATA_PROFILE\` |
| Handshake inclusion mode | \`$HANDSHAKE_INCLUSION_MODE\`; clients connect and warm before timed samples |
| Batching settings | \`$WEBTRANSPORT_BATCHING_SETTINGS\` |

## Environment

| Item | Value |
| --- | --- |
| Kernel | \`$(uname -srmo 2>/dev/null || true)\` |
| CPU | \`$(awk -F: '/model name/ {gsub(/^[ \t]+/, "", $2); print $2; exit}' /proc/cpuinfo 2>/dev/null || true)\` |
| CPU governor | \`$(cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor 2>/dev/null || true)\` |
| Node | \`$(node --version 2>/dev/null || true)\` |
| Browser | \`$(node trevrpc-js/bench/webtransport_browser.js --browser-version 2>/dev/null || true)\` |
| Go | \`$(go version 2>/dev/null || true)\` |
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
            if (unsupported[v1] && !unsupported[v2]) {
                return 1
            }
            if (!unsupported[v1] && unsupported[v2]) {
                return -1
            }
            if (unsupported[v1] && unsupported[v2]) {
                return row_order[v1] < row_order[v2] ? -1 : 1
            }
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
        function display_browser(value) {
            sub(/_webtransport$/, "", value)
            return value
        }
        function display_server(value) {
            if (value == "go_connect") {
                return "go_connect"
            }
            if (value == "go_webtransport") {
                return "go_quic"
            }
            if (value == "rust_webtransport") {
                return "rust_quinn"
            }
            if (value == "c_webtransport") {
                return "c_msquic"
            }
            if (value == "js_webtransport") {
                return "js_msquic"
            }
            return value
        }
        function label_language(value) {
            value = display_server(value)
            split(value, parts, "_")
            return parts[1]
        }
        function label_implementation(value) {
            value = display_server(value)
            if (value == "go_connect") {
                return "connect"
            }
            if (value == "go_quic") {
                return "quic-go"
            }
            split(value, parts, "_")
            return substr(value, length(parts[1]) + 2)
        }
        function framework(value) {
            return value == "go_connect" ? "connectRPC" : "trevRPC"
        }
        function format_latency(value) {
            return value == "N/A" ? "N/A" : sprintf("%.3f", value + 0)
        }
        function format_latency_range(min_value, max_value) {
            return min_value == "N/A" ? "N/A" : sprintf("%.3f..%.3f", min_value + 0, max_value + 0)
        }
        function format_throughput(value) {
            return value == "N/A" ? "N/A" : sprintf("%.0f", value + 0)
        }
        function format_throughput_range(min_value, max_value) {
            return min_value == "N/A" ? "N/A" : sprintf("%.0f..%.0f", min_value + 0, max_value + 0)
        }
        function print_table_header(shape) {
            printf "\n### `%s`\n\n", shape
            if (is_throughput_shape(shape)) {
                print "| Framework | Browser | Server language | Server implementation | Median throughput messages/s | Throughput min..max messages/s |"
                print "| --- | --- | --- | --- | ---: | ---: |"
            } else {
                print "| Framework | Browser | Server language | Server implementation | Median latency us/op | Latency min..max us/op |"
                print "| --- | --- | --- | --- | ---: | ---: |"
            }
        }
        NR > 1 {
            id = NR - 1
            row_order[id] = id
            browser[id] = $1
            server[id] = $2
            shape[id] = $3
            unsupported[id] = ($5 == "N/A" || $8 == "N/A")
            latency[id] = unsupported[id] ? "N/A" : $5 + 0
            latency_min[id] = $6
            latency_max[id] = $7
            throughput[id] = unsupported[id] ? "N/A" : $8 + 0
            throughput_min[id] = $9
            throughput_max[id] = $10
            if (!(shape[id] in seen_shape)) {
                seen_shape[shape[id]] = 1
                shape_order[++shape_count] = shape[id]
            }
            shape_rows[shape[id]] = append(shape_rows[shape[id]], id)
        }
        END {
            for (shape_index = 1; shape_index <= shape_count; shape_index++) {
                current_shape = shape_order[shape_index]
                row_count = split(shape_rows[current_shape], rows, " ")
                asort(rows, sorted_rows, "compare_metric")
                print_table_header(current_shape)
                for (row_index = 1; row_index <= row_count; row_index++) {
                    id = sorted_rows[row_index]
                    if (is_throughput_shape(current_shape)) {
                        printf "| `%s` | `%s` | `%s` | `%s` | %s | %s |\n", framework(server[id]), display_browser(browser[id]), label_language(server[id]), label_implementation(server[id]), format_throughput(throughput[id]), format_throughput_range(throughput_min[id], throughput_max[id])
                    } else {
                        printf "| `%s` | `%s` | `%s` | `%s` | %s | %s |\n", framework(server[id]), display_browser(browser[id]), label_language(server[id]), label_implementation(server[id]), format_latency(latency[id]), format_latency_range(latency_min[id], latency_max[id])
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

| Framework | Run | Browser | Server language | Server implementation | Status | Raw output |
| --- | ---: | --- | --- | --- | ---: | --- |
EOF

            awk -F, -v root_prefix="$ROOT/" '
            function display_browser(value) {
                sub(/_webtransport$/, "", value)
                return value
            }
            function display_server(value) {
                if (value == "go_connect") {
                    return "go_connect"
                }
                if (value == "go_webtransport") {
                    return "go_quic"
                }
                if (value == "rust_webtransport") {
                    return "rust_quinn"
                }
                if (value == "c_webtransport") {
                    return "c_msquic"
                }
                if (value == "js_webtransport") {
                    return "js_msquic"
                }
                return value
            }
            function label_language(value) {
                value = display_server(value)
                split(value, parts, "_")
                return parts[1]
            }
            function label_implementation(value) {
                value = display_server(value)
                if (value == "go_connect") {
                    return "connect"
                }
                if (value == "go_quic") {
                    return "quic-go"
                }
                split(value, parts, "_")
                return substr(value, length(parts[1]) + 2)
            }
            function framework(value) {
                return value == "go_connect" ? "connectRPC" : "trevRPC"
            }
            NR > 1 {
                raw_file = $6
                if (index(raw_file, root_prefix) == 1) {
                    raw_file = substr(raw_file, length(root_prefix) + 1)
                }
                printf "| `%s` | %s | `%s` | `%s` | `%s` | %s | `%s` |\n", framework($3), $1, display_browser($2), label_language($3), label_implementation($3), $5, raw_file
            }' "$FAILURES_CSV"
        fi

        cat <<EOF

## Notes

The WebTransport category benchmarks server implementations with a real Chromium WebTransport client driven by Playwright.

Each sample uses one browser WebTransport session per server. Latency rows measure one RPC operation. Stream latency rows use one request and one response message. Stream throughput rows measure messages per second over one open WebTransport stream.

Within the WebTransport tables, Go / \`quic-go\` is the quic-go HTTP/3 WebTransport server path. C / \`msquic\` and JS / \`msquic\` use the native MsQuic-backed WebTransport stack and are reported separately because browser interoperability can differ from the quic-go path.

Rows with framework \`connectRPC\` benchmark browser Fetch over HTTPS against the Go ConnectRPC server. ConnectRPC does not support client-streaming or bidirectional-streaming RPCs from browsers, so those rows are reported as \`N/A\`.

Latency tables are sorted by median latency ascending. Throughput tables are sorted by median message throughput descending.

Raw command output is saved under \`$RAW_DIR\`. Per-measurement normalized rows are saved in \`$SAMPLES_CSV\`. The exact commands are saved in \`$COMMAND_LOG\`.
EOF
    } >"$MARKDOWN"
}

free_port() {
    node -e 'const net = require("node:net"); const server = net.createServer(); server.listen(0, "127.0.0.1", () => { console.log(server.address().port); server.close(); });'
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

wait_for_static_server() {
    local stdout_file=$1
    local pid=$2
    local deadline=$((SECONDS + SERVER_STARTUP_TIMEOUT_SECONDS))
    while ((SECONDS < deadline)); do
        if ! kill -0 "$pid" 2>/dev/null; then
            printf 'static server process %s exited before becoming ready\n' "$pid" >&2
            return 1
        fi
        if [[ -f "$stdout_file" ]] && awk '/serving trevrpc-js from http:\/\/127\.0\.0\.1:/ {found = 1} END {exit found ? 0 : 1}' "$stdout_file"; then
            return 0
        fi
        sleep 0.1
    done
    printf 'timed out waiting for static server in %s\n' "$stdout_file" >&2
    return 1
}

start_static_server() {
    local cert_file=$1
    local stdout_file="$RAW_DIR/browser-static.stdout.txt"
    local stderr_file="$RAW_DIR/browser-static.stderr.txt"
    : >"$stdout_file"
    : >"$stderr_file"
    STATIC_PORT=$(free_port)
    STATIC_ORIGIN="http://127.0.0.1:$STATIC_PORT"
    STATIC_URL="$STATIC_ORIGIN/examples/greeter/"
    log_command env "PORT=$STATIC_PORT" "TREVRPC_EXAMPLE_CERT=$cert_file" node trevrpc-js/examples/greeter/static-server.js
    env "PORT=$STATIC_PORT" "TREVRPC_EXAMPLE_CERT=$cert_file" node trevrpc-js/examples/greeter/static-server.js >"$stdout_file" 2>"$stderr_file" &
    STATIC_SERVER_PID=$!
    wait_for_static_server "$stdout_file" "$STATIC_SERVER_PID"
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
    RPC_SERVER_PIDS+=("$pid")
    START_SERVER_PORT=$(wait_for_port "$stdout_file" "$pid")
}

stop_pid() {
    local pid=$1
    if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
        kill "$pid" 2>/dev/null || true
        wait "$pid" 2>/dev/null || true
    fi
}

stop_rpc_servers() {
    local pid
    for pid in "${RPC_SERVER_PIDS[@]:-}"; do
        stop_pid "$pid"
    done
    RPC_SERVER_PIDS=()
}

stop_all_processes() {
    stop_rpc_servers
    stop_pid "$STATIC_SERVER_PID"
    STATIC_SERVER_PID=""
}

trap stop_all_processes EXIT

run_webtransport_sample() {
    local name=$1
    local run=$2
    local browser=$3
    local server=$4
    local source=$5
    shift 5
    if run_and_capture "$name" timeout --kill-after=5s "${SAMPLE_TIMEOUT_SECONDS}s" "$@"; then
        append_webtransport_csv "$RAW_DIR/$name.txt" "$run" "$browser" "$server" "$source"
    else
        local status=$?
        printf 'sample %s failed with status %d; omitting from aggregate results\n' "$name" "$status" | tee -a "$RAW_DIR/$name.txt" >&2
        printf '%s,%s,%s,%s,%s,%s\n' "$run" "$browser" "$server" "$source" "$status" "$RAW_DIR/$name.txt" >>"$FAILURES_CSV"
    fi
}

build_prerequisites() {
    if [[ "$RUN_C" == "1" || "$RUN_GO" == "1" || "$RUN_JS" == "1" || "$RUN_GO_CONNECT" == "1" ]]; then
        # Browser WebTransport rejects expired certificate hashes; force a fresh short-lived cert.
        rm -f "$CMAKE_BUILD_DIR/msquic-test-cert.pem" "$CMAKE_BUILD_DIR/msquic-test-key.pem"
        run_and_capture c-configure cmake -S trevrpc-c -B "$CMAKE_BUILD_DIR" -DTREVRPC_BUILD_TESTS=OFF -DTREVRPC_BUILD_BENCHMARKS=ON -DCMAKE_BUILD_TYPE="$CMAKE_BUILD_TYPE"
        run_and_capture c-build cmake --build "$CMAKE_BUILD_DIR" --target trevrpc_rpc_comparison_bench
    fi
    if [[ "$RUN_GO" == "1" || "$RUN_GO_CONNECT" == "1" ]]; then
        run_and_capture go-split-build go build -C trevrpc-go -o "$GO_SPLIT_BENCH" ./cmd/trevrpc-rpc-split-bench
    fi
    if [[ "$RUN_RUST" == "1" ]]; then
        run_and_capture rust-split-build cargo build --manifest-path trevrpc-rust/Cargo.toml --example rpc_split_bench --release
    fi
    if [[ "$RUN_JS" == "1" ]]; then
        run_and_capture js-build env "CMAKE_BUILD_TYPE=$CMAKE_BUILD_TYPE" npm --prefix trevrpc-js run build:native
    fi
}

run_browser_against_server() {
    local raw_prefix=$1
    local server=$2
    local port=$3
    local cert_file=$4
    local run
    for ((run = 1; run <= WEBTRANSPORT_RUNS; run++)); do
        run_webtransport_sample "webtransport-$raw_prefix-run-$run" "$run" chrome "$server" playwright-chromium env "${BENCH_ENV[@]}" node trevrpc-js/bench/webtransport_browser.js "$STATIC_URL" "https://127.0.0.1:$port/trevrpc" "$cert_file" "$WEBTRANSPORT_ITERATIONS"
    done
}

run_browser_against_connect_server() {
    local raw_prefix=$1
    local server=$2
    local port=$3
    local run
    for ((run = 1; run <= WEBTRANSPORT_RUNS; run++)); do
        run_webtransport_sample "connect-$raw_prefix-run-$run" "$run" chrome "$server" playwright-chromium env TREVRPC_CONNECT_INSECURE_SKIP_VERIFY=1 "${BENCH_ENV[@]}" node trevrpc-js/bench/webtransport_browser.js --connect "$STATIC_URL" "https://127.0.0.1:$port" "$WEBTRANSPORT_ITERATIONS"
    done
}

run_webtransport_benchmarks() {
    local c_bench="$CMAKE_BUILD_DIR/trevrpc_rpc_comparison_bench"
    local cert_file="$CMAKE_BUILD_DIR/msquic-test-cert.pem"
    local key_file="$CMAKE_BUILD_DIR/msquic-test-key.pem"
    local port

    start_static_server "$cert_file"

    if [[ "$RUN_GO" == "1" ]]; then
        start_server webtransport-go-server env "${BENCH_ENV[@]}" "$GO_SPLIT_BENCH" -mode server -transport webtransport -addr 127.0.0.1:0 -cert "$cert_file" -key "$key_file"
        port=$START_SERVER_PORT
        run_browser_against_server go go_quic "$port" "$cert_file"
        stop_rpc_servers
    fi

    if [[ "$RUN_RUST" == "1" ]]; then
        local rust_cert="$OUT_DIR_ABS/rust-webtransport-cert.pem"
        rm -f "$rust_cert"
        start_server webtransport-rust-server env "${BENCH_ENV[@]}" "$RUST_SPLIT_BENCH" webtransport-server 127.0.0.1:0 "$rust_cert" "$STATIC_ORIGIN"
        port=$START_SERVER_PORT
        run_browser_against_server rust rust_quinn "$port" "$rust_cert"
        stop_rpc_servers
    fi

    if [[ "$RUN_C" == "1" ]]; then
        start_server webtransport-c-server env "${BENCH_ENV[@]}" "$c_bench" --split-serve
        port=$START_SERVER_PORT
        run_browser_against_server c c_msquic "$port" "$cert_file"
        stop_rpc_servers
    fi

    if [[ "$RUN_JS" == "1" ]]; then
        start_server webtransport-js-server env "TREVRPC_WEBTRANSPORT_ORIGIN=$STATIC_ORIGIN" "${BENCH_ENV[@]}" node trevrpc-js/bench/rpc_split_native.js server "$cert_file" "$key_file"
        port=$START_SERVER_PORT
        run_browser_against_server js js_msquic "$port" "$cert_file"
        stop_rpc_servers
    fi

    if [[ "$RUN_GO_CONNECT" == "1" ]]; then
        start_server connect-go-server env "${BENCH_ENV[@]}" "$GO_SPLIT_BENCH" -mode server -transport connect -addr 127.0.0.1:0 -origin "$STATIC_ORIGIN" -cert "$cert_file" -key "$key_file"
        port=$START_SERVER_PORT
        run_browser_against_connect_server go-connect go_connect "$port"
        stop_rpc_servers
    fi
}

cd "$ROOT"

if [[ "$REPORT_ONLY" == "0" ]]; then
    initialize_output
    build_prerequisites
    run_webtransport_benchmarks
fi

aggregate_samples_csv
assert_no_plaintext_rows
assert_benchmark_labels
write_markdown_report

printf '\nWrote WebTransport CSV: %s\n' "$CSV"
printf 'Wrote WebTransport per-measurement CSV: %s\n' "$SAMPLES_CSV"
printf 'Wrote WebTransport Markdown report: %s\n' "$MARKDOWN"
printf 'Wrote WebTransport raw outputs: %s\n' "$RAW_DIR"
