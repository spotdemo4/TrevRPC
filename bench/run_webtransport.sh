#!/usr/bin/env bash

set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
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
RUN_GO_MSQUIC=${RUN_GO_MSQUIC:-1}
RUN_RUST=${RUN_RUST:-1}
RUN_JS=${RUN_JS:-${RUN_JS_NATIVE:-1}}

GO_SPLIT_BENCH="$OUT_DIR_ABS/trevrpc-go-rpc-split-bench"
GO_SPLIT_BENCH_MSQUIC="$OUT_DIR_ABS/trevrpc-go-rpc-split-webtransport-msquic-bench"
RUST_SPLIT_BENCH="$ROOT/trevrpc-rust/target/release/examples/rpc_split_bench"

usage() {
    cat <<'EOF'
Usage: bench/run_webtransport.sh
       bench/run_webtransport.sh --report-only

Runs browser WebTransport benchmarks with a Chromium client driven by Playwright,
then writes normalized CSV/Markdown reports.

Environment knobs:
  OUT_DIR                    Output directory. Default: target/webtransport
  WEBTRANSPORT_ITERATIONS    Fixed iteration count per browser sample. Default: 10000
  WEBTRANSPORT_RUNS          Measurement command repetitions. Default: 3
  SAMPLE_TIMEOUT_SECONDS     Per-sample timeout. Default: 300
  CMAKE_BUILD_TYPE           CMake build type for C benchmarks/certificates. Default: Release
  RUN_C                      Include trevrpc-c WebTransport server. Default: 1
  RUN_GO                     Include Go WebTransport server. Default: 1
  RUN_GO_MSQUIC              Include Go MsQuic-backed WebTransport server. Default: 1
  RUN_RUST                   Include Rust WebTransport server. Default: 1
  RUN_JS                     Include JS WebTransport server. Default: 1
  TREVRPC_BROWSER_CHROMIUM   Optional Chromium executable path for Playwright.

Examples:
  bench/run_webtransport.sh
  WEBTRANSPORT_ITERATIONS=1000 WEBTRANSPORT_RUNS=1 bench/run_webtransport.sh
  RUN_RUST=0 bench/run_webtransport.sh
EOF
}

REPORT_ONLY=0
case "${1:-}" in
"-h" | "--help")
    usage
    exit 0
    ;;
"--report-only")
    REPORT_ONLY=1
    ;;
"") ;;
*)
    usage >&2
    exit 2
    ;;
esac

initialize_output() {
    mkdir -p "$RAW_DIR"
    : >"$COMMAND_LOG"
    rm -f "$RAW_DIR"/*.txt
    printf 'run,browser,server,shape,latency_us,throughput_per_s,iterations,elapsed_s,source\n' >"$SAMPLES_CSV"
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
    awk -v run="$run" -v browser="$browser" -v server="$server" -v source="$source" -F '' '
        function emit(shape, latency_us, throughput, iterations, elapsed) {
            printf "%s,%s,%s,%s,%.3f,%.3f,%s,%s,%s\n", run, browser, server, shape, latency_us, throughput, iterations, elapsed, source
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
            print "browser,server,shape,measurements,latency_us_median,latency_us_min,latency_us_max,throughput_per_s_median,throughput_per_s_min,throughput_per_s_max,iterations_per_measurement,elapsed_s_total,source"
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
            }
            measurements[key]++
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
                latency = median(latencies[key])
                throughput = median(throughputs[key])
                printf "%s,%s,%s,%d,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.0f,%.3f,%s\n", \
                    browser[key], server[key], shape[key], measurements[key], latency, latency_min[key], latency_max[key], throughput, throughput_min[key], throughput_max[key], median(iterations[key]), elapsed_total[key], source[key]
            }
        }
    ' "$SAMPLES_CSV" >"$CSV"
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
| CMake build type | \`$CMAKE_BUILD_TYPE\` |
| C server included | \`$RUN_C\` |
| Go server included | \`$RUN_GO\` |
| Go MsQuic server included | \`$RUN_GO_MSQUIC\` |
| Rust server included | \`$RUN_RUST\` |
| JS server included | \`$RUN_JS\` |

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
            if (value == "go_webtransport") {
                return "go_quic"
            }
            if (value == "go_webtransport_msquic") {
                return "go_msquic"
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
            if (value == "go_quic") {
                return "quic-go"
            }
            split(value, parts, "_")
            return substr(value, length(parts[1]) + 2)
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
            latency[id] = $5 + 0
            latency_min[id] = $6
            latency_max[id] = $7
            throughput[id] = $8 + 0
            throughput_min[id] = $9 + 0
            throughput_max[id] = $10 + 0
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
                        printf "| `trevRPC` | `%s` | `%s` | `%s` | %.0f | %.0f..%.0f |\n", display_browser(browser[id]), label_language(server[id]), label_implementation(server[id]), throughput[id], throughput_min[id], throughput_max[id]
                    } else {
                        printf "| `trevRPC` | `%s` | `%s` | `%s` | %.3f | %.3f..%.3f |\n", display_browser(browser[id]), label_language(server[id]), label_implementation(server[id]), latency[id], latency_min[id], latency_max[id]
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
                if (value == "go_webtransport") {
                    return "go_quic"
                }
                if (value == "go_webtransport_msquic") {
                    return "go_msquic"
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
                if (value == "go_quic") {
                    return "quic-go"
                }
                split(value, parts, "_")
                return substr(value, length(parts[1]) + 2)
            }
            NR > 1 {
                raw_file = $6
                if (index(raw_file, root_prefix) == 1) {
                    raw_file = substr(raw_file, length(root_prefix) + 1)
                }
                printf "| `trevRPC` | %s | `%s` | `%s` | `%s` | %s | `%s` |\n", $1, display_browser($2), label_language($3), label_implementation($3), $5, raw_file
            }' "$FAILURES_CSV"
        fi

        cat <<EOF

## Notes

The WebTransport category benchmarks server implementations with a real Chromium WebTransport client driven by Playwright.

Each sample uses one browser WebTransport session per server. Latency rows measure one RPC operation. Stream latency rows use one request and one response message. Stream throughput rows measure messages per second over one open WebTransport stream.

Within the WebTransport tables, Go / \`quic-go\` is the quic-go HTTP/3 WebTransport server path. Go / \`msquic\`, C / \`msquic\`, and JS / \`msquic\` use the native MsQuic-backed WebTransport stack and are reported separately because browser interoperability can differ from the quic-go path.

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
    if [[ "$RUN_C" == "1" || "$RUN_GO" == "1" || "$RUN_GO_MSQUIC" == "1" || "$RUN_JS" == "1" ]]; then
        # Browser WebTransport rejects expired certificate hashes; force a fresh short-lived cert.
        rm -f "$CMAKE_BUILD_DIR/msquic-test-cert.pem" "$CMAKE_BUILD_DIR/msquic-test-key.pem"
        run_and_capture c-configure cmake -S trevrpc-c -B "$CMAKE_BUILD_DIR" -DTREVRPC_BUILD_TESTS=OFF -DTREVRPC_BUILD_BENCHMARKS=ON -DCMAKE_BUILD_TYPE="$CMAKE_BUILD_TYPE"
        run_and_capture c-build cmake --build "$CMAKE_BUILD_DIR" --target trevrpc_rpc_comparison_bench
    fi
    if [[ "$RUN_GO" == "1" ]]; then
        run_and_capture go-split-build go build -C trevrpc-go -o "$GO_SPLIT_BENCH" ./cmd/trevrpc-rpc-split-bench
    fi
    if [[ "$RUN_GO_MSQUIC" == "1" ]]; then
        run_and_capture go-split-msquic-build go build -C trevrpc-go -a -tags "trevrpc_msquic trevrpc_webtransport_native" -o "$GO_SPLIT_BENCH_MSQUIC" ./cmd/trevrpc-rpc-split-bench
    fi
    if [[ "$RUN_RUST" == "1" ]]; then
        run_and_capture rust-split-build cargo build --manifest-path trevrpc-rust/Cargo.toml --example rpc_split_bench --release
    fi
    if [[ "$RUN_JS" == "1" ]]; then
        run_and_capture js-build npm --prefix trevrpc-js run build:native
    fi
}

run_browser_against_server() {
    local raw_prefix=$1
    local server=$2
    local port=$3
    local cert_file=$4
    local run
    for ((run = 1; run <= WEBTRANSPORT_RUNS; run++)); do
        run_webtransport_sample "webtransport-$raw_prefix-run-$run" "$run" chrome "$server" playwright-chromium node trevrpc-js/bench/webtransport_browser.js "$STATIC_URL" "https://127.0.0.1:$port/trevrpc" "$cert_file" "$WEBTRANSPORT_ITERATIONS"
    done
}

run_webtransport_benchmarks() {
    local c_bench="$CMAKE_BUILD_DIR/trevrpc_rpc_comparison_bench"
    local cert_file="$CMAKE_BUILD_DIR/msquic-test-cert.pem"
    local key_file="$CMAKE_BUILD_DIR/msquic-test-key.pem"
    local port

    start_static_server "$cert_file"

    if [[ "$RUN_GO" == "1" ]]; then
        start_server webtransport-go-server "$GO_SPLIT_BENCH" -mode server -transport webtransport -addr 127.0.0.1:0 -cert "$cert_file" -key "$key_file"
        port=$START_SERVER_PORT
        run_browser_against_server go go_quic "$port" "$cert_file"
        stop_rpc_servers
    fi

    if [[ "$RUN_GO_MSQUIC" == "1" ]]; then
        start_server webtransport-go-msquic-server "$GO_SPLIT_BENCH_MSQUIC" -mode server -transport webtransport-msquic -addr 127.0.0.1:0 -cert "$cert_file" -key "$key_file" -origin "$STATIC_ORIGIN"
        port=$START_SERVER_PORT
        run_browser_against_server go-msquic go_msquic "$port" "$cert_file"
        stop_rpc_servers
    fi

    if [[ "$RUN_RUST" == "1" ]]; then
        local rust_cert="$OUT_DIR_ABS/rust-webtransport-cert.pem"
        rm -f "$rust_cert"
        start_server webtransport-rust-server "$RUST_SPLIT_BENCH" webtransport-server 127.0.0.1:0 "$rust_cert" "$STATIC_ORIGIN"
        port=$START_SERVER_PORT
        run_browser_against_server rust rust_quinn "$port" "$rust_cert"
        stop_rpc_servers
    fi

    if [[ "$RUN_C" == "1" ]]; then
        start_server webtransport-c-server "$c_bench" --split-serve
        port=$START_SERVER_PORT
        run_browser_against_server c c_msquic "$port" "$cert_file"
        stop_rpc_servers
    fi

    if [[ "$RUN_JS" == "1" ]]; then
        start_server webtransport-js-server env "TREVRPC_WEBTRANSPORT_ORIGIN=$STATIC_ORIGIN" node trevrpc-js/bench/rpc_split_native.js server "$cert_file" "$key_file"
        port=$START_SERVER_PORT
        run_browser_against_server js js_msquic "$port" "$cert_file"
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
write_markdown_report

printf '\nWrote WebTransport CSV: %s\n' "$CSV"
printf 'Wrote WebTransport per-measurement CSV: %s\n' "$SAMPLES_CSV"
printf 'Wrote WebTransport Markdown report: %s\n' "$MARKDOWN"
printf 'Wrote WebTransport raw outputs: %s\n' "$RAW_DIR"
