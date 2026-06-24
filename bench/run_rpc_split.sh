#!/usr/bin/env bash

set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
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
SAMPLE_TIMEOUT_SECONDS=${SAMPLE_TIMEOUT_SECONDS:-300}
CMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE:-Release}
CMAKE_BUILD_DIR=${CMAKE_BUILD_DIR:-"$OUT_DIR/c-build"}
SERVER_STARTUP_TIMEOUT_SECONDS=${SERVER_STARTUP_TIMEOUT_SECONDS:-15}

RUN_CLIENT_AXIS=${RUN_CLIENT_AXIS:-1}
RUN_SERVER_AXIS=${RUN_SERVER_AXIS:-1}
RUN_C_MSQUIC=${RUN_C_MSQUIC:-1}
RUN_GO_QUIC=${RUN_GO_QUIC:-1}
RUN_GO_MSQUIC=${RUN_GO_MSQUIC:-1}
RUN_GO_GRPC=${RUN_GO_GRPC:-1}
RUN_JS_NATIVE=${RUN_JS_NATIVE:-1}
RUN_RUST_QUINN=${RUN_RUST_QUINN:-1}

GO_SPLIT_BENCH="$OUT_DIR_ABS/trevrpc-go-rpc-split-bench"
RUST_SPLIT_BENCH="$ROOT/trevrpc-rust/target/release/examples/rpc_split_bench"

usage() {
    cat <<'EOF'
Usage: bench/run_rpc_split.sh
       bench/run_rpc_split.sh --report-only

Runs split TrevRPC client/server benchmarks plus a Go gRPC baseline, and writes CSV/Markdown reports.

Environment knobs:
  OUT_DIR                 Output directory. Default: target/rpc-split
  SPLIT_ITERATIONS        Shared fixed iteration count. Default: 10000
  SPLIT_RUNS              Measurement command repetitions. Default: 3
  SAMPLE_TIMEOUT_SECONDS  Per-sample timeout. Default: 300
  C_ITERATIONS            C client iteration count. Default: SPLIT_ITERATIONS
  GO_ITERATIONS           Go client iteration count. Default: SPLIT_ITERATIONS
  JS_ITERATIONS           JavaScript client iteration count. Default: SPLIT_ITERATIONS
  RUST_ITERATIONS         Rust client iteration count. Default: SPLIT_ITERATIONS
  CMAKE_BUILD_TYPE        CMake build type for C benchmarks. Default: Release
  RUN_CLIENT_AXIS         Benchmark clients against reference C servers. Default: 1
  RUN_SERVER_AXIS         Benchmark servers with reference C clients. Default: 1
  RUN_C_MSQUIC            Include C MsQuic transport. Default: 1
  RUN_GO_QUIC             Include Go quic-go transport. Default: 1
  RUN_GO_MSQUIC           Include Go MsQuic transport. Default: 1
  RUN_GO_GRPC             Include Go gRPC TCP baseline. Default: 1
  RUN_JS_NATIVE           Include native JS MsQuic transport. Default: 1
  RUN_RUST_QUINN          Include Rust Quinn transport. Default: 1

Examples:
  bench/run_rpc_split.sh
  bench/run_rpc_split.sh --report-only
  SPLIT_ITERATIONS=1000 SPLIT_RUNS=1 bench/run_rpc_split.sh
  RUN_SERVER_AXIS=0 bench/run_rpc_split.sh
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
    printf 'axis,run,client,server,shape,latency_us,throughput_per_s,iterations,elapsed_s,source\n' >"$SAMPLES_CSV"
    printf 'axis,run,client,server,source,status,raw_file\n' >"$FAILURES_CSV"
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
    awk -v axis="$axis" -v run="$run" -v client="$client" -v server="$server" -v source="$source" -F '' '
        function emit(shape, latency_us, throughput, iterations, elapsed) {
            printf "%s,%s,%s,%s,%s,%.3f,%.3f,%s,%s,%s\n", axis, run, client, server, shape, latency_us, throughput, iterations, elapsed, source
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
            print "axis,client,server,shape,measurements,latency_us_median,latency_us_min,latency_us_max,throughput_per_s_median,throughput_per_s_min,throughput_per_s_max,iterations_per_measurement,elapsed_s_total,source"
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
                printf "%s,%s,%s,%s,%d,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.0f,%.3f,%s\n", \
                    axis[key], client[key], server[key], shape[key], measurements[key], latency, latency_min[key], latency_max[key], throughput, throughput_min[key], throughput_max[key], median(iterations[key]), elapsed_total[key], source[key]
            }
        }
    ' "$SAMPLES_CSV" >"$CSV"
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
| Go iterations | \`$GO_ITERATIONS\` |
| JS iterations | \`$JS_ITERATIONS\` |
| Rust iterations | \`$RUST_ITERATIONS\` |
| CMake build type | \`$CMAKE_BUILD_TYPE\` |
| Client axis included | \`$RUN_CLIENT_AXIS\` |
| Server axis included | \`$RUN_SERVER_AXIS\` |
| Go gRPC included | \`$RUN_GO_GRPC\` |

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

Rows with framework \`gRPC\` benchmark a Go gRPC client and Go gRPC server split into separate processes over TCP. They are included in both client and server tables as a baseline only and are not transport-compatible with the TrevRPC transport rows.

Raw command output is saved under \`$RAW_DIR\`. Per-measurement normalized rows are saved in \`$SAMPLES_CSV\`. The exact commands are saved in \`$COMMAND_LOG\`.
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
        printf '%s,%s,%s,%s,%s,%s,%s\n' "$axis" "$run" "$client" "$server" "$source" "$status" "$RAW_DIR/$name.txt" >>"$FAILURES_CSV"
    fi
}

build_prerequisites() {
    run_and_capture c-configure cmake -S trevrpc-c -B "$CMAKE_BUILD_DIR" -DTREVRPC_BUILD_TESTS=OFF -DTREVRPC_BUILD_BENCHMARKS=ON -DCMAKE_BUILD_TYPE="$CMAKE_BUILD_TYPE"
    run_and_capture c-build cmake --build "$CMAKE_BUILD_DIR" --target trevrpc_rpc_comparison_bench

    if [[ "$RUN_GO_QUIC" == "1" || "$RUN_GO_MSQUIC" == "1" || "$RUN_GO_GRPC" == "1" ]]; then
        local go_build=(go build -C trevrpc-go -o "$GO_SPLIT_BENCH")
        if [[ "$RUN_GO_MSQUIC" == "1" ]]; then
            go_build=(go build -C trevrpc-go -tags trevrpc_msquic -o "$GO_SPLIT_BENCH")
        fi
        run_and_capture go-split-build "${go_build[@]}" ./cmd/trevrpc-rpc-split-bench
    fi
    if [[ "$RUN_JS_NATIVE" == "1" ]]; then
        run_and_capture js-native-build npm --prefix trevrpc-js run build:native
    fi
    if [[ "$RUN_RUST_QUINN" == "1" ]]; then
        run_and_capture rust-split-build cargo build --manifest-path trevrpc-rust/Cargo.toml --example rpc_split_bench --release
    fi
}

run_client_axis() {
    local c_bench="$CMAKE_BUILD_DIR/trevrpc_rpc_comparison_bench"
    local cert_file="$CMAKE_BUILD_DIR/msquic-test-cert.pem"
    local port
    start_server client-axis-c-server "$c_bench" --split-serve
    port=$START_SERVER_PORT

    for ((run = 1; run <= SPLIT_RUNS; run++)); do
        if [[ "$RUN_C_MSQUIC" == "1" ]]; then
            run_split_sample "client-c-msquic-run-$run" "$run" client c_msquic c_msquic c-custom "$c_bench" --split-client msquic 127.0.0.1 "$port" "$C_ITERATIONS"
        fi
        if [[ "$RUN_GO_QUIC" == "1" ]]; then
            run_split_sample "client-go-quic-run-$run" "$run" client go_quic c_msquic go-custom "$GO_SPLIT_BENCH" -mode client -transport quic -addr "127.0.0.1:$port" -cert "$cert_file" -iterations "$GO_ITERATIONS"
        fi
        if [[ "$RUN_GO_MSQUIC" == "1" ]]; then
            run_split_sample "client-go-msquic-run-$run" "$run" client go_msquic c_msquic go-custom "$GO_SPLIT_BENCH" -mode client -transport msquic -addr "127.0.0.1:$port" -iterations "$GO_ITERATIONS"
        fi
        if [[ "$RUN_JS_NATIVE" == "1" ]]; then
            run_split_sample "client-js-native-run-$run" "$run" client js_msquic c_msquic js-custom node trevrpc-js/bench/rpc_split_native.js client 127.0.0.1 "$port" "$JS_ITERATIONS"
        fi
        if [[ "$RUN_RUST_QUINN" == "1" ]]; then
            run_split_sample "client-rust-quinn-run-$run" "$run" client rust_quinn c_msquic rust-custom "$RUST_SPLIT_BENCH" client "127.0.0.1:$port" "$cert_file" "$RUST_ITERATIONS"
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
        start_server server-axis-c-server "$c_bench" --split-serve
        port=$START_SERVER_PORT
        for ((run = 1; run <= SPLIT_RUNS; run++)); do
            run_split_sample "server-c-msquic-run-$run" "$run" server c_msquic c_msquic c-custom "$c_bench" --split-client msquic 127.0.0.1 "$port" "$C_ITERATIONS"
        done
        stop_servers
    fi

    if [[ "$RUN_GO_QUIC" == "1" ]]; then
        start_server server-axis-go-quic "$GO_SPLIT_BENCH" -mode server -transport quic -addr 127.0.0.1:0 -cert "$cert_file" -key "$key_file"
        port=$START_SERVER_PORT
        for ((run = 1; run <= SPLIT_RUNS; run++)); do
            run_split_sample "server-go-quic-run-$run" "$run" server c_msquic go_quic c-custom "$c_bench" --split-client msquic 127.0.0.1 "$port" "$C_ITERATIONS"
        done
        stop_servers
    fi

    if [[ "$RUN_GO_MSQUIC" == "1" ]]; then
        start_server server-axis-go-msquic "$GO_SPLIT_BENCH" -mode server -transport msquic -addr 127.0.0.1:0 -cert "$cert_file" -key "$key_file"
        port=$START_SERVER_PORT
        for ((run = 1; run <= SPLIT_RUNS; run++)); do
            run_split_sample "server-go-msquic-run-$run" "$run" server c_msquic go_msquic c-custom "$c_bench" --split-client msquic 127.0.0.1 "$port" "$C_ITERATIONS"
        done
        stop_servers
    fi

    if [[ "$RUN_RUST_QUINN" == "1" ]]; then
        start_server server-axis-rust-quinn "$RUST_SPLIT_BENCH" server 127.0.0.1:0
        port=$START_SERVER_PORT
        for ((run = 1; run <= SPLIT_RUNS; run++)); do
            run_split_sample "server-rust-quinn-run-$run" "$run" server c_msquic rust_quinn c-custom "$c_bench" --split-client msquic 127.0.0.1 "$port" "$C_ITERATIONS"
        done
        stop_servers
    fi

    if [[ "$RUN_JS_NATIVE" == "1" ]]; then
        start_server server-axis-js-native node trevrpc-js/bench/rpc_split_native.js server "$cert_file" "$key_file"
        port=$START_SERVER_PORT
        for ((run = 1; run <= SPLIT_RUNS; run++)); do
            run_split_sample "server-js-native-run-$run" "$run" server c_msquic js_msquic c-custom "$c_bench" --split-client msquic 127.0.0.1 "$port" "$C_ITERATIONS"
        done
        stop_servers
    fi
}

run_grpc_axis() {
    start_server grpc-go-server "$GO_SPLIT_BENCH" -mode server -transport grpc -addr 127.0.0.1:0
    local port=$START_SERVER_PORT
    local run
    for ((run = 1; run <= SPLIT_RUNS; run++)); do
        run_split_sample "grpc-go-run-$run" "$run" grpc grpc_go grpc_go go-custom "$GO_SPLIT_BENCH" -mode client -transport grpc -addr "127.0.0.1:$port" -iterations "$GO_ITERATIONS"
    done
    stop_servers
}

cd "$ROOT"

if [[ "$REPORT_ONLY" == "0" ]]; then
    initialize_output
    build_prerequisites
    if [[ "$RUN_CLIENT_AXIS" == "1" ]]; then
        run_client_axis
    fi
    if [[ "$RUN_SERVER_AXIS" == "1" ]]; then
        run_server_axis
    fi
    if [[ "$RUN_GO_GRPC" == "1" ]]; then
        run_grpc_axis
    fi
fi

aggregate_samples_csv
write_markdown_report

printf '\nWrote split CSV: %s\n' "$CSV"
printf 'Wrote split per-measurement CSV: %s\n' "$SAMPLES_CSV"
printf 'Wrote split Markdown report: %s\n' "$MARKDOWN"
printf 'Wrote split raw outputs: %s\n' "$RAW_DIR"
