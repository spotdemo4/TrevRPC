#!/usr/bin/env bash

set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
OUT_DIR=${OUT_DIR:-"$ROOT/target/rpc-split"}
RAW_DIR="$OUT_DIR/raw"
COMMAND_LOG="$OUT_DIR/commands.txt"
CSV="$OUT_DIR/rpc-split.csv"
SAMPLES_CSV="$OUT_DIR/rpc-split-samples.csv"
MARKDOWN="$OUT_DIR/rpc-split.md"

SPLIT_ITERATIONS=${SPLIT_ITERATIONS:-10000}
C_ITERATIONS=${C_ITERATIONS:-$SPLIT_ITERATIONS}
GO_ITERATIONS=${GO_ITERATIONS:-$SPLIT_ITERATIONS}
JS_ITERATIONS=${JS_ITERATIONS:-$SPLIT_ITERATIONS}
RUST_ITERATIONS=${RUST_ITERATIONS:-$SPLIT_ITERATIONS}
SPLIT_RUNS=${SPLIT_RUNS:-3}
CMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE:-Release}
CMAKE_BUILD_DIR=${CMAKE_BUILD_DIR:-"$OUT_DIR/c-build"}
SERVER_STARTUP_TIMEOUT_SECONDS=${SERVER_STARTUP_TIMEOUT_SECONDS:-15}

RUN_CLIENT_AXIS=${RUN_CLIENT_AXIS:-1}
RUN_SERVER_AXIS=${RUN_SERVER_AXIS:-1}
RUN_C_MSQUIC=${RUN_C_MSQUIC:-1}
RUN_C_WEBTRANSPORT=${RUN_C_WEBTRANSPORT:-1}
RUN_GO_QUIC=${RUN_GO_QUIC:-1}
RUN_GO_MSQUIC=${RUN_GO_MSQUIC:-1}
RUN_JS_NATIVE=${RUN_JS_NATIVE:-1}
RUN_RUST_QUINN=${RUN_RUST_QUINN:-1}

GO_SPLIT_BENCH="$OUT_DIR/trevrpc-go-rpc-split-bench"
RUST_SPLIT_BENCH="$ROOT/trevrpc-rust/target/release/examples/rpc_split_bench"

usage() {
    cat <<'EOF'
Usage: bench/run_rpc_split.sh

Runs split TrevRPC client/server benchmarks and writes CSV/Markdown reports.
gRPC is intentionally excluded; it remains a paired benchmark follow-up.

Environment knobs:
  OUT_DIR                 Output directory. Default: target/rpc-split
  SPLIT_ITERATIONS        Shared fixed iteration count. Default: 10000
  SPLIT_RUNS              Measurement command repetitions. Default: 3
  C_ITERATIONS            C client iteration count. Default: SPLIT_ITERATIONS
  GO_ITERATIONS           Go client iteration count. Default: SPLIT_ITERATIONS
  JS_ITERATIONS           JavaScript client iteration count. Default: SPLIT_ITERATIONS
  RUST_ITERATIONS         Rust client iteration count. Default: SPLIT_ITERATIONS
  CMAKE_BUILD_TYPE        CMake build type for C benchmarks. Default: Release
  RUN_CLIENT_AXIS         Benchmark clients against reference C servers. Default: 1
  RUN_SERVER_AXIS         Benchmark servers with reference C clients. Default: 1
  RUN_C_MSQUIC            Include C MsQuic transport. Default: 1
  RUN_C_WEBTRANSPORT      Include C WebTransport transport. Default: 1
  RUN_GO_QUIC             Include Go quic-go transport. Default: 1
  RUN_GO_MSQUIC           Include Go MsQuic transport. Default: 1
  RUN_JS_NATIVE           Include native JS WebTransport transport. Default: 1
  RUN_RUST_QUINN          Include Rust Quinn transport. Default: 1

Examples:
  bench/run_rpc_split.sh
  SPLIT_ITERATIONS=1000 SPLIT_RUNS=1 bench/run_rpc_split.sh
  RUN_SERVER_AXIS=0 bench/run_rpc_split.sh
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
    usage
    exit 0
fi

mkdir -p "$RAW_DIR"
: >"$COMMAND_LOG"
rm -f "$RAW_DIR"/*.txt
printf 'axis,run,client,server,shape,latency_us,throughput_ops_s,iterations,elapsed_s,source\n' >"$SAMPLES_CSV"

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
        match($0, /^([^:]+):[[:space:]]+([0-9.]+) ops\/s \(([0-9]+) iterations in ([0-9.]+)s\)/, m) {
            ops = m[2] + 0
            latency_us = ops > 0 ? 1000000.0 / ops : 0
            printf "%s,%s,%s,%s,%s,%.3f,%.3f,%s,%s,%s\n", axis, run, client, server, m[1], latency_us, ops, m[3], m[4], source
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
            print "axis,client,server,shape,measurements,latency_us_median,latency_us_min,latency_us_max,throughput_ops_s,iterations_per_measurement,elapsed_s_total,source"
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
            iterations[key] = append(iterations[key], $8)
            elapsed_total[key] += $9
            if (!(key in latency_min) || $6 + 0 < latency_min[key]) {
                latency_min[key] = $6 + 0
            }
            if (!(key in latency_max) || $6 + 0 > latency_max[key]) {
                latency_max[key] = $6 + 0
            }
        }
        END {
            for (i = 1; i <= order_count; i++) {
                key = order[i]
                latency = median(latencies[key])
                throughput = latency > 0 ? 1000000.0 / latency : 0
                printf "%s,%s,%s,%s,%d,%.3f,%.3f,%.3f,%.3f,%.0f,%.3f,%s\n", \
                    axis[key], client[key], server[key], shape[key], measurements[key], latency, latency_min[key], latency_max[key], throughput, median(iterations[key]), elapsed_total[key], source[key]
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
| C iterations | \`$C_ITERATIONS\` |
| Go iterations | \`$GO_ITERATIONS\` |
| JS iterations | \`$JS_ITERATIONS\` |
| Rust iterations | \`$RUST_ITERATIONS\` |
| CMake build type | \`$CMAKE_BUILD_TYPE\` |
| Client axis included | \`$RUN_CLIENT_AXIS\` |
| Server axis included | \`$RUN_SERVER_AXIS\` |

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
        function compare_latency(i1, v1, i2, v2) {
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
        function print_table_header(axis, shape) {
            printf "\n### `%s` / `%s`\n\n", axis, shape
            print "| Client | Server | Measurements | Median latency us/op | Latency min..max us/op | Median throughput ops/s | Iterations per measurement | Source |"
            print "| --- | --- | ---: | ---: | ---: | ---: | ---: | --- |"
        }
        NR > 1 {
            id = NR - 1
            row_order[id] = id
            axis[id] = $1
            client[id] = $2
            server[id] = $3
            shape[id] = $4
            measurements[id] = $5
            latency[id] = $6 + 0
            latency_min[id] = $7
            latency_max[id] = $8
            throughput[id] = $9
            iterations[id] = $10
            source[id] = $12
            group_key = axis[id] SUBSEP shape[id]
            if (!(group_key in seen_group)) {
                seen_group[group_key] = 1
                groups[++group_count] = group_key
                group_axis[group_key] = axis[id]
                group_shape[group_key] = shape[id]
            }
            group_rows[group_key] = append(group_rows[group_key], id)
        }
        END {
            for (group_index = 1; group_index <= group_count; group_index++) {
                group_key = groups[group_index]
                row_count = split(group_rows[group_key], rows, " ")
                asort(rows, sorted_rows, "compare_latency")
                print_table_header(group_axis[group_key], group_shape[group_key])
                for (row_index = 1; row_index <= row_count; row_index++) {
                    id = sorted_rows[row_index]
                    printf "| `%s` | `%s` | %s | %.3f | %.3f..%.3f | %.0f | %s | `%s` |\n", client[id], server[id], measurements[id], latency[id], latency_min[id], latency_max[id], throughput[id], iterations[id], source[id]
                }
            }
        }' "$CSV"

        cat <<EOF

## Notes

The client axis benchmarks each TrevRPC client transport against a reference C server for the matching wire protocol. Raw QUIC transports use the C MsQuic listener; WebTransport transports use the C WebTransport listener.

The server axis benchmarks each TrevRPC server transport using a reference C client for the matching wire protocol.

gRPC is intentionally excluded because it needs a separate split harness and is not transport-compatible with TrevRPC.

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
    run_and_capture "$name" "$@"
    append_split_csv "$RAW_DIR/$name.txt" "$run" "$axis" "$client" "$server" "$source"
}

build_prerequisites() {
    run_and_capture c-configure cmake -S trevrpc-c -B "$CMAKE_BUILD_DIR" -DTREVRPC_BUILD_TESTS=OFF -DTREVRPC_BUILD_BENCHMARKS=ON -DCMAKE_BUILD_TYPE="$CMAKE_BUILD_TYPE"
    run_and_capture c-build cmake --build "$CMAKE_BUILD_DIR" --target trevrpc_rpc_comparison_bench

    if [[ "$RUN_GO_QUIC" == "1" || "$RUN_GO_MSQUIC" == "1" ]]; then
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
        if [[ "$RUN_C_WEBTRANSPORT" == "1" ]]; then
            run_split_sample "client-c-webtransport-run-$run" "$run" client c_webtransport c_webtransport c-custom "$c_bench" --split-client webtransport 127.0.0.1 "$port" "$C_ITERATIONS"
        fi
        if [[ "$RUN_GO_QUIC" == "1" ]]; then
            run_split_sample "client-go-quic-run-$run" "$run" client go_quic c_msquic go-custom "$GO_SPLIT_BENCH" -mode client -transport quic -addr "127.0.0.1:$port" -cert "$cert_file" -iterations "$GO_ITERATIONS"
        fi
        if [[ "$RUN_GO_MSQUIC" == "1" ]]; then
            run_split_sample "client-go-msquic-run-$run" "$run" client go_msquic c_msquic go-custom "$GO_SPLIT_BENCH" -mode client -transport msquic -addr "127.0.0.1:$port" -iterations "$GO_ITERATIONS"
        fi
        if [[ "$RUN_JS_NATIVE" == "1" ]]; then
            run_split_sample "client-js-native-run-$run" "$run" client js_native_webtransport c_webtransport js-custom node trevrpc-js/bench/rpc_split_native.js client 127.0.0.1 "$port" "$JS_ITERATIONS"
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

    if [[ "$RUN_C_MSQUIC" == "1" || "$RUN_C_WEBTRANSPORT" == "1" ]]; then
        start_server server-axis-c-server "$c_bench" --split-serve
        port=$START_SERVER_PORT
        for ((run = 1; run <= SPLIT_RUNS; run++)); do
            if [[ "$RUN_C_MSQUIC" == "1" ]]; then
                run_split_sample "server-c-msquic-run-$run" "$run" server c_msquic c_msquic c-custom "$c_bench" --split-client msquic 127.0.0.1 "$port" "$C_ITERATIONS"
            fi
            if [[ "$RUN_C_WEBTRANSPORT" == "1" ]]; then
                run_split_sample "server-c-webtransport-run-$run" "$run" server c_webtransport c_webtransport c-custom "$c_bench" --split-client webtransport 127.0.0.1 "$port" "$C_ITERATIONS"
            fi
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
            run_split_sample "server-js-native-run-$run" "$run" server c_webtransport js_native_webtransport c-custom "$c_bench" --split-client webtransport 127.0.0.1 "$port" "$C_ITERATIONS"
        done
        stop_servers
    fi
}

cd "$ROOT"

build_prerequisites
if [[ "$RUN_CLIENT_AXIS" == "1" ]]; then
    run_client_axis
fi
if [[ "$RUN_SERVER_AXIS" == "1" ]]; then
    run_server_axis
fi

aggregate_samples_csv
write_markdown_report

printf '\nWrote split CSV: %s\n' "$CSV"
printf 'Wrote split per-measurement CSV: %s\n' "$SAMPLES_CSV"
printf 'Wrote split Markdown report: %s\n' "$MARKDOWN"
printf 'Wrote split raw outputs: %s\n' "$RAW_DIR"
