#!/usr/bin/env bash

set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
OUT_DIR=${OUT_DIR:-"$ROOT/target/rpc-comparison"}
RAW_DIR="$OUT_DIR/raw"
COMMAND_LOG="$OUT_DIR/commands.txt"
CSV="$OUT_DIR/rpc-comparison.csv"
SAMPLES_CSV="$OUT_DIR/rpc-comparison-samples.csv"
MARKDOWN="$OUT_DIR/rpc-comparison.md"

RPC_ITERATIONS=${RPC_ITERATIONS:-10000}
RPC_RUNS=${RPC_RUNS:-3}
C_ITERATIONS=${C_ITERATIONS:-$RPC_ITERATIONS}
GO_BENCHTIME=${GO_BENCHTIME:-${RPC_ITERATIONS}x}
GO_COUNT=${GO_COUNT:-1}
RUST_SAMPLE_SIZE=${RUST_SAMPLE_SIZE:-100}
RUST_WARM_UP_TIME=${RUST_WARM_UP_TIME:-3}
RUST_MEASUREMENT_TIME=${RUST_MEASUREMENT_TIME:-10}
CMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE:-Release}
CMAKE_BUILD_DIR=${CMAKE_BUILD_DIR:-"$OUT_DIR/c-build"}

RUN_C=${RUN_C:-1}
RUN_GO=${RUN_GO:-1}
RUN_GO_NATIVE_MSQUIC=${RUN_GO_NATIVE_MSQUIC:-1}
RUN_RUST=${RUN_RUST:-1}

usage() {
    cat <<'EOF'
Usage: bench/run_rpc_comparison.sh

Runs the checked-in C, Go, and Rust RPC-shape benchmarks, saves raw output,
and writes normalized CSV/Markdown reports.

Environment knobs:
  OUT_DIR                 Output directory. Default: target/rpc-comparison
  RPC_ITERATIONS          Shared fixed iteration count for C and Go. Default: 10000
  RPC_RUNS                Measurement command repetitions. Default: 3
  C_ITERATIONS            C benchmark iterations. Default: RPC_ITERATIONS
  GO_BENCHTIME            Go -benchtime value. Default: ${RPC_ITERATIONS}x
  GO_COUNT                Go -count value per RPC_RUNS repetition. Default: 1
  RUST_SAMPLE_SIZE        Criterion sample size. Default: 100
  RUST_WARM_UP_TIME       Criterion warm-up seconds. Default: 3
  RUST_MEASUREMENT_TIME   Criterion measurement seconds. Default: 10
  CMAKE_BUILD_TYPE        CMake build type for C benchmarks. Default: Release
  RUN_C                   Run C benchmark. Default: 1
  RUN_GO                  Run Go quic-go/grpc benchmark. Default: 1
  RUN_GO_NATIVE_MSQUIC    Run Go native-CGO MsQuic benchmark. Default: 1
  RUN_RUST                Run Rust Criterion benchmark. Default: 1

Examples:
  bench/run_rpc_comparison.sh
  RPC_ITERATIONS=1000 RPC_RUNS=1 RUST_SAMPLE_SIZE=20 RUST_MEASUREMENT_TIME=3 bench/run_rpc_comparison.sh
  RUN_GO_NATIVE_MSQUIC=0 bench/run_rpc_comparison.sh
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
    usage
    exit 0
fi

mkdir -p "$RAW_DIR"
: >"$COMMAND_LOG"
rm -f "$RAW_DIR"/*.txt
printf 'run,language,shape,implementation,latency_us,throughput_ops_s,iterations_or_samples,elapsed_s,alloc_bytes_per_op,allocs_per_op,source\n' >"$SAMPLES_CSV"

require_positive_integer() {
    local name=$1
    local value=$2
    if [[ ! "$value" =~ ^[1-9][0-9]*$ ]]; then
        printf '%s must be a positive integer, got %q\n' "$name" "$value" >&2
        exit 2
    fi
}

require_positive_integer RPC_RUNS "$RPC_RUNS"

quote_command() {
    printf '%q ' "$@"
}

run_and_capture() {
    local name=$1
    shift
    local raw_file="$RAW_DIR/$name.txt"

    {
        printf '$ '
        quote_command "$@"
        printf '\n'
    } | tee -a "$COMMAND_LOG"

    "$@" 2>&1 | tee "$raw_file"
}

append_c_csv() {
    local raw_file=$1
    local run=$2
    awk -v source="c-custom" -v run="$run" -F '' '
        match($0, /^([^:]+):[[:space:]]+([0-9.]+) ops\/s \(([0-9]+) iterations in ([0-9.]+)s\)/, m) {
            split(m[1], name, "/")
            if (length(name) != 2) {
                next
            }
            ops = m[2] + 0
            latency_us = ops > 0 ? 1000000.0 / ops : 0
            printf "%s,c,%s,%s,%.3f,%.3f,%s,%s,,,%s\n", run, name[1], name[2], latency_us, ops, m[3], m[4], source
        }
    ' "$raw_file" >>"$SAMPLES_CSV"
}

append_go_csv() {
    local raw_file=$1
    local run=$2
    awk -v source="go-testing" -v run="$run" '
        /^BenchmarkRPCComparison/ {
            benchmark = $1
            sub(/^BenchmarkRPCComparisonNativeMsQuic\//, "", benchmark)
            sub(/^BenchmarkRPCComparison\//, "", benchmark)
            sub(/-[0-9]+$/, "", benchmark)
            split(benchmark, name, "/")
            if (length(name) != 2) {
                next
            }

            ns = 0
            bytes = ""
            allocs = ""
            for (i = 2; i <= NF; i++) {
                if ($(i + 1) == "ns/op") {
                    ns = $i + 0
                }
                if ($(i + 1) == "B/op") {
                    bytes = $i
                }
                if ($(i + 1) == "allocs/op") {
                    allocs = $i
                }
            }
            if (ns <= 0) {
                next
            }
            latency_us = ns / 1000.0
            ops = 1000000000.0 / ns
            printf "%s,go,%s,%s,%.3f,%.3f,%s,,%s,%s,%s\n", run, name[1], name[2], latency_us, ops, $2, bytes, allocs, source
        }
    ' "$raw_file" >>"$SAMPLES_CSV"
}

append_rust_csv() {
    local raw_file=$1
    local run=$2
    awk -v source="criterion" -v samples="$RUST_SAMPLE_SIZE" -v run="$run" '
        function clean(value) {
            gsub(/[\[\]]/, "", value)
            return value
        }
        function to_us(value, unit) {
            value += 0
            unit = clean(unit)
            if (unit ~ /^ns$/) {
                return value / 1000.0
            }
            if (unit ~ /^(us|µs)$/) {
                return value
            }
            if (unit ~ /^ms$/) {
                return value * 1000.0
            }
            if (unit ~ /^s$/) {
                return value * 1000000.0
            }
            return 0
        }
        /^Benchmarking[[:space:]]+[^[:space:]]+\/[^[:space:]]+/ {
            current = $2
            sub(/:$/, "", current)
        }
        /^[^[:space:]]+\/[^[:space:]]+[[:space:]]*$/ {
            current = $1
        }
        /time:[[:space:]]+\[/ && current != "" {
            latency_us = to_us(clean($4), $5)
            if (latency_us <= 0) {
                next
            }
            split(current, name, "/")
            if (length(name) != 2) {
                next
            }
            ops = 1000000.0 / latency_us
            printf "%s,rust,%s,%s,%.3f,%.3f,%s,,,,%s\n", run, name[1], name[2], latency_us, ops, samples, source
            current = ""
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
        function format_optional_integer(value) {
            return value == "" ? "" : sprintf("%.0f", value + 0)
        }
        BEGIN {
            print "language,shape,implementation,measurements,latency_us_median,latency_us_min,latency_us_max,throughput_ops_s,iterations_or_samples_per_measurement,elapsed_s_total,alloc_bytes_per_op_median,allocs_per_op_median,source"
        }
        NR == 1 {
            next
        }
        {
            key = $2 SUBSEP $3 SUBSEP $4
            if (!(key in seen)) {
                seen[key] = 1
                order[++order_count] = key
                language[key] = $2
                shape[key] = $3
                implementation[key] = $4
                source[key] = $11
            }

            measurements[key]++
            latencies[key] = append(latencies[key], $5)
            iterations[key] = append(iterations[key], $7)

            if (!(key in latency_min) || $5 + 0 < latency_min[key]) {
                latency_min[key] = $5 + 0
            }
            if (!(key in latency_max) || $5 + 0 > latency_max[key]) {
                latency_max[key] = $5 + 0
            }
            if ($8 != "") {
                elapsed_seen[key] = 1
                elapsed_total[key] += $8
            }
            if ($9 != "") {
                bytes[key] = append(bytes[key], $9)
            }
            if ($10 != "") {
                allocs[key] = append(allocs[key], $10)
            }
        }
        END {
            for (i = 1; i <= order_count; i++) {
                key = order[i]
                latency = median(latencies[key])
                throughput = latency > 0 ? 1000000.0 / latency : 0
                iteration_count = median(iterations[key])
                elapsed = (key in elapsed_seen) ? sprintf("%.3f", elapsed_total[key]) : ""
                printf "%s,%s,%s,%d,%.3f,%.3f,%.3f,%.3f,%.0f,%s,%s,%s,%s\n", \
                    language[key], \
                    shape[key], \
                    implementation[key], \
                    measurements[key], \
                    latency, \
                    latency_min[key], \
                    latency_max[key], \
                    throughput, \
                    iteration_count, \
                    elapsed, \
                    format_optional_integer(median(bytes[key])), \
                    format_optional_integer(median(allocs[key])), \
                    source[key]
            }
        }
    ' "$SAMPLES_CSV" >"$CSV"
}

write_markdown_report() {
    local generated_at
    generated_at=$(date -u +'%Y-%m-%dT%H:%M:%SZ')

    {
        cat <<EOF
# TrevRPC Cross-Language RPC Benchmark Report

Generated: $generated_at

## Settings

| Setting | Value |
| --- | --- |
| Output directory | \`$OUT_DIR\` |
| C iterations | \`$C_ITERATIONS\` |
| RPC runs | \`$RPC_RUNS\` |
| Go benchtime | \`$GO_BENCHTIME\` |
| Go count | \`$GO_COUNT\` |
| Rust sample size | \`$RUST_SAMPLE_SIZE\` |
| Rust warm-up time | \`$RUST_WARM_UP_TIME\` s |
| Rust measurement time | \`$RUST_MEASUREMENT_TIME\` s |
| CMake build type | \`$CMAKE_BUILD_TYPE\` |
| Go native-CGO MsQuic included | \`$RUN_GO_NATIVE_MSQUIC\` |

## Environment

| Item | Value |
| --- | --- |
| Kernel | \`$(uname -srmo 2>/dev/null || true)\` |
| CPU | \`$(awk -F: '/model name/ {gsub(/^[ \t]+/, "", $2); print $2; exit}' /proc/cpuinfo 2>/dev/null || true)\` |
| CPU governor | \`$(cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor 2>/dev/null || true)\` |
| Go | \`$(go version 2>/dev/null || true)\` |
| Rust | \`$(rustc --version 2>/dev/null || true)\` |
| Cargo | \`$(cargo --version 2>/dev/null || true)\` |
| CMake | \`$(cmake --version 2>/dev/null | head -n 1 || true)\` |

## Results
EOF

        awk -F, '
        function compare_throughput(i1, v1, i2, v2) {
            if (throughput[v1] > throughput[v2]) {
                return -1
            }
            if (throughput[v1] < throughput[v2]) {
                return 1
            }
            return row_order[v1] < row_order[v2] ? -1 : 1
        }
        function append(values, value) {
            return values == "" ? value : values " " value
        }
        function print_table_header(shape) {
            printf "\n### `%s`\n\n", shape
            print "| Language | Implementation | Measurements | Median latency us/op | Latency min..max us/op | Median throughput ops/s | Iterations/Samples per measurement | Source |"
            print "| --- | --- | ---: | ---: | ---: | ---: | ---: | --- |"
        }
        NR > 1 {
            id = NR - 1
            row_order[id] = id
            language[id] = $1
            shape[id] = $2
            implementation[id] = $3
            measurements[id] = $4
            latency[id] = $5
            latency_min[id] = $6
            latency_max[id] = $7
            throughput[id] = $8 + 0
            iteration_count[id] = $9
            bytes[id] = $11
            allocs[id] = $12
            source[id] = $13

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
                asort(rows, sorted_rows, "compare_throughput")
                print_table_header(current_shape)
                for (row_index = 1; row_index <= row_count; row_index++) {
                    id = sorted_rows[row_index]
                    printf "| `%s` | `%s` | %s | %.3f | %.3f..%.3f | %.0f | %s | `%s` |\n", language[id], implementation[id], measurements[id], latency[id], latency_min[id], latency_max[id], throughput[id], iteration_count[id], source[id]
                }
            }
        }' "$CSV"

        cat <<EOF

## Notes

These rows use the same four RPC shapes: unary, server streaming with 16 response messages, client streaming with 16 request messages, and bidirectional streaming with 16 request/response messages.

Reported latency is the median across measurements. The min..max column shows the observed latency range across repeated measurements. Rust measurements also include Criterion sampling within each repeated command.

Result tables are split by RPC shape and sorted by median throughput descending.

Compare rows with the same shape first. Transport implementations differ: C uses native MsQuic and WebTransport, Go uses quic-go plus native-CGO MsQuic by default, and Rust uses Quinn. gRPC rows are included as language-local baselines, not identical transports.

Raw command output is saved under \`$RAW_DIR\`. Per-measurement normalized rows are saved in \`$SAMPLES_CSV\`. The exact commands are saved in \`$COMMAND_LOG\`.
EOF
    } >"$MARKDOWN"
}

cd "$ROOT"

if [[ "$RUN_C" == "1" ]]; then
    run_and_capture c-configure cmake -S trevrpc-c -B "$CMAKE_BUILD_DIR" -DTREVRPC_BUILD_TESTS=OFF -DTREVRPC_BUILD_BENCHMARKS=ON -DCMAKE_BUILD_TYPE="$CMAKE_BUILD_TYPE"
    run_and_capture c-build cmake --build "$CMAKE_BUILD_DIR" --target trevrpc_rpc_comparison_bench
    for ((run = 1; run <= RPC_RUNS; run++)); do
        run_and_capture "c-rpc-comparison-run-$run" "$CMAKE_BUILD_DIR/trevrpc_rpc_comparison_bench" "$C_ITERATIONS"
        append_c_csv "$RAW_DIR/c-rpc-comparison-run-$run.txt" "$run"
    done
fi

if [[ "$RUN_GO" == "1" ]]; then
    for ((run = 1; run <= RPC_RUNS; run++)); do
        run_and_capture "go-rpc-comparison-run-$run" go test -C trevrpc-go -run '^$' -bench '^BenchmarkRPCComparison$' -benchmem -count="$GO_COUNT" -benchtime="$GO_BENCHTIME"
        append_go_csv "$RAW_DIR/go-rpc-comparison-run-$run.txt" "$run"
    done
fi

if [[ "$RUN_GO_NATIVE_MSQUIC" == "1" ]]; then
    for ((run = 1; run <= RPC_RUNS; run++)); do
        run_and_capture "go-native-msquic-rpc-comparison-run-$run" go test -C trevrpc-go -tags trevrpc_msquic_native -run '^$' -bench '^BenchmarkRPCComparisonNativeMsQuic$' -benchmem -count="$GO_COUNT" -benchtime="$GO_BENCHTIME"
        append_go_csv "$RAW_DIR/go-native-msquic-rpc-comparison-run-$run.txt" "$run"
    done
fi

if [[ "$RUN_RUST" == "1" ]]; then
    for ((run = 1; run <= RPC_RUNS; run++)); do
        run_and_capture "rust-rpc-comparison-run-$run" cargo bench --manifest-path trevrpc-rust/Cargo.toml --bench rpc_comparison -- --sample-size "$RUST_SAMPLE_SIZE" --warm-up-time "$RUST_WARM_UP_TIME" --measurement-time "$RUST_MEASUREMENT_TIME"
        append_rust_csv "$RAW_DIR/rust-rpc-comparison-run-$run.txt" "$run"
    done
fi

aggregate_samples_csv
write_markdown_report

printf '\nWrote normalized CSV: %s\n' "$CSV"
printf 'Wrote per-measurement CSV: %s\n' "$SAMPLES_CSV"
printf 'Wrote Markdown report: %s\n' "$MARKDOWN"
printf 'Wrote raw outputs: %s\n' "$RAW_DIR"
