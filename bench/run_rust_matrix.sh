#!/usr/bin/env bash

set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
MATRIX_ROOT="$ROOT/bench/rust-matrix"
OUT_DIR=${OUT_DIR:-target/rust-rpc-matrix}
case "$OUT_DIR" in
/*) OUT_DIR_ABS=$OUT_DIR ;;
*) OUT_DIR_ABS="$ROOT/$OUT_DIR" ;;
esac
RAW_DIR="$OUT_DIR_ABS/raw"
SAMPLES="$OUT_DIR_ABS/samples.jsonl"
FAILURES="$OUT_DIR_ABS/failures.jsonl"
AGGREGATE="$OUT_DIR_ABS/aggregate.csv"
REPORT="$OUT_DIR_ABS/report.md"
MANIFEST="$OUT_DIR_ABS/manifest.json"
COMMANDS="$OUT_DIR_ABS/commands.txt"
CERTIFICATE="$OUT_DIR_ABS/certificate.pem"
PRIVATE_KEY="$OUT_DIR_ABS/private-key.pem"
CA_CERTIFICATE="$OUT_DIR_ABS/ca-certificate.pem"
CA_PRIVATE_KEY="$OUT_DIR_ABS/ca-private-key.pem"

RUNS=${RUNS:-5}
CONCURRENCIES=${CONCURRENCIES:-1,8,32,64}
WARMUP_MS=${WARMUP_MS:-5000}
MEASUREMENT_MS=${MEASUREMENT_MS:-20000}
SAMPLE_TIMEOUT_SECONDS=${SAMPLE_TIMEOUT_SECONDS:-90}
SERVER_STARTUP_TIMEOUT_SECONDS=${SERVER_STARTUP_TIMEOUT_SECONDS:-15}
SKIP_BUILD=${SKIP_BUILD:-0}
PEER="$MATRIX_ROOT/target/release/trevrpc-rust-matrix-peer"
REPORTER="$MATRIX_ROOT/target/release/trevrpc-rust-matrix-report"

usage() {
    cat <<'EOF'
Usage: bench/run_rust_matrix.sh
       bench/run_rust_matrix.sh --report-only
       bench/run_rust_matrix.sh --smoke

Runs the controlled Rust unary RPC matrix with fresh client/server process pairs.

Environment knobs:
  OUT_DIR                       Output directory. Default: target/rust-rpc-matrix
  RUNS                          Paired repetitions per cell. Default: 5
  CONCURRENCIES                 Comma-separated closed-loop lanes. Default: 1,8,32,64
  WARMUP_MS                     Untimed warmup duration. Default: 5000
  MEASUREMENT_MS                Fixed admission duration. Default: 20000
  SAMPLE_TIMEOUT_SECONDS        Client/server sample guard. Default: 90
  SERVER_STARTUP_TIMEOUT_SECONDS Server readiness guard. Default: 15
  SKIP_BUILD                    Require prebuilt release binaries when 1. Default: 0

The primary matrix compares complete Rust RPC stacks, not QUIC against HTTP/2 in isolation.
EOF
}

MODE=run
case "${1:-}" in
--help | -h)
    usage
    exit 0
    ;;
--report-only) MODE=report ;;
--smoke)
    RUNS=1
    CONCURRENCIES=1
    WARMUP_MS=50
    MEASUREMENT_MS=100
    SAMPLE_TIMEOUT_SECONDS=20
    ;;
"") ;;
*)
    usage >&2
    exit 2
    ;;
esac

require_positive() {
    local name=$1
    local value=$2
    if [[ ! "$value" =~ ^[1-9][0-9]*$ ]]; then
        printf '%s must be a positive integer, got %q\n' "$name" "$value" >&2
        exit 2
    fi
}

require_nonnegative() {
    local name=$1
    local value=$2
    if [[ ! "$value" =~ ^[0-9]+$ ]]; then
        printf '%s must be a non-negative integer, got %q\n' "$name" "$value" >&2
        exit 2
    fi
}

require_positive RUNS "$RUNS"
require_positive MEASUREMENT_MS "$MEASUREMENT_MS"
require_positive SAMPLE_TIMEOUT_SECONDS "$SAMPLE_TIMEOUT_SECONDS"
require_positive SERVER_STARTUP_TIMEOUT_SECONDS "$SERVER_STARTUP_TIMEOUT_SECONDS"
require_nonnegative WARMUP_MS "$WARMUP_MS"
if [[ "$SKIP_BUILD" != 0 && "$SKIP_BUILD" != 1 ]]; then
    printf 'SKIP_BUILD must be 0 or 1\n' >&2
    exit 2
fi

IFS=',' read -r -a CONCURRENCY_VALUES <<<"$CONCURRENCIES"
if ((${#CONCURRENCY_VALUES[@]} == 0)); then
    printf 'CONCURRENCIES must not be empty\n' >&2
    exit 2
fi
for concurrency in "${CONCURRENCY_VALUES[@]}"; do
    require_positive CONCURRENCIES "$concurrency"
done

quote_command() {
    printf '%q ' "$@"
}

log_command() {
    {
        printf '$ '
        quote_command "$@"
        printf '\n'
    } >>"$COMMANDS"
}

run_logged() {
    local name=$1
    shift
    log_command "$@"
    "$@" >"$RAW_DIR/$name.stdout.txt" 2>"$RAW_DIR/$name.stderr.txt"
}

SERVER_PID=""
CONTROL_ROOT=""
stop_server() {
    if [[ -n "$SERVER_PID" ]] && kill -0 "$SERVER_PID" 2>/dev/null; then
        kill -TERM "$SERVER_PID" 2>/dev/null || true
        local deadline=$((SECONDS + 5))
        while kill -0 "$SERVER_PID" 2>/dev/null && ((SECONDS < deadline)); do
            sleep 0.05
        done
        if kill -0 "$SERVER_PID" 2>/dev/null; then
            kill -KILL "$SERVER_PID" 2>/dev/null || true
        fi
        wait "$SERVER_PID" 2>/dev/null || true
    fi
    SERVER_PID=""
}

cleanup() {
    stop_server
    if [[ -n "$CONTROL_ROOT" ]]; then
        rm -rf "$CONTROL_ROOT"
    fi
}
trap cleanup EXIT

wait_for_ready() {
    local ready_file=$1
    local deadline=$((SECONDS + SERVER_STARTUP_TIMEOUT_SECONDS))
    while ((SECONDS < deadline)); do
        if [[ -s "$ready_file" ]]; then
            return 0
        fi
        if [[ -n "$SERVER_PID" ]] && ! kill -0 "$SERVER_PID" 2>/dev/null; then
            printf 'server exited before writing %s\n' "$ready_file" >&2
            return 1
        fi
        sleep 0.05
    done
    printf 'server readiness timed out at %s\n' "$ready_file" >&2
    return 1
}

SERVER_WAIT_STATUS=0
wait_for_server_shutdown() {
    local pid=$1
    (
        sleep "$SAMPLE_TIMEOUT_SECONDS"
        kill -TERM "$pid" 2>/dev/null || true
        sleep 5
        kill -KILL "$pid" 2>/dev/null || true
    ) &
    local watchdog_pid=$!
    set +e
    wait "$pid"
    SERVER_WAIT_STATUS=$?
    set -e
    kill "$watchdog_pid" 2>/dev/null || true
    wait "$watchdog_pid" 2>/dev/null || true
}

record_failure() {
    local run_id=$1
    local stack=$2
    local concurrency=$3
    local repetition=$4
    local status=$5
    jq -cn \
        --arg run_id "$run_id" \
        --arg stack "$stack" \
        --argjson concurrency "$concurrency" \
        --argjson repetition "$repetition" \
        --argjson status "$status" \
        '{schema_version:1,event:"failure",run_id:$run_id,stack:$stack,concurrency:$concurrency,repetition:$repetition,status:$status}' \
        >>"$FAILURES"
}

run_cell() {
    local stack=$1
    local concurrency=$2
    local repetition=$3
    local run_id="${stack}-c${concurrency}-r${repetition}"
    local cell_dir="$RAW_DIR/$run_id"
    local control="$CONTROL_ROOT/$run_id.sock"
    local ready="$cell_dir/ready.json"
    local server_stdout="$cell_dir/server.stdout.jsonl"
    local server_stderr="$cell_dir/server.stderr.txt"
    local client_stdout="$cell_dir/client.stdout.jsonl"
    local client_stderr="$cell_dir/client.stderr.txt"
    mkdir -p "$cell_dir"

    local config_hash
    config_hash=$("$PEER" hash-config \
        --stack "$stack" \
        --concurrency "$concurrency" \
        --warmup-ms "$WARMUP_MS" \
        --measurement-ms "$MEASUREMENT_MS" \
        --certificate-sha256 "$CERTIFICATE_SHA256")

    local server_command=(
        "$PEER" server
        --stack "$stack"
        --listen 127.0.0.1:0
        --control "$control"
        --ready-file "$ready"
        --cert "$CERTIFICATE"
        --key "$PRIVATE_KEY"
        --run-id "$run_id"
        --config-hash "$config_hash"
        --concurrency "$concurrency"
        --warmup-ms "$WARMUP_MS"
        --measurement-ms "$MEASUREMENT_MS"
        --certificate-sha256 "$CERTIFICATE_SHA256"
    )
    log_command "${server_command[@]}"
    "${server_command[@]}" >"$server_stdout" 2>"$server_stderr" &
    SERVER_PID=$!
    if ! wait_for_ready "$ready"; then
        record_failure "$run_id" "$stack" "$concurrency" "$repetition" 125
        stop_server
        return
    fi

    if ! jq -e \
        --arg run_id "$run_id" \
        --arg stack "$stack" \
        --arg hash "$config_hash" \
        '.schema_version == 1 and .event == "ready" and .run_id == $run_id and .stack == $stack and .config_hash == $hash' \
        "$ready" >/dev/null; then
        printf 'invalid ready event for %s\n' "$run_id" >&2
        record_failure "$run_id" "$stack" "$concurrency" "$repetition" 126
        stop_server
        return
    fi
    local address
    address=$(jq -r '.address' "$ready")

    local client_command=(
        env
        "TREVRPC_BENCH_SOURCE_COMMIT=$SOURCE_COMMIT"
        "TREVRPC_BENCH_ARTIFACT_SHA256=$PEER_SHA256"
        timeout --kill-after=5s "${SAMPLE_TIMEOUT_SECONDS}s"
        "$PEER" client
        --stack "$stack"
        --address "$address"
        --control "$control"
        --cert "$CA_CERTIFICATE"
        --identity-cert "$CERTIFICATE"
        --run-id "$run_id"
        --config-hash "$config_hash"
        --concurrency "$concurrency"
        --warmup-ms "$WARMUP_MS"
        --measurement-ms "$MEASUREMENT_MS"
        --repetition "$repetition"
        --certificate-sha256 "$CERTIFICATE_SHA256"
    )
    log_command "${client_command[@]}"
    set +e
    "${client_command[@]}" >"$client_stdout" 2>"$client_stderr"
    local status=$?
    set -e
    if [[ "$status" != 0 ]]; then
        record_failure "$run_id" "$stack" "$concurrency" "$repetition" "$status"
        stop_server
        return
    fi

    wait_for_server_shutdown "$SERVER_PID"
    status=$SERVER_WAIT_STATUS
    if [[ "$status" != 0 ]]; then
        SERVER_PID=""
        record_failure "$run_id" "$stack" "$concurrency" "$repetition" "$status"
        return
    fi
    SERVER_PID=""
    if [[ $(jq -c 'select(.schema_version == 1 and .event == "sample")' "$client_stdout" | wc -l) != 1 ]]; then
        printf 'client %s did not emit exactly one sample\n' "$run_id" >&2
        record_failure "$run_id" "$stack" "$concurrency" "$repetition" 127
        return
    fi
    jq -c 'select(.schema_version == 1 and .event == "sample")' "$client_stdout" >>"$SAMPLES"
}

cd "$ROOT"
mkdir -p "$RAW_DIR"

if [[ "$MODE" == report ]]; then
    if [[ ! -x "$REPORTER" ]]; then
        printf 'report-only requires %s\n' "$REPORTER" >&2
        exit 2
    fi
    "$REPORTER" "$SAMPLES" "$AGGREGATE" "$REPORT" "$RUNS" "$CONCURRENCIES"
    printf 'Wrote Rust matrix aggregate: %s\nWrote Rust matrix report: %s\n' "$AGGREGATE" "$REPORT"
    exit 0
fi

CONTROL_ROOT=$(mktemp -d /tmp/trevrpc-rust-matrix.XXXXXX)
: >"$COMMANDS"
: >"$SAMPLES"
: >"$FAILURES"
rm -rf "$RAW_DIR"
mkdir -p "$RAW_DIR"

if [[ "$SKIP_BUILD" == 0 ]]; then
    run_logged build cargo build --manifest-path "$MATRIX_ROOT/Cargo.toml" --release --locked
elif [[ ! -x "$PEER" || ! -x "$REPORTER" ]]; then
    printf 'SKIP_BUILD=1 requires release binaries under %s\n' "$MATRIX_ROOT/target/release" >&2
    exit 2
fi

ca_command=(
    openssl req -x509 -newkey rsa:2048 -nodes
    -keyout "$CA_PRIVATE_KEY" -out "$CA_CERTIFICATE" -days 1
    -subj '/CN=TrevRPC Benchmark CA'
    -addext basicConstraints=critical,CA:TRUE
    -addext keyUsage=critical,keyCertSign,cRLSign
)
run_logged ca-certificate "${ca_command[@]}"
csr_command=(
    openssl req -newkey rsa:2048 -nodes
    -keyout "$PRIVATE_KEY" -out "$OUT_DIR_ABS/server.csr"
    -subj /CN=localhost -addext subjectAltName=DNS:localhost,IP:127.0.0.1
)
run_logged server-csr "${csr_command[@]}"
sign_command=(
    openssl x509 -req -in "$OUT_DIR_ABS/server.csr"
    -CA "$CA_CERTIFICATE" -CAkey "$CA_PRIVATE_KEY" -CAcreateserial
    -out "$CERTIFICATE" -days 1 -copy_extensions copyall
)
run_logged certificate "${sign_command[@]}"

SOURCE_COMMIT=$(git rev-parse HEAD)
SOURCE_DIRTY=false
if [[ -n $(git status --short) ]]; then SOURCE_DIRTY=true; fi
PEER_SHA256=$(sha256sum "$PEER" | awk '{print $1}')
REPORTER_SHA256=$(sha256sum "$REPORTER" | awk '{print $1}')
CERTIFICATE_SHA256=$(sha256sum "$CERTIFICATE" | awk '{print $1}')
CA_CERTIFICATE_SHA256=$(sha256sum "$CA_CERTIFICATE" | awk '{print $1}')
GENERATED_AT=$(date -u +'%Y-%m-%dT%H:%M:%SZ')
jq -n \
    --arg generated_at "$GENERATED_AT" \
    --arg source_commit "$SOURCE_COMMIT" \
    --argjson source_dirty "$SOURCE_DIRTY" \
    --arg peer_sha256 "$PEER_SHA256" \
    --arg reporter_sha256 "$REPORTER_SHA256" \
    --arg certificate_sha256 "$CERTIFICATE_SHA256" \
    --arg ca_certificate_sha256 "$CA_CERTIFICATE_SHA256" \
    --arg kernel "$(uname -srmo)" \
    --arg cpu "$(awk -F: '/model name/ {gsub(/^[ \t]+/, "", $2); print $2; exit}' /proc/cpuinfo)" \
    --arg governor "$(cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor 2>/dev/null || true)" \
    --arg concurrencies "$CONCURRENCIES" \
    --argjson runs "$RUNS" \
    --argjson warmup_ms "$WARMUP_MS" \
    --argjson measurement_ms "$MEASUREMENT_MS" \
    '{schema_version:1,generated_at:$generated_at,source_commit:$source_commit,source_dirty:$source_dirty,peer_sha256:$peer_sha256,reporter_sha256:$reporter_sha256,certificate_sha256:$certificate_sha256,ca_certificate_sha256:$ca_certificate_sha256,kernel:$kernel,cpu:$cpu,cpu_governor:$governor,runs:$runs,concurrencies:$concurrencies,warmup_ms:$warmup_ms,measurement_ms:$measurement_ms,workload:"tiny",operation:"unary_closed_loop",application_encoding:"protobuf",network_profile:"loopback"}' \
    >"$MANIFEST"

for concurrency in "${CONCURRENCY_VALUES[@]}"; do
    for ((repetition = 1; repetition <= RUNS; repetition++)); do
        if ((repetition % 2 == 1)); then
            stacks=(trevrpc_quinn grpc_tonic_generated)
        else
            stacks=(grpc_tonic_generated trevrpc_quinn)
        fi
        for stack in "${stacks[@]}"; do
            run_cell "$stack" "$concurrency" "$repetition"
        done
    done
done

if [[ -s "$FAILURES" ]]; then
    printf 'Rust matrix recorded failures in %s\n' "$FAILURES" >&2
    exit 1
fi

"$REPORTER" "$SAMPLES" "$AGGREGATE" "$REPORT" "$RUNS" "$CONCURRENCIES"
printf 'Wrote Rust matrix samples: %s\n' "$SAMPLES"
printf 'Wrote Rust matrix aggregate: %s\n' "$AGGREGATE"
printf 'Wrote Rust matrix report: %s\n' "$REPORT"
printf 'Wrote Rust matrix manifest: %s\n' "$MANIFEST"
