#!/usr/bin/env bash

set -uo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
BINARY=${BINARY:-$ROOT/target/c-buffering/trevrpc_buffering_memory_bench}
OUT_DIR=${OUT_DIR:-$ROOT/target/c-buffering-matrix}
RUNS=${RUNS:-3}
MATRIX=${MATRIX:-final}
SAMPLE_TIMEOUT_SECONDS=${SAMPLE_TIMEOUT_SECONDS:-120}
SOURCE=${SOURCE:-$ROOT/trevrpc-c/bench/buffering_memory_bench.c}

if [[ ! -x "$BINARY" ]]; then
    printf 'buffering harness is not executable: %s\n' "$BINARY" >&2
    exit 2
fi
if [[ ! "$RUNS" =~ ^[1-9][0-9]*$ ]]; then
    printf 'RUNS must be a positive integer, got %q\n' "$RUNS" >&2
    exit 2
fi

case "$MATRIX" in
quick)
    SPECS=(
        'slow-reader|4|4096|16384|262144|65536|1'
        'stalled-handler|4|16384|4096|262144|0|5'
        'reset|4|16384|4096|131072|0|1'
        'close|4|16384|4096|131072|32768|1'
        'overload|8|16384|4096|131072|0|5'
        'body-limit|4|16384|4096|131072|0|1'
    )
    ;;
final | soak)
    SPECS=(
        'slow-reader|8|4096|65536|1048576|262144|20'
        'stalled-handler|12|65536|4096|2097152|0|50'
        'reset|12|131072|4096|1048576|0|20'
        'close|10|32768|8192|1048576|131072|20'
        'overload|16|262144|4096|1048576|0|50'
        'body-limit|8|65536|16384|1048576|0|20'
    )
    ;;
*)
    printf 'MATRIX must be quick, final, or soak, got %q\n' "$MATRIX" >&2
    exit 2
    ;;
esac

PROFILES=(safe receive-1m throughput-1m)
RAW_DIR=$OUT_DIR/raw
WARMUP_DIR=$OUT_DIR/warmup
SAMPLES=$OUT_DIR/samples.csv
MANIFEST=$OUT_DIR/manifest.csv
FAILURES=$OUT_DIR/failures.csv
COMMANDS=$OUT_DIR/commands.txt
PROVENANCE=$OUT_DIR/provenance.env

mkdir -p "$RAW_DIR" "$WARMUP_DIR"
: >"$COMMANDS"
printf 'profile,scenario,run,status,raw_csv,stderr,source_sha256,binary_sha256\n' >"$MANIFEST"
printf 'profile,scenario,run,status,raw_csv,stderr\n' >"$FAILURES"
rm -f "$SAMPLES"

SOURCE_SHA256=$(sha256sum "$SOURCE" | awk '{print $1}')
BINARY_SHA256=$(sha256sum "$BINARY" | awk '{print $1}')
GENERATED_AT=$(date -u +'%Y-%m-%dT%H:%M:%SZ')
GOVERNORS=$(for governor in /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor; do
    [[ -r "$governor" ]] && read -r value <"$governor" && printf '%s\n' "$value"
done | sort -u | paste -sd+ -)
{
    printf 'generated_at=%q\n' "$GENERATED_AT"
    printf 'hostname=%q\n' "$(hostname)"
    printf 'nproc=%q\n' "$(nproc)"
    printf 'uptime=%q\n' "$(uptime)"
    printf 'governors=%q\n' "$GOVERNORS"
    printf 'matrix=%q\n' "$MATRIX"
    printf 'runs=%q\n' "$RUNS"
    printf 'sample_timeout_seconds=%q\n' "$SAMPLE_TIMEOUT_SECONDS"
    printf 'source=%q\n' "$SOURCE"
    printf 'source_sha256=%q\n' "$SOURCE_SHA256"
    printf 'binary=%q\n' "$BINARY"
    printf 'binary_sha256=%q\n' "$BINARY_SHA256"
    printf 'git_head=%q\n' "$(git -C "$ROOT" rev-parse HEAD)"
    printf 'git_status_sha256=%q\n' "$(git -C "$ROOT" status --porcelain=v1 | sha256sum | awk '{print $1}')"
} >"$PROVENANCE"

log_command() {
    {
        printf '$ '
        printf '%q ' "$@"
        printf '\n'
    } >>"$COMMANDS"
}

for profile in "${PROFILES[@]}"; do
    warmup_csv=$WARMUP_DIR/$profile.csv
    warmup_stderr=$WARMUP_DIR/$profile.stderr
    command=(timeout --kill-after=5s 30s "$BINARY" "$profile" slow-reader 2 4096 8192 65536 16384 1)
    log_command "${command[@]}"
    if ! "${command[@]}" >"$warmup_csv" 2>"$warmup_stderr"; then
        printf 'warmup failed for %s\n' "$profile" >&2
        exit 1
    fi
done

failures=0
for run in $(seq 1 "$RUNS"); do
    for profile in "${PROFILES[@]}"; do
        for spec in "${SPECS[@]}"; do
            IFS='|' read -r scenario concurrency request_frame response_frame cumulative reader_progress hold_ms <<<"$spec"
            sample_id=$profile-$scenario-run$run
            raw_csv=$RAW_DIR/$sample_id.csv
            stderr_file=$RAW_DIR/$sample_id.stderr
            command=(timeout --kill-after=5s "${SAMPLE_TIMEOUT_SECONDS}s" "$BINARY" "$profile" "$scenario" \
                "$concurrency" "$request_frame" "$response_frame" "$cumulative" "$reader_progress" "$hold_ms")
            log_command "${command[@]}"
            "${command[@]}" >"$raw_csv" 2>"$stderr_file"
            status=$?
            if [[ ! -s "$SAMPLES" && -s "$raw_csv" ]]; then
                printf 'run,' >"$SAMPLES"
                awk 'NR == 1 { print; exit }' "$raw_csv" >>"$SAMPLES"
            fi
            if [[ -s "$raw_csv" ]]; then
                printf '%s,' "$run" >>"$SAMPLES"
                awk 'NR == 2 { print; exit }' "$raw_csv" >>"$SAMPLES"
            fi
            printf '%s,%s,%s,%s,%s,%s,%s,%s\n' "$profile" "$scenario" "$run" "$status" \
                "$raw_csv" "$stderr_file" "$SOURCE_SHA256" "$BINARY_SHA256" >>"$MANIFEST"
            if [[ "$status" != 0 ]]; then
                printf '%s,%s,%s,%s,%s,%s\n' \
                    "$profile" "$scenario" "$run" "$status" "$raw_csv" "$stderr_file" >>"$FAILURES"
                failures=$((failures + 1))
            fi
        done
    done
done

FINAL_SOURCE_SHA256=$(sha256sum "$SOURCE" | awk '{print $1}')
FINAL_BINARY_SHA256=$(sha256sum "$BINARY" | awk '{print $1}')
if [[ "$FINAL_SOURCE_SHA256" != "$SOURCE_SHA256" || "$FINAL_BINARY_SHA256" != "$BINARY_SHA256" ]]; then
    printf 'source or binary changed during matrix\n' >&2
    exit 1
fi

printf 'samples=%s\nmanifest=%s\nfailures=%s\nprovenance=%s\n' "$SAMPLES" "$MANIFEST" "$FAILURES" "$PROVENANCE"
exit "$failures"
