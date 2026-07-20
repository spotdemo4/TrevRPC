#!/bin/sh

set -eu

peer=$1
certificate=$2
private_key=$3
directory=$(mktemp -d "${TMPDIR:-/tmp}/trevrpc-c-grpc-smoke.XXXXXX")
server_pid=

cleanup() {
    status=$?
    trap - EXIT HUP INT TERM
    exec 3>&- 4>&- || true
    if [ -n "$server_pid" ]; then
        kill "$server_pid" 2>/dev/null || true
        wait "$server_pid" 2>/dev/null || true
    fi
    if [ "$status" -eq 0 ]; then
        rm -rf "$directory"
    else
        printf 'gRPC smoke failure artifacts: %s\n' "$directory" >&2
    fi
    exit "$status"
}
trap cleanup EXIT HUP INT TERM

mkfifo "$directory/control" "$directory/events"
exec 3<>"$directory/control"
exec 4<>"$directory/events"

"$peer" server \
    --stack grpc_http2 \
    --listen 127.0.0.1:0 \
    --workers 8 \
    --cert "$certificate" \
    --key "$private_key" \
    <&3 >&4 2>"$directory/server.err" &
server_pid=$!

IFS= read -r ready <&4
case "$ready" in
*'"event":"ready"'*'"stack":"grpc_http2"'*) ;;
*)
    printf 'unexpected ready event: %s\n' "$ready" >&2
    exit 1
    ;;
esac
address=${ready#*\"address\":\"}
address=${address%%\"*}

for rpc in unary client_stream server_stream bidi; do
    printf 'START\n' | "$peer" client \
        --stack grpc_http2 \
        --address "$address" \
        --cert "$certificate" \
        --rpc "$rpc" \
        --concurrency 2 \
        --warmup-ms 0 \
        --measurement-ms 5 \
        --request-bytes 16 \
        --response-bytes 32 \
        --messages-per-stream 3 \
        >"$directory/client-$rpc.out" 2>"$directory/client-$rpc.err"
    found_sample=false
    while IFS= read -r event; do
        case "$event" in
        *'"event":"sample"'*'"failed":"0"'*) found_sample=true ;;
        esac
    done <"$directory/client-$rpc.out"
    if [ "$found_sample" != true ]; then
        printf 'missing successful sample for %s\n' "$rpc" >&2
        exit 1
    fi
    test ! -s "$directory/client-$rpc.err"
done

stress_pids=
for run in 1 2 3 4 5 6 7 8 9 10; do
    printf 'START\n' | "$peer" client \
        --stack grpc_http2 \
        --address "$address" \
        --cert "$certificate" \
        --rpc client_stream \
        --concurrency 4 \
        --warmup-ms 0 \
        --measurement-ms 250 \
        --request-bytes 256 \
        --response-bytes 256 \
        --messages-per-stream 128 \
        >"$directory/client-stress-$run.out" 2>"$directory/client-stress-$run.err" &
    stress_pids="$stress_pids $!"
done
for pid in $stress_pids; do
    wait "$pid"
done
for run in 1 2 3 4 5 6 7 8 9 10; do
    test ! -s "$directory/client-stress-$run.err"
    grep -q '"event":"sample".*"failed":"0"' "$directory/client-stress-$run.out"
done

printf 'SHUTDOWN\n' >&3
IFS= read -r stopped <&4
case "$stopped" in
*'"event":"stopped"'*'"stack":"grpc_http2"'*) ;;
*)
    printf 'unexpected stopped event: %s\n' "$stopped" >&2
    exit 1
    ;;
esac
wait "$server_pid"
server_pid=
test ! -s "$directory/server.err"
