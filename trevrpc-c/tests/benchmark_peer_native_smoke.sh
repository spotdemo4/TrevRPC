#!/bin/sh

set -eu

peer=$1
certificate=$2
private_key=$3
directory=$(mktemp -d "${TMPDIR:-/tmp}/trevrpc-c-benchmark-native.XXXXXX")
server_pid=
client_pid=

cleanup() {
    status=$?
    trap - EXIT HUP INT TERM
    if [ -n "$client_pid" ]; then
        kill "$client_pid" 2>/dev/null || true
        wait "$client_pid" 2>/dev/null || true
    fi
    exec 3>&- 4>&- || true
    if [ -n "$server_pid" ]; then
        kill "$server_pid" 2>/dev/null || true
        wait "$server_pid" 2>/dev/null || true
    fi
    if [ "$status" -eq 0 ]; then
        rm -rf "$directory"
    else
        printf 'native benchmark smoke failure artifacts: %s\n' "$directory" >&2
    fi
    exit "$status"
}
trap cleanup EXIT HUP INT TERM

mkfifo "$directory/server.control" "$directory/client.control"
exec 3<>"$directory/server.control"
exec 4<>"$directory/client.control"
: >"$directory/server.out"
: >"$directory/server.err"

"$peer" server \
    --stack trevrpc_native_quic \
    --listen 127.0.0.1:0 \
    --workers 8 \
    --cert "$certificate" \
    --key "$private_key" \
    <&3 >"$directory/server.out" 2>"$directory/server.err" &
server_pid=$!

attempt=0
ready=
while [ "$attempt" -lt 300 ]; do
    ready=$(grep -m1 '"event":"ready"' "$directory/server.out" || true)
    [ -n "$ready" ] && break
    kill -0 "$server_pid" 2>/dev/null || break
    attempt=$((attempt + 1))
    sleep 0.1
done
if [ -z "$ready" ]; then
    printf 'benchmark server did not become ready\n' >&2
    exit 1
fi
address=${ready#*\"address\":\"}
address=${address%%\"*}

for rpc in unary client_stream server_stream bidi; do
    client_out="$directory/client-$rpc.out"
    client_err="$directory/client-$rpc.err"
    : >"$client_out"
    : >"$client_err"
    "$peer" client \
        --stack trevrpc_native_quic \
        --address "$address" \
        --cert "$certificate" \
        --rpc "$rpc" \
        --concurrency 1 \
        --warmup-ms 0 \
        --measurement-ms 1 \
        --request-bytes 16 \
        --response-bytes 16 \
        --messages-per-stream 2 \
        <&4 >"$client_out" 2>"$client_err" &
    client_pid=$!

    attempt=0
    armed=
    while [ "$attempt" -lt 300 ]; do
        armed=$(grep -m1 '"event":"armed"' "$client_out" || true)
        [ -n "$armed" ] && break
        kill -0 "$client_pid" 2>/dev/null || break
        attempt=$((attempt + 1))
        sleep 0.1
    done
    if [ -z "$armed" ]; then
        printf '%s client did not arm\n' "$rpc" >&2
        exit 1
    fi
    printf 'START\n' >&4
    wait "$client_pid"
    client_pid=
    grep -q '"event":"sample"' "$client_out"
    grep -q '"failed":"0"' "$client_out"
    test ! -s "$client_err"
done

printf 'SHUTDOWN\n' >&3
wait "$server_pid"
server_pid=
grep -q '"event":"stopped"' "$directory/server.out"
test ! -s "$directory/server.err"
