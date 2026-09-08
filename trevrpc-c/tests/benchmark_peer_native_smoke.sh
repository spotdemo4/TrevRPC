#!/bin/sh

set -eu

case $0 in
*/*) script_directory=${0%/*} ;;
*) script_directory=. ;;
esac
# shellcheck disable=SC1091 # Resolved relative to the invoked script.
. "$script_directory/benchmark_peer_smoke_common.sh"

peer=$1
certificate=$2
private_key=$3
directory=$(mktemp -d "${TMPDIR:-/tmp}/trevrpc-c-benchmark-native.XXXXXX")
server_pid=
server_wait_status=
client_pid=
client_wait_status=
client_label=client

cleanup() {
    status=$?
    trap - EXIT HUP INT TERM
    diagnostics=0
    [ "$status" -eq 0 ] || diagnostics=1
    trevrpc_smoke_cleanup_process "$client_label" "$client_pid" "$client_wait_status" "$diagnostics"
    client_pid=
    exec 3>&- 4>&- || true
    trevrpc_smoke_cleanup_process server "$server_pid" "$server_wait_status" "$diagnostics"
    server_pid=
    if [ "$status" -eq 0 ]; then
        rm -rf "$directory"
    else
        trevrpc_smoke_dump_logs "$directory"
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
    client_label="$rpc client"
    client_wait_status=
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
    if wait "$client_pid"; then
        client_wait_status=0
    else
        client_wait_status=$?
        exit "$client_wait_status"
    fi
    client_pid=
    grep -q '"event":"sample"' "$client_out"
    grep -q '"failed":"0"' "$client_out"
    test ! -s "$client_err"
done

printf 'SHUTDOWN\n' >&3
if wait "$server_pid"; then
    server_wait_status=0
else
    server_wait_status=$?
    exit "$server_wait_status"
fi
server_pid=
grep -q '"event":"stopped"' "$directory/server.out"
test ! -s "$directory/server.err"
