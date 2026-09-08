#!/bin/sh

set -eu
# shellcheck disable=SC3045 # Supported by the target shells; suppress crash artifacts.
ulimit -c 0

peer=$1
certificate=$2
private_key=$3
directory=$(mktemp -d "${TMPDIR:-/tmp}/trevrpc-c-webtransport-smoke.XXXXXX")
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
        printf 'WebTransport smoke failure artifacts: %s\n' "$directory" >&2
    fi
    exit "$status"
}
trap cleanup EXIT HUP INT TERM

mkfifo "$directory/server.control" "$directory/client.control"
exec 3<>"$directory/server.control"
exec 4<>"$directory/client.control"

for rpc in unary client_stream server_stream bidi; do
    client_out="$directory/client-$rpc.out"
    client_err="$directory/client-$rpc.err"
    server_out="$directory/server-$rpc.out"
    server_err="$directory/server-$rpc.err"
    : >"$client_out"
    : >"$client_err"
    : >"$server_out"
    : >"$server_err"

    "$peer" client \
        --stack trevrpc_webtransport \
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
    prepared=
    while [ "$attempt" -lt 300 ]; do
        prepared=$(grep -m1 '"event":"prepared"' "$client_out" || true)
        [ -n "$prepared" ] && break
        kill -0 "$client_pid" 2>/dev/null || break
        attempt=$((attempt + 1))
        sleep 0.1
    done
    case "$prepared" in
    *'"schema_version":5'*'"event":"prepared"'*'"peer":"c"'*'"origin":"'* ) ;;
    *)
        printf 'unexpected %s prepared event: %s\n' "$rpc" "$prepared" >&2
        exit 1
        ;;
    esac
    origin=${prepared#*\"origin\":\"}
    origin=${origin%%\"*}
    if [ -z "$origin" ]; then
        printf '%s client emitted an invalid prepared origin\n' "$rpc" >&2
        exit 1
    fi

    "$peer" server \
        --stack trevrpc_webtransport \
        --listen 127.0.0.1:0 \
        --workers 8 \
        --cert "$certificate" \
        --key "$private_key" \
        --webtransport-origin "$origin" \
        <&3 >"$server_out" 2>"$server_err" &
    server_pid=$!

    attempt=0
    ready=
    while [ "$attempt" -lt 300 ]; do
        ready=$(grep -m1 '"event":"ready"' "$server_out" || true)
        [ -n "$ready" ] && break
        kill -0 "$server_pid" 2>/dev/null || break
        attempt=$((attempt + 1))
        sleep 0.1
    done
    case "$ready" in
    *'"schema_version":5'*'"event":"ready"'*'"peer":"c"'*'"address":"'* ) ;;
    *)
        printf 'unexpected %s ready event: %s\n' "$rpc" "$ready" >&2
        exit 1
        ;;
    esac
    address=${ready#*\"address\":\"}
    address=${address%%\"*}
    if [ -z "$address" ]; then
        printf '%s server emitted an invalid ready address\n' "$rpc" >&2
        exit 1
    fi

    printf 'CONNECT %s\n' "$address" >&4
    attempt=0
    armed=
    while [ "$attempt" -lt 300 ]; do
        armed=$(grep -m1 '"event":"armed"' "$client_out" || true)
        [ -n "$armed" ] && break
        kill -0 "$client_pid" 2>/dev/null || break
        attempt=$((attempt + 1))
        sleep 0.1
    done
    case "$armed" in
    *'"schema_version":5'*'"event":"armed"'*'"peer":"c"'* ) ;;
    *)
        printf 'unexpected %s armed event: %s\n' "$rpc" "$armed" >&2
        exit 1
        ;;
    esac

    printf 'START\n' >&4
    wait "$client_pid"
    client_pid=
    sample=$(grep -m1 '"event":"sample"' "$client_out" || true)
    case "$sample" in
    *'"schema_version":5'*'"event":"sample"'*'"peer":"c"'*'"rpc_kind":"'*'"failed":"0"'* ) ;;
    *)
        printf 'unexpected %s sample event: %s\n' "$rpc" "$sample" >&2
        exit 1
        ;;
    esac
    grep -q "\"rpc_kind\":\"$rpc\"" "$client_out"
    test ! -s "$client_err"

    printf 'SHUTDOWN\n' >&3
    wait "$server_pid"
    server_pid=
    stopped=$(grep -m1 '"event":"stopped"' "$server_out" || true)
    case "$stopped" in
    *'"schema_version":5'*'"event":"stopped"'*'"peer":"c"'* ) ;;
    *)
        printf 'unexpected %s stopped event: %s\n' "$rpc" "$stopped" >&2
        exit 1
        ;;
    esac
    test ! -s "$server_err"
done
