#!/bin/sh

set -eu

peer=$1
certificate=$2
private_key=$3
directory=$(mktemp -d "${TMPDIR:-/tmp}/trevrpc-c-webtransport-smoke.XXXXXX")
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
        printf 'WebTransport smoke failure artifacts: %s\n' "$directory" >&2
    fi
    exit "$status"
}
trap cleanup EXIT HUP INT TERM

mkfifo "$directory/control" "$directory/events"
exec 3<>"$directory/control"
exec 4<>"$directory/events"

"$peer" server \
    --stack trevrpc_webtransport \
    --listen 127.0.0.1:0 \
    --workers 8 \
    --cert "$certificate" \
    --key "$private_key" \
    --webtransport-origin https://benchmark.invalid \
    <&3 >&4 2>"$directory/server.err" &
server_pid=$!

IFS= read -r ready <&4
case "$ready" in
*'"schema_version":5'*'"event":"ready"'*) ;;
*)
    printf 'unexpected ready event: %s\n' "$ready" >&2
    exit 1
    ;;
esac

printf 'SHUTDOWN\n' >&3
IFS= read -r stopped <&4
case "$stopped" in
*'"schema_version":5'*'"event":"stopped"'*) ;;
*)
    printf 'unexpected stopped event: %s\n' "$stopped" >&2
    exit 1
    ;;
esac
wait "$server_pid"
server_pid=
test ! -s "$directory/server.err"
