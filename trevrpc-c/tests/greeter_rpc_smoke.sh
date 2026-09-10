#!/bin/sh

set -eu

case $0 in
*/*) script_directory=${0%/*} ;;
*) script_directory=. ;;
esac
# shellcheck disable=SC1091 # Resolved relative to the invoked script.
. "$script_directory/benchmark_peer_smoke_common.sh"

server=$1
client=$2
certificate=$3
private_key=$4
transport=$5
directory=$(mktemp -d "${TMPDIR:-/tmp}/trevrpc-c-greeter-${transport}.XXXXXX")
server_pid=
server_wait_status=
client_pid=
client_wait_status=

cleanup() {
    status=$?
    trap - EXIT HUP INT TERM
    diagnostics=0
    [ "$status" -eq 0 ] || diagnostics=1
    trevrpc_smoke_cleanup_process client "$client_pid" "$client_wait_status" "$diagnostics"
    client_pid=
    trevrpc_smoke_cleanup_process server "$server_pid" "$server_wait_status" "$diagnostics"
    server_pid=
    if [ "$status" -eq 0 ]; then
        rm -rf "$directory"
    else
        trevrpc_smoke_dump_logs "$directory"
        printf 'greeter smoke failure artifacts: %s\n' "$directory" >&2
    fi
    exit "$status"
}
trap cleanup EXIT HUP INT TERM

: >"$directory/server.out"
: >"$directory/server.err"
"$server" 127.0.0.1 0 "$certificate" "$private_key" "$transport" \
    >"$directory/server.out" 2>"$directory/server.err" &
server_pid=$!

attempt=0
while [ ! -s "$directory/server.out" ] && [ "$attempt" -lt 300 ]; do
    if ! kill -0 "$server_pid" 2>/dev/null; then
        if wait "$server_pid"; then
            server_wait_status=0
        else
            server_wait_status=$?
        fi
        server_pid=
        printf 'server exited before readiness\n' >&2
        exit 1
    fi
    attempt=$((attempt + 1))
    sleep 0.1
done
if [ ! -s "$directory/server.out" ]; then
    printf 'server readiness timed out\n' >&2
    exit 1
fi
IFS= read -r ready <"$directory/server.out"
case "$ready" in
"serving $transport on 127.0.0.1:"*) ;;
*)
    printf 'unexpected server readiness: %s\n' "$ready" >&2
    exit 1
    ;;
esac
port=${ready##*:}
: >"$directory/client.out"
: >"$directory/client.err"
"$client" 127.0.0.1 "$port" TrevRPC "$transport" \
    >"$directory/client.out" 2>"$directory/client.err" &
client_pid=$!
if wait "$client_pid"; then
    client_wait_status=0
else
    client_wait_status=$?
    client_pid=
    exit "$client_wait_status"
fi
client_pid=

grep -Fq 'unary: Hello, TrevRPC' "$directory/client.out"
grep -Fq 'server streaming:' "$directory/client.out"
grep -Fq 'client streaming:' "$directory/client.out"
grep -Fq 'bidi streaming:' "$directory/client.out"
test ! -s "$directory/client.err"

kill -TERM "$server_pid"
if wait "$server_pid"; then
    server_wait_status=0
else
    server_wait_status=$?
    server_pid=
    exit "$server_wait_status"
fi
server_pid=
test ! -s "$directory/server.err"
