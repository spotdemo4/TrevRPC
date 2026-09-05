#!/bin/sh

set -eu

server=$1
client=$2
certificate=$3
private_key=$4
transport=$5
directory=$(mktemp -d "${TMPDIR:-/tmp}/trevrpc-c-greeter-${transport}.XXXXXX")
server_pid=

cleanup() {
    status=$?
    trap - EXIT HUP INT TERM
    if [ -n "$server_pid" ]; then
        kill -TERM "$server_pid" 2>/dev/null || true
        wait "$server_pid" 2>/dev/null || true
    fi
    if [ "$status" -eq 0 ]; then
        rm -rf "$directory"
    else
        printf 'greeter smoke failure artifacts: %s\n' "$directory" >&2
    fi
    exit "$status"
}
trap cleanup EXIT HUP INT TERM

: >"$directory/server.out"
"$server" 127.0.0.1 0 "$certificate" "$private_key" "$transport" \
    >"$directory/server.out" 2>"$directory/server.err" &
server_pid=$!

attempt=0
while [ ! -s "$directory/server.out" ] && [ "$attempt" -lt 300 ]; do
    if ! kill -0 "$server_pid" 2>/dev/null; then
        wait "$server_pid" || true
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
"$client" 127.0.0.1 "$port" TrevRPC "$transport" \
    >"$directory/client.out" 2>"$directory/client.err"

grep -Fq 'unary: Hello, TrevRPC' "$directory/client.out"
grep -Fq 'server streaming:' "$directory/client.out"
grep -Fq 'client streaming:' "$directory/client.out"
grep -Fq 'bidi streaming:' "$directory/client.out"
test ! -s "$directory/client.err"

kill -TERM "$server_pid"
wait "$server_pid"
server_pid=
test ! -s "$directory/server.err"
