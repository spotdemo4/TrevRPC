#!/bin/sh

set -eu

peer=$1
certificate=$2
private_key=$3
probe=${4-}
directory=$(mktemp -d "${TMPDIR:-/tmp}/trevrpc-http3-lifecycle.XXXXXX")
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
    for artifact in server.err probe.out probe.err; do
      if [ -s "$directory/$artifact" ]; then
        printf '%s:\n' "$artifact" >&2
        while IFS= read -r line || [ -n "$line" ]; do
          printf '  %s\n' "$line" >&2
        done <"$directory/$artifact"
      fi
    done
    printf 'HTTP/3 lifecycle failure artifacts: %s\n' "$directory" >&2
  fi
  exit "$status"
}
trap cleanup EXIT HUP INT TERM

mkfifo "$directory/control" "$directory/events"
exec 3<>"$directory/control"
exec 4<>"$directory/events"

"$peer" server \
  --stack trevrpc_http3 \
  --listen 127.0.0.1:0 \
  --cert "$certificate" \
  --key "$private_key" \
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

if [ -n "$probe" ]; then
  address=${ready#*'"address":"'}
  address=${address%%'"'*}
  host=${address%:*}
  port=${address##*:}
  if "$probe" "$host" "$port" https://benchmark.invalid >"$directory/probe.out" 2>"$directory/probe.err"; then
    printf 'WebTransport probe unexpectedly succeeded against HTTP/3-only server\n' >&2
    exit 1
  fi
fi

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
if [ -s "$directory/server.err" ]; then
  printf 'HTTP/3 server wrote unexpected stderr output\n' >&2
  exit 1
fi
