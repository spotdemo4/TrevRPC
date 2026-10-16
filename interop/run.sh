#!/usr/bin/env bash
# Run every interop client against every interop server; see README.md.
#
#   run.sh [--out DIR] --server NAME=CMD... [--client NAME=CMD...] [--browser NAME...]
#
# Servers and native clients are interop executables taking the `server` and
# `client` subcommands. Browsers run through `trevrpc-interop-browser`.
set -euo pipefail

servers=()
clients=()
browsers=()
out=""
while (($#)); do
  case "$1" in
    --server) servers+=("$2") ;;
    --client) clients+=("$2") ;;
    --browser) browsers+=("$2") ;;
    --out) out="$2" ;;
    *)
      echo "unknown argument: $1" >&2
      exit 2
      ;;
  esac
  shift 2
done
if ((${#servers[@]} == 0 || ${#clients[@]} + ${#browsers[@]} == 0)); then
  echo "need at least one --server and one --client or --browser" >&2
  exit 2
fi

work=$(mktemp -d)
out=${out:-$work/out}
mkdir -p "$out"
server_pid=""
server_stdin=""
cleanup() {
  stop_server
  rm -rf "$work"
}
trap cleanup EXIT

cert=$work/cert.pem
key=$work/key.pem
openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 -nodes \
  -days 10 -subj /CN=localhost \
  -addext "subjectAltName=DNS:localhost,IP:127.0.0.1" \
  -addext "basicConstraints=critical,CA:FALSE" \
  -keyout "$key" -out "$cert" 2>/dev/null
cert_hash=$(openssl x509 -in "$cert" -outform der | sha256sum | cut -d' ' -f1)

start_server() {
  local name=$1 command=$2 ports=$out/server-$1.ports
  mkfifo "$work/stdin-$name"
  # The server exits when its stdin reaches EOF, so hold the write end open.
  $command server --cert "$cert" --key "$key" \
    <"$work/stdin-$name" >"$ports" 2>"$out/server-$name.log" &
  server_pid=$!
  exec {server_stdin}>"$work/stdin-$name"
  for _ in $(seq 300); do
    if [[ -s $ports ]] && jq -e . "$ports" >/dev/null 2>&1; then
      return 0
    fi
    if ! kill -0 "$server_pid" 2>/dev/null; then
      break
    fi
    sleep 0.1
  done
  echo "server $name did not report its ports; log:" >&2
  cat "$out/server-$name.log" >&2
  return 1
}

stop_server() {
  [[ -n $server_pid ]] || return 0
  [[ -z $server_stdin ]] || exec {server_stdin}>&-
  for _ in $(seq 100); do
    kill -0 "$server_pid" 2>/dev/null || break
    sleep 0.1
  done
  kill -9 "$server_pid" 2>/dev/null || true
  wait "$server_pid" 2>/dev/null || true
  server_pid=""
  server_stdin=""
}

failures=()
run_cell() {
  local cell=$1
  shift
  printf '== %s\n' "$cell"
  if timeout --kill-after=5s 120s "$@" >"$out/$cell.log" 2>&1; then
    cat "$out/$cell.log"
  else
    cat "$out/$cell.log"
    failures+=("$cell")
  fi
}

for server in "${servers[@]}"; do
  server_name=${server%%=*}
  start_server "$server_name" "${server#*=}"
  ports=$out/server-$server_name.ports
  native=$(jq -r .native "$ports")
  webtransport=$(jq -r .webtransport "$ports")

  for client in "${clients[@]}"; do
    client_name=${client%%=*}
    # shellcheck disable=SC2086 # client commands may carry arguments
    run_cell "$client_name-to-$server_name" ${client#*=} client \
      --addr "127.0.0.1:$native" --ca "$cert" --server-name localhost
  done
  for browser in "${browsers[@]}"; do
    run_cell "$browser-to-$server_name" trevrpc-interop-browser \
      --browser "$browser" \
      --url "https://127.0.0.1:$webtransport/trevrpc" \
      --cert-hash "$cert_hash"
  done

  stop_server
done

if ((${#failures[@]})); then
  printf 'failed: %s\n' "${failures[@]}" >&2
  exit 1
fi
echo "all interop cells passed"
