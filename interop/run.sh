#!/usr/bin/env bash
# Run every interop client against every interop server over every transport;
# see README.md.
#
#   run.sh [--out DIR] --server NAME=CMD... [--client NAME=CMD...] [--browser NAME...]
#          [--browser-lanes webtransport,websocket,fallback]
#
# Servers and native clients are interop executables taking the `server` and
# `client` subcommands. Browsers run through `trevrpc-interop-browser`, by
# default over every lane: WebTransport, QMux over WebSocket, and the fallback
# from a blocked UDP port to WebSocket. Safari cannot ignore certificate errors,
# so with `--browser safari` the generated certificate is trusted in the macOS
# System keychain for the duration of the run, through non-interactive sudo.
set -euo pipefail

servers=()
clients=()
browsers=()
browser_lanes="webtransport,websocket,fallback"
out=""
while (($#)); do
  case "$1" in
    --server) servers+=("$2") ;;
    --client) clients+=("$2") ;;
    --browser) browsers+=("$2") ;;
    --browser-lanes) browser_lanes="$2" ;;
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
trusted_cert=""
cleanup() {
  stop_server
  untrust_cert
  rm -rf "$work"
}
trap cleanup EXIT

cert=$work/cert.pem
key=$work/key.pem
openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 -nodes \
  -days 10 -subj /CN=localhost \
  -addext "subjectAltName=DNS:localhost,IP:127.0.0.1" \
  -addext "basicConstraints=critical,CA:FALSE" \
  -addext "extendedKeyUsage=serverAuth" \
  -keyout "$key" -out "$cert" 2>/dev/null
cert_hash=$(openssl x509 -in "$cert" -outform der | sha256sum | cut -d' ' -f1)

system_keychain=/Library/Keychains/System.keychain
# Keychain changes run without prompting and with a deadline, so a missing
# sudo password or an authorization dialog fails the run instead of hanging it.
keychain() {
  timeout --kill-after=5s 30s sudo -n security "$@"
}

trust_cert() {
  trusted_cert=$(openssl x509 -in "$cert" -noout -fingerprint -sha1 | cut -d= -f2 | tr -d :)
  keychain add-trusted-cert -d -r trustRoot -p ssl -k "$system_keychain" "$cert"
}

untrust_cert() {
  [[ -n $trusted_cert ]] || return 0
  keychain remove-trusted-cert -d "$cert" || echo "could not remove the certificate's trust settings" >&2
  keychain delete-certificate -Z "$trusted_cert" "$system_keychain" >/dev/null ||
    echo "could not delete certificate $trusted_cert from $system_keychain" >&2
  trusted_cert=""
}

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

# A UDP port that nothing answers, for cells that exercise the TCP fallback. It
# is below the ephemeral range, so it never collides with a server's own port,
# and is not on the browsers' list of unsafe ports.
unreachable_udp=24999

for browser in "${browsers[@]}"; do
  if [[ $browser == safari ]]; then
    trust_cert
    break
  fi
done

for server in "${servers[@]}"; do
  server_name=${server%%=*}
  start_server "$server_name" "${server#*=}"
  ports=$out/server-$server_name.ports
  native=$(jq -r .native "$ports")
  webtransport=$(jq -r .webtransport "$ports")
  qmux=$(jq -r .qmux "$ports")

  for client in "${clients[@]}"; do
    client_name=${client%%=*}
    # Each native client runs every scenario over QUIC, over QMux over TLS,
    # and over QUIC with a blocked UDP port, where it must fall back to TCP.
    # shellcheck disable=SC2086 # client commands may carry arguments
    run_cell "$client_name-to-$server_name" ${client#*=} client \
      --addr "127.0.0.1:$native" --ca "$cert" --server-name localhost \
      --transport quic --expect-transport quic
    # shellcheck disable=SC2086
    run_cell "$client_name-qmux-to-$server_name" ${client#*=} client \
      --addr "127.0.0.1:$native" --tcp-addr "127.0.0.1:$qmux" --ca "$cert" \
      --server-name localhost --transport tcp --expect-transport qmux
    # shellcheck disable=SC2086
    run_cell "$client_name-fallback-to-$server_name" ${client#*=} client \
      --addr "127.0.0.1:$unreachable_udp" --tcp-addr "127.0.0.1:$qmux" --ca "$cert" \
      --server-name localhost --transport auto --expect-transport qmux
  done
  websocket=$(jq -r .websocket "$ports")
  for browser in "${browsers[@]}"; do
    # Browsers run every scenario over WebTransport, over QMux over WebSocket,
    # and with a blocked UDP port, where they must fall back to WebSocket.
    if [[ ,$browser_lanes, == *,webtransport,* ]]; then
      run_cell "$browser-to-$server_name" trevrpc-interop-browser \
        --browser "$browser" \
        --url "https://127.0.0.1:$webtransport/trevrpc" \
        --cert-hash "$cert_hash"
    fi
    if [[ ,$browser_lanes, == *,websocket,* ]]; then
      run_cell "$browser-wss-to-$server_name" trevrpc-interop-browser \
        --browser "$browser" \
        --url "https://127.0.0.1:$webtransport/trevrpc" \
        --ws-url "wss://127.0.0.1:$websocket/trevrpc" \
        --cert-hash "$cert_hash" --transport websocket --expect-transport websocket
    fi
    if [[ ,$browser_lanes, == *,fallback,* ]]; then
      run_cell "$browser-fallback-to-$server_name" trevrpc-interop-browser \
        --browser "$browser" \
        --url "https://127.0.0.1:$unreachable_udp/trevrpc" \
        --ws-url "wss://127.0.0.1:$websocket/trevrpc" \
        --cert-hash "$cert_hash" --transport auto --expect-transport websocket
    fi
  done

  stop_server
done

if ((${#failures[@]})); then
  printf 'failed: %s\n' "${failures[@]}" >&2
  exit 1
fi
echo "all interop cells passed"
