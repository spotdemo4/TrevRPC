# shellcheck shell=bash
# shellcheck disable=SC2034,SC2154 # globals shared with the sourcing script
# Shared helpers for interop/run.sh and bench/run.sh: a throwaway certificate
# and an interop-style server process. Callers set `work` (a scratch directory)
# and `out` (where logs and port files go) before using them, and call
# stop_server from their EXIT trap.

server_pid=""
server_stdin=""

# generate_cert writes a self-signed ECDSA localhost certificate and key to
# $work and sets `cert` and `key`.
generate_cert() {
  cert=$work/cert.pem
  key=$work/key.pem
  openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 -nodes \
    -days 10 -subj /CN=localhost \
    -addext "subjectAltName=DNS:localhost,IP:127.0.0.1" \
    -addext "basicConstraints=critical,CA:FALSE" \
    -addext "extendedKeyUsage=serverAuth" \
    -keyout "$key" -out "$cert" 2>/dev/null
}

# start_server NAME COMMAND [ARG...] runs `COMMAND server --cert --key ARG...`
# and waits for the JSON port line it prints, which lands in
# $out/server-NAME.ports.
start_server() {
  local name=$1 command=$2 ports=$out/server-$1.ports
  shift 2
  mkfifo "$work/stdin-$name"
  # The server exits when its stdin reaches EOF, so hold the write end open.
  # shellcheck disable=SC2086 # server commands may carry arguments
  $command server --cert "$cert" --key "$key" "$@" \
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

# stop_server closes the running server's stdin and waits for it to exit,
# killing it after ten seconds.
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
