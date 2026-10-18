#!/usr/bin/env bash
# Run every load-benchmark client against every server over every native
# transport, plus the grpc-go baseline; see README.md.
#
#   run.sh --server NAME=CMD... --client NAME=CMD... [--grpc CMD]
#          [--scenarios LIST] [--sizes LIST] [--metadata-sizes LIST] [--concurrency LIST]
#          [--duration SECONDS] [--warmup SECONDS] [--messages N] [--out DIR]
set -euo pipefail

# shellcheck source-path=SCRIPTDIR source=../interop/lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/../interop/lib.sh"

servers=()
clients=()
grpc=""
scenarios="unary,server-stream,client-stream,bidi"
sizes="64"
metadata_sizes="0"
concurrency="1,64"
duration="5"
warmup="1"
messages="100"
out=""
while (($#)); do
  case "$1" in
    --server) servers+=("$2") ;;
    --client) clients+=("$2") ;;
    --grpc) grpc="$2" ;;
    --scenarios) scenarios="$2" ;;
    --sizes) sizes="$2" ;;
    --metadata-sizes) metadata_sizes="$2" ;;
    --concurrency) concurrency="$2" ;;
    --duration) duration="$2" ;;
    --warmup) warmup="$2" ;;
    --messages) messages="$2" ;;
    --out) out="$2" ;;
    *)
      echo "unknown argument: $1" >&2
      exit 2
      ;;
  esac
  shift 2
done
if ((${#servers[@]} == 0 || ${#clients[@]} == 0)) && [[ -z $grpc ]]; then
  echo "need at least one --server and one --client, or --grpc" >&2
  exit 2
fi

work=$(mktemp -d)
# Results outlive the run, unlike the scratch directory.
out=${out:-$(mktemp -d "${TMPDIR:-/tmp}/trevrpc-bench.XXXXXX")}
mkdir -p "$out"
cleanup() {
  stop_server
  rm -rf "$work"
}
trap cleanup EXIT

generate_cert
results=$out/results.jsonl
: >"$results"

IFS=, read -ra scenario_list <<<"$scenarios"
IFS=, read -ra size_list <<<"$sizes"
IFS=, read -ra metadata_size_list <<<"$metadata_sizes"
IFS=, read -ra concurrency_list <<<"$concurrency"

# A run's deadline: warmup and duration, plus generous room for connecting and
# draining.
deadline=$(awk -v d="$duration" -v w="$warmup" 'BEGIN { printf "%d", d + w + 60 }')

failures=()
# run_matrix CLIENT SERVER COMMAND FLAGS... runs `COMMAND bench FLAGS...` with
# every scenario, size, metadata size, and concurrency, tagging each JSON result with the
# client and server names.
run_matrix() {
  local client=$1 server=$2 command=$3
  shift 3
  local scenario size metadata workers cell line transport="" arg previous=""
  for arg in "$@"; do
    [[ $previous == --transport ]] && transport=$arg
    previous=$arg
  done
  for scenario in "${scenario_list[@]}"; do
    for size in "${size_list[@]}"; do
      for metadata in "${metadata_size_list[@]}"; do
        for workers in "${concurrency_list[@]}"; do
          cell="$client-$transport-to-$server-$scenario-$size-md$metadata-c$workers"
          # shellcheck disable=SC2086 # commands may carry arguments
          if ! line=$(timeout --kill-after=5s "${deadline}s" $command bench "$@" \
            --scenario "$scenario" --size "$size" --metadata-size "$metadata" \
            --concurrency "$workers" --duration "$duration" --warmup "$warmup" \
            --messages "$messages" 2>"$out/$cell.log"); then
            echo "failed: $cell (see $out/$cell.log)" >&2
            failures+=("$cell")
            continue
          fi
          if ! jq -ec --arg client "$client" --arg server "$server" \
            '{client: $client, server: $server} + .' <<<"$line" >>"$results"; then
            echo "failed: $cell printed invalid JSON: $line" >&2
            failures+=("$cell")
            continue
          fi
          if [[ $(jq -r .errors <<<"$line") != 0 ]]; then
            echo "failed: $cell reported errors: $line" >&2
            failures+=("$cell")
          fi
        done
      done
    done
  done
}

for server in "${servers[@]}"; do
  server_name=${server%%=*}
  start_server "$server_name" "${server#*=}"
  ports=$out/server-$server_name.ports
  native=$(jq -r .native "$ports")
  qmux=$(jq -r .qmux "$ports")
  for client in "${clients[@]}"; do
    client_name=${client%%=*}
    run_matrix "$client_name" "$server_name" "${client#*=}" \
      --addr "127.0.0.1:$native" --ca "$cert" --server-name localhost --transport quic
    run_matrix "$client_name" "$server_name" "${client#*=}" \
      --addr "127.0.0.1:$native" --tcp-addr "127.0.0.1:$qmux" --ca "$cert" \
      --server-name localhost --transport tcp
  done
  stop_server
done

if [[ -n $grpc ]]; then
  start_server grpc "$grpc"
  port=$(jq -r .grpc "$out/server-grpc.ports")
  run_matrix grpc grpc "$grpc" \
    --addr "127.0.0.1:$port" --ca "$cert" --server-name localhost --transport grpc
  stop_server
fi

# One row per run, grouped by scenario so transports and runtimes line up.
{
  printf 'scenario\tsize\tmd\tconc\ttransport\tclient\tserver\tops/s\tmsgs/s\tMB/s\tp50us\tp99us\twire_B/msg\n'
  jq -sr 'sort_by(.scenario, .size, .metadata_size, .concurrency, .transport, .client, .server)[]
    | [.scenario, .size, (.metadata_size // 0), .concurrency, .transport, .client, .server,
       (.ops_per_sec | floor), (.msgs_per_sec | floor),
       ((.bytes_per_sec / 1e6 * 100 | floor) / 100),
       (.latency_us.p50 | floor), (.latency_us.p99 | floor),
       (if .wire_bytes_per_msg then (.wire_bytes_per_msg | floor) else "-" end)]
    | @tsv' "$results"
} | awk -F'\t' '{ for (i = 1; i <= NF; i++) printf "%-14s", $i; printf "\n" }' | tee "$out/summary.txt"

if ((${#failures[@]})); then
  printf 'failed: %s\n' "${failures[@]}" >&2
  exit 1
fi
echo "all benchmark runs passed; results in $results"
