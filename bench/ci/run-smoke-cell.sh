#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 3 ]]; then
  echo "usage: $0 CAMPAIGN.json CELL_ID OUTPUT_DIRECTORY" >&2
  exit 2
fi

campaign=$1
cell=$2
output=$3

command -v trevrpc-bench >/dev/null
[[ -f $campaign ]]
[[ ! -e $output ]]

export TREVRPC_BENCH_SERVER_WORKERS=${TREVRPC_BENCH_SERVER_WORKERS:-8}
export TREVRPC_BENCH_RUN_ENTIRE_CAMPAIGN=${TREVRPC_BENCH_RUN_ENTIRE_CAMPAIGN:-true}

trevrpc-bench run "$campaign" --out "$output" --cell "$cell"

[[ $(wc -l <"$output/samples.jsonl") -eq 4 ]]
for report in aggregate.csv report.md report.html; do
  [[ -s $output/$report ]]
done
