# TrevRPC Benchmarks

`trevrpc-bench` is the centralized controller for the language-owned benchmark
peers. It runs native QUIC client/server pairs, validates their structured
results, collects process metrics, and generates replayable reports.

## Build

Build the controller and all six peers as one immutable suite:

```sh
nix build .#trevrpc-bench-suite
```

The resulting `bin` directory contains `trevrpc-bench` and one
`trevrpc-bench-peer-*` executable for C, C++, Go, JavaScript, Kotlin, and Rust.
Peers live and build in their associated `trevrpc-*` directory. The canonical
RPC contract and process protocol live under `bench/`.

## Run

Campaigns are JSON files. A cell may use any listed peer as its client and any
listed peer as its server, so the same controller supports product-stack and
split client/server comparisons.

```sh
export PATH="$PWD/result/bin:$PATH"

trevrpc-bench validate bench/campaigns/native-quic.example.json
trevrpc-bench capabilities bench/campaigns/native-quic.example.json
trevrpc-bench run bench/campaigns/native-quic.example.json \
  --out target/bench/native-quic
```

`cross-language-smoke.example.json` uses C as both reference client and
reference server to exercise every peer in both roles. Its deliberately short
windows validate interoperability and the harness only; they are not suitable
for performance comparisons.

Compilation and dependency realization must happen before a publishable run.
Run measurements on `ssh bench`, keep the host otherwise idle, and record the
campaign and environment in `wiki/Benchmarks.md`.

## Artifacts

Every output directory contains:

- `manifest.json`: campaign, source state, artifact digests, and metric scope.
- `samples.jsonl`: canonical validated samples and latency histograms.
- `aggregate.csv`: medians across repetitions.
- `report.md`: human-readable result table and interpretation boundary.
- `report.html`: self-contained SVG throughput and p99 graphs.
- `raw/<sample>/`: exact peer stdout and stderr.
- `certificates/`: one-run private CA and server identity.

Reports are deterministic from `samples.jsonl` and can be regenerated without
rerunning a campaign:

```sh
trevrpc-bench report target/bench/native-quic
```

## Measurement Boundary

Peers establish and validate their connection, run warmup, and arm all lanes
before the controller starts process metrics and sends `START`. The client then
runs closed-loop operations during a fixed admission window and drains work
admitted before its deadline. Connection setup, warmup, histogram serialization,
report generation, and shutdown are excluded.

Latency is measured for a complete bounded RPC. A streaming RPC contains the
configured `messages_per_stream`; message throughput is reported separately
from operation throughput. See `peer-protocol-v1.md` for exact semantics.
