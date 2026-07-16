# RPC Benchmark Harness

`trevrpc-bench` is the centralized controller for language-owned benchmark
peers. It runs TrevRPC native QUIC and gRPC HTTP/2 client/server pairs,
validates their structured results, collects process metrics, and generates
replayable reports.

## Build

Build the controller and all six peers as one immutable suite:

```sh
nix build .#trevrpc-bench-suite
```

The complete suite currently targets `x86_64-linux` because the Kotlin peer
packages Netty's Linux x86-64 native QUIC transport.

The resulting `bin` directory contains `trevrpc-bench` and one
`trevrpc-bench-peer-*` executable for C, C++, Go, JavaScript, Kotlin, and Rust.
Peers are peer-only overrides of their associated `trevrpc-*` consumer packages,
which remain free of benchmark-only gRPC dependencies by default. The canonical
RPC contract and process protocol live under `bench/`. Individual Nix packages
use the `trevrpc-<language>-bench-peer` attribute names and are equivalent to
overriding the associated language package with `benchPeer = true`.
The JavaScript peer's nested npm manifest is private until `trevrpc-js` is
published; the supported distributable peer package is currently its Nix
package.

## Run

Campaigns and peers use schema V3. Every cell selects a required `stack`:
`trevrpc_native_quic` or `grpc_http2`. A cell may use any listed peer as its
client and any listed peer as its server, so the same controller supports stack
and split client/server comparisons. Earlier campaign and peer-event schema
versions are not supported.

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

`stack-comparison.example.json` contains same-language TrevRPC and gRPC cells
for C, C++, Go, JavaScript, Kotlin, and Rust. The controller checks each peer's
advertised stacks, roles, RPC kinds, and histogram before starting a run.

### Single-Host Network Emulation

Campaigns default to the existing loopback backend. Setting `network.backend`
to `netns` makes the controller re-execute itself in a rootless user and network
namespace, create isolated client and server network namespaces, and connect
them with a direct veth pair. Linux traffic control applies each direction's
delay, jitter, random loss, rate, and queue limit to the sending endpoint.

```json
"network": {
  "backend": "netns",
  "client_to_server": {
    "delay_ms": 15,
    "jitter_ms": 2,
    "loss_percent": 0.1,
    "rate_mbit": 100,
    "queue_packets": 1000
  },
  "server_to_client": {
    "delay_ms": 15,
    "jitter_ms": 2,
    "loss_percent": 0.1,
    "rate_mbit": 100,
    "queue_packets": 1000
  },
  "mtu": 1500
}
```

Delay is one-way: 15 ms on each endpoint produces approximately 30 ms RTT.
The controller uses the benchmark-only `198.18.0.0/30` range inside its private
namespace and records the effective addresses and qdisc configuration in
`manifest.json`. Peer processes remain direct children with host-visible PIDs,
so process metrics retain the same scope as loopback campaigns.

The netns backend is Linux-only and requires `unshare`, `ip`, `tc`, and a kernel
that permits unprivileged user namespaces. Unsupported hosts fail explicitly;
the controller never silently substitutes loopback. See
`netns-smoke.example.json` for a short functional campaign. Network emulation
improves repeatability but does not reproduce physical NIC offloads, cloud
routing, or competing traffic.

`regional-wan-netns.example.json` is the retained publication campaign. It
applies approximately 30 ms RTT, 2 ms one-way jitter, 0.1% random loss, and a
100 Mbit/s rate to the full same-language TrevRPC/gRPC matrix.

Compilation and dependency realization must happen before a publishable run.
Run measurements on `ssh bench` and keep the host otherwise idle. Retain the
complete output directory so its manifest, samples, raw peer logs, and generated
reports remain reproducible.

## Artifacts

Every output directory contains:

- `manifest.json`: campaign, source state, artifact digests, metric scope, and
  effective network environment.
- `samples.jsonl`: canonical validated samples and latency histograms.
- `aggregate.csv`: medians across repetitions.
- `report.md`: human-readable result table and interpretation boundary.
- `report.html`: self-contained SVG throughput and p99 graphs.
- `raw/<sample>/`: exact peer stdout and stderr; sample names include the stack.
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
admitted before its deadline. Peak RSS is the highest 10 ms `VmRSS` sample in
that interval rather than the process lifetime high-water mark. Connection
setup, warmup, report generation, and shutdown are excluded; client process CPU
includes encoding and writing the final histogram event before the controller
can stop sampling.

Latency is measured for a complete bounded RPC. A streaming RPC contains the
configured `messages_per_stream`; message throughput is reported separately
from operation throughput. See `peer-protocol-v3.md` for the peer protocol
and exact semantics.
