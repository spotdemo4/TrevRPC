# RPC Benchmark Harness

`trevrpc-bench` is the centralized controller for language-owned benchmark
peers. It runs TrevRPC native QUIC and WebTransport client/server pairs,
validates their structured results, collects process-group metrics, and
generates replayable reports.

## Build

Build the controller, all six language peers, and the host's browser peers as
one immutable suite:

```sh
nix build .#trevrpc-bench-suite
```

The suite supports `x86_64-linux` and `aarch64-darwin`. Its `bin` directory
contains `trevrpc-bench` and one `trevrpc-bench-peer-*` executable for C, C++,
Go, JavaScript, Kotlin, and Rust. Linux adds the Chromium and Firefox
WebTransport client peers; Apple Silicon macOS adds the pinned Playwright Cocoa
WebKit peer. The macOS target provides focused WebKit interoperability coverage,
not a claim that every benchmark mode is supported there.

The GitHub `smoke.yaml` workflow builds the same suite once per platform before
running each client/server cell as a separate check, then publishes one stable
`smoke` gate for branch protection.

The benchmark and conformance executables are included in their associated
`trevrpc-*` packages. The canonical RPC contract and process protocol live under
`bench/`.
The JavaScript peer's nested npm manifest is private until `trevrpc-js` is
published; the supported distributable peer package is currently its Nix
package.

## Run

Campaigns and peers use schema V4. Every cell selects a required `stack`:
`trevrpc_native_quic` or `trevrpc_webtransport`. A cell may use
any listed peer as its client and any listed peer as its server, so the same
controller supports stack and split client/server comparisons. Capabilities are
validated independently for each role; client support never implies server
support or vice versa. Earlier campaign and peer-event schema versions are not
supported.

```sh
export PATH="$PWD/result/bin:$PATH"

trevrpc-bench validate bench/campaigns/native-quic.example.json
trevrpc-bench capabilities bench/campaigns/native-quic.example.json
trevrpc-bench run bench/campaigns/native-quic.example.json \
  --out target/bench/native-quic

# Run one directed client/server cell from a larger campaign.
trevrpc-bench run bench/campaigns/native-smoke.example.json \
  --out target/bench/go-to-c \
  --cell go-to-c
```

`native-smoke.example.json` contains all 36 directed combinations of the six
native clients and servers. Its deliberately short windows validate
interoperability and the harness only; they are not suitable for performance
comparisons. GitHub CI runs every cell as a separate check with four functional
samples, one for each RPC kind. The smoke helper sets
`TREVRPC_BENCH_SERVER_WORKERS=8` to keep concurrent jobs within CI task limits;
normal benchmark runs retain the C peer's 128-worker default.

Campaign runs stop at the first failed sample by default. Set
`TREVRPC_BENCH_RUN_ENTIRE_CAMPAIGN=true` to attempt every remaining sample and
return an aggregate failure after the matrix completes. The variable accepts
only `true` or `false`. Campaign setup failures still stop immediately, and an
incomplete campaign retains its successful samples and raw diagnostics without
generating reports.

`stack-comparison.example.json` contains same-language native QUIC cells for C,
C++, Go, JavaScript, Kotlin, and Rust. The controller checks each peer's
advertised stacks, roles, RPC kinds, and histogram before starting a run.

`chromium-smoke.example.json`, `firefox-smoke.example.json`, and
`webkit-smoke.example.json` each start the respective browser client before
each RPC server. Chromium, Firefox, and WebKit run against all six server
implementations for 24 functional samples. WebKit uses a temporary
`webtransport-go` fork for the Go server to test Safari compatibility and keeps
the Rust server covered while [h3#347](https://github.com/hyperium/h3/issues/347)
remains open. The client first reports its prepared browser origin; the
controller passes that origin to the server, sends
the ready server address back with `CONNECT`, waits for `armed`, and then starts
measurement.

The GitHub smoke workflow runs Chromium and Firefox cells on Linux and WebKit
cells on `aarch64-darwin`. Playwright WebKit uses WPE on Linux, whose network
process does not implement WebTransport, so the WebKit browser bundle is not
selected and the peer does not advertise WebTransport coverage there. On
Apple Silicon macOS, Playwright launches the pinned Cocoa WebKit build. The
Darwin campaign uses the loopback backend; Linux-only `netns` emulation is not
substituted. Process-group procfs metrics are unavailable on Darwin and are
recorded explicitly as unavailable with zero-valued metric fields.

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
`manifest.json`. Peer leaders remain direct children with host-visible PIDs,
and every peer runs in its own session, so process-group metrics retain the same
scope as loopback campaigns while including peer subprocesses.

The netns backend is Linux-only and requires `unshare`, `ip`, `tc`, and a kernel
that permits unprivileged user namespaces. Unsupported hosts fail explicitly;
the controller never silently substitutes loopback. See
`netns-smoke.example.json` for a short functional campaign. Network emulation
improves repeatability but does not reproduce physical NIC offloads, cloud
routing, or competing traffic.

`regional-wan-netns.example.json` is the retained publication campaign. It
applies approximately 30 ms RTT, 2 ms one-way jitter, 0.1% random loss, and a
100 Mbit/s rate to the full same-language native QUIC matrix.

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
- `certificates/`: one-run private CA and ECDSA P-256 server identity.

Reports are deterministic from `samples.jsonl` and can be regenerated without
rerunning a campaign:

```sh
trevrpc-bench report target/bench/native-quic
```

## Measurement Boundary

Peers establish and validate their connection, run warmup, and arm all lanes
before the controller starts process metrics and sends `START`. WebTransport is
the exception only during setup: the browser peer first prepares its browser
origin, then receives `CONNECT HOST:PORT` after the server is ready. The client
then runs closed-loop operations during a fixed admission window and drains work
admitted before its deadline. Peak RSS is the highest 10 ms sum of `VmRSS`
across the peer process group rather than a process lifetime high-water mark.
CPU includes the complete process group and Linux reaped-child CPU accounting;
context-switch counters retain the final observed value for exited members.
Connection setup, warmup, report generation, and shutdown are excluded; client
group CPU includes encoding and writing the final histogram event before the
controller can stop sampling.

Latency is measured for a complete bounded RPC. A streaming RPC contains the
configured `messages_per_stream`; message throughput is reported separately
from operation throughput. See `peer-protocol-v4.md` for the peer protocol
and exact semantics.
