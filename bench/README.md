# Load benchmarks

`bench/` measures end-to-end throughput and latency of every native client
against every server over QUIC and QMux over TLS, plus a grpc-go baseline over
HTTP/2. Per-function microbenchmarks live in each runtime (`go test -bench` in
`trevrpc-go`, `cargo bench` in `trevrpc-rust`), which `bench/micro.py`
(`nix run .#trevrpc-bench-micro`) runs and compares across commits with
benchstat; see the wiki's Benchmarks page.

```text
nix run .#trevrpc-bench              # every pair, default matrix
nix run .#trevrpc-bench -- --scenarios unary,bidi --sizes 64,4096 --duration 10
bench/run.py --server NAME=CMD... --client NAME=CMD... [--grpc CMD]
             [--scenarios LIST] [--sizes LIST] [--concurrency LIST]
             [--metadata-sizes LIST] [--duration SECONDS] [--warmup SECONDS]
             [--messages N] [--out DIR]
```

The packaged `trevrpc-bench` runs the packaged Go and Rust servers and clients
and the grpc-go baseline unless the command line names any `--server`,
`--client`, or `--grpc`, which then replace all three. `run.py` starts each server once, then runs every client over every transport
for every combination of scenario, size, metadata size, and concurrency. It prints a summary
table and writes one JSON object per run to `DIR/results.jsonl`; without
`--out`, `DIR` is a new `trevrpc-bench.*` directory under `$TMPDIR`. A run fails
when its client exits non-zero or reports errors.

Timings depend on the machine and on whatever else is running; compare runs
from the same machine only. Nothing in CI gates on these numbers: the flake
check runs a very short matrix only to keep the tool working.

## Server

Servers are the interop executables' `server` subcommand (see
[`interop/README.md`](../interop/README.md)) serving `trevrpc.interop.v1.Echo` and `trevrpc.interop.v1.UnreliableEcho`.
The grpc-go baseline (`bench/grpc-go`) follows the same contract: it serves the
same `Echo` service over gRPC (HTTP/2 over TLS), prints `{"grpc":<port>}` once
it is accepting, and exits when its stdin reaches EOF.

Servers must not log per RPC while benchmarking, and must not cap the message
count or cumulative size of a stream, since `bidi` workers keep one stream open
for the whole run.

## Client

```text
<binary> bench --addr <host:port> [--tcp-addr <host:port>] --ca <cert.pem>
               [--server-name localhost] --transport quic|tcp|grpc
               --scenario unary|server-stream|client-stream|bidi|unreliable-server-stream|unreliable-client-stream
               [--size BYTES] [--concurrency N] [--duration SECONDS]
               [--warmup SECONDS] [--messages N] [--metadata-size BYTES]
```

- `--transport quic` dials native QUIC at `--addr`. `--transport tcp` dials
  QMux over TLS at `--tcp-addr`. `--transport grpc` is the grpc-go baseline at
  `--addr`. Every worker shares one connection.
- `--size` (default 64) is the length of `EchoRequest.message` in bytes.
- `--concurrency` (default 1) is the number of workers, each with exactly one
  call or stream in flight.
- `--warmup` (default 1) seconds run before measuring; `--duration` (default 5) seconds are measured. Durations accept fractions. Only operations that
  start and finish inside the measured window count.
- `--metadata-size` (default 0) adds one request metadata entry, `bench-pad`,
  with a value of that many bytes to every call, to measure the cost of
  per-call metadata such as a bearer token. The gRPC baseline sends it as gRPC
  metadata. With `0`, calls carry no metadata. The maximum is 8192, the
  TrevRPC per-value metadata limit.
- `--messages` (default 100) is the number of messages per streaming call. The
  `client-stream` response is `messages × size` bytes, so that product must stay
  under the 4 MiB frame limit.

| Scenario                   | One operation                                                                                                                  | Messages per operation |
| -------------------------- | ------------------------------------------------------------------------------------------------------------------------------ | ---------------------- |
| `unary`                    | `Unary {message}`                                                                                                              | 1                      |
| `server-stream`            | `ServerStream {message, count: messages}`, reading every response                                                              | `messages`             |
| `client-stream`            | `ClientStream` sending `messages` requests, then reading the response (`messages × size` bytes)                                | `messages`             |
| `bidi`                     | One round trip on a `Bidi` stream that each worker keeps open: send `{message}`, read its echo                                 | 1                      |
| `unreliable-server-stream` | `UnreliableEcho.Bidi` sending `{message, count: messages}`, closing the send side, and reading every response until the status | responses delivered    |
| `unreliable-client-stream` | `UnreliableEcho.Bidi` sending `messages` requests `{message, count: 0}`, closing the send side, and reading the status         | `interop-received`     |

The `unreliable-*` scenarios measure [datagram messages](../wiki/Protocol-and-Wire-Format.md#datagram-messages): over `quic` the streamed messages travel as datagrams and may be lost, while over `tcp` they stay on the QMux stream. The gRPC baseline has no equivalent and does not run them.

The client prints exactly one JSON line to stdout and exits 0, or exits
non-zero if it could not connect or measure:

```json
{
  "scenario": "unary",
  "transport": "quic",
  "size": 64,
  "metadata_size": 0,
  "concurrency": 1,
  "duration_s": 5.0,
  "ops": 81234,
  "msgs": 81234,
  "errors": 0,
  "ops_per_sec": 16246.8,
  "msgs_per_sec": 16246.8,
  "bytes_per_sec": 1039795.2,
  "latency_us": { "p50": 58.1, "p90": 66.0, "p99": 101.3, "p999": 240.7 },
  "wire_bytes_per_msg": 141.2,
  "lost": 0
}
```

- `transport` is `quic`, `qmux`, or `grpc`.
- `msgs` counts application messages: responses for `unary` and
  `server-stream`, requests for `client-stream`, round trips for `bidi`.
- `bytes_per_sec` is `msgs_per_sec × size`.
- `latency_us` holds percentiles of whole-operation latency in microseconds.
- `wire_bytes_per_msg` is the bytes the client's connection sent and received
  during the measured window (UDP payload for QUIC), divided by `msgs`. It is
  omitted where the transport stack does not expose connection statistics.
- `errors` counts failed operations. Workers carry on after an error.
- `lost` counts messages sent but not delivered, for the `unreliable-*`
  scenarios only (`0` elsewhere). Loss is not an error.
