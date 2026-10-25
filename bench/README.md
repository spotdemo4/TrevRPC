# Load benchmarks

`bench/` measures end-to-end throughput and latency of every native client
against every server over QUIC and QMux over TLS, plus grpc-go and connect-go
baselines over HTTP/2. Per-function microbenchmarks live in each runtime (`go test -bench` in
`trevrpc-go`, `cargo bench` in `trevrpc-rust`), which `bench/micro.py`
(`nix run .#trevrpc-bench-micro`) runs and compares across commits with
benchstat; see the wiki's Benchmarks page.

```text
nix run .#trevrpc-bench              # every pair, default matrix
nix run .#trevrpc-bench -- --scenarios unary,bidi --sizes 64,4096 --duration 10
nix run .#trevrpc-bench -- --netem 'delay 10ms' --netem 'delay 10ms loss 1%'
nix run .#trevrpc-bench -- --payload records --compressions none,gzip,zstd,dictionary,stream,stream-dictionary \
  --scenarios unary,client-stream,bidi --sizes 256,1024,4096
nix run .#trevrpc-bench -- --table comparison --repeat 3 --tolerance 3
bench/run.py --server NAME=CMD... --client NAME=CMD... [--baseline CMD] [--table NAME]
             [--scenarios LIST] [--sizes LIST] [--concurrency LIST]
             [--metadata-sizes LIST] [--payload repeat|records]
             [--compressions LIST] [--duration SECONDS] [--warmup SECONDS]
             [--messages N] [--repeat N] [--tolerance PERCENT]
             [--netem PROFILE...] [--out DIR]
```

The packaged `trevrpc-bench` runs the packaged Go and Rust servers and clients
and the baselines unless the command line names any `--server`, `--client`, or
`--baseline`, which then replace all three. The baseline runs its gRPC client
against its gRPC server and its Connect client against its Connect server.
`run.py` starts each server once, then runs every client over every transport
for every combination of scenario, size, metadata size, and concurrency. It prints a summary
table and writes one JSON object per run to `DIR/results.jsonl`; without
`--out`, `DIR` is a new `trevrpc-bench.*` directory under `$TMPDIR`. A run fails
when its client exits non-zero or reports errors.

Timings depend on the machine and on whatever else is running; compare runs
from the same machine only. `--repeat N` (default 1) runs each cell up to N
times and reports the run with the median messages per second; its JSON object
adds `repeats` and every run's rate as `msgs_per_sec_runs`, and the summary's
`spread%` column is the range of those rates as a share of the median. Each
run logs to `CELL-rI.log`. Single runs on a shared machine can be a third off,
so use at least 3 before reading anything into one cell. `--tolerance PERCENT`
stops repeating a cell as soon as two of its runs differ by at most that
share of the lower one, so steady cells take two runs and noisy ones up to N.

`--table NAME` runs only the cells that one table of the wiki's Benchmarks page
reads, with that table's options (scenarios, sizes, concurrency, payload,
compression modes, netem profiles, messages, duration) as defaults; explicit
flags still override them. The tables are in [`tables.py`](tables.py):
`comparison`, `compression`, `link`, `lossy`, `metadata`, and `datagrams`.
They refer to the packaged servers and clients by name (`go`, `rust`, and the
baseline's `grpc` and `connect`) and run each TrevRPC client against its own
runtime's server only. Nothing in CI gates on these numbers: the flake
check runs a very short matrix only to keep the tool working.

## Compression

`--payload records` replaces the default payload, the letter `x` repeated, with
[realistic messages](#payload) that differ from each other, so compression has
something real to work on. `--compressions LIST` (default `none`) then runs
every cell once per compression mode, on the transports that have it:

| Mode                | Transports        | What compresses                                                                                                           |
| ------------------- | ----------------- | ------------------------------------------------------------------------------------------------------------------------- |
| `none`              | all               | nothing                                                                                                                   |
| `gzip`              | `grpc`, `connect` | each message on its own, with the library's gzip                                                                          |
| `zstd`              | `grpc`, `connect` | each message on its own, with zstd from a third-party codec (see [`baseline`](baseline/README.md))                        |
| `dictionary`        | `quic`, `tcp`     | each message on its own, with TrevRPC's [dictionary compression](../wiki/Compression.md) and a trained dictionary         |
| `stream`            | `quic`, `tcp`     | each streamed direction of a call with one zstd context ([stream compression](../wiki/Compression.md#stream-compression)) |
| `stream-dictionary` | `quic`, `tcp`     | stream compression, starting from the trained dictionary                                                                  |

Compression modes other than `none` run `unary`, `client-stream` and `bidi`
only, and the `stream` modes only the last two, since stream compression never
applies to unary calls. `server-stream` echoes one request message for every
response, and the `unreliable-*` scenarios carry their messages as datagrams,
so neither would show anything real. A mode never runs on a transport that
lacks it.

The trained dictionary comes from `payload.py`: `run.py` generates its training
samples, trains an 8 KiB dictionary with ID 2 using `zstd --train` (the result
is the same on every run), starts every TrevRPC server with `--dictionary`, and
passes it to the `dictionary` modes' clients. `wire_bytes_per_msg` is the
number to compare across modes: it counts what the client's connection actually
carried, framing and TLS included.

## Network emulation

Loopback has no loss and almost no latency, so on its own it cannot show what
QUIC is for. `--netem PROFILE` runs the whole benchmark inside new user and
network namespaces (Linux only, without root) whose loopback link applies
[netem](https://man7.org/linux/man-pages/man8/tc-netem.8.html) with
`PROFILE`'s arguments, such as `delay 10ms loss 1%`. Given more than once, it
runs the matrix once per profile against the same servers. The summary prints
one table per profile, and each JSON record carries the profile as `netem`.

- Every packet crosses the link once, in either direction, so `delay 10ms`
  is a 20 ms round trip and `loss 1%` drops 1% of the packets each way.
- The link has a 1500-byte MTU. A `tbf` qdisc in front of netem segments TCP
  and UDP offload (TSO and GSO) super-packets into frames, as a NIC does, so
  loss applies to each frame. Otherwise netem would drop up to 64 KB of TCP,
  or a QUIC stack's whole batch of datagrams, as one packet. Segmenting caps
  the link at roughly 8 Gbit/s.
- netem's queue holds 1,000,000 packets, so delay alone never drops any. A
  `limit` in the profile overrides it, which matters with `rate`: a queue
  that deep in front of a slow rate adds seconds of delay.
- Size streaming operations to the link. Only operations that finish inside
  the measured window count, and with loss one connection carries far less:
  at a 20 ms round trip and 1% loss, roughly 1 MB/s. The default 100
  messages per call from 64 workers is 6.4 MB per round at 1 KiB, which
  outlasts a 5 s window, so the run fails with no completed operations.
  Lower `--messages` or raise `--duration`.
- Under loss, the `unreliable-*` scenarios lose far more than the link drops.
  Loss shrinks the congestion window, and a sender that blasts datagrams
  faster than the window allows has the excess dropped by its QUIC stack.

## Server

Servers are the interop executables' `server` subcommand (see
[`interop/README.md`](../interop/README.md)) serving `trevrpc.interop.v1.Echo`, `trevrpc.interop.v1.UnreliableEcho` and `trevrpc.interop.v1.CompressedEcho`,
started with `--dictionary` and the [trained dictionary](#compression).
The baseline ([`bench/baseline`](baseline/README.md)) follows the same
contract: it serves the same `Echo` service over gRPC with grpc-go and over the
Connect protocol with connect-go (both HTTP/2 over TLS, on separate ports),
prints `{"grpc":<port>,"connect":<port>}` once it is accepting, and exits when
its stdin reaches EOF. It accepts gzip and zstd requests and compresses its
responses with the encoding of the request.

Servers must not log per RPC while benchmarking, and must not cap the message
count or cumulative size of a stream, since `bidi` workers keep one stream open
for the whole run.

## Client

```text
<binary> bench --addr <host:port> [--tcp-addr <host:port>] --ca <cert.pem>
               [--server-name localhost] --transport quic|tcp|grpc|connect
               --scenario unary|server-stream|client-stream|bidi|unreliable-server-stream|unreliable-client-stream
               [--size BYTES] [--concurrency N] [--duration SECONDS]
               [--warmup SECONDS] [--messages N] [--metadata-size BYTES]
               [--payload repeat|records] [--compression MODE] [--dictionary FILE]
```

- `--transport quic` dials native QUIC at `--addr`. `--transport tcp` dials
  QMux over TLS at `--tcp-addr`. `--transport grpc` and `--transport connect`
  are the baseline's gRPC and Connect clients at `--addr`. Every worker shares
  one channel, which uses one connection, except that a TrevRPC client uses
  its runtime's defaults: the Rust channel spreads QUIC calls over several
  connections (see the Rust guide).
- `--size` (default 64) is the length of `EchoRequest.message` in bytes.
- `--concurrency` (default 1) is the number of workers, each with exactly one
  call or stream in flight.
- `--warmup` (default 1) seconds run before measuring; `--duration` (default 5) seconds are measured. Durations accept fractions. Only operations that
  start and finish inside the measured window count.
- `--metadata-size` (default 0) adds one request metadata entry, `bench-pad`,
  with a value of that many bytes to every call, to measure the cost of
  per-call metadata such as a bearer token. The baseline sends it as gRPC
  metadata or as a Connect request header. With `0`, calls carry no metadata. The maximum is 8192, the
  TrevRPC per-value metadata limit.
- `--messages` (default 100) is the number of messages per streaming call. The
  `client-stream` response is `messages × size` bytes, so that product must stay
  under the 4 MiB frame limit.
- `--payload` (default `repeat`) is what `EchoRequest.message` holds: `repeat`
  is `size` times the letter `x`, the same in every request; `records` is a
  new [records](#payload) message for every request.
- `--compression` (default `none`) is one of the [compression modes](#compression)
  the client's transport has; a client exits non-zero for a mode, or a mode and
  scenario, it does not run. The TrevRPC clients run `dictionary` and
  `stream-dictionary` with the zstd dictionary in `--dictionary` for
  `EchoRequest` and `EchoResponse`, and the `stream` modes against
  `CompressedEcho.Bidi` and `CompressedEcho.ClientStream`, which allow stream
  compression, instead of `Echo`.

| Scenario                   | One operation                                                                                                                  | Messages per operation |
| -------------------------- | ------------------------------------------------------------------------------------------------------------------------------ | ---------------------- |
| `unary`                    | `Unary {message}`                                                                                                              | 1                      |
| `server-stream`            | `ServerStream {message, count: messages}`, reading every response                                                              | `messages`             |
| `client-stream`            | `ClientStream` sending `messages` requests, then reading the response (`messages × size` bytes)                                | `messages`             |
| `bidi`                     | One round trip on a `Bidi` stream that each worker keeps open: send `{message}`, read its echo                                 | 1                      |
| `unreliable-server-stream` | `UnreliableEcho.Bidi` sending `{message, count: messages}`, closing the send side, and reading every response until the status | responses delivered    |
| `unreliable-client-stream` | `UnreliableEcho.Bidi` sending `messages` requests `{message, count: 0}`, closing the send side, and reading the status         | `interop-received`     |

The `unreliable-*` scenarios measure [datagram messages](../wiki/Protocol-and-Wire-Format.md#datagram-messages): over `quic` the streamed messages travel as datagrams and may be lost, while over `tcp` they stay on the QMux stream. The baselines have no equivalent and do not run them.

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
  "lost": 0,
  "payload": "repeat",
  "compression": "none"
}
```

- `transport` is `quic`, `qmux`, `grpc`, or `connect`.
- `payload` and `compression` echo `--payload` and `--compression`.
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

## Payload

The `records` payload is log records, one JSON object per line, generated by
[`payload.py`](payload.py); the Go, Rust and baseline clients implement the same
generator byte for byte and check it against `payload.GOLDEN_SHA256`. A record
looks like this:

```json
{
  "ts": 1790000000011,
  "service": "gateway",
  "region": "us-east-1",
  "method": "GET",
  "route": "/api/v1/cart",
  "status": 201,
  "latency_ms": 368,
  "trace": "abe53deff85f9dfaed43fdd75acd2693",
  "user": 938593
}
```

- A generator is an xorshift64 state (`x ^= x << 13; x ^= x >> 7; x ^= x << 17`,
  on 64 bits), stepped once per random value. Each worker has its own,
  seeded with `0x9E3779B97F4A7C15` plus the worker's index from 0.
- A record draws, in this order: `ts`, the base `1790000000000` plus 13 times
  the generator's record count plus a value from 0 to 12, after which the count
  goes up; `service`, `region`, `method`, `route` and `status` from the fixed
  lists in `payload.py` (`status` weighted, by repeating 200); `latency_ms`
  from 1 to 2000; `trace`, 32 hex digits, one draw each; `user`, from 1 to 1000000. A value from a list is `list[x % len]`, a number from `low` to `high`
  is `low + x % (high - low + 1)`.
- A message of `size` bytes is records appended until they reach `size` bytes,
  cut to exactly `size`. The rest of the last record is discarded; the next
  message starts with a new record.

Every request is a new message, so no two in a run are the same, while the
fields that repeat (keys, services, regions, routes) are what a dictionary or a
stream context can learn.
