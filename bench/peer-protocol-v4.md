# Benchmark Peer Protocol V4

The benchmark controller starts one server peer and one client peer for every
sample. Peers exercise the selected stack through its public language API; the
controller does not implement an RPC transport.

## Process interface

Every peer implements these commands:

```text
trevrpc-bench-peer-<language> capabilities
trevrpc-bench-peer-<language> server [options]
trevrpc-bench-peer-<language> client [options]
```

Standard output is reserved for newline-delimited JSON protocol events. Peers
write diagnostics to standard error and flush every event before continuing.
Configuration is supplied as command-line arguments so peers do not need a
general-purpose JSON parser. Standard input carries the ASCII commands
`CONNECT HOST:PORT`, `START`, and `SHUTDOWN` at the phases described below.

All events contain `schema_version: 4`, `event`, and `peer`. Counters and
nanosecond values are decimal JSON strings so JavaScript can represent them
without loss.

V4 has no compatibility mode for earlier protocol versions. Campaign cells
select one of the closed stack values `trevrpc_native_quic`,
`trevrpc_webtransport`, or `grpc_http2`. Both server and client commands receive
the selected value through `--stack`.

The controller starts every peer as a new session and process-group leader.
Peer subprocesses must remain in that process group. Metrics and forced cleanup
cover the complete group, including Chromium and its helper processes.

## Capabilities

`capabilities` prints one event and exits successfully. Stack capabilities are
scoped to each role: advertising a stack for one role does not advertise it for
the other role. RPC kinds and the client histogram format remain event-wide.

```json
{
  "schema_version": 4,
  "event": "capabilities",
  "peer": "rust",
  "roles": {
    "client": ["trevrpc_native_quic", "grpc_http2"],
    "server": ["trevrpc_native_quic", "trevrpc_webtransport", "grpc_http2"]
  },
  "rpc_kinds": ["unary", "client_stream", "server_stream", "bidi"],
  "histogram": "log_linear_v1"
}
```

A Chromium-only peer therefore advertises only
`"roles":{"client":["trevrpc_webtransport"]}`, while a peer that only serves
WebTransport advertises `trevrpc_webtransport` only under `roles.server`.

## Server

Required arguments for every stack:

```text
--stack trevrpc_native_quic|trevrpc_webtransport|grpc_http2
--listen HOST:PORT --cert FILE --key FILE
```

For `trevrpc_webtransport`, the controller also supplies the origin emitted by
the prepared client:

```text
--webtransport-origin http://HOST:PORT
```

The server must allow that exact browser origin for WebTransport admission.
`PORT` may be zero. The controller may place the peer in an isolated network
namespace and supply a non-loopback literal IP. Once the listener can accept
RPCs, the peer prints:

```json
{ "schema_version": 4, "event": "ready", "peer": "rust", "address": "127.0.0.1:43117", "pid": 1234 }
```

The server continues until it reads `SHUTDOWN` or receives a termination
signal. A graceful protocol shutdown prints a `stopped` event before exiting.

## Client

Required arguments for every stack:

```text
--cert FILE
--stack trevrpc_native_quic|trevrpc_webtransport|grpc_http2
--rpc unary|client_stream|server_stream|bidi
--concurrency N
--warmup-ms N
--measurement-ms N
--request-bytes N
--response-bytes N
--messages-per-stream N
```

Native QUIC and gRPC clients also receive `--address HOST:PORT` at startup.
Their `--cert` file is the campaign CA. Their startup behavior is unchanged:
connect, validate one RPC, warm up, create all lanes, and emit `armed`.

A WebTransport client starts before its RPC server and does not receive
`--address`. Its `--cert` file is the exact `server.pem` leaf certificate, which
the Chromium peer uses to configure trust for the generated server identity.
After its HTTPS browser page and Chromium process are ready, it emits:

```json
{
  "schema_version": 4,
  "event": "prepared",
  "peer": "chromium",
  "origin": "http://127.0.0.1:4443",
  "pid": 1235
}
```

The controller starts the server with that origin, waits for `ready`, and sends
the WebTransport client:

```text
CONNECT 127.0.0.1:43117
```

The client then creates and verifies its WebTransport session, validates one
RPC, runs untimed warmup, creates all workload lanes, and emits the same event
used by native clients:

```json
{ "schema_version": 4, "event": "armed", "peer": "chromium", "pid": 1235 }
```

Every client waits for `START` before measured work. The measurement uses a
fixed admission window. Operations admitted before the deadline may drain
after it; no operation may begin after the deadline.

The client prints one `sample` event after drain:

```json
{
  "schema_version": 4,
  "event": "sample",
  "peer": "rust",
  "rpc_kind": "unary",
  "admission_ns": "1000000000",
  "elapsed_ns": "1000123456",
  "drain_ns": "123456",
  "completed": "1000",
  "failed": "0",
  "request_messages": "1000",
  "response_messages": "1000",
  "histogram": [{ "upper_bound_ns": "1015807", "count": "1000" }]
}
```

The histogram count must equal `completed`. Throughput and quantiles are not
peer fields; the controller derives them from the configured admission window
and histogram.

Campaigns are limited to concurrency 1,024, request and response payloads of
64 MiB each, and 1,000,000 application messages per streaming RPC. Peers reject
wire requests outside the payload and stream-message limits before allocating
responses.

## Workload semantics

One operation is one complete bounded RPC:

| RPC kind        | Completion boundary                                                              |
| --------------- | -------------------------------------------------------------------------------- |
| `unary`         | One validated response and terminal success                                      |
| `client_stream` | All request messages sent, send side closed, summary validated, terminal success |
| `server_stream` | All response messages validated and terminal success                             |
| `bidi`          | Concurrent send and receive complete, all responses validated, terminal success  |

Connection establishment and warmup are outside measurement. Protobuf
serialization, deserialization, request construction, and response validation
are inside each operation timer. A streaming operation contains exactly
`messages-per-stream` application messages. Bidi request and response payload
sizes may differ. Request and response payloads are zero-filled; clients verify
the response sequence, length, and contents so all peer combinations have the
same deterministic application work.

## Histogram

`log_linear_v1` is a sparse integer histogram with approximately 0.1% relative
precision and exact buckets below 1024 ns. For a positive latency `value`:

```text
shift = max(floor(log2(value)) - 9, 0)
upper_bound = (((value >> shift) + 1) << shift) - 1
```

Peers count samples by `upper_bound_ns`, sort buckets in ascending order, and
emit only non-empty buckets. The controller obtains quantiles by finding the
first bucket whose cumulative count reaches `ceil(quantile * total_count)`.

## Failures

A fatal peer error is written as an event when stdout remains usable:

```json
{
  "schema_version": 4,
  "event": "error",
  "peer": "rust",
  "phase": "measure",
  "code": "rpc_failed",
  "message": "unexpected terminal status"
}
```

Any peer exit, malformed event, failed operation, histogram/count mismatch,
timeout, or configuration mismatch invalidates the sample. The controller
kills the full peer process group on failure. Raw stdout and stderr are retained
in the campaign artifact directory.
