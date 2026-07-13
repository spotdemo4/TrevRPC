# TrevRPC Benchmark Peer Protocol V1

The benchmark controller starts one server peer and one client peer for every
sample. Peers exercise their language's public TrevRPC API; the controller does
not implement an RPC transport.

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
general-purpose JSON parser. Standard input carries only the ASCII commands
`START` and `SHUTDOWN`.

All events contain `schema_version: 1`, `event`, and `peer`. Counters and
nanosecond values are decimal JSON strings so JavaScript can represent them
without loss.

## Capabilities

`capabilities` prints one event and exits successfully:

```json
{
  "schema_version": 1,
  "event": "capabilities",
  "peer": "rust",
  "roles": ["client", "server"],
  "rpc_kinds": ["unary", "client_stream", "server_stream", "bidi"],
  "transports": ["native_quic"],
  "histogram": "log_linear_v1"
}
```

## Server

Required arguments:

```text
--listen HOST:PORT --cert FILE --key FILE
```

`PORT` may be zero. Once the listener can accept RPCs, the peer prints:

```json
{ "schema_version": 1, "event": "ready", "peer": "rust", "address": "127.0.0.1:43117", "pid": 1234 }
```

The server continues until it reads `SHUTDOWN` or receives a termination
signal. A graceful protocol shutdown prints a `stopped` event before exiting.

## Client

Required arguments:

```text
--address HOST:PORT
--cert FILE
--rpc unary|client_stream|server_stream|bidi
--concurrency N
--warmup-ms N
--measurement-ms N
--request-bytes N
--response-bytes N
--messages-per-stream N
```

The client establishes one verified connection, validates one RPC, runs the
untimed warmup, creates all workload lanes, then prints:

```json
{ "schema_version": 1, "event": "armed", "peer": "rust", "pid": 1235 }
```

It does not begin measured work until it reads `START`. The measurement uses a
fixed admission window. Operations admitted before the deadline may drain
after it; no operation may begin after the deadline.

The client prints one `sample` event after drain:

```json
{
  "schema_version": 1,
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
  "schema_version": 1,
  "event": "error",
  "peer": "rust",
  "phase": "measure",
  "code": "rpc_failed",
  "message": "unexpected terminal status"
}
```

Any peer exit, malformed event, failed operation, histogram/count mismatch,
timeout, or configuration mismatch invalidates the sample. Raw stdout and
stderr are retained in the campaign artifact directory.
