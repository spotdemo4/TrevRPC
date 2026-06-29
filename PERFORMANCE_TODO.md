# TrevRPC Performance TODO

Generated: 2026-06-26

Scope: this file now contains only remaining performance work that is blocked or deferred after the 2026-06-26 implementation pass. Completed items were removed.

## Measurement Rules

Use these rules for every follow-up below.

1. Build outside measured commands. Use `ssh bench` for benchmark measurements and record host context.
2. Record before/after rows, command, timestamp, relevant environment variables, commit/tree state, failed samples, and anomalies in `wiki/Benchmarks.md` whenever a benchmark is run.
3. Prefer focused split runs while iterating, then broader split or WebTransport runs before claiming broad wins.
4. Preserve cancellation, deadlines, graceful shutdown, stream resets, terminal status handling, overload, and partial-stream semantics.
5. Finish each code change with `nix fmt` and `nix flake check`. Intent-to-add new files before `nix flake check` so Nix sees them.

## Blocked Follow-Ups

### P0.2 Separate Transport Cost From Generated/Protobuf Cost

Blocked on comparable benchmark design across C, Go, JS, and Rust.

Current state:

| Area                 | Status                                                                                                        |
| -------------------- | ------------------------------------------------------------------------------------------------------------- |
| WebTransport         | Existing browser runner already supports payload and concurrency knobs.                                       |
| Split RPC payloads   | Go and Rust split servers partially support `count:payload`; C and JS split paths do not.                     |
| Raw/pre-encoded rows | C and JS have partial micro/profile paths, but there are no comparable split rows across all implementations. |

Concrete blocker: adding raw rows without matching semantics would make the split tables misleading. The next safe step is benchmark-only design work that adds C/JS payload parsing and one comparable raw route/client shape per runtime, with the existing CSV schema preserved by encoding variants in shape names.

### R1 Trace-Driven Rust/Quinn `server / bidi_stream_latency` Outlier

Blocked on frame-level trace evidence.

Current state:

| Evidence item                      | Status                                                                                                  |
| ---------------------------------- | ------------------------------------------------------------------------------------------------------- |
| Speculative final-message-with-FIN | Rejected by prior unstable benchmark evidence; do not reintroduce without traces and correctness tests. |
| Global zero-length FIN             | Rejected by prior correctness failure; do not reintroduce.                                              |
| `bench` packet capture             | Blocked: `tcpdump` is available via Nix but lacks CAP_NET_RAW, and passwordless sudo is unavailable.    |
| Repo qlog/keylog hooks             | Missing for both Rust/Quinn and C/MsQuic split paths.                                                   |

Concrete blocker: R1 needs qlog, decrypted packet capture, or equivalent frame-level evidence showing request frame, response frame, request FIN, terminal status, stream FIN, ACK timing, and reset/stop-sending behavior before any close-path fix is safe.

### C1 Replace Per-Stream Detached Threads

Blocked on worker-pool lifetime and shutdown design.

Concrete blocker: accepted stream tasks currently depend on stack-owned connection stream limiters and cleanup transfer through deferred calls. A worker queue must preserve limiter release, overload rejection, graceful shutdown drain, and stream close ownership without races or leaks.

### C2 Enable Native Frame Mode Before Bytes Accumulate

Blocked on native-RPC stream construction changes.

Concrete blocker: frame mode must be selected before MsQuic receive callbacks buffer bytes, but WebTransport/H3 control and connect streams must remain byte-oriented. The MsQuic layer also needs max-frame-size context at stream creation.

### C3 Encode Directly Into Transport Send Buffers

Blocked on async send-buffer ownership design.

Concrete blocker: direct encode or scatter/gather sends must keep frame/header/body memory valid until MsQuic send completion. This touches wire APIs and generated C response helpers.

### C7 Reduce Server Hot-Path Mutex Work

Blocked on route-freeze and counter semantics.

Concrete blocker: freezing routes requires explicit registration-after-start behavior, and request/task counters are tied to shutdown condition variables, so atomics are not a safe drop-in without a broader server lifecycle change.

### C8 Tune Timers, Backpressure, And MsQuic Settings

Blocked on backpressure and slow-reader evidence.

Current state: idle-timeout disabled paths already avoid per-message clock work in the hot receive/write paths.

Concrete blocker: exposing larger native MsQuic windows, ACK, congestion, or send-buffering profiles before bounded pending-send accounting would increase memory and DoS risk. WebTransport flow-control fields also need browser compatibility tests before changing advertised limits.

### G4 Replace Timeout Goroutines With Transport Deadlines

Blocked on deadline precedence and native parity.

Current state: transport deadlines exist for some initial request/drain reads, but client/server streaming receive helpers still use goroutine/timer fallback with default idle timeouts.

Concrete blocker: a safe replacement must preserve precedence between context deadline, stream idle timeout, cancellation, and terminal status, and must include MsQuic/native timed-read behavior.

### G7 Tune QUIC Flow-Control Windows For Concurrency

Blocked on public memory-budget API design.

Current state: Go derives receive windows from frame size, stream concurrency, and max body size with bounded caps.

Concrete blocker: exposing wider receive windows requires a memory/DoS budget model plus slow-reader and overload tests before defaults or knobs can safely change.

### R3 Replace `async_trait` `MessageStream::next` Hot Path

Blocked on public API compatibility.

Concrete blocker: replacing `MessageStream::next` with `poll_next` or a GAT-based trait affects generated service APIs and wrappers. A future safe sub-slice is an optional ready-drain method for known nonblocking internal streams, backed by allocation profiles and stream correctness tests.

### R10 Share Quinn And WebTransport Framed-Stream Logic

Blocked on transport-specific close/reset semantics.

Concrete blocker: the duplicated read/write loops are similar, but they are currently coupled to transport-specific stream finish, reset, cancellation, and WebTransport interoperability behavior. Extract only after transport-specific tests cover those semantics.

### J4 Reduce C-to-JS And JS-to-C Buffer Copies

Blocked on explicit native memory ownership APIs.

Concrete blocker: inbound frame bodies may point inside `_body_owner`, so external ArrayBuffer finalizers need a C helper that transfers the correct owner allocation. Outbound zero-copy also needs JS buffer references held until MsQuic send completion.

### J9 Long-Term Native Completion Redesign

Blocked on evented native completion/lifetime design.

Concrete blocker: the C APIs used by the Node addon are synchronous/blocking today. Replacing per-operation `napi_async_work` requires a native completion source or persistent poller while preserving close/GC while pending, cancellation, terminal status precedence, and partial stream behavior.
