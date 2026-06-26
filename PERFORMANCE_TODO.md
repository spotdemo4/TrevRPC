# TrevRPC Performance TODO

Generated: 2026-06-26

Scope: improve latency and throughput for TrevRPC clients and servers across `trevrpc-c`, `trevrpc-go`, `trevrpc-js`, and `trevrpc-rust`.

This is an implementation plan for a future agent. No new benchmarks were run while creating this file. Use the current baseline in `wiki/Benchmarks.md` before changing code, and update that file whenever benchmark results are produced.

## Baseline Snapshot

Source: `wiki/Benchmarks.md`, split RPC run generated 2026-06-26T12:19:54Z and WebTransport run generated 2026-06-25T17:46:35Z.

Key split RPC observations:

| Area         | Current signal                                                                                                                                                                                |
| ------------ | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| C / MsQuic   | Leads most split throughput rows: 6.34M server-stream client-axis msg/s, 5.72M client-stream client-axis msg/s, 5.66M server-stream server-axis msg/s, 6.27M client-stream server-axis msg/s. |
| Go / MsQuic  | Best split server-axis unary and simple stream latency: 79.359 us unary, 80.932 us server-stream, 95.152 us client-stream. Throughput lags C and often quic-go.                               |
| Go / quic-go | Usually slower latency than Go/MsQuic, but better throughput than Go/MsQuic in several stream rows. UDP receive-buffer warnings may suppress throughput.                                      |
| Rust / Quinn | Competitive WebTransport browser latency and some split throughput rows, but server-axis `bidi_stream_latency` is a severe outlier at 26,527 us/op.                                           |
| JS / MsQuic  | Slowest split rows and weakest WebTransport throughput, especially browser client-stream and bidi throughput. Native addon build and JS/native boundary overhead are likely major causes.     |

Key WebTransport observations:

| Area               | Current signal                                                                                                                                        |
| ------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------- |
| Browser latency    | Rust / Quinn leads unary and stream latency; C / MsQuic is very close.                                                                                |
| Browser throughput | C / MsQuic leads server-stream and bidi throughput. Rust leads client-stream throughput.                                                              |
| JS server          | Latency is acceptable but throughput has large gaps: 55k client-stream msg/s and 40k bidi msg/s versus roughly 0.5M to 1M+ for other implementations. |

## Measurement Rules

Use these rules for every implementation task below.

1. Build outside measured commands. For serious comparisons, use `ssh bench` for measurement and keep compile, install, sync, and warmup outside the timed command.
2. Record before/after rows, command, timestamp, relevant environment variables, commit/tree state, and anomalies in `wiki/Benchmarks.md` whenever a benchmark is run.
3. Prefer focused split runs while iterating, then a full `bench/run_rpc_split.sh` or `bench/run_webtransport.sh` before claiming broad wins.
4. Run allocation and scheduler profiles in addition to wall-clock measurements. Suggested tools: C `perf stat` and allocator profiling, Go `-benchmem`/pprof, Node `--prof` or native heap profiles, Rust `cargo bench` plus allocation profiling.
5. Preserve correctness under cancellation, deadlines, stream resets, graceful shutdown, overload, and partial streams. Many hot-path changes touch close and lifetime semantics.
6. Finish each code change with `nix fmt` and `nix flake check`. Intent-to-add new files before `nix flake check` so Nix sees them.

## Phase 0: Benchmark And Profiling Groundwork

These tasks should happen before or alongside runtime changes because they affect how wins are interpreted.

### P0.1 Build JS Native Addon In Release Mode

Files:

| File                        | Current issue                                                                               |
| --------------------------- | ------------------------------------------------------------------------------------------- |
| `trevrpc-js/package.json`   | `build:native` does not pass `-DCMAKE_BUILD_TYPE=Release`.                                  |
| `bench/run_rpc_split.sh`    | JS native build invokes `npm --prefix trevrpc-js run build:native` without forcing Release. |
| `bench/run_webtransport.sh` | Same Release-mode gap for JS WebTransport benchmark builds.                                 |

Plan:

| Step            | Detail                                                                                                      |
| --------------- | ----------------------------------------------------------------------------------------------------------- |
| Change          | Make JS native builds default to Release, while preserving an explicit debug override.                      |
| Verify          | Confirm `trevrpc-js/build/native/CMakeCache.txt` has `CMAKE_BUILD_TYPE:STRING=Release` after a clean build. |
| Benchmark       | Focus JS-only split and WebTransport rows before other JS optimizations.                                    |
| Expected impact | Very high if current JS rows include an unoptimized native addon and embedded `trevrpc-c`.                  |
| Risk            | Low. Watch for hidden debug-only assumptions.                                                               |

Status 2026-06-26: implemented. `trevrpc-js/package.json` now defaults `build:native` to `-DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE:-Release}`, and both split benchmark wrappers pass their `CMAKE_BUILD_TYPE` through to the JS native build. Local verification confirmed `trevrpc-js/build/native/CMakeCache.txt` contained `CMAKE_BUILD_TYPE:STRING=Release`. A focused split rerun on `bench` did not show a material JS improvement, so remaining JS gaps should be treated as JS/native boundary, batching, and allocation work rather than a Debug-addon artifact.

### P0.2 Separate Transport Cost From Generated/Protobuf Cost

Files:

| Implementation | References                                                                                                                                            |
| -------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------- |
| C              | `trevrpc-c/bench/rpc_comparison_bench.c` pre-encodes and batches many benchmark bodies.                                                               |
| Go             | `trevrpc-go/rpc_comparison_test.go`, `trevrpc-go/cmd/trevrpc-rpc-split-bench/main.go` use high-level streams and some preallocated benchmark replies. |
| JS             | `trevrpc-js/bench/rpc_split_native.js`, `trevrpc-js/src/framing.js` include protobuf encode/decode and object conversion in reported paths.           |
| Rust           | `trevrpc-rust/examples/rpc_split_bench.rs` uses generated service APIs and allocates/clones messages in hot paths.                                    |

Plan:

| Step              | Detail                                                                                                                  |
| ----------------- | ----------------------------------------------------------------------------------------------------------------------- |
| Add rows          | Add optional raw/pre-encoded transport rows next to generated API rows, not instead of them.                            |
| Payload sweep     | Add 0-byte, small, and larger payload sweeps for unary and streaming.                                                   |
| Concurrency sweep | Add single stream, multiple concurrent streams, and multiple client sessions.                                           |
| Report            | Keep generated API rows as user-facing ergonomics benchmarks and raw rows as transport/framing isolation.               |
| Expected impact   | High benchmark clarity. May reveal that some language gaps are protobuf/object overhead rather than transport overhead. |

### P0.3 Fix Invalid C Runtime Microbenchmark

File: `trevrpc-c/bench/runtime_bench.c`

Issue: the benchmark duplicates a partial internal `trevrpc_msquic_stream` layout and passes it into runtime internals. That makes results undefined and not reliable.

Plan:

| Step            | Detail                                                                                                   |
| --------------- | -------------------------------------------------------------------------------------------------------- |
| Change          | Replace the fake layout with a stable internal test seam, shared internal header, or loopback benchmark. |
| Verify          | Run the benchmark under ASAN/UBSAN or remove it from performance decision-making until fixed.            |
| Expected impact | Benchmark correctness, not direct runtime speed.                                                         |

## Cross-Runtime Priorities

These themes recur in multiple implementations and should guide design choices.

| Priority | Theme                                          | Why it matters                                                                                                                          | Implementations |
| -------: | ---------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------- | --------------- |
|        1 | Remove per-RPC and per-message scheduler work  | Thread/goroutine/task creation dominates sub-200 us latency paths and small-message throughput.                                         | C, Go, JS, Rust |
|        2 | Reduce frame/body copies                       | Current paths often encode into a temporary frame, copy to a transport send buffer, then copy again across FFI/runtime boundaries.      | C, Go, JS, Rust |
|        3 | Batch streaming writes and reads               | Throughput rows are mostly one long stream. Batching cuts syscalls, cgo/N-API calls, allocations, and wakeups.                          | C, Go, JS, Rust |
|        4 | Fast-path no-timeout/no-cancellation calls     | Benchmarks and many production hot paths have no deadline or stream idle timeout. Avoid allocating timers/controllers/goroutines there. | Go, JS, Rust, C |
|        5 | Separate graceful close from abort/cancel      | Abort-on-close can add reset traffic and distort latency, especially after successful FIN/status.                                       | C, Rust, Go, JS |
|        6 | Tune transport windows and ACK behavior safely | Some anomalies look like delayed ACK/FIN behavior or conservative flow-control windows.                                                 | Rust, Go, C     |
|        7 | Add backpressure before maximizing batching    | Batching and disabled send buffering can increase memory/tail latency unless bounded.                                                   | C, Go, JS, Rust |

## C Track

Current strength: fastest split throughput and best or near-best native latency. Main opportunities are reducing per-stream scheduling and copies to move from good to consistently dominant, especially bidi latency.

### C1 Replace Per-Stream Detached Threads

References:

| File                      | Functions                                                                              |
| ------------------------- | -------------------------------------------------------------------------------------- |
| `trevrpc-c/src/trevrpc.c` | `trevrpc_conn_thread`, `trevrpc_stream_thread`, `trevrpc_server_start_connection_task` |

Current behavior: each accepted stream allocates `trevrpc_stream_task`, creates a detached `pthread`, handles one stream/RPC, then exits.

Plan:

| Item            | Detail                                                                                                                                     |
| --------------- | ------------------------------------------------------------------------------------------------------------------------------------------ |
| Change          | Introduce a fixed worker pool or per-server work queue. Connection accept loops enqueue stream tasks; workers run `trevrpc_handle_stream`. |
| Backpressure    | Bound queue length using existing max request/stream limits, and reject with status when saturated.                                        |
| Shutdown        | Preserve graceful drain semantics and per-connection stream limiter release.                                                               |
| Expected impact | Very high for latency and throughput under load; should reduce context switches and allocator pressure.                                    |
| Risk            | Worker starvation, shutdown races, and stream limiter leaks.                                                                               |
| Verify          | C-only split rows, `perf stat` context switches/task-clock, shutdown/overload tests, `trevrpc_msquic_transport_test`.                      |

### C2 Enable Native Frame Mode Before Bytes Accumulate

References:

| File                             | Functions                                                                                                                          |
| -------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------- |
| `trevrpc-c/src/trevrpc_msquic.c` | receive callback frame parser, `trevrpc_msquic_stream_enable_frame_mode_locked`, `trevrpc_msquic_stream_append_frame_bytes_locked` |
| `trevrpc-c/src/trevrpc.c`        | `trevrpc_handle_stream`, `trevrpc_stream_recv`, `trevrpc_stream_recv_ready`                                                        |

Current behavior: streams can first receive generic byte chunks, then switch into frame mode and reparse/copy buffered chunks.

Plan:

| Item            | Detail                                                                                                                                            |
| --------------- | ------------------------------------------------------------------------------------------------------------------------------------------------- |
| Change          | Add an RPC-native stream construction path that enters frame mode before application data arrives. Keep WebTransport/H3 byte-mode paths separate. |
| Expected impact | High for native latency and stream throughput, especially larger frames and server receive paths.                                                 |
| Risk            | WebTransport control/connect stream parsing must stay byte-oriented.                                                                              |
| Verify          | Native split benchmarks, large payload tests, WebTransport regression tests.                                                                      |

### C3 Encode Directly Into Transport Send Buffers

References:

| File                                          | Functions                                                                                              |
| --------------------------------------------- | ------------------------------------------------------------------------------------------------------ |
| `trevrpc-c/src/trevrpc_wire.c`                | `trevrpc_wire_encode_request_view`, `trevrpc_wire_encode_response`, `trevrpc_wire_encode_stream_frame` |
| `trevrpc-c/src/trevrpc_msquic.c`              | `trevrpc_msquic_stream_write`, `trevrpc_msquic_stream_write_fin`, message-frame write helpers          |
| `trevrpc-c/tools/protoc-gen-trevrpc-c/main.c` | generated pack/respond helpers                                                                         |

Current behavior: many paths allocate a wire frame, then allocate/copy into a transport send buffer. Server unary often copies protobuf bodies into `trevrpc_response` first.

Plan:

| Item            | Detail                                                                                                                                     |
| --------------- | ------------------------------------------------------------------------------------------------------------------------------------------ |
| Change          | Add APIs to reserve/acquire a send buffer and encode directly into it, or use MsQuic scatter/gather `QUIC_BUFFER`s for header/body pieces. |
| Lifetime        | Make async send ownership explicit. The buffer must remain valid until MsQuic send-complete.                                               |
| Expected impact | High for unary latency and payload-heavy throughput.                                                                                       |
| Risk            | Memory lifetime bugs, failed send rollback, and more complex generated code.                                                               |
| Verify          | `trevrpc_wire_bench`, ASAN/UBSAN, unary latency, payload sweep.                                                                            |

### C4 Send Terminal Frames With FIN

References:

| File                             | Functions                                                                                                 |
| -------------------------------- | --------------------------------------------------------------------------------------------------------- |
| `trevrpc-c/src/trevrpc.c`        | `trevrpc_server_write_response`, `trevrpc_server_write_stream_status`, `trevrpc_call_write_stream_finish` |
| `trevrpc-c/src/trevrpc_msquic.c` | `trevrpc_msquic_stream_write_fin`                                                                         |

Plan:

| Item            | Detail                                                                                                                                                     |
| --------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Change          | Use `trevrpc_msquic_stream_write_fin` or equivalent for final unary response/status frames instead of write plus separate graceful shutdown when possible. |
| Expected impact | Medium latency win by reducing MsQuic calls, wakeups, and possibly packetization.                                                                          |
| Risk            | Need exact behavior when final write fails after send-closed state changes.                                                                                |
| Verify          | Unary latency, stream latency, close/status tests.                                                                                                         |

### C5 Separate Graceful Close From Abort

References:

| File                             | Functions                                                                   |
| -------------------------------- | --------------------------------------------------------------------------- |
| `trevrpc-c/src/trevrpc.c`        | `trevrpc_stream_close_raw`, `trevrpc_stream_close`, `trevrpc_stream_cancel` |
| `trevrpc-c/src/trevrpc_msquic.c` | `trevrpc_msquic_stream_close`, `trevrpc_msquic_stream_abort`                |

Current behavior: owned MsQuic streams are aborted in `trevrpc_stream_close_raw`, even after a successful FIN/status path.

Plan:

| Item            | Detail                                                                                                             |
| --------------- | ------------------------------------------------------------------------------------------------------------------ |
| Change          | Make `close` graceful by default after success. Keep explicit `cancel`/`abort` APIs for error and abandoned paths. |
| Expected impact | Medium latency/cleanup win and high correctness value.                                                             |
| Risk            | Graceful close may wait on misbehaving peers unless bounded by cancellation/deadline semantics.                    |
| Verify          | Add tests proving successful stream close does not cause peer abort; rerun streaming latency and shutdown tests.   |

### C6 Add Receive Batching API

References:

| File                           | Functions                                          |
| ------------------------------ | -------------------------------------------------- |
| `trevrpc-c/src/trevrpc.c`      | `trevrpc_stream_recv`, `trevrpc_stream_recv_ready` |
| `trevrpc-c/src/trevrpc_wire.c` | `trevrpc_wire_decode_stream_frame_take`            |

Plan:

| Item            | Detail                                                                                                                           |
| --------------- | -------------------------------------------------------------------------------------------------------------------------------- |
| Change          | Add `trevrpc_stream_recv_messages` or a lower-level frame batch API that drains already-ready frames without a call per message. |
| Expected impact | High for server-stream, client-stream, and bidi throughput.                                                                      |
| Risk            | API complexity and fairness if one consumer drains too large a batch.                                                            |
| Verify          | Stream throughput rows and allocation profiles.                                                                                  |

### C7 Reduce Server Hot-Path Mutex Work

References:

| File                      | Functions                                                                                |
| ------------------------- | ---------------------------------------------------------------------------------------- |
| `trevrpc-c/src/trevrpc.c` | `trevrpc_server_find_method`, server option/observer/metrics snapshots, request counters |

Plan:

| Item            | Detail                                                                                                            |
| --------------- | ----------------------------------------------------------------------------------------------------------------- |
| Change          | Freeze route table after start or use copy-on-write/read-mostly data. Use atomics for simple counters where safe. |
| Expected impact | Medium single-client, high concurrent-client impact.                                                              |
| Risk            | Registration/update semantics must be explicit.                                                                   |
| Verify          | Multi-client/concurrent-stream benchmark and route-count sweep.                                                   |

### C8 Tune Timers, Backpressure, And MsQuic Settings

References:

| File                             | Functions                                  |
| -------------------------------- | ------------------------------------------ |
| `trevrpc-c/src/trevrpc.c`        | stream idle timer helpers, server options  |
| `trevrpc-c/src/trevrpc_msquic.c` | settings setup and send buffering behavior |

Plan:

| Item            | Detail                                                                                                                                                                                   |
| --------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Change          | Avoid per-message clock calls when idle timeout is disabled. Add bounded send queue/backpressure. Expose separate latency/throughput MsQuic profiles for native RPC versus WebTransport. |
| Expected impact | Medium throughput and tail-latency improvement; high reliability under slow-reader load.                                                                                                 |
| Risk            | Backpressure can reduce peak synthetic throughput if too conservative.                                                                                                                   |
| Verify          | Slow-reader tests, long-stream memory profiling, default-timeout versus disabled-timeout benchmarks.                                                                                     |

## Go Track

Current strength: best server latency in several split rows. Main opportunities are removing goroutine/channel overhead and batching server streaming paths.

### G1 Remove Per-Message Goroutine/Channel In Request Streaming

References:

| File                   | Functions                                                            |
| ---------------------- | -------------------------------------------------------------------- |
| `trevrpc-go/quic.go`   | `writeRequestBodyFrames`, `recvRequestBody`                          |
| `trevrpc-go/server.go` | `recvByteWithTimeout`, `limitedByteStream.trevrpcContextCancelsRecv` |

Plan:

| Item            | Detail                                                                                                                                                                 |
| --------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Change          | Direct-read when the stream advertises context-cancellable `Recv`, not only when it is nonblocking. Make `MessagePipe` and internal wrappers advertise that correctly. |
| Expected impact | High for client-stream and bidi throughput across quic-go, MsQuic, and WebTransport.                                                                                   |
| Risk            | Treating arbitrary user streams as cancellable can leak goroutines or block shutdown.                                                                                  |
| Verify          | Go split client-stream and bidi rows, `go test -bench . -benchmem`, cancellation/deadline tests.                                                                       |

Status 2026-06-26: implemented with a narrower safety shape than the original note. Request-body writers wrap streams once with `closeStreamOnContext`, then `recvRequestBody` direct-reads streams that are nonblocking or advertise context-cancellable `Recv`. This avoids the per-message goroutine/channel fallback for generated request streams without trusting arbitrary user streams as cancellable. Focused split results showed large client-stream and bidi throughput gains for both Go `quic-go` and Go/MsQuic; see `wiki/Benchmarks.md`.

### G2 Fix WebTransport Response Stream Cancellation Fast Path

References:

| File                         | Functions                                              |
| ---------------------------- | ------------------------------------------------------ |
| `trevrpc-go/webtransport.go` | `webTransportResponseStream`                           |
| `trevrpc-go/client.go`       | `StreamingCall`, `recvOptimizedFrameFieldsWithTimeout` |

Plan:

| Item            | Detail                                                                                                                                                                    |
| --------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Change          | Mirror the quic-go response stream behavior: install context cancellation on native WebTransport streams and implement the `trevrpcContextCancelsRecv` marker where safe. |
| Expected impact | High for Go WebTransport streaming throughput and latency.                                                                                                                |
| Risk            | Must preserve browser/native stream cancellation semantics.                                                                                                               |
| Verify          | Native WebTransport tests and WebTransport split rows.                                                                                                                    |

### G3 Batch Server Response Frames

References:

| File                    | Functions                                    |
| ----------------------- | -------------------------------------------- |
| `trevrpc-go/server.go`  | `serverResponseStream.trevrpcWriteNextFrame` |
| `trevrpc-go/framing.go` | `writeMessageStreamFrames`                   |
| `trevrpc-go/msquic.go`  | `trevrpcWriteMessageStreamFrames`            |

Current behavior: request-body writing batches, but server response streaming writes one frame at a time.

Plan:

| Item            | Detail                                                                                                           |
| --------------- | ---------------------------------------------------------------------------------------------------------------- |
| Change          | Add a batched `trevrpcWriteNextFrames` path for nonblocking response streams, capped by message and byte limits. |
| Expected impact | High for server-stream throughput and JS/Go/Rust comparison fairness.                                            |
| Risk            | Batching interactive streams can increase latency; gate on nonblocking/ready streams.                            |
| Verify          | `server_stream_throughput`, `bidi_stream_throughput`, `-benchmem`.                                               |

### G4 Replace Timeout Goroutines With Transport Deadlines

References:

| File                                 | Functions                                                     |
| ------------------------------------ | ------------------------------------------------------------- |
| `trevrpc-go/client.go`               | `recvFrameWithTimeout`, `recvOptimizedFrameFieldsWithTimeout` |
| `trevrpc-go/server.go`               | `recvByteWithTimeout`                                         |
| `trevrpc-c/include/trevrpc_msquic.h` | native timed frame reads                                      |

Plan:

| Item            | Detail                                                                                                                                         |
| --------------- | ---------------------------------------------------------------------------------------------------------------------------------------------- |
| Change          | Use `SetReadDeadline` for quic-go/WebTransport where available and native timed reads for MsQuic instead of per-recv goroutine/timer fallback. |
| Expected impact | High for default production streaming.                                                                                                         |
| Risk            | Deadline clearing and precedence between context deadline and stream idle timeout.                                                             |
| Verify          | Timeout tests and streaming benchmarks with default options, not only disabled timeouts.                                                       |

### G5 Reduce Empty Metadata, OK Status, And Option Allocations

References:

| File                                           | Functions                                      |
| ---------------------------------------------- | ---------------------------------------------- |
| `trevrpc-go/client.go`                         | response validation and stream receive helpers |
| `trevrpc-go/wire.go`                           | frame constructors/encoders                    |
| `trevrpc-go/status.go`                         | `OK`, status helpers                           |
| `trevrpc-go/context.go`                        | metadata/context setup                         |
| `trevrpc-go/cmd/protoc-gen-trevrpc-go/main.go` | generated `mergeOptions`                       |

Plan:

| Item            | Detail                                                                                                                                                          |
| --------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Change          | Use nil metadata for empty cases, avoid cloning empty maps, add shared/fast OK status frames, and avoid option-slice allocation when base or override is empty. |
| Expected impact | Medium to high for unary latency and allocation count.                                                                                                          |
| Risk            | Nil-vs-empty map API behavior and caller mutation of returned slices.                                                                                           |
| Verify          | Wire golden tests, metadata tests, generated-code tests, unary `-benchmem`.                                                                                     |

### G6 Optimize MsQuic Cgo Write/Batch Path

References:

| File                             | Functions                                                                                |
| -------------------------------- | ---------------------------------------------------------------------------------------- |
| `trevrpc-go/msquic.go`           | `trevrpcWriteFrame`, `trevrpcWriteMessageStreamFrame`, `trevrpcWriteMessageStreamFrames` |
| `trevrpc-c/src/trevrpc_msquic.c` | C message-frame writers                                                                  |

Plan:

| Item            | Detail                                                                                                                                                             |
| --------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Change          | Avoid packing Go bodies into a temporary contiguous buffer before C copies again. Add pointer/length vector or scatter/gather APIs that satisfy cgo pointer rules. |
| Expected impact | Medium to high for Go/MsQuic stream throughput.                                                                                                                    |
| Risk            | cgo pointer lifetime rules and potential latency regressions if send buffering is changed.                                                                         |
| Verify          | Go MsQuic split rows, cgo pointer checks, `-benchmem`.                                                                                                             |

### G7 Tune QUIC Flow-Control Windows For Concurrency

References:

| File                             | Functions                   |
| -------------------------------- | --------------------------- |
| `trevrpc-go/quic_config.go`      | default transport config    |
| `trevrpc-go/transport_limits.go` | receive window calculations |

Plan:

| Item            | Detail                                                                                                                   |
| --------------- | ------------------------------------------------------------------------------------------------------------------------ |
| Change          | Separate max frame size from expected concurrent receive window. Expose safe knobs for parallel large-message workloads. |
| Expected impact | Medium for concurrent streams and larger payloads.                                                                       |
| Risk            | Memory and DoS exposure.                                                                                                 |
| Verify          | Add parallel-stream benchmark and overload tests.                                                                        |

## JS Track

Current weakness: largest gaps in both split and WebTransport throughput. Prioritize native build mode, JS/native boundary cost, and streaming batch APIs.

### J1 Build Native Addon In Release Mode

This is the same task as `P0.1` and should be completed first for JS. Re-baseline JS before judging other JS work.

Status 2026-06-26: complete via `P0.1`. Focused JS split rows were re-run with a Release addon; no material improvement was observed, so continue with `J3`, `J5`, or `J6` before drawing further conclusions about JS runtime ceilings.

### J2 Avoid Unconditional AbortController And Native Cancellation Allocation

References:

| File                       | Functions                                           |
| -------------------------- | --------------------------------------------------- |
| `trevrpc-js/src/client.js` | `callAbortScope`, timeout wrappers                  |
| `trevrpc-js/src/node.js`   | `NodeTransport.call`, `NodeTransport.streamingCall` |

Current behavior: calls allocate an `AbortController` and native cancellation path even when no user signal or deadline exists.

Plan:

| Item            | Detail                                                                                                                                                    |
| --------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Change          | Fast-path calls without signal/deadline using the original options and no native cancellation object. Lazily allocate only when cancellation is possible. |
| Expected impact | High for unary and small-stream latency.                                                                                                                  |
| Risk            | Cancellation/deadline regressions.                                                                                                                        |
| Verify          | `node --test trevrpc-js/test/*.test.js`, profile native unary/client unary.                                                                               |

### J3 Add Native Body-Batch Receive Fast Path

References:

| File                               | Functions                                                                |
| ---------------------------------- | ------------------------------------------------------------------------ |
| `trevrpc-js/src/client.js`         | `responseMessageStream`, `nextBodyBatchWithTimeout` usage                |
| `trevrpc-js/src/node.js`           | `NativeResponseFrameStream`, `recvMany` handling                         |
| `trevrpc-js/native/trevrpc_node.c` | `recv_many_from_stream`, `stream_frame_to_js`, `stream_frame_list_to_js` |

Current behavior: native streams return JS frame objects per message; browser WebTransport already has a body-batch concept.

Plan:

| Item            | Detail                                                                                                                                                                                  |
| --------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Change          | Add native `recvBodyBatch` or iterator `nextBodyBatch()` returning message bodies plus optional terminal status. Avoid constructing per-message frame objects for plain message frames. |
| Expected impact | Very high for JS server-stream and bidi throughput.                                                                                                                                     |
| Risk            | Terminal status, metadata, and error semantics must match existing frame path.                                                                                                          |
| Verify          | Native streaming tests and split `server_stream_throughput`/`bidi_stream_throughput`.                                                                                                   |

### J4 Reduce C-to-JS And JS-to-C Buffer Copies

References:

| File                               | Functions                                                                                         |
| ---------------------------------- | ------------------------------------------------------------------------------------------------- |
| `trevrpc-js/native/trevrpc_node.c` | `response_to_js`, `stream_frame_to_js`, `native_stream_send_messages`, `native_call_send_message` |
| `trevrpc-c/src/trevrpc_wire.c`     | frame/body ownership on decode                                                                    |

Plan:

| Item            | Detail                                                                                                                                                                                 |
| --------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Change          | Use external ArrayBuffers with finalizers for inbound bodies where lifetime is clear. For outbound batches, pass pointer/length vectors or encode directly into C/MsQuic send buffers. |
| Expected impact | High for payload throughput, medium for tiny messages.                                                                                                                                 |
| Risk            | Native memory ownership bugs and Buffer aliasing surprises.                                                                                                                            |
| Verify          | Buffer alias tests, ASAN where possible, payload sweep.                                                                                                                                |

### J5 Batch Node Server Sends And Receives

References:

| File                               | Functions                                                              |
| ---------------------------------- | ---------------------------------------------------------------------- |
| `trevrpc-js/src/node.js`           | `NodeServerCall.sendMessage`, `completeDefault`, `NodeServerCall.recv` |
| `trevrpc-js/native/trevrpc_node.c` | native call send/recv functions                                        |
| `trevrpc-c/include/trevrpc.h`      | C batch send APIs                                                      |

Plan:

| Item            | Detail                                                                                                                                                                                   |
| --------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Change          | Add `NativeCall.sendMessages`, `NodeServerCall.sendMany`, and optionally `recvMany`/body-batch helpers. Update generated server helpers to use batches for nonblocking iterable results. |
| Expected impact | Very high for JS server-axis and WebTransport JS-server throughput.                                                                                                                      |
| Risk            | Backpressure and memory growth if batch sizes are too large.                                                                                                                             |
| Verify          | JS server split rows and WebTransport JS server rows.                                                                                                                                    |

### J6 Expose Batch Knobs And Fix Benchmark Send Paths

References:

| File                                        | Functions                                                     |
| ------------------------------------------- | ------------------------------------------------------------- |
| `trevrpc-js/src/node.js`                    | fixed `RecvManyBatchSize` and `SendManyBatchSize`             |
| `trevrpc-js/bench/rpc_split_native.js`      | client-stream and bidi send loops await single `send()` calls |
| `trevrpc-js/bench/rpc_comparison_native.js` | same pattern in native comparison benchmark                   |

Plan:

| Item            | Detail                                                                                                                               |
| --------------- | ------------------------------------------------------------------------------------------------------------------------------------ |
| Change          | Add options mirroring WebTransport batch knobs. Use `sendMany` in throughput benchmark paths while keeping single-send latency rows. |
| Expected impact | High for JS client-stream and bidi throughput.                                                                                       |
| Risk            | Benchmark comparability. Report single-send and batched variants separately if needed.                                               |
| Verify          | `client_stream_throughput`, `bidi_stream_throughput`, and latency rows to catch batching-induced latency regressions.                |

### J7 Reduce Option And Metadata Normalization Work

References:

| File                       | Functions                                                       |
| -------------------------- | --------------------------------------------------------------- |
| `trevrpc-js/src/client.js` | `createServiceClient`, request preparation, metadata validation |

Plan:

| Item            | Detail                                                                                                                               |
| --------------- | ------------------------------------------------------------------------------------------------------------------------------------ |
| Change          | Avoid duplicate option merges and metadata normalization when generated clients already merged static options and metadata is empty. |
| Expected impact | Medium for tiny unary/stream latency.                                                                                                |
| Risk            | Metadata validation semantics.                                                                                                       |
| Verify          | Metadata tests and unary profile.                                                                                                    |

### J8 Improve Browser WebTransport Batching Internals

References:

| File                             | Functions                               |
| -------------------------------- | --------------------------------------- |
| `trevrpc-js/src/framing.js`      | chunk-array parsing and frame splitting |
| `trevrpc-js/src/webtransport.js` | read/write batch loops                  |

Plan:

| Item            | Detail                                                                                                                                       |
| --------------- | -------------------------------------------------------------------------------------------------------------------------------------------- |
| Change          | Replace `shift()`-based queues with head-index deques, reduce `slice` allocations, and consider read-ahead only for throughput-mode streams. |
| Expected impact | Medium to high for browser throughput.                                                                                                       |
| Risk            | Read-ahead can hurt latency and memory.                                                                                                      |
| Verify          | `bench/run_webtransport.sh` with existing batch environment variables.                                                                       |

### J9 Long-Term Native Completion Redesign

References:

| File                               | Functions                                           |
| ---------------------------------- | --------------------------------------------------- |
| `trevrpc-js/native/trevrpc_node.c` | N-API async work queues for unary/start/recv/finish |

Plan:

| Item            | Detail                                                                                                                                     |
| --------------- | ------------------------------------------------------------------------------------------------------------------------------------------ |
| Change          | Replace per-operation `napi_async_work` threadpool jobs with evented native completions or a persistent native poller feeding JS promises. |
| Expected impact | High ceiling for concurrency and tail latency.                                                                                             |
| Risk            | High complexity; requires careful native lifetime/cancellation design.                                                                     |
| Verify          | Add concurrency benchmarks and test sensitivity to `UV_THREADPOOL_SIZE`.                                                                   |

## Rust Track

Current strength: best WebTransport latency and strong throughput potential. Main issues are async allocation/scheduling overhead and a severe server-axis bidi latency anomaly.

### R1 Investigate And Fix `server / bidi_stream_latency` Outlier

References:

| File                                       | Functions                                                                   |
| ------------------------------------------ | --------------------------------------------------------------------------- |
| `wiki/Benchmarks.md`                       | Rust / Quinn server-axis `bidi_stream_latency` = 26,527 us/op               |
| `trevrpc-c/bench/rpc_comparison_bench.c`   | `split_benchmark_bidi_stream_latency` half-closes after receiving one reply |
| `trevrpc-rust/src/quinn.rs`                | `handle_streaming_rpc`, `QuinnRequestStream`                                |
| `trevrpc-rust/examples/rpc_split_bench.rs` | `BidiReplies::next`                                                         |

Hypothesis: the C client sends one bidi request, waits for the reply, then half-closes and waits for terminal status. The Rust server cannot emit terminal OK until `QuinnRequestStream` observes request EOF. The 26.5 ms timing looks like delayed ACK/FIN timing.

Plan:

| Item             | Detail                                                                                                                                                                                                              |
| ---------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Experiment       | Change only benchmark shape in a scratch branch: half-close immediately after the single request before reading the reply. Compare Rust server-axis bidi latency.                                                   |
| Transport tuning | Test Quinn `ack_frequency_config` or lower max ACK delay if available in the pinned Quinn version.                                                                                                                  |
| Runtime fix      | If the issue is request EOF waiting, consider permitting bidi handlers to emit terminal status when response stream completes and request stream has been explicitly cancelled/drained according to protocol rules. |
| Expected impact  | Very high for the outlier; should move near other Rust stream latency rows if hypothesis is correct.                                                                                                                |
| Risk             | Benchmark semantics and bidi correctness. Do not hide real half-close requirements.                                                                                                                                 |
| Verify           | Rust-only server-axis split run and qlog/packet trace if the result remains ambiguous.                                                                                                                              |

Status 2026-06-26: investigated, not fixed. The split C benchmark was changed to half-close immediately after sending the single request, before reading the reply. A focused 10000-iteration Rust server-axis rerun still reported `bidi_stream_latency` around 26.46 ms, so benchmark half-close timing alone is not the root cause. Two C/MsQuic FIN experiments were tested and reverted: a final-message-with-FIN path was fast at 1000 iterations but unstable at 10000 iterations, and a global zero-length-FIN graceful shutdown path caused failures and did not fix the outlier. Remaining follow-up: capture qlog/packet traces and design a robust C-side final-write-with-FIN path or transport ACK/FIN tuning that preserves graceful shutdown and stream reset semantics.

### R2 Remove Per-Handler `tokio::spawn` On Server Hot Path

References:

| File                         | Functions                                                                 |
| ---------------------------- | ------------------------------------------------------------------------- |
| `trevrpc-rust/src/server.rs` | `run_handler_with_deadline`, `handle_request`, `handle_streaming_request` |

Current behavior: every handler future is wrapped in `tokio::spawn(with_deadline(...))`, adding scheduler and allocation overhead even for trivial handlers.

Plan:

| Item            | Detail                                                                                                                                                          |
| --------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Change          | Await handler futures inline and apply deadlines directly. Preserve panic isolation with `FutureExt::catch_unwind` or a narrower optional panic-isolation mode. |
| Expected impact | High latency win, medium throughput win.                                                                                                                        |
| Risk            | Panic isolation and cancellation semantics change.                                                                                                              |
| Verify          | Server tests covering panic handlers, deadline tests, split unary/stream latency.                                                                               |

### R3 Replace `async_trait` `MessageStream::next` Hot Path

References:

| File                         | Functions                                                   |
| ---------------------------- | ----------------------------------------------------------- |
| `trevrpc-rust/src/stream.rs` | `MessageStream` trait and wrappers                          |
| `trevrpc-rust/src/quinn.rs`  | `QuinnRequestStream`, `QuinnResponseStream`, batching loops |
| `trevrpc-rust/src/server.rs` | `ServerResponseStream`, `LimitedStream`                     |

Current behavior: `async_trait` boxes a future for every `next()` call. Streaming throughput calls this per message, often through multiple wrapper layers.

Plan:

| Item            | Detail                                                                                                                              |
| --------------- | ----------------------------------------------------------------------------------------------------------------------------------- |
| Change          | Move to `poll_next`/`Stream`-style polling or a GAT-based trait to avoid per-message future allocation.                             |
| Compatibility   | This is API-affecting. Consider keeping a compatibility adapter for user streams while optimizing internal generated streams first. |
| Expected impact | Very high for stream throughput, medium for stream latency.                                                                         |
| Risk            | API churn and generated code changes.                                                                                               |
| Verify          | Allocation profile, all stream throughput rows, generated service tests.                                                            |

### R4 Decode Stream Message Frames Without Copying Body Twice

References:

| File                               | Functions                               |
| ---------------------------------- | --------------------------------------- |
| `trevrpc-rust/src/framing.rs`      | `decode_stream_frame_body`              |
| `trevrpc-rust/src/quinn.rs`        | `read_stream_frame_or_eof`, `read_body` |
| `trevrpc-rust/src/webtransport.rs` | WebTransport frame read path            |

Current behavior: `read_body` allocates a frame-body `Vec`, then `decode_stream_frame_body` copies the protobuf body into a second `Vec`.

Plan:

| Item            | Detail                                                                                                                |
| --------------- | --------------------------------------------------------------------------------------------------------------------- |
| Change          | Add an owned decoder that moves/splits the body out of the frame buffer, or use `Bytes` for shared zero-copy slicing. |
| Expected impact | High for stream throughput and allocation pressure.                                                                   |
| Risk            | Unknown fields, status frames, and metadata parsing need careful handling.                                            |
| Verify          | Wire golden vectors, fuzz tests, stream throughput rows.                                                              |

### R5 Reuse Write Buffers And Reduce Stream-Frame Copies

References:

| File                               | Functions                                                     |
| ---------------------------------- | ------------------------------------------------------------- |
| `trevrpc-rust/src/framing.rs`      | `encode_message_stream_frame`, `encode_message_stream_frames` |
| `trevrpc-rust/src/quinn.rs`        | `write_message_stream_frame`, `write_message_stream_frames`   |
| `trevrpc-rust/src/webtransport.rs` | matching WebTransport writers                                 |

Plan:

| Item            | Detail                                                                                                                      |
| --------------- | --------------------------------------------------------------------------------------------------------------------------- |
| Change          | Reuse per-stream batch buffers. Where practical, write header/tag/len/body pieces without constructing a copied full frame. |
| Expected impact | Medium to high for throughput and allocator variance.                                                                       |
| Risk            | Too many split writes can regress throughput; keep batch writes coalesced.                                                  |
| Verify          | Stream throughput rows and allocation profiles.                                                                             |

### R6 Reduce Noop Metrics And Route Lookup Allocations

References:

| File                         | Functions                                                                                                                     |
| ---------------------------- | ----------------------------------------------------------------------------------------------------------------------------- |
| `trevrpc-rust/src/server.rs` | `RequestContext::new`, `RpcStarted`, `RpcFinished`, `handle_request`, `handle_streaming_request`, `finish_streaming_response` |

Current behavior: service/method strings and metric events are cloned/allocated even when metrics is `NoopMetrics`.

Plan:

| Item            | Detail                                                                                                                                                                 |
| --------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Change          | Use borrowed metric fields, borrowed route lookup keys, or skip event construction for `NoopMetrics`. Avoid cloning service/method unless an observer needs ownership. |
| Expected impact | Medium for unary/stream latency and allocations.                                                                                                                       |
| Risk            | Metrics API changes.                                                                                                                                                   |
| Verify          | Unary latency, allocation profile, metrics tests.                                                                                                                      |

### R7 Expose And Tune Quinn Latency Knobs

References:

| File                                       | Functions                                                                      |
| ------------------------------------------ | ------------------------------------------------------------------------------ |
| `trevrpc-rust/src/quinn.rs`                | `apply_transport_limits`, `configure_server_config`, `configure_client_config` |
| `trevrpc-rust/examples/rpc_split_bench.rs` | endpoint config helpers                                                        |

Plan:

| Item            | Detail                                                                                                                                   |
| --------------- | ---------------------------------------------------------------------------------------------------------------------------------------- |
| Change          | Expose max idle timeout, keepalive interval, send window, ACK frequency/max ACK delay, and congestion profile where Quinn supports them. |
| Expected impact | Medium generally, potentially high for the bidi latency outlier.                                                                         |
| Risk            | ACK traffic and memory/window tradeoffs.                                                                                                 |
| Verify          | Rust split rows, qlog traces, concurrency/payload sweeps.                                                                                |

### R8 Avoid Writer Task For Simple Streaming Requests

References:

| File                               | Functions                                           |
| ---------------------------------- | --------------------------------------------------- |
| `trevrpc-rust/src/quinn.rs`        | `Client::streaming_call`, `write_streaming_request` |
| `trevrpc-rust/src/webtransport.rs` | equivalent client streaming path                    |

Current behavior: every streaming call spawns a request-writer task, including server-streaming calls with empty/nonblocking request bodies.

Plan:

| Item            | Detail                                                                                                         |
| --------------- | -------------------------------------------------------------------------------------------------------------- |
| Change          | Inline empty or known nonblocking request streams. Keep a background writer for true interactive bidi streams. |
| Expected impact | Medium for Rust client-axis stream latency.                                                                    |
| Risk            | Deadlocks if interactive streams are inlined incorrectly.                                                      |
| Verify          | Client-axis stream latency rows and bidi correctness tests.                                                    |

### R9 Disable Benchmark Stream Timers Where Other Implementations Do

References:

| File                                       | Functions                                    |
| ------------------------------------------ | -------------------------------------------- |
| `trevrpc-rust/examples/rpc_split_bench.rs` | `benchmark_server_options`, `trevrpc_*_call` |
| `trevrpc-rust/src/client.rs`               | stream timeout receive helpers               |
| `trevrpc-rust/src/server.rs`               | `LimitedStream` timeout path                 |

Current behavior: Rust split benchmarks mostly keep default stream idle timers while C/Go benchmark paths disable some stream idle timeout overhead.

Plan:

| Item            | Detail                                                                                                                                                                                                      |
| --------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Change          | Use `with_stream_idle_timeout(None)` in Rust benchmark server options and matching benchmark `CallOptions` for apples-to-apples no-timeout rows. Also preserve default-timeout rows for production realism. |
| Expected impact | Medium benchmark improvement for streaming rows.                                                                                                                                                            |
| Risk            | Benchmark-only change can hide production timeout cost. Report both modes if possible.                                                                                                                      |
| Verify          | Rust split rows before/after.                                                                                                                                                                               |

### R10 Share Quinn And WebTransport Framed-Stream Logic

References:

| File                               | Functions                                                  |
| ---------------------------------- | ---------------------------------------------------------- |
| `trevrpc-rust/src/quinn.rs`        | framed read/write loops                                    |
| `trevrpc-rust/src/webtransport.rs` | duplicate framed read/write loops and unary drain behavior |

Plan:

| Item            | Detail                                                                                                                                                                                |
| --------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Change          | Extract common framed-stream encode/decode/read/write helpers so Quinn optimizations apply to WebTransport. Review WebTransport unary response finish versus request EOF drain order. |
| Expected impact | Low to medium direct latency, high maintainability.                                                                                                                                   |
| Risk            | Browser interoperability and WebTransport protocol compliance.                                                                                                                        |
| Verify          | Browser WebTransport tests and focused WebTransport benchmark rows.                                                                                                                   |

## Suggested Implementation Order

| Order | Work                                                        | Reason                                                                                  |
| ----: | ----------------------------------------------------------- | --------------------------------------------------------------------------------------- |
|     1 | `P0.1` / `J1` JS Release native build                       | Low risk and may materially change JS baseline.                                         |
|     2 | `R1` Rust bidi latency anomaly                              | Current 26.5 ms row is too large to ignore and may be a benchmark-shape or ACK issue.   |
|     3 | `G1`, `G3`, `J3`, `J5` stream batching/scheduler fast paths | Directly targets biggest non-C throughput gaps.                                         |
|     4 | `R2`, `R4`, `R5`, `R9` Rust async/copy/timer hot paths      | Improves Rust split rows while protecting its WebTransport strengths.                   |
|     5 | `C1`, `C2`, `C3`, `C4`, `C5` C hot-path improvements        | C already leads; these are deeper changes that should be done carefully with profiling. |
|     6 | `P0.2`, `G7`, `R7`, `C8` concurrency/window/profile sweeps  | Needed for predictable performance under load, beyond single-stream benchmarks.         |
|     7 | `R3`, `J4`, `J9`, `C6` API/lifetime-heavy improvements      | High ceiling but higher risk and larger API impact.                                     |

Status 2026-06-26: order 1 is complete; order 2 has been investigated but remains open for a transport-level follow-up; `G1` from order 3 is complete. Recommended next work is `J3`/`J5`/`J6` for JS streaming throughput or a trace-driven R1 follow-up, rather than more speculative Rust runtime changes.

## Focused Verification Commands

Adjust toggles to keep build/setup outside measured benchmark commands on `ssh bench`.

| Goal                        | Command sketch                                                                                                                                                                                                                                 |
| --------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Full required check         | `nix fmt` then `nix flake check`                                                                                                                                                                                                               |
| Split RPC full baseline     | `SAMPLE_TIMEOUT_SECONDS=300 bench/run_rpc_split.sh`                                                                                                                                                                                            |
| WebTransport full baseline  | `WEBTRANSPORT_DISABLE_STREAM_TIMEOUTS=1 WEBTRANSPORT_SEND_MANY_BATCH=64 WEBTRANSPORT_STREAM_READ_BATCH=64 WEBTRANSPORT_STREAM_WRITE_BATCH=64 WEBTRANSPORT_STREAM_WRITE_BATCH_BYTES=65536 SAMPLE_TIMEOUT_SECONDS=300 bench/run_webtransport.sh` |
| C-focused split             | Disable Go, JS, Rust rows with `RUN_GO_QUIC=0 RUN_GO_MSQUIC=0 RUN_GO_GRPC=0 RUN_JS_NATIVE=0 RUN_RUST_QUINN=0 RUN_RUST_GRPC=0`, then run `bench/run_rpc_split.sh`.                                                                              |
| Go-focused split            | Enable only the target Go transport rows plus the required C reference peer; run `bench/run_rpc_split.sh`.                                                                                                                                     |
| JS-focused split            | Enable `RUN_JS_NATIVE=1` plus the required C reference peer; run `bench/run_rpc_split.sh`.                                                                                                                                                     |
| Rust-focused split          | Enable `RUN_RUST_QUINN=1` plus the required C reference peer; run `bench/run_rpc_split.sh`.                                                                                                                                                    |
| Browser server-focused rows | Use `bench/run_webtransport.sh` with only the target server implementation enabled.                                                                                                                                                            |

## Acceptance Criteria For Future Changes

| Criterion          | Requirement                                                                                                           |
| ------------------ | --------------------------------------------------------------------------------------------------------------------- |
| Correctness        | Existing tests pass, plus new tests for changed close/cancel/deadline semantics.                                      |
| Formatting         | `nix fmt` was run.                                                                                                    |
| Repository check   | `nix flake check` passes.                                                                                             |
| Benchmark evidence | Each performance PR includes before/after focused rows, environment, command, and anomaly notes.                      |
| Wiki update        | If benchmarks were run, `wiki/Benchmarks.md` is updated in the same change according to `AGENTS.md`.                  |
| Safety             | No unbounded queues or batch sizes without backpressure. No zero-copy path without explicit ownership/lifetime tests. |
