# TrevRPC Remaining TODO

Updated: 2026-07-09

This file records the completed 2026-07-09 implementation and evidence pass. All previously deferred sections have been resolved through implementation or an evidence-backed no-change decision. No item remains deferred.

## Completed In This Pass

- Benchmark reports now document normalized transport settings, batching/profile boundaries, browser/WebTransport constraints, and the separation between warmed steady-state rows and any future handshake-inclusive rows.
- Split benchmark scripts now label the production-representative profile, log diagnostic env vars when set, and normalize Rust Quinn split idle timeout and keepalive to the same `600000ms` / `5000ms` profile used by C MsQuic and Go quic-go rows.
- Rust Quinn split diagnostics now support `SSLKEYLOGFILE`, `TREVRPC_RUST_QUINN_FRAME_TRACE=1`, and `TREVRPC_RUST_SPLIT_BENCH_SHAPES=...` for focused frame/FIN/reset evidence before any ACK or close-path tuning.
- C wire diagnostics now support `TREVRPC_C_FRAME_TRACE=1` for opt-in request/response/stream-frame metadata logging without payload bytes or metadata values.
- C route registration now freezes when serving starts; post-freeze registration returns `-EALREADY`, and route lookup avoids the server mutex once routes are frozen. Runtime tests cover late registration rejection and frozen-route lookup.
- Go client response receives use transport read deadlines for deadline-capable QUIC/WebTransport response streams, preserving context-deadline versus stream-idle-timeout precedence and keeping the goroutine fallback for generic streams.
- Rust `MessageStream` now has an optional object-safe `drain_ready` hook. In-memory encode/decode streams implement it, and Quinn/WebTransport request-body batching uses it without changing generated service APIs.
- C servers now dispatch accepted streams through a server-owned bounded worker pool with public worker-count and queue-capacity options, heap/refcounted connection stream limiter ownership, queue-full `ResourceExhausted` rejection, queue-wait-aware initial request timeout handling, and runtime coverage for saturation, shutdown, reset, and deferred raw call cleanup.
- C MsQuic streams now enforce per-stream pending-send byte/count budgets around `StreamSend`; sends reserve before submission, roll back on synchronous failure, decrement on every send completion including canceled completions, and keep stream send state alive until completions drain while send buffering stays disabled by default.
- Rust Quinn terminal streaming statuses now drain the following transport FIN before completing, avoiding implicit Quinn `RecvStream` drop `STOP_SENDING(0)` after accepted terminal statuses without waiting for ACKs.
- Rust Quinn/WebTransport framed-stream length-prefix, stream-frame fast path, and request-body batching logic now share one internal Rust implementation while transport-specific reset/STOP_SENDING, terminal FIN drain, and WebTransport unary request drain behavior remain explicit in the transport modules.
- JavaScript browser WebTransport now has unit coverage for unsupported runtime and missing bidirectional stream APIs returning `Code.Unavailable`.
- Browser WebTransport lifecycle coverage now exercises real Chromium page close, navigation, browser-context close, stream-open abort, server shutdown mid-stream, and Go/Rust lifecycle server parity in the Playwright fast path.
- Go and Rust WebTransport lifecycle tests now cover shutdown behavior for active sessions/connections.
- JavaScript native server call close now closes the underlying C call once pending refs drain, even if the JS wrapper is still alive.
- C native accepted TrevRPC streams now enter MsQuic frame mode when peer-started streams are constructed for the `trevrpc/1` ALPN, while H3 control, CONNECT, and WebTransport stream prelude paths remain byte-oriented.
- C native unary responses and terminal streaming statuses now use direct MsQuic frame-part sends for native streams, borrowing response bodies only until `SEND_COMPLETE`, with explicit close/reset lifetime tests and generated-helper stack/heap protobuf coverage.
- Operational docs now include receive-memory budget guidance, benchmark diagnostics, non-cooperative cancellation boundaries, WebTransport browser limitations, and current unsafe zero-copy/buffering boundaries.
- Scheduled/manual protocol fuzz workflows were added for bounded Go fuzz targets covering frame decode, frame length parsing, and metadata validation outside normal fast checks.
- Browser and cross-runtime matrix guardrails now keep Chromium WebTransport in the fast path, add scheduled/manual Chromium WebTransport soak and native Go/Rust lifecycle stress outside normal `nix flake check`, and keep Firefox/WebKit diagnostics manual-only until non-Chromium WebTransport support is stable enough.
- C/Go/Rust/JavaScript resource-budget tests now cover exact-limit and one-unit-over behavior for the existing frame, unary-response, stream-message, cumulative-stream-body, and derived receive-window limits without adding unsupported public knobs.
- JavaScript native inbound response bodies, stream frames, and stream body batches can transfer C owner allocations to external ArrayBuffer finalizers, with forced-GC tests covering bodies that point inside `_body_owner`. Internal borrowed outbound send paths retain JS buffers until native send completion returns, while public sends remain copy-based.
- JavaScript native addon operations now use a binding-owned persistent native completion worker source and Node-API threadsafe-function delivery instead of `napi_async_work`, with debug/native coverage for pending close/GC, retained send refs, terminal-status precedence, partial-stream return cleanup, and send completion behavior.

## Resolved Remaining Items

### C Larger Buffering Profiles

Resolved with an opt-in profile and unchanged safe defaults. `TREVRPC_MSQUIC_TUNING_PROFILE_THROUGHPUT_1M` sets a 1 MiB stream receive window, a 64 MiB connection receive window, MsQuic send buffering, and the max-throughput execution profile. It does not widen the 4 MiB frame limit, 16 MiB cumulative stream-body limit, 64-stream application admission limit, 64 MiB/1024 pending-send limits, or idle timeouts. `TREVRPC_MSQUIC_TUNING_PROFILE_DEFAULT` restores the four safe transport settings.

The high-level C harness ran 54 isolated slow-reader, stalled-handler, reset, close, overload, and exact-body-limit samples across `safe`, rejected `receive-1m`, and `throughput-1m` profiles. Every sample passed its aggregate process peak/convergence bounds, exact byte/status invariants, and zero-send-failure checks. Three-sample cross-runtime A/B results support `throughput-1m` for measured large-stream workloads, including C-to-C client-stream throughput improving from median `5167` to `17992` messages/s and Rust-client-to-C from `4115` to `9755`; results were not universal and the host governor was `powersave`, so the profile remains experimental and opt-in. Full samples, failures, hashes, and commands are in `wiki/Benchmarks.md`.

### Rust Quinn/MsQuic ACK Tuning

Resolved as a diagnostic-only no-default-change decision. Quinn qlog and decoded protocol tracing captured the C MsQuic client to Rust Quinn server request/FIN, ACK, response, terminal status, response FIN, reset, and STOP_SENDING lifecycle. Low-overhead qlog found 351 library-default RTT updates from `25.216` to `25.571` ms; threshold `1` with requested delay `0ms` removed the repeated greater-than-5ms interaction. Ten clean samples per shape changed `client_stream_latency` from median `19736.019` to `78.501` us/op and `bidi_stream_latency` from `18517.809` to `81.371` us/op without failures.

The interaction is specific to one MsQuic peer, tight-loop workload, host, and pacing pattern; full tracing itself suppressed it. Normal and published runs therefore retain Quinn/library ACK defaults. Threshold `1` and requested delay `0ms` remain an opt-in `diagnostic-interoperability` profile. Benchmark rows now persist ACK and instrumentation state, and traced/qlog/keylog runs cannot be regenerated as production rows. Quinn integration tests cover terminal FIN draining, cancellation/reset, expected STOP_SENDING, partial and oversized input, and frames after terminal status.

### JavaScript Zero-Copy Native Paths

Resolved with compatibility-safe opt-in APIs. Copying remains the default. `outboundZeroCopy: true` enables borrowed native unary request/response bodies and single or batched stream messages for Node clients and servers; generated clients can opt in with `outbound_zero_copy=true`, while per-call `false` overrides that generated default.

JavaScript references are retained until every corresponding MsQuic `SEND_COMPLETE`, including canceled completions. Synchronous failure, pending-send exhaustion, reset, cancellation, stream/client/call close, terminal races, active connect cancellation, and environment shutdown release references exactly once. Same-stream and same-call outbound work uses an invocation-ordered FIFO over binding-owned completion workers; receives use nonblocking readiness retries outside that FIFO so duplex traffic progresses. Forced-GC and sanitizer coverage includes unary bodies, typed-array slices, ArrayBuffers, single/batched messages, metadata/native prefixes, overlapping send/finish/terminal work, close/reset, and worker shutdown. Borrowed backing stores must not be mutated, detached, transferred, or resized before the returned operation settles.

Remote profiling through concurrency 64 found no starvation after the scheduler changes, with copy and zero-copy unary throughput both near 33k operations/s. The binding-owned worker source plus monotonic retry scheduler is sufficient for the measured workload, so a deeper exported C completion queue or poll API is not justified.

### Additional Resource Budget APIs

Resolved with an evidence-backed no-new-fields decision. The corrected high-level matrix exercised asymmetric 4 KiB request/64 KiB response slow readers, 64 KiB request/4 KiB response stalled handlers, 4-256 KiB request messages, 4-64 KiB response messages, exact 1 MiB cumulative exhaustion, reset, cleanup, and 16-way overload against an admission limit of 8.

Independent request-body, response-body, streaming-message, combined total-stream, and connection receive-byte fields were rejected as redundant or unsupported by a concrete workload. Existing frame, directional response, message-count, cumulative-body, stream/request admission, connection, deadline, and idle-timeout controls bounded every measured retained-data dimension. The C receive-window fields and throughput helper are transport flow-control tuning, not aggregate memory budgets, because accepted bytes are copied into application-owned queues. Wire compatibility and existing defaults remain unchanged.
