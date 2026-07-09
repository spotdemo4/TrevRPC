# TrevRPC Remaining TODO

Updated: 2026-07-09

This file now tracks the work that remains after the 2026-07-09 implementation pass. Items listed as completed were changed in this pass. Items listed as deferred are intentionally not implemented yet because they require additional evidence, ownership/lifetime tests, or broader design work before they can be made safely.

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
- JavaScript browser WebTransport now has unit coverage for unsupported runtime and missing bidirectional stream APIs returning `Code.Unavailable`.
- Browser WebTransport lifecycle coverage now exercises real Chromium page close, navigation, browser-context close, stream-open abort, server shutdown mid-stream, and Go/Rust lifecycle server parity in the Playwright fast path.
- Go and Rust WebTransport lifecycle tests now cover shutdown behavior for active sessions/connections.
- JavaScript native server call close now closes the underlying C call once pending refs drain, even if the JS wrapper is still alive.
- Operational docs now include receive-memory budget guidance, benchmark diagnostics, non-cooperative cancellation boundaries, WebTransport browser limitations, and current unsafe zero-copy/buffering boundaries.
- Scheduled/manual protocol fuzz workflows were added for bounded Go fuzz targets covering frame decode, frame length parsing, and metadata validation outside normal fast checks.

## Deferred With Guardrails

### C Native Frame Mode At Stream Construction

Deferred. MsQuic streams still switch to native frame mode on first frame read. WebTransport/H3 control and CONNECT streams must remain byte-oriented, so this should be changed only with tests that prove early frame mode is applied only to native TrevRPC streams.

Required before implementation:

- Native accepted stream starts in frame mode before byte buffering.
- Partial header/body, oversized-frame, and malformed-frame behavior remains unchanged.
- WebTransport H3 control, CONNECT, and stream prelude paths remain byte-oriented.

### C Direct/Scatter-Gather Unary And Status Sends

Deferred. C streaming message sends already have direct pooled send paths; unary responses and terminal statuses still allocate complete frames. Scatter/gather or borrowed-buffer sends require explicit send-completion lifetime tests before handler-owned buffers can be referenced by MsQuic.

Required before implementation:

- Unary/status send-completion ownership tests under reset and close.
- Generated C helper tests with stack and heap protobuf buffers.
- Clear rule that frame/header/body memory remains alive until MsQuic `SEND_COMPLETE`.

### C Larger Buffering Profiles

Deferred. Per-stream pending-send accounting is implemented and MsQuic send buffering remains disabled by default. Larger MsQuic receive windows, ACK/congestion tuning, or explicit send-buffering profiles still require separate workload-specific evidence and tests before becoming public profiles.

Required before implementation:

- Slow-reader and overload tests for each proposed profile proving total memory remains bounded.
- Documentation that clearly separates safe defaults from opt-in diagnostic or throughput profiles.
- Cross-runtime benchmark evidence recorded in `wiki/Benchmarks.md` for any profile intended for published rows.

### Rust Quinn/MsQuic ACK Tuning

Deferred. ACK tuning is not baked into published rows. The Rust/C tracing and keylog hooks must be used to capture request frame, request FIN, response frame, terminal status, response FIN, reset/STOP_SENDING, and ACK timing evidence first. The Rust Quinn close-path change is limited to draining FIN after terminal status based on Quinn `RecvStream` drop semantics and integration tests.

Required before implementation:

- Focused C MsQuic client to Rust Quinn server trace for `bidi_stream_latency` and `client_stream_latency`.
- Comparison of default and ACK-threshold/delay diagnostic runs.
- Correctness tests that prove any close-path change preserves terminal status, reset, cancellation, and partial-stream semantics.

### Rust Shared Framed-Stream Extraction

Deferred. Quinn and WebTransport frame logic is still duplicated because close/reset semantics differ. The ready-drain optimization only touched request-body batching and did not extract shared transport logic.

Required before implementation:

- Transport-specific tests for finish, reset, cancellation, terminal status, partial initial body, oversized initial frame, and WebTransport interoperability.
- Explicit preservation of WebTransport unary request drain behavior and Quinn reset/STOP_SENDING behavior.

### JavaScript Zero-Copy Native Paths

Deferred. Native JS still copies inbound and outbound buffers by default. This remains the safe ownership model until C helpers can transfer the correct owner allocation to external ArrayBuffer finalizers and outbound JS buffers can be retained until MsQuic send completion.

Required before implementation:

- External ArrayBuffer finalizer tests for response bodies and stream frames whose body points inside `_body_owner`.
- Forced-GC lifetime tests.
- Native send-completion refs for outbound zero-copy.

### JavaScript Evented Native Completion

Deferred. The addon still uses `napi_async_work` around blocking C APIs. A persistent poller or native completion source needs separate design and correctness coverage before replacing the current blocking path.

Required before implementation:

- Close/GC behavior while operations are pending.
- Cancellation and terminal-status precedence tests.
- Partial-stream cleanup and send-completion tests.

### Broader Browser And Cross-Runtime Matrix

Deferred. Chromium browser WebTransport coverage remains the fast normal path and now includes lifecycle shutdown/teardown parity. Non-Chromium browsers, long-running soak, and broader cross-runtime matrices remain scheduled/manual until stable and low-cost enough for normal checks.

Required before implementation:

- Non-Chromium browser WebTransport coverage once browser support is stable enough.
- Long-running WebTransport soak outside normal `nix flake check`.
- Additional cross-runtime lifecycle stress beyond the current Chromium fast path.

### Additional Resource Budget APIs

Deferred. The docs now define the receive-memory budget model. Public APIs for splitting request-body, response-body, streaming-message, total-stream, and connection receive budgets should be added only when concrete workload requirements justify the extra knobs.

Required before implementation:

- Compatibility tests for every new budget field.
- Limit-boundary tests across C, Go, Rust, and JavaScript.
- Slow-reader and overload tests before widening defaults.
