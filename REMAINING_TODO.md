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

## Deferred With Guardrails

### C Larger Buffering Profiles

Deferred and narrowed. Per-stream pending-send accounting is implemented, slow-reader close-drain coverage exists for the safe default, and MsQuic send buffering remains disabled by default. No larger C buffering profile was added because no workload-specific profile could be justified without A/B benchmark data and peak-memory evidence. A required remote evidence attempt was made against `ssh trev@192.168.0.160`, but the host was unreachable with `No route to host`, so benchmark collection for profile promotion is externally blocked.

Required before implementation:

- A reachable required benchmark host or equivalent approved measurement environment.
- A concrete proposed profile with explicit receive-window, send-buffering, stream-concurrency, frame-size, and cumulative-body settings.
- Slow-reader and overload tests for each proposed profile proving total memory remains bounded, including peak-memory measurements.
- Documentation that clearly separates safe defaults from opt-in diagnostic or throughput profiles.
- Cross-runtime benchmark evidence recorded in `wiki/Benchmarks.md` for any profile intended for published rows.

### Rust Quinn/MsQuic ACK Tuning

Deferred and narrowed after exploratory diagnostics. ACK tuning is not baked into published rows. Focused C MsQuic client to Rust Quinn server diagnostics were run on `ssh trev@192.168.0.160` for `client_stream_latency` and `bidi_stream_latency` with `TREVRPC_RUST_QUINN_FRAME_TRACE=1`, `TREVRPC_C_FRAME_TRACE=1`, `SSLKEYLOGFILE`, and shape filters. The ACK-threshold/delay diagnostic run reduced traced outliers in a small sample, but packet-level ACK timing was not captured, the host was in `powersave`, and a follow-up packet-capture check was blocked when the benchmark host became unreachable. Results are recorded in `wiki/Benchmarks.md`.

Required before implementation:

- Packet-level ACK timing evidence, such as qlog or decoded packet capture, for the same C MsQuic client to Rust Quinn server shapes.
- Broader clean-host benchmark samples without frame tracing before changing published ACK defaults.
- Correctness tests that prove any behavior change preserves terminal status, reset, cancellation, STOP_SENDING, and partial-stream semantics.

### JavaScript Zero-Copy Native Paths

Partially implemented. Native unary response bodies and native stream message bodies can now transfer their C owner allocation to external ArrayBuffer finalizers, including frames whose `body` points inside `_body_owner`. An internal single-message outbound path retains the JS body with a native reference until the borrowed MsQuic send waits through `SEND_COMPLETE`.

Remaining before public/default outbound zero-copy:

- Public API and generator/runtime policy for opting into zero-copy sends.
- Borrowed batched stream-send and unary request-frame coverage.
- A deeper C-level completion queue or exported nonblocking transport poll API if future profiling shows the binding-owned native completion workers are insufficient for high-concurrency workloads.

### Additional Resource Budget APIs

Deferred and narrowed after the 2026-07-09 budget pass. The docs define the receive-memory budget model, and C/Go/Rust/JavaScript tests now cover exact-limit and over-limit behavior for the existing frame, unary-response, stream-message, cumulative-stream-body, and derived receive-window budgets. No public API fields were added because the repository does not yet contain concrete workload evidence showing that independent request-body, response-body, streaming-message, total-stream, or connection receive-budget knobs are needed beyond the current defaults and per-call/server stream limits.

Current blockers:

- Request-body and response-body split: existing frame and cumulative-stream-body limits bound both directions, and no test workload currently proves asymmetric request/response byte caps improve safety or performance.
- Streaming-message split: existing stream message caps already protect both request and response streams, and no compatibility requirement currently justifies separate per-direction fields.
- Total-stream budget: existing cumulative body caps cover total stream bytes; a separate total-stream field needs a workload showing message-count and body-byte caps are insufficient.
- Connection receive budget: Go and Rust derive QUIC receive windows from frame size, stream-body size, and stream concurrency. C and Node native receive-window exposure remains blocked by the C larger-buffering profile requirements: slow-reader and overload memory evidence must come first.

Required before implementation:

- One concrete workload or production requirement for each proposed new knob, including expected payload distributions and concurrency.
- Compatibility tests for every new public budget field in every language that exposes it.
- Limit-boundary tests across C, Go, Rust, and JavaScript showing exact-limit success and one-unit-over failure.
- Slow-reader and overload tests proving bounded memory before widening defaults or exposing larger C/Node native receive-buffer profiles.
- Benchmark evidence recorded in `wiki/Benchmarks.md` for any profile intended to change published performance rows.

Next action: build a small overload/slow-reader harness that records peak memory while varying stream concurrency, frame size, and stream-body limit. Use that evidence to decide whether connection-level receive budgets should become explicit API fields or remain derived transport settings.
