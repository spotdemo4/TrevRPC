# TrevRPC Remaining TODO

Consolidated: 2026-07-09

Scope: this file tracks the most important work still unimplemented after auditing the old benchmark, performance, and production TODO files. Completed benchmark TLS work, current-baseline documentation, payload and metadata profiles, serialization label guardrails, smoke benchmark wiring, and already-added CSV report fields are intentionally omitted.

## Completion Rules

Use these rules for every follow-up below.

1. Preserve cancellation, deadlines, graceful shutdown, stream resets, terminal status handling, overload, and partial-stream semantics.
2. Build outside measured commands. Use `ssh bench` for benchmark measurements and record host context.
3. Whenever a benchmark is run, update `wiki/Benchmarks.md` with before/after rows, command, timestamp, relevant environment variables, commit/tree state, settings, failed samples, and anomalies.
4. Prefer focused split runs while iterating, then broader split or WebTransport runs before claiming broad wins.
5. Finish each code change with `nix fmt` and `nix flake check`. Intent-to-add new files before `nix flake check` so Nix sees them.

## P0 Benchmark Correctness

Goal: published benchmark rows should represent a production RPC path:

`structured data -> serialization -> encryption -> wire -> decryption -> deserialization -> structured data`

### Normalize And Document Transport Settings

Current state: benchmark reports document batching and security model, but do not fully document or normalize stream limits, idle timeouts, keepalive, TCP_NODELAY, QUIC flow control, ACK behavior, send buffering, and browser/WebTransport constraints.

Required work:

- Review transport settings that materially affect throughput and latency.
- Make settings equivalent where possible across trevRPC, gRPC, tonic, and ConnectRPC.
- Where equivalence is impossible, record the difference in normalized rows and `wiki/Benchmarks.md`.
- Decide whether benchmark-specific batching belongs in the production-representative profile or a separate optimized-throughput profile.

### Keep Steady-State And Handshake Results Separate

Current state: existing benchmark families are warmed steady-state measurements. No separate handshake/connect benchmark family exists.

Required work:

- Keep warmed steady-state tables separate from any handshake-inclusive data.
- Add a handshake/connect benchmark only if it is useful.
- If added, measure client construction, dial/connect, TLS handshake, first unary RPC, and clean close.
- Never mix handshake-inclusive rows with warmed steady-state rows in the same sorted table.

### Rerun And Publish Apples-To-Apples Tables

Required work:

- Run focused validation and full benchmark suites on the `bench` host using the `ssh-bench` workflow.
- Build and dependency setup must happen outside measured commands.
- Update only the relevant `wiki/Benchmarks.md` sections after each run.
- Keep historical notes for previous non-apples-to-apples runs so readers understand why results changed.

## P1 Performance Follow-Ups

### Separate Transport Cost From Generated/Protobuf Cost

Current state: split benchmarks now share production-style payload profiles. C and JS have partial micro/profile paths, but there are no comparable raw/pre-encoded split rows across all implementations.

Required work:

- Add one comparable raw route/client shape per runtime.
- Preserve the existing CSV schema by encoding variants in shape names.
- Do not add raw rows to published split tables until semantics match across C, Go, JS, and Rust.

### Add Frame-Level Evidence For Rust/Quinn `server / bidi_stream_latency`

Current state: speculative final-message-with-FIN and global zero-length-FIN changes were rejected by prior evidence. Packet capture on `bench` is blocked by missing CAP_NET_RAW/passwordless sudo. Repo qlog/keylog hooks are missing for Rust/Quinn and C/MsQuic split paths.

Investigation evidence from 2026-07-09, output in `bench:/home/trev/Dev/TrevRPC-bench-20260709-apples/target/rpc-split-20260709-investigate-c-msquic-rust-quinn`:

- The failed published pair reproduced only in the full all-shapes sequence: default C MsQuic client to Rust Quinn server completed at 10, 100, and 1000 iterations, but timed out at 10000 with status 124.
- A line-buffered 10000-iteration run showed `client_stream_throughput` was the last completed shape; timeout occurred during `bidi_stream_latency`.
- Shape-specific unary runs completed through 10000 iterations, so the issue is not a permanent unary or connection-level deadlock.
- Default one-message streaming latency was dominated by roughly 15-22 ms/op waits: at 1000 iterations, `client_stream_latency` was 18591.786 us/op and `bidi_stream_latency` was 15575.834 us/op.
- Rust Quinn ACK tuning removed the delay: with `TREVRPC_RUST_SPLIT_BENCH_QUINN_ACK_THRESHOLD=1` and `TREVRPC_RUST_SPLIT_BENCH_QUINN_ACK_DELAY_MS=0`, the full 10000-iteration sequence completed with `client_stream_latency` 110.951 us/op and `bidi_stream_latency` 120.896 us/op.
- Most likely root-cause hypothesis: delayed ACK / FIN-only request-stream shutdown interaction between the C MsQuic client and Rust Quinn server. The affected shapes send a tiny request stream, half-close it, and wait for the Rust server to observe EOF before responding or finishing.

Required work:

- Add qlog, keylog, or equivalent frame-level tracing hooks for Rust/Quinn and C/MsQuic split benchmarks.
- Capture request frame, response frame, request FIN, terminal status, stream FIN, ACK timing, and reset/stop-sending behavior.
- Use the reproduced ACK-tuning result as the first trace target, but do not bake benchmark ACK settings into published rows until frame evidence explains the interoperability behavior.
- Only attempt close-path fixes after trace evidence and correctness tests justify them.

### Replace C Per-Stream Detached Threads

Current state: accepted stream tasks still use detached pthreads and depend on stack-owned connection stream limiters plus cleanup transfer through deferred calls.

Required work:

- Design a bounded worker queue or pool for accepted streams.
- Preserve limiter release, overload rejection, graceful shutdown drain, and stream close ownership.
- Add race/leak coverage for shutdown, overload, stream reset, and rejected streams.

### Enable C Native Frame Mode Before Bytes Accumulate

Current state: MsQuic streams start byte-oriented, and frame mode is enabled later when `read_frame` is called, after callbacks may already have buffered bytes.

Required work:

- Select native RPC frame mode at stream construction before receive callbacks can buffer bytes.
- Keep WebTransport/H3 control and connect streams byte-oriented.
- Provide max-frame-size context at MsQuic stream creation.
- Preserve malformed-frame, partial-body, and oversized-frame behavior.

### Encode Directly Into Transport Send Buffers

Current state: C has partial streaming-message send-buffer pooling, but unary responses and status frames still allocate full wire frames, and MsQuic sends use a single buffer rather than scatter/gather.

Required work:

- Extend direct encode or scatter/gather send paths to unary responses and terminal status frames where safe.
- Keep frame/header/body memory alive until MsQuic send completion.
- Update wire APIs and generated C response helpers as needed.
- Preserve cancellation, reset, and send-completion ownership behavior.

### Reduce C Server Hot-Path Mutex Work

Current state: route lookup and active counters still use server mutexes. Registration is not frozen after start, and counters remain tied to shutdown condition variables.

Required work:

- Define registration-after-start behavior and freeze routes when serving starts.
- Reduce per-request route lookup locking after routes are frozen.
- Rework request/task/connection counters only if shutdown condition semantics remain correct.
- Add lifecycle tests for registration, shutdown, overload, and metrics behavior.

### Tune Timers, Backpressure, And MsQuic Settings Safely

Current state: idle-timeout-disabled paths avoid per-message clock work, but larger native windows, ACK, congestion, and send-buffering profiles are blocked on bounded pending-send accounting and slow-reader evidence.

Required work:

- Add bounded pending-send accounting before exposing larger buffering/window profiles.
- Add slow-reader and overload tests that prove memory remains bounded.
- Evaluate MsQuic receive windows, ACK behavior, congestion settings, send buffering, and timer costs.
- Validate WebTransport flow-control fields against browser compatibility before changing advertised limits.

### Replace Go Timeout Goroutines With Transport Deadlines

Current state: Go transport deadlines exist for some initial request and drain reads, but client/server streaming receive helpers still use goroutine/timer fallback with default idle timeouts.

Required work:

- Replace remaining timeout goroutine paths with transport deadlines where safe.
- Preserve precedence between context deadline, stream idle timeout, cancellation, and terminal status.
- Include MsQuic/native timed-read parity where applicable.
- Add deterministic tests for racing local cancellation/deadline, terminal status, upload cleanup, and stream close failures.

### Tune QUIC Flow-Control Windows With A Memory Budget API

Current state: Go and Rust derive bounded receive windows from frame size, stream concurrency, and max body size, but there is no public memory/DoS budget model for wider windows.

Required work:

- Design a memory budget API for receive windows and stream concurrency.
- Add slow-reader, overload, and high-concurrency tests before widening defaults or exposing knobs.
- Document safe operational bounds and benchmark-specific profiles separately from production defaults.

### Reduce Rust `MessageStream::next` Hot-Path Overhead

Current state: public Rust `MessageStream` still uses `async_trait` and `async fn next`; batching/ready-drain behavior is only partial and internal.

Required work:

- Evaluate replacing or supplementing `next` with `poll_next`, a GAT-based trait, or an optional ready-drain method.
- Preserve generated service API compatibility or provide a migration plan.
- Back changes with allocation profiles and stream correctness tests.

### Share Rust Quinn And WebTransport Framed-Stream Logic

Current state: Quinn and WebTransport frame read/write, batching, request/response stream, and drain loops remain duplicated and are coupled to transport-specific close/reset semantics.

Required work:

- Add transport-specific tests for finish, reset, cancellation, terminal status, and WebTransport interoperability behavior.
- Extract shared framed-stream logic only after the tests can catch close/reset regressions.

### Reduce JavaScript Native Buffer Copies

Current state: Node native inbound frames are copied into new ArrayBuffers, outbound JS bytes are copied into C buffers, and batched outbound bodies are copied into contiguous C buffers. Frame bodies may point inside `_body_owner`.

Required work:

- Add explicit C helpers for transferring the correct owner allocation to external ArrayBuffer finalizers.
- Hold outbound JS buffer references until MsQuic send completion if zero-copy outbound paths are introduced.
- Preserve GC, close, cancellation, partial-stream, and send-completion behavior.

### Redesign JavaScript Native Completion Long-Term

Current state: the Node addon still queues per-operation `napi_async_work` around synchronous/blocking C APIs.

Required work:

- Design a native completion source or persistent poller.
- Preserve close and GC behavior while operations are pending.
- Preserve cancellation, terminal status precedence, and partial-stream behavior.
- Keep the current blocking path until the evented design has correctness coverage.

## P1 Production Hardening

### Bound Non-Cooperative Cancellation And Shutdown

Current state: deterministic cancellation coverage is strong for cooperative paths, but non-cooperative work can still weaken hard bounds. Go deadline paths may return and release request permits while handler goroutines continue. Rust shutdown aborts active tasks but then waits for joins. JS stream `return()` and close paths can wait for custom iterables or native operations that do not settle.

Required work:

- Audit transport-level waits for hard cancellation bounds, especially stream close, session shutdown, and local cancellation versus remote terminal status races.
- Keep request/concurrency permits held until non-cooperative work is actually stopped or isolated, or document and test the isolation boundary.
- Add tests with handlers, iterables, and background tasks that ignore cancellation or never settle.

### Split Resource Budgets Further Where Needed

Current state: some limits are still coarse or shared across request body, response body, streaming messages, and total stream bytes.

Required work:

- Split request-body, response-body, streaming-message, and total-stream byte budgets where real workload needs justify it.
- Keep defaults bounded and predictable under slow-reader and overload conditions.
- Add compatibility and limit-boundary tests for every new budget.

### Expand Browser WebTransport Lifecycle Coverage

Current state: Playwright Chromium coverage is useful but narrow. It does not cover a broad browser/runtime matrix, page close/navigation, browser context close, server shutdown mid-stream, session close/error propagation, stream-open abort under real browser WebTransport, Rust lifecycle parity for all cases, or close races between local abort/deadline and remote terminal status.

Required work:

- Add real-browser lifecycle tests for the missing cases that Node-style mocks cannot prove.
- Expand coverage across Go and Rust WebTransport servers where behavior should match.
- Add long-running streaming soak sessions outside the normal fast test path.

### Add Longer-Running Protocol And Security Validation

Current state: seed/deterministic fuzz-style tests exist, but long fuzz/property CI jobs are not implemented across frame parsing, metadata validation, stream limits, and protobuf wire compatibility.

Required work:

- Add longer-running fuzz/property CI jobs or scheduled workflows.
- Cover frame parsing, protobuf decode normalization, metadata validation, stream limits, and wire compatibility.
- Preserve fast deterministic tests for normal `nix flake check`.

### Broaden Cross-Runtime And Browser Compatibility Matrices

Current state: native QUIC cross-runtime coverage is Go/Rust focused, and browser WebTransport coverage is Chromium focused.

Required work:

- Expand cross-runtime integration beyond Go/Rust native QUIC and JS-browser WebTransport smoke tests.
- Add browser/runtime compatibility tests where WebTransport support exists.
- Explicitly test unsupported-browser behavior where WebTransport is unavailable.

### Improve Rust And JavaScript Failure Reporting

Current state: Rust background task failures are often only tracing logs, and some JavaScript browser cleanup failures are intentionally swallowed.

Required work:

- Surface Rust background task panics/failures through explicit reporting where the runtime exposes enough information.
- Surface JavaScript stream and cleanup failures where doing so does not break terminal status precedence.
- Add tests for failure reporting, metrics isolation, and cleanup error precedence.

## Suggested Execution Order

1. Normalize/document transport settings and rerun apples-to-apples benchmark tables on `bench`.
2. Add qlog/keylog tracing before attempting Rust/Quinn close-path tuning.
3. Address C threading/frame-mode/send-buffer hot paths behind correctness and shutdown tests.
4. Replace Go timeout goroutine paths and design flow-control memory budget APIs.
5. Reduce Rust stream overhead and extract shared framed-stream logic after close/reset tests are strong enough.
6. Tackle JS zero-copy and evented native completion only after explicit ownership/lifetime tests exist.
7. Expand non-cooperative cancellation, WebTransport lifecycle, fuzz/property, cross-runtime, browser, and failure-reporting coverage.
