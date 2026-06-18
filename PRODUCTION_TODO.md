# Production Hardening TODO

This repository is not production-ready yet. Track the remaining work here so the runtime changes are not lost while the Go, JavaScript, and Rust implementations evolve.

## 1. Cancellation and Deadlines

- Add real cancellation/deadline propagation through all transport operations.
- Ensure call timeouts abort stream opens, frame reads, frame writes, request uploads, response reads, stream closes, and session shutdown waits.
- Prefer relative timeout propagation on the wire over absolute client wall-clock deadlines.
- Expose cancellation/deadline context to server handlers, including streaming handlers.
- Add JavaScript `AbortSignal` support and make timeout handling abort the underlying WebTransport work instead of only rejecting the returned promise.
- Ensure Go and Rust unary and streaming clients surface context cancellation consistently.

## 2. Initial Frame, Header, Body, and Resource Limits

- Add server-side initial request/header/body idle timeouts for native QUIC and WebTransport.
- Avoid acquiring scarce request permits indefinitely for peers that open streams but do not send request frames.
- Reduce default frame and concurrency limits to safer production defaults.
- Separate request-body, response-body, streaming-message, and total-stream byte budgets where needed.
- Avoid eager max-sized allocations before a peer has delivered the frame body.
- Align QUIC/WebTransport transport flow-control limits with TrevRPC frame and stream limits.

## 3. Streaming Lifecycle and Error Propagation

- Give public response-stream APIs an explicit close/cancel contract.
- Cancel or drain receive sides after terminal protocol frames.
- Stop, join, and surface client upload writer tasks for client-streaming and bidirectional-streaming RPCs.
- Make terminal status handling wait for or cancel request producers so infinite or slow request iterables cannot leak.
- Mark client response streams terminal after protocol, decode, size, or transport errors.
- Ensure local encode/write failures cannot be hidden by a successful remote response.

## 4. Protocol and Security Validation

- Reject invalid protocol enum values, including unknown `RpcKind` and stream-frame kinds.
- Enforce native QUIC ALPN validation for `trevrpc/1` and reject protocol confusion.
- Require explicit WebTransport origin, path, and authority policy before browser-facing production use.
- Define and publish the TrevRPC wire schema, compatibility matrix, version negotiation behavior, and rolling-upgrade policy.
- Normalize malformed input, oversized frames, and decode errors to stable canonical statuses.
- Replace bearer-token helper equality checks with constant-time comparison where practical.

## 5. Shutdown, Panics, and Failure Isolation

- Make graceful shutdown timeouts hard bounds rather than waits that can continue forever.
- Propagate server shutdown cancellation to active native QUIC and WebTransport streams immediately.
- Add panic recovery around Go handlers, metrics hooks, and stream adapters.
- Ensure Rust task panics and JavaScript stream failures are isolated and reported without corrupting session state.
- Keep request/concurrency permits held until non-cooperative work is actually stopped or isolated.
- Preserve `rpc_finished`/completion metrics under cancellation and failure paths.

## 6. Adversarial and Interoperability Tests

- Add slowloris tests for opened streams with no frame, partial headers, and partial bodies.
- Add cancellation-race tests for unary, server-streaming, client-streaming, and bidirectional-streaming calls.
- Add tests for writer task failures, request iterable cancellation, terminal status races, and early stream close.
- Add malformed frame, oversized frame, invalid enum, invalid metadata, and decode-error tests.
- Add shutdown tests with stuck handlers and long-running streams.
- Add fuzz/property tests for frame parsing and protobuf wire compatibility.
- Add Go/JavaScript/Rust golden-vector and integration tests for wire interoperability.
- Add WebTransport origin/path policy tests and browser/runtime compatibility tests.
