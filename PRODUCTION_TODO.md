# Production Hardening TODO

This repository is not production-ready yet. Track the remaining work here so runtime hardening is not lost while the Go, JavaScript, and Rust implementations evolve.

## Completed Hardening

- Relative wire timeouts replaced absolute client wall-clock deadlines across Go, JavaScript, and Rust.
- Server handler contexts/deadlines are propagated in Go and Rust unary and streaming handlers.
- JavaScript `AbortSignal` support aborts WebTransport stream opens, reads, writes, request uploads, and response reads.
- Server-side initial request header/body idle timeouts exist for native QUIC and WebTransport.
- Default frame and concurrency limits were reduced to bounded production defaults.
- Request/response stream message count, body-size, and idle-timeout limits exist.
- Unknown `RpcKind` and `RpcStreamFrameKind` values are rejected as protocol errors.
- Malformed frame protobuf, oversized frames, decode errors, transport failures, cancellations, and deadlines map to stable canonical statuses on core paths.
- Native QUIC ALPN confusion, WebTransport path, authority, and browser origin policy are tested.
- Bearer and metadata-value authorizers use constant-time comparison where practical.
- The wire schema, compatibility policy, rolling-upgrade policy, and shared golden vectors are documented and tested across Go, JavaScript, and Rust.
- Initial dependency-free fuzz/property-style tests cover frame parsing, protobuf decode normalization, metadata limits, and stream-limit boundaries.
- Go handlers, metrics callbacks, and server stream adapters recover panics and return stable statuses.
- Rust handler task panics and metrics callback panics are isolated and returned as stable statuses.
- Go and Rust shutdown cancellation reaches active native QUIC/WebTransport stream tasks.
- Streaming terminal-status, early-close, upload-writer, and request-iterable cleanup paths have targeted coverage.
- Public response-stream close/drop/return semantics are documented and covered for Go, JavaScript, and Rust transports.
- Cancellation/deadline race coverage exists for unary, server-streaming, client-streaming, and bidirectional-streaming client paths across Go, JavaScript, and Rust.
- Slowloris-style partial-header, partial-body, and oversized-initial-frame tests exist where the transports expose those states.
- Shutdown tests cover stuck handlers and long-running streams.

## Remaining Work

### 1. Cancellation and Deadlines

- Continue auditing transport-level waits for hard cancellation bounds, especially stream close, session shutdown waits, and races between local cancellation and remote terminal statuses.
- Extend cancellation-race coverage from deterministic unit fakes into native QUIC/WebTransport integration tests where timing is observable.
- Ensure Go and Rust clients consistently surface local context cancellation when request upload, response read, and transport close fail concurrently.

### 2. Initial Frame, Header, Body, and Resource Limits

- Avoid acquiring scarce request permits indefinitely for peers that open streams but do not send request frames on transports that expose such streams before bytes arrive.
- Avoid eager max-sized allocations before a peer has delivered the frame body.
- Align QUIC/WebTransport transport flow-control limits with TrevRPC frame and stream limits.
- Split request-body, response-body, streaming-message, and total-stream byte budgets further if real workloads need different limits.

### 3. Streaming Lifecycle and Error Propagation

- Add more close/drop/return race tests against terminal statuses, local cancellation, and local upload failures.
- Finish cross-runtime coverage for local encode/write failures racing successful remote responses.

### 4. Protocol and Security Validation

- Expand fuzz/property tests into longer-running CI jobs for frame parsing, metadata validation, stream limits, and protobuf wire compatibility.
- Add cross-runtime integration tests beyond golden vectors so Go, JavaScript, and Rust clients/servers exchange real RPCs.

### 5. Shutdown, Panics, and Failure Isolation

- Keep request/concurrency permits held until non-cooperative work is actually stopped or isolated.
- Preserve `rpc_finished`/completion metrics under every cancellation, panic, timeout, decode-error, and transport-failure path.
- Continue isolating Rust background task panics and JavaScript stream failures with explicit reporting where the runtime exposes enough information.

### 6. Browser and WebTransport Compatibility

- Add browser/runtime compatibility tests for WebTransport behavior that Node-style unit tests cannot cover.
- Add production examples documenting explicit WebTransport origin/path/authority policy.
