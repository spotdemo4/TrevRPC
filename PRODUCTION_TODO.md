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
- Terminal streaming status precedence is covered so terminal `OK` surfaces meaningful local upload/close failures while terminal non-OK statuses win once received.
- Cancellation/deadline race coverage exists for unary, server-streaming, client-streaming, and bidirectional-streaming client paths across Go, JavaScript, and Rust.
- Local cancellation/deadline precedence is covered so Go cancellation and Rust deadlines win over ready upload, response-read, and cleanup errors on deterministic client paths.
- Native QUIC/WebTransport integration tests cover pending response-read and request-upload cancellation/deadline races in Go and Rust.
- Slowloris-style partial-header, partial-body, and oversized-initial-frame tests exist where the transports expose those states.
- Frame readers avoid allocating the full advertised body length before bytes arrive, with large partial-body coverage in Go, JavaScript, and Rust.
- Go and Rust request permits are acquired only after the initial request frame completes, with native QUIC regression coverage for large partial initial bodies.
- Go and Rust QUIC/WebTransport setup helpers align receive windows and incoming stream caps with TrevRPC frame, stream, and concurrency limits while preserving over-limit status responses.
- Go and Rust accepted-RPC completion metrics have exact-once coverage for success, handler errors/panics, auth/protocol rejection, deadlines, streaming drops, and request-limit rejection.
- Go and Rust pre-handler transport/protocol failures record start+finish metrics with empty service/method placeholders for malformed and timed-out initial frames.
- Go/Rust native QUIC cross-runtime smoke tests cover unary, all streaming modes, auth failure, and malformed initial-frame protocol errors in both directions.
- Shutdown tests cover stuck handlers and long-running streams.
- Browser WebTransport greeter examples document and enforce explicit path, authority, origin, certificate, and bearer-token policy.
- Playwright Chromium coverage exercises the JavaScript browser WebTransport client against Go and Rust greeter servers across unary, server-streaming, client-streaming, and bidirectional-streaming RPCs with certificate-hash trust, bearer auth, and negative path/origin/authority policy cases.
- Playwright Chromium lifecycle coverage exercises terminal OK/error while uploads are pending, local abort while a response read is pending, and early response-stream return cancelling server work.

## Remaining Work

### 1. Cancellation and Deadlines

- Continue auditing transport-level waits for hard cancellation bounds, especially stream close, session shutdown waits, and races between local cancellation and remote terminal statuses not covered by deterministic integration cases.

### 2. Initial Frame, Header, Body, and Resource Limits

- Split request-body, response-body, streaming-message, and total-stream byte budgets further if real workloads need different limits.

### 3. Streaming Lifecycle and Error Propagation

- Continue expanding browser/runtime WebTransport lifecycle tests into longer-lived streams and failure cases that Node-style mocks cannot prove.

### 4. Protocol and Security Validation

- Expand fuzz/property tests into longer-running CI jobs for frame parsing, metadata validation, stream limits, and protobuf wire compatibility.
- Expand cross-runtime integration beyond Go/Rust native QUIC and JS-browser WebTransport smoke tests to include broader browser matrices and longer-lived streaming sessions.

### 5. Shutdown, Panics, and Failure Isolation

- Keep request/concurrency permits held until non-cooperative work is actually stopped or isolated.
- Continue isolating Rust background task panics and JavaScript stream failures with explicit reporting where the runtime exposes enough information.

### 6. Browser and WebTransport Compatibility

- Expand browser/runtime compatibility tests for WebTransport behavior that Node-style unit tests cannot cover.
