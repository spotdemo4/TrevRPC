# TrevRPC Unification TODO

This tracks relevant implementation differences found while comparing the C, Go, JavaScript, and Rust client/server paths. It is not a protocol spec; use it to drive implementation, documentation, and conformance work.

Last reviewed: 2026-06-25.

## Unification Decisions

- API unification target: use language-native public APIs with behavioral parity, backed by strong conformance tests after the API-shape changes land.
- Generated JavaScript support: generate typed Node server stubs.
- C generated client support: add generated call-options APIs.
- Call options policy: support both implicit defaults and explicit options everywhere.
- Metadata policy: normalize metadata keys in C convenience APIs too.
- C initial request timeout: implement `initial_request_timeout_nanos` instead of leaving it as a no-op option.
- Handler context policy: expose context, metadata, and deadline information to generated handlers everywhere.
- Transport support: treat Go MsQuic and Node native transport as first-class public support.
- WebTransport enablement: use the C-style default shared listener model for high-level server helpers.
- WebTransport policy shape: use callback policy only.
- TLS defaults: validate certificates by default, with explicit insecure opt-out for development and tests.
- Node certificate options: fully wire `caCertFile` and `skipCertificateValidation` through to native behavior.
- Response stream byte default: align Rust to 16 MiB.
- Limit disable semantics: document per-language behavior rather than forcing identical option syntax.
- Batching constants: benchmark before changing them for symmetry.
- Terminal status metadata: make it high-level supported everywhere.
- Response metadata: add response envelope APIs.
- Stream close/cancel API: document language-specific semantics rather than forcing one API shape.
- Node observability: add authorizer, metrics, and logger APIs.
- C transport observer: distinguish WebTransport events explicitly.
- Conformance scope: defer broad conformance-matrix expansion until the API decisions above are implemented.

## Mostly Aligned Baseline

- Wire version is `1` across implementations.
- Native QUIC ALPN is `trevrpc/1` where native QUIC is exposed.
- Unary and streaming wire messages use the same protobuf-compatible fields.
- Frames use a 4-byte big-endian length prefix and default to a 4 MiB maximum frame body.
- Status codes follow the same gRPC-like numeric values.
- Metadata limits are intended to match: 64 entries, 128-byte keys, 8 KiB values, 64 KiB total, lowercase ASCII keys, and reserved `trevrpc-` prefix.
- Streaming RPCs use one bidirectional transport stream and response streams are expected to end with a terminal status frame.
- Server-side default limits mostly match: 256 connections, 64 streams per connection, 1024 concurrent requests, 4096 stream messages, 16 MiB cumulative stream body, 30 second stream idle timeout, and 30 second graceful shutdown timeout.

## Differences To Resolve

### 1. Generated API Parity

Status: not aligned.

Decision: keep language-native generated API shapes, but provide typed generated client/server coverage for all first-class runtimes where the runtime can serve requests.

- Go generates typed clients, typed server interfaces, and typed server registration for all four RPC shapes. See `trevrpc-go/cmd/protoc-gen-trevrpc-go/main.go`.
- Rust generates typed clients, typed service traits, and typed server registration for all four RPC shapes. See `trevrpc-rust/crates/protoc-gen-trevrpc-rust/src/lib.rs`.
- C generates protobuf-c callback structs, typed unary wrappers, stream start helpers, and send/recv helpers, but generated client methods do not accept metadata, timeout, cancellation, or response-limit options. Low-level C calls can carry those fields through `trevrpc_request`. See `trevrpc-c/tools/protoc-gen-trevrpc-c/main.c` and `trevrpc-c/include/trevrpc.h`.
- JavaScript generates protobuf.js client bindings and TypeScript declarations, but not typed server stubs. Node server support exists through raw `NodeServer` and `NodeServerCall` APIs under `trevrpc-js/node`. See `trevrpc-js/src/generator.js`, `trevrpc-js/src/node.js`, and `trevrpc-js/src/node.d.ts`.

TODO:

- Generate typed Node server stubs and declarations for unary, server-streaming, client-streaming, and bidirectional-streaming RPCs.
- Add C generated call-options APIs so generated clients can set metadata, deadlines, cancellation, and response limits without dropping to `trevrpc_client_call_request`.
- Keep generated method naming and stream helper shapes language-native, then document behavior-level equivalence across languages.

### 2. Client Call Options, Metadata, and Deadlines

Status: partially aligned.

Decision: support both implicit defaults and explicit options everywhere; normalize metadata keys in C convenience APIs; implement C initial request timeouts.

- Go and JavaScript generated clients merge constructor-level defaults with per-call options.
- Rust generated clients require an explicit `CallOptions` value for every call, even when callers just want defaults.
- C generated client wrappers expose no call options; only low-level `trevrpc_request` calls expose metadata, `timeout_nanos`, and cancellation.
- Go, JavaScript, and Rust convenience APIs normalize metadata keys to lowercase before validation. C metadata APIs validate exactly what the caller supplies and do not normalize keys.
- C exposes `initial_request_timeout_nanos` in `trevrpc_server_options`, but the runtime currently only defaults/tests the field and does not enforce it. Go and Rust enforce initial-frame timeouts on native QUIC and WebTransport paths.

TODO:

- Add implicit default-call entry points where only explicit options exist today, especially Rust generated clients.
- Add explicit call-options entry points where only implicit defaults exist today, especially C generated clients.
- Add C metadata normalization helpers for convenience-layer APIs while preserving low-level exact metadata validation where necessary.
- Enforce `initial_request_timeout_nanos` in the C native QUIC and WebTransport server paths.

### 3. Handler Context and Request Information

Status: not aligned.

Decision: expose first-class context, metadata, and deadline information to generated handlers everywhere.

- C typed handlers receive `trevrpc_call_context`, and raw/deferred handlers can inspect `trevrpc_call_request`.
- Go generated handlers receive `context.Context` for deadline/cancellation, but generated handlers do not receive request metadata directly.
- Rust runtime enforces deadlines and cancellation internally, but generated service traits receive only decoded messages or message streams. They do not receive a public request context with metadata, peer information, or deadline access.
- JavaScript Node raw handlers receive `NodeServerCall`, including the raw request object. Browser JavaScript is client-only.

TODO:

- Design a minimal shared handler context contract covering metadata, deadline/cancellation, service/method, and future peer/security data.
- Update Rust generated service traits to accept request context.
- Update Go generated server helpers so handlers can read request metadata through context or a generated request context parameter.
- Update C and generated Node APIs to match the same context fields while staying language-native.

### 4. Transport Coverage and Setup Model

Status: intentionally different today, but not fully documented.

Decision: treat Go MsQuic and Node native as first-class, and use the C-style default shared native QUIC plus WebTransport listener model for high-level server helpers.

- C is MsQuic-centric and `trevrpc_server_listen` creates a shared listener advertising both native `trevrpc/1` and HTTP/3 for WebTransport by default because `webtransport_path` defaults to `/trevrpc`.
- Go defaults to quic-go through `Listen`/`Dial`, has optional cgo MsQuic support behind build tags, and only serves WebTransport when `ServerOptions.EnableWebTransport` is true.
- Rust exposes Quinn-native QUIC and WebTransport-over-Quinn helpers. Callers still configure most TLS/ALPN endpoint details themselves.
- JavaScript browser clients use WebTransport. Node has native client/server support backed by the C/MsQuic addon, but the root package export only exposes `connect()` and `WebTransportClient`; raw native APIs are under `trevrpc-js/node`.
- Rust has no MsQuic backend. C has no Quinn backend. Browser JavaScript cannot expose native QUIC.

TODO:

- Promote Go MsQuic and Node native APIs in docs, tests, and support matrix as first-class transport support.
- Align high-level Go, Rust, and Node server helpers with the shared-listener model where the underlying transport can advertise both native QUIC and HTTP/3.
- Keep low-level transport constructors available for applications that need native-only or WebTransport-only binding.
- Update stale wiki text that still describes JavaScript as browser/client-only.

### 5. WebTransport Policy Shape

Status: not aligned.

Decision: use callback policy only for server-side WebTransport admission.

- Go requires `EnableWebTransport` and rejects upgrades when `WebTransportCheckOrigin` is nil.
- Rust uses static WebTransport path, allowed-authority, and allowed-origin lists on `ServerOptions`.
- C uses `webtransport_path` and `webtransport_origin` in server config, with default path `/trevrpc`.
- Browser JavaScript clients pass browser WebTransport options such as `serverCertificateHashes`.
- Node JavaScript accepts `path` and `origin` connection/listen options through the native addon.

TODO:

- Define one server-side callback contract that can inspect path, authority, origin, request metadata available at admission time, and transport security information.
- Replace or wrap Rust static allowlists with callback policy.
- Replace or wrap C/Node path and origin checks with callback policy while preserving default `/trevrpc` behavior in high-level helpers.
- Keep examples explicit about default-deny production policy even when high-level helpers enable WebTransport by default.

### 6. TLS and Certificate Validation

Status: unsafe/inconsistent defaults need attention.

Decision: certificate validation is safe-by-default, with explicit insecure opt-out; Node certificate options must be fully wired to native behavior.

- C native MsQuic client credentials currently set `QUIC_CREDENTIAL_FLAG_NO_CERTIFICATE_VALIDATION` unconditionally.
- JavaScript Node options expose `caCertFile` and `skipCertificateValidation`, and the native addon parses them, but native behavior is limited by the C transport implementation.
- Go and Rust rely on their TLS stack configuration supplied by the caller.
- Browser WebTransport trust depends on browser certificate validation or explicit certificate hashes.

TODO:

- Make C/MsQuic validate certificates by default.
- Add explicit insecure opt-out options for C and Node native clients, and update local examples/tests to use them only where appropriate.
- Fully wire Node `caCertFile` and `skipCertificateValidation` through to native behavior.
- Add negative certificate-validation tests for C and Node native clients.

### 7. Limits, Batching, and Disable Semantics

Status: partly aligned, with user-visible default differences.

Decision: align Rust response-stream byte defaults to 16 MiB, document per-language disable semantics, and benchmark before changing batching constants.

- Go and JavaScript default `max_response_stream_body_size` to 16 MiB. Rust defaults to 64 MiB.
- C server stream body default is 16 MiB, but generated C clients do not expose response stream limits.
- Go uses negative values to disable some limits; Rust uses `Option`; JavaScript uses option object values; C uses signed integer fields for several limits and zero-valued config defaults in some transport structs.
- Request/response message batching differs: Go and browser JavaScript commonly batch 16 request frames; Rust batches 32; Node native receives batches of 32 and sends batches of 16.

TODO:

- Change Rust default `max_response_stream_body_size` from 64 MiB to 16 MiB.
- Document exact per-language limit disable semantics instead of forcing identical option syntax.
- Add generated C response-limit options through the new call-options APIs.
- Benchmark current batching constants before changing any batch sizes for symmetry.

### 8. Streaming Lifecycle and Terminal Status Metadata

Status: behavior mostly aligns, API shape differs.

Decision: keep language-specific close/cancel APIs, document their semantics, and make terminal status metadata high-level supported everywhere.

- Go response streams expose `Close() error`; bidirectional and client-streaming calls expose `CloseSend`, `CloseAndRecv`, and `Close` where applicable.
- Rust response streams are cancelled by drop; sendable client-streaming calls expose `close_send` and `close_and_recv`, but generic response streams do not expose a close method that can report cleanup errors.
- JavaScript response streams are async iterators; `return()`/early loop exit cancels reads and uploads where applicable.
- C exposes imperative `trevrpc_stream_send_message`, `trevrpc_stream_send_status`, `trevrpc_stream_recv`, `trevrpc_stream_finish_send`, `trevrpc_stream_cancel`, and `trevrpc_stream_close`.
- Go and Rust wire helpers can construct terminal status frames with metadata. C public stream status send APIs only accept code/message, so generated C users cannot attach terminal status metadata through the high-level stream API.
- C generated receive helpers return `0` when a status frame arrives and optionally set an out status code, instead of surfacing an EOF/status abstraction like Go/Rust/JS.

TODO:

- Add C and Node high-level APIs for terminal status metadata.
- Update generated streaming server/client helpers to expose terminal status metadata where the language API can carry it.
- Document language-specific stream cancellation and close semantics in API docs, not only the wiki.
- Add targeted lifecycle tests after the API changes land.

### 9. Response Metadata and Generated Handler Output

Status: wire supports it, high-level APIs are incomplete.

Decision: add response envelope APIs so response metadata is part of stable high-level APIs.

- `RpcResponse` and `RpcStreamFrame` carry metadata in every implementation.
- Generated service APIs mostly return bodies and statuses/errors, not response metadata.
- C unary responses can include metadata through `trevrpc_response`, but generated unary callbacks return protobuf messages rather than a response envelope.
- Go/Rust generated handlers return protobuf messages or streams plus errors/statuses, without a first-class response metadata return value.
- JavaScript generated clients can receive metadata at the raw transport level, but generated method returns focus on decoded bodies/streams.

TODO:

- Design language-native response envelope types for unary responses.
- Design terminal streaming status envelope support for response metadata.
- Update generators so handlers can return metadata and clients can read it without raw transport access.
- Preserve simple body-only convenience methods using implicit empty metadata where practical.

### 10. Authorization, Metrics, Observability, and Failure Isolation

Status: mature in Go/Rust/C, thinner in Node JS.

Decision: add Node authorizer, metrics, and logger APIs; distinguish WebTransport events explicitly in C transport observer callbacks.

- Go, Rust, and C expose server authorizer hooks and metrics/lifecycle callbacks.
- C additionally exposes transport observer and logger callbacks.
- Go and Rust isolate handler/metrics panics and map them to stable statuses.
- JavaScript Node raw server handlers can implement authorization manually, but there is no shared `Authorizer`, metrics, transport observer, or logger API in `trevrpc-js/src/node.js`.

TODO:

- Add Node native server authorizer, metrics, and logger APIs with behavior matching Go/Rust/C concepts.
- Add Node tests for handler rejection/throw behavior, request metadata authorization patterns, and lifecycle events.
- Fix or verify C transport observer event transport kinds so WebTransport events are distinguishable on the shared listener path.

### 11. Documentation and Conformance Coverage Drift

Status: docs lag current implementations.

Decision: update docs as first-class APIs are implemented; defer broad conformance-matrix expansion until the API-shape decisions are implemented.

- `wiki/JavaScript-Guide.md` and `wiki/Current-Limitations.md` still describe JavaScript as browser/client-only, despite Node native client/server APIs in `trevrpc-js/node`.
- `wiki/Protocol-and-Wire-Format.md` says shared golden vectors are tested across Go, JavaScript, and Rust, but C now also consumes `testdata/wire-golden-vectors.txt`.
- Go/Rust native QUIC cross-runtime tests exist, and browser JS WebTransport coverage exists. C native, Node native, and broader WebTransport cross-runtime coverage are still thinner.

TODO:

- Update wiki transport and JavaScript support pages after Node native APIs and generated Node server stubs are promoted.
- Update protocol docs to include C wire golden-vector coverage.
- After API-shape changes land, add a conformance matrix that names which runtime pairs are tested for unary, all streaming shapes, auth failure, malformed initial frame, limits, cancellation, metadata envelopes, TLS failure, and WebTransport policy rejection.
