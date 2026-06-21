# trevrpc-c TODO

This tracks missing C runtime work needed to bring `trevrpc-c` closer to feature parity with `trevrpc-go` and make it suitable as the primary native runtime.

## Protocol Parity

- Decide unknown-field preservation behavior for metadata map entries and add CTest coverage for that decision.
- Add explicit timeout/deadline handling for request `timeout_nanos` instead of only preserving the field.

## Runtime Policy

- Add C server options equivalent to Go `ServerOptions`: max concurrent connections, max concurrent streams per connection, max concurrent requests, graceful shutdown timeout, initial request timeout, max stream messages, max stream body size, and stream idle timeout.
- Enforce max stream message count for request and response streams.
- Enforce max total stream body size for request and response streams.
- Enforce stream idle timeout while receiving request streams and while sending response streams.
- Add overload behavior matching Go, including accepting enough streams to return an explicit resource-exhausted status where possible.
- Add graceful shutdown behavior that stops accepting new work, waits for active work, then force-closes remaining sessions after timeout.
- Define and document which C APIs are blocking, which are cancel-safe, and which can be called concurrently.

## Authorization And Metadata Policy

- Add a server authorizer callback that runs after protocol validation and before route dispatch.
- Add helpers equivalent to Go metadata-value and bearer-token authorizers.
- Decide whether authorizers receive decoded metadata only or the full `trevrpc_request`.
- Add tests for unauthenticated, permission-denied, and authorizer failure paths.

## Metrics And Observability

- Add metrics callbacks equivalent to Go `RPCStarted` and `RPCFinished`.
- Include service, method, request body length, response body length, status code, and elapsed time in metrics events.
- Add transport lifecycle callbacks for listener/session/connection/stream open, close, and error events.
- Add optional structured log callback support for the C runtime, not only the libwtf wrapper.
- Add tests proving metrics fire exactly once on success, handler failure, cancellation, and decode errors.

## Error And Status Mapping

- Audit C status/error mapping against Go `StatusFromError` behavior.
- Add first-class helpers for creating common statuses in C, not only numeric constants.
- Normalize transport errors from MsQuic and WebTransport into predictable TrevRPC status responses.
- Add tests for invalid frame, unsupported wire version, unsupported RPC kind, frame too large, handler failure, transport close, and cancellation paths.
- Decide whether `TREVRPC_ERR_FRAME_TOO_LARGE` should remain a TrevRPC error or map directly to transport-specific frame-too-large codes internally.

## Handler Safety

- Document that C handler crashes are process crashes unless the caller isolates them.
- Add defensive checks around handler return values and response ownership.
- Ensure every handler path sends exactly one terminal status for streaming RPCs.
- Add tests for handlers returning errors before and after partial streaming responses.
- Add tests for handlers that omit response bodies, status messages, or stream terminal status.

## Typed Protobuf Runtime

- Decide the boundary between generated service code and runtime-owned typed helpers.
- Add generated C helpers for unary, server-streaming, client-streaming, and bidi client calls.
- Add generated C server registration helpers that decode protobuf-c messages and encode protobuf-c responses.
- Add generated C stream wrappers that expose typed `send` and `recv` functions around `trevrpc_stream`.
- Add tests for generated C services using protobuf-c messages across all RPC shapes.
- Add examples showing C client and C server usage with generated code.

## WebTransport Integration

- Add high-level `trevrpc_client` and `trevrpc_server` support over `trevrpc_wt_*`, not only low-level WebTransport session and stream wrappers.
- Decide whether the public high-level C API should abstract over MsQuic and WebTransport transports or expose separate constructors.
- Add WebTransport C client unary and streaming calls equivalent to the MsQuic-backed `trevrpc_client` APIs.
- Add WebTransport C server accept/serve integration equivalent to `trevrpc_server_serve`.
- Add CTest or integration tests for native WebTransport RPC round trips when libwtf is available.
- Track and remove the draft-07 workaround once libwtf draft-15 stream-credit behavior is fixed.

## Transport And Packaging

- Build and install a complete C runtime library, not only `libtrevrpc_core.a`.
- Split installable libraries deliberately: core wire/value library, MsQuic transport library, WebTransport transport library, and optional high-level runtime library.
- Install internal headers only when explicitly requested for tests or development; keep public headers stable.
- Add CMake options for MsQuic and WebTransport transport builds.
- Add CMake package config files so external C projects can use `find_package(trevrpc)`.
- Add pkg-config files for installed C libraries.
- Decide static vs shared library support and symbol visibility rules.
- Add ABI/versioning policy for public C headers.

## Testing

- Expand CTest coverage for response helpers and stream-frame ownership helpers.
- Add C wire golden-vector tests shared with Go and Rust.
- Add invalid-frame corpus tests and fuzz-style tests for C decode paths.
- Add C integration tests for MsQuic unary and all streaming modes.
- Add C integration tests for server shutdown, client close, stream reset, and partial stream failure.
- Add tests for concurrent streams and concurrent connections under configured limits.
- Add tests for large frames near the max-frame-size boundary.
- Add sanitizers in an optional CMake/Nix check: ASan, UBSan, and potentially TSan for queue/close paths.
- Add leak checks for every CTest executable.

## Documentation

- Document the public C API ownership model for every pointer returned by the runtime.
- Document thread-safety guarantees for client, server, stream, listener, connection, and WebTransport objects.
- Document the difference between `trevrpc-c` high-level RPC APIs and low-level `trevrpc_msquic_*` / `trevrpc_wt_*` transport APIs.
- Add C examples for unary, server-streaming, client-streaming, and bidi streaming.
- Add build instructions for CMake, Nix, and downstream consumers.
- Add a compatibility matrix showing which Go runtime features exist in C.

## Performance

- Benchmark C wire encode/decode directly.
- Benchmark high-level C runtime unary and streaming paths separately from Go cgo wrappers.
- Add zero-copy decode paths where ownership can be made explicit and safe.
- Add send batching decisions equivalent to Go's non-blocking stream batching.
- Audit allocation counts for request/response/stream frame decode paths.
- Pool reusable buffers only where lifetime and concurrency are unambiguous.

## API Cleanup

- Consider renaming `src/trevrpc_wire.h` to `src/trevrpc_wire_internal.h` to make the internal boundary clear.
- Review whether `trevrpc_msquic.h` and `trevrpc_webtransport.h` should stay public or move behind an advanced/transport-specific include path.
- Replace raw numeric `kind` and `status` parameters with typed enums if that improves C API clarity without hurting ABI predictability.
- Add explicit close vs shutdown naming consistency across MsQuic, WebTransport, client, server, and stream APIs.
- Decide whether `trevrpc_stream_close` should always abort receive, finish send, or only release resources based on ownership flags.
