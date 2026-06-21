# trevrpc-c TODO

This tracks missing C runtime work needed to bring `trevrpc-c` closer to feature parity with `trevrpc-go` and make it suitable as the primary native runtime.

## Runtime Policy

## Authorization And Metadata Policy

## Metrics And Observability

## Error And Status Mapping

## Handler Safety

## Typed Protobuf Runtime

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

- Install internal headers only when explicitly requested for tests or development; keep public headers stable.
- Add CMake package config files so external C projects can use `find_package(trevrpc)`.
- Add pkg-config files for installed C libraries.
- Decide static vs shared library support and symbol visibility rules.
- Add ABI/versioning policy for public C headers.

## Testing

- Add invalid-frame corpus tests and fuzz-style tests for C decode paths.
- Add C integration tests for MsQuic unary and all streaming modes.
- Add C integration tests for server shutdown, client close, stream reset, and partial stream failure.
- Add tests for concurrent streams and concurrent connections under configured limits.
- Add tests for large frames near the max-frame-size boundary.
- Add sanitizers in an optional CMake/Nix check: ASan, UBSan, and potentially TSan for queue/close paths.
- Add leak checks for every CTest executable.

## Documentation

- Add C examples for unary, server-streaming, client-streaming, and bidi streaming.

## Performance

- Benchmark C wire encode/decode directly.
- Benchmark high-level C runtime unary and streaming paths separately from Go cgo wrappers.
- Add zero-copy decode paths where ownership can be made explicit and safe.
- Add send batching decisions equivalent to Go's non-blocking stream batching.
- Audit allocation counts for request/response/stream frame decode paths.
- Pool reusable buffers only where lifetime and concurrency are unambiguous.

## API Cleanup

- Review whether `trevrpc_msquic.h` and `trevrpc_webtransport.h` should stay public or move behind an advanced/transport-specific include path.
- Replace raw numeric `kind` and `status` parameters with typed enums if that improves C API clarity without hurting ABI predictability.
- Add explicit close vs shutdown naming consistency across MsQuic, WebTransport, client, server, and stream APIs.
- Decide whether `trevrpc_stream_close` should always abort receive, finish send, or only release resources based on ownership flags.
