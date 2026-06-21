# trevrpc-c TODO

This tracks missing C runtime work needed to bring `trevrpc-c` closer to feature parity with `trevrpc-go` and make it suitable as the primary native runtime.

## Runtime Policy

## Authorization And Metadata Policy

## Metrics And Observability

## Error And Status Mapping

## Handler Safety

## Typed Protobuf Runtime

## WebTransport Integration

- Add high-level `trevrpc_client` and `trevrpc_server` support over `trevrpc_wt_*`, not only low-level WebTransport session and stream wrappers.
- Decide whether the public high-level C API should abstract over MsQuic and WebTransport transports or expose separate constructors.
- Add WebTransport C client unary and streaming calls equivalent to the MsQuic-backed `trevrpc_client` APIs.
- Add WebTransport C server accept/serve integration equivalent to `trevrpc_server_serve`.
- Add CTest or integration tests for native WebTransport RPC round trips when libwtf is available.
- Track and remove the draft-07 workaround once libwtf draft-15 stream-credit behavior is fixed.

## Transport And Packaging

## Testing

- Add C integration tests for MsQuic unary and all streaming modes.
- Add C integration tests for server shutdown, client close, stream reset, and partial stream failure.
- Add tests for concurrent streams and concurrent connections under configured limits.
- Add sanitizers in an optional CMake/Nix check: ASan, UBSan, and potentially TSan for queue/close paths.
- Add leak checks for every CTest executable.

## Documentation

## Performance

- Benchmark C wire encode/decode directly.
- Benchmark high-level C runtime unary and streaming paths separately from Go cgo wrappers.
- Add zero-copy decode paths where ownership can be made explicit and safe.
- Add send batching decisions equivalent to Go's non-blocking stream batching.
- Audit allocation counts for request/response/stream frame decode paths.
- Pool reusable buffers only where lifetime and concurrency are unambiguous.

## API Cleanup
