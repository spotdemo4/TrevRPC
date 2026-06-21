# trevrpc-c TODO

This tracks missing C runtime work needed to bring `trevrpc-c` closer to feature parity with `trevrpc-go` and make it suitable as the primary native runtime.

## Runtime Policy

## Authorization And Metadata Policy

## Metrics And Observability

## Error And Status Mapping

## Handler Safety

## Typed Protobuf Runtime

## WebTransport Integration

- Add high-level `trevrpc_server` support over `trevrpc_wt_*`, not only low-level WebTransport session and stream wrappers.
- Add WebTransport C server accept/serve integration equivalent to `trevrpc_server_serve`.
- Add CTest or integration tests for native WebTransport RPC round trips when libwtf is available.

## Transport And Packaging

## Testing

- Add C integration tests for MsQuic unary and all streaming modes.
- Add C integration tests for server shutdown, client close, stream reset, and partial stream failure.
- Add tests for concurrent streams and concurrent connections under configured limits.

## Documentation

## Performance

## API Cleanup
