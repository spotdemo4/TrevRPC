# Rust Milestone 5 migration

Milestone 5 is one intentional breaking migration for streaming, lifecycle, generated services, and
server configuration.

## Streams

The custom `MessageStream<T>` trait and `BoxMessageStream<T>` alias were removed. Public streaming
APIs now use `futures_core::Stream` and:

```rust
pub type BoxStream<T> =
    std::pin::Pin<Box<dyn futures_core::Stream<Item = trevrpc::Result<T>> + Send + 'static>>;
```

Add `futures-util = "0.3"` when using combinators, import `futures_util::StreamExt`, and replace custom
`next` implementations with standard streams such as `stream::iter`, `stream::unfold`, `map`, or
`chain`. `trevrpc::stream::{empty, from_iter, encode, decode}` remain available.

Streaming transports must implement `StreamingRpcTransport` in addition to the unary-only
`RpcTransport`. This makes streaming capability visible in generic bounds.

## Response lifecycle

Server-streaming and caller-provided bidi request streams now return `ResponseStream<T>`. It
implements `Stream<Item = trevrpc::Result<T>>` and commits terminal status only after a terminal
frame followed by clean EOF. After draining it, use `terminal_status()` or `terminal_metadata()`.
Trailing frames or malformed trailing data are errors and do not commit terminal metadata.

`BidirectionalCall::split()` returns independently movable `RequestSender<T>` and
`ResponseStream<T>` halves. The previous `BidirectionalStreamingCall` name remains as a deprecated
type alias. Call `RequestSender::finish()` for a graceful request half-close. Dropping an unfinished
request sender sends cancellation instead.

## Server handlers and cancellation

Generated service methods now receive `trevrpc::server::RequestContext`. Contexts are cloneable and
expose `cancelled()`, `cancellation_source()`, and `cancelled_signal()`. Cancellation sources are
`Deadline`, `PeerReset`, `ConnectionLost`, and `ServerShutdown`; the first observed source wins.

Every successful generated handler result is wrapped in `ResponseEnvelope<T>`. Use
`ResponseEnvelope::new(value)` when no metadata is needed, or `with_metadata` to attach successful
unary metadata or successful terminal stream metadata.

Example unary signature:

```rust
async fn say_hello(
    &self,
    context: trevrpc::server::RequestContext,
    request: HelloRequest,
) -> Result<trevrpc::ResponseEnvelope<HelloReply>, trevrpc::Status>;
```

## Generated clients

Generated clients separate unary and streaming implementation blocks. Unary methods require only
`RpcTransport`; streaming methods require `StreamingRpcTransport`. Methods with explicit options use
the `_with_options` suffix. Regenerate bindings rather than editing old generated signatures.

Generated unary and client-streaming envelope helpers preserve successful metadata. Server-streaming
and bidi calls expose it through `ResponseStream::terminal_metadata()` after clean EOF.

## Owned configuration

`ServerOptions` and `Server` now own HTTP/3 paths, WebTransport paths, allowed authorities, and
allowed origins. Pass `String`, `&str`, arrays, vectors, or other `IntoIterator<Item = Into<String>>`
values directly. Remove `Box::leak`, leaked slices, and other `'static` workarounds.

## Generator failures

The Rust protoc plugin now treats unresolved input and output protobuf descriptors as hard generator
errors. Ensure every referenced message descriptor is included in the `CodeGeneratorRequest`; the
generator no longer guesses a Rust type from an unresolved protobuf name. Relative descriptor names
use protobuf lexical scope lookup, and the default `google.protobuf` mappings match `prost-build`
(including `Empty` as `()` and other well-known messages under `::prost_types`).

When `prost-build` uses a custom `extern_path`, pass the same mapping to the TrevRPC plugin as
`extern_path=.protobuf.package=::rust::path`. Repeat the option for multiple mappings. Generation now
also rejects protobuf packages, declarations, services, and methods that collapse to the same Rust
module or identifier after Prost-compatible name normalization.
