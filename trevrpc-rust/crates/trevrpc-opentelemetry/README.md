# trevrpc-opentelemetry

`OpenTelemetry` instrumentation for [TrevRPC](https://trev.zip/llc/TrevRPC) servers and clients, following the semantic conventions for RPC with `rpc.system.name` `trevrpc`. It is a separate crate so that `trevrpc` itself does not depend on `OpenTelemetry`.

> **Compatibility:** every TrevRPC package shares one version, and packages of the same version are compatible
> with each other: `trevrpc-rust` 1.7.0 works with `trevrpc-go` 1.7.0, `trevrpc-js` 1.7.0, and `trevrpc-kotlin`
> 1.7.0. Compatibility across different versions isn't guaranteed yet; that's a milestone for 2.0.

```rust
use trevrpc_opentelemetry::{Interceptor, Metrics};

let mut server = trevrpc::server::Server::new();
server.set_metrics(Metrics::global());
// First, so that its span covers the interceptors after it.
server.add_interceptor(Interceptor::new());

// On clients, and in handlers that call other servers:
let reply = client
    .say_hello(&request)
    .options(trevrpc_opentelemetry::propagate)
    .await?;
```

- `Metrics` records `rpc.server.call.duration` and the body size histograms `rpc.server.request.size` and `rpc.server.response.size` with the `OpenTelemetry` metrics API, for every call the server handles, including those it rejects before a handler runs. `rpc.method` is `_OTHER` for methods the server does not register, so clients cannot add series. `Metrics::new(&meter)` records with a meter of your choice.
- `Interceptor` traces each call a handler serves in a `tracing` span, in which the handler and its response stream run. With a [`tracing-opentelemetry`](https://docs.rs/tracing-opentelemetry) layer it becomes a server span. It continues a client's trace only with `with_trust_remote(true)`; otherwise the span links to the client's span.
- `propagate` adds the trace context and baggage of the current `tracing` span to a call's metadata.

The interceptor and `propagate` use the global propagator unless `with_propagator` or `propagate_with` sets one; it propagates nothing until `opentelemetry::global::set_text_map_propagator` sets one. See [Observability](https://trev.zip/llc/TrevRPC/wiki/Rust-Guide#observability).
