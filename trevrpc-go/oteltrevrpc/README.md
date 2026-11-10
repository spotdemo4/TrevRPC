# oteltrevrpc

OpenTelemetry instrumentation for [TrevRPC](https://trev.zip/llc/TrevRPC) Go servers and clients, following the semantic conventions for RPC with `rpc.system.name` `trevrpc`. It is a separate module so that TrevRPC itself does not depend on OpenTelemetry.

> **Compatibility:** every TrevRPC package shares one version, and packages of the same version are compatible
> with each other: `trevrpc-rust` 1.8.0 works with `trevrpc-go` 1.8.0, `trevrpc-js` 1.8.0, and `trevrpc-kotlin`
> 1.8.0. Compatibility across different versions isn't guaranteed yet; that's a milestone for 2.0.

```sh
go get trev.zip/llc/trevrpc/trevrpc-go/oteltrevrpc
```

```go
metrics, err := oteltrevrpc.NewMetrics()
if err != nil {
	return err
}
server.SetMetrics(metrics)
// First, so that its span covers the interceptors after it.
server.SetInterceptors(oteltrevrpc.NewInterceptor())

// On clients, and in handlers that call other servers:
reply, err := client.SayHello(ctx, request, oteltrevrpc.Propagate(ctx))
```

- `NewMetrics` records `rpc.server.call.duration` and the body size histograms `rpc.server.request.size` and `rpc.server.response.size` for every call the server handles, including those it rejects before a handler runs. `rpc.method` is `_OTHER` for methods the server does not register, so clients cannot add series.
- `NewInterceptor` traces each call a handler serves in a server span, which the handler's context carries. It continues a client's trace only with `WithTrustRemote()`; otherwise the span starts a new trace that links to the client's span.
- `Propagate` sends the trace context and baggage of `ctx` as request metadata.

Each uses the global providers and propagator unless `WithMeterProvider`, `WithTracerProvider` or `WithPropagator` sets one. For server diagnostics, use `trevrpc.LogDiagnostics`. See [Observability](https://trev.zip/llc/TrevRPC/wiki/Go-Guide#observability).
