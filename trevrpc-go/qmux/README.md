# qmux

A Go implementation of [QMux](https://datatracker.ietf.org/doc/draft-ietf-quic-qmux/) (draft-ietf-quic-qmux-02): QUIC's streams, flow control, stream limits and datagrams carried over a reliable byte stream such as TLS over TCP, or over WebSocket messages ([draft-lcurley-qmux-websocket-00](https://datatracker.ietf.org/doc/draft-lcurley-qmux-websocket/)). Applications built on QUIC streams can use it as a fallback for networks that block UDP.

It has no dependencies outside the standard library. [TrevRPC](https://trev.zip/llc/TrevRPC) uses it for its TCP fallback. TrevRPC's interop suite runs its bidirectional streams against the [`qmux`](https://crates.io/crates/qmux) crate and TrevRPC's JavaScript and Erlang implementations, and all of them are checked against the same golden vectors. Unidirectional streams and datagrams are covered by this module's own tests only.

```sh
go get trev.zip/llc/trevrpc/trevrpc-go/qmux
```

> **Versioning:** the module shares the TrevRPC version and is released with it. QMux is still an Internet-Draft, so
> a new draft revision may change the API and the wire format.

## Use

QMux has no ALPN identifier of its own. The application defines one per draft version, such as `myapp-qmux-02`, and negotiates it with TLS ALPN, or as the WebSocket subprotocol, before it opens the session.

```go
conn, err := tls.Dial("tcp", "example.com:443", &tls.Config{NextProtos: []string{"myapp-qmux-02"}})
if err != nil {
	return err
}
params := qmux.Parameters{
	MaxIdleTimeout:                30 * time.Second,
	InitialMaxData:                1 << 20,
	InitialMaxStreamDataBidiLocal: 256 << 10,
	InitialMaxStreamDataUni:       256 << 10,
	InitialMaxStreamsUni:          16,
	MaxDatagramFrameSize:          1200,
	MaxRecordSize:                 64 << 10,
}
session, err := qmux.Open(ctx, qmux.NewStreamConn(conn, params.MaxRecordSize), qmux.RoleClient,
	qmux.Config{Parameters: params, KeepAlivePeriod: 10 * time.Second})
if err != nil {
	return err
}
defer session.CloseWithError(0, "")

stream, err := session.OpenStreamSync(ctx)
if err != nil {
	return err
}
_, err = stream.Write([]byte("hello"))
```

A server does the same with `qmux.RoleServer` on each accepted connection, and serves `session.AcceptStream`, `session.AcceptUniStream` and `session.ReceiveDatagram`.

- **Streams.** `OpenStream`/`OpenStreamSync` and `AcceptStream` return a bidirectional `*Stream`. `OpenUniStream`/`OpenUniStreamSync` return a send-only `*SendStream`, and `AcceptUniStream` returns a receive-only `*ReceiveStream`. The `Sync` variants wait for the peer's stream limit; the others fail with `ErrStreamLimitReached`. Streams follow quic-go's API: `Close` sends a FIN, `CancelWrite` and `CancelRead` send RESET_STREAM and STOP_SENDING, and deadlines bound `Read` and `Write`.
- **Datagrams** (RFC 9221) are off unless both endpoints set `MaxDatagramFrameSize`. `SendDatagram` fails with `ErrDatagramsDisabled` or `ErrDatagramTooLarge` (see `MaxDatagramSize`). As over QUIC, a datagram may be dropped when too many wait to be sent or received; unlike over QUIC, one that is sent is delivered reliably and in order.
- **Transports.** `NewStreamConn` frames records with their Size prefix over a `net.Conn`, and closes a `*tls.Conn` with close_notify. `NewMessageConn` carries one record per message over any `MessageConn`, such as a WebSocket connection. Other transports can implement `RecordConn` directly.
- **Errors.** A session ends with an `*ApplicationError` (CONNECTION_CLOSE from either application), a `*TransportError` (a QMux protocol error), `ErrIdleTimeout` or `ErrConnectionLost`. `Session.Context()` is cancelled with that cause. Stream operations fail with a `*StreamError` after a reset or stop.

Not supported: 0-RTT, and the RESET_STREAM_AT extension (draft-02 §9.2). Neither is advertised, so peers do not use them.
