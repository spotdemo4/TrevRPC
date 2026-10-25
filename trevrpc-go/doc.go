// Package trevrpc implements the TrevRPC runtime: an RPC framework like gRPC
// that carries calls over QUIC, with HTTP/3, WebTransport, and TCP fallbacks.
//
// Clients connect with [Dial] and use a [Channel] with generated service
// stubs. Servers register services on a [Server] and serve them with
// [Listen]. Failed calls carry a [Status].
//
// A method can mark the messages of its streams unreliable in its schema, or
// with [WithUnreliable] and [RouteUnreliable]; see [Unreliable]. Those
// messages then travel as QUIC or WebTransport datagrams, and may be lost or
// reordered.
//
// Messages can be compressed with zstd: per message with dictionaries both
// peers hold ([Compression]), per message without a dictionary, which a method
// allows in its schema or with [WithPlainCompression] and
// [RoutePlainCompression] (see [PlainCompression]), and on long streams with
// one context per direction of a call, which a method allows in its schema or
// with [WithStreamCompression] and [RouteStreamCompression]; see
// [StreamCompression].
package trevrpc
