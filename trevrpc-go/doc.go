// Package trevrpc implements the TrevRPC runtime: an RPC framework like gRPC
// that carries calls over QUIC, with HTTP/3, WebTransport, and TCP fallbacks.
//
// Clients connect with [Dial] and use a [Channel] with generated service
// stubs. Servers register services on a [Server] and serve them with
// [Listen]. Failed calls carry a [Status].
package trevrpc
