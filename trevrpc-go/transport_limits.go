package trevrpc

import "math"

const (
	// frameHeaderSize bounds a frame header: a type and a length, each a QUIC
	// variable-length integer of at most 8 bytes.
	frameHeaderSize = 16

	// streamErrorCancelled is the stream error code sent when a call is
	// cancelled locally. Every multiplexed transport (QUIC, WebTransport and
	// QMux) uses the same code.
	streamErrorCancelled = 1

	// defaultConnectionReceiveWindow bounds the data a peer may have in flight
	// on one connection when MaxStreamBodySize sets no smaller bound.
	defaultConnectionReceiveWindow = 16 << 20
)

// frameReceiveWindow is the smallest stream receive window that can hold one
// maximum-size frame.
func frameReceiveWindow(maxFrameSize int) uint64 {
	if maxFrameSize <= 0 {
		return frameHeaderSize
	}
	return saturatingAddUint64(uint64(maxFrameSize), frameHeaderSize)
}

// connectionReceiveWindow bounds the data a peer may have in flight across all
// streams of one connection or session: a frame on every stream, at most
// defaultConnectionReceiveWindow or MaxStreamBodySize when that is smaller, and
// never less than one frame.
func connectionReceiveWindow(options ServerOptions, streamWindow uint64) uint64 {
	connectionWindow := streamWindow
	if options.MaxConcurrentStreamsPerConnection > 1 {
		connectionWindow = saturatingMulUint64(streamWindow, uint64(options.MaxConcurrentStreamsPerConnection))
	}
	bound := uint64(defaultConnectionReceiveWindow)
	if options.MaxStreamBodySize > 0 {
		bound = min(bound, uint64(options.MaxStreamBodySize))
	}
	return max(min(connectionWindow, bound), streamWindow)
}

// streamCreditFactor is how many times MaxConcurrentStreamsPerConnection a
// peer may open as QUIC, WebTransport, or QMux streams. These transports grant
// stream credit cumulatively, and a stream's credit comes back to the client
// about two round trips after it opened the stream: the call takes one, the
// server frees the stream once the client acknowledges its last data, half a
// round trip later, and the MAX_STREAMS frame takes another half to arrive.
// A client with as many one-round-trip calls in flight as the RPC limit
// therefore needs more than twice the limit; with exactly twice, Quinn, which
// also announces credit in batches, held 64 unary calls over a 20 ms round
// trip up to another 17 ms. Four times, as the Rust server allows, leaves room
// for that and for an acknowledgement that a loss delays. The spare streams
// cost per-stream state only: the connection receive window still bounds
// buffered data, and the per-connection RPC limit still rejects an RPC beyond
// it with a status.
const streamCreditFactor = 4

// maxStreamCount is the largest stream count QUIC and QMux can express.
const maxStreamCount = 1 << 60

// serverStreamLimit is how many bidirectional streams a peer may open on one
// QUIC connection, WebTransport session, or QMux session.
func serverStreamLimit(options ServerOptions) int64 {
	limit := saturatingMulUint64(uint64(options.MaxConcurrentStreamsPerConnection), streamCreditFactor)
	return int64(min(limit, maxStreamCount))
}

// http2StreamLimit is SETTINGS_MAX_CONCURRENT_STREAMS. HTTP/2 frees a stream
// as soon as both ends finish it, so one spare stream is enough: it lets an
// over-limit RPC receive a TrevRPC status instead of waiting for a stream. The
// setting is a 32-bit value, and int is only 32 bits wide on some platforms.
func http2StreamLimit(options ServerOptions) int {
	limit := saturatingAddUint64(uint64(options.MaxConcurrentStreamsPerConnection), 1)
	return int(min(limit, math.MaxUint32, math.MaxInt))
}

func saturatingAddUint64(left, right uint64) uint64 {
	if ^uint64(0)-left < right {
		return ^uint64(0)
	}

	return left + right
}

func saturatingMulUint64(left, right uint64) uint64 {
	if left != 0 && right > ^uint64(0)/left {
		return ^uint64(0)
	}

	return left * right
}
