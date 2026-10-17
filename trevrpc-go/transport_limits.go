package trevrpc

const (
	// frameHeaderSize bounds a frame header: a type and a length, each a QUIC
	// variable-length integer of at most 8 bytes.
	frameHeaderSize = 16

	// streamErrorCancelled is the stream error code sent when a call is
	// cancelled locally. Every multiplexed transport (QUIC, WebTransport and
	// QMux) uses the same code.
	streamErrorCancelled = 1
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
// streams of one connection or session.
func connectionReceiveWindow(options ServerOptions, streamWindow uint64) uint64 {
	connectionWindow := streamWindow
	if options.MaxConcurrentStreamsPerConnection > 1 {
		connectionWindow = saturatingMulUint64(streamWindow, uint64(options.MaxConcurrentStreamsPerConnection))
	}
	if options.MaxStreamBodySize > 0 {
		streamBodyWindow := uint64(options.MaxStreamBodySize)
		if connectionWindow > streamBodyWindow {
			connectionWindow = streamBodyWindow
		}
		if connectionWindow < streamWindow {
			connectionWindow = streamWindow
		}
	}

	return connectionWindow
}

// serverStreamLimit is how many bidirectional streams a peer may open on one
// connection or session. It keeps one extra stream available so over-limit
// RPCs receive a TrevRPC status instead of blocking in flow control.
func serverStreamLimit(options ServerOptions) int64 {
	return int64(options.MaxConcurrentStreamsPerConnection) + 1
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
