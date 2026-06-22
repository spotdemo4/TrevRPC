package trevrpc

import "time"

const transportFrameHeaderSize = 4
const maxInt64 = int64(^uint64(0) >> 1)

// TransportConfig configures backend-independent QUIC transport behavior.
type TransportConfig struct {
	MaxIdleTimeout time.Duration
	KeepAlive      time.Duration
}

type transportLimits struct {
	TransportConfig
	StreamReceiveWindow           uint64
	ConnectionReceiveWindow       uint64
	IncomingBidiStreams           int64
	MaxStatelessOperations        int64
	MaxBindingStatelessOperations int64
}

func transportLimitsFromServerOptions(options ServerOptions) transportLimits {
	streamWindow := transportFrameReceiveWindow(options.MaxFrameSize)
	transportConfig := transportConfigFromServerOptions(options)
	return transportLimits{
		TransportConfig:               transportConfig,
		StreamReceiveWindow:           streamWindow,
		ConnectionReceiveWindow:       transportConnectionReceiveWindow(options, streamWindow),
		IncomingBidiStreams:           incomingBidiStreamLimit(options.MaxConcurrentStreamsPerConnection),
		MaxStatelessOperations:        int64(options.MaxConcurrentRequests),
		MaxBindingStatelessOperations: int64(options.MaxConcurrentConnections),
	}
}

func transportConfigFromServerOptions(options ServerOptions) TransportConfig {
	maxIdleTimeout := max(options.StreamIdleTimeout, 0)
	return TransportConfig{MaxIdleTimeout: maxIdleTimeout, KeepAlive: transportKeepAlive(maxIdleTimeout)}
}

func mergeTransportConfig(base, override TransportConfig) TransportConfig {
	if override.MaxIdleTimeout > 0 {
		base.MaxIdleTimeout = override.MaxIdleTimeout
	}
	if override.KeepAlive > 0 {
		base.KeepAlive = override.KeepAlive
	}
	return base
}

func transportKeepAlive(maxIdleTimeout time.Duration) time.Duration {
	if maxIdleTimeout <= 0 {
		return 0
	}
	return maxIdleTimeout / 2
}

func transportFrameReceiveWindow(maxFrameSize int) uint64 {
	if maxFrameSize <= 0 {
		return transportFrameHeaderSize
	}

	return saturatingAddUint64(uint64(maxFrameSize), transportFrameHeaderSize)
}

func transportConnectionReceiveWindow(options ServerOptions, streamWindow uint64) uint64 {
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

func incomingBidiStreamLimit(maxStreams int) int64 {
	if maxStreams <= 0 {
		return 0
	}

	// Keep one extra stream available so over-limit RPCs can receive a TrevRPC status.
	limit := int64(maxStreams)
	if limit == maxInt64 {
		return limit
	}

	return limit + 1
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
