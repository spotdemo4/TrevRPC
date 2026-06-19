package trevrpc

import "github.com/quic-go/quic-go"

const quicFrameHeaderSize = 4
const maxInt64 = int64(^uint64(0) >> 1)

// QUICTransportLimits contains transport-level QUIC flow-control and stream limits.
type QUICTransportLimits struct {
	StreamReceiveWindow     uint64
	ConnectionReceiveWindow uint64
	MaxIncomingStreams      int64
}

// QUICTransportLimitsFromServerOptions builds QUIC transport limits from server options.
func QUICTransportLimitsFromServerOptions(options ServerOptions) QUICTransportLimits {
	streamWindow := quicFrameReceiveWindow(options.MaxFrameSize)
	return QUICTransportLimits{
		StreamReceiveWindow:     streamWindow,
		ConnectionReceiveWindow: quicConnectionReceiveWindow(options, streamWindow),
		MaxIncomingStreams:      quicIncomingStreamLimit(options.MaxConcurrentStreamsPerConnection),
	}
}

// QUICServerConfig clones and caps a QUIC server config for TrevRPC traffic.
func QUICServerConfig(options ServerOptions, base *quic.Config) *quic.Config {
	config := cloneQUICConfig(base)
	limits := QUICTransportLimitsFromServerOptions(options)
	capQUICReceiveWindows(config, limits.StreamReceiveWindow, limits.ConnectionReceiveWindow)
	capQUICIncomingStreams(&config.MaxIncomingStreams, limits.MaxIncomingStreams)
	if options.EnableWebTransport {
		config.EnableDatagrams = true
		config.EnableStreamResetPartialDelivery = true
	} else {
		disableQUICIncomingUniStreams(config)
	}

	return config
}

// QUICClientConfig clones and caps a QUIC client config for TrevRPC traffic.
func QUICClientConfig(maxFrameSize int, base *quic.Config) *quic.Config {
	return quicClientConfig(maxFrameSize, base, false)
}

// WebTransportQUICClientConfig clones and configures a QUIC client config for WebTransport.
func WebTransportQUICClientConfig(maxFrameSize int, base *quic.Config) *quic.Config {
	config := quicClientConfig(maxFrameSize, base, true)
	config.EnableDatagrams = true
	config.EnableStreamResetPartialDelivery = true
	return config
}

func quicClientConfig(maxFrameSize int, base *quic.Config, allowIncomingUniStreams bool) *quic.Config {
	config := cloneQUICConfig(base)
	streamWindow := quicFrameReceiveWindow(maxFrameSize)
	capQUICReceiveWindows(config, streamWindow, streamWindow)
	disableQUICIncomingStreams(config)
	if !allowIncomingUniStreams {
		disableQUICIncomingUniStreams(config)
	}

	return config
}

func cloneQUICConfig(base *quic.Config) *quic.Config {
	if base == nil {
		return &quic.Config{}
	}

	return base.Clone()
}

func capQUICReceiveWindows(config *quic.Config, streamWindow, connectionWindow uint64) {
	capQUICReceiveWindow(&config.InitialStreamReceiveWindow, streamWindow)
	capQUICReceiveWindow(&config.MaxStreamReceiveWindow, streamWindow)
	capQUICReceiveWindow(&config.InitialConnectionReceiveWindow, connectionWindow)
	capQUICReceiveWindow(&config.MaxConnectionReceiveWindow, connectionWindow)
}

func capQUICReceiveWindow(window *uint64, limit uint64) {
	if *window == 0 || *window > limit {
		*window = limit
	}
}

func capQUICIncomingStreams(streams *int64, limit int64) {
	if limit > 0 && (*streams == 0 || *streams > limit) {
		*streams = limit
	}
}

func disableQUICIncomingStreams(config *quic.Config) {
	if config.MaxIncomingStreams >= 0 {
		config.MaxIncomingStreams = -1
	}
}

func disableQUICIncomingUniStreams(config *quic.Config) {
	if config.MaxIncomingUniStreams >= 0 {
		config.MaxIncomingUniStreams = -1
	}
}

func quicFrameReceiveWindow(maxFrameSize int) uint64 {
	if maxFrameSize <= 0 {
		return quicFrameHeaderSize
	}

	return saturatingAddUint64(uint64(maxFrameSize), quicFrameHeaderSize)
}

func quicConnectionReceiveWindow(options ServerOptions, streamWindow uint64) uint64 {
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

func quicIncomingStreamLimit(maxStreams int) int64 {
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
