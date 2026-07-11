package trevrpc

import "github.com/quic-go/quic-go"

// QUICTransportLimits contains transport-level QUIC flow-control and stream limits.
type QUICTransportLimits struct {
	StreamReceiveWindow     uint64
	ConnectionReceiveWindow uint64
	MaxIncomingStreams      int64
}

// QUICTransportLimitsFromServerOptions builds QUIC transport limits from server options.
func QUICTransportLimitsFromServerOptions(options ServerOptions) QUICTransportLimits {
	limits := transportLimitsFromServerOptions(options)
	return QUICTransportLimits{
		StreamReceiveWindow:     limits.StreamReceiveWindow,
		ConnectionReceiveWindow: limits.ConnectionReceiveWindow,
		MaxIncomingStreams:      limits.IncomingBidiStreams,
	}
}

// QUICServerConfig clones and caps a QUIC server config for TrevRPC traffic.
func QUICServerConfig(options ServerOptions, base *quic.Config) *quic.Config {
	config := cloneQUICConfig(base)
	limits := transportLimitsFromServerOptions(options)
	capQUICReceiveWindows(config, limits.StreamReceiveWindow, limits.ConnectionReceiveWindow)
	capQUICIncomingStreams(&config.MaxIncomingStreams, limits.IncomingBidiStreams)
	if options.EnableWebTransport {
		config.EnableDatagrams = true
		config.EnableStreamResetPartialDelivery = true
	}
	if !options.EnableHTTP3 && !options.EnableWebTransport {
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
	streamWindow := transportFrameReceiveWindow(maxFrameSize)
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

func applyDefaultQUICTransportConfig(config *quic.Config, transport TransportConfig) {
	if config.MaxIdleTimeout == 0 && transport.MaxIdleTimeout > 0 {
		config.MaxIdleTimeout = transport.MaxIdleTimeout
	}
	if config.KeepAlivePeriod == 0 && transport.KeepAlive > 0 {
		config.KeepAlivePeriod = transport.KeepAlive
	}
}
