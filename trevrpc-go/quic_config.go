package trevrpc

import (
	"crypto/tls"

	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"
)

const (
	// frameHeaderSize is the length prefix that precedes every frame body.
	frameHeaderSize = 4

	defaultSessionCacheSize = 64
	defaultTokenOrigins     = 32
	defaultTokensPerOrigin  = 4

	// streamErrorCancelled is the QUIC stream error code sent when a call is
	// cancelled locally.
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

// clientQUICConfig returns the QUIC configuration for a client connection.
// Servers never open streams toward clients, so incoming streams are disabled.
func clientQUICConfig(base *quic.Config, maxFrameSize int) *quic.Config {
	config := cloneQUICConfig(base)
	window := frameReceiveWindow(maxFrameSize)
	capWindow(&config.InitialStreamReceiveWindow, window)
	capWindow(&config.MaxStreamReceiveWindow, window)
	capWindow(&config.InitialConnectionReceiveWindow, window)
	capWindow(&config.MaxConnectionReceiveWindow, window)
	config.MaxIncomingStreams = -1
	config.MaxIncomingUniStreams = -1
	return hardenQUICConfig(config)
}

// serverQUICConfig returns the QUIC configuration for the listener. Stream and
// connection flow control are derived from the server limits so that memory per
// connection stays bounded by the configured frame and stream sizes.
func serverQUICConfig(base *quic.Config, options ServerOptions) *quic.Config {
	config := cloneQUICConfig(base)
	streamWindow := frameReceiveWindow(options.MaxFrameSize)
	connectionWindow := connectionReceiveWindow(options, streamWindow)
	capWindow(&config.InitialStreamReceiveWindow, streamWindow)
	capWindow(&config.MaxStreamReceiveWindow, streamWindow)
	capWindow(&config.InitialConnectionReceiveWindow, connectionWindow)
	capWindow(&config.MaxConnectionReceiveWindow, connectionWindow)

	// Keep one extra stream available so over-limit RPCs receive a TrevRPC
	// status instead of blocking at the QUIC layer.
	maxStreams := int64(options.MaxConcurrentStreamsPerConnection) + 1
	if config.MaxIncomingStreams <= 0 || config.MaxIncomingStreams > maxStreams {
		config.MaxIncomingStreams = maxStreams
	}

	if options.StreamIdleTimeout > 0 {
		if config.MaxIdleTimeout == 0 {
			config.MaxIdleTimeout = options.StreamIdleTimeout
		}
		if config.KeepAlivePeriod == 0 {
			config.KeepAlivePeriod = options.StreamIdleTimeout / 2
		}
	}

	if options.EnableWebTransport {
		config.EnableDatagrams = true
		config.EnableStreamResetPartialDelivery = true
	}
	if !options.EnableHTTP3 && !options.EnableWebTransport {
		// Only HTTP/3 needs the control and QPACK unidirectional streams.
		config.MaxIncomingUniStreams = -1
	}
	return hardenQUICConfig(config)
}

func cloneQUICConfig(base *quic.Config) *quic.Config {
	if base == nil {
		return &quic.Config{}
	}
	return base.Clone()
}

// hardenQUICConfig applies the settings TrevRPC never lets callers override.
func hardenQUICConfig(config *quic.Config) *quic.Config {
	// TrevRPC deliberately does not attempt 0-RTT: unary calls are not
	// idempotent, and Dial uses quic.DialAddr rather than quic.DialAddrEarly.
	config.Allow0RTT = false
	if config.TokenStore == nil {
		config.TokenStore = quic.NewLRUTokenStore(defaultTokenOrigins, defaultTokensPerOrigin)
	}
	return config
}

func capWindow(value *uint64, limit uint64) {
	if *value == 0 || *value > limit {
		*value = limit
	}
}

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

// serverTLSConfig clones base and restricts it to the ALPN protocols the
// listener serves.
func serverTLSConfig(base *tls.Config, options ServerOptions) (*tls.Config, error) {
	if base == nil || (len(base.Certificates) == 0 && base.GetCertificate == nil && base.GetConfigForClient == nil) {
		return nil, InvalidArgument("ListenOptions.TLSConfig must provide a server certificate")
	}
	config := base.Clone()
	config.NextProtos = []string{ALPN}
	if options.EnableHTTP3 || options.EnableWebTransport {
		config.NextProtos = append(config.NextProtos, http3.NextProtoH3)
	}
	return config, nil
}

// clientTLSConfig clones base for a TrevRPC connection to host.
func clientTLSConfig(base *tls.Config, host string) *tls.Config {
	config := &tls.Config{}
	if base != nil {
		config = base.Clone()
	}
	config.NextProtos = []string{ALPN}
	if config.ServerName == "" {
		config.ServerName = host
	}
	if config.ClientSessionCache == nil {
		config.ClientSessionCache = tls.NewLRUClientSessionCache(defaultSessionCacheSize)
	}
	return config
}
