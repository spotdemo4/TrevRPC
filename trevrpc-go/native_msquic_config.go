package trevrpc

import "time"

const (
	nativeMsQuicMaxUint16 int64 = 1<<16 - 1
	nativeMsQuicMaxUint32 int64 = 1<<32 - 1
)

// NativeMsQuicConfig configures the experimental C MsQuic backend.
type NativeMsQuicConfig struct {
	CertFile                      string
	KeyFile                       string
	MaxIdleTimeout                time.Duration
	KeepAlive                     time.Duration
	PeerBidiStreamCount           int
	MaxStatelessOperations        int
	MaxBindingStatelessOperations int
}

// NativeMsQuicConfigFromServerOptions derives native MsQuic transport limits from server options.
func NativeMsQuicConfigFromServerOptions(options ServerOptions) NativeMsQuicConfig {
	maxIdleTimeout := max(options.StreamIdleTimeout, 0)

	return NativeMsQuicConfig{
		MaxIdleTimeout:                maxIdleTimeout,
		KeepAlive:                     nativeMsQuicKeepAlive(maxIdleTimeout),
		PeerBidiStreamCount:           capNativeMsQuicPositiveInt64(quicIncomingStreamLimit(options.MaxConcurrentStreamsPerConnection), nativeMsQuicMaxUint16),
		MaxStatelessOperations:        capNativeMsQuicPositiveInt64(int64(options.MaxConcurrentRequests), nativeMsQuicMaxUint32),
		MaxBindingStatelessOperations: capNativeMsQuicPositiveInt64(int64(options.MaxConcurrentConnections), nativeMsQuicMaxUint16),
	}
}

func nativeMsQuicKeepAlive(maxIdleTimeout time.Duration) time.Duration {
	if maxIdleTimeout <= 0 {
		return 0
	}
	return maxIdleTimeout / 2
}

func capNativeMsQuicPositiveInt64(value, max int64) int {
	if value <= 0 {
		return 0
	}
	if value > max {
		value = max
	}
	maxInt := int64(int(^uint(0) >> 1))
	if value > maxInt {
		return int(maxInt)
	}
	return int(value)
}
