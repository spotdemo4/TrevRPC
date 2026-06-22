package trevrpc

import "time"

const (
	msQuicMaxUint16 int64 = 1<<16 - 1
	msQuicMaxUint32 int64 = 1<<32 - 1
)

// MsQuicConfig configures the experimental C MsQuic backend.
type MsQuicConfig struct {
	CertFile                      string
	KeyFile                       string
	MaxIdleTimeout                time.Duration
	KeepAlive                     time.Duration
	PeerBidiStreamCount           int
	MaxStatelessOperations        int
	MaxBindingStatelessOperations int
}

// MsQuicConfigFromServerOptions derives MsQuic transport limits from server options.
func MsQuicConfigFromServerOptions(options ServerOptions) MsQuicConfig {
	maxIdleTimeout := max(options.StreamIdleTimeout, 0)

	return MsQuicConfig{
		MaxIdleTimeout:                maxIdleTimeout,
		KeepAlive:                     msQuicKeepAlive(maxIdleTimeout),
		PeerBidiStreamCount:           capMsQuicPositiveInt64(quicIncomingStreamLimit(options.MaxConcurrentStreamsPerConnection), msQuicMaxUint16),
		MaxStatelessOperations:        capMsQuicPositiveInt64(int64(options.MaxConcurrentRequests), msQuicMaxUint32),
		MaxBindingStatelessOperations: capMsQuicPositiveInt64(int64(options.MaxConcurrentConnections), msQuicMaxUint16),
	}
}

func msQuicKeepAlive(maxIdleTimeout time.Duration) time.Duration {
	if maxIdleTimeout <= 0 {
		return 0
	}
	return maxIdleTimeout / 2
}

func capMsQuicPositiveInt64(value, max int64) int {
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
