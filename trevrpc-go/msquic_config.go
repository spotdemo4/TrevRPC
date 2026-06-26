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
	CACertFile                    string
	SkipCertificateValidation     bool
	MaxIdleTimeout                time.Duration
	KeepAlive                     time.Duration
	PeerBidiStreamCount           int
	MaxStatelessOperations        int
	MaxBindingStatelessOperations int
}

// MsQuicConfigFromServerOptions derives MsQuic transport limits from server options.
func MsQuicConfigFromServerOptions(options ServerOptions) MsQuicConfig {
	limits := transportLimitsFromServerOptions(options)

	return MsQuicConfig{
		MaxIdleTimeout:                limits.MaxIdleTimeout,
		KeepAlive:                     limits.KeepAlive,
		PeerBidiStreamCount:           capMsQuicPositiveInt64(limits.IncomingBidiStreams, msQuicMaxUint16),
		MaxStatelessOperations:        capMsQuicPositiveInt64(limits.MaxStatelessOperations, msQuicMaxUint32),
		MaxBindingStatelessOperations: capMsQuicPositiveInt64(limits.MaxBindingStatelessOperations, msQuicMaxUint16),
	}
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
