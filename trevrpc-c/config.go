package trevrpcc

const (
	DefaultEventCapacity                       uint32 = 256
	DefaultListenerCapacity                    uint32 = 16
	DefaultConnectionCapacity                  uint32 = 1024
	DefaultStreamCapacity                      uint32 = 4096
	DefaultReceiveOwnedCount                   uint32 = 4096
	DefaultReceiveOwnedBytes                   uint64 = 64 * 1024 * 1024
	DefaultMaxFrameSize                        uint64 = 4 * 1024 * 1024
	DefaultMaxFieldSectionSize                 uint64 = 64 * 1024
	DefaultMaxPendingSendBytes                 uint64 = 64 * 1024 * 1024
	DefaultMaxPendingSendCount                 uint32 = 1024
	DefaultMaxPendingReceiveBytes              uint64 = 64 * 1024 * 1024
	DefaultMaxPendingReceiveCount              uint32 = 4096
	DefaultUnresolvedStreamCount               uint32 = 64
	DefaultUnresolvedStreamBytes               uint64 = 64 * 1024 * 1024
	DefaultUnresolvedStreamTimeoutMilliseconds uint64 = 5000
	DefaultPeerBidirectionalCount              uint16 = 100
)

const (
	ProtocolAuto         uint32 = 0
	ProtocolNative       uint32 = 1
	ProtocolHTTP3        uint32 = 2
	ProtocolWebTransport uint32 = 3
	ProtocolMultiplexed  uint32 = 4
)

const (
	WebTransportProfileDraft02      uint32 = 1 << 0
	WebTransportProfileDraft07      uint32 = 1 << 1
	WebTransportProfileDraft14      uint32 = 1 << 2
	WebTransportProfileDraft15      uint32 = 1 << 3
	WebTransportProfileAllSupported        = WebTransportProfileDraft02 |
		WebTransportProfileDraft07 |
		WebTransportProfileDraft14 |
		WebTransportProfileDraft15
)

type EngineConfig struct {
	EventCapacity        uint32
	ListenerCapacity     uint32
	ConnectionCapacity   uint32
	StreamCapacity       uint32
	MaxReceiveOwnedCount uint32
	MaxReceiveOwnedBytes uint64
}

func DefaultEngineConfig() EngineConfig {
	return EngineConfig{
		EventCapacity:        DefaultEventCapacity,
		ListenerCapacity:     DefaultListenerCapacity,
		ConnectionCapacity:   DefaultConnectionCapacity,
		StreamCapacity:       DefaultStreamCapacity,
		MaxReceiveOwnedCount: DefaultReceiveOwnedCount,
		MaxReceiveOwnedBytes: DefaultReceiveOwnedBytes,
	}
}

type TransportConfig struct {
	EventCapacity        uint32
	ListenerCapacity     uint32
	ConnectionCapacity   uint32
	StreamCapacity       uint32
	MaxReceiveOwnedCount uint32
	MaxReceiveOwnedBytes uint64
}

func DefaultTransportConfig() TransportConfig {
	return TransportConfig{
		EventCapacity:        DefaultEventCapacity,
		ListenerCapacity:     DefaultListenerCapacity,
		ConnectionCapacity:   DefaultConnectionCapacity,
		StreamCapacity:       DefaultStreamCapacity,
		MaxReceiveOwnedCount: DefaultReceiveOwnedCount,
		MaxReceiveOwnedBytes: DefaultReceiveOwnedBytes,
	}
}

type AdmissionHeader struct {
	Name  string
	Value string
}

type AdmissionRequest struct {
	Kind      uint32
	Method    string
	Path      string
	Authority string
	Origin    string
	Secure    bool
	Headers   []AdmissionHeader
}

type EndpointConfig struct {
	Protocol                    uint32
	Host                        string
	Port                        uint16
	ServerName                  string
	ALPN                        []byte
	CertificateFile             string
	PrivateKeyFile              string
	CACertificateFile           string
	Certificate                 []byte
	PrivateKey                  []byte
	CACertificate               []byte
	SkipCertificateValidation   bool
	DisableSendBuffering        bool
	DeferAdmission              bool
	PeerBidirectionalStreams    uint16
	Path                        string
	Origin                      string
	WebTransportProfiles        uint32
	MaxSessions                 uint32
	MaxPendingSendBytes         uint64
	MaxPendingSendCount         uint32
	MaxPendingReceiveBytes      uint64
	MaxPendingReceiveCount      uint32
	MaxFrameSize                uint64
	MaxFieldSectionSize         uint64
	MaxIdleTimeoutMilliseconds  uint64
	KeepAliveMilliseconds       uint32
	StreamReceiveWindow         uint32
	ConnectionFlowControlWindow uint32
	UnresolvedStreamCount       uint32
	UnresolvedStreamBytes       uint64
	UnresolvedStreamTimeoutMS   uint64
}

func DefaultEndpointConfig() EndpointConfig {
	return EndpointConfig{
		Protocol:                  ProtocolNative,
		PeerBidirectionalStreams:  DefaultPeerBidirectionalCount,
		WebTransportProfiles:      WebTransportProfileAllSupported,
		MaxSessions:               1,
		MaxPendingSendBytes:       DefaultMaxPendingSendBytes,
		MaxPendingSendCount:       DefaultMaxPendingSendCount,
		MaxPendingReceiveBytes:    DefaultMaxPendingReceiveBytes,
		MaxPendingReceiveCount:    DefaultMaxPendingReceiveCount,
		MaxFrameSize:              DefaultMaxFrameSize,
		MaxFieldSectionSize:       DefaultMaxFieldSectionSize,
		UnresolvedStreamCount:     DefaultUnresolvedStreamCount,
		UnresolvedStreamBytes:     DefaultUnresolvedStreamBytes,
		UnresolvedStreamTimeoutMS: DefaultUnresolvedStreamTimeoutMilliseconds,
	}
}
