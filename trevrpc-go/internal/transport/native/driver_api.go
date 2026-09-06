package native

import (
	"errors"
	"fmt"
)

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
	AdmissionHTTP3        uint32 = 13
	AdmissionWebTransport uint32 = 14
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

var errNativeWouldBlock = errors.New("native Engine operation would block")

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

type AdmissionHandler func(AdmissionRequest) uint16

type EndpointConfig struct {
	Protocol                    uint32
	Host                        string
	Port                        uint16
	ServerName                  string
	ALPN                        []byte
	CertificateFile             string
	PrivateKeyFile              string
	CACertificateFile           string
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
	Admission                   AdmissionHandler
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

type EventError struct {
	Status               int
	ProviderErrorCode    uint64
	ApplicationErrorCode uint64
	Local                bool
	Peer                 bool
	PeerReset            bool
	TransportError       bool
	Clean                bool
	Message              string
}

func (e *EventError) Error() string {
	if e.Message != "" {
		return e.Message
	}
	if e.ProviderErrorCode != 0 {
		return fmt.Sprintf("native transport failed with status %d and provider error %#x", e.Status, e.ProviderErrorCode)
	}
	if e.ApplicationErrorCode != 0 {
		return fmt.Sprintf("native transport failed with status %d and application error %#x", e.Status, e.ApplicationErrorCode)
	}
	return fmt.Sprintf("native transport failed with status %d", e.Status)
}
