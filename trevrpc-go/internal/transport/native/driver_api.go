package native

import (
	trevrpcc "trev.zip/llc/trevrpc/trevrpc-c"
)

const (
	DefaultEventCapacity                       = trevrpcc.DefaultEventCapacity
	DefaultListenerCapacity                    = trevrpcc.DefaultListenerCapacity
	DefaultConnectionCapacity                  = trevrpcc.DefaultConnectionCapacity
	DefaultStreamCapacity                      = trevrpcc.DefaultStreamCapacity
	DefaultReceiveOwnedCount                   = trevrpcc.DefaultReceiveOwnedCount
	DefaultReceiveOwnedBytes                   = trevrpcc.DefaultReceiveOwnedBytes
	DefaultMaxFrameSize                        = trevrpcc.DefaultMaxFrameSize
	DefaultMaxFieldSectionSize                 = trevrpcc.DefaultMaxFieldSectionSize
	DefaultMaxPendingSendBytes                 = trevrpcc.DefaultMaxPendingSendBytes
	DefaultMaxPendingSendCount                 = trevrpcc.DefaultMaxPendingSendCount
	DefaultMaxPendingReceiveBytes              = trevrpcc.DefaultMaxPendingReceiveBytes
	DefaultMaxPendingReceiveCount              = trevrpcc.DefaultMaxPendingReceiveCount
	DefaultUnresolvedStreamCount               = trevrpcc.DefaultUnresolvedStreamCount
	DefaultUnresolvedStreamBytes               = trevrpcc.DefaultUnresolvedStreamBytes
	DefaultUnresolvedStreamTimeoutMilliseconds = trevrpcc.DefaultUnresolvedStreamTimeoutMilliseconds
	DefaultPeerBidirectionalCount              = trevrpcc.DefaultPeerBidirectionalCount
)

const (
	ProtocolAuto         = trevrpcc.ProtocolAuto
	ProtocolNative       = trevrpcc.ProtocolNative
	ProtocolHTTP3        = trevrpcc.ProtocolHTTP3
	ProtocolWebTransport = trevrpcc.ProtocolWebTransport
	ProtocolMultiplexed  = trevrpcc.ProtocolMultiplexed
)

const (
	AdmissionHTTP3        = trevrpcc.EventHTTP3Admission
	AdmissionWebTransport = trevrpcc.EventWebTransportAdmission
)

const (
	WebTransportProfileDraft02      = trevrpcc.WebTransportProfileDraft02
	WebTransportProfileDraft07      = trevrpcc.WebTransportProfileDraft07
	WebTransportProfileDraft14      = trevrpcc.WebTransportProfileDraft14
	WebTransportProfileDraft15      = trevrpcc.WebTransportProfileDraft15
	WebTransportProfileAllSupported = trevrpcc.WebTransportProfileAllSupported
)

var errNativeWouldBlock = trevrpcc.ErrWouldBlock

type EngineConfig = trevrpcc.EngineConfig

func DefaultEngineConfig() EngineConfig {
	return trevrpcc.DefaultEngineConfig()
}

type AdmissionHeader = trevrpcc.AdmissionHeader

type AdmissionRequest = trevrpcc.AdmissionRequest

type AdmissionHandler func(AdmissionRequest) uint16

// EndpointConfig augments the provider-neutral endpoint configuration with the
// Go admission callback used by the high-level native server driver.
type EndpointConfig struct {
	trevrpcc.EndpointConfig
	Admission AdmissionHandler
}

func DefaultEndpointConfig() EndpointConfig {
	return EndpointConfig{EndpointConfig: trevrpcc.DefaultEndpointConfig()}
}

func (c EndpointConfig) providerConfig() trevrpcc.EndpointConfig {
	result := c.EndpointConfig
	result.ALPN = append([]byte(nil), result.ALPN...)
	result.Certificate = append([]byte(nil), result.Certificate...)
	result.PrivateKey = append([]byte(nil), result.PrivateKey...)
	result.CACertificate = append([]byte(nil), result.CACertificate...)
	return result
}

type EventError = trevrpcc.EventError
