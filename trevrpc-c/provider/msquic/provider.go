package msquic

import (
	trevrpcc "trev.zip/llc/trevrpc/trevrpc-c"
	"trev.zip/llc/trevrpc/trevrpc-c/provider/msquic/v2/internal/cabi"
)

const Version = "2.6.0-trevrpc.1"

// Provider constructs independent patched-MsQuic Engine and Transport
// runtimes. A zero Provider is ready for use.
type Provider struct{}

var _ trevrpcc.Provider = Provider{}

// New returns a stateless MsQuic provider factory.
func New() Provider { return Provider{} }

func (Provider) Metadata() trevrpcc.ProviderMetadata {
	return trevrpcc.ProviderMetadata{
		Name:    "msquic",
		Version: Version,
		Capabilities: trevrpcc.ProviderCapabilityEngine |
			trevrpcc.ProviderCapabilityTransport |
			trevrpcc.ProviderCapabilityNativeQUIC |
			trevrpcc.ProviderCapabilityHTTP3 |
			trevrpcc.ProviderCapabilityWebTransport |
			trevrpcc.ProviderCapabilityMultiplexed,
	}
}

func (Provider) NewEngine(config trevrpcc.EngineConfig) (trevrpcc.EngineRuntime, error) {
	return cabi.NewEngine(config)
}

func (Provider) NewTransport(config trevrpcc.TransportConfig) (trevrpcc.TransportRuntime, error) {
	return cabi.NewTransport(config)
}
