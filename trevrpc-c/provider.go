package trevrpcc

const (
	ProviderCapabilityEngine       uint64 = 1 << 0
	ProviderCapabilityTransport    uint64 = 1 << 1
	ProviderCapabilityNativeQUIC   uint64 = 1 << 2
	ProviderCapabilityHTTP3        uint64 = 1 << 3
	ProviderCapabilityWebTransport uint64 = 1 << 4
	ProviderCapabilityMultiplexed  uint64 = 1 << 5
)

// ProviderMetadata identifies a native transport provider without selecting it
// globally. Providers are constructed explicitly and may coexist in one process.
type ProviderMetadata struct {
	Name         string
	Version      string
	Capabilities uint64
}

// Provider creates independent low-level Engine and Transport runtimes.
type Provider interface {
	Metadata() ProviderMetadata
	NewEngine(EngineConfig) (EngineRuntime, error)
	NewTransport(TransportConfig) (TransportRuntime, error)
}
