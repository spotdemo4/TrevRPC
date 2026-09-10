package consumertest

import (
	"errors"
	"testing"

	trevrpcc "trev.zip/llc/trevrpc/trevrpc-c"
)

type unavailableProvider struct{}

func (unavailableProvider) Metadata() trevrpcc.ProviderMetadata {
	return trevrpcc.ProviderMetadata{Name: "unavailable-test"}
}

func (unavailableProvider) NewEngine(trevrpcc.EngineConfig) (trevrpcc.EngineRuntime, error) {
	return nil, trevrpcc.ErrUnavailable
}

func (unavailableProvider) NewTransport(trevrpcc.TransportConfig) (trevrpcc.TransportRuntime, error) {
	return nil, trevrpcc.ErrUnavailable
}

func TestExternalProviderContract(t *testing.T) {
	var provider trevrpcc.Provider = unavailableProvider{}
	if provider.Metadata().Name != "unavailable-test" {
		t.Fatal("provider metadata was not preserved")
	}
	if _, err := provider.NewEngine(trevrpcc.DefaultEngineConfig()); !errors.Is(err, trevrpcc.ErrUnavailable) {
		t.Fatalf("NewEngine error = %v", err)
	}
	if _, err := provider.NewTransport(trevrpcc.DefaultTransportConfig()); !errors.Is(err, trevrpcc.ErrUnavailable) {
		t.Fatalf("NewTransport error = %v", err)
	}
}
