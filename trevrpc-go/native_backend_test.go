package trevrpc

import (
	"testing"

	trevrpcc "trev.zip/llc/trevrpc/trevrpc-c"
)

func TestNativeBackendRequiresExplicitProvider(t *testing.T) {
	_, err := (NativeBackend{}).providerWithCapabilities(trevrpcc.ProviderCapabilityEngine)
	status := StatusFromError(err)
	if status.Code != CodeUnimplemented {
		t.Fatalf("providerWithCapabilities() error = %v, want Unimplemented", err)
	}
	if got, want := status.Message, "native transport backend has no provider; use NewNativeBackend to select one"; got != want {
		t.Fatalf("providerWithCapabilities() message = %q, want %q", got, want)
	}
}

func TestNativeBackendValidatesProviderMetadata(t *testing.T) {
	tests := []struct {
		name     string
		metadata trevrpcc.ProviderMetadata
		message  string
	}{
		{
			name:    "missing name",
			message: "native transport provider metadata name is empty",
		},
		{
			name:     "missing version",
			metadata: trevrpcc.ProviderMetadata{Name: "fake"},
			message:  "native transport provider \"fake\" metadata version is empty",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			backend := NewNativeBackend(metadataProvider{metadata: test.metadata})
			_, err := backend.providerWithCapabilities(trevrpcc.ProviderCapabilityEngine)
			status := StatusFromError(err)
			if status.Code != CodeInvalidArgument {
				t.Fatalf("providerWithCapabilities() error = %v, want InvalidArgument", err)
			}
			if status.Message != test.message {
				t.Fatalf("providerWithCapabilities() message = %q, want %q", status.Message, test.message)
			}
		})
	}
}

func TestNativeBackendValidatesProviderCapabilities(t *testing.T) {
	provider := metadataProvider{metadata: trevrpcc.ProviderMetadata{
		Name:         "fake",
		Version:      "1.0.0",
		Capabilities: trevrpcc.ProviderCapabilityEngine,
	}}
	backend := NewNativeBackend(provider)
	got, err := backend.providerWithCapabilities(trevrpcc.ProviderCapabilityEngine)
	if err != nil {
		t.Fatalf("providerWithCapabilities() error = %v", err)
	}
	if got != provider {
		t.Fatalf("providerWithCapabilities() provider = %T, want metadataProvider", got)
	}

	_, err = backend.providerWithCapabilities(
		trevrpcc.ProviderCapabilityEngine | trevrpcc.ProviderCapabilityNativeQUIC,
	)
	status := StatusFromError(err)
	if status.Code != CodeUnimplemented {
		t.Fatalf("providerWithCapabilities() error = %v, want Unimplemented", err)
	}
	if got, want := status.Message, "native transport provider \"fake\" version \"1.0.0\" lacks required capabilities 0x4"; got != want {
		t.Fatalf("providerWithCapabilities() message = %q, want %q", got, want)
	}
}

type metadataProvider struct {
	metadata trevrpcc.ProviderMetadata
}

func (p metadataProvider) Metadata() trevrpcc.ProviderMetadata {
	return p.metadata
}

func (metadataProvider) NewEngine(trevrpcc.EngineConfig) (trevrpcc.EngineRuntime, error) {
	return nil, trevrpcc.ErrUnavailable
}

func (metadataProvider) NewTransport(trevrpcc.TransportConfig) (trevrpcc.TransportRuntime, error) {
	return nil, trevrpcc.ErrUnavailable
}
