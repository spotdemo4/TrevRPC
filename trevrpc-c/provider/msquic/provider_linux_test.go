//go:build cgo && linux && (amd64 || arm64)

package msquic_test

import (
	"testing"
	"time"

	trevrpcc "trev.zip/llc/trevrpc/trevrpc-c"
	msquic "trev.zip/llc/trevrpc/trevrpc-c/provider/msquic/v2"
)

func TestProviderMetadata(t *testing.T) {
	metadata := msquic.New().Metadata()
	if metadata.Name != "msquic" || metadata.Version != msquic.Version {
		t.Fatalf("Metadata() = %+v", metadata)
	}
	want := trevrpcc.ProviderCapabilityEngine |
		trevrpcc.ProviderCapabilityTransport |
		trevrpcc.ProviderCapabilityNativeQUIC |
		trevrpcc.ProviderCapabilityHTTP3 |
		trevrpcc.ProviderCapabilityWebTransport |
		trevrpcc.ProviderCapabilityMultiplexed
	if metadata.Capabilities != want {
		t.Fatalf("capabilities = %#x, want %#x", metadata.Capabilities, want)
	}
}

func TestProviderRuntimeLifecycle(t *testing.T) {
	provider := msquic.New()
	tests := []struct {
		name string
		open func() (trevrpcc.Runtime, error)
		abi  uint32
	}{
		{
			name: "engine",
			open: func() (trevrpcc.Runtime, error) {
				return provider.NewEngine(trevrpcc.EngineConfig{})
			},
			abi: trevrpcc.EngineABIVersion,
		},
		{
			name: "transport",
			open: func() (trevrpcc.Runtime, error) {
				return provider.NewTransport(trevrpcc.TransportConfig{})
			},
			abi: trevrpcc.TransportABIVersion,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			runtime, err := test.open()
			if err != nil {
				t.Fatal(err)
			}
			if runtime.ABIVersion() != test.abi {
				t.Fatalf("ABI version = %d, want %d", runtime.ABIVersion(), test.abi)
			}
			wakes, err := runtime.WakeSources()
			if err != nil {
				t.Fatal(err)
			}
			if len(wakes) == 0 || wakes[0].Kind != trevrpcc.WakeSourcePOSIXFD {
				t.Fatalf("wake sources = %+v", wakes)
			}
			if _, err := runtime.Diagnostics(); err != nil {
				t.Fatal(err)
			}
			closeRuntime(t, runtime)
		})
	}
}

func TestProviderAPIOwnerCoexistenceAndReacquisition(t *testing.T) {
	provider := msquic.New()
	first, err := provider.NewEngine(trevrpcc.EngineConfig{})
	if err != nil {
		t.Fatal(err)
	}
	second, err := provider.NewEngine(trevrpcc.EngineConfig{})
	if err != nil {
		closeRuntime(t, first)
		t.Fatal(err)
	}
	transport, err := provider.NewTransport(trevrpcc.TransportConfig{})
	if err != nil {
		closeRuntime(t, second)
		closeRuntime(t, first)
		t.Fatal(err)
	}
	closeRuntime(t, second)
	closeRuntime(t, transport)
	closeRuntime(t, first)

	reacquired, err := provider.NewEngine(trevrpcc.EngineConfig{})
	if err != nil {
		t.Fatal(err)
	}
	closeRuntime(t, reacquired)
}

func closeRuntime(t *testing.T, runtime trevrpcc.Runtime) {
	t.Helper()
	if err := runtime.BeginClose(); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		event, ok, err := runtime.NextEvent()
		if err != nil {
			t.Fatal(err)
		}
		if ok && event.Kind == trevrpcc.EventStopped {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for stopped event")
		}
		time.Sleep(time.Millisecond)
	}
	if err := runtime.WaitDrained(); err != nil {
		t.Fatal(err)
	}
	consumed, err := runtime.Release()
	if err != nil || !consumed {
		t.Fatalf("Release() = (%v, %v), want (true, nil)", consumed, err)
	}
}
