//go:build !cgo || (!linux && !darwin)

package cabi

import (
	"errors"
	"testing"

	trevrpcc "trev.zip/llc/trevrpc/trevrpc-c"
	"trev.zip/llc/trevrpc/trevrpc-c/internal/providerabi"
)

func TestUnavailableWithoutCgo(t *testing.T) {
	if host := EngineHost(); host.Operations != nil {
		t.Fatalf("EngineHost() = %+v", host)
	}
	if _, err := AdoptEngine(trevrpcc.EngineConfig{}, providerabi.EngineDescriptor{}); !errors.Is(err, trevrpcc.ErrUnavailable) {
		t.Fatalf("AdoptEngine error = %v", err)
	}
	if _, err := AdoptTransport(providerabi.TransportDescriptor{}); !errors.Is(err, trevrpcc.ErrUnavailable) {
		t.Fatalf("AdoptTransport error = %v", err)
	}
}
