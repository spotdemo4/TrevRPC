//go:build !cgo || (!linux && !darwin)

package cabi

import (
	trevrpcc "trev.zip/llc/trevrpc/trevrpc-c"
	"trev.zip/llc/trevrpc/trevrpc-c/internal/providerabi"
)

func EngineHost() providerabi.EngineHost {
	return providerabi.EngineHost{}
}

func AdoptEngine(trevrpcc.EngineConfig, providerabi.EngineDescriptor) (trevrpcc.EngineRuntime, error) {
	return nil, trevrpcc.ErrUnavailable
}

func AdoptTransport(providerabi.TransportDescriptor) (trevrpcc.TransportRuntime, error) {
	return nil, trevrpcc.ErrUnavailable
}
