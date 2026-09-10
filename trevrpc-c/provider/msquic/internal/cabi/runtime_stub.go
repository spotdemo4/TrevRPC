//go:build !cgo || !linux || (!amd64 && !arm64)

package cabi

import (
	"fmt"
	"runtime"

	trevrpcc "trev.zip/llc/trevrpc/trevrpc-c"
)

func NewEngine(trevrpcc.EngineConfig) (trevrpcc.EngineRuntime, error) {
	return nil, unavailableError()
}

func NewTransport(trevrpcc.TransportConfig) (trevrpcc.TransportRuntime, error) {
	return nil, unavailableError()
}

func unavailableError() error {
	return fmt.Errorf(
		"%w: MsQuic requires cgo and a bundled native artifact; unavailable for %s/%s",
		trevrpcc.ErrUnavailable,
		runtime.GOOS,
		runtime.GOARCH,
	)
}
