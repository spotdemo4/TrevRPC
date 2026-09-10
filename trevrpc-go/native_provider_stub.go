//go:build !cgo || !linux || (!amd64 && !arm64)

package trevrpc

import trevrpcc "trev.zip/llc/trevrpc/trevrpc-c"

func defaultNativeProvider() trevrpcc.Provider {
	return nil
}
