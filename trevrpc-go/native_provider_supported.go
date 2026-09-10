//go:build cgo && linux && (amd64 || arm64)

package trevrpc

import (
	trevrpcc "trev.zip/llc/trevrpc/trevrpc-c"
	"trev.zip/llc/trevrpc/trevrpc-c/provider/msquic/v2"
)

func defaultNativeProvider() trevrpcc.Provider {
	return msquic.New()
}
