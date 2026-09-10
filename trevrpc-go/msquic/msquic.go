// Package msquic exposes high-level TrevRPC dialing and listening backed by the
// bundled patched MsQuic provider.
package msquic

import (
	"context"

	provider "trev.zip/llc/trevrpc/trevrpc-c/provider/msquic/v2"
	trevrpc "trev.zip/llc/trevrpc/trevrpc-go"
)

// Backend returns a new endpoint-scoped MsQuic backend value.
func Backend() trevrpc.NativeBackend {
	return trevrpc.NewNativeBackend(provider.New())
}

// Dial establishes a reconnecting channel using the bundled MsQuic provider.
func Dial(
	ctx context.Context,
	target string,
	options trevrpc.DialOptions,
) (*trevrpc.Channel, error) {
	return trevrpc.DialWithBackend(ctx, target, options, Backend())
}

// Listen creates a server listener using the bundled MsQuic provider.
func Listen(
	addr string,
	server *trevrpc.Server,
	options trevrpc.ListenOptions,
) (trevrpc.ServerListener, error) {
	return trevrpc.ListenWithBackend(addr, server, options, Backend())
}
