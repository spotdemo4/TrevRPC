//go:build !trevrpc_msquic_native || !cgo

package trevrpc

import "context"

func listenNativeMsQuic(addr string, server *Server, options ListenOptions) (ServerListener, error) {
	_ = addr
	_ = server
	_ = options
	return nil, Unavailable("native MsQuic transport unavailable: build with -tags trevrpc_msquic_native and cgo")
}

func dialNativeMsQuic(ctx context.Context, addr string, options DialOptions, maxFrameSize int) (ClientTransport, error) {
	_ = ctx
	_ = addr
	_ = options
	_ = maxFrameSize
	return nil, Unavailable("native MsQuic transport unavailable: build with -tags trevrpc_msquic_native and cgo")
}
