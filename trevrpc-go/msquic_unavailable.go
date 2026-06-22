//go:build !trevrpc_msquic || !cgo

package trevrpc

import "context"

func listenMsQuic(addr string, server *Server, options ListenOptions) (ServerListener, error) {
	_ = addr
	_ = server
	_ = options
	return nil, Unavailable("MsQuic transport unavailable: build with -tags trevrpc_msquic and cgo")
}

func dialMsQuic(ctx context.Context, addr string, options DialOptions, maxFrameSize int) (ClientTransport, error) {
	_ = ctx
	_ = addr
	_ = options
	_ = maxFrameSize
	return nil, Unavailable("MsQuic transport unavailable: build with -tags trevrpc_msquic and cgo")
}
