//go:build !trevrpc_webtransport_native || !cgo

package main

import "errors"

func runNativeWebTransportServer(addr, certFile, keyFile, origin string) error {
	_ = addr
	_ = certFile
	_ = keyFile
	_ = origin
	return errors.New("webtransport-msquic server unavailable: build with -tags 'trevrpc_msquic trevrpc_webtransport_native' and cgo")
}
