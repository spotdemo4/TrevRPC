package trevrpc

import (
	"context"
	"errors"

	"github.com/quic-go/quic-go"
	"github.com/quic-go/webtransport-go"
	"trev.zip/llc/trevrpc/trevrpc-go/internal/qmux"
)

// resetByPeer reports whether err is the reset of a stream by the peer, over
// QUIC, WebTransport or QMux, whatever its error code. Clients reset the
// request side of a call they abandon with streamErrorCancelled, and older
// browser clients with 0.
func resetByPeer(err error) bool {
	if quicErr, ok := errors.AsType[*quic.StreamError](err); ok {
		return quicErr.Remote
	}
	if wtErr, ok := errors.AsType[*webtransport.StreamError](err); ok {
		return wtErr.Remote
	}
	if qmuxErr, ok := errors.AsType[*qmux.StreamError](err); ok {
		return qmuxErr.Remote
	}
	return false
}

// requestReadStatus is the status a failed read of a call's request ends the
// call with. A call whose context is done ends as the context says. A request
// stream that the client reset before its end means that the client abandoned
// the call, so it is Cancelled, not a transport failure: the client often resets
// the request side before the reset of the response side cancels the context.
// Any other error is classified by transportStatus.
func requestReadStatus(ctx context.Context, err error) error {
	if status, ok := errors.AsType[*Status](err); ok {
		return status
	}
	if ctx != nil {
		if ctx.Err() != nil {
			return contextStatus(ctx)
		}
	}
	if resetByPeer(err) {
		return Cancelled("RPC cancelled by the client")
	}
	return transportStatus(err)
}
