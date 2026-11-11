package trevrpc

import (
	"context"
	"time"
)

// responseWriteGrace is how long past its deadline a call may still write its
// response. A call that runs out of time ends with a DeadlineExceeded STATUS,
// which must be able to go out after the deadline, to a client that is still
// reading. A client that is not reading, because it stopped or because its
// flow-control window is full, gets no more time than this.
//
// It is a variable only so that tests can shorten it.
var responseWriteGrace = 500 * time.Millisecond

// writeDeadlineStream is a stream whose writes can be bounded in time. A write
// that passes the deadline fails with an error that is os.ErrDeadlineExceeded.
type writeDeadlineStream interface {
	SetWriteDeadline(time.Time) error
}

// contextAbortStream is a stream that can end what is pending on it when a
// context ends: its reads (see contextCancelReadStream) and its writes, for a
// write that waits for flow control would otherwise outlast the shutdown of
// the server, or the client that left.
type contextAbortStream interface {
	trevrpcAbortOnContext(context.Context) func()
}

// abortOnContext makes stream give up its reads, and its writes if it can,
// when ctx, the context of the stream, ends. The returned func stops the
// watch. One watch serves the call, from its first read to the end of its
// response.
func abortOnContext(ctx context.Context, stream rpcStream) func() {
	if aborting, ok := stream.(contextAbortStream); ok {
		return aborting.trevrpcAbortOnContext(ctx)
	}
	return cancelReadOnContext(ctx, stream)
}

// boundResponseWrites bounds the writes of a call's response by the call's
// deadline, plus responseWriteGrace, so that a client that stops reading
// cannot hold the call, its slot and its lease past it. It costs one call per
// call that has a deadline, and the stream's own timer for a write that has to
// wait. ctx is the context of the stream, requestCtx the call's.
func boundResponseWrites(ctx, requestCtx context.Context, stream rpcStream) {
	deadline, ok := requestCtx.Deadline()
	if !ok {
		return
	}
	bounded, ok := stream.(writeDeadlineStream)
	if !ok {
		return
	}
	_ = bounded.SetWriteDeadline(deadline.Add(responseWriteGrace))
	if ctx.Err() != nil {
		// The stream was aborted while the deadline was set, and the abort
		// stands.
		_ = bounded.SetWriteDeadline(time.Now())
	}
}

// SetWriteDeadline bounds the writes to the response, if the HTTP server
// supports it. It belongs to the goroutine of the HTTP handler: the response
// writer must not be used once the handler returns, so no watcher of a context
// calls it. A client that leaves ends the request's context and fails the
// writes by itself.
func (b *httpBody) SetWriteDeadline(deadline time.Time) error {
	return b.controller.SetWriteDeadline(deadline)
}
