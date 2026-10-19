package trevrpc

import (
	"context"
	"fmt"
	"io"
	"runtime/debug"
	"sync"
	"time"
)

// streamReceiver receives from a stream a handler provides or consumes, so
// that a call ends on time even when the stream does not. Recv returns early
// when the call context ends, or when the stream is idle for longer than the
// idle timeout. A Recv that gave up is detached: it keeps running, and keeps
// its call's admission, until the stream returns.
//
// A stream that can block is read by a goroutine of the receiver's own, which
// lives as long as the call (or until close), instead of one goroutine per
// message.
type streamReceiver[T any] struct {
	stream      MessageStream[T]
	ctx         context.Context
	idleTimeout time.Duration
	direction   string
	lease       *requestExecutionLease

	// inline is set for a stream that never blocks (or that unblocks on its
	// own when ctx ends), which is read without a goroutine.
	inline bool
	// execution stands for the Recv in progress. Recv reuses it, because the
	// goroutine has finished with it before the result is delivered; a
	// receiver that gave up is never used again.
	execution executionReference
	// started, wake, results and timer belong to the goroutine, which is
	// started by the first Recv that needs it. wake and results hold at most
	// one item: Recv waits for the result before it sends the next request.
	started bool
	wake    chan *executionReference
	results chan receiveResult[T]
	timer   *time.Timer
	// abandoned is the error of the Recv that gave up. The stream is not read
	// again, because the goroutine is still inside its last Recv.
	abandoned error

	// done is closed by close, and ends the goroutine. It is nil when inline.
	closeOnce sync.Once
	done      chan struct{}
}

type receiveResult[T any] struct {
	value T
	err   error
}

func newStreamReceiver[T any](ctx context.Context, stream MessageStream[T], idleTimeout time.Duration, direction string, lease *requestExecutionLease) *streamReceiver[T] {
	r := &streamReceiver[T]{
		stream:      stream,
		ctx:         ctx,
		idleTimeout: idleTimeout,
		direction:   direction,
		lease:       lease,
		inline:      isNonBlockingStream(stream) || (idleTimeout <= 0 && streamContextCancelsRecv(stream)),
	}
	if !r.inline {
		r.done = make(chan struct{})
	}
	return r
}

// close ends the goroutine, if there is one. It is safe to call from any
// goroutine, repeatedly, and while Recv runs.
func (r *streamReceiver[T]) close() {
	if r.inline {
		return
	}
	r.closeOnce.Do(func() { close(r.done) })
}

func (r *streamReceiver[T]) phase() ServerDiagnosticPhase {
	if r.direction == "response" {
		return ServerDiagnosticResponseStreamPanic
	}
	return ServerDiagnosticRequestStreamPanic
}

// recv returns the next message of the stream. It must not run concurrently
// with itself.
func (r *streamReceiver[T]) recv() (T, error) {
	var zero T
	if err := r.ctx.Err(); err != nil {
		return zero, statusFromContextError(err)
	}
	if r.inline {
		body, err := recvByte(r.stream, r.direction)
		if ctxErr := r.ctx.Err(); ctxErr != nil {
			return zero, statusFromContextError(ctxErr)
		}
		return body, err
	}
	if r.abandoned != nil {
		return zero, r.abandoned
	}
	r.start()

	execution := r.lease.retainAs(&r.execution, r.phase())
	r.wake <- execution
	var idle <-chan time.Time
	if r.idleTimeout > 0 {
		r.timer.Reset(r.idleTimeout)
		idle = r.timer.C
	}
	select {
	case result := <-r.results:
		if idle != nil {
			r.timer.Stop()
		}
		if ctxErr := r.ctx.Err(); ctxErr != nil {
			return zero, statusFromContextError(ctxErr)
		}
		return result.value, result.err
	case <-r.ctx.Done():
		return zero, r.abandon(execution, statusFromContextError(r.ctx.Err()))
	case <-idle:
		return zero, r.abandon(execution, Unavailable(fmt.Sprintf("%s stream idle timeout", r.direction)))
	case <-r.done:
		// Closed while receiving.
		r.withdraw(execution)
		if ctxErr := r.ctx.Err(); ctxErr != nil {
			return zero, statusFromContextError(ctxErr)
		}
		return zero, io.EOF
	}
}

// abandon gives up the Recv in progress. It runs on as a detached execution,
// unless the goroutine has not started it.
func (r *streamReceiver[T]) abandon(execution *executionReference, err error) error {
	r.abandoned = err
	cancelStreamRead(r.stream)
	if !r.withdraw(execution) {
		execution.detach()
	}
	r.close()
	return err
}

// withdraw takes back a request the goroutine has not picked up, which it
// never will once it has ended, and ends its execution. It reports whether
// the request was still there. A request the goroutine has picked up ends its
// execution itself.
func (r *streamReceiver[T]) withdraw(execution *executionReference) bool {
	select {
	case <-r.wake:
		execution.finish()
		return true
	default:
		return false
	}
}

func (r *streamReceiver[T]) start() {
	if r.started {
		return
	}
	r.started = true
	r.wake = make(chan *executionReference, 1)
	r.results = make(chan receiveResult[T], 1)
	if r.idleTimeout > 0 {
		r.timer = time.NewTimer(r.idleTimeout)
		r.timer.Stop()
	}
	go r.run()
}

func (r *streamReceiver[T]) run() {
	for {
		var execution *executionReference
		select {
		case execution = <-r.wake:
		case <-r.done:
			return
		case <-r.ctx.Done():
			// The goroutine must not outlive the call, whatever the stream does.
			return
		}
		value, err := recvByte(r.stream, r.direction)
		// The execution ends before the result is delivered, so a caller
		// that has the result also finds the admission released.
		execution.finish()
		select {
		case r.results <- receiveResult[T]{value: value, err: err}:
		case <-r.done:
			return
		}
	}
}

func recvByte[T any](stream MessageStream[T], direction string) (body T, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			phase := ServerDiagnosticRequestStreamPanic
			if direction == "response" {
				phase = ServerDiagnosticResponseStreamPanic
			}
			err = &serverPanicError{phase: phase, recovered: recovered, stack: debug.Stack()}
		}
	}()
	return stream.Recv()
}
