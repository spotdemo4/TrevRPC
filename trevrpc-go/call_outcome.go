package trevrpc

import (
	"context"
	"errors"
	"os"
	"time"
)

// A call has one outcome, the code and error its interceptors and metrics see
// and its STATUS carries. It is decided in two steps, each in one place:
//
//   - contextStatus and afterHandler decide how a call whose context ended
//     ends, whatever the handler or its streams returned, and
//   - callRecord.finish reports the outcome once the response was written (or
//     failed to be), so that a response that never reached the client is not
//     reported as one that did.

// serverShutdownStatus is the STATUS of a call that the server's shutdown ended.
// A call that has not started to run is safe to retry elsewhere, and so is one
// that was cut off, for the server it asked is gone.
func serverShutdownStatus() *Status {
	return Unavailable(errServerShutdown.Error())
}

// contextStatus returns the status of a call whose context ended: Unavailable
// for a server that is shutting down, DeadlineExceeded for a call that ran out
// of time, and Cancelled otherwise.
func contextStatus(ctx context.Context) *Status {
	if errors.Is(context.Cause(ctx), errServerShutdown) {
		return serverShutdownStatus()
	}
	return statusFromContextError(ctx.Err())
}

// afterHandler is the one place where the context of a call is checked once a
// handler, or a stream the handler returned, has produced err. A call whose
// context ended while the handler ran ends as its context says, whether the
// handler obeyed it, ignored it or failed because of it; a response produced
// after the call ended is not wanted. A panic is kept, because it is a bug the
// operator must see.
func afterHandler(ctx context.Context, err error) error {
	if ctx.Err() == nil {
		return err
	}
	if _, panicked := errors.AsType[*serverPanicError](err); panicked {
		return err
	}
	return contextStatus(ctx)
}

// callRecord is what the server needs to report the end of a call to the
// call's interceptors and the metrics. A response owns the record of its call
// and finishes it once the response was written.
type callRecord struct {
	runtime         *serverRuntime
	request         *wireRequest
	service         string
	method          string
	requestBodyLen  int
	responseBodyLen int
	startedAt       time.Time
	// reported is set once the end of the call was reported, which happens
	// once.
	reported bool
}

// callOutcome is how a call ended.
type callOutcome struct {
	code Code
	// err is the error the call ended with, nil if it succeeded.
	err error
}

// finish reports the end of the call, once.
func (c *callRecord) finish(outcome callOutcome) {
	if c.reported {
		return
	}
	c.reported = true
	c.runtime.recordFinished(RPCFinished{Service: c.service, Method: c.method, RequestBodyLen: c.requestBodyLen, ResponseBodyLen: c.responseBodyLen, Code: outcome.code, Err: outcome.err, Elapsed: time.Since(c.startedAt)}, c.request)
}

// finishWritten reports the end of a call whose response ended with status,
// and with cause if the call failed with an error, once writing it returned
// writeErr. ctx is the context of the stream the response was written to.
func (c *callRecord) finishWritten(ctx context.Context, status *Status, cause error, writeErr error) {
	if writeErr == nil {
		c.finish(callOutcome{code: status.Code, err: failureCause(cause, status)})
		return
	}
	c.finish(writeFailure(ctx, writeErr))
}

// writeFailure is the outcome of a call whose response could not be written
// as the call decided. What the client did not get is not reported as what the
// handler returned:
//
//   - a response that does not fit the frame limit is ResourceExhausted,
//   - a stream the shutdown or the client ended is as the stream's context says,
//   - a write that ran into the call's deadline is DeadlineExceeded, and
//   - any other failed write means the client is gone: Cancelled.
func writeFailure(ctx context.Context, writeErr error) callOutcome {
	if _, tooLarge := errors.AsType[*FrameTooLargeError](writeErr); tooLarge {
		return callOutcome{code: CodeResourceExhausted, err: writeErr}
	}
	if ctx != nil && ctx.Err() != nil {
		status := contextStatus(ctx)
		return callOutcome{code: status.Code, err: status}
	}
	if errors.Is(writeErr, os.ErrDeadlineExceeded) {
		return callOutcome{code: CodeDeadlineExceeded, err: writeErr}
	}
	return callOutcome{code: CodeCancelled, err: writeErr}
}

// oversizedResponseStatus is the STATUS of a call whose response does not fit
// the frame limit, which the call's own limits cannot change: ResourceExhausted,
// as the client gives a response it cannot take. The connection is healthy, so
// the client is told, instead of seeing a reset it cannot tell from a network
// failure. Such a response is a bug or a misconfiguration of the server, so it
// is also reported to the operator.
func (c *callRecord) oversizedResponseStatus(ctx context.Context, tooLarge *FrameTooLargeError) *Status {
	c.runtime.emitDiagnostic(ServerDiagnostic{Phase: ServerDiagnosticInvalidResponse, Context: diagnosticContext(ctx, c.request), Service: c.service, Method: c.method, Kind: requestKind(c.request), Err: tooLarge})
	return ResourceExhausted("response " + tooLarge.Error())
}
