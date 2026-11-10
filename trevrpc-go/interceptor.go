package trevrpc

import (
	"context"
	"runtime/debug"
	"slices"
	"sync"
)

// Interceptor runs around the handler of every call, for what each call needs
// once, such as a tracing span, a log line or a rate limit. It runs after the
// authorizer, route lookup and the negotiation of stream compression,
// immediately before the handler, so it runs once per handler invocation: a
// call the client replays after a refusal of optimistic stream compression
// reaches it once, unlike the authorizer (see StreamCompression). It does not
// see request or response messages.
type Interceptor interface {
	// Intercept returns the context the handler gets, which must be ctx or
	// derive from it, and a function the server calls once when the call
	// ends, with how it ended, or nil. A returned error ends the call instead,
	// without running the handler or later interceptors, like a handler
	// error: a *Status is sent to the caller and any other error becomes
	// Internal. The finish functions of earlier interceptors still run.
	//
	// The call's deadline and cancellation apply to Intercept as to the
	// handler; a call that ends while it runs still gets its finish function
	// called once it returns one. info.Metadata is the handler's, and must
	// not be changed.
	Intercept(ctx context.Context, info HandlerContext) (context.Context, func(RPCFinished), error)
}

// InterceptorFunc adapts a function to an Interceptor.
type InterceptorFunc func(ctx context.Context, info HandlerContext) (context.Context, func(RPCFinished), error)

// Intercept calls f(ctx, info).
func (f InterceptorFunc) Intercept(ctx context.Context, info HandlerContext) (context.Context, func(RPCFinished), error) {
	return f(ctx, info)
}

// SetInterceptors installs interceptors that run in order around every
// handler, replacing any installed before; the first is the outermost, whose
// finish function runs last. It panics after first use.
func (s *Server) SetInterceptors(interceptors ...Interceptor) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.requireMutableLocked()
	s.interceptors = append([]Interceptor(nil), interceptors...)
}

// interceptedCall holds the finish functions of a call's interceptors until
// the call ends. A handler goroutine the call's deadline abandoned may run an
// interceptor after the call ended, so a function added then runs at once.
type interceptedCall struct {
	mu       sync.Mutex
	finished bool
	event    RPCFinished
	// ctx is the context the last interceptor that ran returned, nil before
	// the first, for the diagnostics about the call.
	ctx      context.Context
	finishes []func(RPCFinished)
}

// setContext records the context the interceptors have derived so far.
func (c *interceptedCall) setContext(ctx context.Context) {
	c.mu.Lock()
	c.ctx = ctx
	c.mu.Unlock()
}

// context returns the context recorded by setContext, or nil.
func (c *interceptedCall) context() context.Context {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.ctx
}

// add records finish, or runs it if the call already ended.
func (c *interceptedCall) add(r *serverRuntime, request *wireRequest, finish func(RPCFinished)) {
	c.mu.Lock()
	if !c.finished {
		c.finishes = append(c.finishes, finish)
		c.mu.Unlock()
		return
	}
	event := c.event
	c.mu.Unlock()
	r.runFinish(request, finish, event)
}

// finish runs the recorded finish functions, the last added first.
func (c *interceptedCall) finish(r *serverRuntime, request *wireRequest, event RPCFinished) {
	c.mu.Lock()
	if c.finished {
		c.mu.Unlock()
		return
	}
	c.finished = true
	c.event = event
	finishes := c.finishes
	c.finishes = nil
	c.mu.Unlock()
	for _, finish := range slices.Backward(finishes) {
		r.runFinish(request, finish, event)
	}
}

// runFinish calls finish, reporting a panic as a diagnostic.
func (r *serverRuntime) runFinish(request *wireRequest, finish func(RPCFinished), event RPCFinished) {
	defer func() {
		if recovered := recover(); recovered != nil {
			r.emitDiagnostic(ServerDiagnostic{Phase: ServerDiagnosticInterceptorPanic, Context: interceptedContext(request), Service: requestService(request), Method: requestMethod(request), Kind: requestKind(request), Panic: recovered, Stack: debug.Stack()})
		}
	}()
	finish(event)
}

// intercept runs the interceptors of a call on ctx, the handler's context,
// and returns the context the handler gets. It runs where the handler runs,
// so the call's deadline and panic handling cover it.
func (r *serverRuntime) intercept(ctx context.Context, request *wireRequest) (context.Context, error) {
	state, _ := ctx.Value(handlerContextKey{}).(*callState)
	call := request.intercepted
	for _, interceptor := range r.interceptors {
		next, finish, err := invokeInterceptor(ctx, interceptor, state.info)
		if finish != nil {
			call.add(r, request, finish)
		}
		if err != nil {
			call.setContext(ctx)
			return ctx, err
		}
		// The handler's response metadata and stream compression live in
		// the call's state, which a context that does not derive from ctx
		// would lose.
		if !carriesCall(next, state) {
			call.setContext(ctx)
			return ctx, foreignContextStatus("interceptor")
		}
		ctx = next
	}
	call.setContext(ctx)
	return ctx, nil
}

func invokeInterceptor(ctx context.Context, interceptor Interceptor, info HandlerContext) (next context.Context, finish func(RPCFinished), err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = &serverPanicError{phase: ServerDiagnosticInterceptorPanic, recovered: recovered, stack: debug.Stack()}
		}
	}()
	return interceptor.Intercept(ctx, info)
}

// interceptUnary returns handler, run after the interceptors of request when
// the server has any.
func (r *serverRuntime) interceptUnary(handler unaryHandler, request *wireRequest) unaryHandler {
	if len(r.interceptors) == 0 {
		return handler
	}
	request.intercepted = &interceptedCall{}
	return func(ctx context.Context, body []byte) (encodedMessage, error) {
		ctx, err := r.intercept(ctx, request)
		if err != nil {
			return encodedMessage{}, err
		}
		return handler(ctx, body)
	}
}

// interceptStreaming is interceptUnary for a streaming handler.
func (r *serverRuntime) interceptStreaming(handler streamingHandler, request *wireRequest) streamingHandler {
	if len(r.interceptors) == 0 {
		return handler
	}
	request.intercepted = &interceptedCall{}
	return func(ctx context.Context, body []byte, requests byteStream) (encodedStream, error) {
		ctx, err := r.intercept(ctx, request)
		if err != nil {
			return nil, err
		}
		return handler(ctx, body, requests)
	}
}
