package trevrpc

import (
	"context"
	"errors"
	"net/http"
	"runtime/debug"

	transportapi "trev.zip/llc/trevrpc/trevrpc-go/transport"
)

func (r *serverRuntime) transportWebTransportAdmitted(
	request transportapi.WebTransportRequest,
) (bool, error) {
	return r.invokeWebTransportAdmission(WebTransportAdmissionRequest{
		Headers:   request.Headers().Clone(),
		Path:      request.Path(),
		Authority: request.Authority(),
		Origin:    request.Origin(),
		Secure:    request.Secure(),
	})
}

func (r *serverRuntime) invokeWebTransportAdmission(
	request WebTransportAdmissionRequest,
) (admitted bool, err error) {
	callback := r.options.WebTransportAdmission
	if callback == nil {
		return false, nil
	}
	if !tryAcquire(r.admissionLimit) {
		return false, errAdmissionSaturated
	}
	defer release(r.admissionLimit)
	defer func() {
		if recovered := recover(); recovered != nil {
			err = &serverPanicError{
				phase:     ServerDiagnosticAdmissionPanic,
				recovered: recovered,
				stack:     debug.Stack(),
			}
			r.emitDiagnostic(ServerDiagnostic{
				Phase: ServerDiagnosticAdmissionPanic,
				Panic: recovered,
				Stack: debug.Stack(),
				Err:   err,
			})
		}
	}()
	return callback(request), nil
}

func handleWebTransportRequest(
	request transportapi.WebTransportRequest,
	runtime *serverRuntime,
) (transportapi.WebTransportSession, bool) {
	info := "path=" + request.Path() +
		" authority=" + request.Authority() +
		" origin=" + request.Origin() +
		" remote=" + request.RemoteAddress().String()
	runtime.emitDiagnostic(ServerDiagnostic{
		Phase:   ServerDiagnosticWebTransportConnect,
		Message: info,
	})
	admitted, err := runtime.transportWebTransportAdmitted(request)
	if err != nil {
		status := http.StatusInternalServerError
		if errors.Is(err, errAdmissionSaturated) {
			status = http.StatusServiceUnavailable
		}
		runtime.emitDiagnostic(ServerDiagnostic{
			Phase:   ServerDiagnosticWebTransportAdmission,
			Message: "error",
			Err:     err,
		})
		request.WriteError("internal server error", status)
		return nil, false
	}
	if !admitted {
		runtime.emitDiagnostic(ServerDiagnostic{
			Phase:   ServerDiagnosticWebTransportAdmission,
			Message: "denied",
		})
		request.WriteError(
			"WebTransport admission denied",
			http.StatusForbidden,
		)
		return nil, false
	}
	runtime.emitDiagnostic(ServerDiagnostic{
		Phase:   ServerDiagnosticWebTransportAdmission,
		Message: "accepted",
	})

	session, err := request.Upgrade()
	if err != nil {
		runtime.emitDiagnostic(ServerDiagnostic{
			Phase: ServerDiagnosticWebTransportUpgrade,
			Err:   err,
		})
		request.WriteError(
			"WebTransport upgrade failed",
			http.StatusBadRequest,
		)
		return nil, false
	}
	runtime.emitDiagnostic(ServerDiagnostic{
		Phase:   ServerDiagnosticWebTransportUpgradeSuccess,
		Message: "accepted",
	})
	return session, true
}

func endpointCloseReason(
	endpoint transportapi.StreamEndpoint,
	err error,
) TransportCloseReason {
	if mapper, ok := endpoint.(transportapi.CloseReasonMapper); ok {
		return mapper.CloseReason(err)
	}
	return TransportCloseReason{Err: err, Message: errorString(err)}
}

func describeEndpointError(
	endpoint transportapi.StreamEndpoint,
	err error,
) string {
	if describer, ok := endpoint.(transportapi.ErrorDescriber); ok {
		return describer.DescribeError(err)
	}
	return errorString(err)
}

func handleWebTransportSession(
	ctx context.Context,
	session transportapi.WebTransportSession,
	server *Server,
	requestLimit semaphore,
) {
	runtime := server.freeze()
	handleTransportStreamEndpoint(
		ctx,
		session,
		server,
		requestLimit,
		transportStreamEndpointOptions{
			overloadMessage:          "too many concurrent streams on WebTransport session",
			drainTimeoutMessage:      "server WebTransport stream drain timed out",
			shutdownMessage:          "server shutdown",
			closeImmediatelyOnCancel: true,
			onAcceptError: func(err error) {
				reason := endpointCloseReason(session, err)
				if ctx.Err() != nil {
					reason = TransportCloseReason{
						Local:   true,
						Clean:   true,
						Message: "server shutdown",
						Err:     err,
					}
				}
				runtime.emitDiagnostic(ServerDiagnostic{
					Phase:       ServerDiagnosticWebTransportSessionClosed,
					Message:     describeEndpointError(session, err),
					Err:         err,
					Connection:  session.Info(),
					CloseReason: reason,
				})
			},
		},
	)
}

func contextWithAdditionalCancel(ctx context.Context, cancelOn context.Context) (context.Context, context.CancelFunc) {
	if cancelOn.Done() == nil {
		return ctx, func() {}
	}

	ctx, cancel := context.WithCancel(ctx)
	go func() {
		select {
		case <-cancelOn.Done():
			cancel()
		case <-ctx.Done():
		}
	}()

	return ctx, cancel
}
