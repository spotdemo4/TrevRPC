package trevrpc

import (
	"errors"
	"mime"
	"net/http"
	"runtime/debug"
	"strings"

	transportapi "trev.zip/llc/trevrpc/trevrpc-go/transport"
)

const (
	// DefaultHTTP3Path is the default endpoint for TrevRPC over HTTP/3.
	DefaultHTTP3Path = "/trevrpc"
	// HTTP3ContentType is the media type for TrevRPC request and response bodies.
	HTTP3ContentType = "application/trevrpc"
)

func errorString(err error) string {
	if err == nil {
		return "<nil>"
	}
	return err.Error()
}

func handleTransportHTTP3RPC(
	request transportapi.HTTP3Request,
	server *Server,
	requestLimit,
	streamLimit semaphore,
) {
	runtime := server.freeze()
	if !runtime.options.EnableHTTP3 ||
		request.Path() != http3Path(runtime.options) {
		request.WriteError("404 page not found", http.StatusNotFound)
		return
	}
	if request.Method() != http.MethodPost {
		request.SetResponseHeader("Allow", http.MethodPost)
		request.WriteError("method must be POST", http.StatusMethodNotAllowed)
		return
	}
	if !isTrevRPCMediaType(
		headerFieldValues(request.Headers(), "Content-Type"),
	) {
		request.WriteError(
			"unsupported media type",
			http.StatusUnsupportedMediaType,
		)
		return
	}
	admitted, err := runtime.transportHTTP3Admitted(request)
	if err != nil {
		status := http.StatusInternalServerError
		if errors.Is(err, errAdmissionSaturated) {
			status = http.StatusServiceUnavailable
		}
		request.WriteError("internal server error", status)
		return
	}
	if !admitted {
		request.WriteError("HTTP/3 admission denied", http.StatusForbidden)
		return
	}
	if !tryAcquire(streamLimit) {
		request.WriteError(
			"too many concurrent RPCs on HTTP/3 connection",
			http.StatusServiceUnavailable,
		)
		return
	}
	defer release(streamLimit)

	request.SetResponseHeader("Content-Type", HTTP3ContentType)
	request.WriteResponseHeader(http.StatusOK)
	if err := request.Flush(); err != nil {
		return
	}
	handleRPCStream(
		request.Context(),
		server,
		requestLimit,
		newTransportRPCStream(request.Stream()),
	)
}

func http3Path(options ServerOptions) string {
	if options.HTTP3Path == "" {
		return DefaultHTTP3Path
	}
	return options.HTTP3Path
}

func isTrevRPCMediaType(values []string) bool {
	if len(values) != 1 {
		return false
	}
	mediaType, parameters, err := mime.ParseMediaType(values[0])
	return err == nil && strings.EqualFold(mediaType, HTTP3ContentType) && len(parameters) == 0
}

var errAdmissionSaturated = errors.New("server admission callbacks saturated")

func (r *serverRuntime) transportHTTP3Admitted(
	request transportapi.HTTP3Request,
) (bool, error) {
	return r.invokeHTTP3Admission(HTTP3AdmissionRequest{
		Headers:   request.Headers().Clone(),
		Path:      request.Path(),
		Method:    request.Method(),
		Authority: request.Authority(),
		Secure:    request.Secure(),
	})
}

func (r *serverRuntime) invokeHTTP3Admission(
	request HTTP3AdmissionRequest,
) (admitted bool, err error) {
	callback := r.options.HTTP3Admission
	if callback == nil {
		return true, nil
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
