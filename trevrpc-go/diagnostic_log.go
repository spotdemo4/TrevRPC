package trevrpc

import (
	"context"
	"log/slog"
)

// Level returns how severe a diagnostic of the phase is, for logging it:
//
//   - slog.LevelError for a panic, an error a handler returned that became
//     Internal, an invalid handler response, and a shutdown that did not
//     finish in time;
//   - slog.LevelWarn for a handler that kept running for more than a second
//     after its call ended, and a missing or invalid PROXY header from a
//     trusted proxy, which suggests a misconfigured load balancer;
//   - slog.LevelInfo for a handler that finally returned after its call ended;
//   - slog.LevelDebug for the sessions and connections clients open and close,
//     and failed TLS handshakes, which clients cause.
//
// An unknown phase is slog.LevelWarn.
func (p ServerDiagnosticPhase) Level() slog.Level {
	switch p {
	case ServerDiagnosticHandlerPanic,
		ServerDiagnosticAuthorizerPanic,
		ServerDiagnosticInterceptorPanic,
		ServerDiagnosticRequestStreamPanic,
		ServerDiagnosticResponseStreamPanic,
		ServerDiagnosticMetricsPanic,
		ServerDiagnosticAdmissionPanic,
		ServerDiagnosticInvalidResponse,
		ServerDiagnosticInternalError,
		ServerDiagnosticShutdownIncomplete:
		return slog.LevelError
	case ServerDiagnosticExecutionFinallyExited:
		return slog.LevelInfo
	case ServerDiagnosticSessionConnect,
		ServerDiagnosticSessionAdmission,
		ServerDiagnosticSessionUpgrade,
		ServerDiagnosticSessionUpgradeSuccess,
		ServerDiagnosticSessionClosed,
		ServerDiagnosticHTTP3ConnectionClosed,
		ServerDiagnosticTLSHandshakeFailed:
		return slog.LevelDebug
	default:
		return slog.LevelWarn
	}
}

// LogDiagnostics returns a ServerDiagnosticHandler that logs each diagnostic
// to logger, or slog.Default() when logger is nil, at the level of its phase
// (see ServerDiagnosticPhase.Level). The message is "trevrpc: " and the phase;
// the diagnostic's other fields that are set become attributes. A diagnostic
// about a call is logged with the call's context, ServerDiagnostic.Context, so
// a handler of logger that reads trace IDs or other values from the context
// can tie the record to the call; the context may already be done. Any other
// is logged with context.Background().
func LogDiagnostics(logger *slog.Logger) ServerDiagnosticHandler {
	return func(diagnostic ServerDiagnostic) {
		logger := logger
		if logger == nil {
			logger = slog.Default()
		}
		ctx := diagnostic.Context
		if ctx == nil {
			ctx = context.Background()
		}
		level := diagnostic.Phase.Level()
		if !logger.Enabled(ctx, level) {
			return
		}
		logger.LogAttrs(ctx, level, "trevrpc: "+string(diagnostic.Phase), diagnosticAttrs(diagnostic)...)
	}
}

// diagnosticAttrs returns the fields of diagnostic that are set, other than
// its phase, as log attributes.
func diagnosticAttrs(diagnostic ServerDiagnostic) []slog.Attr {
	attrs := make([]slog.Attr, 0, 8)
	if diagnostic.Message != "" {
		attrs = append(attrs, slog.String("message", diagnostic.Message))
	}
	if diagnostic.RemoteAddr != "" {
		attrs = append(attrs, slog.String("remote_addr", diagnostic.RemoteAddr))
	}
	if diagnostic.Service != "" || diagnostic.Method != "" {
		attrs = append(attrs,
			slog.String("service", diagnostic.Service),
			slog.String("method", diagnostic.Method),
			slog.String("kind", diagnostic.Kind.String()),
		)
	}
	if diagnostic.Err != nil {
		attrs = append(attrs, slog.Any("error", diagnostic.Err))
	}
	if diagnostic.Panic != nil {
		attrs = append(attrs, slog.Any("panic", diagnostic.Panic))
	}
	if len(diagnostic.Stack) > 0 {
		attrs = append(attrs, slog.String("stack", string(diagnostic.Stack)))
	}
	if diagnostic.ActiveExecutionCount != 0 {
		attrs = append(attrs, slog.Int64("active_executions", diagnostic.ActiveExecutionCount))
	}
	if diagnostic.DetachedExecutionCount != 0 {
		attrs = append(attrs, slog.Int64("detached_executions", diagnostic.DetachedExecutionCount))
	}
	if diagnostic.DroppedEventCount != 0 {
		attrs = append(attrs, slog.Uint64("dropped_events", diagnostic.DroppedEventCount))
	}
	return attrs
}
