package trevrpc

import (
	"bytes"
	"encoding/json"
	"errors"
	"log/slog"
	"testing"
)

func TestServerDiagnosticPhaseLevels(t *testing.T) {
	for phase, want := range map[ServerDiagnosticPhase]slog.Level{
		ServerDiagnosticHandlerPanic:           slog.LevelError,
		ServerDiagnosticAuthorizerPanic:        slog.LevelError,
		ServerDiagnosticInterceptorPanic:       slog.LevelError,
		ServerDiagnosticRequestStreamPanic:     slog.LevelError,
		ServerDiagnosticResponseStreamPanic:    slog.LevelError,
		ServerDiagnosticMetricsPanic:           slog.LevelError,
		ServerDiagnosticAdmissionPanic:         slog.LevelError,
		ServerDiagnosticInvalidResponse:        slog.LevelError,
		ServerDiagnosticInternalError:          slog.LevelError,
		ServerDiagnosticShutdownIncomplete:     slog.LevelError,
		ServerDiagnosticExecutionDetached:      slog.LevelWarn,
		ServerDiagnosticProxyHeaderFailed:      slog.LevelWarn,
		ServerDiagnosticExecutionFinallyExited: slog.LevelInfo,
		ServerDiagnosticSessionConnect:         slog.LevelDebug,
		ServerDiagnosticSessionAdmission:       slog.LevelDebug,
		ServerDiagnosticSessionUpgrade:         slog.LevelDebug,
		ServerDiagnosticSessionUpgradeSuccess:  slog.LevelDebug,
		ServerDiagnosticSessionClosed:          slog.LevelDebug,
		ServerDiagnosticHTTP3ConnectionClosed:  slog.LevelDebug,
		ServerDiagnosticTLSHandshakeFailed:     slog.LevelDebug,
		"unknown":                              slog.LevelWarn,
	} {
		if got := phase.Level(); got != want {
			t.Errorf("%s.Level() = %v, want %v", phase, got, want)
		}
	}
}

func TestLogDiagnosticsLogsSetFieldsAtThePhaseLevel(t *testing.T) {
	var out bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&out, &slog.HandlerOptions{Level: slog.LevelInfo}))
	handler := LogDiagnostics(logger)

	handler(ServerDiagnostic{Phase: ServerDiagnosticSessionClosed, RemoteAddr: "192.0.2.1:5"})
	handler(ServerDiagnostic{
		Phase:                ServerDiagnosticInternalError,
		Service:              "example.Greeter",
		Method:               "SayHello",
		Kind:                 RPCKindServerStreaming,
		Err:                  errors.New("boom"),
		ActiveExecutionCount: 2,
	})

	var record map[string]any
	if err := json.Unmarshal(out.Bytes(), &record); err != nil {
		t.Fatalf("decode %q: %v", out.String(), err)
	}
	want := map[string]any{
		"level":             "ERROR",
		"msg":               "trevrpc: internal_error",
		"service":           "example.Greeter",
		"method":            "SayHello",
		"kind":              "server_streaming",
		"error":             "boom",
		"active_executions": float64(2),
	}
	delete(record, "time")
	if len(record) != len(want) {
		t.Fatalf("record = %v, want %v", record, want)
	}
	for key, value := range want {
		if record[key] != value {
			t.Fatalf("record = %v, want %v", record, want)
		}
	}
}
