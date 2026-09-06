//go:build trevrpc_native && cgo && (linux || darwin) && (amd64 || arm64)

package native

import "testing"

func TestEngineMsQuicProbe(t *testing.T) {
	if !Available() {
		t.Fatal("Available() = false in native cgo build")
	}

	abi, err := CheckABI()
	if err != nil {
		t.Fatalf("CheckABI() error = %v", err)
	}
	if abi.Engine != EngineABIVersion {
		t.Fatalf("Engine ABI = %d, want %d", abi.Engine, EngineABIVersion)
	}
	if abi.EngineMsQuic != EngineMsQuicABIVersion {
		t.Fatalf("Engine MsQuic ABI = %d, want %d", abi.EngineMsQuic, EngineMsQuicABIVersion)
	}

	runtime, err := Open()
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}

	wake, err := runtime.WakeSource()
	if err != nil {
		_ = runtime.Close()
		t.Fatalf("WakeSource() error = %v", err)
	}
	if wake.Kind != WakeSourcePOSIXFD {
		_ = runtime.Close()
		t.Fatalf("wake source kind = %d, want %d", wake.Kind, WakeSourcePOSIXFD)
	}
	if !wake.Borrowed() {
		_ = runtime.Close()
		t.Fatalf("wake source flags = %#x, want borrowed", wake.Flags)
	}
	if !wake.LevelTriggered() {
		_ = runtime.Close()
		t.Fatalf("wake source flags = %#x, want level-triggered", wake.Flags)
	}

	if err := runtime.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if err := runtime.Close(); err != nil {
		t.Fatalf("second Close() error = %v", err)
	}
	if _, err := runtime.WakeSource(); err == nil {
		t.Fatal("WakeSource() after Close() returned nil error")
	}
}

func TestTransportMsQuicProbe(t *testing.T) {
	abi, err := CheckTransportABI()
	if err != nil {
		t.Fatalf("CheckTransportABI() error = %v", err)
	}
	if abi.Transport != TransportABIVersion {
		t.Fatalf("Transport ABI = %d, want %d", abi.Transport, TransportABIVersion)
	}
	if abi.TransportMsQuic != TransportMsQuicABIVersion {
		t.Fatalf(
			"Transport MsQuic ABI = %d, want %d",
			abi.TransportMsQuic,
			TransportMsQuicABIVersion,
		)
	}

	runtime, err := openTransportRuntime(EngineConfig{})
	if err != nil {
		t.Fatalf("openTransportRuntime() error = %v", err)
	}
	wakes, err := runtime.WakeSources()
	if err != nil {
		t.Fatalf("WakeSources() error = %v", err)
	}
	if len(wakes) < 3 || len(wakes) > maxTransportWakeSources {
		t.Fatalf("wake source count = %d, want 3..%d", len(wakes), maxTransportWakeSources)
	}
	for _, wake := range wakes {
		if wake.Kind != WakeSourcePOSIXFD || !wake.Borrowed() || !wake.LevelTriggered() {
			t.Fatalf("unsupported wake source kind=%d flags=%#x", wake.Kind, wake.Flags)
		}
	}

	engine, err := newDriver(runtime, EngineConfig{}, "Transport")
	if err != nil {
		t.Fatalf("newDriver() error = %v", err)
	}
	if err := engine.Close(); err != nil {
		t.Fatalf("Transport Close() error = %v", err)
	}
}
