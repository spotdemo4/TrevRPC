//go:build cgo && (linux || darwin)

package cabi_test

import (
	"errors"
	"testing"
	"unsafe"

	trevrpcc "trev.zip/llc/trevrpc/trevrpc-c"
	"trev.zip/llc/trevrpc/trevrpc-c/internal/cabi"
	"trev.zip/llc/trevrpc/trevrpc-c/internal/cabi/testprovider"
	"trev.zip/llc/trevrpc/trevrpc-c/internal/providerabi"
)

func TestEngineProviderAdoptionAndTeardown(t *testing.T) {
	start := testprovider.EngineDestroyCount()
	first := adoptTestEngine(t, 101)
	second := adoptTestEngine(t, 202)
	for _, runtime := range []trevrpcc.EngineRuntime{first, second} {
		if runtime.ABIVersion() != trevrpcc.EngineABIVersion {
			t.Fatalf("ABI version = %d", runtime.ABIVersion())
		}
		wakes, err := runtime.WakeSources()
		if err != nil {
			t.Fatal(err)
		}
		if len(wakes) != 1 || wakes[0].Kind != trevrpcc.WakeSourcePOSIXFD || !wakes[0].Borrowed() {
			t.Fatalf("unexpected wake sources: %+v", wakes)
		}
		if _, present, err := runtime.NextEvent(); err != nil || present {
			t.Fatalf("NextEvent before close = present %v, err %v", present, err)
		}
		if _, err := runtime.Diagnostics(); err != nil {
			t.Fatal(err)
		}
		if err := runtime.BeginClose(); err != nil {
			t.Fatal(err)
		}
		event, present, err := runtime.NextEvent()
		if err != nil || !present || event.Kind != trevrpcc.EventStopped {
			t.Fatalf("STOPPED event = %+v, present %v, err %v", event, present, err)
		}
		if err := runtime.WaitDrained(); err != nil {
			t.Fatal(err)
		}
		consumed, err := runtime.Release()
		if err != nil || !consumed {
			t.Fatalf("Release = consumed %v, err %v", consumed, err)
		}
	}
	if got := testprovider.EngineDestroyCount(); got != start+2 {
		t.Fatalf("Engine destroy count = %d, want %d", got, start+2)
	}
}

func TestTransportProviderAdoptionAndTeardown(t *testing.T) {
	start := testprovider.TransportDestroyCount()
	outOfOrder := testprovider.TransportDestroyBeforeDrainCount()
	descriptor, err := testprovider.NewTransport()
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := cabi.AdoptTransport(descriptor)
	if err != nil {
		testprovider.DisposeTransport(descriptor)
		t.Fatal(err)
	}
	if runtime.ABIVersion() != trevrpcc.TransportABIVersion || runtime.PollTimeoutMilliseconds() != -1 {
		t.Fatalf("unexpected Transport metadata")
	}
	if _, present, err := runtime.NextEvent(); err != nil || present {
		t.Fatalf("NextEvent = present %v, err %v", present, err)
	}
	if _, err := runtime.Diagnostics(); err != nil {
		t.Fatal(err)
	}
	listener, err := runtime.Listen(trevrpcc.EndpointConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if port, err := runtime.ListenerPort(listener); err != nil || port != 4242 {
		t.Fatalf("ListenerPort = %d, err %v", port, err)
	}
	connection, err := runtime.Dial(trevrpcc.EndpointConfig{}, 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.CancelDial(connection); err != nil {
		t.Fatal(err)
	}
	stream, err := runtime.OpenStream(connection, 2)
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.SendFrame(stream, 3, []byte("frame")); err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.ReceiveFrame(stream); !errors.Is(err, trevrpcc.ErrWouldBlock) {
		t.Fatalf("ReceiveFrame error = %v", err)
	}
	for _, operation := range []func() error{
		func() error { return runtime.FinishStreamSend(stream) },
		func() error { return runtime.AbortStreamRead(stream, 4) },
		func() error { return runtime.AbortStreamWrite(stream, 5) },
		func() error { return runtime.AbortStream(stream, 6) },
		func() error { return runtime.CloseStream(stream) },
		func() error { return runtime.CloseConnection(connection, 7) },
		func() error { return runtime.CloseListener(listener) },
		func() error { return runtime.ReleaseHandle(stream, trevrpcc.ObjectStream) },
		func() error { return runtime.ReleaseHandle(connection, trevrpcc.ObjectConnection) },
		func() error { return runtime.ReleaseHandle(listener, trevrpcc.ObjectListener) },
	} {
		if err := operation(); err != nil {
			t.Fatal(err)
		}
	}
	if err := runtime.BeginClose(); err != nil {
		t.Fatal(err)
	}
	if err := runtime.WaitDrained(); err != nil {
		t.Fatal(err)
	}
	consumed, err := runtime.Release()
	if err != nil || !consumed {
		t.Fatalf("Release = consumed %v, err %v", consumed, err)
	}
	if got := testprovider.TransportDestroyCount(); got != start+1 {
		t.Fatalf("Transport destroy count = %d, want %d", got, start+1)
	}
	if got := testprovider.TransportDestroyBeforeDrainCount(); got != outOfOrder {
		t.Fatalf("Transport destroy-before-drain count = %d, want %d", got, outOfOrder)
	}
}

func TestDescriptorValidation(t *testing.T) {
	engine, err := testprovider.NewEngine(cabi.EngineHost(), 303)
	if err != nil {
		t.Fatal(err)
	}
	engine.StructVersion++
	if _, err := cabi.AdoptEngine(trevrpcc.DefaultEngineConfig(), engine); err == nil {
		testprovider.DisposeEngine(engine)
		t.Fatal("invalid Engine descriptor unexpectedly succeeded")
	}
	testprovider.DisposeEngine(engine)

	testInvalidTransport := func(name string, mutate func(*providerabi.TransportDescriptor)) {
		t.Helper()
		start := testprovider.TransportDestroyCount()
		descriptor, err := testprovider.NewTransport()
		if err != nil {
			t.Fatal(err)
		}
		mutate(&descriptor)
		if _, err := cabi.AdoptTransport(descriptor); err == nil {
			testprovider.DisposeTransport(descriptor)
			t.Fatalf("%s unexpectedly succeeded", name)
		}
		if got := testprovider.TransportDestroyCount(); got != start {
			t.Fatalf("%s consumed provider: destroy count = %d, want %d", name, got, start)
		}
		testprovider.DisposeTransport(descriptor)
	}
	testInvalidTransport("small Transport descriptor", func(descriptor *providerabi.TransportDescriptor) {
		descriptor.StructSize = 1
	})
	testInvalidTransport("new Transport descriptor version", func(descriptor *providerabi.TransportDescriptor) {
		descriptor.StructVersion++
	})
	testInvalidTransport("reserved Transport descriptor", func(descriptor *providerabi.TransportDescriptor) {
		descriptor.Reserved[0] = 1
	})
	testInvalidTransport("small Transport operations", func(descriptor *providerabi.TransportDescriptor) {
		testprovider.SetTransportOperationsSize(*descriptor, 1)
	})
	testInvalidTransport("new Transport operations version", func(descriptor *providerabi.TransportDescriptor) {
		testprovider.SetTransportOperationsVersion(*descriptor, providerabi.StructVersion1+1)
	})
	testInvalidTransport("reserved Transport operations", func(descriptor *providerabi.TransportDescriptor) {
		testprovider.SetTransportOperationsReserved(*descriptor, 1)
	})

	if _, err := cabi.AdoptEngine(trevrpcc.EngineConfig{}, providerabi.EngineDescriptor{
		StructSize: uint32(unsafe.Sizeof(providerabi.EngineDescriptor{})), StructVersion: providerabi.StructVersion1,
	}); err == nil || errors.Is(err, trevrpcc.ErrUnavailable) {
		t.Fatalf("invalid provider fields returned %v", err)
	}
}

func adoptTestEngine(t *testing.T, owner uint64) trevrpcc.EngineRuntime {
	t.Helper()
	descriptor, err := testprovider.NewEngine(cabi.EngineHost(), owner)
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := cabi.AdoptEngine(trevrpcc.DefaultEngineConfig(), descriptor)
	if err != nil {
		testprovider.DisposeEngine(descriptor)
		t.Fatal(err)
	}
	return runtime
}
