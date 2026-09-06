//go:build trevrpc_native && cgo && (linux || darwin) && (amd64 || arm64)

package native

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	transportinternal "trev.zip/llc/trevrpc/trevrpc-go/internal/transport"
)

func TestStoppedErrorIsNonNilAfterCleanShutdown(t *testing.T) {
	engine := &Engine{}
	if err := engine.stoppedError(); !errors.Is(err, errEngineClosed) {
		t.Fatalf("stoppedError() = %v, want errEngineClosed", err)
	}
	expected := errors.New("native failure")
	engine.closeErr = expected
	if err := engine.stoppedError(); !errors.Is(err, expected) {
		t.Fatalf("stoppedError() = %v, want %v", err, expected)
	}
}

func TestCleanupCommandWaitsForDriverCapacity(t *testing.T) {
	engine := newScriptedDriverEngine(t, &scriptedDriverRuntime{})
	engine.commands = make(chan driverCommand, 1)
	engine.commands <- func(*driverState) {}

	admitted := make(chan struct{})
	go func() {
		engine.enqueueCleanup(func(*driverState) {})
		close(admitted)
	}()
	select {
	case <-admitted:
		t.Fatal("cleanup command bypassed the full command queue")
	case <-time.After(20 * time.Millisecond):
	}

	<-engine.commands
	select {
	case <-admitted:
	case <-time.After(time.Second):
		t.Fatal("cleanup command was lost after capacity returned")
	}
	select {
	case <-engine.commands:
	default:
		t.Fatal("cleanup command did not enter the command queue")
	}
}

func TestCloseUnstartedRuntimeReleasesAfterDrainFailure(t *testing.T) {
	tests := []struct {
		name      string
		configure func(*scriptedDriverRuntime, error)
	}{
		{
			name: "event drain",
			configure: func(runtime *scriptedDriverRuntime, failure error) {
				runtime.nextEventErr = failure
			},
		},
		{
			name: "runtime drain",
			configure: func(runtime *scriptedDriverRuntime, failure error) {
				runtime.waitDrainedErr = failure
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			failure := errors.New("drain failed")
			runtime := &scriptedDriverRuntime{}
			test.configure(runtime, failure)

			err := closeUnstartedRuntime(runtime)

			if !errors.Is(err, failure) {
				t.Fatalf("closeUnstartedRuntime() error = %v, want %v", err, failure)
			}
			if runtime.releaseCalls != 1 {
				t.Fatalf("release() calls = %d, want 1", runtime.releaseCalls)
			}
		})
	}
}

func TestConsumedRuntimeReleaseErrorDoesNotRetry(t *testing.T) {
	failure := errors.New("consumed release failed")
	runtime := &scriptedDriverRuntime{
		releaseResults: []scriptedReleaseResult{{consumed: true, err: failure}},
	}
	engine := &Engine{runtime: runtime}
	state := &driverState{engine: engine}

	state.finishStopped()

	if !state.released {
		t.Fatal("driver did not exit after a consumed release error")
	}
	if !errors.Is(engine.Err(), failure) {
		t.Fatalf("Engine error = %v, want %v", engine.Err(), failure)
	}
	time.Sleep(30 * time.Millisecond)
	runtime.mu.Lock()
	releaseCalls := runtime.releaseCalls
	runtime.mu.Unlock()
	if releaseCalls != 1 {
		t.Fatalf("release() calls = %d, want 1", releaseCalls)
	}
}

func TestRuntimeReaperRetainsUnconsumedRuntimeWithBackoff(t *testing.T) {
	failure := errors.New("release retained ownership")
	runtime := &scriptedDriverRuntime{
		releaseResults: []scriptedReleaseResult{
			{consumed: false, err: failure},
			{consumed: false, err: failure},
			{consumed: true},
		},
		releaseCalled: make(chan struct{}, 3),
	}
	started := time.Now()
	startRuntimeReaper(runtime, nil)
	for range 3 {
		select {
		case <-runtime.releaseCalled:
		case <-time.After(time.Second):
			t.Fatal("runtime reaper did not finish retained release")
		}
	}
	if elapsed := time.Since(started); elapsed < 40*time.Millisecond {
		t.Fatalf("runtime reaper retried too quickly in %s", elapsed)
	}
	runtime.mu.Lock()
	releaseCalls := runtime.releaseCalls
	runtime.mu.Unlock()
	if releaseCalls != 3 {
		t.Fatalf("release() calls = %d, want 3", releaseCalls)
	}
}

func TestRuntimeReaperRetriesHandlesBeforeRuntimeRelease(t *testing.T) {
	handle := nativeHandle{owner: 1, slot: 5, generation: 1}
	failure := &StatusError{Status: -5, Operation: "release test handle"}
	runtime := &scriptedDriverRuntime{
		releaseHandleErrors: []error{failure, nil},
		releaseResults:      []scriptedReleaseResult{{consumed: true}},
		releaseCalled:       make(chan struct{}, 1),
	}
	state := &driverState{
		engine:          &Engine{runtime: runtime},
		pendingReleases: make(map[handleReleaseKey]struct{}),
	}
	state.releaseHandle(handle, transportObjectStream)
	if len(state.pendingReleases) != 1 {
		t.Fatalf("pending handle releases = %d, want 1", len(state.pendingReleases))
	}

	startRuntimeReaper(runtime, state.pendingReleases)
	select {
	case <-runtime.releaseCalled:
	case <-time.After(time.Second):
		t.Fatal("runtime release did not follow handle retry")
	}
	runtime.mu.Lock()
	handleCalls := len(runtime.releaseHandleCalls)
	releaseCalls := runtime.releaseCalls
	runtime.mu.Unlock()
	if handleCalls != 2 || releaseCalls != 1 {
		t.Fatalf("cleanup calls = handle %d, runtime %d, want 2, 1", handleCalls, releaseCalls)
	}
}

func TestDrainEventsYieldsAfterBoundedBatch(t *testing.T) {
	runtime := &scriptedDriverRuntime{}
	for range 65 {
		runtime.events = append(runtime.events, nativeEvent{kind: eventDiagnostic})
	}
	engine := &Engine{runtime: runtime, capacityChanged: make(chan struct{})}
	state := &driverState{engine: engine}

	if err := state.drainEvents(); err != nil {
		t.Fatalf("first drainEvents() error = %v", err)
	}
	if len(runtime.events) != 1 {
		t.Fatalf("events remaining after first drain = %d, want 1", len(runtime.events))
	}
	if err := state.drainEvents(); err != nil {
		t.Fatalf("second drainEvents() error = %v", err)
	}
	if len(runtime.events) != 0 {
		t.Fatalf("events remaining after second drain = %d, want 0", len(runtime.events))
	}
}

func TestUnknownEventInitiatesShutdown(t *testing.T) {
	runtime := &scriptedDriverRuntime{}
	engine := &Engine{runtime: runtime}
	state := &driverState{engine: engine}

	state.handleEvent(nativeEvent{kind: 999})

	if !state.closingStarted || !engine.closing.Load() {
		t.Fatal("unknown event did not initiate shutdown")
	}
	if runtime.beginCloseCalls != 1 {
		t.Fatalf("beginClose() calls = %d, want 1", runtime.beginCloseCalls)
	}
	if state.terminalErr == nil {
		t.Fatal("unknown event did not record a terminal error")
	}
}

func TestReadyEventsValidatePendingHandles(t *testing.T) {
	t.Run("connection", func(t *testing.T) {
		runtime := &scriptedDriverRuntime{}
		engine := &Engine{runtime: runtime}
		expected := nativeHandle{owner: 1, slot: 1, generation: 1}
		unexpected := nativeHandle{owner: 1, slot: 2, generation: 1}
		result := make(chan connectionResult, 1)
		state := &driverState{
			engine: engine,
			pendingDials: map[uint64]pendingDial{
				7: {handle: expected, result: result},
			},
			connections: map[nativeHandle]*Connection{
				unexpected: {handle: unexpected},
			},
		}

		state.handleEvent(nativeEvent{
			kind:        eventConnectionReady,
			operationID: 7,
			subject:     unexpected,
		})

		if value := <-result; value.err == nil || value.connection != nil {
			t.Fatalf("connection result = %+v, want handle validation error", value)
		}
		if !state.closingStarted || runtime.beginCloseCalls != 1 {
			t.Fatal("connection handle mismatch did not initiate shutdown")
		}
	})

	t.Run("stream", func(t *testing.T) {
		runtime := &scriptedDriverRuntime{}
		engine := &Engine{runtime: runtime}
		expected := nativeHandle{owner: 1, slot: 3, generation: 1}
		unexpected := nativeHandle{owner: 1, slot: 4, generation: 1}
		result := make(chan streamResult, 1)
		state := &driverState{
			engine: engine,
			pendingOpens: map[uint64]pendingOpen{
				9: {handle: expected, result: result},
			},
			streams: map[nativeHandle]*Stream{
				unexpected: {handle: unexpected},
			},
		}

		state.handleEvent(nativeEvent{
			kind:        eventStreamReady,
			flags:       eventFlagLocal,
			operationID: 9,
			subject:     unexpected,
		})

		if value := <-result; value.err == nil || value.stream != nil {
			t.Fatalf("stream result = %+v, want handle validation error", value)
		}
		if !state.closingStarted || runtime.beginCloseCalls != 1 {
			t.Fatal("stream handle mismatch did not initiate shutdown")
		}
	})
}

func TestTerminalStreamDetachesQueuedReceiveAccounting(t *testing.T) {
	handle := nativeHandle{owner: 1, slot: 2, generation: 3}
	runtime := &scriptedDriverRuntime{
		frames: map[nativeHandle][][]byte{
			handle: {[]byte("terminal body")},
		},
	}
	engine := &Engine{
		runtime:        runtime,
		maxQueuedCount: 1,
		maxQueuedBytes: 1,
	}
	stream := newStream(engine, handle, transportinternal.ConnectionInfo{}, 1024)
	stream.receiveDone = true
	stream.readable = true
	state := &driverState{
		engine:             engine,
		streams:            map[nativeHandle]*Stream{handle: stream},
		queuedReceiveCount: 1,
		queuedReceiveBytes: 1,
	}

	state.handleEvent(nativeEvent{kind: eventStreamClosed, subject: handle})

	if _, present := state.streams[handle]; present {
		t.Fatal("terminal stream remained registered after draining accepted data")
	}
	if state.queuedReceiveCount != 1 || state.queuedReceiveBytes != 1 {
		t.Fatalf(
			"unrelated receive accounting = %d, %d, want 1, 1",
			state.queuedReceiveCount,
			state.queuedReceiveBytes,
		)
	}
	if stream.queuedAccounted {
		t.Fatal("terminal stream bodies remained globally accounted")
	}
	if len(stream.queuedBodies) != 1 || string(stream.queuedBodies[0]) != "terminal body" {
		t.Fatalf("terminal stream bodies = %q", stream.queuedBodies)
	}
	if len(runtime.released) != 1 || runtime.released[0] != handle {
		t.Fatalf("released handles = %+v, want %+v", runtime.released, handle)
	}
}

func TestNormalizeMultiplexedEndpointPreservesWebTransportDisable(t *testing.T) {
	config := normalizeEndpointConfig(EndpointConfig{Protocol: ProtocolMultiplexed})
	if config.WebTransportProfiles != 0 || config.MaxSessions != 0 {
		t.Fatalf("disabled multiplexed WebTransport normalized to %+v", config)
	}
	config = DefaultEndpointConfig()
	config.Protocol = ProtocolMultiplexed
	config = normalizeEndpointConfig(config)
	if config.WebTransportProfiles != WebTransportProfileAllSupported || config.MaxSessions != 1 {
		t.Fatalf("enabled multiplexed WebTransport normalized to %+v", config)
	}
}

func TestInvokeAdmissionHandlerContainsPanicsAndInvalidStatus(t *testing.T) {
	request := AdmissionRequest{Kind: AdmissionHTTP3, Path: "/trevrpc"}
	if status := invokeAdmissionHandler(func(AdmissionRequest) uint16 { return 200 }, request); status != 200 {
		t.Fatalf("accepted admission status = %d", status)
	}
	if status := invokeAdmissionHandler(func(AdmissionRequest) uint16 { return 201 }, request); status != 500 {
		t.Fatalf("invalid admission status = %d", status)
	}
	if status := invokeAdmissionHandler(func(AdmissionRequest) uint16 { panic("boom") }, request); status != 500 {
		t.Fatalf("panicked admission status = %d", status)
	}
}

func TestStreamHalfCancellationIsDirectional(t *testing.T) {
	t.Run("blocked read", func(t *testing.T) {
		runtime := &scriptedDriverRuntime{}
		engine := newScriptedDriverEngine(t, runtime)
		handle := nativeHandle{owner: 1, slot: 9, generation: 1}
		stream := newStream(engine, handle, transportinternal.ConnectionInfo{}, 1024)
		state := &driverState{
			engine:  engine,
			streams: map[nativeHandle]*Stream{handle: stream},
		}

		readResult := make(chan error, 1)
		go func() {
			var data [1]byte
			_, err := stream.Read(data[:])
			readResult <- err
		}()
		command := <-engine.commands
		command(state)
		if stream.readWaiter == nil {
			t.Fatal("Read() did not install a native receive waiter")
		}

		cancelDone := make(chan struct{})
		go func() {
			stream.CancelRead(transportinternal.CloseReason{ApplicationCode: 13})
			close(cancelDone)
		}()
		command = <-engine.commands
		command(state)
		<-cancelDone
		select {
		case err := <-readResult:
			if err == nil {
				t.Fatal("blocked Read() woke without a cancellation error")
			}
		case <-time.After(time.Second):
			t.Fatal("CancelRead did not wake blocked Read()")
		}
	})

	t.Run("read", func(t *testing.T) {
		runtime := &scriptedDriverRuntime{}
		engine := newScriptedDriverEngine(t, runtime)
		handle := nativeHandle{owner: 1, slot: 10, generation: 1}
		stream := newStream(engine, handle, transportinternal.ConnectionInfo{}, 1024)
		stream.queuedBodies = [][]byte{[]byte("discard")}
		stream.queuedBytes = 7
		stream.queuedAccounted = true
		state := &driverState{
			engine:             engine,
			streams:            map[nativeHandle]*Stream{handle: stream},
			queuedReceiveCount: 1,
			queuedReceiveBytes: 7,
		}
		reason := transportinternal.CloseReason{ApplicationCode: 17, Message: "stop reading"}

		done := make(chan struct{})
		go func() {
			stream.CancelRead(reason)
			close(done)
		}()
		command := <-engine.commands
		command(state)
		<-done

		if len(runtime.abortReadCalls) != 1 || runtime.abortReadCalls[0] != (abortCall{handle: handle, code: 17}) {
			t.Fatalf("read abort calls = %+v", runtime.abortReadCalls)
		}
		if len(runtime.abortWriteCalls) != 0 || len(runtime.abortStreamCalls) != 0 {
			t.Fatalf("non-read abort calls = write %+v, whole %+v", runtime.abortWriteCalls, runtime.abortStreamCalls)
		}
		if !stream.receiveDone || !stream.readCancelled || stream.sendDone {
			t.Fatalf("stream half state = receiveDone %t, readCancelled %t, sendDone %t", stream.receiveDone, stream.readCancelled, stream.sendDone)
		}
		if len(stream.queuedBodies) != 0 || state.queuedReceiveCount != 0 || state.queuedReceiveBytes != 0 {
			t.Fatalf("queued receive state = %q, %d, %d", stream.queuedBodies, state.queuedReceiveCount, state.queuedReceiveBytes)
		}
		select {
		case <-stream.Context().Done():
			t.Fatal("CancelRead canceled the whole stream context")
		default:
		}
		var data [1]byte
		if _, err := stream.Read(data[:]); err == nil {
			t.Fatal("Read() after CancelRead succeeded")
		}
	})

	t.Run("write", func(t *testing.T) {
		runtime := &scriptedDriverRuntime{}
		engine := newScriptedDriverEngine(t, runtime)
		handle := nativeHandle{owner: 1, slot: 11, generation: 1}
		stream := newStream(engine, handle, transportinternal.ConnectionInfo{}, 1024)
		state := &driverState{
			engine:  engine,
			streams: map[nativeHandle]*Stream{handle: stream},
		}
		reason := transportinternal.CloseReason{ApplicationCode: 19, Message: "stop writing"}

		done := make(chan struct{})
		go func() {
			stream.CancelWrite(reason)
			close(done)
		}()
		command := <-engine.commands
		command(state)
		<-done

		if len(runtime.abortWriteCalls) != 1 || runtime.abortWriteCalls[0] != (abortCall{handle: handle, code: 19}) {
			t.Fatalf("write abort calls = %+v", runtime.abortWriteCalls)
		}
		if len(runtime.abortReadCalls) != 0 || len(runtime.abortStreamCalls) != 0 {
			t.Fatalf("non-write abort calls = read %+v, whole %+v", runtime.abortReadCalls, runtime.abortStreamCalls)
		}
		if !stream.sendDone || stream.receiveDone {
			t.Fatalf("stream half state = sendDone %t, receiveDone %t", stream.sendDone, stream.receiveDone)
		}
		select {
		case <-stream.Context().Done():
			t.Fatal("CancelWrite canceled the whole stream context")
		default:
		}
		if _, err := stream.Write([]byte{0, 0, 0, 0}); err == nil {
			t.Fatal("Write() after CancelWrite succeeded")
		}

		closed := make(chan error, 1)
		go func() { closed <- stream.Close() }()
		command = <-engine.commands
		command(state)
		if err := <-closed; err == nil {
			t.Fatal("Close() after CancelWrite did not preserve the write cancellation error")
		}
		if len(runtime.abortStreamCalls) != 0 {
			t.Fatalf("Close() after CancelWrite issued whole abort %+v", runtime.abortStreamCalls)
		}
	})
}

func TestStreamHalfCancellationLoopback(t *testing.T) {
	tests := []struct {
		name         string
		useTransport bool
		protocol     uint32
	}{
		{name: "engine", protocol: ProtocolNative},
		{name: "transport-native", useTransport: true, protocol: ProtocolNative},
		{name: "transport-http3", useTransport: true, protocol: ProtocolHTTP3},
		{name: "transport-webtransport", useTransport: true, protocol: ProtocolWebTransport},
	}
	for _, test := range tests {
		t.Run(test.name+"/cancel-read", func(t *testing.T) {
			testStreamHalfCancellationLoopback(t, test.useTransport, test.protocol, true)
		})
		t.Run(test.name+"/cancel-write", func(t *testing.T) {
			testStreamHalfCancellationLoopback(t, test.useTransport, test.protocol, false)
		})
	}
}

func testStreamHalfCancellationLoopback(t *testing.T, useTransport bool, protocol uint32, cancelRead bool) {
	t.Helper()
	certificateFile, keyFile := writeNativeTestIdentity(t)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	open := NewEngine
	if useTransport {
		open = NewTransport
	}
	serverEngine, err := open(DefaultEngineConfig())
	if err != nil {
		t.Fatalf("open server runtime error = %v", err)
	}
	t.Cleanup(func() { _ = serverEngine.Close() })
	clientEngine, err := open(DefaultEngineConfig())
	if err != nil {
		t.Fatalf("open client runtime error = %v", err)
	}
	t.Cleanup(func() { _ = clientEngine.Close() })

	serverConfig := DefaultEndpointConfig()
	serverConfig.Protocol = protocol
	serverConfig.Host = "127.0.0.1"
	serverConfig.ALPN = nil
	serverConfig.Path = "/trevrpc"
	serverConfig.Origin = "https://origin.test"
	serverConfig.CertificateFile = certificateFile
	serverConfig.PrivateKeyFile = keyFile
	listener, err := serverEngine.Listen(ctx, serverConfig)
	if err != nil {
		t.Fatalf("Listen() error = %v", err)
	}
	t.Cleanup(func() { _ = listener.Close() })

	clientConfig := DefaultEndpointConfig()
	clientConfig.Protocol = protocol
	clientConfig.Host = "127.0.0.1"
	clientConfig.Port = listener.Address().Port
	clientConfig.ALPN = nil
	clientConfig.Path = serverConfig.Path
	clientConfig.Origin = serverConfig.Origin
	clientConfig.SkipCertificateValidation = true
	clientConnection, err := clientEngine.Dial(ctx, clientConfig)
	if err != nil {
		t.Fatalf("Dial() error = %v", err)
	}
	t.Cleanup(func() {
		_ = clientConnection.Close(transportinternal.CloseReason{Clean: true})
	})
	accepted, err := listener.Accept(ctx)
	if err != nil {
		t.Fatalf("Accept() error = %v", err)
	}
	serverConnection := accepted.(*Connection)
	t.Cleanup(func() {
		_ = serverConnection.Close(transportinternal.CloseReason{Clean: true})
	})

	opened, err := clientConnection.OpenStream(ctx)
	if err != nil {
		t.Fatalf("OpenStream() error = %v", err)
	}
	clientStream := opened.(*Stream)
	peer, err := serverConnection.AcceptStream(ctx)
	if err != nil {
		t.Fatalf("AcceptStream() error = %v", err)
	}
	serverStream := peer.(*Stream)

	if cancelRead {
		clientStream.CancelRead(transportinternal.CloseReason{
			ApplicationCode: 31,
			Message:         "cancel receive half",
		})
		request := framedBytes([]byte("write survives read cancellation"))
		if written, writeErr := clientStream.Write(request); writeErr != nil || written != len(request) {
			t.Fatalf("Write() after CancelRead() = %d, %v, want %d, nil", written, writeErr, len(request))
		}
		if closeErr := clientStream.Close(); closeErr != nil {
			t.Fatalf("Close() after CancelRead() error = %v", closeErr)
		}
		if deadlineErr := serverStream.SetReadDeadline(time.Now().Add(5 * time.Second)); deadlineErr != nil {
			t.Fatalf("server SetReadDeadline() error = %v", deadlineErr)
		}
		actual, readErr := io.ReadAll(serverStream)
		if readErr != nil {
			t.Fatalf("peer ReadAll() after CancelRead() error = %#v", readErr)
		}
		if !bytes.Equal(actual, request) {
			t.Fatalf("peer request after CancelRead() = %x, want %x", actual, request)
		}
		return
	}

	clientStream.CancelWrite(transportinternal.CloseReason{
		ApplicationCode: 37,
		Message:         "cancel send half",
	})
	response := framedBytes([]byte("read survives write cancellation"))
	if written, writeErr := serverStream.Write(response); writeErr != nil || written != len(response) {
		t.Fatalf("peer Write() after CancelWrite() = %d, %v, want %d, nil", written, writeErr, len(response))
	}
	if closeErr := serverStream.Close(); closeErr != nil {
		t.Fatalf("peer Close() after CancelWrite() error = %v", closeErr)
	}
	if deadlineErr := clientStream.SetReadDeadline(time.Now().Add(5 * time.Second)); deadlineErr != nil {
		t.Fatalf("client SetReadDeadline() error = %v", deadlineErr)
	}
	actual, readErr := io.ReadAll(clientStream)
	if readErr != nil {
		t.Fatalf("ReadAll() after CancelWrite() error = %#v", readErr)
	}
	if !bytes.Equal(actual, response) {
		t.Fatalf("response after CancelWrite() = %x, want %x", actual, response)
	}
	if closeErr := clientStream.Close(); closeErr == nil {
		t.Fatal("Close() after CancelWrite() returned nil")
	}
}

func TestNativeEngineLoopback(t *testing.T) {
	certificateFile, keyFile := writeNativeTestIdentity(t)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	engineConfig := EngineConfig{
		MaxReceiveOwnedCount: 1,
		MaxReceiveOwnedBytes: 16,
	}
	serverEngine, err := NewEngine(engineConfig)
	if err != nil {
		t.Fatalf("NewEngine(server) error = %v", err)
	}
	t.Cleanup(func() { _ = serverEngine.Close() })
	clientEngine, err := NewEngine(engineConfig)
	if err != nil {
		t.Fatalf("NewEngine(client) error = %v", err)
	}
	t.Cleanup(func() { _ = clientEngine.Close() })

	serverConfig := DefaultEndpointConfig()
	serverConfig.Host = "127.0.0.1"
	serverConfig.MaxFrameSize = 16
	serverConfig.CertificateFile = certificateFile
	serverConfig.PrivateKeyFile = keyFile
	listener, err := serverEngine.Listen(ctx, serverConfig)
	if err != nil {
		t.Fatalf("Listen() error = %v", err)
	}

	clientConfig := DefaultEndpointConfig()
	clientConfig.Host = "127.0.0.1"
	clientConfig.Port = listener.Address().Port
	clientConfig.MaxFrameSize = 16
	clientConfig.SkipCertificateValidation = true
	clientConnection, err := clientEngine.Dial(ctx, clientConfig)
	if err != nil {
		t.Fatalf("Dial() error = %v", err)
	}
	accepted, err := listener.Accept(ctx)
	if err != nil {
		t.Fatalf("Accept() error = %v", err)
	}
	serverConnection := accepted.(*Connection)

	opened, err := clientConnection.OpenStream(ctx)
	if err != nil {
		t.Fatalf("OpenStream() error = %v", err)
	}
	clientStream := opened.(*Stream)
	peer, err := serverConnection.AcceptStream(ctx)
	if err != nil {
		t.Fatalf("AcceptStream() error = %v", err)
	}
	serverStream := peer.(*Stream)

	request := framedBytes([]byte("request one"), nil, []byte("request two"))
	if written, err := clientStream.Write(request); err != nil || written != len(request) {
		t.Fatalf("client Write() = %d, %v, want %d, nil", written, err, len(request))
	}
	if err := clientStream.Close(); err != nil {
		t.Fatalf("client stream Close() error = %v", err)
	}
	actualRequest, err := io.ReadAll(serverStream)
	if err != nil {
		t.Fatalf("server ReadAll() error = %v", err)
	}
	if !bytes.Equal(actualRequest, request) {
		t.Fatalf("server request = %x, want %x", actualRequest, request)
	}

	response := framedBytes([]byte("response"))
	if written, err := serverStream.Write(response); err != nil || written != len(response) {
		t.Fatalf("server Write() = %d, %v, want %d, nil", written, err, len(response))
	}
	if err := serverStream.Close(); err != nil {
		t.Fatalf("server stream Close() error = %v", err)
	}
	actualResponse, err := io.ReadAll(clientStream)
	if err != nil {
		t.Fatalf("client ReadAll() error = %v", err)
	}
	if !bytes.Equal(actualResponse, response) {
		t.Fatalf("client response = %x, want %x", actualResponse, response)
	}

	closeReason := transportinternal.CloseReason{Clean: true, Message: "test complete"}
	if err := clientConnection.Close(closeReason); err != nil {
		t.Fatalf("client connection Close() error = %v", err)
	}
	if err := serverConnection.Close(closeReason); err != nil {
		t.Fatalf("server connection Close() error = %v", err)
	}
	if err := listener.Close(); err != nil {
		t.Fatalf("listener Close() error = %v", err)
	}
	if err := clientEngine.Close(); err != nil {
		t.Fatalf("client Engine Close() error = %v", err)
	}
	if err := serverEngine.Close(); err != nil {
		t.Fatalf("server Engine Close() error = %v", err)
	}
}

func TestNativeStreamReadDeadlineCanBeReset(t *testing.T) {
	certificateFile, keyFile := writeNativeTestIdentity(t)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	serverEngine, err := NewEngine(DefaultEngineConfig())
	if err != nil {
		t.Fatalf("NewEngine(server) error = %v", err)
	}
	t.Cleanup(func() { _ = serverEngine.Close() })
	clientEngine, err := NewEngine(DefaultEngineConfig())
	if err != nil {
		t.Fatalf("NewEngine(client) error = %v", err)
	}
	t.Cleanup(func() { _ = clientEngine.Close() })

	serverConfig := DefaultEndpointConfig()
	serverConfig.Host = "127.0.0.1"
	serverConfig.CertificateFile = certificateFile
	serverConfig.PrivateKeyFile = keyFile
	listener, err := serverEngine.Listen(ctx, serverConfig)
	if err != nil {
		t.Fatalf("Listen() error = %v", err)
	}
	clientConfig := DefaultEndpointConfig()
	clientConfig.Host = "127.0.0.1"
	clientConfig.Port = listener.Address().Port
	clientConfig.SkipCertificateValidation = true
	clientConnection, err := clientEngine.Dial(ctx, clientConfig)
	if err != nil {
		t.Fatalf("Dial() error = %v", err)
	}
	accepted, err := listener.Accept(ctx)
	if err != nil {
		t.Fatalf("Accept() error = %v", err)
	}
	serverConnection := accepted.(*Connection)
	opened, err := clientConnection.OpenStream(ctx)
	if err != nil {
		t.Fatalf("OpenStream() error = %v", err)
	}
	clientStream := opened.(*Stream)
	peer, err := serverConnection.AcceptStream(ctx)
	if err != nil {
		t.Fatalf("AcceptStream() error = %v", err)
	}
	serverStream := peer.(*Stream)

	if err := serverStream.SetReadDeadline(time.Now().Add(20 * time.Millisecond)); err != nil {
		t.Fatalf("SetReadDeadline() error = %v", err)
	}
	var one [1]byte
	if _, err := serverStream.Read(one[:]); !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("Read() error = %v, want deadline exceeded", err)
	}
	if err := serverStream.SetReadDeadline(time.Time{}); err != nil {
		t.Fatalf("clear SetReadDeadline() error = %v", err)
	}
	request := framedBytes([]byte("after deadline"))
	if written, err := clientStream.Write(request); err != nil || written != len(request) {
		t.Fatalf("client Write() = %d, %v, want %d, nil", written, err, len(request))
	}
	if err := clientStream.Close(); err != nil {
		t.Fatalf("client stream Close() error = %v", err)
	}
	actual, err := io.ReadAll(serverStream)
	if err != nil {
		t.Fatalf("server ReadAll() error = %v", err)
	}
	if !bytes.Equal(actual, request) {
		t.Fatalf("server request = %x, want %x", actual, request)
	}
}

func TestTransportHTTP3Loopback(t *testing.T) {
	testTransportProtocolLoopback(t, ProtocolHTTP3, false)
}

func TestTransportHTTP3DeferredAdmissionLoopback(t *testing.T) {
	testTransportProtocolLoopback(t, ProtocolHTTP3, true)
}

func TestTransportWebTransportLoopback(t *testing.T) {
	testTransportProtocolLoopback(t, ProtocolWebTransport, false)
}

func TestTransportWebTransportDeferredAdmissionLoopback(t *testing.T) {
	testTransportProtocolLoopback(t, ProtocolWebTransport, true)
}

func testTransportProtocolLoopback(t *testing.T, protocol uint32, deferredAdmission bool) {
	t.Helper()
	certificateFile, keyFile := writeNativeTestIdentity(t)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	serverEngine, err := NewTransport(EngineConfig{})
	if err != nil {
		t.Fatalf("NewTransport(server) error = %v", err)
	}
	t.Cleanup(func() { _ = serverEngine.Close() })
	clientEngine, err := NewTransport(EngineConfig{})
	if err != nil {
		t.Fatalf("NewTransport(client) error = %v", err)
	}
	t.Cleanup(func() { _ = clientEngine.Close() })

	serverConfig := DefaultEndpointConfig()
	serverConfig.Protocol = protocol
	serverConfig.Host = "127.0.0.1"
	serverConfig.ALPN = nil
	serverConfig.Path = "/trevrpc"
	serverConfig.Origin = "https://origin.test"
	serverConfig.CertificateFile = certificateFile
	serverConfig.PrivateKeyFile = keyFile
	admissions := make(chan AdmissionRequest, 1)
	if deferredAdmission {
		serverConfig.DeferAdmission = true
		serverConfig.Admission = func(request AdmissionRequest) uint16 {
			admissions <- request
			return 200
		}
	}
	listener, err := serverEngine.Listen(ctx, serverConfig)
	if err != nil {
		t.Fatalf("Listen() error = %v", err)
	}
	clientConfig := DefaultEndpointConfig()
	clientConfig.Protocol = protocol
	clientConfig.Host = "127.0.0.1"
	clientConfig.Port = listener.Address().Port
	clientConfig.ALPN = nil
	clientConfig.Path = "/trevrpc"
	clientConfig.Origin = "https://origin.test"
	clientConfig.SkipCertificateValidation = true
	clientConnection, err := clientEngine.Dial(ctx, clientConfig)
	if err != nil {
		t.Fatalf("Dial() error = %v", err)
	}
	accepted, err := listener.Accept(ctx)
	if err != nil {
		t.Fatalf("Accept() error = %v", err)
	}
	serverConnection := accepted.(*Connection)
	waitAdmission := func() {
		select {
		case admission := <-admissions:
			if admission.Path != serverConfig.Path || !admission.Secure {
				t.Fatalf("admission request = %+v", admission)
			}
			if protocol == ProtocolHTTP3 && admission.Kind != AdmissionHTTP3 {
				t.Fatalf("HTTP/3 admission kind = %d", admission.Kind)
			}
			if protocol == ProtocolWebTransport && (admission.Kind != AdmissionWebTransport || admission.Origin != serverConfig.Origin) {
				t.Fatalf("WebTransport admission = %+v", admission)
			}
		case <-ctx.Done():
			t.Fatalf("admission callback did not run: %v", ctx.Err())
		}
	}
	if deferredAdmission && protocol == ProtocolWebTransport {
		waitAdmission()
	}

	opened, err := clientConnection.OpenStream(ctx)
	if err != nil {
		t.Fatalf("OpenStream() error = %v", err)
	}
	clientStream := opened.(*Stream)
	if deferredAdmission && protocol == ProtocolHTTP3 {
		waitAdmission()
	}
	peer, err := serverConnection.AcceptStream(ctx)
	if err != nil {
		t.Fatalf("AcceptStream() error = %v", err)
	}
	serverStream := peer.(*Stream)

	request := framedBytes([]byte("request"))
	if written, err := clientStream.Write(request); err != nil || written != len(request) {
		t.Fatalf("client Write() = %d, %v, want %d, nil", written, err, len(request))
	}
	if err := clientStream.Close(); err != nil {
		t.Fatalf("client stream Close() error = %v", err)
	}
	actualRequest, err := io.ReadAll(serverStream)
	if err != nil {
		t.Fatalf("server ReadAll() error = %v", err)
	}
	if !bytes.Equal(actualRequest, request) {
		t.Fatalf("server request = %x, want %x", actualRequest, request)
	}

	response := framedBytes([]byte("response"))
	if written, err := serverStream.Write(response); err != nil || written != len(response) {
		t.Fatalf("server Write() = %d, %v, want %d, nil", written, err, len(response))
	}
	if err := serverStream.Close(); err != nil {
		t.Fatalf("server stream Close() error = %v", err)
	}
	actualResponse, err := io.ReadAll(clientStream)
	if err != nil {
		t.Fatalf("client ReadAll() error = %v", err)
	}
	if !bytes.Equal(actualResponse, response) {
		t.Fatalf("client response = %x, want %x", actualResponse, response)
	}

	closeReason := transportinternal.CloseReason{Clean: true, Message: "test complete"}
	if err := clientConnection.Close(closeReason); err != nil {
		t.Fatalf("client connection Close() error = %v", err)
	}
	if err := serverConnection.Close(closeReason); err != nil {
		t.Fatalf("server connection Close() error = %v", err)
	}
	if err := listener.Close(); err != nil {
		t.Fatalf("listener Close() error = %v", err)
	}
	if err := clientEngine.Close(); err != nil {
		t.Fatalf("client Transport Close() error = %v", err)
	}
	if err := serverEngine.Close(); err != nil {
		t.Fatalf("server Transport Close() error = %v", err)
	}
}

func TestTransportDeferredAdmissionsCanReject(t *testing.T) {
	for _, protocol := range []uint32{ProtocolHTTP3, ProtocolWebTransport} {
		t.Run(fmt.Sprintf("protocol-%d", protocol), func(t *testing.T) {
			certificateFile, keyFile := writeNativeTestIdentity(t)
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()

			serverEngine, err := NewTransport(EngineConfig{})
			if err != nil {
				t.Fatalf("NewTransport(server) error = %v", err)
			}
			defer func() { _ = serverEngine.Close() }()
			clientEngine, err := NewTransport(EngineConfig{})
			if err != nil {
				t.Fatalf("NewTransport(client) error = %v", err)
			}
			defer func() { _ = clientEngine.Close() }()

			admissions := make(chan AdmissionRequest, 1)
			serverConfig := DefaultEndpointConfig()
			serverConfig.Protocol = protocol
			serverConfig.Host = "127.0.0.1"
			serverConfig.ALPN = nil
			serverConfig.Path = "/trevrpc"
			serverConfig.Origin = "https://origin.test"
			serverConfig.CertificateFile = certificateFile
			serverConfig.PrivateKeyFile = keyFile
			serverConfig.DeferAdmission = true
			serverConfig.Admission = func(request AdmissionRequest) uint16 {
				admissions <- request
				return 403
			}
			listener, err := serverEngine.Listen(ctx, serverConfig)
			if err != nil {
				t.Fatalf("Listen() error = %v", err)
			}
			defer func() { _ = listener.Close() }()

			clientConfig := DefaultEndpointConfig()
			clientConfig.Protocol = protocol
			clientConfig.Host = "127.0.0.1"
			clientConfig.Port = listener.Address().Port
			clientConfig.ALPN = nil
			clientConfig.Path = serverConfig.Path
			clientConfig.Origin = serverConfig.Origin
			clientConfig.SkipCertificateValidation = true
			clientConnection, dialErr := clientEngine.Dial(ctx, clientConfig)
			var rejectedStream *Stream
			if protocol == ProtocolWebTransport {
				if dialErr == nil {
					t.Fatal("rejected WebTransport Dial() returned nil error")
				}
			} else {
				if dialErr != nil {
					t.Fatalf("HTTP/3 Dial() error = %v", dialErr)
				}
				defer func() { _ = clientConnection.Close(transportinternal.CloseReason{Clean: true}) }()
				serverConnectionValue, err := listener.Accept(ctx)
				if err != nil {
					t.Fatalf("Accept() error = %v", err)
				}
				serverConnection := serverConnectionValue.(*Connection)
				defer func() { _ = serverConnection.Close(transportinternal.CloseReason{Clean: true}) }()
				opened, err := clientConnection.OpenStream(ctx)
				if err != nil {
					t.Fatalf("HTTP/3 OpenStream() error = %v", err)
				}
				rejectedStream = opened.(*Stream)
			}
			select {
			case admission := <-admissions:
				if admission.Path != serverConfig.Path {
					t.Fatalf("admission path = %q", admission.Path)
				}
			case <-ctx.Done():
				t.Fatalf("admission callback did not run: %v", ctx.Err())
			}
			if rejectedStream != nil {
				select {
				case <-rejectedStream.Context().Done():
					if context.Cause(rejectedStream.Context()) == nil {
						t.Fatal("rejected HTTP/3 stream closed without an error")
					}
				case <-ctx.Done():
					t.Fatalf("rejected HTTP/3 stream did not terminate: %v", ctx.Err())
				}
			}
		})
	}
}

func TestTransportShutdownDoesNotWaitForAdmissionHandler(t *testing.T) {
	certificateFile, keyFile := writeNativeTestIdentity(t)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	serverEngine, err := NewTransport(EngineConfig{})
	if err != nil {
		t.Fatalf("NewTransport(server) error = %v", err)
	}
	t.Cleanup(func() { _ = serverEngine.Close() })
	clientEngine, err := NewTransport(EngineConfig{})
	if err != nil {
		t.Fatalf("NewTransport(client) error = %v", err)
	}
	t.Cleanup(func() { _ = clientEngine.Close() })

	entered := make(chan struct{})
	release := make(chan struct{})
	handlerDone := make(chan struct{})
	releaseAdmission := func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}
	t.Cleanup(releaseAdmission)
	serverConfig := DefaultEndpointConfig()
	serverConfig.Protocol = ProtocolHTTP3
	serverConfig.Host = "127.0.0.1"
	serverConfig.ALPN = nil
	serverConfig.Path = "/trevrpc"
	serverConfig.CertificateFile = certificateFile
	serverConfig.PrivateKeyFile = keyFile
	serverConfig.DeferAdmission = true
	serverConfig.Admission = func(AdmissionRequest) uint16 {
		defer close(handlerDone)
		close(entered)
		<-release
		return 200
	}
	listener, err := serverEngine.Listen(ctx, serverConfig)
	if err != nil {
		t.Fatalf("Listen() error = %v", err)
	}

	clientConfig := DefaultEndpointConfig()
	clientConfig.Protocol = ProtocolHTTP3
	clientConfig.Host = "127.0.0.1"
	clientConfig.Port = listener.Address().Port
	clientConfig.ALPN = nil
	clientConfig.Path = serverConfig.Path
	clientConfig.SkipCertificateValidation = true
	clientConnection, err := clientEngine.Dial(ctx, clientConfig)
	if err != nil {
		t.Fatalf("Dial() error = %v", err)
	}
	t.Cleanup(func() {
		_ = clientConnection.Close(transportinternal.CloseReason{Clean: true})
	})
	if _, err := listener.Accept(ctx); err != nil {
		t.Fatalf("Accept() error = %v", err)
	}
	opened, err := clientConnection.OpenStream(ctx)
	if err != nil {
		t.Fatalf("OpenStream() error = %v", err)
	}
	stream := opened.(*Stream)
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatalf("admission callback did not start: %v", ctx.Err())
	}

	closed := make(chan error, 1)
	go func() { closed <- serverEngine.Close() }()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatalf("server Transport Close() error = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("server Transport Close() waited for admission handler")
	}
	releaseAdmission()
	select {
	case <-handlerDone:
	case <-ctx.Done():
		t.Fatalf("admission handler did not return: %v", ctx.Err())
	}
	select {
	case <-stream.Context().Done():
		if context.Cause(stream.Context()) == nil {
			t.Fatal("stream closed without an error during server shutdown")
		}
	case <-ctx.Done():
		t.Fatalf("client stream did not terminate: %v", ctx.Err())
	}
}

func TestProviderStoppedCompletesConcurrentCommands(t *testing.T) {
	certificateFile, keyFile := writeNativeTestIdentity(t)
	engine, err := NewEngine(EngineConfig{})
	if err != nil {
		t.Fatalf("NewEngine() error = %v", err)
	}
	t.Cleanup(func() { _ = engine.Close() })
	endpoint := DefaultEndpointConfig()
	endpoint.Host = "127.0.0.1"
	endpoint.CertificateFile = certificateFile
	endpoint.PrivateKeyFile = keyFile
	listener, err := engine.Listen(t.Context(), endpoint)
	if err != nil {
		t.Fatalf("Listen() error = %v", err)
	}

	stopResult := make(chan error, 1)
	if err := engine.enqueue(t.Context(), func(*driverState) {
		stopResult <- engine.runtime.beginClose()
	}); err != nil {
		t.Fatalf("enqueue provider close error = %v", err)
	}
	if err := <-stopResult; err != nil {
		t.Fatalf("provider close error = %v", err)
	}

	const commandCount = 64
	var commands sync.WaitGroup
	commands.Add(commandCount + 1)
	go func() {
		defer commands.Done()
		_ = listener.Close()
	}()
	for range commandCount {
		go func() {
			defer commands.Done()
			_, _ = engine.Listen(context.Background(), endpoint)
		}()
	}
	completed := make(chan struct{})
	go func() {
		commands.Wait()
		close(completed)
	}()
	select {
	case <-completed:
	case <-time.After(5 * time.Second):
		t.Fatal("commands did not complete after provider STOPPED")
	}
	select {
	case <-engine.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("Engine did not finish after provider STOPPED")
	}
}

func newScriptedDriverEngine(t *testing.T, runtime driverRuntime) *Engine {
	t.Helper()
	commandRead, commandWrite, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe() error = %v", err)
	}
	t.Cleanup(func() {
		_ = commandRead.Close()
		_ = commandWrite.Close()
	})
	return &Engine{
		runtime:         runtime,
		commandRead:     commandRead,
		commandWrite:    commandWrite,
		commands:        make(chan driverCommand, 8),
		done:            make(chan struct{}),
		capacityChanged: make(chan struct{}),
	}
}

type abortCall struct {
	handle nativeHandle
	code   uint64
}

type scriptedReleaseResult struct {
	consumed bool
	err      error
}

type scriptedDriverRuntime struct {
	mu                  sync.Mutex
	events              []nativeEvent
	frames              map[nativeHandle][][]byte
	released            []nativeHandle
	releaseHandleCalls  []handleReleaseKey
	releaseHandleErrors []error
	releaseResults      []scriptedReleaseResult
	releaseCalled       chan struct{}
	abortReadCalls      []abortCall
	abortWriteCalls     []abortCall
	abortStreamCalls    []abortCall
	nextEventErr        error
	waitDrainedErr      error
	beginCloseCalls     int
	releaseCalls        int
}

func (runtime *scriptedDriverRuntime) WakeSources() ([]WakeSource, error) {
	return nil, nil
}

func (runtime *scriptedDriverRuntime) pollTimeoutMilliseconds() int {
	return -1
}

func (runtime *scriptedDriverRuntime) transportABIVersion() uint32 {
	return 1
}

func (runtime *scriptedDriverRuntime) nextEvent() (nativeEvent, bool, error) {
	if runtime.nextEventErr != nil {
		return nativeEvent{}, false, runtime.nextEventErr
	}
	if len(runtime.events) == 0 {
		return nativeEvent{}, false, nil
	}
	event := runtime.events[0]
	runtime.events = runtime.events[1:]
	return event, true, nil
}

func (runtime *scriptedDriverRuntime) listen(EndpointConfig) (nativeHandle, error) {
	return nativeHandle{}, errors.New("unexpected listen")
}

func (runtime *scriptedDriverRuntime) listenerPort(nativeHandle) (uint16, error) {
	return 0, errors.New("unexpected listener port")
}

func (runtime *scriptedDriverRuntime) dial(EndpointConfig, uint64) (nativeHandle, error) {
	return nativeHandle{}, errors.New("unexpected dial")
}

func (runtime *scriptedDriverRuntime) cancelDial(nativeHandle) error {
	return errors.New("unexpected cancel dial")
}

func (runtime *scriptedDriverRuntime) openStream(nativeHandle, uint64) (nativeHandle, error) {
	return nativeHandle{}, errors.New("unexpected open stream")
}

func (runtime *scriptedDriverRuntime) sendFrame(nativeHandle, uint64, []byte) error {
	return errors.New("unexpected send frame")
}

func (runtime *scriptedDriverRuntime) receiveFrame(handle nativeHandle) ([]byte, error) {
	frames := runtime.frames[handle]
	if len(frames) == 0 {
		return nil, errNativeWouldBlock
	}
	body := frames[0]
	runtime.frames[handle] = frames[1:]
	return body, nil
}

func (runtime *scriptedDriverRuntime) finishStreamSend(nativeHandle) error {
	return errors.New("unexpected finish stream send")
}

func (runtime *scriptedDriverRuntime) abortStreamRead(handle nativeHandle, code uint64) error {
	runtime.abortReadCalls = append(runtime.abortReadCalls, abortCall{handle: handle, code: code})
	return nil
}

func (runtime *scriptedDriverRuntime) abortStreamWrite(handle nativeHandle, code uint64) error {
	runtime.abortWriteCalls = append(runtime.abortWriteCalls, abortCall{handle: handle, code: code})
	return nil
}

func (runtime *scriptedDriverRuntime) abortStream(handle nativeHandle, code uint64) error {
	runtime.abortStreamCalls = append(runtime.abortStreamCalls, abortCall{handle: handle, code: code})
	return nil
}

func (runtime *scriptedDriverRuntime) closeConnection(nativeHandle, uint64) error {
	return errors.New("unexpected close connection")
}

func (runtime *scriptedDriverRuntime) closeListener(nativeHandle) error {
	return errors.New("unexpected close listener")
}

func (runtime *scriptedDriverRuntime) releaseHandle(handle nativeHandle, kind uint32) error {
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	runtime.releaseHandleCalls = append(
		runtime.releaseHandleCalls,
		handleReleaseKey{handle: handle, kind: kind},
	)
	var err error
	if len(runtime.releaseHandleErrors) != 0 {
		err = runtime.releaseHandleErrors[0]
		runtime.releaseHandleErrors = runtime.releaseHandleErrors[1:]
	}
	if err == nil {
		runtime.released = append(runtime.released, handle)
	}
	return err
}

func (runtime *scriptedDriverRuntime) beginClose() error {
	runtime.beginCloseCalls++
	return nil
}

func (runtime *scriptedDriverRuntime) waitDrained() error {
	return runtime.waitDrainedErr
}

func (runtime *scriptedDriverRuntime) release() (bool, error) {
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	runtime.releaseCalls++
	if len(runtime.releaseResults) == 0 {
		return true, nil
	}
	result := runtime.releaseResults[0]
	runtime.releaseResults = runtime.releaseResults[1:]
	if runtime.releaseCalled != nil {
		select {
		case runtime.releaseCalled <- struct{}{}:
		default:
		}
	}
	return result.consumed, result.err
}

func writeNativeTestIdentity(t *testing.T) (string, string) {
	t.Helper()
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("GenerateKey() error = %v", err)
	}
	now := time.Now()
	certificateDER, err := x509.CreateCertificate(rand.Reader, &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "localhost"},
		NotBefore:    now.Add(-time.Hour),
		NotAfter:     now.Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:     []string{"localhost"},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
	}, &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "localhost"},
		NotBefore:    now.Add(-time.Hour),
		NotAfter:     now.Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:     []string{"localhost"},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
	}, &privateKey.PublicKey, privateKey)
	if err != nil {
		t.Fatalf("CreateCertificate() error = %v", err)
	}

	directory := t.TempDir()
	certificateFile := filepath.Join(directory, "certificate.pem")
	keyFile := filepath.Join(directory, "private-key.pem")
	certificatePEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certificateDER})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(privateKey)})
	if err := os.WriteFile(certificateFile, certificatePEM, 0o600); err != nil {
		t.Fatalf("WriteFile(certificate) error = %v", err)
	}
	if err := os.WriteFile(keyFile, keyPEM, 0o600); err != nil {
		t.Fatalf("WriteFile(key) error = %v", err)
	}
	return certificateFile, keyFile
}
