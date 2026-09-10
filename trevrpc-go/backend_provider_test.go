package trevrpc

import (
	"context"
	"errors"
	"testing"
	"time"

	transportapi "trev.zip/llc/trevrpc/trevrpc-go/transport"
)

func TestDialValidatesCanceledContextBeforeBackendSetup(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, err := Dial(ctx, "invalid://target", DialOptions{}); StatusFromError(err).Code != CodeCancelled {
		t.Fatalf("Dial() error = %v, want Cancelled", err)
	}
	if _, err := DialWithBackend(ctx, "target", DialOptions{}, nil); StatusFromError(err).Code != CodeCancelled {
		t.Fatalf("DialWithBackend() error = %v, want Cancelled", err)
	}
}

func TestOptionalDialBackendValidation(t *testing.T) {
	if _, err := newBackendChannelConnector("example.test:443", DialOptions{}, nil); StatusFromError(err).Code != CodeInvalidArgument {
		t.Fatalf("nil backend error = %v, want InvalidArgument", err)
	}
	if _, err := newBackendChannelConnector("example.test:443", DialOptions{}, testDialBackend{backend: TransportBackendAuto}); StatusFromError(err).Code != CodeInvalidArgument {
		t.Fatalf("automatic backend identity error = %v, want InvalidArgument", err)
	}
	for _, backend := range []TransportBackend{TransportBackendNative, TransportBackendQUICGo} {
		if _, err := newBackendChannelConnector("example.test:443", DialOptions{}, testDialBackend{backend: backend}); StatusFromError(err).Code != CodeInvalidArgument {
			t.Fatalf("backend %v nil connector error = %v, want InvalidArgument", backend, err)
		}
	}
}

func TestOptionalDialBackendReceivesCopiedOptions(t *testing.T) {
	credentials := &TransportCredentials{RootCAPEM: []byte("root")}
	headers := HeaderFields{{Name: "Origin", Value: "https://origin.test"}}
	backend := &recordingDialBackend{}
	connector, err := newBackendChannelConnector("https://example.test/rpc", DialOptions{
		Credentials:  credentials,
		MaxFrameSize: 4096,
		WebTransport: WebTransportOptions{RequestHeaders: headers},
	}, backend)
	if err != nil {
		t.Fatalf("newBackendChannelConnector() error = %v", err)
	}
	if _, ok := connector.(backendChannelConnector); !ok {
		t.Fatalf("connector type = %T", connector)
	}
	credentials.RootCAPEM[0] = 'x'
	headers[0].Value = "changed"
	if backend.target != "https://example.test/rpc" || backend.options.MaxFrameSize != 4096 ||
		string(backend.options.Credentials.RootCAPEM) != "root" ||
		backend.options.WebTransport.RequestHeaders[0].Value != "https://origin.test" {
		t.Fatalf("backend options were not copied: target=%q options=%+v", backend.target, backend.options)
	}
}

func TestBackendChannelGenerationReleasesDisconnectedExactlyOnce(t *testing.T) {
	endpoint := &testBackendEndpoint{done: make(chan struct{})}
	closeCalls := 0
	generation := &backendChannelGeneration{
		connection: BackendConnection{
			Endpoint: endpoint,
			Close: func() error {
				closeCalls++
				return nil
			},
		},
	}

	generation.releaseDisconnected()
	if err := generation.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if closeCalls != 1 {
		t.Fatalf("backend close calls = %d, want 1", closeCalls)
	}

	reasonErr := errors.New("peer closed")
	reason := generation.CloseReason(reasonErr)
	if !reason.Peer || reason.ApplicationCode != 42 || !errors.Is(reason.Err, reasonErr) {
		t.Fatalf("endpoint close reason = %+v", reason)
	}
}

func TestOptionalListenBackendReceivesEffectiveServerLimits(t *testing.T) {
	server := NewServer()
	serverOptions := server.Options()
	serverOptions.MaxFrameSize = 8192
	serverOptions.MaxConcurrentConnections = 25
	serverOptions.MaxConcurrentStreamsPerConnection = 200
	serverOptions.StreamIdleTimeout = 10 * time.Second
	server.SetOptions(serverOptions)

	backend := &recordingListenBackend{listener: &testBackendListener{}}
	listener, err := ListenWithBackend("127.0.0.1:0", server, ListenOptions{
		Transport: TransportConfig{KeepAlive: time.Second},
		Limits: TransportLimits{
			IncomingBidirectionalStreams: 50,
		},
	}, backend)
	if err != nil {
		t.Fatalf("ListenWithBackend() error = %v", err)
	}
	defer listener.Close()

	if backend.options.Server.MaxFrameSize != 8192 ||
		backend.options.Server.MaxConcurrentConnections != 25 ||
		backend.options.Server.MaxConcurrentStreamsPerConnection != 200 {
		t.Fatalf("backend server options = %+v", backend.options.Server)
	}
	if backend.options.Transport.MaxIdleTimeout != 10*time.Second ||
		backend.options.Transport.KeepAlive != time.Second {
		t.Fatalf("backend transport options = %+v", backend.options.Transport)
	}
	if backend.options.Limits.StreamReceiveWindow != 8192+transportFrameHeaderSize ||
		backend.options.Limits.IncomingBidirectionalStreams != 50 {
		t.Fatalf("backend transport limits = %+v", backend.options.Limits)
	}
	if backend.options.AdmitHTTP3 == nil || backend.options.AdmitWebTransport == nil {
		t.Fatal("backend admission callbacks were not populated")
	}
}

func TestOptionalListenBackendValidationAndFreeze(t *testing.T) {
	server := NewServer()
	if _, err := ListenWithBackend("127.0.0.1:0", server, ListenOptions{}, nil); StatusFromError(err).Code != CodeInvalidArgument {
		t.Fatalf("nil backend error = %v, want InvalidArgument", err)
	}
	if _, err := ListenWithBackend("127.0.0.1:0", server, ListenOptions{}, testListenBackend{backend: TransportBackendAuto}); StatusFromError(err).Code != CodeInvalidArgument {
		t.Fatalf("automatic backend identity error = %v, want InvalidArgument", err)
	}
	if _, err := ListenWithBackend("127.0.0.1:0", server, ListenOptions{}, testListenBackend{backend: TransportBackendNative}); StatusFromError(err).Code != CodeInvalidArgument {
		t.Fatalf("native nil listener error = %v, want InvalidArgument", err)
	}

	server = NewServer()
	backend := &recordingListenBackend{listener: &testBackendListener{}}
	listener, err := ListenWithBackend("127.0.0.1:0", server, ListenOptions{}, backend)
	if err != nil {
		t.Fatalf("ListenWithBackend() error = %v", err)
	}
	if listener.Addr().String() != "127.0.0.1:8443" {
		t.Fatalf("listener address = %q", listener.Addr())
	}
	defer listener.Close()
	defer func() {
		if recovered := recover(); recovered != ErrServerFrozen {
			t.Fatalf("post-Listen mutation panic = %#v, want ErrServerFrozen", recovered)
		}
	}()
	server.SetOptions(ServerOptions{})
}

type testDialBackend struct {
	backend TransportBackend
}

func (b testDialBackend) Backend() TransportBackend { return b.backend }

func (testDialBackend) NewConnector(string, BackendDialOptions) (BackendConnector, error) {
	return nil, nil
}

type recordingDialBackend struct {
	target  string
	options BackendDialOptions
}

func (*recordingDialBackend) Backend() TransportBackend { return TransportBackendQUICGo }

func (b *recordingDialBackend) NewConnector(target string, options BackendDialOptions) (BackendConnector, error) {
	b.target = target
	b.options = options
	return testBackendConnector{}, nil
}

type testBackendConnector struct{}

func (testBackendConnector) Connect(context.Context) (BackendConnection, error) {
	return BackendConnection{}, errors.New("not connected")
}

type testBackendEndpoint struct {
	done chan struct{}
}

func (*testBackendEndpoint) OpenStream(context.Context) (transportapi.BidirectionalStream, error) {
	return nil, errors.New("not implemented")
}
func (*testBackendEndpoint) AcceptStream(context.Context) (transportapi.BidirectionalStream, error) {
	return nil, errors.New("not implemented")
}
func (e *testBackendEndpoint) Done() <-chan struct{} { return e.done }
func (*testBackendEndpoint) Err() error              { return nil }
func (*testBackendEndpoint) Info() ConnectionInfo    { return ConnectionInfo{} }
func (*testBackendEndpoint) Close(TransportCloseReason) error {
	return nil
}
func (*testBackendEndpoint) CloseReason(err error) TransportCloseReason {
	return TransportCloseReason{Peer: true, ApplicationCode: 42, Err: err}
}

type testListenBackend struct {
	backend TransportBackend
}

func (b testListenBackend) Backend() TransportBackend { return b.backend }
func (testListenBackend) Listen(string, BackendListenOptions) (transportapi.Listener, error) {
	return nil, nil
}
func (testListenBackend) ServeConnection(context.Context, transportapi.Connection, BackendServerConnection) {
}
func (testListenBackend) ListenerClosed(error) bool { return false }
func (testListenBackend) MapStatus(err error) error { return err }

type recordingListenBackend struct {
	listener *testBackendListener
	options  BackendListenOptions
}

func (*recordingListenBackend) Backend() TransportBackend { return TransportBackendQUICGo }
func (b *recordingListenBackend) Listen(_ string, options BackendListenOptions) (transportapi.Listener, error) {
	b.options = options
	return b.listener, nil
}
func (*recordingListenBackend) ServeConnection(context.Context, transportapi.Connection, BackendServerConnection) {
}
func (*recordingListenBackend) ListenerClosed(error) bool { return false }
func (*recordingListenBackend) MapStatus(err error) error { return err }

type testBackendListener struct {
	closed bool
}

func (*testBackendListener) Accept(context.Context) (transportapi.Connection, error) {
	return nil, errors.New("closed")
}
func (*testBackendListener) Address() transportapi.Address {
	return transportapi.Address{Network: "udp", Host: "127.0.0.1", Port: 8443}
}
func (l *testBackendListener) Close() error {
	l.closed = true
	return nil
}
