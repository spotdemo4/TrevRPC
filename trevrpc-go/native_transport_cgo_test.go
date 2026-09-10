//go:build cgo && linux && (amd64 || arm64)

package trevrpc

import (
	"context"
	"testing"
	"time"

	"trev.zip/llc/trevrpc/trevrpc-go/internal/transport/native"
)

const nativeTestTimeout = 10 * time.Second

func nativeAuthenticatedOptions() []CallOption {
	return []CallOption{
		WithTimeout(nativeTestTimeout),
		WithMetadata("authorization", []byte("Bearer "+testAuthToken)),
	}
}

func TestNativeBackendRoundTripsAllRPCShapes(t *testing.T) {
	certificate, key := testCertificateMaterial(t)
	server := NewServer()
	registerTestGreeter(server)
	server.SetAuthorizer(BearerAuthorizer(testAuthToken))
	listener, err := Listen("127.0.0.1:0", server, ListenOptions{
		Credentials: &TransportCredentials{
			CertificateChainPEM: certificate,
			PrivateKeyPEM:       key,
		},
	})
	if err != nil {
		t.Fatalf("Listen() error = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	serveDone := make(chan error, 1)
	go func() { serveDone <- listener.Serve(ctx) }()
	t.Cleanup(func() {
		cancel()
		if err := <-serveDone; err != nil {
			t.Errorf("Serve() error = %v", err)
		}
	})

	dialCtx, dialCancel := context.WithTimeout(t.Context(), nativeTestTimeout)
	defer dialCancel()
	channel, err := Dial(dialCtx, listener.Addr().String(), DialOptions{
		Credentials: &TransportCredentials{
			RootCAPEM: certificate,
		},
	})
	if err != nil {
		t.Fatalf("Dial() error = %v", err)
	}
	t.Cleanup(func() { _ = channel.Close() })
	generation, err := channel.currentGeneration()
	if err != nil {
		t.Fatalf("current generation error = %v", err)
	}
	info := generation.Info()
	if info.RequestedBackend != TransportBackendAuto ||
		info.ResolvedBackend != TransportBackendNative ||
		info.Protocol != TransportProtocolNativeQUIC ||
		info.NegotiatedProtocol != ALPN ||
		info.Provider != "msquic" ||
		info.TransportABIVersion != native.EngineABIVersion {
		t.Fatalf("native connection info = %+v", info)
	}

	for index := range 4 {
		if err := runMixedQUICCallWithOptions(channel, index, nativeAuthenticatedOptions()); err != nil {
			t.Fatalf("RPC shape %d error = %v", index, err)
		}
	}
}

func TestNativeWebTransportClientRoundTripsAllRPCShapes(t *testing.T) {
	certificate, key := testCertificateMaterial(t)
	server := NewServer()
	registerTestGreeter(server)
	server.SetAuthorizer(BearerAuthorizer(testAuthToken))
	options := server.Options()
	options.EnableWebTransport = true
	options.WebTransportAdmission = func(request WebTransportAdmissionRequest) bool {
		return request.Path == DefaultHTTP3Path
	}
	server.SetOptions(options)
	listener, err := Listen("127.0.0.1:0", server, ListenOptions{
		Credentials: &TransportCredentials{
			CertificateChainPEM: certificate,
			PrivateKeyPEM:       key,
		},
	})
	if err != nil {
		t.Fatalf("Listen() error = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	serveDone := make(chan error, 1)
	go func() { serveDone <- listener.Serve(ctx) }()
	t.Cleanup(func() {
		cancel()
		if err := <-serveDone; err != nil {
			t.Errorf("Serve() error = %v", err)
		}
	})

	dialCtx, dialCancel := context.WithTimeout(t.Context(), nativeTestTimeout)
	defer dialCancel()
	channel, err := Dial(
		dialCtx,
		"https://"+listener.Addr().String()+DefaultHTTP3Path,
		DialOptions{
			Credentials: &TransportCredentials{RootCAPEM: certificate},
		},
	)
	if err != nil {
		t.Fatalf("Dial() error = %v", err)
	}
	t.Cleanup(func() { _ = channel.Close() })
	generation, err := channel.currentGeneration()
	if err != nil {
		t.Fatalf("current generation error = %v", err)
	}
	info := generation.Info()
	if info.RequestedBackend != TransportBackendAuto ||
		info.ResolvedBackend != TransportBackendNative ||
		info.Protocol != TransportProtocolWebTransport ||
		info.NegotiatedProtocol != "h3" ||
		info.Provider != "msquic" ||
		info.TransportABIVersion != native.TransportABIVersion {
		t.Fatalf("native WebTransport connection info = %+v", info)
	}

	for index := range 4 {
		if err := runMixedQUICCallWithOptions(channel, index, nativeAuthenticatedOptions()); err != nil {
			t.Fatalf("RPC shape %d error = %v", index, err)
		}
	}
}

func TestNativeChannelReconnectsWithoutReplayOwnership(t *testing.T) {
	certificate, key := testCertificateMaterial(t)
	server := NewServer()
	registerTestGreeter(server)
	server.SetAuthorizer(BearerAuthorizer(testAuthToken))
	firstListener, firstCancel, firstDone := startNativeTestServer(
		t,
		"127.0.0.1:0",
		server,
		certificate,
		key,
	)
	address := firstListener.Addr().String()

	events := make(chan ChannelEvent, 16)
	dialCtx, dialCancel := context.WithTimeout(t.Context(), nativeTestTimeout)
	defer dialCancel()
	channel, err := Dial(dialCtx, address, DialOptions{
		Credentials: &TransportCredentials{RootCAPEM: certificate},
		OnEvent: func(event ChannelEvent) {
			events <- event
		},
	})
	if err != nil {
		t.Fatalf("Dial() error = %v", err)
	}
	defer func() { _ = channel.Close() }()

	firstCancel()
	if err := <-firstDone; err != nil {
		t.Fatalf("first Serve() error = %v", err)
	}
	_, secondCancel, secondDone := startNativeTestServer(
		t,
		address,
		server,
		certificate,
		key,
	)
	defer func() {
		secondCancel()
		if err := <-secondDone; err != nil {
			t.Errorf("second Serve() error = %v", err)
		}
	}()

	var sawDisconnect bool
	for channel.Generation() < 2 {
		select {
		case event := <-events:
			if event.Type == ChannelEventDisconnected {
				sawDisconnect = true
			}
		case <-dialCtx.Done():
			t.Fatalf("timed out waiting for native reconnect: %v", dialCtx.Err())
		}
	}
	if !sawDisconnect {
		t.Fatal("native reconnect did not publish a disconnect event")
	}
	response, err := Unary(
		dialCtx,
		channel,
		testServiceName,
		"SayHello",
		&testMessage{Value: "reconnected"},
		func() *testMessage { return &testMessage{} },
		authenticatedOptions()...,
	)
	if err != nil {
		t.Fatalf("reconnected unary RPC error = %v", err)
	}
	if response.Value != "hello, reconnected" {
		t.Fatalf("reconnected unary response = %q", response.Value)
	}
}

func startNativeTestServer(
	t *testing.T,
	address string,
	server *Server,
	certificate []byte,
	key []byte,
) (ServerListener, context.CancelFunc, <-chan error) {
	t.Helper()
	listener, err := Listen(address, server, ListenOptions{
		Credentials: &TransportCredentials{
			CertificateChainPEM: certificate,
			PrivateKeyPEM:       key,
		},
	})
	if err != nil {
		t.Fatalf("Listen(%q) error = %v", address, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- listener.Serve(ctx) }()
	return listener, cancel, done
}

func TestNativeWebTransportEndpointMapping(t *testing.T) {
	connector, err := newNativeWebTransportConnector(
		"https://example.test:8443/custom",
		DialOptions{
			Credentials: &TransportCredentials{
				ServerName:         "override.test",
				InsecureSkipVerify: true,
			},
			WebTransport: WebTransportOptions{
				RequestHeaders:          HeaderFields{{Name: "Origin", Value: "https://origin.test"}},
				StreamReorderingTimeout: 250 * time.Millisecond,
			},
		},
	)
	if err != nil {
		t.Fatalf("newNativeWebTransportConnector() error = %v", err)
	}
	if connector.endpoint.Protocol != native.ProtocolWebTransport ||
		connector.endpoint.Host != "example.test" ||
		connector.endpoint.Port != 8443 ||
		connector.endpoint.ServerName != "override.test" ||
		connector.endpoint.Path != "/custom" ||
		connector.endpoint.Origin != "https://origin.test" ||
		connector.endpoint.UnresolvedStreamTimeoutMS != 250 {
		t.Fatalf("native WebTransport endpoint = %+v", connector.endpoint)
	}

	defaultConnector, err := newNativeWebTransportConnector(
		"https://example.test",
		DialOptions{
			Credentials: &TransportCredentials{InsecureSkipVerify: true},
		},
	)
	if err != nil {
		t.Fatalf("default newNativeWebTransportConnector() error = %v", err)
	}
	if defaultConnector.endpoint.Port != 443 || defaultConnector.endpoint.Path != DefaultHTTP3Path {
		t.Fatalf("default native WebTransport endpoint = %+v", defaultConnector.endpoint)
	}
}

func TestNativeWebTransportRejectsUnsupportedOptions(t *testing.T) {
	tests := []struct {
		name    string
		target  string
		options WebTransportOptions
	}{
		{
			name:   "arbitrary header",
			target: "https://example.test/trevrpc",
			options: WebTransportOptions{
				RequestHeaders: HeaderFields{{Name: "X-Test", Value: "value"}},
			},
		},
		{
			name:   "duplicate origin",
			target: "https://example.test/trevrpc",
			options: WebTransportOptions{
				RequestHeaders: HeaderFields{
					{Name: "Origin", Value: "https://one.test"},
					{Name: "origin", Value: "https://two.test"},
				},
			},
		},
		{
			name:   "query",
			target: "https://example.test/trevrpc?value=1",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := newNativeWebTransportConnector(test.target, DialOptions{
				Credentials:  &TransportCredentials{InsecureSkipVerify: true},
				WebTransport: test.options,
			})
			if err == nil {
				t.Fatal("newNativeWebTransportConnector() returned nil error")
			}
		})
	}
}

func TestNativeServerEndpointSettings(t *testing.T) {
	certificate, key := testCertificateMaterial(t)
	credentials := &TransportCredentials{
		CertificateChainPEM: certificate,
		PrivateKeyPEM:       key,
	}
	defaults := native.DefaultEndpointConfig()
	defaults.Host = "127.0.0.1"
	defaults.ALPN = []byte(ALPN)

	tests := []struct {
		name      string
		configure func(*native.EndpointConfig)
	}{
		{name: "defaults"},
		{name: "stream count", configure: func(config *native.EndpointConfig) {
			config.PeerBidirectionalStreams = 65
		}},
		{name: "stream receive window", configure: func(config *native.EndpointConfig) {
			config.StreamReceiveWindow = DefaultMaxFrameSize
		}},
		{name: "connection receive window", configure: func(config *native.EndpointConfig) {
			config.ConnectionFlowControlWindow = 16 * 1024 * 1024
		}},
		{name: "receive windows", configure: func(config *native.EndpointConfig) {
			config.StreamReceiveWindow = DefaultMaxFrameSize
			config.ConnectionFlowControlWindow = 16 * 1024 * 1024
		}},
		{name: "timeouts", configure: func(config *native.EndpointConfig) {
			config.MaxIdleTimeoutMilliseconds = 30_000
			config.KeepAliveMilliseconds = 15_000
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			config := defaults
			if test.configure != nil {
				test.configure(&config)
			}
			engine, err := native.NewEngine(defaultNativeProvider(), native.DefaultEngineConfig())
			if err != nil {
				t.Fatalf("NewEngine() error = %v", err)
			}
			t.Cleanup(func() { _ = engine.Close() })
			config, cleanup, err := materializeNativeCredentials(config, credentials, true)
			if err != nil {
				t.Fatalf("materialize credentials error = %v", err)
			}
			listener, listenErr := engine.Listen(context.Background(), config)
			cleanupErr := cleanup()
			if cleanupErr != nil {
				t.Fatalf("credential cleanup error = %v", cleanupErr)
			}
			if listenErr != nil {
				t.Fatalf("Listen() error = %v", listenErr)
			}
			if err := listener.Close(); err != nil {
				t.Fatalf("listener Close() error = %v", err)
			}
		})
	}
}
