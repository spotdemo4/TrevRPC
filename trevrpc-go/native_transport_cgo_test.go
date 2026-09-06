//go:build trevrpc_native && cgo && (linux || darwin) && (amd64 || arm64)

package trevrpc

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"net/http"
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
		Backend: TransportBackendNative,
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
		Backend: TransportBackendNative,
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
	if info.RequestedBackend != TransportBackendNative ||
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

func TestNativeMultiplexedServerRoundTripsAllTransports(t *testing.T) {
	certificate, key := testCertificateMaterial(t)
	server := NewServer()
	registerTestGreeter(server)
	server.SetAuthorizer(BearerAuthorizer(testAuthToken))
	options := server.Options()
	options.EnableHTTP3 = true
	options.EnableWebTransport = true
	options.WebTransportAdmission = func(request WebTransportAdmissionRequest) bool {
		return request.Path == DefaultHTTP3Path
	}
	server.SetOptions(options)
	listener, err := Listen("127.0.0.1:0", server, ListenOptions{
		Backend: TransportBackendNative,
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

	address := listener.Addr().String()
	dialCtx, dialCancel := context.WithTimeout(t.Context(), nativeTestTimeout)
	defer dialCancel()
	nativeChannel, err := Dial(dialCtx, address, DialOptions{
		Backend:     TransportBackendNative,
		Credentials: &TransportCredentials{RootCAPEM: certificate},
	})
	if err != nil {
		t.Fatalf("native QUIC Dial() error = %v", err)
	}
	t.Cleanup(func() { _ = nativeChannel.Close() })
	for index := range 4 {
		if err := runMixedQUICCallWithOptions(nativeChannel, index, nativeAuthenticatedOptions()); err != nil {
			t.Fatalf("native QUIC RPC shape %d error = %v", index, err)
		}
	}

	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(certificate) {
		t.Fatal("append native server certificate")
	}
	running := &runningTestQUICServer{
		addr: address,
		clientTLS: &tls.Config{
			MinVersion: tls.VersionTLS13,
			RootCAs:    roots,
		},
	}
	http3Transport := connectTestHTTP3Client(t, running)
	for index := range 4 {
		if err := runMixedQUICCallWithOptions(http3Transport, index, nativeAuthenticatedOptions()); err != nil {
			t.Fatalf("HTTP/3 RPC shape %d error = %v", index, err)
		}
	}

	webTransport := connectTestWebTransportClient(t, running)
	t.Cleanup(func() { _ = webTransport.Close() })
	for index := range 4 {
		if err := runMixedQUICCallWithOptions(webTransport, index, nativeAuthenticatedOptions()); err != nil {
			t.Fatalf("WebTransport RPC shape %d error = %v", index, err)
		}
	}
}

func TestNativeMultiplexedServerHTTP3AdmissionResponses(t *testing.T) {
	for _, test := range []struct {
		name      string
		admission HTTP3Admission
		want      int
	}{
		{
			name:      "denied",
			admission: func(HTTP3AdmissionRequest) bool { return false },
			want:      http.StatusForbidden,
		},
		{
			name:      "panic",
			admission: func(HTTP3AdmissionRequest) bool { panic("boom") },
			want:      http.StatusInternalServerError,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			certificate, key := testCertificateMaterial(t)
			server := NewServer()
			options := server.Options()
			options.EnableHTTP3 = true
			options.HTTP3Admission = test.admission
			server.SetOptions(options)
			listener, err := Listen("127.0.0.1:0", server, ListenOptions{
				Backend: TransportBackendNative,
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

			roots := x509.NewCertPool()
			if !roots.AppendCertsFromPEM(certificate) {
				t.Fatal("append native server certificate")
			}
			client := newTestHTTP3HTTPClient(t, &runningTestQUICServer{
				addr: listener.Addr().String(),
				clientTLS: &tls.Config{
					MinVersion: tls.VersionTLS13,
					RootCAs:    roots,
				},
			})
			request, err := http.NewRequest(
				http.MethodPost,
				"https://"+listener.Addr().String()+DefaultHTTP3Path,
				http.NoBody,
			)
			if err != nil {
				t.Fatalf("NewRequest() error = %v", err)
			}
			request.Header.Set("Content-Type", HTTP3ContentType)
			response, err := client.Do(request)
			if err != nil {
				t.Fatalf("HTTP/3 request error = %v", err)
			}
			defer response.Body.Close()
			if response.StatusCode != test.want {
				t.Fatalf("HTTP/3 admission status = %d, want %d", response.StatusCode, test.want)
			}
		})
	}
}

func TestNativeMultiplexedServerAdmissionSaturation(t *testing.T) {
	certificate, key := testCertificateMaterial(t)
	entered := make(chan struct{})
	release := make(chan struct{})
	releaseAdmission := func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}
	defer releaseAdmission()
	server := NewServer()
	options := server.Options()
	options.EnableHTTP3 = true
	options.MaxConcurrentAdmissionCallbacks = 1
	options.HTTP3Admission = func(HTTP3AdmissionRequest) bool {
		close(entered)
		<-release
		return true
	}
	server.SetOptions(options)
	listener, err := Listen("127.0.0.1:0", server, ListenOptions{
		Backend: TransportBackendNative,
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

	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(certificate) {
		t.Fatal("append native server certificate")
	}
	client := newTestHTTP3HTTPClient(t, &runningTestQUICServer{
		addr: listener.Addr().String(),
		clientTLS: &tls.Config{
			MinVersion: tls.VersionTLS13,
			RootCAs:    roots,
		},
	})
	newRequest := func() (*http.Request, error) {
		request, err := http.NewRequest(
			http.MethodPost,
			"https://"+listener.Addr().String()+DefaultHTTP3Path,
			http.NoBody,
		)
		if err != nil {
			return nil, err
		}
		request.Header.Set("Content-Type", HTTP3ContentType)
		return request, nil
	}
	firstRequest, err := newRequest()
	if err != nil {
		t.Fatalf("NewRequest(first) error = %v", err)
	}
	secondRequest, err := newRequest()
	if err != nil {
		t.Fatalf("NewRequest(second) error = %v", err)
	}
	type result struct {
		response *http.Response
		err      error
	}
	first := make(chan result, 1)
	go func() {
		response, err := client.Do(firstRequest)
		first <- result{response: response, err: err}
	}()
	select {
	case <-entered:
	case <-time.After(nativeTestTimeout):
		t.Fatal("first native admission callback did not start")
	}
	response, err := client.Do(secondRequest)
	if err != nil {
		t.Fatalf("saturated HTTP/3 request error = %v", err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("saturated HTTP/3 status = %d", response.StatusCode)
	}
	releaseAdmission()
	select {
	case value := <-first:
		if value.err != nil {
			t.Fatalf("first HTTP/3 request error = %v", value.err)
		}
		value.response.Body.Close()
		if value.response.StatusCode != http.StatusOK {
			t.Fatalf("first HTTP/3 status = %d", value.response.StatusCode)
		}
	case <-time.After(nativeTestTimeout):
		t.Fatal("first HTTP/3 request did not finish")
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
		Backend: TransportBackendLegacy,
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
			Backend:     TransportBackendNative,
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
	if info.RequestedBackend != TransportBackendNative ||
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
		Backend:     TransportBackendNative,
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
		Backend: TransportBackendNative,
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
			Backend: TransportBackendNative,
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
			Backend:     TransportBackendNative,
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
			name:   "application protocol",
			target: "https://example.test/trevrpc",
			options: WebTransportOptions{
				ApplicationProtocols: []string{"custom"},
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
				Backend:      TransportBackendNative,
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
			engine, err := native.NewEngine(native.DefaultEngineConfig())
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
