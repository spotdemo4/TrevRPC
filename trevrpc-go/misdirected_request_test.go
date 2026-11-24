package trevrpc

import (
	"crypto/tls"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestMisdirected(t *testing.T) {
	for _, c := range []struct {
		host, serverName string
		want             bool
	}{
		{"b.example", "a.example", true},
		{"b.example:443", "a.example", true},
		{"a.example.evil", "a.example", true},
		{"a.example", "a.example", false},
		{"a.example:8443", "a.example", false},
		{"A.Example.", "a.example", false},
		{"a.example.:443", "A.EXAMPLE", false},
		{"a.example", "a.example.", false},
		{"b.example", "", false},
		{"", "a.example", false},
		{"127.0.0.1:8080", "localhost", false},
		{"[::1]:8080", "localhost", false},
		{"[::1]", "localhost", false},
		{"localhost", "127.0.0.1", false},
	} {
		if got := misdirected(c.host, c.serverName); got != c.want {
			t.Errorf("misdirected(%q, %q) = %v, want %v", c.host, c.serverName, got, c.want)
		}
	}
}

// misdirectedTestRequest sends a request for host to url over client, after
// configure prepares it, and returns the response status and body.
func misdirectedTestRequest(t *testing.T, client *http.Client, method, url, host string, configure func(*http.Request)) (int, string) {
	t.Helper()
	request, err := http.NewRequestWithContext(t.Context(), method, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Host = host
	if configure != nil {
		configure(request)
	}
	response, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = response.Body.Close() }()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	return response.StatusCode, string(body)
}

func TestMisdirectedRequestsGet421OverEveryHTTPVersion(t *testing.T) {
	pki := newTestPKI(t)
	diagnostics := make(chan ServerDiagnostic, 64)
	listener := startTestListenerWith(t, pki, func(options *ServerOptions) { options.HTTPFallback = fallbackTestHandler }, func(server *Server) {
		registerTestService(server)
		server.SetDiagnostics(func(diagnostic ServerDiagnostic) {
			if diagnostic.Phase == ServerDiagnosticMisdirectedRequest {
				diagnostics <- diagnostic
			}
		})
	})
	websocketUpgrade := func(request *http.Request) {
		request.Header.Set("Connection", "Upgrade")
		request.Header.Set("Upgrade", "websocket")
		request.Header.Set("Sec-WebSocket-Version", "13")
		request.Header.Set("Sec-WebSocket-Key", "dGhlIHNhbXBsZSBub25jZQ==")
		request.Header.Set("Sec-WebSocket-Protocol", QMuxALPN)
		request.Header.Set("Origin", testOrigin)
	}

	// The clients send the server name localhost.
	for proto, c := range fallbackTestClients(t, pki, listener) {
		t.Run(proto, func(t *testing.T) {
			requests := map[string]struct {
				method, path string
				configure    func(*http.Request)
			}{
				"fallback":           {http.MethodGet, "/app/index.html", nil},
				"RPC":                {http.MethodPost, DefaultHTTPPath, nil},
				"certificate hashes": {http.MethodGet, DefaultHTTPPath + certificateHashesSuffix, nil},
			}
			if proto == "HTTP/1.1" {
				requests["WebSocket"] = struct {
					method, path string
					configure    func(*http.Request)
				}{http.MethodGet, DefaultHTTPPath, websocketUpgrade}
			}
			for name, request := range requests {
				status, body := misdirectedTestRequest(t, c.client, request.method, c.origin+request.path, "other.example", request.configure)
				if status != http.StatusMisdirectedRequest {
					t.Fatalf("%s for another name: %d %q, want 421", name, status, body)
				}
				select {
				case diagnostic := <-diagnostics:
					if want := "host other.example on a connection for localhost"; diagnostic.Message != want {
						t.Fatalf("diagnostic message = %q, want %q", diagnostic.Message, want)
					}
				case <-time.After(rpcTestTimeout):
					t.Fatal("no diagnostic for the 421")
				}
			}

			// The port, case and a trailing dot do not count.
			for _, host := range []string{"localhost", "LocalHost.", "localhost.:443"} {
				if status, body := misdirectedTestRequest(t, c.client, http.MethodGet, c.origin+"/app/index.html", host, nil); status != http.StatusOK || body != "fallback "+proto+" /app/index.html" {
					t.Fatalf("host %s: %d %q, want the fallback", host, status, body)
				}
			}
		})
	}

	// Clients send no server name when they reach the server by address.
	noServerName := testPKI{client: &tls.Config{RootCAs: pki.client.RootCAs}}
	for proto, c := range fallbackTestClients(t, noServerName, listener) {
		t.Run(proto+" without a server name", func(t *testing.T) {
			if status, body := misdirectedTestRequest(t, c.client, http.MethodGet, c.origin+"/app/index.html", "other.example", nil); status != http.StatusOK || body != "fallback "+proto+" /app/index.html" {
				t.Fatalf("%d %q, want the fallback", status, body)
			}
		})
	}
}

func TestMisdirectedWebTransportConnectGets421(t *testing.T) {
	listener, _ := newHandlerTestListener(func(options *ServerOptions) {
		options.EnableWebTransport = true
		options.WebTransportAdmission = func(AdmissionRequest) bool { return true }
	})
	request := newHTTP3TestRequest(t, http.MethodConnect, DefaultHTTPPath, nil)
	request.Proto = "webtransport"
	request.TLS = &tls.ConnectionState{ServerName: "a.example"}
	request.Host = "b.example"
	recorder := httptest.NewRecorder()
	listener.handleHTTP3(recorder, request)
	if recorder.Code != http.StatusMisdirectedRequest {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusMisdirectedRequest)
	}
}

func TestDisableMisdirectedRequestCheckServesEveryHost(t *testing.T) {
	listener, _ := newHandlerTestListener(func(options *ServerOptions) {
		options.HTTPFallback = fallbackTestHandler
		options.DisableMisdirectedRequestCheck = true
	})
	request := newHTTP3TestRequest(t, http.MethodGet, "/index.html", nil)
	request.TLS = &tls.ConnectionState{ServerName: "a.example"}
	request.Host = "b.example"
	recorder := httptest.NewRecorder()
	listener.handleHTTP3(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusOK)
	}
}
