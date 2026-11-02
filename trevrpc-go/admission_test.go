package trevrpc

import (
	"context"
	"net/http"
	"testing"
)

func TestSameOriginAdmission(t *testing.T) {
	admit := SameOriginAdmission("http://localhost:3000", "HTTPS://Partner.Example:443", "https://[::1]:8443")
	for _, test := range []struct {
		name      string
		origin    string
		authority string
		insecure  bool
		want      bool
	}{
		{name: "no origin", origin: "", authority: "rpc.example", want: true},
		{name: "own origin", origin: "https://rpc.example", authority: "rpc.example", want: true},
		{name: "own origin with port", origin: "https://rpc.example:8443", authority: "rpc.example:8443", want: true},
		{name: "own origin ignores case", origin: "HTTPS://RPC.Example", authority: "rpc.EXAMPLE", want: true},
		{name: "default port in origin", origin: "https://rpc.example:443", authority: "rpc.example", want: true},
		{name: "default port in authority", origin: "https://rpc.example", authority: "rpc.example:443", want: true},
		{name: "own IPv6 origin", origin: "https://[::1]:9000", authority: "[::1]:9000", want: true},
		{name: "insecure own origin", origin: "http://rpc.example", authority: "rpc.example", insecure: true, want: true},
		{name: "http page on secure server", origin: "http://rpc.example", authority: "rpc.example", want: false},
		{name: "https page on insecure server", origin: "https://rpc.example", authority: "rpc.example", insecure: true, want: false},
		{name: "other port", origin: "https://rpc.example:8443", authority: "rpc.example", want: false},
		{name: "other host", origin: "https://evil.example", authority: "rpc.example", want: false},
		{name: "suffix host", origin: "https://rpc.example.evil", authority: "rpc.example", want: false},
		{name: "null origin", origin: "null", authority: "rpc.example", want: false},
		{name: "origin with path", origin: "https://rpc.example/", authority: "rpc.example", want: false},
		{name: "origin with user", origin: "https://user@rpc.example", authority: "rpc.example", want: false},
		{name: "empty port", origin: "https://rpc.example:", authority: "rpc.example", want: false},
		{name: "allowed origin", origin: "http://localhost:3000", authority: "rpc.example", want: true},
		{name: "allowed origin, other port", origin: "http://localhost:3001", authority: "rpc.example", want: false},
		{name: "allowed origin, other scheme", origin: "https://localhost:3000", authority: "rpc.example", want: false},
		{name: "allowed origin normalized", origin: "https://partner.example", authority: "rpc.example", want: true},
		{name: "allowed IPv6 origin", origin: "https://[::1]:8443", authority: "rpc.example", want: true},
		{name: "unbracketed IPv6", origin: "https://::1", authority: "rpc.example", want: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			request := AdmissionRequest{Origin: test.origin, Authority: test.authority, Secure: !test.insecure}
			if got := admit(request); got != test.want {
				t.Fatalf("admit(%+v) = %v, want %v", request, got, test.want)
			}
		})
	}
}

func TestSameOriginAdmissionPanicsOnInvalidAllowedOrigin(t *testing.T) {
	for _, origin := range []string{"null", "localhost:3000", "https://", "https://host/path", "https://host:http"} {
		t.Run(origin, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Fatalf("SameOriginAdmission(%q) did not panic", origin)
				}
			}()
			SameOriginAdmission(origin)
		})
	}
}

func TestSameOriginAdmissionOverBrowserTransports(t *testing.T) {
	pki := newTestPKI(t)
	server := NewServer()
	options := server.Options()
	options.EnableWebTransport = true
	options.WebTransportAdmission = SameOriginAdmission(testOrigin)
	server.SetOptions(options)
	routeBytes(server, "/service/method", func(context.Context, []byte) ([]byte, error) { return []byte("ok"), nil })
	listener := serveTestListener(t, server, ListenOptions{TLSConfig: pki.server})

	for name, origin := range map[string]string{"own": "https://" + listener.Addr().String(), "allowed": testOrigin} {
		t.Run("webtransport "+name, func(t *testing.T) {
			response, session, err := dialWebTransportTest(pki, listener, webTransportTestDial{origin: origin})
			if err != nil {
				t.Fatalf("WebTransport session from %s was refused: %v", origin, err)
			}
			_ = session.CloseWithError(0, "test done")
			_ = response.Body.Close()
		})
	}
	for name, origin := range map[string]string{"own": "https://" + listener.TCPAddr().String(), "allowed": testOrigin} {
		t.Run("websocket "+name, func(t *testing.T) {
			session, err := dialWebSocketTestSession(t, pki, listener, origin)
			if err != nil {
				t.Fatalf("WebSocket session from %s was refused: %v", origin, err)
			}
			_ = session.CloseWithError(0, "test done")
		})
	}

	const other = "https://evil.example"
	response, session, err := dialWebTransportTest(pki, listener, webTransportTestDial{origin: other})
	if err == nil {
		_ = session.CloseWithError(0, "test done")
		t.Fatal("WebTransport session from another origin was admitted")
	}
	if response == nil || response.StatusCode != http.StatusForbidden {
		t.Fatalf("WebTransport session from another origin got %v, want 403", err)
	}
	_ = response.Body.Close()
	if session, err := dialWebSocketTestSession(t, pki, listener, other); err == nil {
		_ = session.CloseWithError(0, "test done")
		t.Fatal("WebSocket session from another origin was admitted")
	}
}
