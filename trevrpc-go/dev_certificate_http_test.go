package trevrpc

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// startDevelopmentCertificateListener serves the test service with
// ServerOptions.DevelopmentCertificate set to a new certificate, and returns
// the test PKI whose clients trust it.
func startDevelopmentCertificateListener(t *testing.T, configure func(*ServerOptions)) (*DevelopmentCertificate, testPKI, *Listener) {
	t.Helper()
	certificate, err := NewDevelopmentCertificate()
	if err != nil {
		t.Fatal(err)
	}
	certificatePEM, err := certificate.PEM()
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(certificatePEM) {
		t.Fatal("PEM holds no certificates")
	}
	pki := testPKI{client: &tls.Config{RootCAs: roots, ServerName: "localhost"}}
	server := NewServer()
	options := server.Options()
	options.EnableHTTP = true
	options.EnableWebTransport = true
	options.DevelopmentCertificate = certificate
	if configure != nil {
		configure(&options)
	}
	server.SetOptions(options)
	registerTestService(server)
	return certificate, pki, serveTestListener(t, server, ListenOptions{})
}

func TestServerServesDevelopmentCertificateHashesOnEveryHTTPTransport(t *testing.T) {
	certificate, pki, listener := startDevelopmentCertificateListener(t, func(options *ServerOptions) {
		options.HTTPFallback = fallbackTestHandler
	})
	hashes := mustHashes(t, certificate)
	for proto, c := range fallbackTestClients(t, pki, listener) {
		t.Run(proto, func(t *testing.T) {
			response := getFallbackTest(t, c.client, c.origin+DefaultHTTPPath+"/certificate-hashes")
			if response.status != http.StatusOK || response.proto != proto {
				t.Fatalf("response %s %d %q, want %s 200", response.proto, response.status, response.body, proto)
			}
			for name, want := range map[string]string{
				"Content-Type":                "application/json",
				"Cache-Control":               "no-store",
				"Access-Control-Allow-Origin": "*",
			} {
				if got := response.header.Get(name); got != want {
					t.Fatalf("%s = %q, want %q", name, got, want)
				}
			}
			var entries []struct{ Algorithm, Value string }
			if err := json.Unmarshal([]byte(response.body), &entries); err != nil {
				t.Fatalf("body %q: %v", response.body, err)
			}
			if len(entries) != len(hashes) {
				t.Fatalf("entries = %+v, want %d", entries, len(hashes))
			}
			for i, entry := range entries {
				if entry.Algorithm != "sha-256" || entry.Value != hex.EncodeToString(hashes[i][:]) {
					t.Fatalf("entry %d = %+v, want sha-256 %x", i, entry, hashes[i])
				}
			}

			// Every other path still reaches the fallback.
			response = getFallbackTest(t, c.client, c.origin+"/certificate-hashes")
			if want := "fallback " + proto + " /certificate-hashes"; response.body != want {
				t.Fatalf("body of the path outside HTTPPath = %q, want %q", response.body, want)
			}
		})
	}

	// The same certificate secures native QUIC, and RPCs run next to the hashes.
	if _, err := Unary(t.Context(), dialNative(t, pki, listener), "/example.Greeter/Echo", &testMessage{Value: "x"}, newTestMessage); err != nil {
		t.Fatalf("native call: %v", err)
	}
	if _, err := Unary(t.Context(), newHTTP3TestClient(t, pki, listener), "/example.Greeter/Echo", &testMessage{Value: "x"}, newTestMessage); err != nil {
		t.Fatalf("HTTP/3 call: %v", err)
	}
}

func TestServerAnswers404ForHashesWithoutDevelopmentCertificate(t *testing.T) {
	pki := newTestPKI(t)
	listener := startTestListener(t, pki, func(options *ServerOptions) { options.HTTPFallback = fallbackTestHandler })
	for proto, c := range fallbackTestClients(t, pki, listener) {
		t.Run(proto, func(t *testing.T) {
			response := getFallbackTest(t, c.client, c.origin+DefaultHTTPPath+"/certificate-hashes")
			if response.status != http.StatusNotFound || strings.HasPrefix(response.body, "fallback") {
				t.Fatalf("response %d %q, want 404 that the fallback did not answer", response.status, response.body)
			}
			// A page on another origin can read the 404, which means that the browser trusts the certificate.
			if got := response.header.Get("Access-Control-Allow-Origin"); got != "*" {
				t.Fatalf("Access-Control-Allow-Origin = %q, want *", got)
			}
		})
	}
}

func TestDevelopmentCertificateHashesFollowHTTPPath(t *testing.T) {
	certificate, err := NewDevelopmentCertificate()
	if err != nil {
		t.Fatal(err)
	}
	fallback := http.HandlerFunc(func(http.ResponseWriter, *http.Request) {})
	for _, test := range []struct {
		name       string
		options    ServerOptions
		path       string
		wantHashes bool
	}{
		{"default path", ServerOptions{EnableHTTP: true, HTTPPath: "/trevrpc"}, "/trevrpc/certificate-hashes", true},
		{"root path", ServerOptions{EnableHTTP: true, HTTPPath: "/"}, "/certificate-hashes", true},
		{"nested path", ServerOptions{EnableWebTransport: true, HTTPPath: "/api/rpc"}, "/api/rpc/certificate-hashes", true},
		{"another prefix", ServerOptions{EnableHTTP: true, HTTPPath: "/trevrpc"}, "/other/certificate-hashes", false},
		{"app path", ServerOptions{EnableHTTP: true, HTTPPath: "/trevrpc"}, "/certificate-hashes", false},
		{"suffix of another name", ServerOptions{EnableHTTP: true, HTTPPath: "/trevrpc"}, "/trevrpc/x-certificate-hashes", false},
		{"no RPC transport still claims hashes path", ServerOptions{HTTPPath: "/trevrpc"}, "/trevrpc/certificate-hashes", true},
	} {
		t.Run(test.name, func(t *testing.T) {
			options := test.options
			options.HTTPFallback = fallback
			options.DevelopmentCertificate = certificate
			handler := options.unclaimedHandler(test.path)
			if gotHashes := handler == http.Handler(certificate); gotHashes != test.wantHashes {
				t.Fatalf("serves the hashes = %t, want %t", gotHashes, test.wantHashes)
			}
			if test.wantHashes {
				// Without a certificate, the path stays TrevRPC's and answers 404.
				options.DevelopmentCertificate = nil
				recorder := httptest.NewRecorder()
				options.unclaimedHandler(test.path).ServeHTTP(recorder, httptest.NewRequestWithContext(context.Background(), http.MethodGet, test.path, nil))
				if recorder.Code != http.StatusNotFound {
					t.Fatalf("status without a certificate = %d, want 404", recorder.Code)
				}
			}
		})
	}
}

func TestListenRejectsTLSConfigWithDevelopmentCertificate(t *testing.T) {
	certificate, err := NewDevelopmentCertificate()
	if err != nil {
		t.Fatal(err)
	}
	server := NewServer()
	options := server.Options()
	options.DevelopmentCertificate = certificate
	server.SetOptions(options)
	_, err = Listen("127.0.0.1:0", server, ListenOptions{TLSConfig: newTestPKI(t).server})
	if StatusFromError(err).Code != CodeInvalidArgument {
		t.Fatalf("Listen = %v, want InvalidArgument", err)
	}
}
