package trevrpc

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/quic-go/quic-go/http3"
)

// fallbackTestHandler answers every request with its protocol and path.
var fallbackTestHandler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
	_, _ = io.WriteString(w, "fallback "+r.Proto+" "+r.URL.Path)
})

// fallbackTestClients returns an HTTP client for each HTTP version the
// listener serves, keyed by the response protocol they produce.
func fallbackTestClients(t *testing.T, pki testPKI, listener *Listener) map[string]struct {
	client *http.Client
	origin string
} {
	t.Helper()
	h3 := &http3.Transport{TLSClientConfig: pki.client.Clone()}
	t.Cleanup(func() { _ = h3.Close() })
	tcpClient := func(configure func(*http.Protocols)) *http.Client {
		protocols := new(http.Protocols)
		configure(protocols)
		transport := &http.Transport{TLSClientConfig: pki.client.Clone(), Protocols: protocols}
		t.Cleanup(transport.CloseIdleConnections)
		return &http.Client{Transport: transport}
	}
	udp := "https://" + listener.Addr().String()
	tcp := "https://" + listener.TCPAddr().String()
	return map[string]struct {
		client *http.Client
		origin string
	}{
		"HTTP/3.0": {&http.Client{Transport: h3}, udp},
		"HTTP/2.0": {tcpClient(func(p *http.Protocols) { p.SetHTTP2(true) }), tcp},
		"HTTP/1.1": {tcpClient(func(p *http.Protocols) { p.SetHTTP1(true) }), tcp},
	}
}

// fallbackTestResponse is what a test reads from a response.
type fallbackTestResponse struct {
	status int
	proto  string
	altSvc string
	body   string
}

func getFallbackTest(t *testing.T, client *http.Client, url string) fallbackTestResponse {
	t.Helper()
	request, err := http.NewRequestWithContext(t.Context(), http.MethodGet, url, nil)
	if err != nil {
		t.Fatal(err)
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
	return fallbackTestResponse{
		status: response.StatusCode,
		proto:  response.Proto,
		altSvc: response.Header.Get("Alt-Svc"),
		body:   string(body),
	}
}

func TestHTTPFallbackServesUnclaimedPathsAlongsideRPCs(t *testing.T) {
	pki := newTestPKI(t)
	listener := startTestListener(t, pki, func(options *ServerOptions) { options.HTTPFallback = fallbackTestHandler })
	wantAltSvc := `h3=":` + strings.TrimPrefix(listener.Addr().String(), "127.0.0.1:") + `"; ma=86400`

	for proto, c := range fallbackTestClients(t, pki, listener) {
		t.Run(proto, func(t *testing.T) {
			response := getFallbackTest(t, c.client, c.origin+"/app/index.html")
			if response.status != http.StatusOK || response.proto != proto {
				t.Fatalf("response %s %d, want %s 200", response.proto, response.status, proto)
			}
			if want := "fallback " + proto + " /app/index.html"; response.body != want {
				t.Fatalf("body = %q, want %q", response.body, want)
			}
			wantHeader := wantAltSvc
			if proto == "HTTP/3.0" {
				wantHeader = ""
			}
			if response.altSvc != wantHeader {
				t.Fatalf("Alt-Svc = %q, want %q", response.altSvc, wantHeader)
			}

			// HTTPPath stays TrevRPC's: a GET is rejected rather than served by the fallback.
			response = getFallbackTest(t, c.client, c.origin+DefaultHTTPPath)
			if strings.HasPrefix(response.body, "fallback") {
				t.Fatalf("GET %s reached the fallback: %q", DefaultHTTPPath, response.body)
			}
			want := http.StatusMethodNotAllowed
			if proto == "HTTP/1.1" {
				want = http.StatusHTTPVersionNotSupported
			}
			if response.status != want {
				t.Fatalf("GET %s status = %d, want %d", DefaultHTTPPath, response.status, want)
			}
		})
	}

	for name, transport := range map[string]Transport{
		"HTTP/3": newHTTP3TestClient(t, pki, listener),
		"HTTP/2": newHTTP2TestClient(t, pki, listener),
	} {
		if _, err := Unary(t.Context(), transport, "/example.Greeter/Echo", &testMessage{Value: "x"}, newTestMessage); err != nil {
			t.Fatalf("%s RPC alongside the fallback: %v", name, err)
		}
	}
}

func TestHTTPFallbackAloneServesEveryHTTPVersion(t *testing.T) {
	pki := newTestPKI(t)
	listener := startTestListener(t, pki, func(options *ServerOptions) {
		options.EnableHTTP = false
		options.EnableWebTransport = false
		options.HTTPFallback = fallbackTestHandler
	})
	for proto, c := range fallbackTestClients(t, pki, listener) {
		t.Run(proto, func(t *testing.T) {
			// TrevRPC claims no path, so even HTTPPath is the fallback's.
			response := getFallbackTest(t, c.client, c.origin+DefaultHTTPPath)
			if response.status != http.StatusOK || response.body != "fallback "+proto+" "+DefaultHTTPPath {
				t.Fatalf("response %s %d %q, want the fallback", response.proto, response.status, response.body)
			}
		})
	}
	if _, err := Unary(context.Background(), dialNative(t, pki, listener), "/example.Greeter/Echo", &testMessage{Value: "x"}, newTestMessage); err != nil {
		t.Fatalf("native call: %v", err)
	}
}

func TestHTTPWithoutFallbackAnswersUnclaimedPathsWith404(t *testing.T) {
	listener, _ := newHandlerTestListener(nil)
	recorder := httptest.NewRecorder()
	listener.handleHTTP3(recorder, newHTTP3TestRequest(t, http.MethodGet, "/index.html", nil))
	if recorder.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusNotFound)
	}
}

func TestHTTPFallbackHoldsAStreamSlot(t *testing.T) {
	listener, _ := newHandlerTestListener(func(options *ServerOptions) { options.HTTPFallback = fallbackTestHandler })
	state := &httpConnState{streamLimit: newSemaphore(1)}
	request := newHTTP3TestRequest(t, http.MethodGet, "/index.html", nil)
	request = request.WithContext(context.WithValue(request.Context(), httpConnStateKey{}, state))

	slot, ok := acquireHTTPStreamSlot(request)
	if !ok {
		t.Fatal("first slot was not acquired")
	}
	recorder := httptest.NewRecorder()
	listener.handleHTTP3(recorder, request)
	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("status on a saturated connection = %d, want %d", recorder.Code, http.StatusServiceUnavailable)
	}

	slot.release()
	recorder = httptest.NewRecorder()
	listener.handleHTTP3(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status after the slot was freed = %d, want %d", recorder.Code, http.StatusOK)
	}
	// The fallback releases its slot when it returns.
	if slot, ok := acquireHTTPStreamSlot(request); !ok {
		t.Fatal("fallback kept its slot")
	} else {
		slot.release()
	}
}
