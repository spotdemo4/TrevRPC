package trevrpc

import (
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"trev.zip/llc/trevrpc/trevrpc-go/internal/transport/native"
)

func TestNativeStreamReceiveWindowPreservesRequiredCapacity(t *testing.T) {
	tests := []struct {
		name  string
		value uint64
		want  uint32
	}{
		{name: "zero", value: 0, want: 0},
		{name: "power of two", value: 1 << 22, want: 1 << 22},
		{name: "power of two plus one", value: 1<<22 + 1, want: 1 << 23},
		{name: "maximum encoded frame", value: 1<<22 + transportFrameHeaderSize, want: 1 << 23},
		{name: "largest power of two", value: 1 << 31, want: 1 << 31},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := nativeStreamReceiveWindow(test.value)
			if err != nil {
				t.Fatalf("nativeStreamReceiveWindow(%d) error = %v", test.value, err)
			}
			if got != test.want {
				t.Fatalf("nativeStreamReceiveWindow(%d) = %d, want %d", test.value, got, test.want)
			}
		})
	}
	if _, err := nativeStreamReceiveWindow(1<<31 + 1); err == nil {
		t.Fatal("nativeStreamReceiveWindow accepted a value that cannot round up within uint32")
	}
}

func TestNativeServerHTTP3AdmissionValidationAndProjection(t *testing.T) {
	var calls atomic.Int64
	var seen HTTP3AdmissionRequest
	server := NewServer()
	options := server.Options()
	options.EnableHTTP3 = true
	options.HTTP3Admission = func(request HTTP3AdmissionRequest) bool {
		calls.Add(1)
		seen = request
		return true
	}
	server.SetOptions(options)
	handler := nativeServerAdmissionHandler(server)

	request := native.AdmissionRequest{
		Kind:      native.AdmissionHTTP3,
		Method:    http.MethodPost,
		Path:      DefaultHTTP3Path,
		Authority: "example.test",
		Secure:    true,
		Headers: []native.AdmissionHeader{
			{Name: "X-First", Value: "one"},
			{Name: "Content-Type", Value: HTTP3ContentType},
			{Name: "X-First", Value: "two"},
		},
	}
	if status := handler(request); status != http.StatusOK {
		t.Fatalf("valid HTTP/3 admission status = %d", status)
	}
	if calls.Load() != 1 || seen.Request != nil || seen.Method != request.Method ||
		seen.Path != request.Path || seen.Authority != request.Authority || !seen.Secure ||
		len(seen.Headers) != len(request.Headers) {
		t.Fatalf("HTTP/3 admission projection = %#v, calls = %d", seen, calls.Load())
	}
	for index, header := range request.Headers {
		if seen.Headers[index].Name != header.Name || seen.Headers[index].Value != header.Value {
			t.Fatalf("HTTP/3 header %d = %#v", index, seen.Headers[index])
		}
	}

	tests := []struct {
		name   string
		mutate func(*native.AdmissionRequest)
		want   uint16
	}{
		{name: "path", mutate: func(request *native.AdmissionRequest) {
			request.Path = "/other"
		}, want: http.StatusNotFound},
		{name: "method", mutate: func(request *native.AdmissionRequest) {
			request.Method = http.MethodGet
		}, want: http.StatusMethodNotAllowed},
		{name: "content type", mutate: func(request *native.AdmissionRequest) {
			request.Headers[1].Value = "application/octet-stream"
		}, want: http.StatusUnsupportedMediaType},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			invalid := request
			invalid.Headers = append([]native.AdmissionHeader(nil), request.Headers...)
			test.mutate(&invalid)
			if status := handler(invalid); status != test.want {
				t.Fatalf("admission status = %d, want %d", status, test.want)
			}
		})
	}
	if calls.Load() != 1 {
		t.Fatalf("invalid requests invoked admission callback %d times", calls.Load()-1)
	}
}

func TestNativeServerMultiplexedEndpointSettings(t *testing.T) {
	options := DefaultServerOptions()
	endpoint, err := nativeServerEndpointConfig(
		"127.0.0.1",
		8443,
		options,
		TransportConfig{},
		TransportLimits{},
	)
	if err != nil {
		t.Fatalf("nativeServerEndpointConfig() error = %v", err)
	}
	if endpoint.Protocol != native.ProtocolNative || string(endpoint.ALPN) != ALPN ||
		endpoint.DeferAdmission || endpoint.Path != "" {
		t.Fatalf("native-only endpoint = %+v", endpoint)
	}

	options.EnableHTTP3 = true
	endpoint, err = nativeServerEndpointConfig(
		"127.0.0.1",
		8443,
		options,
		TransportConfig{},
		TransportLimits{},
	)
	if err != nil {
		t.Fatalf("HTTP/3 nativeServerEndpointConfig() error = %v", err)
	}
	if endpoint.Protocol != native.ProtocolMultiplexed ||
		endpoint.WebTransportProfiles != 0 || endpoint.MaxSessions != 0 {
		t.Fatalf("HTTP/3-only multiplexed endpoint = %+v", endpoint)
	}

	options.EnableWebTransport = true
	options.HTTP3Path = "/rpc"
	options.WebTransportDraft07Only = true
	endpoint, err = nativeServerEndpointConfig(
		"127.0.0.1",
		8443,
		options,
		TransportConfig{},
		TransportLimits{},
	)
	if err != nil {
		t.Fatalf("multiplexed nativeServerEndpointConfig() error = %v", err)
	}
	if endpoint.Protocol != native.ProtocolMultiplexed || len(endpoint.ALPN) != 0 ||
		!endpoint.DeferAdmission || endpoint.Path != "/rpc" ||
		endpoint.WebTransportProfiles != native.WebTransportProfileDraft07 {
		t.Fatalf("multiplexed endpoint = %+v", endpoint)
	}
}

func TestNativeServerAdmissionStatusPolicy(t *testing.T) {
	http3Server := NewServer()
	http3Options := http3Server.Options()
	http3Options.EnableHTTP3 = true
	http3Server.SetOptions(http3Options)
	http3Request := native.AdmissionRequest{
		Kind:   native.AdmissionHTTP3,
		Method: http.MethodPost,
		Path:   DefaultHTTP3Path,
		Headers: []native.AdmissionHeader{
			{Name: "Content-Type", Value: HTTP3ContentType},
		},
	}
	if status := nativeServerAdmissionHandler(http3Server)(http3Request); status != http.StatusOK {
		t.Fatalf("nil HTTP/3 admission status = %d", status)
	}

	webTransportRequest := native.AdmissionRequest{
		Kind:      native.AdmissionWebTransport,
		Method:    http.MethodConnect,
		Path:      DefaultHTTP3Path,
		Authority: "example.test",
		Origin:    "https://origin.test",
		Secure:    true,
	}
	newWebTransportHandler := func(
		admission WebTransportAdmission,
		maxConcurrent int,
	) native.AdmissionHandler {
		server := NewServer()
		options := server.Options()
		options.EnableWebTransport = true
		options.WebTransportAdmission = admission
		options.MaxConcurrentAdmissionCallbacks = maxConcurrent
		server.SetOptions(options)
		return nativeServerAdmissionHandler(server)
	}
	if status := newWebTransportHandler(nil, 0)(webTransportRequest); status != http.StatusForbidden {
		t.Fatalf("nil WebTransport admission status = %d", status)
	}

	admit := func(request WebTransportAdmissionRequest) bool {
		return request.Path == webTransportRequest.Path &&
			request.Authority == webTransportRequest.Authority &&
			request.Origin == webTransportRequest.Origin && request.Secure
	}
	if status := newWebTransportHandler(admit, 0)(webTransportRequest); status != http.StatusOK {
		t.Fatalf("accepted WebTransport admission status = %d", status)
	}
	if status := newWebTransportHandler(func(WebTransportAdmissionRequest) bool { return false }, 0)(webTransportRequest); status != http.StatusForbidden {
		t.Fatalf("denied WebTransport admission status = %d", status)
	}
	if status := newWebTransportHandler(func(WebTransportAdmissionRequest) bool { panic("boom") }, 0)(webTransportRequest); status != http.StatusInternalServerError {
		t.Fatalf("panicked WebTransport admission status = %d", status)
	}

	entered := make(chan struct{})
	release := make(chan struct{})
	handler := newWebTransportHandler(func(WebTransportAdmissionRequest) bool {
		close(entered)
		<-release
		return true
	}, 1)
	first := make(chan uint16, 1)
	go func() { first <- handler(webTransportRequest) }()
	select {
	case <-entered:
	case <-time.After(testTimeout):
		t.Fatal("admission callback did not start")
	}
	if status := handler(webTransportRequest); status != http.StatusServiceUnavailable {
		t.Fatalf("saturated WebTransport admission status = %d", status)
	}
	close(release)
	select {
	case status := <-first:
		if status != http.StatusOK {
			t.Fatalf("first WebTransport admission status = %d", status)
		}
	case <-time.After(testTimeout):
		t.Fatal("first admission callback did not finish")
	}
}
