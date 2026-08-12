package trevrpc

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"
	"github.com/quic-go/quic-go/quicvarint"
)

func TestHTTP3DefaultsAndMediaTypeValidation(t *testing.T) {
	options := DefaultServerOptions()
	if options.EnableHTTP3 {
		t.Fatal("HTTP/3 RPC should be opt-in")
	}
	if options.HTTP3Path != DefaultHTTP3Path || http3Path(ServerOptions{}) != DefaultHTTP3Path {
		t.Fatalf("unexpected default HTTP/3 path: options=%q zero-value=%q", options.HTTP3Path, http3Path(ServerOptions{}))
	}
	if path := http3Path(ServerOptions{HTTP3Path: "/rpc"}); path != "/rpc" {
		t.Fatalf("configured HTTP/3 path = %q", path)
	}
	options.EnableHTTP3 = true
	config := QUICServerConfig(options, nil)
	if config.MaxIncomingUniStreams != 0 || config.EnableDatagrams {
		t.Fatalf("HTTP/3 QUIC config has uni=%d datagrams=%t", config.MaxIncomingUniStreams, config.EnableDatagrams)
	}

	for _, value := range []string{HTTP3ContentType, "Application/TrevRPC"} {
		if !isTrevRPCMediaType([]string{value}) {
			t.Errorf("expected media type %q to be accepted", value)
		}
	}
	for _, values := range [][]string{
		nil,
		{""},
		{"application/octet-stream"},
		{"application/trevrpc; charset=utf-8"},
		{"application/trevrpc; version=1"},
		{HTTP3ContentType, HTTP3ContentType},
	} {
		if isTrevRPCMediaType(values) {
			t.Errorf("expected media type values %#v to be rejected", values)
		}
	}
}

func TestWebTransportServerAdvertisesSingleSessionWithoutFlowControl(t *testing.T) {
	running := startTestWebTransportServer(t, func(*Server) {})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, err := quic.DialAddr(ctx, running.addr, running.clientTLS.Clone(), &quic.Config{
		EnableDatagrams:                  true,
		EnableStreamResetPartialDelivery: true,
	})
	if err != nil {
		t.Fatalf("dial WebTransport server: %v", err)
	}
	defer conn.CloseWithError(0, "test complete")

	settings := []struct {
		id    uint64
		value uint64
	}{
		{id: 0x33, value: 1},
		{id: 0x2c7cf000, value: 1},
		{id: 0x2b61, value: 1},
		{id: 0x2b64, value: 1},
		{id: 0x2b65, value: 1},
	}
	payload := make([]byte, 0, 64)
	for _, setting := range settings {
		payload = quicvarint.Append(payload, setting.id)
		payload = quicvarint.Append(payload, setting.value)
	}
	control := quicvarint.Append(nil, 0)
	control = quicvarint.Append(control, 4)
	control = quicvarint.Append(control, uint64(len(payload)))
	control = append(control, payload...)
	clientControl, err := conn.OpenUniStreamSync(ctx)
	if err != nil {
		t.Fatalf("open client control stream: %v", err)
	}
	if _, err := clientControl.Write(control); err != nil {
		t.Fatalf("write client SETTINGS: %v", err)
	}
	if err := clientControl.Close(); err != nil {
		t.Fatalf("close client control stream: %v", err)
	}

	serverControl, err := conn.AcceptUniStream(ctx)
	if err != nil {
		t.Fatalf("accept server control stream: %v", err)
	}
	reader := bufio.NewReader(serverControl)
	streamType, err := quicvarint.Read(reader)
	if err != nil {
		t.Fatalf("read server control stream type: %v", err)
	}
	if streamType != 0 {
		t.Fatalf("server first unidirectional stream type = %#x, want control stream", streamType)
	}
	frameType, err := quicvarint.Read(reader)
	if err != nil {
		t.Fatalf("read server SETTINGS frame type: %v", err)
	}
	if frameType != 4 {
		t.Fatalf("server first control frame type = %#x, want SETTINGS", frameType)
	}
	frameLength, err := quicvarint.Read(reader)
	if err != nil {
		t.Fatalf("read server SETTINGS frame length: %v", err)
	}
	serverPayload := make([]byte, frameLength)
	if _, err := io.ReadFull(reader, serverPayload); err != nil {
		t.Fatalf("read server SETTINGS payload: %v", err)
	}
	serverReader := bytes.NewReader(serverPayload)
	serverSettings := make(map[uint64]uint64)
	for serverReader.Len() > 0 {
		id, err := quicvarint.Read(serverReader)
		if err != nil {
			t.Fatalf("read server setting identifier: %v", err)
		}
		value, err := quicvarint.Read(serverReader)
		if err != nil {
			t.Fatalf("read server setting value: %v", err)
		}
		serverSettings[id] = value
	}
	if value := serverSettings[0x14e9cd29]; value != 1 {
		t.Fatalf("server SETTINGS_WT_MAX_SESSIONS = %d, want 1", value)
	}
	for _, setting := range []uint64{0x2b61, 0x2b64, 0x2b65} {
		if value, ok := serverSettings[setting]; ok {
			t.Fatalf("server SETTINGS %#x = %d, want omitted", setting, value)
		}
	}
}

func TestHTTP3RoundTripsUnaryAndAllStreamingModes(t *testing.T) {
	running := startTestHTTP3Server(t, false, func(server *Server) {
		server.SetAuthorizer(BearerAuthorizer(testAuthToken))
	})
	transport := connectTestHTTP3Client(t, running)

	for index := range 4 {
		if err := runMixedQUICCall(transport, index); err != nil {
			t.Fatal(err)
		}
	}
}

func TestHTTP3RejectsInvalidRequestsBeforeRPCHandling(t *testing.T) {
	metrics := &recordingMetrics{}
	running := startTestHTTP3Server(t, false, func(server *Server) {
		server.SetMetrics(metrics)
	})
	client := newTestHTTP3HTTPClient(t, running)
	body := encodeTestHTTP3Request(t, NewRpcRequest(testServiceName, "SayHello", nil))

	tests := []struct {
		name           string
		method         string
		path           string
		contentType    string
		addContentType string
		wantStatus     int
	}{
		{name: "path", method: http.MethodPost, path: "/wrong", contentType: HTTP3ContentType, wantStatus: http.StatusNotFound},
		{name: "method", method: http.MethodGet, path: DefaultHTTP3Path, contentType: HTTP3ContentType, wantStatus: http.StatusMethodNotAllowed},
		{name: "missing media type", method: http.MethodPost, path: DefaultHTTP3Path, wantStatus: http.StatusUnsupportedMediaType},
		{name: "wrong media type", method: http.MethodPost, path: DefaultHTTP3Path, contentType: "application/octet-stream", wantStatus: http.StatusUnsupportedMediaType},
		{name: "media type parameter", method: http.MethodPost, path: DefaultHTTP3Path, contentType: HTTP3ContentType + "; charset=utf-8", wantStatus: http.StatusUnsupportedMediaType},
		{name: "multiple media type values", method: http.MethodPost, path: DefaultHTTP3Path, contentType: HTTP3ContentType, addContentType: HTTP3ContentType, wantStatus: http.StatusUnsupportedMediaType},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request, err := http.NewRequest(test.method, "https://"+running.addr+test.path, bytes.NewReader(body))
			if err != nil {
				t.Fatalf("create request: %v", err)
			}
			if test.contentType != "" {
				request.Header.Set("Content-Type", test.contentType)
			}
			if test.addContentType != "" {
				request.Header.Add("Content-Type", test.addContentType)
			}

			response, err := client.Do(request)
			if err != nil {
				t.Fatalf("send request: %v", err)
			}
			defer response.Body.Close()
			if response.StatusCode != test.wantStatus {
				t.Fatalf("status = %d, want %d", response.StatusCode, test.wantStatus)
			}
			if test.wantStatus == http.StatusMethodNotAllowed && response.Header.Get("Allow") != http.MethodPost {
				t.Fatalf("Allow = %q, want POST", response.Header.Get("Allow"))
			}
		})
	}

	started, _ := metrics.snapshot()
	if len(started) != 0 {
		t.Fatalf("invalid HTTP requests reached RPC handling: %#v", started)
	}
}

func TestHTTP3AdmissionReceivesRequestAndCanReject(t *testing.T) {
	seen := make(chan HTTP3AdmissionRequest, 2)
	running := startTestHTTP3Server(t, false, func(server *Server) {
		options := server.Options()
		options.HTTP3Admission = func(request HTTP3AdmissionRequest) bool {
			seen <- request
			return request.Request.Header.Get("X-Allow") == "yes"
		}
		server.SetOptions(options)
	})
	client := newTestHTTP3HTTPClient(t, running)
	body := encodeTestHTTP3Request(t, NewRpcRequest(testServiceName, "SayHello", nil))

	request, err := http.NewRequest(http.MethodPost, "https://"+running.addr+DefaultHTTP3Path, bytes.NewReader(body))
	if err != nil {
		t.Fatalf("create denied request: %v", err)
	}
	request.Header.Set("Content-Type", HTTP3ContentType)
	response, err := client.Do(request)
	if err != nil {
		t.Fatalf("send denied request: %v", err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusForbidden {
		t.Fatalf("denied status = %d, want %d", response.StatusCode, http.StatusForbidden)
	}

	request, err = http.NewRequest(http.MethodPost, "https://"+running.addr+DefaultHTTP3Path, bytes.NewReader(body))
	if err != nil {
		t.Fatalf("create admitted request: %v", err)
	}
	request.Header.Set("Content-Type", HTTP3ContentType)
	request.Header.Set("X-Allow", "yes")
	response, err = client.Do(request)
	if err != nil {
		t.Fatalf("send admitted request: %v", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("admitted status = %d, want %d", response.StatusCode, http.StatusOK)
	}
	if response.Header.Get("Content-Type") != HTTP3ContentType {
		t.Fatalf("response content type = %q", response.Header.Get("Content-Type"))
	}

	for range 2 {
		admission := <-seen
		if admission.Request == nil || admission.Path != DefaultHTTP3Path || admission.Method != http.MethodPost || admission.Authority == "" || !admission.Secure {
			t.Fatalf("incomplete admission request: %#v", admission)
		}
	}
}

func TestServerAdmissionCallbacksUseSnapshotsAndAreBounded(t *testing.T) {
	for _, test := range []struct {
		name      string
		configure func(*Server, chan struct{}, chan struct{})
		invoke    func(*serverRuntime, *http.Request) (bool, error)
	}{
		{name: "http3", configure: func(server *Server, entered, release chan struct{}) {
			server.SetOptions(ServerOptions{MaxConcurrentAdmissionCallbacks: 1, HTTP3Admission: func(request HTTP3AdmissionRequest) bool {
				request.Request.Header.Set("X-Mutated", "yes")
				request.Request.URL.Path = "/mutated"
				close(entered)
				<-release
				return true
			}})
		}, invoke: func(runtime *serverRuntime, request *http.Request) (bool, error) {
			return runtime.http3Admitted(request)
		}},
		{name: "webtransport", configure: func(server *Server, entered, release chan struct{}) {
			server.SetOptions(ServerOptions{MaxConcurrentAdmissionCallbacks: 1, WebTransportAdmission: func(request WebTransportAdmissionRequest) bool {
				request.Request.Header.Set("X-Mutated", "yes")
				request.Request.URL.Path = "/mutated"
				close(entered)
				<-release
				return true
			}})
		}, invoke: func(runtime *serverRuntime, request *http.Request) (bool, error) {
			return runtime.webTransportAdmitted(request)
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			server := NewServer()
			entered := make(chan struct{})
			release := make(chan struct{})
			test.configure(server, entered, release)
			runtime := server.freeze()
			request := httptest.NewRequest(http.MethodPost, "https://example.test/trevrpc", nil)
			request.Header.Set("X-Original", "yes")
			first := make(chan error, 1)
			go func() { _, err := test.invoke(runtime, request); first <- err }()
			select {
			case <-entered:
			case <-time.After(testTimeout):
				t.Fatal("admission callback did not start")
			}
			if admitted, err := test.invoke(runtime, request); admitted || err != errAdmissionSaturated {
				t.Fatalf("saturated admission = %t, %v", admitted, err)
			}
			if request.Header.Get("X-Mutated") != "" || request.URL.Path != "/trevrpc" {
				t.Fatalf("admission callback mutated original request: %s %#v", request.URL.Path, request.Header)
			}
			close(release)
			select {
			case err := <-first:
				if err != nil {
					t.Fatalf("first admission: %v", err)
				}
			case <-time.After(testTimeout):
				t.Fatal("first admission did not finish")
			}
		})
	}
}

func TestHTTP3AdmissionPanicIsGenericAndFutureRequestsContinue(t *testing.T) {
	var calls atomic.Int64
	running := startTestHTTP3Server(t, false, func(server *Server) {
		options := server.Options()
		options.HTTP3Admission = func(HTTP3AdmissionRequest) bool {
			if calls.Add(1) == 1 {
				panic("secret admission panic")
			}
			return true
		}
		server.SetOptions(options)
	})
	client := newTestHTTP3HTTPClient(t, running)
	body := encodeTestHTTP3Request(t, NewRpcRequest(testServiceName, "SayHello", mustEncodeTestMessage(t, "after panic")))
	request, err := http.NewRequest(http.MethodPost, "https://"+running.addr+DefaultHTTP3Path, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", HTTP3ContentType)
	response, err := client.Do(request)
	if err != nil {
		t.Fatalf("panic request: %v", err)
	}
	failureBody, _ := io.ReadAll(response.Body)
	response.Body.Close()
	if response.StatusCode != http.StatusInternalServerError || string(failureBody) != "internal server error\n" {
		t.Fatalf("admission panic leaked remotely: status=%d body=%q", response.StatusCode, failureBody)
	}

	request, err = http.NewRequest(http.MethodPost, "https://"+running.addr+DefaultHTTP3Path, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", HTTP3ContentType)
	response, err = client.Do(request)
	if err != nil {
		t.Fatalf("request after admission panic: %v", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("status after admission panic = %d", response.StatusCode)
	}
}

func TestAdmissionCallbackPanicsAreContainedForBothTransports(t *testing.T) {
	for _, test := range []struct {
		name string
		set  func(*Server)
		call func(*serverRuntime, *http.Request) (bool, error)
	}{
		{name: "http3", set: func(server *Server) {
			server.SetOptions(ServerOptions{HTTP3Admission: func(HTTP3AdmissionRequest) bool { panic("boom") }})
		}, call: func(runtime *serverRuntime, request *http.Request) (bool, error) {
			return runtime.http3Admitted(request)
		}},
		{name: "webtransport", set: func(server *Server) {
			server.SetOptions(ServerOptions{WebTransportAdmission: func(WebTransportAdmissionRequest) bool { panic("boom") }})
		}, call: func(runtime *serverRuntime, request *http.Request) (bool, error) {
			return runtime.webTransportAdmitted(request)
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			server := NewServer()
			test.set(server)
			admitted, err := test.call(server.freeze(), httptest.NewRequest(http.MethodPost, "https://example.test/trevrpc", nil))
			if admitted || err == nil {
				t.Fatalf("panic admission = %t, %v", admitted, err)
			}
		})
	}
}

func TestHTTP3CoexistsWithWebTransport(t *testing.T) {
	running := startTestHTTP3Server(t, true, func(server *Server) {
		server.SetAuthorizer(BearerAuthorizer(testAuthToken))
	})
	httpTransport := connectTestHTTP3Client(t, running)
	if err := runMixedQUICCall(httpTransport, 0); err != nil {
		t.Fatalf("HTTP/3 RPC: %v", err)
	}

	webTransport := connectTestWebTransportClient(t, running)
	defer webTransport.Session().CloseWithError(cancelledWebTransportSessionCode, "test complete")
	if err := runMixedQUICCall(webTransport, 1); err != nil {
		t.Fatalf("WebTransport RPC: %v", err)
	}
}

func TestHTTP3ShutdownWithPartialPOSTAndActiveWebTransport(t *testing.T) {
	const shutdownTimeout = 100 * time.Millisecond
	postStarted := make(chan struct{})
	postDone := make(chan struct{})
	running := startTestHTTP3Server(t, true, func(server *Server) {
		registerHTTP3PendingResponseRoute(server)
		server.RouteStreaming(testServiceName, "PartialUpload", RpcKindClientStreaming, func(_ context.Context, _ []byte, requests ByteStream) (ByteStream, error) {
			close(postStarted)
			_, err := requests.Recv()
			close(postDone)
			return nil, err
		})
		options := server.Options()
		options.GracefulShutdownTimeout = shutdownTimeout
		server.SetOptions(options)
		server.SetAuthorizer(BearerAuthorizer(testAuthToken))
	})

	webTransport := connectTestWebTransportClient(t, running)
	webTransportResponses := holdServerStreamOpen(t, webTransport)

	httpClient := newTestHTTP3HTTPClient(t, running)
	bodyReader, bodyWriter := io.Pipe()
	requestContext, cancelRequest := context.WithCancel(context.Background())
	defer cancelRequest()
	request, err := http.NewRequestWithContext(requestContext, http.MethodPost, "https://"+running.addr+DefaultHTTP3Path, bodyReader)
	if err != nil {
		t.Fatalf("create partial request: %v", err)
	}
	request.Header.Set("Content-Type", HTTP3ContentType)

	responseResult := make(chan struct {
		response *http.Response
		err      error
	}, 1)
	go func() {
		response, err := httpClient.Do(request)
		responseResult <- struct {
			response *http.Response
			err      error
		}{response: response, err: err}
	}()

	partialWritten := make(chan error, 1)
	go func() {
		rpcRequest := NewRpcRequest(testServiceName, "PartialUpload", nil)
		rpcRequest.Kind = RpcKindClientStreaming
		rpcRequest.Metadata["authorization"] = []byte("Bearer " + testAuthToken)
		if err := WriteFrame(bodyWriter, rpcRequest, DefaultMaxFrameSize); err != nil {
			partialWritten <- err
			return
		}
		_, err := bodyWriter.Write([]byte{0, 0})
		partialWritten <- err
	}()

	if err := waitTestHTTP3Result(t, partialWritten, "write partial request body"); err != nil {
		t.Fatalf("write partial request body: %v", err)
	}
	waitTestHTTP3Signal(t, postStarted, "partial POST handler did not start")

	var response *http.Response
	select {
	case result := <-responseResult:
		if result.err != nil {
			t.Fatalf("start partial POST: %v", result.err)
		}
		response = result.response
		if response.StatusCode != http.StatusOK {
			t.Fatalf("partial POST status = %d, want %d", response.StatusCode, http.StatusOK)
		}
	case <-time.After(testTimeout):
		t.Fatal("partial POST did not receive response headers")
	}

	startedAt := time.Now()
	running.stop(t)
	if elapsed := time.Since(startedAt); elapsed > shutdownTimeout+500*time.Millisecond {
		t.Fatalf("shutdown took %s with timeout %s", elapsed, shutdownTimeout)
	}

	waitTestHTTP3Signal(t, postDone, "partial POST handler did not stop")
	waitTestHTTP3Signal(t, webTransport.Session().Context().Done(), "WebTransport session did not close")
	cancelRequest()
	_ = bodyWriter.Close()
	_ = bodyReader.Close()
	_ = response.Body.Close()
	_ = webTransportResponses.Close()
}

func TestHTTP3CancellationDeadlineAndRequestLimits(t *testing.T) {
	t.Run("cancellation", func(t *testing.T) {
		metrics := &recordingMetrics{}
		running := startTestHTTP3Server(t, false, func(server *Server) {
			registerHTTP3PendingResponseRoute(server)
			server.SetMetrics(metrics)
			server.SetAuthorizer(BearerAuthorizer(testAuthToken))
		})
		responses := holdServerStreamOpen(t, connectTestHTTP3Client(t, running))
		if err := responses.Close(); err != nil {
			t.Fatalf("close response stream: %v", err)
		}
		waitForMetricCode(t, metrics, CodeCancelled)
	})

	t.Run("deadline", func(t *testing.T) {
		running := startTestHTTP3Server(t, false, func(server *Server) {
			registerHTTP3PendingResponseRoute(server)
			server.SetAuthorizer(BearerAuthorizer(testAuthToken))
		})
		responses, err := ServerStreaming(context.Background(), connectTestHTTP3Client(t, running), testServiceName, "LotsOfReplies", &testMessage{Value: "cancel"}, func() *testMessage { return &testMessage{} }, append(authenticatedOptions(), WithTimeout(500*time.Millisecond))...)
		if err != nil {
			t.Fatalf("start deadline stream: %v", err)
		}
		defer responses.Close()
		if _, err := responses.Recv(); err != nil {
			t.Fatalf("receive first response: %v", err)
		}
		if _, err := responses.Recv(); StatusFromError(err).Code != CodeDeadlineExceeded {
			t.Fatalf("deadline status = %v, want %v", err, CodeDeadlineExceeded)
		}
	})

	t.Run("request message limit", func(t *testing.T) {
		running := startTestHTTP3Server(t, false, func(server *Server) {
			options := server.Options()
			options.MaxStreamMessages = 1
			server.SetOptions(options)
			server.SetAuthorizer(BearerAuthorizer(testAuthToken))
		})
		_, err := runTestClientStreaming(context.Background(), connectTestHTTP3Client(t, running), testServiceName, "LotsOfGreetings", []string{"one", "two"}, authenticatedOptions()...)
		if StatusFromError(err).Code != CodeResourceExhausted {
			t.Fatalf("limit status = %v, want %v", err, CodeResourceExhausted)
		}
	})

	t.Run("connection stream limit", func(t *testing.T) {
		running := startTestHTTP3Server(t, false, func(server *Server) {
			registerHTTP3PendingResponseRoute(server)
			options := server.Options()
			options.MaxConcurrentStreamsPerConnection = 1
			server.SetOptions(options)
			server.SetAuthorizer(BearerAuthorizer(testAuthToken))
		})
		transport := connectTestHTTP3Client(t, running)
		responses := holdServerStreamOpen(t, transport)
		defer responses.Close()

		request := NewRpcRequest(testServiceName, "SayHello", mustEncodeTestMessage(t, "second"))
		request.Metadata = Metadata{"authorization": []byte("Bearer " + testAuthToken)}
		body := encodeTestHTTP3Request(t, request)
		httpRequest, err := http.NewRequest(http.MethodPost, transport.url, bytes.NewReader(body))
		if err != nil {
			t.Fatalf("create over-limit request: %v", err)
		}
		httpRequest.Header.Set("Content-Type", HTTP3ContentType)
		response, err := transport.client.Do(httpRequest)
		if err != nil {
			t.Fatalf("send over-limit request: %v", err)
		}
		defer response.Body.Close()
		if response.StatusCode != http.StatusServiceUnavailable {
			t.Fatalf("stream limit HTTP status = %d, want %d", response.StatusCode, http.StatusServiceUnavailable)
		}
	})
}

func mustEncodeTestMessage(t *testing.T, value string) []byte {
	t.Helper()
	body, err := MarshalMessage(&testMessage{Value: value})
	if err != nil {
		t.Fatalf("encode test message: %v", err)
	}
	return body
}

func startTestHTTP3Server(t *testing.T, enableWebTransport bool, configure func(*Server)) *runningTestQUICServer {
	t.Helper()
	serverTLS, clientTLS := testTLSConfig(t)
	serverTLS.NextProtos = []string{http3.NextProtoH3}
	clientTLS.NextProtos = []string{http3.NextProtoH3}
	return startTestQUICServerWithTLS(t, serverTLS, clientTLS, func(server *Server) {
		configure(server)
		options := server.Options()
		options.EnableHTTP3 = true
		options.EnableWebTransport = enableWebTransport
		if enableWebTransport {
			options.WebTransportAdmission = func(request WebTransportAdmissionRequest) bool {
				return request.Path == DefaultHTTP3Path
			}
		}
		server.SetOptions(options)
	})
}

func newTestHTTP3HTTPClient(t *testing.T, running *runningTestQUICServer) *http.Client {
	t.Helper()
	roundTripper := &http3.Transport{
		TLSClientConfig: running.clientTLS.Clone(),
		QUICConfig:      &quic.Config{},
	}
	t.Cleanup(func() { _ = roundTripper.Close() })
	return &http.Client{Transport: roundTripper}
}

func connectTestHTTP3Client(t *testing.T, running *runningTestQUICServer) *testHTTP3Transport {
	t.Helper()
	return &testHTTP3Transport{
		client:       newTestHTTP3HTTPClient(t, running),
		url:          "https://" + running.addr + DefaultHTTP3Path,
		maxFrameSize: DefaultMaxFrameSize,
	}
}

type testHTTP3Transport struct {
	client       *http.Client
	url          string
	maxFrameSize int
}

func (t *testHTTP3Transport) Call(ctx context.Context, request *RpcRequest) (*RpcResponse, error) {
	body, err := EncodeFrame(request, t.maxFrameSize)
	if err != nil {
		return nil, err
	}
	response, err := t.post(ctx, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()

	rpcResponse := &RpcResponse{}
	if err := ReadFrame(response.Body, rpcResponse, t.maxFrameSize); err != nil {
		return nil, err
	}
	return rpcResponse, nil
}

func (t *testHTTP3Transport) StreamingCall(ctx context.Context, request *RpcRequest, requestBody ByteStream) (FrameStream, error) {
	streamCtx, cancel := context.WithCancel(ctx)
	reader, writer := io.Pipe()
	writerDone := make(chan error, 1)
	go func() {
		err := writeTestHTTP3StreamingRequest(streamCtx, writer, request, requestBody, t.maxFrameSize)
		if err != nil {
			_ = writer.CloseWithError(err)
		} else {
			_ = writer.Close()
		}
		writerDone <- err
	}()

	response, err := t.post(streamCtx, reader)
	if err != nil {
		cancel()
		_ = reader.CloseWithError(err)
		return nil, err
	}
	return &testHTTP3ResponseStream{body: response.Body, writerDone: writerDone, cancel: cancel, maxFrameSize: t.maxFrameSize}, nil
}

func (t *testHTTP3Transport) post(ctx context.Context, body io.Reader) (*http.Response, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, t.url, body)
	if err != nil {
		return nil, err
	}
	request.Header.Set("Content-Type", HTTP3ContentType)
	response, err := t.client.Do(request)
	if err != nil {
		return nil, err
	}
	if response.StatusCode != http.StatusOK {
		response.Body.Close()
		return nil, fmt.Errorf("HTTP/3 status %d", response.StatusCode)
	}
	if response.Header.Get("Content-Type") != HTTP3ContentType {
		response.Body.Close()
		return nil, fmt.Errorf("HTTP/3 content type %q", response.Header.Get("Content-Type"))
	}
	return response, nil
}

func writeTestHTTP3StreamingRequest(ctx context.Context, writer io.Writer, request *RpcRequest, requestBody ByteStream, maxFrameSize int) error {
	requestBody = closeStreamOnContext(ctx, requestBody)
	defer closeMessageStream(requestBody)
	if err := WriteFrame(writer, request, maxFrameSize); err != nil {
		return err
	}
	return writeRequestBodyFrames(ctx, writer, requestBody, maxFrameSize)
}

type testHTTP3ResponseStream struct {
	body         io.ReadCloser
	writerDone   <-chan error
	cancel       context.CancelFunc
	maxFrameSize int
	done         atomic.Bool
	finishOnce   sync.Once
}

func (s *testHTTP3ResponseStream) trevrpcContextCancelsRecv() bool { return true }

func (s *testHTTP3ResponseStream) Recv() (*RpcStreamFrame, error) {
	if s.done.Load() {
		return nil, io.EOF
	}
	frame := &RpcStreamFrame{}
	read, err := ReadFrameOrEOF(s.body, frame, s.maxFrameSize)
	if err != nil {
		s.finish()
		return nil, err
	}
	if !read {
		s.finish()
		return nil, io.EOF
	}
	if frame.Kind == RpcStreamFrameKindStatus {
		s.done.Store(true)
		s.finish()
	}
	return frame, nil
}

func (s *testHTTP3ResponseStream) Close() error {
	s.done.Store(true)
	s.finish()
	return nil
}

func (s *testHTTP3ResponseStream) finish() {
	s.finishOnce.Do(func() {
		s.cancel()
		_ = s.body.Close()
		<-s.writerDone
	})
}

func registerHTTP3PendingResponseRoute(server *Server) {
	server.RouteStreaming(testServiceName, "LotsOfReplies", RpcKindServerStreaming, func(ctx context.Context, _ []byte, _ ByteStream) (ByteStream, error) {
		responses := NewMessagePipe[*testMessage](ctx)
		go func() { _ = responses.Send(&testMessage{Value: "first"}) }()
		return EncodeStream(responses), nil
	})
}

func waitTestHTTP3Signal(t *testing.T, signal <-chan struct{}, message string) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(testTimeout):
		t.Fatal(message)
	}
}

func waitTestHTTP3Result(t *testing.T, result <-chan error, message string) error {
	t.Helper()
	select {
	case err := <-result:
		return err
	case <-time.After(testTimeout):
		t.Fatal(message)
		return nil
	}
}

func encodeTestHTTP3Request(t *testing.T, request *RpcRequest) []byte {
	t.Helper()
	body, err := EncodeFrame(request, DefaultMaxFrameSize)
	if err != nil {
		t.Fatalf("encode request: %v", err)
	}
	return body
}

var _ Transport = (*testHTTP3Transport)(nil)
