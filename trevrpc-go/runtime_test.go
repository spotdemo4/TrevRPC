package trevrpc

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type testMessage struct {
	Value string `protobuf:"bytes,1,opt,name=value,proto3" json:"value,omitempty"`
}

func (m *testMessage) Reset()         { *m = testMessage{} }
func (m *testMessage) String() string { return m.Value }
func (*testMessage) ProtoMessage()    {}

type localTransport struct {
	server *Server
}

const (
	testAuthToken = "integration-token"
	testTimeout   = 2 * time.Second
)

func (t localTransport) call(ctx context.Context, request *wireRequest) (*wireResponse, error) {
	return t.server.handleRequest(ctx, request), nil
}

func (t localTransport) streamingCall(ctx context.Context, request *wireRequest, requestBody byteStream) (frameStream, error) {
	responses := make(chan frameStream, 1)
	go func() {
		responses <- t.server.handleStreamingRequest(ctx, request, requestBody)
	}()
	return &localFrameStream{responses: responses}, nil
}

type localFrameStream struct {
	responses <-chan frameStream
	once      sync.Once
	stream    frameStream
}

func (s *localFrameStream) Recv() (*wireStreamFrame, error) {
	return s.responseStream().Recv()
}

func (s *localFrameStream) Close() error {
	if s.stream == nil {
		return nil
	}
	return s.stream.Close()
}

func (s *localFrameStream) responseStream() frameStream {
	s.once.Do(func() {
		s.stream = <-s.responses
	})
	return s.stream
}

func TestFrameRoundTrip(t *testing.T) {
	request := newWireRequest("example.Greeter", "SayHello", []byte("trev"))
	request.Metadata["authorization"] = []byte("ok")

	frame, err := encodeFrame(request, DefaultMaxFrameSize)
	if err != nil {
		t.Fatalf("encode frame: %v", err)
	}

	decoded := &wireRequest{}
	if err := readFrame(bytes.NewReader(frame), decoded, DefaultMaxFrameSize); err != nil {
		t.Fatalf("decode frame: %v", err)
	}

	if decoded.Service != request.Service || decoded.Method != request.Method || !bytes.Equal(decoded.Body, request.Body) {
		t.Fatalf("decoded request mismatch: %#v", decoded)
	}
}

func TestFrameDecodeErrorMapsToInvalidArgument(t *testing.T) {
	request := &wireRequest{}
	err := decodeFrame([]byte{0xff, 0xff}, request)

	if status := StatusFromError(err); status.Code != CodeInvalidArgument {
		t.Fatalf("expected invalid argument for malformed protobuf frame, got %v", status)
	}
	if status := StatusFromError(transportStatus(err)); status.Code != CodeInvalidArgument {
		t.Fatalf("expected transport invalid argument for malformed protobuf frame, got %v", status)
	}
}

func TestTypedProtobufPreflightRejectsKnownWireTypesAndAllowsUnknownFields(t *testing.T) {
	if err := unmarshalMessage([]byte{0x0a, 0x00}, &wireResponse{}); err == nil {
		t.Fatal("known status field with a length-delimited wire type was accepted")
	}

	for _, body := range [][]byte{
		{0x40, 0x01},
		{0x49, 1, 2, 3, 4, 5, 6, 7, 8},
		{0x52, 0x01, 0xff},
		{0x5d, 1, 2, 3, 4},
		{0x63, 0x08, 0x01, 0x64},
	} {
		if err := unmarshalMessage(body, &wireResponse{}); err != nil {
			t.Fatalf("valid unknown field %x was rejected: %v", body, err)
		}
	}
}

func TestReadFrameLargePartialBodyDoesNotAllocateAdvertisedLength(t *testing.T) {
	const length = 1 << 30
	header := make([]byte, 4)
	binary.BigEndian.PutUint32(header, length)

	read, err := readFrameOrEOF(bytes.NewReader(header), &wireRequest{}, length)
	if read {
		t.Fatal("incomplete frame should not be reported as read")
	}
	if !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("expected EOF for incomplete large frame body, got %v", err)
	}
}

func TestMetadataValidation(t *testing.T) {
	if err := validateMetadata(Metadata{"authorization": []byte("ok")}); err != nil {
		t.Fatalf("valid metadata was rejected: %v", err)
	}

	if err := validateMetadata(Metadata{"Authorization": []byte("ok")}); err == nil {
		t.Fatal("uppercase metadata key should be rejected")
	}

	if NormalizeMetadataKey("Authorization") != "authorization" {
		t.Fatal("metadata key was not normalized")
	}

	for _, key := range []string{"", "trevrpc-timeout", "bad key"} {
		if err := validateMetadata(Metadata{key: []byte("ok")}); err == nil {
			t.Fatalf("metadata key %q should be rejected", key)
		}
	}

	tooManyEntries := Metadata{}
	for i := 0; i <= maxMetadataEntries; i++ {
		tooManyEntries[fmt.Sprintf("key-%d", i)] = nil
	}
	if err := validateMetadata(tooManyEntries); err == nil {
		t.Fatal("metadata entry limit should be rejected")
	}

	if err := validateMetadata(Metadata{"key": make([]byte, maxMetadataValueLen+1)}); err == nil {
		t.Fatal("metadata value limit should be rejected")
	}

	if err := validateMetadata(Metadata{"key": make([]byte, maxMetadataTotalSize+1)}); err == nil {
		t.Fatal("metadata total size limit should be rejected")
	}
}

func TestWireProtocolValidation(t *testing.T) {
	request := newWireRequest("service", "method", nil)
	if request.rpcKind() != RPCKindUnary {
		t.Fatalf("expected unary default kind, got %d", request.rpcKind())
	}
	if request.Version != wireVersion {
		t.Fatalf("expected wire version %d, got %d", wireVersion, request.Version)
	}

	request.Version = wireVersion + 1
	if status := StatusFromError(request.validateProtocol()); status.Code != CodeFailedPrecondition {
		t.Fatalf("expected failed precondition, got %v", status)
	}

	request.Version = wireVersion
	request.Kind = RPCKind(99)
	if status := StatusFromError(request.validateProtocol()); status.Code != CodeInvalidArgument {
		t.Fatalf("expected invalid argument for unknown RPC kind, got %v", status)
	}

	frame := statusFrame(Unavailable("retry later"))
	if kind, ok := frame.frameKind(); !ok || kind != wireFrameStatus {
		t.Fatalf("expected status frame kind, got %d %t", kind, ok)
	}
	if status := frame.statusValue(); status.Code != CodeUnavailable || status.Message != "retry later" {
		t.Fatalf("unexpected status value: %v", status)
	}
}

func TestUnaryClientServer(t *testing.T) {
	server := NewServer()
	server.SetAuthorizer(NewMetadataValueAuthorizer("Authorization", []byte("ok")))
	RegisterUnary(server, "example.Greeter", "SayHello", func() *testMessage { return &testMessage{} }, func(_ context.Context, request *testMessage) (*Response[*testMessage], error) {
		return testResponse(&testMessage{Value: "hello " + request.Value}), nil
	})

	response, err := Unary(context.Background(), localTransport{server: server}, "example.Greeter", "SayHello", &testMessage{Value: "TrevRPC"}, func() *testMessage { return &testMessage{} }, WithMetadata("Authorization", []byte("ok")), WithTimeout(time.Second))
	if err != nil {
		t.Fatalf("unary RPC failed: %v", err)
	}

	if response.Value != "hello TrevRPC" {
		t.Fatalf("unexpected response: %q", response.Value)
	}

	_, err = Unary(context.Background(), localTransport{server: server}, "example.Greeter", "SayHello", &testMessage{Value: "TrevRPC"}, func() *testMessage { return &testMessage{} })
	status := StatusFromError(err)
	if status.Code != CodeUnauthenticated {
		t.Fatalf("expected unauthenticated status, got %v", err)
	}
}

func TestStreamingClientServer(t *testing.T) {
	server := NewServer()
	server.routeStreaming("example.Greeter", "LotsOfReplies", RPCKindServerStreaming, func(_ context.Context, body []byte, _ byteStream) (byteStream, error) {
		request := &testMessage{}
		if err := unmarshalMessage(body, request); err != nil {
			return nil, err
		}

		return newEncodeStream(newSliceStream(
			&testMessage{Value: request.Value + " 1"},
			&testMessage{Value: request.Value + " 2"},
		)), nil
	})

	stream, err := ServerStreaming(context.Background(), localTransport{server: server}, "example.Greeter", "LotsOfReplies", &testMessage{Value: "hello"}, func() *testMessage { return &testMessage{} })
	if err != nil {
		t.Fatalf("server streaming RPC failed: %v", err)
	}

	first, err := stream.Recv()
	if err != nil {
		t.Fatalf("first response failed: %v", err)
	}
	second, err := stream.Recv()
	if err != nil {
		t.Fatalf("second response failed: %v", err)
	}
	_, err = stream.Recv()
	if !errors.Is(err, io.EOF) {
		t.Fatalf("expected final EOF, got %v", err)
	}

	if first.Value != "hello 1" || second.Value != "hello 2" {
		t.Fatalf("unexpected stream responses: %q %q", first.Value, second.Value)
	}
}

func TestClientStreamingClientServer(t *testing.T) {
	server := NewServer()
	server.routeStreaming("example.Greeter", "LotsOfGreetings", RPCKindClientStreaming, func(_ context.Context, _ []byte, requests byteStream) (byteStream, error) {
		var combined strings.Builder
		for {
			body, err := requests.Recv()
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				return nil, err
			}

			request := &testMessage{}
			if err := unmarshalMessage(body, request); err != nil {
				return nil, err
			}

			combined.WriteString(request.Value)
		}

		return singleMessageStream(&testMessage{Value: combined.String()}), nil
	})

	response, err := runTestClientStreaming(context.Background(), localTransport{server: server}, "example.Greeter", "LotsOfGreetings", []string{"hello", " world"})
	if err != nil {
		t.Fatalf("client streaming RPC failed: %v", err)
	}

	if response.Value != "hello world" {
		t.Fatalf("unexpected response: %q", response.Value)
	}
}

func TestBidirectionalStreamingClientServer(t *testing.T) {
	server := NewServer()
	server.routeStreaming("example.Greeter", "BidiHello", RPCKindBidirectionalStreaming, func(_ context.Context, _ []byte, requests byteStream) (byteStream, error) {
		return newEncodeStream[*testMessage](&echoTestMessages{requests: newDecodeStream[*testMessage](requests, func() *testMessage { return &testMessage{} })}), nil
	})

	stream, err := runTestBidiStreaming(context.Background(), localTransport{server: server}, "example.Greeter", "BidiHello", []string{"left", "right"})
	if err != nil {
		t.Fatalf("bidi RPC failed: %v", err)
	}

	first, err := stream.Recv()
	if err != nil {
		t.Fatalf("first bidi response failed: %v", err)
	}
	second, err := stream.Recv()
	if err != nil {
		t.Fatalf("second bidi response failed: %v", err)
	}
	if _, err := stream.Recv(); !errors.Is(err, io.EOF) {
		t.Fatalf("expected final EOF, got %v", err)
	}

	if first.Value != "echo, left" || second.Value != "echo, right" {
		t.Fatalf("unexpected bidi responses: %q %q", first.Value, second.Value)
	}
}

func TestResponseStreamTerminalOKDoesNotHideCloseError(t *testing.T) {
	message := &testMessage{Value: "first"}
	body, err := marshalMessage(message)
	if err != nil {
		t.Fatalf("marshal message: %v", err)
	}
	closeErr := InvalidArgument("local upload failed")
	stream := newResponseMessageStream[*testMessage](
		context.Background(),
		&closeErrorFrameStream{
			frames: []*wireStreamFrame{messageFrame(body), statusFrame(OK())},
			err:    closeErr,
		},
		func() *testMessage { return &testMessage{} },
		defaultCallConfig(),
		func() {},
	)

	first, err := stream.Recv()
	if err != nil {
		t.Fatalf("first response failed: %v", err)
	}
	if first.Value != message.Value {
		t.Fatalf("unexpected first response: %q", first.Value)
	}

	_, err = stream.Recv()
	if !errors.Is(err, closeErr) {
		t.Fatalf("expected close error %v, got %v", closeErr, err)
	}
}

func TestResponseStreamTerminalErrorWinsOverCloseError(t *testing.T) {
	closeErr := InvalidArgument("local upload failed")
	stream := newResponseMessageStream[*testMessage](
		context.Background(),
		&closeErrorFrameStream{
			frames: []*wireStreamFrame{statusFrame(NewStatus(CodePermissionDenied, "remote rejected upload"))},
			err:    closeErr,
		},
		func() *testMessage { return &testMessage{} },
		defaultCallConfig(),
		func() {},
	)

	_, err := stream.Recv()
	if code := StatusFromError(err).Code; code != CodePermissionDenied {
		t.Fatalf("expected remote permission denied to win, got %v (%v)", code, err)
	}
}

func TestResponseStreamLocalCancelWinsOverReadyStreamError(t *testing.T) {
	errors := make(chan error, 1)
	errors <- InvalidArgument("response read failed")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	stream := newResponseMessageStream[*testMessage](
		ctx,
		uploadErrorFrameStream{errors: errors},
		func() *testMessage { return &testMessage{} },
		defaultCallConfig(),
		func() {},
	)

	_, err := stream.Recv()
	if code := StatusFromError(err).Code; code != CodeCancelled {
		t.Fatalf("expected cancelled to win over response read error, got %v (%v)", code, err)
	}
}

func TestResponseStreamLocalCancelWinsOverTerminalOKCloseError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	stream := newResponseMessageStream[*testMessage](
		ctx,
		&closeErrorFrameStream{
			frames: []*wireStreamFrame{statusFrame(OK())},
			err:    InvalidArgument("local close failed"),
		},
		func() *testMessage { return &testMessage{} },
		defaultCallConfig(),
		func() {},
	)

	_, err := stream.Recv()
	if code := StatusFromError(err).Code; code != CodeCancelled {
		t.Fatalf("expected cancelled to win over close error, got %v (%v)", code, err)
	}
}

func TestRecvRequestBodyLocalCancelWinsOverRequestError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := recvRequestBody(ctx, byteErrorStream{err: InvalidArgument("upload failed")})
	if code := StatusFromError(err).Code; code != CodeCancelled {
		t.Fatalf("expected cancelled to win over upload error, got %v (%v)", code, err)
	}
}

func TestResponseStreamCloseIsIdempotentAndSurfacesCloseError(t *testing.T) {
	closeErr := InvalidArgument("local close failed")
	inner := &closeErrorFrameStream{err: closeErr}
	cancels := 0
	stream := newResponseMessageStream[*testMessage](
		context.Background(),
		inner,
		func() *testMessage { return &testMessage{} },
		defaultCallConfig(),
		func() { cancels++ },
	)

	if err := stream.Close(); !errors.Is(err, closeErr) {
		t.Fatalf("expected first close error %v, got %v", closeErr, err)
	}
	if err := stream.Close(); err != nil {
		t.Fatalf("second close should be idempotent, got %v", err)
	}
	if inner.closed != 1 {
		t.Fatalf("underlying stream closed %d times, want 1", inner.closed)
	}
	if cancels != 1 {
		t.Fatalf("cancel called %d times, want 1", cancels)
	}
	if _, err := stream.Recv(); !errors.Is(err, io.EOF) {
		t.Fatalf("closed response stream Recv() = %v, want EOF", err)
	}
}

func TestServerRejectsUnsupportedWireVersionBeforeRouteLookup(t *testing.T) {
	server := NewServer()
	request := newWireRequest("example.Greeter", "Missing", nil)
	request.Version = wireVersion + 1

	response := server.handleRequest(context.Background(), request)
	if CodeFromUint32(response.Status) != CodeFailedPrecondition {
		t.Fatalf("expected failed precondition, got %#v", response)
	}
}

func TestServerRejectsUnsupportedRPCKindBeforeRouteLookup(t *testing.T) {
	server := NewServer()
	request := newWireRequest("example.Greeter", "Missing", nil)
	request.Kind = RPCKind(99)

	response := server.handleRequest(context.Background(), request)
	if CodeFromUint32(response.Status) != CodeInvalidArgument {
		t.Fatalf("expected invalid argument, got %#v", response)
	}
}

func TestServerRejectsOversizedTimeoutBeforeRouteLookup(t *testing.T) {
	server := NewServer()
	request := newWireRequest("example.Greeter", "Missing", nil)
	request.TimeoutNanos = math.MaxInt64 + 1

	response := server.handleRequest(context.Background(), request)
	if CodeFromUint32(response.Status) != CodeInvalidArgument {
		t.Fatalf("expected invalid argument, got %#v", response)
	}
}

func TestServerUnaryDeadlineCancelsSlowHandler(t *testing.T) {
	server := NewServer()
	routeBytes(server, "example.Greeter", "Slow", func(context.Context, []byte) ([]byte, error) {
		select {}
	})
	request := newWireRequest("example.Greeter", "Slow", nil)
	request.TimeoutNanos = uint64(time.Millisecond)

	response := server.handleRequest(context.Background(), request)
	if CodeFromUint32(response.Status) != CodeDeadlineExceeded {
		t.Fatalf("expected deadline exceeded, got %#v", response)
	}
}

func TestServerUnaryHandlerPanicReturnsInternal(t *testing.T) {
	server := NewServer()
	routeBytes(server, "example.Greeter", "Panic", func(context.Context, []byte) ([]byte, error) {
		panic("boom")
	})

	response := server.handleRequest(context.Background(), newWireRequest("example.Greeter", "Panic", nil))
	if CodeFromUint32(response.Status) != CodeInternal {
		t.Fatalf("expected internal status, got %#v", response)
	}
}

func TestServerMetricsPanicDoesNotFailRequest(t *testing.T) {
	server := NewServer()
	server.SetMetrics(panickingMetrics{})
	routeBytes(server, "example.Greeter", "SayHello", func(context.Context, []byte) ([]byte, error) {
		return nil, nil
	})

	response := server.handleRequest(context.Background(), newWireRequest("example.Greeter", "SayHello", nil))
	if CodeFromUint32(response.Status) != CodeOK {
		t.Fatalf("expected ok status, got %#v", response)
	}
}

func TestServerUnaryCompletionMetricsForTerminalStatuses(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		metrics := &recordingMetrics{}
		server := NewServer()
		server.SetMetrics(metrics)
		routeBytes(server, "example.Greeter", "SayHello", func(context.Context, []byte) ([]byte, error) {
			return []byte("hello"), nil
		})

		response := server.handleRequest(context.Background(), newWireRequest("example.Greeter", "SayHello", nil))

		if CodeFromUint32(response.Status) != CodeOK {
			t.Fatalf("expected ok status, got %#v", response)
		}
		expectMetrics(t, metrics, CodeOK)
	})

	t.Run("handler error", func(t *testing.T) {
		metrics := &recordingMetrics{}
		server := NewServer()
		server.SetMetrics(metrics)
		routeBytes(server, "example.Greeter", "Denied", func(context.Context, []byte) ([]byte, error) {
			return nil, NewStatus(CodePermissionDenied, "denied")
		})

		response := server.handleRequest(context.Background(), newWireRequest("example.Greeter", "Denied", nil))

		if CodeFromUint32(response.Status) != CodePermissionDenied {
			t.Fatalf("expected permission denied status, got %#v", response)
		}
		expectMetrics(t, metrics, CodePermissionDenied)
	})

	t.Run("panic", func(t *testing.T) {
		metrics := &recordingMetrics{}
		server := NewServer()
		server.SetMetrics(metrics)
		routeBytes(server, "example.Greeter", "Panic", func(context.Context, []byte) ([]byte, error) {
			panic("boom")
		})

		response := server.handleRequest(context.Background(), newWireRequest("example.Greeter", "Panic", nil))

		if CodeFromUint32(response.Status) != CodeInternal {
			t.Fatalf("expected internal status, got %#v", response)
		}
		expectMetrics(t, metrics, CodeInternal)
	})

	t.Run("malformed body", func(t *testing.T) {
		metrics := &recordingMetrics{}
		server := NewServer()
		server.SetMetrics(metrics)
		RegisterUnary(server, "example.Greeter", "Decode", func() *testMessage { return &testMessage{} }, func(context.Context, *testMessage) (*Response[*testMessage], error) {
			return testResponse(&testMessage{}), nil
		})

		response := server.handleRequest(context.Background(), newWireRequest("example.Greeter", "Decode", []byte{0xff}))

		if CodeFromUint32(response.Status) != CodeInvalidArgument {
			t.Fatalf("expected invalid argument status, got %#v", response)
		}
		expectMetrics(t, metrics, CodeInvalidArgument)
	})

	t.Run("auth rejection", func(t *testing.T) {
		metrics := &recordingMetrics{}
		server := NewServer()
		server.SetMetrics(metrics)
		server.SetAuthorizer(BearerAuthorizer("token"))
		routeBytes(server, "example.Greeter", "SayHello", func(context.Context, []byte) ([]byte, error) {
			return []byte("hello"), nil
		})

		response := server.handleRequest(context.Background(), newWireRequest("example.Greeter", "SayHello", nil))

		if CodeFromUint32(response.Status) != CodeUnauthenticated {
			t.Fatalf("expected unauthenticated status, got %#v", response)
		}
		expectMetrics(t, metrics, CodeUnauthenticated)
	})

	t.Run("deadline", func(t *testing.T) {
		metrics := &recordingMetrics{}
		server := NewServer()
		server.SetMetrics(metrics)
		routeBytes(server, "example.Greeter", "Slow", func(context.Context, []byte) ([]byte, error) {
			select {}
		})
		request := newWireRequest("example.Greeter", "Slow", nil)
		request.TimeoutNanos = uint64(time.Millisecond)

		response := server.handleRequest(context.Background(), request)

		if CodeFromUint32(response.Status) != CodeDeadlineExceeded {
			t.Fatalf("expected deadline exceeded status, got %#v", response)
		}
		expectMetrics(t, metrics, CodeDeadlineExceeded)
	})
}

func TestServerRequestStreamPanicReturnsInternal(t *testing.T) {
	server := NewServer()
	server.routeStreaming("example.Greeter", "Upload", RPCKindClientStreaming, func(_ context.Context, _ []byte, requests byteStream) (byteStream, error) {
		_, err := requests.Recv()
		if err != nil {
			return nil, err
		}

		return newEmptyStream[[]byte](), nil
	})

	request := newWireRequest("example.Greeter", "Upload", nil)
	request.Kind = RPCKindClientStreaming
	response := server.handleStreamingRequest(context.Background(), request, panickingByteStream{})
	frame, err := response.Recv()
	if err != nil {
		t.Fatalf("read status frame: %v", err)
	}
	if frame.Kind != wireFrameStatus || CodeFromUint32(frame.Status) != CodeInternal {
		t.Fatalf("expected internal status, got %#v", frame)
	}
}

func TestServerResponseStreamPanicReturnsInternal(t *testing.T) {
	server := NewServer()
	server.routeStreaming("example.Greeter", "Download", RPCKindServerStreaming, func(context.Context, []byte, byteStream) (byteStream, error) {
		return panickingByteStream{}, nil
	})

	request := newWireRequest("example.Greeter", "Download", nil)
	request.Kind = RPCKindServerStreaming
	response := server.handleStreamingRequest(context.Background(), request, newEmptyStream[[]byte]())
	frame, err := response.Recv()
	if err != nil {
		t.Fatalf("read status frame: %v", err)
	}
	if frame.Kind != wireFrameStatus || CodeFromUint32(frame.Status) != CodeInternal {
		t.Fatalf("expected internal status, got %#v", frame)
	}
}

func TestRequestStreamMessageLimitReturnsResourceExhausted(t *testing.T) {
	server := NewServer()
	options := DefaultServerOptions()
	options.MaxStreamMessages = 1
	server.SetOptions(options)
	server.routeStreaming("example.Greeter", "Upload", RPCKindClientStreaming, func(_ context.Context, _ []byte, requests byteStream) (byteStream, error) {
		for {
			_, err := requests.Recv()
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				return nil, err
			}
		}

		return newEmptyStream[[]byte](), nil
	})

	request := newWireRequest("example.Greeter", "Upload", nil)
	request.Kind = RPCKindClientStreaming
	response := server.handleStreamingRequest(context.Background(), request, newSliceStream([]byte("one"), []byte("two")))
	frame, err := response.Recv()
	if err != nil {
		t.Fatalf("read status frame: %v", err)
	}
	if frame.Kind != wireFrameStatus || CodeFromUint32(frame.Status) != CodeResourceExhausted {
		t.Fatalf("expected resource exhausted status, got %#v", frame)
	}
}

func TestRequestStreamLimitBoundaries(t *testing.T) {
	server := NewServer()
	options := DefaultServerOptions()
	options.MaxStreamMessages = 2
	options.MaxStreamBodySize = 3
	options.StreamIdleTimeout = 0
	options.DisableStreamIdleTimeout = true
	server.SetOptions(options)
	server.routeStreaming("example.Greeter", "Upload", RPCKindClientStreaming, func(_ context.Context, _ []byte, requests byteStream) (byteStream, error) {
		for {
			_, err := requests.Recv()
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				return nil, err
			}
		}

		return newEmptyStream[[]byte](), nil
	})

	request := newWireRequest("example.Greeter", "Upload", nil)
	request.Kind = RPCKindClientStreaming
	response := server.handleStreamingRequest(context.Background(), request, newSliceStream([]byte("a"), []byte("bc")))
	frame, err := response.Recv()
	if err != nil {
		t.Fatalf("read exact-limit status frame: %v", err)
	}
	if frame.Kind != wireFrameStatus || CodeFromUint32(frame.Status) != CodeOK {
		t.Fatalf("expected ok status at exact limit, got %#v", frame)
	}

	response = server.handleStreamingRequest(context.Background(), request, newSliceStream([]byte("abc"), []byte("d")))
	frame, err = response.Recv()
	if err != nil {
		t.Fatalf("read over-limit status frame: %v", err)
	}
	if frame.Kind != wireFrameStatus || CodeFromUint32(frame.Status) != CodeResourceExhausted {
		t.Fatalf("expected resource exhausted status over limit, got %#v", frame)
	}
}

func TestRequestStreamIdleTimeoutReturnsUnavailable(t *testing.T) {
	server := NewServer()
	options := DefaultServerOptions()
	options.MaxStreamMessages = -1
	options.StreamIdleTimeout = time.Millisecond
	server.SetOptions(options)
	server.routeStreaming("example.Greeter", "Upload", RPCKindClientStreaming, func(_ context.Context, _ []byte, requests byteStream) (byteStream, error) {
		for {
			_, err := requests.Recv()
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				return nil, err
			}
		}

		return newEmptyStream[[]byte](), nil
	})

	request := newWireRequest("example.Greeter", "Upload", nil)
	request.Kind = RPCKindClientStreaming
	response := server.handleStreamingRequest(context.Background(), request, pendingByteStream{})
	frame, err := response.Recv()
	if err != nil {
		t.Fatalf("read status frame: %v", err)
	}
	if frame.Kind != wireFrameStatus || CodeFromUint32(frame.Status) != CodeUnavailable {
		t.Fatalf("expected unavailable status, got %#v", frame)
	}
}

func TestResponseStreamBodyLimitReturnsResourceExhausted(t *testing.T) {
	server := NewServer()
	options := DefaultServerOptions()
	options.MaxStreamBodySize = 3
	server.SetOptions(options)
	server.routeStreaming("example.Greeter", "Download", RPCKindServerStreaming, func(context.Context, []byte, byteStream) (byteStream, error) {
		return newSliceStream([]byte("four")), nil
	})

	request := newWireRequest("example.Greeter", "Download", nil)
	request.Kind = RPCKindServerStreaming
	response := server.handleStreamingRequest(context.Background(), request, newEmptyStream[[]byte]())
	frame, err := response.Recv()
	if err != nil {
		t.Fatalf("read status frame: %v", err)
	}
	if frame.Kind != wireFrameStatus || CodeFromUint32(frame.Status) != CodeResourceExhausted {
		t.Fatalf("expected resource exhausted status, got %#v", frame)
	}
}

func TestResponseStreamLimitBoundaries(t *testing.T) {
	server := NewServer()
	options := DefaultServerOptions()
	options.MaxStreamMessages = 2
	options.MaxStreamBodySize = 3
	options.StreamIdleTimeout = 0
	options.DisableStreamIdleTimeout = true
	server.SetOptions(options)
	server.routeStreaming("example.Greeter", "Download", RPCKindServerStreaming, func(context.Context, []byte, byteStream) (byteStream, error) {
		return newSliceStream([]byte("a"), []byte("bc")), nil
	})

	request := newWireRequest("example.Greeter", "Download", nil)
	request.Kind = RPCKindServerStreaming
	response := server.handleStreamingRequest(context.Background(), request, newEmptyStream[[]byte]())
	for _, expected := range [][]byte{[]byte("a"), []byte("bc")} {
		frame, err := response.Recv()
		if err != nil {
			t.Fatalf("read exact-limit response frame: %v", err)
		}
		if frame.Kind != wireFrameMessage || !bytes.Equal(frame.Body, expected) {
			t.Fatalf("expected exact-limit message %q, got %#v", expected, frame)
		}
	}
	frame, err := response.Recv()
	if err != nil {
		t.Fatalf("read exact-limit terminal status: %v", err)
	}
	if frame.Kind != wireFrameStatus || CodeFromUint32(frame.Status) != CodeOK {
		t.Fatalf("expected ok status at exact limit, got %#v", frame)
	}

	server = NewServer()
	options.MaxStreamMessages = 1
	options.MaxStreamBodySize = 3
	server.SetOptions(options)
	server.routeStreaming("example.Greeter", "Download", RPCKindServerStreaming, func(context.Context, []byte, byteStream) (byteStream, error) {
		return newSliceStream([]byte("a"), []byte("bc")), nil
	})
	response = server.handleStreamingRequest(context.Background(), request, newEmptyStream[[]byte]())
	frame, err = response.Recv()
	if err != nil {
		t.Fatalf("read first response frame: %v", err)
	}
	if frame.Kind != wireFrameMessage || !bytes.Equal(frame.Body, []byte("a")) {
		t.Fatalf("expected first message before message limit, got %#v", frame)
	}
	frame, err = response.Recv()
	if err != nil {
		t.Fatalf("read over-limit terminal status: %v", err)
	}
	if frame.Kind != wireFrameStatus || CodeFromUint32(frame.Status) != CodeResourceExhausted {
		t.Fatalf("expected resource exhausted after message limit, got %#v", frame)
	}
}

func TestUnaryResponseBodyLimitBoundary(t *testing.T) {
	response := okWireResponse([]byte("ok"))

	if err := validateResponse(response, len(response.Body)); err != nil {
		t.Fatalf("exact response body limit should pass: %v", err)
	}
	var frameTooLarge *FrameTooLargeError
	if err := validateResponse(response, len(response.Body)-1); !errors.As(err, &frameTooLarge) {
		t.Fatalf("expected frame-too-large error over response body limit, got %T %v", err, err)
	}
}

func TestResponseStreamIdleTimeoutReturnsUnavailable(t *testing.T) {
	server := NewServer()
	options := DefaultServerOptions()
	options.StreamIdleTimeout = time.Millisecond
	server.SetOptions(options)
	server.routeStreaming("example.Greeter", "Download", RPCKindServerStreaming, func(context.Context, []byte, byteStream) (byteStream, error) {
		return pendingByteStream{}, nil
	})

	request := newWireRequest("example.Greeter", "Download", nil)
	request.Kind = RPCKindServerStreaming
	response := server.handleStreamingRequest(context.Background(), request, newEmptyStream[[]byte]())
	frame, err := response.Recv()
	if err != nil {
		t.Fatalf("read status frame: %v", err)
	}
	if frame.Kind != wireFrameStatus || CodeFromUint32(frame.Status) != CodeUnavailable {
		t.Fatalf("expected unavailable status, got %#v", frame)
	}
}

func TestResponseStreamCloseRecordsCancelled(t *testing.T) {
	metrics := &recordingMetrics{}
	server := NewServer()
	server.SetMetrics(metrics)
	server.routeStreaming("example.Greeter", "Download", RPCKindServerStreaming, func(context.Context, []byte, byteStream) (byteStream, error) {
		return pendingByteStream{}, nil
	})

	request := newWireRequest("example.Greeter", "Download", nil)
	request.Kind = RPCKindServerStreaming
	response := server.handleStreamingRequest(context.Background(), request, newEmptyStream[[]byte]())
	for range 2 {
		if err := closeMessageStream(response); err != nil {
			t.Fatalf("closeMessageStream() error = %v", err)
		}
	}

	expectMetrics(t, metrics, CodeCancelled)
}

func TestServerDrainsFiniteRequestBeforeResponseClose(t *testing.T) {
	tests := []struct {
		name      string
		kind      RPCKind
		configure func(*Server)
	}{
		{
			name: "unary",
			kind: RPCKindUnary,
			configure: func(server *Server) {
				routeBytes(server, "example.Greeter", "Call", func(context.Context, []byte) ([]byte, error) {
					return []byte("response"), nil
				})
			},
		},
		{
			name: "server streaming",
			kind: RPCKindServerStreaming,
			configure: func(server *Server) {
				server.routeStreaming("example.Greeter", "Call", RPCKindServerStreaming, func(context.Context, []byte, byteStream) (byteStream, error) {
					return newSliceStream([]byte("response")), nil
				})
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := NewServer()
			test.configure(server)
			request := newWireRequest("example.Greeter", "Call", []byte("request"))
			request.Kind = test.kind
			var input bytes.Buffer
			if err := writeFrame(&input, request, DefaultMaxFrameSize); err != nil {
				t.Fatalf("write request frame: %v", err)
			}
			stream := &countingRPCStream{reader: bytes.NewReader(input.Bytes())}

			handleRPCStream(context.Background(), server, stream)

			if !stream.readEOF {
				t.Fatal("expected request EOF to be observed")
			}
			if stream.closedBeforeReadEOF {
				t.Fatal("response closed before request EOF was observed")
			}
			if stream.closeCount != 1 {
				t.Fatalf("expected one response close, got %d", stream.closeCount)
			}
			if stream.cancelReadCount != 0 {
				t.Fatalf("expected graceful request completion, got %d read cancellations", stream.cancelReadCount)
			}
		})
	}
}

func TestServerFiniteRequestDrainUsesRPCDeadline(t *testing.T) {
	for _, kind := range []RPCKind{RPCKindUnary, RPCKindServerStreaming} {
		t.Run(fmt.Sprintf("kind-%d", kind), func(t *testing.T) {
			server := NewServer()
			options := DefaultServerOptions()
			options.InitialRequestTimeout = time.Second
			server.SetOptions(options)
			if kind == RPCKindUnary {
				routeBytes(server, "example.Greeter", "Call", func(context.Context, []byte) ([]byte, error) {
					time.Sleep(20 * time.Millisecond)
					return []byte("response"), nil
				})
			} else {
				server.routeStreaming("example.Greeter", "Call", RPCKindServerStreaming, func(context.Context, []byte, byteStream) (byteStream, error) {
					time.Sleep(20 * time.Millisecond)
					return newSliceStream([]byte("response")), nil
				})
			}

			request := newWireRequest("example.Greeter", "Call", nil)
			request.Kind = kind
			request.TimeoutNanos = uint64(100 * time.Millisecond)
			var input bytes.Buffer
			if err := writeFrame(&input, request, DefaultMaxFrameSize); err != nil {
				t.Fatalf("write request frame: %v", err)
			}
			stream := newRequestThenBlockingRPCStream(input.Bytes())

			startedAt := time.Now()
			handleRPCStream(context.Background(), server, stream)
			elapsed := time.Since(startedAt)

			if elapsed < 50*time.Millisecond {
				t.Fatalf("request completed before RPC deadline: %s", elapsed)
			}
			if elapsed > 500*time.Millisecond {
				t.Fatalf("request drain exceeded RPC deadline bound: %s", elapsed)
			}
			if stream.cancelReadCount != 1 {
				t.Fatalf("request read cancellations = %d, want 1", stream.cancelReadCount)
			}
			if stream.closeCount != 1 {
				t.Fatalf("response closes = %d, want 1", stream.closeCount)
			}
			if want := []string{"cancel read", "close"}; !slices.Equal(stream.cleanupOrder, want) {
				t.Fatalf("cleanup order = %v, want %v", stream.cleanupOrder, want)
			}
		})
	}
}

func TestFiniteRequestDrainMapsInitialTimeoutToDeadlineExceeded(t *testing.T) {
	options := DefaultServerOptions()
	err := drainRequestEnd(context.Background(), options, time.Now(), timeoutRPCStream{err: os.ErrDeadlineExceeded})
	if code := StatusFromError(err).Code; code != CodeDeadlineExceeded {
		t.Fatalf("finite request timeout status = %v, want deadline exceeded", code)
	}
}

func TestFiniteRequestDrainPreservesTransportTimeout(t *testing.T) {
	options := DefaultServerOptions()
	err := drainRequestEnd(context.Background(), options, time.Now(), timeoutRPCStream{err: errors.New("transport timeout")})
	if code := StatusFromError(err).Code; code != CodeUnavailable {
		t.Fatalf("finite request transport timeout status = %v, want unavailable", code)
	}
}

func TestServerFiniteRequestDrainReusesInitialRequestDeadline(t *testing.T) {
	for _, kind := range []RPCKind{RPCKindUnary, RPCKindServerStreaming} {
		t.Run(fmt.Sprintf("kind-%d", kind), func(t *testing.T) {
			server := NewServer()
			options := DefaultServerOptions()
			options.InitialRequestTimeout = time.Second
			server.SetOptions(options)
			if kind == RPCKindUnary {
				routeBytes(server, "example.Greeter", "Call", func(context.Context, []byte) ([]byte, error) {
					time.Sleep(20 * time.Millisecond)
					return []byte("response"), nil
				})
			} else {
				server.routeStreaming("example.Greeter", "Call", RPCKindServerStreaming, func(context.Context, []byte, byteStream) (byteStream, error) {
					time.Sleep(20 * time.Millisecond)
					return newSliceStream([]byte("response")), nil
				})
			}

			request := newWireRequest("example.Greeter", "Call", nil)
			request.Kind = kind
			var input bytes.Buffer
			if err := writeFrame(&input, request, DefaultMaxFrameSize); err != nil {
				t.Fatalf("write request frame: %v", err)
			}
			stream := &countingRPCStream{reader: bytes.NewReader(input.Bytes())}

			handleRPCStream(context.Background(), server, stream)

			if len(stream.readDeadlines) != 4 {
				t.Fatalf("read deadlines = %v, want initial set/clear and drain set/clear", stream.readDeadlines)
			}
			if stream.readDeadlines[0].IsZero() || stream.readDeadlines[2].IsZero() {
				t.Fatalf("expected non-zero initial and drain deadlines: %v", stream.readDeadlines)
			}
			if !stream.readDeadlines[0].Equal(stream.readDeadlines[2]) {
				t.Fatalf("drain deadline %s restarted initial deadline %s", stream.readDeadlines[2], stream.readDeadlines[0])
			}
			if !stream.readDeadlines[1].IsZero() || !stream.readDeadlines[3].IsZero() {
				t.Fatalf("read deadlines were not cleared: %v", stream.readDeadlines)
			}
		})
	}
}

func TestServerRejectsFiniteRequestTrailingDataBeforeHandler(t *testing.T) {
	tests := []struct {
		name      string
		kind      RPCKind
		configure func(*Server, *bool)
	}{
		{
			name: "unary",
			kind: RPCKindUnary,
			configure: func(server *Server, called *bool) {
				routeBytes(server, "example.Greeter", "Call", func(context.Context, []byte) ([]byte, error) {
					*called = true
					return []byte("response"), nil
				})
			},
		},
		{
			name: "server streaming",
			kind: RPCKindServerStreaming,
			configure: func(server *Server, called *bool) {
				server.routeStreaming("example.Greeter", "Call", RPCKindServerStreaming, func(context.Context, []byte, byteStream) (byteStream, error) {
					*called = true
					return newSliceStream([]byte("response")), nil
				})
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := NewServer()
			called := false
			test.configure(server, &called)
			request := newWireRequest("example.Greeter", "Call", nil)
			request.Kind = test.kind
			var input bytes.Buffer
			if err := writeFrame(&input, request, DefaultMaxFrameSize); err != nil {
				t.Fatalf("write request frame: %v", err)
			}
			input.WriteString("invalid trailing request data")
			stream := &countingRPCStream{reader: bytes.NewReader(input.Bytes())}

			handleRPCStream(context.Background(), server, stream)

			if called {
				t.Fatal("handler ran before the finite request was validated")
			}
			if stream.cancelReadCount != 1 || stream.closeCount != 1 {
				t.Fatalf("cleanup counts = close %d, cancel read %d; want 1 each", stream.closeCount, stream.cancelReadCount)
			}
			if want := []string{"close", "cancel read"}; !slices.Equal(stream.cleanupOrder, want) {
				t.Fatalf("cleanup order = %v, want %v", stream.cleanupOrder, want)
			}
			if test.kind == RPCKindUnary {
				response := &wireResponse{}
				if err := readFrame(bytes.NewReader(stream.written.Bytes()), response, DefaultMaxFrameSize); err != nil {
					t.Fatalf("read unary rejection: %v", err)
				}
				if CodeFromUint32(response.Status) != CodeInvalidArgument {
					t.Fatalf("unary rejection status = %d, want invalid argument", response.Status)
				}
				return
			}
			frames := readStreamFramesFromBytes(t, stream.written.Bytes(), DefaultMaxFrameSize)
			if len(frames) != 1 || frames[0].Kind != wireFrameStatus || CodeFromUint32(frames[0].Status) != CodeInvalidArgument {
				t.Fatalf("expected invalid argument status, got %#v", frames)
			}
		})
	}
}

func TestAdmissionRejectedUploadCancelsRead(t *testing.T) {
	for _, kind := range []RPCKind{RPCKindClientStreaming, RPCKindBidirectionalStreaming} {
		t.Run(fmt.Sprintf("kind-%d", kind), func(t *testing.T) {
			server := NewServer()
			options := DefaultServerOptions()
			options.MaxConcurrentRequests = 1
			server.SetOptions(options)
			runtime := server.freeze()
			lease, ok := runtime.tryRequestLease(newWireRequest("holder", "Call", nil))
			if !ok {
				t.Fatal("acquire request lease")
			}
			defer lease.release()

			request := newWireRequest("example.Greeter", "Call", nil)
			request.Kind = kind
			var input bytes.Buffer
			if err := writeFrame(&input, request, DefaultMaxFrameSize); err != nil {
				t.Fatalf("write request frame: %v", err)
			}
			trailing := []byte("active upload")
			input.Write(trailing)
			reader := bytes.NewReader(input.Bytes())
			stream := &countingRPCStream{reader: reader}

			handleRPCStream(context.Background(), server, stream)

			if reader.Len() != len(trailing) {
				t.Fatalf("rejected upload consumed %d bytes", len(trailing)-reader.Len())
			}
			if stream.cancelReadCount != 1 {
				t.Fatalf("request read cancellations = %d, want 1", stream.cancelReadCount)
			}
			if stream.closeCount != 1 {
				t.Fatalf("response closes = %d, want 1", stream.closeCount)
			}
			if want := []string{"close", "cancel read"}; !slices.Equal(stream.cleanupOrder, want) {
				t.Fatalf("cleanup order = %v, want %v", stream.cleanupOrder, want)
			}
			frames := readStreamFramesFromBytes(t, stream.written.Bytes(), DefaultMaxFrameSize)
			if len(frames) != 1 || frames[0].Kind != wireFrameStatus || CodeFromUint32(frames[0].Status) != CodeUnavailable {
				t.Fatalf("expected unavailable status, got %#v", frames)
			}
		})
	}
}

func TestMalformedFiniteRequestDoesNotWaitForFIN(t *testing.T) {
	tests := []struct {
		name      string
		configure func(*wireRequest)
	}{
		{name: "invalid kind", configure: func(request *wireRequest) { request.Kind = RPCKind(99) }},
		{name: "oversized timeout", configure: func(request *wireRequest) { request.TimeoutNanos = math.MaxInt64 + 1 }},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := NewServer()
			options := DefaultServerOptions()
			options.DisableInitialRequestTimeout = true
			server.SetOptions(options)
			request := newWireRequest("example.Greeter", "Call", nil)
			test.configure(request)
			var input bytes.Buffer
			if err := writeFrame(&input, request, DefaultMaxFrameSize); err != nil {
				t.Fatalf("write request frame: %v", err)
			}
			stream := newRequestThenBlockingRPCStream(input.Bytes())
			done := make(chan struct{})
			go func() {
				handleRPCStream(context.Background(), server, stream)
				close(done)
			}()

			select {
			case <-done:
			case <-time.After(500 * time.Millisecond):
				t.Fatal("malformed request waited for request FIN")
			}
			if stream.cancelReadCount != 1 || stream.closeCount != 1 {
				t.Fatalf("cleanup counts = close %d, cancel read %d; want 1 each", stream.closeCount, stream.cancelReadCount)
			}
			if want := []string{"close", "cancel read"}; !slices.Equal(stream.cleanupOrder, want) {
				t.Fatalf("cleanup order = %v, want %v", stream.cleanupOrder, want)
			}
			response := &wireResponse{}
			if err := readFrame(bytes.NewReader(stream.written.Bytes()), response, DefaultMaxFrameSize); err != nil {
				t.Fatalf("read malformed request response: %v", err)
			}
			if CodeFromUint32(response.Status) != CodeInvalidArgument {
				t.Fatalf("response status = %d, want invalid argument", response.Status)
			}
		})
	}
}

func TestServerAbortsRPCStreamOnResponseWriteError(t *testing.T) {
	writeErr := errors.New("response write failed")
	tests := []struct {
		name      string
		kind      RPCKind
		configure func(*Server)
	}{
		{
			name: "unary",
			kind: RPCKindUnary,
			configure: func(server *Server) {
				routeBytes(server, "example.Greeter", "Call", func(context.Context, []byte) ([]byte, error) {
					return []byte("response"), nil
				})
			},
		},
		{
			name: "server streaming batch",
			kind: RPCKindServerStreaming,
			configure: func(server *Server) {
				server.routeStreaming("example.Greeter", "Call", RPCKindServerStreaming, func(context.Context, []byte, byteStream) (byteStream, error) {
					return newSliceStream([]byte("response")), nil
				})
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := NewServer()
			test.configure(server)
			request := newWireRequest("example.Greeter", "Call", nil)
			request.Kind = test.kind
			var input bytes.Buffer
			if err := writeFrame(&input, request, DefaultMaxFrameSize); err != nil {
				t.Fatalf("write request frame: %v", err)
			}
			stream := &countingRPCStream{reader: bytes.NewReader(input.Bytes()), writeErr: writeErr}

			handleRPCStream(context.Background(), server, stream)

			if stream.cancelReadCount != 1 {
				t.Fatalf("request read cancellations = %d, want 1", stream.cancelReadCount)
			}
			if stream.closeCount != 1 {
				t.Fatalf("response closes = %d, want 1", stream.closeCount)
			}
			if want := []string{"close", "cancel read"}; !slices.Equal(stream.cleanupOrder, want) {
				t.Fatalf("cleanup order = %v, want %v", stream.cleanupOrder, want)
			}
		})
	}
}

func TestServerAbortsRPCStreamOnLocalResponseFrameTooLarge(t *testing.T) {
	server := NewServer()
	options := DefaultServerOptions()
	options.MaxFrameSize = 128
	server.SetOptions(options)
	routeBytes(server, "example.Greeter", "Call", func(context.Context, []byte) ([]byte, error) {
		return make([]byte, 1024), nil
	})
	request := newWireRequest("example.Greeter", "Call", nil)
	var input bytes.Buffer
	if err := writeFrame(&input, request, options.MaxFrameSize); err != nil {
		t.Fatalf("write request frame: %v", err)
	}
	stream := &countingRPCStream{reader: bytes.NewReader(input.Bytes())}

	handleRPCStream(context.Background(), server, stream)

	if stream.writeCount != 0 {
		t.Fatalf("transport writes = %d, want 0 for local frame rejection", stream.writeCount)
	}
	if stream.cancelReadCount != 1 {
		t.Fatalf("request read cancellations = %d, want 1", stream.cancelReadCount)
	}
	if stream.closeCount != 1 {
		t.Fatalf("response closes = %d, want 1", stream.closeCount)
	}
	if want := []string{"close", "cancel read"}; !slices.Equal(stream.cleanupOrder, want) {
		t.Fatalf("cleanup order = %v, want %v", stream.cleanupOrder, want)
	}
}

func TestServerResponseBatchWriterBatchesNonBlockingMessages(t *testing.T) {
	server := NewServer()
	options := DefaultServerOptions()
	options.MaxStreamMessages = -1
	options.StreamIdleTimeout = 0
	options.DisableStreamIdleTimeout = true
	server.SetOptions(options)
	server.routeStreaming("example.Greeter", "Download", RPCKindServerStreaming, func(context.Context, []byte, byteStream) (byteStream, error) {
		return newSliceStream([]byte("one"), []byte("two"), []byte("three")), nil
	})

	request := newWireRequest("example.Greeter", "Download", nil)
	request.Kind = RPCKindServerStreaming
	var input bytes.Buffer
	if err := writeFrame(&input, request, DefaultMaxFrameSize); err != nil {
		t.Fatalf("write request frame: %v", err)
	}
	stream := &countingRPCStream{reader: bytes.NewReader(input.Bytes())}

	handleRPCStream(context.Background(), server, stream)

	if stream.writeCount != 2 {
		t.Fatalf("expected one message batch write and one status write, got %d", stream.writeCount)
	}
	if !stream.closed {
		t.Fatal("expected stream to close gracefully")
	}
	frames := readStreamFramesFromBytes(t, stream.written.Bytes(), DefaultMaxFrameSize)
	if len(frames) != 4 {
		t.Fatalf("expected three messages and status, got %d frames", len(frames))
	}
	for index, want := range [][]byte{[]byte("one"), []byte("two"), []byte("three")} {
		if frames[index].Kind != wireFrameMessage || !bytes.Equal(frames[index].Body, want) {
			t.Fatalf("unexpected message frame %d: %#v", index, frames[index])
		}
	}
	if frames[3].Kind != wireFrameStatus || CodeFromUint32(frames[3].Status) != CodeOK {
		t.Fatalf("expected terminal OK status, got %#v", frames[3])
	}
}

func TestServerResponseBatchWriterSplitsOnByteCap(t *testing.T) {
	server := NewServer()
	options := DefaultServerOptions()
	options.MaxStreamMessages = -1
	options.StreamIdleTimeout = 0
	options.DisableStreamIdleTimeout = true
	server.SetOptions(options)
	server.routeStreaming("example.Greeter", "Download", RPCKindServerStreaming, func(context.Context, []byte, byteStream) (byteStream, error) {
		return newSliceStream([]byte("abcd"), []byte("efgh"), []byte("ijkl")), nil
	})

	request := newWireRequest("example.Greeter", "Download", nil)
	request.Kind = RPCKindServerStreaming
	response := server.handleStreamingRequest(context.Background(), request, newEmptyStream[[]byte]())
	defer discardMessageStream(response)
	frameWriter, ok := response.(transportResponseFramesWriter)
	if !ok {
		t.Fatal("server response stream should expose batched frame writer")
	}
	writer := &countingWriter{}

	done, err := frameWriter.trevrpcWriteNextFrames(context.Background(), writer, 16)
	if err != nil {
		t.Fatalf("write first batch: %v", err)
	}
	if done {
		t.Fatal("first capped batch should not be terminal")
	}
	if writer.writeCount != 1 {
		t.Fatalf("expected one write for first capped batch, got %d", writer.writeCount)
	}
	frames := readStreamFramesFromBytes(t, writer.Bytes(), 16)
	if len(frames) != 1 || frames[0].Kind != wireFrameMessage || !bytes.Equal(frames[0].Body, []byte("abcd")) {
		t.Fatalf("expected only first message in capped batch, got %#v", frames)
	}

	writer.Reset()
	writer.writeCount = 0
	done, err = frameWriter.trevrpcWriteNextFrames(context.Background(), writer, 16)
	if err != nil {
		t.Fatalf("write second batch: %v", err)
	}
	if done {
		t.Fatal("second capped batch should not be terminal")
	}
	frames = readStreamFramesFromBytes(t, writer.Bytes(), 16)
	if len(frames) != 1 || frames[0].Kind != wireFrameMessage || !bytes.Equal(frames[0].Body, []byte("efgh")) {
		t.Fatalf("expected pending second message in next batch, got %#v", frames)
	}
}

func TestServerResponseBatchWriterFallsBackWithIdleTimeout(t *testing.T) {
	server := NewServer()
	options := DefaultServerOptions()
	options.MaxStreamMessages = -1
	options.StreamIdleTimeout = testTimeout
	server.SetOptions(options)
	server.routeStreaming("example.Greeter", "Download", RPCKindServerStreaming, func(context.Context, []byte, byteStream) (byteStream, error) {
		return newSliceStream([]byte("one"), []byte("two")), nil
	})

	request := newWireRequest("example.Greeter", "Download", nil)
	request.Kind = RPCKindServerStreaming
	response := server.handleStreamingRequest(context.Background(), request, newEmptyStream[[]byte]())
	defer discardMessageStream(response)
	frameWriter, ok := response.(transportResponseFramesWriter)
	if !ok {
		t.Fatal("server response stream should expose batched frame writer")
	}
	writer := &countingWriter{}

	done, err := frameWriter.trevrpcWriteNextFrames(context.Background(), writer, DefaultMaxFrameSize)
	if err != nil {
		t.Fatalf("write first frame: %v", err)
	}
	if done {
		t.Fatal("first message frame should not be terminal")
	}
	frames := readStreamFramesFromBytes(t, writer.Bytes(), DefaultMaxFrameSize)
	if len(frames) != 1 || frames[0].Kind != wireFrameMessage || !bytes.Equal(frames[0].Body, []byte("one")) {
		t.Fatalf("idle timeout should fall back to one message per write, got %#v", frames)
	}
}

func TestServerResponseBatchWriterPreservesTerminalStatusAfterBatch(t *testing.T) {
	server := NewServer()
	options := DefaultServerOptions()
	options.MaxStreamBodySize = 3
	options.StreamIdleTimeout = 0
	options.DisableStreamIdleTimeout = true
	server.SetOptions(options)
	server.routeStreaming("example.Greeter", "Download", RPCKindServerStreaming, func(context.Context, []byte, byteStream) (byteStream, error) {
		return newSliceStream([]byte("ok"), []byte("too")), nil
	})

	request := newWireRequest("example.Greeter", "Download", nil)
	request.Kind = RPCKindServerStreaming
	response := server.handleStreamingRequest(context.Background(), request, newEmptyStream[[]byte]())
	defer discardMessageStream(response)
	frameWriter, ok := response.(transportResponseFramesWriter)
	if !ok {
		t.Fatal("server response stream should expose batched frame writer")
	}
	writer := &countingWriter{}

	done, err := frameWriter.trevrpcWriteNextFrames(context.Background(), writer, DefaultMaxFrameSize)
	if err != nil {
		t.Fatalf("write message batch: %v", err)
	}
	if done {
		t.Fatal("message batch should be followed by terminal status")
	}
	frames := readStreamFramesFromBytes(t, writer.Bytes(), DefaultMaxFrameSize)
	if len(frames) != 1 || frames[0].Kind != wireFrameMessage || !bytes.Equal(frames[0].Body, []byte("ok")) {
		t.Fatalf("expected valid message before terminal status, got %#v", frames)
	}

	writer.Reset()
	done, err = frameWriter.trevrpcWriteNextFrames(context.Background(), writer, DefaultMaxFrameSize)
	if err != nil {
		t.Fatalf("write terminal status: %v", err)
	}
	if !done {
		t.Fatal("terminal status should complete the stream")
	}
	frames = readStreamFramesFromBytes(t, writer.Bytes(), DefaultMaxFrameSize)
	if len(frames) != 1 || frames[0].Kind != wireFrameStatus || CodeFromUint32(frames[0].Status) != CodeResourceExhausted {
		t.Fatalf("expected resource exhausted terminal status, got %#v", frames)
	}
}

func TestClientResponseStreamDeadlineReturnsDeadlineExceeded(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	stream := newResponseMessageStream[*testMessage](ctx, pendingFrameStream{}, func() *testMessage { return &testMessage{} }, defaultCallConfig(), func() {})

	_, err := stream.Recv()
	if code := StatusFromError(err).Code; code != CodeDeadlineExceeded {
		t.Fatalf("expected deadline exceeded, got %v (%v)", code, err)
	}
	if _, err := stream.Recv(); !errors.Is(err, io.EOF) {
		t.Fatalf("expected EOF after deadline, got %v", err)
	}
}

func TestClientFrameReceiveMapsTransportDeadlineToContextDeadline(t *testing.T) {
	stream := &deadlineFrameStream{err: os.ErrDeadlineExceeded}
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(time.Hour))
	defer cancel()

	_, err := recvClientFrame(ctx, stream)
	if code := StatusFromError(err).Code; code != CodeDeadlineExceeded {
		t.Fatalf("expected deadline exceeded, got %v (%v)", code, err)
	}
}

func TestClientOptimizedFrameReceiveUsesTransportDeadline(t *testing.T) {
	stream := &deadlineFieldsStream{deadlineFrameStream: deadlineFrameStream{err: os.ErrDeadlineExceeded}}
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(time.Hour))
	defer cancel()

	_, err := recvClientFrame(ctx, stream)
	if code := StatusFromError(err).Code; code != CodeDeadlineExceeded {
		t.Fatalf("expected deadline exceeded, got %v (%v)", code, err)
	}
	if len(stream.deadlines) != 2 {
		t.Fatalf("expected set and clear deadlines, got %d", len(stream.deadlines))
	}
	if stream.deadlines[0].IsZero() || !stream.deadlines[1].IsZero() {
		t.Fatalf("deadline was not set then cleared: %#v", stream.deadlines)
	}
}

func TestUnaryContextCancellationReturnsCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := Unary(ctx, contextBlockingTransport{}, "example.Greeter", "SayHello", &testMessage{Value: "cancel"}, func() *testMessage { return &testMessage{} })
	if code := StatusFromError(err).Code; code != CodeCancelled {
		t.Fatalf("expected cancelled, got %v (%v)", code, err)
	}
}

func TestServerStreamingContextCancellationReturnsCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	stream, err := ServerStreaming(ctx, contextBlockingTransport{}, "example.Greeter", "LotsOfReplies", &testMessage{Value: "cancel"}, func() *testMessage { return &testMessage{} })
	if err != nil {
		t.Fatalf("start server stream: %v", err)
	}
	cancel()

	_, err = stream.Recv()
	if code := StatusFromError(err).Code; code != CodeCancelled {
		t.Fatalf("expected cancelled, got %v (%v)", code, err)
	}
}

func TestClientStreamingDeadlineCancelsPendingUpload(t *testing.T) {
	stream, err := ClientStreaming[*testMessage, *testMessage](context.Background(), uploadWaitingTransport{}, "example.Greeter", "LotsOfGreetings", func() *testMessage { return &testMessage{} }, WithTimeout(time.Millisecond))
	if err != nil {
		t.Fatalf("start client stream: %v", err)
	}
	time.Sleep(2 * time.Millisecond)
	_, err = stream.CloseAndRecv()
	if code := StatusFromError(err).Code; code != CodeDeadlineExceeded {
		t.Fatalf("expected deadline exceeded, got %v (%v)", code, err)
	}
}

func TestBidirectionalStreamingContextCancellationReturnsCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	stream, err := BidirectionalStreaming[*testMessage, *testMessage](ctx, contextBlockingTransport{}, "example.Greeter", "BidiHello", func() *testMessage { return &testMessage{} })
	if err != nil {
		t.Fatalf("start bidi stream: %v", err)
	}
	cancel()

	_, err = stream.Recv()
	if code := StatusFromError(err).Code; code != CodeCancelled {
		t.Fatalf("expected cancelled, got %v (%v)", code, err)
	}
}

func TestServerOptionsCanonicalizeSafePartialValues(t *testing.T) {
	server := NewServer()
	server.SetOptions(ServerOptions{MaxConcurrentRequests: 1})
	options := server.Options()
	defaults := DefaultServerOptions()
	if options.MaxConcurrentRequests != 1 || options.MaxFrameSize != defaults.MaxFrameSize || options.MaxConcurrentConnections != defaults.MaxConcurrentConnections || options.MaxConcurrentAdmissionCallbacks != defaults.MaxConcurrentAdmissionCallbacks {
		t.Fatalf("partial options were not safely canonicalized: %+v", options)
	}
	if options.InitialRequestTimeout != defaults.InitialRequestTimeout || options.StreamIdleTimeout != defaults.StreamIdleTimeout || options.GracefulShutdownTimeout != defaults.GracefulShutdownTimeout || options.HTTPPath != DefaultHTTPPath {
		t.Fatalf("partial options removed safety defaults: %+v", options)
	}

	server = NewServer()
	server.SetOptions(ServerOptions{DisableInitialRequestTimeout: true, DisableStreamIdleTimeout: true, DisableGracefulShutdownTimeout: true, MaxStreamMessages: -1, MaxStreamBodySize: -1})
	options = server.Options()
	if options.InitialRequestTimeout != 0 || options.StreamIdleTimeout != 0 || options.GracefulShutdownTimeout != 0 || options.MaxStreamMessages != -1 || options.MaxStreamBodySize != -1 {
		t.Fatalf("explicit advanced controls were not preserved: %+v", options)
	}
}

func TestServerOptionsRejectInvalidValues(t *testing.T) {
	tests := []ServerOptions{
		{MaxFrameSize: -1},
		{MaxFrameSize: int(uint64(math.MaxUint32) + 1)},
		{MaxConcurrentConnections: -1},
		{MaxConcurrentStreamsPerConnection: -1},
		{MaxConcurrentRequests: -1},
		{MaxConcurrentAdmissionCallbacks: -1},
		{MaxStreamMessages: -2},
		{MaxStreamBodySize: -2},
		{InitialRequestTimeout: -time.Nanosecond},
		{StreamIdleTimeout: -time.Nanosecond},
		{GracefulShutdownTimeout: -time.Nanosecond},
		{HTTPPath: "relative"},
	}
	for index, options := range tests {
		func() {
			defer func() {
				if recover() == nil {
					t.Errorf("invalid options case %d did not panic: %+v", index, options)
				}
			}()
			NewServer().SetOptions(options)
		}()
	}
}

func TestServerFreezesConfigurationOnDirectHandle(t *testing.T) {
	server := NewServer()
	routeBytes(server, "service", "method", func(context.Context, []byte) ([]byte, error) { return nil, nil })
	response := server.handleRequest(context.Background(), newWireRequest("service", "method", nil))
	if CodeFromUint32(response.Status) != CodeOK {
		t.Fatalf("initial response = %#v", response)
	}

	mutations := []func(){
		func() { server.SetOptions(ServerOptions{}) },
		func() { server.SetAuthorizer(nil) },
		func() { server.SetMetrics(nil) },
		func() { server.SetDiagnostics(nil) },
		func() {
			routeBytes(server, "service", "other", func(context.Context, []byte) ([]byte, error) { return nil, nil })
		},
		func() {
			server.routeUnary("service", "other", func(context.Context, []byte) (*wireResponse, error) { return okWireResponse(nil), nil })
		},
		func() {
			server.routeStreaming("service", "stream", RPCKindServerStreaming, func(context.Context, []byte, byteStream) (byteStream, error) { return newEmptyStream[[]byte](), nil })
		},
	}
	for index, mutate := range mutations {
		func() {
			defer func() {
				recovered := recover()
				if err, ok := recovered.(error); !ok || !errors.Is(err, ErrServerFrozen) {
					t.Errorf("mutation %d panic = %#v, want ErrServerFrozen", index, recovered)
				}
			}()
			mutate()
		}()
	}
	if options := server.Options(); options.MaxConcurrentRequests != DefaultServerOptions().MaxConcurrentRequests {
		t.Fatalf("frozen options changed: %+v", options)
	}
}

func TestServerRejectsInvalidRoutesBeforeServing(t *testing.T) {
	tests := []func(*Server){
		func(server *Server) {
			routeBytes(server, "", "method", func(context.Context, []byte) ([]byte, error) { return nil, nil })
		},
		func(server *Server) {
			routeBytes(server, "service", "", func(context.Context, []byte) ([]byte, error) { return nil, nil })
		},
		func(server *Server) { server.routeUnary("service", "method", nil) },
		func(server *Server) {
			server.routeStreaming("service", "method", RPCKindUnary, func(context.Context, []byte, byteStream) (byteStream, error) { return nil, nil })
		},
		func(server *Server) {
			server.routeStreaming("service", "method", RPCKind(99), func(context.Context, []byte, byteStream) (byteStream, error) { return nil, nil })
		},
		func(server *Server) { server.routeStreaming("service", "method", RPCKindServerStreaming, nil) },
	}
	for index, register := range tests {
		func() {
			defer func() {
				if recover() == nil {
					t.Errorf("invalid route case %d did not panic", index)
				}
			}()
			register(NewServer())
		}()
	}
}

func TestMetadataValueAuthorizerSnapshotsExpectedValue(t *testing.T) {
	value := bytes.Repeat([]byte{'a'}, 256)
	expected := append([]byte(nil), value...)
	authorizer := NewMetadataValueAuthorizer("authorization", value)
	value[0] = 'X'

	request := HandlerContext{Service: "service", Method: "method", Metadata: Metadata{"authorization": expected}}
	if err := authorizer.Authorize(context.Background(), request); err != nil {
		t.Fatalf("authorization changed after source mutation: %v", err)
	}

	stop := make(chan struct{})
	mutated := make(chan struct{})
	go func() {
		defer close(mutated)
		for {
			select {
			case <-stop:
				return
			default:
				value[0] ^= 1
			}
		}
	}()
	for range 1000 {
		if err := authorizer.Authorize(context.Background(), request); err != nil {
			close(stop)
			<-mutated
			t.Fatalf("authorization changed during source mutation: %v", err)
		}
	}
	close(stop)
	<-mutated
}

func TestServerDeadlineWinsCooperativeCallbackResults(t *testing.T) {
	deadlineRequest := func(method string, kind RPCKind) *wireRequest {
		request := newWireRequest("service", method, nil)
		request.Kind = kind
		request.TimeoutNanos = uint64(10 * time.Millisecond)
		return request
	}

	t.Run("unary handler", func(t *testing.T) {
		server := NewServer()
		routeBytes(server, "service", "method", func(ctx context.Context, _ []byte) ([]byte, error) {
			<-ctx.Done()
			return []byte("late success"), nil
		})
		response := server.handleRequest(context.Background(), deadlineRequest("method", RPCKindUnary))
		if CodeFromUint32(response.Status) != CodeDeadlineExceeded {
			t.Fatalf("response = %#v, want deadline exceeded", response)
		}
	})

	t.Run("unary response handler", func(t *testing.T) {
		server := NewServer()
		server.routeUnary("service", "method", func(ctx context.Context, _ []byte) (*wireResponse, error) {
			<-ctx.Done()
			return okWireResponse([]byte("late success")), nil
		})
		response := server.handleRequest(context.Background(), deadlineRequest("method", RPCKindUnary))
		if CodeFromUint32(response.Status) != CodeDeadlineExceeded {
			t.Fatalf("response = %#v, want deadline exceeded", response)
		}
	})

	t.Run("authorizer", func(t *testing.T) {
		server := NewServer()
		server.SetAuthorizer(authorizerFunc(func(ctx context.Context, _ HandlerContext) error {
			<-ctx.Done()
			return nil
		}))
		routeBytes(server, "service", "method", func(context.Context, []byte) ([]byte, error) { return nil, nil })
		response := server.handleRequest(context.Background(), deadlineRequest("method", RPCKindUnary))
		if CodeFromUint32(response.Status) != CodeDeadlineExceeded {
			t.Fatalf("response = %#v, want deadline exceeded", response)
		}
	})

	t.Run("streaming handler closes late stream", func(t *testing.T) {
		server := NewServer()
		late := &closeCountingByteStream{}
		server.routeStreaming("service", "method", RPCKindServerStreaming, func(ctx context.Context, _ []byte, _ byteStream) (byteStream, error) {
			<-ctx.Done()
			return late, nil
		})
		frame, err := server.handleStreamingRequest(context.Background(), deadlineRequest("method", RPCKindServerStreaming), newEmptyStream[[]byte]()).Recv()
		if err != nil {
			t.Fatalf("receive deadline status: %v", err)
		}
		if frame.Kind != wireFrameStatus || CodeFromUint32(frame.Status) != CodeDeadlineExceeded {
			t.Fatalf("frame = %#v, want deadline exceeded", frame)
		}
		waitForAtomicCount(t, &late.closed, 1, "late response stream close")
		time.Sleep(10 * time.Millisecond)
		if got := late.closed.Load(); got != 1 {
			t.Fatalf("late response stream close count = %d, want 1", got)
		}
	})

	t.Run("stream recv", func(t *testing.T) {
		server := NewServer()
		runtime := server.freeze()
		request := deadlineRequest("method", RPCKindClientStreaming)
		lease, ok := runtime.tryRequestLease(request)
		if !ok {
			t.Fatal("failed to acquire request lease")
		}
		defer lease.release()
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
		defer cancel()
		_, err := recvByteWithTimeout(ctx, contextResultByteStream{done: ctx.Done()}, time.Minute, "request", lease)
		if StatusFromError(err).Code != CodeDeadlineExceeded {
			t.Fatalf("recv error = %v, want deadline exceeded", err)
		}
		waitForServerExecutionCount(t, server, 0)
	})
}

func TestServerDiagnosticDispatcherExitsAfterDraining(t *testing.T) {
	delivered := make(chan struct{}, 1)
	server := NewServer()
	server.SetDiagnostics(func(ServerDiagnostic) { delivered <- struct{}{} })
	runtime := server.freeze()
	runtime.emitDiagnostic(ServerDiagnostic{Phase: ServerDiagnosticInternalError})
	select {
	case <-delivered:
	case <-time.After(testTimeout):
		t.Fatal("diagnostic was not delivered")
	}

	deadline := time.Now().Add(testTimeout)
	for time.Now().Before(deadline) {
		runtime.diagnostics.mu.Lock()
		idle := !runtime.diagnostics.running && len(runtime.diagnostics.queue) == 0
		runtime.diagnostics.mu.Unlock()
		if idle {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("diagnostic dispatcher remained running after draining")
}

func TestServerNonCooperativeUnaryRetainsAdmissionUntilExit(t *testing.T) {
	server := NewServer()
	server.SetOptions(ServerOptions{MaxConcurrentRequests: 1})
	started := make(chan struct{})
	releaseHandler := make(chan struct{})
	exited := make(chan struct{})
	routeBytes(server, "service", "hang", func(context.Context, []byte) ([]byte, error) {
		close(started)
		<-releaseHandler
		close(exited)
		return nil, nil
	})
	routeBytes(server, "service", "fast", func(context.Context, []byte) ([]byte, error) { return []byte("ok"), nil })

	request := newWireRequest("service", "hang", nil)
	request.TimeoutNanos = uint64(20 * time.Millisecond)
	response := server.handleRequest(context.Background(), request)
	if CodeFromUint32(response.Status) != CodeDeadlineExceeded {
		t.Fatalf("deadline response = %#v", response)
	}
	<-started
	for range 64 {
		if response := server.handleRequest(context.Background(), newWireRequest("service", "fast", nil)); CodeFromUint32(response.Status) != CodeUnavailable {
			t.Fatalf("detached handler released admission: %#v", response)
		}
	}
	close(releaseHandler)
	select {
	case <-exited:
	case <-time.After(testTimeout):
		t.Fatal("handler did not exit")
	}
	waitForServerExecutionCount(t, server, 0)
	if response := server.handleRequest(context.Background(), newWireRequest("service", "fast", nil)); CodeFromUint32(response.Status) != CodeOK {
		t.Fatalf("request after handler exit = %#v", response)
	}
}

func TestServerNonCooperativeRequestAndResponseRecvRetainAdmission(t *testing.T) {
	for _, test := range []struct {
		name string
		kind RPCKind
		run  func(*Server, *controlledByteStream) *wireStreamFrame
	}{
		{name: "request recv", kind: RPCKindClientStreaming, run: func(server *Server, blocked *controlledByteStream) *wireStreamFrame {
			server.routeStreaming("service", "stream", RPCKindClientStreaming, func(_ context.Context, _ []byte, requests byteStream) (byteStream, error) {
				_, err := requests.Recv()
				return newEmptyStream[[]byte](), err
			})
			request := newWireRequest("service", "stream", nil)
			request.Kind = RPCKindClientStreaming
			request.TimeoutNanos = uint64(20 * time.Millisecond)
			frame, err := server.handleStreamingRequest(context.Background(), request, blocked).Recv()
			if err != nil {
				t.Fatalf("receive request timeout status: %v", err)
			}
			return frame
		}},
		{name: "response recv", kind: RPCKindServerStreaming, run: func(server *Server, blocked *controlledByteStream) *wireStreamFrame {
			server.routeStreaming("service", "stream", RPCKindServerStreaming, func(context.Context, []byte, byteStream) (byteStream, error) { return blocked, nil })
			request := newWireRequest("service", "stream", nil)
			request.Kind = RPCKindServerStreaming
			request.TimeoutNanos = uint64(20 * time.Millisecond)
			frame, err := server.handleStreamingRequest(context.Background(), request, newEmptyStream[[]byte]()).Recv()
			if err != nil {
				t.Fatalf("receive response timeout status: %v", err)
			}
			return frame
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			server := NewServer()
			server.SetOptions(ServerOptions{MaxConcurrentRequests: 1})
			routeBytes(server, "service", "fast", func(context.Context, []byte) ([]byte, error) { return nil, nil })
			blocked := newControlledByteStream()
			frame := test.run(server, blocked)
			if frame.Kind != wireFrameStatus || CodeFromUint32(frame.Status) != CodeDeadlineExceeded {
				t.Fatalf("timeout frame = %#v", frame)
			}
			if response := server.handleRequest(context.Background(), newWireRequest("service", "fast", nil)); CodeFromUint32(response.Status) != CodeUnavailable {
				t.Fatalf("blocked Recv released admission: %#v", response)
			}
			close(blocked.release)
			waitForServerExecutionCount(t, server, 0)
			if response := server.handleRequest(context.Background(), newWireRequest("service", "fast", nil)); CodeFromUint32(response.Status) != CodeOK {
				t.Fatalf("request after Recv exit = %#v", response)
			}
		})
	}
}

func TestServerSanitizesInternalFailuresAndRetainsDiagnostics(t *testing.T) {
	diagnostics := make(chan ServerDiagnostic, 8)
	server := NewServer()
	server.SetDiagnostics(func(event ServerDiagnostic) { diagnostics <- event })
	routeBytes(server, "service", "secret", func(context.Context, []byte) ([]byte, error) { return nil, errors.New("secret local detail") })
	routeBytes(server, "service", "panic", func(context.Context, []byte) ([]byte, error) { panic("boom secret") })
	server.routeUnary("service", "internal", func(context.Context, []byte) (*wireResponse, error) {
		return Internal("secret status").WithMetadata(Metadata{"private": []byte("value")}).intoResponse(nil), nil
	})

	for _, method := range []string{"secret", "panic", "internal"} {
		response := server.handleRequest(context.Background(), newWireRequest("service", method, nil))
		if CodeFromUint32(response.Status) != CodeInternal || response.Message != "internal server error" || len(response.Metadata) != 0 {
			t.Fatalf("%s leaked remote details: %#v", method, response)
		}
	}
	seenLocalDetail := false
	deadline := time.After(testTimeout)
	for range 3 {
		select {
		case diagnostic := <-diagnostics:
			if diagnostic.Err != nil && strings.Contains(diagnostic.Err.Error(), "secret") {
				seenLocalDetail = true
			}
			if diagnostic.Panic != nil && strings.Contains(fmt.Sprint(diagnostic.Panic), "secret") {
				seenLocalDetail = true
			}
		case <-deadline:
			t.Fatal("timed out waiting for server diagnostics")
		}
	}
	if !seenLocalDetail {
		t.Fatal("local diagnostics did not retain internal detail")
	}
}

func TestServerAuthorizerAndDiagnosticPanicsAreContained(t *testing.T) {
	var authorizerCalls atomic.Int64
	var diagnosticCalls atomic.Int64
	server := NewServer()
	server.SetAuthorizer(authorizerFunc(func(context.Context, HandlerContext) error {
		if authorizerCalls.Add(1) == 1 {
			panic("auth boom")
		}
		return nil
	}))
	server.SetDiagnostics(func(ServerDiagnostic) {
		if diagnosticCalls.Add(1) == 1 {
			panic("diagnostic boom")
		}
	})
	routeBytes(server, "service", "method", func(context.Context, []byte) ([]byte, error) { return nil, nil })
	first := server.handleRequest(context.Background(), newWireRequest("service", "method", nil))
	if CodeFromUint32(first.Status) != CodeInternal || first.Message != "internal server error" {
		t.Fatalf("authorizer panic response = %#v", first)
	}
	second := server.handleRequest(context.Background(), newWireRequest("service", "method", nil))
	if CodeFromUint32(second.Status) != CodeOK {
		t.Fatalf("request after callback panic = %#v", second)
	}
}

func waitForServerExecutionCount(t *testing.T, server *Server, want int64) {
	t.Helper()
	deadline := time.Now().Add(testTimeout)
	for time.Now().Before(deadline) {
		if server.freeze().active.Load() == want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("active server executions = %d, want %d", server.freeze().active.Load(), want)
}

type authorizerFunc func(context.Context, HandlerContext) error

func (f authorizerFunc) Authorize(ctx context.Context, info HandlerContext) error {
	return f(ctx, info)
}

func waitForAtomicCount(t *testing.T, count *atomic.Int64, want int64, description string) {
	t.Helper()
	deadline := time.Now().Add(testTimeout)
	for time.Now().Before(deadline) {
		if count.Load() == want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("%s count = %d, want %d", description, count.Load(), want)
}

type closeCountingByteStream struct{ closed atomic.Int64 }

func (*closeCountingByteStream) Recv() ([]byte, error) { return nil, io.EOF }
func (s *closeCountingByteStream) Close() error {
	s.closed.Add(1)
	return nil
}

type contextResultByteStream struct{ done <-chan struct{} }

func (s contextResultByteStream) Recv() ([]byte, error) {
	<-s.done
	return []byte("late success"), nil
}
func (contextResultByteStream) Close() error { return nil }

type controlledByteStream struct{ release chan struct{} }

func newControlledByteStream() *controlledByteStream {
	return &controlledByteStream{release: make(chan struct{})}
}
func (s *controlledByteStream) Recv() ([]byte, error) { <-s.release; return nil, io.EOF }
func (*controlledByteStream) Close() error            { return nil }

type echoTestMessages struct {
	requests MessageStream[*testMessage]
}

func (s *echoTestMessages) Recv() (*testMessage, error) {
	request, err := s.requests.Recv()
	if err != nil {
		return nil, err
	}

	return &testMessage{Value: "echo, " + request.Value}, nil
}

func (s *echoTestMessages) Close() error {
	return s.requests.Close()
}

type pendingByteStream struct{}

func (pendingByteStream) Recv() ([]byte, error) {
	select {}
}

func (pendingByteStream) Close() error { return nil }

type pendingFrameStream struct{}

func (pendingFrameStream) Recv() (*wireStreamFrame, error) {
	select {}
}

func (pendingFrameStream) Close() error { return nil }

type deadlineFrameStream struct {
	deadlines []time.Time
	err       error
}

func (s *deadlineFrameStream) Recv() (*wireStreamFrame, error) {
	return nil, s.err
}

func (s *deadlineFrameStream) Close() error { return nil }

func (s *deadlineFrameStream) SetReadDeadline(deadline time.Time) error {
	s.deadlines = append(s.deadlines, deadline)
	return nil
}

type deadlineFieldsStream struct {
	deadlineFrameStream
}

func (s *deadlineFieldsStream) trevrpcRecvStreamFrameFields() (streamFrameFields, error) {
	return streamFrameFields{}, s.err
}

type panickingByteStream struct{}

func (panickingByteStream) Recv() ([]byte, error) {
	panic("boom")
}

func (panickingByteStream) Close() error {
	panic("close boom")
}

type recordingMetrics struct {
	mu       sync.Mutex
	started  []RPCStarted
	finished []RPCFinished
	codes    []Code
}

type panickingMetrics struct{}

func (panickingMetrics) RPCStarted(RPCStarted) {
	panic("started")
}

func (panickingMetrics) RPCFinished(RPCFinished) {
	panic("finished")
}

func (m *recordingMetrics) RPCStarted(event RPCStarted) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.started = append(m.started, event)
}

func (m *recordingMetrics) RPCFinished(event RPCFinished) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.finished = append(m.finished, event)
	m.codes = append(m.codes, event.Code)
}

func (m *recordingMetrics) snapshot() ([]RPCStarted, []RPCFinished) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return slices.Clone(m.started), slices.Clone(m.finished)
}

func expectMetrics(t *testing.T, metrics *recordingMetrics, codes ...Code) {
	t.Helper()
	started, finished := metrics.snapshot()
	if len(started) != len(codes) {
		t.Fatalf("started metrics = %d, want %d (%#v)", len(started), len(codes), started)
	}
	if len(finished) != len(codes) {
		t.Fatalf("finished metrics = %d, want %d (%#v)", len(finished), len(codes), finished)
	}
	for index, code := range codes {
		if finished[index].Code != code {
			t.Fatalf("finished metric %d code = %v, want %v; all=%#v", index, finished[index].Code, code, finished)
		}
	}
}
