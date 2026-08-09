package trevrpc

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/binary"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"math"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"
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

func (t localTransport) Call(ctx context.Context, request *RpcRequest) (*RpcResponse, error) {
	return t.server.HandleRequest(ctx, request), nil
}

func (t localTransport) StreamingCall(ctx context.Context, request *RpcRequest, requestBody ByteStream) (FrameStream, error) {
	responses := make(chan FrameStream, 1)
	go func() {
		responses <- t.server.HandleStreamingRequest(ctx, request, requestBody)
	}()
	return &localFrameStream{responses: responses}, nil
}

type localFrameStream struct {
	responses <-chan FrameStream
	once      sync.Once
	stream    FrameStream
}

func (s *localFrameStream) Recv() (*RpcStreamFrame, error) {
	return s.responseStream().Recv()
}

func (s *localFrameStream) Close() error {
	if s.stream == nil {
		return nil
	}
	return s.stream.Close()
}

func (s *localFrameStream) responseStream() FrameStream {
	s.once.Do(func() {
		s.stream = <-s.responses
	})
	return s.stream
}

func TestFrameRoundTrip(t *testing.T) {
	request := NewRpcRequest("example.Greeter", "SayHello", []byte("trev"))
	request.Metadata["authorization"] = []byte("ok")

	frame, err := EncodeFrame(request, DefaultMaxFrameSize)
	if err != nil {
		t.Fatalf("encode frame: %v", err)
	}

	decoded := &RpcRequest{}
	if err := ReadFrame(bytes.NewReader(frame), decoded, DefaultMaxFrameSize); err != nil {
		t.Fatalf("decode frame: %v", err)
	}

	if decoded.Service != request.Service || decoded.Method != request.Method || !bytes.Equal(decoded.Body, request.Body) {
		t.Fatalf("decoded request mismatch: %#v", decoded)
	}
}

func TestFrameDecodeErrorMapsToInvalidArgument(t *testing.T) {
	request := &RpcRequest{}
	err := DecodeFrame([]byte{0xff, 0xff}, request)

	if status := StatusFromError(err); status.Code != CodeInvalidArgument {
		t.Fatalf("expected invalid argument for malformed protobuf frame, got %v", status)
	}
	if status := StatusFromError(transportStatus(err)); status.Code != CodeInvalidArgument {
		t.Fatalf("expected transport invalid argument for malformed protobuf frame, got %v", status)
	}
}

func TestTypedProtobufPreflightRejectsKnownWireTypesAndAllowsUnknownFields(t *testing.T) {
	if err := UnmarshalMessage([]byte{0x0a, 0x00}, &RpcResponse{}); err == nil {
		t.Fatal("known status field with a length-delimited wire type was accepted")
	}

	for _, body := range [][]byte{
		{0x40, 0x01},
		{0x49, 1, 2, 3, 4, 5, 6, 7, 8},
		{0x52, 0x01, 0xff},
		{0x5d, 1, 2, 3, 4},
		{0x63, 0x08, 0x01, 0x64},
	} {
		if err := UnmarshalMessage(body, &RpcResponse{}); err != nil {
			t.Fatalf("valid unknown field %x was rejected: %v", body, err)
		}
	}
}

func TestReadFrameLargePartialBodyDoesNotAllocateAdvertisedLength(t *testing.T) {
	const length = 1 << 30
	header := make([]byte, 4)
	binary.BigEndian.PutUint32(header, length)

	read, err := ReadFrameOrEOF(bytes.NewReader(header), &RpcRequest{}, length)
	if read {
		t.Fatal("incomplete frame should not be reported as read")
	}
	if !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("expected EOF for incomplete large frame body, got %v", err)
	}
}

func TestMetadataValidation(t *testing.T) {
	if err := ValidateMetadata(Metadata{"authorization": []byte("ok")}); err != nil {
		t.Fatalf("valid metadata was rejected: %v", err)
	}

	if err := ValidateMetadata(Metadata{"Authorization": []byte("ok")}); err == nil {
		t.Fatal("uppercase metadata key should be rejected")
	}

	if NormalizeMetadataKey("Authorization") != "authorization" {
		t.Fatal("metadata key was not normalized")
	}

	for _, key := range []string{"", "trevrpc-timeout", "bad key"} {
		if err := ValidateMetadata(Metadata{key: []byte("ok")}); err == nil {
			t.Fatalf("metadata key %q should be rejected", key)
		}
	}

	tooManyEntries := Metadata{}
	for i := 0; i <= MaxMetadataEntries; i++ {
		tooManyEntries[fmt.Sprintf("key-%d", i)] = nil
	}
	if err := ValidateMetadata(tooManyEntries); err == nil {
		t.Fatal("metadata entry limit should be rejected")
	}

	if err := ValidateMetadata(Metadata{"key": make([]byte, MaxMetadataValueLen+1)}); err == nil {
		t.Fatal("metadata value limit should be rejected")
	}

	if err := ValidateMetadata(Metadata{"key": make([]byte, MaxMetadataTotalSize+1)}); err == nil {
		t.Fatal("metadata total size limit should be rejected")
	}
}

func TestWireProtocolValidation(t *testing.T) {
	request := NewRpcRequest("service", "method", nil)
	if request.RPCKind() != RpcKindUnary {
		t.Fatalf("expected unary default kind, got %d", request.RPCKind())
	}
	if request.Version != WireVersion {
		t.Fatalf("expected wire version %d, got %d", WireVersion, request.Version)
	}

	request.Version = WireVersion + 1
	if status := StatusFromError(request.ValidateProtocol()); status.Code != CodeFailedPrecondition {
		t.Fatalf("expected failed precondition, got %v", status)
	}

	request.Version = WireVersion
	request.Kind = RpcKind(99)
	if status := StatusFromError(request.ValidateProtocol()); status.Code != CodeInvalidArgument {
		t.Fatalf("expected invalid argument for unknown RPC kind, got %v", status)
	}

	frame := StatusFrame(Unavailable("retry later"))
	if kind, ok := frame.FrameKind(); !ok || kind != RpcStreamFrameKindStatus {
		t.Fatalf("expected status frame kind, got %d %t", kind, ok)
	}
	if status := frame.StatusValue(); status.Code != CodeUnavailable || status.Message != "retry later" {
		t.Fatalf("unexpected status value: %v", status)
	}
}

func TestUnaryClientServer(t *testing.T) {
	server := NewServer()
	server.SetAuthorizer(NewMetadataValueAuthorizer("Authorization", []byte("ok")))
	RegisterUnary(server, "example.Greeter", "SayHello", func() *testMessage { return &testMessage{} }, func(_ context.Context, request *testMessage) (*testMessage, error) {
		return &testMessage{Value: "hello " + request.Value}, nil
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
	server.RouteStreaming("example.Greeter", "LotsOfReplies", RpcKindServerStreaming, func(_ context.Context, body []byte, _ ByteStream) (ByteStream, error) {
		request := &testMessage{}
		if err := UnmarshalMessage(body, request); err != nil {
			return nil, err
		}

		return EncodeStream(FromSlice(
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
	if err != io.EOF {
		t.Fatalf("expected final EOF, got %v", err)
	}

	if first.Value != "hello 1" || second.Value != "hello 2" {
		t.Fatalf("unexpected stream responses: %q %q", first.Value, second.Value)
	}
}

func TestClientStreamingClientServer(t *testing.T) {
	server := NewServer()
	server.RouteStreaming("example.Greeter", "LotsOfGreetings", RpcKindClientStreaming, func(_ context.Context, _ []byte, requests ByteStream) (ByteStream, error) {
		var combined strings.Builder
		for {
			body, err := requests.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				return nil, err
			}

			request := &testMessage{}
			if err := UnmarshalMessage(body, request); err != nil {
				return nil, err
			}

			combined.WriteString(request.Value)
		}

		return SingleMessageStream(&testMessage{Value: combined.String()}), nil
	})

	response, err := runTestClientStreaming(context.Background(), localTransport{server: server}, "example.Greeter", "LotsOfGreetings", []string{"hello", " world"})
	if err != nil {
		t.Fatalf("client streaming RPC failed: %v", err)
	}

	if response.Value != "hello world" {
		t.Fatalf("unexpected response: %q", response.Value)
	}
}

func TestClientStreamingFromStreamClientServer(t *testing.T) {
	server := NewServer()
	server.RouteStreaming("example.Greeter", "LotsOfGreetings", RpcKindClientStreaming, func(_ context.Context, _ []byte, requests ByteStream) (ByteStream, error) {
		var combined strings.Builder
		for {
			body, err := requests.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				return nil, err
			}

			request := &testMessage{}
			if err := UnmarshalMessage(body, request); err != nil {
				return nil, err
			}

			combined.WriteString(request.Value)
		}

		return SingleMessageStream(&testMessage{Value: combined.String()}), nil
	})

	response, err := ClientStreamingFromStream[*testMessage, *testMessage](
		context.Background(),
		localTransport{server: server},
		"example.Greeter",
		"LotsOfGreetings",
		FromSlice(&testMessage{Value: "hello"}, &testMessage{Value: " direct"}),
		func() *testMessage { return &testMessage{} },
	)
	if err != nil {
		t.Fatalf("direct client streaming RPC failed: %v", err)
	}

	if response.Value != "hello direct" {
		t.Fatalf("unexpected response: %q", response.Value)
	}
}

func TestBidirectionalStreamingClientServer(t *testing.T) {
	server := NewServer()
	server.RouteStreaming("example.Greeter", "BidiHello", RpcKindBidirectionalStreaming, func(_ context.Context, _ []byte, requests ByteStream) (ByteStream, error) {
		return EncodeStream[*testMessage](&echoTestMessages{requests: DecodeStream[*testMessage](requests, func() *testMessage { return &testMessage{} })}), nil
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
	if _, err := stream.Recv(); err != io.EOF {
		t.Fatalf("expected final EOF, got %v", err)
	}

	if first.Value != "echo, left" || second.Value != "echo, right" {
		t.Fatalf("unexpected bidi responses: %q %q", first.Value, second.Value)
	}
}

func TestBidirectionalStreamingFromStreamClientServer(t *testing.T) {
	server := NewServer()
	server.RouteStreaming("example.Greeter", "BidiHello", RpcKindBidirectionalStreaming, func(_ context.Context, _ []byte, requests ByteStream) (ByteStream, error) {
		return EncodeStream[*testMessage](&echoTestMessages{requests: DecodeStream[*testMessage](requests, func() *testMessage { return &testMessage{} })}), nil
	})

	stream, err := BidirectionalStreamingFromStream[*testMessage, *testMessage](
		context.Background(),
		localTransport{server: server},
		"example.Greeter",
		"BidiHello",
		FromSlice(&testMessage{Value: "left"}, &testMessage{Value: "right"}),
		func() *testMessage { return &testMessage{} },
	)
	if err != nil {
		t.Fatalf("direct bidi RPC failed: %v", err)
	}

	first, err := stream.Recv()
	if err != nil {
		t.Fatalf("first bidi response failed: %v", err)
	}
	second, err := stream.Recv()
	if err != nil {
		t.Fatalf("second bidi response failed: %v", err)
	}
	if _, err := stream.Recv(); err != io.EOF {
		t.Fatalf("expected final EOF, got %v", err)
	}

	if first.Value != "echo, left" || second.Value != "echo, right" {
		t.Fatalf("unexpected bidi responses: %q %q", first.Value, second.Value)
	}
}

func TestResponseStreamTerminalOKDoesNotHideCloseError(t *testing.T) {
	message := &testMessage{Value: "first"}
	body, err := MarshalMessage(message)
	if err != nil {
		t.Fatalf("marshal message: %v", err)
	}
	closeErr := InvalidArgument("local upload failed")
	stream := newResponseMessageStream[*testMessage](
		&closeErrorFrameStream{
			frames: []*RpcStreamFrame{MessageFrame(body), StatusFrame(OK())},
			err:    closeErr,
		},
		func() *testMessage { return &testMessage{} },
		DefaultCallOptions(),
		context.Background(),
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
		&closeErrorFrameStream{
			frames: []*RpcStreamFrame{StatusFrame(NewStatus(CodePermissionDenied, "remote rejected upload"))},
			err:    closeErr,
		},
		func() *testMessage { return &testMessage{} },
		DefaultCallOptions(),
		context.Background(),
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
		uploadErrorFrameStream{errors: errors},
		func() *testMessage { return &testMessage{} },
		DefaultCallOptions(),
		ctx,
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
		&closeErrorFrameStream{
			frames: []*RpcStreamFrame{StatusFrame(OK())},
			err:    InvalidArgument("local close failed"),
		},
		func() *testMessage { return &testMessage{} },
		DefaultCallOptions(),
		ctx,
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
		inner,
		func() *testMessage { return &testMessage{} },
		DefaultCallOptions(),
		context.Background(),
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
	if _, err := stream.Recv(); err != io.EOF {
		t.Fatalf("closed response stream Recv() = %v, want EOF", err)
	}
}

func TestServerRejectsUnsupportedWireVersionBeforeRouteLookup(t *testing.T) {
	server := NewServer()
	request := NewRpcRequest("example.Greeter", "Missing", nil)
	request.Version = WireVersion + 1

	response := server.HandleRequest(context.Background(), request)
	if CodeFromUint32(response.Status) != CodeFailedPrecondition {
		t.Fatalf("expected failed precondition, got %#v", response)
	}
}

func TestServerRejectsUnsupportedRpcKindBeforeRouteLookup(t *testing.T) {
	server := NewServer()
	request := NewRpcRequest("example.Greeter", "Missing", nil)
	request.Kind = RpcKind(99)

	response := server.HandleRequest(context.Background(), request)
	if CodeFromUint32(response.Status) != CodeInvalidArgument {
		t.Fatalf("expected invalid argument, got %#v", response)
	}
}

func TestServerRejectsOversizedTimeoutBeforeRouteLookup(t *testing.T) {
	server := NewServer()
	request := NewRpcRequest("example.Greeter", "Missing", nil)
	request.TimeoutNanos = math.MaxInt64 + 1

	response := server.HandleRequest(context.Background(), request)
	if CodeFromUint32(response.Status) != CodeInvalidArgument {
		t.Fatalf("expected invalid argument, got %#v", response)
	}
}

func TestServerUnaryDeadlineCancelsSlowHandler(t *testing.T) {
	server := NewServer()
	server.Route("example.Greeter", "Slow", func(context.Context, []byte) ([]byte, error) {
		select {}
	})
	request := NewRpcRequest("example.Greeter", "Slow", nil)
	request.TimeoutNanos = uint64(time.Millisecond)

	response := server.HandleRequest(context.Background(), request)
	if CodeFromUint32(response.Status) != CodeDeadlineExceeded {
		t.Fatalf("expected deadline exceeded, got %#v", response)
	}
}

func TestServerUnaryHandlerPanicReturnsInternal(t *testing.T) {
	server := NewServer()
	server.Route("example.Greeter", "Panic", func(context.Context, []byte) ([]byte, error) {
		panic("boom")
	})

	response := server.HandleRequest(context.Background(), NewRpcRequest("example.Greeter", "Panic", nil))
	if CodeFromUint32(response.Status) != CodeInternal {
		t.Fatalf("expected internal status, got %#v", response)
	}
}

func TestServerMetricsPanicDoesNotFailRequest(t *testing.T) {
	server := NewServer()
	server.SetMetrics(panickingMetrics{})
	server.Route("example.Greeter", "SayHello", func(context.Context, []byte) ([]byte, error) {
		return nil, nil
	})

	response := server.HandleRequest(context.Background(), NewRpcRequest("example.Greeter", "SayHello", nil))
	if CodeFromUint32(response.Status) != CodeOK {
		t.Fatalf("expected ok status, got %#v", response)
	}
}

func TestServerUnaryCompletionMetricsForTerminalStatuses(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		metrics := &recordingMetrics{}
		server := NewServer()
		server.SetMetrics(metrics)
		server.Route("example.Greeter", "SayHello", func(context.Context, []byte) ([]byte, error) {
			return []byte("hello"), nil
		})

		response := server.HandleRequest(context.Background(), NewRpcRequest("example.Greeter", "SayHello", nil))

		if CodeFromUint32(response.Status) != CodeOK {
			t.Fatalf("expected ok status, got %#v", response)
		}
		expectMetrics(t, metrics, CodeOK)
	})

	t.Run("handler error", func(t *testing.T) {
		metrics := &recordingMetrics{}
		server := NewServer()
		server.SetMetrics(metrics)
		server.Route("example.Greeter", "Denied", func(context.Context, []byte) ([]byte, error) {
			return nil, NewStatus(CodePermissionDenied, "denied")
		})

		response := server.HandleRequest(context.Background(), NewRpcRequest("example.Greeter", "Denied", nil))

		if CodeFromUint32(response.Status) != CodePermissionDenied {
			t.Fatalf("expected permission denied status, got %#v", response)
		}
		expectMetrics(t, metrics, CodePermissionDenied)
	})

	t.Run("panic", func(t *testing.T) {
		metrics := &recordingMetrics{}
		server := NewServer()
		server.SetMetrics(metrics)
		server.Route("example.Greeter", "Panic", func(context.Context, []byte) ([]byte, error) {
			panic("boom")
		})

		response := server.HandleRequest(context.Background(), NewRpcRequest("example.Greeter", "Panic", nil))

		if CodeFromUint32(response.Status) != CodeInternal {
			t.Fatalf("expected internal status, got %#v", response)
		}
		expectMetrics(t, metrics, CodeInternal)
	})

	t.Run("malformed body", func(t *testing.T) {
		metrics := &recordingMetrics{}
		server := NewServer()
		server.SetMetrics(metrics)
		RegisterUnary(server, "example.Greeter", "Decode", func() *testMessage { return &testMessage{} }, func(context.Context, *testMessage) (*testMessage, error) {
			return &testMessage{}, nil
		})

		response := server.HandleRequest(context.Background(), NewRpcRequest("example.Greeter", "Decode", []byte{0xff}))

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
		server.Route("example.Greeter", "SayHello", func(context.Context, []byte) ([]byte, error) {
			return []byte("hello"), nil
		})

		response := server.HandleRequest(context.Background(), NewRpcRequest("example.Greeter", "SayHello", nil))

		if CodeFromUint32(response.Status) != CodeUnauthenticated {
			t.Fatalf("expected unauthenticated status, got %#v", response)
		}
		expectMetrics(t, metrics, CodeUnauthenticated)
	})

	t.Run("deadline", func(t *testing.T) {
		metrics := &recordingMetrics{}
		server := NewServer()
		server.SetMetrics(metrics)
		server.Route("example.Greeter", "Slow", func(context.Context, []byte) ([]byte, error) {
			select {}
		})
		request := NewRpcRequest("example.Greeter", "Slow", nil)
		request.TimeoutNanos = uint64(time.Millisecond)

		response := server.HandleRequest(context.Background(), request)

		if CodeFromUint32(response.Status) != CodeDeadlineExceeded {
			t.Fatalf("expected deadline exceeded status, got %#v", response)
		}
		expectMetrics(t, metrics, CodeDeadlineExceeded)
	})
}

func TestServerRequestStreamPanicReturnsInternal(t *testing.T) {
	server := NewServer()
	server.RouteStreaming("example.Greeter", "Upload", RpcKindClientStreaming, func(_ context.Context, _ []byte, requests ByteStream) (ByteStream, error) {
		_, err := requests.Recv()
		if err != nil {
			return nil, err
		}

		return EmptyStream[[]byte](), nil
	})

	request := NewRpcRequest("example.Greeter", "Upload", nil)
	request.Kind = RpcKindClientStreaming
	response := server.HandleStreamingRequest(context.Background(), request, panickingByteStream{})
	frame, err := response.Recv()
	if err != nil {
		t.Fatalf("read status frame: %v", err)
	}
	if frame.Kind != RpcStreamFrameKindStatus || CodeFromUint32(frame.Status) != CodeInternal {
		t.Fatalf("expected internal status, got %#v", frame)
	}
}

func TestServerResponseStreamPanicReturnsInternal(t *testing.T) {
	server := NewServer()
	server.RouteStreaming("example.Greeter", "Download", RpcKindServerStreaming, func(context.Context, []byte, ByteStream) (ByteStream, error) {
		return panickingByteStream{}, nil
	})

	request := NewRpcRequest("example.Greeter", "Download", nil)
	request.Kind = RpcKindServerStreaming
	response := server.HandleStreamingRequest(context.Background(), request, EmptyStream[[]byte]())
	frame, err := response.Recv()
	if err != nil {
		t.Fatalf("read status frame: %v", err)
	}
	if frame.Kind != RpcStreamFrameKindStatus || CodeFromUint32(frame.Status) != CodeInternal {
		t.Fatalf("expected internal status, got %#v", frame)
	}
}

func TestRequestStreamMessageLimitReturnsResourceExhausted(t *testing.T) {
	server := NewServer()
	options := DefaultServerOptions()
	options.MaxStreamMessages = 1
	server.SetOptions(options)
	server.RouteStreaming("example.Greeter", "Upload", RpcKindClientStreaming, func(_ context.Context, _ []byte, requests ByteStream) (ByteStream, error) {
		for {
			_, err := requests.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				return nil, err
			}
		}

		return EmptyStream[[]byte](), nil
	})

	request := NewRpcRequest("example.Greeter", "Upload", nil)
	request.Kind = RpcKindClientStreaming
	response := server.HandleStreamingRequest(context.Background(), request, FromSlice([]byte("one"), []byte("two")))
	frame, err := response.Recv()
	if err != nil {
		t.Fatalf("read status frame: %v", err)
	}
	if frame.Kind != RpcStreamFrameKindStatus || CodeFromUint32(frame.Status) != CodeResourceExhausted {
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
	server.RouteStreaming("example.Greeter", "Upload", RpcKindClientStreaming, func(_ context.Context, _ []byte, requests ByteStream) (ByteStream, error) {
		for {
			_, err := requests.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				return nil, err
			}
		}

		return EmptyStream[[]byte](), nil
	})

	request := NewRpcRequest("example.Greeter", "Upload", nil)
	request.Kind = RpcKindClientStreaming
	response := server.HandleStreamingRequest(context.Background(), request, FromSlice([]byte("a"), []byte("bc")))
	frame, err := response.Recv()
	if err != nil {
		t.Fatalf("read exact-limit status frame: %v", err)
	}
	if frame.Kind != RpcStreamFrameKindStatus || CodeFromUint32(frame.Status) != CodeOK {
		t.Fatalf("expected ok status at exact limit, got %#v", frame)
	}

	response = server.HandleStreamingRequest(context.Background(), request, FromSlice([]byte("abc"), []byte("d")))
	frame, err = response.Recv()
	if err != nil {
		t.Fatalf("read over-limit status frame: %v", err)
	}
	if frame.Kind != RpcStreamFrameKindStatus || CodeFromUint32(frame.Status) != CodeResourceExhausted {
		t.Fatalf("expected resource exhausted status over limit, got %#v", frame)
	}
}

func TestRequestStreamIdleTimeoutReturnsUnavailable(t *testing.T) {
	server := NewServer()
	options := DefaultServerOptions()
	options.MaxStreamMessages = -1
	options.StreamIdleTimeout = time.Millisecond
	server.SetOptions(options)
	server.RouteStreaming("example.Greeter", "Upload", RpcKindClientStreaming, func(_ context.Context, _ []byte, requests ByteStream) (ByteStream, error) {
		for {
			_, err := requests.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				return nil, err
			}
		}

		return EmptyStream[[]byte](), nil
	})

	request := NewRpcRequest("example.Greeter", "Upload", nil)
	request.Kind = RpcKindClientStreaming
	response := server.HandleStreamingRequest(context.Background(), request, pendingByteStream{})
	frame, err := response.Recv()
	if err != nil {
		t.Fatalf("read status frame: %v", err)
	}
	if frame.Kind != RpcStreamFrameKindStatus || CodeFromUint32(frame.Status) != CodeUnavailable {
		t.Fatalf("expected unavailable status, got %#v", frame)
	}
}

func TestResponseStreamBodyLimitReturnsResourceExhausted(t *testing.T) {
	server := NewServer()
	options := DefaultServerOptions()
	options.MaxStreamBodySize = 3
	server.SetOptions(options)
	server.RouteStreaming("example.Greeter", "Download", RpcKindServerStreaming, func(context.Context, []byte, ByteStream) (ByteStream, error) {
		return FromSlice([]byte("four")), nil
	})

	request := NewRpcRequest("example.Greeter", "Download", nil)
	request.Kind = RpcKindServerStreaming
	response := server.HandleStreamingRequest(context.Background(), request, EmptyStream[[]byte]())
	frame, err := response.Recv()
	if err != nil {
		t.Fatalf("read status frame: %v", err)
	}
	if frame.Kind != RpcStreamFrameKindStatus || CodeFromUint32(frame.Status) != CodeResourceExhausted {
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
	server.RouteStreaming("example.Greeter", "Download", RpcKindServerStreaming, func(context.Context, []byte, ByteStream) (ByteStream, error) {
		return FromSlice([]byte("a"), []byte("bc")), nil
	})

	request := NewRpcRequest("example.Greeter", "Download", nil)
	request.Kind = RpcKindServerStreaming
	response := server.HandleStreamingRequest(context.Background(), request, EmptyStream[[]byte]())
	for _, expected := range [][]byte{[]byte("a"), []byte("bc")} {
		frame, err := response.Recv()
		if err != nil {
			t.Fatalf("read exact-limit response frame: %v", err)
		}
		if frame.Kind != RpcStreamFrameKindMessage || !bytes.Equal(frame.Body, expected) {
			t.Fatalf("expected exact-limit message %q, got %#v", expected, frame)
		}
	}
	frame, err := response.Recv()
	if err != nil {
		t.Fatalf("read exact-limit terminal status: %v", err)
	}
	if frame.Kind != RpcStreamFrameKindStatus || CodeFromUint32(frame.Status) != CodeOK {
		t.Fatalf("expected ok status at exact limit, got %#v", frame)
	}

	server = NewServer()
	options.MaxStreamMessages = 1
	options.MaxStreamBodySize = 3
	server.SetOptions(options)
	server.RouteStreaming("example.Greeter", "Download", RpcKindServerStreaming, func(context.Context, []byte, ByteStream) (ByteStream, error) {
		return FromSlice([]byte("a"), []byte("bc")), nil
	})
	response = server.HandleStreamingRequest(context.Background(), request, EmptyStream[[]byte]())
	frame, err = response.Recv()
	if err != nil {
		t.Fatalf("read first response frame: %v", err)
	}
	if frame.Kind != RpcStreamFrameKindMessage || !bytes.Equal(frame.Body, []byte("a")) {
		t.Fatalf("expected first message before message limit, got %#v", frame)
	}
	frame, err = response.Recv()
	if err != nil {
		t.Fatalf("read over-limit terminal status: %v", err)
	}
	if frame.Kind != RpcStreamFrameKindStatus || CodeFromUint32(frame.Status) != CodeResourceExhausted {
		t.Fatalf("expected resource exhausted after message limit, got %#v", frame)
	}
}

func TestUnaryResponseBodyLimitBoundary(t *testing.T) {
	response := OKResponse([]byte("ok"))

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
	server.RouteStreaming("example.Greeter", "Download", RpcKindServerStreaming, func(context.Context, []byte, ByteStream) (ByteStream, error) {
		return pendingByteStream{}, nil
	})

	request := NewRpcRequest("example.Greeter", "Download", nil)
	request.Kind = RpcKindServerStreaming
	response := server.HandleStreamingRequest(context.Background(), request, EmptyStream[[]byte]())
	frame, err := response.Recv()
	if err != nil {
		t.Fatalf("read status frame: %v", err)
	}
	if frame.Kind != RpcStreamFrameKindStatus || CodeFromUint32(frame.Status) != CodeUnavailable {
		t.Fatalf("expected unavailable status, got %#v", frame)
	}
}

func TestResponseStreamCloseRecordsCancelled(t *testing.T) {
	metrics := &recordingMetrics{}
	server := NewServer()
	server.SetMetrics(metrics)
	server.RouteStreaming("example.Greeter", "Download", RpcKindServerStreaming, func(context.Context, []byte, ByteStream) (ByteStream, error) {
		return pendingByteStream{}, nil
	})

	request := NewRpcRequest("example.Greeter", "Download", nil)
	request.Kind = RpcKindServerStreaming
	response := server.HandleStreamingRequest(context.Background(), request, EmptyStream[[]byte]())
	closeMessageStream(response)
	closeMessageStream(response)

	expectMetrics(t, metrics, CodeCancelled)
}

func TestServerDrainsFiniteRequestBeforeResponseClose(t *testing.T) {
	tests := []struct {
		name      string
		kind      RpcKind
		configure func(*Server)
	}{
		{
			name: "unary",
			kind: RpcKindUnary,
			configure: func(server *Server) {
				server.Route("example.Greeter", "Call", func(context.Context, []byte) ([]byte, error) {
					return []byte("response"), nil
				})
			},
		},
		{
			name: "server streaming",
			kind: RpcKindServerStreaming,
			configure: func(server *Server) {
				server.RouteStreaming("example.Greeter", "Call", RpcKindServerStreaming, func(context.Context, []byte, ByteStream) (ByteStream, error) {
					return FromSlice([]byte("response")), nil
				})
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := NewServer()
			test.configure(server)
			request := NewRpcRequest("example.Greeter", "Call", []byte("request"))
			request.Kind = test.kind
			var input bytes.Buffer
			if err := WriteFrame(&input, request, DefaultMaxFrameSize); err != nil {
				t.Fatalf("write request frame: %v", err)
			}
			stream := &countingRPCStream{reader: bytes.NewReader(input.Bytes())}

			handleRPCStream(context.Background(), server, nil, stream)

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
	for _, kind := range []RpcKind{RpcKindUnary, RpcKindServerStreaming} {
		t.Run(fmt.Sprintf("kind-%d", kind), func(t *testing.T) {
			server := NewServer()
			options := DefaultServerOptions()
			options.InitialRequestTimeout = time.Second
			server.SetOptions(options)
			if kind == RpcKindUnary {
				server.Route("example.Greeter", "Call", func(context.Context, []byte) ([]byte, error) {
					time.Sleep(20 * time.Millisecond)
					return []byte("response"), nil
				})
			} else {
				server.RouteStreaming("example.Greeter", "Call", RpcKindServerStreaming, func(context.Context, []byte, ByteStream) (ByteStream, error) {
					time.Sleep(20 * time.Millisecond)
					return FromSlice([]byte("response")), nil
				})
			}

			request := NewRpcRequest("example.Greeter", "Call", nil)
			request.Kind = kind
			request.TimeoutNanos = uint64(100 * time.Millisecond)
			var input bytes.Buffer
			if err := WriteFrame(&input, request, DefaultMaxFrameSize); err != nil {
				t.Fatalf("write request frame: %v", err)
			}
			stream := newRequestThenBlockingRPCStream(input.Bytes())

			startedAt := time.Now()
			handleRPCStream(context.Background(), server, nil, stream)
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

func TestServerFiniteRequestDrainReusesInitialRequestDeadline(t *testing.T) {
	for _, kind := range []RpcKind{RpcKindUnary, RpcKindServerStreaming} {
		t.Run(fmt.Sprintf("kind-%d", kind), func(t *testing.T) {
			server := NewServer()
			options := DefaultServerOptions()
			options.InitialRequestTimeout = time.Second
			server.SetOptions(options)
			if kind == RpcKindUnary {
				server.Route("example.Greeter", "Call", func(context.Context, []byte) ([]byte, error) {
					time.Sleep(20 * time.Millisecond)
					return []byte("response"), nil
				})
			} else {
				server.RouteStreaming("example.Greeter", "Call", RpcKindServerStreaming, func(context.Context, []byte, ByteStream) (ByteStream, error) {
					time.Sleep(20 * time.Millisecond)
					return FromSlice([]byte("response")), nil
				})
			}

			request := NewRpcRequest("example.Greeter", "Call", nil)
			request.Kind = kind
			var input bytes.Buffer
			if err := WriteFrame(&input, request, DefaultMaxFrameSize); err != nil {
				t.Fatalf("write request frame: %v", err)
			}
			stream := &countingRPCStream{reader: bytes.NewReader(input.Bytes())}

			handleRPCStream(context.Background(), server, nil, stream)

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
		kind      RpcKind
		configure func(*Server, *bool)
	}{
		{
			name: "unary",
			kind: RpcKindUnary,
			configure: func(server *Server, called *bool) {
				server.Route("example.Greeter", "Call", func(context.Context, []byte) ([]byte, error) {
					*called = true
					return []byte("response"), nil
				})
			},
		},
		{
			name: "server streaming",
			kind: RpcKindServerStreaming,
			configure: func(server *Server, called *bool) {
				server.RouteStreaming("example.Greeter", "Call", RpcKindServerStreaming, func(context.Context, []byte, ByteStream) (ByteStream, error) {
					*called = true
					return FromSlice([]byte("response")), nil
				})
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := NewServer()
			called := false
			test.configure(server, &called)
			request := NewRpcRequest("example.Greeter", "Call", nil)
			request.Kind = test.kind
			var input bytes.Buffer
			if err := WriteFrame(&input, request, DefaultMaxFrameSize); err != nil {
				t.Fatalf("write request frame: %v", err)
			}
			input.WriteString("invalid trailing request data")
			stream := &countingRPCStream{reader: bytes.NewReader(input.Bytes())}

			handleRPCStream(context.Background(), server, nil, stream)

			if called {
				t.Fatal("handler ran before the finite request was validated")
			}
			if stream.cancelReadCount != 1 || stream.closeCount != 1 {
				t.Fatalf("cleanup counts = close %d, cancel read %d; want 1 each", stream.closeCount, stream.cancelReadCount)
			}
			if want := []string{"close", "cancel read"}; !slices.Equal(stream.cleanupOrder, want) {
				t.Fatalf("cleanup order = %v, want %v", stream.cleanupOrder, want)
			}
			if test.kind == RpcKindUnary {
				response := &RpcResponse{}
				if err := ReadFrame(bytes.NewReader(stream.written.Bytes()), response, DefaultMaxFrameSize); err != nil {
					t.Fatalf("read unary rejection: %v", err)
				}
				if CodeFromUint32(response.Status) != CodeInvalidArgument {
					t.Fatalf("unary rejection status = %d, want invalid argument", response.Status)
				}
				return
			}
			frames := readStreamFramesFromBytes(t, stream.written.Bytes(), DefaultMaxFrameSize)
			if len(frames) != 1 || frames[0].Kind != RpcStreamFrameKindStatus || CodeFromUint32(frames[0].Status) != CodeInvalidArgument {
				t.Fatalf("expected invalid argument status, got %#v", frames)
			}
		})
	}
}

func TestAdmissionRejectedUploadCancelsRead(t *testing.T) {
	for _, kind := range []RpcKind{RpcKindClientStreaming, RpcKindBidirectionalStreaming} {
		t.Run(fmt.Sprintf("kind-%d", kind), func(t *testing.T) {
			server := NewServer()
			options := DefaultServerOptions()
			options.MaxConcurrentRequests = 1
			server.SetOptions(options)
			runtime := server.freeze()
			lease, ok := runtime.tryRequestLease(NewRpcRequest("holder", "Call", nil))
			if !ok {
				t.Fatal("acquire request lease")
			}
			defer lease.release()

			request := NewRpcRequest("example.Greeter", "Call", nil)
			request.Kind = kind
			var input bytes.Buffer
			if err := WriteFrame(&input, request, DefaultMaxFrameSize); err != nil {
				t.Fatalf("write request frame: %v", err)
			}
			trailing := []byte("active upload")
			input.Write(trailing)
			reader := bytes.NewReader(input.Bytes())
			stream := &countingRPCStream{reader: reader}

			handleRPCStream(context.Background(), server, nil, stream)

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
			if len(frames) != 1 || frames[0].Kind != RpcStreamFrameKindStatus || CodeFromUint32(frames[0].Status) != CodeUnavailable {
				t.Fatalf("expected unavailable status, got %#v", frames)
			}
		})
	}
}

func TestAdmissionRejectedFiniteRequestDoesNotWaitForFIN(t *testing.T) {
	for _, kind := range []RpcKind{RpcKindUnary, RpcKindServerStreaming} {
		t.Run(fmt.Sprintf("kind-%d", kind), func(t *testing.T) {
			server := NewServer()
			options := DefaultServerOptions()
			options.MaxConcurrentRequests = 1
			options.DisableInitialRequestTimeout = true
			server.SetOptions(options)
			runtime := server.freeze()
			lease, ok := runtime.tryRequestLease(NewRpcRequest("holder", "Call", nil))
			if !ok {
				t.Fatal("acquire request lease")
			}
			defer lease.release()

			request := NewRpcRequest("example.Greeter", "Call", nil)
			request.Kind = kind
			var input bytes.Buffer
			if err := WriteFrame(&input, request, DefaultMaxFrameSize); err != nil {
				t.Fatalf("write request frame: %v", err)
			}
			stream := newRequestThenBlockingRPCStream(input.Bytes())
			done := make(chan struct{})
			go func() {
				handleRPCStream(context.Background(), server, nil, stream)
				close(done)
			}()

			select {
			case <-done:
			case <-time.After(500 * time.Millisecond):
				t.Fatal("admission rejection waited for request FIN")
			}
			if stream.cancelReadCount != 1 || stream.closeCount != 1 {
				t.Fatalf("cleanup counts = close %d, cancel read %d; want 1 each", stream.closeCount, stream.cancelReadCount)
			}
			if want := []string{"close", "cancel read"}; !slices.Equal(stream.cleanupOrder, want) {
				t.Fatalf("cleanup order = %v, want %v", stream.cleanupOrder, want)
			}
		})
	}
}

func TestServerAbortsRPCStreamOnResponseWriteError(t *testing.T) {
	writeErr := errors.New("response write failed")
	tests := []struct {
		name      string
		kind      RpcKind
		configure func(*Server)
	}{
		{
			name: "unary",
			kind: RpcKindUnary,
			configure: func(server *Server) {
				server.Route("example.Greeter", "Call", func(context.Context, []byte) ([]byte, error) {
					return []byte("response"), nil
				})
			},
		},
		{
			name: "server streaming batch",
			kind: RpcKindServerStreaming,
			configure: func(server *Server) {
				server.RouteStreaming("example.Greeter", "Call", RpcKindServerStreaming, func(context.Context, []byte, ByteStream) (ByteStream, error) {
					return FromSlice([]byte("response")), nil
				})
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := NewServer()
			test.configure(server)
			request := NewRpcRequest("example.Greeter", "Call", nil)
			request.Kind = test.kind
			var input bytes.Buffer
			if err := WriteFrame(&input, request, DefaultMaxFrameSize); err != nil {
				t.Fatalf("write request frame: %v", err)
			}
			stream := &countingRPCStream{reader: bytes.NewReader(input.Bytes()), writeErr: writeErr}

			handleRPCStream(context.Background(), server, nil, stream)

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
	server.Route("example.Greeter", "Call", func(context.Context, []byte) ([]byte, error) {
		return make([]byte, 1024), nil
	})
	request := NewRpcRequest("example.Greeter", "Call", nil)
	var input bytes.Buffer
	if err := WriteFrame(&input, request, options.MaxFrameSize); err != nil {
		t.Fatalf("write request frame: %v", err)
	}
	stream := &countingRPCStream{reader: bytes.NewReader(input.Bytes())}

	handleRPCStream(context.Background(), server, nil, stream)

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
	server.RouteStreaming("example.Greeter", "Download", RpcKindServerStreaming, func(context.Context, []byte, ByteStream) (ByteStream, error) {
		return FromSlice([]byte("one"), []byte("two"), []byte("three")), nil
	})

	request := NewRpcRequest("example.Greeter", "Download", nil)
	request.Kind = RpcKindServerStreaming
	var input bytes.Buffer
	if err := WriteFrame(&input, request, DefaultMaxFrameSize); err != nil {
		t.Fatalf("write request frame: %v", err)
	}
	stream := &countingRPCStream{reader: bytes.NewReader(input.Bytes())}

	handleRPCStream(context.Background(), server, nil, stream)

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
		if frames[index].Kind != RpcStreamFrameKindMessage || !bytes.Equal(frames[index].Body, want) {
			t.Fatalf("unexpected message frame %d: %#v", index, frames[index])
		}
	}
	if frames[3].Kind != RpcStreamFrameKindStatus || CodeFromUint32(frames[3].Status) != CodeOK {
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
	server.RouteStreaming("example.Greeter", "Download", RpcKindServerStreaming, func(context.Context, []byte, ByteStream) (ByteStream, error) {
		return FromSlice([]byte("abcd"), []byte("efgh"), []byte("ijkl")), nil
	})

	request := NewRpcRequest("example.Greeter", "Download", nil)
	request.Kind = RpcKindServerStreaming
	response := server.HandleStreamingRequest(context.Background(), request, EmptyStream[[]byte]())
	defer closeMessageStream(response)
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
	if len(frames) != 1 || frames[0].Kind != RpcStreamFrameKindMessage || !bytes.Equal(frames[0].Body, []byte("abcd")) {
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
	if len(frames) != 1 || frames[0].Kind != RpcStreamFrameKindMessage || !bytes.Equal(frames[0].Body, []byte("efgh")) {
		t.Fatalf("expected pending second message in next batch, got %#v", frames)
	}
}

func TestServerResponseBatchWriterFallsBackWithIdleTimeout(t *testing.T) {
	server := NewServer()
	options := DefaultServerOptions()
	options.MaxStreamMessages = -1
	options.StreamIdleTimeout = testTimeout
	server.SetOptions(options)
	server.RouteStreaming("example.Greeter", "Download", RpcKindServerStreaming, func(context.Context, []byte, ByteStream) (ByteStream, error) {
		return FromSlice([]byte("one"), []byte("two")), nil
	})

	request := NewRpcRequest("example.Greeter", "Download", nil)
	request.Kind = RpcKindServerStreaming
	response := server.HandleStreamingRequest(context.Background(), request, EmptyStream[[]byte]())
	defer closeMessageStream(response)
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
	if len(frames) != 1 || frames[0].Kind != RpcStreamFrameKindMessage || !bytes.Equal(frames[0].Body, []byte("one")) {
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
	server.RouteStreaming("example.Greeter", "Download", RpcKindServerStreaming, func(context.Context, []byte, ByteStream) (ByteStream, error) {
		return FromSlice([]byte("ok"), []byte("too")), nil
	})

	request := NewRpcRequest("example.Greeter", "Download", nil)
	request.Kind = RpcKindServerStreaming
	response := server.HandleStreamingRequest(context.Background(), request, EmptyStream[[]byte]())
	defer closeMessageStream(response)
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
	if len(frames) != 1 || frames[0].Kind != RpcStreamFrameKindMessage || !bytes.Equal(frames[0].Body, []byte("ok")) {
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
	if len(frames) != 1 || frames[0].Kind != RpcStreamFrameKindStatus || CodeFromUint32(frames[0].Status) != CodeResourceExhausted {
		t.Fatalf("expected resource exhausted terminal status, got %#v", frames)
	}
}

func TestWebTransportResponseStreamAdvertisesContextCancellation(t *testing.T) {
	stream := &webTransportResponseStream{}
	if !streamContextCancelsRecv(stream) {
		t.Fatal("webtransport response stream should cancel pending receives from context")
	}
}

func TestClientResponseStreamIdleTimeoutReturnsUnavailable(t *testing.T) {
	options := DefaultCallOptions()
	options.StreamIdleTimeout = time.Millisecond
	stream := newResponseMessageStream[*testMessage](pendingFrameStream{}, func() *testMessage { return &testMessage{} }, options, context.Background(), func() {})

	_, err := stream.Recv()
	if code := StatusFromError(err).Code; code != CodeUnavailable {
		t.Fatalf("expected unavailable, got %v (%v)", code, err)
	}
	if _, err := stream.Recv(); err != io.EOF {
		t.Fatalf("expected EOF after idle timeout, got %v", err)
	}
}

func TestClientResponseStreamDeadlineReturnsDeadlineExceeded(t *testing.T) {
	options := DefaultCallOptions()
	options.StreamIdleTimeout = testTimeout
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	stream := newResponseMessageStream[*testMessage](pendingFrameStream{}, func() *testMessage { return &testMessage{} }, options, ctx, func() {})

	_, err := stream.Recv()
	if code := StatusFromError(err).Code; code != CodeDeadlineExceeded {
		t.Fatalf("expected deadline exceeded, got %v (%v)", code, err)
	}
	if _, err := stream.Recv(); err != io.EOF {
		t.Fatalf("expected EOF after deadline, got %v", err)
	}
}

func TestClientFrameReceiveUsesTransportDeadlineForIdleTimeout(t *testing.T) {
	stream := &deadlineFrameStream{err: os.ErrDeadlineExceeded}

	_, err := recvFrameWithTimeout(context.Background(), stream, time.Minute)
	if code := StatusFromError(err).Code; code != CodeUnavailable {
		t.Fatalf("expected unavailable idle timeout, got %v (%v)", code, err)
	}
	if len(stream.deadlines) != 2 {
		t.Fatalf("expected set and clear deadlines, got %d", len(stream.deadlines))
	}
	if stream.deadlines[0].IsZero() || !stream.deadlines[1].IsZero() {
		t.Fatalf("deadline was not set then cleared: %#v", stream.deadlines)
	}
}

func TestClientFrameReceiveMapsTransportDeadlineToContextDeadline(t *testing.T) {
	stream := &deadlineFrameStream{err: os.ErrDeadlineExceeded}
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(time.Hour))
	defer cancel()

	_, err := recvFrameWithTimeout(ctx, stream, 0)
	if code := StatusFromError(err).Code; code != CodeDeadlineExceeded {
		t.Fatalf("expected deadline exceeded, got %v (%v)", code, err)
	}
}

func TestClientOptimizedFrameReceiveUsesTransportDeadline(t *testing.T) {
	stream := &deadlineFieldsStream{deadlineFrameStream: deadlineFrameStream{err: os.ErrDeadlineExceeded}}

	_, _, err := recvOptimizedFrameFieldsWithTimeout(context.Background(), stream, time.Minute, true)
	if code := StatusFromError(err).Code; code != CodeUnavailable {
		t.Fatalf("expected unavailable idle timeout, got %v (%v)", code, err)
	}
	if len(stream.deadlines) != 2 {
		t.Fatalf("expected set and clear deadlines, got %d", len(stream.deadlines))
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
	stream, err := ServerStreaming(ctx, contextBlockingTransport{}, "example.Greeter", "LotsOfReplies", &testMessage{Value: "cancel"}, func() *testMessage { return &testMessage{} }, WithoutStreamIdleTimeout())
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
	stream, err := ClientStreaming[*testMessage, *testMessage](context.Background(), uploadWaitingTransport{}, "example.Greeter", "LotsOfGreetings", func() *testMessage { return &testMessage{} }, WithTimeout(time.Millisecond), WithoutStreamIdleTimeout())
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
	stream, err := BidirectionalStreaming[*testMessage, *testMessage](ctx, contextBlockingTransport{}, "example.Greeter", "BidiHello", func() *testMessage { return &testMessage{} }, WithoutStreamIdleTimeout())
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
	if options.InitialRequestTimeout != defaults.InitialRequestTimeout || options.StreamIdleTimeout != defaults.StreamIdleTimeout || options.GracefulShutdownTimeout != defaults.GracefulShutdownTimeout || options.HTTP3Path != DefaultHTTP3Path {
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
		{HTTP3Path: "relative"},
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
	server.Route("service", "method", func(context.Context, []byte) ([]byte, error) { return nil, nil })
	response := server.HandleRequest(context.Background(), NewRpcRequest("service", "method", nil))
	if CodeFromUint32(response.Status) != CodeOK {
		t.Fatalf("initial response = %#v", response)
	}

	mutations := []func(){
		func() { server.SetOptions(ServerOptions{}) },
		func() { server.SetAuthorizer(nil) },
		func() { server.ClearAuthorizer() },
		func() { server.SetMetrics(nil) },
		func() { server.SetDiagnostics(nil) },
		func() {
			server.Route("service", "other", func(context.Context, []byte) ([]byte, error) { return nil, nil })
		},
		func() {
			server.RouteResponse("service", "other", func(context.Context, []byte) (*RpcResponse, error) { return OKResponse(nil), nil })
		},
		func() {
			server.RouteStreaming("service", "stream", RpcKindServerStreaming, func(context.Context, []byte, ByteStream) (ByteStream, error) { return EmptyStream[[]byte](), nil })
		},
	}
	for index, mutate := range mutations {
		func() {
			defer func() {
				if recovered := recover(); recovered != ErrServerFrozen {
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

func TestServerFreezesAtListen(t *testing.T) {
	server := NewServer()
	serverTLS, _ := testTLSConfig(t)
	listener, err := Listen("127.0.0.1:0", server, ListenOptions{TLSConfig: serverTLS})
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer listener.Close()
	defer func() {
		if recovered := recover(); recovered != ErrServerFrozen {
			t.Fatalf("post-Listen mutation panic = %#v, want ErrServerFrozen", recovered)
		}
	}()
	server.SetOptions(ServerOptions{})
}

func TestServerRejectsInvalidRoutesBeforeServing(t *testing.T) {
	tests := []func(*Server){
		func(server *Server) {
			server.Route("", "method", func(context.Context, []byte) ([]byte, error) { return nil, nil })
		},
		func(server *Server) {
			server.Route("service", "", func(context.Context, []byte) ([]byte, error) { return nil, nil })
		},
		func(server *Server) { server.Route("service", "method", nil) },
		func(server *Server) { server.RouteResponse("service", "method", nil) },
		func(server *Server) {
			server.RouteStreaming("service", "method", RpcKindUnary, func(context.Context, []byte, ByteStream) (ByteStream, error) { return nil, nil })
		},
		func(server *Server) {
			server.RouteStreaming("service", "method", RpcKind(99), func(context.Context, []byte, ByteStream) (ByteStream, error) { return nil, nil })
		},
		func(server *Server) { server.RouteStreaming("service", "method", RpcKindServerStreaming, nil) },
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

	request := NewRpcRequest("service", "method", nil)
	request.Metadata["authorization"] = expected
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
	deadlineRequest := func(method string, kind RpcKind) *RpcRequest {
		request := NewRpcRequest("service", method, nil)
		request.Kind = kind
		request.TimeoutNanos = uint64(10 * time.Millisecond)
		return request
	}

	t.Run("unary handler", func(t *testing.T) {
		server := NewServer()
		server.Route("service", "method", func(ctx context.Context, _ []byte) ([]byte, error) {
			<-ctx.Done()
			return []byte("late success"), nil
		})
		response := server.HandleRequest(context.Background(), deadlineRequest("method", RpcKindUnary))
		if CodeFromUint32(response.Status) != CodeDeadlineExceeded {
			t.Fatalf("response = %#v, want deadline exceeded", response)
		}
	})

	t.Run("unary response handler", func(t *testing.T) {
		server := NewServer()
		server.RouteResponse("service", "method", func(ctx context.Context, _ []byte) (*RpcResponse, error) {
			<-ctx.Done()
			return OKResponse([]byte("late success")), nil
		})
		response := server.HandleRequest(context.Background(), deadlineRequest("method", RpcKindUnary))
		if CodeFromUint32(response.Status) != CodeDeadlineExceeded {
			t.Fatalf("response = %#v, want deadline exceeded", response)
		}
	})

	t.Run("authorizer", func(t *testing.T) {
		server := NewServer()
		server.SetAuthorizer(authorizerFunc(func(ctx context.Context, _ *RpcRequest) error {
			<-ctx.Done()
			return nil
		}))
		server.Route("service", "method", func(context.Context, []byte) ([]byte, error) { return nil, nil })
		response := server.HandleRequest(context.Background(), deadlineRequest("method", RpcKindUnary))
		if CodeFromUint32(response.Status) != CodeDeadlineExceeded {
			t.Fatalf("response = %#v, want deadline exceeded", response)
		}
	})

	t.Run("streaming handler closes late stream", func(t *testing.T) {
		server := NewServer()
		late := &closeCountingByteStream{}
		server.RouteStreaming("service", "method", RpcKindServerStreaming, func(ctx context.Context, _ []byte, _ ByteStream) (ByteStream, error) {
			<-ctx.Done()
			return late, nil
		})
		frame, err := server.HandleStreamingRequest(context.Background(), deadlineRequest("method", RpcKindServerStreaming), EmptyStream[[]byte]()).Recv()
		if err != nil {
			t.Fatalf("receive deadline status: %v", err)
		}
		if frame.Kind != RpcStreamFrameKindStatus || CodeFromUint32(frame.Status) != CodeDeadlineExceeded {
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
		request := deadlineRequest("method", RpcKindClientStreaming)
		lease, ok := runtime.tryRequestLease(request)
		if !ok {
			t.Fatal("failed to acquire request lease")
		}
		defer lease.release()
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
		defer cancel()
		_, err := recvByteWithTimeout(ctx, contextResultByteStream{done: ctx.Done()}, time.Minute, "request", lease, request)
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
	server.Route("service", "hang", func(context.Context, []byte) ([]byte, error) {
		close(started)
		<-releaseHandler
		close(exited)
		return nil, nil
	})
	server.Route("service", "fast", func(context.Context, []byte) ([]byte, error) { return []byte("ok"), nil })

	request := NewRpcRequest("service", "hang", nil)
	request.TimeoutNanos = uint64(20 * time.Millisecond)
	response := server.HandleRequest(context.Background(), request)
	if CodeFromUint32(response.Status) != CodeDeadlineExceeded {
		t.Fatalf("deadline response = %#v", response)
	}
	<-started
	for range 64 {
		if response := server.HandleRequest(context.Background(), NewRpcRequest("service", "fast", nil)); CodeFromUint32(response.Status) != CodeUnavailable {
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
	if response := server.HandleRequest(context.Background(), NewRpcRequest("service", "fast", nil)); CodeFromUint32(response.Status) != CodeOK {
		t.Fatalf("request after handler exit = %#v", response)
	}
}

func TestServerNonCooperativeRequestAndResponseRecvRetainAdmission(t *testing.T) {
	for _, test := range []struct {
		name string
		kind RpcKind
		run  func(*Server, *controlledByteStream) *RpcStreamFrame
	}{
		{name: "request recv", kind: RpcKindClientStreaming, run: func(server *Server, blocked *controlledByteStream) *RpcStreamFrame {
			server.RouteStreaming("service", "stream", RpcKindClientStreaming, func(_ context.Context, _ []byte, requests ByteStream) (ByteStream, error) {
				_, err := requests.Recv()
				return EmptyStream[[]byte](), err
			})
			request := NewRpcRequest("service", "stream", nil)
			request.Kind = RpcKindClientStreaming
			request.TimeoutNanos = uint64(20 * time.Millisecond)
			frame, err := server.HandleStreamingRequest(context.Background(), request, blocked).Recv()
			if err != nil {
				t.Fatalf("receive request timeout status: %v", err)
			}
			return frame
		}},
		{name: "response recv", kind: RpcKindServerStreaming, run: func(server *Server, blocked *controlledByteStream) *RpcStreamFrame {
			server.RouteStreaming("service", "stream", RpcKindServerStreaming, func(context.Context, []byte, ByteStream) (ByteStream, error) { return blocked, nil })
			request := NewRpcRequest("service", "stream", nil)
			request.Kind = RpcKindServerStreaming
			request.TimeoutNanos = uint64(20 * time.Millisecond)
			frame, err := server.HandleStreamingRequest(context.Background(), request, EmptyStream[[]byte]()).Recv()
			if err != nil {
				t.Fatalf("receive response timeout status: %v", err)
			}
			return frame
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			server := NewServer()
			server.SetOptions(ServerOptions{MaxConcurrentRequests: 1})
			server.Route("service", "fast", func(context.Context, []byte) ([]byte, error) { return nil, nil })
			blocked := newControlledByteStream()
			frame := test.run(server, blocked)
			if frame.Kind != RpcStreamFrameKindStatus || CodeFromUint32(frame.Status) != CodeDeadlineExceeded {
				t.Fatalf("timeout frame = %#v", frame)
			}
			if response := server.HandleRequest(context.Background(), NewRpcRequest("service", "fast", nil)); CodeFromUint32(response.Status) != CodeUnavailable {
				t.Fatalf("blocked Recv released admission: %#v", response)
			}
			close(blocked.release)
			waitForServerExecutionCount(t, server, 0)
			if response := server.HandleRequest(context.Background(), NewRpcRequest("service", "fast", nil)); CodeFromUint32(response.Status) != CodeOK {
				t.Fatalf("request after Recv exit = %#v", response)
			}
		})
	}
}

func TestServerSanitizesInternalFailuresAndRetainsDiagnostics(t *testing.T) {
	diagnostics := make(chan ServerDiagnostic, 8)
	server := NewServer()
	server.SetDiagnostics(func(event ServerDiagnostic) { diagnostics <- event })
	server.Route("service", "secret", func(context.Context, []byte) ([]byte, error) { return nil, errors.New("secret local detail") })
	server.Route("service", "panic", func(context.Context, []byte) ([]byte, error) { panic("boom secret") })
	server.RouteResponse("service", "internal", func(context.Context, []byte) (*RpcResponse, error) {
		return Internal("secret status").WithMetadata(Metadata{"private": []byte("value")}).IntoResponse(nil), nil
	})

	for _, method := range []string{"secret", "panic", "internal"} {
		response := server.HandleRequest(context.Background(), NewRpcRequest("service", method, nil))
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
	server.SetAuthorizer(authorizerFunc(func(context.Context, *RpcRequest) error {
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
	server.Route("service", "method", func(context.Context, []byte) ([]byte, error) { return nil, nil })
	first := server.HandleRequest(context.Background(), NewRpcRequest("service", "method", nil))
	if CodeFromUint32(first.Status) != CodeInternal || first.Message != "internal server error" {
		t.Fatalf("authorizer panic response = %#v", first)
	}
	second := server.HandleRequest(context.Background(), NewRpcRequest("service", "method", nil))
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

type authorizerFunc func(context.Context, *RpcRequest) error

func (f authorizerFunc) Authorize(ctx context.Context, request *RpcRequest) error {
	return f(ctx, request)
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

func (pendingFrameStream) Recv() (*RpcStreamFrame, error) {
	select {}
}

func (pendingFrameStream) Close() error { return nil }

type deadlineFrameStream struct {
	deadlines []time.Time
	err       error
}

func (s *deadlineFrameStream) Recv() (*RpcStreamFrame, error) {
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

func (s *deadlineFieldsStream) trevrpcRecvStreamFrameFields() (streamFrameFields, func(), error) {
	return streamFrameFields{}, nil, s.err
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

func (m *recordingMetrics) hasCode(code Code) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return slices.Contains(m.codes, code)
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

func expectMetricCodeSnapshot(t *testing.T, metrics *recordingMetrics, codes ...Code) {
	t.Helper()
	_, finished := metrics.snapshot()
	if len(finished) != len(codes) {
		t.Fatalf("finished metrics = %d, want %d (%#v)", len(finished), len(codes), finished)
	}
	for index, code := range codes {
		if finished[index].Code != code {
			t.Fatalf("finished metric %d code = %v, want %v; all=%#v", index, finished[index].Code, code, finished)
		}
	}
}

func expectPreHandlerMetrics(t *testing.T, metrics *recordingMetrics, code Code) {
	t.Helper()
	started, finished := metrics.snapshot()
	if len(started) != 1 {
		t.Fatalf("started metrics = %d, want 1 (%#v)", len(started), started)
	}
	if len(finished) != 1 {
		t.Fatalf("finished metrics = %d, want 1 (%#v)", len(finished), finished)
	}
	if started[0].Service != "" || started[0].Method != "" || started[0].RequestBodyLen != 0 {
		t.Fatalf("unexpected pre-handler started metric: %#v", started[0])
	}
	if finished[0].Service != "" || finished[0].Method != "" || finished[0].RequestBodyLen != 0 || finished[0].ResponseBodyLen != 0 || finished[0].Code != code {
		t.Fatalf("unexpected pre-handler finished metric: %#v", finished[0])
	}
}

func TestTransportLimitsFromServerOptions(t *testing.T) {
	options := DefaultServerOptions()
	options.MaxFrameSize = 1024
	options.MaxStreamBodySize = 4096
	options.MaxConcurrentStreamsPerConnection = 10
	options.MaxConcurrentRequests = 123
	options.MaxConcurrentConnections = 7
	options.StreamIdleTimeout = 20 * time.Second

	limits := transportLimitsFromServerOptions(options)

	if limits.StreamReceiveWindow != 1028 {
		t.Fatalf("stream receive window should match one TrevRPC frame, got %d", limits.StreamReceiveWindow)
	}
	if limits.ConnectionReceiveWindow != 4096 {
		t.Fatalf("connection receive window should match bounded stream budget, got %d", limits.ConnectionReceiveWindow)
	}
	if limits.IncomingBidiStreams != 11 {
		t.Fatalf("incoming bidi streams should include one rejection slot, got %d", limits.IncomingBidiStreams)
	}
	if limits.MaxIdleTimeout != 20*time.Second || limits.KeepAlive != 10*time.Second {
		t.Fatalf("transport idle defaults should derive from stream idle timeout, got idle=%s keepalive=%s", limits.MaxIdleTimeout, limits.KeepAlive)
	}
	if limits.MaxStatelessOperations != 123 || limits.MaxBindingStatelessOperations != 7 {
		t.Fatalf("stateless operation limits should derive from server concurrency, got max=%d binding=%d", limits.MaxStatelessOperations, limits.MaxBindingStatelessOperations)
	}

	options.StreamIdleTimeout = -time.Second
	limits = transportLimitsFromServerOptions(options)
	if limits.MaxIdleTimeout != 0 || limits.KeepAlive != 0 {
		t.Fatalf("non-positive idle timeout should disable transport idle defaults, got idle=%s keepalive=%s", limits.MaxIdleTimeout, limits.KeepAlive)
	}
}

func TestQUICServerConfigAlignsTransportLimits(t *testing.T) {
	options := DefaultServerOptions()
	options.MaxFrameSize = 1024
	options.MaxStreamBodySize = 4096
	options.MaxConcurrentStreamsPerConnection = 10
	base := &quic.Config{
		InitialStreamReceiveWindow:     1 << 20,
		MaxStreamReceiveWindow:         1 << 20,
		InitialConnectionReceiveWindow: 1 << 20,
		MaxConnectionReceiveWindow:     1 << 20,
		MaxIncomingStreams:             100,
	}

	config := QUICServerConfig(options, base)

	if base.InitialStreamReceiveWindow != 1<<20 {
		t.Fatal("QUICServerConfig should not mutate the base config")
	}
	if config.InitialStreamReceiveWindow != 1028 || config.MaxStreamReceiveWindow != 1028 {
		t.Fatalf("stream receive windows should match one TrevRPC frame, got initial=%d max=%d", config.InitialStreamReceiveWindow, config.MaxStreamReceiveWindow)
	}
	if config.InitialConnectionReceiveWindow != 4096 || config.MaxConnectionReceiveWindow != 4096 {
		t.Fatalf("connection receive windows should match bounded stream budget, got initial=%d max=%d", config.InitialConnectionReceiveWindow, config.MaxConnectionReceiveWindow)
	}
	if config.MaxIncomingStreams != 11 {
		t.Fatalf("incoming bidirectional streams should include one rejection slot, got %d", config.MaxIncomingStreams)
	}
	if config.MaxIncomingUniStreams != -1 {
		t.Fatalf("native QUIC should disable peer-initiated unidirectional streams, got %d", config.MaxIncomingUniStreams)
	}

	options.EnableWebTransport = true
	config = QUICServerConfig(options, nil)
	if !config.EnableDatagrams || !config.EnableStreamResetPartialDelivery {
		t.Fatal("WebTransport QUIC config should enable required QUIC extensions")
	}
	if config.MaxIncomingUniStreams != 0 {
		t.Fatalf("WebTransport QUIC config should leave HTTP/3 unidirectional stream defaults available, got %d", config.MaxIncomingUniStreams)
	}
}

func TestQUICClientConfigAlignsTransportLimits(t *testing.T) {
	config := QUICClientConfig(2048, &quic.Config{MaxStreamReceiveWindow: 1 << 20})

	if config.InitialStreamReceiveWindow != 2052 || config.MaxStreamReceiveWindow != 2052 {
		t.Fatalf("client stream receive windows should match one TrevRPC frame, got initial=%d max=%d", config.InitialStreamReceiveWindow, config.MaxStreamReceiveWindow)
	}
	if config.InitialConnectionReceiveWindow != 2052 || config.MaxConnectionReceiveWindow != 2052 {
		t.Fatalf("client connection receive windows should match one response frame, got initial=%d max=%d", config.InitialConnectionReceiveWindow, config.MaxConnectionReceiveWindow)
	}
	if config.MaxIncomingStreams != -1 || config.MaxIncomingUniStreams != -1 {
		t.Fatalf("native client should reject peer-initiated streams, got bidi=%d uni=%d", config.MaxIncomingStreams, config.MaxIncomingUniStreams)
	}

	config = WebTransportQUICClientConfig(2048, nil)
	if !config.EnableDatagrams || !config.EnableStreamResetPartialDelivery {
		t.Fatal("WebTransport client QUIC config should enable required QUIC extensions")
	}
	if config.MaxIncomingStreams != -1 || config.MaxIncomingUniStreams != 0 {
		t.Fatalf("WebTransport client should reject peer-initiated bidi streams while leaving HTTP/3 uni defaults, got bidi=%d uni=%d", config.MaxIncomingStreams, config.MaxIncomingUniStreams)
	}
}

func TestQUICTransportConfigDefaultsPreserveExplicitBackendConfig(t *testing.T) {
	config := &quic.Config{}
	applyDefaultQUICTransportConfig(config, TransportConfig{MaxIdleTimeout: 20 * time.Second, KeepAlive: 10 * time.Second})
	if config.MaxIdleTimeout != 20*time.Second || config.KeepAlivePeriod != 10*time.Second {
		t.Fatalf("transport config should fill empty quic-go idle settings, got idle=%s keepalive=%s", config.MaxIdleTimeout, config.KeepAlivePeriod)
	}

	config = &quic.Config{MaxIdleTimeout: 5 * time.Second, KeepAlivePeriod: 2 * time.Second}
	applyDefaultQUICTransportConfig(config, TransportConfig{MaxIdleTimeout: 20 * time.Second, KeepAlive: 10 * time.Second})
	if config.MaxIdleTimeout != 5*time.Second || config.KeepAlivePeriod != 2*time.Second {
		t.Fatalf("explicit quic-go idle settings should win, got idle=%s keepalive=%s", config.MaxIdleTimeout, config.KeepAlivePeriod)
	}
}

func TestChannelNativeUnary(t *testing.T) {
	server := NewServer()
	RegisterUnary(server, "example.Greeter", "SayHello", func() *testMessage { return &testMessage{} }, func(_ context.Context, request *testMessage) (*testMessage, error) {
		return &testMessage{Value: "hello " + request.Value}, nil
	})

	serverTLS, clientTLS := testTLSConfig(t)
	listener, err := Listen("127.0.0.1:0", server, ListenOptions{TLSConfig: serverTLS})
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer listener.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	go func() {
		_ = listener.Serve(ctx)
	}()

	transport, err := Dial(ctx, listener.Addr().String(), DialOptions{TLSConfig: clientTLS})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer transport.Close()

	response, err := Unary(ctx, transport, "example.Greeter", "SayHello", &testMessage{Value: "QUIC"}, func() *testMessage { return &testMessage{} })
	if err != nil {
		t.Fatalf("quic unary RPC failed: %v", err)
	}

	if response.Value != "hello QUIC" {
		t.Fatalf("unexpected response: %q", response.Value)
	}
}

func TestRawQUICRoundTripsUnaryAndAllStreamingModes(t *testing.T) {
	running := startTestQUICServer(t, func(server *Server) {
		server.SetAuthorizer(BearerAuthorizer(testAuthToken))
	})
	conn := connectTestQUICClient(t, running)
	defer conn.CloseWithError(0, "test complete")
	transport := Advanced.NewRawQUICClient(conn)

	reply, err := Unary(context.Background(), transport, testServiceName, "SayHello", &testMessage{Value: "unary"}, func() *testMessage { return &testMessage{} }, authenticatedOptions()...)
	if err != nil {
		t.Fatalf("unary RPC failed: %v", err)
	}
	if reply.Value != "hello, unary" {
		t.Fatalf("unexpected unary reply: %q", reply.Value)
	}

	replies, err := ServerStreaming(context.Background(), transport, testServiceName, "LotsOfReplies", &testMessage{Value: "server stream"}, func() *testMessage { return &testMessage{} }, authenticatedOptions()...)
	if err != nil {
		t.Fatalf("server-streaming RPC failed: %v", err)
	}
	if messages := collectTestMessages(t, replies); !equalStrings(messages, []string{"hello, server stream", "goodbye, server stream"}) {
		t.Fatalf("unexpected server stream replies: %#v", messages)
	}

	summary, err := runTestClientStreaming(context.Background(), transport, testServiceName, "LotsOfGreetings", []string{"one", "two"}, authenticatedOptions()...)
	if err != nil {
		t.Fatalf("client-streaming RPC failed: %v", err)
	}
	if summary.Value != "one,two" {
		t.Fatalf("unexpected client stream summary: %q", summary.Value)
	}

	bidi, err := runTestBidiStreaming(context.Background(), transport, testServiceName, "BidiHello", []string{"left", "right"}, authenticatedOptions()...)
	if err != nil {
		t.Fatalf("bidi RPC failed: %v", err)
	}
	if messages := collectTestMessages(t, bidi); !equalStrings(messages, []string{"echo, left", "echo, right"}) {
		t.Fatalf("unexpected bidi replies: %#v", messages)
	}
}

func TestRawWebTransportRoundTripsUnaryAndAllStreamingModes(t *testing.T) {
	running := startTestWebTransportServer(t, func(server *Server) {
		server.SetAuthorizer(BearerAuthorizer(testAuthToken))
	})
	transport := connectTestWebTransportClient(t, running)
	defer transport.Session().CloseWithError(cancelledWebTransportSessionCode, "test complete")

	for index := range 4 {
		if err := runMixedQUICCall(transport, index); err != nil {
			t.Fatal(err)
		}
	}
}

func TestWebTransportAdmissionAllowsBrowserOrigin(t *testing.T) {
	origin := "http://127.0.0.1:8080"
	running := startTestWebTransportServerWithAdmission(t, func(request WebTransportAdmissionRequest) bool {
		return request.Path == "/trevrpc" && request.Origin == origin
	}, func(server *Server) {
		server.SetAuthorizer(BearerAuthorizer(testAuthToken))
	})

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()
	transport, err := Advanced.DialRawWebTransport(ctx, "https://"+running.addr+"/trevrpc", RawWebTransportDialOptions{
		TLSClientConfig: running.clientTLS.Clone(),
		QUICConfig:      WebTransportQUICClientConfig(DefaultMaxFrameSize, nil),
		RequestHeader:   http.Header{"Origin": []string{origin}},
	})
	if err != nil {
		t.Fatalf("dial WebTransport with origin: %v", err)
	}
	defer transport.Session().CloseWithError(cancelledWebTransportSessionCode, "test complete")

	_, err = Unary(context.Background(), transport, testServiceName, "SayHello", &testMessage{Value: "origin"}, func() *testMessage { return &testMessage{} }, authenticatedOptions()...)
	if err != nil {
		t.Fatalf("unary call failed: %v", err)
	}
}

func TestWebTransportAdmissionReceivesRequestFields(t *testing.T) {
	options := DefaultServerOptions()
	var seen WebTransportAdmissionRequest
	options.WebTransportAdmission = func(request WebTransportAdmissionRequest) bool {
		seen = request
		return request.Path == "/trevrpc" && request.Origin == "https://origin.test" && request.Request != nil
	}

	server := NewServer()
	server.SetOptions(options)
	runtime := server.freeze()
	request := httptest.NewRequest(http.MethodGet, "https://example.test/trevrpc", nil)
	request.Header.Set("Origin", "https://origin.test")
	admitted, err := runtime.webTransportAdmitted(request)
	if err != nil || !admitted {
		t.Fatal("expected admission callback to accept matching request")
	}
	if seen.Authority == "" {
		t.Fatal("expected admission callback to receive authority")
	}

	wrongPath := httptest.NewRequest(http.MethodGet, "https://example.test/wrong", nil)
	wrongPath.Header.Set("Origin", "https://origin.test")
	admitted, err = runtime.webTransportAdmitted(wrongPath)
	if err != nil || admitted {
		t.Fatal("expected admission callback to reject unexpected path")
	}
}

func TestWebTransportAdmissionRequired(t *testing.T) {
	options := DefaultServerOptions()
	options.EnableWebTransport = true
	server := NewServer()
	server.SetOptions(options)
	request := httptest.NewRequest(http.MethodGet, "https://example.test/trevrpc", nil)

	if admitted, err := server.freeze().webTransportAdmitted(request); err != nil || admitted {
		t.Fatal("WebTransport without an admission callback should be rejected")
	}
}

func TestWebTransportRejectsWithoutAdmission(t *testing.T) {
	running := startTestWebTransportServerWithAdmission(t, nil, func(*Server) {})

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()
	transport, err := Advanced.DialRawWebTransport(ctx, "https://"+running.addr+"/trevrpc", RawWebTransportDialOptions{
		TLSClientConfig: running.clientTLS.Clone(),
		QUICConfig:      WebTransportQUICClientConfig(DefaultMaxFrameSize, nil),
	})
	if err == nil {
		transport.Session().CloseWithError(cancelledWebTransportSessionCode, "test complete")
		t.Fatal("WebTransport without an admission callback should be rejected")
	}
}

func TestWebTransportRejectsUnexpectedPath(t *testing.T) {
	running := startTestWebTransportServer(t, func(server *Server) {
		server.SetAuthorizer(BearerAuthorizer(testAuthToken))
	})

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()
	transport, err := Advanced.DialRawWebTransport(ctx, "https://"+running.addr+"/wrong", RawWebTransportDialOptions{
		TLSClientConfig: running.clientTLS.Clone(),
		QUICConfig:      WebTransportQUICClientConfig(DefaultMaxFrameSize, nil),
	})
	if err == nil {
		transport.Session().CloseWithError(cancelledWebTransportSessionCode, "test complete")
		t.Fatal("unexpected WebTransport path should be rejected")
	}
}

func TestQuicAuthFailuresReturnStatusErrors(t *testing.T) {
	running := startTestQUICServer(t, func(server *Server) {
		server.SetAuthorizer(BearerAuthorizer(testAuthToken))
	})
	conn := connectTestQUICClient(t, running)
	defer conn.CloseWithError(0, "test complete")

	_, err := Unary(context.Background(), Advanced.NewRawQUICClient(conn), testServiceName, "SayHello", &testMessage{Value: "missing auth"}, func() *testMessage { return &testMessage{} }, WithTimeout(testTimeout))
	if code := StatusFromError(err).Code; code != CodeUnauthenticated {
		t.Fatalf("expected unauthenticated, got %v (%v)", code, err)
	}
}

func TestQuicOversizedTimeoutRejectedOverWire(t *testing.T) {
	running := startTestQUICServer(t, func(*Server) {})
	conn := connectTestQUICClient(t, running)
	defer conn.CloseWithError(0, "test complete")
	request := NewRpcRequest(testServiceName, "Missing", nil)
	request.TimeoutNanos = math.MaxInt64 + 1

	response, err := Advanced.NewRawQUICClient(conn).Call(context.Background(), request)
	if err != nil {
		t.Fatalf("raw transport call failed: %v", err)
	}
	if CodeFromUint32(response.Status) != CodeInvalidArgument {
		t.Fatalf("expected invalid argument, got %#v", response)
	}
}

func TestQuicRequestIdleTimeoutRecoversAdmission(t *testing.T) {
	running := startTestQUICServer(t, configureIdleTimeoutAdmissionTestServer)
	conn := connectTestQUICClient(t, running)
	defer conn.CloseWithError(0, "test complete")
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()
	stream, err := conn.OpenStreamSync(ctx)
	if err != nil {
		t.Fatalf("open idle request stream: %v", err)
	}
	defer stream.CancelRead(cancelledStreamCode)
	defer stream.CancelWrite(cancelledStreamCode)
	writeIdleTimeoutRequest(t, stream)
	assertIdleTimeoutStatus(t, stream)
	waitForServerExecutionCount(t, running.server, 0)
	assertUnaryAdmissionRecovered(t, Advanced.NewRawQUICClient(conn))
}

func TestWebTransportRequestIdleTimeoutRecoversAdmission(t *testing.T) {
	running := startTestWebTransportServer(t, configureIdleTimeoutAdmissionTestServer)
	transport := connectTestWebTransportClient(t, running)
	defer transport.Session().CloseWithError(cancelledWebTransportSessionCode, "test complete")
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()
	stream, err := transport.Session().OpenStreamSync(ctx)
	if err != nil {
		t.Fatalf("open idle WebTransport request stream: %v", err)
	}
	defer stream.CancelRead(cancelledWebTransportStreamCode)
	defer stream.CancelWrite(cancelledWebTransportStreamCode)
	writeIdleTimeoutRequest(t, stream)
	assertIdleTimeoutStatus(t, stream)
	waitForServerExecutionCount(t, running.server, 0)
	assertUnaryAdmissionRecovered(t, transport)
}

func TestServeQUICWaitsForDetachedExecutions(t *testing.T) {
	const shutdownTimeout = 60 * time.Millisecond
	releaseHandler := make(chan struct{})
	diagnostics := make(chan ServerDiagnostic, 8)
	running := startTestQUICServer(t, func(server *Server) {
		options := DefaultServerOptions()
		options.GracefulShutdownTimeout = shutdownTimeout
		server.SetOptions(options)
		server.SetDiagnostics(func(event ServerDiagnostic) { diagnostics <- event })
		server.Route("service", "hang", func(context.Context, []byte) ([]byte, error) {
			<-releaseHandler
			return nil, nil
		})
	})
	conn := connectTestQUICClient(t, running)
	defer conn.CloseWithError(0, "test complete")
	request := NewRpcRequest("service", "hang", nil)
	request.TimeoutNanos = uint64(10 * time.Millisecond)
	response, err := Advanced.NewRawQUICClient(conn).Call(context.Background(), request)
	if err != nil {
		t.Fatalf("deadline call: %v", err)
	}
	if CodeFromUint32(response.Status) != CodeDeadlineExceeded {
		t.Fatalf("deadline response = %#v", response)
	}

	startedAt := time.Now()
	running.stop(t)
	if elapsed := time.Since(startedAt); elapsed < shutdownTimeout/2 {
		t.Fatalf("shutdown returned after %s, before detached execution timeout %s", elapsed, shutdownTimeout)
	}
	seenIncomplete := false
	deadline := time.After(testTimeout)
	for !seenIncomplete {
		select {
		case diagnostic := <-diagnostics:
			seenIncomplete = diagnostic.Phase == ServerDiagnosticShutdownIncomplete
		case <-deadline:
			t.Fatal("shutdown did not report incomplete detached execution")
		}
	}
	close(releaseHandler)
	waitForServerExecutionCount(t, running.server, 0)
}

func TestQuicRequestStreamLimitsReturnResourceExhausted(t *testing.T) {
	running := startTestQUICServer(t, func(server *Server) {
		options := DefaultServerOptions()
		options.MaxStreamMessages = 1
		options.StreamIdleTimeout = testTimeout
		server.SetOptions(options)
		server.SetAuthorizer(BearerAuthorizer(testAuthToken))
	})
	conn := connectTestQUICClient(t, running)
	defer conn.CloseWithError(0, "test complete")

	_, err := runTestClientStreaming(context.Background(), Advanced.NewRawQUICClient(conn), testServiceName, "LotsOfGreetings", []string{"one", "two"}, authenticatedOptions()...)
	if code := StatusFromError(err).Code; code != CodeResourceExhausted {
		t.Fatalf("expected resource exhausted, got %v (%v)", code, err)
	}
}

func TestQuicResponseStreamLimitsReturnResourceExhausted(t *testing.T) {
	running := startTestQUICServer(t, func(server *Server) {
		server.SetAuthorizer(BearerAuthorizer(testAuthToken))
	})
	conn := connectTestQUICClient(t, running)
	defer conn.CloseWithError(0, "test complete")

	replies, err := ServerStreaming(context.Background(), Advanced.NewRawQUICClient(conn), testServiceName, "LotsOfReplies", &testMessage{Value: "limited"}, func() *testMessage { return &testMessage{} }, append(authenticatedOptions(), WithMaxResponseMessages(1))...)
	if err != nil {
		t.Fatalf("server-streaming RPC failed: %v", err)
	}
	first, err := replies.Recv()
	if err != nil {
		t.Fatalf("first response failed: %v", err)
	}
	if first.Value != "hello, limited" {
		t.Fatalf("unexpected first response: %q", first.Value)
	}
	_, err = replies.Recv()
	if code := StatusFromError(err).Code; code != CodeResourceExhausted {
		t.Fatalf("expected resource exhausted, got %v (%v)", code, err)
	}
}

func TestQuicStreamConcurrencyLimitReturnsUnavailable(t *testing.T) {
	running := startTestQUICServer(t, func(server *Server) {
		options := DefaultServerOptions()
		options.MaxConcurrentStreamsPerConnection = 1
		options.GracefulShutdownTimeout = 50 * time.Millisecond
		server.SetOptions(options)
		server.SetAuthorizer(BearerAuthorizer(testAuthToken))
	})
	conn := connectTestQUICClient(t, running)
	defer conn.CloseWithError(0, "test complete")
	transport := Advanced.NewRawQUICClient(conn)
	hanging := holdServerStreamOpen(t, transport)
	defer closeMessageStream(hanging)

	_, err := Unary(context.Background(), transport, testServiceName, "SayHello", &testMessage{Value: "second"}, func() *testMessage { return &testMessage{} }, authenticatedOptions()...)
	if code := StatusFromError(err).Code; code != CodeUnavailable {
		t.Fatalf("expected unavailable, got %v (%v)", code, err)
	}
}

func TestQuicRequestConcurrencyLimitReturnsUnavailable(t *testing.T) {
	metrics := &recordingMetrics{}
	running := startTestQUICServer(t, func(server *Server) {
		options := DefaultServerOptions()
		options.MaxConcurrentRequests = 1
		options.GracefulShutdownTimeout = 50 * time.Millisecond
		server.SetOptions(options)
		server.SetMetrics(metrics)
		server.SetAuthorizer(BearerAuthorizer(testAuthToken))
	})
	conn := connectTestQUICClient(t, running)
	defer conn.CloseWithError(0, "test complete")
	transport := Advanced.NewRawQUICClient(conn)
	hanging := holdServerStreamOpen(t, transport)
	defer closeMessageStream(hanging)

	_, err := Unary(context.Background(), transport, testServiceName, "SayHello", &testMessage{Value: "second"}, func() *testMessage { return &testMessage{} }, authenticatedOptions()...)
	if code := StatusFromError(err).Code; code != CodeUnavailable {
		t.Fatalf("expected unavailable, got %v (%v)", code, err)
	}
	waitForMetricCode(t, metrics, CodeUnavailable)
	expectMetricCodeSnapshot(t, metrics, CodeUnavailable)
}

func TestQuicConnectionLimitRefusesNewConnections(t *testing.T) {
	running := startTestQUICServer(t, func(server *Server) {
		options := DefaultServerOptions()
		options.MaxConcurrentConnections = 1
		options.GracefulShutdownTimeout = 50 * time.Millisecond
		server.SetOptions(options)
		server.SetAuthorizer(BearerAuthorizer(testAuthToken))
	})
	conn := connectTestQUICClient(t, running)
	defer conn.CloseWithError(0, "test complete")
	hanging := holdServerStreamOpen(t, Advanced.NewRawQUICClient(conn))
	defer closeMessageStream(hanging)

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()
	secondConn, err := quic.DialAddr(ctx, running.addr, running.clientTLS.Clone(), &quic.Config{})
	if err != nil {
		return
	}
	defer secondConn.CloseWithError(0, "test complete")

	_, err = Unary(context.Background(), Advanced.NewRawQUICClient(secondConn), testServiceName, "SayHello", &testMessage{Value: "refused"}, func() *testMessage { return &testMessage{} }, authenticatedOptions()...)
	if code := StatusFromError(err).Code; code != CodeUnavailable {
		t.Fatalf("expected unavailable, got %v (%v)", code, err)
	}
}

func TestQuicDroppedResponseStreamCancelsServerWork(t *testing.T) {
	metrics := &recordingMetrics{}
	running := startTestQUICServer(t, func(server *Server) {
		options := server.Options()
		options.GracefulShutdownTimeout = 50 * time.Millisecond
		server.SetOptions(options)
		server.SetMetrics(metrics)
		server.SetAuthorizer(BearerAuthorizer(testAuthToken))
	})
	conn := connectTestQUICClient(t, running)
	defer conn.CloseWithError(0, "test complete")
	replies := holdServerStreamOpen(t, Advanced.NewRawQUICClient(conn))

	closeMessageStream(replies)
	waitForMetricCode(t, metrics, CodeCancelled)
}

func TestWebTransportDroppedResponseStreamCancelsServerWork(t *testing.T) {
	metrics := &recordingMetrics{}
	running := startTestWebTransportServer(t, func(server *Server) {
		options := server.Options()
		options.GracefulShutdownTimeout = 50 * time.Millisecond
		server.SetOptions(options)
		server.SetMetrics(metrics)
		server.SetAuthorizer(BearerAuthorizer(testAuthToken))
	})
	transport := connectTestWebTransportClient(t, running)
	defer transport.Session().CloseWithError(cancelledWebTransportSessionCode, "test complete")
	replies := holdServerStreamOpen(t, transport)

	closeMessageStream(replies)
	waitForMetricCode(t, metrics, CodeCancelled)
}

func TestQuicServerStreamingContextCancelWhileResponsePending(t *testing.T) {
	metrics := &recordingMetrics{}
	running := startTestQUICServer(t, func(server *Server) {
		options := server.Options()
		options.GracefulShutdownTimeout = 50 * time.Millisecond
		server.SetOptions(options)
		server.SetMetrics(metrics)
		server.SetAuthorizer(BearerAuthorizer(testAuthToken))
	})
	conn := connectTestQUICClient(t, running)
	defer conn.CloseWithError(0, "test complete")

	ctx, cancel := context.WithCancel(context.Background())
	replies, err := ServerStreaming(ctx, Advanced.NewRawQUICClient(conn), testServiceName, "LotsOfReplies", &testMessage{Value: "cancel"}, func() *testMessage { return &testMessage{} }, authenticatedOptions()...)
	if err != nil {
		t.Fatalf("start server stream: %v", err)
	}
	first, err := replies.Recv()
	if err != nil {
		t.Fatalf("first response failed: %v", err)
	}
	if first.Value != "first" {
		t.Fatalf("unexpected first response: %q", first.Value)
	}

	cancel()
	_, err = replies.Recv()
	if code := StatusFromError(err).Code; code != CodeCancelled {
		t.Fatalf("expected cancelled, got %v (%v)", code, err)
	}
	waitForMetricCode(t, metrics, CodeCancelled)
}

func TestWebTransportServerStreamingContextCancelWhileResponsePending(t *testing.T) {
	metrics := &recordingMetrics{}
	running := startTestWebTransportServer(t, func(server *Server) {
		options := server.Options()
		options.GracefulShutdownTimeout = 50 * time.Millisecond
		server.SetOptions(options)
		server.SetMetrics(metrics)
		server.SetAuthorizer(BearerAuthorizer(testAuthToken))
	})
	transport := connectTestWebTransportClient(t, running)
	defer transport.Session().CloseWithError(cancelledWebTransportSessionCode, "test complete")

	ctx, cancel := context.WithCancel(context.Background())
	replies, err := ServerStreaming(ctx, transport, testServiceName, "LotsOfReplies", &testMessage{Value: "cancel"}, func() *testMessage { return &testMessage{} }, authenticatedOptions()...)
	if err != nil {
		t.Fatalf("start WebTransport server stream: %v", err)
	}
	first, err := replies.Recv()
	if err != nil {
		t.Fatalf("first WebTransport response failed: %v", err)
	}
	if first.Value != "first" {
		t.Fatalf("unexpected first WebTransport response: %q", first.Value)
	}

	cancel()
	_, err = replies.Recv()
	if code := StatusFromError(err).Code; code != CodeCancelled {
		t.Fatalf("expected cancelled, got %v (%v)", code, err)
	}
	waitForMetricCode(t, metrics, CodeCancelled)
}

func TestQuicClientStreamingDeadlineWhileUploadPending(t *testing.T) {
	running := startTestQUICServer(t, func(server *Server) {
		server.SetAuthorizer(BearerAuthorizer(testAuthToken))
	})
	conn := connectTestQUICClient(t, running)
	defer conn.CloseWithError(0, "test complete")

	stream, err := ClientStreaming[*testMessage, *testMessage](context.Background(), Advanced.NewRawQUICClient(conn), testServiceName, "LotsOfGreetings", func() *testMessage { return &testMessage{} }, WithTimeout(50*time.Millisecond), WithMetadata("authorization", []byte("Bearer "+testAuthToken)))
	if err != nil {
		t.Fatalf("start client stream: %v", err)
	}
	time.Sleep(60 * time.Millisecond)
	_, err = stream.CloseAndRecv()
	if code := StatusFromError(err).Code; code != CodeDeadlineExceeded {
		t.Fatalf("expected deadline exceeded, got %v (%v)", code, err)
	}
}

func TestWebTransportClientStreamingDeadlineWhileUploadPending(t *testing.T) {
	running := startTestWebTransportServer(t, func(server *Server) {
		server.SetAuthorizer(BearerAuthorizer(testAuthToken))
	})
	transport := connectTestWebTransportClient(t, running)
	defer transport.Session().CloseWithError(cancelledWebTransportSessionCode, "test complete")

	stream, err := ClientStreaming[*testMessage, *testMessage](context.Background(), transport, testServiceName, "LotsOfGreetings", func() *testMessage { return &testMessage{} }, WithTimeout(50*time.Millisecond), WithMetadata("authorization", []byte("Bearer "+testAuthToken)))
	if err != nil {
		t.Fatalf("start WebTransport client stream: %v", err)
	}
	time.Sleep(60 * time.Millisecond)
	_, err = stream.CloseAndRecv()
	if code := StatusFromError(err).Code; code != CodeDeadlineExceeded {
		t.Fatalf("expected deadline exceeded, got %v (%v)", code, err)
	}
}

func TestQuicBidirectionalContextCancelWhileUploadAndResponsePending(t *testing.T) {
	running := startTestQUICServer(t, func(server *Server) {
		server.SetAuthorizer(BearerAuthorizer(testAuthToken))
	})
	conn := connectTestQUICClient(t, running)
	defer conn.CloseWithError(0, "test complete")
	ctx, cancel := context.WithCancel(context.Background())

	replies, err := BidirectionalStreaming[*testMessage, *testMessage](ctx, Advanced.NewRawQUICClient(conn), testServiceName, "BidiHello", func() *testMessage { return &testMessage{} }, authenticatedOptions()...)
	if err != nil {
		t.Fatalf("start bidi stream: %v", err)
	}
	cancel()
	_, err = replies.Recv()
	if code := StatusFromError(err).Code; code != CodeCancelled {
		t.Fatalf("expected cancelled, got %v (%v)", code, err)
	}
}

func TestWebTransportBidirectionalContextCancelWhileUploadAndResponsePending(t *testing.T) {
	running := startTestWebTransportServer(t, func(server *Server) {
		server.SetAuthorizer(BearerAuthorizer(testAuthToken))
	})
	transport := connectTestWebTransportClient(t, running)
	defer transport.Session().CloseWithError(cancelledWebTransportSessionCode, "test complete")
	ctx, cancel := context.WithCancel(context.Background())

	replies, err := BidirectionalStreaming[*testMessage, *testMessage](ctx, transport, testServiceName, "BidiHello", func() *testMessage { return &testMessage{} }, authenticatedOptions()...)
	if err != nil {
		t.Fatalf("start WebTransport bidi stream: %v", err)
	}
	cancel()
	_, err = replies.Recv()
	if code := StatusFromError(err).Code; code != CodeCancelled {
		t.Fatalf("expected cancelled, got %v (%v)", code, err)
	}
}

func TestQuicTerminalStatusClosesPendingRequestStream(t *testing.T) {
	running := startTestQUICServer(t, registerRejectUploadRoute)
	conn := connectTestQUICClient(t, running)
	defer conn.CloseWithError(0, "test complete")

	replies, err := BidirectionalStreaming[*testMessage, *testMessage](context.Background(), Advanced.NewRawQUICClient(conn), testServiceName, "RejectUpload", func() *testMessage { return &testMessage{} }, WithTimeout(testTimeout))
	if err != nil {
		t.Fatalf("start rejecting bidi stream: %v", err)
	}
	_, err = replies.Recv()
	if code := StatusFromError(err).Code; code != CodePermissionDenied {
		t.Fatalf("expected permission denied, got %v (%v)", code, err)
	}
}

func TestQuicTerminalOKSurfacesLocalUploadError(t *testing.T) {
	firstReceived := make(chan struct{})
	releaseResponse := make(chan struct{})
	running := startTestQUICServer(t, func(server *Server) {
		registerAcceptAfterUploadErrorRoute(server, firstReceived, releaseResponse)
	})
	conn := connectTestQUICClient(t, running)
	defer conn.CloseWithError(0, "test complete")

	replies, err := BidirectionalStreaming[*testMessage, *testMessage](context.Background(), Advanced.NewRawQUICClient(conn), testServiceName, "AcceptAfterUploadError", func() *testMessage { return &testMessage{} }, WithTimeout(testTimeout))
	if err != nil {
		t.Fatalf("start accepting bidi stream: %v", err)
	}
	if err := replies.Send(&testMessage{Value: "uploaded"}); err != nil {
		t.Fatalf("send request: %v", err)
	}
	<-firstReceived
	close(releaseResponse)
	if err := replies.CloseSend(); err != nil {
		t.Fatalf("close request stream: %v", err)
	}
	_, err = replies.Recv()
	if err != io.EOF {
		t.Fatalf("expected EOF, got %v", err)
	}
}

func TestWebTransportTerminalStatusClosesPendingRequestStream(t *testing.T) {
	running := startTestWebTransportServer(t, registerRejectUploadRoute)
	transport := connectTestWebTransportClient(t, running)
	defer transport.Session().CloseWithError(cancelledWebTransportSessionCode, "test complete")

	replies, err := BidirectionalStreaming[*testMessage, *testMessage](context.Background(), transport, testServiceName, "RejectUpload", func() *testMessage { return &testMessage{} }, WithTimeout(testTimeout))
	if err != nil {
		t.Fatalf("start rejecting WebTransport bidi stream: %v", err)
	}
	_, err = replies.Recv()
	if code := StatusFromError(err).Code; code != CodePermissionDenied {
		t.Fatalf("expected permission denied, got %v (%v)", code, err)
	}
}

func TestWebTransportTerminalOKSurfacesLocalUploadError(t *testing.T) {
	firstReceived := make(chan struct{})
	releaseResponse := make(chan struct{})
	running := startTestWebTransportServer(t, func(server *Server) {
		registerAcceptAfterUploadErrorRoute(server, firstReceived, releaseResponse)
	})
	transport := connectTestWebTransportClient(t, running)
	defer transport.Session().CloseWithError(cancelledWebTransportSessionCode, "test complete")

	replies, err := BidirectionalStreaming[*testMessage, *testMessage](context.Background(), transport, testServiceName, "AcceptAfterUploadError", func() *testMessage { return &testMessage{} }, WithTimeout(testTimeout))
	if err != nil {
		t.Fatalf("start accepting WebTransport bidi stream: %v", err)
	}
	if err := replies.Send(&testMessage{Value: "uploaded"}); err != nil {
		t.Fatalf("send request: %v", err)
	}
	<-firstReceived
	close(releaseResponse)
	if err := replies.CloseSend(); err != nil {
		t.Fatalf("close request stream: %v", err)
	}
	_, err = replies.Recv()
	if err != io.EOF {
		t.Fatalf("expected EOF, got %v", err)
	}
}

func TestQuicShutdownClosesActiveConnections(t *testing.T) {
	running := startTestQUICServer(t, func(*Server) {})
	conn := connectTestQUICClient(t, running)
	transport := Advanced.NewRawQUICClient(conn)
	running.stop(t)
	select {
	case <-conn.Context().Done():
	case <-time.After(testTimeout):
		t.Fatal("client did not observe server shutdown")
	}

	_, err := Unary(context.Background(), transport, testServiceName, "SayHello", &testMessage{Value: "after shutdown"}, func() *testMessage { return &testMessage{} }, WithTimeout(100*time.Millisecond))
	if code := StatusFromError(err).Code; code != CodeUnavailable {
		t.Fatalf("expected unavailable, got %v (%v)", code, err)
	}
	conn.CloseWithError(0, "test complete")
}

func TestWebTransportShutdownClosesActiveSessions(t *testing.T) {
	running := startTestWebTransportServer(t, func(*Server) {})
	transport := connectTestWebTransportClient(t, running)
	running.stop(t)
	select {
	case <-transport.Session().Context().Done():
	case <-time.After(testTimeout):
		t.Fatal("client did not observe WebTransport server shutdown")
	}

	_, err := Unary(context.Background(), transport, testServiceName, "SayHello", &testMessage{Value: "after shutdown"}, func() *testMessage { return &testMessage{} }, WithTimeout(100*time.Millisecond))
	if code := StatusFromError(err).Code; code != CodeUnavailable && code != CodeCancelled {
		t.Fatalf("expected unavailable or cancelled, got %v (%v)", code, err)
	}
	transport.Session().CloseWithError(cancelledWebTransportSessionCode, "test complete")
}

func TestQuicShutdownCancelsActiveUnaryHandler(t *testing.T) {
	started := make(chan struct{})
	cancelled := make(chan struct{})
	running := startTestQUICServer(t, func(server *Server) {
		server.Route(testServiceName, "ObserveCancel", func(ctx context.Context, _ []byte) ([]byte, error) {
			close(started)
			<-ctx.Done()
			close(cancelled)
			return nil, ctx.Err()
		})
	})
	conn := connectTestQUICClient(t, running)
	defer conn.CloseWithError(0, "test complete")
	done := make(chan error, 1)
	go func() {
		_, err := Unary(context.Background(), Advanced.NewRawQUICClient(conn), testServiceName, "ObserveCancel", &testMessage{}, func() *testMessage { return &testMessage{} }, WithTimeout(testTimeout))
		done <- err
	}()

	select {
	case <-started:
	case <-time.After(testTimeout):
		t.Fatal("handler did not start")
	}
	running.stop(t)
	select {
	case <-cancelled:
	case <-time.After(testTimeout):
		t.Fatal("handler did not observe shutdown cancellation")
	}
	select {
	case <-done:
	case <-time.After(testTimeout):
		t.Fatal("client call did not finish after shutdown")
	}
}

func TestWebTransportShutdownCancelsActiveUnaryHandler(t *testing.T) {
	started := make(chan struct{})
	cancelled := make(chan struct{})
	running := startTestWebTransportServer(t, func(server *Server) {
		options := server.Options()
		options.GracefulShutdownTimeout = 50 * time.Millisecond
		server.SetOptions(options)
		server.Route(testServiceName, "ObserveCancel", func(ctx context.Context, _ []byte) ([]byte, error) {
			close(started)
			<-ctx.Done()
			close(cancelled)
			return nil, ctx.Err()
		})
	})
	transport := connectTestWebTransportClient(t, running)
	defer transport.Session().CloseWithError(cancelledWebTransportSessionCode, "test complete")
	done := make(chan error, 1)
	go func() {
		_, err := Unary(context.Background(), transport, testServiceName, "ObserveCancel", &testMessage{}, func() *testMessage { return &testMessage{} }, WithTimeout(testTimeout))
		done <- err
	}()

	select {
	case <-started:
	case <-time.After(testTimeout):
		t.Fatal("WebTransport handler did not start")
	}
	running.stop(t)
	select {
	case <-cancelled:
	case <-time.After(testTimeout):
		t.Fatal("WebTransport handler did not observe shutdown cancellation")
	}
	select {
	case err := <-done:
		if code := StatusFromError(err).Code; code != CodeCancelled && code != CodeUnavailable && code != CodeDeadlineExceeded {
			t.Fatalf("expected cancelled, unavailable, or deadline exceeded, got %v (%v)", code, err)
		}
	case <-time.After(testTimeout):
		t.Fatal("WebTransport client call did not finish after shutdown")
	}
}

func TestQuicRequestPermitReleasedAfterDeadline(t *testing.T) {
	running := startTestQUICServer(t, func(server *Server) {
		options := DefaultServerOptions()
		options.MaxConcurrentRequests = 1
		server.SetOptions(options)
		server.Route(testServiceName, "UntilCancelled", func(ctx context.Context, _ []byte) ([]byte, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		})
	})
	conn := connectTestQUICClient(t, running)
	defer conn.CloseWithError(0, "test complete")
	transport := Advanced.NewRawQUICClient(conn)

	_, err := Unary(context.Background(), transport, testServiceName, "UntilCancelled", &testMessage{}, func() *testMessage { return &testMessage{} }, WithTimeout(50*time.Millisecond))
	if code := StatusFromError(err).Code; code != CodeDeadlineExceeded {
		t.Fatalf("expected deadline exceeded, got %v (%v)", code, err)
	}
	response, err := Unary(context.Background(), transport, testServiceName, "SayHello", &testMessage{Value: "after deadline"}, func() *testMessage { return &testMessage{} }, authenticatedOptions()...)
	if err != nil {
		t.Fatalf("second RPC should acquire released request permit: %v", err)
	}
	if response.Value != "hello, after deadline" {
		t.Fatalf("unexpected response after permit release: %q", response.Value)
	}
}

func TestQuicNonCooperativeHandlerRetainsRequestPermit(t *testing.T) {
	releaseHandler := make(chan struct{})
	exited := make(chan struct{})
	running := startTestQUICServer(t, func(server *Server) {
		server.SetOptions(ServerOptions{MaxConcurrentRequests: 1})
		server.Route(testServiceName, "NonCooperative", func(context.Context, []byte) ([]byte, error) { <-releaseHandler; close(exited); return nil, nil })
	})
	conn := connectTestQUICClient(t, running)
	defer conn.CloseWithError(0, "test complete")
	transport := Advanced.NewRawQUICClient(conn)
	request := NewRpcRequest(testServiceName, "NonCooperative", nil)
	request.TimeoutNanos = uint64(50 * time.Millisecond)
	response, err := transport.Call(context.Background(), request)
	if err != nil {
		t.Fatalf("deadline call transport: %v", err)
	}
	if CodeFromUint32(response.Status) != CodeDeadlineExceeded {
		t.Fatalf("deadline response = %#v", response)
	}

	response, err = transport.Call(context.Background(), NewRpcRequest(testServiceName, "SayHello", mustEncodeTestMessage(t, "bounded")))
	if err != nil {
		t.Fatalf("bounded call transport: %v", err)
	}
	if CodeFromUint32(response.Status) != CodeUnavailable {
		t.Fatalf("non-cooperative handler released global admission: %#v", response)
	}
	close(releaseHandler)
	select {
	case <-exited:
	case <-time.After(testTimeout):
		t.Fatal("non-cooperative handler did not exit")
	}
	waitForServerExecutionCount(t, running.server, 0)
	response, err = transport.Call(context.Background(), NewRpcRequest(testServiceName, "SayHello", mustEncodeTestMessage(t, "released")))
	if err != nil {
		t.Fatalf("call after handler exit: %v", err)
	}
	if CodeFromUint32(response.Status) != CodeOK {
		t.Fatalf("call after handler exit = %#v", response)
	}
}

func TestQuicLocalCloseMapsToCancelled(t *testing.T) {
	running := startTestQUICServer(t, func(*Server) {})
	conn := connectTestQUICClient(t, running)
	conn.CloseWithError(0, "client closed")

	_, err := Unary(context.Background(), Advanced.NewRawQUICClient(conn), testServiceName, "SayHello", &testMessage{Value: "after local close"}, func() *testMessage { return &testMessage{} }, WithTimeout(100*time.Millisecond))
	if code := StatusFromError(err).Code; code != CodeCancelled {
		t.Fatalf("expected cancelled, got %v (%v)", code, err)
	}
}

func TestQuicRejectsALPNMismatch(t *testing.T) {
	running := startTestQUICServer(t, func(*Server) {})
	clientTLS := running.clientTLS.Clone()
	clientTLS.NextProtos = []string{"not-trevrpc"}

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()
	conn, err := quic.DialAddr(ctx, running.addr, clientTLS, &quic.Config{})
	if err == nil {
		conn.CloseWithError(0, "test complete")
		t.Fatal("ALPN mismatch should reject connection")
	}
}

func TestQuicRejectsTLSIdentityMismatch(t *testing.T) {
	running := startTestQUICServer(t, func(*Server) {})
	clientTLS := running.clientTLS.Clone()
	clientTLS.ServerName = "wronghost"

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()
	conn, err := quic.DialAddr(ctx, running.addr, clientTLS, &quic.Config{})
	if err == nil {
		conn.CloseWithError(0, "test complete")
		t.Fatal("identity mismatch should reject connection")
	}
}

func TestQuicMTLSRejectsClientsWithoutCertificates(t *testing.T) {
	serverTLS, clientTLS := testTLSConfig(t)
	serverTLS.ClientAuth = tls.RequireAnyClientCert
	running := startTestQUICServerWithTLS(t, serverTLS, clientTLS, func(*Server) {})

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()
	conn, err := quic.DialAddr(ctx, running.addr, running.clientTLS, &quic.Config{})
	if err == nil {
		_, rpcErr := Unary(context.Background(), Advanced.NewRawQUICClient(conn), testServiceName, "SayHello", &testMessage{Value: "anonymous"}, func() *testMessage { return &testMessage{} }, WithTimeout(100*time.Millisecond))
		conn.CloseWithError(0, "test complete")
		if code := StatusFromError(rpcErr).Code; code != CodeUnavailable {
			t.Fatalf("expected unavailable for anonymous mTLS client, got %v (%v)", code, rpcErr)
		}
	}
}

func TestQuicMalformedRequestFramesReturnInvalidArgumentStatus(t *testing.T) {
	metrics := &recordingMetrics{}
	running := startTestQUICServer(t, func(server *Server) {
		server.SetMetrics(metrics)
	})
	conn := connectTestQUICClient(t, running)
	defer conn.CloseWithError(0, "test complete")
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()
	stream, err := conn.OpenStreamSync(ctx)
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	if _, err := stream.Write([]byte{0, 0, 0, 2, 0xff, 0xff}); err != nil {
		t.Fatalf("write malformed frame: %v", err)
	}
	if err := stream.Close(); err != nil {
		t.Fatalf("close malformed stream: %v", err)
	}

	response := &RpcResponse{}
	if err := ReadFrame(stream, response, DefaultMaxFrameSize); err != nil {
		t.Fatalf("read malformed response: %v", err)
	}
	if CodeFromUint32(response.Status) != CodeInvalidArgument {
		t.Fatalf("expected invalid argument status, got %#v", response)
	}
	expectPreHandlerMetrics(t, metrics, CodeInvalidArgument)
}

func TestWebTransportMalformedRequestFramesReturnInvalidArgumentStatus(t *testing.T) {
	metrics := &recordingMetrics{}
	running := startTestWebTransportServer(t, func(server *Server) {
		server.SetMetrics(metrics)
	})
	transport := connectTestWebTransportClient(t, running)
	defer transport.Session().CloseWithError(cancelledWebTransportSessionCode, "test complete")
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()
	stream, err := transport.Session().OpenStreamSync(ctx)
	if err != nil {
		t.Fatalf("open WebTransport stream: %v", err)
	}
	defer stream.CancelRead(cancelledWebTransportStreamCode)
	defer stream.CancelWrite(cancelledWebTransportStreamCode)
	if _, err := stream.Write([]byte{0, 0, 0, 2, 0xff, 0xff}); err != nil {
		t.Fatalf("write malformed WebTransport frame: %v", err)
	}
	if err := stream.Close(); err != nil {
		t.Fatalf("close malformed WebTransport stream: %v", err)
	}

	response := readRawTestStreamResponse(t, stream)

	if CodeFromUint32(response.Status) != CodeInvalidArgument {
		t.Fatalf("expected invalid argument status, got %#v", response)
	}
	expectPreHandlerMetrics(t, metrics, CodeInvalidArgument)
}

func TestQuicMalformedRequestStreamMessageReturnsInvalidArgumentStatus(t *testing.T) {
	running := startTestQUICServer(t, func(*Server) {})
	stream := openRawTestQUICStream(t, running)
	defer stream.CancelRead(cancelledStreamCode)
	defer stream.CancelWrite(cancelledStreamCode)

	request := NewRpcRequest(testServiceName, "LotsOfGreetings", nil)
	request.Kind = RpcKindClientStreaming
	if err := WriteFrame(stream, request, DefaultMaxFrameSize); err != nil {
		t.Fatalf("write initial request: %v", err)
	}
	if err := WriteFrame(stream, MessageFrame([]byte{0xff, 0xff}), DefaultMaxFrameSize); err != nil {
		t.Fatalf("write malformed message frame: %v", err)
	}
	if err := stream.Close(); err != nil {
		t.Fatalf("close malformed stream: %v", err)
	}

	frame := &RpcStreamFrame{}
	if err := stream.SetReadDeadline(time.Now().Add(testTimeout)); err != nil {
		t.Fatalf("set stream read deadline: %v", err)
	}
	defer stream.SetReadDeadline(time.Time{})
	if err := ReadFrame(stream, frame, DefaultMaxFrameSize); err != nil {
		t.Fatalf("read status frame: %v", err)
	}
	if frame.Kind != RpcStreamFrameKindStatus || CodeFromUint32(frame.Status) != CodeInvalidArgument {
		t.Fatalf("expected invalid argument status, got %#v", frame)
	}
}

func TestQuicInitialRequestTimeoutRejectsPartialHeader(t *testing.T) {
	metrics := &recordingMetrics{}
	running := startTestQUICServer(t, func(server *Server) {
		options := DefaultServerOptions()
		options.InitialRequestTimeout = 50 * time.Millisecond
		options.GracefulShutdownTimeout = 50 * time.Millisecond
		server.SetOptions(options)
		server.SetMetrics(metrics)
	})
	stream := openRawTestQUICStream(t, running)
	defer stream.CancelRead(cancelledStreamCode)
	defer stream.CancelWrite(cancelledStreamCode)
	if _, err := stream.Write([]byte{0, 0}); err != nil {
		t.Fatalf("write partial header: %v", err)
	}

	response := readRawTestQUICResponse(t, stream)

	if CodeFromUint32(response.Status) != CodeDeadlineExceeded {
		t.Fatalf("expected deadline exceeded status, got %#v", response)
	}
	expectPreHandlerMetrics(t, metrics, CodeDeadlineExceeded)
}

func TestQuicInitialRequestTimeoutRejectsPartialBody(t *testing.T) {
	metrics := &recordingMetrics{}
	running := startTestQUICServer(t, func(server *Server) {
		options := DefaultServerOptions()
		options.InitialRequestTimeout = 50 * time.Millisecond
		options.GracefulShutdownTimeout = 50 * time.Millisecond
		server.SetOptions(options)
		server.SetMetrics(metrics)
	})
	stream := openRawTestQUICStream(t, running)
	defer stream.CancelRead(cancelledStreamCode)
	defer stream.CancelWrite(cancelledStreamCode)
	header := make([]byte, 4)
	binary.BigEndian.PutUint32(header, 8)
	if _, err := stream.Write(append(header, 1)); err != nil {
		t.Fatalf("write partial body: %v", err)
	}

	response := readRawTestQUICResponse(t, stream)

	if CodeFromUint32(response.Status) != CodeDeadlineExceeded {
		t.Fatalf("expected deadline exceeded status, got %#v", response)
	}
	expectPreHandlerMetrics(t, metrics, CodeDeadlineExceeded)
}

func TestWebTransportInitialRequestTimeoutRejectsPartialHeader(t *testing.T) {
	metrics := &recordingMetrics{}
	running := startTestWebTransportServer(t, func(server *Server) {
		options := DefaultServerOptions()
		options.InitialRequestTimeout = 50 * time.Millisecond
		options.GracefulShutdownTimeout = 50 * time.Millisecond
		server.SetOptions(options)
		server.SetMetrics(metrics)
	})
	transport := connectTestWebTransportClient(t, running)
	defer transport.Session().CloseWithError(cancelledWebTransportSessionCode, "test complete")
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()
	stream, err := transport.Session().OpenStreamSync(ctx)
	if err != nil {
		t.Fatalf("open partial WebTransport stream: %v", err)
	}
	defer stream.CancelRead(cancelledWebTransportStreamCode)
	defer stream.CancelWrite(cancelledWebTransportStreamCode)
	if _, err := stream.Write([]byte{0, 0}); err != nil {
		t.Fatalf("write WebTransport partial header: %v", err)
	}

	response := readRawTestStreamResponse(t, stream)

	if CodeFromUint32(response.Status) != CodeDeadlineExceeded {
		t.Fatalf("expected deadline exceeded status, got %#v", response)
	}
	expectPreHandlerMetrics(t, metrics, CodeDeadlineExceeded)
}

func TestServerTransportReadsCancelWithContext(t *testing.T) {
	server := NewServer()
	options := DefaultServerOptions()
	options.InitialRequestTimeout = time.Minute
	server.SetOptions(options)

	tests := []struct {
		name string
		read func(context.Context, rpcStream) error
	}{
		{
			name: "initial request",
			read: func(ctx context.Context, stream rpcStream) error {
				return readInitialRequestFrame(ctx, server.Options(), time.Now(), stream, &RpcRequest{})
			},
		},
		{
			name: "finite request end",
			read: func(ctx context.Context, stream rpcStream) error {
				return drainRequestEnd(ctx, server.Options(), time.Now(), stream)
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			stream := newContextCancellableRPCStream()
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan error, 1)
			go func() { done <- test.read(ctx, stream) }()

			select {
			case <-stream.readStarted:
			case <-time.After(testTimeout):
				t.Fatal("transport read did not start")
			}
			cancel()

			select {
			case err := <-done:
				if code := StatusFromError(err).Code; !errors.Is(err, context.Canceled) && code != CodeCancelled {
					t.Fatalf("expected cancelled, got %v (%v)", code, err)
				}
			case <-time.After(testTimeout):
				t.Fatal("transport read did not stop after context cancellation")
			}
		})
	}
}

func TestQuicLargePartialInitialBodyDoesNotHoldRequestPermit(t *testing.T) {
	const largeFrameSize = 1 << 30
	running := startTestQUICServer(t, func(server *Server) {
		options := DefaultServerOptions()
		options.MaxFrameSize = largeFrameSize
		options.MaxConcurrentRequests = 1
		options.InitialRequestTimeout = time.Minute
		server.SetOptions(options)
	})
	conn := connectTestQUICClient(t, running)
	defer conn.CloseWithError(0, "test complete")
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()
	stream, err := conn.OpenStreamSync(ctx)
	if err != nil {
		t.Fatalf("open partial stream: %v", err)
	}
	defer stream.CancelRead(cancelledStreamCode)
	defer stream.CancelWrite(cancelledStreamCode)
	header := make([]byte, 4)
	binary.BigEndian.PutUint32(header, largeFrameSize)
	if _, err := stream.Write(append(header, 1)); err != nil {
		t.Fatalf("write partial large body: %v", err)
	}

	response, err := Unary(context.Background(), Advanced.NewRawQUICClient(conn), testServiceName, "SayHello", &testMessage{Value: "after partial"}, func() *testMessage { return &testMessage{} }, WithTimeout(testTimeout))
	if err != nil {
		t.Fatalf("request permit should not be held before initial frame completes: %v", err)
	}
	if response.Value != "hello, after partial" {
		t.Fatalf("unexpected response after partial initial frame: %q", response.Value)
	}
	running.stop(t)
}

func TestQuicOversizedInitialFrameIsRejectedBeforeBody(t *testing.T) {
	running := startTestQUICServerWithInitialRequestTimeout(t, testTimeout)
	stream := openRawTestQUICStream(t, running)
	defer stream.CancelRead(cancelledStreamCode)
	defer stream.CancelWrite(cancelledStreamCode)
	header := make([]byte, 4)
	binary.BigEndian.PutUint32(header, uint32(DefaultMaxFrameSize+1))
	if _, err := stream.Write(header); err != nil {
		t.Fatalf("write oversized frame header: %v", err)
	}

	response := readRawTestQUICResponse(t, stream)

	if CodeFromUint32(response.Status) != CodeResourceExhausted {
		t.Fatalf("expected resource exhausted status, got %#v", response)
	}
}

func TestQuicHandlesManyConcurrentUnaryCalls(t *testing.T) {
	running := startTestQUICServer(t, func(server *Server) {
		server.SetAuthorizer(BearerAuthorizer(testAuthToken))
	})
	conn := connectTestQUICClient(t, running)
	defer conn.CloseWithError(0, "test complete")
	transport := Advanced.NewRawQUICClient(conn)

	var group sync.WaitGroup
	errors := make(chan error, 64)
	for i := range 64 {
		group.Add(1)
		go func(index int) {
			defer group.Done()
			name := fmt.Sprintf("concurrent-%d", index)
			response, err := Unary(context.Background(), transport, testServiceName, "SayHello", &testMessage{Value: name}, func() *testMessage { return &testMessage{} }, authenticatedOptions()...)
			if err != nil {
				errors <- err
				return
			}
			if response.Value != "hello, "+name {
				errors <- fmt.Errorf("unexpected response %q", response.Value)
			}
		}(i)
	}
	group.Wait()
	close(errors)
	for err := range errors {
		t.Fatal(err)
	}
}

func TestQuicHandlesBoundedMixedLoad(t *testing.T) {
	running := startTestQUICServer(t, func(server *Server) {
		options := DefaultServerOptions()
		options.MaxConcurrentStreamsPerConnection = 512
		options.MaxConcurrentRequests = 1024
		server.SetOptions(options)
		server.SetAuthorizer(BearerAuthorizer(testAuthToken))
	})
	conn := connectTestQUICClient(t, running)
	defer conn.CloseWithError(0, "test complete")
	transport := Advanced.NewRawQUICClient(conn)

	var group sync.WaitGroup
	errors := make(chan error, 64)
	for i := range 64 {
		group.Add(1)
		go func(index int) {
			defer group.Done()
			if err := runMixedQUICCall(transport, index); err != nil {
				errors <- err
			}
		}(i)
	}
	group.Wait()
	close(errors)
	for err := range errors {
		t.Fatal(err)
	}
}

const testServiceName = "example.Greeter"

type runningTestQUICServer struct {
	addr      string
	clientTLS *tls.Config
	server    *Server
	cancel    context.CancelFunc
	done      chan error
}

func startTestQUICServer(t *testing.T, configure func(*Server)) *runningTestQUICServer {
	t.Helper()
	serverTLS, clientTLS := testTLSConfig(t)
	return startTestQUICServerWithTLS(t, serverTLS, clientTLS, configure)
}

func startTestQUICServerWithInitialRequestTimeout(t *testing.T, timeout time.Duration) *runningTestQUICServer {
	t.Helper()
	return startTestQUICServer(t, func(server *Server) {
		options := DefaultServerOptions()
		options.InitialRequestTimeout = timeout
		options.GracefulShutdownTimeout = 50 * time.Millisecond
		server.SetOptions(options)
	})
}

func startTestQUICServerWithTLS(t *testing.T, serverTLS, clientTLS *tls.Config, configure func(*Server)) *runningTestQUICServer {
	t.Helper()
	server := NewServer()
	registerTestGreeter(server)
	configure(server)

	listener, err := quic.ListenAddr("127.0.0.1:0", serverTLS, QUICServerConfig(server.Options(), nil))
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- ServeQUIC(ctx, listener, server)
	}()

	running := &runningTestQUICServer{
		addr:      listener.Addr().String(),
		clientTLS: clientTLS,
		server:    server,
		cancel:    cancel,
		done:      done,
	}
	t.Cleanup(func() {
		running.stop(t)
	})

	return running
}

func startTestWebTransportServer(t *testing.T, configure func(*Server)) *runningTestQUICServer {
	t.Helper()
	return startTestWebTransportServerWithAdmission(t, func(request WebTransportAdmissionRequest) bool {
		return request.Path == "/trevrpc"
	}, configure)
}

func startTestWebTransportServerWithAdmission(t *testing.T, admission WebTransportAdmission, configure func(*Server)) *runningTestQUICServer {
	t.Helper()
	serverTLS, clientTLS := testTLSConfig(t)
	serverTLS.NextProtos = []string{http3.NextProtoH3}
	clientTLS.NextProtos = []string{http3.NextProtoH3}

	server := NewServer()
	registerTestGreeter(server)
	configure(server)
	options := server.Options()
	options.EnableWebTransport = true
	options.WebTransportAdmission = admission
	server.SetOptions(options)

	listener, err := quic.ListenAddr("127.0.0.1:0", serverTLS, QUICServerConfig(server.Options(), nil))
	if err != nil {
		t.Fatalf("listen WebTransport: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- ServeQUIC(ctx, listener, server)
	}()

	running := &runningTestQUICServer{
		addr:      listener.Addr().String(),
		clientTLS: clientTLS,
		server:    server,
		cancel:    cancel,
		done:      done,
	}
	t.Cleanup(func() {
		running.stop(t)
	})

	return running
}

func (s *runningTestQUICServer) stop(t *testing.T) {
	t.Helper()
	if s.cancel == nil {
		return
	}

	s.cancel()
	s.cancel = nil
	select {
	case err := <-s.done:
		if err != nil {
			t.Fatalf("serve QUIC: %v", err)
		}
	case <-time.After(testTimeout):
		t.Fatal("server did not shut down")
	}
}

func connectTestQUICClient(t *testing.T, running *runningTestQUICServer) *quic.Conn {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()
	conn, err := quic.DialAddr(ctx, running.addr, running.clientTLS.Clone(), QUICClientConfig(DefaultMaxFrameSize, nil))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}

	return conn
}

func configureIdleTimeoutAdmissionTestServer(server *Server) {
	options := DefaultServerOptions()
	options.MaxConcurrentRequests = 1
	options.StreamIdleTimeout = 30 * time.Millisecond
	options.GracefulShutdownTimeout = 100 * time.Millisecond
	server.SetOptions(options)
}

func writeIdleTimeoutRequest(t *testing.T, stream io.Writer) {
	t.Helper()
	request := NewRpcRequest(testServiceName, "LotsOfGreetings", nil)
	request.Kind = RpcKindClientStreaming
	if err := WriteFrame(stream, request, DefaultMaxFrameSize); err != nil {
		t.Fatalf("write idle timeout request: %v", err)
	}
}

type rawTestStatusStream interface {
	io.Reader
	SetReadDeadline(time.Time) error
}

func assertIdleTimeoutStatus(t *testing.T, stream rawTestStatusStream) {
	t.Helper()
	if err := stream.SetReadDeadline(time.Now().Add(testTimeout)); err != nil {
		t.Fatalf("set idle timeout response deadline: %v", err)
	}
	defer stream.SetReadDeadline(time.Time{})
	frame := &RpcStreamFrame{}
	if err := ReadFrame(stream, frame, DefaultMaxFrameSize); err != nil {
		t.Fatalf("read idle timeout status: %v", err)
	}
	if frame.Kind != RpcStreamFrameKindStatus || CodeFromUint32(frame.Status) != CodeUnavailable {
		t.Fatalf("idle timeout frame = %#v, want unavailable status", frame)
	}
}

func assertUnaryAdmissionRecovered(t *testing.T, transport Transport) {
	t.Helper()
	response, err := Unary(context.Background(), transport, testServiceName, "SayHello", &testMessage{Value: "after idle timeout"}, func() *testMessage { return &testMessage{} }, WithTimeout(testTimeout))
	if err != nil {
		t.Fatalf("unary after idle timeout: %v", err)
	}
	if response.Value != "hello, after idle timeout" {
		t.Fatalf("unary response after idle timeout = %q", response.Value)
	}
}

func openRawTestQUICStream(t *testing.T, running *runningTestQUICServer) *quic.Stream {
	t.Helper()
	conn := connectTestQUICClient(t, running)
	t.Cleanup(func() { conn.CloseWithError(0, "test complete") })
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()
	stream, err := conn.OpenStreamSync(ctx)
	if err != nil {
		t.Fatalf("open raw stream: %v", err)
	}

	return stream
}

func readRawTestQUICResponse(t *testing.T, stream *quic.Stream) *RpcResponse {
	t.Helper()
	return readRawTestStreamResponse(t, stream)
}

type rawTestResponseStream interface {
	io.Reader
	SetReadDeadline(time.Time) error
}

func readRawTestStreamResponse(t *testing.T, stream rawTestResponseStream) *RpcResponse {
	t.Helper()
	if err := stream.SetReadDeadline(time.Now().Add(testTimeout)); err != nil {
		t.Fatalf("set raw stream read deadline: %v", err)
	}
	defer stream.SetReadDeadline(time.Time{})
	response := &RpcResponse{}
	if err := ReadFrame(stream, response, DefaultMaxFrameSize); err != nil {
		t.Fatalf("read raw response: %v", err)
	}

	return response
}

func connectTestWebTransportClient(t *testing.T, running *runningTestQUICServer) *RawWebTransportClient {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	transport, err := Advanced.DialRawWebTransport(ctx, "https://"+running.addr+"/trevrpc", RawWebTransportDialOptions{
		TLSClientConfig: running.clientTLS.Clone(),
		QUICConfig:      WebTransportQUICClientConfig(DefaultMaxFrameSize, nil),
	})
	if err != nil {
		t.Fatalf("dial WebTransport: %v", err)
	}

	return transport
}

func registerTestGreeter(server *Server) {
	RegisterUnary(server, testServiceName, "SayHello", func() *testMessage { return &testMessage{} }, func(_ context.Context, request *testMessage) (*testMessage, error) {
		return &testMessage{Value: "hello, " + request.Value}, nil
	})
	server.RouteStreaming(testServiceName, "LotsOfReplies", RpcKindServerStreaming, func(_ context.Context, body []byte, _ ByteStream) (ByteStream, error) {
		request := &testMessage{}
		if err := UnmarshalMessage(body, request); err != nil {
			return nil, err
		}
		if request.Value == "cancel" {
			return EncodeStream[*testMessage](&firstThenPendingTestMessages{first: &testMessage{Value: "first"}}), nil
		}

		return EncodeStream(FromSlice(
			&testMessage{Value: "hello, " + request.Value},
			&testMessage{Value: "goodbye, " + request.Value},
		)), nil
	})
	server.RouteStreaming(testServiceName, "LotsOfGreetings", RpcKindClientStreaming, func(_ context.Context, _ []byte, requests ByteStream) (ByteStream, error) {
		decoded := DecodeStream[*testMessage](requests, func() *testMessage { return &testMessage{} })
		var values []string
		for {
			request, err := decoded.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				return nil, err
			}

			values = append(values, request.Value)
		}

		return SingleMessageStream(&testMessage{Value: strings.Join(values, ",")}), nil
	})
	server.RouteStreaming(testServiceName, "BidiHello", RpcKindBidirectionalStreaming, func(_ context.Context, _ []byte, requests ByteStream) (ByteStream, error) {
		return EncodeStream[*testMessage](&echoTestMessages{requests: DecodeStream[*testMessage](requests, func() *testMessage { return &testMessage{} })}), nil
	})
}

func registerRejectUploadRoute(server *Server) {
	server.RouteStreaming(testServiceName, "RejectUpload", RpcKindBidirectionalStreaming, func(context.Context, []byte, ByteStream) (ByteStream, error) {
		return nil, NewStatus(CodePermissionDenied, "upload rejected")
	})
}

func registerAcceptAfterUploadErrorRoute(server *Server, firstReceived chan<- struct{}, uploadFailed <-chan struct{}) {
	server.RouteStreaming(testServiceName, "AcceptAfterUploadError", RpcKindBidirectionalStreaming, func(_ context.Context, _ []byte, requests ByteStream) (ByteStream, error) {
		if _, err := requests.Recv(); err != nil {
			close(firstReceived)
			return nil, err
		}

		close(firstReceived)
		<-uploadFailed
		return EmptyStream[[]byte](), nil
	})
}

type closeErrorFrameStream struct {
	frames []*RpcStreamFrame
	err    error
	closed int
}

type countingWriter struct {
	bytes.Buffer
	writeCount int
}

func (w *countingWriter) Write(data []byte) (int, error) {
	w.writeCount++
	return w.Buffer.Write(data)
}

type countingRPCStream struct {
	reader              *bytes.Reader
	written             bytes.Buffer
	writeErr            error
	readCount           int
	writeCount          int
	closeCount          int
	cancelReadCount     int
	cleanupOrder        []string
	readDeadlines       []time.Time
	readEOF             bool
	closedBeforeReadEOF bool
	closed              bool
}

func (s *countingRPCStream) Read(data []byte) (int, error) {
	s.readCount++
	read, err := s.reader.Read(data)
	if errors.Is(err, io.EOF) {
		s.readEOF = true
	}
	return read, err
}

func (s *countingRPCStream) Write(data []byte) (int, error) {
	s.writeCount++
	if s.writeErr != nil {
		return 0, s.writeErr
	}
	return s.written.Write(data)
}

func (s *countingRPCStream) SetReadDeadline(deadline time.Time) error {
	s.readDeadlines = append(s.readDeadlines, deadline)
	return nil
}

func (s *countingRPCStream) Close() error {
	s.closeCount++
	s.cleanupOrder = append(s.cleanupOrder, "close")
	s.closedBeforeReadEOF = !s.readEOF
	s.closed = true
	return nil
}

func (s *countingRPCStream) trevrpcCancelRead() {
	s.cancelReadCount++
	s.cleanupOrder = append(s.cleanupOrder, "cancel read")
}

type requestThenBlockingRPCStream struct {
	reader          *bytes.Reader
	written         bytes.Buffer
	readCancelled   chan struct{}
	cancelOnce      sync.Once
	cleanupMu       sync.Mutex
	cleanupOrder    []string
	cancelReadCount int
	closeCount      int
}

func newRequestThenBlockingRPCStream(request []byte) *requestThenBlockingRPCStream {
	return &requestThenBlockingRPCStream{
		reader:        bytes.NewReader(request),
		readCancelled: make(chan struct{}),
	}
}

func (s *requestThenBlockingRPCStream) Read(data []byte) (int, error) {
	if s.reader.Len() > 0 {
		return s.reader.Read(data)
	}
	<-s.readCancelled
	return 0, errors.New("transport read cancelled")
}

func (s *requestThenBlockingRPCStream) Write(data []byte) (int, error) {
	return s.written.Write(data)
}

func (s *requestThenBlockingRPCStream) Close() error {
	s.cleanupMu.Lock()
	s.closeCount++
	s.cleanupOrder = append(s.cleanupOrder, "close")
	s.cleanupMu.Unlock()
	return nil
}

func (s *requestThenBlockingRPCStream) trevrpcCancelRead() {
	s.cancelOnce.Do(func() {
		s.cleanupMu.Lock()
		s.cancelReadCount++
		s.cleanupOrder = append(s.cleanupOrder, "cancel read")
		s.cleanupMu.Unlock()
		close(s.readCancelled)
	})
}

func (s *requestThenBlockingRPCStream) trevrpcCancelReadOnContext(ctx context.Context) func() {
	done := make(chan struct{})
	var stopOnce sync.Once
	go func() {
		select {
		case <-ctx.Done():
			s.trevrpcCancelRead()
		case <-done:
		}
	}()
	return func() { stopOnce.Do(func() { close(done) }) }
}

func readStreamFramesFromBytes(t *testing.T, data []byte, maxFrameSize int) []*RpcStreamFrame {
	t.Helper()
	reader := bytes.NewReader(data)
	var frames []*RpcStreamFrame
	for {
		frame := &RpcStreamFrame{}
		read, err := ReadFrameOrEOF(reader, frame, maxFrameSize)
		if err != nil {
			t.Fatalf("read stream frame %d: %v", len(frames), err)
		}
		if !read {
			return frames
		}
		frames = append(frames, frame)
	}
}

type contextBlockingTransport struct{}

func (contextBlockingTransport) Call(ctx context.Context, _ *RpcRequest) (*RpcResponse, error) {
	<-ctx.Done()
	return nil, statusFromContextError(ctx.Err())
}

func (contextBlockingTransport) StreamingCall(ctx context.Context, _ *RpcRequest, _ ByteStream) (FrameStream, error) {
	return contextBlockingFrameStream{ctx: ctx}, nil
}

type contextBlockingFrameStream struct {
	ctx context.Context
}

func (s contextBlockingFrameStream) Recv() (*RpcStreamFrame, error) {
	<-s.ctx.Done()
	return nil, statusFromContextError(s.ctx.Err())
}

func (contextBlockingFrameStream) Close() error { return nil }

type contextCancellableRPCStream struct {
	readStarted   chan struct{}
	readCancelled chan struct{}
	startOnce     sync.Once
	cancelOnce    sync.Once
}

func newContextCancellableRPCStream() *contextCancellableRPCStream {
	return &contextCancellableRPCStream{
		readStarted:   make(chan struct{}),
		readCancelled: make(chan struct{}),
	}
}

func (s *contextCancellableRPCStream) Read([]byte) (int, error) {
	s.startOnce.Do(func() { close(s.readStarted) })
	<-s.readCancelled
	return 0, errors.New("transport read cancelled")
}

func (*contextCancellableRPCStream) Write(data []byte) (int, error) { return len(data), nil }
func (*contextCancellableRPCStream) Close() error                   { return nil }

func (s *contextCancellableRPCStream) trevrpcCancelReadOnContext(ctx context.Context) func() {
	done := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			s.cancelOnce.Do(func() { close(s.readCancelled) })
		case <-done:
		}
	}()

	var stopOnce sync.Once
	return func() { stopOnce.Do(func() { close(done) }) }
}

type uploadWaitingTransport struct{}

func (uploadWaitingTransport) Call(context.Context, *RpcRequest) (*RpcResponse, error) {
	return nil, Unimplemented("unary not implemented")
}

func (uploadWaitingTransport) StreamingCall(ctx context.Context, _ *RpcRequest, requestBody ByteStream) (FrameStream, error) {
	errors := make(chan error, 1)
	go func() {
		_, err := recvRequestBody(ctx, requestBody)
		errors <- err
	}()

	return uploadErrorFrameStream{errors: errors}, nil
}

type uploadErrorFrameStream struct {
	errors <-chan error
}

type byteErrorStream struct {
	err error
}

func (s uploadErrorFrameStream) Recv() (*RpcStreamFrame, error) {
	return nil, <-s.errors
}

func (uploadErrorFrameStream) Close() error { return nil }

func (s byteErrorStream) Recv() ([]byte, error) {
	return nil, s.err
}

func (byteErrorStream) Close() error { return nil }

func (s *closeErrorFrameStream) Recv() (*RpcStreamFrame, error) {
	if len(s.frames) == 0 {
		return nil, io.EOF
	}

	frame := s.frames[0]
	s.frames = s.frames[1:]
	return frame, nil
}

func (s *closeErrorFrameStream) Close() error {
	s.closed++
	return s.err
}

type firstThenPendingTestMessages struct {
	first *testMessage
}

func (s *firstThenPendingTestMessages) Recv() (*testMessage, error) {
	if s.first != nil {
		first := s.first
		s.first = nil
		return first, nil
	}

	select {}
}

func (s *firstThenPendingTestMessages) Close() error {
	return nil
}

func authenticatedOptions() []CallOption {
	return []CallOption{WithTimeout(testTimeout), WithMetadata("authorization", []byte("Bearer "+testAuthToken))}
}

func collectTestMessages(t *testing.T, stream MessageStream[*testMessage]) []string {
	t.Helper()
	var messages []string
	for {
		message, err := stream.Recv()
		if err == io.EOF {
			return messages
		}
		if err != nil {
			t.Fatalf("receive stream message: %v", err)
		}

		messages = append(messages, message.Value)
	}
}

func equalStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}

	return true
}

func holdServerStreamOpen(t *testing.T, transport Transport) MessageStream[*testMessage] {
	t.Helper()
	replies, err := ServerStreaming(context.Background(), transport, testServiceName, "LotsOfReplies", &testMessage{Value: "cancel"}, func() *testMessage { return &testMessage{} }, authenticatedOptions()...)
	if err != nil {
		t.Fatalf("start hanging stream: %v", err)
	}
	first, err := replies.Recv()
	if err != nil {
		t.Fatalf("read first hanging response: %v", err)
	}
	if first.Value != "first" {
		t.Fatalf("unexpected first hanging response: %q", first.Value)
	}

	return replies
}

func waitForMetricCode(t *testing.T, metrics *recordingMetrics, code Code) {
	t.Helper()
	deadline := time.Now().Add(testTimeout)
	for time.Now().Before(deadline) {
		if metrics.hasCode(code) {
			return
		}
		time.Sleep(time.Millisecond)
	}

	t.Fatalf("metric code %v was not recorded", code)
}

func runMixedQUICCall(transport Transport, index int) error {
	switch index % 4 {
	case 0:
		name := fmt.Sprintf("load-unary-%d", index)
		response, err := Unary(context.Background(), transport, testServiceName, "SayHello", &testMessage{Value: name}, func() *testMessage { return &testMessage{} }, authenticatedOptions()...)
		if err != nil {
			return err
		}
		if response.Value != "hello, "+name {
			return fmt.Errorf("unexpected unary response %q", response.Value)
		}
	case 1:
		name := fmt.Sprintf("load-server-%d", index)
		responses, err := ServerStreaming(context.Background(), transport, testServiceName, "LotsOfReplies", &testMessage{Value: name}, func() *testMessage { return &testMessage{} }, authenticatedOptions()...)
		if err != nil {
			return err
		}
		messages := collectTestMessagesNoFatal(responses)
		if messages.err != nil {
			return messages.err
		}
		if !equalStrings(messages.values, []string{"hello, " + name, "goodbye, " + name}) {
			return fmt.Errorf("unexpected server stream responses %#v", messages.values)
		}
	case 2:
		response, err := runTestClientStreaming(context.Background(), transport, testServiceName, "LotsOfGreetings", []string{
			fmt.Sprintf("load-client-%d-a", index),
			fmt.Sprintf("load-client-%d-b", index),
		}, authenticatedOptions()...)
		if err != nil {
			return err
		}
		expected := fmt.Sprintf("load-client-%d-a,load-client-%d-b", index, index)
		if response.Value != expected {
			return fmt.Errorf("unexpected client stream response %q", response.Value)
		}
	default:
		responses, err := runTestBidiStreaming(context.Background(), transport, testServiceName, "BidiHello", []string{
			fmt.Sprintf("load-bidi-%d-a", index),
			fmt.Sprintf("load-bidi-%d-b", index),
		}, authenticatedOptions()...)
		if err != nil {
			return err
		}
		messages := collectTestMessagesNoFatal(responses)
		if messages.err != nil {
			return messages.err
		}
		if len(messages.values) != 2 || !strings.HasPrefix(messages.values[0], "echo, load-bidi-") || !strings.HasPrefix(messages.values[1], "echo, load-bidi-") {
			return fmt.Errorf("unexpected bidi responses %#v", messages.values)
		}
	}

	return nil
}

type collectedMessages struct {
	values []string
	err    error
}

func runTestClientStreaming(ctx context.Context, transport Transport, service, method string, values []string, options ...CallOption) (*testMessage, error) {
	stream, err := ClientStreaming[*testMessage, *testMessage](ctx, transport, service, method, func() *testMessage { return &testMessage{} }, options...)
	if err != nil {
		return nil, err
	}
	for _, value := range values {
		if err := stream.Send(&testMessage{Value: value}); err != nil {
			_ = stream.Close()
			return nil, err
		}
	}
	return stream.CloseAndRecv()
}

func runTestBidiStreaming(ctx context.Context, transport Transport, service, method string, values []string, options ...CallOption) (MessageStream[*testMessage], error) {
	stream, err := BidirectionalStreaming[*testMessage, *testMessage](ctx, transport, service, method, func() *testMessage { return &testMessage{} }, options...)
	if err != nil {
		return nil, err
	}
	sendDone := make(chan error, 1)
	go func() {
		for _, value := range values {
			if err := stream.Send(&testMessage{Value: value}); err != nil {
				sendDone <- err
				return
			}
		}
		sendDone <- stream.CloseSend()
	}()
	return &bidiTestStream{inner: stream, sendDone: sendDone}, nil
}

type bidiTestStream struct {
	inner    MessageStream[*testMessage]
	sendDone <-chan error
}

func (s *bidiTestStream) Recv() (*testMessage, error) {
	message, err := s.inner.Recv()
	if err == io.EOF {
		if sendErr := <-s.sendDone; sendErr != nil {
			return nil, sendErr
		}
	}
	return message, err
}

func (s *bidiTestStream) Close() error {
	return s.inner.Close()
}

func collectTestMessagesNoFatal(stream MessageStream[*testMessage]) collectedMessages {
	var messages []string
	for {
		message, err := stream.Recv()
		if err == io.EOF {
			return collectedMessages{values: messages}
		}
		if err != nil {
			return collectedMessages{values: messages, err: err}
		}

		messages = append(messages, message.Value)
	}
}

func testTLSConfig(t *testing.T) (*tls.Config, *tls.Config) {
	t.Helper()

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}

	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "localhost"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:     []string{"localhost"},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
	}

	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create certificate: %v", err)
	}

	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		t.Fatalf("load certificate: %v", err)
	}

	certPool := x509.NewCertPool()
	if !certPool.AppendCertsFromPEM(certPEM) {
		t.Fatal("append generated certificate to root pool")
	}

	serverTLS := &tls.Config{Certificates: []tls.Certificate{cert}, NextProtos: []string{ALPN}}
	clientTLS := &tls.Config{RootCAs: certPool, ServerName: "localhost", NextProtos: []string{ALPN}}
	return serverTLS, clientTLS
}
