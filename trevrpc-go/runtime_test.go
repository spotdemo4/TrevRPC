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
	"slices"
	"strings"
	"sync"
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
	return t.server.HandleStreamingRequest(ctx, request, requestBody), nil
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

	response, err := ClientStreaming(context.Background(), localTransport{server: server}, "example.Greeter", "LotsOfGreetings", FromSlice(
		&testMessage{Value: "hello"},
		&testMessage{Value: " world"},
	), func() *testMessage { return &testMessage{} })
	if err != nil {
		t.Fatalf("client streaming RPC failed: %v", err)
	}

	if response.Value != "hello world" {
		t.Fatalf("unexpected response: %q", response.Value)
	}
}

func TestBidirectionalStreamingClientServer(t *testing.T) {
	server := NewServer()
	server.RouteStreaming("example.Greeter", "BidiHello", RpcKindBidirectionalStreaming, func(_ context.Context, _ []byte, requests ByteStream) (ByteStream, error) {
		return EncodeStream[*testMessage](&echoTestMessages{requests: DecodeStream[*testMessage](requests, func() *testMessage { return &testMessage{} })}), nil
	})

	stream, err := BidirectionalStreaming(context.Background(), localTransport{server: server}, "example.Greeter", "BidiHello", FromSlice(
		&testMessage{Value: "left"},
		&testMessage{Value: "right"},
	), func() *testMessage { return &testMessage{} })
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

	if !metrics.hasCode(CodeCancelled) {
		t.Fatalf("expected cancelled metric, got %#v", metrics.codes)
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
	requests := newBlockingTestMessages()
	defer requests.Close()

	_, err := ClientStreaming(context.Background(), uploadWaitingTransport{}, "example.Greeter", "LotsOfGreetings", requests, func() *testMessage { return &testMessage{} }, WithTimeout(time.Millisecond), WithoutStreamIdleTimeout())
	if code := StatusFromError(err).Code; code != CodeDeadlineExceeded {
		t.Fatalf("expected deadline exceeded, got %v (%v)", code, err)
	}
}

func TestBidirectionalStreamingContextCancellationReturnsCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	stream, err := BidirectionalStreaming(ctx, contextBlockingTransport{}, "example.Greeter", "BidiHello", EmptyStream[*testMessage](), func() *testMessage { return &testMessage{} }, WithoutStreamIdleTimeout())
	if err != nil {
		t.Fatalf("start bidi stream: %v", err)
	}
	cancel()

	_, err = stream.Recv()
	if code := StatusFromError(err).Code; code != CodeCancelled {
		t.Fatalf("expected cancelled, got %v (%v)", code, err)
	}
}

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

type panickingByteStream struct{}

func (panickingByteStream) Recv() ([]byte, error) {
	panic("boom")
}

func (panickingByteStream) Close() error {
	panic("close boom")
}

type recordingMetrics struct {
	mu    sync.Mutex
	codes []Code
}

type panickingMetrics struct{}

func (panickingMetrics) RPCStarted(RPCStarted) {
	panic("started")
}

func (panickingMetrics) RPCFinished(RPCFinished) {
	panic("finished")
}

func (m *recordingMetrics) RPCStarted(RPCStarted) {}

func (m *recordingMetrics) RPCFinished(event RPCFinished) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.codes = append(m.codes, event.Code)
}

func (m *recordingMetrics) hasCode(code Code) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return slices.Contains(m.codes, code)
}

func TestQuicClientUnary(t *testing.T) {
	server := NewServer()
	RegisterUnary(server, "example.Greeter", "SayHello", func() *testMessage { return &testMessage{} }, func(_ context.Context, request *testMessage) (*testMessage, error) {
		return &testMessage{Value: "hello " + request.Value}, nil
	})

	serverTLS, clientTLS := testTLSConfig(t)
	listener, err := quic.ListenAddr("127.0.0.1:0", serverTLS, &quic.Config{})
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer listener.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	go func() {
		_ = ServeQUIC(ctx, listener, server)
	}()

	conn, err := quic.DialAddr(ctx, listener.Addr().String(), clientTLS, &quic.Config{})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.CloseWithError(0, "test complete")

	response, err := Unary(ctx, NewQuicClient(conn), "example.Greeter", "SayHello", &testMessage{Value: "QUIC"}, func() *testMessage { return &testMessage{} })
	if err != nil {
		t.Fatalf("quic unary RPC failed: %v", err)
	}

	if response.Value != "hello QUIC" {
		t.Fatalf("unexpected response: %q", response.Value)
	}
}

func TestQuicRoundTripsUnaryAndAllStreamingModes(t *testing.T) {
	running := startTestQUICServer(t, func(server *Server) {
		server.SetAuthorizer(BearerAuthorizer(testAuthToken))
	})
	conn := connectTestQUICClient(t, running)
	defer conn.CloseWithError(0, "test complete")
	transport := NewQuicClient(conn)

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

	summary, err := ClientStreaming(context.Background(), transport, testServiceName, "LotsOfGreetings", FromSlice(
		&testMessage{Value: "one"},
		&testMessage{Value: "two"},
	), func() *testMessage { return &testMessage{} }, authenticatedOptions()...)
	if err != nil {
		t.Fatalf("client-streaming RPC failed: %v", err)
	}
	if summary.Value != "one,two" {
		t.Fatalf("unexpected client stream summary: %q", summary.Value)
	}

	bidi, err := BidirectionalStreaming(context.Background(), transport, testServiceName, "BidiHello", FromSlice(
		&testMessage{Value: "left"},
		&testMessage{Value: "right"},
	), func() *testMessage { return &testMessage{} }, authenticatedOptions()...)
	if err != nil {
		t.Fatalf("bidi RPC failed: %v", err)
	}
	if messages := collectTestMessages(t, bidi); !equalStrings(messages, []string{"echo, left", "echo, right"}) {
		t.Fatalf("unexpected bidi replies: %#v", messages)
	}
}

func TestWebTransportRoundTripsUnaryAndAllStreamingModes(t *testing.T) {
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

func TestWebTransportCheckOriginAllowsBrowserOrigin(t *testing.T) {
	origin := "http://127.0.0.1:8080"
	running := startTestWebTransportServer(t, func(server *Server) {
		options := server.Options()
		options.WebTransportCheckOrigin = func(r *http.Request) bool {
			return r.Header.Get("Origin") == origin
		}
		server.SetOptions(options)
		server.SetAuthorizer(BearerAuthorizer(testAuthToken))
	})

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()
	transport, err := DialWebTransport(ctx, "https://"+running.addr+"/trevrpc", WebTransportDialOptions{
		TLSClientConfig: running.clientTLS.Clone(),
		QUICConfig:      testWebTransportQUICConfig(),
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

func TestWebTransportRejectsUnexpectedPath(t *testing.T) {
	running := startTestWebTransportServer(t, func(server *Server) {
		server.SetAuthorizer(BearerAuthorizer(testAuthToken))
	})

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()
	transport, err := DialWebTransport(ctx, "https://"+running.addr+"/wrong", WebTransportDialOptions{
		TLSClientConfig: running.clientTLS.Clone(),
		QUICConfig:      testWebTransportQUICConfig(),
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

	_, err := Unary(context.Background(), NewQuicClient(conn), testServiceName, "SayHello", &testMessage{Value: "missing auth"}, func() *testMessage { return &testMessage{} }, WithTimeout(testTimeout))
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

	response, err := NewQuicClient(conn).Call(context.Background(), request)
	if err != nil {
		t.Fatalf("raw transport call failed: %v", err)
	}
	if CodeFromUint32(response.Status) != CodeInvalidArgument {
		t.Fatalf("expected invalid argument, got %#v", response)
	}
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

	_, err := ClientStreaming(context.Background(), NewQuicClient(conn), testServiceName, "LotsOfGreetings", FromSlice(
		&testMessage{Value: "one"},
		&testMessage{Value: "two"},
	), func() *testMessage { return &testMessage{} }, authenticatedOptions()...)
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

	replies, err := ServerStreaming(context.Background(), NewQuicClient(conn), testServiceName, "LotsOfReplies", &testMessage{Value: "limited"}, func() *testMessage { return &testMessage{} }, append(authenticatedOptions(), WithMaxResponseMessages(1))...)
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
	transport := NewQuicClient(conn)
	hanging := holdServerStreamOpen(t, transport)
	defer closeMessageStream(hanging)

	_, err := Unary(context.Background(), transport, testServiceName, "SayHello", &testMessage{Value: "second"}, func() *testMessage { return &testMessage{} }, authenticatedOptions()...)
	if code := StatusFromError(err).Code; code != CodeUnavailable {
		t.Fatalf("expected unavailable, got %v (%v)", code, err)
	}
}

func TestQuicRequestConcurrencyLimitReturnsUnavailable(t *testing.T) {
	running := startTestQUICServer(t, func(server *Server) {
		options := DefaultServerOptions()
		options.MaxConcurrentRequests = 1
		options.GracefulShutdownTimeout = 50 * time.Millisecond
		server.SetOptions(options)
		server.SetAuthorizer(BearerAuthorizer(testAuthToken))
	})
	conn := connectTestQUICClient(t, running)
	defer conn.CloseWithError(0, "test complete")
	transport := NewQuicClient(conn)
	hanging := holdServerStreamOpen(t, transport)
	defer closeMessageStream(hanging)

	_, err := Unary(context.Background(), transport, testServiceName, "SayHello", &testMessage{Value: "second"}, func() *testMessage { return &testMessage{} }, authenticatedOptions()...)
	if code := StatusFromError(err).Code; code != CodeUnavailable {
		t.Fatalf("expected unavailable, got %v (%v)", code, err)
	}
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
	hanging := holdServerStreamOpen(t, NewQuicClient(conn))
	defer closeMessageStream(hanging)

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()
	secondConn, err := quic.DialAddr(ctx, running.addr, running.clientTLS.Clone(), &quic.Config{})
	if err != nil {
		return
	}
	defer secondConn.CloseWithError(0, "test complete")

	_, err = Unary(context.Background(), NewQuicClient(secondConn), testServiceName, "SayHello", &testMessage{Value: "refused"}, func() *testMessage { return &testMessage{} }, authenticatedOptions()...)
	if code := StatusFromError(err).Code; code != CodeUnavailable {
		t.Fatalf("expected unavailable, got %v (%v)", code, err)
	}
}

func TestQuicDroppedResponseStreamCancelsServerWork(t *testing.T) {
	metrics := &recordingMetrics{}
	running := startTestQUICServer(t, func(server *Server) {
		server.SetMetrics(metrics)
		server.SetAuthorizer(BearerAuthorizer(testAuthToken))
	})
	conn := connectTestQUICClient(t, running)
	defer conn.CloseWithError(0, "test complete")
	replies := holdServerStreamOpen(t, NewQuicClient(conn))

	closeMessageStream(replies)
	waitForMetricCode(t, metrics, CodeCancelled)
}

func TestWebTransportDroppedResponseStreamCancelsServerWork(t *testing.T) {
	metrics := &recordingMetrics{}
	running := startTestWebTransportServer(t, func(server *Server) {
		server.SetMetrics(metrics)
		server.SetAuthorizer(BearerAuthorizer(testAuthToken))
	})
	transport := connectTestWebTransportClient(t, running)
	defer transport.Session().CloseWithError(cancelledWebTransportSessionCode, "test complete")
	replies := holdServerStreamOpen(t, transport)

	closeMessageStream(replies)
	waitForMetricCode(t, metrics, CodeCancelled)
}

func TestQuicTerminalStatusClosesPendingRequestStream(t *testing.T) {
	running := startTestQUICServer(t, registerRejectUploadRoute)
	conn := connectTestQUICClient(t, running)
	defer conn.CloseWithError(0, "test complete")
	requests := newBlockingTestMessages()

	replies, err := BidirectionalStreaming(context.Background(), NewQuicClient(conn), testServiceName, "RejectUpload", requests, func() *testMessage { return &testMessage{} }, WithTimeout(testTimeout))
	if err != nil {
		t.Fatalf("start rejecting bidi stream: %v", err)
	}
	_, err = replies.Recv()
	if code := StatusFromError(err).Code; code != CodePermissionDenied {
		t.Fatalf("expected permission denied, got %v (%v)", code, err)
	}
	requests.waitClosed(t)
}

func TestWebTransportTerminalStatusClosesPendingRequestStream(t *testing.T) {
	running := startTestWebTransportServer(t, registerRejectUploadRoute)
	transport := connectTestWebTransportClient(t, running)
	defer transport.Session().CloseWithError(cancelledWebTransportSessionCode, "test complete")
	requests := newBlockingTestMessages()

	replies, err := BidirectionalStreaming(context.Background(), transport, testServiceName, "RejectUpload", requests, func() *testMessage { return &testMessage{} }, WithTimeout(testTimeout))
	if err != nil {
		t.Fatalf("start rejecting WebTransport bidi stream: %v", err)
	}
	_, err = replies.Recv()
	if code := StatusFromError(err).Code; code != CodePermissionDenied {
		t.Fatalf("expected permission denied, got %v (%v)", code, err)
	}
	requests.waitClosed(t)
}

func TestQuicShutdownClosesActiveConnections(t *testing.T) {
	running := startTestQUICServer(t, func(*Server) {})
	conn := connectTestQUICClient(t, running)
	transport := NewQuicClient(conn)
	running.stop(t)

	_, err := Unary(context.Background(), transport, testServiceName, "SayHello", &testMessage{Value: "after shutdown"}, func() *testMessage { return &testMessage{} }, WithTimeout(100*time.Millisecond))
	if code := StatusFromError(err).Code; code != CodeUnavailable {
		t.Fatalf("expected unavailable, got %v (%v)", code, err)
	}
	conn.CloseWithError(0, "test complete")
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
		_, err := Unary(context.Background(), NewQuicClient(conn), testServiceName, "ObserveCancel", &testMessage{}, func() *testMessage { return &testMessage{} }, WithTimeout(testTimeout))
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
	transport := NewQuicClient(conn)

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

func TestQuicLocalCloseMapsToCancelled(t *testing.T) {
	running := startTestQUICServer(t, func(*Server) {})
	conn := connectTestQUICClient(t, running)
	conn.CloseWithError(0, "client closed")

	_, err := Unary(context.Background(), NewQuicClient(conn), testServiceName, "SayHello", &testMessage{Value: "after local close"}, func() *testMessage { return &testMessage{} }, WithTimeout(100*time.Millisecond))
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
		_, rpcErr := Unary(context.Background(), NewQuicClient(conn), testServiceName, "SayHello", &testMessage{Value: "anonymous"}, func() *testMessage { return &testMessage{} }, WithTimeout(100*time.Millisecond))
		conn.CloseWithError(0, "test complete")
		if code := StatusFromError(rpcErr).Code; code != CodeUnavailable {
			t.Fatalf("expected unavailable for anonymous mTLS client, got %v (%v)", code, rpcErr)
		}
	}
}

func TestQuicMalformedRequestFramesReturnInvalidArgumentStatus(t *testing.T) {
	running := startTestQUICServer(t, func(*Server) {})
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
	running := startTestQUICServerWithInitialRequestTimeout(t, 50*time.Millisecond)
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
}

func TestQuicInitialRequestTimeoutRejectsPartialBody(t *testing.T) {
	running := startTestQUICServerWithInitialRequestTimeout(t, 50*time.Millisecond)
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
	transport := NewQuicClient(conn)

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
	transport := NewQuicClient(conn)

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
	listener, err := quic.ListenAddr("127.0.0.1:0", serverTLS, &quic.Config{})
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	server := NewServer()
	registerTestGreeter(server)
	configure(server)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- ServeQUIC(ctx, listener, server)
	}()

	running := &runningTestQUICServer{
		addr:      listener.Addr().String(),
		clientTLS: clientTLS,
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
	serverTLS, clientTLS := testTLSConfig(t)
	serverTLS.NextProtos = []string{http3.NextProtoH3}
	clientTLS.NextProtos = []string{http3.NextProtoH3}

	listener, err := quic.ListenAddr("127.0.0.1:0", serverTLS, testWebTransportQUICConfig())
	if err != nil {
		t.Fatalf("listen WebTransport: %v", err)
	}

	server := NewServer()
	registerTestGreeter(server)
	configure(server)
	options := server.Options()
	options.EnableWebTransport = true
	if options.WebTransportCheckOrigin == nil {
		options.WebTransportCheckOrigin = func(*http.Request) bool { return true }
	}
	server.SetOptions(options)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- ServeQUIC(ctx, listener, server)
	}()

	running := &runningTestQUICServer{
		addr:      listener.Addr().String(),
		clientTLS: clientTLS,
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
	conn, err := quic.DialAddr(ctx, running.addr, running.clientTLS.Clone(), &quic.Config{})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}

	return conn
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

func connectTestWebTransportClient(t *testing.T, running *runningTestQUICServer) *WebTransportClient {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	transport, err := DialWebTransport(ctx, "https://"+running.addr+"/trevrpc", WebTransportDialOptions{
		TLSClientConfig: running.clientTLS.Clone(),
		QUICConfig:      testWebTransportQUICConfig(),
	})
	if err != nil {
		t.Fatalf("dial WebTransport: %v", err)
	}

	return transport
}

func testWebTransportQUICConfig() *quic.Config {
	return &quic.Config{
		EnableDatagrams:                  true,
		EnableStreamResetPartialDelivery: true,
	}
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

type closeErrorFrameStream struct {
	frames []*RpcStreamFrame
	err    error
	closed int
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

func (s uploadErrorFrameStream) Recv() (*RpcStreamFrame, error) {
	return nil, <-s.errors
}

func (uploadErrorFrameStream) Close() error { return nil }

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

type blockingTestMessages struct {
	closed chan struct{}
	once   sync.Once
}

func newBlockingTestMessages() *blockingTestMessages {
	return &blockingTestMessages{closed: make(chan struct{})}
}

func (s *blockingTestMessages) Recv() (*testMessage, error) {
	<-s.closed
	return nil, io.EOF
}

func (s *blockingTestMessages) Close() error {
	s.once.Do(func() { close(s.closed) })
	return nil
}

func (s *blockingTestMessages) waitClosed(t *testing.T) {
	t.Helper()
	select {
	case <-s.closed:
	case <-time.After(testTimeout):
		t.Fatal("request stream was not closed")
	}
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
	s.first = nil
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
		response, err := ClientStreaming(context.Background(), transport, testServiceName, "LotsOfGreetings", FromSlice(
			&testMessage{Value: fmt.Sprintf("load-client-%d-a", index)},
			&testMessage{Value: fmt.Sprintf("load-client-%d-b", index)},
		), func() *testMessage { return &testMessage{} }, authenticatedOptions()...)
		if err != nil {
			return err
		}
		expected := fmt.Sprintf("load-client-%d-a,load-client-%d-b", index, index)
		if response.Value != expected {
			return fmt.Errorf("unexpected client stream response %q", response.Value)
		}
	default:
		responses, err := BidirectionalStreaming(context.Background(), transport, testServiceName, "BidiHello", FromSlice(
			&testMessage{Value: fmt.Sprintf("load-bidi-%d-a", index)},
			&testMessage{Value: fmt.Sprintf("load-bidi-%d-b", index)},
		), func() *testMessage { return &testMessage{} }, authenticatedOptions()...)
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
