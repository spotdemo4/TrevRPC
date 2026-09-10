package trevrpc

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"strings"
	"sync"
	"testing"
	"time"
)

func authenticatedOptions() []CallOption {
	return []CallOption{
		WithTimeout(testTimeout),
		WithMetadata("authorization", []byte("Bearer "+testAuthToken)),
	}
}

const testServiceName = "example.Greeter"

func registerTestGreeter(server *Server) {
	RegisterUnary(server, testServiceName, "SayHello", func() *testMessage { return &testMessage{} }, func(_ context.Context, request *testMessage) (*testMessage, error) {
		return &testMessage{Value: "hello, " + request.Value}, nil
	})
	server.RouteStreaming(testServiceName, "LotsOfReplies", RpcKindServerStreaming, func(_ context.Context, body []byte, _ ByteStream) (ByteStream, error) {
		request := &testMessage{}
		if err := UnmarshalMessage(body, request); err != nil {
			return nil, err
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

func runMixedQUICCallWithOptions(transport Transport, index int, options []CallOption) error {
	switch index % 4 {
	case 0:
		name := fmt.Sprintf("load-unary-%d", index)
		response, err := Unary(context.Background(), transport, testServiceName, "SayHello", &testMessage{Value: name}, func() *testMessage { return &testMessage{} }, options...)
		if err != nil {
			return err
		}
		if response.Value != "hello, "+name {
			return fmt.Errorf("unexpected unary response %q", response.Value)
		}
	case 1:
		name := fmt.Sprintf("load-server-%d", index)
		responses, err := ServerStreaming(context.Background(), transport, testServiceName, "LotsOfReplies", &testMessage{Value: name}, func() *testMessage { return &testMessage{} }, options...)
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
		}, options...)
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
		}, options...)
		if err != nil {
			return err
		}
		messages := collectTestMessagesNoFatal(responses)
		if messages.err != nil {
			return messages.err
		}
		expected := []string{
			fmt.Sprintf("echo, load-bidi-%d-a", index),
			fmt.Sprintf("echo, load-bidi-%d-b", index),
		}
		if !equalStrings(messages.values, expected) {
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

func equalStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
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
	sendOnce sync.Once
	sendErr  error
}

func (s *bidiTestStream) Recv() (*testMessage, error) {
	message, err := s.inner.Recv()
	if err == io.EOF {
		s.sendOnce.Do(func() { s.sendErr = <-s.sendDone })
		if s.sendErr != nil {
			return nil, s.sendErr
		}
	}
	return message, err
}

func (s *bidiTestStream) Close() error {
	return s.inner.Close()
}

type closeErrorFrameStream struct {
	frames []*RpcStreamFrame
	err    error
	closed int
}

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

type uploadErrorFrameStream struct {
	errors <-chan error
}

func (s uploadErrorFrameStream) Recv() (*RpcStreamFrame, error) {
	return nil, <-s.errors
}

func (uploadErrorFrameStream) Close() error { return nil }

type byteErrorStream struct {
	err error
}

func (s byteErrorStream) Recv() ([]byte, error) { return nil, s.err }
func (byteErrorStream) Close() error            { return nil }

type countingWriter struct {
	bytes.Buffer
	writeCount int
}

func (w *countingWriter) Write(data []byte) (int, error) {
	w.writeCount++
	return w.Buffer.Write(data)
}

type timeoutRPCStream struct{ err error }

func (s timeoutRPCStream) Read([]byte) (int, error)      { return 0, s.err }
func (timeoutRPCStream) Write(data []byte) (int, error)  { return len(data), nil }
func (timeoutRPCStream) Close() error                    { return nil }
func (timeoutRPCStream) SetReadDeadline(time.Time) error { return nil }

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

type cancellableTransportTestStream struct{}

func (*cancellableTransportTestStream) Read([]byte) (int, error)        { return 0, io.EOF }
func (*cancellableTransportTestStream) Write(data []byte) (int, error)  { return len(data), nil }
func (*cancellableTransportTestStream) Close() error                    { return nil }
func (*cancellableTransportTestStream) CancelRead(TransportCloseReason) {}

func testCertificateMaterial(t *testing.T) ([]byte, []byte) {
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

	der, err := x509.CreateCertificate(
		rand.Reader,
		template,
		template,
		&key.PublicKey,
		key,
	)
	if err != nil {
		t.Fatalf("create certificate: %v", err)
	}

	return pem.EncodeToMemory(&pem.Block{
			Type:  "CERTIFICATE",
			Bytes: der,
		}), pem.EncodeToMemory(&pem.Block{
			Type:  "RSA PRIVATE KEY",
			Bytes: x509.MarshalPKCS1PrivateKey(key),
		})
}
