package trevrpc

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"time"
)

// closeOnCleanup closes c when the test ends and fails the test if that errors.
func closeOnCleanup(t testing.TB, c io.Closer) {
	t.Helper()
	t.Cleanup(func() {
		if err := c.Close(); err != nil {
			t.Errorf("Close() error = %v", err)
		}
	})
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
	if errors.Is(err, io.EOF) {
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
	frames []*wireStreamFrame
	err    error
	closed int
}

func (s *closeErrorFrameStream) Recv() (*wireStreamFrame, error) {
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

func (s uploadErrorFrameStream) Recv() (*wireStreamFrame, error) {
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

func readStreamFramesFromBytes(t *testing.T, data []byte, maxFrameSize int) []*wireStreamFrame {
	t.Helper()
	reader := bytes.NewReader(data)
	var frames []*wireStreamFrame
	for {
		frame := &wireStreamFrame{}
		read, err := readFrameOrEOF(reader, frame, maxFrameSize)
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

func (contextBlockingTransport) call(ctx context.Context, _ *wireRequest) (*wireResponse, error) {
	<-ctx.Done()
	return nil, statusFromContextError(ctx.Err())
}

func (contextBlockingTransport) streamingCall(ctx context.Context, _ *wireRequest, _ byteStream) (frameStream, error) {
	return contextBlockingFrameStream{ctx: ctx}, nil
}

type contextBlockingFrameStream struct {
	ctx context.Context
}

func (s contextBlockingFrameStream) Recv() (*wireStreamFrame, error) {
	<-s.ctx.Done()
	return nil, statusFromContextError(s.ctx.Err())
}

func (contextBlockingFrameStream) Close() error { return nil }

type uploadWaitingTransport struct{}

func (uploadWaitingTransport) call(context.Context, *wireRequest) (*wireResponse, error) {
	return nil, Unimplemented("unary not implemented")
}

func (uploadWaitingTransport) streamingCall(ctx context.Context, _ *wireRequest, requestBody byteStream) (frameStream, error) {
	errors := make(chan error, 1)
	go func() {
		_, err := recvRequestBody(ctx, requestBody)
		errors <- err
	}()
	return uploadErrorFrameStream{errors: errors}, nil
}

// routeBytes registers a unary handler that works on encoded request and
// response bodies.
func routeBytes(server *Server, service, method string, handler func(context.Context, []byte) ([]byte, error)) {
	server.routeUnary(service, method, func(ctx context.Context, body []byte) (*wireResponse, error) {
		responseBody, err := handler(ctx, body)
		if err != nil {
			return nil, err
		}
		return okWireResponse(responseBody), nil
	})
}

// testResponse wraps message in a handler Response without metadata.
func testResponse[T ProtoMessage](message T) *Response[T] {
	return &Response[T]{Message: message}
}

// handleRequest runs a unary request through the server as the transports do,
// including the concurrency admission check.
func (s *Server) handleRequest(ctx context.Context, request *wireRequest) *wireResponse {
	runtime := s.freeze()
	lease, ok := runtime.tryRequestLease(request)
	if !ok {
		startedAt := time.Now()
		service, method, bodyLen := requestService(request), requestMethod(request), 0
		if request != nil {
			bodyLen = len(request.Body)
		}
		runtime.recordStarted(RPCStarted{Service: service, Method: method, RequestBodyLen: bodyLen}, request)
		return runtime.finishResponse(service, method, bodyLen, startedAt, Unavailable("too many concurrent RPCs").intoResponse(nil), request)
	}
	defer lease.release()
	return runtime.handleRequest(ctx, request, lease)
}

// handleStreamingRequest runs a streaming request through the server as the
// transports do, including the concurrency admission check.
func (s *Server) handleStreamingRequest(ctx context.Context, request *wireRequest, requestBody byteStream) frameStream {
	runtime := s.freeze()
	lease, ok := runtime.tryRequestLease(request)
	if !ok {
		startedAt := time.Now()
		service, method := requestService(request), requestMethod(request)
		bodyLen := 0
		if request != nil {
			bodyLen = len(request.Body)
		}
		runtime.recordStarted(RPCStarted{Service: service, Method: method, RequestBodyLen: bodyLen}, request)
		return runtime.finishStreamingStatus(service, method, bodyLen, startedAt, Unavailable("too many concurrent RPCs"), request)
	}
	return runtime.handleStreamingRequest(ctx, request, requestBody, lease)
}

// okWireResponse creates a successful RPC response with the provided body.
func okWireResponse(body []byte) *wireResponse {
	return &wireResponse{Status: uint32(CodeOK), Body: body, Metadata: Metadata{}}
}

// frameKind returns the decoded stream frame kind.
func (m *wireStreamFrame) frameKind() (wireFrameKind, bool) {
	switch m.Kind {
	case wireFrameMessage, wireFrameStatus:
		return m.Kind, true
	default:
		return 0, false
	}
}

// statusValue returns the status represented by the frame status fields.
func (m *wireStreamFrame) statusValue() *Status {
	return NewStatus(CodeFromUint32(m.Status), m.Message).WithMetadata(m.Metadata)
}

// singleMessageStream returns a byte stream containing one encoded protobuf message.
func singleMessageStream[T ProtoMessage](message T) byteStream {
	return newEncodeStream(newSliceStream(message))
}

// responseStreamFailureReason returns the stable reason for a response-stream failure.
func responseStreamFailureReason(err error) (string, bool) {
	var failure *responseStreamFailure
	if !errors.As(err, &failure) {
		return "", false
	}
	return failure.reason, true
}
