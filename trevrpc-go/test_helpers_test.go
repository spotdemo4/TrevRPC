package trevrpc

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
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
	frames []responseFrame
	err    error
	closed int
}

func (s *closeErrorFrameStream) Recv() (responseFrame, error) {
	if len(s.frames) == 0 {
		return responseFrame{}, io.EOF
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

func (s uploadErrorFrameStream) Recv() (responseFrame, error) {
	return responseFrame{}, <-s.errors
}

func (uploadErrorFrameStream) Close() error { return nil }

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

// marshalMessage encodes message as a protobuf payload.
func marshalMessage(message ProtoMessage) ([]byte, error) {
	return proto.Marshal(protoMessageV2(message))
}

// messageFrame is a received MESSAGE frame carrying body.
func messageFrame(body []byte) responseFrame {
	return responseFrame{kind: frameTypeMessage, body: body}
}

// headersFrame is a received HEADERS frame.
func headersFrame(metadata Metadata) responseFrame {
	return responseFrame{kind: frameTypeHeaders, headers: metadata}
}

// statusFrame is a received STATUS frame.
func statusFrame(status *Status) responseFrame {
	return responseFrame{kind: frameTypeStatus, status: status}
}

// statusFrameWithMetadata is a received STATUS frame carrying trailing metadata.
func statusFrameWithMetadata(status *Status, metadata Metadata) responseFrame {
	return responseFrame{kind: frameTypeStatus, status: status.WithMetadata(metadata)}
}

// rawMessage is an encodedMessage whose payload is body, which need not be a
// valid protobuf message.
func rawMessage(body []byte) encodedMessage {
	frame := appendFrameHeader(nil, frameTypeMessage, len(body))
	return encodedMessage{frame: append(frame, body...), size: len(body)}
}

// rawStream is a non-blocking encodedStream of messages with the given payloads.
func rawStream(bodies ...string) encodedStream {
	messages := make([]encodedMessage, len(bodies))
	for index, body := range bodies {
		messages[index] = rawMessage([]byte(body))
	}
	return newSliceStream(messages...)
}

// rawResponse is a streaming handler response of messages with the given payloads.
func rawResponse(bodies ...string) (streamResponse, error) {
	return streamResponse{stream: rawStream(bodies...)}, nil
}

// respond is a streaming handler response over stream.
func respond(stream encodedStream) (streamResponse, error) {
	return streamResponse{stream: stream}, nil
}

// testRequestBytes is the request side of a stream: a REQUEST frame and then a
// MESSAGE frame for every message.
func testRequestBytes(t testing.TB, request *wireRequest, messages ...string) []byte {
	t.Helper()
	data, err := appendRequestFrame(nil, request, DefaultMaxFrameSize)
	if err != nil {
		t.Fatalf("encode request frame: %v", err)
	}
	for _, message := range messages {
		data = append(data, rawMessage([]byte(message)).frame...)
	}
	return data
}

// writeTestRequestFrame writes a REQUEST frame to w.
func writeTestRequestFrame(t testing.TB, w io.Writer, request *wireRequest) {
	t.Helper()
	if _, err := w.Write(testRequestBytes(t, request)); err != nil {
		t.Fatalf("write request frame: %v", err)
	}
}

// readTestFrame reads one known frame from r.
func readTestFrame(t testing.TB, r io.Reader) testFrame {
	t.Helper()
	next, err := newFrameReader(r, DefaultMaxFrameSize, -1).next()
	if err != nil {
		t.Fatalf("read frame: %v", err)
	}
	decoded, err := decodeTestFrame(next)
	if err != nil {
		t.Fatalf("decode frame: %v", err)
	}
	return decoded
}

// finiteRequestBytes is the request side of a unary or server-streaming call,
// a REQUEST and an empty request message, or of any other call, a REQUEST.
func finiteRequestBytes(t testing.TB, request *wireRequest) []byte {
	t.Helper()
	if requestEndsAfterMessage(request.rpcKind()) {
		return testRequestBytes(t, request, "")
	}
	return testRequestBytes(t, request)
}

// testFrame is a frame read from a byte stream, decoded as far as the tests need.
type testFrame struct {
	Kind    frameType
	Request *wireRequest
	Body    []byte
	Headers Metadata
	Status  uint32
	Message string
	// Metadata is the trailing metadata of a STATUS frame.
	Metadata Metadata
}

func (f testFrame) statusValue() *Status {
	return NewStatus(CodeFromUint32(f.Status), f.Message).WithMetadata(f.Metadata)
}

// decodeTestFrame decodes a known frame.
func decodeTestFrame(next frame) (testFrame, error) {
	decoded := testFrame{Kind: next.typ}
	switch next.typ {
	case frameTypeRequest:
		decoded.Request = &wireRequest{}
		if err := decodePayload(next.payload, decoded.Request); err != nil {
			return testFrame{}, err
		}
	case frameTypeMessage:
		decoded.Body = next.payload
	case frameTypeHeaders:
		headers, err := decodeHeaders(next.payload)
		if err != nil {
			return testFrame{}, err
		}
		decoded.Headers = headers
	case frameTypeStatus:
		var status wireStatus
		if err := decodePayload(next.payload, &status); err != nil {
			return testFrame{}, err
		}
		decoded.Status, decoded.Message, decoded.Metadata = status.Code, status.Message, status.Metadata
	}
	return decoded, nil
}

// readTestFrames reads every known frame of data with the real frame reader.
func readTestFrames(t testing.TB, data []byte, maxFrameSize int) []testFrame {
	t.Helper()
	reader := newFrameReader(bytes.NewReader(data), maxFrameSize, -1)
	var frames []testFrame
	for {
		next, err := reader.next()
		if errors.Is(err, io.EOF) {
			return frames
		}
		if err != nil {
			t.Fatalf("read frame %d: %v", len(frames), err)
		}
		decoded, err := decodeTestFrame(next)
		if err != nil {
			t.Fatalf("decode frame %d: %v", len(frames), err)
		}
		frames = append(frames, decoded)
	}
}

// requireStatusFrame fails the test unless frames is exactly one STATUS frame
// with the given code, and returns it.
func requireStatusFrame(t testing.TB, frames []testFrame, code Code) testFrame {
	t.Helper()
	if len(frames) != 1 || frames[0].Kind != frameTypeStatus || CodeFromUint32(frames[0].Status) != code {
		t.Fatalf("frames = %#v, want one STATUS %v", frames, code)
	}
	return frames[0]
}

// serverFrames is a streaming server response read the way a client does:
// every call drives the response writer and decodes what it wrote.
type serverFrames struct {
	writer  responseWriter
	pending []testFrame
	done    bool
	maxSize int
}

func newServerFrames(writer responseWriter) *serverFrames {
	return &serverFrames{writer: writer, maxSize: DefaultMaxFrameSize}
}

func (s *serverFrames) Recv() (*testFrame, error) {
	for len(s.pending) == 0 {
		if s.done {
			return nil, io.EOF
		}
		var buffer bytes.Buffer
		done, err := s.writer.writeNext(context.Background(), &buffer, s.maxSize)
		if err != nil {
			return nil, err
		}
		s.done = done
		reader := newFrameReader(&buffer, s.maxSize, -1)
		for {
			next, err := reader.next()
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				return nil, err
			}
			decoded, err := decodeTestFrame(next)
			if err != nil {
				return nil, err
			}
			s.pending = append(s.pending, decoded)
		}
	}
	frame := s.pending[0]
	s.pending = s.pending[1:]
	return &frame, nil
}

func (s *serverFrames) Close() error { return s.writer.Close() }

// localTransport runs RPCs against a server in process. Requests and responses
// pass through the frame encoding and decoding but not through a stream.
type localTransport struct {
	server *Server
}

func (t localTransport) call(ctx context.Context, request *wireRequest, message ProtoMessage) (frameStream, error) {
	body, err := proto.Marshal(protoMessageV2(message))
	if err != nil {
		return nil, err
	}
	if request.rpcKind() == RPCKindUnary {
		return localResponse(newServerFrames(t.server.handleUnaryRequest(ctx, request, body))), nil
	}
	return localResponse(t.server.handleLocalStreaming(ctx, request, body, newEmptyStream[[]byte]())), nil
}

// streamingCall dispatches in the background: a handler may read its requests
// before it returns, and they are only sent once the call has been started.
func (t localTransport) streamingCall(ctx context.Context, request *wireRequest, requests encodedStream) (frameStream, error) {
	responses := make(chan *serverFrames, 1)
	go func() {
		responses <- t.server.handleLocalStreaming(ctx, request, nil, payloadStream{requests})
	}()
	return &localFrameStream{responses: responses}, nil
}

// payloadStream yields the payloads of encoded messages.
type payloadStream struct{ encodedStream }

func (s payloadStream) Recv() ([]byte, error) {
	message, err := s.encodedStream.Recv()
	if err != nil {
		return nil, err
	}
	return payloadOf(message), nil
}

func (s payloadStream) trevrpcNonBlockingStream() bool { return isNonBlockingStream(s.encodedStream) }
func (s payloadStream) trevrpcContextCancelsRecv() bool {
	return streamContextCancelsRecv(s.encodedStream)
}

// localResponse turns decoded server frames into the response frames of a client.
func localResponse(frames *serverFrames) frameStream {
	responses := make(chan *serverFrames, 1)
	responses <- frames
	return &localFrameStream{responses: responses}
}

type localFrameStream struct {
	responses <-chan *serverFrames
	once      sync.Once
	frames    *serverFrames
}

func (s *localFrameStream) serverFrames() *serverFrames {
	s.once.Do(func() { s.frames = <-s.responses })
	return s.frames
}

func (s *localFrameStream) Recv() (responseFrame, error) {
	frame, err := s.serverFrames().Recv()
	if err != nil {
		return responseFrame{}, err
	}
	switch frame.Kind {
	case frameTypeMessage:
		return messageFrame(frame.Body), nil
	case frameTypeHeaders:
		return headersFrame(frame.Headers), nil
	default:
		return statusFrame(frame.statusValue()), nil
	}
}

func (s *localFrameStream) Close() error {
	if s.frames == nil {
		return nil
	}
	return s.frames.Close()
}

type contextBlockingTransport struct{}

func (contextBlockingTransport) call(ctx context.Context, _ *wireRequest, _ ProtoMessage) (frameStream, error) {
	return contextBlockingFrameStream{ctx: ctx}, nil
}

func (contextBlockingTransport) streamingCall(ctx context.Context, _ *wireRequest, _ encodedStream) (frameStream, error) {
	return contextBlockingFrameStream{ctx: ctx}, nil
}

type contextBlockingFrameStream struct {
	ctx context.Context
}

func (s contextBlockingFrameStream) Recv() (responseFrame, error) {
	<-s.ctx.Done()
	return responseFrame{}, statusFromContextError(s.ctx.Err())
}

func (contextBlockingFrameStream) Close() error { return nil }

type uploadWaitingTransport struct{}

func (uploadWaitingTransport) call(context.Context, *wireRequest, ProtoMessage) (frameStream, error) {
	return nil, Unimplemented("unary not implemented")
}

func (uploadWaitingTransport) streamingCall(ctx context.Context, _ *wireRequest, requests encodedStream) (frameStream, error) {
	errors := make(chan error, 1)
	go func() {
		_, err := recvRequestBody(ctx, requests)
		errors <- err
	}()
	return uploadErrorFrameStream{errors: errors}, nil
}

// routeBytes registers a unary handler that works on raw request and response
// payloads.
func routeBytes(server *Server, service, method string, handler func(context.Context, []byte) ([]byte, error)) {
	server.routeUnary(service, method, func(ctx context.Context, body []byte) (*unaryReply, error) {
		responseBody, err := handler(ctx, body)
		if err != nil {
			return nil, err
		}
		return &unaryReply{message: rawMessage(responseBody)}, nil
	})
}

// testResponse wraps message in a handler Response without metadata.
func testResponse[T ProtoMessage](message T) *Response[T] {
	return &Response[T]{Message: message}
}

// unaryResult is a unary response as the tests inspect it.
type unaryResult struct {
	Status   uint32
	Message  string
	Body     []byte
	Headers  Metadata
	Metadata Metadata
}

// handleRequest runs a unary request through the server as the transports do,
// including the concurrency admission check.
func (s *Server) handleRequest(ctx context.Context, request *wireRequest, body ...[]byte) *unaryResult {
	var payload []byte
	if len(body) > 0 {
		payload = body[0]
	}
	response := s.handleUnaryRequest(ctx, request, payload)
	result := &unaryResult{Status: uint32(response.status.Code), Message: response.status.Message, Headers: response.headers, Metadata: response.status.Metadata}
	if response.hasMessage {
		result.Body = payloadOf(response.message)
	}
	return result
}

// payloadOf returns the payload of an encoded message.
func payloadOf(message encodedMessage) []byte {
	return message.frame[len(message.frame)-message.size:]
}

func (s *Server) handleUnaryRequest(ctx context.Context, request *wireRequest, body []byte) *unaryResponse {
	runtime := s.freeze()
	lease, ok := runtime.tryRequestLease(request)
	if !ok {
		startedAt := time.Now()
		service, method := requestService(request), requestMethod(request)
		runtime.recordStarted(RPCStarted{Service: service, Method: method, RequestBodyLen: len(body)}, request)
		return runtime.finishResponse(service, method, len(body), startedAt, failedUnaryResponse(Unavailable("too many concurrent RPCs")), request)
	}
	defer lease.release()
	return runtime.handleRequest(ctx, request, body, lease)
}

// handleStreamingRequest runs a streaming request through the server as the
// transports do, including the concurrency admission check.
func (s *Server) handleStreamingRequest(ctx context.Context, request *wireRequest, requestBody byteStream) *serverFrames {
	return s.handleLocalStreaming(ctx, request, nil, requestBody)
}

// handleLocalStreaming is handleStreamingRequest with the request message of a
// server-streaming call.
func (s *Server) handleLocalStreaming(ctx context.Context, request *wireRequest, body []byte, requestBody byteStream) *serverFrames {
	return newServerFrames(s.handleStreamingWriter(ctx, request, body, requestBody))
}

// handleStreamingWriter is handleLocalStreaming without decoding the response.
func (s *Server) handleStreamingWriter(ctx context.Context, request *wireRequest, body []byte, requestBody byteStream) responseWriter {
	runtime := s.freeze()
	lease, ok := runtime.tryRequestLease(request)
	if !ok {
		startedAt := time.Now()
		service, method := requestService(request), requestMethod(request)
		runtime.recordStarted(RPCStarted{Service: service, Method: method, RequestBodyLen: len(body)}, request)
		return runtime.finishStreamingStatus(service, method, len(body), startedAt, Unavailable("too many concurrent RPCs"), request)
	}
	return runtime.handleStreamingRequest(ctx, request, body, requestBody, lease)
}

// singleMessageResponse is a streaming handler response of one message.
func singleMessageResponse[T ProtoMessage](message T) (streamResponse, error) {
	return newStreamResponse(NewResponseStream(newSliceStream(message), nil))
}

// responseStreamFailureReason returns the stable reason for a response-stream failure.
func responseStreamFailureReason(err error) (string, bool) {
	var failure *responseStreamFailure
	if !errors.As(err, &failure) {
		return "", false
	}
	return failure.reason, true
}

type emptyStream[T any] struct{}

// newEmptyStream returns a stream that immediately ends.
func newEmptyStream[T any]() MessageStream[T] {
	return emptyStream[T]{}
}

func (emptyStream[T]) trevrpcNonBlockingStream() bool { return true }

func (emptyStream[T]) Recv() (T, error) {
	var zero T
	return zero, io.EOF
}

func (emptyStream[T]) Close() error { return nil }
