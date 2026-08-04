package trevrpc

import (
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"time"
)

func TestResolveClientDeadline(t *testing.T) {
	now := time.Date(2040, time.January, 2, 3, 4, 5, 0, time.UTC)
	clock := func() time.Time { return now }

	contextDeadline := func(offset time.Duration) context.Context {
		return deadlineTestContext{Context: context.Background(), deadline: now.Add(offset)}
	}

	tests := []struct {
		name       string
		ctx        context.Context
		options    []CallOption
		wantSet    bool
		wantOffset time.Duration
		wantNanos  uint64
		wantCode   Code
	}{
		{name: "no deadline", ctx: context.Background(), wantNanos: 0},
		{name: "option only", ctx: context.Background(), options: []CallOption{WithTimeout(5 * time.Second)}, wantSet: true, wantOffset: 5 * time.Second, wantNanos: uint64(5 * time.Second)},
		{name: "context only", ctx: contextDeadline(7 * time.Second), wantSet: true, wantOffset: 7 * time.Second, wantNanos: uint64(7 * time.Second)},
		{name: "option earlier", ctx: contextDeadline(7 * time.Second), options: []CallOption{WithTimeout(5 * time.Second)}, wantSet: true, wantOffset: 5 * time.Second, wantNanos: uint64(5 * time.Second)},
		{name: "context earlier", ctx: contextDeadline(3 * time.Second), options: []CallOption{WithTimeout(5 * time.Second)}, wantSet: true, wantOffset: 3 * time.Second, wantNanos: uint64(3 * time.Second)},
		{name: "without timeout preserves context", ctx: contextDeadline(4 * time.Second), options: []CallOption{WithTimeout(time.Second), WithoutTimeout()}, wantSet: true, wantOffset: 4 * time.Second, wantNanos: uint64(4 * time.Second)},
		{name: "zero option", ctx: context.Background(), options: []CallOption{WithTimeout(0)}, wantSet: true, wantOffset: 0, wantNanos: 1},
		{name: "expired context", ctx: contextDeadline(-time.Second), wantSet: true, wantOffset: -time.Second, wantNanos: 1},
		{name: "negative option", ctx: context.Background(), options: []CallOption{WithTimeout(-time.Nanosecond)}, wantCode: CodeInvalidArgument},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			deadline, err := resolveClientDeadline(test.ctx, applyCallOptions(test.options), clock)
			if test.wantCode != CodeOK {
				if got := StatusFromError(err).Code; got != test.wantCode {
					t.Fatalf("resolveClientDeadline() code = %v, want %v", got, test.wantCode)
				}
				return
			}
			if err != nil {
				t.Fatalf("resolveClientDeadline() error = %v", err)
			}
			if deadline.set != test.wantSet {
				t.Fatalf("deadline set = %t, want %t", deadline.set, test.wantSet)
			}
			if test.wantSet && !deadline.at.Equal(now.Add(test.wantOffset)) {
				t.Fatalf("deadline = %v, want %v", deadline.at, now.Add(test.wantOffset))
			}
			if got := deadlineTimeoutNanos(deadline, clock); got != test.wantNanos {
				t.Fatalf("timeout nanos = %d, want %d", got, test.wantNanos)
			}
		})
	}
}

func TestClientEntryPointsTransmitEffectiveDeadline(t *testing.T) {
	responseBody, err := MarshalMessage(&testMessage{Value: "response"})
	if err != nil {
		t.Fatalf("marshal response: %v", err)
	}

	tests := []struct {
		name string
		call func(context.Context, Transport, ...CallOption) error
	}{
		{name: "unary", call: func(ctx context.Context, transport Transport, options ...CallOption) error {
			_, err := UnaryResponse(ctx, transport, "service", "method", &testMessage{}, func() *testMessage { return &testMessage{} }, options...)
			return err
		}},
		{name: "server streaming", call: func(ctx context.Context, transport Transport, options ...CallOption) error {
			stream, err := ServerStreamingResponse(ctx, transport, "service", "method", &testMessage{}, func() *testMessage { return &testMessage{} }, options...)
			if err != nil {
				return err
			}
			return stream.Close()
		}},
		{name: "client streaming", call: func(ctx context.Context, transport Transport, options ...CallOption) error {
			call, err := ClientStreamingResponse[*testMessage, *testMessage](ctx, transport, "service", "method", func() *testMessage { return &testMessage{} }, options...)
			if err != nil {
				return err
			}
			return call.Close()
		}},
		{name: "client streaming from stream", call: func(ctx context.Context, transport Transport, options ...CallOption) error {
			_, err := ClientStreamingFromStreamResponse(ctx, transport, "service", "method", EmptyStream[*testMessage](), func() *testMessage { return &testMessage{} }, options...)
			return err
		}},
		{name: "bidirectional", call: func(ctx context.Context, transport Transport, options ...CallOption) error {
			call, err := BidirectionalStreamingResponse[*testMessage, *testMessage](ctx, transport, "service", "method", func() *testMessage { return &testMessage{} }, options...)
			if err != nil {
				return err
			}
			return call.Close()
		}},
		{name: "bidirectional from stream", call: func(ctx context.Context, transport Transport, options ...CallOption) error {
			stream, err := BidirectionalStreamingFromStreamResponse(ctx, transport, "service", "method", EmptyStream[*testMessage](), func() *testMessage { return &testMessage{} }, options...)
			if err != nil {
				return err
			}
			return stream.Close()
		}},
	}

	scenarios := []struct {
		name          string
		parentTimeout time.Duration
		options       []CallOption
		maxTimeout    time.Duration
	}{
		{name: "option earlier", parentTimeout: 5 * time.Second, options: []CallOption{WithTimeout(2 * time.Second)}, maxTimeout: 2 * time.Second},
		{name: "context earlier", parentTimeout: 2 * time.Second, options: []CallOption{WithTimeout(5 * time.Second)}, maxTimeout: 2 * time.Second},
		{name: "without explicit timeout", parentTimeout: 2 * time.Second, options: []CallOption{WithTimeout(time.Second), WithoutTimeout()}, maxTimeout: 2 * time.Second},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			for _, scenario := range scenarios {
				t.Run(scenario.name, func(t *testing.T) {
					transport := &capturingClientTransport{responseBody: responseBody}
					ctx, cancel := context.WithTimeout(context.Background(), scenario.parentTimeout)
					defer cancel()
					if err := test.call(ctx, transport, scenario.options...); err != nil {
						t.Fatalf("call failed: %v", err)
					}

					request, callDeadline := transport.snapshot()
					if request == nil {
						t.Fatal("transport did not receive a request")
					}
					if request.TimeoutNanos == 0 || request.TimeoutNanos > uint64(scenario.maxTimeout) {
						t.Fatalf("TimeoutNanos = %d, want 0 < timeout <= %d", request.TimeoutNanos, scenario.maxTimeout)
					}
					if callDeadline.IsZero() {
						t.Fatal("transport context had no deadline")
					}
					remaining := time.Until(callDeadline)
					if remaining <= 0 || remaining > scenario.maxTimeout {
						t.Fatalf("transport deadline remaining = %v, want 0 < remaining <= %v", remaining, scenario.maxTimeout)
					}
				})
			}
		})
	}
}

func TestWithoutTimeoutPreservesParentCancellation(t *testing.T) {
	transport := &contextWaitingClientTransport{entered: make(chan struct{})}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := UnaryResponse(
			ctx,
			transport,
			"service",
			"method",
			&testMessage{},
			func() *testMessage { return &testMessage{} },
			WithTimeout(time.Hour),
			WithoutTimeout(),
		)
		done <- err
	}()

	<-transport.entered
	cancel()
	if code := StatusFromError(<-done).Code; code != CodeCancelled {
		t.Fatalf("call status = %v, want Cancelled", code)
	}
	if transport.timeoutNanos != 0 {
		t.Fatalf("TimeoutNanos = %d, want 0 without an explicit or parent deadline", transport.timeoutNanos)
	}
}

func TestClientMetadataOptionsOwnSnapshots(t *testing.T) {
	responseBody, err := MarshalMessage(&testMessage{Value: "response"})
	if err != nil {
		t.Fatalf("marshal response: %v", err)
	}

	mapValue := []byte("map-original")
	singleValue := []byte("single-original")
	source := Metadata{"first": mapValue}
	options := []CallOption{WithMetadataMap(source), WithMetadata("Second", singleValue)}

	mapValue[0] = 'X'
	singleValue[0] = 'Y'
	source["first"] = []byte("map-replaced")
	source["third"] = []byte("unexpected")

	transport := &mutatingClientTransport{responseBody: responseBody}
	for range 2 {
		if _, err := UnaryResponse(context.Background(), transport, "service", "method", &testMessage{}, func() *testMessage { return &testMessage{} }, options...); err != nil {
			t.Fatalf("unary call failed: %v", err)
		}
	}

	for index, metadata := range transport.snapshots {
		if got := string(metadata["first"]); got != "map-original" {
			t.Fatalf("call %d first metadata = %q, want map-original", index, got)
		}
		if got := string(metadata["second"]); got != "single-original" {
			t.Fatalf("call %d second metadata = %q, want single-original", index, got)
		}
		if _, ok := metadata["third"]; ok {
			t.Fatalf("call %d unexpectedly included later map mutation", index)
		}
	}
	if got := string(source["first"]); got != "map-replaced" {
		t.Fatalf("transport mutation changed caller metadata: %q", got)
	}
}

func TestPrepareClientRequestClonesCustomOptionMetadataForAsyncUse(t *testing.T) {
	responseBody, err := MarshalMessage(&testMessage{Value: "response"})
	if err != nil {
		t.Fatalf("marshal response: %v", err)
	}

	value := []byte("caller-owned")
	metadata := Metadata{"custom": value}
	transport := newReadingClientTransport(responseBody)
	done := make(chan error, 1)
	go func() {
		_, err := UnaryResponse(
			context.Background(),
			transport,
			"service",
			"method",
			&testMessage{},
			func() *testMessage { return &testMessage{} },
			func(options *CallOptions) { options.Metadata = metadata },
		)
		done <- err
	}()

	<-transport.entered
	for index := range value {
		value[index]++
	}
	close(transport.release)
	if err := <-done; err != nil {
		t.Fatalf("unary call failed: %v", err)
	}
}

func TestClientStreamingResponseMetadata(t *testing.T) {
	responseBody, err := MarshalMessage(&testMessage{Value: "response"})
	if err != nil {
		t.Fatalf("marshal response: %v", err)
	}
	transport := &capturingClientTransport{
		responseBody: responseBody,
		metadata:     Metadata{"result": []byte("ok")},
	}

	call, err := ClientStreamingResponse[*testMessage, *testMessage](context.Background(), transport, "service", "method", func() *testMessage { return &testMessage{} })
	if err != nil {
		t.Fatalf("start client stream: %v", err)
	}
	response, err := call.CloseAndRecvResponse()
	if err != nil {
		t.Fatalf("CloseAndRecvResponse: %v", err)
	}
	if response.Message.Value != "response" || string(response.Metadata["result"]) != "ok" {
		t.Fatalf("unexpected response envelope: %#v", response)
	}

	bidi, err := BidirectionalStreamingFromStreamResponse(context.Background(), transport, "service", "method", EmptyStream[*testMessage](), func() *testMessage { return &testMessage{} })
	if err != nil {
		t.Fatalf("start bidi stream: %v", err)
	}
	if _, err := bidi.Recv(); err != nil {
		t.Fatalf("receive bidi message: %v", err)
	}
	if _, err := bidi.Recv(); err != io.EOF {
		t.Fatalf("receive bidi terminal status: %v, want EOF", err)
	}
	status, ok := bidi.TerminalStatus()
	if !ok || string(status.Metadata["result"]) != "ok" {
		t.Fatalf("unexpected terminal status: %#v, %t", status, ok)
	}
}

func TestResponseMessageStreamClassifiesMalformedReadAfterTerminalAsTrailing(t *testing.T) {
	stream := newResponseMessageStream[*testMessage](
		&errorAfterFramesStream{
			frames:   []*RpcStreamFrame{StatusFrame(OK())},
			finalErr: &FrameDecodeError{Err: errors.New("malformed trailing frame")},
		},
		func() *testMessage { return &testMessage{} },
		DefaultCallOptions(),
		context.Background(),
		func() {},
	)

	_, err := stream.Recv()
	if reason, ok := ResponseStreamFailureReason(err); !ok || reason != "trailing_frame" {
		t.Fatalf("Recv() reason = %q, %t, want trailing_frame: %v", reason, ok, err)
	}
}

func TestClientStreamingResponseDrainsBeforeCardinality(t *testing.T) {
	body, err := MarshalMessage(&testMessage{Value: "response"})
	if err != nil {
		t.Fatalf("marshal response: %v", err)
	}

	tests := []struct {
		name     string
		frames   []*RpcStreamFrame
		finalErr error
		reason   string
		code     Code
	}{
		{
			name:   "remote status",
			frames: []*RpcStreamFrame{MessageFrame(body), MessageFrame(body), StatusFrame(Unavailable("down"))},
			reason: "remote_status",
			code:   CodeUnavailable,
		},
		{
			name:   "missing terminal",
			frames: []*RpcStreamFrame{MessageFrame(body), MessageFrame(body)},
			reason: "missing_terminal_status",
			code:   CodeInternal,
		},
		{
			name:     "malformed trailing read",
			frames:   []*RpcStreamFrame{MessageFrame(body), MessageFrame(body), StatusFrame(OK())},
			finalErr: &FrameDecodeError{Err: errors.New("malformed trailing frame")},
			reason:   "trailing_frame",
			code:     CodeInternal,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			responses := newResponseMessageStream[*testMessage](
				&errorAfterFramesStream{frames: test.frames, finalErr: test.finalErr},
				func() *testMessage { return &testMessage{} },
				DefaultCallOptions(),
				context.Background(),
				func() {},
			)

			_, err := readUnaryResponseFromResponseStream[*testMessage](responses)
			if reason, ok := ResponseStreamFailureReason(err); !ok || reason != test.reason {
				t.Fatalf("failure reason = %q, %t, want %q: %v", reason, ok, test.reason, err)
			}
			if code := StatusFromError(err).Code; code != test.code {
				t.Fatalf("status code = %v, want %v", code, test.code)
			}
		})
	}
}

func TestResponseMessageStreamTerminalStatusReturnsOwnedSnapshots(t *testing.T) {
	stream := newResponseMessageStream[*testMessage](
		&countingFrameStream{frames: []*RpcStreamFrame{StatusFrameWithMetadata(OK(), Metadata{"result": []byte("original")})}},
		func() *testMessage { return &testMessage{} },
		DefaultCallOptions(),
		context.Background(),
		func() {},
	)

	if _, err := stream.Recv(); err != io.EOF {
		t.Fatalf("Recv() = %v, want EOF", err)
	}
	first, ok := stream.TerminalStatus()
	if !ok {
		t.Fatal("TerminalStatus() did not report the terminal status")
	}
	first.Code = CodeInternal
	first.Message = "mutated"
	first.Metadata["result"][0] = 'X'
	first.Metadata["added"] = []byte("mutated")

	second, ok := stream.TerminalStatus()
	if !ok || second.Code != CodeOK || second.Message != "" || string(second.Metadata["result"]) != "original" {
		t.Fatalf("TerminalStatus() after caller mutation = %#v, %t", second, ok)
	}
	if _, exists := second.Metadata["added"]; exists {
		t.Fatalf("TerminalStatus() retained caller-added metadata: %#v", second.Metadata)
	}
}

func TestResponseMessageStreamContainsFactoryPanicAndFinishes(t *testing.T) {
	body, err := MarshalMessage(&testMessage{Value: "response"})
	if err != nil {
		t.Fatalf("marshal response: %v", err)
	}

	inner := &countingFrameStream{frames: []*RpcStreamFrame{MessageFrame(body)}}
	cancels := 0
	stream := newResponseMessageStream[*testMessage](
		inner,
		func() *testMessage { panic("caller-controlled panic detail") },
		DefaultCallOptions(),
		context.Background(),
		func() { cancels++ },
	)

	_, err = stream.Recv()
	status := StatusFromError(err)
	if status.Code != CodeInternal || status.Message != "response factory panicked" {
		t.Fatalf("Recv() status = %#v, want sanitized Internal", status)
	}
	if inner.closed != 1 {
		t.Fatalf("underlying stream closed %d times, want 1", inner.closed)
	}
	if cancels != 1 {
		t.Fatalf("cancel called %d times, want 1", cancels)
	}

	if err := stream.Close(); err != nil {
		t.Fatalf("Close() after factory panic = %v", err)
	}
	if inner.closed != 1 {
		t.Fatalf("underlying stream closed %d times after Close, want 1", inner.closed)
	}
	if cancels != 1 {
		t.Fatalf("cancel called %d times after Close, want 1", cancels)
	}
}

func TestResponseMessageStreamCloseMayRaceBlockedRecv(t *testing.T) {
	inner := &blockingFrameStream{entered: make(chan struct{}), closed: make(chan struct{})}
	stream := newResponseMessageStream[*testMessage](inner, func() *testMessage { return &testMessage{} }, DefaultCallOptions(), context.Background(), func() {})

	recvDone := make(chan error, 1)
	go func() {
		_, err := stream.Recv()
		recvDone <- err
	}()

	select {
	case <-inner.entered:
	case <-time.After(time.Second):
		t.Fatal("Recv did not reach the underlying stream")
	}

	closeDone := make(chan error, 1)
	go func() { closeDone <- stream.Close() }()

	select {
	case err := <-closeDone:
		if err != nil {
			t.Fatalf("Close() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Close blocked while Recv was pending")
	}

	select {
	case err := <-recvDone:
		if err != io.EOF {
			t.Fatalf("Recv() error = %v, want EOF", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Recv remained blocked after Close")
	}
}

type deadlineTestContext struct {
	context.Context
	deadline time.Time
}

func (c deadlineTestContext) Deadline() (time.Time, bool) { return c.deadline, true }

type capturingClientTransport struct {
	mu           sync.Mutex
	request      *RpcRequest
	callDeadline time.Time
	responseBody []byte
	metadata     Metadata
}

func (t *capturingClientTransport) Call(ctx context.Context, request *RpcRequest) (*RpcResponse, error) {
	t.capture(ctx, request)
	return OK().IntoResponseWithMetadata(t.responseBody, t.metadata), nil
}

func (t *capturingClientTransport) StreamingCall(ctx context.Context, request *RpcRequest, _ ByteStream) (FrameStream, error) {
	t.capture(ctx, request)
	return FromSlice(MessageFrame(t.responseBody), StatusFrameWithMetadata(OK(), t.metadata)), nil
}

func (t *capturingClientTransport) capture(ctx context.Context, request *RpcRequest) {
	t.mu.Lock()
	defer t.mu.Unlock()
	copy := *request
	copy.Metadata = cloneMetadata(request.Metadata)
	t.request = &copy
	t.callDeadline, _ = ctx.Deadline()
}

func (t *capturingClientTransport) snapshot() (*RpcRequest, time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.request, t.callDeadline
}

type contextWaitingClientTransport struct {
	entered      chan struct{}
	timeoutNanos uint64
}

func (t *contextWaitingClientTransport) Call(ctx context.Context, request *RpcRequest) (*RpcResponse, error) {
	t.timeoutNanos = request.TimeoutNanos
	close(t.entered)
	<-ctx.Done()
	return nil, statusFromContextError(ctx.Err())
}

func (t *contextWaitingClientTransport) StreamingCall(context.Context, *RpcRequest, ByteStream) (FrameStream, error) {
	return nil, errors.New("unexpected streaming call")
}

type mutatingClientTransport struct {
	responseBody []byte
	snapshots    []Metadata
}

func (t *mutatingClientTransport) Call(_ context.Context, request *RpcRequest) (*RpcResponse, error) {
	t.snapshots = append(t.snapshots, cloneMetadata(request.Metadata))
	for key, value := range request.Metadata {
		if len(value) > 0 {
			value[0]++
		}
		delete(request.Metadata, key)
	}
	request.Metadata["transport"] = []byte("mutation")
	return OK().IntoResponse(t.responseBody), nil
}

func (t *mutatingClientTransport) StreamingCall(context.Context, *RpcRequest, ByteStream) (FrameStream, error) {
	return nil, errors.New("unexpected streaming call")
}

type readingClientTransport struct {
	responseBody []byte
	entered      chan struct{}
	release      chan struct{}
}

func newReadingClientTransport(responseBody []byte) *readingClientTransport {
	return &readingClientTransport{responseBody: responseBody, entered: make(chan struct{}), release: make(chan struct{})}
}

func (t *readingClientTransport) Call(_ context.Context, request *RpcRequest) (*RpcResponse, error) {
	close(t.entered)
	for {
		select {
		case <-t.release:
			return OK().IntoResponse(t.responseBody), nil
		default:
			value := request.Metadata["custom"]
			if len(value) > 0 {
				_ = value[0]
			}
		}
	}
}

func (t *readingClientTransport) StreamingCall(context.Context, *RpcRequest, ByteStream) (FrameStream, error) {
	return nil, errors.New("unexpected streaming call")
}

type errorAfterFramesStream struct {
	frames   []*RpcStreamFrame
	finalErr error
	closed   bool
}

func (s *errorAfterFramesStream) trevrpcNonBlockingStream() bool { return true }

func (s *errorAfterFramesStream) Recv() (*RpcStreamFrame, error) {
	if s.closed {
		return nil, io.EOF
	}
	if len(s.frames) > 0 {
		frame := s.frames[0]
		s.frames = s.frames[1:]
		return frame, nil
	}
	if s.finalErr != nil {
		err := s.finalErr
		s.finalErr = nil
		return nil, err
	}
	return nil, io.EOF
}

func (s *errorAfterFramesStream) Close() error {
	s.closed = true
	return nil
}

type countingFrameStream struct {
	frames []*RpcStreamFrame
	closed int
}

func (s *countingFrameStream) Recv() (*RpcStreamFrame, error) {
	if len(s.frames) == 0 {
		return nil, io.EOF
	}

	frame := s.frames[0]
	s.frames = s.frames[1:]
	return frame, nil
}

func (s *countingFrameStream) Close() error {
	s.closed++
	return nil
}

type blockingFrameStream struct {
	enterOnce sync.Once
	closeOnce sync.Once
	entered   chan struct{}
	closed    chan struct{}
}

func (s *blockingFrameStream) Recv() (*RpcStreamFrame, error) {
	s.enterOnce.Do(func() { close(s.entered) })
	<-s.closed
	return nil, io.EOF
}

func (s *blockingFrameStream) Close() error {
	s.closeOnce.Do(func() { close(s.closed) })
	return nil
}
