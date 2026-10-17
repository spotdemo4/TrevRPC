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
	responseBody, err := marshalMessage(&testMessage{Value: "response"})
	if err != nil {
		t.Fatalf("marshal response: %v", err)
	}

	tests := []struct {
		name string
		call func(context.Context, Transport, ...CallOption) error
	}{
		{name: "unary", call: func(ctx context.Context, transport Transport, options ...CallOption) error {
			_, err := Unary(ctx, transport, "service", "method", &testMessage{}, func() *testMessage { return &testMessage{} }, options...)
			return err
		}},
		{name: "server streaming", call: func(ctx context.Context, transport Transport, options ...CallOption) error {
			stream, err := ServerStreaming(ctx, transport, "service", "method", &testMessage{}, func() *testMessage { return &testMessage{} }, options...)
			if err != nil {
				return err
			}
			return stream.Close()
		}},
		{name: "client streaming", call: func(ctx context.Context, transport Transport, options ...CallOption) error {
			call, err := ClientStreaming[*testMessage, *testMessage](ctx, transport, "service", "method", func() *testMessage { return &testMessage{} }, options...)
			if err != nil {
				return err
			}
			return call.Close()
		}},
		{name: "bidirectional", call: func(ctx context.Context, transport Transport, options ...CallOption) error {
			call, err := BidirectionalStreaming[*testMessage, *testMessage](ctx, transport, "service", "method", func() *testMessage { return &testMessage{} }, options...)
			if err != nil {
				return err
			}
			return call.Close()
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
		{name: "without explicit timeout", parentTimeout: 2 * time.Second, maxTimeout: 2 * time.Second},
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

func TestParentCancellationWithoutTimeoutOption(t *testing.T) {
	transport := &contextWaitingClientTransport{entered: make(chan struct{})}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := Unary(
			ctx,
			transport,
			"service",
			"method",
			&testMessage{},
			func() *testMessage { return &testMessage{} },
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
	responseBody, err := marshalMessage(&testMessage{Value: "response"})
	if err != nil {
		t.Fatalf("marshal response: %v", err)
	}

	firstValue := []byte("first-original")
	secondValue := []byte("second-original")
	options := []CallOption{WithMetadata("First", firstValue), WithMetadata("Second", secondValue)}

	firstValue[0] = 'X'
	secondValue[0] = 'Y'

	transport := &mutatingClientTransport{responseBody: responseBody}
	for range 2 {
		if _, err := Unary(context.Background(), transport, "service", "method", &testMessage{}, func() *testMessage { return &testMessage{} }, options...); err != nil {
			t.Fatalf("unary call failed: %v", err)
		}
	}

	for index, metadata := range transport.snapshots {
		if got := string(metadata["first"]); got != "first-original" {
			t.Fatalf("call %d first metadata = %q, want first-original", index, got)
		}
		if got := string(metadata["second"]); got != "second-original" {
			t.Fatalf("call %d second metadata = %q, want second-original", index, got)
		}
	}
	if got := string(firstValue); got != "Xirst-original" {
		t.Fatalf("transport mutation changed caller metadata: %q", got)
	}
}

func TestPrepareClientRequestClonesCustomOptionMetadataForAsyncUse(t *testing.T) {
	responseBody, err := marshalMessage(&testMessage{Value: "response"})
	if err != nil {
		t.Fatalf("marshal response: %v", err)
	}

	value := []byte("caller-owned")
	metadata := Metadata{"custom": value}
	transport := newReadingClientTransport(responseBody)
	done := make(chan error, 1)
	go func() {
		_, err := Unary(
			context.Background(),
			transport,
			"service",
			"method",
			&testMessage{},
			func() *testMessage { return &testMessage{} },
			func(config *callConfig) { config.metadata = metadata },
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

func TestResponseMetadataIsCapturedForUnaryAndClientStreaming(t *testing.T) {
	responseBody, err := marshalMessage(&testMessage{Value: "response"})
	if err != nil {
		t.Fatalf("marshal response: %v", err)
	}
	transport := &capturingClientTransport{
		responseBody: responseBody,
		metadata:     Metadata{"result": []byte("ok")},
	}
	newMessage := func() *testMessage { return &testMessage{} }

	var unaryMetadata Metadata
	unary, err := Unary(context.Background(), transport, "service", "method", &testMessage{}, newMessage, CaptureResponseMetadata(&unaryMetadata))
	if err != nil {
		t.Fatalf("unary call: %v", err)
	}
	if unary.Value != "response" || string(unaryMetadata["result"]) != "ok" {
		t.Fatalf("unexpected unary response %#v with metadata %#v", unary, unaryMetadata)
	}

	var streamMetadata Metadata
	call, err := ClientStreaming[*testMessage, *testMessage](context.Background(), transport, "service", "method", newMessage, CaptureResponseMetadata(&streamMetadata))
	if err != nil {
		t.Fatalf("start client stream: %v", err)
	}
	if streamMetadata != nil {
		t.Fatalf("metadata captured before the response arrived: %#v", streamMetadata)
	}
	response, err := call.CloseAndRecv()
	if err != nil {
		t.Fatalf("CloseAndRecv: %v", err)
	}
	if response.Value != "response" || string(streamMetadata["result"]) != "ok" {
		t.Fatalf("unexpected client-streaming response %#v with metadata %#v", response, streamMetadata)
	}
}

func TestResponseMetadataIsNotCapturedOnFailure(t *testing.T) {
	transport := &capturingClientTransport{status: Unavailable("down"), metadata: Metadata{"result": []byte("ok")}}
	captured := Metadata{"sentinel": []byte("kept")}
	_, err := Unary(context.Background(), transport, "service", "method", &testMessage{}, func() *testMessage { return &testMessage{} }, CaptureResponseMetadata(&captured))
	status := StatusFromError(err)
	if status.Code != CodeUnavailable || string(status.Metadata["result"]) != "ok" {
		t.Fatalf("failure status = %#v, want Unavailable carrying the response metadata", status)
	}
	if string(captured["sentinel"]) != "kept" || len(captured) != 1 {
		t.Fatalf("failed call overwrote captured metadata: %#v", captured)
	}
}

func TestBidirectionalStreamingExposesTerminalMetadata(t *testing.T) {
	responseBody, err := marshalMessage(&testMessage{Value: "response"})
	if err != nil {
		t.Fatalf("marshal response: %v", err)
	}
	transport := &capturingClientTransport{
		responseBody: responseBody,
		metadata:     Metadata{"result": []byte("ok")},
	}

	bidi, err := BidirectionalStreaming[*testMessage, *testMessage](context.Background(), transport, "service", "method", func() *testMessage { return &testMessage{} })
	if err != nil {
		t.Fatalf("start bidi stream: %v", err)
	}
	if _, ok := bidi.TerminalStatus(); ok {
		t.Fatal("terminal status reported before the stream ended")
	}
	if _, err := bidi.Recv(); err != nil {
		t.Fatalf("receive bidi message: %v", err)
	}
	if _, err := bidi.Recv(); !errors.Is(err, io.EOF) {
		t.Fatalf("receive bidi terminal status: %v, want EOF", err)
	}
	status, ok := bidi.TerminalStatus()
	if !ok || string(status.Metadata["result"]) != "ok" {
		t.Fatalf("unexpected terminal status: %#v, %t", status, ok)
	}
}

func TestResponseMessageStreamClassifiesMalformedReadAfterTerminalAsTrailing(t *testing.T) {
	stream := newResponseMessageStream[*testMessage](
		context.Background(),
		&errorAfterFramesStream{
			frames:   []*wireStreamFrame{statusFrame(OK())},
			finalErr: &frameDecodeError{Err: errors.New("malformed trailing frame")},
		},
		func() *testMessage { return &testMessage{} },
		defaultCallConfig(),
		func() {},
	)

	_, err := stream.Recv()
	if reason, ok := responseStreamFailureReason(err); !ok || reason != "trailing_frame" {
		t.Fatalf("Recv() reason = %q, %t, want trailing_frame: %v", reason, ok, err)
	}
}

func TestClientStreamingResponseDrainsBeforeCardinality(t *testing.T) {
	body, err := marshalMessage(&testMessage{Value: "response"})
	if err != nil {
		t.Fatalf("marshal response: %v", err)
	}

	tests := []struct {
		name     string
		frames   []*wireStreamFrame
		finalErr error
		reason   string
		code     Code
	}{
		{
			name:   "remote status",
			frames: []*wireStreamFrame{messageFrame(body), messageFrame(body), statusFrame(Unavailable("down"))},
			reason: "remote_status",
			code:   CodeUnavailable,
		},
		{
			name:   "missing terminal",
			frames: []*wireStreamFrame{messageFrame(body), messageFrame(body)},
			reason: "missing_terminal_status",
			code:   CodeInternal,
		},
		{
			name:     "malformed trailing read",
			frames:   []*wireStreamFrame{messageFrame(body), messageFrame(body), statusFrame(OK())},
			finalErr: &frameDecodeError{Err: errors.New("malformed trailing frame")},
			reason:   "trailing_frame",
			code:     CodeInternal,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			responses := newResponseMessageStream[*testMessage](
				context.Background(),
				&errorAfterFramesStream{frames: test.frames, finalErr: test.finalErr},
				func() *testMessage { return &testMessage{} },
				defaultCallConfig(),
				func() {},
			)

			_, _, err := readSingleResponse[*testMessage](responses)
			if reason, ok := responseStreamFailureReason(err); !ok || reason != test.reason {
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
		context.Background(),
		&countingFrameStream{frames: []*wireStreamFrame{statusFrameWithMetadata(OK(), Metadata{"result": []byte("original")})}},
		func() *testMessage { return &testMessage{} },
		defaultCallConfig(),
		func() {},
	)

	if _, err := stream.Recv(); !errors.Is(err, io.EOF) {
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
	body, err := marshalMessage(&testMessage{Value: "response"})
	if err != nil {
		t.Fatalf("marshal response: %v", err)
	}

	inner := &countingFrameStream{frames: []*wireStreamFrame{messageFrame(body)}}
	cancels := 0
	stream := newResponseMessageStream[*testMessage](
		context.Background(),
		inner,
		func() *testMessage { panic("caller-controlled panic detail") },
		defaultCallConfig(),
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
	stream := newResponseMessageStream[*testMessage](context.Background(), inner, func() *testMessage { return &testMessage{} }, defaultCallConfig(), func() {})

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
		if !errors.Is(err, io.EOF) {
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
	request      *wireRequest
	callDeadline time.Time
	responseBody []byte
	metadata     Metadata
	status       *Status // unary status; OK when nil
}

func (t *capturingClientTransport) call(ctx context.Context, request *wireRequest) (*wireResponse, error) {
	t.capture(ctx, request)
	status := t.status
	if status == nil {
		status = OK()
	}
	return status.intoResponseWithMetadata(t.responseBody, t.metadata), nil
}

func (t *capturingClientTransport) streamingCall(ctx context.Context, request *wireRequest, _ byteStream) (frameStream, error) {
	t.capture(ctx, request)
	return newSliceStream(messageFrame(t.responseBody), statusFrameWithMetadata(OK(), t.metadata)), nil
}

func (t *capturingClientTransport) capture(ctx context.Context, request *wireRequest) {
	t.mu.Lock()
	defer t.mu.Unlock()
	captured := *request
	captured.Metadata = cloneMetadata(request.Metadata)
	t.request = &captured
	t.callDeadline, _ = ctx.Deadline()
}

func (t *capturingClientTransport) snapshot() (*wireRequest, time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.request, t.callDeadline
}

type contextWaitingClientTransport struct {
	entered      chan struct{}
	timeoutNanos uint64
}

func (t *contextWaitingClientTransport) call(ctx context.Context, request *wireRequest) (*wireResponse, error) {
	t.timeoutNanos = request.TimeoutNanos
	close(t.entered)
	<-ctx.Done()
	return nil, statusFromContextError(ctx.Err())
}

func (t *contextWaitingClientTransport) streamingCall(context.Context, *wireRequest, byteStream) (frameStream, error) {
	return nil, errors.New("unexpected streaming call")
}

type mutatingClientTransport struct {
	responseBody []byte
	snapshots    []Metadata
}

func (t *mutatingClientTransport) call(_ context.Context, request *wireRequest) (*wireResponse, error) {
	t.snapshots = append(t.snapshots, cloneMetadata(request.Metadata))
	for key, value := range request.Metadata {
		if len(value) > 0 {
			value[0]++
		}
		delete(request.Metadata, key)
	}
	request.Metadata["transport"] = []byte("mutation")
	return OK().intoResponse(t.responseBody), nil
}

func (t *mutatingClientTransport) streamingCall(context.Context, *wireRequest, byteStream) (frameStream, error) {
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

func (t *readingClientTransport) call(_ context.Context, request *wireRequest) (*wireResponse, error) {
	close(t.entered)
	for {
		select {
		case <-t.release:
			return OK().intoResponse(t.responseBody), nil
		default:
			value := request.Metadata["custom"]
			if len(value) > 0 {
				_ = value[0]
			}
		}
	}
}

func (t *readingClientTransport) streamingCall(context.Context, *wireRequest, byteStream) (frameStream, error) {
	return nil, errors.New("unexpected streaming call")
}

type errorAfterFramesStream struct {
	frames   []*wireStreamFrame
	finalErr error
	closed   bool
}

func (s *errorAfterFramesStream) trevrpcNonBlockingStream() bool { return true }

func (s *errorAfterFramesStream) Recv() (*wireStreamFrame, error) {
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
	frames []*wireStreamFrame
	closed int
}

func (s *countingFrameStream) Recv() (*wireStreamFrame, error) {
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

func (s *blockingFrameStream) Recv() (*wireStreamFrame, error) {
	s.enterOnce.Do(func() { close(s.entered) })
	<-s.closed
	return nil, io.EOF
}

func (s *blockingFrameStream) Close() error {
	s.closeOnce.Do(func() { close(s.closed) })
	return nil
}
