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
			_, err := Unary(ctx, transport, "/service/method", &testMessage{}, func() *testMessage { return &testMessage{} }, options...)
			return err
		}},
		{name: "server streaming", call: func(ctx context.Context, transport Transport, options ...CallOption) error {
			stream, err := ServerStreaming(ctx, transport, "/service/method", &testMessage{}, func() *testMessage { return &testMessage{} }, options...)
			if err != nil {
				return err
			}
			return stream.Close()
		}},
		{name: "client streaming", call: func(ctx context.Context, transport Transport, options ...CallOption) error {
			call, err := ClientStreaming[*testMessage, *testMessage](ctx, transport, "/service/method", func() *testMessage { return &testMessage{} }, options...)
			if err != nil {
				return err
			}
			return call.Close()
		}},
		{name: "bidirectional", call: func(ctx context.Context, transport Transport, options ...CallOption) error {
			call, err := BidirectionalStreaming[*testMessage, *testMessage](ctx, transport, "/service/method", func() *testMessage { return &testMessage{} }, options...)
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
			"/service/method",
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
		if _, err := Unary(context.Background(), transport, "/service/method", &testMessage{}, func() *testMessage { return &testMessage{} }, options...); err != nil {
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
			"/service/method",
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
	unary, err := Unary(context.Background(), transport, "/service/method", &testMessage{}, newMessage, CaptureResponseMetadata(&unaryMetadata))
	if err != nil {
		t.Fatalf("unary call: %v", err)
	}
	if unary.Value != "response" || string(unaryMetadata["result"]) != "ok" {
		t.Fatalf("unexpected unary response %#v with metadata %#v", unary, unaryMetadata)
	}

	var streamMetadata Metadata
	call, err := ClientStreaming[*testMessage, *testMessage](context.Background(), transport, "/service/method", newMessage, CaptureResponseMetadata(&streamMetadata))
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
	_, err := Unary(context.Background(), transport, "/service/method", &testMessage{}, func() *testMessage { return &testMessage{} }, CaptureResponseMetadata(&captured))
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

	bidi, err := BidirectionalStreaming[*testMessage, *testMessage](context.Background(), transport, "/service/method", func() *testMessage { return &testMessage{} })
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
			frames:   []responseFrame{statusFrame(OK())},
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
		frames   []responseFrame
		finalErr error
		reason   string
		code     Code
	}{
		{
			name:   "remote status",
			frames: []responseFrame{messageFrame(body), messageFrame(body), statusFrame(Unavailable("down"))},
			reason: "remote_status",
			code:   CodeUnavailable,
		},
		{
			name:   "missing terminal",
			frames: []responseFrame{messageFrame(body), messageFrame(body)},
			reason: "missing_terminal_status",
			code:   CodeInternal,
		},
		{
			name:     "malformed trailing read",
			frames:   []responseFrame{messageFrame(body), messageFrame(body), statusFrame(OK())},
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

			_, err := readSingleResponse[*testMessage](responses)
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
		&countingFrameStream{frames: []responseFrame{statusFrameWithMetadata(OK(), Metadata{"result": []byte("original")})}},
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

	inner := &countingFrameStream{frames: []responseFrame{messageFrame(body)}}
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
	headers      Metadata
	metadata     Metadata
	status       *Status // unary status; OK when nil
}

func (t *capturingClientTransport) call(ctx context.Context, request *wireRequest, _ ProtoMessage) (frameStream, error) {
	t.capture(ctx, request)
	status := t.status
	if status == nil {
		status = OK()
	}
	return t.response(status), nil
}

func (t *capturingClientTransport) streamingCall(ctx context.Context, request *wireRequest, _ encodedStream) (frameStream, error) {
	t.capture(ctx, request)
	return t.response(OK()), nil
}

// response is the response of a call that ended with status.
func (t *capturingClientTransport) response(status *Status) frameStream {
	var frames []responseFrame
	if len(t.headers) > 0 {
		frames = append(frames, headersFrame(t.headers))
	}
	if status.IsOK() {
		frames = append(frames, messageFrame(t.responseBody))
	}
	return newSliceStream(append(frames, statusFrameWithMetadata(status, t.metadata))...)
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

func (t *contextWaitingClientTransport) call(ctx context.Context, request *wireRequest, _ ProtoMessage) (frameStream, error) {
	t.timeoutNanos = request.TimeoutNanos
	close(t.entered)
	<-ctx.Done()
	return nil, statusFromContextError(ctx.Err())
}

func (t *contextWaitingClientTransport) streamingCall(context.Context, *wireRequest, encodedStream) (frameStream, error) {
	return nil, errors.New("unexpected streaming call")
}

type mutatingClientTransport struct {
	responseBody []byte
	snapshots    []Metadata
}

func (t *mutatingClientTransport) call(_ context.Context, request *wireRequest, _ ProtoMessage) (frameStream, error) {
	t.snapshots = append(t.snapshots, cloneMetadata(request.Metadata))
	for key, value := range request.Metadata {
		if len(value) > 0 {
			value[0]++
		}
		delete(request.Metadata, key)
	}
	request.Metadata["transport"] = []byte("mutation")
	return newSliceStream(messageFrame(t.responseBody), statusFrame(OK())), nil
}

func (t *mutatingClientTransport) streamingCall(context.Context, *wireRequest, encodedStream) (frameStream, error) {
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

func (t *readingClientTransport) call(_ context.Context, request *wireRequest, _ ProtoMessage) (frameStream, error) {
	close(t.entered)
	for {
		select {
		case <-t.release:
			return newSliceStream(messageFrame(t.responseBody), statusFrame(OK())), nil
		default:
			value := request.Metadata["custom"]
			if len(value) > 0 {
				_ = value[0]
			}
		}
	}
}

func (t *readingClientTransport) streamingCall(context.Context, *wireRequest, encodedStream) (frameStream, error) {
	return nil, errors.New("unexpected streaming call")
}

type errorAfterFramesStream struct {
	frames   []responseFrame
	finalErr error
	closed   bool
}

func (s *errorAfterFramesStream) trevrpcNonBlockingStream() bool { return true }

func (s *errorAfterFramesStream) Recv() (responseFrame, error) {
	if s.closed {
		return responseFrame{}, io.EOF
	}
	if len(s.frames) > 0 {
		frame := s.frames[0]
		s.frames = s.frames[1:]
		return frame, nil
	}
	if s.finalErr != nil {
		err := s.finalErr
		s.finalErr = nil
		return responseFrame{}, err
	}
	return responseFrame{}, io.EOF
}

func (s *errorAfterFramesStream) Close() error {
	s.closed = true
	return nil
}

type countingFrameStream struct {
	frames []responseFrame
	closed int
}

func (s *countingFrameStream) Recv() (responseFrame, error) {
	if len(s.frames) == 0 {
		return responseFrame{}, io.EOF
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

func (s *blockingFrameStream) Recv() (responseFrame, error) {
	s.enterOnce.Do(func() { close(s.entered) })
	<-s.closed
	return responseFrame{}, io.EOF
}

func (s *blockingFrameStream) Close() error {
	s.closeOnce.Do(func() { close(s.closed) })
	return nil
}

func newStreamForFrames(frames ...responseFrame) *responseMessageStream[*testMessage] {
	return newResponseMessageStream[*testMessage](
		context.Background(),
		&countingFrameStream{frames: frames},
		func() *testMessage { return &testMessage{} },
		defaultCallConfig(),
		func() {},
	)
}

func TestResponseMessageStreamRejectsMisplacedHeaders(t *testing.T) {
	body, err := marshalMessage(&testMessage{Value: "response"})
	if err != nil {
		t.Fatal(err)
	}
	headers := headersFrame(Metadata{"x-a": []byte("1")})

	tests := []struct {
		name   string
		frames []responseFrame
		// messages is how many messages Recv returns before the failure.
		messages int
	}{
		{name: "second HEADERS", frames: []responseFrame{headers, headers, messageFrame(body), statusFrame(OK())}},
		{name: "HEADERS after MESSAGE", frames: []responseFrame{messageFrame(body), headers, statusFrame(OK())}, messages: 1},
		{name: "HEADERS after the second MESSAGE", frames: []responseFrame{messageFrame(body), messageFrame(body), headers, statusFrame(OK())}, messages: 2},
		{name: "HEADERS after STATUS", frames: []responseFrame{statusFrame(OK()), headers}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			stream := newStreamForFrames(test.frames...)
			for range test.messages {
				if _, err := stream.Recv(); err != nil {
					t.Fatalf("Recv() before the misplaced frame: %v", err)
				}
			}
			_, err := stream.Recv()
			if test.name == "HEADERS after STATUS" {
				// The stream ends with STATUS, and a frame after it is a trailing frame.
				if reason, _ := responseStreamFailureReason(err); reason != "trailing_frame" {
					t.Fatalf("Recv() = %v (reason %q), want trailing_frame", err, reason)
				}
				return
			}
			if reason, _ := responseStreamFailureReason(err); reason != "unexpected_headers" || StatusFromError(err).Code != CodeInternal {
				t.Fatalf("Recv() = %v (reason %q), want Internal unexpected_headers", err, reason)
			}
		})
	}
}

func TestResponseMessageStreamHeaders(t *testing.T) {
	body, err := marshalMessage(&testMessage{Value: "response"})
	if err != nil {
		t.Fatal(err)
	}
	recvValue := func(t *testing.T, stream *responseMessageStream[*testMessage]) string {
		t.Helper()
		message, err := stream.Recv()
		if err != nil {
			t.Fatalf("Recv() error = %v", err)
		}
		return message.Value
	}

	t.Run("headers do not consume the first message", func(t *testing.T) {
		stream := newStreamForFrames(messageFrame(body), messageFrame(body), statusFrame(OK()))
		headers, err := stream.Headers()
		if err != nil || len(headers) != 0 {
			t.Fatalf("Headers() = %v, %v, want empty metadata", headers, err)
		}
		if value := recvValue(t, stream); value != "response" {
			t.Fatalf("first message = %q", value)
		}
		if value := recvValue(t, stream); value != "response" {
			t.Fatalf("second message = %q", value)
		}
		if _, err := stream.Recv(); !errors.Is(err, io.EOF) {
			t.Fatalf("Recv() = %v, want EOF", err)
		}
	})

	t.Run("headers before messages", func(t *testing.T) {
		stream := newStreamForFrames(headersFrame(Metadata{"x-a": []byte("1")}), messageFrame(body), statusFrame(OK()))
		for range 2 {
			headers, err := stream.Headers()
			if err != nil || string(headers["x-a"]) != "1" {
				t.Fatalf("Headers() = %v, %v", headers, err)
			}
		}
		recvValue(t, stream)
		if headers, err := stream.Headers(); err != nil || string(headers["x-a"]) != "1" {
			t.Fatalf("Headers() after a message = %v, %v", headers, err)
		}
		if _, err := stream.Recv(); !errors.Is(err, io.EOF) {
			t.Fatalf("Recv() = %v, want EOF", err)
		}
	})

	t.Run("headers returns as soon as HEADERS arrives", func(t *testing.T) {
		frames := make(chan responseFrame)
		stream := newResponseMessageStream[*testMessage](context.Background(), &channelFrameStream{frames: frames}, func() *testMessage { return &testMessage{} }, defaultCallConfig(), func() {})
		recv := make(chan string, 1)
		go func() {
			message, err := stream.Recv()
			if err != nil {
				recv <- err.Error()
				return
			}
			recv <- message.Value
		}()
		time.Sleep(10 * time.Millisecond) // let Recv block on the first frame

		headers := make(chan Metadata, 1)
		go func() {
			metadata, _ := stream.Headers()
			headers <- metadata
		}()
		frames <- headersFrame(Metadata{"x-a": []byte("1")})
		select {
		case metadata := <-headers:
			if string(metadata["x-a"]) != "1" {
				t.Fatalf("Headers() = %v", metadata)
			}
		case <-time.After(testTimeout):
			t.Fatal("Headers() waited for a message although HEADERS had arrived")
		}
		frames <- messageFrame(body)
		if value := <-recv; value != "response" {
			t.Fatalf("Recv() = %q", value)
		}
	})

	t.Run("failed call", func(t *testing.T) {
		stream := newStreamForFrames(statusFrame(Unavailable("down")))
		if headers, err := stream.Headers(); err != nil || len(headers) != 0 {
			t.Fatalf("Headers() = %v, %v, want empty metadata", headers, err)
		}
		if _, err := stream.Recv(); StatusFromError(err).Code != CodeUnavailable {
			t.Fatalf("Recv() = %v, want Unavailable", err)
		}
	})

	t.Run("transport failure is reported by both", func(t *testing.T) {
		failure := Unavailable("transport broke")
		stream := newResponseMessageStream[*testMessage](context.Background(), &errorAfterFramesStream{finalErr: failure}, func() *testMessage { return &testMessage{} }, defaultCallConfig(), func() {})
		if _, err := stream.Headers(); !errors.Is(err, failure) {
			t.Fatalf("Headers() = %v, want the transport failure", err)
		}
		if _, err := stream.Recv(); !errors.Is(err, failure) {
			t.Fatalf("Recv() = %v, want the transport failure", err)
		}
	})

	t.Run("invalid headers", func(t *testing.T) {
		stream := newStreamForFrames(headersFrame(Metadata{"Bad Key": nil}), statusFrame(OK()))
		_, err := stream.Headers()
		if reason, _ := responseStreamFailureReason(err); reason != "invalid_metadata" {
			t.Fatalf("Headers() = %v (reason %q), want invalid_metadata", err, reason)
		}
	})

	t.Run("closed before any frame", func(t *testing.T) {
		stream := newStreamForFrames()
		if err := stream.Close(); err != nil {
			t.Fatal(err)
		}
		if _, err := stream.Headers(); StatusFromError(err).Code != CodeCancelled {
			t.Fatalf("Headers() after Close = %v, want Cancelled", err)
		}
	})
}

// channelFrameStream yields the frames sent on a channel.
type channelFrameStream struct {
	frames chan responseFrame
}

func (s *channelFrameStream) Recv() (responseFrame, error) {
	frame, ok := <-s.frames
	if !ok {
		return responseFrame{}, io.EOF
	}
	return frame, nil
}

func (*channelFrameStream) Close() error { return nil }

func TestResponseHeadersAreCapturedForUnaryAndClientStreaming(t *testing.T) {
	responseBody, err := marshalMessage(&testMessage{Value: "response"})
	if err != nil {
		t.Fatalf("marshal response: %v", err)
	}
	transport := &capturingClientTransport{
		responseBody: responseBody,
		headers:      Metadata{"x-h": []byte("header")},
		metadata:     Metadata{"result": []byte("trailer")},
	}
	newMessage := func() *testMessage { return &testMessage{} }

	var headers, trailers Metadata
	if _, err := Unary(context.Background(), transport, "/service/method", &testMessage{}, newMessage, CaptureResponseHeaders(&headers), CaptureResponseMetadata(&trailers)); err != nil {
		t.Fatalf("unary call: %v", err)
	}
	if string(headers["x-h"]) != "header" || len(headers) != 1 || string(trailers["result"]) != "trailer" || len(trailers) != 1 {
		t.Fatalf("headers = %v, trailers = %v", headers, trailers)
	}

	var streamHeaders Metadata
	call, err := ClientStreaming[*testMessage, *testMessage](context.Background(), transport, "/service/method", newMessage, CaptureResponseHeaders(&streamHeaders))
	if err != nil {
		t.Fatalf("start client stream: %v", err)
	}
	if streamHeaders != nil {
		t.Fatalf("headers captured before the response arrived: %v", streamHeaders)
	}
	if _, err := call.CloseAndRecv(); err != nil {
		t.Fatalf("CloseAndRecv: %v", err)
	}
	if string(streamHeaders["x-h"]) != "header" {
		t.Fatalf("client-streaming headers = %v", streamHeaders)
	}

	// Headers of server- and bidirectional-streaming calls come from Headers.
	stream, err := ServerStreaming(context.Background(), transport, "/service/method", &testMessage{}, newMessage)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := stream.Headers(); err != nil || string(got["x-h"]) != "header" {
		t.Fatalf("server-streaming Headers() = %v, %v", got, err)
	}
	bidi, err := BidirectionalStreaming[*testMessage, *testMessage](context.Background(), transport, "/service/method", newMessage)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := bidi.Headers(); err != nil || string(got["x-h"]) != "header" {
		t.Fatalf("bidirectional Headers() = %v, %v", got, err)
	}
}

func TestResponseHeadersAreNotCapturedOnFailure(t *testing.T) {
	transport := &capturingClientTransport{status: Unavailable("down"), headers: Metadata{"x-h": []byte("header")}}
	captured := Metadata{"sentinel": []byte("kept")}
	_, err := Unary(context.Background(), transport, "/service/method", &testMessage{}, func() *testMessage { return &testMessage{} }, CaptureResponseHeaders(&captured))
	if StatusFromError(err).Code != CodeUnavailable {
		t.Fatalf("Unary() = %v, want Unavailable", err)
	}
	if string(captured["sentinel"]) != "kept" || len(captured) != 1 {
		t.Fatalf("failed call overwrote captured headers: %v", captured)
	}
}

// A call whose own deadline or cancellation interrupts the wait for the end of
// the stream after STATUS still reports the server's status.
func TestResponseStatusSurvivesLocalCancellationAfterStatus(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status *Status
		check  func(error) bool
	}{
		{"non-OK", Unavailable("down"), func(err error) bool { return StatusFromError(err).Code == CodeUnavailable }},
		{"OK", OK(), func(err error) bool { return errors.Is(err, io.EOF) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			// STATUS arrives, but the transport never ends the stream.
			frames := make(chan responseFrame, 1)
			frames <- statusFrame(tc.status)
			stream := newResponseMessageStream[*testMessage](ctx, &channelFrameStream{frames: frames}, func() *testMessage { return &testMessage{} }, defaultCallConfig(), cancel)

			result := make(chan error, 1)
			go func() {
				_, err := stream.Recv()
				result <- err
			}()
			time.Sleep(10 * time.Millisecond) // let Recv wait for the end of the stream
			cancel()
			select {
			case err := <-result:
				if !tc.check(err) {
					t.Fatalf("Recv() = %v, want the server's %v status", err, tc.status.Code)
				}
			case <-time.After(testTimeout):
				t.Fatal("Recv() did not return after cancellation")
			}
		})
	}
}
