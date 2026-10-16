package trevrpc

import (
	"context"
	"io"
	"iter"
	"sync"
)

// MessageStream yields messages until Recv returns io.EOF.
type MessageStream[T any] interface {
	// Recv returns the next message or io.EOF when the stream is complete.
	Recv() (T, error)
	// Close releases stream resources and cancels any pending work.
	Close() error
}

// Messages returns a single-use iterator over stream. It closes stream when
// iteration ends, including when the loop exits early. Close errors are not
// yielded; use Recv and Close directly when close errors must be observed.
func Messages[T any](stream MessageStream[T]) iter.Seq2[T, error] {
	return func(yield func(T, error) bool) {
		defer stream.Close()
		for {
			message, err := stream.Recv()
			if err == io.EOF {
				return
			}
			if !yield(message, err) || err != nil {
				return
			}
		}
	}
}

// byteStream is a stream of encoded protobuf message bodies.
type byteStream = MessageStream[[]byte]

// frameStream is a stream of TrevRPC stream frames.
type frameStream = MessageStream[*wireStreamFrame]

type nonBlockingStream interface {
	trevrpcNonBlockingStream() bool
}

type contextCancelsRecvStream interface {
	trevrpcContextCancelsRecv() bool
}

type contextCancelReadStream interface {
	trevrpcCancelReadOnContext(context.Context) func()
}

// messagePipe is a sendable message stream.
type messagePipe[T any] struct {
	ctx       context.Context
	messages  chan T
	done      chan struct{}
	closeOnce sync.Once
	errMu     sync.Mutex
	err       error
}

// newMessagePipe returns a stream that yields messages sent with Send until closed.
func newMessagePipe[T any](ctx context.Context) *messagePipe[T] {
	if ctx == nil {
		ctx = context.Background()
	}

	return &messagePipe[T]{ctx: ctx, messages: make(chan T), done: make(chan struct{})}
}

// Send adds one message to the stream.
func (s *messagePipe[T]) Send(message T) error {
	select {
	case <-s.done:
		return s.closedError()
	default:
	}

	select {
	case s.messages <- message:
		return nil
	case <-s.done:
		return s.closedError()
	case <-s.ctx.Done():
		return statusFromContextError(s.ctx.Err())
	}
}

func (s *messagePipe[T]) Recv() (T, error) {
	select {
	case message := <-s.messages:
		return message, nil
	case <-s.done:
		var zero T
		if err := s.closeError(); err != nil {
			return zero, err
		}
		return zero, io.EOF
	case <-s.ctx.Done():
		var zero T
		return zero, statusFromContextError(s.ctx.Err())
	}
}

// Close completes the stream successfully.
func (s *messagePipe[T]) Close() error {
	return s.CloseWithError(nil)
}

// CloseWithError completes the stream with err.
func (s *messagePipe[T]) CloseWithError(err error) error {
	s.closeOnce.Do(func() {
		s.errMu.Lock()
		s.err = err
		s.errMu.Unlock()
		close(s.done)
	})
	return nil
}

func (s *messagePipe[T]) closeError() error {
	s.errMu.Lock()
	defer s.errMu.Unlock()
	return s.err
}

func (s *messagePipe[T]) closedError() error {
	if err := s.closeError(); err != nil {
		return err
	}
	return io.ErrClosedPipe
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

type sliceStream[T any] struct {
	items []T
	next  int
}

// newSliceStream returns a stream that yields the provided items in order.
func newSliceStream[T any](items ...T) MessageStream[T] {
	return &sliceStream[T]{items: items}
}

func (s *sliceStream[T]) trevrpcNonBlockingStream() bool { return true }

func (s *sliceStream[T]) Recv() (T, error) {
	if s.next >= len(s.items) {
		var zero T
		return zero, io.EOF
	}

	item := s.items[s.next]
	s.next++
	return item, nil
}

func (s *sliceStream[T]) Close() error {
	s.next = len(s.items)
	return nil
}

type statusFrameStream struct {
	status *Status
	done   bool
}

// statusStream returns a frame stream containing a single terminal status frame.
func statusStream(status *Status) frameStream {
	return &statusFrameStream{status: status}
}

func (s *statusFrameStream) Recv() (*wireStreamFrame, error) {
	if s.done {
		return nil, io.EOF
	}

	s.done = true
	return statusFrame(s.status), nil
}

func (s *statusFrameStream) trevrpcNonBlockingStream() bool { return true }

func (s *statusFrameStream) Close() error {
	s.done = true
	return nil
}

type encodeStream[T ProtoMessage] struct {
	inner MessageStream[T]
}

type terminalEncodeStream[T ProtoMessage] struct {
	*encodeStream[T]
	terminal ResponseStream[T]
}

type terminalStatusByteStream interface {
	trevrpcTerminalStatus() (*Status, bool)
}

// newEncodeStream wraps a protobuf message stream and yields encoded message bodies.
// ResponseStream inputs retain their terminal status for the server transport.
func newEncodeStream[T ProtoMessage](inner MessageStream[T]) byteStream {
	encoded := &encodeStream[T]{inner: inner}
	if terminal, ok := inner.(ResponseStream[T]); ok {
		return &terminalEncodeStream[T]{encodeStream: encoded, terminal: terminal}
	}
	return encoded
}

func (s *terminalEncodeStream[T]) trevrpcTerminalStatus() (*Status, bool) {
	return s.terminal.TerminalStatus()
}

func (s *encodeStream[T]) trevrpcNonBlockingStream() bool {
	return isNonBlockingStream(s.inner)
}

func (s *encodeStream[T]) trevrpcContextCancelsRecv() bool {
	return streamContextCancelsRecv(s.inner)
}

func (s *encodeStream[T]) Recv() ([]byte, error) {
	message, err := s.inner.Recv()
	if err != nil {
		return nil, err
	}

	return marshalMessage(message)
}

func (s *encodeStream[T]) Close() error {
	return closeMessageStream(s.inner)
}

type decodeStream[T ProtoMessage] struct {
	inner      byteStream
	newMessage func() T
}

// newDecodeStream wraps a byte stream and yields decoded protobuf messages.
func newDecodeStream[T ProtoMessage](inner byteStream, newMessage func() T) MessageStream[T] {
	return &decodeStream[T]{inner: inner, newMessage: newMessage}
}

func (s *decodeStream[T]) Recv() (T, error) {
	body, err := s.inner.Recv()
	if err != nil {
		var zero T
		return zero, err
	}

	message := s.newMessage()
	if err := unmarshalMessage(body, message); err != nil {
		var zero T
		return zero, InvalidArgument("failed to decode message: " + err.Error())
	}

	return message, nil
}

func (s *decodeStream[T]) Close() error {
	return closeMessageStream(s.inner)
}

func (s *decodeStream[T]) trevrpcNonBlockingStream() bool {
	return isNonBlockingStream(s.inner)
}

func (s *decodeStream[T]) trevrpcContextCancelsRecv() bool {
	return streamContextCancelsRecv(s.inner)
}

type contextCloseStream[T any] struct {
	inner     MessageStream[T]
	done      chan struct{}
	closeOnce sync.Once
}

type terminalContextCloseStream[T any] struct {
	*contextCloseStream[T]
	terminal terminalStatusByteStream
}

func closeStreamOnContext[T any](ctx context.Context, inner MessageStream[T]) MessageStream[T] {
	if inner == nil || isNonBlockingStream(inner) || streamContextCancelsRecv(inner) {
		return inner
	}

	stream := &contextCloseStream[T]{inner: inner, done: make(chan struct{})}
	go func() {
		select {
		case <-ctx.Done():
			_ = stream.Close()
		case <-stream.done:
		}
	}()

	if terminal, ok := inner.(terminalStatusByteStream); ok {
		return &terminalContextCloseStream[T]{contextCloseStream: stream, terminal: terminal}
	}
	return stream
}

func (s *terminalContextCloseStream[T]) trevrpcTerminalStatus() (*Status, bool) {
	return s.terminal.trevrpcTerminalStatus()
}

func (s *contextCloseStream[T]) trevrpcContextCancelsRecv() bool { return true }

func (s *contextCloseStream[T]) Recv() (T, error) {
	return s.inner.Recv()
}

func (s *contextCloseStream[T]) Close() error {
	var err error
	s.closeOnce.Do(func() {
		close(s.done)
		err = closeMessageStream(s.inner)
	})
	return err
}

func isNonBlockingStream(stream any) bool {
	if stream == nil {
		return false
	}
	nonBlocking, ok := stream.(nonBlockingStream)
	return ok && nonBlocking.trevrpcNonBlockingStream()
}

func streamContextCancelsRecv(stream any) bool {
	if stream == nil {
		return false
	}
	cancellable, ok := stream.(contextCancelsRecvStream)
	return ok && cancellable.trevrpcContextCancelsRecv()
}

func closeMessageStream(stream any) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = Internal("stream close panicked")
		}
	}()

	if closer, ok := stream.(io.Closer); ok {
		return closer.Close()
	}

	return nil
}
