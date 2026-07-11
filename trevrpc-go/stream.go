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

// ByteStream is a stream of encoded protobuf message bodies.
type ByteStream = MessageStream[[]byte]

// FrameStream is a stream of TrevRPC stream frames.
type FrameStream = MessageStream[*RpcStreamFrame]

type nonBlockingStream interface {
	trevrpcNonBlockingStream() bool
}

type contextCancelsRecvStream interface {
	trevrpcContextCancelsRecv() bool
}

type contextCancelReadStream interface {
	trevrpcCancelReadOnContext(context.Context) func()
}

type releasableByteStream interface {
	trevrpcRecvBytes() ([]byte, func(), error)
}

// MessagePipe is a sendable message stream.
type MessagePipe[T any] struct {
	ctx       context.Context
	messages  chan T
	done      chan struct{}
	closeOnce sync.Once
	errMu     sync.Mutex
	err       error
}

// NewMessagePipe returns a stream that yields messages sent with Send until closed.
func NewMessagePipe[T any](ctx context.Context) *MessagePipe[T] {
	if ctx == nil {
		ctx = context.Background()
	}

	return &MessagePipe[T]{ctx: ctx, messages: make(chan T), done: make(chan struct{})}
}

// Send adds one message to the stream.
func (s *MessagePipe[T]) Send(message T) error {
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

func (s *MessagePipe[T]) Recv() (T, error) {
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
func (s *MessagePipe[T]) Close() error {
	return s.CloseWithError(nil)
}

// CloseWithError completes the stream with err.
func (s *MessagePipe[T]) CloseWithError(err error) error {
	s.closeOnce.Do(func() {
		s.errMu.Lock()
		s.err = err
		s.errMu.Unlock()
		close(s.done)
	})
	return nil
}

func (s *MessagePipe[T]) closeError() error {
	s.errMu.Lock()
	defer s.errMu.Unlock()
	return s.err
}

func (s *MessagePipe[T]) closedError() error {
	if err := s.closeError(); err != nil {
		return err
	}
	return io.ErrClosedPipe
}

type emptyStream[T any] struct{}

// EmptyStream returns a stream that immediately ends.
func EmptyStream[T any]() MessageStream[T] {
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

// FromSlice returns a stream that yields the provided items in order.
func FromSlice[T any](items ...T) MessageStream[T] {
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

// StatusStream returns a frame stream containing a single terminal status frame.
func StatusStream(status *Status) FrameStream {
	return &statusFrameStream{status: status}
}

func (s *statusFrameStream) Recv() (*RpcStreamFrame, error) {
	if s.done {
		return nil, io.EOF
	}

	s.done = true
	return StatusFrame(s.status), nil
}

func (s *statusFrameStream) trevrpcNonBlockingStream() bool { return true }

func (s *statusFrameStream) Close() error {
	s.done = true
	return nil
}

type encodeStream[T ProtoMessage] struct {
	inner MessageStream[T]
}

// EncodeStream wraps a protobuf message stream and yields encoded message bodies.
func EncodeStream[T ProtoMessage](inner MessageStream[T]) ByteStream {
	return &encodeStream[T]{inner: inner}
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

	return MarshalMessage(message)
}

func (s *encodeStream[T]) Close() error {
	return closeMessageStream(s.inner)
}

type decodeStream[T ProtoMessage] struct {
	inner      ByteStream
	newMessage func() T
}

// DecodeStream wraps a byte stream and yields decoded protobuf messages.
func DecodeStream[T ProtoMessage](inner ByteStream, newMessage func() T) MessageStream[T] {
	return &decodeStream[T]{inner: inner, newMessage: newMessage}
}

func (s *decodeStream[T]) Recv() (T, error) {
	body, release, err := recvDecodeBody(s.inner)
	if release != nil {
		defer release()
	}
	if err != nil {
		var zero T
		return zero, err
	}

	message := s.newMessage()
	if err := UnmarshalMessage(body, message); err != nil {
		var zero T
		return zero, InvalidArgument("failed to decode message: " + err.Error())
	}

	return message, nil
}

func recvDecodeBody(stream ByteStream) ([]byte, func(), error) {
	if stream, ok := stream.(releasableByteStream); ok {
		return stream.trevrpcRecvBytes()
	}

	body, err := stream.Recv()
	return body, nil, err
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

	return stream
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

// SingleMessageStream returns a byte stream containing one encoded protobuf message.
func SingleMessageStream[T ProtoMessage](message T) ByteStream {
	return EncodeStream(FromSlice(message))
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
