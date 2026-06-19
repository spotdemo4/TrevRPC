package trevrpc

import "io"

// MessageStream yields messages until Recv returns io.EOF.
type MessageStream[T any] interface {
	// Recv returns the next message or io.EOF when the stream is complete.
	Recv() (T, error)
	// Close releases stream resources and cancels any pending work.
	Close() error
}

// ByteStream is a stream of encoded protobuf message bodies.
type ByteStream = MessageStream[[]byte]

// FrameStream is a stream of TrevRPC stream frames.
type FrameStream = MessageStream[*RpcStreamFrame]

type emptyStream[T any] struct{}

// EmptyStream returns a stream that immediately ends.
func EmptyStream[T any]() MessageStream[T] {
	return emptyStream[T]{}
}

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
	body, err := s.inner.Recv()
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

func (s *decodeStream[T]) Close() error {
	return closeMessageStream(s.inner)
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
