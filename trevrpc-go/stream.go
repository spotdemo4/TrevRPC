package trevrpc

import "io"

type MessageStream[T any] interface {
	Recv() (T, error)
}

type ByteStream = MessageStream[[]byte]
type FrameStream = MessageStream[*RpcStreamFrame]

type emptyStream[T any] struct{}

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
	closeMessageStream(s.inner)
	return nil
}

type decodeStream[T ProtoMessage] struct {
	inner      ByteStream
	newMessage func() T
}

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
		return zero, err
	}

	return message, nil
}

func (s *decodeStream[T]) Close() error {
	closeMessageStream(s.inner)
	return nil
}

func SingleMessageStream[T ProtoMessage](message T) ByteStream {
	return EncodeStream(FromSlice(message))
}

func closeMessageStream(stream any) {
	if closer, ok := stream.(io.Closer); ok {
		_ = closer.Close()
	}
}
