package trevrpc

import (
	"io"

	"github.com/golang/protobuf/proto"
)

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

type encodeStream[T proto.Message] struct {
	inner MessageStream[T]
}

func EncodeStream[T proto.Message](inner MessageStream[T]) ByteStream {
	return &encodeStream[T]{inner: inner}
}

func (s *encodeStream[T]) Recv() ([]byte, error) {
	message, err := s.inner.Recv()
	if err != nil {
		return nil, err
	}

	return proto.Marshal(message)
}

type decodeStream[T proto.Message] struct {
	inner      ByteStream
	newMessage func() T
}

func DecodeStream[T proto.Message](inner ByteStream, newMessage func() T) MessageStream[T] {
	return &decodeStream[T]{inner: inner, newMessage: newMessage}
}

func (s *decodeStream[T]) Recv() (T, error) {
	body, err := s.inner.Recv()
	if err != nil {
		var zero T
		return zero, err
	}

	message := s.newMessage()
	if err := proto.Unmarshal(body, message); err != nil {
		var zero T
		return zero, err
	}

	return message, nil
}

func SingleMessageStream[T proto.Message](message T) ByteStream {
	return EncodeStream(FromSlice(message))
}
