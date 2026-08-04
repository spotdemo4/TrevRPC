package trevrpc

import (
	"io"
	"sync"
)

// Response is a decoded unary response together with response metadata.
type Response[T ProtoMessage] struct {
	Message  T
	Metadata Metadata
}

// ResponseOption mutates a response envelope.
type ResponseOption[T ProtoMessage] func(*Response[T])

// NewResponse creates a response envelope for handlers that need metadata.
func NewResponse[T ProtoMessage](message T, options ...ResponseOption[T]) *Response[T] {
	response := &Response[T]{Message: message, Metadata: Metadata{}}
	for _, option := range options {
		option(response)
	}
	return response
}

// WithResponseMetadata sets one normalized response metadata entry.
func WithResponseMetadata[T ProtoMessage](key string, value []byte) ResponseOption[T] {
	key = NormalizeMetadataKey(key)
	value = append([]byte(nil), value...)
	return func(response *Response[T]) {
		metadata := cloneMetadata(response.Metadata)
		metadata[key] = append([]byte(nil), value...)
		response.Metadata = metadata
	}
}

// WithResponseMetadataMap replaces response metadata.
func WithResponseMetadataMap[T ProtoMessage](metadata Metadata) ResponseOption[T] {
	metadata = cloneMetadata(metadata)
	return func(response *Response[T]) {
		response.Metadata = cloneMetadata(metadata)
	}
}

// NewResponseStream wraps a successful message stream with zero terminal metadata.
func NewResponseStream[T ProtoMessage](stream MessageStream[T]) ResponseStream[T] {
	return WithResponseStreamMetadata(stream, nil)
}

// WithResponseStreamMetadata wraps a successful message stream with terminal metadata.
// The terminal status becomes available only after Recv reports io.EOF.
func WithResponseStreamMetadata[T ProtoMessage](stream MessageStream[T], metadata Metadata) ResponseStream[T] {
	if stream == nil {
		return nil
	}
	return &responseEnvelopeStream[T]{inner: stream, metadata: cloneMetadata(metadata)}
}

type responseEnvelopeStream[T ProtoMessage] struct {
	inner     MessageStream[T]
	metadata  Metadata
	terminal  *Status
	done      bool
	closeOnce sync.Once
	closeErr  error
}

func (s *responseEnvelopeStream[T]) Recv() (T, error) {
	if s.done {
		var zero T
		return zero, io.EOF
	}

	message, err := s.inner.Recv()
	if err != io.EOF {
		return message, err
	}

	s.done = true
	s.terminal = OK().WithMetadata(s.metadata)
	if inner, ok := s.inner.(ResponseStream[T]); ok {
		if status, present := inner.TerminalStatus(); present && status != nil && !status.IsOK() {
			s.terminal = cloneResponseStatus(status)
		}
	}
	return message, io.EOF
}

func (s *responseEnvelopeStream[T]) TerminalStatus() (*Status, bool) {
	if s.terminal == nil {
		return nil, false
	}
	return cloneResponseStatus(s.terminal), true
}

func (s *responseEnvelopeStream[T]) Close() error {
	s.closeOnce.Do(func() {
		s.closeErr = closeMessageStream(s.inner)
	})
	return s.closeErr
}

func (s *responseEnvelopeStream[T]) trevrpcNonBlockingStream() bool {
	return isNonBlockingStream(s.inner)
}

func (s *responseEnvelopeStream[T]) trevrpcContextCancelsRecv() bool {
	return streamContextCancelsRecv(s.inner)
}

func cloneResponseStatus(status *Status) *Status {
	if status == nil {
		return nil
	}
	return NewStatus(status.Code, status.Message).WithMetadata(status.Metadata)
}
