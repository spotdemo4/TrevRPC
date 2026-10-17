package trevrpc

import (
	"errors"
	"io"
	"sync"
)

// Response is what unary and client-streaming handlers return: the response
// message and optional response metadata. Metadata keys must be lowercase (see
// NormalizeMetadataKey).
type Response[T ProtoMessage] struct {
	Message  T
	Metadata Metadata
}

// ResponseStream is a MessageStream that also reports the stream's terminal
// status, and with it the terminal metadata. Clients receive one from
// ServerStreaming; streaming handlers may return one, created with
// NewResponseStream, to send terminal metadata.
type ResponseStream[T ProtoMessage] interface {
	MessageStream[T]
	// TerminalStatus returns the final status once the stream has ended. A
	// client sees it after Recv has returned io.EOF or the server's status
	// error; a server sends it after Recv returns io.EOF.
	TerminalStatus() (*Status, bool)
}

// NewResponseStream wraps a successful message stream so that metadata is sent
// with the final OK status. The terminal status becomes available only after
// Recv reports io.EOF.
func NewResponseStream[T ProtoMessage](stream MessageStream[T], metadata Metadata) ResponseStream[T] {
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
	if !errors.Is(err, io.EOF) {
		return message, err
	}

	s.done = true
	s.terminal = OK().WithMetadata(s.metadata)
	if inner, ok := s.inner.(ResponseStream[T]); ok {
		if status, present := inner.TerminalStatus(); present && status != nil && !status.IsOK() {
			s.terminal = cloneStatus(status)
		}
	}
	return message, io.EOF
}

func (s *responseEnvelopeStream[T]) TerminalStatus() (*Status, bool) {
	if s.terminal == nil {
		return nil, false
	}
	return cloneStatus(s.terminal), true
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

func cloneStatus(status *Status) *Status {
	if status == nil {
		return nil
	}
	return NewStatus(status.Code, status.Message).WithMetadata(status.Metadata)
}
