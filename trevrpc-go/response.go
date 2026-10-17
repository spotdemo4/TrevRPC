package trevrpc

import (
	"errors"
	"io"
	"sync"
)

// Response is what unary and client-streaming handlers return: the response
// message, optional response headers and optional trailing metadata. Metadata
// keys must be lowercase (see NormalizeMetadataKey).
type Response[T ProtoMessage] struct {
	Message T
	// Headers is sent in a HEADERS frame ahead of the message. Clients read it
	// with CaptureResponseHeaders.
	Headers Metadata
	// Metadata is trailing metadata, sent with the final OK status. Clients
	// read it with CaptureResponseMetadata.
	Metadata Metadata
}

// ResponseStream is a MessageStream that also reports the stream's terminal
// status, and with it the terminal metadata. Clients see the terminal status
// of a streaming call through TerminalStatus; streaming handlers may return
// one, created with NewResponseStream, to send trailing metadata.
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

// WithResponseHeaders wraps a handler's response stream so that headers are
// sent in a HEADERS frame as soon as the handler returns, before the first
// message is pulled from the stream. It composes with NewResponseStream in
// either order: trailing metadata and response headers are both kept. Header
// keys must be lowercase (see NormalizeMetadataKey).
func WithResponseHeaders[T ProtoMessage](stream MessageStream[T], headers Metadata) MessageStream[T] {
	if stream == nil {
		return nil
	}
	wrapped := &headersStream[T]{inner: stream, headers: cloneMetadata(headers)}
	if _, ok := stream.(ResponseStream[T]); ok {
		return &terminalHeadersStream[T]{headersStream: wrapped}
	}
	return wrapped
}

// responseHeadersProvider is implemented by the streams WithResponseHeaders
// and NewResponseStream return.
type responseHeadersProvider interface {
	trevrpcResponseHeaders() Metadata
}

// responseHeadersOf returns the response headers a handler stream carries.
func responseHeadersOf(stream any) Metadata {
	if provider, ok := stream.(responseHeadersProvider); ok {
		return provider.trevrpcResponseHeaders()
	}
	return nil
}

type headersStream[T ProtoMessage] struct {
	inner     MessageStream[T]
	headers   Metadata
	closeOnce sync.Once
	closeErr  error
}

// terminalHeadersStream is a headersStream over a ResponseStream, which keeps
// reporting the terminal status of the inner stream.
type terminalHeadersStream[T ProtoMessage] struct {
	*headersStream[T]
}

func (s *headersStream[T]) Recv() (T, error) { return s.inner.Recv() }

func (s *headersStream[T]) Close() error {
	s.closeOnce.Do(func() {
		s.closeErr = closeMessageStream(s.inner)
	})
	return s.closeErr
}

func (s *headersStream[T]) trevrpcResponseHeaders() Metadata {
	headers := cloneMetadata(responseHeadersOf(s.inner))
	for key, value := range s.headers {
		headers[key] = append([]byte(nil), value...)
	}
	return headers
}

func (s *headersStream[T]) trevrpcNonBlockingStream() bool {
	return isNonBlockingStream(s.inner)
}

func (s *headersStream[T]) trevrpcContextCancelsRecv() bool {
	return streamContextCancelsRecv(s.inner)
}

func (s *terminalHeadersStream[T]) TerminalStatus() (*Status, bool) {
	return s.inner.(ResponseStream[T]).TerminalStatus()
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

func (s *responseEnvelopeStream[T]) trevrpcResponseHeaders() Metadata {
	return responseHeadersOf(s.inner)
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
