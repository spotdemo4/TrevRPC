package transport

import (
	"context"
	"io"
	"time"
)

// BidirectionalStream is the transport primitive consumed by the RPC framing layer.
type BidirectionalStream interface {
	io.Reader
	io.Writer
	io.Closer
}

// StreamContext is implemented by streams with an independent lifetime context.
type StreamContext interface {
	Context() context.Context
}

// ReadCanceler aborts only the receive direction of a stream.
type ReadCanceler interface {
	CancelRead(CloseReason)
}

// WriteCanceler aborts only the send direction of a stream.
type WriteCanceler interface {
	CancelWrite(CloseReason)
}

// ReadDeadlineSetter changes the receive deadline of a stream.
type ReadDeadlineSetter interface {
	SetReadDeadline(time.Time) error
}

// StreamEndpoint opens and accepts bidirectional streams on one connection or session.
type StreamEndpoint interface {
	OpenStream(context.Context) (BidirectionalStream, error)
	AcceptStream(context.Context) (BidirectionalStream, error)
	Done() <-chan struct{}
	Err() error
	Info() ConnectionInfo
	Close(CloseReason) error
}

// CloseReasonMapper normalizes a backend-owned endpoint error.
type CloseReasonMapper interface {
	CloseReason(error) CloseReason
}

// ErrorDescriber returns a stable diagnostic description for a backend-owned error.
type ErrorDescriber interface {
	DescribeError(error) string
}

// Connection is a transport connection carrying native QUIC or HTTP/3.
type Connection interface {
	StreamEndpoint
}

// WebTransportSession is a logical session whose parent HTTP/3 connection may be shared.
type WebTransportSession interface {
	StreamEndpoint
}

// Listener accepts transport connections.
type Listener interface {
	Accept(context.Context) (Connection, error)
	Address() Address
	Close() error
}

// HTTP3Request is one ordinary HTTP/3 request and response stream.
type HTTP3Request interface {
	Context() context.Context
	Method() string
	Path() string
	Authority() string
	Secure() bool
	Headers() HeaderFields
	Stream() BidirectionalStream
	SetResponseHeader(string, string)
	WriteResponseHeader(int)
	WriteError(string, int)
	Flush() error
}

// WebTransportRequest is an HTTP/3 CONNECT request that can be upgraded by
// its backend adapter without exposing backend-owned request or session types.
type WebTransportRequest interface {
	Context() context.Context
	Path() string
	Authority() string
	Origin() string
	Secure() bool
	Headers() HeaderFields
	RemoteAddress() Address
	WriteError(string, int)
	Upgrade() (WebTransportSession, error)
}
