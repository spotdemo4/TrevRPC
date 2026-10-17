package qmux

import (
	"errors"
	"fmt"
)

// StreamError reports that a stream was reset or stopped, locally or by the
// peer, with an application error code.
type StreamError struct {
	StreamID  uint64
	ErrorCode uint64
	Remote    bool
}

func (e *StreamError) Error() string {
	side := "local"
	if e.Remote {
		side = "remote"
	}
	return fmt.Sprintf("stream %d canceled by %s with error code %d", e.StreamID, side, e.ErrorCode)
}

// ApplicationError reports that a session was closed by the application with
// CONNECTION_CLOSE (type 0x1d).
type ApplicationError struct {
	Remote       bool
	ErrorCode    uint64
	ErrorMessage string
}

func (e *ApplicationError) Error() string {
	side := "local"
	if e.Remote {
		side = "remote"
	}
	if e.ErrorMessage == "" {
		return fmt.Sprintf("session closed by %s application with error code %d", side, e.ErrorCode)
	}
	return fmt.Sprintf("session closed by %s application with error code %d: %s", side, e.ErrorCode, e.ErrorMessage)
}

// TransportError reports a QMux protocol error, detected locally or reported
// by the peer with CONNECTION_CLOSE (type 0x1c).
type TransportError struct {
	Remote       bool
	ErrorCode    TransportErrorCode
	FrameType    uint64
	ErrorMessage string
}

func (e *TransportError) Error() string {
	side := "local"
	if e.Remote {
		side = "remote"
	}
	if e.ErrorMessage == "" {
		return fmt.Sprintf("QMux %s error %s", side, e.ErrorCode)
	}
	return fmt.Sprintf("QMux %s error %s: %s", side, e.ErrorCode, e.ErrorMessage)
}

func protocolError(code TransportErrorCode, frameType uint64, format string, args ...any) *TransportError {
	return &TransportError{ErrorCode: code, FrameType: frameType, ErrorMessage: fmt.Sprintf(format, args...)}
}

var (
	// ErrIdleTimeout reports that no record was sent or received for the
	// negotiated idle timeout.
	ErrIdleTimeout = errors.New("QMux session idle timeout")
	// ErrConnectionLost reports that the underlying transport failed or ended
	// without a CONNECTION_CLOSE.
	ErrConnectionLost = errors.New("QMux connection lost")
)
