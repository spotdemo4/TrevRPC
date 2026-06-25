package trevrpc

import (
	"context"
	"errors"
	"fmt"
)

// Code is a TrevRPC status code.
type Code uint32

const (
	CodeOK                 Code = 0
	CodeCancelled          Code = 1
	CodeUnknown            Code = 2
	CodeInvalidArgument    Code = 3
	CodeDeadlineExceeded   Code = 4
	CodeNotFound           Code = 5
	CodeAlreadyExists      Code = 6
	CodePermissionDenied   Code = 7
	CodeResourceExhausted  Code = 8
	CodeFailedPrecondition Code = 9
	CodeAborted            Code = 10
	CodeOutOfRange         Code = 11
	CodeUnimplemented      Code = 12
	CodeInternal           Code = 13
	CodeUnavailable        Code = 14
	CodeDataLoss           Code = 15
	CodeUnauthenticated    Code = 16
)

// CodeFromUint32 converts a wire-format numeric status code into a Code.
func CodeFromUint32(code uint32) Code {
	switch Code(code) {
	case CodeOK, CodeCancelled, CodeInvalidArgument, CodeDeadlineExceeded, CodeNotFound, CodeAlreadyExists, CodePermissionDenied, CodeResourceExhausted, CodeFailedPrecondition, CodeAborted, CodeOutOfRange, CodeUnimplemented, CodeInternal, CodeUnavailable, CodeDataLoss, CodeUnauthenticated:
		return Code(code)
	default:
		return CodeUnknown
	}
}

// Status is an RPC status with a code, message, and optional metadata.
type Status struct {
	Code     Code
	Message  string
	Metadata Metadata
}

// NewStatus creates a status from a code and message.
func NewStatus(code Code, message string) *Status {
	return &Status{Code: code, Message: message, Metadata: Metadata{}}
}

// WithMetadata returns a copy of the status carrying terminal metadata.
func (s *Status) WithMetadata(metadata Metadata) *Status {
	if s == nil {
		s = OK()
	}

	return &Status{Code: s.Code, Message: s.Message, Metadata: cloneMetadata(metadata)}
}

// OK creates an OK status.
func OK() *Status { return NewStatus(CodeOK, "") }

// Cancelled creates a cancelled status.
func Cancelled(message string) *Status { return NewStatus(CodeCancelled, message) }

// Unknown creates an unknown status.
func Unknown(message string) *Status { return NewStatus(CodeUnknown, message) }

// Internal creates an internal error status.
func Internal(message string) *Status { return NewStatus(CodeInternal, message) }

// InvalidArgument creates an invalid-argument status.
func InvalidArgument(message string) *Status {
	return NewStatus(CodeInvalidArgument, message)
}

// DeadlineExceeded creates a deadline-exceeded status.
func DeadlineExceeded(message string) *Status { return NewStatus(CodeDeadlineExceeded, message) }

// NotFound creates a not-found status.
func NotFound(message string) *Status { return NewStatus(CodeNotFound, message) }

// ResourceExhausted creates a resource-exhausted status.
func ResourceExhausted(message string) *Status {
	return NewStatus(CodeResourceExhausted, message)
}

// Unavailable creates an unavailable status.
func Unavailable(message string) *Status { return NewStatus(CodeUnavailable, message) }

// FailedPrecondition creates a failed-precondition status.
func FailedPrecondition(message string) *Status {
	return NewStatus(CodeFailedPrecondition, message)
}

// Unauthenticated creates an unauthenticated status.
func Unauthenticated(message string) *Status { return NewStatus(CodeUnauthenticated, message) }

// Unimplemented creates an unimplemented status.
func Unimplemented(message string) *Status { return NewStatus(CodeUnimplemented, message) }

// Error returns the status as a human-readable error string.
func (s *Status) Error() string {
	if s == nil {
		return "<nil>"
	}

	if s.Message == "" {
		return s.Code.String()
	}

	return fmt.Sprintf("%s: %s", s.Code, s.Message)
}

// IsOK reports whether the status is OK.
func (s *Status) IsOK() bool {
	return s != nil && s.Code == CodeOK
}

// IntoResponse converts the status and response body into an RPC response.
func (s *Status) IntoResponse(body []byte) *RpcResponse {
	if s == nil || len(s.Metadata) == 0 {
		return s.IntoResponseWithMetadata(body, Metadata{})
	}

	return s.IntoResponseWithMetadata(body, s.Metadata)
}

// IntoResponseWithMetadata converts the status, response body, and metadata into an RPC response.
func (s *Status) IntoResponseWithMetadata(body []byte, metadata Metadata) *RpcResponse {
	if s == nil {
		s = OK()
	}

	return &RpcResponse{
		Status:   uint32(s.Code),
		Message:  s.Message,
		Body:     body,
		Metadata: metadata,
	}
}

// StatusFromResponse builds a status from an RPC response.
func StatusFromResponse(response *RpcResponse) *Status {
	if response == nil {
		return Internal("missing RPC response")
	}

	return NewStatus(CodeFromUint32(response.Status), response.Message).WithMetadata(response.Metadata)
}

// StatusFromError converts an error into the status returned to RPC callers.
func StatusFromError(err error) *Status {
	if err == nil {
		return OK()
	}

	var status *Status
	if errors.As(err, &status) {
		return status
	}

	var frameTooLarge *FrameTooLargeError
	if errors.As(err, &frameTooLarge) {
		return ResourceExhausted(frameTooLarge.Error())
	}

	var frameDecode *FrameDecodeError
	if errors.As(err, &frameDecode) {
		return InvalidArgument(frameDecode.Error())
	}

	return Internal(err.Error())
}

func statusFromContextError(err error) *Status {
	if errors.Is(err, context.DeadlineExceeded) {
		return DeadlineExceeded("RPC deadline exceeded")
	}

	if errors.Is(err, context.Canceled) {
		return Cancelled("RPC cancelled")
	}

	return Unavailable("context unavailable: " + err.Error())
}

// String returns the canonical status code name.
func (c Code) String() string {
	switch c {
	case CodeOK:
		return "Ok"
	case CodeCancelled:
		return "Cancelled"
	case CodeInvalidArgument:
		return "InvalidArgument"
	case CodeDeadlineExceeded:
		return "DeadlineExceeded"
	case CodeNotFound:
		return "NotFound"
	case CodeAlreadyExists:
		return "AlreadyExists"
	case CodePermissionDenied:
		return "PermissionDenied"
	case CodeResourceExhausted:
		return "ResourceExhausted"
	case CodeFailedPrecondition:
		return "FailedPrecondition"
	case CodeAborted:
		return "Aborted"
	case CodeOutOfRange:
		return "OutOfRange"
	case CodeUnimplemented:
		return "Unimplemented"
	case CodeInternal:
		return "Internal"
	case CodeUnavailable:
		return "Unavailable"
	case CodeDataLoss:
		return "DataLoss"
	case CodeUnauthenticated:
		return "Unauthenticated"
	default:
		return "Unknown"
	}
}
