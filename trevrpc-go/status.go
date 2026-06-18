package trevrpc

import (
	"context"
	"errors"
	"fmt"
)

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

func CodeFromUint32(code uint32) Code {
	switch Code(code) {
	case CodeOK, CodeCancelled, CodeInvalidArgument, CodeDeadlineExceeded, CodeNotFound, CodeAlreadyExists, CodePermissionDenied, CodeResourceExhausted, CodeFailedPrecondition, CodeAborted, CodeOutOfRange, CodeUnimplemented, CodeInternal, CodeUnavailable, CodeDataLoss, CodeUnauthenticated:
		return Code(code)
	default:
		return CodeUnknown
	}
}

type Status struct {
	Code    Code
	Message string
}

func NewStatus(code Code, message string) *Status {
	return &Status{Code: code, Message: message}
}

func OK() *Status                      { return NewStatus(CodeOK, "") }
func Cancelled(message string) *Status { return NewStatus(CodeCancelled, message) }
func Unknown(message string) *Status   { return NewStatus(CodeUnknown, message) }
func Internal(message string) *Status  { return NewStatus(CodeInternal, message) }
func InvalidArgument(message string) *Status {
	return NewStatus(CodeInvalidArgument, message)
}
func DeadlineExceeded(message string) *Status { return NewStatus(CodeDeadlineExceeded, message) }
func NotFound(message string) *Status         { return NewStatus(CodeNotFound, message) }
func ResourceExhausted(message string) *Status {
	return NewStatus(CodeResourceExhausted, message)
}
func Unavailable(message string) *Status { return NewStatus(CodeUnavailable, message) }
func FailedPrecondition(message string) *Status {
	return NewStatus(CodeFailedPrecondition, message)
}
func Unauthenticated(message string) *Status { return NewStatus(CodeUnauthenticated, message) }
func Unimplemented(message string) *Status   { return NewStatus(CodeUnimplemented, message) }

func (s *Status) Error() string {
	if s == nil {
		return "<nil>"
	}

	if s.Message == "" {
		return s.Code.String()
	}

	return fmt.Sprintf("%s: %s", s.Code, s.Message)
}

func (s *Status) IsOK() bool {
	return s != nil && s.Code == CodeOK
}

func (s *Status) IntoResponse(body []byte) *RpcResponse {
	return s.IntoResponseWithMetadata(body, Metadata{})
}

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

func StatusFromResponse(response *RpcResponse) *Status {
	if response == nil {
		return Internal("missing RPC response")
	}

	return NewStatus(CodeFromUint32(response.Status), response.Message)
}

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
