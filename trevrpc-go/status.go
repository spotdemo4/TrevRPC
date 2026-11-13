package trevrpc

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"google.golang.org/protobuf/proto"
)

// Code is a TrevRPC status code.
type Code uint32

// Status codes, numbered to match the gRPC status codes.
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

// Status is an RPC status with a code, message, and optional metadata and
// details.
type Status struct {
	Code     Code
	Message  string
	Metadata Metadata
	// Details are protobuf messages that describe a failure in more depth,
	// such as the buf.validate.Violations of a request that broke its
	// validation rules. Unlike Metadata they belong to the error, not to the
	// application's headers; see Detail.
	Details []Detail

	// refusedStreamCompression marks the STATUS of a server that refused an
	// optimistic call before any application code ran; see
	// wireStatus.RefusedStreamCompression.
	refusedStreamCompression bool
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

	return &Status{Code: s.Code, Message: s.Message, Metadata: cloneMetadata(metadata), Details: cloneDetails(s.Details)}
}

// WithDetails returns a copy of the status carrying details in place of any
// it had.
func (s *Status) WithDetails(details ...Detail) *Status {
	if s == nil {
		s = OK()
	}

	return &Status{Code: s.Code, Message: s.Message, Metadata: cloneMetadata(s.Metadata), Details: cloneDetails(details)}
}

// FindDetail decodes into message the first detail whose type is message's
// full name. It reports false when there is none, and an error when that
// detail does not decode.
func (s *Status) FindDetail(message ProtoMessage) (bool, error) {
	if s == nil {
		return false, nil
	}
	name := string(protoMessageV2(message).ProtoReflect().Descriptor().FullName())
	for _, detail := range s.Details {
		if detail.Type == name {
			return true, detail.UnmarshalTo(message)
		}
	}
	return false, nil
}

// clone returns a deep copy of the status that application code may hold or
// change without affecting s. It drops refusedStreamCompression, which only
// the runtime sets on the STATUS it writes itself.
func (s *Status) clone() *Status {
	return &Status{Code: s.Code, Message: s.Message, Metadata: cloneMetadata(s.Metadata), Details: cloneDetails(s.Details)}
}

// Detail is one detail of a Status: a protobuf message, carried as its full
// name and its encoding so that a receiver decodes only the details it reads.
type Detail struct {
	// Type is the message's full name, such as "buf.validate.Violations".
	Type string
	// Value is the message's protobuf encoding.
	Value []byte
}

// NewDetail encodes message as a status detail.
func NewDetail(message ProtoMessage) (Detail, error) {
	protoMessage := protoMessageV2(message)
	value, err := proto.MarshalOptions{Deterministic: true}.Marshal(protoMessage)
	if err != nil {
		return Detail{}, err
	}
	return Detail{Type: string(protoMessage.ProtoReflect().Descriptor().FullName()), Value: value}, nil
}

// UnmarshalTo decodes the detail into message, whose full name must be the
// detail's Type.
func (d Detail) UnmarshalTo(message ProtoMessage) error {
	protoMessage := protoMessageV2(message)
	if name := string(protoMessage.ProtoReflect().Descriptor().FullName()); name != d.Type {
		return fmt.Errorf("trevrpc: detail is a %s, not a %s", d.Type, name)
	}
	return proto.Unmarshal(d.Value, protoMessage)
}

func cloneDetails(details []Detail) []Detail {
	if len(details) == 0 {
		return nil
	}
	cloned := make([]Detail, len(details))
	for i, detail := range details {
		cloned[i] = Detail{Type: detail.Type, Value: slices.Clone(detail.Value)}
	}
	return cloned
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

// AlreadyExists creates an already-exists status.
func AlreadyExists(message string) *Status { return NewStatus(CodeAlreadyExists, message) }

// PermissionDenied creates a permission-denied status.
func PermissionDenied(message string) *Status {
	return NewStatus(CodePermissionDenied, message)
}

// ResourceExhausted creates a resource-exhausted status.
func ResourceExhausted(message string) *Status {
	return NewStatus(CodeResourceExhausted, message)
}

// Aborted creates an aborted status.
func Aborted(message string) *Status { return NewStatus(CodeAborted, message) }

// OutOfRange creates an out-of-range status.
func OutOfRange(message string) *Status { return NewStatus(CodeOutOfRange, message) }

// Unavailable creates an unavailable status.
func Unavailable(message string) *Status { return NewStatus(CodeUnavailable, message) }

// FailedPrecondition creates a failed-precondition status.
func FailedPrecondition(message string) *Status {
	return NewStatus(CodeFailedPrecondition, message)
}

// DataLoss creates a data-loss status.
func DataLoss(message string) *Status { return NewStatus(CodeDataLoss, message) }

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

// wireDetails converts status details to their wire form, which aliases them.
func wireDetails(details []Detail) []*wireDetail {
	if len(details) == 0 {
		return nil
	}
	wire := make([]*wireDetail, len(details))
	for i, detail := range details {
		wire[i] = &wireDetail{Type: detail.Type, Value: detail.Value}
	}
	return wire
}

// detailsFromWire converts wire details to status details, copying them.
func detailsFromWire(wire []*wireDetail) []Detail {
	if len(wire) == 0 {
		return nil
	}
	details := make([]Detail, len(wire))
	for i, detail := range wire {
		details[i] = Detail{Type: detail.Type, Value: slices.Clone(detail.Value)}
	}
	return details
}

// StatusFromError converts an error into the status returned to RPC callers.
func StatusFromError(err error) *Status {
	if err == nil {
		return OK()
	}

	if status, ok := errors.AsType[*Status](err); ok {
		return status
	}

	if frameTooLarge, ok := errors.AsType[*FrameTooLargeError](err); ok {
		return ResourceExhausted(frameTooLarge.Error())
	}

	if frameDecode, ok := errors.AsType[*frameDecodeError](err); ok {
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
