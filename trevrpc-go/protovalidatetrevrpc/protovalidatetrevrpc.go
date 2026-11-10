// Package protovalidatetrevrpc enforces buf.validate rules on the request
// messages of TrevRPC Go servers with protovalidate. It is a separate module
// so that TrevRPC itself does not depend on protovalidate or CEL.
//
//	server.SetValidator(protovalidatetrevrpc.New(nil))
//
// A request that breaks its rules fails with InvalidArgument and a
// buf.validate.Violations status detail, which clients read field by field. A
// streamed request message that breaks them fails the Recv that returned it.
// Routes whose request type has no rules, directly or in a message it holds,
// skip validation entirely.
//
// Validate checks one message the same way, for handler tests and for tools
// that want the status a call would fail with:
//
//	err := protovalidatetrevrpc.Validate(&pb.AddRequest{})
//
// Clients check the requests they send, without a round trip, with
// trevrpc.DialOptions.Validator.
package protovalidatetrevrpc

import (
	"errors"
	"fmt"

	"buf.build/go/protovalidate"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
	"trev.zip/llc/trevrpc/trevrpc-go"
)

// New returns a trevrpc.Validator that enforces buf.validate rules with
// validator, or with protovalidate.GlobalValidator when validator is nil.
func New(validator protovalidate.Validator) trevrpc.Validator {
	if validator == nil {
		validator = protovalidate.GlobalValidator
	}
	return requestValidator{validator: validator}
}

// global validates with protovalidate.GlobalValidator, keeping the rules it
// compiled for each message type.
var global = trevrpc.NewMessageValidator(New(nil))

// Validate checks message as the server checks a request of its type with
// protovalidate.GlobalValidator, and returns the *trevrpc.Status the call
// would fail with, or nil: InvalidArgument with a buf.validate.Violations
// detail for a message that breaks its rules, Internal for rules that cannot
// be evaluated. A message of a type without rules is never invalid. The rules
// of each message type are compiled once. It is safe for concurrent use.
//
// To check with another protovalidate.Validator, wrap New(validator) with
// trevrpc.NewMessageValidator.
func Validate(message proto.Message) error {
	return global.Validate(message)
}

type requestValidator struct {
	validator protovalidate.Validator
}

// ValidateFunc returns nil for a type without rules. For the others it
// compiles the rules now, through the validator's cache, so a rule that
// cannot compile fails Listen instead of the first call.
func (v requestValidator) ValidateFunc(desc protoreflect.MessageDescriptor) (func(proto.Message) error, error) {
	if !hasRules(desc, map[protoreflect.FullName]bool{}) {
		return nil, nil
	}
	if err := v.validator.Validate(dynamicpb.NewMessage(desc)); err != nil {
		if _, ok := errors.AsType[*protovalidate.ValidationError](err); !ok {
			return nil, err
		}
	}
	validator := v.validator
	return func(message proto.Message) error {
		return statusFromError(validator.Validate(message))
	}, nil
}

// MaxDetailViolations is how many violations the buf.validate.Violations
// detail of a broken request lists at most, so that a request breaking a rule
// on each of many items still fails with a status that fits a frame. Its
// message names the first violation and counts the rest.
const MaxDetailViolations = 100

// statusFromError converts a protovalidate error into the status a call fails
// with: InvalidArgument with a buf.validate.Violations detail for broken
// rules, and Internal for rules that could not be evaluated.
func statusFromError(err error) error {
	if err == nil {
		return nil
	}
	validationErr, ok := errors.AsType[*protovalidate.ValidationError](err)
	if !ok {
		return trevrpc.Internal("request validation failed: " + err.Error())
	}
	violations := validationErr.Violations
	if len(violations) == 0 {
		return trevrpc.InvalidArgument("validation error")
	}
	message := "validation error: " + violations[0].String()
	if more := len(violations) - 1; more > 0 {
		message += fmt.Sprintf(" (and %d more)", more)
	}
	status := trevrpc.InvalidArgument(message)
	listed := (&protovalidate.ValidationError{Violations: violations[:min(len(violations), MaxDetailViolations)]}).ToProto()
	detail, err := trevrpc.NewDetail(listed)
	if err != nil {
		return status
	}
	return status.WithDetails(detail)
}

// hasRules reports whether desc, or a message type it holds however deep,
// carries buf.validate rules. A rule that cannot be read counts as one, so
// the validator reports it. seen breaks recursive types.
func hasRules(desc protoreflect.MessageDescriptor, seen map[protoreflect.FullName]bool) bool {
	if seen[desc.FullName()] {
		return false
	}
	seen[desc.FullName()] = true
	if rules, err := protovalidate.ResolveMessageRules(desc); err != nil || rules != nil {
		return true
	}
	oneofs := desc.Oneofs()
	for i := range oneofs.Len() {
		if rules, err := protovalidate.ResolveOneofRules(oneofs.Get(i)); err != nil || rules != nil {
			return true
		}
	}
	fields := desc.Fields()
	for i := range fields.Len() {
		field := fields.Get(i)
		if rules, err := protovalidate.ResolveFieldRules(field); err != nil || rules != nil {
			return true
		}
		if field.IsMap() {
			field = field.MapValue()
		}
		if message := field.Message(); message != nil && hasRules(message, seen) {
			return true
		}
	}
	return false
}
