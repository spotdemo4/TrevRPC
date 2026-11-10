package trevrpc

import (
	"errors"
	"fmt"
	"sync"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// Validator checks request messages after they are decoded and before a
// handler sees them; see Server.SetValidator. Clients check the requests they
// send with it before encoding them; see DialOptions.Validator. The
// protovalidatetrevrpc module implements it with buf.validate rules.
type Validator interface {
	// ValidateFunc returns the function that checks request messages of type
	// desc, or nil when messages of that type never need checking, which
	// costs such routes nothing. The server calls it once per route
	// registered with Register*, when its configuration is frozen, and a
	// MessageValidator once per message type. An error
	// means desc's rules cannot be enforced: Listen reports it, and the
	// route fails every call with Internal.
	//
	// The returned function reports a message that breaks its rules with a
	// *Status, which the call fails with as it is, normally InvalidArgument
	// with details that say why. Any other error fails the call with
	// InvalidArgument. On a streaming route, the error fails the Recv that
	// returned the message, as a decode error does.
	ValidateFunc(desc protoreflect.MessageDescriptor) (func(proto.Message) error, error)
}

// validateFunc checks one decoded request message; see Validator.
type validateFunc func(proto.Message) error

// SetValidator installs a validator that checks every request message of the
// routes registered with Register*. A nil validator removes it. Without one,
// requests are not checked and routes pay nothing for it.
func (s *Server) SetValidator(validator Validator) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.requireMutableLocked()
	s.validator = validator
}

// bindValidator returns the validation function of the messages of type desc
// that subject, such as a route, receives or sends, and why it has none if the
// validator failed.
func bindValidator(validator Validator, subject string, desc protoreflect.MessageDescriptor) (validateFunc, error) {
	validate, err := validator.ValidateFunc(desc)
	if err != nil {
		err = fmt.Errorf("trevrpc: %s cannot validate %s: %w", subject, desc.FullName(), err)
		// Fail closed: a route whose rules cannot be enforced serves no calls.
		failed := Internal(err.Error())
		return func(proto.Message) error { return failed }, err
	}
	return validate, nil
}

// checkRequest checks request with validate, if any.
func checkRequest(validate validateFunc, request ProtoMessage) error {
	if validate == nil {
		return nil
	}
	return checkMessage(validate, protoMessageV2(request))
}

// checkMessage checks message with validate, if any, and returns the *Status
// the call fails with: the one validate returned, or InvalidArgument for any
// other error.
func checkMessage(validate validateFunc, message proto.Message) error {
	if validate == nil {
		return nil
	}
	if err := validate(message); err != nil {
		if status, ok := errors.AsType[*Status](err); ok {
			return status
		}
		return InvalidArgument(err.Error())
	}
	return nil
}

// MessageValidator checks single messages with a Validator, as the server
// checks the requests of a route and as a client configured with
// DialOptions.Validator checks the requests it sends. It compiles the rules of
// each message type once and reuses them, so it is meant to be built once and
// shared. It is safe for concurrent use.
//
// Handler tests use it to assert that the server rejects a request, without
// starting a server:
//
//	err := trevrpc.NewMessageValidator(validator).Validate(&pb.AddRequest{})
//	if trevrpc.StatusFromError(err).Code != trevrpc.CodeInvalidArgument { ... }
type MessageValidator struct {
	validator Validator
	// compiled maps each message descriptor to its validateFunc, nil for a
	// type that needs no checking.
	compiled sync.Map
}

// NewMessageValidator returns a MessageValidator that checks messages with
// validator.
func NewMessageValidator(validator Validator) *MessageValidator {
	return &MessageValidator{validator: validator}
}

// Validate checks message as the server checks a request of its type, and
// returns the *Status the call would fail with, or nil. A message that breaks
// the rules fails with the *Status the Validator reports for it, normally
// InvalidArgument with details that say why; any other error the Validator
// returns becomes InvalidArgument. A type without rules is never invalid, and
// a type whose rules cannot be enforced fails with Internal, as its route
// would on the server.
func (m *MessageValidator) Validate(message proto.Message) error {
	return checkMessage(m.compile(message.ProtoReflect().Descriptor()), message)
}

// compile returns the function that checks messages of type desc, compiling it
// on first use. A type whose rules cannot be enforced gets a
// function that fails every message.
func (m *MessageValidator) compile(desc protoreflect.MessageDescriptor) validateFunc {
	if compiled, ok := m.compiled.Load(desc); ok {
		return compiled.(validateFunc)
	}
	// Concurrent first uses may compile twice; the validators cache their
	// own compilations, and the first result stored wins.
	validate, _ := bindValidator(m.validator, "message", desc)
	compiled, _ := m.compiled.LoadOrStore(desc, validate)
	return compiled.(validateFunc)
}

// check validates a request a client is about to send, if m is not nil.
func (m *MessageValidator) check(request ProtoMessage) error {
	if m == nil {
		return nil
	}
	return m.Validate(protoMessageV2(request))
}
