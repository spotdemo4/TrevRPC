package trevrpc

import (
	"errors"
	"fmt"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// Validator checks request messages after they are decoded and before a
// handler sees them; see Server.SetValidator. The protovalidatetrevrpc module
// implements it with buf.validate rules.
type Validator interface {
	// ValidateFunc returns the function that checks request messages of type
	// desc, or nil when messages of that type never need checking, which
	// costs such routes nothing. The server calls it once per route
	// registered with Register*, when its configuration is frozen. An error
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

// bindValidator returns the validation function of a route whose request
// type is desc, and why it has none if the validator failed.
func bindValidator(validator Validator, method string, desc protoreflect.MessageDescriptor) (validateFunc, error) {
	validate, err := validator.ValidateFunc(desc)
	if err != nil {
		err = fmt.Errorf("trevrpc: route %s cannot validate %s: %w", method, desc.FullName(), err)
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
	if err := validate(protoMessageV2(request)); err != nil {
		if status, ok := errors.AsType[*Status](err); ok {
			return status
		}
		return InvalidArgument(err.Error())
	}
	return nil
}
