package trevrpcc

import (
	"errors"
	"fmt"
)

var (
	ErrUnavailable = errors.New("trevrpc native runtime is unavailable in this build")
	ErrWouldBlock  = errors.New("trevrpc native runtime operation would block")
	ErrClosed      = errors.New("trevrpc native runtime is closed")
)

type StatusError struct {
	Operation string
	Status    int
}

func (e *StatusError) Error() string {
	return fmt.Sprintf("%s failed with native status %d", e.Operation, e.Status)
}

type EventError struct {
	Status               int
	ProviderErrorCode    uint64
	ApplicationErrorCode uint64
	Local                bool
	Peer                 bool
	PeerReset            bool
	TransportError       bool
	Clean                bool
	Message              string
}

func (e *EventError) Error() string {
	if e.Message != "" {
		return e.Message
	}
	if e.ProviderErrorCode != 0 {
		return fmt.Sprintf("native transport failed with status %d and provider error %#x", e.Status, e.ProviderErrorCode)
	}
	if e.ApplicationErrorCode != 0 {
		return fmt.Sprintf("native transport failed with status %d and application error %#x", e.Status, e.ApplicationErrorCode)
	}
	return fmt.Sprintf("native transport failed with status %d", e.Status)
}
