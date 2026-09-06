package native

import (
	"errors"
	"fmt"
)

const (
	EngineABIVersion          uint32 = 1
	EngineMsQuicABIVersion    uint32 = 1
	TransportABIVersion       uint32 = 1
	TransportMsQuicABIVersion uint32 = 1

	WakeSourcePOSIXFD      uint32 = 1
	WakeFlagBorrowed       uint32 = 1 << 0
	WakeFlagLevelTriggered uint32 = 1 << 1
)

var ErrUnavailable = errors.New("trevrpc native transport is unavailable in this build")

type ABIInfo struct {
	Engine       uint32
	EngineMsQuic uint32
}

type TransportABIInfo struct {
	Transport       uint32
	TransportMsQuic uint32
}

type WakeSource struct {
	Kind         uint32
	Flags        uint32
	NativeHandle uintptr
}

func (w WakeSource) Borrowed() bool {
	return w.Flags&WakeFlagBorrowed != 0
}

func (w WakeSource) LevelTriggered() bool {
	return w.Flags&WakeFlagLevelTriggered != 0
}

type StatusError struct {
	Operation string
	Status    int
}

func (e *StatusError) Error() string {
	return fmt.Sprintf("%s failed with native status %d", e.Operation, e.Status)
}

func statusError(operation string, status int) error {
	if status == 0 {
		return nil
	}
	return &StatusError{Operation: operation, Status: status}
}
