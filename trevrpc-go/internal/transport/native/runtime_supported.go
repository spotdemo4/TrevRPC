//go:build cgo && linux && (amd64 || arm64)

package native

import (
	"errors"
	"fmt"
	"sync"

	"golang.org/x/sys/unix"
	trevrpcc "trev.zip/llc/trevrpc/trevrpc-c"
)

const (
	transportObjectListener   = trevrpcc.ObjectListener
	transportObjectConnection = trevrpcc.ObjectConnection
	transportObjectStream     = trevrpcc.ObjectStream
)

type providerRuntime struct {
	runtime trevrpcc.Runtime
}

type nativeHandle struct {
	owner      uint64
	slot       uint32
	generation uint32
}

type nativeEvent struct {
	kind                 uint32
	flags                uint32
	status               int
	subjectKind          uint32
	sequence             uint64
	subject              nativeHandle
	parent               nativeHandle
	operationID          uint64
	applicationErrorCode uint64
	providerErrorCode    uint64
	protocol             uint32
	data                 []byte
	admission            *transportAdmission
}

type transportAdmission struct {
	mu        sync.Mutex
	admission trevrpcc.Admission
	request   AdmissionRequest
}

func (a *transportAdmission) respond(httpStatus uint16) error {
	if a == nil {
		return nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.admission == nil {
		return nil
	}
	err := a.admission.Respond(httpStatus)
	a.admission = nil
	return err
}

func Available() bool {
	return true
}

func newProviderRuntime(runtime trevrpcc.Runtime) (*providerRuntime, error) {
	if runtime == nil {
		return nil, errors.New("native provider returned a nil runtime")
	}
	return &providerRuntime{runtime: runtime}, nil
}

func (r *providerRuntime) WakeSources() ([]WakeSource, error) {
	return r.runtime.WakeSources()
}

func (r *providerRuntime) pollTimeoutMilliseconds() int {
	return r.runtime.PollTimeoutMilliseconds()
}

func (r *providerRuntime) transportABIVersion() uint32 {
	return r.runtime.ABIVersion()
}

func (r *providerRuntime) nextEvent() (nativeEvent, bool, error) {
	event, present, err := r.runtime.NextEvent()
	if err != nil || !present {
		return nativeEvent{}, present, err
	}
	result := nativeEvent{
		kind:                 event.Kind,
		flags:                event.Flags,
		status:               event.Status,
		subjectKind:          event.SubjectKind,
		sequence:             event.Sequence,
		subject:              nativeHandleFromProvider(event.Subject),
		parent:               nativeHandleFromProvider(event.Parent),
		operationID:          event.OperationID,
		applicationErrorCode: event.ApplicationErrorCode,
		providerErrorCode:    event.ProviderErrorCode,
		protocol:             event.Protocol,
		data:                 event.Data,
	}
	if event.Admission != nil {
		request := event.Admission.Request()
		result.admission = &transportAdmission{
			admission: event.Admission,
			request:   request,
		}
	}
	return result, true, nil
}

func (r *providerRuntime) listen(config EndpointConfig) (nativeHandle, error) {
	handle, err := r.runtime.Listen(config.providerConfig())
	return nativeHandleFromProvider(handle), err
}

func (r *providerRuntime) listenerPort(handle nativeHandle) (uint16, error) {
	return r.runtime.ListenerPort(handle.provider())
}

func (r *providerRuntime) dial(config EndpointConfig, operationID uint64) (nativeHandle, error) {
	handle, err := r.runtime.Dial(config.providerConfig(), operationID)
	return nativeHandleFromProvider(handle), err
}

func (r *providerRuntime) cancelDial(handle nativeHandle) error {
	return r.runtime.CancelDial(handle.provider())
}

func (r *providerRuntime) openStream(connection nativeHandle, operationID uint64) (nativeHandle, error) {
	handle, err := r.runtime.OpenStream(connection.provider(), operationID)
	return nativeHandleFromProvider(handle), err
}

func (r *providerRuntime) sendFrame(stream nativeHandle, operationID uint64, body []byte) error {
	return r.runtime.SendFrame(stream.provider(), operationID, body)
}

func (r *providerRuntime) receiveFrame(stream nativeHandle) ([]byte, error) {
	return r.runtime.ReceiveFrame(stream.provider())
}

func (r *providerRuntime) finishStreamSend(stream nativeHandle) error {
	return r.runtime.FinishStreamSend(stream.provider())
}

func (r *providerRuntime) abortStreamRead(stream nativeHandle, code uint64) error {
	return r.runtime.AbortStreamRead(stream.provider(), code)
}

func (r *providerRuntime) abortStreamWrite(stream nativeHandle, code uint64) error {
	return r.runtime.AbortStreamWrite(stream.provider(), code)
}

func (r *providerRuntime) abortStream(stream nativeHandle, code uint64) error {
	return r.runtime.AbortStream(stream.provider(), code)
}

func (r *providerRuntime) closeConnection(connection nativeHandle, code uint64) error {
	return r.runtime.CloseConnection(connection.provider(), code)
}

func (r *providerRuntime) closeListener(listener nativeHandle) error {
	return r.runtime.CloseListener(listener.provider())
}

func (r *providerRuntime) releaseHandle(handle nativeHandle, kind uint32) error {
	return r.runtime.ReleaseHandle(handle.provider(), kind)
}

func (r *providerRuntime) beginClose() error {
	return r.runtime.BeginClose()
}

func (r *providerRuntime) waitDrained() error {
	return r.runtime.WaitDrained()
}

func (r *providerRuntime) release() (bool, error) {
	return r.runtime.Release()
}

func nativeHandleFromProvider(handle trevrpcc.Handle) nativeHandle {
	return nativeHandle{
		owner:      handle.Owner,
		slot:       handle.Slot,
		generation: handle.Generation,
	}
}

func (handle nativeHandle) provider() trevrpcc.Handle {
	return trevrpcc.Handle{
		Owner:      handle.owner,
		Slot:       handle.slot,
		Generation: handle.generation,
	}
}

func pollNativeWake(wakeDescriptors []int, commandDescriptor, timeoutMilliseconds int) (uint32, error) {
	if len(wakeDescriptors) == 0 || len(wakeDescriptors) > 8 {
		return 0, fmt.Errorf("poll native wake sources received %d descriptors", len(wakeDescriptors))
	}
	descriptors := make([]unix.PollFd, len(wakeDescriptors)+1)
	for index, descriptor := range wakeDescriptors {
		descriptors[index] = unix.PollFd{Fd: int32(descriptor), Events: unix.POLLIN}
	}
	descriptors[len(wakeDescriptors)] = unix.PollFd{Fd: int32(commandDescriptor), Events: unix.POLLIN}

	for {
		count, err := unix.Poll(descriptors, timeoutMilliseconds)
		if errors.Is(err, unix.EINTR) {
			continue
		}
		if err != nil {
			return 0, fmt.Errorf("poll native wake sources: %w", err)
		}
		var ready uint32
		for _, descriptor := range descriptors[:len(wakeDescriptors)] {
			if descriptor.Revents&(unix.POLLIN|unix.POLLERR|unix.POLLHUP) != 0 {
				ready |= pollEngineReady
				break
			}
		}
		command := descriptors[len(wakeDescriptors)]
		if command.Revents&(unix.POLLIN|unix.POLLERR|unix.POLLHUP) != 0 {
			ready |= pollCommandReady
		}
		if count == 0 {
			ready |= pollTimeoutReady
		}
		return ready, nil
	}
}
