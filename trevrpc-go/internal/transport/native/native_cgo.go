//go:build trevrpc_native && cgo && (linux || darwin) && (amd64 || arm64)

package native

/*
#cgo pkg-config: --static trevrpc_engine_msquic

#include <errno.h>
#include <poll.h>
#include <stdlib.h>
#include <trevrpc_engine.h>
#include <trevrpc_engine_msquic.h>

static int trevrpc_go_poll(const intptr_t* wake_fds,
		size_t wake_count,
		int command_fd,
		int timeout_ms) {
	struct pollfd descriptors[8 + 1];
	size_t index;
	int result;
	int ready = 0;
	if (wake_count > 8) {
		return -EINVAL;
	}
	for (index = 0; index < wake_count; ++index) {
		descriptors[index].fd = (int)wake_fds[index];
		descriptors[index].events = POLLIN;
		descriptors[index].revents = 0;
	}
	descriptors[wake_count].fd = command_fd;
	descriptors[wake_count].events = POLLIN;
	descriptors[wake_count].revents = 0;
	do {
		result = poll(descriptors, wake_count + 1, timeout_ms);
	} while (result < 0 && errno == EINTR);
	if (result < 0) {
		return -errno;
	}
	for (index = 0; index < wake_count; ++index) {
		if ((descriptors[index].revents & (POLLIN | POLLERR | POLLHUP)) != 0) {
			ready |= 1;
			break;
		}
	}
	if ((descriptors[wake_count].revents & (POLLIN | POLLERR | POLLHUP)) != 0) {
		ready |= 2;
	}
	if (result == 0) {
		ready |= 4;
	}
	return ready;
}
*/
import "C"

import (
	"errors"
	"fmt"
	"sync"
	"unsafe"
)

type Runtime struct {
	mu     sync.Mutex
	engine *C.trevrpc_engine
}

func Available() bool {
	return true
}

func CheckABI() (ABIInfo, error) {
	C.trevrpc_engine_abi_1_anchor()
	C.trevrpc_engine_msquic_abi_1_anchor()

	info := ABIInfo{
		Engine:       uint32(C.trevrpc_engine_abi_version()),
		EngineMsQuic: uint32(C.trevrpc_engine_msquic_abi_version()),
	}
	if uint32(C.TREVRPC_ENGINE_ABI_VERSION) != EngineABIVersion || info.Engine != EngineABIVersion {
		return info, fmt.Errorf(
			"trevrpc Engine ABI mismatch: headers=%d runtime=%d expected=%d",
			uint32(C.TREVRPC_ENGINE_ABI_VERSION),
			info.Engine,
			EngineABIVersion,
		)
	}
	if uint32(C.TREVRPC_ENGINE_MSQUIC_ABI_VERSION) != EngineMsQuicABIVersion || info.EngineMsQuic != EngineMsQuicABIVersion {
		return info, fmt.Errorf(
			"trevrpc Engine MsQuic ABI mismatch: headers=%d runtime=%d expected=%d",
			uint32(C.TREVRPC_ENGINE_MSQUIC_ABI_VERSION),
			info.EngineMsQuic,
			EngineMsQuicABIVersion,
		)
	}
	return info, nil
}

func Open() (*Runtime, error) {
	return openRuntime(EngineConfig{})
}

func openRuntime(config EngineConfig) (*Runtime, error) {
	if _, err := CheckABI(); err != nil {
		return nil, err
	}

	var engineConfig C.trevrpc_engine_config_v1
	if err := statusError(
		"initialize Engine configuration",
		int(C.trevrpc_engine_config_v1_init(
			&engineConfig,
			C.size_t(C.sizeof_trevrpc_engine_config_v1),
		)),
	); err != nil {
		return nil, err
	}
	if config.EventCapacity != 0 {
		engineConfig.event_capacity = C.uint32_t(config.EventCapacity)
	}
	if config.ListenerCapacity != 0 {
		engineConfig.listener_capacity = C.uint32_t(config.ListenerCapacity)
	}
	if config.ConnectionCapacity != 0 {
		engineConfig.connection_capacity = C.uint32_t(config.ConnectionCapacity)
	}
	if config.StreamCapacity != 0 {
		engineConfig.stream_capacity = C.uint32_t(config.StreamCapacity)
	}
	if config.MaxReceiveOwnedCount != 0 {
		engineConfig.max_receive_owned_count = C.uint32_t(config.MaxReceiveOwnedCount)
	}
	if config.MaxReceiveOwnedBytes != 0 {
		engineConfig.max_receive_owned_bytes = C.uint64_t(config.MaxReceiveOwnedBytes)
	}

	var providerConfig C.trevrpc_engine_msquic_config_v1
	if err := statusError(
		"initialize Engine MsQuic configuration",
		int(C.trevrpc_engine_msquic_config_v1_init(
			&providerConfig,
			C.size_t(C.sizeof_trevrpc_engine_msquic_config_v1),
		)),
	); err != nil {
		return nil, err
	}

	var engine *C.trevrpc_engine
	if err := statusError(
		"create Engine MsQuic runtime",
		int(C.trevrpc_engine_msquic_create_v1(
			&engineConfig,
			&providerConfig,
			&engine,
		)),
	); err != nil {
		return nil, err
	}
	if engine == nil {
		return nil, errors.New("create Engine MsQuic runtime returned a nil runtime")
	}
	return &Runtime{engine: engine}, nil
}

func (r *Runtime) WakeSources() ([]WakeSource, error) {
	wake, err := r.WakeSource()
	if err != nil {
		return nil, err
	}
	return []WakeSource{wake}, nil
}

func (r *Runtime) pollTimeoutMilliseconds() int {
	return -1
}

func (r *Runtime) transportABIVersion() uint32 {
	return EngineABIVersion
}

func (r *Runtime) WakeSource() (WakeSource, error) {
	if r == nil {
		return WakeSource{}, errors.New("get Engine wake source from a nil runtime")
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if r.engine == nil {
		return WakeSource{}, errors.New("get Engine wake source after runtime close")
	}

	var source C.trevrpc_engine_wake_source_v1
	if err := statusError(
		"initialize Engine wake source",
		int(C.trevrpc_engine_wake_source_v1_init(
			&source,
			C.size_t(C.sizeof_trevrpc_engine_wake_source_v1),
		)),
	); err != nil {
		return WakeSource{}, err
	}
	if err := statusError(
		"get Engine wake source",
		int(C.trevrpc_engine_get_wake_source_v1(r.engine, &source)),
	); err != nil {
		return WakeSource{}, err
	}
	if source.native_handle < 0 {
		return WakeSource{}, fmt.Errorf("get Engine wake source returned invalid native handle %d", source.native_handle)
	}
	return WakeSource{
		Kind:         uint32(source.kind),
		Flags:        uint32(source.flags),
		NativeHandle: uintptr(source.native_handle),
	}, nil
}

func (r *Runtime) Close() error {
	if r == nil {
		return nil
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if r.engine == nil {
		return nil
	}

	var closeErrors []error
	if err := statusError("close Engine", int(C.trevrpc_engine_close(r.engine))); err != nil {
		closeErrors = append(closeErrors, err)
	}
	if err := statusError("drain Engine", int(C.trevrpc_engine_drain(r.engine))); err != nil {
		closeErrors = append(closeErrors, err)
	}
	if err := drainEvents(r.engine); err != nil {
		closeErrors = append(closeErrors, err)
	}
	releaseStatus := int(C.trevrpc_engine_release(r.engine))
	// Engine release consumes the object after the release gate is admitted,
	// even when provider shutdown reports a nonzero status.
	r.engine = nil
	if err := statusError("release Engine", releaseStatus); err != nil {
		closeErrors = append(closeErrors, err)
	}
	return errors.Join(closeErrors...)
}

func drainEvents(engine *C.trevrpc_engine) error {
	for {
		var event *C.trevrpc_engine_event
		status := int(C.trevrpc_engine_next_event(engine, &event))
		if status == -int(C.EAGAIN) {
			return nil
		}
		if status != 0 {
			return statusError("drain Engine event queue", status)
		}
		if event == nil {
			return errors.New("drain Engine event queue returned a nil event")
		}
		C.trevrpc_engine_event_release(event)
	}
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

func handleFromC(handle C.trevrpc_engine_handle_v1) nativeHandle {
	return nativeHandle{
		owner:      uint64(handle.owner),
		slot:       uint32(handle.slot),
		generation: uint32(handle.generation),
	}
}

func (handle nativeHandle) c() C.trevrpc_engine_handle_v1 {
	return C.trevrpc_engine_handle_v1{
		owner:      C.uint64_t(handle.owner),
		slot:       C.uint32_t(handle.slot),
		generation: C.uint32_t(handle.generation),
	}
}

func pollNativeWake(wakeDescriptors []int, commandDescriptor, timeoutMilliseconds int) (uint32, error) {
	if len(wakeDescriptors) == 0 || len(wakeDescriptors) > 8 {
		return 0, fmt.Errorf("poll native wake sources received %d descriptors", len(wakeDescriptors))
	}
	var descriptors [8]C.intptr_t
	for index, descriptor := range wakeDescriptors {
		descriptors[index] = C.intptr_t(descriptor)
	}
	status := int(C.trevrpc_go_poll(
		&descriptors[0],
		C.size_t(len(wakeDescriptors)),
		C.int(commandDescriptor),
		C.int(timeoutMilliseconds),
	))
	if status < 0 {
		return 0, statusError("poll native wake sources", status)
	}
	return uint32(status), nil
}

func (r *Runtime) nextEvent() (nativeEvent, bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.engine == nil {
		return nativeEvent{}, false, errFrameStreamClosed
	}

	var event *C.trevrpc_engine_event
	status := int(C.trevrpc_engine_next_event(r.engine, &event))
	if status == -int(C.EAGAIN) {
		return nativeEvent{}, false, nil
	}
	if status != 0 {
		return nativeEvent{}, false, statusError("get next Engine event", status)
	}
	if event == nil {
		return nativeEvent{}, false, errors.New("get next Engine event returned a nil event")
	}
	defer C.trevrpc_engine_event_release(event)

	var info C.trevrpc_engine_event_info_v1
	if err := statusError(
		"initialize Engine event information",
		int(C.trevrpc_engine_event_info_v1_init(
			&info,
			C.size_t(C.sizeof_trevrpc_engine_event_info_v1),
		)),
	); err != nil {
		return nativeEvent{}, false, err
	}
	if err := statusError(
		"get Engine event information",
		int(C.trevrpc_engine_event_get_info_v1(event, &info)),
	); err != nil {
		return nativeEvent{}, false, err
	}

	result := nativeEvent{
		kind:                 uint32(info.kind),
		flags:                uint32(info.flags),
		status:               int(info.status),
		subjectKind:          uint32(info.subject_kind),
		sequence:             uint64(info.sequence),
		subject:              handleFromC(info.subject),
		parent:               handleFromC(info.parent),
		operationID:          uint64(info.operation_id),
		applicationErrorCode: uint64(info.application_error_code),
		providerErrorCode:    uint64(info.provider_error_code),
	}
	if info.data != nil && info.data_len != 0 {
		if uint64(C.int(info.data_len)) != uint64(info.data_len) {
			return nativeEvent{}, false, errors.New("Engine event payload exceeds Go copy limit")
		}
		result.data = C.GoBytes(unsafe.Pointer(info.data), C.int(info.data_len))
	}
	return result, true, nil
}

func (r *Runtime) listen(config EndpointConfig) (nativeHandle, error) {
	var handle nativeHandle
	err := withEndpointConfig(config, func(endpoint *C.trevrpc_engine_endpoint_config_v1) error {
		r.mu.Lock()
		defer r.mu.Unlock()
		if r.engine == nil {
			return errFrameStreamClosed
		}
		var result C.trevrpc_engine_handle_v1
		if err := nativeStatusError("listen with Engine", int(C.trevrpc_engine_listen_v1(r.engine, endpoint, &result))); err != nil {
			return err
		}
		handle = handleFromC(result)
		return nil
	})
	return handle, err
}

func (r *Runtime) listenerPort(handle nativeHandle) (uint16, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.engine == nil {
		return 0, errFrameStreamClosed
	}
	var port C.uint16_t
	if err := nativeStatusError(
		"get Engine listener port",
		int(C.trevrpc_engine_listener_get_port_v1(r.engine, handle.c(), &port)),
	); err != nil {
		return 0, err
	}
	return uint16(port), nil
}

func (r *Runtime) dial(config EndpointConfig, operationID uint64) (nativeHandle, error) {
	var handle nativeHandle
	err := withEndpointConfig(config, func(endpoint *C.trevrpc_engine_endpoint_config_v1) error {
		r.mu.Lock()
		defer r.mu.Unlock()
		if r.engine == nil {
			return errFrameStreamClosed
		}
		var result C.trevrpc_engine_handle_v1
		if err := nativeStatusError(
			"dial with Engine",
			int(C.trevrpc_engine_dial_v1(r.engine, endpoint, C.uint64_t(operationID), &result)),
		); err != nil {
			return err
		}
		handle = handleFromC(result)
		return nil
	})
	return handle, err
}

func (r *Runtime) cancelDial(handle nativeHandle) error {
	return r.handleCall("cancel Engine dial", func(engine *C.trevrpc_engine) C.int {
		return C.int(C.trevrpc_engine_dial_cancel(engine, handle.c()))
	})
}

func (r *Runtime) openStream(connection nativeHandle, operationID uint64) (nativeHandle, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.engine == nil {
		return nativeHandle{}, errFrameStreamClosed
	}
	var result C.trevrpc_engine_handle_v1
	if err := nativeStatusError(
		"open Engine bidirectional stream",
		int(C.trevrpc_engine_connection_open_bidi_stream_v1(
			r.engine,
			connection.c(),
			C.uint64_t(operationID),
			&result,
		)),
	); err != nil {
		return nativeHandle{}, err
	}
	return handleFromC(result), nil
}

func (r *Runtime) sendFrame(stream nativeHandle, operationID uint64, body []byte) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.engine == nil {
		return errFrameStreamClosed
	}
	var data *C.uint8_t
	if len(body) != 0 {
		data = (*C.uint8_t)(unsafe.Pointer(unsafe.SliceData(body)))
	}
	return nativeStatusError(
		"send Engine frame",
		int(C.trevrpc_engine_stream_send_frame_v1(
			r.engine,
			stream.c(),
			C.uint64_t(operationID),
			data,
			C.size_t(len(body)),
		)),
	)
}

func (r *Runtime) receiveFrame(stream nativeHandle) ([]byte, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.engine == nil {
		return nil, errFrameStreamClosed
	}

	var receive *C.trevrpc_engine_receive
	status := int(C.trevrpc_engine_stream_receive_frame(r.engine, stream.c(), &receive))
	if status == -int(C.EAGAIN) {
		return nil, errNativeWouldBlock
	}
	if status != 0 {
		return nil, statusError("receive Engine frame", status)
	}
	if receive == nil {
		return nil, errors.New("receive Engine frame returned a nil receive")
	}
	defer C.trevrpc_engine_receive_release(receive)

	var info C.trevrpc_engine_receive_info_v1
	if err := statusError(
		"initialize Engine receive information",
		int(C.trevrpc_engine_receive_info_v1_init(
			&info,
			C.size_t(C.sizeof_trevrpc_engine_receive_info_v1),
		)),
	); err != nil {
		return nil, err
	}
	if err := statusError(
		"get Engine receive information",
		int(C.trevrpc_engine_receive_get_info_v1(receive, &info)),
	); err != nil {
		return nil, err
	}
	if uint64(C.int(info.data_len)) != uint64(info.data_len) {
		return nil, errors.New("Engine receive exceeds Go copy limit")
	}
	return C.GoBytes(unsafe.Pointer(info.data), C.int(info.data_len)), nil
}

func (r *Runtime) finishStreamSend(stream nativeHandle) error {
	return r.handleCall("finish Engine stream send", func(engine *C.trevrpc_engine) C.int {
		return C.int(C.trevrpc_engine_stream_finish_send(engine, stream.c()))
	})
}

func (r *Runtime) abortStreamRead(stream nativeHandle, code uint64) error {
	return r.handleCall("abort Engine stream receive", func(engine *C.trevrpc_engine) C.int {
		return C.int(C.trevrpc_engine_stream_abort_receive(engine, stream.c(), C.uint64_t(code)))
	})
}

func (r *Runtime) abortStreamWrite(stream nativeHandle, code uint64) error {
	return r.handleCall("abort Engine stream send", func(engine *C.trevrpc_engine) C.int {
		return C.int(C.trevrpc_engine_stream_abort_send(engine, stream.c(), C.uint64_t(code)))
	})
}

func (r *Runtime) abortStream(stream nativeHandle, code uint64) error {
	return r.handleCall("abort Engine stream", func(engine *C.trevrpc_engine) C.int {
		return C.int(C.trevrpc_engine_stream_abort(engine, stream.c(), C.uint64_t(code)))
	})
}

func (*Runtime) releaseHandle(nativeHandle, uint32) error {
	return nil
}

func (r *Runtime) closeConnection(connection nativeHandle, code uint64) error {
	return r.handleCall("close Engine connection", func(engine *C.trevrpc_engine) C.int {
		return C.int(C.trevrpc_engine_connection_close(engine, connection.c(), C.uint64_t(code)))
	})
}

func (r *Runtime) closeListener(listener nativeHandle) error {
	return r.handleCall("close Engine listener", func(engine *C.trevrpc_engine) C.int {
		return C.int(C.trevrpc_engine_listener_close(engine, listener.c()))
	})
}

func (r *Runtime) handleCall(operation string, call func(*C.trevrpc_engine) C.int) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.engine == nil {
		return errFrameStreamClosed
	}
	return nativeStatusError(operation, int(call(r.engine)))
}

func (r *Runtime) beginClose() error {
	return r.handleCall("close Engine", func(engine *C.trevrpc_engine) C.int {
		return C.int(C.trevrpc_engine_close(engine))
	})
}

func (r *Runtime) waitDrained() error {
	return r.handleCall("drain Engine", func(engine *C.trevrpc_engine) C.int {
		return C.int(C.trevrpc_engine_drain(engine))
	})
}

func (r *Runtime) release() (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.engine == nil {
		return true, nil
	}
	releaseStatus := int(C.trevrpc_engine_release(r.engine))
	// trevrpc_engine_release destroys an admitted Engine even when its
	// provider reports a shutdown error.
	r.engine = nil
	return true, statusError("release Engine", releaseStatus)
}

func nativeStatusError(operation string, status int) error {
	if status == -int(C.EAGAIN) {
		return errNativeWouldBlock
	}
	return statusError(operation, status)
}

func withEndpointConfig(config EndpointConfig, call func(*C.trevrpc_engine_endpoint_config_v1) error) error {
	var endpoint C.trevrpc_engine_endpoint_config_v1
	if err := statusError(
		"initialize Engine endpoint configuration",
		int(C.trevrpc_engine_endpoint_config_v1_init(
			&endpoint,
			C.size_t(C.sizeof_trevrpc_engine_endpoint_config_v1),
		)),
	); err != nil {
		return err
	}

	var allocations []unsafe.Pointer
	defer func() {
		for _, allocation := range allocations {
			C.free(allocation)
		}
	}()
	copyString := func(value string) (*C.char, C.uint32_t, error) {
		if uint64(len(value)) > uint64(^uint32(0)) {
			return nil, 0, errors.New("Engine endpoint string exceeds uint32 length")
		}
		if value == "" {
			return nil, 0, nil
		}
		result := C.CString(value)
		allocations = append(allocations, unsafe.Pointer(result))
		return result, C.uint32_t(len(value)), nil
	}

	var err error
	if endpoint.host, endpoint.host_len, err = copyString(config.Host); err != nil {
		return err
	}
	if endpoint.cert_file, endpoint.cert_file_len, err = copyString(config.CertificateFile); err != nil {
		return err
	}
	if endpoint.key_file, endpoint.key_file_len, err = copyString(config.PrivateKeyFile); err != nil {
		return err
	}
	if endpoint.ca_cert_file, endpoint.ca_cert_file_len, err = copyString(config.CACertificateFile); err != nil {
		return err
	}
	if uint64(len(config.ALPN)) > uint64(^uint32(0)) {
		return errors.New("Engine endpoint ALPN exceeds uint32 length")
	}
	if len(config.ALPN) != 0 {
		alpn := C.CBytes(config.ALPN)
		allocations = append(allocations, alpn)
		endpoint.alpn = (*C.uint8_t)(alpn)
		endpoint.alpn_len = C.uint32_t(len(config.ALPN))
	}

	endpoint.port = C.uint16_t(config.Port)
	endpoint.peer_bidi_stream_count = C.uint16_t(config.PeerBidirectionalStreams)
	if config.SkipCertificateValidation {
		endpoint.flags |= C.TREVRPC_ENGINE_ENDPOINT_SKIP_CERTIFICATE_VALIDATION
	}
	if config.DisableSendBuffering {
		endpoint.flags |= C.TREVRPC_ENGINE_ENDPOINT_DISABLE_SEND_BUFFERING
	}
	endpoint.max_pending_send_count = C.uint32_t(config.MaxPendingSendCount)
	endpoint.max_pending_send_bytes = C.uint64_t(config.MaxPendingSendBytes)
	endpoint.max_frame_size = C.uint64_t(config.MaxFrameSize)
	endpoint.max_idle_timeout_ms = C.uint64_t(config.MaxIdleTimeoutMilliseconds)
	endpoint.keep_alive_ms = C.uint32_t(config.KeepAliveMilliseconds)
	endpoint.stream_recv_window = C.uint32_t(config.StreamReceiveWindow)
	endpoint.conn_flow_control_window = C.uint32_t(config.ConnectionFlowControlWindow)
	return call(&endpoint)
}
