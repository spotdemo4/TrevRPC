//go:build trevrpc_native && cgo && (linux || darwin) && (amd64 || arm64)

package native

/*
#cgo pkg-config: --static trevrpc_transport_msquic

#include <errno.h>
#include <stdlib.h>
#include <trevrpc_transport.h>
#include <trevrpc_transport_msquic.h>
*/
import "C"

import (
	"errors"
	"fmt"
	"sync"
	"unsafe"
)

const maxTransportWakeSources = 8

const (
	transportObjectListener   uint32 = 1
	transportObjectConnection uint32 = 2
	transportObjectStream     uint32 = 3
)

type transportRuntime struct {
	mu        sync.Mutex
	transport *C.trevrpc_transport
}

type transportAdmission struct {
	mu       sync.Mutex
	runtime  *transportRuntime
	event    *C.trevrpc_transport_event_v1
	listener nativeHandle
	protocol uint32
	request  AdmissionRequest
}

func (a *transportAdmission) respond(httpStatus uint16) error {
	if a == nil {
		return nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.event == nil {
		return nil
	}
	a.runtime.mu.Lock()
	defer a.runtime.mu.Unlock()
	if a.runtime.transport == nil {
		return errFrameStreamClosed
	}
	result := nativeStatusError(
		"respond to Transport admission",
		int(C.trevrpc_transport_admission_respond_v1(
			a.runtime.transport,
			a.event,
			C.uint16_t(httpStatus),
		)),
	)
	C.trevrpc_transport_event_release(a.runtime.transport, a.event)
	a.event = nil
	return result
}

func CheckTransportABI() (TransportABIInfo, error) {
	C.trevrpc_transport_abi_1_anchor()
	C.trevrpc_transport_msquic_abi_1_anchor()

	info := TransportABIInfo{
		Transport:       uint32(C.trevrpc_transport_abi_version()),
		TransportMsQuic: uint32(C.trevrpc_transport_msquic_abi_version()),
	}
	if uint32(C.TREVRPC_TRANSPORT_ABI_VERSION) != TransportABIVersion || info.Transport != TransportABIVersion {
		return info, fmt.Errorf(
			"trevrpc Transport ABI mismatch: headers=%d runtime=%d expected=%d",
			uint32(C.TREVRPC_TRANSPORT_ABI_VERSION),
			info.Transport,
			TransportABIVersion,
		)
	}
	if uint32(C.TREVRPC_TRANSPORT_MSQUIC_ABI_VERSION) != TransportMsQuicABIVersion || info.TransportMsQuic != TransportMsQuicABIVersion {
		return info, fmt.Errorf(
			"trevrpc Transport MsQuic ABI mismatch: headers=%d runtime=%d expected=%d",
			uint32(C.TREVRPC_TRANSPORT_MSQUIC_ABI_VERSION),
			info.TransportMsQuic,
			TransportMsQuicABIVersion,
		)
	}
	return info, nil
}

func openTransportRuntime(config EngineConfig) (*transportRuntime, error) {
	if _, err := CheckTransportABI(); err != nil {
		return nil, err
	}

	var transportConfig C.trevrpc_transport_config_v1
	if err := statusError(
		"initialize Transport configuration",
		int(C.trevrpc_transport_config_v1_init(
			&transportConfig,
			C.size_t(C.sizeof_trevrpc_transport_config_v1),
		)),
	); err != nil {
		return nil, err
	}
	if config.EventCapacity != 0 {
		transportConfig.event_capacity = C.uint32_t(config.EventCapacity)
	}
	if config.ListenerCapacity != 0 {
		transportConfig.listener_capacity = C.uint32_t(config.ListenerCapacity)
	}
	if config.ConnectionCapacity != 0 {
		transportConfig.connection_capacity = C.uint32_t(config.ConnectionCapacity)
	}
	if config.StreamCapacity != 0 {
		transportConfig.stream_capacity = C.uint32_t(config.StreamCapacity)
	}
	if config.MaxReceiveOwnedCount != 0 {
		transportConfig.max_receive_owned_count = C.uint32_t(config.MaxReceiveOwnedCount)
	}
	if config.MaxReceiveOwnedBytes != 0 {
		transportConfig.max_receive_owned_bytes = C.uint64_t(config.MaxReceiveOwnedBytes)
	}

	var providerConfig C.trevrpc_transport_msquic_config_v1
	if err := statusError(
		"initialize Transport MsQuic configuration",
		int(C.trevrpc_transport_msquic_config_v1_init(
			&providerConfig,
			C.size_t(C.sizeof_trevrpc_transport_msquic_config_v1),
		)),
	); err != nil {
		return nil, err
	}

	var transport *C.trevrpc_transport
	if err := statusError(
		"create Transport MsQuic runtime",
		int(C.trevrpc_transport_msquic_create_v1(
			&transportConfig,
			&providerConfig,
			&transport,
		)),
	); err != nil {
		return nil, err
	}
	if transport == nil {
		return nil, errors.New("create Transport MsQuic runtime returned a nil runtime")
	}
	return &transportRuntime{transport: transport}, nil
}

func (r *transportRuntime) WakeSources() ([]WakeSource, error) {
	if r == nil {
		return nil, errors.New("get Transport wake sources from a nil runtime")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.transport == nil {
		return nil, errors.New("get Transport wake sources after runtime close")
	}

	var sources [maxTransportWakeSources]C.trevrpc_transport_wake_source_v1
	for index := range sources {
		if err := statusError(
			"initialize Transport wake source",
			int(C.trevrpc_transport_wake_source_v1_init(
				&sources[index],
				C.size_t(C.sizeof_trevrpc_transport_wake_source_v1),
			)),
		); err != nil {
			return nil, err
		}
	}
	var count C.size_t
	if err := statusError(
		"get Transport wake sources",
		int(C.trevrpc_transport_get_wake_sources_v1(
			r.transport,
			&sources[0],
			C.size_t(len(sources)),
			&count,
		)),
	); err != nil {
		return nil, err
	}
	if count == 0 || uint64(count) > uint64(len(sources)) {
		return nil, fmt.Errorf("get Transport wake sources returned invalid count %d", uint64(count))
	}
	result := make([]WakeSource, int(count))
	for index := range result {
		if sources[index].native_handle < 0 {
			return nil, fmt.Errorf(
				"get Transport wake source returned invalid native handle %d",
				sources[index].native_handle,
			)
		}
		result[index] = WakeSource{
			Kind:         uint32(sources[index].kind),
			Flags:        uint32(sources[index].flags),
			NativeHandle: uintptr(sources[index].native_handle),
		}
	}
	return result, nil
}

func (r *transportRuntime) pollTimeoutMilliseconds() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.transport == nil {
		return -1
	}
	return int(C.trevrpc_transport_poll_timeout_ms(r.transport))
}

func (*transportRuntime) transportABIVersion() uint32 {
	return TransportABIVersion
}

func (r *transportRuntime) nextEvent() (nativeEvent, bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.transport == nil {
		return nativeEvent{}, false, errFrameStreamClosed
	}

	var event *C.trevrpc_transport_event_v1
	status := int(C.trevrpc_transport_next_event(r.transport, &event))
	if status == -int(C.EAGAIN) || status == -int(C.EPIPE) {
		return nativeEvent{}, false, nil
	}
	if status != 0 {
		return nativeEvent{}, false, statusError("get next Transport event", status)
	}
	if event == nil {
		return nativeEvent{}, false, errors.New("get next Transport event returned a nil event")
	}
	releaseEvent := true
	defer func() {
		if releaseEvent {
			C.trevrpc_transport_event_release(r.transport, event)
		}
	}()

	var info C.trevrpc_transport_event_info_v1
	if err := statusError(
		"initialize Transport event information",
		int(C.trevrpc_transport_event_info_v1_init(
			&info,
			C.size_t(C.sizeof_trevrpc_transport_event_info_v1),
		)),
	); err != nil {
		return nativeEvent{}, false, err
	}
	if err := statusError(
		"get Transport event information",
		int(C.trevrpc_transport_event_get_info_v1(r.transport, event, &info)),
	); err != nil {
		return nativeEvent{}, false, err
	}

	result := nativeEvent{
		kind:                 uint32(info.kind),
		flags:                uint32(info.flags),
		status:               int(info.status),
		subjectKind:          uint32(info.subject_kind),
		sequence:             uint64(info.sequence),
		subject:              transportHandleFromC(info.subject),
		parent:               transportHandleFromC(info.parent),
		operationID:          uint64(info.operation_id),
		applicationErrorCode: uint64(info.application_error_code),
		providerErrorCode:    uint64(info.provider_error_code),
	}
	if info.data != nil && info.data_len != 0 {
		data, err := copyTransportBytes("event payload", info.data, info.data_len)
		if err != nil {
			return nativeEvent{}, false, err
		}
		result.data = data
	}

	var protocolInfo C.trevrpc_transport_event_protocol_info_v1
	if err := statusError(
		"initialize Transport event protocol information",
		int(C.trevrpc_transport_event_protocol_info_v1_init(
			&protocolInfo,
			C.size_t(C.sizeof_trevrpc_transport_event_protocol_info_v1),
		)),
	); err != nil {
		return nativeEvent{}, false, err
	}
	protocolStatus := int(C.trevrpc_transport_event_get_protocol_info_v1(
		r.transport,
		event,
		&protocolInfo,
	))
	if protocolStatus == 0 {
		result.protocol = uint32(protocolInfo.protocol)
	} else if protocolStatus != -int(C.ENOTSUP) {
		return nativeEvent{}, false, statusError("get Transport event protocol information", protocolStatus)
	}

	if result.kind == AdmissionHTTP3 || result.kind == AdmissionWebTransport {
		admission, err := r.copyAdmission(event, result.kind)
		if err != nil {
			return nativeEvent{}, false, err
		}
		result.protocol = admission.protocol
		result.parent = admission.listener
		result.admission = admission
		releaseEvent = false
	}
	return result, true, nil
}

func (r *transportRuntime) copyAdmission(
	event *C.trevrpc_transport_event_v1,
	kind uint32,
) (*transportAdmission, error) {
	var info C.trevrpc_transport_admission_info_v1
	if err := statusError(
		"initialize Transport admission information",
		int(C.trevrpc_transport_admission_info_v1_init(
			&info,
			C.size_t(C.sizeof_trevrpc_transport_admission_info_v1),
		)),
	); err != nil {
		return nil, err
	}
	if err := statusError(
		"get Transport admission information",
		int(C.trevrpc_transport_event_get_admission_info_v1(r.transport, event, &info)),
	); err != nil {
		return nil, err
	}
	if uint64(info.header_count) > uint64(int(^uint(0)>>1)) {
		return nil, errors.New("transport admission header count exceeds Go capacity")
	}
	headers := make([]AdmissionHeader, int(info.header_count))
	if len(headers) != 0 {
		if info.headers == nil {
			return nil, errors.New("transport admission returned nil headers")
		}
		fields := unsafe.Slice(info.headers, len(headers))
		for index, field := range fields {
			name, err := copyTransportBytes("admission header name", field.name, field.name_len)
			if err != nil {
				return nil, err
			}
			value, err := copyTransportBytes("admission header value", field.value, field.value_len)
			if err != nil {
				return nil, err
			}
			headers[index] = AdmissionHeader{Name: string(name), Value: string(value)}
		}
	}
	method, err := copyTransportBytes("admission method", info.method, info.method_len)
	if err != nil {
		return nil, err
	}
	path, err := copyTransportBytes("admission path", info.path, info.path_len)
	if err != nil {
		return nil, err
	}
	authority, err := copyTransportBytes("admission authority", info.authority, info.authority_len)
	if err != nil {
		return nil, err
	}
	origin, err := copyTransportBytes("admission origin", info.origin, info.origin_len)
	if err != nil {
		return nil, err
	}
	return &transportAdmission{
		runtime:  r,
		event:    event,
		listener: transportHandleFromC(info.listener),
		protocol: uint32(info.protocol),
		request: AdmissionRequest{
			Kind:      kind,
			Method:    string(method),
			Path:      string(path),
			Authority: string(authority),
			Origin:    string(origin),
			Secure:    true,
			Headers:   headers,
		},
	}, nil
}

func copyTransportBytes(name string, data *C.uint8_t, dataLength C.uint64_t) ([]byte, error) {
	if dataLength == 0 {
		return nil, nil
	}
	if data == nil {
		return nil, fmt.Errorf("transport %s returned a nil pointer", name)
	}
	if uint64(dataLength) > uint64(^uint32(0)>>1) {
		return nil, fmt.Errorf("transport %s exceeds Go copy limit", name)
	}
	return C.GoBytes(unsafe.Pointer(data), C.int(dataLength)), nil
}

func (r *transportRuntime) listen(config EndpointConfig) (nativeHandle, error) {
	var handle nativeHandle
	err := withTransportEndpointConfig(config, func(endpoint *C.trevrpc_transport_endpoint_config_v1) error {
		r.mu.Lock()
		defer r.mu.Unlock()
		if r.transport == nil {
			return errFrameStreamClosed
		}
		var result C.trevrpc_transport_handle_v1
		if err := nativeStatusError(
			"listen with Transport",
			int(C.trevrpc_transport_listen_v1(r.transport, endpoint, &result)),
		); err != nil {
			return err
		}
		handle = transportHandleFromC(result)
		return nil
	})
	return handle, err
}

func (r *transportRuntime) listenerPort(handle nativeHandle) (uint16, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.transport == nil {
		return 0, errFrameStreamClosed
	}
	var port C.uint16_t
	if err := nativeStatusError(
		"get Transport listener port",
		int(C.trevrpc_transport_listener_get_port_v1(r.transport, handle.transportC(), &port)),
	); err != nil {
		return 0, err
	}
	return uint16(port), nil
}

func (r *transportRuntime) dial(config EndpointConfig, operationID uint64) (nativeHandle, error) {
	var handle nativeHandle
	err := withTransportEndpointConfig(config, func(endpoint *C.trevrpc_transport_endpoint_config_v1) error {
		r.mu.Lock()
		defer r.mu.Unlock()
		if r.transport == nil {
			return errFrameStreamClosed
		}
		var result C.trevrpc_transport_handle_v1
		if err := nativeStatusError(
			"dial with Transport",
			int(C.trevrpc_transport_dial_v1(
				r.transport,
				endpoint,
				C.uint64_t(operationID),
				&result,
			)),
		); err != nil {
			return err
		}
		handle = transportHandleFromC(result)
		return nil
	})
	return handle, err
}

func (r *transportRuntime) cancelDial(handle nativeHandle) error {
	return r.handleCall("cancel Transport dial", func(transport *C.trevrpc_transport) C.int {
		return C.int(C.trevrpc_transport_dial_cancel(transport, handle.transportC()))
	})
}

func (r *transportRuntime) openStream(connection nativeHandle, operationID uint64) (nativeHandle, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.transport == nil {
		return nativeHandle{}, errFrameStreamClosed
	}
	var result C.trevrpc_transport_handle_v1
	if err := nativeStatusError(
		"open Transport bidirectional stream",
		int(C.trevrpc_transport_connection_open_bidi_stream_v1(
			r.transport,
			connection.transportC(),
			C.uint64_t(operationID),
			&result,
		)),
	); err != nil {
		return nativeHandle{}, err
	}
	return transportHandleFromC(result), nil
}

func (r *transportRuntime) sendFrame(stream nativeHandle, operationID uint64, body []byte) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.transport == nil {
		return errFrameStreamClosed
	}
	var data *C.uint8_t
	if len(body) != 0 {
		data = (*C.uint8_t)(unsafe.Pointer(unsafe.SliceData(body)))
	}
	return nativeStatusError(
		"send Transport frame",
		int(C.trevrpc_transport_stream_send_v1(
			r.transport,
			stream.transportC(),
			C.uint64_t(operationID),
			data,
			C.size_t(len(body)),
		)),
	)
}

func (r *transportRuntime) receiveFrame(stream nativeHandle) ([]byte, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.transport == nil {
		return nil, errFrameStreamClosed
	}

	var receive *C.trevrpc_transport_receive_v1
	status := int(C.trevrpc_transport_stream_receive(r.transport, stream.transportC(), &receive))
	if status == -int(C.EAGAIN) {
		return nil, errNativeWouldBlock
	}
	if status != 0 {
		return nil, statusError("receive Transport frame", status)
	}
	if receive == nil {
		return nil, errors.New("receive Transport frame returned a nil receive")
	}
	defer C.trevrpc_transport_receive_release(r.transport, receive)

	var info C.trevrpc_transport_receive_info_v1
	if err := statusError(
		"initialize Transport receive information",
		int(C.trevrpc_transport_receive_info_v1_init(
			&info,
			C.size_t(C.sizeof_trevrpc_transport_receive_info_v1),
		)),
	); err != nil {
		return nil, err
	}
	if err := statusError(
		"get Transport receive information",
		int(C.trevrpc_transport_receive_get_info_v1(r.transport, receive, &info)),
	); err != nil {
		return nil, err
	}
	if uint64(C.int(info.data_len)) != uint64(info.data_len) {
		return nil, errors.New("transport receive exceeds Go copy limit")
	}
	return C.GoBytes(unsafe.Pointer(info.data), C.int(info.data_len)), nil
}

func (r *transportRuntime) finishStreamSend(stream nativeHandle) error {
	return r.handleCall("finish Transport stream send", func(transport *C.trevrpc_transport) C.int {
		return C.int(C.trevrpc_transport_stream_finish_send(transport, stream.transportC()))
	})
}

func (r *transportRuntime) abortStreamRead(stream nativeHandle, code uint64) error {
	return r.handleCall("abort Transport stream receive", func(transport *C.trevrpc_transport) C.int {
		return C.int(C.trevrpc_transport_stream_abort_receive(transport, stream.transportC(), C.uint64_t(code)))
	})
}

func (r *transportRuntime) abortStreamWrite(stream nativeHandle, code uint64) error {
	return r.handleCall("abort Transport stream send", func(transport *C.trevrpc_transport) C.int {
		return C.int(C.trevrpc_transport_stream_abort_send(transport, stream.transportC(), C.uint64_t(code)))
	})
}

func (r *transportRuntime) abortStream(stream nativeHandle, code uint64) error {
	return r.handleCall("abort Transport stream", func(transport *C.trevrpc_transport) C.int {
		return C.int(C.trevrpc_transport_stream_abort(transport, stream.transportC(), C.uint64_t(code)))
	})
}

func (r *transportRuntime) closeConnection(connection nativeHandle, code uint64) error {
	return r.handleCall("close Transport connection", func(transport *C.trevrpc_transport) C.int {
		return C.int(C.trevrpc_transport_connection_close(transport, connection.transportC(), C.uint64_t(code)))
	})
}

func (r *transportRuntime) closeListener(listener nativeHandle) error {
	return r.handleCall("close Transport listener", func(transport *C.trevrpc_transport) C.int {
		return C.int(C.trevrpc_transport_listener_close(transport, listener.transportC()))
	})
}

func (r *transportRuntime) releaseHandle(handle nativeHandle, kind uint32) error {
	return r.handleCall("release Transport handle", func(transport *C.trevrpc_transport) C.int {
		return C.int(C.trevrpc_transport_release_handle(transport, handle.transportC(), C.uint32_t(kind)))
	})
}

func (r *transportRuntime) handleCall(
	operation string,
	call func(*C.trevrpc_transport) C.int,
) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.transport == nil {
		return errFrameStreamClosed
	}
	return nativeStatusError(operation, int(call(r.transport)))
}

func (r *transportRuntime) beginClose() error {
	return r.handleCall("close Transport", func(transport *C.trevrpc_transport) C.int {
		return C.int(C.trevrpc_transport_close(transport))
	})
}

func (r *transportRuntime) waitDrained() error {
	return r.handleCall("drain Transport", func(transport *C.trevrpc_transport) C.int {
		return C.int(C.trevrpc_transport_drain(transport))
	})
}

func (r *transportRuntime) release() (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.transport == nil {
		return true, nil
	}
	if err := statusError("release Transport", int(C.trevrpc_transport_release(r.transport))); err != nil {
		return false, err
	}
	r.transport = nil
	return true, nil
}

func transportHandleFromC(handle C.trevrpc_transport_handle_v1) nativeHandle {
	return nativeHandle{
		owner:      uint64(handle.owner),
		slot:       uint32(handle.slot),
		generation: uint32(handle.generation),
	}
}

func (handle nativeHandle) transportC() C.trevrpc_transport_handle_v1 {
	return C.trevrpc_transport_handle_v1{
		owner:      C.uint64_t(handle.owner),
		slot:       C.uint32_t(handle.slot),
		generation: C.uint32_t(handle.generation),
	}
}

func withTransportEndpointConfig(
	config EndpointConfig,
	call func(*C.trevrpc_transport_endpoint_config_v1) error,
) error {
	var endpoint C.trevrpc_transport_endpoint_config_v1
	if err := statusError(
		"initialize Transport endpoint configuration",
		int(C.trevrpc_transport_endpoint_config_v1_init(
			&endpoint,
			C.size_t(C.sizeof_trevrpc_transport_endpoint_config_v1),
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
			return nil, 0, errors.New("transport endpoint string exceeds uint32 length")
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
	if endpoint.server_name, endpoint.server_name_len, err = copyString(config.ServerName); err != nil {
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
	if endpoint.path, endpoint.path_len, err = copyString(config.Path); err != nil {
		return err
	}
	if endpoint.origin, endpoint.origin_len, err = copyString(config.Origin); err != nil {
		return err
	}
	if uint64(len(config.ALPN)) > uint64(^uint32(0)) {
		return errors.New("transport endpoint ALPN exceeds uint32 length")
	}
	if len(config.ALPN) != 0 {
		alpn := C.CBytes(config.ALPN)
		allocations = append(allocations, alpn)
		endpoint.alpn = (*C.uint8_t)(alpn)
		endpoint.alpn_len = C.uint32_t(len(config.ALPN))
	}

	endpoint.protocol = C.uint32_t(config.Protocol)
	endpoint.port = C.uint16_t(config.Port)
	endpoint.peer_bidi_stream_count = C.uint16_t(config.PeerBidirectionalStreams)
	if config.SkipCertificateValidation {
		endpoint.flags |= C.TREVRPC_TRANSPORT_ENDPOINT_SKIP_CERTIFICATE_VALIDATION
	}
	if config.DeferAdmission {
		endpoint.flags |= C.TREVRPC_TRANSPORT_ENDPOINT_DEFER_ADMISSION
	}
	endpoint.webtransport_profiles = C.uint32_t(config.WebTransportProfiles)
	endpoint.max_sessions = C.uint32_t(config.MaxSessions)
	endpoint.max_pending_send_count = C.uint32_t(config.MaxPendingSendCount)
	endpoint.max_pending_receive_count = C.uint32_t(config.MaxPendingReceiveCount)
	endpoint.max_pending_send_bytes = C.uint64_t(config.MaxPendingSendBytes)
	endpoint.max_pending_receive_bytes = C.uint64_t(config.MaxPendingReceiveBytes)
	endpoint.max_frame_size = C.uint64_t(config.MaxFrameSize)
	endpoint.max_field_section_size = C.uint64_t(config.MaxFieldSectionSize)
	endpoint.max_idle_timeout_ms = C.uint64_t(config.MaxIdleTimeoutMilliseconds)
	endpoint.keep_alive_ms = C.uint32_t(config.KeepAliveMilliseconds)
	endpoint.stream_recv_window = C.uint32_t(config.StreamReceiveWindow)
	endpoint.conn_flow_control_window = C.uint32_t(config.ConnectionFlowControlWindow)
	endpoint.unresolved_stream_count = C.uint32_t(config.UnresolvedStreamCount)
	endpoint.unresolved_stream_bytes = C.uint64_t(config.UnresolvedStreamBytes)
	endpoint.unresolved_stream_timeout_ms = C.uint64_t(config.UnresolvedStreamTimeoutMS)
	return call(&endpoint)
}
