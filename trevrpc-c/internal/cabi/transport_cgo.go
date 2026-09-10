//go:build cgo && (linux || darwin)

package cabi

/*
#cgo CFLAGS: -std=c11 -Wall -Wextra -Werror -I${SRCDIR}/../../include -I${SRCDIR}/../../src
#include <errno.h>
#include <stdlib.h>
#include "bridge.h"
*/
import "C"

import (
	"errors"
	"fmt"
	"sync"
	"unsafe"

	trevrpcc "trev.zip/llc/trevrpc/trevrpc-c"
	"trev.zip/llc/trevrpc/trevrpc-c/internal/providerabi"
)

const maxTransportWakeSources = 8

type transportRuntime struct {
	mu        sync.Mutex
	transport *C.trevrpc_transport
}

type transportAdmission struct {
	mu       sync.Mutex
	runtime  *transportRuntime
	event    *C.trevrpc_transport_event_v1
	listener trevrpcc.Handle
	protocol uint32
	request  trevrpcc.AdmissionRequest
}

var _ trevrpcc.TransportRuntime = (*transportRuntime)(nil)
var _ trevrpcc.Admission = (*transportAdmission)(nil)

func AdoptTransport(descriptor providerabi.TransportDescriptor) (trevrpcc.TransportRuntime, error) {
	if descriptor.StructSize < uint32(unsafe.Sizeof(providerabi.TransportDescriptor{})) {
		return nil, errors.New("adopt Transport provider descriptor: invalid structure size")
	}
	if descriptor.StructVersion != providerabi.StructVersion1 {
		return nil, errors.New("adopt Transport provider descriptor: unsupported structure version")
	}
	if descriptor.Operations == nil {
		return nil, errors.New("adopt Transport provider descriptor: nil operations")
	}
	for _, reserved := range descriptor.Reserved {
		if reserved != 0 {
			return nil, errors.New("adopt Transport provider descriptor: nonzero reserved field")
		}
	}
	var nativeDescriptor C.trevrpc_transport_provider_descriptor_v1
	if err := statusError("initialize Transport provider descriptor", int(C.trevrpc_transport_provider_descriptor_v1_init(
		&nativeDescriptor, C.size_t(C.sizeof_trevrpc_transport_provider_descriptor_v1)))); err != nil {
		return nil, err
	}
	nativeDescriptor.operations = (*C.trevrpc_transport_provider_ops_v1)(descriptor.Operations)
	nativeDescriptor.context = descriptor.Context
	var transport *C.trevrpc_transport
	if err := statusError("adopt Transport provider", int(C.trevrpc_transport_provider_adopt_v1(
		&nativeDescriptor, &transport))); err != nil {
		return nil, err
	}
	if transport == nil {
		return nil, errors.New("adopt Transport provider returned a nil runtime")
	}
	return &transportRuntime{transport: transport}, nil
}

func (*transportRuntime) ABIVersion() uint32 { return trevrpcc.TransportABIVersion }

func (r *transportRuntime) WakeSources() ([]trevrpcc.WakeSource, error) {
	if r == nil {
		return nil, errors.New("get Transport wake sources from a nil runtime")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.transport == nil {
		return nil, trevrpcc.ErrClosed
	}
	var sources [maxTransportWakeSources]C.trevrpc_transport_wake_source_v1
	for index := range sources {
		if err := statusError("initialize Transport wake source", int(C.trevrpc_transport_wake_source_v1_init(
			&sources[index], C.size_t(C.sizeof_trevrpc_transport_wake_source_v1)))); err != nil {
			return nil, err
		}
	}
	var count C.size_t
	if err := statusError("get Transport wake sources", int(C.trevrpc_transport_get_wake_sources_v1(
		r.transport, &sources[0], C.size_t(len(sources)), &count))); err != nil {
		return nil, err
	}
	if count == 0 || uint64(count) > uint64(len(sources)) {
		return nil, fmt.Errorf("get Transport wake sources returned invalid count %d", uint64(count))
	}
	result := make([]trevrpcc.WakeSource, int(count))
	for index := range result {
		if sources[index].native_handle < 0 {
			return nil, fmt.Errorf("get Transport wake source returned invalid native handle %d", sources[index].native_handle)
		}
		result[index] = trevrpcc.WakeSource{
			Kind: uint32(sources[index].kind), Flags: uint32(sources[index].flags),
			NativeHandle: uintptr(sources[index].native_handle),
		}
	}
	return result, nil
}

func (r *transportRuntime) PollTimeoutMilliseconds() int {
	if r == nil {
		return -1
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.transport == nil {
		return -1
	}
	return int(C.trevrpc_transport_poll_timeout_ms(r.transport))
}

func (r *transportRuntime) NextEvent() (trevrpcc.Event, bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.transport == nil {
		return trevrpcc.Event{}, false, trevrpcc.ErrClosed
	}
	var event *C.trevrpc_transport_event_v1
	status := int(C.trevrpc_transport_next_event(r.transport, &event))
	if status == -int(C.EAGAIN) || status == -int(C.EPIPE) {
		return trevrpcc.Event{}, false, nil
	}
	if err := statusError("get next Transport event", status); err != nil {
		return trevrpcc.Event{}, false, err
	}
	if event == nil {
		return trevrpcc.Event{}, false, errors.New("get next Transport event returned a nil event")
	}
	releaseEvent := true
	defer func() {
		if releaseEvent {
			C.trevrpc_transport_event_release(r.transport, event)
		}
	}()
	var info C.trevrpc_transport_event_info_v1
	if err := statusError("initialize Transport event information", int(C.trevrpc_transport_event_info_v1_init(
		&info, C.size_t(C.sizeof_trevrpc_transport_event_info_v1)))); err != nil {
		return trevrpcc.Event{}, false, err
	}
	if err := statusError("get Transport event information", int(C.trevrpc_transport_event_get_info_v1(
		r.transport, event, &info))); err != nil {
		return trevrpcc.Event{}, false, err
	}
	data, err := copyBytes("Transport event payload", info.data, uint64(info.data_len))
	if err != nil {
		return trevrpcc.Event{}, false, err
	}
	result := trevrpcc.Event{
		Kind: uint32(info.kind), Flags: uint32(info.flags), Status: int(info.status),
		SubjectKind: uint32(info.subject_kind), Sequence: uint64(info.sequence),
		Subject: transportHandleFromC(info.subject), Parent: transportHandleFromC(info.parent),
		OperationID: uint64(info.operation_id), ApplicationErrorCode: uint64(info.application_error_code),
		ProviderErrorCode: uint64(info.provider_error_code), Data: data,
	}
	var protocol C.trevrpc_transport_event_protocol_info_v1
	if err := statusError("initialize Transport event protocol information", int(C.trevrpc_transport_event_protocol_info_v1_init(
		&protocol, C.size_t(C.sizeof_trevrpc_transport_event_protocol_info_v1)))); err != nil {
		return trevrpcc.Event{}, false, err
	}
	protocolStatus := int(C.trevrpc_transport_event_get_protocol_info_v1(r.transport, event, &protocol))
	if protocolStatus == 0 {
		result.Protocol = uint32(protocol.protocol)
	} else if protocolStatus != -int(C.ENOTSUP) {
		return trevrpcc.Event{}, false, statusError("get Transport event protocol information", protocolStatus)
	}
	if result.Kind == trevrpcc.EventHTTP3Admission || result.Kind == trevrpcc.EventWebTransportAdmission {
		admission, err := r.copyAdmission(event, result.Kind)
		if err != nil {
			return trevrpcc.Event{}, false, err
		}
		result.Protocol = admission.protocol
		result.Parent = admission.listener
		result.Admission = admission
		releaseEvent = false
	}
	return result, true, nil
}

func (r *transportRuntime) copyAdmission(event *C.trevrpc_transport_event_v1, kind uint32) (*transportAdmission, error) {
	var info C.trevrpc_transport_admission_info_v1
	if err := statusError("initialize Transport admission information", int(C.trevrpc_transport_admission_info_v1_init(
		&info, C.size_t(C.sizeof_trevrpc_transport_admission_info_v1)))); err != nil {
		return nil, err
	}
	if err := statusError("get Transport admission information", int(C.trevrpc_transport_event_get_admission_info_v1(
		r.transport, event, &info))); err != nil {
		return nil, err
	}
	if uint64(info.header_count) > uint64(int(^uint(0)>>1)) {
		return nil, errors.New("Transport admission header count exceeds Go capacity")
	}
	headers := make([]trevrpcc.AdmissionHeader, int(info.header_count))
	if len(headers) != 0 {
		if info.headers == nil {
			return nil, errors.New("Transport admission returned nil headers")
		}
		for index, field := range unsafe.Slice(info.headers, len(headers)) {
			name, err := copyBytes("Transport admission header name", field.name, uint64(field.name_len))
			if err != nil {
				return nil, err
			}
			value, err := copyBytes("Transport admission header value", field.value, uint64(field.value_len))
			if err != nil {
				return nil, err
			}
			headers[index] = trevrpcc.AdmissionHeader{Name: string(name), Value: string(value)}
		}
	}
	method, err := copyBytes("Transport admission method", info.method, uint64(info.method_len))
	if err != nil {
		return nil, err
	}
	path, err := copyBytes("Transport admission path", info.path, uint64(info.path_len))
	if err != nil {
		return nil, err
	}
	authority, err := copyBytes("Transport admission authority", info.authority, uint64(info.authority_len))
	if err != nil {
		return nil, err
	}
	origin, err := copyBytes("Transport admission origin", info.origin, uint64(info.origin_len))
	if err != nil {
		return nil, err
	}
	return &transportAdmission{
		runtime:  r,
		event:    event,
		listener: transportHandleFromC(info.listener),
		protocol: uint32(info.protocol),
		request: trevrpcc.AdmissionRequest{
			Kind: kind, Method: string(method), Path: string(path), Authority: string(authority),
			Origin: string(origin), Secure: true, Headers: headers,
		},
	}, nil
}

func (a *transportAdmission) Request() trevrpcc.AdmissionRequest {
	if a == nil {
		return trevrpcc.AdmissionRequest{}
	}
	return a.request
}

func (a *transportAdmission) Respond(httpStatus uint16) error {
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
		return trevrpcc.ErrClosed
	}
	result := nativeStatusError("respond to Transport admission", int(C.trevrpc_transport_admission_respond_v1(
		a.runtime.transport, a.event, C.uint16_t(httpStatus))))
	C.trevrpc_transport_event_release(a.runtime.transport, a.event)
	a.event = nil
	return result
}

func (r *transportRuntime) Diagnostics() (trevrpcc.Diagnostics, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.transport == nil {
		return trevrpcc.Diagnostics{}, trevrpcc.ErrClosed
	}
	var value C.trevrpc_transport_diagnostics_v1
	if err := statusError("initialize Transport diagnostics", int(C.trevrpc_transport_diagnostics_v1_init(
		&value, C.size_t(C.sizeof_trevrpc_transport_diagnostics_v1)))); err != nil {
		return trevrpcc.Diagnostics{}, err
	}
	if err := statusError("get Transport diagnostics", int(C.trevrpc_transport_get_diagnostics_v1(r.transport, &value))); err != nil {
		return trevrpcc.Diagnostics{}, err
	}
	return transportDiagnosticsFromC(value), nil
}

func (r *transportRuntime) Listen(config trevrpcc.EndpointConfig) (trevrpcc.Handle, error) {
	var handle trevrpcc.Handle
	err := withTransportEndpoint(config, func(endpoint *C.trevrpc_transport_endpoint_config_v1) error {
		r.mu.Lock()
		defer r.mu.Unlock()
		if r.transport == nil {
			return trevrpcc.ErrClosed
		}
		var result C.trevrpc_transport_handle_v1
		if err := nativeStatusError("listen with Transport", int(C.trevrpc_transport_listen_v1(
			r.transport, endpoint, &result))); err != nil {
			return err
		}
		handle = transportHandleFromC(result)
		return nil
	})
	return handle, err
}

func (r *transportRuntime) ListenerPort(handle trevrpcc.Handle) (uint16, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.transport == nil {
		return 0, trevrpcc.ErrClosed
	}
	var port C.uint16_t
	if err := nativeStatusError("get Transport listener port", int(C.trevrpc_transport_listener_get_port_v1(
		r.transport, transportHandleToC(handle), &port))); err != nil {
		return 0, err
	}
	return uint16(port), nil
}

func (r *transportRuntime) Dial(config trevrpcc.EndpointConfig, operationID uint64) (trevrpcc.Handle, error) {
	var handle trevrpcc.Handle
	err := withTransportEndpoint(config, func(endpoint *C.trevrpc_transport_endpoint_config_v1) error {
		r.mu.Lock()
		defer r.mu.Unlock()
		if r.transport == nil {
			return trevrpcc.ErrClosed
		}
		var result C.trevrpc_transport_handle_v1
		if err := nativeStatusError("dial with Transport", int(C.trevrpc_transport_dial_v1(
			r.transport, endpoint, C.uint64_t(operationID), &result))); err != nil {
			return err
		}
		handle = transportHandleFromC(result)
		return nil
	})
	return handle, err
}

func (r *transportRuntime) CancelDial(handle trevrpcc.Handle) error {
	return r.call("cancel Transport dial", func(transport *C.trevrpc_transport) C.int {
		return C.int(C.trevrpc_transport_dial_cancel(transport, transportHandleToC(handle)))
	})
}

func (r *transportRuntime) OpenStream(connection trevrpcc.Handle, operationID uint64) (trevrpcc.Handle, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.transport == nil {
		return trevrpcc.Handle{}, trevrpcc.ErrClosed
	}
	var result C.trevrpc_transport_handle_v1
	if err := nativeStatusError("open Transport bidirectional stream", int(C.trevrpc_transport_connection_open_bidi_stream_v1(
		r.transport, transportHandleToC(connection), C.uint64_t(operationID), &result))); err != nil {
		return trevrpcc.Handle{}, err
	}
	return transportHandleFromC(result), nil
}

func (r *transportRuntime) SendFrame(stream trevrpcc.Handle, operationID uint64, body []byte) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.transport == nil {
		return trevrpcc.ErrClosed
	}
	var data *C.uint8_t
	if len(body) != 0 {
		data = (*C.uint8_t)(unsafe.Pointer(unsafe.SliceData(body)))
	}
	return nativeStatusError("send Transport frame", int(C.trevrpc_transport_stream_send_v1(
		r.transport, transportHandleToC(stream), C.uint64_t(operationID), data, C.size_t(len(body)))))
}

func (r *transportRuntime) ReceiveFrame(stream trevrpcc.Handle) ([]byte, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.transport == nil {
		return nil, trevrpcc.ErrClosed
	}
	var receive *C.trevrpc_transport_receive_v1
	status := int(C.trevrpc_transport_stream_receive(r.transport, transportHandleToC(stream), &receive))
	if status == -int(C.EAGAIN) {
		return nil, trevrpcc.ErrWouldBlock
	}
	if err := statusError("receive Transport frame", status); err != nil {
		return nil, err
	}
	if receive == nil {
		return nil, errors.New("receive Transport frame returned a nil receive")
	}
	defer C.trevrpc_transport_receive_release(r.transport, receive)
	var info C.trevrpc_transport_receive_info_v1
	if err := statusError("initialize Transport receive information", int(C.trevrpc_transport_receive_info_v1_init(
		&info, C.size_t(C.sizeof_trevrpc_transport_receive_info_v1)))); err != nil {
		return nil, err
	}
	if err := statusError("get Transport receive information", int(C.trevrpc_transport_receive_get_info_v1(
		r.transport, receive, &info))); err != nil {
		return nil, err
	}
	return copyBytes("Transport receive", info.data, uint64(info.data_len))
}

func (r *transportRuntime) FinishStreamSend(stream trevrpcc.Handle) error {
	return r.call("finish Transport stream send", func(transport *C.trevrpc_transport) C.int {
		return C.int(C.trevrpc_transport_stream_finish_send(transport, transportHandleToC(stream)))
	})
}
func (r *transportRuntime) AbortStreamRead(stream trevrpcc.Handle, code uint64) error {
	return r.call("abort Transport stream receive", func(transport *C.trevrpc_transport) C.int {
		return C.int(C.trevrpc_transport_stream_abort_receive(transport, transportHandleToC(stream), C.uint64_t(code)))
	})
}
func (r *transportRuntime) AbortStreamWrite(stream trevrpcc.Handle, code uint64) error {
	return r.call("abort Transport stream send", func(transport *C.trevrpc_transport) C.int {
		return C.int(C.trevrpc_transport_stream_abort_send(transport, transportHandleToC(stream), C.uint64_t(code)))
	})
}
func (r *transportRuntime) AbortStream(stream trevrpcc.Handle, code uint64) error {
	return r.call("abort Transport stream", func(transport *C.trevrpc_transport) C.int {
		return C.int(C.trevrpc_transport_stream_abort(transport, transportHandleToC(stream), C.uint64_t(code)))
	})
}
func (r *transportRuntime) CloseStream(stream trevrpcc.Handle) error {
	return r.call("close Transport stream", func(transport *C.trevrpc_transport) C.int {
		return C.int(C.trevrpc_transport_stream_close(transport, transportHandleToC(stream)))
	})
}
func (r *transportRuntime) CloseConnection(connection trevrpcc.Handle, code uint64) error {
	return r.call("close Transport connection", func(transport *C.trevrpc_transport) C.int {
		return C.int(C.trevrpc_transport_connection_close(transport, transportHandleToC(connection), C.uint64_t(code)))
	})
}
func (r *transportRuntime) CloseListener(listener trevrpcc.Handle) error {
	return r.call("close Transport listener", func(transport *C.trevrpc_transport) C.int {
		return C.int(C.trevrpc_transport_listener_close(transport, transportHandleToC(listener)))
	})
}
func (r *transportRuntime) ReleaseHandle(handle trevrpcc.Handle, kind uint32) error {
	return r.call("release Transport handle", func(transport *C.trevrpc_transport) C.int {
		return C.int(C.trevrpc_transport_release_handle(transport, transportHandleToC(handle), C.uint32_t(kind)))
	})
}
func (r *transportRuntime) BeginClose() error {
	return r.call("close Transport", func(transport *C.trevrpc_transport) C.int { return C.int(C.trevrpc_transport_close(transport)) })
}
func (r *transportRuntime) WaitDrained() error {
	return r.call("drain Transport", func(transport *C.trevrpc_transport) C.int { return C.int(C.trevrpc_transport_drain(transport)) })
}

func (r *transportRuntime) Release() (bool, error) {
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

func (r *transportRuntime) call(operation string, call func(*C.trevrpc_transport) C.int) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.transport == nil {
		return trevrpcc.ErrClosed
	}
	return nativeStatusError(operation, int(call(r.transport)))
}

func transportHandleFromC(handle C.trevrpc_transport_handle_v1) trevrpcc.Handle {
	return trevrpcc.Handle{Owner: uint64(handle.owner), Slot: uint32(handle.slot), Generation: uint32(handle.generation)}
}
func transportHandleToC(handle trevrpcc.Handle) C.trevrpc_transport_handle_v1 {
	return C.trevrpc_transport_handle_v1{
		owner: C.uint64_t(handle.Owner), slot: C.uint32_t(handle.Slot), generation: C.uint32_t(handle.Generation),
	}
}

func withTransportEndpoint(config trevrpcc.EndpointConfig, call func(*C.trevrpc_transport_endpoint_config_v1) error) error {
	var endpoint C.trevrpc_transport_endpoint_config_v1
	if err := statusError("initialize Transport endpoint configuration", int(C.trevrpc_transport_endpoint_config_v1_init(
		&endpoint, C.size_t(C.sizeof_trevrpc_transport_endpoint_config_v1)))); err != nil {
		return err
	}
	allocations := cAllocations{}
	defer allocations.free()
	var err error
	if endpoint.host, endpoint.host_len, err = allocations.string32(config.Host); err != nil {
		return err
	}
	if endpoint.server_name, endpoint.server_name_len, err = allocations.string32(config.ServerName); err != nil {
		return err
	}
	if endpoint.cert_file, endpoint.cert_file_len, err = allocations.string32(config.CertificateFile); err != nil {
		return err
	}
	if endpoint.key_file, endpoint.key_file_len, err = allocations.string32(config.PrivateKeyFile); err != nil {
		return err
	}
	if endpoint.ca_cert_file, endpoint.ca_cert_file_len, err = allocations.string32(config.CACertificateFile); err != nil {
		return err
	}
	if endpoint.path, endpoint.path_len, err = allocations.string32(config.Path); err != nil {
		return err
	}
	if endpoint.origin, endpoint.origin_len, err = allocations.string32(config.Origin); err != nil {
		return err
	}
	if endpoint.alpn, endpoint.alpn_len, err = allocations.bytes32(config.ALPN); err != nil {
		return err
	}
	endpoint.cert_data, endpoint.cert_data_len = allocations.bytes64(config.Certificate)
	endpoint.key_data, endpoint.key_data_len = allocations.bytes64(config.PrivateKey)
	endpoint.ca_cert_data, endpoint.ca_cert_data_len = allocations.bytes64(config.CACertificate)
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

func transportDiagnosticsFromC(value C.trevrpc_transport_diagnostics_v1) trevrpcc.Diagnostics {
	return trevrpcc.Diagnostics{
		ABIVersion: uint32(value.transport_abi_version), State: uint32(value.state), TerminalStatus: int(value.terminal_status),
		EventCapacity: uint32(value.event_capacity), QueueDepth: uint32(value.queue_depth),
		OrdinaryQueueDepth: uint32(value.ordinary_queue_depth), EventsEnqueued: uint64(value.events_enqueued),
		EventsDequeued: uint64(value.events_dequeued), EventsRejected: uint64(value.events_rejected),
		ReceiveOwnedCount: uint64(value.receive_owned_count), PeakReceiveOwnedCount: uint64(value.peak_receive_owned_count),
		ReceiveOwnedBytes: uint64(value.receive_owned_bytes), PeakReceiveOwnedBytes: uint64(value.peak_receive_owned_bytes),
		PendingSendBytes: uint64(value.pending_send_bytes), PendingSendCount: uint64(value.pending_send_count),
		LiveListeners: uint64(value.live_listeners), LiveConnections: uint64(value.live_connections), LiveStreams: uint64(value.live_streams),
		ActiveCallbacks: uint64(value.active_callbacks), ActiveAPICalls: uint64(value.active_api_calls),
		WakeSignals: uint64(value.wake_signals), WakeWriteWouldBlock: uint64(value.wake_write_eagain),
		WakeFailures: uint64(value.wake_failures), ProviderErrorCode: uint64(value.provider_error_code),
		MandatoryReservations: uint64(value.mandatory_reservations),
	}
}
