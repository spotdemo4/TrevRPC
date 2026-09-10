//go:build cgo && (linux || darwin)

package cabi

/*
#cgo CFLAGS: -std=c11 -Wall -Wextra -Werror -I${SRCDIR}/../../include -I${SRCDIR}/../../src
#cgo linux CFLAGS: -DTREVRPC_ENGINE_HAVE_PIPE2

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

type engineRuntime struct {
	mu     sync.Mutex
	engine *C.trevrpc_engine
}

var _ trevrpcc.EngineRuntime = (*engineRuntime)(nil)

func EngineHost() providerabi.EngineHost {
	host := C.trevrpc_engine_provider_host_v1_get()
	return providerabi.EngineHost{
		StructSize:    uint32(host.struct_size),
		StructVersion: uint32(host.struct_version),
		Operations:    unsafe.Pointer(host),
	}
}

func AdoptEngine(config trevrpcc.EngineConfig, descriptor providerabi.EngineDescriptor) (trevrpcc.EngineRuntime, error) {
	if descriptor.StructSize < uint32(unsafe.Sizeof(providerabi.EngineDescriptor{})) {
		return nil, errors.New("adopt Engine provider descriptor: invalid structure size")
	}
	if descriptor.StructVersion != providerabi.StructVersion1 {
		return nil, errors.New("adopt Engine provider descriptor: unsupported structure version")
	}
	if descriptor.Operations == nil || descriptor.OwnerCookie == 0 {
		return nil, errors.New("adopt Engine provider descriptor: invalid provider fields")
	}
	for _, reserved := range descriptor.Reserved {
		if reserved != 0 {
			return nil, errors.New("adopt Engine provider descriptor: nonzero reserved field")
		}
	}
	var nativeConfig C.trevrpc_engine_config_v1
	if err := statusError("initialize Engine configuration", int(C.trevrpc_engine_config_v1_init(
		&nativeConfig, C.size_t(C.sizeof_trevrpc_engine_config_v1)))); err != nil {
		return nil, err
	}
	applyEngineConfig(&nativeConfig, config)
	var nativeDescriptor C.trevrpc_engine_provider_descriptor_v1
	if err := statusError("initialize Engine provider descriptor", int(C.trevrpc_engine_provider_descriptor_v1_init(
		&nativeDescriptor, C.size_t(C.sizeof_trevrpc_engine_provider_descriptor_v1)))); err != nil {
		return nil, err
	}
	nativeDescriptor.operations = (*C.trevrpc_engine_provider_ops_v1)(descriptor.Operations)
	nativeDescriptor.context = descriptor.Context
	nativeDescriptor.owner_cookie = C.uint64_t(descriptor.OwnerCookie)
	var engine *C.trevrpc_engine
	if err := statusError("adopt Engine provider", int(C.trevrpc_engine_provider_adopt_v1(
		&nativeConfig,
		&nativeDescriptor,
		&engine,
	))); err != nil {
		return nil, err
	}
	if engine == nil {
		return nil, errors.New("adopt Engine provider returned a nil runtime")
	}
	return &engineRuntime{engine: engine}, nil
}

func applyEngineConfig(destination *C.trevrpc_engine_config_v1, config trevrpcc.EngineConfig) {
	if config.EventCapacity != 0 {
		destination.event_capacity = C.uint32_t(config.EventCapacity)
	}
	if config.ListenerCapacity != 0 {
		destination.listener_capacity = C.uint32_t(config.ListenerCapacity)
	}
	if config.ConnectionCapacity != 0 {
		destination.connection_capacity = C.uint32_t(config.ConnectionCapacity)
	}
	if config.StreamCapacity != 0 {
		destination.stream_capacity = C.uint32_t(config.StreamCapacity)
	}
	if config.MaxReceiveOwnedCount != 0 {
		destination.max_receive_owned_count = C.uint32_t(config.MaxReceiveOwnedCount)
	}
	if config.MaxReceiveOwnedBytes != 0 {
		destination.max_receive_owned_bytes = C.uint64_t(config.MaxReceiveOwnedBytes)
	}
}

func (*engineRuntime) PollTimeoutMilliseconds() int { return -1 }
func (*engineRuntime) ABIVersion() uint32           { return trevrpcc.EngineABIVersion }

func (r *engineRuntime) WakeSources() ([]trevrpcc.WakeSource, error) {
	if r == nil {
		return nil, errors.New("get Engine wake source from a nil runtime")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.engine == nil {
		return nil, trevrpcc.ErrClosed
	}
	var source C.trevrpc_engine_wake_source_v1
	if err := statusError("initialize Engine wake source", int(C.trevrpc_engine_wake_source_v1_init(
		&source, C.size_t(C.sizeof_trevrpc_engine_wake_source_v1)))); err != nil {
		return nil, err
	}
	if err := statusError("get Engine wake source", int(C.trevrpc_engine_get_wake_source_v1(r.engine, &source))); err != nil {
		return nil, err
	}
	if source.native_handle < 0 {
		return nil, fmt.Errorf("get Engine wake source returned invalid native handle %d", source.native_handle)
	}
	return []trevrpcc.WakeSource{{
		Kind: uint32(source.kind), Flags: uint32(source.flags), NativeHandle: uintptr(source.native_handle),
	}}, nil
}

func (r *engineRuntime) NextEvent() (trevrpcc.Event, bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.engine == nil {
		return trevrpcc.Event{}, false, trevrpcc.ErrClosed
	}
	var event *C.trevrpc_engine_event
	status := int(C.trevrpc_engine_next_event(r.engine, &event))
	if status == -int(C.EAGAIN) || status == -int(C.EPIPE) {
		return trevrpcc.Event{}, false, nil
	}
	if err := statusError("get next Engine event", status); err != nil {
		return trevrpcc.Event{}, false, err
	}
	if event == nil {
		return trevrpcc.Event{}, false, errors.New("get next Engine event returned a nil event")
	}
	defer C.trevrpc_engine_event_release(event)
	var info C.trevrpc_engine_event_info_v1
	if err := statusError("initialize Engine event information", int(C.trevrpc_engine_event_info_v1_init(
		&info, C.size_t(C.sizeof_trevrpc_engine_event_info_v1)))); err != nil {
		return trevrpcc.Event{}, false, err
	}
	if err := statusError("get Engine event information", int(C.trevrpc_engine_event_get_info_v1(event, &info))); err != nil {
		return trevrpcc.Event{}, false, err
	}
	data, err := copyBytes("Engine event payload", info.data, uint64(info.data_len))
	if err != nil {
		return trevrpcc.Event{}, false, err
	}
	return trevrpcc.Event{
		Kind: uint32(info.kind), Flags: uint32(info.flags), Status: int(info.status),
		SubjectKind: uint32(info.subject_kind), Sequence: uint64(info.sequence),
		Subject: engineHandleFromC(info.subject), Parent: engineHandleFromC(info.parent),
		OperationID: uint64(info.operation_id), ApplicationErrorCode: uint64(info.application_error_code),
		ProviderErrorCode: uint64(info.provider_error_code), Data: data,
	}, true, nil
}

func (r *engineRuntime) Diagnostics() (trevrpcc.Diagnostics, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.engine == nil {
		return trevrpcc.Diagnostics{}, trevrpcc.ErrClosed
	}
	var value C.trevrpc_engine_diagnostics_v1
	if err := statusError("initialize Engine diagnostics", int(C.trevrpc_engine_diagnostics_v1_init(
		&value, C.size_t(C.sizeof_trevrpc_engine_diagnostics_v1)))); err != nil {
		return trevrpcc.Diagnostics{}, err
	}
	if err := statusError("get Engine diagnostics", int(C.trevrpc_engine_get_diagnostics_v1(r.engine, &value))); err != nil {
		return trevrpcc.Diagnostics{}, err
	}
	return engineDiagnosticsFromC(value), nil
}

func (r *engineRuntime) Listen(config trevrpcc.EndpointConfig) (trevrpcc.Handle, error) {
	var handle trevrpcc.Handle
	err := withEngineEndpoint(config, func(endpoint *C.trevrpc_engine_endpoint_config_v1) error {
		r.mu.Lock()
		defer r.mu.Unlock()
		if r.engine == nil {
			return trevrpcc.ErrClosed
		}
		var result C.trevrpc_engine_handle_v1
		if err := nativeStatusError("listen with Engine", int(C.trevrpc_engine_listen_v1(r.engine, endpoint, &result))); err != nil {
			return err
		}
		handle = engineHandleFromC(result)
		return nil
	})
	return handle, err
}

func (r *engineRuntime) ListenerPort(handle trevrpcc.Handle) (uint16, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.engine == nil {
		return 0, trevrpcc.ErrClosed
	}
	var port C.uint16_t
	if err := nativeStatusError("get Engine listener port", int(C.trevrpc_engine_listener_get_port_v1(
		r.engine, engineHandleToC(handle), &port))); err != nil {
		return 0, err
	}
	return uint16(port), nil
}

func (r *engineRuntime) Dial(config trevrpcc.EndpointConfig, operationID uint64) (trevrpcc.Handle, error) {
	var handle trevrpcc.Handle
	err := withEngineEndpoint(config, func(endpoint *C.trevrpc_engine_endpoint_config_v1) error {
		r.mu.Lock()
		defer r.mu.Unlock()
		if r.engine == nil {
			return trevrpcc.ErrClosed
		}
		var result C.trevrpc_engine_handle_v1
		if err := nativeStatusError("dial with Engine", int(C.trevrpc_engine_dial_v1(
			r.engine, endpoint, C.uint64_t(operationID), &result))); err != nil {
			return err
		}
		handle = engineHandleFromC(result)
		return nil
	})
	return handle, err
}

func (r *engineRuntime) CancelDial(handle trevrpcc.Handle) error {
	return r.call("cancel Engine dial", func(engine *C.trevrpc_engine) C.int {
		return C.int(C.trevrpc_engine_dial_cancel(engine, engineHandleToC(handle)))
	})
}

func (r *engineRuntime) OpenStream(connection trevrpcc.Handle, operationID uint64) (trevrpcc.Handle, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.engine == nil {
		return trevrpcc.Handle{}, trevrpcc.ErrClosed
	}
	var result C.trevrpc_engine_handle_v1
	if err := nativeStatusError("open Engine bidirectional stream", int(C.trevrpc_engine_connection_open_bidi_stream_v1(
		r.engine, engineHandleToC(connection), C.uint64_t(operationID), &result))); err != nil {
		return trevrpcc.Handle{}, err
	}
	return engineHandleFromC(result), nil
}

func (r *engineRuntime) SendFrame(stream trevrpcc.Handle, operationID uint64, body []byte) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.engine == nil {
		return trevrpcc.ErrClosed
	}
	var data *C.uint8_t
	if len(body) != 0 {
		data = (*C.uint8_t)(unsafe.Pointer(unsafe.SliceData(body)))
	}
	return nativeStatusError("send Engine frame", int(C.trevrpc_engine_stream_send_frame_v1(
		r.engine, engineHandleToC(stream), C.uint64_t(operationID), data, C.size_t(len(body)))))
}

func (r *engineRuntime) ReceiveFrame(stream trevrpcc.Handle) ([]byte, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.engine == nil {
		return nil, trevrpcc.ErrClosed
	}
	var receive *C.trevrpc_engine_receive
	status := int(C.trevrpc_engine_stream_receive_frame(r.engine, engineHandleToC(stream), &receive))
	if status == -int(C.EAGAIN) {
		return nil, trevrpcc.ErrWouldBlock
	}
	if err := statusError("receive Engine frame", status); err != nil {
		return nil, err
	}
	if receive == nil {
		return nil, errors.New("receive Engine frame returned a nil receive")
	}
	defer C.trevrpc_engine_receive_release(receive)
	var info C.trevrpc_engine_receive_info_v1
	if err := statusError("initialize Engine receive information", int(C.trevrpc_engine_receive_info_v1_init(
		&info, C.size_t(C.sizeof_trevrpc_engine_receive_info_v1)))); err != nil {
		return nil, err
	}
	if err := statusError("get Engine receive information", int(C.trevrpc_engine_receive_get_info_v1(receive, &info))); err != nil {
		return nil, err
	}
	return copyBytes("Engine receive", info.data, uint64(info.data_len))
}

func (r *engineRuntime) FinishStreamSend(stream trevrpcc.Handle) error {
	return r.call("finish Engine stream send", func(engine *C.trevrpc_engine) C.int {
		return C.int(C.trevrpc_engine_stream_finish_send(engine, engineHandleToC(stream)))
	})
}
func (r *engineRuntime) AbortStreamRead(stream trevrpcc.Handle, code uint64) error {
	return r.call("abort Engine stream receive", func(engine *C.trevrpc_engine) C.int {
		return C.int(C.trevrpc_engine_stream_abort_receive(engine, engineHandleToC(stream), C.uint64_t(code)))
	})
}
func (r *engineRuntime) AbortStreamWrite(stream trevrpcc.Handle, code uint64) error {
	return r.call("abort Engine stream send", func(engine *C.trevrpc_engine) C.int {
		return C.int(C.trevrpc_engine_stream_abort_send(engine, engineHandleToC(stream), C.uint64_t(code)))
	})
}
func (r *engineRuntime) AbortStream(stream trevrpcc.Handle, code uint64) error {
	return r.call("abort Engine stream", func(engine *C.trevrpc_engine) C.int {
		return C.int(C.trevrpc_engine_stream_abort(engine, engineHandleToC(stream), C.uint64_t(code)))
	})
}
func (r *engineRuntime) CloseStream(stream trevrpcc.Handle) error {
	return r.call("close Engine stream", func(engine *C.trevrpc_engine) C.int {
		return C.int(C.trevrpc_engine_stream_close(engine, engineHandleToC(stream)))
	})
}
func (r *engineRuntime) CloseConnection(connection trevrpcc.Handle, code uint64) error {
	return r.call("close Engine connection", func(engine *C.trevrpc_engine) C.int {
		return C.int(C.trevrpc_engine_connection_close(engine, engineHandleToC(connection), C.uint64_t(code)))
	})
}
func (r *engineRuntime) CloseListener(listener trevrpcc.Handle) error {
	return r.call("close Engine listener", func(engine *C.trevrpc_engine) C.int {
		return C.int(C.trevrpc_engine_listener_close(engine, engineHandleToC(listener)))
	})
}
func (*engineRuntime) ReleaseHandle(trevrpcc.Handle, uint32) error { return nil }
func (r *engineRuntime) BeginClose() error {
	return r.call("close Engine", func(engine *C.trevrpc_engine) C.int { return C.int(C.trevrpc_engine_close(engine)) })
}
func (r *engineRuntime) WaitDrained() error {
	return r.call("drain Engine", func(engine *C.trevrpc_engine) C.int { return C.int(C.trevrpc_engine_drain(engine)) })
}

func (r *engineRuntime) Release() (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.engine == nil {
		return true, nil
	}
	status := int(C.trevrpc_engine_release(r.engine))
	r.engine = nil
	return true, statusError("release Engine", status)
}

func (r *engineRuntime) call(operation string, call func(*C.trevrpc_engine) C.int) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.engine == nil {
		return trevrpcc.ErrClosed
	}
	return nativeStatusError(operation, int(call(r.engine)))
}

func engineHandleFromC(handle C.trevrpc_engine_handle_v1) trevrpcc.Handle {
	return trevrpcc.Handle{Owner: uint64(handle.owner), Slot: uint32(handle.slot), Generation: uint32(handle.generation)}
}
func engineHandleToC(handle trevrpcc.Handle) C.trevrpc_engine_handle_v1 {
	return C.trevrpc_engine_handle_v1{
		owner: C.uint64_t(handle.Owner), slot: C.uint32_t(handle.Slot), generation: C.uint32_t(handle.Generation),
	}
}

func withEngineEndpoint(config trevrpcc.EndpointConfig, call func(*C.trevrpc_engine_endpoint_config_v1) error) error {
	var endpoint C.trevrpc_engine_endpoint_config_v1
	if err := statusError("initialize Engine endpoint configuration", int(C.trevrpc_engine_endpoint_config_v1_init(
		&endpoint, C.size_t(C.sizeof_trevrpc_engine_endpoint_config_v1)))); err != nil {
		return err
	}
	allocations := cAllocations{}
	defer allocations.free()
	var err error
	if endpoint.host, endpoint.host_len, err = allocations.string32(config.Host); err != nil {
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
	if endpoint.alpn, endpoint.alpn_len, err = allocations.bytes32(config.ALPN); err != nil {
		return err
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

func engineDiagnosticsFromC(value C.trevrpc_engine_diagnostics_v1) trevrpcc.Diagnostics {
	return trevrpcc.Diagnostics{
		ABIVersion: uint32(value.engine_abi_version), State: uint32(value.state), TerminalStatus: int(value.terminal_status),
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
