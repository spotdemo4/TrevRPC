//go:build cgo && linux && (amd64 || arm64)

package cabi

/*
#cgo CFLAGS: -std=c11 -Wall -Wextra -Werror -DQUIC_API_ENABLE_PREVIEW_FEATURES -DQUIC_API_ENABLE_VERSIONED_FEATURES=1 -DTREVRPC_MSQUIC_PROVIDER_CAPABILITIES -DTREVRPC_GO_PROVIDER_BUILD -I${SRCDIR}/../../include -I${SRCDIR}/../../src -I${SRCDIR}/../../third_party/msquic/include
#cgo amd64 LDFLAGS: ${SRCDIR}/../../lib/linux_amd64/libmsquic.a -ldl -lpthread -lm
#cgo arm64 LDFLAGS: ${SRCDIR}/../../lib/linux_arm64/libmsquic.a -ldl -lpthread -lm

#include "../../src/trevrpc_engine_msquic_internal.h"
#include "../../src/trevrpc_rpc_transport_msquic_internal.h"
*/
import "C"

import (
	"errors"
	"unsafe"

	trevrpcc "trev.zip/llc/trevrpc/trevrpc-c"
	neutralcabi "trev.zip/llc/trevrpc/trevrpc-c/internal/cabi"
	"trev.zip/llc/trevrpc/trevrpc-c/internal/providerabi"
)

func NewEngine(config trevrpcc.EngineConfig) (trevrpcc.EngineRuntime, error) {
	config = normalizeEngineConfig(config)
	host := neutralcabi.EngineHost()
	if host.Operations == nil {
		return nil, trevrpcc.ErrUnavailable
	}
	var nativeConfig C.trevrpc_engine_config_v1
	nativeConfig.struct_size = C.uint32_t(C.sizeof_trevrpc_engine_config_v1)
	nativeConfig.struct_version = C.TREVRPC_ENGINE_STRUCT_VERSION_1
	nativeConfig.event_capacity = C.uint32_t(config.EventCapacity)
	nativeConfig.listener_capacity = C.uint32_t(config.ListenerCapacity)
	nativeConfig.connection_capacity = C.uint32_t(config.ConnectionCapacity)
	nativeConfig.stream_capacity = C.uint32_t(config.StreamCapacity)
	nativeConfig.max_receive_owned_count = C.uint32_t(config.MaxReceiveOwnedCount)
	nativeConfig.max_receive_owned_bytes = C.uint64_t(config.MaxReceiveOwnedBytes)
	var providerConfig C.trevrpc_engine_msquic_config_v1
	providerConfig.struct_size = C.uint32_t(C.sizeof_trevrpc_engine_msquic_config_v1)
	providerConfig.struct_version = C.TREVRPC_ENGINE_MSQUIC_STRUCT_VERSION_1
	var descriptor C.trevrpc_engine_provider_descriptor_v1
	status := int(C.trevrpc_engine_msquic_provider_descriptor_create_v1(
		(*C.trevrpc_engine_provider_host_v1)(host.Operations),
		&nativeConfig,
		&providerConfig,
		&descriptor,
	))
	if status != 0 {
		return nil, &trevrpcc.StatusError{Operation: "create MsQuic Engine provider", Status: status}
	}
	goDescriptor := providerabi.NewEngineDescriptor(
		unsafe.Pointer(descriptor.operations), descriptor.context, uint64(descriptor.owner_cookie))
	return neutralcabi.AdoptEngine(config, goDescriptor)
}

func NewTransport(config trevrpcc.TransportConfig) (trevrpcc.TransportRuntime, error) {
	config = normalizeTransportConfig(config)
	host := neutralcabi.EngineHost()
	if host.Operations == nil {
		return nil, trevrpcc.ErrUnavailable
	}
	var nativeConfig C.trevrpc_transport_config_v1
	nativeConfig.struct_size = C.uint32_t(C.sizeof_trevrpc_transport_config_v1)
	nativeConfig.struct_version = C.TREVRPC_TRANSPORT_STRUCT_VERSION_1
	nativeConfig.event_capacity = C.uint32_t(config.EventCapacity)
	nativeConfig.listener_capacity = C.uint32_t(config.ListenerCapacity)
	nativeConfig.connection_capacity = C.uint32_t(config.ConnectionCapacity)
	nativeConfig.stream_capacity = C.uint32_t(config.StreamCapacity)
	nativeConfig.max_receive_owned_count = C.uint32_t(config.MaxReceiveOwnedCount)
	nativeConfig.max_receive_owned_bytes = C.uint64_t(config.MaxReceiveOwnedBytes)
	var providerConfig C.trevrpc_transport_msquic_config_v1
	providerConfig.struct_size = C.uint32_t(C.sizeof_trevrpc_transport_msquic_config_v1)
	providerConfig.struct_version = C.TREVRPC_TRANSPORT_MSQUIC_STRUCT_VERSION_1
	var descriptor C.trevrpc_transport_provider_descriptor_v1
	status := int(C.trevrpc_transport_msquic_provider_descriptor_create_v1(
		(*C.trevrpc_engine_provider_host_v1)(host.Operations),
		&nativeConfig,
		&providerConfig,
		&descriptor,
	))
	if status != 0 {
		return nil, &trevrpcc.StatusError{Operation: "create MsQuic Transport provider", Status: status}
	}
	if descriptor.operations == nil || descriptor.context == nil {
		return nil, errors.New("create MsQuic Transport provider returned an invalid descriptor")
	}
	return neutralcabi.AdoptTransport(providerabi.NewTransportDescriptor(
		unsafe.Pointer(descriptor.operations), descriptor.context))
}

func normalizeEngineConfig(config trevrpcc.EngineConfig) trevrpcc.EngineConfig {
	defaults := trevrpcc.DefaultEngineConfig()
	if config.EventCapacity == 0 {
		config.EventCapacity = defaults.EventCapacity
	}
	if config.ListenerCapacity == 0 {
		config.ListenerCapacity = defaults.ListenerCapacity
	}
	if config.ConnectionCapacity == 0 {
		config.ConnectionCapacity = defaults.ConnectionCapacity
	}
	if config.StreamCapacity == 0 {
		config.StreamCapacity = defaults.StreamCapacity
	}
	if config.MaxReceiveOwnedCount == 0 {
		config.MaxReceiveOwnedCount = defaults.MaxReceiveOwnedCount
	}
	if config.MaxReceiveOwnedBytes == 0 {
		config.MaxReceiveOwnedBytes = defaults.MaxReceiveOwnedBytes
	}
	return config
}

func normalizeTransportConfig(config trevrpcc.TransportConfig) trevrpcc.TransportConfig {
	defaults := trevrpcc.DefaultTransportConfig()
	if config.EventCapacity == 0 {
		config.EventCapacity = defaults.EventCapacity
	}
	if config.ListenerCapacity == 0 {
		config.ListenerCapacity = defaults.ListenerCapacity
	}
	if config.ConnectionCapacity == 0 {
		config.ConnectionCapacity = defaults.ConnectionCapacity
	}
	if config.StreamCapacity == 0 {
		config.StreamCapacity = defaults.StreamCapacity
	}
	if config.MaxReceiveOwnedCount == 0 {
		config.MaxReceiveOwnedCount = defaults.MaxReceiveOwnedCount
	}
	if config.MaxReceiveOwnedBytes == 0 {
		config.MaxReceiveOwnedBytes = defaults.MaxReceiveOwnedBytes
	}
	return config
}
