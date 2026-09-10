//go:build cgo && (linux || darwin)

package testprovider

/*
#cgo CFLAGS: -std=c11 -Wall -Wextra -Werror -I${SRCDIR}/../../../include
#include "provider.h"
*/
import "C"

import (
	"errors"
	"unsafe"

	"trev.zip/llc/trevrpc/trevrpc-c/internal/providerabi"
)

func NewEngine(host providerabi.EngineHost, ownerCookie uint64) (providerabi.EngineDescriptor, error) {
	if host.Operations == nil {
		return providerabi.EngineDescriptor{}, errors.New("test Engine provider received an empty host")
	}
	var operations *C.trevrpc_engine_provider_ops_v1
	var context unsafe.Pointer
	status := int(C.trevrpc_go_test_engine_provider_create(
		(*C.trevrpc_engine_provider_host_v1)(host.Operations), C.uint64_t(ownerCookie), &operations, &context))
	if status != 0 {
		return providerabi.EngineDescriptor{}, errors.New("create test Engine provider")
	}
	return providerabi.NewEngineDescriptor(unsafe.Pointer(operations), context, ownerCookie), nil
}

func DisposeEngine(descriptor providerabi.EngineDescriptor) {
	if descriptor.Context != nil {
		C.trevrpc_go_test_engine_provider_dispose(descriptor.Context)
	}
}

func EngineDestroyCount() uint64 {
	return uint64(C.trevrpc_go_test_engine_destroy_count())
}

func NewTransport() (providerabi.TransportDescriptor, error) {
	var operations *C.trevrpc_transport_provider_ops_v1
	var context unsafe.Pointer
	if C.trevrpc_go_test_transport_provider_create(&operations, &context) != 0 {
		return providerabi.TransportDescriptor{}, errors.New("create test Transport provider")
	}
	return providerabi.NewTransportDescriptor(unsafe.Pointer(operations), context), nil
}

func DisposeTransport(descriptor providerabi.TransportDescriptor) {
	if descriptor.Context != nil {
		C.trevrpc_go_test_transport_provider_dispose(descriptor.Context)
	}
}

func SetTransportOperationsSize(descriptor providerabi.TransportDescriptor, size uint32) {
	C.trevrpc_go_test_transport_ops_set_size(
		(*C.trevrpc_transport_provider_ops_v1)(descriptor.Operations), C.uint32_t(size))
}

func SetTransportOperationsVersion(descriptor providerabi.TransportDescriptor, version uint32) {
	C.trevrpc_go_test_transport_ops_set_version(
		(*C.trevrpc_transport_provider_ops_v1)(descriptor.Operations), C.uint32_t(version))
}

func SetTransportOperationsReserved(descriptor providerabi.TransportDescriptor, value uint64) {
	C.trevrpc_go_test_transport_ops_set_reserved(
		(*C.trevrpc_transport_provider_ops_v1)(descriptor.Operations), C.uint64_t(value))
}

func TransportDestroyCount() uint64 {
	return uint64(C.trevrpc_go_test_transport_destroy_count())
}

func TransportDestroyBeforeDrainCount() uint64 {
	return uint64(C.trevrpc_go_test_transport_destroy_before_drain_count())
}
