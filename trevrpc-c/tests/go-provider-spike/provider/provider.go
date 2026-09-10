//go:build cgo && linux && amd64

package msquic

/*
#cgo CFLAGS: -I${SRCDIR}/include
#cgo LDFLAGS: ${SRCDIR}/lib/linux_amd64/libprovider.a

#include <stdint.h>
#include "abi.h"

int trevrpc_go_provider_spike_make_descriptor(
    uint32_t kind,
    uint64_t identity,
    uint32_t mutation,
    trevrpc_go_provider_spike_descriptor_v1* descriptor);
void trevrpc_go_provider_spike_dispose_descriptor(
    trevrpc_go_provider_spike_descriptor_v1* descriptor);
*/
import "C"

import (
	"fmt"
	"unsafe"

	trevrpcc "trev.zip/llc/trevrpc/trevrpc-c"
	neutralcabi "trev.zip/llc/trevrpc/trevrpc-c/internal/cabi"
	"trev.zip/llc/trevrpc/trevrpc-c/internal/providerabi"
)

func NewEngine(identity uint64) (trevrpcc.Engine, error) {
	return newEngine(identity, 0)
}

func NewTransport(identity uint64) (trevrpcc.Transport, error) {
	return newTransport(identity, 0)
}

func newEngine(identity uint64, mutation uint32) (trevrpcc.Engine, error) {
	descriptor, native, err := makeDescriptor(providerabi.EngineKind, identity, mutation)
	if err != nil {
		return nil, err
	}
	engine, err := neutralcabi.AdoptEngine(descriptor)
	if err != nil {
		C.trevrpc_go_provider_spike_dispose_descriptor(native)
		return nil, err
	}
	return engine, nil
}

func newTransport(identity uint64, mutation uint32) (trevrpcc.Transport, error) {
	descriptor, native, err := makeDescriptor(providerabi.TransportKind, identity, mutation)
	if err != nil {
		return nil, err
	}
	transport, err := neutralcabi.AdoptTransport(descriptor)
	if err != nil {
		C.trevrpc_go_provider_spike_dispose_descriptor(native)
		return nil, err
	}
	return transport, nil
}

func makeDescriptor(
	kind uint32,
	identity uint64,
	mutation uint32,
) (providerabi.Descriptor, *C.trevrpc_go_provider_spike_descriptor_v1, error) {
	native := new(C.trevrpc_go_provider_spike_descriptor_v1)
	status := int(C.trevrpc_go_provider_spike_make_descriptor(
		C.uint32_t(kind),
		C.uint64_t(identity),
		C.uint32_t(mutation),
		native,
	))
	if status != 0 {
		return providerabi.Descriptor{}, nil, fmt.Errorf("create provider descriptor: status %d", status)
	}
	return providerabi.Descriptor{
		StructSize:    uint32(native.struct_size),
		StructVersion: uint32(native.struct_version),
		Kind:          uint32(native.kind),
		Reserved:      uint32(native.reserved0),
		Operations:    unsafe.Pointer(native.operations),
		Context:       native.context,
	}, native, nil
}
