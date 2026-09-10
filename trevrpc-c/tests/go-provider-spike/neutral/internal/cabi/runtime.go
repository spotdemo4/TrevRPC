package cabi

/*
#cgo CFLAGS: -std=c11
#include "abi.h"
*/
import "C"

import (
	"errors"
	"fmt"
	"sync"

	trevrpcc "trev.zip/llc/trevrpc/trevrpc-c"
	"trev.zip/llc/trevrpc/trevrpc-c/internal/providerabi"
)

type runtime struct {
	mu       sync.Mutex
	native   *C.trevrpc_go_provider_spike_runtime
	identity uint64
}

type engine struct{ runtime }
type transport struct{ runtime }

var _ trevrpcc.Engine = (*engine)(nil)
var _ trevrpcc.Transport = (*transport)(nil)

func AdoptEngine(descriptor providerabi.Descriptor) (trevrpcc.Engine, error) {
	native, err := adopt(descriptor, C.TREVRPC_GO_PROVIDER_SPIKE_ENGINE)
	if err != nil {
		return nil, err
	}
	return &engine{runtime{native: native, identity: uint64(C.trevrpc_go_provider_spike_identity(native))}}, nil
}

func AdoptTransport(descriptor providerabi.Descriptor) (trevrpcc.Transport, error) {
	native, err := adopt(descriptor, C.TREVRPC_GO_PROVIDER_SPIKE_TRANSPORT)
	if err != nil {
		return nil, err
	}
	return &transport{runtime{native: native, identity: uint64(C.trevrpc_go_provider_spike_identity(native))}}, nil
}

func adopt(
	descriptor providerabi.Descriptor,
	expectedKind C.uint32_t,
) (*C.trevrpc_go_provider_spike_runtime, error) {
	var nativeDescriptor C.trevrpc_go_provider_spike_descriptor_v1
	nativeDescriptor.struct_size = C.uint32_t(descriptor.StructSize)
	nativeDescriptor.struct_version = C.uint32_t(descriptor.StructVersion)
	nativeDescriptor.kind = C.uint32_t(descriptor.Kind)
	nativeDescriptor.reserved0 = C.uint32_t(descriptor.Reserved)
	nativeDescriptor.operations = (*C.trevrpc_go_provider_spike_ops_v1)(descriptor.Operations)
	nativeDescriptor.context = descriptor.Context
	var native *C.trevrpc_go_provider_spike_runtime
	status := int(C.trevrpc_go_provider_spike_adopt_v1(&nativeDescriptor, expectedKind, &native))
	if status != 0 {
		return nil, fmt.Errorf("adopt provider runtime: status %d", status)
	}
	if native == nil {
		return nil, errors.New("adopt provider runtime returned nil")
	}
	return native, nil
}

func (r *runtime) Identity() uint64 {
	if r == nil {
		return 0
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.native == nil {
		return r.identity
	}
	return uint64(C.trevrpc_go_provider_spike_identity(r.native))
}

func (r *runtime) Close() error {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.native != nil {
		C.trevrpc_go_provider_spike_release(r.native)
		r.native = nil
	}
	return nil
}
