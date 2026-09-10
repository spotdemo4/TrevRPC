//go:build cgo && (linux || darwin)

package cabi

/*
#cgo CFLAGS: -std=c11 -Wall -Wextra -Werror -I${SRCDIR}/../../include -I${SRCDIR}/../../src
#include <errno.h>
#include <stdlib.h>
#include <stdint.h>
*/
import "C"

import (
	"errors"
	"fmt"
	"unsafe"

	trevrpcc "trev.zip/llc/trevrpc/trevrpc-c"
)

func statusError(operation string, status int) error {
	if status == 0 {
		return nil
	}
	return &trevrpcc.StatusError{Operation: operation, Status: status}
}

func nativeStatusError(operation string, status int) error {
	if status == -int(C.EAGAIN) {
		return trevrpcc.ErrWouldBlock
	}
	return statusError(operation, status)
}

func copyBytes(name string, data *C.uint8_t, length uint64) ([]byte, error) {
	if length == 0 {
		return nil, nil
	}
	if data == nil {
		return nil, fmt.Errorf("%s returned a nil pointer", name)
	}
	if length > uint64(^uint32(0)>>1) {
		return nil, fmt.Errorf("%s exceeds Go copy limit", name)
	}
	return C.GoBytes(unsafe.Pointer(data), C.int(length)), nil
}

type cAllocations []unsafe.Pointer

func (a *cAllocations) keep(pointer unsafe.Pointer) {
	if pointer != nil {
		*a = append(*a, pointer)
	}
}

func (a cAllocations) free() {
	for _, allocation := range a {
		C.free(allocation)
	}
}

func (a *cAllocations) string32(value string) (*C.char, C.uint32_t, error) {
	if uint64(len(value)) > uint64(^uint32(0)) {
		return nil, 0, errors.New("native endpoint string exceeds uint32 length")
	}
	if value == "" {
		return nil, 0, nil
	}
	result := C.CString(value)
	a.keep(unsafe.Pointer(result))
	return result, C.uint32_t(len(value)), nil
}

func (a *cAllocations) bytes32(value []byte) (*C.uint8_t, C.uint32_t, error) {
	if uint64(len(value)) > uint64(^uint32(0)) {
		return nil, 0, errors.New("native endpoint bytes exceed uint32 length")
	}
	if len(value) == 0 {
		return nil, 0, nil
	}
	result := C.CBytes(value)
	a.keep(result)
	return (*C.uint8_t)(result), C.uint32_t(len(value)), nil
}

func (a *cAllocations) bytes64(value []byte) (*C.uint8_t, C.uint64_t) {
	if len(value) == 0 {
		return nil, 0
	}
	result := C.CBytes(value)
	a.keep(result)
	return (*C.uint8_t)(result), C.uint64_t(len(value))
}
