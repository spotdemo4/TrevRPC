//go:build cgo

package zstd

// libzstd comes from github.com/DataDog/zstd, which compiles its bundled copy;
// the blank import links those objects, and the prototypes below declare only
// what this file calls, so nothing here vendors C. The format parameters are
// "experimental" in zstd.h but always compiled into the library.

/*
#include <stddef.h>
size_t ZSTD_isError(size_t code);
void* ZSTD_createCCtx(void);
size_t ZSTD_freeCCtx(void* cctx);
size_t ZSTD_CCtx_setParameter(void* cctx, int param, int value);
size_t ZSTD_CCtx_refCDict(void* cctx, const void* cdict);
size_t ZSTD_compress2(void* cctx, void* dst, size_t dstCapacity, const void* src, size_t srcSize);
void* ZSTD_createCDict(const void* dict, size_t dictSize, int level);
size_t ZSTD_freeCDict(void* cdict);
void* ZSTD_createDCtx(void);
size_t ZSTD_freeDCtx(void* dctx);
size_t ZSTD_DCtx_setParameter(void* dctx, int param, int value);
void* ZSTD_createDDict(const void* dict, size_t dictSize);
size_t ZSTD_freeDDict(void* ddict);
size_t ZSTD_decompress_usingDDict(void* dctx, void* dst, size_t dstCapacity, const void* src, size_t srcSize, const void* ddict);
size_t ZSTD_decompressDCtx(void* dctx, void* dst, size_t dstCapacity, const void* src, size_t srcSize);
*/
import "C"

import (
	"errors"
	"runtime"
	"slices"
	"sync"
	"unsafe"

	_ "github.com/DataDog/zstd" // links libzstd
)

// Parameter IDs and values from zstd.h 1.5.7.
const (
	cParamChecksum    = 201  // ZSTD_c_checksumFlag
	cParamDictID      = 202  // ZSTD_c_dictIDFlag
	cParamContentSize = 200  // ZSTD_c_contentSizeFlag
	cParamFormat      = 10   // ZSTD_c_format (ZSTD_c_experimentalParam2)
	dParamFormat      = 1000 // ZSTD_d_format (ZSTD_d_experimentalParam1)
	formatMagicless   = 1    // ZSTD_f_zstd1_magicless
)

// Available reports whether this binary can compress and decompress.
const Available = true

// Dict is a dictionary digested for both directions, or, from NewPlain, the
// absence of one. It is safe for concurrent use and frees its libzstd state
// when garbage collected.
type Dict struct {
	ID          uint32
	Fingerprint [8]byte
	handles     *dictHandles
	// plain holds the compression contexts of a Dict without a dictionary,
	// which carry its level; nil for a dictionary, whose level is in its
	// handles.
	plain *sync.Pool
}

type dictHandles struct{ cdict, ddict unsafe.Pointer }

// NewDict digests a zstd-format dictionary for compression at level and for
// decompression.
func NewDict(data []byte, level int) (*Dict, error) {
	id, err := DictID(data)
	if err != nil {
		return nil, err
	}
	ptr, size := unsafe.Pointer(unsafe.SliceData(data)), C.size_t(len(data))
	h := &dictHandles{cdict: C.ZSTD_createCDict(ptr, size, C.int(level)), ddict: C.ZSTD_createDDict(ptr, size)}
	if h.cdict == nil || h.ddict == nil {
		h.free()
		return nil, errors.New("zstd: cannot load dictionary")
	}
	d := &Dict{ID: id, Fingerprint: Fingerprint(data), handles: h}
	runtime.AddCleanup(d, (*dictHandles).free, h)
	return d, nil
}

// NewPlain returns the Dict of messages compressed at level without a
// dictionary: its frames carry no Dictionary_ID, and its ID is zero.
func NewPlain(level int) *Dict {
	return &Dict{plain: &sync.Pool{New: func() any {
		c := newCCtx()
		C.ZSTD_CCtx_setParameter(c.p, cParamLevel, C.int(level))
		return c
	}}}
}

func (h *dictHandles) free() {
	if h.cdict != nil {
		C.ZSTD_freeCDict(h.cdict)
	}
	if h.ddict != nil {
		C.ZSTD_freeDDict(h.ddict)
	}
}

// Contexts are pooled: creating one per message costs about as much as
// compressing a small message.
type cctx struct{ p unsafe.Pointer }

type dctx struct{ p unsafe.Pointer }

// cctxPool holds the contexts of dictionaries, which take their level from the
// dictionary they reference.
var cctxPool = sync.Pool{New: func() any { return newCCtx() }}

// newCCtx creates a context that writes ZSTD_MESSAGE payloads.
func newCCtx() *cctx {
	c := &cctx{p: C.ZSTD_createCCtx()}
	C.ZSTD_CCtx_setParameter(c.p, cParamChecksum, 0)
	C.ZSTD_CCtx_setParameter(c.p, cParamDictID, 1)
	C.ZSTD_CCtx_setParameter(c.p, cParamContentSize, 1)
	C.ZSTD_CCtx_setParameter(c.p, cParamFormat, formatMagicless)
	runtime.AddCleanup(c, func(p unsafe.Pointer) { C.ZSTD_freeCCtx(p) }, c.p)
	return c
}

var dctxPool = sync.Pool{New: func() any {
	d := &dctx{p: C.ZSTD_createDCtx()}
	C.ZSTD_DCtx_setParameter(d.p, dParamFormat, formatMagicless)
	runtime.AddCleanup(d, func(p unsafe.Pointer) { C.ZSTD_freeDCtx(p) }, d.p)
	return d
}}

// Compress appends src, compressed with d into a ZSTD_MESSAGE payload, to dst.
func (d *Dict) Compress(dst, src []byte) ([]byte, error) {
	bound := CompressBound(len(src))
	dst = slices.Grow(dst, bound)
	out := dst[len(dst) : len(dst)+bound]
	pool := d.plain
	if pool == nil {
		pool = &cctxPool
	}
	c := pool.Get().(*cctx)
	defer pool.Put(c)
	if d.handles != nil && C.ZSTD_isError(C.ZSTD_CCtx_refCDict(c.p, d.handles.cdict)) != 0 {
		return dst, errors.New("zstd: cannot reference dictionary")
	}
	n := C.ZSTD_compress2(c.p, unsafe.Pointer(unsafe.SliceData(out)), C.size_t(len(out)),
		unsafe.Pointer(unsafe.SliceData(src)), C.size_t(len(src)))
	runtime.KeepAlive(d)
	if C.ZSTD_isError(n) != 0 {
		return dst, errors.New("zstd: compression failed")
	}
	return dst[:len(dst)+int(n)], nil
}

// Decompress appends the message of payload, whose header h came from
// ParseHeader, to dst. The caller has checked h.ContentSize against its limits
// and that h.DictID is d.ID.
func (d *Dict) Decompress(dst, payload []byte, h Header) ([]byte, error) {
	size := int(h.ContentSize)
	dst = slices.Grow(dst, size)
	out := dst[len(dst) : len(dst)+size]
	x := dctxPool.Get().(*dctx)
	defer dctxPool.Put(x)
	dstPtr, srcPtr := unsafe.Pointer(unsafe.SliceData(out)), unsafe.Pointer(unsafe.SliceData(payload))
	var n C.size_t
	if d.handles == nil {
		n = C.ZSTD_decompressDCtx(x.p, dstPtr, C.size_t(size), srcPtr, C.size_t(len(payload)))
	} else {
		n = C.ZSTD_decompress_usingDDict(x.p, dstPtr, C.size_t(size), srcPtr, C.size_t(len(payload)), d.handles.ddict)
	}
	runtime.KeepAlive(d)
	if C.ZSTD_isError(n) != 0 || int(n) != size {
		return dst, ErrInvalid
	}
	return dst[:len(dst)+size], nil
}
