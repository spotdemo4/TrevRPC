//go:build cgo

package zstd

// The streaming half of the shim: one compression or decompression context
// per direction of a call, fed a message at a time. As in zstd_cgo.go, the
// prototypes below declare only what this file calls; the two small helpers
// keep libzstd's buffer structs on the C side.

/*
#include <stddef.h>
typedef struct { void* dst; size_t size; size_t pos; } trevrpc_out_buffer;
typedef struct { const void* src; size_t size; size_t pos; } trevrpc_in_buffer;
typedef struct { unsigned windowLog, chainLog, hashLog, searchLog, minMatch, targetLength; int strategy; } trevrpc_cparams;
size_t ZSTD_isError(size_t code);
void* ZSTD_createCCtx(void);
size_t ZSTD_freeCCtx(void* cctx);
size_t ZSTD_CCtx_setParameter(void* cctx, int param, int value);
size_t ZSTD_CCtx_refCDict(void* cctx, const void* cdict);
size_t ZSTD_compressStream2(void* cctx, trevrpc_out_buffer* output, trevrpc_in_buffer* input, int endOp);
void* ZSTD_createDCtx(void);
size_t ZSTD_freeDCtx(void* dctx);
size_t ZSTD_DCtx_setParameter(void* dctx, int param, int value);
size_t ZSTD_DCtx_refDDict(void* dctx, const void* ddict);
size_t ZSTD_decompressStream(void* dctx, trevrpc_out_buffer* output, trevrpc_in_buffer* input);
trevrpc_cparams ZSTD_getCParams(int level, unsigned long long srcSize, size_t dictSize);
size_t ZSTD_estimateCStreamSize_usingCParams(trevrpc_cparams params);
size_t ZSTD_estimateDStreamSize(size_t windowSize);

// Feeds src to cctx with endOp until it is consumed and flushed or dst is
// full. It returns libzstd's result: zero when done, an error code, or how
// much is left to flush into a bigger dst.
static size_t trevrpc_compress_stream(void* cctx, void* dst, size_t cap, size_t* written,
		const void* src, size_t len, size_t* consumed, int endOp) {
	trevrpc_out_buffer out = { dst, cap, 0 };
	trevrpc_in_buffer in = { src, len, 0 };
	size_t left;
	do {
		left = ZSTD_compressStream2(cctx, &out, &in, endOp);
		if (ZSTD_isError(left)) {
			return left;
		}
	} while (left != 0 && out.pos < out.size);
	*written = out.pos;
	*consumed = in.pos;
	return left;
}

// Decompresses src into dst until src is consumed or no progress is made,
// and returns zero or an error code.
static size_t trevrpc_decompress_stream(void* dctx, void* dst, size_t cap, size_t* written,
		const void* src, size_t len, size_t* consumed) {
	trevrpc_out_buffer out = { dst, cap, 0 };
	trevrpc_in_buffer in = { src, len, 0 };
	while (in.pos < in.size) {
		size_t inBefore = in.pos, outBefore = out.pos;
		size_t ret = ZSTD_decompressStream(dctx, &out, &in);
		if (ZSTD_isError(ret)) {
			return ret;
		}
		if (in.pos == inBefore && out.pos == outBefore) {
			break;
		}
	}
	*written = out.pos;
	*consumed = in.pos;
	return 0;
}

static size_t trevrpc_cstream_cost(int level, unsigned windowLog) {
	trevrpc_cparams params = ZSTD_getCParams(level, 0, 0);
	params.windowLog = windowLog;
	return ZSTD_estimateCStreamSize_usingCParams(params);
}
*/
import "C"

import (
	"errors"
	"runtime"
	"slices"
	"sync"
	"unsafe"
)

// Parameter IDs and values from zstd.h 1.5.7, beside those of zstd_cgo.go.
const (
	cParamLevel     = 100 // ZSTD_c_compressionLevel
	cParamWindowLog = 101 // ZSTD_c_windowLog
	dParamWindowMax = 100 // ZSTD_d_windowLogMax
	endFlush        = 1   // ZSTD_e_flush
)

// errClosed reports a stream context used after Close.
var errClosed = errors.New("zstd: stream context is closed")

// streamContext is a libzstd context that Close frees at once and the garbage
// collector frees if Close never runs. Its mutex serializes the uses with
// Close, which may come from another goroutine as a call ends.
type streamContext struct {
	mu   sync.Mutex
	p    unsafe.Pointer
	free func(unsafe.Pointer)
}

func (c *streamContext) close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.p != nil {
		c.free(c.p)
		c.p = nil
	}
}

func freeCCtx(p unsafe.Pointer) { C.ZSTD_freeCCtx(p) }

func freeDCtx(p unsafe.Pointer) { C.ZSTD_freeDCtx(p) }

// StreamEncoder compresses the messages of one direction of a call into
// ZSTD_STREAM_MESSAGE chunks: one magicless frame without checksum or content
// size, flushed after every message. It is used by one goroutine at a time,
// and Close may be called from any.
type StreamEncoder struct {
	ctx  *streamContext
	dict *Dict
}

// NewStreamEncoder creates an encoder at level with a window of
// 2^windowLog bytes, whose frame starts from dict if it is not nil.
func NewStreamEncoder(dict *Dict, level, windowLog int) (*StreamEncoder, error) {
	p := C.ZSTD_createCCtx()
	if p == nil {
		return nil, errors.New("zstd: cannot create a compression context")
	}
	ctx := &streamContext{p: p, free: freeCCtx}
	params := [...][2]int{
		{cParamLevel, level},
		{cParamWindowLog, windowLog},
		{cParamChecksum, 0},
		{cParamContentSize, 0},
		{cParamDictID, 1},
		{cParamFormat, formatMagicless},
	}
	for _, param := range params {
		if C.ZSTD_isError(C.ZSTD_CCtx_setParameter(p, C.int(param[0]), C.int(param[1]))) != 0 {
			ctx.close()
			return nil, errors.New("zstd: invalid stream compression parameters")
		}
	}
	if dict != nil && C.ZSTD_isError(C.ZSTD_CCtx_refCDict(p, dict.handles.cdict)) != 0 {
		ctx.close()
		return nil, errors.New("zstd: cannot reference dictionary")
	}
	e := &StreamEncoder{ctx: ctx, dict: dict}
	runtime.AddCleanup(e, (*streamContext).close, ctx)
	return e, nil
}

// Encode appends the chunk that src, one message, compresses to.
func (e *StreamEncoder) Encode(dst, src []byte) ([]byte, error) {
	e.ctx.mu.Lock()
	defer e.ctx.mu.Unlock()
	if e.ctx.p == nil {
		return dst, errClosed
	}
	// Room for the message and the frame header that may precede it.
	dst = slices.Grow(dst, CompressBound(len(src))+32)
	for {
		out := dst[len(dst):cap(dst)]
		var written, consumed C.size_t
		left := C.trevrpc_compress_stream(e.ctx.p, unsafe.Pointer(unsafe.SliceData(out)), C.size_t(len(out)), &written,
			unsafe.Pointer(unsafe.SliceData(src)), C.size_t(len(src)), &consumed, endFlush)
		if C.ZSTD_isError(left) != 0 {
			return dst, errors.New("zstd: compression failed")
		}
		dst = dst[:len(dst)+int(written)]
		src = src[consumed:]
		if left == 0 {
			break
		}
		dst = slices.Grow(dst, max(int(left), 1024))
	}
	runtime.KeepAlive(e.dict)
	return dst, nil
}

// Close frees the encoder's context. Encode fails afterwards.
func (e *StreamEncoder) Close() { e.ctx.close() }

// StreamDecoder decompresses the ZSTD_STREAM_MESSAGE chunks of one direction
// of a call, in order. It is used by one goroutine at a time, and Close may be
// called from any.
type StreamDecoder struct {
	ctx *streamContext
	// dict is the dictionary of the current frame, kept alive while libzstd
	// references it.
	dict *Dict
}

// NewStreamDecoder creates a decoder that accepts windows of at most
// 2^maxWindowLog bytes.
func NewStreamDecoder(maxWindowLog int) (*StreamDecoder, error) {
	p := C.ZSTD_createDCtx()
	if p == nil {
		return nil, errors.New("zstd: cannot create a decompression context")
	}
	ctx := &streamContext{p: p, free: freeDCtx}
	if C.ZSTD_isError(C.ZSTD_DCtx_setParameter(p, dParamFormat, formatMagicless)) != 0 ||
		C.ZSTD_isError(C.ZSTD_DCtx_setParameter(p, dParamWindowMax, C.int(maxWindowLog))) != 0 {
		ctx.close()
		return nil, errors.New("zstd: invalid stream decompression parameters")
	}
	d := &StreamDecoder{ctx: ctx}
	runtime.AddCleanup(d, (*streamContext).close, ctx)
	return d, nil
}

// Decode appends the message of chunk to dst: exactly size bytes. info is
// what ParseStreamChunk returned for chunk, and dict the dictionary named by
// the frame it starts, nil for none. A chunk that does not decompress to
// exactly size bytes is ErrInvalid, and the decoder cannot be used after it.
func (d *StreamDecoder) Decode(dst, chunk []byte, info StreamChunk, dict *Dict, size int) ([]byte, error) {
	d.ctx.mu.Lock()
	defer d.ctx.mu.Unlock()
	if d.ctx.p == nil {
		return dst, errClosed
	}
	if info.StartsFrame {
		var ddict unsafe.Pointer
		if dict != nil {
			ddict = dict.handles.ddict
		}
		// A frame without a dictionary must not inherit the previous one's.
		if C.ZSTD_isError(C.ZSTD_DCtx_refDDict(d.ctx.p, ddict)) != 0 {
			return dst, ErrInvalid
		}
		d.dict = dict
	}
	if len(chunk) == 0 {
		if size != 0 {
			return dst, ErrInvalid
		}
		return dst, nil
	}
	// One spare byte: libzstd holds back output that does not fit, so a chunk
	// holding more than size bytes shows only as an extra byte written.
	dst = slices.Grow(dst, size+1)
	out := dst[len(dst) : len(dst)+size+1]
	var written, consumed C.size_t
	ret := C.trevrpc_decompress_stream(d.ctx.p, unsafe.Pointer(unsafe.SliceData(out)), C.size_t(len(out)), &written,
		unsafe.Pointer(unsafe.SliceData(chunk)), C.size_t(len(chunk)), &consumed)
	runtime.KeepAlive(d.dict)
	if C.ZSTD_isError(ret) != 0 || int(consumed) != len(chunk) || int(written) != size {
		return dst, ErrInvalid
	}
	return dst[:len(dst)+size], nil
}

// Close frees the decoder's context. Decode fails afterwards.
func (d *StreamDecoder) Close() { d.ctx.close() }

// StreamEncoderCost is libzstd's estimate of the memory of a StreamEncoder at
// level with a window of 2^windowLog bytes.
func StreamEncoderCost(level, windowLog int) int64 {
	return int64(C.trevrpc_cstream_cost(C.int(level), C.uint(windowLog)))
}

// StreamDecoderCost is libzstd's estimate of the memory of a StreamDecoder for
// windows of at most 2^windowLog bytes.
func StreamDecoderCost(windowLog int) int64 {
	return int64(C.ZSTD_estimateDStreamSize(C.size_t(1) << windowLog))
}
