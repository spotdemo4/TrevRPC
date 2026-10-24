//! The libzstd WebAssembly module behind `@trevrpc/trevrpc-js/zstd`.
//!
//! A thin C ABI over the reference libzstd for `ZSTD_MESSAGE` payloads (magicless frames with a
//! `Dictionary_ID` and a `Frame_Content_Size` and without a checksum) and for the per-call contexts
//! of `ZSTD_STREAM_MESSAGE` chunks. JavaScript validates payloads and chunks before decompressing
//! them, so this module only compresses and decompresses.
//!
//! Every export takes and returns plain numbers: pointers into linear memory and sizes. The
//! per-message contexts and digested dictionaries live as long as the module instance, which
//! JavaScript creates once per compression configuration; stream contexts are created per call
//! and freed when it ends. Compressing and decompressing return the output size, or -1 on any
//! failure.

use core::ffi::{c_int, c_void};
use std::alloc::{Layout, alloc, dealloc};

use zstd_sys::{
    ZSTD_CCtx, ZSTD_CCtx_refCDict, ZSTD_CCtx_setParameter, ZSTD_CDict, ZSTD_DCtx,
    ZSTD_DCtx_refDDict, ZSTD_DCtx_setParameter, ZSTD_DDict, ZSTD_EndDirective, ZSTD_cParameter,
    ZSTD_compress2, ZSTD_compressBound, ZSTD_compressStream2, ZSTD_createCCtx, ZSTD_createCDict,
    ZSTD_createDCtx, ZSTD_createDDict, ZSTD_dParameter, ZSTD_decompress_usingDDict,
    ZSTD_decompressStream, ZSTD_estimateCStreamSize_usingCParams, ZSTD_estimateDStreamSize,
    ZSTD_freeCCtx, ZSTD_freeDCtx, ZSTD_getCParams, ZSTD_inBuffer, ZSTD_isError, ZSTD_outBuffer,
};

/// `ZSTD_f_zstd1_magicless`, the value of the format parameters (`ZSTD_c_format`, named
/// `ZSTD_c_experimentalParam2` in the stable header, and `ZSTD_d_format`, named
/// `ZSTD_d_experimentalParam1`).
const FORMAT_MAGICLESS: c_int = 1;

/// Allocates `size` bytes of linear memory for JavaScript to fill.
#[unsafe(no_mangle)]
pub extern "C" fn zs_alloc(size: usize) -> *mut u8 {
    Layout::from_size_align(size.max(1), 8).map_or(core::ptr::null_mut(), |layout| {
        // SAFETY: the layout has a non-zero size.
        unsafe { alloc(layout) }
    })
}

/// Frees memory from `zs_alloc`.
///
/// # Safety
///
/// `ptr` must come from `zs_alloc(size)` and not have been freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn zs_free(ptr: *mut u8, size: usize) {
    if let Ok(layout) = Layout::from_size_align(size.max(1), 8) {
        // SAFETY: the caller passes a live allocation of this layout.
        unsafe { dealloc(ptr, layout) }
    }
}

/// The largest payload `zs_compress` writes for `len` bytes.
#[unsafe(no_mangle)]
pub extern "C" fn zs_compress_bound(len: usize) -> usize {
    // SAFETY: a pure function of its argument.
    unsafe { ZSTD_compressBound(len) }
}

/// Creates the compression context that writes `ZSTD_MESSAGE` payloads, or returns null.
#[unsafe(no_mangle)]
pub extern "C" fn zs_cctx_new() -> *mut ZSTD_CCtx {
    // SAFETY: the context is checked before use and only ever passed back to libzstd.
    unsafe {
        let cctx = ZSTD_createCCtx();
        if cctx.is_null() {
            return cctx;
        }
        let params = [
            (ZSTD_cParameter::ZSTD_c_experimentalParam2, FORMAT_MAGICLESS),
            (ZSTD_cParameter::ZSTD_c_checksumFlag, 0),
            (ZSTD_cParameter::ZSTD_c_dictIDFlag, 1),
            (ZSTD_cParameter::ZSTD_c_contentSizeFlag, 1),
        ];
        for (param, value) in params {
            if ZSTD_isError(ZSTD_CCtx_setParameter(cctx, param, value)) != 0 {
                return core::ptr::null_mut();
            }
        }
        cctx
    }
}

/// Creates the decompression context that reads magicless frames, or returns null.
#[unsafe(no_mangle)]
pub extern "C" fn zs_dctx_new() -> *mut ZSTD_DCtx {
    // SAFETY: the context is checked before use and only ever passed back to libzstd.
    unsafe {
        let dctx = ZSTD_createDCtx();
        if dctx.is_null()
            || ZSTD_isError(ZSTD_DCtx_setParameter(
                dctx,
                ZSTD_dParameter::ZSTD_d_experimentalParam1,
                FORMAT_MAGICLESS,
            )) != 0
        {
            return core::ptr::null_mut();
        }
        dctx
    }
}

/// Digests a dictionary for compression at `level`, or returns null. The dictionary is copied.
///
/// # Safety
///
/// `dict` must point to `len` readable bytes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn zs_cdict_new(
    dict: *const u8,
    len: usize,
    level: c_int,
) -> *mut ZSTD_CDict {
    // SAFETY: the caller passes a readable dictionary.
    unsafe { ZSTD_createCDict(dict.cast::<c_void>(), len, level) }
}

/// Digests a dictionary for decompression, or returns null. The dictionary is copied.
///
/// # Safety
///
/// `dict` must point to `len` readable bytes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn zs_ddict_new(dict: *const u8, len: usize) -> *mut ZSTD_DDict {
    // SAFETY: the caller passes a readable dictionary.
    unsafe { ZSTD_createDDict(dict.cast::<c_void>(), len) }
}

/// Compresses `src` with `cdict` into `dst`, returning the payload size or -1.
///
/// # Safety
///
/// The handles must come from this module; `dst` and `src` must be valid for `cap` and `len`
/// bytes and must not overlap.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn zs_compress(
    cctx: *mut ZSTD_CCtx,
    cdict: *const ZSTD_CDict,
    dst: *mut u8,
    cap: usize,
    src: *const u8,
    len: usize,
) -> isize {
    // SAFETY: the caller passes live handles and valid buffers.
    unsafe {
        if ZSTD_isError(ZSTD_CCtx_refCDict(cctx, cdict)) != 0 {
            return -1;
        }
        result(ZSTD_compress2(
            cctx,
            dst.cast::<c_void>(),
            cap,
            src.cast::<c_void>(),
            len,
        ))
    }
}

/// Decompresses the frame in `src` with `ddict` into `dst`, returning the message size or -1.
///
/// # Safety
///
/// The handles must come from this module; `dst` and `src` must be valid for `cap` and `len`
/// bytes and must not overlap.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn zs_decompress(
    dctx: *mut ZSTD_DCtx,
    ddict: *const ZSTD_DDict,
    dst: *mut u8,
    cap: usize,
    src: *const u8,
    len: usize,
) -> isize {
    // SAFETY: the caller passes live handles and valid buffers.
    unsafe {
        result(ZSTD_decompress_usingDDict(
            dctx,
            dst.cast::<c_void>(),
            cap,
            src.cast::<c_void>(),
            len,
            ddict,
        ))
    }
}

/// Maps a libzstd size result to the size, or -1 for an error code.
fn result(code: usize) -> isize {
    // SAFETY: a pure function of its argument.
    if unsafe { ZSTD_isError(code) } != 0 {
        return -1;
    }
    isize::try_from(code).unwrap_or(-1)
}

/// libzstd's estimate of the memory of a stream compressing context at `level` with a window of
/// `2^window_log` bytes.
#[unsafe(no_mangle)]
pub extern "C" fn zs_stream_encoder_cost(level: c_int, window_log: u32) -> usize {
    // SAFETY: pure functions of their arguments.
    unsafe {
        let mut params = ZSTD_getCParams(level, 0, 0);
        params.windowLog = window_log;
        ZSTD_estimateCStreamSize_usingCParams(params)
    }
}

/// libzstd's estimate of the memory of a stream decompressing context for a window of
/// `2^window_log` bytes.
#[unsafe(no_mangle)]
pub extern "C" fn zs_stream_decoder_cost(window_log: u32) -> usize {
    // SAFETY: a pure function of its argument.
    unsafe { ZSTD_estimateDStreamSize(1 << window_log) }
}

/// Creates the compressing context of one direction of a call: magicless frames without checksum
/// or content size, at `level`, with a window of `2^window_log` bytes, starting from `cdict`
/// unless it is null. Returns null on failure.
///
/// # Safety
///
/// `cdict` must be null or come from this module.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn zs_stream_encoder_new(
    level: c_int,
    window_log: c_int,
    cdict: *const ZSTD_CDict,
) -> *mut ZSTD_CCtx {
    // SAFETY: the context is checked before use and freed on failure.
    unsafe {
        let cctx = ZSTD_createCCtx();
        if cctx.is_null() {
            return cctx;
        }
        let params = [
            (ZSTD_cParameter::ZSTD_c_compressionLevel, level),
            (ZSTD_cParameter::ZSTD_c_windowLog, window_log),
            (ZSTD_cParameter::ZSTD_c_experimentalParam2, FORMAT_MAGICLESS),
            (ZSTD_cParameter::ZSTD_c_checksumFlag, 0),
            (ZSTD_cParameter::ZSTD_c_contentSizeFlag, 0),
            (ZSTD_cParameter::ZSTD_c_dictIDFlag, 1),
        ];
        let mut ok = params
            .into_iter()
            .all(|(param, value)| ZSTD_isError(ZSTD_CCtx_setParameter(cctx, param, value)) == 0);
        if ok && !cdict.is_null() {
            ok = ZSTD_isError(ZSTD_CCtx_refCDict(cctx, cdict)) == 0;
        }
        if !ok {
            ZSTD_freeCCtx(cctx);
            return core::ptr::null_mut();
        }
        cctx
    }
}

/// Compresses one message through a stream compressing context and flushes it, writing the chunk
/// to `dst`. Returns the chunk size, or -1 if it failed or `cap` was too small; the context cannot
/// be used after a failure.
///
/// # Safety
///
/// `cctx` must come from `zs_stream_encoder_new`; `dst` and `src` must be valid for `cap` and `len`
/// bytes and must not overlap.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn zs_stream_compress(
    cctx: *mut ZSTD_CCtx,
    dst: *mut u8,
    cap: usize,
    src: *const u8,
    len: usize,
) -> isize {
    let mut input = ZSTD_inBuffer {
        src: src.cast::<c_void>(),
        size: len,
        pos: 0,
    };
    let mut output = ZSTD_outBuffer {
        dst: dst.cast::<c_void>(),
        size: cap,
        pos: 0,
    };
    loop {
        // SAFETY: the caller passes a live context and valid buffers.
        let left = unsafe {
            ZSTD_compressStream2(
                cctx,
                &raw mut output,
                &raw mut input,
                ZSTD_EndDirective::ZSTD_e_flush,
            )
        };
        // SAFETY: a pure function of its argument.
        if unsafe { ZSTD_isError(left) } != 0 {
            return -1;
        }
        if left == 0 {
            return isize::try_from(output.pos).unwrap_or(-1);
        }
        if output.pos == output.size {
            return -1;
        }
    }
}

/// Frees a stream compressing context.
///
/// # Safety
///
/// `cctx` must come from `zs_stream_encoder_new` and not have been freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn zs_stream_encoder_free(cctx: *mut ZSTD_CCtx) {
    // SAFETY: the caller passes a live context.
    unsafe {
        ZSTD_freeCCtx(cctx);
    }
}

/// Creates the decompressing context of one direction of a call: magicless frames with a window
/// of at most `2^window_log_max` bytes. Returns null on failure.
#[unsafe(no_mangle)]
pub extern "C" fn zs_stream_decoder_new(window_log_max: c_int) -> *mut ZSTD_DCtx {
    // SAFETY: the context is checked before use and freed on failure.
    unsafe {
        let dctx = ZSTD_createDCtx();
        if dctx.is_null() {
            return dctx;
        }
        let params = [
            (ZSTD_dParameter::ZSTD_d_experimentalParam1, FORMAT_MAGICLESS),
            (ZSTD_dParameter::ZSTD_d_windowLogMax, window_log_max),
        ];
        if !params
            .into_iter()
            .all(|(param, value)| ZSTD_isError(ZSTD_DCtx_setParameter(dctx, param, value)) == 0)
        {
            ZSTD_freeDCtx(dctx);
            return core::ptr::null_mut();
        }
        dctx
    }
}

/// Makes the frame that starts with the next chunk decompress with `ddict`, or with no
/// dictionary when it is null. Returns 0, or -1 on failure.
///
/// # Safety
///
/// `dctx` must come from `zs_stream_decoder_new`, and `ddict` must be null or come from this
/// module.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn zs_stream_decoder_ref(
    dctx: *mut ZSTD_DCtx,
    ddict: *const ZSTD_DDict,
) -> isize {
    // SAFETY: the caller passes live handles.
    if unsafe { ZSTD_isError(ZSTD_DCtx_refDDict(dctx, ddict)) } != 0 {
        return -1;
    }
    0
}

/// Decompresses one chunk through a stream decompressing context into `dst`. Returns how many
/// bytes it wrote, or -1 on failure; JavaScript passes room for one byte more than the message
/// size, since libzstd holds back output that does not fit. The context cannot be used after a
/// failure.
///
/// # Safety
///
/// `dctx` must come from `zs_stream_decoder_new`; `dst` and `src` must be valid for `cap` and `len`
/// bytes and must not overlap.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn zs_stream_decompress(
    dctx: *mut ZSTD_DCtx,
    dst: *mut u8,
    cap: usize,
    src: *const u8,
    len: usize,
) -> isize {
    let mut input = ZSTD_inBuffer {
        src: src.cast::<c_void>(),
        size: len,
        pos: 0,
    };
    let mut output = ZSTD_outBuffer {
        dst: dst.cast::<c_void>(),
        size: cap,
        pos: 0,
    };
    while input.pos < input.size {
        let (read, written) = (input.pos, output.pos);
        // SAFETY: the caller passes a live context and valid buffers.
        let result = unsafe { ZSTD_decompressStream(dctx, &raw mut output, &raw mut input) };
        // SAFETY: a pure function of its argument.
        if unsafe { ZSTD_isError(result) } != 0 || (input.pos == read && output.pos == written) {
            return -1;
        }
    }
    isize::try_from(output.pos).unwrap_or(-1)
}

/// Frees a stream decompressing context.
///
/// # Safety
///
/// `dctx` must come from `zs_stream_decoder_new` and not have been freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn zs_stream_decoder_free(dctx: *mut ZSTD_DCtx) {
    // SAFETY: the caller passes a live context.
    unsafe {
        ZSTD_freeDCtx(dctx);
    }
}
