//! The libzstd WebAssembly module behind `@trevrpc/trevrpc-js/zstd`.
//!
//! A thin C ABI over the reference libzstd for `ZSTD_MESSAGE` payloads: magicless frames with a
//! `Dictionary_ID` and a `Frame_Content_Size` and without a checksum. JavaScript validates
//! payloads before decompressing them, so this module only compresses and decompresses.
//!
//! Every export takes and returns plain numbers: pointers into linear memory and sizes. Contexts
//! and digested dictionaries live as long as the module instance; JavaScript creates one instance
//! per compression configuration and never frees them. Compressing and decompressing return the
//! output size, or -1 on any failure.

use core::ffi::{c_int, c_void};
use std::alloc::{Layout, alloc, dealloc};

use zstd_sys::{
    ZSTD_CCtx, ZSTD_CCtx_refCDict, ZSTD_CCtx_setParameter, ZSTD_CDict, ZSTD_DCtx,
    ZSTD_DCtx_setParameter, ZSTD_DDict, ZSTD_cParameter, ZSTD_compress2, ZSTD_compressBound,
    ZSTD_createCCtx, ZSTD_createCDict, ZSTD_createDCtx, ZSTD_createDDict, ZSTD_dParameter,
    ZSTD_decompress_usingDDict, ZSTD_isError,
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
