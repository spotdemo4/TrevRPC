"""The zstd codecs of compressed messages: dictionaries, ZSTD_MESSAGE payloads, and the contexts of
stream compression (ZSTD_STREAM_MESSAGE payloads).

TrevRPC's zstd frames are magicless (`ZSTD_f_zstd1_magicless`): standard frames without their
4-byte Magic_Number. Python's `compression.zstd` writes and reads only standard frames, so the
magic number is cut from what it writes and put back in front of what it reads; frame headers are
validated here before libzstd sees them.

Errors are `CompressionError`s, which say whether the payload was invalid or declared a message
larger than the limit; the call layer maps them to the status codes of its direction.
"""

from __future__ import annotations

import hashlib
from collections.abc import Callable
from dataclasses import dataclass
from types import ModuleType
from typing import TYPE_CHECKING, Literal

from ._varint import decode_varint, encode_varint

if TYPE_CHECKING:
    from compression import zstd
else:
    zstd: ModuleType | None
    try:
        from compression import zstd
    except ImportError:  # A Python built without libzstd: nothing compresses.
        zstd = None


def _available() -> bool:
    return zstd is not None  # pyright: ignore[reportUnnecessaryComparison]


AVAILABLE = _available()
"""Whether this Python has `compression.zstd`. Without it, nothing is compressed or offered."""

_MAGIC = b"\x28\xb5\x2f\xfd"
_DICTIONARY_MAGIC = b"\x37\xa4\x30\xec"

STREAM_WINDOW_LOG = 16
"""Every stream frame's window is at most 64 KiB."""

STREAM_FRAME_LIMIT = 60 * 1024
"""A stream frame ends with the message that brings it to this many bytes of messages, before
libzstd's 64 KiB streaming buffer wraps and matching costs a quarter more CPU."""

_BLOCK_RAW = 0
_BLOCK_RLE = 1
_BLOCK_RESERVED = 3


class CompressionError(Exception):
    """A compressed payload that cannot be accepted."""

    def __init__(self, kind: Literal["invalid", "too_large"], reason: str) -> None:
        super().__init__(reason)
        self.kind: Literal["invalid", "too_large"] = kind
        self.reason = reason


def _invalid(reason: str) -> CompressionError:
    return CompressionError("invalid", reason)


# Dictionaries


@dataclass(frozen=True, slots=True, eq=False)
class LoadedDictionary:
    """A validated dictionary: its ID, fingerprint, and the libzstd dictionary."""

    id: int
    fingerprint: bytes
    """The first 8 bytes of the dictionary's SHA-256 digest."""
    zstd_dict: zstd.ZstdDict


def load_dictionary(data: bytes) -> LoadedDictionary:
    """Validates a zstd-format dictionary, as `zstd --train` writes them. Raw-content dictionaries
    have no ID and are refused."""
    if not AVAILABLE:
        raise ValueError("this Python has no zstd support (compression.zstd)")
    if not data.startswith(_DICTIONARY_MAGIC):
        raise ValueError("not a zstd-format dictionary: raw-content dictionaries have no ID")
    try:
        zstd_dict = zstd.ZstdDict(data)
    except (ValueError, zstd.ZstdError) as error:
        raise ValueError(f"invalid zstd dictionary: {error}") from error
    if zstd_dict.dict_id == 0:
        raise ValueError("a dictionary's ID must not be 0")
    return LoadedDictionary(zstd_dict.dict_id, hashlib.sha256(data).digest()[:8], zstd_dict)


# Frame headers


@dataclass(frozen=True, slots=True)
class _FrameHeader:
    size: int
    """The header's length in bytes, from the Frame_Header_Descriptor."""
    single_segment: bool
    checksum: bool
    window: int | None
    """The window size in bytes, when the header has a Window_Descriptor."""
    dictionary_id: int
    content_size: int | None


_DICTIONARY_ID_SIZES = (0, 1, 2, 4)
_CONTENT_SIZE_SIZES = (0, 2, 4, 8)


def _frame_header(data: bytes | memoryview, start: int = 0) -> _FrameHeader:
    """Parses a magicless frame header (RFC 8878 section 3.1.1.1) at `start`."""
    if len(data) <= start:
        raise _invalid("missing frame header")
    descriptor = data[start]
    content_flag = descriptor >> 6
    single_segment = bool(descriptor & 0x20)
    if descriptor & 0x08:
        raise _invalid("frame header with its reserved bit set")
    pos = start + 1
    window = None
    if not single_segment:
        if len(data) <= pos:
            raise _invalid("truncated frame header")
        exponent, mantissa = data[pos] >> 3, data[pos] & 0x07
        base = 1 << (10 + exponent)
        window = base + (base >> 3) * mantissa
        pos += 1
    id_size = _DICTIONARY_ID_SIZES[descriptor & 0x03]
    content_size_size = _CONTENT_SIZE_SIZES[content_flag]
    if content_flag == 0 and single_segment:
        content_size_size = 1
    end = pos + id_size + content_size_size
    if len(data) < end:
        raise _invalid("truncated frame header")
    dictionary_id = int.from_bytes(data[pos : pos + id_size], "little")
    pos += id_size
    content_size = None
    if content_size_size:
        content_size = int.from_bytes(data[pos:end], "little")
        if content_size_size == 2:
            content_size += 256
    return _FrameHeader(
        end - start, single_segment, bool(descriptor & 0x04), window, dictionary_id, content_size
    )


type DictionaryLookup = Callable[[int], LoadedDictionary | Literal[False] | None]
"""The dictionaries a receiver accepts in a direction, by ID: the dictionary, `None` for ID 0 (no
dictionary) when it is accepted, or `False` for an ID it does not accept."""


# ZSTD_MESSAGE


class MessageCompressor:
    """Compresses ZSTD_MESSAGE payloads with one dictionary, or none, at one level."""

    __slots__ = ("_compressor",)

    def __init__(self, dictionary: LoadedDictionary | None, level: int) -> None:
        self._compressor = zstd.ZstdCompressor(
            level=level, zstd_dict=dictionary.zstd_dict if dictionary is not None else None
        )

    def compress(self, message: bytes) -> bytes | None:
        """The payload, or `None` when it would not be shorter than the message."""
        frame = self._compressor.compress(message, zstd.ZstdCompressor.FLUSH_FRAME)
        # The whole message in one call: libzstd writes its content size into the header.
        payload = frame[len(_MAGIC) :]
        return payload if len(payload) < len(message) else None


def decompress_message(payload: bytes, lookup: DictionaryLookup, max_size: int) -> bytes:
    """Decompresses a ZSTD_MESSAGE payload: exactly one magicless frame that declares its content
    size, with a dictionary the receiver accepts."""
    header = _frame_header(payload)
    if header.content_size is None:
        raise _invalid("compressed message without Frame_Content_Size")
    if header.content_size > max_size:
        raise CompressionError(
            "too_large",
            f"compressed message of {header.content_size} bytes exceeds the {max_size}-byte limit",
        )
    dictionary = lookup(header.dictionary_id)
    if dictionary is False:
        raise _invalid(f"compressed message with dictionary {header.dictionary_id}, not accepted")
    decompressor = zstd.ZstdDecompressor(
        zstd_dict=dictionary.zstd_dict if dictionary is not None else None
    )
    try:
        message = decompressor.decompress(_MAGIC + payload, max_length=header.content_size + 1)
    except zstd.ZstdError as error:
        raise _invalid(f"compressed message does not decompress: {error}") from error
    if not decompressor.eof:
        raise _invalid("compressed message is truncated")
    if decompressor.unused_data:
        raise _invalid("bytes after a compressed message's frame")
    if len(message) != header.content_size:
        raise _invalid("compressed message does not match its Frame_Content_Size")
    return message


# ZSTD_STREAM_MESSAGE


class StreamCompressor:
    """The compressing context of one direction of a call."""

    __slots__ = ("_compressor", "_frame_open", "_in_frame")

    def __init__(self, dictionary: LoadedDictionary | None, level: int) -> None:
        parameter = zstd.CompressionParameter
        self._compressor = zstd.ZstdCompressor(
            options={
                parameter.compression_level: level,
                parameter.window_log: STREAM_WINDOW_LOG,
                parameter.content_size_flag: 0,
                parameter.checksum_flag: 0,
            },
            zstd_dict=dictionary.zstd_dict if dictionary is not None else None,
        )
        self._frame_open = False
        self._in_frame = 0

    def compress(self, message: bytes) -> bytes:
        """The message's payload: its size, then its chunk. Every message ends a block, so that it
        can be decoded on arrival; the one that brings the frame to `STREAM_FRAME_LIMIT` ends the
        frame, and the next starts another from the same dictionary."""
        if not message:
            # libzstd writes nothing at all for an empty message.
            return encode_varint(0)
        self._in_frame += len(message)
        end = self._in_frame >= STREAM_FRAME_LIMIT
        mode = zstd.ZstdCompressor.FLUSH_FRAME if end else zstd.ZstdCompressor.FLUSH_BLOCK
        chunk = self._compressor.compress(message, mode)
        if not self._frame_open:
            chunk = chunk[len(_MAGIC) :]
            self._frame_open = True
        if end:
            self._frame_open = False
            self._in_frame = 0
        return encode_varint(len(message)) + chunk


class StreamDecompressor:
    """The decompressing context of one direction of a call. Its chunks, in order, are a sequence of
    magicless frames: each chunk is whole blocks, after a frame header when it starts a frame. The
    context cannot continue after an error."""

    __slots__ = ("_decompressor", "_lookup")

    def __init__(self, lookup: DictionaryLookup) -> None:
        self._lookup = lookup
        self._decompressor: zstd.ZstdDecompressor | None = None
        """The current frame's decompressor, or `None` between frames."""

    def decompress(self, payload: bytes, max_size: int) -> bytes:
        decoded = decode_varint(payload, 0)
        if decoded is None:
            raise _invalid("stream-compressed message without its size")
        size, pos = decoded
        if size > max_size:
            raise CompressionError(
                "too_large",
                f"stream-compressed message of {size} bytes exceeds the {max_size}-byte limit",
            )
        if pos == len(payload):
            # An empty chunk is an empty message, whatever the frame state.
            if size:
                raise _invalid("empty chunk for a non-empty stream-compressed message")
            return b""
        chunk = memoryview(payload)[pos:]
        starts = self._decompressor is None
        blocks = 0
        seed: LoadedDictionary | None = None
        if starts:
            header = _frame_header(chunk)
            if header.single_segment or header.content_size is not None:
                raise _invalid("stream frame with Single_Segment_flag or Frame_Content_Size")
            if header.checksum:
                raise _invalid("stream frame with Content_Checksum_flag")
            if header.window is None or header.window > 1 << STREAM_WINDOW_LOG:
                raise _invalid("stream frame with a window above 64 KiB")
            dictionary = self._lookup(header.dictionary_id)
            if dictionary is False:
                raise _invalid(f"stream frame from dictionary {header.dictionary_id}, not accepted")
            seed = dictionary
            blocks = header.size
        ends = _walk_blocks(chunk, blocks)
        if starts:
            self._decompressor = zstd.ZstdDecompressor(
                zstd_dict=seed.zstd_dict if seed is not None else None,
                options={zstd.DecompressionParameter.window_log_max: STREAM_WINDOW_LOG},
            )
            data = _MAGIC + chunk
        else:
            data = chunk
        decompressor = self._decompressor
        assert decompressor is not None
        try:
            # libzstd holds back output that does not fit: room for one byte more catches a chunk
            # that holds more than its size.
            message = decompressor.decompress(data, max_length=size + 1)
        except (zstd.ZstdError, EOFError) as error:
            raise _invalid(f"stream-compressed message does not decompress: {error}") from error
        if len(message) != size:
            raise _invalid("stream-compressed message does not match its size")
        if ends:
            if not decompressor.eof or decompressor.unused_data:
                raise _invalid("stream frame does not end with its last block")
            self._decompressor = None
        elif not decompressor.needs_input:
            raise _invalid("stream-compressed message holds more than its size")
        return message


def _walk_blocks(chunk: memoryview, pos: int) -> bool:
    """Checks that a chunk is whole blocks from `pos`; returns whether it ends the frame."""
    end = len(chunk)
    if pos == end:
        raise _invalid("stream chunk without a block")
    while pos < end:
        if end - pos < 3:
            raise _invalid("truncated block header in a stream chunk")
        header = chunk[pos] | chunk[pos + 1] << 8 | chunk[pos + 2] << 16
        last = header & 1
        block_type = (header >> 1) & 3
        if block_type == _BLOCK_RESERVED:
            raise _invalid("reserved block type in a stream chunk")
        length = 1 if block_type == _BLOCK_RLE else header >> 3
        pos += 3 + length
        if pos > end:
            raise _invalid("truncated block in a stream chunk")
        if last:
            if pos != end:
                raise _invalid("bytes after the last block of a stream frame")
            return True
    return False
