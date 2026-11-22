"""TrevRPC frames: a QUIC varint type, a QUIC varint length, and a payload."""

from __future__ import annotations

from enum import IntEnum

from ._status import Code, TrevRpcError
from ._varint import decode_varint, encode_varint


class FrameType(IntEnum):
    MESSAGE = 0x00
    REQUEST = 0x01
    HEADERS = 0x02
    STATUS = 0x03
    ZSTD_MESSAGE = 0x04
    ZSTD_STREAM_MESSAGE = 0x05
    ANSWER = 0x06
    DATAGRAM_COUNT = 0x07


KNOWN_TYPES = frozenset(FrameType)

DEFAULT_MAX_FRAME_SIZE = 4 * 1024 * 1024
"""The default limit on a frame's payload, in bytes."""

MAX_FRAME_HEADER_SIZE = 16
"""Two 8-byte varints: a stream receive window of the frame size plus this always fits a frame."""


def encode_frame(frame_type: int, payload: bytes | bytearray | memoryview) -> bytes:
    """Encodes a frame with minimal varints."""
    return b"".join((encode_varint(frame_type), encode_varint(len(payload)), payload))


class FrameReader:
    """Reads frames from a byte stream that arrives in pieces.

    Frames of unknown type are skipped. A declared length over the limit fails with
    `ResourceExhausted` before the payload is buffered, whatever the frame's type.
    """

    __slots__ = ("_buffer", "_max_frame_size", "_pos", "consumed")

    def __init__(self, max_frame_size: int = DEFAULT_MAX_FRAME_SIZE) -> None:
        self._buffer = bytearray()
        self._pos = 0
        self._max_frame_size = max_frame_size
        self.consumed = 0
        """Every byte read as part of a frame so far, skipped frames included."""

    @property
    def buffered(self) -> int:
        """The bytes received but not yet read as part of a frame."""
        return len(self._buffer) - self._pos

    def feed(self, data: bytes | bytearray | memoryview) -> None:
        """Appends received bytes."""
        if self._pos and self._pos * 2 >= len(self._buffer):
            del self._buffer[: self._pos]
            self._pos = 0
        self._buffer += data

    def next(self) -> tuple[int, bytes] | None:
        """Returns the next known frame's type and payload, or `None` until more bytes arrive."""
        buffer = self._buffer
        while True:
            start = self._pos
            header = decode_varint(buffer, start)
            if header is None:
                return None
            frame_type, pos = header
            header = decode_varint(buffer, pos)
            if header is None:
                return None
            length, pos = header
            if length > self._max_frame_size:
                raise TrevRpcError(
                    Code.RESOURCE_EXHAUSTED,
                    f"frame of {length} bytes exceeds the {self._max_frame_size}-byte limit",
                )
            end = pos + length
            if end > len(buffer):
                return None
            self._pos = end
            self.consumed += end - start
            if frame_type in KNOWN_TYPES:
                # One copy: slicing the bytearray itself would copy twice.
                with memoryview(buffer) as view:
                    return frame_type, view[pos:end].tobytes()
