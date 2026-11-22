"""QUIC variable-length integers (RFC 9000, section 16), which frame headers use."""

from __future__ import annotations

MAX_VARINT = (1 << 62) - 1
"""The largest value a QUIC variable-length integer holds."""


def varint_len(value: int) -> int:
    """The length of the minimal encoding of `value`."""
    if value < 0x40:
        return 1
    if value < 0x4000:
        return 2
    if value < 0x4000_0000:
        return 4
    return 8


def encode_varint(value: int) -> bytes:
    """Encodes `value` minimally; raises `ValueError` if it does not fit."""
    if value < 0 or value > MAX_VARINT:
        raise ValueError(f"varint {value} out of range")
    if value < 0x40:
        return bytes((value,))
    if value < 0x4000:
        return (value | 0x4000).to_bytes(2)
    if value < 0x4000_0000:
        return (value | 0x8000_0000).to_bytes(4)
    return (value | 0xC000_0000_0000_0000).to_bytes(8)


def decode_varint(data: bytes | bytearray | memoryview, pos: int) -> tuple[int, int] | None:
    """Decodes the varint at `pos`, accepting any valid encoding.

    Returns the value and the position after it, or `None` when `data` ends first.
    """
    if pos >= len(data):
        return None
    first = data[pos]
    length = 1 << (first >> 6)
    end = pos + length
    if end > len(data):
        return None
    if length == 1:
        return first, end
    value = first & 0x3F
    for i in range(pos + 1, end):
        value = (value << 8) | data[i]
    return value, end
