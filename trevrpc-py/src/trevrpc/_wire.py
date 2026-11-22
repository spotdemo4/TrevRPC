"""The protobuf control messages, coded by hand so the runtime needs no generated code for them.

Senders write fields in field-number order, leave out default values and sort map entries by key,
so the bytes match the golden vectors. Receivers skip unknown fields of every wire type.
"""

from __future__ import annotations

from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass, field
from enum import IntEnum

from ._status import StatusDetail


class WireError(ValueError):
    """A control message that does not decode."""


class RpcKind(IntEnum):
    UNARY = 0
    CLIENT_STREAMING = 1
    SERVER_STREAMING = 2
    BIDI_STREAMING = 3


class StreamCompression(IntEnum):
    NONE = 0
    ENABLED = 1
    OPTIMISTIC = 2


# Encoding


def _uvarint(value: int) -> bytes:
    out = bytearray()
    while value > 0x7F:
        out.append((value & 0x7F) | 0x80)
        value >>= 7
    out.append(value)
    return bytes(out)


def _tag(number: int, wire_type: int) -> bytes:
    return _uvarint(number << 3 | wire_type)


def _len_field(number: int, data: bytes) -> bytes:
    return _tag(number, 2) + _uvarint(len(data)) + data


def _varint_field(number: int, value: int) -> bytes:
    return _tag(number, 0) + _uvarint(value) if value else b""


def _map_field(number: int, entries: Mapping[str, bytes]) -> bytes:
    out = bytearray()
    for key in sorted(entries):
        value = entries[key]
        entry = (_len_field(1, key.encode()) if key else b"") + (
            _len_field(2, value) if value else b""
        )
        out += _len_field(number, entry)
    return bytes(out)


@dataclass(frozen=True, slots=True)
class DictionaryInfo:
    id: int
    fingerprint: bytes = b""

    def encode(self) -> bytes:
        return _varint_field(1, self.id) + (
            _len_field(2, self.fingerprint) if self.fingerprint else b""
        )


@dataclass(frozen=True, slots=True)
class ConnectionMetadata:
    id: int
    metadata: Mapping[str, bytes] = field(default_factory=dict[str, bytes])

    def encode(self) -> bytes:
        return _varint_field(1, self.id) + _map_field(2, self.metadata)


@dataclass(frozen=True, slots=True)
class RpcRequest:
    method: str
    metadata: Mapping[str, bytes] = field(default_factory=dict[str, bytes])
    kind: RpcKind = RpcKind.UNARY
    timeout_nanos: int = 0
    datagram_flow: int = 0
    accept_dictionaries: Sequence[int] = ()
    dictionary_query: Sequence[DictionaryInfo] = ()
    stream_compression: StreamCompression = StreamCompression.NONE
    connection_metadata: ConnectionMetadata | None = None

    def encode(self) -> bytes:
        out = bytearray()
        if self.method:
            out += _len_field(1, self.method.encode())
        out += _map_field(2, self.metadata)
        out += _varint_field(3, self.kind)
        out += _varint_field(4, self.timeout_nanos)
        out += _varint_field(5, self.datagram_flow)
        if self.accept_dictionaries:
            out += _len_field(6, b"".join(_uvarint(i) for i in self.accept_dictionaries))
        for info in self.dictionary_query:
            out += _len_field(7, info.encode())
        out += _varint_field(8, self.stream_compression)
        if self.connection_metadata is not None:
            out += _len_field(9, self.connection_metadata.encode())
        return bytes(out)


# Decoding


def read_fields(data: bytes) -> Iterator[tuple[int, int, int | bytes]]:
    """Yields each field's number, wire type and value: an `int` for varint and fixed-width
    fields, `bytes` for length-delimited ones. Groups are skipped."""
    pos = 0
    end = len(data)
    while pos < end:
        key, pos = _read_uvarint(data, pos)
        number, wire_type = key >> 3, key & 7
        if number == 0:
            raise WireError("field number 0")
        if wire_type == 0:
            value, pos = _read_uvarint(data, pos)
            yield number, wire_type, value
        elif wire_type == 1:
            if pos + 8 > end:
                raise WireError("truncated fixed64")
            yield number, wire_type, int.from_bytes(data[pos : pos + 8], "little")
            pos += 8
        elif wire_type == 2:
            length, pos = _read_uvarint(data, pos)
            if pos + length > end:
                raise WireError("truncated length-delimited field")
            yield number, wire_type, data[pos : pos + length]
            pos += length
        elif wire_type == 5:
            if pos + 4 > end:
                raise WireError("truncated fixed32")
            yield number, wire_type, int.from_bytes(data[pos : pos + 4], "little")
            pos += 4
        elif wire_type == 3:
            pos = _skip_group(data, pos, number)
        else:
            raise WireError(f"invalid wire type {wire_type}")


def _read_uvarint(data: bytes, pos: int) -> tuple[int, int]:
    value = 0
    shift = 0
    end = len(data)
    while True:
        if pos >= end:
            raise WireError("truncated varint")
        byte = data[pos]
        pos += 1
        value |= (byte & 0x7F) << shift
        if byte < 0x80:
            if shift == 63 and byte > 1:
                raise WireError("varint overflows 64 bits")
            return value, pos
        shift += 7
        if shift > 63:
            raise WireError("varint is longer than 10 bytes")


def _skip_group(data: bytes, pos: int, number: int) -> int:
    depth = [number]
    end = len(data)
    while depth:
        key, pos = _read_uvarint(data, pos)
        field_number, wire_type = key >> 3, key & 7
        if wire_type == 0:
            _, pos = _read_uvarint(data, pos)
        elif wire_type == 1:
            pos += 8
        elif wire_type == 2:
            length, pos = _read_uvarint(data, pos)
            pos += length
        elif wire_type == 5:
            pos += 4
        elif wire_type == 3:
            depth.append(field_number)
        elif wire_type == 4:
            if depth.pop() != field_number:
                raise WireError("mismatched end group")
        else:
            raise WireError(f"invalid wire type {wire_type}")
        if pos > end:
            raise WireError("truncated group")
    return pos


def _string(value: int | bytes) -> str:
    if not isinstance(value, bytes):
        raise WireError("string field with a non-length-delimited wire type")
    try:
        return value.decode()
    except UnicodeDecodeError as error:
        raise WireError("string field is not UTF-8") from error


def _bytes(value: int | bytes) -> bytes:
    if not isinstance(value, bytes):
        raise WireError("bytes field with a non-length-delimited wire type")
    return value


def _int(value: int | bytes) -> int:
    if not isinstance(value, int):
        raise WireError("integer field with a length-delimited wire type")
    return value


def _uint32(value: int | bytes) -> int:
    return _int(value) & 0xFFFF_FFFF


def _map_entry(value: int | bytes, into: dict[str, bytes]) -> None:
    key = ""
    entry_value = b""
    for number, _, item in read_fields(_bytes(value)):
        if number == 1:
            key = _string(item)
        elif number == 2:
            entry_value = _bytes(item)
    into[key] = entry_value


@dataclass(frozen=True, slots=True)
class RpcHeaders:
    metadata: dict[str, bytes]

    @classmethod
    def decode(cls, data: bytes) -> RpcHeaders:
        metadata: dict[str, bytes] = {}
        for number, _, value in read_fields(data):
            if number == 1:
                _map_entry(value, metadata)
        return cls(metadata)


@dataclass(frozen=True, slots=True)
class RpcStatus:
    code: int
    message: str
    metadata: dict[str, bytes]
    refused_stream_compression: bool
    details: tuple[StatusDetail, ...]

    @classmethod
    def decode(cls, data: bytes) -> RpcStatus:
        code = 0
        message = ""
        metadata: dict[str, bytes] = {}
        refused = False
        details: list[StatusDetail] = []
        for number, _, value in read_fields(data):
            if number == 1:
                code = _uint32(value)
            elif number == 2:
                message = _string(value)
            elif number == 3:
                _map_entry(value, metadata)
            elif number == 4:
                refused = _int(value) != 0
            elif number == 5:
                detail_type = ""
                detail_value = b""
                for detail_number, _, item in read_fields(_bytes(value)):
                    if detail_number == 1:
                        detail_type = _string(item)
                    elif detail_number == 2:
                        detail_value = _bytes(item)
                details.append(StatusDetail(detail_type, detail_value))
        return cls(code, message, metadata, refused, tuple(details))


@dataclass(frozen=True, slots=True)
class RpcAnswer:
    dictionaries: tuple[DictionaryInfo, ...]
    stream_compression: bool
    connection_metadata: int

    @classmethod
    def decode(cls, data: bytes) -> RpcAnswer:
        dictionaries: list[DictionaryInfo] = []
        stream_compression = False
        connection_metadata = 0
        for number, _, value in read_fields(data):
            if number == 1:
                info_id = 0
                fingerprint = b""
                for info_number, _, item in read_fields(_bytes(value)):
                    if info_number == 1:
                        info_id = _uint32(item)
                    elif info_number == 2:
                        fingerprint = _bytes(item)
                dictionaries.append(DictionaryInfo(info_id, fingerprint))
            elif number == 2:
                stream_compression = _int(value) != 0
            elif number == 3:
                connection_metadata = _uint32(value)
        return cls(tuple(dictionaries), stream_compression, connection_metadata)
