"""Metadata: maps from lowercase ASCII keys to byte values, with the protocol's limits."""

from __future__ import annotations

from collections.abc import Mapping

from ._status import Code, TrevRpcError

type Metadata = dict[str, bytes]
"""Metadata as the runtime hands it out: keys to their byte values."""

type MetadataInput = Mapping[str, str | bytes | bytearray | memoryview]
"""Metadata as callers pass it: string values are encoded as UTF-8, and keys are lowercased."""

MAX_ENTRIES = 64
MAX_KEY_LEN = 128
MAX_VALUE_LEN = 8 * 1024
MAX_TOTAL_SIZE = 64 * 1024
RESERVED_PREFIX = "trevrpc-"

_KEY_CHARS = frozenset("abcdefghijklmnopqrstuvwxyz0123456789._-")


def normalize_metadata(metadata: MetadataInput | None) -> Metadata:
    """Lowercases the keys, encodes string values, and validates the result.

    Raises an `INVALID_ARGUMENT` `TrevRpcError` for metadata that breaks the protocol's rules.
    """
    if not metadata:
        return {}
    normalized: Metadata = {}
    for key, value in metadata.items():
        if isinstance(value, str):
            data = value.encode()
        elif isinstance(value, bytes | bytearray | memoryview):  # pyright: ignore[reportUnnecessaryIsInstance]
            data = bytes(value)
        else:
            raise TrevRpcError(Code.INVALID_ARGUMENT, "metadata values must be bytes or strings")
        normalized[key.lower()] = data
    validate_metadata(normalized, Code.INVALID_ARGUMENT)
    return normalized


def validate_metadata(metadata: Mapping[str, bytes], code: Code) -> None:
    """Raises a `TrevRpcError` with `code` unless `metadata` keeps the protocol's rules."""
    if len(metadata) > MAX_ENTRIES:
        raise TrevRpcError(code, f"metadata has {len(metadata)} entries, maximum is {MAX_ENTRIES}")
    total = 0
    for key, value in metadata.items():
        _validate_key(key, code)
        if len(value) > MAX_VALUE_LEN:
            raise TrevRpcError(
                code,
                f"metadata value {key!r} is {len(value)} bytes, maximum is {MAX_VALUE_LEN}",
            )
        total += len(key) + len(value)
    if total > MAX_TOTAL_SIZE:
        raise TrevRpcError(code, f"metadata is {total} bytes, maximum is {MAX_TOTAL_SIZE}")


def _validate_key(key: str, code: Code) -> None:
    if not key:
        raise TrevRpcError(code, "metadata key is empty")
    if len(key) > MAX_KEY_LEN:
        raise TrevRpcError(
            code, f"metadata key {key!r} is {len(key)} bytes, maximum is {MAX_KEY_LEN}"
        )
    if key.startswith(RESERVED_PREFIX):
        raise TrevRpcError(code, f"metadata key {key!r} uses reserved prefix {RESERVED_PREFIX!r}")
    if not _KEY_CHARS.issuperset(key):
        raise TrevRpcError(
            code,
            f"metadata key {key!r} must use lowercase ASCII letters, digits, '.', '_' or '-'",
        )
