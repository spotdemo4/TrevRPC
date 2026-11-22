"""Status codes and the error that carries a call's non-OK status."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from enum import IntEnum
from typing import TYPE_CHECKING, Protocol, Self

if TYPE_CHECKING:
    from ._metadata import Metadata


class Code(IntEnum):
    """Canonical status codes, numbered as gRPC's."""

    OK = 0
    CANCELLED = 1
    UNKNOWN = 2
    INVALID_ARGUMENT = 3
    DEADLINE_EXCEEDED = 4
    NOT_FOUND = 5
    ALREADY_EXISTS = 6
    PERMISSION_DENIED = 7
    RESOURCE_EXHAUSTED = 8
    FAILED_PRECONDITION = 9
    ABORTED = 10
    OUT_OF_RANGE = 11
    UNIMPLEMENTED = 12
    INTERNAL = 13
    UNAVAILABLE = 14
    DATA_LOSS = 15
    UNAUTHENTICATED = 16

    @classmethod
    def of(cls, value: int) -> Code:
        """The code numbered `value`; unknown numbers are `UNKNOWN`."""
        try:
            return cls(value)
        except ValueError:
            return cls.UNKNOWN


@dataclass(frozen=True, slots=True)
class StatusDetail:
    """A status detail: an encoded protobuf message and its full name, such as
    `buf.validate.Violations`."""

    type: str
    value: bytes


class _Decodable[T](Protocol):
    def from_binary(self, data: bytes, /) -> T: ...

    def desc(self) -> _Named: ...


class _Named(Protocol):
    @property
    def type_name(self) -> str: ...


RETRY_INFO = "google.rpc.RetryInfo"


class TrevRpcError(Exception):
    """A call's non-OK status: its code, message, trailing metadata and details."""

    code: Code
    message: str
    metadata: Metadata
    details: tuple[StatusDetail, ...]

    def __init__(
        self,
        code: int,
        message: str = "",
        metadata: Mapping[str, bytes] | None = None,
        details: Sequence[StatusDetail] = (),
    ) -> None:
        self.code = Code.of(code)
        self.message = message
        self.metadata = dict(metadata or {})
        self.details = tuple(details)
        super().__init__(f"{self.code.name}: {message}" if message else self.code.name)

    @property
    def retry_delay(self) -> float | None:
        """How long the server told the client to wait before it retries, in seconds, from the
        status's `google.rpc.RetryInfo` detail; `None` when there is none. A detail that does not
        decode, or holds a negative or invalid `google.protobuf.Duration`, is ignored."""
        for detail in self.details:
            if detail.type == RETRY_INFO:
                delay = _retry_delay(detail.value)
                if delay is not None:
                    return delay
        return None

    def find_details[T](self, message_type: _Decodable[T]) -> list[T]:
        """Decodes the details whose type is the full name of `message_type`, a protobuf-py
        message class such as `Violations` for `buf.validate.Violations`."""
        name = message_type.desc().type_name
        return [message_type.from_binary(d.value) for d in self.details if d.type == name]

    @classmethod
    def wrap(cls, error: BaseException) -> Self | TrevRpcError:
        """`error` itself if it is a status error, otherwise an `INTERNAL` one describing it."""
        if isinstance(error, TrevRpcError):
            return error
        return TrevRpcError(Code.INTERNAL, str(error) or type(error).__name__)


_MAX_DURATION_SECONDS = 315_576_000_000


def _retry_delay(data: bytes) -> float | None:
    """The `retry_delay` of an encoded `google.rpc.RetryInfo`, in seconds."""
    from ._wire import WireError, read_fields

    try:
        duration = b""
        for number, _, value in read_fields(data):
            if number == 1 and isinstance(value, bytes):
                duration = value
        seconds = nanos = 0
        for number, wire_type, value in read_fields(duration):
            if wire_type != 0 or not isinstance(value, int):
                continue
            if number == 1:
                seconds = _int64(value)
            elif number == 2:
                nanos = _int32(value)
    except WireError:
        return None
    if not -_MAX_DURATION_SECONDS <= seconds <= _MAX_DURATION_SECONDS:
        return None
    if not -999_999_999 <= nanos <= 999_999_999 or (seconds > 0 > nanos) or (seconds < 0 < nanos):
        return None
    if seconds < 0 or nanos < 0:
        return None
    return seconds + nanos / 1e9


def _int64(value: int) -> int:
    return value - (1 << 64) if value >= 1 << 63 else value


def _int32(value: int) -> int:
    value = _int64(value)
    return value if -(1 << 31) <= value < 1 << 31 else 1 << 40
