"""Method descriptors, which generated clients pass to the channel."""

from __future__ import annotations

from dataclasses import dataclass
from enum import IntEnum

from protobuf import Message

from ._wire import RpcKind

MethodKind = RpcKind
"""How many messages a method takes and returns: unary, client, server or bidi streaming."""


class Compression(IntEnum):
    """How a method's messages may be compressed without a dictionary, from its
    `(trevrpc.method).compression` option."""

    UNSPECIFIED = 0
    MESSAGE = 1
    STREAM = 2


@dataclass(frozen=True, slots=True)
class Method[Req: Message, Resp: Message]:
    """A method of a service: its path, kind, message types and `(trevrpc.method)` options."""

    path: str
    """`/<package>.<Service>/<Method>`."""
    kind: MethodKind
    request: type[Req]
    response: type[Resp]
    unreliable_requests: bool = False
    unreliable_responses: bool = False
    compression: Compression = Compression.UNSPECIFIED
