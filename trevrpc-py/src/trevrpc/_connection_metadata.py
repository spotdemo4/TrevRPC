"""Connection metadata: request metadata a channel sets once for all its calls, such as a bearer
token, which crosses each connection in its first calls instead of in every REQUEST.

A call offers the channel's current set (`RpcRequest.connection_metadata` with its entries) until
an ANSWER on the same connection confirms that the server stored it, and refers to it by ID after
that. Calls on different streams reach the server in any order, so a reference only follows a
confirmation. Replacing the set gives it a new ID, which each connection then confirms afresh. A
call keeps the set that was current when it started, and refers to it only while it is still the
current one: the server may have dropped a replaced set.
"""

from __future__ import annotations

from dataclasses import dataclass

from ._metadata import Metadata, MetadataInput, normalize_metadata
from ._wire import ConnectionMetadata, RpcAnswer

_MAX_ID = (1 << 32) - 1


@dataclass(frozen=True, slots=True)
class ConnectionMetadataSet:
    """One version of a channel's connection metadata."""

    id: int
    metadata: Metadata


class ConnectionMetadataSource:
    """A channel's connection metadata, shared by its connections."""

    __slots__ = ("_ids", "current")

    def __init__(self, metadata: MetadataInput | None = None) -> None:
        self._ids = 0
        self.current: ConnectionMetadataSet | None = None
        self.set(metadata)

    def set(self, metadata: MetadataInput | None) -> None:
        """Replaces the set; empty metadata removes it. Raises `INVALID_ARGUMENT` for metadata that
        breaks the limits of request metadata."""
        normalized = normalize_metadata(metadata)
        if not normalized:
            self.current = None
            return
        # IDs count from 1 and wrap past 0, which means none.
        self._ids = self._ids % _MAX_ID + 1
        self.current = ConnectionMetadataSet(self._ids, normalized)


@dataclass(slots=True)
class ConnectionMetadataState:
    """What a connection's server confirmed it stored."""

    confirmed: int = 0
    """The ID of the set an ANSWER confirmed, or 0."""


class CallConnectionMetadata:
    """One call's connection metadata: the set current when the call started."""

    __slots__ = ("_offered", "_set", "_source", "_state")

    def __init__(self, source: ConnectionMetadataSource) -> None:
        self._source = source
        self._set = source.current
        self._state: ConnectionMetadataState | None = None
        self._offered = 0

    def prepare(self, state: ConnectionMetadataState) -> ConnectionMetadata | None:
        """The REQUEST's connection_metadata on the connection whose state is `state`: a reference
        once the connection confirmed the call's set while it is still current, an offer
        otherwise."""
        self._state = state
        current = self._set
        if current is None:
            return None
        if state.confirmed == current.id and self._source.current is current:
            self._offered = 0
            return ConnectionMetadata(current.id)
        self._offered = current.id
        return ConnectionMetadata(current.id, current.metadata)

    def learn(self, answer: RpcAnswer) -> None:
        """Takes the call's ANSWER: a confirmation of the offered set counts while it is still the
        channel's current one, since a late confirmation of a replaced set would make calls refer
        to a set the server may have dropped."""
        offered = self._offered
        state = self._state
        if not offered or state is None or answer.connection_metadata != offered:
            return
        current = self._source.current
        if current is not None and current.id == offered:
            state.confirmed = offered
