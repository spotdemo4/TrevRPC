"""Datagram messages: the messages of a method's unreliable directions, which may travel in QUIC
datagrams instead of MESSAGE frames, on native QUIC where the server accepts datagrams.

A call gets a datagram flow when its schema marks a streamed direction unreliable and its
connection supports datagrams. The flow's ID is the first thing in each of its datagrams, then
TrevRPC frames to the end of the datagram. The client sends a request message as a datagram when
it fits and the connection takes it, and on the stream otherwise; it accepts the server's datagram
messages on a call whose responses stream. A direction that sent datagrams ends with
DATAGRAM_COUNT, so the receiver can wait a little for those still on their way.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import Callable
from typing import Protocol

from ._frames import FrameType, encode_frame
from ._varint import decode_varint, encode_varint

MAX_DATAGRAM_FRAME_SIZE = 65535
"""The largest DATAGRAM frame the client accepts, which it advertises (RFC 9221)."""

SEND_QUEUE = 32
"""Datagrams the connection may hold unsent. A message that finds the queue full goes on the
stream, whose flow control then paces the sender."""

HELD_MESSAGES = 128
HELD_BYTES = 64 * 1024
"""The datagram messages a call holds before the caller takes them; more are dropped, since
datagrams cannot be paced."""

COUNT_WAIT = 0.01
"""How long a call whose responses ended waits for the datagrams DATAGRAM_COUNT announced."""


class DatagramConnection(Protocol):
    """What a flow needs of its connection."""

    def send_datagram(self, data: bytes) -> bool:
        """Sends a datagram; `False` when it does not fit, the queue is full, or the connection
        cannot send datagrams."""
        ...

    def close_flow(self, flow_id: int) -> None: ...


class DatagramReceiver(Protocol):
    def on_datagram(self, payload: bytes) -> None:
        """Takes a datagram of the flow, after its flow ID. It must never block."""
        ...


def datagram_frames(payload: bytes | memoryview) -> list[tuple[int, bytes]] | None:
    """The MESSAGE and ZSTD_MESSAGE frames of a datagram's payload, after its flow ID, or `None`
    for one to drop whole: frames that do not exactly fill it, or a ZSTD_STREAM_MESSAGE. Other
    frame types are skipped."""
    frames: list[tuple[int, bytes]] = []
    pos = 0
    end = len(payload)
    while pos < end:
        decoded = decode_varint(payload, pos)
        if decoded is None:
            return None
        frame_type, pos = decoded
        decoded = decode_varint(payload, pos)
        if decoded is None or decoded[0] > end - decoded[1]:
            return None
        length, pos = decoded
        if frame_type == FrameType.ZSTD_STREAM_MESSAGE:
            return None
        if frame_type in (FrameType.MESSAGE, FrameType.ZSTD_MESSAGE):
            frames.append((frame_type, bytes(payload[pos : pos + length])))
        pos += length
    return frames


class DatagramFlow:
    """One call's datagram flow.

    Messages that arrive before the call has read the response's HEADERS (or its first other
    frame) are held, within `HELD_MESSAGES` and `HELD_BYTES`, then delivered in arrival order. Every
    datagram of the flow counts toward DATAGRAM_COUNT, including those dropped whole.
    """

    __slots__ = (
        "_closed",
        "_connection",
        "_decode",
        "_deliver",
        "_held",
        "_held_bytes",
        "_open",
        "_waiter",
        "id",
        "received",
        "receiving",
        "sending",
        "sent",
    )

    def __init__(
        self,
        connection: DatagramConnection,
        *,
        send: bool,
        receive: bool,
        decode: Callable[[int, bytes], bytes],
        deliver: Callable[[list[bytes]], None],
    ) -> None:
        self._connection = connection
        self.id = 0
        """Set once the connection registered the flow."""
        self.sending = send
        """Whether the call sends its requests as datagrams: its schema marks them unreliable,
        and the request stream has not ended."""
        self.receiving = receive
        """Whether the responses stream, so that their datagram messages are accepted."""
        self._decode = decode
        """A message frame's payload, decompressed; raises for one to drop."""
        self._deliver = deliver
        self.sent = 0
        """Datagrams the connection accepted, which DATAGRAM_COUNT reports."""
        self.received = 0
        self._open = False
        self._closed = False
        self._held: list[bytes] = []
        self._held_bytes = 0
        self._waiter: asyncio.Future[None] | None = None

    # Requests

    def send(self, frame: bytes) -> bool:
        """Sends a message frame as a datagram; `False` when it must go on the stream instead."""
        if not self.sending:
            return False
        if not self._connection.send_datagram(encode_varint(self.id) + frame):
            return False
        self.sent += 1
        return True

    def end_send(self) -> bytes:
        """Ends the request direction normally: the DATAGRAM_COUNT to write before its end, if any
        datagram was sent."""
        self.sending = False
        if not self.sent:
            return b""
        return encode_frame(FrameType.DATAGRAM_COUNT, encode_varint(self.sent))

    # Responses

    def on_datagram(self, payload: bytes) -> None:
        if not self.receiving or self._closed:
            return
        self.received += 1
        self._wake()
        frames = datagram_frames(payload)
        if frames is None:
            return
        try:
            messages = [self._decode(frame_type, data) for frame_type, data in frames]
        except Exception:
            # A message that cannot be decompressed drops its datagram whole.
            return
        if self._open:
            self._deliver(messages)
            return
        for message in messages:
            if self._held and (
                len(self._held) >= HELD_MESSAGES or self._held_bytes + len(message) > HELD_BYTES
            ):
                return
            self._held.append(message)
            self._held_bytes += len(message)

    def open(self) -> None:
        """The call read the response's HEADERS, or its first other frame: held messages are
        delivered, and later ones as they arrive."""
        if self._open:
            return
        self._open = True
        held, self._held, self._held_bytes = self._held, [], 0
        if held:
            self._deliver(held)

    async def await_count(self, count: int) -> None:
        """Waits until `count` datagrams have arrived, at most `COUNT_WAIT`, or the flow closes."""
        if self.received >= count or self._closed:
            return
        self._waiter = asyncio.get_running_loop().create_future()
        try:
            with contextlib.suppress(TimeoutError):
                async with asyncio.timeout(COUNT_WAIT):
                    while self.received < count and not self._closed:
                        await self._waiter
                        self._waiter = asyncio.get_running_loop().create_future()
        finally:
            self._waiter = None

    def _wake(self) -> None:
        if self._waiter is not None and not self._waiter.done():
            self._waiter.set_result(None)

    def close(self) -> None:
        """Ends the flow: no more datagrams are sent, and later ones are dropped."""
        if self._closed:
            return
        self._closed = True
        self.sending = False
        self._held, self._held_bytes = [], 0
        self._connection.close_flow(self.id)
        self._wake()
