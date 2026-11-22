"""Native QUIC: one aioquic connection per channel, driven from an asyncio datagram endpoint.

aioquic's own asyncio streams neither pause writers nor hold back the peer, so this module drives
`QuicConnection` itself and keeps every stream's memory bounded:

- Writes wait until the stream's unsent bytes drop below `_SEND_LOW_WATER`, so a peer that stops
  reading blocks the writer at its own flow-control credit plus one frame.
- Receive credit follows what the call has read. `_CreditConnection` keeps aioquic from counting
  data it delivered as consumed until the call's frame reader takes it, so a slow reader holds the
  peer at the stream's window.
"""

from __future__ import annotations

import asyncio
import socket
import ssl
from typing import TYPE_CHECKING, Any, Literal, cast, override

from aioquic.quic import events
from aioquic.quic.configuration import QuicConfiguration
from aioquic.quic.connection import QuicConnection
from aioquic.quic.exceptions import ConnectionStateError, DatagramFrameCapacityError

from ._compression import ConnectionCompression
from ._datagram import MAX_DATAGRAM_FRAME_SIZE, SEND_QUEUE, DatagramReceiver
from ._frames import MAX_FRAME_HEADER_SIZE
from ._status import Code, TrevRpcError
from ._stream import CANCEL_CODE, Stream
from ._varint import decode_varint

if TYPE_CHECKING:
    from aioquic.quic.packet_builder import QuicPacketBuilder
    from aioquic.quic.recovery import QuicPacketSpace
    from aioquic.quic.stream import QuicStream

ALPN = "trevrpc/1"
"""The ALPN of native TrevRPC over QUIC, wire version 1."""

_SEND_LOW_WATER = 64 * 1024
"""A write returns once at most this many of the stream's bytes are still unsent."""

_CONNECTION_WINDOW = 16 * 1024 * 1024
"""The initial connection receive window, which aioquic grows while data is read quickly."""


class _CreditConnection(QuicConnection):
    """A `QuicConnection` whose receive credit follows what the application has read.

    aioquic counts stream data as consumed once it hands it over in a `StreamDataReceived`
    event, and grows windows from that. The bytes a stream holds unread are subtracted from the
    connection's consumed count, and from the stream's delivered offset while its limits are
    written, until `release` gives them back. Stream windows never grow past their initial size,
    which `stream_window` sets to one frame and its header, the most a call buffers.

    This relies on aioquic 1.9's private flow-control state (`_local_max_data_consumed`,
    `_write_stream_limits` and the receiver's `_buffer_start`), which the version bounds in
    pyproject.toml pin, and `test_quic` checks.
    """

    def __init__(self, *, configuration: QuicConfiguration, stream_window: int) -> None:
        super().__init__(configuration=configuration)
        self._held: dict[int, int] = {}
        self._half_window = stream_window // 2

    def hold(self, stream_id: int, size: int) -> None:
        """Counts `size` delivered bytes of a stream as not yet read."""
        self._held[stream_id] = self._held.get(stream_id, 0) + size
        self._local_max_data_consumed -= size

    def release(self, stream_id: int, size: int) -> None:
        """Counts `size` held bytes of a stream as read."""
        held = self._held.get(stream_id, 0) - size
        if held:
            self._held[stream_id] = held
        else:
            self._held.pop(stream_id, None)
        self._local_max_data_consumed += size

    def forget(self, stream_id: int) -> None:
        """Counts every held byte of a stream as read, for a stream nobody will read again."""
        self._local_max_data_consumed += self._held.pop(stream_id, 0)

    def unsent(self, stream_id: int) -> int:
        """The stream's bytes written but not sent yet, or 0 for a stream aioquic forgot."""
        stream = self._streams.get(stream_id)
        if stream is None:
            return 0
        sender = stream.sender
        return sender._buffer_stop - sender.highest_offset  # pyright: ignore[reportPrivateUsage]

    @override
    def _write_stream_limits(
        self, builder: QuicPacketBuilder, space: QuicPacketSpace, stream: QuicStream
    ) -> None:
        # aioquic doubles the window from this when it raises the limit, so it never passes the
        # initial window.
        stream.max_stream_data_local_window = min(
            stream.max_stream_data_local_window, self._half_window
        )
        held = self._held.get(stream.stream_id, 0) if stream.stream_id is not None else 0
        if not held:
            super()._write_stream_limits(builder, space, stream)
            return
        receiver = stream.receiver
        receiver._buffer_start -= held  # pyright: ignore[reportPrivateUsage, reportUnknownMemberType]
        try:
            super()._write_stream_limits(builder, space, stream)
        finally:
            receiver._buffer_start += held  # pyright: ignore[reportPrivateUsage, reportUnknownMemberType]


class QuicTransport(asyncio.DatagramProtocol):
    """A client QUIC connection and its streams."""

    kind: Literal["quic"] = "quic"

    def __init__(self, configuration: QuicConfiguration, *, max_frame_size: int) -> None:
        loop = asyncio.get_running_loop()
        self._loop = loop
        self.max_frame_size = max_frame_size
        self._quic = _CreditConnection(
            configuration=configuration, stream_window=configuration.max_stream_data
        )
        self._transport: asyncio.DatagramTransport | None = None
        self._streams: dict[int, Stream] = {}
        self._handshake: asyncio.Future[None] = loop.create_future()
        self._timer: asyncio.TimerHandle | None = None
        self._timer_at: float | None = None
        self._transmit_scheduled = False
        self._blocked: set[Stream] = set()
        self._closed_locally = False
        self.compression = ConnectionCompression()
        self._flows: dict[int, DatagramReceiver] = {}
        self._next_flow = 1
        self.datagrams_received = 0
        """DATAGRAM frames the connection received, for diagnostics."""
        self.error: TrevRpcError | None = None
        """Why the connection ended, once it has."""
        self.closed: asyncio.Future[None] = loop.create_future()

    @classmethod
    async def connect(
        cls,
        host: str,
        port: int,
        *,
        server_name: str,
        cafile: str | None,
        cadata: bytes | None,
        verify: bool,
        max_frame_size: int,
        idle_timeout: float,
    ) -> QuicTransport:
        """Opens a connection and waits for its handshake."""
        loop = asyncio.get_running_loop()
        configuration = QuicConfiguration(
            is_client=True,
            alpn_protocols=[ALPN],
            server_name=server_name,
            max_stream_data=max_frame_size + MAX_FRAME_HEADER_SIZE,
            max_data=max(_CONNECTION_WINDOW, max_frame_size + MAX_FRAME_HEADER_SIZE),
            idle_timeout=idle_timeout,
            max_datagram_frame_size=MAX_DATAGRAM_FRAME_SIZE,
        )
        if verify:
            configuration.verify_mode = ssl.CERT_REQUIRED
            if cafile is not None or cadata is not None:
                configuration.load_verify_locations(cafile=cafile, cadata=cadata)
        else:
            configuration.verify_mode = ssl.CERT_NONE
        try:
            infos = await loop.getaddrinfo(host, port, type=socket.SOCK_DGRAM)
        except OSError as error:
            raise TrevRpcError(
                Code.UNAVAILABLE, f"transport unavailable: resolving {host}: {error}"
            ) from error
        if not infos:
            raise TrevRpcError(Code.UNAVAILABLE, f"transport unavailable: no address for {host}")
        family, _, _, _, sockaddr = infos[0]
        address = cast("tuple[str, int]", sockaddr[:2])
        transport, protocol = await loop.create_datagram_endpoint(
            lambda: cls(configuration, max_frame_size=max_frame_size),
            remote_addr=address,
            family=family,
        )
        del transport
        try:
            await protocol.handshake(address)
        except BaseException:
            protocol.close()
            raise
        return protocol

    async def handshake(self, address: Any) -> None:
        self._quic.connect(address, now=self._loop.time())
        self._transmit()
        await asyncio.shield(self._handshake)

    # asyncio.DatagramProtocol

    @override
    def connection_made(self, transport: asyncio.BaseTransport) -> None:
        self._transport = cast("asyncio.DatagramTransport", transport)

    @override
    def datagram_received(self, data: bytes, addr: tuple[str | Any, int]) -> None:
        self._quic.receive_datagram(data, addr, now=self._loop.time())
        self._process_events()
        self._transmit()

    @override
    def error_received(self, exc: Exception) -> None:
        # ICMP errors, such as an unreachable port, are left to QUIC's own timeouts: a later
        # datagram may still get through.
        pass

    @override
    def connection_lost(self, exc: Exception | None) -> None:
        self._terminated(
            TrevRpcError(Code.UNAVAILABLE, f"transport unavailable: {exc or 'closed'}")
        )

    # Connection

    @property
    def is_open(self) -> bool:
        return self.error is None

    def close(self) -> None:
        """Closes the connection; its calls fail with `CANCELLED`."""
        if self.error is not None:
            return
        self._closed_locally = True
        self._quic.close(reason_phrase="closed")
        self._transmit()
        # The CONNECTION_CLOSE is sent, so the calls are over whether or not it arrives.
        self._terminated(TrevRpcError(Code.CANCELLED, "transport closed locally"))

    async def open_stream(self, max_frame_size: int) -> Stream:
        """Opens a bidirectional stream; it reaches the peer with its first write. aioquic holds
        streams past the peer's stream limit until it grants more."""
        if self.error is not None:
            raise self.error
        stream_id = self._quic.get_next_available_stream_id()
        stream = Stream(self, stream_id, max_frame_size)
        self._streams[stream_id] = stream
        return stream

    # Datagrams

    def open_flow(self, receiver: DatagramReceiver) -> int | None:
        """Registers a datagram flow, when the server accepts datagrams. IDs count up from 1 and
        are never reused on the connection."""
        if self.error is not None or self._quic.get_datagram_frame_capacity() is None:
            return None
        flow_id = self._next_flow
        self._next_flow += 1
        self._flows[flow_id] = receiver
        return flow_id

    def close_flow(self, flow_id: int) -> None:
        self._flows.pop(flow_id, None)

    def send_datagram(self, data: bytes) -> bool:
        if self.error is not None:
            return False
        if len(self._quic._datagrams_pending) >= SEND_QUEUE:  # pyright: ignore[reportPrivateUsage]
            return False
        try:
            self._quic.send_datagram_frame(data)
        except DatagramFrameCapacityError, ConnectionStateError:
            return False
        self.transmit_soon()
        return True

    def _process_events(self) -> None:
        quic = self._quic
        while (event := quic.next_event()) is not None:
            match event:
                case events.StreamDataReceived(stream_id=stream_id, data=data, end_stream=end):
                    stream = self._streams.get(stream_id)
                    if stream is None:
                        # A stream this side gave up on: its data is never read.
                        continue
                    if data:
                        quic.hold(stream_id, len(data))
                    stream.on_data(data, end)
                case events.StreamReset(stream_id=stream_id, error_code=code):
                    stream = self._streams.get(stream_id)
                    if stream is not None:
                        stream.on_reset(code)
                case events.StopSendingReceived(stream_id=stream_id, error_code=code):
                    stream = self._streams.get(stream_id)
                    if stream is not None:
                        stream.on_stop_sending(code)
                case events.DatagramFrameReceived(data=data):
                    self.datagrams_received += 1
                    decoded = decode_varint(data, 0)
                    if decoded is not None and decoded[0] != 0:
                        receiver = self._flows.get(decoded[0])
                        if receiver is not None:
                            receiver.on_datagram(data[decoded[1] :])
                case events.HandshakeCompleted():
                    if not self._handshake.done():
                        self._handshake.set_result(None)
                case events.ConnectionTerminated(error_code=code, reason_phrase=reason):
                    if self._closed_locally:
                        error = TrevRpcError(Code.CANCELLED, "transport closed locally")
                    else:
                        detail = reason or f"error code {code}"
                        error = TrevRpcError(
                            Code.UNAVAILABLE, f"transport unavailable: connection closed: {detail}"
                        )
                    self._terminated(error)
                case _:
                    pass

    def _terminated(self, error: TrevRpcError) -> None:
        if self.error is not None:
            return
        self.error = error
        if not self._handshake.done():
            self._handshake.set_exception(error)
            # Retrieved by connect(), which may itself have been cancelled.
            self._handshake.exception()
        if self._timer is not None:
            self._timer.cancel()
            self._timer = None
        streams = list(self._streams.values())
        self._streams.clear()
        self._blocked.clear()
        self._flows.clear()
        for stream in streams:
            stream.on_connection_error(error)
        if self._transport is not None:
            self._transport.close()
        if not self.closed.done():
            self.closed.set_result(None)

    def transmit_soon(self) -> None:
        """Sends what the connection has queued once the current callbacks are done, so that
        writes made together share packets."""
        if not self._transmit_scheduled and self.error is None:
            self._transmit_scheduled = True
            self._loop.call_soon(self._transmit)

    def _transmit(self) -> None:
        self._transmit_scheduled = False
        transport = self._transport
        if transport is None or self.error is not None:
            return
        now = self._loop.time()
        for data, _ in self._quic.datagrams_to_send(now=now):
            transport.sendto(data)
        if self._blocked:
            for stream in list(self._blocked):
                if self._quic.unsent(stream.id) <= _SEND_LOW_WATER:
                    self._blocked.discard(stream)
                    stream.on_drained()
        self._schedule_timer()

    def _schedule_timer(self) -> None:
        timer_at = self._quic.get_timer()
        if timer_at == self._timer_at and self._timer is not None:
            return
        if self._timer is not None:
            self._timer.cancel()
            self._timer = None
        self._timer_at = timer_at
        if timer_at is not None:
            self._timer = self._loop.call_at(timer_at, self._on_timer)

    def _on_timer(self) -> None:
        self._timer = None
        self._timer_at = None
        self._quic.handle_timer(now=self._loop.time())
        self._process_events()
        self._transmit()

    # Called by streams

    def write(self, stream: Stream, data: bytes, end: bool) -> bool:
        """Queues data on a stream; returns whether the stream has room for more."""
        self._quic.send_stream_data(stream.id, data, end_stream=end)
        self.transmit_soon()
        if self._quic.unsent(stream.id) <= _SEND_LOW_WATER:
            return True
        self._blocked.add(stream)
        return False

    def release(self, stream: Stream, size: int) -> None:
        self._quic.release(stream.id, size)
        self.transmit_soon()

    def reset_send(self, stream: Stream) -> None:
        """Resets the stream's send side with the cancel code."""
        if stream.id not in self._streams or self.error is not None:
            return
        self._blocked.discard(stream)
        self._quic.reset_stream(stream.id, CANCEL_CODE)
        self.transmit_soon()

    def abort(self, stream: Stream, *, reset: bool, stop: bool) -> None:
        """Resets the stream's send side and stops its receive side with the cancel code, and
        forgets it."""
        if self._streams.pop(stream.id, None) is None or self.error is not None:
            return
        self._blocked.discard(stream)
        if reset:
            self._quic.reset_stream(stream.id, CANCEL_CODE)
        if stop:
            self._quic.stop_stream(stream.id, CANCEL_CODE)
        self._quic.forget(stream.id)
        self.transmit_soon()

    def finished(self, stream: Stream) -> None:
        """Forgets a stream whose both sides ended."""
        self._streams.pop(stream.id, None)
        self._blocked.discard(stream)
        self._quic.forget(stream.id)
