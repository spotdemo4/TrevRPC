"""The TrevRPC side of a transport stream: TrevRPC frames read from it, writes paced by it, and the
cancel semantics of the protocol, whatever the transport (native QUIC or QMux)."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Protocol

from ._frames import FrameReader
from ._status import Code, TrevRpcError

if TYPE_CHECKING:
    from ._compression import ConnectionCompression

CANCEL_CODE = 1
"""The stream error code of a cancelled call."""


class StreamTransport(Protocol):
    """What a `Stream` needs of the connection it belongs to. The connection feeds the stream
    with `on_data`, `on_reset`, `on_stop_sending`, `on_connection_error` and `on_drained`."""

    compression: ConnectionCompression
    """What the connection has negotiated for compressed messages."""

    def write(self, stream: Stream, data: bytes, end: bool) -> bool:
        """Queues data, and the end of the stream with `end`; returns whether the stream has
        room for more, or else calls `on_drained` once it has."""
        ...

    def release(self, stream: Stream, size: int) -> None:
        """Gives `size` received bytes the stream has read back to the peer as credit."""
        ...

    def reset_send(self, stream: Stream) -> None:
        """Resets the send side with the cancel code."""
        ...

    def abort(self, stream: Stream, *, reset: bool, stop: bool) -> None:
        """Resets the send side and stops the receive side with the cancel code, and forgets the
        stream: its unread bytes count as read."""
        ...

    def finished(self, stream: Stream) -> None:
        """Forgets a stream whose both sides ended; its unread bytes count as read."""
        ...


class Stream:
    """One bidirectional transport stream, read as TrevRPC frames."""

    __slots__ = (
        "_connection",
        "_detached",
        "_drained",
        "_error",
        "_fin",
        "_read_error",
        "_read_finished",
        "_readable",
        "_reader",
        "_send_done",
        "_write_error",
        "id",
    )

    def __init__(self, connection: StreamTransport, stream_id: int, max_frame_size: int) -> None:
        self._connection = connection
        self.id = stream_id
        self._reader = FrameReader(max_frame_size)
        self._fin = False
        self._send_done = False
        self._error: TrevRpcError | None = None
        self._read_error: TrevRpcError | None = None
        self._read_finished = False
        self._write_error: TrevRpcError | None = None
        self._readable: asyncio.Future[None] | None = None
        self._drained: asyncio.Future[None] | None = None
        self._detached = False

    # Reading

    async def read_frame(self) -> tuple[int, bytes] | None:
        """Returns the next known frame, or `None` at the end of the stream."""
        reader = self._reader
        while True:
            if self._error is not None:
                raise self._error
            before = reader.consumed
            try:
                frame = reader.next()
            except TrevRpcError as error:
                self.fail(error)
                raise
            read = reader.consumed - before
            if read:
                self._connection.release(self, read)
            if frame is not None:
                return frame
            if self._read_error is not None:
                self._read_finished = True
                self._maybe_finished()
                raise self._read_error
            if self._fin:
                if reader.buffered:
                    error = TrevRpcError(Code.INTERNAL, "stream ended in the middle of a frame")
                    self.fail(error)
                    raise error
                self._read_finished = True
                self._maybe_finished()
                return None
            self._readable = asyncio.get_running_loop().create_future()
            try:
                await self._readable
            finally:
                self._readable = None

    def on_data(self, data: bytes, end: bool) -> None:
        if data:
            self._reader.feed(data)
        if end:
            self._fin = True
        if self._detached:
            self._drain_detached()
        else:
            self._wake_reader()

    def on_reset(self, code: int) -> None:
        self._read_error = TrevRpcError(
            Code.CANCELLED, f"stream {self.id} reset by peer with error code {code}"
        )
        if self._detached:
            self._drain_detached()
        else:
            self._wake_reader()

    def detach(self) -> None:
        """Stops reading for a caller, once the response's STATUS arrived: what follows is read as
        it arrives, a known frame aborts the stream, and the stream is forgotten at its end."""
        if self._read_finished or self._error is not None:
            return
        self._detached = True
        self._drain_detached()

    def _drain_detached(self) -> None:
        reader = self._reader
        before = reader.consumed
        try:
            frame = reader.next()
        except TrevRpcError as error:
            self.fail(error)
            return
        read = reader.consumed - before
        if read:
            self._connection.release(self, read)
        if frame is not None:
            self.fail(TrevRpcError(Code.INTERNAL, f"frame of type {frame[0]} after STATUS"))
        elif self._read_error is not None or (self._fin and not reader.buffered):
            self._read_finished = True
            self._maybe_finished()
        elif self._fin:
            self.fail(TrevRpcError(Code.INTERNAL, "stream ended in the middle of a frame"))

    def on_stop_sending(self, code: int) -> None:
        self._write_error = TrevRpcError(
            Code.CANCELLED, f"stream {self.id} stopped by peer with error code {code}"
        )
        self._send_done = True
        self._wake_writer()
        self._maybe_finished()

    def on_connection_error(self, error: TrevRpcError) -> None:
        if self._error is None:
            self._error = error
        self._wake_reader()
        self._wake_writer()

    def _wake_reader(self) -> None:
        if self._readable is not None and not self._readable.done():
            self._readable.set_result(None)

    # Writing

    async def write(self, data: bytes, *, end: bool = False) -> None:
        """Writes `data`, and the end of the stream with `end`, and waits until most of it is sent,
        which the peer's flow control and the network pace."""
        if self._error is not None:
            raise self._error
        if self._write_error is not None:
            raise self._write_error
        if self._send_done:
            raise TrevRpcError(Code.INTERNAL, "write after the end of the request stream")
        if end:
            self._send_done = True
        if self._connection.write(self, data, end):
            if end:
                self._maybe_finished()
            return
        self._drained = asyncio.get_running_loop().create_future()
        try:
            await self._drained
        finally:
            self._drained = None
        if self._error is not None:
            raise self._error
        if self._write_error is not None:
            raise self._write_error
        if end:
            self._maybe_finished()

    def on_drained(self) -> None:
        self._wake_writer()

    def _wake_writer(self) -> None:
        if self._drained is not None and not self._drained.done():
            self._drained.set_result(None)

    @property
    def send_done(self) -> bool:
        return self._send_done

    @property
    def compression(self) -> ConnectionCompression:
        """What the stream's connection has negotiated for compressed messages."""
        return self._connection.compression

    @property
    def stopped_by_peer(self) -> bool:
        """Whether the peer stopped the send side (STOP_SENDING) while the stream is still read."""
        return self._write_error is not None and self._error is None

    # Ending

    def fail(self, error: TrevRpcError) -> None:
        """Ends the stream with `error`: pending and later reads and writes raise it, the send side
        is reset and the receive side stopped with the cancel code."""
        if self._error is not None:
            return
        self._error = error
        self._send_done = True
        # aioquic ignores the reset once every byte sent was acknowledged.
        stop = not self._fin and self._read_error is None
        self._connection.abort(self, reset=True, stop=stop)
        self._wake_reader()
        self._wake_writer()

    def reset_send(self, error: TrevRpcError, *, abandon: bool = False) -> None:
        """Resets the send side with the cancel code and keeps reading; a pending write raises
        `error`. A send side whose end was written is left alone, unless `abandon` says to drop
        what of it is still unsent; the connection leaves one that sent everything alone."""
        if self._error is not None or self._write_error is not None:
            return
        self._write_error = error
        if not self._send_done or abandon:
            self._send_done = True
            self._connection.reset_send(self)
        self._wake_writer()
        self._maybe_finished()

    def _maybe_finished(self) -> None:
        if self._send_done and self._read_finished:
            self._connection.finished(self)
