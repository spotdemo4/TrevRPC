"""QMux over TLS (draft-ietf-quic-qmux-02): QUIC's streams and flow control over a TCP connection,
TrevRPC's fallback for networks that block UDP.

Records carry QUIC frames behind a `Size` prefix. Each endpoint's first frame is
QX_TRANSPORT_PARAMETERS. The session keeps the same bounds as the QUIC transport: a write waits
until the stream's unsent bytes drop below `_SEND_LOW_WATER`, and receive credit follows what the
stream's reader has taken, so a slow reader holds the peer at the stream window.

Only bidirectional streams are used. A client lets the server open none; the server role exists for
tests, which serve TrevRPC over it. Datagrams are refused, so unreliable methods stay on the stream.
"""

from __future__ import annotations

import asyncio
import contextlib
import ssl
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from enum import IntEnum
from typing import Literal

from ._compression import ConnectionCompression
from ._frames import MAX_FRAME_HEADER_SIZE
from ._status import Code, TrevRpcError
from ._stream import CANCEL_CODE, Stream
from ._varint import MAX_VARINT, decode_varint, encode_varint, varint_len

QMUX_ALPN = "qmux-02.trevrpc-1"
"""The TLS ALPN of native TrevRPC over QMux over TLS, wire version 1."""

TRANSPORT_PARAMETERS = 0x3F5153300D0A0D0A
PING = 0x348C67529EF8C7BD
PING_RESPONSE = 0x348C67529EF8C7BE
MAX_RECORD_SIZE_PARAMETER = 0x0571C59429CD0845

DEFAULT_MAX_RECORD_SIZE = 16382
"""The max_record_size a peer may assume until the transport parameters say otherwise, and the
smallest one a peer may advertise."""

RECORD_SIZE = 64 * 1024
"""The largest record TrevRPC endpoints accept and send."""

PADDING = 0x00
RESET_STREAM = 0x04
STOP_SENDING = 0x05
STREAM = 0x08  # through 0x0f
MAX_DATA = 0x10
MAX_STREAM_DATA = 0x11
MAX_STREAMS_BIDI = 0x12
MAX_STREAMS_UNI = 0x13
DATA_BLOCKED = 0x14
STREAM_DATA_BLOCKED = 0x15
STREAMS_BLOCKED_BIDI = 0x16
STREAMS_BLOCKED_UNI = 0x17
CONNECTION_CLOSE = 0x1C
APPLICATION_CLOSE = 0x1D
DATAGRAM = 0x30
DATAGRAM_LEN = 0x31

_FIN = 0x01
_LEN = 0x02
_OFF = 0x04
_MAX_STREAMS = 1 << 60
_MAX_REASON = 256

_SEND_LOW_WATER = 64 * 1024
"""A write returns once at most this many of the stream's bytes are still unsent."""

_STREAM_QUANTUM = 16 * 1024
"""How much of one stream a record carries before the next stream with data gets a turn."""

_CLOSE_GRACE = 1.0
"""How long a closing session may take to send its CONNECTION_CLOSE before the connection is
aborted, as when the peer stopped reading and the send buffer is full."""


class ErrorCode(IntEnum):
    """QUIC transport error codes (RFC 9000 section 20.1) that QMux uses."""

    NO_ERROR = 0x00
    INTERNAL_ERROR = 0x01
    FLOW_CONTROL_ERROR = 0x03
    STREAM_LIMIT_ERROR = 0x04
    STREAM_STATE_ERROR = 0x05
    FINAL_SIZE_ERROR = 0x06
    FRAME_ENCODING_ERROR = 0x07
    TRANSPORT_PARAMETER_ERROR = 0x08
    PROTOCOL_VIOLATION = 0x0A


class QMuxError(Exception):
    """A QMux protocol error, which closes the connection with a transport CONNECTION_CLOSE."""

    def __init__(self, code: ErrorCode, frame_type: int, reason: str) -> None:
        super().__init__(f"{code.name}: {reason}")
        self.code = code
        self.frame_type = frame_type
        self.reason = reason


# Frames


def _varints(*values: int) -> bytes:
    return b"".join(encode_varint(value) for value in values)


def stream_frame(stream_id: int, offset: int, data: bytes | memoryview, fin: bool) -> bytes:
    """A STREAM frame. It always carries an explicit length, so more frames may follow."""
    frame_type = STREAM | _LEN | (_OFF if offset else 0) | (_FIN if fin else 0)
    header = (
        _varints(frame_type, stream_id, offset, len(data))
        if offset
        else (_varints(frame_type, stream_id, len(data)))
    )
    return header + data


def reset_stream_frame(stream_id: int, code: int, final_size: int) -> bytes:
    return _varints(RESET_STREAM, stream_id, code, final_size)


def stop_sending_frame(stream_id: int, code: int) -> bytes:
    return _varints(STOP_SENDING, stream_id, code)


def max_data_frame(maximum: int) -> bytes:
    return _varints(MAX_DATA, maximum)


def max_stream_data_frame(stream_id: int, maximum: int) -> bytes:
    return _varints(MAX_STREAM_DATA, stream_id, maximum)


def max_streams_bidi_frame(maximum: int) -> bytes:
    return _varints(MAX_STREAMS_BIDI, maximum)


def application_close_frame(code: int, reason: str) -> bytes:
    data = reason.encode()[:_MAX_REASON]
    return _varints(APPLICATION_CLOSE, code, len(data)) + data


def transport_close_frame(code: int, frame_type: int, reason: str) -> bytes:
    data = reason.encode()[:_MAX_REASON]
    return _varints(CONNECTION_CLOSE, code, frame_type, len(data)) + data


def ping_frame(frame_type: int, sequence: int) -> bytes:
    return _varints(frame_type, sequence)


def record(frames: bytes) -> bytes:
    """A record of QMux over TLS: its `Size`, then its frames."""
    return encode_varint(len(frames)) + frames


@dataclass(slots=True)
class Frame:
    """One decoded frame; only the fields of its type are set."""

    type: int
    stream_id: int = 0
    offset: int = 0
    data: bytes = b""
    fin: bool = False
    code: int = 0
    final_size: int = 0
    maximum: int = 0
    frame_type: int = 0
    reason: bytes = b""
    sequence: int = 0


def parse_frames(data: bytes) -> Iterator[Frame]:
    """Decodes the frames of one record. Frames QMux forbids, and extensions it does not implement,
    are FRAME_ENCODING_ERRORs, as are truncated ones."""
    pos = 0
    end = len(data)

    def varint(frame_type: int) -> int:
        nonlocal pos
        decoded = decode_varint(data, pos)
        if decoded is None:
            raise QMuxError(ErrorCode.FRAME_ENCODING_ERROR, frame_type, "truncated frame")
        value, pos = decoded
        return value

    def take(frame_type: int, length: int) -> bytes:
        nonlocal pos
        if length > end - pos:
            raise QMuxError(ErrorCode.FRAME_ENCODING_ERROR, frame_type, "truncated frame")
        chunk = data[pos : pos + length]
        pos += length
        return chunk

    while pos < end:
        frame_type = varint(0)
        frame = Frame(frame_type)
        if frame_type == PADDING:
            pass
        elif STREAM <= frame_type <= STREAM | 0x07:
            frame.stream_id = varint(frame_type)
            if frame_type & _OFF:
                frame.offset = varint(frame_type)
            length = varint(frame_type) if frame_type & _LEN else end - pos
            frame.data = take(frame_type, length)
            frame.fin = bool(frame_type & _FIN)
            if frame.offset + len(frame.data) > MAX_VARINT:
                raise QMuxError(
                    ErrorCode.FRAME_ENCODING_ERROR, frame_type, "stream data beyond the maximum"
                )
        elif frame_type == RESET_STREAM:
            frame.stream_id = varint(frame_type)
            frame.code = varint(frame_type)
            frame.final_size = varint(frame_type)
        elif frame_type == STOP_SENDING:
            frame.stream_id = varint(frame_type)
            frame.code = varint(frame_type)
        elif frame_type in (
            MAX_DATA,
            DATA_BLOCKED,
            MAX_STREAMS_BIDI,
            MAX_STREAMS_UNI,
            STREAMS_BLOCKED_BIDI,
            STREAMS_BLOCKED_UNI,
        ):
            frame.maximum = varint(frame_type)
        elif frame_type in (MAX_STREAM_DATA, STREAM_DATA_BLOCKED):
            frame.stream_id = varint(frame_type)
            frame.maximum = varint(frame_type)
        elif frame_type in (CONNECTION_CLOSE, APPLICATION_CLOSE):
            frame.code = varint(frame_type)
            if frame_type == CONNECTION_CLOSE:
                frame.frame_type = varint(frame_type)
            frame.reason = take(frame_type, varint(frame_type))
        elif frame_type in (DATAGRAM, DATAGRAM_LEN):
            length = varint(frame_type) if frame_type == DATAGRAM_LEN else end - pos
            frame.data = take(frame_type, length)
        elif frame_type == TRANSPORT_PARAMETERS:
            frame.data = take(frame_type, varint(frame_type))
        elif frame_type in (PING, PING_RESPONSE):
            frame.sequence = varint(frame_type)
        else:
            raise QMuxError(
                ErrorCode.FRAME_ENCODING_ERROR,
                frame_type,
                f"frame type 0x{frame_type:x} is not allowed in QMux",
            )
        yield frame


# Transport parameters

_IDLE_TIMEOUT = 0x01
_INITIAL_MAX_DATA = 0x04
_INITIAL_MAX_STREAM_DATA_BIDI_LOCAL = 0x05
_INITIAL_MAX_STREAM_DATA_BIDI_REMOTE = 0x06
_INITIAL_MAX_STREAM_DATA_UNI = 0x07
_INITIAL_MAX_STREAMS_BIDI = 0x08
_INITIAL_MAX_STREAMS_UNI = 0x09
_MAX_DATAGRAM_FRAME_SIZE = 0x20
# Parameters that only make sense over QUIC packets (RFC 9000 section 18.2).
_FORBIDDEN_PARAMETERS = frozenset((0x00, 0x02, 0x03, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F, 0x10))


@dataclass(slots=True)
class Parameters:
    """The transport parameters of an endpoint's QX_TRANSPORT_PARAMETERS frame. Zero fields are
    left out of the frame, and take the protocol default: no credit, no streams, no datagrams."""

    max_idle_timeout: int = 0
    """In milliseconds; zero for none."""
    initial_max_data: int = 0
    initial_max_stream_data_bidi_local: int = 0
    initial_max_stream_data_bidi_remote: int = 0
    initial_max_stream_data_uni: int = 0
    initial_max_streams_bidi: int = 0
    initial_max_streams_uni: int = 0
    max_datagram_frame_size: int = 0
    max_record_size: int = DEFAULT_MAX_RECORD_SIZE

    def encode(self) -> bytes:
        """The QX_TRANSPORT_PARAMETERS frame advertising these parameters."""
        body = bytearray()
        for parameter, value in (
            (_IDLE_TIMEOUT, self.max_idle_timeout),
            (_INITIAL_MAX_DATA, self.initial_max_data),
            (_INITIAL_MAX_STREAM_DATA_BIDI_LOCAL, self.initial_max_stream_data_bidi_local),
            (_INITIAL_MAX_STREAM_DATA_BIDI_REMOTE, self.initial_max_stream_data_bidi_remote),
            (_INITIAL_MAX_STREAM_DATA_UNI, self.initial_max_stream_data_uni),
            (_INITIAL_MAX_STREAMS_BIDI, self.initial_max_streams_bidi),
            (_INITIAL_MAX_STREAMS_UNI, self.initial_max_streams_uni),
            (_MAX_DATAGRAM_FRAME_SIZE, self.max_datagram_frame_size),
            (MAX_RECORD_SIZE_PARAMETER, self.max_record_size),
        ):
            if value:
                body += _varints(parameter, varint_len(value), value)
        return _varints(TRANSPORT_PARAMETERS, len(body)) + body

    @classmethod
    def decode(cls, body: bytes) -> Parameters:
        """Decodes the body of a peer's QX_TRANSPORT_PARAMETERS frame. Parameters that only make
        sense over QUIC packets are refused, and unknown ones ignored."""

        def error(reason: str) -> QMuxError:
            return QMuxError(ErrorCode.TRANSPORT_PARAMETER_ERROR, TRANSPORT_PARAMETERS, reason)

        fields = {
            _IDLE_TIMEOUT: "max_idle_timeout",
            _INITIAL_MAX_DATA: "initial_max_data",
            _INITIAL_MAX_STREAM_DATA_BIDI_LOCAL: "initial_max_stream_data_bidi_local",
            _INITIAL_MAX_STREAM_DATA_BIDI_REMOTE: "initial_max_stream_data_bidi_remote",
            _INITIAL_MAX_STREAM_DATA_UNI: "initial_max_stream_data_uni",
            _INITIAL_MAX_STREAMS_BIDI: "initial_max_streams_bidi",
            _INITIAL_MAX_STREAMS_UNI: "initial_max_streams_uni",
            _MAX_DATAGRAM_FRAME_SIZE: "max_datagram_frame_size",
            MAX_RECORD_SIZE_PARAMETER: "max_record_size",
        }
        parameters = cls()
        seen: set[int] = set()
        pos = 0
        while pos < len(body):
            decoded = decode_varint(body, pos)
            if decoded is None:
                raise error("truncated transport parameter ID")
            parameter, pos = decoded
            decoded = decode_varint(body, pos)
            if decoded is None or decoded[0] > len(body) - decoded[1]:
                raise error(f"truncated transport parameter 0x{parameter:x}")
            length, pos = decoded
            value = body[pos : pos + length]
            pos += length
            if parameter in seen:
                raise error(f"duplicate transport parameter 0x{parameter:x}")
            seen.add(parameter)
            if parameter in _FORBIDDEN_PARAMETERS:
                raise error(f"transport parameter 0x{parameter:x} is not allowed in QMux")
            name = fields.get(parameter)
            if name is None:
                continue
            decoded = decode_varint(value, 0)
            if decoded is None or decoded[1] != len(value):
                raise error(f"malformed transport parameter 0x{parameter:x}")
            setattr(parameters, name, decoded[0])
        if parameters.max_record_size < DEFAULT_MAX_RECORD_SIZE:
            raise error(
                f"max_record_size {parameters.max_record_size} is below {DEFAULT_MAX_RECORD_SIZE}"
            )
        for name in ("initial_max_streams_bidi", "initial_max_streams_uni"):
            if getattr(parameters, name) > _MAX_STREAMS:
                raise error(f"{name} exceeds 2^60")
        return parameters


# Session


def _is_uni(stream_id: int) -> bool:
    return bool(stream_id & 2)


class _StreamState:
    """The QMux side of one bidirectional stream: send buffer, offsets and credit."""

    __slots__ = (
        "fin_received",
        "fin_requested",
        "fin_sent",
        "final_size",
        "held",
        "pending",
        "read_cancelled",
        "ready",
        "received",
        "recv_consumed",
        "recv_max",
        "reset_received",
        "reset_sent",
        "send_max",
        "sent",
        "stream",
    )

    def __init__(self, stream: Stream, send_max: int, recv_max: int) -> None:
        self.stream = stream
        self.pending = bytearray()
        self.sent = 0
        self.send_max = send_max
        self.fin_requested = False
        self.fin_sent = False
        self.reset_sent = False
        self.ready = False
        self.received = 0
        self.recv_max = recv_max
        self.recv_consumed = 0
        self.held = 0
        """Bytes delivered to the stream that it has not read yet."""
        self.final_size: int | None = None
        self.fin_received = False
        self.reset_received = False
        self.read_cancelled = False

    @property
    def send_done(self) -> bool:
        return self.fin_sent or self.reset_sent

    @property
    def receive_done(self) -> bool:
        return self.final_size is not None and self.received == self.final_size


class QMuxSession:
    """A QMux session over a TLS connection, and the TrevRPC streams on it."""

    kind: Literal["qmux"] = "qmux"

    def __init__(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        *,
        client: bool,
        parameters: Parameters,
        max_frame_size: int,
        on_stream: Callable[[Stream], None] | None = None,
    ) -> None:
        loop = asyncio.get_running_loop()
        self._loop = loop
        self._reader = reader
        self._writer = writer
        self._client = client
        self.local = parameters
        self.peer: Parameters | None = None
        self.max_frame_size = max_frame_size
        self._on_stream = on_stream
        self._states: dict[int, _StreamState] = {}
        self._opened = 0
        self._open_max = 0
        self._open_waiters: list[asyncio.Future[None]] = []
        self._incoming_opened = 0
        self._incoming_max = parameters.initial_max_streams_bidi
        self._send_max_streams = False
        self._conn_send_max = 0
        self._conn_sent = 0
        self._conn_recv_window = parameters.initial_max_data
        self._conn_recv_max = parameters.initial_max_data
        self._conn_received = 0
        self._conn_consumed = 0
        self._send_max_data = False
        self._stream_credit: set[int] = set()
        self._control: list[bytes] = [parameters.encode()]
        self._ready: dict[int, _StreamState] = {}
        self._blocked: set[_StreamState] = set()
        self._ping_received: int | None = None
        self._send_ping_reply = False
        self._idle_timeout = 0.0
        self._now = loop.time
        """The clock of the idle timeout, which tests replace."""
        self._last_activity = self._now()
        self._timer: asyncio.TimerHandle | None = None
        self._closed_locally = False
        self._close_frame: bytes | None = None
        self._abort_timer: asyncio.TimerHandle | None = None
        self._closed_task: asyncio.Task[None] | None = None
        self._handshake: asyncio.Future[None] = loop.create_future()
        self.compression = ConnectionCompression()
        self._wake = asyncio.Event()
        self.error: TrevRpcError | None = None
        """Why the connection ended, once it has."""
        self.closed: asyncio.Future[None] = loop.create_future()
        self._wake.set()
        self._writer_task = loop.create_task(self._write_loop())
        self._reader_task = loop.create_task(self._read_loop())

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
    ) -> QMuxSession:
        """Connects over TLS with ALPN `qmux-02.trevrpc-1` and waits for the server's transport
        parameters. Receive windows hold one frame, and the server may open no streams."""
        context = ssl.create_default_context()
        if verify:
            if cafile is not None or cadata is not None:
                context = ssl.create_default_context(cafile=cafile, cadata=_cadata(cadata))
        else:
            context.check_hostname = False
            context.verify_mode = ssl.CERT_NONE
        context.set_alpn_protocols([QMUX_ALPN])
        try:
            reader, writer = await asyncio.open_connection(
                host, port, ssl=context, server_hostname=server_name
            )
        except (OSError, ssl.SSLError) as error:
            raise TrevRpcError(Code.UNAVAILABLE, f"transport unavailable: {error}") from error
        protocol = writer.get_extra_info("ssl_object").selected_alpn_protocol()
        if protocol != QMUX_ALPN:
            writer.close()
            raise TrevRpcError(
                Code.UNAVAILABLE,
                f"transport unavailable: server negotiated ALPN {protocol!r} instead of "
                f"{QMUX_ALPN!r}",
            )
        window = max_frame_size + MAX_FRAME_HEADER_SIZE
        parameters = Parameters(
            max_idle_timeout=round(idle_timeout * 1000),
            initial_max_data=window,
            initial_max_stream_data_bidi_local=window,
            max_record_size=RECORD_SIZE,
        )
        session = cls(
            reader, writer, client=True, parameters=parameters, max_frame_size=max_frame_size
        )
        try:
            await asyncio.shield(session._handshake)
        except BaseException:
            session.close()
            raise
        return session

    @property
    def is_open(self) -> bool:
        return self.error is None

    # Streams

    async def open_stream(self, max_frame_size: int) -> Stream:
        """Opens a bidirectional stream, waiting for the peer's stream limit."""
        while True:
            if self.error is not None:
                raise self.error
            if self._opened < self._open_max:
                break
            waiter = self._loop.create_future()
            self._open_waiters.append(waiter)
            try:
                await waiter
            finally:
                if waiter in self._open_waiters:
                    self._open_waiters.remove(waiter)
        stream_id = self._opened << 2 | (0 if self._client else 1)
        self._opened += 1
        peer = self.peer
        assert peer is not None
        stream = Stream(self, stream_id, max_frame_size)
        self._states[stream_id] = _StreamState(
            stream,
            send_max=peer.initial_max_stream_data_bidi_remote,
            recv_max=self.local.initial_max_stream_data_bidi_local,
        )
        return stream

    def _local_stream(self, stream_id: int) -> bool:
        return (stream_id & 1) == (0 if self._client else 1)

    def _state(self, stream_id: int, frame_type: int) -> _StreamState | None:
        """The stream a frame names, opening the peer's streams up to it; `None` for one that
        ended and was forgotten."""
        number = stream_id >> 2
        if self._local_stream(stream_id):
            if _is_uni(stream_id) or number >= self._opened:
                raise QMuxError(
                    ErrorCode.STREAM_STATE_ERROR, frame_type, f"stream {stream_id} was never opened"
                )
            return self._states.get(stream_id)
        if _is_uni(stream_id):
            raise QMuxError(
                ErrorCode.STREAM_LIMIT_ERROR,
                frame_type,
                f"unidirectional stream {stream_id} exceeds the stream limit 0",
            )
        if number < self._incoming_opened:
            return self._states.get(stream_id)
        if number >= self._incoming_max:
            raise QMuxError(
                ErrorCode.STREAM_LIMIT_ERROR,
                frame_type,
                f"stream {stream_id} exceeds the stream limit {self._incoming_max}",
            )
        peer = self.peer
        assert peer is not None
        while self._incoming_opened <= number:
            opened_id = self._incoming_opened << 2 | (1 if self._client else 0)
            self._incoming_opened += 1
            stream = Stream(self, opened_id, self.max_frame_size)
            self._states[opened_id] = _StreamState(
                stream,
                send_max=peer.initial_max_stream_data_bidi_local,
                recv_max=self.local.initial_max_stream_data_bidi_remote,
            )
            if self._on_stream is not None:
                self._on_stream(stream)
        return self._states.get(stream_id)

    def _maybe_forget(self, state: _StreamState) -> None:
        """Forgets a stream once both sides ended on the wire and its reader is done with it,
        granting the peer a new stream when the peer opened it."""
        if not (state.send_done and (state.receive_done or state.reset_received)):
            return
        if not state.read_cancelled:
            return
        stream_id = state.stream.id
        if self._states.pop(stream_id, None) is None:
            return
        self._ready.pop(stream_id, None)
        self._blocked.discard(state)
        self._stream_credit.discard(stream_id)
        if not self._local_stream(stream_id):
            self._incoming_max += 1
            self._send_max_streams = True
            self._wake.set()

    # Called by streams (StreamTransport)

    def write(self, stream: Stream, data: bytes, end: bool) -> bool:
        state = self._states.get(stream.id)
        if state is None or state.send_done or self.error is not None:
            return True
        state.pending += data
        if end:
            state.fin_requested = True
        if not state.ready:
            state.ready = True
            self._ready[stream.id] = state
        self._wake.set()
        if len(state.pending) <= _SEND_LOW_WATER:
            return True
        self._blocked.add(state)
        return False

    def release(self, stream: Stream, size: int) -> None:
        state = self._states.get(stream.id)
        if state is None:
            return
        state.held -= size
        self._consumed(state, size)

    def reset_send(self, stream: Stream) -> None:
        state = self._states.get(stream.id)
        if state is not None:
            self._reset(state, CANCEL_CODE)
            self._maybe_forget(state)

    def abort(self, stream: Stream, *, reset: bool, stop: bool) -> None:
        state = self._states.get(stream.id)
        if state is None or self.error is not None:
            return
        if reset:
            self._reset(state, CANCEL_CODE)
        if stop and not (state.receive_done or state.reset_received):
            self._control.append(stop_sending_frame(stream.id, CANCEL_CODE))
            self._wake.set()
        self._cancel_read(state)

    def finished(self, stream: Stream) -> None:
        state = self._states.get(stream.id)
        if state is not None:
            self._cancel_read(state)

    def _reset(self, state: _StreamState, code: int) -> None:
        """Resets the send side unless it already ended. QMux delivers what was sent, so a stream
        whose FIN went out is complete."""
        if state.send_done or self.error is not None:
            return
        state.reset_sent = True
        state.pending = bytearray()
        state.ready = False
        self._ready.pop(state.stream.id, None)
        self._blocked.discard(state)
        self._control.append(reset_stream_frame(state.stream.id, code, state.sent))
        self._wake.set()

    def _cancel_read(self, state: _StreamState) -> None:
        """Nobody reads the stream any more: what it holds, and what still arrives, is credited."""
        if not state.read_cancelled:
            state.read_cancelled = True
            held, state.held = state.held, 0
            self._consumed(state, held)
        self._maybe_forget(state)

    def _consumed(self, state: _StreamState, size: int) -> None:
        """Returns `size` received bytes of the stream as credit to the peer."""
        if size <= 0:
            return
        state.recv_consumed += size
        window = (
            self.local.initial_max_stream_data_bidi_local
            if self._local_stream(state.stream.id)
            else self.local.initial_max_stream_data_bidi_remote
        )
        if (
            state.final_size is None
            and not state.read_cancelled
            and state.recv_max - state.recv_consumed <= window // 2
        ):
            state.recv_max = state.recv_consumed + window
            self._stream_credit.add(state.stream.id)
            self._wake.set()
        self._conn_consumed += size
        if self._conn_recv_max - self._conn_consumed <= self._conn_recv_window // 2:
            self._conn_recv_max = self._conn_consumed + self._conn_recv_window
            self._send_max_data = True
            self._wake.set()

    # Reading

    async def _read_loop(self) -> None:
        reader = self._reader
        limit = self.local.max_record_size
        try:
            while self.error is None:
                try:
                    first = await reader.readexactly(1)
                except asyncio.IncompleteReadError:
                    self._terminated(_unavailable("connection closed by the server"))
                    return
                length = 1 << (first[0] >> 6)
                header = first + (await reader.readexactly(length - 1) if length > 1 else b"")
                size, _ = decode_varint(header, 0) or (0, 0)
                if size > limit:
                    raise QMuxError(
                        ErrorCode.FRAME_ENCODING_ERROR,
                        0,
                        f"record of {size} bytes exceeds max_record_size {limit}",
                    )
                data = await reader.readexactly(size)
                self._last_activity = self._now()
                self._handle_record(data)
        except QMuxError as error:
            self._fail(error)
        except asyncio.IncompleteReadError:
            self._terminated(_unavailable("connection closed in the middle of a record"))
        except (OSError, ssl.SSLError) as error:
            self._terminated(_unavailable(str(error) or type(error).__name__))
        except asyncio.CancelledError:
            raise

    def _handle_record(self, data: bytes) -> None:
        for frame in parse_frames(data):
            if self.error is not None:
                return
            if self.peer is None and frame.type != TRANSPORT_PARAMETERS:
                raise QMuxError(
                    ErrorCode.PROTOCOL_VIOLATION,
                    frame.type,
                    "first frame is not QX_TRANSPORT_PARAMETERS",
                )
            self._handle_frame(frame)

    def _handle_frame(self, frame: Frame) -> None:
        frame_type = frame.type
        if STREAM <= frame_type <= STREAM | 0x07:
            self._on_stream_frame(frame)
        elif frame_type == MAX_STREAM_DATA:
            state = self._state(frame.stream_id, frame_type)
            if state is not None and frame.maximum > state.send_max:
                state.send_max = frame.maximum
                if state.pending or (state.fin_requested and not state.fin_sent):
                    if not state.ready:
                        state.ready = True
                        self._ready[frame.stream_id] = state
                    self._wake.set()
        elif frame_type == MAX_DATA:
            if frame.maximum > self._conn_send_max:
                self._conn_send_max = frame.maximum
                self._wake.set()
        elif frame_type == RESET_STREAM:
            self._on_reset_stream(frame)
        elif frame_type == STOP_SENDING:
            state = self._state(frame.stream_id, frame_type)
            if state is not None and not state.send_done:
                self._reset(state, frame.code)
                state.stream.on_stop_sending(frame.code)
                self._maybe_forget(state)
        elif frame_type in (MAX_STREAMS_BIDI, MAX_STREAMS_UNI, STREAMS_BLOCKED_BIDI):
            if frame.maximum > _MAX_STREAMS:
                raise QMuxError(
                    ErrorCode.FRAME_ENCODING_ERROR, frame_type, f"{frame.maximum} exceeds 2^60"
                )
            if frame_type == MAX_STREAMS_BIDI and frame.maximum > self._open_max:
                self._open_max = frame.maximum
                self._wake_openers()
        elif frame_type == STREAMS_BLOCKED_UNI:
            if frame.maximum > _MAX_STREAMS:
                raise QMuxError(
                    ErrorCode.FRAME_ENCODING_ERROR, frame_type, f"{frame.maximum} exceeds 2^60"
                )
        elif frame_type == STREAM_DATA_BLOCKED:
            self._state(frame.stream_id, frame_type)
        elif frame_type in (CONNECTION_CLOSE, APPLICATION_CLOSE):
            reason = frame.reason.decode(errors="replace") or f"error code {frame.code}"
            self._terminated(_unavailable(f"connection closed by the server: {reason}"))
        elif frame_type == PING:
            if self._ping_received is not None and frame.sequence <= self._ping_received:
                raise QMuxError(
                    ErrorCode.PROTOCOL_VIOLATION,
                    frame_type,
                    f"QX_PING sequence number {frame.sequence} is not increasing",
                )
            self._ping_received = frame.sequence
            self._send_ping_reply = True
            self._wake.set()
        elif frame_type == PING_RESPONSE:
            # This endpoint sends no QX_PING, so no response is ever due.
            raise QMuxError(
                ErrorCode.PROTOCOL_VIOLATION,
                frame_type,
                f"QX_PING response {frame.sequence} was never requested",
            )
        elif frame_type == TRANSPORT_PARAMETERS:
            self._on_parameters(frame)
        elif frame_type in (DATAGRAM, DATAGRAM_LEN):
            raise QMuxError(
                ErrorCode.PROTOCOL_VIOLATION,
                frame_type,
                "DATAGRAM frame without max_datagram_frame_size",
            )
        # PADDING and DATA_BLOCKED need nothing.

    def _on_parameters(self, frame: Frame) -> None:
        if self.peer is not None:
            raise QMuxError(
                ErrorCode.PROTOCOL_VIOLATION, frame.type, "repeated QX_TRANSPORT_PARAMETERS"
            )
        peer = Parameters.decode(frame.data)
        self.peer = peer
        self._open_max = peer.initial_max_streams_bidi
        self._conn_send_max = peer.initial_max_data
        timeouts = [t for t in (self.local.max_idle_timeout, peer.max_idle_timeout) if t > 0]
        self._idle_timeout = min(timeouts) / 1000 if timeouts else 0.0
        if self._idle_timeout:
            self._schedule_timer()
        if not self._handshake.done():
            self._handshake.set_result(None)
        self._wake_openers()

    def _on_stream_frame(self, frame: Frame) -> None:
        state = self._state(frame.stream_id, frame.type)
        if state is None:
            # The stream ended and every byte up to its final size arrived: a conforming peer
            # sends nothing more. Data would skip the flow control checks, so it ends the
            # connection, while an empty frame costs nothing to ignore.
            if frame.data:
                raise QMuxError(
                    ErrorCode.FINAL_SIZE_ERROR,
                    frame.type,
                    f"stream {frame.stream_id} data beyond its final size",
                )
            return
        end = frame.offset + len(frame.data)
        if state.final_size is not None and (
            end > state.final_size or (frame.fin and end != state.final_size)
        ):
            raise QMuxError(
                ErrorCode.FINAL_SIZE_ERROR,
                frame.type,
                f"stream {frame.stream_id} data beyond its final size",
            )
        if frame.offset != state.received:
            raise QMuxError(
                ErrorCode.PROTOCOL_VIOLATION,
                frame.type,
                f"stream {frame.stream_id} data at offset {frame.offset}, "
                f"expected {state.received}",
            )
        if end > state.recv_max:
            raise QMuxError(
                ErrorCode.FLOW_CONTROL_ERROR,
                frame.type,
                f"stream {frame.stream_id} exceeds its flow control limit",
            )
        self._conn_received += len(frame.data)
        if self._conn_received > self._conn_recv_max:
            raise QMuxError(
                ErrorCode.FLOW_CONTROL_ERROR, frame.type, "connection flow control limit exceeded"
            )
        state.received = end
        if frame.fin:
            state.final_size = end
            state.fin_received = True
        if state.read_cancelled or state.reset_received:
            self._consumed(state, len(frame.data))
            self._maybe_forget(state)
            return
        state.held += len(frame.data)
        state.stream.on_data(frame.data, frame.fin)

    def _on_reset_stream(self, frame: Frame) -> None:
        state = self._state(frame.stream_id, frame.type)
        if state is None:
            return
        if state.fin_received and frame.final_size == state.final_size:
            # Every byte arrived already, so the reset changes nothing.
            return
        if (
            state.final_size is not None and frame.final_size != state.final_size
        ) or frame.final_size < state.received:
            raise QMuxError(
                ErrorCode.FINAL_SIZE_ERROR,
                frame.type,
                f"stream {frame.stream_id} reset with an inconsistent final size",
            )
        if frame.final_size > state.recv_max:
            raise QMuxError(
                ErrorCode.FLOW_CONTROL_ERROR,
                frame.type,
                f"stream {frame.stream_id} final size exceeds its flow control limit",
            )
        unsent = frame.final_size - state.received
        self._conn_received += unsent
        if self._conn_received > self._conn_recv_max:
            raise QMuxError(
                ErrorCode.FLOW_CONTROL_ERROR, frame.type, "connection flow control limit exceeded"
            )
        state.received = state.final_size = frame.final_size
        if not state.reset_received:
            state.reset_received = True
            # Bytes that will never arrive count as read.
            self._consumed(state, unsent)
            if not state.read_cancelled:
                state.stream.on_reset(frame.code)
        self._maybe_forget(state)

    def _wake_openers(self) -> None:
        waiters, self._open_waiters = self._open_waiters, []
        for waiter in waiters:
            if not waiter.done():
                waiter.set_result(None)

    # Writing

    async def _write_loop(self) -> None:
        writer = self._writer
        try:
            while True:
                await self._wake.wait()
                self._wake.clear()
                while (frames := self._build_record()) is not None:
                    writer.write(record(frames))
                    self._last_activity = self._now()
                    if self._buffered() >= RECORD_SIZE:
                        await writer.drain()
                if self._close_frame is not None:
                    writer.write(record(self._close_frame))
                    self._close_transport()
                    return
                await writer.drain()
                self._check_drained()
        except (OSError, ssl.SSLError, ConnectionError) as error:
            self._terminated(_unavailable(str(error) or type(error).__name__))

    def _check_drained(self) -> None:
        for state in list(self._blocked):
            if len(state.pending) <= _SEND_LOW_WATER:
                self._blocked.discard(state)
                state.stream.on_drained()

    def _build_record(self) -> bytes | None:
        """The frames of the next record, or `None` when nothing waits to be sent."""
        if self.error is not None and self._close_frame is None:
            return None
        peer_limit = self.peer.max_record_size if self.peer is not None else DEFAULT_MAX_RECORD_SIZE
        limit = min(peer_limit, RECORD_SIZE)
        out = bytearray()
        while self._control and len(out) + len(self._control[0]) <= limit:
            out += self._control.pop(0)
        if self._control:
            return bytes(out)
        if self.peer is None or self.error is not None:
            return bytes(out) if out else None
        if self._send_max_data:
            out += max_data_frame(self._conn_recv_max)
            self._send_max_data = False
        if self._send_max_streams:
            out += max_streams_bidi_frame(self._incoming_max)
            self._send_max_streams = False
        while self._stream_credit and len(out) + 17 <= limit:
            stream_id = self._stream_credit.pop()
            state = self._states.get(stream_id)
            if state is not None and state.final_size is None:
                out += max_stream_data_frame(stream_id, state.recv_max)
        if self._send_ping_reply and self._ping_received is not None:
            out += ping_frame(PING_RESPONSE, self._ping_received)
            self._send_ping_reply = False
        self._append_stream_data(out, limit)
        return bytes(out) if out else None

    def _append_stream_data(self, out: bytearray, limit: int) -> None:
        """Appends STREAM frames of the streams with data, a quantum each in turn, until the
        record is full or credit runs out."""
        again: list[_StreamState] = []
        progress = False
        for stream_id, state in list(self._ready.items()):
            room = limit - len(out) - 32
            if room <= 0:
                break
            del self._ready[stream_id]
            if state.send_done:
                state.ready = False
                continue
            credit = min(state.send_max - state.sent, self._conn_send_max - self._conn_sent)
            size = max(0, min(len(state.pending), credit, _STREAM_QUANTUM, room))
            fin = state.fin_requested and size == len(state.pending)
            if size == 0 and not fin:
                if state.pending and state.sent < state.send_max:
                    # Blocked on the connection limit, which MAX_DATA lifts.
                    again.append(state)
                else:
                    # Nothing to send, or blocked on the stream limit, which MAX_STREAM_DATA lifts.
                    state.ready = False
                continue
            out += stream_frame(stream_id, state.sent, memoryview(state.pending)[:size], fin)
            progress = True
            del state.pending[:size]
            state.sent += size
            self._conn_sent += size
            if fin:
                state.fin_sent = True
                state.ready = False
                self._maybe_forget(state)
            elif state.pending or state.fin_requested:
                again.append(state)
            else:
                state.ready = False
        for state in again:
            if state.ready:
                self._ready[state.stream.id] = state
        # Another turn for what is left, but only after progress: a stream blocked on the
        # connection's credit waits for MAX_DATA, which wakes the writer. Waking it anyway would
        # spin without yielding, starving the reader that would receive that MAX_DATA.
        if progress and any(state.pending for state in again):
            self._wake.set()

    # Idle timeout

    def _schedule_timer(self) -> None:
        tick = max(self._idle_timeout / 4, 0.01)
        self._timer = self._loop.call_later(tick, self._on_timer)

    def _on_timer(self) -> None:
        self._timer = None
        if self.error is not None or not self._idle_timeout:
            return
        # Every record sent or received restarts the idle timeout. The client sends no
        # keep-alives, so an idle session closes and the channel reconnects at its next call.
        if self._now() - self._last_activity >= self._idle_timeout:
            self._terminated(_unavailable("connection idle timeout"))
            return
        self._schedule_timer()

    # Ending

    def close(self) -> None:
        """Closes the session with an application CONNECTION_CLOSE; its calls fail with
        `CANCELLED`."""
        if self.error is not None:
            return
        self._closed_locally = True
        self._close_frame = application_close_frame(0, "client closed")
        self._terminated(TrevRpcError(Code.CANCELLED, "transport closed locally"))
        self._closing()

    def _fail(self, error: QMuxError) -> None:
        """Closes the session after a protocol error of the peer's."""
        if self.error is not None:
            return
        self._close_frame = transport_close_frame(error.code, error.frame_type, error.reason)
        self._terminated(_unavailable(f"QMux protocol error: {error}"))
        self._closing()

    def _closing(self) -> None:
        """Lets the writer send the CONNECTION_CLOSE, within the grace period: a write loop held
        in drain() by a peer that stopped reading never gets to it."""
        self._wake.set()
        self._arm_abort()

    def _close_transport(self) -> None:
        """Closes the connection once what it buffered is sent, within the grace period. A peer
        that stopped reading never takes it, and the socket would stay open forever."""
        buffered = self._buffered()
        self._writer.close()
        if buffered:
            self._arm_abort()
        if self._closed_task is None:
            self._closed_task = self._loop.create_task(self._closed())

    _close_grace = _CLOSE_GRACE

    def _buffered(self) -> int:
        """The bytes the transport has yet to send; none once its connection is gone, when
        asyncio's TLS transport has nothing left to ask."""
        try:
            return self._writer.transport.get_write_buffer_size()
        except AttributeError:
            return 0

    def _arm_abort(self) -> None:
        if self._abort_timer is None:
            self._abort_timer = self._loop.call_later(self._close_grace, self._abort)

    async def _closed(self) -> None:
        """Disarms the abort once the connection closed by itself."""
        with contextlib.suppress(Exception):
            await self._writer.wait_closed()
        if self._abort_timer is not None:
            self._abort_timer.cancel()

    def _abort(self) -> None:
        if not self._writer_task.done():
            self._writer_task.cancel()
        self._writer.transport.abort()

    def _terminated(self, error: TrevRpcError) -> None:
        if self.error is not None:
            return
        self.error = error
        if not self._handshake.done():
            self._handshake.set_exception(error)
            self._handshake.exception()
        if self._timer is not None:
            self._timer.cancel()
            self._timer = None
        states = list(self._states.values())
        self._states.clear()
        self._ready.clear()
        self._blocked.clear()
        self._wake_openers()
        for state in states:
            state.stream.on_connection_error(error)
        self._reader_task.cancel()
        if self._close_frame is None:
            self._writer_task.cancel()
            self._close_transport()
        if not self.closed.done():
            self.closed.set_result(None)


def _unavailable(reason: str) -> TrevRpcError:
    return TrevRpcError(Code.UNAVAILABLE, f"transport unavailable: {reason}")


def _cadata(cadata: bytes | None) -> str | bytes | None:
    """`ssl` takes PEM as text and DER as bytes."""
    if cadata is not None and cadata.lstrip().startswith(b"-----"):
        return cadata.decode()
    return cadata
