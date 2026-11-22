# pyright: reportPrivateUsage=false
"""A small TrevRPC server for testing the client's call paths over real connections: native QUIC on
aioquic, and QMux over TLS on the runtime's own QMux session in its server role.

Its methods take and return `google.protobuf.StringValue`:

- `/test.Echo/Unary` echoes the value with the `suffix` metadata appended; `fail:<code>:<message>`
  fails with that status, and `headers` sends headers `{h: 1}` and trailers `{t: 2}`.
- `/test.Echo/ServerStream` sends the value's number of responses `0`, `1`, ...
- `/test.Echo/ClientStream` responds with every request's value joined.
- `/test.Echo/Bidi` echoes each request; `fail:<code>:<message>` ends the call with that status and
  stops the request stream, both in one packet.
- `/test.Echo/FloodFail` sends 40 responses at the first request, then fails with `ABORTED` and
  stops the request stream in one packet.
- `/test.Echo/StopFirst` stops the request stream at the first request, then fails with
  `FAILED_PRECONDITION` "stopped": the STOP_SENDING comes first.
- `/test.Echo/Reset` resets the stream with code 7 at the first request.
- `/test.Echo/Truncate` ends the stream without a STATUS at the first request.
- `/test.Echo/Drop` closes the connection at the first request.
- `/test.Echo/Flood` sends 64 KiB responses for as long as the client lets it.
- `/test.Echo/Hang` never responds.

Tests wait on what the server saw through `Signal`s, never by sleeping.
"""

from __future__ import annotations

import asyncio
import datetime
import ipaddress
import ssl
import tempfile
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Protocol, cast

from aioquic.asyncio.protocol import QuicConnectionProtocol
from aioquic.asyncio.server import serve  # pyright: ignore[reportUnknownVariableType]
from aioquic.quic import events
from aioquic.quic.configuration import QuicConfiguration
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.x509.oid import NameOID
from protobuf.wkt.google.protobuf.wrappers_pb import StringValue
from trevrpc import TrevRpcError
from trevrpc._frames import FrameReader, FrameType, encode_frame
from trevrpc._qmux import (
    QMUX_ALPN,
    RECORD_SIZE,
    RESET_STREAM,
    STOP_SENDING,
    Frame,
    Parameters,
    QMuxSession,
    application_close_frame,
    stop_sending_frame,
)
from trevrpc._stream import Stream
from trevrpc._wire import RpcHeaders, _len_field, _map_field, _varint_field, read_fields

FLOOD_MESSAGE = 64 * 1024
WAIT = 5.0
"""How long a test waits for the server to see something."""


class Signal:
    """Wakes whoever waits for a condition each time `notify()` says something changed."""

    def __init__(self) -> None:
        self._waiters: list[asyncio.Future[None]] = []

    def notify(self) -> None:
        waiters, self._waiters = self._waiters, []
        for waiter in waiters:
            if not waiter.done():
                waiter.set_result(None)

    async def wait_until(self, condition: Callable[[], bool], timeout: float = WAIT) -> None:
        async with asyncio.timeout(timeout):
            while not condition():
                waiter = asyncio.get_running_loop().create_future()
                self._waiters.append(waiter)
                await waiter


def _status_frame(
    code: int = 0, message: str = "", metadata: dict[str, bytes] | None = None
) -> bytes:
    payload = (
        _varint_field(1, code)
        + (_len_field(2, message.encode()) if message else b"")
        + _map_field(3, metadata or {})
    )
    return encode_frame(FrameType.STATUS, payload)


def _message_frame(value: str) -> bytes:
    return encode_frame(FrameType.MESSAGE, StringValue(value=value).to_binary())


def _headers_frame(metadata: dict[str, bytes]) -> bytes:
    return encode_frame(FrameType.HEADERS, _map_field(1, metadata))


@dataclass
class ServerCall:
    """What the server saw of one call."""

    stream_id: int
    method: str = ""
    metadata: dict[str, bytes] = field(default_factory=dict[str, bytes])
    timeout_nanos: int = 0
    reset_code: int | None = None
    stop_code: int | None = None
    blocked_at: int | None = None
    """For `Flood`: the bytes sent when the client's credit ran out."""

    @property
    def ended_by_client(self) -> bool:
        return self.reset_code is not None or self.stop_code is not None

    @classmethod
    def from_request(cls, stream_id: int, payload: bytes) -> ServerCall:
        call = cls(stream_id)
        for number, _, value in read_fields(payload):
            if number == 1 and isinstance(value, bytes):
                call.method = value.decode()
            elif number == 2 and isinstance(value, bytes):
                call.metadata.update(RpcHeaders.decode(_len_field(1, value)).metadata)
            elif number == 4 and isinstance(value, int):
                call.timeout_nanos = value
        return call


class _Wire(Protocol):
    """One call's stream on one transport."""

    def send(self, data: bytes, end: bool = False) -> None: ...

    def stop(self, code: int) -> None: ...

    def reset(self, code: int) -> None: ...

    def drop(self) -> None:
        """Closes the whole connection."""
        ...

    def flow(self) -> tuple[int, int, int] | None:
        """The stream's unsent bytes, sent bytes and send credit, or `None` once it is gone."""
        ...


class Connection(Protocol):
    def close(self) -> None: ...

    def sent(self, stream_id: int) -> int:
        """The bytes of a stream the server has sent, which the client's credit bounds."""
        ...


async def _handle(
    server: TestServer, call: ServerCall, wire: _Wire, queue: asyncio.Queue[str | None]
) -> None:
    method = call.method.rsplit("/", 1)[-1]
    try:
        match method:
            case "Unary":
                value = await queue.get()
                assert value is not None
                if value.startswith("fail:"):
                    _, code, message = value.split(":", 2)
                    wire.send(_status_frame(int(code), message), end=True)
                elif value == "headers":
                    frames = _headers_frame({"h": b"1"}) + _message_frame(value)
                    wire.send(frames + _status_frame(0, "", {"t": b"2"}), end=True)
                else:
                    suffix = call.metadata.get("suffix", b"").decode()
                    wire.send(_message_frame(value + suffix) + _status_frame(), end=True)
            case "ServerStream":
                value = await queue.get()
                assert value is not None
                for i in range(int(value)):
                    wire.send(_message_frame(str(i)))
                wire.send(_status_frame(), end=True)
            case "ClientStream":
                values: list[str] = []
                while (value := await queue.get()) is not None:
                    values.append(value)
                wire.send(_message_frame("".join(values)) + _status_frame(), end=True)
            case "Bidi":
                while (value := await queue.get()) is not None:
                    if value.startswith("fail:"):
                        _, code, message = value.split(":", 2)
                        # One packet, as Go and Rust servers send them.
                        wire.send(_status_frame(int(code), message), end=True)
                        wire.stop(0)
                        return
                    wire.send(_message_frame(value))
                wire.send(_status_frame(), end=True)
            case "FloodFail":
                await queue.get()
                frames = b"".join(_message_frame(str(i)) for i in range(40))
                wire.send(frames + _status_frame(10, "abort"), end=True)
                wire.stop(0)
            case "StopFirst":
                await queue.get()
                wire.stop(0)
                wire.send(_status_frame(9, "stopped"), end=True)
            case "Reset":
                await queue.get()
                wire.reset(7)
            case "Truncate":
                await queue.get()
                wire.send(b"", end=True)
            case "Drop":
                await queue.get()
                wire.drop()
            case "Flood":
                await queue.get()
                await _flood(server, call, wire)
            case _:  # Hang
                await server.changed.wait_until(lambda: call.ended_by_client, 60)
    except Exception:
        # The stream is gone, as when the client reset it.
        pass


async def _flood(server: TestServer, call: ServerCall, wire: _Wire) -> None:
    payload = _message_frame("x" * FLOOD_MESSAGE)
    while not call.ended_by_client and (flow := wire.flow()) is not None:
        unsent, sent, credit = flow
        # Keep a few messages queued past what the client allows.
        if unsent < 4 * len(payload):
            wire.send(payload)
            server.changed.notify()
            continue
        if sent >= credit and call.blocked_at is None:
            call.blocked_at = sent
            server.changed.notify()

        def moved(sent: int = sent, credit: int = credit) -> bool:
            """More was sent, the client granted more credit, or it stopped the stream."""
            flow = wire.flow()
            return call.ended_by_client or flow is None or flow[1:] != (sent, credit)

        await server.changed.wait_until(moved, 60)
        flow = wire.flow()
        if flow is not None and flow[2] != credit:
            call.blocked_at = None


# Native QUIC


class _QuicWire:
    """Writes made together go out together, as frames written together share QMux records:
    a STATUS and the STOP_SENDING after it arrive in one packet, as Go and Rust servers send
    them."""

    def __init__(self, protocol: _QuicProtocol, stream_id: int) -> None:
        self._protocol = protocol
        self._id = stream_id

    def send(self, data: bytes, end: bool = False) -> None:
        self._protocol._quic.send_stream_data(self._id, data, end_stream=end)
        self._protocol._transmit_soon()

    def stop(self, code: int) -> None:
        self._protocol._quic.stop_stream(self._id, code)
        self._protocol._transmit_soon()

    def reset(self, code: int) -> None:
        self._protocol._quic.reset_stream(self._id, code)
        self._protocol._transmit_soon()

    def drop(self) -> None:
        self._protocol.close(error_code=0x101, reason_phrase="dropped")

    def flow(self) -> tuple[int, int, int] | None:
        stream = self._protocol._quic._streams.get(self._id)
        if stream is None:
            return None
        sender = stream.sender
        return (
            sender._buffer_stop - sender.highest_offset,
            sender.highest_offset,
            stream.max_stream_data_remote,
        )


class _QuicProtocol(QuicConnectionProtocol):
    server: TestServer

    def __init__(self, *args: object, **kwargs: object) -> None:
        super().__init__(*args, **kwargs)  # pyright: ignore[reportArgumentType]
        self._readers: dict[int, FrameReader] = {}
        self._queues: dict[int, asyncio.Queue[str | None]] = {}
        self._calls: dict[int, ServerCall] = {}

    def datagram_received(self, data: bytes | str, addr: object) -> None:
        super().datagram_received(data, addr)  # pyright: ignore[reportArgumentType]
        self.server.changed.notify()

    def quic_event_received(self, event: events.QuicEvent) -> None:
        match event:
            case events.StreamDataReceived(stream_id=stream_id, data=data, end_stream=end):
                reader = self._readers.setdefault(stream_id, FrameReader())
                reader.feed(data)
                while (frame := reader.next()) is not None:
                    self._frame(stream_id, *frame)
                if end and stream_id in self._queues:
                    self._queues[stream_id].put_nowait(None)
            case events.StreamReset(stream_id=stream_id, error_code=code):
                if (call := self._calls.get(stream_id)) is not None:
                    call.reset_code = code
                if stream_id in self._queues:
                    self._queues[stream_id].put_nowait(None)
            case events.StopSendingReceived(stream_id=stream_id, error_code=code):
                if (call := self._calls.get(stream_id)) is not None:
                    call.stop_code = code
            case _:
                pass
        self.server.changed.notify()

    def _frame(self, stream_id: int, frame_type: int, payload: bytes) -> None:
        if frame_type == FrameType.REQUEST:
            call = ServerCall.from_request(stream_id, payload)
            self.server.calls.append(call)
            self._calls[stream_id] = call
            queue: asyncio.Queue[str | None] = asyncio.Queue()
            self._queues[stream_id] = queue
            wire = _QuicWire(self, stream_id)
            asyncio.get_running_loop().create_task(_handle(self.server, call, wire, queue))
        elif frame_type == FrameType.MESSAGE:
            self._queues[stream_id].put_nowait(StringValue.from_binary(payload).value)

    def sent(self, stream_id: int) -> int:
        stream = self._quic._streams.get(stream_id)
        return 0 if stream is None else stream.sender.highest_offset


# QMux over TLS


class _QMuxWire:
    def __init__(self, session: _QMuxServerSession, stream: Stream) -> None:
        self._session = session
        self._stream = stream

    def send(self, data: bytes, end: bool = False) -> None:
        self._session.write(self._stream, data, end)
        if end:
            # Writes bypass the stream, which must still learn that its send side ended, so the
            # session forgets it and grants the client a new one.
            self._stream._send_done = True
            self._stream._maybe_finished()

    def stop(self, code: int) -> None:
        self._session._control.append(stop_sending_frame(self._stream.id, code))
        self._session._wake.set()

    def reset(self, code: int) -> None:
        state = self._session._states.get(self._stream.id)
        if state is not None:
            self._session._reset(state, code)

    def drop(self) -> None:
        session = self._session
        session._close_frame = application_close_frame(0x101, "dropped")
        session._terminated(TrevRpcError(14, "dropped"))
        session._wake.set()

    def flow(self) -> tuple[int, int, int] | None:
        state = self._session._states.get(self._stream.id)
        if state is None:
            return None
        return len(state.pending), state.sent, state.send_max


class _QMuxServerSession(QMuxSession):
    """The server role, which notifies the test server of every record and records how the client
    ended each call."""

    server: TestServer
    calls: dict[int, ServerCall]

    def _handle_frame(self, frame: Frame) -> None:
        super()._handle_frame(frame)
        if frame.type in (RESET_STREAM, STOP_SENDING):
            call = self.calls.get(frame.stream_id)
            if call is not None:
                if frame.type == RESET_STREAM:
                    call.reset_code = frame.code
                else:
                    call.stop_code = frame.code
        self.server.changed.notify()

    def _build_record(self) -> bytes | None:
        frames = super()._build_record()
        self.server.changed.notify()
        return frames

    def sent(self, stream_id: int) -> int:
        state = self._states.get(stream_id)
        return 0 if state is None else state.sent

    async def serve(self, stream: Stream) -> None:
        queue: asyncio.Queue[str | None] = asyncio.Queue()
        try:
            while (frame := await stream.read_frame()) is not None:
                frame_type, payload = frame
                if frame_type == FrameType.REQUEST:
                    call = ServerCall.from_request(stream.id, payload)
                    self.server.calls.append(call)
                    self.calls[stream.id] = call
                    wire = _QMuxWire(self, stream)
                    asyncio.get_running_loop().create_task(_handle(self.server, call, wire, queue))
                elif frame_type == FrameType.MESSAGE:
                    queue.put_nowait(StringValue.from_binary(payload).value)
        except TrevRpcError:
            pass
        queue.put_nowait(None)


# The server


@dataclass
class TestServer:
    __test__ = False

    port: int = 0
    """The UDP port of native QUIC."""
    tcp_port: int = 0
    """The TCP port of QMux over TLS."""
    ca: str = ""
    calls: list[ServerCall] = field(default_factory=list[ServerCall])
    connections: list[Connection] = field(default_factory=list[Connection])
    changed: Signal = field(default_factory=Signal)
    close: Callable[[], None] = lambda: None

    async def call(self, index: int = 0) -> ServerCall:
        """The server's `index`th call, once it arrived."""
        await self.changed.wait_until(lambda: len(self.calls) > index)
        return self.calls[index]

    async def ended(self, call: ServerCall) -> None:
        """Waits until the client reset or stopped the call's stream."""
        await self.changed.wait_until(lambda: call.ended_by_client)


def _certificate(directory: Path) -> tuple[Path, Path]:
    key = ec.generate_private_key(ec.SECP256R1())
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "localhost")])
    now = datetime.datetime.now(datetime.UTC)
    certificate = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - datetime.timedelta(minutes=1))
        .not_valid_after(now + datetime.timedelta(days=1))
        .add_extension(
            x509.SubjectAlternativeName(
                [x509.DNSName("localhost"), x509.IPAddress(ipaddress.ip_address("127.0.0.1"))]
            ),
            critical=False,
        )
        .sign(key, hashes.SHA256())
    )
    cert_path = directory / "cert.pem"
    key_path = directory / "key.pem"
    cert_path.write_bytes(certificate.public_bytes(serialization.Encoding.PEM))
    key_path.write_bytes(
        key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        )
    )
    return cert_path, key_path


async def start_server(*, max_streams: int | None = None) -> TestServer:
    """Starts the server on loopback ports; `max_streams` lowers how many bidirectional streams
    the client may have open at once (128 by default)."""
    directory = tempfile.TemporaryDirectory()
    cert, key = _certificate(Path(directory.name))
    test_server = TestServer(ca=str(cert))

    configuration = QuicConfiguration(is_client=False, alpn_protocols=["trevrpc/1"])
    configuration.load_cert_chain(str(cert), str(key))  # pyright: ignore[reportUnknownMemberType]

    def create_protocol(*args: object, **kwargs: object) -> _QuicProtocol:
        protocol = _QuicProtocol(*args, **kwargs)
        protocol.server = test_server
        if max_streams is not None:
            # Read into the transport parameters at the handshake, which comes later.
            limit = protocol._quic._local_max_streams_bidi
            limit.value = limit.sent = max_streams
        test_server.connections.append(protocol)
        return protocol

    quic = await serve("127.0.0.1", 0, configuration=configuration, create_protocol=create_protocol)
    sockname = quic._transport.get_extra_info("sockname")  # pyright: ignore[reportOptionalMemberAccess]
    test_server.port = cast(tuple[str, int], sockname)[1]

    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(cert, key)
    context.set_alpn_protocols([QMUX_ALPN])
    parameters = Parameters(
        max_idle_timeout=30000,
        initial_max_data=16 * 1024 * 1024,
        initial_max_stream_data_bidi_remote=4 * 1024 * 1024 + 16,
        initial_max_streams_bidi=max_streams if max_streams is not None else 128,
        max_record_size=RECORD_SIZE,
    )

    async def accept(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        def on_stream(stream: Stream) -> None:
            asyncio.get_running_loop().create_task(session.serve(stream))

        session = _QMuxServerSession(
            reader,
            writer,
            client=False,
            parameters=parameters,
            max_frame_size=4 * 1024 * 1024,
            on_stream=on_stream,
        )
        session.server = test_server
        session.calls = {}
        test_server.connections.append(session)
        await session.closed

    tcp = await asyncio.start_server(accept, "127.0.0.1", 0, ssl=context)
    test_server.tcp_port = tcp.sockets[0].getsockname()[1]

    def close() -> None:
        quic.close()
        tcp.close()
        for connection in test_server.connections:
            connection.close()
        directory.cleanup()

    test_server.close = close
    return test_server
