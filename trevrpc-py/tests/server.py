"""A small TrevRPC server on aioquic, for testing the client's call paths over real QUIC.

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
  `FAILED_PRECONDITION` "stopped" in a later packet.
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
import tempfile
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import cast

from aioquic.asyncio.protocol import QuicConnectionProtocol
from aioquic.asyncio.server import serve  # pyright: ignore[reportUnknownVariableType]
from aioquic.quic import events
from aioquic.quic.configuration import QuicConfiguration
from aioquic.quic.stream import QuicStream
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.x509.oid import NameOID
from protobuf.wkt.google.protobuf.wrappers_pb import StringValue
from trevrpc._frames import FrameReader, FrameType, encode_frame
from trevrpc._wire import RpcHeaders, _len_field, _map_field, _varint_field, read_fields  # pyright: ignore[reportPrivateUsage]

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


class _Protocol(QuicConnectionProtocol):
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
                call = self._calls.get(stream_id)
                if call is not None:
                    call.reset_code = code
                if stream_id in self._queues:
                    self._queues[stream_id].put_nowait(None)
            case events.StopSendingReceived(stream_id=stream_id, error_code=code):
                call = self._calls.get(stream_id)
                if call is not None:
                    call.stop_code = code
            case _:
                pass
        self.server.changed.notify()

    def _frame(self, stream_id: int, frame_type: int, payload: bytes) -> None:
        if frame_type == FrameType.REQUEST:
            call = ServerCall(stream_id)
            for number, _, value in read_fields(payload):
                if number == 1 and isinstance(value, bytes):
                    call.method = value.decode()
                elif number == 2 and isinstance(value, bytes):
                    entry = _len_field(1, value)
                    call.metadata.update(RpcHeaders.decode(entry).metadata)
                elif number == 4 and isinstance(value, int):
                    call.timeout_nanos = value
            self.server.calls.append(call)
            self._calls[stream_id] = call
            queue: asyncio.Queue[str | None] = asyncio.Queue()
            self._queues[stream_id] = queue
            asyncio.get_running_loop().create_task(self._handle(call, queue))
        elif frame_type == FrameType.MESSAGE:
            self._queues[stream_id].put_nowait(StringValue.from_binary(payload).value)

    def _send(self, stream_id: int, data: bytes, end: bool = False) -> None:
        self._quic.send_stream_data(stream_id, data, end_stream=end)
        self.transmit()

    async def _handle(self, call: ServerCall, queue: asyncio.Queue[str | None]) -> None:
        stream_id = call.stream_id
        method = call.method.rsplit("/", 1)[-1]
        try:
            match method:
                case "Unary":
                    value = await queue.get()
                    assert value is not None
                    if value.startswith("fail:"):
                        _, code, message = value.split(":", 2)
                        self._send(stream_id, _status_frame(int(code), message), end=True)
                    elif value == "headers":
                        frames = _headers_frame({"h": b"1"}) + _message_frame(value)
                        frames += _status_frame(0, "", {"t": b"2"})
                        self._send(stream_id, frames, end=True)
                    else:
                        suffix = call.metadata.get("suffix", b"").decode()
                        frames = _message_frame(value + suffix) + _status_frame()
                        self._send(stream_id, frames, end=True)
                case "ServerStream":
                    value = await queue.get()
                    assert value is not None
                    for i in range(int(value)):
                        self._send(stream_id, _message_frame(str(i)))
                    self._send(stream_id, _status_frame(), end=True)
                case "ClientStream":
                    values: list[str] = []
                    while (value := await queue.get()) is not None:
                        values.append(value)
                    frames = _message_frame("".join(values)) + _status_frame()
                    self._send(stream_id, frames, end=True)
                case "Bidi":
                    while (value := await queue.get()) is not None:
                        if value.startswith("fail:"):
                            _, code, message = value.split(":", 2)
                            # One packet, as Go and Rust servers send them.
                            self._quic.send_stream_data(
                                stream_id, _status_frame(int(code), message), end_stream=True
                            )
                            self._quic.stop_stream(stream_id, 0)
                            self.transmit()
                            return
                        self._send(stream_id, _message_frame(value))
                    self._send(stream_id, _status_frame(), end=True)
                case "FloodFail":
                    await queue.get()
                    frames = b"".join(_message_frame(str(i)) for i in range(40))
                    self._quic.send_stream_data(
                        stream_id, frames + _status_frame(10, "abort"), end_stream=True
                    )
                    self._quic.stop_stream(stream_id, 0)
                    self.transmit()
                case "StopFirst":
                    await queue.get()
                    self._quic.stop_stream(stream_id, 0)
                    self.transmit()
                    self._send(stream_id, _status_frame(9, "stopped"), end=True)
                case "Reset":
                    await queue.get()
                    self._quic.reset_stream(stream_id, 7)
                    self.transmit()
                case "Truncate":
                    await queue.get()
                    self._send(stream_id, b"", end=True)
                case "Drop":
                    await queue.get()
                    self.close(error_code=0x101, reason_phrase="dropped")
                case "Flood":
                    await queue.get()
                    await self._flood(call)
                case _:  # Hang
                    await self.server.changed.wait_until(lambda: call.ended_by_client, 60)
        except Exception:
            # The stream is gone, as when the client reset it.
            pass

    async def _flood(self, call: ServerCall) -> None:
        payload = _message_frame("x" * FLOOD_MESSAGE)
        streams = self._quic._streams  # pyright: ignore[reportPrivateUsage]
        while not call.ended_by_client and (stream := streams.get(call.stream_id)) is not None:
            sender = stream.sender
            # Keep a few messages queued past what the client allows.
            if sender._buffer_stop - sender.highest_offset < 4 * len(payload):  # pyright: ignore[reportPrivateUsage]
                self._send(call.stream_id, payload)
                self.server.changed.notify()
                continue
            if sender.highest_offset >= stream.max_stream_data_remote and call.blocked_at is None:
                call.blocked_at = sender.highest_offset
                self.server.changed.notify()
            credit = stream.max_stream_data_remote

            def moved(
                stream: QuicStream = stream, sent: int = sender.highest_offset, credit: int = credit
            ) -> bool:
                """More was sent (the congestion window opened), the client granted more credit,
                or it stopped the stream."""
                return (
                    call.ended_by_client
                    or stream.sender.highest_offset != sent
                    or stream.max_stream_data_remote != credit
                )

            await self.server.changed.wait_until(moved, 60)
            if stream.max_stream_data_remote != credit:
                call.blocked_at = None

    def sent(self, stream_id: int) -> int:
        """The bytes of a stream the server has sent, which the client's credit bounds."""
        stream = self._quic._streams.get(stream_id)  # pyright: ignore[reportPrivateUsage]
        return 0 if stream is None else stream.sender.highest_offset


@dataclass
class TestServer:
    __test__ = False

    port: int = 0
    ca: str = ""
    calls: list[ServerCall] = field(default_factory=list[ServerCall])
    protocols: list[_Protocol] = field(default_factory=list[_Protocol])
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
    """Starts the server on a loopback port; `max_streams` lowers how many bidirectional streams
    the client may have open at once (aioquic's default is 128)."""
    directory = tempfile.TemporaryDirectory()
    cert, key = _certificate(Path(directory.name))
    configuration = QuicConfiguration(is_client=False, alpn_protocols=["trevrpc/1"])
    configuration.load_cert_chain(str(cert), str(key))  # pyright: ignore[reportUnknownMemberType]
    test_server = TestServer(ca=str(cert))

    def create_protocol(*args: object, **kwargs: object) -> _Protocol:
        protocol = _Protocol(*args, **kwargs)
        protocol.server = test_server
        if max_streams is not None:
            # Read into the transport parameters at the handshake, which comes later.
            limit = protocol._quic._local_max_streams_bidi  # pyright: ignore[reportPrivateUsage]
            limit.value = limit.sent = max_streams
        test_server.protocols.append(protocol)
        return protocol

    server = await serve(
        "127.0.0.1", 0, configuration=configuration, create_protocol=create_protocol
    )
    sockname = server._transport.get_extra_info("sockname")  # pyright: ignore[reportPrivateUsage, reportOptionalMemberAccess]
    test_server.port = cast(tuple[str, int], sockname)[1]

    def close() -> None:
        server.close()
        directory.cleanup()

    test_server.close = close
    return test_server
