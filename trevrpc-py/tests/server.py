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
- `/test.Echo/Invalid` fails with `INVALID_ARGUMENT` and a `buf.validate.Violations` detail, as a
  validating server rejects a request: rule `string.min_len` on the field `name`.

Compression follows the server side of the protocol: it answers dictionary queries from its own
dictionaries (which compress `google.protobuf.StringValue`), decodes every compressed request,
compresses responses as the call accepts, accepts optimistic calls at dispatch, and records each
message's frame type. Methods for compression:

- `/test.Echo/Refuse` is `ClientStream` registered without stream compression: it refuses
  optimistic calls.
- `/test.Echo/AnswerLater` is `ClientStream`, holding its ANSWER until `TestServer.answer_later`
  is set.
- `/test.Echo/NoAnswer` is `ClientStream` that sends HEADERS first and never answers.
- `/test.Echo/BadZstd` and `/test.Echo/TooLarge` respond with an invalid ZSTD_MESSAGE, and one
  declaring more than 4 MiB.
- `/test.Echo/UnaskedStream` sends a ZSTD_STREAM_MESSAGE on a call that did not enable it.
- `/test.Echo/RefuseLater` is `ClientStream` registered without stream compression. Over QMux, it
  reads nothing of an optimistic call until `TestServer.refuse_now` is set, then refuses it and
  discards its requests.
- `/test.Echo/AnswerEarly` and `/test.Echo/StopEarly` are streaming methods registered
  without stream compression that refuse an optimistic call once `TestServer.refuse_after`
  requests arrived. Over QMux, on the replay, they stop reading requests after the first:
  `AnswerEarly` responds `early` and OK, and `StopEarly` stops the request stream, then fails
  with `FAILED_PRECONDITION` "stopped".
- `/test.Echo/BoundedEcho` is `Bidi` registered without stream compression. It refuses an
  optimistic call only once `TestServer.refuse_after` requests have arrived, so the replay carries
  them all, and over QMux it stops reading requests while more than 4 KiB of its echoes are unsent,
  as a server applying backpressure does.

Datagrams, on native QUIC: `/test.Echo/Unreliable` is `Bidi` with both directions unreliable. On a
call with a datagram flow it sends each echo as a datagram, after HEADERS, and ends with
DATAGRAM_COUNT; its OK status carries trailing metadata `received` and `datagrams`, the requests
it received and how many came as datagrams. `TestServer.datagram_before_headers` sends the first
echo before HEADERS, `TestServer.malformed_datagrams` sends two malformed datagrams first, and
`TestServer.datagram_count_extra` announces more datagrams than were sent.

Connection metadata follows the server side of the protocol: each connection stores up to 4 offered
sets, merges the set an offer or a reference names into the call's metadata (the call's own entries
winning), confirms a stored offer in the call's ANSWER, and fails a reference to a set it does not
hold with `INVALID_ARGUMENT`. Each call records what its REQUEST carried.

Tests wait on what the server saw through `Signal`s, never by sleeping.
"""

from __future__ import annotations

import asyncio
import contextlib
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
from aioquic.quic.exceptions import DatagramFrameCapacityError
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.x509.oid import NameOID
from protobuf.wkt.google.protobuf.wrappers_pb import StringValue
from trevrpc import TrevRpcError
from trevrpc._datagram import datagram_frames
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
from trevrpc._varint import decode_varint, encode_varint
from trevrpc._wire import RpcHeaders, _len_field, _map_field, _varint_field, read_fields
from trevrpc._zstd import (
    CompressionError,
    LoadedDictionary,
    MessageCompressor,
    StreamCompressor,
    StreamDecompressor,
    decompress_message,
)

FLOOD_MESSAGE = 64 * 1024
MAX_FRAME = 4 * 1024 * 1024
BAD_ZSTD = bytes.fromhex("2101478d0000203238397d047c00cac4a5bd2f4f41")  # truncated
TOO_LARGE_ZSTD = bytes.fromhex("e0010040000001000000")  # declares 4 MiB + 1 bytes
UNASKED_STREAM = bytes.fromhex(
    "404700300c020072440f15b037070d68b79c3112489a6c69c9831d33539f4c558931077d0238d2d2cdc1870c42bf6dee7d3e659f5ade8c5e850a94f92c24967aa3352983e98d0400"
)
_REFUSE_LATER = ("BoundedEcho", "RefuseLater", "AnswerEarly", "StopEarly")
_EARLY = ("AnswerEarly", "StopEarly")
REFUSAL = encode_frame(FrameType.STATUS, _varint_field(1, 8) + _varint_field(4, 1))
ECHO_UNSENT_LIMIT = 4 * 1024
_MESSAGE_KINDS = (FrameType.MESSAGE, FrameType.ZSTD_MESSAGE, FrameType.ZSTD_STREAM_MESSAGE)
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
    code: int = 0,
    message: str = "",
    metadata: dict[str, bytes] | None = None,
    details: tuple[tuple[str, bytes], ...] = (),
) -> bytes:
    payload = (
        _varint_field(1, code)
        + (_len_field(2, message.encode()) if message else b"")
        + _map_field(3, metadata or {})
        + b"".join(
            _len_field(5, _len_field(1, name.encode()) + _len_field(2, value))
            for name, value in details
        )
    )
    return encode_frame(FrameType.STATUS, payload)


def _violations() -> bytes:
    """An encoded `buf.validate.Violations`, built with the tests' own bindings rather than
    protovalidate's: rule `string.min_len` on the field `name`."""
    from .gen.buf.validate.validate_pb import FieldPath, FieldPathElement, Violation, Violations

    violation = Violation(
        field=FieldPath(elements=[FieldPathElement(field_number=1, field_name="name")]),
        rule_id="string.min_len",
        message="must be at least 1 characters",
    )
    return Violations(violations=[violation]).to_binary()


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
    kind: int = 0
    accept: list[int] = field(default_factory=list[int])
    query: list[tuple[int, bytes]] = field(default_factory=list[tuple[int, bytes]])
    """The REQUEST's dictionary_query, as (ID, fingerprint)."""
    stream_compression: int = 0
    request_frames: list[int] = field(default_factory=list[int])
    """The frame type of each request message."""
    response_frames: list[int] = field(default_factory=list[int])
    flow: int = 0
    """The REQUEST's datagram_flow."""
    connection_metadata: tuple[int, dict[str, bytes]] | None = None
    """The REQUEST's connection_metadata: the set's ID, and its entries in an offer."""
    stored_metadata: int = 0
    """The offered set the server stored, which its ANSWER confirms."""
    datagram_requests: int = 0
    """Request messages that came as datagrams."""
    client_count: int = 0
    """The request stream's DATAGRAM_COUNT."""
    datagrams_received: int = 0
    datagrams_sent: int = 0
    refused: bool = False
    refusing: bool = False
    """Refuses once enough requests arrived (`BoundedEcho`)."""
    dictionaries: dict[int, LoadedDictionary] = field(default_factory=dict[int, LoadedDictionary])
    _inflater: StreamDecompressor | None = None
    _deflater: StreamCompressor | None = None

    @property
    def ended_by_client(self) -> bool:
        return self.reset_code is not None or self.stop_code is not None

    @property
    def name(self) -> str:
        return self.method.rsplit("/", 1)[-1]

    @classmethod
    def from_request(cls, stream_id: int, payload: bytes, server: TestServer) -> ServerCall:
        call = cls(stream_id, dictionaries=server.dictionaries)
        for number, _, value in read_fields(payload):
            if number == 1 and isinstance(value, bytes):
                call.method = value.decode()
            elif number == 2 and isinstance(value, bytes):
                call.metadata.update(RpcHeaders.decode(_len_field(1, value)).metadata)
            elif number == 3 and isinstance(value, int):
                call.kind = value
            elif number == 4 and isinstance(value, int):
                call.timeout_nanos = value
            elif number == 5 and isinstance(value, int):
                call.flow = value
            elif number == 6:
                if isinstance(value, int):
                    call.accept.append(value)
                else:
                    pos = 0
                    while (decoded := decode_varint(value, pos)) is not None:
                        call.accept.append(decoded[0])
                        pos = decoded[1]
            elif number == 7 and isinstance(value, bytes):
                info_id, fingerprint = 0, b""
                for info_number, _, item in read_fields(value):
                    if info_number == 1 and isinstance(item, int):
                        info_id = item
                    elif info_number == 2 and isinstance(item, bytes):
                        fingerprint = item
                call.query.append((info_id, fingerprint))
            elif number == 8 and isinstance(value, int):
                call.stream_compression = value
            elif number == 9 and isinstance(value, bytes):
                set_id, entries = 0, b""
                for set_number, _, item in read_fields(value):
                    if set_number == 1 and isinstance(item, int):
                        set_id = item
                    elif set_number == 2 and isinstance(item, bytes):
                        entries += _len_field(1, item)
                call.connection_metadata = (set_id, RpcHeaders.decode(entries).metadata)
        return call

    # The server side of compression

    def _confirmed(self) -> dict[int, LoadedDictionary | None]:
        """The queried dictionaries the server holds with the same fingerprint, and 0."""
        confirmed: dict[int, LoadedDictionary | None] = {}
        for info_id, fingerprint in self.query:
            if info_id == 0 and not fingerprint:
                confirmed[0] = None
            elif (d := self.dictionaries.get(info_id)) is not None and d.fingerprint == fingerprint:
                confirmed[info_id] = d
        return confirmed

    @property
    def _stream_requests(self) -> bool:
        return (
            self.stream_compression != 0
            and self.kind in (1, 3)
            and self.name != "Refuse"
            and self.name not in _REFUSE_LATER
        )

    def opening(self) -> bytes:
        """What the server sends at dispatch: a refusal, an ANSWER, or nothing."""
        if self.stream_compression == 2 and self.name == "Refuse":
            self.refused = True
            return REFUSAL
        if self.stream_compression == 2 and self.name in _REFUSE_LATER:
            self.refusing = True
            return b""
        if self.name in ("AnswerLater", "NoAnswer"):
            return b""
        return self.answer()

    def answer(self) -> bytes:
        if not self.query and not self._stream_requests and not self.stored_metadata:
            return b""
        payload = b"".join(
            _len_field(1, _varint_field(1, i) + (_len_field(2, d.fingerprint) if d else b""))
            for i, d in self._confirmed().items()
        )
        if self._stream_requests:
            payload += _varint_field(2, 1)
        if self.stored_metadata:
            payload += _varint_field(3, self.stored_metadata)
        return encode_frame(FrameType.ANSWER, payload)

    def resolve_metadata(self, store: dict[int, dict[str, bytes]]) -> bool:
        """Stores an offered set on the connection and merges the set into the call's metadata;
        `False` for a reference to a set the connection does not hold."""
        if self.connection_metadata is None:
            return True
        set_id, entries = self.connection_metadata
        if entries:
            store.pop(set_id, None)
            if len(store) == 4:
                del store[next(iter(store))]
            store[set_id] = entries
            self.stored_metadata = set_id
        elif set_id in store:
            entries = store[set_id]
        else:
            return False
        self.metadata = {**entries, **self.metadata}
        return True

    def _lookup(self, dictionary_id: int) -> LoadedDictionary | bool | None:
        if dictionary_id == 0:
            return None
        return self.dictionaries.get(dictionary_id, False)

    def receive(self, frame_type: int, payload: bytes) -> str:
        """A request message's value; raises `CompressionError`."""
        self.request_frames.append(frame_type)
        if frame_type == FrameType.ZSTD_MESSAGE:
            payload = decompress_message(payload, self._lookup, MAX_FRAME)  # pyright: ignore[reportArgumentType]
        elif frame_type == FrameType.ZSTD_STREAM_MESSAGE:
            if not self._stream_requests:
                raise CompressionError("invalid", "stream compression not enabled")
            if self._inflater is None:
                self._inflater = StreamDecompressor(self._lookup)  # pyright: ignore[reportArgumentType]
            payload = self._inflater.decompress(payload, MAX_FRAME)
        return StringValue.from_binary(payload).value

    def message(self, value: str) -> bytes:
        """A response message's frame, compressed as the call accepts."""
        payload = StringValue(value=value).to_binary()
        frame = encode_frame(FrameType.MESSAGE, payload)
        usable = self._confirmed() if self.query else {0: None, **self.dictionaries}
        accepted = [i for i in self.accept if i in usable]
        streams = self.stream_compression != 0 and self.kind in (2, 3)
        if streams and len(self.response_frames) >= 2:
            if self._deflater is None:
                seed = next((usable[i] for i in accepted if i), None)
                self._deflater = StreamCompressor(seed, 1)
            frame = encode_frame(FrameType.ZSTD_STREAM_MESSAGE, self._deflater.compress(payload))
        elif dictionary := next((usable[i] for i in accepted if i), None):
            compressed = MessageCompressor(dictionary, 1).compress(payload)
            if compressed is not None:
                frame = encode_frame(FrameType.ZSTD_MESSAGE, compressed)
        elif 0 in accepted and len(payload) >= 256:
            compressed = MessageCompressor(None, 1).compress(payload)
            if compressed is not None:
                frame = encode_frame(FrameType.ZSTD_MESSAGE, compressed)
        self.response_frames.append(frame[0])
        return frame


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

    async def flushed(self) -> None:
        """Waits until what was sent so far has gone out."""
        ...

    def send_datagram(self, payload: bytes) -> bool:
        """Sends a datagram of the call's flow; `False` without datagrams."""
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
                    frames = _headers_frame({"h": b"1"}) + call.message(value)
                    wire.send(frames + _status_frame(0, "", {"t": b"2"}), end=True)
                else:
                    suffix = call.metadata.get("suffix", b"").decode()
                    wire.send(call.message(value + suffix) + _status_frame(), end=True)
            case "Invalid":
                await queue.get()
                message = "validation error: name: must be at least 1 characters"
                detail = ("buf.validate.Violations", _violations())
                wire.send(_status_frame(3, message, details=(detail,)), end=True)
            case "ServerStream":
                value = await queue.get()
                assert value is not None
                for i in range(int(value)):
                    wire.send(call.message(str(i)))
                wire.send(_status_frame(), end=True)
            case "ClientStream" | "Refuse" | "RefuseLater" | "AnswerLater" | "NoAnswer":
                if call.name == "AnswerLater":
                    await server.answer_later.wait()
                    wire.send(call.answer())
                elif call.name == "NoAnswer":
                    wire.send(_headers_frame({}))
                values: list[str] = []
                while (value := await queue.get()) is not None:
                    values.append(value)
                wire.send(call.message("".join(values)) + _status_frame(), end=True)
            case "AnswerEarly":
                await queue.get()
                wire.send(call.message("early") + _status_frame(), end=True)
            case "StopEarly":
                await queue.get()
                wire.stop(0)
                # In a later record: the client sees its requests stopped before the status.
                await wire.flushed()
                wire.send(_status_frame(9, "stopped"), end=True)
            case "BadZstd" | "TooLarge":
                await queue.get()
                payload = BAD_ZSTD if call.name == "BadZstd" else TOO_LARGE_ZSTD
                wire.send(encode_frame(FrameType.ZSTD_MESSAGE, payload) + _status_frame(), end=True)
            case "UnaskedStream":
                await queue.get()
                frame = encode_frame(FrameType.ZSTD_STREAM_MESSAGE, UNASKED_STREAM)
                wire.send(frame + _status_frame(), end=True)
            case "Unreliable":
                await _unreliable(server, call, wire, queue)
            case "Bidi" | "BoundedEcho":
                while (value := await queue.get()) is not None:
                    if value.startswith("fail:"):
                        _, code, message = value.split(":", 2)
                        # One packet, as Go and Rust servers send them.
                        wire.send(_status_frame(int(code), message), end=True)
                        wire.stop(0)
                        return
                    wire.send(call.message(value))
                wire.send(_status_frame(), end=True)
            case "FloodFail":
                await queue.get()
                frames = b"".join(call.message(str(i)) for i in range(40))
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


async def _unreliable(
    server: TestServer, call: ServerCall, wire: _Wire, queue: asyncio.Queue[str | None]
) -> None:
    received = 0
    headers_sent = False

    def headers() -> None:
        nonlocal headers_sent
        if not headers_sent:
            headers_sent = True
            wire.send(_headers_frame({}))

    if server.malformed_datagrams and call.flow:
        headers()
        wire.send_datagram(b"\x00\x05ab")  # a MESSAGE frame longer than the datagram
        wire.send_datagram(encode_frame(FrameType.ZSTD_STREAM_MESSAGE, UNASKED_STREAM))
    while (value := await queue.get()) is not None:
        received += 1
        frame = call.message(value)
        if not server.datagram_before_headers:
            headers()
        if not wire.send_datagram(frame):
            headers()
            wire.send(frame)
        headers()
    # Datagrams the client announced may still be on their way.
    with contextlib.suppress(TimeoutError):
        await server.changed.wait_until(lambda: call.datagrams_received >= call.client_count, 1)
    received = len(call.request_frames)
    trailers = {
        "received": str(received).encode(),
        "datagrams": str(call.datagram_requests).encode(),
    }
    count = call.datagrams_sent + server.datagram_count_extra
    frames = encode_frame(FrameType.DATAGRAM_COUNT, encode_varint(count)) if count else b""
    wire.send(frames + _status_frame(0, "", trailers), end=True)


def _dispatch(
    server: TestServer,
    call: ServerCall,
    wire: _Wire,
    queue: asyncio.Queue[str | None],
    metadata_sets: dict[int, dict[str, bytes]],
) -> None:
    """Starts a call: its connection metadata, its refusal or ANSWER at dispatch, then its
    handler."""
    server.calls.append(call)
    if not call.resolve_metadata(metadata_sets):
        call.refused = True
        wire.send(_status_frame(3, "unknown connection metadata set"), end=True)
        return
    opening = call.opening()
    if call.refusing:
        return
    if call.refused:
        # A lone STATUS; the rest of the request stream is discarded.
        wire.send(opening, end=True)
        return
    if opening:
        wire.send(opening)
    asyncio.get_running_loop().create_task(_handle(server, call, wire, queue))


def _receive(
    server: TestServer,
    call: ServerCall,
    frame_type: int,
    payload: bytes,
    queue: asyncio.Queue[str | None],
    wire: _Wire,
) -> None:
    if call.refusing:
        call.request_frames.append(frame_type)
        if len(call.request_frames) >= server.refuse_after:
            call.refusing = False
            call.refused = True
            wire.send(REFUSAL, end=True)
        return
    if call.refused:
        call.request_frames.append(frame_type)
        return
    try:
        queue.put_nowait(call.receive(frame_type, payload))
    except CompressionError as error:
        call.refused = True
        wire.send(_status_frame(3, str(error)), end=True)


async def _flood(server: TestServer, call: ServerCall, wire: _Wire) -> None:
    payload = call.message("x" * FLOOD_MESSAGE)
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

    async def flushed(self) -> None:
        self._protocol.transmit()

    def send_datagram(self, payload: bytes) -> bool:
        call = self._protocol._calls.get(self._id)
        if call is None or not call.flow:
            return False
        try:
            self._protocol._quic.send_datagram_frame(encode_varint(call.flow) + payload)
        except DatagramFrameCapacityError:
            return False
        self._protocol._transmit_soon()
        call.datagrams_sent += 1
        return True

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
        self._flows: dict[int, int] = {}
        """Stream IDs by datagram flow."""
        self.metadata_sets: dict[int, dict[str, bytes]] = {}
        self._early: list[bytes] = []
        """Datagrams that overtook their REQUEST."""

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
            case events.DatagramFrameReceived(data=data):
                self._datagram(data)
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

    def _datagram(self, data: bytes) -> None:
        decoded = decode_varint(data, 0)
        if decoded is None:
            return
        flow, pos = decoded
        stream_id = self._flows.get(flow)
        if stream_id is None:
            self._early.append(data)
            return
        call = self._calls[stream_id]
        call.datagrams_received += 1
        frames = datagram_frames(data[pos:])
        for frame_type, payload in frames or ():
            call.datagram_requests += 1
            _receive(
                self.server,
                call,
                frame_type,
                payload,
                self._queues[stream_id],
                _QuicWire(self, stream_id),
            )

    def _frame(self, stream_id: int, frame_type: int, payload: bytes) -> None:
        if frame_type == FrameType.DATAGRAM_COUNT:
            count = decode_varint(payload, 0)
            if (call := self._calls.get(stream_id)) is not None and count is not None:
                call.client_count = count[0]
        if frame_type == FrameType.REQUEST:
            call = ServerCall.from_request(stream_id, payload, self.server)
            queue: asyncio.Queue[str | None] = asyncio.Queue()
            self._calls[stream_id] = call
            self._queues[stream_id] = queue
            _dispatch(self.server, call, _QuicWire(self, stream_id), queue, self.metadata_sets)
            if call.flow:
                self._flows[call.flow] = stream_id
                early, self._early = self._early, []
                for data in early:
                    self._datagram(data)
        elif frame_type in _MESSAGE_KINDS:
            _receive(
                self.server,
                self._calls[stream_id],
                frame_type,
                payload,
                self._queues[stream_id],
                _QuicWire(self, stream_id),
            )

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

    async def flushed(self) -> None:
        session = self._session
        await session.server.changed.wait_until(lambda: not session._control)

    def send_datagram(self, payload: bytes) -> bool:
        return False


class _QMuxServerSession(QMuxSession):
    """The server role, which notifies the test server of every record and records how the client
    ended each call."""

    server: TestServer
    calls: dict[int, ServerCall]
    metadata_sets: dict[int, dict[str, bytes]]

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
                    call = ServerCall.from_request(stream.id, payload, self.server)
                    self.calls[stream.id] = call
                    wire = _QMuxWire(self, stream)
                    _dispatch(self.server, call, wire, queue, self.metadata_sets)
                    if call.refusing and call.name == "RefuseLater":
                        # Reads nothing, then refuses when told to, and discards the requests
                        # instead of reading them: the client's writes wait until it replays.
                        await self.server.refuse_now.wait()
                        call.refusing, call.refused = False, True
                        wire.send(REFUSAL, end=True)
                        return
                elif frame_type in _MESSAGE_KINDS:
                    call = self.calls[stream.id]
                    wire = _QMuxWire(self, stream)
                    _receive(self.server, call, frame_type, payload, queue, wire)
                    if call.name in _EARLY and not (call.refused or call.refusing):
                        # Reads nothing more: the handler answers the first request.
                        return
                    if call.name == "BoundedEcho" and not (call.refused or call.refusing):
                        # Backpressure: no more requests while the echoes are not taken. The
                        # handler, another task, echoes the request first.
                        while not queue.empty():
                            await asyncio.sleep(0)
                        await self.server.changed.wait_until(
                            lambda wire=wire: (wire.flow() or (0,))[0] <= ECHO_UNSENT_LIMIT, 60
                        )
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
    dictionaries: dict[int, LoadedDictionary] = field(default_factory=dict[int, LoadedDictionary])
    answer_later: asyncio.Event = field(default_factory=asyncio.Event)
    refuse_after: int = 1
    datagram_before_headers: bool = False
    malformed_datagrams: bool = False
    datagram_count_extra: int = 0
    refuse_now: asyncio.Event = field(default_factory=asyncio.Event)
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


async def start_server(
    *,
    max_streams: int | None = None,
    dictionaries: tuple[LoadedDictionary, ...] = (),
    qmux_stream_window: int = 4 * 1024 * 1024 + 16,
) -> TestServer:
    """Starts the server on loopback ports; `max_streams` lowers how many bidirectional streams
    the client may have open at once (128 by default), and `qmux_stream_window` how much of a
    request stream the QMux server receives before it reads."""
    directory = tempfile.TemporaryDirectory()
    cert, key = _certificate(Path(directory.name))
    test_server = TestServer(ca=str(cert), dictionaries={d.id: d for d in dictionaries})

    configuration = QuicConfiguration(
        is_client=False, alpn_protocols=["trevrpc/1"], max_datagram_frame_size=65535
    )
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
        initial_max_stream_data_bidi_remote=qmux_stream_window,
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
        session.metadata_sets = {}
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
