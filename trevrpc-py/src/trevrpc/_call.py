"""Calls: the request and response streams of one RPC on one transport stream."""

from __future__ import annotations

import asyncio
import contextlib
import math
from collections import deque
from collections.abc import AsyncIterable, AsyncIterator, Callable, Iterable
from dataclasses import dataclass, replace
from types import TracebackType
from typing import TYPE_CHECKING, Any, Protocol, Self, TypedDict

from protobuf import Message

from ._compression import CallCompression, Compression, StreamBudget
from ._datagram import HELD_BYTES, HELD_MESSAGES, DatagramFlow
from ._frames import FrameType, encode_frame
from ._metadata import Metadata, MetadataInput, normalize_metadata, validate_metadata
from ._method import Method, MethodKind
from ._status import Code, TrevRpcError
from ._varint import decode_varint
from ._wire import RpcAnswer, RpcHeaders, RpcRequest, RpcStatus, StreamCompression, WireError

if TYPE_CHECKING:
    from ._stream import Stream

_MAX_TIMEOUT_NANOS = (1 << 64) - 1

_READ_AHEAD_MESSAGES = 32
_READ_AHEAD_BYTES = 1024 * 1024
"""A streaming call reads at most this far ahead of its caller, plus one message."""


class CallOptions(TypedDict, total=False):
    """Options of one call, passed to a generated client's methods as keyword arguments."""

    timeout: float
    """Deadline for the whole call in seconds, from its start to its final status."""
    metadata: MetadataInput
    """Request metadata. Keys are lowercased; the `trevrpc-` prefix is reserved."""
    max_frame_size: int
    """Largest frame accepted or sent, in bytes: from 1 up to the channel's `max_frame_size`, which
    is the default (4 MiB). The channel's sizes the connection's receive windows, so a larger value
    raises `INVALID_ARGUMENT`."""
    on_response_headers: Callable[[Metadata], None]
    """Called with the response headers, empty when the server sent none, as soon as they, the
    first message or the final status arrive. A callback that raises fails the call."""
    on_response_metadata: Callable[[Metadata], None]
    """Called with the trailing metadata of a successful call's final status, before its result is
    returned or its responses end. A failed call reports its metadata on the `TrevRpcError`."""


@dataclass(frozen=True, slots=True)
class Status:
    """A call's final OK status."""

    code: Code
    message: str
    metadata: Metadata


class StreamOpener(Protocol):
    """What a call needs of a channel."""

    max_frame_size: int
    compression: Compression
    stream_budget: StreamBudget | None

    async def open_stream(self, max_frame_size: int) -> Stream: ...


def _prepare(
    method: Method[Message, Message], options: CallOptions
) -> tuple[RpcRequest, float | None]:
    """The call's REQUEST, before its negotiation fields, and its deadline (on the event loop's
    clock)."""
    metadata = normalize_metadata(options.get("metadata"))
    timeout = options.get("timeout")
    timeout_nanos = 0
    deadline = None
    if timeout is not None:
        if not (timeout >= 0 and math.isfinite(timeout)):
            raise TrevRpcError(
                Code.INVALID_ARGUMENT, "RPC timeout must be a non-negative finite number of seconds"
            )
        timeout_nanos = max(1, math.ceil(timeout * 1e9))
        if timeout_nanos > _MAX_TIMEOUT_NANOS:
            raise TrevRpcError(
                Code.INVALID_ARGUMENT, "RPC timeout does not fit in 64-bit nanoseconds"
            )
        deadline = asyncio.get_running_loop().time() + timeout
    return RpcRequest(method.path, metadata, method.kind, timeout_nanos), deadline


def _negotiate(
    channel: StreamOpener,
    stream: Stream,
    method: Method[Message, Message],
    request: RpcRequest,
    flow: DatagramFlow | None = None,
) -> tuple[CallCompression, RpcRequest]:
    """The call's compression on the stream's connection, and its REQUEST with the negotiation
    fields and its datagram flow."""
    compression = CallCompression(
        channel.compression,
        stream.compression,
        channel.stream_budget,
        method,
        datagrams=flow is not None,
    )
    request = replace(
        request,
        accept_dictionaries=compression.accept_dictionaries,
        dictionary_query=compression.dictionary_query,
        stream_compression=compression.stream_compression,
        datagram_flow=flow.id if flow is not None else 0,
    )
    return compression, request


def _request_frame(request: RpcRequest) -> bytes:
    return encode_frame(FrameType.REQUEST, request.encode())


def _frame_limit(channel: StreamOpener, options: CallOptions) -> int:
    """The call's frame size limit. It cannot exceed the channel's, which sizes the connection's
    stream receive windows: a larger frame could never arrive in full."""
    limit = options.get("max_frame_size")
    if limit is None:
        return channel.max_frame_size
    if not 0 < limit <= channel.max_frame_size:
        raise TrevRpcError(
            Code.INVALID_ARGUMENT,
            f"max_frame_size must be between 1 and the channel's {channel.max_frame_size} bytes",
        )
    return limit


def _serialize(message: Message, max_frame_size: int) -> bytes:
    """A request message's bytes, which its frame then carries, compressed or not."""
    try:
        payload = message.to_binary()
    except Exception as error:
        raise TrevRpcError(
            Code.INVALID_ARGUMENT, f"failed to encode request message: {error}"
        ) from error
    if len(payload) > max_frame_size:
        raise TrevRpcError(
            Code.RESOURCE_EXHAUSTED,
            f"request message of {len(payload)} bytes exceeds the {max_frame_size}-byte limit",
        )
    return payload


def _deadline_exceeded() -> TrevRpcError:
    return TrevRpcError(Code.DEADLINE_EXCEEDED, "deadline exceeded")


def _cancelled() -> TrevRpcError:
    return TrevRpcError(Code.CANCELLED, "call cancelled")


class _Response:
    """The response stream's grammar: `[ANSWER] [HEADERS] MESSAGE* [DATAGRAM_COUNT] STATUS`."""

    __slots__ = (
        "_answered",
        "_compression",
        "_counted",
        "_kind",
        "_max_size",
        "_on_headers",
        "_on_metadata",
        "_seen",
        "answer",
        "datagram_count",
        "headers",
        "messages",
        "status",
    )

    def __init__(
        self, kind: MethodKind, options: CallOptions, compression: CallCompression, max_size: int
    ) -> None:
        self._kind = kind
        self._compression = compression
        self._max_size = max_size
        self.answer: RpcAnswer | None = None
        self.datagram_count = 0
        """The datagrams the server sent, from DATAGRAM_COUNT."""
        self._on_headers = options.get("on_response_headers")
        self._on_metadata = options.get("on_response_metadata")
        self._seen = False
        self._answered = False
        self._counted = False
        self.headers: Metadata | None = None
        self.messages = 0
        self.status: RpcStatus | None = None

    def handle(self, frame_type: int, payload: bytes) -> bytes | None:
        """Takes one known frame; returns a message's payload, decompressed. Raises an `INTERNAL`
        `TrevRpcError` for a frame the grammar does not allow."""
        if self.status is not None:
            raise TrevRpcError(Code.INTERNAL, f"frame of type {frame_type} after STATUS")
        try:
            match frame_type:
                case FrameType.MESSAGE | FrameType.ZSTD_MESSAGE | FrameType.ZSTD_STREAM_MESSAGE:
                    if self._counted:
                        raise TrevRpcError(Code.INTERNAL, "MESSAGE after DATAGRAM_COUNT")
                    self._set_headers({})
                    self.messages += 1
                    if self.messages > 1 and self._kind in (
                        MethodKind.UNARY,
                        MethodKind.CLIENT_STREAMING,
                    ):
                        raise TrevRpcError(Code.INTERNAL, "more than one response message")
                    self._seen = True
                    return self._compression.decode(frame_type, payload, self._max_size)
                case FrameType.STATUS:
                    status = RpcStatus.decode(payload)
                    validate_metadata(status.metadata, Code.INTERNAL)
                    self._set_headers({})
                    self.status = status
                case FrameType.HEADERS:
                    if self.headers is not None:
                        raise TrevRpcError(Code.INTERNAL, "HEADERS after a message or HEADERS")
                    headers = RpcHeaders.decode(payload)
                    validate_metadata(headers.metadata, Code.INTERNAL)
                    self._seen = True
                    self._set_headers(headers.metadata)
                case FrameType.ANSWER:
                    if self._seen or self._answered:
                        raise TrevRpcError(Code.INTERNAL, "ANSWER after another frame")
                    answer = RpcAnswer.decode(payload)
                    self._answered = True
                    self.answer = answer
                    self._compression.learn(answer)
                case FrameType.DATAGRAM_COUNT:
                    if self._counted:
                        raise TrevRpcError(Code.INTERNAL, "second DATAGRAM_COUNT")
                    count = decode_varint(payload, 0)
                    if count is None or count[1] != len(payload):
                        raise TrevRpcError(Code.INTERNAL, "malformed DATAGRAM_COUNT")
                    self._counted = True
                    self._seen = True
                    self.datagram_count = count[0]
                case _:
                    raise TrevRpcError(Code.INTERNAL, f"frame of type {frame_type} on a response")
        except WireError as error:
            raise TrevRpcError(
                Code.INTERNAL, f"malformed frame of type {frame_type}: {error}"
            ) from error
        return None

    def _set_headers(self, headers: Metadata) -> None:
        if self.headers is None:
            self.headers = headers
            if self._on_headers is not None:
                self._on_headers(headers)

    def outcome(self) -> Status:
        """The call's OK status, or raises its error, once STATUS arrived."""
        status = self.status
        assert status is not None
        if status.code != Code.OK:
            raise TrevRpcError(status.code, status.message, status.metadata, status.details)
        if self._kind in (MethodKind.UNARY, MethodKind.CLIENT_STREAMING) and self.messages != 1:
            raise TrevRpcError(Code.INTERNAL, "OK status without a response message")
        if self._on_metadata is not None:
            self._on_metadata(status.metadata)
        return Status(Code.OK, status.message, status.metadata)


def _call_error(error: BaseException) -> TrevRpcError:
    """The call's error for an exception that ended a step of it."""
    if isinstance(error, TrevRpcError):
        return error
    if isinstance(error, asyncio.CancelledError):
        return _cancelled()
    return TrevRpcError.wrap(error)


def _refused(frame_type: int, payload: bytes) -> bool:
    """Whether the first frame of an optimistic call's response refuses it."""
    if frame_type != FrameType.STATUS:
        return False
    try:
        return RpcStatus.decode(payload).refused_stream_compression
    except WireError:
        return False


def _check_optimistic_answer(frame_type: int, payload: bytes) -> None:
    """An optimistic call's response starts with a STATUS, or an ANSWER accepting it."""
    if frame_type == FrameType.STATUS:
        return
    if frame_type == FrameType.ANSWER:
        try:
            if RpcAnswer.decode(payload).stream_compression:
                return
        except WireError:
            pass
    raise TrevRpcError(
        Code.INTERNAL,
        f"response to an optimistic call started with a frame of type {frame_type}, "
        "not an ANSWER accepting it",
    )


def _decode[T: Message](message_type: type[T], payload: bytes) -> T:
    try:
        return message_type.from_binary(payload)
    except Exception as error:
        raise TrevRpcError(Code.INTERNAL, f"failed to decode response message: {error}") from error


async def unary_call[Req: Message, Resp: Message](
    channel: StreamOpener, method: Method[Req, Resp], request: Req, options: CallOptions
) -> Resp:
    """Sends one request and returns the response."""
    base, deadline = _prepare(method, options)  # pyright: ignore[reportArgumentType]
    max_frame_size = _frame_limit(channel, options)
    payload = _serialize(request, max_frame_size)
    stream: Stream | None = None
    compression: CallCompression | None = None
    try:
        async with asyncio.timeout_at(deadline):
            stream = await channel.open_stream(max_frame_size)
            compression, full = _negotiate(channel, stream, method, base)  # pyright: ignore[reportArgumentType]
            frame = _request_frame(full) + compression.encode(payload)
            try:
                await stream.write(frame, end=True)
            except TrevRpcError:
                # A server that ends the call early stops the request after its STATUS, which
                # the response then reports.
                if not stream.stopped_by_peer:
                    raise
            response = _Response(method.kind, options, compression, max_frame_size)
            payload = b""
            while response.status is None:
                received = await stream.read_frame()
                if received is None:
                    raise TrevRpcError(Code.INTERNAL, "response stream ended before STATUS")
                message = response.handle(*received)
                if message is not None:
                    payload = message
            stream.detach()
            response.outcome()
            return _decode(method.response, payload)
    except TimeoutError:
        error = _deadline_exceeded()
        if stream is not None:
            stream.fail(error)
        raise error from None
    except asyncio.CancelledError:
        if stream is not None:
            stream.fail(_cancelled())
        raise
    except TrevRpcError as error:
        if stream is not None:
            stream.fail(error)
        raise
    except BaseException as error:
        wrapped = TrevRpcError.wrap(error)
        if stream is not None:
            stream.fail(wrapped)
        raise
    finally:
        if compression is not None:
            compression.close()


class _StreamingCall[Req: Message, Resp: Message]:
    """The shared state of a streaming call, whatever its kind.

    Opening the stream and writing the REQUEST start at once. While the call runs, its responses
    are read ahead into a bounded queue, so that the final status is seen even while nobody calls
    `recv()`; the queue pauses reading once it holds `_READ_AHEAD_MESSAGES` messages or
    `_READ_AHEAD_BYTES` bytes, which holds back the server through flow control.

    An optimistic call (see `CallCompression`) keeps its requests until the server answers. If the
    server refuses it, the call is replayed once on a new stream without optimism: the REQUEST,
    with the time left of the deadline, then the kept requests. Sends made meanwhile succeed, since
    the replay carries them.
    """

    def __init__(
        self,
        channel: StreamOpener,
        method: Method[Req, Resp],
        options: CallOptions,
        first: Req | None,
    ) -> None:
        loop = asyncio.get_running_loop()
        self._loop = loop
        self._channel = channel
        self._method = method
        self._options = options
        self._max_frame_size = _frame_limit(channel, options)
        self._request, self._deadline = _prepare(method, options)  # pyright: ignore[reportArgumentType]
        first_payload = _serialize(first, self._max_frame_size) if first is not None else None
        self._compression: CallCompression | None = None
        self._response: _Response | None = None
        self._stream: Stream | None = None
        self._error: TrevRpcError | None = None
        self._queue: deque[tuple[Resp, int, bool]] = deque()
        """Responses read ahead: each message, its size, and whether it came in a datagram."""
        self._queued_bytes = 0
        self._flow: DatagramFlow | None = None
        self._held_datagrams = 0
        self._held_datagram_bytes = 0
        self._end: TrevRpcError | Status | None = None
        """What follows the queued messages: the OK status or the call's error."""
        self._outcome: asyncio.Future[Status] = loop.create_future()
        self._outcome.add_done_callback(_retrieve)
        self._headers: asyncio.Future[Metadata] = loop.create_future()
        self._headers.add_done_callback(_retrieve)
        self._readable: asyncio.Future[None] | None = None
        self._room: asyncio.Future[None] | None = None
        self._stall_waiters: list[asyncio.Future[None]] = []
        self._settled = asyncio.Event()
        """Set once an optimistic call was accepted or refused, or the call ended."""
        self._replaying: asyncio.Future[None] | None = None
        self._replay_task: asyncio.Task[None] | None = None
        self._generation = 0
        """Counts replays: a write to an older generation's stream was replayed."""
        self._deadline_timer = (
            loop.call_at(self._deadline, self._fail, _deadline_exceeded())
            if self._deadline
            else None
        )
        self._upload_ended = first is not None
        self._send_lock = asyncio.Lock()
        self._pump_task: asyncio.Task[None] | None = None
        self._started = loop.create_task(self._start(first_payload))
        # Its failure is the call's, which every operation reports.
        self._started.add_done_callback(_retrieve)

    async def _start(self, first: bytes | None) -> Stream:
        try:
            stream = await self._channel.open_stream(self._max_frame_size)
        except TrevRpcError as error:
            self._fail(error)
            raise
        if self._error is not None:
            stream.fail(self._error)
            raise self._error
        self._stream = stream
        flow = self._open_flow(stream)
        compression, self._request = _negotiate(
            self._channel,
            stream,
            self._method,  # pyright: ignore[reportArgumentType]
            self._request,
            flow,
        )
        self._compression = compression
        if compression.settled:
            self._settled.set()
        self._response = _Response(
            self._method.kind, self._options, compression, self._max_frame_size
        )
        frame = _request_frame(self._request)
        if first is not None:
            # Only server-streaming calls have a first request, and they are never optimistic.
            frame += compression.encode(first)
        try:
            await stream.write(frame, end=first is not None)
        except TrevRpcError as error:
            self._fail(self._upload_error(error))
            raise
        self._pump_task = self._loop.create_task(self._pump(stream))
        return stream

    def _open_flow(self, stream: Stream) -> DatagramFlow | None:
        """A datagram flow, when the schema marks a streamed direction unreliable and the
        connection has datagrams. Response datagrams are accepted whenever the responses stream."""
        method = self._method
        requests = method.kind in (MethodKind.CLIENT_STREAMING, MethodKind.BIDI_STREAMING)
        responses = method.kind in (MethodKind.SERVER_STREAMING, MethodKind.BIDI_STREAMING)
        send = method.unreliable_requests and requests
        if not (send or (method.unreliable_responses and responses)):
            return None
        flow = DatagramFlow(
            stream.connection,
            send=send,
            receive=responses,
            decode=self._decode_datagram_message,
            deliver=self._deliver_datagrams,
        )
        flow_id = stream.connection.open_flow(flow)
        if flow_id is None:
            return None
        flow.id = flow_id
        self._flow = flow
        return flow

    def _decode_datagram_message(self, frame_type: int, payload: bytes) -> bytes:
        compression = self._compression
        assert compression is not None
        return compression.decode(frame_type, payload, self._max_frame_size)

    def _deliver_datagrams(self, payloads: list[bytes]) -> None:
        """Queues datagram messages with the stream's, in arrival order, while the call holds
        fewer than `HELD_MESSAGES` and `HELD_BYTES` of them; the rest are dropped."""
        if self._end is not None or self._error is not None:
            return
        for payload in payloads:
            size = len(payload)
            if size > self._max_frame_size or (
                self._held_datagrams
                and (
                    self._held_datagrams >= HELD_MESSAGES
                    or self._held_datagram_bytes + size > HELD_BYTES
                )
            ):
                continue
            try:
                message = _decode(self._method.response, payload)
            except TrevRpcError as error:
                self._fail(error)
                return
            self._queue.append((message, size, True))
            self._queued_bytes += size
            self._held_datagrams += 1
            self._held_datagram_bytes += size
        self._wake_reader()

    async def _stream_ready(self) -> Stream:
        try:
            await asyncio.shield(self._started)
        except TrevRpcError:
            raise self._error or _cancelled() from None
        replaying = self._replaying
        if replaying is not None:
            await asyncio.shield(replaying)
        stream = self._stream
        assert stream is not None
        return stream

    async def _pump(self, stream: Stream) -> None:
        compression = self._compression
        assert compression is not None
        try:
            while True:
                received = await stream.read_frame()
                if received is None:
                    raise TrevRpcError(Code.INTERNAL, "response stream ended before STATUS")
                if not compression.settled:
                    if _refused(*received):
                        stream = await self._replay(stream)
                        continue
                    self._settle()
                    _check_optimistic_answer(*received)
                response = self._response
                assert response is not None
                payload = response.handle(*received)
                if self._headers.done() is False and response.headers is not None:
                    self._headers.set_result(response.headers)
                flow = self._flow
                if flow is not None and received[0] != FrameType.ANSWER:
                    # Datagram messages are delivered once the response's HEADERS were read.
                    flow.open()
                if payload is not None:
                    message = _decode(self._method.response, payload)
                    while (
                        self._queue
                        and (
                            len(self._queue) >= _READ_AHEAD_MESSAGES
                            or self._queued_bytes >= _READ_AHEAD_BYTES
                        )
                        and self._error is None
                    ):
                        for waiter in self._stall_waiters:
                            if not waiter.done():
                                waiter.set_result(None)
                        self._room = self._loop.create_future()
                        try:
                            await self._room
                        finally:
                            self._room = None
                    if self._error is not None:
                        return
                    self._queue.append((message, len(payload), False))
                    self._queued_bytes += len(payload)
                    self._wake_reader()
                if response.status is not None:
                    break
            if self._flow is not None:
                # The responses end once the datagrams DATAGRAM_COUNT announced arrived, or after
                # a short wait for them; later ones are dropped.
                await self._flow.await_count(response.datagram_count)
                self._flow.close()
                if self._error is not None:
                    return
            stream.detach()
            try:
                status = response.outcome()
            except TrevRpcError as error:
                self._finish(error)
            else:
                self._finish(status)
        except TrevRpcError as error:
            self._fail(error)
        except Exception as error:
            self._fail(TrevRpcError.wrap(error))

    def _settle(self) -> None:
        compression = self._compression
        if compression is not None:
            compression.settle()
        self._settled.set()

    async def _replay(self, refused: Stream) -> Stream:
        """Replays a refused optimistic call on a new stream of the same connection, once: the
        REQUEST without optimism, with the time left of the deadline, then the kept requests and
        the end of the request stream if it ended. Writes waiting on the refused stream fail, and
        their sends succeed once the replay is written.

        The replay is written beside the response reader, which returns the new stream at once: a
        server that echoes, and stops reading requests while its responses are not read, would
        otherwise stall the write until the deadline. A call never moves between connections, so
        one whose connection ended meanwhile fails with `UNAVAILABLE`."""
        compression = self._compression
        assert compression is not None
        replaying = self._loop.create_future()
        replaying.add_done_callback(_retrieve)
        self._replaying = replaying
        self._generation += 1
        kept = compression.refused()
        self._settled.set()
        refused.fail(_cancelled())
        try:
            stream = await self._channel.open_stream(self._max_frame_size)
            if self._error is not None:
                stream.fail(self._error)
                raise self._error
            if stream.compression is not compression.connection:
                error = TrevRpcError(
                    Code.UNAVAILABLE,
                    "transport unavailable: "
                    "the connection ended before a refused call was replayed",
                )
                stream.fail(error)
                raise error
            self._stream = stream
            request = replace(self._request, stream_compression=StreamCompression.ENABLED)
            if self._deadline is not None:
                remaining = self._deadline - self._loop.time()
                request = replace(request, timeout_nanos=max(1, math.ceil(remaining * 1e9)))
            self._request = request
            self._response = _Response(
                self._method.kind, self._options, compression, self._max_frame_size
            )
            frame = _request_frame(request) + b"".join(compression.encode_message(p) for p in kept)
        except BaseException as error:
            self._replayed(replaying, self._error or _call_error(error))
            raise
        self._replay_task = self._loop.create_task(self._write_replay(stream, frame, replaying))
        return stream

    async def _write_replay(
        self, stream: Stream, frame: bytes, replaying: asyncio.Future[None]
    ) -> None:
        try:
            await stream.write(frame, end=self._upload_ended)
        except BaseException as error:
            if isinstance(error, asyncio.CancelledError):
                failure = self._error or _cancelled()
                self._replayed(replaying, failure)
                self._fail(failure)
                raise
            if self._error is None and self._end is None and stream.stopped_by_peer:
                # The server stops reading requests after writing its status.
                await self._outcome_or_stall()
            if self._error is None and (self._end is not None or stream.stopped_by_peer):
                # The server ended the call, or stopped reading its requests: the responses
                # report how the call ended, after the ones already queued.
                self._replayed(replaying, self._upload_error(_call_error(error)))
                return
            failure = self._error or _call_error(error)
            self._replayed(replaying, failure)
            self._fail(failure)
            return
        self._replayed(replaying, None)

    def _replayed(self, replaying: asyncio.Future[None], error: TrevRpcError | None) -> None:
        """Ends a replay, waking the sends that wait for it."""
        if self._replaying is replaying:
            self._replaying = None
        if replaying.done():
            return
        if error is None:
            replaying.set_result(None)
        else:
            replaying.set_exception(error)

    def _finish(self, end: TrevRpcError | Status) -> None:
        """Ends the call with its final status, after the queued messages."""
        if self._error is not None or self._end is not None:
            return
        self._end = end
        self._cancel_timer()
        self._release()
        if isinstance(end, Status):
            self._outcome.set_result(end)
            upload_error = _upload_after_ok()
        else:
            self._outcome.set_exception(end)
            upload_error = end
        if not self._headers.done():
            self._headers.set_exception(upload_error)
        stream = self._stream
        if stream is not None and (not stream.send_done or self._replaying is not None):
            # A replay still being written is abandoned too: nobody needs its requests now.
            stream.reset_send(upload_error, abandon=True)
        self._wake_reader()

    def _fail(self, error: TrevRpcError) -> None:
        """Ends the call with `error`: the queued messages are dropped, and the stream is reset and
        stopped."""
        if self._error is not None:
            return
        self._error = error
        self._cancel_timer()
        self._release()
        if self._replaying is not None:
            self._replayed(self._replaying, error)
        self._queue.clear()
        self._queued_bytes = 0
        if self._end is None:
            self._end = error
            self._outcome.set_exception(error)
        if not self._headers.done():
            self._headers.set_exception(error)
        if self._stream is not None:
            self._stream.fail(error)
        self._wake_reader()
        if self._room is not None and not self._room.done():
            self._room.set_result(None)

    def _release(self) -> None:
        """Gives back what the call holds of the stream compression budget, and wakes sends
        waiting for room."""
        if self._compression is not None:
            self._compression.close()
        if self._flow is not None:
            self._flow.close()
        self._settled.set()

    def _cancel_timer(self) -> None:
        if self._deadline_timer is not None:
            self._deadline_timer.cancel()
            self._deadline_timer = None

    def _wake_reader(self) -> None:
        if self._readable is not None and not self._readable.done():
            self._readable.set_result(None)

    async def _outcome_or_stall(self) -> None:
        """Waits for the call's outcome after the server stopped the request stream, which it does
        after writing its STATUS. Reading ahead may be paused behind messages nobody received, and
        waiting for them would never end when the caller is the one to receive them, so the wait
        ends there too and `send()` raises `CANCELLED`; `send_all()` returns instead, leaving the
        status to the responses. The deadline ends the wait at the latest."""
        if self._end is not None or self._error is not None or self._room is not None:
            return
        stall = self._loop.create_future()
        self._stall_waiters.append(stall)
        try:
            await asyncio.wait((self._outcome, stall), return_when=asyncio.FIRST_COMPLETED)
        finally:
            self._stall_waiters.remove(stall)
            stall.cancel()

    def _upload_error(self, error: TrevRpcError) -> TrevRpcError:
        """What a failed write reports: the call's outcome once it has one."""
        if self._error is not None:
            return self._error
        if isinstance(self._end, TrevRpcError):
            return self._end
        if isinstance(self._end, Status):
            return _upload_after_ok()
        return error

    # Responses

    async def recv(self) -> Resp | None:
        """The next response message, or `None` after the final OK status."""
        while True:
            if self._queue:
                message, size, datagram = self._queue.popleft()
                self._queued_bytes -= size
                if datagram:
                    self._held_datagrams -= 1
                    self._held_datagram_bytes -= size
                self._take_room()
                return message
            end = self._end
            if end is not None:
                if isinstance(end, TrevRpcError):
                    raise end
                return None
            self._readable = self._loop.create_future()
            try:
                await self._readable
            finally:
                self._readable = None

    def _take_room(self) -> None:
        if self._room is not None and not self._room.done():
            self._room.set_result(None)

    async def headers(self) -> Metadata:
        """The response headers, empty when the server sent none, once they, the first message or
        the final status arrived."""
        return await asyncio.shield(self._headers)

    async def status(self) -> Status:
        """The final OK status, once it arrived; raises the call's error otherwise."""
        return await asyncio.shield(self._outcome)

    # Requests

    async def send(self, request: Req) -> None:
        """Sends one request message, waiting while the transport is not ready for more."""
        async with self._send_lock:
            if self._upload_ended:
                raise self._upload_error(
                    TrevRpcError(Code.FAILED_PRECONDITION, "the request stream is closed")
                )
            payload = _serialize(request, self._max_frame_size)
            while True:
                stream = await self._stream_ready()
                if self._end is not None or self._error is not None:
                    raise self._upload_error(_cancelled())
                compression = self._compression
                assert compression is not None
                if not compression.settled and not compression.keep(payload):
                    # No room to keep it for a replay: wait for the server's answer.
                    await self._settled.wait()
                    continue
                generation = self._generation
                frame = compression.encode(payload)
                flow = self._flow
                if flow is not None and flow.send(frame):
                    return
                try:
                    await stream.write(frame)
                except TrevRpcError as error:
                    if self._generation != generation:
                        # The refused call's stream: the replay carries this request.
                        await self._stream_ready()
                        return
                    if stream.stopped_by_peer:
                        await self._outcome_or_stall()
                    raise self._upload_error(error) from None
                return

    async def send_all(self, requests: Iterable[Req] | AsyncIterable[Req]) -> None:
        """Sends every message of `requests`, then closes the request stream. If the server ended
        the call first, the rest is not sent and this returns; the responses report how it
        ended."""
        try:
            if isinstance(requests, AsyncIterable):
                async for request in requests:
                    if self._end is not None:
                        return
                    await self.send(request)
            else:
                for request in requests:
                    if self._end is not None:
                        return
                    await self.send(request)
        except TrevRpcError:
            # The server ended the call, or stopped reading the requests, which it does after
            # writing its status: the responses report how the call ended, even when that status
            # is still queued behind responses nobody has received.
            stream = self._stream
            if self._error is None and (
                self._end is not None or (stream is not None and stream.stopped_by_peer)
            ):
                return
            raise
        except BaseException as error:
            if not isinstance(error, asyncio.CancelledError):
                self._fail(TrevRpcError.wrap(error))
            raise
        await self.close_send()

    async def close_send(self) -> None:
        """Closes the request stream while the responses stay readable."""
        async with self._send_lock:
            if self._upload_ended:
                return
            self._upload_ended = True
            try:
                stream = await self._stream_ready()
            except TrevRpcError:
                return
            if self._end is not None or self._error is not None or stream.send_done:
                return
            generation = self._generation
            count = self._flow.end_send() if self._flow is not None else b""
            try:
                await stream.write(count, end=True)
            except TrevRpcError:
                if self._generation != generation:
                    # The replay ends the new stream's requests.
                    with contextlib.suppress(TrevRpcError):
                        await self._stream_ready()
                return

    # Cancellation

    async def aclose(self) -> None:
        """Cancels the call unless it already finished."""
        self.cancel()

    def cancel(self) -> None:
        """Cancels the call unless it already finished."""
        if self._end is None:
            self._fail(_cancelled())
        elif self._stream is not None and not self._stream.send_done:
            self._stream.reset_send(_cancelled())


def _upload_after_ok() -> TrevRpcError:
    return TrevRpcError(Code.CANCELLED, "the call ended before the request stream did")


def _cancel_dropped(call: _StreamingCall[Any, Any] | None) -> None:
    """Cancels a call whose wrapper was garbage-collected: its tasks keep the call itself alive,
    and nobody can read its responses or end its requests any more."""
    if call is not None:
        # The event loop may already be closed.
        with contextlib.suppress(RuntimeError):
            call.cancel()


def _retrieve[T](future: asyncio.Future[T]) -> None:
    if not future.cancelled():
        future.exception()


class ResponseStream[Resp: Message]:
    """The responses of a server-streaming call: iterate with `async for`.

    Iteration ends after the final OK status and raises a `TrevRpcError` for any other outcome.
    Use the stream as an `async with` block, or call `aclose()`, to cancel it when leaving early.
    """

    def __init__(self, call: _StreamingCall[Any, Resp]) -> None:
        self._call = call

    def __del__(self) -> None:
        _cancel_dropped(getattr(self, "_call", None))

    async def recv(self) -> Resp | None:
        """The next response message, or `None` after the final OK status."""
        return await self._call.recv()

    async def headers(self) -> Metadata:
        """The response headers, empty when the server sent none."""
        return await self._call.headers()

    async def status(self) -> Status:
        """The final OK status; raises the call's error otherwise."""
        return await self._call.status()

    async def aclose(self) -> None:
        """Cancels the call unless it already finished."""
        await self._call.aclose()

    def __aiter__(self) -> AsyncIterator[Resp]:
        return self

    async def __anext__(self) -> Resp:
        message = await self._call.recv()
        if message is None:
            raise StopAsyncIteration
        return message

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        await self.aclose()


class ClientStreamingCall[Req: Message, Resp: Message]:
    """A client-streaming call: `send()` requests, then `close_and_recv()` the response."""

    def __init__(self, call: _StreamingCall[Req, Resp]) -> None:
        self._call = call

    def __del__(self) -> None:
        _cancel_dropped(getattr(self, "_call", None))

    async def send(self, request: Req) -> None:
        """Sends one request message, waiting while the transport is not ready for more.

        Raises as soon as the server ended the call: with its error if the status is not OK, and
        with `CANCELLED` if it is (`close_and_recv()` returns the response)."""
        await self._call.send(request)

    async def send_all(self, requests: Iterable[Req] | AsyncIterable[Req]) -> None:
        """Sends every message of `requests`, then closes the request stream."""
        await self._call.send_all(requests)

    async def close_and_recv(self) -> Resp:
        """Closes the request stream and returns the response."""
        await self._call.close_send()
        message = await self._call.recv()
        if message is None:
            raise TrevRpcError(Code.INTERNAL, "OK status without a response message")
        await self._call.status()
        return message

    async def headers(self) -> Metadata:
        """The response headers, empty when the server sent none."""
        return await self._call.headers()

    async def status(self) -> Status:
        """The final OK status; raises the call's error otherwise."""
        return await self._call.status()

    async def aclose(self) -> None:
        """Cancels the call unless it already finished."""
        await self._call.aclose()

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        await self.aclose()


class BidiStreamingCall[Req: Message, Resp: Message](ResponseStream[Resp]):
    """A bidirectional-streaming call: `send()` requests and iterate over the responses."""

    def __init__(self, call: _StreamingCall[Req, Resp]) -> None:
        super().__init__(call)
        self._bidi = call

    async def send(self, request: Req) -> None:
        """Sends one request message, waiting while the transport is not ready for more.

        Raises as soon as the server ended the call: with its error if the status is not OK, and
        with `CANCELLED` if it is (the responses then end after those the server sent)."""
        await self._bidi.send(request)

    async def send_all(self, requests: Iterable[Req] | AsyncIterable[Req]) -> None:
        """Sends every message of `requests`, then closes the request stream."""
        await self._bidi.send_all(requests)

    async def close_send(self) -> None:
        """Closes the request stream while the responses stay readable."""
        await self._bidi.close_send()
