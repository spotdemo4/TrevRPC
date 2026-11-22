"""Channels: a connection to one server, which generated clients make calls on."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from types import TracebackType
from typing import Any, Literal, Protocol, Self, TypedDict, Unpack

from protobuf import Message

from ._call import (
    BidiStreamingCall,
    CallOptions,
    ClientStreamingCall,
    ResponseStream,
    _StreamingCall,  # pyright: ignore[reportPrivateUsage]
    unary_call,
)
from ._frames import DEFAULT_MAX_FRAME_SIZE
from ._method import Method, MethodKind
from ._qmux import QMuxSession
from ._quic import QuicTransport
from ._status import Code, TrevRpcError
from ._stream import Stream

type Transport = Literal["quic", "qmux"]
"""The transport a channel's connection uses: native QUIC, or QMux over TLS."""

type TransportPolicy = Literal["auto", "quic", "tcp"]
"""Which transports a channel uses: QUIC with a QMux over TLS fallback, or either alone."""

DEFAULT_CONNECT_TIMEOUT = 10.0
DEFAULT_IDLE_TIMEOUT = 30.0
DEFAULT_FALLBACK_DELAY = 0.3
DEFAULT_REPROBE_INTERVAL = 300.0


class _Connection(Protocol):
    """A connection a channel makes calls on: a QUIC connection or a QMux session."""

    @property
    def kind(self) -> Transport: ...

    @property
    def is_open(self) -> bool: ...

    @property
    def closed(self) -> asyncio.Future[None]: ...

    def close(self) -> None: ...

    async def open_stream(self, max_frame_size: int) -> Stream: ...


class ChannelOptions(TypedDict, total=False):
    """How a channel connects, passed to `Channel` and `connect` as keyword arguments."""

    server_name: str
    """The name to verify the server's certificate against; the host by default."""
    ca: str
    """A PEM file of the certificates to trust instead of the system's."""
    cadata: bytes
    """PEM or DER certificates to trust instead of the system's."""
    verify: bool
    """Whether to verify the server's certificate at all; `True` by default."""
    max_frame_size: int
    """Largest frame accepted or sent, in bytes; 4 MiB by default."""
    connect_timeout: float
    """How long a connection attempt may take, in seconds; 10 by default."""
    idle_timeout: float
    """How long the connection may stay silent before it closes, in seconds; 30 by default."""
    transport: TransportPolicy
    """`auto` (the default) connects over QUIC and falls back to QMux over TLS on the server's
    TCP listener when QUIC does not connect within `fallback_delay`; `quic` and `tcp` use one
    transport only."""
    tcp_address: str
    """The `host:port` of the server's TCP listener; the channel's address by default."""
    fallback_delay: float
    """How long QUIC may take to connect, in seconds, before QMux over TLS races it; 0.3 by
    default. A QUIC attempt that fails starts the race at once."""
    reprobe_interval: float
    """How long reconnects keep using QMux over TLS after it won a race, in seconds, before QUIC
    is tried again; 300 by default."""


def parse_address(address: str) -> tuple[str, int]:
    """Splits `host:port` or `[ipv6]:port`."""
    host, sep, port = address.rpartition(":")
    if not sep or not host or not port.isdigit() or not 0 < int(port) < 65536:
        raise TrevRpcError(Code.INVALID_ARGUMENT, f"address {address!r} is not host:port")
    if host.startswith("[") and host.endswith("]"):
        host = host[1:-1]
    elif ":" in host:
        raise TrevRpcError(
            Code.INVALID_ARGUMENT, f"address {address!r}: put an IPv6 host in brackets"
        )
    return host, int(port)


class Channel:
    """A connection to one server, opened when first needed and again after it fails: native QUIC,
    with QMux over TLS as the fallback for networks that block UDP. A call never moves between
    connections: one that was running when its connection failed fails too.

    Generated clients take a channel; its `unary`, `server_stream`, `client_stream` and
    `bidi_stream` methods are what they call. Use it as an `async with` block, or call `close()`.
    """

    def __init__(self, address: str, /, **options: Unpack[ChannelOptions]) -> None:
        """Creates a channel to `address`, the server's `host:port`; it connects at the first
        call. See `ChannelOptions` for the options."""
        self._host, self._port = parse_address(address)
        self._server_name = options.get("server_name") or self._host
        self._ca = options.get("ca")
        self._cadata = options.get("cadata")
        self._verify = options.get("verify", True)
        self.max_frame_size = options.get("max_frame_size", DEFAULT_MAX_FRAME_SIZE)
        self._connect_timeout = options.get("connect_timeout", DEFAULT_CONNECT_TIMEOUT)
        self._idle_timeout = options.get("idle_timeout", DEFAULT_IDLE_TIMEOUT)
        self._policy = options.get("transport", "auto")
        if self._policy not in ("auto", "quic", "tcp"):
            raise TrevRpcError(
                Code.INVALID_ARGUMENT, f"transport {self._policy!r} is not auto, quic or tcp"
            )
        tcp_address = options.get("tcp_address")
        self._tcp_host, self._tcp_port = (
            parse_address(tcp_address) if tcp_address else (self._host, self._port)
        )
        self._fallback_delay = options.get("fallback_delay", DEFAULT_FALLBACK_DELAY)
        self._reprobe_interval = options.get("reprobe_interval", DEFAULT_REPROBE_INTERVAL)
        self._quic_failed_at: float | None = None
        """When QMux last won a race, while reconnects skip QUIC."""
        self._connection: _Connection | None = None
        self._dialing: asyncio.Task[_Connection] | None = None
        self._closed = False

    @property
    def transport(self) -> Transport | None:
        """The transport of the current connection, or `None` while there is none."""
        connection = self._connection
        return connection.kind if connection is not None and connection.is_open else None

    async def connect(self) -> None:
        """Connects now instead of at the first call; raises an `UNAVAILABLE` `TrevRpcError` if
        the server cannot be reached."""
        await self._connected()

    async def close(self) -> None:
        """Closes the connection; calls still running fail with `CANCELLED`."""
        self._closed = True
        if self._dialing is not None:
            self._dialing.cancel()
            self._dialing = None
        if self._connection is not None:
            self._connection.close()
            self._connection = None

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        await self.close()

    async def _connected(self) -> _Connection:
        if self._closed:
            raise TrevRpcError(Code.CANCELLED, "channel closed")
        connection = self._connection
        if connection is not None and connection.is_open:
            return connection
        if self._dialing is None:
            self._dialing = asyncio.get_running_loop().create_task(self._dial())
        dialing = self._dialing
        try:
            return await asyncio.shield(dialing)
        finally:
            if self._dialing is dialing and dialing.done():
                self._dialing = None

    async def _dial(self) -> _Connection:
        loop = asyncio.get_running_loop()
        if self._policy == "quic":
            connection = await self._attempt(self._dial_quic, self._host, self._port)
        elif self._policy == "tcp" or (
            self._quic_failed_at is not None
            and loop.time() - self._quic_failed_at < self._reprobe_interval
        ):
            connection = await self._attempt(self._dial_qmux, self._tcp_host, self._tcp_port)
        else:
            connection = await self._race()
            self._quic_failed_at = loop.time() if connection.kind == "qmux" else None
        if self._closed:
            connection.close()
            raise TrevRpcError(Code.CANCELLED, "channel closed")
        self._connection = connection
        return connection

    async def _race(self) -> _Connection:
        """Connects over QUIC and, when it has not connected within the fallback delay or has
        failed, over QMux over TLS at the same time. The first to connect wins and the other is
        closed."""
        loop = asyncio.get_running_loop()
        quic = loop.create_task(self._attempt(self._dial_quic, self._host, self._port))
        qmux: asyncio.Task[_Connection] | None = None
        pending: set[asyncio.Task[_Connection]] = {quic}
        errors: list[str] = []
        try:
            done, _ = await asyncio.wait(pending, timeout=self._fallback_delay)
            while True:
                for task in done:
                    pending.discard(task)
                    error = task.exception()
                    if error is None:
                        return task.result()
                    errors.append(str(error))
                if qmux is None:
                    qmux = loop.create_task(
                        self._attempt(self._dial_qmux, self._tcp_host, self._tcp_port)
                    )
                    pending.add(qmux)
                if not pending:
                    raise TrevRpcError(
                        Code.UNAVAILABLE, "transport unavailable: " + "; ".join(errors)
                    )
                done, _ = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
        finally:
            for task in pending:
                task.cancel()
                task.add_done_callback(_close_abandoned)

    async def _attempt(
        self, dial: Callable[[str, int], Awaitable[_Connection]], host: str, port: int
    ) -> _Connection:
        """One connection attempt, bounded by the connect timeout."""
        try:
            async with asyncio.timeout(self._connect_timeout):
                return await dial(host, port)
        except TimeoutError:
            raise TrevRpcError(
                Code.UNAVAILABLE,
                f"transport unavailable: no connection to {host}:{port} within "
                f"{self._connect_timeout:g}s",
            ) from None
        except TrevRpcError:
            raise
        except OSError as error:
            raise TrevRpcError(Code.UNAVAILABLE, f"transport unavailable: {error}") from error

    async def _dial_quic(self, host: str, port: int) -> _Connection:
        return await QuicTransport.connect(
            host,
            port,
            server_name=self._server_name,
            cafile=self._ca,
            cadata=self._cadata,
            verify=self._verify,
            max_frame_size=self.max_frame_size,
            idle_timeout=self._idle_timeout,
        )

    async def _dial_qmux(self, host: str, port: int) -> _Connection:
        return await QMuxSession.connect(
            host,
            port,
            server_name=self._server_name,
            cafile=self._ca,
            cadata=self._cadata,
            verify=self._verify,
            max_frame_size=self.max_frame_size,
            idle_timeout=self._idle_timeout,
        )

    async def open_stream(self, max_frame_size: int) -> Stream:
        """Opens a stream for a call on the current connection."""
        connection = await self._connected()
        return await connection.open_stream(max_frame_size)

    # Calls, which generated clients make

    async def unary[Req: Message, Resp: Message](
        self, method: Method[Req, Resp], request: Req, /, **options: Unpack[CallOptions]
    ) -> Resp:
        """Sends one request and returns the response."""
        _check_kind(method, MethodKind.UNARY)
        return await unary_call(self, method, request, options)

    def server_stream[Req: Message, Resp: Message](
        self, method: Method[Req, Resp], request: Req, /, **options: Unpack[CallOptions]
    ) -> ResponseStream[Resp]:
        """Sends one request and returns the stream of responses."""
        _check_kind(method, MethodKind.SERVER_STREAMING)
        return ResponseStream(_StreamingCall(self, method, options, request))

    def client_stream[Req: Message, Resp: Message](
        self, method: Method[Req, Resp], /, **options: Unpack[CallOptions]
    ) -> ClientStreamingCall[Req, Resp]:
        """Starts a call that sends a stream of requests and receives one response."""
        _check_kind(method, MethodKind.CLIENT_STREAMING)
        return ClientStreamingCall(_StreamingCall(self, method, options, None))

    def bidi_stream[Req: Message, Resp: Message](
        self, method: Method[Req, Resp], /, **options: Unpack[CallOptions]
    ) -> BidiStreamingCall[Req, Resp]:
        """Starts a call that sends and receives streams of messages."""
        _check_kind(method, MethodKind.BIDI_STREAMING)
        return BidiStreamingCall(_StreamingCall(self, method, options, None))


def _close_abandoned(task: asyncio.Task[_Connection]) -> None:
    """Closes what a losing attempt connected after the race was decided."""
    if not task.cancelled() and task.exception() is None:
        task.result().close()


def _check_kind(method: Method[Any, Any], kind: MethodKind) -> None:
    if method.kind != kind:
        raise TrevRpcError(
            Code.INVALID_ARGUMENT, f"{method.path} is {method.kind.name}, not {kind.name}"
        )


async def connect(address: str, /, **options: Unpack[ChannelOptions]) -> Channel:
    """Creates a channel to `address` and connects it; see `ChannelOptions` for the options."""
    channel = Channel(address, **options)
    try:
        await channel.connect()
    except BaseException:
        await channel.close()
        raise
    return channel
