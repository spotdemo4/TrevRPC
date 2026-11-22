"""Channels: a connection to one server, which generated clients make calls on."""

from __future__ import annotations

import asyncio
from types import TracebackType
from typing import Any, Literal, Self, TypedDict, Unpack

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
from ._quic import QuicTransport, Stream
from ._status import Code, TrevRpcError

type Transport = Literal["quic"]
"""The transport a channel's connection uses."""

DEFAULT_CONNECT_TIMEOUT = 10.0
DEFAULT_IDLE_TIMEOUT = 30.0


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
    """A connection to one server over native QUIC, opened when first needed and again after it
    fails.

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
        self._connection: QuicTransport | None = None
        self._dialing: asyncio.Task[QuicTransport] | None = None
        self._closed = False

    @property
    def transport(self) -> Transport:
        """The transport the channel connects over."""
        return "quic"

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

    async def _connected(self) -> QuicTransport:
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

    async def _dial(self) -> QuicTransport:
        try:
            async with asyncio.timeout(self._connect_timeout):
                connection = await QuicTransport.connect(
                    self._host,
                    self._port,
                    server_name=self._server_name,
                    cafile=self._ca,
                    cadata=self._cadata,
                    verify=self._verify,
                    max_frame_size=self.max_frame_size,
                    idle_timeout=self._idle_timeout,
                )
        except TimeoutError:
            raise TrevRpcError(
                Code.UNAVAILABLE,
                f"transport unavailable: no connection to {self._host}:{self._port} within "
                f"{self._connect_timeout:g}s",
            ) from None
        except TrevRpcError:
            raise
        except OSError as error:
            raise TrevRpcError(Code.UNAVAILABLE, f"transport unavailable: {error}") from error
        if self._closed:
            connection.close()
            raise TrevRpcError(Code.CANCELLED, "channel closed")
        self._connection = connection
        return connection

    async def open_stream(self, max_frame_size: int) -> Stream:
        """Opens a stream for a call on the current connection."""
        connection = await self._connected()
        return connection.open_stream(max_frame_size)

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
