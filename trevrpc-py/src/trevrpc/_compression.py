"""Compressed messages: configuration, per-connection negotiation, and each call's choices.

- **Dictionaries** (`Compression`, `Dictionary`) are negotiated per connection: calls query them
  until the connection's first answer, which is its shared set (`ConnectionCompression`).
- **Compression without a dictionary** is the dictionary with ID 0, on methods whose schema sets
  `COMPRESSION_MESSAGE` or `COMPRESSION_STREAM`, for messages of 256 bytes or more.
- **Stream compression** compresses each streamed direction of a `COMPRESSION_STREAM` method with
  one context, from its third message, within the channel's memory budget (`StreamBudget`).
  Requests are compressed optimistically, before the server's answer, unless the server refused an
  optimistic call on the connection and no call has been answered since.

`CallCompression` makes one call's choices: the REQUEST's negotiation fields, each request
message's frame, the decoding of response messages, and the copies an optimistic call keeps.
"""

from __future__ import annotations

import functools
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from typing import Literal

from protobuf import DescMessage, Message

from ._frames import FrameType, encode_frame
from ._method import Method, MethodCompression, MethodKind
from ._status import Code, TrevRpcError
from ._wire import DictionaryInfo, RpcAnswer, StreamCompression
from ._zstd import (
    AVAILABLE,
    CompressionError,
    LoadedDictionary,
    MessageCompressor,
    StreamCompressor,
    StreamDecompressor,
    decompress_message,
    load_dictionary,
)

PLAIN_MIN_SIZE = 256
"""Messages compress without a dictionary from this size."""

STREAM_START = 3
"""A direction's compressing context starts with its third message, so short streams cost
nothing."""

ENCODER_COST = 585 * 1024
"""The budget a compressing context reserves: libzstd's estimate at a 64 KiB window."""

DECODER_COST = 350 * 1024
"""The budget a decompressing context reserves."""

MAX_OPTIMISTIC_COPIES = 1024 * 1024
"""The most request bytes an optimistic call keeps until the server's answer."""

DEFAULT_STREAM_COMPRESSION_MEMORY = 64 * 1024 * 1024


@dataclass(frozen=True, slots=True)
class Dictionary:
    """A zstd dictionary, as `zstd --train` writes them, and the protobuf message types it
    compresses, by full name. One without message types is offered and accepted, but this peer
    compresses nothing with it."""

    data: bytes
    messages: Sequence[str] = ()


class Compression:
    """Compression settings for a channel: dictionaries and the zstd level.

    Compression without a dictionary and stream compression need no settings; the level applies to
    them too. Raises `ValueError` for an invalid dictionary, an ID used twice, or a message type in
    two dictionaries.
    """

    __slots__ = ("_by_id", "_by_type", "_compressors", "_plain", "_query", "level")

    def __init__(self, dictionaries: Iterable[Dictionary] = (), *, level: int = 1) -> None:
        self.level = level
        self._by_id: dict[int, LoadedDictionary] = {}
        self._by_type: dict[str, LoadedDictionary] = {}
        self._compressors: dict[int, MessageCompressor] = {}
        for dictionary in dictionaries:
            loaded = load_dictionary(dictionary.data)
            if loaded.id in self._by_id:
                raise ValueError(f"compression dictionaries share ID {loaded.id}")
            self._by_id[loaded.id] = loaded
            self._compressors[loaded.id] = MessageCompressor(loaded, level)
            for message in dictionary.messages:
                if message in self._by_type:
                    raise ValueError(f"message type {message} is in two compression dictionaries")
                self._by_type[message] = loaded
        self._plain = MessageCompressor(None, level) if AVAILABLE else None
        self._query: tuple[DictionaryInfo, ...] = (
            DictionaryInfo(0),
            *(DictionaryInfo(d.id, d.fingerprint) for d in self._by_id.values()),
        )

    @property
    def available(self) -> bool:
        """Whether this Python has zstd (`compression.zstd`); without it, nothing compresses."""
        return self._plain is not None


class StreamBudget:
    """The memory a channel's calls may hold for stream compression: contexts and the copies of
    optimistic calls."""

    __slots__ = ("limit", "used")

    def __init__(self, limit: int) -> None:
        self.limit = limit
        self.used = 0

    def reserve(self, size: int) -> bool:
        if self.used + size > self.limit:
            return False
        self.used += size
        return True

    def release(self, size: int) -> None:
        self.used -= size


@dataclass(slots=True)
class ConnectionCompression:
    """What a connection has negotiated."""

    shared: dict[int, LoadedDictionary | None] | None = None
    """The dictionaries the server confirmed, with 0 (`None`) for compression without one; `None`
    until a call is answered."""
    pessimistic: bool = False
    """The server refused an optimistic call, and no call has been answered with stream
    compression since: calls wait for the answer before stream-compressing requests."""


@functools.cache
def _redacted(message: DescMessage) -> bool:
    """Whether a message type contains a field marked `debug_redact`, directly or in a nested
    message. Such types neither compress without a dictionary nor stream-compress."""
    seen: set[str] = set()

    def walk(desc: DescMessage) -> bool:
        if desc.type_name in seen:
            return False
        seen.add(desc.type_name)
        for f in desc.fields:
            options = f.proto.options
            if options is not None and options.debug_redact:
                return True
            value = f.value
            nested = getattr(value, "message", None) or getattr(value, "element", None)
            if nested is None:
                nested = getattr(value, "value", None)
            if isinstance(nested, DescMessage) and walk(nested):
                return True
        return False

    return walk(message)


@dataclass(slots=True)
class _Kept:
    payload: bytes


class CallCompression:
    """The compression of one call on one connection."""

    __slots__ = (
        "_accept",
        "_budget",
        "_config",
        "_connection",
        "_copies",
        "_decoder_reserved",
        "_encoder",
        "_encoder_reserved",
        "_inflater",
        "_plain_requests",
        "_queried",
        "_request_dictionary",
        "_request_stream_allowed",
        "_requests_enabled",
        "_response_stream",
        "_sent",
        "_stopped",
        "dictionary_query",
        "kept",
        "optimistic",
        "settled",
        "stream_compression",
    )

    def __init__(
        self,
        config: Compression,
        connection: ConnectionCompression,
        budget: StreamBudget | None,
        method: Method[Message, Message],
    ) -> None:
        self._config = config
        self._connection = connection
        self._budget = budget
        available = config.available
        request_type, response_type = method.request.desc(), method.response.desc()
        allows_plain = method.compression >= MethodCompression.MESSAGE
        streams = method.compression == MethodCompression.STREAM
        self._plain_requests = available and allows_plain and not _redacted(request_type)
        plain_responses = available and allows_plain and not _redacted(response_type)
        shared = connection.shared

        # The dictionaries to ask about, until the connection has an answer.
        query: tuple[DictionaryInfo, ...] = ()
        if available and shared is None and (config._by_id or self._plain_requests):  # pyright: ignore[reportPrivateUsage]
            query = config._query  # pyright: ignore[reportPrivateUsage]
        self.dictionary_query = query
        self._queried = bool(query)

        # Responses may use the response type's dictionary, unless the answer left it out, and no
        # dictionary where the method allows it.
        accept: dict[int, LoadedDictionary | None] = {}
        response_dictionary = config._by_type.get(response_type.type_name) if available else None  # pyright: ignore[reportPrivateUsage]
        if response_dictionary is not None and (shared is None or response_dictionary.id in shared):
            accept[response_dictionary.id] = response_dictionary
        if plain_responses:
            accept[0] = None
        self._accept = accept

        request_dictionary = config._by_type.get(request_type.type_name) if available else None  # pyright: ignore[reportPrivateUsage]
        self._request_dictionary = request_dictionary

        # Stream compression, of the directions that stream.
        kind = method.kind
        request_streams = kind in (MethodKind.CLIENT_STREAMING, MethodKind.BIDI_STREAMING)
        response_streams = kind in (MethodKind.SERVER_STREAMING, MethodKind.BIDI_STREAMING)
        allowed = available and streams and budget is not None
        self._request_stream_allowed = (
            allowed
            and request_streams
            and not method.unreliable_requests
            and not _redacted(request_type)
        )
        response_stream_allowed = (
            allowed
            and response_streams
            and not method.unreliable_responses
            and not _redacted(response_type)
        )
        self._decoder_reserved = False
        self.stream_compression = StreamCompression.NONE
        if self._request_stream_allowed or response_stream_allowed:
            assert budget is not None
            if response_stream_allowed and not budget.reserve(DECODER_COST):
                # Without room to decode responses, the call offers nothing.
                self._request_stream_allowed = response_stream_allowed = False
            else:
                self._decoder_reserved = response_stream_allowed
                self.stream_compression = StreamCompression.ENABLED
        self._response_stream = response_stream_allowed
        self._inflater: StreamDecompressor | None = None
        self._encoder: StreamCompressor | None = None
        self._encoder_reserved = False
        self._stopped = False
        self._sent = 0
        self._requests_enabled = False

        # Optimistic: stream-compress requests from the start, when the budget has room for a
        # compressing context and the server has not refused optimism on the connection.
        # (A call with a datagram flow would not be: its replay would need a flow of its own.)
        self.optimistic = (
            self._request_stream_allowed
            and not connection.pessimistic
            and budget is not None
            and budget.used + ENCODER_COST <= budget.limit
        )
        if self.optimistic:
            self.stream_compression = StreamCompression.OPTIMISTIC
            self._requests_enabled = True
        self.kept: list[_Kept] = []
        self._copies = 0
        self.settled = not self.optimistic
        """Whether an optimistic call has been accepted or refused; always true otherwise."""

    @property
    def connection(self) -> ConnectionCompression:
        """The negotiation state of the connection the call was negotiated on."""
        return self._connection

    @property
    def accept_dictionaries(self) -> tuple[int, ...]:
        return tuple(self._accept)

    # Requests

    def encode(self, payload: bytes) -> bytes:
        """A request message's frame: stream-compressed once the direction's context runs,
        compressed with the shared dictionary of its type or, from 256 bytes, without one where
        that is shared and allowed, when that makes it shorter, and a MESSAGE otherwise."""
        self._sent += 1
        if (
            self._requests_enabled
            and not self._stopped
            and self._encoder is None
            and self._sent >= STREAM_START
        ):
            self._start_encoder()
        if self._encoder is not None:
            return encode_frame(FrameType.ZSTD_STREAM_MESSAGE, self._encoder.compress(payload))
        return self.encode_message(payload)

    def encode_message(self, payload: bytes) -> bytes:
        """A request message's frame without the stream context."""
        shared = self._connection.shared
        if shared is not None:
            dictionary = self._request_dictionary
            compressor: MessageCompressor | None = None
            if dictionary is not None and dictionary.id in shared:
                compressor = self._config._compressors[dictionary.id]  # pyright: ignore[reportPrivateUsage]
            elif self._plain_requests and 0 in shared and len(payload) >= PLAIN_MIN_SIZE:
                compressor = self._config._plain  # pyright: ignore[reportPrivateUsage]
            if compressor is not None:
                compressed = compressor.compress(payload)
                if compressed is not None:
                    return encode_frame(FrameType.ZSTD_MESSAGE, compressed)
        return encode_frame(FrameType.MESSAGE, payload)

    def _start_encoder(self) -> None:
        budget = self._budget
        if budget is None or not budget.reserve(ENCODER_COST):
            # No room: the direction stays per-message for the rest of the call.
            self._stopped = True
            return
        self._encoder_reserved = True
        shared = self._connection.shared
        dictionary = self._request_dictionary
        seed = (
            dictionary
            if shared is not None and dictionary is not None and dictionary.id in shared
            else None
        )
        self._encoder = StreamCompressor(seed, self._config.level)

    # Optimistic copies

    def keep(self, payload: bytes) -> bool:
        """Keeps a request of an undecided optimistic call for its replay; `False` when the call's
        cap or the budget has no room, and the send must wait for the answer."""
        size = len(payload)
        if self._copies + size > MAX_OPTIMISTIC_COPIES:
            return False
        budget = self._budget
        if budget is None or not budget.reserve(size):
            return False
        self._copies += size
        self.kept.append(_Kept(payload))
        return True

    def settle(self) -> None:
        """The optimistic call was accepted, or ended: its copies go."""
        self.settled = True
        self._release_copies()

    def _release_copies(self) -> None:
        if self._copies and self._budget is not None:
            self._budget.release(self._copies)
        self._copies = 0
        self.kept = []

    def refused(self) -> list[bytes]:
        """The server refused the optimistic call: the connection stops being optimistic, and
        this returns the kept requests, which the replay sends per message. The replay waits for
        its answer before stream-compressing."""
        self._connection.pessimistic = True
        kept = [k.payload for k in self.kept]
        self._release_copies()
        self.settled = True
        self.optimistic = False
        self.stream_compression = StreamCompression.ENABLED
        self._close_encoder()
        self._requests_enabled = False
        self._stopped = False
        self._sent = len(kept)
        self._inflater = None
        return kept

    # Responses

    def learn(self, answer: RpcAnswer) -> None:
        """Takes the call's ANSWER: the connection's shared dictionaries, when the call queried
        them, and whether requests may be stream-compressed."""
        connection = self._connection
        if self._queried and connection.shared is None:
            config = self._config
            shared: dict[int, LoadedDictionary | None] = {}
            for info in answer.dictionaries:
                if info.id == 0:
                    if not info.fingerprint:
                        shared[0] = None
                    continue
                dictionary = config._by_id.get(info.id)  # pyright: ignore[reportPrivateUsage]
                if dictionary is not None and dictionary.fingerprint == info.fingerprint:
                    shared[info.id] = dictionary
            connection.shared = shared
            # Dictionaries the answer left out are not shared.
            self._accept = {i: d for i, d in self._accept.items() if i == 0 or i in shared}
        if answer.stream_compression and self._request_stream_allowed:
            self._requests_enabled = True
            connection.pessimistic = False

    def decode(self, frame_type: int, payload: bytes, max_size: int) -> bytes:
        """A response message's payload, decompressed. Raises `INTERNAL` for one that cannot be
        accepted, and `RESOURCE_EXHAUSTED` for one larger than the limit."""
        if frame_type == FrameType.MESSAGE:
            return payload
        try:
            if frame_type == FrameType.ZSTD_MESSAGE:
                if not self._accept:
                    raise CompressionError("invalid", "compressed message on a call accepting none")
                return decompress_message(payload, self._lookup, max_size)
            if not self._response_stream:
                raise CompressionError(
                    "invalid", "stream-compressed message on a call that did not enable it"
                )
            if self._inflater is None:
                self._inflater = StreamDecompressor(self._lookup)
            return self._inflater.decompress(payload, max_size)
        except CompressionError as error:
            code = Code.RESOURCE_EXHAUSTED if error.kind == "too_large" else Code.INTERNAL
            raise TrevRpcError(code, f"response message: {error.reason}") from None

    def _lookup(self, dictionary_id: int) -> LoadedDictionary | Literal[False] | None:
        if dictionary_id in self._accept:
            return self._accept[dictionary_id]
        return False

    # Ending

    def close(self) -> None:
        """Releases what the call holds of the budget."""
        self._release_copies()
        self._close_encoder()
        if self._decoder_reserved and self._budget is not None:
            self._budget.release(DECODER_COST)
            self._decoder_reserved = False
        self._inflater = None

    def _close_encoder(self) -> None:
        if self._encoder_reserved and self._budget is not None:
            self._budget.release(ENCODER_COST)
        self._encoder_reserved = False
        self._encoder = None
