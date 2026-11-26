"""The Python load-benchmark client: the `bench` subcommand of bench/README.md.

    trevrpc_interop.py bench --addr <host:port> [--tcp-addr <host:port>] --ca <cert.pem>
                             [--server-name localhost] --transport quic|tcp --scenario <name>
                             [--size BYTES] [--concurrency N] [--connections N]
                             [--duration SECONDS] [--warmup SECONDS] [--messages N] [--rate N]
                             [--metadata-size BYTES] [--metadata-scope call|connection]
                             [--payload repeat|records] [--compression MODE] [--dictionary FILE]
                             [--validation none|standard|cel]

Every worker is a task on one event loop, and the channels' connections run on it too, so the
numbers measure one Python thread. It prints one JSON line and exits 0, or prints why it failed as
the last line of stderr and exits 1: `could not connect: ...` when a channel did not connect, and
`no operation completed in the ...` when no operation started and finished inside the window.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import math
import sys
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Protocol

import trevrpc
from gen.compressed_trevrpc import CompressedEchoClient
from gen.interop_pb import EchoRequest, EchoResponse
from gen.interop_trevrpc import EchoClient
from gen.unreliable_trevrpc import UnreliableEchoClient
from gen.validation_bench_pb import BenchRequest
from trevrpc import Method, MethodKind, TrevRpcError

# The message types the benchmark's dictionary compresses.
DICTIONARY_MESSAGE_TYPES = ("trevrpc.interop.v1.EchoRequest", "trevrpc.interop.v1.EchoResponse")

SCENARIOS = (
    "unary",
    "server-stream",
    "client-stream",
    "bidi",
    "unreliable-server-stream",
    "unreliable-client-stream",
    "paced",
    "unreliable-paced",
    "validation-unary",
    "validation-client-stream",
)
# The scenarios each compression mode runs; None is every scenario. The stream modes call
# CompressedEcho, whose streaming methods allow stream compression and whose Unary allows
# compression without a dictionary.
COMPRESSIONS: dict[str, tuple[str, ...] | None] = {
    "none": None,
    "dictionary": ("unary", "client-stream", "bidi"),
    "stream": ("unary", "client-stream", "bidi"),
    "stream-dictionary": ("unary", "client-stream", "bidi"),
}
PACED_SCENARIOS = ("paced", "unreliable-paced")
VALIDATION_SCENARIOS = ("validation-unary", "validation-client-stream")

# How long operations still running when the window ends get to finish before they are cancelled.
GRACE = 10.0
# How long a channel may take to connect.
CONNECT_TIMEOUT = 10.0
# How long a worker pauses after a failed operation, so a dead connection is not a busy loop.
ERROR_BACKOFF = 0.001
# How long after its send a paced message's echo may arrive and still count as delivered.
PACED_DEADLINE = 0.5
# The request metadata key of --metadata-size, and the largest value TrevRPC metadata allows.
PAD_KEY = "bench-pad"
MAX_METADATA_SIZE = 8 * 1024
# What the server's trailing metadata says it received, for unreliable-client-stream.
RECEIVED_KEY = "interop-received"


def _validation_method(name: str, kind: MethodKind) -> Method[BenchRequest, EchoResponse]:
    """A ValidationBench method, sent BenchRequests: every request type of the service has
    BenchRequest's encoding, so only the server's work differs between them."""
    return Method(f"/trevrpc.interop.v1.ValidationBench/{name}", kind, BenchRequest, EchoResponse)


# The ValidationBench methods of each --validation: unary, then client-streaming.
VALIDATIONS = {
    "none": ("Unary", "ClientStream"),
    "standard": ("StandardUnary", "StandardClientStream"),
    "cel": ("CelUnary", "CelClientStream"),
}
VALIDATION_METHODS = {
    rules: (
        _validation_method(unary, MethodKind.UNARY),
        _validation_method(client_stream, MethodKind.CLIENT_STREAMING),
    )
    for rules, (unary, client_stream) in VALIDATIONS.items()
}


def bench_request(message: str) -> BenchRequest:
    """The request the validation scenarios send: values that keep every rule of
    StandardBenchRequest and CelBenchRequest, as the Go and Rust clients send, around the
    payload."""
    return BenchRequest(
        request_id="0b6e8f3c-2d4a-4e7b-9c1f-5a8d7e6b3c20",
        email="alice@example.com",
        region="eu-west-1",
        quantity=3,
        tags=["checkout", "mobile", "beta"],
        start_ms=1_790_000_000_000,
        end_ms=1_790_000_060_000,
        message=message,
    )


# The records payload, as bench/payload.py generates it byte for byte.

RECORDS_WORKER_SEED = 0x9E3779B97F4A7C15
_MASK = (1 << 64) - 1
_SERVICES = ("checkout", "cart", "payments", "inventory", "search", "auth", "gateway", "notifier")
_REGIONS = ("us-east-1", "us-west-2", "eu-west-1", "eu-central-1", "ap-southeast-1")
_METHODS = ("GET", "POST", "PUT", "DELETE")
_ROUTES = (
    "/api/v1/orders",
    "/api/v1/orders/{id}",
    "/api/v1/cart",
    "/api/v1/users/{id}",
    "/api/v1/search",
    "/healthz",
)
_STATUSES = (200, 200, 200, 200, 200, 201, 204, 400, 404, 500)
_HEX = "0123456789abcdef"
_BASE_TIMESTAMP_MS = 1_790_000_000_000


class Payload(Protocol):
    def next(self) -> str: ...


@dataclass
class RepeatPayload:
    """The letter x, `size` times, in every request."""

    message: str

    def next(self) -> str:
        return self.message


@dataclass
class RecordsPayload:
    """New log records for every request, cut to `size` bytes."""

    state: int
    size: int
    count: int = 0

    def _step(self) -> int:
        x = self.state
        x ^= (x << 13) & _MASK
        x ^= x >> 7
        x ^= (x << 17) & _MASK
        self.state = x
        return x

    def _between(self, low: int, high: int) -> int:
        return low + self._step() % (high - low + 1)

    def _record(self) -> str:
        timestamp = _BASE_TIMESTAMP_MS + self.count * 13 + self._between(0, 12)
        self.count += 1
        service = _SERVICES[self._step() % len(_SERVICES)]
        region = _REGIONS[self._step() % len(_REGIONS)]
        method = _METHODS[self._step() % len(_METHODS)]
        route = _ROUTES[self._step() % len(_ROUTES)]
        status = _STATUSES[self._step() % len(_STATUSES)]
        latency = self._between(1, 2000)
        trace = "".join(_HEX[self._step() % 16] for _ in range(32))
        user = self._between(1, 1_000_000)
        return (
            f'{{"ts":{timestamp},"service":"{service}","region":"{region}",'
            f'"method":"{method}","route":"{route}","status":{status},'
            f'"latency_ms":{latency},"trace":"{trace}","user":{user}}}\n'
        )

    def next(self) -> str:
        parts: list[str] = []
        length = 0
        while length < self.size:
            record = self._record()
            parts.append(record)
            length += len(record)
        return "".join(parts)[: self.size]


def new_payload(kind: str, size: int, worker: int) -> Payload:
    if kind == "records":
        return RecordsPayload((RECORDS_WORKER_SEED + worker) & _MASK, size)
    return RepeatPayload("x" * size)


# Measuring


@dataclass
class Worker:
    """What one worker measured."""

    latencies: list[float] = field(default_factory=list[float])
    messages: int = 0
    lost: int = 0
    errors: int = 0
    delivered: int = 0
    """Messages its operations delivered, or paced echoes that arrived in time, in the window or
    not, for the wire sampler to read at the same instants as the bytes."""


@dataclass
class WireSampler:
    """Samples the wire bytes and the messages delivered at the start and the end of the window,
    so the bytes are divided by the messages of the same interval. Whoever first sees a boundary
    pass takes its sample: the main task when its timer fires, or a worker about to start an
    operation or send a paced message, since a busy event loop delays timers."""

    total: Callable[[], int]
    workers: list[Worker]
    start: float
    end: float
    at: list[tuple[int, int] | None] = field(default_factory=lambda: [None, None])
    # Called whenever a worker or the main task looks at the time, so that the byte counter sees
    # every connection a channel has had, even one that came and went between the samples.
    track: Callable[[], None] = lambda: None

    def observe(self, now: float) -> None:
        self.track()
        if now < self.start:
            return
        if self.at[0] is None:
            self.at[0] = self._snapshot()
        if now >= self.end and self.at[1] is None:
            self.at[1] = self._snapshot()

    def _snapshot(self) -> tuple[int, int]:
        return self.total(), sum(worker.delivered for worker in self.workers)

    def per_message(self, windowed: int) -> float | None:
        first, last = self.at
        if first is None or last is None:
            return None
        messages = last[1] - first[1] or windowed
        return (last[0] - first[0]) / messages if messages else None


class WireCounter:
    """The UDP payload bytes the channels' QUIC connections sent and received. A channel replaces
    its connection when it reconnects, starting the new one's counters at zero, so the counter
    keeps every connection it has seen, by identity, and adds up the bytes of them all."""

    def __init__(self, channels: list[Any]) -> None:
        self._channels = channels
        self._connections: dict[int, object] = {}

    def track(self) -> None:
        """Notes the channels' current connections."""
        for channel in self._channels:
            connection = channel._connection
            if connection is not None:
                self._connections.setdefault(id(connection), connection)

    def total(self) -> int:
        self.track()
        return sum(
            getattr(connection, "bytes_sent", 0) + getattr(connection, "bytes_received", 0)
            for connection in self._connections.values()
        )


@dataclass(frozen=True)
class Config:
    scenario: str
    size: int
    concurrency: int
    connections: int
    messages: int
    rate: float
    duration: float
    warmup: float
    metadata_size: int
    metadata_scope: str
    payload: str
    compression: str
    validation: str

    @property
    def stream_compressed(self) -> bool:
        return self.compression in ("stream", "stream-dictionary")

    def call_options(self) -> dict[str, Any]:
        """The options every call carries: the bench-pad entry, unless it is connection
        metadata."""
        if self.metadata_size == 0 or self.metadata_scope == "connection":
            return {}
        return {"metadata": {PAD_KEY: "x" * self.metadata_size}}


@dataclass(frozen=True)
class Clients:
    channel: trevrpc.Channel
    echo: EchoClient
    unreliable: UnreliableEchoClient
    compressed: CompressedEchoClient


class Operation(Protocol):
    async def run(self) -> int:
        """Performs one operation and returns the messages it carried."""
        ...

    async def close(self) -> None: ...


class Lossy(Protocol):
    def lost(self) -> int: ...


class BenchError(Exception):
    pass


def _check_size(message: str, got: str) -> None:
    if len(got) != len(message):
        raise BenchError(f"response has {len(got)} bytes, want {len(message)}")


@dataclass
class UnaryOperation:
    call: Callable[..., Awaitable[EchoResponse]]
    request: Callable[[str], Any]
    options: dict[str, Any]
    payload: Payload

    async def run(self) -> int:
        message = self.payload.next()
        response = await self.call(self.request(message), **self.options)
        _check_size(message, response.message)
        return 1

    async def close(self) -> None:
        pass


@dataclass
class ServerStreamOperation:
    echo: EchoClient
    options: dict[str, Any]
    payload: Payload
    count: int

    async def run(self) -> int:
        message = self.payload.next()
        received = 0
        async with self.echo.server_stream(
            EchoRequest(message=message, count=self.count), **self.options
        ) as stream:
            async for response in stream:
                _check_size(message, response.message)
                received += 1
        if received != self.count:
            raise BenchError(f"received {received} responses, want {self.count}")
        return received

    async def close(self) -> None:
        pass


@dataclass
class ClientStreamOperation:
    start: Callable[..., trevrpc.ClientStreamingCall[Any, EchoResponse]]
    request: Callable[[str], Any]
    options: dict[str, Any]
    payload: Payload
    count: int

    async def run(self) -> int:
        sent = 0
        async with self.start(**self.options) as call:
            for _ in range(self.count):
                message = self.payload.next()
                sent += len(message)
                await call.send(self.request(message))
            response = await call.close_and_recv()
        if response.index != self.count or len(response.message) != sent:
            raise BenchError(
                f"response covers {response.index} messages and {len(response.message)} bytes, "
                f"want {self.count} and {sent}"
            )
        return self.count

    async def close(self) -> None:
        pass


@dataclass
class BidiOperation:
    """One round trip on a stream kept open across operations, opened again after a failure."""

    start: Callable[..., trevrpc.BidiStreamingCall[EchoRequest, EchoResponse]]
    options: dict[str, Any]
    payload: Payload
    call: trevrpc.BidiStreamingCall[EchoRequest, EchoResponse] | None = None

    async def run(self) -> int:
        if self.call is None:
            self.call = self.start(**self.options)
        message = self.payload.next()
        try:
            await self.call.send(EchoRequest(message=message))
            response = await self.call.recv()
            if response is None:
                raise BenchError("the stream ended before the echo")
            _check_size(message, response.message)
        except BaseException:
            await self.close()
            raise
        return 1

    async def close(self) -> None:
        call, self.call = self.call, None
        if call is not None:
            await call.aclose()


@dataclass
class UnreliableServerStreamOperation:
    """Asks for `count` responses, which may travel as datagrams, and reads those that arrive
    until the status."""

    client: UnreliableEchoClient
    options: dict[str, Any]
    message: str
    count: int
    missing: int = 0

    async def run(self) -> int:
        received = 0
        async with self.client.bidi(**self.options) as call:
            await call.send(EchoRequest(message=self.message, count=self.count))
            await call.close_send()
            while (response := await call.recv()) is not None:
                _check_size(self.message, response.message)
                received += 1
        if received > self.count:
            raise BenchError(f"received {received} responses, want at most {self.count}")
        self.missing = self.count - received
        return received

    def lost(self) -> int:
        return self.missing

    async def close(self) -> None:
        pass


@dataclass
class UnreliableClientStreamOperation:
    """Sends `count` requests that ask for no response, which may travel as datagrams, and reads
    from the status how many the server received."""

    client: UnreliableEchoClient
    options: dict[str, Any]
    message: str
    count: int
    missing: int = 0

    async def run(self) -> int:
        async with self.client.bidi(**self.options) as call:
            request = EchoRequest(message=self.message)
            for _ in range(self.count):
                await call.send(request)
            await call.close_send()
            if await call.recv() is not None:
                raise BenchError("a response to requests that ask for none")
            status = await call.status()
        value = status.metadata.get(RECEIVED_KEY)
        if value is None or not value.isdigit():
            raise BenchError(f"trailing metadata {RECEIVED_KEY} = {value!r}, want a count")
        received = int(value)
        if received > self.count:
            raise BenchError(f"server received {received} requests, want at most {self.count}")
        self.missing = self.count - received
        return received

    def lost(self) -> int:
        return self.missing

    async def close(self) -> None:
        pass


class Workload(Protocol):
    async def work(self, worker: Worker, start: float, end: float) -> None: ...


@dataclass(frozen=True)
class Clock:
    """Where a workload reads the time and waits: the event loop's clock, or a test's."""

    now: Callable[[], float]
    sleep: Callable[[float], Awaitable[None]]


def loop_clock() -> Clock:
    return Clock(asyncio.get_running_loop().time, asyncio.sleep)


@dataclass
class OperationLoop:
    """Runs its operation back to back until the end of the window, counting only the operations
    that start and finish inside it."""

    operation: Operation
    sampler: WireSampler | None
    clock: Clock | None = None

    async def work(self, worker: Worker, start: float, end: float) -> None:
        clock = self.clock or loop_clock()
        operation = self.operation
        try:
            while True:
                began = clock.now()
                if self.sampler is not None:
                    self.sampler.observe(began)
                if began >= end:
                    return
                try:
                    messages = await operation.run()
                except TrevRpcError, BenchError:
                    finished = clock.now()
                    if began >= start and finished <= end:
                        worker.errors += 1
                    await clock.sleep(ERROR_BACKOFF)
                    continue
                # Before finished is read, so that an operation that ends in the window is
                # delivered before the end sample.
                worker.delivered += messages
                finished = clock.now()
                if began >= start and finished <= end:
                    worker.latencies.append(finished - began)
                    worker.messages += messages
                    lost = getattr(operation, "lost", None)
                    if lost is not None:
                        worker.lost += lost()
        finally:
            await operation.close()


def paced_message(sequence: int, payload: str) -> str:
    """The number in decimal and a space in place of the start of the payload."""
    prefix = f"{sequence} "
    return prefix if len(payload) <= len(prefix) else prefix + payload[len(prefix) :]


def paced_sequence(message: str) -> int | None:
    digits, space, _ = message.partition(" ")
    return int(digits) if space and digits.isdigit() else None


@dataclass
class PacedWorkload:
    """Keeps one Bidi stream open and sends a request asking for one echo every `interval`,
    reading the echoes as they arrive. Each message scheduled inside the window is one operation:
    delivered, with the time from its send to its echo, if the echo arrives within
    PACED_DEADLINE of the send, and lost otherwise."""

    start_call: Callable[..., trevrpc.BidiStreamingCall[EchoRequest, EchoResponse]]
    options: dict[str, Any]
    payload: Payload
    interval: float
    sampler: WireSampler | None
    clock: Clock | None = None

    async def work(self, worker: Worker, start: float, end: float) -> None:
        clock = self.clock or loop_clock()
        base = clock.now()
        # Sending goes on past the window, so the echoes of its last messages arrive under the
        # same load.
        total = int((end + PACED_DEADLINE - base) / self.interval) + 1
        sent: list[float | None] = [None] * total
        rtts: list[float | None] = [None] * total
        next_message = 0
        while next_message < total:
            try:
                next_message = await self._stream(clock, worker, base, sent, rtts, next_message)
                break
            except TrevRpcError, BenchError:
                now = clock.now()
                if start <= now <= end:
                    worker.errors += 1
                await clock.sleep(ERROR_BACKOFF)
                # The messages whose time passed while the stream was down are lost, not sent
                # late.
                next_message = max(next_message, int((clock.now() - base) / self.interval) + 1)
        for index, rtt in enumerate(rtts):
            scheduled = base + index * self.interval
            if scheduled < start or scheduled >= end:
                continue
            if rtt is not None and rtt <= PACED_DEADLINE:
                worker.latencies.append(rtt)
                worker.messages += 1
            else:
                worker.lost += 1

    async def _stream(
        self,
        clock: Clock,
        worker: Worker,
        base: float,
        sent: list[float | None],
        rtts: list[float | None],
        next_message: int,
    ) -> int:
        loop = asyncio.get_running_loop()
        async with self.start_call(**self.options) as call:

            async def receive() -> None:
                while (response := await call.recv()) is not None:
                    arrived = clock.now()
                    sequence = paced_sequence(response.message)
                    if sequence is None or sequence >= len(sent):
                        raise BenchError(f"echo {response.message[:20]!r} carries no number")
                    sent_at = sent[sequence]
                    if sent_at is None:
                        raise BenchError(f"echo of message {sequence}, which was not sent")
                    if rtts[sequence] is None:
                        rtt = arrived - sent_at
                        rtts[sequence] = rtt
                        if rtt <= PACED_DEADLINE:
                            worker.delivered += 1

            receiver = loop.create_task(receive())
            try:
                while next_message < len(sent):
                    delay = base + next_message * self.interval - clock.now()
                    if delay > 0:
                        await clock.sleep(delay)
                    if receiver.done():
                        break
                    request = EchoRequest(
                        message=paced_message(next_message, self.payload.next()), count=1
                    )
                    now = clock.now()
                    # A sender that wakes behind schedule after the window sends its overdue
                    # messages after the end sample: their bytes are not in it, as their echoes
                    # will not be.
                    if self.sampler is not None:
                        self.sampler.observe(now)
                    sent[next_message] = now
                    next_message += 1
                    await call.send(request)
                if not receiver.done():
                    await call.close_send()
                await receiver
            finally:
                if not receiver.done():
                    receiver.cancel()
                    with contextlib.suppress(asyncio.CancelledError, TrevRpcError, BenchError):
                        await receiver
        return next_message


def workload(
    config: Config, clients: Clients, payload: Payload, sampler: WireSampler | None
) -> Workload:
    options = config.call_options()
    scenario = config.scenario
    compressed = config.stream_compressed
    match scenario:
        case "unary":
            call = clients.compressed.unary if compressed else clients.echo.unary
            operation: Operation = UnaryOperation(
                call, lambda m: EchoRequest(message=m), options, payload
            )
        case "server-stream":
            operation = ServerStreamOperation(clients.echo, options, payload, config.messages)
        case "client-stream":
            start = clients.compressed.client_stream if compressed else clients.echo.client_stream
            operation = ClientStreamOperation(
                start, lambda m: EchoRequest(message=m), options, payload, config.messages
            )
        case "bidi":
            start_bidi = clients.compressed.bidi if compressed else clients.echo.bidi
            operation = BidiOperation(start_bidi, options, payload)
        case "unreliable-server-stream":
            operation = UnreliableServerStreamOperation(
                clients.unreliable, options, payload.next(), config.messages
            )
        case "unreliable-client-stream":
            operation = UnreliableClientStreamOperation(
                clients.unreliable, options, payload.next(), config.messages
            )
        case "paced" | "unreliable-paced":
            start_paced = clients.echo.bidi if scenario == "paced" else clients.unreliable.bidi
            return PacedWorkload(start_paced, options, payload, 1 / config.rate, sampler)
        case "validation-unary":
            method = VALIDATION_METHODS[config.validation][0]
            channel = clients.channel

            async def validated_unary(request: BenchRequest, **call: Any) -> EchoResponse:
                return await channel.unary(method, request, **call)

            operation = UnaryOperation(validated_unary, bench_request, options, payload)
        case "validation-client-stream":
            stream_method = VALIDATION_METHODS[config.validation][1]
            channel = clients.channel

            def validated_client_stream(
                **call: Any,
            ) -> trevrpc.ClientStreamingCall[BenchRequest, EchoResponse]:
                return channel.client_stream(stream_method, **call)

            operation = ClientStreamOperation(
                validated_client_stream, bench_request, options, payload, config.messages
            )
        case _:
            raise AssertionError(scenario)
    return OperationLoop(operation, sampler)


def percentile_us(ordered: list[float], p: float) -> float:
    """The nearest-rank percentile of sorted latencies in seconds, in microseconds."""
    rank = max(math.ceil(p * len(ordered)), 1)
    return ordered[rank - 1] * 1e6


def summarize(config: Config, transport: str, workers: list[Worker]) -> dict[str, object]:
    latencies = sorted(latency for worker in workers for latency in worker.latencies)
    errors = sum(worker.errors for worker in workers)
    if not latencies:
        raise BenchError(
            f"no operation completed in the {config.duration:g}s measured window ({errors} failed)"
        )
    messages = sum(worker.messages for worker in workers)
    result: dict[str, object] = {
        "scenario": config.scenario,
        "transport": transport,
        "size": config.size,
        "metadata_size": config.metadata_size,
        "metadata_scope": config.metadata_scope,
        "connections": config.connections,
        "concurrency": config.concurrency,
        "duration_s": config.duration,
        "ops": len(latencies),
        "msgs": messages,
        "errors": errors,
        "ops_per_sec": len(latencies) / config.duration,
        "msgs_per_sec": messages / config.duration,
        "bytes_per_sec": messages / config.duration * config.size,
        "latency_us": {
            "p50": percentile_us(latencies, 0.50),
            "p90": percentile_us(latencies, 0.90),
            "p99": percentile_us(latencies, 0.99),
            "p999": percentile_us(latencies, 0.999),
        },
        "lost": sum(worker.lost for worker in workers),
        "payload": config.payload,
        "compression": config.compression,
    }
    if config.scenario in PACED_SCENARIOS:
        result["rate"] = config.rate
    if config.scenario in VALIDATION_SCENARIOS:
        result["validation"] = config.validation
    return result


async def run(args: argparse.Namespace) -> int:
    config = Config(
        scenario=args.scenario,
        size=args.size,
        concurrency=args.concurrency,
        connections=args.connections,
        messages=args.messages,
        rate=args.rate,
        duration=args.duration,
        warmup=args.warmup,
        metadata_size=args.metadata_size,
        metadata_scope=args.metadata_scope,
        payload=args.payload,
        compression=args.compression,
        validation=args.validation,
    )
    options: dict[str, Any] = {
        "ca": args.ca,
        "server_name": args.server_name,
        "transport": args.transport,
        "tcp_address": args.tcp_addr or args.addr,
        "connect_timeout": CONNECT_TIMEOUT,
    }
    if args.dictionary is not None:
        data = Path(args.dictionary).read_bytes()
        options["compression"] = trevrpc.Compression(
            (trevrpc.Dictionary(data, DICTIONARY_MESSAGE_TYPES),)
        )
    if config.metadata_scope == "connection" and config.metadata_size > 0:
        options["connection_metadata"] = {PAD_KEY: "x" * config.metadata_size}

    channels = [trevrpc.Channel(args.addr, **options) for _ in range(config.connections)]
    try:
        try:
            for channel in channels:
                await channel.connect()
        except TrevRpcError as error:
            print(f"bench: could not connect: {error}", file=sys.stderr)
            return 1
        transport = channels[0].transport or args.transport

        wire = WireCounter(channels)

        loop = asyncio.get_running_loop()
        start = loop.time() + config.warmup
        end = start + config.duration
        workers = [Worker() for _ in range(config.concurrency)]
        # Only the QUIC connection counts its bytes; QMux runs on asyncio's TLS streams.
        sampler = (
            WireSampler(wire.total, workers, start, end, track=wire.track)
            if transport == "quic"
            else None
        )
        clients = [
            Clients(channel, EchoClient(channel), UnreliableEchoClient(channel),
                    CompressedEchoClient(channel))
            for channel in channels
        ]  # fmt: skip
        tasks = [
            loop.create_task(
                workload(
                    config,
                    clients[index % len(clients)],
                    new_payload(config.payload, config.size, index),
                    sampler,
                ).work(worker, start, end)
            )
            for index, worker in enumerate(workers)
        ]
        await asyncio.sleep(max(start - loop.time(), 0))
        if sampler is not None:
            sampler.observe(loop.time())
        await asyncio.sleep(max(end - loop.time(), 0))
        if sampler is not None:
            sampler.observe(loop.time())
        _, pending = await asyncio.wait(tasks, timeout=max(end + GRACE - loop.time(), 0))
        for task in pending:
            task.cancel()
        await asyncio.gather(*pending, return_exceptions=True)
        for task in tasks:
            if not task.cancelled() and (error := task.exception()) is not None:
                raise error

        try:
            result = summarize(config, transport, workers)
        except BenchError as error:
            print(f"bench: {error}", file=sys.stderr)
            return 1
        if sampler is not None:
            per_message = sampler.per_message(int(result["msgs"]))  # pyright: ignore[reportArgumentType]
            if per_message is not None:
                result["wire_bytes_per_msg"] = per_message
        print(json.dumps(result, separators=(",", ":")), flush=True)
        return 0
    finally:
        for channel in channels:
            await channel.close()


def add_parser(commands: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:  # pyright: ignore[reportPrivateUsage]
    bench = commands.add_parser("bench", help="run one load-benchmark cell against a server")
    bench.add_argument("--addr", required=True)
    bench.add_argument("--tcp-addr")
    bench.add_argument("--ca", required=True)
    bench.add_argument("--server-name", default="localhost")
    bench.add_argument("--transport", choices=("quic", "tcp"), required=True)
    bench.add_argument("--scenario", choices=SCENARIOS, required=True)
    bench.add_argument("--size", type=int, default=64)
    bench.add_argument("--concurrency", type=int, default=1)
    bench.add_argument("--connections", type=int, default=1)
    bench.add_argument("--duration", type=float, default=5.0)
    bench.add_argument("--warmup", type=float, default=1.0)
    bench.add_argument("--messages", type=int, default=100)
    bench.add_argument("--rate", type=float, default=100.0)
    bench.add_argument("--metadata-size", type=int, default=0)
    bench.add_argument("--metadata-scope", choices=("call", "connection"), default="call")
    bench.add_argument("--payload", choices=("repeat", "records"), default="repeat")
    bench.add_argument("--compression", choices=tuple(COMPRESSIONS), default="none")
    bench.add_argument("--dictionary")
    bench.add_argument("--validation", choices=tuple(VALIDATIONS), default="none")


def check(args: argparse.Namespace) -> str | None:
    """Why the arguments do not make a cell, or None."""
    scenarios = COMPRESSIONS[args.compression]
    if scenarios is not None and args.scenario not in scenarios:
        return f"--compression {args.compression} does not run the {args.scenario} scenario"
    if (args.compression in ("dictionary", "stream-dictionary")) != (args.dictionary is not None):
        return f"--compression {args.compression} and --dictionary go together"
    if args.validation != "none" and args.scenario not in VALIDATION_SCENARIOS:
        return f"--validation {args.validation} runs the validation scenarios only"
    if args.size < 0:
        return "--size must not be negative"
    if not 0 <= args.metadata_size <= MAX_METADATA_SIZE:
        return f"--metadata-size must be between 0 and {MAX_METADATA_SIZE}"
    if args.concurrency < 1:
        return "--concurrency must be at least 1"
    if not 1 <= args.connections <= args.concurrency:
        return "--connections must be between 1 and --concurrency"
    if not 1 <= args.messages < 2**32:
        return "--messages must be between 1 and 2^32-1"
    if not (args.rate > 0 and math.isfinite(args.rate)):
        return "--rate must be positive"
    if not (args.duration > 0 and math.isfinite(args.duration)):
        return "--duration must be positive"
    if not (args.warmup >= 0 and math.isfinite(args.warmup)):
        return "--warmup must not be negative"
    return None


def main(args: argparse.Namespace) -> int:
    if (problem := check(args)) is not None:
        print(f"bench: {problem}", file=sys.stderr)
        return 2
    return asyncio.run(run(args))
