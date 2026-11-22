"""The Python interop client: runs the scenarios of interop/README.md against a server.

    trevrpc_interop.py client --addr <host:port> --ca <cert.pem> [--server-name localhost]
                              [--transport quic|tcp|auto] [--tcp-addr <host:port>]
                              [--expect-transport quic|qmux] [--dictionary <file>]
                              [--scenarios <name,...>]

Prints `ok <name>`, `fail <name>: <reason>` or `skip <name>: <reason>` for each scenario, and exits
0 only if every scenario it ran passed. `--transport` maps to the channel's transport policy: QUIC
only, QMux over TLS only (`tcp`), or QUIC with a QMux fallback (`auto`).
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from pathlib import Path

import trevrpc
from gen.compressed_pb import PlainMessage
from gen.compressed_trevrpc import CompressedEchoClient, RefusingEchoClient
from gen.interop_pb import EchoRequest, EchoResponse
from gen.interop_trevrpc import EchoClient
from trevrpc import Code, TrevRpcError

# The message types the interop dictionary compresses.
INTEROP_MESSAGE_TYPES = ("trevrpc.interop.v1.EchoRequest", "trevrpc.interop.v1.EchoResponse")

STREAM_COMPRESSED_RECEIVED = "interop-stream-compressed-received"
PLAIN_COMPRESSED_RECEIVED = "interop-plain-compressed-received"


@dataclass(frozen=True)
class Clients:
    echo: EchoClient
    compressed: CompressedEchoClient
    refusing: RefusingEchoClient


class ScenarioError(Exception):
    pass


def expect(expected: object, actual: object, what: str) -> None:
    if expected != actual:
        raise ScenarioError(f"{what}: expected {expected!r}, got {actual!r}")


def expect_response(response: object, message: str, index: int) -> None:
    expect(message, getattr(response, "message", None), "message")
    expect(index, getattr(response, "index", None), "index")


async def expect_status(call: Awaitable[object], code: Code, message: str) -> TrevRpcError:
    try:
        result = await call
    except TrevRpcError as error:
        expect((code, message), (error.code, error.message), "status")
        return error
    raise ScenarioError(f"expected {code.name} {message!r}, got {result!r}")


async def unary(echo: EchoClient) -> None:
    expect_response(await echo.unary(EchoRequest(message="hello")), "hello", 0)


async def unary_metadata(echo: EchoClient) -> None:
    response = await echo.unary(EchoRequest(message="hello"), metadata={"interop-suffix": "!"})
    expect_response(response, "hello!", 0)


async def unary_error(echo: EchoClient) -> None:
    await expect_status(
        echo.unary(EchoRequest(fail_code=5, fail_message="missing")), Code.NOT_FOUND, "missing"
    )


async def unary_retry_delay(echo: EchoClient) -> None:
    exhausted = await expect_status(
        echo.unary(EchoRequest(fail_code=8, fail_message="slow down")),
        Code.RESOURCE_EXHAUSTED,
        "slow down",
    )
    expect(1.5, exhausted.retry_delay, "retry delay")
    missing = await expect_status(
        echo.unary(EchoRequest(fail_code=5, fail_message="missing")), Code.NOT_FOUND, "missing"
    )
    expect(None, missing.retry_delay, "retry delay")


async def unary_large(echo: EchoClient) -> None:
    large = "x" * 1048576
    expect_response(await echo.unary(EchoRequest(message=large)), large, 0)


async def client_stream(echo: EchoClient) -> None:
    call = echo.client_stream()
    for message in ("a", "b", "c"):
        await call.send(EchoRequest(message=message))
    expect_response(await call.close_and_recv(), "abc", 3)


async def server_stream(echo: EchoClient) -> None:
    async with echo.server_stream(EchoRequest(message="s", count=3)) as stream:
        responses = [(r.message, r.index) async for r in stream]
    expect([("s", 0), ("s", 1), ("s", 2)], responses, "responses")


async def server_stream_error(echo: EchoClient) -> None:
    request = EchoRequest(message="s", count=2, fail_code=9, fail_message="stop")
    responses: list[tuple[str, int]] = []

    async def read() -> None:
        async with echo.server_stream(request) as stream:
            async for response in stream:
                responses.append((response.message, response.index))

    await expect_status(read(), Code.FAILED_PRECONDITION, "stop")
    expect([("s", 0), ("s", 1)], responses, "responses")


async def bidi(echo: EchoClient) -> None:
    async with echo.bidi() as call:
        for index, message in enumerate(("1", "2", "3")):
            await call.send(EchoRequest(message=message))
            response = await call.recv()
            if response is None:
                raise ScenarioError(f"responses ended before the echo of {message!r}")
            expect_response(response, message, index)
        await call.close_send()
        expect(None, await call.recv(), "end of the responses")


async def bidi_error(echo: EchoClient) -> None:
    async with echo.bidi() as call:
        await call.send(EchoRequest(message="1"))
        response = await call.recv()
        if response is None:
            raise ScenarioError("responses ended before the first echo")
        expect_response(response, "1", 0)
        try:
            await call.send(EchoRequest(fail_code=10, fail_message="abort"))
        except TrevRpcError as error:
            # The server can end the call before the second send.
            expect((Code.ABORTED, "abort"), (error.code, error.message), "send status")
        await expect_status(call.recv(), Code.ABORTED, "abort")


async def unary_headers(echo: EchoClient) -> None:
    headers: list[trevrpc.Metadata] = []
    trailers: list[trevrpc.Metadata] = []
    response = await echo.unary(
        EchoRequest(message="hello"),
        metadata={"interop-header": "h"},
        on_response_headers=headers.append,
        on_response_metadata=trailers.append,
    )
    expect_response(response, "hello", 0)
    expect([{"interop-header": b"h"}], headers, "headers")
    expect([{"interop-trailer": b"h"}], trailers, "trailers")


async def server_stream_headers(echo: EchoClient) -> None:
    trailers: list[trevrpc.Metadata] = []
    request = EchoRequest(message="s", count=2)
    async with echo.server_stream(
        request, metadata={"interop-header": "h"}, on_response_metadata=trailers.append
    ) as stream:
        expect({"interop-header": b"h"}, await stream.headers(), "headers")
        responses = [(r.message, r.index) async for r in stream]
    expect([("s", 0), ("s", 1)], responses, "responses")
    expect([{"interop-trailer": b"h"}], trailers, "trailers")


# Compression scenarios


# The message of the plain.records compression golden vector: a JSON record that compresses without
# a dictionary.
PLAIN_RECORD = (
    '{"service":"search","region":"us-east-1","status":200,"latency_ms":118,"path":"/v1/search",'
    '"query":"wireless headphones","results":[{"id":"p-1001","title":"Wireless headphones",'
    '"price":59.99},{"id":"p-1002","title":"Wireless earbuds","price":39.99},{"id":"p-1003",'
    '"title":"Wireless headphones pro","price":129.99}]}'
)

COMPRESSED_MESSAGES = 3
COMPRESSED_STREAM_MESSAGES = 8
OPTIMISTIC_STREAM_COMPRESSED = COMPRESSED_STREAM_MESSAGES - 2
"""The compressed-stream messages a server receives stream-compressed from an optimistic client:
the third and every later one."""


def compressed_message(i: int) -> str:
    """Like the documents the interop dictionary was trained on, so it compresses."""
    return f'{{"service":"search","region":"us-east-1","status":404,"latency_ms":{100 + i}}}'


def metadata_count(metadata: trevrpc.Metadata, key: str) -> int:
    value = metadata.get(key)
    if value is None or not value.isdigit():
        raise ScenarioError(f"trailing metadata {key} = {value!r}, want a count")
    return int(value)


async def compressed(clients: Clients) -> None:
    """Messages that compress, in unary calls and on a bidirectional stream: with the interop
    dictionary on both sides they travel as ZSTD_MESSAGE frames."""
    for i in range(2):
        response = await clients.echo.unary(EchoRequest(message=compressed_message(i)))
        expect(compressed_message(i), response.message, f"unary {i}")
    async with clients.echo.bidi() as call:
        for i in range(COMPRESSED_MESSAGES):
            await call.send(EchoRequest(message=compressed_message(i)))
        await call.close_send()
        for i in range(COMPRESSED_MESSAGES):
            response = await call.recv()
            if response is None:
                raise ScenarioError(f"responses ended before response {i}")
            expect_response(response, compressed_message(i), i)
        expect(None, await call.recv(), "end of the responses")


async def compressed_bidi(clients: Clients, min_compressed: int) -> None:
    """The compressed-stream messages echoed one at a time, with response headers read before the
    first echo; the server must have received at least `min_compressed` stream-compressed."""
    async with clients.compressed.bidi(metadata={"interop-header": "h"}) as call:
        for i in range(COMPRESSED_STREAM_MESSAGES):
            await call.send(EchoRequest(message=compressed_message(i)))
            if i == 0:
                expect({"interop-header": b"h"}, await call.headers(), "headers")
            response = await call.recv()
            if response is None:
                raise ScenarioError(f"responses ended before response {i}")
            expect_response(response, compressed_message(i), i)
        await call.close_send()
        expect(None, await call.recv(), "end of the responses")
        status = await call.status()
    expect(b"h", status.metadata.get("interop-trailer"), "trailing metadata interop-trailer")
    received = metadata_count(status.metadata, STREAM_COMPRESSED_RECEIVED)
    if received < min_compressed:
        raise ScenarioError(
            f"{STREAM_COMPRESSED_RECEIVED} = {received}: expected at least {min_compressed} "
            "requests to arrive stream-compressed"
        )


async def compressed_client_stream(
    start: Callable[..., trevrpc.ClientStreamingCall[EchoRequest, EchoResponse]],
    min_compressed: int,
) -> None:
    """The compressed-stream messages sent at once, their concatenation echoed, each received once;
    with `min_compressed` above zero, at least that many arrived stream-compressed."""
    trailers: list[trevrpc.Metadata] = []
    async with start(on_response_metadata=trailers.append) as call:
        for i in range(COMPRESSED_STREAM_MESSAGES):
            await call.send(EchoRequest(message=compressed_message(i)))
        response = await call.close_and_recv()
    want = "".join(compressed_message(i) for i in range(COMPRESSED_STREAM_MESSAGES))
    expect_response(response, want, COMPRESSED_STREAM_MESSAGES)
    if min_compressed:
        received = metadata_count(trailers[0], STREAM_COMPRESSED_RECEIVED)
        if received < min_compressed:
            raise ScenarioError(
                f"{STREAM_COMPRESSED_RECEIVED} = {received}: expected at least {min_compressed} "
                "requests to arrive stream-compressed"
            )


async def compressed_stream(clients: Clients) -> None:
    """Optimistic stream compression: the server receives the third message and every later one
    stream-compressed, without the client waiting for its answer."""
    try:
        await compressed_bidi(clients, OPTIMISTIC_STREAM_COMPRESSED)
    except ScenarioError as error:
        raise ScenarioError(f"bidi: {error}") from None
    try:
        await compressed_client_stream(
            clients.compressed.client_stream, OPTIMISTIC_STREAM_COMPRESSED
        )
    except ScenarioError as error:
        raise ScenarioError(f"client stream: {error}") from None


async def refused_stream(clients: Clients) -> None:
    """The server refuses an optimistic call, which the client replays; the next call waits for its
    answer, which makes the one after it optimistic again."""
    try:
        await compressed_client_stream(clients.refusing.client_stream, 0)
    except ScenarioError as error:
        raise ScenarioError(f"refused client stream: {error}") from None
    try:
        await compressed_bidi(clients, 1)
    except ScenarioError as error:
        raise ScenarioError(f"bidi: {error}") from None
    try:
        await compressed_client_stream(
            clients.compressed.client_stream, OPTIMISTIC_STREAM_COMPRESSED
        )
    except ScenarioError as error:
        raise ScenarioError(f"client stream: {error}") from None


async def compressed_plain(clients: Clients) -> None:
    """A record that compresses without a dictionary, twice: the first call's answer lets the
    second compress its request."""
    text = PLAIN_RECORD * 2
    for call in range(2):
        trailers: list[trevrpc.Metadata] = []
        response = await clients.compressed.plain(
            PlainMessage(text=text), on_response_metadata=trailers.append
        )
        expect(text, response.text, f"call {call} echo")
        received = trailers[0].get(PLAIN_COMPRESSED_RECEIVED)
        expect(True, received in (b"0", b"1"), f"call {call} {PLAIN_COMPRESSED_RECEIVED}")
        if call == 1:
            expect(b"1", received, f"call {call} {PLAIN_COMPRESSED_RECEIVED}")


def on_echo(
    scenario: Callable[[EchoClient], Awaitable[None]],
) -> Callable[[Clients], Awaitable[None]]:
    return lambda clients: scenario(clients.echo)


type Scenario = Callable[[Clients], Awaitable[None]] | str

# Every scenario of interop/README.md, in order; those this client cannot run yet name why.
SCENARIOS: dict[str, Scenario] = {
    "unary": on_echo(unary),
    "unary-metadata": on_echo(unary_metadata),
    "unary-error": on_echo(unary_error),
    "unary-retry-delay": on_echo(unary_retry_delay),
    "unary-large": on_echo(unary_large),
    "client-stream": on_echo(client_stream),
    "server-stream": on_echo(server_stream),
    "server-stream-error": on_echo(server_stream_error),
    "bidi": on_echo(bidi),
    "bidi-error": on_echo(bidi_error),
    "unary-headers": on_echo(unary_headers),
    "server-stream-headers": on_echo(server_stream_headers),
    "unreliable": "datagram messages are not implemented",
    "compressed": compressed,
    "compressed-stream": compressed_stream,
    "refused-stream": refused_stream,
    "compressed-plain": compressed_plain,
    "connection-metadata": "connection metadata is not implemented",
    "validation": "buf.validate.Violations details are not decoded",
}

DEADLINE = 10.0


async def run(args: argparse.Namespace) -> int:
    selected: set[str] | None = None
    if args.scenarios is not None:
        selected = {name for name in args.scenarios.split(",") if name}
        if unknown := selected - SCENARIOS.keys():
            print(f"client: unknown scenarios: {', '.join(sorted(unknown))}", file=sys.stderr)
            return 2
    dictionaries = ()
    if args.dictionary is not None:
        data = Path(args.dictionary).read_bytes()
        dictionaries = (trevrpc.Dictionary(data, INTEROP_MESSAGE_TYPES),)
    channel = trevrpc.Channel(
        args.addr,
        ca=args.ca,
        server_name=args.server_name,
        transport=args.transport,
        tcp_address=args.tcp_addr or args.addr,
        compression=trevrpc.Compression(dictionaries),
    )
    try:
        try:
            await channel.connect()
        except TrevRpcError as error:
            print(f"client: could not connect: {error}", file=sys.stderr)
            return 1
        if args.expect_transport is not None and args.expect_transport != channel.transport:
            print(
                f"client: connected over {channel.transport}, expected {args.expect_transport}",
                file=sys.stderr,
            )
            return 1
        clients = Clients(
            EchoClient(channel), CompressedEchoClient(channel), RefusingEchoClient(channel)
        )
        passed = True
        for name, scenario in SCENARIOS.items():
            if selected is not None and name not in selected:
                print(f"skip {name}: the server does not support it", flush=True)
            elif isinstance(scenario, str):
                print(f"skip {name}: {scenario}", flush=True)
            else:
                try:
                    async with asyncio.timeout(DEADLINE):
                        await scenario(clients)
                except TimeoutError:
                    passed = False
                    print(f"fail {name}: no result within {DEADLINE:g}s", flush=True)
                except (ScenarioError, TrevRpcError) as error:
                    passed = False
                    print(f"fail {name}: {error}", flush=True)
                else:
                    print(f"ok {name}", flush=True)
        return 0 if passed else 1
    finally:
        await channel.close()


def main() -> int:
    parser = argparse.ArgumentParser(prog="trevrpc-interop-python")
    commands = parser.add_subparsers(dest="command", required=True)
    client = commands.add_parser("client", help="run the interop scenarios against a server")
    client.add_argument("--addr", required=True)
    client.add_argument("--ca", required=True)
    client.add_argument("--server-name", default="localhost")
    client.add_argument("--transport", choices=("quic", "tcp", "auto"), default="quic")
    client.add_argument("--tcp-addr")
    client.add_argument("--expect-transport", choices=("quic", "qmux"))
    client.add_argument("--dictionary")
    client.add_argument("--scenarios")
    args = parser.parse_args()
    return asyncio.run(run(args))


if __name__ == "__main__":
    sys.exit(main())
