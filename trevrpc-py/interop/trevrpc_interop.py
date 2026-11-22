"""The Python interop client: runs the scenarios of interop/README.md against a server.

    trevrpc_interop.py client --addr <host:port> --ca <cert.pem> [--server-name localhost]
                              [--transport quic|tcp|auto] [--tcp-addr <host:port>]
                              [--expect-transport quic|qmux] [--dictionary <file>]
                              [--scenarios <name,...>]

Prints `ok <name>`, `fail <name>: <reason>` or `skip <name>: <reason>` for each scenario, exits 0
only if every scenario it ran passed, and 77 for a transport it does not speak (only QUIC yet).
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from collections.abc import Awaitable, Callable

import trevrpc
from gen.interop_pb import EchoRequest
from gen.interop_trevrpc import EchoClient
from trevrpc import Code, TrevRpcError

UNSUPPORTED_TRANSPORT = 77


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


type Scenario = Callable[[EchoClient], Awaitable[None]] | str

# Every scenario of interop/README.md, in order; those this client cannot run yet name why.
SCENARIOS: dict[str, Scenario] = {
    "unary": unary,
    "unary-metadata": unary_metadata,
    "unary-error": unary_error,
    "unary-retry-delay": unary_retry_delay,
    "unary-large": unary_large,
    "client-stream": client_stream,
    "server-stream": server_stream,
    "server-stream-error": server_stream_error,
    "bidi": bidi,
    "bidi-error": bidi_error,
    "unary-headers": unary_headers,
    "server-stream-headers": server_stream_headers,
    "unreliable": "datagram messages are not implemented",
    "compressed": "compression is not implemented",
    "compressed-stream": "compression is not implemented",
    "refused-stream": "compression is not implemented",
    "compressed-plain": "compression is not implemented",
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
    if args.transport != "quic":
        print(f"client: transport {args.transport} is not implemented", file=sys.stderr)
        return UNSUPPORTED_TRANSPORT

    channel = trevrpc.Channel(args.addr, ca=args.ca, server_name=args.server_name)
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
        echo = EchoClient(channel)
        passed = True
        for name, scenario in SCENARIOS.items():
            if selected is not None and name not in selected:
                print(f"skip {name}: the server does not support it", flush=True)
            elif isinstance(scenario, str):
                print(f"skip {name}: {scenario}", flush=True)
            else:
                try:
                    async with asyncio.timeout(DEADLINE):
                        await scenario(echo)
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
