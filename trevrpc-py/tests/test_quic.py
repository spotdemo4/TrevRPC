"""Calls over native QUIC against the test server in server.py."""

from __future__ import annotations

import asyncio
import gc
import socket
import unittest
from collections.abc import AsyncIterator, Awaitable
from typing import Unpack
from unittest import mock

import trevrpc
from protobuf.wkt.google.protobuf.wrappers_pb import StringValue
from trevrpc import Code, Method, MethodKind, TrevRpcError
from trevrpc._stream import Stream

from .server import FLOOD_MESSAGE, WAIT, TestServer, start_server


def _method(name: str, kind: MethodKind) -> Method[StringValue, StringValue]:
    return Method(f"/test.Echo/{name}", kind, StringValue, StringValue)


UNARY = _method("Unary", MethodKind.UNARY)
SERVER_STREAM = _method("ServerStream", MethodKind.SERVER_STREAMING)
CLIENT_STREAM = _method("ClientStream", MethodKind.CLIENT_STREAMING)
BIDI = _method("Bidi", MethodKind.BIDI_STREAMING)
STOP_FIRST = _method("StopFirst", MethodKind.BIDI_STREAMING)
FLOOD_FAIL = _method("FloodFail", MethodKind.BIDI_STREAMING)
RESET = _method("Reset", MethodKind.UNARY)
TRUNCATE = _method("Truncate", MethodKind.UNARY)
DROP = _method("Drop", MethodKind.UNARY)
FLOOD = _method("Flood", MethodKind.SERVER_STREAMING)
HANG = _method("Hang", MethodKind.UNARY)
HANG_STREAM = _method("Hang", MethodKind.BIDI_STREAMING)


def s(value: str) -> StringValue:
    return StringValue(value=value)


class QuicTest(unittest.IsolatedAsyncioTestCase):
    """Runs over native QUIC; the `QMux` subclasses run the same tests over QMux over TLS."""

    server: TestServer
    channel: trevrpc.Channel
    transport: trevrpc.TransportPolicy = "quic"

    async def asyncSetUp(self) -> None:
        self.server = await self.start_server()
        self.channel = self.new_channel()

    def new_channel(self, **options: Unpack[trevrpc.ChannelOptions]) -> trevrpc.Channel:
        port = self.server.port if self.transport == "quic" else self.server.tcp_port
        settings: trevrpc.ChannelOptions = {
            "ca": self.server.ca,
            "server_name": "localhost",
            "transport": self.transport,
        }
        settings.update(options)
        return trevrpc.Channel(f"127.0.0.1:{port}", **settings)

    async def start_server(self) -> TestServer:
        return await start_server()

    async def asyncTearDown(self) -> None:
        await self.channel.close()
        self.server.close()

    async def expect_status(self, call: Awaitable[object], code: Code) -> TrevRpcError:
        with self.assertRaises(TrevRpcError) as caught:
            await asyncio.wait_for(call, WAIT)
        self.assertEqual(caught.exception.code, code, caught.exception)
        return caught.exception

    def send_when_stopped(
        self, call: trevrpc.BidiStreamingCall[StringValue, StringValue]
    ) -> tuple[
        list[asyncio.Task[None]], mock._patch[object]  # pyright: ignore[reportPrivateUsage]
    ]:
        """Patches streams to send on `call` the moment the server's STOP_SENDING arrives, before
        the call reads anything that came with it."""
        sends: list[asyncio.Task[None]] = []
        on_stop_sending = Stream.on_stop_sending

        def stopped(stream: Stream, code: int) -> None:
            on_stop_sending(stream, code)
            sends.append(asyncio.create_task(call.send(s("late")), eager_start=True))

        return sends, mock.patch.object(Stream, "on_stop_sending", stopped)


class CallTest(QuicTest):
    async def test_unary(self) -> None:
        reply = await self.channel.unary(UNARY, s("hello"), metadata={"suffix": "!"})
        self.assertEqual(reply.value, "hello!")
        self.assertEqual(self.server.calls[0].metadata, {"suffix": b"!"})

    async def test_unary_headers_and_trailers(self) -> None:
        headers: list[trevrpc.Metadata] = []
        trailers: list[trevrpc.Metadata] = []
        reply = await self.channel.unary(
            UNARY,
            s("headers"),
            on_response_headers=headers.append,
            on_response_metadata=trailers.append,
        )
        self.assertEqual(reply.value, "headers")
        self.assertEqual(headers, [{"h": b"1"}])
        self.assertEqual(trailers, [{"t": b"2"}])

    async def test_unary_error(self) -> None:
        error = await self.expect_status(
            self.channel.unary(UNARY, s("fail:5:missing")), Code.NOT_FOUND
        )
        self.assertEqual(error.message, "missing")

    async def test_unary_large(self) -> None:
        reply = await self.channel.unary(UNARY, s("x" * 1024 * 1024))
        self.assertEqual(len(reply.value), 1024 * 1024)

    async def test_concurrent_unary_calls_share_the_connection(self) -> None:
        replies = await asyncio.gather(*(self.channel.unary(UNARY, s(str(i))) for i in range(50)))
        self.assertEqual([r.value for r in replies], [str(i) for i in range(50)])
        self.assertEqual(len(self.server.connections), 1)

    async def test_server_stream(self) -> None:
        async with self.channel.server_stream(SERVER_STREAM, s("3")) as stream:
            self.assertEqual(await stream.headers(), {})
            values = [reply.value async for reply in stream]
            self.assertEqual((await stream.status()).code, Code.OK)
        self.assertEqual(values, ["0", "1", "2"])

    async def test_client_stream(self) -> None:
        call = self.channel.client_stream(CLIENT_STREAM)
        for value in "abc":
            await call.send(s(value))
        reply = await call.close_and_recv()
        self.assertEqual(reply.value, "abc")

    async def test_client_stream_send_all(self) -> None:
        call = self.channel.client_stream(CLIENT_STREAM)
        await call.send_all(s(v) for v in "xyz")
        self.assertEqual((await call.close_and_recv()).value, "xyz")

    async def test_bidi(self) -> None:
        async with self.channel.bidi_stream(BIDI) as call:
            for value in "123":
                await call.send(s(value))
                reply = await call.recv()
                assert reply is not None
                self.assertEqual(reply.value, value)
            await call.close_send()
            self.assertIsNone(await call.recv())

    async def test_bidi_error(self) -> None:
        call = self.channel.bidi_stream(BIDI)
        await call.send(s("1"))
        self.assertEqual((await call.recv() or s("")).value, "1")
        await call.send(s("fail:10:abort"))
        await self.expect_status(call.recv(), Code.ABORTED)
        # Once the server ended the call, sends report its outcome.
        await self.expect_status(call.send(s("2")), Code.ABORTED)


class FailureTest(QuicTest):
    async def test_send_racing_the_status_reports_it(self) -> None:
        """The server's STATUS and STOP_SENDING arrive in one packet: a send that runs before the
        call has read the STATUS reports the server's status, not the stopped stream."""
        call = self.channel.bidi_stream(BIDI)
        sends, patch = self.send_when_stopped(call)
        with patch:
            await call.send(s("fail:10:abort"))
            await self.server.changed.wait_until(lambda: bool(sends))
        await self.expect_status(sends[0], Code.ABORTED)
        await self.expect_status(call.recv(), Code.ABORTED)

    async def test_peer_stop_before_the_status(self) -> None:
        """The server stops the request stream, then fails the call: a send between the two
        waits for the status."""
        call = self.channel.bidi_stream(STOP_FIRST)
        sends, patch = self.send_when_stopped(call)
        with patch:
            await call.send(s("first"))
            await self.server.changed.wait_until(lambda: bool(sends))
        error = await self.expect_status(sends[0], Code.FAILED_PRECONDITION)
        self.assertEqual(error.message, "stopped")

    async def test_send_all_leaves_a_status_behind_unread_responses_to_them(self) -> None:
        """The server's status and STOP_SENDING come after more responses than the call reads
        ahead: `send_all()` stops sending and returns, and the responses end with the status."""
        call = self.channel.bidi_stream(FLOOD_FAIL)
        stopped = asyncio.Event()
        on_stop_sending = Stream.on_stop_sending

        def stop(stream: Stream, code: int) -> None:
            on_stop_sending(stream, code)
            stopped.set()

        async def requests() -> AsyncIterator[StringValue]:
            yield s("first")
            await stopped.wait()
            for _ in range(3):
                yield s("after")

        with mock.patch.object(Stream, "on_stop_sending", stop):
            await asyncio.wait_for(call.send_all(requests()), WAIT)
        responses: list[str] = []

        async def read() -> None:
            async for response in call:
                responses.append(response.value)

        await self.expect_status(read(), Code.ABORTED)
        self.assertEqual(responses, [str(i) for i in range(40)])

    async def test_peer_reset_is_cancelled(self) -> None:
        await self.expect_status(self.channel.unary(RESET, s("")), Code.CANCELLED)

    async def test_stream_ending_before_status_is_internal(self) -> None:
        await self.expect_status(self.channel.unary(TRUNCATE, s("")), Code.INTERNAL)

    async def test_connection_lost_mid_call_is_unavailable(self) -> None:
        await self.expect_status(self.channel.unary(DROP, s("")), Code.UNAVAILABLE)
        # The channel reconnects for the next call.
        self.assertEqual((await self.channel.unary(UNARY, s("again"))).value, "again")

    async def test_unary_deadline(self) -> None:
        await self.expect_status(
            self.channel.unary(HANG, s(""), timeout=0.2), Code.DEADLINE_EXCEEDED
        )
        call = await self.server.call()
        self.assertGreater(call.timeout_nanos, 0)
        await self.server.ended(call)
        # The request was all sent and acknowledged, so only the response is stopped.
        self.assertEqual(call.stop_code, 1)

    async def test_streaming_deadline(self) -> None:
        call = self.channel.bidi_stream(HANG_STREAM, timeout=0.2)
        await self.expect_status(call.recv(), Code.DEADLINE_EXCEEDED)

    async def test_cancelling_a_unary_call_stops_its_stream(self) -> None:
        task = asyncio.create_task(self.channel.unary(HANG, s("")))
        call = await self.server.call()
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        await self.server.ended(call)
        self.assertEqual(call.stop_code, 1)

    async def test_aclose_cancels_a_stream(self) -> None:
        stream = self.channel.bidi_stream(HANG_STREAM)
        await stream.send(s("x"))
        call = await self.server.call()
        await stream.aclose()
        await self.expect_status(stream.recv(), Code.CANCELLED)
        await self.server.ended(call)
        self.assertEqual(call.reset_code, 1)

    async def test_dropping_a_call_cancels_it(self) -> None:
        stream = self.channel.bidi_stream(HANG_STREAM)
        await stream.send(s("x"))
        call = await self.server.call()
        del stream
        gc.collect()
        await self.server.ended(call)
        self.assertEqual(call.reset_code, 1)

    async def test_frame_limit_above_the_channels_is_rejected(self) -> None:
        limit = self.channel.max_frame_size + 1
        await self.expect_status(
            self.channel.unary(UNARY, s("x"), max_frame_size=limit), Code.INVALID_ARGUMENT
        )
        with self.assertRaises(TrevRpcError) as caught:
            self.channel.bidi_stream(BIDI, max_frame_size=limit)
        self.assertEqual(caught.exception.code, Code.INVALID_ARGUMENT)

    async def test_frame_limit_below_the_channels_applies(self) -> None:
        await self.expect_status(
            self.channel.unary(UNARY, s("x" * 300), max_frame_size=200), Code.RESOURCE_EXHAUSTED
        )

    async def test_unreachable_server_is_unavailable(self) -> None:
        channel = trevrpc.Channel(
            "127.0.0.1:1", ca=self.server.ca, transport=self.transport, connect_timeout=0.5
        )
        await self.expect_status(channel.unary(UNARY, s("x")), Code.UNAVAILABLE)

    async def test_untrusted_certificate_is_unavailable(self) -> None:
        port = self.server.port if self.transport == "quic" else self.server.tcp_port
        channel = trevrpc.Channel(f"127.0.0.1:{port}", transport=self.transport, connect_timeout=2)
        await self.expect_status(channel.unary(UNARY, s("x")), Code.UNAVAILABLE)

    async def test_reconnects_after_the_connection_closes(self) -> None:
        await self.channel.unary(UNARY, s("a"))
        connection = self.channel._connection  # pyright: ignore[reportPrivateUsage]
        assert connection is not None
        self.server.connections[0].close()
        await asyncio.wait_for(connection.closed, WAIT)
        reply = await self.channel.unary(UNARY, s("b"))
        self.assertEqual(reply.value, "b")
        self.assertEqual(len(self.server.connections), 2)

    async def test_closed_channel_cancels_calls(self) -> None:
        task = asyncio.create_task(self.channel.unary(HANG, s("")))
        await self.server.call()
        await self.channel.close()
        await self.expect_status(task, Code.CANCELLED)


class FlowControlTest(QuicTest):
    async def test_a_slow_reader_holds_back_the_server(self) -> None:
        async with self.channel.server_stream(FLOOD, s("")) as stream:
            self.assertIsNotNone(await stream.recv())
            # Stop reading: the server is held at the read-ahead queue plus the stream window.
            call = await self.server.call()
            protocol = self.server.connections[0]
            limit = 1024 * 1024 + 2 * FLOOD_MESSAGE + trevrpc.DEFAULT_MAX_FRAME_SIZE + 64
            # Over 5 MiB has to pass first, which a loaded machine can take well over WAIT to
            # move: wait as long as it moves, and stop once it passes the limit.
            await self.server.changed.wait_while_progressing(
                lambda: call.blocked_at is not None or protocol.sent(call.stream_id) >= limit,
                lambda: protocol.sent(call.stream_id),
            )
            sent = call.blocked_at
            self.assertIsNotNone(sent, f"the server sent {protocol.sent(call.stream_id)} bytes")
            assert sent is not None
            self.assertLess(sent, limit)
            # Reading again releases it.
            for _ in range(64):
                await stream.recv()
            await self.server.changed.wait_until(lambda: protocol.sent(call.stream_id) > sent)


class StreamLimitTest(QuicTest):
    async def start_server(self) -> TestServer:
        return await start_server(max_streams=4)

    async def test_calls_wait_for_stream_credit(self) -> None:
        """More concurrent calls than the server allows streams wait for credit instead of
        breaking the connection."""
        replies = await asyncio.wait_for(
            asyncio.gather(*(self.channel.unary(UNARY, s(str(i))) for i in range(40))), WAIT
        )
        self.assertEqual([r.value for r in replies], [str(i) for i in range(40)])
        self.assertEqual(len(self.server.connections), 1)


class QMuxCallTest(CallTest):
    transport = "tcp"


class QMuxFailureTest(FailureTest):
    transport = "tcp"


class QMuxFlowControlTest(FlowControlTest):
    transport = "tcp"


class QMuxStreamLimitTest(StreamLimitTest):
    transport = "tcp"


class RaceTest(QuicTest):
    """The `auto` policy: QUIC, with QMux over TLS racing it after the fallback delay."""

    def racing_channel(
        self, udp_port: int, **options: Unpack[trevrpc.ChannelOptions]
    ) -> trevrpc.Channel:
        settings: trevrpc.ChannelOptions = {
            "ca": self.server.ca,
            "server_name": "localhost",
            "transport": "auto",
            "tcp_address": f"127.0.0.1:{self.server.tcp_port}",
        }
        settings.update(options)
        return trevrpc.Channel(f"127.0.0.1:{udp_port}", **settings)

    async def test_quic_wins_when_it_connects(self) -> None:
        channel = self.racing_channel(self.server.port)
        self.addAsyncCleanup(channel.close)
        self.assertEqual((await channel.unary(UNARY, s("a"))).value, "a")
        self.assertEqual(channel.transport, "quic")

    async def test_falls_back_when_udp_is_blocked(self) -> None:
        # Nothing answers this UDP port, so QUIC never connects.
        channel = self.racing_channel(self.unused_udp_port(), fallback_delay=0.05)
        self.addAsyncCleanup(channel.close)
        self.assertEqual((await channel.unary(UNARY, s("a"))).value, "a")
        self.assertEqual(channel.transport, "qmux")

    async def test_reconnects_over_qmux_until_the_reprobe_interval(self) -> None:
        channel = self.racing_channel(
            self.unused_udp_port(), fallback_delay=0.05, reprobe_interval=60
        )
        self.addAsyncCleanup(channel.close)
        await channel.unary(UNARY, s("a"))
        connection = channel._connection  # pyright: ignore[reportPrivateUsage]
        assert connection is not None
        self.server.connections[-1].close()
        await asyncio.wait_for(connection.closed, WAIT)
        # QUIC would take a fallback delay to give up on; the reconnect skips it.
        with mock.patch.object(channel, "_dial_quic", side_effect=AssertionError("raced QUIC")):
            self.assertEqual((await channel.unary(UNARY, s("b"))).value, "b")
        self.assertEqual(channel.transport, "qmux")

    async def test_both_failing_is_unavailable(self) -> None:
        channel = trevrpc.Channel(
            "127.0.0.1:1", ca=self.server.ca, transport="auto", connect_timeout=0.5
        )
        error = await self.expect_status(channel.unary(UNARY, s("x")), Code.UNAVAILABLE)
        self.assertIn(";", error.message)

    @staticmethod
    def unused_udp_port() -> int:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            sock.bind(("127.0.0.1", 0))
            return sock.getsockname()[1]


if __name__ == "__main__":
    unittest.main()
