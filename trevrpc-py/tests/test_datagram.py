"""Datagram messages of unreliable methods against the test server's native QUIC side, which records
which requests came as datagrams: flows, sending and its stream fallback, receiving, malformed
datagrams, DATAGRAM_COUNT, and the calls that get no flow."""

from __future__ import annotations

import asyncio
import unittest
from unittest import mock

import trevrpc
from protobuf.wkt.google.protobuf.wrappers_pb import StringValue
from trevrpc import Code, Method, MethodCompression, MethodKind
from trevrpc._datagram import SEND_QUEUE, DatagramFlow, datagram_frames
from trevrpc._quic import QuicTransport
from trevrpc._wire import StreamCompression

from .server import WAIT, TestServer, start_server


def method(
    kind: MethodKind = MethodKind.BIDI_STREAMING,
    *,
    requests: bool = True,
    responses: bool = True,
    compression: MethodCompression = MethodCompression.UNSPECIFIED,
    name: str = "Unreliable",
) -> Method[StringValue, StringValue]:
    return Method(
        f"/test.Echo/{name}",
        kind,
        StringValue,
        StringValue,
        unreliable_requests=requests,
        unreliable_responses=responses,
        compression=compression,
    )


UNRELIABLE = method()
LARGE = "".join(chr(33 + (i * 7919) % 90) for i in range(4096))
"""Too large for a datagram."""


class DatagramTest(unittest.IsolatedAsyncioTestCase):
    server: TestServer
    channel: trevrpc.Channel

    async def asyncSetUp(self) -> None:
        self.server = await start_server()
        self.addCleanup(self.server.close)
        self.channel = trevrpc.Channel(
            f"127.0.0.1:{self.server.port}",
            ca=self.server.ca,
            server_name="localhost",
            transport="quic",
        )
        self.addAsyncCleanup(self.channel.close)

    async def echo(
        self, values: list[str], m: Method[StringValue, StringValue] = UNRELIABLE
    ) -> tuple[list[str], trevrpc.Status]:
        """Sends `values`, reading each echo before the next send, then closes the requests."""
        responses: list[str] = []
        async with self.channel.bidi_stream(m) as call:
            for value in values:
                await call.send(StringValue(value=value))
                response = await asyncio.wait_for(call.recv(), WAIT)
                assert response is not None
                responses.append(response.value)
            await call.close_send()
            self.assertIsNone(await asyncio.wait_for(call.recv(), WAIT))
            status = await call.status()
        return responses, status


class FlowTest(DatagramTest):
    async def test_requests_and_responses_travel_as_datagrams(self) -> None:
        values = [str(i) for i in range(5)]
        responses, status = await self.echo(values)
        self.assertEqual(responses, values)
        call = self.server.calls[0]
        self.assertEqual(call.flow, 1)
        # Loopback loses nothing.
        self.assertEqual((call.datagram_requests, call.client_count), (5, 5))
        self.assertEqual(call.datagrams_sent, 5)
        self.assertEqual(status.metadata, {"received": b"5", "datagrams": b"5"})

    async def test_flows_count_up_on_the_connection(self) -> None:
        await self.echo(["a"])
        await self.echo(["b"])
        self.assertEqual([c.flow for c in self.server.calls], [1, 2])

    async def test_a_message_too_large_for_a_datagram_goes_on_the_stream(self) -> None:
        responses, _ = await self.echo(["small", LARGE])
        self.assertEqual(responses, ["small", LARGE])
        call = self.server.calls[0]
        # Only the small one was a datagram, and the count says so.
        self.assertEqual((call.datagram_requests, call.client_count), (1, 1))

    async def test_a_full_send_queue_sends_on_the_stream(self) -> None:
        # Every send finds aioquic's queue of unsent datagrams full.
        with mock.patch("trevrpc._quic.SEND_QUEUE", 0):
            responses, _ = await self.echo(["a", "b"])
        self.assertEqual(responses, ["a", "b"])
        call = self.server.calls[0]
        # Nothing went as a datagram, so the requests end without DATAGRAM_COUNT.
        self.assertEqual((call.datagram_requests, call.client_count), (0, 0))

    async def test_the_send_queue_counts_the_datagrams_aioquic_holds(self) -> None:
        await self.echo(["a"])
        connection = self.channel._connection  # pyright: ignore[reportPrivateUsage]
        assert isinstance(connection, QuicTransport)
        pending = connection._quic._datagrams_pending  # pyright: ignore[reportPrivateUsage]
        self.assertFalse(pending)
        # Nothing flushes between these lines, so aioquic holds what they queue.
        pending.extend([b"\x00"] * (SEND_QUEUE - 1))
        self.assertTrue(connection.send_datagram(b"\x00"))
        self.assertFalse(connection.send_datagram(b"\x00"))
        pending.clear()

    async def test_responses_only(self) -> None:
        responses, _ = await self.echo(["a", "b"], method(requests=False))
        self.assertEqual(responses, ["a", "b"])
        call = self.server.calls[0]
        self.assertNotEqual(call.flow, 0)
        self.assertEqual((call.datagram_requests, call.datagrams_sent), (0, 2))


class ReceiveTest(DatagramTest):
    async def test_a_datagram_before_the_headers_waits_for_them(self) -> None:
        self.server.datagram_before_headers = True
        events: list[str] = []
        call = self.channel.bidi_stream(
            UNRELIABLE, on_response_headers=lambda _: events.append("headers")
        )
        async with call:
            await call.send(StringValue(value="a"))
            response = await asyncio.wait_for(call.recv(), WAIT)
            assert response is not None
            events.append(response.value)
            await call.close_send()
            self.assertIsNone(await asyncio.wait_for(call.recv(), WAIT))
        self.assertEqual(events, ["headers", "a"])
        self.assertEqual(self.server.calls[0].datagrams_sent, 1)

    async def test_malformed_datagrams_are_dropped_and_counted(self) -> None:
        self.server.malformed_datagrams = True
        responses, _ = await self.echo(["a"])
        self.assertEqual(responses, ["a"])
        # Two malformed and one echo, all announced: nothing is waited for.
        self.assertEqual(self.server.calls[0].datagrams_sent, 3)

    async def test_the_call_waits_for_the_announced_count(self) -> None:
        self.server.datagram_count_extra = 2
        counts: list[int] = []
        await_count = DatagramFlow.await_count

        async def spy(flow: DatagramFlow, count: int) -> None:
            counts.append(count)
            await await_count(flow, count)

        with mock.patch.object(DatagramFlow, "await_count", spy):
            responses, status = await self.echo(["a"])
        self.assertEqual((responses, status.code), (["a"], Code.OK))
        # One echo was sent, and two more announced that never come.
        self.assertEqual(counts, [3])

    async def test_the_wait_ends_when_the_announced_datagrams_arrived(self) -> None:
        delivered: list[list[bytes]] = []
        connection = mock.Mock()
        flow = DatagramFlow(
            connection,
            send=False,
            receive=True,
            decode=lambda frame_type, payload: payload,
            deliver=delivered.append,
        )
        flow.open()
        datagram = bytes((0x00, 0x01)) + b"x"
        waiting = asyncio.create_task(flow.await_count(2), eager_start=True)
        flow.on_datagram(datagram)
        await asyncio.sleep(0)
        self.assertFalse(waiting.done())
        flow.on_datagram(datagram)
        # The arrival wakes the wait at once: one turn of the loop ends it, while one that ran
        # until its timeout would still be pending.
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        self.assertTrue(waiting.done())
        self.assertEqual(delivered, [[b"x"], [b"x"]])
        # Once closed, late datagrams are dropped.
        flow.close()
        flow.on_datagram(datagram)
        self.assertEqual(len(delivered), 2)

    def test_datagram_frames(self) -> None:
        message = bytes((0x00, 0x02)) + b"ab"
        unknown = bytes((0x21, 0x01)) + b"x"
        self.assertEqual(datagram_frames(message + unknown + message), [(0, b"ab"), (0, b"ab")])
        self.assertIsNone(datagram_frames(message[:-1]))
        self.assertIsNone(datagram_frames(bytes((0x05, 0x00))))
        self.assertEqual(datagram_frames(b""), [])


class NoFlowTest(DatagramTest):
    async def test_methods_without_unreliable_streamed_directions(self) -> None:
        await self.echo(["a"], method(requests=False, responses=False, name="Bidi"))
        reply = await self.channel.unary(
            method(MethodKind.UNARY, name="Unary"), StringValue(value="u")
        )
        self.assertEqual(reply.value, "u")
        self.assertEqual([c.flow for c in self.server.calls], [0, 0])

    async def test_qmux_has_no_datagrams(self) -> None:
        channel = trevrpc.Channel(
            f"127.0.0.1:{self.server.tcp_port}",
            ca=self.server.ca,
            server_name="localhost",
            transport="tcp",
        )
        self.addAsyncCleanup(channel.close)
        async with channel.bidi_stream(UNRELIABLE) as call:
            await call.send(StringValue(value="a"))
            response = await asyncio.wait_for(call.recv(), WAIT)
            assert response is not None
            self.assertEqual(response.value, "a")
            await call.close_send()
            self.assertIsNone(await asyncio.wait_for(call.recv(), WAIT))
        self.assertEqual(self.server.calls[0].flow, 0)

    async def test_a_call_with_a_flow_is_not_optimistic(self) -> None:
        m = method(requests=False, compression=MethodCompression.STREAM)
        await self.echo(["a"], m)
        call = self.server.calls[0]
        self.assertNotEqual(call.flow, 0)
        self.assertEqual(call.stream_compression, StreamCompression.ENABLED)


if __name__ == "__main__":
    unittest.main()
