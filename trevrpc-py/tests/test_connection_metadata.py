"""Connection metadata against the test server, which stores offered sets per connection, merges
them into each call's metadata, and records what each REQUEST carried: offers until a confirmation,
references after it, replaced and removed sets, new connections, late confirmations, and replays.
The `QMux` subclass runs the same tests over QMux over TLS."""

from __future__ import annotations

import asyncio
import unittest
from unittest import mock

import trevrpc
from protobuf.wkt.google.protobuf.wrappers_pb import StringValue
from trevrpc import Code, Method, MethodCompression, MethodKind, TrevRpcError
from trevrpc._stream import Stream

from .server import WAIT, TestServer, start_server

UNARY = Method("/test.Echo/Unary", MethodKind.UNARY, StringValue, StringValue)
BIDI = Method("/test.Echo/Bidi", MethodKind.BIDI_STREAMING, StringValue, StringValue)
ANSWER_LATER = Method(
    "/test.Echo/AnswerLater",
    MethodKind.CLIENT_STREAMING,
    StringValue,
    StringValue,
    compression=MethodCompression.STREAM,
)
REFUSE = Method(
    "/test.Echo/Refuse",
    MethodKind.CLIENT_STREAMING,
    StringValue,
    StringValue,
    compression=MethodCompression.STREAM,
)


class ConnectionMetadataTest(unittest.IsolatedAsyncioTestCase):
    server: TestServer
    channel: trevrpc.Channel
    transport: trevrpc.TransportPolicy = "quic"

    async def asyncSetUp(self) -> None:
        self.server = await start_server()
        self.addCleanup(self.server.close)
        self.channel = self.new_channel(connection_metadata={"suffix": "!"})

    def new_channel(self, **options: object) -> trevrpc.Channel:
        port = self.server.port if self.transport == "quic" else self.server.tcp_port
        channel = trevrpc.Channel(
            f"127.0.0.1:{port}",
            ca=self.server.ca,
            server_name="localhost",
            transport=self.transport,
            **options,  # pyright: ignore[reportArgumentType]
        )
        self.addAsyncCleanup(channel.close)
        return channel

    async def unary(self, value: str, **options: object) -> str:
        reply = await asyncio.wait_for(
            self.channel.unary(UNARY, StringValue(value=value), **options),  # pyright: ignore[reportArgumentType]
            WAIT,
        )
        return reply.value

    def carried(self) -> list[tuple[int, bool] | None]:
        """Each call's connection metadata: the set's ID, and whether it was an offer."""
        return [
            None
            if c.connection_metadata is None
            else (c.connection_metadata[0], bool(c.connection_metadata[1]))
            for c in self.server.calls
        ]

    async def test_offers_until_confirmed_then_refers(self) -> None:
        self.assertEqual([await self.unary(v) for v in "abc"], ["a!", "b!", "c!"])
        self.assertEqual(self.carried(), [(1, True), (1, False), (1, False)])

    async def test_the_calls_own_entry_wins(self) -> None:
        await self.unary("a")
        self.assertEqual(await self.unary("d", metadata={"suffix": "?"}), "d?")
        self.assertEqual(self.carried()[1], (1, False))
        self.assertEqual(self.server.calls[1].metadata, {"suffix": b"?"})

    async def test_a_replaced_set_is_offered_afresh(self) -> None:
        await self.unary("a")
        self.channel.set_connection_metadata({"suffix": "#"})
        self.assertEqual([await self.unary(v) for v in "bc"], ["b#", "c#"])
        self.assertEqual(self.carried(), [(1, True), (2, True), (2, False)])

    async def test_removing_the_set(self) -> None:
        await self.unary("a")
        self.channel.set_connection_metadata(None)
        self.assertEqual(await self.unary("b"), "b")
        self.assertEqual(self.carried(), [(1, True), None])

    async def test_calls_before_the_confirmation_all_offer(self) -> None:
        replies = await asyncio.gather(*(self.unary(v) for v in "abc"))
        self.assertEqual(sorted(replies), ["a!", "b!", "c!"])
        self.assertEqual(self.carried(), [(1, True)] * 3)

    async def test_a_new_connection_confirms_afresh(self) -> None:
        await self.unary("a")
        await self.unary("b")
        connection = self.channel._connection  # pyright: ignore[reportPrivateUsage]
        assert connection is not None
        self.server.connections[-1].close()
        await asyncio.wait_for(connection.closed, WAIT)
        self.assertEqual(await self.unary("c"), "c!")
        self.assertEqual(self.carried(), [(1, True), (1, False), (1, True)])

    async def test_a_late_confirmation_of_a_replaced_set_is_ignored(self) -> None:
        # The first call offers set 1, whose confirmation the server holds back.
        held = self.channel.client_stream(ANSWER_LATER)
        await held.send(StringValue(value="x"))
        await self.server.call()
        # Set 2 is offered and confirmed meanwhile.
        self.channel.set_connection_metadata({"suffix": "#"})
        self.assertEqual(await self.unary("a"), "a#")
        self.assertEqual(await self.unary("b"), "b#")
        # The late confirmation of set 1 must not undo set 2's.
        self.server.answer_later.set()
        await asyncio.wait_for(held.close_and_recv(), WAIT)
        self.assertEqual(await self.unary("c"), "c#")
        self.assertEqual(self.carried(), [(1, True), (2, True), (2, False), (2, False)])

    async def test_a_call_whose_set_was_replaced_offers_it_in_full(self) -> None:
        await self.unary("a")
        async with self.channel.bidi_stream(BIDI) as call:
            # Replaced after the call started, before its REQUEST went out: the server may have
            # dropped set 1, so the call offers it instead of referring to it.
            self.channel.set_connection_metadata({"suffix": "#"})
            await call.send(StringValue(value="x"))
            self.assertIsNotNone(await asyncio.wait_for(call.recv(), WAIT))
            await call.close_send()
        self.assertEqual(self.carried(), [(1, True), (1, True)])
        self.assertEqual(self.server.calls[1].metadata, {"suffix": b"!"})

    async def test_a_replay_keeps_the_calls_set(self) -> None:
        await self.unary("a")
        call = self.channel.client_stream(REFUSE)
        await call.send(StringValue(value="x"))
        self.assertEqual((await asyncio.wait_for(call.close_and_recv(), WAIT)).value, "x")
        refused, replay = self.server.calls[1:]
        self.assertTrue(refused.refused)
        self.assertEqual(self.carried()[1:], [(1, False), (1, False)])
        self.assertEqual(replay.metadata, {"suffix": b"!"})

    async def test_a_replay_offers_a_set_replaced_since_the_first_request(self) -> None:
        await self.unary("a")
        open_stream = self.channel.open_stream
        opened = 0

        async def replace_before_the_replay(max_frame_size: int) -> Stream:
            nonlocal opened
            opened += 1
            if opened == 2:
                # The refused call referred to set 1; it is replaced before the replay.
                self.channel.set_connection_metadata({"suffix": "#"})
            return await open_stream(max_frame_size)

        with mock.patch.object(self.channel, "open_stream", replace_before_the_replay):
            call = self.channel.client_stream(REFUSE)
            await call.send(StringValue(value="x"))
            self.assertEqual((await asyncio.wait_for(call.close_and_recv(), WAIT)).value, "x")
        # The replay offers set 1 in full, and keeps it.
        self.assertEqual(self.carried()[1:], [(1, False), (1, True)])
        self.assertEqual(self.server.calls[2].metadata, {"suffix": b"!"})

    async def test_an_invalid_set_is_refused(self) -> None:
        for metadata in ({"trevrpc-reserved": "x"}, {"bad key": "x"}, {"k": "x" * 9000}):
            with self.assertRaises(TrevRpcError) as caught:
                self.channel.set_connection_metadata(metadata)
            self.assertEqual(caught.exception.code, Code.INVALID_ARGUMENT, metadata)
            with self.assertRaises(TrevRpcError):
                self.new_channel(connection_metadata=metadata)
        # The current set is unchanged.
        self.assertEqual(await self.unary("a"), "a!")


class QMuxConnectionMetadataTest(ConnectionMetadataTest):
    transport = "tcp"


if __name__ == "__main__":
    unittest.main()
