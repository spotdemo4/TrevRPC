"""Compressed messages end to end against the test server, which records each message's frame
type: dictionary negotiation, compression without a dictionary, stream compression, optimistic
calls and their replay, the memory budget, and malformed compressed responses."""

from __future__ import annotations

import asyncio
import base64
import random
import unittest
from collections.abc import Awaitable, Callable
from unittest import mock

import trevrpc
from protobuf._file_registry import create_file_registry  # pyright: ignore[reportUnknownVariableType]
from protobuf.wkt.google.protobuf.descriptor_pb import (
    DescriptorProto,
    FieldDescriptorProto,
    FieldOptions,
    FileDescriptorProto,
)
from protobuf.wkt.google.protobuf.wrappers_pb import StringValue
from trevrpc import Code, Method, MethodCompression, MethodKind, TrevRpcError
from trevrpc import _compression as compression_module  # pyright: ignore[reportPrivateUsage]
from trevrpc._compression import DECODER_COST, ENCODER_COST, _redacted  # pyright: ignore[reportPrivateUsage]
from trevrpc._frames import FrameType
from trevrpc._qmux import QMuxSession
from trevrpc._stream import Stream
from trevrpc._wire import StreamCompression
from trevrpc._zstd import load_dictionary

from .server import WAIT, TestServer, start_server
from .test_zstd import D1, b

M, Z, S = FrameType.MESSAGE, FrameType.ZSTD_MESSAGE, FrameType.ZSTD_STREAM_MESSAGE
TYPE = "google.protobuf.StringValue"


def method(
    name: str, kind: MethodKind, compression: MethodCompression = MethodCompression.UNSPECIFIED
) -> Method[StringValue, StringValue]:
    return Method(f"/test.Echo/{name}", kind, StringValue, StringValue, compression=compression)


UNARY = method("Unary", MethodKind.UNARY)
PLAIN_UNARY = method("Unary", MethodKind.UNARY, MethodCompression.MESSAGE)
STREAM_BIDI = method("Bidi", MethodKind.BIDI_STREAMING, MethodCompression.STREAM)
STREAM_CLIENT = method("ClientStream", MethodKind.CLIENT_STREAMING, MethodCompression.STREAM)
STREAM_SERVER = method("ServerStream", MethodKind.SERVER_STREAMING, MethodCompression.STREAM)
REFUSE = method("Refuse", MethodKind.CLIENT_STREAMING, MethodCompression.STREAM)
ANSWER_LATER = method("AnswerLater", MethodKind.CLIENT_STREAMING, MethodCompression.STREAM)
NO_ANSWER = method("NoAnswer", MethodKind.CLIENT_STREAMING, MethodCompression.STREAM)
BOUNDED_ECHO = method("BoundedEcho", MethodKind.BIDI_STREAMING, MethodCompression.STREAM)
REFUSE_LATER = method("RefuseLater", MethodKind.CLIENT_STREAMING, MethodCompression.STREAM)
ANSWER_EARLY = method("AnswerEarly", MethodKind.BIDI_STREAMING, MethodCompression.STREAM)
ANSWER_EARLY_CLIENT = method("AnswerEarly", MethodKind.CLIENT_STREAMING, MethodCompression.STREAM)
STOP_EARLY = method("StopEarly", MethodKind.BIDI_STREAMING, MethodCompression.STREAM)


def record(i: int) -> str:
    """Like the documents dict.d1 was trained on."""
    return f'{{"service":"search","region":"us-east-1","status":404,"latency_ms":{100 + i}}}'


LARGE = b("plain.records.message").decode() * 2


class CompressionTest(unittest.IsolatedAsyncioTestCase):
    server: TestServer
    channel: trevrpc.Channel
    server_dictionaries = (D1,)
    client_dictionaries: tuple[trevrpc.Dictionary, ...] = (
        trevrpc.Dictionary(b("dict.d1.dictionary"), (TYPE,)),
    )

    async def asyncSetUp(self) -> None:
        self.server = await start_server(dictionaries=self.server_dictionaries)
        self.addCleanup(self.server.close)
        self.channel = self.new_channel()

    def new_channel(self, **options: object) -> trevrpc.Channel:
        settings: dict[str, object] = {
            "ca": self.server.ca,
            "server_name": "localhost",
            "transport": "quic",
            "compression": trevrpc.Compression(self.client_dictionaries),
        }
        settings.update(options)
        channel = trevrpc.Channel(f"127.0.0.1:{self.server.port}", **settings)  # pyright: ignore[reportArgumentType]
        self.addAsyncCleanup(channel.close)
        return channel

    async def echo_stream(
        self, channel: trevrpc.Channel, m: Method[StringValue, StringValue], count: int
    ) -> None:
        async with channel.bidi_stream(m) as call:
            for i in range(count):
                await call.send(StringValue(value=record(i)))
                response = await call.recv()
                assert response is not None
                self.assertEqual(response.value, record(i))
            await call.close_send()
            self.assertIsNone(await call.recv())

    async def client_stream(
        self, channel: trevrpc.Channel, m: Method[StringValue, StringValue], count: int
    ) -> None:
        call = channel.client_stream(m)
        for i in range(count):
            await call.send(StringValue(value=record(i)))
        response = await call.close_and_recv()
        self.assertEqual(response.value, "".join(record(i) for i in range(count)))


class DictionaryTest(CompressionTest):
    async def test_requests_compress_once_the_connection_is_answered(self) -> None:
        for i in range(2):
            reply = await self.channel.unary(UNARY, StringValue(value=record(i)))
            self.assertEqual(reply.value, record(i))
        first, second = self.server.calls
        self.assertEqual(first.query, [(0, b""), (1, b("dict.d1.fingerprint"))])
        self.assertEqual(first.accept, [1])
        self.assertEqual(first.request_frames, [M])
        # Responses use the response type's dictionary from the first call, which the answer
        # confirms in the same call.
        self.assertEqual(first.response_frames, [Z])
        self.assertEqual(second.query, [])
        self.assertEqual(second.request_frames, [Z])
        self.assertEqual(second.response_frames, [Z])

    async def test_a_dictionary_for_another_type_is_not_used(self) -> None:
        channel = trevrpc.Channel(
            f"127.0.0.1:{self.server.port}",
            ca=self.server.ca,
            server_name="localhost",
            compression=trevrpc.Compression(
                (trevrpc.Dictionary(b("dict.d1.dictionary"), ("example.Other",)),)
            ),
        )
        self.addAsyncCleanup(channel.close)
        for i in range(2):
            await channel.unary(UNARY, StringValue(value=record(i)))
        self.assertEqual([c.accept for c in self.server.calls], [[], []])
        self.assertEqual([c.request_frames for c in self.server.calls], [[M], [M]])


class FingerprintMismatchTest(CompressionTest):
    # The server's dictionary 1 differs in its last content byte, so its fingerprint differs.
    data = bytearray(b("dict.d1.dictionary"))
    data[-1] ^= 1
    server_dictionaries = (load_dictionary(bytes(data)),)

    async def test_a_reused_id_disables_the_dictionary(self) -> None:
        for i in range(2):
            reply = await self.channel.unary(UNARY, StringValue(value=record(i)))
            self.assertEqual(reply.value, record(i))
        first, second = self.server.calls
        self.assertEqual(first.response_frames, [M])
        self.assertEqual(second.accept, [])
        self.assertEqual(second.request_frames, [M])


class PlainTest(CompressionTest):
    client_dictionaries = ()

    async def test_large_messages_compress_on_methods_that_allow_it(self) -> None:
        for _ in range(2):
            reply = await self.channel.unary(PLAIN_UNARY, StringValue(value=LARGE))
            self.assertEqual(reply.value, LARGE)
        await self.channel.unary(PLAIN_UNARY, StringValue(value=record(0)))
        first, second, small = self.server.calls
        self.assertEqual(first.query, [(0, b"")])
        self.assertEqual(first.accept, [0])
        self.assertEqual((first.request_frames, first.response_frames), ([M], [Z]))
        self.assertEqual((second.request_frames, second.response_frames), ([Z], [Z]))
        # Under 256 bytes, nothing compresses without a dictionary.
        self.assertEqual((small.request_frames, small.response_frames), ([M], [M]))

    async def test_methods_without_the_option_neither_offer_nor_compress(self) -> None:
        for _ in range(2):
            await self.channel.unary(UNARY, StringValue(value=LARGE))
        for call in self.server.calls:
            self.assertEqual((call.query, call.accept), ([], []))
            self.assertEqual((call.request_frames, call.response_frames), ([M], [M]))

    async def test_without_zstd_nothing_is_offered(self) -> None:
        with mock.patch.object(compression_module, "AVAILABLE", False):
            channel = self.new_channel(compression=trevrpc.Compression())
        self.assertFalse(channel.compression.available)
        for _ in range(2):
            await channel.unary(PLAIN_UNARY, StringValue(value=LARGE))
        await self.echo_stream(channel, STREAM_BIDI, 3)
        for call in self.server.calls:
            self.assertEqual((call.query, call.accept, call.stream_compression), ([], [], 0))
            self.assertEqual(set(call.request_frames), {M})

    def test_redacted_types_are_excluded(self) -> None:
        string, message = FieldDescriptorProto.Type.STRING, FieldDescriptorProto.Type.MESSAGE
        optional, repeated = (
            FieldDescriptorProto.Label.OPTIONAL,
            FieldDescriptorProto.Label.REPEATED,
        )
        registry = create_file_registry(
            FileDescriptorProto(
                name="redact.proto",
                package="test",
                syntax="proto3",
                message_type=[
                    DescriptorProto(
                        name="Secret",
                        field=[
                            FieldDescriptorProto(
                                name="token",
                                number=1,
                                type=string,
                                label=optional,
                                options=FieldOptions(debug_redact=True),
                            )
                        ],
                    ),
                    DescriptorProto(
                        name="Outer",
                        field=[
                            FieldDescriptorProto(
                                name="secrets",
                                number=1,
                                type=message,
                                label=repeated,
                                type_name=".test.Secret",
                            ),
                            FieldDescriptorProto(
                                name="next",
                                number=2,
                                type=message,
                                label=optional,
                                type_name=".test.Outer",
                            ),
                        ],
                    ),
                ],
            ),
            lambda name: None,
        )
        for name in ("test.Secret", "test.Outer"):
            message_desc = registry.message(name)
            assert message_desc is not None
            self.assertTrue(_redacted(message_desc), name)
        self.assertFalse(_redacted(StringValue.desc()))


class StreamTest(CompressionTest):
    client_dictionaries = ()

    async def test_optimistic_requests_and_streamed_responses(self) -> None:
        await self.echo_stream(self.channel, STREAM_BIDI, 6)
        call = self.server.calls[0]
        self.assertEqual(call.stream_compression, StreamCompression.OPTIMISTIC)
        # From each direction's third message.
        self.assertEqual(call.request_frames, [M, M, S, S, S, S])
        self.assertEqual(call.response_frames, [M, M, S, S, S, S])

    async def test_server_streamed_responses(self) -> None:
        async with self.channel.server_stream(STREAM_SERVER, StringValue(value="10")) as stream:
            values = [r.value async for r in stream]
        self.assertEqual(values, [str(i) for i in range(10)])
        call = self.server.calls[0]
        # The request does not stream, so the call is not optimistic.
        self.assertEqual(call.stream_compression, StreamCompression.ENABLED)
        self.assertEqual(call.response_frames, [M, M] + [S] * 8)

    async def test_a_refused_call_is_replayed_and_optimism_returns_after_an_answer(self) -> None:
        await self.client_stream(self.channel, REFUSE, 8)
        refused, replay = self.server.calls
        self.assertTrue(refused.refused)
        self.assertEqual(refused.stream_compression, StreamCompression.OPTIMISTIC)
        self.assertEqual(replay.stream_compression, StreamCompression.ENABLED)
        # Each request once, none stream-compressed: the server never answered the replay.
        self.assertEqual(replay.request_frames, [M] * 8)
        # The next call waits for its answer before compressing...
        await self.echo_stream(self.channel, STREAM_BIDI, 4)
        waiting = self.server.calls[2]
        self.assertEqual(waiting.stream_compression, StreamCompression.ENABLED)
        self.assertIn(S, waiting.request_frames)
        # ...which makes the one after it optimistic again.
        await self.client_stream(self.channel, STREAM_CLIENT, 8)
        optimistic = self.server.calls[3]
        self.assertEqual(optimistic.stream_compression, StreamCompression.OPTIMISTIC)
        self.assertEqual(optimistic.request_frames, [M, M] + [S] * 6)

    async def test_a_send_without_room_for_its_copy_waits_for_the_answer(self) -> None:
        payload = "x" * (64 * 1024)
        call = self.channel.client_stream(ANSWER_LATER)

        async def send_all() -> None:
            for _ in range(24):
                await call.send(StringValue(value=payload))

        sending = asyncio.create_task(send_all())
        server_call = await self.server.call()
        # 1 MiB of copies holds 15 such requests.
        await self.server.changed.wait_until(lambda: len(server_call.request_frames) >= 15)
        self.assertFalse(sending.done())
        self.assertEqual(len(server_call.request_frames), 15)
        self.server.answer_later.set()
        await asyncio.wait_for(sending, WAIT)
        response = await call.close_and_recv()
        self.assertEqual(len(response.value), 24 * len(payload))

    async def test_an_optimistic_response_must_start_with_an_answer(self) -> None:
        call = self.channel.client_stream(NO_ANSWER)
        await call.send(StringValue(value="a"))
        with self.assertRaises(TrevRpcError) as caught:
            await asyncio.wait_for(call.close_and_recv(), WAIT)
        self.assertEqual(caught.exception.code, Code.INTERNAL)


class ReplayTest(CompressionTest):
    client_dictionaries = ()

    def patch_open_stream(
        self, channel: trevrpc.Channel, on_replay: Callable[[], Awaitable[None]]
    ) -> mock._patch[object]:  # pyright: ignore[reportPrivateUsage]
        """Runs `on_replay` when the call's second stream, its replay's, is opened."""
        open_stream = channel.open_stream
        opened = 0

        async def wrapped(max_frame_size: int) -> Stream:
            nonlocal opened
            opened += 1
            if opened == 2:
                await on_replay()
            return await open_stream(max_frame_size)

        return mock.patch.object(channel, "open_stream", wrapped)

    async def test_the_replay_carries_the_time_left_of_the_deadline(self) -> None:
        call = self.channel.client_stream(REFUSE, timeout=WAIT)
        for i in range(3):
            await call.send(StringValue(value=record(i)))
        await call.close_and_recv()
        refused, replay = self.server.calls
        self.assertGreater(refused.timeout_nanos, 0)
        self.assertLess(replay.timeout_nanos, refused.timeout_nanos)

    async def test_a_replay_never_moves_to_another_connection(self) -> None:
        channel = self.channel

        async def reconnect() -> None:
            connection = channel._connection  # pyright: ignore[reportPrivateUsage]
            assert connection is not None
            connection.close()

        with self.patch_open_stream(channel, reconnect):
            call = channel.client_stream(REFUSE)
            await call.send(StringValue(value=record(0)))
            with self.assertRaises(TrevRpcError) as caught:
                await asyncio.wait_for(call.close_and_recv(), WAIT)
        self.assertEqual(caught.exception.code, Code.UNAVAILABLE, caught.exception)
        # The refused call only: nothing was replayed on the new connection.
        self.assertEqual(len(self.server.calls), 1)

    async def test_cancelling_during_the_replay_wakes_waiting_sends(self) -> None:
        entered, release = asyncio.Event(), asyncio.Event()
        self.addCleanup(release.set)

        async def stall() -> None:
            entered.set()
            await release.wait()

        with self.patch_open_stream(self.channel, stall):
            call = self.channel.client_stream(REFUSE)
            await call.send(StringValue(value=record(0)))
            await asyncio.wait_for(entered.wait(), WAIT)
            # Runs until it waits for the replay.
            sending = asyncio.create_task(call.send(StringValue(value=record(1))), eager_start=True)
            self.assertFalse(sending.done())
            await call.aclose()
            with self.assertRaises(TrevRpcError) as caught:
                await asyncio.wait_for(sending, WAIT)
        self.assertEqual(caught.exception.code, Code.CANCELLED)

    async def test_a_send_blocked_on_the_refused_stream_succeeds_through_the_replay(self) -> None:
        server = await start_server(qmux_stream_window=16 * 1024)
        self.addCleanup(server.close)
        channel = trevrpc.Channel(
            f"127.0.0.1:{server.tcp_port}", ca=server.ca, server_name="localhost", transport="tcp"
        )
        self.addAsyncCleanup(channel.close)
        blocked = asyncio.Event()
        write = QMuxSession.write

        def watched(session: QMuxSession, stream: Stream, data: bytes, end: bool) -> bool:
            room = write(session, stream, data, end)
            if not room and session._client:  # pyright: ignore[reportPrivateUsage]
                blocked.set()
            return room

        generator = random.Random(2)
        values = [base64.b64encode(generator.randbytes(6 * 1024)).decode() for _ in range(40)]
        call = channel.client_stream(REFUSE_LATER)

        async def send_all() -> None:
            for value in values:
                await call.send(StringValue(value=value))

        with mock.patch.object(QMuxSession, "write", watched):
            sending = asyncio.create_task(send_all())
            # The server reads nothing, so a send waits on the refused stream...
            await asyncio.wait_for(blocked.wait(), WAIT)
            self.assertFalse(sending.done())
            # ...when the refusal arrives.
            server.refuse_now.set()
            await asyncio.wait_for(sending, WAIT)
        response = await asyncio.wait_for(call.close_and_recv(), WAIT)
        self.assertEqual(response.value, "".join(values))
        refused, replay = server.calls
        self.assertTrue(refused.refused)
        self.assertEqual(len(replay.request_frames), len(values))

    async def early_channel(self) -> trevrpc.Channel:
        server = await start_server(qmux_stream_window=16 * 1024)
        self.addCleanup(server.close)
        server.refuse_after = 20
        channel = trevrpc.Channel(
            f"127.0.0.1:{server.tcp_port}", ca=server.ca, server_name="localhost", transport="tcp"
        )
        self.addAsyncCleanup(channel.close)
        return channel

    @staticmethod
    def early_values() -> list[StringValue]:
        """More than the server's window, which it stops reading after the replay's first."""
        generator = random.Random(3)
        return [
            StringValue(value=base64.b64encode(generator.randbytes(6 * 1024)).decode())
            for _ in range(20)
        ]

    async def test_a_response_before_the_replay_is_written_is_kept(self) -> None:
        channel = await self.early_channel()
        async with channel.bidi_stream(ANSWER_EARLY) as call:
            for value in self.early_values():
                await call.send(value)
            # The request stream is still open, so the replay's write fails when the call ends.
            await asyncio.wait_for(call.status(), WAIT)
            task = call._bidi._replay_task  # pyright: ignore[reportPrivateUsage]
            assert task is not None
            await asyncio.wait({task}, timeout=WAIT)
            response = await asyncio.wait_for(call.recv(), WAIT)
            assert response is not None
            self.assertEqual(response.value, "early")
            self.assertIsNone(await asyncio.wait_for(call.recv(), WAIT))

    async def test_a_replay_stopped_by_the_server_reports_its_status(self) -> None:
        channel = await self.early_channel()
        async with channel.bidi_stream(STOP_EARLY) as call:
            for value in self.early_values():
                await call.send(value)
            with self.assertRaises(TrevRpcError) as caught:
                await asyncio.wait_for(call.recv(), WAIT)
        self.assertEqual(
            (caught.exception.code, caught.exception.message), (Code.FAILED_PRECONDITION, "stopped")
        )

    async def test_a_finished_call_abandons_its_replay(self) -> None:
        channel = await self.early_channel()
        call = channel.client_stream(ANSWER_EARLY_CLIENT)
        for value in self.early_values():
            await call.send(value)
        # Closed first: the replay's write carries the end of the request stream.
        response = await asyncio.wait_for(call.close_and_recv(), WAIT)
        self.assertEqual(response.value, "early")
        task = call._call._replay_task  # pyright: ignore[reportPrivateUsage]
        assert task is not None
        await asyncio.wait({task}, timeout=WAIT)
        self.assertTrue(task.done())

    async def test_the_replay_is_written_while_responses_are_read(self) -> None:
        """A server that echoes, and stops reading requests while its echoes are not read, takes
        a replay larger than its request window only if the client reads meanwhile."""
        server = await start_server(qmux_stream_window=16 * 1024)
        self.addCleanup(server.close)
        server.refuse_after = 100
        channel = trevrpc.Channel(
            f"127.0.0.1:{server.tcp_port}",
            ca=server.ca,
            server_name="localhost",
            transport="tcp",
            max_frame_size=8 * 1024,
        )
        self.addAsyncCleanup(channel.close)
        # Incompressible and all different, so that the echoes fill the client's window even
        # stream-compressed.
        generator = random.Random(1)
        values = [base64.b64encode(generator.randbytes(1500)).decode() for _ in range(100)]
        responses: list[str] = []
        async with channel.bidi_stream(BOUNDED_ECHO) as call:

            async def read() -> None:
                while (response := await call.recv()) is not None:
                    responses.append(response.value)

            reader = asyncio.create_task(read())
            for value in values:
                await call.send(StringValue(value=value))
            await asyncio.wait_for(call.close_send(), WAIT)
            await asyncio.wait_for(reader, WAIT)
        self.assertEqual(responses, values)
        refused, replay = server.calls
        self.assertTrue(refused.refused)
        self.assertEqual(len(replay.request_frames), 100)


class BudgetTest(CompressionTest):
    client_dictionaries = ()

    async def test_the_budget_is_released(self) -> None:
        await self.echo_stream(self.channel, STREAM_BIDI, 5)
        await self.client_stream(self.channel, REFUSE, 4)
        budget = self.channel.stream_budget
        assert budget is not None
        self.assertEqual(budget.used, 0)

    async def test_without_room_to_decode_a_call_offers_nothing(self) -> None:
        channel = self.new_channel(stream_compression_memory=DECODER_COST - 1)
        await self.echo_stream(channel, STREAM_BIDI, 4)
        call = self.server.calls[0]
        self.assertEqual(call.stream_compression, StreamCompression.NONE)
        self.assertNotIn(S, call.request_frames + call.response_frames)

    async def test_without_room_to_compress_requests_go_per_message(self) -> None:
        channel = self.new_channel(stream_compression_memory=ENCODER_COST - 1)
        await self.echo_stream(channel, STREAM_BIDI, 4)
        call = self.server.calls[0]
        # Room for the response decoder only: not optimistic, and no compressing context.
        self.assertEqual(call.stream_compression, StreamCompression.ENABLED)
        self.assertNotIn(S, call.request_frames)
        self.assertEqual(call.response_frames, [M, M, S, S])

    async def test_a_negative_budget_turns_stream_compression_off(self) -> None:
        channel = self.new_channel(stream_compression_memory=-1)
        self.assertIsNone(channel.stream_budget)
        await self.echo_stream(channel, STREAM_BIDI, 4)
        self.assertEqual(self.server.calls[0].stream_compression, StreamCompression.NONE)


class MalformedTest(CompressionTest):
    async def expect_code(self, name: str, kind: MethodKind, code: Code) -> None:
        m = method(name, kind, MethodCompression.MESSAGE)
        with self.assertRaises(TrevRpcError) as caught:
            if kind == MethodKind.UNARY:
                await asyncio.wait_for(self.channel.unary(m, StringValue(value="a")), WAIT)
            else:
                async with self.channel.server_stream(m, StringValue(value="a")) as stream:
                    await asyncio.wait_for(stream.recv(), WAIT)
        self.assertEqual(caught.exception.code, code, caught.exception)

    async def test_an_invalid_compressed_response_is_internal(self) -> None:
        await self.expect_code("BadZstd", MethodKind.UNARY, Code.INTERNAL)

    async def test_a_compressed_response_too_large_is_resource_exhausted(self) -> None:
        await self.expect_code("TooLarge", MethodKind.UNARY, Code.RESOURCE_EXHAUSTED)

    async def test_a_stream_compressed_response_not_enabled_is_internal(self) -> None:
        await self.expect_code("UnaskedStream", MethodKind.SERVER_STREAMING, Code.INTERNAL)


if __name__ == "__main__":
    unittest.main()
