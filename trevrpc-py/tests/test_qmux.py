"""QMux against testdata/qmux-golden-vectors.txt, and the session's protocol checks against a raw
peer that writes records by hand."""

from __future__ import annotations

import asyncio
import os
import signal
import unittest
from pathlib import Path
from unittest import mock

from trevrpc import Code, TrevRpcError
from trevrpc._qmux import (
    APPLICATION_CLOSE,
    CONNECTION_CLOSE,
    PING,
    PING_RESPONSE,
    RECORD_SIZE,
    ErrorCode,
    Frame,
    Parameters,
    QMuxError,
    QMuxSession,
    application_close_frame,
    max_data_frame,
    max_stream_data_frame,
    max_streams_bidi_frame,
    parse_frames,
    ping_frame,
    record,
    reset_stream_frame,
    stop_sending_frame,
    stream_frame,
    transport_close_frame,
)
from trevrpc._varint import decode_varint, encode_varint

from .server import WAIT


def _golden() -> dict[str, bytes]:
    path = os.environ.get("TREVRPC_QMUX_GOLDEN") or str(
        Path(__file__).resolve().parents[2] / "testdata" / "qmux-golden-vectors.txt"
    )
    vectors: dict[str, bytes] = {}
    for line in Path(path).read_text().splitlines():
        if line and not line.startswith("#"):
            name, _, value = line.partition("=")
            vectors[name.strip()] = bytes.fromhex(value.strip())
    return vectors


GOLDEN = _golden()

SERVER_PARAMETERS = Parameters(
    max_idle_timeout=30000,
    initial_max_data=16777216,
    initial_max_stream_data_bidi_remote=4194308,
    initial_max_streams_bidi=65,
    max_record_size=65536,
)
CLIENT_PARAMETERS = Parameters(
    initial_max_data=4194308, initial_max_stream_data_bidi_local=4194308, max_record_size=65536
)


class GoldenTest(unittest.TestCase):
    def test_varints(self) -> None:
        for name, value in (
            ("varint.1byte", 37),
            ("varint.2byte", 15293),
            ("varint.4byte", 494878333),
            ("varint.8byte", 151288809941952652),
        ):
            self.assertEqual(encode_varint(value).hex(), GOLDEN[name].hex(), name)
            self.assertEqual(decode_varint(GOLDEN[name], 0), (value, len(GOLDEN[name])), name)

    def test_frames_encode_and_decode(self) -> None:
        encoded = {
            "frame.stream.fin": stream_frame(0, 0, b"hi", True),
            "frame.stream.offset": stream_frame(4, 16, b"hi", False),
            "frame.reset_stream": reset_stream_frame(0, 1, 5),
            "frame.stop_sending": stop_sending_frame(0, 1),
            "frame.max_data": max_data_frame(65),
            "frame.max_stream_data": max_stream_data_frame(4, 1028),
            "frame.max_streams_bidi": max_streams_bidi_frame(65),
            "frame.application_close": application_close_frame(0, "bye"),
            "frame.transport_close": transport_close_frame(ErrorCode.PROTOCOL_VIOLATION, 0x08, ""),
            "frame.ping": ping_frame(PING, 1),
            "frame.ping_response": ping_frame(PING_RESPONSE, 1),
            "params.server": SERVER_PARAMETERS.encode(),
            "params.client": CLIENT_PARAMETERS.encode(),
        }
        for name, data in encoded.items():
            self.assertEqual(data.hex(), GOLDEN[name].hex(), name)
            self.assertEqual(len(list(parse_frames(GOLDEN[name]))), 1, name)
        stream = next(parse_frames(GOLDEN["frame.stream.offset"]))
        self.assertEqual(
            (stream.stream_id, stream.offset, stream.data, stream.fin), (4, 16, b"hi", False)
        )
        close = next(parse_frames(GOLDEN["frame.transport_close"]))
        self.assertEqual((close.type, close.code, close.frame_type), (CONNECTION_CLOSE, 0x0A, 0x08))

    def test_parameters_decode(self) -> None:
        for name, expected in (
            ("params.server", SERVER_PARAMETERS),
            ("params.client", CLIENT_PARAMETERS),
        ):
            frame = next(parse_frames(GOLDEN[name]))
            self.assertEqual(Parameters.decode(frame.data), expected, name)

    def test_records_and_messages(self) -> None:
        frames = stream_frame(0, 0, b"hi", True) + max_data_frame(65)
        self.assertEqual(frames.hex(), GOLDEN["message.stream_and_max_data"].hex())
        self.assertEqual(record(frames).hex(), GOLDEN["record.stream_and_max_data"].hex())

    def test_invalid_frames_are_rejected_with_their_error(self) -> None:
        codes = {
            "frame_encoding": ErrorCode.FRAME_ENCODING_ERROR,
            "protocol_violation": ErrorCode.PROTOCOL_VIOLATION,
            "transport_parameter": ErrorCode.TRANSPORT_PARAMETER_ERROR,
        }
        names = [name for name in GOLDEN if name.startswith("invalid.")]
        self.assertTrue(names)
        for name in names:
            expected = codes[name.split(".")[1]]

            def decode(data: bytes) -> None:
                for frame in parse_frames(data):
                    if frame.type == 0x3F5153300D0A0D0A:
                        Parameters.decode(frame.data)
                    elif frame.type in (0x30, 0x31):
                        # The session refuses DATAGRAM frames, as it advertises none.
                        raise QMuxError(ErrorCode.PROTOCOL_VIOLATION, frame.type, "datagram")

            with self.assertRaises(QMuxError, msg=name) as caught:
                decode(GOLDEN[name])
            self.assertEqual(caught.exception.code, expected, name)


class RawPeer:
    """The server side of a plain TCP connection, which tests drive record by record against a
    client session."""

    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        self.reader = reader
        self.writer = writer

    def send(self, *frames: bytes) -> None:
        self.writer.write(record(b"".join(frames)))

    async def receive(self) -> list[tuple[int, Frame]]:
        """The frames of the next record, as (type, frame)."""
        first = await self.reader.readexactly(1)
        length = 1 << (first[0] >> 6)
        header = first + await self.reader.readexactly(length - 1)
        size, _ = decode_varint(header, 0) or (0, 0)
        data = await self.reader.readexactly(size)
        return [(frame.type, frame) for frame in parse_frames(data)]

    async def receive_until(self, frame_type: int) -> Frame:
        async with asyncio.timeout(WAIT):
            while True:
                for received_type, frame in await self.receive():
                    if received_type == frame_type:
                        return frame


class SessionTest(unittest.IsolatedAsyncioTestCase):
    async def start(
        self, parameters: Parameters = SERVER_PARAMETERS, *, idle_timeout: int = 0
    ) -> tuple[QMuxSession, RawPeer]:
        accepted: asyncio.Future[RawPeer] = asyncio.get_running_loop().create_future()

        async def serve(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
            accepted.set_result(RawPeer(reader, writer))

        server = await asyncio.start_server(serve, "127.0.0.1", 0)
        self.addAsyncCleanup(self.stop, server)
        port = server.sockets[0].getsockname()[1]
        reader, writer = await asyncio.open_connection("127.0.0.1", port)
        local = Parameters(
            max_idle_timeout=idle_timeout,
            initial_max_data=1024,
            initial_max_stream_data_bidi_local=1024,
            max_record_size=RECORD_SIZE,
        )
        session = QMuxSession(reader, writer, client=True, parameters=local, max_frame_size=1000)
        self.addCleanup(session.close)
        peer = await asyncio.wait_for(accepted, WAIT)
        first = await peer.receive()
        self.assertEqual(first[0][0], 0x3F5153300D0A0D0A)
        peer.send(parameters.encode())
        await asyncio.wait_for(asyncio.shield(session._handshake), WAIT)  # pyright: ignore[reportPrivateUsage]
        return session, peer

    @staticmethod
    async def stop(server: asyncio.Server) -> None:
        server.close()

    async def expect_closed(self, session: QMuxSession, peer: RawPeer, code: ErrorCode) -> None:
        close = await peer.receive_until(CONNECTION_CLOSE)
        self.assertEqual(close.code, code)
        await asyncio.wait_for(session.closed, WAIT)
        assert session.error is not None
        self.assertEqual(session.error.code, Code.UNAVAILABLE)

    async def test_first_frame_must_be_the_parameters(self) -> None:
        accepted: asyncio.Future[RawPeer] = asyncio.get_running_loop().create_future()

        async def serve(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
            accepted.set_result(RawPeer(reader, writer))

        server = await asyncio.start_server(serve, "127.0.0.1", 0)
        self.addAsyncCleanup(self.stop, server)
        reader, writer = await asyncio.open_connection(
            "127.0.0.1", server.sockets[0].getsockname()[1]
        )
        session = QMuxSession(
            reader, writer, client=True, parameters=CLIENT_PARAMETERS, max_frame_size=1000
        )
        peer = await asyncio.wait_for(accepted, WAIT)
        peer.send(max_data_frame(65))
        await self.expect_closed(session, peer, ErrorCode.PROTOCOL_VIOLATION)

    async def test_refuses_streams_the_server_opens(self) -> None:
        session, peer = await self.start()
        peer.send(stream_frame(1, 0, b"x", False))
        await self.expect_closed(session, peer, ErrorCode.STREAM_LIMIT_ERROR)

    async def test_refuses_data_beyond_the_stream_window(self) -> None:
        session, peer = await self.start()
        stream = await session.open_stream(1000)
        await stream.write(b"request")
        peer.send(stream_frame(0, 0, b"x" * 1025, False))
        await self.expect_closed(session, peer, ErrorCode.FLOW_CONTROL_ERROR)

    async def test_refuses_datagrams(self) -> None:
        session, peer = await self.start()
        peer.send(GOLDEN["invalid.protocol_violation.datagram"])
        await self.expect_closed(session, peer, ErrorCode.PROTOCOL_VIOLATION)

    async def test_refuses_oversized_records(self) -> None:
        session, peer = await self.start()
        peer.writer.write(encode_varint(RECORD_SIZE + 1))
        await self.expect_closed(session, peer, ErrorCode.FRAME_ENCODING_ERROR)

    async def test_answers_pings_and_refuses_repeated_sequences(self) -> None:
        session, peer = await self.start()
        peer.send(ping_frame(PING, 0))
        response = await peer.receive_until(PING_RESPONSE)
        self.assertEqual(response.sequence, 0)
        peer.send(ping_frame(PING, 0))
        await self.expect_closed(session, peer, ErrorCode.PROTOCOL_VIOLATION)

    async def test_streams_wait_for_the_stream_limit(self) -> None:
        session, peer = await self.start(
            Parameters(
                initial_max_data=1 << 20,
                initial_max_stream_data_bidi_remote=1 << 20,
                initial_max_streams_bidi=1,
                max_record_size=RECORD_SIZE,
            )
        )
        first = await session.open_stream(1000)
        self.assertEqual(first.id, 0)
        # Runs until it waits for credit.
        second = asyncio.create_task(session.open_stream(1000), eager_start=True)
        self.assertFalse(second.done())
        self.assertEqual(len(session._open_waiters), 1)  # pyright: ignore[reportPrivateUsage]
        peer.send(max_streams_bidi_frame(2))
        self.assertEqual((await asyncio.wait_for(second, WAIT)).id, 4)

    async def test_credit_follows_what_the_stream_reads(self) -> None:
        session, peer = await self.start()
        stream = await session.open_stream(1000)
        await stream.write(b"request")
        # A MESSAGE frame of 603 bytes: more than half the 1024-byte window.
        peer.send(stream_frame(0, 0, bytes((0, 0x42, 0x58)) + b"x" * 600, False))
        frame = await stream.read_frame()
        assert frame is not None
        credit = await peer.receive_until(0x11)  # MAX_STREAM_DATA
        self.assertEqual(credit.maximum, 603 + 1024)

    def fake_clock(self, session: QMuxSession) -> list[float]:
        """Replaces the session's idle clock; tests advance `clock[0]` and run its timer."""
        clock = [session._now()]  # pyright: ignore[reportPrivateUsage]
        session._now = lambda: clock[0]  # pyright: ignore[reportPrivateUsage]
        return clock

    async def test_idle_timeout(self) -> None:
        session, _ = await self.start(idle_timeout=10_000)
        clock = self.fake_clock(session)
        clock[0] += 9.9
        session._on_timer()  # pyright: ignore[reportPrivateUsage]
        self.assertIsNone(session.error)
        clock[0] += 0.1
        session._on_timer()  # pyright: ignore[reportPrivateUsage]
        assert session.error is not None
        self.assertIn("idle", session.error.message)

    async def test_a_session_that_only_sends_stays_open(self) -> None:
        """Every record sent restarts the idle timer (draft-02 section 7.1), so a session that
        keeps sending stays open while its peer sends nothing back."""
        session, peer = await self.start(idle_timeout=10_000)
        clock = self.fake_clock(session)
        stream = await session.open_stream(1000)
        clock[0] += 7.5
        await stream.write(b"x")
        await peer.receive_until(0x0A)  # STREAM with a length
        # 15 s after the peer was last heard from, 7.5 s after the last record sent.
        clock[0] += 7.5
        session._on_timer()  # pyright: ignore[reportPrivateUsage]
        self.assertIsNone(session.error)
        clock[0] += 2.5
        session._on_timer()  # pyright: ignore[reportPrivateUsage]
        self.assertIsNotNone(session.error)

    async def test_waits_for_connection_credit_without_spinning(self) -> None:
        """A stream with data and stream credit, but no connection credit, waits for MAX_DATA:
        the peer, on the same event loop, gets to run and grant it."""
        session, peer = await self.start(
            Parameters(
                initial_max_data=1000,
                initial_max_stream_data_bidi_remote=1 << 20,
                initial_max_streams_bidi=4,
                max_record_size=RECORD_SIZE,
            )
        )
        stream = await session.open_stream(1000)

        # A writer that spins starves the event loop, whose own timeouts then never fire: a
        # SIGALRM interrupts it instead, so the test fails rather than hangs.
        def spun(signum: int, frame: object) -> None:
            raise TimeoutError("the writer spun without yielding")

        previous = signal.signal(signal.SIGALRM, spun)
        signal.setitimer(signal.ITIMER_REAL, WAIT)
        self.addCleanup(signal.signal, signal.SIGALRM, previous)
        self.addCleanup(signal.setitimer, signal.ITIMER_REAL, 0)
        await stream.write(b"y" * 5000, end=True)

        async def receive(total: int) -> bool:
            received = 0
            fin = False
            while received < total:
                for frame_type, frame in await peer.receive():
                    if 0x08 <= frame_type <= 0x0F:
                        received += len(frame.data)
                        fin = frame.fin
            self.assertEqual(received, total)
            return fin

        self.assertFalse(await asyncio.wait_for(receive(1000), WAIT))
        peer.send(max_data_frame(5000))
        self.assertTrue(await asyncio.wait_for(receive(4000), WAIT))

    async def test_close_aborts_a_writer_the_peer_holds_up(self) -> None:
        """A peer that stops reading holds the writer in drain(): close() still ends the
        connection, after its grace period."""
        session, _ = await self.start()
        writer = session._writer  # pyright: ignore[reportPrivateUsage]
        parked = asyncio.Event()

        async def stuck() -> None:
            parked.set()
            await asyncio.get_running_loop().create_future()

        writer.drain = stuck
        stream = await session.open_stream(1000)
        await stream.write(b"x")
        await asyncio.wait_for(parked.wait(), WAIT)
        session._close_grace = 0.05  # pyright: ignore[reportPrivateUsage]
        session.close()
        task = session._writer_task  # pyright: ignore[reportPrivateUsage]
        await asyncio.wait({task}, timeout=WAIT)
        self.assertTrue(task.done())
        self.assertTrue(writer.transport.is_closing())

    async def test_idle_timeout_aborts_a_connection_the_peer_holds_up(self) -> None:
        """A peer that stops reading leaves data buffered, which closing the transport waits to
        send forever: the idle timeout aborts the connection after the grace period."""
        session, _ = await self.start(idle_timeout=10_000)
        clock = self.fake_clock(session)
        transport = session._writer.transport  # pyright: ignore[reportPrivateUsage]
        aborted = asyncio.Event()
        abort = transport.abort

        def spy() -> None:
            aborted.set()
            abort()

        with (
            mock.patch.object(transport, "get_write_buffer_size", return_value=RECORD_SIZE),
            mock.patch.object(transport, "close"),
            mock.patch.object(transport, "abort", spy),
        ):
            session._close_grace = 0.05  # pyright: ignore[reportPrivateUsage]
            clock[0] += 10
            session._on_timer()  # pyright: ignore[reportPrivateUsage]
            self.assertIsNotNone(session.error)
            await asyncio.wait_for(aborted.wait(), WAIT)

    async def test_a_clean_close_disarms_the_abort(self) -> None:
        session, peer = await self.start()
        session._close_grace = 0.05  # pyright: ignore[reportPrivateUsage]
        session.close()
        await peer.receive_until(APPLICATION_CLOSE)
        peer.writer.close()
        await session.closed
        await asyncio.wait_for(session._writer_task, WAIT)  # pyright: ignore[reportPrivateUsage]
        closed = session._closed_task  # pyright: ignore[reportPrivateUsage]
        assert closed is not None
        await asyncio.wait_for(closed, WAIT)
        timer = session._abort_timer  # pyright: ignore[reportPrivateUsage]
        self.assertTrue(timer is None or timer.cancelled())

    async def test_close_sends_an_application_close(self) -> None:
        session, peer = await self.start()
        session.close()
        close = await peer.receive_until(APPLICATION_CLOSE)
        self.assertEqual(close.code, 0)
        with self.assertRaises(TrevRpcError) as caught:
            await session.open_stream(1000)
        self.assertEqual(caught.exception.code, Code.CANCELLED)


if __name__ == "__main__":
    unittest.main()
