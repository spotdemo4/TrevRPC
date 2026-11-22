"""Frames and control messages against testdata/wire-golden-vectors.txt."""

from __future__ import annotations

import os
import unittest
from pathlib import Path

from trevrpc._frames import FrameReader, encode_frame
from trevrpc._metadata import normalize_metadata, validate_metadata
from trevrpc._status import Code, StatusDetail, TrevRpcError
from trevrpc._varint import MAX_VARINT, decode_varint, encode_varint, varint_len
from trevrpc._wire import (
    ConnectionMetadata,
    RpcAnswer,
    RpcHeaders,
    RpcKind,
    RpcRequest,
    RpcStatus,
    WireError,
    read_fields,
)


def _golden() -> dict[str, bytes]:
    path = os.environ.get("TREVRPC_WIRE_GOLDEN") or str(
        Path(__file__).resolve().parents[2] / "testdata" / "wire-golden-vectors.txt"
    )
    vectors: dict[str, bytes] = {}
    for line in Path(path).read_text().splitlines():
        if not line or line.startswith("#"):
            continue
        name, _, value = line.partition("=")
        value = value.strip()
        name = name.strip()
        if name.endswith((".frames", ".messages", ".flow")):
            vectors[name] = value.encode()
        else:
            vectors[name] = bytes.fromhex(value)
    return vectors


GOLDEN = _golden()


class VarintTest(unittest.TestCase):
    def test_round_trips_every_length(self) -> None:
        for value in (0, 63, 64, 16383, 16384, (1 << 30) - 1, 1 << 30, MAX_VARINT):
            data = encode_varint(value)
            self.assertEqual(len(data), varint_len(value))
            self.assertEqual(decode_varint(data, 0), (value, len(data)))

    def test_accepts_non_minimal_encodings(self) -> None:
        self.assertEqual(decode_varint(bytes.fromhex("4001"), 0), (1, 2))
        self.assertEqual(decode_varint(bytes.fromhex("c000000000000001"), 0), (1, 8))

    def test_reports_truncation(self) -> None:
        self.assertIsNone(decode_varint(b"", 0))
        self.assertIsNone(decode_varint(bytes.fromhex("80"), 0))

    def test_rejects_out_of_range(self) -> None:
        with self.assertRaises(ValueError):
            encode_varint(MAX_VARINT + 1)
        with self.assertRaises(ValueError):
            encode_varint(-1)


class GoldenEncodingTest(unittest.TestCase):
    def check(self, name: str, frame_type: int, payload: bytes) -> None:
        self.assertEqual(payload.hex(), GOLDEN[f"{name}.payload"].hex(), name)
        self.assertEqual(
            encode_frame(frame_type, payload).hex(), GOLDEN[f"{name}.frame"].hex(), name
        )

    def test_requests(self) -> None:
        self.check("request.unary", 1, RpcRequest("/svc/m").encode())
        self.check(
            "request.bidi_timeout",
            1,
            RpcRequest("/svc/m", kind=RpcKind.BIDI_STREAMING, timeout_nanos=123456).encode(),
        )
        self.check("request.metadata", 1, RpcRequest("/svc/m", {"authorization": b"ok"}).encode())
        self.check(
            "request.datagram_flow",
            1,
            RpcRequest("/svc/m", kind=RpcKind.BIDI_STREAMING, datagram_flow=1).encode(),
        )
        self.check(
            "request.connection_metadata_offer",
            1,
            RpcRequest(
                "/svc/m", connection_metadata=ConnectionMetadata(1, {"authorization": b"ok"})
            ).encode(),
        )
        self.check(
            "request.connection_metadata_reference",
            1,
            RpcRequest("/svc/m", {"x-a": b"1"}, connection_metadata=ConnectionMetadata(1)).encode(),
        )

    def test_messages(self) -> None:
        self.check("message.hi", 0, b"hi")
        self.check("message.empty", 0, b"")

    def test_grease(self) -> None:
        self.check("grease.two_byte_type", 0x40, bytes.fromhex("000102"))


class GoldenDecodingTest(unittest.TestCase):
    def test_headers(self) -> None:
        self.assertEqual(
            RpcHeaders.decode(GOLDEN["headers.metadata.payload"]).metadata, {"x-a": b"1"}
        )
        self.assertEqual(RpcHeaders.decode(GOLDEN["headers.empty.payload"]).metadata, {})

    def test_answers(self) -> None:
        self.assertEqual(
            RpcAnswer.decode(GOLDEN["answer.connection_metadata.payload"]).connection_metadata, 1
        )
        empty = RpcAnswer.decode(GOLDEN["answer.empty.payload"])
        self.assertEqual((empty.dictionaries, empty.stream_compression), ((), False))

    def test_statuses(self) -> None:
        ok = RpcStatus.decode(GOLDEN["status.ok.payload"])
        self.assertEqual((ok.code, ok.message, ok.metadata), (0, "", {}))
        unavailable = RpcStatus.decode(GOLDEN["status.unavailable.payload"])
        self.assertEqual((unavailable.code, unavailable.message), (14, "down"))
        metadata = RpcStatus.decode(GOLDEN["status.metadata.payload"])
        self.assertEqual(
            (metadata.code, metadata.message, metadata.metadata), (9, "stop", {"x-t": b"2"})
        )
        details = RpcStatus.decode(GOLDEN["status.details.payload"])
        self.assertEqual((details.code, details.message), (3, "bad"))
        self.assertEqual(details.details, (StatusDetail("x.Y", bytes.fromhex("0801")),))

    def test_streams_yield_their_known_frames_at_every_split(self) -> None:
        names = sorted(n.removesuffix(".stream") for n in GOLDEN if n.endswith(".stream"))
        self.assertTrue(names)
        for name in names:
            stream = GOLDEN[f"{name}.stream"]
            expected = [
                (int(t), bytes.fromhex(p))
                for t, _, p in (
                    f.partition(":") for f in GOLDEN[f"{name}.frames"].decode().split(",")
                )
            ]
            for split in range(len(stream) + 1):
                reader = FrameReader()
                frames: list[tuple[int, bytes]] = []
                for piece in (stream[:split], stream[split:]):
                    reader.feed(piece)
                    while (frame := reader.next()) is not None:
                        frames.append(frame)
                self.assertEqual(frames, expected, f"{name} split at {split}")
                self.assertEqual(reader.consumed, len(stream), name)

    def test_streams_byte_by_byte(self) -> None:
        stream = GOLDEN["stream.unknown_frames.stream"]
        reader = FrameReader()
        frames: list[tuple[int, bytes]] = []
        for byte in stream:
            reader.feed(bytes((byte,)))
            while (frame := reader.next()) is not None:
                frames.append(frame)
        self.assertEqual([t for t, _ in frames], [2, 0, 3])


class FrameLimitTest(unittest.TestCase):
    def test_rejects_an_oversized_frame_from_its_header(self) -> None:
        reader = FrameReader(max_frame_size=4)
        reader.feed(bytes.fromhex("0005"))
        with self.assertRaises(TrevRpcError) as caught:
            reader.next()
        self.assertEqual(caught.exception.code, Code.RESOURCE_EXHAUSTED)

    def test_counts_unknown_frames_against_the_limit(self) -> None:
        reader = FrameReader(max_frame_size=4)
        reader.feed(bytes.fromhex("2105"))
        with self.assertRaises(TrevRpcError):
            reader.next()


class ControlMessageTest(unittest.TestCase):
    def test_skips_unknown_fields_of_every_wire_type(self) -> None:
        # field 9 varint, field 10 fixed64, field 11 bytes, field 12 fixed32, field 13 group,
        # then code 5.
        data = bytes.fromhex(
            "4801" + "51" + "00" * 8 + "5a0161" + "65" + "00" * 4 + "6b0801" + "6c"
        ) + bytes.fromhex("0805")
        self.assertEqual(RpcStatus.decode(data).code, 5)

    def test_rejects_truncated_messages(self) -> None:
        with self.assertRaises(WireError):
            RpcStatus.decode(bytes.fromhex("1205"))
        with self.assertRaises(WireError):
            list(read_fields(bytes.fromhex("08")))

    def test_rejects_invalid_utf8_strings(self) -> None:
        with self.assertRaises(WireError):
            RpcStatus.decode(bytes.fromhex("1201ff"))

    def test_last_map_entry_wins(self) -> None:
        data = bytes.fromhex("0a080a03782d611201310a080a03782d61120132")
        self.assertEqual(RpcHeaders.decode(data).metadata, {"x-a": b"2"})


class MetadataTest(unittest.TestCase):
    def test_normalizes_keys_and_values(self) -> None:
        self.assertEqual(normalize_metadata({"X-A": "1", "b": b"2"}), {"x-a": b"1", "b": b"2"})

    def test_rejects_invalid_metadata(self) -> None:
        for metadata in (
            {"": "x"},
            {"a b": "x"},
            {"trevrpc-x": "x"},
            {"k" * 129: "x"},
            {"k": "v" * 8193},
            {f"k{i}": "v" for i in range(65)},
            {f"k{i}": "v" * 8000 for i in range(9)},
        ):
            with self.assertRaises(TrevRpcError) as caught:
                normalize_metadata(metadata)
            self.assertEqual(caught.exception.code, Code.INVALID_ARGUMENT)

    def test_validates_received_metadata_with_the_given_code(self) -> None:
        with self.assertRaises(TrevRpcError) as caught:
            validate_metadata({"Bad": b""}, Code.INTERNAL)
        self.assertEqual(caught.exception.code, Code.INTERNAL)


class RetryDelayTest(unittest.TestCase):
    @staticmethod
    def retry_info(duration: bytes) -> StatusDetail:
        return StatusDetail("google.rpc.RetryInfo", b"\x0a" + bytes((len(duration),)) + duration)

    def test_reads_the_delay(self) -> None:
        # Duration{seconds: 1, nanos: 500000000}
        error = TrevRpcError(
            8, "slow down", details=[self.retry_info(bytes.fromhex("08011080cab5ee01"))]
        )
        self.assertEqual(error.retry_delay, 1.5)

    def test_ignores_negative_and_malformed_delays(self) -> None:
        negative = self.retry_info(bytes.fromhex("08ffffffffffffffffff01"))
        malformed = StatusDetail("google.rpc.RetryInfo", bytes.fromhex("0a05"))
        for detail in (negative, malformed):
            self.assertIsNone(TrevRpcError(8, details=[detail]).retry_delay)
        self.assertIsNone(TrevRpcError(8).retry_delay)

    def test_maps_unknown_codes_to_unknown(self) -> None:
        self.assertEqual(TrevRpcError(99).code, Code.UNKNOWN)


if __name__ == "__main__":
    unittest.main()
