"""The zstd codecs against testdata/zstd-golden-vectors.txt, and round trips through the
encoders."""

from __future__ import annotations

import os
import unittest
from pathlib import Path
from typing import Literal

from trevrpc._frames import FrameType, encode_frame
from trevrpc._wire import DictionaryInfo, RpcAnswer, RpcRequest, RpcStatus, StreamCompression
from trevrpc._zstd import (
    STREAM_FRAME_LIMIT,
    CompressionError,
    DictionaryLookup,
    LoadedDictionary,
    MessageCompressor,
    StreamCompressor,
    StreamDecompressor,
    decompress_message,
    load_dictionary,
)

MAX = 4 * 1024 * 1024


def _golden() -> dict[str, bytes | int]:
    path = os.environ.get("TREVRPC_ZSTD_GOLDEN") or str(
        Path(__file__).resolve().parents[2] / "testdata" / "zstd-golden-vectors.txt"
    )
    vectors: dict[str, bytes | int] = {}
    for line in Path(path).read_text().splitlines():
        if line and not line.startswith("#"):
            name, _, value = line.partition("=")
            name, value = name.strip(), value.strip()
            vectors[name] = int(value, 16) if name.endswith(".id") else bytes.fromhex(value)
    return vectors


GOLDEN = _golden()


def b(name: str) -> bytes:
    value = GOLDEN[name]
    assert isinstance(value, bytes)
    return value


def names(prefix: str, suffix: str) -> list[str]:
    return sorted(
        {
            n[len(prefix) : -len(suffix)]
            for n in GOLDEN
            if n.startswith(prefix) and n.endswith(suffix)
        }
    )


D1 = load_dictionary(b("dict.d1.dictionary"))


def accepting(
    *, plain: bool, dictionaries: tuple[LoadedDictionary, ...] = (D1,)
) -> DictionaryLookup:
    def lookup(dictionary_id: int) -> LoadedDictionary | Literal[False] | None:
        if dictionary_id == 0:
            return None if plain else False
        for dictionary in dictionaries:
            if dictionary.id == dictionary_id:
                return dictionary
        return False

    return lookup


class DictionaryTest(unittest.TestCase):
    def test_id_and_fingerprint(self) -> None:
        self.assertEqual(D1.id, GOLDEN["dict.d1.id"])
        self.assertEqual(D1.fingerprint, b("dict.d1.fingerprint"))

    def test_raw_content_is_refused(self) -> None:
        with self.assertRaises(ValueError):
            load_dictionary(b"raw content that has no dictionary header" * 8)


class MessageTest(unittest.TestCase):
    def test_accepted_payloads(self) -> None:
        cases = names("ok.", ".payload")
        self.assertIn("small", cases)
        for name in cases:
            message = decompress_message(b(f"ok.{name}.payload"), accepting(plain=False), MAX)
            self.assertEqual(message, b(f"ok.{name}.message"), name)

    def test_rejected_payloads(self) -> None:
        cases = names("bad.", ".payload")
        self.assertIn("no_dictionary_id", cases)
        for name in cases:
            # bad.no_dictionary_id must fail even where ID 0 is accepted.
            with self.assertRaises(CompressionError, msg=name) as caught:
                decompress_message(b(f"bad.{name}.payload"), accepting(plain=True), MAX)
            self.assertEqual(caught.exception.kind, "invalid", name)

    def test_plain_payloads(self) -> None:
        for name in names("plain.", ".payload"):
            payload = b(f"plain.{name}.payload")
            message = decompress_message(payload, accepting(plain=True), MAX)
            self.assertEqual(message, b(f"plain.{name}.message"), name)
            with self.assertRaises(CompressionError, msg=name):
                decompress_message(payload, accepting(plain=False), MAX)

    def test_too_large_payloads_are_refused_before_decompressing(self) -> None:
        for name in names("too_large.", ".payload"):
            with self.assertRaises(CompressionError, msg=name) as caught:
                decompress_message(b(f"too_large.{name}.payload"), accepting(plain=True), MAX)
            self.assertEqual(caught.exception.kind, "too_large", name)

    def test_compressor_round_trips_and_keeps_only_smaller_payloads(self) -> None:
        compressor = MessageCompressor(D1, 1)
        message = b("ok.small.message")
        for _ in range(2):
            payload = compressor.compress(message)
            assert payload is not None
            self.assertEqual(decompress_message(payload, accepting(plain=False), MAX), message)
        self.assertIsNone(MessageCompressor(None, 1).compress(b("ok.raw_block.message")))

    def test_frames(self) -> None:
        self.assertEqual(
            encode_frame(FrameType.ZSTD_MESSAGE, b("zstd_message.small.payload")),
            b("zstd_message.small.frame"),
        )


class StreamTest(unittest.TestCase):
    def test_accepted_streams(self) -> None:
        streams = {name.split(".")[0] for name in names("stream.", ".payload")}
        self.assertEqual(streams, {"plain", "seeded", "reset", "empty"})
        for stream in sorted(streams):
            decompressor = StreamDecompressor(accepting(plain=True))
            i = 0
            while f"stream.{stream}.{i}.payload" in GOLDEN:
                message = decompressor.decompress(b(f"stream.{stream}.{i}.payload"), MAX)
                self.assertEqual(message, b(f"stream.{stream}.{i}.message"), f"{stream}.{i}")
                i += 1

    def test_rejected_streams(self) -> None:
        cases = names("stream_bad.", ".payload")
        self.assertIn("missing_frame_header", cases)
        for name in cases:
            decompressor = StreamDecompressor(accepting(plain=True))
            with self.assertRaises(CompressionError, msg=name) as caught:
                decompressor.decompress(b(f"stream_bad.{name}.payload"), MAX)
            self.assertEqual(caught.exception.kind, "invalid", name)

    def test_too_large_streams(self) -> None:
        for name in names("stream_too_large.", ".payload"):
            decompressor = StreamDecompressor(accepting(plain=True))
            with self.assertRaises(CompressionError, msg=name) as caught:
                decompressor.decompress(b(f"stream_too_large.{name}.payload"), MAX)
            self.assertEqual(caught.exception.kind, "too_large", name)

    def test_compressor_writes_the_vectors(self) -> None:
        for stream, dictionary in (("plain", None), ("seeded", D1)):
            compressor = StreamCompressor(dictionary, 1)
            i = 0
            while f"stream.{stream}.{i}.payload" in GOLDEN:
                payload = compressor.compress(b(f"stream.{stream}.{i}.message"))
                self.assertEqual(payload, b(f"stream.{stream}.{i}.payload"), f"{stream}.{i}")
                i += 1

    def test_frames_end_at_the_limit_and_restart_from_the_dictionary(self) -> None:
        compressor = StreamCompressor(D1, 1)
        decompressor = StreamDecompressor(accepting(plain=False))
        message = b("ok.small.message")
        count = 2 * STREAM_FRAME_LIMIT // len(message) + 3
        for i in range(count):
            payload = compressor.compress(message + str(i).encode())
            self.assertEqual(decompressor.decompress(payload, MAX), message + str(i).encode())
        # The decompressor accepts no frame without dictionary 1, so every frame started from it.
        self.assertEqual(compressor.compress(b""), b"\x00")

    def test_frames(self) -> None:
        self.assertEqual(
            encode_frame(FrameType.ZSTD_STREAM_MESSAGE, b("zstd_stream_message.first.payload")),
            b("zstd_stream_message.first.frame"),
        )


class NegotiationTest(unittest.TestCase):
    def test_requests(self) -> None:
        cases = {
            "dictionaries": RpcRequest(
                "/svc/m",
                accept_dictionaries=(1, 300),
                dictionary_query=(DictionaryInfo(1, b("dict.d1.fingerprint")),),
            ),
            "plain": RpcRequest(
                "/svc/m", accept_dictionaries=(0,), dictionary_query=(DictionaryInfo(0),)
            ),
            "stream_compression": RpcRequest(
                "/svc/m", stream_compression=StreamCompression.ENABLED
            ),
            "optimistic_stream_compression": RpcRequest(
                "/svc/m", stream_compression=StreamCompression.OPTIMISTIC
            ),
        }
        for name, request in cases.items():
            self.assertEqual(request.encode().hex(), b(f"request.{name}.payload").hex(), name)
            self.assertEqual(
                encode_frame(FrameType.REQUEST, request.encode()), b(f"request.{name}.frame"), name
            )

    def test_answers(self) -> None:
        self.assertEqual(
            RpcAnswer.decode(b("answer.dictionaries.payload")).dictionaries,
            (DictionaryInfo(1, b("dict.d1.fingerprint")),),
        )
        self.assertEqual(
            RpcAnswer.decode(b("answer.plain.payload")).dictionaries, (DictionaryInfo(0),)
        )
        self.assertTrue(RpcAnswer.decode(b("answer.stream_compression.payload")).stream_compression)

    def test_refused_status(self) -> None:
        status = RpcStatus.decode(b("status.refused_stream_compression.payload"))
        self.assertEqual(status.code, 8)
        self.assertTrue(status.refused_stream_compression)


if __name__ == "__main__":
    unittest.main()
