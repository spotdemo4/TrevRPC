"""Unit tests for the benchmark runner. Run with `python3 -I -m unittest discover -s bench`."""

import contextlib
import io
import json
import shutil
import tempfile
import unittest
from pathlib import Path

import run

RESULT = {
    "scenario": "unary",
    "transport": "quic",
    "size": 64,
    "metadata_size": 0,
    "concurrency": 4,
    "duration_s": 0.2,
    "ops": 100,
    "msgs": 100,
    "errors": 0,
    "ops_per_sec": 1234.9,
    "msgs_per_sec": 1234.9,
    "bytes_per_sec": 12345678.9,
    "latency_us": {"p50": 10.7, "p90": 20, "p99": 30.2, "p999": 40},
}


class ParseResultTest(unittest.TestCase):
    def test_tags_the_record_with_client_and_server(self) -> None:
        result = run.parse_result("go", "rust", json.dumps(RESULT))

        self.assertEqual({"client": "go", "server": "rust", **RESULT}, result.record)
        self.assertIsNone(result.netem)
        self.assertEqual(0, result.lost)
        self.assertIsNone(result.wire_bytes_per_msg)

    def test_rejects_invalid_results(self) -> None:
        for line in [
            "",
            "[]",
            json.dumps({**RESULT, "size": "64"}),
            json.dumps({**RESULT, "latency_us": None}),
            json.dumps({key: value for key, value in RESULT.items() if key != "errors"}),
        ]:
            with self.subTest(line=line), self.assertRaises(ValueError):
                _ = run.parse_result("go", "rust", line)


class SummaryTest(unittest.TestCase):
    def test_formats_and_sorts_rows(self) -> None:
        results = [
            run.parse_result("go", "go", json.dumps({**RESULT, "scenario": "bidi"})),
            run.parse_result(
                "rust",
                "go",
                json.dumps({**RESULT, "lost": 1, "msgs": 2, "wire_bytes_per_msg": 80.9}),
            ),
        ]

        lines = run.summary(results).splitlines()

        self.assertTrue(lines[0].startswith("scenario" + " " * 18 + "size"))
        bidi, unary = (line.split() for line in lines[1:])
        self.assertEqual(["bidi", "64", "0", "4", "quic", "none", "go", "go"], bidi[:8])
        # ops/s, msgs/s, MB/s, p50us, p99us, wire_B/msg, lost%, spread%
        self.assertEqual(["1234", "1234", "12.34", "10", "30", "-", "0", "-"], bidi[8:])
        self.assertEqual(["unary", "rust", "go"], [unary[0], unary[6], unary[7]])
        self.assertEqual(["80", "33.33", "-"], unary[13:])

    def test_shows_the_compression_mode(self) -> None:
        result = run.parse_result(
            "go", "go", json.dumps({**RESULT, "payload": "records", "compression": "stream"})
        )

        self.assertEqual("stream", result.compression)
        self.assertEqual("stream", run.summary([result]).splitlines()[1].split()[5])

    def test_groups_rows_by_netem_profile_in_run_order(self) -> None:
        results = [
            run.parse_result("go", "go", json.dumps(RESULT), "delay 10ms loss 1%"),
            run.parse_result("go", "go", json.dumps(RESULT), "delay 10ms"),
            run.parse_result("rust", "go", json.dumps(RESULT), "delay 10ms loss 1%"),
        ]

        lines = run.summary(results).splitlines()

        self.assertEqual("netem delay 10ms loss 1%", lines[0])
        self.assertTrue(lines[1].startswith("scenario"))
        self.assertEqual(["go", "rust"], [line.split()[6] for line in lines[2:4]])
        self.assertEqual(["", "netem delay 10ms"], lines[4:6])
        self.assertEqual(8, len(lines))
        self.assertEqual("delay 10ms", results[1].record["netem"])

    def test_repeated_cells_report_the_median_run_and_the_spread(self) -> None:
        results = [
            run.parse_result("go", "go", json.dumps({**RESULT, "msgs_per_sec": rate, "msgs": rate}))
            for rate in (1100.0, 1000.0, 1300.0)
        ]

        median = run.median_run(results)

        self.assertEqual(1100.0, median.msgs_per_sec)
        self.assertEqual((1100.0, 1000.0, 1300.0), median.runs)
        self.assertEqual(3, median.record["repeats"])
        self.assertEqual([1100.0, 1000.0, 1300.0], median.record["msgs_per_sec_runs"])
        # (1300 - 1000) / 1100
        self.assertEqual("27.27", median.row()[-1])
        self.assertIs(results[0], run.median_run(results[:1]))
        self.assertEqual(1000.0, run.median_run(results[:2]).msgs_per_sec)

    def test_plain_drops_a_trailing_zero(self) -> None:
        self.assertEqual("5", run.plain(5.0))
        self.assertEqual("0.2", run.plain(0.2))
        self.assertEqual("12345678.9", run.plain(12345678.9))


class CompressionTest(unittest.TestCase):
    def test_modes_run_only_where_they_mean_something(self) -> None:
        self.assertTrue(run.runs("none", "quic", "server-stream"))
        self.assertTrue(run.runs("none", "quic", "unreliable-client-stream"))
        self.assertFalse(run.runs("none", "grpc", "unreliable-client-stream"))
        self.assertTrue(run.runs("zstd", "grpc", "bidi"))
        self.assertFalse(run.runs("zstd", "quic", "bidi"))
        self.assertFalse(run.runs("stream", "connect", "bidi"))
        self.assertTrue(run.runs("dictionary", "tcp", "unary"))
        self.assertFalse(run.runs("stream", "quic", "unary"))
        self.assertTrue(run.runs("stream-dictionary", "quic", "client-stream"))
        self.assertFalse(run.runs("gzip", "connect", "server-stream"))
        self.assertFalse(run.runs("dictionary", "quic", "unreliable-server-stream"))

    def test_compression_needs_the_records_payload(self) -> None:
        with self.assertRaises(SystemExit), contextlib.redirect_stderr(io.StringIO()):
            _ = run.parse_args(["--baseline", "b", "--compressions", "none,zstd"])
        with self.assertRaises(SystemExit), contextlib.redirect_stderr(io.StringIO()):
            _ = run.parse_args(["--baseline", "b", "--payload", "records", "--compressions", "lz4"])
        args = run.parse_args(["--baseline", "b", "--payload", "records", "--compressions", "zstd"])
        self.assertEqual(["zstd"], args.compressions)

    def test_the_dictionary_is_trained_the_same_way_every_time(self) -> None:
        if shutil.which("zstd") is None:
            self.skipTest("zstd is not installed")
        with tempfile.TemporaryDirectory() as first, tempfile.TemporaryDirectory() as second:
            dictionaries = [
                run.train_dictionary(Path(directory)).read_bytes() for directory in (first, second)
            ]
        self.assertEqual(dictionaries[0], dictionaries[1])
        # A zstd-format dictionary with ID 2.
        self.assertEqual(bytes.fromhex("37a430ec02000000"), dictionaries[0][:8])


class MatrixTest(unittest.TestCase):
    def test_deadline_allows_for_warmup_and_draining(self) -> None:
        matrix = run.Matrix(["unary"], [64], [0], [1], 0.2, 0, 10)

        self.assertEqual(60, matrix.deadline)


if __name__ == "__main__":
    _ = unittest.main()
