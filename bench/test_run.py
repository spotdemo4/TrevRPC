"""Unit tests for the benchmark runner. Run with `python3 -I -m unittest discover -s bench`."""

import json
import unittest

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
        self.assertEqual(["bidi", "64", "0", "4", "quic", "go", "go"], bidi[:7])
        # ops/s, msgs/s, MB/s, p50us, p99us, wire_B/msg, lost%
        self.assertEqual(["1234", "1234", "12.34", "10", "30", "-", "0"], bidi[7:])
        self.assertEqual(["unary", "rust", "go"], [unary[0], unary[5], unary[6]])
        self.assertEqual(["80", "33.33"], unary[12:])

    def test_plain_drops_a_trailing_zero(self) -> None:
        self.assertEqual("5", run.plain(5.0))
        self.assertEqual("0.2", run.plain(0.2))
        self.assertEqual("12345678.9", run.plain(12345678.9))


class MatrixTest(unittest.TestCase):
    def test_deadline_allows_for_warmup_and_draining(self) -> None:
        matrix = run.Matrix(["unary"], [64], [0], [1], 0.2, 0, 10)

        self.assertEqual(60, matrix.deadline)


if __name__ == "__main__":
    _ = unittest.main()
