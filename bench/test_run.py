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
        self.assertTrue(run.runs("stream", "quic", "unary"))
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


class TableTest(unittest.TestCase):
    # Which client runs against which server over which transport: the packaged runtimes against
    # each other, and the baseline's clients against the baseline.
    PAIRS = [
        (client, server, transport)
        for client in ("go", "rust")
        for server in ("go", "rust")
        for transport in ("quic", "tcp")
    ] + [("grpc", "grpc", "grpc"), ("connect", "connect", "connect")]

    def cells(self, table: str) -> dict[tuple[str, str], int]:
        args = run.parse_args(
            ["--table", table, "--server", "go=go", "--server", "rust=rust",
             "--client", "go=go", "--client", "rust=rust", "--baseline", "baseline"]
        )  # fmt: skip
        matrix = run.build_matrix(args)
        counts: dict[tuple[str, str], int] = {}
        for client, server, transport in self.PAIRS:
            for profile in args.netem or [None]:
                count = len(matrix.cells(client, server, transport, profile))
                if count:
                    self.assertEqual(client, server, (table, client, server))
                    key = (client, transport)
                    counts[key] = counts.get(key, 0) + count
        return counts

    def test_tables_run_only_the_cells_their_wiki_table_reads(self) -> None:
        self.assertEqual(
            {("go", "quic"): 12, ("go", "tcp"): 12, ("rust", "quic"): 12, ("rust", "tcp"): 12,
             ("grpc", "grpc"): 12, ("connect", "connect"): 12},
            self.cells("comparison"),
        )  # fmt: skip
        self.assertEqual(
            {("go", "quic"): 12, ("go", "tcp"): 4, ("rust", "quic"): 12, ("rust", "tcp"): 8,
             ("grpc", "grpc"): 15, ("connect", "connect"): 9},
            self.cells("compression"),
        )  # fmt: skip
        self.assertEqual(
            {("rust", "quic"): 12, ("grpc", "grpc"): 9, ("connect", "connect"): 3},
            self.cells("link"),
        )
        self.assertEqual(16, self.cells("lossy")[("go", "quic")])
        self.assertEqual({("go", "quic"): 6, ("rust", "quic"): 6}, self.cells("datagrams"))

    def test_explicit_flags_override_the_table(self) -> None:
        args = run.parse_args(
            ["--table", "lossy", "--netem", "delay 1ms", "--sizes", "64", "--baseline", "b"]
        )

        self.assertEqual(["delay 1ms"], args.netem)
        self.assertEqual([64], args.sizes)
        self.assertEqual(10.0, args.duration)


class RepeatTest(unittest.TestCase):
    def results(self, *rates: float) -> list[run.Result]:
        return [
            run.parse_result("go", "go", json.dumps({**RESULT, "msgs_per_sec": rate}))
            for rate in rates
        ]

    def test_spread_is_the_range_as_a_share_of_the_median(self) -> None:
        self.assertEqual(2.0, run.spread(self.results(1000, 1020)))
        self.assertAlmostEqual(27.27, run.spread(self.results(1000, 1300, 1100)), places=2)

    def test_two_runs_agree_when_they_differ_by_at_most_the_tolerance(self) -> None:
        self.assertTrue(run.agree(self.results(1000, 1030), 3))
        self.assertFalse(run.agree(self.results(1000, 1031), 3))
        self.assertTrue(run.agree(self.results(1000, 1300, 1290), 3))
        self.assertFalse(run.agree(self.results(1000), 3))

    def test_cells_stop_repeating_once_two_runs_agree(self) -> None:
        rates = iter([1000.0, 1300.0, 1290.0, 999.0])
        runner = run.Runner(
            run.Matrix(["unary"], [64], [0], [1], 0.2, 0, 10, repeat=5, tolerance=3), Path("/")
        )
        calls: list[str] = []

        def run_once(name: str, *_: object) -> run.Result:
            calls.append(name)
            return self.results(next(rates))[0]

        runner.run_once = run_once  # pyright: ignore[reportAttributeAccessIssue]
        with tempfile.TemporaryDirectory() as out:
            runner.out = Path(out)
            runner.run_one("cell", "go", "go", None, [])

        # 1000 and 1300 differ by 30%; 1300 and 1290 agree within 3% of the median, 1290.
        self.assertEqual(["cell-r1", "cell-r2", "cell-r3"], calls)
        self.assertEqual(1290.0, runner.results[0].msgs_per_sec)


if __name__ == "__main__":
    _ = unittest.main()
