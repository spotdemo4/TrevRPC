"""Unit tests for the interop runner. Run with `python3 -I -m unittest discover -s interop`."""

import contextlib
import io
import tempfile
import unittest
from pathlib import Path

import harness
import run

PORTS = {
    "native": 1001,
    "qmux": 1002,
    "webtransport": 1003,
    "websocket": 1002,
    "http2": 1002,
    "http3": 1004,
}


class FakeServer(harness.Server):
    """A Server with fixed ports and no process."""

    def __init__(  # pyright: ignore[reportMissingSuperCall]
        self,
        name: str,
        ports: dict[str, int] | None = None,
        scenarios: list[str] | None = None,
    ) -> None:
        self.name = name
        self.ports = PORTS if ports is None else ports
        self.scenarios = scenarios


class CellsTest(unittest.TestCase):
    certificate: harness.Certificate

    @classmethod
    def setUpClass(cls) -> None:
        directory = tempfile.TemporaryDirectory()
        cls.addClassCleanup(directory.cleanup)
        cls.certificate = harness.generate_certificate(Path(directory.name))

    def test_native_cells_cover_quic_qmux_and_the_fallback(self) -> None:
        client = harness.NamedCommand("go", ["trevrpc-interop-go"])

        cells = run.native_cells(client, FakeServer("rust"), self.certificate, Path("d.dict"))

        self.assertEqual(
            ["go-to-rust", "go-qmux-to-rust", "go-fallback-to-rust"], [c.name for c in cells]
        )
        # The QUIC and QMux lanes compress; the fallback lane does not.
        self.assertEqual(["--dictionary", "d.dict"], cells[0].command[-2:])
        self.assertEqual(["--dictionary", "d.dict"], cells[1].command[-2:])
        self.assertEqual(
            [
                "trevrpc-interop-go", "client", "--addr", "127.0.0.1:24999",
                "--tcp-addr", "127.0.0.1:1002", "--ca", str(self.certificate.cert),
                "--server-name", "localhost",
                "--transport", "auto", "--expect-transport", "qmux",
            ],
            cells[2].command,
        )  # fmt: skip

    def test_a_server_without_qmux_skips_those_lanes_and_lists_its_scenarios(self) -> None:
        client = harness.NamedCommand("go", ["trevrpc-interop-go"])
        server = FakeServer("erlang", {"native": 1001}, ["unary", "bidi"])

        cells = run.native_cells(client, server, self.certificate, Path("d.dict"))

        self.assertEqual(
            [None, "server erlang does not serve qmux", "server erlang does not serve qmux"],
            [cell.skip for cell in cells],
        )
        self.assertEqual(["--scenarios", "unary,bidi"], cells[0].command[-2:])
        self.assertEqual([], cells[1].command)

    def test_cells_name_their_runtimes(self) -> None:
        client = harness.NamedCommand("go", ["trevrpc-interop-go"])
        native = run.native_cells(client, FakeServer("rust"), self.certificate, Path("d.dict"))
        browser = run.browser_cells(
            "chromium", FakeServer("rust"), self.certificate, ["webtransport"], Path("d.dict")
        )
        self.assertEqual({("go", "rust")}, {cell.runtimes for cell in native})
        self.assertEqual([("chromium", "rust")], [cell.runtimes for cell in browser])

    def test_the_dictionary_comes_from_the_golden_vectors(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = run.write_dictionary(run.ZSTD_VECTORS, Path(directory))
            # A zstd-format dictionary starts with its magic number.
            self.assertEqual(bytes.fromhex("37a430ec"), path.read_bytes()[:4])

    def test_browser_cells_follow_the_selected_lanes_in_order(self) -> None:
        cells = run.browser_cells(
            "firefox",
            FakeServer("go"),
            self.certificate,
            ["app-websocket", "webtransport"],
            Path("d.dict"),
        )

        self.assertEqual(
            ["firefox-webtransport-to-go", "firefox-app-websocket-to-go"], [c.name for c in cells]
        )

    def test_some_browser_lanes_compress_and_the_others_do_not(self) -> None:
        cells = run.browser_cells(
            "chromium", FakeServer("go"), self.certificate, list(run.BROWSER_LANES), Path("d.dict")
        )

        compressed = {
            cell.name.removeprefix("chromium-").removesuffix("-to-go")
            for cell in cells
            if cell.command[-2:] == ["--dictionary", "d.dict"]
        }
        self.assertEqual({"webtransport", "websocket", "app-webtransport"}, compressed)
        self.assertEqual(2, sum("--dictionary" not in cell.command for cell in cells))

    def test_browser_lanes_need_their_ports_and_every_scenario(self) -> None:
        native_only = FakeServer("erlang", {"native": 1001})
        partial = FakeServer("erlang", PORTS, ["unary"])

        missing = run.browser_cells(
            "chromium", native_only, self.certificate, list(run.BROWSER_LANES), Path("d.dict")
        )
        selective = run.browser_cells(
            "chromium", partial, self.certificate, ["webtransport"], Path("d.dict")
        )

        self.assertEqual(
            [
                "server erlang does not serve webtransport",
                "server erlang does not serve webtransport, websocket",
                "server erlang does not serve websocket",
                "server erlang does not serve http3, webtransport",
                "server erlang does not serve http2, webtransport, websocket",
            ],
            [cell.skip for cell in missing],
        )
        self.assertEqual(
            ["server erlang supports only some scenarios"], [c.skip for c in selective]
        )

    def test_app_lanes_pin_the_page_protocol_where_the_browser_can_be_steered(self) -> None:
        pages: dict[str, list[str]] = {}
        for browser in ("chromium", "firefox", "safari"):
            [cell] = run.browser_cells(
                browser, FakeServer("go"), self.certificate, ["app-webtransport"], Path("d.dict")
            )
            start = cell.command.index("--page-url")
            pages[browser] = cell.command[start + 1 : start + 4]

        self.assertEqual(["https://127.0.0.1:1004/", "--page-protocol", "h3"], pages["chromium"])
        self.assertEqual(["https://127.0.0.1:1002/", "--page-protocol", "h2"], pages["firefox"])
        self.assertNotIn("--page-protocol", pages["safari"])


class RunCellTest(unittest.TestCase):
    def run_cell(self, cell: run.Cell) -> run.Failure | run.Skipped | run.Passed:
        with (
            tempfile.TemporaryDirectory() as directory,
            contextlib.redirect_stdout(io.StringIO()),
        ):
            server = FakeServer("go")
            server.log = Path(directory) / "server.log"
            _ = server.log.write_text("", encoding="utf-8")
            return run.run_cell(cell, server, Path(directory))

    def test_counts_the_scenarios_a_passing_client_skipped(self) -> None:
        script = "echo 'ok unary'; echo 'skip bidi: not in --scenarios'; echo 'skip validation: no'"
        outcome = self.run_cell(run.Cell("go-to-erlang", ["sh", "-c", script]))
        self.assertEqual(run.Passed("go-to-erlang", 2), outcome)

    def test_a_client_without_the_transport_skips_the_cell(self) -> None:
        script = "echo 'no --transport tcp yet' >&2; exit 77"
        outcome = self.run_cell(run.Cell("erlang-qmux-to-go", ["sh", "-c", script]))
        self.assertEqual(run.Skipped("erlang-qmux-to-go", "no --transport tcp yet"), outcome)

    def test_a_cell_without_its_transport_does_not_run(self) -> None:
        outcome = self.run_cell(run.Cell("go-qmux-to-erlang", [], skip="no qmux"))
        self.assertEqual(run.Skipped("go-qmux-to-erlang", "no qmux"), outcome)

    def test_skip_lines_do_not_explain_a_failure(self) -> None:
        script = "echo 'skip bidi: no'; echo 'fail unary: boom'; exit 1"
        outcome = self.run_cell(run.Cell("go-to-go", ["sh", "-c", script]))
        assert isinstance(outcome, run.Failure)
        self.assertEqual(["fail unary: boom"], outcome.details[:1])

    def test_only_runtimes_that_may_skip_do(self) -> None:
        go_to_erlang = run.Cell("go-to-erlang", [], ("go", "erlang"))
        go_to_rust = run.Cell("go-to-rust", [], ("go", "rust"))
        skipped = run.Skipped("x", "no qmux")
        scenarios = run.Passed("x", 3)

        self.assertIsNone(run.unexpected_skip(go_to_erlang, skipped, ["erlang"]))
        self.assertIsNone(run.unexpected_skip(go_to_erlang, scenarios, ["erlang"]))
        self.assertIsNone(run.unexpected_skip(go_to_rust, run.Passed("x", 0), ["erlang"]))
        self.assertEqual(
            run.Failure(
                "go-to-rust", ["skipped, but none of go, rust may skip (--may-skip): no qmux"]
            ),
            run.unexpected_skip(go_to_rust, skipped, ["erlang"]),
        )
        self.assertEqual(
            run.Failure(
                "go-to-rust", ["skipped 3 scenarios, but none of go, rust may skip (--may-skip)"]
            ),
            run.unexpected_skip(go_to_rust, scenarios, []),
        )

    def test_the_summary_lists_skipped_cells_and_scenarios(self) -> None:
        outcomes = run.Outcomes(
            [], [run.Skipped("go-qmux-to-erlang", "no qmux")], [run.Passed("go-to-erlang", 5)]
        )
        self.assertEqual(
            "skipped: 1 cells, 5 scenarios in passing cells\n"
            "    go-qmux-to-erlang: no qmux\n"
            "    go-to-erlang: 5 scenarios",
            outcomes.skip_summary(),
        )


def quiet() -> contextlib.redirect_stderr[io.StringIO]:
    return contextlib.redirect_stderr(io.StringIO())


class ArgumentsTest(unittest.TestCase):
    def test_requires_a_server_and_a_client_or_browser(self) -> None:
        for argv in [["--client", "go=a"], ["--server", "go=a"]]:
            with self.subTest(argv=argv), self.assertRaises(SystemExit), quiet():
                _ = run.parse_args(argv)

    def test_rejects_unknown_lanes(self) -> None:
        with self.assertRaises(SystemExit), quiet():
            _ = run.parse_args(["--server", "go=a", "--browser", "x", "--browser-lanes", "nope"])


if __name__ == "__main__":
    _ = unittest.main()
