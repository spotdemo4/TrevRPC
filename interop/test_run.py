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

    def __init__(self, name: str) -> None:  # pyright: ignore[reportMissingSuperCall]
        self.name = name
        self.ports = PORTS


class CellsTest(unittest.TestCase):
    certificate: harness.Certificate

    @classmethod
    def setUpClass(cls) -> None:
        directory = tempfile.TemporaryDirectory()
        cls.addClassCleanup(directory.cleanup)
        cls.certificate = harness.generate_certificate(Path(directory.name))

    def test_native_cells_cover_quic_qmux_and_the_fallback(self) -> None:
        client = harness.NamedCommand("go", ["trevrpc-interop-go"])

        cells = run.native_cells(client, FakeServer("rust"), self.certificate)

        self.assertEqual(
            ["go-to-rust", "go-qmux-to-rust", "go-fallback-to-rust"], [c.name for c in cells]
        )
        self.assertEqual(
            [
                "trevrpc-interop-go", "client", "--addr", "127.0.0.1:24999",
                "--tcp-addr", "127.0.0.1:1002", "--ca", str(self.certificate.cert),
                "--server-name", "localhost",
                "--transport", "auto", "--expect-transport", "qmux",
            ],
            cells[2].command,
        )  # fmt: skip

    def test_browser_cells_follow_the_selected_lanes_in_order(self) -> None:
        cells = run.browser_cells(
            "firefox", FakeServer("go"), self.certificate, ["app-websocket", "webtransport"]
        )

        self.assertEqual(
            ["firefox-webtransport-to-go", "firefox-app-websocket-to-go"], [c.name for c in cells]
        )

    def test_app_lanes_pin_the_page_protocol_where_the_browser_can_be_steered(self) -> None:
        pages: dict[str, list[str]] = {}
        for browser in ("chromium", "firefox", "safari"):
            [cell] = run.browser_cells(
                browser, FakeServer("go"), self.certificate, ["app-webtransport"]
            )
            start = cell.command.index("--page-url")
            pages[browser] = cell.command[start + 1 : start + 4]

        self.assertEqual(["https://127.0.0.1:1004/", "--page-protocol", "h3"], pages["chromium"])
        self.assertEqual(["https://127.0.0.1:1002/", "--page-protocol", "h2"], pages["firefox"])
        self.assertNotIn("--page-protocol", pages["safari"])


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
