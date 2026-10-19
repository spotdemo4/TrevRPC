#!/usr/bin/env python3
"""Runs every interop client against every interop server over every transport; see README.md.

    run.py [--out DIR] --server NAME=CMD... [--client NAME=CMD...] [--browser NAME...]
           [--browser-lanes webtransport,websocket,fallback,app-webtransport,app-websocket]

Servers and native clients are interop executables taking the `server` and `client` subcommands.
Browsers run through `trevrpc-interop-browser`, by default over every lane: WebTransport, QMux over
WebSocket, the fallback from a blocked UDP port to WebSocket, and the app lanes, where the server
itself serves the page through its HTTP fallback, as a web app's server would, and the page calls
it over WebTransport or WebSocket. The app-webtransport lane loads the page over HTTP/3 where the
browser can be made to (Chromium), and app-websocket over HTTP/2, as on a network that blocks UDP.
Safari cannot ignore certificate errors, so with `--browser safari` the generated certificate is
trusted in the macOS System keychain for the duration of the run, through non-interactive sudo.
"""

from __future__ import annotations

import argparse
import hashlib
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path

# The shared harness; appended so it never shadows modules next to this script.
sys.path.append(str(Path(__file__).resolve().parents[1] / "interop"))

from harness import (
    Certificate,
    NamedCommand,
    RunnerError,
    Server,
    comma_list,
    generate_certificate,
    named_command,
    parse_with_defaults,
    run_checked,
    run_with_deadline,
)

BROWSER_LANES = ("webtransport", "websocket", "fallback", "app-webtransport", "app-websocket")
BROWSER = "trevrpc-interop-browser"
CELL_TIMEOUT = 120
# A UDP port that nothing answers, for cells that exercise the TCP fallback. It is below the
# ephemeral range, so it never collides with a server's own port, and is not on the browsers'
# list of unsafe ports.
UNREACHABLE_UDP = 24999
SYSTEM_KEYCHAIN = "/Library/Keychains/System.keychain"
KEYCHAIN_TIMEOUT = 30


@dataclass(frozen=True)
class Cell:
    name: str
    command: list[str]


def native_cells(client: NamedCommand, server: Server, certificate: Certificate) -> list[Cell]:
    """Every scenario over QUIC, over QMux over TLS, and over QUIC with a blocked UDP port, where
    the client must fall back to TCP."""
    native = server.port("native")
    qmux = server.port("qmux")
    tls = ["--ca", str(certificate.cert), "--server-name", "localhost"]
    lanes = [
        ("", native, None, "quic", "quic"),
        ("-qmux", native, qmux, "tcp", "qmux"),
        ("-fallback", UNREACHABLE_UDP, qmux, "auto", "qmux"),
    ]
    cells: list[Cell] = []
    for suffix, udp, tcp, transport, expected in lanes:
        command = [*client.command, "client", "--addr", f"127.0.0.1:{udp}"]
        if tcp is not None:
            command += ["--tcp-addr", f"127.0.0.1:{tcp}"]
        command += [*tls, "--transport", transport, "--expect-transport", expected]
        cells.append(Cell(f"{client.name}{suffix}-to-{server.name}", command))
    return cells


def browser_cells(
    browser: str, server: Server, certificate: Certificate, lanes: list[str]
) -> list[Cell]:
    """Every scenario over the selected lanes: WebTransport, QMux over WebSocket, the fallback
    from a blocked UDP port to WebSocket, and the app lanes, where the server serves the page."""
    cert_hash = hashlib.sha256(certificate.der()).hexdigest()
    webtransport = ["--url", f"https://127.0.0.1:{server.port('webtransport')}/trevrpc"]
    websocket = ["--ws-url", f"wss://127.0.0.1:{server.port('websocket')}/trevrpc"]
    pin = ["--cert-hash", cert_hash]
    over_websocket = ["--transport", "websocket", "--expect-transport", "websocket"]
    page_tls = ["--cert", str(certificate.cert), "--key", str(certificate.key)]
    # Over HTTP/2 the page comes from the server's TCP listener, over HTTP/3 from its UDP socket.
    # Their ports differ for the Rust server.
    page_h2 = ["--page-url", f"https://127.0.0.1:{server.port('http2')}/"]
    page_h3 = ["--page-url", f"https://127.0.0.1:{server.port('http3')}/"]
    # The app lanes assert the protocol the page arrived over, except in Safari, which cannot be
    # steered either way. Firefox does not use HTTP/3 with an overridden certificate, so its
    # app-webtransport lane loads over HTTP/2.
    match browser:
        case "chromium":
            app_webtransport = [*page_h3, "--page-protocol", "h3"]
        case "firefox":
            app_webtransport = [*page_h2, "--page-protocol", "h2"]
        case _:
            app_webtransport = page_h2
    app_websocket = page_h2 if browser == "safari" else [*page_h2, "--page-protocol", "h2"]
    commands = {
        "webtransport": [*webtransport, *pin],
        "websocket": [*webtransport, *websocket, *pin, *over_websocket],
        "fallback": [
            "--url", f"https://127.0.0.1:{UNREACHABLE_UDP}/trevrpc", *websocket, *pin,
            "--transport", "auto", "--expect-transport", "websocket",
        ],
        "app-webtransport": [
            *app_webtransport, *page_tls, *webtransport, *pin,
            "--expect-transport", "webtransport",
        ],
        "app-websocket": [
            *app_websocket, *page_tls, *webtransport, *websocket, *pin, *over_websocket,
        ],
    }  # fmt: skip
    return [
        Cell(f"{browser}-{lane}-to-{server.name}", [BROWSER, "--browser", browser, *arguments])
        for lane, arguments in commands.items()
        if lane in lanes
    ]


def run_cell(cell: Cell, out: Path) -> bool:
    """Runs one cell, logging to OUT/CELL.log, and prints the log."""
    print(f"== {cell.name}", flush=True)
    log = out / f"{cell.name}.log"
    with log.open("w", encoding="utf-8") as output:
        result = run_with_deadline(cell.command, CELL_TIMEOUT, stdout=output, stderr=output)
        if result.returncode is None:
            print(f"timed out after {CELL_TIMEOUT}s", file=output)
    _ = sys.stdout.write(log.read_text(encoding="utf-8", errors="replace"))
    _ = sys.stdout.flush()
    return result.returncode == 0


class Keychain:
    """Trusts a certificate in the macOS System keychain until removed. Changes run without
    prompting and with a deadline, so a missing sudo password or an authorization dialog fails
    the run instead of hanging it."""

    certificate: Certificate
    fingerprint: str | None = None

    def __init__(self, certificate: Certificate) -> None:
        self.certificate = certificate

    @staticmethod
    def _security(*arguments: str) -> bool:
        command = ["sudo", "-n", "security", *arguments]
        result = run_with_deadline(command, KEYCHAIN_TIMEOUT, stdout=sys.stderr, stderr=sys.stderr)
        return result.returncode == 0

    def trust(self) -> None:
        cert = str(self.certificate.cert)
        if not self._security(
            "add-trusted-cert", "-d", "-r", "trustRoot", "-p", "ssl", "-k", SYSTEM_KEYCHAIN, cert
        ):
            raise RunnerError(f"could not trust {cert} in {SYSTEM_KEYCHAIN}")
        self.fingerprint = hashlib.sha1(self.certificate.der()).hexdigest().upper()

    def untrust(self) -> None:
        if self.fingerprint is None:
            return
        if not self._security("remove-trusted-cert", "-d", str(self.certificate.cert)):
            print("could not remove the certificate's trust settings", file=sys.stderr)
        if not self._security("delete-certificate", "-Z", self.fingerprint, SYSTEM_KEYCHAIN):
            print(
                f"could not delete certificate {self.fingerprint} from {SYSTEM_KEYCHAIN}",
                file=sys.stderr,
            )
        self.fingerprint = None


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="trevrpc-interop",
        description="Runs every interop client against every interop server over every transport. "
        "Without --server, --client, or --browser, the packaged runner runs every packaged "
        "server and client, and the browsers it ships.",
    )
    _ = parser.add_argument("--out", type=Path, help="where logs go (default: a scratch directory)")
    _ = parser.add_argument(
        "--server", type=named_command, action="append", default=[], metavar="NAME=CMD"
    )
    _ = parser.add_argument(
        "--client", type=named_command, action="append", default=[], metavar="NAME=CMD"
    )
    _ = parser.add_argument("--browser", action="append", default=[], metavar="NAME")
    _ = parser.add_argument(
        "--browser-lanes", type=comma_list, default=list(BROWSER_LANES), metavar="LIST"
    )
    args = parse_with_defaults(parser, argv, ("server", "client", "browser"))
    if not args.server or not (args.client or args.browser):
        parser.error("need at least one --server and one --client or --browser")
    if unknown := set(args.browser_lanes) - set(BROWSER_LANES):
        parser.error(f"unknown browser lanes: {', '.join(sorted(unknown))}")
    return args


def execute(args: argparse.Namespace, work: Path) -> list[str]:
    """Runs every cell and returns the names of the failed ones."""
    servers: list[NamedCommand] = args.server
    clients: list[NamedCommand] = args.client
    browsers: list[str] = args.browser
    lanes: list[str] = args.browser_lanes
    out: Path = args.out or work / "out"
    out.mkdir(parents=True, exist_ok=True)
    certificate = generate_certificate(work)

    # With browsers, every server also serves the interop page from its HTTP fallback, for the
    # app lanes.
    server_arguments: list[str] = []
    if browsers:
        _ = run_checked([BROWSER, "--write-page", str(work / "web")])
        server_arguments = ["--web-root", str(work / "web")]

    failures: list[str] = []
    keychain = Keychain(certificate)
    try:
        if "safari" in browsers:
            keychain.trust()
        for spec in servers:
            server = Server(spec.name, spec.command, certificate, out, server_arguments)
            try:
                cells = [
                    cell for client in clients for cell in native_cells(client, server, certificate)
                ]
                cells += [
                    cell
                    for browser in browsers
                    for cell in browser_cells(browser, server, certificate, lanes)
                ]
                failures += [cell.name for cell in cells if not run_cell(cell, out)]
            finally:
                if (problem := server.stop()) is not None:
                    print(problem, file=sys.stderr)
                    failures.append(f"server-{spec.name}")
    finally:
        keychain.untrust()
    return failures


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    with tempfile.TemporaryDirectory(prefix="trevrpc-interop.") as work:
        try:
            failures = execute(args, Path(work))
        except RunnerError as error:
            print(f"trevrpc-interop: {error}", file=sys.stderr)
            return 1
    if failures:
        print("\n".join(f"failed: {name}" for name in failures), file=sys.stderr)
        return 1
    print("all interop cells passed")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
