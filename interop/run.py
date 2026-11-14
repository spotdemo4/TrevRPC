#!/usr/bin/env python3
"""Runs every interop client against every interop server over every transport; see README.md.

    run.py [--out DIR] --server NAME=CMD... [--client NAME=CMD...] [--browser NAME...]
           [--browser-lanes webtransport,websocket,fallback,app-webtransport,app-websocket]

Servers and native clients are interop executables taking the `server` and `client` subcommands.
A runtime that does not implement everything yet takes part in what it does: a cell is skipped
when its server reports no port for the transport it needs or its client exits 77 for a transport
it lacks, and native clients skip the scenarios their server does not list. Only cells of the
runtimes named by `--may-skip` may skip; a skip anywhere else fails the run.
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
    Failure,
    NamedCommand,
    RunnerError,
    Server,
    comma_list,
    failure_details,
    failure_summary,
    generate_certificate,
    named_command,
    parse_with_defaults,
    run_checked,
    run_with_deadline,
    server_details,
    stop_server,
    write_vectors_dictionary,
)

BROWSER_LANES = ("webtransport", "websocket", "fallback", "app-webtransport", "app-websocket")
BROWSER = "trevrpc-interop-browser"
CELL_TIMEOUT = 120
# A UDP port that nothing answers, for cells that exercise the TCP fallback. It is below the
# ephemeral range, so it never collides with a server's own port, and is not on the browsers'
# list of unsafe ports.
UNREACHABLE_UDP = 24999
SYSTEM_KEYCHAIN = "/Library/Keychains/System.keychain"
# The shared compression golden vectors, next to the runner in the checkout and in the package.
# Their dictionary is the one the native servers and clients compress Echo messages with.
ZSTD_VECTORS = Path(__file__).resolve().parents[1] / "testdata" / "zstd-golden-vectors.txt"
KEYCHAIN_TIMEOUT = 30
# The exit status of a client that does not support the transport it was given.
UNSUPPORTED = 77


@dataclass(frozen=True)
class Cell:
    name: str
    command: list[str]
    # The runtimes of the cell: its client (or browser) and its server.
    runtimes: tuple[str, ...] = ()
    # Why the cell cannot run, such as a transport its server does not serve.
    skip: str | None = None


@dataclass(frozen=True)
class Skipped:
    """A cell that did not run, and why."""

    name: str
    reason: str


@dataclass(frozen=True)
class Passed:
    """A cell that passed, and how many scenarios its client skipped."""

    name: str
    skipped_scenarios: int


@dataclass
class Outcomes:
    """What the cells of a run came to."""

    failures: list[Failure]
    skipped: list[Skipped]
    passed: list[Passed]

    def add(self, outcome: Failure | Skipped | Passed) -> None:
        match outcome:
            case Failure():
                self.failures.append(outcome)
            case Skipped():
                self.skipped.append(outcome)
            case Passed():
                self.passed.append(outcome)

    def skip_summary(self) -> str:
        """The skipped cells and scenarios, so a runtime that implements only part of the matrix
        never looks like it covers all of it."""
        scenarios = sum(cell.skipped_scenarios for cell in self.passed)
        lines = [f"skipped: {len(self.skipped)} cells, {scenarios} scenarios in passing cells"]
        lines += [f"    {cell.name}: {cell.reason}" for cell in self.skipped]
        lines += [
            f"    {cell.name}: {cell.skipped_scenarios} scenarios"
            for cell in self.passed
            if cell.skipped_scenarios
        ]
        return "\n".join(lines)


def skipped_cell(name: str, runtimes: tuple[str, ...], server: Server, ports: list[str]) -> Cell:
    """A cell skipped because `server` serves none of `ports`."""
    return Cell(name, [], runtimes, skip=f"server {server.name} does not serve {', '.join(ports)}")


def unexpected_skip(
    cell: Cell, outcome: Failure | Skipped | Passed, may_skip: list[str]
) -> Failure | None:
    """A failure for a skipped cell, or a passing cell that skipped scenarios, when none of its
    runtimes may skip, so that a regression in a complete runtime cannot pass as a skip."""
    if any(runtime in may_skip for runtime in cell.runtimes):
        return None
    allowed = f"none of {', '.join(cell.runtimes)} may skip (--may-skip)"
    match outcome:
        case Skipped():
            return Failure(cell.name, [f"skipped, but {allowed}: {outcome.reason}"])
        case Passed() if outcome.skipped_scenarios:
            return Failure(
                cell.name, [f"skipped {outcome.skipped_scenarios} scenarios, but {allowed}"]
            )
        case _:
            return None


def native_cells(
    client: NamedCommand, server: Server, certificate: Certificate, dictionary: Path
) -> list[Cell]:
    """Every scenario over QUIC, over QMux over TLS, and over QUIC with a blocked UDP port, where
    the client must fall back to TCP. The QUIC and QMux lanes compress with `dictionary`, which
    every server holds; the fallback lane does not, so both kinds of client are covered."""
    native = server.ports.get("native")
    qmux = server.ports.get("qmux")
    tls = ["--ca", str(certificate.cert), "--server-name", "localhost"]
    # A server that supports only some scenarios has its clients skip the others.
    scenarios = [] if server.scenarios is None else ["--scenarios", ",".join(server.scenarios)]
    lanes = [
        ("", ["native"], native, None, "quic", "quic", True),
        ("-qmux", ["native", "qmux"], native, qmux, "tcp", "qmux", True),
        ("-fallback", ["qmux"], UNREACHABLE_UDP, qmux, "auto", "qmux", False),
    ]
    cells: list[Cell] = []
    runtimes = (client.name, server.name)
    for suffix, needs, udp, tcp, transport, expected, compressed in lanes:
        name = f"{client.name}{suffix}-to-{server.name}"
        if missing := server.missing(*needs):
            cells.append(skipped_cell(name, runtimes, server, missing))
            continue
        command = [*client.command, "client", "--addr", f"127.0.0.1:{udp}"]
        if tcp is not None:
            command += ["--tcp-addr", f"127.0.0.1:{tcp}"]
        command += [*tls, "--transport", transport, "--expect-transport", expected]
        if compressed:
            command += ["--dictionary", str(dictionary)]
        cells.append(Cell(name, [*command, *scenarios], runtimes))
    return cells


def write_dictionary(vectors: Path, directory: Path) -> Path:
    """Writes the dictionary of the compression golden vectors to DIRECTORY and returns its path."""
    try:
        text = vectors.read_text(encoding="utf-8")
    except OSError as error:
        raise RunnerError(f"could not read {vectors}: {error}") from error
    return write_vectors_dictionary(text, str(vectors), directory)


def browser_cells(
    browser: str, server: Server, certificate: Certificate, lanes: list[str], dictionary: Path
) -> list[Cell]:
    """Every scenario over the selected lanes: WebTransport, QMux over WebSocket, the fallback
    from a blocked UDP port to WebSocket, and the app lanes, where the server serves the page.
    The webtransport, websocket and app-webtransport lanes compress with `dictionary`; the
    others do not, so both kinds of browser client are covered."""
    # The ports each lane needs. Over HTTP/2 the page comes from the server's TCP listener, over
    # HTTP/3 from its UDP socket.
    app_page = "http3" if browser == "chromium" else "http2"
    needs = {
        "webtransport": ["webtransport"],
        "websocket": ["webtransport", "websocket"],
        "fallback": ["websocket"],
        "app-webtransport": [app_page, "webtransport"],
        "app-websocket": ["http2", "webtransport", "websocket"],
    }
    cells: list[Cell] = []
    runtimes = (browser, server.name)
    for lane in [lane for lane in BROWSER_LANES if lane in lanes]:
        name = f"{browser}-{lane}-to-{server.name}"
        if missing := server.missing(*needs[lane]):
            cells.append(skipped_cell(name, runtimes, server, missing))
        elif server.scenarios is not None:
            # The browser runner cannot leave scenarios out yet.
            reason = f"server {server.name} supports only some scenarios"
            cells.append(Cell(name, [], runtimes, skip=reason))
        else:
            arguments = browser_arguments(browser, lane, server, certificate, dictionary)
            cells.append(Cell(name, [BROWSER, "--browser", browser, *arguments], runtimes))
    return cells


def browser_arguments(
    browser: str, lane: str, server: Server, certificate: Certificate, dictionary: Path
) -> list[str]:
    """The `trevrpc-interop-browser` arguments of one browser lane."""
    cert_hash = hashlib.sha256(certificate.der()).hexdigest()
    port = server.ports.get
    webtransport = ["--url", f"https://127.0.0.1:{port('webtransport')}/trevrpc"]
    websocket = ["--ws-url", f"wss://127.0.0.1:{port('websocket')}/trevrpc"]
    pin = ["--cert-hash", cert_hash]
    compress = ["--dictionary", str(dictionary)]
    over_websocket = ["--transport", "websocket", "--expect-transport", "websocket"]
    page_tls = ["--cert", str(certificate.cert), "--key", str(certificate.key)]
    # Over HTTP/2 the page comes from the server's TCP listener, over HTTP/3 from its UDP socket.
    # Their ports differ for the Rust server.
    page_h2 = ["--page-url", f"https://127.0.0.1:{port('http2')}/"]
    page_h3 = ["--page-url", f"https://127.0.0.1:{port('http3')}/"]
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
        "webtransport": [*webtransport, *pin, *compress],
        "websocket": [*webtransport, *websocket, *pin, *over_websocket, *compress],
        "fallback": [
            "--url", f"https://127.0.0.1:{UNREACHABLE_UDP}/trevrpc", *websocket, *pin,
            "--transport", "auto", "--expect-transport", "websocket",
        ],
        "app-webtransport": [
            *app_webtransport, *page_tls, *webtransport, *pin,
            "--expect-transport", "webtransport", *compress,
        ],
        "app-websocket": [
            *app_websocket, *page_tls, *webtransport, *websocket, *pin, *over_websocket,
        ],
    }  # fmt: skip
    return commands[lane]


def run_cell(cell: Cell, server: Server, out: Path) -> Failure | Skipped | Passed:
    """Runs one cell, logging to OUT/CELL.log, prints the log, and returns why it failed, if
    it did, with what the server logged meanwhile, or why it was skipped."""
    print(f"== {cell.name}", flush=True)
    if cell.skip is not None:
        print(f"skipped: {cell.skip}", flush=True)
        return Skipped(cell.name, cell.skip)
    log = out / f"{cell.name}.log"
    offset = server.log_size()
    with log.open("w", encoding="utf-8") as output:
        result = run_with_deadline(cell.command, CELL_TIMEOUT, stdout=output, stderr=output)
        if result.returncode is None:
            print(f"timed out after {CELL_TIMEOUT}s", file=output)
    text = log.read_text(encoding="utf-8", errors="replace")
    _ = sys.stdout.write(text)
    _ = sys.stdout.flush()
    if result.returncode == 0:
        skipped = sum(1 for line in text.splitlines() if line.startswith("skip "))
        return Passed(cell.name, skipped)
    if result.returncode == UNSUPPORTED:
        reason = (text.strip().splitlines() or ["the client does not support this transport"])[-1]
        print(f"skipped: {reason}", flush=True)
        return Skipped(cell.name, reason)
    details = failure_details(text) or [f"exited {result.returncode} without output"]
    return Failure(cell.name, [*details, *server_details(server, offset)])


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
    _ = parser.add_argument(
        "--may-skip",
        action="append",
        default=[],
        metavar="NAME",
        help="a runtime whose cells may skip transports and scenarios it does not implement yet",
    )
    args = parse_with_defaults(parser, argv, ("server", "client", "browser"))
    if not args.server or not (args.client or args.browser):
        parser.error("need at least one --server and one --client or --browser")
    if unknown := set(args.browser_lanes) - set(BROWSER_LANES):
        parser.error(f"unknown browser lanes: {', '.join(sorted(unknown))}")
    return args


def execute(args: argparse.Namespace, work: Path) -> Outcomes:
    """Runs every cell and returns what each came to."""
    servers: list[NamedCommand] = args.server
    clients: list[NamedCommand] = args.client
    browsers: list[str] = args.browser
    lanes: list[str] = args.browser_lanes
    out: Path = args.out or work / "out"
    out.mkdir(parents=True, exist_ok=True)
    certificate = generate_certificate(work)
    dictionary = write_dictionary(ZSTD_VECTORS, work)

    # Every server holds the compression dictionary. With browsers, every server also serves the
    # interop page from its HTTP fallback, for the app lanes.
    server_arguments: list[str] = ["--dictionary", str(dictionary)]
    if browsers:
        _ = run_checked([BROWSER, "--write-page", str(work / "web")])
        server_arguments += ["--web-root", str(work / "web")]

    outcomes = Outcomes([], [], [])
    keychain = Keychain(certificate)
    try:
        if "safari" in browsers:
            keychain.trust()
        for spec in servers:
            server = Server(spec.name, spec.command, certificate, out, server_arguments)
            try:
                cells = [
                    cell
                    for client in clients
                    for cell in native_cells(client, server, certificate, dictionary)
                ]
                cells += [
                    cell
                    for browser in browsers
                    for cell in browser_cells(browser, server, certificate, lanes, dictionary)
                ]
                for cell in cells:
                    outcome = run_cell(cell, server, out)
                    outcomes.add(outcome)
                    if (failure := unexpected_skip(cell, outcome, args.may_skip)) is not None:
                        outcomes.add(failure)
            finally:
                if (failure := stop_server(server)) is not None:
                    outcomes.add(failure)
    finally:
        keychain.untrust()
    return outcomes


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    with tempfile.TemporaryDirectory(prefix="trevrpc-interop.") as work:
        try:
            outcomes = execute(args, Path(work))
        except RunnerError as error:
            print(f"trevrpc-interop: {error}", file=sys.stderr)
            return 1
    if outcomes.skipped or any(cell.skipped_scenarios for cell in outcomes.passed):
        print(outcomes.skip_summary(), file=sys.stderr)
    if outcomes.failures:
        print(failure_summary(outcomes.failures), file=sys.stderr)
        return 1
    if outcomes.skipped or any(cell.skipped_scenarios for cell in outcomes.passed):
        allowed = ", ".join(args.may_skip)
        print(f"all interop cells that ran passed; the skips above are allowed for {allowed}")
    else:
        print("all interop cells passed")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
