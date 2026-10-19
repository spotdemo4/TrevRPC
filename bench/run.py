#!/usr/bin/env python3
"""Runs every load-benchmark client against every server over every native transport, plus the
grpc-go and connect-go baselines; see README.md.

    run.py --server NAME=CMD... --client NAME=CMD... [--baseline CMD]
           [--scenarios LIST] [--sizes LIST] [--metadata-sizes LIST] [--concurrency LIST]
           [--duration SECONDS] [--warmup SECONDS] [--messages N] [--out DIR]
"""

from __future__ import annotations

import argparse
import json
import math
import subprocess
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import cast

# The shared harness; appended so it never shadows modules next to this script.
sys.path.append(str(Path(__file__).resolve().parents[1] / "interop"))

from harness import (
    NamedCommand,
    RunnerError,
    Server,
    comma_list,
    command_line,
    generate_certificate,
    named_command,
    parse_with_defaults,
    run_with_deadline,
)

SCENARIOS = (
    "unary",
    "server-stream",
    "client-stream",
    "bidi",
    "unreliable-server-stream",
    "unreliable-client-stream",
)
# The baseline's transports, each served on the port of the same name. They have no datagram
# messages, so they skip the unreliable-* scenarios.
BASELINE_TRANSPORTS = ("grpc", "connect")
# Room on top of warmup and duration for connecting and draining.
DEADLINE_SLACK = 60
COLUMNS = (
    "scenario", "size", "md", "conc", "transport", "client", "server",
    "ops/s", "msgs/s", "MB/s", "p50us", "p99us", "wire_B/msg", "lost%",
)  # fmt: skip


@dataclass(frozen=True)
class Matrix:
    scenarios: list[str]
    sizes: list[int]
    metadata_sizes: list[int]
    concurrency: list[int]
    duration: float
    warmup: float
    messages: int

    @property
    def deadline(self) -> int:
        return int(self.duration + self.warmup + DEADLINE_SLACK)


@dataclass(frozen=True)
class Result:
    """One run's JSON result, tagged with the client and server that produced it."""

    client: str
    server: str
    scenario: str
    transport: str
    size: int
    metadata_size: int
    concurrency: int
    errors: float
    msgs: float
    lost: float
    ops_per_sec: float
    msgs_per_sec: float
    bytes_per_sec: float
    p50_us: float
    p99_us: float
    wire_bytes_per_msg: float | None
    record: dict[str, object]

    def row(self) -> list[str]:
        lost = self.lost / (self.lost + self.msgs) * 100 if self.lost > 0 else 0
        return [
            self.scenario,
            str(self.size),
            str(self.metadata_size),
            str(self.concurrency),
            self.transport,
            self.client,
            self.server,
            str(math.floor(self.ops_per_sec)),
            str(math.floor(self.msgs_per_sec)),
            hundredths(self.bytes_per_sec / 1e6),
            str(math.floor(self.p50_us)),
            str(math.floor(self.p99_us)),
            "-" if self.wire_bytes_per_msg is None else str(math.floor(self.wire_bytes_per_msg)),
            hundredths(lost),
        ]

    def sort_key(self) -> tuple[str, int, int, int, str, str, str]:
        return (
            self.scenario,
            self.size,
            self.metadata_size,
            self.concurrency,
            self.transport,
            self.client,
            self.server,
        )


def plain(value: float) -> str:
    """Formats a number without a trailing `.0`."""
    return str(int(value)) if value.is_integer() else str(value)


def hundredths(value: float) -> str:
    """Rounds down to two decimals."""
    return plain(math.floor(value * 100) / 100)


def text_field(container: dict[str, object], name: str) -> str:
    value = container.get(name)
    if not isinstance(value, str):
        raise ValueError(f"no valid {name!r}")
    return value


def int_field(container: dict[str, object], name: str, default: int | None = None) -> int:
    value = container.get(name, default)
    if not isinstance(value, int) or isinstance(value, bool):
        raise ValueError(f"no valid {name!r}")
    return value


def number_field(container: dict[str, object], name: str, default: float | None = None) -> float:
    value = container.get(name, default)
    if not isinstance(value, int | float) or isinstance(value, bool):
        raise ValueError(f"no valid {name!r}")
    return float(value)


def parse_result(client: str, server: str, line: str) -> Result:
    """Parses a client's JSON result line, raising ValueError if it is not a valid result."""
    value = cast(object, json.loads(line))  # JSONDecodeError is a ValueError
    if not isinstance(value, dict):
        raise ValueError("not a JSON object")
    record: dict[str, object] = {"client": client, "server": server}
    record.update(cast(dict[str, object], value))
    latency = record.get("latency_us")
    if not isinstance(latency, dict):
        raise ValueError("no valid 'latency_us'")
    latency = cast(dict[str, object], latency)
    return Result(
        client=client,
        server=server,
        scenario=text_field(record, "scenario"),
        transport=text_field(record, "transport"),
        size=int_field(record, "size"),
        metadata_size=int_field(record, "metadata_size", 0),
        concurrency=int_field(record, "concurrency"),
        errors=number_field(record, "errors"),
        msgs=number_field(record, "msgs"),
        lost=number_field(record, "lost", 0),
        ops_per_sec=number_field(record, "ops_per_sec"),
        msgs_per_sec=number_field(record, "msgs_per_sec"),
        bytes_per_sec=number_field(record, "bytes_per_sec"),
        p50_us=number_field(latency, "p50"),
        p99_us=number_field(latency, "p99"),
        wire_bytes_per_msg=(
            None
            if record.get("wire_bytes_per_msg") is None
            else number_field(record, "wire_bytes_per_msg")
        ),
        record=record,
    )


class Runner:
    """Runs the matrix for each client and server, collecting results and failures."""

    matrix: Matrix
    out: Path
    results: list[Result]
    failures: list[str]

    def __init__(self, matrix: Matrix, out: Path) -> None:
        self.matrix = matrix
        self.out = out
        self.results = []
        self.failures = []

    def run_matrix(
        self, client: str, server: str, command: list[str], transport: str, flags: list[str]
    ) -> None:
        """Runs `COMMAND bench FLAGS...` with every scenario, size, metadata size, and
        concurrency."""
        m = self.matrix
        for scenario in m.scenarios:
            if transport in BASELINE_TRANSPORTS and scenario.startswith("unreliable-"):
                continue
            for size in m.sizes:
                for metadata in m.metadata_sizes:
                    for workers in m.concurrency:
                        cell = (
                            f"{client}-{transport}-to-{server}-{scenario}-{size}"
                            f"-md{metadata}-c{workers}"
                        )
                        self.run_one(
                            cell,
                            client,
                            server,
                            [
                                *command, "bench", *flags, "--transport", transport,
                                "--scenario", scenario, "--size", str(size),
                                "--metadata-size", str(metadata), "--concurrency", str(workers),
                                "--duration", plain(m.duration), "--warmup", plain(m.warmup),
                                "--messages", str(m.messages),
                            ],
                        )  # fmt: skip

    def run_one(self, cell: str, client: str, server: str, command: list[str]) -> None:
        log = self.out / f"{cell}.log"
        with log.open("w", encoding="utf-8") as stderr:
            completed = run_with_deadline(
                command, self.matrix.deadline, stdout=subprocess.PIPE, stderr=stderr
            )
        line = completed.stdout.strip()
        if completed.returncode is None:
            self.fail(cell, f"timed out after {self.matrix.deadline}s (see {log})")
            return
        if completed.returncode != 0:
            self.fail(cell, f"exited {completed.returncode} (see {log})")
            return
        try:
            result = parse_result(client, server, line)
        except ValueError as error:
            self.fail(cell, f"printed an invalid result ({error}): {line}")
            return
        self.results.append(result)
        with (self.out / "results.jsonl").open("a", encoding="utf-8") as results:
            _ = results.write(json.dumps(result.record, separators=(",", ":")) + "\n")
        if result.errors != 0:
            self.fail(cell, f"reported errors: {line}")

    def fail(self, cell: str, reason: str) -> None:
        print(f"failed: {cell} {reason}", file=sys.stderr, flush=True)
        self.failures.append(cell)

    def stop(self, server: Server) -> None:
        if (problem := server.stop()) is not None:
            print(problem, file=sys.stderr)
            self.failures.append(f"server-{server.name}")


def summary(results: list[Result]) -> str:
    """One row per run, grouped by scenario so transports and runtimes line up."""
    rows = [list(COLUMNS), *(r.row() for r in sorted(results, key=Result.sort_key))]
    return "".join(
        f"{row[0]:<26}" + "".join(f"{column:<14}" for column in row[1:]) + "\n" for row in rows
    )


def int_list(value: str) -> list[int]:
    try:
        return [int(item) for item in comma_list(value)]
    except ValueError as error:
        raise argparse.ArgumentTypeError(f"expected comma-separated integers: {value!r}") from error


def seconds(value: str) -> float:
    try:
        parsed = float(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError(f"expected seconds: {value!r}") from error
    if not math.isfinite(parsed) or parsed < 0:
        raise argparse.ArgumentTypeError(f"expected non-negative seconds: {value!r}")
    return parsed


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="trevrpc-bench",
        description="Runs every load-benchmark client against every server, plus the grpc-go and "
        "connect-go baselines. Without --server, --client, or --baseline, the packaged runner "
        "uses the packaged ones.",
    )
    _ = parser.add_argument(
        "--server", type=named_command, action="append", default=[], metavar="NAME=CMD"
    )
    _ = parser.add_argument(
        "--client", type=named_command, action="append", default=[], metavar="NAME=CMD"
    )
    _ = parser.add_argument(
        "--baseline", type=command_line, metavar="CMD", help="the grpc-go and connect-go baseline"
    )
    _ = parser.add_argument("--scenarios", type=comma_list, default=list(SCENARIOS))
    _ = parser.add_argument("--sizes", type=int_list, default=[64])
    _ = parser.add_argument("--metadata-sizes", type=int_list, default=[0])
    _ = parser.add_argument("--concurrency", type=int_list, default=[1, 64])
    _ = parser.add_argument("--duration", type=seconds, default=5.0)
    _ = parser.add_argument("--warmup", type=seconds, default=1.0)
    _ = parser.add_argument("--messages", type=int, default=100)
    _ = parser.add_argument(
        "--out", type=Path, help="where results go (default: a new trevrpc-bench.* in $TMPDIR)"
    )
    args = parse_with_defaults(parser, argv, ("server", "client", "baseline"))
    if (not args.server or not args.client) and args.baseline is None:
        parser.error("need at least one --server and one --client, or --baseline")
    return args


def execute(args: argparse.Namespace, work: Path, out: Path) -> Runner:
    servers: list[NamedCommand] = args.server
    clients: list[NamedCommand] = args.client
    baseline: list[str] | None = args.baseline
    runner = Runner(
        Matrix(
            scenarios=args.scenarios,
            sizes=args.sizes,
            metadata_sizes=args.metadata_sizes,
            concurrency=args.concurrency,
            duration=args.duration,
            warmup=args.warmup,
            messages=args.messages,
        ),
        out,
    )
    certificate = generate_certificate(work)
    _ = (out / "results.jsonl").write_text("", encoding="utf-8")
    tls = ["--ca", str(certificate.cert), "--server-name", "localhost"]

    for spec in servers:
        server = Server(spec.name, spec.command, certificate, out)
        try:
            native = f"127.0.0.1:{server.port('native')}"
            qmux = f"127.0.0.1:{server.port('qmux')}"
            transports = {
                "quic": ["--addr", native, *tls],
                "tcp": ["--addr", native, "--tcp-addr", qmux, *tls],
            }
            for client in clients:
                for transport, flags in transports.items():
                    runner.run_matrix(client.name, spec.name, client.command, transport, flags)
        finally:
            runner.stop(server)

    if baseline is not None:
        server = Server("baseline", baseline, certificate, out)
        try:
            for transport in BASELINE_TRANSPORTS:
                address = f"127.0.0.1:{server.port(transport)}"
                runner.run_matrix(
                    transport, transport, baseline, transport, ["--addr", address, *tls]
                )
        finally:
            runner.stop(server)
    return runner


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    # Results outlive the run, unlike the scratch directory.
    out: Path = args.out or Path(tempfile.mkdtemp(prefix="trevrpc-bench."))
    out.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="trevrpc-bench-work.") as work:
        try:
            runner = execute(args, Path(work), out)
        except RunnerError as error:
            print(f"trevrpc-bench: {error}", file=sys.stderr)
            return 1
    table = summary(runner.results)
    _ = sys.stdout.write(table)
    _ = (out / "summary.txt").write_text(table, encoding="utf-8")
    if runner.failures:
        print("\n".join(f"failed: {cell}" for cell in runner.failures), file=sys.stderr)
        return 1
    print(f"all benchmark runs passed; results in {out / 'results.jsonl'}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
