#!/usr/bin/env python3
"""Runs every load-benchmark client against every server over every native transport, plus the
grpc-go and connect-go baselines; see README.md.

    run.py --server NAME=CMD... --client NAME=CMD... [--baseline CMD]
           [--scenarios LIST] [--sizes LIST] [--metadata-sizes LIST] [--concurrency LIST]
           [--payload repeat|records] [--compressions LIST]
           [--duration SECONDS] [--warmup SECONDS] [--messages N] [--netem PROFILE...]
           [--out DIR]
"""

from __future__ import annotations

import argparse
import json
import math
import os
import re
import subprocess
import sys
import tempfile
from dataclasses import dataclass, replace
from pathlib import Path
from typing import cast

# Modules next to this script, which -I leaves off the path, then the shared harness; appended so
# they never shadow the standard library.
sys.path.append(str(Path(__file__).resolve().parent))
sys.path.append(str(Path(__file__).resolve().parents[1] / "interop"))

import netem
import payload
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
# The TrevRPC transports, as the clients' --transport names them.
NATIVE_TRANSPORTS = ("quic", "tcp")
PAYLOADS = ("repeat", "records")
# Each compression mode, with the transports that have it; see README.md.
COMPRESSIONS = {
    "none": (*NATIVE_TRANSPORTS, *BASELINE_TRANSPORTS),
    "gzip": BASELINE_TRANSPORTS,
    "zstd": BASELINE_TRANSPORTS,
    "dictionary": NATIVE_TRANSPORTS,
    "stream": NATIVE_TRANSPORTS,
    "stream-dictionary": NATIVE_TRANSPORTS,
}
# The modes that compress with the trained dictionary.
DICTIONARY_COMPRESSIONS = ("dictionary", "stream-dictionary")
# The scenarios that compression modes other than none run: every message differs in them.
COMPRESSED_SCENARIOS = ("unary", "client-stream", "bidi")
# Room on top of warmup and duration for connecting and draining.
DEADLINE_SLACK = 60
COLUMNS = (
    "scenario", "size", "md", "conc", "transport", "compression", "client", "server",
    "ops/s", "msgs/s", "MB/s", "p50us", "p99us", "wire_B/msg", "lost%", "spread%",
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
    payload: str = "repeat"
    compressions: tuple[str, ...] = ("none",)
    # The trained dictionary, when a mode needs it.
    dictionary: Path | None = None
    # How many times each cell runs; the median run by messages per second stands for it.
    repeat: int = 1

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
    compression: str
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
    netem: str | None
    record: dict[str, object]
    # The messages per second of every run of the cell, when it ran more than once.
    runs: tuple[float, ...] = ()

    def row(self) -> list[str]:
        lost = self.lost / (self.lost + self.msgs) * 100 if self.lost > 0 else 0
        spread = (
            (max(self.runs) - min(self.runs)) / self.msgs_per_sec * 100
            if len(self.runs) > 1 and self.msgs_per_sec > 0
            else None
        )
        return [
            self.scenario,
            str(self.size),
            str(self.metadata_size),
            str(self.concurrency),
            self.transport,
            self.compression,
            self.client,
            self.server,
            str(math.floor(self.ops_per_sec)),
            str(math.floor(self.msgs_per_sec)),
            hundredths(self.bytes_per_sec / 1e6),
            str(math.floor(self.p50_us)),
            str(math.floor(self.p99_us)),
            "-" if self.wire_bytes_per_msg is None else str(math.floor(self.wire_bytes_per_msg)),
            hundredths(lost),
            "-" if spread is None else hundredths(spread),
        ]

    def sort_key(self) -> tuple[str, int, int, int, str, str, str, str]:
        return (
            self.scenario,
            self.size,
            self.metadata_size,
            self.concurrency,
            self.transport,
            self.compression,
            self.client,
            self.server,
        )


def plain(value: float) -> str:
    """Formats a number without a trailing `.0`."""
    return str(int(value)) if value.is_integer() else str(value)


def hundredths(value: float) -> str:
    """Rounds down to two decimals."""
    return plain(math.floor(value * 100) / 100)


def text_field(container: dict[str, object], name: str, default: str | None = None) -> str:
    value = container.get(name, default)
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


def parse_result(client: str, server: str, line: str, netem: str | None = None) -> Result:
    """Parses a client's JSON result line, raising ValueError if it is not a valid result. The
    record is tagged with the client, the server, and the netem profile, if any."""
    value = cast(object, json.loads(line))  # JSONDecodeError is a ValueError
    if not isinstance(value, dict):
        raise ValueError("not a JSON object")
    record: dict[str, object] = {"client": client, "server": server}
    if netem is not None:
        record["netem"] = netem
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
        compression=text_field(record, "compression", "none"),
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
        netem=netem,
        record=record,
    )


def median_run(results: list[Result]) -> Result:
    """The run with the median messages per second (the lower of the two middle ones for an even
    count), carrying every run's rate in `runs` and its record."""
    ordered = sorted(results, key=lambda result: result.msgs_per_sec)
    median = ordered[(len(ordered) - 1) // 2]
    if len(results) == 1:
        return median
    rates = tuple(result.msgs_per_sec for result in results)
    record = {**median.record, "repeats": len(results), "msgs_per_sec_runs": list(rates)}
    return replace(median, runs=rates, record=record)


def runs(compression: str, transport: str, scenario: str) -> bool:
    """Whether a cell runs: its transport has the compression mode, and the scenario shows
    something real with it (see README.md)."""
    if transport not in COMPRESSIONS[compression]:
        return False
    if transport in BASELINE_TRANSPORTS and scenario.startswith("unreliable-"):
        return False
    if compression == "none":
        return True
    if scenario not in COMPRESSED_SCENARIOS:
        return False
    return not (compression.startswith("stream") and scenario == "unary")


def train_dictionary(directory: Path) -> Path:
    """Trains the benchmark's dictionary from payload.py's samples with `zstd --train` in
    DIRECTORY and returns its path. The same samples always give the same dictionary."""
    samples = directory / "samples"
    samples.mkdir()
    for index, sample in enumerate(payload.training_samples()):
        _ = (samples / f"{index:04}").write_bytes(sample)
    dictionary = directory / "records.dict"
    command = ["zstd", "-q", "-f", "--train", "-r", str(samples), "-o", str(dictionary)]
    try:
        _ = subprocess.run(
            [*command, "--maxdict=8192", "--dictID=2"], check=True, capture_output=True
        )
    except FileNotFoundError as error:
        raise RunnerError("the dictionary modes need the zstd command to train it") from error
    except subprocess.CalledProcessError as error:
        raise RunnerError(f"zstd --train failed: {error.stderr.decode().strip()}") from error
    return dictionary


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
        self,
        client: str,
        server: str,
        command: list[str],
        transport: str,
        flags: list[str],
        netem: str | None,
    ) -> None:
        """Runs `COMMAND bench FLAGS...` with every scenario, compression mode, size, metadata
        size, and concurrency that runs on the transport, over a link with the `netem` profile
        applied, if any."""
        m = self.matrix
        profile = "" if netem is None else "-" + re.sub(r"[^A-Za-z0-9.]+", "_", netem)
        for scenario in m.scenarios:
            for compression in m.compressions:
                if not runs(compression, transport, scenario):
                    continue
                mode = ["--payload", m.payload, "--compression", compression]
                if compression in DICTIONARY_COMPRESSIONS and m.dictionary is not None:
                    mode += ["--dictionary", str(m.dictionary)]
                suffix = "" if compression == "none" else f"-{compression}"
                for size in m.sizes:
                    for metadata in m.metadata_sizes:
                        for workers in m.concurrency:
                            cell = (
                                f"{client}-{transport}-to-{server}-{scenario}{suffix}-{size}"
                                f"-md{metadata}-c{workers}{profile}"
                            )
                            self.run_one(
                                cell,
                                client,
                                server,
                                netem,
                                [
                                    *command, "bench", *flags, "--transport", transport,
                                    "--scenario", scenario, "--size", str(size),
                                    "--metadata-size", str(metadata),
                                    "--concurrency", str(workers),
                                    "--duration", plain(m.duration), "--warmup", plain(m.warmup),
                                    "--messages", str(m.messages), *mode,
                                ],
                            )  # fmt: skip

    def run_one(
        self, cell: str, client: str, server: str, netem: str | None, command: list[str]
    ) -> None:
        """Runs the cell `matrix.repeat` times and records its median run."""
        results: list[Result] = []
        repeat = self.matrix.repeat
        for index in range(repeat):
            name = cell if repeat == 1 else f"{cell}-r{index + 1}"
            result = self.run_once(name, client, server, netem, command)
            if result is None:
                return
            results.append(result)
        result = median_run(results)
        self.results.append(result)
        with (self.out / "results.jsonl").open("a", encoding="utf-8") as jsonl:
            _ = jsonl.write(json.dumps(result.record, separators=(",", ":")) + "\n")

    def run_once(
        self, cell: str, client: str, server: str, netem: str | None, command: list[str]
    ) -> Result | None:
        """Runs the cell once, logging to CELL.log, and returns its result, or None after
        recording why it failed."""
        log = self.out / f"{cell}.log"
        with log.open("w", encoding="utf-8") as stderr:
            completed = run_with_deadline(
                command, self.matrix.deadline, stdout=subprocess.PIPE, stderr=stderr
            )
        line = completed.stdout.strip()
        if completed.returncode is None:
            self.fail(cell, f"timed out after {self.matrix.deadline}s (see {log})")
            return None
        if completed.returncode != 0:
            self.fail(cell, f"exited {completed.returncode} (see {log})")
            return None
        try:
            result = parse_result(client, server, line, netem)
        except ValueError as error:
            self.fail(cell, f"printed an invalid result ({error}): {line}")
            return None
        if result.errors != 0:
            self.fail(cell, f"reported errors: {line}")
        return result

    def fail(self, cell: str, reason: str) -> None:
        print(f"failed: {cell} {reason}", file=sys.stderr, flush=True)
        self.failures.append(cell)

    def stop(self, server: Server) -> None:
        if (problem := server.stop()) is not None:
            print(problem, file=sys.stderr)
            self.failures.append(f"server-{server.name}")


def summary(results: list[Result]) -> str:
    """One table per netem profile, in the order they ran, with one row per run, grouped by
    scenario so transports and runtimes line up."""
    profiles: dict[str | None, list[Result]] = {}
    for result in results:
        profiles.setdefault(result.netem, []).append(result)
    return "\n".join(
        ("" if profile is None else f"netem {profile}\n") + table(group)
        for profile, group in profiles.items()
    )


def table(results: list[Result]) -> str:
    rows = [list(COLUMNS), *(r.row() for r in sorted(results, key=Result.sort_key))]
    return "".join(
        f"{row[0]:<26}" + "".join(f"{column:<14}" for column in row[1:]) + "\n" for row in rows
    )


def int_list(value: str) -> list[int]:
    try:
        return [int(item) for item in comma_list(value)]
    except ValueError as error:
        raise argparse.ArgumentTypeError(f"expected comma-separated integers: {value!r}") from error


def positive_int(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError(f"expected a positive integer: {value!r}") from error
    if parsed < 1:
        raise argparse.ArgumentTypeError(f"expected a positive integer: {value!r}")
    return parsed


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
        "--repeat",
        type=positive_int,
        default=1,
        metavar="N",
        help="run each cell N times and report the run with the median messages per second",
    )
    _ = parser.add_argument(
        "--payload",
        choices=PAYLOADS,
        default="repeat",
        help="what each request message holds: the letter x repeated, or realistic records",
    )
    _ = parser.add_argument(
        "--compressions",
        type=comma_list,
        default=["none"],
        metavar="LIST",
        help=f"compression modes to run, from {', '.join(COMPRESSIONS)}",
    )
    _ = parser.add_argument(
        "--netem",
        type=netem.profile,
        action="append",
        default=[],
        metavar="PROFILE",
        help="run the matrix once per profile over a link emulated with these netem arguments, "
        "such as 'delay 10ms loss 1%%', inside a network namespace (Linux)",
    )
    _ = parser.add_argument(
        "--out", type=Path, help="where results go (default: a new trevrpc-bench.* in $TMPDIR)"
    )
    args = parse_with_defaults(parser, argv, ("server", "client", "baseline"))
    if (not args.server or not args.client) and args.baseline is None:
        parser.error("need at least one --server and one --client, or --baseline")
    if unknown := set(args.compressions) - set(COMPRESSIONS):
        parser.error(f"unknown compression modes: {', '.join(sorted(unknown))}")
    if args.payload == "repeat" and set(args.compressions) != {"none"}:
        parser.error("compression modes other than none need --payload records")
    return args


def execute(args: argparse.Namespace, work: Path, out: Path) -> Runner:
    servers: list[NamedCommand] = args.server
    clients: list[NamedCommand] = args.client
    baseline: list[str] | None = args.baseline
    compressions: tuple[str, ...] = tuple(args.compressions)
    # Servers hold the trained dictionary only when a mode uses it, so zstd is needed only then.
    dictionary = (
        train_dictionary(work)
        if servers and set(compressions) & set(DICTIONARY_COMPRESSIONS)
        else None
    )
    runner = Runner(
        Matrix(
            scenarios=args.scenarios,
            sizes=args.sizes,
            metadata_sizes=args.metadata_sizes,
            concurrency=args.concurrency,
            duration=args.duration,
            warmup=args.warmup,
            messages=args.messages,
            repeat=args.repeat,
            payload=args.payload,
            compressions=compressions,
            dictionary=dictionary,
        ),
        out,
    )
    profiles: list[str | None] = [*args.netem] or [None]
    if args.netem:
        netem.configure(args.netem)
    certificate = generate_certificate(work)
    _ = (out / "results.jsonl").write_text("", encoding="utf-8")
    tls = ["--ca", str(certificate.cert), "--server-name", "localhost"]

    server_arguments = [] if dictionary is None else ["--dictionary", str(dictionary)]
    for spec in servers:
        server = Server(spec.name, spec.command, certificate, out, server_arguments)
        try:
            native = f"127.0.0.1:{server.port('native')}"
            qmux = f"127.0.0.1:{server.port('qmux')}"
            transports = {
                "quic": ["--addr", native, *tls],
                "tcp": ["--addr", native, "--tcp-addr", qmux, *tls],
            }
            for profile in profiles:
                apply(profile)
                for client in clients:
                    for transport, flags in transports.items():
                        runner.run_matrix(
                            client.name, spec.name, client.command, transport, flags, profile
                        )
        finally:
            runner.stop(server)

    if baseline is not None:
        server = Server("baseline", baseline, certificate, out)
        try:
            for profile in profiles:
                apply(profile)
                for transport in BASELINE_TRANSPORTS:
                    address = f"127.0.0.1:{server.port(transport)}"
                    runner.run_matrix(
                        transport,
                        transport,
                        baseline,
                        transport,
                        ["--addr", address, *tls],
                        profile,
                    )
        finally:
            runner.stop(server)
    return runner


def apply(profile: str | None) -> None:
    if profile is not None:
        netem.apply(profile)


def main(argv: list[str]) -> int:
    # parse_args consumes the package wrapper's defaults from the environment, and a runner
    # re-executed in a namespace needs them again.
    environment = dict(os.environ)
    args = parse_args(argv)
    if args.netem and not netem.inside_namespace():
        try:
            netem.enter_namespace(Path(__file__).resolve(), argv, environment)
        except RunnerError as error:
            print(f"trevrpc-bench: {error}", file=sys.stderr)
            return 1
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
