#!/usr/bin/env python3
"""Runs the Go and Rust microbenchmarks from the checkout; see the wiki's Benchmarks page.

    micro.py [--runtime go|rust]... [--count N] [--bench REGEX] [--baseline DIR] [--out DIR]

Go results land in DIR/go.txt and Rust results in DIR/rust.txt. With `--baseline`, benchstat
compares the baseline's go.txt with this run's into DIR/benchstat.txt. divan has no comparison
tool, so compare the two rust.txt files side by side.
"""

from __future__ import annotations

import argparse
import shlex
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import IO, cast

# The shared harness; appended so it never shadows modules next to this script.
sys.path.append(str(Path(__file__).resolve().parents[1] / "interop"))

from harness import RunnerError

RUNTIMES = ("go", "rust")


def find_root(start: Path) -> Path:
    """Returns the checkout that contains `start`."""
    for directory in (start, *start.parents):
        if (directory / "trevrpc-go" / "go.mod").is_file() and (
            directory / "trevrpc-rust" / "Cargo.toml"
        ).is_file():
            return directory
    raise RunnerError(f"{start} is not inside a TrevRPC checkout; run from one or pass --root")


def tee(command: list[str], output: Path, cwd: Path | None = None) -> None:
    """Runs a command, copying its stdout to the terminal and to `output`; stderr passes through.

    Raises RunnerError unless the command exits 0.
    """
    print(f"== {shlex.join(command)}", flush=True)
    with output.open("w", encoding="utf-8") as file:
        try:
            process = subprocess.Popen(command, cwd=cwd, stdout=subprocess.PIPE, text=True)
        except OSError as error:
            raise RunnerError(f"could not run {shlex.join(command)}: {error}") from error
        with process:
            for line in cast(IO[str], process.stdout):
                _ = sys.stdout.write(line)
                _ = sys.stdout.flush()
                _ = file.write(line)
    if process.returncode != 0:
        raise RunnerError(f"exited {process.returncode}: {shlex.join(command)}")


def go_command(root: Path, pattern: str, count: int) -> list[str]:
    trevrpc_go = str(root / "trevrpc-go")
    return [
        "go", "-C", trevrpc_go, "test", "-run", "^$", "-bench", pattern, "-count", str(count),
        "./...",
    ]  # fmt: skip


def benchstat_command(old: Path, new: Path) -> list[str]:
    return ["benchstat", str(old), str(new)]


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="trevrpc-bench-micro",
        description="Runs the Go and Rust microbenchmarks and compares Go runs with benchstat.",
    )
    _ = parser.add_argument(
        "--runtime",
        choices=RUNTIMES,
        action="append",
        help="runtime to benchmark; repeat for both (default: both)",
    )
    _ = parser.add_argument(
        "--count", type=int, default=5, help="Go runs per benchmark; use 10+ to compare (5)"
    )
    _ = parser.add_argument(
        "--bench", default=".", metavar="REGEX", help="Go benchmarks to run (all)"
    )
    _ = parser.add_argument(
        "--baseline", type=Path, metavar="DIR", help="an earlier --out to compare Go results with"
    )
    _ = parser.add_argument(
        "--out",
        type=Path,
        help="where results go (default: a new trevrpc-bench-micro.* in $TMPDIR)",
    )
    _ = parser.add_argument("--root", type=Path, help="the checkout (default: the current one)")
    args = parser.parse_args(argv)
    if args.count < 1:
        parser.error("--count must be at least 1")
    return args


def execute(args: argparse.Namespace) -> Path:
    runtimes: list[str] = args.runtime or list(RUNTIMES)
    baseline: Path | None = args.baseline
    if baseline is not None:
        if "go" not in runtimes:
            raise RunnerError("--baseline compares Go results, so it needs --runtime go")
        if not (baseline / "go.txt").is_file():
            raise RunnerError(f"{baseline / 'go.txt'} does not exist")
    root = find_root((args.root or Path.cwd()).resolve())
    out: Path = args.out or Path(tempfile.mkdtemp(prefix="trevrpc-bench-micro."))
    if baseline is not None and baseline.resolve() == out.resolve():
        raise RunnerError("--out must differ from --baseline, or the run overwrites it")
    out.mkdir(parents=True, exist_ok=True)

    if "go" in runtimes:
        tee(go_command(root, args.bench, args.count), out / "go.txt")
    if "rust" in runtimes:
        tee(["cargo", "bench"], out / "rust.txt", cwd=root / "trevrpc-rust")
    if baseline is not None:
        tee(benchstat_command(baseline / "go.txt", out / "go.txt"), out / "benchstat.txt")
    return out


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    try:
        out = execute(args)
    except RunnerError as error:
        print(f"trevrpc-bench-micro: {error}", file=sys.stderr)
        return 1
    print(f"results in {out}; compare a later run with --baseline {out}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
