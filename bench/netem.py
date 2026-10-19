"""Network emulation for `run.py --netem`; see README.md.

The runner re-executes itself in a new user and network namespace, whose loopback link it shapes
with netem, so benchmarks see delay and loss without root and without touching the host's network.
Every server and client the runner starts inherits the namespace.

The link carries Ethernet-sized frames. A root tbf qdisc segments TCP (TSO) and UDP (GSO)
super-packets into frames before its netem child sees them, as a NIC does before the network
drops anything. Without it netem would drop whole super-packets: up to 64 KB of TCP, or a QUIC
stack's whole batch of datagrams, as one "packet". Its rate is far above what loopback reaches, so
it segments without shaping.
"""

from __future__ import annotations

import argparse
import os
import shlex
import subprocess
import sys
from pathlib import Path
from typing import NoReturn

from harness import RunnerError

# Set in the environment of the runner re-executed inside the namespace.
NAMESPACE_VARIABLE = "TREVRPC_BENCH_NETNS"
MTU = 1500
# An MTU-sized IP packet plus its Ethernet header: the largest frame tbf passes unsegmented.
FRAME_BYTES = MTU + 14
# netem's default queue of 1000 packets overflows from delay alone at loopback rates, which would
# add loss nobody asked for. A `limit` in a profile comes later on the command line and wins.
NETEM_LIMIT = 1_000_000


def profile(value: str) -> str:
    """Parses one --netem profile, netem's arguments as on a tc command line, as an argparse
    type."""
    try:
        words = shlex.split(value)
    except ValueError as error:
        message = f"cannot parse netem profile {value!r}: {error}"
        raise argparse.ArgumentTypeError(message) from error
    if not words:
        raise argparse.ArgumentTypeError("empty netem profile")
    return shlex.join(words)


def inside_namespace() -> bool:
    return os.environ.get(NAMESPACE_VARIABLE) == "1"


def namespace_command(script: Path, argv: list[str]) -> list[str]:
    """The command that runs `script ARGV...` in new user and network namespaces, as root in the
    user namespace so that it may configure the network namespace's links."""
    return [
        "unshare", "--user", "--map-root-user", "--net", "--",
        sys.executable, "-I", str(script), *argv,
    ]  # fmt: skip


def enter_namespace(script: Path, argv: list[str], environment: dict[str, str]) -> NoReturn:
    """Replaces this process with `script ARGV...` inside new namespaces. `environment` is the
    environment to run it with, as this process started with it."""
    if not sys.platform.startswith("linux"):
        raise RunnerError("--netem needs Linux network namespaces")
    command = namespace_command(script, argv)
    try:
        os.execvpe(command[0], command, {**environment, NAMESPACE_VARIABLE: "1"})
    except OSError as error:
        raise RunnerError(f"cannot run unshare (from util-linux): {error}") from error


def link_commands() -> list[list[str]]:
    """Brings up the namespace's loopback link with an Ethernet MTU and the segmenting tbf."""
    return [
        ["ip", "link", "set", "lo", "mtu", str(MTU), "up"],
        [
            "tc", "qdisc", "add", "dev", "lo", "root", "handle", "1:",
            "tbf", "rate", "100gbit", "burst", str(FRAME_BYTES), "limit", "100mb",
        ],
    ]  # fmt: skip


def profile_command(netem: str) -> list[str]:
    """Sets the netem child of the tbf to `netem`, adding it on first use."""
    return [
        "tc", "qdisc", "replace", "dev", "lo", "parent", "1:1", "handle", "10:",
        "netem", "limit", str(NETEM_LIMIT), *shlex.split(netem),
    ]  # fmt: skip


def configure(profiles: list[str]) -> None:
    """Sets up the namespace's link and checks that tc accepts every profile, so a bad one fails
    before any benchmark runs. The last profile stays applied."""
    for command in [*link_commands(), *map(profile_command, profiles)]:
        _run(command)


def apply(netem: str) -> None:
    _run(profile_command(netem))


def _run(command: list[str]) -> None:
    try:
        completed = subprocess.run(command, capture_output=True, text=True, check=False)
    except OSError as error:
        raise RunnerError(f"cannot run {command[0]} (from iproute2): {error}") from error
    if completed.returncode != 0:
        raise RunnerError(f"{shlex.join(command)} failed: {completed.stderr.strip()}")
