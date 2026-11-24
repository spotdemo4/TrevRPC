"""Network emulation for `run.py --netem`; see README.md.

The runner re-executes itself in a new user and network namespace, whose loopback link it shapes
with netem, so benchmarks see delay and loss without root and without touching the host's network.
Every server and client the runner starts inherits the namespace.

The link carries Ethernet-sized frames. A root tbf qdisc segments TCP (TSO) and UDP (GSO)
super-packets into frames before its netem children see them, as a NIC does before the network
drops anything. Without it netem would drop whole super-packets: up to 64 KB of TCP, or a QUIC
stack's whole batch of datagrams, as one "packet". Its rate is far above what loopback reaches, so
it segments without shaping.

Under the tbf, a prio qdisc has two bands, each with its own netem: band 1 (UP_NETEM) and band 2
(DOWN_NETEM). Every packet goes to band 1 unless a filter sends it to band 2. A profile that
names no direction has no filters, so both directions share band 1's queue and rate, as on a
shared medium such as Wi-Fi. One that does gets a filter per server port, which sends what the
servers send to band 2, so each direction has its own queue, as on a cellular link. An outage
adds a filter ahead of those that drops every packet.

A profile is netem's arguments, as on a tc command line, optionally followed by sections:

    NETEM_ARGS [up NETEM_ARGS] [down NETEM_ARGS] [outage DURATION every PERIOD]

`up` (client to server) and `down` (server to client) add arguments to the shared ones for that
direction only; netem takes the last of a repeated option, so they can also override one.
"""

from __future__ import annotations

import argparse
import os
import re
import shlex
import subprocess
import sys
import threading
import time
from collections.abc import Callable, Generator, Iterable, Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
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
# The prio qdisc under the tbf, its bands, and the netem in each.
PRIO = "10:"
UP_BAND, DOWN_BAND = "10:1", "10:2"
UP_NETEM, DOWN_NETEM = "20:", "30:"
# Filter priorities on the prio qdisc: the outage's drop-everything filter comes first.
OUTAGE_PRIORITY, DIRECTION_PRIORITY = "1", "2"
# The port of the direction filter configure adds and deletes to check that tc can add one.
PROBE_PORT = 1
# Ports the kernel never picks for a client's socket, so no client sends from a server's port.
RESERVED_PORTS = Path("/proc/sys/net/ipv4/ip_local_reserved_ports")
SECTIONS = ("up", "down", "outage")
DURATION = re.compile(r"(\d+(?:\.\d+)?)(ms|s)")


@dataclass(frozen=True)
class Outage:
    """The link drops every packet for `duration` seconds out of every `period`, the first
    starting half a period after the client does."""

    duration: float
    period: float

    def windows(self, until: float) -> Iterator[tuple[float, float]]:
        """The outages, as (start, end) in seconds after the client starts, that end by `until`:
        the end of the client's measured window, after which an outage would only cut short the
        calls the window counts."""
        start = self.period / 2
        while start + self.duration <= until:
            yield start, start + self.duration
            start += self.period


@dataclass(frozen=True)
class Profile:
    """A parsed profile: netem's arguments for each direction, and the outages, if any."""

    up: tuple[str, ...]
    down: tuple[str, ...]
    # Whether each direction has its own queue; see the module docstring.
    split: bool
    outage: Outage | None


def seconds(value: str) -> float:
    match = DURATION.fullmatch(value)
    if match is None:
        raise ValueError(f"not a duration such as 500ms or 2s: {value!r}")
    number = float(match[1])
    return number / 1000 if match[2] == "ms" else number


def parse(value: str) -> Profile:
    """Parses a profile; see the module docstring."""
    words = shlex.split(value)
    shared: list[str] = []
    directions: dict[str, list[str]] = {}
    outage: Outage | None = None
    target = shared
    index = 0
    while index < len(words):
        word = words[index]
        if word == "outage":
            clause = words[index + 1 : index + 4]
            if len(clause) != 3 or clause[1] != "every":
                raise ValueError("outage needs DURATION every PERIOD, such as outage 1s every 5s")
            if outage is not None:
                raise ValueError("more than one outage")
            outage = Outage(seconds(clause[0]), seconds(clause[2]))
            if not 0 < outage.duration < outage.period:
                raise ValueError("an outage must be shorter than its period")
            index += 4
            continue
        if word in directions:
            raise ValueError(f"more than one {word} section")
        if word in SECTIONS:
            target = directions[word] = []
        else:
            target.append(word)
        index += 1
    for name, section in directions.items():
        if not section:
            raise ValueError(f"empty {name} section")
    return Profile(
        up=(*shared, *directions.get("up", [])),
        down=(*shared, *directions.get("down", [])),
        split=bool(directions),
        outage=outage,
    )


def profile(value: str) -> str:
    """Parses one --netem profile as an argparse type, keeping it as text: the text names the
    profile in results."""
    try:
        words = shlex.split(value)
        if not words:
            raise ValueError("empty netem profile")
        normalized = shlex.join(words)
        _ = parse(normalized)
    except ValueError as error:
        message = f"cannot parse netem profile {value!r}: {error}"
        raise argparse.ArgumentTypeError(message) from error
    return normalized


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
    """Brings up the namespace's loopback link with an Ethernet MTU, the segmenting tbf, and the
    prio qdisc whose bands hold each direction's netem."""
    return [
        ["ip", "link", "set", "lo", "mtu", str(MTU), "up"],
        [
            "tc", "qdisc", "add", "dev", "lo", "root", "handle", "1:",
            "tbf", "rate", "100gbit", "burst", str(FRAME_BYTES), "limit", "100mb",
        ],
        [
            "tc", "qdisc", "add", "dev", "lo", "parent", "1:1", "handle", PRIO,
            "prio", "bands", "2", "priomap", *["0"] * 16,
        ],
    ]  # fmt: skip


def netem_commands(netem: Profile) -> list[list[str]]:
    """Sets each band's netem to the profile's arguments for its direction, adding it on first
    use."""
    return [
        [
            "tc", "qdisc", "replace", "dev", "lo", "parent", band, "handle", handle,
            "netem", "limit", str(NETEM_LIMIT), *arguments,
        ]
        for band, handle, arguments in (
            (UP_BAND, UP_NETEM, netem.up),
            (DOWN_BAND, DOWN_NETEM, netem.down),
        )
    ]  # fmt: skip


def direction_command(port: int) -> list[str]:
    """Sends what a server sends from `port`, over TCP or UDP, to the downstream band."""
    return [
        "tc", "filter", "add", "dev", "lo", "parent", PRIO, "protocol", "ip",
        "prio", DIRECTION_PRIORITY, "u32", "match", "ip", "sport", str(port), "0xffff",
        "flowid", DOWN_BAND,
    ]  # fmt: skip


def delete_filters_command(priority: str) -> list[str]:
    return ["tc", "filter", "del", "dev", "lo", "parent", PRIO, "prio", priority]


OUTAGE_START = [
    "tc", "filter", "add", "dev", "lo", "parent", PRIO, "protocol", "all",
    "prio", OUTAGE_PRIORITY, "matchall", "action", "drop",
]  # fmt: skip
OUTAGE_END = delete_filters_command(OUTAGE_PRIORITY)


def run_tc(command: list[str]) -> None:
    try:
        completed = subprocess.run(command, capture_output=True, text=True, check=False)
    except OSError as error:
        raise RunnerError(f"cannot run {command[0]} (from iproute2): {error}") from error
    if completed.returncode != 0:
        raise RunnerError(f"{shlex.join(command)} failed: {completed.stderr.strip()}")


class Link:
    """The namespace's emulated link, which the runner sets up once, points at each server's
    ports, and switches between profiles."""

    def __init__(
        self,
        run: Callable[[list[str]], None] = run_tc,
        reserve: Callable[[list[int]], None] | None = None,
    ) -> None:
        self._run = run
        self._reserve = reserve if reserve is not None else _reserve_ports
        self._ports: list[int] = []
        self._split = False
        self._filtered = False

    def configure(self, profiles: Sequence[str]) -> None:
        """Sets up the link and checks that tc accepts every profile, so a bad one fails before
        any benchmark runs. The last profile stays applied.

        The filters a profile needs are tried too, by adding and deleting each kind once: a
        namespace that cannot load the classifier or action they use would otherwise fail only
        once a server is up, or once a cell's first outage starts."""
        parsed = [parse(netem) for netem in profiles]
        for command in link_commands():
            self._run(command)
        if any(netem.split for netem in parsed):
            self._reserve([])
            self._run(direction_command(PROBE_PORT))
            self._run(delete_filters_command(DIRECTION_PRIORITY))
        if any(netem.outage is not None for netem in parsed):
            self._run(OUTAGE_START)
            self._run(OUTAGE_END)
        for netem in profiles:
            self.apply(netem)

    def serve(self, ports: Iterable[int]) -> None:
        """Points the direction filters at a newly started server's ports."""
        self._ports = sorted(set(ports))
        self._reserve(self._ports)
        self._filter()

    def apply(self, netem: str) -> None:
        """Switches to a profile."""
        parsed = parse(netem)
        for command in netem_commands(parsed):
            self._run(command)
        self._split = parsed.split
        self._filter()

    def _filter(self) -> None:
        if self._filtered:
            self._run(delete_filters_command(DIRECTION_PRIORITY))
            self._filtered = False
        if self._split and self._ports:
            for port in self._ports:
                self._run(direction_command(port))
            self._filtered = True

    @contextmanager
    def outages(self, netem: str, until: float) -> Generator[None]:
        """Runs the profile's outages, if any, while the block runs: the block starts the client,
        and `until` is when, after that, its measured window ends."""
        outage = parse(netem).outage
        if outage is None:
            yield
            return
        stop = threading.Event()
        errors: list[RunnerError] = []

        def run() -> None:
            began = time.monotonic()
            try:
                for start, end in outage.windows(until):
                    if stop.wait(began + start - time.monotonic()):
                        return
                    self._run(OUTAGE_START)
                    try:
                        _ = stop.wait(began + end - time.monotonic())
                    finally:
                        self._run(OUTAGE_END)
                    if stop.is_set():
                        return
            except RunnerError as error:
                errors.append(error)

        thread = threading.Thread(target=run, name="netem-outages", daemon=True)
        thread.start()
        try:
            yield
        finally:
            stop.set()
            thread.join()
        if errors:
            raise errors[0]


def _reserve_ports(ports: list[int]) -> None:
    try:
        _ = RESERVED_PORTS.write_text(",".join(map(str, ports)) + "\n", encoding="ascii")
    except OSError as error:
        message = f"cannot reserve the servers' ports in {RESERVED_PORTS}: {error}"
        raise RunnerError(message) from error
