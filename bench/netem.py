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
adds a filter ahead of those that drops every packet. A profile with pauses puts a plug qdisc under
each netem, which holds every packet while it is blocked and lets them go when released.

A profile is netem's arguments, as on a tc command line, optionally followed by sections:

    NETEM_ARGS [up NETEM_ARGS] [down NETEM_ARGS] [outage DURATION every PERIOD]
               [pause DURATION every PERIOD] [episode DURATION every PERIOD NETEM_ARGS]
               [trace NAME [from OFFSET]]

`up` (client to server) and `down` (server to client) add arguments to the shared ones for that
direction only; netem takes the last of a repeated option, so they can also override one. The
timed sections start half a period after the client does: `outage` drops every packet, `pause`
holds them, and `episode` adds its arguments to both directions' for its duration, by changing the
netems in place, which keeps the packets they hold. `trace` steps each direction's rate through a
recorded trace of a link's capacity, NAME.up and NAME.down in TRACES_VARIABLE's directory, in the
format of Mahimahi's traces, from OFFSET into it (its start by default) when the client starts.
"""

from __future__ import annotations

import argparse
import itertools
import math
import os
import re
import shlex
import subprocess
import sys
import threading
import time
from collections.abc import Callable, Generator, Iterable, Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, replace
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
# The prio qdisc under the tbf, its bands, the netem in each, and the plug under each netem.
PRIO = "10:"
UP_BAND, DOWN_BAND = "10:1", "10:2"
UP_NETEM, DOWN_NETEM = "20:", "30:"
UP_PLUG, DOWN_PLUG = "21:", "31:"
# What a plug may hold while blocked, in bytes: far more than any profile's queue.
PLUG_LIMIT = 100_000_000
# The directory of the traces a profile's `trace` names.
TRACES_VARIABLE = "TREVRPC_BENCH_TRACES"
# How often a trace's rate changes, in seconds, and the floor of its rates in bits per second.
# netem times a packet when it arrives, at the rate of that moment, so a packet that arrives while
# the trace's rate is near zero would be held long after it rises; at the floor an MTU-sized
# packet waits at most one step.
TRACE_STEP = 0.1
TRACE_FLOOR = math.ceil(MTU * 8 / TRACE_STEP)
# What a trace's delivery opportunity carries, in bytes, as in Mahimahi.
TRACE_OPPORTUNITY = 1500
# Filter priorities on the prio qdisc: the outage's drop-everything filter comes first.
OUTAGE_PRIORITY, DIRECTION_PRIORITY = "1", "2"
# The port of the direction filter configure adds and deletes to check that tc can add one.
PROBE_PORT = 1
# Ports the kernel never picks for a client's socket, so no client sends from a server's port.
RESERVED_PORTS = Path("/proc/sys/net/ipv4/ip_local_reserved_ports")
SECTIONS = ("up", "down", "outage", "pause", "episode", "trace")
TIMED_SECTIONS = ("outage", "pause", "episode")
# Options netem sends as attributes of their own, which `tc qdisc change` keeps when the new
# arguments leave them out: a rate, slots (with their own distribution), a delay distribution
# table, a loss model (gemodel or state) and ECN marking. An episode may only change those its
# profile already sets, so that its end restores them.
STICKY_OPTIONS = ("rate", "slot", "delay distribution", "gemodel", "state", "ecn")
# The netem options a `distribution` can follow, which it belongs to: `delay`'s distribution is
# a table of its own, `slot`'s part of the slot.
NETEM_OPTIONS = ("delay", "slot", "loss", "corrupt", "duplicate", "reorder", "rate", "limit", "ecn")


def sticky_options(arguments: Sequence[str]) -> set[str]:
    """The STICKY_OPTIONS that netem arguments set."""
    found: set[str] = set()
    owner: str | None = None
    for word in arguments:
        if word in NETEM_OPTIONS:
            owner = word
        if word == "distribution" and owner == "delay":
            found.add("delay distribution")
        elif word in STICKY_OPTIONS:
            found.add(word)
    return found


DURATION = re.compile(r"(\d+(?:\.\d+)?)(ms|s)")
# A time as tc takes it, in microseconds without a unit.
TC_TIME = re.compile(r"(\d+(?:\.\d+)?)(s|sec|secs|ms|msec|msecs|us|usec|usecs)?")
TC_TIME_UNITS = {
    **dict.fromkeys(("s", "sec", "secs"), 1.0),
    **dict.fromkeys(("ms", "msec", "msecs"), 1e-3),
    **dict.fromkeys(("us", "usec", "usecs"), 1e-6),
}


@dataclass(frozen=True)
class Outage:
    """Something that happens to the link for `duration` seconds out of every `period`, the first
    starting half a period after the client does: an outage, a pause or an episode."""

    duration: float
    period: float

    def windows(self, until: float) -> Iterator[tuple[float, float]]:
        """The times it happens, as (start, end) in seconds after the client starts, that end by
        `until`: the end of the client's measured window, after which it would only cut short the
        calls the window counts."""
        start = self.period / 2
        while start + self.duration <= until:
            yield start, start + self.duration
            start += self.period


@dataclass(frozen=True)
class Profile:
    """A parsed profile: netem's arguments for each direction, and what happens to the link over
    time, if anything."""

    up: tuple[str, ...]
    down: tuple[str, ...]
    # Whether each direction has its own queue; see the module docstring.
    split: bool
    outage: Outage | None = None
    pause: Outage | None = None
    episode: Outage | None = None
    # What an episode adds to both directions' arguments.
    episode_arguments: tuple[str, ...] = ()
    trace: str | None = None
    # Where in the trace the client starts, in seconds.
    trace_offset: float = 0.0

    @property
    def interrupted(self) -> bool:
        """Whether the link stops carrying packets at times: outages and pauses, after which
        the runner measures how long clients take to recover."""
        return self.outage is not None or self.pause is not None


def round_trip(value: str) -> float | None:
    """The profile's round trip in seconds, from the delay of each direction (netem takes the last
    of a repeated option), leaving out jitter, slots and queueing; None without any delay."""
    netem = parse(value)
    total = 0.0
    found = False
    for arguments in (netem.up, netem.down):
        delays = [arguments[i + 1] for i, word in enumerate(arguments[:-1]) if word == "delay"]
        if delays:
            match = TC_TIME.fullmatch(delays[-1])
            if match is None:
                raise ValueError(f"not a tc time such as 10ms: {delays[-1]!r}")
            total += float(match[1]) * TC_TIME_UNITS[match[2] or "us"]
            found = True
    return total if found else None


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
    sections: dict[str, list[str]] = {}
    timed: dict[str, Outage] = {}
    trace: str | None = None
    trace_offset = 0.0
    target = shared
    # Where words went before an episode: a section after the episode's arguments ends them.
    before_episode = shared
    index = 0
    while index < len(words):
        word = words[index]
        if word in sections or word in timed or (word == "trace" and trace is not None):
            raise ValueError(f"more than one {word} section")
        if word in TIMED_SECTIONS:
            clause = words[index + 1 : index + 4]
            if len(clause) != 3 or clause[1] != "every":
                raise ValueError(f"{word} needs DURATION every PERIOD, such as {word} 1s every 5s")
            timed[word] = Outage(seconds(clause[0]), seconds(clause[2]))
            if not 0 < timed[word].duration < timed[word].period:
                raise ValueError(f"{word} must be shorter than its period")
            index += 4
            if word == "episode":
                before_episode = target
                target = sections["episode"] = []
            elif target is sections.get("episode"):
                target = before_episode
            continue
        if word == "trace":
            if index + 1 >= len(words) or words[index + 1] in SECTIONS:
                raise ValueError("trace needs the NAME of a trace")
            if target is sections.get("episode"):
                target = before_episode
            trace = words[index + 1]
            if "/" in trace or trace.startswith("."):
                raise ValueError(f"a trace is named, not a path: {trace!r}")
            index += 2
            if index < len(words) and words[index] == "from":
                if index + 1 >= len(words):
                    raise ValueError("trace NAME from needs an OFFSET, such as from 157s")
                trace_offset = seconds(words[index + 1])
                index += 2
            continue
        if word in SECTIONS:
            target = sections[word] = []
        else:
            target.append(word)
        index += 1
    for name, section in sections.items():
        if not section:
            raise ValueError(f"empty {name} section")
    up = (*shared, *sections.get("up", []))
    down = (*shared, *sections.get("down", []))
    episode_arguments = tuple(sections.get("episode", []))
    unset = sticky_options(episode_arguments) - (sticky_options(up) & sticky_options(down))
    if unset:
        option = min(unset)
        raise ValueError(f"an episode may only change {option} where the profile sets it")
    if trace is not None and episode_arguments:
        raise ValueError("a trace and an episode would both change the netems")
    return Profile(
        up=up,
        down=down,
        # A trace has a rate for each direction, so it splits them.
        split="up" in sections or "down" in sections or trace is not None,
        outage=timed.get("outage"),
        pause=timed.get("pause"),
        episode=timed.get("episode"),
        episode_arguments=episode_arguments,
        trace=trace,
        trace_offset=trace_offset,
    )


class Trace:
    """A recorded trace of a link's capacity, as Mahimahi writes them: each line is the
    millisecond, from the trace's start, of an opportunity to carry TRACE_OPPORTUNITY bytes,
    repeated for several in one millisecond, and the trace repeats every PERIOD milliseconds, its
    last line's: the opportunity at millisecond t of cycle c comes at c * PERIOD + t. A trace of
    one line, 1, carries one opportunity every millisecond."""

    def __init__(self, path: Path) -> None:
        try:
            times = [int(line) for line in path.read_text(encoding="ascii").split()]
        except (OSError, ValueError) as error:
            raise RunnerError(f"cannot read the trace {path}: {error}") from error
        if not times or times != sorted(times) or times[0] < 0 or times[-1] == 0:
            raise RunnerError(f"the trace {path} is not a list of rising milliseconds")
        self.period = times[-1]
        # before[m] is how many opportunities a cycle has up to its millisecond m.
        counts = [0] * (self.period + 1)
        for at in times:
            counts[at] += 1
        self.before = list(itertools.accumulate(counts))

    def opportunities(self, first: int, last: int) -> int:
        """How many opportunities the repeating trace has from millisecond `first` to `last` of
        its start, both included."""
        return self._through(last) - self._through(first - 1)

    def _through(self, millisecond: int) -> int:
        if millisecond < 0:
            return 0
        cycles, rest = divmod(millisecond, self.period)
        return cycles * self.before[-1] + self.before[rest]

    def rate(self, step: int) -> int:
        """The rate, in bits per second, of the trace's `step`th TRACE_STEP from its start, no
        lower than TRACE_FLOOR."""
        width = round(TRACE_STEP * 1000)
        count = self.opportunities(step * width, (step + 1) * width - 1)
        return max(count * TRACE_OPPORTUNITY * 8 * 1000 // width, TRACE_FLOOR)


def trace_path(name: str, direction: str) -> Path:
    directory = os.environ.get(TRACES_VARIABLE)
    if not directory:
        raise RunnerError(f"a profile names the trace {name!r}, but {TRACES_VARIABLE} is not set")
    return Path(directory) / f"{name}.{direction}"


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
    """Brings up the namespace's loopback link with an Ethernet MTU and the segmenting tbf."""
    return [
        ["ip", "link", "set", "lo", "mtu", str(MTU), "up"],
        [
            "tc", "qdisc", "add", "dev", "lo", "root", "handle", "1:",
            "tbf", "rate", "100gbit", "burst", str(FRAME_BYTES), "limit", "100mb",
        ],
    ]  # fmt: skip


def netem_command(verb: str, direction: str, arguments: Sequence[str]) -> list[str]:
    """Adds or changes the netem of a direction, `up` or `down`, with these arguments."""
    band, handle = (UP_BAND, UP_NETEM) if direction == "up" else (DOWN_BAND, DOWN_NETEM)
    return [
        "tc", "qdisc", verb, "dev", "lo", "parent", band, "handle", handle,
        "netem", "limit", str(NETEM_LIMIT), *arguments,
    ]  # fmt: skip


def plug_command(direction: str, action: str) -> list[str]:
    """Blocks (`block`) or releases (`release_indefinite`) the plug under a direction's netem."""
    handle = UP_PLUG if direction == "up" else DOWN_PLUG
    return ["tc", "qdisc", "change", "dev", "lo", "handle", handle, "plug", action]


def tree_commands(netem: Profile, ports: Iterable[int]) -> list[list[str]]:
    """Builds the profile's qdiscs under the tbf: the prio qdisc, each band's netem with the
    profile's arguments for its direction, a released plug under each netem if the profile
    pauses, and, if the profile splits the directions, a filter for each server port."""
    prio = [
        "tc", "qdisc", "add", "dev", "lo", "parent", "1:1", "handle", PRIO,
        "prio", "bands", "2", "priomap", *["0"] * 16,
    ]  # fmt: skip
    netems = [netem_command("add", "up", netem.up), netem_command("add", "down", netem.down)]
    plugs: list[list[str]] = []
    if netem.pause is not None:
        for direction, parent, handle in (
            ("up", UP_NETEM, UP_PLUG),
            ("down", DOWN_NETEM, DOWN_PLUG),
        ):
            plugs += [
                [
                    "tc", "qdisc", "add", "dev", "lo", "parent", parent.removesuffix(":") + ":1",
                    "handle", handle, "plug", "limit", str(PLUG_LIMIT),
                ],
                plug_command(direction, "release_indefinite"),
            ]  # fmt: skip
    filters = [direction_command(port) for port in ports] if netem.split else []
    return [prio, *netems, *plugs, *filters]


# Removes the prio qdisc with its netems and filters. A profile's qdiscs are built anew rather
# than changed, since changing a netem keeps every option the new arguments leave out: a rate
# or a slot would carry over to a profile that has none.
REMOVE_TREE = ["tc", "qdisc", "del", "dev", "lo", "parent", "1:1", "handle", PRIO]


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
PAUSE_START = [plug_command("up", "block"), plug_command("down", "block")]
PAUSE_END = [plug_command("up", "release_indefinite"), plug_command("down", "release_indefinite")]


@dataclass(frozen=True)
class Change:
    """Commands that change the link `at` seconds after the client starts. A change that starts
    an interruption (an outage or a pause) has the `ending` that ends it, which also runs when
    the client exits first, and which `ends` the interruption of the same key."""

    at: float
    commands: tuple[tuple[str, ...], ...]
    starts: int | None = None
    ending: tuple[tuple[str, ...], ...] = ()
    ends: int | None = None
    # Whether what it starts or ends is an interruption, whose times the runner keeps.
    interruption: bool = False


def changes(netem: Profile, until: float) -> list[Change]:
    """What happens to the link while a client runs, in order: each outage, pause and episode
    that ends by `until`, and every step of the trace, from the client's start to `until`. At
    the same instant, endings come first."""
    found: list[Change] = []
    key = 0

    def timed(
        outage: Outage | None, start: list[list[str]], end: list[list[str]], interruption: bool
    ) -> None:
        nonlocal key
        if outage is None:
            return
        begin = tuple(tuple(command) for command in start)
        finish = tuple(tuple(command) for command in end)
        for down, up in outage.windows(until):
            found.append(Change(down, begin, starts=key, ending=finish, interruption=interruption))
            found.append(Change(up, finish, ends=key, interruption=interruption))
            key += 1

    timed(netem.outage, [OUTAGE_START], [OUTAGE_END], True)
    timed(netem.pause, PAUSE_START, PAUSE_END, True)
    timed(
        netem.episode,
        [
            netem_command("change", "up", (*netem.up, *netem.episode_arguments)),
            netem_command("change", "down", (*netem.down, *netem.episode_arguments)),
        ],
        [netem_command("change", "up", netem.up), netem_command("change", "down", netem.down)],
        False,
    )
    if netem.trace is not None:
        up = Trace(trace_path(netem.trace, "up"))
        down = Trace(trace_path(netem.trace, "down"))
        first = round(netem.trace_offset / TRACE_STEP)
        for index in range(math.ceil(until / TRACE_STEP)):
            step = first + index
            commands = (
                tuple(netem_command("change", "up", (*netem.up, "rate", f"{up.rate(step)}bit"))),
                tuple(
                    netem_command("change", "down", (*netem.down, "rate", f"{down.rate(step)}bit"))
                ),
            )
            found.append(Change(index * TRACE_STEP, commands))
    return sorted(found, key=lambda change: (change.at, change.ends is None))


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
        # The profile whose qdiscs are built, if any.
        self._profile: str | None = None

    def configure(self, profiles: Sequence[str]) -> None:
        """Sets up the link and checks that tc accepts every profile, so a bad one fails before
        any benchmark runs. The last profile stays applied.

        The filters a profile needs are tried too, by adding and deleting each kind once, and so
        are the changes it makes in place: a namespace that cannot load the classifier, action or
        qdisc they use, or a trace that cannot be read, would otherwise fail only once a server
        is up, or once a cell's first outage starts."""
        parsed = [parse(netem) for netem in profiles]
        for command in link_commands():
            self._run(command)
        for text, netem in zip(profiles, parsed, strict=True):
            self.apply(text)
            # The changes a profile makes in place, once each: blocking and releasing its plugs,
            # an episode and its end, and its trace's first step.
            probes: list[Change] = []
            if netem.pause is not None:
                probes += [Change(0, tuple(map(tuple, [*PAUSE_START, *PAUSE_END])))]
            if netem.episode is not None:
                window = Outage(netem.episode.duration, 2 * netem.episode.duration)
                probes += changes(
                    replace(netem, episode=window, pause=None, outage=None, trace=None),
                    2 * netem.episode.duration,
                )
            if netem.trace is not None:
                probes += changes(replace(netem, episode=None, pause=None, outage=None), TRACE_STEP)
            for probe in probes:
                for command in probe.commands:
                    self._run(list(command))
        # No server has started, so the last profile's qdiscs have no direction filter yet.
        if any(netem.split for netem in parsed):
            self._reserve([])
            self._run(direction_command(PROBE_PORT))
            self._run(delete_filters_command(DIRECTION_PRIORITY))
        if any(netem.outage is not None for netem in parsed):
            self._run(OUTAGE_START)
            self._run(OUTAGE_END)

    def serve(self, ports: Iterable[int]) -> None:
        """Points the direction filters at a newly started server's ports."""
        self._ports = sorted(set(ports))
        self._reserve(self._ports)
        if self._profile is not None:
            self.apply(self._profile)

    def apply(self, netem: str) -> None:
        """Switches to a profile, building its qdiscs anew."""
        parsed = parse(netem)
        if self._profile is not None:
            self._run(REMOVE_TREE)
        self._profile = None
        for command in tree_commands(parsed, self._ports):
            self._run(command)
        self._profile = netem

    @contextmanager
    def impairments(self, netem: str, until: float) -> Generator[list[tuple[float, float]]]:
        """Makes the profile's changes to the link while the block runs: its outages, pauses,
        episodes and trace. The block starts the client, and `until` is when, after that, its
        measured window ends. The block gets a list that holds each outage and pause as it ends,
        as (start, end) by the wall clock, in seconds since the Unix epoch, which the clients'
        timelines use too. Whatever started and has not ended when the block does ends then."""
        planned = changes(parse(netem), until)
        times: list[tuple[float, float]] = []
        if not planned:
            yield times
            return
        stop = threading.Event()
        errors: list[RunnerError] = []

        def run() -> None:
            began = time.monotonic()
            # What has started and not yet ended: its ending, and when it started.
            active: dict[int, tuple[Change, float]] = {}
            try:
                for change in planned:
                    if stop.wait(began + change.at - time.monotonic()):
                        break
                    for command in change.commands:
                        self._run(list(command))
                    if change.starts is not None:
                        active[change.starts] = (change, time.time())
                    elif change.ends is not None:
                        started, at = active.pop(change.ends)
                        if started.interruption:
                            times.append((at, time.time()))
            except RunnerError as error:
                errors.append(error)
            finally:
                for started, at in active.values():
                    try:
                        for command in started.ending:
                            self._run(list(command))
                    except RunnerError as error:
                        errors.append(error)
                    if started.interruption:
                        times.append((at, time.time()))

        thread = threading.Thread(target=run, name="netem-changes", daemon=True)
        thread.start()
        try:
            yield times
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
