"""The cells each table of the wiki's Benchmarks page reads, so that `--table NAME` runs only those.

A table names its options, which become the defaults of the command line (explicit flags still
win), and a filter over the cells those options span. Tables refer to the packaged servers and
clients by name: `go` and `rust` run against their own server only, `python`, which has no server,
against the Go server, and the baseline's clients are `grpc` and `connect`. Browsers are clients
named after themselves, such as `chromium`, whose TrevRPC lanes run against every TrevRPC server
and whose fetch lanes run against `connect`. The Erlang runtime, which the packaged runner leaves
out, is `erlang` when named on the command line.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field


@dataclass(frozen=True)
class Cell:
    """One benchmark run's coordinates."""

    client: str
    server: str
    transport: str
    scenario: str
    compression: str
    size: int
    metadata_size: int
    # Where the metadata goes: "call" (every call) or "connection" (connection metadata).
    metadata_scope: str
    concurrency: int
    # How many channels the workers spread over; TrevRPC clients only.
    connections: int
    # Messages per second per worker, for the paced scenarios only.
    rate: int | None
    # The rules of the requests, for the validation scenarios only: none, standard or cel.
    validation: str | None
    netem: str | None


@dataclass(frozen=True)
class Table:
    description: str
    # argparse destinations and their values, applied as the command line's defaults.
    options: dict[str, object] = field(default_factory=lambda: dict[str, object]())
    keep: Callable[[Cell], bool] = lambda _cell: True

    def runs(self, cell: Cell) -> bool:
        """Whether the table runs a cell: it reads the cell, and the cell's client runs it."""
        return self.keep(cell) and not refused(cell)


def refused(cell: Cell) -> bool:
    """The cells a client refuses, which no table runs: the Erlang client has no validation
    scenarios yet (see README.md)."""
    return cell.client == "erlang" and cell.validation is not None


# The server a client runs against when its runtime has none.
HOME_SERVERS = {"python": "go"}


def own_server(cell: Cell) -> bool:
    """TrevRPC clients against their own runtime's server, or for a client-only runtime, against
    its home server; the baselines always are."""
    return cell.server == HOME_SERVERS.get(cell.client, cell.client)


def comparison(cell: Cell) -> bool:
    return own_server(cell) and not (cell.size == 4096 and cell.concurrency == 1)


def compression(cell: Cell) -> bool:
    if not own_server(cell):
        return False
    if cell.scenario == "bidi":
        # Wire bytes at every size for Go over QUIC and the baselines; throughput at 1 KiB.
        every_size = cell.client in ("go", "grpc", "connect") and cell.transport != "tcp"
        return every_size or cell.size == 1024
    if cell.size != 1024:
        return False
    if cell.scenario == "unary":
        return cell.client in ("rust", "grpc") and cell.transport != "tcp"
    return cell.client in ("rust", "grpc")


def link(cell: Cell) -> bool:
    if not own_server(cell) or cell.transport == "tcp":
        return False
    if cell.scenario == "bidi":
        return cell.size == 4096 and cell.client in ("rust", "grpc")
    return cell.client in ("rust", "grpc") or (cell.client == "connect" and cell.size == 1024)


def lossy(cell: Cell) -> bool:
    # Real-time traffic is small messages; 1 KiB at a fixed rate would only measure how little a
    # lossy connection carries. Paced streams run over QUIC only: over QMux, unreliable-paced is
    # already a stream.
    paced = cell.scenario.endswith("paced")
    if cell.scenario == "paced" and cell.transport != "quic":
        return False
    return own_server(cell) and (cell.size == 64) == paced


def datagrams(cell: Cell) -> bool:
    if not own_server(cell):
        return False
    if cell.scenario == "unreliable-paced":
        # Datagrams against a QMux stream at the same rates.
        return cell.concurrency == 64
    if cell.scenario == "paced":
        # And against a QUIC stream; over QMux, unreliable-paced is already a stream.
        return cell.concurrency == 64 and cell.transport == "quic"
    if cell.transport != "quic":
        return False
    return cell.scenario.startswith("unreliable-") or cell.concurrency == 1


def validation(cell: Cell) -> bool:
    """Each runtime's client against its own server over QUIC: what validation costs next to the
    transport does not depend on it."""
    return own_server(cell) and cell.transport == "quic"


def browser(cell: Cell) -> bool:
    """Chromium's lanes, and for reference the native TrevRPC clients over QUIC and connect-go's
    client, each against its own server."""
    if cell.client == "chromium":
        return True
    return own_server(cell) and (cell.transport == "quic" or cell.client == "connect")


def browser_compression(cell: Cell) -> bool:
    """Chromium's TrevRPC lanes, and for reference the native TrevRPC clients over QUIC against
    their own server: bidi at every size over WebTransport and QUIC, the rest at 1 KiB."""
    if cell.client == "chromium":
        if cell.transport not in ("webtransport", "websocket"):
            return False
    elif not own_server(cell) or cell.transport != "quic":
        return False
    every_size = cell.scenario == "bidi" and cell.transport in ("webtransport", "quic")
    return every_size or cell.size == 1024


ALL_COMPRESSIONS = ["none", "gzip", "zstd", "dictionary", "stream", "stream-dictionary"]

# Loopback cells settle well inside half a second. Only operations that start and end inside the
# window count, which undercounts by about an operation's length over the window: 3% for the
# slowest loopback cells (80 ms calls) at 3 s. Runs of 2 s read those 5-6% low, and runs of 1 s
# were noisy; 5 s measured the same as 3 s within the runs' spread.
LOOPBACK = {"duration": 3.0, "warmup": 0.5}

# How long the tables of impaired links measure each cell, after how much warmup: deep queues
# take seconds to fill and settle, outages every 5 s need several per window, and burst loss sends
# TCP into backoffs of seconds. In 10 s windows, outage cells swung by up to a quarter between
# runs, and TCP cells at about 3% burst loss by up to 15 times (#740).
IMPAIRED_WINDOW = {"duration": 20.0, "warmup": 3.0}

# What the tables of impaired links run: few messages per call, small paced ones, and long
# windows, which loss and outages need to average out.
IMPAIRED = {
    "scenarios": ["unary", "bidi", "server-stream", "client-stream", "paced", "unreliable-paced"],
    "sizes": [64, 1024],
    "concurrency": [64],
    "messages": 10,
    # 64 workers at 25 messages of 64 bytes per second. One connection on these links carries
    # roughly 1 MB/s, and a load near that queues until every echo is late: 25 messages of
    # 1 KiB per worker lost nearly all of them, even over TCP, and 100 of 64 bytes already
    # doubled the round trips.
    "rates": [25],
    **IMPAIRED_WINDOW,
}

# Real links lose packets in bursts, not one at a time: Wi-Fi and cellular retransmit below IP, so
# their losses show up as delay, and what does get dropped goes in runs. netem's Gilbert-Elliott
# model alternates between a good state that loses nothing and a bad one, entered with
# probability p per packet and left with probability r, that loses 1-h of its packets. These
# lose about 1% (p 0.5%, r 25%, half the packets of 4-packet bad runs) and about 3% (p 1%,
# r 20%, 60% of 5-packet bad runs). The access networks below are the realistic case; these
# stress loss recovery.
GILBERT_ELLIOTT = [
    "delay 10ms loss gemodel 0.5% 25% 50% 0%",
    "delay 10ms loss gemodel 1% 20% 60% 0%",
]

# Access networks as their measurements describe them, where what users see is delay, a
# bottleneck with a deep queue, and outages, more than random loss: Wi-Fi and cellular
# retransmit below IP.
#
# Wi-Fi: one medium, so one queue for both directions, at 100 Mbit/s. Sui et al. (MobiSys 2016)
# measured Wi-Fi's share of the round trip on a campus network under its users' own traffic: 3 ms
# at the median, 20 ms at p90 and 250 ms at p99, the tail coming in episodes that last minutes.
# Small Pareto slots give the median, and episodes of large ones, 4 s of every 10 s, the tail:
# with 64 workers of 1 KiB unary calls, Go's calls took 24 ms at the median and 240-275 ms at
# p99 over the 20 ms base round trip, and 33-68 ms at p90, which falls where the episodes' share
# of the calls ends, so it swings from run to run (#741). Slots alone cannot: those that gave
# the median under load capped the tail near 30 ms, and those fitted to single packets added
# 20 ms at the median under load.
WIFI = (
    "delay 10ms rate 100mbit limit 1000 slot distribution pareto 0.2ms 1ms"
    " episode 4s every 10s slot distribution pareto 10ms 60ms"
)
ACCESS = [
    WIFI,
    # The same Wi-Fi, roaming between access points every 5 s: 60-400 ms without 802.11r
    # (Shin et al., MobiSys 2004), the longest here. Roaming drops what was in flight.
    f"{WIFI} outage 400ms every 5s",
    # LTE: a 60 ms round trip, 20 Mbit/s down and 5 up (Huang et al., SIGCOMM 2013; Ghoshal et
    # al., IMC 2023), each direction behind about a second of queue (Jiang et al., CellNet 2012),
    # and the 0.06% that TCP retransmits on it.
    "delay 30ms loss 0.05% up rate 5mbit limit 450 down rate 20mbit limit 1700",
    # The same LTE with the capacity of a phone on the road, which changes from one tenth of a
    # second to the next: Mahimahi's Verizon LTE driving trace (Winstein et al., NSDI 2013; see
    # traces.nix), from 157 s in, the 23 s whose uplink rates match the whole trace's and whose
    # downlink median is within 15% of it (5.5 against 6.4 Mbit/s); the trace's first 23 s run
    # at twice its average.
    "delay 30ms loss 0.05% trace Verizon-LTE-driving from 157s up limit 450 down limit 1700",
    # 5G NSA on the move: a 70 ms round trip, 40 Mbit/s down and 12 up (Ghoshal et al.), half a
    # second of queue, and a 170 ms handover every 5 s (167 ms on average, Hassan et al.,
    # SIGCOMM 2022), far more often than on a freeway, so that each run sees several. The
    # network forwards what the old cell held to the new one (X2/Xn data forwarding), so a
    # handover pauses the link instead of dropping packets.
    "delay 35ms up rate 12mbit limit 500 down rate 40mbit limit 1700 pause 170ms every 5s",
]

# Poor coverage: LTE at its 5th percentile, 0.5 Mbit/s down and 38 kbit/s up over a 467 ms round
# trip (#734's sources), where 64 workers complete nothing, so one and four workers. Cellular
# buffers are sized in bytes, so at these rates they hold seconds: 30 packets up are 9.5 s of
# queue, and 100 down 2.4 s. Paced messages would arrive right at their 500 ms deadline,
# and bidi round trips run as unary calls do, so neither runs.
COVERAGE = "delay 233ms up rate 38kbit limit 30 down rate 500kbit limit 100"

TABLES: dict[str, Table] = {
    "comparison": Table(
        "Comparison with gRPC and Connect",
        {
            **LOOPBACK,
            "scenarios": ["unary", "server-stream", "client-stream", "bidi"],
            "sizes": [64, 4096],
            "concurrency": [1, 64],
        },
        comparison,
    ),
    "compression": Table(
        "Compression over loopback: wire bytes and throughput",
        {
            **LOOPBACK,
            "payload": "records",
            "compressions": ALL_COMPRESSIONS,
            "scenarios": ["unary", "client-stream", "bidi"],
            "sizes": [256, 1024, 4096],
            "concurrency": [64],
        },
        compression,
    ),
    "link": Table(
        "Compression over a 100 Mbit/s link with a 20 ms round trip",
        {
            "payload": "records",
            "compressions": ALL_COMPRESSIONS,
            "scenarios": ["client-stream", "bidi"],
            "sizes": [1024, 4096],
            "concurrency": [64],
            "messages": 20,
            "netem": ["delay 10ms rate 100mbit"],
        },
        link,
    ),
    "lossy": Table(
        "Lossy links",
        {
            **IMPAIRED,
            "netem": [
                "delay 10ms",
                *GILBERT_ELLIOTT,
            ],
        },
        lossy,
    ),
    "access": Table("Access networks", {**IMPAIRED, "netem": ACCESS}, lossy),
    "coverage": Table(
        "Poor coverage",
        {
            **IMPAIRED_WINDOW,
            "scenarios": ["unary", "server-stream", "client-stream"],
            "sizes": [1024],
            "concurrency": [1, 4],
            "messages": 10,
            "netem": [COVERAGE],
        },
        own_server,
    ),
    "browser": Table(
        "Browsers",
        {
            **LOOPBACK,
            "scenarios": ["unary", "server-stream", "client-stream", "bidi"],
            "sizes": [64, 4096],
            "concurrency": [1, 64],
        },
        browser,
    ),
    "browser-compression": Table(
        "Compression in browsers",
        {
            **LOOPBACK,
            "payload": "records",
            "compressions": ["none", "dictionary", "stream", "stream-dictionary"],
            "scenarios": ["unary", "client-stream", "bidi"],
            "sizes": [256, 1024, 4096],
            "concurrency": [64],
        },
        browser_compression,
    ),
    "browser-lossy": Table(
        "Browsers on a lossy link",
        {
            "scenarios": ["unary", "server-stream", "client-stream", "bidi"],
            "sizes": [1024],
            "concurrency": [64],
            "messages": 10,
            **IMPAIRED_WINDOW,
            "netem": [GILBERT_ELLIOTT[1]],
        },
        browser,
    ),
    "metadata": Table(
        "Metadata cost",
        {
            **LOOPBACK,
            "scenarios": ["unary", "bidi"],
            "sizes": [64],
            "metadata_sizes": [0, 1024],
            "metadata_scopes": ["call", "connection"],
            "concurrency": [1, 64],
        },
        own_server,
    ),
    "validation": Table(
        "Request validation",
        {
            **LOOPBACK,
            "scenarios": ["validation-unary", "validation-client-stream"],
            "sizes": [64, 4096],
            "concurrency": [1, 64],
            "validations": ["none", "standard", "cel"],
        },
        validation,
    ),
    "datagrams": Table(
        "Datagram messages",
        {
            **LOOPBACK,
            "scenarios": [
                "unreliable-server-stream",
                "unreliable-client-stream",
                "paced",
                "unreliable-paced",
                "server-stream",
                "client-stream",
            ],
            "sizes": [64],
            "concurrency": [1, 64],
            # 6.4k, 64k and 640k messages per second from 64 workers.
            "rates": [100, 1000, 10000],
        },
        datagrams,
    ),
}
