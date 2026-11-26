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
    "duration": 10.0,
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
ACCESS = [
    # Wi-Fi: one medium, so one queue for both directions, at 100 Mbit/s. Pareto slots add 2 ms
    # to the round trip at the median and 19 ms at p90, as Sui et al. (MobiSys 2016) measured on
    # a campus network, but only 36 ms at p99, where they saw over 250 ms: their tail comes in
    # episodes of minutes, which netem's independent slots do not model.
    "delay 10ms rate 100mbit limit 1000 slot distribution pareto 1ms 8ms",
    # The same Wi-Fi, roaming between access points every 5 s: 60-400 ms without 802.11r
    # (Shin et al., MobiSys 2004), the longest here.
    "delay 10ms rate 100mbit limit 1000 slot distribution pareto 1ms 8ms outage 400ms every 5s",
    # LTE: a 60 ms round trip, 20 Mbit/s down and 5 up (Huang et al., SIGCOMM 2013; Ghoshal et
    # al., IMC 2023), each direction behind about a second of queue (Jiang et al., CellNet 2012),
    # and the 0.06% that TCP retransmits on it.
    "delay 30ms loss 0.05% up rate 5mbit limit 450 down rate 20mbit limit 1700",
    # 5G NSA on the move: a 70 ms round trip, 40 Mbit/s down and 12 up (Ghoshal et al.), half a
    # second of queue, and a 170 ms handover every 5 s (167 ms on average, Hassan et al.,
    # SIGCOMM 2022), far more often than on a freeway, so that each run sees two.
    "delay 35ms up rate 12mbit limit 500 down rate 40mbit limit 1700 outage 170ms every 5s",
]

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
    "access": Table(
        "Access networks",
        # Deep queues take seconds to fill and settle, and outages every 5 s need several per
        # window: in 10 s windows, cells swung by up to a quarter between runs.
        {**IMPAIRED, "duration": 20.0, "warmup": 3.0, "netem": ACCESS},
        lossy,
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
            "duration": 10.0,
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
