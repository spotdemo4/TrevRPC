"""The cells each table of the wiki's Benchmarks page reads, so that `--table NAME` runs only those.

A table names its options, which become the defaults of the command line (explicit flags still
win), and a filter over the cells those options span. Tables refer to the packaged servers and
clients by name: `go` and `rust` run against their own server only, and the baseline's clients are
`grpc` and `connect`. Browsers are clients named after themselves, such as `chromium`, whose
TrevRPC lanes run against every TrevRPC server and whose fetch lanes run against `connect`.
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
    netem: str | None


@dataclass(frozen=True)
class Table:
    description: str
    # argparse destinations and their values, applied as the command line's defaults.
    options: dict[str, object] = field(default_factory=lambda: dict[str, object]())
    keep: Callable[[Cell], bool] = lambda _cell: True


def own_server(cell: Cell) -> bool:
    """TrevRPC clients against their own runtime's server; the baselines always are."""
    return cell.client == cell.server


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


def browser(cell: Cell) -> bool:
    """Chromium's lanes, and for reference the native TrevRPC clients over QUIC and connect-go's
    client, each against its own server."""
    if cell.client == "chromium":
        return True
    return own_server(cell) and (cell.transport == "quic" or cell.client == "connect")


ALL_COMPRESSIONS = ["none", "gzip", "zstd", "dictionary", "stream", "stream-dictionary"]

# Loopback cells settle well inside half a second. Only operations that start and end inside the
# window count, which undercounts by about an operation's length over the window: 3% for the
# slowest loopback cells (80 ms calls) at 3 s. Runs of 2 s read those 5-6% low, and runs of 1 s
# were noisy; 5 s measured the same as 3 s within the runs' spread.
LOOPBACK = {"duration": 3.0, "warmup": 0.5}

# Real links lose packets in bursts, not one at a time: Wi-Fi and cellular retransmit below IP, so
# their losses show up as delay, and what does get dropped goes in runs. netem's Gilbert-Elliott
# model alternates between a good state that loses nothing and a bad one, entered with
# probability p per packet and left with probability r, that loses 1-h of its packets. These
# lose about 1% (p 0.5%, r 25%, half the packets of 4-packet bad runs) and about 3% (p 1%,
# r 20%, 60% of 5-packet bad runs). The random 1% profile stays for comparison.
GILBERT_ELLIOTT = [
    "delay 10ms loss gemodel 0.5% 25% 50% 0%",
    "delay 10ms loss gemodel 1% 20% 60% 0%",
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
            "scenarios": [
                "unary",
                "bidi",
                "server-stream",
                "client-stream",
                "paced",
                "unreliable-paced",
            ],
            "sizes": [64, 1024],
            "concurrency": [64],
            "messages": 10,
            # 64 workers at 25 messages of 64 bytes per second. One connection on these links
            # carries roughly 1 MB/s, and a load near that queues until every echo is late:
            # 25 messages of 1 KiB per worker lost nearly all of them, even over TCP, and 100 of
            # 64 bytes already doubled the round trips.
            "rates": [25],
            "duration": 10.0,
            "netem": [
                "delay 10ms",
                "delay 10ms loss 1%",
                *GILBERT_ELLIOTT,
            ],
        },
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
