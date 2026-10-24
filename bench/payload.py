"""The `records` payload of the load benchmarks: realistic, compressible message text in which
every message differs; see README.md. The Go, Rust and baseline clients implement the same
generator byte for byte, and check it against GOLDEN_SHA256.

A generator is an xorshift64 state. Each message is log records, one JSON object per line,
appended until the message reaches its size and then cut to exactly that size; the rest of the
last record is discarded, and the next message starts a new record.
"""

from __future__ import annotations

import hashlib

# The seed of worker i is WORKER_SEED + i.
WORKER_SEED = 0x9E3779B97F4A7C15
# The seed of the dictionary's training samples.
TRAINING_SEED = 0xD1B54A32D192ED03
TRAINING_SIZES = (64, 256, 1024, 4096)
TRAINING_MESSAGES = 2000
# SHA-256 of the messages of sizes 64, 256, 1024 and 4096, in that order, from WORKER_SEED.
GOLDEN_SHA256 = "1414f2e3835a891eecc8579d6736157e456eee7f4f3547b5a326b48f625a8a98"

MASK = (1 << 64) - 1
SERVICES = ("checkout", "cart", "payments", "inventory", "search", "auth", "gateway", "notifier")
REGIONS = ("us-east-1", "us-west-2", "eu-west-1", "eu-central-1", "ap-southeast-1")
METHODS = ("GET", "POST", "PUT", "DELETE")
ROUTES = (
    "/api/v1/orders",
    "/api/v1/orders/{id}",
    "/api/v1/cart",
    "/api/v1/users/{id}",
    "/api/v1/search",
    "/healthz",
)
STATUSES = (200, 200, 200, 200, 200, 201, 204, 400, 404, 500)
HEX = "0123456789abcdef"
BASE_TIMESTAMP_MS = 1_790_000_000_000


class Records:
    """A generator of `records` messages."""

    def __init__(self, seed: int) -> None:
        self.state: int = seed & MASK
        self.count: int = 0

    def next(self) -> int:
        x = self.state
        x ^= (x << 13) & MASK
        x ^= x >> 7
        x ^= (x << 17) & MASK
        self.state = x
        return x

    def pick(self, values: tuple[object, ...]) -> object:
        return values[self.next() % len(values)]

    def between(self, low: int, high: int) -> int:
        return low + self.next() % (high - low + 1)

    def record(self) -> str:
        timestamp = BASE_TIMESTAMP_MS + self.count * 13 + self.between(0, 12)
        self.count += 1
        service = self.pick(SERVICES)
        region = self.pick(REGIONS)
        method = self.pick(METHODS)
        route = self.pick(ROUTES)
        status = self.pick(STATUSES)
        latency = self.between(1, 2000)
        trace = "".join(HEX[self.next() % 16] for _ in range(32))
        user = self.between(1, 1_000_000)
        return (
            f'{{"ts":{timestamp},"service":"{service}","region":"{region}",'
            + f'"method":"{method}","route":"{route}","status":{status},'
            + f'"latency_ms":{latency},"trace":"{trace}","user":{user}}}\n'
        )

    def message(self, size: int) -> bytes:
        parts: list[str] = []
        length = 0
        while length < size:
            record = self.record()
            parts.append(record)
            length += len(record)
        return "".join(parts).encode("ascii")[:size]


def golden_digest() -> str:
    generator = Records(WORKER_SEED)
    digest = hashlib.sha256()
    for size in TRAINING_SIZES:
        digest.update(generator.message(size))
    return digest.hexdigest()


def training_samples() -> list[bytes]:
    """The messages the benchmark's dictionary is trained on: TRAINING_MESSAGES of the training
    sizes in turn, from TRAINING_SEED, so they never repeat a worker's messages."""
    generator = Records(TRAINING_SEED)
    return [
        generator.message(TRAINING_SIZES[index % len(TRAINING_SIZES)])
        for index in range(TRAINING_MESSAGES)
    ]
