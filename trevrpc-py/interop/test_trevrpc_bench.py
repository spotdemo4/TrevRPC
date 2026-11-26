"""Unit tests for the load-benchmark client, which the trevrpc-interop-python package runs."""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import unittest
from types import SimpleNamespace

import trevrpc_bench
from gen.interop_pb import EchoRequest, EchoResponse
from trevrpc import Code, TrevRpcError
from trevrpc_bench import (
    BenchError,
    Clock,
    Config,
    OperationLoop,
    RecordsPayload,
    WireSampler,
    Worker,
    paced_message,
    paced_sequence,
    percentile_us,
    summarize,
)

# bench/payload.py's GOLDEN_SHA256: the SHA-256 of the messages of sizes 64, 256, 1024 and 4096,
# in that order, from the seed of worker 0.
GOLDEN_SHA256 = "1414f2e3835a891eecc8579d6736157e456eee7f4f3547b5a326b48f625a8a98"


def config(**fields: object) -> Config:
    values: dict[str, object] = {
        "scenario": "unary",
        "size": 64,
        "concurrency": 1,
        "connections": 1,
        "messages": 10,
        "rate": 100.0,
        "duration": 1.0,
        "warmup": 0.0,
        "metadata_size": 0,
        "metadata_scope": "call",
        "payload": "repeat",
        "compression": "none",
        "validation": "none",
    }
    values.update(fields)
    return Config(**values)  # pyright: ignore[reportArgumentType]


class PayloadTest(unittest.TestCase):
    def test_records_match_the_golden_digest(self) -> None:
        payload = RecordsPayload(trevrpc_bench.RECORDS_WORKER_SEED, 0)
        digest = hashlib.sha256()
        for size in (64, 256, 1024, 4096):
            payload.size = size
            digest.update(payload.next().encode("ascii"))
        self.assertEqual(GOLDEN_SHA256, digest.hexdigest())

    def test_each_worker_has_its_own_records(self) -> None:
        first = trevrpc_bench.new_payload("records", 256, 0).next()
        second = trevrpc_bench.new_payload("records", 256, 1).next()
        self.assertEqual(256, len(first))
        self.assertNotEqual(first, second)

    def test_repeat_is_the_same_letters(self) -> None:
        payload = trevrpc_bench.new_payload("repeat", 5, 3)
        self.assertEqual(["xxxxx", "xxxxx"], [payload.next(), payload.next()])


class PacedTest(unittest.TestCase):
    def test_message_carries_its_number_in_place_of_the_payload(self) -> None:
        self.assertEqual("12 xxx", paced_message(12, "xxxxxx"))
        self.assertEqual(12, paced_sequence(paced_message(12, "xxxxxx")))

    def test_number_longer_than_the_payload_makes_the_message_longer(self) -> None:
        self.assertEqual("12345 ", paced_message(12345, "xx"))

    def test_echo_without_a_number(self) -> None:
        self.assertIsNone(paced_sequence("xxxx"))
        self.assertIsNone(paced_sequence("-1 xx"))


class SummaryTest(unittest.TestCase):
    def test_nearest_rank_percentiles(self) -> None:
        ordered = [i / 1000 for i in range(1, 101)]
        self.assertAlmostEqual(50_000, percentile_us(ordered, 0.5))
        self.assertAlmostEqual(99_000, percentile_us(ordered, 0.99))
        self.assertAlmostEqual(100_000, percentile_us(ordered, 0.999))

    def test_result_line(self) -> None:
        workers = [Worker(latencies=[0.001, 0.002], messages=20, lost=1, errors=0)]
        result = summarize(config(scenario="paced", duration=2.0), "quic", workers)
        self.assertEqual(2, result["ops"])
        self.assertEqual(10.0, result["msgs_per_sec"])
        self.assertEqual(640.0, result["bytes_per_sec"])
        self.assertEqual(1, result["lost"])
        self.assertEqual(100.0, result["rate"])
        self.assertNotIn("validation", result)

    def test_slow_counts_the_latencies_above_the_threshold(self) -> None:
        workers = [Worker(latencies=[0.010, 0.040, 0.041, 0.100], messages=4)]
        result = summarize(config(slow_us=40_000), "quic", workers)
        self.assertEqual(3 - 1, result["slow"])

    def test_no_slow_count_without_the_flag(self) -> None:
        result = summarize(config(), "quic", [Worker(latencies=[1.0], messages=1)])
        self.assertNotIn("slow", result)

    def test_no_operation_completed(self) -> None:
        with self.assertRaisesRegex(
            BenchError, r"^no operation completed in the 1s measured window \(3 failed\)$"
        ):
            summarize(config(), "quic", [Worker(errors=3)])


class WireSamplerTest(unittest.TestCase):
    def test_bytes_over_the_messages_between_the_samples(self) -> None:
        wire = [100]
        workers = [Worker(delivered=5)]
        sampler = WireSampler(lambda: wire[0], workers, start=1.0, end=2.0)
        sampler.observe(0.5)
        self.assertEqual([None, None], sampler.at)
        sampler.observe(1.0)
        wire[0] = 400
        workers[0].delivered = 8
        sampler.observe(2.5)
        self.assertEqual(100.0, sampler.per_message(windowed=99))

    def test_window_messages_when_none_were_delivered_between_the_samples(self) -> None:
        sampler = WireSampler(lambda: 60, [Worker()], start=0.0, end=0.0)
        sampler.observe(1.0)
        self.assertIsNone(sampler.per_message(windowed=0))
        sampler.at = [(0, 0), (60, 0)]
        self.assertEqual(20.0, sampler.per_message(windowed=3))


class TimelineTest(unittest.TestCase):
    def test_counts_land_in_the_bucket_of_their_completion(self) -> None:
        timeline = trevrpc_bench.Timeline.of(start=10.0, duration=0.25, bucket_ms=100)
        self.assertEqual([0, 0, 0], timeline.msgs)
        timeline.add(9.99, 5)  # before the window
        timeline.add(10.0, 1)
        timeline.add(10.099, 2)
        timeline.add(10.1, 3)
        timeline.add(10.249, 4)
        timeline.add(10.3, 6)  # after the window
        self.assertEqual([3, 3, 4], timeline.msgs)


class FakeClock:
    """A clock that moves only when the code under test waits on it."""

    def __init__(self) -> None:
        self.at = 0.0
        self.clock = Clock(self.now, self.sleep)

    def now(self) -> float:
        return self.at

    async def sleep(self, delay: float) -> None:
        self.at += max(delay, 0.0)
        await asyncio.sleep(0)


class Scripted:
    """An operation that takes the time, then fails or delivers, as its script says."""

    def __init__(self, clock: FakeClock, script: list[tuple[float, bool]]) -> None:
        self.clock = clock
        self.script = script
        self.closed = False

    async def run(self) -> int:
        delay, ok = self.script.pop(0) if self.script else (10.0, True)
        await self.clock.sleep(delay)
        if not ok:
            raise TrevRpcError(Code.UNAVAILABLE, "scripted")
        return 2

    async def close(self) -> None:
        self.closed = True


class OperationLoopTest(unittest.IsolatedAsyncioTestCase):
    async def test_counts_only_operations_inside_the_window(self) -> None:
        clock = FakeClock()
        # The window is [0.05, 0.25]. One operation straddles its start, two complete inside it
        # around a failure (with the failure's 1 ms backoff), and one straddles its end: they
        # finish 0.05, 0.08, 0.10, 0.151 and 0.351 s into the window.
        operation = Scripted(
            clock, [(0.1, True), (0.03, True), (0.02, False), (0.05, True), (0.2, True)]
        )
        timeline = trevrpc_bench.Timeline.of(0.05, 0.2, 100)
        worker = Worker(timeline=timeline)
        await OperationLoop(operation, None, clock.clock).work(worker, 0.05, 0.25)

        self.assertEqual([0.03, 0.05], [round(latency, 9) for latency in worker.latencies])
        self.assertEqual(4, worker.messages)
        self.assertEqual(1, worker.errors)
        self.assertEqual(8, worker.delivered)
        # The first two complete in the first 100 ms bucket and the fourth in the second; the
        # last ends after the window.
        self.assertEqual([4, 2], timeline.msgs)
        self.assertTrue(operation.closed)


class FakeCall:
    """A bidirectional call that echoes each request at once, and fails its send when told to."""

    def __init__(self, fail_after: int | None = None) -> None:
        self.fail_after = fail_after
        self.sent: list[str] = []
        self.responses: asyncio.Queue[EchoResponse | None] = asyncio.Queue()
        self.closed = False

    async def __aenter__(self) -> FakeCall:
        return self

    async def __aexit__(self, *_: object) -> None:
        self.closed = True

    async def send(self, request: EchoRequest) -> None:
        if self.fail_after is not None and len(self.sent) == self.fail_after:
            raise TrevRpcError(Code.UNAVAILABLE, "stream reset")
        self.sent.append(request.message)
        self.responses.put_nowait(EchoResponse(message=request.message))

    async def recv(self) -> EchoResponse | None:
        return await self.responses.get()

    async def close_send(self) -> None:
        self.responses.put_nowait(None)

    async def aclose(self) -> None:
        self.closed = True


class PacedWorkloadTest(unittest.IsolatedAsyncioTestCase):
    async def test_a_failed_stream_loses_the_messages_it_held_and_those_while_it_was_down(
        self,
    ) -> None:
        clock = FakeClock()
        # One message every 100 ms from 0 s; the first stream fails sending message 7, at 0.7 s.
        calls = [FakeCall(fail_after=7), FakeCall()]
        opened = iter(calls)
        paced = trevrpc_bench.PacedWorkload(
            lambda **_: next(opened),  # pyright: ignore[reportArgumentType, reportUnknownLambdaType]
            {},
            trevrpc_bench.new_payload("repeat", 64, 0),
            0.1,
            None,
            clock.clock,
        )
        worker = Worker()
        await paced.work(worker, 0.45, 1.45)

        # The second stream starts with message 8, after the 1 ms backoff: message 7 was marked
        # sent when the send failed, and never comes back.
        self.assertEqual(7, len(calls[0].sent))
        self.assertTrue(calls[1].sent[0].startswith("8 "))
        self.assertEqual(1, worker.errors)
        # Messages 5 to 14 are scheduled inside the window: all but 7 come back in time.
        self.assertEqual(9, worker.messages)
        self.assertEqual(1, worker.lost)
        self.assertTrue(all(call.closed for call in calls))


class BidiOperationTest(unittest.IsolatedAsyncioTestCase):
    async def test_a_failed_round_trip_closes_the_stream_and_the_next_opens_another(
        self,
    ) -> None:
        calls = [FakeCall(fail_after=1), FakeCall()]
        opened = iter(calls)
        operation = trevrpc_bench.BidiOperation(
            lambda **_: next(opened),  # pyright: ignore[reportArgumentType, reportUnknownLambdaType]
            {},
            trevrpc_bench.new_payload("repeat", 8, 0),
        )

        self.assertEqual(1, await operation.run())
        with self.assertRaises(TrevRpcError):
            _ = await operation.run()
        self.assertTrue(calls[0].closed)
        self.assertIsNone(operation.call)
        self.assertEqual(1, await operation.run())
        self.assertEqual(["xxxxxxxx"], calls[1].sent)
        await operation.close()
        self.assertTrue(calls[1].closed)


class WireCounterTest(unittest.TestCase):
    def test_bytes_of_a_replaced_connection_still_count(self) -> None:
        first = SimpleNamespace(bytes_sent=100, bytes_received=50)
        channel = SimpleNamespace(_connection=first)
        counter = trevrpc_bench.WireCounter([channel])

        self.assertEqual(150, counter.total())
        # The channel reconnects; the new connection starts at zero.
        first.bytes_sent = 120
        channel._connection = None
        self.assertEqual(170, counter.total())
        channel._connection = SimpleNamespace(bytes_sent=10, bytes_received=5)
        self.assertEqual(185, counter.total())

    def test_track_sees_a_connection_that_comes_and_goes_between_totals(self) -> None:
        channel = SimpleNamespace(_connection=SimpleNamespace(bytes_sent=1, bytes_received=1))
        counter = trevrpc_bench.WireCounter([channel])
        self.assertEqual(2, counter.total())
        channel._connection = SimpleNamespace(bytes_sent=30, bytes_received=0)
        counter.track()
        channel._connection = SimpleNamespace(bytes_sent=4, bytes_received=0)

        self.assertEqual(36, counter.total())


class CheckTest(unittest.TestCase):
    def parse(self, *argv: str) -> argparse.Namespace:
        parser = argparse.ArgumentParser()
        trevrpc_bench.add_parser(parser.add_subparsers(dest="command"))
        common = ["bench", "--addr", "127.0.0.1:1", "--ca", "ca.pem", "--transport", "quic"]
        return parser.parse_args([*common, *argv])

    def test_valid_cell(self) -> None:
        self.assertIsNone(trevrpc_bench.check(self.parse("--scenario", "unary")))

    def test_compression_mode_that_does_not_run_the_scenario(self) -> None:
        args = self.parse("--scenario", "server-stream", "--compression", "stream")
        self.assertIn("does not run the server-stream scenario", trevrpc_bench.check(args) or "")

    def test_dictionary_modes_need_a_dictionary(self) -> None:
        args = self.parse("--scenario", "bidi", "--compression", "dictionary")
        self.assertIn("go together", trevrpc_bench.check(args) or "")

    def test_rules_only_for_the_validation_scenarios(self) -> None:
        args = self.parse("--scenario", "unary", "--validation", "cel")
        self.assertIn("validation scenarios only", trevrpc_bench.check(args) or "")

    def test_more_connections_than_workers(self) -> None:
        args = self.parse("--scenario", "unary", "--connections", "2")
        self.assertIn("--connections", trevrpc_bench.check(args) or "")


if __name__ == "__main__":
    unittest.main()
