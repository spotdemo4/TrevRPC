"""Unit tests for the records payload. Run with `python3 -I -m unittest discover -s bench`."""

import json
import unittest

import payload


class RecordsTest(unittest.TestCase):
    def test_matches_the_golden_digest(self) -> None:
        self.assertEqual(payload.GOLDEN_SHA256, payload.golden_digest())

    def test_messages_are_records_cut_to_size(self) -> None:
        generator = payload.Records(payload.WORKER_SEED)
        message = generator.message(1024)

        self.assertEqual(1024, len(message))
        first = json.loads(message.split(b"\n")[0])
        self.assertEqual(
            ["ts", "service", "region", "method", "route", "status", "latency_ms", "trace", "user"],
            list(first),
        )
        # The next message starts with a new record, and differs.
        following = generator.message(1024)
        self.assertTrue(following.startswith(b'{"ts":'))
        self.assertNotEqual(message, following)

    def test_training_samples_cycle_through_the_sizes(self) -> None:
        samples = payload.training_samples()

        self.assertEqual(payload.TRAINING_MESSAGES, len(samples))
        self.assertEqual([64, 256, 1024, 4096, 64], [len(sample) for sample in samples[:5]])


if __name__ == "__main__":
    _ = unittest.main()
