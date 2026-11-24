"""The test server's `Signal`, which the other tests rely on to wait."""

from __future__ import annotations

import asyncio
import unittest
from unittest import mock

from .server import Signal

TIMEOUT = 1.0


class WaitWhileProgressingTest(unittest.IsolatedAsyncioTestCase):
    """Runs the loop on a clock that only moves when the test moves it, so deadlines pass
    exactly when the test says, however loaded the machine is."""

    async def asyncSetUp(self) -> None:
        self.now = 0.0
        loop = asyncio.get_running_loop()
        patcher = mock.patch.object(loop, "time", lambda: self.now)
        patcher.start()
        self.addCleanup(patcher.stop)
        self.signal = Signal()
        self.progress = 0
        self.done = False
        self.wait = asyncio.create_task(
            self.signal.wait_while_progressing(lambda: self.done, lambda: self.progress, TIMEOUT)
        )
        await self.settle()

    async def settle(self) -> None:
        """Lets the wait and its deadline run to where they block again."""
        for _ in range(8):
            await asyncio.sleep(0)

    async def step(self, elapsed: float, progress: int = 0) -> None:
        self.now += elapsed
        self.progress += progress
        self.signal.notify()
        await self.settle()

    async def test_progress_extends_the_deadline(self) -> None:
        for _ in range(5):
            await self.step(0.6 * TIMEOUT, progress=1)
            self.assertFalse(self.wait.done())
        self.done = True
        await self.step(0.6 * TIMEOUT)
        self.assertTrue(self.wait.done())
        await self.wait

    async def test_fails_once_progress_stalls_for_the_timeout(self) -> None:
        await self.step(0.6 * TIMEOUT, progress=1)
        await self.step(0.6 * TIMEOUT)
        self.assertFalse(self.wait.done())
        await self.step(0.6 * TIMEOUT)
        self.assertTrue(self.wait.done())
        with self.assertRaises(TimeoutError):
            await self.wait

    async def test_notifications_without_progress_do_not_extend_it(self) -> None:
        await self.step(0.9 * TIMEOUT)
        self.assertFalse(self.wait.done())
        await self.step(0.2 * TIMEOUT)
        self.assertTrue(self.wait.done())
        with self.assertRaises(TimeoutError):
            await self.wait


if __name__ == "__main__":
    unittest.main()
