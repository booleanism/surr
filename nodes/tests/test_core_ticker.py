"""``core.ticker``: the async pacing generator.

The ticker computes each deadline from a fixed start time, so delays do not
accumulate drift. A non-positive interval used to make every delay 0, which
turned the ticker into an unthrottled ``call_later(0)`` loop that starved the
event loop it shares with the router -- MIN_INTERVAL is the guard.
"""

from __future__ import annotations

import asyncio
import unittest

from core import ticker

class TickerTests(unittest.IsolatedAsyncioTestCase):
    async def _take(self, interval: float, count: int) -> tuple[list[int], float]:
        loop = asyncio.get_running_loop()
        start = loop.time()
        ticks: list[int] = []
        async for tick in ticker.ticker(interval):
            ticks.append(tick)
            if len(ticks) == count:
                break
        return ticks, loop.time() - start

    async def test_yields_consecutive_tick_numbers_from_zero(self):
        ticks, _ = await self._take(0.001, 4)
        self.assertEqual(ticks, [0, 1, 2, 3])

    async def test_waits_between_ticks(self):
        # 4 yields -> 3 waits of 50ms.
        _, elapsed = await self._take(0.05, 4)
        self.assertGreaterEqual(elapsed, 3 * 0.05)

    async def test_zero_interval_is_clamped(self):
        # Without the clamp this returns almost immediately (a hot loop).
        _, elapsed = await self._take(0.0, 3)
        self.assertGreaterEqual(elapsed, 2 * ticker.MIN_INTERVAL)

    async def test_negative_interval_is_clamped(self):
        _, elapsed = await self._take(-5.0, 3)
        self.assertGreaterEqual(elapsed, 2 * ticker.MIN_INTERVAL)

    async def test_clamped_interval_does_not_spin(self):
        # 20 yields at the floor must take at least 19 floors of wall clock,
        # i.e. the generator really is awaiting between yields.
        _, elapsed = await self._take(0.0, 20)
        self.assertGreaterEqual(elapsed, 19 * ticker.MIN_INTERVAL)

    async def test_min_interval_is_positive(self):
        self.assertGreater(ticker.MIN_INTERVAL, 0)

    async def test_interval_does_not_drift_over_many_ticks(self):
        # Deadlines are absolute (start + n*interval), so 30 ticks at 1ms
        # cannot accumulate the per-iteration scheduling overhead.
        _, elapsed = await self._take(0.001, 30)
        self.assertLess(elapsed, 0.5)

if __name__ == "__main__":
    unittest.main()
