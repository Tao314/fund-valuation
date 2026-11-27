"""Curve/card sync guardrails (v1.6.2 fix): the card estimate and the
intraday sparkline must agree.

Regression: 2026-11-26, fund 024481 showed card +4.11% while the curve
peaked at 2.44% and its axis only spanned +/-2.44%, then synced later.
Root causes: (1) the series refreshes on a separate cycle and can lag
the card by minutes; (2) curve buckets were raw weighted changes while
only the final point got the beta calibration; (3) /api/history/today
returned a stale series immediately while a background recompute ran.
"""
import asyncio
import time
import unittest
from pathlib import Path

from tests._common import REPO_ROOT, app

ROW = {"alpha": 0.1, "beta": 1.5}


class TestCalibrateCurve(unittest.TestCase):
    def test_calibrates_all_but_last_point(self):
        # Reported case shape: raw buckets 0.5 / 2.44, final point 4.11
        # already calibrated by the caller.
        pts = [[1000, 0.5], [2000, 2.44], [3000, 4.11]]
        out = app._calibrate_curve(pts, ROW)
        self.assertEqual(out, [[1000, 0.85], [2000, 3.76], [3000, 4.11]])

    def test_no_beta_row_is_identity(self):
        pts = [[1000, 0.5], [2000, 2.44]]
        self.assertEqual(app._calibrate_curve(pts, None), pts)
        self.assertEqual(
            app._calibrate_curve(pts, {"alpha": None, "beta": 1.5}), pts)
        self.assertEqual(
            app._calibrate_curve(pts, {"alpha": 0.1, "beta": None}), pts)

    def test_single_point_unchanged(self):
        pts = [[1000, 4.11]]
        self.assertEqual(app._calibrate_curve(pts, ROW), pts)
        self.assertEqual(app._calibrate_curve([], ROW), [])


class TestSeriesStaleThreshold(unittest.TestCase):
    def test_stale_max_exceeds_ttl(self):
        # The wait path must only trigger past the normal refresh
        # cadence, never on every request.
        self.assertGreater(app._SERIES_STALE_MAX, app._SERIES_TTL)
        self.assertEqual(app._SERIES_TTL, 60)
        # 120s, not minutes (perf tier 1: curve 60-120s): the sparkline's
        # last point is overridden with the card value, so only mid-curve
        # staleness is at stake.
        self.assertEqual(app._SERIES_STALE_MAX, 120)


class TestHistoryTodayStale(unittest.IsolatedAsyncioTestCase):
    UID = 7

    async def asyncSetUp(self):
        self._orig_uid = app.current_user_id
        app.current_user_id = lambda session: self.UID
        self._cache = dict(app._series_cache)
        self._tasks = dict(app._series_tasks)

    async def asyncTearDown(self):
        app.current_user_id = self._orig_uid
        app._series_cache.clear()
        app._series_cache.update(self._cache)
        for t in list(app._series_tasks.values()):
            if t is not self._tasks.get(self.UID):
                t.cancel()
        app._series_tasks.clear()
        app._series_tasks.update(self._tasks)

    async def test_too_stale_waits_for_recompute(self):
        stale = {"024481": [[1000, 1.0], [2000, 2.0]]}
        fresh = {"024481": [[1000, 1.0], [2000, 4.11]]}
        app._series_cache[self.UID] = (time.time() - 100, stale)

        async def updater():
            app._series_cache[self.UID] = (time.time(), fresh)

        task = asyncio.create_task(updater())
        await task  # recompute finished; cache now fresh
        app._series_tasks[self.UID] = task
        result = await app.history_today(session="x")
        self.assertEqual(result, {"series": fresh})

    async def test_moderately_stale_returns_immediately(self):
        stale = {"024481": [[1000, 1.0], [2000, 2.0]]}
        app._series_cache[self.UID] = (time.time() - 70, stale)
        event = asyncio.Event()
        task = asyncio.create_task(event.wait())
        app._series_tasks[self.UID] = task
        try:
            result = await asyncio.wait_for(
                app.history_today(session="x"), timeout=5)
        finally:
            task.cancel()
        self.assertEqual(result, {"series": stale})


class TestSparklineOverride(unittest.TestCase):
    def test_sparkline_overrides_last_point_with_card_value(self):
        html = (REPO_ROOT / "static" / "index.html").read_text()
        # The copy + override moved into sparkGeom (shared by the renderer
        # and the tap/hover tip binder); behaviour is unchanged.
        start = html.index("function sparkGeom")
        end = html.index("function sparklineHtml", start)
        body = html[start:end]
        # Copy (never mutate the shared historySeries) + override.
        self.assertIn("raw.map(p => [p[0], p[1]])", body)
        self.assertIn("pts[pts.length - 1][1] = f.est_change_pct", body)
        # Published cards keep the series' own last point.
        self.assertIn("!published && hasPts && f.est_change_pct != null", body)


if __name__ == "__main__":
    unittest.main()


class TestRefreshSeriesFailureFallback(unittest.IsolatedAsyncioTestCase):
    UID = 8

    async def asyncSetUp(self):
        self._orig_compute = app._compute_series
        self._cache = dict(app._series_cache)

    async def asyncTearDown(self):
        app._compute_series = self._orig_compute
        app._series_cache.clear()
        app._series_cache.update(self._cache)

    async def test_failed_fund_keeps_fresh_old_curve(self):
        old = {"024481": [[1000, 1.0], [2000, 2.0]]}
        app._series_cache[self.UID] = (time.time(), old)

        async def failing(uid):
            return {"024481": None}

        app._compute_series = failing
        await app._refresh_series(self.UID)
        _, series = app._series_cache[self.UID]
        self.assertEqual(series, old)

    async def test_failed_fund_drops_stale_old_curve(self):
        old = {"024481": [[1000, 1.0], [2000, 2.0]]}
        app._series_cache[self.UID] = (time.time() - 3600, old)

        async def failing(uid):
            return {"024481": None}

        app._compute_series = failing
        await app._refresh_series(self.UID)
        _, series = app._series_cache[self.UID]
        self.assertEqual(series, {"024481": []})


class TestLoadEstimatesDecoupling(unittest.TestCase):
    def test_cards_render_before_history(self):
        html = (REPO_ROOT / "static" / "index.html").read_text()
        start = html.index("async function loadEstimates()")
        end = html.index('$("refresh-btn").onclick = loadEstimates;', start)
        body = html[start:end]
        self.assertLess(body.index("renderFunds(data)"),
                        body.index("histP.then"))
        self.assertNotIn("await histP;", body)
        self.assertIn("refreshSparklines(data)", body)

    def test_stale_cache_paints_with_flag(self):
        html = (REPO_ROOT / "static" / "index.html").read_text()
        self.assertIn("refreshSparklines", html)
        self.assertIn("_stale_cache", html)
        self.assertIn("昨日缓存", html)
        start = html.index("function renderCachedEstimates()")
        end = html.index("function showDash(", start)
        body = html[start:end]
        self.assertIn("prevTradingDay(todayStr())", body)
