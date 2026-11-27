"""Perf tier 1 (2026-11-27) guardrails: singleflight, shared HTTP client,
upstream breaker, shared memos, active-user gating, layered TTL."""
import asyncio
import json
import tempfile
import time
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import AsyncMock, patch

from tests._common import app, cn, frozen_time

_TMPDIR = None
_TMP_DB = None


def setUpModule():
    global _TMPDIR, _TMP_DB
    _TMPDIR = tempfile.TemporaryDirectory()
    _TMP_DB = Path(_TMPDIR.name) / "test.db"
    patch.object(app, "DB_PATH", _TMP_DB).start()
    app.init_db()


def tearDownModule():
    patch.stopall()
    _TMPDIR.cleanup()
    app._upstream_breaker.clear()
    app._flight_tasks.clear()


class TestSingleflight(unittest.IsolatedAsyncioTestCase):
    async def test_concurrent_callers_share_one_compute(self):
        calls = 0

        async def slow():
            nonlocal calls
            calls += 1
            await asyncio.sleep(0.05)
            return {"v": 42}

        results = await asyncio.gather(
            *[app._singleflight("t1:shared", slow) for _ in range(5)])
        self.assertEqual(calls, 1)
        self.assertTrue(all(r == {"v": 42} for r in results))
        await asyncio.sleep(0)
        self.assertNotIn("t1:shared", app._flight_tasks)

    async def test_failure_releases_key(self):
        calls = 0

        async def flaky():
            nonlocal calls
            calls += 1
            if calls == 1:
                raise RuntimeError("boom")
            return "ok"

        with self.assertRaises(RuntimeError):
            await app._singleflight("t1:flaky", flaky)
        self.assertEqual(await app._singleflight("t1:flaky", flaky), "ok")
        self.assertEqual(calls, 2)


class TestSharedHttpClient(unittest.TestCase):
    def test_client_is_shared_and_bounded(self):
        c1 = app._http_client()
        c2 = app._http_client()
        self.assertIs(c1, c2)
        self.assertTrue(c1.follow_redirects)
        self.assertEqual(c1.timeout.read, 15.0)

    def test_breaker_opens_after_three_failures_and_resets(self):
        app._upstream_breaker.clear()
        self.assertFalse(app._breaker_open("sina"))
        app._breaker_note("sina", False)
        app._breaker_note("sina", False)
        self.assertFalse(app._breaker_open("sina"))
        app._breaker_note("sina", False)
        self.assertTrue(app._breaker_open("sina"))
        app._breaker_note("sina", True)
        self.assertFalse(app._breaker_open("sina"))


class TestBreakerSuppression(unittest.IsolatedAsyncioTestCase):
    async def test_suppressed_call_raises_fast_without_recording(self):
        app._upstream_breaker.clear()
        app._upstream_breaker["sina"] = {"fails": 3, "open_until": time.time() + 60}
        before = (app._source_stats.get("sina") or {}).get("fail", 0)
        t0 = time.monotonic()
        with self.assertRaisesRegex(RuntimeError, "circuit open"):
            await app.upstream_get("https://hq.sinajs.cn/list=sh600000")
        self.assertLess(time.monotonic() - t0, 2.0)
        after = (app._source_stats.get("sina") or {}).get("fail", 0)
        self.assertEqual(after, before)
        app._upstream_breaker.clear()


class TestLayeredTtl(unittest.TestCase):
    def test_ttl_layers(self):
        self.assertEqual(app._EST_TTL, 30)
        self.assertEqual(app._EST_STALE_MAX, 60)
        self.assertEqual(app._SERIES_TTL, 60)
        self.assertEqual(app._SERIES_STALE_MAX, 120)
        self.assertEqual(app.QUOTE_TTL, 45)
        self.assertEqual(app._UPSTREAM_TIMEOUT_CAPS["sina"], 10)


class TestMemos(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        app._holdings_memo.clear()
        app._industry_memo.clear()
        app._ftype_memo.clear()

    async def test_holdings_memo_returns_same_object(self):
        code = "000001"
        with app.db() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO holdings_cache"
                "(fund_code, holdings_json, report_label, report_date, fetched_at)"
                " VALUES(?,?,?,?,?)",
                (code, json.dumps(
                    [{"code": "600519", "name": "贵州茅台", "weight": 9.5}],
                    ensure_ascii=False),
                 "2026年三季度", "2026-09-30", time.time()))
        first = await app.fetch_holdings(code)
        second = await app.fetch_holdings(code)
        self.assertIs(first, second)
        self.assertEqual(first["holdings"][0]["code"], "600519")

    async def test_holdings_fuyao_empty_falls_back_to_stale(self):
        # Regression: the fuyao fallback must not raise / short-circuit
        # the stale-DB fallback when it yields nothing usable.
        code = "000002"
        with app.db() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO holdings_cache"
                "(fund_code, holdings_json, report_label, report_date, fetched_at)"
                " VALUES(?,?,?,?,?)",
                (code, json.dumps(
                    [{"code": "600519", "name": "贵州茅台", "weight": 9.5}]),
                 "2026年三季度", "2026-09-30",
                 time.time() - app.HOLDINGS_TTL - 10))
        with patch.object(app, "upstream_get",
                          new=AsyncMock(side_effect=Exception("jjcc down"))), \
                patch.object(app, "FUYAO_ENABLED", True), \
                patch.object(app, "_fuyao_holdings_raw",
                             new=AsyncMock(return_value=({}, 0))):
            out = await app.fetch_holdings(code)
        self.assertTrue(out["stale"])
        self.assertEqual(len(out["holdings"]), 1)

    async def test_industry_memo_returns_manual_value(self):
        code = next(iter(app.MANUAL_SECTOR))
        first = await app.fetch_industry(code)
        second = await app.fetch_industry(code)
        self.assertEqual(first, second)
        self.assertIn(code, app._industry_memo)


class TestActiveUids(unittest.TestCase):
    def test_only_recent_heartbeats_count(self):
        now = datetime.now(timezone.utc)
        with app.db() as conn:
            conn.execute(
                "INSERT INTO users(username, password_hash, created_at)"
                " VALUES('u1','x','2026-01-01')")
            conn.execute(
                "INSERT INTO users(username, password_hash, created_at)"
                " VALUES('u2','x','2026-01-01')")
            conn.execute(
                "INSERT INTO sessions(token, user_id, created_at, last_seen_at)"
                " VALUES('t1',1,'2026-01-01',?)", (now.isoformat(),))
            conn.execute(
                "INSERT INTO sessions(token, user_id, created_at, last_seen_at)"
                " VALUES('t2',2,'2026-01-01',?)",
                ((now - timedelta(hours=2)).isoformat(),))
        active, total = app._active_uids()
        self.assertEqual(active, [1])
        self.assertEqual(total, 2)


class TestTrendsWarmSkip(unittest.IsolatedAsyncioTestCase):
    async def test_off_hours_skips_upstream(self):
        with frozen_time(cn(2026, 11, 29, 12, 0)):  # Sunday
            out = await app._trends_warm_once()
        self.assertTrue(out["skipped"])
        self.assertEqual(out["secids"], 0)


if __name__ == "__main__":
    unittest.main()
