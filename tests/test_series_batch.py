"""_compute_series two-pass batching regression (item 2).

Mocks all upstream fetchers (no real network). Covers: holdings curve
from batched trends, feeder curve from a batched index fetch, bond
curve from the batched 511260 fetch, and graceful degradation when
the feeder index resolution fails. Time is frozen to a Tuesday 10:00
(session hours) so pre-open / holiday guards don't interfere.
"""
import asyncio
import tempfile
import time
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

from tests._common import CN_TZ, app

_TMPDIR = None
_TMP_DB = None


def setUpModule():
    global _TMPDIR, _TMP_DB
    _TMPDIR = tempfile.TemporaryDirectory()
    _TMP_DB = Path(_TMPDIR.name) / "test.db"
    patch.object(app, "DB_PATH", _TMP_DB).start()
    app.init_db()
    with app.db() as conn:
        conn.execute(
            "INSERT INTO users(username, password_hash, created_at, is_admin)"
            " VALUES('u','x','2026-11-26',0)")
        for code, name in (("000001", "混合基金A"),
                           ("004103", "中信保诚稳悦债券C"),
                           ("110022", "易方达沪深300ETF联接A")):
            conn.execute(
                "INSERT INTO user_funds(user_id, fund_code, fund_name, created_at)"
                " VALUES(1, ?, ?, '2026-11-26')", (code, name))


def tearDownModule():
    patch.stopall()
    _TMPDIR.cleanup()


class _FrozenDT(datetime):
    """datetime frozen to Tue 2026-11-24 10:00 Asia/Shanghai (in session)."""

    @classmethod
    def now(cls, tz=None):
        return cls(2026, 11, 24, 10, 0, tzinfo=CN_TZ)


def _pts(base=100.0):
    now = int(time.time())
    return {"pre_close": base,
            "points": [[now - 3600, base * 1.01], [now - 1800, base * 1.02]]}


class TestComputeSeriesBatch(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self._saved = {}
        for name in ("fetch_holdings", "fetch_stock_market_map",
                     "holding_quote_symbol", "fetch_stock_position",
                     "fetch_holding_trends", "fetch_quotes",
                     "fetch_fund_type", "resolve_fund_index",
                     "fetch_stock_trends", "_ensure_beta", "_beta_row"):
            self._saved[name] = getattr(app, name)
        self._trend_calls = []
        self._fail_index = False
        self._dt_patch = patch.object(app, "datetime", _FrozenDT)
        self._dt_patch.start()

        # Mixed A+US holdings: dodges both the pre-open guard (has a US
        # holding) and the US-holiday guard (not all-US).
        holdings = {
            "000001": [{"code": "600519", "name": "贵州茅台", "weight": 25.0},
                       {"code": "AAPL", "name": "苹果", "weight": 25.0}],
            "004103": [{"code": "2600002", "name": "债券A", "weight": 40.0}],
            "110022": [],
        }

        async def fake_holdings(code):
            return {"holdings": holdings[code], "report_label": None,
                    "report_date": None, "stale": False}

        async def fake_mmap(code):
            return {}

        async def fake_sym(h, mmap):
            return app.to_sina_symbol(h["code"])

        async def fake_position(code):
            return 80.0 if code == "000001" else 0.0

        async def fake_holding_trends(code, mmap, date_str, name):
            # Bond holdings have no minute trends (like the real feed).
            if code == "2600002":
                return None
            return _pts()

        async def fake_quotes(symbols):
            return {s: {"change_pct": 1.0, "current": 100.0,
                        "prev_close": 99.0, "quote_time": "2026-11-26 14:00"}
                    for s in symbols}

        async def fake_ftype(code):
            return {"000001": "混合型-偏股",
                    "004103": "债券型-可转债",
                    "110022": "混合型基金"}[code]

        async def fake_index(code):
            if self._fail_index:
                raise RuntimeError("index down")
            return {"secid": "1.000300", "index_name": "沪深300"}

        async def fake_trends(secid, date_str):
            self._trend_calls.append(secid)
            return _pts()

        app.fetch_holdings = fake_holdings
        app.fetch_stock_market_map = fake_mmap
        app.holding_quote_symbol = fake_sym
        app.fetch_stock_position = fake_position
        app.fetch_holding_trends = fake_holding_trends
        app.fetch_quotes = fake_quotes
        app.fetch_fund_type = fake_ftype
        app.resolve_fund_index = fake_index
        app.fetch_stock_trends = fake_trends
        app._ensure_beta = lambda code: None
        app._beta_row = lambda code: None

    async def asyncTearDown(self):
        self._dt_patch.stop()
        for name, fn in self._saved.items():
            setattr(app, name, fn)

    async def test_all_three_paths_batched(self):
        series = await app._compute_series(1)
        self.assertEqual(sorted(series), ["000001", "004103", "110022"])
        for code in ("000001", "004103", "110022"):
            self.assertTrue(len(series[code]) >= 2, code)
        # One trends fetch per index secid, not one per fund; the
        # convertible bond fund curves off its own proxy (1.511380).
        self.assertEqual(sorted(set(self._trend_calls)),
                         ["1.000300", "1.511380"])

    async def test_feeder_index_failure_degrades_gracefully(self):
        self._fail_index = True
        series = await app._compute_series(1)
        self.assertEqual(series["110022"], [])
        self.assertTrue(len(series["000001"]) >= 2)
        self.assertTrue(len(series["004103"]) >= 2)


if __name__ == "__main__":
    unittest.main()
