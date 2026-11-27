"""Bond-fund 511260 proxy trigger + admin notice endpoints (2026-11-26).

Regression: 004103 中信保诚稳悦债券C top-10 all bonds (~81.6%):
7-digit codes (2600002/2600004/...) have no Sina symbol, 6-digit bond
codes (250215/220208/...) come back with a forced 0.00% quote, so the
old `coverage == 0` trigger never fired and the card showed 0.00%
while the curve (511260 trends) moved. The trigger is now "no quotable
holding" (quoted weight 0) + bond fund type. All DB writes go to a temp
file; upstream fetchers are mocked (no real network).
"""
import asyncio
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from fastapi import HTTPException

from tests._common import app, cn, frozen_time

# 004103-shaped fixture: 7-digit bond codes (no Sina symbol) + 6-digit
# bond codes whose only quote is the forced 0.00% (current == prev).
BOND_HOLDINGS = [
    {"code": "2600002", "name": "债券A", "weight": 42.16},
    {"code": "2600004", "name": "债券B", "weight": 28.89},
    {"code": "2500002", "name": "债券C", "weight": 7.28},
    {"code": "250215", "name": "国开债", "weight": 1.5},
    {"code": "220208", "name": "国开债", "weight": 1.2},
]
BOND_SYMS = [app.to_sina_symbol(h["code"]) for h in BOND_HOLDINGS]
BOND_QUOTES = {
    "sz250215": {"name": None, "prev_close": 100.0, "current": 100.0,
                 "change_pct": 0.0, "quote_time": "2026-11-26 14:00"},
    "sz220208": {"name": None, "prev_close": 100.0, "current": 100.0,
                 "change_pct": 0.0, "quote_time": "2026-11-26 14:00"},
    "sh511260": {"name": "十年国债ETF国泰", "prev_close": 135.83,
                 "current": 135.882, "change_pct": 0.04,
                 "quote_time": "2026-11-26 14:00"},
    "sh511380": {"name": "可转债ETF博时", "prev_close": 11.685,
                 "current": 11.626, "change_pct": -0.50,
                 "quote_time": "2026-11-26 14:00"},
    "sh511360": {"name": "短融ETF海富通", "prev_close": 114.20,
                 "current": 114.21, "change_pct": 0.01,
                 "quote_time": "2026-11-26 14:00"},
    "sh511200": {"name": "信用债ETF华夏", "prev_close": 102.70,
                 "current": 102.73, "change_pct": 0.03,
                 "quote_time": "2026-11-26 14:00"},
}

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
            " VALUES('admin','x','2026-11-26',1)")
        conn.execute(
            "INSERT INTO users(username, password_hash, created_at, is_admin)"
            " VALUES('plain','x','2026-11-26',0)")


def tearDownModule():
    patch.stopall()
    _TMPDIR.cleanup()


class TestQuotedWeight(unittest.TestCase):
    def test_bond_fixture_has_no_quotable_holding(self):
        # 7-digit codes have no symbol; 6-digit bonds only show 0.00%.
        self.assertEqual(BOND_SYMS, [None, None, None, "sz250215", "sz220208"])
        self.assertEqual(app._quoted_weight(BOND_HOLDINGS, BOND_SYMS, BOND_QUOTES), 0.0)

    def test_moved_stock_counts_as_quotable(self):
        holdings = [{"code": "600519", "name": "贵州茅台", "weight": 10.0}]
        quotes = {"sh600519": {"change_pct": 1.5, "current": 1500.0, "prev_close": 1477.8}}
        self.assertEqual(app._quoted_weight(holdings, ["sh600519"], quotes), 10.0)

    def test_missing_quote_is_not_quotable(self):
        holdings = [{"code": "600519", "name": "贵州茅台", "weight": 10.0}]
        self.assertEqual(app._quoted_weight(holdings, ["sh600519"], {}), 0.0)


class TestBondProxy(unittest.TestCase):
    def test_bond_fund_no_quotable_uses_511260(self):
        self.assertEqual(
            app._bond_proxy_change(0.0, "债券型基金", BOND_QUOTES), 0.04)

    def test_quotable_holding_blocks_proxy(self):
        self.assertIsNone(app._bond_proxy_change(10.0, "债券型基金", BOND_QUOTES))

    def test_non_bond_fund_blocks_proxy(self):
        self.assertIsNone(app._bond_proxy_change(0.0, "混合型基金", BOND_QUOTES))

    def test_missing_511260_quote_blocks_proxy(self):
        self.assertIsNone(app._bond_proxy_change(0.0, "债券型基金", {}))

    def test_convertible_bond_fund_uses_511380(self):
        self.assertEqual(
            app._bond_proxy_change(0.0, "债券型-可转债", BOND_QUOTES), -0.50)

    def test_short_bond_fund_uses_511360(self):
        self.assertEqual(
            app._bond_proxy_change(0.0, "债券型-中短债", BOND_QUOTES), 0.01)

    def test_credit_bond_fund_uses_511200(self):
        self.assertEqual(
            app._bond_proxy_change(0.0, "债券型-信用债", BOND_QUOTES), 0.03)

    def test_long_and_mixed_bond_funds_keep_511260(self):
        self.assertEqual(
            app._bond_proxy_change(0.0, "债券型-长债", BOND_QUOTES), 0.04)
        self.assertEqual(
            app._bond_proxy_change(0.0, "债券型-混合债", BOND_QUOTES), 0.04)

    def test_missing_proxy_quote_blocks_proxy(self):
        quotes = {k: v for k, v in BOND_QUOTES.items() if k != "sh511380"}
        self.assertIsNone(
            app._bond_proxy_change(0.0, "债券型-可转债", quotes))


class TestMarketOfHolding(unittest.TestCase):
    def test_seven_digit_code_is_bond(self):
        self.assertEqual(app.market_of_holding("2600002"), "债券")
        self.assertEqual(app.market_of_holding("2600004"), "债券")

    def test_other_codes_unchanged(self):
        self.assertEqual(app.market_of_holding("600519"), "A")
        self.assertEqual(app.market_of_holding("00700"), "HK")
        self.assertEqual(app.market_of_holding("AAPL"), "US")


class TestEstimateOneBond(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        # _estimate_one is time-dependent (CN pre-open sends no estimate):
        # pin to a trading-hours instant so the proxy assertions hold no
        # matter when the suite runs.
        self._time_patch = frozen_time(cn(2026, 11, 26, 14, 0))
        self._time_patch.start()
        self._saved = {}
        for name in ("fetch_nav", "fetch_holdings", "fetch_stock_market_map",
                     "holding_quote_symbol", "fetch_stock_position",
                     "fetch_quotes", "fetch_fund_type"):
            self._saved[name] = getattr(app, name)

        async def fake_nav(code, *a):
            return {"nav_date": "2026-11-26", "dwjz": 1.0, "jzzzl": 0.0,
                    "prev_nav_date": None, "prev_dwjz": None,
                    "prev_jzzzl": None, "stale": False}

        async def fake_holdings(code):
            return {"holdings": BOND_HOLDINGS, "report_label": "2026年三季报",
                    "report_date": "2026-09-30", "stale": False}

        async def fake_mmap(code):
            return {}

        async def fake_sym(h, mmap):
            return app.to_sina_symbol(h["code"])

        async def fake_position(code):
            return 0.0

        async def fake_quotes(symbols):
            return dict(BOND_QUOTES)

        self._ftype = "债券型基金"

        async def fake_ftype(code):
            return self._ftype

        app.fetch_nav = fake_nav
        app.fetch_holdings = fake_holdings
        app.fetch_stock_market_map = fake_mmap
        app.holding_quote_symbol = fake_sym
        app.fetch_stock_position = fake_position
        app.fetch_quotes = fake_quotes
        app.fetch_fund_type = fake_ftype

    async def asyncTearDown(self):
        self._time_patch.stop()
        for name, fn in self._saved.items():
            setattr(app, name, fn)

    async def test_bond_fund_uses_511260_change(self):
        est = await app._estimate_one("004103", "中信保诚稳悦债券C")
        self.assertTrue(est["bond_est"])
        self.assertEqual(est["est_change_pct"], 0.04)
        self.assertEqual(est["bond_proxy_name"], "十年国债ETF国泰")
        self.assertEqual(est["coverage_pct"], 81.03)

    async def test_convertible_bond_fund_uses_511380(self):
        self._ftype = "债券型-可转债"
        est = await app._estimate_one("004103", "中信保诚稳悦债券C")
        self.assertTrue(est["bond_est"])
        self.assertEqual(est["est_change_pct"], -0.50)
        self.assertEqual(est["bond_proxy_name"], "可转债ETF博时")

    async def test_non_bond_fund_does_not_use_proxy(self):
        self._ftype = "混合型基金"
        est = await app._estimate_one("004103", "中信保诚稳悦债券C")
        self.assertFalse(est["bond_est"])
        self.assertEqual(est["est_change_pct"], 0.0)


class TestAdminNotice(unittest.TestCase):
    def setUp(self):
        self._orig_uid = app.current_user_id
        with app.db() as conn:
            conn.execute("DELETE FROM admin_notice WHERE id=1")

    def tearDown(self):
        app.current_user_id = self._orig_uid
        with app.db() as conn:
            conn.execute("DELETE FROM admin_notice WHERE id=1")

    def _as_admin(self):
        app.current_user_id = lambda session: 1

    def _as_plain(self):
        app.current_user_id = lambda session: 2

    def test_empty_then_publish_overwrite_then_clear(self):
        self._as_admin()
        self.assertEqual(app.get_notice(session="x")["message"], None)
        self.assertEqual(
            app.admin_publish_notice(app.NoticeBody(message="系统维护"), session="x"),
            {"ok": True})
        got = app.get_notice(session="x")
        self.assertEqual(got["message"], "系统维护")
        self.assertIsInstance(got["created_at"], float)
        app.admin_publish_notice(app.NoticeBody(message="第二条"), session="x")
        self.assertEqual(app.get_notice(session="x")["message"], "第二条")
        self.assertEqual(app.admin_clear_notice(session="x"), {"ok": True})
        self.assertEqual(app.get_notice(session="x")["message"], None)

    def test_plain_user_can_read_but_not_publish(self):
        self._as_admin()
        app.admin_publish_notice(app.NoticeBody(message="公告"), session="x")
        self._as_plain()
        self.assertEqual(app.get_notice(session="x")["message"], "公告")
        with self.assertRaises(HTTPException) as cm:
            app.admin_publish_notice(app.NoticeBody(message="冒充"), session="x")
        self.assertEqual(cm.exception.status_code, 403)
        with self.assertRaises(HTTPException) as cm:
            app.admin_clear_notice(session="x")
        self.assertEqual(cm.exception.status_code, 403)

    def test_unauthenticated_read_is_401(self):
        with self.assertRaises(HTTPException) as cm:
            app.get_notice(session="bogus-token")
        self.assertEqual(cm.exception.status_code, 401)

    def test_empty_and_too_long_messages_rejected(self):
        self._as_admin()
        with self.assertRaises(HTTPException) as cm:
            app.admin_publish_notice(app.NoticeBody(message="   "), session="x")
        self.assertEqual(cm.exception.status_code, 400)
        with self.assertRaises(HTTPException) as cm:
            app.admin_publish_notice(
                app.NoticeBody(message="x" * 501), session="x")
        self.assertEqual(cm.exception.status_code, 400)


if __name__ == "__main__":
    unittest.main()
