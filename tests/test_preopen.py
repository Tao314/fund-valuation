"""CN pre-open: no estimate on the card (2026-11-27).

Regression: 2026-11-27 09:12, fund 024481 showed -0.02% (est NAV 1.0286
off yesterday's 1.0288) before the open. The raw estimate was 0.00%
(Sina quotes at the previous close) and the beta calibration added the
fund's alpha (≈ -0.02). The curve already gated this (_preopen_no_us);
the card did not, so card and curve contradicted each other.
Fix: _compute_estimates / _estimate_one reuse _preopen_no_us and send
est_change_pct / est_nav null + cn_preopen; the card shows yesterday's
NAV + 未开盘·暂无估值 instead of an estimate row.
All DB writes go to a temp file; upstream fetchers are mocked.
"""
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from tests._common import REPO_ROOT, app, cn, frozen_time

# 024481-shaped fixture: one A-share holding, quote flat at prev close.
HOLDINGS = [{"code": "600519", "name": "贵州茅台", "weight": 10.0}]
US_HOLDINGS = [{"code": "AAPL", "name": "苹果", "weight": 10.0}]
QUOTES = {"sh600519": {"name": "贵州茅台", "prev_close": 1500.0,
                       "current": 1500.0, "change_pct": 0.0,
                       "quote_time": "2026-11-26 15:00"},
          "gb_AAPL": {"name": "苹果", "prev_close": 270.0,
                      "current": 266.25, "change_pct": -1.39,
                      "quote_time": "2026-11-26 16:00"}}
NAV = {"nav_date": "2026-11-26", "dwjz": 1.0288, "jzzzl": 0.12,
       "prev_nav_date": "2026-11-25", "prev_dwjz": 1.0276,
       "prev_jzzzl": 0.05, "stale": False}
BETA_ROW = {"fund_code": "024481", "alpha": -0.02, "beta": 1.0, "n": 60,
            "mae_raw": 0.5, "mae_cal": 0.4, "updated_at": 9999999999}

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
            "INSERT INTO user_funds(user_id, fund_code, fund_name, sort_order, created_at)"
            " VALUES(1,'024481','测试基金',0,'2026-11-26')")


def tearDownModule():
    patch.stopall()
    _TMPDIR.cleanup()


class _MockUpstream:
    """Mocked fetchers + beta row for _compute_estimates / _estimate_one."""
    holdings = HOLDINGS
    ftype = "混合型基金"

    def __init__(self):
        self._saved = {}
        for name in ("fetch_nav", "fetch_holdings", "fetch_stock_market_map",
                     "holding_quote_symbol", "fetch_stock_position",
                     "fetch_quotes", "fetch_fund_type", "fetch_industry",
                     "_beta_row", "_ensure_beta"):
            self._saved[name] = getattr(app, name)

    def __enter__(self):
        async def fake_nav(code, *a):
            return dict(NAV)

        async def fake_holdings(code):
            return {"holdings": [dict(h) for h in self.holdings],
                    "report_label": "2026年三季报",
                    "report_date": "2026-09-30", "stale": False}

        async def fake_mmap(code):
            return {}

        async def fake_sym(h, mmap):
            return {"600519": "sh600519", "AAPL": "gb_AAPL"}[h["code"]]

        async def fake_position(code):
            return 10.0

        async def fake_quotes(symbols):
            return dict(QUOTES)

        async def fake_ftype(code):
            return self.ftype

        async def fake_industry(code):
            return "酿酒行业"

        app.fetch_nav = fake_nav
        app.fetch_holdings = fake_holdings
        app.fetch_stock_market_map = fake_mmap
        app.holding_quote_symbol = fake_sym
        app.fetch_stock_position = fake_position
        app.fetch_quotes = fake_quotes
        app.fetch_fund_type = fake_ftype
        app.fetch_industry = fake_industry
        app._beta_row = lambda code: dict(BETA_ROW)
        app._ensure_beta = lambda code: None
        return self

    def __exit__(self, *exc):
        for name, fn in self._saved.items():
            setattr(app, name, fn)
        return False


def _freeze(year, month, day, hour, minute):
    return frozen_time(cn(year, month, day, hour, minute))


class TestPreopenNoUs(unittest.TestCase):
    def test_weekday_0912_no_us_is_preopen(self):
        self.assertTrue(app._preopen_no_us(cn(2026, 11, 27, 9, 12), HOLDINGS))

    def test_weekday_0912_with_us_is_not_preopen(self):
        self.assertFalse(app._preopen_no_us(cn(2026, 11, 27, 9, 12), US_HOLDINGS))

    def test_empty_holdings_count_as_non_us(self):
        self.assertTrue(app._preopen_no_us(cn(2026, 11, 27, 9, 12), []))

    def test_at_0930_is_not_preopen(self):
        self.assertFalse(app._preopen_no_us(cn(2026, 11, 27, 9, 30), HOLDINGS))

    def test_weekend_is_not_preopen(self):
        self.assertFalse(app._preopen_no_us(cn(2026, 11, 28, 9, 12), HOLDINGS))


class TestComputeEstimatesPreopen(unittest.IsolatedAsyncioTestCase):
    async def test_preopen_sends_no_estimate(self):
        # Reported case: raw 0.00% + alpha -0.02 must not become -0.02%.
        with _MockUpstream(), _freeze(2026, 11, 27, 9, 12):
            payload = await app._compute_estimates(1)
        (fund,) = payload["funds"]
        self.assertTrue(fund["cn_preopen"])
        self.assertIsNone(fund["est_change_pct"])
        self.assertIsNone(fund["est_nav"])
        self.assertEqual(fund["dwjz"], 1.0288)

    async def test_same_inputs_at_1000_show_calibrated_estimate(self):
        # Control: after the open the same quotes give alpha + 0.00%.
        with _MockUpstream(), _freeze(2026, 11, 27, 10, 0):
            payload = await app._compute_estimates(1)
        (fund,) = payload["funds"]
        self.assertFalse(fund["cn_preopen"])
        self.assertEqual(fund["est_change_pct"], -0.02)
        self.assertEqual(fund["est_nav"], 1.0286)

    async def test_us_holdings_still_estimate_preopen(self):
        # US overnight session is real data — the gate must not fire.
        # Frozen on 2026-11-24 09:12 Beijing (= ET 2026-11-23 20:12, a
        # normal US session day): 2026-11-27 09:12 Beijing is ET
        # Thanksgiving evening, when an all-US fund is 休市无估值
        # instead (see test_us_holdings_no_estimate_on_us_holiday).
        with _MockUpstream() as m, _freeze(2026, 11, 24, 9, 12):
            m.holdings = US_HOLDINGS
            payload = await app._compute_estimates(1)
        (fund,) = payload["funds"]
        self.assertFalse(fund["cn_preopen"])
        self.assertFalse(fund["us_holiday"])
        self.assertEqual(fund["est_change_pct"], -0.16)

    async def test_us_holdings_no_estimate_on_us_holiday(self):
        # Beijing 2026-11-27 09:12 is still Thanksgiving in ET: an
        # all-US fund has no session, so no estimate is computed even
        # though the (mocked) US quotes carry a move.
        with _MockUpstream() as m, _freeze(2026, 11, 27, 9, 12):
            m.holdings = US_HOLDINGS
            payload = await app._compute_estimates(1)
        (fund,) = payload["funds"]
        self.assertTrue(fund["us_holiday"])
        self.assertIsNone(fund["est_change_pct"])
        self.assertIsNone(fund["est_nav"])


class TestEstimateOnePreopen(unittest.IsolatedAsyncioTestCase):
    async def test_preopen_sends_no_estimate(self):
        with _MockUpstream(), _freeze(2026, 11, 27, 9, 12):
            est = await app._estimate_one("024481", "测试基金")
        self.assertTrue(est["cn_preopen"])
        self.assertIsNone(est["est_change_pct"])
        self.assertIsNone(est["est_nav"])

    async def test_open_shows_calibrated_estimate(self):
        with _MockUpstream(), _freeze(2026, 11, 27, 10, 0):
            est = await app._estimate_one("024481", "测试基金")
        self.assertFalse(est["cn_preopen"])
        self.assertEqual(est["est_change_pct"], -0.02)
        self.assertEqual(est["est_nav"], 1.0286)


class TestFrontendPreopen(unittest.TestCase):
    def test_card_shows_no_estimate_note(self):
        html = (REPO_ROOT / "static" / "index.html").read_text()
        self.assertIn("cn_preopen", html)
        self.assertIn("未开盘·暂无估值", html)


if __name__ == "__main__":
    unittest.main()
