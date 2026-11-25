"""TrueNAV 1.2 core regression tests (pure functions, no network/DB).

Every expected number below was hand-computed from the formulas in
app.py and cross-checked once against the live code before being pinned
here. If a value in app.py changes, these tests must fail — that is the
point: a silent drift in the estimate, the beta calibration or the
credibility tier must never reach main unnoticed.
"""
import unittest
from datetime import datetime

from tests._common import CN_TZ, app

# Fixed (est, actual) pairs used for the OLS beta pin.
PAIRS = [(1.0, 1.2), (2.0, 2.1), (-1.0, -0.8), (0.5, 0.6), (3.0, 2.9)]
# Hand-computed OLS for PAIRS: beta = 8.6/9.2, alpha = 1.2 - beta*1.1.
BETA = 8.6 / 9.2
ALPHA = 1.2 - BETA * 1.1


class TestLiveEstimate(unittest.TestCase):
    HOLDINGS = [
        {"weight": 40.0},
        {"weight": 30.0},
        {"weight": 20.0},
        {"weight": 10.0},
    ]
    SYMS = ["sh600000", "sz000001", "hk00700", "gb_NVDA"]
    QUOTES = {
        "sh600000": {"change_pct": 2.0},
        "sz000001": {"change_pct": -1.0},
        "hk00700": {"change_pct": 0.5},
        "gb_NVDA": {"change_pct": 4.0},
    }

    def test_full_coverage_weighted_sum(self):
        # (40*2 + 30*-1 + 20*0.5 + 10*4) / 100 = 1.0; residual 0
        # (stock_position 95 < coverage 100).
        est, coverage = app._live_est_pct(
            self.HOLDINGS, self.SYMS, self.QUOTES, 95.0
        )
        self.assertAlmostEqual(est, 1.0)
        self.assertAlmostEqual(coverage, 100.0)

    def test_residual_fill_moves_at_disclosed_average(self):
        # Top-3 only: coverage 90, residual 10 (position 100 - 90).
        # Disclosed weighted change = 60, avg = 60/90; est = 0.6 + 10*avg/100.
        est, coverage = app._live_est_pct(
            self.HOLDINGS[:3], self.SYMS[:3], self.QUOTES, 100.0
        )
        self.assertAlmostEqual(est, 2.0 / 3.0)
        self.assertAlmostEqual(coverage, 90.0)

    def test_missing_quote_is_not_renormalized(self):
        # Only sh600000 quoted: est = 40*2/100 = 0.8. Coverage still
        # counts the unquoted weight (100), so the estimate is a
        # partial-coverage number, never silently rescaled to 100%.
        est, coverage = app._live_est_pct(
            self.HOLDINGS, self.SYMS,
            {"sh600000": {"change_pct": 2.0}}, None,
        )
        self.assertAlmostEqual(est, 0.8)
        self.assertAlmostEqual(coverage, 100.0)

    def test_residual_pct_floor_and_unknown_position(self):
        self.assertEqual(app._residual_pct(90.0, 100.0), 10.0)
        self.assertEqual(app._residual_pct(100.0, 95.0), 0.0)
        self.assertEqual(app._residual_pct(90.0, None), 0.0)


class TestFundCurve(unittest.TestCase):
    def test_two_buckets_and_forward_fill(self):
        t0 = int(datetime(2026, 11, 25, 9, 30, tzinfo=CN_TZ).timestamp())
        holdings = [{"code": "A", "weight": 50.0}, {"code": "B", "weight": 50.0}]
        trends = {
            "A": {"pre_close": 10.0, "points": [(t0, 10.2), (t0 + 300, 10.5)]},
            "B": {"pre_close": 20.0, "points": [(t0, 19.8)]},
        }
        curve = app._fund_curve(holdings, trends, t0)
        # t0: 50*2/100 + 50*(-1)/100 = 0.5; t0+300: B forward-filled at
        # -1%, A at 5% -> 2.5 - 0.5 = 2.0.
        self.assertEqual(curve, [[(t0 // 300) * 300, 0.5],
                                 [((t0 + 300) // 300) * 300, 2.0]])

    def test_curve_residual_fill(self):
        t0 = int(datetime(2026, 11, 25, 9, 30, tzinfo=CN_TZ).timestamp())
        holdings = [{"code": "A", "weight": 50.0}]
        trends = {"A": {"pre_close": 10.0, "points": [(t0, 10.2)]}}
        # Disclosed 50% at +2% = 1.0; residual 10% at the same +2% = 0.2.
        curve = app._fund_curve(holdings, trends, t0, residual_pct=10.0)
        self.assertEqual(curve, [[(t0 // 300) * 300, 1.2]])

    def test_curve_empty_without_trends(self):
        t0 = int(datetime(2026, 11, 25, 9, 30, tzinfo=CN_TZ).timestamp())
        self.assertEqual(
            app._fund_curve([{"code": "A", "weight": 50.0}], {}, t0), [])


class TestBetaCalibration(unittest.TestCase):
    def test_ols_beta_fixed_pairs(self):
        alpha, beta = app._ols_beta(PAIRS)
        self.assertAlmostEqual(alpha, ALPHA, places=9)
        self.assertAlmostEqual(beta, BETA, places=9)

    def test_ols_beta_clamps(self):
        # Flat estimates (sxx = 0) -> beta 1.0, alpha clamped to 0.5.
        self.assertEqual(app._ols_beta([(1.0, 10.0), (1.0, 20.0)]), (0.5, 1.0))
        # Perfectly negative slope -> beta clamped to 0.5, alpha to -0.5.
        self.assertEqual(
            app._ols_beta([(0.0, 0.0), (1.0, -1.0), (2.0, -2.0)]), (-0.5, 0.5))

    def test_apply_beta(self):
        self.assertAlmostEqual(
            app._apply_beta(2.0, {"alpha": 0.1, "beta": 0.9}), 1.9)
        self.assertEqual(app._apply_beta(2.0, None), 2.0)
        self.assertEqual(app._apply_beta(2.0, {"alpha": None, "beta": 0.9}), 2.0)

    def test_calibrated_mae_beats_raw_mae(self):
        raw_mae = sum(abs(a - e) for e, a in PAIRS) / len(PAIRS)
        cal_mae = sum(abs(a - (ALPHA + BETA * e)) for e, a in PAIRS) / len(PAIRS)
        self.assertAlmostEqual(raw_mae, 0.14)
        self.assertLess(cal_mae, raw_mae)
        self.assertAlmostEqual(cal_mae, 0.0609, places=3)

    def test_credibility_tiers(self):
        self.assertEqual(app._credibility({"mae_cal": 0.3}), "高")
        self.assertEqual(app._credibility({"mae_cal": 0.31}), "中")
        self.assertEqual(app._credibility({"mae_cal": 0.6}), "中")
        self.assertEqual(app._credibility({"mae_cal": 0.61}), "低")
        self.assertEqual(app._credibility({"mae_raw": 0.5}), "中")
        self.assertIsNone(app._credibility({"mae_cal": None, "mae_raw": None}))
        self.assertIsNone(app._credibility(None))


class TestDailyChanges(unittest.TestCase):
    def test_changes_vs_previous_trading_day(self):
        out = app._daily_changes({
            "2026-11-20": 10.0, "2026-11-21": 11.0, "2026-11-24": 9.9})
        self.assertAlmostEqual(out["2026-11-21"], 10.0)
        self.assertAlmostEqual(out["2026-11-24"], -10.0)

    def test_zero_previous_close_skipped(self):
        out = app._daily_changes({"2026-11-20": 0.0, "2026-11-21": 11.0})
        self.assertEqual(out, {})


if __name__ == "__main__":
    unittest.main()
