"""US / A-share holiday calendars (item 1).

Regression: 2026-11-26 Thanksgiving was treated as a trading day and
002891 replayed 11-25\'s close as a -1.39% estimate. The calendars are
hard-coded in app.py (NYSE + SSE 2026 notice) and mirrored in
static/index.html; the last test guards the two copies against drift.
Sina gb_ field 3 is a Beijing datetime ("YYYY-MM-DD HH:MM:SS").
"""
import unittest
from datetime import date

from tests._common import REPO_ROOT, app, cn, et


def gb_fields(change="-1.39", quote_time="2026-11-26 05:00:00",
              current="135.00", prev="137.00", ext_price="135.50",
              ext_ts="Nov 25 07:59PM EST"):
    f = ["NVDA"] + [""] * 26
    f[1] = current
    f[2] = change
    f[3] = quote_time
    f[21] = ext_price
    f[24] = ext_ts
    f[26] = prev
    return f


class TestUsSessionHoliday(unittest.TestCase):
    def test_thanksgiving_2026_is_closed_all_day(self):
        self.assertEqual(app.us_session(et(2026, 11, 26, 10, 0)), "closed")
        self.assertEqual(app.us_session(et(2026, 11, 26, 21, 0)), "closed")

    def test_thanksgiving_2027_and_observed_july_3(self):
        self.assertEqual(app.us_session(et(2027, 11, 25, 10, 0)), "closed")
        # 2026-07-04 is a Saturday; NYSE observes it Fri 2026-07-03.
        self.assertEqual(app.us_session(et(2026, 7, 3, 10, 0)), "closed")

    def test_day_before_thanksgiving_still_trades(self):
        self.assertEqual(app.us_session(et(2026, 11, 25, 10, 0)), "trading")

    def test_last_regular_close_skips_thanksgiving(self):
        # Fri 2026-11-27 10:00 ET -> Wed 2026-11-25 16:00 ET.
        close = app._last_regular_close(et(2026, 11, 27, 10, 0))
        self.assertEqual((close.weekday(), close.day, close.hour), (2, 25, 16))
        # Thanksgiving morning itself -> also Wed 11-25 16:00.
        close = app._last_regular_close(et(2026, 11, 26, 10, 0))
        self.assertEqual((close.weekday(), close.day, close.hour), (2, 25, 16))


class TestCnHolidays(unittest.TestCase):
    def test_national_day_2026_closed(self):
        self.assertFalse(app._cn_market_open(cn(2026, 10, 1, 10, 0)))
        self.assertFalse(app._cn_market_open(cn(2026, 10, 7, 10, 0)))

    def test_first_day_back_open(self):
        self.assertTrue(app._cn_market_open(cn(2026, 10, 8, 10, 0)))

    def test_spring_festival_last_day_closed(self):
        self.assertFalse(app._cn_market_open(cn(2026, 2, 23, 10, 0)))
        self.assertTrue(app._cn_market_open(cn(2026, 2, 24, 10, 0)))

    def test_last_cn_trading_day_skips_golden_week(self):
        self.assertEqual(app._last_cn_trading_day(date(2026, 10, 8)),
                         date(2026, 10, 8))
        self.assertEqual(app._last_cn_trading_day(date(2026, 10, 7)),
                         date(2026, 9, 30))
        self.assertEqual(app._last_cn_trading_day(date(2026, 10, 1)),
                         date(2026, 9, 30))

    def test_last_us_trading_day_skips_thanksgiving(self):
        self.assertEqual(app._last_us_trading_day(date(2026, 11, 26)),
                         date(2026, 11, 25))
        self.assertEqual(app._last_us_trading_day(date(2026, 11, 27)),
                         date(2026, 11, 27))


class TestTradingWindowHolidays(unittest.TestCase):
    def test_thanksgiving_night_beijing_is_outside_window(self):
        # 22:00 Beijing 11-26 = 09:00 ET 11-26 (Thanksgiving).
        self.assertFalse(app._in_trading_window(cn(2026, 11, 26, 22, 0)))
        # 02:00 Beijing 11-27 = 13:00 ET 11-26 (Thanksgiving).
        self.assertFalse(app._in_trading_window(cn(2026, 11, 27, 2, 0)))

    def test_normal_us_night_still_in_window(self):
        self.assertTrue(app._in_trading_window(cn(2026, 11, 25, 22, 0)))

    def test_cn_holiday_day_session_is_outside_window(self):
        self.assertFalse(app._in_trading_window(cn(2026, 10, 1, 10, 0)))


class TestUsQuoteHoliday(unittest.TestCase):
    def test_thanksgiving_replay_is_zeroed(self):
        # The reported 002891 case: Sina replays the 11-25 close with
        # 11-25's -1.39% change on Thanksgiving.
        q = app.us_quote_with_ext(gb_fields(), et(2026, 11, 26, 10, 0))
        self.assertEqual(q["change_pct"], 0.0)
        self.assertEqual(q["current"], 137.0)  # carried at prev close
        self.assertIsNone(q["change_pct_ext"])

    def test_stale_quote_is_zeroed(self):
        # Fri 11-27 10:00 ET, quote still stamped with the 11-24
        # session (Beijing 11-25 05:00 = ET 11-24 16:00): stuck feed.
        q = app.us_quote_with_ext(
            gb_fields(quote_time="2026-11-25 05:00:00"),
            et(2026, 11, 27, 10, 0))
        self.assertEqual(q["change_pct"], 0.0)

    def test_preopen_last_close_is_not_stale(self):
        # Tue 11-25 08:00 ET pre-open: the quote IS the 11-24 close
        # (Beijing 11-25 05:00 = ET 11-24 16:00) and must be kept.
        q = app.us_quote_with_ext(
            gb_fields(quote_time="2026-11-25 05:00:00"),
            et(2026, 11, 25, 8, 0))
        self.assertEqual(q["change_pct"], -1.39)

    def test_ref_day_before_open_is_previous_trading_day(self):
        self.assertEqual(app._us_quote_ref_day(et(2026, 11, 27, 8, 0)),
                         date(2026, 11, 25))
        self.assertEqual(app._us_quote_ref_day(et(2026, 11, 27, 10, 0)),
                         date(2026, 11, 27))
        self.assertEqual(app._cn_quote_ref_day(cn(2026, 10, 8, 8, 0)),
                         date(2026, 9, 30))
        self.assertEqual(app._cn_quote_ref_day(cn(2026, 10, 8, 10, 0)),
                         date(2026, 10, 8))


class TestFrontendCalendarMirror(unittest.TestCase):
    def test_frontend_tables_match_backend(self):
        html = (REPO_ROOT / "static" / "index.html").read_text()
        for d in app.US_HOLIDAYS:
            self.assertIn(str(d), html, f"US holiday {d} missing in frontend")
        for d in app.CN_HOLIDAYS:
            self.assertIn(str(d), html, f"CN holiday {d} missing in frontend")
        self.assertIn("us_holiday", html)
        self.assertIn("今日休市", html)
        self.assertIn("休市无估值", html)


if __name__ == "__main__":
    unittest.main()
