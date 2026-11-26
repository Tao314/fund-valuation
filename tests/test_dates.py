"""Date / trading-session boundary tests.

The session helpers are holiday-aware now (US_HOLIDAYS / CN_HOLIDAYS
in app.py, mirrored in static/index.html); the 2026-10-01 National Day
case below used to be an expectedFailure documenting the gap.
What IS guarded today: a session with no quotes must never be read as
a normal trading day at the payload-validation layer (see
test_fallback.py) — an empty intraday payload inside market hours is a
failure, so a holiday can at worst degrade, never fake a normal value.
"""
import unittest

from tests._common import app, cn, et


class TestCnMarketOpen(unittest.TestCase):
    def test_weekday_session(self):
        self.assertTrue(app._cn_market_open(cn(2026, 11, 23, 10, 0)))  # Mon
        self.assertTrue(app._cn_market_open(cn(2026, 11, 23, 9, 30)))
        self.assertTrue(app._cn_market_open(cn(2026, 11, 23, 16, 30)))

    def test_weekday_outside_session(self):
        self.assertFalse(app._cn_market_open(cn(2026, 11, 23, 9, 29)))
        self.assertFalse(app._cn_market_open(cn(2026, 11, 23, 16, 31)))
        self.assertFalse(app._cn_market_open(cn(2026, 11, 23, 8, 0)))
        self.assertFalse(app._cn_market_open(cn(2026, 11, 23, 20, 0)))

    def test_weekend_is_never_open(self):
        self.assertFalse(app._cn_market_open(cn(2026, 11, 21, 10, 0)))  # Sat
        self.assertFalse(app._cn_market_open(cn(2026, 11, 22, 10, 0)))  # Sun

    def test_holiday_weekday_is_not_a_trading_day(self):
        # 2026-10-01 is National Day (Thu): the market is closed.
        # Fixed by the CN_HOLIDAYS calendar (item 1); was expectedFailure.
        self.assertFalse(app._cn_market_open(cn(2026, 10, 1, 10, 0)))


class TestTradingWindow(unittest.TestCase):
    def test_cn_day_session(self):
        self.assertTrue(app._in_trading_window(cn(2026, 11, 23, 10, 0)))
        self.assertTrue(app._in_trading_window(cn(2026, 11, 23, 9, 15)))
        self.assertFalse(app._in_trading_window(cn(2026, 11, 23, 9, 14)))

    def test_us_session_as_seen_from_beijing(self):
        self.assertTrue(app._in_trading_window(cn(2026, 11, 23, 22, 0)))
        self.assertTrue(app._in_trading_window(cn(2026, 11, 24, 2, 0)))
        self.assertFalse(app._in_trading_window(cn(2026, 11, 23, 2, 0)))
        self.assertFalse(app._in_trading_window(cn(2026, 11, 21, 22, 0)))

    def test_weekend_is_outside_window(self):
        self.assertFalse(app._in_trading_window(cn(2026, 11, 21, 10, 0)))
        self.assertFalse(app._in_trading_window(cn(2026, 11, 22, 10, 0)))


class TestUsSession(unittest.TestCase):
    def test_sessions(self):
        self.assertEqual(app.us_session(et(2026, 11, 23, 4, 0)), "pre")
        self.assertEqual(app.us_session(et(2026, 11, 23, 10, 0)), "trading")
        self.assertEqual(app.us_session(et(2026, 11, 23, 17, 0)), "post")
        self.assertEqual(app.us_session(et(2026, 11, 23, 21, 0)), "closed")
        self.assertEqual(app.us_session(et(2026, 11, 21, 10, 0)), "closed")

    def test_last_regular_close_skips_weekend(self):
        # Monday 10:00 ET -> previous Friday 16:00 ET.
        close = app._last_regular_close(et(2026, 11, 23, 10, 0))
        self.assertEqual((close.weekday(), close.hour), (4, 16))
        self.assertEqual(close.day, 20)
        # Monday 17:00 ET -> Monday 16:00 ET itself.
        close = app._last_regular_close(et(2026, 11, 23, 17, 0))
        self.assertEqual((close.weekday(), close.day, close.hour), (0, 23, 16))


if __name__ == "__main__":
    unittest.main()


class TestNextBackupTime(unittest.TestCase):
    def test_before_0100_is_same_day(self):
        self.assertEqual(app._next_backup_time(cn(2026, 11, 27, 0, 15)),
                         cn(2026, 11, 27, 1, 0))
        self.assertEqual(app._next_backup_time(cn(2026, 11, 27, 0, 45)),
                         cn(2026, 11, 27, 1, 0))

    def test_after_0100_is_next_day(self):
        self.assertEqual(app._next_backup_time(cn(2026, 11, 27, 2, 0)),
                         cn(2026, 11, 28, 1, 0))
