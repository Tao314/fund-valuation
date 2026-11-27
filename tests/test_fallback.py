"""Upstream-failure / fallback-chain tests.

Two rules are pinned here:

1. A 200 with an empty/throttled payload is a FAILURE, not a success
   (_validate_upstream_payload), and it is recorded as one
   (upstream_get -> _record_source) so the 数据接口 page and the
   数据流异常 judgement see it. Callers get the response back, but
   nothing downstream may treat it as a normal value.
2. fetch_daily_closes degrades in order (Eastmoney kline -> Yahoo
   daily closes -> stale DB cache) and never returns a fabricated
   price: when every source fails, the caller gets the old cache,
   labelled stale by its age, not a fresh-looking number.

All DB writes in this module go to a temp file (app.DB_PATH is
patched in setUpModule); the real fund.db is never touched.
"""
import json
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

import httpx

from tests._common import FakeResp, app, cn


def _em_nav_url(page=1):
    return ("https://api.fund.eastmoney.com/f10/lsjz?fundCode=000001"
            f"&pageIndex={page}&pageSize=20")


def _em_trends_url(secid="1.600000"):
    return ("https://push2his.eastmoney.com/api/qt/stock/trends2/get"
            f"?secid={secid}")


class _FailingClient:
    def __init__(self, *a, **k):
        pass

    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        return False

    async def get(self, url, headers=None, **kwargs):
        raise httpx.ConnectTimeout("boom")


class _EmptyClient(_FailingClient):
    async def get(self, url, headers=None, **kwargs):
        return FakeResp(text="", content=b"")


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


class TestValidateUpstreamPayload(unittest.TestCase):
    def test_empty_body_is_failure(self):
        ok, err = app._validate_upstream_payload(
            _em_nav_url(), FakeResp(text="", content=b""))
        self.assertFalse(ok)
        self.assertTrue(err)

    def test_whitespace_body_is_failure(self):
        ok, _ = app._validate_upstream_payload(
            _em_nav_url(), FakeResp(text="   ", content=b"   "))
        self.assertFalse(ok)

    def test_bad_json_is_failure(self):
        ok, err = app._validate_upstream_payload(
            _em_nav_url(), FakeResp(text="not json", content=b"not json"))
        self.assertFalse(ok)
        self.assertIn("JSON", err)

    def test_valid_nav_payload(self):
        body = json.dumps({"Data": {"LSJZList": [{"FSRQ": "2026-11-24", "DWJZ": "1.23"}]}})
        ok, err = app._validate_upstream_payload(_em_nav_url(), FakeResp(text=body, content=body.encode()))
        self.assertTrue(ok)
        self.assertIsNone(err)

    def test_nav_empty_page_2_is_legitimate(self):
        body = json.dumps({"Data": {"LSJZList": []}})
        ok, _ = app._validate_upstream_payload(
            _em_nav_url(page=2), FakeResp(text=body, content=body.encode()))
        self.assertTrue(ok)

    def test_nav_empty_page_1_is_failure(self):
        body = json.dumps({"Data": {"LSJZList": []}})
        ok, _ = app._validate_upstream_payload(
            _em_nav_url(page=1), FakeResp(text=body, content=body.encode()))
        self.assertFalse(ok)

    def test_search_no_hits_is_legitimate(self):
        body = json.dumps({"Datas": []})
        ok, _ = app._validate_upstream_payload(
            "https://fundsuggest.eastmoney.com/FundSearch/api/FundSearchAPI.ashx?q=zzz",
            FakeResp(text=body, content=body.encode()))
        self.assertTrue(ok)

    def test_trends_empty_in_session_is_failure(self):
        body = json.dumps({"data": {"trends": []}})
        ok, _ = app._validate_upstream_payload(
            _em_trends_url(), FakeResp(text=body, content=body.encode()),
            now=cn(2026, 11, 23, 10, 0))  # Mon 10:00: market open
        self.assertFalse(ok)

    def test_trends_empty_off_session_is_legitimate(self):
        body = json.dumps({"data": {"trends": []}})
        resp = FakeResp(text=body, content=body.encode())
        self.assertTrue(app._validate_upstream_payload(
            _em_trends_url(), resp, now=cn(2026, 11, 23, 8, 0))[0])
        self.assertTrue(app._validate_upstream_payload(
            _em_trends_url(), resp, now=cn(2026, 11, 21, 10, 0))[0])  # Sat

    def test_trends_empty_on_holiday_weekday_is_legitimate(self):
        # CN_HOLIDAYS calendar (item 1): 2026-10-01 National Day is
        # off-session, so an empty intraday payload is expected, not
        # a failure. (Was test_trends_empty_on_holiday_weekday_is_failure
        # before the calendar landed.)
        body = json.dumps({"data": {"trends": []}})
        ok, _ = app._validate_upstream_payload(
            _em_trends_url(), FakeResp(text=body, content=body.encode()),
            now=cn(2026, 10, 1, 10, 0))
        self.assertTrue(ok)

    def test_sina_empty_quotes_is_failure(self):
        ok, _ = app._validate_upstream_payload(
            "https://hq.sinajs.cn/list=sh600000",
            FakeResp(text='var hq_str_sh600000="";', content=b'x'))
        self.assertFalse(ok)

    def test_sina_valid_quotes(self):
        ok, _ = app._validate_upstream_payload(
            "https://hq.sinajs.cn/list=sh600000",
            FakeResp(text='var hq_str_sh600000="浦发银行,10.0,9.9,10.1";',
                     content=b'x'))
        self.assertTrue(ok)

    def test_yahoo_empty_result_is_failure(self):
        body = json.dumps({"chart": {"result": []}})
        ok, _ = app._validate_upstream_payload(
            "https://query1.finance.yahoo.com/v8/finance/chart/00700.HK",
            FakeResp(text=body, content=body.encode()))
        self.assertFalse(ok)

    def test_fuyao_nonzero_code_is_failure(self):
        body = json.dumps({"code": 1, "message": "no data"})
        ok, err = app._validate_upstream_payload(
            "https://fuyao.aicubes.cn/performance/nav?code=000001",
            FakeResp(text=body, content=body.encode()))
        self.assertFalse(ok)
        self.assertIn("code=1", err)

    def test_rank_without_allrecords_is_failure(self):
        ok, _ = app._validate_upstream_payload(
            "https://fund.eastmoney.com/data/rankhandler.aspx?op=ph",
            FakeResp(text="var rankData={};", content=b"x"))
        self.assertFalse(ok)

    def test_validator_never_raises(self):
        ok, err = app._validate_upstream_payload(_em_nav_url(), object())
        self.assertTrue(ok)
        self.assertIsNone(err)


class TestUpstreamGetRecording(unittest.IsolatedAsyncioTestCase):
    async def test_connect_failure_raises_and_is_recorded(self):
        url = "https://hq.sinajs.cn/list=sh600000"
        before = (app._source_stats.get("sina") or {}).get("fail", 0)
        with patch.object(app, "_http_client", lambda: _FailingClient()):
            with self.assertRaises(httpx.ConnectTimeout):
                await app.upstream_get(url)
        after = (app._source_stats.get("sina") or {}).get("fail", 0)
        self.assertEqual(after, before + 1)
        self.assertTrue(app._source_stats["sina"]["last_error"])

    async def test_200_empty_is_returned_but_recorded_as_failure(self):
        url = "https://hq.sinajs.cn/list=sh600000"
        before = (app._source_stats.get("sina") or {}).get("fail", 0)
        with patch.object(app, "_http_client", lambda: _EmptyClient()):
            resp = await app.upstream_get(url)
        self.assertIsNotNone(resp)
        after = (app._source_stats.get("sina") or {}).get("fail", 0)
        self.assertEqual(after, before + 1)


class TestDailyClosesFallback(unittest.IsolatedAsyncioTestCase):
    SECID = "1.00700"
    STALE = {"2026-11-20": 340.0, "2026-11-21": 342.0, "2026-11-24": 344.0}

    def _seed_stale_cache(self):
        with app.db() as conn:
            conn.executemany(
                "INSERT OR REPLACE INTO stock_daily_cache(secid, date, close, fetched_at)"
                " VALUES(?,?,?,?)",
                [(self.SECID, d, c, time.time() - 10 * 86400)
                 for d, c in self.STALE.items()])

    async def test_em_down_falls_back_to_yahoo(self):
        self._seed_stale_cache()
        with patch.object(app, "FUYAO_ENABLED", False), \
                patch.object(app, "upstream_get",
                             new=AsyncMock(side_effect=Exception("em down"))), \
                patch.object(app, "_fetch_yahoo_daily_closes",
                             new=AsyncMock(return_value={"2026-11-25": 345.0})):
            out = await app.fetch_daily_closes(self.SECID, days=5)
        self.assertEqual(out["2026-11-25"], 345.0)
        self.assertEqual(out["2026-11-24"], 344.0)  # stale rows kept
        with app.db() as conn:
            row = conn.execute(
                "SELECT close FROM stock_daily_cache WHERE secid=? AND date=?",
                (self.SECID, "2026-11-25")).fetchone()
        self.assertIsNotNone(row)
        self.assertEqual(row["close"], 345.0)

    async def test_all_sources_down_returns_stale_cache(self):
        self._seed_stale_cache()
        with patch.object(app, "FUYAO_ENABLED", False), \
                patch.object(app, "upstream_get",
                             new=AsyncMock(side_effect=Exception("em down"))), \
                patch.object(app, "_fetch_yahoo_daily_closes",
                             new=AsyncMock(side_effect=Exception("yahoo down"))):
            out = await app.fetch_daily_closes(self.SECID, days=5)
        self.assertEqual(out, self.STALE)


if __name__ == "__main__":
    unittest.main()
