"""Ranking-data normalization tests.

parse_rank_rows is the single choke point for rankhandler text: short
rows are dropped, unparseable numbers become None instead of raising.
These tests pin that contract and the rule every consumer must follow:
a row is only board-ready when code/name/date/nav/daily are all
present — anything else must be filtered out before sorting, never
mixed into the board. Also pins the v1.6.0 page tolerance in
fetch_returns_all: a failed later page yields a partial board, a
failed first page still raises.
"""
import unittest
from unittest.mock import AsyncMock, patch

from tests._common import FakeResp, app


def _row(code="000001", name="华夏成长", date="2026-11-24", nav="1.2345",
         daily="0.50", w1="1.20", m1="2.30", m3="3.40", m6="4.50",
         y1="5.60", ytd="0.90"):
    fields = [code, name, "x", date, nav, "x", daily, w1, m1, m3, m6, y1,
              "x", "x", ytd, "x"]
    return '"' + ",".join(fields) + '"'


def _is_board_ready(row):
    return all(row[k] is not None for k in ("code", "name", "date", "nav", "daily"))


class TestParseRankRows(unittest.TestCase):
    def test_valid_rows_parsed(self):
        rows = app.parse_rank_rows(_row() + "," + _row("000002", "易方达蓝筹", daily="-1.25"))
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["code"], "000001")
        self.assertEqual(rows[0]["name"], "华夏成长")
        self.assertEqual(rows[0]["date"], "2026-11-24")
        self.assertAlmostEqual(rows[0]["nav"], 1.2345)
        self.assertAlmostEqual(rows[0]["daily"], 0.50)
        self.assertAlmostEqual(rows[1]["daily"], -1.25)
        self.assertAlmostEqual(rows[0]["ytd"], 0.90)

    def test_short_row_dropped(self):
        text = '"000001,华夏成长,x,2026-11-24,1.23"'  # <16 fields
        self.assertEqual(app.parse_rank_rows(text), [])

    def test_empty_and_garbage_text(self):
        self.assertEqual(app.parse_rank_rows(""), [])
        self.assertEqual(app.parse_rank_rows("allRecords:0;"), [])

    def test_bad_numbers_become_none_not_crash(self):
        rows = app.parse_rank_rows(_row(nav="--", daily="null"))
        self.assertEqual(len(rows), 1)
        self.assertIsNone(rows[0]["nav"])
        self.assertIsNone(rows[0]["daily"])
        self.assertFalse(_is_board_ready(rows[0]))

    def test_incomplete_rows_filtered_before_sort(self):
        rows = app.parse_rank_rows(
            _row(daily="2.00") + "," + _row("000002", daily="--") + ","
            + _row("000003", daily="1.00"))
        ready = [r for r in rows if _is_board_ready(r)]
        ready.sort(key=lambda r: r["daily"], reverse=True)
        self.assertEqual([r["code"] for r in ready], ["000001", "000003"])
        # Sorting the unfiltered rows would mix a None daily into the board.
        self.assertEqual(len(rows), 3)


class TestFetchReturnsAll(unittest.IsolatedAsyncioTestCase):
    async def test_failed_later_page_serves_partial_board(self):
        async def fake_get(url, **kw):
            if "pi=1" in url:
                return FakeResp(text="allRecords:12000;" + _row() + "," + _row("000002"))
            if "pi=2" in url:
                raise ConnectionError("boom")
            return FakeResp(text=_row("000003"))

        with patch.object(app, "upstream_get", new=AsyncMock(side_effect=fake_get)):
            rows = await app.fetch_returns_all("all")
        self.assertEqual([r["code"] for r in rows], ["000001", "000002", "000003"])

    async def test_failed_first_page_still_raises(self):
        async def fake_get(url, **kw):
            raise ConnectionError("boom")

        with patch.object(app, "upstream_get", new=AsyncMock(side_effect=fake_get)):
            with self.assertRaises(ValueError):
                await app.fetch_returns_all("all")

    async def test_empty_board_raises(self):
        async def fake_get(url, **kw):
            return FakeResp(text="allRecords:0;")

        with patch.object(app, "upstream_get", new=AsyncMock(side_effect=fake_get)):
            with self.assertRaises(ValueError):
                await app.fetch_returns_all("all")


if __name__ == "__main__":
    unittest.main()
