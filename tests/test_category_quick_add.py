"""Per-category quick add ("添加自选" under each category's fund list).

Covers the quick-add contract, all per-user, against the reused APIs
(POST /api/watchlist + GET /api/search are not reimplemented):
- add into a named category lands in that category (auto-created), at the
  end of the category (max sort_order);
- adding while browsing "全部" lands in uncategorized (""); the literal
  reserved names ("全部", "未分组") are rejected as real categories by
  both add and set_category, so they can never become real rows;
- duplicate add is refused (409) and leaves exactly one row;
- isolation: one user cannot delete / re-categorize another user's fund,
  and the same code may live in both users' lists independently;
- served page: the quick-add button + collapsible card markup exists, the
  target mapping is 全部 -> "" (uncategorized), and the success path of
  catQuickAdd never calls loadEstimates() (no full valuation refresh).

All DB writes go to a temp file (app.DB_PATH patched in setUpModule);
the real fund.db is never touched. TestClient is used without a context
manager so the lifespan background loops never start. Funds/categories
are inserted directly into sqlite so no upstream or background task runs.
"""
import re
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from fastapi.testclient import TestClient

from tests._common import app

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


def _mk_user(username, password):
    conn = sqlite3.connect(_TMP_DB)
    cur = conn.execute(
        "INSERT INTO users(username, password_hash, created_at) "
        "VALUES(?,?,?)",
        (username, app.hash_password(password), "2026-11-26T00:00:00"))
    uid = cur.lastrowid
    conn.commit()
    conn.close()
    return uid


def _login(username, password):
    c = TestClient(app.app)
    r = c.post("/api/login", json={"username": username, "password": password})
    assert r.status_code == 200, r.text
    return c


def _seed(uid, categories, funds):
    """categories: [(name, sort_order)]; funds: [(code, name, category)]."""
    conn = sqlite3.connect(_TMP_DB)
    for name, order in categories:
        conn.execute(
            "INSERT INTO user_categories(user_id, name, sort_order) "
            "VALUES(?,?,?)", (uid, name, order))
    for i, (code, name, cat) in enumerate(funds):
        conn.execute(
            "INSERT INTO user_funds(user_id, fund_code, fund_name, "
            "sort_order, category, created_at) VALUES(?,?,?,?,?,?)",
            (uid, code, name, i, cat, "2026-11-26T00:00:00"))
    conn.commit()
    conn.close()


def _wipe(uid):
    conn = sqlite3.connect(_TMP_DB)
    conn.execute("DELETE FROM user_funds WHERE user_id=?", (uid,))
    conn.execute("DELETE FROM user_categories WHERE user_id=?", (uid,))
    conn.commit()
    conn.close()


def _funds(client):
    r = client.get("/api/watchlist")
    assert r.status_code == 200, r.text
    return r.json()["funds"]


class TestCategoryQuickAdd(unittest.TestCase):
    def setUp(self):
        self.uid_a = _mk_user("qaA", "pw123456")
        self.uid_b = _mk_user("qaB", "pw123456")
        self.ca = _login("qaA", "pw123456")
        self.cb = _login("qaB", "pw123456")

    def tearDown(self):
        _wipe(self.uid_a)
        _wipe(self.uid_b)
        conn = sqlite3.connect(_TMP_DB)
        conn.execute("DELETE FROM users WHERE username LIKE 'qa%'")
        conn.commit()
        conn.close()

    def test_add_into_named_category_lands_last(self):
        _seed(self.uid_a, [("全球", 0)], [("000001", "基金一", "全球")])
        r = self.ca.post("/api/watchlist", json={
            "fund_code": "000002", "fund_name": "基金二", "category": "全球"})
        self.assertEqual(r.status_code, 200, r.text)
        self.assertEqual(r.json()["category"], "全球")
        funds = _funds(self.ca)
        self.assertEqual([f["fund_code"] for f in funds], ["000001", "000002"])
        self.assertEqual(funds[-1]["category"], "全球")

    def test_add_new_category_auto_created(self):
        r = self.ca.post("/api/watchlist", json={
            "fund_code": "000003", "fund_name": "基金三", "category": "债券"})
        self.assertEqual(r.status_code, 200, r.text)
        d = self.ca.get("/api/watchlist").json()
        self.assertIn("债券", d["categories"])
        self.assertEqual(d["funds"][0]["category"], "债券")

    def test_add_in_all_goes_uncategorized_and_reserved_rejected(self):
        # Frontend maps browsing "全部" to category "" (see served-page
        # test); "" is the uncategorized bucket.
        r = self.ca.post("/api/watchlist", json={
            "fund_code": "000001", "fund_name": "基金一", "category": ""})
        self.assertEqual(r.status_code, 200, r.text)
        self.assertEqual(r.json()["category"], "")
        # Literal reserved names must never become real categories.
        for bad in ("全部", "未分组"):
            r = self.ca.post("/api/watchlist", json={
                "fund_code": "000002", "fund_name": "基金二", "category": bad})
            self.assertEqual(r.status_code, 400, bad)
        d = self.ca.get("/api/watchlist").json()
        self.assertEqual(len(d["funds"]), 1)
        self.assertEqual(d["categories"], [])

    def test_set_category_reserved_rejected(self):
        _seed(self.uid_a, [("全球", 0)], [("000001", "基金一", "全球")])
        fid = _funds(self.ca)[0]["id"]
        for bad in ("全部", "未分组"):
            r = self.ca.post(f"/api/watchlist/{fid}/category",
                             json={"category": bad})
            self.assertEqual(r.status_code, 400, bad)
        self.assertEqual(_funds(self.ca)[0]["category"], "全球")

    def test_duplicate_add_refused(self):
        r = self.ca.post("/api/watchlist", json={
            "fund_code": "000001", "fund_name": "基金一", "category": "全球"})
        self.assertEqual(r.status_code, 200, r.text)
        r = self.ca.post("/api/watchlist", json={
            "fund_code": "000001", "fund_name": "基金一", "category": "国内"})
        self.assertEqual(r.status_code, 409, r.text)
        funds = _funds(self.ca)
        self.assertEqual(len(funds), 1)
        self.assertEqual(funds[0]["category"], "全球")  # first add wins

    def test_cross_user_isolation(self):
        r = self.ca.post("/api/watchlist", json={
            "fund_code": "000001", "fund_name": "基金一", "category": "全球"})
        self.assertEqual(r.status_code, 200, r.text)
        fid_a = r.json()["id"]
        # Same code in B's own list is fine and independent.
        r = self.cb.post("/api/watchlist", json={
            "fund_code": "000001", "fund_name": "基金一", "category": "全球"})
        self.assertEqual(r.status_code, 200, r.text)
        # B cannot delete or re-categorize A's fund; A is untouched.
        self.assertEqual(self.cb.delete(f"/api/watchlist/{fid_a}").status_code, 404)
        self.assertEqual(self.cb.post(
            f"/api/watchlist/{fid_a}/category",
            json={"category": "国内"}).status_code, 404)
        funds_a = _funds(self.ca)
        self.assertEqual(len(funds_a), 1)
        self.assertEqual(funds_a[0]["category"], "全球")
        self.assertEqual(len(_funds(self.cb)), 1)

    def test_served_page_contains_quick_add(self):
        r = self.ca.get("/")
        self.assertEqual(r.status_code, 200)
        html = r.text
        for marker in ('id="cat-quick-add-btn"', 'id="cat-quick-add-card"',
                       'id="cat-quick-search-q"', 'id="cat-quick-results"',
                       "catTargetCategory", "catQuickAdd",
                       "runCatQuickSearch", '"/api/search?q="',
                       '"/api/watchlist"'):
            self.assertIn(marker, html, marker)
        # Browsing "全部" (activeCat null) adds into 未分组 ("").
        self.assertIn('return activeCat === null ? "" : activeCat;', html)
        # Success path never triggers a full valuation refresh: no
        # loadEstimates() anywhere inside catQuickAdd.
        m = re.search(r"async function catQuickAdd\(.*?\n\}\n", html, re.S)
        self.assertIsNotNone(m, "catQuickAdd not found")
        self.assertNotIn("loadEstimates", m.group(0))


if __name__ == "__main__":
    unittest.main()
