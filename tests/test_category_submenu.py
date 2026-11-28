"""Valuation-page category submenu: backend coverage.

Covers the submenu's data source and the editor's APIs, all per-user:
- GET /api/watchlist returns the ordered category list; per-category fund
  counts are derivable from the returned funds (submenu counts).
- create/rename/delete/reorder validation: empty/overlong/duplicate/
  reserved ("全部", "未分组") names rejected; rename moves the funds;
  delete moves its funds to uncategorized (""), never deletes funds;
  reorder requires the full set and applies the new order.
- Isolation: one user cannot rename/delete/reorder another user's
  categories, and a failed cross-user call leaves the owner's data intact.
- Served page: GET / contains the submenu bar + editor drawer markup.

All DB writes go to a temp file (app.DB_PATH patched in setUpModule);
the real fund.db is never touched. TestClient is used without a context
manager so the lifespan background loops never start. Funds/categories
are inserted directly into sqlite so no upstream or background task runs.
"""
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


class TestCategorySubmenu(unittest.TestCase):
    def setUp(self):
        self.uid_a = _mk_user("subA", "pw123456")
        self.uid_b = _mk_user("subB", "pw123456")
        self.ca = _login("subA", "pw123456")
        self.cb = _login("subB", "pw123456")

    def tearDown(self):
        _wipe(self.uid_a)
        _wipe(self.uid_b)
        conn = sqlite3.connect(_TMP_DB)
        conn.execute("DELETE FROM users WHERE username LIKE 'sub%'")
        conn.commit()
        conn.close()

    def test_watchlist_returns_categories_and_counts(self):
        _seed(self.uid_a, [("全球", 0), ("国内", 1)], [
            ("000001", "基金一", "全球"), ("000002", "基金二", "全球"),
            ("000003", "基金三", "国内"), ("000004", "基金四", ""),
        ])
        r = self.ca.get("/api/watchlist")
        self.assertEqual(r.status_code, 200)
        d = r.json()
        self.assertEqual(d["categories"], ["全球", "国内"])
        counts = {}
        for f in d["funds"]:
            counts[f["category"]] = counts.get(f["category"], 0) + 1
        # Submenu counts: 全部 4, 全球 2, 国内 1, 未分组 1
        self.assertEqual(len(d["funds"]), 4)
        self.assertEqual(counts.get("全球"), 2)
        self.assertEqual(counts.get("国内"), 1)
        self.assertEqual(counts.get(""), 1)

    def test_create_validation(self):
        _seed(self.uid_a, [("全球", 0)], [])
        for bad in ("", "   ", "x" * 21, "全部", "未分组"):
            r = self.ca.post("/api/categories", json={"name": bad})
            self.assertEqual(r.status_code, 400, bad)
        r = self.ca.post("/api/categories", json={"name": "全球"})
        self.assertEqual(r.status_code, 400)
        r = self.ca.post("/api/categories", json={"name": "  新分类  "})
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.json()["name"], "新分类")
        self.assertEqual(self.ca.get("/api/watchlist").json()["categories"],
                         ["全球", "新分类"])

    def test_rename_moves_funds_and_validates(self):
        _seed(self.uid_a, [("全球", 0), ("国内", 1)],
              [("000001", "基金一", "全球")])
        r = self.ca.post("/api/categories/rename",
                         json={"old": "不存在", "new": "海外"})
        self.assertEqual(r.status_code, 404)
        r = self.ca.post("/api/categories/rename",
                         json={"old": "全球", "new": "国内"})
        self.assertEqual(r.status_code, 400)
        for bad in ("", "全部", "未分组", "x" * 21):
            r = self.ca.post("/api/categories/rename",
                             json={"old": "全球", "new": bad})
            self.assertEqual(r.status_code, 400, bad)
        r = self.ca.post("/api/categories/rename",
                         json={"old": "全球", "new": "海外"})
        self.assertEqual(r.status_code, 200)
        d = self.ca.get("/api/watchlist").json()
        self.assertEqual(d["categories"], ["海外", "国内"])
        self.assertEqual(d["funds"][0]["category"], "海外")

    def test_delete_moves_funds_to_uncategorized(self):
        _seed(self.uid_a, [("全球", 0), ("国内", 1)], [
            ("000001", "基金一", "全球"), ("000002", "基金二", "国内"),
        ])
        r = self.ca.post("/api/categories/delete", json={"name": "不存在"})
        self.assertEqual(r.status_code, 404)
        r = self.ca.post("/api/categories/delete", json={"name": "全球"})
        self.assertEqual(r.status_code, 200)
        d = self.ca.get("/api/watchlist").json()
        self.assertEqual(d["categories"], ["国内"])
        self.assertEqual(len(d["funds"]), 2)  # no fund deleted
        by_code = {f["fund_code"]: f["category"] for f in d["funds"]}
        self.assertEqual(by_code["000001"], "")
        self.assertEqual(by_code["000002"], "国内")

    def test_reorder_full_set_only(self):
        _seed(self.uid_a, [("全球", 0), ("国内", 1), ("债券", 2)], [])
        r = self.ca.post("/api/categories/reorder",
                         json={"names": ["国内", "全球"]})
        self.assertEqual(r.status_code, 400)  # partial set rejected
        r = self.ca.post("/api/categories/reorder",
                         json={"names": ["国内", "国内", "债券"]})
        self.assertEqual(r.status_code, 400)  # duplicate rejected
        r = self.ca.post("/api/categories/reorder",
                         json={"names": ["债券", "国内", "全球"]})
        self.assertEqual(r.status_code, 200)
        self.assertEqual(self.ca.get("/api/watchlist").json()["categories"],
                         ["债券", "国内", "全球"])

    def test_cross_user_isolation(self):
        _seed(self.uid_a, [("全球", 0)],
              [("000001", "基金一", "全球")])
        _seed(self.uid_b, [("我的", 0)], [])
        # B cannot rename / delete A's category; A is untouched.
        r = self.cb.post("/api/categories/rename",
                         json={"old": "全球", "new": "海外"})
        self.assertEqual(r.status_code, 404)
        r = self.cb.post("/api/categories/delete", json={"name": "全球"})
        self.assertEqual(r.status_code, 404)
        r = self.cb.post("/api/categories/reorder", json={"names": ["全球"]})
        self.assertEqual(r.status_code, 400)
        d = self.ca.get("/api/watchlist").json()
        self.assertEqual(d["categories"], ["全球"])
        self.assertEqual(d["funds"][0]["category"], "全球")
        # B's own list is independent.
        self.assertEqual(self.cb.get("/api/watchlist").json()["categories"],
                         ["我的"])

    def test_served_page_contains_submenu(self):
        r = self.ca.get("/")
        self.assertEqual(r.status_code, 200)
        html = r.text
        for marker in ('id="cat-pills"', 'id="cat-edit-btn"',
                       'id="cat-overlay"', 'id="cat-draft-list"',
                       "renderCatBar", "cat-save-btn"):
            self.assertIn(marker, html, marker)


if __name__ == "__main__":
    unittest.main()
