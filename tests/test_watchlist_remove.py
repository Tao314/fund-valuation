"""Remove a fund from the watchlist, from its valuation card (v2.1.0).

Covers the remove contract, all per-user, against the reused API
(DELETE /api/watchlist/{id} is not reimplemented):
- deleting your own fund returns 200 and the fund is gone from
  GET /api/watchlist afterwards;
- deleting a missing id is 404; unauthenticated is 401;
- isolation: one user cannot delete another user's fund (404, victim
  untouched);
- served page: every fund card carries a small gray "移除" button that
  stops propagation (card click opens the detail page), asks for
  confirmation naming the fund, and calls DELETE /api/watchlist/{id};
  the success path never calls loadEstimates() (no full refresh, the
  card just disappears via optimisticRemove).

All DB writes go to a temp file (app.DB_PATH patched in setUpModule);
the real fund.db is never touched. TestClient is used without a context
manager so the lifespan background loops never start. Funds are
inserted directly into sqlite so no upstream or background task runs.
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


def _seed(uid, funds):
    """funds: [(code, name, category)]."""
    conn = sqlite3.connect(_TMP_DB)
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
    conn.commit()
    conn.close()


def _funds(client):
    r = client.get("/api/watchlist")
    assert r.status_code == 200, r.text
    return r.json()["funds"]


class TestWatchlistRemove(unittest.TestCase):
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

    def test_delete_own_fund_removes_it(self):
        _seed(self.uid_a, [("000001", "基金一", "全球"),
                           ("000002", "基金二", "国内")])
        fid = _funds(self.ca)[0]["id"]
        r = self.ca.delete(f"/api/watchlist/{fid}")
        self.assertEqual(r.status_code, 200, r.text)
        self.assertEqual([f["fund_code"] for f in _funds(self.ca)],
                         ["000002"])

    def test_delete_missing_is_404(self):
        r = self.ca.delete("/api/watchlist/999999")
        self.assertEqual(r.status_code, 404, r.text)

    def test_delete_requires_login(self):
        anon = TestClient(app.app)
        self.assertEqual(anon.delete("/api/watchlist/1").status_code, 401)

    def test_cross_user_isolation(self):
        _seed(self.uid_a, [("000001", "基金一", "全球")])
        fid_a = _funds(self.ca)[0]["id"]
        self.assertEqual(self.cb.delete(f"/api/watchlist/{fid_a}").status_code,
                         404)
        self.assertEqual(len(_funds(self.ca)), 1)
        self.assertEqual(len(_funds(self.cb)), 0)

    def test_served_page_has_card_remove_button(self):
        html = self.ca.get("/").text
        for marker in ("fc-remove", "removeFund(", "移除</button>",
                       "event.stopPropagation(); removeFund",
                       '"/api/watchlist/" + id',
                       "移除自选基金", "optimisticRemove(id)"):
            self.assertIn(marker, html, marker)
        # Remove button is a small gray tertiary text button, not a red
        # danger button.
        self.assertIn("fc-remove { font-size: 11px; color: #6B7280;", html)
        # Success path never triggers a full valuation refresh: no
        # loadEstimates() anywhere inside removeFund.
        start = html.index("async function removeFund")
        end = html.index("}", html.index("optimisticRemove(id);", start)) + 1
        self.assertNotIn("loadEstimates", html[start:end])


if __name__ == "__main__":
    unittest.main()
