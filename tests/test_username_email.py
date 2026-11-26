"""Username rename + email-login tests.

Covers: rename rejected without a bound email, rename ok with email,
rename keeps the current session, duplicate rename rejected, username
login rejected once an email is bound, email login ok, username login
still ok without an email, and the send-code -> bind flow (existing
/api/email/send-code purpose=bind + /api/email/bind, code captured by
patching _send_code_mail so no SMTP is needed).

All DB writes go to a temp file (app.DB_PATH patched in setUpModule);
the real fund.db is never touched. TestClient is used without a
context manager so the lifespan background loops never start.
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
_CODES = {}


def _capture_mail(email, purpose, code):
    _CODES[(email, purpose)] = code


def setUpModule():
    global _TMPDIR, _TMP_DB
    _TMPDIR = tempfile.TemporaryDirectory()
    _TMP_DB = Path(_TMPDIR.name) / "test.db"
    patch.object(app, "DB_PATH", _TMP_DB).start()
    patch.object(app, "EMAIL_AUTH", True).start()
    patch.object(app, "_send_code_mail", _capture_mail).start()
    app.init_db()


def tearDownModule():
    patch.stopall()
    _TMPDIR.cleanup()


def _mk_user(username, password, email=None):
    conn = sqlite3.connect(_TMP_DB)
    cur = conn.execute(
        "INSERT INTO users(username, password_hash, created_at, email) "
        "VALUES(?,?,?,?)",
        (username, app.hash_password(password), "2026-11-26T00:00:00", email))
    uid = cur.lastrowid
    conn.commit()
    conn.close()
    return uid


def _login(ident, password):
    c = TestClient(app.app)
    r = c.post("/api/login", json={"username": ident, "password": password})
    return c, r


def _sessions(uid):
    conn = sqlite3.connect(_TMP_DB)
    n = conn.execute(
        "SELECT COUNT(*) FROM sessions WHERE user_id=?", (uid,)).fetchone()[0]
    conn.close()
    return n


class TestRename(unittest.TestCase):
    def test_rename_rejected_without_email(self):
        _mk_user("noemail1", "pw123456")
        c, r = _login("noemail1", "pw123456")
        self.assertEqual(r.status_code, 200)
        r = c.post("/api/username/change", json={"username": "noemail2"})
        self.assertEqual(r.status_code, 400)
        me = c.get("/api/me").json()
        self.assertEqual(me["username"], "noemail1")

    def test_rename_ok_and_session_survives(self):
        uid = _mk_user("bob1", "pw123456", "bob1@example.com")
        c, r = _login("bob1@example.com", "pw123456")
        self.assertEqual(r.status_code, 200)
        self.assertEqual(_sessions(uid), 1)
        r = c.post("/api/username/change", json={"username": "bobby1"})
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.json()["username"], "bobby1")
        # Same client, same cookie: still logged in, new name visible.
        me = c.get("/api/me")
        self.assertEqual(me.status_code, 200)
        self.assertEqual(me.json()["username"], "bobby1")
        self.assertEqual(_sessions(uid), 1)

    def test_rename_duplicate_rejected(self):
        _mk_user("dup-a", "pw123456", "dupa@example.com")
        _mk_user("dup-b", "pw123456", "dupb@example.com")
        c, r = _login("dupa@example.com", "pw123456")
        self.assertEqual(r.status_code, 200)
        r = c.post("/api/username/change", json={"username": "dup-b"})
        self.assertEqual(r.status_code, 409)

    def test_rename_invalid_name_rejected(self):
        _mk_user("short1", "pw123456", "short1@example.com")
        c, r = _login("short1@example.com", "pw123456")
        self.assertEqual(r.status_code, 200)
        self.assertEqual(
            c.post("/api/username/change", json={"username": "ab"}).status_code,
            400)

    def test_rename_unauthenticated(self):
        c = TestClient(app.app)
        self.assertEqual(
            c.post("/api/username/change",
                   json={"username": "whoever"}).status_code, 401)


class TestLoginRouting(unittest.TestCase):
    def test_bound_email_user_username_login_rejected(self):
        _mk_user("carol1", "pw123456", "carol1@example.com")
        _, r = _login("carol1", "pw123456")
        self.assertEqual(r.status_code, 401)
        self.assertIn("邮箱", r.json()["detail"])

    def test_bound_email_user_email_login_ok(self):
        _mk_user("dave1", "pw123456", "dave1@example.com")
        _, r = _login("dave1@example.com", "pw123456")
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.json()["username"], "dave1")
        # Wrong password via email still fails generically.
        _, r = _login("dave1@example.com", "wrongpw")
        self.assertEqual(r.status_code, 401)

    def test_no_email_user_username_login_ok(self):
        _mk_user("erin1", "pw123456")
        _, r = _login("erin1", "pw123456")
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.json()["username"], "erin1")

    def test_unknown_identifier_rejected(self):
        _, r = _login("ghost1", "pw123456")
        self.assertEqual(r.status_code, 401)


class TestBindFlow(unittest.TestCase):
    def test_send_code_bind_and_login_switch(self):
        uid = _mk_user("frank1", "pw123456")
        c, r = _login("frank1", "pw123456")
        self.assertEqual(r.status_code, 200)
        email = "frank1@example.com"
        r = c.post("/api/email/send-code",
                   json={"email": email, "purpose": "bind"})
        self.assertEqual(r.status_code, 200)
        code = _CODES[(email, "bind")]
        # Wrong code fails, correct code binds.
        r = c.post("/api/email/bind",
                   json={"email": email, "code": "000000"})
        self.assertEqual(r.status_code, 400)
        r = c.post("/api/email/bind", json={"email": email, "code": code})
        self.assertEqual(r.status_code, 200)
        self.assertEqual(c.get("/api/me").json()["email"], email)
        # Already bound: second bind refused.
        self.assertEqual(
            c.post("/api/email/bind",
                   json={"email": "other@example.com",
                         "code": "000000"}).status_code, 400)
        # Login routing flips: username now refused, email works, and the
        # pre-bind session is still valid.
        self.assertEqual(c.get("/api/me").status_code, 200)
        _, r = _login("frank1", "pw123456")
        self.assertEqual(r.status_code, 401)
        _, r = _login(email, "pw123456")
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.json()["username"], "frank1")
        self.assertEqual(_sessions(uid), 2)

    def test_send_code_bind_rejected_when_already_bound(self):
        _mk_user("grace1", "pw123456", "grace1@example.com")
        c, r = _login("grace1@example.com", "pw123456")
        self.assertEqual(r.status_code, 200)
        r = c.post("/api/email/send-code",
                   json={"email": "new@example.com", "purpose": "bind"})
        self.assertEqual(r.status_code, 400)


if __name__ == "__main__":
    unittest.main()
