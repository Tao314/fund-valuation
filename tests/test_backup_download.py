"""Direct latest-backup download endpoint tests.

GET /api/backup/download?latest=1 — admin session cookie only.
Covers: unauthenticated 401, non-admin 403, admin downloads the newest
.db.gz byte-identical to the file on disk (application/gzip, filename in
Content-Disposition), latest != 1 rejected, no backup -> 404, and a newer
legacy plain .db is never served (only .db.gz).

All DB writes go to a temp file (app.DB_PATH patched in setUpModule)
and backup files live in a temp BACKUP_DIR; the real fund.db and real
backups are never touched. TestClient is used without a context manager
so the lifespan background loops never start.
"""
import gzip
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from fastapi.testclient import TestClient

from tests._common import app

_TMPDIR = None
_TMP_DB = None
_BACKUP_DIR = None


def setUpModule():
    global _TMPDIR, _TMP_DB, _BACKUP_DIR
    _TMPDIR = tempfile.TemporaryDirectory()
    _TMP_DB = Path(_TMPDIR.name) / "test.db"
    _BACKUP_DIR = Path(_TMPDIR.name) / "backups"
    _BACKUP_DIR.mkdir()
    patch.object(app, "DB_PATH", _TMP_DB).start()
    patch.object(app, "BACKUP_DIR", _BACKUP_DIR).start()
    app.init_db()


def tearDownModule():
    patch.stopall()
    _TMPDIR.cleanup()


def _mk_user(username, password, is_admin=False):
    conn = sqlite3.connect(_TMP_DB)
    cur = conn.execute(
        "INSERT INTO users(username, password_hash, created_at, is_admin) "
        "VALUES(?,?,?,?)",
        (username, app.hash_password(password), "2026-11-26T00:00:00",
         1 if is_admin else 0))
    uid = cur.lastrowid
    conn.commit()
    conn.close()
    return uid


def _login(username, password):
    c = TestClient(app.app)
    r = c.post("/api/login", json={"username": username, "password": password})
    assert r.status_code == 200, r.text
    return c


def _put_backup(name, data: bytes):
    (_BACKUP_DIR / name).write_bytes(data)


class TestBackupDownload(unittest.TestCase):
    def setUp(self):
        for p in _BACKUP_DIR.iterdir():
            p.unlink()

    def test_unauthenticated_401(self):
        c = TestClient(app.app)
        self.assertEqual(c.get("/api/backup/download?latest=1").status_code, 401)

    def test_non_admin_403(self):
        _mk_user("plain1", "pw123456")
        c = _login("plain1", "pw123456")
        self.assertEqual(c.get("/api/backup/download?latest=1").status_code, 403)

    def test_admin_downloads_newest_gz_byte_identical(self):
        _put_backup("fund-backup-20261127-120000.db.gz",
                    gzip.compress(b"older-backup"))
        newest = gzip.compress(b"newest-backup-bytes")
        _put_backup("fund-backup-20261127-130000.db.gz", newest)
        # A newer legacy plain .db must never be served by this endpoint.
        _put_backup("fund-backup-20261127-140000.db", b"legacy-plain-db")
        _mk_user("allen314", "pw123456", is_admin=True)
        c = _login("allen314", "pw123456")
        r = c.get("/api/backup/download?latest=1")
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.content, newest)
        self.assertEqual(r.headers["content-type"], "application/gzip")
        self.assertIn("fund-backup-20261127-130000.db.gz",
                      r.headers["content-disposition"])

    def test_latest_other_than_1_rejected(self):
        _mk_user("admin2", "pw123456", is_admin=True)
        c = _login("admin2", "pw123456")
        self.assertEqual(c.get("/api/backup/download?latest=2").status_code, 400)

    def test_no_backup_404(self):
        _mk_user("admin3", "pw123456", is_admin=True)
        c = _login("admin3", "pw123456")
        self.assertEqual(c.get("/api/backup/download?latest=1").status_code, 404)
