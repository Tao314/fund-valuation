"""Login password visibility toggle (eye icon on the login password box).

Covers the static contract of the toggle, all client-side, no backend change:
- exactly one toggle, on the login #password only (register / reset /
  change-password inputs are untouched);
- default state is masked (type=password, closed-eye icon shown, open-eye hidden);
- the handler only flips input type (password <-> text), never the value;
- mousedown is swallowed so clicking the eye never blurs the password field;
- inline SVG only, no external icon library.

The served page is fetched with TestClient (temp DB, as in the other
frontend-marker tests); the real fund.db is never touched.
"""
import re
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


def _served_html():
    c = TestClient(app.app)
    r = c.get("/")
    assert r.status_code == 200, r.text
    return r.text


class TestPasswordToggle(unittest.TestCase):
    def test_served_page_contains_toggle(self):
        html = _served_html()
        for marker in ('id="pwd-toggle"', 'class="pwd-toggle"',
                       'type="button" class="pwd-toggle"',
                       'class="pwd-wrap"', 'class="eye-closed"',
                       'class="eye-open"', 'aria-pressed="false"',
                       'aria-label="显示密码"'):
            self.assertIn(marker, html, marker)

    def test_default_state_is_masked(self):
        html = _served_html()
        self.assertIn('id="password" type="password"', html)
        # Closed eye visible, open eye hidden by default.
        self.assertIn('class="eye-open" style="display:none"', html)
        m = re.search(r'class="eye-closed"[^>]*style="display:none"', html)
        self.assertIsNone(m, "closed eye must be visible by default")

    def test_toggle_only_on_login_password(self):
        html = _served_html()
        self.assertEqual(html.count('id="pwd-toggle"'), 1)
        self.assertEqual(html.count('class="pwd-wrap"'), 1)
        m = re.search(r'<div class="pwd-wrap">(.*?)</div>', html, re.S)
        self.assertIsNotNone(m, "pwd-wrap not found")
        self.assertIn('id="password"', m.group(1))
        for other in ('id="reg-password"', 'id="reset-password"',
                      'id="new-password"', 'id="old-password"',
                      'id="confirm-password"'):
            self.assertIn(other, html, other)

    def test_toggle_logic_only_flips_type(self):
        html = _served_html()
        m = re.search(r'\$\("pwd-toggle"\)\.onclick = \(\) => \{(.*?)\n\};', html, re.S)
        self.assertIsNotNone(m, "toggle onclick handler not found")
        body = m.group(1)
        self.assertIn('pw.type = show ? "text" : "password";', body)
        self.assertNotIn(".value", body, "toggle must never touch the value")
        self.assertNotIn("loadEstimates", body)
        self.assertIn('aria-pressed', body)
        self.assertIn('隐藏密码', body)
        # Clicking the eye must not blur the password field.
        self.assertIn('addEventListener("mousedown", e => e.preventDefault())', html)

    def test_no_external_icon_library(self):
        html = _served_html()
        for banned in ("font-awesome", "iconfont", "fonts.googleapis",
                       "cdn.jsdelivr.net/npm/@fortawesome"):
            self.assertNotIn(banned, html, banned)


if __name__ == "__main__":
    unittest.main()
