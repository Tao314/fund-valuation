"""Full-site button system (ChatGPT scheme, 2026-11-28).

Static contract of the served page, no backend change:
- tiers: primary solid blue #2563EB/600/44px; secondary gray #F3F4F6;
  tertiary transparent gray (link-type blue); danger solid red #DC2626;
- selectors (nav tabs / ranking chips / category pills / sort toggle):
  8px rounded-square (Tristan 2026-11-28: no full-round), unselected gray,
  selected light-blue #DBEAFE + blue #1D4ED8;
- icon buttons 8px square, gray; :hover + :focus-visible present;
- no green left in button/input chrome (semantic greens — up/down,
  status dots, badges — are out of scope and stay);
- no `mini danger` class conflict; settings account card keeps exactly
  one primary (bind), the other two are secondary.

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


class TestButtonSystem(unittest.TestCase):
    def setUp(self):
        self.html = _served_html()

    def test_tiers(self):
        h = self.html
        self.assertIn("button { padding: 9px 16px; border: 0; border-radius: 8px; "
                      "background: #2563EB; color: #fff; font-size: 14px; "
                      "font-weight: 600;", h)
        self.assertIn("button.secondary { background: #F3F4F6; color: #374151;", h)
        self.assertIn("button.tertiary { background: transparent; color: #6B7280;", h)
        self.assertIn("button.tertiary.link { color: #2563EB; }", h)
        self.assertIn("button.danger { background: #DC2626; color: #fff;", h)
        self.assertIn("button:hover { background: #1D4ED8; }", h)
        self.assertIn("button:focus-visible { outline: 2px solid #2563EB;", h)

    def test_selectors_light_blue_8px(self):
        h = self.html
        for rule in (".tab-btn.active { background: #DBEAFE; color: #1D4ED8;",
                     ".chip.active { background: #DBEAFE; color: #1D4ED8;",
                     ".cat-pill.active { background: #DBEAFE; color: #1D4ED8;",
                     ".sort-toggle button.active { background: #DBEAFE; color: #1D4ED8;"):
            self.assertIn(rule, h, rule)
        for rule in (".tab-btn {", ".chip {", ".cat-pill {", ".sort-toggle button {"):
            start = h.index(rule)
            self.assertIn("border-radius: 8px;", h[start:start + 400], rule)

    def test_no_mini_danger_conflict(self):
        self.assertNotIn("mini danger", self.html)
        self.assertIn("button.mini.danger { background: #DC2626;", self.html)

    def test_no_green_in_button_chrome(self):
        h = self.html
        self.assertNotIn("border-color: #07845B", h)
        self.assertNotIn("#D9E4DC", h)
        self.assertNotIn("background: #FBFDFC", h)
        # search-result disabled must read as disabled, not selected
        self.assertIn(".search-results button:disabled { background: #F2F3F5; "
                      "color: #98A2B3; }", h)

    def test_tertiary_instances(self):
        h = self.html
        self.assertIn('<button class="tertiary link" id="auth-forgot">', h)
        self.assertIn('<button class="tertiary link" id="auth-switch">', h)
        self.assertIn('<button class="tertiary" id="notice-clear-btn">', h)
        self.assertIn('<button class="tertiary" id="logout-btn"', h)
        self.assertIn('<button class="tertiary" id="cat-drawer-close"', h)

    def test_settings_one_primary(self):
        h = self.html
        self.assertIn('<button id="set-bind-btn">绑定邮箱</button>', h)
        self.assertIn('<button class="secondary" id="change-username-btn">', h)
        self.assertIn('<button class="secondary" id="change-password-btn">', h)

    def test_sort_toggle_uses_selector_class(self):
        h = self.html
        self.assertIn('$("sort-default").className = mode === "default" ? "active" : "";', h)
        self.assertNotIn('className = "secondary" + (mode', h)

    def test_icon_buttons_square_gray(self):
        h = self.html
        self.assertIn("#refresh-btn { width: 40px; height: 40px;", h)
        self.assertIn(".cat-edit-btn { flex: none; width: 40px; height: 40px;", h)
        # round icon button is gone (50% radius survives only on status dots)
        start = h.index(".cat-edit-btn {")
        self.assertIn("border-radius: 8px;", h[start:start + 300])

    def test_rank_add_is_small_primary(self):
        h = self.html
        self.assertIn(".rk-add { flex: none; min-height: 28px; height: 28px;", h)
        self.assertIn("background: #2563EB; color: #fff; font-weight: 600;", h)


if __name__ == "__main__":
    unittest.main()
