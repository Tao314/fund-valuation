"""iOS keyboard covering the focused input (2026-11-28).

Static contract of the served page, no backend change:
- viewport meta carries interactive-widget=resizes-content, so the
  keyboard resizes the layout instead of covering it;
- a keepInputVisible() helper scrolls the focused input into the
  visible area on focusin, again 300ms later (keyboard animation),
  and on every visualViewport resize while an input stays focused;
- runCatQuickSearch re-asserts input visibility after results render.

The served page is fetched with TestClient (temp DB, as in the other
frontend-marker tests); the real fund.db is never touched.
"""
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


class TestKeyboardViewport(unittest.TestCase):
    def setUp(self):
        self.html = _served_html()

    def test_viewport_resizes_for_keyboard(self):
        self.assertIn(
            '<meta name="viewport" content="width=device-width, '
            'initial-scale=1, interactive-widget=resizes-content">',
            self.html)

    def test_keep_input_visible_helper(self):
        h = self.html
        for marker in ("function keepInputVisible(input)",
                       'input.scrollIntoView({ block: "center", behavior: "smooth" })',
                       'document.addEventListener("focusin"',
                       'window.visualViewport.addEventListener("resize"',
                       "setTimeout(() => keepInputVisible(t), 300)",
                       "keepInputVisible(q);  // results grew below the input"):
            self.assertIn(marker, h, marker)


if __name__ == "__main__":
    unittest.main()
