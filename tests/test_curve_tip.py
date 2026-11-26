"""Curve axis labels + tap/hover tooltip (v1.6.3 work).

Regression: 2026-11-26, a card sparkline labelled the middle gridline
-1.86% (its -bound label was hard-pinned at top:34px, right above the
zero line) while the bottom edge had no label at all. Fix: -bound moves
to the bottom edge, an explicit 0.00% label sits on the zero line.
Feature: tapping (touch) or hovering (mouse) a curve shows the nearest
point as time + change (e.g. 10:24 +1.21%); series carry [ts, pct] only,
so no NAV is ever shown.
"""
import json
import os
import subprocess
import tempfile
import unittest
from pathlib import Path

# Local REPO_ROOT (not tests._common: that imports app, which needs httpx).
REPO_ROOT = Path(__file__).resolve().parent.parent

HTML = (REPO_ROOT / "static" / "index.html").read_text()
NODE = """const fs = require("fs");
const html = fs.readFileSync(__HTML_PATH__, "utf8");
function fn(name) {
  const s = html.indexOf("function " + name);
  const e = html.indexOf("\\n}\\n", s) + 3;
  return html.slice(s, e);
}
const src = fn("fmtHM") + fn("fmtDiff") + fn("ensureChartTip") + fn("bindChartTip");
eval(src);
function fakeEl() {
  const el = { style: {}, children: [], _handlers: {}, isConnected: false, textContent: "",
    appendChild(c) { c.isConnected = true; this.children.push(c); return c; },
    addEventListener(t, f) { (this._handlers[t] = this._handlers[t] || []).push(f); },
    getBoundingClientRect() { return { left: 0, width: 300 }; } };
  return el;
}
global.document = { createElement: () => fakeEl() };
const box = fakeEl();
const t0 = Date.UTC(2026, 10, 26, 1, 30) / 1000;  // 09:30 Asia/Shanghai
const t1 = Date.UTC(2026, 10, 26, 2, 24) / 1000;  // 10:24
const t2 = Date.UTC(2026, 10, 26, 3, 30) / 1000;  // 11:30
const pts = [[t0, -0.50], [t1, 1.21], [t2, 1.86]];
bindChartTip(box, pts, p => (p[0] - t0) / (t2 - t0) * 100, p => 40 - p[1] * 10,
  p => fmtHM(p[0]) + " " + fmtDiff(p[1]));
const fire = (type, x) => box._handlers[type].forEach(f => f({ clientX: x, stopPropagation() {} }));
const tip = box._tip.tip, dot = box._tip.dot;
fire("pointermove", 135);  // 45% across -> nearest point is 10:24
const out = { move: tip.textContent, moveShown: tip.style.display, dotShown: dot.style.display,
  dotLeft: dot.style.left, dotTop: dot.style.top };
fire("pointerleave", 0);
out.leaveShown = tip.style.display;
fire("pointerdown", 290);  // near the right edge -> last point
out.down = tip.textContent;
console.log(JSON.stringify(out));
"""


class TestAxisLabels(unittest.TestCase):
    def test_min_label_at_bottom_edge(self):
        # SVG is 80px tall, Y(-bound) = 78px; a 10px label sits at 68px.
        self.assertIn(".fund-card .fc-spark .sp-min { top: 68px; }", HTML)
        self.assertNotIn(".fund-card .fc-spark .sp-min { top: 34px; }", HTML)

    def test_zero_label_on_zero_line(self):
        # Zero line Y(0) = 40px; a 10px label sits at 35px.
        self.assertIn(".fund-card .fc-spark .sp-zero", HTML)
        self.assertIn("top: 35px;", HTML)
        self.assertIn('<span class="sp-zero">0.00%</span>', HTML)

    def test_detail_intraday_zero_label(self):
        # Detail intraday zero line is at 78px of 170px; label at 73px.
        self.assertIn(".dt-chart .sp-zero", HTML)
        self.assertIn("top: 73px;", HTML)
        # Detail .sp-min was already correct (bottom: 16px) and untouched.
        self.assertIn(".dt-chart .sp-min { bottom: 16px; }", HTML)


class TestChartTipWiring(unittest.TestCase):
    def test_tip_helper_and_bindings(self):
        self.assertIn("function bindChartTip(box, pts, xPct, yPx, labelOf)", HTML)
        self.assertIn("function bindSparkTips()", HTML)
        self.assertIn("bindSparkTips();", HTML)
        self.assertIn("function sparkGeom(f, published)", HTML)
        self.assertIn("const { pts, vals, bound, t0, t1, X, Y, last } = sparkGeom(f, published);", HTML)
        # Card tip label is time + change; detail intraday + perf the same.
        self.assertIn('p => fmtHM(p[0]) + " " + fmtDiff(p[1])', HTML)
        self.assertIn('p => p[0] + " " + fmtDiff(p[1])', HTML)
        # Tap must not open the detail page; leaving hides the tip.
        self.assertIn('box.addEventListener("click", e => e.stopPropagation());', HTML)
        self.assertIn('box.addEventListener("pointerleave", hide);', HTML)
        self.assertIn('box.addEventListener("pointermove", show);', HTML)
        self.assertIn('box.addEventListener("pointerdown"', HTML)

    def test_tip_nearest_point_live_in_node(self):
        with tempfile.NamedTemporaryFile("w", suffix=".js", delete=False) as f:
            f.write(NODE.replace("__HTML_PATH__", json.dumps(str(REPO_ROOT / "static" / "index.html"))))
            path = f.name
        try:
            env = dict(os.environ, TZ="Asia/Shanghai")
            r = subprocess.run(["node", path], capture_output=True, text=True, env=env, timeout=30)
        finally:
            os.unlink(path)
        self.assertEqual(r.returncode, 0, r.stderr)
        out = json.loads(r.stdout.strip())
        self.assertEqual(out["move"], "10:24 +1.21%")
        self.assertEqual(out["moveShown"], "block")
        self.assertEqual(out["dotShown"], "block")
        self.assertEqual(out["leaveShown"], "none")
        self.assertEqual(out["down"], "11:30 +1.86%")


class TestRenderAxisLabels(unittest.TestCase):
    """Actually render sparklineHtml in node: bound=1.86, asymmetric,
    and the flat-day 0.5% floor."""
    RENDER_NODE = """const fs = require("fs");
const html = fs.readFileSync(__HTML_PATH__, "utf8");
function fn(name) {
  const s = html.indexOf("function " + name);
  const e = html.indexOf("\\n}\\n", s) + 3;
  return html.slice(s, e);
}
const src = ["fmtHM", "fmtDiff", "sparkSegments", "tradingMinutesAt", "sparkGeom", "sparklineHtml"].map(fn).join("\\n");
eval(src);
global.lastTradingDay = () => "2026-11-26";
function render(pts, code) {
  global.historySeries = { [code]: pts };
  return sparklineHtml({ fund_code: code, holdings: [], est_change_pct: null }, false, false);
}
const t0 = 1793000000, t1 = 1793003600, t2 = 1793007200;
const cases = {
  reported: render([[t0, -1.20], [t1, 1.86], [t2, 0.50]], "A"),
  asymmetric: render([[t0, -0.40], [t1, 1.86]], "B"),
  flat: render([[t0, 0.10], [t1, -0.10]], "C"),
};
console.log(JSON.stringify(cases));
"""

    def test_render_bound_186_asymmetric_and_flat(self):
        import json as _json
        with tempfile.NamedTemporaryFile("w", suffix=".js", delete=False) as f:
            f.write(self.RENDER_NODE.replace(
                "__HTML_PATH__", json.dumps(str(REPO_ROOT / "static" / "index.html"))))
            path = f.name
        try:
            env = dict(os.environ, TZ="Asia/Shanghai")
            r = subprocess.run(["node", path], capture_output=True, text=True, env=env, timeout=30)
        finally:
            os.unlink(path)
        self.assertEqual(r.returncode, 0, r.stderr)
        cases = _json.loads(r.stdout.strip())
        for name in ("reported", "asymmetric"):
            h = cases[name]
            self.assertIn("+1.86%", h, name)
            self.assertIn("-1.86%", h, name)
            self.assertIn("0.00%", h, name)
            self.assertIn('y1="40.00"', h, name)  # zero line at the centre
        flat = cases["flat"]
        self.assertIn("+0.50%", flat)
        self.assertIn("-0.50%", flat)
        self.assertIn("0.00%", flat)


if __name__ == "__main__":
    unittest.main()
