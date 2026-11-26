"""Notice bar marquee: seamless loop, no jump (v1.6.6 follow-up).

Regression: 2026-11-26, the v1.6.6 marquee jumped back mid-message.
Two causes: (1) the scrolling copy was built with cloneNode(false), a
shallow clone, so the copy spans were empty and the -50% loop point
landed mid-message; (2) fetchNotice() rewrote the text on every 30s
poll, restarting the animation. Fix: cloneNode(true), duration from the
single-copy width (w/60), and skip the re-fill when message+time are
unchanged. Verified live in Node with a faithful DOM stub (shallow
clone really copies nothing; widths computed from content).

Follow-up (2026-11-26, no version bump): the loop still jumped ~1.9px
at the seam and iOS showed the text enlarged. Causes: (1) the 4 spaces
appended after the timestamp collapse to one space in the original but
are trimmed entirely at the end of the copy, so the copies differed by
3.81px; fixed by a 4px margin-right on .nt-time instead. (2) every
resize rebuilt the copy -- iOS fires resize while scrolling; fixed by
skipping the rebuild when bar width and message are unchanged, and by
only re-setting animationDuration when it changes. (3) iOS
text-size-adjust on the wide track; fixed by pinning 100% on
#notice-bar and font-size 12px on .nt-track/.nt-copy.
"""
import json
import os
import subprocess
import tempfile
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
HTML = (REPO_ROOT / "static" / "index.html").read_text()

NODE = """const fs = require("fs");
const html = fs.readFileSync(__HTML_PATH__, "utf8");
const start = html.indexOf("function fmtNoticeTime");
const end = html.indexOf("function startNoticeTimer()");
const code = html.slice(start, end);
const CHAR = ch => (ch.charCodeAt(0) > 127 ? 14 : ch === " " ? 4 : 7);
function textW(s) { return [...s].reduce((a, c) => a + CHAR(c), 0); }
function makeEl(tag) {
  const el = { tagName: tag || "span", textContent: "", children: [], parent: null,
    style: {}, className: "", clientWidth: 360, _cls: new Set(),
    get scrollWidth() {
      if (this.children.length) return this.children.reduce((a, c) => a + c.scrollWidth, 0);
      return textW(this.textContent) + (this.className.includes("nt-time") ? 12 : 0);  // margin-left 8 + margin-right 4
    },
    classList: { add(c) { el._cls.add(c); }, remove(c) { el._cls.delete(c); },
      contains(c) { return el._cls.has(c); } },
    appendChild(c) { c.parent = el; el.children.push(c); return c; },
    remove() { if (el.parent) { const i = el.parent.children.indexOf(el); if (i >= 0) el.parent.children.splice(i, 1); } },
    querySelectorAll(sel) { return el.children.filter(c => c.className && c.className.includes("nt-copy")); },
    cloneNode(deep) {
      const n = makeEl(el.tagName); n.className = el.className;
      if (deep) { n.textContent = el.textContent; el.children.forEach(c => n.appendChild(c.cloneNode(true))); }
      return n;
    },
    removeAttribute() {} };
  return el;
}
const bar = makeEl("div"); bar.style.display = "none";
const track = makeEl("span");
const textEl = makeEl("span"); const timeEl = makeEl("span");
timeEl.className = "nt-time";
track.appendChild(textEl); track.appendChild(timeEl);
const els = { "notice-bar": bar, "notice-track": track, "notice-text": textEl, "notice-time": timeEl };
global.$ = id => els[id];
global.window = { addEventListener() {} };
global.document = { createElement(tag) { return makeEl(tag); } };
global.requestAnimationFrame = fn => fn();
let apiMsg = null;
global.api = async () => apiMsg;
eval(code);
let durSets = 0, _dur = "";
Object.defineProperty(track, "style", { value: { get animationDuration() { return _dur; }, set animationDuration(v) { if (v !== _dur) { durSets++; _dur = v; } } } });
const _upd = updateNoticeScroll;
let updCalls = 0;
updateNoticeScroll = (...a) => { updCalls++; return _upd(...a); };
function singleWidth() { return textEl.scrollWidth + timeEl.scrollWidth; }
function copies() { return track.children.filter(c => c.className.includes("nt-copy")); }
async function fetch(msg, ts) { apiMsg = { message: msg, created_at: ts }; await fetchNotice(); }
(async () => {
  const ts = Math.floor(new Date("2026-11-26T14:35:00+08:00").getTime() / 1000);
  const LONG = "如果基金估值页没有任何信息显示，请退出后重新登录";
  await fetch(LONG, ts);
  const w = singleWidth();
  const cp = copies();
  const out = { scrolling: bar._cls.has("scrolling"), copies: cp.length,
    copyText: cp.length ? cp[0].children.map(c => c.textContent).join("") : "",
    copyWidth: cp.length ? cp[0].scrollWidth : 0, singleWidth: w,
    trackWidth: track.scrollWidth, duration: track.style.animationDuration,
    timeShown: timeEl.textContent };
  out.callsAfterLong = updCalls;
  out.durSets = durSets;
  await fetch(LONG, ts);
  out.callsAfterRepoll = updCalls;
  out.durationAfterRepoll = track.style.animationDuration;
  out.copiesAfterRepoll = copies().length;
  const cp0 = copies()[0];
  updateNoticeScroll(); updateNoticeScroll();
  out.copiesAfterResize = copies().length;
  out.copySameAfterResize = copies()[0] === cp0;
  out.durSetsAfterResize = durSets;
  out.timeAfterResize = timeEl.textContent;
  await fetch("系统维护中，预计十分钟后恢复，请大家稍等片刻不要着急", ts);
  out.changedScrolling = bar._cls.has("scrolling");
  out.changedSeamless = copies().length === 1 && copies()[0].scrollWidth === singleWidth();
  await fetch("测试消息", ts);
  out.shortScrolling = bar._cls.has("scrolling");
  out.shortCopies = copies().length;
  await fetch(null, ts);
  out.emptyDisplay = bar.style.display;
  console.log(JSON.stringify(out));
})();
"""


def run_node():
    with tempfile.NamedTemporaryFile("w", suffix=".js", delete=False) as f:
        f.write(NODE.replace("__HTML_PATH__", json.dumps(str(REPO_ROOT / "static" / "index.html"))))
        path = f.name
    try:
        env = dict(os.environ, TZ="Asia/Shanghai")
        r = subprocess.run(["node", path], capture_output=True, text=True, env=env, timeout=30)
    finally:
        os.unlink(path)
    assert r.returncode == 0, "node harness failed: " + r.stderr
    return json.loads(r.stdout.strip().splitlines()[-1])


class TestNoticeMarqueeSeamless(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.out = run_node()

    def test_copy_is_deep_clone_identical(self):
        o = self.out
        self.assertTrue(o["scrolling"])
        self.assertEqual(o["copies"], 1)
        # copy carries the full text AND the full timestamp
        self.assertIn("如果基金估值页没有任何信息显示，请退出后重新登录", o["copyText"])
        self.assertIn("14:35", o["copyText"])
        self.assertIn("14:35", o["timeShown"])

    def test_loop_point_is_seamless(self):
        o = self.out
        # copy width == original width and track == 2x single, so the
        # -50% frame is identical to the 0% frame: no jump, no blank gap
        self.assertEqual(o["copyWidth"], o["singleWidth"])
        self.assertEqual(o["trackWidth"], 2 * o["singleWidth"])

    def test_duration_uses_single_width(self):
        o = self.out
        self.assertEqual(o["duration"], "8s")  # w=422 (336 msg + 74 time + 12 time margins), max(8, 422/60)

    def test_poll_does_not_restart_animation(self):
        o = self.out
        self.assertEqual(o["callsAfterRepoll"], o["callsAfterLong"])
        self.assertEqual(o["durationAfterRepoll"], o["duration"])
        self.assertEqual(o["copiesAfterRepoll"], 1)
        self.assertEqual(o["copiesAfterResize"], 1)
        self.assertEqual(o["timeAfterResize"], "11/26 14:35")  # no trailing-space hack
        # resize with unchanged bar width must not rebuild the copy or
        # re-set the duration: the live marquee is left alone
        self.assertTrue(o["copySameAfterResize"])
        self.assertEqual(o["durSetsAfterResize"], o["durSets"])

    def test_changed_and_short_messages(self):
        o = self.out
        self.assertTrue(o["changedScrolling"])
        self.assertTrue(o["changedSeamless"])
        self.assertFalse(o["shortScrolling"])
        self.assertEqual(o["shortCopies"], 0)
        self.assertEqual(o["emptyDisplay"], "none")


class TestNoticeMarqueeSource(unittest.TestCase):
    def test_no_shallow_clone_left(self):
        # the v1.6.6 bug, nailed down: cloneNode(false) copies empty spans
        seg = HTML[HTML.index("function fmtNoticeTime"):HTML.index("function startNoticeTimer()")]
        self.assertIn("cloneNode(true)", seg)
        self.assertNotIn("cloneNode(false)", seg)
        self.assertIn("w / 60", seg)
        self.assertIn("noticeShown.message === d.message", seg)

    def test_font_size_pinned_and_gap_is_margin(self):
        # iOS Safari auto-enlarges text in the ultra-wide track; pin it.
        css = HTML[HTML.index("<style>"):HTML.index("</style>")]
        self.assertIn("-webkit-text-size-adjust: 100%", css)
        self.assertIn("text-size-adjust: 100%", css)
        self.assertIn(".nt-copy { font-size: 12px; }", css)
        self.assertIn("margin-right: 4px", css)
        seg = HTML[HTML.index("function fmtNoticeTime"):HTML.index("function startNoticeTimer()")]
        # trailing spaces after the time are trimmed at the end of the
        # copy, so the two copies measured apart and the loop jumped
        self.assertNotIn('endsWith("    ")', seg)
        self.assertNotIn('+= "    "', seg)
        # resize with unchanged width skips the rebuild
        self.assertIn("noticeScrollW", seg)
