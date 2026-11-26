"""TrueNAV 1.4 experiment 0: PIT backtest infrastructure (pure, no network).

Covers: multi-quarter jjcc table parsing (current 9-col and historical
7-col layouts), disclosure-availability / holdings-asof logic, metric
percentiles, walk-forward folds + held-out tail, regime/type grouping.
"""
import sys
import unittest
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "tools"))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import backtest_truenav as btn  # noqa: E402


def _jjcc_doc():
    """One jjcc response containing two quarterly tables: 2026 Q3 in the
    current 9-col layout and 2026 Q2 in the historical 7-col layout."""
    def h4(label, date_str):
        return ("<div class='box'><div class='boxitem w790'><h4 class='t'>"
                f"<label class='left'>基金  {label}股票投资明细</label>"
                f"<label class='right lab2 xq505'>截止至：<font class='px12'>{date_str}</font>"
                "</label></h4>")
    head9 = ("<table><thead><tr><th>序号</th><th>股票代码</th><th>股票名称</th>"
             "<th>最新价</th><th>涨跌幅</th><th>相关资讯</th><th>占净值<br />比例</th>"
             "<th>持股数<br />（万股）</th><th>持仓市值<br />（万元）</th></tr></thead><tbody>")
    head7 = ("<table><thead><tr><th>序号</th><th>股票代码</th><th>股票名称</th>"
             "<th>相关资讯</th><th>占净值<br />比例</th>"
             "<th>持股数<br />（万股）</th><th>持仓市值<br />（万元）</th></tr></thead><tbody>")
    rows9 = (
        "<tr><td>1</td><td>300408</td><td>新股A</td><td>10.0</td><td>1.0%</td>"
        "<td>股吧</td><td>7.96%</td><td>1.0</td><td>100.0</td></tr>"
        "<tr><td>2</td><td>301377</td><td>新股B</td><td>20.0</td><td>2.0%</td>"
        "<td>股吧</td><td>7.59%</td><td>2.0</td><td>200.0</td></tr></tbody></table>")
    rows7 = (
        "<tr><td>1</td><td>300502</td><td>老股A</td><td>股吧</td>"
        "<td>10.25%</td><td>6.43</td><td>2770.56</td></tr>"
        "<tr><td>2</td><td>300308</td><td>老股B</td><td>股吧</td>"
        "<td>9.70%</td><td>4.30</td><td>2623.00</td></tr></tbody></table>")
    content = (h4("2026年3季度", "2026-09-30") + head9 + rows9
               + h4("2026年2季度", "2026-06-30") + head7 + rows7)
    return 'var apidata={ content:"' + content + '",arryear:[2026,2025],curyear:2026};'


def _snaps():
    return [
        {"report_label": "2025年4季度", "report_date": "2025-12-31",
         "holdings": [{"code": "000001", "name": "A", "weight": 5.0}]},
        {"report_label": "2026年1季度", "report_date": "2026-03-31",
         "holdings": [{"code": "000002", "name": "B", "weight": 6.0}]},
    ]


class ParseHoldingsTablesTest(unittest.TestCase):
    def test_two_quarters_both_layouts(self):
        snaps = btn.parse_holdings_tables(_jjcc_doc())
        self.assertEqual([s["report_date"] for s in snaps],
                         ["2026-06-30", "2026-09-30"])
        q2, q3 = snaps
        self.assertEqual(q2["report_label"], "2026年2季度")
        self.assertEqual([(h["code"], h["weight"]) for h in q2["holdings"]],
                         [("300502", 10.25), ("300308", 9.70)])
        self.assertEqual(q3["report_label"], "2026年3季度")
        self.assertEqual([(h["code"], h["weight"]) for h in q3["holdings"]],
                         [("300408", 7.96), ("301377", 7.59)])


class AvailabilityTest(unittest.TestCase):
    def test_deadline_based(self):
        self.assertEqual(btn.available_on("2026-03-31"), date(2026, 5, 1))
        self.assertEqual(btn.available_on("2026-06-30"), date(2026, 9, 1))
        self.assertEqual(btn.available_on("2026-09-30"), date(2026, 11, 1))
        self.assertEqual(btn.available_on("2025-12-31"), date(2026, 4, 1))

    def test_holdings_asof_never_backfills(self):
        snaps = _snaps()
        self.assertIsNone(btn.holdings_asof(snaps, date(2026, 3, 1)))
        self.assertEqual(btn.holdings_asof(snaps, date(2026, 4, 15))["report_date"],
                         "2025-12-31")
        self.assertEqual(btn.holdings_asof(snaps, date(2026, 5, 1))["report_date"],
                         "2026-03-31")


class MetricsTest(unittest.TestCase):
    def test_percentiles(self):
        m = btn.metrics2([(0.1, 0.0), (0.2, 0.0), (0.3, 0.0), (0.4, 0.0)])
        self.assertAlmostEqual(m["mae"], 0.25)
        self.assertAlmostEqual(m["p50"], 0.25)
        self.assertAlmostEqual(m["p95"], 0.385)

    def test_empty(self):
        self.assertEqual(btn.metrics2([]), {"n": 0})


class WalkForwardTest(unittest.TestCase):
    def _triples(self, n=100):
        # Perfect estimates: MAE must be 0 in every fold and in the tail.
        return [{"date": f"2026-01-{(i % 28) + 1:02d}", "est_old": 0.5,
                 "est_new": 0.5, "official": 0.5, "vintage": "2025-12-31",
                 "stale_pos": False} for i in range(n)]

    def test_folds_cover_test_days_and_tail_is_last_20pct(self):
        wf = btn.walk_forward(self._triples())
        # 100 triples, MIN_N=20 -> 80 test days.
        self.assertEqual(wf["1.2"]["n"], 80)
        self.assertEqual(sum(f["n"] for f in wf["1.2"]["folds"]), 80)
        self.assertEqual([f["n"] for f in wf["1.2"]["folds"]], [26, 27, 27])
        self.assertEqual(wf["1.2"]["tail"]["n"], 16)
        self.assertAlmostEqual(wf["1.2"]["mae"], 0.0)


class SplitTest(unittest.TestCase):
    def test_regime(self):
        self.assertEqual(btn.regime(0.1), "平静<=0.5%")
        self.assertEqual(btn.regime(-1.5), "常规0.5-2%")
        self.assertEqual(btn.regime(3.0), "剧烈>2%")

    def test_type_group(self):
        self.assertEqual(btn.type_group("QDII-混合偏股"), "QDII")
        self.assertEqual(btn.type_group("债券型-长债"), "债券")
        self.assertEqual(btn.type_group("指数型-股票"), "指数")
        self.assertEqual(btn.type_group("混合型-偏股"), "主动权益")
        self.assertEqual(btn.type_group(None), "未知")


if __name__ == "__main__":
    unittest.main()
