#!/usr/bin/env python3
"""TrueNAV algorithm-version backtest (v1.4.0).

Computes MAE for the three TrueNAV algorithm versions on the SAME
120-day window and the SAME walk-forward test days, so the numbers are
directly comparable (used by the 演变 page credibility curve):

  TrueNAV 1.0  est_old = Σ(weight% × stock_change%) / 100      (top-10 only)
  TrueNAV 1.1  est_new = est_old + residual fill               (P0)
  TrueNAV 1.2  α + β·est_new, β fit walk-forward on prior ≤60 pairs (P1)

Test-day selection is identical to tools/backtest_beta.py: a day is a
test day when at least 20 prior (est_new, official) pairs exist; the
regression uses only prior pairs (no look-ahead in the fit). Holdings
are still today's top-10 applied to the whole window (same look-ahead
limitation as the P0/P1 baselines).

Run:  venv/bin/python tools/backtest_truenav.py [--days 120]
Output: stdout table + tools/truenav-backtest.json + appended section
in backtest-report.md. Shares /tmp/backtest_cache.json with backtest.py.
"""
import argparse
import asyncio
import json
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import app  # noqa: E402
import backtest as bt  # noqa: E402  (repoints app.DB_PATH, helpers, cache)

FUNDS = bt.FUNDS
REPORT_PATH = bt.REPORT_PATH
JSON_PATH = Path(__file__).resolve().parent / "truenav-backtest.json"
WINDOW = 60
MIN_N = 20


async def dated_triples(code: str, days: int) -> list:
    """[(date, est_old, est_new, official)] ascending by date."""
    nav_rows = await bt.fetch_nav_history(code, days)  # newest first
    hold = await app.fetch_holdings(code)
    holdings = hold["holdings"]
    mmap = await app.fetch_stock_market_map(code)
    stock_position = await app.fetch_stock_position(code)
    coverage = sum(h["weight"] for h in holdings)
    residual = app._residual_pct(coverage, stock_position)
    changes_by_code = {}
    for h in holdings:
        closes = await bt.holding_closes(h, mmap)
        if closes:
            changes_by_code[h["code"]] = bt.daily_changes(closes)
    triples = []
    for date, official in nav_rows:
        num, quoted_w = 0.0, 0.0
        for h in holdings:
            chg = changes_by_code.get(h["code"], {}).get(date)
            if chg is not None:
                num += h["weight"] * chg
                quoted_w += h["weight"]
        if coverage <= 0 or quoted_w < 0.5 * coverage:
            continue
        est_old = num / 100
        est_new = est_old + (residual * (num / quoted_w) / 100 if quoted_w else 0.0)
        triples.append((date, est_old, est_new, official))
    triples.sort()
    return triples


def walk_forward(triples: list) -> dict:
    """Same test days for all three versions; β fit on prior est_new pairs."""
    p10, p11, p12 = [], [], []
    for i in range(len(triples)):
        train = [(e, o) for _, _, e, o in triples[max(0, i - WINDOW):i]]
        if len(train) < MIN_N:
            continue
        alpha, beta = app._ols_beta(train)
        _, est_old, est_new, official = triples[i]
        p10.append((est_old, official))
        p11.append((est_new, official))
        p12.append((alpha + beta * est_new, official))
    return {"1.0": bt.metrics(p10), "1.1": bt.metrics(p11), "1.2": bt.metrics(p12),
            "pairs": {"1.0": p10, "1.1": p11, "1.2": p12}}


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=120)
    args = ap.parse_args()
    t0 = time.time()
    results = []
    for code in FUNDS:
        print(f"[{code}] fetching...", flush=True)
        try:
            triples = await dated_triples(code, args.days)
        except Exception as e:
            print(f"[{code}] FAILED: {e!r}", flush=True)
            continue
        wf = walk_forward(triples)
        results.append({"code": code, "wf": wf})
        print(f"[{code}] triples={len(triples)} test_n={wf['1.1'].get('n', 0)}")
        for v in ("1.0", "1.1", "1.2"):
            print(f"  TrueNAV {v}: {bt.fmt_m(wf[v])}", flush=True)

    overall = {}
    for v in ("1.0", "1.1", "1.2"):
        pairs = [p for r in results for p in r["wf"]["pairs"][v]]
        overall[v] = bt.metrics(pairs)
        print(f"\nOVERALL TrueNAV {v}: {bt.fmt_m(overall[v])}")

    now = datetime.now(timezone(timedelta(hours=8)))
    payload = {
        "generated_at": now.strftime("%Y-%m-%d %H:%M CST"),
        "window_days": args.days,
        "funds": FUNDS,
        "method": ("walk-forward, same test days for all versions; "
                   "1.0 top-10 only, 1.1 +residual fill, "
                   "1.2 +beta calibration fit on prior <=60 pairs"),
        "overall": {v: {"n": overall[v].get("n", 0),
                        "mae": round(overall[v]["mae"], 3) if overall[v].get("n") else None,
                        "rmse": round(overall[v]["rmse"], 3) if overall[v].get("n") else None,
                        "bias": round(overall[v]["bias"], 3) if overall[v].get("n") else None,
                        "dir_acc": (round(overall[v]["dir_acc"] * 100, 1)
                                    if overall[v].get("dir_acc") is not None else None)}
                    for v in ("1.0", "1.1", "1.2")},
        "per_fund": {r["code"]: {v: (round(r["wf"][v]["mae"], 3) if r["wf"][v].get("n") else None)
                                 for v in ("1.0", "1.1", "1.2")}
                     for r in results},
    }
    JSON_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
                         encoding="utf-8")
    print(f"\njson -> {JSON_PATH}")

    lines = [
        "",
        "## TrueNAV 三版本同窗回测（v1.4.0）",
        "",
        f"生成时间：{payload['generated_at']} · 窗口：每只基金最近 {args.days} 个净值日 · "
        f"耗时 {time.time() - t0:.0f}s",
        "",
        "方法：同一 120 日窗口、同一批 walk-forward 测试日（此前 ≥20 个样本才计入，"
        "与 P1 回测相同的测试日选择），对比三个算法版本：TrueNAV 1.0 = 前十大加权；"
        "1.1 = +未披露仓位补齐（P0）；1.2 = +β 校准（P1，每个测试日只用此前至多 60 个"
        "样本拟合）。持仓仍为当前前十大（前视偏差同前两份基线）。",
        "",
        "| 基金 | 测试日 n | MAE 1.0 | MAE 1.1 | MAE 1.2 |",
        "|---|---|---|---|---|",
    ]
    for r in results:
        wf = r["wf"]
        if not wf["1.1"].get("n"):
            continue
        lines.append(f"| {r['code']} | {wf['1.1']['n']} | {wf['1.0']['mae']:.3f} | "
                     f"{wf['1.1']['mae']:.3f} | {wf['1.2']['mae']:.3f} |")
    if overall["1.1"].get("n"):
        lines.append(f"| **全部** | {overall['1.1']['n']} | **{overall['1.0']['mae']:.3f}** | "
                     f"**{overall['1.1']['mae']:.3f}** | **{overall['1.2']['mae']:.3f}** |")
    with REPORT_PATH.open("a", encoding="utf-8") as fh:
        fh.write("\n".join(lines) + "\n")
    print(f"appended -> {REPORT_PATH}")


if __name__ == "__main__":
    asyncio.run(main())
