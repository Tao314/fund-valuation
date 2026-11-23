#!/usr/bin/env python3
"""Walk-forward backtest for P1 β calibration (v1.22.0).

For each of the 6 baseline funds, build dated (estimate, official)
pairs with the current method (top-10 + P0 residual fill, same as
tools/backtest.py), then walk forward: for every test day, fit
actual = α + β·est on the PRIOR up-to-60 pairs only (no look-ahead in
the regression) and compare MAE of raw vs calibrated estimates on the
test days. Also reports the final full-window (last 60 pairs) α/β and
in-sample MAE — the values the live tool stores in fund_beta.

Holdings are still today's top-10 applied to the whole window (same
look-ahead limitation as the P0 baseline); only the regression itself
is walk-forward.

Run:  venv/bin/python tools/backtest_beta.py [--days 120]
Output: stdout table + appended section in backtest-report.md.
Shares /tmp/backtest_cache.json with tools/backtest.py.
"""
import argparse
import asyncio
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
WINDOW = 60
MIN_N = 20


async def dated_pairs(code: str, days: int) -> list:
    """[(date, est_new, official)] ascending by date."""
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
    pairs = []
    for date, official in nav_rows:
        num, quoted_w = 0.0, 0.0
        for h in holdings:
            chg = changes_by_code.get(h["code"], {}).get(date)
            if chg is not None:
                num += h["weight"] * chg
                quoted_w += h["weight"]
        if coverage <= 0 or quoted_w < 0.5 * coverage:
            continue
        est = num / 100 + (residual * (num / quoted_w) / 100 if quoted_w else 0.0)
        pairs.append((date, est, official))
    pairs.sort()
    return pairs


def walk_forward(pairs: list) -> dict:
    """pairs ascending [(date, est, official)] -> test-day MAE raw vs cal."""
    test_raw, test_cal = [], []
    for i in range(len(pairs)):
        train = [(e, o) for _, e, o in pairs[max(0, i - WINDOW):i]]
        if len(train) < MIN_N:
            continue
        alpha, beta = app._ols_beta(train)
        _, est, official = pairs[i]
        test_raw.append((est, official))
        test_cal.append((alpha + beta * est, official))
    return {"raw": bt.metrics(test_raw), "cal": bt.metrics(test_cal),
            "pairs_raw": test_raw, "pairs_cal": test_cal}


def final_fit(pairs: list) -> dict:
    """Last-WINDOW in-sample fit — what compute_fund_beta stores live."""
    tail = [(e, o) for _, e, o in pairs[-WINDOW:]]
    if len(tail) < MIN_N:
        return {"n": len(tail)}
    alpha, beta = app._ols_beta(tail)
    mae_raw = sum(abs(e - o) for e, o in tail) / len(tail)
    mae_cal = sum(abs(alpha + beta * e - o) for e, o in tail) / len(tail)
    return {"n": len(tail), "alpha": alpha, "beta": beta,
            "mae_raw": mae_raw, "mae_cal": mae_cal}


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=120)
    args = ap.parse_args()
    t0 = time.time()
    results = []
    for code in FUNDS:
        print(f"[{code}] fetching...", flush=True)
        try:
            pairs = await dated_pairs(code, args.days)
        except Exception as e:
            print(f"[{code}] FAILED: {e!r}", flush=True)
            continue
        wf = walk_forward(pairs)
        ff = final_fit(pairs)
        results.append({"code": code, "pairs": pairs, "wf": wf, "ff": ff})
        print(f"[{code}] pairs={len(pairs)} test_n={wf['raw'].get('n', 0)}")
        print(f"  walk-forward raw: {bt.fmt_m(wf['raw'])}")
        print(f"  walk-forward cal: {bt.fmt_m(wf['cal'])}")
        if ff.get("beta") is not None:
            print(f"  final fit (last {ff['n']}): α={ff['alpha']:+.3f} β={ff['beta']:.3f} "
                  f"MAE raw={ff['mae_raw']:.3f} cal={ff['mae_cal']:.3f}", flush=True)

    all_raw = [p for r in results for p in r["wf"]["pairs_raw"]]
    all_cal = [p for r in results for p in r["wf"]["pairs_cal"]]
    mo, mc = bt.metrics(all_raw), bt.metrics(all_cal)
    print(f"\nOVERALL walk-forward raw: {bt.fmt_m(mo)}")
    print(f"OVERALL walk-forward cal: {bt.fmt_m(mc)}")

    lines = [
        "",
        "## P1 β 校准 walk-forward 回测（v1.22.0）",
        "",
        f"生成时间：{datetime.now(timezone(timedelta(hours=8))):%Y-%m-%d %H:%M} CST · "
        f"窗口：每只基金最近 {args.days} 个净值日 · 耗时 {time.time() - t0:.0f}s",
        "",
        "方法：与 P0 基线相同的估算（前十大 + 未披露补齐）；walk-forward：每个测试日"
        "只用此前至多 60 个（估算，官方）样本拟合 actual = α + β·est（β 限 0.5–1.5、"
        "α 限 ±0.5，训练样本 <20 不校准），比较测试日上校准前后的 MAE。"
        "持仓仍为当前前十大（前视偏差同 P0 基线），仅回归本身无前视。",
        "",
        "| 基金 | 测试日 n | MAE 原始 | MAE 校准后 | RMSE 原始 | RMSE 校准后 | 末窗 α | 末窗 β | 末窗 MAE 原始 | 末窗 MAE 校准 |",
        "|---|---|---|---|---|---|---|---|---|---|",
    ]
    for r in results:
        wf, ff = r["wf"], r["ff"]
        if not wf["raw"].get("n"):
            continue
        lines.append(
            f"| {r['code']} | {wf['raw']['n']} | {wf['raw']['mae']:.3f} | {wf['cal']['mae']:.3f} | "
            f"{wf['raw']['rmse']:.3f} | {wf['cal']['rmse']:.3f} | "
            f"{ff.get('alpha', float('nan')):+.3f} | {ff.get('beta', float('nan')):.3f} | "
            f"{ff.get('mae_raw', float('nan')):.3f} | {ff.get('mae_cal', float('nan')):.3f} |")
    if mo.get("n"):
        lines.append(f"| **全部** | {mo['n']} | **{mo['mae']:.3f}** | **{mc['mae']:.3f}** | "
                     f"{mo['rmse']:.3f} | {mc['rmse']:.3f} | - | - | - | - |")
    with REPORT_PATH.open("a", encoding="utf-8") as fh:
        fh.write("\n".join(lines) + "\n")
    print(f"\nappended -> {REPORT_PATH}")


if __name__ == "__main__":
    asyncio.run(main())
