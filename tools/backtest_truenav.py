#!/usr/bin/env python3
"""TrueNAV 1.4 experiment 0: point-in-time backtest infrastructure.

Replaces the old look-ahead baseline (today's top-10 holdings applied to
the whole window) with a point-in-time (PIT) design:

1. PIT holdings: for each NAV date D, use the holdings of the latest
   quarterly disclosure whose disclosure deadline had passed by D.
   Holdings come from the Eastmoney jjcc endpoint, which returns every
   quarter of a given year in one response (one table per quarter);
   each table is parsed with a layout-aware weight column (current
   9-col layout vs historical 7-col layout). Disclosure availability
   is conservative (deadline-based): Q1 (Mar 31) -> May 1, Q2 (Jun 30)
   -> Sep 1, Q3 (Sep 30) -> Nov 1, Q4 (Dec 31) -> Apr 1 next year.
   NAV dates before any disclosure are skipped, never backfilled.
2. Metrics: fund-equal-weighted MAE is the primary metric (mean of
   per-fund MAE); pooled MAE is also reported for comparison. Per fund
   and overall: MAE / RMSE / Bias / P50 / P75 / P90 / P95 of |error|.
3. Splits: by fund type (QDII / 债券 / 指数 / 主动权益, via
   app.fetch_fund_type) and by market regime (|official| <=0.5% calm,
   0.5-2% normal, >2% volatile; plus up/down). Walk-forward is
   expanding-window (beta fit on prior <=60 pairs only); test days are
   split into 3 contiguous folds, and the last 20% of test days are
   reported separately as a held-out tail. Beta window (60) and min_n
   (20) are fixed a priori from TrueNAV 1.2, not tuned on this run,
   so train / validation / test stay isolated.
4. Window: --days up to 500 (lsjz pages and Yahoo range extended in
   tools/backtest.py). PIT holdings history limits the usable window:
   jjcc serves quarters back to each fund's inception, but the stock
   position (residual fill) only has 4 quarters of history in
   pingzhongdata; older quarters reuse the oldest known position and
   are counted as stale_pos_days in the report.

Stock closes: Yahoo chart API (range=2y) / Eastmoney kline, shared
cache /tmp/backtest_cache.json with tools/backtest.py.

Run:  venv/bin/python tools/backtest_truenav.py [--days 500] [--legacy]
  --legacy reproduces the old口径 (current holdings, whole window)
  for an apples-to-apples baseline comparison at the same window.
Output: stdout tables + tools/truenav-backtest.json + appended section
in backtest-report.md. Does NOT touch app.py or live valuation logic.
"""
import argparse
import asyncio
import json
import re
import sys
import time
from datetime import date, datetime, timedelta, timezone
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

# Quarter-end month -> (available month, available day, year offset).
# Conservative: statutory disclosure deadlines, not actual (earlier)
# disclosure dates, so no look-ahead is possible.
_AVAIL = {3: (5, 1, 0), 6: (9, 1, 0), 9: (11, 1, 0), 12: (4, 1, 1)}


def available_on(report_date: str) -> date:
    """First date a quarterly disclosure may be used (deadline-based)."""
    y, m, _d = (int(x) for x in report_date.split("-"))
    am, ad, yoff = _AVAIL[m]
    return date(y + yoff, am, ad)


def parse_holdings_tables(text: str) -> list:
    """All quarterly top-10 tables in one jjcc response.

    Returns [{report_label, report_date, holdings}] sorted by
    report_date. The weight column is located from the header row
    ("占净值比例"), which handles both the current 9-col layout
    (with 最新价/涨跌幅) and the historical 7-col layout.
    """
    m = re.search(r'content:"(.*)"\s*[,}]', text, re.S)
    html = m.group(1).replace('\\"', '"') if m else text
    out = []
    for sec in re.split(r"<h4 class='t'>", html)[1:]:
        lm = re.search(r"(\d{4}年\d季度)", sec)
        dm = re.search(r"截止至：<font[^>]*>([\d-]+)</font>", sec)
        if not lm or not dm:
            continue
        parser = app._TableParser()
        parser.feed(sec)
        wcol = None
        for row in parser.rows:
            if "占净值比例" in row:
                wcol = row.index("占净值比例")
                break
        if wcol is None:
            continue
        holdings = []
        for row in parser.rows:
            if len(row) > wcol and re.fullmatch(r"\d+", row[0] or "") and row[1]:
                wm = re.search(r"([\d.]+)\s*%", row[wcol])
                if wm:
                    holdings.append({"code": row[1].strip(),
                                     "name": row[2].strip(),
                                     "weight": float(wm.group(1))})
        if holdings:
            out.append({"report_label": lm.group(1), "report_date": dm.group(1),
                        "holdings": holdings[:10]})
    out.sort(key=lambda s: s["report_date"])
    return out


async def fetch_holdings_history(code: str, years) -> list:
    """PIT holdings snapshots for a fund, merged across years."""
    snaps = {}
    for y in years:
        url = ("https://fundf10.eastmoney.com/FundArchivesDatas.aspx?type=jjcc"
               f"&code={code}&topline=10&year={y}&month=12&rt=0.1")
        try:
            text = await bt.fetch_text(
                url, referer=f"https://fundf10.eastmoney.com/ccmx_{code}.html")
        except Exception:
            continue
        for s in parse_holdings_tables(text):
            snaps[s["report_date"]] = s
    return sorted(snaps.values(), key=lambda s: s["report_date"])


def holdings_asof(snaps: list, day: date):
    """Latest snapshot whose disclosure deadline had passed by `day`."""
    best = None
    for s in snaps:
        if available_on(s["report_date"]) <= day:
            best = s
    return best


async def fetch_stock_positions(code: str) -> dict:
    """{quarter-end date: 股票占净比%} from pingzhongdata (last 4 qtrs)."""
    try:
        text = await app.fetch_pingzhong_text(code)
    except Exception:
        return {}
    m = re.search(r"Data_assetAllocation\s*=\s*(\{.*?\});", text, re.S)
    if not m:
        return {}
    try:
        d = json.loads(m.group(1))
    except Exception:
        return {}
    cats = d.get("categories") or []
    for s in d.get("series") or []:
        if s.get("name") == "股票占净比":
            return {c: float(v) for c, v in zip(cats, s.get("data") or [])
                    if v is not None}
    return {}


def _percentile(sorted_vals: list, q: float):
    if not sorted_vals:
        return None
    i = q * (len(sorted_vals) - 1)
    lo, hi = int(i), min(int(i) + 1, len(sorted_vals) - 1)
    f = i - lo
    return sorted_vals[lo] * (1 - f) + sorted_vals[hi] * f


def metrics2(pairs: list) -> dict:
    """bt.metrics + P50/P75/P90/P95 of |error|."""
    m = bt.metrics(pairs)
    if m.get("n"):
        ae = sorted(abs(e - o) for e, o in pairs)
        for name, q in (("p50", 0.5), ("p75", 0.75), ("p90", 0.9), ("p95", 0.95)):
            m[name] = _percentile(ae, q)
    return m


def _est_for_day(holdings, changes_by_code, residual, date_str):
    num, quoted_w = 0.0, 0.0
    for h in holdings:
        chg = changes_by_code.get(h["code"], {}).get(date_str)
        if chg is not None:
            num += h["weight"] * chg
            quoted_w += h["weight"]
    coverage = sum(h["weight"] for h in holdings)
    if coverage <= 0 or quoted_w < 0.5 * coverage:
        return None
    top10avg = num / quoted_w if quoted_w else 0.0
    est_old = num / 100
    est_new = est_old + (residual * top10avg / 100 if quoted_w else 0.0)
    return est_old, est_new, residual, top10avg


async def dated_triples_pit(code: str, days: int):
    """[(date, est_old, est_new, official, vintage, stale_pos)] PIT."""
    nav_rows = await bt.fetch_nav_history(code, days)  # newest first
    today = datetime.now(timezone(timedelta(hours=8))).date()
    years = list(range(today.year - 2, today.year + 1))
    snaps = await fetch_holdings_history(code, years)
    positions = await fetch_stock_positions(code)
    mmap = await app.fetch_stock_market_map(code)
    changes_by_code = {}
    seen = {}
    for s in snaps:
        for h in s["holdings"]:
            seen[h["code"]] = h
    for h in seen.values():
        closes = await bt.holding_closes(h, mmap)
        if closes:
            changes_by_code[h["code"]] = bt.daily_changes(closes)
    oldest_pos = min(positions.items()) if positions else (None, None)
    triples, skipped_nodisc, stale_pos_days = [], 0, 0
    for date_str, official in nav_rows:
        day = date.fromisoformat(date_str)
        snap = holdings_asof(snaps, day)
        if snap is None:
            skipped_nodisc += 1
            continue
        holdings = snap["holdings"]
        coverage = sum(h["weight"] for h in holdings)
        pos = positions.get(snap["report_date"])
        stale = pos is None
        if stale:
            pos = oldest_pos[1]
            stale_pos_days += 1
        residual = app._residual_pct(coverage, pos)
        est = _est_for_day(holdings, changes_by_code, residual, date_str)
        if est is None:
            continue
        triples.append({"date": date_str, "est_old": est[0], "est_new": est[1],
                        "official": official, "vintage": snap["report_date"],
                        "stale_pos": stale, "residual": est[2],
                        "top10avg": est[3]})
    triples.sort(key=lambda t: t["date"])
    info = {"skipped_no_disclosure": skipped_nodisc,
            "stale_pos_days": stale_pos_days,
            "snapshots": [(s["report_label"], s["report_date"]) for s in snaps],
            "positions": positions,
            "window": (triples[0]["date"], triples[-1]["date"]) if triples else None}
    return triples, info


async def dated_triples_legacy(code: str, days: int):
    """Old口径: current holdings applied to the whole window."""
    nav_rows = await bt.fetch_nav_history(code, days)
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
    for date_str, official in nav_rows:
        est = _est_for_day(holdings, changes_by_code, residual, date_str)
        if est is None:
            continue
        triples.append({"date": date_str, "est_old": est[0], "est_new": est[1],
                        "official": official, "vintage": "current",
                        "stale_pos": False, "residual": est[2],
                        "top10avg": est[3]})
    triples.sort(key=lambda t: t["date"])
    return triples, {"skipped_no_disclosure": 0, "stale_pos_days": 0,
                     "snapshots": [(hold.get("report_label"), hold.get("report_date"))],
                     "positions": {}, "window": (triples[0]["date"], triples[-1]["date"]) if triples else None}


def walk_forward(triples: list) -> dict:
    """Expanding-window beta fit (prior <=60 pairs only).

    Same test days for all three versions. Adds 3 contiguous folds and
    a held-out tail (last 20% of test days) per version.
    """
    rows = {"1.0": [], "1.1": [], "1.2": []}  # (est, official, triple)
    for i, t in enumerate(triples):
        train = [(x["est_new"], x["official"])
                 for x in triples[max(0, i - WINDOW):i]]
        if len(train) < MIN_N:
            continue
        alpha, beta = app._ols_beta(train)
        rows["1.0"].append((t["est_old"], t["official"], t))
        rows["1.1"].append((t["est_new"], t["official"], t))
        rows["1.2"].append((alpha + beta * t["est_new"], t["official"], t))
    out = {}
    for v in ("1.0", "1.1", "1.2"):
        pairs = [(e, o) for e, o, _ in rows[v]]
        out[v] = metrics2(pairs)
        out[v]["_rows"] = rows[v]
        n = len(rows[v])
        out[v]["folds"] = [metrics2([(e, o) for e, o, _ in
                                     rows[v][k * n // 3:(k + 1) * n // 3]])
                           for k in range(3)]
        out[v]["tail"] = metrics2([(e, o) for e, o, _ in rows[v][int(n * 0.8):]])
    return out


def regime(official: float) -> str:
    a = abs(official)
    if a <= 0.5:
        return "平静<=0.5%"
    if a <= 2:
        return "常规0.5-2%"
    return "剧烈>2%"


def type_group(ftype) -> str:
    if not ftype:
        return "未知"
    if "QDII" in ftype:
        return "QDII"
    if "债券" in ftype:
        return "债券"
    if "指数" in ftype:
        return "指数"
    return "主动权益"


def split_metrics(rows, key_fn):
    groups = {}
    for e, o, t in rows:
        groups.setdefault(key_fn(t, o), []).append((e, o))
    return {k: metrics2(v) for k, v in sorted(groups.items())}


def _fmt(m: dict) -> str:
    if not m.get("n"):
        return "n=0"
    return (f"n={m['n']} MAE={m['mae']:.3f} RMSE={m['rmse']:.3f} "
            f"bias={m['bias']:+.3f} P50={m['p50']:.3f} P90={m['p90']:.3f} "
            f"P95={m['p95']:.3f}")


def _m_json(m: dict):
    if not m.get("n"):
        return {"n": 0}
    return {"n": m["n"], "mae": round(m["mae"], 3), "rmse": round(m["rmse"], 3),
            "bias": round(m["bias"], 3), "p50": round(m["p50"], 3),
            "p75": round(m["p75"], 3), "p90": round(m["p90"], 3),
            "p95": round(m["p95"], 3)}


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=500)
    ap.add_argument("--legacy", action="store_true",
                    help="old口径: current holdings over the whole window")
    args = ap.parse_args()
    t0 = time.time()
    mode = "legacy" if args.legacy else "pit"
    results = []
    for code in FUNDS:
        print(f"[{code}] fetching ({mode})...", flush=True)
        try:
            ftype = await app.fetch_fund_type(code)
            if args.legacy:
                triples, info = await dated_triples_legacy(code, args.days)
            else:
                triples, info = await dated_triples_pit(code, args.days)
        except Exception as e:
            print(f"[{code}] FAILED: {e!r}", flush=True)
            continue
        wf = walk_forward(triples)
        results.append({"code": code, "ftype": ftype, "group": type_group(ftype),
                        "wf": wf, "info": info, "n_triples": len(triples)})
        print(f"[{code}] {ftype} | triples={len(triples)} "
              f"test_n={wf['1.2'].get('n', 0)} window={info['window']} "
              f"skipped_no_disc={info['skipped_no_disclosure']} "
              f"stale_pos_days={info['stale_pos_days']}")
        for v in ("1.0", "1.1", "1.2"):
            print(f"  TrueNAV {v}: {_fmt(wf[v])}", flush=True)
        print(f"  folds(1.2): " + " | ".join(
            f"F{k + 1} MAE={f['mae']:.3f} (n={f['n']})" if f.get("n") else f"F{k + 1} n=0"
            for k, f in enumerate(wf["1.2"]["folds"])))
        print(f"  tail20%(1.2): {_fmt(wf['1.2']['tail'])}", flush=True)
        sp = split_metrics(wf["1.2"]["_rows"], lambda t, o: regime(o))
        print("  regime(1.2): " + " | ".join(
            f"{k} MAE={v['mae']:.3f} (n={v['n']})" for k, v in sp.items()))

    overall, equal_mae = {}, {}
    for v in ("1.0", "1.1", "1.2"):
        pairs = [p for r in results for p in
                 [(e, o) for e, o, _ in r["wf"][v]["_rows"]]]
        overall[v] = metrics2(pairs)
        maes = [r["wf"][v]["mae"] for r in results if r["wf"][v].get("n")]
        equal_mae[v] = sum(maes) / len(maes) if maes else None
        print(f"\nOVERALL TrueNAV {v}: {_fmt(overall[v])} "
              f"fund-equal MAE={equal_mae[v]:.3f}" if equal_mae[v] is not None else "")
    # type split (1.2, pooled across funds in group)
    type_rows = {}
    for r in results:
        for e, o, t in r["wf"]["1.2"]["_rows"]:
            type_rows.setdefault(r["group"], []).append((e, o))
    type_split = {k: metrics2(v) for k, v in sorted(type_rows.items())}
    print("TYPE(1.2): " + " | ".join(
        f"{k} MAE={v['mae']:.3f} (n={v['n']})" for k, v in type_split.items()))

    now = datetime.now(timezone(timedelta(hours=8)))
    payload = {
        "generated_at": now.strftime("%Y-%m-%d %H:%M CST"),
        "experiment": "truenav-1.4-exp0",
        "mode": mode,
        "window_days": args.days,
        "funds": FUNDS,
        "method": ("PIT holdings: latest quarterly disclosure whose deadline "
                   "had passed by each NAV date (Q1->May 1, Q2->Sep 1, "
                   "Q3->Nov 1, Q4->Apr 1 next year); beta walk-forward on "
                   "prior <=60 pairs; primary metric fund-equal MAE."
                   if mode == "pit" else
                   "LEGACY: current holdings applied to whole window."),
        "overall": {v: {**_m_json(overall[v]),
                        "fund_equal_mae": (round(equal_mae[v], 3)
                                           if equal_mae[v] is not None else None)}
                    for v in ("1.0", "1.1", "1.2")},
        "type_split_1.2": {k: _m_json(v) for k, v in type_split.items()},
        "per_fund": {},
    }
    for r in results:
        wf = r["wf"]
        payload["per_fund"][r["code"]] = {
            "ftype": r["ftype"], "group": r["group"],
            "window": r["info"]["window"],
            "triples": r["n_triples"],
            "skipped_no_disclosure": r["info"]["skipped_no_disclosure"],
            "stale_pos_days": r["info"]["stale_pos_days"],
            "snapshots": r["info"]["snapshots"],
            **{v: _m_json(wf[v]) for v in ("1.0", "1.1", "1.2")},
            "folds_1.2": [_m_json(f) for f in wf["1.2"]["folds"]],
            "tail_1.2": _m_json(wf["1.2"]["tail"]),
            "regime_1.2": {k: _m_json(v) for k, v in
                           split_metrics(wf["1.2"]["_rows"],
                                         lambda t, o: regime(o)).items()},
        }
    JSON_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
                         encoding="utf-8")
    print(f"\njson -> {JSON_PATH}")

    title = ("## TrueNAV 1.4 实验0：回测基建（PIT口径）" if mode == "pit"
             else "## TrueNAV 1.4 实验0：旧口径对照（legacy，当前持仓回填）")
    lines = [
        "",
        title,
        "",
        f"生成时间：{payload['generated_at']} · 窗口：每只基金最近 {args.days} 个净值日 · "
        f"耗时 {time.time() - t0:.0f}s",
        "",
        payload["method"],
        "",
        f"主指标（基金等权 MAE，1.2）：{equal_mae['1.2']:.3f}；"
        f" pooled MAE 1.0/1.1/1.2 = {overall['1.0']['mae']:.3f} / "
        f"{overall['1.1']['mae']:.3f} / {overall['1.2']['mae']:.3f}。",
        "",
        "| 基金 | 类型 | 样本n | 测试日n | MAE 1.0 | MAE 1.1 | MAE 1.2 | RMSE 1.2 | Bias 1.2 | P50 1.2 | P90 1.2 | P95 1.2 | 无披露跳过 | 仓位兜底日 |",
        "|---|---|---|---|---|---|---|---|---|---|---|---|---|---|",
    ]
    for r in results:
        wf = r["wf"]
        if not wf["1.2"].get("n"):
            continue
        m = wf["1.2"]
        lines.append(
            f"| {r['code']} | {r['group']} | {r['n_triples']} | {m['n']} | "
            f"{wf['1.0']['mae']:.3f} | {wf['1.1']['mae']:.3f} | {m['mae']:.3f} | "
            f"{m['rmse']:.3f} | {m['bias']:+.3f} | {m['p50']:.3f} | {m['p90']:.3f} | "
            f"{m['p95']:.3f} | {r['info']['skipped_no_disclosure']} | "
            f"{r['info']['stale_pos_days']} |")
    lines += [
        f"| **全部 pooled** | - | - | {overall['1.2']['n']} | "
        f"{overall['1.0']['mae']:.3f} | {overall['1.1']['mae']:.3f} | "
        f"**{overall['1.2']['mae']:.3f}** | {overall['1.2']['rmse']:.3f} | "
        f"{overall['1.2']['bias']:+.3f} | {overall['1.2']['p50']:.3f} | "
        f"{overall['1.2']['p90']:.3f} | {overall['1.2']['p95']:.3f} | - | - |",
        f"| **基金等权 MAE** | - | - | - | {equal_mae['1.0']:.3f} | "
        f"{equal_mae['1.1']:.3f} | **{equal_mae['1.2']:.3f}** | - | - | - | - | - | - | - |",
        "",
        "类型分拆（1.2 pooled）： " + "；".join(
            f"{k} MAE={v['mae']:.3f} (n={v['n']})" for k, v in type_split.items()),
        "",
        "Walk-forward 三段（1.2 MAE）： " + "；".join(
            f"{r['code']} F1/F2/F3 = " + "/".join(
                f"{f['mae']:.3f}" if f.get("n") else "-"
                for f in r["wf"]["1.2"]["folds"])
            for r in results if r["wf"]["1.2"].get("n")),
        "",
        "末 20% 持有尾段（1.2 MAE）： " + "；".join(
            f"{r['code']} {r['wf']['1.2']['tail']['mae']:.3f} "
            f"(n={r['wf']['1.2']['tail']['n']})"
            for r in results if r["wf"]["1.2"]["tail"].get("n")),
    ]
    with REPORT_PATH.open("a", encoding="utf-8") as fh:
        fh.write("\n".join(lines) + "\n")
    print(f"appended -> {REPORT_PATH}")


if __name__ == "__main__":
    asyncio.run(main())
