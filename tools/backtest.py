#!/usr/bin/env python3
"""Backtest baseline for the self-calculated fund estimate (v1.19.0 P0).

Compares, over the last ~60 NAV trading days, the official daily NAV
change (Eastmoney lsjz JZZZL) against two estimates computed from the
fund's CURRENT top-10 holdings and each holding's daily closes:

  est_old = sum(weight% * stock_change%) / 100            (pre-P0)
  est_new = est_old + residual * disclosed_avg_change / 100 (P0 fill)
  residual = max(stock_position% - top10_coverage%, 0)

Daily closes come from Yahoo Finance chart API (uniform symbols);
JP/KR/TW holdings (ISIN / bare local codes) go through the Eastmoney
kline API via the app's secid resolution, with retries.

Known biases (baseline only, do not over-read):
  * Look-ahead: today's top-10 holdings and latest stock position are
    applied to the whole window; managers trade, weights drift.
  * QDII alignment: official NAV date D is compared with stock changes
    on calendar date D in each stock's own market (US date D for US
    holdings). The live tool's intraday semantics differ slightly.
  * Yahoo closes are split-adjusted; EM kline uses fqt=1.

Run:  venv/bin/python tools/backtest.py [--days 60]
Output: backtest-report.md next to the repo root + stdout summary.
Responses are cached in /tmp/backtest_cache.json.
"""
import argparse
import asyncio
import json
import math
import re
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import app  # noqa: E402  (module-level init_db runs; repoint DB below)

# Keep backtest caches out of the production DB.
app.DB_PATH = Path("/tmp/backtest.db")
app.init_db()

FUNDS = ["024481", "010390", "012922", "002891", "014064", "016665"]
CACHE_PATH = Path("/tmp/backtest_cache.json")
REPORT_PATH = Path(__file__).resolve().parent.parent / "backtest-report.md"
_cache: dict = {}
if CACHE_PATH.exists():
    try:
        _cache = json.loads(CACHE_PATH.read_text())
    except Exception:
        _cache = {}
_sem = asyncio.Semaphore(4)


def _save_cache():
    CACHE_PATH.write_text(json.dumps(_cache, ensure_ascii=False))


async def fetch_text(url: str, referer: str = None, retries: int = 3) -> str:
    if url in _cache:
        return _cache[url]
    last_err = None
    for attempt in range(retries):
        try:
            async with _sem:
                resp = await app.upstream_get(url, referer=referer, timeout=30)
            text = resp.text
            if text and text.strip():
                _cache[url] = text
                _save_cache()
                return text
            last_err = ValueError("empty response")
        except Exception as e:  # throttling / transient upstream errors
            last_err = e
        await asyncio.sleep(2 * (attempt + 1))
    raise RuntimeError(f"fetch failed after {retries} tries: {url} ({last_err!r})")


# ------------------------------------------------------------------ NAV
async def fetch_nav_history(code: str, days: int) -> list:
    """[(date, jzzzl)] newest first, up to `days` rows (lsjz caps 20/page)."""
    rows = []
    page = 1
    while len(rows) < days and page <= 6:
        url = (f"https://api.fund.eastmoney.com/f10/lsjz?fundCode={code}"
               f"&pageIndex={page}&pageSize=20")
        data = json.loads(await fetch_text(
            url, referer=f"https://fund.eastmoney.com/f10/jjjz_{code}.html"))
        items = data["Data"]["LSJZList"]
        if not items:
            break
        for it in items:
            try:
                rows.append((it["FSRQ"], float(it["JZZZL"] or 0)))
            except (TypeError, ValueError):
                continue
        page += 1
    return rows[:days]


# ------------------------------------------------------------- daily closes
def yahoo_symbol(code: str) -> str:
    c = code.strip().upper()
    if re.fullmatch(r"\d{6}", c):
        return c + (".SS" if c[0] in "69" else ".SZ")
    if re.fullmatch(r"\d{5}", c):
        return c.lstrip("0").zfill(4) + ".HK"
    return c.replace(".", "-")  # US ticker (BRK.B -> BRK-B)


async def yahoo_closes(sym: str) -> dict:
    """{local_date: close} from Yahoo chart API (6mo daily)."""
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{sym}?range=1y&interval=1d"
    data = json.loads(await fetch_text(url))
    result = (data.get("chart") or {}).get("result")
    if not result:
        return {}
    r = result[0]
    offset = (r.get("meta") or {}).get("gmtoffset", 0)
    tz = timezone(timedelta(seconds=offset))
    closes = ((r.get("indicators") or {}).get("quote") or [{}])[0].get("close") or []
    out = {}
    for ts, close in zip(r.get("timestamp") or [], closes):
        if close is None:
            continue
        out[datetime.fromtimestamp(ts, tz).date().isoformat()] = float(close)
    return out


async def em_closes(secid: str) -> dict:
    """{date: close} from Eastmoney daily kline (fqt=1)."""
    url = ("https://push2his.eastmoney.com/api/qt/stock/kline/get"
           f"?secid={secid}&fields1=f1,f2,f3&fields2=f51,f52,f53"
           "&klt=101&fqt=1&beg=20260101&end=20500101")
    data = json.loads(await fetch_text(url, referer="https://quote.eastmoney.com/"))
    klines = ((data.get("data") or {}).get("klines")) or []
    out = {}
    for row in klines:
        f = row.split(",")
        if len(f) >= 3:
            try:
                out[f[0]] = float(f[2])
            except ValueError:
                continue
    return out


async def holding_closes(h: dict, mmap) -> dict:
    """Daily closes for one holding; {} when no source yields data."""
    code = h["code"]
    if app._em_want_types(code, mmap):  # JP/KR/TW: name-resolved EM secid
        secid = await app.resolve_em_secid(code, h.get("name"), mmap)
        if secid:
            try:
                got = await em_closes(secid)
                if got:
                    return got
            except Exception:
                pass
        return {}
    try:
        return await yahoo_closes(yahoo_symbol(code))
    except Exception:
        return {}


def daily_changes(closes: dict) -> dict:
    """{date: change%} vs the stock's own previous trading day."""
    dates = sorted(closes)
    out = {}
    for prev, cur in zip(dates, dates[1:]):
        if closes[prev]:
            out[cur] = (closes[cur] - closes[prev]) / closes[prev] * 100
    return out


# ---------------------------------------------------------------- metrics
def metrics(pairs: list) -> dict:
    """pairs: [(est, official)] -> MAE/RMSE/bias/direction accuracy."""
    n = len(pairs)
    if not n:
        return {"n": 0}
    errs = [e - o for e, o in pairs]
    mae = sum(abs(x) for x in errs) / n
    rmse = math.sqrt(sum(x * x for x in errs) / n)
    bias = sum(errs) / n
    dir_pairs = [(e, o) for e, o in pairs if abs(o) >= 0.05]
    dir_acc = (sum(1 for e, o in dir_pairs if (e > 0) == (o > 0)) / len(dir_pairs)
               if dir_pairs else None)
    return {"n": n, "mae": mae, "rmse": rmse, "bias": bias,
            "dir_acc": dir_acc, "dir_n": len(dir_pairs)}


async def backtest_fund(code: str, days: int) -> dict:
    nav_rows = await fetch_nav_history(code, days)
    hold = await app.fetch_holdings(code)
    holdings = hold["holdings"]
    mmap = await app.fetch_stock_market_map(code)
    stock_position = await app.fetch_stock_position(code)
    coverage = sum(h["weight"] for h in holdings)
    residual = app._residual_pct(coverage, stock_position)

    changes_by_code = {}
    missing = []
    for h in holdings:
        closes = await holding_closes(h, mmap)
        if closes:
            changes_by_code[h["code"]] = daily_changes(closes)
        else:
            missing.append(h["code"])

    pairs_old, pairs_new, skipped = [], [], 0
    for date, official in nav_rows:
        num, quoted_w = 0.0, 0.0
        for h in holdings:
            chg = changes_by_code.get(h["code"], {}).get(date)
            if chg is not None:
                num += h["weight"] * chg
                quoted_w += h["weight"]
        if coverage <= 0 or quoted_w < 0.5 * coverage:
            skipped += 1
            continue
        est_old = num / 100
        est_new = est_old + (residual * (num / quoted_w) / 100 if quoted_w else 0.0)
        pairs_old.append((est_old, official))
        pairs_new.append((est_new, official))
    return {
        "code": code, "coverage": coverage, "stock_position": stock_position,
        "residual": residual, "missing": missing, "skipped": skipped,
        "old": metrics(pairs_old), "new": metrics(pairs_new),
        "pairs_old": pairs_old, "pairs_new": pairs_new,
        "window": (nav_rows[-1][0], nav_rows[0][0]) if nav_rows else None,
    }


def fmt_m(m: dict) -> str:
    if not m.get("n"):
        return "n=0"
    da = f"{m['dir_acc'] * 100:.1f}%" if m.get("dir_acc") is not None else "-"
    return (f"n={m['n']} MAE={m['mae']:.3f} RMSE={m['rmse']:.3f} "
            f"bias={m['bias']:+.3f} dir={da} (n={m['dir_n']})")


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=60)
    args = ap.parse_args()
    t0 = time.time()
    results = []
    for code in FUNDS:
        print(f"[{code}] fetching...", flush=True)
        try:
            r = await backtest_fund(code, args.days)
        except Exception as e:
            print(f"[{code}] FAILED: {e!r}", flush=True)
            continue
        results.append(r)
        print(f"[{code}] cov={r['coverage']:.2f} stock_pos={r['stock_position']} "
              f"residual={r['residual']:.2f} missing={r['missing']} skipped={r['skipped']}")
        print(f"  old: {fmt_m(r['old'])}")
        print(f"  new: {fmt_m(r['new'])}", flush=True)

    all_old = [p for r in results for p in r["pairs_old"]]
    all_new = [p for r in results for p in r["pairs_new"]]
    mo, mn = metrics(all_old), metrics(all_new)
    print(f"\nOVERALL old: {fmt_m(mo)}")
    print(f"OVERALL new: {fmt_m(mn)}")

    lines = [
        "# 估值回测基线（P0 未披露仓位补齐）",
        "",
        f"生成时间：{datetime.now(timezone(timedelta(hours=8))):%Y-%m-%d %H:%M} CST · "
        f"窗口：每只基金最近 {args.days} 个净值日 · 耗时 {time.time() - t0:.0f}s",
        "",
        "方法：官方日涨跌（东财 lsjz JZZZL）vs 用当前前十大持仓与各持仓股日线收盘"
        "（Yahoo Finance；日/韩/台股走东财 kline）计算的估算涨跌。"
        "est_old = Σ（权重×涨跌）/100；est_new = est_old + 残余仓位 × 已披露持仓加权平均涨跌 /100，"
        "残余仓位 = 股票总仓位（最新季报资产配置）− 前十大覆盖。",
        "",
        "局限：① 前视偏差——全窗口使用当前持仓与最新股票仓位，经理调仓未计；"
        "② QDII 对齐——净值日 D 与各市场股票在 D 日的涨跌对齐（美股为美东 D 日），与盘中工具口径略有差异；"
        "③ 方向准确率剔除 |官方涨跌| < 0.05% 的日子；④ 单日已披露权重有行情覆盖 <50% 的日子跳过。",
        "",
        "| 基金 | 覆盖% | 股票仓位% | 残余% | 口径 | n | MAE | RMSE | bias | 方向准确率 |",
        "|---|---|---|---|---|---|---|---|---|---|",
    ]
    for r in results:
        for label, m in (("旧（无补齐）", r["old"]), ("新（P0补齐）", r["new"])):
            if not m.get("n"):
                continue
            da = f"{m['dir_acc'] * 100:.1f}%" if m.get("dir_acc") is not None else "-"
            lines.append(
                f"| {r['code']} | {r['coverage']:.2f} | "
                f"{r['stock_position'] if r['stock_position'] is not None else '-'} | "
                f"{r['residual']:.2f} | {label} | {m['n']} | {m['mae']:.3f} | "
                f"{m['rmse']:.3f} | {m['bias']:+.3f} | {da} |")
    for label, m in (("旧（无补齐）", mo), ("新（P0补齐）", mn)):
        if m.get("n"):
            da = f"{m['dir_acc'] * 100:.1f}%" if m.get("dir_acc") is not None else "-"
            lines.append(f"| **全部** | - | - | - | **{label}** | {m['n']} | "
                         f"{m['mae']:.3f} | {m['rmse']:.3f} | {m['bias']:+.3f} | {da} |")
    lines += ["", "缺失行情的持仓（整窗无日线）：" +
              ("、".join(f"{r['code']}:{'/'.join(r['missing'])}" for r in results if r["missing"]) or "无")]
    REPORT_PATH.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"\nreport -> {REPORT_PATH}")


if __name__ == "__main__":
    asyncio.run(main())
