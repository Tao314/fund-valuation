#!/usr/bin/env python3
"""Fund self-calculated real-time valuation MVP.

Estimation: est_change_pct = sum(weight_i% * stock_change_i%) / 100 over
disclosed top-10 holdings; est_nav = latest official NAV * (1 + est/100).
Data: Eastmoney (search / official NAV / holdings), Sina (stock quotes).
All upstream calls go through the backend with global caches.
"""
import hashlib
import hmac
import json
import os
import re
import secrets
import sqlite3
import time
from contextlib import contextmanager
from typing import Optional
from datetime import datetime, timezone
from html.parser import HTMLParser
from pathlib import Path

import httpx
from fastapi import Cookie, FastAPI, HTTPException, Request, Response
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel

# Some environments set NO_PROXY with IPv6 entries that crash httpx URL parsing;
# normalize to a simple value (harmless where no proxy is configured).
for _k in ("NO_PROXY", "no_proxy"):
    if os.environ.get(_k):
        os.environ[_k] = "localhost,127.0.0.1"

BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "fund.db"
STATIC_DIR = BASE_DIR / "static"
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36"
FUND_CODE_RE = re.compile(r"^\d{6}$")
QUOTE_TTL = 45          # seconds, in-memory global cache
NAV_TTL = 3600          # seconds, DB cache
HOLDINGS_TTL = 86400    # seconds, DB cache
SESSION_DAYS = 30

app = FastAPI(title="Fund Valuation MVP")

# ---------------------------------------------------------------- DB
def init_db():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS users(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE NOT NULL,
            password_hash TEXT NOT NULL,
            created_at TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS sessions(
            token TEXT PRIMARY KEY,
            user_id INTEGER NOT NULL REFERENCES users(id),
            created_at TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS user_funds(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL REFERENCES users(id),
            fund_code TEXT NOT NULL,
            fund_name TEXT NOT NULL,
            sort_order INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL,
            UNIQUE(user_id, fund_code));
        CREATE INDEX IF NOT EXISTS idx_user_funds_user ON user_funds(user_id);
        CREATE TABLE IF NOT EXISTS holdings_cache(
            fund_code TEXT PRIMARY KEY,
            holdings_json TEXT NOT NULL,
            report_label TEXT,
            report_date TEXT,
            fetched_at REAL NOT NULL);
        CREATE TABLE IF NOT EXISTS nav_cache(
            fund_code TEXT PRIMARY KEY,
            nav_date TEXT,
            dwjz REAL,
            jzzzl REAL,
            fetched_at REAL NOT NULL);
        """
    )
    # Migration: user_funds.category (added 2026-11-19)
    cols = [r[1] for r in conn.execute("PRAGMA table_info(user_funds)").fetchall()]
    if "category" not in cols:
        conn.execute("ALTER TABLE user_funds ADD COLUMN category TEXT NOT NULL DEFAULT ''")
    conn.commit()
    conn.close()


@contextmanager
def db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def now_iso():
    return datetime.now(timezone.utc).isoformat()


# ---------------------------------------------------------------- auth
def hash_password(password: str) -> str:
    salt = secrets.token_hex(16)
    dk = hashlib.pbkdf2_hmac("sha256", password.encode(), bytes.fromhex(salt), 200_000)
    return f"{salt}${dk.hex()}"


def check_password(password: str, stored: str) -> bool:
    try:
        salt, hexdk = stored.split("$", 1)
        dk = hashlib.pbkdf2_hmac("sha256", password.encode(), bytes.fromhex(salt), 200_000)
        return hmac.compare_digest(dk.hex(), hexdk)
    except Exception:
        return False


def current_user_id(session: Optional[str]) -> int:
    if not session:
        raise HTTPException(401, "未登录")
    with db() as conn:
        row = conn.execute("SELECT user_id FROM sessions WHERE token=?", (session,)).fetchone()
    if not row:
        raise HTTPException(401, "会话已失效，请重新登录")
    return row["user_id"]


class AuthBody(BaseModel):
    username: str
    password: str


class AddFundBody(BaseModel):
    fund_code: str


class ReorderBody(BaseModel):
    ids: list


class CategoryBody(BaseModel):
    category: str


@app.post("/api/register")
def register(body: AuthBody):
    username = body.username.strip()
    if len(username) < 3 or len(username) > 32:
        raise HTTPException(400, "用户名需 3-32 个字符")
    if len(body.password) < 6:
        raise HTTPException(400, "密码至少 6 位")
    with db() as conn:
        try:
            cur = conn.execute(
                "INSERT INTO users(username, password_hash, created_at) VALUES(?,?,?)",
                (username, hash_password(body.password), now_iso()),
            )
        except sqlite3.IntegrityError:
            raise HTTPException(409, "用户名已存在")
        user_id = cur.lastrowid
    return {"ok": True, "user_id": user_id, "username": username}


@app.post("/api/login")
def login(body: AuthBody, response: Response):
    with db() as conn:
        row = conn.execute("SELECT id, password_hash FROM users WHERE username=?", (body.username.strip(),)).fetchone()
        if not row or not check_password(body.password, row["password_hash"]):
            raise HTTPException(401, "用户名或密码错误")
        token = secrets.token_urlsafe(32)
        conn.execute("INSERT INTO sessions(token, user_id, created_at) VALUES(?,?,?)", (token, row["id"], now_iso()))
    response.set_cookie("session", token, max_age=SESSION_DAYS * 86400, httponly=True, samesite="lax", path="/")
    return {"ok": True, "username": body.username.strip()}


@app.post("/api/logout")
def logout(response: Response, session: Optional[str] = Cookie(default=None)):
    if session:
        with db() as conn:
            conn.execute("DELETE FROM sessions WHERE token=?", (session,))
    response.delete_cookie("session", path="/")
    return {"ok": True}


@app.get("/api/me")
def me(session: Optional[str] = Cookie(default=None)):
    uid = current_user_id(session)
    with db() as conn:
        row = conn.execute("SELECT username FROM users WHERE id=?", (uid,)).fetchone()
    return {"user_id": uid, "username": row["username"]}


# ---------------------------------------------------------------- upstream
async def upstream_get(url: str, referer: Optional[str] = None) -> httpx.Response:
    headers = {"User-Agent": UA}
    if referer:
        headers["Referer"] = referer
    async with httpx.AsyncClient(timeout=10, follow_redirects=True) as client:
        resp = await client.get(url, headers=headers)
        resp.raise_for_status()
        return resp


@app.get("/api/search")
async def search(q: str, session: Optional[str] = Cookie(default=None)):
    current_user_id(session)
    q = q.strip()
    if not q:
        return {"results": []}
    from urllib.parse import quote
    resp = await upstream_get(f"https://fundsuggest.eastmoney.com/FundSearch/api/FundSearchAPI.ashx?m=1&key={quote(q)}")
    data = resp.json()
    results = [{"code": d.get("CODE"), "name": d.get("NAME")} for d in data.get("Datas", []) if d.get("CODE")]
    return {"results": results[:20]}


async def fetch_nav(code: str) -> dict:
    with db() as conn:
        row = conn.execute("SELECT * FROM nav_cache WHERE fund_code=?", (code,)).fetchone()
    if row and time.time() - row["fetched_at"] < NAV_TTL:
        return {"nav_date": row["nav_date"], "dwjz": row["dwjz"], "jzzzl": row["jzzzl"], "stale": False}
    try:
        resp = await upstream_get(
            f"https://api.fund.eastmoney.com/f10/lsjz?fundCode={code}&pageIndex=1&pageSize=1",
            referer=f"https://fund.eastmoney.com/f10/jjjz_{code}.html",
        )
        item = resp.json()["Data"]["LSJZList"][0]
        nav = {"nav_date": item["FSRQ"], "dwjz": float(item["DWJZ"]), "jzzzl": float(item["JZZZL"] or 0)}
        with db() as conn:
            conn.execute(
                "INSERT INTO nav_cache(fund_code, nav_date, dwjz, jzzzl, fetched_at) VALUES(?,?,?,?,?) "
                "ON CONFLICT(fund_code) DO UPDATE SET nav_date=excluded.nav_date, dwjz=excluded.dwjz, jzzzl=excluded.jzzzl, fetched_at=excluded.fetched_at",
                (code, nav["nav_date"], nav["dwjz"], nav["jzzzl"], time.time()),
            )
        return {**nav, "stale": False}
    except Exception:
        if row:
            return {"nav_date": row["nav_date"], "dwjz": row["dwjz"], "jzzzl": row["jzzzl"], "stale": True}
        raise HTTPException(502, f"净值数据暂时不可用（{code}）")


class _TableParser(HTMLParser):
    """Collect table rows as lists of cell text."""

    def __init__(self):
        super().__init__()
        self.rows, self._row, self._cell = [], None, None

    def handle_starttag(self, tag, attrs):
        if tag == "tr":
            self._row = []
        elif tag in ("td", "th") and self._row is not None:
            self._cell = []

    def handle_endtag(self, tag):
        if tag in ("td", "th") and self._cell is not None:
            self._row.append("".join(self._cell).strip())
            self._cell = None
        elif tag == "tr" and self._row is not None:
            if self._row:
                self.rows.append(self._row)
            self._row = None

    def handle_data(self, data):
        if self._cell is not None:
            self._cell.append(data)


def parse_holdings(text: str) -> dict:
    # Extract the HTML inside var apidata={ content:"..." }
    m = re.search(r'content:"(.*)"\s*[,}]', text, re.S)
    html = m.group(1).replace('\\"', '"') if m else text
    label_m = re.search(r"(\d{4}年\d季度)", html)
    date_m = re.search(r"截止至：<font[^>]*>([\d-]+)</font>", html)
    parser = _TableParser()
    parser.feed(html)
    holdings = []
    for row in parser.rows:
        # data rows: 序号, 股票代码, 股票名称, 最新价, 涨跌幅, 相关资讯, 占净值比例, 持股数, 持仓市值
        if len(row) >= 7 and re.fullmatch(r"\d+", row[0] or "") and row[1]:
            wm = re.search(r"([\d.]+)\s*%", row[6])
            if not wm:
                continue
            holdings.append({"code": row[1].strip(), "name": row[2].strip(), "weight": float(wm.group(1))})
    return {
        "holdings": holdings[:10],
        "report_label": label_m.group(1) if label_m else None,
        "report_date": date_m.group(1) if date_m else None,
    }


async def fetch_holdings(code: str) -> dict:
    with db() as conn:
        row = conn.execute("SELECT * FROM holdings_cache WHERE fund_code=?", (code,)).fetchone()
    if row and time.time() - row["fetched_at"] < HOLDINGS_TTL:
        return {"holdings": json.loads(row["holdings_json"]), "report_label": row["report_label"],
                "report_date": row["report_date"], "stale": False}
    try:
        resp = await upstream_get(
            f"https://fundf10.eastmoney.com/FundArchivesDatas.aspx?type=jjcc&code={code}&topline=10&year=&month=&rt=0.1",
            referer=f"https://fundf10.eastmoney.com/ccmx_{code}.html",
        )
        parsed = parse_holdings(resp.text)
        if not parsed["holdings"]:
            raise ValueError("no holdings parsed")
        with db() as conn:
            conn.execute(
                "INSERT INTO holdings_cache(fund_code, holdings_json, report_label, report_date, fetched_at) VALUES(?,?,?,?,?) "
                "ON CONFLICT(fund_code) DO UPDATE SET holdings_json=excluded.holdings_json, report_label=excluded.report_label, report_date=excluded.report_date, fetched_at=excluded.fetched_at",
                (code, json.dumps(parsed["holdings"], ensure_ascii=False), parsed["report_label"], parsed["report_date"], time.time()),
            )
        return {**parsed, "stale": False}
    except Exception:
        if row:
            return {"holdings": json.loads(row["holdings_json"]), "report_label": row["report_label"],
                    "report_date": row["report_date"], "stale": True}
        return {"holdings": [], "report_label": None, "report_date": None, "stale": True}


def to_sina_symbol(code: str) -> Optional[str]:
    code = code.strip()
    if re.fullmatch(r"\d{6}", code):
        return ("sh" if code[0] in "69" else "sz") + code
    if re.fullmatch(r"\d{5}", code):
        return "hk" + code
    if re.fullmatch(r"[A-Za-z][A-Za-z0-9.\-]*", code):
        return "gb_" + code.lower()  # US stocks via Sina gb_ symbols (verified 2026-11-19)
    return None


_quote_cache: dict = {"at": 0.0, "quotes": {}}


async def fetch_quotes(symbols: list[str]) -> dict:
    """Return {symbol: {name, prev_close, current, change_pct, quote_time}}."""
    symbols = sorted(set(symbols))
    if not symbols:
        return {}
    now = time.time()
    cached = _quote_cache["quotes"]
    if now - _quote_cache["at"] < QUOTE_TTL and all(s in cached for s in symbols):
        return {s: cached[s] for s in symbols}
    try:
        resp = await upstream_get("https://hq.sinajs.cn/list=" + ",".join(symbols), referer="https://finance.sina.com.cn/")
        text = resp.content.decode("gbk", errors="replace")
        quotes = {}
        for line in text.splitlines():
            m = re.match(r'var hq_str_(\w+)="(.*)";', line.strip())
            if not m:
                continue
            sym, fields = m.group(1), m.group(2).split(",")
            try:
                if sym.startswith("gb_"):
                    # US layout (verified 2026-11-19): 0=name,1=current,2=change%,3=Beijing datetime,26=prev close
                    cur = float(fields[1])
                    prev = float(fields[26]) if len(fields) > 26 and fields[26] else cur
                    quotes[sym] = {"name": fields[0], "prev_close": prev, "current": cur,
                                   "change_pct": float(fields[2]),
                                   "quote_time": fields[3] if len(fields) > 3 else None}
                elif sym.startswith("hk"):
                    # HK layout (verified 2026-11-19): 0=EN name,1=CN name,2=open,3=prev close,
                    # 4=high,5=low,6=current,7=change,8=change%,17=date,18=time
                    prev, cur = float(fields[3]), float(fields[6])
                    quotes[sym] = {"name": fields[1], "prev_close": prev, "current": cur,
                                   "change_pct": (cur - prev) / prev * 100 if prev else 0.0,
                                   "quote_time": f"{fields[17]} {fields[18]}" if len(fields) > 18 else None}
                else:
                    # A-share layout: 0=name,1=open,2=prev close,3=current,30=date,31=time
                    prev, cur = float(fields[2]), float(fields[3])
                    quotes[sym] = {"name": fields[0], "prev_close": prev, "current": cur,
                                   "change_pct": (cur - prev) / prev * 100 if prev else 0.0,
                                   "quote_time": f"{fields[30]} {fields[31]}" if len(fields) > 31 else None}
            except (ValueError, IndexError):
                continue
        _quote_cache.update(at=now, quotes={**cached, **quotes})
        return {s: _quote_cache["quotes"][s] for s in symbols if s in _quote_cache["quotes"]}
    except Exception:
        return {s: cached[s] for s in symbols if s in cached}


# ---------------------------------------------------------------- watchlist
@app.get("/api/watchlist")
def get_watchlist(session: Optional[str] = Cookie(default=None)):
    uid = current_user_id(session)
    with db() as conn:
        rows = conn.execute(
            "SELECT id, fund_code, fund_name, sort_order, category FROM user_funds WHERE user_id=? ORDER BY sort_order, id", (uid,)
        ).fetchall()
    return {"funds": [dict(r) for r in rows]}


@app.post("/api/watchlist")
async def add_watchlist(body: AddFundBody, session: Optional[str] = Cookie(default=None)):
    uid = current_user_id(session)
    code = body.fund_code.strip()
    if not FUND_CODE_RE.fullmatch(code):
        raise HTTPException(400, "基金代码需为 6 位数字")
    # Resolve fund name via search API (also validates the code exists)
    resp = await upstream_get(f"https://fundsuggest.eastmoney.com/FundSearch/api/FundSearchAPI.ashx?m=1&key={code}")
    name = next((d["NAME"] for d in resp.json().get("Datas", []) if d.get("CODE") == code), None)
    if not name:
        raise HTTPException(404, "未找到该基金代码")
    with db() as conn:
        try:
            cur = conn.execute(
                "INSERT INTO user_funds(user_id, fund_code, fund_name, sort_order, created_at) "
                "VALUES(?,?,?,(SELECT COALESCE(MAX(sort_order),-1)+1 FROM user_funds WHERE user_id=?),?)",
                (uid, code, name, uid, now_iso()),
            )
        except sqlite3.IntegrityError:
            raise HTTPException(409, "该基金已在列表中")
    return {"ok": True, "id": cur.lastrowid, "fund_code": code, "fund_name": name}


@app.delete("/api/watchlist/{item_id}")
def delete_watchlist(item_id: int, session: Optional[str] = Cookie(default=None)):
    uid = current_user_id(session)
    with db() as conn:
        cur = conn.execute("DELETE FROM user_funds WHERE id=? AND user_id=?", (item_id, uid))
    if cur.rowcount == 0:
        raise HTTPException(404, "记录不存在")
    return {"ok": True}


@app.post("/api/watchlist/reorder")
def reorder_watchlist(body: ReorderBody, session: Optional[str] = Cookie(default=None)):
    uid = current_user_id(session)
    ids = []
    for v in body.ids:
        try:
            ids.append(int(v))
        except (TypeError, ValueError):
            continue
    with db() as conn:
        owned = {r["id"] for r in conn.execute("SELECT id FROM user_funds WHERE user_id=?", (uid,)).fetchall()}
        updated = 0
        for pos, item_id in enumerate(ids):
            if item_id in owned:  # foreign ids are ignored, never touched
                conn.execute("UPDATE user_funds SET sort_order=? WHERE id=? AND user_id=?", (pos, item_id, uid))
                updated += 1
    return {"ok": True, "updated": updated}


@app.post("/api/watchlist/{item_id}/category")
def set_category(item_id: int, body: CategoryBody, session: Optional[str] = Cookie(default=None)):
    uid = current_user_id(session)
    category = (body.category or "").strip()
    if len(category) > 20:
        raise HTTPException(400, "分类名最多 20 个字符")
    with db() as conn:
        cur = conn.execute(
            "UPDATE user_funds SET category=? WHERE id=? AND user_id=?", (category, item_id, uid)
        )
    if cur.rowcount == 0:
        raise HTTPException(404, "记录不存在")
    return {"ok": True, "category": category}


@app.get("/api/estimates")
async def estimates(session: Optional[str] = Cookie(default=None)):
    uid = current_user_id(session)
    with db() as conn:
        rows = conn.execute(
            "SELECT id, fund_code, fund_name, category FROM user_funds WHERE user_id=? ORDER BY sort_order, id", (uid,)
        ).fetchall()
    # Fetch NAV + holdings per fund, collect all symbols, one batch quote call
    per_fund, all_symbols = [], []
    for r in rows:
        nav = await fetch_nav(r["fund_code"])
        hold = await fetch_holdings(r["fund_code"])
        syms = [to_sina_symbol(h["code"]) for h in hold["holdings"]]
        all_symbols += [s for s in syms if s]
        per_fund.append((r, nav, hold, syms))
    quotes = await fetch_quotes(all_symbols)
    out = []
    for r, nav, hold, syms in per_fund:
        est_pct, coverage, quote_time, skipped = 0.0, 0.0, None, []
        detail = []
        for h, sym in zip(hold["holdings"], syms):
            coverage += h["weight"]
            q = quotes.get(sym) if sym else None
            if q:
                est_pct += h["weight"] * q["change_pct"] / 100
                qt = q["quote_time"]
                if qt and (quote_time is None or qt.replace("/", "-") > quote_time.replace("/", "-")):
                    quote_time = qt
                detail.append({**h, "change_pct": round(q["change_pct"], 2)})
            else:
                skipped.append(h["code"])
                detail.append({**h, "change_pct": None})
        est_nav = nav["dwjz"] * (1 + est_pct / 100) if nav["dwjz"] else None
        out.append({
            "id": r["id"], "fund_code": r["fund_code"], "fund_name": r["fund_name"], "category": r["category"] or "",
            "nav_date": nav["nav_date"], "dwjz": nav["dwjz"], "jzzzl": nav["jzzzl"],
            "est_nav": round(est_nav, 4) if est_nav else None,
            "est_change_pct": round(est_pct, 2),
            "coverage_pct": round(coverage, 2),
            "report_label": hold["report_label"], "report_date": hold["report_date"],
            "quote_time": quote_time, "stale": nav["stale"] or hold["stale"],
            "skipped_holdings": skipped, "holdings": detail,
        })
    return {"funds": out, "disclaimer": "自算估值，仅供参考，以基金公司公布净值为准"}


@app.get("/")
def index():
    return FileResponse(STATIC_DIR / "index.html")


init_db()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host=os.environ.get("HOST", "127.0.0.1"), port=int(os.environ.get("PORT", "8000")))
