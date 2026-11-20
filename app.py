#!/usr/bin/env python3
"""Fund self-calculated real-time valuation MVP.

Estimation: est_change_pct = sum(weight_i% * stock_change_i%) / 100 over
disclosed top-10 holdings; est_nav = latest official NAV * (1 + est/100).
Data: Eastmoney (search / official NAV / holdings), Sina (stock quotes).
All upstream calls go through the backend with global caches.
"""
import asyncio
import hashlib
import hmac
import json
import math
import os
import re
import secrets
import sqlite3
import time
from contextlib import asynccontextmanager, contextmanager
from typing import Optional
from datetime import datetime, timedelta, timezone
from html.parser import HTMLParser
from zoneinfo import ZoneInfo
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
NAV_TTL = 600           # seconds, DB cache
HOLDINGS_TTL = 86400    # seconds, DB cache
SESSION_DAYS = 30

@asynccontextmanager
async def lifespan(_app):
    # Ranking caches warm in the background (see _rank_warm_loop); startup
    # itself never waits on upstream fetches. The quote warmer keeps the
    # in-memory quote cache hot so first page loads don't wait on Sina.
    tasks = [asyncio.create_task(_rank_warm_loop()),
             asyncio.create_task(_quote_warm_loop()),
             asyncio.create_task(_trends_warm_loop())]
    try:
        yield
    finally:
        for t in tasks:
            t.cancel()


app = FastAPI(title="Fund Valuation MVP", lifespan=lifespan)

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
            prev_nav_date TEXT,
            prev_dwjz REAL,
            prev_jzzzl REAL,
            fetched_at REAL NOT NULL);
        CREATE TABLE IF NOT EXISTS rank_cache(
            key TEXT PRIMARY KEY,
            payload TEXT NOT NULL,
            fetched_at TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS est_history(
            fund_code TEXT NOT NULL,
            ts INTEGER NOT NULL,
            est_change_pct REAL NOT NULL);
        CREATE INDEX IF NOT EXISTS idx_est_history ON est_history(fund_code, ts);
        CREATE TABLE IF NOT EXISTS stock_trends_cache(
            secid TEXT NOT NULL,
            date TEXT NOT NULL,
            payload TEXT NOT NULL,
            fetched_at REAL NOT NULL,
            PRIMARY KEY(secid, date));
        CREATE TABLE IF NOT EXISTS fund_index_map(
            fund_code TEXT PRIMARY KEY,
            index_name TEXT,
            secid TEXT,
            fetched_at REAL NOT NULL);
        CREATE TABLE IF NOT EXISTS em_secid_map(
            code TEXT PRIMARY KEY,
            name TEXT,
            secid TEXT,
            fetched_at REAL NOT NULL);
        """
    )
    # Migration: user_funds.category (added 2026-11-19)
    cols = [r[1] for r in conn.execute("PRAGMA table_info(user_funds)").fetchall()]
    if "category" not in cols:
        conn.execute("ALTER TABLE user_funds ADD COLUMN category TEXT NOT NULL DEFAULT ''")
    # Migration: users.refresh_seconds (added 2026-11-20)
    ucols = [r[1] for r in conn.execute("PRAGMA table_info(users)").fetchall()]
    if "refresh_seconds" not in ucols:
        conn.execute("ALTER TABLE users ADD COLUMN refresh_seconds INTEGER NOT NULL DEFAULT 60")
    # Migration: users.is_admin (added 2026-11-20, v1.12.0)
    if "is_admin" not in ucols:
        conn.execute("ALTER TABLE users ADD COLUMN is_admin INTEGER NOT NULL DEFAULT 0")
    # Login log table (added 2026-11-20, v1.12.0)
    conn.execute(
        "CREATE TABLE IF NOT EXISTS login_log("
        "id INTEGER PRIMARY KEY AUTOINCREMENT, username TEXT NOT NULL, "
        "user_id INTEGER, success INTEGER NOT NULL, ip TEXT, "
        "user_agent TEXT, created_at TEXT NOT NULL)"
    )
    # Promote the configured admin account (idempotent; no-op until it registers)
    admin_name = os.environ.get("ADMIN_USERNAME", "allen314").strip()
    if admin_name:
        conn.execute("UPDATE users SET is_admin=1 WHERE username=?", (admin_name,))
    # Migration: nav_cache prev-day NAV columns (added 2026-11-20)
    ncols = [r[1] for r in conn.execute("PRAGMA table_info(nav_cache)").fetchall()]
    for col, ddl in (("prev_nav_date", "TEXT"), ("prev_dwjz", "REAL"), ("prev_jzzzl", "REAL")):
        if col not in ncols:
            conn.execute(f"ALTER TABLE nav_cache ADD COLUMN {col} {ddl}")
    # Category ordering table + backfill from existing fund categories (idempotent)
    conn.execute(
        "CREATE TABLE IF NOT EXISTS user_categories("
        "user_id INTEGER NOT NULL, name TEXT NOT NULL, "
        "sort_order INTEGER NOT NULL DEFAULT 0, PRIMARY KEY(user_id, name))"
    )
    cat_rows = conn.execute(
        "SELECT user_id, category, MIN(sort_order) AS m FROM user_funds "
        "WHERE category <> '' GROUP BY user_id, category ORDER BY user_id, m, category"
    ).fetchall()
    for c_uid, c_name, _m in cat_rows:
        nxt = conn.execute(
            "SELECT COALESCE(MAX(sort_order),-1)+1 FROM user_categories WHERE user_id=?", (c_uid,)
        ).fetchone()[0]
        conn.execute(
            "INSERT OR IGNORE INTO user_categories(user_id, name, sort_order) VALUES(?,?,?)",
            (c_uid, c_name, nxt),
        )
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


def require_admin(session: Optional[str]) -> int:
    uid = current_user_id(session)  # 401 when unauthenticated
    with db() as conn:
        row = conn.execute("SELECT is_admin FROM users WHERE id=?", (uid,)).fetchone()
    if not row or not row["is_admin"]:
        raise HTTPException(403, "需要管理员权限")
    return uid


def _client_ip(request: Request) -> str:
    xff = request.headers.get("x-forwarded-for", "")
    if xff:
        return xff.split(",")[0].strip()
    return request.client.host if request.client else ""


def _log_login(conn, username: str, user_id, success: bool, request: Request):
    conn.execute(
        "INSERT INTO login_log(username, user_id, success, ip, user_agent, created_at) VALUES(?,?,?,?,?,?)",
        (username, user_id, 1 if success else 0, _client_ip(request),
         request.headers.get("user-agent", "")[:200], now_iso()),
    )


class AuthBody(BaseModel):
    username: str
    password: str


class AddFundBody(BaseModel):
    fund_code: str
    category: str = ""


class ReorderBody(BaseModel):
    ids: list
    category: str = ""


class CategoryReorderBody(BaseModel):
    names: list


class CategoryBody(BaseModel):
    category: str


class SettingsBody(BaseModel):
    refresh_seconds: int


class CategoryCreateBody(BaseModel):
    name: str


class CategoryRenameBody(BaseModel):
    old: str
    new: str


class CategoryDeleteBody(BaseModel):
    name: str


class ChangePasswordBody(BaseModel):
    old_password: str
    new_password: str


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
def login(body: AuthBody, response: Response, request: Request):
    username = body.username.strip()
    with db() as conn:
        row = conn.execute("SELECT id, password_hash FROM users WHERE username=?", (username,)).fetchone()
        if not row or not check_password(body.password, row["password_hash"]):
            _log_login(conn, username, row["id"] if row else None, False, request)
            conn.commit()  # db() skips its commit when the block raises
            raise HTTPException(401, "用户名或密码错误")
        token = secrets.token_urlsafe(32)
        conn.execute("INSERT INTO sessions(token, user_id, created_at) VALUES(?,?,?)", (token, row["id"], now_iso()))
        _log_login(conn, username, row["id"], True, request)
    response.set_cookie("session", token, max_age=SESSION_DAYS * 86400, httponly=True, samesite="lax", path="/")
    return {"ok": True, "username": username}


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
        row = conn.execute("SELECT username, is_admin FROM users WHERE id=?", (uid,)).fetchone()
    return {"user_id": uid, "username": row["username"], "is_admin": bool(row["is_admin"])}


@app.get("/api/settings")
def get_settings(session: Optional[str] = Cookie(default=None)):
    uid = current_user_id(session)
    with db() as conn:
        row = conn.execute("SELECT refresh_seconds FROM users WHERE id=?", (uid,)).fetchone()
    return {"refresh_seconds": row["refresh_seconds"] if row else 60}


@app.post("/api/settings")
def save_settings(body: SettingsBody, session: Optional[str] = Cookie(default=None)):
    uid = current_user_id(session)
    n = body.refresh_seconds
    if n < 10 or n > 600:
        raise HTTPException(400, "自动刷新时间需 10-600 秒")
    with db() as conn:
        conn.execute("UPDATE users SET refresh_seconds=? WHERE id=?", (n, uid))
    return {"ok": True, "refresh_seconds": n}


@app.post("/api/change-password")
def change_password(body: ChangePasswordBody, session: Optional[str] = Cookie(default=None)):
    uid = current_user_id(session)
    if len(body.new_password) < 6:  # same rule as register
        raise HTTPException(400, "密码至少 6 位")
    with db() as conn:
        row = conn.execute("SELECT password_hash FROM users WHERE id=?", (uid,)).fetchone()
        if not row or not check_password(body.old_password, row["password_hash"]):
            raise HTTPException(400, "旧密码不正确")
        conn.execute("UPDATE users SET password_hash=? WHERE id=?", (hash_password(body.new_password), uid))
        # Invalidate all other sessions, keep the current one
        conn.execute("DELETE FROM sessions WHERE user_id=? AND token<>?", (uid, session))
    return {"ok": True}


# ---------------------------------------------------------------- admin (v1.12.0)
class AdminPasswordBody(BaseModel):
    password: str


@app.get("/api/admin/users")
def admin_list_users(session: Optional[str] = Cookie(default=None)):
    require_admin(session)
    with db() as conn:
        rows = conn.execute(
            "SELECT u.id, u.username, u.is_admin, u.created_at, "
            "(SELECT COUNT(*) FROM user_funds WHERE user_id=u.id) AS fund_count, "
            "(SELECT MAX(created_at) FROM login_log WHERE user_id=u.id AND success=1) AS last_login "
            "FROM users u ORDER BY u.id"
        ).fetchall()
    return {"users": [
        {"id": r["id"], "username": r["username"], "is_admin": bool(r["is_admin"]),
         "created_at": r["created_at"], "fund_count": r["fund_count"], "last_login": r["last_login"]}
        for r in rows
    ]}


@app.post("/api/admin/users")
def admin_create_user(body: AuthBody, session: Optional[str] = Cookie(default=None)):
    require_admin(session)
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


@app.delete("/api/admin/users/{user_id}")
def admin_delete_user(user_id: int, session: Optional[str] = Cookie(default=None)):
    uid = require_admin(session)
    if user_id == uid:
        raise HTTPException(400, "不能删除自己")
    with db() as conn:
        row = conn.execute("SELECT username, is_admin FROM users WHERE id=?", (user_id,)).fetchone()
        if not row:
            raise HTTPException(404, "用户不存在")
        if row["is_admin"]:
            raise HTTPException(400, "不能删除管理员")
        conn.execute("DELETE FROM sessions WHERE user_id=?", (user_id,))
        conn.execute("DELETE FROM user_funds WHERE user_id=?", (user_id,))
        conn.execute("DELETE FROM user_categories WHERE user_id=?", (user_id,))
        conn.execute("DELETE FROM users WHERE id=?", (user_id,))
    return {"ok": True}


@app.post("/api/admin/users/{user_id}/password")
def admin_reset_password(user_id: int, body: AdminPasswordBody, session: Optional[str] = Cookie(default=None)):
    require_admin(session)
    if len(body.password) < 6:
        raise HTTPException(400, "密码至少 6 位")
    with db() as conn:
        row = conn.execute("SELECT id FROM users WHERE id=?", (user_id,)).fetchone()
        if not row:
            raise HTTPException(404, "用户不存在")
        conn.execute("UPDATE users SET password_hash=? WHERE id=?", (hash_password(body.password), user_id))
        conn.execute("DELETE FROM sessions WHERE user_id=?", (user_id,))
    return {"ok": True}


@app.get("/api/admin/login-logs")
def admin_login_logs(limit: int = 200, session: Optional[str] = Cookie(default=None)):
    require_admin(session)
    limit = max(1, min(limit, 500))
    with db() as conn:
        rows = conn.execute(
            "SELECT username, user_id, success, ip, user_agent, created_at "
            "FROM login_log ORDER BY id DESC LIMIT ?", (limit,)
        ).fetchall()
    return {"logs": [
        {"username": r["username"], "user_id": r["user_id"], "success": bool(r["success"]),
         "ip": r["ip"], "user_agent": (r["user_agent"] or "")[:120], "created_at": r["created_at"]}
        for r in rows
    ]}


# ---------------------------------------------------------------- upstream
async def upstream_get(url: str, referer: Optional[str] = None, timeout: float = 10) -> httpx.Response:
    headers = {"User-Agent": UA}
    if referer:
        headers["Referer"] = referer
    async with httpx.AsyncClient(timeout=timeout, follow_redirects=True) as client:
        resp = await client.get(url, headers=headers)
        resp.raise_for_status()
        return resp


def _is_fund_entry(d: dict) -> bool:
    """FundSearchAPI also returns stocks; funds have CATEGORY 700 (verified 2026-11-19)."""
    cat = d.get("CATEGORY")
    if cat is not None:
        return str(cat) == "700"
    return d.get("FundBaseInfo") is not None


@app.get("/api/search")
async def search(q: str, session: Optional[str] = Cookie(default=None)):
    current_user_id(session)
    q = q.strip()
    if not q:
        return {"results": []}
    from urllib.parse import quote
    resp = await upstream_get(f"https://fundsuggest.eastmoney.com/FundSearch/api/FundSearchAPI.ashx?m=1&key={quote(q)}")
    data = resp.json()
    results = [{"code": d.get("CODE"), "name": d.get("NAME")} for d in data.get("Datas", []) if d.get("CODE") and _is_fund_entry(d)]
    return {"results": results[:20]}


def _nav_from_row(row) -> dict:
    keys = row.keys()
    return {
        "nav_date": row["nav_date"], "dwjz": row["dwjz"], "jzzzl": row["jzzzl"],
        "prev_nav_date": row["prev_nav_date"] if "prev_nav_date" in keys else None,
        "prev_dwjz": row["prev_dwjz"] if "prev_dwjz" in keys else None,
        "prev_jzzzl": row["prev_jzzzl"] if "prev_jzzzl" in keys else None,
    }


async def fetch_nav(code: str) -> dict:
    with db() as conn:
        row = conn.execute("SELECT * FROM nav_cache WHERE fund_code=?", (code,)).fetchone()
    # Cache rows from before v1.8.0 have NULL prev_* (migrated columns); treat as stale
    # so prev-day NAV gets backfilled immediately instead of waiting out the TTL.
    if row and time.time() - row["fetched_at"] < NAV_TTL and _nav_from_row(row)["prev_dwjz"] is not None:
        return {**_nav_from_row(row), "stale": False}
    try:
        resp = await upstream_get(
            f"https://api.fund.eastmoney.com/f10/lsjz?fundCode={code}&pageIndex=1&pageSize=2",
            referer=f"https://fund.eastmoney.com/f10/jjjz_{code}.html",
        )
        items = resp.json()["Data"]["LSJZList"]
        item = items[0]
        prev = items[1] if len(items) > 1 else None
        nav = {
            "nav_date": item["FSRQ"], "dwjz": float(item["DWJZ"]), "jzzzl": float(item["JZZZL"] or 0),
            "prev_nav_date": prev["FSRQ"] if prev else None,
            "prev_dwjz": float(prev["DWJZ"]) if prev else None,
            "prev_jzzzl": float(prev["JZZZL"] or 0) if prev else None,
        }
        with db() as conn:
            conn.execute(
                "INSERT INTO nav_cache(fund_code, nav_date, dwjz, jzzzl, prev_nav_date, prev_dwjz, prev_jzzzl, fetched_at) "
                "VALUES(?,?,?,?,?,?,?,?) "
                "ON CONFLICT(fund_code) DO UPDATE SET nav_date=excluded.nav_date, dwjz=excluded.dwjz, jzzzl=excluded.jzzzl, "
                "prev_nav_date=excluded.prev_nav_date, prev_dwjz=excluded.prev_dwjz, prev_jzzzl=excluded.prev_jzzzl, "
                "fetched_at=excluded.fetched_at",
                (code, nav["nav_date"], nav["dwjz"], nav["jzzzl"],
                 nav["prev_nav_date"], nav["prev_dwjz"], nav["prev_jzzzl"], time.time()),
            )
        return {**nav, "stale": False}
    except Exception:
        if row:
            return {**_nav_from_row(row), "stale": True}
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
        parsed = await asyncio.to_thread(parse_holdings, resp.text)
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


ET_TZ = ZoneInfo("America/New_York")
CN_TZ = ZoneInfo("Asia/Shanghai")
_MONTHS = {m: i + 1 for i, m in enumerate(
    ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"])}


def us_session(now_et: Optional[datetime] = None) -> str:
    """US market session in ET (Mon-Fri): pre 04:00-09:30, trading 09:30-16:00,
    post 16:00-20:00, else closed. Holidays ignored."""
    if now_et is None:
        now_et = datetime.now(ET_TZ)
    if now_et.weekday() >= 5:
        return "closed"
    mins = now_et.hour * 60 + now_et.minute
    if 240 <= mins < 570:
        return "pre"
    if 570 <= mins < 960:
        return "trading"
    if 960 <= mins < 1200:
        return "post"
    return "closed"


def parse_ext_timestamp(s: str, now_et: datetime) -> Optional[datetime]:
    """Parse a Sina extended-hours timestamp like 'Nov 19 07:59PM EST'.

    Returns the ET datetime of the print (date + time) or None. The year is
    taken from now_et, adjusted across the year boundary to the closest match.
    """
    m = re.match(r"([A-Z][a-z]{2})\s+(\d{1,2})\s+(\d{1,2}):(\d{2})\s*([AP]M)", (s or "").strip())
    if not m:
        return None
    month = _MONTHS.get(m.group(1))
    if not month:
        return None
    day = int(m.group(2))
    hour = int(m.group(3)) % 12 + (12 if m.group(5) == "PM" else 0)
    minute = int(m.group(4))
    try:
        cand = datetime(now_et.year, month, day, hour, minute, tzinfo=ET_TZ)
        # Year boundary: pick the year that puts the date closest to today.
        if (cand.date() - now_et.date()).days > 183:
            cand = datetime(now_et.year - 1, month, day, hour, minute, tzinfo=ET_TZ)
        elif (now_et.date() - cand.date()).days > 183:
            cand = datetime(now_et.year + 1, month, day, hour, minute, tzinfo=ET_TZ)
    except ValueError:
        return None
    return cand


def _last_regular_close(now_et: datetime) -> datetime:
    """Most recent weekday 16:00 ET at or before now_et (holidays ignored)."""
    d = now_et.replace(hour=16, minute=0, second=0, microsecond=0)
    if d > now_et:
        d -= timedelta(days=1)
    while d.weekday() >= 5:
        d -= timedelta(days=1)
    return d


def us_quote_with_ext(fields: list, now_et: Optional[datetime] = None) -> dict:
    """Build a US quote dict from Sina gb_ fields.

    Regular layout: 0=name, 1=current, 2=change%, 3=Beijing datetime, 26=prev close.
    Extended hours: 21=ext price, 24=ext timestamp ('Nov 19 07:59PM EST').

    Returns BOTH changes:
    - change_pct: regular-session change (live price during regular hours,
      last close otherwise) — feeds the close-based main estimate.
    - change_pct_ext: extended-hours change (ext price vs prev close), or
      None when not valid — feeds the separate pre/post estimate.
    Ext is valid when:
    - pre/post session with a same-ET-date ext print, or
    - closed (overnight/weekend) and the ext print is from the most recent
      regular trading day's post session (at/after its 16:00 ET close) and
      no more than 4 days old.
    Regular session never has ext. Malformed ext data -> None.
    """
    if now_et is None:
        now_et = datetime.now(ET_TZ)
    cur = float(fields[1])
    prev = float(fields[26]) if len(fields) > 26 and fields[26] else cur
    change_pct_ext = None
    current_ext = None
    try:
        sess = us_session(now_et)
        if sess != "trading" and len(fields) > 24 and fields[21]:
            ext_price = float(fields[21])
            ext_dt = parse_ext_timestamp(fields[24], now_et)
            if ext_price > 0 and prev > 0 and ext_dt is not None:
                use_ext = False
                if sess in ("pre", "post") and ext_dt.date() == now_et.date():
                    use_ext = True
                elif sess == "closed" and ext_dt >= _last_regular_close(now_et) \
                        and now_et - ext_dt <= timedelta(days=4):
                    use_ext = True
                if use_ext:
                    current_ext = ext_price
                    change_pct_ext = (ext_price - prev) / prev * 100
    except Exception:
        pass  # malformed ext data -> no ext change
    return {"name": fields[0], "prev_close": prev, "current": cur,
            "change_pct": float(fields[2]),
            "current_ext": current_ext, "change_pct_ext": change_pct_ext,
            "quote_time": fields[3] if len(fields) > 3 else None}


_quote_cache: dict = {"at": 0.0, "quotes": {}}
_quote_refresh_lock = asyncio.Lock()
_quote_refresh_task: Optional[asyncio.Task] = None


async def _fetch_em_quotes(secids: list[str]) -> dict:
    """Batch quotes for JP/KR/TW Eastmoney secids via ulist (push2delay;
    push2 302-redirects there). Grouped by market prefix so identical
    local codes in different markets (2330 JP vs TW) can't collide.
    Returns {secid: {name, prev_close, current, change_pct, quote_time}}
    in the same shape as Sina quotes (verified 2026-11-20: f2 price,
    f3 change%, f18 prev close match trends2 last close / preClose)."""
    out = {}
    by_prefix: dict = {}
    for s in secids:
        by_prefix.setdefault(s.split(".", 1)[0], []).append(s)
    qtime = datetime.now(CN_TZ).strftime("%Y-%m-%d %H:%M:%S")
    for prefix, group in by_prefix.items():
        try:
            resp = await upstream_get(
                "https://push2delay.eastmoney.com/api/qt/ulist.np/get?secids=" +
                ",".join(group) + "&fields=f2,f3,f12,f14,f18&fltt=2",
                referer="https://quote.eastmoney.com/", timeout=15)
            rows = ((resp.json().get("data") or {}).get("diff")) or []
            want = {s.split(".", 1)[1]: s for s in group}
            for r in rows:
                secid = want.get(str(r.get("f12") or ""))
                cur, prev, chg = _fnum(r.get("f2")), _fnum(r.get("f18")), _fnum(r.get("f3"))
                if secid and cur and prev:
                    out[secid] = {"name": r.get("f14"), "prev_close": prev, "current": cur,
                                  "change_pct": chg if chg is not None else (cur - prev) / prev * 100,
                                  "quote_time": qtime}
        except Exception:
            continue
    return out


async def _refresh_quotes(symbols: list[str]) -> dict:
    """Fetch quotes (Sina for A/HK/US, Eastmoney ulist for JP/KR/TW secids)
    and merge into the cache. Caller holds the lock."""
    symbols = sorted(set(symbols))
    if not symbols:
        return {}
    now = time.time()
    cached = _quote_cache["quotes"]
    sina_syms = [s for s in symbols if "." not in s]
    em_syms = [s for s in symbols if "." in s]
    quotes = {}
    if em_syms:
        quotes.update(await _fetch_em_quotes(em_syms))
    if not sina_syms:
        _quote_cache.update(at=now, quotes={**cached, **quotes})
        return {s: _quote_cache["quotes"][s] for s in symbols if s in _quote_cache["quotes"]}
    try:
        resp = await upstream_get("https://hq.sinajs.cn/list=" + ",".join(sina_syms), referer="https://finance.sina.com.cn/")
        text = resp.content.decode("gbk", errors="replace")
        for line in text.splitlines():
            m = re.match(r'var hq_str_(\w+)="(.*)";', line.strip())
            if not m:
                continue
            sym, fields = m.group(1), m.group(2).split(",")
            try:
                if sym.startswith("gb_"):
                    # US layout (verified 2026-11-19): 0=name,1=current,2=change%,3=Beijing datetime,26=prev close
                    # Extended hours (verified 2026-11-20): 21=ext price,24=ext timestamp; see us_quote_with_ext
                    quotes[sym] = us_quote_with_ext(fields)
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
        if quotes:  # Eastmoney part succeeded even if Sina failed
            _quote_cache.update(at=now, quotes={**cached, **quotes})
        return {s: _quote_cache["quotes"][s] for s in symbols if s in _quote_cache["quotes"]}


def _schedule_quote_refresh(symbols: list[str]) -> None:
    """Kick off a background quote refresh unless one is already running."""
    global _quote_refresh_task
    if _quote_refresh_task is not None and not _quote_refresh_task.done():
        return

    async def _run():
        async with _quote_refresh_lock:
            await _refresh_quotes(symbols)

    _quote_refresh_task = asyncio.create_task(_run())


async def fetch_quotes(symbols: list[str]) -> dict:
    """Return {symbol: {name, prev_close, current, change_pct, quote_time}}.

    Stale-while-revalidate: complete-but-expired cache is served immediately
    and refreshed in the background; only a cache missing symbols blocks on
    the upstream fetch (deduped via _quote_refresh_lock).
    """
    symbols = sorted(set(symbols))
    if not symbols:
        return {}
    cached = _quote_cache["quotes"]
    have_all = all(s in cached for s in symbols)
    if have_all:
        if time.time() - _quote_cache["at"] >= QUOTE_TTL:
            _schedule_quote_refresh(symbols)
        return {s: cached[s] for s in symbols}
    async with _quote_refresh_lock:
        # Another coroutine may have refreshed while we waited for the lock.
        cached = _quote_cache["quotes"]
        if all(s in cached for s in symbols) and time.time() - _quote_cache["at"] < QUOTE_TTL:
            return {s: cached[s] for s in symbols}
        return await _refresh_quotes(symbols)


def quotes_stale() -> bool:
    """True when the quote cache is past TTL (served stale or never filled)."""
    return time.time() - _quote_cache["at"] >= QUOTE_TTL


# ---------------------------------------------------------------- stock minute trends
# Eastmoney trends2 minute data per stock; the intraday fund curve is
# computed from holdings' minute closes (replaces snapshot recording,
# v1.15.0). Verified 2026-11-20: data.preClose present; rows are
# "YYYY-MM-DD HH:MM,open,close,high,low,vol,amount,avg" in Beijing time.
TRENDS_TTL = 300            # seconds, DB cache for today's session
TRENDS_REF = "https://quote.eastmoney.com/"
_trends_sem = asyncio.Semaphore(5)
_us_prefix_cache: dict = {}     # ticker -> working Eastmoney prefix (105/106/107)
_secid_fail_at: dict = {}       # holding code -> last all-prefixes-failed ts
_market_map_cache: dict = {}    # fund code -> (market map, ts)


def _secid_candidates(code: str, market_map: Optional[dict]) -> list:
    """Ordered Eastmoney secid candidates for a holding code (verified
    2026-11-20): SH 6xxxxx/900xxx B-shares -> 1.; SZ 0/3 -> 0.; BJ 4/8 and
    920xxx -> 0.; HK 5-digit -> 116.; US alpha -> pingzhongdata prefix
    (105 NASDAQ / 106 NYSE / 107 AMEX) or try 105, 106, 107 in order."""
    c = (code or "").strip().upper()
    if re.fullmatch(r"\d{6}", c):
        if c[0] == "6":
            return [f"1.{c}"]
        if c[0] == "9":
            return [f"0.{c}", f"1.{c}"] if c.startswith("92") else [f"1.{c}"]
        return [f"0.{c}"]  # 0/3 Shenzhen, 4/8 Beijing
    if re.fullmatch(r"\d{5}", c):
        return [f"116.{c}"]
    if re.fullmatch(r"[A-Z][A-Z0-9.\-]*", c):
        known = _us_prefix_cache.get(c)
        if known:
            return [f"{known}.{c}"]
        pref = (market_map or {}).get(c)
        if pref in ("105", "106", "107"):
            return [f"{pref}.{c}"]
        return [f"{p}.{c}" for p in ("105", "106", "107")]
    return []


# ---------------------------------------------------------------- JP/KR/TW via Eastmoney
# Sina has no JP/KR/TW quotes, so those holdings use Eastmoney directly
# (ulist quotes + trends2 minute data, v1.15.3). Secid prefixes verified
# 2026-11-20 via the suggest API: JP=176, KR=177, TW=178. Holding codes
# come in three shapes: ISIN (JP3236330001), bare local codes (TW 2330,
# KR 005930 — the latter indistinguishable from A-share codes by shape),
# and pingzhongdata stockCodesNew gives NO numeric prefix for any of them.
# Resolution is therefore name-based via the suggest API, DB-cached.
EM_TYPE_BY_COUNTRY = {"JP": "日股", "KR": "韩股", "TW": "台股"}
EM_PREFIX_BY_TYPE = {"日股": "176", "韩股": "177", "台股": "178"}
_ISIN_RE = re.compile(r"^[A-Z]{2}[A-Z0-9]{9}\d$")


def _bare_foreign(code: str, market_map: Optional[dict]) -> bool:
    """True when pingzhongdata lists this 6-digit code WITHOUT an A-share
    0./1. prefix — i.e. it is a KR local code (005930), not an A-share."""
    c = (code or "").strip().upper()
    return bool(market_map) and re.fullmatch(r"\d{6}", c) and c in market_map \
        and market_map[c] is None


def _em_want_types(code: str, market_map: Optional[dict]) -> Optional[list]:
    """Eastmoney SecurityTypeNames to accept for this holding, or None when
    the holding is not a JP/KR/TW candidate."""
    c = (code or "").strip().upper()
    if _ISIN_RE.match(c):
        t = EM_TYPE_BY_COUNTRY.get(c[:2])
        return [t] if t else None
    if re.fullmatch(r"\d{4}[A-Z]?", c):
        return ["日股", "台股"]  # 4-char local code: JP (7203/285A) or TW (2330)
    if _bare_foreign(c, market_map):
        return ["韩股"]
    return None


def _name_match(holding_name: str, cand_name: str) -> bool:
    a, b = (holding_name or "").strip(), (cand_name or "").strip()
    return bool(a and b) and (a == b or a.startswith(b) or b.startswith(a))


async def _suggest_stock_secid(code: str, name: str, want_types: list) -> Optional[str]:
    """Resolve a JP/KR/TW holding to an Eastmoney secid via the suggest
    API. ISINs are not searchable (verified empty), so search by stock
    name with progressive shortening; bare local codes are also tried as
    the query. A row is accepted only when its SecurityTypeName is in
    want_types and its name prefix-matches the holding name."""
    from urllib.parse import quote
    queries = []
    c = (code or "").strip().upper()
    if re.fullmatch(r"\d{4}[A-Z]?|\d{6}", c):
        queries.append(c)
    n = (name or "").strip()
    if n:
        queries.append(n)
        s = n
        while len(s) > 2:
            s = s[:-1]
            if s not in queries:
                queries.append(s)
    for q in queries:
        try:
            resp = await upstream_get(
                "https://searchapi.eastmoney.com/api/suggest/get?input=" + quote(q) +
                f"&type=14&count=10&token={SUGGEST_TOKEN}",
                referer="https://quote.eastmoney.com/", timeout=10)
            rows = (resp.json().get("QuotationCodeTable") or {}).get("Data") or []
            for r in rows:
                qid = str(r.get("QuoteID") or "")
                if str(r.get("SecurityTypeName") or "") in want_types and \
                        re.fullmatch(r"(176|177|178)\..+", qid) and \
                        _name_match(n, str(r.get("Name") or "")):
                    return qid
        except Exception:
            continue
    return None


async def resolve_em_secid(code: str, name: Optional[str],
                           market_map: Optional[dict]) -> Optional[str]:
    """Eastmoney secid for a JP/KR/TW holding, DB-cached in em_secid_map
    (keyed by holding code). Failures are cached and retried after a day."""
    c = (code or "").strip().upper()
    want = _em_want_types(c, market_map)
    if not want:
        return None
    with db() as conn:
        row = conn.execute(
            "SELECT secid, fetched_at FROM em_secid_map WHERE code=?", (c,)).fetchone()
    if row and row["secid"]:
        return row["secid"]
    if row and time.time() - row["fetched_at"] < 86400:
        return None
    secid = await _suggest_stock_secid(c, name or "", want)
    with db() as conn:
        conn.execute(
            "INSERT INTO em_secid_map(code, name, secid, fetched_at) VALUES(?,?,?,?) "
            "ON CONFLICT(code) DO UPDATE SET name=excluded.name, "
            "secid=excluded.secid, fetched_at=excluded.fetched_at",
            (c, name, secid, time.time()))
    return secid


async def holding_quote_symbol(h: dict, market_map: Optional[dict]) -> Optional[str]:
    """Quote-cache symbol for one holding: Sina symbol for A/HK/US, or the
    Eastmoney secid (contains '.') for JP/KR/TW, or None."""
    if _em_want_types(h["code"], market_map):
        return await resolve_em_secid(h["code"], h.get("name"), market_map)
    return to_sina_symbol(h["code"])


async def fetch_stock_trends(secid: str, date_str: str) -> Optional[dict]:
    """Minute closes for one stock: {"pre_close": float|None,
    "points": [[ts, close], ...]}. DB-cached per (secid, date); today's
    rows refresh after TRENDS_TTL, past dates never refetch. Returns None
    when the secid yields no data (wrong prefix / upstream failure) and
    no cache exists."""
    today_str = datetime.now(CN_TZ).strftime("%Y-%m-%d")
    with db() as conn:
        row = conn.execute(
            "SELECT payload, fetched_at FROM stock_trends_cache WHERE secid=? AND date=?",
            (secid, date_str)).fetchone()
    if row and (date_str < today_str or time.time() - row["fetched_at"] < TRENDS_TTL):
        return json.loads(row["payload"])
    try:
        resp = await upstream_get(
            "https://push2his.eastmoney.com/api/qt/stock/trends2/get"
            f"?secid={secid}&fields1=f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f11,f12,f13"
            "&fields2=f51,f52,f53,f54,f55,f56,f57,f58&iscr=0&iscca=0",
            referer=TRENDS_REF, timeout=15)
        data = resp.json().get("data")
        if not data:
            return json.loads(row["payload"]) if row else None
        points = []
        for line in data.get("trends") or []:
            parts = line.split(",")
            try:
                ts = int(datetime.strptime(parts[0], "%Y-%m-%d %H:%M")
                         .replace(tzinfo=CN_TZ).timestamp())
                points.append([ts, float(parts[2])])
            except (ValueError, IndexError):
                continue
        pre_close = _fnum(data.get("preClose"))
        if not pre_close and secid.split(".", 1)[0] in ("0", "1", "116", "105", "106", "107"):
            # Fall back to the Sina quote cache's prev close (A/HK/US only).
            sym = to_sina_symbol(secid.split(".", 1)[1])
            q = _quote_cache["quotes"].get(sym) if sym else None
            if q:
                pre_close = q.get("prev_close") or (
                    q["current"] / (1 + q["change_pct"] / 100) if q.get("current") else None)
        payload = {"pre_close": pre_close, "points": points}
        with db() as conn:
            conn.execute(
                "INSERT INTO stock_trends_cache(secid, date, payload, fetched_at) VALUES(?,?,?,?) "
                "ON CONFLICT(secid, date) DO UPDATE SET payload=excluded.payload, fetched_at=excluded.fetched_at",
                (secid, date_str, json.dumps(payload), time.time()))
        return payload
    except Exception:
        return json.loads(row["payload"]) if row else None


async def fetch_holding_trends(code: str, market_map: Optional[dict], date_str: str,
                               name: Optional[str] = None) -> Optional[dict]:
    """Trends for one holding, trying secid candidates in order; remembers
    the working prefix per ticker and backs off TRENDS_TTL after a total
    failure so a bad code doesn't hammer upstream on every page refresh.
    JP/KR/TW holdings (ISIN / bare local codes) resolve via the suggest
    API instead (see resolve_em_secid)."""
    c = (code or "").strip().upper()
    if time.time() - _secid_fail_at.get(c, 0) < TRENDS_TTL:
        return None
    async with _trends_sem:
        if _em_want_types(c, market_map):
            secid = await resolve_em_secid(c, name, market_map)
            if secid:
                tr = await fetch_stock_trends(secid, date_str)
                if tr and tr.get("points"):
                    return tr
        else:
            for secid in _secid_candidates(c, market_map):
                tr = await fetch_stock_trends(secid, date_str)
                if tr and tr.get("points"):
                    _us_prefix_cache[c] = secid.split(".", 1)[0]
                    return tr
    _secid_fail_at[c] = time.time()
    return None


def _fund_curve(holdings: list, trends_by_code: dict, start_ts: int) -> list:
    """Fund intraday estimate curve from holdings' minute trends:
    pct(t) = sum(weight% * stock change%(t)) / 100 on the union minute
    grid (Asia/Shanghai day from start_ts), forward-filled per stock
    (excluded before its first point), no renormalization — same
    convention as /api/estimates. Downsampled to 5-minute buckets."""
    per_stock = []  # (weight, [(ts, chg_pct), ...])
    for h in holdings:
        if not h["weight"]:
            continue  # zero-weight rows (e.g. feeder-fund artifacts) carry no signal
        tr = trends_by_code.get(h["code"])
        if not tr or not tr.get("pre_close"):
            continue
        pre = tr["pre_close"]
        pts = [(ts, (close - pre) / pre * 100) for ts, close in tr["points"] if ts >= start_ts]
        if pts:
            per_stock.append((h["weight"], pts))
    if not per_stock:
        return []
    grid = sorted({ts for _, pts in per_stock for ts, _ in pts})
    idx = [0] * len(per_stock)
    last = [None] * len(per_stock)
    buckets = {}
    for ts in grid:
        total, any_data = 0.0, False
        for i, (w, pts) in enumerate(per_stock):
            while idx[i] < len(pts) and pts[idx[i]][0] <= ts:
                last[i] = pts[idx[i]][1]
                idx[i] += 1
            if last[i] is not None:
                total += w * last[i] / 100
                any_data = True
        if any_data:
            # Bucket timestamp = bucket start (5-min grid); the value is the
            # last computed point inside the bucket. Beijing session opens
            # (09:30 etc.) fall on the grid, so the first bucket is the open.
            buckets[(ts // 300) * 300] = round(total, 2)
    return [[ts, pct] for ts, pct in sorted(buckets.items())]


# ETF feeder funds （联接） disclose no stock holdings; their curve (and,
# as a bonus, estimate) comes from the tracking index instead (v1.15.0).
SUGGEST_TOKEN = "D43BF722C8E33BDC906FB84D85E326E8"  # Eastmoney public web suggest token


def _is_feeder(fund_name: Optional[str], holdings: list) -> bool:
    return "ETF联接" in (fund_name or "") or not holdings


async def _fetch_benchmark_index_name(fund_code: str) -> Optional[str]:
    """Tracking index name from the fund's F10 基金概况 page （跟踪标的 /
    业绩比较基准）, e.g. 中证畜牧养殖指数 (verified 2026-11-20)."""
    try:
        resp = await upstream_get(
            f"https://fundf10.eastmoney.com/jbgk_{fund_code}.html",
            referer="https://fundf10.eastmoney.com/", timeout=15)
        text = re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", resp.text))
        m = re.search(r"跟踪标的\s*([^\s]+)", text)
        if m and "指数" in m.group(1):
            return m.group(1).strip()
        m = re.search(r"业绩比较基准\s*(.+?指数)", text)
        if m:
            return m.group(1).strip()
    except Exception:
        pass
    return None


async def _suggest_index_secid(index_name: str) -> Optional[str]:
    """Resolve an index name to an Eastmoney secid via the suggest API.
    Tries the full name, then progressively shorter prefixes (the suggest
    index lists short names, e.g. 中证畜牧 for 中证畜牧养殖指数）."""
    from urllib.parse import quote
    core = index_name[:-2] if index_name.endswith("指数") else index_name
    queries = []
    for q in [index_name, core]:
        if q and q not in queries:
            queries.append(q)
    c = core
    while len(c) > 4:
        c = c[:-1]
        if c not in queries:
            queries.append(c)
    for q in queries:
        try:
            resp = await upstream_get(
                "https://searchapi.eastmoney.com/api/suggest/get?input=" + quote(q) +
                f"&type=14&token={SUGGEST_TOKEN}",
                referer="https://quote.eastmoney.com/", timeout=10)
            rows = (resp.json().get("QuotationCodeTable") or {}).get("Data") or []
            for r in rows:
                qid = str(r.get("QuoteID") or "")
                name = str(r.get("Name") or "")
                if re.fullmatch(r"[012]\.\d{6}", qid) and len(name) >= 4 and \
                        (index_name.startswith(name) or name in index_name or core.startswith(name)):
                    return qid
        except Exception:
            continue
    return None


async def resolve_fund_index(fund_code: str) -> Optional[dict]:
    """{"index_name", "secid"} for a feeder fund, DB-cached in
    fund_index_map. Failures are cached too and retried after a day."""
    with db() as conn:
        row = conn.execute(
            "SELECT index_name, secid, fetched_at FROM fund_index_map WHERE fund_code=?",
            (fund_code,)).fetchone()
    if row and row["secid"]:
        return {"index_name": row["index_name"], "secid": row["secid"]}
    if row and time.time() - row["fetched_at"] < 86400:
        return None
    index_name = await _fetch_benchmark_index_name(fund_code)
    secid = await _suggest_index_secid(index_name) if index_name else None
    with db() as conn:
        conn.execute(
            "INSERT INTO fund_index_map(fund_code, index_name, secid, fetched_at) VALUES(?,?,?,?) "
            "ON CONFLICT(fund_code) DO UPDATE SET index_name=excluded.index_name, "
            "secid=excluded.secid, fetched_at=excluded.fetched_at",
            (fund_code, index_name, secid, time.time()))
    return {"index_name": index_name, "secid": secid} if secid else None


def _live_est_pct(holdings: list, syms: list, quotes: dict):
    """Main estimate from live quotes: sum(weight% * change%) / 100 over
    quoted holdings (no renormalization). Single source for both
    /api/estimates and the final point of /api/history/today, so the
    curve end and the card number are the same computation."""
    est_pct, coverage = 0.0, 0.0
    for h, sym in zip(holdings, syms):
        coverage += h["weight"]
        q = quotes.get(sym) if sym else None
        if q:
            est_pct += h["weight"] * q["change_pct"] / 100
    return est_pct, coverage


def _index_latest_chg(tr: Optional[dict]) -> Optional[float]:
    """Tracking index latest change% vs preClose (feeder-fund estimate)."""
    if tr and tr.get("pre_close") and tr["points"]:
        return (tr["points"][-1][1] - tr["pre_close"]) / tr["pre_close"] * 100
    return None


def _index_curve(tr: Optional[dict], start_ts: int) -> list:
    """Index minute change% vs preClose as a fund curve (weight 100%),
    downsampled to 5-minute buckets."""
    if not tr or not tr.get("pre_close"):
        return []
    pre = tr["pre_close"]
    buckets = {}
    for ts, close in tr["points"]:
        if ts >= start_ts:
            buckets[(ts // 300) * 300] = round((close - pre) / pre * 100, 2)
    return [[ts, pct] for ts, pct in sorted(buckets.items())]


async def _quote_warm_loop():
    """Keep the quote cache hot: every 40s refresh quotes for the union of
    holding symbols across all users' watchlists, so page loads never wait
    on Sina. Startup is never blocked; errors are logged and skipped."""
    await asyncio.sleep(5)
    while True:
        try:
            with db() as conn:
                codes = [r[0] for r in conn.execute(
                    "SELECT DISTINCT fund_code FROM user_funds").fetchall()]
            if codes:
                syms = set()
                holdings_by_code = {}
                for code in codes:
                    hold = await fetch_holdings(code)
                    holdings_by_code[code] = hold["holdings"]
                    mmap = await fetch_stock_market_map(code)
                    for h in hold["holdings"]:
                        s = await holding_quote_symbol(h, mmap)
                        if s:
                            syms.add(s)
                if syms:
                    async with _quote_refresh_lock:
                        await _refresh_quotes(sorted(syms))
                    print(f"[quote-warm] refreshed {len(syms)} symbols "
                          f"for {len(codes)} funds", flush=True)
        except Exception as e:
            print(f"[quote-warm] failed: {e!r}", flush=True)
        await asyncio.sleep(40)


async def _trends_warm_loop():
    """Keep stock_trends_cache hot: every 240s (< TRENDS_TTL 300s) refresh
    minute trends for the union of holding codes across all users'
    watchlists (plus feeder funds' tracking-index secids), so the first
    /api/history/today call never waits on Eastmoney. Runs beside the
    quote warmer; startup is never blocked; errors are logged and skipped."""
    await asyncio.sleep(10)
    while True:
        try:
            with db() as conn:
                rows = conn.execute(
                    "SELECT DISTINCT fund_code, fund_name FROM user_funds").fetchall()
            if rows:
                date_str = datetime.now(CN_TZ).strftime("%Y-%m-%d")
                uniq = {}          # holding code -> (market map, name)
                feeder_codes = []
                for r in rows:
                    hold = await fetch_holdings(r["fund_code"])
                    mmap = await fetch_stock_market_map(r["fund_code"])
                    for h in hold["holdings"]:
                        if h["weight"]:
                            uniq.setdefault(h["code"], (mmap, h.get("name")))
                    if _is_feeder(r["fund_name"], hold["holdings"]):
                        feeder_codes.append(r["fund_code"])
                await asyncio.gather(*[
                    fetch_holding_trends(c, m, date_str, n) for c, (m, n) in uniq.items()])
                n = len(uniq)
                for fc in feeder_codes:
                    idx = await resolve_fund_index(fc)
                    if idx:
                        async with _trends_sem:
                            await fetch_stock_trends(idx["secid"], date_str)
                        n += 1
                print(f"[trends-warm] refreshed {n} secids "
                      f"for {len(rows)} funds", flush=True)
        except Exception as e:
            print(f"[trends-warm] failed: {e!r}", flush=True)
        await asyncio.sleep(240)


# ---------------------------------------------------------------- watchlist
def category_order(conn, uid) -> list:
    return [r["name"] for r in conn.execute(
        "SELECT name FROM user_categories WHERE user_id=? ORDER BY sort_order, name", (uid,)
    ).fetchall()]


def sort_funds(rows, cat_order) -> list:
    """Order: named categories by category sort_order, uncategorized last,
    then fund sort_order, then id."""
    idx = {name: i for i, name in enumerate(cat_order)}

    def key(r):
        cat = r["category"] or ""
        if not cat:
            return (1, 0, r["sort_order"], r["id"])
        return (0, idx.get(cat, len(idx)), r["sort_order"], r["id"])

    return sorted(rows, key=key)


@app.get("/api/watchlist")
def get_watchlist(session: Optional[str] = Cookie(default=None)):
    uid = current_user_id(session)
    with db() as conn:
        rows = conn.execute(
            "SELECT id, fund_code, fund_name, sort_order, category FROM user_funds WHERE user_id=? ORDER BY sort_order, id", (uid,)
        ).fetchall()
        cats = category_order(conn, uid)
    return {"funds": [dict(r) for r in sort_funds(rows, cats)]}


@app.post("/api/watchlist")
async def add_watchlist(body: AddFundBody, session: Optional[str] = Cookie(default=None)):
    uid = current_user_id(session)
    code = body.fund_code.strip()
    if not FUND_CODE_RE.fullmatch(code):
        raise HTTPException(400, "基金代码需为 6 位数字")
    # Same semantics as set_category: strip, <=20 chars, "" = uncategorized,
    # named categories are auto-created if missing.
    category = (body.category or "").strip()
    if len(category) > 20:
        raise HTTPException(400, "分类名最多 20 个字符")
    # Resolve fund name via search API (also validates the code exists)
    resp = await upstream_get(f"https://fundsuggest.eastmoney.com/FundSearch/api/FundSearchAPI.ashx?m=1&key={code}")
    name = next((d["NAME"] for d in resp.json().get("Datas", []) if d.get("CODE") == code and _is_fund_entry(d)), None)
    if not name:
        raise HTTPException(404, "未找到该基金代码")
    with db() as conn:
        try:
            cur = conn.execute(
                "INSERT INTO user_funds(user_id, fund_code, fund_name, sort_order, category, created_at) "
                "VALUES(?,?,?,(SELECT COALESCE(MAX(sort_order),-1)+1 FROM user_funds WHERE user_id=? AND category=?),?,?)",
                (uid, code, name, uid, category, category, now_iso()),
            )
            if category:
                conn.execute(
                    "INSERT OR IGNORE INTO user_categories(user_id, name, sort_order) VALUES(?,?,"
                    "(SELECT COALESCE(MAX(sort_order),-1)+1 FROM user_categories WHERE user_id=?))",
                    (uid, category, uid),
                )
        except sqlite3.IntegrityError:
            raise HTTPException(409, "该基金已在列表中")
    _series_cache.pop(uid, None)  # watchlist changed: recompute curves next call
    return {"ok": True, "id": cur.lastrowid, "fund_code": code, "fund_name": name, "category": category}


@app.delete("/api/watchlist/{item_id}")
def delete_watchlist(item_id: int, session: Optional[str] = Cookie(default=None)):
    uid = current_user_id(session)
    with db() as conn:
        cur = conn.execute("DELETE FROM user_funds WHERE id=? AND user_id=?", (item_id, uid))
    if cur.rowcount == 0:
        raise HTTPException(404, "记录不存在")
    _series_cache.pop(uid, None)  # watchlist changed: recompute curves next call
    return {"ok": True}


@app.post("/api/watchlist/reorder")
def reorder_watchlist(body: ReorderBody, session: Optional[str] = Cookie(default=None)):
    """Reorder funds within ONE category only; never affects category order."""
    uid = current_user_id(session)
    category = (body.category or "").strip()
    try:
        ids = [int(v) for v in body.ids]
    except (TypeError, ValueError):
        raise HTTPException(400, "基金列表不匹配，请刷新后重试")
    with db() as conn:
        existing = [r["id"] for r in conn.execute(
            "SELECT id FROM user_funds WHERE user_id=? AND category=?", (uid, category)
        ).fetchall()]
        if len(ids) != len(existing) or set(ids) != set(existing):
            raise HTTPException(400, "基金列表不匹配，请刷新后重试")
        updated = 0
        for pos, item_id in enumerate(ids):
            conn.execute(
                "UPDATE user_funds SET sort_order=? WHERE id=? AND user_id=? AND category=?",
                (pos, item_id, uid, category),
            )
            updated += 1
    return {"ok": True, "updated": updated}


@app.post("/api/categories/reorder")
def reorder_categories(body: CategoryReorderBody, session: Optional[str] = Cookie(default=None)):
    """Reorder whole categories; funds move with their category."""
    uid = current_user_id(session)
    names = []
    for v in body.names:
        if not isinstance(v, str) or not v.strip():
            raise HTTPException(400, "分类名不能为空")
        names.append(v.strip())
    if len(names) != len(set(names)):
        raise HTTPException(400, "分类名不能重复")
    with db() as conn:
        updated = 0
        for pos, name in enumerate(names):
            cur = conn.execute(
                "UPDATE user_categories SET sort_order=? WHERE user_id=? AND name=?", (pos, uid, name)
            )
            updated += cur.rowcount
    return {"ok": True, "updated": updated}


@app.post("/api/categories")
def create_category(body: CategoryCreateBody, session: Optional[str] = Cookie(default=None)):
    uid = current_user_id(session)
    name = (body.name or "").strip()
    if not name or len(name) > 20:
        raise HTTPException(400, "分类名需 1-20 个字符")
    with db() as conn:
        if conn.execute("SELECT 1 FROM user_categories WHERE user_id=? AND name=?", (uid, name)).fetchone():
            raise HTTPException(400, "分类已存在")
        conn.execute(
            "INSERT INTO user_categories(user_id, name, sort_order) VALUES(?,?,"
            "(SELECT COALESCE(MAX(sort_order),-1)+1 FROM user_categories WHERE user_id=?))",
            (uid, name, uid),
        )
    return {"ok": True, "name": name}


@app.post("/api/categories/rename")
def rename_category(body: CategoryRenameBody, session: Optional[str] = Cookie(default=None)):
    uid = current_user_id(session)
    old = (body.old or "").strip()
    new = (body.new or "").strip()
    if not new or len(new) > 20:
        raise HTTPException(400, "分类名需 1-20 个字符")
    with db() as conn:
        if not conn.execute("SELECT 1 FROM user_categories WHERE user_id=? AND name=?", (uid, old)).fetchone():
            raise HTTPException(404, "分类不存在")
        if new != old and conn.execute("SELECT 1 FROM user_categories WHERE user_id=? AND name=?", (uid, new)).fetchone():
            raise HTTPException(400, "分类已存在")
        conn.execute("UPDATE user_categories SET name=? WHERE user_id=? AND name=?", (new, uid, old))
        conn.execute("UPDATE user_funds SET category=? WHERE user_id=? AND category=?", (new, uid, old))
    return {"ok": True, "name": new}


@app.post("/api/categories/delete")
def delete_category(body: CategoryDeleteBody, session: Optional[str] = Cookie(default=None)):
    uid = current_user_id(session)
    name = (body.name or "").strip()
    with db() as conn:
        if not conn.execute("SELECT 1 FROM user_categories WHERE user_id=? AND name=?", (uid, name)).fetchone():
            raise HTTPException(404, "分类不存在")
        conn.execute("UPDATE user_funds SET category='' WHERE user_id=? AND category=?", (uid, name))
        conn.execute("DELETE FROM user_categories WHERE user_id=? AND name=?", (uid, name))
    return {"ok": True}


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
        if category:
            conn.execute(
                "INSERT OR IGNORE INTO user_categories(user_id, name, sort_order) VALUES(?,?,"
                "(SELECT COALESCE(MAX(sort_order),-1)+1 FROM user_categories WHERE user_id=?))",
                (uid, category, uid),
            )
        # Move fund to the end of its (new) category
        conn.execute(
            "UPDATE user_funds SET sort_order=(SELECT COALESCE(MAX(sort_order),-1)+1 FROM user_funds "
            "WHERE user_id=? AND category=? AND id<>?) WHERE id=? AND user_id=?",
            (uid, category, item_id, item_id, uid),
        )
    return {"ok": True, "category": category}


@app.get("/api/estimates")
async def estimates(session: Optional[str] = Cookie(default=None)):
    uid = current_user_id(session)
    with db() as conn:
        rows = conn.execute(
            "SELECT id, fund_code, fund_name, category, sort_order FROM user_funds WHERE user_id=? ORDER BY sort_order, id", (uid,)
        ).fetchall()
        cats = category_order(conn, uid)
        urow = conn.execute("SELECT refresh_seconds FROM users WHERE id=?", (uid,)).fetchone()
        refresh_seconds = urow["refresh_seconds"] if urow else 60
    rows = sort_funds(rows, cats)
    # Fetch NAV + holdings per fund in parallel, collect all symbols,
    # then one batch quote call.
    async def _fund_inputs(r):
        nav = await fetch_nav(r["fund_code"])
        hold = await fetch_holdings(r["fund_code"])
        mmap = await fetch_stock_market_map(r["fund_code"])
        syms = [await holding_quote_symbol(h, mmap) for h in hold["holdings"]]
        return (r, nav, hold, syms)

    per_fund = await asyncio.gather(*[_fund_inputs(r) for r in rows])
    all_symbols = [s for (_, _, _, syms) in per_fund for s in syms if s]
    quotes = await fetch_quotes(all_symbols)
    q_stale = quotes_stale()
    out = []
    for r, nav, hold, syms in per_fund:
        est_pct, est_pct_ext, coverage, quote_time, skipped = 0.0, 0.0, 0.0, None, []
        has_us, has_ext = False, False
        detail = []
        for h, sym in zip(hold["holdings"], syms):
            coverage += h["weight"]
            q = quotes.get(sym) if sym else None
            if q:
                # Extended-hours estimate: US holdings use ext change when
                # valid (else their regular change); others same as main.
                chg_ext = q.get("change_pct_ext")
                if sym and sym.startswith("gb_"):
                    has_us = True
                    if chg_ext is not None:
                        has_ext = True
                    est_pct_ext += h["weight"] * (chg_ext if chg_ext is not None else q["change_pct"]) / 100
                else:
                    est_pct_ext += h["weight"] * q["change_pct"] / 100
                qt = q["quote_time"]
                if qt and (quote_time is None or qt.replace("/", "-") > quote_time.replace("/", "-")):
                    quote_time = qt
                detail.append({**h, "change_pct": round(q["change_pct"], 2),
                               "change_pct_ext": round(chg_ext, 2) if chg_ext is not None else None})
            else:
                skipped.append(h["code"])
                detail.append({**h, "change_pct": None, "change_pct_ext": None})
        # Main estimate: same helper the history endpoint uses for its
        # final curve point (same quotes, same formula).
        est_pct, _ = _live_est_pct(hold["holdings"], syms, quotes)
        # ETF feeder funds have no usable stock holdings (est would be a
        # meaningless 0.00%): use the tracking index's latest change.
        if coverage == 0 and _is_feeder(r["fund_name"], hold["holdings"]):
            idx = await resolve_fund_index(r["fund_code"])
            if idx:
                async with _trends_sem:
                    tr = await fetch_stock_trends(
                        idx["secid"], datetime.now(CN_TZ).strftime("%Y-%m-%d"))
                chg = _index_latest_chg(tr)
                if chg is not None:
                    ts_last = tr["points"][-1][0]
                    est_pct = chg
                    coverage = 100.0
                    detail = [{"code": idx["secid"].split(".", 1)[1],
                               "name": idx["index_name"], "weight": 100.0,
                               "change_pct": round(chg, 2), "change_pct_ext": None}]
                    skipped = []
                    quote_time = datetime.fromtimestamp(ts_last, CN_TZ).strftime("%Y-%m-%d %H:%M")
        est_nav = nav["dwjz"] * (1 + est_pct / 100) if nav["dwjz"] else None
        # Ext estimate only when the fund holds US stocks with valid ext data
        # (change_pct_ext is None during US regular hours -> null then too).
        show_ext = has_us and has_ext
        est_nav_ext = nav["dwjz"] * (1 + est_pct_ext / 100) if (show_ext and nav["dwjz"]) else None
        out.append({
            "id": r["id"], "fund_code": r["fund_code"], "fund_name": r["fund_name"], "category": r["category"] or "",
            "nav_date": nav["nav_date"], "dwjz": nav["dwjz"], "jzzzl": nav["jzzzl"],
            "prev_nav_date": nav.get("prev_nav_date"), "prev_dwjz": nav.get("prev_dwjz"), "prev_jzzzl": nav.get("prev_jzzzl"),
            "est_nav": round(est_nav, 4) if est_nav else None,
            "est_change_pct": round(est_pct, 2),
            "est_nav_ext": round(est_nav_ext, 4) if est_nav_ext else None,
            "est_change_pct_ext": round(est_pct_ext, 2) if show_ext else None,
            "coverage_pct": round(coverage, 2),
            "report_label": hold["report_label"], "report_date": hold["report_date"],
            "quote_time": quote_time, "stale": nav["stale"] or hold["stale"] or q_stale,
            "skipped_holdings": skipped, "holdings": detail,
        })
    return {"funds": out, "categories": cats, "refresh_seconds": refresh_seconds, "disclaimer": "自算估值，仅供参考，以基金公司公布净值为准"}


# Per-user cache of the computed curve series: a full recompute touches
# holdings + trends + quotes for every fund and takes seconds, so page
# refreshes serve the cached series instantly. Fresh (<20s) -> returned
# as-is; stale -> returned immediately while a deduped background task
# recomputes; no entry -> computed synchronously.
_series_cache = {}   # uid -> (computed_at_epoch, series dict)
_series_tasks = {}   # uid -> asyncio.Task running _refresh_series
_SERIES_TTL = 20


async def _compute_series(uid):
    """Intraday estimate curve per watchlisted fund for the current
    session day, computed from holdings' Eastmoney minute trends and
    downsampled to 5-minute buckets for the fund-card sparklines. Falls
    back to the legacy est_history snapshots when trends yield nothing."""
    now_cn = datetime.now(CN_TZ)
    date_str = now_cn.strftime("%Y-%m-%d")  # trends cache key = fetch date
    # Curve window starts at the session day's 00:00 (Asia/Shanghai): the
    # US session that opened the previous Beijing evening runs past
    # midnight, so before 09:30 (and on weekends) the session day is the
    # previous weekday — its A/HK segment and tonight's US points then
    # stay in one curve instead of being cut at midnight.
    day = now_cn.replace(hour=0, minute=0, second=0, microsecond=0)
    if not (day.weekday() < 5 and now_cn.hour * 60 + now_cn.minute >= 570):
        day -= timedelta(days=1)
        while day.weekday() >= 5:
            day -= timedelta(days=1)
    start_ts = int(day.timestamp())
    with db() as conn:
        fund_rows = conn.execute(
            "SELECT fund_code, fund_name FROM user_funds WHERE user_id=?", (uid,)).fetchall()

    # Holdings per fund in parallel; market map disambiguates US secid
    # prefixes and bare KR local codes from A-share codes.
    async def _inputs(fr):
        code = fr["fund_code"]
        hold = await fetch_holdings(code)
        mmap = await fetch_stock_market_map(code)
        syms = [await holding_quote_symbol(h, mmap) for h in hold["holdings"]]
        return code, fr["fund_name"], hold["holdings"], mmap, syms

    # Fault-tolerant: one fund's upstream failure must not blank every
    # curve (a 500 here leaves all sparklines empty).
    raw_inputs = await asyncio.gather(
        *[_inputs(fr) for fr in fund_rows], return_exceptions=True)
    per_fund = []
    for fr, res in zip(fund_rows, raw_inputs):
        if isinstance(res, Exception):
            print(f"[history] inputs failed for {fr['fund_code']}: {res!r}")
            per_fund.append((fr["fund_code"], fr["fund_name"], [], None, []))
        else:
            per_fund.append(res)
    # One trends fetch per unique holding code across all funds.
    uniq = {}
    for _, _, holdings, mmap, _ in per_fund:
        for h in holdings:
            uniq.setdefault(h["code"], (mmap, h.get("name")))
    fetched = await asyncio.gather(*[
        fetch_holding_trends(c, mmap, date_str, n) for c, (mmap, n) in uniq.items()],
        return_exceptions=True)
    trends_by_code = {c: (None if isinstance(v, Exception) else v)
                      for c, v in zip(uniq.keys(), fetched)}
    # Live quotes from the same 45s cache /api/estimates reads, for the
    # final curve point (curve end == card estimate by construction).
    all_symbols = [s for (_, _, _, _, syms) in per_fund for s in syms if s]
    quotes = await fetch_quotes(all_symbols)
    now_ts = int(time.time())
    series = {}
    for code, fund_name, holdings, _, syms in per_fund:
      try:
        pts = _fund_curve(holdings, trends_by_code, start_ts)
        feeder_tr = None
        if not pts and _is_feeder(fund_name, holdings):
            # ETF feeder fund: curve from its tracking index (weight 100%).
            idx = await resolve_fund_index(code)
            if idx:
                async with _trends_sem:
                    feeder_tr = await fetch_stock_trends(idx["secid"], date_str)
                pts = _index_curve(feeder_tr, start_ts)
        if not pts:
            with db() as conn:
                rows = conn.execute(
                    "SELECT ts, est_change_pct FROM est_history WHERE fund_code=? AND ts>=? ORDER BY ts",
                    (code, start_ts)).fetchall()
            buckets = {}
            for ts, pct in rows:
                buckets[(ts // 300) * 300] = pct
            pts = [[ts, pct] for ts, pct in sorted(buckets.items())]
        if pts:
            # Final point = live estimate from the same quotes/formula as
            # /api/estimates (feeder funds: tracking index latest change).
            est_pct, coverage = _live_est_pct(holdings, syms, quotes)
            if coverage == 0 and _is_feeder(fund_name, holdings):
                if feeder_tr is None:
                    idx = await resolve_fund_index(code)
                    if idx:
                        async with _trends_sem:
                            feeder_tr = await fetch_stock_trends(idx["secid"], date_str)
                chg = _index_latest_chg(feeder_tr)
                if chg is not None:
                    est_pct = chg
            val = round(est_pct, 2)
            if now_ts > pts[-1][0]:
                pts.append([now_ts, val])
            else:
                pts[-1] = [pts[-1][0], val]
        series[code] = pts
      except Exception as e:
        print(f"[history] curve failed for {code}: {e!r}")
        series[code] = []
    return series


async def _refresh_series(uid):
    try:
        series = await _compute_series(uid)
        _series_cache[uid] = (time.time(), series)
    except Exception as e:
        print(f"[history] background refresh failed for uid {uid}: {e!r}")
    finally:
        _series_tasks.pop(uid, None)


@app.get("/api/history/today")
async def history_today(session: Optional[str] = Cookie(default=None)):
    uid = current_user_id(session)
    hit = _series_cache.get(uid)
    if hit is not None:
        computed_at, series = hit
        if time.time() - computed_at > _SERIES_TTL and uid not in _series_tasks:
            _series_tasks[uid] = asyncio.create_task(_refresh_series(uid))
        return {"series": series}
    series = await _compute_series(uid)
    _series_cache[uid] = (time.time(), series)
    return {"series": series}


# ---------------------------------------------------------------- rankings
# 排行榜 data: Eastmoney rankhandler (full-market period returns, QDII list),
# holdings (QDII US share), pingzhongdata NAV trend (volatility board, pooled
# from the returns boards' per-period top-30 union since v1.12.1).
RANK_REF = "https://fund.eastmoney.com/data/fundranking.html"
RETURNS_TTL = 12 * 3600     # full-market returns dataset
QDII_US_TTL = 7 * 86400     # holdings are quarterly disclosures
VOL_TTL = 24 * 3600         # NAV-based volatility (returns-top pool)
PERIOD_FIELDS = {"daily": "daily", "1w": "w1", "1m": "m1", "3m": "m3",
                 "6m": "m6", "1y": "y1", "ytd": "ytd"}


def _fnum(s):
    try:
        return float(s)
    except (TypeError, ValueError):
        return None


def parse_rank_rows(text: str) -> list:
    rows = []
    for m in re.finditer(r'"(\d{6},[^"]+)"', text):
        f = m.group(1).split(",")
        if len(f) < 16:
            continue
        rows.append({
            "code": f[0], "name": f[1], "date": f[3], "nav": _fnum(f[4]),
            "daily": _fnum(f[6]), "w1": _fnum(f[7]), "m1": _fnum(f[8]),
            "m3": _fnum(f[9]), "m6": _fnum(f[10]), "y1": _fnum(f[11]), "ytd": _fnum(f[14]),
        })
    return rows


def rank_cache_get(key: str):
    with db() as conn:
        row = conn.execute("SELECT payload, fetched_at FROM rank_cache WHERE key=?", (key,)).fetchone()
    if not row:
        return None, None
    return json.loads(row["payload"]), row["fetched_at"]


def rank_cache_set(key: str, payload: dict):
    with db() as conn:
        conn.execute(
            "INSERT INTO rank_cache(key, payload, fetched_at) VALUES(?,?,?) "
            "ON CONFLICT(key) DO UPDATE SET payload=excluded.payload, fetched_at=excluded.fetched_at",
            (key, json.dumps(payload, ensure_ascii=False), payload["updated_at"]),
        )


def _cache_fresh(fetched_at, ttl: int) -> bool:
    try:
        dt = datetime.fromisoformat(fetched_at)
        return (datetime.now(timezone.utc) - dt).total_seconds() < ttl
    except Exception:
        return False


async def fetch_returns_all() -> list:
    async def one_page(page):
        resp = await upstream_get(
            "https://fund.eastmoney.com/data/rankhandler.aspx?op=ph&dt=kf&ft=all&rs=&gs=0"
            f"&sc=zzf&st=desc&sd=&ed=&qdii=&tabSubtype=,,,,,&pi={page}&pn=5000&dx=1&v=0.123456",
            referer=RANK_REF, timeout=60,
        )
        # 5000-row pages: parse in a thread so the event loop stays responsive
        return await asyncio.to_thread(parse_rank_rows, resp.text)

    rows = []
    for batch in await asyncio.gather(*(one_page(p) for p in range(1, 6))):
        rows += batch
    if not rows:
        raise ValueError("empty rank data")
    return rows


async def fetch_qdii_rows() -> list:
    resp = await upstream_get(
        "https://fund.eastmoney.com/data/rankhandler.aspx?op=ph&dt=kf&ft=qdii&rs=&gs=0"
        "&sc=zzf&st=desc&sd=&ed=&qdii=&tabSubtype=,,,,,&pi=1&pn=500&dx=1&v=0.123456",
        referer=RANK_REF, timeout=30,
    )
    rows = parse_rank_rows(resp.text)
    if not rows:
        raise ValueError("empty qdii data")
    return rows


def market_of_holding(code: str) -> str:
    """Mirror of frontend marketOfHolding: A / HK / US / JP / TW / KR / OTHER.

    NOTE: alpha ticker -> US here is only a rough guess; QDII US-share ranking
    must use is_us_holding() with the pingzhongdata market-prefix map instead.
    """
    c = (code or "").strip().upper()
    if re.fullmatch(r"\d{6}", c):
        return "A"
    if re.fullmatch(r"\d{5}", c):
        return "HK"
    if re.fullmatch(r"[A-Z]{2}[A-Z0-9]{9}\d", c):
        return {"JP": "JP", "TW": "TW", "KR": "KR", "HK": "HK", "US": "US", "CN": "A"}.get(c[:2], "OTHER")
    if re.fullmatch(r"[A-Z][A-Z.\-]*", c):
        return "US"
    return "OTHER"


async def fetch_pingzhong_text(code: str) -> str:
    resp = await upstream_get(
        f"https://fund.eastmoney.com/pingzhongdata/{code}.js",
        referer="https://fund.eastmoney.com/", timeout=30,
    )
    return resp.text


def parse_stock_market_map(text: str) -> dict:
    """ticker(upper) -> Eastmoney market prefix from pingzhongdata stockCodesNew.

    Entries look like "105.NVDA" (US), "116.00883" (HK), "0.300308"/"1.600519" (A);
    non-US/HK/A listings (e.g. India NSE tickers "ICICIBC") have no numeric prefix.
    """
    m = re.search(r"stockCodesNew\s*=\s*(\[[^\]]*\])", text)
    if not m:
        return {}
    try:
        entries = json.loads(m.group(1))
    except Exception:
        return {}
    out = {}
    for e in entries:
        e = str(e).strip()
        pm = re.match(r"^(\d+)\.(.+)$", e)
        if pm:
            out[pm.group(2).strip().upper()] = pm.group(1)
        elif e:
            out[e.upper()] = None
    return out


async def fetch_stock_market_map(code: str):
    """Market-prefix map for one fund; None if pingzhongdata fetch fails twice.
    Cached in memory for 24h (holdings are quarterly disclosures)."""
    hit = _market_map_cache.get(code)
    if hit and time.time() - hit[1] < HOLDINGS_TTL:
        return hit[0]
    for _ in range(2):
        try:
            mmap = parse_stock_market_map(await fetch_pingzhong_text(code))
            _market_map_cache[code] = (mmap, time.time())
            return mmap
        except Exception:
            continue
    return hit[0] if hit else None


def is_us_holding(code: str, market_map: dict) -> bool:
    """US holding per pingzhongdata market prefix (105/106/107) or US ISIN.

    Alpha tickers without a US prefix (e.g. India NSE names) are NOT US.
    """
    c = (code or "").strip().upper()
    if re.fullmatch(r"[A-Z]{2}[A-Z0-9]{9}\d", c):
        return c[:2] == "US"
    if re.fullmatch(r"\d{5,6}", c):
        return False
    if re.fullmatch(r"[A-Z][A-Z.\-]*", c):
        return market_map.get(c) in ("105", "106", "107")
    return False


async def compute_qdii_us() -> dict:
    qdii = await fetch_qdii_rows()
    sem = asyncio.Semaphore(5)  # keep upstream load low on the small VPS

    async def one(r):
        async with sem:
            hold, market_map = await asyncio.gather(
                fetch_holdings(r["code"]), fetch_stock_market_map(r["code"]))
        hs = hold["holdings"]
        total_w = sum(h["weight"] for h in hs)
        if not hs or total_w <= 0:
            return None
        if total_w < 5:
            return {"_low_coverage": True}
        if market_map is None:
            return None  # cannot classify reliably; skip rather than guess 0%
        us_w = sum(h["weight"] for h in hs if is_us_holding(h["code"], market_map))
        return {
            "code": r["code"], "name": r["name"],
            "us_pct": round(us_w / total_w * 100, 1),
            "coverage": round(total_w, 2), "y1": r["y1"],
            "report_label": hold["report_label"],
        }

    results = await asyncio.gather(*(one(r) for r in qdii))
    excluded_low_coverage = sum(1 for x in results if x and x.get("_low_coverage"))
    items = [x for x in results if x and not x.get("_low_coverage")]
    items.sort(key=lambda x: x["us_pct"], reverse=True)
    return {"updated_at": now_iso(), "total": len(qdii),
            "excluded_low_coverage": excluded_low_coverage, "items": items[:30]}


def _parse_vol(text: str):
    m = re.search(r"Data_netWorthTrend\s*=\s*(\[.*?\]);", text, re.S)
    if not m:
        return None
    trend = json.loads(m.group(1))
    rets = [p.get("equityReturn") for p in trend if isinstance(p.get("equityReturn"), (int, float))]
    rets = rets[-250:]
    if len(rets) < 60:
        return None
    mean = sum(rets) / len(rets)
    var = sum((x - mean) ** 2 for x in rets) / (len(rets) - 1)
    return round(math.sqrt(var) * math.sqrt(250), 2), len(rets)


async def fetch_vol(code: str):
    text = await fetch_pingzhong_text(code)
    # regex/JSON over the full NAV history: keep it off the event loop
    return await asyncio.to_thread(_parse_vol, text)


async def compute_vol_top() -> dict:
    """Volatility board (v1.12.1): pool = union of the top-30 funds of each
    returns period (daily/1w/1m/3m/6m/1y/ytd), deduplicated; rank pool funds
    by annualized volatility (std of last 250 daily returns x sqrt(250))."""
    payload, _stale = await _get_or_compute(
        "returns_all", RETURNS_TTL, _compute_returns_payload)
    rows = payload["rows"]
    pool = {}  # code -> {"row": returns row, "periods_hit": [period, ...]}
    for period, field in PERIOD_FIELDS.items():
        top = sorted(rows, key=lambda r: (r[field] is None, -(r[field] or 0)))[:30]
        for r in top:
            entry = pool.setdefault(r["code"], {"row": r, "periods_hit": []})
            entry["periods_hit"].append(period)
    sem = asyncio.Semaphore(5)  # keep upstream load low on the small VPS

    async def one(code, entry):
        async with sem:
            try:
                got = await fetch_vol(code)
            except Exception:
                got = None
        if not got:
            return None
        vol, points = got
        r = entry["row"]
        return {"code": code, "name": r["name"], "vol": vol, "points": points,
                "y1": r["y1"], "periods_hit": entry["periods_hit"]}

    results = await asyncio.gather(*(one(c, e) for c, e in pool.items()))
    items = [x for x in results if x]
    items.sort(key=lambda x: x["vol"], reverse=True)
    return {"updated_at": now_iso(), "total": len(pool),
            "pool_size": len(pool), "items": items[:30]}


# Per-board locks: a user request arriving while a board is being computed
# (by pre-warm or another request) waits for that result instead of
# starting a duplicate multi-minute computation.
_rank_locks: dict = {}


def _rank_lock(key: str) -> asyncio.Lock:
    lock = _rank_locks.get(key)
    if lock is None:
        lock = _rank_locks[key] = asyncio.Lock()
    return lock


# Typical full-compute durations in seconds (measured) for warming ETAs.
_RANK_ETA = {"returns_all": 100, "vol_top": 200, "qdii_us_v2": 240}
# In-memory per-board compute state: {"state": "computing", "started_at": epoch}
_rank_state: dict = {}
_rank_tasks: dict = {}


def _rank_mark_computing(key: str):
    _rank_state[key] = {"state": "computing", "started_at": time.time()}


def _rank_mark_idle(key: str):
    _rank_state[key] = {"state": "idle", "started_at": None}


async def _compute_returns_payload() -> dict:
    rows = await fetch_returns_all()
    return {"rows": rows, "updated_at": now_iso()}


async def _get_or_compute(key: str, ttl: int, compute) -> tuple:
    """Return (payload, stale) for a ranking board, computing under a per-key
    lock when the cache is missing/stale. Raises only when there is no cached
    payload at all and computation fails. Cache reads/writes (the returns
    payload is ~20k rows) run in a thread to keep the event loop responsive."""
    payload, fetched_at = await asyncio.to_thread(rank_cache_get, key)
    if payload is not None and _cache_fresh(fetched_at, ttl):
        return payload, False
    async with _rank_lock(key):
        payload, fetched_at = await asyncio.to_thread(rank_cache_get, key)
        if payload is not None and _cache_fresh(fetched_at, ttl):
            return payload, False
        _rank_mark_computing(key)
        try:
            payload = await compute()
            await asyncio.to_thread(rank_cache_set, key, payload)
            return payload, False
        except Exception:
            if payload is not None:
                return payload, True
            raise
        finally:
            _rank_mark_idle(key)


async def _rank_compute_bg(key: str, ttl: int, compute):
    try:
        await _get_or_compute(key, ttl, compute)
    except Exception as e:
        print(f"[rank] {key}: background compute failed: {e!r}", flush=True)


def _ensure_rank_task(key: str, ttl: int, compute):
    task = _rank_tasks.get(key)
    if task is None or task.done():
        _rank_tasks[key] = asyncio.create_task(_rank_compute_bg(key, ttl, compute))


async def _board_payload(key: str, ttl: int, compute):
    """(payload, stale) if data can be served now, else None (warming).

    Fresh cache -> serve. Stale cache -> serve immediately and refresh in
    the background. No cache -> start background compute; caller sends 202.
    """
    payload, fetched_at = await asyncio.to_thread(rank_cache_get, key)
    if payload is not None and _cache_fresh(fetched_at, ttl):
        return payload, False
    _ensure_rank_task(key, ttl, compute)
    if payload is not None:
        return payload, True
    return None


def _warming_response(key: str) -> JSONResponse:
    st = _rank_state.get(key) or {}
    started = st.get("started_at") or time.time()
    eta = max(10, int(_RANK_ETA[key] - (time.time() - started)))
    return JSONResponse(status_code=202, content={
        "warming": True, "board": key, "eta_seconds": eta, "started_at": started,
    })


_RANK_BOARDS = [
    ("returns_all", RETURNS_TTL, _compute_returns_payload),
    ("vol_top", VOL_TTL, compute_vol_top),
    ("qdii_us_v2", QDII_US_TTL, compute_qdii_us),
]


async def _rank_warm_loop():
    """Background pre-warm: after a short startup delay, warm each ranking
    cache that is missing/stale, one board at a time, then re-check hourly
    so cache expiry never lands on a user request."""
    await asyncio.sleep(5)  # let the server start answering requests first
    while True:
        for key, ttl, compute in _RANK_BOARDS:
            try:
                payload, fetched_at = await asyncio.to_thread(rank_cache_get, key)
                if payload is not None and _cache_fresh(fetched_at, ttl):
                    print(f"[rank-warm] {key}: cache fresh, skip", flush=True)
                    continue
                print(f"[rank-warm] {key}: warming...", flush=True)
                t0 = time.time()
                _, stale = await _get_or_compute(key, ttl, compute)
                dt = time.time() - t0
                if stale:
                    print(f"[rank-warm] {key}: compute failed, kept stale cache ({dt:.1f}s)", flush=True)
                else:
                    print(f"[rank-warm] {key}: done in {dt:.1f}s", flush=True)
            except Exception as e:
                print(f"[rank-warm] {key}: failed: {e!r}", flush=True)
        await asyncio.sleep(3600)


@app.get("/api/rank/returns")
async def rank_returns(period: str = "1y", session: Optional[str] = Cookie(default=None)):
    current_user_id(session)
    if period not in PERIOD_FIELDS:
        raise HTTPException(400, "period 不支持")
    got = await _board_payload("returns_all", RETURNS_TTL, _compute_returns_payload)
    if got is None:
        return _warming_response("returns_all")
    payload, stale = got
    field = PERIOD_FIELDS[period]
    rows = sorted(payload["rows"], key=lambda r: (r[field] is None, -(r[field] or 0)))
    items = []
    for i, r in enumerate(rows[:30]):
        items.append({"rank": i + 1, **{k: r[k] for k in
                      ("code", "name", "date", "nav", "daily", "w1", "m1", "m3", "m6", "y1", "ytd")}})
    return {"period": period, "updated_at": payload["updated_at"],
            "total": len(payload["rows"]), "stale": stale, "items": items}


@app.get("/api/rank/qdii-us")
async def rank_qdii_us(session: Optional[str] = Cookie(default=None)):
    current_user_id(session)
    got = await _board_payload("qdii_us_v2", QDII_US_TTL, compute_qdii_us)
    if got is None:
        return _warming_response("qdii_us_v2")
    payload, stale = got
    items = [{"rank": i + 1, **x} for i, x in enumerate(payload["items"])]
    return {"updated_at": payload["updated_at"], "total": payload["total"], "stale": stale,
            "excluded_low_coverage": payload.get("excluded_low_coverage", 0), "items": items}


@app.get("/api/rank/vol")
@app.get("/api/rank/qdii-vol")  # legacy alias (pre-v1.12.1 path)
async def rank_vol(session: Optional[str] = Cookie(default=None)):
    current_user_id(session)
    got = await _board_payload("vol_top", VOL_TTL, compute_vol_top)
    if got is None:
        return _warming_response("vol_top")
    payload, stale = got
    items = [{"rank": i + 1, **x} for i, x in enumerate(payload["items"])]
    return {"updated_at": payload["updated_at"], "total": payload["total"],
            "pool_size": payload.get("pool_size", payload["total"]),
            "stale": stale, "items": items}


@app.get("/")
def index():
    # no-cache: browsers must revalidate index.html on every load so frontend
    # updates take effect immediately after deployment (no stale cached page)
    return FileResponse(STATIC_DIR / "index.html",
                        headers={"Cache-Control": "no-cache, must-revalidate"})


init_db()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host=os.environ.get("HOST", "127.0.0.1"), port=int(os.environ.get("PORT", "8000")))
