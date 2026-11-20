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
from datetime import datetime, timezone
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
NAV_TTL = 3600          # seconds, DB cache
HOLDINGS_TTL = 86400    # seconds, DB cache
SESSION_DAYS = 30

@asynccontextmanager
async def lifespan(_app):
    # Ranking caches warm in the background (see _rank_warm_loop); startup
    # itself never waits on upstream fetches.
    task = asyncio.create_task(_rank_warm_loop())
    try:
        yield
    finally:
        task.cancel()


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

    Returns a datetime (midnight ET on that date) or None. The year is taken
    from now_et, adjusted across the year boundary to the closest match.
    """
    m = re.match(r"([A-Z][a-z]{2})\s+(\d{1,2})\s+(\d{1,2}):(\d{2})\s*([AP]M)", (s or "").strip())
    if not m:
        return None
    month = _MONTHS.get(m.group(1))
    if not month:
        return None
    day = int(m.group(2))
    try:
        cand = datetime(now_et.year, month, day, tzinfo=ET_TZ)
        # Year boundary: pick the year that puts the date closest to today.
        if (cand.date() - now_et.date()).days > 183:
            cand = datetime(now_et.year - 1, month, day, tzinfo=ET_TZ)
        elif (now_et.date() - cand.date()).days > 183:
            cand = datetime(now_et.year + 1, month, day, tzinfo=ET_TZ)
    except ValueError:
        return None
    return cand


def us_quote_with_ext(fields: list, now_et: Optional[datetime] = None) -> dict:
    """Build a US quote dict from Sina gb_ fields.

    Regular layout: 0=name, 1=current, 2=change%, 3=Beijing datetime, 26=prev close.
    Extended hours: 21=ext price, 24=ext timestamp ('Nov 19 07:59PM EST').
    During pre/post sessions with a same-ET-date ext print, use the ext price
    and compute change vs prev close; otherwise fall back to regular fields.
    """
    if now_et is None:
        now_et = datetime.now(ET_TZ)
    cur = float(fields[1])
    prev = float(fields[26]) if len(fields) > 26 and fields[26] else cur
    price, change_pct = cur, float(fields[2])
    try:
        if us_session(now_et) in ("pre", "post") and len(fields) > 24 and fields[21]:
            ext_price = float(fields[21])
            ext_dt = parse_ext_timestamp(fields[24], now_et)
            if ext_price > 0 and prev > 0 and ext_dt is not None and ext_dt.date() == now_et.date():
                price = ext_price
                change_pct = (ext_price - prev) / prev * 100
    except Exception:
        pass  # malformed ext data -> regular-session fallback
    return {"name": fields[0], "prev_close": prev, "current": price,
            "change_pct": change_pct,
            "quote_time": fields[3] if len(fields) > 3 else None}


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
        return {s: cached[s] for s in symbols if s in cached}


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
    # Resolve fund name via search API (also validates the code exists)
    resp = await upstream_get(f"https://fundsuggest.eastmoney.com/FundSearch/api/FundSearchAPI.ashx?m=1&key={code}")
    name = next((d["NAME"] for d in resp.json().get("Datas", []) if d.get("CODE") == code and _is_fund_entry(d)), None)
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
            "prev_nav_date": nav.get("prev_nav_date"), "prev_dwjz": nav.get("prev_dwjz"), "prev_jzzzl": nav.get("prev_jzzzl"),
            "est_nav": round(est_nav, 4) if est_nav else None,
            "est_change_pct": round(est_pct, 2),
            "coverage_pct": round(coverage, 2),
            "report_label": hold["report_label"], "report_date": hold["report_date"],
            "quote_time": quote_time, "stale": nav["stale"] or hold["stale"],
            "skipped_holdings": skipped, "holdings": detail,
        })
    return {"funds": out, "categories": cats, "refresh_seconds": refresh_seconds, "disclaimer": "自算估值，仅供参考，以基金公司公布净值为准"}


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
    """Market-prefix map for one fund; None if pingzhongdata fetch fails twice."""
    for _ in range(2):
        try:
            return parse_stock_market_map(await fetch_pingzhong_text(code))
        except Exception:
            continue
    return None


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
    return FileResponse(STATIC_DIR / "index.html")


init_db()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host=os.environ.get("HOST", "127.0.0.1"), port=int(os.environ.get("PORT", "8000")))
