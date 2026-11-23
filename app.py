#!/usr/bin/env python3
"""Fund self-calculated real-time valuation MVP.

Estimation: est_change_pct = sum(weight_i% * stock_change_i%) / 100 over
disclosed top-10 holdings; est_nav = latest official NAV * (1 + est/100).
Data: Eastmoney (search / official NAV / holdings), Sina (stock quotes).
All upstream calls go through the backend with global caches.
"""
import asyncio
import gzip
import hashlib
import ipaddress
import hmac
import json
import logging
import math
import os
import random
import re
import secrets
import shutil
import smtplib
import sqlite3
import time
from email.message import EmailMessage
from contextlib import asynccontextmanager, contextmanager
from typing import Optional
from datetime import datetime, timedelta, timezone
from html.parser import HTMLParser
from urllib.parse import parse_qs, urlparse
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
APP_VERSION = "v1.4.8"  # keep in sync with the footer in static/index.html

# --- Email verification codes (v1.4.0, feature-flagged in v1.4.0) ---
# EMAIL_AUTH=1 enables email-code registration / reset / bind. Default OFF:
# registration is username+password only, so a broken SMTP setup can never
# block customers from registering.
EMAIL_AUTH = os.environ.get("EMAIL_AUTH", "0").strip() == "1"
# Ranking-fund prewarm backfill (v1.4.2): kill switch + rate limit
# (funds processed per minute; the worker is strictly sequential).
PREWARM_ENABLED = os.environ.get("PREWARM_ENABLED", "1").strip() != "0"
try:
    PREWARM_PER_MIN = float(os.environ.get("PREWARM_PER_MIN", "1") or 1)
except ValueError:
    PREWARM_PER_MIN = 1.0
if PREWARM_PER_MIN <= 0:
    PREWARM_PER_MIN = 1.0
# Tonghuashun fuyao official API (v1.4.7): independent fallback source for
# fund NAV / holdings / stock position when Eastmoney fails. The key comes
# from the environment only (never committed); unset = fallback disabled.
FUYAO_API_KEY = os.environ.get("FUYAO_API_KEY", "").strip()
FUYAO_ENABLED = bool(FUYAO_API_KEY)
FUYAO_BASE = "https://fuyao.aicubes.cn"
# SMTP config comes from environment variables only (never committed).
# When SMTP_HOST is empty the app runs in LOG MODE: codes are written to
# the server log instead of being emailed, so the flow is testable before
# real SMTP is configured on the deployment host.
SMTP_HOST = os.environ.get("SMTP_HOST", "").strip()
SMTP_PORT = int(os.environ.get("SMTP_PORT", "465") or 465)
SMTP_USER = os.environ.get("SMTP_USER", "").strip()
SMTP_PASS = os.environ.get("SMTP_PASS", "")
SMTP_FROM = os.environ.get("SMTP_FROM", "").strip() or SMTP_USER
_smtp_ssl_env = os.environ.get("SMTP_SSL", "").strip()
SMTP_SSL = (_smtp_ssl_env == "1") if _smtp_ssl_env else (SMTP_PORT == 465)
EMAIL_CODE_TTL = 600        # seconds a code stays valid
EMAIL_CODE_COOLDOWN = 60    # seconds between sends (same email+purpose)
EMAIL_CODE_DAILY_MAX = 10   # sends per email per day (all purposes)
EMAIL_CODE_MAX_ATTEMPTS = 5
EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
logger = logging.getLogger("fund-valuation")
BACKUP_DIR = DB_PATH.parent / "backups"
BACKUP_KEEP = 10
# New backups are gzip-compressed (.db.gz); legacy plain .db backups from
# v1.4.0 remain listable/downloadable/restorable.
BACKUP_NAME_RE = re.compile(r"^fund-backup-\d{8}-\d{6}\.db(\.gz)?$")
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36"
FUND_CODE_RE = re.compile(r"^\d{6}$")
QUOTE_TTL = 45          # seconds, in-memory global cache
NAV_TTL = 600           # seconds, DB cache
HOLDINGS_TTL = 86400    # seconds, DB cache
FUND_TYPE_TTL = 30 * 86400  # seconds, DB cache (fund type rarely changes)
SESSION_DAYS = 30

@asynccontextmanager
async def lifespan(_app):
    # Ranking caches warm in the background (see _rank_warm_loop); startup
    # itself never waits on upstream fetches. The quote warmer keeps the
    # in-memory quote cache hot so first page loads don't wait on Sina.
    tasks = [asyncio.create_task(_rank_warm_loop()),
             asyncio.create_task(_quote_warm_loop()),
             asyncio.create_task(_trends_warm_loop()),
             asyncio.create_task(_backup_loop()),
             asyncio.create_task(_beta_loop()),
             asyncio.create_task(_prewarm_loop())]
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
        CREATE TABLE IF NOT EXISTS stock_industry(
            code TEXT PRIMARY KEY,
            industry TEXT,
            fetched_at REAL NOT NULL);
        CREATE TABLE IF NOT EXISTS fund_type_cache(
            fund_code TEXT PRIMARY KEY,
            ftype TEXT,
            fetched_at REAL NOT NULL);
        CREATE TABLE IF NOT EXISTS mrf_symbol_map(
            fund_code TEXT PRIMARY KEY,
            isin TEXT,
            yahoo_symbol TEXT,
            resolved_at REAL NOT NULL);
        CREATE TABLE IF NOT EXISTS fund_stock_position(
            fund_code TEXT PRIMARY KEY,
            stock_pct REAL,
            fetched_at REAL NOT NULL);
        CREATE TABLE IF NOT EXISTS fund_beta(
            fund_code TEXT PRIMARY KEY,
            alpha REAL,
            beta REAL,
            n INTEGER NOT NULL DEFAULT 0,
            mae_raw REAL,
            mae_cal REAL,
            updated_at REAL NOT NULL);
        CREATE TABLE IF NOT EXISTS stock_daily_cache(
            secid TEXT NOT NULL,
            date TEXT NOT NULL,
            close REAL NOT NULL,
            fetched_at REAL NOT NULL,
            PRIMARY KEY(secid, date));
        CREATE TABLE IF NOT EXISTS prewarm_queue(
            fund_code TEXT PRIMARY KEY,
            status TEXT NOT NULL DEFAULT 'pending',
            priority INTEGER NOT NULL DEFAULT 0,
            attempts INTEGER NOT NULL DEFAULT 0,
            updated_at REAL NOT NULL);
        CREATE TABLE IF NOT EXISTS system_errors(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts REAL NOT NULL,
            source TEXT NOT NULL,
            message TEXT NOT NULL);
        CREATE INDEX IF NOT EXISTS idx_system_errors_ts ON system_errors(ts);
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
    # Migration: users.is_admin (added 2026-11-20, v1.2.0)
    if "is_admin" not in ucols:
        conn.execute("ALTER TABLE users ADD COLUMN is_admin INTEGER NOT NULL DEFAULT 0")
    # Migration: users.email (added 2026-11-23, v1.4.0) — nullable for
    # legacy users; unique via index (SQLite ADD COLUMN cannot take UNIQUE;
    # a unique index still allows multiple NULLs).
    if "email" not in ucols:
        conn.execute("ALTER TABLE users ADD COLUMN email TEXT")
    conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_users_email ON users(email)")
    # Migration: online tracking (added 2026-11-23, v1.4.0) — session
    # heartbeat timestamp + accumulated online seconds per user, plus an
    # IP geolocation cache (province/city via ip-api.com, fetched once).
    if "total_online_seconds" not in ucols:
        conn.execute("ALTER TABLE users ADD COLUMN total_online_seconds INTEGER NOT NULL DEFAULT 0")
    scols = [r[1] for r in conn.execute("PRAGMA table_info(sessions)").fetchall()]
    if "last_seen_at" not in scols:
        conn.execute("ALTER TABLE sessions ADD COLUMN last_seen_at TEXT")
    conn.execute(
        "CREATE TABLE IF NOT EXISTS ip_geo("
        "ip TEXT PRIMARY KEY, country TEXT, region TEXT, city TEXT, "
        "fetched_at REAL NOT NULL)"
    )
    # Email verification codes (v1.4.0): one active code per (email,
    # purpose); codes stored hashed. email_code_log backs the daily cap.
    conn.execute(
        "CREATE TABLE IF NOT EXISTS email_codes("
        "email TEXT NOT NULL, purpose TEXT NOT NULL, code_hash TEXT NOT NULL, "
        "expires_at REAL NOT NULL, attempts INTEGER NOT NULL DEFAULT 0, "
        "created_at REAL NOT NULL, PRIMARY KEY(email, purpose))"
    )
    conn.execute(
        "CREATE TABLE IF NOT EXISTS email_code_log("
        "id INTEGER PRIMARY KEY AUTOINCREMENT, email TEXT NOT NULL, "
        "purpose TEXT NOT NULL, sent_at REAL NOT NULL)"
    )
    # Login log table (added 2026-11-20, v1.2.0)
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
    # Migration: mrf_symbol_map.isin (added 2026-11-23, v1.4.0)
    mcols = [r[1] for r in conn.execute("PRAGMA table_info(mrf_symbol_map)").fetchall()]
    if mcols and "isin" not in mcols:
        conn.execute("ALTER TABLE mrf_symbol_map ADD COLUMN isin TEXT")
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


def _iso(ts):
    """Epoch seconds -> UTC ISO string (None-safe)."""
    if not ts:
        return None
    return datetime.fromtimestamp(ts, timezone.utc).isoformat()


# ------------------------------------------------- system status (v1.4.3)
# In-memory background-worker status for the admin 系统状态 page. Each
# loop records last_run / last_result / next_run (epoch seconds) here;
# the page combines this with DB-derived stats.
_worker_status: dict = {}


def _wstat(name: str, **kw):
    st = _worker_status.setdefault(name, {})
    st.update(kw)
    return st


_ERR_DEDUPE_SEC = 600   # same source+message recorded at most once per 10 min
_ERR_KEEP_SEC = 7 * 86400


def log_error(source: str, message: str):
    """Record a system error for the admin 系统状态 page (v1.4.3).
    Dedupes identical source+message within 10 minutes so an upstream
    outage can't flood the table; prunes rows older than 7 days.
    Never raises — error logging must not break the caller."""
    try:
        msg = str(message)[:500]
        now = time.time()
        with db() as conn:
            last = conn.execute(
                "SELECT MAX(ts) AS t FROM system_errors WHERE source=? AND message=?",
                (source, msg)).fetchone()
            if last and last["t"] and now - last["t"] < _ERR_DEDUPE_SEC:
                return
            conn.execute(
                "INSERT INTO system_errors(ts, source, message) VALUES(?,?,?)",
                (now, source, msg))
            conn.execute("DELETE FROM system_errors WHERE ts < ?",
                         (now - _ERR_KEEP_SEC,))
    except Exception:
        pass


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


def _parse_iso(ts):
    if not ts:
        return None
    try:
        return datetime.fromisoformat(ts)
    except Exception:
        return None


def _heartbeat(conn, token: str, user_id: int, last_seen_at):
    """Session heartbeat (v1.4.0): at most one write per minute per
    session; accumulate the elapsed gap into users.total_online_seconds
    when it looks like continuous activity (<=600s)."""
    now = datetime.now(timezone.utc)
    last = _parse_iso(last_seen_at)
    if last is not None:
        delta = (now - last).total_seconds()
        if delta < 60:
            return
        if 0 < delta <= 600:
            conn.execute(
                "UPDATE users SET total_online_seconds = total_online_seconds + ? WHERE id=?",
                (int(delta), user_id),
            )
    conn.execute("UPDATE sessions SET last_seen_at=? WHERE token=?", (now.isoformat(), token))


def current_user_id(session: Optional[str]) -> int:
    if not session:
        raise HTTPException(401, "未登录")
    with db() as conn:
        row = conn.execute(
            "SELECT user_id, last_seen_at FROM sessions WHERE token=?", (session,)
        ).fetchone()
        if not row:
            raise HTTPException(401, "会话已失效，请重新登录")
        _heartbeat(conn, session, row["user_id"], row["last_seen_at"])
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


# --- email verification codes (v1.4.0) ---
def _require_email_auth():
    if not EMAIL_AUTH:
        raise HTTPException(404, "功能未启用")


def _norm_email(email: str) -> str:
    email = (email or "").strip().lower()
    if not EMAIL_RE.match(email):
        raise HTTPException(400, "邮箱格式不正确")
    return email


def _code_hash(email: str, purpose: str, code: str) -> str:
    return hashlib.sha256(f"{email}:{purpose}:{code}".encode()).hexdigest()


def _send_code_mail(email: str, purpose: str, code: str):
    """Send the code by SMTP, or log it when SMTP is not configured.

    Codes are never logged when real SMTP is configured.
    """
    if not SMTP_HOST:
        logger.warning("[email-code] to=%s purpose=%s code=%s", email, purpose, code)
        return
    msg = EmailMessage()
    msg["Subject"] = "【基金自算估值】验证码"
    msg["From"] = SMTP_FROM
    msg["To"] = email
    msg.set_content(
        f"您的验证码是：{code}\n\n"
        "验证码 10 分钟内有效，请勿泄露给他人。\n"
        "如非本人操作，请忽略本邮件。"
    )
    if SMTP_SSL:
        with smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT, timeout=15) as s:
            if SMTP_USER:
                s.login(SMTP_USER, SMTP_PASS)
            s.send_message(msg)
    else:
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=15) as s:
            s.starttls()
            if SMTP_USER:
                s.login(SMTP_USER, SMTP_PASS)
            s.send_message(msg)


def _issue_code(conn, email: str, purpose: str):
    """Rate-limit, generate, store and send a fresh code (replaces old)."""
    now = time.time()
    row = conn.execute(
        "SELECT created_at FROM email_codes WHERE email=? AND purpose=?", (email, purpose)
    ).fetchone()
    if row and now - row["created_at"] < EMAIL_CODE_COOLDOWN:
        retry = int(EMAIL_CODE_COOLDOWN - (now - row["created_at"])) + 1
        raise HTTPException(429, f"发送太频繁，请 {retry} 秒后再试")
    day_start = datetime.now(ZoneInfo("Asia/Shanghai")).replace(
        hour=0, minute=0, second=0, microsecond=0).timestamp()
    sent_today = conn.execute(
        "SELECT COUNT(*) AS n FROM email_code_log WHERE email=? AND sent_at>=?",
        (email, day_start),
    ).fetchone()["n"]
    if sent_today >= EMAIL_CODE_DAILY_MAX:
        raise HTTPException(429, "今日验证码发送次数已达上限，请明天再试")
    code = f"{secrets.randbelow(1_000_000):06d}"
    conn.execute(
        "INSERT INTO email_codes(email, purpose, code_hash, expires_at, attempts, created_at) "
        "VALUES(?,?,?,?,0,?) ON CONFLICT(email, purpose) DO UPDATE SET "
        "code_hash=excluded.code_hash, expires_at=excluded.expires_at, "
        "attempts=0, created_at=excluded.created_at",
        (email, purpose, _code_hash(email, purpose, code), now + EMAIL_CODE_TTL, now),
    )
    conn.execute(
        "INSERT INTO email_code_log(email, purpose, sent_at) VALUES(?,?,?)",
        (email, purpose, now),
    )
    conn.commit()
    try:
        _send_code_mail(email, purpose, code)
    except Exception:
        logger.exception("email code send failed")
        # Roll back the issued code so the user can retry immediately.
        conn.execute("DELETE FROM email_codes WHERE email=? AND purpose=?", (email, purpose))
        conn.execute(
            "DELETE FROM email_code_log WHERE id=(SELECT MAX(id) FROM email_code_log WHERE email=?)",
            (email,),
        )
        conn.commit()
        raise HTTPException(502, "邮件发送失败，请稍后重试")


def _verify_code(conn, email: str, purpose: str, code: str):
    """Verify a code; raises HTTPException on any failure.

    Wrong attempts are committed immediately (the db() context would roll
    back on raise). Success consumes (deletes) the code.
    """
    code = (code or "").strip()
    row = conn.execute(
        "SELECT code_hash, expires_at, attempts FROM email_codes WHERE email=? AND purpose=?",
        (email, purpose),
    ).fetchone()
    if not row:
        raise HTTPException(400, "验证码不存在或已失效，请重新获取")
    if time.time() > row["expires_at"]:
        conn.execute("DELETE FROM email_codes WHERE email=? AND purpose=?", (email, purpose))
        conn.commit()
        raise HTTPException(400, "验证码已过期，请重新获取")
    if row["attempts"] >= EMAIL_CODE_MAX_ATTEMPTS:
        conn.execute("DELETE FROM email_codes WHERE email=? AND purpose=?", (email, purpose))
        conn.commit()
        raise HTTPException(400, "验证码错误次数过多，请重新获取")
    if not hmac.compare_digest(_code_hash(email, purpose, code), row["code_hash"]):
        attempts = row["attempts"] + 1
        if attempts >= EMAIL_CODE_MAX_ATTEMPTS:
            conn.execute("DELETE FROM email_codes WHERE email=? AND purpose=?", (email, purpose))
        else:
            conn.execute(
                "UPDATE email_codes SET attempts=? WHERE email=? AND purpose=?",
                (attempts, email, purpose),
            )
        conn.commit()
        raise HTTPException(400, "验证码不正确")
    conn.execute("DELETE FROM email_codes WHERE email=? AND purpose=?", (email, purpose))
    conn.commit()


class AuthBody(BaseModel):
    username: str
    password: str


class AddFundBody(BaseModel):
    fund_code: str
    category: str = ""
    fund_name: str = ""  # from the frontend's search result; resolved in
    # the background when omitted so the mutation never waits on upstream


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


class SendCodeBody(BaseModel):
    email: str
    purpose: str  # register | reset | bind


class RegisterBody(BaseModel):
    username: str
    password: str
    # Optional so username+password registration works when EMAIL_AUTH is
    # off; when EMAIL_AUTH=1 both are required and verified in register().
    email: str = ""
    code: str = ""


class ResetPasswordBody(BaseModel):
    email: str
    code: str
    new_password: str


class BindEmailBody(BaseModel):
    email: str
    code: str


@app.get("/api/config")
def public_config():
    # Unauthenticated bootstrap config for the frontend (the register form
    # is shown before login, so /api/me alone cannot carry this flag).
    return {"email_auth": EMAIL_AUTH, "version": APP_VERSION,
            "data_flow_ok": _data_flow_ok()}


@app.post("/api/email/send-code")
def send_email_code(body: SendCodeBody, session: Optional[str] = Cookie(default=None)):
    _require_email_auth()
    email = _norm_email(body.email)
    purpose = (body.purpose or "").strip()
    if purpose not in ("register", "reset", "bind"):
        raise HTTPException(400, "purpose 不正确")
    with db() as conn:
        owner = conn.execute("SELECT id FROM users WHERE email=?", (email,)).fetchone()
        if purpose == "register":
            if owner:
                raise HTTPException(409, "该邮箱已注册")
        elif purpose == "reset":
            if not owner:
                # Same response as a real send — don't reveal existence.
                return {"ok": True}
        elif purpose == "bind":
            uid = current_user_id(session)
            me_row = conn.execute("SELECT email FROM users WHERE id=?", (uid,)).fetchone()
            if me_row and me_row["email"]:
                raise HTTPException(400, "已绑定邮箱，如需更换请联系管理员")
            if owner and owner["id"] != uid:
                raise HTTPException(409, "该邮箱已被其他账号使用")
        _issue_code(conn, email, purpose)
    return {"ok": True}


@app.post("/api/register")
def register(body: RegisterBody, response: Response):
    username = body.username.strip()
    if len(username) < 3 or len(username) > 32:
        raise HTTPException(400, "用户名需 3-32 个字符")
    if len(body.password) < 6:
        raise HTTPException(400, "密码至少 6 位")
    # EMAIL_AUTH off (default): plain username+password registration; any
    # email/code in the body is ignored. On: v1.4.0 verified-email flow.
    email = _norm_email(body.email) if EMAIL_AUTH else None
    with db() as conn:
        if EMAIL_AUTH:
            _verify_code(conn, email, "register", body.code)
        try:
            cur = conn.execute(
                "INSERT INTO users(username, password_hash, created_at, email) VALUES(?,?,?,?)",
                (username, hash_password(body.password), now_iso(), email),
            )
        except sqlite3.IntegrityError:
            raise HTTPException(409, "用户名或邮箱已存在" if EMAIL_AUTH else "用户名已存在")
        user_id = cur.lastrowid
        token = secrets.token_urlsafe(32)
        conn.execute("INSERT INTO sessions(token, user_id, created_at) VALUES(?,?,?)", (token, user_id, now_iso()))
    response.set_cookie("session", token, max_age=SESSION_DAYS * 86400, httponly=True, samesite="lax", path="/")
    return {"ok": True, "user_id": user_id, "username": username, "email": email}


@app.post("/api/password/reset")
def reset_password(body: ResetPasswordBody):
    _require_email_auth()
    email = _norm_email(body.email)
    if len(body.new_password) < 6:
        raise HTTPException(400, "密码至少 6 位")
    with db() as conn:
        _verify_code(conn, email, "reset", body.code)
        row = conn.execute("SELECT id FROM users WHERE email=?", (email,)).fetchone()
        if not row:
            raise HTTPException(400, "验证码不正确")
        conn.execute("UPDATE users SET password_hash=? WHERE id=?", (hash_password(body.new_password), row["id"]))
        conn.execute("DELETE FROM sessions WHERE user_id=?", (row["id"],))
    return {"ok": True}


@app.post("/api/email/bind")
def bind_email(body: BindEmailBody, session: Optional[str] = Cookie(default=None)):
    _require_email_auth()
    uid = current_user_id(session)
    email = _norm_email(body.email)
    with db() as conn:
        me_row = conn.execute("SELECT email FROM users WHERE id=?", (uid,)).fetchone()
        if me_row and me_row["email"]:
            raise HTTPException(400, "已绑定邮箱，如需更换请联系管理员")
        _verify_code(conn, email, "bind", body.code)
        try:
            conn.execute("UPDATE users SET email=? WHERE id=?", (email, uid))
        except sqlite3.IntegrityError:
            raise HTTPException(409, "该邮箱已被其他账号使用")
    return {"ok": True, "email": email}


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
        row = conn.execute("SELECT username, is_admin, email FROM users WHERE id=?", (uid,)).fetchone()
    return {"user_id": uid, "username": row["username"], "is_admin": bool(row["is_admin"]),
            "email": row["email"], "email_auth": EMAIL_AUTH}


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
    _est_cache.pop(uid, None)  # payload carries refresh_seconds
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


# --- IP geolocation (v1.4.0): province/city via ip-api.com, cached
# forever in ip_geo; failures retried after 7 days. ---
IP_GEO_RETRY_SECONDS = 7 * 86400
ONLINE_WINDOW_SECONDS = 300


def _is_public_ip(ip: str) -> bool:
    try:
        addr = ipaddress.ip_address(ip)
    except ValueError:
        return False
    return not (addr.is_private or addr.is_loopback or addr.is_link_local
                or addr.is_reserved or addr.is_multicast or addr.is_unspecified)


def _fetch_ip_geo(ip: str):
    """One ip-api.com lookup (sync, 2s timeout). Returns (country, region,
    city) or None on failure."""
    try:
        with httpx.Client(timeout=2.0) as client:
            resp = client.get(
                f"http://ip-api.com/json/{ip}",
                params={"lang": "zh-CN", "fields": "status,country,regionName,city"},
            )
            data = resp.json()
        if data.get("status") != "success":
            _record_source("http://ip-api.com/json/", False,
                           f"status={data.get('status')}")
            return None
        _record_source("http://ip-api.com/json/", True)
        return (data.get("country") or "", data.get("regionName") or "",
                data.get("city") or "")
    except Exception as e:
        _record_source("http://ip-api.com/json/", False,
                       f"{type(e).__name__}: {e}")
        return None


def _ip_region(conn, ip: Optional[str]) -> str:
    """Human-readable region for an IP: '广东省 深圳市', country prefixed
    when not 中国； private/invalid IPs -> '内网/未知'."""
    if not ip or not _is_public_ip(ip):
        return "内网/未知"
    row = conn.execute(
        "SELECT country, region, city, fetched_at FROM ip_geo WHERE ip=?", (ip,)
    ).fetchone()
    if row is None or (not row["region"] and not row["city"]
                        and time.time() - row["fetched_at"] >= IP_GEO_RETRY_SECONDS):
        got = _fetch_ip_geo(ip)
        country, region, city = got if got else ("", "", "")
        conn.execute(
            "INSERT INTO ip_geo(ip, country, region, city, fetched_at) VALUES(?,?,?,?,?) "
            "ON CONFLICT(ip) DO UPDATE SET country=excluded.country, "
            "region=excluded.region, city=excluded.city, fetched_at=excluded.fetched_at",
            (ip, country, region, city, time.time()),
        )
        conn.commit()
    else:
        country, region, city = row["country"] or "", row["region"] or "", row["city"] or ""
    parts = []
    if country and country != "中国":
        parts.append(country)
    if region:
        parts.append(region)
    if city and city != region:
        parts.append(city)
    return " ".join(parts) if parts else "未知"


# ---------------------------------------------------------------- admin (v1.2.0)
class AdminPasswordBody(BaseModel):
    password: str


@app.get("/api/admin/users")
def admin_list_users(session: Optional[str] = Cookie(default=None)):
    require_admin(session)
    now = datetime.now(timezone.utc)
    with db() as conn:
        rows = conn.execute(
            "SELECT u.id, u.username, u.is_admin, u.created_at, u.email, "
            "u.total_online_seconds, "
            "(SELECT COUNT(*) FROM user_funds WHERE user_id=u.id) AS fund_count, "
            "(SELECT MAX(created_at) FROM login_log WHERE user_id=u.id AND success=1) AS last_login, "
            "(SELECT COUNT(*) FROM login_log WHERE user_id=u.id AND success=1) AS login_count, "
            "(SELECT MAX(last_seen_at) FROM sessions WHERE user_id=u.id) AS last_seen_at, "
            "(SELECT ip FROM login_log WHERE user_id=u.id AND success=1 ORDER BY id DESC LIMIT 1) AS last_ip_ok, "
            "(SELECT ip FROM login_log WHERE user_id=u.id ORDER BY id DESC LIMIT 1) AS last_ip_any "
            "FROM users u ORDER BY u.created_at, u.id"
        ).fetchall()
        users = []
        for seq, r in enumerate(rows, start=1):
            last_seen = _parse_iso(r["last_seen_at"])
            online = bool(last_seen and (now - last_seen).total_seconds() <= ONLINE_WINDOW_SECONDS)
            last_logins = [x["created_at"] for x in conn.execute(
                "SELECT created_at FROM login_log WHERE user_id=? AND success=1 "
                "ORDER BY id DESC LIMIT 2", (r["id"],)
            ).fetchall()]
            last_ip = r["last_ip_ok"] or r["last_ip_any"]
            users.append({
                "seq": seq,
                "id": r["id"], "username": r["username"], "is_admin": bool(r["is_admin"]),
                "created_at": r["created_at"], "fund_count": r["fund_count"],
                "last_login": r["last_login"], "email": r["email"],
                "online": online, "last_seen_at": r["last_seen_at"],
                "last_ip": last_ip, "ip_region": _ip_region(conn, last_ip),
                "total_online_seconds": r["total_online_seconds"] or 0,
                "login_count": r["login_count"], "last_logins": last_logins,
            })
    return {"users": users}


class AdminCreateUserBody(BaseModel):
    username: str
    password: str
    email: Optional[str] = None


@app.post("/api/admin/users")
def admin_create_user(body: AdminCreateUserBody, session: Optional[str] = Cookie(default=None)):
    require_admin(session)
    username = body.username.strip()
    if len(username) < 3 or len(username) > 32:
        raise HTTPException(400, "用户名需 3-32 个字符")
    if len(body.password) < 6:
        raise HTTPException(400, "密码至少 6 位")
    email = _norm_email(body.email) if (body.email or "").strip() else None
    with db() as conn:
        try:
            cur = conn.execute(
                "INSERT INTO users(username, password_hash, created_at, email) VALUES(?,?,?,?)",
                (username, hash_password(body.password), now_iso(), email),
            )
        except sqlite3.IntegrityError:
            raise HTTPException(409, "用户名或邮箱已存在")
        user_id = cur.lastrowid
    return {"ok": True, "user_id": user_id, "username": username, "email": email}


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
    _est_cache.pop(user_id, None)
    _series_cache.pop(user_id, None)
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


# ---------------------------------------------------------------- backup
# System backup & restore (admin only). A backup is a consistent SQLite
# copy (sqlite3 backup API, WAL-safe) in BACKUP_DIR, gzip-compressed to
# .db.gz to save space (v1.4.0; legacy plain .db backups from v1.4.0
# still list/download/restore), plus a .json sidecar with metadata.
# Retention: newest BACKUP_KEEP across all kinds. Restore copies the
# backup INTO the live DB via the backup API (no file swap, so WAL stays
# consistent), then re-runs init_db() so every migration applies — old
# backups restore into new code. Version is recorded for information
# only and never blocks a restore.

def _backup_sidecar(path: Path) -> Path:
    """Sidecar .json for a backup file (.db or .db.gz): same stem."""
    name = path.name
    if name.endswith(".gz"):
        name = name[:-3]
    if name.endswith(".db"):
        name = name[:-3]
    return path.with_name(name + ".json")


def _backup_stamp_str(path: Path) -> str:
    """Filename without .db/.db.gz suffixes, for timestamp parsing."""
    name = path.name
    if name.endswith(".gz"):
        name = name[:-3]
    if name.endswith(".db"):
        name = name[:-3]
    return name


def _backup_entry(path: Path) -> dict:
    meta = {}
    sidecar = _backup_sidecar(path)
    if sidecar.is_file():
        try:
            meta = json.loads(sidecar.read_text(encoding="utf-8"))
        except Exception:
            meta = {}
    if not meta.get("created_at"):
        # Sidecar missing/corrupt (e.g. a .db copied in by hand): derive
        # the timestamp from the filename.
        try:
            dt = datetime.strptime(_backup_stamp_str(path), "fund-backup-%Y%m%d-%H%M%S").replace(tzinfo=CN_TZ)
            meta["created_at"] = dt.isoformat()
        except ValueError:
            meta["created_at"] = datetime.fromtimestamp(path.stat().st_mtime, CN_TZ).isoformat()
    return {
        "name": path.name,
        "created_at": meta.get("created_at", ""),
        "kind": meta.get("kind") or "manual",
        "size_bytes": path.stat().st_size,
        "app_version": meta.get("app_version") or "",
        "compressed": path.name.endswith(".gz"),
    }


def _list_backups() -> list:
    if not BACKUP_DIR.is_dir():
        return []
    paths = [p for p in list(BACKUP_DIR.glob("*.db")) + list(BACKUP_DIR.glob("*.db.gz"))
             if BACKUP_NAME_RE.fullmatch(p.name)]
    # Timestamped names sort chronologically; newest first.
    return [_backup_entry(p) for p in sorted(paths, key=lambda p: p.name, reverse=True)]


def _prune_backups():
    entries = _list_backups()
    for e in entries[BACKUP_KEEP:]:
        p = BACKUP_DIR / e["name"]
        p.unlink(missing_ok=True)
        _backup_sidecar(p).unlink(missing_ok=True)


def _make_backup(kind: str) -> dict:
    BACKUP_DIR.mkdir(parents=True, exist_ok=True)
    created = datetime.now(CN_TZ)
    stamp = created
    while True:  # same-second collisions bump the filename stamp
        name = f"fund-backup-{stamp:%Y%m%d-%H%M%S}.db.gz"
        dest = BACKUP_DIR / name
        legacy = BACKUP_DIR / f"fund-backup-{stamp:%Y%m%d-%H%M%S}.db"
        if not dest.exists() and not legacy.exists():
            break
        stamp += timedelta(seconds=1)
    raw = BACKUP_DIR / (name + ".rawtmp")
    try:
        src = sqlite3.connect(DB_PATH)
        try:
            dst = sqlite3.connect(raw)
            try:
                src.backup(dst)
            finally:
                dst.close()
        finally:
            src.close()
        with open(raw, "rb") as fin, gzip.open(dest, "wb") as fout:
            shutil.copyfileobj(fin, fout)
    finally:
        raw.unlink(missing_ok=True)
    meta = {"created_at": created.isoformat(), "app_version": APP_VERSION,
            "kind": kind, "size_bytes": dest.stat().st_size, "compressed": True}
    _backup_sidecar(dest).write_text(
        json.dumps(meta, ensure_ascii=False), encoding="utf-8")
    _prune_backups()
    return {"name": name, **meta}


def _restore_backup(name: str) -> dict:
    """Validate + restore one backup into the live DB. Returns the
    pre-restore backup entry. Raises ValueError on an unusable file.
    Accepts both gzip-compressed (.db.gz, current) and legacy plain
    (.db, v1.4.0) backups."""
    path = BACKUP_DIR / name
    # Materialize a plain .db temp file first (gunzip when needed). This
    # also snapshots the target: the pre-restore backup below prunes to
    # BACKUP_KEEP and could delete the very file being restored (when it
    # is among the oldest), which would leave sqlite3.connect() to create
    # an empty DB and copy that over the live one.
    tmp = BACKUP_DIR / (name + ".restore-tmp")
    try:
        if name.endswith(".gz"):
            try:
                with gzip.open(path, "rb") as fin, open(tmp, "wb") as fout:
                    shutil.copyfileobj(fin, fout)
            except (OSError, EOFError):
                raise ValueError("备份文件解压失败，不是有效的 gzip 备份")
        else:
            shutil.copyfile(path, tmp)
        try:
            conn = sqlite3.connect(tmp.as_uri() + "?mode=ro", uri=True)
            try:
                ok = conn.execute("PRAGMA integrity_check").fetchone()[0]
                has_users = conn.execute(
                    "SELECT 1 FROM sqlite_master WHERE type='table' AND name='users'").fetchone()
            finally:
                conn.close()
        except sqlite3.Error:
            raise ValueError("备份文件不是有效的 SQLite 数据库")
        if ok != "ok":
            raise ValueError("备份文件完整性校验失败")
        if not has_users:
            raise ValueError("备份文件缺少 users 表，不是有效的系统备份")
        pre = _make_backup("pre-restore")
        src = sqlite3.connect(tmp)
        try:
            dst = sqlite3.connect(DB_PATH)
            try:
                dst.execute("PRAGMA busy_timeout=10000")
                src.backup(dst)
            finally:
                dst.close()
        finally:
            src.close()
    finally:
        tmp.unlink(missing_ok=True)
    init_db()  # apply all migrations so old backups work on new code
    return pre


async def _backup_loop():
    """Daily 00:00 (Asia/Shanghai) automatic backup; creates one initial
    backup at startup when none exists so the feature is visibly on."""
    await asyncio.sleep(15)
    try:
        if not _list_backups():
            entry = await asyncio.to_thread(_make_backup, "auto")
            _wstat("backup", last_run=time.time(),
                   last_result=f"initial backup {entry['name']}")
            print(f"[backup] initial backup {entry['name']}", flush=True)
    except Exception as e:
        log_error("backup", f"initial backup failed: {e!r}")
        print(f"[backup] initial backup failed: {e!r}", flush=True)
    while True:
        now = datetime.now(CN_TZ)
        nxt = (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        _wstat("backup", next_run=nxt.timestamp())
        await asyncio.sleep(max(1.0, (nxt - now).total_seconds()))
        try:
            entry = await asyncio.to_thread(_make_backup, "auto")
            _wstat("backup", last_run=time.time(),
                   last_result=f"daily backup {entry['name']}")
            print(f"[backup] daily backup {entry['name']}", flush=True)
        except Exception as e:
            log_error("backup", f"daily backup failed: {e!r}")
            _wstat("backup", last_run=time.time(),
                   last_result=f"failed: {e!r}")
            print(f"[backup] daily backup failed: {e!r}", flush=True)


@app.get("/api/admin/backups")
def admin_list_backups(session: Optional[str] = Cookie(default=None)):
    require_admin(session)
    return {"backups": _list_backups()}


@app.post("/api/admin/backups")
async def admin_create_backup(session: Optional[str] = Cookie(default=None)):
    require_admin(session)
    try:
        entry = await asyncio.to_thread(_make_backup, "manual")
    except Exception as e:
        log_error("backup", f"manual backup failed: {e!r}")
        raise HTTPException(500, f"备份失败：{e}")
    _wstat("backup", last_run=time.time(),
           last_result=f"manual backup {entry['name']}")
    return {"ok": True, "backup": entry}


@app.get("/api/admin/backups/{name}/download")
def admin_download_backup(name: str, session: Optional[str] = Cookie(default=None)):
    require_admin(session)
    if not BACKUP_NAME_RE.fullmatch(name):
        raise HTTPException(400, "备份文件名不合法")
    path = BACKUP_DIR / name
    if not path.is_file():
        raise HTTPException(404, "备份不存在")
    return FileResponse(path, filename=name, media_type="application/octet-stream")


@app.post("/api/admin/backups/{name}/restore")
async def admin_restore_backup(name: str, session: Optional[str] = Cookie(default=None)):
    require_admin(session)
    if not BACKUP_NAME_RE.fullmatch(name):
        raise HTTPException(400, "备份文件名不合法")
    if not (BACKUP_DIR / name).is_file():
        raise HTTPException(404, "备份不存在")
    try:
        pre = await asyncio.to_thread(_restore_backup, name)
    except ValueError as e:
        raise HTTPException(400, str(e))
    # Data was replaced wholesale: drop every in-memory cache (DB-backed
    # caches like holdings/nav/trends/industry came with the restore).
    _quote_cache.update(at=0.0, quotes={})
    _est_cache.clear()
    _series_cache.clear()
    for t in list(_est_tasks.values()) + list(_series_tasks.values()):
        t.cancel()
    _est_tasks.clear()
    _series_tasks.clear()
    return {"ok": True, "restored": name, "pre_restore_backup": pre["name"],
            "note": "已恢复；会话来自恢复后的数据库，如当前会话不在其中请重新登录"}


# ---------------------------------------------------------------- upstream
# ------------------------------------------------- upstream source stats (v1.4.6)
# In-memory per-interface success/failure tracking for the admin 数据接口
# page and the 数据流异常 banner. Every upstream_get call is classified by
# URL into one catalog entry; stats live in process memory only (a restart
# zeroes them — the page says so). Success = the response actually
# carries the data asked for (see _validate_upstream_payload): an
# HTTP 200 with an empty body / null data — Eastmoney's soft-throttle
# signature — counts as FAILURE, while legitimately-empty answers
# (search with no hits, pre-open trends, ...) stay success.
_SOURCE_CATALOG = [
    # key, name, provider, purpose, role（主源/兜底）
    ("em_search", "东财基金搜索", "东方财富",
     "基金搜索、基金类型（FTYPE，QDII/债基判定）", "主源"),
    ("em_nav", "东财官方净值 lsjz", "东方财富",
     "基金官方净值与涨跌幅（含 β 回测净值历史）", "主源"),
    ("em_holdings", "东财前十大持仓 jjcc", "东方财富",
     "基金最新披露前十大重仓股与报告期", "主源"),
    ("em_pingzhong", "东财 pingzhongdata", "东方财富",
     "股票仓位（未披露补齐）、持仓市场映射", "主源"),
    ("em_trends", "东财分时 trends2", "东方财富",
     "股票分钟分时（估值曲线）；push2his 主机、push2delay 备用主机", "主源"),
    ("em_ulist", "东财批量行情 ulist", "东方财富",
     "日/韩/台股持仓批量行情（新浪不覆盖）", "主源"),
    ("em_f10", "东财 F10 行业", "东方财富",
     "A股持仓行业（板块识别，证监会行业）", "主源"),
    ("em_kline", "东财股票日线", "东方财富",
     "股票日线收盘价（β 校准/可信度回测）", "主源"),
    ("em_rank", "东财排行榜 rankhandler", "东方财富",
     "全市场基金涨跌数据（涨幅榜/波动率榜池子）", "主源"),
    ("em_suggest", "东财代码联想", "东方财富",
     "指数与日/韩/台股代码解析", "主源"),
    ("em_jbgk", "东财 F10 基金概况", "东方财富",
     "联接基金跟踪标的、互认基金 ISIN 查询", "主源"),
    ("sina", "新浪行情", "新浪",
     "A/港/美股持仓实时行情（含盘前盘后，估值核心）", "主源"),
    ("yahoo_nav", "Yahoo 净值", "Yahoo Finance",
     "香港互认基金净值（东财无此数据）", "主源"),
    ("yahoo_trends", "Yahoo 分时", "Yahoo Finance",
     "股票 1 分钟分时（东财双主机被限流时接管曲线）", "兜底"),
    ("tencent", "腾讯分时", "腾讯",
     "股票分钟分时（东财限流时兜底，A/港股）", "兜底"),
    ("ipapi", "IP 归属地", "ip-api.com",
     "登录 IP 省市（管理页用户列表）", "主源"),
    ("fuyao_nav", "同花顺净值", "同花顺",
     "基金官方净值（东财 lsjz 失败时兜底，涨跌幅按相邻净值自算）", "兜底"),
    ("fuyao_holdings", "同花顺持仓", "同花顺",
     "基金前十大持仓（东财 jjcc 失败时兜底）", "兜底"),
    ("fuyao_asset", "同花顺资产配置", "同花顺",
     "股票仓位（东财 pingzhongdata 失败时兜底）", "兜底"),
    ("fuyao_kline", "同花顺日线", "同花顺",
     "A股日线收盘价（东财日线失败时兜底，仅A股）", "兜底"),
    ("fuyao_snapshot", "同花顺A股快照", "同花顺",
     "A股实时行情（新浪失败时兜底，仅A股）", "兜底"),
    ("fuyao_industry", "同花顺行业配置", "同花顺",
     "基金行业配置（东财 F10 行业失败时兜底，基金层面口径）", "兜底"),
]
_SOURCE_KEYS = [c[0] for c in _SOURCE_CATALOG]
# Data-type groups for the 数据接口 page (v1.4.7): each group shows its
# primary + fallback sources together so missing fallbacks are visible.
_SOURCE_GROUPS = [
    ("基金净值", ["em_nav", "fuyao_nav", "yahoo_nav"]),
    ("基金持仓", ["em_holdings", "fuyao_holdings"]),
    ("股票仓位", ["em_pingzhong", "fuyao_asset"]),
    ("分时曲线", ["em_trends", "yahoo_trends", "tencent"]),
    ("股票行情", ["sina", "em_ulist", "fuyao_snapshot"]),
    ("股票日线", ["em_kline", "fuyao_kline"]),
    ("基金搜索", ["em_search"]),
    ("排行榜", ["em_rank"]),
    ("行业板块", ["em_f10", "fuyao_industry"]),
    ("其他", ["em_suggest", "em_jbgk", "ipapi"]),
]
# Core data paths for the 数据流异常判定： live quotes + official NAV +
# holdings + intraday trends. Fallback sources (Yahoo/Tencent) don't count
# here — the banner means "every primary path is blocked".
_CRITICAL_SOURCES = ("sina", "em_nav", "em_holdings", "em_trends", "em_ulist")
_FLOW_WINDOW = 600          # seconds, data-flow judgement window
_FLOW_MIN_FAILS = 5         # failures in window (with zero successes) -> abnormal
_FLOW_CONSEC_FAILS = 3      # consecutive failures on one critical source
_source_stats: dict = {}    # key -> {ok, fail, last_ok, last_fail, last_error, consec_fail, events}


def _classify_source(url: str) -> str:
    u = url or ""
    if "fundsuggest.eastmoney.com" in u:
        return "em_search"
    if "api.fund.eastmoney.com/f10/lsjz" in u:
        return "em_nav"
    if "FundArchivesDatas" in u:
        return "em_holdings"
    if "pingzhongdata" in u:
        return "em_pingzhong"
    if "trends2/get" in u:
        return "em_trends"
    if "ulist.np/get" in u:
        return "em_ulist"
    if "CompanySurvey" in u:
        return "em_f10"
    if "kline/get" in u:
        return "em_kline"
    if "rankhandler" in u:
        return "em_rank"
    if "searchapi.eastmoney.com" in u:
        return "em_suggest"
    if "jbgk_" in u:
        return "em_jbgk"
    if "hq.sinajs.cn" in u:
        return "sina"
    if "finance.yahoo.com" in u:
        return "yahoo_trends" if "interval=1m" in u else "yahoo_nav"
    if "ifzq.gtimg.cn" in u:
        return "tencent"
    if "ip-api.com" in u:
        return "ipapi"
    if "fuyao.aicubes.cn" in u:
        if "performance/nav" in u:
            return "fuyao_nav"
        if "portfolio/holdings" in u:
            return "fuyao_holdings"
        if "portfolio/asset-allocation" in u:
            return "fuyao_asset"
        if "prices/historical" in u:
            return "fuyao_kline"
        if "prices/snapshot" in u:
            return "fuyao_snapshot"
        if "portfolio/industry-allocation" in u:
            return "fuyao_industry"
        return "other"
    return "other"


def _record_source(url: str, ok: bool, err: Optional[str] = None):
    """Record one upstream call outcome. Never raises."""
    try:
        key = _classify_source(url)
        now = time.time()
        st = _source_stats.setdefault(key, {
            "ok": 0, "fail": 0, "last_ok": None, "last_fail": None,
            "last_error": None, "consec_fail": 0, "events": []})
        if ok:
            st["ok"] += 1
            st["last_ok"] = now
            st["consec_fail"] = 0
        else:
            st["fail"] += 1
            st["last_fail"] = now
            st["consec_fail"] += 1
            st["last_error"] = (err or "")[:300] or None
        st["events"].append((now, ok))
        # keep the event log bounded to roughly the judgement window
        cutoff = now - _FLOW_WINDOW - 60
        while st["events"] and st["events"][0][0] < cutoff:
            st["events"].pop(0)
    except Exception:
        pass


_EMPTY_PAYLOAD_ERR = "200 空数据（疑似软限流）"
_BAD_JSON_ERR = "200 非 JSON 响应（疑似软限流）"
_JSON_INVALID = object()


def _resp_json(resp):
    try:
        return resp.json()
    except Exception:
        return _JSON_INVALID


def _cn_market_open(now: datetime) -> bool:
    """Weekday 09:30-16:30 Asia/Shanghai: A/HK (and JP/KR/TW) sessions
    have started, so an empty intraday payload is suspicious rather
    than pre-open."""
    mins = now.hour * 60 + now.minute
    return now.weekday() < 5 and 9 * 60 + 30 <= mins <= 16 * 60 + 30


def _validate_upstream_payload(url: str, resp, now: Optional[datetime] = None):
    """(ok, err) for one HTTP-200 upstream response, judged per interface.

    Success means the response carries the data the caller asked for.
    Eastmoney's soft throttling answers 200 with an empty body or null
    data — that is a failure here even though HTTP succeeded.
    Legitimately-empty answers stay success: fund search / code suggest
    with no hits, lsjz pages past the end of NAV history, intraday
    trends outside the relevant market session (pre-open), F10 pages
    whose industry field is simply absent, jjcc pages of funds with no
    stock holdings (the disclosure page itself is the data).
    `now` is Asia/Shanghai-aware and injectable for tests. Never raises:
    on any validator error the old rule applies (non-empty = success),
    so statistics can never break a data call.
    """
    try:
        key = _classify_source(url)
        now = now or datetime.now(CN_TZ)
        body = resp.content or b""
        if not body.strip():
            return False, _EMPTY_PAYLOAD_ERR
        text = resp.text or ""
        if key == "em_search":
            data = _resp_json(resp)
            if data is _JSON_INVALID:
                return False, _BAD_JSON_ERR
            if isinstance(data, dict) and isinstance(data.get("Datas"), list):
                return True, None  # empty list = legitimate no-hit search
            return False, _EMPTY_PAYLOAD_ERR
        if key == "em_nav":
            data = _resp_json(resp)
            if data is _JSON_INVALID:
                return False, _BAD_JSON_ERR
            items = ((data or {}).get("Data") or {}).get("LSJZList") \
                if isinstance(data, dict) else None
            if isinstance(items, list) and items:
                return True, None
            page = parse_qs(urlparse(url).query).get("pageIndex", ["1"])[0]
            if isinstance(items, list) and str(page) != "1":
                return True, None  # past the end of NAV history
            return False, _EMPTY_PAYLOAD_ERR
        if key == "em_holdings":
            m = re.search(r'content:"(.*)"\s*[,}]', text, re.S)
            if (m and m.group(1).strip()) or "<table" in text:
                return True, None  # disclosure page answered (rows may be 0)
            return False, _EMPTY_PAYLOAD_ERR
        if key == "em_pingzhong":
            if any(s in text for s in ("Data_assetAllocation", "stockCodesNew",
                                       "Data_netWorthTrend", "Data_grandTotal")):
                return True, None
            return False, _EMPTY_PAYLOAD_ERR
        if key == "em_trends":
            data = _resp_json(resp)
            if data is _JSON_INVALID or not isinstance(data, dict):
                return False, _BAD_JSON_ERR
            node = data.get("data")
            trends = node.get("trends") if isinstance(node, dict) else None
            if isinstance(trends, list) and trends:
                return True, None
            secid = parse_qs(urlparse(url).query).get("secid", [""])[0]
            if secid.split(".", 1)[0] in ("105", "106", "107"):
                if us_session(now.astimezone(ET_TZ)) == "trading":
                    return False, _EMPTY_PAYLOAD_ERR
            elif _cn_market_open(now):
                return False, _EMPTY_PAYLOAD_ERR
            return True, None  # pre-open / off-session: empty is normal
        if key == "em_ulist":
            data = _resp_json(resp)
            if data is _JSON_INVALID:
                return False, _BAD_JSON_ERR
            diff = ((data or {}).get("data") or {}).get("diff") \
                if isinstance(data, dict) else None
            if isinstance(diff, list) and diff:
                return True, None
            return False, _EMPTY_PAYLOAD_ERR
        if key == "em_f10":
            data = _resp_json(resp)
            if data is _JSON_INVALID:
                return False, _BAD_JSON_ERR
            if isinstance(data, dict) and "jbzl" in data:
                return True, None  # industry field may legitimately be absent
            return False, _EMPTY_PAYLOAD_ERR
        if key == "em_kline":
            data = _resp_json(resp)
            if data is _JSON_INVALID:
                return False, _BAD_JSON_ERR
            klines = ((data or {}).get("data") or {}).get("klines") \
                if isinstance(data, dict) else None
            if isinstance(klines, list) and klines:
                return True, None
            return False, _EMPTY_PAYLOAD_ERR
        if key == "em_rank":
            if "allRecords" in text:
                return True, None
            return False, _EMPTY_PAYLOAD_ERR
        if key == "em_suggest":
            data = _resp_json(resp)
            if data is _JSON_INVALID:
                return False, _BAD_JSON_ERR
            if isinstance(data, dict) and "QuotationCodeTable" in data:
                return True, None  # Data may be empty = no match
            return False, _EMPTY_PAYLOAD_ERR
        if key == "sina":
            if any(s.strip() for s in re.findall(r'hq_str_\w+="([^"]*)"', text)):
                return True, None
            return False, _EMPTY_PAYLOAD_ERR
        if key == "yahoo_nav":
            data = _resp_json(resp)
            if data is _JSON_INVALID:
                return False, _BAD_JSON_ERR
            if "/v1/finance/search" in url:
                if isinstance(data, dict) and isinstance(data.get("quotes"), list):
                    return True, None  # empty list = legitimate no match
                return False, _EMPTY_PAYLOAD_ERR
            chart = (data or {}).get("chart") if isinstance(data, dict) else None
            result = chart.get("result") if isinstance(chart, dict) else None
            if isinstance(result, list) and result:
                return True, None
            return False, _EMPTY_PAYLOAD_ERR
        if key == "yahoo_trends":
            data = _resp_json(resp)
            if data is _JSON_INVALID or not isinstance(data, dict):
                return False, _BAD_JSON_ERR
            chart = data.get("chart")
            if not isinstance(chart, dict):
                return False, _EMPTY_PAYLOAD_ERR
            result = chart.get("result")
            if isinstance(result, list) and result and (result[0].get("timestamp") or []):
                return True, None
            if urlparse(url).path.endswith((".SS", ".SZ", ".HK")):
                if _cn_market_open(now):
                    return False, _EMPTY_PAYLOAD_ERR
            elif us_session(now.astimezone(ET_TZ)) == "trading":
                return False, _EMPTY_PAYLOAD_ERR
            return True, None  # off-session: no points yet is normal
        if key == "tencent":
            data = _resp_json(resp)
            if data is _JSON_INVALID:
                return False, _BAD_JSON_ERR
            node = (data or {}).get("data") if isinstance(data, dict) else None
            code = parse_qs(urlparse(url).query).get("code", [""])[0]
            if isinstance(node, dict) and isinstance(node.get(code), dict):
                return True, None  # rows<10 (US placeholder) judged by caller
            return False, _EMPTY_PAYLOAD_ERR
        if key in ("fuyao_nav", "fuyao_holdings", "fuyao_asset",
                   "fuyao_kline", "fuyao_snapshot", "fuyao_industry"):
            data = _resp_json(resp)
            if data is _JSON_INVALID or not isinstance(data, dict):
                return False, _BAD_JSON_ERR
            if data.get("code") != 0:
                return False, f"fuyao code={data.get('code')} {data.get('message') or ''}".strip()
            node = data.get("data")
            if not isinstance(node, dict):
                return False, _EMPTY_PAYLOAD_ERR
            items = node.get("item")
            if key == "fuyao_holdings":
                # item may legitimately be [] (fund holds no stocks)
                if isinstance(items, list):
                    return True, None
                return False, _EMPTY_PAYLOAD_ERR
            if isinstance(items, list) and items:
                return True, None
            return False, _EMPTY_PAYLOAD_ERR
        # em_jbgk / ipapi / other: the page may legitimately lack the
        # field being looked for; any non-empty body means it answered.
        return True, None
    except Exception:
        return True, None


def _data_flow_ok() -> bool:
    """False only when every critical data path looks blocked: within the
    last 10 minutes the critical sources produced failures and zero
    successes (>=5 failures total, or >=3 failures with one critical
    source failing >=3 times in a row). A single success anywhere in the
    window means data is flowing. No calls yet (fresh start) -> True."""
    now = time.time()
    fails = successes = 0
    consec_bad = False
    for key in _CRITICAL_SOURCES:
        st = _source_stats.get(key)
        if not st:
            continue
        if st["consec_fail"] >= _FLOW_CONSEC_FAILS:
            consec_bad = True
        for ts, ok in st["events"]:
            if now - ts <= _FLOW_WINDOW:
                if ok:
                    successes += 1
                else:
                    fails += 1
    if successes > 0:
        return True
    if fails >= _FLOW_MIN_FAILS:
        return False
    if consec_bad and fails >= _FLOW_CONSEC_FAILS:
        return False
    return True


async def upstream_get(url: str, referer: Optional[str] = None, timeout: float = 10,
                       headers: Optional[dict] = None) -> httpx.Response:
    hdrs = {"User-Agent": UA}
    if referer:
        hdrs["Referer"] = referer
    if headers:
        hdrs.update(headers)
    try:
        async with httpx.AsyncClient(timeout=timeout, follow_redirects=True) as client:
            resp = await client.get(url, headers=hdrs)
            resp.raise_for_status()
    except Exception as e:
        _record_source(url, False, f"{type(e).__name__}: {e}")
        # Single choke point for upstream failures -> admin 系统状态
        # error log (deduped inside log_error, so expected fallbacks
        # like trends2 throttling don't flood it).
        log_error("upstream", f"{url.split('?')[0]} -> {type(e).__name__}: {e}")
        raise
    # HTTP succeeded; the payload still has to carry data. A 200 with an
    # empty/throttled payload records as failure for the 数据接口 page and
    # the 数据流异常 judgement, but the response is returned unchanged —
    # callers already handle empty payloads (fallback chains unchanged).
    ok, err = _validate_upstream_payload(url, resp)
    _record_source(url, ok, err)
    if not ok:
        log_error("upstream", f"{url.split('?')[0]} -> {err}")
    return resp


def _is_fund_entry(d: dict) -> bool:
    """FundSearchAPI also returns stocks; funds have CATEGORY 700, Hong Kong
    MRF funds （互认基金， e.g. 968061) have CATEGORY 780 (verified 2026-11-23)."""
    cat = d.get("CATEGORY")
    if cat is not None:
        return str(cat) in ("700", "780")
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


async def fetch_fund_type(code: str) -> Optional[str]:
    """Fund type string (e.g. "QDII-混合偏股", "混合型-偏股", "债券型-长债")
    from Eastmoney FundSearchAPI FundBaseInfo.FTYPE, DB-cached 30d.
    Returns None when unknown (upstream failure and no cache)."""
    with db() as conn:
        row = conn.execute("SELECT * FROM fund_type_cache WHERE fund_code=?", (code,)).fetchone()
    if row and time.time() - row["fetched_at"] < FUND_TYPE_TTL:
        return row["ftype"]
    try:
        from urllib.parse import quote
        resp = await upstream_get(
            f"https://fundsuggest.eastmoney.com/FundSearch/api/FundSearchAPI.ashx?m=1&key={quote(code)}")
        ftype = None
        for d in resp.json().get("Datas", []):
            if d.get("CODE") == code and _is_fund_entry(d):
                ftype = (d.get("FundBaseInfo") or {}).get("FTYPE")
                if not ftype and str(d.get("CATEGORY")) == "780":
                    ftype = "香港互认基金"  # MRF: FundBaseInfo is null upstream
                break
        if ftype:
            with db() as conn:
                conn.execute(
                    "INSERT INTO fund_type_cache(fund_code, ftype, fetched_at) VALUES(?,?,?) "
                    "ON CONFLICT(fund_code) DO UPDATE SET ftype=excluded.ftype, fetched_at=excluded.fetched_at",
                    (code, ftype, time.time()))
            return ftype
    except Exception:
        pass
    return row["ftype"] if row else None


def _nav_from_row(row) -> dict:
    keys = row.keys()
    return {
        "nav_date": row["nav_date"], "dwjz": row["dwjz"], "jzzzl": row["jzzzl"],
        "prev_nav_date": row["prev_nav_date"] if "prev_nav_date" in keys else None,
        "prev_dwjz": row["prev_dwjz"] if "prev_dwjz" in keys else None,
        "prev_jzzzl": row["prev_jzzzl"] if "prev_jzzzl" in keys else None,
    }


# Hong Kong MRF funds （互认基金， 968/969xxx): Eastmoney lsjz returns an
# empty list for them, so NAV comes from Yahoo Finance daily closes
# (verified 2026-11-23: 968061 ISIN HK0000555752 -> 0P0001IVWZ.HK,
# 2026-11-19 close 18.38 CNH, matching other apps). Yahoo search by fund
# code returns nothing and by Chinese name HTTP 400; search by ISIN is
# exact. ISIN sources tried in order: Eastmoney F10 jbgk page, cifm fund
# page (neither exposed an ISIN when checked 2026-11-23 — kept as cheap
# future-proof attempts), then the seed map below (from JPM factsheets).
# Resolution result cached in mrf_symbol_map: success permanently,
# failure 1 day.
MRF_ISIN_SEED = {"968061": "HK0000555752", "968062": "HK0000555760",
                 "968063": "HK0000555778"}
MRF_YAHOO_SEED = {"968061": "0P0001IVWZ.HK", "968062": "0P0001IVX0.HK"}
MRF_FAIL_TTL = 86400  # seconds
_ISIN_RE = re.compile(r"HK\d{10}")


def _is_mrf_code(code: str) -> bool:
    return code[:3] in ("968", "969")


async def _yahoo_search_symbol(query: str, require_hk: bool = True) -> Optional[str]:
    """First Yahoo MUTUALFUND quote for the query. Code/name searches
    require a .HK symbol (Hong Kong listing); ISIN searches are exact
    (unique identifier) and accept any listing — e.g. 968063's ISIN
    resolves to 0P0001IVX1 on OTC Markets without a .HK suffix."""
    if not query:
        return None
    try:
        from urllib.parse import quote
        resp = await upstream_get(
            f"https://query2.finance.yahoo.com/v1/finance/search?q={quote(query)}&quotesCount=10",
            timeout=15)
        for x in resp.json().get("quotes", []):
            sym = x.get("symbol") or ""
            if x.get("quoteType") == "MUTUALFUND" and (sym.endswith(".HK") or not require_hk):
                return sym
    except Exception:
        pass
    return None


async def _fetch_mrf_isin(code: str) -> Optional[str]:
    """Best-effort ISIN lookup for an MRF fund code: Eastmoney F10 jbgk
    page, then the cifm fund page. Both are server-rendered but carried
    no ISIN when checked 2026-11-23; kept so resolution starts working
    if either page adds one."""
    for url, referer in (
        (f"https://fundf10.eastmoney.com/jbgk_{code}.html",
         "https://fundf10.eastmoney.com/"),
        (f"https://www.cifm.com/fund/{code}/", None),
    ):
        try:
            resp = await upstream_get(url, referer=referer, timeout=10)
            m = _ISIN_RE.search(resp.text)
            if m:
                return m.group(0)
        except Exception:
            pass
    return None


async def resolve_mrf_symbol(code: str, name: str = "") -> Optional[str]:
    with db() as conn:
        row = conn.execute(
            "SELECT isin, yahoo_symbol, resolved_at FROM mrf_symbol_map WHERE fund_code=?",
            (code,)).fetchone()
    if row:
        if row["yahoo_symbol"]:
            return row["yahoo_symbol"]
        if time.time() - row["resolved_at"] < MRF_FAIL_TTL:
            return None
    isin = (row["isin"] if row else None) or await _fetch_mrf_isin(code) \
        or MRF_ISIN_SEED.get(code)
    symbol = None
    if isin:
        symbol = await _yahoo_search_symbol(isin, require_hk=False)
    if not symbol:
        symbol = MRF_YAHOO_SEED.get(code)
    if not symbol:
        symbol = await _yahoo_search_symbol(code)
    if not symbol and name:
        symbol = await _yahoo_search_symbol(name)
    with db() as conn:
        conn.execute(
            "INSERT INTO mrf_symbol_map(fund_code, isin, yahoo_symbol, resolved_at) VALUES(?,?,?,?) "
            "ON CONFLICT(fund_code) DO UPDATE SET isin=excluded.isin, "
            "yahoo_symbol=excluded.yahoo_symbol, resolved_at=excluded.resolved_at",
            (code, isin, symbol, time.time()))
    return symbol


async def fetch_mrf_nav(code: str, name: str = "") -> dict:
    """NAV for an MRF fund from Yahoo daily closes. Same return shape and
    nav_cache (TTL 600s) as fetch_nav. Raises HTTPException(502) when no
    source works; the caller renders empty NAV fields then."""
    with db() as conn:
        row = conn.execute("SELECT * FROM nav_cache WHERE fund_code=?", (code,)).fetchone()
    if row and time.time() - row["fetched_at"] < NAV_TTL and _nav_from_row(row)["prev_dwjz"] is not None:
        return {**_nav_from_row(row), "stale": False}
    try:
        symbol = await resolve_mrf_symbol(code, name)
        if not symbol:
            raise ValueError("no Yahoo symbol")
        resp = await upstream_get(
            f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}?range=1mo&interval=1d",
            timeout=15)
        result = resp.json()["chart"]["result"][0]
        closes = result["indicators"]["quote"][0]["close"]
        pts = [(datetime.fromtimestamp(t, CN_TZ).strftime("%Y-%m-%d"), float(c))
               for t, c in zip(result["timestamp"], closes) if c is not None]
        if len(pts) < 2:
            raise ValueError("not enough NAV points")
        (prev_date, prev_close), (nav_date, close) = pts[-2], pts[-1]
        prev_jzzzl = (round((prev_close - pts[-3][1]) / pts[-3][1] * 100, 2)
                      if len(pts) >= 3 and pts[-3][1] else None)
        nav = {
            "nav_date": nav_date, "dwjz": round(close, 4),
            "jzzzl": round((close - prev_close) / prev_close * 100, 2) if prev_close else 0.0,
            "prev_nav_date": prev_date, "prev_dwjz": round(prev_close, 4),
            "prev_jzzzl": prev_jzzzl,
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


# ------------------------------------------------- fuyao fallback (v1.4.7)
# Tonghuashun official API as an independent fallback for NAV / holdings /
# stock position (v1.4.7) plus A-share daily closes / A-share snapshot /
# fund industry allocation (v1.4.8). Eastmoney stays primary; these run
# only when the
# Eastmoney call failed or returned no data, and only when FUYAO_API_KEY
# is configured. All calls go through upstream_get (stats + payload
# validation); the key travels in the X-api-key header, never in URLs.
async def _fuyao_get(path: str) -> dict:
    resp = await upstream_get(FUYAO_BASE + path,
                              headers={"X-api-key": FUYAO_API_KEY})
    data = resp.json()
    if not isinstance(data, dict) or data.get("code") != 0:
        raise ValueError(f"fuyao error: {(data or {}).get('message')}")
    return data.get("data") or {}


def _fuyao_date(ms) -> Optional[str]:
    try:
        return datetime.fromtimestamp(float(ms) / 1000, tz=CN_TZ).strftime("%Y-%m-%d")
    except Exception:
        return None


async def fetch_nav_fuyao(code: str) -> Optional[dict]:
    """NAV dict (same shape as fetch_nav's lsjz result) from fuyao.
    Fuyao returns unit NAVs only (no change%), so jzzzl is computed from
    consecutive NAVs. None on any failure."""
    try:
        node = await _fuyao_get(
            f"/api/fund/performance/nav?thscode={code}.OF&range=month&nav_type=unit")
        items = [it for it in (node.get("item") or [])
                 if isinstance(it, dict) and it.get("unit_nav") is not None]
        items.sort(key=lambda it: it.get("nav_date") or 0)
        if not items:
            return None
        last = items[-1]
        prev = items[-2] if len(items) > 1 else None
        prev2 = items[-3] if len(items) > 2 else None
        dwjz = float(last["unit_nav"])
        prev_dwjz = float(prev["unit_nav"]) if prev else None
        prev2_dwjz = float(prev2["unit_nav"]) if prev2 else None

        def _chg(cur, base):
            return round((cur / base - 1) * 100, 2) if cur and base else 0.0

        return {
            "nav_date": _fuyao_date(last.get("nav_date")), "dwjz": dwjz,
            "jzzzl": _chg(dwjz, prev_dwjz),
            "prev_nav_date": _fuyao_date(prev.get("nav_date")) if prev else None,
            "prev_dwjz": prev_dwjz,
            "prev_jzzzl": _chg(prev_dwjz, prev2_dwjz) if prev else None,
        }
    except Exception:
        return None


async def fetch_nav_history_fuyao(code: str, days: int) -> list:
    """[(date, jzzzl)] newest first from fuyao (range=year), change%
    computed from consecutive unit NAVs. [] on failure."""
    try:
        node = await _fuyao_get(
            f"/api/fund/performance/nav?thscode={code}.OF&range=year&nav_type=unit")
        items = [it for it in (node.get("item") or [])
                 if isinstance(it, dict) and it.get("unit_nav") is not None]
        items.sort(key=lambda it: it.get("nav_date") or 0)
        rows = []
        for i in range(1, len(items)):
            cur, base = float(items[i]["unit_nav"]), float(items[i - 1]["unit_nav"])
            if base:
                rows.append((_fuyao_date(items[i].get("nav_date")),
                             round((cur / base - 1) * 100, 2)))
        return rows[::-1][:days]
    except Exception:
        return []


def _fuyao_holding_code(item: dict) -> Optional[str]:
    """Map a fuyao holding item to the internal holding-code format used
    by the jjcc parser (A: 6-digit, HK: 5-digit, US: ticker, others bare)."""
    ticker = str(item.get("ticker") or "").strip()
    thscode = str(item.get("thscode") or "").strip()
    if not ticker and "." in thscode:
        ticker = thscode.split(".", 1)[0]
    if not ticker:
        return None
    suffix = thscode.rsplit(".", 1)[-1].upper() if "." in thscode else ""
    if suffix == "HK" and ticker.isdigit():
        return ticker.zfill(5)
    if ticker.isalpha():
        return ticker.upper()  # US ticker
    return ticker


async def _fuyao_holdings_raw(code: str):
    """(parsed holdings dict, stock_ratio_pct|None) from fuyao, or
    (None, None) on failure. Parsed dict matches parse_holdings output."""
    try:
        node = await _fuyao_get(f"/api/fund/portfolio/holdings?thscode={code}.OF")
        items = [it for it in (node.get("item") or []) if isinstance(it, dict)]
        items.sort(key=lambda it: it.get("investment_rank")
                   if isinstance(it.get("investment_rank"), (int, float)) else 999)
        holdings = []
        for it in items:
            hcode = _fuyao_holding_code(it)
            try:
                weight = float(it.get("hold_ratio"))
            except (TypeError, ValueError):
                continue
            if hcode:
                holdings.append({"code": hcode,
                                 "name": str(it.get("stock_name") or "").strip(),
                                 "weight": weight})
        report_date = _fuyao_date(items[0].get("end_date_ms")) if items else None
        label = None
        if report_date:
            y, m = int(report_date[:4]), int(report_date[5:7])
            label = f"{y}年{(m - 1) // 3 + 1}季度"
        ratio = node.get("stock_ratio_pct")
        return ({"holdings": holdings[:10], "report_label": label,
                 "report_date": report_date},
                float(ratio) if isinstance(ratio, (int, float)) else None)
    except Exception:
        return None, None


async def fetch_stock_position_fuyao(code: str) -> Optional[float]:
    """Stock position % from fuyao: holdings endpoint's stock_ratio_pct
    first (same disclosure as the holdings), then asset-allocation."""
    _parsed, ratio = await _fuyao_holdings_raw(code)
    if ratio is not None:
        return ratio
    try:
        node = await _fuyao_get(f"/api/fund/portfolio/asset-allocation?thscode={code}.OF")
        items = [it for it in (node.get("item") or []) if isinstance(it, dict)]
        for it in items:
            v = it.get("stock_ratio_pct")
            if isinstance(v, (int, float)):
                return float(v)
    except Exception:
        pass
    return None


def _fuyao_thscode(secid: str) -> Optional[str]:
    """Fuyao thscode for an A-share Eastmoney secid ("1.600519" ->
    "600519.SH", "0.000001" -> "000001.SZ"). None for anything else
    (HK/US/JP/KR/TW, indices, BJ 4/8/92 codes): fuyao has no fallback
    for those markets."""
    prefix, _, code = (secid or "").partition(".")
    if not re.fullmatch(r"\d{6}", code):
        return None
    if prefix == "1":
        return f"{code}.SH"
    if prefix == "0" and not (code.startswith("92") or code[0] in "48"):
        return f"{code}.SZ"
    return None


async def fetch_daily_closes_fuyao(secid: str) -> dict:
    """{date: close} A-share daily closes from fuyao (v1.4.8 fallback
    for the Eastmoney kline API; same forward-adjusted basis, fqt=1).
    {} for non-A-share secids or on any failure."""
    thscode = _fuyao_thscode(secid)
    if not thscode:
        return {}
    try:
        end = datetime.now(CN_TZ)
        start = end - timedelta(days=240)
        node = await _fuyao_get(
            f"/api/a-share/prices/historical?thscode={thscode}&interval=1d"
            f"&adjust=forward&start={int(start.timestamp() * 1000)}"
            f"&end={int(end.timestamp() * 1000)}")
        out = {}
        for it in node.get("item") or []:
            if not isinstance(it, dict):
                continue
            d = _fuyao_date(it.get("date_ms"))
            try:
                c = float(it.get("close_price"))
            except (TypeError, ValueError):
                continue
            if d and c > 0:
                out[d] = c
        return out
    except Exception:
        return {}


async def fetch_snapshot_fuyao(symbols: list[str]) -> dict:
    """A-share quotes from fuyao snapshot (v1.4.8 fallback when Sina
    produced nothing for these symbols). Takes Sina symbols (sh/sz +
    6 digits), returns {sina_symbol: quote} in the Sina quote shape.
    Fuyao items carry no stock name; name stays None (callers use the
    holding's own name). {} on failure."""
    ths_of = {}
    for s in symbols:
        m = re.fullmatch(r"(sh|sz)(\d{6})", s or "")
        if m:
            ths_of[f"{m.group(2)}.{'SH' if m.group(1) == 'sh' else 'SZ'}"] = s
    if not ths_of:
        return {}
    try:
        node = await _fuyao_get(
            "/api/a-share/prices/snapshot?thscodes=" + ",".join(ths_of))
        qtime = datetime.now(CN_TZ).strftime("%Y-%m-%d %H:%M:%S")
        out = {}
        for it in node.get("item") or []:
            if not isinstance(it, dict):
                continue
            sym = ths_of.get(str(it.get("thscode") or ""))
            if not sym:
                continue
            try:
                cur = float(it.get("last_price"))
                prev = float(it.get("prev_price"))
            except (TypeError, ValueError):
                continue
            if cur <= 0 and prev > 0:
                cur = prev  # pre-open: treat as prev close (0 change)
            if prev > 0 and cur > 0:
                chg = it.get("price_change_ratio_pct")
                out[sym] = {"name": None, "prev_close": prev, "current": cur,
                            "change_pct": float(chg) if isinstance(chg, (int, float))
                            else (cur - prev) / prev * 100,
                            "quote_time": qtime}
        return out
    except Exception:
        return {}


# Fuyao fund industry allocation is a quarterly disclosure; cache the
# derived sector in memory (success 1 day, failure 10 minutes).
_fuyao_ind_cache: dict = {}
_FUYAO_IND_TTL = 86400
_FUYAO_IND_FAIL_TTL = 600


async def fetch_industry_allocation_fuyao(code: str) -> Optional[dict]:
    """Sector dict from fuyao fund industry allocation (v1.4.8 fallback
    when per-stock F10 industries resolve nothing). Fund-level 口径：
    the largest industry of the newest report period that carries a
    non-zero ratio. share_pct is fuyao's ratio_pct (% of fund assets,
    A-share holdings only per fuyao); change_pct stays None — there is
    no per-stock mapping to weight a live change from. None on failure."""
    now = time.time()
    hit = _fuyao_ind_cache.get(code)
    if hit:
        ts, sector = hit
        if now - ts < (_FUYAO_IND_TTL if sector else _FUYAO_IND_FAIL_TTL):
            return sector
    sector = None
    try:
        node = await _fuyao_get(
            f"/api/fund/portfolio/industry-allocation?thscode={code}.OF")
        by_period: dict = {}
        for it in node.get("item") or []:
            if not isinstance(it, dict):
                continue
            try:
                ratio = float(it.get("ratio_pct"))
            except (TypeError, ValueError):
                continue
            name = str(it.get("industry_name") or "").strip()
            period = str(it.get("report_period") or "")
            if name and ratio > 0:
                by_period.setdefault(period, []).append((ratio, name))
        for period in sorted(by_period, reverse=True):
            ratio, name = max(by_period[period])
            sector = {"name": _short_industry(name),
                      "share_pct": round(ratio, 1), "change_pct": None}
            break
    except Exception:
        sector = None
    _fuyao_ind_cache[code] = (now, sector)
    return sector


async def fetch_nav(code: str, name: str = "") -> dict:
    if _is_mrf_code(code):  # MRF: Eastmoney lsjz is empty, use Yahoo
        return await fetch_mrf_nav(code, name)
    with db() as conn:
        row = conn.execute("SELECT * FROM nav_cache WHERE fund_code=?", (code,)).fetchone()
    # Cache rows from before v1.1.0 have NULL prev_* (migrated columns); treat as stale
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
        # Eastmoney lsjz failed/empty -> fuyao fallback (v1.4.7), cached
        # the same way so the rest of the system can't tell the source.
        if FUYAO_ENABLED:
            nav = await fetch_nav_fuyao(code)
            if nav and nav["dwjz"]:
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
        # Eastmoney jjcc failed/empty -> fuyao fallback (v1.4.7). Only a
        # non-empty result is cached (mirrors the jjcc empty check).
        if FUYAO_ENABLED:
            parsed, _ratio = await _fuyao_holdings_raw(code)
            if parsed and parsed["holdings"]:
                with db() as conn:
                    conn.execute(
                        "INSERT INTO holdings_cache(fund_code, holdings_json, report_label, report_date, fetched_at) VALUES(?,?,?,?,?) "
                        "ON CONFLICT(fund_code) DO UPDATE SET holdings_json=excluded.holdings_json, report_label=excluded.report_label, report_date=excluded.report_date, fetched_at=excluded.fetched_at",
                        (code, json.dumps(parsed["holdings"], ensure_ascii=False), parsed["report_label"], parsed["report_date"], time.time()),
                    )
                return {**parsed, "stale": False}
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


INDUSTRY_TTL = 30 * 86400        # seconds, DB cache for a resolved industry
INDUSTRY_FAIL_TTL = 600          # seconds, retry delay after a failed lookup
INDUSTRY_REF = "https://emweb.securities.eastmoney.com/"

# F10 throttles/closes connections under parallel bursts (observed
# 2026-11-21: live server sectors all null after a burst failed and got
# cached). Serialize to 2 in flight with ~0.3s spacing between request
# starts; retries back off outside the semaphore.
_f10_sem = asyncio.Semaphore(2)
_f10_lock = asyncio.Lock()
_f10_last_start = 0.0


async def _f10_throttle() -> None:
    global _f10_last_start
    async with _f10_lock:
        wait = _f10_last_start + 0.3 - time.monotonic()
        if wait > 0:
            await asyncio.sleep(wait)
        _f10_last_start = time.monotonic()

# Manual sector table for overseas holdings (F10 has no US/HK industry
# lookup). Keys: US tickers (uppercase) and HK 5-digit codes. Extend as
# new names show up in watchlists; unknown codes simply get no sector.
MANUAL_SECTOR = {
    # US semiconductors / hardware
    "NVDA": "半导体", "AMD": "半导体", "INTC": "半导体", "MU": "半导体",
    "TSM": "半导体", "AVGO": "半导体", "MRVL": "半导体", "QCOM": "半导体",
    "TXN": "半导体", "ADI": "半导体", "ONTO": "半导体", "SKHY": "半导体",
    "ASML": "半导体", "STX": "半导体", "WDC": "半导体", "AMAT": "半导体",
    "LRCX": "半导体", "KLAC": "半导体",
    "LITE": "光通信", "COHR": "光通信",
    "AAPL": "消费电子",
    "MSFT": "软件",
    "GOOGL": "互联网", "GOOG": "互联网", "META": "互联网",
    "AMZN": "互联网", "NFLX": "互联网",
    "TSLA": "汽车",
    "LLY": "医药", "UNH": "医药", "JNJ": "医药",
    "JPM": "金融", "V": "金融", "MA": "金融",
    "XOM": "能源", "CVX": "能源",
    # HK
    "00700": "互联网", "09988": "互联网", "03690": "互联网", "09999": "互联网",
    "01810": "消费电子",
    "09868": "汽车", "02015": "汽车", "01211": "汽车",
    "02318": "金融", "01398": "金融", "00939": "金融", "00388": "金融",
    "00941": "通信",
    "01876": "消费", "02020": "消费", "09992": "消费",
    "02476": "半导体",
}

# Short display names for CSRC level-2 industries (F10 INDUSTRYCSRC1 part
# after "-"). Full names are too long for the fund-card meta line and push
# the sector change % off-screen (user screenshot 2026-11-21). Grouping
# stays by full name; only the displayed sector name is shortened.
INDUSTRY_SHORT = {
    "计算机、通信和其他电子设备制造业": "电子设备",
    "电气机械和器材制造业": "电气机械",
    "专用设备制造业": "专用设备",
    "通用设备制造业": "通用设备",
    "汽车制造业": "汽车",
    "医药制造业": "医药",
    "化学原料和化学制品制造业": "化工",
    "化学纤维制造业": "化纤",
    "非金属矿物制品业": "非金属矿物",
    "黑色金属冶炼和压延加工业": "钢铁",
    "有色金属冶炼和压延加工业": "有色金属",
    "金属制品业": "金属制品",
    "酒、饮料和精制茶制造业": "酒饮料",
    "农副食品加工业": "农副食品",
    "食品制造业": "食品",
    "纺织服装、服饰业": "纺织服装",
    "纺织业": "纺织",
    "橡胶和塑料制品业": "橡塑",
    "软件和信息技术服务业": "软件信息",
    "互联网和相关服务": "互联网",
    "货币金融服务": "银行",
    "资本市场服务": "证券",
    "保险业": "保险",
    "房地产业": "房地产",
    "土木工程建筑业": "建筑",
    "电力、热力生产和供应业": "电力",
    "农、林、牧、渔业": "农林牧渔",
    "石油和天然气开采业": "油气开采",
    "煤炭开采和洗选业": "煤炭",
    "有色金属矿采选业": "有色采选",
    "黑色金属矿采选业": "黑色采选",
    "仪器仪表制造业": "仪器仪表",
    "造纸和纸制品业": "造纸",
    "印刷和记录媒介复制业": "印刷",
    "家具制造业": "家具",
    "皮革、毛皮、羽毛及其制品和制鞋业": "皮革制鞋",
    "木材加工和木、竹、藤、棕、草制品业": "木材加工",
    "文教、工美、体育和娱乐用品制造业": "文体用品",
    "废弃资源综合利用业": "环保回收",
    "其他制造业": "其他制造",
    "铁路、船舶、航空航天和其他运输设备制造业": "运输设备",
    "水上运输业": "水运",
    "航空运输业": "航空",
    "道路运输业": "公路运输",
    "仓储业": "仓储",
    "批发业": "批发",
    "零售业": "零售",
    "住宿业": "住宿",
    "餐饮业": "餐饮",
    "电信、广播电视和卫星传输服务": "通信",
    "广播、电视、电影和影视录音制作业": "传媒影视",
    "新闻和出版业": "出版",
    "研究和试验发展": "研发",
    "专业技术服务业": "专业服务",
    "商务服务业": "商务服务",
    "公共设施管理业": "公共设施",
    "生态保护和环境治理业": "环保",
    "卫生": "卫生",
    "教育": "教育",
}


def _short_industry(name: str) -> str:
    """Short display name for a sector. Manual-table names (半导体 etc.)
    are already short and pass through; unmapped CSRC names longer than
    8 chars drop a trailing 制造业 (keeping at least 4 chars)."""
    short = INDUSTRY_SHORT.get(name)
    if short:
        return short
    if len(name) > 8 and name.endswith("制造业") and len(name) - 3 >= 4:
        return name[:-3]
    return name


async def fetch_industry(code: str) -> Optional[str]:
    """Sector/industry (shortened display name) for a holding code.
    Overseas codes (US tickers, HK 5-digit) resolve via MANUAL_SECTOR.
    A-share 6-digit codes use Eastmoney F10 CompanySurvey CSRC industry,
    DB-cached 30 days (failures cached 10 minutes). Returns None when unknown.
    Verified 2026-11-21: SH600519 -> 制造业-酒、饮料和精制茶制造业."""
    code = (code or "").strip()
    manual = MANUAL_SECTOR.get(code.upper()) or MANUAL_SECTOR.get(code)
    if manual:
        return manual
    if not re.fullmatch(r"\d{6}", code):
        return None
    with db() as conn:
        row = conn.execute(
            "SELECT industry, fetched_at FROM stock_industry WHERE code=?", (code,)
        ).fetchone()
    if row:
        age = time.time() - row["fetched_at"]
        if row["industry"] and age < INDUSTRY_TTL:
            return row["industry"]
        if not row["industry"] and age < INDUSTRY_FAIL_TTL:
            return None
    if code.startswith("92") or code[0] in "48":
        prefix = "BJ"
    elif code[0] in "69":
        prefix = "SH"
    else:
        prefix = "SZ"
    industry = None
    # F10 endpoint is flaky under load (~50% connection-closed observed
    # 2026-11-21, worse in parallel bursts): serialize via _f10_sem with
    # request spacing, retry up to 5 times with backoff before giving up.
    for attempt, backoff in enumerate((0.5, 1, 2, 3, 0)):
        try:
            async with _f10_sem:
                await _f10_throttle()
                resp = await upstream_get(
                    f"https://emweb.securities.eastmoney.com/PC_HSF10/CompanySurvey/PageAjax?code={prefix}{code}",
                    referer=INDUSTRY_REF, timeout=10)
            jbzl = resp.json().get("jbzl")
            if isinstance(jbzl, list):
                jbzl = jbzl[0] if jbzl else None
            raw = (jbzl or {}).get("INDUSTRYCSRC1") or ""
            raw = raw.strip()
            if raw:
                industry = raw.split("-", 1)[1].strip() if "-" in raw else raw
            break
        except Exception:
            industry = None
            if backoff:
                await asyncio.sleep(backoff)
    if industry:
        with db() as conn:
            conn.execute(
                "INSERT INTO stock_industry(code, industry, fetched_at) VALUES(?,?,?) "
                "ON CONFLICT(code) DO UPDATE SET industry=excluded.industry, fetched_at=excluded.fetched_at",
                (code, industry, time.time()))
        return industry
    # Failure: keep a stale resolved industry if we have one; otherwise
    # cache the failure so we don't hammer the endpoint.
    if row and row["industry"]:
        return row["industry"]
    with db() as conn:
        conn.execute(
            "INSERT INTO stock_industry(code, industry, fetched_at) VALUES(?,?,?) "
            "ON CONFLICT(code) DO UPDATE SET industry=excluded.industry, fetched_at=excluded.fetched_at",
            (code, "", time.time()))
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
    change_pct = float(fields[2]) if len(fields) > 2 and fields[2] else 0.0
    if cur <= 0 and prev > 0:
        # Pre-open Sina may return 0 as current; treat as prev close (0 change).
        cur = prev
        change_pct = 0.0
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
            "change_pct": change_pct,
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
                if secid and prev and cur is not None and cur <= 0:
                    # Pre-open current=0; treat as prev close (0 change).
                    cur, chg = prev, 0.0
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
    sina_failed = False
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
                    if cur <= 0 and prev > 0:
                        cur = prev  # pre-open current=0 -> prev close (0 change)
                    quotes[sym] = {"name": fields[1], "prev_close": prev, "current": cur,
                                   "change_pct": (cur - prev) / prev * 100 if prev else 0.0,
                                   "quote_time": f"{fields[17]} {fields[18]}" if len(fields) > 18 else None}
                else:
                    # A-share layout: 0=name,1=open,2=prev close,3=current,30=date,31=time
                    prev, cur = float(fields[2]), float(fields[3])
                    if cur <= 0 and prev > 0:
                        cur = prev  # pre-open current=0 -> prev close (0 change)
                    quotes[sym] = {"name": fields[0], "prev_close": prev, "current": cur,
                                   "change_pct": (cur - prev) / prev * 100 if prev else 0.0,
                                   "quote_time": f"{fields[30]} {fields[31]}" if len(fields) > 31 else None}
            except (ValueError, IndexError):
                continue
    except Exception:
        sina_failed = True
    # A-share symbols Sina produced nothing for (hard failure, or empty
    # lines under throttling) fall back to fuyao snapshot (v1.4.8). HK/US
    # have no fuyao coverage and keep whatever is cached.
    if FUYAO_ENABLED:
        missing_a = [s for s in sina_syms
                     if s not in quotes and re.fullmatch(r"(sh|sz)\d{6}", s)]
        if missing_a:
            quotes.update(await fetch_snapshot_fuyao(missing_a))
    if quotes or not sina_failed:
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
# v1.2.0). Verified 2026-11-20: data.preClose present; rows are
# "YYYY-MM-DD HH:MM,open,close,high,low,vol,amount,avg" in Beijing time.
TRENDS_TTL = 300            # seconds, DB cache for today's session (trading window)
TRENDS_TTL_OFF = 1800       # seconds, DB cache outside trading windows (v1.4.0)
TRENDS_FAIL_BACKOFF = 900   # seconds, per-secid upstream failure backoff (v1.4.0)


def _in_trading_window(now: Optional[datetime] = None) -> bool:
    """True during CN day session (weekdays 09:15-16:30 Asia/Shanghai) or
    the US session as seen from Beijing (21:00-05:00; the after-midnight
    part belongs to the previous weekday's US session, so Tue-Sat
    00:00-05:00 counts). Weekends outside that: False."""
    now = now or datetime.now(CN_TZ)
    mins = now.hour * 60 + now.minute
    wd = now.weekday()  # Mon=0 .. Sun=6
    if wd < 5 and 9 * 60 + 15 <= mins <= 16 * 60 + 30:
        return True
    if wd < 5 and mins >= 21 * 60:
        return True
    if 1 <= wd <= 5 and mins <= 5 * 60:
        return True
    return False


def _trends_ttl(now: Optional[datetime] = None) -> int:
    """Trends cache TTL: 300s in trading windows, 1800s outside (v1.4.0)."""
    return TRENDS_TTL if _in_trading_window(now) else TRENDS_TTL_OFF


def _warm_interval(now: Optional[datetime] = None) -> int:
    """Trends warm-loop sleep: 240s in trading windows, 900s outside."""
    return 240 if _in_trading_window(now) else 900
TRENDS_REF = "https://quote.eastmoney.com/"
# Eastmoney trends2 hosts tried in order (v1.4.4): push2his primary,
# push2delay alternate (same API/format incl. preClose, verified
# 2026-11-23) when the primary throttles the server IP. Env override
# for testing, e.g. TRENDS_HOSTS=push2delay.
TRENDS_HOSTS = [h.strip() for h in os.environ.get(
    "TRENDS_HOSTS", "push2his,push2delay").split(",") if h.strip()]
_trends_sem = asyncio.Semaphore(5)
_trends_fail_until: dict = {}   # secid -> ts until which upstream calls are skipped
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
# (ulist quotes + trends2 minute data, v1.2.0). Secid prefixes verified
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


def _tencent_code(secid: str) -> Optional[str]:
    """Tencent (gtimg) symbol for an Eastmoney secid, or None when Tencent
    has no equivalent (JP/KR/TW 176/177/178, CSI indices 2.)."""
    prefix, _, code = secid.partition(".")
    if prefix == "1":
        return "sh" + code
    if prefix == "0":
        return "sz" + code
    if prefix == "116":
        return "hk" + code
    if prefix in ("105", "106", "107"):
        return "us" + code.upper()
    return None


def _yahoo_symbol(secid: str) -> Optional[str]:
    """Yahoo Finance symbol for an Eastmoney secid, or None when Yahoo
    has no reliable equivalent (JP/KR/TW 176/177/178, CSI indices 2.,
    Beijing 4/8/92 under prefix 0.). US: bare ticker with Yahoo's dash
    convention (BRK.B -> BRK-B); HK: 4-digit padded + .HK (00700 ->
    0700.HK, 09988 -> 9988.HK); SH 1. -> .SS; SZ 0/3 under 0. -> .SZ."""
    prefix, _, code = secid.partition(".")
    if prefix in ("105", "106", "107"):
        return code.upper().replace(".", "-")
    if prefix == "116":
        return code.lstrip("0").zfill(4) + ".HK"
    if prefix == "1":
        return code + ".SS"
    if prefix == "0" and code[:1] in ("0", "3"):
        return code + ".SZ"
    return None


async def _fetch_tencent_trends(secid: str, date_str: str) -> Optional[dict]:
    """Minute closes from Tencent as an Eastmoney fallback (v1.4.0).
    Tencent's minute query only serves the current session, so callers
    must restrict this to date_str == today. Rows are "HHMM price vol amt"
    with no date; A/HK times are Beijing time, US times are ET (converted
    via ET_TZ, shifted back a day when they land in the future, i.e. the
    previous US session). pre_close comes from qt[4] (verified 2026-11-23
    against Sina prev_close for 600519/688137/301080). Payloads with
    fewer than 10 points are rejected (Tencent US returns a single
    placeholder row outside US hours)."""
    tcode = _tencent_code(secid)
    if not tcode:
        return None
    resp = await upstream_get(
        f"https://web.ifzq.gtimg.cn/appstock/app/minute/query?code={tcode}",
        referer="https://gu.qq.com/", timeout=10)
    node = (resp.json().get("data") or {}).get(tcode) or {}
    rows = ((node.get("data") or {}).get("data")) or []
    qt = ((node.get("qt") or {}).get(tcode)) or []
    pre_close = _fnum(qt[4]) if len(qt) > 4 else None
    is_us = tcode.startswith("us")
    points = []
    for line in rows:
        parts = line.split()
        if len(parts) < 2 or len(parts[0]) != 4:
            continue
        try:
            hh, mm = int(parts[0][:2]), int(parts[0][2:])
            close = float(parts[1])
        except ValueError:
            continue
        if is_us:
            dt = datetime.strptime(date_str, "%Y-%m-%d").replace(
                hour=hh, minute=mm, tzinfo=ET_TZ)
            ts = int(dt.timestamp())
            if ts > time.time() + 3600:
                ts -= 86400
        else:
            ts = int(datetime.strptime(f"{date_str} {hh:02d}:{mm:02d}",
                                       "%Y-%m-%d %H:%M")
                     .replace(tzinfo=CN_TZ).timestamp())
        points.append([ts, close])
    if len(points) < 10:
        return None
    points.sort()
    return {"pre_close": pre_close, "points": points}


async def _fetch_yahoo_trends(secid: str, date_str: str) -> Optional[dict]:
    """Minute closes from the Yahoo Finance chart API as a
    non-Eastmoney fallback (v1.4.5). s-sj2's IP is throttled on both
    Eastmoney trends hosts (502/empty) and Tencent's US minute data is
    a single point, so US curves need this path; Yahoo also covers HK
    and A-shares. range=1d serves the current/last session only, so
    callers must restrict this to date_str == today (same restriction
    as Tencent). Timestamps are absolute unix seconds (no tz
    conversion needed); null closes (lunch breaks, halts) are
    forward-filled and leading nulls dropped. pre_close from
    meta.chartPreviousClose (verified 2026-11-23: NVDA 125.53 and
    600519.SS 1485.75 both match Eastmoney/Tencent prev close)."""
    symbol = _yahoo_symbol(secid)
    if not symbol:
        return None
    result = None
    for host in ("query1", "query2"):
        try:
            resp = await upstream_get(
                f"https://{host}.finance.yahoo.com/v8/finance/chart/{symbol}"
                "?interval=1m&range=1d&includePrePost=false",
                timeout=15)
            found = (resp.json().get("chart") or {}).get("result")
            if found:
                result = found[0]
                break
        except Exception:
            continue
    if not result:
        return None
    meta = result.get("meta") or {}
    pre_close = _fnum(meta.get("chartPreviousClose")) or _fnum(meta.get("previousClose"))
    timestamps = result.get("timestamp") or []
    quote = ((result.get("indicators") or {}).get("quote") or [{}])[0]
    closes = quote.get("close") or []
    points = []
    last = None
    for ts, close in zip(timestamps, closes):
        c = _fnum(close)
        if c is None:
            if last is None:
                continue    # leading nulls: no price yet this session
            c = last
        last = c
        points.append([int(ts), c])
    if len(points) < 2:
        return None
    points.sort()
    print(f"[trends] {secid} via yahoo", flush=True)
    return {"pre_close": pre_close, "points": points}


async def fetch_stock_trends(secid: str, date_str: str) -> Optional[dict]:
    """Minute closes for one stock: {"pre_close": float|None,
    "points": [[ts, close], ...]}. DB-cached per (secid, date); today's
    rows refresh after TRENDS_TTL, past dates never refetch. Returns None
    when the secid yields no data (wrong prefix / upstream failure) and
    no cache exists. When the fetch yields nothing usable and there is no
    same-date cache row, falls back to the most recent cached payload for
    this secid under any date (weekend/early-morning: Saturday asks for
    Saturday, but Friday's session is the data the curve needs).
    v1.4.0: when Eastmoney yields nothing for today (throttled server
    IP), Tencent minute data is tried before the cache fallback; after a
    hard failure on all sources the secid backs off for
    TRENDS_FAIL_BACKOFF seconds (upstream skipped, cache fallback only).
    v1.4.4: Eastmoney hosts are tried in TRENDS_HOSTS order (push2his,
    then push2delay — Tencent US minute data has no usable series, so
    US curves need the alternate Eastmoney host as their fallback).
    v1.4.5: when both Eastmoney hosts fail (s-sj2 IP throttled on
    both), Yahoo Finance 1m chart data is tried before Tencent."""
    today_str = datetime.now(CN_TZ).strftime("%Y-%m-%d")
    with db() as conn:
        row = conn.execute(
            "SELECT payload, fetched_at FROM stock_trends_cache WHERE secid=? AND date=?",
            (secid, date_str)).fetchone()
    if row and (date_str < today_str or time.time() - row["fetched_at"] < _trends_ttl()):
        return json.loads(row["payload"])

    def _fallback():
        if row:
            return json.loads(row["payload"])
        with db() as conn:
            latest = conn.execute(
                "SELECT payload FROM stock_trends_cache WHERE secid=? "
                "ORDER BY date DESC LIMIT 1", (secid,)).fetchone()
        return json.loads(latest["payload"]) if latest else None

    # Per-secid failure backoff (v1.4.0): after all upstreams fail, skip
    # upstream calls for TRENDS_FAIL_BACKOFF so the 240s warm loop doesn't
    # hammer a throttled IP. Fresh same-date cache already returned above.
    if time.time() < _trends_fail_until.get(secid, 0):
        return _fallback()

    payload = None
    hard_fail = True   # stays True only if every Eastmoney host hard-fails
    for host in TRENDS_HOSTS:
        try:
            resp = await upstream_get(
                f"https://{host}.eastmoney.com/api/qt/stock/trends2/get"
                f"?secid={secid}&fields1=f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f11,f12,f13"
                "&fields2=f51,f52,f53,f54,f55,f56,f57,f58&iscr=0&iscca=0",
                referer=TRENDS_REF, timeout=15)
            data = resp.json().get("data")
            if not data:
                continue    # throttled/empty on this host: try the next
            hard_fail = False   # host answered (points may still be empty)
            points = []
            for line in data.get("trends") or []:
                parts = line.split(",")
                try:
                    ts = int(datetime.strptime(parts[0], "%Y-%m-%d %H:%M")
                             .replace(tzinfo=CN_TZ).timestamp())
                    points.append([ts, float(parts[2])])
                except (ValueError, IndexError):
                    continue
            if points:
                payload = {"pre_close": _fnum(data.get("preClose")),
                           "points": points}
                if host != TRENDS_HOSTS[0]:
                    print(f"[trends] {secid} via {host}", flush=True)
                break
        except Exception:
            continue

    if payload is None and date_str == today_str:
        # Eastmoney yielded nothing usable (throttled IP returns empty):
        # try Yahoo first (v1.4.5 — Tencent has no usable US series),
        # then Tencent (v1.4.0), before giving up.
        try:
            payload = await _fetch_yahoo_trends(secid, date_str)
        except Exception:
            payload = None
        if payload is None:
            try:
                payload = await _fetch_tencent_trends(secid, date_str)
            except Exception:
                payload = None

    if payload and payload.get("points"):
        if not payload.get("pre_close") and \
                secid.split(".", 1)[0] in ("0", "1", "116", "105", "106", "107"):
            # Fall back to the Sina quote cache's prev close (A/HK/US only).
            sym = to_sina_symbol(secid.split(".", 1)[1])
            q = _quote_cache["quotes"].get(sym) if sym else None
            if q:
                payload["pre_close"] = q.get("prev_close") or (
                    q["current"] / (1 + q["change_pct"] / 100) if q.get("current") else None)
        _trends_fail_until.pop(secid, None)
        with db() as conn:
            conn.execute(
                "INSERT INTO stock_trends_cache(secid, date, payload, fetched_at) VALUES(?,?,?,?) "
                "ON CONFLICT(secid, date) DO UPDATE SET payload=excluded.payload, fetched_at=excluded.fetched_at",
                (secid, date_str, json.dumps(payload), time.time()))
        return payload

    if hard_fail:
        _trends_fail_until[secid] = time.time() + TRENDS_FAIL_BACKOFF
    return _fallback()


async def fetch_holding_trends(code: str, market_map: Optional[dict], date_str: str,
                               name: Optional[str] = None) -> Optional[dict]:
    """Trends for one holding, trying secid candidates in order; remembers
    the working prefix per ticker and backs off TRENDS_TTL after a total
    failure so a bad code doesn't hammer upstream on every page refresh.
    JP/KR/TW holdings (ISIN / bare local codes) resolve via the suggest
    API instead (see resolve_em_secid)."""
    c = (code or "").strip().upper()
    if time.time() - _secid_fail_at.get(c, 0) < _trends_ttl():
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


def _fund_curve(holdings: list, trends_by_code: dict, start_ts: int,
                residual_pct: float = 0.0) -> list:
    """Fund intraday estimate curve from holdings' minute trends:
    pct(t) = sum(weight% * stock change%(t)) / 100 on the union minute
    grid (Asia/Shanghai day from start_ts), forward-filled per stock
    (excluded before its first point), no renormalization — same
    convention as /api/estimates. With residual_pct > 0 (P0 fill), the
    undisclosed stock weight additionally moves at the disclosed
    holdings' weighted-average change at each timestamp, mirroring
    _live_est_pct. Downsampled to 5-minute buckets."""
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
        wchg, quoted_w = 0.0, 0.0
        for i, (w, pts) in enumerate(per_stock):
            while idx[i] < len(pts) and pts[idx[i]][0] <= ts:
                last[i] = pts[idx[i]][1]
                idx[i] += 1
            if last[i] is not None:
                total += w * last[i] / 100
                wchg += w * last[i]
                quoted_w += w
                any_data = True
        if any_data and residual_pct > 0 and quoted_w > 0:
            total += residual_pct * (wchg / quoted_w) / 100
        if any_data:
            # Bucket timestamp = bucket start (5-min grid); the value is the
            # last computed point inside the bucket. Beijing session opens
            # (09:30 etc.) fall on the grid, so the first bucket is the open.
            buckets[(ts // 300) * 300] = round(total, 2)
    return [[ts, pct] for ts, pct in sorted(buckets.items())]


# ETF feeder funds （联接） disclose no stock holdings; their curve (and,
# as a bonus, estimate) comes from the tracking index instead (v1.2.0).
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


def _residual_pct(coverage: float, stock_position: Optional[float]) -> float:
    """Undisclosed stock weight (% of NAV): total stock position minus the
    disclosed top-10 coverage, floored at 0. Unknown position -> 0 (old
    no-fill behavior)."""
    if stock_position is None:
        return 0.0
    return max(stock_position - coverage, 0.0)


def _live_est_pct(holdings: list, syms: list, quotes: dict,
                  stock_position: Optional[float] = None):
    """Main estimate from live quotes: sum(weight% * change%) / 100 over
    quoted holdings (no renormalization), plus P0 residual fill: the
    undisclosed stock weight (total stock position - top-10 coverage)
    moves at the disclosed holdings' weighted-average change instead of
    being assumed flat. Single source for both /api/estimates and the
    final point of /api/history/today, so the curve end and the card
    number are the same computation."""
    est_pct, coverage, quoted_w, wchg = 0.0, 0.0, 0.0, 0.0
    for h, sym in zip(holdings, syms):
        coverage += h["weight"]
        q = quotes.get(sym) if sym else None
        if q:
            est_pct += h["weight"] * q["change_pct"] / 100
            quoted_w += h["weight"]
            wchg += h["weight"] * q["change_pct"]
    residual = _residual_pct(coverage, stock_position)
    if residual > 0 and quoted_w > 0:
        est_pct += residual * (wchg / quoted_w) / 100
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
                uids = [r[0] for r in conn.execute(
                    "SELECT DISTINCT user_id FROM user_funds").fetchall()]
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
                syms.add("sh511260")  # bond-fund estimate source
                if syms:
                    async with _quote_refresh_lock:
                        await _refresh_quotes(sorted(syms))
                    _wstat("quote_warm", last_run=time.time(),
                           last_result=f"refreshed {len(syms)} symbols for {len(codes)} funds")
                    print(f"[quote-warm] refreshed {len(syms)} symbols "
                          f"for {len(codes)} funds", flush=True)
                # Precompute per-user estimates payloads (quotes are hot
                # now) so the first page open after startup is served
                # from cache instead of computing synchronously.
                for uid in uids:
                    await _refresh_estimates(uid)
                if uids:
                    print(f"[quote-warm] estimates cache refreshed for "
                          f"{len(uids)} users", flush=True)
            else:
                _wstat("quote_warm", last_run=time.time(),
                       last_result="no watchlist funds")
        except Exception as e:
            log_error("quote-warm", repr(e))
            _wstat("quote_warm", last_run=time.time(),
                   last_result=f"failed: {e!r}")
            print(f"[quote-warm] failed: {e!r}", flush=True)
        _wstat("quote_warm", next_run=time.time() + 40)
        await asyncio.sleep(40)


async def _jittered(coro):
    """Stagger warm-loop upstream fetches with a random 0-0.3s delay so a
    whole watchlist's worth of requests doesn't burst at once (v1.4.0)."""
    await asyncio.sleep(random.uniform(0, 0.3))
    return await coro


async def _trends_warm_loop():
    """Keep stock_trends_cache hot: refresh minute trends for the union of
    holding codes across all users' watchlists (plus feeder funds'
    tracking-index secids), so the first /api/history/today call never
    waits on Eastmoney. Runs beside the quote warmer; startup is never
    blocked; errors are logged and skipped. Cycle: 240s in trading
    windows, 900s outside (v1.4.0)."""
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
                    _jittered(fetch_holding_trends(c, m, date_str, n))
                    for c, (m, n) in uniq.items()])
                n = len(uniq)
                for fc in feeder_codes:
                    idx = await resolve_fund_index(fc)
                    if idx:
                        async with _trends_sem:
                            await fetch_stock_trends(idx["secid"], date_str)
                        n += 1
                # Bond-fund curve source: 10Y treasury ETF minute trends.
                async with _trends_sem:
                    await fetch_stock_trends("1.511260", date_str)
                n += 1
                _wstat("trends_warm", last_run=time.time(),
                       last_result=f"refreshed {n} secids for {len(rows)} funds")
                print(f"[trends-warm] refreshed {n} secids "
                      f"for {len(rows)} funds", flush=True)
            else:
                _wstat("trends_warm", last_run=time.time(),
                       last_result="no watchlist funds")
        except Exception as e:
            log_error("trends-warm", repr(e))
            _wstat("trends_warm", last_run=time.time(),
                   last_result=f"failed: {e!r}")
            print(f"[trends-warm] failed: {e!r}", flush=True)
        _wstat("trends_warm", next_run=time.time() + _warm_interval())
        await asyncio.sleep(_warm_interval())


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


async def _resolve_fund_name(item_id: int, code: str):
    """Fill in a placeholder fund name (inserted as the bare code) via the
    search API, in the background. Only replaces the placeholder, so a
    name supplied by the frontend is never overwritten."""
    try:
        resp = await upstream_get(f"https://fundsuggest.eastmoney.com/FundSearch/api/FundSearchAPI.ashx?m=1&key={code}")
        name = next((d["NAME"] for d in resp.json().get("Datas", []) if d.get("CODE") == code and _is_fund_entry(d)), None)
        if name:
            with db() as conn:
                conn.execute(
                    "UPDATE user_funds SET fund_name=? WHERE id=? AND fund_name=?",
                    (name, item_id, code),
                )
    except Exception as e:
        print(f"[watchlist] name resolve failed for {code}: {e!r}", flush=True)


async def _warm_fund_trends(code: str):
    """Warm holdings + minute trends for one newly added fund (mirrors
    _trends_warm_loop for a single fund) so its first curve is instant."""
    try:
        hold = await fetch_holdings(code)
        mmap = await fetch_stock_market_map(code)
        date_str = datetime.now(CN_TZ).strftime("%Y-%m-%d")
        await asyncio.gather(*[
            fetch_holding_trends(h["code"], mmap, date_str, h.get("name"))
            for h in hold["holdings"] if h["weight"]])
    except Exception as e:
        print(f"[watchlist] trends warm failed for {code}: {e!r}", flush=True)


def _schedule_user_refresh(uid, warm_code=None):
    """After a watchlist mutation: invalidate the user's cached payloads
    and recompute them in the background (superseding any in-flight
    refresh) so the mutation itself returns immediately."""
    _est_gen[uid] = _est_gen.get(uid, 0) + 1
    _series_gen[uid] = _series_gen.get(uid, 0) + 1
    _est_cache.pop(uid, None)
    _series_cache.pop(uid, None)
    t = _est_tasks.get(uid)
    if t is not None and not t.done():
        t.cancel()
    _est_tasks[uid] = asyncio.create_task(_refresh_estimates(uid))
    t = _series_tasks.get(uid)
    if t is not None and not t.done():
        t.cancel()
    _series_tasks[uid] = asyncio.create_task(_refresh_series(uid))
    if warm_code:
        asyncio.create_task(_warm_fund_trends(warm_code))


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
    # Fast path: the frontend sends the name from its search result, so no
    # upstream call blocks the mutation. Without a name we insert the code
    # as a placeholder and resolve the real name in the background.
    name = (body.fund_name or "").strip()[:60] or code
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
    item_id = cur.lastrowid
    if name == code:
        asyncio.create_task(_resolve_fund_name(item_id, code))
    _schedule_user_refresh(uid, warm_code=code)
    return {"ok": True, "id": item_id, "fund_code": code, "fund_name": name, "category": category}


@app.delete("/api/watchlist/{item_id}")
async def delete_watchlist(item_id: int, session: Optional[str] = Cookie(default=None)):
    uid = current_user_id(session)
    with db() as conn:
        cur = conn.execute("DELETE FROM user_funds WHERE id=? AND user_id=?", (item_id, uid))
    if cur.rowcount == 0:
        raise HTTPException(404, "记录不存在")
    _schedule_user_refresh(uid)
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
    _est_cache.pop(uid, None)  # fund order changed
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
    _est_cache.pop(uid, None)  # category order changed
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
    _est_cache.pop(uid, None)  # payload carries category list
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
    _est_cache.pop(uid, None)  # category renamed
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
    _est_cache.pop(uid, None)  # category deleted
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
    _est_cache.pop(uid, None)  # fund category changed
    return {"ok": True, "category": category}


# Per-user cache of the computed estimates payload: a full recompute
# fetches NAV + holdings + quotes for every fund and takes seconds, so
# page opens serve the cached payload instantly. Fresh (<30s) ->
# returned as-is; stale -> returned immediately while a deduped
# background task recomputes; no entry -> computed synchronously. The
# quote warmer also precomputes it every cycle, so even the first open
# after a server start hits the cache.
_est_cache = {}   # uid -> (computed_at_epoch, payload dict)
_est_tasks = {}   # uid -> asyncio.Task running _refresh_estimates
_est_gen = {}     # uid -> mutation counter; stale computes don't overwrite
_EST_TTL = 30


async def _compute_estimates(uid):
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
        # Per-fund fault tolerance: one fund with no reachable NAV source
        # (e.g. HK MRF 968061 — lsjz returns an empty list) must not 502
        # the whole page; it renders with empty NAV fields instead.
        try:
            nav = await fetch_nav(r["fund_code"], r["fund_name"])
        except Exception:
            nav = {"nav_date": None, "dwjz": None, "jzzzl": None,
                   "prev_nav_date": None, "prev_dwjz": None, "prev_jzzzl": None,
                   "stale": True}
        try:
            hold = await fetch_holdings(r["fund_code"])
        except Exception:
            hold = {"holdings": [], "report_label": None, "report_date": None, "stale": True}
        mmap = await fetch_stock_market_map(r["fund_code"])
        syms = [await holding_quote_symbol(h, mmap) for h in hold["holdings"]]
        try:
            stock_position = await fetch_stock_position(r["fund_code"])
        except Exception:
            stock_position = None
        return (r, nav, hold, syms, stock_position)

    per_fund = await asyncio.gather(*[_fund_inputs(r) for r in rows])
    all_symbols = [s for (_, _, _, syms, _) in per_fund for s in syms if s]
    # Bond funds (no stock holdings) are estimated from the 10Y treasury
    # ETF (511260) daily change; always fetch its quote alongside.
    all_symbols.append("sh511260")
    quotes = await fetch_quotes(all_symbols)
    q_stale = quotes_stale()
    # Industries/sectors for the union of holding codes (A-share via F10,
    # DB-cached 30d; overseas via manual table; failures yield no sector).
    ind_codes = sorted({h["code"] for (_, _, hold, _, _) in per_fund
                        for h in hold["holdings"] if h["code"]})
    ind_results = await asyncio.gather(
        *[fetch_industry(c) for c in ind_codes], return_exceptions=True)
    industries = {c: (v if isinstance(v, str) else None)
                  for c, v in zip(ind_codes, ind_results)}
    # Fund types (QDII detection for NAV publish timing; bond/money funds
    # have no stock holdings to estimate from). DB-cached 30d.
    type_results = await asyncio.gather(
        *[fetch_fund_type(r["fund_code"]) for r in rows], return_exceptions=True)
    fund_types = {r["fund_code"]: (v if isinstance(v, str) else None)
                  for r, v in zip(rows, type_results)}
    out = []
    for r, nav, hold, syms, stock_position in per_fund:
        est_pct, est_pct_ext, coverage, quote_time, skipped = 0.0, 0.0, 0.0, None, []
        has_us, has_ext = False, False
        quoted_w_ext, wchg_ext = 0.0, 0.0
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
                    chg_used = chg_ext if chg_ext is not None else q["change_pct"]
                else:
                    chg_used = q["change_pct"]
                est_pct_ext += h["weight"] * chg_used / 100
                quoted_w_ext += h["weight"]
                wchg_ext += h["weight"] * chg_used
                qt = q["quote_time"]
                if qt and (quote_time is None or qt.replace("/", "-") > quote_time.replace("/", "-")):
                    quote_time = qt
                detail.append({**h, "change_pct": round(q["change_pct"], 2),
                               "change_pct_ext": round(chg_ext, 2) if chg_ext is not None else None})
            else:
                skipped.append(h["code"])
                detail.append({**h, "change_pct": None, "change_pct_ext": None})
        # P0 residual fill for the ext estimate too: undisclosed stock
        # weight moves at the disclosed holdings' ext weighted average.
        residual = _residual_pct(coverage, stock_position)
        if residual > 0 and quoted_w_ext > 0:
            est_pct_ext += residual * (wchg_ext / quoted_w_ext) / 100
        # Main estimate: same helper the history endpoint uses for its
        # final curve point (same quotes, same formula).
        est_pct, _ = _live_est_pct(hold["holdings"], syms, quotes, stock_position)
        # ETF feeder funds have no usable stock holdings (est would be a
        # meaningless 0.00%): use the tracking index's latest change.
        feeder_applied = False
        if coverage == 0 and _is_feeder(r["fund_name"], hold["holdings"]):
            idx = await resolve_fund_index(r["fund_code"])
            if idx:
                async with _trends_sem:
                    tr = await fetch_stock_trends(
                        idx["secid"], datetime.now(CN_TZ).strftime("%Y-%m-%d"))
                chg = _index_latest_chg(tr)
                if chg is not None:
                    feeder_applied = True
                    ts_last = tr["points"][-1][0]
                    est_pct = chg
                    coverage = 100.0
                    detail = [{"code": idx["secid"].split(".", 1)[1],
                               "name": idx["index_name"], "weight": 100.0,
                               "change_pct": round(chg, 2), "change_pct_ext": None}]
                    skipped = []
                    quote_time = datetime.fromtimestamp(ts_last, CN_TZ).strftime("%Y-%m-%d %H:%M")
        # Bond funds disclose no stock holdings: estimate from the 10Y
        # treasury ETF (511260) daily change, like other apps' bond-index
        # estimates. Money-market funds stay NAV-only (different semantics).
        bond_applied = False
        ftype0 = fund_types.get(r["fund_code"]) or ""
        if coverage == 0 and not feeder_applied and "债券" in ftype0:
            bq = quotes.get("sh511260")
            if bq:
                bond_applied = True
                est_pct = bq["change_pct"]
                detail = [{"code": "511260", "name": "十年国债ETF国泰", "weight": 100.0,
                           "change_pct": round(bq["change_pct"], 2), "change_pct_ext": None}]
                skipped = []
                quote_time = bq["quote_time"]
        # Main sector: group top-10 holdings by industry/sector, take the
        # largest summed weight; sector change = weighted change of that
        # sector's own holdings (same quotes as the estimate). share_pct
        # is absolute (% of fund NAV, same basis as the holding weights),
        # not normalized by coverage. Computed from the raw holdings so
        # feeder funds (zero weights) get null.
        sector = None
        sec_w: dict = {}
        sec_num: dict = {}
        sec_den: dict = {}
        for h, sym in zip(hold["holdings"], syms):
            ind = industries.get(h["code"])
            if not ind or not h["weight"]:
                continue
            sec_w[ind] = sec_w.get(ind, 0.0) + h["weight"]
            q = quotes.get(sym) if sym else None
            if q:
                sec_num[ind] = sec_num.get(ind, 0.0) + h["weight"] * q["change_pct"]
                sec_den[ind] = sec_den.get(ind, 0.0) + h["weight"]
        raw_coverage = sum(h["weight"] for h in hold["holdings"])
        if sec_w and raw_coverage > 0:
            main_ind = max(sec_w, key=sec_w.get)
            sector = {
                "name": _short_industry(main_ind),
                "share_pct": round(sec_w[main_ind], 1),
                "change_pct": round(sec_num[main_ind] / sec_den[main_ind], 2)
                if sec_den.get(main_ind) else None,
            }
        elif raw_coverage > 0 and FUYAO_ENABLED:
            # No per-stock industry resolved at all (F10 down / unknown
            # codes) -> fund-level fuyao industry allocation (v1.4.8).
            # Display only: name + disclosed ratio, no live change.
            sector = await fetch_industry_allocation_fuyao(r["fund_code"])
        est_nav = nav["dwjz"] * (1 + est_pct / 100) if nav["dwjz"] else None
        # Ext estimate only when the fund holds US stocks with valid ext data
        # (change_pct_ext is None during US regular hours -> null then too).
        show_ext = has_us and has_ext
        est_nav_ext = nav["dwjz"] * (1 + est_pct_ext / 100) if (show_ext and nav["dwjz"]) else None
        ftype = fund_types.get(r["fund_code"])
        # QDII funds publish NAV T-1; domestic funds (incl. A+H via 港股通)
        # publish same-day. Judge by fund type, not by holdings' markets.
        is_qdii = bool(ftype and "QDII" in ftype) or "QDII" in (r["fund_name"] or "")
        # Hong Kong MRF funds （互认基金） publish NAV T+1 or later, like QDII.
        is_mrf = ftype == "香港互认基金" or r["fund_code"][:3] in ("968", "969")
        # Bond/money funds disclose no stock holdings: no estimate, no
        # curve, and no stale warning (their empty holdings page is not
        # "stale data"). Feeder-index and bond-ETF-estimated funds excluded.
        no_stock_holdings = coverage == 0 and not feeder_applied and not bond_applied
        # P1 β calibration: correct the holdings-based estimate with the
        # fund's historical α/β (60d regression vs official NAV changes).
        # Feeder/bond estimates have their own anchors and MRF funds have
        # no holdings estimate — none of those are calibrated. The ext
        # (pre/post-market) estimate stays raw by design.
        beta_row = _beta_row(r["fund_code"])
        _ensure_beta(r["fund_code"])
        beta_applied = False
        if not feeder_applied and not bond_applied and not no_stock_holdings and not is_mrf:
            cal = _apply_beta(est_pct, beta_row)
            if cal != est_pct:
                beta_applied = True
                est_pct = cal
                est_nav = nav["dwjz"] * (1 + est_pct / 100) if nav["dwjz"] else None
        out.append({
            "id": r["id"], "fund_code": r["fund_code"], "fund_name": r["fund_name"], "category": r["category"] or "",
            "fund_type": ftype, "is_qdii": is_qdii, "is_mrf": is_mrf,
            "bond_est": bond_applied, "no_stock_holdings": no_stock_holdings,
            "nav_date": nav["nav_date"], "dwjz": nav["dwjz"], "jzzzl": nav["jzzzl"],
            "prev_nav_date": nav.get("prev_nav_date"), "prev_dwjz": nav.get("prev_dwjz"), "prev_jzzzl": nav.get("prev_jzzzl"),
            "est_nav": round(est_nav, 4) if est_nav else None,
            "est_change_pct": round(est_pct, 2),
            "credibility": _credibility(beta_row) if not (
                feeder_applied or bond_applied or no_stock_holdings or is_mrf) else None,
            "beta": round(beta_row["beta"], 3) if (
                beta_applied and beta_row and beta_row.get("beta") is not None) else None,
            "est_nav_ext": round(est_nav_ext, 4) if est_nav_ext else None,
            "est_change_pct_ext": round(est_pct_ext, 2) if show_ext else None,
            "coverage_pct": round(coverage, 2),
            "stock_position_pct": round(stock_position, 2) if stock_position is not None else None,
            "residual_pct": round(_residual_pct(raw_coverage, stock_position), 2),
            "report_label": hold["report_label"], "report_date": hold["report_date"],
            "quote_time": quote_time,
            "stale": False if no_stock_holdings else (nav["stale"] or hold["stale"] or q_stale),
            "skipped_holdings": skipped, "holdings": detail,
            "sector": sector,
        })
    return {"funds": out, "categories": cats, "refresh_seconds": refresh_seconds, "disclaimer": "自算估值，仅供参考，以基金公司公布净值为准"}


async def _refresh_estimates(uid):
    gen = _est_gen.get(uid, 0)
    try:
        payload = await _compute_estimates(uid)
        if _est_gen.get(uid, 0) == gen:  # not superseded by a mutation
            _est_cache[uid] = (time.time(), payload)
    except Exception as e:
        print(f"[estimates] background refresh failed for uid {uid}: {e!r}")
    finally:
        if _est_tasks.get(uid) is asyncio.current_task():
            _est_tasks.pop(uid, None)


async def _await_refresh(tasks: dict, uid):
    """Wait for an in-flight background refresh (following superseding
    tasks) so a cache-miss request reuses it instead of duplicating the
    compute. Re-raises only if this request itself is cancelled."""
    while True:
        t = tasks.get(uid)
        if t is None or t.done():
            return
        try:
            await asyncio.shield(t)
            return
        except asyncio.CancelledError:
            if asyncio.current_task().cancelling():
                raise
            continue  # superseded by a newer refresh; wait for that one
        except Exception:
            return


@app.get("/api/estimates")
async def estimates(session: Optional[str] = Cookie(default=None)):
    uid = current_user_id(session)
    hit = _est_cache.get(uid)
    if hit is None:
        await _await_refresh(_est_tasks, uid)
        hit = _est_cache.get(uid)
    if hit is not None:
        computed_at, payload = hit
        if time.time() - computed_at > _EST_TTL and uid not in _est_tasks:
            _est_tasks[uid] = asyncio.create_task(_refresh_estimates(uid))
        # data_flow_ok is attached per response (never baked into the
        # cached payload) so the banner reflects the current judgement.
        return {**payload, "data_flow_ok": _data_flow_ok()}
    gen = _est_gen.get(uid, 0)
    payload = await _compute_estimates(uid)
    if _est_gen.get(uid, 0) == gen:
        _est_cache[uid] = (time.time(), payload)
    return {**payload, "data_flow_ok": _data_flow_ok()}


# Per-user cache of the computed curve series: a full recompute touches
# holdings + trends + quotes for every fund and takes seconds, so page
# refreshes serve the cached series instantly. Fresh (<20s) -> returned
# as-is; stale -> returned immediately while a deduped background task
# recomputes; no entry -> computed synchronously.
_series_cache = {}   # uid -> (computed_at_epoch, series dict)
_series_tasks = {}   # uid -> asyncio.Task running _refresh_series
_series_gen = {}     # uid -> mutation counter; stale computes don't overwrite
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
        try:
            stock_position = await fetch_stock_position(code)
        except Exception:
            stock_position = None
        return code, fr["fund_name"], hold["holdings"], mmap, syms, stock_position

    # Fault-tolerant: one fund's upstream failure must not blank every
    # curve (a 500 here leaves all sparklines empty).
    raw_inputs = await asyncio.gather(
        *[_inputs(fr) for fr in fund_rows], return_exceptions=True)
    per_fund = []
    for fr, res in zip(fund_rows, raw_inputs):
        if isinstance(res, Exception):
            print(f"[history] inputs failed for {fr['fund_code']}: {res!r}")
            per_fund.append((fr["fund_code"], fr["fund_name"], [], None, [], None))
        else:
            per_fund.append(res)
    # One trends fetch per unique holding code across all funds.
    uniq = {}
    for _, _, holdings, mmap, _, _ in per_fund:
        for h in holdings:
            uniq.setdefault(h["code"], (mmap, h.get("name")))
    fetched = await asyncio.gather(*[
        fetch_holding_trends(c, mmap, date_str, n) for c, (mmap, n) in uniq.items()],
        return_exceptions=True)
    trends_by_code = {c: (None if isinstance(v, Exception) else v)
                      for c, v in zip(uniq.keys(), fetched)}
    # Live quotes from the same 45s cache /api/estimates reads, for the
    # final curve point (curve end == card estimate by construction).
    all_symbols = [s for (_, _, _, _, syms, _) in per_fund for s in syms if s]
    all_symbols.append("sh511260")  # bond-fund estimate source (see _compute_estimates)
    quotes = await fetch_quotes(all_symbols)
    now_ts = int(time.time())
    series = {}
    for code, fund_name, holdings, _, syms, stock_position in per_fund:
      try:
        raw_cov = sum(h["weight"] for h in holdings)
        pts = _fund_curve(holdings, trends_by_code, start_ts,
                          _residual_pct(raw_cov, stock_position))
        feeder_tr = None
        if not pts and _is_feeder(fund_name, holdings):
            # ETF feeder fund: curve from its tracking index (weight 100%).
            idx = await resolve_fund_index(code)
            if idx:
                async with _trends_sem:
                    feeder_tr = await fetch_stock_trends(idx["secid"], date_str)
                pts = _index_curve(feeder_tr, start_ts)
        bond_tr = None
        if not pts:
            # Bond fund: curve from the 10Y treasury ETF (511260) minute
            # trends, same weight-100% machinery as feeder index curves.
            ftype_s = await fetch_fund_type(code) or ""
            if "债券" in ftype_s:
                async with _trends_sem:
                    bond_tr = await fetch_stock_trends("1.511260", date_str)
                pts = _index_curve(bond_tr, start_ts)
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
            est_pct, coverage = _live_est_pct(holdings, syms, quotes, stock_position)
            if coverage == 0 and _is_feeder(fund_name, holdings):
                if feeder_tr is None:
                    idx = await resolve_fund_index(code)
                    if idx:
                        async with _trends_sem:
                            feeder_tr = await fetch_stock_trends(idx["secid"], date_str)
                chg = _index_latest_chg(feeder_tr)
                if chg is not None:
                    est_pct = chg
            if coverage == 0 and bond_tr is not None:
                # Bond fund final point = 511260 quote change (same source
                # as the card estimate), falling back to trends latest.
                bq = quotes.get("sh511260")
                chg = bq["change_pct"] if bq else _index_latest_chg(bond_tr)
                if chg is not None:
                    est_pct = chg
            if coverage > 0:
                # Holdings-based estimate: same P1 β calibration as the
                # card (feeder/bond overrides above only fire at
                # coverage == 0), keeping curve end == card value.
                _ensure_beta(code)
                est_pct = _apply_beta(est_pct, _beta_row(code))
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
    gen = _series_gen.get(uid, 0)
    try:
        series = await _compute_series(uid)
        if _series_gen.get(uid, 0) == gen:  # not superseded by a mutation
            _series_cache[uid] = (time.time(), series)
    except Exception as e:
        print(f"[history] background refresh failed for uid {uid}: {e!r}")
    finally:
        if _series_tasks.get(uid) is asyncio.current_task():
            _series_tasks.pop(uid, None)


@app.get("/api/history/today")
async def history_today(session: Optional[str] = Cookie(default=None)):
    uid = current_user_id(session)
    hit = _series_cache.get(uid)
    if hit is None:
        await _await_refresh(_series_tasks, uid)
        hit = _series_cache.get(uid)
    if hit is not None:
        computed_at, series = hit
        if time.time() - computed_at > _SERIES_TTL and uid not in _series_tasks:
            _series_tasks[uid] = asyncio.create_task(_refresh_series(uid))
        return {"series": series}
    gen = _series_gen.get(uid, 0)
    series = await _compute_series(uid)
    if _series_gen.get(uid, 0) == gen:
        _series_cache[uid] = (time.time(), series)
    return {"series": series}


# ---------------------------------------------------------------- rankings
# 排行榜 data: Eastmoney rankhandler (full-market period returns, QDII list),
# holdings (QDII US share), pingzhongdata NAV trend (volatility board, pooled
# from the returns boards' per-period top-30 union since v1.2.0).
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


RANK_CATS = {"all", "gp", "hh", "zq", "zs", "qdii", "fof"}


async def fetch_returns_all(ft: str = "all") -> list:
    """Full rankhandler dataset for one fund category (ft=all/gp/hh/zq/zs/
    qdii/fof). Page count comes from the response's allRecords (5000 rows
    per page); parse in a thread so the event loop stays responsive."""
    async def one_page(page):
        resp = await upstream_get(
            f"https://fund.eastmoney.com/data/rankhandler.aspx?op=ph&dt=kf&ft={ft}&rs=&gs=0"
            f"&sc=zzf&st=desc&sd=&ed=&qdii=&tabSubtype=,,,,,&pi={page}&pn=5000&dx=1&v=0.123456",
            referer=RANK_REF, timeout=60,
        )
        return resp.text

    first = await one_page(1)
    m = re.search(r"allRecords:(\d+)", first)
    total = int(m.group(1)) if m else 5000
    pages = max(1, math.ceil(total / 5000))
    texts = [first]
    if pages > 1:
        texts += await asyncio.gather(*(one_page(p) for p in range(2, pages + 1)))
    rows = []
    for t in texts:
        rows += await asyncio.to_thread(parse_rank_rows, t)
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


def parse_stock_position(text: str) -> Optional[float]:
    """Latest 股票占净比 (%) from pingzhongdata Data_assetAllocation
    (quarterly series; verified 2026-11-23: 024481 -> 84.55 at 2026-09-30).
    None when absent/unparseable."""
    m = re.search(r"Data_assetAllocation\s*=\s*(\{.*?\});", text, re.S)
    if not m:
        return None
    try:
        data = json.loads(m.group(1))
        for s in data.get("series", []):
            if s.get("name") == "股票占净比":
                vals = [v for v in s.get("data", []) if isinstance(v, (int, float))]
                return float(vals[-1]) if vals else None
    except Exception:
        return None
    return None


async def fetch_stock_position(code: str) -> Optional[float]:
    """Fund's total stock position (% of NAV) from the latest quarterly
    asset-allocation disclosure. DB-cached 24h (same cadence as holdings);
    failures cached as NULL and retried after a day. None -> caller falls
    back to the old no-fill estimate; never raises."""
    with db() as conn:
        row = conn.execute(
            "SELECT stock_pct, fetched_at FROM fund_stock_position WHERE fund_code=?",
            (code,)).fetchone()
    if row and time.time() - row["fetched_at"] < HOLDINGS_TTL:
        return row["stock_pct"]
    pct = None
    try:
        pct = parse_stock_position(await fetch_pingzhong_text(code))
    except Exception:
        pct = None
    if pct is None and FUYAO_ENABLED:
        # pingzhongdata failed/absent -> fuyao fallback (v1.4.7)
        pct = await fetch_stock_position_fuyao(code)
    if pct is None and row and row["stock_pct"] is not None:
        return row["stock_pct"]  # keep last known value on upstream failure
    with db() as conn:
        conn.execute(
            "INSERT INTO fund_stock_position(fund_code, stock_pct, fetched_at) VALUES(?,?,?) "
            "ON CONFLICT(fund_code) DO UPDATE SET stock_pct=excluded.stock_pct, "
            "fetched_at=excluded.fetched_at",
            (code, pct, time.time()))
    return pct


# ---------------------------------------------------------------- P1 β calibration
# Per-fund regression of official daily NAV change on the holdings-based
# estimate (top-10 + P0 residual fill) over the last 60 NAV days:
# actual = α + β·est. Live estimates are corrected as α + β·est_raw to
# absorb manager trading / weight drift since the last disclosure.
# Holdings are the CURRENT disclosed top-10 applied to the whole window
# (same look-ahead limitation as tools/backtest.py). Bond/money/MRF and
# feeder funds are skipped (their estimates have other anchors); a row
# with n=0 is still stored as a negative cache. P4 credibility tiers
# come from the stored MAE (calibrated when available, else raw).
BETA_WINDOW = 60        # NAV days per regression
BETA_MIN_N = 20         # minimum pairs to calibrate
BETA_TTL = 86400        # recompute at most once a day per fund
_kline_sem = asyncio.Semaphore(5)
_beta_tasks: dict = {}  # fund_code -> asyncio.Task running compute_fund_beta
_beta_fail_at: dict = {}  # fund_code -> last compute failure ts (600s backoff)


async def fetch_nav_history(code: str, days: int = BETA_WINDOW) -> list:
    """[(date, jzzzl)] newest first from lsjz (20 rows/page, max 6 pages)."""
    rows = []
    page = 1
    try:
        while len(rows) < days and page <= 6:
            resp = await upstream_get(
                f"https://api.fund.eastmoney.com/f10/lsjz?fundCode={code}"
                f"&pageIndex={page}&pageSize=20",
                referer=f"https://fund.eastmoney.com/f10/jjjz_{code}.html",
            )
            items = resp.json()["Data"]["LSJZList"]
            if not items:
                break
            for it in items:
                try:
                    rows.append((it["FSRQ"], float(it["JZZZL"] or 0)))
                except (TypeError, ValueError, KeyError):
                    continue
            page += 1
    except Exception:
        if rows:
            return rows[:days]  # partial lsjz history beats a source switch
        if FUYAO_ENABLED:
            # lsjz fully down -> fuyao fallback (v1.4.7)
            return await fetch_nav_history_fuyao(code, days)
        raise
    return rows[:days]


async def fetch_daily_closes(secid: str) -> dict:
    """{date: close} daily closes for one secid from the Eastmoney kline
    API (klt=101, fqt=1), DB-cached in stock_daily_cache. A cache younger
    than a day is served as-is. When Eastmoney fails or returns nothing,
    A-share secids fall back to fuyao daily closes (v1.4.8, written to
    the same cache); other markets return whatever is cached."""
    with db() as conn:
        rows = conn.execute(
            "SELECT date, close, fetched_at FROM stock_daily_cache WHERE secid=?",
            (secid,)).fetchall()
    cached = {r["date"]: r["close"] for r in rows}
    if rows and time.time() - max(r["fetched_at"] for r in rows) < BETA_TTL:
        return cached
    beg = (datetime.now(CN_TZ) - timedelta(days=240)).strftime("%Y%m%d")
    fresh = {}
    try:
        async with _kline_sem:
            resp = await upstream_get(
                "https://push2his.eastmoney.com/api/qt/stock/kline/get"
                f"?secid={secid}&fields1=f1,f2,f3&fields2=f51,f52,f53"
                f"&klt=101&fqt=1&beg={beg}&end=20500101",
                referer=TRENDS_REF, timeout=15)
        klines = ((resp.json().get("data") or {}).get("klines")) or []
        for row in klines:
            f = row.split(",")
            if len(f) >= 3:
                try:
                    fresh[f[0]] = float(f[2])
                except ValueError:
                    continue
    except Exception:
        fresh = {}
    if not fresh and FUYAO_ENABLED:
        # Eastmoney kline failed/empty -> fuyao fallback (v1.4.8, A-share
        # only; other markets keep whatever is cached).
        fresh = await fetch_daily_closes_fuyao(secid)
    if fresh:
        now = time.time()
        with db() as conn:
            conn.executemany(
                "INSERT INTO stock_daily_cache(secid, date, close, fetched_at) "
                "VALUES(?,?,?,?) ON CONFLICT(secid, date) DO UPDATE SET "
                "close=excluded.close, fetched_at=excluded.fetched_at",
                [(secid, d, c, now) for d, c in fresh.items()])
        cached.update(fresh)
    return cached


def _daily_changes(closes: dict) -> dict:
    """{date: change%} vs the stock's own previous trading day."""
    dates = sorted(closes)
    out = {}
    for prev, cur in zip(dates, dates[1:]):
        if closes[prev]:
            out[cur] = (closes[cur] - closes[prev]) / closes[prev] * 100
    return out


async def _holding_daily_changes(h: dict, mmap) -> dict:
    """Daily change% map for one holding via Eastmoney kline; {} when no
    secid yields enough closes. JP/KR/TW resolve by name like trends."""
    c = (h["code"] or "").strip().upper()
    if _em_want_types(c, mmap):
        secid = await resolve_em_secid(c, h.get("name"), mmap)
        secids = [secid] if secid else []
    else:
        secids = _secid_candidates(c, mmap)
    for secid in secids:
        closes = await fetch_daily_closes(secid)
        if len(closes) >= 30:
            _us_prefix_cache[c] = secid.split(".", 1)[0]
            return _daily_changes(closes)
    return {}


def _ols_beta(pairs: list):
    """OLS actual = α + β·est over [(est, actual)] pairs, β clamped to
    [0.5, 1.5] and α (recomputed with the clamped β) to [-0.5, 0.5]."""
    n = len(pairs)
    mx = sum(p[0] for p in pairs) / n
    my = sum(p[1] for p in pairs) / n
    sxx = sum((p[0] - mx) ** 2 for p in pairs)
    sxy = sum((p[0] - mx) * (p[1] - my) for p in pairs)
    beta = sxy / sxx if sxx > 0 else 1.0
    beta = min(1.5, max(0.5, beta))
    alpha = min(0.5, max(-0.5, my - beta * mx))
    return alpha, beta


def _beta_row(code: str) -> Optional[dict]:
    with db() as conn:
        row = conn.execute(
            "SELECT * FROM fund_beta WHERE fund_code=?", (code,)).fetchone()
    return dict(row) if row else None


def _credibility(row: Optional[dict]) -> Optional[str]:
    """P4 tier from stored MAE (calibrated preferred): ≤0.3 高, ≤0.6 中,
    else 低. None when no backtest data exists for the fund."""
    if not row:
        return None
    mae = row.get("mae_cal")
    if mae is None:
        mae = row.get("mae_raw")
    if mae is None:
        return None
    return "高" if mae <= 0.3 else ("中" if mae <= 0.6 else "低")


def _apply_beta(est_pct: float, row: Optional[dict]) -> float:
    if row and row.get("beta") is not None and row.get("alpha") is not None:
        return row["alpha"] + row["beta"] * est_pct
    return est_pct


async def compute_fund_beta(code: str) -> Optional[dict]:
    """Fit and store α/β for one fund. Ineligible funds (MRF, bond/money
    type, no stock holdings) store an n=0 row as a daily negative cache.
    Returns the stored row dict, or None for ineligible funds."""
    def _store(alpha, beta, n, mae_raw, mae_cal):
        row = {"fund_code": code, "alpha": alpha, "beta": beta, "n": n,
               "mae_raw": mae_raw, "mae_cal": mae_cal, "updated_at": time.time()}
        with db() as conn:
            conn.execute(
                "INSERT INTO fund_beta(fund_code, alpha, beta, n, mae_raw, mae_cal, updated_at) "
                "VALUES(?,?,?,?,?,?,?) ON CONFLICT(fund_code) DO UPDATE SET "
                "alpha=excluded.alpha, beta=excluded.beta, n=excluded.n, "
                "mae_raw=excluded.mae_raw, mae_cal=excluded.mae_cal, "
                "updated_at=excluded.updated_at",
                (code, alpha, beta, n, mae_raw, mae_cal, row["updated_at"]))
        return row

    if _is_mrf_code(code):
        _store(None, None, 0, None, None)
        return None
    ftype = await fetch_fund_type(code) or ""
    if "债券" in ftype or "货币" in ftype:
        _store(None, None, 0, None, None)
        return None
    hold = await fetch_holdings(code)
    holdings = hold["holdings"]
    coverage = sum(h["weight"] for h in holdings)
    if coverage <= 0:
        _store(None, None, 0, None, None)
        return None
    mmap = await fetch_stock_market_map(code)
    try:
        stock_position = await fetch_stock_position(code)
    except Exception:
        stock_position = None
    residual = _residual_pct(coverage, stock_position)
    nav_rows = await fetch_nav_history(code, BETA_WINDOW)
    changes = {}
    for h in holdings:
        changes[h["code"]] = await _holding_daily_changes(h, mmap)
    pairs = []
    for date, official in nav_rows:
        num, quoted_w = 0.0, 0.0
        for h in holdings:
            chg = changes.get(h["code"], {}).get(date)
            if chg is not None:
                num += h["weight"] * chg
                quoted_w += h["weight"]
        if quoted_w < 0.5 * coverage:
            continue
        est = num / 100 + (residual * (num / quoted_w) / 100 if quoted_w else 0.0)
        pairs.append((est, official))
    n = len(pairs)
    if n == 0:
        _store(None, None, 0, None, None)
        return None
    mae_raw = sum(abs(e - o) for e, o in pairs) / n
    if n < BETA_MIN_N:
        return _store(None, None, n, mae_raw, None)
    alpha, beta = _ols_beta(pairs)
    mae_cal = sum(abs(alpha + beta * e - o) for e, o in pairs) / n
    return _store(alpha, beta, n, mae_raw, mae_cal)


async def _compute_beta_bg(code: str):
    try:
        await compute_fund_beta(code)
        _beta_fail_at.pop(code, None)
    except Exception as e:
        _beta_fail_at[code] = time.time()
        log_error("beta", f"{code}: {e!r}")
        print(f"[beta] compute failed for {code}: {e!r}", flush=True)
    finally:
        if _beta_tasks.get(code) is asyncio.current_task():
            _beta_tasks.pop(code, None)


def _ensure_beta(code: str):
    """Schedule a background β compute when the stored row is missing or
    older than a day. Never blocks the caller; estimates use the raw
    value until the row lands (next 30s cache refresh picks it up).
    Failures back off 600s so an upstream outage doesn't retry every
    estimates refresh."""
    row = _beta_row(code)
    if row and time.time() - row["updated_at"] < BETA_TTL:
        return
    if time.time() - _beta_fail_at.get(code, 0) < 600:
        return
    t = _beta_tasks.get(code)
    if t and not t.done():
        return
    _beta_tasks[code] = asyncio.create_task(_compute_beta_bg(code))


async def _beta_loop():
    """Daily β recalibration for all watchlisted funds after the CN
    close (15:35 CST). Lazy _ensure_beta covers funds added intraday."""
    await asyncio.sleep(30)
    last_run = None
    while True:
        try:
            now = datetime.now(CN_TZ)
            today = now.strftime("%Y-%m-%d")
            nxt = now.replace(hour=15, minute=35, second=0, microsecond=0)
            if nxt <= now:
                nxt += timedelta(days=1)
            _wstat("beta", next_run=nxt.timestamp())
            if now.hour * 60 + now.minute >= 15 * 60 + 35 and last_run != today:
                last_run = today
                with db() as conn:
                    codes = [r[0] for r in conn.execute(
                        "SELECT DISTINCT fund_code FROM user_funds").fetchall()]
                for code in codes:
                    await _jittered(_compute_beta_bg(code))
                _wstat("beta", last_run=time.time(),
                       last_result=f"daily recalibration for {len(codes)} funds")
                if codes:
                    print(f"[beta] daily recalibration for {len(codes)} funds",
                          flush=True)
        except Exception as e:
            log_error("beta-loop", repr(e))
            print(f"[beta] loop failed: {e!r}", flush=True)
        await asyncio.sleep(600)


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
            # absolute share: US weight as % of fund NAV (v1.4.0; was
            # relative to the disclosed top-10 total)
            "us_pct": round(us_w, 1),
            "coverage": round(total_w, 2), "y1": r["y1"],
            "report_label": hold["report_label"],
        }

    results = await asyncio.gather(*(one(r) for r in qdii))
    excluded_low_coverage = sum(1 for x in results if x and x.get("_low_coverage"))
    items = [x for x in results if x and not x.get("_low_coverage")]
    items.sort(key=lambda x: x["us_pct"], reverse=True)
    return {"updated_at": now_iso(), "total": len(qdii),
            "excluded_low_coverage": excluded_low_coverage, "items": items[:30]}


# Volatility / return-volatility-ratio boards (v1.4.0): four windows in
# trading days, computed in one pass per fund from pingzhongdata NAV history.
VOL_WINDOWS = {"1m": 20, "3m": 60, "6m": 120, "1y": 250}
# Per-window minimum daily-return counts; fewer -> fund skipped for that
# window only (other windows unaffected). Stops short-history new funds
# from topping long-window boards on a handful of samples.
VOL_MIN_RETS = {"1m": 15, "3m": 40, "6m": 80, "1y": 120}


def _parse_vol_windows(text: str):
    """Per-window {ret, vol, ratio} from Data_netWorthTrend.

    ret = (last NAV / first NAV - 1) * 100 over the last N+1 NAV points;
    vol = std of last N daily returns (equityReturn, %) x sqrt(250);
    ratio = ret / vol (None when vol is 0). A window with fewer than
    VOL_MIN_RETS daily returns is omitted."""
    m = re.search(r"Data_netWorthTrend\s*=\s*(\[.*?\]);", text, re.S)
    if not m:
        return None
    trend = json.loads(m.group(1))
    rets = [p.get("equityReturn") for p in trend if isinstance(p.get("equityReturn"), (int, float))]
    navs = [p.get("y") for p in trend if isinstance(p.get("y"), (int, float))]
    out = {}
    for label, n in VOL_WINDOWS.items():
        rs = rets[-n:]
        nv = navs[-(n + 1):]
        if len(rs) < VOL_MIN_RETS[label] or len(nv) < 2 or not nv[0]:
            continue
        mean = sum(rs) / len(rs)
        var = sum((x - mean) ** 2 for x in rs) / (len(rs) - 1)
        vol = round(math.sqrt(var) * math.sqrt(250), 2)
        ret = round((nv[-1] / nv[0] - 1) * 100, 2)
        ratio = round(ret / vol, 2) if vol > 0 else None
        out[label] = {"ret": ret, "vol": vol, "ratio": ratio, "points": len(rs)}
    return out or None


async def fetch_vol_windows(code: str):
    text = await fetch_pingzhong_text(code)
    # regex/JSON over the full NAV history: keep it off the event loop
    return await asyncio.to_thread(_parse_vol_windows, text)


async def compute_vol2(cat: str = "all") -> dict:
    """Volatility + return/volatility-ratio boards (v1.4.0): pool = union
    of the top-30 funds of each returns period (daily/1w/1m/3m/6m/1y/ytd),
    deduplicated; each pool fund gets per-window (1m/3m/6m/1y) ret/vol/ratio
    metrics. Sorting/filtering happens per request in the endpoints, so the
    cached payload keeps every pool fund that has at least one window.
    cat restricts the pool to one fund category's returns board (v1.4.0)."""
    ret_key = "returns_all" if cat == "all" else f"returns_{cat}"
    payload, _stale = await _get_or_compute(
        ret_key, RETURNS_TTL, lambda cat=cat: _compute_returns_payload(cat))
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
                w = await fetch_vol_windows(code)
            except Exception:
                w = None
        if not w:
            return None
        r = entry["row"]
        return {"code": code, "name": r["name"], "w": w,
                "y1": r["y1"], "periods_hit": entry["periods_hit"]}

    results = await asyncio.gather(*(one(c, e) for c, e in pool.items()))
    items = [x for x in results if x]
    return {"updated_at": now_iso(), "cat": cat, "total": len(pool),
            "pool_size": len(pool), "items": items}


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
_RANK_ETA = {"returns_all": 100, "vol_top_v2": 200, "qdii_us_v3": 240,
             "returns_gp": 60, "returns_hh": 80, "returns_zq": 60,
             "returns_zs": 60, "returns_qdii": 60, "returns_fof": 60,
             "vol_gp_v1": 200, "vol_hh_v1": 200, "vol_zq_v1": 200,
             "vol_zs_v1": 200, "vol_qdii_v1": 200, "vol_fof_v1": 200,
             "vol3_top": 200, "vol3_gp": 200, "vol3_hh": 200, "vol3_zq": 200,
             "vol3_zs": 200, "vol3_qdii": 200, "vol3_fof": 200}
# In-memory per-board compute state: {"state": "computing", "started_at": epoch}
_rank_state: dict = {}
_rank_tasks: dict = {}


def _rank_mark_computing(key: str):
    _rank_state[key] = {"state": "computing", "started_at": time.time()}


def _rank_mark_idle(key: str):
    _rank_state[key] = {"state": "idle", "started_at": None}


async def _compute_returns_payload(cat: str = "all") -> dict:
    rows = await fetch_returns_all(cat)
    return {"rows": rows, "updated_at": now_iso(), "cat": cat}


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
        log_error(f"rank:{key}", f"background compute failed: {e!r}")
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
    ("vol3_top", VOL_TTL, compute_vol2),
    ("qdii_us_v3", QDII_US_TTL, compute_qdii_us),
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
                    log_error(f"rank-warm:{key}",
                              "compute failed, kept stale cache")
                    print(f"[rank-warm] {key}: compute failed, kept stale cache ({dt:.1f}s)", flush=True)
                else:
                    print(f"[rank-warm] {key}: done in {dt:.1f}s", flush=True)
            except Exception as e:
                log_error(f"rank-warm:{key}", repr(e))
                print(f"[rank-warm] {key}: failed: {e!r}", flush=True)
        _wstat("rank_warm", last_run=time.time(),
               last_result="hourly board check done",
               next_run=time.time() + 3600)
        await asyncio.sleep(3600)


# ---------------------------------------------------------------- ranking prewarm backfill (v1.4.2)
# Funds shown in the rankings get the same data treatment as watchlist
# funds, backfilled slowly in the background: holdings, NAV, stock
# position, fund type, daily klines, β calibration (credibility) and
# today's minute trends — so a customer who adds a ranking fund to their
# pool sees valuation + credibility immediately. Risk controls: strictly
# sequential (one fund at a time), rate-limited by PREWARM_PER_MIN
# (default 1/min), per-fund failure backoff, and the worker pauses
# 30 min after 3 consecutive upstream failures so it can never hammer
# Eastmoney into throttling the server IP.
PREWARM_SYNC_INTERVAL = 600      # re-scan ranking caches for new entrants
PREWARM_MAX_ATTEMPTS = 10        # -> status 'failed' (retried daily)
PREWARM_FAIL_PAUSE = 1800        # pause after consecutive failures
_prewarm_paused_until = 0.0
_prewarm_consec_fail = 0


def _prewarm_pool_codes() -> set:
    """Union of fund codes currently shown across all ranking boards,
    derived from cached board payloads only (no upstream calls):
    returns boards contribute each period's top-30 per category,
    vol/ratio boards their whole pool, qdii-us its top-30 list."""
    codes = set()
    with db() as conn:
        rows = conn.execute("SELECT key, payload FROM rank_cache").fetchall()
    for r in rows:
        key = r["key"]
        try:
            payload = json.loads(r["payload"])
        except Exception:
            continue
        if key.startswith("returns_"):
            all_rows = payload.get("rows") or []
            for field in PERIOD_FIELDS.values():
                top = sorted(all_rows,
                             key=lambda x: (x.get(field) is None, -(x.get(field) or 0)))[:30]
                codes.update(x["code"] for x in top if x.get("code"))
        elif key.startswith("vol3_") or key == "qdii_us_v3":
            codes.update(x["code"] for x in (payload.get("items") or []) if x.get("code"))
    return codes


def _prewarm_sync_pool() -> int:
    """Insert ranking-pool codes missing from prewarm_queue as pending.
    New entrants get priority 1 so they jump ahead of the initial
    backlog (priority 0); existing rows (incl. done) are untouched, and
    funds that leave the rankings keep their queued/done state."""
    codes = _prewarm_pool_codes()
    if not codes:
        return 0
    now = time.time()
    with db() as conn:
        have = {r[0] for r in conn.execute("SELECT fund_code FROM prewarm_queue").fetchall()}
        new = [c for c in codes if c not in have]
        if not new:
            return 0
        prio = 1 if have else 0
        conn.executemany(
            "INSERT OR IGNORE INTO prewarm_queue(fund_code, status, priority, attempts, updated_at) "
            "VALUES(?, 'pending', ?, 0, ?)",
            [(c, prio, now) for c in new])
    return len(new)


def _prewarm_counts() -> dict:
    with db() as conn:
        rows = conn.execute(
            "SELECT status, COUNT(*) AS n FROM prewarm_queue GROUP BY status").fetchall()
    counts = {"done": 0, "pending": 0, "failed": 0}
    for r in rows:
        counts[r["status"]] = r["n"]
    counts["total"] = sum(counts.values())
    return counts


def _prewarm_next_code() -> Optional[str]:
    """Highest-priority pending fund whose failure backoff has elapsed.
    Backoff grows with attempts (60s x attempts, cap 30 min); 'failed'
    funds become eligible again after a day."""
    now = time.time()
    with db() as conn:
        rows = conn.execute(
            "SELECT fund_code, status, attempts, updated_at FROM prewarm_queue "
            "WHERE status IN ('pending', 'failed') "
            "ORDER BY priority DESC, attempts ASC, updated_at ASC").fetchall()
    for r in rows:
        if r["status"] == "failed":
            if now - r["updated_at"] >= 86400:
                return r["fund_code"]
            continue
        if now - r["updated_at"] >= min(1800, 60 * r["attempts"]):
            return r["fund_code"]
    return None


async def _prewarm_one(code: str):
    """Warm every cache a watchlist fund would get, for one ranking fund.
    Raises on upstream failure (caller counts it); β row existence is
    the done criterion (n=0 negative-cache rows count as done)."""
    hold = await fetch_holdings(code)
    if hold.get("stale") and not hold["holdings"]:
        raise RuntimeError(f"holdings unavailable for {code}")
    await fetch_nav(code)               # nav_cache (raises 502 if upstream down & no cache)
    await fetch_stock_position(code)    # fund_stock_position (never raises)
    ftype = await fetch_fund_type(code) or ""   # fund_type_cache
    # An n=0 β row only counts as done for funds that are definitively
    # skipped (MRF / bond / money / no stock holdings). For an equity
    # fund, n=0 usually means daily klines failed upstream (throttle),
    # so treat it as a failure and retry with backoff instead of
    # marking the fund done with no credibility forever.
    coverage = sum(h["weight"] for h in hold["holdings"])
    definitive_skip = (_is_mrf_code(code) or "债券" in ftype
                       or "货币" in ftype or coverage <= 0)

    def _beta_usable(row):
        return (bool(row) and time.time() - row["updated_at"] < BETA_TTL
                and (row["n"] > 0 or definitive_skip))

    if not _beta_usable(_beta_row(code)):
        t = _beta_tasks.get(code)
        if t is not None and not t.done():
            await t  # a lazy β compute is already running; don't duplicate it
        if not _beta_usable(_beta_row(code)):
            await compute_fund_beta(code)   # fills stock_daily_cache + fund_beta
    # Today's minute trends for the curve (best effort; per-secid
    # backoffs inside fetch_holding_trends keep upstream load bounded).
    mmap = await fetch_stock_market_map(code)
    date_str = datetime.now(CN_TZ).strftime("%Y-%m-%d")
    for h in hold["holdings"]:
        if h["weight"]:
            await fetch_holding_trends(h["code"], mmap, date_str, h.get("name"))
    row = _beta_row(code)
    if row is None:
        raise RuntimeError(f"beta row missing after compute for {code}")
    if row["n"] == 0 and not definitive_skip:
        raise RuntimeError(f"beta pairs empty for {code} (upstream data missing?)")


async def _prewarm_loop():
    """Slow sequential backfill of ranking-fund data (v1.4.2). One fund
    at a time at PREWARM_PER_MIN (default 1/min) + jitter; pauses 30 min
    after 3 consecutive failures. Never blocks estimates/first paint."""
    global _prewarm_paused_until, _prewarm_consec_fail
    if not PREWARM_ENABLED:
        print("[prewarm] disabled (PREWARM_ENABLED=0)", flush=True)
        return
    await asyncio.sleep(20)
    interval = 60.0 / PREWARM_PER_MIN
    last_sync = 0.0
    queue_empty = True  # until the first pool sync lands, re-check every minute
    while True:
        try:
            if time.time() < _prewarm_paused_until:
                await asyncio.sleep(min(60, _prewarm_paused_until - time.time()))
                continue
            sync_gap = 60 if queue_empty else PREWARM_SYNC_INTERVAL
            if time.time() - last_sync >= sync_gap:
                last_sync = time.time()
                n_new = await asyncio.to_thread(_prewarm_sync_pool)
                if n_new:
                    queue_empty = False
                    print(f"[prewarm] pool sync: {n_new} new funds queued", flush=True)
            code = await asyncio.to_thread(_prewarm_next_code)
            if code is None:
                _wstat("prewarm", current=None, last_run=time.time(),
                       last_result="queue empty / all in backoff")
                await asyncio.sleep(60)
                continue
            _wstat("prewarm", current=code)
            try:
                await _prewarm_one(code)
                _prewarm_consec_fail = 0
                with db() as conn:
                    conn.execute(
                        "UPDATE prewarm_queue SET status='done', updated_at=? WHERE fund_code=?",
                        (time.time(), code))
                _wstat("prewarm", current=None, last_run=time.time(),
                       last_result=f"done {code}")
            except Exception as e:
                _prewarm_consec_fail += 1
                log_error("prewarm", f"{code}: {e!r}")
                _wstat("prewarm", current=None, last_run=time.time(),
                       last_result=f"failed {code}: {e!r}")
                with db() as conn:
                    conn.execute(
                        "UPDATE prewarm_queue SET attempts=attempts+1, updated_at=?, "
                        "status=CASE WHEN attempts+1>=? THEN 'failed' ELSE 'pending' END "
                        "WHERE fund_code=?",
                        (time.time(), PREWARM_MAX_ATTEMPTS, code))
                print(f"[prewarm] {code} failed "
                      f"({_prewarm_consec_fail} consecutive): {e!r}", flush=True)
                if _prewarm_consec_fail >= 3:
                    _prewarm_paused_until = time.time() + PREWARM_FAIL_PAUSE
                    _prewarm_consec_fail = 0
                    print(f"[prewarm] pausing {PREWARM_FAIL_PAUSE}s after "
                          f"consecutive failures", flush=True)
            counts = await asyncio.to_thread(_prewarm_counts)
            print(f"[prewarm] done {counts['done']}/{counts['total']} "
                  f"pending {counts['pending']} failed {counts['failed']}", flush=True)
        except Exception as e:
            log_error("prewarm", f"loop: {e!r}")
            _wstat("prewarm", current=None, last_run=time.time(),
                   last_result=f"loop failed: {e!r}")
            print(f"[prewarm] loop failed: {e!r}", flush=True)
        await asyncio.sleep(interval * random.uniform(0.9, 1.1))


@app.get("/api/admin/prewarm")
def admin_prewarm(session: Optional[str] = Cookie(default=None)):
    require_admin(session)
    counts = _prewarm_counts()
    paused = time.time() < _prewarm_paused_until
    return {"enabled": PREWARM_ENABLED, "per_min": PREWARM_PER_MIN,
            "total": counts["total"], "done": counts["done"],
            "pending": counts["pending"], "failed": counts["failed"],
            "paused": paused,
            "paused_until": (datetime.fromtimestamp(
                _prewarm_paused_until, timezone.utc).isoformat() if paused else None)}


_BOARD_LABELS = {
    "returns_all": "涨幅榜（全部）", "returns_gp": "涨幅榜（股票）",
    "returns_hh": "涨幅榜（混合）", "returns_zq": "涨幅榜（债券）",
    "returns_zs": "涨幅榜（指数）", "returns_qdii": "涨幅榜（QDII）",
    "returns_fof": "涨幅榜（FOF）",
    "vol3_top": "波动率/收益波动比池（全部）", "vol3_gp": "波动率池（股票）",
    "vol3_hh": "波动率池（混合）", "vol3_zq": "波动率池（债券）",
    "vol3_zs": "波动率池（指数）", "vol3_qdii": "波动率池（QDII）",
    "vol3_fof": "波动率池（FOF）",
    "qdii_us_v3": "美股占比榜",
}


def _board_ttl(key: str) -> Optional[int]:
    if key.startswith("returns_"):
        return RETURNS_TTL
    if key.startswith("vol3_"):
        return VOL_TTL
    if key == "qdii_us_v3":
        return QDII_US_TTL
    return None


_FILE_TREE_SKIP = {"venv", "__pycache__", ".git"}
_FILE_TREE_MAX = 500    # entry cap so the payload can never explode


def _system_files() -> dict:
    """Directory tree of the system dir (BASE_DIR) with real byte sizes
    for the admin 系统状态 page (v1.4.6). Skips venv/__pycache__/.git,
    caps at _FILE_TREE_MAX entries. Dir size = recursive total of the
    entries included. Returns {tree, total_entries, truncated}."""
    counter = {"n": 0, "truncated": False}

    def walk(path: Path, rel: str) -> dict:
        node = {"name": path.name, "path": rel, "is_dir": True,
                "size": 0, "children": []}
        try:
            entries = sorted(path.iterdir(),
                             key=lambda p: (not p.is_dir(), p.name.lower()))
        except OSError:
            return node
        for p in entries:
            if counter["n"] >= _FILE_TREE_MAX:
                counter["truncated"] = True
                break
            if p.name in _FILE_TREE_SKIP:
                continue
            counter["n"] += 1
            child_rel = f"{rel}/{p.name}" if rel else p.name
            if p.is_dir() and not p.is_symlink():
                child = walk(p, child_rel)
                node["children"].append(child)
                node["size"] += child["size"]
            else:
                try:
                    size = p.stat().st_size
                except OSError:
                    size = 0
                node["children"].append({
                    "name": p.name, "path": child_rel, "is_dir": False,
                    "size": size, "children": []})
                node["size"] += size
        return node

    tree = walk(BASE_DIR, "")
    tree["name"] = BASE_DIR.name
    return {"tree": tree, "total_entries": counter["n"],
            "truncated": counter["truncated"]}


@app.get("/api/admin/data-sources")
def admin_data_sources(session: Optional[str] = Cookie(default=None)):
    """Upstream interface catalog with last-call status for the admin
    数据接口 page (v1.4.6; grouped by data type in v1.4.7). Stats are
    in-memory since process start. Fuyao entries show 未配置 when no
    FUYAO_API_KEY is set. `sources` (flat) is kept for compatibility."""
    require_admin(session)
    by_key = {}
    items = []
    for key, name, provider, purpose, role in _SOURCE_CATALOG:
        st = _source_stats.get(key)
        if key.startswith("fuyao_") and not FUYAO_ENABLED:
            status, last_time, last_error = "未配置", None, None
            ok_n = fail_n = 0
        elif not st or (not st["ok"] and not st["fail"]):
            status, last_time, last_error = "无记录", None, None
            ok_n = fail_n = 0
        else:
            ok_n, fail_n = st["ok"], st["fail"]
            if (st["last_ok"] or 0) >= (st["last_fail"] or 0):
                status, last_time, last_error = "成功", _iso(st["last_ok"]), None
            else:
                status, last_time = "失败", _iso(st["last_fail"])
                last_error = st["last_error"]
        item = {
            "key": key, "name": name, "provider": provider,
            "purpose": purpose, "role": role, "last_status": status,
            "last_time": last_time, "last_error": last_error,
            "success_count": ok_n, "fail_count": fail_n,
        }
        by_key[key] = item
        items.append(item)
    groups = []
    for gname, keys in _SOURCE_GROUPS:
        members = [by_key[k] for k in keys if k in by_key]
        # group status over sources that actually ran （未配置 excluded):
        # any success -> 正常； failures only -> 异常； nothing ran -> 无记录
        ran = [m for m in members if m["last_status"] in ("成功", "失败")]
        if any(m["last_status"] == "成功" for m in ran):
            gstatus = "正常"
        elif ran:
            gstatus = "异常"
        else:
            gstatus = "无记录"
        groups.append({
            "group": gname,
            "has_fallback": any(m["role"] == "兜底" for m in members),
            "group_status": gstatus,
            "sources": members,
        })
    return {"version": APP_VERSION, "now": _iso(time.time()),
            "data_flow_ok": _data_flow_ok(), "fuyao_configured": FUYAO_ENABLED,
            "sources": items, "groups": groups}


@app.get("/api/admin/system-status")
def admin_system_status(session: Optional[str] = Cookie(default=None)):
    """Full system visibility for the admin 系统状态 page (v1.4.3):
    DB fund-data stats, background worker state, upcoming scheduled
    tasks, and the last 24h of recorded system errors."""
    require_admin(session)
    now = time.time()
    today_cn = datetime.now(CN_TZ).strftime("%Y-%m-%d")
    with db() as conn:
        def one(sql, args=()):
            r = conn.execute(sql, args).fetchone()
            return r[0] if r and r[0] is not None else 0

        stats = {
            "users": one("SELECT COUNT(*) FROM users"),
            "watchlist_funds": one(
                "SELECT COUNT(DISTINCT fund_code) FROM user_funds"),
            "watchlist_rows": one("SELECT COUNT(*) FROM user_funds"),
            "holdings_cached": one("SELECT COUNT(*) FROM holdings_cache"),
            "nav_cached": one("SELECT COUNT(*) FROM nav_cache"),
            "beta_ok": one("SELECT COUNT(*) FROM fund_beta WHERE n>0"),
            "beta_skip": one("SELECT COUNT(*) FROM fund_beta WHERE n=0"),
            "beta_last_at": _iso(one(
                "SELECT MAX(updated_at) FROM fund_beta")),
            "stock_position_cached": one(
                "SELECT COUNT(*) FROM fund_stock_position"),
            "fund_type_cached": one("SELECT COUNT(*) FROM fund_type_cache"),
            "daily_stocks": one(
                "SELECT COUNT(DISTINCT secid) FROM stock_daily_cache"),
            "daily_rows": one("SELECT COUNT(*) FROM stock_daily_cache"),
            "trends_rows": one("SELECT COUNT(*) FROM stock_trends_cache"),
            "trends_today": one(
                "SELECT COUNT(*) FROM stock_trends_cache WHERE date=?",
                (today_cn,)),
            "industry_cached": one(
                "SELECT COUNT(*) FROM stock_industry WHERE industry IS NOT NULL"),
            "prewarm": _prewarm_counts(),
        }
        board_rows = conn.execute(
            "SELECT key, fetched_at FROM rank_cache ORDER BY key").fetchall()
        err_groups = conn.execute(
            "SELECT source, COUNT(*) AS n, MAX(ts) AS last_ts "
            "FROM system_errors WHERE ts>=? GROUP BY source "
            "ORDER BY n DESC, last_ts DESC", (now - 86400,)).fetchall()
        err_latest = conn.execute(
            "SELECT ts, source, message FROM system_errors WHERE ts>=? "
            "ORDER BY ts DESC LIMIT 50", (now - 86400,)).fetchall()

    # Boards: every cached board + the three always-warmed ones even
    # when not yet computed.
    boards = []
    seen_keys = set()
    for r in board_rows:
        key, fetched_at = r["key"], r["fetched_at"]
        seen_keys.add(key)
        ttl = _board_ttl(key)
        nxt = None
        if ttl:
            try:
                nxt = (datetime.fromisoformat(fetched_at)
                       + timedelta(seconds=ttl)).isoformat()
            except Exception:
                nxt = None
        boards.append({
            "key": key, "label": _BOARD_LABELS.get(key, key),
            "ready": True,
            "fresh": _cache_fresh(fetched_at, ttl) if ttl else None,
            "fetched_at": fetched_at, "next_refresh": nxt,
            "computing": (_rank_state.get(key) or {}).get("state") == "computing",
        })
    for key, _ttl, _compute in _RANK_BOARDS:
        if key not in seen_keys:
            boards.append({
                "key": key, "label": _BOARD_LABELS.get(key, key),
                "ready": False, "fresh": False, "fetched_at": None,
                "next_refresh": None,
                "computing": (_rank_state.get(key) or {}).get("state") == "computing",
            })

    # Workers (in-memory status + derived values).
    def _w(name):
        st = _worker_status.get(name) or {}
        return {"last_run": _iso(st.get("last_run")),
                "last_result": st.get("last_result"),
                "next_run": _iso(st.get("next_run"))}

    counts = stats["prewarm"]
    paused = now < _prewarm_paused_until
    eta_seconds = None
    if PREWARM_ENABLED and counts["pending"] > 0:
        eta_seconds = counts["pending"] / PREWARM_PER_MIN * 60
        if paused:
            eta_seconds += max(0.0, _prewarm_paused_until - now)
    pw = _w("prewarm")
    pw.update({
        "enabled": PREWARM_ENABLED, "per_min": PREWARM_PER_MIN,
        "current": (_worker_status.get("prewarm") or {}).get("current"),
        "counts": counts, "paused": paused,
        "paused_until": _iso(_prewarm_paused_until) if paused else None,
        "eta_seconds": int(eta_seconds) if eta_seconds is not None else None,
        "eta_at": _iso(now + eta_seconds) if eta_seconds is not None else None,
        "next_code": _prewarm_next_code(),
    })
    backups = _list_backups()
    bk = _w("backup")
    bk["last_backup_at"] = backups[0]["created_at"] if backups else None
    bt = _w("beta")
    bt["last_beta_at"] = stats["beta_last_at"]
    workers = {
        "prewarm": pw,
        "quote_warm": _w("quote_warm"),
        "trends_warm": _w("trends_warm"),
        "rank_warm": _w("rank_warm"),
        "backup": bk,
        "beta": bt,
    }

    # Upcoming tasks, soonest first.
    now_cn = datetime.now(CN_TZ)
    next_midnight = (now_cn + timedelta(days=1)).replace(
        hour=0, minute=0, second=0, microsecond=0)
    next_beta = now_cn.replace(hour=15, minute=35, second=0, microsecond=0)
    if next_beta <= now_cn:
        next_beta += timedelta(days=1)
    next_tasks = []
    if PREWARM_ENABLED:
        next_tasks.append({
            "name": "排行榜预热补全",
            "at": _iso(_prewarm_paused_until) if paused else _iso(now),
            "detail": ("暂停中，恢复后继续" if paused else
                       (f"下一只 {pw['next_code']}" if pw["next_code"]
                        else "队列已空/等待退避")),
        })
    next_tasks.append({"name": "每日自动备份",
                       "at": next_midnight.astimezone(timezone.utc).isoformat(),
                       "detail": "每天 00:00（北京时间），保留最近 10 份"})
    next_tasks.append({"name": "β 每日重算",
                       "at": next_beta.astimezone(timezone.utc).isoformat(),
                       "detail": "每天 15:35（北京时间）重算自选基金 β/可信度"})
    for wname, label in (("quote_warm", "行情预热"), ("trends_warm", "分时预热"),
                         ("rank_warm", "榜单预热检查")):
        nxt_run = workers[wname]["next_run"]
        if nxt_run:
            next_tasks.append({"name": label, "at": nxt_run, "detail": "后台循环"})
    for b in boards:
        if b["next_refresh"]:
            next_tasks.append({"name": f"榜单刷新 · {b['label']}",
                               "at": b["next_refresh"],
                               "detail": "缓存到期后后台重算"})
    next_tasks.sort(key=lambda t: t["at"] or "")

    errors = {
        "total_24h": sum(g["n"] for g in err_groups),
        "groups": [{"source": g["source"], "count": g["n"],
                    "last_at": _iso(g["last_ts"])} for g in err_groups],
        "latest": [{"ts": _iso(e["ts"]), "source": e["source"],
                    "message": e["message"]} for e in err_latest],
    }
    return {"version": APP_VERSION, "now": _iso(now), "stats": stats,
            "boards": boards, "workers": workers,
            "next_tasks": next_tasks, "errors": errors,
            "files": _system_files()}


@app.get("/api/rank/returns")
async def rank_returns(period: str = "1y", cat: str = "all",
                       session: Optional[str] = Cookie(default=None)):
    current_user_id(session)
    if period not in PERIOD_FIELDS:
        raise HTTPException(400, "period 不支持")
    if cat not in RANK_CATS:
        cat = "all"
    key = "returns_all" if cat == "all" else f"returns_{cat}"
    got = await _board_payload(key, RETURNS_TTL,
                               lambda cat=cat: _compute_returns_payload(cat))
    if got is None:
        return _warming_response(key)
    payload, stale = got
    field = PERIOD_FIELDS[period]
    rows = sorted(payload["rows"], key=lambda r: (r[field] is None, -(r[field] or 0)))
    items = []
    for i, r in enumerate(rows[:30]):
        items.append({"rank": i + 1, **{k: r[k] for k in
                      ("code", "name", "date", "nav", "daily", "w1", "m1", "m3", "m6", "y1", "ytd")}})
    return {"period": period, "cat": cat, "updated_at": payload["updated_at"],
            "total": len(payload["rows"]), "stale": stale, "items": items}


@app.get("/api/rank/qdii-us")
async def rank_qdii_us(session: Optional[str] = Cookie(default=None)):
    current_user_id(session)
    got = await _board_payload("qdii_us_v3", QDII_US_TTL, compute_qdii_us)
    if got is None:
        return _warming_response("qdii_us_v3")
    payload, stale = got
    items = [{"rank": i + 1, **x} for i, x in enumerate(payload["items"])]
    return {"updated_at": payload["updated_at"], "total": payload["total"], "stale": stale,
            "excluded_low_coverage": payload.get("excluded_low_coverage", 0), "items": items}


def _vol2_key(cat: str) -> str:
    return "vol3_top" if cat == "all" else f"vol3_{cat}"


async def _vol2_board(cat: str, window: str, sort_field: str, reverse: bool):
    """Shared vol/ratio board logic (v1.4.0): serve the vol2_<cat> cache,
    flatten the chosen window's metrics, sort, top 30. Returns a warming
    JSONResponse when the cache is not ready yet."""
    if window not in VOL_WINDOWS:
        window = "1y"
    key = _vol2_key(cat)
    got = await _board_payload(key, VOL_TTL,
                               lambda cat=cat: compute_vol2(cat))
    if got is None:
        return _warming_response(key)
    payload, stale = got
    rows = []
    for x in payload["items"]:
        m = (x.get("w") or {}).get(window)
        if not m or m.get(sort_field) is None:
            continue
        rows.append({"code": x["code"], "name": x["name"],
                     "vol": m["vol"], "ret": m["ret"], "ratio": m["ratio"],
                     "y1": x.get("y1"), "periods_hit": x.get("periods_hit")})
    rows.sort(key=lambda r: r[sort_field], reverse=reverse)
    items = [{"rank": i + 1, **r} for i, r in enumerate(rows[:30])]
    return {"cat": payload.get("cat", cat), "window": window,
            "updated_at": payload["updated_at"], "total": payload["total"],
            "pool_size": payload.get("pool_size", payload["total"]),
            "stale": stale, "items": items}


@app.get("/api/rank/vol")
@app.get("/api/rank/qdii-vol")  # legacy alias (pre-v1.2.0 path)
async def rank_vol(cat: str = "all", window: str = "1y",
                   session: Optional[str] = Cookie(default=None)):
    current_user_id(session)
    if cat not in RANK_CATS:
        cat = "all"
    # ascending: lower volatility ranks first (v1.4.0)
    return await _vol2_board(cat, window, "vol", reverse=False)


@app.get("/api/rank/ratio")
async def rank_ratio(cat: str = "all", window: str = "1y",
                     session: Optional[str] = Cookie(default=None)):
    current_user_id(session)
    if cat not in RANK_CATS:
        cat = "all"
    # descending: higher return-per-volatility ranks first (v1.4.0)
    return await _vol2_board(cat, window, "ratio", reverse=True)


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
