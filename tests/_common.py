"""Shared helpers for the fund-valuation guardrail tests.

Importing ``app`` runs ``init_db()`` at module level, which creates
``fund.db`` next to ``app.py``. That file is git-ignored; this module
removes it again on interpreter exit when it did not exist before the
import, so running the suite never leaves a stray database behind.
DB-touching tests instead point ``app.DB_PATH`` at a temp file.
"""
import atexit
import os
import sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

_FUND_DB = REPO_ROOT / "fund.db"
_DB_EXISTED = _FUND_DB.exists()


def _cleanup_import_db():
    if not _DB_EXISTED:
        for suffix in ("", "-wal", "-shm"):
            try:
                (_FUND_DB.with_name(_FUND_DB.name + suffix)).unlink(missing_ok=True)
            except OSError:
                pass


atexit.register(_cleanup_import_db)

import app  # noqa: E402  (imported after sys.path setup on purpose)

CN_TZ = ZoneInfo("Asia/Shanghai")
ET_TZ = ZoneInfo("America/New_York")


def cn(year, month, day, hour=0, minute=0):
    """Asia/Shanghai-aware datetime for date-boundary tests."""
    return datetime(year, month, day, hour, minute, tzinfo=CN_TZ)


def et(year, month, day, hour=0, minute=0):
    """US Eastern-aware datetime for us_session tests."""
    return datetime(year, month, day, hour, minute, tzinfo=ET_TZ)


class FrozenDatetime(datetime):
    """datetime.now() pinned to a class-level instant (any tz arg).

    Patch ``app.datetime`` with this to run time-dependent estimate code
    at a fixed Beijing time in tests.
    """
    _frozen = None

    @classmethod
    def now(cls, tz=None):
        if cls._frozen is None:
            return super().now(tz)
        return cls._frozen if tz is None else cls._frozen.astimezone(tz)


def frozen_time(moment):
    """Context manager patching app.datetime so datetime.now() returns
    ``moment`` (converted to the requested tz)."""
    from unittest.mock import patch
    FrozenDatetime._frozen = moment
    return patch.object(app, "datetime", FrozenDatetime)


class FakeResp:
    """Minimal httpx.Response stand-in for _validate_upstream_payload."""

    def __init__(self, text="", content=None):
        self.text = text
        self.content = b"" if content is None else content

    def json(self):
        import json

        return json.loads(self.text)

    def raise_for_status(self):
        return None


__all__ = ["app", "cn", "et", "CN_TZ", "ET_TZ", "FakeResp", "REPO_ROOT"]
