"""Startup smoke test: the service comes up in a throwaway copy.

Runs uvicorn against a temp copy of the repo (its own fresh fund.db),
so the real workdir database and the production VPS are never touched.
Checks: / -> 200, /api/config -> 200 with the current version,
/api/search without a session -> 401.
"""
import json
import shutil
import socket
import subprocess
import sys
import tempfile
import time
import unittest
import urllib.error
import urllib.request
from pathlib import Path

from tests._common import REPO_ROOT, app


def _free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _get(port, path):
    req = urllib.request.Request(f"http://127.0.0.1:{port}{path}")
    try:
        with urllib.request.urlopen(req, timeout=5) as resp:
            return resp.status, resp.read()
    except urllib.error.HTTPError as e:
        return e.code, e.read()


class TestSmoke(unittest.TestCase):
    def test_server_boots_and_serves(self):
        port = _free_port()
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "fund-valuation"
            shutil.copytree(
                REPO_ROOT, root,
                ignore=shutil.ignore_patterns(
                    ".git", ".venv", "__pycache__", "fund.db*"))
            proc = subprocess.Popen(
                [sys.executable, "-m", "uvicorn", "app:app",
                 "--host", "127.0.0.1", "--port", str(port)],
                cwd=root, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            try:
                deadline = time.time() + 60
                status = None
                while time.time() < deadline:
                    if proc.poll() is not None:
                        self.fail(f"uvicorn exited early with code {proc.returncode}")
                    try:
                        status, _ = _get(port, "/api/config")
                        break
                    except Exception:
                        time.sleep(0.5)
                self.assertEqual(status, 200, "server did not come up in 60s")

                status, body = _get(port, "/")
                self.assertEqual(status, 200)
                status, body = _get(port, "/api/config")
                self.assertEqual(status, 200)
                self.assertEqual(json.loads(body)["version"], app.APP_VERSION)
                status, _ = _get(port, "/api/search?q=%E5%8D%8E%E5%A4%8F")
                self.assertEqual(status, 401)
            finally:
                proc.terminate()
                try:
                    proc.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    proc.kill()


if __name__ == "__main__":
    unittest.main()
