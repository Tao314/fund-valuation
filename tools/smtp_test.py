#!/usr/bin/env python3
"""Standalone SMTP test for the fund-valuation deployment.

Usage:
    python3 tools/smtp_test.py <recipient_email> [--verbose]

Reads SMTP_HOST / SMTP_PORT / SMTP_USER / SMTP_PASS / SMTP_FROM /
SMTP_SSL from the environment. If SMTP_HOST is not set in the
environment, falls back to parsing the systemd drop-in used in
production: /etc/systemd/system/fund-valuation.service.d/smtp.conf

Prints each step's result. The password is never printed. With
--verbose, smtplib debug output is enabled, which DOES contain
base64-encoded credentials -- do not paste that output anywhere.
Exit code 0 on success.
"""
import argparse
import os
import re
import smtplib
import sys
from datetime import datetime
from email.message import EmailMessage
from pathlib import Path

SYSTEMD_CONF = Path("/etc/systemd/system/fund-valuation.service.d/smtp.conf")
EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
KEYS = ("SMTP_HOST", "SMTP_PORT", "SMTP_USER", "SMTP_PASS", "SMTP_FROM", "SMTP_SSL")


def _unquote(value: str) -> str:
    value = value.strip()
    if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
        return value[1:-1]
    return value


def load_systemd_conf(path: Path) -> dict:
    cfg = {}
    try:
        lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    except OSError:
        return cfg
    for line in lines:
        line = line.strip()
        if not line.startswith("Environment="):
            continue
        rest = line[len("Environment="):]
        # One assignment per line in our generated drop-in; also tolerate
        # systemd's space-separated quoted assignments on a single line.
        for key, val in re.findall(r"(\w+)=(\"[^\"]*\"|'[^']*'|\S+)", rest):
            if key in KEYS:
                cfg[key] = _unquote(val)
    return cfg


def load_config() -> dict:
    cfg = {k: os.environ.get(k, "").strip() for k in KEYS}
    if not cfg["SMTP_HOST"]:
        file_cfg = load_systemd_conf(SYSTEMD_CONF)
        for key in KEYS:
            if not cfg[key] and file_cfg.get(key):
                cfg[key] = file_cfg[key].strip()
    if not cfg["SMTP_FROM"]:
        cfg["SMTP_FROM"] = cfg["SMTP_USER"]
    return cfg


def main() -> int:
    parser = argparse.ArgumentParser(description="Send a test mail via the configured SMTP server.")
    parser.add_argument("recipient", help="recipient email address")
    parser.add_argument("--verbose", action="store_true",
                        help="enable smtplib debug output (contains encoded credentials; do not paste it)")
    args = parser.parse_args()

    recipient = args.recipient.strip()
    if not EMAIL_RE.match(recipient):
        print(f"错误：收件人邮箱格式不正确：{recipient}", file=sys.stderr)
        return 2

    cfg = load_config()
    host = cfg["SMTP_HOST"]
    if not host:
        print("错误：未配置 SMTP_HOST（环境变量和 systemd 配置文件里都没有）。", file=sys.stderr)
        print(f"请设置环境变量 SMTP_HOST/SMTP_PORT/SMTP_USER/SMTP_PASS/SMTP_FROM，"
              f"或检查 {SYSTEMD_CONF} 是否存在。", file=sys.stderr)
        return 2
    try:
        port = int(cfg["SMTP_PORT"] or 465)
    except ValueError:
        print(f"错误：SMTP_PORT 不是数字：{cfg['SMTP_PORT']!r}", file=sys.stderr)
        return 2
    user, password, sender = cfg["SMTP_USER"], cfg["SMTP_PASS"], cfg["SMTP_FROM"]
    if not sender:
        print("错误：未配置发件人（SMTP_FROM / SMTP_USER 都为空）。", file=sys.stderr)
        return 2
    ssl_env = cfg["SMTP_SSL"]
    use_ssl = (ssl_env == "1") if ssl_env else (port == 465)

    print(f"配置：host={host} port={port} ssl={'yes' if use_ssl else 'starttls'} "
          f"user={user or '(无)'} from={sender} password={'已设置(隐藏)' if password else '未设置'}")
    if args.verbose:
        print("警告：--verbose 输出包含 base64 编码的登录凭据，不要把输出粘贴到聊天或工单里！",
              file=sys.stderr)

    msg = EmailMessage()
    msg["Subject"] = "【基金自算估值】SMTP 测试"
    msg["From"] = sender
    msg["To"] = recipient
    msg.set_content(
        "这是一封 SMTP 测试邮件。\n\n"
        f"发送时间：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
        "收到此邮件说明发信配置可用。"
    )

    try:
        print(f"连接 {host}:{port} …")
        if use_ssl:
            server = smtplib.SMTP_SSL(host, port, timeout=15)
        else:
            server = smtplib.SMTP(host, port, timeout=15)
        with server as s:
            if args.verbose:
                s.set_debuglevel(1)
            if not use_ssl:
                print("STARTTLS …")
                s.starttls()
            if user:
                print(f"登录 {user} …")
                s.login(user, password)
                print("登录成功")
            print(f"发送测试邮件到 {recipient} …")
            s.send_message(msg)
        print("发送成功")
        return 0
    except Exception as e:
        print(f"失败：{type(e).__name__}: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
