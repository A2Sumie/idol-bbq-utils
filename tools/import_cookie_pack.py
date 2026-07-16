#!/usr/bin/env python3
"""Import a cookie pack (Netscape txt or JSON export) into per-platform Netscape jars.

Usage:
    python3 tools/import_cookie_pack.py PACK_FILE [--apply]
        [--instagram-out PATH] [--tiktok-out PATH]

Default mode is a dry-run summary. --apply writes the jars (with timestamped
backups, atomic replace, mode 600).

Routing:
    instagram.com                       -> instagram jar
    tiktok/musically/tiktokcdn/etc      -> tiktok jar
    google family (accounts.google.com,
      gstatic, googleapis, doubleclick) -> tiktok jar (TikTok Google-linked login;
                                           do NOT split these out)
    anything else                       -> reported as unassigned, never written
"""

import argparse
import json
import os
import re
import shutil
import sys
import tempfile
from datetime import datetime, timezone

NETSCAPE_HEADER = (
    "# Netscape HTTP Cookie File\n"
    "# https://curl.haxx.se/rfc/cookie_spec.html\n"
    "# This is a generated file! Do not edit.\n\n"
)

IG_SUFFIXES = ("instagram.com",)
TIKTOK_SUFFIXES = (
    "tiktok.com",
    "tiktokv.com",
    "tiktokcdn.com",
    "tiktokcdn-us.com",
    "tiktokd.org",
    "tiktokd.net",
    "tiktokw.us",
    "musical.ly",
    "muscdn.com",
)
GOOGLE_FAMILY_SUFFIXES = (
    "google.com",
    "googleapis.com",
    "gstatic.com",
    "googleusercontent.com",
    "doubleclick.net",
    "googlesyndication.com",
    "ggpht.com",
    "google.co.jp",
    "googleadservices.com",
    "googletagmanager.com",
    "google-analytics.com",
    "recaptcha.net",
)


def host_matches(domain: str, suffixes) -> bool:
    d = domain.lstrip(".").lower()
    return any(d == s or d.endswith("." + s) for s in suffixes)


def classify(domain: str) -> str:
    if host_matches(domain, IG_SUFFIXES):
        return "instagram"
    if host_matches(domain, TIKTOK_SUFFIXES) or host_matches(domain, GOOGLE_FAMILY_SUFFIXES):
        return "tiktok"
    return "unassigned"


def parse_netscape(text: str):
    cookies = []
    for line in text.splitlines():
        line = line.rstrip("\n")
        if not line or line.startswith("#"):
            continue
        parts = line.split("\t")
        if len(parts) < 7:
            continue
        domain, _flag, path, secure, expires, name, value = parts[:7]
        cookies.append(
            {
                "domain": domain,
                "path": path or "/",
                "secure": secure.upper() == "TRUE",
                "expires": int(expires) if re.fullmatch(r"-?\d+", expires or "") else 0,
                "name": name,
                "value": value,
                "httpOnly": False,
            }
        )
    return cookies


def parse_json_cookies(text: str):
    data = json.loads(text)
    if isinstance(data, dict):
        data = data.get("cookies") or data.get("Cookies") or []
    if not isinstance(data, list):
        raise ValueError("JSON cookie pack must be a list or contain a 'cookies' list")
    cookies = []
    for item in data:
        if not isinstance(item, dict) or "name" not in item or "domain" not in item:
            continue
        expires = item.get("expirationDate", item.get("expires", item.get("expiry", 0)))
        try:
            expires = int(float(expires or 0))
        except (TypeError, ValueError):
            expires = 0
        cookies.append(
            {
                "domain": str(item["domain"]),
                "path": str(item.get("path") or "/"),
                "secure": bool(item.get("secure", False)),
                "expires": expires,
                "name": str(item["name"]),
                "value": str(item.get("value", "")),
                "httpOnly": bool(item.get("httpOnly", False)),
            }
        )
    return cookies


def load_pack(path: str):
    text = open(path, "r", encoding="utf-8-sig").read()
    stripped = text.lstrip()
    if stripped.startswith("[") or stripped.startswith("{"):
        return parse_json_cookies(text)
    return parse_netscape(text)


def to_netscape(cookies) -> str:
    lines = [NETSCAPE_HEADER]
    for c in sorted(cookies, key=lambda c: (c["domain"], c["path"], c["name"])):
        domain = c["domain"]
        flag = "TRUE" if domain.startswith(".") else "FALSE"
        secure = "TRUE" if c["secure"] else "FALSE"
        lines.append(
            "\t".join(
                [domain, flag, c["path"], secure, str(c["expires"]), c["name"], c["value"]]
            )
            + "\n"
        )
    return "".join(lines)


def atomic_write(path: str, content: str, backup: bool = True):
    if backup and os.path.exists(path):
        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        shutil.copy2(path, f"{path}.bak-import-{ts}")
    directory = os.path.dirname(os.path.abspath(path)) or "."
    fd, tmp = tempfile.mkstemp(prefix=".cookie-import-", dir=directory)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            fh.write(content)
        os.chmod(tmp, 0o600)
        os.replace(tmp, path)
    except BaseException:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("pack", help="cookie pack file (Netscape txt or JSON export)")
    parser.add_argument("--instagram-out", default="assets/cookies/inscks0318.txt")
    parser.add_argument("--tiktok-out", default="assets/cookies/tcookies.txt")
    parser.add_argument("--apply", action="store_true", help="write the jars (default: dry-run)")
    args = parser.parse_args()

    cookies = load_pack(args.pack)
    if not cookies:
        print("error: no cookies parsed from pack", file=sys.stderr)
        return 2

    jars = {"instagram": [], "tiktok": [], "unassigned": []}
    seen = {"instagram": set(), "tiktok": set()}
    for cookie in cookies:
        jar = classify(cookie["domain"])
        if jar == "unassigned":
            jars["unassigned"].append(cookie)
            continue
        key = (cookie["domain"], cookie["path"], cookie["name"])
        if key in seen[jar]:
            continue
        seen[jar].add(key)
        jars[jar].append(cookie)

    for jar in ("instagram", "tiktok"):
        entries = jars[jar]
        names = sorted({c["name"] for c in entries})
        domains = sorted({c["domain"].lstrip(".") for c in entries})
        print(f"[{jar}] {len(entries)} cookies | domains: {', '.join(domains) or '-'}")
        print(f"  names: {', '.join(names[:20])}{' ...' if len(names) > 20 else ''}")
        google = [c for c in entries if host_matches(c['domain'], GOOGLE_FAMILY_SUFFIXES)]
        if jar == "tiktok" and google:
            print(f"  includes {len(google)} google-family cookies (kept for TikTok linked login)")
    if jars["unassigned"]:
        domains = sorted({c["domain"].lstrip(".") for c in jars["unassigned"]})
        print(f"[unassigned] {len(jars['unassigned'])} cookies NOT written | domains: {', '.join(domains)}")

    if not args.apply:
        print("dry-run only; re-run with --apply to write")
        return 0

    outputs = {"instagram": args.instagram_out, "tiktok": args.tiktok_out}
    for jar, out_path in outputs.items():
        if not jars[jar]:
            print(f"[{jar}] empty jar, leaving {out_path} untouched")
            continue
        atomic_write(out_path, to_netscape(jars[jar]))
        print(f"[{jar}] wrote {len(jars[jar])} cookies -> {out_path} (backup created)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
