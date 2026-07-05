"""ArchiveDB server - browse, search, read, and download translated novels.

This is intentionally kept as a single file because the deployment launches
this exact script. It is organised into clearly separated sections:

    1.  Configuration & environment loading
    2.  Flask app setup & startup warnings
    3.  Text utilities
    4.  Security / path safety
    5.  Atomic persistence (user data, custom metadata)
    6.  Auth (allowed e-mails, accounts, login gate, auto-ban)
    7.  Rate limiting & download accounting
    8.  Metadata loading & gallery assembly (cached)
    8b. Novelpia notice-image gallery (manifests + direct CDN URLs)
    9.  Reader pipeline (TOC, chapters, assets)
   10.  Telegram background client & streaming
   11.  API response helpers
   12.  Routes
"""

import asyncio
import collections
from collections import defaultdict, deque
import csv
import hashlib
import ipaddress
import json
import logging
import math
import os
import queue
import random
import re
import secrets
import smtplib
import threading
import time
import uuid
import zipfile
from datetime import date, timedelta
from email.message import EmailMessage
from functools import wraps
from html import escape
from urllib.parse import quote, unquote, urlsplit, urlunsplit

import requests
from flask import (
    Flask,
    Response,
    jsonify,
    redirect,
    render_template,
    request,
    send_file,
    session,
    url_for,
)
from jinja2 import ChainableUndefined
from werkzeug.security import generate_password_hash, check_password_hash
from telethon import TelegramClient, connection
from telethon.errors import AuthKeyUnregisteredError

# ====================================================================
# 1. CONFIGURATION & ENVIRONMENT LOADING
# ====================================================================

_BASE_DIR = os.path.dirname(os.path.abspath(__file__))
_STARTUP_WARNINGS = []

def _warn(message):
    _STARTUP_WARNINGS.append(message)

def _env_str(name, default=None):
    """Read a trimmed string environment variable with a fallback."""
    value = os.environ.get(name, "").strip()
    return value if value else default

def _env_int(name, default):
    raw = os.environ.get(name, "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        _warn(f"{name}={raw!r} is not an integer - using default {default}.")
        return default

# --- Secrets ----------------------------------------------------------------
FLASK_SECRET_KEY = _env_str("FLASK_SECRET_KEY")
if not FLASK_SECRET_KEY:
    raise RuntimeError(
        "FLASK_SECRET_KEY is not set. Generate one with "
        "`python3 -c \"import secrets; print(secrets.token_hex(32))\"` and "
        "export it before launching. Changing this value logs out all users."
    )

TELEGRAM_API_ID = _env_int("TELEGRAM_API_ID", 0)
TELEGRAM_API_HASH = _env_str("TELEGRAM_API_HASH")
TELEGRAM_PHONE = _env_str("TELEGRAM_PHONE")
TELEGRAM_CONFIGURED = bool(TELEGRAM_API_ID and TELEGRAM_API_HASH and TELEGRAM_PHONE)
if not TELEGRAM_CONFIGURED:
    _warn(
        "TELEGRAM_API_ID / TELEGRAM_API_HASH / TELEGRAM_PHONE are not all set - "
        "the Telegram client will not start and downloads stay disabled until "
        "all three are exported in the environment."
    )

# --- Email (SMTP) for verification codes ------------------------------------
SMTP_HOST = _env_str("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT = _env_int("SMTP_PORT", 587)
SMTP_USER = _env_str("SMTP_USER")              # sending address
SMTP_PASS = _env_str("SMTP_PASS")              # Gmail App Password (NOT account pw)
if not (SMTP_USER and SMTP_PASS):
    _warn("SMTP_USER/SMTP_PASS not set - verification codes will only print to the console.")

# ---- PROXY CONFIG (MTProto). Set USE_PROXY=1 plus MTPROXY_* in the env. ----
USE_PROXY = _env_str("USE_PROXY", "0") == "1"
MTPROXY_SERVER = _env_str("MTPROXY_SERVER")
MTPROXY_PORT = _env_int("MTPROXY_PORT", 443)
MTPROXY_SECRET = _env_str("MTPROXY_SECRET")
if USE_PROXY and not (MTPROXY_SERVER and MTPROXY_SECRET):
    _warn("USE_PROXY=1 but MTPROXY_SERVER/MTPROXY_SECRET are missing - connecting directly.")
    USE_PROXY = False

# --- Paths -------------------------------------------------------------------
SESSION_PATH = _env_str("SESSION_PATH", os.path.join(_BASE_DIR, "sigma_reverse_session"))
TRANSLATED_CSV_PATH = _env_str("TRANSLATED_CSV_PATH", os.path.join(_BASE_DIR, "uploaded_novels_tracker.csv"))
RAW_MASTER_CSV_PATH = _env_str("RAW_MASTER_CSV_PATH", "/home/ubuntu/master_library_index.csv")
LOCAL_OUTPUT_DIR = _env_str("LOCAL_OUTPUT_DIR", "/home/ubuntu/nvidia_chat_bot/output/")
META_DIR = _env_str("META_DIR", "/home/ubuntu/metadata/")

JSON_DB_PATH = os.path.join(META_DIR, "novels_full.json")
TITLES_EN_PATH = os.path.join(META_DIR, "titles_en.txt")
TAGS_EN_PATH = os.path.join(META_DIR, "tags_en.txt")
DESC_EN_PATH = os.path.join(META_DIR, "descriptions.txt")
CUSTOM_META_PATH = os.path.join(META_DIR, "custom_meta.json")
USER_DATA_PATH = os.path.join(META_DIR, "user_data.json")
COLLECTIONS_PATH = os.path.join(META_DIR, "collections.json")
LEGACY_BOOKMARKS_PATH = os.path.join(META_DIR, "bookmarks.json")
DOWNLOAD_ABUSE_LOG_PATH = _env_str("DOWNLOAD_ABUSE_LOG_PATH", os.path.join(META_DIR, "download_abuse.jsonl"))
DOWNLOAD_LOG_PATH = _env_str("DOWNLOAD_LOG_PATH", os.path.join(META_DIR, "download_log.jsonl"))
ALLOWED_EMAILS_PATH = _env_str("ALLOWED_EMAILS_PATH", os.path.join(META_DIR, "allowed_gmails.txt"))
USERS_PATH = _env_str("USERS_PATH", os.path.join(META_DIR, "users.json"))

# --- Access & limits ---------------------------------------------------------
ADMIN_EMAILS = [
    e.strip().lower()
    for e in _env_str("ADMIN_EMAILS", "").split(",")
    if e.strip()
]
if not ADMIN_EMAILS:
    _warn("ADMIN_EMAILS is not set - no account has admin access until it is exported.")

# Public contact shown in the DMCA modal; hidden there when unset.
DMCA_EMAIL = _env_str("DMCA_EMAIL")
DAILY_DOWNLOAD_LIMIT = _env_int("DAILY_DOWNLOAD_LIMIT", 10)
SESSION_LIFETIME_DAYS = _env_int("SESSION_LIFETIME_DAYS", 30)

# Account / verification settings
CODE_TTL_SECONDS = _env_int("CODE_TTL_SECONDS", 600)   # verification code valid 10 min
MAX_CODE_ATTEMPTS = _env_int("MAX_CODE_ATTEMPTS", 5)
MIN_PASSWORD_LEN = _env_int("MIN_PASSWORD_LEN", 8)

# Auto-ban / Multi-account configuration
AUTOBAN_IPS = [
    tok.strip().lower()
    for tok in _env_str("AUTOBAN_IPS", "").split(",")
    if tok.strip()
]

MAX_EMAILS_PER_IP      = _env_int("MAX_EMAILS_PER_IP", 0)                  # 0 = OFF; 1 = one email per IP
MULTI_ACCOUNT_ENFORCE = _env_str("MULTI_ACCOUNT_ENFORCE", "log").lower()  # log | remove_new | remove_all
IP_EMAIL_WINDOW_HRS    = _env_int("IP_EMAIL_WINDOW_HRS", 24)
IP_GROUP_IPV6_64       = _env_str("IP_GROUP_IPV6_64", "1") == "1"
IP_EMAIL_MAP_PATH      = _env_str("IP_EMAIL_MAP_PATH", os.path.join(META_DIR, "ip_email_map.json"))

# In-Memory Firewall Auto-ban Settings
MAX_REQUESTS_PER_WINDOW = 60      # more than 60 requests
RATE_WINDOW_SECONDS = 60          # within 60 seconds
BAN_SECONDS = 86400               # ban for 1 day

# Do not count images/assets toward IP auto-ban
AUTO_BAN_IGNORE_PREFIXES = (
    "/favicon.ico",
    "/static/",
)

AUTO_BAN_IGNORE_CONTAINS = (
    "/asset/",
    "/img/",
)

# Runtime memory states
ip_requests = defaultdict(deque)  # ip -> timestamps
banned_ips = {}                   # ip -> ban_until_timestamp
AUTO_BAN_LOCK = threading.Lock()

# --- Domain constants --------------------------------------------------------
CHAPTER_EXTENSIONS = (".html", ".xhtml", ".htm")
IMAGE_EXTENSIONS = (".jpg", ".jpeg", ".png", ".webp", ".gif")
ASSET_IMAGE_EXTENSIONS = IMAGE_EXTENSIONS + (".svg", ".bmp")
REWRITABLE_ASSET_EXTENSIONS = (".png", ".jpg", ".jpeg", ".gif", ".webp", ".svg", ".bmp", ".css")
NON_CHAPTER_FILES = {
    "nav.xhtml", "nav.html", "toc.xhtml", "toc.html", "titlepage.xhtml",
    "title.xhtml", "cover.xhtml", "cover.html", "copyright.xhtml", "copyright.html",
}

TOC_MISSING_PREFIX = "MISSING||"
MAX_GAP_FILL = 100  
DOWNLOAD_CHUNK_SIZE = 512 * 1024
VALID_READING_STATUSES = {"none", "want_to_read", "reading", "finished"}
LEGACY_UPLOAD_DATE = "2024-01-01"

# ====================================================================
# 2. FLASK APP SETUP & STARTUP WARNINGS
# ====================================================================
app = Flask(__name__)
logging.getLogger("werkzeug").setLevel(logging.ERROR)
app.jinja_env.undefined = ChainableUndefined
app.secret_key = FLASK_SECRET_KEY
app.permanent_session_lifetime = timedelta(days=SESSION_LIFETIME_DAYS)
app.config.update(
    SESSION_COOKIE_HTTPONLY=True,
    SESSION_COOKIE_SAMESITE="Lax",
    # The public site is HTTPS via Cloudflare Tunnel; set COOKIE_SECURE=0 only
    # for plain-HTTP local testing (login cookies are dropped otherwise).
    SESSION_COOKIE_SECURE=_env_str("COOKIE_SECURE", "1") == "1",
)

for _message in _STARTUP_WARNINGS:
    print(f"[CONFIG WARNING] {_message}")

def get_client_ip():
    """Extract real client IP considering Cloudflare Tunnel proxies."""
    ip = request.headers.get("CF-Connecting-IP")
    if not ip:
        forwarded = request.headers.get("X-Forwarded-For", "")
        if forwarded:
            ip = forwarded.split(",")[0].strip()
    if not ip:
        ip = request.remote_addr or ""
    return ip.strip().lower()

@app.before_request
def auto_ban_spammers():
    client_ip = get_client_ip()
    now = time.time()

    if not client_ip:
        return None

    user_email = (session.get("user_email") or "").strip().lower()

    # Admins bypass global auto-ban entirely
    if user_email in ADMIN_EMAILS:
        return None

    path = request.path or ""

    # Do not count image/static asset requests toward auto-ban.
    # Already-banned IPs are still blocked below.
    skip_counting = (
        path.startswith(AUTO_BAN_IGNORE_PREFIXES)
        or any(part in path for part in AUTO_BAN_IGNORE_CONTAINS)
    )

    with AUTO_BAN_LOCK:
        # If IP is already banned, block instantly
        ban_until = banned_ips.get(client_ip)

        if ban_until:
            if now < ban_until:
                return "Forbidden", 403
            else:
                # Ban expired
                banned_ips.pop(client_ip, None)
                ip_requests.pop(client_ip, None)

        # If this is an image/static asset request, do not count it
        if skip_counting:
            return None

        # Track requests in current time window
        q = ip_requests[client_ip]

        while q and q[0] < now - RATE_WINDOW_SECONDS:
            q.popleft()

        q.append(now)

        # If IP exceeds request limit, ban IP and remove logged-in email
        if len(q) > MAX_REQUESTS_PER_WINDOW:
            banned_ips[client_ip] = now + BAN_SECONDS
            ip_requests.pop(client_ip, None)

            removed = False

            # Only remove logged-in non-admin users. (Admins bypass above anyway)
            if user_email and user_email not in ADMIN_EMAILS:
                removed = remove_email_from_allowlist(user_email)

                # Kill current session immediately
                session.pop("user_email", None)

            print(
                f"[AUTO-BAN] ip={client_ip} banned_for={BAN_SECONDS}s "
                f"email={user_email or '-'} removed={removed}"
            )

            return "Forbidden", 403

    return None

# --- Universal Live Console Audit Logger --------------------------------------
_ACCESS_LOG_SKIP_PREFIXES = ("/favicon.ico",)

@app.after_request
def log_request(response):
    try:
        path = request.path

        # Suppress noisy logging for favicons and images/assets
        if path.startswith(_ACCESS_LOG_SKIP_PREFIXES) or any(part in path for part in AUTO_BAN_IGNORE_CONTAINS):
            return response

        client_ip = get_client_ip()
        now = time.time()

        # Do not print endless logs from already-banned IPs
        with AUTO_BAN_LOCK:
            ban_until = banned_ips.get(client_ip)
            if ban_until and now < ban_until:
                return response

        email = session.get("user_email", "") or "-"

        print(
            f"[ACCESS] email={email} ip={client_ip} "
            f"{request.method} {path} -> {response.status_code}"
        )

    except Exception:
        pass

    return response

# 'unsafe-inline' is required: all page CSS/JS is inline in the templates.
# img-src stays open because covers and notice images are hotlinked from
# arbitrary external hosts.
_CSP = (
    "default-src 'self'; "
    "script-src 'self' 'unsafe-inline'; "
    "style-src 'self' 'unsafe-inline'; "
    "img-src * data: blob:; "
    "font-src 'self' data:; "
    "connect-src 'self'; "
    "object-src 'none'; "
    "frame-ancestors 'none'; "
    "base-uri 'self'; "
    "form-action 'self'"
)

@app.after_request
def set_security_headers(response):
    response.headers.setdefault("X-Content-Type-Options", "nosniff")
    response.headers.setdefault("X-Frame-Options", "DENY")
    response.headers.setdefault("Referrer-Policy", "strict-origin-when-cross-origin")
    response.headers.setdefault("Content-Security-Policy", _CSP)
    return response

# ====================================================================
# 3. TEXT UTILITIES
# ====================================================================
_CJK_PATTERN = re.compile(r"[\u4e00-\u9fff\u3040-\u309f\u30a0-\u30ff\uac00-\ud7a3]")
_ALNUM_PATTERN = re.compile(r"[a-zA-Z0-9]")
_NATSORT_SPLIT = re.compile(r"(\d+)")

_ENGLISH_CHAR = re.compile(r"[A-Za-z0-9]")
_TRAILING_EN_PUNCT = set(" \t!?.,\u2026~-:;''\"`\u2019\u201d\u201c·*&")
_ID_PREFIX_PATTERN = re.compile(r"^\s*\[(\d+)\]\s*")  

def _top_level_paren_groups(text):
    groups, depth, start = [], 0, None
    for i, ch in enumerate(text):
        if ch == "(":
            if depth == 0:
                start = i
            depth += 1
        elif ch == ")" and depth > 0:
            depth -= 1
            if depth == 0 and start is not None:
                groups.append((start, i, text[start + 1:i]))
                start = None
    return groups

def extract_korean_block(filename):
    """Inner text of the parens holding the Korean title."""
    name = re.sub(r"\.epub\s*$", "", str(filename), flags=re.IGNORECASE).strip()
    groups = _top_level_paren_groups(name)
    if not groups:
        return ""
    for open_idx, _close, inner in groups:
        j = open_idx - 1
        while j >= 0 and name[j] in _TRAILING_EN_PUNCT:
            j -= 1
        if j >= 0 and _ENGLISH_CHAR.match(name[j]):
            return inner.strip()
    return groups[-1][2].strip()  

def extract_novel_id(filename):
    m = _ID_PREFIX_PATTERN.match(extract_korean_block(filename))
    return m.group(1) if m else ""

def extract_korean_name(filename):
    """Korean title with the optional '[id]' prefix stripped."""
    return _ID_PREFIX_PATTERN.sub("", extract_korean_block(filename)).strip()

def normalize_korean_key(text):
    """Whitespace-insensitive key for exact whole-name matching."""
    return re.sub(r"\s+", "", str(text)).lower()

def get_pure_cjk(text):
    """Keep only CJK characters - used as a fuzzy matching key."""
    return "".join(_CJK_PATTERN.findall(str(text)))

def get_pure_english(text):
    """Keep only ASCII alphanumerics, lowercased - fuzzy matching key."""
    return "".join(_ALNUM_PATTERN.findall(str(text))).lower()

def natural_sort_key(s):
    # Tuple tags keep comparisons type-stable when one name has digits where
    # another has letters (plain int vs str would raise TypeError).
    return [(0, int(part)) if part.isdigit() else (1, part.lower()) for part in _NATSORT_SPLIT.split(s)]

def to_int(value, default=0):
    """Best-effort int coercion for request parameters."""
    try:
        return int(value)
    except (TypeError, ValueError):
        return default

# ====================================================================
# 4. SECURITY / PATH SAFETY
# ====================================================================
def resolve_under(base_dir, relative_path):
    """Resolve relative_path strictly inside base_dir."""
    cleaned = unquote(str(relative_path)).replace("\\", "/").split("?")[0]
    candidate = os.path.realpath(os.path.join(base_dir, cleaned))
    base_real = os.path.realpath(base_dir)
    if candidate == base_real or candidate.startswith(base_real + os.sep):
        return candidate
    return None

# ====================================================================
# 5. ATOMIC PERSISTENCE (USER DATA, CUSTOM METADATA)
# ====================================================================
_USER_DATA_LOCK = threading.Lock()
_CUSTOM_META_LOCK = threading.Lock()
_COLLECTIONS_LOCK = threading.Lock()

def read_json_file(path, default):
    try:
        with open(path, "r", encoding="utf-8") as fh:
            return json.load(fh)
    except FileNotFoundError:
        return default
    except (json.JSONDecodeError, OSError) as exc:
        print(f"[WARN] Could not read {path}: {exc}")
        return default

def write_json_atomic(path, data, **dump_kwargs):
    """Write JSON via a temp file + rename so readers never see torn files."""
    directory = os.path.dirname(path) or "."
    os.makedirs(directory, exist_ok=True)
    tmp_path = f"{path}.tmp.{os.getpid()}.{threading.get_ident()}"
    with open(tmp_path, "w", encoding="utf-8") as fh:
        json.dump(data, fh, **dump_kwargs)
        fh.flush()
        os.fsync(fh.fileno())
    os.replace(tmp_path, path)

def _load_user_data_unlocked():
    data = read_json_file(USER_DATA_PATH, None)
    if data is not None:
        return data
    legacy = read_json_file(LEGACY_BOOKMARKS_PATH, None)
    if legacy is not None:
        migrated = {}
        for email, entry in legacy.items():
            if isinstance(entry, list):
                migrated[email] = {str(i): {"status": "want_to_read", "progress": 0} for i in entry}
            else:
                migrated[email] = entry
        try:
            write_json_atomic(USER_DATA_PATH, migrated)
        except OSError as exc:
            print(f"[WARN] Could not persist migrated user data: {exc}")
        return migrated
    return {}

def load_user_data():
    with _USER_DATA_LOCK:
        return _load_user_data_unlocked()

def save_user_data(data):
    with _USER_DATA_LOCK:
        write_json_atomic(USER_DATA_PATH, data)

def mutate_user_data(mutator):
    with _USER_DATA_LOCK:
        data = _load_user_data_unlocked()
        result = mutator(data)
        write_json_atomic(USER_DATA_PATH, data)
        return result

def load_custom_meta():
    return read_json_file(CUSTOM_META_PATH, {})

def save_custom_meta_entry(filename, entry):
    with _CUSTOM_META_LOCK:
        custom_meta = load_custom_meta()
        custom_meta[filename] = entry
        write_json_atomic(CUSTOM_META_PATH, custom_meta, indent=4, ensure_ascii=False)

# ===================== Collections =====================
def load_collections():
    with _COLLECTIONS_LOCK:
        return read_json_file(COLLECTIONS_PATH, {})

def save_collections(data):
    with _COLLECTIONS_LOCK:
        write_json_atomic(COLLECTIONS_PATH, data, ensure_ascii=False, indent=2)

def get_user_collections(email):
    return load_collections().get(email, [])

def collection_counts(email):
    counts = {}
    udata = load_user_data().get(email, {})
    for entry in udata.values():
        if isinstance(entry, dict):
            for cid in (entry.get("collections") or []):
                counts[cid] = counts.get(cid, 0) + 1
    return counts

# ====================================================================
# 6. AUTH (ALLOWED E-MAILS, ACCOUNTS, LOGIN GATE, AUTO-BAN)
# ====================================================================
_allowed_emails_cache = {
    "emails": set(),
    "mtime": None,
}
_ALLOWED_EMAILS_LOCK = threading.Lock()

def get_allowed_emails():
    with _ALLOWED_EMAILS_LOCK:
        try:
            mtime = os.path.getmtime(ALLOWED_EMAILS_PATH)
        except OSError as exc:
            print(f"[WARN] Allowed emails file not found/readable: {ALLOWED_EMAILS_PATH} ({exc})")
            _allowed_emails_cache["emails"] = set()
            _allowed_emails_cache["mtime"] = None
            return set()

        if _allowed_emails_cache["mtime"] == mtime:
            return _allowed_emails_cache["emails"]

        emails = set()
        try:
            with open(ALLOWED_EMAILS_PATH, "r", encoding="utf-8") as fh:
                for line in fh:
                    email = line.strip().lower()
                    if not email or line.startswith("#"):
                        continue
                    emails.add(email)
            _allowed_emails_cache["emails"] = emails
            _allowed_emails_cache["mtime"] = mtime
            return emails
        except OSError as exc:
            print(f"[WARN] Could not read allowed emails file: {ALLOWED_EMAILS_PATH} ({exc})")
            return _allowed_emails_cache["emails"]

# ---- Allowlist editing & auto-ban / multi-account ---------------------------
_ALLOWLIST_WRITE_LOCK = threading.Lock()

def remove_email_from_allowlist(email):
    """Atomically drop an email from the allowlist file. Returns True if removed."""
    email = (email or "").strip().lower()
    if not email:
        return False
    with _ALLOWLIST_WRITE_LOCK:
        try:
            with open(ALLOWED_EMAILS_PATH, "r", encoding="utf-8") as fh:
                lines = fh.readlines()
        except OSError:
            return False
        kept, removed = [], False
        for line in lines:
            if line.strip().lower() == email:
                removed = True
                continue
            kept.append(line)
        if not removed:
            return False
        tmp = f"{ALLOWED_EMAILS_PATH}.tmp.{os.getpid()}.{threading.get_ident()}"
        with open(tmp, "w", encoding="utf-8") as fh:
            fh.writelines(kept)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, ALLOWED_EMAILS_PATH)
    with _ALLOWED_EMAILS_LOCK:
        _allowed_emails_cache["mtime"] = None
    return True

def extract_emails_from_text(text):
    """Extract emails from pasted text, comma lists, Telegram messages, etc."""
    found = re.findall(
        r"[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}",
        text or ""
    )

    cleaned = []
    seen = set()

    for email in found:
        email = email.strip().lower()
        if email and email not in seen:
            seen.add(email)
            cleaned.append(email)

    return cleaned

def add_emails_to_allowlist(emails):
    """
    Add emails to allowed_gmails.txt safely.
    Deduplicates existing + new emails.
    Returns only newly added emails.
    """
    emails = [
        e.strip().lower()
        for e in emails
        if e and e.strip()
    ]
    if not emails:
        return []
    with _ALLOWLIST_WRITE_LOCK:
        existing_lines = []
        try:
            with open(ALLOWED_EMAILS_PATH, "r", encoding="utf-8") as fh:
                existing_lines = fh.readlines()
        except FileNotFoundError:
            existing_lines = []
        except OSError:
            existing_lines = []
        kept = []
        seen = set()
        for line in existing_lines:
            raw = line.strip()
            email = raw.lower()
            if not raw:
                continue
            # Preserve comments
            if raw.startswith("#"):
                kept.append(raw)
                continue
            if email not in seen:
                seen.add(email)
                kept.append(email)
        added = []
        for email in emails:
            if email not in seen:
                seen.add(email)
                kept.append(email)
                added.append(email)
        os.makedirs(os.path.dirname(ALLOWED_EMAILS_PATH) or ".", exist_ok=True)
        tmp = f"{ALLOWED_EMAILS_PATH}.tmp.{os.getpid()}.{threading.get_ident()}"
        with open(tmp, "w", encoding="utf-8") as fh:
            fh.write("\n".join(kept).strip() + "\n")
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, ALLOWED_EMAILS_PATH)
    # Force allowlist cache refresh
    with _ALLOWED_EMAILS_LOCK:
        _allowed_emails_cache["mtime"] = None
    return added

_IP_EMAIL_LOCK = threading.Lock()

def _ip_group_key(client_ip):
    """Group key for an IP. IPv6 collapses to its /64 (one household) when enabled."""
    ip = (client_ip or "").strip().lower()
    if not ip:
        return ""
    if IP_GROUP_IPV6_64:
        try:
            addr = ipaddress.ip_address(ip)
            if addr.version == 6:
                return str(ipaddress.ip_network(f"{ip}/64", strict=False).network_address) + "/64"
        except ValueError:
            pass
    return ip

def _record_ip_login(group_key, email):
    """Record email under group_key, prune to the window, return {email: last_seen}."""
    now = time.time()
    window = IP_EMAIL_WINDOW_HRS * 3600
    email = (email or "").lower()
    with _IP_EMAIL_LOCK:
        data = read_json_file(IP_EMAIL_MAP_PATH, {})
        entry = {e: ts for e, ts in (data.get(group_key) or {}).items() if now - ts <= window}
        entry[email] = now
        data[group_key] = entry
        data = {k: v for k, v in data.items() if v}   
        write_json_atomic(IP_EMAIL_MAP_PATH, data)
    return entry

def log_multi_account(group_key, emails, action, removed):
    print(f"[MULTI-ACCT] group={group_key} emails={emails} action={action} removed={removed}")
    try:
        os.makedirs(os.path.dirname(DOWNLOAD_ABUSE_LOG_PATH) or ".", exist_ok=True)
        event = {"ts": time.time(), "date": date.today().isoformat(),
                 "reason": "multi_account_ip", "group": group_key,
                 "emails": emails, "action": action, "removed": removed}
        with _DOWNLOAD_ABUSE_LOCK:
            with open(DOWNLOAD_ABUSE_LOG_PATH, "a", encoding="utf-8") as fh:
                fh.write(json.dumps(event, ensure_ascii=False) + "\n")
    except Exception as exc:
        print(f"[WARN] Could not write multi-account log: {exc}")

def enforce_multi_account(client_ip, current_email):
    """Returns True if the CURRENT user was removed (caller should revoke the session)."""
    if MAX_EMAILS_PER_IP <= 0 or current_email in ADMIN_EMAILS:
        return False
    group = _ip_group_key(client_ip)
    if not group:
        return False
    entry = _record_ip_login(group, current_email)
    emails = [e for e in entry if e not in ADMIN_EMAILS]
    if len(emails) <= MAX_EMAILS_PER_IP:
        return False

    if MULTI_ACCOUNT_ENFORCE == "log":
        log_multi_account(group, emails, "log_only", [])
        return False
    if MULTI_ACCOUNT_ENFORCE == "remove_all":
        removed = [e for e in emails if remove_email_from_allowlist(e)]
        log_multi_account(group, emails, "remove_all", removed)
        return current_email.lower() in [r.lower() for r in removed]
    
    ordered = sorted(emails, key=lambda e: entry.get(e, 0))
    to_remove = ordered[MAX_EMAILS_PER_IP:]
    removed = [e for e in to_remove if remove_email_from_allowlist(e)]
    log_multi_account(group, emails, "remove_new", removed)
    return current_email.lower() in [r.lower() for r in removed]

def _ip_is_autoban(client_ip):
    if not client_ip or not AUTOBAN_IPS:
        return False
    ip = client_ip.strip().lower()
    return any(ip == tok or ip.startswith(tok) for tok in AUTOBAN_IPS)

# ---- Accounts (password + email verification) -------------------------------
_USERS_LOCK = threading.Lock()

def _load_users_unlocked():
    return read_json_file(USERS_PATH, {})

def load_users():
    with _USERS_LOCK:
        return _load_users_unlocked()

def mutate_users(mutator):
    with _USERS_LOCK:
        data = _load_users_unlocked()
        result = mutator(data)
        write_json_atomic(USERS_PATH, data)
        return result

def _new_code():
    return f"{secrets.randbelow(1_000_000):06d}"   

def _hash_code(code):
    return hashlib.sha256(f"{FLASK_SECRET_KEY}:{code}".encode("utf-8")).hexdigest()

def send_email(to_addr, subject, body):
    """Send a plain-text email. Returns True on success.
    If SMTP is not configured, it prints the email body/code to the console.
    """
    if not (SMTP_USER and SMTP_PASS):
        print(f"[EMAIL] (SMTP not configured) to={to_addr} :: {body}")
        return True
    try:
        msg = EmailMessage()
        msg["From"] = SMTP_USER
        msg["To"] = to_addr
        msg["Subject"] = subject
        msg.set_content(body)
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=20) as s:
            s.starttls()
            s.login(SMTP_USER, SMTP_PASS)
            s.send_message(msg)
        return True
    except Exception as exc:
        print(f"[EMAIL] Failed to send to {to_addr}: {exc}")
        return False

def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        email = session.get("user_email")
        if not email:
            if request.path.startswith("/api/"):
                return json_error("Authentication required.", 401)
            return redirect(url_for("login"))
        if email not in ADMIN_EMAILS and email not in get_allowed_emails():
            session.pop("user_email", None)
            if request.path.startswith("/api/"):
                return json_error("Access revoked.", 403)
            return redirect(url_for("login"))
        return f(*args, **kwargs)
    return decorated_function

# ====================================================================
# 7. RATE LIMITING & DOWNLOAD ACCOUNTING
# ====================================================================
_email_download_tracker = {}
_DOWNLOAD_LIMIT_LOCK = threading.Lock()

def check_download_limit(user_email):
    with _DOWNLOAD_LIMIT_LOCK:
        entry = _email_download_tracker.get(user_email)
        if not entry or entry["date"] != date.today().isoformat():
            return True
        return entry["count"] < DAILY_DOWNLOAD_LIMIT

def increment_download_count(user_email):
    """Bump today's per-email download count and return the new total."""
    today = date.today().isoformat()
    with _DOWNLOAD_LIMIT_LOCK:
        entry = _email_download_tracker.get(user_email)
        if not entry or entry["date"] != today:
            entry = {"date": today, "count": 0}
        entry["count"] += 1
        _email_download_tracker[user_email] = entry
        return entry["count"]

RATE_LIMITS = {
    "read":   {"email": (10, 200),   "ip": (10, 200)},
    "asset":  {"email": (100, 1500), "ip": (200, 2500)},
    "library": {"email": (30, 300),   "ip": (60, 500)},
    "auth":   {"email": (5, 30),     "ip": (10, 60)},   
}
_RATE_LIMIT_LOCK = threading.Lock()
_rate_buckets = collections.defaultdict(collections.deque)

def _would_exceed(key, per_min, per_hour, now):
    dq = _rate_buckets.get(key)
    if not dq:
        return False
    cutoff = now - 3600
    while dq and dq[0] < cutoff:
        dq.popleft()
    if not dq:
        _rate_buckets.pop(key, None)
        return False
    in_hour = len(dq)
    in_min = sum(1 for t in dq if t >= now - 60)
    return in_min >= per_min or in_hour >= per_hour

def enforce_rate_limit(route_class, as_json=False):
    user_email = session.get("user_email", "")
    if user_email in ADMIN_EMAILS:
        return None
    client_ip = get_client_ip()
    em, eh = RATE_LIMITS[route_class]["email"]
    im, ih = RATE_LIMITS[route_class]["ip"]
    email_key = (route_class, "email", user_email)
    ip_key = (route_class, "ip", client_ip)
    now = time.time()

    with _RATE_LIMIT_LOCK:
        if ((user_email and _would_exceed(email_key, em, eh, now)) or
            (client_ip and _would_exceed(ip_key, im, ih, now))):
            msg = "Rate limit exceeded - please slow down."
            return json_error(msg, 429) if as_json else (msg, 429)
        if user_email:
            _rate_buckets[email_key].append(now)
        if client_ip:
            _rate_buckets[ip_key].append(now)
        return None

_DOWNLOAD_ABUSE_LOCK = threading.Lock()
def log_download_limit_exceeded(user_email, tg_link, client_ip):
    try:
        os.makedirs(os.path.dirname(DOWNLOAD_ABUSE_LOG_PATH) or ".", exist_ok=True)
        event = {
            "ts": time.time(),
            "date": date.today().isoformat(),
            "email": (user_email or "").lower(),
            "ip": client_ip or "",
            "tg_link": tg_link,
            "limit": DAILY_DOWNLOAD_LIMIT,
        }
        line = json.dumps(event, ensure_ascii=False)
        with _DOWNLOAD_ABUSE_LOCK:
            with open(DOWNLOAD_ABUSE_LOG_PATH, "a", encoding="utf-8") as fh:
                fh.write(line + "\n")
    except Exception as exc:
        print(f"[WARN] Could not write abuse log: {exc}")

def log_autoban(user_email, client_ip, tg_link, removed):
    print(f"[AUTOBAN] ip={client_ip} email={user_email or '-'} removed={removed}")
    try:
        os.makedirs(os.path.dirname(DOWNLOAD_ABUSE_LOG_PATH) or ".", exist_ok=True)
        event = {
            "ts": time.time(), "date": date.today().isoformat(),
            "reason": "autoban_ip", "email": (user_email or "").lower(),
            "ip": client_ip or "", "tg_link": tg_link, "removed": bool(removed),
        }
        with _DOWNLOAD_ABUSE_LOCK:
            with open(DOWNLOAD_ABUSE_LOG_PATH, "a", encoding="utf-8") as fh:
                fh.write(json.dumps(event, ensure_ascii=False) + "\n")
    except Exception as exc:
        print(f"[WARN] Could not write autoban log: {exc}")

_DOWNLOAD_LOG_LOCK = threading.Lock()
def log_download_event(user_email, novel, tg_link, want_raw, client_ip, count_today):
    """Record every successful download: live console line + durable JSONL."""
    label = (novel.get("title_en") or novel.get("title_kr") or "").strip()
    novel_id = novel_key(novel)
    kind = "raw" if want_raw else "translated"
    print(f"[DOWNLOAD] email={user_email or '-'} ip={client_ip} "
          f"novel={novel_id} kind={kind} count_today={count_today} title={label!r}")
    try:
        os.makedirs(os.path.dirname(DOWNLOAD_LOG_PATH) or ".", exist_ok=True)
        event = {
            "ts": time.time(),
            "date": date.today().isoformat(),
            "email": (user_email or "").lower(),
            "ip": client_ip or "",
            "novel_id": novel_id,
            "title": label,
            "kind": kind,
            "tg_link": tg_link,
        }
        line = json.dumps(event, ensure_ascii=False)
        with _DOWNLOAD_LOG_LOCK:
            with open(DOWNLOAD_LOG_PATH, "a", encoding="utf-8") as fh:
                fh.write(line + "\n")
    except Exception as exc:
        print(f"[WARN] Could not write download log: {exc}")

# ====================================================================
# 8. METADATA LOADING & GALLERY ASSEMBLY (CACHED)
# ====================================================================
def _public_novel(n):
    """Return a copy of a gallery item safe to send to the browser."""
    safe = {k: v for k, v in n.items() if k not in ("tg_link", "raw_tg_link", "local_folder")}
    safe["has_download"] = bool(n.get("tg_link"))
    safe["has_raw"] = bool(n.get("raw_tg_link"))
    return safe

def load_text_map(filepath, key_is_int=False, is_tag=False):
    data_map = {}
    if not os.path.exists(filepath):
        return data_map
    try:
        with open(filepath, "r", encoding="utf-8") as fh:
            for line in fh:
                parts = line.strip().split("|||")
                if len(parts) < 2:
                    continue
                key = parts[0].strip()
                if key_is_int and key.isdigit():
                    key = int(key)
                if is_tag:
                    data_map[key] = parts[1].strip()
                elif len(parts) >= 3:
                    data_map[key] = parts[2].strip()
    except OSError as exc:
        print(f"[WARN] Could not read {filepath}: {exc}")
    return data_map

def load_raw_library_lookup():
    lookup = {}
    if not os.path.exists(RAW_MASTER_CSV_PATH):
        return lookup
    try:
        with open(RAW_MASTER_CSV_PATH, "r", encoding="utf-8") as fh:
            for row in csv.reader(fh):
                try:
                    if not row or len(row) < 2:
                        continue
                    name_str = row[0].strip() if ".epub" in row[0] else row[1].strip()
                    link_str = row[1].strip() if ("t.me" in row[1] or "http" in row[1]) else row[2].strip()
                    novel_id = extract_novel_id(name_str)
                    korean_name = extract_korean_name(name_str) or get_pure_cjk(name_str)
                    pure = get_pure_cjk(korean_name or name_str)
                    if novel_id:
                        lookup[f"id:{novel_id}"] = link_str
                    if korean_name:
                        lookup[f"kr:{normalize_korean_key(korean_name)}"] = link_str
                    if pure:
                        lookup.setdefault(f"cjk:{pure}", link_str)
                except IndexError:
                    continue
    except OSError as exc:
        print(f"[WARN] Could not read {RAW_MASTER_CSV_PATH}: {exc}")
    return lookup

def _scan_local_folders():
    folder_cjk_map, folder_en_map = {}, {}
    if os.path.isdir(LOCAL_OUTPUT_DIR):
        try:
            for entry in os.listdir(LOCAL_OUTPUT_DIR):
                if os.path.isdir(os.path.join(LOCAL_OUTPUT_DIR, entry)):
                    folder_cjk_map[get_pure_cjk(entry)] = entry
                    folder_en_map[get_pure_english(entry)] = entry
        except OSError as exc:
            print(f"[WARN] Could not scan {LOCAL_OUTPUT_DIR}: {exc}")
    return folder_cjk_map, folder_en_map

def _load_novel_db_lookups():
    db_exact, db_pure = {}, {}
    novels_json = read_json_file(JSON_DB_PATH, [])
    if isinstance(novels_json, list):
        for novel in novels_json:
            title = str(novel.get("title", "")).strip()
            pure_cjk = get_pure_cjk(title)
            if title:
                db_exact[title] = novel
            if pure_cjk:
                db_pure[pure_cjk] = novel
    return db_exact, db_pure

def _matched_item(match, filename, tg_link, upload_date, has_custom, en_titles, en_tags, en_descs):
    novel_id = match["id"]
    return {
        "has_meta": True, "is_custom": has_custom, "id": novel_id,
        "filename": filename, "tg_link": tg_link,
        "title_en": en_titles.get(novel_id, match.get("title")),
        "title_kr": match.get("title"),
        "author": match.get("author", "Unknown"),
        "cover": match.get("cover", ""),
        "tags": [en_tags.get(str(t).strip(), str(t).strip()) for t in match.get("tags", [])],
        "synopsis": en_descs.get(novel_id, match.get("synopsis", "")),
        "views": match.get("views", 0), "likes": match.get("likes", 0),
        "chapters": match.get("chapters", 0), "age": match.get("age", 0),
        "complete": match.get("complete", 0), "upload_date": upload_date,
    }

def _unmatched_item(filename, tg_link, upload_date, has_custom, custom_cjk):
    return {
        "has_meta": False, "is_custom": has_custom, "id": "",
        "filename": filename, "tg_link": tg_link,
        "title_en": filename.replace(".epub", ""),
        "title_kr": custom_cjk if custom_cjk else "",
        "author": "Raw Upload", "cover": "", "tags": ["Unmatched"],
        "synopsis": "No metadata found.",
        "views": 0, "likes": 0, "chapters": 0, "age": 0, "complete": 0,
        "upload_date": upload_date,
    }

def _apply_custom_overrides(item, c_data):
    if c_data.get("title_en", "").strip():
        item["title_en"] = c_data["title_en"].strip()
    if c_data.get("title_kr", "").strip():
        item["title_kr"] = c_data["title_kr"].strip()
    if c_data.get("author", "").strip():
        item["author"] = c_data["author"].strip()
    c_cover = c_data.get("cover", "").strip()
    if c_cover and not c_cover.startswith("data:image"):
        item["cover"] = c_cover
    c_tags = [t for t in c_data.get("tags", []) if t and t != "Unmatched"]
    if c_tags:
        item["tags"] = c_tags
    if c_data.get("synopsis", "").strip():
        item["synopsis"] = c_data["synopsis"].strip()
    item["has_meta"] = True

def _build_gallery_items():
    folder_cjk_map, folder_en_map = _scan_local_folders()
    en_titles = load_text_map(TITLES_EN_PATH, key_is_int=True)
    en_tags = load_text_map(TAGS_EN_PATH, is_tag=True)
    en_descs = load_text_map(DESC_EN_PATH, key_is_int=True)
    raw_lookup = load_raw_library_lookup()
    db_exact, db_pure = _load_novel_db_lookups()
    custom_meta = load_custom_meta()
    items = []

    if not os.path.exists(TRANSLATED_CSV_PATH):
        return items
    try:
        with open(TRANSLATED_CSV_PATH, "r", encoding="utf-8") as fh:
            reader = csv.reader(fh)
            next(reader, None)  
            for row in reader:
                try:
                    if len(row) < 2:
                        continue
                    filename = row[0].strip()
                    tg_link = row[1].strip()
                    upload_date = row[2].strip() if len(row) > 2 else LEGACY_UPLOAD_DATE
                    pure_cjk = get_pure_cjk(filename)
                    novel_id = extract_novel_id(filename)
                    korean_name = extract_korean_name(filename)
                    has_custom = filename in custom_meta
                    c_data = custom_meta.get(filename, {}) if has_custom else {}
                    custom_cjk = c_data.get("title_kr", "").strip()

                    match = None
                    if novel_id:
                        match = db_pure.get(get_pure_cjk(korean_name))
                    if not match and custom_cjk:
                        match = db_exact.get(custom_cjk) or db_pure.get(get_pure_cjk(custom_cjk))
                    if not match and korean_name:
                        match = db_exact.get(korean_name) or db_pure.get(get_pure_cjk(korean_name))
                    if not match:
                        match = db_pure.get(pure_cjk)

                    if match:
                        item = _matched_item(match, filename, tg_link, upload_date,
                                             has_custom, en_titles, en_tags, en_descs)
                    else:
                        item = _unmatched_item(filename, tg_link, upload_date,
                                               has_custom, custom_cjk or korean_name)
                    if has_custom:
                        _apply_custom_overrides(item, c_data)

                    search_name = (item["title_kr"].strip() if item.get("title_kr") else "") or korean_name
                    item["raw_tg_link"] = (
                        (raw_lookup.get(f"id:{novel_id}") if novel_id else "")
                        or raw_lookup.get(f"kr:{normalize_korean_key(search_name)}")
                        or raw_lookup.get(f"cjk:{get_pure_cjk(search_name)}", "")
                    )
                    search_cjk = get_pure_cjk(item["title_kr"]) if item["title_kr"] else pure_cjk
                    search_en = get_pure_english(item["title_en"]) if item["title_en"] else get_pure_english(filename)
                    target_folder = folder_cjk_map.get(search_cjk) or folder_en_map.get(search_en)
                    item["local_folder"] = target_folder
                    item["has_local_read"] = bool(target_folder)
                    items.append(item)
                except Exception as exc:
                    print(f"[WARN] Skipping malformed tracker row {row!r}: {exc}")
    except OSError as exc:
        print(f"[WARN] Could not read {TRANSLATED_CSV_PATH}: {exc}")
    return items

_gallery_cache = {"items": [], "csv_mtime": 0.0, "custom_mtime": 0.0, "out_mtime": 0.0, "gen": 0}
_GALLERY_CACHE_LOCK = threading.Lock()

def _mtime(path):
    try:
        return os.path.getmtime(path)
    except OSError:
        return 0.0

def load_gallery_data():
    csv_mtime = _mtime(TRANSLATED_CSV_PATH)
    custom_mtime = _mtime(CUSTOM_META_PATH)
    out_mtime = _mtime(LOCAL_OUTPUT_DIR)
    with _GALLERY_CACHE_LOCK:
        cache = _gallery_cache
        if (cache["items"] and cache["csv_mtime"] == csv_mtime
                and cache["custom_mtime"] == custom_mtime
                and cache["out_mtime"] == out_mtime):
            return cache["items"]
        items = _build_gallery_items()
        cache.update(items=items, csv_mtime=csv_mtime,
                     custom_mtime=custom_mtime, out_mtime=out_mtime,
                     gen=cache["gen"] + 1)
        return items

def find_novel(novel_id):
    return next(
        (n for n in load_gallery_data()
         if str(n.get("id")) == novel_id or n.get("filename") == novel_id),
        None,
    )

def novel_key(novel):
    return str(novel.get("id")) if novel.get("id") else novel.get("filename", "")

# ====================================================================
# 8b. NOVELPIA NOTICE-IMAGE GALLERY
# ====================================================================

NOTICE_DIR = os.path.join(META_DIR, "notice_images")
NP_IMG_CACHE = os.path.join(META_DIR, "np_image_cache")

NP_CDN_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Linux; Android 13) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0 Mobile Safari/537.36"
    ),
    "Referer": "https://novelpia.com/",
    "Origin": "https://novelpia.com",
    "Accept": "image/avif,image/webp,image/apng,image/svg+xml,image/*,*/*;q=0.8",
    "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
}

_notice_cache = {}
_NOTICE_LOCK = threading.Lock()

def _notice_manifest_path(np_id):
    return os.path.join(NOTICE_DIR, f"image_gallery_{np_id}.json")

def load_notice_manifest(np_id):
    np_id = str(np_id)
    path = _notice_manifest_path(np_id)
    try:
        mtime = os.path.getmtime(path)
    except OSError:
        return None
    with _NOTICE_LOCK:
        cached = _notice_cache.get(np_id)
        if cached and cached["mtime"] == mtime:
            return cached["data"]
    data = read_json_file(path, None)
    if not isinstance(data, dict):
        return None
    with _NOTICE_LOCK:
        _notice_cache[np_id] = {
            "mtime": mtime,
            "data": data,
        }
    return data

def _sniff_image_mime(head):
    if not head:
        return "application/octet-stream"
    if head[:3] == b"\xff\xd8\xff":
        return "image/jpeg"
    if head[:8] == b"\x89PNG\r\n\x1a\n":
        return "image/png"
    if head[:6] in (b"GIF87a", b"GIF89a"):
        return "image/gif"
    if head[:4] == b"RIFF" and len(head) >= 12 and head[8:12] == b"WEBP":
        return "image/webp"
    if head[:4].lower() == b"<svg" or b"<svg" in head[:256].lower():
        return "image/svg+xml"
    return "application/octet-stream"

def _resolve_novelpia_key(novel):
    if not novel:
        return ""
    nid = str(novel.get("id") or "").strip()
    return nid if nid.isdigit() else ""

def _normalize_cdn_url(src):
    src = str(src or "").strip()
    if not src:
        return ""

    src = src.replace("\\/", "/").replace("&amp;", "&")

    for sep in ('"', "'", "\\", "<", ">", " "):
        src = src.split(sep)[0]

    if src.startswith("//"):
        src = "https:" + src
    if src.startswith("http://"):
        src = "https://" + src[len("http://"):]
    if not src.startswith("https://images.novelpia.com/"):
        return ""

    return src

def _cache_cdn_image(url):
    url = _normalize_cdn_url(url)
    if not url:
        return None, None
    key = hashlib.md5(url.encode("utf-8")).hexdigest()
    path = os.path.join(NP_IMG_CACHE, key)

    if os.path.isfile(path) and os.path.getsize(path) > 0:
        try:
            with open(path, "rb") as fh:
                head = fh.read(256)
            mime = _sniff_image_mime(head)
            if mime.startswith("image/"):
                return path, mime
            try:
                os.remove(path)
            except OSError:
                pass
        except OSError:
            pass

    os.makedirs(NP_IMG_CACHE, exist_ok=True)
    tmp = f"{path}.tmp.{os.getpid()}.{threading.get_ident()}"
    try:
        with requests.get(
            url,
            headers=NP_CDN_HEADERS,
            stream=True,
            timeout=30,
            allow_redirects=True,
        ) as resp:
            content_type = (resp.headers.get("Content-Type") or "").lower()
            if resp.status_code != 200:
                print(f"[WARN] CDN image status {resp.status_code}: {url}")
                return None, None

            iterator = resp.iter_content(64 * 1024)
            first_chunk = b""
            for chunk in iterator:
                if chunk:
                    first_chunk = chunk
                    break

            if not first_chunk:
                print(f"[WARN] CDN returned empty image response: {url}")
                return None, None

            mime = _sniff_image_mime(first_chunk[:256])
            if not mime.startswith("image/"):
                print(
                    f"[WARN] CDN returned non-image for {url}; "
                    f"content-type={content_type!r}; "
                    f"head={first_chunk[:100]!r}"
                )
                return None, None

            with open(tmp, "wb") as out:
                out.write(first_chunk)
                for chunk in iterator:
                    if chunk:
                        out.write(chunk)
            os.replace(tmp, path)
            return path, mime
    except (requests.RequestException, OSError) as exc:
        print(f"[WARN] Could not cache CDN image {url}: {exc}")
        try:
            if os.path.exists(tmp):
                os.remove(tmp)
        except OSError:
            pass
        return None, None

# ====================================================================
# 8c. TAG SIMILARITY & RECOMMENDATIONS
# ====================================================================
# IDF-weighted tag overlap: novels sharing rare tags ("Regression") are far
# more alike than novels sharing ubiquitous ones ("Fantasy"). The inverted
# index piggybacks on the gallery cache generation so it rebuilds exactly
# when the gallery does.
SIMILAR_MAX = 24
_GENERIC_AUTHORS = ("", "Unknown", "Raw Upload")
_REC_STATUS_WEIGHTS = {"reading": 1.5, "finished": 1.0, "want_to_read": 0.5}

_SIM_LOCK = threading.Lock()
_sim_state = {"gen": None, "novel_tags": {}, "tag_novels": {}, "tag_idf": {},
              "by_key": {}, "topk": {}}

def _novel_is_adult(novel):
    return to_int(novel.get("age"), 0) == 19

def _similarity_state():
    items = load_gallery_data()
    with _GALLERY_CACHE_LOCK:
        gen = _gallery_cache["gen"]
    with _SIM_LOCK:
        if _sim_state["gen"] == gen:
            return _sim_state
        tag_novels = defaultdict(set)
        novel_tags, by_key = {}, {}
        for n in items:
            key = novel_key(n)
            by_key[key] = n
            tags = {t for t in n.get("tags", []) if t and t != "Unmatched"}
            novel_tags[key] = tags
            for t in tags:
                tag_novels[t].add(key)
        total = max(1, len(by_key))
        tag_idf = {t: math.log(total / len(keys)) for t, keys in tag_novels.items()}
        _sim_state.update(gen=gen, novel_tags=novel_tags, tag_novels=dict(tag_novels),
                          tag_idf=tag_idf, by_key=by_key, topk={})
        return _sim_state

def _popularity_bonus(novel):
    """Small tiebreaker only - must never outrank a rare shared tag."""
    return 0.05 * math.log10(1 + max(0, to_int(novel.get("likes"), 0)))

def _rank_similar(seed, state):
    seed_key = novel_key(seed)
    seed_tags = state["novel_tags"].get(seed_key) or set()
    seed_author = (seed.get("author") or "").strip()
    adult_ok = _novel_is_adult(seed)

    overlap = defaultdict(float)
    for t in seed_tags:
        idf = state["tag_idf"].get(t, 0.0)
        for key in state["tag_novels"].get(t, ()):
            if key != seed_key:
                overlap[key] += idf

    scored = []
    for key, shared in overlap.items():
        cand = state["by_key"][key]
        if not adult_ok and _novel_is_adult(cand):
            continue
        cand_tags = state["novel_tags"].get(key) or set()
        score = shared / math.sqrt(max(1, len(seed_tags)) * max(1, len(cand_tags)))
        if seed_author not in _GENERIC_AUTHORS and (cand.get("author") or "").strip() == seed_author:
            score += 0.3
        score += _popularity_bonus(cand)
        scored.append((score, key))
    scored.sort(key=lambda pair: pair[0], reverse=True)
    return scored[:SIMILAR_MAX]

def get_similar_novels(seed, limit):
    """Return (basis, [(score, novel), ...]) for a seed novel.

    basis: "tags" normally, "author" / "popular" when the seed has no
    usable tag overlap. Adult titles are only suggested for adult seeds.
    """
    state = _similarity_state()
    seed_key = novel_key(seed)
    with _SIM_LOCK:
        ranked = state["topk"].get(seed_key)
    if ranked is None:
        ranked = _rank_similar(seed, state)
        with _SIM_LOCK:
            state["topk"][seed_key] = ranked
    results = [(score, state["by_key"][key]) for score, key in ranked[:limit]
               if key in state["by_key"]]
    if results:
        return "tags", results

    adult_ok = _novel_is_adult(seed)
    seed_author = (seed.get("author") or "").strip()
    if seed_author not in _GENERIC_AUTHORS:
        same_author = [n for n in state["by_key"].values()
                       if novel_key(n) != seed_key
                       and (n.get("author") or "").strip() == seed_author
                       and (adult_ok or not _novel_is_adult(n))]
        if same_author:
            same_author.sort(key=lambda n: to_int(n.get("likes"), 0), reverse=True)
            return "author", [(0.0, n) for n in same_author[:limit]]

    popular = [n for n in state["by_key"].values()
               if novel_key(n) != seed_key and (adult_ok or not _novel_is_adult(n))]
    popular.sort(key=lambda n: to_int(n.get("likes"), 0), reverse=True)
    return "popular", [(0.0, n) for n in popular[:limit]]

def get_user_recommendations(email, limit):
    """Personalised shelf: score the library against the user's tag profile.

    Saved novels (any status/progress entry) are never recommended back;
    adult titles only appear if the user's own list already contains one.
    """
    state = _similarity_state()
    udata = load_user_data().get(email, {})
    profile = defaultdict(float)
    saved = set()
    allow_adult = False
    for key, record in udata.items():
        if not isinstance(record, dict):
            continue
        saved.add(key)
        weight = _REC_STATUS_WEIGHTS.get(record.get("status"), 0.0)
        novel = state["by_key"].get(key)
        if weight <= 0 or novel is None:
            continue
        if _novel_is_adult(novel):
            allow_adult = True
        for t in state["novel_tags"].get(key, ()):
            profile[t] += weight

    if profile:
        overlap = defaultdict(float)
        for t, weight in profile.items():
            idf = state["tag_idf"].get(t, 0.0)
            for key in state["tag_novels"].get(t, ()):
                if key not in saved:
                    overlap[key] += idf * weight
        scored = []
        for key, shared in overlap.items():
            cand = state["by_key"][key]
            if not allow_adult and _novel_is_adult(cand):
                continue
            cand_tags = state["novel_tags"].get(key) or set()
            scored.append((shared / math.sqrt(max(1, len(cand_tags)))
                           + _popularity_bonus(cand), key))
        if scored:
            scored.sort(key=lambda pair: pair[0], reverse=True)
            return "tags", [(score, state["by_key"][key]) for score, key in scored[:limit]]

    popular = [n for n in state["by_key"].values()
               if novel_key(n) not in saved and (allow_adult or not _novel_is_adult(n))]
    popular.sort(key=lambda n: (to_int(n.get("likes"), 0), str(n.get("upload_date") or "")),
                 reverse=True)
    return "popular", [(0.0, n) for n in popular[:limit]]

# ====================================================================
# 9. READER PIPELINE (TOC, CHAPTERS, ASSETS)
# ====================================================================
def novel_base_path(novel):
    return os.path.join(LOCAL_OUTPUT_DIR, novel["local_folder"])

def _fill_sequence_gaps(chapter_files):
    final_toc = []
    prev = None
    for rel_path in chapter_files:
        filename = os.path.basename(rel_path)
        match = re.search(r"^([^0-9]*)(\d+)([^0-9]*)$", filename)
        if match:
            prefix, num_str, suffix = match.groups()
            dir_path = os.path.dirname(rel_path)
            current_num = int(num_str)
            if prev:
                p_dir, p_prefix, p_num_str, p_suffix, p_num = prev
                if (p_dir == dir_path and p_prefix == prefix and p_suffix == suffix
                        and current_num > p_num + 1
                        and (current_num - p_num) < MAX_GAP_FILL):
                    pad_len = len(num_str) if num_str.startswith("0") else (
                        len(p_num_str) if p_num_str.startswith("0") else 0)
                    for missing_num in range(p_num + 1, current_num):
                        m_num = str(missing_num).zfill(pad_len) if pad_len else str(missing_num)
                        m_name = f"{prefix}{m_num}{suffix}"
                        m_rel = f"{dir_path}/{m_name}" if dir_path else m_name
                        final_toc.append(f"{TOC_MISSING_PREFIX}{m_rel}")
            prev = (dir_path, prefix, num_str, suffix, current_num)
        else:
            prev = None
        final_toc.append(rel_path)
    return final_toc

def get_novel_toc(novel):
    base_path = novel_base_path(novel)
    chapter_files = []
    if os.path.isdir(base_path):
        for root, _, files in os.walk(base_path):
            for file in files:
                lower_f = file.lower()
                if "title_translator" in lower_f or "translated__book" in lower_f:
                    continue
                if lower_f.endswith(CHAPTER_EXTENSIONS) and lower_f not in NON_CHAPTER_FILES:
                    rel_path = os.path.relpath(os.path.join(root, file), base_path)
                    chapter_files.append(rel_path.replace("\\", "/"))
        chapter_files.sort(key=natural_sort_key)
        return _fill_sequence_gaps(chapter_files)
    return []

def get_novel_images(novel):
    base_path = novel_base_path(novel)
    images = set()
    if os.path.isdir(base_path):
        for root, _, files in os.walk(base_path):
            for f in files:
                if f.lower().endswith(IMAGE_EXTENSIONS):
                    rel = os.path.relpath(os.path.join(root, f), base_path)
                    images.add(rel.replace("\\", "/"))
    if not images:
        try:
            for epub_file in (f for f in os.listdir(base_path) if f.lower().endswith(".epub")):
                with zipfile.ZipFile(os.path.join(base_path, epub_file), "r") as z:
                    for zip_info in z.infolist():
                        if zip_info.filename.lower().endswith(IMAGE_EXTENSIONS):
                            images.add(zip_info.filename)
        except (OSError, zipfile.BadZipFile):
            pass
    return sorted(images)

_MEDIA_REF_PATTERN = re.compile(r"(src=|href=|url\()(['\"]?)([^'\" \)>]+)\2([\s)>]|$)", re.IGNORECASE)

# Chapters come from third-party EPUBs, so anything executable must be
# stripped before the reader injects the HTML into its DOM. Formatting
# markup is left untouched; this is defense-in-depth alongside the CSP.
_SCRIPT_BLOCK_RE = re.compile(r"<script\b[^>]*>[\s\S]*?</script\s*>", re.IGNORECASE)
_ORPHAN_SCRIPT_RE = re.compile(r"</?script\b[^>]*>", re.IGNORECASE)
_FORBIDDEN_TAG_RE = re.compile(
    r"</?(?:iframe|frame|frameset|object|embed|form|meta|base|applet)\b[^>]*>",
    re.IGNORECASE,
)
_EVENT_ATTR_RE = re.compile(r"\s+on[a-z]+\s*=\s*(?:\"[^\"]*\"|'[^']*'|[^\s>]+)", re.IGNORECASE)
_JS_URL_RE = re.compile(r"(\b(?:href|src|xlink:href)\s*=\s*[\"']?)\s*javascript:[^\"'\s>]*", re.IGNORECASE)

def sanitize_chapter_html(content):
    content = _SCRIPT_BLOCK_RE.sub("", content)
    content = _ORPHAN_SCRIPT_RE.sub("", content)
    content = _FORBIDDEN_TAG_RE.sub("", content)
    content = _EVENT_ATTR_RE.sub("", content)
    content = _JS_URL_RE.sub(r"\1#", content)
    return content

def rewrite_chapter_assets(content, novel_id, chap_rel_path):
    chap_dir = os.path.dirname(chap_rel_path)
    def replace_media(match):
        attr, q, path_str, close_paren = match.group(1), match.group(2), match.group(3), match.group(4)
        clean_path_str = path_str.split("?")[0].lower()
        if not clean_path_str.endswith(REWRITABLE_ASSET_EXTENSIONS):
            return match.group(0)
        if path_str.startswith(("http", "data:")):
            return match.group(0)
        parts = (chap_dir + "/" + path_str).replace("\\", "/").split("/")
        resolved = []
        for p in parts:
            if p == "..":
                if resolved:
                    resolved.pop()
            elif p and p != ".":
                resolved.append(p)
        clean_asset = "/".join(resolved)
        return f"{attr}{q}/api/read/{novel_id}/asset/{quote(clean_asset)}{q}{close_paren}"
    return _MEDIA_REF_PATTERN.sub(replace_media, content)

def _find_case_insensitive(base, parts):
    current = base
    for part in parts:
        if not os.path.isdir(current):
            return None
        try:
            entry = next((e for e in os.listdir(current) if e.lower() == part.lower()), None)
        except OSError:
            return None
        if not entry:
            return None
        current = os.path.join(current, entry)
    return current if os.path.isfile(current) else None

def _extract_asset_from_epubs(base_path, rel_path):
    target_filename = os.path.basename(rel_path).lower()
    target_stem = os.path.splitext(target_filename)[0]
    save_full_path = resolve_under(base_path, rel_path)
    if not save_full_path:
        return None
    asset_dir = os.path.dirname(save_full_path)
    try:
        epubs = [f for f in os.listdir(base_path) if f.lower().endswith(".epub")]
    except OSError:
        return None
    for epub_file in epubs:
        try:
            with zipfile.ZipFile(os.path.join(base_path, epub_file), "r") as z:
                infos = z.infolist()
                for zi in infos:
                    if zi.filename.split("/")[-1].lower() == target_filename:
                        os.makedirs(asset_dir, exist_ok=True)
                        with open(save_full_path, "wb") as out:
                            out.write(z.read(zi.filename))
                        return save_full_path
                for zi in infos:
                    zip_name = zi.filename.split("/")[-1].lower()
                    if zip_name.startswith(target_stem + ".") and zip_name.endswith(ASSET_IMAGE_EXTENSIONS):
                        true_ext = zip_name.rsplit(".", 1)[-1]
                        true_full_path = os.path.join(asset_dir, f"{target_stem}.{true_ext}")
                        os.makedirs(asset_dir, exist_ok=True)
                        with open(true_full_path, "wb") as out:
                            out.write(z.read(zi.filename))
                        return true_full_path
        except (zipfile.BadZipFile, OSError):
            continue
    return None

# ====================================================================
# 10. TELEGRAM BACKGROUND CLIENT & STREAMING
# ====================================================================
telethon_loop = asyncio.new_event_loop()
client = None

def run_telethon():
    global client
    asyncio.set_event_loop(telethon_loop)
    client_kwargs = dict(
        loop=telethon_loop,
        receive_updates=False,
        connection_retries=3,
        request_retries=1,
        timeout=8,
    )
    if USE_PROXY:
        client_kwargs["connection"] = connection.ConnectionTcpMTProxyRandomizedIntermediate
        client_kwargs["proxy"] = (MTPROXY_SERVER, MTPROXY_PORT, MTPROXY_SECRET)
        print(f"[TELEGRAM] Using MTProxy proxy: {MTPROXY_SERVER}:{MTPROXY_PORT}")

    client = TelegramClient(
        SESSION_PATH, TELEGRAM_API_ID, TELEGRAM_API_HASH, **client_kwargs
    )

    async def boot_sequence():
        try:
            await client.start(phone=TELEGRAM_PHONE)
            print("\n[TELEGRAM] Caching dialogs to resolve private channel IDs...")
            await client.get_dialogs()
            print("[TELEGRAM] Gallery proxy active.\n")

            async def heartbeat():
                while True:
                    await asyncio.sleep(150)
                    try:
                        if not client.is_connected():
                            await asyncio.wait_for(client.connect(), timeout=8.0)
                        else:
                            await asyncio.wait_for(client.get_me(), timeout=5.0)
                    except AuthKeyUnregisteredError:
                        break
                    except Exception:
                        pass
            telethon_loop.create_task(heartbeat())
        except AuthKeyUnregisteredError:
            print("[TELEGRAM] Session key unregistered - downloads disabled.")

    try:
        telethon_loop.run_until_complete(boot_sequence())
        telethon_loop.run_forever()
    except Exception as exc:
        print(f"[TELEGRAM] Background loop stopped: {exc}")

if os.environ.get("ARCHIVEDB_NO_TELEGRAM"):
    print("[TELEGRAM] ARCHIVEDB_NO_TELEGRAM set - background client disabled.")
elif not TELEGRAM_CONFIGURED:
    print("[TELEGRAM] Credentials not configured - downloads disabled until "
          "TELEGRAM_API_ID/TELEGRAM_API_HASH/TELEGRAM_PHONE are exported.")
else:
    threading.Thread(target=run_telethon, daemon=True).start()

def parse_telegram_link(tg_link):
    try:
        parts = tg_link.rstrip("/").split("/")
        msg_digits = re.sub(r"\D", "", parts[-1])
        chan_digits = re.sub(r"\D", "", parts[-2])
        message_id = int(msg_digits)
        channel = int("-100" + chan_digits) if chan_digits else parts[-2]
        return channel, message_id
    except (ValueError, IndexError):
        return None

# ====================================================================
# 11. API RESPONSE HELPERS
# ====================================================================
def json_error(message, status):
    return jsonify({"status": "error", "error": message}), status

def require_json(*required_keys):
    data = request.get_json(silent=True)
    if not isinstance(data, dict):
        return None, json_error("Request body must be a JSON object.", 400)
    missing = [k for k in required_keys if k not in data]
    if missing:
        return None, json_error(f"Missing required field(s): {', '.join(missing)}.", 400)
    return data, None

# ====================================================================
# 12. ROUTES - PAGES & AUTH
# ====================================================================
@app.route("/")
@login_required
def index():
    return render_template("gallery.html", dmca_email=DMCA_EMAIL)

@app.route("/register", methods=["GET", "POST"])
def register():
    if request.method == "GET":
        return render_template("register.html", error=None)
    limited = enforce_rate_limit("auth")
    if limited:
        return limited
    email = request.form.get("email", "").strip().lower()
    pw = request.form.get("password", "")
    if email not in ADMIN_EMAILS and email not in get_allowed_emails():
        return render_template(
            "register.html", 
            error="This email is not invited. Send your email to @sigma619 to get access."
        )
    if len(pw) < MIN_PASSWORD_LEN:
        return render_template("register.html", error=f"Password must be {MIN_PASSWORD_LEN}+ characters.")
    code = _new_code()

    def m(users):
        u = users.get(email)
        if u and u.get("verified"):
            return "exists"
        users[email] = {
            "pwd_hash": generate_password_hash(pw),
            "verified": False,
            "code_hash": _hash_code(code),
            "code_expires": time.time() + CODE_TTL_SECONDS,
            "code_attempts": 0,
            "created_at": time.time(),
        }
        return "ok"

    if mutate_users(m) == "exists":
        return render_template("register.html", error="Account already exists - please log in.")
    send_email(email, "Your verification code",
               f"Your verification code is {code}\nIt expires in 10 minutes.")
    return redirect(url_for("verify", email=email))

@app.route("/verify", methods=["GET", "POST"])
def verify():
    email = request.values.get("email", "").strip().lower()
    if request.method == "GET":
        return render_template("verify.html", email=email, error=None)
    limited = enforce_rate_limit("auth")
    if limited:
        return limited
    code = request.form.get("code", "").strip()

    def m(users):
        u = users.get(email)
        if not u or u.get("verified"):
            return "bad"
        if time.time() > u.get("code_expires", 0):
            return "expired"
        if u.get("code_attempts", 0) >= MAX_CODE_ATTEMPTS:
            return "locked"
        u["code_attempts"] = u.get("code_attempts", 0) + 1
        if _hash_code(code) != u.get("code_hash"):
            return "wrong"
        u["verified"] = True
        u.pop("code_hash", None)
        u.pop("code_expires", None)
        u.pop("code_attempts", None)
        return "ok"

    status = mutate_users(m)
    if status == "ok":
        client_ip = get_client_ip()
        if enforce_multi_account(client_ip, email):
            session.pop("user_email", None)
            return render_template("verify.html", email=email,
                error="Access revoked: multiple accounts detected from your network.")
        session.permanent = True
        session["user_email"] = email
        return redirect(url_for("index"))
    msgs = {
        "wrong": "Incorrect code.",
        "expired": "Code expired - register again to get a new one.",
        "locked": "Too many attempts - register again to get a new code.",
        "bad": "Nothing to verify - please register.",
    }
    return render_template("verify.html", email=email, error=msgs.get(status, "Verification error."))

@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "GET":
        return render_template("login.html", error=None)
    limited = enforce_rate_limit("auth")
    if limited:
        return limited
    email = request.form.get("email", "").strip().lower()
    pw = request.form.get("password", "")
    if email not in ADMIN_EMAILS and email not in get_allowed_emails():
        return render_template("login.html", error="Invalid credentials or access revoked.")
    u = load_users().get(email)
    if not u or not u.get("verified") or not check_password_hash(u.get("pwd_hash", ""), pw):
        return render_template("login.html", error="Invalid credentials or unverified email.")
    client_ip = get_client_ip()
    if enforce_multi_account(client_ip, email):
        session.pop("user_email", None)
        return render_template("login.html",
            error="Access revoked: multiple accounts detected from your network.")
    session.permanent = True
    session["user_email"] = email
    return redirect(url_for("index"))

@app.route("/logout")
def logout():
    session.pop("user_email", None)
    return redirect(url_for("login"))

@app.route("/read/<novel_id>")
@login_required
def read_novel(novel_id):
    novel = find_novel(novel_id)
    if not novel or not novel.get("has_local_read"):
        return "Novel not available for online reading.", 404
    toc = get_novel_toc(novel)
    images = get_novel_images(novel)
    user_email = session.get("user_email", "")
    user_record = load_user_data().get(user_email, {}).get(novel_key(novel), {})
    server_progress = to_int(user_record.get("progress"), 0)
    return render_template(
        "reader.html",
        novel=novel,
        toc=toc,
        images=images,
        server_progress=server_progress,
    )

# ====================================================================
# 12. ROUTES - GALLERY API
# ====================================================================
@app.route("/api/collections", methods=["GET", "POST"])
@login_required
def api_collections():
    email = session["user_email"]
    counts = collection_counts(email)
    cols = get_user_collections(email)
    return jsonify({"collections": [
        {"id": c["id"], "name": c["name"], "count": counts.get(c["id"], 0)}
        for c in cols
    ]})

@app.route("/api/collection_create", methods=["POST"])
@login_required
def api_collection_create():
    email = session["user_email"]
    data = request.get_json(silent=True) or {}
    name = str(data.get("name", "")).strip()[:60]
    if not name:
        return jsonify({"error": "Collection name is required"}), 400
    allcols = load_collections()
    user_cols = allcols.get(email, [])
    if any(c["name"].lower() == name.lower() for c in user_cols):
        return jsonify({"error": "You already have a collection with that name"}), 400
    new_col = {"id": uuid.uuid4().hex[:12], "name": name}
    user_cols.append(new_col)
    allcols[email] = user_cols
    save_collections(allcols)
    return jsonify({"collection": {**new_col, "count": 0}})

@app.route("/api/collection_rename", methods=["POST"])
@login_required
def api_collection_rename():
    email = session["user_email"]
    data = request.get_json(silent=True) or {}
    cid = str(data.get("id", "")).strip()
    name = str(data.get("name", "")).strip()[:60]
    if not cid or not name:
        return jsonify({"error": "Collection id and name are required"}), 400
    allcols = load_collections()
    user_cols = allcols.get(email, [])
    found = False
    for c in user_cols:
        if c["id"] == cid:
            c["name"] = name
            found = True
        elif c["name"].lower() == name.lower():
            return jsonify({"error": "Another collection already uses that name"}), 400
    if not found:
        return jsonify({"error": "Collection not found"}), 404
    allcols[email] = user_cols
    save_collections(allcols)
    return jsonify({"ok": True})

@app.route("/api/collection_delete", methods=["POST"])
@login_required
def api_collection_delete():
    email = session["user_email"]
    data = request.get_json(silent=True) or {}
    cid = str(data.get("id", "")).strip()
    if not cid:
        return jsonify({"error": "Collection id is required"}), 400
    allcols = load_collections()
    allcols[email] = [c for c in allcols.get(email, []) if c["id"] != cid]
    save_collections(allcols)

    all_udata = load_user_data()
    udata = all_udata.get(email, {})
    changed = False
    for entry in udata.values():
        if isinstance(entry, dict) and cid in (entry.get("collections") or []):
            entry["collections"] = [x for x in entry["collections"] if x != cid]
            changed = True
    if changed:
        all_udata[email] = udata
        save_user_data(all_udata)
    return jsonify({"ok": True})

@app.route("/api/collection_assign", methods=["POST"])
@login_required
def api_collection_assign():
    email = session["user_email"]
    data = request.get_json(silent=True) or {}
    novel_id = str(data.get("id", "")).strip()
    cid = str(data.get("collection", "")).strip()
    add = bool(data.get("add", True))
    if not novel_id or not cid:
        return jsonify({"error": "Novel id and collection id are required"}), 400
    if not any(c["id"] == cid for c in get_user_collections(email)):
        return jsonify({"error": "Collection not found"}), 404

    all_udata = load_user_data()
    udata = all_udata.setdefault(email, {})
    entry = udata.setdefault(novel_id, {})
    cur = [x for x in (entry.get("collections") or []) if x != cid]
    if add:
        cur.append(cid)
    entry["collections"] = cur
    all_udata[email] = udata
    save_user_data(all_udata)
    return jsonify({"user_data": udata})

@app.route("/api/tags", methods=["GET"])
@login_required
def api_tags():
    tag_counts = {}
    for n in load_gallery_data():
        for t in n.get("tags", []):
            if t != "Unmatched":
                tag_counts[t] = tag_counts.get(t, 0) + 1
    return jsonify(tag_counts)

@app.route("/api/authors", methods=["GET"])
@login_required
def api_authors():
    authors = set()
    for n in load_gallery_data():
        a = n.get("author", "").strip()
        if a and a not in ("Unknown", "Raw Upload"):
            authors.add(a)
    return jsonify(sorted(authors))

@app.route("/api/user_status", methods=["POST"])
@login_required
def api_user_status():
    data, error = require_json("id")
    if error:
        return error
    user_email = session["user_email"]
    target_id = str(data.get("id"))
    status = data.get("status", "none")
    if status not in VALID_READING_STATUSES:
        return json_error(f"Invalid status {status!r}.", 400)

    def mutator(store):
        user = store.setdefault(user_email, {})
        record = user.setdefault(target_id, {"status": "none", "progress": 0})
        if status == "none":
            if record.get("progress", 0) == 0:
                user.pop(target_id, None)
            else:
                record["status"] = "none"
        else:
            record["status"] = status
        if target_id in user:
            user[target_id]["last_read"] = time.time()
        return user

    user_record = mutate_user_data(mutator)
    return jsonify({"status": "success", "user_data": user_record})

@app.route("/api/user_progress", methods=["POST"])
@login_required
def api_user_progress():
    data, error = require_json("id")
    if error:
        return error
    user_email = session["user_email"]
    target_id = str(data.get("id"))
    progress = to_int(data.get("progress"), 0)

    def mutator(store):
        user = store.setdefault(user_email, {})
        record = user.get(target_id)
        if record is None:
            record = {"status": "reading", "progress": progress}
            user[target_id] = record
        else:
            record["progress"] = progress
        if record.get("status") in ("want_to_read", "none"):
            record["status"] = "reading"
        record["last_read"] = time.time()

    mutate_user_data(mutator)
    return jsonify({"status": "success"})

def _passes_filters(novel, filters, user_data):
    n_id = novel_key(novel)
    u_status = user_data.get(n_id, {}).get("status", "none")
    reading_status = filters["reading_status"]
    if reading_status == "any" and u_status in ("none", ""):
        return False
    if reading_status in ("want_to_read", "reading", "finished") and u_status != reading_status:
        return False

    n_age = to_int(novel.get("age"), 0) if str(novel.get("age", "0")).isdigit() else 0
    if filters["audience"] == "non_adult" and n_age == 19:
        return False
    if filters["audience"] == "adult" and n_age != 19:
        return False

    n_status = to_int(novel.get("complete"), 0) if str(novel.get("complete", "0")).isdigit() else 0
    if filters["status"] == "ongoing" and n_status != 0:
        return False
    if filters["status"] == "complete" and n_status != 1:
        return False

    author_filter = filters["author"]
    if author_filter and author_filter != "all":
        if author_filter not in novel.get("author", "").strip().lower():
            return False

    n_chaps = to_int(novel.get("chapters"), 0) if str(novel.get("chapters", "0")).isdigit() else 0
    if not (filters["min_chapters"] <= n_chaps <= filters["max_chapters"]):
        return False

    search = filters["search"]
    if search:
        title_en = (novel.get("title_en") or "").lower()
        title_kr = (novel.get("title_kr") or "").lower()
        if search not in title_en and search not in title_kr and search not in n_id:
            return False

    n_tags = set(novel.get("tags", []))
    if filters["excludes"] and not filters["excludes"].isdisjoint(n_tags):
        return False
    if filters["includes"]:
        if filters["tag_match"] == "or":
            if filters["includes"].isdisjoint(n_tags):
                return False
        elif not filters["includes"].issubset(n_tags):
            return False

    coll = filters.get("collection", "all")
    if coll and coll not in ("all", ""):
        member_of = (user_data.get(novel_key(novel), {}) or {}).get("collections", []) or []
        if coll == "none":
            if member_of:
                return False
        elif coll not in member_of:
            return False
    return True

@app.route("/api/library", methods=["POST"])
@login_required
def api_library():
    limited = enforce_rate_limit("library", as_json=True)
    if limited:
        return limited
    data = request.get_json(silent=True) or {}
    page = max(1, to_int(data.get("page"), 1))
    limit = max(1, to_int(data.get("limit"), 30))
    sort_by = data.get("sortBy", "upload_date")
    sort_order = data.get("sortOrder", "desc")

    filters = {
        "search": str(data.get("search", "")).strip().lower(),
        "includes": set(data.get("includes", []) or []),
        "excludes": set(data.get("excludes", []) or []),
        "reading_status": data.get("readingStatus", "all"),
        "audience": data.get("audience", "all"),
        "status": data.get("status", "all"),
        "author": str(data.get("author", "")).strip().lower(),
        "min_chapters": to_int(data.get("minChapters"), 0),
        "max_chapters": to_int(data.get("maxChapters"), 999999) or 999999,
        "tag_match": str(data.get("tagMatch", "and")).strip().lower(),
        "collection": str(data.get("collection", "all")).strip(),
    }

    user_email = session.get("user_email", "")
    user_data = load_user_data().get(user_email, {})
    filtered = [n for n in load_gallery_data() if _passes_filters(n, filters, user_data)]

    if data.get("random"):
        if filtered:
            return jsonify({"random_id": novel_key(random.choice(filtered))})
        return jsonify({"random_id": None})

    target_sort = sort_by
    if filters["reading_status"] != "all" and sort_by == "upload_date":
        target_sort = "last_read"

    def sort_key(x):
        if target_sort == "last_read":
            return user_data.get(novel_key(x), {}).get("last_read", 0)
        val = x.get(target_sort)
        if target_sort == "upload_date":
            return val if val and val != LEGACY_UPLOAD_DATE else ""
        if val == "-" or val is None:
            return -1
        try:
            return float(val)
        except (TypeError, ValueError):
            return -1

    filtered.sort(key=sort_key, reverse=(sort_order == "desc"))
    total_items = len(filtered)
    total_pages = max(1, (total_items + limit - 1) // limit)
    start_idx = (page - 1) * limit
    page_items = filtered[start_idx:start_idx + limit]

    return jsonify({
        "novels": [_public_novel(n) for n in page_items],
        "total": total_items,
        "totalPages": total_pages,
        "currentPage": page,
        "userData": user_data,
    })

def _clamped_limit(default=12):
    return min(SIMILAR_MAX, max(1, to_int(request.args.get("limit"), default)))

@app.route("/api/novel/<path:novel_id>", methods=["GET"])
@login_required
def api_novel_detail(novel_id):
    limited = enforce_rate_limit("library", as_json=True)
    if limited:
        return limited
    novel = find_novel(novel_id)
    if not novel:
        return json_error("Novel not found.", 404)
    record = load_user_data().get(session["user_email"], {}).get(novel_key(novel), {})
    return jsonify({"novel": _public_novel(novel), "user_record": record})

@app.route("/api/novel/<path:novel_id>/similar", methods=["GET"])
@login_required
def api_novel_similar(novel_id):
    limited = enforce_rate_limit("library", as_json=True)
    if limited:
        return limited
    novel = find_novel(novel_id)
    if not novel:
        return json_error("Novel not found.", 404)
    basis, results = get_similar_novels(novel, _clamped_limit())
    return jsonify({
        "basis": basis,
        "novels": [dict(_public_novel(n), score=round(score, 4)) for score, n in results],
    })

@app.route("/api/recommendations", methods=["GET"])
@login_required
def api_recommendations():
    limited = enforce_rate_limit("library", as_json=True)
    if limited:
        return limited
    basis, results = get_user_recommendations(session["user_email"], _clamped_limit())
    return jsonify({
        "basis": basis,
        "novels": [dict(_public_novel(n), score=round(score, 4)) for score, n in results],
    })

@app.route("/api/edit", methods=["POST"])
@login_required
def edit_metadata():
    data, error = require_json("filename")
    if error:
        return error
    filename = str(data.get("filename", "")).strip()
    if not filename:
        return json_error("'filename' must be a non-empty string.", 400)
    novel = next((n for n in load_gallery_data() if n.get("filename") == filename), None)
    if novel is None:
        return json_error("Unknown novel filename.", 404)
    # Mirror the frontend rule server-side: automatically matched metadata is
    # read-only for non-admins; unmatched or already-customised items are open.
    if (session.get("user_email", "") not in ADMIN_EMAILS
            and novel.get("has_meta") and not novel.get("is_custom")):
        return json_error("Only admins can edit automatically matched metadata.", 403)
    cover_url = str(data.get("cover", "")).strip()
    if cover_url and not cover_url.startswith(("http://", "https://")):
        cover_url = ""

    save_custom_meta_entry(filename, {
        "title_en": str(data.get("title_en", "")),
        "title_kr": str(data.get("title_kr", "")),
        "author": str(data.get("author", "")),
        "cover": cover_url,
        "tags": [t.strip() for t in str(data.get("tags", "")).split(",") if t.strip()],
        "synopsis": str(data.get("synopsis", "")),
    })
    return jsonify({"status": "success"})

# ====================================================================
# 12. ROUTES - ADMIN
# ====================================================================
@app.route("/admin/downloads")
@login_required
def admin_downloads():
    """Per-email download report for a given day (admins only)."""
    if session.get("user_email", "") not in ADMIN_EMAILS:
        return "Forbidden", 403
    day = request.args.get("date", date.today().isoformat())
    counts = {}
    novels = collections.defaultdict(collections.Counter)
    try:
        with open(DOWNLOAD_LOG_PATH, "r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    ev = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if ev.get("date") != day:
                    continue
                em = ev.get("email", "")
                counts[em] = counts.get(em, 0) + 1
                novels[em][ev.get("title") or ev.get("novel_id")] += 1
    except FileNotFoundError:
        pass
    report = sorted(counts.items(), key=lambda kv: kv[1], reverse=True)
    return jsonify({
        "date": day,
        "total": sum(counts.values()),
        "per_email": [
            {"email": em, "count": c, "novels": novels[em].most_common()}
            for em, c in report
        ],
    })

@app.route("/admin/access", methods=["GET", "POST"])
@login_required
def admin_access():
    if session.get("user_email", "") not in ADMIN_EMAILS:
        return "Forbidden", 403

    message = ""
    added = []

    if request.method == "POST":
        raw = request.form.get("emails", "")
        emails = extract_emails_from_text(raw)
        added = add_emails_to_allowlist(emails)

        message = (
            f"Found {len(emails)} email(s). "
            f"Added {len(added)} new email(s). "
            f"Skipped {len(emails) - len(added)} duplicate/already-approved email(s)."
        )

    current_emails = sorted(get_allowed_emails())

    current_html = "\n".join(
        f"<li>{escape(email)}</li>"
        for email in current_emails
    )

    added_html = ""
    if added:
        added_items = "\n".join(
            f"<li>{escape(email)}</li>"
            for email in added
        )
        added_html = f"""
        <div class="added">
          <strong>Newly added:</strong>
          <ul>{added_items}</ul>
        </div>
        """

    message_html = ""
    if message:
        message_html = f'<div class="message">{escape(message)}</div>'

    if not current_html:
        current_html = "<li>No emails approved yet.</li>"

    html = f"""
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>Access list - ArchiveDB</title>
  <meta name="viewport" content="width=device-width, initial-scale=1">

  <style>
    * {{
      box-sizing: border-box;
    }}

    body {{
      margin: 0;
      min-height: 100vh;
      font-family: system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      background:
        radial-gradient(circle at top left, rgba(34, 197, 94, 0.20), transparent 32%),
        radial-gradient(circle at bottom right, rgba(59, 130, 246, 0.22), transparent 30%),
        #0f172a;
      color: #e5e7eb;
      padding: 24px;
    }}

    .wrap {{
      width: 100%;
      max-width: 900px;
      margin: 0 auto;
    }}

    .top-links {{
      display: flex;
      gap: 12px;
      flex-wrap: wrap;
      margin-bottom: 18px;
    }}

    .card {{
      background: rgba(15, 23, 42, 0.92);
      border: 1px solid rgba(148, 163, 184, 0.25);
      border-radius: 22px;
      padding: 24px;
      margin-bottom: 18px;
      box-shadow: 0 24px 80px rgba(0, 0, 0, 0.35);
    }}

    h1 {{
      margin: 0 0 8px;
      font-size: 30px;
      letter-spacing: -0.04em;
    }}

    h2 {{
      margin: 0 0 14px;
      font-size: 22px;
    }}

    p {{
      color: #94a3b8;
      line-height: 1.55;
      margin: 0 0 16px;
    }}

    code {{
      color: #bfdbfe;
      background: rgba(59, 130, 246, 0.12);
      padding: 2px 6px;
      border-radius: 6px;
    }}

    textarea {{
      width: 100%;
      min-height: 180px;
      resize: vertical;
      padding: 14px;
      border-radius: 14px;
      border: 1px solid rgba(148, 163, 184, 0.32);
      background: rgba(2, 6, 23, 0.72);
      color: #f8fafc;
      font-size: 15px;
      line-height: 1.45;
      outline: none;
    }}

    textarea:focus {{
      border-color: #22c55e;
      box-shadow: 0 0 0 4px rgba(34, 197, 94, 0.13);
    }}

    button {{
      width: 100%;
      margin-top: 14px;
      padding: 13px 16px;
      border: 0;
      border-radius: 14px;
      background: linear-gradient(135deg, #22c55e, #3b82f6);
      color: white;
      font-size: 15px;
      font-weight: 800;
      cursor: pointer;
    }}

    button:hover {{
      filter: brightness(1.08);
    }}

    .message {{
      margin-top: 16px;
      padding: 13px 14px;
      border-radius: 14px;
      background: rgba(59, 130, 246, 0.12);
      border: 1px solid rgba(59, 130, 246, 0.30);
      color: #bfdbfe;
      font-size: 14px;
    }}

    .added {{
      margin-top: 16px;
      padding: 13px 14px;
      border-radius: 14px;
      background: rgba(34, 197, 94, 0.12);
      border: 1px solid rgba(34, 197, 94, 0.30);
      color: #bbf7d0;
      font-size: 14px;
    }}

    .current {{
      max-height: 360px;
      overflow: auto;
      border-radius: 14px;
      border: 1px solid rgba(148, 163, 184, 0.18);
      background: rgba(2, 6, 23, 0.38);
      padding: 12px;
    }}

    ul {{
      margin: 0;
      padding-left: 20px;
    }}

    li {{
      padding: 4px 0;
      word-break: break-word;
    }}

    a {{
      color: #93c5fd;
      text-decoration: none;
      font-weight: 800;
    }}

    a:hover {{
      text-decoration: underline;
    }}

    .count {{
      color: #94a3b8;
      font-weight: 500;
    }}
  </style>
</head>

<body>
  <div class="wrap">
    <div class="top-links">
      <a href="/">Back to gallery</a>
      <a href="/admin/downloads">Download report</a>
      <a href="/logout">Logout</a>
    </div>

    <section class="card">
      <h1>Access list</h1>
      <p>
        Paste emails here. You can paste one per line, comma-separated, or from a full message.
        The app will extract emails, lowercase them, skip duplicates, and update
        <code>allowed_gmails.txt</code>.
      </p>

      <form method="post" action="/admin/access">
        <textarea name="emails" placeholder="user1@gmail.com&#10;user2@gmail.com&#10;or paste a message like: please add me, my email is example@gmail.com"></textarea>
        <button type="submit">Add emails</button>
      </form>

      {message_html}
      {added_html}
    </section>

    <section class="card">
      <h2>Currently approved emails <span class="count">({len(current_emails)})</span></h2>
      <div class="current">
        <ul>
          {current_html}
        </ul>
      </div>
    </section>
  </div>
</body>
</html>
"""

    return Response(html, mimetype="text/html")

# ====================================================================
# 12. ROUTES - READER API
# ====================================================================
@app.route("/api/read/<novel_id>/chapter/<path:chap_path>")
@login_required
def api_read_chapter(novel_id, chap_path):
    limited = enforce_rate_limit("read")
    if limited:
        return limited
    novel = find_novel(novel_id)
    if not novel or not novel.get("has_local_read"):
        return "Not found", 404
    base_path = novel_base_path(novel)
    clean_chap_path = chap_path.replace(TOC_MISSING_PREFIX, "")
    full_path = resolve_under(base_path, clean_chap_path)
    if not full_path or not os.path.isfile(full_path):
        return "Chapter file missing.", 404
    try:
        with open(full_path, "r", encoding="utf-8", errors="ignore") as fh:
            content = fh.read()
    except OSError:
        return "Chapter file unreadable.", 500

    content = rewrite_chapter_assets(content, novel_id, clean_chap_path)
    body_match = re.search(r"<body[^>]*>([\s\S]*?)</body>", content, re.IGNORECASE)
    if body_match:
        content = body_match.group(1)
    return sanitize_chapter_html(content)

@app.route("/api/read/<novel_id>/asset/<path:asset_path>")
@login_required
def api_read_asset(novel_id, asset_path):
    limited = enforce_rate_limit("asset")
    if limited:
        return limited
    novel = find_novel(novel_id)
    if not novel or not novel.get("has_local_read"):
        return "Not found", 404
    base_path = novel_base_path(novel)
    rel_path = unquote(asset_path).split("?")[0].replace("\\", "/")
    parts = [p for p in rel_path.split("/") if p and p not in (".", "..")]
    if not parts:
        return "Asset missing", 404

    local_match = _find_case_insensitive(base_path, parts)
    if local_match:
        return send_file(local_match)

    extracted = _extract_asset_from_epubs(base_path, "/".join(parts))
    if extracted:
        return send_file(extracted)
    return "Asset missing", 404

# ====================================================================
# NOTICE GALLERY ROUTES
# ====================================================================
@app.route("/api/read/<novel_id>/notice_gallery")
@login_required
def api_notice_gallery(novel_id):
    limited = enforce_rate_limit("library", as_json=True)
    if limited:
        return limited

    novel = find_novel(novel_id)
    if not novel:
        return json_error("Novel not found.", 404)

    np_id = _resolve_novelpia_key(novel)
    if not np_id:
        return jsonify({
            "available": False,
            "notices": [],
        })

    manifest = load_notice_manifest(np_id)
    if not manifest:
        return jsonify({
            "available": False,
            "notices": [],
        })

    notices = []
    for ntc in manifest.get("notices", []):
        raw_imgs = ntc.get("images", []) or []
        clean_imgs = []
        for src in raw_imgs:
            u = _normalize_cdn_url(src)
            if u:
                clean_imgs.append(u)
        if clean_imgs:
            notices.append({
                "id": str(ntc.get("id", "")),
                "count": len(clean_imgs),
                "images": clean_imgs,
            })

    return jsonify({
        "available": bool(notices),
        "novel_id": str(novel_id),
        "np_id": str(np_id),
        "notices": notices,
    })

@app.route("/api/read/<novel_id>/notice/<notice_id>/img/<int:idx>")
@login_required
def api_notice_image(novel_id, notice_id, idx):
    limited = enforce_rate_limit("asset")
    if limited:
        return limited

    novel = find_novel(novel_id)
    if not novel:
        return "Not found", 404

    np_id = _resolve_novelpia_key(novel)
    if not np_id:
        return "Not found", 404

    manifest = load_notice_manifest(np_id)
    if not manifest:
        return "Not found", 404

    target = next(
        (n for n in manifest.get("notices", []) if str(n.get("id")) == str(notice_id)),
        None,
    )
    if not target:
        return "Not found", 404

    images = target.get("images", []) or []
    if idx < 0 or idx >= len(images):
        return "Not found", 404

    src = _normalize_cdn_url(images[idx])
    if not src:
        return "Bad image URL.", 400

    path, mime = _cache_cdn_image(src)
    if not path:
        return "Upstream image unavailable.", 502

    resp = send_file(path, mimetype=mime)
    resp.headers["Cache-Control"] = "public, max-age=604800"
    resp.headers["X-Content-Type-Options"] = "nosniff"
    return resp

# ====================================================================
# 12. ROUTES - TELEGRAM DOWNLOAD STREAMING
# ====================================================================
@app.route("/download/<path:novel_ref>")
@login_required
def download_file(novel_ref):
    if client is None:
        return "System Booting.", 503

    novel = find_novel(novel_ref)
    if not novel:
        return "Not found.", 404

    want_raw = request.args.get("type", "").lower() == "raw"
    tg_link = (novel.get("raw_tg_link") if want_raw else novel.get("tg_link")) or ""
    if not tg_link:
        return "File not available.", 404

    user_email = session.get("user_email", "")
    is_admin = user_email in ADMIN_EMAILS
    client_ip = get_client_ip()

    if not is_admin and _ip_is_autoban(client_ip):
        removed = remove_email_from_allowlist(user_email)
        log_autoban(user_email, client_ip, tg_link, removed)
        session.pop("user_email", None)
        return "Access revoked.", 403

    if not is_admin and not check_download_limit(user_email):
        log_download_limit_exceeded(user_email, tg_link, client_ip)
        return "Daily limit reached.", 429

    parsed = parse_telegram_link(tg_link)
    if not parsed:
        return "Invalid Link.", 400
    channel_id, message_id = parsed

    async def fetch_meta():
        try:
            return await asyncio.wait_for(client.get_messages(channel_id, ids=message_id), timeout=10.0)
        except Exception:
            return None

    future = asyncio.run_coroutine_threadsafe(fetch_meta(), telethon_loop)
    try:
        msg = future.result(timeout=15)
    except Exception:
        return "Timeout fetching from Telegram.", 504

    if not msg or not msg.media:
        return "File missing from channel.", 404

    new_count = increment_download_count(user_email)
    log_download_event(user_email, novel, tg_link, want_raw, client_ip, new_count)

    filename = msg.file.name if msg.file and getattr(msg.file, "name", None) else "file.epub"
    file_size = msg.file.size if msg.file and getattr(msg.file, "size", None) else None
    chunk_queue = queue.Queue(maxsize=10)
    abort_event = threading.Event()

    async def download_stream():
        try:
            async for chunk in client.iter_download(msg.media, chunk_size=DOWNLOAD_CHUNK_SIZE):
                if abort_event.is_set():
                    break
                while chunk_queue.full():
                    if abort_event.is_set():
                        break
                    await asyncio.sleep(0.05)
                if abort_event.is_set():
                    break
                chunk_queue.put_nowait(chunk)
        except Exception:
            abort_event.set()
        finally:
            while chunk_queue.full():
                if abort_event.is_set():
                    break
                await asyncio.sleep(0.05)
            if not abort_event.is_set():
                chunk_queue.put_nowait(None)

    asyncio.run_coroutine_threadsafe(download_stream(), telethon_loop)

    def generate_response():
        try:
            while not abort_event.is_set():
                try:
                    chunk = chunk_queue.get(timeout=2.0)
                    if chunk is None:
                        break
                    yield chunk
                except queue.Empty:
                    continue
        except GeneratorExit:
            abort_event.set()

    safe_filename = quote(filename)
    headers = {"Content-Disposition": f"attachment; filename*=UTF-8''{safe_filename}"}
    if file_size:
        headers["Content-Length"] = str(file_size)
    return Response(generate_response(), mimetype="application/epub+zip", headers=headers)

# ====================================================================
# PASSWORD RESET & FORGOT ROUTES
# ====================================================================
@app.route("/forgot", methods=["GET", "POST"])
def forgot_password():
    if request.method == "GET":
        return render_template("forgot_password.html", error=None)

    limited = enforce_rate_limit("auth")
    if limited:
        return limited

    email = request.form.get("email", "").strip().lower()

    if email not in ADMIN_EMAILS and email not in get_allowed_emails():
        return render_template(
            "forgot_password.html",
            error="This email is not currently approved. Send your email to @sigma619 again to get access."
        )

    code = _new_code()

    def m(users):
        u = users.get(email)
        if not u or not u.get("verified"):
            return "missing"

        u["reset_code_hash"] = _hash_code(code)
        u["reset_code_expires"] = time.time() + CODE_TTL_SECONDS
        u["reset_code_attempts"] = 0
        return "ok"

    status = mutate_users(m)

    if status != "ok":
        return render_template(
            "forgot_password.html",
            error="No verified account exists for this email. Please register first."
        )

    send_email(
        email,
        "Your ArchiveDB password reset code",
        f"Your password reset code is {code}\nIt expires in 10 minutes."
    )

    return redirect(url_for("reset_password", email=email))

@app.route("/reset_password", methods=["GET", "POST"])
def reset_password():
    email = request.values.get("email", "").strip().lower()
    if request.method == "GET":
        return render_template("reset_password.html", email=email, error=None)
        
    limited = enforce_rate_limit("auth")
    if limited:
        return limited

    if not email:
        return render_template(
            "reset_password.html",
            email=email,
            error="Please enter your email."
        )

    code = request.form.get("code", "").strip()
    new_password = request.form.get("password", "")
    
    if len(new_password) < MIN_PASSWORD_LEN:
        return render_template(
            "reset_password.html",
            email=email,
            error=f"Password must be {MIN_PASSWORD_LEN}+ characters."
        )

    def m(users):
        u = users.get(email)
        if not u or not u.get("verified"):
            return "bad"
        if time.time() > u.get("reset_code_expires", 0):
            return "expired"
        if u.get("reset_code_attempts", 0) >= MAX_CODE_ATTEMPTS:
            return "locked"
        
        u["reset_code_attempts"] = u.get("reset_code_attempts", 0) + 1
        if _hash_code(code) != u.get("reset_code_hash"):
            return "wrong"
            
        u["pwd_hash"] = generate_password_hash(new_password)
        u.pop("reset_code_hash", None)
        u.pop("reset_code_expires", None)
        u.pop("reset_code_attempts", None)
        return "ok"

    status = mutate_users(m)
    if status == "ok":
        session.permanent = True
        session["user_email"] = email
        return redirect(url_for("index"))

    msgs = {
        "wrong": "Incorrect reset code.",
        "expired": "Reset code expired. Request a new one.",
        "locked": "Too many attempts. Request a new reset code.",
        "bad": "Invalid reset request.",
    }
    return render_template(
        "reset_password.html",
        email=email,
        error=msgs.get(status, "Password reset error.")
    )

if __name__ == "__main__":
    app.run(host=_env_str("HOST", "127.0.0.1"), port=_env_int("PORT", 5004))
