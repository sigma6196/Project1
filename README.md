# ArchiveDB

A private, invite-only library and online reader for translated novels. Single-file
Flask backend (`gallery_app.py`) with self-contained HTML templates — no build step,
no database (flat JSON/CSV files), downloads streamed from a private Telegram channel
via Telethon.

## Repo layout vs. server layout

This repo keeps the HTML files at the root for convenience. On the server, Flask
requires them inside a `templates/` directory:

| Repo file | Deployed location |
|---|---|
| `gallery_app.py` | `~/gallery_app.py` |
| `*.html` (all 7) | `~/templates/*.html` |

Deploying a change is: copy the file(s) to their deployed location, then restart the
Flask process. Do **not** move files around inside the repo — the deployment layout
is intentionally left untouched.

## Deployment (Oracle Cloud + Cloudflare Tunnel)

The production setup this app is written for:

- Oracle Cloud free-tier instance (SSH access, e.g. from Termux).
- The app runs directly with `python3 gallery_app.py` — no nginx/apache/gunicorn.
- It binds `127.0.0.1:5004` (defaults; override with `HOST`/`PORT` env vars, but the
  Cloudflare Tunnel must point at whatever you choose).
- A free Cloudflare Tunnel (`cloudflared tunnel --url http://localhost:5004`) exposes
  it publicly over HTTPS. TLS terminates at Cloudflare; the app itself speaks plain
  HTTP on localhost, which is why `CF-Connecting-IP` is trusted for client IPs.

### Environment variables

The app reads all configuration from the environment. On the instance the vars live
in `~/archivedb.env`; load them before starting the app:

```bash
set -a; source ~/archivedb.env; set +a
python3 ~/gallery_app.py
```

(Add new vars by appending `NAME=value` lines to `~/archivedb.env`.)

Required:

| Variable | Purpose |
|---|---|
| `FLASK_SECRET_KEY` | Session signing. App refuses to start without it. Changing it logs everyone out. |
| `ADMIN_EMAILS` | Comma-separated admin accounts. **No longer has a built-in default** — if unset, nobody has admin access. |

Required for downloads (the app boots without them, but Telegram downloads stay
disabled and a startup warning is printed):

| Variable | Purpose |
|---|---|
| `TELEGRAM_API_ID` | Telegram API ID (my.telegram.org). **No longer baked into the source.** |
| `TELEGRAM_API_HASH` | Telegram API hash. |
| `TELEGRAM_PHONE` | Phone number of the account that owns the session file. |

Optional:

| Variable | Default | Purpose |
|---|---|---|
| `SMTP_USER` / `SMTP_PASS` | – | Gmail address + app password for verification emails (codes print to console if unset). |
| `SMTP_HOST` / `SMTP_PORT` | `smtp.gmail.com` / `587` | SMTP server. |
| `DMCA_EMAIL` | – | Public contact shown in the DMCA modal (modal shows a placeholder if unset). |
| `USE_PROXY` | `0` | Set `1` to route Telegram through an MTProto proxy; needs `MTPROXY_SERVER`, `MTPROXY_PORT`, `MTPROXY_SECRET`. |
| `COOKIE_SECURE` | `1` | Session cookies are HTTPS-only (correct behind Cloudflare). Set `0` **only** for plain-HTTP local testing, otherwise login won't stick. |
| `HOST` / `PORT` | `127.0.0.1` / `5004` | Bind address. Keep in sync with the tunnel. |
| `ARCHIVEDB_NO_TELEGRAM` | – | Any value disables the Telegram client (for dev/tests). |
| `META_DIR`, `LOCAL_OUTPUT_DIR`, `TRANSLATED_CSV_PATH`, `RAW_MASTER_CSV_PATH`, `SESSION_PATH`, … | production paths | Data locations; see the config section at the top of `gallery_app.py`. |
| `DAILY_DOWNLOAD_LIMIT`, `MAX_EMAILS_PER_IP`, `MULTI_ACCOUNT_ENFORCE`, `IP_EMAIL_WINDOW_HRS`, `AUTOBAN_IPS` | see source | Abuse controls. |

### Local development

```bash
pip install -r requirements.txt
mkdir -p templates && cp *.html templates/
FLASK_SECRET_KEY=dev ARCHIVEDB_NO_TELEGRAM=1 COOKIE_SECURE=0 \
  META_DIR=./devdata LOCAL_OUTPUT_DIR=./devdata/output \
  python3 gallery_app.py
```

The app tolerates missing data files (empty library, console warnings).

## API overview

All endpoints are JSON over the session cookie (login required; `library`-class
rate limits apply). Telegram links never appear in any response.

| Endpoint | Purpose |
|---|---|
| `POST /api/library` | Filtered/sorted/paginated novel list (search, tags AND/OR, audience, status, author, chapters, collection, `random`) |
| `GET /api/novel/<id>` | Full detail for one novel + the caller's status/progress/collections record |
| `GET /api/novel/<id>/similar?limit=N` | Tag-based similar novels (`basis`: `tags` / `author` / `popular` fallback) |
| `GET /api/recommendations?limit=N` | Personalised shelf from the user's status-weighted tag profile |
| `GET /api/tags`, `GET /api/authors` | Filter vocabularies |
| `POST /api/user_status`, `POST /api/user_progress` | Reading list status & chapter progress |
| `POST /api/collections`, `/api/collection_create` / `_rename` / `_delete` / `_assign` | Collections CRUD |
| `POST /api/edit` | Metadata overrides (admins, or unmatched/custom novels) |
| `GET /api/read/<id>/chapter/…`, `/asset/…` | Sanitised chapter HTML and its assets |
| `GET /download/<ref>?type=raw` | EPUB streamed from Telegram (daily quota applies) |

## Tests

```bash
pip install pytest
python3 -m pytest tests/ -q
```

The suite stages `gallery_app.py` and the templates into a temp directory that
mirrors the server layout (`templates/` subfolder) with fixture data — live
data paths are never read or written.

## Security notes

- Secrets are **never** committed. Earlier revisions contained baked-in Telegram
  credentials and an MTProto proxy secret as fallbacks; they were removed and must be
  treated as compromised (rotate the API hash and retire the proxy).
- All state-changing APIs require a JSON body with `Content-Type: application/json`;
  session cookies are `HttpOnly`, `SameSite=Lax`, and `Secure` by default.
- Every response carries CSP / `X-Frame-Options` / `nosniff` / referrer-policy
  headers. Chapter HTML extracted from EPUBs is sanitized server-side (scripts,
  event handlers, and `javascript:` URLs are stripped) before it reaches the reader.
- Telegram links are resolved server-side only and never sent to the browser.
