# Automated Website Data Fetcher

A production-ready multi-website monitoring system that:

- monitors unlimited website URLs on a schedule (default every 15 minutes)
- detects new content per website using fingerprint-based diffing
- sends HTML email alerts that clearly show which website changed
- provides a live Flask dashboard with website management + full run history
- runs 24/7 on Railway with PostgreSQL, with sqlite fallback for local development

## Tech Stack

- Python 3.11
- Flask
- APScheduler
- requests + BeautifulSoup4
- psycopg2-binary (PostgreSQL) with sqlite fallback
- smtplib + `email.mime` (Gmail SMTP)
- python-dotenv
- Gunicorn

## How Detection Works

1. Each monitored website is fetched separately.
2. Visible text blocks are extracted from major tags.
3. Every block is normalized and hashed (SHA-256).
4. Hashes are stored per website.
5. Only never-seen hashes for that same website are treated as "new".
6. First successful run for a website creates baseline (no alert).
7. Later runs send alert emails for that exact website when new blocks appear.

## Project Structure

```text
.
├── app.py
├── config.py
├── db.py
├── emailer.py
├── monitor.py
├── scraper.py
├── templates/
│   ├── base.html
│   ├── dashboard.html
│   ├── history.html
│   └── run_detail.html
├── static/
│   └── style.css
├── requirements.txt
├── Procfile
├── railway.toml
├── runtime.txt
└── .env.example
```

## Local Setup (Windows PowerShell)

1. Create and activate virtual environment:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
```

2. Install dependencies:

```powershell
pip install -r requirements.txt
```

3. Configure environment:

```powershell
Copy-Item .env.example .env
```

4. Edit `.env`:
- set Gmail values (`SMTP_USERNAME`, `SMTP_PASSWORD`, `ALERT_TO_EMAILS`)
- optionally seed startup websites with `TARGET_URLS`
- keep `DATABASE_URL` empty locally (sqlite fallback)

5. Run app:

```powershell
python app.py
```

Dashboard: `http://localhost:5000`

## Gmail SMTP Setup

Use a Google App Password (not your main Gmail password):

1. Enable 2-Step Verification.
2. Go to Google App Passwords.
3. Generate a Mail app password.
4. Put it into `SMTP_PASSWORD`.

## Using Multiple Websites

You have two ways:

1. Add websites in dashboard (`/`) using the "Add Website" form.
   - You can paste one URL, or many URLs separated by comma/newline/semicolon.
2. Pre-seed websites at startup with:

```env
TARGET_URLS=https://site1.com,https://site2.com/news,https://blog.site3.com
```

You can run checks manually:

- all active websites: `POST /run-now` (Dashboard button)
- one website: `POST /websites/<id>/run-now` (row action button)

You can pause/resume websites from the dashboard row actions.

## Railway Deployment (24/7 + PostgreSQL)

1. Push repo to GitHub.
2. Create Railway project from repo.
3. Add PostgreSQL service.
4. Configure app env vars:
- `FLASK_SECRET_KEY`
- `SMTP_USERNAME`
- `SMTP_PASSWORD`
- `ALERT_FROM_EMAIL`
- `ALERT_TO_EMAILS`
- `DASHBOARD_BASE_URL` (Railway public URL)
- optional: `TARGET_URLS`, `CHECK_INTERVAL_MINUTES`, `SCHEDULER_TIMEZONE`
5. Set `DATABASE_URL` from Railway PostgreSQL connection string.
6. Deploy (uses provided `Procfile` and `railway.toml`).

## Why Single Gunicorn Worker

Scheduler runs in-process. Multiple workers can duplicate scheduled jobs and emails.
This app uses:

```bash
gunicorn --workers 1 --threads 4 --timeout 120 app:app
```

## Endpoints

- `GET /` dashboard
- `GET /history` run history
- `GET /run/<id>` run details
- `GET /health` health JSON
- `POST /run-now` run all active websites now
- `POST /websites/add` add/reactivate website
- `POST /websites/<id>/toggle` pause/resume website
- `POST /websites/<id>/run-now` run single website now
- `POST /websites/<id>/delete` delete website URL (and its stored run history)

## Environment Variables

| Variable | Required | Default | Description |
|---|---|---|---|
| `TARGET_URLS` | No | empty | Comma-separated startup seed URLs |
| `TARGET_URL` | No | empty | Legacy single startup URL seed |
| `CHECK_INTERVAL_MINUTES` | No | `15` | Scheduler interval |
| `REQUEST_TIMEOUT_SECONDS` | No | `30` | HTTP request timeout |
| `REQUEST_USER_AGENT` | No | monitor UA | Scraper user-agent |
| `SCHEDULER_TIMEZONE` | No | `Asia/Kolkata` | APScheduler timezone |
| `RUN_ON_STARTUP` | No | `true` | Run all active sites once at boot |
| `HISTORY_PAGE_SIZE` | No | `30` | Rows per history page |
| `DASHBOARD_BASE_URL` | No | `http://localhost:5000` | Links in alert email |
| `FLASK_SECRET_KEY` | Yes | dev fallback | Flask secret key |
| `DATABASE_URL` | Cloud yes / local no | empty | PostgreSQL DSN |
| `SQLITE_PATH` | No | `data/monitor.db` | Local sqlite path |
| `SMTP_HOST` | Yes (for email) | `smtp.gmail.com` | SMTP host |
| `SMTP_PORT` | Yes (for email) | `587` | SMTP port |
| `SMTP_USERNAME` | Yes (for email) | empty | SMTP username |
| `SMTP_PASSWORD` | Yes (for email) | empty | Gmail app password |
| `SMTP_USE_TLS` | No | `true` | STARTTLS |
| `ALERT_FROM_EMAIL` | Yes (for email) | SMTP username | Sender email |
| `ALERT_TO_EMAILS` | Yes (for email) | empty | Comma-separated recipients |

## Operational Notes

- Baseline is independent per website.
- Email alerts are also independent per website.
- If SMTP is missing, checks continue and data is still saved.
- Health endpoint returns `503` when DB is unhealthy.
