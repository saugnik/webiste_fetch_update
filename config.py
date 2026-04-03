from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv


load_dotenv()


def _as_bool(value: str | None, default: bool) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _as_int(value: str | None, default: int, minimum: int | None = None) -> int:
    if value is None or value.strip() == "":
        return default
    try:
        parsed = int(value)
    except ValueError:
        return default
    if minimum is not None and parsed < minimum:
        return minimum
    return parsed


def _csv_to_list(value: str | None) -> list[str]:
    if not value:
        return []
    return [item.strip() for item in value.replace(";", ",").split(",") if item.strip()]


def _dedupe_preserve_order(values: list[str]) -> list[str]:
    seen: set[str] = set()
    output: list[str] = []
    for value in values:
        key = value.strip()
        if not key:
            continue
        if key in seen:
            continue
        seen.add(key)
        output.append(key)
    return output


@dataclass(frozen=True)
class AppConfig:
    seed_target_urls: list[str]
    check_interval_minutes: int
    request_timeout_seconds: int
    request_user_agent: str
    scheduler_timezone: str
    run_on_startup: bool
    flask_secret_key: str
    history_page_size: int
    dashboard_base_url: str
    database_url: str | None
    sqlite_path: Path
    smtp_host: str
    smtp_port: int
    smtp_username: str
    smtp_password: str
    smtp_use_tls: bool
    alert_from_email: str
    alert_to_emails: list[str]

    @property
    def is_postgres(self) -> bool:
        return bool(self.database_url)

    @classmethod
    def from_env(cls) -> "AppConfig":
        raw_database_url = os.getenv("DATABASE_URL", "").strip()
        if raw_database_url.startswith("postgres://"):
            raw_database_url = "postgresql://" + raw_database_url[len("postgres://") :]
        database_url = raw_database_url or None

        sqlite_path = Path(os.getenv("SQLITE_PATH", "data/monitor.db")).expanduser().resolve()

        smtp_username = os.getenv("SMTP_USERNAME", "").strip()
        alert_from_email = os.getenv("ALERT_FROM_EMAIL", "").strip() or smtp_username

        seed_urls: list[str] = []
        seed_urls.extend(_csv_to_list(os.getenv("TARGET_URLS", "")))
        legacy_single_url = os.getenv("TARGET_URL", "").strip()
        if legacy_single_url:
            seed_urls.append(legacy_single_url)
        seed_urls = _dedupe_preserve_order(seed_urls)

        return cls(
            seed_target_urls=seed_urls,
            check_interval_minutes=_as_int(os.getenv("CHECK_INTERVAL_MINUTES"), 15, minimum=1),
            request_timeout_seconds=_as_int(os.getenv("REQUEST_TIMEOUT_SECONDS"), 30, minimum=5),
            request_user_agent=os.getenv(
                "REQUEST_USER_AGENT",
                "WebsiteMonitorBot/1.0 (+https://railway.app/)",
            ).strip(),
            scheduler_timezone=os.getenv("SCHEDULER_TIMEZONE", "Asia/Kolkata").strip() or "Asia/Kolkata",
            run_on_startup=_as_bool(os.getenv("RUN_ON_STARTUP"), True),
            flask_secret_key=os.getenv("FLASK_SECRET_KEY", "change-this-secret-key"),
            history_page_size=_as_int(os.getenv("HISTORY_PAGE_SIZE"), 30, minimum=5),
            dashboard_base_url=os.getenv("DASHBOARD_BASE_URL", "http://localhost:5000").strip(),
            database_url=database_url,
            sqlite_path=sqlite_path,
            smtp_host=os.getenv("SMTP_HOST", "smtp.gmail.com").strip(),
            smtp_port=_as_int(os.getenv("SMTP_PORT"), 587, minimum=1),
            smtp_username=smtp_username,
            smtp_password=os.getenv("SMTP_PASSWORD", "").strip(),
            smtp_use_tls=_as_bool(os.getenv("SMTP_USE_TLS"), True),
            alert_from_email=alert_from_email,
            alert_to_emails=_csv_to_list(os.getenv("ALERT_TO_EMAILS", "")),
        )
