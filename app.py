from __future__ import annotations

import atexit
import functools
import html as html_module
import logging
import math
import os
import re
import threading
import warnings
from datetime import datetime, timezone
from hmac import compare_digest
from typing import Any
from urllib.parse import urlparse, urlunparse
from zoneinfo import ZoneInfo

from apscheduler.schedulers.background import BackgroundScheduler
from bs4 import BeautifulSoup, MarkupResemblesLocatorWarning
from apscheduler.triggers.interval import IntervalTrigger
from flask import Flask, abort, flash, g, jsonify, redirect, render_template, request, session, url_for
from werkzeug.security import check_password_hash, generate_password_hash

from config import AppConfig
from db import Database
from emailer import EmailClient
from financial_updates import build_latest_financial_summary, is_financial_relevant
from psu_tev_sources import PSU_TEV_SOURCES
from tev_updates import build_latest_tev_summary, is_tev_relevant
from monitor import MonitorService


logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)
logger = logging.getLogger(__name__)
IST_ZONE = ZoneInfo("Asia/Kolkata")


def format_datetime(value: Any) -> str:
    if value is None:
        return "N/A"
    if isinstance(value, datetime):
        dt_value = value
    else:
        raw = str(value).strip()
        if not raw:
            return "N/A"
        try:
            dt_value = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        except ValueError:
            return raw
    if dt_value.tzinfo is None:
        dt_value = dt_value.replace(tzinfo=timezone.utc)
    return dt_value.astimezone(IST_ZONE).strftime("%Y-%m-%d %H:%M:%S IST")


def clean_item_text(value: Any) -> str:
    raw = str(value or "")
    decoded = html_module.unescape(raw)
    if "<" in decoded and ">" in decoded:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", MarkupResemblesLocatorWarning)
            text_only = BeautifulSoup(decoded, "html.parser").get_text(" ", strip=True)
    else:
        text_only = decoded
    return re.sub(r"\s+", " ", text_only).strip()


def preview_item_text(value: Any, max_chars: int = 340) -> str:
    cleaned = clean_item_text(value)
    if len(cleaned) <= max_chars:
        return cleaned
    clipped = cleaned[:max_chars].rsplit(" ", 1)[0].strip()
    return f"{clipped}..."


def site_label(target_url: str | None, website_display_name: Any = None) -> str:
    cleaned_name = str(website_display_name or "").strip()
    if cleaned_name:
        return cleaned_name
    cleaned_url = str(target_url or "").strip()
    parsed = urlparse(cleaned_url)
    return parsed.netloc or cleaned_url or "Unknown Site"


def normalize_website_url(raw_url: str) -> str:
    cleaned = (raw_url or "").strip()
    if not cleaned:
        raise ValueError("Website URL is required.")

    if not re.match(r"^[a-zA-Z][a-zA-Z0-9+\-.]*://", cleaned):
        cleaned = f"https://{cleaned}"

    parsed = urlparse(cleaned)
    if parsed.scheme.lower() not in {"http", "https"}:
        raise ValueError("Only http and https URLs are supported.")
    if not parsed.netloc:
        raise ValueError("URL is invalid. Please include a valid domain.")

    scheme = parsed.scheme.lower()
    netloc = parsed.netloc.lower()
    path = parsed.path or ""
    if path == "/":
        path = ""

    normalized = urlunparse((scheme, netloc, path, parsed.params, parsed.query, ""))
    return normalized


def normalize_email(raw_email: str) -> str:
    return (raw_email or "").strip().lower()


def _safe_next_path(raw_next: str | None) -> str | None:
    if not raw_next:
        return None
    candidate = raw_next.strip()
    if not candidate or not candidate.startswith("/") or candidate.startswith("//"):
        return None
    parsed = urlparse(candidate)
    if parsed.scheme or parsed.netloc:
        return None
    return candidate


def login_required(view_func):
    @functools.wraps(view_func)
    def wrapped_view(*args, **kwargs):
        return view_func(*args, **kwargs)

    return wrapped_view


def to_json_safe(value: Any) -> Any:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.astimezone(IST_ZONE).isoformat()
    if isinstance(value, dict):
        return {key: to_json_safe(item) for key, item in value.items()}
    if isinstance(value, list):
        return [to_json_safe(item) for item in value]
    return value


def status_class(status: str) -> str:
    cleaned = (status or "").strip().lower()
    return {
        "success": "status-success",
        "new_content": "status-new-content",
        "error": "status-error",
        "baseline": "status-baseline",
        "running": "status-running",
        "skipped": "status-skipped",
    }.get(cleaned, "status-default")


def financial_update_class(status: str) -> str:
    cleaned = (status or "").strip().lower()
    return {
        "yes": "status-success",
        "no": "status-default",
    }.get(cleaned, "status-default")


def financial_category_class(status: str) -> str:
    cleaned = (status or "").strip().lower()
    return {
        "results": "status-success",
        "tender": "status-baseline",
        "credit": "status-new-content",
        "rates": "status-running",
        "budget": "status-skipped",
        "general_finance": "status-default",
        "not_available": "status-default",
    }.get(cleaned, "status-default")


def tev_status_class(status: str) -> str:
    cleaned = (status or "").strip().lower()
    return {
        "open": "status-success",
        "closed": "status-error",
        "upcoming": "status-running",
        "not_specified": "status-default",
    }.get(cleaned, "status-default")


def tev_notice_class(value: str) -> str:
    cleaned = (value or "").strip().lower()
    return {
        "yes": "status-success",
        "not_clear": "status-default",
    }.get(cleaned, "status-default")


def should_seed_psu_tev() -> bool:
    raw = os.getenv("AUTO_SEED_PSU_TEV", "true").strip().lower()
    return raw in {"1", "true", "yes", "on"}


def should_start_scheduler() -> bool:
    if os.getenv("DISABLE_SCHEDULER", "").strip().lower() in {"1", "true", "yes", "on"}:
        return False

    # Flask dev server launches two processes with reloader. Only start in the child process.
    running_from_flask_cli = os.getenv("FLASK_RUN_FROM_CLI", "").lower() == "true"
    is_reloader_child = os.getenv("WERKZEUG_RUN_MAIN", "").lower() == "true"
    if running_from_flask_cli and not is_reloader_child:
        return False
    return True


def create_app() -> Flask:
    config = AppConfig.from_env()

    app = Flask(__name__)
    app.config["SECRET_KEY"] = config.flask_secret_key
    app.config["APP_STARTED_AT"] = datetime.now(timezone.utc)

    db = Database(config)
    db.init_schema()

    # URLs are managed exclusively via the dashboard.
    # ENV seeding and PSU TEV auto-seeding are disabled so startup never
    # pollutes the DB with stale or wrong URLs.

    email_client = EmailClient(config)
    monitor = MonitorService(config=config, db=db, email_client=email_client)
    scheduler = BackgroundScheduler(timezone=config.scheduler_timezone)

    app.jinja_env.filters["fmt_dt"] = format_datetime
    app.jinja_env.filters["clean_item_text"] = clean_item_text
    app.jinja_env.filters["preview_item_text"] = preview_item_text
    app.jinja_env.globals["status_class"] = status_class
    app.jinja_env.globals["financial_update_class"] = financial_update_class
    app.jinja_env.globals["financial_category_class"] = financial_category_class
    app.jinja_env.globals["tev_status_class"] = tev_status_class
    app.jinja_env.globals["tev_notice_class"] = tev_notice_class
    app.jinja_env.globals["site_label"] = site_label

    app.extensions["monitor_config"] = config
    app.extensions["monitor_db"] = db
    app.extensions["monitor_service"] = monitor
    app.extensions["monitor_scheduler"] = scheduler
    app.extensions["monitor_email_client"] = email_client

    @app.before_request
    def load_current_user() -> None:
        g.current_user = None

    @app.context_processor
    def inject_template_globals() -> dict[str, Any]:
        return {
            "current_user": None,
            "fixed_auth_enabled": False,
        }

    def run_all_active_sites(trigger_type: str) -> dict[str, Any]:
        active_sites = db.get_websites(active_only=True)
        return monitor.run_all_sites(websites=active_sites, trigger_type=trigger_type)

    @app.route("/auth/register", methods=["GET", "POST"])
    def register():
        return redirect(url_for("dashboard"))

    @app.route("/auth/login", methods=["GET", "POST"])
    def login():
        requested_next = _safe_next_path(request.values.get("next"))
        if requested_next and requested_next not in {"/auth/login", "/auth/register"}:
            return redirect(requested_next)
        return redirect(url_for("dashboard"))

    @app.post("/auth/logout")
    def logout():
        session.clear()
        return redirect(url_for("dashboard"))

    def start_scheduler() -> None:
        if scheduler.running:
            return

        scheduler.add_job(
            id="website_monitor_job",
            func=run_all_active_sites,
            kwargs={"trigger_type": "scheduled"},
            trigger=IntervalTrigger(minutes=config.check_interval_minutes),
            replace_existing=True,
            coalesce=True,
            max_instances=1,
            misfire_grace_time=max(120, config.check_interval_minutes * 60),
        )
        scheduler.start()
        logger.info(
            "Scheduler started. interval=%s minutes timezone=%s",
            config.check_interval_minutes,
            config.scheduler_timezone,
        )

        if config.run_on_startup:
            startup_thread = threading.Thread(
                target=run_all_active_sites,
                kwargs={"trigger_type": "startup"},
                daemon=True,
                name="startup-monitor-run",
            )
            startup_thread.start()

    @app.get("/")
    @login_required
    def dashboard():
        stats = db.get_stats()
        latest_run = db.get_latest_run()
        recent_runs = db.get_recent_runs(limit=None)
        raw_recent_items = db.get_recent_new_items(limit=None)
        financial_items = [
            item for item in raw_recent_items if is_financial_relevant(str(item.get("item_text") or ""))
        ]
        tev_items = [item for item in raw_recent_items if is_tev_relevant(str(item.get("item_text") or ""))]
        websites = db.get_websites(active_only=None)
        active_websites = [item for item in websites if bool(item.get("is_active"))]
        inactive_websites = [item for item in websites if not bool(item.get("is_active"))]
        financial_summary = build_latest_financial_summary(websites=websites, recent_items=financial_items)
        tev_summary = build_latest_tev_summary(websites=websites, recent_items=tev_items)

        next_run_time = None
        job = scheduler.get_job("website_monitor_job")
        if job is not None:
            next_run_time = job.next_run_time

        started_at = app.config.get("APP_STARTED_AT")
        uptime_text = "N/A"
        if isinstance(started_at, datetime):
            elapsed = datetime.now(timezone.utc) - started_at
            total_seconds = int(elapsed.total_seconds())
            hours, remainder = divmod(total_seconds, 3600)
            minutes, seconds = divmod(remainder, 60)
            uptime_text = f"{hours}h {minutes}m {seconds}s"

        return render_template(
            "dashboard.html",
            config=config,
            latest_run=latest_run,
            recent_runs=recent_runs,
            financial_items=financial_items,
            tev_items=tev_items,
            financial_summary=financial_summary,
            tev_summary=tev_summary,
            websites=websites,
            active_websites=active_websites,
            inactive_websites=inactive_websites,
            stats=stats,
            next_run_time=next_run_time,
            scheduler_running=scheduler.running,
            email_configured=email_client.is_configured,
            uptime_text=uptime_text,
        )

    @app.get("/history")
    @login_required
    def history():
        requested_page = request.args.get("page", default=1, type=int) or 1
        page = max(1, requested_page)
        runs, total_count = db.get_runs_page(page=page, page_size=config.history_page_size)
        total_pages = max(1, math.ceil(total_count / config.history_page_size)) if total_count else 1
        if page > total_pages:
            page = total_pages
            runs, total_count = db.get_runs_page(page=page, page_size=config.history_page_size)

        return render_template(
            "history.html",
            config=config,
            runs=runs,
            page=page,
            total_pages=total_pages,
            total_count=total_count,
        )

    @app.get("/run/<int:run_id>")
    @login_required
    def run_detail(run_id: int):
        run = db.get_run_by_id(run_id)
        if not run:
            abort(404)
        items = db.get_run_items(run_id)
        return render_template(
            "run_detail.html",
            config=config,
            run=run,
            items=items,
        )

    @app.post("/websites/add")
    @login_required
    def add_website():
        raw_url = request.form.get("url", "")
        display_name = request.form.get("display_name", "").strip() or None
        candidate_urls = [item.strip() for item in re.split(r"[\n,;]+", raw_url) if item.strip()]
        if not candidate_urls:
            flash("Website URL is required.", "error")
            return redirect(url_for("dashboard"))

        if len(candidate_urls) > 1:
            created_count = 0
            reactivated_count = 0
            failed_items: list[str] = []
            for raw_candidate in candidate_urls:
                try:
                    normalized_url = normalize_website_url(raw_candidate)
                    _, created = db.add_or_activate_website(normalized_url, display_name=None)
                    if created:
                        created_count += 1
                    else:
                        reactivated_count += 1
                except Exception:
                    failed_items.append(raw_candidate)

            flash(
                f"Bulk add finished. Added: {created_count}, updated/reactivated: {reactivated_count}, failed: {len(failed_items)}.",
                "success" if not failed_items else "warning",
            )
            if failed_items:
                preview = ", ".join(failed_items[:5])
                if len(failed_items) > 5:
                    preview = f"{preview}, and {len(failed_items) - 5} more"
                flash(f"Invalid URLs skipped: {preview}", "warning")
            return redirect(url_for("dashboard"))

        try:
            normalized_url = normalize_website_url(candidate_urls[0])
            website, created = db.add_or_activate_website(normalized_url, display_name=display_name)
        except ValueError as exc:
            flash(str(exc), "error")
            return redirect(url_for("dashboard"))
        except Exception as exc:
            flash(f"Failed to add website: {exc}", "error")
            return redirect(url_for("dashboard"))

        label = site_label(website.get("url"), website.get("display_name"))
        if created:
            flash(f"Added website: {label}", "success")
        else:
            flash(f"Website already existed. Updated/reactivated: {label}", "warning")
        return redirect(url_for("dashboard"))

    @app.post("/websites/<int:website_id>/toggle")
    @login_required
    def toggle_website(website_id: int):
        website = db.get_website_by_id(website_id)
        if not website:
            abort(404)
        current_state = bool(website.get("is_active"))
        target_state = not current_state
        updated = db.set_website_active(website_id=website_id, is_active=target_state)
        if not updated:
            flash("No changes were applied.", "warning")
            return redirect(url_for("dashboard"))

        label = site_label(website.get("url"), website.get("display_name"))
        if target_state:
            flash(f"Website enabled: {label}", "success")
        else:
            flash(f"Website paused: {label}", "warning")
        return redirect(url_for("dashboard"))

    @app.post("/websites/<int:website_id>/delete")
    @login_required
    def delete_website(website_id: int):
        deleted, website, removed_runs = db.delete_website(website_id=website_id, delete_history=True)
        if not deleted or not website:
            abort(404)

        label = site_label(website.get("url"), website.get("display_name"))
        flash(
            f"Deleted website: {label}. Removed {removed_runs} run(s) and related detected-item history.",
            "success",
        )
        return redirect(url_for("dashboard"))

    @app.post("/websites/<int:website_id>/run-now")
    @login_required
    def run_site_now(website_id: int):
        website = db.get_website_by_id(website_id)
        if not website:
            abort(404)

        label = site_label(website.get("url"), website.get("display_name"))
        result = monitor.run_check_for_site(
            target_url=str(website["url"]),
            trigger_type="manual_site",
            website_label=(website.get("display_name") or "").strip() or None,
        )
        status = result.get("status")
        if status == "error":
            flash(f"{label}: check failed ({result.get('error', 'Unknown error')})", "error")
        elif status == "skipped":
            flash(f"{label}: run skipped because another run is still in progress.", "warning")
        elif status == "baseline":
            flash(f"{label}: baseline created. Future changes will send alerts.", "success")
        elif status == "new_content":
            if result.get("email_sent"):
                flash(f"{label}: found {result.get('new_items_count', 0)} new item(s), email sent.", "success")
            else:
                flash(
                    f"{label}: found {result.get('new_items_count', 0)} new item(s), email not sent ({result.get('email_error', 'missing settings')}).",
                    "warning",
                )
        else:
            flash(f"{label}: no new content.", "success")
        return redirect(request.referrer or url_for("dashboard"))

    @app.post("/run-now")
    @login_required
    def run_now():
        result = run_all_active_sites(trigger_type="manual_all")
        status = result.get("status", "")
        if status == "error":
            flash(f"Manual run failed: {result.get('error', 'Unknown error')}", "error")
        elif status == "skipped":
            flash("A run is already in progress. This manual all-site run was skipped.", "warning")
        elif status == "no_websites":
            flash("No active websites to monitor. Add at least one URL first.", "warning")
        elif status == "completed":
            sites_checked = int(result.get("sites_checked", 0))
            sites_with_new = int(result.get("sites_with_new_content", 0))
            sites_with_errors = int(result.get("sites_with_errors", 0))
            total_new_items = int(result.get("total_new_items", 0))
            emails_sent = int(result.get("emails_sent", 0))
            flash(
                f"Manual all-site run finished. Sites checked: {sites_checked}, new-content sites: {sites_with_new}, total new items: {total_new_items}, emails sent: {emails_sent}, errors: {sites_with_errors}.",
                "success" if sites_with_errors == 0 else "warning",
            )

            changed_sites = [
                site_label(item.get("target_url"), item.get("website_label"))
                for item in result.get("results", [])
                if item.get("status") == "new_content"
            ]
            if changed_sites:
                preview = ", ".join(changed_sites[:5])
                if len(changed_sites) > 5:
                    preview = f"{preview}, and {len(changed_sites) - 5} more"
                flash(f"Detected changes on: {preview}", "success")
        else:
            flash("Manual all-site run completed.", "success")
        return redirect(request.referrer or url_for("dashboard"))

    @app.get("/health")
    def health():
        db_ok = True
        db_error = None
        try:
            db.ping()
        except Exception as exc:
            db_ok = False
            db_error = str(exc)

        latest = db.get_latest_run() if db_ok else None
        websites = db.get_websites(active_only=None) if db_ok else []
        active_sites = [site for site in websites if bool(site.get("is_active"))]
        payload = {
            "status": "ok" if db_ok else "degraded",
            "ist_time": datetime.now(IST_ZONE).isoformat(),
            "timezone": "Asia/Kolkata",
            "scheduler_running": scheduler.running,
            "next_run_time": format_datetime(scheduler.get_job("website_monitor_job").next_run_time)
            if scheduler.get_job("website_monitor_job")
            else None,
            "database": "postgresql" if config.is_postgres else "sqlite",
            "database_ok": db_ok,
            "database_error": db_error,
            "total_websites": len(websites),
            "active_websites": len(active_sites),
            "active_website_urls": [site.get("url") for site in active_sites[:100]],
            "latest_run": to_json_safe(latest),
        }
        return jsonify(payload), 200 if db_ok else 503

    if should_start_scheduler():
        start_scheduler()
    else:
        logger.info("Scheduler startup skipped for this process.")

    @atexit.register
    def _shutdown_scheduler() -> None:
        if scheduler.running:
            scheduler.shutdown(wait=False)

    return app


app = create_app()


if __name__ == "__main__":
    port = int(os.getenv("PORT", "5000"))
    app.run(host="0.0.0.0", port=port, debug=False)
