from __future__ import annotations

import logging
import threading
from typing import Any

from db import Database
from emailer import EmailClient
from scraper import compute_snapshot_hash, extract_text_items, fetch_page, fingerprint_text

from config import AppConfig


logger = logging.getLogger(__name__)


class MonitorService:
    def __init__(self, config: AppConfig, db: Database, email_client: EmailClient) -> None:
        self.config = config
        self.db = db
        self.email_client = email_client
        self._run_lock = threading.Lock()

    @staticmethod
    def _site_label(target_url: str, website_label: str | None) -> str:
        cleaned = (website_label or "").strip()
        if cleaned:
            return cleaned
        return target_url

    def run_check_for_site(
        self,
        target_url: str,
        trigger_type: str = "scheduled",
        website_label: str | None = None,
    ) -> dict[str, Any]:
        if not self._run_lock.acquire(blocking=False):
            logger.warning("Skipped monitor run because a previous run is still in progress.")
            return {
                "status": "skipped",
                "reason": "already_running",
                "target_url": target_url,
                "website_label": website_label,
            }
        try:
            return self._run_check_locked(
                target_url=target_url,
                trigger_type=trigger_type,
                website_label=website_label,
            )
        finally:
            self._run_lock.release()

    def run_all_sites(self, websites: list[dict[str, Any]], trigger_type: str = "scheduled") -> dict[str, Any]:
        if not self._run_lock.acquire(blocking=False):
            logger.warning("Skipped all-site run because a previous run is still in progress.")
            return {"status": "skipped", "reason": "already_running", "results": []}

        try:
            active_sites = [site for site in websites if str(site.get("url", "")).strip()]
            if not active_sites:
                return {"status": "no_websites", "reason": "no_active_websites", "results": []}

            results: list[dict[str, Any]] = []
            for site in active_sites:
                result = self._run_check_locked(
                    target_url=str(site["url"]).strip(),
                    trigger_type=trigger_type,
                    website_label=(site.get("display_name") or "").strip() or None,
                )
                results.append(result)

            return {
                "status": "completed",
                "sites_checked": len(results),
                "sites_with_new_content": sum(1 for item in results if item.get("status") == "new_content"),
                "sites_with_errors": sum(1 for item in results if item.get("status") == "error"),
                "total_new_items": sum(int(item.get("new_items_count", 0) or 0) for item in results),
                "emails_sent": sum(1 for item in results if item.get("email_sent") is True),
                "results": results,
            }
        finally:
            self._run_lock.release()

    def _run_check_locked(
        self,
        target_url: str,
        trigger_type: str,
        website_label: str | None,
    ) -> dict[str, Any]:
        cleaned_url = target_url.strip()
        if not cleaned_url:
            return {"status": "error", "error": "Target URL cannot be empty."}

        run_id: int | None = None
        site_label = self._site_label(cleaned_url, website_label)
        try:
            run_id = self.db.create_run(cleaned_url, trigger_type)
            logger.info(
                "Monitor run started. run_id=%s trigger=%s site=%s",
                run_id,
                trigger_type,
                cleaned_url,
            )

            has_existing_baseline = self.db.has_any_items(cleaned_url)

            scrape_result = fetch_page(
                url=cleaned_url,
                timeout_seconds=self.config.request_timeout_seconds,
                user_agent=self.config.request_user_agent,
            )

            if scrape_result.status_code >= 400:
                raise RuntimeError(f"Target returned HTTP {scrape_result.status_code}")

            extracted_items = extract_text_items(scrape_result.html, base_url=cleaned_url)
            unique_items: list[dict[str, Any]] = []
            seen_fingerprints: set[str] = set()
            for extracted in extracted_items:
                text = str(extracted.get("text") or "").strip()
                source_url = str(extracted.get("source_url") or "").strip() or None
                if not text:
                    continue
                fingerprint = fingerprint_text(text)
                if fingerprint in seen_fingerprints:
                    continue
                seen_fingerprints.add(fingerprint)
                unique_items.append(
                    {
                        "fingerprint": fingerprint,
                        "text": text,
                        "source_url": source_url,
                    }
                )

            inserted_items = self.db.insert_new_items(
                run_id=run_id,
                target_url=cleaned_url,
                items=unique_items,
            )
            snapshot_hash = compute_snapshot_hash([item["text"] for item in unique_items])

            if not has_existing_baseline:
                new_items_for_alert: list[dict[str, str]] = []
                run_status = "baseline"
            else:
                new_items_for_alert = inserted_items
                run_status = "new_content" if new_items_for_alert else "success"

            self.db.complete_run(
                run_id=run_id,
                status=run_status,
                http_status=scrape_result.status_code,
                response_time_ms=scrape_result.response_time_ms,
                snapshot_hash=snapshot_hash,
                total_items=len(unique_items),
                new_items_count=len(new_items_for_alert),
                raw_content_length=scrape_result.content_length,
                error_message=None,
            )

            email_sent = False
            email_error = None
            if new_items_for_alert:
                if self.email_client.is_configured:
                    try:
                        subject, recipients = self.email_client.send_new_content_alert(
                            new_items=new_items_for_alert,
                            run_id=run_id,
                            target_url=cleaned_url,
                            website_label=website_label,
                        )
                        for recipient in recipients:
                            self.db.record_email_alert(
                                run_id=run_id,
                                recipient=recipient,
                                subject=subject,
                                status="sent",
                                error_message=None,
                            )
                        self.db.mark_items_notified(
                            target_url=cleaned_url,
                            fingerprints=[item["fingerprint"] for item in new_items_for_alert],
                        )
                        email_sent = True
                    except Exception as exc:
                        email_error = str(exc)
                        logger.exception("Failed to send email for run_id=%s site=%s", run_id, cleaned_url)
                        subject = f"[Website Monitor] New content detected on {site_label}"
                        for recipient in self.config.alert_to_emails:
                            self.db.record_email_alert(
                                run_id=run_id,
                                recipient=recipient,
                                subject=subject,
                                status="failed",
                                error_message=email_error,
                            )
                else:
                    email_error = "Email settings are missing."
                    logger.warning("New content detected but email is not configured. run_id=%s", run_id)

            logger.info(
                "Monitor run finished. run_id=%s site=%s status=%s new_items=%s email_sent=%s",
                run_id,
                cleaned_url,
                run_status,
                len(new_items_for_alert),
                email_sent,
            )
            return {
                "run_id": run_id,
                "target_url": cleaned_url,
                "website_label": website_label,
                "status": run_status,
                "new_items_count": len(new_items_for_alert),
                "email_sent": email_sent,
                "email_error": email_error,
                "http_status": scrape_result.status_code,
                "response_time_ms": scrape_result.response_time_ms,
            }
        except Exception as exc:
            logger.exception("Monitor run failed. run_id=%s site=%s", run_id, cleaned_url)
            if run_id is not None:
                self.db.complete_run(
                    run_id=run_id,
                    status="error",
                    http_status=None,
                    response_time_ms=None,
                    snapshot_hash=None,
                    total_items=None,
                    new_items_count=0,
                    raw_content_length=None,
                    error_message=str(exc),
                )
            return {
                "run_id": run_id,
                "target_url": cleaned_url,
                "website_label": website_label,
                "status": "error",
                "error": str(exc),
            }
