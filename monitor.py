from __future__ import annotations

import logging
import threading
from typing import Any

from db import Database
from emailer import EmailClient
from financial_updates import is_financial_relevant
from tev_updates import is_tev_relevant
from scraper import compute_snapshot_hash, extract_text_items, fetch_page, fingerprint_text

from config import AppConfig


logger = logging.getLogger(__name__)


class MonitorService:
    def __init__(self, config: AppConfig, db: Database, email_client: EmailClient) -> None:
        self.config = config
        self.db = db
        self.email_client = email_client
        # Global lock: prevents two all-site scheduled runs from overlapping.
        self._global_run_lock = threading.Lock()
        # Per-site locks: allows a single-site manual run to proceed independently.
        self._site_locks: dict[str, threading.Lock] = {}
        self._site_locks_mutex = threading.Lock()

    def _get_site_lock(self, url: str) -> threading.Lock:
        with self._site_locks_mutex:
            if url not in self._site_locks:
                self._site_locks[url] = threading.Lock()
            return self._site_locks[url]

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
        """Run a check for a single site. Uses a per-site lock so it never
        blocks (or gets blocked by) checks on other sites."""
        site_lock = self._get_site_lock(target_url)
        if not site_lock.acquire(blocking=False):
            logger.warning(
                "Skipped single-site run because a check for this site is already in progress. site=%s",
                target_url,
            )
            return {
                "status": "skipped",
                "reason": "already_running",
                "target_url": target_url,
                "website_label": website_label,
            }
        try:
            return self._run_check_core(
                target_url=target_url,
                trigger_type=trigger_type,
                website_label=website_label,
            )
        finally:
            site_lock.release()

    def run_all_sites(self, websites: list[dict[str, Any]], trigger_type: str = "scheduled") -> dict[str, Any]:
        """Run checks for all active sites sequentially. Uses the global lock
        so two all-site runs never overlap, but single-site manual runs are
        unaffected."""
        if not self._global_run_lock.acquire(blocking=False):
            logger.warning("Skipped all-site run because a previous scheduled run is still in progress.")
            return {"status": "skipped", "reason": "already_running", "results": []}

        try:
            active_sites = [site for site in websites if str(site.get("url", "")).strip()]
            if not active_sites:
                return {"status": "no_websites", "reason": "no_active_websites", "results": []}

            results: list[dict[str, Any]] = []
            for site in active_sites:
                url = str(site["url"]).strip()
                label = (site.get("display_name") or "").strip() or None

                # Acquire per-site lock so a concurrent manual run for this
                # specific site doesn't race with us.
                site_lock = self._get_site_lock(url)
                if not site_lock.acquire(blocking=True, timeout=10):
                    logger.warning("Could not acquire per-site lock for %s during all-site run. Skipping.", url)
                    results.append({
                        "status": "skipped",
                        "reason": "site_lock_timeout",
                        "target_url": url,
                        "website_label": label,
                    })
                    continue
                try:
                    result = self._run_check_core(
                        target_url=url,
                        trigger_type=trigger_type,
                        website_label=label,
                    )
                finally:
                    site_lock.release()

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
            self._global_run_lock.release()

    def _run_check_core(
        self,
        target_url: str,
        trigger_type: str,
        website_label: str | None,
    ) -> dict[str, Any]:
        """Core check logic — shared by both single-site and all-site runs.
        Caller is responsible for holding the appropriate lock."""
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
                if not (is_financial_relevant(text) or is_tev_relevant(text)):
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

            # Only genuinely new items (not seen before) trigger alerts.
            # Baseline run items are marked notified immediately, so they never appear here.
            new_items_for_alert = inserted_items


            if not has_existing_baseline and unique_items:
                # ── First run: this is the baseline snapshot ───────────────────────
                # Mark every item as already-notified immediately so that future
                # runs only alert on content that is genuinely NEW (i.e. added to
                # the site after we first fetched it).
                run_status = "baseline"
                self.db.mark_items_notified(
                    target_url=cleaned_url,
                    fingerprints=[item["fingerprint"] for item in unique_items],
                )
                new_items_for_alert = []   # nothing to alert on for the baseline
            else:
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
                # ── Split into financial vs TEV buckets ──────────────────────────
                # Email is ONLY sent when at least one bucket has content.
                # Items are already pre-filtered to financial/TEV at extraction time,
                # but we re-check here to be explicit and build a clear subject line.
                financial_new = [
                    item for item in new_items_for_alert
                    if is_financial_relevant(str(item.get("text") or ""))
                ]
                tev_new = [
                    item for item in new_items_for_alert
                    if is_tev_relevant(str(item.get("text") or ""))
                ]
                has_relevant_alert = bool(financial_new or tev_new)

                if not has_relevant_alert:
                    # Inserted items don't match either category — store but don't email.
                    logger.info(
                        "New items inserted but none are financial/TEV relevant. "
                        "No email sent. run_id=%s site=%s count=%s",
                        run_id, cleaned_url, len(new_items_for_alert),
                    )
                else:
                    # Build a clear summary for logs and subject.
                    parts = []
                    if financial_new:
                        parts.append(f"{len(financial_new)} Financial")
                    if tev_new:
                        parts.append(f"{len(tev_new)} TEV")
                    alert_summary = " + ".join(parts)
                    logger.info(
                        "FINANCIAL/TEV ALERT TRIGGERED. run_id=%s site=%s detected=[%s] — sending email.",
                        run_id, cleaned_url, alert_summary,
                    )

                    if self.email_client.is_configured:
                        try:
                            subject, recipients = self.email_client.send_new_content_alert(
                                new_items=new_items_for_alert,
                                run_id=run_id,
                                target_url=cleaned_url,
                                website_label=website_label,
                                alert_summary=alert_summary,
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
                            logger.info(
                                "Email alert SENT. run_id=%s recipients=%s subject=%r",
                                run_id, recipients, subject,
                            )
                        except Exception as exc:
                            email_error = str(exc)
                            logger.exception(
                                "EMAIL SEND FAILED for run_id=%s site=%s error=%s",
                                run_id, cleaned_url, exc,
                            )
                            subject = (
                                f"[Website Monitor] [{alert_summary}] update(s) on {site_label}"
                            )
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
                        logger.warning(
                            "FINANCIAL/TEV content detected but EMAIL IS NOT CONFIGURED. run_id=%s "
                            "Check SMTP_USERNAME, SMTP_PASSWORD, ALERT_FROM_EMAIL, ALERT_TO_EMAILS in .env",
                            run_id,
                        )
            else:
                reason = "baseline snapshot captured" if run_status == "baseline" else "no new financial/TEV content"
                logger.info(
                    "No email sent. run_id=%s site=%s reason=%r",
                    run_id, cleaned_url, reason,
                )

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
