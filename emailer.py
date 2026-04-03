from __future__ import annotations

import html
from datetime import datetime, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from smtplib import SMTP
from urllib.parse import urlparse

from config import AppConfig


class EmailClient:
    def __init__(self, config: AppConfig) -> None:
        self.config = config

    @property
    def is_configured(self) -> bool:
        return bool(
            self.config.smtp_username
            and self.config.smtp_password
            and self.config.alert_from_email
            and self.config.alert_to_emails
        )

    @staticmethod
    def _site_label(target_url: str, website_label: str | None) -> str:
        cleaned = (website_label or "").strip()
        if cleaned:
            return cleaned
        return urlparse(target_url).netloc or target_url

    def _build_subject(self, item_count: int, target_url: str, website_label: str | None) -> str:
        label = self._site_label(target_url, website_label)
        return f"[Website Monitor] {item_count} new item(s) detected on {label}"

    def _build_html(
        self,
        new_items: list[dict[str, str]],
        run_id: int,
        target_url: str,
        website_label: str | None,
    ) -> str:
        utc_now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        dashboard_link = self.config.dashboard_base_url.rstrip("/")
        history_link = f"{dashboard_link}/run/{run_id}"
        site_label = self._site_label(target_url, website_label)

        rendered_items = []
        for item in new_items[:100]:
            safe_text = html.escape(item["text"])
            rendered_items.append(f"<li style='margin-bottom:8px; line-height:1.4'>{safe_text}</li>")
        remaining_count = max(0, len(new_items) - 100)

        overflow_html = ""
        if remaining_count > 0:
            overflow_html = (
                f"<p style='margin-top:12px;'>...and {remaining_count} more item(s). "
                f"Open the dashboard for the full list.</p>"
            )

        return f"""
<!doctype html>
<html>
  <body style="font-family:Arial,sans-serif;background:#f5f7fb;padding:20px;color:#222;">
    <div style="max-width:760px;margin:0 auto;background:#fff;padding:24px;border-radius:10px;border:1px solid #d9e1ec;">
      <h2 style="margin-top:0;">Website Monitor Alert</h2>
      <p><strong>Website:</strong> {html.escape(site_label)}</p>
      <p><strong>Target URL:</strong> {html.escape(target_url)}</p>
      <p><strong>Detected at:</strong> {utc_now}</p>
      <p><strong>New items found:</strong> {len(new_items)}</p>
      <p>
        <a href="{html.escape(dashboard_link)}">Open Dashboard</a> |
        <a href="{html.escape(history_link)}">Open This Run</a>
      </p>
      <hr style="border:none;border-top:1px solid #e5eaf2;margin:20px 0;">
      <ol style="padding-left:20px;">
        {''.join(rendered_items)}
      </ol>
      {overflow_html}
    </div>
  </body>
</html>
"""

    @staticmethod
    def _build_plain_text(
        new_items: list[dict[str, str]],
        target_url: str,
        website_label: str | None,
        run_id: int,
        base_url: str,
    ) -> str:
        lines = [
            "Website Monitor Alert",
            "",
            f"Website: {website_label or (urlparse(target_url).netloc or target_url)}",
            f"Target URL: {target_url}",
            f"Run ID: {run_id}",
            f"New items found: {len(new_items)}",
            f"Dashboard: {base_url}",
            f"Run details: {base_url.rstrip('/')}/run/{run_id}",
            "",
            "New content:",
        ]
        for idx, item in enumerate(new_items[:100], start=1):
            lines.append(f"{idx}. {item['text']}")
        remaining = len(new_items) - 100
        if remaining > 0:
            lines.append(f"...and {remaining} more item(s).")
        return "\n".join(lines)

    def send_new_content_alert(
        self,
        new_items: list[dict[str, str]],
        run_id: int,
        target_url: str,
        website_label: str | None = None,
    ) -> tuple[str, list[str]]:
        if not self.is_configured:
            raise RuntimeError(
                "Email is not configured. Set SMTP_USERNAME, SMTP_PASSWORD, ALERT_FROM_EMAIL, and ALERT_TO_EMAILS."
            )

        subject = self._build_subject(
            item_count=len(new_items),
            target_url=target_url,
            website_label=website_label,
        )
        html_body = self._build_html(
            new_items=new_items,
            run_id=run_id,
            target_url=target_url,
            website_label=website_label,
        )
        text_body = self._build_plain_text(
            new_items=new_items,
            target_url=target_url,
            website_label=website_label,
            run_id=run_id,
            base_url=self.config.dashboard_base_url,
        )

        message = MIMEMultipart("alternative")
        message["Subject"] = subject
        message["From"] = self.config.alert_from_email
        message["To"] = ", ".join(self.config.alert_to_emails)
        message.attach(MIMEText(text_body, "plain", "utf-8"))
        message.attach(MIMEText(html_body, "html", "utf-8"))

        with SMTP(self.config.smtp_host, self.config.smtp_port, timeout=30) as smtp:
            smtp.ehlo()
            if self.config.smtp_use_tls:
                smtp.starttls()
                smtp.ehlo()
            smtp.login(self.config.smtp_username, self.config.smtp_password)
            smtp.sendmail(
                self.config.alert_from_email,
                self.config.alert_to_emails,
                message.as_string(),
            )

        return subject, self.config.alert_to_emails
