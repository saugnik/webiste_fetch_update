from __future__ import annotations

import html
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from smtplib import SMTP
from urllib.parse import urlparse
from zoneinfo import ZoneInfo

from config import AppConfig
from financial_updates import build_financial_insights_for_email, is_financial_relevant
from tev_updates import build_tev_insights_for_email, extract_timeline_hint, is_tev_relevant, summarize_text

IST_ZONE = ZoneInfo("Asia/Kolkata")


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

    def _build_subject(
        self,
        item_count: int,
        target_url: str,
        website_label: str | None,
        alert_summary: str = "",
    ) -> str:
        label = self._site_label(target_url, website_label)
        tag = f"[{alert_summary}] " if alert_summary else ""
        return f"[Website Monitor] {tag}{item_count} update(s) on {label}"

    def _build_html(
        self,
        new_items: list[dict[str, str]],
        run_id: int,
        target_url: str,
        website_label: str | None,
        alert_summary: str = "",
    ) -> str:
        ist_now = datetime.now(IST_ZONE).strftime("%Y-%m-%d %H:%M:%S IST")
        dashboard_link = self.config.dashboard_base_url.rstrip("/")
        history_link = f"{dashboard_link}/run/{run_id}"
        site_label = self._site_label(target_url, website_label)

        financial_insights = build_financial_insights_for_email(new_items)
        tev_insights = build_tev_insights_for_email(new_items)
        fin_count = len(financial_insights)
        tev_count = len(tev_insights)

        # ── helpers ──────────────────────────────────────────────────────────
        def badge(text: str, color: str) -> str:
            return (
                f"<span style='display:inline-block;padding:2px 8px;border-radius:12px;"
                f"font-size:11px;font-weight:bold;background:{color};color:#fff;"
                f"text-transform:uppercase;letter-spacing:.5px'>{html.escape(text)}</span>"
            )

        def card_open(border: str, bg: str) -> str:
            return (
                f"<div style='border-left:4px solid {border};background:{bg};"
                f"padding:14px 16px;margin-bottom:14px;border-radius:4px;"
                f"box-shadow:0 1px 3px rgba(0,0,0,.06)'>"
            )

        def field(label: str, value: str) -> str:
            if not value or value.strip() in ("-", "", "Not specified in source.", "Not clearly mentioned"):
                return ""
            return (
                f"<div style='margin-top:6px;font-size:13px'>"
                f"<span style='color:#64748b;font-weight:600;min-width:110px;display:inline-block'>"
                f"{html.escape(label)}:</span> {html.escape(value)}</div>"
            )

        def source_field(label: str, url: str) -> str:
            if not url:
                return ""
            safe = html.escape(url)
            return (
                f"<div style='margin-top:6px;font-size:13px'>"
                f"<span style='color:#64748b;font-weight:600;min-width:110px;display:inline-block'>"
                f"{html.escape(label)}:</span> "
                f"<a href='{safe}' style='color:#2563eb;word-break:break-all'>{safe}</a></div>"
            )

        # ── Financial section ─────────────────────────────────────────────────
        financial_section = ""
        if financial_insights:
            cards = ""
            for i, ins in enumerate(financial_insights, 1):
                cat = ins.get("category", "").replace("_", " ").title()
                cat_colors = {
                    "Tev Empanelment": "#16a34a",
                    "Project Finance": "#0369a1",
                    "Consultancy": "#7c3aed",
                    "Tev Related": "#d97706",
                }
                cat_color = cat_colors.get(cat, "#374151")
                cards += card_open("#16a34a", "#f0fdf4")
                cards += (
                    f"<div style='display:flex;align-items:center;gap:8px;flex-wrap:wrap'>"
                    f"<strong style='font-size:14px;color:#166534'>#{i}</strong>"
                    f"{badge(cat, cat_color)}"
                    f"</div>"
                )
                cards += field("Key numbers", ins.get("key_numbers", ""))
                cards += field("Timeline", ins.get("timeline_hint", ""))
                cards += (
                    f"<div style='margin-top:8px;font-size:13px;color:#1e293b;line-height:1.55'>"
                    f"{html.escape(ins.get('summary', ''))}</div>"
                )
                cards += source_field("Source", ins.get("source_url", ""))
                cards += "</div>"

            financial_section = (
                "<div style='margin-top:24px'>"
                "<h3 style='margin:0 0 12px;color:#166534;font-size:16px;display:flex;align-items:center;gap:8px'>"
                "💰 Financial Data Detected"
                f"<span style='font-size:12px;background:#dcfce7;color:#166534;padding:2px 8px;"
                f"border-radius:10px;font-weight:600'>{fin_count} item(s)</span></h3>"
                + cards +
                "</div>"
            )

        # ── TEV section ───────────────────────────────────────────────────────
        tev_section = ""
        if tev_insights:
            status_colors = {
                "open": "#16a34a", "closed": "#dc2626",
                "upcoming": "#d97706", "not_specified": "#64748b",
            }
            notice_colors = {"yes": "#16a34a", "not_clear": "#64748b"}
            cards = ""
            for i, ins in enumerate(tev_insights, 1):
                status = ins.get("status", "not_specified").lower()
                notice = ins.get("notice_released", "not_clear").lower()
                sc = status_colors.get(status, "#64748b")
                nc = notice_colors.get(notice, "#64748b")
                cards += card_open("#2563eb", "#eff6ff")
                cards += (
                    f"<div style='display:flex;align-items:center;gap:8px;flex-wrap:wrap'>"
                    f"<strong style='font-size:14px;color:#1e40af'>#{i}</strong>"
                    f"{badge('Status: ' + status.replace('_', ' '), sc)}"
                    f"{badge('Notice: ' + notice.replace('_', ' '), nc)}"
                    f"</div>"
                )
                cards += field("Timeline", ins.get("timeline_hint", ""))
                cards += (
                    f"<div style='margin-top:8px;font-size:13px;color:#1e293b;line-height:1.55'>"
                    f"{html.escape(ins.get('summary', ''))}</div>"
                )
                cards += source_field("Source", ins.get("source_url", ""))
                cards += "</div>"

            tev_section = (
                "<div style='margin-top:24px'>"
                "<h3 style='margin:0 0 12px;color:#1e40af;font-size:16px;display:flex;align-items:center;gap:8px'>"
                "🏦 TEV / Empanelment Data Detected"
                f"<span style='font-size:12px;background:#dbeafe;color:#1e40af;padding:2px 8px;"
                f"border-radius:10px;font-weight:600'>{tev_count} item(s)</span></h3>"
                + cards +
                "</div>"
            )

        # ── Footer raw items (collapsed at bottom) ────────────────────────────
        raw_rows = []
        for item in new_items[:50]:
            text = str(item.get("text") or "")
            tags = []
            if is_tev_relevant(text):
                tags.append("TEV")
            if is_financial_relevant(text):
                tags.append("Financial")
            tag_label = " + ".join(tags) if tags else "Other"
            src = (item.get("source_url") or "").strip()
            src_html = f"<a href='{html.escape(src)}' style='color:#2563eb'>{html.escape(src)}</a>" if src else "—"
            raw_rows.append(
                f"<tr>"
                f"<td style='border:1px solid #e2e8f0;padding:7px 10px;font-size:12px;white-space:nowrap'>"
                f"{html.escape(tag_label)}</td>"
                f"<td style='border:1px solid #e2e8f0;padding:7px 10px;font-size:12px;color:#374151;"
                f"line-height:1.4'>{html.escape(text[:300])}{'…' if len(text)>300 else ''}</td>"
                f"<td style='border:1px solid #e2e8f0;padding:7px 10px;font-size:12px'>{src_html}</td>"
                f"</tr>"
            )
        extra = len(new_items) - 50
        raw_table = (
            "<details style='margin-top:24px'>"
            "<summary style='cursor:pointer;font-size:13px;color:#64748b;padding:6px 0'>"
            f"▶ View all {len(new_items)} raw detected items</summary>"
            "<div style='margin-top:10px;overflow-x:auto'>"
            "<table style='width:100%;border-collapse:collapse;font-size:12px'>"
            "<thead><tr>"
            "<th style='border:1px solid #e2e8f0;padding:7px 10px;background:#f8fafc;text-align:left'>Type</th>"
            "<th style='border:1px solid #e2e8f0;padding:7px 10px;background:#f8fafc;text-align:left'>Detected text</th>"
            "<th style='border:1px solid #e2e8f0;padding:7px 10px;background:#f8fafc;text-align:left'>Source</th>"
            "</tr></thead>"
            f"<tbody>{''.join(raw_rows)}</tbody>"
            "</table>"
            + (f"<p style='font-size:12px;color:#64748b;margin-top:8px'>…and {extra} more items.</p>" if extra > 0 else "")
            + "</div></details>"
        )

        return f"""<!doctype html>
<html>
<body style="font-family:'Segoe UI',Arial,sans-serif;background:#f1f5f9;padding:24px;color:#1e293b;margin:0">
  <div style="max-width:720px;margin:0 auto">

    <!-- Header -->
    <div style="background:linear-gradient(135deg,#1e40af 0%,#0369a1 100%);
                border-radius:10px 10px 0 0;padding:20px 24px;color:#fff">
      <div style="font-size:12px;opacity:.8;letter-spacing:.5px;text-transform:uppercase">
        Website Monitor Alert
      </div>
      <h1 style="margin:6px 0 0;font-size:20px;font-weight:700">
        {html.escape(alert_summary or "New Content Detected")}
      </h1>
      <div style="margin-top:8px;font-size:13px;opacity:.85">
        <strong>{html.escape(site_label)}</strong> &nbsp;·&nbsp; {ist_now}
      </div>
    </div>

    <!-- Body card -->
    <div style="background:#fff;border:1px solid #e2e8f0;border-top:none;
                border-radius:0 0 10px 10px;padding:24px">

      <!-- Meta row -->
      <div style="display:flex;gap:16px;flex-wrap:wrap;margin-bottom:16px">
        <div style="font-size:13px;color:#64748b">
          <strong>Source:</strong>
          <a href="{html.escape(target_url)}" style="color:#2563eb">{html.escape(target_url)}</a>
        </div>
        <div style="font-size:13px;color:#64748b">
          <strong>Run:</strong>
          <a href="{html.escape(history_link)}" style="color:#2563eb">#{run_id}</a>
        </div>
        <div style="font-size:13px;color:#64748b">
          <strong>Total new items:</strong> {len(new_items)}
          &nbsp;|&nbsp; <strong>Financial:</strong> {fin_count}
          &nbsp;|&nbsp; <strong>TEV:</strong> {tev_count}
        </div>
      </div>

      <hr style="border:none;border-top:1px solid #e2e8f0;margin:0 0 4px">

      {financial_section}
      {tev_section}
      {raw_table}

      <!-- Footer links -->
      <div style="margin-top:24px;padding-top:16px;border-top:1px solid #e2e8f0;
                  font-size:12px;color:#94a3b8;text-align:center">
        <a href="{html.escape(dashboard_link)}" style="color:#2563eb;margin-right:16px">Open Dashboard</a>
        <a href="{html.escape(history_link)}" style="color:#2563eb">View This Run #{run_id}</a>
      </div>

    </div>
  </div>
</body>
</html>"""



    @staticmethod
    def _build_plain_text(
        new_items: list[dict[str, str]],
        target_url: str,
        website_label: str | None,
        run_id: int,
        base_url: str,
    ) -> str:
        financial_insights = build_financial_insights_for_email(new_items)
        tev_insights = build_tev_insights_for_email(new_items)
        lines = [
            "Website Monitor Alert",
            "",
            f"Website: {website_label or (urlparse(target_url).netloc or target_url)}",
            f"Target URL: {target_url}",
            f"Run ID: {run_id}",
            f"New monitored updates found: {len(new_items)}",
            f"Dashboard: {base_url}",
            f"Run details: {base_url.rstrip('/')}/run/{run_id}",
            "",
            f"Financial matches: {len(financial_insights)} | TEV matches: {len(tev_insights)}",
            "",
            "Change summary:",
        ]
        for idx, item in enumerate(new_items[:30], start=1):
            text = str(item.get("text") or "")
            tags = []
            if is_tev_relevant(text):
                tags.append("TEV")
            if is_financial_relevant(text):
                tags.append("Financial")
            tag_label = ", ".join(tags) if tags else "Other"
            summary_text = summarize_text(text, max_chars=160)
            timeline_hint = extract_timeline_hint(text)
            source_url = (item.get("source_url") or "").strip()
            lines.append(f"{idx}. [{tag_label}] {summary_text}")
            lines.append(f"   Timeline: {timeline_hint}")
            if source_url:
                lines.append(f"   Source: {source_url}")
        remaining = len(new_items) - 30
        if remaining > 0:
            lines.append(f"...and {remaining} more item(s).")

        if financial_insights:
            lines.append("")
            lines.append("Financial update summary (from monitored website text):")
            for idx, insight in enumerate(financial_insights, start=1):
                lines.append(f"{idx}. Category: {insight['category']}")
                lines.append(f"   Summary: {insight['summary']}")
                lines.append(f"   Key numbers: {insight['key_numbers']}")
                lines.append(f"   Timeline: {insight['timeline_hint']}")
                source_url = (insight.get("source_url") or "").strip()
                lines.append(f"   Reference: {source_url or 'Not available'}")

        tev_insights = build_tev_insights_for_email(new_items)
        if tev_insights:
            lines.append("")
            lines.append("TEV empanelment summary (from monitored website text):")
            for idx, insight in enumerate(tev_insights, start=1):
                lines.append(f"{idx}. Status: {insight['status']}")
                lines.append(f"   Notice: {insight['notice_released']}")
                lines.append(f"   Summary: {insight['summary']}")
                lines.append(f"   Timeline: {insight['timeline_hint']}")
                source_url = (insight.get('source_url') or '').strip()
                lines.append(f"   Reference: {source_url or 'Not available'}")
        return "\n".join(lines)

    def send_new_content_alert(
        self,
        new_items: list[dict[str, str]],
        run_id: int,
        target_url: str,
        website_label: str | None = None,
        alert_summary: str = "",
    ) -> tuple[str, list[str]]:
        if not self.is_configured:
            raise RuntimeError(
                "Email is not configured. Set SMTP_USERNAME, SMTP_PASSWORD, ALERT_FROM_EMAIL, and ALERT_TO_EMAILS."
            )

        subject = self._build_subject(
            item_count=len(new_items),
            target_url=target_url,
            website_label=website_label,
            alert_summary=alert_summary,
        )
        html_body = self._build_html(
            new_items=new_items,
            run_id=run_id,
            target_url=target_url,
            website_label=website_label,
            alert_summary=alert_summary,
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
