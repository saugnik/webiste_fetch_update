from __future__ import annotations

# tev_updates.py
#
# Detects TEV (Techno Economic Viability) and LIE (Lender's Independent
# Engineer) empanelment notices from bank/FI websites.
#
# Strict matching: a text block must explicitly mention TEV, TEVS, LIE,
# or "Lender's Independent Engineer" — generic "empanelment" alone is
# NOT enough.

import re
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlparse


# ---------------------------------------------------------------------------
# Primary TEV/LIE keyword phrases
# ---------------------------------------------------------------------------

TEV_PHRASES = (
    "techno economic viability",
    "techno-economic viability",
    "techno economic viability study",
    "techno-economic viability study",
    "tev study",
    "tev/lie",
    "lie/tev",
    "tev & lie",
    "tev and lie",
    "lender's independent engineer",
    "lenders independent engineer",
    "lender s independent engineer",
    "independent engineer and tev",
    "tev consultant",
    "lie consultant",
    "empanelment of tev",
    "empanelment for tev",
    "empanelment of lie",
    "empanelment for lie",
    "tev empanelment",
    "lie empanelment",
    "tevs consultant",
    "techno economic study",
    "techno-economic study",
    "notice for empanelment of consultant for techno",
    "notice inviting applications for tev",
)

TEV_TOKEN_PATTERN = re.compile(r"\btev(s)?\b", re.IGNORECASE)
LIE_TOKEN_PATTERN = re.compile(r"\blie\b", re.IGNORECASE)
LENDER_ENGINEER_PATTERN = re.compile(r"lender.?s\s+independent\s+engineer", re.IGNORECASE)

EMPANELMENT_CONTEXT = (
    "empanelment",
    "consultant",
    "engineer",
    "viability",
    "appraisal",
    "notice",
    "application",
    "invite",
    "invited",
    "rfp",
    "eoi",
    "last date",
    "submission",
)

NOTICE_KEYWORDS = (
    "notice",
    "circular",
    "tender",
    "rfp",
    "request for proposal",
    "invitation",
    "applications invited",
    "empanelment",
    "expression of interest",
    "eoi",
)

STATUS_OPEN_KEYWORDS = (
    "applications invited",
    "invited",
    "apply now",
    "submission open",
    "last date",
    "accepting applications",
    "open for",
    "inviting applications",
    "notice inviting",
)

STATUS_CLOSED_KEYWORDS = (
    "closed",
    "concluded",
    "ended",
    "deadline over",
    "no longer accepting",
    "submission period ended",
    "empanelment closed",
)

STATUS_UPCOMING_KEYWORDS = (
    "upcoming",
    "expected",
    "likely",
    "proposed",
    "to be released",
    "will be released",
    "soon",
    "shortly",
)

MONTHS_PATTERN = (
    "jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|"
    "jul(?:y)?|aug(?:ust)?|sep(?:tember)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?"
)

# FIX: date patterns now require proper 4-digit year (not "24-25" or "26/08/001")
# and use word boundaries carefully to avoid matching reference numbers like
# "24-25/003", "26/08/001", "2025-26/08" etc.
DATE_PATTERNS = [
    # "15 March 2025" or "15 Mar 2025"
    re.compile(rf"\b(\d{{1,2}})\s+({MONTHS_PATTERN})[,]?\s+(\d{{4}})\b", re.IGNORECASE),
    # "March 15, 2025" or "Mar 15 2025"
    re.compile(rf"\b({MONTHS_PATTERN})\s+(\d{{1,2}})[,]?\s+(\d{{4}})\b", re.IGNORECASE),
    # "15/03/2025" or "15-03-2025" — must have full 4-digit year
    re.compile(r"\b(\d{1,2})[/-](\d{1,2})[/-](20\d{2})\b"),
    # "15.03.2025" — must have full 4-digit year
    re.compile(r"\b(\d{1,2})[.](\d{1,2})[.](20\d{2})\b"),
]


@dataclass
class TevInsight:
    bank_label: str
    target_url: str
    detected_at: Any
    status: str
    notice_released: str
    timeline_hint: str
    summary: str
    source_url: str | None
    run_id: int | None


def _normalize_text(value: Any) -> str:
    text = str(value or "")
    compact = re.sub(r"\s+", " ", text).strip()
    return compact


def _contains_any(text: str, keywords: tuple[str, ...]) -> bool:
    lowered = text.lower()
    return any(keyword in lowered for keyword in keywords)


def is_tev_relevant(text: str) -> bool:
    """
    Returns True ONLY if the text explicitly mentions TEV/LIE empanelment.
    Generic "empanelment" alone will NOT match.
    """
    if not text:
        return False

    lowered = text.lower()

    # Rule 1: explicit TEV phrases
    if any(phrase in lowered for phrase in TEV_PHRASES):
        return True

    # Rule 2: "Lender's Independent Engineer" pattern
    if LENDER_ENGINEER_PATTERN.search(lowered):
        return True

    # Rule 3: standalone "tev"/"tevs" token + empanelment context word
    if TEV_TOKEN_PATTERN.search(lowered):
        if _contains_any(lowered, EMPANELMENT_CONTEXT):
            return True

    # Rule 4: standalone "lie" + engineer/lender context + empanelment context
    if LIE_TOKEN_PATTERN.search(lowered):
        has_engineer_context = (
            "independent engineer" in lowered or "lender" in lowered
        )
        if has_engineer_context and _contains_any(lowered, EMPANELMENT_CONTEXT):
            return True

    return False


def classify_status(text: str) -> tuple[str, str]:
    lowered = text.lower()
    if _contains_any(lowered, STATUS_CLOSED_KEYWORDS):
        return "closed", "Found closed/concluded wording in detected text."
    if _contains_any(lowered, STATUS_OPEN_KEYWORDS):
        return "open", "Found open/invitation wording in detected text."
    if _contains_any(lowered, STATUS_UPCOMING_KEYWORDS):
        return "upcoming", "Found upcoming/expected wording in detected text."
    return "not_specified", "No explicit open/closed/upcoming wording found."


def notice_status(text: str) -> str:
    if _contains_any(text, NOTICE_KEYWORDS):
        return "yes"
    return "not_clear"


def extract_timeline_hint(text: str) -> str:
    """
    Extract a real date from text. Returns the matched date string or a
    descriptive fallback. Avoids matching reference numbers like '24-25/003'.
    """
    for pattern in DATE_PATTERNS:
        match = pattern.search(text)
        if match:
            return match.group(0).strip()

    lowered = text.lower()
    if "last date" in lowered:
        # Try to extract what follows "last date"
        m = re.search(r"last date[^:]*[:\-]?\s*([^\n.]{5,40})", lowered)
        if m:
            return f"Last date: {m.group(1).strip()}"
        return "Last date mentioned (see source text)."
    if "due date" in lowered:
        return "Due date mentioned (see source text)."
    if "for the period" in lowered:
        return "Service period mentioned (see source text)."
    if "effective" in lowered or "took effect" in lowered:
        return "Effective date mentioned (see source text)."
    if "2025-26" in text or "2026-27" in text:
        # financial year reference
        m = re.search(r"20\d{2}-\d{2}", text)
        if m:
            return f"FY {m.group(0)}"
    return "Not specified in source."


def summarize_text(text: str, max_chars: int = 220) -> str:
    if len(text) <= max_chars:
        return text
    clipped = text[:max_chars].rsplit(" ", 1)[0].strip()
    return f"{clipped}..."


def _label_from_item(item: dict[str, Any]) -> str:
    display_name = str(item.get("website_display_name") or "").strip()
    if display_name:
        return display_name
    target_url = str(item.get("target_url") or "").strip()
    return urlparse(target_url).netloc or target_url or "Unknown bank"


def build_tev_insight(item: dict[str, Any]) -> TevInsight | None:
    text = _normalize_text(item.get("item_text"))
    if not text or not is_tev_relevant(text):
        return None

    status, _ = classify_status(text)
    return TevInsight(
        bank_label=_label_from_item(item),
        target_url=str(item.get("target_url") or "").strip(),
        detected_at=item.get("detected_at"),
        status=status,
        notice_released=notice_status(text),
        timeline_hint=extract_timeline_hint(text),
        summary=summarize_text(text),
        source_url=(str(item.get("item_source_url") or "").strip() or None),
        run_id=int(item["run_id"]) if item.get("run_id") is not None else None,
    )


def build_latest_tev_summary(
    websites: list[dict[str, Any]],
    recent_items: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    latest_by_url: dict[str, TevInsight] = {}
    for item in recent_items:
        target_url = str(item.get("target_url") or "").strip()
        if not target_url or target_url in latest_by_url:
            continue
        insight = build_tev_insight(item)
        if insight is None:
            continue
        latest_by_url[target_url] = insight

    output: list[dict[str, Any]] = []
    for website in websites:
        website_url = str(website.get("url") or "").strip()
        display_name = str(website.get("display_name") or "").strip()
        bank_label = display_name or urlparse(website_url).netloc or website_url

        insight = latest_by_url.get(website_url)
        if insight is None:
            output.append(
                {
                    "bank_label": bank_label,
                    "target_url": website_url,
                    "status": "not_specified",
                    "notice_released": "not_clear",
                    "timeline_hint": "Not specified in source.",
                    "summary": "No TEV/LIE empanelment notice detected yet from this monitored website.",
                    "source_url": None,
                    "run_id": None,
                    "detected_at": None,
                }
            )
            continue

        output.append(
            {
                "bank_label": insight.bank_label,
                "target_url": insight.target_url,
                "status": insight.status,
                "notice_released": insight.notice_released,
                "timeline_hint": insight.timeline_hint,
                "summary": insight.summary,
                "source_url": insight.source_url,
                "run_id": insight.run_id,
                "detected_at": insight.detected_at,
            }
        )
    return output


def build_tev_insights_for_email(new_items: list[dict[str, str]]) -> list[dict[str, str]]:
    insights: list[dict[str, str]] = []
    for item in new_items:
        text = _normalize_text(item.get("text"))
        if not text or not is_tev_relevant(text):
            continue
        status, reason = classify_status(text)
        insights.append(
            {
                "status": status,
                "status_reason": reason,
                "notice_released": notice_status(text),
                "timeline_hint": extract_timeline_hint(text),
                "summary": summarize_text(text, max_chars=260),
                "source_url": (str(item.get("source_url") or "").strip() or ""),
            }
        )
        if len(insights) >= 30:
            break
    return insights
