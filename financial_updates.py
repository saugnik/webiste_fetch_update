from __future__ import annotations

# financial_updates.py
#
# STRICTLY scoped: only catches content directly about TEV/LIE/project
# finance empanelment. Retail banking, financial results, agri loans,
# personal loans, navigation menus — all rejected.

import re
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlparse


# ---------------------------------------------------------------------------
# POSITIVE keywords — text must contain at least one of these to pass
# ---------------------------------------------------------------------------
FINANCIAL_KEYWORDS = (
    "techno economic viability",
    "techno-economic viability",
    "tev study",
    "tev/lie",
    "lie/tev",
    "tev & lie",
    "tev and lie",
    "tev consultant",
    "lie consultant",
    "tev empanelment",
    "lie empanelment",
    "empanelment of tev",
    "empanelment for tev",
    "lender's independent engineer",
    "lenders independent engineer",
    "project finance",
    "infrastructure finance",
    "financial closure",
    "financial appraisal",
    "financial viability",
    "project cost",
    "total project cost",
    "cost of project",
    "capital cost of project",
    "debt equity ratio",
    "debt-equity ratio",
    "consultancy fee",
    "professional fee for tev",
    "scope of work for tev",
    "request for proposal for tev",
    "expression of interest for tev",
)

# ---------------------------------------------------------------------------
# NOISE keywords — reject outright if ANY of these appear, even if a
# positive keyword also matched.
# ---------------------------------------------------------------------------
NOISE_KEYWORDS = (
    "personal loan",
    "home loan",
    "education loan",
    "car loan",
    "auto loan",
    "gold loan",
    "mudra loan",
    "agri loan",
    "agricultural loan",
    "kisan credit",
    "crop loan",
    "vehicle loan",
    "two wheeler loan",
    "consumer loan",
    "retail loan",
    "savings account",
    "current account",
    "fixed deposit",
    "recurring deposit",
    "credit card",
    "debit card",
    "yono",
    "mobile banking",
    "internet banking",
    "net banking",
    "atm",
    "neft",
    "rtgs",
    "imps",
    "upi",
    "kyc",
    "pm vidyalaxmi",
    "pradhan mantri",
    "pmay",
    "jan dhan",
    "aadhaar",
    "pm kisan",
    "pm mudra",
    "life insurance",
    "health insurance",
    "motor insurance",
    "mutual fund",
    "insurance premium",
    "chartered accountant",
    "ca empanelment",
    "stock broker",
    "valuer empanelment",
    "empanelment of valuer",
    "electrical safety auditor",
    "empanelment of electrical",
    "security agency",
    "housekeeping",
    "catering",
    "financial results",
    "quarterly results",
    "annual report",
    "quarter ended",
    "half year",
    "processing charges",
    "service charges",
    "reserve bank of india",
    "nabard",
    "deposit insurance",
    "bank notes and security",
    "aviator",
    "gambling",
    "betting",
    "know more",
)

MONEY_PATTERNS = [
    re.compile(
        r"\b(?:inr|rs\.?|usd|eur|gbp)\s?\d[\d,]*(?:\.\d+)?(?:\s?(?:crore|lakh|million|billion|cr|mn|bn))?\b",
        re.IGNORECASE,
    ),
    re.compile(r"\b\d[\d,]*(?:\.\d+)?\s?(?:crore|lakh|million|billion|cr|mn|bn)\b", re.IGNORECASE),
]

MONTHS_PATTERN = (
    "jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|"
    "jul(?:y)?|aug(?:ust)?|sep(?:tember)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?"
)

DATE_PATTERNS = [
    re.compile(rf"\b(\d{{1,2}})\s+({MONTHS_PATTERN})[,]?\s+(20\d{{2}})\b", re.IGNORECASE),
    re.compile(rf"\b({MONTHS_PATTERN})\s+(\d{{1,2}})[,]?\s+(20\d{{2}})\b", re.IGNORECASE),
    re.compile(r"\b(\d{1,2})[/-](\d{1,2})[/-](20\d{2})\b"),
    re.compile(r"\b(\d{1,2})[.](\d{1,2})[.](20\d{2})\b"),
]

TENDER_KEYWORDS = (
    "tev empanelment", "lie empanelment", "tev/lie",
    "request for proposal for tev", "expression of interest for tev",
)
PROJECT_FINANCE_KEYWORDS = (
    "project finance", "infrastructure finance", "financial closure",
    "project cost", "total project cost", "debt equity",
)
FEE_KEYWORDS = ("consultancy fee", "professional fee for tev",)


@dataclass
class FinancialInsight:
    source_label: str
    target_url: str
    detected_at: Any
    has_financial_update: str
    category: str
    key_numbers: list[str]
    timeline_hint: str
    summary: str
    source_url: str | None
    run_id: int | None


def _normalize_text(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def _contains_any(text: str, keywords: tuple[str, ...]) -> bool:
    lowered = text.lower()
    return any(keyword in lowered for keyword in keywords)


def _is_noise(text: str) -> bool:
    lowered = text.lower()
    return any(noise in lowered for noise in NOISE_KEYWORDS)


def is_financial_relevant(text: str) -> bool:
    """
    Returns True ONLY if text contains a positive TEV/project-finance keyword
    AND does NOT contain any noise keyword.
    """
    cleaned = _normalize_text(text)
    if not cleaned:
        return False
    if _is_noise(cleaned):
        return False
    lowered = cleaned.lower()
    return any(keyword in lowered for keyword in FINANCIAL_KEYWORDS)


def _classify_category(text: str) -> str:
    lowered = text.lower()
    if _contains_any(lowered, TENDER_KEYWORDS):
        return "tev_empanelment"
    if _contains_any(lowered, PROJECT_FINANCE_KEYWORDS):
        return "project_finance"
    if _contains_any(lowered, FEE_KEYWORDS):
        return "consultancy"
    return "tev_related"


def _extract_timeline_hint(text: str) -> str:
    for pattern in DATE_PATTERNS:
        match = pattern.search(text)
        if match:
            return match.group(0).strip()
    lowered = text.lower()
    if "last date" in lowered:
        m = re.search(r"last date[^:]*[:\-]?\s*([^\n.]{5,40})", lowered)
        if m:
            return f"Last date: {m.group(1).strip()}"
        return "Last date mentioned (see source text)."
    if "due date" in lowered:
        return "Due date mentioned (see source text)."
    m = re.search(r"20\d{2}-\d{2}", text)
    if m:
        return f"FY {m.group(0)}"
    return "Not specified in source."


def _extract_number_matches(text: str, max_items: int = 5) -> list[str]:
    found: list[str] = []
    seen: set[str] = set()
    for pattern in MONEY_PATTERNS:
        for match in pattern.findall(text):
            cleaned = str(match).strip()
            normalized = cleaned.lower()
            if not cleaned or normalized in seen:
                continue
            seen.add(normalized)
            found.append(cleaned)
            if len(found) >= max_items:
                return found
    return found


def _summarize_text(text: str, max_chars: int = 220) -> str:
    if len(text) <= max_chars:
        return text
    clipped = text[:max_chars].rsplit(" ", 1)[0].strip()
    return f"{clipped}..."


def _label_from_item(item: dict[str, Any]) -> str:
    display_name = str(item.get("website_display_name") or "").strip()
    if display_name:
        return display_name
    target_url = str(item.get("target_url") or "").strip()
    return urlparse(target_url).netloc or target_url or "Unknown source"


def build_financial_insight(item: dict[str, Any]) -> FinancialInsight | None:
    text = _normalize_text(item.get("item_text"))
    if not text or not is_financial_relevant(text):
        return None
    return FinancialInsight(
        source_label=_label_from_item(item),
        target_url=str(item.get("target_url") or "").strip(),
        detected_at=item.get("detected_at"),
        has_financial_update="yes",
        category=_classify_category(text),
        key_numbers=_extract_number_matches(text),
        timeline_hint=_extract_timeline_hint(text),
        summary=_summarize_text(text),
        source_url=(str(item.get("item_source_url") or "").strip() or None),
        run_id=int(item["run_id"]) if item.get("run_id") is not None else None,
    )


def build_latest_financial_summary(
    websites: list[dict[str, Any]],
    recent_items: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    latest_by_url: dict[str, FinancialInsight] = {}
    for item in recent_items:
        target_url = str(item.get("target_url") or "").strip()
        if not target_url or target_url in latest_by_url:
            continue
        insight = build_financial_insight(item)
        if insight is None:
            continue
        latest_by_url[target_url] = insight

    output: list[dict[str, Any]] = []
    for website in websites:
        website_url = str(website.get("url") or "").strip()
        display_name = str(website.get("display_name") or "").strip()
        source_label = display_name or urlparse(website_url).netloc or website_url
        insight = latest_by_url.get(website_url)
        if insight is None:
            output.append({
                "source_label": source_label,
                "target_url": website_url,
                "has_financial_update": "no",
                "category": "not_available",
                "key_numbers": [],
                "timeline_hint": "Not specified in source.",
                "summary": "No TEV-related financial update detected yet.",
                "source_url": None,
                "run_id": None,
                "detected_at": None,
            })
            continue
        output.append({
            "source_label": insight.source_label,
            "target_url": insight.target_url,
            "has_financial_update": insight.has_financial_update,
            "category": insight.category,
            "key_numbers": insight.key_numbers,
            "timeline_hint": insight.timeline_hint,
            "summary": insight.summary,
            "source_url": insight.source_url,
            "run_id": insight.run_id,
            "detected_at": insight.detected_at,
        })
    return output


def build_financial_insights_for_email(new_items: list[dict[str, str]]) -> list[dict[str, str]]:
    insights: list[dict[str, str]] = []
    for item in new_items:
        text = _normalize_text(item.get("text"))
        if not text or not is_financial_relevant(text):
            continue
        insights.append({
            "category": _classify_category(text),
            "key_numbers": ", ".join(_extract_number_matches(text)) or "Not clearly mentioned",
            "timeline_hint": _extract_timeline_hint(text),
            "summary": _summarize_text(text, max_chars=260),
            "source_url": (str(item.get("source_url") or "").strip() or ""),
        })
        if len(insights) >= 30:
            break
    return insights
