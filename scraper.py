from __future__ import annotations

import hashlib
import re
import time
import warnings
from dataclasses import dataclass
import io
from urllib.parse import urljoin

import requests

try:
    from pypdf import PdfReader  # type: ignore
except Exception:  # pragma: no cover - optional dependency
    PdfReader = None
from bs4 import BeautifulSoup, XMLParsedAsHTMLWarning


TEXT_TAGS = (
    "article",
    "section",
    "main",
    "h1",
    "h2",
    "h3",
    "h4",
    "p",
    "li",
    "td",
    "th",
    "a",
)


@dataclass(frozen=True)
class ScrapeResult:
    html: str
    status_code: int
    response_time_ms: int
    content_length: int
    content_type: str


def _extract_pdf_text(content: bytes, max_pages: int = 25) -> str:
    if PdfReader is None:
        return ""
    try:
        reader = PdfReader(io.BytesIO(content))
        pages = reader.pages[:max_pages]
        extracted = []
        for page in pages:
            page_text = page.extract_text() or ""
            if page_text:
                extracted.append(page_text)
        return "\n".join(extracted).strip()
    except Exception:
        return ""


def fetch_page(url: str, timeout_seconds: int, user_agent: str) -> ScrapeResult:
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/123.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
    }
    start = time.perf_counter()
    try:
        response = requests.get(url, headers=headers, timeout=timeout_seconds, verify=True)
    except requests.exceptions.SSLError:
        # Retry without SSL verification for sites with expired/invalid certificates
        warnings.warn(f"SSL verification failed for {url}, retrying without verification.", stacklevel=2)
        response = requests.get(url, headers=headers, timeout=timeout_seconds, verify=False)
    elapsed_ms = int((time.perf_counter() - start) * 1000)
    content_type = (response.headers.get("content-type") or "").lower()
    content = response.content or b""

    html_text = response.text
    if "application/pdf" in content_type or url.lower().endswith(".pdf"):
        pdf_text = _extract_pdf_text(content)
        if pdf_text:
            html_text = pdf_text
    return ScrapeResult(
        html=html_text,
        status_code=response.status_code,
        response_time_ms=elapsed_ms,
        content_length=len(content),
        content_type=content_type,
    )



def normalize_text(value: str) -> str:
    compact = re.sub(r"\s+", " ", value).strip()
    return compact


def _extract_source_url(element, base_url: str) -> str | None:
    href = None
    if element.name == "a" and element.has_attr("href"):
        href = element.get("href")
    else:
        best_link = None
        all_links = element.find_all("a", href=True)
        for link in all_links:
            link_href = (link.get("href") or "").strip()
            if not link_href:
                continue
            if link_href.startswith("http://") or link_href.startswith("https://"):
                best_link = link_href
                break
            if best_link is None:
                best_link = link_href
        href = best_link

    href = (href or "").strip()
    if not href:
        return None
    return urljoin(base_url, href)


def _is_noise_text(text: str) -> bool:
    lowered = text.lower()
    if "point by" in lowered and "| hide | past |" in lowered:
        return True
    if lowered.startswith("hide | past |"):
        return True
    return False


def extract_text_items(html: str, base_url: str) -> list[dict[str, str | None]]:
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", XMLParsedAsHTMLWarning)
        soup = BeautifulSoup(html, "html.parser")

    for element in soup(["script", "style", "noscript", "svg"]):
        element.extract()

    unique_seen: set[str] = set()
    extracted: list[dict[str, str | None]] = []

    for tag_name in TEXT_TAGS:
        for element in soup.find_all(tag_name):
            text = normalize_text(element.get_text(" ", strip=True))
            if not text:
                continue
            if len(text) < 20:
                continue
            if _is_noise_text(text):
                continue
            if text in unique_seen:
                continue
            unique_seen.add(text)
            extracted.append({"text": text, "source_url": _extract_source_url(element, base_url)})
            if len(extracted) >= 2000:
                return extracted

    if extracted:
        return extracted

    fallback_text = normalize_text(soup.get_text(" ", strip=True))
    if fallback_text:
        return [{"text": fallback_text[:2000], "source_url": base_url}]
    return []


def fingerprint_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def compute_snapshot_hash(items: list[str]) -> str:
    if not items:
        return hashlib.sha256(b"").hexdigest()
    joined = "\n".join(fingerprint_text(item) for item in items)
    return hashlib.sha256(joined.encode("utf-8")).hexdigest()
