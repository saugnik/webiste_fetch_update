from __future__ import annotations

import hashlib
import re
import time
from dataclasses import dataclass

import requests
from bs4 import BeautifulSoup


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


def fetch_page(url: str, timeout_seconds: int, user_agent: str) -> ScrapeResult:
    headers = {
        "User-Agent": user_agent,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
    }
    start = time.perf_counter()
    response = requests.get(url, headers=headers, timeout=timeout_seconds)
    elapsed_ms = int((time.perf_counter() - start) * 1000)
    return ScrapeResult(
        html=response.text,
        status_code=response.status_code,
        response_time_ms=elapsed_ms,
        content_length=len(response.content),
    )


def normalize_text(value: str) -> str:
    compact = re.sub(r"\s+", " ", value).strip()
    return compact


def extract_text_items(html: str) -> list[str]:
    soup = BeautifulSoup(html, "html.parser")

    for element in soup(["script", "style", "noscript", "svg"]):
        element.extract()

    unique_seen: set[str] = set()
    extracted: list[str] = []

    for tag_name in TEXT_TAGS:
        for element in soup.find_all(tag_name):
            text = normalize_text(element.get_text(" ", strip=True))
            if not text:
                continue
            if len(text) < 20:
                continue
            if text in unique_seen:
                continue
            unique_seen.add(text)
            extracted.append(text)
            if len(extracted) >= 2000:
                return extracted

    if extracted:
        return extracted

    fallback_text = normalize_text(soup.get_text(" ", strip=True))
    if fallback_text:
        return [fallback_text[:2000]]
    return []


def fingerprint_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def compute_snapshot_hash(items: list[str]) -> str:
    if not items:
        return hashlib.sha256(b"").hexdigest()
    joined = "\n".join(fingerprint_text(item) for item in items)
    return hashlib.sha256(joined.encode("utf-8")).hexdigest()
