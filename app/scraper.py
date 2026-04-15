#!/usr/bin/env python3

from __future__ import annotations

import csv
import logging
import os
import re
import sys
import time
from dataclasses import dataclass, asdict
from decimal import Decimal, InvalidOperation
from typing import Iterable, Iterator

import requests
from bs4 import BeautifulSoup
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential


USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

OUTPUT_CSV = os.getenv("SCRAPER_OUTPUT_CSV", "texas_businesses_for_sale.csv")
MAX_PAGES_PER_SITE = int(os.getenv("SCRAPER_MAX_PAGES", "5"))
REQUEST_DELAY_SECONDS = float(os.getenv("SCRAPER_DELAY_SECONDS", "2.0"))
REQUEST_TIMEOUT_SECONDS = int(os.getenv("SCRAPER_TIMEOUT_SECONDS", "25"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger("tx_biz_scraper")


@dataclass
class Listing:
    title: str
    price_raw: str
    price_amount_usd: str
    location: str
    city: str
    state: str
    category: str
    summary: str
    listing_url: str
    source: str


TEXAS_PATTERNS = [
    r"\btexas\b",
    r"\btx\b",
    r"\bdallas\b",
    r"\bhouston\b",
    r"\baustin\b",
    r"\bsan antonio\b",
    r"\bfort worth\b",
    r"\bel paso\b",
]


def looks_like_texas(*fields: str) -> bool:
    haystack = " ".join((field or "") for field in fields).lower()
    return any(re.search(pattern, haystack) for pattern in TEXAS_PATTERNS)


def extract_city_state(location: str) -> tuple[str, str]:
    if not location:
        return "", ""
    match = re.search(r"([^,]+),\s*(TX|Texas)\b", location, re.IGNORECASE)
    if match:
        return match.group(1).strip(), "TX"
    if re.search(r"\b(TX|Texas)\b", location, re.IGNORECASE):
        return location.strip(), "TX"
    return location.strip(), ""


def normalize_money(value: str) -> str:
    if not value:
        return ""
    cleaned = value.replace(",", "")
    match = re.search(r"\$?([0-9]+(?:\.[0-9]+)?)", cleaned)
    if not match:
        return ""
    try:
        amount = Decimal(match.group(1))
        return str(amount.quantize(Decimal("1.00")))
    except InvalidOperation:
        return ""


def clean_text(node) -> str:
    return node.get_text(" ", strip=True) if node else ""


class HttpClient:
    def __init__(self) -> None:
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": USER_AGENT})

    @retry(
        retry=retry_if_exception_type((requests.RequestException,)),
        wait=wait_exponential(multiplier=1, min=2, max=20),
        stop=stop_after_attempt(3),
        reraise=True,
    )
    def get_html(self, url: str) -> str:
        logger.info("fetch url=%s", url)
        response = self.session.get(url, timeout=REQUEST_TIMEOUT_SECONDS)
        response.raise_for_status()
        return response.text


def parse_bizquest(html: str, page_url: str) -> list[Listing]:
    soup = BeautifulSoup(html, "lxml")
    cards = soup.select("article, .listing-card, .result, .search-result")
    rows: list[Listing] = []

    for card in cards:
        link = card.select_one("a[href]")
        title_node = card.select_one("h2, h3, .listing-title")
        price_node = card.select_one(".price, [class*='price']")
        location_node = card.select_one(".location, [class*='location']")
        category_node = card.select_one(".category, [class*='category']")
        summary_node = card.select_one(".description, [class*='description'], p")

        title = clean_text(title_node)
        price_raw = clean_text(price_node)
        location = clean_text(location_node)
        category = clean_text(category_node)
        summary = clean_text(summary_node)
        listing_url = link.get("href", "").strip() if link else ""
        if listing_url.startswith("/"):
            listing_url = f"https://www.bizquest.com{listing_url}"

        if not title:
            continue
        if not looks_like_texas(title, location, summary, listing_url):
            continue

        city, state = extract_city_state(location)

        rows.append(
            Listing(
                title=title,
                price_raw=price_raw,
                price_amount_usd=normalize_money(price_raw),
                location=location,
                city=city,
                state=state,
                category=category,
                summary=summary,
                listing_url=listing_url,
                source="BizQuest",
            )
        )

    return rows


def scrape_bizquest(client: HttpClient) -> Iterator[Listing]:
    for page in range(1, MAX_PAGES_PER_SITE + 1):
        url = f"https://www.bizquest.com/businesses-for-sale-in-texas-tx/?page={page}"
        try:
            html = client.get_html(url)
            rows = parse_bizquest(html, url)
            logger.info("parsed source=BizQuest url=%s rows=%s", url, len(rows))
            for row in rows:
                yield row
        except Exception as exc:
            logger.exception("failed source=BizQuest url=%s error=%s", url, exc)
        time.sleep(REQUEST_DELAY_SECONDS)


def dedupe(rows: Iterable[Listing]) -> list[Listing]:
    seen: set[str] = set()
    out: list[Listing] = []
    for row in rows:
        key = row.listing_url or f"{row.source}|{row.title}|{row.location}"
        if key in seen:
            continue
        seen.add(key)
        out.append(row)
    return out


def write_csv(rows: list[Listing], path: str) -> None:
    fieldnames = list(Listing.__dataclass_fields__.keys())
    with open(path, "w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(asdict(row))
    logger.info("saved file=%s rows=%s", path, len(rows))


def main() -> int:
    client = HttpClient()
    rows = list(scrape_bizquest(client))
    final_rows = dedupe(rows)
    write_csv(final_rows, OUTPUT_CSV)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
