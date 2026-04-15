#!/usr/bin/env python3

from __future__ import annotations

import csv
import logging
import os
import re
import sys
from dataclasses import dataclass, asdict
from decimal import Decimal, InvalidOperation
from typing import Iterable

import requests
from bs4 import BeautifulSoup
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential


USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

OUTPUT_CSV = os.getenv("SCRAPER_OUTPUT_CSV", "texas_businesses_for_sale.csv")
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
        logger.info("fetched url=%s status=%s bytes=%s", url, response.status_code, len(response.text))
        return response.text


def parse_bizbuysell(html: str, page_url: str) -> list[Listing]:
    soup = BeautifulSoup(html, "lxml")
    logger.info("page title=%s", soup.title.get_text(strip=True) if soup.title else "NO_TITLE")

    cards = soup.select(".listingResult")
    logger.info("listing card count=%s", len(cards))

    rows: list[Listing] = []

    for card in cards:
        title = clean_text(card.select_one(".listingResult__title"))
        price = clean_text(card.select_one(".listingResult__price"))
        location = clean_text(card.select_one(".listingResult__location"))
        summary = clean_text(card.select_one(".listingResult__description"))

        link = card.select_one("a[href]")
        url = link.get("href", "").strip() if link else ""
        if url.startswith("/"):
            url = f"https://www.bizbuysell.com{url}"

        if not title:
            continue

        city, state = extract_city_state(location)

        rows.append(
            Listing(
                title=title,
                price_raw=price,
                price_amount_usd=normalize_money(price),
                location=location,
                city=city,
                state=state,
                category="",
                summary=summary,
                listing_url=url,
                source="BizBuySell",
            )
        )

    logger.info("parsed rows=%s", len(rows))
    return rows


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
    url = "https://www.bizbuysell.com/texas-businesses-for-sale/"
    html = client.get_html(url)
    rows = parse_bizbuysell(html, url)
    final_rows = dedupe(rows)
    write_csv(final_rows, OUTPUT_CSV)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
