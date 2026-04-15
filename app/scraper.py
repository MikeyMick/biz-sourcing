#!/usr/bin/env python3

from __future__ import annotations

import csv
import logging
import os
import sys
from dataclasses import dataclass, asdict
from typing import Iterable

import requests
from bs4 import BeautifulSoup

OUTPUT_CSV = os.getenv("SCRAPER_OUTPUT_CSV", "texas_businesses_for_sale.csv")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger("tx_biz_scraper")


@dataclass
class Listing:
    title: str
    location: str
    summary: str
    listing_url: str
    source: str


def clean_text(node):
    return node.get_text(" ", strip=True) if node else ""


def scrape_businessesforsale():
    url = "https://us.businessesforsale.com/us/search/businesses-for-sale-in-texas"
    headers = {
        "User-Agent": "Mozilla/5.0"
    }

    response = requests.get(url, headers=headers)
    logger.info("status code=%s", response.status_code)

    soup = BeautifulSoup(response.text, "lxml")

    cards = soup.select("article")

    logger.info("found cards=%s", len(cards))

    results = []

    for card in cards:
        title = clean_text(card.select_one("h2, h3"))
        location = clean_text(card.select_one(".location"))
        summary = clean_text(card.select_one("p"))

        link = card.select_one("a[href]")
        listing_url = link.get("href") if link else ""

        if listing_url.startswith("/"):
            listing_url = f"https://us.businessesforsale.com{listing_url}"

        if not title:
            continue

        results.append(
            Listing(
                title=title,
                location=location,
                summary=summary,
                listing_url=listing_url,
                source="BusinessesForSale",
            )
        )

    return results


def dedupe(rows: Iterable[Listing]):
    seen = set()
    out = []

    for row in rows:
        if row.listing_url in seen:
            continue
        seen.add(row.listing_url)
        out.append(row)

    return out


def write_csv(rows, path):
    fieldnames = list(Listing.__dataclass_fields__.keys())

    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for row in rows:
            writer.writerow(asdict(row))

    logger.info("saved rows=%s", len(rows))


def main():
    rows = scrape_businessesforsale()
    rows = dedupe(rows)
    write_csv(rows, OUTPUT_CSV)


if __name__ == "__main__":
    main()
