"""
Business Standard 15-day Reliance news scraper
================================================
Uses BOTH Business Standard sections:
  - https://www.business-standard.com/companies/news
  - https://www.business-standard.com/markets/news

Important fix:
  Business Standard article text is extracted from:
      <div class="storycontent">
  This is the selector confirmed by the user's successful inspection script.

The script discovers article links, extracts article publication dates and
body text, filters Reliance-related articles within the last 15 days, and
writes them to reliance_15day_articles.json.

Install:
    pip install requests beautifulsoup4 python-dateutil

Run:
    python bs_reliance_15day_scraper.py
"""

from __future__ import annotations

import json
import re
import time
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup
from dateutil import parser as date_parser


# ---------------------------------------------------------------------
# CONFIGURATION
# ---------------------------------------------------------------------

BASE_URL = "https://www.business-standard.com"

SECTION_URLS = [
    f"{BASE_URL}/companies/news",
    f"{BASE_URL}/markets/news",
]

OUTPUT_FILE = Path("reliance_15day_articles.json")
LOOKBACK_DAYS = 15
REQUEST_TIMEOUT = 25
REQUEST_DELAY_SECONDS = 0.5

IST = timezone(timedelta(hours=5, minutes=30))
NOW = datetime.now(IST)
CUTOFF = NOW - timedelta(days=LOOKBACK_DAYS)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/130.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-IN,en;q=0.9",
    "Referer": BASE_URL,
}

# Company-name phrases reduce accidental matches.
RELIANCE_TERMS = (
    "reliance industries",
    "reliance industries limited",
    "reliance consumer products",
    "reliance retail",
    "reliance jio",
)

session = requests.Session()
session.headers.update(HEADERS)


# ---------------------------------------------------------------------
# HTTP
# ---------------------------------------------------------------------

def download_soup(url: str) -> Optional[BeautifulSoup]:
    try:
        response = session.get(url, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        return BeautifulSoup(response.text, "html.parser")
    except requests.RequestException as exc:
        print(f"REQUEST ERROR: {url}\n  {exc}")
        return None


# ---------------------------------------------------------------------
# LINK DISCOVERY
# ---------------------------------------------------------------------

def is_article_url(url: str) -> bool:
    parsed = urlparse(url)

    if parsed.netloc and not (
        parsed.netloc == "business-standard.com"
        or parsed.netloc.endswith(".business-standard.com")
    ):
        return False

    path = parsed.path.lower()

    if not path or path in {
        "/",
        "/companies/news",
        "/markets/news",
    }:
        return False

    excluded = (
        "/tag/",
        "/topic/",
        "/author/",
        "/videos/",
        "/photos/",
        "/podcasts/",
        "/webstories/",
        "/rss/",
    )
    if any(part in path for part in excluded):
        return False

    return (
        "/news/" in path
        or "/article/" in path
        or bool(re.search(r"-\d{8,}(?:_\d+)?\.html?$", path))
    )


def discover_article_links(section_url: str) -> list[dict]:
    print(f"\nChecking section: {section_url}")

    soup = download_soup(section_url)
    if soup is None:
        return []

    discovered = {}

    for anchor in soup.find_all("a", href=True):
        url = urljoin(section_url, anchor["href"].strip()).split("#", 1)[0]

        if not is_article_url(url):
            continue

        headline = anchor.get_text(" ", strip=True)

        if not headline:
            heading = anchor.find(["h1", "h2", "h3", "h4"])
            if heading:
                headline = heading.get_text(" ", strip=True)

        if len(headline) < 12:
            continue

        discovered[url] = {
            "url": url,
            "discovered_headline": headline,
            "section": section_url,
        }

    results = list(discovered.values())
    print("Article links found:", len(results))
    return results


# ---------------------------------------------------------------------
# PUBLICATION DATE
# ---------------------------------------------------------------------

def parse_date(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None

    try:
        parsed = date_parser.parse(value, fuzzy=True)
    except (ValueError, TypeError, OverflowError):
        return None

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=IST)

    return parsed.astimezone(IST)


def extract_published_date(
    soup: BeautifulSoup,
) -> tuple[Optional[datetime], str]:

    # Business Standard's article:published_time metadata.
    meta = soup.find(
        "meta",
        attrs={"property": "article:published_time"},
    )
    if meta and meta.get("content"):
        parsed = parse_date(meta["content"])
        if parsed:
            return parsed, "article:published_time"

    # Other common publication metadata.
    meta_candidates = [
        {"name": "article:published_time"},
        {"itemprop": "datePublished"},
        {"property": "og:published_time"},
        {"name": "publish-date"},
        {"name": "pubdate"},
    ]

    for attrs in meta_candidates:
        tag = soup.find("meta", attrs=attrs)
        if tag:
            parsed = parse_date(tag.get("content"))
            if parsed:
                return parsed, f"metadata:{attrs}"

    # JSON-LD publication date.
    for script in soup.find_all("script", type="application/ld+json"):
        raw = script.string or script.get_text(" ", strip=True)
        if not raw:
            continue

        try:
            payload = json.loads(raw)
        except (json.JSONDecodeError, TypeError):
            continue

        objects = payload if isinstance(payload, list) else [payload]

        for obj in objects:
            if not isinstance(obj, dict):
                continue

            candidates = [obj]
            graph = obj.get("@graph")
            if isinstance(graph, list):
                candidates.extend(
                    item for item in graph if isinstance(item, dict)
                )

            for item in candidates:
                parsed = parse_date(item.get("datePublished"))
                if parsed:
                    return parsed, "jsonld:datePublished"

    # <time> elements.
    for tag in soup.find_all("time"):
        value = tag.get("datetime") or tag.get_text(" ", strip=True)
        parsed = parse_date(value)
        if parsed:
            return parsed, "time-tag"

    # Visible "First Published" / "Published" date.
    page_text = soup.get_text(" ", strip=True)
    patterns = [
        r"(?:First\s+Published|Published)\s*:\s*"
        r"([A-Za-z]{3,9}\s+\d{1,2},?\s+\d{4}"
        r"(?:\s*\|\s*\d{1,2}:\d{2}\s*[AP]M\s*IST)?)",
        r"(?:First\s+Published|Published)\s*:\s*"
        r"(\d{1,2}\s+[A-Za-z]{3,9}\s+\d{4}"
        r"(?:\s*\|\s*\d{1,2}:\d{2}\s*[AP]M)?)",
    ]

    for pattern in patterns:
        match = re.search(pattern, page_text, flags=re.IGNORECASE)
        if match:
            parsed = parse_date(match.group(1))
            if parsed:
                return parsed, "visible-publication-label"

    return None, "date-not-found"


# ---------------------------------------------------------------------
# ARTICLE BODY - CONFIRMED BUSINESS STANDARD SELECTOR
# ---------------------------------------------------------------------

def extract_article_body(soup: BeautifulSoup) -> str:
    """
    Extract text from the confirmed Business Standard storycontent div.

    The user's test confirmed:
        soup.find("div", class_="storycontent")

    Remove recommendation/latest-news widgets before extracting text.
    """
    body = soup.find("div", class_="storycontent")

    if body is None:
        print("ARTICLE BODY: div.storycontent not found")
        return ""

    # Deep-copy the container before removing unwanted child elements.
    clean_body = deepcopy(body)

    unwanted_selectors = [
        '[class*="latestnews"]',
        '[class*="recommendation"]',
        '[class*="alsoRead"]',
        '[class*="also-read"]',
        '[class*="related"]',
        "style",
        "script",
    ]

    for selector in unwanted_selectors:
        for element in clean_body.select(selector):
            element.decompose()

    # Use the complete cleaned storycontent text. This matches the
    # extraction approach that previously returned about 2,450 characters.
    full_text = clean_body.get_text(" ", strip=True)

    # Normalize whitespace.
    full_text = re.sub(r"\s+", " ", full_text).strip()

    # Reject short promotional snippets, not genuine article bodies.
    if len(full_text) < 300:
        print(
            "ARTICLE BODY WARNING: storycontent text is too short "
            f"({len(full_text)} characters)"
        )

    return full_text


# ---------------------------------------------------------------------
# COMPANY MATCHING
# ---------------------------------------------------------------------

def is_reliance_article(headline: str, full_text: str) -> bool:
    combined = f"{headline} {full_text}".lower()

    if any(term in combined for term in RELIANCE_TERMS):
        return True

    # Accept RIL only as a separate abbreviation.
    return bool(re.search(r"\bril\b", combined))


# ---------------------------------------------------------------------
# SCRAPE ONE ARTICLE
# ---------------------------------------------------------------------

def scrape_article(article: dict) -> Optional[dict]:
    url = article["url"]
    discovered_headline = article["discovered_headline"]

    print("\n" + "-" * 75)
    print("CHECKING:", discovered_headline)
    print("URL:", url)

    soup = download_soup(url)
    if soup is None:
        print("REJECTED: Could not download article page")
        return None

    heading = soup.find("h1")
    headline = (
        heading.get_text(" ", strip=True)
        if heading
        else discovered_headline
    )

    published_at, date_source = extract_published_date(soup)
    full_text = extract_article_body(soup)

    print("HEADLINE:", headline)
    print("DATE SOURCE:", date_source)
    print("PUBLISHED AT:", published_at)
    print("BODY LENGTH:", len(full_text))
    print("BODY PREVIEW:", full_text[:300] if full_text else "[EMPTY]")

    matched = is_reliance_article(headline, full_text)
    print("RELIANCE MATCH:", matched)

    if published_at is None:
        print("REJECTED: Publication date not found")
        return None

    if published_at < CUTOFF or published_at > NOW:
        print("REJECTED: Publication date outside 15-day range")
        return None

    if len(full_text) < 300:
        print("REJECTED: Article body is missing or too short")
        return None

    if not matched:
        print("REJECTED: Not a Reliance-related article")
        return None

    result = {
        "company_ticker": "RELIANCE",
        "company_name": "Reliance Industries",
        "source": "Business Standard",
        "section": article.get("section"),
        "headline": headline,
        "published_at": published_at.isoformat(),
        "url": url,
        "full_text": full_text,
        "ingested_at": datetime.now(IST).isoformat(),
    }

    print("SUCCESS: Reliance article extracted")
    return result


# ---------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------

def main():
    print("=" * 75)
    print("BUSINESS STANDARD - RELIANCE 15-DAY SCRAPER")
    print("=" * 75)
    print("Current IST:", NOW.isoformat())
    print("15-day cutoff:", CUTOFF.isoformat())
    print("Sections: Companies News + Markets News")

    all_articles = {}

    for section_url in SECTION_URLS:
        for article in discover_article_links(section_url):
            all_articles[article["url"]] = article

    articles = list(all_articles.values())

    print("\nTOTAL UNIQUE ARTICLE LINKS:", len(articles))
    print("\n========== SCRAPING ARTICLES ==========")

    results = []

    for index, article in enumerate(articles, start=1):
        print(f"\n[{index}/{len(articles)}]", end=" ")
        result = scrape_article(article)

        if result:
            results.append(result)

        time.sleep(REQUEST_DELAY_SECONDS)

    results.sort(
        key=lambda item: item["published_at"],
        reverse=True,
    )

    OUTPUT_FILE.write_text(
        json.dumps(results, indent=4, ensure_ascii=False),
        encoding="utf-8",
    )

    print("\n" + "=" * 75)
    print("SCRAPING COMPLETED")
    print("=" * 75)
    print("Companies News + Markets News links:", len(articles))
    print("Reliance articles in 15 days:", len(results))
    print("Saved file:", OUTPUT_FILE.resolve())

    for index, item in enumerate(results, start=1):
        print(f"\nARTICLE {index}")
        print("Headline:", item["headline"])
        print("Published:", item["published_at"])
        print("URL:", item["url"])
        print("Body characters:", len(item["full_text"]))


if __name__ == "__main__":
    main()
