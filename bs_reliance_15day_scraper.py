"""
Business Standard Reliance 15-day article scraper
-------------------------------------------------
Checks BOTH:
  1. https://www.business-standard.com/companies/news
  2. https://www.business-standard.com/markets/news

Discovers article links, downloads each article page, extracts its publication
date and article body, and keeps Reliance-related articles published within
the last 15 days.

Install dependencies:
    pip install requests beautifulsoup4 python-dateutil

Run:
    python bs_reliance_15day_scraper.py

Output:
    reliance_15day_articles.json

This is a web-page scraper (HTML + BeautifulSoup), not RSS ingestion.
"""

from __future__ import annotations

import json
import re
import time
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
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
REQUEST_DELAY_SECONDS = 0.6

IST = timezone(timedelta(hours=5, minutes=30))
NOW = datetime.now(IST)
CUTOFF = NOW - timedelta(days=LOOKBACK_DAYS)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/130.0.0.0 Safari/537.36"
    ),
    "Accept": (
        "text/html,application/xhtml+xml,application/xml;q=0.9,"
        "image/avif,image/webp,*/*;q=0.8"
    ),
    "Accept-Language": "en-IN,en;q=0.9",
    "Referer": BASE_URL,
}

# Use phrases rather than very short tokens to reduce false matches.
RELIANCE_TERMS = [
    "reliance industries",
    "reliance industries limited",
    "reliance consumer products",
    "reliance retail",
    "reliance jio",
    "ril",
]


# ---------------------------------------------------------------------
# HTTP SESSION
# ---------------------------------------------------------------------

session = requests.Session()
session.headers.update(HEADERS)


def get_soup(url: str) -> Optional[BeautifulSoup]:
    """Download a page and return BeautifulSoup, or None on failure."""
    try:
        response = session.get(url, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()

        content_type = response.headers.get("Content-Type", "").lower()
        if "html" not in content_type and response.text.lstrip().startswith("{"):
            print("Unexpected non-HTML response:", url)
            return None

        return BeautifulSoup(response.text, "html.parser")

    except requests.RequestException as exc:
        print(f"REQUEST ERROR: {url}\n  {exc}")
        return None


# ---------------------------------------------------------------------
# ARTICLE LINK DISCOVERY
# ---------------------------------------------------------------------

def is_probable_article_url(url: str) -> bool:
    """Keep Business Standard article URLs and discard navigation links."""
    parsed = urlparse(url)

    if parsed.netloc and not (
        parsed.netloc == "business-standard.com"
        or parsed.netloc.endswith(".business-standard.com")
    ):
        return False

    path = parsed.path.lower()

    if not path or path in {"/", "/companies/news", "/markets/news"}:
        return False

    excluded_parts = [
        "/tag/",
        "/topic/",
        "/author/",
        "/videos/",
        "/photos/",
        "/podcasts/",
        "/webstories/",
        "/rss/",
    ]
    if any(part in path for part in excluded_parts):
        return False

    # Article paths commonly contain a date/id suffix or an article section.
    return (
        "/news/" in path
        or "/article/" in path
        or bool(re.search(r"-\d{8,}(?:_\d+)?\.html?$", path))
    )


def discover_article_links(section_url: str) -> list[dict]:
    """Discover article links from one section page."""
    print(f"\nChecking section: {section_url}")

    soup = get_soup(section_url)
    if soup is None:
        return []

    found = {}
    for anchor in soup.find_all("a", href=True):
        href = urljoin(section_url, anchor["href"].strip())
        href = href.split("#", 1)[0]

        if not is_probable_article_url(href):
            continue

        headline = anchor.get_text(" ", strip=True)
        if not headline:
            # Some article cards place text in a child heading.
            heading = anchor.find(["h1", "h2", "h3", "h4"])
            if heading:
                headline = heading.get_text(" ", strip=True)

        if len(headline) < 12:
            continue

        found[href] = {
            "url": href,
            "discovered_headline": headline,
            "section": section_url,
        }

    articles = list(found.values())
    print("Article links found:", len(articles))
    return articles


# ---------------------------------------------------------------------
# DATE EXTRACTION
# ---------------------------------------------------------------------

def parse_date_value(value: Optional[str]) -> Optional[datetime]:
    """Parse ISO, RFC, or common visible date strings into IST-aware datetime."""
    if not value:
        return None

    value = value.strip()
    if not value:
        return None

    parsed = None

    try:
        parsed = date_parser.parse(value, fuzzy=True)
    except (ValueError, TypeError, OverflowError):
        try:
            parsed = parsedate_to_datetime(value)
        except (ValueError, TypeError, OverflowError):
            return None

    if parsed.tzinfo is None:
        # Business Standard visible dates are generally IST.
        parsed = parsed.replace(tzinfo=IST)

    return parsed.astimezone(IST)


def extract_published_date(soup: BeautifulSoup) -> tuple[Optional[datetime], str]:
    """
    Try structured publication metadata first, then time tags and visible
    publication labels. Avoid using Last-Modified as publication date unless
    no publication-specific date is present.
    """
    meta_candidates = [
        ("meta", {"property": "article:published_time"}),
        ("meta", {"name": "article:published_time"}),
        ("meta", {"itemprop": "datePublished"}),
        ("meta", {"property": "og:published_time"}),
        ("meta", {"name": "publish-date"}),
        ("meta", {"name": "pubdate"}),
    ]

    for tag_name, attrs in meta_candidates:
        tag = soup.find(tag_name, attrs=attrs)
        if tag:
            value = tag.get("content") or tag.get("datetime")
            parsed = parse_date_value(value)
            if parsed:
                return parsed, f"metadata:{attrs}"

    # JSON-LD often carries datePublished.
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
                candidates.extend(item for item in graph if isinstance(item, dict))

            for item in candidates:
                for key in ("datePublished", "dateCreated"):
                    parsed = parse_date_value(item.get(key))
                    if parsed:
                        return parsed, f"jsonld:{key}"

    # Time tags may have datetime attributes or visible date text.
    for tag in soup.find_all("time"):
        value = tag.get("datetime") or tag.get_text(" ", strip=True)
        parsed = parse_date_value(value)
        if parsed:
            return parsed, "time-tag"

    # Search visible text near explicit publication labels.
    page_text = soup.get_text(" ", strip=True)
    patterns = [
        r"(?:First\s+Published|Published|Updated)\s*:\s*"
        r"([A-Za-z]{3,9}\s+\d{1,2},?\s+\d{4}(?:\s*\|\s*\d{1,2}:\d{2}\s*[AP]M\s*IST)?)",
        r"(?:First\s+Published|Published)\s*:\s*"
        r"(\d{1,2}\s+[A-Za-z]{3,9}\s+\d{4}(?:\s*\|\s*\d{1,2}:\d{2}\s*[AP]M)?)",
    ]

    for pattern in patterns:
        match = re.search(pattern, page_text, flags=re.IGNORECASE)
        if match:
            parsed = parse_date_value(match.group(1))
            if parsed:
                return parsed, "visible-publication-label"

    return None, "date-not-found"


# ---------------------------------------------------------------------
# ARTICLE BODY EXTRACTION
# ---------------------------------------------------------------------

BODY_SELECTORS = [
    "div.story-content",
    "div.story_content",
    "div.artText",
    "div.article-content",
    "div.articleContent",
    "div.content-area",
    "div[class*='story-content']",
    "div[class*='article-content']",
    "section[class*='story-content']",
    "article",
]


def extract_article_body(soup: BeautifulSoup) -> str:
    """Extract article paragraphs while excluding navigation and related links."""
    for selector in BODY_SELECTORS:
        container = soup.select_one(selector)
        if not container:
            continue

        # Work on a copy so we don't alter the original page soup.
        container = BeautifulSoup(str(container), "html.parser")

        for unwanted in container.select(
            "script, style, nav, footer, header, "
            ".related-news, .recommended, .advertisement, "
            "[class*='social'], [class*='share'], [class*='related']"
        ):
            unwanted.decompose()

        paragraphs = []
        for paragraph in container.find_all("p"):
            text = paragraph.get_text(" ", strip=True)
            if len(text) >= 35:
                paragraphs.append(text)

        body = "\n\n".join(paragraphs).strip()
        if len(body) >= 100:
            return body

    # Fallback: collect substantial paragraphs from the article element/page.
    article = soup.find("article") or soup
    paragraphs = []
    for paragraph in article.find_all("p"):
        text = paragraph.get_text(" ", strip=True)
        if len(text) >= 50:
            paragraphs.append(text)

    # Remove exact duplicates while preserving order.
    seen = set()
    unique = []
    for paragraph in paragraphs:
        normalized = re.sub(r"\s+", " ", paragraph).strip()
        if normalized and normalized not in seen:
            seen.add(normalized)
            unique.append(normalized)

    return "\n\n".join(unique).strip()


# ---------------------------------------------------------------------
# COMPANY MATCHING
# ---------------------------------------------------------------------

def is_reliance_article(headline: str, full_text: str) -> bool:
    """Check title and body for Reliance-related company references."""
    combined = f" {headline} {full_text} ".lower()

    # Prefer full company phrases.
    strong_terms = [
        "reliance industries",
        "reliance consumer products",
        "reliance retail",
        "reliance jio",
    ]
    if any(term in combined for term in strong_terms):
        return True

    # RIL is accepted only as a standalone abbreviation.
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

    soup = get_soup(url)
    if soup is None:
        print("REJECTED: Article page could not be downloaded")
        return None

    # Headline
    heading = soup.find("h1")
    headline = (
        heading.get_text(" ", strip=True)
        if heading
        else discovered_headline
    )

    # Publication date
    published_at, date_source = extract_published_date(soup)
    print("HEADLINE:", headline)
    print("DATE SOURCE:", date_source)
    print("PUBLISHED AT:", published_at)

    # Article body
    full_text = extract_article_body(soup)
    print("BODY LENGTH:", len(full_text))
    if full_text:
        print("BODY PREVIEW:", full_text[:250])

    # Company matching
    matched = is_reliance_article(headline, full_text)
    print("RELIANCE MATCH:", matched)

    # Validate publication date.
    if published_at is None:
        print("REJECTED: Publication date not found")
        return None

    if published_at < CUTOFF or published_at > NOW:
        print("REJECTED: Publication date outside the 15-day window")
        return None

    if not full_text:
        print("REJECTED: Article body not extracted")
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

    print("SUCCESS: Matching Reliance article extracted")
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

    all_links = {}

    # Discover from both Business Standard sections.
    for section_url in SECTION_URLS:
        section_articles = discover_article_links(section_url)
        for article in section_articles:
            all_links[article["url"]] = article

    articles_to_check = list(all_links.values())

    print("\nTOTAL UNIQUE ARTICLE LINKS:", len(articles_to_check))
    print("\n========== SCRAPING ARTICLES ==========")

    results = []

    for index, article in enumerate(articles_to_check, start=1):
        print(f"\n[{index}/{len(articles_to_check)}]", end=" ")
        result = scrape_article(article)

        if result:
            results.append(result)

        time.sleep(REQUEST_DELAY_SECONDS)

    # Newest first.
    results.sort(
        key=lambda item: item["published_at"],
        reverse=True
    )

    OUTPUT_FILE.write_text(
        json.dumps(results, indent=4, ensure_ascii=False),
        encoding="utf-8"
    )

    print("\n" + "=" * 75)
    print("SCRAPING COMPLETED")
    print("=" * 75)
    print("Companies News + Markets News links:", len(articles_to_check))
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
