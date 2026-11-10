"""
Three-publisher Indian financial news scraper
Sources: Moneycontrol, CNBC-TV18, Mint
- Discovers articles from RSS feeds and section pages
- Scrapes article publication date and article body
- Filters the user's 13 target companies and a rolling 15-day window
- Saves one JSON file per publisher and a combined JSON file

Install if needed:
    pip install requests beautifulsoup4 feedparser python-dateutil

Run:
    python other3_sources_15day_scraper.py
"""

import json
import re
import time
import hashlib
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from urllib.parse import urljoin, urlparse, urldefrag

import requests
import feedparser
from bs4 import BeautifulSoup

try:
    from zoneinfo import ZoneInfo
    IST = ZoneInfo("Asia/Kolkata")
except Exception:
    IST = timezone(timedelta(hours=5, minutes=30))


# ---------------- CONFIGURATION ----------------

LOOKBACK_DAYS = 15
REQUEST_TIMEOUT = 25
DELAY_BETWEEN_ARTICLES = 0.35
OUTPUT_DIR = Path("other3_source_output")

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/131.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-IN,en;q=0.9",
}

# Prefer full company names. Short aliases are intentionally avoided where
# they can create false positives (for example SBI, ITC, Airtel, RIL).
COMPANIES = {
    "RELIANCE": [
        "Reliance Industries",
        "Reliance Industries Limited",
        "Reliance Retail",
        "Reliance Jio",
        "Jio Financial Services",
        "Reliance Consumer Products",
    ],
    "TCS": [
        "Tata Consultancy Services",
        "TCS",
    ],
    "INFY": [
        "Infosys Limited",
        "Infosys",
    ],
    "HDFCBANK": [
        "HDFC Bank Limited",
        "HDFC Bank",
    ],
    "ICICIBANK": [
        "ICICI Bank Limited",
        "ICICI Bank",
    ],
    "TATASTEEL": [
        "Tata Steel Limited",
        "Tata Steel",
    ],
    "SBIN": [
        "State Bank of India",
    ],
    "ITC": [
        "ITC Limited",
        "ITC Ltd",
    ],
    "SUNPHARMA": [
        "Sun Pharmaceutical Industries",
        "Sun Pharmaceutical",
        "Sun Pharma",
    ],
    "BHARTIARTL": [
        "Bharti Airtel",
        "Bharti Airtel Limited",
    ],
    "ADANIENT": [
        "Adani Enterprises Limited",
        "Adani Enterprises",
    ],
    "COALINDIA": [
        "Coal India Limited",
        "Coal India",
    ],
    "BAJFINANCE": [
        "Bajaj Finance Limited",
        "Bajaj Finance",
    ],
}

# Feeds/section pages are discovery sources only. RSS history is finite;
# the 15-day cutoff is applied to the actual article publication timestamp.
SOURCES = {
    "Moneycontrol": {
        "feeds": [
            "https://www.moneycontrol.com/rss/latestnews.xml",
        ],
        "sections": [
            "https://www.moneycontrol.com/news/business/markets/",
            "https://www.moneycontrol.com/news/business/",
        ],
        "article_domains": ["moneycontrol.com"],
        "body_selectors": [
            "div.content_wrapper",
            "div.article_content",
            "div#article",
            "div.article_body",
            "div.article_content_wrapper",
            "section.article_content",
            "div[class*='article_content']",
            "div[class*='content_wrapper']",
            "div[class*='articleBody']",
            "article",
        ],
    },
    "CNBC-TV18": {
        "feeds": [
            "https://www.cnbctv18.com/commonfeeds/v1/cne/rss/market.xml",
            "https://www.cnbctv18.com/commonfeeds/v1/cne/rss/business.xml",
        ],
        "sections": [
            "https://www.cnbctv18.com/market/",
            "https://www.cnbctv18.com/business/",
        ],
        "article_domains": ["cnbctv18.com"],
        "body_selectors": [
            "div.article-body",
            "div.articleBody",
            "div[class*='article-body']",
            "div[class*='ArticleBody']",
            "div[class*='story__content']",
            "div[class*='story-content']",
            "article",
        ],
    },
    "Mint": {
        "feeds": [
            "https://www.livemint.com/rss/companies",
            "https://www.livemint.com/rss/markets",
        ],
        "sections": [
            "https://www.livemint.com/companies",
            "https://www.livemint.com/market",
        ],
        "article_domains": ["livemint.com"],
        "body_selectors": [
            "div[class*='paywall']",
            "div[class*='articleBody']",
            "div[class*='story__content']",
            "div[class*='mainSec']",
            "div[class*='contentSec']",
            "article",
        ],
    },
}

session = requests.Session()
session.headers.update(HEADERS)


# ---------------- COMMON HELPERS ----------------

def normalize_url(url):
    if not url:
        return ""
    url = urldefrag(url.strip())[0]
    return url.rstrip("/")


def parse_datetime(value):
    """Parse common ISO/RSS/date formats and return an aware datetime."""
    if not value:
        return None

    if isinstance(value, datetime):
        dt = value
    else:
        text = str(value).strip()
        try:
            dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError:
            try:
                dt = parsedate_to_datetime(text)
            except Exception:
                # Common visible publisher format:
                # Nov 09 2026 | 6:10 PM IST
                cleaned = re.sub(r"\s+IST$", "", text)
                dt = None
                for fmt in (
                    "%b %d %Y | %I:%M %p",
                    "%d %b %Y | %I:%M %p",
                    "%B %d, %Y %I:%M %p",
                ):
                    try:
                        dt = datetime.strptime(cleaned, fmt).replace(tzinfo=IST)
                        break
                    except ValueError:
                        pass
                if dt is None:
                    return None

    if dt.tzinfo is None:
        # Publisher timestamps without a timezone are treated as IST.
        dt = dt.replace(tzinfo=IST)
    return dt.astimezone(IST)


def company_matches(text):
    """Return all ticker codes matched in the supplied article text."""
    if not text:
        return []

    normalized = re.sub(r"\s+", " ", text).casefold()
    matches = []

    for ticker, names in COMPANIES.items():
        for name in names:
            # For TCS, enforce word boundaries. Other aliases are full names.
            pattern = r"(?<![A-Za-z0-9])" + re.escape(name.casefold()) + r"(?![A-Za-z0-9])"
            if re.search(pattern, normalized, flags=re.IGNORECASE):
                matches.append(ticker)
                break

    return matches


def get_page(url):
    response = session.get(url, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    return response


def discover_from_feed(source_name, feed_url):
    found = {}
    try:
        response = get_page(feed_url)
        parsed = feedparser.parse(response.content)

        for entry in parsed.entries:
            link = normalize_url(entry.get("link", ""))
            title = entry.get("title", "").strip()
            if not link or not title:
                continue

            published = (
                entry.get("published")
                or entry.get("updated")
                or entry.get("created")
            )
            summary = BeautifulSoup(
                entry.get("summary", "") or "",
                "html.parser"
            ).get_text(" ", strip=True)

            found[link] = {
                "url": link,
                "headline": title,
                "feed_date": published,
                "feed_summary": summary,
                "discovery_method": "RSS",
                "source": source_name,
            }

        print(f"  RSS {feed_url} -> {len(parsed.entries)} entries")
    except Exception as exc:
        print(f"  RSS ERROR {feed_url}: {exc}")

    return found


def discover_from_section(source_name, section_url, allowed_domains):
    found = {}
    try:
        response = get_page(section_url)
        soup = BeautifulSoup(response.text, "html.parser")

        for anchor in soup.find_all("a", href=True):
            href = normalize_url(urljoin(section_url, anchor["href"]))
            parsed = urlparse(href)
            domain = parsed.netloc.lower().replace("www.", "")

            if not any(domain.endswith(d) for d in allowed_domains):
                continue
            if not href.startswith("http"):
                continue

            headline = anchor.get_text(" ", strip=True)
            if len(headline) < 18:
                continue

            # Avoid section pages, tags, author pages, and non-article links.
            path = parsed.path.lower()
            if not any(token in path for token in (
                "/news/", "/article", "/market", "/companies/",
                "/business/", "/stocks/", "/stock/"
            )):
                continue
            if path.endswith((".jpg", ".jpeg", ".png", ".webp", ".pdf")):
                continue

            if href not in found:
                found[href] = {
                    "url": href,
                    "headline": headline,
                    "feed_date": None,
                    "feed_summary": "",
                    "discovery_method": "Section page",
                    "source": source_name,
                }

        print(f"  SECTION {section_url} -> {len(found)} links")
    except Exception as exc:
        print(f"  SECTION ERROR {section_url}: {exc}")

    return found


# ---------------- ARTICLE EXTRACTION ----------------

def extract_date(soup, feed_date=None):
    """Try structured metadata first, then visible date text."""
    candidates = []

    if feed_date:
        candidates.append(("RSS date", feed_date))

    # Open Graph / article metadata
    for key in (
        "article:published_time",
        "article:modified_time",
        "datePublished",
        "date",
        "pubdate",
    ):
        tag = soup.find("meta", attrs={"property": key})
        if not tag:
            tag = soup.find("meta", attrs={"name": key})
        if tag and tag.get("content"):
            candidates.append((f"meta:{key}", tag["content"]))

    # JSON-LD structured data
    for script in soup.find_all("script", type="application/ld+json"):
        raw = script.string or script.get_text()
        if not raw:
            continue
        try:
            data = json.loads(raw)
        except Exception:
            continue

        stack = data if isinstance(data, list) else [data]
        while stack:
            item = stack.pop()
            if isinstance(item, dict):
                for key in ("datePublished", "dateCreated", "dateModified"):
                    if item.get(key):
                        candidates.append((f"JSON-LD:{key}", item[key]))
                graph = item.get("@graph")
                if isinstance(graph, list):
                    stack.extend(graph)
            elif isinstance(item, list):
                stack.extend(item)

    # time elements
    for tag in soup.find_all("time"):
        value = tag.get("datetime") or tag.get_text(" ", strip=True)
        if value:
            candidates.append(("time element", value))

    # Use first parseable candidate, preferring published metadata.
    for source, value in candidates:
        dt = parse_datetime(value)
        if dt:
            return dt, source

    # Last resort: visible date snippets.
    visible = soup.get_text(" ", strip=True)
    patterns = [
        r"(?:First Published|Published|Updated)\s*[:|]?\s*"
        r"([A-Za-z]{3,9}\s+\d{1,2},?\s+\d{4}(?:\s*[|,]?\s*\d{1,2}:\d{2}\s*(?:AM|PM))?)",
        r"(\d{1,2}\s+[A-Za-z]{3}\s+\d{4}\s*[|,]?\s*\d{1,2}:\d{2}\s*(?:AM|PM))",
    ]
    for pattern in patterns:
        match = re.search(pattern, visible, flags=re.IGNORECASE)
        if match:
            dt = parse_datetime(match.group(1))
            if dt:
                return dt, "visible date text"

    return None, "date not found"


def extract_article_body(soup, selectors):
    # Try publisher-specific selectors in order.
    for selector in selectors:
        node = soup.select_one(selector)
        if not node:
            continue

        # Work on a copy so the original parsed document is untouched.
        from copy import copy
        node = copy(node)

        for unwanted in node.select(
            "script, style, noscript, iframe, "
            "[class*='related'], [class*='recommend'], "
            "[class*='latest'], [class*='also-read'], "
            "[class*='social'], [class*='newsletter'], "
            "[class*='advert'], [class*='comment']"
        ):
            unwanted.decompose()

        paragraphs = [
            p.get_text(" ", strip=True)
            for p in node.find_all("p")
            if len(p.get_text(" ", strip=True)) > 35
        ]

        if paragraphs:
            text = "\n\n".join(paragraphs)
        else:
            text = node.get_text(" ", strip=True)

        text = re.sub(r"\s+", " ", text).strip()
        if len(text) >= 250:
            return text, selector

    # Generic fallback: choose the largest paragraph-rich article/main block.
    candidates = soup.select("article, main, div")
    best_text = ""
    best_selector = "generic fallback"

    for node in candidates:
        paragraphs = [
            p.get_text(" ", strip=True)
            for p in node.find_all("p")
            if len(p.get_text(" ", strip=True)) > 35
        ]
        text = re.sub(r"\s+", " ", " ".join(paragraphs)).strip()
        if len(text) > len(best_text):
            best_text = text
            best_selector = f"{node.name}.{'.'.join(node.get('class', []))}"

    if len(best_text) >= 250:
        return best_text, best_selector

    return "", "article body selector not found"


def extract_article(source_name, config, discovered):
    url = discovered["url"]

    try:
        response = get_page(url)
        soup = BeautifulSoup(response.text, "html.parser")

        # Use page title when the discovery headline is missing/weak.
        headline = discovered.get("headline") or ""
        if len(headline) < 18:
            og = soup.find("meta", property="og:title")
            headline = og.get("content", "") if og else soup.title.get_text(" ", strip=True)

        published_at, date_source = extract_date(
            soup,
            discovered.get("feed_date")
        )

        body, body_selector = extract_article_body(
            soup,
            config["body_selectors"]
        )

        searchable_text = " ".join([
            headline or "",
            discovered.get("feed_summary", ""),
            body,
        ])
        tickers = company_matches(searchable_text)

        if not published_at:
            return None, "publication date not found"

        cutoff = datetime.now(IST) - timedelta(days=LOOKBACK_DAYS)
        if published_at < cutoff:
            return None, "older than 15-day cutoff"

        if not tickers:
            return None, "not related to target companies"

        if len(body) < 250:
            return None, "article body missing/too short"

        article_id = hashlib.sha256(url.encode("utf-8")).hexdigest()[:20]

        article = {
            "article_id": article_id,
            "company_tickers": tickers,
            "source": source_name,
            "headline": headline,
            "published_at": published_at.isoformat(),
            "date_source": date_source,
            "url": url,
            "full_text": body,
            "body_selector_used": body_selector,
            "ingested_at": datetime.now(IST).isoformat(),
        }
        return article, "success"

    except requests.RequestException as exc:
        return None, f"request error: {exc}"
    except Exception as exc:
        return None, f"extraction error: {exc}"


# ---------------- RUNNER ----------------

def run_source(source_name, config):
    print("\n" + "=" * 76)
    print(f"{source_name.upper()} - 15-DAY COMPANY NEWS SCRAPER")
    print("=" * 76)

    discovered = {}

    for feed_url in config["feeds"]:
        discovered.update(discover_from_feed(source_name, feed_url))

    for section_url in config["sections"]:
        section_items = discover_from_section(
            source_name,
            section_url,
            config["article_domains"]
        )
        # RSS discovery has richer date/summary metadata; don't overwrite it.
        for url, item in section_items.items():
            discovered.setdefault(url, item)

    print(f"\nUnique discovered links: {len(discovered)}")

    cutoff = datetime.now(IST) - timedelta(days=LOOKBACK_DAYS)
    print("Current IST:", datetime.now(IST).isoformat())
    print("15-day cutoff:", cutoff.isoformat())

    articles = []
    failures = {}

    print("\n========== SCRAPING ARTICLES ==========")

    for index, item in enumerate(discovered.values(), 1):
        print(f"\n[{index}/{len(discovered)}] {item['headline'][:110]}")
        article, status = extract_article(source_name, config, item)

        if article:
            articles.append(article)
            print("SUCCESS:", ", ".join(article["company_tickers"]))
            print("PUBLISHED:", article["published_at"])
            print("BODY CHARACTERS:", len(article["full_text"]))
        else:
            failures[status] = failures.get(status, 0) + 1
            print("SKIPPED:", status)

        time.sleep(DELAY_BETWEEN_ARTICLES)

    articles.sort(key=lambda x: x["published_at"], reverse=True)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    safe_name = source_name.lower().replace("-", "_").replace(" ", "_")
    output_file = OUTPUT_DIR / f"{safe_name}_15day_articles.json"

    payload = {
        "source": source_name,
        "lookback_days": LOOKBACK_DAYS,
        "generated_at": datetime.now(IST).isoformat(),
        "cutoff": cutoff.isoformat(),
        "discovered_links": len(discovered),
        "matched_articles": len(articles),
        "articles": articles,
    }

    output_file.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8"
    )

    print("\n" + "=" * 76)
    print("SCRAPING COMPLETED:", source_name)
    print("=" * 76)
    print("Discovered links:", len(discovered))
    print("Matched company articles:", len(articles))
    print("Saved:", output_file.resolve())
    print("Skip summary:", failures or "None")

    return articles


def main():
    all_articles = []

    for source_name, config in SOURCES.items():
        all_articles.extend(run_source(source_name, config))

    all_articles.sort(key=lambda x: x["published_at"], reverse=True)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    combined_file = OUTPUT_DIR / "all_three_sources_15day_articles.json"

    combined_payload = {
        "generated_at": datetime.now(IST).isoformat(),
        "lookback_days": LOOKBACK_DAYS,
        "sources": list(SOURCES.keys()),
        "total_matched_articles": len(all_articles),
        "articles": all_articles,
    }

    combined_file.write_text(
        json.dumps(combined_payload, ensure_ascii=False, indent=2),
        encoding="utf-8"
    )

    print("\n" + "#" * 76)
    print("ALL THREE PUBLISHERS COMPLETED")
    print("#" * 76)
    print("Total matched articles:", len(all_articles))
    print("Combined JSON:", combined_file.resolve())

    print("\nCompany-wise counts:")
    for ticker in COMPANIES:
        count = sum(ticker in a["company_tickers"] for a in all_articles)
        print(f"{ticker:12s}: {count}")


if __name__ == "__main__":
    main()
