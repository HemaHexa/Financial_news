import os
import json
import time
import hashlib
import logging
import calendar
import re
import tempfile
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
import feedparser
from kafka import KafkaProducer


# ---------------- CONFIGURATION ----------------

BASE_DIR = r"C:\FinancePipeline"
KAFKA_SERVER = "localhost:9092"
KAFKA_TOPIC = "raw-news-events"
CHECK_INTERVAL_SECONDS = 15 * 60

BASELINE_DIR = os.path.join(BASE_DIR, "data", "baseline")
BASELINE_FILE = os.path.join(BASELINE_DIR, "latest_news.json")

# Candidate RSS endpoints. Test these from your Windows machine.
# Mint has two feeds, grouped under the source name "Mint".
FEED_JOBS = [
    ("Business Standard", "markets",
     "https://www.business-standard.com/rss/markets-106.rss"),
    ("Moneycontrol", "latest",
     "https://www.moneycontrol.com/rss/latestnews.xml"),
    ("CNBC-TV18", "markets",
     "https://www.cnbctv18.com/commonfeeds/v1/cne/rss/market.xml"),
    ("Mint", "companies",
     "https://www.livemint.com/rss/companiesRSS"),
    ("Mint", "markets",
     "https://www.livemint.com/rss/marketsRSS"),
]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 Chrome/120.0 Safari/537.36"
}

COMPANIES = {
    "RELIANCE": ["Reliance Industries", "Reliance Industries Limited", "RIL"],
    "TCS": ["Tata Consultancy Services"],
    "INFY": ["Infosys", "Infosys Limited"],
    "HDFCBANK": ["HDFC Bank", "HDFC Bank Limited"],
    "ICICIBANK": ["ICICI Bank", "ICICI Bank Limited"],
    "TATASTEEL": ["Tata Steel", "Tata Steel Limited"],
    "SBIN": ["State Bank of India", "SBI"],
    "ITC": ["ITC Limited", "ITC Ltd"],
    "SUNPHARMA": ["Sun Pharmaceutical", "Sun Pharma",
                  "Sun Pharmaceutical Industries"],
    "BHARTIARTL": ["Bharti Airtel", "Airtel"],
    "ADANIENT": ["Adani Enterprises", "Adani Enterprises Limited"],
    "COALINDIA": ["Coal India", "Coal India Limited"],
    "BAJFINANCE": ["Bajaj Finance", "Bajaj Finance Limited"],
}

SOURCES = ["Business Standard", "Moneycontrol", "CNBC-TV18", "Mint"]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

producer = None


# ---------------- HELPERS ----------------

def find_companies(text):
    text = (text or "").lower()
    matches = []
    for ticker, keywords in COMPANIES.items():
        for keyword in keywords:
            pattern = r"(?<!\w)" + re.escape(keyword.lower()) + r"(?!\w)"
            if re.search(pattern, text):
                matches.append(ticker)
                break
    return matches


def article_id_for(link, title):
    identity = link.strip() if link else title.strip()
    return hashlib.sha256(identity.encode("utf-8")).hexdigest()


def parse_published_at(entry):
    parsed = entry.get("published_parsed") or entry.get("updated_parsed")
    if not parsed:
        return None
    return datetime.fromtimestamp(
        calendar.timegm(parsed), timezone.utc
    ).isoformat()


def article_datetime(article):
    value = article.get("published_at") or article.get("ingested_at")
    if not value:
        return datetime.min.replace(tzinfo=timezone.utc)
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    except (TypeError, ValueError):
        return datetime.min.replace(tzinfo=timezone.utc)


# ---------------- PARALLEL RSS FETCH ----------------

def fetch_one_feed(source, feed_name, feed_url):
    logging.info("Fetching %s (%s)", source, feed_name)
    response = requests.get(feed_url, headers=HEADERS, timeout=30)
    response.raise_for_status()

    feed = feedparser.parse(response.content)
    if feed.bozo and not feed.entries:
        raise ValueError(f"RSS parsing failed: {feed.bozo_exception}")

    fetched_at = datetime.now(timezone.utc).isoformat()
    articles = []

    for entry in feed.entries:
        headline = (entry.get("title") or "").strip()
        link = (entry.get("link") or "").strip()
        description = entry.get("summary") or ""
        content_parts = entry.get("content") or []
        full_text = (
            content_parts[0].get("value", description)
            if content_parts else description
        )

        tickers = find_companies(" ".join([headline, description, full_text]))
        if not tickers or not headline:
            continue

        articles.append({
            "article_id": article_id_for(link, headline),
            "published_at": parse_published_at(entry),
            "source": source,
            "source_feed": feed_name,
            "headline": headline,
            "full_text": full_text,
            "url": link,
            "ingested_at": fetched_at,
            "matched_companies": tickers
        })

    logging.info("%s (%s): %d relevant / %d total entries",
                 source, feed_name, len(articles), len(feed.entries))

    return {
        "source": source,
        "feed_name": feed_name,
        "articles": articles,
        "entry_count": len(feed.entries),
        "error": None
    }


def fetch_all_sources():
    results = []
    with ThreadPoolExecutor(max_workers=len(FEED_JOBS)) as executor:
        futures = {
            executor.submit(fetch_one_feed, source, feed_name, url):
                (source, feed_name)
            for source, feed_name, url in FEED_JOBS
        }
        for future in as_completed(futures):
            source, feed_name = futures[future]
            try:
                results.append(future.result())
            except Exception as exc:
                logging.error("%s (%s) failed: %s", source, feed_name, exc)
                results.append({
                    "source": source,
                    "feed_name": feed_name,
                    "articles": [],
                    "entry_count": 0,
                    "error": str(exc)
                })
    return results


# ---------------- KAFKA PUBLISH ----------------

def publish_to_kafka(articles):
    sent = 0
    failed = 0

    for article in articles:
        try:
            metadata = producer.send(
                KAFKA_TOPIC,
                key=article["article_id"],
                value=article
            ).get(timeout=30)
            sent += 1
            logging.info("Kafka published | %s | partition=%s | offset=%s",
                         article["source"], metadata.partition, metadata.offset)
        except Exception as exc:
            failed += 1
            logging.error("Kafka publish failed: %s", exc)

    producer.flush()
    print(f"Kafka publish result: {sent} succeeded, {failed} failed.")
    return sent, failed


# ---------------- BASELINE ----------------

def empty_baseline():
    return {
        ticker: {source: None for source in SOURCES}
        for ticker in COMPANIES
    }


def load_baseline():
    os.makedirs(BASELINE_DIR, exist_ok=True)
    if not os.path.exists(BASELINE_FILE):
        return empty_baseline()

    with open(BASELINE_FILE, "r", encoding="utf-8") as file:
        loaded = json.load(file)

    baseline = empty_baseline()
    for ticker in COMPANIES:
        if isinstance(loaded.get(ticker), dict):
            for source in SOURCES:
                article = loaded[ticker].get(source)
                if isinstance(article, dict):
                    baseline[ticker][source] = article
    return baseline


def save_baseline(baseline):
    os.makedirs(BASELINE_DIR, exist_ok=True)
    fd, temp_path = tempfile.mkstemp(dir=BASELINE_DIR, suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as file:
            json.dump(baseline, file, indent=4, ensure_ascii=False)
        os.replace(temp_path, BASELINE_FILE)
    except Exception:
        if os.path.exists(temp_path):
            os.remove(temp_path)
        raise


def flatten_articles(feed_results):
    return [
        article
        for result in feed_results
        for article in result["articles"]
    ]


def latest_by_company_and_source(articles):
    latest = {}
    for ticker in COMPANIES:
        for source in SOURCES:
            candidates = [
                article for article in articles
                if article["source"] == source
                and ticker in article["matched_companies"]
            ]
            if candidates:
                latest[(ticker, source)] = max(
                    candidates, key=article_datetime
                )
    return latest


def update_baseline(baseline, latest_records):
    changes = []
    for (ticker, source), article in latest_records.items():
        previous = baseline[ticker].get(source)

        if previous is None:
            baseline[ticker][source] = article
            changes.append((ticker, source, "INITIALIZED"))
        elif article_datetime(article) > article_datetime(previous):
            baseline[ticker][source] = article
            changes.append((ticker, source, "UPDATED"))
        else:
            changes.append((ticker, source, "RETAINED"))

    return changes


# ---------------- DISPLAY ----------------

def print_initial_baseline(baseline):
    print("\n" + "=" * 88)
    print("                    INITIAL NEWS BASELINE")
    print("=" * 88)

    count = 0
    for ticker, source_records in baseline.items():
        print(f"\n{ticker}")
        for source, article in source_records.items():
            # Empty slots are omitted; no "No matching news available".
            if article:
                count += 1
                print(f"  {source}: {article['headline']}")
                print(f"    Published: {article.get('published_at')}")
                print(f"    URL: {article.get('url')}")

    print(f"\nBaseline records populated: {count} of 52")
    print(f"Baseline file: {BASELINE_FILE}")
    print("=" * 88)


def print_cycle_report(baseline, changes, feed_results, cycle_number):
    print("\n" + "=" * 88)
    print(f"                 NEWS UPDATE - CYCLE {cycle_number}")
    print("=" * 88)

    print("\nSOURCE FETCH STATUS")
    for result in sorted(feed_results,
                         key=lambda item: (item["source"], item["feed_name"])):
        if result["error"]:
            print(f"  {result['source']} ({result['feed_name']}): FETCH FAILED")
        else:
            print(f"  {result['source']} ({result['feed_name']}): "
                  f"{result['entry_count']} entries; "
                  f"{len(result['articles'])} relevant")

    status_map = {
        (ticker, source): status
        for ticker, source, status in changes
    }

    print("\nCOMPANY-WISE LATEST BASELINE")
    for ticker in COMPANIES:
        print(f"\n{ticker}")
        for source in SOURCES:
            article = baseline[ticker].get(source)
            if article:
                status = status_map.get((ticker, source), "RETAINED")
                print(f"  {source} [{status}]: {article['headline']}")
                print(f"    Published: {article.get('published_at')}")
                print(f"    URL: {article.get('url')}")

    counts = {"INITIALIZED": 0, "UPDATED": 0, "RETAINED": 0}
    for _, _, status in changes:
        counts[status] = counts.get(status, 0) + 1

    print("\nCYCLE CHANGE COUNTS")
    for status, count in counts.items():
        print(f"  {status}: {count}")

    print(f"\nBaseline saved: {BASELINE_FILE}")
    print("=" * 88)


# ---------------- CYCLE ----------------

def run_cycle(baseline, cycle_number, initial=False):
    print("\nFetching all configured RSS feeds in parallel...")
    feed_results = fetch_all_sources()
    articles = flatten_articles(feed_results)

    print(f"Relevant raw articles fetched: {len(articles)}")
    publish_to_kafka(articles)

    latest_records = latest_by_company_and_source(articles)
    changes = update_baseline(baseline, latest_records)
    save_baseline(baseline)

    if initial:
        print_initial_baseline(baseline)
    else:
        print_cycle_report(baseline, changes, feed_results, cycle_number)


# ---------------- MAIN ----------------

def main():
    global producer

    print("\nINDIAN COMPANY NEWS PRODUCER STARTED")
    print("Companies: 13")
    print("Sources: Business Standard, Moneycontrol, CNBC-TV18, Mint")
    print("Monitoring interval: 15 minutes")

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_SERVER,
        key_serializer=lambda key: key.encode("utf-8"),
        value_serializer=lambda value: json.dumps(
            value, ensure_ascii=False
        ).encode("utf-8"),
        acks="all",
        retries=10,
        request_timeout_ms=30000
    )

    baseline = load_baseline()
    first_run = not any(
        article
        for records in baseline.values()
        for article in records.values()
    )

    if first_run:
        print("\nCreating initial baseline from latest articles available in feeds...")
        run_cycle(baseline, cycle_number=0, initial=True)
    else:
        print("\nExisting baseline loaded.")
        print_initial_baseline(baseline)

    cycle_number = 1

    try:
        while True:
            print("\nNext collection in 15 minutes...")
            time.sleep(CHECK_INTERVAL_SECONDS)
            run_cycle(baseline, cycle_number)
            cycle_number += 1

    except KeyboardInterrupt:
        print("\nProducer stopped by user.")

    finally:
        if producer is not None:
            producer.flush()
            producer.close()


if __name__ == "__main__":
    main()
