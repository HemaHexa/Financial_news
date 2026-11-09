import hashlib
import json
import logging
import time
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from urllib.parse import urlsplit, urlunsplit

import feedparser
import requests
import trafilatura
from kafka import KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable

from config.settings import (
    KAFKA_BOOTSTRAP_SERVERS, NEWS_TOPIC, NEWS_POLL_SECONDS,
    NEWS_BUFFER_FILE, RSS_FEEDS, ARTICLE_FETCH_TIMEOUT, MAX_ARTICLE_HTML_CHARS
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("news-producer")
HEADERS = {"User-Agent": "FinancialNewsResearchBot/1.0 (educational project)"}

def now_utc():
    return datetime.now(timezone.utc).isoformat()

def normalize_url(url):
    parts = urlsplit((url or "").strip())
    query = [p for p in parts.query.split("&") if p and not p.lower().startswith(
        ("utm_", "ocid=", "ved=", "usg=")
    )]
    return urlunsplit((parts.scheme.lower(), parts.netloc.lower(), parts.path.rstrip("/"), "&".join(query), ""))

def article_id_for(url, headline):
    identity = normalize_url(url) or headline.strip().lower()
    return hashlib.sha256(identity.encode("utf-8")).hexdigest()

def published_at(entry):
    raw = entry.get("published") or entry.get("updated")
    if raw:
        try:
            dt = parsedate_to_datetime(raw)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc).isoformat()
        except Exception:
            pass
    return now_utc()

def extract_page(url):
    if not url:
        return "", ""
    try:
        response = requests.get(url, headers=HEADERS, timeout=ARTICLE_FETCH_TIMEOUT)
        response.raise_for_status()
        html = response.text[:MAX_ARTICLE_HTML_CHARS]
        text = trafilatura.extract(html, include_comments=False, include_tables=True) or ""
        return text.strip(), html
    except Exception as exc:
        log.warning("Article fetch failed for %s: %s", url, exc)
        return "", ""

def make_event(entry, feed_url):
    headline = (entry.get("title") or "").strip()
    url = normalize_url(entry.get("link") or "")
    description = (entry.get("summary") or entry.get("description") or "").strip()
    full_text, raw_html = extract_page(url)
    source = ""
    if entry.get("source"):
        source = entry.source.get("title", "")
    if not source and url:
        source = urlsplit(url).netloc
    return {
        "schema_version": "1.0",
        "article_id": article_id_for(url, headline),
        "published_at": published_at(entry),
        "source": source or "unknown",
        "headline": headline,
        "rss_description": description,
        "full_text": full_text,
        "url": url,
        "raw_html": raw_html,
        "feed_url": feed_url,
        "ingested_at": now_utc(),
    }

class NewsPublisher:
    def __init__(self):
        self.producer = None
        self.seen_ids = set()
        self.buffer_path = NEWS_BUFFER_FILE
        self.buffer_path.parent.mkdir(parents=True, exist_ok=True)
        if self.buffer_path.exists():
            for line in self.buffer_path.read_text(encoding="utf-8").splitlines():
                try:
                    self.seen_ids.add(json.loads(line)["article_id"])
                except Exception:
                    continue

    def connect(self):
        if self.producer is None:
            self.producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(","),
                acks="all", retries=5, linger_ms=50,
                key_serializer=lambda key: key.encode("utf-8"),
                value_serializer=lambda value: json.dumps(value, ensure_ascii=False).encode("utf-8"),
            )
        return self.producer

    def buffer(self, event):
        with self.buffer_path.open("a", encoding="utf-8") as stream:
            stream.write(json.dumps(event, ensure_ascii=False) + "\n")
        self.seen_ids.add(event["article_id"])
        log.warning("Buffered event locally: %s", event["article_id"])

    def publish(self, event):
        try:
            self.connect().send(NEWS_TOPIC, key=event["article_id"], value=event).get(timeout=20)
            self.seen_ids.add(event["article_id"])
            log.info("Published: %s", event["headline"][:100])
            return True
        except (NoBrokersAvailable, KafkaError, OSError, TimeoutError) as exc:
            log.error("Kafka publish failed: %s", exc)
            self.producer = None
            self.buffer(event)
            return False

    def replay_buffer(self):
        if not self.buffer_path.exists() or self.buffer_path.stat().st_size == 0:
            return
        lines = self.buffer_path.read_text(encoding="utf-8").splitlines()
        remaining = []
        for index, line in enumerate(lines):
            try:
                event = json.loads(line)
                self.connect().send(NEWS_TOPIC, key=event["article_id"], value=event).get(timeout=20)
                log.info("Replayed buffered event: %s", event["article_id"])
            except Exception as exc:
                log.error("Replay stopped at buffered record %d: %s", index + 1, exc)
                remaining = lines[index:]
                break
        temp = self.buffer_path.with_suffix(".tmp")
        temp.write_text("\n".join(remaining) + ("\n" if remaining else ""), encoding="utf-8")
        temp.replace(self.buffer_path)

    def close(self):
        if self.producer is not None:
            self.producer.flush(timeout=10)
            self.producer.close(timeout=10)

def collect_once(publisher):
    publisher.replay_buffer()
    for feed_url in RSS_FEEDS:
        log.info("Fetching RSS: %s", feed_url)
        parsed = feedparser.parse(feed_url)
        if parsed.bozo:
            log.warning("RSS parse warning: %s", parsed.bozo_exception)
        for entry in parsed.entries:
            try:
                event = make_event(entry, feed_url)
                if not event["headline"] or not event["url"]:
                    continue
                if event["article_id"] in publisher.seen_ids:
                    continue
                publisher.publish(event)
            except Exception:
                log.exception("Failed to process an RSS entry")

def main():
    publisher = NewsPublisher()
    log.info("News producer running; topic=%s", NEWS_TOPIC)
    try:
        while True:
            collect_once(publisher)
            time.sleep(NEWS_POLL_SECONDS)
    except KeyboardInterrupt:
        log.info("Stopping producer")
    finally:
        publisher.close()

if __name__ == "__main__":
    main()
