"""
giitaayan_scraper.py
--------------------
Scrapes https://www.giitaayan.com/showlyrics?isb=N for song pages.

Use this as a SUPPLEMENT to isb_parser.py (git repo) to:
  - Pick up songs added after your last git clone
  - Capture rendered/transliterated Devanagari text from the site
  - Verify metadata against the live site

Design principles
  ✓ Polite: 1-2 s delay between requests, respects retry-after headers
  ✓ Resumable: tracks progress in SQLite so crashes don't lose work
  ✓ Session-based: persistent TCP connections, browser-like headers
  ✓ Modular: parse_page() is pure (no I/O) and fully testable

Usage:
    python giitaayan_scraper.py --start 1 --end 25121 --db scrape.db
    python giitaayan_scraper.py --start 1 --end 25121 --db scrape.db --resume
"""

import argparse
import sqlite3
import time
import random
import logging
from typing import Optional

import requests
from bs4 import BeautifulSoup

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

BASE_URL = "https://www.giitaayan.com/showlyrics"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9,hi;q=0.8",
    "Referer": "https://www.giitaayan.com/",
}


# ── HTML parser ───────────────────────────────────────────────────────────────

def parse_page(html: str, isb_id: int) -> Optional[dict]:
    """
    Parse the HTML of a giitaayan showlyrics page.

    The page structure (verified manually):
      - <title> contains song name | film | giitaayan
      - A metadata table with rows like:
          Film:  <value>   Singer:  <value>   etc.
      - The lyrics block is in a <pre> or <div class="lyrics"> element.

    Returns None if the page appears to be a 404 / invalid ISB.
    """
    soup = BeautifulSoup(html, "lxml")

    # ── Guard: check for "not found" indicators ───────────────────────────
    title_tag = soup.find("title")
    page_title = title_tag.get_text(strip=True) if title_tag else ""
    if "not found" in page_title.lower() or len(html) < 500:
        return None

    record = {
        "isb_id":   isb_id,
        "stitle":   "",
        "film":     "",
        "year":     None,
        "starring": "",
        "singer":   "",
        "music":    "",
        "lyricist": "",
        "lyrics_rendered": "",   # Devanagari / romanised from the live site
    }

    # ── Metadata: look for <b>Label:</b> pattern ──────────────────────────
    label_map = {
        "film":      "film",
        "movie":     "film",
        "year":      "year",
        "singer":    "singer",
        "singers":   "singer",
        "music":     "music",
        "composer":  "music",
        "lyrics":    "lyricist",
        "lyricist":  "lyricist",
        "starring":  "starring",
        "cast":      "starring",
    }

    for b_tag in soup.find_all("b"):
        label_text = b_tag.get_text(strip=True).rstrip(":").lower()
        if label_text in label_map:
            key = label_map[label_text]
            # Value is usually the next sibling text node
            sibling = b_tag.next_sibling
            if sibling:
                value = str(sibling).strip().lstrip(":").strip()
                if key == "year" and value.isdigit():
                    record["year"] = int(value)
                elif key != "year":
                    record[key] = value

    # ── Song title: from <h1> or <title> ─────────────────────────────────
    h1 = soup.find("h1")
    if h1:
        record["stitle"] = h1.get_text(strip=True)
    elif "|" in page_title:
        record["stitle"] = page_title.split("|")[0].strip()

    # ── Lyrics block ──────────────────────────────────────────────────────
    # giitaayan wraps lyrics in <pre> or a div with id/class containing "lyric"
    lyrics_el = (
        soup.find("pre") or
        soup.find(id=lambda i: i and "lyric" in i.lower()) or
        soup.find(class_=lambda c: c and "lyric" in " ".join(c).lower())
    )
    if lyrics_el:
        record["lyrics_rendered"] = lyrics_el.get_text("\n").strip()

    return record


# ── SQLite progress store ─────────────────────────────────────────────────────

def init_db(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS songs (
            isb_id          INTEGER PRIMARY KEY,
            stitle          TEXT,
            film            TEXT,
            year            INTEGER,
            starring        TEXT,
            singer          TEXT,
            music           TEXT,
            lyricist        TEXT,
            lyrics_rendered TEXT,
            scraped_at      TEXT DEFAULT (datetime('now'))
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS failures (
            isb_id   INTEGER PRIMARY KEY,
            reason   TEXT,
            ts       TEXT DEFAULT (datetime('now'))
        )
    """)
    conn.commit()
    return conn


def already_done(conn: sqlite3.Connection, isb_id: int) -> bool:
    row = conn.execute(
        "SELECT 1 FROM songs WHERE isb_id=? UNION SELECT 1 FROM failures WHERE isb_id=?",
        (isb_id, isb_id)
    ).fetchone()
    return row is not None


def save_record(conn: sqlite3.Connection, rec: dict):
    conn.execute("""
        INSERT OR REPLACE INTO songs
            (isb_id, stitle, film, year, starring, singer, music, lyricist, lyrics_rendered)
        VALUES (?,?,?,?,?,?,?,?,?)
    """, (
        rec["isb_id"], rec["stitle"], rec["film"], rec["year"],
        rec["starring"], rec["singer"], rec["music"], rec["lyricist"],
        rec["lyrics_rendered"],
    ))
    conn.commit()


def save_failure(conn: sqlite3.Connection, isb_id: int, reason: str):
    conn.execute(
        "INSERT OR REPLACE INTO failures (isb_id, reason) VALUES (?,?)",
        (isb_id, reason)
    )
    conn.commit()


# ── Main scrape loop ──────────────────────────────────────────────────────────

def scrape(
    start: int,
    end: int,
    db_path: str,
    resume: bool = True,
    min_delay: float = 1.0,
    max_delay: float = 2.5,
    max_retries: int = 3,
):
    conn = init_db(db_path)
    session = requests.Session()
    session.headers.update(HEADERS)

    total = end - start + 1
    done = 0
    skipped = 0

    log.info(f"Scraping ISB {start}–{end} ({total:,} pages) → {db_path}")

    for isb_id in range(start, end + 1):
        if resume and already_done(conn, isb_id):
            skipped += 1
            continue

        url = f"{BASE_URL}?isb={isb_id}"
        rec = None

        for attempt in range(1, max_retries + 1):
            try:
                resp = session.get(url, timeout=15)

                if resp.status_code == 200:
                    rec = parse_page(resp.text, isb_id)
                    break
                elif resp.status_code == 429:
                    wait = int(resp.headers.get("Retry-After", 30))
                    log.warning(f"Rate limited on isb={isb_id}. Sleeping {wait}s …")
                    time.sleep(wait)
                elif resp.status_code == 404:
                    log.debug(f"isb={isb_id}: 404 (no such song)")
                    save_failure(conn, isb_id, "404")
                    break
                else:
                    log.warning(f"isb={isb_id}: HTTP {resp.status_code} (attempt {attempt})")
                    time.sleep(5 * attempt)

            except requests.RequestException as e:
                log.warning(f"isb={isb_id}: request error {e} (attempt {attempt})")
                time.sleep(5 * attempt)

        if rec is not None:
            save_record(conn, rec)
            done += 1
        else:
            save_failure(conn, isb_id, "parse_failed_or_404")

        if done % 100 == 0 and done > 0:
            log.info(f"  Progress: {done:,} saved, {skipped:,} skipped (isb={isb_id})")

        # Polite random delay
        time.sleep(random.uniform(min_delay, max_delay))

    log.info(f"\n✓ Done. Saved: {done:,}  Skipped (resumed): {skipped:,}")
    conn.close()


# ── CLI ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scrape giitaayan.com ISB pages")
    parser.add_argument("--start",   type=int, default=1,     help="First ISB ID")
    parser.add_argument("--end",     type=int, default=25121, help="Last ISB ID")
    parser.add_argument("--db",      type=str, default="scrape.db", help="SQLite output path")
    parser.add_argument("--resume",  action="store_true", default=True,
                        help="Skip already-scraped IDs (default: True)")
    parser.add_argument("--no-resume", dest="resume", action="store_false")
    parser.add_argument("--min-delay", type=float, default=1.0)
    parser.add_argument("--max-delay", type=float, default=2.5)
    args = parser.parse_args()

    scrape(
        start=args.start,
        end=args.end,
        db_path=args.db,
        resume=args.resume,
        min_delay=args.min_delay,
        max_delay=args.max_delay,
    )
