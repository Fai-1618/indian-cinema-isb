"""
pipeline.py
-----------
Orchestrates the full Indian Song Book (ISB) data pipeline:

  Step 1 – Parse all ISB source files from the cloned giit repo
  Step 2 – (Optional) Enrich with IMDb IDs via cinemagoer
  Step 3 – Export to CSV (and optionally SQLite)

Usage examples:

  # Full run from the git repo + IMDb enrichment:
  python pipeline.py --docs ../giit/docs --imdb --out isb_full.csv

  # Fast run, no IMDb (just parse + export):
  python pipeline.py --docs ../giit/docs --out isb_lyrics_only.csv

  # Export from a previously scraped SQLite DB (from giitaayan_scraper.py):
  python pipeline.py --from-db scrape.db --imdb --out isb_scraped.csv

  # Merge git-repo parse + scraped DB (scraped takes priority for duplicates):
  python pipeline.py --docs ../giit/docs --from-db scrape.db --imdb --out isb_merged.csv

Output columns:
  isb_id, stitle, film, year, starring, singer, music, lyricist,
  lyrics_itrans, lyrics_rendered, imdb_film_id,
  imdb_singer_ids, imdb_music_ids, imdb_lyricist_ids, source_file
"""

import argparse
import logging
import sqlite3
import sys
import time
from pathlib import Path

import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


# ── Step 1a: Parse git repo ───────────────────────────────────────────────────

def load_from_git(docs_dir: str) -> list[dict]:
    from isb_parser import parse_all_files
    log.info(f"Parsing ISB source files from: {docs_dir}")
    return parse_all_files(docs_dir, verbose=True)


# ── Step 1b: Load from scraper SQLite DB ─────────────────────────────────────

def load_from_db(db_path: str) -> list[dict]:
    log.info(f"Loading scraped records from: {db_path}")
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    rows = conn.execute("SELECT * FROM songs ORDER BY isb_id").fetchall()
    conn.close()
    records = [dict(r) for r in rows]
    log.info(f"  Loaded {len(records):,} records from SQLite")
    return records


# ── Merge two record lists (scraped data wins on conflict) ────────────────────

def merge_records(git_records: list[dict], db_records: list[dict]) -> list[dict]:
    merged = {r["isb_id"]: r for r in git_records}
    for r in db_records:
        if r["isb_id"] in merged:
            # Scraped record may have lyrics_rendered; git record has lyrics_itrans.
            # Merge: keep both where present.
            git_r = merged[r["isb_id"]]
            r.setdefault("lyrics_itrans", git_r.get("lyrics_itrans", ""))
            r.setdefault("source_file", git_r.get("source_file", ""))
        merged[r["isb_id"]] = r
    result = sorted(merged.values(), key=lambda x: x.get("isb_id") or 0)
    log.info(f"Merged → {len(result):,} unique records")
    return result


# ── Step 2: IMDb enrichment ───────────────────────────────────────────────────

def enrich_with_imdb(records: list[dict], cache_path: str) -> list[dict]:
    try:
        from imdb_enricher import IMDbEnricher
    except ImportError:
        log.error("imdb_enricher.py not found. Skipping IMDb enrichment.")
        return records

    enricher = IMDbEnricher(cache_path=cache_path)
    return enricher.enrich_records(records)


# ── Step 3: Export CSV ────────────────────────────────────────────────────────

COLUMN_ORDER = [
    "isb_id",
    "stitle",
    "film",
    "year",
    "starring",
    "singer",
    "music",
    "lyricist",
    "lyrics_itrans",
    "lyrics_rendered",
    "imdb_film_id",
    "imdb_singer_ids",
    "imdb_music_ids",
    "imdb_lyricist_ids",
    "source_file",
]


def export_csv(records: list[dict], out_path: str):
    df = pd.DataFrame(records)

    # Ensure all expected columns exist (some may be absent if IMDb was skipped)
    for col in COLUMN_ORDER:
        if col not in df.columns:
            df[col] = None

    df = df[COLUMN_ORDER]
    df = df.sort_values("isb_id").reset_index(drop=True)

    df.to_csv(out_path, index=False, encoding="utf-8-sig")   # utf-8-sig for Excel compat
    log.info(f"✓ Exported {len(df):,} rows → {out_path}")

    # Summary stats
    print("\n── Dataset summary ─────────────────────────────────────────")
    print(f"  Total songs    : {len(df):,}")
    print(f"  Films covered  : {df['film'].nunique():,}")
    print(f"  Year range     : {int(df['year'].min())} – {int(df['year'].max())}")
    if "imdb_film_id" in df and df["imdb_film_id"].notna().any():
        matched = df["imdb_film_id"].notna().sum()
        print(f"  IMDb matched   : {matched:,} / {len(df):,} ({100*matched/len(df):.1f}%)")
    print("────────────────────────────────────────────────────────────\n")


# ── CLI ───────────────────────────────────────────────────────────────────────

def main():
    p = argparse.ArgumentParser(
        description="ISB → CSV pipeline with optional IMDb enrichment",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("--docs",     type=str, default=None,
                   help="Path to giit/docs directory (git repo source files)")
    p.add_argument("--from-db",  type=str, default=None, dest="from_db",
                   help="Path to SQLite DB from giitaayan_scraper.py")
    p.add_argument("--out",      type=str, default="isb_songs.csv",
                   help="Output CSV file path (default: isb_songs.csv)")
    p.add_argument("--imdb",     action="store_true",
                   help="Enable IMDb ID enrichment (slow, ~hours for full set)")
    p.add_argument("--imdb-cache", type=str, default="imdb_cache.json",
                   help="Path for IMDb cache JSON (default: imdb_cache.json)")
    p.add_argument("--limit",    type=int, default=None,
                   help="Process only the first N records (for testing)")
    args = p.parse_args()

    if not args.docs and not args.from_db:
        p.error("Provide at least one of --docs or --from-db")

    t0 = time.time()
    records = []

    # ── Load ──────────────────────────────────────────────────────────────
    git_records, db_records = [], []
    if args.docs:
        git_records = load_from_git(args.docs)
    if args.from_db:
        db_records = load_from_db(args.from_db)

    if git_records and db_records:
        records = merge_records(git_records, db_records)
    else:
        records = git_records or db_records

    if args.limit:
        records = records[:args.limit]
        log.info(f"Limited to first {args.limit} records")

    # ── Enrich ────────────────────────────────────────────────────────────
    if args.imdb:
        log.info("Starting IMDb enrichment (this will take a while) …")
        records = enrich_with_imdb(records, args.imdb_cache)
    else:
        log.info("Skipping IMDb enrichment (use --imdb to enable)")

    # ── Export ────────────────────────────────────────────────────────────
    export_csv(records, args.out)

    elapsed = time.time() - t0
    log.info(f"Pipeline complete in {elapsed:.1f}s")


if __name__ == "__main__":
    main()
