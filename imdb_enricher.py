"""
imdb_enricher.py
----------------
Resolves film names and person names (singers, directors, lyricists) to
their IMDb IDs using the `cinemagoer` library (formerly IMDbPY).

Design
  ✓ Disk cache (JSON) – each unique film/person is looked up ONCE,
    then saved to avoid re-querying
  ✓ Fuzzy matching via rapidfuzz – handles transliteration variants
    (e.g. "Pyaasa" vs "Pyaasaa", "Laxmikant-Pyarelal" vs "Laxmikant Pyarelal")
  ✓ Year-aware film search – passing the year dramatically improves precision
  ✓ Graceful fallback – returns None when no confident match is found,
    rather than guessing wrong

Usage:
    from imdb_enricher import IMDbEnricher

    enricher = IMDbEnricher(cache_path="imdb_cache.json")

    film_id = enricher.resolve_film("Sholay", year=1975)
    singer_id = enricher.resolve_person("Kishore Kumar")

    enricher.save_cache()   # persist to disk
"""

import json
import logging
import re
import time
from pathlib import Path
from typing import Optional

log = logging.getLogger(__name__)

# Soft dependency – gracefully degrade if cinemagoer not installed
try:
    from imdb import Cinemagoer
    _CINEMAGOER_AVAILABLE = True
except ImportError:
    _CINEMAGOER_AVAILABLE = False
    log.warning("cinemagoer not installed – IMDb resolution disabled. "
                "Run: pip install cinemagoer")

try:
    from rapidfuzz import fuzz, process as rfprocess
    _RAPIDFUZZ_AVAILABLE = True
except ImportError:
    _RAPIDFUZZ_AVAILABLE = False
    log.warning("rapidfuzz not installed – falling back to exact title match. "
                "Run: pip install rapidfuzz")


# ── Helpers ───────────────────────────────────────────────────────────────────

def _normalise(text: str) -> str:
    """Lower-case, strip punctuation, collapse spaces – for fuzzy comparison."""
    text = text.lower()
    text = re.sub(r"[^\w\s]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _best_match(query: str, candidates: list[str], threshold: float = 82.0) -> Optional[int]:
    """
    Return the index of the best fuzzy match in `candidates` for `query`,
    or None if the best score is below `threshold`.
    """
    if not _RAPIDFUZZ_AVAILABLE or not candidates:
        # Exact match fallback
        q = _normalise(query)
        for i, c in enumerate(candidates):
            if _normalise(c) == q:
                return i
        return None

    result = rfprocess.extractOne(
        _normalise(query),
        [_normalise(c) for c in candidates],
        scorer=fuzz.token_sort_ratio,
    )
    if result and result[1] >= threshold:
        return result[2]   # index
    return None


# ── Main class ────────────────────────────────────────────────────────────────

class IMDbEnricher:
    """
    Resolves film/person names to IMDb IDs, with local JSON caching.

    Cache structure:
        {
          "films":   {"Sholay|1975": "tt0073708", ...},
          "persons": {"Kishore Kumar": "nm0356691", ...}
        }
    """

    def __init__(
        self,
        cache_path: str = "imdb_cache.json",
        request_delay: float = 0.5,    # seconds between IMDb API calls
        film_threshold: float = 82.0,  # min fuzzy score for film match
        person_threshold: float = 78.0,
    ):
        self.cache_path = Path(cache_path)
        self.delay = request_delay
        self.film_threshold = film_threshold
        self.person_threshold = person_threshold

        self._cache: dict = {"films": {}, "persons": {}}
        self._ia = None

        self._load_cache()
        if _CINEMAGOER_AVAILABLE:
            self._ia = Cinemagoer()

    # ── Cache I/O ─────────────────────────────────────────────────────────

    def _load_cache(self):
        if self.cache_path.exists():
            with open(self.cache_path, encoding="utf-8") as f:
                loaded = json.load(f)
            self._cache["films"].update(loaded.get("films", {}))
            self._cache["persons"].update(loaded.get("persons", {}))
            log.info(
                f"Loaded cache: {len(self._cache['films'])} films, "
                f"{len(self._cache['persons'])} persons"
            )

    def save_cache(self):
        with open(self.cache_path, "w", encoding="utf-8") as f:
            json.dump(self._cache, f, ensure_ascii=False, indent=2)
        log.info(f"Cache saved → {self.cache_path}")

    # ── Film resolution ───────────────────────────────────────────────────

    def resolve_film(self, title: str, year: Optional[int] = None) -> Optional[str]:
        """
        Return the IMDb ID (e.g. 'tt0073708') for a film, or None.

        Strategy:
          1. Cache hit → return immediately
          2. Search IMDb with title + year
          3. Fuzzy-match the top results against the queried title
          4. Prefer results with matching year if available
        """
        if not title:
            return None

        cache_key = f"{title}|{year}" if year else title

        # Cache hit
        if cache_key in self._cache["films"]:
            return self._cache["films"][cache_key]

        if not self._ia:
            return None

        time.sleep(self.delay)

        try:
            results = self._ia.search_movie(title)
        except Exception as e:
            log.warning(f"IMDb search failed for film '{title}': {e}")
            return None

        if not results:
            self._cache["films"][cache_key] = None
            return None

        # Filter to plausible year range (±2 years)
        if year:
            year_filtered = [
                r for r in results
                if abs(int(r.get("year", year or 0)) - year) <= 2
            ]
            candidates = year_filtered if year_filtered else results
        else:
            candidates = results

        # Fuzzy match on title
        candidate_titles = [r.get("title", "") for r in candidates]
        idx = _best_match(title, candidate_titles, threshold=self.film_threshold)

        if idx is None:
            log.debug(f"No confident match for film '{title}' (year={year})")
            self._cache["films"][cache_key] = None
            return None

        imdb_id = f"tt{candidates[idx].movieID}"
        log.debug(f"Resolved film: '{title}' → {imdb_id} ({candidate_titles[idx]})")
        self._cache["films"][cache_key] = imdb_id
        return imdb_id

    # ── Person resolution ─────────────────────────────────────────────────

    def resolve_person(self, name: str) -> Optional[str]:
        """
        Return the IMDb ID (e.g. 'nm0356691') for a person, or None.

        Handles comma-separated names – resolves the FIRST name only
        (use resolve_persons() for multi-value fields).
        """
        name = name.split(",")[0].strip()
        if not name:
            return None

        if name in self._cache["persons"]:
            return self._cache["persons"][name]

        if not self._ia:
            return None

        time.sleep(self.delay)

        try:
            results = self._ia.search_person(name)
        except Exception as e:
            log.warning(f"IMDb search failed for person '{name}': {e}")
            return None

        if not results:
            self._cache["persons"][name] = None
            return None

        candidate_names = [r.get("name", "") for r in results]
        idx = _best_match(name, candidate_names, threshold=self.person_threshold)

        if idx is None:
            log.debug(f"No confident match for person '{name}'")
            self._cache["persons"][name] = None
            return None

        imdb_id = f"nm{results[idx].personID}"
        log.debug(f"Resolved person: '{name}' → {imdb_id}")
        self._cache["persons"][name] = imdb_id
        return imdb_id

    def resolve_persons(self, names_str: str) -> list[Optional[str]]:
        """
        Resolve a comma/pipe-separated list of names.
        Returns a list of IMDb IDs (or None for each unresolved name).
        """
        names = re.split(r"[,|/]", names_str)
        return [self.resolve_person(n.strip()) for n in names if n.strip()]

    # ── Batch enrichment ──────────────────────────────────────────────────

    def enrich_records(self, records: list[dict]) -> list[dict]:
        """
        Add IMDb ID columns to a list of parsed ISB records in-place.

        Adds:
            imdb_film_id      – IMDb film tt-ID
            imdb_singer_ids   – pipe-separated nm-IDs for singers
            imdb_music_ids    – pipe-separated nm-IDs for music directors
            imdb_lyricist_ids – pipe-separated nm-IDs for lyricists
        """
        # Deduplicate films for cache efficiency
        unique_films = {}
        for rec in records:
            key = (rec.get("film", ""), rec.get("year"))
            if key not in unique_films:
                unique_films[key] = None

        log.info(f"Resolving {len(unique_films):,} unique films against IMDb …")
        for i, (film, year) in enumerate(unique_films.keys()):
            unique_films[(film, year)] = self.resolve_film(film, year)
            if (i + 1) % 100 == 0:
                log.info(f"  Film resolution: {i+1:,} / {len(unique_films):,}")
                self.save_cache()  # checkpoint

        log.info("Enriching records with IMDb IDs …")
        for rec in records:
            key = (rec.get("film", ""), rec.get("year"))
            rec["imdb_film_id"] = unique_films.get(key)

            rec["imdb_singer_ids"]   = "|".join(filter(None, self.resolve_persons(rec.get("singer", ""))))
            rec["imdb_music_ids"]    = "|".join(filter(None, self.resolve_persons(rec.get("music", ""))))
            rec["imdb_lyricist_ids"] = "|".join(filter(None, self.resolve_persons(rec.get("lyricist", ""))))

        self.save_cache()
        log.info("✓ IMDb enrichment complete.")
        return records


# ── Quick smoke test ──────────────────────────────────────────────────────────

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG, format="%(levelname)s %(message)s")
    e = IMDbEnricher(cache_path="/tmp/imdb_test_cache.json")

    tests = [
        ("Sholay", 1975),
        ("Mother India", 1957),
        ("Mughal-E-Azam", 1960),
        ("Pyaasa", 1957),
    ]
    for film, yr in tests:
        fid = e.resolve_film(film, yr)
        print(f"{film} ({yr}) → {fid}")

    pid = e.resolve_person("Lata Mangeshkar")
    print(f"Lata Mangeshkar → {pid}")

    e.save_cache()
