# Indian Cinema ISB Pipeline

Extract, parse, and enrich the complete **ITRANS Song Book (ISB)** — 25,000+ Hindi/Urdu film songs from [giitaayan.com](https://www.giitaayan.com) — into a research-ready CSV with IMDb IDs attached to every film, singer, music director, and lyricist.

---

## Project structure

```
indian_cinema_isb/
├── giit/                     ← cloned from github.com/v9y/giit (the ISB source data)
│   └── docs/                 ← 10,831 .isb.txt files (25121.isb.txt is the highest)
│
└── project/
    ├── isb_parser.py         ← Stage 1: parse .isb.txt → Python dicts
    ├── giitaayan_scraper.py  ← Stage 1 (alt): live-scrape giitaayan.com → SQLite
    ├── imdb_enricher.py      ← Stage 2: resolve film/person names → IMDb IDs
    ├── pipeline.py           ← Stage 3: orchestrate + export CSV
    ├── test_pipeline.py      ← Unit tests
    └── README.md
```

---

## Quick start

### 1 — Clone the ISB source data

```bash
git clone --depth=1 https://github.com/v9y/giit.git
```

### 2 — Install dependencies

```bash
pip install requests beautifulsoup4 lxml pandas tqdm cinemagoer rapidfuzz
```

### 3 — Run the pipeline

**Fast (no IMDb, ~30 seconds):**
```bash
cd project
python pipeline.py --docs ../giit/docs --out isb_songs.csv
```

**Full (with IMDb enrichment, ~4–8 hours):**
```bash
python pipeline.py --docs ../giit/docs --imdb --out isb_full.csv
```

**Resume interrupted IMDb run:**
```bash
python pipeline.py --docs ../giit/docs --imdb --imdb-cache imdb_cache.json --out isb_full.csv
```

---

## Data source

The ISB source files use **ITRANS notation** — an ASCII romanisation of Devanagari/Hindi script. Each `.isb.txt` file follows this template:

```
\startsong
\stitle{song title in ITRANS}%
\film{Film Name}%
\year{1977}%
\starring{Actor 1, Actor 2}%
\singer{Singer Name}%
\music{Music Director}%
\lyrics{Lyricist Name}%
#indian
... lyrics in ITRANS notation ...
#endindian
\endsong
```

---

## Output CSV columns

| Column | Description |
|---|---|
| `isb_id` | ISB item number (1–25121) |
| `stitle` | Song title (ITRANS) |
| `film` | Film name |
| `year` | Release year |
| `starring` | Cast (comma-separated) |
| `singer` | Singer(s) (comma-separated) |
| `music` | Music director(s) |
| `lyricist` | Lyricist(s) |
| `lyrics_itrans` | Full lyrics in ITRANS notation |
| `lyrics_rendered` | Rendered lyrics from live site (if scraped) |
| `imdb_film_id` | IMDb film ID e.g. `tt0073708` |
| `imdb_singer_ids` | Pipe-separated IMDb nm-IDs for singers |
| `imdb_music_ids` | Pipe-separated IMDb nm-IDs for music directors |
| `imdb_lyricist_ids` | Pipe-separated IMDb nm-IDs for lyricists |
| `source_file` | Source `.isb.txt` filename |

---

## Scraping giitaayan.com directly (optional)

If you want to supplement the git repo with the live site (e.g. for rendered Devanagari):

```bash
# Scrape ISBs 1–25121, resume-safe
python giitaayan_scraper.py --start 1 --end 25121 --db scrape.db

# Then merge scraped + git-parsed data
python pipeline.py --docs ../giit/docs --from-db scrape.db --imdb --out isb_merged.csv
```

The scraper adds a 1–2.5 second polite delay between requests and uses SQLite
for crash-safe resumption. At 1.5s average, scraping all 25k pages takes ~10 hours.

---

## Running tests

```bash
cd project
python test_pipeline.py               # all tests
python test_pipeline.py TestParser    # just parser tests
```

---

## IMDb matching notes

- **Films** are matched by title + year (±2 years). Fuzzy threshold: 82%.
- **Persons** are matched by name. Fuzzy threshold: 78% (lower because transliteration varies widely).
- Results are cached in `imdb_cache.json`. Delete the cache to re-query.
- Songs where no confident match is found get `None` in the IMDb columns — better than a wrong ID.
- The cache is saved as a checkpoint every 100 film lookups so you can restart safely.

---

## License / attribution

- ISB source data: [giitaayan.com](https://www.giitaayan.com) / [v9y/giit](https://github.com/v9y/giit) — for private study, scholarship, and research only (see ISB Notice in the repo).
- IMDb data: © IMDb.com — non-commercial use only.
