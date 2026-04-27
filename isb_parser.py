"""
isb_parser.py
-------------
Parses all ITRANS Song Book (.isb.txt) source files from the v9y/giit repository
into clean Python dicts, ready for CSV export or IMDb enrichment.

ISB file format (from TEMPLATE.md):
    \stitle{...}     song title in ITRANS
    \film{...}       film name
    \year{...}       release year
    \starring{...}   cast (comma-separated)
    \singer{...}     singer(s) (comma-separated)
    \music{...}      music director(s)
    \lyrics{...}     lyricist(s)
    #indian ... #endindian   raw lyrics in ITRANS/HiTrans notation

Usage:
    from isb_parser import parse_all_files
    records = parse_all_files("path/to/giit/docs")
"""

import glob
import os
import re
import pathlib
from typing import Optional


# ── Field prefix constants ────────────────────────────────────────────────────

FIELD_MAP = {
    r"\stitle":    "stitle",
    "\\film":      "film",
    r"\year":      "year",
    r"\starring":  "starring",
    r"\singer":    "singer",
    r"\music":     "music",
    r"\lyrics":    "lyricist",
}

IGNORE_PREFIXES = (
    "%", "#", r"\starring", r"\printtitle", r"\startsong", r"\endsong",
)


# ── Core extraction helpers ───────────────────────────────────────────────────

def _extract_braces(line: str) -> str:
    """Pull text from inside the first {...} pair on a line."""
    m = re.search(r'\{(.*?)\}', line)
    return m.group(1).strip() if m else ""


def _is_comment_or_directive(line: str) -> bool:
    return any(line.startswith(p) for p in IGNORE_PREFIXES)


def _clean_itrans(text: str) -> str:
    """
    Light normalisation of ITRANS-notation lyrics:
    - strip leading/trailing whitespace per line
    - collapse multiple blank lines into one
    - remove inline comment markers (\threedots, \- 2, etc.)
    """
    text = re.sub(r'threedots', '...', text)
    text = re.sub(r'\-\s*\d+', '', text)        # \- 2  (repeat markers)
    text = re.sub(r'##([^#]*)##', r'\1', text)    # ##chorus## → chorus
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()


# ── Single-file parser ────────────────────────────────────────────────────────

def parse_file(filepath: str) -> Optional[dict]:
    """
    Parse one .isb.txt file and return a dict of fields, or None on failure.

    Returned keys:
        isb_id, stitle, film, year, starring, singer, music, lyricist,
        lyrics_itrans, source_file
    """
    try:
        with open(filepath, encoding="utf-8", errors="ignore") as f:
            lines = f.readlines()
    except OSError:
        return None

    # Derive ISB numeric ID from filename  e.g. "1234.isb.txt" → 1234
    fname = pathlib.Path(filepath).name
    isb_id_match = re.match(r'^(\d+)\.isb\.txt$', fname)
    isb_id = int(isb_id_match.group(1)) if isb_id_match else None

    record = {
        "isb_id":       isb_id,
        "stitle":       "",
        "film":         "",
        "year":         None,
        "starring":     "",
        "singer":       "",
        "music":        "",
        "lyricist":     "",
        "lyrics_itrans": "",
        "source_file":  fname,
    }

    in_lyrics = False
    lyrics_lines = []

    for raw in lines:
        line = raw.rstrip("\n").rstrip()

        # ── Lyrics block ──────────────────────────────────────────────────
        if line.startswith("#indian"):
            in_lyrics = True
            continue
        if line.startswith("#endindian"):
            in_lyrics = False
            continue
        if in_lyrics:
            if not line.startswith("%"):          # skip inline comments
                lyrics_lines.append(line)
            continue

        # ── Skip comment / directive lines ────────────────────────────────
        if _is_comment_or_directive(line):
            continue

        # ── Metadata fields ───────────────────────────────────────────────
        for prefix, key in FIELD_MAP.items():
            if line.startswith(prefix):
                value = _extract_braces(line)
                if key == "year":
                    record["year"] = int(value) if value.isdigit() else None
                else:
                    record[key] = value
                break

    record["lyrics_itrans"] = _clean_itrans("\n".join(lyrics_lines))
    # Strip ## chorus markers from stitle (e.g. "##Dream Girl##" → "Dream Girl")
    record["stitle"] = re.sub(r"##([^#]*)##", r"\1", record["stitle"]).strip("#").strip()
    return record


# ── Batch parser ──────────────────────────────────────────────────────────────

def parse_all_files(docs_dir: str, verbose: bool = True) -> list[dict]:
    """
    Parse every *.isb.txt file in docs_dir.

    Returns a list of record dicts, sorted by isb_id.
    """
    pattern = os.path.join(docs_dir, "*.isb.txt")
    files = sorted(glob.glob(pattern), key=lambda p: int(
        re.match(r'(\d+)', pathlib.Path(p).name).group(1)
    ) if re.match(r'(\d+)', pathlib.Path(p).name) else 0)

    if not files:
        raise FileNotFoundError(f"No .isb.txt files found in: {docs_dir}")

    records = []
    failed = []

    for i, fp in enumerate(files):
        rec = parse_file(fp)
        if rec is None:
            failed.append(fp)
        else:
            records.append(rec)

        if verbose and (i + 1) % 1000 == 0:
            print(f"  Parsed {i+1:,} / {len(files):,} files …")

    if verbose:
        print(f"\n✓ Parsed {len(records):,} songs  ({len(failed)} failures)")

    return records


# ── Quick sanity test ─────────────────────────────────────────────────────────

if __name__ == "__main__":
    import json, sys

    docs = sys.argv[1] if len(sys.argv) > 1 else "../giit/docs"
    recs = parse_all_files(docs)
    print(json.dumps(recs[0], indent=2, default=str))
    print(f"\nTotal records: {len(recs):,}")
