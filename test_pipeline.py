"""
test_pipeline.py
----------------
Unit + integration tests for the ISB pipeline.

Run:
    python test_pipeline.py                    # all tests
    python test_pipeline.py TestParser         # just parser tests
"""

import sys
import os
import unittest
import tempfile
import json
import textwrap

# Make sure imports work from the project dir
sys.path.insert(0, os.path.dirname(__file__))
from isb_parser import parse_file, parse_all_files, _extract_braces, _clean_itrans


# ── Fixtures ──────────────────────────────────────────────────────────────────

SAMPLE_ISB_FULL = textwrap.dedent("""\
    %
    \\startsong
    \\stitle{##Dream Girl##}%
    \\film{Dream Girl}%
    \\year{1977}%
    \\starring{Ashok Kumar, Dharmendra, Hema Malini}%
    \\singer{Kishore Kumar}%
    \\music{Laxmikant-Pyarelal}%
    \\lyrics{Anand Bakshi}%
    %
    % Contributor: Prabhakar
    % Editor: Someone
    %
    \\printtitle
    #indian
    %
    ##Dream-girl, dream-girl##
    kisI shaayar kI Gazal, ##dream-girl##
    kabhI to milegI, kahI.n to milegI
    %
    #endindian
    \\endsong
""")

SAMPLE_ISB_MINIMAL = textwrap.dedent("""\
    \\startsong
    \\stitle{ek pyaar ka naGmaa hai}%
    \\film{Shor}%
    \\year{1972}%
    \\singer{Lata Mangeshkar, Mukesh}%
    \\music{S D Burman}%
    \\lyrics{Santosh Anand}%
    #indian
    ek pyaar ka naGmaa hai
    mo.Do.n ka ek silsilaa hai
    #endindian
    \\endsong
""")

SAMPLE_ISB_BAD_YEAR = textwrap.dedent("""\
    \\startsong
    \\stitle{test song}%
    \\film{Test Film}%
    \\year{unknown}%
    \\singer{Someone}%
    \\music{Someone Else}%
    \\lyrics{Writer}%
    #indian
    la la la
    #endindian
    \\endsong
""")


# ── Parser tests ──────────────────────────────────────────────────────────────

class TestHelpers(unittest.TestCase):

    def test_extract_braces_normal(self):
        self.assertEqual(_extract_braces(r"\film{Dream Girl}%"), "Dream Girl")

    def test_extract_braces_empty(self):
        self.assertEqual(_extract_braces(r"\film{}%"), "")

    def test_extract_braces_no_braces(self):
        self.assertEqual(_extract_braces("% comment line"), "")

    def test_clean_itrans_threedots(self):
        result = _clean_itrans("line one \\threedots\nline two")
        self.assertIn("...", result)
        self.assertNotIn("\\threedots", result)

    def test_clean_itrans_hash_markers(self):
        result = _clean_itrans("##chorus line##")
        self.assertEqual(result, "chorus line")

    def test_clean_itrans_collapse_blank_lines(self):
        result = _clean_itrans("a\n\n\n\nb")
        self.assertNotIn("\n\n\n", result)


class TestParser(unittest.TestCase):

    def _write_tmp(self, content: str, isb_id: int = 42) -> str:
        d = tempfile.mkdtemp()
        p = os.path.join(d, f"{isb_id}.isb.txt")
        with open(p, "w", encoding="utf-8") as f:
            f.write(content)
        return p

    def test_full_record(self):
        path = self._write_tmp(SAMPLE_ISB_FULL, isb_id=99)
        rec = parse_file(path)
        self.assertIsNotNone(rec)
        self.assertEqual(rec["isb_id"], 99)
        self.assertEqual(rec["film"], "Dream Girl")
        self.assertEqual(rec["year"], 1977)
        self.assertEqual(rec["singer"], "Kishore Kumar")
        self.assertEqual(rec["music"], "Laxmikant-Pyarelal")
        self.assertEqual(rec["lyricist"], "Anand Bakshi")
        self.assertIn("dream-girl", rec["lyrics_itrans"])
        # ##markers## should be stripped
        self.assertNotIn("##", rec["lyrics_itrans"])

    def test_minimal_record(self):
        path = self._write_tmp(SAMPLE_ISB_MINIMAL, isb_id=7)
        rec = parse_file(path)
        self.assertIsNotNone(rec)
        self.assertEqual(rec["film"], "Shor")
        self.assertEqual(rec["year"], 1972)
        self.assertIn("ek pyaar", rec["lyrics_itrans"])

    def test_bad_year_becomes_none(self):
        path = self._write_tmp(SAMPLE_ISB_BAD_YEAR, isb_id=5)
        rec = parse_file(path)
        self.assertIsNone(rec["year"])

    def test_isb_id_from_filename(self):
        path = self._write_tmp(SAMPLE_ISB_MINIMAL, isb_id=12345)
        rec = parse_file(path)
        self.assertEqual(rec["isb_id"], 12345)

    def test_comments_excluded_from_lyrics(self):
        path = self._write_tmp(SAMPLE_ISB_FULL, isb_id=1)
        rec = parse_file(path)
        self.assertNotIn("Contributor:", rec["lyrics_itrans"])
        self.assertNotIn("Editor:", rec["lyrics_itrans"])

    def test_parse_all_files(self):
        d = tempfile.mkdtemp()
        for i, content in [(1, SAMPLE_ISB_FULL), (2, SAMPLE_ISB_MINIMAL)]:
            with open(os.path.join(d, f"{i}.isb.txt"), "w", encoding="utf-8") as f:
                f.write(content)

        recs = parse_all_files(d, verbose=False)
        self.assertEqual(len(recs), 2)
        # Should be sorted by isb_id
        self.assertEqual(recs[0]["isb_id"], 1)
        self.assertEqual(recs[1]["isb_id"], 2)

    def test_parse_all_files_empty_dir(self):
        d = tempfile.mkdtemp()
        with self.assertRaises(FileNotFoundError):
            parse_all_files(d, verbose=False)


# ── Fuzzy matching tests (rapidfuzz) ─────────────────────────────────────────

class TestFuzzyMatch(unittest.TestCase):

    def test_normalise_import(self):
        from imdb_enricher import _normalise
        self.assertEqual(_normalise("Mughal-E-Azam"), "mughal e azam")

    def test_best_match_exact(self):
        from imdb_enricher import _best_match
        idx = _best_match("Sholay", ["Devdas", "Sholay", "Mother India"])
        self.assertEqual(idx, 1)

    def test_best_match_transliteration(self):
        from imdb_enricher import _best_match
        # "Pyaasa" vs "Pyaasaa" should still match
        idx = _best_match("Pyaasaa", ["Pyaasa", "Devdas", "Guide"])
        self.assertEqual(idx, 0)

    def test_best_match_below_threshold(self):
        from imdb_enricher import _best_match
        idx = _best_match("Completely Different Title", ["Sholay", "Devdas"], threshold=90)
        self.assertIsNone(idx)


# ── IMDb enricher cache tests ─────────────────────────────────────────────────

class TestIMDbEnricherCache(unittest.TestCase):

    def setUp(self):
        self.tmp = tempfile.mktemp(suffix=".json")

    def tearDown(self):
        if os.path.exists(self.tmp):
            os.remove(self.tmp)

    def test_cache_round_trip(self):
        from imdb_enricher import IMDbEnricher
        e = IMDbEnricher(cache_path=self.tmp)
        e._cache["films"]["Sholay|1975"] = "tt0073708"
        e._cache["persons"]["Lata Mangeshkar"] = "nm0483758"
        e.save_cache()

        e2 = IMDbEnricher(cache_path=self.tmp)
        self.assertEqual(e2._cache["films"]["Sholay|1975"], "tt0073708")
        self.assertEqual(e2._cache["persons"]["Lata Mangeshkar"], "nm0483758")

    def test_resolve_film_cache_hit(self):
        from imdb_enricher import IMDbEnricher
        e = IMDbEnricher(cache_path=self.tmp)
        e._cache["films"]["Devdas|1955"] = "tt0047956"
        result = e.resolve_film("Devdas", year=1955)
        self.assertEqual(result, "tt0047956")

    def test_resolve_person_cache_hit(self):
        from imdb_enricher import IMDbEnricher
        e = IMDbEnricher(cache_path=self.tmp)
        e._cache["persons"]["Kishore Kumar"] = "nm0356691"
        result = e.resolve_person("Kishore Kumar")
        self.assertEqual(result, "nm0356691")

    def test_resolve_persons_multi(self):
        from imdb_enricher import IMDbEnricher
        e = IMDbEnricher(cache_path=self.tmp)
        e._cache["persons"]["Lata Mangeshkar"] = "nm0483758"
        e._cache["persons"]["Asha Bhosle"] = "nm0080372"
        results = e.resolve_persons("Lata Mangeshkar, Asha Bhosle")
        self.assertEqual(results, ["nm0483758", "nm0080372"])

    def test_enrich_records_adds_columns(self):
        from imdb_enricher import IMDbEnricher
        e = IMDbEnricher(cache_path=self.tmp)
        # Pre-populate cache so no live calls are made
        e._cache["films"]["Sholay|1975"] = "tt0073708"
        e._cache["persons"]["Lata Mangeshkar"] = "nm0483758"

        records = [{
            "isb_id": 1,
            "film": "Sholay",
            "year": 1975,
            "singer": "Lata Mangeshkar",
            "music": "R.D. Burman",
            "lyricist": "Anand Bakshi",
        }]
        enriched = e.enrich_records(records)
        self.assertEqual(enriched[0]["imdb_film_id"], "tt0073708")
        self.assertEqual(enriched[0]["imdb_singer_ids"], "nm0483758")


# ── Run ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()

    # Run specific class if passed as CLI arg
    if len(sys.argv) > 1:
        cls_name = sys.argv.pop(1)
        cls_map = {
            "TestHelpers": TestHelpers,
            "TestParser": TestParser,
            "TestFuzzyMatch": TestFuzzyMatch,
            "TestIMDbEnricherCache": TestIMDbEnricherCache,
        }
        suite.addTests(loader.loadTestsFromTestCase(cls_map[cls_name]))
    else:
        suite.addTests(loader.loadTestsFromTestCase(TestHelpers))
        suite.addTests(loader.loadTestsFromTestCase(TestParser))
        suite.addTests(loader.loadTestsFromTestCase(TestFuzzyMatch))
        suite.addTests(loader.loadTestsFromTestCase(TestIMDbEnricherCache))

    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    sys.exit(0 if result.wasSuccessful() else 1)
