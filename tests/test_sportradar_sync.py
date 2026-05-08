"""Tests for sportradar_sync (P3.2 reconciliation)."""

from __future__ import annotations

import asyncio
import unittest
from pathlib import Path

from schema_inspector.services.sportradar_sync import (
    XlsxRow,
    _fuzz_ratio,
    _normalize_name,
    load_manual_overrides,
    reconcile_row,
)


class _FakeConn:
    """In-memory fake matching SqlExecutor protocol."""

    def __init__(self) -> None:
        self.unique_tournaments: list[dict] = []
        self.executes: list[tuple] = []

    async def fetchrow(self, sql: str, *args):
        if "WHERE ut.id = $1" in sql:
            ut_id = int(args[0])
            for row in self.unique_tournaments:
                if row["ut_id"] == ut_id:
                    return row
            return None
        return None

    async def fetch(self, sql: str, *args):
        # Name match: filter by sport + category
        if "lower(c.name) = lower($2)" in sql:
            sport_slug = args[0]
            category = args[1]
            return [
                r for r in self.unique_tournaments
                if r.get("sport_slug") == sport_slug
                and r.get("category_name", "").lower() == str(category).lower()
            ]
        return []

    async def execute(self, sql: str, *args):
        self.executes.append((sql, args))


class _FakeDatabase:
    def __init__(self, conn: _FakeConn) -> None:
        self._conn = conn

    def connection(self):
        return _AsyncCM(self._conn)


class _AsyncCM:
    def __init__(self, value):
        self._value = value

    async def __aenter__(self):
        return self._value

    async def __aexit__(self, *_):
        return None


class NameNormalizationTests(unittest.TestCase):
    def test_strips_diacritics(self) -> None:
        self.assertEqual(_normalize_name("Brasileirão"), "brasileirao")

    def test_lowercases(self) -> None:
        self.assertEqual(_normalize_name("Premier League"), "premier league")

    def test_removes_filler_words(self) -> None:
        self.assertIn("liverpool", _normalize_name("Liverpool FC"))
        # Apostrophe stripped; "King's Cup" → "king s cup" — still fuzzy-matches well
        normalized = _normalize_name("King's Cup")
        self.assertIn("king", normalized)
        self.assertIn("cup", normalized)
        self.assertNotIn("fc", _normalize_name("Liverpool FC").split())

    def test_fuzz_ratio_identical_names(self) -> None:
        self.assertEqual(_fuzz_ratio("Premier League", "Premier League"), 1.0)

    def test_fuzz_ratio_diacritic_difference(self) -> None:
        # Brasileirão vs Brasileiro Serie A → similar but not identical
        score = _fuzz_ratio("Brasileirão", "Brasileiro Serie A")
        self.assertGreater(score, 0.4)

    def test_fuzz_ratio_unrelated_names_low(self) -> None:
        score = _fuzz_ratio("Premier League", "Bundesliga")
        self.assertLess(score, 0.5)


class ReconcileRowTests(unittest.TestCase):
    def test_manual_override_takes_priority(self) -> None:
        conn = _FakeConn()
        # No DB rows at all
        result = asyncio.run(reconcile_row(
            executor=conn,
            xlsx_row=XlsxRow(
                sr_competition_id=2110,
                sr_competition_name="Kings Cup",
                sr_category_name="Saudi Arabia",
                coverage_attrs={"Live": 2},
            ),
            sport_slug="football",
            manual_overrides={2110: 2058},
        ))
        self.assertEqual(result.method, "manual")
        self.assertEqual(result.confidence, 1.0)
        self.assertEqual(result.unique_tournament_id, 2058)

    def test_blind_id_match_same_sport_and_name(self) -> None:
        conn = _FakeConn()
        conn.unique_tournaments.append({
            "ut_id": 17, "ut_name": "Premier League",
            "sport_slug": "football", "category_name": "England",
        })
        result = asyncio.run(reconcile_row(
            executor=conn,
            xlsx_row=XlsxRow(
                sr_competition_id=17,
                sr_competition_name="Premier League",
                sr_category_name="England",
                coverage_attrs={"Live": 2},
            ),
            sport_slug="football",
            manual_overrides={},
        ))
        self.assertEqual(result.method, "blind_id")
        self.assertEqual(result.confidence, 1.0)
        self.assertEqual(result.unique_tournament_id, 17)

    def test_blind_id_rejected_different_sport(self) -> None:
        conn = _FakeConn()
        # ID 2462 in Sofascore is tennis — should NOT blind-match SR football
        conn.unique_tournaments.append({
            "ut_id": 2462, "ut_name": "Miami, Doubles",
            "sport_slug": "tennis", "category_name": "ATP",
        })
        result = asyncio.run(reconcile_row(
            executor=conn,
            xlsx_row=XlsxRow(
                sr_competition_id=2462,
                sr_competition_name="King of Bahrain Cup",
                sr_category_name="Bahrain",
                coverage_attrs={},
            ),
            sport_slug="football",
            manual_overrides={},
        ))
        # Different sport → blind ID rejected, no name candidates → unmatched
        self.assertEqual(result.method, "unmatched")
        self.assertEqual(result.confidence, 0.0)
        self.assertIsNone(result.unique_tournament_id)

    def test_blind_id_rejected_when_name_unrelated(self) -> None:
        conn = _FakeConn()
        # ID 2110 collision case — same sport but completely different name
        conn.unique_tournaments.append({
            "ut_id": 2110, "ut_name": "Division 3 Nordöstra Götaland",
            "sport_slug": "football", "category_name": "Sweden Amateur",
        })
        result = asyncio.run(reconcile_row(
            executor=conn,
            xlsx_row=XlsxRow(
                sr_competition_id=2110,
                sr_competition_name="Kings Cup",
                sr_category_name="Saudi Arabia",
                coverage_attrs={},
            ),
            sport_slug="football",
            manual_overrides={},
        ))
        # blind ID name fuzz < 0.7 → falls through to name_match (Saudi Arabia, no candidates) → unmatched
        self.assertEqual(result.method, "unmatched")

    def test_name_match_when_blind_id_fails(self) -> None:
        conn = _FakeConn()
        # SR id doesn't exist locally; but a UT in same country + similar name does.
        conn.unique_tournaments.append({
            "ut_id": 999, "ut_name": "Premier League",
            "sport_slug": "football", "category_name": "England",
        })
        result = asyncio.run(reconcile_row(
            executor=conn,
            xlsx_row=XlsxRow(
                sr_competition_id=12345,  # not 999, blind ID fails
                sr_competition_name="Premier League",
                sr_category_name="England",
                coverage_attrs={"Live": 2},
            ),
            sport_slug="football",
            manual_overrides={},
        ))
        self.assertEqual(result.method, "name_match")
        self.assertEqual(result.unique_tournament_id, 999)
        self.assertGreaterEqual(result.confidence, 0.85)

    def test_unmatched_when_no_candidates(self) -> None:
        conn = _FakeConn()
        result = asyncio.run(reconcile_row(
            executor=conn,
            xlsx_row=XlsxRow(
                sr_competition_id=99999,
                sr_competition_name="Mystery League",
                sr_category_name="Atlantis",
                coverage_attrs={},
            ),
            sport_slug="football",
            manual_overrides={},
        ))
        self.assertEqual(result.method, "unmatched")
        self.assertIsNone(result.unique_tournament_id)


class ManualOverridesTests(unittest.TestCase):
    def test_loads_real_config_file(self) -> None:
        # The actual config file in repo has 3 known collisions.
        path = Path(__file__).parent.parent / "config" / "sportradar_id_overrides.yaml"
        if not path.exists():
            self.skipTest("config not present in this checkout")
        overrides = load_manual_overrides(path)
        self.assertEqual(overrides[2110], 2058)
        self.assertEqual(overrides[2296], 2119)
        self.assertEqual(overrides[2462], 2736)

    def test_missing_file_returns_empty(self) -> None:
        result = load_manual_overrides(Path("/no/such/file.yaml"))
        self.assertEqual(result, {})


if __name__ == "__main__":
    unittest.main()
