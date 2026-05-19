"""TDD tests for Phase 4 Step 3 — ``_load_season_rounds_catalog``.

Replaces the chicken-and-egg ``_load_round_numbers`` helper that
reads from ``event_round_info`` (chicken: needs events) to drive
round fetches (egg: events come from round fetches). The new
helper reads from ``season_round`` which is the canonical mirror
of Sofascore's ``/season/{s}/rounds`` payload — populated long
before any per-round event fetch.

Return shape: tuple of ``(round_number, round_slug)`` pairs sorted
by ``round_number``. ``round_slug`` is None for group-stage rounds
(Sofascore returns ``{"round": N}`` without slug for those) and a
slug string for knockout rounds (e.g. ``"final"``,
``"quarterfinals"``).
"""

from __future__ import annotations

import unittest


class LoadSeasonRoundsCatalogContractTests(unittest.IsolatedAsyncioTestCase):
    async def test_filters_by_unique_tournament_and_season(self) -> None:
        from schema_inspector.default_tournaments_pipeline_cli import (
            _load_season_rounds_catalog,
        )

        captured: dict[str, object] = {}

        class _StubConn:
            async def fetch(self, query: str, *args: object):
                captured["query"] = query
                captured["args"] = args
                return []

        await _load_season_rounds_catalog(
            _StubConn(),
            unique_tournament_id=16,
            season_id=58210,
        )

        self.assertEqual(captured["args"], (16, 58210))
        query = str(captured["query"])
        self.assertIn("season_round", query)
        self.assertIn("unique_tournament_id = $1", query)
        self.assertIn("season_id = $2", query)

    async def test_query_selects_round_number_and_slug(self) -> None:
        from schema_inspector.default_tournaments_pipeline_cli import (
            _load_season_rounds_catalog,
        )

        captured: dict[str, object] = {}

        class _StubConn:
            async def fetch(self, query: str, *args: object):
                captured["query"] = query
                return []

        await _load_season_rounds_catalog(
            _StubConn(),
            unique_tournament_id=1,
            season_id=1,
        )
        query = str(captured["query"])
        self.assertIn("round_number", query)
        self.assertIn("round_slug", query)

    async def test_query_orders_by_round_number_ascending(self) -> None:
        from schema_inspector.default_tournaments_pipeline_cli import (
            _load_season_rounds_catalog,
        )

        captured: dict[str, object] = {}

        class _StubConn:
            async def fetch(self, query: str, *args: object):
                captured["query"] = query
                return []

        await _load_season_rounds_catalog(
            _StubConn(),
            unique_tournament_id=1,
            season_id=1,
        )
        query = str(captured["query"])
        self.assertIn("ORDER BY round_number", query)
        # Default ASC — no DESC keyword.
        self.assertNotRegex(query, r"ORDER BY.*DESC")

    async def test_returns_pairs_sorted_with_null_slug_first(self) -> None:
        """For a season with group + knockout rounds (WC pattern),
        catalog returns ``(round_number, slug)`` tuples. Group-stage
        slugs are None; knockout slugs are strings."""
        from schema_inspector.default_tournaments_pipeline_cli import (
            _load_season_rounds_catalog,
        )

        class _StubConn:
            async def fetch(self, query: str, *args: object):
                # Mirror real WC-2022 season_round contents (sorted ASC
                # by round_number, as the SQL does).
                return [
                    {"round_number": 1, "round_slug": None},
                    {"round_number": 2, "round_slug": None},
                    {"round_number": 3, "round_slug": None},
                    {"round_number": 5, "round_slug": "round-of-16"},
                    {"round_number": 27, "round_slug": "quarterfinals"},
                    {"round_number": 28, "round_slug": "semifinals"},
                    {"round_number": 29, "round_slug": "final"},
                    {"round_number": 50, "round_slug": "match-for-3rd-place"},
                ]

        catalog = await _load_season_rounds_catalog(
            _StubConn(),
            unique_tournament_id=16,
            season_id=41087,
        )

        self.assertEqual(len(catalog), 8)
        self.assertEqual(catalog[0], (1, None))
        self.assertEqual(catalog[1], (2, None))
        self.assertEqual(catalog[2], (3, None))
        self.assertEqual(catalog[3], (5, "round-of-16"))
        self.assertEqual(catalog[6], (29, "final"))
        self.assertEqual(catalog[7], (50, "match-for-3rd-place"))

    async def test_empty_string_slug_normalized_to_none(self) -> None:
        """Sofascore sometimes returns ``""`` instead of NULL for group
        rounds. The helper normalizes both to ``None`` so the
        orchestrator's "if slug is None" branch works uniformly."""
        from schema_inspector.default_tournaments_pipeline_cli import (
            _load_season_rounds_catalog,
        )

        class _StubConn:
            async def fetch(self, query: str, *args: object):
                return [
                    {"round_number": 1, "round_slug": ""},
                    {"round_number": 2, "round_slug": None},
                    {"round_number": 3, "round_slug": "   "},  # whitespace
                ]

        catalog = await _load_season_rounds_catalog(
            _StubConn(),
            unique_tournament_id=1,
            season_id=1,
        )
        self.assertEqual(catalog[0], (1, None))
        self.assertEqual(catalog[1], (2, None))
        self.assertEqual(catalog[2], (3, None))


if __name__ == "__main__":
    unittest.main()
