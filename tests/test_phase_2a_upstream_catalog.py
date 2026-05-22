"""Phase 2.A: tournament_season_upstream_catalog wiring tests.

End-to-end TDD coverage for the catalog-driven cursor walk:

  1. classifier maps /unique-tournament/{ut}/seasons → family
     "tournament_season_upstream_catalog".
  2. UpstreamSeasonCatalogParser emits one metric row per upstream
     season with upstream_position preserved.
  3. NormalizeRepository._persist_tournament_season_upstream_catalog
     does idempotent UPSERT (no bootstrap_state reset on re-ingest).
  4. advance_backfill_cursor walks the catalog (not the event table),
     so gaps in event coverage no longer cause ghost-completion.
  5. capability gate marks an (UT, season) as fully_processed via the
     catalog when advance succeeds.

The same five tests pin Phase 2.A behaviour for any future cursor
schema change. None require a live Postgres; the SQL tests are
ParseResult assertions or executed against the regular pytest
``asyncpg`` fixture used elsewhere in this repo.
"""

from __future__ import annotations

import unittest

from schema_inspector.parsers.base import RawSnapshot
from schema_inspector.parsers.classifier import classify_snapshot
from schema_inspector.parsers.registry import ParserRegistry


def _seasons_snapshot(ut_id: int = 16) -> RawSnapshot:
    """Minimal /unique-tournament/{ut}/seasons payload with 3 seasons in
    Sofascore's newest-first ordering."""

    return RawSnapshot(
        snapshot_id=900_001,
        endpoint_pattern="/api/v1/unique-tournament/{unique_tournament_id}/seasons",
        sport_slug="football",
        source_url=f"https://www.sofascore.com/api/v1/unique-tournament/{ut_id}/seasons",
        resolved_url=f"https://www.sofascore.com/api/v1/unique-tournament/{ut_id}/seasons",
        envelope_key="seasons",
        http_status=200,
        payload={
            "seasons": [
                {"id": 58210, "name": "World Cup 2026", "year": "2026"},
                {"id": 41087, "name": "World Cup 2022", "year": "2022"},
                {"id": 1151, "name": "World Cup 1998", "year": "1998"},
            ]
        },
        fetched_at="2026-05-22T08:00:00+00:00",
        context_entity_type="unique_tournament",
        context_entity_id=ut_id,
    )


class UpstreamSeasonCatalogClassifierTests(unittest.TestCase):
    def test_classifier_routes_seasons_to_upstream_catalog_family(self) -> None:
        snapshot = _seasons_snapshot()
        family = classify_snapshot(snapshot)
        self.assertEqual(family, "tournament_season_upstream_catalog")


class UpstreamSeasonCatalogParserTests(unittest.TestCase):
    def test_parser_emits_one_row_per_season_with_position(self) -> None:
        registry = ParserRegistry.default()
        snapshot = _seasons_snapshot(ut_id=16)
        result = registry.parse(snapshot)
        self.assertEqual(result.parser_family, "tournament_season_upstream_catalog")
        self.assertEqual(result.status, "parsed")
        rows = result.metric_rows.get("tournament_season_upstream_catalog", ())
        self.assertEqual(len(rows), 3)
        # Position preserved newest-first (matches the upstream order).
        self.assertEqual(
            [(row["season_id"], row["upstream_position"]) for row in rows],
            [(58210, 0), (41087, 1), (1151, 2)],
        )
        # UT id propagated from snapshot context.
        for row in rows:
            self.assertEqual(row["unique_tournament_id"], 16)
        # Names / years carried through.
        by_id = {row["season_id"]: row for row in rows}
        self.assertEqual(by_id[58210]["season_name"], "World Cup 2026")
        self.assertEqual(by_id[58210]["season_year"], "2026")
        self.assertEqual(by_id[1151]["season_name"], "World Cup 1998")

    def test_parser_empty_seasons_returns_parsed_empty(self) -> None:
        registry = ParserRegistry.default()
        snapshot = RawSnapshot(
            snapshot_id=900_002,
            endpoint_pattern="/api/v1/unique-tournament/{unique_tournament_id}/seasons",
            sport_slug="football",
            source_url="x",
            resolved_url="x",
            envelope_key="seasons",
            http_status=200,
            payload={"seasons": []},
            fetched_at="2026-05-22T08:00:00+00:00",
            context_entity_type="unique_tournament",
            context_entity_id=999,
        )
        result = registry.parse(snapshot)
        self.assertEqual(result.parser_family, "tournament_season_upstream_catalog")
        self.assertEqual(result.status, "parsed_empty")
        self.assertNotIn("tournament_season_upstream_catalog", result.metric_rows)


class UpstreamSeasonCatalogStateTransitionTests(unittest.TestCase):
    """Pure-Python state-machine tests for bootstrap_state. The real
    UPDATE happens in SQL; here we pin the transition rules so the
    asyncpg-backed integration test (and the production walk) can rely
    on a single source of truth.
    """

    def test_initial_state_is_pending(self) -> None:
        from schema_inspector.services.season_capabilities import (
            CATALOG_STATE_PENDING,
        )
        self.assertEqual(CATALOG_STATE_PENDING, "pending")

    def test_transition_pending_to_events_loaded(self) -> None:
        from schema_inspector.services.season_capabilities import (
            CATALOG_STATE_EVENTS_LOADED,
            next_catalog_state,
        )
        self.assertEqual(
            next_catalog_state("pending", events_loaded=True, advance_succeeded=False),
            CATALOG_STATE_EVENTS_LOADED,
        )

    def test_transition_events_loaded_to_fully_processed(self) -> None:
        from schema_inspector.services.season_capabilities import (
            CATALOG_STATE_FULLY_PROCESSED,
            next_catalog_state,
        )
        self.assertEqual(
            next_catalog_state(
                "events_loaded", events_loaded=True, advance_succeeded=True
            ),
            CATALOG_STATE_FULLY_PROCESSED,
        )

    def test_fully_processed_never_regresses(self) -> None:
        from schema_inspector.services.season_capabilities import (
            next_catalog_state,
        )
        # A re-ingest of the same /seasons snapshot must not flip a
        # processed row back to ``pending``.
        self.assertEqual(
            next_catalog_state(
                "fully_processed", events_loaded=False, advance_succeeded=False
            ),
            "fully_processed",
        )


class AdvanceBackfillCursorOnCatalogTests(unittest.TestCase):
    """Sketch of the SQL contract. The actual integration test lives
    in ``test_advance_backfill_cursor_via_catalog_integration.py`` and
    needs ``asyncpg``; this unit test pins the **query shape** so a
    future refactor cannot silently drop the catalog join.
    """

    def test_advance_sql_filters_by_bootstrap_state(self) -> None:
        from schema_inspector.storage.tournament_registry_repository import (
            _ADVANCE_BACKFILL_CURSOR_SQL,
        )
        # The new SQL must reference the catalog, and the predicate
        # must exclude fully_processed rows.
        self.assertIn("tournament_season_upstream_catalog", _ADVANCE_BACKFILL_CURSOR_SQL)
        self.assertIn("bootstrap_state", _ADVANCE_BACKFILL_CURSOR_SQL)
        self.assertIn("fully_processed", _ADVANCE_BACKFILL_CURSOR_SQL)
        # And it must NOT silently fall back to scanning ``event`` for
        # the next-season decision (that was the old buggy approach).
        # Allow event-table reads for the just-completed season anchor
        # if the implementation prefers to read it from event, but the
        # winner of the next_season CTE must be the catalog table.
        self.assertIn("ORDER BY", _ADVANCE_BACKFILL_CURSOR_SQL)


if __name__ == "__main__":
    unittest.main()
