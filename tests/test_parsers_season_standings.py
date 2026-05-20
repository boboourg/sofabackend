"""TDD tests for Stage 3.2 (2026-05-20 historical layer):
``SeasonStandingsParser``.

The classifier already returns ``"season_standings"`` for
``…/standings/{scope}`` patterns (parsers/classifier.py:87-88), but until
this commit there was no parser registered for it — every season
standings snapshot landed in ``api_payload_snapshot`` and was dropped on
the floor of the stream-flow path.

This parser extracts the champion (rows[0] of the ``total`` standings)
and emits a ``unique_tournament_season_champion`` metric row consumed by
``NormalizeRepository._persist_unique_tournament_season_champion``
(added in Stage 3.3).

The standings + standing_row themselves are still loaded via the batch
``standings_repository.py`` stack (see audit B, section 5). We
deliberately do NOT duplicate that path here — this parser is
narrow-purpose: capture the champion so historic season-by-season title
chains are queryable from the canonical relational schema.
"""

from __future__ import annotations

import unittest

from schema_inspector.parsers.base import RawSnapshot
from schema_inspector.parsers.families.season_standings import SeasonStandingsParser


def _snapshot(payload: object, *, ut_id: int = 7, season_id: int = 41897) -> RawSnapshot:
    return RawSnapshot(
        snapshot_id=42,
        endpoint_pattern="/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/standings/{scope}",
        sport_slug="football",
        source_url=f"https://www.sofascore.com/api/v1/unique-tournament/{ut_id}/season/{season_id}/standings/total",
        resolved_url=None,
        envelope_key="ut-season-standings",
        http_status=200,
        payload=payload,
        fetched_at="2025-05-25T22:00:00+00:00",
        context_unique_tournament_id=ut_id,
        context_season_id=season_id,
    )


_TOTAL_STANDINGS_PAYLOAD = {
    "standings": [
        {
            "id": 555000,
            "tournament": {"id": 1},
            "type": "total",
            "name": "Premier League",
            "rows": [
                {
                    "team": {"id": 17, "name": "Manchester City"},
                    "position": 1,
                    "points": 91,
                    "matches": 38,
                },
                {
                    "team": {"id": 38, "name": "Arsenal"},
                    "position": 2,
                    "points": 89,
                    "matches": 38,
                },
            ],
        }
    ]
}


class SeasonStandingsParserTests(unittest.TestCase):
    def test_parser_family_is_season_standings(self) -> None:
        parser = SeasonStandingsParser()
        self.assertEqual(parser.parser_family, "season_standings")

    def test_total_standings_extracts_champion_from_row_position_1(self) -> None:
        parser = SeasonStandingsParser()
        result = parser.parse(_snapshot(_TOTAL_STANDINGS_PAYLOAD))

        self.assertEqual(result.parser_family, "season_standings")
        champion_rows = result.metric_rows.get("unique_tournament_season_champion", ())
        self.assertEqual(
            len(champion_rows), 1,
            msg="Expected exactly one champion row (position 1)",
        )
        champion = champion_rows[0]
        self.assertEqual(champion["unique_tournament_id"], 7)
        self.assertEqual(champion["season_id"], 41897)
        self.assertEqual(champion["team_id"], 17)
        self.assertEqual(champion["ordinal"], 1)
        self.assertEqual(
            champion["source"], "standings",
            msg=(
                "source='standings' lets readers distinguish live-flow "
                "rows from offline standings_backfill derivation rows."
            ),
        )

    def test_non_total_standings_scope_emits_no_champion(self) -> None:
        """``/standings/home`` and ``/standings/away`` are subset views —
        the champion is only well-defined on the ``total`` aggregate."""
        parser = SeasonStandingsParser()
        home_only = {
            "standings": [
                {
                    "id": 555001,
                    "type": "home",
                    "rows": [
                        {"team": {"id": 99}, "position": 1, "points": 50},
                    ],
                }
            ]
        }
        result = parser.parse(_snapshot(home_only))
        self.assertEqual(result.metric_rows.get("unique_tournament_season_champion", ()), ())

    def test_missing_context_yields_empty_result(self) -> None:
        """Without ``context_unique_tournament_id`` + ``context_season_id``
        we have nothing to key the champion row by — must yield empty
        (not raise, not emit a malformed row)."""
        parser = SeasonStandingsParser()
        snap = RawSnapshot(
            snapshot_id=43,
            endpoint_pattern="/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/standings/{scope}",
            sport_slug="football",
            source_url="x",
            resolved_url=None,
            envelope_key="x",
            http_status=200,
            payload=_TOTAL_STANDINGS_PAYLOAD,
            fetched_at=None,
            context_unique_tournament_id=None,
            context_season_id=None,
        )
        result = parser.parse(snap)
        self.assertEqual(result.metric_rows.get("unique_tournament_season_champion", ()), ())

    def test_empty_standings_array_yields_empty_result(self) -> None:
        parser = SeasonStandingsParser()
        result = parser.parse(_snapshot({"standings": []}))
        self.assertEqual(result.metric_rows.get("unique_tournament_season_champion", ()), ())

    def test_missing_team_id_in_first_row_is_skipped(self) -> None:
        parser = SeasonStandingsParser()
        broken = {
            "standings": [
                {
                    "type": "total",
                    "rows": [
                        {"team": {}, "position": 1, "points": 50},  # team.id absent
                    ],
                }
            ]
        }
        result = parser.parse(_snapshot(broken))
        self.assertEqual(result.metric_rows.get("unique_tournament_season_champion", ()), ())


class SeasonStandingsParserRegistrationTests(unittest.TestCase):
    def test_parser_is_registered_in_default_registry(self) -> None:
        """Until now classifier emitted 'season_standings' but the
        registry had no entry → PARSE_STATUS_UNSUPPORTED on every
        standings snapshot. This test pins that the registration is
        in place so the family is actually consumed."""
        from schema_inspector.parsers.registry import ParserRegistry

        registry = ParserRegistry.default()
        parser = registry.parsers.get("season_standings")
        self.assertIsNotNone(
            parser,
            msg=(
                "ParserRegistry.default() must register SeasonStandingsParser "
                "under the 'season_standings' family so the classifier "
                "result actually reaches a parser instead of falling "
                "into PARSE_STATUS_UNSUPPORTED."
            ),
        )
        self.assertEqual(parser.parser_family, "season_standings")


# ---------------------------------------------------------------------------
# Stage 3.3 — persist behaviour for unique_tournament_season_champion.
# Stage 1 patterns applied:
#   * id-sort before executemany (anti-deadlock)
#   * IS DISTINCT FROM guard inside ON CONFLICT DO UPDATE (anti-bloat)
# ---------------------------------------------------------------------------


class _CapturingExecutor:
    def __init__(self) -> None:
        self.executemany_calls: list[tuple[str, list[tuple[object, ...]]]] = []

    async def execute(self, query: str, *args: object) -> str:
        return "OK"

    async def executemany(self, query: str, rows: list[tuple[object, ...]]) -> str:
        self.executemany_calls.append((query, rows))
        return "OK"

    async def fetch(self, query: str, *args: object):
        return []


class UniqueTournamentSeasonChampionPersistTests(unittest.IsolatedAsyncioTestCase):
    async def test_persist_emits_sorted_rows_with_is_distinct_from_guard(self) -> None:
        from schema_inspector.storage.normalize_repository import NormalizeRepository

        repository = NormalizeRepository()
        executor = _CapturingExecutor()
        await repository._persist_unique_tournament_season_champion(
            executor,
            rows=(
                {"unique_tournament_id": 7, "season_id": 41897, "team_id": 17, "ordinal": 1, "source": "standings"},
                {"unique_tournament_id": 7, "season_id": 35777, "team_id": 38, "ordinal": 1, "source": "standings"},
                {"unique_tournament_id": 39, "season_id": 50000, "team_id": 100, "ordinal": 1, "source": "standings"},
            ),
        )

        # Single INSERT batch.
        inserts = [
            (sql, rows)
            for sql, rows in executor.executemany_calls
            if "INSERT INTO unique_tournament_season_champion" in sql
        ]
        self.assertEqual(len(inserts), 1, msg="Expected exactly one executemany call")
        sql, rows = inserts[0]

        # Stage 1.2 — IS DISTINCT FROM guard.
        self.assertIn("IS DISTINCT FROM", sql)

        # Stage 1.1 — rows sorted by (ut, season).
        sorted_keys = [(row[0], row[1]) for row in rows]
        self.assertEqual(
            sorted_keys,
            sorted(sorted_keys),
            msg=(
                "Rows must be sorted by (unique_tournament_id, season_id) "
                "before executemany to ensure two parallel transactions "
                "take row-locks in canonical order — same Stage 1.1 "
                "anti-deadlock discipline as player/manager upserts."
            ),
        )

    async def test_persist_emits_no_call_when_rows_empty(self) -> None:
        from schema_inspector.storage.normalize_repository import NormalizeRepository

        repository = NormalizeRepository()
        executor = _CapturingExecutor()
        await repository._persist_unique_tournament_season_champion(executor, rows=())
        self.assertEqual(executor.executemany_calls, [])


if __name__ == "__main__":
    unittest.main()
