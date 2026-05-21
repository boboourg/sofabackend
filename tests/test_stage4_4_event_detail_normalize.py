"""Stage 4.4 (2026-05-21) — RED test for historical-backfill match-center.

Stage 4.2 added ``_fetch_incidents`` / ``_fetch_statistics`` to
``EventDetailParser.fetch_bundle`` so archive matches captured by the
``historical-backfill`` CLI would carry the raw payloads. The Stage 4.2
commit body promised the rest:

    Normalisation of goals/cards/substitutions into ``event_incident`` is
    done downstream by the standard ``EventIncidentsParser`` (already
    registered in ``ParserRegistry.default()``); ``EventDetailBackfillJob``
    invokes the registry on each captured snapshot so the row lands in
    the relational schema even for archive matches.

…but the wire was never plugged in. ``EventDetailIngestJob.run()`` only
calls ``EventDetailRepository.upsert_bundle``; the repository's upsert
pipeline never touches ``event_incident`` or ``event_statistic``. Empirical
proof on prod: after running ``cli event --event-id 10640568`` for three
UCL 2022/23 matches, ``api_payload_snapshot`` has the incidents+statistics
snapshots but the normalized tables are empty (verified via
``audit-db``).

This test pins down the contract: ``EventDetailIngestJob`` must, in the
same transaction as ``upsert_bundle``, push every payload_snapshot whose
endpoint_pattern is ``/incidents`` or ``/statistics`` through
``ParserRegistry.default()`` and persist the result via the supplied
``NormalizeRepository``.
"""

from __future__ import annotations

import unittest
from contextlib import asynccontextmanager
from dataclasses import replace
from typing import Any

from schema_inspector.competition_parser import (
    ApiPayloadSnapshotRecord,
    CategoryRecord,
    CountryRecord,
    SportRecord,
    UniqueTournamentRecord,
)
from schema_inspector.endpoints import EndpointRegistryEntry
from schema_inspector.event_detail_job import EventDetailIngestJob
from schema_inspector.event_detail_parser import EventDetailBundle
from schema_inspector.event_detail_repository import EventDetailWriteResult


def _empty_bundle() -> dict[str, tuple[Any, ...]]:
    """Construct kwargs for EventDetailBundle with all tuple fields empty.

    EventDetailBundle is a frozen dataclass with ~40 tuple-shaped fields.
    Tests only need to override ``payload_snapshots``; this helper
    initializes every other field to an empty tuple."""
    return {
        "registry_entries": (),
        "payload_snapshots": (),
        "sports": (),
        "countries": (),
        "categories": (),
        "unique_tournaments": (),
        "seasons": (),
        "tournaments": (),
        "teams": (),
        "venues": (),
        "referees": (),
        "managers": (),
        "manager_performances": (),
        "manager_team_memberships": (),
        "players": (),
        "event_statuses": (),
        "events": (),
        "event_round_infos": (),
        "event_status_times": (),
        "event_times": (),
        "event_var_in_progress_items": (),
        "event_scores": (),
        "event_filter_values": (),
        "event_change_items": (),
        "event_manager_assignments": (),
        "event_duels": (),
        "event_pregame_forms": (),
        "event_pregame_form_sides": (),
        "event_pregame_form_items": (),
        "event_vote_options": (),
        "event_comment_feeds": (),
        "event_comments": (),
        "event_graphs": (),
        "event_graph_points": (),
        "event_team_heatmaps": (),
        "event_team_heatmap_points": (),
        "providers": (),
        "provider_configurations": (),
        "event_markets": (),
        "event_market_choices": (),
        "event_winning_odds": (),
        "event_lineups": (),
        "event_lineup_players": (),
        "event_lineup_missing_players": (),
        "event_best_player_entries": (),
        "event_player_statistics": (),
        "event_player_stat_values": (),
        "event_player_rating_breakdown_actions": (),
    }


def _make_bundle(*, event_id: int, payload_snapshots: tuple[ApiPayloadSnapshotRecord, ...]) -> EventDetailBundle:
    return EventDetailBundle(**{**_empty_bundle(), "payload_snapshots": payload_snapshots})


class _FakeConnection:
    """Stand-in for an asyncpg connection inside an open transaction."""


class _FakeAsyncpgDatabase:
    def __init__(self) -> None:
        self.connection = _FakeConnection()

    @asynccontextmanager
    async def transaction(self):
        yield self.connection


class _FakeEventDetailParser:
    """Returns a bundle with the configured payload_snapshots."""

    def __init__(self, *, event_id: int, snapshots: tuple[ApiPayloadSnapshotRecord, ...]) -> None:
        self.event_id = event_id
        self.snapshots = snapshots
        self.calls: list[tuple[int, tuple[int, ...]]] = []

    async def fetch_bundle(self, event_id, *, provider_ids=(1,), timeout=20.0, profile=None):
        self.calls.append((event_id, tuple(provider_ids)))
        return _make_bundle(event_id=event_id, payload_snapshots=self.snapshots)


class _FakeEventDetailRepository:
    def __init__(self) -> None:
        self.calls: list[tuple[object, EventDetailBundle]] = []

    async def upsert_bundle(self, connection, bundle, *, profile=None):
        self.calls.append((connection, bundle))
        # EventDetailWriteResult has ~45 *_rows fields. The test does
        # not care what numbers come back — every required field gets
        # 0 via construction from the dataclass annotations.
        kwargs = {
            field_name: 0 for field_name in EventDetailWriteResult.__dataclass_fields__
        }
        return EventDetailWriteResult(**kwargs)


class _FakeNormalizeRepository:
    def __init__(self) -> None:
        self.persist_calls: list[tuple[str, dict[str, tuple]]] = []

    async def persist_parse_result(self, executor, result, *, skip_entity_upserts=False):
        # Snapshot the metric_rows shape so the test can assert on it.
        self.persist_calls.append((result.parser_family, dict(result.metric_rows)))


def _incidents_snapshot(event_id: int) -> ApiPayloadSnapshotRecord:
    return ApiPayloadSnapshotRecord(
        endpoint_pattern="/api/v1/event/{event_id}/incidents",
        source_url=f"https://www.sofascore.com/api/v1/event/{event_id}/incidents",
        envelope_key="incidents",
        context_entity_type="event",
        context_entity_id=event_id,
        payload={
            "incidents": [
                {"id": 1, "incidentType": "goal", "time": 25, "homeScore": 1, "awayScore": 0, "text": ""},
                {"id": 2, "incidentType": "card", "time": 38, "text": "Yellow card"},
                {"id": 3, "incidentType": "goal", "time": 67, "homeScore": 1, "awayScore": 1, "text": ""},
            ],
        },
        fetched_at="2026-05-21T00:00:00+00:00",
    )


def _statistics_snapshot(event_id: int) -> ApiPayloadSnapshotRecord:
    return ApiPayloadSnapshotRecord(
        endpoint_pattern="/api/v1/event/{event_id}/statistics",
        source_url=f"https://www.sofascore.com/api/v1/event/{event_id}/statistics",
        envelope_key="statistics",
        context_entity_type="event",
        context_entity_id=event_id,
        payload={
            "statistics": [
                {
                    "period": "ALL",
                    "groups": [
                        {
                            "groupName": "Match overview",
                            "statisticsItems": [
                                {"name": "Ball possession", "home": "59%", "away": "41%", "compareCode": 1, "statisticsType": "positive"},
                                {"name": "Total shots", "home": "12", "away": "9", "compareCode": 1, "statisticsType": "positive"},
                            ],
                        }
                    ],
                }
            ],
        },
        fetched_at="2026-05-21T00:00:00+00:00",
    )


class EventDetailIngestJobNormalizesIncidentsAndStatisticsTests(unittest.IsolatedAsyncioTestCase):
    """Pinning the Stage 4.4 contract: a bundle with /incidents and
    /statistics snapshots must produce normalized event_incident /
    event_statistic rows in the same transaction as upsert_bundle."""

    async def test_run_pushes_incidents_snapshot_through_parser_registry(self) -> None:
        event_id = 10640568
        parser = _FakeEventDetailParser(
            event_id=event_id, snapshots=(_incidents_snapshot(event_id),),
        )
        repository = _FakeEventDetailRepository()
        normalize_repository = _FakeNormalizeRepository()
        database = _FakeAsyncpgDatabase()

        job = EventDetailIngestJob(
            parser=parser,
            repository=repository,
            database=database,
            normalize_repository=normalize_repository,
        )
        await job.run(event_id)

        self.assertEqual(len(repository.calls), 1, msg="upsert_bundle must still be called.")
        incident_calls = [
            (family, rows) for family, rows in normalize_repository.persist_calls
            if family == "event_incidents"
        ]
        self.assertEqual(
            len(incident_calls), 1,
            msg=(
                "EventDetailIngestJob.run() must invoke ParserRegistry on every "
                "captured /incidents snapshot and forward the parsed metric_rows "
                "to NormalizeRepository.persist_parse_result so event_incident "
                "is populated for archive matches."
            ),
        )
        _, metric_rows = incident_calls[0]
        self.assertIn("event_incident", metric_rows)
        self.assertEqual(
            len(metric_rows["event_incident"]), 3,
            msg="Three incident rows were in the payload — all three must surface in the parser metric_rows.",
        )

    async def test_run_pushes_statistics_snapshot_through_parser_registry(self) -> None:
        event_id = 10640568
        parser = _FakeEventDetailParser(
            event_id=event_id, snapshots=(_statistics_snapshot(event_id),),
        )
        repository = _FakeEventDetailRepository()
        normalize_repository = _FakeNormalizeRepository()
        database = _FakeAsyncpgDatabase()

        job = EventDetailIngestJob(
            parser=parser,
            repository=repository,
            database=database,
            normalize_repository=normalize_repository,
        )
        await job.run(event_id)

        stat_calls = [
            (family, rows) for family, rows in normalize_repository.persist_calls
            if family == "event_statistics"
        ]
        self.assertEqual(len(stat_calls), 1)
        _, metric_rows = stat_calls[0]
        self.assertIn("event_statistic", metric_rows)
        self.assertEqual(len(metric_rows["event_statistic"]), 2)

    async def test_normalize_repository_called_after_upsert_bundle_same_transaction(self) -> None:
        """Persistence order matters: upsert_bundle creates the parent event
        row, normalize sink writes the dependent event_incident / event_statistic
        rows. Both must share the same connection so failure rollback is
        atomic."""

        event_id = 10640568
        parser = _FakeEventDetailParser(
            event_id=event_id,
            snapshots=(_incidents_snapshot(event_id), _statistics_snapshot(event_id)),
        )
        repository = _FakeEventDetailRepository()
        normalize_repository = _FakeNormalizeRepository()
        database = _FakeAsyncpgDatabase()

        job = EventDetailIngestJob(
            parser=parser,
            repository=repository,
            database=database,
            normalize_repository=normalize_repository,
        )
        await job.run(event_id)

        # Repository call and normalize calls all received the same connection.
        self.assertEqual(len(repository.calls), 1)
        repo_connection, _ = repository.calls[0]
        self.assertIs(repo_connection, database.connection)
        # Both families were normalized.
        families = {family for family, _ in normalize_repository.persist_calls}
        self.assertEqual(families, {"event_incidents", "event_statistics"})

    async def test_run_skips_normalize_for_unrelated_endpoint_patterns(self) -> None:
        """Only /incidents and /statistics are routed through the parser
        registry from this hook — other snapshots are already covered by
        their own dedicated parser inside ``upsert_bundle`` (lineups,
        managers, etc.) or by ``PilotOrchestrator`` on the live path.
        Double-parsing would be a waste and could cause duplicate DELETEs."""

        event_id = 10640568
        unrelated_snapshot = ApiPayloadSnapshotRecord(
            endpoint_pattern="/api/v1/event/{event_id}/managers",
            source_url=f"https://www.sofascore.com/api/v1/event/{event_id}/managers",
            envelope_key="managers",
            context_entity_type="event",
            context_entity_id=event_id,
            payload={"managers": []},
            fetched_at="2026-05-21T00:00:00+00:00",
        )
        parser = _FakeEventDetailParser(event_id=event_id, snapshots=(unrelated_snapshot,))
        repository = _FakeEventDetailRepository()
        normalize_repository = _FakeNormalizeRepository()
        database = _FakeAsyncpgDatabase()

        job = EventDetailIngestJob(
            parser=parser,
            repository=repository,
            database=database,
            normalize_repository=normalize_repository,
        )
        await job.run(event_id)

        self.assertEqual(
            normalize_repository.persist_calls, [],
            msg="Only /incidents and /statistics must trigger the registry-based normalize hook.",
        )


if __name__ == "__main__":
    unittest.main()
