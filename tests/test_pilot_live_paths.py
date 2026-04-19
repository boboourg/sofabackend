from __future__ import annotations

import json
import unittest

from schema_inspector.fetch_executor import FetchExecutor
from schema_inspector.normalizers.worker import NormalizeWorker
from schema_inspector.parsers.registry import ParserRegistry
from schema_inspector.pipeline.pilot_orchestrator import PilotOrchestrator
from schema_inspector.planner.planner import Planner
from schema_inspector.queue.live_state import LIVE_COLD_ZSET, LIVE_HOT_ZSET, LIVE_WARM_ZSET, LiveEventStateStore
from schema_inspector.queue.streams import RedisStreamQueue, STREAM_LIVE_HOT, STREAM_LIVE_WARM
from schema_inspector.runtime import TransportAttempt, TransportResult
from schema_inspector.workers.live_worker import LiveWorker


class _FakeTransport:
    def __init__(self, responses: dict[str, TransportResult]) -> None:
        self.responses = responses
        self.seen_urls: list[str] = []

    async def fetch(self, url: str, *, headers=None, timeout: float = 20.0) -> TransportResult:
        del headers, timeout
        self.seen_urls.append(url)
        return self.responses[url]


class _FakeRawSnapshotStore:
    def __init__(self) -> None:
        self.snapshots_by_id = {}
        self._next_id = 1

    async def insert_request_log(self, executor, record) -> None:
        del executor, record

    async def insert_payload_snapshot_returning_id(self, executor, record) -> int:
        del executor
        snapshot_id = self._next_id
        self._next_id += 1
        self.snapshots_by_id[snapshot_id] = record
        return snapshot_id

    async def insert_payload_snapshot_if_missing_returning_id(self, executor, record) -> int:
        return await self.insert_payload_snapshot_returning_id(executor, record)

    async def upsert_snapshot_head(self, executor, record) -> None:
        del executor, record

    def load_snapshot(self, snapshot_id: int):
        from schema_inspector.parsers.base import RawSnapshot

        record = self.snapshots_by_id[snapshot_id]
        return RawSnapshot(
            snapshot_id=snapshot_id,
            endpoint_pattern=record.endpoint_pattern,
            sport_slug=record.sport_slug,
            source_url=record.source_url,
            resolved_url=record.resolved_url,
            envelope_key=record.envelope_key,
            http_status=record.http_status,
            payload=record.payload,
            fetched_at=record.fetched_at,
            context_entity_type=record.context_entity_type,
            context_entity_id=record.context_entity_id,
            context_unique_tournament_id=record.context_unique_tournament_id,
            context_season_id=record.context_season_id,
            context_event_id=record.context_event_id,
        )


class _FakeCapabilityRepository:
    async def insert_observation(self, executor, record) -> None:
        del executor, record

    async def upsert_rollup(self, executor, record) -> None:
        del executor, record


class _FakeLiveBackend:
    def __init__(self) -> None:
        self.hashes: dict[str, dict[str, object]] = {}
        self.zsets: dict[str, dict[str, float]] = {}

    def hset(self, key: str, mapping: dict[str, object]) -> int:
        self.hashes.setdefault(key, {}).update(mapping)
        return 1

    def zadd(self, key: str, mapping: dict[str, float]) -> int:
        bucket = self.zsets.setdefault(key, {})
        for member, score in mapping.items():
            bucket[str(member)] = float(score)
        return 1

    def zrem(self, key: str, *members: str) -> int:
        bucket = self.zsets.setdefault(key, {})
        removed = 0
        for member in members:
            if member in bucket:
                removed += 1
                del bucket[member]
        return removed

    def zrangebyscore(self, key: str, min_score: float, max_score: float):
        del min_score, max_score
        bucket = self.zsets.get(key, {})
        return [member for member, _ in sorted(bucket.items(), key=lambda item: (item[1], item[0]))]


class _FakeStreamBackend:
    def __init__(self) -> None:
        self.streams: dict[str, list[tuple[str, dict[str, str]]]] = {}
        self.counters: dict[str, int] = {}

    def xadd(self, stream: str, fields: dict[str, str]) -> str:
        counter = self.counters.get(stream, 0) + 1
        self.counters[stream] = counter
        message_id = f"1-{counter}"
        self.streams.setdefault(stream, []).append((message_id, dict(fields)))
        return message_id


class PilotLivePathsTests(unittest.IsolatedAsyncioTestCase):
    async def test_inprogress_event_is_enqueued_on_hot_lane(self) -> None:
        now_ms = 1_800_000_000_000
        live_backend = _FakeLiveBackend()
        stream_backend = _FakeStreamBackend()
        orchestrator = _build_orchestrator(
            transport=_FakeTransport(_tennis_responses(event_id=15921219, status_type="inprogress", start_timestamp=(now_ms // 1000) - 300)),
            live_state_store=LiveEventStateStore(live_backend),
            stream_queue=RedisStreamQueue(stream_backend),
            now_ms_factory=lambda: now_ms,
        )

        await orchestrator.run_event(event_id=15921219, sport_slug="tennis")

        state = live_backend.hashes["live:event:15921219"]
        self.assertEqual(state["poll_profile"], "hot")
        self.assertEqual(state["is_finalized"], 0)
        self.assertEqual(live_backend.zsets[LIVE_HOT_ZSET]["15921219"], float(now_ms + 10_000))
        self.assertEqual(stream_backend.streams[STREAM_LIVE_HOT][0][1]["job_type"], "refresh_live_event")
        self.assertEqual(stream_backend.streams[STREAM_LIVE_HOT][0][1]["event_id"], "15921219")

    async def test_starting_soon_event_is_enqueued_on_warm_lane(self) -> None:
        now_ms = 1_800_000_000_000
        live_backend = _FakeLiveBackend()
        stream_backend = _FakeStreamBackend()
        orchestrator = _build_orchestrator(
            transport=_FakeTransport(_tennis_responses(event_id=15921220, status_type="scheduled", start_timestamp=(now_ms // 1000) + 20 * 60)),
            live_state_store=LiveEventStateStore(live_backend),
            stream_queue=RedisStreamQueue(stream_backend),
            now_ms_factory=lambda: now_ms,
        )

        await orchestrator.run_event(event_id=15921220, sport_slug="tennis")

        state = live_backend.hashes["live:event:15921220"]
        self.assertEqual(state["poll_profile"], "warm")
        self.assertEqual(live_backend.zsets[LIVE_WARM_ZSET]["15921220"], float(now_ms + 60_000))
        self.assertEqual(stream_backend.streams[STREAM_LIVE_WARM][0][1]["job_type"], "refresh_live_event")

    async def test_halftime_event_stays_on_hot_lane(self) -> None:
        now_ms = 1_800_000_000_000
        live_backend = _FakeLiveBackend()
        stream_backend = _FakeStreamBackend()
        orchestrator = _build_orchestrator(
            transport=_FakeTransport(
                _tennis_responses(
                    event_id=15921222,
                    status_type="halftime",
                    start_timestamp=(now_ms // 1000) - 300,
                )
            ),
            live_state_store=LiveEventStateStore(live_backend),
            stream_queue=RedisStreamQueue(stream_backend),
            now_ms_factory=lambda: now_ms,
        )

        await orchestrator.run_event(event_id=15921222, sport_slug="tennis")

        state = live_backend.hashes["live:event:15921222"]
        self.assertEqual(state["poll_profile"], "hot")
        self.assertEqual(live_backend.zsets[LIVE_HOT_ZSET]["15921222"], float(now_ms + 10_000))
        self.assertEqual(stream_backend.streams[STREAM_LIVE_HOT][0][1]["job_type"], "refresh_live_event")

    async def test_finished_event_runs_final_sweep_and_marks_terminal_state(self) -> None:
        now_ms = 1_800_000_000_000
        live_backend = _FakeLiveBackend()
        stream_backend = _FakeStreamBackend()
        transport = _FakeTransport(
            _tennis_responses(
                event_id=15921221,
                status_type="finished",
                start_timestamp=(now_ms // 1000) - 7200,
            )
        )
        orchestrator = _build_orchestrator(
            transport=transport,
            live_state_store=LiveEventStateStore(live_backend),
            stream_queue=RedisStreamQueue(stream_backend),
            now_ms_factory=lambda: now_ms,
        )

        report = await orchestrator.run_event(event_id=15921221, sport_slug="tennis")

        statistics_url = "https://www.sofascore.com/api/v1/event/15921221/statistics"
        lineups_url = "https://www.sofascore.com/api/v1/event/15921221/lineups"
        incidents_url = "https://www.sofascore.com/api/v1/event/15921221/incidents"
        self.assertEqual(transport.seen_urls.count(statistics_url), 2)
        self.assertEqual(transport.seen_urls.count(lineups_url), 2)
        self.assertEqual(transport.seen_urls.count(incidents_url), 2)
        self.assertTrue(report.finalized)
        state = live_backend.hashes["live:event:15921221"]
        self.assertEqual(state["status_type"], "finished")
        self.assertEqual(state["is_finalized"], 1)
        self.assertNotIn("15921221", live_backend.zsets.get(LIVE_HOT_ZSET, {}))
        self.assertNotIn("15921221", live_backend.zsets.get(LIVE_WARM_ZSET, {}))
        self.assertNotIn("15921221", live_backend.zsets.get(LIVE_COLD_ZSET, {}))
        self.assertEqual(stream_backend.streams, {})


def _build_orchestrator(*, transport, live_state_store, stream_queue, now_ms_factory):
    raw_store = _FakeRawSnapshotStore()
    return PilotOrchestrator(
        fetch_executor=FetchExecutor(transport=transport, raw_repository=raw_store, sql_executor=object()),
        snapshot_store=raw_store,
        normalize_worker=NormalizeWorker(ParserRegistry.default()),
        planner=Planner(
            capability_rollup={
                "/api/v1/event/{event_id}/graph": "unsupported",
                "/api/v1/event/{event_id}/incidents": "supported",
            }
        ),
        capability_repository=_FakeCapabilityRepository(),
        sql_executor=object(),
        live_worker=LiveWorker(),
        live_state_store=live_state_store,
        stream_queue=stream_queue,
        now_ms_factory=now_ms_factory,
    )


def _tennis_responses(*, event_id: int, status_type: str, start_timestamp: int) -> dict[str, TransportResult]:
    event_url = f"https://www.sofascore.com/api/v1/event/{event_id}"
    statistics_url = f"https://www.sofascore.com/api/v1/event/{event_id}/statistics"
    lineups_url = f"https://www.sofascore.com/api/v1/event/{event_id}/lineups"
    incidents_url = f"https://www.sofascore.com/api/v1/event/{event_id}/incidents"
    point_by_point_url = f"https://www.sofascore.com/api/v1/event/{event_id}/point-by-point"
    tennis_power_url = f"https://www.sofascore.com/api/v1/event/{event_id}/tennis-power"
    return {
        event_url: _json_result(
            event_url,
            {
                "event": {
                    "id": event_id,
                    "slug": "sinner-alcaraz",
                    "tournament": {
                        "id": 300,
                        "slug": "wimbledon",
                        "name": "Wimbledon",
                        "uniqueTournament": {"id": 2361, "slug": "wimbledon", "name": "Wimbledon"},
                    },
                    "season": {"id": 90001, "name": "Wimbledon 2026", "year": "2026"},
                    "status": {"type": status_type},
                    "startTimestamp": start_timestamp,
                    "homeTeam": {"id": 101, "slug": "sinner", "name": "Jannik Sinner"},
                    "awayTeam": {"id": 102, "slug": "alcaraz", "name": "Carlos Alcaraz"},
                }
            },
        ),
        statistics_url: _json_result(statistics_url, {"statistics": []}),
        lineups_url: _json_result(lineups_url, {"home": {"players": []}, "away": {"players": []}}),
        incidents_url: _json_result(incidents_url, {"incidents": []}),
        point_by_point_url: _json_result(point_by_point_url, {"pointByPoint": []}),
        tennis_power_url: _json_result(tennis_power_url, {"tennisPowerRankings": {"home": {"current": 0.61}, "away": {"current": 0.39}}}),
    }


def _json_result(url: str, payload: object, *, status_code: int = 200) -> TransportResult:
    return TransportResult(
        resolved_url=url,
        status_code=status_code,
        headers={"Content-Type": "application/json"},
        body_bytes=json.dumps(payload).encode("utf-8"),
        attempts=(TransportAttempt(1, "proxy_1", status_code, None, None),),
        final_proxy_name="proxy_1",
        challenge_reason=None,
    )


if __name__ == "__main__":
    unittest.main()
