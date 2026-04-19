from __future__ import annotations

import json
import os
import unittest
from pathlib import Path

from schema_inspector.cli import HybridApp
from schema_inspector.db import AsyncpgDatabase, DatabaseConfig
from schema_inspector.runtime import RuntimeConfig, TransportAttempt, TransportResult
from schema_inspector.storage.normalize_repository import RetriableRepositoryError


class _MappedTransport:
    def __init__(self, payloads_by_url: dict[str, object]) -> None:
        self.payloads_by_url = dict(payloads_by_url)
        self.calls: list[str] = []
        self.closed = False

    async def fetch(self, url: str, *, headers=None, timeout: float = 20.0):
        del headers, timeout
        self.calls.append(url)
        payload = self.payloads_by_url[url]
        return TransportResult(
            resolved_url=url,
            status_code=200,
            headers={"Content-Type": "application/json"},
            body_bytes=json.dumps(payload).encode("utf-8"),
            attempts=(TransportAttempt(1, "direct", 200, None, None),),
            final_proxy_name="direct",
            challenge_reason=None,
        )

    async def close(self) -> None:
        self.closed = True


class HybridCliIntegrationTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        database_url = os.getenv("TEST_DATABASE_URL")
        if not database_url:
            raise unittest.SkipTest("TEST_DATABASE_URL not set; skipping hybrid CLI integration tests")
        try:
            import asyncpg
        except ModuleNotFoundError as exc:
            raise unittest.SkipTest("asyncpg is not installed; skipping hybrid CLI integration tests") from exc

        self.database_url = database_url
        self.admin_connection = await asyncpg.connect(database_url)
        await self.admin_connection.execute("DROP SCHEMA public CASCADE")
        await self.admin_connection.execute("CREATE SCHEMA public")

        repo_root = Path(__file__).resolve().parent.parent
        await self.admin_connection.execute((repo_root / "postgres_schema.sql").read_text(encoding="utf-8"))
        await self.admin_connection.execute(
            (repo_root / "migrations" / "2026-04-16_hybrid_control_plane.sql").read_text(encoding="utf-8")
        )

        self.database = AsyncpgDatabase(DatabaseConfig(dsn=database_url, min_size=1, max_size=4, command_timeout=30.0))
        await self.database.connect()
        self.app = HybridApp(database=self.database, runtime_config=RuntimeConfig(require_proxy=False), redis_backend=None)

    async def asyncTearDown(self) -> None:
        if hasattr(self, "app"):
            await self.app.close()
        if hasattr(self, "database"):
            await self.database.close()
        if hasattr(self, "admin_connection"):
            await self.admin_connection.close()

    async def test_split_persist_nullifies_stale_cached_event_venue(self) -> None:
        event_id = 14090001
        await self._prepare_transport(self._football_full_payloads(event_id))
        await self.app.ensure_endpoint_registry("football")

        prefetched = await self.app._prefetch_event_run(event_id=event_id, sport_slug="football", hydration_mode="full")
        committed = await self.app._commit_prefetched_run(prefetched)
        self.app.normalize_repository._known_minimal_entities["venue"].add(11505)

        await self.app._persist_prefetched_run(committed, hydration_mode="full")

        row = await self.admin_connection.fetchrow("SELECT venue_id FROM event WHERE id = $1", event_id)
        self.assertIsNotNone(row)
        self.assertIsNone(row["venue_id"])

    async def test_split_persist_raises_retryable_error_for_stale_cached_unique_tournament(self) -> None:
        event_id = 14090002
        await self._prepare_transport(self._football_full_payloads(event_id))
        await self.app.ensure_endpoint_registry("football")

        prefetched = await self.app._prefetch_event_run(event_id=event_id, sport_slug="football", hydration_mode="full")
        committed = await self.app._commit_prefetched_run(prefetched)
        self.app.normalize_repository._known_minimal_entities["unique_tournament"].add(17)

        with self.assertRaises(RetriableRepositoryError):
            await self.app._persist_prefetched_run(committed, hydration_mode="full")

    async def test_split_retry_reuses_existing_payload_snapshots_after_persist_failure(self) -> None:
        event_id = 14090003
        await self._prepare_transport(self._football_full_payloads(event_id))
        await self.app.ensure_endpoint_registry("football")

        for _ in range(2):
            prefetched = await self.app._prefetch_event_run(
                event_id=event_id,
                sport_slug="football",
                hydration_mode="full",
            )
            committed = await self.app._commit_prefetched_run(prefetched)
            self.app.normalize_repository._known_minimal_entities["unique_tournament"].add(17)
            with self.assertRaises(RetriableRepositoryError):
                await self.app._persist_prefetched_run(committed, hydration_mode="full")

        snapshot_count = await self.admin_connection.fetchval(
            "SELECT COUNT(*) FROM api_payload_snapshot WHERE source_url = $1",
            self._event_url(event_id),
        )
        snapshot_head_count = await self.admin_connection.fetchval(
            "SELECT COUNT(*) FROM api_snapshot_head WHERE scope_key = $1",
            f"event:{event_id}:/api/v1/event/{{event_id}}",
        )
        self.assertEqual(snapshot_count, 1)
        self.assertEqual(snapshot_head_count, 1)

    async def test_split_retry_appends_request_logs_by_design_after_persist_failure(self) -> None:
        event_id = 14090004
        await self._prepare_transport(self._football_full_payloads(event_id))
        await self.app.ensure_endpoint_registry("football")

        for _ in range(2):
            prefetched = await self.app._prefetch_event_run(
                event_id=event_id,
                sport_slug="football",
                hydration_mode="full",
            )
            committed = await self.app._commit_prefetched_run(prefetched)
            self.app.normalize_repository._known_minimal_entities["unique_tournament"].add(17)
            with self.assertRaises(RetriableRepositoryError):
                await self.app._persist_prefetched_run(committed, hydration_mode="full")

        request_log_count = await self.admin_connection.fetchval(
            "SELECT COUNT(*) FROM api_request_log WHERE source_url = $1",
            self._event_url(event_id),
        )
        self.assertEqual(request_log_count, 2)

    async def _prepare_transport(self, payloads_by_url: dict[str, object]) -> None:
        await self.app.transport.close()
        self.app.transport = _MappedTransport(payloads_by_url)

    def _football_full_payloads(self, event_id: int) -> dict[str, object]:
        category = {
            "id": 10,
            "slug": "england",
            "name": "England",
            "sport": {"id": 1, "slug": "football", "name": "Football"},
            "country": {"alpha2": "EN", "alpha3": "ENG", "slug": "england", "name": "England"},
        }
        unique_tournament = {
            "id": 17,
            "slug": "premier-league",
            "name": "Premier League",
            "category": category,
            "country": {"alpha2": "EN", "alpha3": "ENG", "slug": "england", "name": "England"},
        }
        tournament = {
            "id": 100,
            "slug": "premier-league",
            "name": "Premier League",
            "category": category,
            "uniqueTournament": unique_tournament,
        }
        home_team = {
            "id": 42,
            "slug": "arsenal",
            "name": "Arsenal",
            "shortName": "Arsenal",
        }
        away_team = {
            "id": 43,
            "slug": "chelsea",
            "name": "Chelsea",
            "shortName": "Chelsea",
        }
        venue = {
            "id": 11505,
            "slug": "emirates-stadium",
            "name": "Emirates Stadium",
            "country": {"alpha2": "EN", "alpha3": "ENG", "slug": "england", "name": "England"},
        }
        return {
            self._event_url(event_id): {
                "event": {
                    "id": event_id,
                    "slug": f"arsenal-chelsea-{event_id}",
                    "startTimestamp": 1_800_000_000,
                    "status": {"type": "notstarted"},
                    "season": {"id": 76986, "name": "Premier League 25/26", "year": "25/26", "editor": False},
                    "tournament": tournament,
                    "venue": venue,
                    "homeTeam": home_team,
                    "awayTeam": away_team,
                }
            },
            self._statistics_url(event_id): {"statistics": []},
            self._lineups_url(event_id): {
                "home": {"formation": "4-3-3", "players": []},
                "away": {"formation": "4-2-3-1", "players": []},
            },
            self._incidents_url(event_id): {"incidents": []},
            self._team_url(42): {
                "team": {
                    **home_team,
                    "sport": {"id": 1, "slug": "football", "name": "Football"},
                    "category": category,
                    "country": {"alpha2": "EN", "alpha3": "ENG", "slug": "england", "name": "England"},
                    "tournament": tournament,
                }
            },
            self._team_url(43): {
                "team": {
                    **away_team,
                    "sport": {"id": 1, "slug": "football", "name": "Football"},
                    "category": category,
                    "country": {"alpha2": "EN", "alpha3": "ENG", "slug": "england", "name": "England"},
                    "tournament": tournament,
                }
            },
        }

    @staticmethod
    def _event_url(event_id: int) -> str:
        return f"https://www.sofascore.com/api/v1/event/{event_id}"

    @staticmethod
    def _statistics_url(event_id: int) -> str:
        return f"https://www.sofascore.com/api/v1/event/{event_id}/statistics"

    @staticmethod
    def _lineups_url(event_id: int) -> str:
        return f"https://www.sofascore.com/api/v1/event/{event_id}/lineups"

    @staticmethod
    def _incidents_url(event_id: int) -> str:
        return f"https://www.sofascore.com/api/v1/event/{event_id}/incidents"

    @staticmethod
    def _team_url(team_id: int) -> str:
        return f"https://www.sofascore.com/api/v1/team/{team_id}"


if __name__ == "__main__":
    unittest.main()
