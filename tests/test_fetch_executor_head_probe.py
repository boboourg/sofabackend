from __future__ import annotations

import unittest

from schema_inspector.fetch_executor import FetchExecutor
from schema_inspector.fetch_models import FetchTask
from schema_inspector.runtime import TransportAttempt, TransportResult


class _RecordingTransport:
    """Captures the (url, method) of each fetch call."""

    def __init__(self, *, head_status=None, get_status=200, head_raises=False) -> None:
        self.calls: list[tuple[str, str]] = []
        self.head_status = head_status
        self.get_status = get_status
        self.head_raises = head_raises

    async def fetch(self, url, *, headers=None, timeout=20.0, method="GET"):
        self.calls.append((url, method))
        if method == "HEAD":
            if self.head_raises:
                raise RuntimeError("simulated transport failure on HEAD")
            return TransportResult(
                resolved_url=url,
                status_code=int(self.head_status or 0),
                headers={},
                body_bytes=b"",
                attempts=(TransportAttempt(1, "p1", int(self.head_status or 0), None, None),),
                final_proxy_name="p1",
                final_proxy_address=None,
                challenge_reason=None,
            )
        return TransportResult(
            resolved_url=url,
            status_code=int(self.get_status),
            headers={"Content-Type": "application/json"},
            body_bytes=b'{"topPlayers":{"rating":[]}}',
            attempts=(TransportAttempt(1, "p1", int(self.get_status), None, None),),
            final_proxy_name="p1",
            final_proxy_address=None,
            challenge_reason=None,
        )


class _FakeRawRepo:
    def __init__(self) -> None:
        self.request_logs: list = []
        self.snapshots: list = []
        self.heads: list = []

    async def insert_request_log(self, ex, record) -> None:
        del ex
        self.request_logs.append(record)

    async def insert_payload_snapshot_if_missing_returning_id(self, ex, record) -> int:
        del ex
        self.snapshots.append(record)
        return 100 + len(self.snapshots)

    async def upsert_snapshot_head(self, ex, record) -> None:
        del ex
        self.heads.append(record)


def _task(*, prefer_head_probe: bool = True, ut: int | None = 17, season: int | None = 76986) -> FetchTask:
    return FetchTask(
        trace_id="t-1",
        job_id="j-1",
        sport_slug="football",
        endpoint_pattern="/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/top-players/overall",
        source_url="https://www.sofascore.com/api/v1/unique-tournament/17/season/76986/top-players/overall",
        timeout_profile="resource_refresh",
        timeout_seconds=20.0,
        method="GET",
        context_entity_type="season",
        context_entity_id=season,
        context_unique_tournament_id=ut,
        context_season_id=season,
        fetch_reason="resource_refresh",
        prefer_head_probe=prefer_head_probe,
    )


class FetchExecutorHeadProbeTests(unittest.IsolatedAsyncioTestCase):
    async def test_head_404_skips_get_and_returns_synthetic_outcome(self) -> None:
        transport = _RecordingTransport(head_status=404, get_status=200)
        repo = _FakeRawRepo()
        executor = FetchExecutor(
            transport=transport,
            raw_repository=repo,
            sql_executor=object(),
        )
        outcome = await executor.execute(_task())
        # Only HEAD call happened, no GET.
        methods = [m for _, m in transport.calls]
        self.assertEqual(methods, ["HEAD"])
        self.assertEqual(outcome.classification, "head_probe_skipped")
        self.assertEqual(outcome.http_status, 404)
        self.assertIsNone(outcome.snapshot_id)
        # Forensics: request log written, but no snapshot.
        self.assertEqual(len(repo.request_logs), 1)
        self.assertEqual(len(repo.snapshots), 0)

    async def test_head_200_proceeds_to_get(self) -> None:
        transport = _RecordingTransport(head_status=200, get_status=200)
        repo = _FakeRawRepo()
        executor = FetchExecutor(
            transport=transport,
            raw_repository=repo,
            sql_executor=object(),
        )
        outcome = await executor.execute(_task())
        methods = [m for _, m in transport.calls]
        self.assertEqual(methods, ["HEAD", "GET"])
        self.assertEqual(outcome.classification, "success_json")
        self.assertEqual(outcome.http_status, 200)
        self.assertIsNotNone(outcome.snapshot_id)

    async def test_head_5xx_falls_through_to_get(self) -> None:
        transport = _RecordingTransport(head_status=503, get_status=200)
        executor = FetchExecutor(
            transport=transport,
            raw_repository=_FakeRawRepo(),
            sql_executor=object(),
        )
        outcome = await executor.execute(_task())
        methods = [m for _, m in transport.calls]
        # 503 is treated as transient — caller should not trust HEAD.
        self.assertEqual(methods, ["HEAD", "GET"])
        self.assertEqual(outcome.http_status, 200)

    async def test_head_transport_failure_falls_through(self) -> None:
        transport = _RecordingTransport(head_status=200, get_status=200, head_raises=True)
        executor = FetchExecutor(
            transport=transport,
            raw_repository=_FakeRawRepo(),
            sql_executor=object(),
        )
        outcome = await executor.execute(_task())
        # HEAD raised -> we don't trust it, GET runs as normal.
        methods = [m for _, m in transport.calls]
        self.assertEqual(methods, ["HEAD", "GET"])
        self.assertEqual(outcome.http_status, 200)

    async def test_prefer_head_probe_false_skips_head_entirely(self) -> None:
        transport = _RecordingTransport(head_status=200, get_status=200)
        executor = FetchExecutor(
            transport=transport,
            raw_repository=_FakeRawRepo(),
            sql_executor=object(),
        )
        await executor.execute(_task(prefer_head_probe=False))
        methods = [m for _, m in transport.calls]
        self.assertEqual(methods, ["GET"])

    async def test_recent_200_lookup_forces_get_when_head_says_404(self) -> None:
        """Defence-in-depth: if HEAD lies but recent 200 exists, force GET."""

        transport = _RecordingTransport(head_status=404, get_status=200)
        repo = _FakeRawRepo()

        async def _has_recent_200(task: FetchTask) -> bool:
            return True  # we have a real 200 within window

        executor = FetchExecutor(
            transport=transport,
            raw_repository=repo,
            sql_executor=object(),
            recent_200_lookup=_has_recent_200,
        )
        outcome = await executor.execute(_task())
        methods = [m for _, m in transport.calls]
        self.assertEqual(methods, ["HEAD", "GET"])
        self.assertEqual(outcome.http_status, 200)
        self.assertIsNotNone(outcome.snapshot_id)

    async def test_recent_200_lookup_returns_false_skips_get(self) -> None:
        transport = _RecordingTransport(head_status=404, get_status=200)
        repo = _FakeRawRepo()

        async def _no_recent(task: FetchTask) -> bool:
            return False

        executor = FetchExecutor(
            transport=transport,
            raw_repository=repo,
            sql_executor=object(),
            recent_200_lookup=_no_recent,
        )
        outcome = await executor.execute(_task())
        methods = [m for _, m in transport.calls]
        self.assertEqual(methods, ["HEAD"])
        self.assertEqual(outcome.classification, "head_probe_skipped")

    async def test_head_429_falls_through_to_get(self) -> None:
        # 429 = rate limited, transient — don't short-circuit.
        transport = _RecordingTransport(head_status=429, get_status=200)
        executor = FetchExecutor(
            transport=transport,
            raw_repository=_FakeRawRepo(),
            sql_executor=object(),
        )
        outcome = await executor.execute(_task())
        methods = [m for _, m in transport.calls]
        self.assertEqual(methods, ["HEAD", "GET"])


if __name__ == "__main__":
    unittest.main()
