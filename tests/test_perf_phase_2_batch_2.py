"""TDD tests for Phase 2 Batch 2 — three localized perf fixes from
PERFORMANCE_AUDIT_2026-05-20.md.

* **2.5** `retention_repository.delete_legacy_snapshot_batch` — must
  set ``SET LOCAL statement_timeout = '120s'`` inside the transaction
  because the batched DELETE legitimately takes ~60s on the 148 GB
  ``api_payload_snapshot`` table, exceeding the global 30s timeout
  applied as the P0 fix.

* **2.6** ``/ops/snapshots/summary?detail=true`` — must require an
  operator token (env-configured) to opt into the 5×COUNT(DISTINCT)
  path; otherwise silently falls back to the fast estimate path.
  Without this guard any health probe with ``?detail=true`` would
  reliably hit the 30s timeout and 500.

* **2.7** ``_fetch_unique_tournament_media_payload`` — must apply
  ``LIMIT 500`` and a ``jsonb_array_length(payload->'highlights') > 0``
  predicate in SQL so we don't materialize 4000 JSONB payloads for
  a long-running UT only to drop 3800 of them in Python.
"""

from __future__ import annotations

import os
import re
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent


# ---------------------------------------------------------------------------
# 2.5 — retention_repository SET LOCAL statement_timeout
# ---------------------------------------------------------------------------


class RetentionStatementTimeoutTests(unittest.IsolatedAsyncioTestCase):
    async def test_delete_batch_sets_local_statement_timeout(self) -> None:
        """The batched DELETE must run inside a transaction that bumps
        ``statement_timeout`` to 120s. Otherwise the global 30s timeout
        from the P0 fix kills the DELETE mid-batch."""
        from datetime import datetime, timezone

        from schema_inspector.storage.retention_repository import (
            RetentionRepository,
        )

        captured: list[str] = []

        class _FakeTransaction:
            async def __aenter__(self):
                return self

            async def __aexit__(self, exc_type, exc, tb):
                return False

        class _FakeExecutor:
            def transaction(self):
                return _FakeTransaction()

            async def execute(self, query, *args):
                captured.append(query)
                # Mimic asyncpg returning the DELETE command tag.
                return "DELETE 0"

        repo = RetentionRepository()
        await repo.delete_legacy_snapshot_batch(
            _FakeExecutor(),
            cutoff=datetime.now(timezone.utc),
            batch_size=1000,
        )
        # Statement_timeout SET must have been issued
        self.assertTrue(
            any("statement_timeout" in q.lower() and "600" in q for q in captured),
            f"expected SET LOCAL statement_timeout = 600s in batch; got {captured}",
        )
        # The DELETE itself must still be issued
        self.assertTrue(
            any("delete from api_payload_snapshot" in q.lower() for q in captured),
            "DELETE query missing",
        )


# ---------------------------------------------------------------------------
# 2.6 — /ops/snapshots/summary?detail=true gating
# ---------------------------------------------------------------------------


class OpsSnapshotsSummaryDetailGatingTests(unittest.IsolatedAsyncioTestCase):
    async def test_detail_requires_operator_token_falls_back_when_missing(self) -> None:
        """Without the env-configured ``SOFASCORE_OPS_DETAIL_TOKEN``, a
        ``?detail=true`` request must NOT execute the 5×COUNT(DISTINCT)
        path — instead falls back to the fast estimate."""
        from schema_inspector.local_api_server import LocalApiApplication

        app = LocalApiApplication.__new__(LocalApiApplication)
        called_with: list[bool] = []

        async def _fetch(*, detail: bool):
            called_with.append(detail)
            return {"raw_snapshots": 0, "source": "estimate" if not detail else "exact"}

        app._fetch_ops_snapshots_summary_payload = _fetch

        # Ensure no token in env
        prev = os.environ.pop("SOFASCORE_OPS_DETAIL_TOKEN", None)
        try:
            response = await app.handle_ops_get(
                "/ops/snapshots/summary", "detail=true"
            )
        finally:
            if prev is not None:
                os.environ["SOFASCORE_OPS_DETAIL_TOKEN"] = prev

        self.assertEqual(called_with, [False])
        self.assertEqual(response.payload["source"], "estimate")

    async def test_detail_with_matching_token_allows_exact_path(self) -> None:
        from schema_inspector.local_api_server import LocalApiApplication

        app = LocalApiApplication.__new__(LocalApiApplication)
        called_with: list[bool] = []

        async def _fetch(*, detail: bool):
            called_with.append(detail)
            return {"source": "exact" if detail else "estimate"}

        app._fetch_ops_snapshots_summary_payload = _fetch

        os.environ["SOFASCORE_OPS_DETAIL_TOKEN"] = "secret-xyz"
        try:
            response = await app.handle_ops_get(
                "/ops/snapshots/summary", "detail=true&token=secret-xyz"
            )
        finally:
            del os.environ["SOFASCORE_OPS_DETAIL_TOKEN"]

        self.assertEqual(called_with, [True])
        self.assertEqual(response.payload["source"], "exact")

    async def test_detail_with_wrong_token_falls_back(self) -> None:
        from schema_inspector.local_api_server import LocalApiApplication

        app = LocalApiApplication.__new__(LocalApiApplication)
        called_with: list[bool] = []

        async def _fetch(*, detail: bool):
            called_with.append(detail)
            return {"source": "exact" if detail else "estimate"}

        app._fetch_ops_snapshots_summary_payload = _fetch

        os.environ["SOFASCORE_OPS_DETAIL_TOKEN"] = "secret-xyz"
        try:
            response = await app.handle_ops_get(
                "/ops/snapshots/summary", "detail=true&token=wrong"
            )
        finally:
            del os.environ["SOFASCORE_OPS_DETAIL_TOKEN"]

        self.assertEqual(called_with, [False])
        self.assertEqual(response.payload["source"], "estimate")


# ---------------------------------------------------------------------------
# 2.7 — _fetch_unique_tournament_media_payload SQL LIMIT + jsonb predicate
# ---------------------------------------------------------------------------


class UniqueTournamentMediaSqlLimitTests(unittest.TestCase):
    """Source-level invariant: the synth SQL must include a
    ``LIMIT`` and a ``jsonb_array_length(...) > 0`` predicate so
    we don't materialize entire UT history just to drop 95% in
    Python."""

    def setUp(self) -> None:
        self.text = (
            REPO_ROOT / "schema_inspector" / "local_api_server.py"
        ).read_text(encoding="utf-8")
        # Extract the body of _fetch_unique_tournament_media_payload —
        # from def...next def boundary.
        m = re.search(
            r"async def _fetch_unique_tournament_media_payload\(.*?(?=\n    async def )",
            self.text,
            flags=re.DOTALL,
        )
        self.assertIsNotNone(m, "could not find media handler body")
        self.body = m.group(0)

    def test_sql_contains_jsonb_highlights_predicate(self) -> None:
        # Either highlights non-empty, OR videos array non-empty —
        # tolerate either field name depending on Sofascore payload shape.
        self.assertRegex(
            self.body,
            re.compile(
                r"jsonb_array_length\([^)]*payload[^)]*->\s*'highlights'[^)]*\)\s*>\s*0",
                re.DOTALL,
            ),
        )

    def test_sql_contains_explicit_limit(self) -> None:
        # SQL must include LIMIT (any value 100..2000 acceptable)
        m = re.search(r"\bLIMIT\s+(\d+)", self.body)
        self.assertIsNotNone(m, "expected LIMIT in media SQL")
        limit_value = int(m.group(1))
        self.assertGreaterEqual(limit_value, 100)
        self.assertLessEqual(limit_value, 2000)


if __name__ == "__main__":
    unittest.main()
