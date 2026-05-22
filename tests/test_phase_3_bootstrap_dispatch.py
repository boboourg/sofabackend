"""Phase 3: bootstrap dispatch + lightweight path tests.

Two-phase archive backfill architecture:

  * pending  ─ bootstrap pipeline (lightweight: only event-list fetched).
                After success: state → events_loaded.
  * events_loaded ─ full walk pipeline (cursor advance picks it up).
                After success: state → fully_processed.
  * fully_processed ─ done.

These tests pin the dispatch:

  1. ``bootstrap_mode=True`` skips statistics / standings / leaderboards /
     featured-events / entities / event-detail.
  2. Worker handle queries the catalog row, dispatches bootstrap mode
     for ``pending`` and full mode for ``events_loaded`` / missing.
  3. Service signature accepts ``bootstrap_mode`` kwarg (default False)
     so existing callers stay binary-compatible.
"""

from __future__ import annotations

import asyncio
import unittest
from unittest.mock import AsyncMock, MagicMock


class BootstrapModeSkipFlagsTests(unittest.TestCase):
    """Service path: when bootstrap_mode=True, all macro stages
    (statistics/standings/leaderboards/featured) AND event-detail and
    entities must be skipped. Only round/last/{p} event-list fetch
    is left enabled."""

    def test_run_historical_tournament_archive_accepts_bootstrap_mode_kwarg(self) -> None:
        from schema_inspector.services.historical_archive_service import (
            run_historical_tournament_archive,
        )
        import inspect

        sig = inspect.signature(run_historical_tournament_archive)
        self.assertIn(
            "bootstrap_mode",
            sig.parameters,
            "Service must accept bootstrap_mode kwarg.",
        )
        self.assertEqual(
            sig.parameters["bootstrap_mode"].default, False,
            "bootstrap_mode default must be False (backwards-compatible).",
        )

    def test_bootstrap_mode_passes_all_skip_flags_to_worker(self) -> None:
        """When bootstrap_mode=True the service must call
        ``_run_tournament_worker`` with skip_statistics, skip_standings,
        skip_leaderboards, skip_featured_events, skip_event_detail,
        skip_entities all True. Only skip_round_events stays False
        (event-list is the whole point of bootstrap).
        """
        from schema_inspector.services import historical_archive_service as svc

        captured: dict[str, object] = {}

        async def fake_worker(*args, **kwargs):
            captured.update(kwargs)
            class R:
                season_ids = ()
                completed_seasons = 0
                discovered_event_ids = 0
                stage_failures = 0
                success = True
                capabilities_completed = frozenset()
            return R()

        original_worker = svc._run_tournament_worker
        svc._run_tournament_worker = fake_worker
        try:
            class FakeApp:
                runtime_config = MagicMock(source_slug="sofascore")
                transport = MagicMock()
                database = MagicMock()
            fake_app = FakeApp()
            # build_source_adapter dependency — patch out
            original_adapter = svc.build_source_adapter
            svc.build_source_adapter = MagicMock()
            try:
                asyncio.run(
                    svc.run_historical_tournament_archive(
                        fake_app,
                        unique_tournament_id=7,
                        sport_slug="football",
                        target_season_id=41897,
                        bootstrap_mode=True,
                    )
                )
            finally:
                svc.build_source_adapter = original_adapter
        finally:
            svc._run_tournament_worker = original_worker

        # All five macro skips ON + event_detail + entities ON.
        self.assertTrue(captured.get("skip_statistics"), "bootstrap must skip statistics")
        self.assertTrue(captured.get("skip_standings"), "bootstrap must skip standings")
        self.assertTrue(captured.get("skip_leaderboards"), "bootstrap must skip leaderboards")
        self.assertTrue(captured.get("skip_featured_events"), "bootstrap must skip featured-events")
        self.assertTrue(captured.get("skip_event_detail"), "bootstrap must skip event_detail")
        self.assertTrue(captured.get("skip_entities"), "bootstrap must skip entities")
        # Round events MUST stay enabled (the whole point of bootstrap).
        self.assertFalse(captured.get("skip_round_events"), "bootstrap must NOT skip round_events")

    def test_default_mode_keeps_full_matrix(self) -> None:
        """Without bootstrap_mode (or with =False) the existing full
        archive path stays intact — no regression of macro stages."""
        from schema_inspector.services import historical_archive_service as svc

        captured: dict[str, object] = {}

        async def fake_worker(*args, **kwargs):
            captured.update(kwargs)
            class R:
                season_ids = ()
                completed_seasons = 0
                discovered_event_ids = 0
                stage_failures = 0
                success = True
                capabilities_completed = frozenset()
            return R()

        original_worker = svc._run_tournament_worker
        original_adapter = svc.build_source_adapter
        svc._run_tournament_worker = fake_worker
        svc.build_source_adapter = MagicMock()
        try:
            class FakeApp:
                runtime_config = MagicMock(source_slug="sofascore")
                transport = MagicMock()
                database = MagicMock()
            asyncio.run(
                svc.run_historical_tournament_archive(
                    FakeApp(),
                    unique_tournament_id=7,
                    sport_slug="football",
                    target_season_id=41897,
                )
            )
        finally:
            svc._run_tournament_worker = original_worker
            svc.build_source_adapter = original_adapter

        # Default flags from the existing pipeline.
        self.assertFalse(captured.get("skip_statistics"))
        self.assertFalse(captured.get("skip_standings"))
        self.assertFalse(captured.get("skip_leaderboards"))
        self.assertFalse(captured.get("skip_featured_events"))


class WorkerDispatchByCatalogStateTests(unittest.TestCase):
    """Worker queries the catalog row for (UT, target_season) and
    dispatches bootstrap_mode=True only when state=='pending'.
    Missing row → default (full) mode (defensive — legacy seasons
    before catalog populated should still process normally)."""

    def test_worker_resolves_bootstrap_flag_from_catalog_pending(self) -> None:
        from schema_inspector.workers.historical_archive_worker import (
            _resolve_bootstrap_mode_from_catalog_state,
        )
        self.assertTrue(_resolve_bootstrap_mode_from_catalog_state("pending"))

    def test_worker_resolves_full_for_events_loaded(self) -> None:
        from schema_inspector.workers.historical_archive_worker import (
            _resolve_bootstrap_mode_from_catalog_state,
        )
        self.assertFalse(_resolve_bootstrap_mode_from_catalog_state("events_loaded"))

    def test_worker_resolves_full_for_missing_catalog_row(self) -> None:
        from schema_inspector.workers.historical_archive_worker import (
            _resolve_bootstrap_mode_from_catalog_state,
        )
        self.assertFalse(_resolve_bootstrap_mode_from_catalog_state(None))

    def test_worker_resolves_full_for_fully_processed(self) -> None:
        """A re-publish of an already-processed season should NOT
        regress to bootstrap mode (would lose match-center for
        idempotent re-fetch). Stays full mode."""
        from schema_inspector.workers.historical_archive_worker import (
            _resolve_bootstrap_mode_from_catalog_state,
        )
        self.assertFalse(_resolve_bootstrap_mode_from_catalog_state("fully_processed"))


class HybridAppBootstrapWiringTests(unittest.TestCase):
    """HybridApp.run_historical_tournament_archive must forward the
    bootstrap_mode kwarg to the service function. Without this wire,
    the worker can call bootstrap_mode=True but the service will not
    see it."""

    def test_hybridapp_method_accepts_bootstrap_mode(self) -> None:
        from schema_inspector.cli import HybridApp
        import inspect

        sig = inspect.signature(HybridApp.run_historical_tournament_archive)
        self.assertIn("bootstrap_mode", sig.parameters)
        self.assertEqual(sig.parameters["bootstrap_mode"].default, False)


if __name__ == "__main__":
    unittest.main()
