"""TDD tests for Stage 3.4 (2026-05-20 historical layer):
``historical-backfill`` CLI subcommand.

Until this commit there was no one-shot CLI to backfill a single
``(unique_tournament_id, season_id)`` pair — the only options were:

* ``historical-tournament-planner-daemon`` — a long-running loop that
  reads ``tournament_registry`` cursors and publishes jobs at its own
  cadence;
* publishing a ``JOB_SYNC_TOURNAMENT_ARCHIVE`` envelope to Redis
  manually (works but undocumented and hard to script).

This subcommand closes the gap: ``python -m schema_inspector.cli
historical-backfill --unique-tournament-id 7 --season-id 41897 --sport-slug football``
synchronously walks the same code path as the historical tournament
worker (``services/historical_archive_service.run_historical_tournament_archive``)
for the requested (UT, season) and returns when ingest is complete.

This is a thin orchestration shim — the heavy lifting already exists
in ``run_historical_tournament_archive``. No new write-path is
introduced.
"""

from __future__ import annotations

import unittest


class HistoricalBackfillCliRegistrationTests(unittest.TestCase):
    def test_command_registered_in_argparse_parser(self) -> None:
        from schema_inspector.cli import _build_parser

        parser = _build_parser()
        # argparse stores subparsers in a private map; we walk it.
        subparsers_action = next(
            action for action in parser._actions if action.dest == "command"
        )
        self.assertIn(
            "historical-backfill",
            subparsers_action.choices,
            msg=(
                "Subcommand 'historical-backfill' must be registered on "
                "the top-level argparse parser."
            ),
        )

    def test_command_accepts_required_unique_tournament_and_season(self) -> None:
        from schema_inspector.cli import _build_parser

        parser = _build_parser()
        # Successful parse — full happy-path arguments.
        args = parser.parse_args(
            [
                "historical-backfill",
                "--unique-tournament-id", "7",
                "--season-id", "41897",
                "--sport-slug", "football",
            ]
        )
        self.assertEqual(args.command, "historical-backfill")
        self.assertEqual(args.unique_tournament_id, 7)
        self.assertEqual(args.season_id, 41897)
        self.assertEqual(args.sport_slug, "football")

    def test_command_requires_unique_tournament_id(self) -> None:
        """Missing --unique-tournament-id must fail with SystemExit (argparse)."""
        from schema_inspector.cli import _build_parser

        parser = _build_parser()
        with self.assertRaises(SystemExit):
            parser.parse_args(
                ["historical-backfill", "--season-id", "41897", "--sport-slug", "football"]
            )

    def test_command_requires_season_id(self) -> None:
        from schema_inspector.cli import _build_parser

        parser = _build_parser()
        with self.assertRaises(SystemExit):
            parser.parse_args(
                [
                    "historical-backfill",
                    "--unique-tournament-id", "7",
                    "--sport-slug", "football",
                ]
            )

    def test_command_appears_in_historical_commands_set(self) -> None:
        """Membership in ``_HISTORICAL_COMMANDS`` switches the runtime
        config to the historical proxy pool (separate non-residential
        pool reserved for archival work). Without it, the backfill would
        consume residential proxy budget intended for live polling."""
        from schema_inspector.cli import _HISTORICAL_COMMANDS

        self.assertIn(
            "historical-backfill",
            _HISTORICAL_COMMANDS,
            msg=(
                "historical-backfill must be in _HISTORICAL_COMMANDS so "
                "the dispatcher loads ``load_historical_runtime_config``. "
                "Otherwise the backfill would steal residential-proxy "
                "budget that the live contour relies on."
            ),
        )


class HistoricalBackfillCliDispatchSourceTests(unittest.TestCase):
    """Source-level pinning: the dispatch arm must wire to the
    existing ``run_historical_tournament_archive`` helper, not
    re-implement the ingest flow."""

    def test_dispatch_calls_run_historical_tournament_archive(self) -> None:
        import re
        from pathlib import Path

        text = (
            Path(__file__).resolve().parent.parent
            / "schema_inspector"
            / "cli.py"
        ).read_text(encoding="utf-8")
        # Find the dispatch arm for the new command.
        match = re.search(
            r'if args\.command == "historical-backfill"\s*:.*?return',
            text,
            flags=re.DOTALL,
        )
        self.assertIsNotNone(
            match,
            msg="Dispatch arm for historical-backfill not found in cli.py",
        )
        block = match.group(0)
        self.assertIn(
            "run_historical_tournament_archive",
            block,
            msg=(
                "Dispatch arm must delegate to "
                "services.historical_archive_service."
                "run_historical_tournament_archive — no duplicate "
                "ingest implementation."
            ),
        )
        self.assertIn(
            "target_season_id",
            block,
            msg=(
                "Dispatch arm must pass target_season_id so the "
                "tournament worker scopes to exactly the requested "
                "season (not the full backfill chain)."
            ),
        )


# ---------------------------------------------------------------------------
# Stage 4.1 (2026-05-20 historical match-center fix): the
# historical-backfill CLI handler must explicitly opt INTO per-event
# fan-out by passing ``skip_event_detail=False`` and
# ``skip_entities=False`` to ``run_historical_tournament_archive``.
#
# Why: by default the helper hardcodes both flags to True
# (historical_archive_service.py:70-71). Worker-historical-tournament
# compensates by publishing enrichment child-jobs AFTER archive
# (historical_archive_worker.py:160-172). The CLI handler does the
# direct synchronous call and never publishes those child-jobs, so
# without flipping the defaults the CLI version cannot pull lineups
# / incidents / statistics / player-stats for archive matches.
# ---------------------------------------------------------------------------


class HistoricalBackfillCliMatchCenterTests(unittest.TestCase):
    def test_run_historical_tournament_archive_accepts_skip_flag_kwargs(self) -> None:
        """Stage 4.1: skip_event_detail / skip_entities must be exposed
        as keyword arguments on the helper so the CLI handler can flip
        the worker-flow defaults without forking the function."""
        import inspect
        from schema_inspector.services.historical_archive_service import (
            run_historical_tournament_archive,
        )

        sig = inspect.signature(run_historical_tournament_archive)
        for name in ("skip_event_detail", "skip_entities"):
            self.assertIn(
                name,
                sig.parameters,
                msg=(
                    f"run_historical_tournament_archive must expose "
                    f"`{name}` as kwarg so the CLI handler can opt into "
                    "per-event fan-out for archive matches."
                ),
            )

    def test_run_historical_tournament_archive_defaults_preserve_worker_flow(self) -> None:
        """Defaults must remain True so worker-historical-tournament
        keeps its existing skip behaviour (it publishes enrichment-jobs
        afterwards to do the per-event work). Otherwise we double-fetch
        every archive match through both paths."""
        import inspect
        from schema_inspector.services.historical_archive_service import (
            run_historical_tournament_archive,
        )

        sig = inspect.signature(run_historical_tournament_archive)
        self.assertIs(sig.parameters["skip_event_detail"].default, True)
        self.assertIs(sig.parameters["skip_entities"].default, True)

    def test_cli_dispatch_arm_overrides_skip_flags_to_false(self) -> None:
        """Source-level: the cli.py dispatch arm for historical-backfill
        must pass skip_event_detail=False AND skip_entities=False so
        the operator-driven path actually pulls match-center."""
        import re
        from pathlib import Path

        text = (
            Path(__file__).resolve().parent.parent
            / "schema_inspector"
            / "cli.py"
        ).read_text(encoding="utf-8")
        match = re.search(
            r'if args\.command == "historical-backfill"\s*:.*?return',
            text,
            flags=re.DOTALL,
        )
        self.assertIsNotNone(match)
        block = match.group(0)
        self.assertIn(
            "skip_event_detail=False",
            block,
            msg=(
                "Dispatch arm must override skip_event_detail to False — "
                "this is what unlocks the per-event fan-out branch in "
                "default_tournaments_pipeline_cli._run_tournament_worker."
            ),
        )
        self.assertIn(
            "skip_entities=False",
            block,
            msg="Dispatch arm must also override skip_entities to False",
        )


# ---------------------------------------------------------------------------
# Stage 4.3 sanity (2026-05-20): the CLI handler must NOT inherit the
# 730-day window that the enrichment path uses. Verified at source
# level — the dispatch arm goes through ``run_historical_tournament_archive``
# (which under skip_event_detail=False delegates to
# ``_run_event_detail_batch`` with explicit ``event_ids=...``), NOT
# through ``run_historical_tournament_event_detail_batch`` (which
# applies ``choose_recent_history_window(sport)``).
# ---------------------------------------------------------------------------


class HistoricalBackfillCliBypassesRecentWindowTests(unittest.TestCase):
    def test_dispatch_does_not_call_enrichment_batch_helper(self) -> None:
        """The CLI handler must NOT invoke run_historical_tournament_event_detail_batch
        directly — that helper applies the 730-day window and would
        exclude archive matches older than ~2 years."""
        import re
        from pathlib import Path

        text = (
            Path(__file__).resolve().parent.parent
            / "schema_inspector"
            / "cli.py"
        ).read_text(encoding="utf-8")
        match = re.search(
            r'if args\.command == "historical-backfill"\s*:.*?return',
            text,
            flags=re.DOTALL,
        )
        self.assertIsNotNone(match)
        block = match.group(0)
        self.assertNotIn(
            "run_historical_tournament_event_detail_batch",
            block,
            msg=(
                "CLI handler must delegate to run_historical_tournament_archive "
                "(no window) — NOT to run_historical_tournament_event_detail_batch "
                "which gates events by choose_recent_history_window() = 730 days "
                "for football, excluding archive matches older than ~2 years."
            ),
        )

    def test_run_event_detail_batch_in_default_tournaments_pipeline_accepts_event_ids(self) -> None:
        """When skip_event_detail=False the archive helper goes through
        _run_event_detail_batch in default_tournaments_pipeline_cli with
        explicit event_ids — no timestamp filter at this layer."""
        import re
        from pathlib import Path

        text = (
            Path(__file__).resolve().parent.parent
            / "schema_inspector"
            / "default_tournaments_pipeline_cli.py"
        ).read_text(encoding="utf-8")
        # The line should look like _run_event_detail_batch(... event_ids=season_event_ids ...).
        match = re.search(
            r"_run_event_detail_batch\([^)]*event_ids=",
            text,
            flags=re.DOTALL,
        )
        self.assertIsNotNone(
            match,
            msg=(
                "_run_event_detail_batch must be called with explicit "
                "event_ids=... — that is the no-timestamp-window path "
                "the operator-driven CLI relies on for archive seasons."
            ),
        )


if __name__ == "__main__":
    unittest.main()
