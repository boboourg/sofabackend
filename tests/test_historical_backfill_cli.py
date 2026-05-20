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


if __name__ == "__main__":
    unittest.main()
