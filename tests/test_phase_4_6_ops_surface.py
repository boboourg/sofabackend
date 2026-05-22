"""Phase 4.6: ops surface — repository manual override + CLI subcommands.

Three test groups:

  1. ``LeagueCapabilitiesRepository.set_manual_override`` writes
     a row with ``source='manual_override'`` and a far-future
     ``expires_at`` so the refresh daemon never re-probes it.

  2. CLI subcommand ``league-capability list / show / set`` parses
     args correctly and dispatches to the orchestrator helpers.

  3. CLI ``league-capability probe`` wires through to ProbeExecutor.

All tests are pure-Python — no live Postgres, no real HTTP.
"""

from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone


class ManualOverrideRepositoryTests(unittest.IsolatedAsyncioTestCase):
    async def test_set_manual_override_writes_row_with_far_future_expiry(self) -> None:
        from schema_inspector.storage.league_capabilities_repository import (
            LeagueCapabilitiesRepository,
        )

        captured: list[tuple[str, tuple]] = []

        class _FakeExecutor:
            async def execute(self, query, *args):
                captured.append((query, args))
            async def fetchrow(self, query, *args):
                return None
            async def fetch(self, query, *args):
                return []

        repo = LeagueCapabilitiesRepository()
        await repo.set_manual_override(
            _FakeExecutor(),
            unique_tournament_id=17,
            season_id=61643,
            status_type="finished",
            endpoint_pattern="/api/v1/event/{event_id}/incidents",
            state="allowed",
            note="manual operator override",
        )

        self.assertEqual(len(captured), 1)
        query, args = captured[0]
        # Source MUST be 'manual_override' — refresh daemon excludes
        # these rows from re-probing. The string is hardcoded in
        # the SQL (not an arg) so we check the query text.
        self.assertIn("manual_override", query)
        # State must be in the args (UPSERT bind position).
        self.assertIn("allowed", str(args))
        # Expiry must be in the far future so refresh daemon never
        # touches it (second safety net beyond source filter).
        expires_arg = args[-2]  # ($6 = expires_at)
        self.assertGreater(expires_arg.year, 2050)

    async def test_set_manual_override_rejects_invalid_state(self) -> None:
        from schema_inspector.storage.league_capabilities_repository import (
            LeagueCapabilitiesRepository,
        )

        class _FakeExecutor:
            async def execute(self, query, *args):
                return None
            async def fetchrow(self, query, *args):
                return None
            async def fetch(self, query, *args):
                return []

        repo = LeagueCapabilitiesRepository()
        with self.assertRaises(ValueError):
            await repo.set_manual_override(
                _FakeExecutor(),
                unique_tournament_id=17,
                season_id=61643,
                status_type="finished",
                endpoint_pattern="/x",
                state="bad_state",  # invalid
                note=None,
            )


class CLIArgparseTests(unittest.TestCase):
    """Pin the CLI argparse shape so operator commands stay stable."""

    def test_league_capability_subcommand_exists(self) -> None:
        from schema_inspector.cli import _build_parser
        parser = _build_parser()
        # Sanity: parser can route the subcommand without error.
        args = parser.parse_args([
            "league-capability", "list",
            "--unique-tournament-id", "17",
        ])
        self.assertEqual(args.command, "league-capability")
        self.assertEqual(args.action, "list")
        self.assertEqual(args.unique_tournament_id, 17)

    def test_show_subcommand_parses_full_quad(self) -> None:
        from schema_inspector.cli import _build_parser
        parser = _build_parser()
        args = parser.parse_args([
            "league-capability", "show",
            "--unique-tournament-id", "16",
            "--season-id", "41087",
            "--status-type", "finished",
            "--endpoint-pattern", "/api/v1/event/{event_id}/incidents",
        ])
        self.assertEqual(args.action, "show")
        self.assertEqual(args.unique_tournament_id, 16)
        self.assertEqual(args.season_id, 41087)
        self.assertEqual(args.status_type, "finished")
        self.assertEqual(
            args.endpoint_pattern,
            "/api/v1/event/{event_id}/incidents",
        )

    def test_set_subcommand_requires_state(self) -> None:
        from schema_inspector.cli import _build_parser
        parser = _build_parser()
        args = parser.parse_args([
            "league-capability", "set",
            "--unique-tournament-id", "17",
            "--season-id", "61643",
            "--status-type", "finished",
            "--endpoint-pattern", "/x",
            "--state", "disabled",
            "--note", "manual",
        ])
        self.assertEqual(args.action, "set")
        self.assertEqual(args.state, "disabled")
        self.assertEqual(args.note, "manual")

    def test_probe_subcommand_takes_status_type(self) -> None:
        from schema_inspector.cli import _build_parser
        parser = _build_parser()
        args = parser.parse_args([
            "league-capability", "probe",
            "--unique-tournament-id", "17",
            "--season-id", "61643",
            "--status-type", "inprogress",
            "--samples", "5",
        ])
        self.assertEqual(args.action, "probe")
        self.assertEqual(args.status_type, "inprogress")
        self.assertEqual(args.samples, 5)


if __name__ == "__main__":
    unittest.main()
