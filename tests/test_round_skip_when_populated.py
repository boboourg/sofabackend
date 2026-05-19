"""TDD tests for Phase 5.3 — skip ``run_round[_with_slug]`` fetch when
``event_round_info`` is already populated for that (UT, season,
round_number, slug) combination.

Why
---
Phase 4 lands the round structure into ``event_round_info`` on the
first cursor walk through a cup-style (UT, season). On subsequent
walks (cursor revisit when planner re-queues, periodic resets) we
were re-fetching ``/events/round/{N}/slug/{slug}`` and
``/events/round/{N}`` without need — the data is already in DB and
the local API serves the route via Phase 5.2 / existing round
synthesizer.

What
----
Pre-check helper ``_round_already_populated`` (boolean) + a guard
in the orchestrator's per-round loop:

  for round_number, round_slug in catalog:
      if await _round_already_populated(...):
          capabilities.add(ROUNDS)
          continue
      if round_slug is None: await run_round(...)
      else:                  await run_round_with_slug(...)

The capability is still marked when the skip fires — events DID land
for that combo, just not in this run. Cursor advance gate (Fix 1)
relies on ROUNDS capability being set when events are present, and a
no-op skip is just as valid evidence of "rounds completed" as a
successful fetch.

Edge case: group-stage rows have ``slug IS NULL``. The check uses
``IS NOT DISTINCT FROM`` so ``NULL = NULL`` evaluates true, matching
the orchestrator's parameter binding for both flows.
"""

from __future__ import annotations

import unittest


class RoundAlreadyPopulatedHelperContractTests(unittest.IsolatedAsyncioTestCase):
    async def test_passes_all_four_args(self) -> None:
        from schema_inspector.default_tournaments_pipeline_cli import (
            _round_already_populated,
        )

        captured: dict[str, object] = {}

        class _StubConn:
            async def fetchval(self, query: str, *args: object):
                captured["query"] = query
                captured["args"] = args
                return False

        await _round_already_populated(
            _StubConn(),
            unique_tournament_id=16,
            season_id=41087,
            round_number=29,
            round_slug="final",
        )

        self.assertEqual(captured["args"], (16, 41087, 29, "final"))

    async def test_query_uses_event_round_info(self) -> None:
        from schema_inspector.default_tournaments_pipeline_cli import (
            _round_already_populated,
        )

        captured: dict[str, object] = {}

        class _StubConn:
            async def fetchval(self, query: str, *args: object):
                captured["query"] = query
                return False

        await _round_already_populated(
            _StubConn(),
            unique_tournament_id=1,
            season_id=1,
            round_number=1,
            round_slug=None,
        )
        query = str(captured["query"])
        self.assertIn("event_round_info", query)

    async def test_query_uses_is_not_distinct_from_for_slug(self) -> None:
        """The orchestrator passes ``slug=None`` for group-stage
        catalog entries. SQL ``=`` is NULL-unsafe, so the helper must
        use ``IS NOT DISTINCT FROM`` (treats NULL=NULL as equal)
        otherwise the group-stage skip never fires."""
        from schema_inspector.default_tournaments_pipeline_cli import (
            _round_already_populated,
        )

        captured: dict[str, object] = {}

        class _StubConn:
            async def fetchval(self, query: str, *args: object):
                captured["query"] = query
                return False

        await _round_already_populated(
            _StubConn(),
            unique_tournament_id=1,
            season_id=1,
            round_number=1,
            round_slug=None,
        )
        query = str(captured["query"])
        self.assertIn("IS NOT DISTINCT FROM", query)

    async def test_returns_true_when_row_exists(self) -> None:
        from schema_inspector.default_tournaments_pipeline_cli import (
            _round_already_populated,
        )

        class _StubConn:
            async def fetchval(self, query: str, *args: object):
                return True  # asyncpg returns the EXISTS result directly

        result = await _round_already_populated(
            _StubConn(),
            unique_tournament_id=16,
            season_id=41087,
            round_number=29,
            round_slug="final",
        )
        self.assertTrue(result)

    async def test_returns_false_when_no_row(self) -> None:
        from schema_inspector.default_tournaments_pipeline_cli import (
            _round_already_populated,
        )

        class _StubConn:
            async def fetchval(self, query: str, *args: object):
                return False

        result = await _round_already_populated(
            _StubConn(),
            unique_tournament_id=1,
            season_id=1,
            round_number=1,
            round_slug=None,
        )
        self.assertFalse(result)

    async def test_returns_false_when_fetchval_returns_none(self) -> None:
        """asyncpg sometimes returns ``None`` from ``fetchval`` for
        boolean SELECTs that don't match. Helper normalises to False
        so the caller never has to guard against tri-state."""
        from schema_inspector.default_tournaments_pipeline_cli import (
            _round_already_populated,
        )

        class _StubConn:
            async def fetchval(self, query: str, *args: object):
                return None

        result = await _round_already_populated(
            _StubConn(),
            unique_tournament_id=1,
            season_id=1,
            round_number=1,
            round_slug=None,
        )
        self.assertFalse(result)


class OrchestratorSkipBehaviorTests(unittest.IsolatedAsyncioTestCase):
    """The orchestrator's catalog loop should skip the fetch when the
    helper returns True, and still mark the ROUNDS capability."""

    async def test_skip_fires_for_populated_entry(self) -> None:
        """Sibling helper used in the orchestrator: for each catalog
        entry, if ``_round_already_populated`` returns True the per-
        round fetch is skipped. We test the skip *decision* (not the
        full orchestrator) — the actual call wiring is verified
        end-to-end via integration tests."""
        from schema_inspector.default_tournaments_pipeline_cli import (
            _should_skip_round_fetch,
        )

        # Trivial wrapper test: the orchestrator currently embeds the
        # if/else routing inline. ``_should_skip_round_fetch`` is the
        # thin decision adapter that lets us unit-test the boolean
        # outcome without spinning up the database / job runtime.

        async def _populated_yes(*args, **kwargs):
            return True

        async def _populated_no(*args, **kwargs):
            return False

        self.assertTrue(
            await _should_skip_round_fetch(
                populated_check=_populated_yes,
                connection=None,
                unique_tournament_id=16,
                season_id=41087,
                round_number=29,
                round_slug="final",
            )
        )
        self.assertFalse(
            await _should_skip_round_fetch(
                populated_check=_populated_no,
                connection=None,
                unique_tournament_id=16,
                season_id=41087,
                round_number=29,
                round_slug="final",
            )
        )


if __name__ == "__main__":
    unittest.main()
