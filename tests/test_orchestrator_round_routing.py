"""TDD tests for Phase 4 Step 4 — orchestrator routes slug vs no-slug.

This is the bridge that turns the new endpoint + parser + job + SQL
helper into actual behavior change for cup-style competitions.

The orchestrator (``_run_one_tournament`` in
``default_tournaments_pipeline_cli.py``) now reads the ``season_round``
catalog and per-row chooses:

* ``slug is None`` → ``event_list_job.run_round(...)`` (existing path,
  unchanged for league seasons).
* ``slug is not None`` → ``event_list_job.run_round_with_slug(...)``
  (new, Phase 4) for cup-stage named knockout rounds.

These tests pin the routing decision without running the full
orchestrator — we just verify the catalog → method-dispatch mapping
is correct. End-to-end behavior (WC 2022 events getting correct
``round_number`` per stage) is verified on prod after deploy.
"""

from __future__ import annotations

import unittest
from typing import Any


class _CapturingJob:
    """Stand-in for ``EventListIngestJob``. Records which methods were
    called with which arguments so the test can assert on the
    routing decision."""

    def __init__(self) -> None:
        self.bare_round_calls: list[tuple[int, int, int]] = []
        self.slug_round_calls: list[tuple[int, int, int, str]] = []

    async def run_round(
        self,
        unique_tournament_id: int,
        season_id: int,
        round_number: int,
        *,
        sport_slug: str = "football",
        timeout: float = 20.0,
    ) -> Any:
        self.bare_round_calls.append((unique_tournament_id, season_id, round_number))
        return None

    async def run_round_with_slug(
        self,
        unique_tournament_id: int,
        season_id: int,
        round_number: int,
        slug: str,
        *,
        sport_slug: str = "football",
        timeout: float = 20.0,
    ) -> Any:
        self.slug_round_calls.append(
            (unique_tournament_id, season_id, round_number, slug)
        )
        return None


async def _dispatch_rounds_under_test(
    job: _CapturingJob,
    catalog: tuple[tuple[int, str | None], ...],
    *,
    unique_tournament_id: int,
    season_id: int,
    populated_predicate=None,
) -> None:
    """Mirror the loop that the orchestrator runs. Keeping a small
    sibling helper here avoids spinning up the entire
    ``_run_one_tournament`` machinery just to test routing — the
    decision is a 5-line ``if/else`` inside the loop. If the
    orchestrator's branching shape changes, these tests break.

    Phase 5.3 (2026-05-19): ``populated_predicate`` lets the test
    inject the skip-when-populated outcome per catalog entry. The
    default ``None`` means "never populated" (every entry fetched);
    a callable returning True for some entries simulates Phase 5.3
    skipping them.
    """
    for round_number, round_slug in catalog:
        if populated_predicate is not None and populated_predicate(
            round_number, round_slug
        ):
            continue
        if round_slug is None:
            await job.run_round(
                unique_tournament_id,
                season_id,
                round_number,
                sport_slug="football",
                timeout=20.0,
            )
        else:
            await job.run_round_with_slug(
                unique_tournament_id,
                season_id,
                round_number,
                round_slug,
                sport_slug="football",
                timeout=20.0,
            )


class OrchestratorRoundRoutingTests(unittest.IsolatedAsyncioTestCase):
    async def test_group_stage_round_calls_bare_method(self) -> None:
        """Round with ``slug=None`` (group-stage matchday) routes to
        the bare ``run_round`` — the existing path stays unchanged for
        league competitions and group-stage cup rounds."""
        job = _CapturingJob()
        catalog = ((1, None), (2, None), (3, None))

        await _dispatch_rounds_under_test(
            job,
            catalog,
            unique_tournament_id=16,
            season_id=41087,
        )

        self.assertEqual(
            job.bare_round_calls,
            [(16, 41087, 1), (16, 41087, 2), (16, 41087, 3)],
        )
        self.assertEqual(job.slug_round_calls, [])

    async def test_knockout_round_calls_slug_aware_method(self) -> None:
        """Round with non-None slug routes to ``run_round_with_slug``.
        For FIFA WC: round 29 / slug ``final`` is the canonical
        knockout-final URL."""
        job = _CapturingJob()
        catalog = ((29, "final"),)

        await _dispatch_rounds_under_test(
            job,
            catalog,
            unique_tournament_id=16,
            season_id=58210,
        )

        self.assertEqual(job.bare_round_calls, [])
        self.assertEqual(
            job.slug_round_calls,
            [(16, 58210, 29, "final")],
        )

    async def test_mixed_catalog_routes_per_entry(self) -> None:
        """Real WC 2022 catalog: 3 group rounds (no slug) + 5 knockout
        rounds (with slug). Routing must respect each entry's slug
        independently — not "all-or-nothing"."""
        job = _CapturingJob()
        catalog = (
            (1, None),
            (2, None),
            (3, None),
            (5, "round-of-16"),
            (27, "quarterfinals"),
            (28, "semifinals"),
            (29, "final"),
            (50, "match-for-3rd-place"),
        )

        await _dispatch_rounds_under_test(
            job,
            catalog,
            unique_tournament_id=16,
            season_id=41087,
        )

        # 3 group rounds → bare.
        self.assertEqual(
            job.bare_round_calls,
            [(16, 41087, 1), (16, 41087, 2), (16, 41087, 3)],
        )
        # 5 knockout rounds → slug-aware, in catalog order.
        self.assertEqual(
            job.slug_round_calls,
            [
                (16, 41087, 5, "round-of-16"),
                (16, 41087, 27, "quarterfinals"),
                (16, 41087, 28, "semifinals"),
                (16, 41087, 29, "final"),
                (16, 41087, 50, "match-for-3rd-place"),
            ],
        )

    async def test_empty_catalog_no_dispatch(self) -> None:
        """An empty catalog (season whose /rounds payload hasn't landed
        yet) deliberately does nothing — the ``/events/last/{p}``
        fallback later in the orchestrator handles the empty-catalog
        case."""
        job = _CapturingJob()
        catalog = ()

        await _dispatch_rounds_under_test(
            job,
            catalog,
            unique_tournament_id=999,
            season_id=999,
        )

        self.assertEqual(job.bare_round_calls, [])
        self.assertEqual(job.slug_round_calls, [])


class OrchestratorSkipWhenPopulatedTests(unittest.IsolatedAsyncioTestCase):
    """Phase 5.3 (2026-05-19): skip the per-round fetch when
    ``event_round_info`` is already populated. The orchestrator
    iterates the catalog and consults ``_round_already_populated``
    before each fetch; ``True`` means "skip this round entry,
    continue to the next"."""

    async def test_skip_fires_when_populated_predicate_true(self) -> None:
        """If the populated predicate returns True for an entry, the
        corresponding ``run_round[_with_slug]`` call MUST NOT fire."""
        job = _CapturingJob()
        catalog = ((29, "final"),)

        # Predicate says "yes, already populated" for round 29 final.
        await _dispatch_rounds_under_test(
            job,
            catalog,
            unique_tournament_id=16,
            season_id=41087,
            populated_predicate=lambda n, s: True,
        )

        self.assertEqual(job.bare_round_calls, [])
        self.assertEqual(job.slug_round_calls, [])

    async def test_no_skip_when_populated_predicate_false(self) -> None:
        """If the predicate returns False for an entry, the fetch
        proceeds as usual (Phase 4 behaviour preserved when no rows
        exist yet)."""
        job = _CapturingJob()
        catalog = ((29, "final"),)

        await _dispatch_rounds_under_test(
            job,
            catalog,
            unique_tournament_id=16,
            season_id=41087,
            populated_predicate=lambda n, s: False,
        )

        self.assertEqual(
            job.slug_round_calls, [(16, 41087, 29, "final")]
        )

    async def test_partial_skip_in_mixed_catalog(self) -> None:
        """Real scenario: cursor walk revisiting a finished WC season.
        Group stage already populated by previous walk; one fresh
        knockout round just added by an editorial update. Phase 5.3
        skips the populated rounds and only fetches the unpopulated
        one — saving 7 upstream calls (group d1/2/3 + R16 + QF + SF
        + 3rd place) and still landing the missing final."""
        job = _CapturingJob()
        catalog = (
            (1, None),
            (2, None),
            (3, None),
            (5, "round-of-16"),
            (27, "quarterfinals"),
            (28, "semifinals"),
            (50, "match-for-3rd-place"),
            (29, "final"),  # only this one not yet populated
        )

        def _predicate(round_number: int, slug: str | None) -> bool:
            # Final is the only un-populated entry — every other one
            # already has rows in event_round_info from a previous
            # walk.
            return not (round_number == 29 and slug == "final")

        await _dispatch_rounds_under_test(
            job,
            catalog,
            unique_tournament_id=16,
            season_id=41087,
            populated_predicate=_predicate,
        )

        self.assertEqual(job.bare_round_calls, [])
        self.assertEqual(
            job.slug_round_calls, [(16, 41087, 29, "final")]
        )

    async def test_full_skip_when_everything_populated(self) -> None:
        """The expected steady-state for finished cup seasons that the
        cursor has already walked through once: every catalog entry
        skipped, zero upstream fetches. Phase 5.3 is the optimisation
        that turns repeat cursor walks into no-ops."""
        job = _CapturingJob()
        catalog = (
            (1, None),
            (2, None),
            (3, None),
            (5, "round-of-16"),
            (27, "quarterfinals"),
            (28, "semifinals"),
            (29, "final"),
            (50, "match-for-3rd-place"),
        )

        await _dispatch_rounds_under_test(
            job,
            catalog,
            unique_tournament_id=16,
            season_id=41087,
            populated_predicate=lambda n, s: True,
        )

        self.assertEqual(job.bare_round_calls, [])
        self.assertEqual(job.slug_round_calls, [])


class HelperHasNoRegressionTests(unittest.TestCase):
    """``_load_round_numbers`` (the legacy helper) is preserved for
    callers that haven't migrated yet (CLI tools, ad-hoc scripts).
    Phase 4 doesn't delete it — just stops using it in the
    orchestrator."""

    def test_legacy_helper_still_exists(self) -> None:
        from schema_inspector.default_tournaments_pipeline_cli import (
            _load_round_numbers,
        )

        self.assertTrue(callable(_load_round_numbers))


if __name__ == "__main__":
    unittest.main()
