"""TDD tests for ``schema_inspector.services.season_capabilities``.

This module formalizes the cursor-advance gate as a capability
dependency graph, replacing the legacy ``stage_failures == 0`` rule
(2026-05-16) and the subsequent ``discovered_event_ids > 0`` hack
(2026-05-18). The capability model gives:

* explicit list of season-level capabilities (events / rounds /
  standings / brackets / leaderboards / statistics);
* a single source of truth for what must complete before the historical
  cursor walks to the next season;
* a parameterless ``required_capabilities_for_cursor_advance()`` that
  callers use without knowing season-type detection — when we extend
  later (e.g. per-sport required sets), the change is local to this
  module.

The contract is intentionally minimal in v1: only ``EVENTS`` is
required. This pins the FIFA WC 2022 fix as deliberate behavior, not
an accident — cup-style competitions advance even when standings /
leaderboards / round_events return 404, because those capabilities are
*optional* at the season level.
"""

from __future__ import annotations

import unittest


class CapabilityConstantsTests(unittest.TestCase):
    """Each capability has a stable string name. The names are the keys
    used in ``_WorkerResult.capabilities_completed`` set the worker
    reads when deciding to advance the cursor — they must NOT drift."""

    def test_events_constant_is_string(self) -> None:
        from schema_inspector.services.season_capabilities import EVENTS

        self.assertEqual(EVENTS, "events")

    def test_rounds_constant_is_string(self) -> None:
        from schema_inspector.services.season_capabilities import ROUNDS

        self.assertEqual(ROUNDS, "rounds")

    def test_brackets_constant_is_string(self) -> None:
        from schema_inspector.services.season_capabilities import BRACKETS

        self.assertEqual(BRACKETS, "brackets")

    def test_standings_constant_is_string(self) -> None:
        from schema_inspector.services.season_capabilities import STANDINGS

        self.assertEqual(STANDINGS, "standings")

    def test_leaderboards_constant_is_string(self) -> None:
        from schema_inspector.services.season_capabilities import LEADERBOARDS

        self.assertEqual(LEADERBOARDS, "leaderboards")

    def test_statistics_constant_is_string(self) -> None:
        from schema_inspector.services.season_capabilities import STATISTICS

        self.assertEqual(STATISTICS, "statistics")


class KnownCapabilitiesTests(unittest.TestCase):
    """``known_capabilities()`` returns the canonical set of all
    capability names. Used to validate user-supplied sets at the
    orchestrator ↔ worker boundary, and to enumerate capabilities for
    monitoring / health checks."""

    def test_returns_frozenset(self) -> None:
        from schema_inspector.services.season_capabilities import known_capabilities

        self.assertIsInstance(known_capabilities(), frozenset)

    def test_contains_events(self) -> None:
        from schema_inspector.services.season_capabilities import (
            EVENTS,
            known_capabilities,
        )

        self.assertIn(EVENTS, known_capabilities())

    def test_contains_all_module_constants(self) -> None:
        from schema_inspector.services.season_capabilities import (
            BRACKETS,
            EVENTS,
            LEADERBOARDS,
            ROUNDS,
            STANDINGS,
            STATISTICS,
            known_capabilities,
        )

        kc = known_capabilities()
        self.assertIn(EVENTS, kc)
        self.assertIn(ROUNDS, kc)
        self.assertIn(BRACKETS, kc)
        self.assertIn(STANDINGS, kc)
        self.assertIn(LEADERBOARDS, kc)
        self.assertIn(STATISTICS, kc)

    def test_set_is_immutable(self) -> None:
        """Callers must not be able to mutate the canonical set."""
        from schema_inspector.services.season_capabilities import known_capabilities

        kc = known_capabilities()
        with self.assertRaises(AttributeError):
            kc.add("malicious_capability")  # type: ignore[attr-defined]


class RequiredCapabilitiesForCursorAdvanceTests(unittest.TestCase):
    """The advance gate: which capabilities must be completed before
    the historical cursor moves to the next season.

    v1: only ``EVENTS``. This intentionally permits cup-style
    competitions to advance after the /events/last/{p} fallback
    succeeded even when standings/leaderboards/round_events 404'd.
    """

    def test_returns_frozenset(self) -> None:
        from schema_inspector.services.season_capabilities import (
            required_capabilities_for_cursor_advance,
        )

        result = required_capabilities_for_cursor_advance()
        self.assertIsInstance(result, frozenset)

    def test_includes_events(self) -> None:
        """A season must have collected at least one event before its
        cursor walks forward. This is the bare minimum."""
        from schema_inspector.services.season_capabilities import (
            EVENTS,
            required_capabilities_for_cursor_advance,
        )

        self.assertIn(EVENTS, required_capabilities_for_cursor_advance())

    def test_does_not_require_standings(self) -> None:
        """Cup-style competitions (FIFA WC) 404 on standings. Standings
        MUST NOT be required for advance — that was the 2026-05-18 bug."""
        from schema_inspector.services.season_capabilities import (
            STANDINGS,
            required_capabilities_for_cursor_advance,
        )

        self.assertNotIn(STANDINGS, required_capabilities_for_cursor_advance())

    def test_does_not_require_leaderboards(self) -> None:
        """Leaderboards can 404 (TLS issues, cup competitions). Optional."""
        from schema_inspector.services.season_capabilities import (
            LEADERBOARDS,
            required_capabilities_for_cursor_advance,
        )

        self.assertNotIn(LEADERBOARDS, required_capabilities_for_cursor_advance())

    def test_does_not_require_rounds(self) -> None:
        """Cup competitions have no /events/round/{N} structure. Optional."""
        from schema_inspector.services.season_capabilities import (
            ROUNDS,
            required_capabilities_for_cursor_advance,
        )

        self.assertNotIn(ROUNDS, required_capabilities_for_cursor_advance())

    def test_required_set_is_subset_of_known(self) -> None:
        """All required capabilities must be in the canonical known set
        — protects against typos in the required set definition."""
        from schema_inspector.services.season_capabilities import (
            known_capabilities,
            required_capabilities_for_cursor_advance,
        )

        self.assertTrue(
            required_capabilities_for_cursor_advance().issubset(known_capabilities())
        )

    def test_minimal_set_is_just_events(self) -> None:
        """Pin v1 minimum exactly — adding more required capabilities
        is a deliberate, test-modifying decision."""
        from schema_inspector.services.season_capabilities import (
            EVENTS,
            required_capabilities_for_cursor_advance,
        )

        self.assertEqual(
            required_capabilities_for_cursor_advance(),
            frozenset({EVENTS}),
        )


if __name__ == "__main__":
    unittest.main()
