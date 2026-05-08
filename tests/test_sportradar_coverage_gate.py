"""Tests for SportradarCoverageGate (P3.2)."""

from __future__ import annotations

import asyncio
import unittest

from schema_inspector.services.gate_context_resolver import GateContext
from schema_inspector.services.sportradar_coverage_gate import (
    DEFAULT_DRYRUN_ONLY_ENDPOINTS,
    ENDPOINT_TO_COVERAGE_REQUIREMENT,
    SportradarCoverageGate,
)


class _FakeCoverageLookup:
    """In-memory coverage table fake. Indexed by ut_id."""

    def __init__(self, rows: dict[int, dict[str, object]] | None = None) -> None:
        self.rows = rows or {}
        self.calls: list[int] = []

    async def fetch_for_ut(self, ut_id: int) -> dict[str, object] | None:
        self.calls.append(ut_id)
        return self.rows.get(ut_id)


def _ctx(
    *,
    sport: str | None = "football",
    is_editor: bool = False,
    ut_id: int | None = 17,
    season_id: int | None = 76986,
    event_id: int | None = 12345,
    has_full: bool = True,
    missing_reason: str = "",
) -> GateContext:
    return GateContext(
        sport_slug=sport,
        is_editor=is_editor,
        unique_tournament_id=ut_id,
        season_id=season_id,
        event_id=event_id,
        has_full_context=has_full,
        missing_reason=missing_reason,
    )


def _row(*, attrs: dict[str, int], confidence: float = 1.0) -> dict[str, object]:
    return {
        "coverage_attrs": attrs,
        "reconciliation_confidence": confidence,
    }


class GateDecisionTreeTests(unittest.TestCase):
    """Walks priority order top-to-bottom."""

    def test_step1_kill_switch_disabled_allows_everything(self) -> None:
        lookup = _FakeCoverageLookup({17: _row(attrs={"Lineups": 0}, confidence=1.0)})
        gate = SportradarCoverageGate(coverage_lookup=lookup, env={})  # ENABLED missing → false
        d = asyncio.run(gate.should_fetch(
            path_template="/api/v1/event/{event_id}/lineups",
            ctx=_ctx(),
        ))
        self.assertTrue(d.allow)
        self.assertEqual(d.reason, "gate_disabled")

    def test_step2_missing_context_allows_with_log(self) -> None:
        lookup = _FakeCoverageLookup({})
        gate = SportradarCoverageGate(
            coverage_lookup=lookup,
            env={"SCHEMA_INSPECTOR_SPORTRADAR_GATE_ENABLED": "true"},
        )
        d = asyncio.run(gate.should_fetch(
            path_template="/api/v1/team/{team_id}/players",
            ctx=_ctx(has_full=False, missing_reason="team-level"),
        ))
        self.assertTrue(d.allow)
        self.assertEqual(d.reason, "missing_context")
        self.assertIsNotNone(d.logged_event)

    def test_step3_sport_out_of_scope_allows(self) -> None:
        lookup = _FakeCoverageLookup({})
        gate = SportradarCoverageGate(
            coverage_lookup=lookup,
            env={"SCHEMA_INSPECTOR_SPORTRADAR_GATE_ENABLED": "true"},
        )
        d = asyncio.run(gate.should_fetch(
            path_template="/api/v1/event/{event_id}/lineups",
            ctx=_ctx(sport="basketball"),
        ))
        self.assertTrue(d.allow)
        self.assertEqual(d.reason, "sport_out_of_scope")

    def test_step4_is_editor_true_allows(self) -> None:
        lookup = _FakeCoverageLookup({17: _row(attrs={"Lineups": 0})})
        gate = SportradarCoverageGate(
            coverage_lookup=lookup,
            env={
                "SCHEMA_INSPECTOR_SPORTRADAR_GATE_ENABLED": "true",
                "SCHEMA_INSPECTOR_SPORTRADAR_GATE_DRY_RUN": "false",
            },
        )
        d = asyncio.run(gate.should_fetch(
            path_template="/api/v1/event/{event_id}/lineups",
            ctx=_ctx(is_editor=True),
        ))
        self.assertTrue(d.allow)
        self.assertEqual(d.reason, "is_editor_true")
        self.assertEqual(lookup.calls, [])  # never queried

    def test_step5_path_not_in_mapping_allows(self) -> None:
        lookup = _FakeCoverageLookup({})
        gate = SportradarCoverageGate(
            coverage_lookup=lookup,
            env={"SCHEMA_INSPECTOR_SPORTRADAR_GATE_ENABLED": "true"},
        )
        d = asyncio.run(gate.should_fetch(
            path_template="/api/v1/event/{event_id}/comments",  # sofascore-only
            ctx=_ctx(),
        ))
        self.assertTrue(d.allow)
        self.assertEqual(d.reason, "path_template_not_mapped")

    def test_step6_bypass_attrs_short_circuits_before_db(self) -> None:
        lookup = _FakeCoverageLookup({17: _row(attrs={"Lineups": 0})})
        gate = SportradarCoverageGate(
            coverage_lookup=lookup,
            env={
                "SCHEMA_INSPECTOR_SPORTRADAR_GATE_ENABLED": "true",
                "SCHEMA_INSPECTOR_SPORTRADAR_GATE_DRY_RUN": "false",
                "SCHEMA_INSPECTOR_SPORTRADAR_GATE_BYPASS_ATTRS": "Lineups,Leaders",
            },
        )
        d = asyncio.run(gate.should_fetch(
            path_template="/api/v1/event/{event_id}/lineups",
            ctx=_ctx(),
        ))
        self.assertTrue(d.allow)
        self.assertTrue(d.reason.startswith("bypass_attr:"))
        # Critical: BYPASS_ATTRS must short-circuit BEFORE DB lookup.
        self.assertEqual(lookup.calls, [])

    def test_step7_dryrun_only_endpoint_forces_dryrun_log(self) -> None:
        # top-ratings/overall is in DEFAULT_DRYRUN_ONLY_ENDPOINTS
        path = "/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/top-ratings/overall"
        lookup = _FakeCoverageLookup({17: _row(attrs={"Leaders": 0}, confidence=1.0)})
        gate = SportradarCoverageGate(
            coverage_lookup=lookup,
            env={
                "SCHEMA_INSPECTOR_SPORTRADAR_GATE_ENABLED": "true",
                "SCHEMA_INSPECTOR_SPORTRADAR_GATE_DRY_RUN": "false",  # global LIVE
            },
        )
        d = asyncio.run(gate.should_fetch(path_template=path, ctx=_ctx()))
        # Forced dry-run: would block but allows + logs
        self.assertTrue(d.allow)
        self.assertEqual(d.reason, "dry_run_would_block")
        self.assertIsNotNone(d.logged_event)
        self.assertTrue(d.logged_event["force_dry_run"])

    def test_step8_no_coverage_row_allows(self) -> None:
        lookup = _FakeCoverageLookup({})  # ut 17 not present
        gate = SportradarCoverageGate(
            coverage_lookup=lookup,
            env={"SCHEMA_INSPECTOR_SPORTRADAR_GATE_ENABLED": "true"},
        )
        d = asyncio.run(gate.should_fetch(
            path_template="/api/v1/event/{event_id}/lineups",
            ctx=_ctx(),
        ))
        self.assertTrue(d.allow)
        self.assertEqual(d.reason, "coverage_missing")

    def test_step9_low_confidence_allows(self) -> None:
        lookup = _FakeCoverageLookup({17: _row(attrs={"Lineups": 0}, confidence=0.7)})
        gate = SportradarCoverageGate(
            coverage_lookup=lookup,
            env={"SCHEMA_INSPECTOR_SPORTRADAR_GATE_ENABLED": "true"},
        )
        d = asyncio.run(gate.should_fetch(
            path_template="/api/v1/event/{event_id}/lineups",
            ctx=_ctx(),
        ))
        self.assertTrue(d.allow)
        self.assertTrue(d.reason.startswith("low_confidence:"))

    def test_step10_attr_value_null_allows(self) -> None:
        lookup = _FakeCoverageLookup({17: _row(attrs={"OtherAttr": 2}, confidence=1.0)})
        gate = SportradarCoverageGate(
            coverage_lookup=lookup,
            env={"SCHEMA_INSPECTOR_SPORTRADAR_GATE_ENABLED": "true"},
        )
        d = asyncio.run(gate.should_fetch(
            path_template="/api/v1/event/{event_id}/lineups",
            ctx=_ctx(),
        ))
        self.assertTrue(d.allow)
        self.assertEqual(d.reason, "attr_value_null")

    def test_step11_coverage_adequate_allows(self) -> None:
        lookup = _FakeCoverageLookup({17: _row(attrs={"Lineups": 2}, confidence=1.0)})
        gate = SportradarCoverageGate(
            coverage_lookup=lookup,
            env={
                "SCHEMA_INSPECTOR_SPORTRADAR_GATE_ENABLED": "true",
                "SCHEMA_INSPECTOR_SPORTRADAR_GATE_DRY_RUN": "false",
            },
        )
        d = asyncio.run(gate.should_fetch(
            path_template="/api/v1/event/{event_id}/lineups",
            ctx=_ctx(),
        ))
        self.assertTrue(d.allow)
        self.assertEqual(d.reason, "coverage_adequate")

    def test_step12_global_dryrun_logs_would_block(self) -> None:
        lookup = _FakeCoverageLookup({17: _row(attrs={"Lineups": 0}, confidence=1.0)})
        gate = SportradarCoverageGate(
            coverage_lookup=lookup,
            env={
                "SCHEMA_INSPECTOR_SPORTRADAR_GATE_ENABLED": "true",
                "SCHEMA_INSPECTOR_SPORTRADAR_GATE_DRY_RUN": "true",
            },
        )
        d = asyncio.run(gate.should_fetch(
            path_template="/api/v1/event/{event_id}/lineups",
            ctx=_ctx(),
        ))
        self.assertTrue(d.allow)
        self.assertEqual(d.reason, "dry_run_would_block")
        self.assertEqual(d.logged_event["coverage_value"], 0)
        self.assertEqual(d.logged_event["min_value"], 1)
        self.assertEqual(d.logged_event["required_attr"], "Lineups")

    def test_step13_block_when_coverage_zero_no_dryrun(self) -> None:
        lookup = _FakeCoverageLookup({17: _row(attrs={"Lineups": 0}, confidence=1.0)})
        gate = SportradarCoverageGate(
            coverage_lookup=lookup,
            env={
                "SCHEMA_INSPECTOR_SPORTRADAR_GATE_ENABLED": "true",
                "SCHEMA_INSPECTOR_SPORTRADAR_GATE_DRY_RUN": "false",
            },
        )
        d = asyncio.run(gate.should_fetch(
            path_template="/api/v1/event/{event_id}/lineups",
            ctx=_ctx(),
        ))
        self.assertFalse(d.allow)
        self.assertEqual(d.reason, "coverage_insufficient")
        self.assertIsNotNone(d.logged_event)


class GateConfigurationTests(unittest.TestCase):
    def test_endpoint_to_coverage_requirement_has_all_expected_paths(self) -> None:
        # Per design doc Section 4.2: 11 hard-gate + 1 dry-run-observation = 12
        self.assertEqual(len(ENDPOINT_TO_COVERAGE_REQUIREMENT), 12)
        # Lineups must be there
        self.assertEqual(
            ENDPOINT_TO_COVERAGE_REQUIREMENT["/api/v1/event/{event_id}/lineups"],
            ("Lineups", 1),
        )
        # top-ratings/overall must be there (dry-run observation)
        self.assertIn(
            "/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/top-ratings/overall",
            ENDPOINT_TO_COVERAGE_REQUIREMENT,
        )

    def test_dryrun_only_default_includes_top_ratings(self) -> None:
        self.assertIn(
            "/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/top-ratings/overall",
            DEFAULT_DRYRUN_ONLY_ENDPOINTS,
        )

    def test_min_confidence_env_override(self) -> None:
        lookup = _FakeCoverageLookup({17: _row(attrs={"Lineups": 0}, confidence=0.85)})
        gate = SportradarCoverageGate(
            coverage_lookup=lookup,
            env={
                "SCHEMA_INSPECTOR_SPORTRADAR_GATE_ENABLED": "true",
                "SCHEMA_INSPECTOR_SPORTRADAR_GATE_DRY_RUN": "false",
                "SCHEMA_INSPECTOR_SPORTRADAR_GATE_MIN_CONFIDENCE": "0.7",
            },
        )
        # 0.85 >= 0.7 → blocks (Lineups=0, min=1)
        d = asyncio.run(gate.should_fetch(
            path_template="/api/v1/event/{event_id}/lineups",
            ctx=_ctx(),
        ))
        self.assertFalse(d.allow)

    def test_dryrun_only_endpoints_env_override_replaces_default(self) -> None:
        path = "/api/v1/event/{event_id}/lineups"
        lookup = _FakeCoverageLookup({17: _row(attrs={"Lineups": 0}, confidence=1.0)})
        gate = SportradarCoverageGate(
            coverage_lookup=lookup,
            env={
                "SCHEMA_INSPECTOR_SPORTRADAR_GATE_ENABLED": "true",
                "SCHEMA_INSPECTOR_SPORTRADAR_GATE_DRY_RUN": "false",
                "SCHEMA_INSPECTOR_SPORTRADAR_GATE_DRYRUN_ONLY_ENDPOINTS": path,
            },
        )
        d = asyncio.run(gate.should_fetch(path_template=path, ctx=_ctx()))
        # Now /lineups is forced into dry-run by env override
        self.assertTrue(d.allow)
        self.assertEqual(d.reason, "dry_run_would_block")
        self.assertTrue(d.logged_event["force_dry_run"])

    def test_lookup_exception_falls_open(self) -> None:
        class BrokenLookup:
            calls = []

            async def fetch_for_ut(self, ut_id):
                self.calls.append(ut_id)
                raise RuntimeError("boom")

        gate = SportradarCoverageGate(
            coverage_lookup=BrokenLookup(),
            env={
                "SCHEMA_INSPECTOR_SPORTRADAR_GATE_ENABLED": "true",
                "SCHEMA_INSPECTOR_SPORTRADAR_GATE_DRY_RUN": "false",
            },
        )
        d = asyncio.run(gate.should_fetch(
            path_template="/api/v1/event/{event_id}/lineups",
            ctx=_ctx(),
        ))
        self.assertTrue(d.allow)
        self.assertEqual(d.reason, "lookup_error")

    def test_bypass_sports_emergency_override(self) -> None:
        lookup = _FakeCoverageLookup({17: _row(attrs={"Lineups": 0}, confidence=1.0)})
        gate = SportradarCoverageGate(
            coverage_lookup=lookup,
            env={
                "SCHEMA_INSPECTOR_SPORTRADAR_GATE_ENABLED": "true",
                "SCHEMA_INSPECTOR_SPORTRADAR_GATE_DRY_RUN": "false",
                "SCHEMA_INSPECTOR_SPORTRADAR_GATE_BYPASS_SPORTS": "basketball,tennis",
            },
        )
        # basketball is bypass-listed → gate evaluates as if in scope
        # but ctx.sport_slug=basketball is NOT in scope, so this still passes via step 3.
        d = asyncio.run(gate.should_fetch(
            path_template="/api/v1/event/{event_id}/lineups",
            ctx=_ctx(sport="basketball"),
        ))
        # bypass_sports just allows checking endpoints whose sport is in bypass
        # i.e., these sports get GATED rather than skipped at step 3.
        # Coverage 0 + dry_run=false → block
        self.assertFalse(d.allow)


if __name__ == "__main__":
    unittest.main()
