"""Phase 4.7.3 (2026-05-23): wire ``football_detail_endpoint_allowed`` into
the orchestrator's detail-spec pipeline.

The gate function itself has accepted ``capability_verdict: str | None``
since Phase 4.7 (commit ``04ba7a7``). What was missing was the wiring:

  * ``filter_football_detail_specs`` discarded the verdict entirely — every
    spec was filtered by legacy tier-based logic alone.
  * ``build_event_detail_request_specs`` had no way to surface per-endpoint
    verdicts from a caller.
  * ``PilotOrchestrator`` never resolved verdicts for the detail patterns
    in its 2 ``build_event_detail_request_specs(...)`` call sites
    (live_delta path ~line 998 and full hydration path ~line 1207).

This file proves the wire end-to-end:

  1. ``filter_football_detail_specs`` accepts ``capability_verdicts: dict[
     str, str] | None``, uses ``.get(endpoint_pattern)`` to look up the
     verdict for each spec, and passes it to ``football_detail_endpoint_allowed``.
  2. ``build_event_detail_request_specs`` accepts the same kwarg and
     pipes it through ``_filter_specs``.
  3. ``PilotOrchestrator._resolve_detail_capability_verdicts(...)`` exists,
     returns ``{}`` when the flag is OFF or registry is None, and returns
     a populated dict when ON.
  4. AST-strap: every ``build_event_detail_request_specs(...)`` call inside
     ``PilotOrchestrator`` passes ``capability_verdicts=`` (regression
     guard against future code paths reverting the wire).
"""

from __future__ import annotations

import ast
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock


_VERDICT_ALLOWED = "allowed"
_VERDICT_DISABLED = "disabled"


def _spec(pattern: str):
    """Lightweight EventDetailRequestSpec-like object — the policy filter
    only reads ``.endpoint.pattern``."""
    return SimpleNamespace(endpoint=SimpleNamespace(pattern=pattern))


# ---------------------------------------------------------------------------
# 1. filter_football_detail_specs accepts and uses capability_verdicts
# ---------------------------------------------------------------------------


class FilterFootballDetailSpecsCapabilityVerdictsTests(unittest.TestCase):
    """Pure-functional fanout filter must consult ``capability_verdicts.get(
    pattern)`` for each spec and pass it down to the gate. Without this,
    the dict from the orchestrator is silently dropped on the floor and
    Phase 4.7.3 is a no-op."""

    def test_disabled_verdict_drops_the_spec(self) -> None:
        from schema_inspector.match_center_policy import filter_football_detail_specs

        # Use a real allowlisted pattern so the *legacy* path would allow it
        # (otherwise we can't observe the verdict overriding anything).
        pattern = "/api/v1/event/{event_id}/incidents"
        result = filter_football_detail_specs(
            [_spec(pattern)],
            sport_slug="football",
            detail_id=1,              # tier_1, legacy = allowed
            status_type="inprogress",
            has_xg=True,
            has_event_player_heat_map=True,
            has_event_player_statistics=True,
            has_global_highlights=None,
            start_timestamp=0,
            now_timestamp=0,
            is_editor=False,
            capability_verdicts={pattern: _VERDICT_DISABLED},
        )
        self.assertEqual(
            result, (),
            "capability_verdict='disabled' must drop the spec even when "
            "legacy tier_1 logic would allow it",
        )

    def test_allowed_verdict_keeps_the_spec_even_against_legacy_denial(self) -> None:
        from schema_inspector.match_center_policy import filter_football_detail_specs

        # Pick a pattern that legacy would deny for tier_5 + finished:
        # /comments requires tier_1.
        pattern = "/api/v1/event/{event_id}/comments"
        spec = _spec(pattern)
        result = filter_football_detail_specs(
            [spec],
            sport_slug="football",
            detail_id=None,           # tier_5 → comments DENIED by legacy
            status_type="finished",
            has_xg=None,
            has_event_player_heat_map=None,
            has_event_player_statistics=None,
            has_global_highlights=None,
            start_timestamp=0,
            now_timestamp=0,
            is_editor=False,
            capability_verdicts={pattern: _VERDICT_ALLOWED},
        )
        self.assertEqual(
            result, (spec,),
            "capability_verdict='allowed' must keep the spec even when "
            "legacy tier_5 logic would deny",
        )

    def test_missing_pattern_in_dict_falls_back_to_legacy(self) -> None:
        """Patterns not present in the registry result come through as
        ``.get(pattern) == None`` — which keeps legacy gating intact.
        Without this fallback, a half-populated registry would silently
        deny all unknown endpoints."""
        from schema_inspector.match_center_policy import filter_football_detail_specs

        # /comments is in _FOOTBALL_INPROGRESS_DETAIL_ENDPOINTS and is
        # allowed by legacy logic for tier_1 inprogress matches.
        pattern = "/api/v1/event/{event_id}/comments"
        spec = _spec(pattern)
        result = filter_football_detail_specs(
            [spec],
            sport_slug="football",
            detail_id=1,
            status_type="inprogress",
            has_xg=True,
            has_event_player_heat_map=True,
            has_event_player_statistics=True,
            has_global_highlights=None,
            start_timestamp=0,
            now_timestamp=0,
            is_editor=False,
            capability_verdicts={"/some/other/pattern": _VERDICT_DISABLED},
        )
        self.assertEqual(result, (spec,))

    def test_none_capability_verdicts_kwarg_is_identity_with_legacy(self) -> None:
        """Default ``capability_verdicts=None`` (caller didn't pass it) is
        backwards-compatible: behaves exactly like pre-Phase-4.7.3."""
        from schema_inspector.match_center_policy import filter_football_detail_specs

        pattern = "/api/v1/event/{event_id}/comments"
        spec = _spec(pattern)
        result = filter_football_detail_specs(
            [spec],
            sport_slug="football",
            detail_id=1,
            status_type="inprogress",
            has_xg=True,
            has_event_player_heat_map=True,
            has_event_player_statistics=True,
            has_global_highlights=None,
            start_timestamp=0,
            now_timestamp=0,
            is_editor=False,
        )
        self.assertEqual(result, (spec,))


# ---------------------------------------------------------------------------
# 2. build_event_detail_request_specs accepts and pipes capability_verdicts
# ---------------------------------------------------------------------------


class BuildEventDetailRequestSpecsCapabilityVerdictsTests(unittest.TestCase):
    def test_build_event_detail_request_specs_accepts_capability_verdicts(self) -> None:
        import inspect
        from schema_inspector.detail_resource_policy import (
            build_event_detail_request_specs,
        )

        sig = inspect.signature(build_event_detail_request_specs)
        self.assertIn(
            "capability_verdicts", sig.parameters,
            "build_event_detail_request_specs must accept "
            "capability_verdicts kwarg (Phase 4.7.3 wire surface)",
        )
        self.assertIs(
            sig.parameters["capability_verdicts"].default, None,
            "default must be None (backwards-compatible)",
        )

    def test_build_event_detail_request_specs_drops_disabled_pattern(self) -> None:
        """End-to-end through build_event_detail_request_specs: a
        capability_verdict='disabled' for /comments must remove that spec
        from the output even when legacy tier-based logic would allow it."""
        from schema_inspector.detail_resource_policy import (
            build_event_detail_request_specs,
        )

        # tier_1 (detail_id=1) finished allows /comments under legacy logic.
        # Override with capability_verdicts and confirm /comments is dropped.
        specs_without_verdict = build_event_detail_request_specs(
            sport_slug="football",
            status_type="finished",
            team_ids=(),
            detail_id=1,
            has_xg=True,
            has_event_player_heat_map=True,
            has_event_player_statistics=True,
            has_global_highlights=None,
            start_timestamp=0,
            now_timestamp=0,
        )
        patterns_legacy = {
            getattr(spec.endpoint, "pattern", "")
            for spec in specs_without_verdict
        }
        self.assertIn(
            "/api/v1/event/{event_id}/comments", patterns_legacy,
            "sanity: legacy tier_1 must include /comments — if this fails "
            "the policy changed and this test needs a different pattern",
        )

        specs_with_verdict = build_event_detail_request_specs(
            sport_slug="football",
            status_type="finished",
            team_ids=(),
            detail_id=1,
            has_xg=True,
            has_event_player_heat_map=True,
            has_event_player_statistics=True,
            has_global_highlights=None,
            start_timestamp=0,
            now_timestamp=0,
            capability_verdicts={
                "/api/v1/event/{event_id}/comments": _VERDICT_DISABLED,
            },
        )
        patterns_gated = {
            getattr(spec.endpoint, "pattern", "")
            for spec in specs_with_verdict
        }
        self.assertNotIn(
            "/api/v1/event/{event_id}/comments", patterns_gated,
            "capability_verdict='disabled' must drop /comments from "
            "build_event_detail_request_specs output",
        )


# ---------------------------------------------------------------------------
# 3. PilotOrchestrator._resolve_detail_capability_verdicts behaviour
# ---------------------------------------------------------------------------


class PilotOrchestratorResolveDetailCapabilityVerdictsTests(
    unittest.IsolatedAsyncioTestCase
):
    """The helper queries the registry for every detail pattern allowed at
    the current status, collects results, and returns ``{}`` whenever the
    feature is off — so the build_event_detail_request_specs call is
    backwards-compatible by default."""

    def _make_orchestrator(self, *, registry):
        """Synthesise a bare orchestrator with only the attributes the
        helper reads. Constructing the real one pulls in Redis + asyncpg —
        we just need ``self.league_capabilities``."""
        from schema_inspector.pipeline.pilot_orchestrator import PilotOrchestrator

        orchestrator = PilotOrchestrator.__new__(PilotOrchestrator)
        orchestrator.league_capabilities = registry
        return orchestrator

    async def test_returns_empty_dict_when_flag_off(self) -> None:
        from unittest.mock import patch

        orch = self._make_orchestrator(registry=None)
        with patch.dict("os.environ", {}, clear=False):
            import os
            os.environ.pop("SOFASCORE_LEAGUE_CAPABILITIES_ENABLED", None)
            result = await orch._resolve_detail_capability_verdicts(
                unique_tournament_id=17,
                season_id=61643,
                status_type="finished",
            )
        self.assertEqual(result, {})

    async def test_returns_empty_dict_when_registry_none(self) -> None:
        from unittest.mock import patch

        orch = self._make_orchestrator(registry=None)
        with patch.dict("os.environ", {"SOFASCORE_LEAGUE_CAPABILITIES_ENABLED": "true"}):
            result = await orch._resolve_detail_capability_verdicts(
                unique_tournament_id=17,
                season_id=61643,
                status_type="finished",
            )
        self.assertEqual(result, {})

    async def test_populates_dict_from_registry_when_flag_on(self) -> None:
        """When the flag is on AND a registry is wired, every detail
        pattern allowed at the current status gets resolved via the
        registry. Phase 4.7.4 moved this from a per-pattern loop to a
        single ``get_verdicts_batch`` call — the registry contract for
        this test now mocks the batch API."""
        from unittest.mock import patch
        from schema_inspector.services.league_capabilities_registry import (
            EndpointVerdict,
        )

        async def _fake_batch(**kw):
            return {
                pattern: (
                    EndpointVerdict.DISABLED
                    if "comments" in pattern
                    else EndpointVerdict.ALLOWED
                )
                for pattern in kw["endpoint_patterns"]
            }

        fake_registry = SimpleNamespace(
            get_verdicts_batch=AsyncMock(side_effect=_fake_batch),
        )

        orch = self._make_orchestrator(registry=fake_registry)
        with patch.dict("os.environ", {"SOFASCORE_LEAGUE_CAPABILITIES_ENABLED": "true"}):
            result = await orch._resolve_detail_capability_verdicts(
                unique_tournament_id=17,
                season_id=61643,
                status_type="finished",
            )

        # We expect at least one disabled (/comments) and several allowed.
        self.assertGreater(len(result), 0)
        comments_pattern = "/api/v1/event/{event_id}/comments"
        self.assertIn(comments_pattern, result)
        self.assertEqual(result[comments_pattern], _VERDICT_DISABLED)


# ---------------------------------------------------------------------------
# 4. AST regression guard — both build_event_detail_request_specs sites
#    inside PilotOrchestrator must pass capability_verdicts=...
# ---------------------------------------------------------------------------


class PilotOrchestratorWiresCapabilityVerdictsTests(unittest.TestCase):
    def _orchestrator_module_path(self) -> Path:
        return (
            Path(__file__).resolve().parent.parent
            / "schema_inspector" / "pipeline" / "pilot_orchestrator.py"
        )

    def _build_specs_call_sites(self) -> list[ast.Call]:
        tree = ast.parse(self._orchestrator_module_path().read_text(encoding="utf-8-sig"))
        sites: list[ast.Call] = []
        for node in ast.walk(tree):
            if isinstance(node, ast.Call):
                func = node.func
                name = (
                    func.attr if isinstance(func, ast.Attribute)
                    else (func.id if isinstance(func, ast.Name) else None)
                )
                if name == "build_event_detail_request_specs":
                    sites.append(node)
        return sites

    def test_at_least_two_build_call_sites_exist(self) -> None:
        sites = self._build_specs_call_sites()
        self.assertGreaterEqual(
            len(sites), 2,
            f"Expected ≥2 build_event_detail_request_specs sites in "
            f"pilot_orchestrator.py, found {len(sites)}",
        )

    def test_every_build_call_passes_capability_verdicts(self) -> None:
        sites = self._build_specs_call_sites()
        missing: list[int] = []
        for call in sites:
            arg = next(
                (kw for kw in call.keywords if kw.arg == "capability_verdicts"),
                None,
            )
            if arg is None:
                missing.append(call.lineno)
        self.assertFalse(
            missing,
            f"build_event_detail_request_specs(...) call sites missing "
            f"`capability_verdicts=` kwarg at lines: {missing}. "
            f"Without it the Phase 4.7.3 wire is half-done — the gate "
            f"function accepts the verdict but never sees it.",
        )


if __name__ == "__main__":
    unittest.main()
