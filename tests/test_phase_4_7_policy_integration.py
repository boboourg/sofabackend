"""Phase 4.7: policy + orchestrator integration with feature flag.

Three test groups:

  1. ``football_edge_allowed`` accepts optional ``capability_verdict``
     and honours it as the authoritative answer when present:
       * 'allowed'  → True (overrides legacy tier_5 denial).
       * 'disabled' → False (overrides legacy allowance).
       * 'unknown' / None → falls back to legacy tier-based logic.
     Editor HARD-BAN still wins over any capability verdict.

  2. ``football_detail_endpoint_allowed`` does the same.

  3. Feature flag wiring — ``is_league_capabilities_enabled()`` reads
     ``SOFASCORE_LEAGUE_CAPABILITIES_ENABLED`` env, defaulting OFF
     (no behaviour change until operator explicitly turns it on).

All tests are pure-Python — no live infra.
"""

from __future__ import annotations

import os
import unittest
from unittest.mock import patch


class FootballEdgeAllowedCapabilityTests(unittest.TestCase):
    def test_capability_allowed_overrides_legacy_tier_5_denial(self) -> None:
        from schema_inspector.match_center_policy import football_edge_allowed
        # Legacy: tier_5 (detail_id=None) finished → statistics DISALLOWED.
        # Registry verdict 'allowed' must override that.
        result = football_edge_allowed(
            sport_slug="football",
            edge_kind="statistics",
            detail_id=None,           # → tier_5
            status_type="finished",
            has_xg=None,
            is_editor=False,
            capability_verdict="allowed",
        )
        self.assertTrue(result)

    def test_capability_disabled_overrides_legacy_tier_1_allowance(self) -> None:
        from schema_inspector.match_center_policy import football_edge_allowed
        # Legacy: tier_1 finished + statistics → ALLOWED.
        # Registry verdict 'disabled' must override (we measured it doesn't exist).
        result = football_edge_allowed(
            sport_slug="football",
            edge_kind="statistics",
            detail_id=1,              # → tier_1
            status_type="finished",
            has_xg=True,
            is_editor=False,
            capability_verdict="disabled",
        )
        self.assertFalse(result)

    def test_capability_unknown_falls_back_to_legacy(self) -> None:
        from schema_inspector.match_center_policy import football_edge_allowed
        # 'unknown' must NOT short-circuit. Legacy tier_5 + statistics →
        # False per the existing rule.
        result = football_edge_allowed(
            sport_slug="football",
            edge_kind="statistics",
            detail_id=None,
            status_type="finished",
            has_xg=None,
            is_editor=False,
            capability_verdict="unknown",
        )
        self.assertFalse(result, "unknown must fall back to legacy tier_5 denial")

    def test_capability_none_default_keeps_legacy_behaviour(self) -> None:
        """Default kwarg (no capability_verdict passed) must be identity
        with pre-Phase-4.7 behaviour. Backwards-compat invariant."""
        from schema_inspector.match_center_policy import football_edge_allowed
        result = football_edge_allowed(
            sport_slug="football",
            edge_kind="incidents",
            detail_id=None,
            status_type="inprogress",
            has_xg=None,
            is_editor=False,
        )
        self.assertTrue(result, "incidents must stay allowed for tier_5 (X'' unlock)")

    def test_editor_hard_ban_wins_over_capability_allowed(self) -> None:
        """Operator-published events (is_editor=True) are NEVER fetched
        downstream of ROOT, regardless of registry verdict."""
        from schema_inspector.match_center_policy import football_edge_allowed
        result = football_edge_allowed(
            sport_slug="football",
            edge_kind="statistics",
            detail_id=1,
            status_type="inprogress",
            has_xg=True,
            is_editor=True,                # HARD BAN
            capability_verdict="allowed",   # registry says ok
        )
        self.assertFalse(result, "is_editor HARD BAN wins over capability")


class FootballDetailEndpointAllowedCapabilityTests(unittest.TestCase):
    def test_capability_allowed_overrides_legacy_denial(self) -> None:
        from schema_inspector.match_center_policy import (
            football_detail_endpoint_allowed,
        )
        # Use a real pattern from the function's allowlist — pick one
        # that gets denied by legacy tier_5 for finished.
        result = football_detail_endpoint_allowed(
            sport_slug="football",
            endpoint_pattern="/api/v1/event/{event_id}/comments",
            detail_id=None,
            status_type="finished",
            has_xg=None,
            has_event_player_heat_map=None,
            has_event_player_statistics=None,
            has_global_highlights=None,
            start_timestamp=0,
            now_timestamp=0,
            capability_verdict="allowed",
        )
        self.assertTrue(result, "capability_verdict='allowed' must win")

    def test_capability_disabled_overrides_legacy_allowance(self) -> None:
        from schema_inspector.match_center_policy import (
            football_detail_endpoint_allowed,
        )
        result = football_detail_endpoint_allowed(
            sport_slug="football",
            endpoint_pattern="/api/v1/event/{event_id}/incidents",
            detail_id=1,
            status_type="inprogress",
            has_xg=True,
            has_event_player_heat_map=True,
            has_event_player_statistics=True,
            has_global_highlights=None,
            start_timestamp=0,
            now_timestamp=0,
            capability_verdict="disabled",
        )
        self.assertFalse(result, "capability_verdict='disabled' must win")


class FeatureFlagTests(unittest.TestCase):
    def test_flag_default_is_disabled(self) -> None:
        from schema_inspector.services.league_capabilities_registry import (
            is_league_capabilities_enabled,
        )
        # Wipe env to test default.
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("SOFASCORE_LEAGUE_CAPABILITIES_ENABLED", None)
            self.assertFalse(is_league_capabilities_enabled())

    def test_flag_enabled_when_env_true(self) -> None:
        from schema_inspector.services.league_capabilities_registry import (
            is_league_capabilities_enabled,
        )
        with patch.dict(os.environ, {"SOFASCORE_LEAGUE_CAPABILITIES_ENABLED": "true"}):
            self.assertTrue(is_league_capabilities_enabled())
        with patch.dict(os.environ, {"SOFASCORE_LEAGUE_CAPABILITIES_ENABLED": "1"}):
            self.assertTrue(is_league_capabilities_enabled())
        with patch.dict(os.environ, {"SOFASCORE_LEAGUE_CAPABILITIES_ENABLED": "on"}):
            self.assertTrue(is_league_capabilities_enabled())

    def test_flag_disabled_when_env_falsy(self) -> None:
        from schema_inspector.services.league_capabilities_registry import (
            is_league_capabilities_enabled,
        )
        for falsy in ("false", "0", "off", "no", ""):
            with patch.dict(os.environ, {"SOFASCORE_LEAGUE_CAPABILITIES_ENABLED": falsy}):
                self.assertFalse(
                    is_league_capabilities_enabled(),
                    f"Falsy env value {falsy!r} must disable",
                )


if __name__ == "__main__":
    unittest.main()
