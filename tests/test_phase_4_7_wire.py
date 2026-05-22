"""Phase 4.7 wire: resolve_capability_verdict helper tests.

The orchestrator calls this helper before each gate decision —
it consolidates the feature flag check, the registry lookup, and
the verdict→str conversion in one place. The orchestrator itself
stays simple: ``verdict = await resolve_capability_verdict(...);
if not football_edge_allowed(..., capability_verdict=verdict): ...``.

All tests are pure-Python with a fake registry.
"""

from __future__ import annotations

import unittest


class _FakeRegistry:
    def __init__(self, verdict_value: str | None = "allowed") -> None:
        from schema_inspector.services.league_capabilities_registry import (
            EndpointVerdict,
        )
        self.verdict_value = verdict_value
        self.calls: list = []

    async def get_verdict(self, **kwargs):
        from schema_inspector.services.league_capabilities_registry import (
            EndpointVerdict,
        )
        self.calls.append(kwargs)
        return EndpointVerdict.from_str(self.verdict_value)


class _RaisingRegistry:
    def __init__(self) -> None:
        self.calls: list = []

    async def get_verdict(self, **kwargs):
        self.calls.append(kwargs)
        raise RuntimeError("registry exploded")


class ResolveCapabilityVerdictTests(unittest.IsolatedAsyncioTestCase):
    async def test_flag_off_returns_none_without_calling_registry(self) -> None:
        from schema_inspector.services.league_capabilities_registry import (
            resolve_capability_verdict,
        )
        registry = _FakeRegistry(verdict_value="disabled")
        result = await resolve_capability_verdict(
            registry=registry,
            enabled=False,
            unique_tournament_id=17,
            season_id=61643,
            status_type="finished",
            endpoint_pattern="/x",
        )
        self.assertIsNone(result)
        self.assertEqual(registry.calls, [])

    async def test_registry_none_returns_none(self) -> None:
        from schema_inspector.services.league_capabilities_registry import (
            resolve_capability_verdict,
        )
        result = await resolve_capability_verdict(
            registry=None,
            enabled=True,
            unique_tournament_id=17,
            season_id=61643,
            status_type="finished",
            endpoint_pattern="/x",
        )
        self.assertIsNone(result)

    async def test_missing_ut_id_returns_none(self) -> None:
        from schema_inspector.services.league_capabilities_registry import (
            resolve_capability_verdict,
        )
        registry = _FakeRegistry()
        result = await resolve_capability_verdict(
            registry=registry,
            enabled=True,
            unique_tournament_id=None,
            season_id=42,
            status_type="finished",
            endpoint_pattern="/x",
        )
        self.assertIsNone(result)
        self.assertEqual(registry.calls, [])

    async def test_missing_status_type_returns_none(self) -> None:
        from schema_inspector.services.league_capabilities_registry import (
            resolve_capability_verdict,
        )
        registry = _FakeRegistry()
        result = await resolve_capability_verdict(
            registry=registry,
            enabled=True,
            unique_tournament_id=17,
            season_id=42,
            status_type=None,
            endpoint_pattern="/x",
        )
        self.assertIsNone(result)

    async def test_returns_allowed_string(self) -> None:
        from schema_inspector.services.league_capabilities_registry import (
            resolve_capability_verdict,
        )
        registry = _FakeRegistry(verdict_value="allowed")
        result = await resolve_capability_verdict(
            registry=registry,
            enabled=True,
            unique_tournament_id=17,
            season_id=61643,
            status_type="finished",
            endpoint_pattern="/x",
        )
        self.assertEqual(result, "allowed")
        self.assertEqual(len(registry.calls), 1)

    async def test_returns_disabled_string(self) -> None:
        from schema_inspector.services.league_capabilities_registry import (
            resolve_capability_verdict,
        )
        registry = _FakeRegistry(verdict_value="disabled")
        result = await resolve_capability_verdict(
            registry=registry,
            enabled=True,
            unique_tournament_id=17,
            season_id=61643,
            status_type="finished",
            endpoint_pattern="/x",
        )
        self.assertEqual(result, "disabled")

    async def test_returns_unknown_when_registry_unknown(self) -> None:
        from schema_inspector.services.league_capabilities_registry import (
            resolve_capability_verdict,
        )
        registry = _FakeRegistry(verdict_value="unknown")
        result = await resolve_capability_verdict(
            registry=registry,
            enabled=True,
            unique_tournament_id=17,
            season_id=61643,
            status_type="finished",
            endpoint_pattern="/x",
        )
        self.assertEqual(result, "unknown")

    async def test_registry_exception_swallowed_to_none(self) -> None:
        """Hot path safety: any registry failure (Redis timeout,
        Postgres connection drop) must NOT raise — orchestrator
        falls back to legacy policy via None."""
        from schema_inspector.services.league_capabilities_registry import (
            resolve_capability_verdict,
        )
        registry = _RaisingRegistry()
        result = await resolve_capability_verdict(
            registry=registry,
            enabled=True,
            unique_tournament_id=17,
            season_id=61643,
            status_type="finished",
            endpoint_pattern="/x",
        )
        self.assertIsNone(result)


class PilotOrchestratorAcceptsRegistryTests(unittest.TestCase):
    """Constructor accepts league_capabilities kwarg (default None)
    so existing call sites pass identically with no kwarg = backwards
    compatible. Don't construct the full orchestrator — that pulls in
    Redis + asyncpg — just verify the signature."""

    def test_pilot_orchestrator_init_accepts_league_capabilities(self) -> None:
        from schema_inspector.pipeline.pilot_orchestrator import PilotOrchestrator
        import inspect

        sig = inspect.signature(PilotOrchestrator.__init__)
        self.assertIn("league_capabilities", sig.parameters)
        self.assertIs(sig.parameters["league_capabilities"].default, None)


if __name__ == "__main__":
    unittest.main()
