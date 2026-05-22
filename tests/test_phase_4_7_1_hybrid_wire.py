"""Phase 4.7.1 (2026-05-23): HybridApp wires LeagueCapabilitiesRegistry
into all PilotOrchestrator(...) call sites.

The Phase 4.7 wire commit (3b1aa85) added a ``league_capabilities`` kwarg
to ``PilotOrchestrator.__init__`` (default None, backwards-compatible).
This file proves that the actual long-running runtime (``HybridApp``):

  1. Constructs the registry lazily — only when the feature flag is ON
     AND a real Redis backend is wired (because the registry caches in
     Redis; without Redis the registry is useless and would slow the hot
     path with DB-only lookups).
  2. Stays None when the flag is OFF, so default behaviour is identical
     to pre-Phase-4.7 production (zero overhead on every event hydrate).
  3. Passes ``self.league_capabilities`` to EVERY ``PilotOrchestrator(...)``
     call site inside HybridApp — without this the wire is half-done and
     the orchestrator's gate fallback never sees the registry.

The AST test in ``HybridAppPassesRegistryToAllOrchestratorsTests`` is the
critical regression — adding a fourth construction site in the future
without ``league_capabilities=`` would silently revert the wiring for
that code path. The structural assertion catches it.
"""

from __future__ import annotations

import ast
import os
import unittest
from pathlib import Path
from unittest.mock import patch


_FLAG_ENV = "SOFASCORE_LEAGUE_CAPABILITIES_ENABLED"


class _FakeAsyncpgDatabase:
    """Minimal stub matching the .connection()/.transaction() surface
    of ``AsyncpgDatabase`` — only required by HybridApp's __init__ path
    (it stashes a reference; no calls are made during construction)."""

    def connection(self):
        raise NotImplementedError

    def transaction(self):
        raise NotImplementedError


class _FakeRedisBackend:
    def __init__(self) -> None:
        self.ping_calls = 0

    def ping(self) -> bool:
        self.ping_calls += 1
        return True

    def close(self) -> None:
        pass


class HybridAppLeagueCapabilitiesInitTests(unittest.TestCase):
    """Coverage for the lazy-construction rules described in the module
    docstring. None of these should hit real I/O — HybridApp.__init__ is
    pure setup, no DB or Redis calls."""

    def _make_app(self, *, redis_backend) -> object:
        from schema_inspector.cli import HybridApp
        from schema_inspector.runtime import RuntimeConfig

        return HybridApp(
            database=_FakeAsyncpgDatabase(),
            runtime_config=RuntimeConfig(require_proxy=False),
            redis_backend=redis_backend,
        )

    def test_league_capabilities_is_none_when_flag_off(self) -> None:
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop(_FLAG_ENV, None)
            app = self._make_app(redis_backend=_FakeRedisBackend())
        self.assertIsNone(
            app.league_capabilities,
            "flag OFF must leave league_capabilities as None (no overhead)",
        )

    def test_league_capabilities_is_none_when_redis_backend_none(self) -> None:
        """Even with the flag ON, the registry needs a Redis backend to
        cache verdicts. Without one it would degrade to DB-on-every-call
        in the hot path. Skip construction entirely."""
        with patch.dict(os.environ, {_FLAG_ENV: "true"}):
            app = self._make_app(redis_backend=None)
        self.assertIsNone(app.league_capabilities)

    def test_league_capabilities_constructed_when_flag_on_and_redis_present(self) -> None:
        from schema_inspector.services.league_capabilities_registry import (
            LeagueCapabilitiesRegistry,
        )

        redis = _FakeRedisBackend()
        with patch.dict(os.environ, {_FLAG_ENV: "true"}):
            app = self._make_app(redis_backend=redis)
        self.assertIsInstance(app.league_capabilities, LeagueCapabilitiesRegistry)
        # Wires through the actual redis_backend and database — without
        # this the registry holds references to garbage / wrong handles.
        self.assertIs(app.league_capabilities.redis_backend, redis)
        self.assertIs(app.league_capabilities.database, app.database)


class HybridAppPassesRegistryToAllOrchestratorsTests(unittest.TestCase):
    """Structural assertion: every ``PilotOrchestrator(...)`` constructed
    inside ``HybridApp`` methods must include ``league_capabilities=
    self.league_capabilities``. Without it, the wire is half-done — the
    orchestrator stays None-tolerant but never actually sees the registry
    for that code path.

    Walking the AST instead of running the hydrate flow keeps this test
    independent of the heavy fetch/normalize stack (no DB, no Redis, no
    transport)."""

    def _hybrid_app_class(self) -> ast.ClassDef:
        cli_path = Path(__file__).resolve().parent.parent / "schema_inspector" / "cli.py"
        # cli.py is saved with a UTF-8 BOM; utf-8-sig strips it so ast.parse
        # doesn't choke on ﻿ at offset 0.
        tree = ast.parse(cli_path.read_text(encoding="utf-8-sig"))
        for node in tree.body:
            if isinstance(node, ast.ClassDef) and node.name == "HybridApp":
                return node
        raise AssertionError("HybridApp class not found in cli.py")

    def _orchestrator_call_sites(self) -> list[ast.Call]:
        sites: list[ast.Call] = []
        for node in ast.walk(self._hybrid_app_class()):
            if isinstance(node, ast.Call):
                func = node.func
                name = (
                    func.attr if isinstance(func, ast.Attribute)
                    else (func.id if isinstance(func, ast.Name) else None)
                )
                if name == "PilotOrchestrator":
                    sites.append(node)
        return sites

    def test_at_least_three_orchestrator_call_sites_exist(self) -> None:
        """Sanity: today (2026-05-23) there are 3 call sites inside
        HybridApp (run_event_details, _prefetch_event_run,
        _persist_prefetched_run). If this drops below 3, code structure
        changed and the next test's coverage is misleading."""
        sites = self._orchestrator_call_sites()
        self.assertGreaterEqual(
            len(sites), 3,
            f"Expected ≥3 PilotOrchestrator(...) sites in HybridApp, found {len(sites)}",
        )

    def test_every_orchestrator_call_passes_league_capabilities(self) -> None:
        sites = self._orchestrator_call_sites()
        self.assertTrue(sites, "no PilotOrchestrator call sites found (regression)")
        missing: list[int] = []
        wrong_value: list[tuple[int, str]] = []
        for call in sites:
            arg = next(
                (kw for kw in call.keywords if kw.arg == "league_capabilities"),
                None,
            )
            if arg is None:
                missing.append(call.lineno)
                continue
            # Must be self.league_capabilities (the HybridApp attribute).
            value_src = ast.unparse(arg.value)
            if value_src != "self.league_capabilities":
                wrong_value.append((call.lineno, value_src))
        self.assertFalse(
            missing,
            f"PilotOrchestrator(...) call sites missing "
            f"`league_capabilities=` kwarg at lines: {missing}",
        )
        self.assertFalse(
            wrong_value,
            f"PilotOrchestrator(...) call sites pass wrong value for "
            f"league_capabilities (must be self.league_capabilities): {wrong_value}",
        )


if __name__ == "__main__":
    unittest.main()
