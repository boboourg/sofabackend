"""Canonical set of valid ``scope_kind`` strings.

Why a top-level private module
------------------------------
``SofascoreEndpoint.__post_init__`` validates ``scope_kind`` against
this set at construction time. Importing the set from
``services.resource_scope`` would trigger ``services/__init__.py``,
which transitively pulls in modules (live_discovery_planner,
worker stream jobs, ...) that need ``endpoints`` already initialised
— a circular import the first time an endpoint with a non-None
``scope_kind`` is declared.

Keeping the constant in a top-level module that depends on nothing
else inside ``schema_inspector`` breaks the cycle: ``endpoints.py``
imports directly from here, no side imports fire.

Drift protection
----------------
The set is a hardcoded literal so the constant can stand alone.
``tests/test_scope_kind_validation.py`` asserts that this set equals
``{resolver.kind for resolver in <all resolvers>}`` so any new
resolver kind (or a typo in an existing one) is caught on the next
CI run.
"""

from __future__ import annotations


KNOWN_SCOPE_KINDS: frozenset[str] = frozenset(
    {
        "custom-id-of-managed-events",
        "custom-id-of-registry-events",
        "event-of-active-baseball",
        "managed",
        "period-of-managed-football-pairs",
        "period-of-registry-football",
        "player-of-active-squad",
        "player-of-active-squad-first-page",
        "player-of-national-team-history",
        "round-of-managed-football-pairs",
        "round-of-registry-football",
        "season-of-active-ut-events",
        "season-of-active-ut-standings",
        "season-of-registry-ut",
        # Item 1 (2026-05-19): historical /rounds pre-fetch scope.
        # Covers any (UT, season) pair from registry-active +
        # historical_enabled UTs whose season_round table is still
        # empty. Decouples round catalog fetching from the cursor
        # walk so the strict cat-priority barrier doesn't block
        # UCL/EURO/etc. round structure landing.
        "season-of-registry-ut-rounds-historical",
        "team-of-active-ut",
        "team-of-active-ut-first-page",
        "team-of-active-ut-season",
        "team-of-registry-ut",
    }
)


__all__ = ["KNOWN_SCOPE_KINDS"]
