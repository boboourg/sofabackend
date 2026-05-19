"""Capability model for the historical-tournament backfill cursor.

Background
----------
The historical tournament archive walks ``tournament_registry`` season by
season. After each season is "processed", the orchestrator hands the
result back to ``HistoricalTournamentWorker`` which decides whether to
advance the per-UT cursor to the next season or leave it in place for
another attempt on the next planner tick.

The advance gate has gone through three iterations:

* **2026-05-16 (original)**: ``stage_failures == 0``. Worked for league
  competitions but failed for cup-style (FIFA WC, EURO, Olympics) which
  reliably 404 on ``/standings/home``, ``/events/round/{N}``, and
  sometimes ``/leaderboards`` (TLS). One pre-fixed cup UT (=16, FIFA WC)
  stayed stuck on season 41087 for 3+ hours after we had 64/64 events.
* **2026-05-18 (hack-fix)**: ``discovered_event_ids > 0``. Fixed the
  symptom — cups advanced once events were in DB — but didn't express
  the intent. Stage failures were silently ignored.
* **2026-05-19 (this module)**: capability dependency graph. The
  orchestrator reports the set of capabilities it completed for the
  season; the worker advances when the *required* subset is a subset of
  the *completed* set. Adding a new required capability is a 1-line
  change here, not a fragile if/elif in the worker.

What is a "capability"?
-----------------------
A named, season-level fact that the orchestrator can produce
independently of the others. Examples:

* ``EVENTS`` — at least one event was collected for the season (via
  ``/events/round/{N}`` or the ``/events/last/{p}`` fallback).
* ``ROUNDS`` — round metadata was fetched (only meaningful for league
  competitions; cups return 404).
* ``STANDINGS`` — total/home/away standings landed (league only —
  national-team cups have no standings).
* ``LEADERBOARDS`` — top players / top teams / POTS aggregations
  succeeded.
* ``STATISTICS`` — UT-level statistics paged dump succeeded.
* ``BRACKETS`` — playoff/knockout bracket metadata fetched (cup only).

v1 minimum required: ``{EVENTS}``
---------------------------------
Only ``EVENTS`` is required for cursor advance. Everything else is
best-effort enrichment that does not block the walk. This deliberately
permits cup-style competitions to advance even when standings /
leaderboards / round_events 404, because those capabilities are simply
not provided by Sofascore for cup-style season types.

The signature of :func:`required_capabilities_for_cursor_advance` is
intentionally parameterless so callers (the worker) don't need to know
season-type detection logic. When we later extend with per-sport or
per-season-type required sets, the extension lives entirely inside this
module.
"""

from __future__ import annotations

from typing import Final


# Canonical capability names. These are the keys used in
# ``_WorkerResult.capabilities_completed`` and the values in
# ``required_capabilities_for_cursor_advance()``. They are string
# constants (not an Enum) so they serialize trivially across the
# orchestrator → service → worker dict boundary without needing custom
# json encoders.
EVENTS: Final[str] = "events"
ROUNDS: Final[str] = "rounds"
BRACKETS: Final[str] = "brackets"
STANDINGS: Final[str] = "standings"
LEADERBOARDS: Final[str] = "leaderboards"
STATISTICS: Final[str] = "statistics"


# Module-level constant. Materialized once because callers (worker,
# health checks, future operator CLI) read it on every advance decision.
_KNOWN_CAPABILITIES: Final[frozenset[str]] = frozenset(
    {
        EVENTS,
        ROUNDS,
        BRACKETS,
        STANDINGS,
        LEADERBOARDS,
        STATISTICS,
    }
)

# Required-for-advance set. v1: just EVENTS. Kept as a separate constant
# so the test that pins "minimum is exactly {EVENTS}" has a single source
# of truth — a future PR adding ROUNDS for league-only paths will touch
# this constant + one test, nothing else.
_REQUIRED_FOR_CURSOR_ADVANCE: Final[frozenset[str]] = frozenset({EVENTS})


def known_capabilities() -> frozenset[str]:
    """Stable set of all capability names this module recognizes.

    Used to validate orchestrator-reported sets at the worker boundary
    (typos in ``capabilities_completed`` should never silently pass) and
    to enumerate capabilities for monitoring / ``/ops/`` endpoints.
    """
    return _KNOWN_CAPABILITIES


def required_capabilities_for_cursor_advance() -> frozenset[str]:
    """Minimum capabilities a season must have completed before the
    historical cursor walks to the next season.

    v1 (2026-05-19): only ``EVENTS``. See module docstring for the
    history and rationale — this is deliberate, not minimalism for
    minimalism's sake.

    Future extension points (not needed yet):

    * Sport-specific required sets — e.g. ``tennis`` only needs
      ``EVENTS`` because there are no rounds/standings concepts;
      ``football league`` could require ``{EVENTS, ROUNDS}`` to prevent
      premature advance on a season where only 1 round of 38 has been
      played.
    * Season-type-aware — cup vs league vs knockout. Detection logic
      would belong in this module too, fed by tournament metadata.

    Until that need lands, the parameterless signature keeps the worker
    blissfully ignorant of season-type detection.
    """
    return _REQUIRED_FOR_CURSOR_ADVANCE


__all__ = [
    "EVENTS",
    "ROUNDS",
    "BRACKETS",
    "STANDINGS",
    "LEADERBOARDS",
    "STATISTICS",
    "known_capabilities",
    "required_capabilities_for_cursor_advance",
]
