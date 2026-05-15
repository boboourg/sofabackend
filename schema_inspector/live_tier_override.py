"""Per-UT live-tier override registry.

A3 Phase 0 (2026-05-16): operator-controlled exception layer that
overrides the default heuristic in
``resolve_live_dispatch_tier`` (live_dispatch_policy.py).

Database table: ``live_tier_override`` (see
``migrations/2026-05-16_live_tier_override.sql``).

Flow:

1. ``LiveTierOverrideRegistry.load()`` snapshots the whole table into
   a process-local ``dict[int, str]`` at orchestrator startup.
2. ``resolve_live_dispatch_tier`` looks the unique_tournament_id up in
   the registry; non-None override returns immediately, otherwise the
   heuristic runs as before.
3. Phase 2 (admin UI) will call ``LiveTierOverrideRegistry.refresh()``
   from an HTTP endpoint after an INSERT/UPDATE. Phase 0 expects an
   operator-driven restart for changes to apply.

The registry is intentionally synchronous in the hot path (dict
lookup) — async load happens once at startup so per-event dispatch
adds zero overhead.
"""

from __future__ import annotations

import logging
from typing import Any, Mapping

logger = logging.getLogger(__name__)

_VALID_TIERS = frozenset({"tier_1", "tier_2", "tier_3"})


class LiveTierOverrideRegistry:
    """Snapshot of ``live_tier_override`` rows held in process memory.

    Construct with the asyncpg-style ``sql_executor``; call ``load()``
    once at startup to populate. Lookups via ``get()`` are O(1) dict
    access.
    """

    def __init__(self, *, sql_executor: Any) -> None:
        self.sql_executor = sql_executor
        self._overrides: dict[int, str] = {}
        self._loaded = False

    async def load(self) -> int:
        """Snapshot the override table into memory. Returns row count."""

        try:
            rows = await self.sql_executor.fetch(
                "SELECT unique_tournament_id, override_tier FROM live_tier_override"
            )
        except Exception as exc:  # noqa: BLE001 — defensive
            logger.warning(
                "LiveTierOverrideRegistry: load failed, keeping previous "
                "snapshot of %d entries: %r",
                len(self._overrides),
                exc,
            )
            return len(self._overrides)
        snapshot: dict[int, str] = {}
        for row in rows:
            try:
                ut_id = int(row["unique_tournament_id"])
                tier = str(row["override_tier"]).strip().lower()
            except (KeyError, TypeError, ValueError):
                continue
            if tier not in _VALID_TIERS:
                logger.warning(
                    "LiveTierOverrideRegistry: skipping invalid tier=%r for ut_id=%s",
                    tier,
                    ut_id,
                )
                continue
            snapshot[ut_id] = tier
        self._overrides = snapshot
        self._loaded = True
        logger.info(
            "LiveTierOverrideRegistry: loaded %d override(s) from live_tier_override",
            len(snapshot),
        )
        return len(snapshot)

    async def refresh(self) -> int:
        """Re-snapshot the table. Identical to load() but kept as a
        distinct entry point for Phase 2 admin-triggered reloads."""

        return await self.load()

    def get(self, unique_tournament_id: int | None) -> str | None:
        """Return the override tier for the UT, or None if no row."""

        if unique_tournament_id is None:
            return None
        try:
            key = int(unique_tournament_id)
        except (TypeError, ValueError):
            return None
        return self._overrides.get(key)

    def is_loaded(self) -> bool:
        return self._loaded

    def snapshot(self) -> Mapping[int, str]:
        """Read-only view of current overrides (for /ops/admin/tier-overrides)."""

        return dict(self._overrides)


class _NoOpRegistry:
    """Placeholder registry that always returns None.

    Used when the orchestrator is constructed without a SQL executor
    (e.g. unit tests). Keeps the call site branch-free.
    """

    def get(self, unique_tournament_id: int | None) -> str | None:  # noqa: ARG002
        return None

    def is_loaded(self) -> bool:
        return False

    def snapshot(self) -> Mapping[int, str]:
        return {}


NO_OP_REGISTRY = _NoOpRegistry()
