"""Semantic freshness windows for event hydration fanout."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class FreshnessPolicy:
    store: object
    core_ttl_ms: int = 300_000
    full_ttl_ms: int = 60_000

    def claim_event_hydration(
        self,
        *,
        event_id: int,
        hydration_mode: str,
        force_rehydrate: bool,
        now_ms: int | None = None,
    ) -> bool:
        if force_rehydrate:
            return True
        resolved_mode = str(hydration_mode or "full").strip().lower() or "full"
        ttl_ms = self.core_ttl_ms if resolved_mode == "core" else self.full_ttl_ms
        claim_job = getattr(self.store, "claim_job", None)
        if not callable(claim_job):
            return True
        return bool(
            claim_job(
                self.event_hydration_key(event_id=event_id, hydration_mode=resolved_mode),
                ttl_ms=int(ttl_ms),
                now_ms=now_ms,
            )
        )

    @staticmethod
    def event_hydration_key(*, event_id: int, hydration_mode: str) -> str:
        resolved_mode = str(hydration_mode or "full").strip().lower() or "full"
        return f"fresh:event:{int(event_id)}:{resolved_mode}"
