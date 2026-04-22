"""Shared policy for event detail resources that depend on current event state."""

from __future__ import annotations


LIVE_DETAIL_STATUS_TYPES = frozenset({"inprogress", "finished"})


def supports_live_detail_resources(status_type: str | None) -> bool:
    normalized = str(status_type or "").strip().lower()
    return normalized in LIVE_DETAIL_STATUS_TYPES
