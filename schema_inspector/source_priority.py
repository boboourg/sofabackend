"""Shared source-priority helpers for reconcile and persistence decisions."""

from __future__ import annotations

SOURCE_PRIORITY: dict[str, int] = {
    "sofascore": 100,
    "secondary_source": 80,
}


def source_priority(source_slug: str | None) -> int:
    normalized = str(source_slug or "").strip().lower()
    return SOURCE_PRIORITY.get(normalized, 0)


def should_existing_source_win(*, existing_source_slug: str | None, incoming_source_slug: str | None) -> bool:
    return source_priority(existing_source_slug) > source_priority(incoming_source_slug)
