"""Helpers for coercing storage-bound temporal values into asyncpg-safe objects."""

from __future__ import annotations

from datetime import UTC, date, datetime


def coerce_timestamptz(value: str | datetime | None) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=UTC)
        return value
    normalized = str(value).strip()
    if not normalized:
        return None
    if normalized.endswith("Z"):
        normalized = normalized[:-1] + "+00:00"
    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed


def coerce_date(value: str | date | None) -> date | None:
    if value is None:
        return None
    if isinstance(value, date):
        return value
    normalized = str(value).strip()
    if not normalized:
        return None
    return date.fromisoformat(normalized)
