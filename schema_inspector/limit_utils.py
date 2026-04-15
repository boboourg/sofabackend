"""Helpers for optional backfill limits."""

from __future__ import annotations


def normalize_limit(limit: int | None) -> int | None:
    if limit is None or limit <= 0:
        return None
    return limit
