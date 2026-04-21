"""Shared coverage-policy helpers for selective refill decisions."""

from __future__ import annotations


LINEUP_RECHECK_WINDOW_MINUTES = 50


def lineup_recheck_window_open(*, start_timestamp: int | None, now_timestamp: int) -> bool:
    if not isinstance(start_timestamp, int):
        return True
    return int(start_timestamp) <= int(now_timestamp) + (LINEUP_RECHECK_WINDOW_MINUTES * 60)


def lineups_coverage_state(
    *,
    lineup_sides: int,
    lineup_players: int,
    start_timestamp: int | None,
    now_timestamp: int,
) -> tuple[str, float]:
    signals = (int(lineup_sides > 0), int(lineup_players > 0))
    completeness_ratio = sum(signals) / len(signals)
    if completeness_ratio < 1.0:
        freshness_status = "partial" if completeness_ratio > 0 else "missing"
        return freshness_status, completeness_ratio
    freshness_status = "fresh" if lineup_recheck_window_open(start_timestamp=start_timestamp, now_timestamp=now_timestamp) else "possible"
    return freshness_status, completeness_ratio
