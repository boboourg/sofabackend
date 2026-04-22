"""Live polling lane decisions for the hybrid ETL planner."""

from __future__ import annotations

from dataclasses import dataclass

from ..sport_profiles import resolve_sport_profile

# Statuses where the match is actively in progress — use HOT lane with
# sport-specific hot_poll_seconds.
ACTIVE_LIVE_STATUS_TYPES = frozenset(
    {
        "inprogress",
        "live",
        "overtime",
        "extra",
        "awaitingextra",
        "awaitingpenalties",
        "penalties",
        "interrupted",
    }
)

# Statuses where play is paused — use HOT lane with break_poll_seconds.
BREAK_STATUS_TYPES = frozenset(
    {
        "halftime",
        "paused",
        "pause",
        "break",
    }
)

TERMINAL_STATUS_TYPES = frozenset(
    {"finished", "afterextra", "afterpen", "cancelled", "canceled", "postponed"}
)


@dataclass(frozen=True)
class LivePollingDecision:
    lane: str
    next_poll_seconds: int | None
    terminal: bool


def classify_live_polling(
    *,
    status_type: str | None,
    minutes_to_start: int | None,
    sport_slug: str | None = None,
) -> LivePollingDecision:
    """Return the correct lane and polling interval for a given event state.

    Decision order:
      1. Terminal  → remove from tracking.
      2. Active    → HOT, sport-specific hot_poll_seconds (e.g. football=10s, esports=30s).
      3. Break     → HOT, break_poll_seconds (120 s for all sports).
      4. Warmup    → WARM, if 0 < minutes_to_start <= warmup_window_minutes (30 min).
                     Poll every warmup_poll_seconds (600 s / 10 min).
      5. Default   → WARM, generic 300 s fallback for anything else not yet started.
    """
    profile = resolve_sport_profile(sport_slug or "football")
    normalized = str(status_type or "").strip().lower()

    # 1. Terminal — stop tracking.
    if normalized in TERMINAL_STATUS_TYPES:
        return LivePollingDecision(lane="terminal", next_poll_seconds=None, terminal=True)

    # 2. Active match in progress.
    if normalized in ACTIVE_LIVE_STATUS_TYPES:
        return LivePollingDecision(
            lane="hot",
            next_poll_seconds=profile.hot_poll_seconds,
            terminal=False,
        )

    # 3. Break / halftime / pause.
    if normalized in BREAK_STATUS_TYPES:
        return LivePollingDecision(
            lane="hot",
            next_poll_seconds=profile.break_poll_seconds,
            terminal=False,
        )

    # 4. Warmup window: match hasn't started, we're within warmup_window_minutes.
    if minutes_to_start is not None and 0 < minutes_to_start <= profile.warmup_window_minutes:
        return LivePollingDecision(
            lane="warm",
            next_poll_seconds=profile.warmup_poll_seconds,
            terminal=False,
        )

    # 5. Unknown / pre-match outside window → WARM with a conservative interval.
    return LivePollingDecision(lane="warm", next_poll_seconds=300, terminal=False)
