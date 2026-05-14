"""Hot URL targets for the cache warmer."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone


@dataclass(frozen=True)
class UrlTarget:
    """A URL that the warmer should periodically fetch.

    ``url`` is the API path (without base URL). ``interval_seconds`` is
    how often to re-fetch it. The warmer maintains a per-target "last
    fetched at" timestamp and only refetches when the elapsed time
    exceeds ``interval_seconds``.
    """

    url: str
    interval_seconds: int
    description: str = ""


def default_sports() -> tuple[str, ...]:
    return (
        "football",
        "basketball",
        "tennis",
        "ice-hockey",
        "volleyball",
        "handball",
        "baseball",
        "american-football",
        "rugby",
        "cricket",
        "mma",
        "esports",
        "table-tennis",
        "futsal",
    )


def build_url_targets(
    *,
    sports: tuple[str, ...],
    today: str,
    tomorrow: str | None = None,
    live_interval_seconds: int = 5,
    scheduled_today_interval_seconds: int = 60,
    scheduled_tomorrow_interval_seconds: int = 300,
    scheduled_tournaments_interval_seconds: int = 60,
    warm_tomorrow_scheduled: bool = False,
) -> list[UrlTarget]:
    """Construct the full target list for one warmer process.

    For each sport: 3 targets (live, scheduled today, scheduled-
    tournaments today). If ``warm_tomorrow_scheduled=True`` we add a
    fourth (scheduled tomorrow) per sport — useful when frontend
    pre-fetches "next day's events" UI heavily.
    """

    targets: list[UrlTarget] = []
    for slug in sports:
        slug = slug.strip().lower()
        if not slug:
            continue
        targets.append(
            UrlTarget(
                url=f"/api/v1/sport/{slug}/events/live",
                interval_seconds=int(live_interval_seconds),
                description=f"live events for {slug}",
            )
        )
        targets.append(
            UrlTarget(
                url=f"/api/v1/sport/{slug}/scheduled-events/{today}",
                interval_seconds=int(scheduled_today_interval_seconds),
                description=f"scheduled events for {slug} today",
            )
        )
        targets.append(
            UrlTarget(
                url=f"/api/v1/sport/{slug}/scheduled-tournaments/{today}/page/0",
                interval_seconds=int(scheduled_tournaments_interval_seconds),
                description=f"scheduled tournaments for {slug} today",
            )
        )
        if warm_tomorrow_scheduled and tomorrow:
            targets.append(
                UrlTarget(
                    url=f"/api/v1/sport/{slug}/scheduled-events/{tomorrow}",
                    interval_seconds=int(scheduled_tomorrow_interval_seconds),
                    description=f"scheduled events for {slug} tomorrow",
                )
            )
    return targets


def today_and_tomorrow_iso(now: datetime | None = None) -> tuple[str, str]:
    """Return (today, tomorrow) as ISO date strings, both in UTC."""

    base = now or datetime.now(timezone.utc)
    today = base.date().isoformat()
    tomorrow = (base + timedelta(days=1)).date().isoformat()
    return today, tomorrow
