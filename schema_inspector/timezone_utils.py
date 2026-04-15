"""Helpers for resolving Sofascore timezone offsets and date bounds."""

from __future__ import annotations

from datetime import date as Date
from datetime import datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo


def resolve_timezone_offset_seconds(
    *,
    observed_date: str | Date,
    timezone_name: str | None = None,
    timezone_offset_seconds: int | None = None,
) -> int:
    """Resolve the timezone offset in seconds for a given date."""

    if timezone_offset_seconds is not None:
        return timezone_offset_seconds

    resolved_date = Date.fromisoformat(observed_date) if isinstance(observed_date, str) else observed_date
    if timezone_name:
        tzinfo = ZoneInfo(timezone_name)
    else:
        tzinfo = datetime.now().astimezone().tzinfo

    if tzinfo is None:
        return 0

    localized = datetime.combine(resolved_date, time(hour=12), tzinfo=tzinfo)
    offset = localized.utcoffset() or timedelta()
    return int(offset.total_seconds())


def resolve_timestamp_bounds(
    *,
    date_from: str | Date | None = None,
    date_to: str | Date | None = None,
    timezone_name: str | None = None,
) -> tuple[int | None, int | None]:
    """Resolve inclusive Unix timestamp bounds for local calendar dates."""

    if date_from is None and date_to is None:
        return None, None

    if timezone_name:
        tzinfo = ZoneInfo(timezone_name)
    else:
        tzinfo = datetime.now().astimezone().tzinfo
    if tzinfo is None:
        tzinfo = timezone.utc

    start_value = Date.fromisoformat(date_from) if isinstance(date_from, str) else date_from
    end_value = Date.fromisoformat(date_to) if isinstance(date_to, str) else date_to

    start_timestamp: int | None = None
    end_timestamp: int | None = None
    if start_value is not None:
        start_dt = datetime.combine(start_value, time.min, tzinfo=tzinfo)
        start_timestamp = int(start_dt.timestamp())
    if end_value is not None:
        end_dt = datetime.combine(end_value, time.max, tzinfo=tzinfo)
        end_timestamp = int(end_dt.timestamp())
    return start_timestamp, end_timestamp
