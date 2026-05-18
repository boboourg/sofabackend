"""Map Sofascore WS deltas to our normalized schema.

The upstream NATS feed at wss://ws.sofascore.com:9222 publishes
event-state changes as **partial** JSON payloads using a flat
dot-notation key style — e.g.::

    {
      "id": 16046462,
      "status.code": 7,
      "status.description": "2nd half",
      "homeScore.period2": 0,
      "awayScore.period2": 0,
      "time.played": 600,
      "time.playedLastUpdated": {"date": "2026-04-25 11:07:05.802240",
                                  "timezone_type": 3, "timezone": "UTC"},
      "changes.changeTimestamp": 1777115225
    }

This module turns one such payload into a :class:`NormalizedEventDelta`
bundle: a set of per-table column-value maps the writer can use to
issue narrow UPSERTs against ``event``, ``event_score``,
``event_status`` (via lookup), ``event_time``, ``event_status_time``
and ``event_change_item``.

It is a **pure** function — no DB, no asyncio — so it tests cleanly
against archived deltas. The writer module owns the IO half.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any


@dataclass
class NormalizedEventDelta:
    """Result of normalizing one event-type WS push."""

    event_id: int
    change_timestamp: int | None = None
    # event_score rows: one per side that appears in the delta.
    # Each row is a dict ``{"side": "home"|"away", "<column>": value, ...}``.
    event_score_rows: list[dict[str, Any]] = field(default_factory=list)
    # event columns to UPSERT (status_code, winner_code, last_period,
    # first_to_serve, ...). Does NOT include the id.
    event_fields: dict[str, Any] = field(default_factory=dict)
    # The status block (code, description, type). Writer will use code
    # to look up event_status row and patch event.status_code.
    event_status_fields: dict[str, Any] = field(default_factory=dict)
    # event_time columns (initial, max, extra, played, ...).
    event_time_fields: dict[str, Any] = field(default_factory=dict)
    # event_status_time columns (prefix, timestamp, initial, max, extra).
    event_status_time_fields: dict[str, Any] = field(default_factory=dict)
    # VAR-in-progress row (football). ``None`` when the delta does not
    # touch VAR state. Shape: ``{"home_team": bool, "away_team": bool}``.
    event_var_in_progress: dict[str, Any] | None = None


@dataclass
class NormalizedOddsDelta:
    """Result of normalizing one odds-type WS push.

    Stage 1 stores these in a flat structure; Stage 2 will wire them
    into the odds tables (event_odds_* / event_odds_choice_*).
    """

    offer_id: int
    # choice index (1, 2, 3) → fields (fractionalValue, initialFractionalValue)
    choices: dict[int, dict[str, str]] = field(default_factory=dict)


# Map ``homeScore.X`` / ``awayScore.X`` to event_score columns.
_SCORE_COLUMN_BY_KEY: dict[str, str] = {
    "current": "current",
    "display": "display",
    "aggregated": "aggregated",
    "normaltime": "normaltime",
    "overtime": "overtime",
    "penalties": "penalties",
    "period1": "period1",
    "period2": "period2",
    "period3": "period3",
    "period4": "period4",
    "extra1": "extra1",
    "extra2": "extra2",
    "series": "series",
    # tennis-specific: the "point" score is a string ("15"/"30"/"40"/"AD").
    "point": "point",
    # additional period observed in volleyball/table-tennis (5+ sets):
    "period5": "period5",
}

# Map ``time.X`` to event_time columns.
_TIME_COLUMN_BY_KEY: dict[str, str] = {
    "initial": "initial",
    "max": "max",
    "extra": "extra",
    "currentPeriodStartTimestamp": "current_period_start_timestamp",
    "periodLength": "period_length",
    "totalPeriodCount": "total_period_count",
    "overtimeLength": "overtime_length",
    "injuryTime1": "injury_time1",
    "injuryTime2": "injury_time2",
    "injuryTime3": "injury_time3",
    "injuryTime4": "injury_time4",
    "played": "played",
    "playedLastUpdated": "played_last_updated",
    "clockRunning": "clock_running",
    "clockRunningLastUpdated": "clock_running_last_updated",
}

# Map ``status.X`` to event_status_fields keys.
_STATUS_COLUMN_BY_KEY: dict[str, str] = {
    "code": "code",
    "description": "description",
    "type": "type",
}


def _parse_php_datetime(value: Any) -> Any:
    """Sofascore sometimes ships timestamps as the PHP DateTimeInterface
    serialised shape::

        {"date": "2026-04-25 11:07:05.802240",
         "timezone_type": 3,
         "timezone": "UTC"}

    Convert to ``datetime`` with the right tz. Pass-through for any
    value that isn't this shape (e.g. already a unix int)."""
    if not isinstance(value, dict) or "date" not in value:
        return value
    date_str = value.get("date")
    tz_name = value.get("timezone") or "UTC"
    if not isinstance(date_str, str):
        return value
    try:
        # The PHP format is "YYYY-MM-DD HH:MM:SS.uuuuuu" — fromisoformat
        # tolerates the space separator since 3.11.
        naive = datetime.fromisoformat(date_str)
    except ValueError:
        return value
    if naive.tzinfo is not None:
        return naive
    if tz_name.upper() == "UTC":
        return naive.replace(tzinfo=timezone.utc)
    # We don't ship the IANA tz db with the consumer — fall back to UTC
    # and stamp the row regardless. The exact offset rarely matters for
    # cache invalidation (we only need monotonic ordering).
    return naive.replace(tzinfo=timezone.utc)


def normalize_event_delta(payload: dict[str, Any]) -> NormalizedEventDelta | None:
    """Convert one WS event-type push into a NormalizedEventDelta.

    Returns ``None`` if the payload is empty or has no usable event id —
    the caller (the consumer) drops these silently."""
    if not isinstance(payload, dict) or not payload:
        return None
    event_id = payload.get("id")
    if not isinstance(event_id, int):
        return None

    delta = NormalizedEventDelta(event_id=event_id)

    # Score rows keyed by side, merged across keys.
    score_by_side: dict[str, dict[str, Any]] = {}

    for key, value in payload.items():
        if key == "id":
            continue

        if key == "changes.changeTimestamp":
            if isinstance(value, int):
                delta.change_timestamp = value
            continue

        if key == "winnerCode":
            delta.event_fields["winner_code"] = value
            continue

        if key == "lastPeriod":
            delta.event_fields["last_period"] = value
            continue

        if key == "firstToServe":
            delta.event_fields["first_to_serve"] = value
            continue

        if key == "statusDescription":
            # Top-level statusDescription (a label like "46" for live
            # minute) is less authoritative than status.description.
            # We capture it but it does not overwrite a value already
            # set from status.description.
            delta.event_status_fields.setdefault("description_label", value)
            continue

        if key == "currentPeriodStartTimestamp":
            delta.event_time_fields["current_period_start_timestamp"] = value
            continue

        if key == "statusTime":
            if isinstance(value, dict):
                for inner_key in ("prefix", "timestamp", "initial", "max", "extra"):
                    if inner_key in value:
                        delta.event_status_time_fields[inner_key] = value[inner_key]
            continue

        if key == "varInProgress":
            if isinstance(value, dict):
                row: dict[str, Any] = {}
                if "homeTeam" in value:
                    row["home_team"] = bool(value["homeTeam"])
                if "awayTeam" in value:
                    row["away_team"] = bool(value["awayTeam"])
                if row:
                    delta.event_var_in_progress = row
            continue

        if key == "cardsCode":
            # Sofascore packs per-side red-card counts into a string —
            # "00" = no cards, "01" = away has 1, "10" = home has 1.
            # The format is exactly 2 characters, both ASCII digits;
            # anything else is upstream weirdness we ignore.
            if isinstance(value, str) and len(value) == 2 and value.isdigit():
                delta.event_fields["home_red_cards"] = int(value[0])
                delta.event_fields["away_red_cards"] = int(value[1])
            continue

        if key.startswith("eventState."):
            # eventState.{timestamp,statusIndicator} are derived from
            # varInProgress + side — no independent storage. Drop.
            continue

        if key.startswith("homeScore."):
            inner = key.split(".", 1)[1]
            col = _SCORE_COLUMN_BY_KEY.get(inner)
            if col is None:
                continue
            row = score_by_side.setdefault("home", {"side": "home"})
            row[col] = value
            continue

        if key.startswith("awayScore."):
            inner = key.split(".", 1)[1]
            col = _SCORE_COLUMN_BY_KEY.get(inner)
            if col is None:
                continue
            row = score_by_side.setdefault("away", {"side": "away"})
            row[col] = value
            continue

        if key.startswith("status."):
            inner = key.split(".", 1)[1]
            col = _STATUS_COLUMN_BY_KEY.get(inner)
            if col is None:
                continue
            delta.event_status_fields[col] = value
            continue

        if key.startswith("time."):
            inner = key.split(".", 1)[1]
            col = _TIME_COLUMN_BY_KEY.get(inner)
            if col is None:
                continue
            if inner in ("playedLastUpdated", "clockRunningLastUpdated"):
                value = _parse_php_datetime(value)
            delta.event_time_fields[col] = value
            continue

        # Unknown keys: silently ignored so a forward-compat upstream
        # change cannot break us. The consumer logs the unknown set
        # via counters so we can react.

    # Materialize score rows in a stable order (home first, away second).
    if "home" in score_by_side:
        delta.event_score_rows.append(score_by_side["home"])
    if "away" in score_by_side:
        delta.event_score_rows.append(score_by_side["away"])

    return delta


def normalize_odds_delta(payload: dict[str, Any]) -> NormalizedOddsDelta | None:
    """Convert one WS odds-type push into a NormalizedOddsDelta."""
    if not isinstance(payload, dict) or not payload:
        return None
    offer_id = payload.get("id")
    if not isinstance(offer_id, int):
        return None

    bundle = NormalizedOddsDelta(offer_id=offer_id)
    for key, value in payload.items():
        if key == "id":
            continue
        if not key.startswith("choice"):
            continue
        # key like "choice1.fractionalValue" / "choice2.initialFractionalValue"
        parts = key.split(".", 1)
        if len(parts) != 2:
            continue
        choice_label, field_name = parts
        # Extract the integer choice index.
        try:
            idx = int(choice_label.removeprefix("choice"))
        except ValueError:
            continue
        if idx < 1:
            continue
        bundle.choices.setdefault(idx, {})[field_name] = value

    return bundle
