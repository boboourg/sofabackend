"""Apply a normalized WS delta to the normalized event tables.

The writer is the I/O half of the WS pipeline:

  raw NATS push  →  ws_delta_normalizer.normalize_event_delta  →  this
                                                                  module
                                                                  →  asyncpg upserts
                                                                  →  event_payload_cache invalidation

It issues **narrow** UPSERTs against the existing schema — same columns
that the bulk live snapshot parser already writes — so the two paths
converge on the same row state. Whichever path runs last wins.

Single-event apply: each call is intended to be wrapped in its own
short transaction by the caller (the consumer dispatches one delta
per task). The writer does not start its own BEGIN/COMMIT so callers
can batch when it helps.
"""
from __future__ import annotations

import logging
from typing import Any

from .ws_delta_normalizer import NormalizedEventDelta, NormalizedOddsDelta

logger = logging.getLogger(__name__)


# Allowlist of event_score columns that may appear in a NormalizedDelta
# row. Anything else is dropped (defence against forward-compat WS
# schema drift).
_EVENT_SCORE_COLUMNS: frozenset[str] = frozenset({
    "current", "display", "aggregated", "normaltime", "overtime",
    "penalties", "period1", "period2", "period3", "period4",
    "extra1", "extra2", "series",
    # NOTE: period5 is observed in table-tennis / volleyball deltas
    # (~5K archive rows) but event_score has no column for it today.
    # Dropped here; would need a migration to add the column before
    # the writer can persist it. Same for tennis "point" (string).
})

_EVENT_FIELDS_ALLOWED: frozenset[str] = frozenset({
    "winner_code", "last_period", "first_to_serve",
    "home_red_cards", "away_red_cards",
})

_EVENT_TIME_COLUMNS: frozenset[str] = frozenset({
    "current_period_start_timestamp", "initial", "max", "extra",
    "period_length", "total_period_count", "overtime_length",
    "injury_time1", "injury_time2", "injury_time3", "injury_time4",
    "played", "played_last_updated", "clock_running",
    "clock_running_last_updated",
})

_EVENT_STATUS_TIME_COLUMNS: frozenset[str] = frozenset({
    "prefix", "timestamp", "initial", "max", "extra",
})


async def apply_event_delta(connection: Any, delta: NormalizedEventDelta) -> None:
    """Apply a single normalized event delta against the live tables.

    The writes are issued in deterministic order:
      1. event_status (upsert lookup row)  →  event.status_code, ...
      2. event (UPDATE for misc fields + status_code + updated_at bump)
      3. event_score (UPSERT per side)
      4. event_time (UPSERT)
      5. event_status_time (UPSERT)
      6. event_var_in_progress (UPSERT)
      7. event_change_item (INSERT, append-only)
      8. event_payload_cache (DELETE — invalidate persistent cache)
    """
    event_id = delta.event_id

    # 1. event_status lookup row.
    # event_status.type is NOT NULL. WS deltas frequently ship just
    # {code, description} without ``type`` for events whose status was
    # last set during a poll. INSERTing such a row would violate the
    # constraint, so we split into two paths:
    #   - if ``type`` is present: full UPSERT (existing rows get any
    #     non-null fields refreshed).
    #   - if ``type`` is absent: UPDATE the existing row's description
    #     only; do not INSERT a brand-new status code (all 25 codes
    #     observed in the 6-day archive are already seeded).
    if delta.event_status_fields:
        code = delta.event_status_fields.get("code")
        if code is not None:
            type_value = delta.event_status_fields.get("type")
            description_value = delta.event_status_fields.get("description")
            if type_value is not None:
                await connection.execute(
                    """
                    INSERT INTO event_status (code, description, type)
                    VALUES ($1, $2, $3)
                    ON CONFLICT (code) DO UPDATE SET
                        description = COALESCE(EXCLUDED.description, event_status.description),
                        type        = COALESCE(EXCLUDED.type, event_status.type)
                    """,
                    code,
                    description_value,
                    type_value,
                )
            elif description_value is not None:
                await connection.execute(
                    """
                    UPDATE event_status
                    SET description = $2
                    WHERE code = $1
                    """,
                    code,
                    description_value,
                )

    # 2. event UPDATE — combine status_code with any misc fields.
    update_columns: dict[str, Any] = {}
    if delta.event_status_fields.get("code") is not None:
        update_columns["status_code"] = delta.event_status_fields["code"]
    for k, v in delta.event_fields.items():
        if k in _EVENT_FIELDS_ALLOWED:
            update_columns[k] = v
    # Always bump updated_at so downstream stale-detection sees the
    # change (even on id-only deltas there's no UPDATE, but those
    # already hit the cache invalidate at the end which is enough).
    if update_columns:
        set_clauses = ", ".join(
            f"{col} = ${i + 2}" for i, col in enumerate(update_columns)
        )
        # Append updated_at = now()
        set_clauses += ", updated_at = now()"
        sql = f"UPDATE event SET {set_clauses} WHERE id = $1"
        await connection.execute(sql, event_id, *update_columns.values())
    else:
        # Still bump updated_at when other tables changed so the
        # stale-detection layer (local_api_server._fetch_snapshot_payload)
        # picks up the score/time/status_time delta even though the
        # event row itself was untouched.
        any_other_writes = (
            delta.event_score_rows
            or delta.event_time_fields
            or delta.event_status_time_fields
            or delta.event_var_in_progress
            or delta.change_timestamp is not None
        )
        if any_other_writes:
            await connection.execute(
                "UPDATE event SET updated_at = now() WHERE id = $1",
                event_id,
            )

    # 3. event_score upserts.
    for row in delta.event_score_rows:
        side = row.get("side")
        if side not in ("home", "away"):
            continue
        # Keep only allowlisted columns, ignore unknowns.
        cols = {k: v for k, v in row.items() if k != "side" and k in _EVENT_SCORE_COLUMNS}
        if not cols:
            continue
        col_names = list(cols.keys())
        # Build a SQL with explicit columns + placeholders so the
        # ON CONFLICT update is minimal.
        col_list = ", ".join(col_names)
        placeholder_list = ", ".join(f"${i + 3}" for i in range(len(col_names)))
        update_set = ", ".join(f"{c} = EXCLUDED.{c}" for c in col_names)
        sql = f"""
            INSERT INTO event_score (event_id, side, {col_list})
            VALUES ($1, $2, {placeholder_list})
            ON CONFLICT (event_id, side) DO UPDATE SET {update_set}
        """
        await connection.execute(sql, event_id, side, *cols.values())

    # 4. event_time upsert.
    et_fields = {
        k: v for k, v in delta.event_time_fields.items() if k in _EVENT_TIME_COLUMNS
    }
    if et_fields:
        col_list = ", ".join(et_fields.keys())
        placeholders = ", ".join(f"${i + 2}" for i in range(len(et_fields)))
        update_set = ", ".join(f"{c} = EXCLUDED.{c}" for c in et_fields)
        sql = f"""
            INSERT INTO event_time (event_id, {col_list})
            VALUES ($1, {placeholders})
            ON CONFLICT (event_id) DO UPDATE SET {update_set}
        """
        await connection.execute(sql, event_id, *et_fields.values())

    # 5. event_status_time upsert.
    est = {
        k: v for k, v in delta.event_status_time_fields.items()
        if k in _EVENT_STATUS_TIME_COLUMNS
    }
    if est:
        col_list = ", ".join(est.keys())
        placeholders = ", ".join(f"${i + 2}" for i in range(len(est)))
        update_set = ", ".join(f"{c} = EXCLUDED.{c}" for c in est)
        sql = f"""
            INSERT INTO event_status_time (event_id, {col_list})
            VALUES ($1, {placeholders})
            ON CONFLICT (event_id) DO UPDATE SET {update_set}
        """
        await connection.execute(sql, event_id, *est.values())

    # 6. event_var_in_progress upsert.
    if delta.event_var_in_progress:
        v = delta.event_var_in_progress
        await connection.execute(
            """
            INSERT INTO event_var_in_progress (event_id, home_team, away_team)
            VALUES ($1, $2, $3)
            ON CONFLICT (event_id) DO UPDATE SET
                home_team = COALESCE(EXCLUDED.home_team, event_var_in_progress.home_team),
                away_team = COALESCE(EXCLUDED.away_team, event_var_in_progress.away_team)
            """,
            event_id,
            v.get("home_team"),
            v.get("away_team"),
        )

    # 7. event_change_item — append-only ordinal log.
    if delta.change_timestamp is not None:
        # Append with next ordinal. We use a single INSERT with subquery
        # to derive the next ordinal atomically.
        await connection.execute(
            """
            INSERT INTO event_change_item (event_id, change_timestamp, ordinal, change_value)
            SELECT $1, $2,
                COALESCE((SELECT MAX(ordinal) FROM event_change_item WHERE event_id = $1), -1) + 1,
                'ws_delta'
            ON CONFLICT (event_id, ordinal) DO NOTHING
            """,
            event_id,
            delta.change_timestamp,
        )

    # 8. Invalidate persistent payload cache (event_payload_cache) for
    # this event. Always — even on id-only deltas, since the delta
    # arrived because something changed upstream.
    await connection.execute(
        """
        DELETE FROM event_payload_cache
        WHERE context_entity_type = 'event'
          AND context_entity_id = $1
        """,
        event_id,
    )


async def apply_odds_delta(connection: Any, bundle: NormalizedOddsDelta) -> None:
    """Apply a WS odds delta to event_market_choice + invalidate the
    persistent payload cache for the affected event. Delegates to the
    odds-writer module which owns the choice-name resolution and the
    UPDATE statement."""
    from .ws_odds_writer import apply_odds_delta as _apply

    await _apply(connection, bundle)
