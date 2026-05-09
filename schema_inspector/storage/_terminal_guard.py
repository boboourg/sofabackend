"""SQL helpers for the F-8 monotonic terminal-state guard.

Shared between every repository that UPSERTs lifecycle-sensitive fields
on the ``event`` family of tables. The guard preserves an existing
column value when ``event_terminal_state`` already holds a terminal row
for the event — so a delayed-insert snapshot whose payload says
``inprogress`` (e.g., the 14083568 case audited on 2026-05-09) cannot
overwrite a finished match's ``status_code`` / ``winner_code`` /
``aggregated_winner_code`` / ``last_period`` / scores.

The terminal status types mirror
``schema_inspector/planner/live.py::TERMINAL_STATUS_TYPES`` so adding
a new terminal status (e.g. a new sport's "abandoned" code) requires
updating both the planner switch (which decides
JOB_FINALIZE_EVENT vs JOB_TRACK_LIVE_EVENT) and this SQL list.
"""
from __future__ import annotations

# WARNING: keep in sync with planner/live.py::TERMINAL_STATUS_TYPES.
# A mismatch would either:
#   * leave new terminal types ungated (regression vector reopens), or
#   * gate a status the planner still treats as live (events stuck).
TERMINAL_STATUS_TYPES_SQL = (
    "('finished', 'afterextra', 'afterpen', 'cancelled', 'canceled', 'postponed')"
)


def terminal_guard_case(*, table: str, event_fk: str, column: str) -> str:
    """Return a CASE expression that prevents downgrading a non-NULL
    column value once the event has a terminal row in
    ``event_terminal_state`` — while still allowing an initial NULL→value
    fill.

    Use inline inside an ``ON CONFLICT ... DO UPDATE SET`` clause:

        f"winner_code = {terminal_guard_case(table='event', event_fk='id', column='winner_code')},"

    Truth table per (existing column value, incoming EXCLUDED value)
    when ``event_terminal_state`` already exists for the event:

        existing  EXCLUDED   result        rationale
        --------  --------   ------        ---------
        NULL      NULL       NULL          no-op
        NULL      v          v             initial fill ALLOWED (e.g.,
                                           winner_code transitions
                                           NULL→3 when finalize parse
                                           lands after a terminal_state
                                           insert that beat it)
        a         NULL       a             NULL erasure BLOCKED
        a         a          a             idempotent
        a         b          a             value→value regression BLOCKED
                                           (the 14083568-class bug —
                                           stale "1st half" cannot
                                           overwrite finished status)

    Without ``event_terminal_state``: pure ``EXCLUDED.<column>`` — the
    legacy semantics for every writer that previously did not use
    COALESCE (event_list / event_detail / event_score). Note: this
    fall-through is also COALESCE-free because legacy callers expect
    NULLs to flow through (e.g., capability flags reset to NULL).

    Hotfix history: the original Phase 1.5 helper used
    ``THEN {table}.{column}`` directly which over-blocked NULL→value
    transitions and produced 138 events with NULL winner_code in 30
    minutes on 2026-05-09. Replaced with COALESCE pattern so the guard
    only blocks downgrades, not initial fills.

    Args:
        table:     name of the table being UPSERTed (e.g. ``"event"``,
                   ``"event_score"``).
        event_fk:  column on ``table`` that references ``event.id``
                   (``"id"`` for the ``event`` table itself,
                   ``"event_id"`` for child tables).
        column:    column being guarded.
    """
    return (
        "CASE "
        "WHEN EXISTS ("
        "SELECT 1 FROM event_terminal_state ets "
        f"WHERE ets.event_id = {table}.{event_fk} "
        f"AND ets.terminal_status IN {TERMINAL_STATUS_TYPES_SQL}"
        ") "
        f"THEN COALESCE({table}.{column}, EXCLUDED.{column}) "
        f"ELSE EXCLUDED.{column} "
        "END"
    )
