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
    """Return a CASE expression that preserves the existing column value
    when the event has a terminal row in ``event_terminal_state``.

    Use inline inside an ``ON CONFLICT ... DO UPDATE SET`` clause:

        f"winner_code = {terminal_guard_case(table='event', event_fk='id', column='winner_code')},"

    Args:
        table:     name of the table being UPSERTed (e.g. ``"event"``,
                   ``"event_score"``).
        event_fk:  column on ``table`` that references ``event.id``
                   (``"id"`` for the ``event`` table itself,
                   ``"event_id"`` for child tables).
        column:    column being guarded.

    The fall-through branch is plain ``EXCLUDED.<column>`` — no NULL
    fallback, matching the existing legacy behaviour for every guarded
    field in event_list/event_detail/event_score writers (which never
    used COALESCE). For tables that previously DID use COALESCE (only
    normalize_repository's status_code), use a dedicated CASE expression
    inline (see normalize_repository.py).
    """
    return (
        "CASE "
        "WHEN EXISTS ("
        "SELECT 1 FROM event_terminal_state ets "
        f"WHERE ets.event_id = {table}.{event_fk} "
        f"AND ets.terminal_status IN {TERMINAL_STATUS_TYPES_SQL}"
        ") "
        f"THEN {table}.{column} "
        f"ELSE EXCLUDED.{column} "
        "END"
    )
