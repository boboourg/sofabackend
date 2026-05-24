"""diagnose-event CLI subcommand — single-event end-to-end coverage audit.

For a given event_id, compares EXPECTED endpoints (resolved via
``detail_resource_policy.build_event_detail_request_specs``) against
ACTUAL state in ``api_payload_snapshot`` + normalized fact tables.

Each endpoint is classified into one verdict:

  - ``GREEN``       snapshot fresh + normalized rows present
  - ``SILENT_DROP`` snapshot present, http_status 2xx, but 0 normalized rows
  - ``NO_FETCH``    snapshot never recorded for this event
  - ``NO_PARSER``   endpoint requested + snapshot present, no parser maps
                    it to a normalized table (raw passthrough only)
  - ``NEGATIVE``    last snapshot http_status == 404 (upstream said no data)
  - ``EMPTY_RAW``   snapshot present but ``is_soft_error_payload=True``

The verdict table mirrors what would happen if we replay the same event
through the full backfill pipeline. Use this as the *pre-flight check*
before launching ``slice-pipeline`` or full historical backfill.

CLI examples::

    # known event, full audit
    python -m schema_inspector.cli diagnose-event \
        --sport-slug football --event-id 14083191

    # auto-pick a recent finished top-tier event
    python -m schema_inspector.cli diagnose-event \
        --sport-slug football --auto-pick

    # CI gate: exit 2 if any silent drop detected
    python -m schema_inspector.cli diagnose-event \
        --sport-slug football --auto-pick --exit-on-silent-drop

The module is *read-only* w.r.t. production state — no HTTP fetches,
no Redis writes, only SELECT against PostgreSQL. Safe to run on prod.
"""
from __future__ import annotations

import argparse
import logging
from dataclasses import dataclass, field
from typing import Any, Sequence

from .detail_resource_policy import (
    EventDetailRequestSpec,
    build_event_detail_request_specs,
    supports_live_detail_resources,
)

# Core edges — always fetched by pilot_orchestrator for any event being
# hydrated (status notstarted/inprogress/finished). These are wired
# directly in ``pipeline/pilot_orchestrator.py`` (search for
# EVENT_LINEUPS_ENDPOINT, EVENT_INCIDENTS_ENDPOINT, EVENT_STATISTICS_ENDPOINT)
# rather than via detail_resource_policy.
_CORE_EDGE_PATTERNS: tuple[str, ...] = (
    "/api/v1/event/{event_id}/lineups",
    "/api/v1/event/{event_id}/incidents",
    "/api/v1/event/{event_id}/statistics",
)

logger = logging.getLogger(__name__)

# Verdicts -----------------------------------------------------------------

VERDICT_GREEN = "GREEN"
VERDICT_SILENT_DROP = "SILENT_DROP"
VERDICT_NO_FETCH = "NO_FETCH"
VERDICT_NO_PARSER = "NO_PARSER"
VERDICT_NEGATIVE = "NEGATIVE"
VERDICT_EMPTY_RAW = "EMPTY_RAW"

_VERDICT_ICON: dict[str, str] = {
    VERDICT_GREEN: "[+]",
    VERDICT_SILENT_DROP: "[!]",
    VERDICT_NO_FETCH: "[-]",
    VERDICT_NO_PARSER: "[~]",
    VERDICT_NEGATIVE: "[404]",
    VERDICT_EMPTY_RAW: "[!]",
}

# Endpoint → list of normalized tables that should receive rows ------------
#
# Sources: schema_inspector/parsers/registry.py + storage/normalize_repository.py.
# Endpoint patterns must match ``api_payload_snapshot.endpoint_pattern`` exactly.
#
# Endpoints with empty target tuple = NO_PARSER (snapshot kept raw only).
_ENDPOINT_TARGETS: dict[str, tuple[str, ...]] = {
    "/api/v1/event/{event_id}": (
        "event",
        "event_score",
        "event_status",
        "event_time",
        "event_round_info",
    ),
    "/api/v1/event/{event_id}/lineups": (
        "event_lineup",
        "event_lineup_player",
    ),
    "/api/v1/event/{event_id}/statistics": ("event_statistic",),
    "/api/v1/event/{event_id}/incidents": ("event_incident",),
    "/api/v1/event/{event_id}/comments": ("event_comment",),
    "/api/v1/event/{event_id}/managers": ("event_manager_assignment",),
    "/api/v1/event/{event_id}/h2h": ("event_duel",),
    "/api/v1/event/{event_id}/pregame-form": ("event_pregame_form",),
    "/api/v1/event/{event_id}/votes": ("event_vote_option",),
    "/api/v1/event/{event_id}/graph": ("event_graph", "event_graph_point"),
    "/api/v1/event/{event_id}/heatmap/{team_id}": (
        "event_team_heatmap",
        "event_team_heatmap_point",
    ),
    "/api/v1/event/{event_id}/odds/{provider_id}/all": (
        "event_market",
        "event_market_choice",
    ),
    "/api/v1/event/{event_id}/odds/{provider_id}/featured": (
        "event_market",
        "event_market_choice",
    ),
    "/api/v1/event/{event_id}/provider/{provider_id}/winning-odds": (
        "event_winning_odds",
    ),
    "/api/v1/event/{event_id}/best-players/summary": (
        "event_best_player_entry",
    ),
    "/api/v1/event/{event_id}/shotmap": ("shotmap_point",),
    "/api/v1/event/{event_id}/point-by-point": ("tennis_point_by_point",),
    "/api/v1/event/{event_id}/tennis-power": ("tennis_power",),
    "/api/v1/event/{event_id}/atbat/{at_bat_id}/pitches": ("baseball_pitch",),
    "/api/v1/event/{event_id}/innings": ("baseball_inning",),
    "/api/v1/event/{event_id}/esports-games": ("esports_game",),
    "/api/v1/event/{event_id}/h2h/{custom_id}/events": (),  # synthesizer-only
    # Known unparsed (snapshot kept raw, no normalized target):
    "/api/v1/event/{event_id}/highlights": (),
    "/api/v1/event/{event_id}/team-streaks": (),
    "/api/v1/event/{event_id}/team-streaks/betting-odds/{provider_id}": (),
    "/api/v1/event/{event_id}/official-tweets": (),
    "/api/v1/event/{event_id}/average-positions": (),
    "/api/v1/event/{event_id}/player/{player_id}/heatmap": (),
    "/api/v1/event/{event_id}/shotmap/player/{player_id}": (),
    "/api/v1/event/{event_id}/goalkeeper-shotmap/player/{player_id}": (),
    "/api/v1/event/{event_id}/player/{player_id}/rating-breakdown": (
        "event_player_rating_breakdown_action",
    ),
    "/api/v1/event/{event_id}/player/{player_id}/statistics": (
        "event_player_statistics",
        "event_player_stat_value",
    ),
}


# Data classes -------------------------------------------------------------


@dataclass
class EndpointAudit:
    """One endpoint's audit row.

    ``normalized_rows`` is a per-target-table count, e.g.
    ``{'event_lineup': 2, 'event_lineup_player': 30}``. ``verdict`` is the
    final classification used in CI / dashboards.
    """

    endpoint_pattern: str
    target_tables: tuple[str, ...] = ()
    latest_snapshot_id: int | None = None
    latest_snapshot_fetched_at: Any = None
    latest_snapshot_http_status: int | None = None
    latest_snapshot_size_bytes: int | None = None
    latest_snapshot_is_soft_error: bool = False
    normalized_rows: dict[str, int] = field(default_factory=dict)
    verdict: str = "UNKNOWN"
    note: str | None = None

    def normalized_rows_total(self) -> int:
        return sum(self.normalized_rows.values())


@dataclass
class EventDiagnosis:
    """Aggregate audit result for one event.

    The integer counts (``green_count``/etc) are derived from ``audits``
    on demand so the dataclass stays immutable-friendly.
    """

    event_id: int
    sport_slug: str
    status_type: str | None = None
    status_code: int | None = None
    detail_id: int | None = None
    is_editor: bool | None = None
    start_timestamp: int | None = None
    audits: tuple[EndpointAudit, ...] = ()
    event_found_in_db: bool = True

    def count_by_verdict(self, verdict: str) -> int:
        return sum(1 for audit in self.audits if audit.verdict == verdict)

    @property
    def green_count(self) -> int:
        return self.count_by_verdict(VERDICT_GREEN)

    @property
    def silent_drop_count(self) -> int:
        return self.count_by_verdict(VERDICT_SILENT_DROP)

    @property
    def no_fetch_count(self) -> int:
        return self.count_by_verdict(VERDICT_NO_FETCH)

    @property
    def no_parser_count(self) -> int:
        return self.count_by_verdict(VERDICT_NO_PARSER)


# DB queries ---------------------------------------------------------------


_EVENT_META_QUERY = """
SELECT
    e.id,
    e.status_code,
    e.detail_id,
    e.custom_id,
    e.start_timestamp,
    e.is_editor,
    e.has_event_player_statistics,
    e.has_event_player_heat_map,
    e.has_global_highlights,
    e.has_xg,
    e.home_team_id,
    e.away_team_id,
    es.type AS status_type
FROM event e
LEFT JOIN event_status es ON es.code = e.status_code
WHERE e.id = $1
"""


_SNAPSHOT_QUERY = """
SELECT
    id,
    fetched_at,
    http_status,
    payload_size_bytes,
    is_soft_error_payload
FROM api_payload_snapshot
WHERE context_event_id = $1
  AND endpoint_pattern = $2
ORDER BY fetched_at DESC
LIMIT 1
"""


_AUTO_PICK_QUERY = """
SELECT e.id
FROM event e
JOIN event_status es ON es.code = e.status_code
LEFT JOIN unique_tournament ut ON ut.id = e.unique_tournament_id
WHERE es.type = 'finished'
  AND e.start_timestamp > extract(epoch FROM NOW() - INTERVAL '7 days')
  AND COALESCE(ut.user_count, 0) > $1
  AND (e.is_editor IS NULL OR e.is_editor = FALSE)
ORDER BY ut.user_count DESC NULLS LAST, e.start_timestamp DESC
LIMIT 1
"""


async def _fetch_event_metadata(connection: Any, *, event_id: int) -> dict | None:
    row = await connection.fetchrow(_EVENT_META_QUERY, int(event_id))
    if row is None:
        return None
    return dict(row)


async def _fetch_endpoint_snapshot(
    connection: Any,
    *,
    event_id: int,
    endpoint_pattern: str,
) -> dict | None:
    row = await connection.fetchrow(
        _SNAPSHOT_QUERY,
        int(event_id),
        str(endpoint_pattern),
    )
    if row is None:
        return None
    return dict(row)


async def _count_normalized_rows(
    connection: Any,
    *,
    event_id: int,
    table_name: str,
) -> int:
    # All target tables in _ENDPOINT_TARGETS have an event_id column.
    # SQL injection is impossible here: table_name comes from a fixed
    # dict above, never from user input.
    sql = f"SELECT COUNT(*) FROM {table_name} WHERE event_id = $1"
    try:
        value = await connection.fetchval(sql, int(event_id))
    except Exception as exc:  # pragma: no cover - defensive logging
        logger.warning(
            "diagnose_event: count failed table=%s event_id=%s err=%r",
            table_name,
            event_id,
            exc,
        )
        return 0
    return int(value or 0)


async def auto_pick_event_id(
    connection: Any,
    *,
    sport_slug: str,
    min_user_count: int = 50000,
) -> int | None:
    """Pick a recent (≤7 d) finished event from a top-tier tournament.

    Filters out SofaEditor events (3-layer HARD BAN). Uses ``user_count``
    as a proxy for "top-tier" — defaults to 50k. Falls back to most-
    recent if no tournament meets the threshold.
    """
    # sport_slug is currently informational only — we don't have a direct
    # sport_slug column on event. A proper join sport→category→ut adds
    # cost. Most operators only run prod on football today, so this is
    # acceptable; document in CLI help.
    del sport_slug
    value = await connection.fetchval(_AUTO_PICK_QUERY, int(min_user_count))
    return int(value) if value is not None else None


# Core diagnose -----------------------------------------------------------


def _classify(
    snapshot: dict | None,
    target_tables: tuple[str, ...],
    normalized_rows: dict[str, int],
) -> tuple[str, str | None]:
    """Map a single endpoint's raw+normalized state into a verdict."""
    if snapshot is None:
        return VERDICT_NO_FETCH, "Никогда не запрашивалось"

    http_status = snapshot.get("http_status")
    is_soft_error = bool(snapshot.get("is_soft_error_payload"))

    if http_status == 404:
        return VERDICT_NEGATIVE, "Upstream 404 (negative cache)"
    if is_soft_error:
        return VERDICT_EMPTY_RAW, "Sofascore вернул soft-error payload"

    if not target_tables:
        return VERDICT_NO_PARSER, "Snapshot есть, но в proj нет парсера"

    total = sum(normalized_rows.values())
    if total <= 0:
        return (
            VERDICT_SILENT_DROP,
            "Snapshot http_status ok, но 0 строк в normalized таблицах",
        )

    return VERDICT_GREEN, None


def _expected_endpoint_patterns(
    metadata: dict,
    *,
    sport_slug: str,
    provider_ids: Sequence[int] = (1,),
) -> tuple[str, ...]:
    """Run ``build_event_detail_request_specs`` on event metadata.

    Always prepends the root ``/api/v1/event/{event_id}`` because that's
    the canonical first fetch handled by the hydrate worker before
    detail fanout — it's not part of ``build_event_detail_request_specs``
    output.
    """
    team_ids: tuple[int, ...] = ()
    home = metadata.get("home_team_id")
    away = metadata.get("away_team_id")
    if isinstance(home, int):
        team_ids = team_ids + (home,)
    if isinstance(away, int):
        team_ids = team_ids + (away,)

    specs: tuple[EventDetailRequestSpec, ...] = build_event_detail_request_specs(
        sport_slug=sport_slug,
        status_type=metadata.get("status_type"),
        team_ids=team_ids,
        provider_ids=tuple(int(pid) for pid in provider_ids),
        has_event_player_statistics=metadata.get("has_event_player_statistics"),
        has_event_player_heat_map=metadata.get("has_event_player_heat_map"),
        has_global_highlights=metadata.get("has_global_highlights"),
        has_xg=metadata.get("has_xg"),
        detail_id=metadata.get("detail_id"),
        custom_id=metadata.get("custom_id"),
        start_timestamp=metadata.get("start_timestamp"),
        is_editor=metadata.get("is_editor"),
    )

    patterns: list[str] = ["/api/v1/event/{event_id}"]
    seen: set[str] = {patterns[0]}

    # Core edges (lineups/incidents/statistics) — always fetched by
    # pilot_orchestrator for inprogress/finished football events. We
    # add them unconditionally so the audit reports the SILENT_DROP
    # case when the snapshot exists but the normalized row count is 0
    # (the original parser-reliability finding for /h2h applies here too).
    # The is_editor short-circuit above means football editors get only
    # the root — that's already handled by build_event_detail_request_specs
    # returning ().
    is_football = str(sport_slug or "").strip().lower() == "football"
    if is_football and metadata.get("is_editor") is True:
        return tuple(patterns)
    if supports_live_detail_resources(metadata.get("status_type")):
        for pattern in _CORE_EDGE_PATTERNS:
            if pattern not in seen:
                seen.add(pattern)
                patterns.append(pattern)

    for spec in specs:
        pattern = spec.endpoint.pattern
        if pattern in seen:
            continue
        seen.add(pattern)
        patterns.append(pattern)
    return tuple(patterns)


async def diagnose_event(
    database: Any,
    *,
    event_id: int,
    sport_slug: str,
    provider_ids: Sequence[int] = (1,),
) -> EventDiagnosis:
    """Build a full ``EventDiagnosis`` for one event_id.

    Pulls one DB connection from ``database`` and runs all SELECTs over
    it. Order:
      1. event metadata + status_type
      2. expected endpoints (policy)
      3. for each endpoint: latest snapshot + per-target-table COUNT(*)
      4. classify each endpoint
    """
    async with database.connection() as connection:
        metadata = await _fetch_event_metadata(connection, event_id=event_id)
        if metadata is None:
            return EventDiagnosis(
                event_id=int(event_id),
                sport_slug=sport_slug,
                event_found_in_db=False,
            )

        expected_patterns = _expected_endpoint_patterns(
            metadata,
            sport_slug=sport_slug,
            provider_ids=provider_ids,
        )

        audits: list[EndpointAudit] = []
        for pattern in expected_patterns:
            target_tables = _ENDPOINT_TARGETS.get(pattern, ())
            snapshot = await _fetch_endpoint_snapshot(
                connection,
                event_id=event_id,
                endpoint_pattern=pattern,
            )
            rows: dict[str, int] = {}
            for table_name in target_tables:
                rows[table_name] = await _count_normalized_rows(
                    connection,
                    event_id=event_id,
                    table_name=table_name,
                )

            verdict, note = _classify(snapshot, target_tables, rows)

            audits.append(
                EndpointAudit(
                    endpoint_pattern=pattern,
                    target_tables=target_tables,
                    latest_snapshot_id=snapshot.get("id") if snapshot else None,
                    latest_snapshot_fetched_at=(
                        snapshot.get("fetched_at") if snapshot else None
                    ),
                    latest_snapshot_http_status=(
                        snapshot.get("http_status") if snapshot else None
                    ),
                    latest_snapshot_size_bytes=(
                        snapshot.get("payload_size_bytes") if snapshot else None
                    ),
                    latest_snapshot_is_soft_error=(
                        bool(snapshot.get("is_soft_error_payload"))
                        if snapshot
                        else False
                    ),
                    normalized_rows=rows,
                    verdict=verdict,
                    note=note,
                )
            )

        return EventDiagnosis(
            event_id=int(event_id),
            sport_slug=sport_slug,
            status_type=metadata.get("status_type"),
            status_code=metadata.get("status_code"),
            detail_id=metadata.get("detail_id"),
            is_editor=metadata.get("is_editor"),
            start_timestamp=metadata.get("start_timestamp"),
            audits=tuple(audits),
        )


# Output ------------------------------------------------------------------


def format_diagnosis(diagnosis: EventDiagnosis) -> str:
    """Render an ``EventDiagnosis`` as a multi-line plain-text report."""
    if not diagnosis.event_found_in_db:
        return (
            f"diagnose-event: event_id={diagnosis.event_id} "
            f"sport={diagnosis.sport_slug} NOT FOUND in event table"
        )

    lines: list[str] = []
    lines.append("=== diagnose-event ===")
    lines.append(
        f"event_id={diagnosis.event_id} "
        f"sport={diagnosis.sport_slug} "
        f"status={diagnosis.status_type or '-'} "
        f"status_code={diagnosis.status_code or '-'} "
        f"detail_id={diagnosis.detail_id or '-'} "
        f"is_editor={diagnosis.is_editor}"
    )
    lines.append("")
    lines.append(f"endpoints expected: {len(diagnosis.audits)}")
    lines.append(f"  GREEN:       {diagnosis.green_count}")
    lines.append(f"  SILENT_DROP: {diagnosis.silent_drop_count}")
    lines.append(f"  NO_FETCH:    {diagnosis.no_fetch_count}")
    lines.append(f"  NO_PARSER:   {diagnosis.no_parser_count}")
    lines.append(
        f"  NEGATIVE:    {diagnosis.count_by_verdict(VERDICT_NEGATIVE)}"
    )
    lines.append(
        f"  EMPTY_RAW:   {diagnosis.count_by_verdict(VERDICT_EMPTY_RAW)}"
    )
    lines.append("")

    # Table headers
    lines.append(
        f"{'endpoint_pattern':<60} {'verdict':<14} {'http':<5} "
        f"{'snapshot_age':<13} {'rows':<30}"
    )
    lines.append(
        f"{'-'*60} {'-'*14} {'-'*5} {'-'*13} {'-'*30}"
    )

    for audit in diagnosis.audits:
        icon = _VERDICT_ICON.get(audit.verdict, "?")
        verdict_label = f"{icon} {audit.verdict}"
        http = (
            str(audit.latest_snapshot_http_status)
            if audit.latest_snapshot_http_status is not None
            else "-"
        )
        snapshot_age = _format_snapshot_age(audit.latest_snapshot_fetched_at)
        rows_str = (
            ", ".join(f"{t}={n}" for t, n in audit.normalized_rows.items())
            if audit.normalized_rows
            else "(no parser)"
        )

        # Truncate endpoint pattern if too long for table.
        pattern_display = audit.endpoint_pattern
        if len(pattern_display) > 60:
            pattern_display = pattern_display[:57] + "..."

        lines.append(
            f"{pattern_display:<60} {verdict_label:<14} {http:<5} "
            f"{snapshot_age:<13} {rows_str:<30}"
        )
        if audit.note:
            lines.append(f"{'':>60}    {audit.note}")

    return "\n".join(lines)


def _format_snapshot_age(fetched_at: Any) -> str:
    """Pretty-print a snapshot's age relative to now.

    ``fetched_at`` is a ``datetime`` from asyncpg. Returns ``-`` for None,
    ``Xs/Xm/Xh/Xd`` for ages. Failures gracefully return ``?`` so the
    report never crashes on a malformed timestamp.
    """
    if fetched_at is None:
        return "-"
    try:
        from datetime import datetime, timezone

        now = datetime.now(timezone.utc)
        if fetched_at.tzinfo is None:
            fetched_at = fetched_at.replace(tzinfo=timezone.utc)
        delta = now - fetched_at
        seconds = int(delta.total_seconds())
        if seconds < 60:
            return f"{seconds}s"
        if seconds < 3600:
            return f"{seconds // 60}m"
        if seconds < 86400:
            return f"{seconds // 3600}h"
        return f"{seconds // 86400}d"
    except Exception:  # pragma: no cover - defensive
        return "?"


# Argparse wiring ---------------------------------------------------------


def add_diagnose_event_parser(subparsers: Any) -> None:
    """Register the ``diagnose-event`` subparser on a parent argparse group.

    Called from ``cli.py`` to plug into the global CLI alongside ``event``,
    ``audit-db``, ``replay``, etc.
    """
    parser = subparsers.add_parser(
        "diagnose-event",
        help=(
            "Audit single-event end-to-end coverage. Compares EXPECTED "
            "endpoints (policy) vs ACTUAL snapshot+normalized state. "
            "Read-only; safe to run on prod. Useful as pre-flight before "
            "historical backfill."
        ),
    )
    parser.add_argument(
        "--sport-slug",
        required=True,
        help="Sport slug for policy resolution (e.g. football).",
    )
    parser.add_argument(
        "--event-id",
        type=int,
        default=None,
        help="Explicit event id. Mutually exclusive with --auto-pick.",
    )
    parser.add_argument(
        "--auto-pick",
        action="store_true",
        help=(
            "Pick a recent (≤7d) finished event from a top-tier "
            "tournament (user_count > 50k). Useful for CI."
        ),
    )
    parser.add_argument(
        "--min-user-count",
        type=int,
        default=50000,
        help="Minimum tournament user_count for --auto-pick (default 50000).",
    )
    parser.add_argument(
        "--provider-id",
        type=int,
        action="append",
        default=None,
        help="Odds provider ids to include (repeatable). Defaults to [1].",
    )
    parser.add_argument(
        "--exit-on-silent-drop",
        action="store_true",
        help=(
            "Exit with code 2 if any SILENT_DROP detected. Use as CI gate "
            "before launching historical backfill."
        ),
    )


async def dispatch_diagnose_event(args: argparse.Namespace, *, app: Any) -> int:
    """Run the ``diagnose-event`` subcommand and return a process exit code.

    Wired from ``cli.py`` dispatcher. Returns:
      0 = audit complete (regardless of verdict mix)
      1 = bad args / event not found
      2 = SILENT_DROP found and ``--exit-on-silent-drop`` set
    """
    if args.event_id is None and not args.auto_pick:
        logger.error("diagnose-event: must specify --event-id or --auto-pick")
        return 1

    provider_ids = tuple(int(pid) for pid in (args.provider_id or [1]))

    if args.event_id is None:
        async with app.database.connection() as connection:
            picked = await auto_pick_event_id(
                connection,
                sport_slug=args.sport_slug,
                min_user_count=int(args.min_user_count),
            )
        if picked is None:
            logger.error(
                "diagnose-event: no event found via --auto-pick "
                "(sport=%s min_user_count=%s)",
                args.sport_slug,
                args.min_user_count,
            )
            return 1
        print(f"auto-picked event_id={picked}")
        event_id = picked
    else:
        event_id = int(args.event_id)

    diagnosis = await diagnose_event(
        app.database,
        event_id=event_id,
        sport_slug=args.sport_slug,
        provider_ids=provider_ids,
    )
    print(format_diagnosis(diagnosis))

    if not diagnosis.event_found_in_db:
        return 1
    if args.exit_on_silent_drop and diagnosis.silent_drop_count > 0:
        return 2
    return 0
