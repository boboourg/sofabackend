"""Stale live-event detector.

Read-only diagnostics for live freshness regressions: identifies active
``refresh_live_event`` events whose root ``/api/v1/event/{event_id}``
snapshot has not advanced past a configurable threshold. Designed to be
called from CLI / cron / monitoring; returns counts at multiple
thresholds plus a top-N detail list with per-event success/retry/fail
counters from ``etl_job_run`` so on-call can immediately spot whether
the staleness is global (proxy/transport regression) or concentrated on
specific events / sports / tournaments.

Snapshot-age semantic: ``api_snapshot_head.latest_fetched_at`` advances
only on a *new* snapshot insert (different ``payload_hash``). Staleness
therefore captures both "no fetches succeeded" and "fetches succeeded
but upstream payload unchanged" — that's by design: from a freshness
point of view both look identical to the read API.

In-progress filter uses Sofascore's status_code semantics (6/7/8/31/91/
92/93 = first half / halftime / second half / paused / extra-time /
penalties); these are codes that imply ``score is changing`` upstream
so a snapshot age drift signals a problem. Terminal codes (100/110/
120/60/70 = finished/AET/AP/postponed/cancelled) are excluded because
their /event payload is permanently frozen post-finalize and natural
staleness > threshold is expected.
"""

from __future__ import annotations

from dataclasses import dataclass


# Sofascore status_code values that imply an actively progressing match
# (score may change). Used to distinguish "stale because nothing is
# happening upstream" from "stale because we cannot fetch".
_INPROGRESS_STATUS_CODES = (6, 7, 8, 31, 91, 92, 93)


@dataclass(frozen=True)
class StaleLiveEventsSummary:
    active_total: int
    inprogress_total: int
    stale_60s: int
    stale_180s: int
    stale_300s: int
    stale_600s: int
    stale_1200s: int
    inprogress_stale_60s: int
    inprogress_stale_180s: int
    inprogress_stale_300s: int
    inprogress_stale_600s: int
    inprogress_stale_1200s: int


@dataclass(frozen=True)
class StaleLiveEventDetail:
    event_id: int
    age_sec: int
    status_code: int | None
    sport_slug: str | None
    unique_tournament_name: str | None
    success_count: int
    retry_count: int
    fail_count: int


_SUMMARY_QUERY = """
WITH live_events AS (
    SELECT DISTINCT entity_id
    FROM etl_job_run
    WHERE job_type = 'refresh_live_event'
      AND started_at > now() - interval '10 minutes'
),
with_age AS (
    SELECT le.entity_id,
           ev.status_code,
           EXTRACT(EPOCH FROM (now() - ash.latest_fetched_at))::int AS age_sec
    FROM live_events le
    LEFT JOIN event ev ON ev.id = le.entity_id
    LEFT JOIN api_snapshot_head ash
        ON ash.context_entity_id = le.entity_id
        AND ash.endpoint_pattern = '/api/v1/event/{event_id}'
        AND ash.context_entity_type = 'event'
    WHERE ash.latest_fetched_at IS NOT NULL
)
SELECT
    count(*)::int AS active_total,
    count(*) FILTER (WHERE status_code = ANY($1::int[]))::int AS inprogress_total,
    count(*) FILTER (WHERE age_sec > 60)::int AS stale_60s,
    count(*) FILTER (WHERE age_sec > 180)::int AS stale_180s,
    count(*) FILTER (WHERE age_sec > 300)::int AS stale_300s,
    count(*) FILTER (WHERE age_sec > 600)::int AS stale_600s,
    count(*) FILTER (WHERE age_sec > 1200)::int AS stale_1200s,
    count(*) FILTER (WHERE age_sec > 60 AND status_code = ANY($1::int[]))::int AS inprogress_stale_60s,
    count(*) FILTER (WHERE age_sec > 180 AND status_code = ANY($1::int[]))::int AS inprogress_stale_180s,
    count(*) FILTER (WHERE age_sec > 300 AND status_code = ANY($1::int[]))::int AS inprogress_stale_300s,
    count(*) FILTER (WHERE age_sec > 600 AND status_code = ANY($1::int[]))::int AS inprogress_stale_600s,
    count(*) FILTER (WHERE age_sec > 1200 AND status_code = ANY($1::int[]))::int AS inprogress_stale_1200s
FROM with_age
"""


_TOP_DETAIL_QUERY = """
WITH live_events AS (
    SELECT DISTINCT entity_id
    FROM etl_job_run
    WHERE job_type = 'refresh_live_event'
      AND started_at > now() - interval '10 minutes'
),
per_event_jobs AS (
    SELECT entity_id,
           count(*) FILTER (WHERE status = 'succeeded')::int AS succ,
           count(*) FILTER (WHERE status = 'retry_scheduled')::int AS retry,
           count(*) FILTER (WHERE status = 'failed')::int AS fail
    FROM etl_job_run
    WHERE job_type = 'refresh_live_event'
      AND started_at > now() - interval '30 minutes'
    GROUP BY entity_id
)
SELECT le.entity_id::bigint AS event_id,
       EXTRACT(EPOCH FROM (now() - ash.latest_fetched_at))::int AS age_sec,
       ev.status_code,
       aps.sport_slug,
       ut.name AS unique_tournament_name,
       COALESCE(j.succ, 0) AS success_count,
       COALESCE(j.retry, 0) AS retry_count,
       COALESCE(j.fail, 0) AS fail_count
FROM live_events le
LEFT JOIN event ev ON ev.id = le.entity_id
LEFT JOIN unique_tournament ut ON ut.id = ev.unique_tournament_id
LEFT JOIN api_snapshot_head ash
    ON ash.context_entity_id = le.entity_id
    AND ash.endpoint_pattern = '/api/v1/event/{event_id}'
    AND ash.context_entity_type = 'event'
LEFT JOIN api_payload_snapshot aps ON aps.id = ash.latest_snapshot_id
LEFT JOIN per_event_jobs j ON j.entity_id = le.entity_id
WHERE ash.latest_fetched_at IS NOT NULL
  AND EXTRACT(EPOCH FROM (now() - ash.latest_fetched_at)) > $1::int
  AND (
      $2::boolean IS FALSE
      OR ev.status_code = ANY($3::int[])
  )
ORDER BY age_sec DESC
LIMIT $4::int
"""


async def collect_stale_live_events_summary(connection) -> StaleLiveEventsSummary:
    """Return aggregate stale-event counts at multiple thresholds.

    The query uses ``etl_job_run`` rows with job_type=refresh_live_event
    in the last 10 minutes as the "currently active" universe. Events
    not yet polled by the live worker (e.g. just promoted from
    scheduled) are excluded — their /event snapshot is irrelevant to
    live freshness.
    """
    row = await connection.fetchrow(_SUMMARY_QUERY, list(_INPROGRESS_STATUS_CODES))
    if row is None:
        return StaleLiveEventsSummary(
            active_total=0,
            inprogress_total=0,
            stale_60s=0,
            stale_180s=0,
            stale_300s=0,
            stale_600s=0,
            stale_1200s=0,
            inprogress_stale_60s=0,
            inprogress_stale_180s=0,
            inprogress_stale_300s=0,
            inprogress_stale_600s=0,
            inprogress_stale_1200s=0,
        )
    return StaleLiveEventsSummary(
        active_total=int(row["active_total"]),
        inprogress_total=int(row["inprogress_total"]),
        stale_60s=int(row["stale_60s"]),
        stale_180s=int(row["stale_180s"]),
        stale_300s=int(row["stale_300s"]),
        stale_600s=int(row["stale_600s"]),
        stale_1200s=int(row["stale_1200s"]),
        inprogress_stale_60s=int(row["inprogress_stale_60s"]),
        inprogress_stale_180s=int(row["inprogress_stale_180s"]),
        inprogress_stale_300s=int(row["inprogress_stale_300s"]),
        inprogress_stale_600s=int(row["inprogress_stale_600s"]),
        inprogress_stale_1200s=int(row["inprogress_stale_1200s"]),
    )


async def collect_top_stale_live_events(
    connection,
    *,
    threshold_seconds: int,
    limit: int = 20,
    inprogress_only: bool = True,
) -> tuple[StaleLiveEventDetail, ...]:
    """Return top-N events with snapshot age > threshold, sorted by age desc."""
    rows = await connection.fetch(
        _TOP_DETAIL_QUERY,
        int(threshold_seconds),
        bool(inprogress_only),
        list(_INPROGRESS_STATUS_CODES),
        int(limit),
    )
    return tuple(
        StaleLiveEventDetail(
            event_id=int(row["event_id"]),
            age_sec=int(row["age_sec"]),
            status_code=int(row["status_code"]) if row["status_code"] is not None else None,
            sport_slug=row["sport_slug"],
            unique_tournament_name=row["unique_tournament_name"],
            success_count=int(row["success_count"]),
            retry_count=int(row["retry_count"]),
            fail_count=int(row["fail_count"]),
        )
        for row in rows
    )


def format_stale_live_events_report(
    summary: StaleLiveEventsSummary,
    top: tuple[StaleLiveEventDetail, ...],
    *,
    threshold_seconds: int,
    inprogress_only: bool,
) -> str:
    """Format a human-readable report (used by the CLI)."""
    lines: list[str] = []
    lines.append(
        f"active={summary.active_total} inprogress={summary.inprogress_total}"
    )
    lines.append(
        "stale_60s={s60} stale_180s={s180} stale_300s={s300} stale_600s={s600} stale_1200s={s1200}".format(
            s60=summary.stale_60s,
            s180=summary.stale_180s,
            s300=summary.stale_300s,
            s600=summary.stale_600s,
            s1200=summary.stale_1200s,
        )
    )
    lines.append(
        "inprogress_stale_60s={s60} inprogress_stale_180s={s180} inprogress_stale_300s={s300} inprogress_stale_600s={s600} inprogress_stale_1200s={s1200}".format(
            s60=summary.inprogress_stale_60s,
            s180=summary.inprogress_stale_180s,
            s300=summary.inprogress_stale_300s,
            s600=summary.inprogress_stale_600s,
            s1200=summary.inprogress_stale_1200s,
        )
    )
    if top:
        lines.append("")
        scope = "inprogress only" if inprogress_only else "all events"
        lines.append(f"top {len(top)} stale events (age > {threshold_seconds}s, {scope}):")
        lines.append(
            f"  {'event_id':>10} {'age_s':>6} {'status':>6} {'succ':>4} {'retry':>5} {'fail':>4}  {'sport':<14} tournament"
        )
        for ev in top:
            lines.append(
                f"  {ev.event_id:>10} {ev.age_sec:>6} {(ev.status_code if ev.status_code is not None else '-'):>6} "
                f"{ev.success_count:>4} {ev.retry_count:>5} {ev.fail_count:>4}  "
                f"{(ev.sport_slug or '-'):<14} {ev.unique_tournament_name or '-'}"
            )
    return "\n".join(lines)
