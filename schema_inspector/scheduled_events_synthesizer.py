"""Synthesize Sofascore-style ``/api/v1/sport/{slug}/scheduled-events/{date}``
payloads from our normalized tables.

Why this exists:
  The cache_warmer + planner only fetch today's (and optionally tomorrow's)
  ``/scheduled-events/{date}`` from upstream. For any other date the API
  used to return ``{"events": []}`` — even though the historical-tournament
  backfill has already ingested the full season fixture list for every
  active competition into ``event`` + sibling normalized tables.

  This module assembles the same envelope from those normalized rows,
  so a request for a date 3 weeks / 6 months ahead returns a real,
  Sofascore-compatible payload with no upstream fetch.

Public surface (incremental — added per TDD cycle):

  * :func:`build_payload(rows)`: pure JSON assembler. Takes a sequence of
    row dicts (one per event, already joined with team / tournament /
    season / status / score), returns the wire-format ``{"events": [...]}``
    envelope. DB-free so we can pin payload shape from unit tests.
"""

from __future__ import annotations

import logging
from typing import Any, Sequence

logger = logging.getLogger(__name__)


# Column expression list used by the fetch query. Single source of truth so
# the row keys consumed by ``_build_event`` cannot drift from the SQL.
# The query is one big LEFT JOIN with LATERAL subqueries for the side-
# specific score row and the pre-aggregated changes payload.
#
# Both fetchers (date-range scheduled and active-live) share the same SELECT
# projection and FROM/JOIN graph below; they only differ in the WHERE clause.
_FETCH_SELECT_AND_JOINS = """
SELECT
    e.id                                        AS event_id,
    e.slug                                      AS event_slug,
    e.custom_id                                 AS custom_id,
    e.detail_id                                 AS detail_id,
    e.start_timestamp                           AS start_timestamp,
    e.winner_code                               AS winner_code,
    e.aggregated_winner_code                    AS aggregated_winner_code,
    e.has_xg                                    AS has_xg,
    e.has_global_highlights                     AS has_global_highlights,
    e.has_event_player_statistics               AS has_event_player_statistics,
    e.has_event_player_heat_map                 AS has_event_player_heat_map,
    e.feed_locked                               AS feed_locked,
    e.is_editor                                 AS is_editor,
    e.show_toto_promo                           AS show_toto_promo,
    e.crowdsourcing_enabled                     AS crowdsourcing_enabled,
    e.crowdsourcing_data_display_enabled        AS crowdsourcing_data_display_enabled,
    e.final_result_only                         AS final_result_only,
    e.coverage                                  AS coverage,
    e.home_red_cards                            AS home_red_cards,
    e.away_red_cards                            AS away_red_cards,
    e.previous_leg_event_id                     AS previous_leg_event_id,
    e.cup_matches_in_round                      AS cup_matches_in_round,
    e.default_period_count                      AS default_period_count,
    e.default_period_length                     AS default_period_length,
    e.default_overtime_length                   AS default_overtime_length,
    e.last_period                               AS last_period,
    e.status_code                               AS status_code,
    st.type                                     AS status_type,
    st.description                              AS status_description,
    se.id                                       AS season_id,
    se.name                                     AS season_name,
    se.year                                     AS season_year,
    se.editor                                   AS season_editor,
    -- home team
    ht.id                  AS home_team_id,
    ht.name                AS home_team_name,
    ht.slug                AS home_team_slug,
    ht.short_name          AS home_team_short_name,
    ht.full_name           AS home_team_full_name,
    ht.name_code           AS home_team_name_code,
    ht.gender              AS home_team_gender,
    ht.type                AS home_team_type,
    ht.national            AS home_team_national,
    ht.disabled            AS home_team_disabled,
    ht.user_count          AS home_team_user_count,
    ht.team_colors         AS home_team_team_colors,
    ht.field_translations  AS home_team_field_translations,
    ht.country_alpha2      AS home_team_country_alpha2,
    hc.name                AS home_team_country_name,
    hc.slug                AS home_team_country_slug,
    hc.alpha3              AS home_team_country_alpha3,
    hsp.id                 AS home_team_sport_id,
    hsp.name               AS home_team_sport_name,
    hsp.slug               AS home_team_sport_slug,
    -- away team
    at_.id                 AS away_team_id,
    at_.name               AS away_team_name,
    at_.slug               AS away_team_slug,
    at_.short_name         AS away_team_short_name,
    at_.full_name          AS away_team_full_name,
    at_.name_code          AS away_team_name_code,
    at_.gender             AS away_team_gender,
    at_.type               AS away_team_type,
    at_.national           AS away_team_national,
    at_.disabled           AS away_team_disabled,
    at_.user_count         AS away_team_user_count,
    at_.team_colors        AS away_team_team_colors,
    at_.field_translations AS away_team_field_translations,
    at_.country_alpha2     AS away_team_country_alpha2,
    ac.name                AS away_team_country_name,
    ac.slug                AS away_team_country_slug,
    ac.alpha3              AS away_team_country_alpha3,
    asp.id                 AS away_team_sport_id,
    asp.name               AS away_team_sport_name,
    asp.slug               AS away_team_sport_slug,
    -- tournament
    t.id                   AS tournament_id,
    t.name                 AS tournament_name,
    t.slug                 AS tournament_slug,
    t.priority             AS tournament_priority,
    t.is_group             AS tournament_is_group,
    t.is_live              AS tournament_is_live,
    t.competition_type     AS tournament_competition_type,
    t.group_name           AS tournament_group_name,
    t.group_sign           AS tournament_group_sign,
    t.field_translations   AS tournament_field_translations,
    ut.id                  AS ut_id,
    ut.name                AS ut_name,
    ut.slug                AS ut_slug,
    ut.user_count          AS ut_user_count,
    ut.has_event_player_statistics  AS ut_has_event_player_statistics,
    ut.has_performance_graph_feature AS ut_has_performance_graph_feature,
    ut.tier                AS ut_tier,
    ut.primary_color_hex   AS ut_primary_color_hex,
    ut.secondary_color_hex AS ut_secondary_color_hex,
    c.id                   AS category_id,
    c.name                 AS category_name,
    c.slug                 AS category_slug,
    c.priority             AS category_priority,
    c.flag                 AS category_flag,
    sp.id                  AS category_sport_id,
    sp.name                AS category_sport_name,
    sp.slug                AS category_sport_slug,
    -- scores (LATERAL with explicit side filter)
    hs.current             AS home_score_current,
    hs.display             AS home_score_display,
    hs.aggregated          AS home_score_aggregated,
    hs.normaltime          AS home_score_normaltime,
    hs.overtime            AS home_score_overtime,
    hs.penalties           AS home_score_penalties,
    hs.period1             AS home_score_period1,
    hs.period2             AS home_score_period2,
    as_.current            AS away_score_current,
    as_.display            AS away_score_display,
    as_.aggregated         AS away_score_aggregated,
    as_.normaltime         AS away_score_normaltime,
    as_.overtime           AS away_score_overtime,
    as_.penalties          AS away_score_penalties,
    as_.period1            AS away_score_period1,
    as_.period2            AS away_score_period2,
    -- round info
    eri.round_number       AS round_number,
    eri.slug               AS round_slug,
    eri.name               AS round_name,
    eri.cup_round_type     AS round_cup_round_type,
    -- time: pulled from event_time which the bulk live-snapshot parser
    -- normalizes from the upstream payload's ``time`` object. Carries
    -- injuryTime1/2, currentPeriodStartTimestamp, initial/max/extra plus
    -- overtime/period length metadata.
    et.injury_time1                    AS time_injury_time_1,
    et.injury_time2                    AS time_injury_time_2,
    et.current_period_start_timestamp  AS time_current_period_start_timestamp,
    et.initial                         AS time_initial,
    et.max                             AS time_max,
    et.extra                           AS time_extra,
    et.overtime_length                 AS time_overtime_length,
    et.period_length                   AS time_period_length,
    et.total_period_count              AS time_total_period_count,
    et.injury_time3                    AS time_injury_time_3,
    et.injury_time4                    AS time_injury_time_4,
    -- changes: aggregate the LATEST changeTimestamp's items from
    -- event_change_item into the Sofascore-shape {changes:[...],
    -- changeTimestamp: N}. Pkey (event_id, ordinal) keeps the seek
    -- fast; the inner MAX() is an index-only scan over a small slice.
    eci_agg.payload        AS changes_payload
FROM event e
JOIN tournament t          ON t.id  = e.tournament_id
JOIN category c            ON c.id  = t.category_id
JOIN sport sp              ON sp.id = c.sport_id
LEFT JOIN unique_tournament ut ON ut.id = e.unique_tournament_id
JOIN season se             ON se.id = e.season_id
LEFT JOIN event_status st  ON st.code = e.status_code
LEFT JOIN team ht          ON ht.id = e.home_team_id
LEFT JOIN sport hsp        ON hsp.id = ht.sport_id
LEFT JOIN country hc       ON hc.alpha2 = ht.country_alpha2
LEFT JOIN team at_         ON at_.id = e.away_team_id
LEFT JOIN sport asp        ON asp.id = at_.sport_id
LEFT JOIN country ac       ON ac.alpha2 = at_.country_alpha2
LEFT JOIN event_score hs   ON hs.event_id = e.id AND hs.side = 'home'
LEFT JOIN event_score as_  ON as_.event_id = e.id AND as_.side = 'away'
LEFT JOIN event_round_info eri ON eri.event_id = e.id
LEFT JOIN event_time et ON et.event_id = e.id
LEFT JOIN LATERAL (
    SELECT jsonb_build_object(
        'changes', array_agg(change_value ORDER BY ordinal),
        'changeTimestamp', change_timestamp
    ) AS payload
    FROM event_change_item
    WHERE event_id = e.id
      AND change_timestamp = (
          SELECT MAX(change_timestamp) FROM event_change_item WHERE event_id = e.id
      )
    GROUP BY change_timestamp
    LIMIT 1
) eci_agg ON TRUE
"""

# Scheduled-events fetcher: filter by start_timestamp date range.
_FETCH_QUERY_SCHEDULED = (
    _FETCH_SELECT_AND_JOINS
    + """
WHERE sp.slug = $1
  AND e.start_timestamp >= $2
  AND e.start_timestamp <  $3
  AND e.is_editor IS NOT TRUE
ORDER BY e.start_timestamp, e.id
"""
)

# Single-event fetcher: pin by event.id. Returns one row or None.
_FETCH_QUERY_SINGLE = (
    _FETCH_SELECT_AND_JOINS
    + """
WHERE e.id = $1
"""
)

# Season events last (finished) — most recent results in a season.
# finished / afterextra / afterpen / cancelled / postponed.
_FETCH_QUERY_SEASON_LAST = (
    _FETCH_SELECT_AND_JOINS
    + """
WHERE e.unique_tournament_id = $1
  AND e.season_id = $2
  AND st.type IN ('finished', 'afterextra', 'afterpen', 'cancelled', 'postponed')
  AND e.start_timestamp <= EXTRACT(EPOCH FROM NOW())::bigint
  AND e.is_editor IS NOT TRUE
ORDER BY e.start_timestamp DESC NULLS LAST, e.id DESC
OFFSET $3
LIMIT $4
"""
)

# Season events next (notstarted) — upcoming fixtures in a season.
_FETCH_QUERY_SEASON_NEXT = (
    _FETCH_SELECT_AND_JOINS
    + """
WHERE e.unique_tournament_id = $1
  AND e.season_id = $2
  AND st.type = 'notstarted'
  AND e.start_timestamp >= EXTRACT(EPOCH FROM NOW())::bigint
  AND e.is_editor IS NOT TRUE
ORDER BY e.start_timestamp ASC NULLS LAST, e.id ASC
OFFSET $3
LIMIT $4
"""
)

# Live-events fetcher: filter by active-live status type, last-12h window,
# skip events with terminal_state (already finalized upstream) and skip
# SofaEditor crowdsourced events (X4 rule, 2026-05-13). Matches the
# semantics of Sofascore's /sport/{slug}/events/live exactly.
_LIVE_STATUS_TYPES = (
    "inprogress",
    "live",
    "overtime",
    "extra",
    "awaitingextra",
    "awaitingpenalties",
    "penalties",
    "interrupted",
    "halftime",
    "paused",
    "pause",
    "break",
)
_FETCH_QUERY_LIVE = (
    _FETCH_SELECT_AND_JOINS
    + """
WHERE sp.slug = $1
  AND st.type = ANY($2::text[])
  AND e.start_timestamp >= EXTRACT(EPOCH FROM NOW() - INTERVAL '12 hours')::bigint
  AND NOT EXISTS (
      SELECT 1 FROM event_terminal_state ets WHERE ets.event_id = e.id
  )
  AND e.is_editor IS NOT TRUE
ORDER BY e.start_timestamp DESC NULLS LAST, e.id DESC
LIMIT 500
"""
)

# Back-compat alias — older tests import _FETCH_QUERY.
_FETCH_QUERY = _FETCH_QUERY_SCHEDULED


# JSONB columns that asyncpg can return as raw strings depending on
# server-side codec setup. We decode them here so the synthesizer sees
# dicts and the emitted payload is valid JSON-of-JSON (not double-encoded
# strings inside the response).
_JSONB_COLUMNS: tuple[str, ...] = (
    "home_team_team_colors",
    "home_team_field_translations",
    "away_team_team_colors",
    "away_team_field_translations",
    "tournament_field_translations",
    "changes_payload",
)


def _decode_jsonb_fields(row: dict[str, Any]) -> dict[str, Any]:
    """Convert any JSONB columns that came back as raw strings into dicts.
    Idempotent — pre-decoded dicts pass through unchanged."""
    for key in _JSONB_COLUMNS:
        value = row.get(key)
        if isinstance(value, str) and value:
            try:
                import orjson
                row[key] = orjson.loads(value)
            except Exception:  # noqa: BLE001 - leave the string in place if decode fails
                logger.warning("scheduled-events synthesizer: failed to decode JSONB column %s", key)
    return row


async def fetch_rows(
    connection: Any,
    *,
    sport_slug: str,
    start_ts: int,
    end_ts: int,
) -> list[dict[str, Any]]:
    """Run the joined fetch and return a list of plain dicts ready for
    :func:`build_payload`. ``connection`` is any object with an async
    ``fetch`` method (asyncpg compatible)."""
    records = await connection.fetch(_FETCH_QUERY_SCHEDULED, sport_slug, start_ts, end_ts)
    return [_decode_jsonb_fields(dict(record)) for record in records]


async def fetch_single_event_row(
    connection: Any,
    *,
    event_id: int,
) -> dict[str, Any] | None:
    """Fetch one event by id with the same column projection as the
    list fetchers. Returns None if no event matches.
    """
    record = await connection.fetchrow(_FETCH_QUERY_SINGLE, event_id)
    if record is None:
        return None
    return _decode_jsonb_fields(dict(record))


async def fetch_season_events_rows(
    connection: Any,
    *,
    unique_tournament_id: int,
    season_id: int,
    direction: str,
    page: int,
    page_size: int = 30,
) -> list[dict[str, Any]]:
    """Powers /unique-tournament/{ut_id}/season/{sid}/events/last|next/{page}.

    ``direction='last'`` returns finished-or-cancelled events ordered by
    most recent first; ``direction='next'`` returns not-started events
    ordered by closest fixture first. Returns ``page_size + 1`` rows so
    the caller can detect ``hasNextPage`` by comparing list length.
    """
    if direction == "next":
        query = _FETCH_QUERY_SEASON_NEXT
    else:
        query = _FETCH_QUERY_SEASON_LAST
    offset = max(0, int(page)) * int(page_size)
    limit = int(page_size) + 1
    records = await connection.fetch(
        query, unique_tournament_id, season_id, offset, limit
    )
    return [_decode_jsonb_fields(dict(record)) for record in records]


async def fetch_live_rows(
    connection: Any,
    *,
    sport_slug: str,
    lookback_hours: int = 12,
) -> list[dict[str, Any]]:
    """Live-events counterpart of :func:`fetch_rows`. Returns rows for
    events whose status_type matches the active-live set, that started
    within the last ``lookback_hours``, that have no
    ``event_terminal_state`` row, and that are not editor-flagged.

    ``lookback_hours`` is accepted for API symmetry but the current
    implementation hard-codes 12 hours in the SQL. A future change can
    parametrise this without touching callers.
    """
    del lookback_hours  # currently fixed at 12 hours in SQL
    records = await connection.fetch(_FETCH_QUERY_LIVE, sport_slug, list(_LIVE_STATUS_TYPES))
    return [_decode_jsonb_fields(dict(record)) for record in records]


def build_payload(rows: Sequence[Any]) -> dict[str, Any]:
    """Assemble the ``{"events": [...]}`` envelope from joined event rows."""
    return {"events": [_build_event(row) for row in rows]}


def build_single_event_payload(row: Any) -> dict[str, Any]:
    """Single-event envelope for ``/api/v1/event/{event_id}`` —
    Sofascore wraps the event in ``{"event": ...}`` not ``{"events": [...]}``.
    """
    return {"event": _build_event(row)}


def _build_event(row: Any) -> dict[str, Any]:
    """Map one joined-row dict to the Sofascore event envelope.

    Uses ``.get()`` for every column so the function is safe to call on
    a row that came from a partial query (e.g. an older API entry that
    fetched a narrower column set, or a test fake). Missing keys
    surface as ``None`` in the payload, which matches Sofascore's own
    behaviour for not-applicable fields.
    """
    event: dict[str, Any] = {
        "id": row.get("event_id") or row.get("id"),
        "slug": row.get("event_slug") or row.get("slug"),
        "customId": row.get("custom_id"),
        "detailId": row.get("detail_id"),
        "startTimestamp": row.get("start_timestamp"),
        "winnerCode": row.get("winner_code"),
        "hasXg": row.get("has_xg"),
        "hasGlobalHighlights": row.get("has_global_highlights"),
        "hasEventPlayerStatistics": row.get("has_event_player_statistics"),
        "hasEventPlayerHeatMap": row.get("has_event_player_heat_map"),
        "feedLocked": row.get("feed_locked"),
        "isEditor": row.get("is_editor"),
        "crowdsourcingEnabled": row.get("crowdsourcing_enabled"),
        "finalResultOnly": row.get("final_result_only"),
        "status": _build_status(row),
        "homeTeam": _build_team(row, side="home"),
        "awayTeam": _build_team(row, side="away"),
        "tournament": _build_tournament(row),
        "season": _build_season(row),
        "homeScore": _build_score(row, side="home"),
        "awayScore": _build_score(row, side="away"),
    }
    round_info = _build_round_info(row)
    if round_info:
        event["roundInfo"] = round_info
    time_block = _build_time(row)
    if time_block:
        event["time"] = time_block
    if row.get("changes_payload"):
        event["changes"] = row["changes_payload"]
    return event


def _build_score(row: Any, *, side: str) -> dict[str, Any]:
    """Map ``home_score_*`` / ``away_score_*`` columns to a Sofascore score
    object. Defaults missing fields to 0 (Sofascore convention for not-
    yet-started events). Defensive: uses .get() so partial rows do not
    raise KeyError.
    """
    p = f"{side}_score_"
    current = row.get(f"{p}current")
    display = row.get(f"{p}display")
    score: dict[str, Any] = {
        "current": current if current is not None else 0,
        "display": display if display is not None else 0,
    }
    for key in ("normaltime", "overtime", "penalties", "aggregated", "period1", "period2"):
        value = row.get(f"{p}{key}")
        if value is not None:
            score[key] = value
    return score


def _build_round_info(row: Any) -> dict[str, Any] | None:
    """Return roundInfo dict or None if no round metadata present."""
    fields = [
        ("round", row.get("round_number")),
        ("slug", row.get("round_slug")),
        ("name", row.get("round_name")),
        ("cupRoundType", row.get("round_cup_round_type")),
    ]
    populated = {key: value for key, value in fields if value is not None}
    return populated or None


def _build_time(row: Any) -> dict[str, Any] | None:
    """Return time block matching the Sofascore ``time`` envelope.

    Maps event_time columns (normalized by the bulk live-snapshot parser
    from upstream's ``time`` object) onto the Sofascore camelCase shape.
    Returns None if every field is NULL so the synthesizer can omit the
    block entirely (Sofascore convention for not-started / no-time
    events).
    """
    fields = [
        ("injuryTime1", row.get("time_injury_time_1")),
        ("injuryTime2", row.get("time_injury_time_2")),
        ("injuryTime3", row.get("time_injury_time_3")),
        ("injuryTime4", row.get("time_injury_time_4")),
        ("currentPeriodStartTimestamp", row.get("time_current_period_start_timestamp")),
        ("initial", row.get("time_initial")),
        ("max", row.get("time_max")),
        ("extra", row.get("time_extra")),
        ("overtimeLength", row.get("time_overtime_length")),
        ("periodLength", row.get("time_period_length")),
        ("totalPeriodCount", row.get("time_total_period_count")),
    ]
    populated = {key: value for key, value in fields if value is not None}
    return populated or None


def _build_tournament(row: Any) -> dict[str, Any]:
    """Tournament + nested uniqueTournament + nested category.

    Defensive: legacy callers can supply only ``unique_tournament_id``
    (no joined unique_tournament/category data) — we emit empty nested
    objects in that case rather than crash.
    """
    # Support both new (ut_id, category_id) and legacy
    # (unique_tournament_id, no category) column shapes.
    ut_id = row.get("ut_id") or row.get("unique_tournament_id")
    ut_name = row.get("ut_name") or row.get("unique_tournament_name")
    ut_slug = row.get("ut_slug") or row.get("unique_tournament_slug")
    return {
        "id": row.get("tournament_id"),
        "name": row.get("tournament_name"),
        "slug": row.get("tournament_slug"),
        "priority": row.get("tournament_priority"),
        "competitionType": row.get("tournament_competition_type"),
        "isGroup": row.get("tournament_is_group"),
        "isLive": row.get("tournament_is_live"),
        "category": {
            "id": row.get("category_id"),
            "name": row.get("category_name"),
            "slug": row.get("category_slug"),
            "flag": row.get("category_flag"),
            "sport": {
                "id": row.get("category_sport_id"),
                "name": row.get("category_sport_name"),
                "slug": row.get("category_sport_slug"),
            },
        },
        "uniqueTournament": {
            "id": ut_id,
            "name": ut_name,
            "slug": ut_slug,
            "userCount": row.get("ut_user_count"),
            "hasEventPlayerStatistics": row.get("ut_has_event_player_statistics"),
            "hasPerformanceGraphFeature": row.get("ut_has_performance_graph_feature"),
            "tier": row.get("ut_tier"),
            "primaryColorHex": row.get("ut_primary_color_hex"),
            "secondaryColorHex": row.get("ut_secondary_color_hex"),
        },
    }


def _build_season(row: Any) -> dict[str, Any]:
    return {
        "id": row.get("season_id"),
        "name": row.get("season_name"),
        "year": row.get("season_year"),
        "editor": row.get("season_editor"),
    }


def _build_status(row: Any) -> dict[str, Any]:
    return {
        "code": row.get("status_code"),
        "type": row.get("status_type"),
        "description": row.get("status_description"),
    }


def _build_team(row: Any, *, side: str) -> dict[str, Any]:
    """Map the home_* / away_* prefixed columns into a Sofascore team.

    Defensive: any missing column is treated as None so partial rows
    (legacy callers, narrow test fakes) still produce a valid envelope.
    Sub-objects (country, sport) are omitted only when their primary
    discriminator is missing.
    """
    p = f"{side}_team_"
    country_alpha2 = row.get(f"{p}country_alpha2")
    if country_alpha2:
        country = {
            "name": row.get(f"{p}country_name"),
            "slug": row.get(f"{p}country_slug"),
            "alpha2": country_alpha2,
            "alpha3": row.get(f"{p}country_alpha3"),
        }
    else:
        country = {}
    sport_id = row.get(f"{p}sport_id")
    if sport_id is not None:
        sport = {
            "id": sport_id,
            "name": row.get(f"{p}sport_name"),
            "slug": row.get(f"{p}sport_slug"),
        }
    else:
        sport = {}
    team: dict[str, Any] = {
        "id": row.get(f"{p}id"),
        "name": row.get(f"{p}name"),
        "slug": row.get(f"{p}slug"),
        "shortName": row.get(f"{p}short_name"),
        "nameCode": row.get(f"{p}name_code"),
        "type": row.get(f"{p}type"),
        "gender": row.get(f"{p}gender"),
        "national": row.get(f"{p}national"),
        "disabled": row.get(f"{p}disabled"),
        "userCount": row.get(f"{p}user_count"),
        "subTeams": [],
        "teamColors": row.get(f"{p}team_colors") or {},
        "fieldTranslations": row.get(f"{p}field_translations") or {},
        "country": country,
        "sport": sport,
    }
    return team
