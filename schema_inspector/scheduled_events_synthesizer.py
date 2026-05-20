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
import re
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
    e.updated_at                                AS event_updated_at,
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

# Featured events — approximation of Sofascore's editorial top-match
# list. We do not have the "featured" flag normalized; instead we
# return events for the unique tournament in a +/- 7 day window
# around now, ordered by closest fixture first, capped at a small
# limit (12 — matches what Sofascore typically returns).
_FETCH_QUERY_FEATURED_EVENTS = (
    _FETCH_SELECT_AND_JOINS
    + """
WHERE e.unique_tournament_id = $1
  AND e.start_timestamp BETWEEN
      EXTRACT(EPOCH FROM NOW() - interval '7 days')::bigint
      AND
      EXTRACT(EPOCH FROM NOW() + interval '7 days')::bigint
  AND e.is_editor IS NOT TRUE
ORDER BY abs(e.start_timestamp - EXTRACT(EPOCH FROM NOW())::bigint), e.id
LIMIT 12
"""
)


# Player events last — events the player participated in (via lineup).
# Joins event_lineup_player → event, filters by player_id, returns most
# recent first. The composite index
# idx_event_lineup_player_player_id_event_id powers the seek.
_FETCH_QUERY_PLAYER_EVENTS_LAST = (
    _FETCH_SELECT_AND_JOINS
    + """
JOIN event_lineup_player elp ON elp.event_id = e.id
WHERE elp.player_id = $1
  AND st.type IN ('finished', 'afterextra', 'afterpen', 'cancelled', 'postponed')
  AND e.start_timestamp <= EXTRACT(EPOCH FROM NOW())::bigint
  AND e.is_editor IS NOT TRUE
ORDER BY e.start_timestamp DESC NULLS LAST, e.id DESC
OFFSET $2
LIMIT $3
"""
)


# Round events — events in a specific round of a unique tournament season.
# Round number lives on event_round_info (eri alias is in the shared JOIN
# block), so we filter on eri.round_number = $3.
_FETCH_QUERY_ROUND_EVENTS = (
    _FETCH_SELECT_AND_JOINS
    + """
WHERE e.unique_tournament_id = $1
  AND e.season_id = $2
  AND eri.round_number = $3
  AND e.is_editor IS NOT TRUE
ORDER BY e.start_timestamp, e.id
"""
)


# Phase 5.2 (2026-05-19): slug-aware round filter.
#
# Sofascore's ``/events/round/{N}/slug/{slug}`` URL serves the events
# for a named knockout round (e.g. ``round=29 slug=final``). Phase 4
# made sure ``event_round_info`` carries both ``round_number`` AND
# ``slug`` per event for cup-style competitions; this synthesizer
# filters on both so we serve the route from DB without re-fetching
# upstream. The slug disambiguates same-``round_number`` rows that
# can exist for different stages (e.g. UCL classic format had
# ``round_number=4`` for both group matchday 4 AND knockout Round
# of 16; only the latter has ``slug='round-of-16'``).
_FETCH_QUERY_ROUND_SLUG_EVENTS = (
    _FETCH_SELECT_AND_JOINS
    + """
WHERE e.unique_tournament_id = $1
  AND e.season_id = $2
  AND eri.round_number = $3
  AND eri.slug = $4
  AND e.is_editor IS NOT TRUE
ORDER BY e.start_timestamp, e.id
"""
)


# Unique-tournament scheduled events — same as sport-wide scheduled, but
# filtered to a single unique_tournament_id (Sofascore's
# /unique-tournament/{ut_id}/scheduled-events/{date} surface).
_FETCH_QUERY_UT_SCHEDULED = (
    _FETCH_SELECT_AND_JOINS
    + """
WHERE e.unique_tournament_id = $1
  AND e.start_timestamp >= $2
  AND e.start_timestamp <  $3
  AND e.is_editor IS NOT TRUE
ORDER BY e.start_timestamp, e.id
"""
)


# Team events last (finished) — most recent results for a team.
_FETCH_QUERY_TEAM_LAST = (
    _FETCH_SELECT_AND_JOINS
    + """
WHERE (e.home_team_id = $1 OR e.away_team_id = $1)
  AND st.type IN ('finished', 'afterextra', 'afterpen', 'cancelled', 'postponed')
  AND e.start_timestamp <= EXTRACT(EPOCH FROM NOW())::bigint
  AND e.is_editor IS NOT TRUE
ORDER BY e.start_timestamp DESC NULLS LAST, e.id DESC
OFFSET $2
LIMIT $3
"""
)

# Team events next (notstarted) — upcoming fixtures for a team.
_FETCH_QUERY_TEAM_NEXT = (
    _FETCH_SELECT_AND_JOINS
    + """
WHERE (e.home_team_id = $1 OR e.away_team_id = $1)
  AND st.type = 'notstarted'
  AND e.start_timestamp >= EXTRACT(EPOCH FROM NOW())::bigint
  AND e.is_editor IS NOT TRUE
ORDER BY e.start_timestamp ASC NULLS LAST, e.id ASC
OFFSET $2
LIMIT $3
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


# Phase 2.2 (Item 2, 2026-05-19) — synthesize ``/team/{id}/players``
# from recent lineup activity. Hybrid model with snapshot primary;
# this synthesizer fires only when the snapshot is missing.
#
# The squad list reflects players seen in this team's lineups within
# the last 60 days. Sofascore's editorial ``foreignPlayers`` /
# ``nationalPlayers`` partitions are intentionally NOT synthesized
# (their logic isn't simple country compare — Sofascore considers
# naturalisation, dual nationality, etc.). When the snapshot lands,
# all three partitions ride through 1:1.
_FETCH_QUERY_TEAM_SQUAD = (
    """
    SELECT DISTINCT
        p.id                AS player_id,
        p.name              AS player_name,
        p.slug              AS player_slug,
        elp.position        AS position,
        elp.jersey_number   AS jersey_number
    FROM event_lineup_player elp
    JOIN player p ON p.id = elp.player_id
    JOIN event e ON e.id = elp.event_id
    WHERE elp.team_id = $1
      AND e.start_timestamp >= $2
    ORDER BY p.name NULLS LAST
    """
)


# Phase 2.3 (Item 2, 2026-05-19) — synthesize ``/team/{id}/transfers``
# from ``player_transfer_history``. Hybrid model: raw passthrough is
# the primary serving path (1:1 wire format with nested editorial
# fields); this synthesizer fires only as fallback when no snapshot
# exists, producing a minimal Sofascore-shape envelope with the
# fields frontend lists actually use (player id/name/slug, transfer
# date, fee, from/to team names + ids).
#
# Single query covers both directions; the builder splits into
# transfersIn / transfersOut based on which side carries
# ``team_id`` parameter. Ordered DESC by transfer_date_timestamp to
# match Sofascore's most-recent-first wire order.
_FETCH_QUERY_TEAM_TRANSFERS = (
    """
    SELECT
        pth.id                                   AS id,
        pth.type                                 AS type,
        pth.player_id                            AS player_id,
        p.name                                   AS player_name,
        p.slug                                   AS player_slug,
        pth.transfer_from_team_id                AS transfer_from_team_id,
        pth.from_team_name                       AS from_team_name,
        pth.transfer_to_team_id                  AS transfer_to_team_id,
        pth.to_team_name                         AS to_team_name,
        pth.transfer_date_timestamp              AS transfer_date_timestamp,
        pth.transfer_fee                         AS transfer_fee,
        pth.transfer_fee_description             AS transfer_fee_description
    FROM player_transfer_history pth
    LEFT JOIN player p ON p.id = pth.player_id
    WHERE pth.transfer_to_team_id = $1
       OR pth.transfer_from_team_id = $1
    ORDER BY pth.transfer_date_timestamp DESC NULLS LAST, pth.id DESC
    """
)


# Hybrid synth for
# ``/unique-tournament/{ut}/season/{s}/cuptrees``.
#
# Bracket structure (rounds → blocks → participants → team) for cup
# competitions. Snapshot path serves the full upstream wire 1:1 with
# editorial nesting (tournament, teamColors, fieldTranslations, sport,
# country, etc.). This synth assembles the minimal wire shape from
# the 4 normalized tables when no snapshot exists.
#
# Single flat query produces one row per participant (or one row per
# block if block has no participants — via LEFT JOIN). Builder
# groups by cup_tree_id → round_order → entry_id (block) → participant.
# Ordered for stable adjacency so the builder can collect with a
# linear pass.
_FETCH_QUERY_CUPTREES = (
    """
    SELECT
        sct.cup_tree_id                AS cup_tree_id,
        sct.name                       AS cup_tree_name,
        sct.current_round              AS current_round,
        sctr.round_order               AS round_order,
        sctr.round_type                AS round_type,
        sctr.description               AS round_description,
        sctb.entry_id                  AS entry_id,
        sctb.block_id                  AS block_id,
        sctb.block_order               AS block_order,
        sctb.finished                  AS block_finished,
        sctb.matches_in_round          AS matches_in_round,
        sctb.result                    AS result,
        sctb.home_team_score           AS home_team_score,
        sctb.away_team_score           AS away_team_score,
        sctb.has_next_round_link       AS has_next_round_link,
        sctb.series_start_date_timestamp AS series_start_date_timestamp,
        sctb.automatic_progression     AS automatic_progression,
        sctb.event_ids_json            AS event_ids_json,
        sctp.participant_id            AS participant_id,
        sctp.order_value               AS participant_order,
        sctp.winner                    AS winner,
        t.id                           AS team_id,
        t.name                         AS team_name,
        t.slug                         AS team_slug,
        t.short_name                   AS team_short_name,
        t.name_code                    AS team_name_code,
        t.gender                       AS team_gender
    FROM season_cup_tree sct
    LEFT JOIN season_cup_tree_round sctr ON sctr.cup_tree_id = sct.cup_tree_id
    LEFT JOIN season_cup_tree_block sctb
        ON sctb.cup_tree_id = sct.cup_tree_id
       AND sctb.round_order = sctr.round_order
    LEFT JOIN season_cup_tree_participant sctp ON sctp.entry_id = sctb.entry_id
    LEFT JOIN team t ON t.id = sctp.team_id
    WHERE sct.unique_tournament_id = $1
      AND sct.season_id = $2
    ORDER BY
        sct.cup_tree_id,
        sctr.round_order NULLS LAST,
        sctb.block_order NULLS LAST,
        sctb.entry_id NULLS LAST,
        sctp.order_value NULLS LAST
    """
)


# Hybrid synth for
# ``/unique-tournament/{ut}/season/{s}/team-of-the-week/{period_id}``.
#
# Sofascore's wire shape is flat: ``{"formation": "...", "players":
# [{"player": {...}, "team": {...}, "rating": "..."}]}``. The generic
# normalized dispatcher previously read ``team_of_the_week`` alone and
# emitted ``{"teamOfTheWeeks": [{"formation", "periodId"}]}`` — two
# bugs: wrong outer wrapper + missing ``players`` array because the
# squad lives in a separate ``team_of_the_week_player`` table.
#
# This synth produces the correct flat envelope by JOIN-ing the parent
# row with the lineup table, plus ``player`` and ``team`` for nested
# id/name/slug fields. Snapshot path still serves rich editorial
# (event, country, teamColors, fieldTranslations) 1:1 when fresh.
#
# Empty result → ``{"players": []}`` (no formation key) — matches the
# DB-down fallback in the dispatcher for a consistent frontend shape.
_FETCH_QUERY_TEAM_OF_THE_WEEK = (
    """
    SELECT
        totw.formation             AS formation,
        totwp.entry_id             AS entry_id,
        totwp.order_value          AS order_value,
        totwp.rating               AS rating,
        totwp.player_id            AS player_id,
        p.name                     AS player_name,
        p.slug                     AS player_slug,
        totwp.team_id              AS team_id,
        t.name                     AS team_name,
        t.slug                     AS team_slug,
        t.short_name               AS team_short_name,
        t.name_code                AS team_name_code,
        t.gender                   AS team_gender
    FROM team_of_the_week totw
    LEFT JOIN team_of_the_week_player totwp ON totwp.period_id = totw.period_id
    LEFT JOIN player p ON p.id = totwp.player_id
    LEFT JOIN team t   ON t.id = totwp.team_id
    WHERE totw.period_id = $1
    ORDER BY totwp.order_value NULLS LAST, totwp.entry_id
    """
)


# Phase 5.1 — synthesize ``/season/{s}/groups`` from sub-tournaments.
#
# Sofascore groups under cup-style unique tournaments (FIFA WC,
# classic UCL until 2023/24) live as individual sub-tournaments
# under the umbrella ``unique_tournament_id``. Each event in our DB
# references its sub-tournament via ``event.tournament_id``, and the
# sub-tournament row in ``tournament`` carries a name like
# ``"FIFA World Cup, Group A"``.
#
# The DISTINCT scan over ``event`` + ``tournament`` for the given
# (UT, season) yields one row per group-stage sub-tournament. The
# payload builder filters to ``tournament_name`` matching the
# ``Group X`` pattern (Sofascore's wire format strips everything
# before the comma) and emits the Sofascore envelope.
_FETCH_QUERY_GROUPS = (
    """
    SELECT DISTINCT
        e.tournament_id      AS tournament_id,
        t.name               AS tournament_name
    FROM event e
    JOIN tournament t ON t.id = e.tournament_id
    WHERE e.unique_tournament_id = $1
      AND e.season_id = $2
      AND t.name IS NOT NULL
    ORDER BY t.name
    """
)


# Phase 2.4 — synthesize ``/event/{custom_id}/h2h/events``. Step 1 of
# the CTE resolves the anchor event's (home_team_id, away_team_id) pair
# by custom_id; step 2 filters the same ``event`` table for every match
# played between those two teams (either direction), ordered most
# recent first. Source data is already in our DB; no upstream call.
_FETCH_QUERY_H2H_EVENTS = (
    """
    WITH anchor AS (
        SELECT home_team_id, away_team_id
        FROM event
        WHERE custom_id = $1
          AND home_team_id IS NOT NULL
          AND away_team_id IS NOT NULL
        LIMIT 1
    )
    """
    + _FETCH_SELECT_AND_JOINS
    + """
    JOIN anchor a ON
        (e.home_team_id = a.home_team_id AND e.away_team_id = a.away_team_id)
        OR (e.home_team_id = a.away_team_id AND e.away_team_id = a.home_team_id)
    ORDER BY e.start_timestamp DESC NULLS LAST, e.id DESC
    LIMIT $2
    """
)


# Phase 2.1 — synthesize ``/calendar/.../months-with-events`` from
# ``event.start_timestamp``. The Sofascore endpoint returns months that
# have at least one event for a given (unique_tournament, season). One
# DISTINCT scan over the ``event`` table reproduces it.
#
# Output format: ``YYYY-MM`` strings sorted chronologically ascending.
_FETCH_QUERY_CALENDAR_MONTHS = (
    """
    SELECT DISTINCT
        TO_CHAR(
            TO_TIMESTAMP(e.start_timestamp) AT TIME ZONE 'UTC',
            'YYYY-MM'
        ) AS month
    FROM event e
    WHERE e.unique_tournament_id = $1
      AND e.season_id = $2
      AND e.start_timestamp IS NOT NULL
    ORDER BY month
    """
)


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


async def fetch_featured_events_rows(
    connection: Any,
    *,
    unique_tournament_id: int,
) -> list[dict[str, Any]]:
    """Powers /unique-tournament/{ut_id}/featured-events.

    Approximation of the editorial "featured" list: events of the UT
    that fall in a ±7 day window around now, ordered by closest
    fixture, capped at 12 events (Sofascore's typical featured size).
    """
    records = await connection.fetch(_FETCH_QUERY_FEATURED_EVENTS, unique_tournament_id)
    return [_decode_jsonb_fields(dict(record)) for record in records]


async def fetch_player_events_rows(
    connection: Any,
    *,
    player_id: int,
    page: int,
    page_size: int = 30,
) -> list[dict[str, Any]]:
    """Powers /player/{player_id}/events/last/{page}.

    Joins event_lineup_player → event to find events the player took
    part in. Filter: finished/cancelled/postponed status, ordered most
    recent first. Returns page_size+1 rows (caller detects hasNextPage).
    """
    offset = max(0, int(page)) * int(page_size)
    limit = int(page_size) + 1
    records = await connection.fetch(
        _FETCH_QUERY_PLAYER_EVENTS_LAST, player_id, offset, limit
    )
    return [_decode_jsonb_fields(dict(record)) for record in records]


async def fetch_round_events_rows(
    connection: Any,
    *,
    unique_tournament_id: int,
    season_id: int,
    round_number: int,
) -> list[dict[str, Any]]:
    """Powers /unique-tournament/{ut_id}/season/{sid}/events/round/{round_number}.
    Filter: UT + season + event_round_info.round_number.
    """
    records = await connection.fetch(
        _FETCH_QUERY_ROUND_EVENTS, unique_tournament_id, season_id, round_number
    )
    return [_decode_jsonb_fields(dict(record)) for record in records]


async def fetch_round_slug_events_rows(
    connection: Any,
    *,
    unique_tournament_id: int,
    season_id: int,
    round_number: int,
    slug: str,
) -> list[dict[str, Any]]:
    """Phase 5.2 (2026-05-19): powers
    /unique-tournament/{ut}/season/{s}/events/round/{N}/slug/{slug}.

    Filter: UT + season + event_round_info.round_number + slug. The
    slug filter disambiguates same-``round_number`` rows that can
    legitimately coexist for different stages of a cup competition
    (classic UCL had ``round=4 slug=NULL`` for group matchday 4 AND
    ``round=4 slug=round-of-16`` for the knockout stage).

    Phase 4 ingestion populated the ``event_round_info`` rows we
    filter on; this synthesizer serves the slug-aware URL directly
    from DB. Once Phase 5.3 lands the skip-when-populated check,
    cursor walks won't re-fetch this endpoint on subsequent passes.
    """
    records = await connection.fetch(
        _FETCH_QUERY_ROUND_SLUG_EVENTS,
        unique_tournament_id,
        season_id,
        round_number,
        slug,
    )
    return [_decode_jsonb_fields(dict(record)) for record in records]


async def fetch_ut_scheduled_events_rows(
    connection: Any,
    *,
    unique_tournament_id: int,
    start_ts: int,
    end_ts: int,
) -> list[dict[str, Any]]:
    """Powers /unique-tournament/{ut_id}/scheduled-events/{date}.
    Same date-range filter as the sport-wide scheduled fetcher, but
    pinned to a single unique tournament.
    """
    records = await connection.fetch(
        _FETCH_QUERY_UT_SCHEDULED, unique_tournament_id, start_ts, end_ts
    )
    return [_decode_jsonb_fields(dict(record)) for record in records]


async def fetch_team_events_rows(
    connection: Any,
    *,
    team_id: int,
    direction: str,
    page: int,
    page_size: int = 30,
) -> list[dict[str, Any]]:
    """Powers /team/{team_id}/events/last|next/{page}.

    Filter on home_team_id = $1 OR away_team_id = $1. Direction switches
    between finished (last, DESC) and notstarted (next, ASC). Returns
    page_size+1 rows so the caller can detect hasNextPage cheaply.
    """
    query = _FETCH_QUERY_TEAM_NEXT if direction == "next" else _FETCH_QUERY_TEAM_LAST
    offset = max(0, int(page)) * int(page_size)
    limit = int(page_size) + 1
    records = await connection.fetch(query, team_id, offset, limit)
    return [_decode_jsonb_fields(dict(record)) for record in records]


def synthesize_away_standing_rows(
    total_rows: Sequence[Any],
    home_rows: Sequence[Any],
) -> list[dict[str, Any]]:
    """Phase 2.5 — compute the ``away`` standings rows arithmetically.

    For every team present in ``total_rows``, ``away = total - home``
    for the seven additive metrics: matches, wins, draws, losses,
    scoresFor, scoresAgainst, points. Teams in ``home_rows`` but not
    ``total_rows`` are skipped (no baseline to subtract from — likely
    a data anomaly). Teams in ``total_rows`` but not ``home_rows`` are
    treated as having played 0 home matches (away simply equals total).

    Position is re-ranked over the synthesized rows by
    ``(points DESC, goal_diff DESC, scoresFor DESC)`` — Sofascore's
    standard tie-breaker. The position field is the only one where
    Sofascore could disagree slightly (custom league-specific
    tie-breaking rules), so this synthesizer is best-effort for
    position but exact for every other column.

    ``score_diff_formatted`` is regenerated from the synthesized
    (scoresFor - scoresAgainst) with the ``+N`` / ``-N`` / ``0``
    convention Sofascore uses.

    Input rows are the same dict shape returned by
    ``_fetch_standings_payload``'s inner SELECT — team_id, team_name,
    team_slug, team_short_name, matches, wins, draws, losses,
    scores_for, scores_against, points, position. The returned rows
    have the same shape with values recomputed.
    """
    home_by_team: dict[int, dict[str, Any]] = {
        int(row["team_id"]): row for row in home_rows
    }
    synthesized: list[dict[str, Any]] = []
    for total_row in total_rows:
        team_id = int(total_row["team_id"])
        home_row = home_by_team.get(team_id)

        def _sub(key: str) -> int:
            t = int(total_row.get(key) or 0)
            h = int(home_row.get(key) or 0) if home_row is not None else 0
            return t - h

        matches = _sub("matches")
        wins = _sub("wins")
        draws = _sub("draws")
        losses = _sub("losses")
        scores_for = _sub("scores_for")
        scores_against = _sub("scores_against")
        points = _sub("points")

        diff = scores_for - scores_against
        if diff > 0:
            score_diff_formatted = f"+{diff}"
        elif diff < 0:
            score_diff_formatted = str(diff)
        else:
            score_diff_formatted = "0"

        synthesized.append(
            {
                "team_id": team_id,
                "team_name": total_row.get("team_name"),
                "team_slug": total_row.get("team_slug"),
                "team_short_name": total_row.get("team_short_name"),
                "matches": matches,
                "wins": wins,
                "draws": draws,
                "losses": losses,
                "scores_for": scores_for,
                "scores_against": scores_against,
                "points": points,
                "score_diff_formatted": score_diff_formatted,
                "position": 0,  # placeholder; re-ranked below
            }
        )

    # Re-rank by (points DESC, goal_diff DESC, scoresFor DESC). Stable
    # sort keeps insertion order for ties beyond the tie-breaker.
    synthesized.sort(
        key=lambda r: (
            -int(r["points"]),
            -(int(r["scores_for"]) - int(r["scores_against"])),
            -int(r["scores_for"]),
        )
    )
    for idx, row in enumerate(synthesized, start=1):
        row["position"] = idx

    return synthesized


async def fetch_groups_rows(
    connection: Any,
    *,
    unique_tournament_id: int,
    season_id: int,
) -> list[dict[str, Any]]:
    """Phase 5.1 — DISTINCT sub-tournaments under a (UT, season) pair.

    Source of truth: ``event`` table (knows about both
    ``unique_tournament_id`` and ``tournament_id``) JOIN ``tournament``
    (for the human-readable name we slice the group letter out of).
    Returned rows have ``tournament_id`` and ``tournament_name`` keys.

    Note: this returns ALL sub-tournaments — knockout, qualification,
    playoff round, etc. The group filter (``Group X``) is applied in
    ``build_groups_payload`` so non-group rows are dropped at envelope
    assembly. Keeping that filter in Python (rather than the SQL
    ``WHERE`` clause) makes the helper reusable if/when we add other
    sub-tournament endpoints later (e.g. ``/knockout``).
    """
    records = await connection.fetch(
        _FETCH_QUERY_GROUPS, unique_tournament_id, season_id
    )
    return [dict(record) for record in records]


async def fetch_team_squad_rows(
    connection: Any,
    *,
    team_id: int,
) -> list[dict[str, Any]]:
    """Phase 2.2 (Item 2, 2026-05-19): squad rows for a team —
    DISTINCT players from ``event_lineup_player`` filtered to
    matches whose ``start_timestamp`` is within the last 60 days.

    Returned rows are plain dicts ready for
    :func:`build_team_players_payload`. The 60-day window ensures
    the synthesized squad list reflects current activity; players
    who left the club months ago drop off naturally.
    """
    import time
    cutoff = int(time.time()) - 60 * 86_400
    records = await connection.fetch(_FETCH_QUERY_TEAM_SQUAD, team_id, cutoff)
    return [dict(record) for record in records]


async def fetch_team_transfers_rows(
    connection: Any,
    *,
    team_id: int,
) -> list[dict[str, Any]]:
    """Phase 2.3 (Item 2, 2026-05-19): pulls transfer history rows
    for a team in either direction (in / out).

    Returned rows are plain dicts ready for
    :func:`build_team_transfers_payload`. The builder applies the
    direction split based on which of ``transfer_to_team_id`` /
    ``transfer_from_team_id`` matches the caller's ``team_id``.
    """
    records = await connection.fetch(_FETCH_QUERY_TEAM_TRANSFERS, team_id)
    return [dict(record) for record in records]


async def fetch_cuptrees_rows(
    connection: Any,
    *,
    unique_tournament_id: int,
    season_id: int,
) -> list[dict[str, Any]]:
    """Pulls flat rows for ``/cuptrees`` synth.

    Each row is one (cup_tree × round × block × participant) tuple,
    LEFT JOIN-ed so blocks without participants still emit a row
    (TBD pairings / bye blocks). Returns 0 rows if the (UT, season)
    pair has no stored cup tree.
    """
    records = await connection.fetch(
        _FETCH_QUERY_CUPTREES, unique_tournament_id, season_id
    )
    return [_decode_jsonb_fields(dict(record)) for record in records]


async def fetch_team_of_the_week_rows(
    connection: Any,
    *,
    period_id: int,
) -> list[dict[str, Any]]:
    """Pulls the ``team_of_the_week`` row joined with the lineup table
    (``team_of_the_week_player``) and the player/team metadata needed
    by :func:`build_team_of_the_week_payload`.

    Returns 0 rows if ``period_id`` doesn't match any stored ToTW;
    returns 1 row (with NULL lineup columns) if the parent exists but
    no players are stored — the builder handles both shapes.
    """
    records = await connection.fetch(_FETCH_QUERY_TEAM_OF_THE_WEEK, period_id)
    return [dict(record) for record in records]


async def fetch_h2h_events_rows(
    connection: Any,
    *,
    custom_id: str,
    limit: int = 20,
) -> list[dict[str, Any]]:
    """Powers ``/api/v1/event/{custom_id}/h2h/events``.

    Step 1: resolve ``custom_id`` to the anchor event's team pair.
    Step 2: return every event we have between the same two teams in
    either direction, ordered most recent first, capped at ``limit``.

    Returns an empty list if the custom_id doesn't match any event we
    have (no anchor → CTE empty → outer JOIN empty).
    """
    records = await connection.fetch(
        _FETCH_QUERY_H2H_EVENTS, str(custom_id), int(limit)
    )
    return [_decode_jsonb_fields(dict(record)) for record in records]


async def fetch_calendar_months_rows(
    connection: Any,
    *,
    unique_tournament_id: int,
    season_id: int,
) -> list[dict[str, Any]]:
    """Powers ``/api/v1/calendar/unique-tournament/{ut}/season/{s}/months-with-events``.

    DISTINCT YYYY-MM month strings drawn from ``event.start_timestamp``
    for the given (unique_tournament, season). Sorted chronologically
    ascending (oldest first). Rows with NULL ``start_timestamp`` are
    excluded — they have no month to bucket into.

    Result rows are plain dicts with a single ``"month"`` key. The
    response builder (:func:`build_calendar_months_payload`) flattens
    them into the ``{"months": [...]}`` Sofascore envelope.
    """
    records = await connection.fetch(
        _FETCH_QUERY_CALENDAR_MONTHS, unique_tournament_id, season_id
    )
    return [dict(record) for record in records]


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


def build_team_players_payload(rows: Sequence[Any]) -> dict[str, Any]:
    """Phase 2.2 (Item 2, 2026-05-19): minimal Sofascore-shape
    ``{"players": [{"player": {...}}, ...]}`` envelope.

    Each entry wraps the player in a ``player`` key (matching the
    upstream wire format). Minimal fields: id, name, slug, position,
    jerseyNumber. Editorial partitions ``foreignPlayers`` /
    ``nationalPlayers`` deliberately omitted — see hybrid model
    docstring in ``_FETCH_QUERY_TEAM_SQUAD`` for rationale.

    Dedupes on ``player_id`` defensively (the SQL already DISTINCTs)
    so hand-constructed inputs don't emit doubles. Rows with NULL
    ``player_id`` are dropped (data anomaly).
    """
    seen: set[int] = set()
    players: list[dict[str, Any]] = []
    for row in rows:

        def _get(name: str) -> Any:
            if isinstance(row, dict):
                return row.get(name)
            return getattr(row, name, None)

        player_id = _get("player_id")
        if player_id is None:
            continue
        try:
            pid = int(player_id)
        except (TypeError, ValueError):
            continue
        if pid in seen:
            continue
        seen.add(pid)
        players.append(
            {
                "player": {
                    "id": pid,
                    "name": _get("player_name"),
                    "slug": _get("player_slug"),
                    "position": _get("position"),
                    "jerseyNumber": _get("jersey_number"),
                }
            }
        )
    return {"players": players}


def build_team_transfers_payload(
    rows: Sequence[Any], *, team_id: int
) -> dict[str, Any]:
    """Phase 2.3 (Item 2, 2026-05-19): minimal Sofascore-shape
    ``{"transfersIn": [...], "transfersOut": [...]}`` envelope from
    transfer-history rows.

    Direction split: a row whose ``transfer_to_team_id`` matches
    ``team_id`` is an incoming transfer; ``transfer_from_team_id``
    match is outgoing. Rows where neither column matches (data
    integrity anomaly — shouldn't happen given the SQL ``WHERE``
    filter, but defensive) are dropped.

    Player object is minimal — id/name/slug only. Rich editorial
    fields (fieldTranslations, sport, country, dateOfBirth,
    proposedMarketValue, …) live in the raw snapshot path only. The
    explicit trade-off, captured in the hybrid model docstring, is
    that synthesized responses cover the **list-rendering use case**
    not full player-profile fidelity.
    """
    transfers_in: list[dict[str, Any]] = []
    transfers_out: list[dict[str, Any]] = []
    for row in rows:

        def _get(name: str) -> Any:
            if isinstance(row, dict):
                return row.get(name)
            return getattr(row, name, None)

        to_team_id = _get("transfer_to_team_id")
        from_team_id = _get("transfer_from_team_id")
        if to_team_id != team_id and from_team_id != team_id:
            continue
        entry: dict[str, Any] = {
            "id": int(_get("id")) if _get("id") is not None else None,
            "type": int(_get("type")) if _get("type") is not None else None,
            "player": {
                "id": int(_get("player_id")) if _get("player_id") is not None else None,
                "name": _get("player_name"),
                "slug": _get("player_slug"),
            },
            "fromTeamName": _get("from_team_name"),
            "toTeamName": _get("to_team_name"),
            "transferDateTimestamp": _get("transfer_date_timestamp"),
            "transferFee": _get("transfer_fee"),
            "transferFeeDescription": _get("transfer_fee_description"),
        }
        if to_team_id == team_id:
            transfers_in.append(entry)
        else:
            transfers_out.append(entry)
    return {"transfersIn": transfers_in, "transfersOut": transfers_out}


def build_cuptrees_payload(rows: Sequence[Any]) -> dict[str, Any]:
    """Nest flat rows into Sofascore-shape ``{"cupTrees": [{...}]}``.

    Single linear pass: rows are already sorted by (cup_tree_id,
    round_order, block_order, entry_id, participant_order) so we can
    detect transitions and emit nested children.

    Rows with NULL ``cup_tree_id`` are dropped (defensive — shouldn't
    happen given the SQL WHERE filter). Blocks without participants
    (NULL ``participant_id``) still emit the block with an empty
    ``participants`` array. Same for rounds without blocks (NULL
    ``entry_id``) — empty ``blocks`` array.
    """
    if not rows:
        return {"cupTrees": []}

    def _get(row: Any, name: str) -> Any:
        if isinstance(row, dict):
            return row.get(name)
        return getattr(row, name, None)

    trees: list[dict[str, Any]] = []
    tree_index: dict[Any, dict[str, Any]] = {}
    round_index: dict[tuple[Any, Any], dict[str, Any]] = {}
    block_index: dict[Any, dict[str, Any]] = {}

    for row in rows:
        cup_tree_id = _get(row, "cup_tree_id")
        if cup_tree_id is None:
            continue
        try:
            ct_id = int(cup_tree_id)
        except (TypeError, ValueError):
            continue

        tree = tree_index.get(ct_id)
        if tree is None:
            tree = {
                "id": ct_id,
                "name": _get(row, "cup_tree_name"),
                "currentRound": _get(row, "current_round"),
                "rounds": [],
            }
            tree_index[ct_id] = tree
            trees.append(tree)

        round_order = _get(row, "round_order")
        if round_order is None:
            continue  # no rounds yet for this tree
        try:
            ro = int(round_order)
        except (TypeError, ValueError):
            continue

        round_key = (ct_id, ro)
        round_obj = round_index.get(round_key)
        if round_obj is None:
            round_obj = {
                "order": ro,
                "type": _get(row, "round_type"),
                "description": _get(row, "round_description"),
                "blocks": [],
            }
            round_index[round_key] = round_obj
            tree["rounds"].append(round_obj)

        entry_id = _get(row, "entry_id")
        if entry_id is None:
            continue  # round without any blocks
        try:
            eid = int(entry_id)
        except (TypeError, ValueError):
            continue

        block = block_index.get(eid)
        if block is None:
            event_ids_raw = _get(row, "event_ids_json") or []
            events_list: list[dict[str, Any]] = []
            if isinstance(event_ids_raw, list):
                for raw_id in event_ids_raw:
                    try:
                        events_list.append({"id": int(raw_id)})
                    except (TypeError, ValueError):
                        continue
            block_order_raw = _get(row, "block_order")
            try:
                block_order_int: Any = (
                    int(block_order_raw) if block_order_raw is not None else None
                )
            except (TypeError, ValueError):
                block_order_int = None
            block_id_raw = _get(row, "block_id")
            try:
                block_id_int: Any = (
                    int(block_id_raw) if block_id_raw is not None else None
                )
            except (TypeError, ValueError):
                block_id_int = None
            block = {
                "id": eid,
                "blockId": block_id_int,
                "order": block_order_int,
                "finished": _get(row, "block_finished"),
                "matchesInRound": _get(row, "matches_in_round"),
                "result": _get(row, "result"),
                "homeTeamScore": _get(row, "home_team_score"),
                "awayTeamScore": _get(row, "away_team_score"),
                "hasNextRoundLink": _get(row, "has_next_round_link"),
                "seriesStartDateTimestamp": _get(row, "series_start_date_timestamp"),
                "automaticProgression": _get(row, "automatic_progression"),
                "events": events_list,
                "participants": [],
            }
            block_index[eid] = block
            round_obj["blocks"].append(block)

        participant_id = _get(row, "participant_id")
        if participant_id is None:
            continue  # block without participants (TBD/bye)
        try:
            pid = int(participant_id)
        except (TypeError, ValueError):
            continue

        team_id_raw = _get(row, "team_id")
        try:
            team_id_int = int(team_id_raw) if team_id_raw is not None else None
        except (TypeError, ValueError):
            team_id_int = None
        participant_order_raw = _get(row, "participant_order")
        try:
            participant_order_int: Any = (
                int(participant_order_raw)
                if participant_order_raw is not None
                else None
            )
        except (TypeError, ValueError):
            participant_order_int = None
        block["participants"].append(
            {
                "id": pid,
                "order": participant_order_int,
                "winner": _get(row, "winner"),
                "team": {
                    "id": team_id_int,
                    "name": _get(row, "team_name"),
                    "slug": _get(row, "team_slug"),
                    "shortName": _get(row, "team_short_name"),
                    "nameCode": _get(row, "team_name_code"),
                    "gender": _get(row, "team_gender"),
                },
            }
        )

    return {"cupTrees": trees}


def build_team_of_the_week_payload(rows: Sequence[Any]) -> dict[str, Any]:
    """Flat Sofascore-shape ``{"formation": "...", "players": [...]}``.

    Each entry is ``{"player": {id, name, slug}, "team": {id, name,
    slug, shortName, nameCode, gender}, "rating": "..."}``. Rich
    editorial fields (``team.fieldTranslations``, ``team.teamColors``,
    ``team.sport``, ``player.country``, ``event``, etc.) are omitted —
    they live in the raw snapshot path only.

    Empty rows or rows lacking ``player_id`` (data anomaly) are
    skipped. Result is sorted by ``order_value`` defensively (the SQL
    ORDER BY already sorts, but a hand-constructed input should still
    yield deterministic output).
    """
    if not rows:
        return {"players": []}

    def _get(row: Any, name: str) -> Any:
        if isinstance(row, dict):
            return row.get(name)
        return getattr(row, name, None)

    formation: Any = None
    entries: list[dict[str, Any]] = []
    for row in rows:
        if formation is None:
            formation = _get(row, "formation")
        player_id = _get(row, "player_id")
        if player_id is None:
            continue
        try:
            pid = int(player_id)
        except (TypeError, ValueError):
            continue
        team_id_raw = _get(row, "team_id")
        try:
            team_id_int = int(team_id_raw) if team_id_raw is not None else None
        except (TypeError, ValueError):
            team_id_int = None
        order_value_raw = _get(row, "order_value")
        try:
            order_value_int = int(order_value_raw) if order_value_raw is not None else 0
        except (TypeError, ValueError):
            order_value_int = 0
        entries.append(
            {
                "_order": order_value_int,
                "player": {
                    "id": pid,
                    "name": _get(row, "player_name"),
                    "slug": _get(row, "player_slug"),
                },
                "team": {
                    "id": team_id_int,
                    "name": _get(row, "team_name"),
                    "slug": _get(row, "team_slug"),
                    "shortName": _get(row, "team_short_name"),
                    "nameCode": _get(row, "team_name_code"),
                    "gender": _get(row, "team_gender"),
                },
                "rating": _get(row, "rating"),
            }
        )

    entries.sort(key=lambda e: e["_order"])
    for entry in entries:
        del entry["_order"]

    payload: dict[str, Any] = {"players": entries}
    if formation is not None:
        payload["formation"] = formation
        # Place formation first to match upstream key order (cosmetic).
        payload = {"formation": formation, "players": entries}
    return payload


def build_groups_payload(rows: Sequence[Any]) -> dict[str, Any]:
    """Wrap ``fetch_groups_rows`` output in Sofascore's ``/groups``
    envelope.

    Sofascore tournament names look like ``"<Cup>, Group X"`` —
    everything after the rightmost ``", "`` is the group name (Sofascore
    UI strips the cup prefix). Rows whose name lacks that pattern or
    whose suffix doesn't start with ``Group `` are dropped — they
    represent non-group sub-tournaments (Knockout, Qualification,
    Playoff Round, single-phase Swiss UCL, etc.).

    Output shape:
        {"groups": [
            {"groupName": "Group A", "tournamentId": 3954},
            ...
        ]}
    """
    groups: list[dict[str, Any]] = []
    for row in rows:
        tournament_id = (
            row.get("tournament_id") if isinstance(row, dict) else getattr(row, "tournament_id", None)
        )
        tournament_name = (
            row.get("tournament_name") if isinstance(row, dict) else getattr(row, "tournament_name", None)
        )
        if tournament_id is None or not isinstance(tournament_name, str):
            continue
        # The group suffix comes after the rightmost ``", "``. If the
        # name has no comma, the entire string IS the suffix — that's a
        # single-phase Swiss tournament and not a group.
        suffix = tournament_name.rsplit(", ", 1)[-1].strip()
        if not suffix.startswith("Group "):
            continue
        groups.append({"groupName": suffix, "tournamentId": int(tournament_id)})
    return {"groups": groups}


def build_calendar_months_payload(rows: Sequence[Any]) -> dict[str, Any]:
    """Wrap the months fetcher rows in the Sofascore envelope.

    Drops rows with NULL/empty month values (defensive — the fetcher
    already filters via ``start_timestamp IS NOT NULL``, but a test
    pins the dedupe+null-strip behavior in case the input is hand-
    constructed). Order is preserved from the input list (the fetcher
    already returns them chronologically ASC).
    """
    seen: set[str] = set()
    months: list[str] = []
    for row in rows:
        value = row.get("month") if isinstance(row, dict) else getattr(row, "month", None)
        if not value:
            continue
        text = str(value).strip()
        if not text or text in seen:
            continue
        seen.add(text)
        months.append(text)
    return {"months": months}


def build_payload(rows: Sequence[Any]) -> dict[str, Any]:
    """Assemble the ``{"events": [...]}`` envelope from joined event rows."""
    return {"events": [_build_event(row) for row in rows]}


def build_single_event_payload(row: Any) -> dict[str, Any]:
    """Single-event envelope for ``/api/v1/event/{event_id}`` —
    Sofascore wraps the event in ``{"event": ...}`` not ``{"events": [...]}``.
    """
    return {"event": _build_event(row)}


_EVENT_ID_PATH_RE = re.compile(r"^/api/v1/event/(\d+)(?:/|$)")


def extract_event_id_from_path(path: str) -> int | None:
    """Return the numeric event_id from any /api/v1/event/{id}[/...] path.

    Returns None for non-event paths and for ``custom_id`` paths
    (e.g. /api/v1/event/Pdbsceb/h2h/events) where the event identifier
    is alphanumeric rather than numeric — those go through a different
    normalisation flow.
    """
    if not path:
        return None
    match = _EVENT_ID_PATH_RE.match(path)
    if match is None:
        return None
    try:
        return int(match.group(1))
    except (TypeError, ValueError):
        return None


# Narrow opt-in allowlist of endpoint patterns whose snapshot lookup
# should bail out when the snapshot is stale relative to event.updated_at.
#
# A skip is only beneficial when there is a downstream code path that
# can produce a *better* response than the stale snapshot. In practice
# that means: a path that rebuilds a full payload with fresh
# status/score/time on top of the snapshot's static fields (player meta,
# colors, tournament context, etc.).
#
# Today that path exists only for the /event/{event_id} root, via
# _fetch_event_root_payload + overlay_live_fields. Every other event
# sub-resource (incidents / lineups / statistics / best-players /
# graph / shotmap / heatmap / passing-network / ...) falls through
# either to a thin specialised handler that cannot reproduce the
# upstream payload's player/assist/passing-network/team-colors detail
# or to a plain 404. For those, a 30-60s stale snapshot with 100% of
# the upstream shape is strictly better than a fresh 20% subset.
#
# Past mistake (Stage B central + first refinement): default was
# sensitive=True with a small static allowlist of insensitive patterns.
# That dropped 1-2 KB of unique fields per /event/{id}/incidents call
# every time the bulk live-snapshot parser bumped event.updated_at
# within the snapshot's lifetime (i.e. constantly during live).
_STALENESS_SENSITIVE_PATTERNS = frozenset({
    "/api/v1/event/{event_id}",
})


def is_staleness_sensitive_endpoint(endpoint_pattern: str | None) -> bool:
    """Decide whether stale snapshot rows for this endpoint should be
    skipped (True) so the waterfall falls through to an overlay path,
    or kept as-is (False) because no downstream path can improve on
    the snapshot's payload shape.

    Default policy is **insensitive** (False): keep the snapshot. Only
    the few endpoints in _STALENESS_SENSITIVE_PATTERNS — currently just
    the /event/{event_id} root, which has _fetch_event_root_payload +
    overlay_live_fields — benefit from a skip.
    """
    if not endpoint_pattern:
        return False
    return endpoint_pattern in _STALENESS_SENSITIVE_PATTERNS


def overlay_live_fields(snapshot_payload: dict[str, Any], row: Any) -> dict[str, Any]:
    """Patch volatile fields (status / homeScore / awayScore / time /
    changes) on a stale ``/event/{id}`` snapshot with fresh values from
    a normalized synth row.

    The static parts of the snapshot (teamColors, country, fieldTranslations,
    season, tournament, hasXg, feedLocked, ...) survive untouched —
    overlay only mutates the four blocks that the bulk live-snapshot
    parser keeps fresh in our normalized tables.

    Mutates and returns ``snapshot_payload`` in place; callers that
    care about pristine inputs should ``copy.deepcopy`` first.
    """
    if not isinstance(snapshot_payload, dict):
        return snapshot_payload
    event = snapshot_payload.get("event")
    if not isinstance(event, dict):
        return snapshot_payload
    event["status"] = _build_status(row)
    event["homeScore"] = _build_score(row, side="home")
    event["awayScore"] = _build_score(row, side="away")
    time_block = _build_time(row)
    if time_block:
        event["time"] = time_block
    if row.get("changes_payload"):
        event["changes"] = row["changes_payload"]
    return snapshot_payload


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
