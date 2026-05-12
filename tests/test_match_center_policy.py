from schema_inspector.match_center_policy import (
    football_detail_endpoint_allowed,
    football_edge_allowed,
    football_highlights_allowed,
    football_special_allowed,
)


def test_football_null_detail_blocks_premium_edges_but_not_incidents() -> None:
    """X'' patch: detail_id=None blocks premium edges but ALLOWS /incidents
    and /lineups.

    Empirical /api audit (67 events) confirmed 100% upstream availability
    of /incidents regardless of detail_id. /lineups also returns 200 even
    before kickoff with ``confirmed=false`` (probable lineup); legacy
    tier_5 block was over-conservative. /statistics, /graph still gated.
    """
    # /statistics still blocked — needs detailId∈{1,4,6} (tier_1/2).
    assert football_edge_allowed(
        sport_slug="football",
        edge_kind="statistics",
        detail_id=None,
        status_type="inprogress",
        has_xg=True,
    ) is False
    # /graph still blocked.
    assert football_edge_allowed(
        sport_slug="football",
        edge_kind="graph",
        detail_id=None,
        status_type="inprogress",
        has_xg=True,
    ) is False
    # /incidents UNLOCKED for non-isEditor football regardless of detail_id.
    assert football_edge_allowed(
        sport_slug="football",
        edge_kind="incidents",
        detail_id=None,
        status_type="finished",
        has_xg=None,
    ) is True
    # /lineups UNLOCKED for non-isEditor football regardless of status_type
    # — upstream returns probable lineups (confirmed=false) before kickoff.
    for status in ("finished", "inprogress", "live", "notstarted", "scheduled"):
        assert football_edge_allowed(
            sport_slug="football",
            edge_kind="lineups",
            detail_id=None,
            status_type=status,
            has_xg=None,
        ) is True, f"/lineups must be allowed for status={status!r} (null detail, non-isEditor)"


def test_football_notstarted_only_keeps_safe_core_edges() -> None:
    assert football_edge_allowed(
        sport_slug="football",
        edge_kind="lineups",
        detail_id=1,
        status_type="scheduled",
        has_xg=True,
    ) is True
    assert football_edge_allowed(
        sport_slug="football",
        edge_kind="incidents",
        detail_id=1,
        status_type="notstarted",
        has_xg=True,
    ) is True
    assert football_edge_allowed(
        sport_slug="football",
        edge_kind="statistics",
        detail_id=1,
        status_type="scheduled",
        has_xg=True,
    ) is False
    assert football_edge_allowed(
        sport_slug="football",
        edge_kind="graph",
        detail_id=1,
        status_type="scheduled",
        has_xg=True,
    ) is False


def test_football_problem_detail_keeps_only_incidents() -> None:
    assert football_edge_allowed(
        sport_slug="football",
        edge_kind="incidents",
        detail_id=3,
        status_type="finished",
        has_xg=None,
    ) is True
    assert football_edge_allowed(
        sport_slug="football",
        edge_kind="lineups",
        detail_id=3,
        status_type="finished",
        has_xg=None,
    ) is False


def test_football_tier_one_allows_analytics_with_flags() -> None:
    assert football_detail_endpoint_allowed(
        sport_slug="football",
        endpoint_pattern="/api/v1/event/{event_id}/shotmap",
        detail_id=1,
        status_type="inprogress",
        has_xg=True,
        has_event_player_heat_map=False,
        has_event_player_statistics=True,
        has_global_highlights=False,
        start_timestamp=1_000,
        now_timestamp=20_000,
    ) is True
    assert football_special_allowed(
        sport_slug="football",
        special_kind="event_player_rating_breakdown",
        detail_id=1,
        has_event_player_statistics=False,
        has_event_player_heat_map=False,
        has_xg=False,
    ) is True


def test_football_notstarted_detail_routes_follow_matrix() -> None:
    assert football_detail_endpoint_allowed(
        sport_slug="football",
        endpoint_pattern="/api/v1/event/{event_id}/h2h",
        detail_id=1,
        status_type="scheduled",
        has_xg=True,
        has_event_player_heat_map=True,
        has_event_player_statistics=True,
        has_global_highlights=False,
        start_timestamp=1_000,
        now_timestamp=20_000,
    ) is True
    assert football_detail_endpoint_allowed(
        sport_slug="football",
        endpoint_pattern="/api/v1/event/{event_id}/graph",
        detail_id=1,
        status_type="scheduled",
        has_xg=True,
        has_event_player_heat_map=True,
        has_event_player_statistics=True,
        has_global_highlights=False,
        start_timestamp=1_000,
        now_timestamp=20_000,
    ) is False


def test_football_custom_id_and_team_streak_routes_are_allowed_pre_match() -> None:
    assert football_detail_endpoint_allowed(
        sport_slug="football",
        endpoint_pattern="/api/v1/event/{custom_id}/h2h/events",
        detail_id=1,
        status_type="scheduled",
        has_xg=False,
        has_event_player_heat_map=False,
        has_event_player_statistics=False,
        has_global_highlights=False,
        start_timestamp=1_000,
        now_timestamp=20_000,
    ) is True
    assert football_detail_endpoint_allowed(
        sport_slug="football",
        endpoint_pattern="/api/v1/event/{event_id}/team-streaks",
        detail_id=4,
        status_type="scheduled",
        has_xg=False,
        has_event_player_heat_map=False,
        has_event_player_statistics=False,
        has_global_highlights=False,
        start_timestamp=1_000,
        now_timestamp=20_000,
    ) is True


def test_football_tier_two_blocks_comments_but_keeps_safe_routes() -> None:
    assert football_detail_endpoint_allowed(
        sport_slug="football",
        endpoint_pattern="/api/v1/event/{event_id}/comments",
        detail_id=4,
        status_type="finished",
        has_xg=True,
        has_event_player_heat_map=True,
        has_event_player_statistics=True,
        has_global_highlights=False,
        start_timestamp=1_000,
        now_timestamp=20_000,
    ) is False
    assert football_detail_endpoint_allowed(
        sport_slug="football",
        endpoint_pattern="/api/v1/event/{event_id}/graph",
        detail_id=4,
        status_type="finished",
        has_xg=True,
        has_event_player_heat_map=True,
        has_event_player_statistics=True,
        has_global_highlights=False,
        start_timestamp=1_000,
        now_timestamp=20_000,
    ) is True


def test_football_highlights_wait_until_finished_plus_one_nominal_hour() -> None:
    assert football_highlights_allowed(
        sport_slug="football",
        detail_id=1,
        status_type="finished",
        has_global_highlights=True,
        start_timestamp=1_000,
        now_timestamp=1_000 + 149 * 60,
    ) is False
    assert football_highlights_allowed(
        sport_slug="football",
        detail_id=1,
        status_type="finished",
        has_global_highlights=True,
        start_timestamp=1_000,
        now_timestamp=1_000 + 150 * 60,
    ) is True


def test_highlights_gate_is_football_only() -> None:
    assert football_highlights_allowed(
        sport_slug="tennis",
        detail_id=1,
        status_type="finished",
        has_global_highlights=True,
        start_timestamp=1_000,
        now_timestamp=1_000 + 150 * 60,
    ) is False


def test_football_special_gates_require_matching_capabilities() -> None:
    assert football_special_allowed(
        sport_slug="football",
        special_kind="event_player_heatmap",
        detail_id=1,
        has_event_player_statistics=True,
        has_event_player_heat_map=True,
        has_xg=False,
    ) is True
    assert football_special_allowed(
        sport_slug="football",
        special_kind="event_player_heatmap",
        detail_id=1,
        has_event_player_statistics=True,
        has_event_player_heat_map=False,
        has_xg=False,
    ) is False
    assert football_special_allowed(
        sport_slug="football",
        special_kind="event_player_shotmap",
        detail_id=1,
        has_event_player_statistics=True,
        has_event_player_heat_map=True,
        has_xg=True,
    ) is True
    assert football_special_allowed(
        sport_slug="football",
        special_kind="event_goalkeeper_shotmap",
        detail_id=4,
        has_event_player_statistics=True,
        has_event_player_heat_map=True,
        has_xg=False,
    ) is False


# ---------------------------------------------------------------------------
# X'' (2026-05-12) matchcenter policy fix — ``is_editor`` HARD BAN tests.
#
# Production policy: events whose root payload reports ``isEditor=true`` are
# published via SofaEditor's crowdsourcing app. The user requirement is that
# such events are never matchcenter-fanned-out, regardless of detail_id,
# status, capability flags, or sport-specific bypass paths.
# ---------------------------------------------------------------------------


def test_x2_isEditor_true_blocks_every_edge_even_when_tier_one() -> None:
    """isEditor=True overrides tier_1 path. No edges fetched ever."""
    for edge in ("incidents", "lineups", "statistics", "graph"):
        assert football_edge_allowed(
            sport_slug="football",
            edge_kind=edge,
            detail_id=1,
            status_type="inprogress",
            has_xg=True,
            is_editor=True,
        ) is False, f"isEditor=True must block edge_kind={edge!r}"


def test_x2_isEditor_true_blocks_edges_for_no_detail() -> None:
    """isEditor=True + detail_id=None: every edge stays blocked."""
    for edge in ("incidents", "lineups", "statistics", "graph"):
        assert football_edge_allowed(
            sport_slug="football",
            edge_kind=edge,
            detail_id=None,
            status_type="finished",
            has_xg=None,
            is_editor=True,
        ) is False


def test_x2_isEditor_false_unblocks_incidents_with_null_detail() -> None:
    """Non-isEditor football + null detail_id: /incidents now allowed."""
    assert football_edge_allowed(
        sport_slug="football",
        edge_kind="incidents",
        detail_id=None,
        status_type="inprogress",
        has_xg=None,
        is_editor=False,
    ) is True
    # And also when is_editor is unknown/None (legacy callers).
    assert football_edge_allowed(
        sport_slug="football",
        edge_kind="incidents",
        detail_id=None,
        status_type="notstarted",
        has_xg=None,
        is_editor=None,
    ) is True


def test_x2_isEditor_false_lineups_unblocked_for_null_detail_any_status() -> None:
    """/lineups: tier_5 (null detail) allowed for ALL statuses for non-isEditor.

    Empirical: upstream returns ``/lineups`` 200 even pre-kickoff with
    ``confirmed=false`` (probable lineup, not 404). status_type must NOT
    be used to gate lineups for tier_5 events.
    """
    for status in ("finished", "inprogress", "live", "notstarted", "scheduled"):
        assert football_edge_allowed(
            sport_slug="football",
            edge_kind="lineups",
            detail_id=None,
            status_type=status,
            has_xg=None,
            is_editor=False,
        ) is True, f"/lineups must be allowed for status={status!r}"


def test_x2_isEditor_true_blocks_lineups_even_notstarted() -> None:
    """isEditor=True overrides /lineups unlock even for pre-match events."""
    for status in ("finished", "inprogress", "live", "notstarted", "scheduled"):
        assert football_edge_allowed(
            sport_slug="football",
            edge_kind="lineups",
            detail_id=None,
            status_type=status,
            has_xg=None,
            is_editor=True,
        ) is False, f"isEditor=True must block lineups even at status={status!r}"
        # Also when detail_id=1 (tier_1) — HARD BAN trumps tier privileges.
        assert football_edge_allowed(
            sport_slug="football",
            edge_kind="lineups",
            detail_id=1,
            status_type=status,
            has_xg=True,
            is_editor=True,
        ) is False


def test_x2_tier_one_unchanged_for_non_isEditor() -> None:
    """Premier League-style events (detail_id=1, isEditor=false) keep
    pre-patch behaviour: full ACTIVE_CORE_EDGES + NOTSTARTED_CORE_EDGES."""
    for edge, status in (
        ("incidents", "inprogress"),
        ("lineups", "inprogress"),
        ("statistics", "inprogress"),
        ("graph", "inprogress"),
        ("incidents", "notstarted"),
        ("lineups", "notstarted"),
    ):
        assert football_edge_allowed(
            sport_slug="football",
            edge_kind=edge,
            detail_id=1,
            status_type=status,
            has_xg=True,
            is_editor=False,
        ) is True, f"tier_1 must keep edge={edge!r} status={status!r}"
    # statistics/graph still blocked pre-match for tier_1 (legacy behaviour).
    assert football_edge_allowed(
        sport_slug="football",
        edge_kind="statistics",
        detail_id=1,
        status_type="notstarted",
        has_xg=True,
        is_editor=False,
    ) is False


def test_x2_player_followups_banned_for_isEditor() -> None:
    """All player specials skipped for isEditor=True regardless of flags."""
    for special in (
        "event_player_statistics",
        "event_player_heatmap",
        "event_player_shotmap",
        "event_player_rating_breakdown",
        "best_players_summary",
    ):
        assert football_special_allowed(
            sport_slug="football",
            special_kind=special,
            detail_id=1,
            has_event_player_statistics=True,
            has_event_player_heat_map=True,
            has_xg=True,
            is_editor=True,
        ) is False, f"isEditor=True must block special={special!r}"


def test_x2_player_followups_unchanged_for_non_isEditor() -> None:
    """Existing tier_1 player followup behaviour preserved."""
    assert football_special_allowed(
        sport_slug="football",
        special_kind="event_player_statistics",
        detail_id=1,
        has_event_player_statistics=True,
        has_event_player_heat_map=True,
        has_xg=True,
        is_editor=False,
    ) is True


def test_x2_detail_endpoints_banned_for_isEditor() -> None:
    """Every detail endpoint (managers/h2h/comments/heatmap/shotmap/...)
    blocked under HARD BAN."""
    for endpoint in (
        "/api/v1/event/{event_id}/managers",
        "/api/v1/event/{event_id}/h2h",
        "/api/v1/event/{event_id}/comments",
        "/api/v1/event/{event_id}/shotmap",
        "/api/v1/event/{event_id}/heatmap/{team_id}",
        "/api/v1/event/{event_id}/best-players/summary",
        "/api/v1/event/{event_id}/highlights",
    ):
        assert football_detail_endpoint_allowed(
            sport_slug="football",
            endpoint_pattern=endpoint,
            detail_id=1,
            status_type="finished",
            has_xg=True,
            has_event_player_heat_map=True,
            has_event_player_statistics=True,
            has_global_highlights=True,
            start_timestamp=1_000,
            now_timestamp=1_000 + 200 * 60,
            is_editor=True,
        ) is False, f"isEditor=True must block detail endpoint={endpoint!r}"


def test_x2_highlights_banned_for_isEditor() -> None:
    """Highlights specifically blocked even when aging + tier + feature OK."""
    assert football_highlights_allowed(
        sport_slug="football",
        detail_id=1,
        status_type="finished",
        has_global_highlights=True,
        start_timestamp=1_000,
        now_timestamp=1_000 + 200 * 60,
        is_editor=True,
    ) is False
    # Sanity: same call without is_editor still passes.
    assert football_highlights_allowed(
        sport_slug="football",
        detail_id=1,
        status_type="finished",
        has_global_highlights=True,
        start_timestamp=1_000,
        now_timestamp=1_000 + 200 * 60,
        is_editor=False,
    ) is True


def test_x2_filter_specs_short_circuits_empty_for_isEditor() -> None:
    """filter_football_detail_specs returns () for isEditor=True football."""
    from dataclasses import dataclass

    @dataclass(frozen=True)
    class _StubEndpoint:
        pattern: str

    @dataclass(frozen=True)
    class _StubSpec:
        endpoint: _StubEndpoint

    specs = (
        _StubSpec(_StubEndpoint("/api/v1/event/{event_id}/managers")),
        _StubSpec(_StubEndpoint("/api/v1/event/{event_id}/h2h")),
    )
    from schema_inspector.match_center_policy import filter_football_detail_specs

    out = filter_football_detail_specs(
        specs,
        sport_slug="football",
        detail_id=1,
        status_type="finished",
        has_xg=True,
        has_event_player_heat_map=True,
        has_event_player_statistics=True,
        has_global_highlights=True,
        start_timestamp=1_000,
        now_timestamp=1_000 + 200 * 60,
        is_editor=True,
    )
    assert out == ()


def test_x2_non_football_sport_bypass_unchanged() -> None:
    """is_editor flag has no effect on non-football sports."""
    for edge in ("incidents", "lineups", "statistics", "graph"):
        assert football_edge_allowed(
            sport_slug="tennis",
            edge_kind=edge,
            detail_id=None,
            status_type="inprogress",
            has_xg=None,
            is_editor=True,
        ) is True


def test_x2_legacy_callers_without_is_editor_keep_working() -> None:
    """Defensive: existing callers that omit is_editor see legacy behaviour
    (treated as is_editor=None → not banned)."""
    # tier_1 incidents still allowed (no is_editor kwarg).
    assert football_edge_allowed(
        sport_slug="football",
        edge_kind="incidents",
        detail_id=1,
        status_type="inprogress",
        has_xg=True,
    ) is True
    # And the new /incidents unlock for null detail also applies.
    assert football_edge_allowed(
        sport_slug="football",
        edge_kind="incidents",
        detail_id=None,
        status_type="finished",
        has_xg=None,
    ) is True


# ---------------------------------------------------------------------------
# X3 (2026-05-12) matchcenter policy fix — ``detail_id IS NULL`` pre-match
# unlock for non-isEditor football events.
#
# Production rationale: empirical UI audit on Premier League pre-match event
# 15999228 (uniqueTournament=17) showed ``detailId`` is **simply missing**
# from the root payload before kickoff (NOT NULL, the field is absent
# entirely). 93.6% of all football events in prod fall into this cohort.
# Upstream still returns 200 for /managers, /h2h, /pregame-form, /votes,
# /odds/{p}/all, /odds/{p}/featured, /winning-odds, /team-streaks,
# /team-streaks/betting-odds/{p}. Premium endpoints (graph, comments,
# heatmap, shotmap, best-players, official-tweets, average-positions,
# highlights) stay blocked for tier_5 because those genuinely require
# detailId coverage.
# ---------------------------------------------------------------------------

_PREMATCH_DETAIL_ENDPOINTS = (
    "/api/v1/event/{event_id}/managers",
    "/api/v1/event/{event_id}/h2h",
    "/api/v1/event/{event_id}/pregame-form",
    "/api/v1/event/{event_id}/votes",
    "/api/v1/event/{event_id}/odds/{provider_id}/all",
    "/api/v1/event/{event_id}/odds/{provider_id}/featured",
    "/api/v1/event/{event_id}/provider/{provider_id}/winning-odds",
    "/api/v1/event/{event_id}/team-streaks",
    "/api/v1/event/{event_id}/team-streaks/betting-odds/{provider_id}",
    "/api/v1/event/{custom_id}/h2h/events",
)

_PREMIUM_DETAIL_ENDPOINTS = (
    "/api/v1/event/{event_id}/graph",
    "/api/v1/event/{event_id}/comments",
    "/api/v1/event/{event_id}/best-players/summary",
    "/api/v1/event/{event_id}/official-tweets",
    "/api/v1/event/{event_id}/heatmap/{team_id}",
    "/api/v1/event/{event_id}/shotmap",
    "/api/v1/event/{event_id}/average-positions",
    "/api/v1/event/{event_id}/highlights",
)


def test_x3_null_detail_allows_prematch_detail_endpoints_for_non_editor() -> None:
    """detail_id=None + isEditor=false: pre-match detail endpoints allowed.

    Empirical: Manchester City vs Crystal Palace (Premier League, status
    notstarted, detailId missing from root payload) — UI fetched and
    upstream returned 200 for the full pre-match detail bundle.
    """
    for status in ("notstarted", "scheduled", "inprogress", "live", "finished"):
        for endpoint in _PREMATCH_DETAIL_ENDPOINTS:
            assert football_detail_endpoint_allowed(
                sport_slug="football",
                endpoint_pattern=endpoint,
                detail_id=None,
                status_type=status,
                has_xg=None,
                has_event_player_heat_map=None,
                has_event_player_statistics=None,
                has_global_highlights=None,
                start_timestamp=1_000,
                now_timestamp=20_000,
                is_editor=False,
            ) is True, f"endpoint={endpoint!r} must be allowed for tier_5 status={status!r}"


def test_x3_null_detail_blocks_premium_detail_endpoints_for_non_editor() -> None:
    """detail_id=None: premium endpoints stay blocked even for non-editor.

    These endpoints genuinely require tier_1/tier_2 coverage; running them
    without detailId would cost requests for very low success rates.
    """
    # /highlights uses football_highlights_allowed, which has its own
    # tier check. The remaining premium endpoints go through the main
    # tier_5 block.
    for status in ("inprogress", "live", "finished"):
        for endpoint in _PREMIUM_DETAIL_ENDPOINTS:
            assert football_detail_endpoint_allowed(
                sport_slug="football",
                endpoint_pattern=endpoint,
                detail_id=None,
                status_type=status,
                has_xg=True,
                has_event_player_heat_map=True,
                has_event_player_statistics=True,
                has_global_highlights=True,
                start_timestamp=1_000,
                now_timestamp=1_000 + 200 * 60,
                is_editor=False,
            ) is False, f"premium endpoint={endpoint!r} must stay blocked for tier_5 status={status!r}"


def test_x3_null_detail_with_is_editor_true_blocks_everything() -> None:
    """detail_id=None + isEditor=True: HARD BAN supersedes pre-match unlock."""
    for endpoint in (*_PREMATCH_DETAIL_ENDPOINTS, *_PREMIUM_DETAIL_ENDPOINTS):
        for status in ("notstarted", "inprogress", "finished"):
            assert football_detail_endpoint_allowed(
                sport_slug="football",
                endpoint_pattern=endpoint,
                detail_id=None,
                status_type=status,
                has_xg=True,
                has_event_player_heat_map=True,
                has_event_player_statistics=True,
                has_global_highlights=True,
                start_timestamp=1_000,
                now_timestamp=1_000 + 200 * 60,
                is_editor=True,
            ) is False, (
                f"isEditor=True must block endpoint={endpoint!r} for tier_5 status={status!r}"
            )


def test_x3_tier_one_unchanged_after_null_detail_unlock() -> None:
    """Sanity: detail_id=1 (tier_1) keeps full pre-X3 behaviour."""
    # tier_1 + finished: everything allowed (premium endpoints too).
    for endpoint in (
        "/api/v1/event/{event_id}/managers",
        "/api/v1/event/{event_id}/h2h",
        "/api/v1/event/{event_id}/pregame-form",
        "/api/v1/event/{event_id}/votes",
        "/api/v1/event/{event_id}/comments",
        "/api/v1/event/{event_id}/best-players/summary",
        "/api/v1/event/{event_id}/official-tweets",
        "/api/v1/event/{event_id}/average-positions",
        "/api/v1/event/{event_id}/graph",
    ):
        assert football_detail_endpoint_allowed(
            sport_slug="football",
            endpoint_pattern=endpoint,
            detail_id=1,
            status_type="finished",
            has_xg=True,
            has_event_player_heat_map=True,
            has_event_player_statistics=True,
            has_global_highlights=True,
            start_timestamp=1_000,
            now_timestamp=1_000 + 200 * 60,
            is_editor=False,
        ) is True, f"tier_1 finished must keep endpoint={endpoint!r}"


def test_x3_tier_two_unchanged_after_null_detail_unlock() -> None:
    """Sanity: detail_id ∈ {4, 6} (tier_2) keeps pre-X3 _TIER_2_DETAIL_ENDPOINTS
    whitelist behaviour (no comments, no best-players, no heatmap, no shotmap).
    """
    for tier_2_detail in (4, 6):
        # tier_2 keeps managers/h2h/pregame-form/odds/team-streaks.
        for endpoint in _PREMATCH_DETAIL_ENDPOINTS:
            assert football_detail_endpoint_allowed(
                sport_slug="football",
                endpoint_pattern=endpoint,
                detail_id=tier_2_detail,
                status_type="finished",
                has_xg=True,
                has_event_player_heat_map=True,
                has_event_player_statistics=True,
                has_global_highlights=False,
                start_timestamp=1_000,
                now_timestamp=20_000,
                is_editor=False,
            ) is True, f"tier_2 (detail={tier_2_detail}) keeps endpoint={endpoint!r}"
        # tier_2 still blocks /comments (only tier_1 gets it).
        assert football_detail_endpoint_allowed(
            sport_slug="football",
            endpoint_pattern="/api/v1/event/{event_id}/comments",
            detail_id=tier_2_detail,
            status_type="finished",
            has_xg=True,
            has_event_player_heat_map=True,
            has_event_player_statistics=True,
            has_global_highlights=False,
            start_timestamp=1_000,
            now_timestamp=20_000,
            is_editor=False,
        ) is False


def test_x3_tier_three_unchanged_after_null_detail_unlock() -> None:
    """Sanity: detail_id ∈ {2, 3, 5} (tier_3 = "problem detail" cohort)
    stays fully blocked except for /incidents at edge level (not detail).

    detailId=2/3/5 represents events where upstream coverage IS poor — the
    X3 unlock for tier_5 (= detailId missing entirely) does NOT extend to
    tier_3 because those are different empirical cohorts.
    """
    for tier_3_detail in (2, 3, 5):
        for endpoint in _PREMATCH_DETAIL_ENDPOINTS:
            assert football_detail_endpoint_allowed(
                sport_slug="football",
                endpoint_pattern=endpoint,
                detail_id=tier_3_detail,
                status_type="finished",
                has_xg=True,
                has_event_player_heat_map=True,
                has_event_player_statistics=True,
                has_global_highlights=False,
                start_timestamp=1_000,
                now_timestamp=20_000,
                is_editor=False,
            ) is False, f"tier_3 (detail={tier_3_detail}) must keep blocking endpoint={endpoint!r}"


def test_x3_event_14083649_simulation_notstarted_lineups() -> None:
    """Simulates event 14083649 (notstarted football, detail_id unknown).

    /lineups must be allowed regardless of detail_id (probable lineup
    pre-kickoff). /incidents must be allowed.
    """
    # /lineups via edge gate.
    assert football_edge_allowed(
        sport_slug="football",
        edge_kind="lineups",
        detail_id=None,
        status_type="notstarted",
        has_xg=None,
        is_editor=False,
    ) is True
    # /incidents via edge gate.
    assert football_edge_allowed(
        sport_slug="football",
        edge_kind="incidents",
        detail_id=None,
        status_type="notstarted",
        has_xg=None,
        is_editor=False,
    ) is True


def test_x3_event_14090461_simulation_incidents_and_lineups_allowed() -> None:
    """Simulates event 14090461 (football, low-tier or detail unknown).

    /incidents and /lineups must be allowed; /statistics, /graph, /shotmap
    must remain blocked when capability flags say so.
    """
    common_kwargs = dict(
        sport_slug="football",
        detail_id=None,
        status_type="inprogress",
        has_xg=None,
        is_editor=False,
    )
    assert football_edge_allowed(**common_kwargs, edge_kind="incidents") is True
    assert football_edge_allowed(**common_kwargs, edge_kind="lineups") is True
    assert football_edge_allowed(**common_kwargs, edge_kind="statistics") is False
    assert football_edge_allowed(**common_kwargs, edge_kind="graph") is False
    # /shotmap is gated via detail endpoint policy not edge policy; with
    # has_xg=None and detail_id=None → blocked.
    assert football_detail_endpoint_allowed(
        sport_slug="football",
        endpoint_pattern="/api/v1/event/{event_id}/shotmap",
        detail_id=None,
        status_type="inprogress",
        has_xg=None,
        has_event_player_heat_map=None,
        has_event_player_statistics=None,
        has_global_highlights=None,
        start_timestamp=1_000,
        now_timestamp=20_000,
        is_editor=False,
    ) is False


def test_x3_event_15999228_simulation_premier_league_prematch() -> None:
    """Simulates event 15999228 (Manchester City vs Crystal Palace,
    Premier League pre-match, detailId MISSING from root payload).

    All pre-match detail endpoints must be allowed (upstream returns 200).
    Premium endpoints blocked because of tier_5 + no capability flags.
    """
    pl_event_common = dict(
        sport_slug="football",
        detail_id=None,
        status_type="notstarted",
        has_xg=None,
        has_event_player_heat_map=None,
        has_event_player_statistics=None,
        has_global_highlights=None,
        start_timestamp=1_000,
        now_timestamp=1_000,
        is_editor=False,
    )
    # Pre-match detail bundle — UI confirmed 200 for ALL of these.
    expected_allowed = (
        "/api/v1/event/{event_id}/managers",
        "/api/v1/event/{event_id}/h2h",
        "/api/v1/event/{event_id}/pregame-form",
        "/api/v1/event/{event_id}/votes",
        "/api/v1/event/{event_id}/odds/{provider_id}/all",
        "/api/v1/event/{event_id}/odds/{provider_id}/featured",
        "/api/v1/event/{event_id}/provider/{provider_id}/winning-odds",
        "/api/v1/event/{event_id}/team-streaks",
        "/api/v1/event/{event_id}/team-streaks/betting-odds/{provider_id}",
        "/api/v1/event/{custom_id}/h2h/events",
    )
    for endpoint in expected_allowed:
        assert football_detail_endpoint_allowed(
            endpoint_pattern=endpoint, **pl_event_common
        ) is True, f"Premier League pre-match must allow {endpoint!r}"
    # Premium endpoints — upstream returned 404 in UI audit, policy blocks.
    for endpoint in (
        "/api/v1/event/{event_id}/comments",
        "/api/v1/event/{event_id}/best-players/summary",
        "/api/v1/event/{event_id}/heatmap/{team_id}",
        "/api/v1/event/{event_id}/shotmap",
        "/api/v1/event/{event_id}/average-positions",
        "/api/v1/event/{event_id}/highlights",
        "/api/v1/event/{event_id}/official-tweets",
        "/api/v1/event/{event_id}/graph",
    ):
        assert football_detail_endpoint_allowed(
            endpoint_pattern=endpoint, **pl_event_common
        ) is False, f"Premier League pre-match must block premium {endpoint!r}"


def test_x3_null_detail_filter_specs_passes_prematch_for_non_editor() -> None:
    """filter_football_detail_specs keeps pre-match specs for tier_5
    non-editor events, drops premium specs."""
    from dataclasses import dataclass

    @dataclass(frozen=True)
    class _StubEndpoint:
        pattern: str

    @dataclass(frozen=True)
    class _StubSpec:
        endpoint: _StubEndpoint

    specs = (
        _StubSpec(_StubEndpoint("/api/v1/event/{event_id}/managers")),  # pre-match → kept
        _StubSpec(_StubEndpoint("/api/v1/event/{event_id}/h2h")),  # pre-match → kept
        _StubSpec(_StubEndpoint("/api/v1/event/{event_id}/pregame-form")),  # pre-match → kept
        _StubSpec(_StubEndpoint("/api/v1/event/{event_id}/comments")),  # premium → dropped
        _StubSpec(_StubEndpoint("/api/v1/event/{event_id}/shotmap")),  # premium → dropped
        _StubSpec(_StubEndpoint("/api/v1/event/{event_id}/graph")),  # premium → dropped
    )
    from schema_inspector.match_center_policy import filter_football_detail_specs

    out = filter_football_detail_specs(
        specs,
        sport_slug="football",
        detail_id=None,  # tier_5
        status_type="inprogress",
        has_xg=None,
        has_event_player_heat_map=None,
        has_event_player_statistics=None,
        has_global_highlights=None,
        start_timestamp=1_000,
        now_timestamp=20_000,
        is_editor=False,
    )
    kept_patterns = {s.endpoint.pattern for s in out}
    assert kept_patterns == {
        "/api/v1/event/{event_id}/managers",
        "/api/v1/event/{event_id}/h2h",
        "/api/v1/event/{event_id}/pregame-form",
    }


def test_x3_non_football_unchanged_for_null_detail() -> None:
    """Other sports bypass football policy unchanged."""
    for sport in ("tennis", "basketball", "baseball", "esports"):
        # All detail endpoints bypass for non-football → True.
        assert football_detail_endpoint_allowed(
            sport_slug=sport,
            endpoint_pattern="/api/v1/event/{event_id}/managers",
            detail_id=None,
            status_type="inprogress",
            has_xg=None,
            has_event_player_heat_map=None,
            has_event_player_statistics=None,
            has_global_highlights=None,
            start_timestamp=1_000,
            now_timestamp=20_000,
            is_editor=False,
        ) is True
