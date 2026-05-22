"""Snapshot classification for the replayable parser registry."""

from __future__ import annotations

from .base import RawSnapshot


def is_soft_error_payload(payload: object) -> bool:
    if not isinstance(payload, dict):
        return False
    return isinstance(payload.get("error"), dict)


def classify_snapshot(snapshot: RawSnapshot) -> str:
    pattern = snapshot.endpoint_pattern
    root_keys = snapshot.observed_root_keys

    if pattern == "/api/v1/event/{event_id}" or "event" in root_keys:
        return "event_root"
    if pattern == "/api/v1/event/{event_id}/lineups":
        return "event_lineups"
    if pattern == "/api/v1/event/{event_id}/incidents":
        return "event_incidents"
    if pattern == "/api/v1/event/{event_id}/statistics":
        return "event_statistics"
    if pattern == "/api/v1/event/{event_id}/comments":
        return "event_comments"
    if pattern == "/api/v1/event/{event_id}/graph":
        return "event_graph"
    if pattern == "/api/v1/event/{event_id}/managers":
        return "event_managers"
    if pattern == "/api/v1/event/{event_id}/h2h":
        return "event_h2h"
    if pattern == "/api/v1/event/{event_id}/pregame-form":
        return "event_pregame_form"
    if pattern == "/api/v1/event/{event_id}/votes":
        return "event_votes"
    if pattern == "/api/v1/event/{event_id}/heatmap/{team_id}":
        return "event_team_heatmap"
    if pattern in {
        "/api/v1/event/{event_id}/odds/{provider_id}/all",
        "/api/v1/event/{event_id}/odds/{provider_id}/featured",
    }:
        return "event_odds"
    if pattern == "/api/v1/event/{event_id}/provider/{provider_id}/winning-odds":
        return "event_winning_odds"
    if pattern == "/api/v1/event/{event_id}/best-players/summary":
        return "event_best_players"
    if pattern == "/api/v1/event/{event_id}/player/{player_id}/statistics":
        return "event_player_statistics"
    if pattern == "/api/v1/event/{event_id}/player/{player_id}/rating-breakdown":
        return "event_player_rating_breakdown"
    if pattern == "/api/v1/event/{event_id}/innings":
        # P0.2: live probe (2026-05-07) confirmed /innings is cricket-only on
        # prod — baseball events 100% return soft-error 404. Cricket payload
        # shape ``{innings: [{number, battingTeam, ..., wickets, overs}]}``
        # is incompatible with BaseballInningsParser. Sport-aware routing
        # keeps legacy baseball snapshots parseable while new cricket
        # snapshots fall through to ``unknown`` (raw passthrough until a
        # CricketInningsParser is registered).
        sport = str(snapshot.sport_slug or "").strip().lower()
        if sport == "cricket":
            return "cricket_innings"
        return "baseball_innings"
    if pattern == "/api/v1/event/{event_id}/atbat/{at_bat_id}/pitches":
        return "baseball_pitches"
    if pattern in {"/api/v1/event/{event_id}/shotmap", "/api/v1/event/{event_id}/shotmap/{team_id}"}:
        return "shotmap"
    if pattern == "/api/v1/event/{event_id}/esports-games":
        return "esports_games"
    if pattern in {
        "/api/v1/team/{team_id}",
        "/api/v1/player/{player_id}",
        "/api/v1/manager/{manager_id}",
    }:
        return "entity_profiles"
    if pattern == "/api/v1/event/{event_id}/point-by-point":
        return "tennis_point_by_point"
    if pattern == "/api/v1/event/{event_id}/tennis-power":
        return "tennis_power"
    # Phase 2.A: upstream seasons catalog (drives historical-backfill
    # cursor walk). Match the UT-level /seasons endpoint specifically
    # so the catalog walks UT-by-UT.
    if pattern == "/api/v1/unique-tournament/{unique_tournament_id}/seasons":
        return "tournament_season_upstream_catalog"
    if pattern.endswith("/season/{season_id}/rounds"):
        return "season_rounds"
    if pattern.endswith("/season/{season_id}/cuptrees"):
        return "season_cuptrees"
    if pattern.endswith("/statistics/info"):
        return "season_info"
    if pattern.endswith("/standings/{scope}"):
        return "season_standings"
    if pattern.endswith("/statistics"):
        return "entity_season_statistics"
    return "unknown"
