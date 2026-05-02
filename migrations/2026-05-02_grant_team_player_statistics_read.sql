-- Allow the local API read role to serve
-- /api/v1/team/{team_id}/player-statistics/seasons from normalized tables.

GRANT SELECT ON TABLE team_player_statistics_season TO sofascore_user;
GRANT SELECT ON TABLE team_player_statistics_type TO sofascore_user;
