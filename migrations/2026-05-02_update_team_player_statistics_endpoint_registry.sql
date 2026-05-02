-- Keep endpoint metadata aligned with the normalized team-context tables.

UPDATE endpoint_registry
SET target_table = 'team_player_statistics_season'
WHERE pattern = '/api/v1/team/{team_id}/player-statistics/seasons';
