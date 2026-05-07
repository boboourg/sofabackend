-- D1 endpoint_registry seed: 5 new patterns added to endpoints.py must also
-- be present in endpoint_registry, otherwise api_payload_snapshot inserts
-- fail with a foreign-key violation on endpoint_pattern. Workers fetch
-- upstream successfully but the snapshot never lands.
--
-- This migration is idempotent (ON CONFLICT DO NOTHING) so it can be replayed.
-- It is the operational complement of commit e298833 (D1 constants).

INSERT INTO endpoint_registry (pattern, path_template, query_template, envelope_key, target_table, notes, source_slug, contract_version)
VALUES
('/api/v1/event/{event_id}/at-bats',
 '/api/v1/event/{event_id}/at-bats',
 NULL,
 'atBats',
 'api_payload_snapshot',
 'D1: Baseball game at-bats list; parent of /atbat/{at_bat_id}/pitches.',
 'sofascore', 'v1'),
('/api/v1/team/{team_id}/unique-tournament/{unique_tournament_id}/season/{season_id}/goal-distributions',
 '/api/v1/team/{team_id}/unique-tournament/{unique_tournament_id}/season/{season_id}/goal-distributions',
 NULL,
 'goalDistributions',
 'api_payload_snapshot',
 'D1: Football team per-season goal distribution by minute buckets.',
 'sofascore', 'v1'),
('/api/v1/player/{player_id}/statistics/match-type/overall',
 '/api/v1/player/{player_id}/statistics/match-type/overall',
 NULL,
 'seasons,typesMap',
 'api_payload_snapshot',
 'D1: Per-match-type aggregated season statistics.',
 'sofascore', 'v1'),
('/api/v1/player/{player_id}/national-team-statistics',
 '/api/v1/player/{player_id}/national-team-statistics',
 NULL,
 'statistics',
 'api_payload_snapshot',
 'D1: Player career national-team aggregate.',
 'sofascore', 'v1'),
('/api/v1/player/{player_id}/last-year-summary',
 '/api/v1/player/{player_id}/last-year-summary',
 NULL,
 'summary,uniqueTournamentsMap',
 'api_payload_snapshot',
 'D1: Player rolling 12-month per-tournament summary.',
 'sofascore', 'v1')
ON CONFLICT (pattern) DO NOTHING;
