-- Preserve team context for /api/v1/team/{team_id}/player-statistics/seasons.
-- The global season_statistics_type table remains the competition-level catalog;
-- these tables model which player-stat seasons/types are available under a
-- specific team context.

CREATE TABLE IF NOT EXISTS team_player_statistics_season (
    team_id BIGINT NOT NULL REFERENCES team(id),
    unique_tournament_id BIGINT NOT NULL REFERENCES unique_tournament(id),
    season_id BIGINT NOT NULL REFERENCES season(id),
    PRIMARY KEY (team_id, unique_tournament_id, season_id)
);

CREATE TABLE IF NOT EXISTS team_player_statistics_type (
    team_id BIGINT NOT NULL,
    unique_tournament_id BIGINT NOT NULL,
    season_id BIGINT NOT NULL,
    stat_type TEXT NOT NULL,
    PRIMARY KEY (team_id, unique_tournament_id, season_id, stat_type),
    FOREIGN KEY (team_id, unique_tournament_id, season_id)
        REFERENCES team_player_statistics_season(team_id, unique_tournament_id, season_id)
        ON DELETE CASCADE
);

WITH latest_snapshots AS (
    SELECT DISTINCT ON (context_entity_id, source_url)
        context_entity_id AS team_id,
        payload
    FROM api_payload_snapshot
    WHERE endpoint_pattern = '/api/v1/team/{team_id}/player-statistics/seasons'
      AND context_entity_type = 'team'
      AND context_entity_id IS NOT NULL
    ORDER BY context_entity_id, source_url, fetched_at DESC, id DESC
),
season_rows AS (
    SELECT
        latest_snapshots.team_id,
        (ut_item->'uniqueTournament'->>'id')::bigint AS unique_tournament_id,
        (season_item->>'id')::bigint AS season_id
    FROM latest_snapshots
    CROSS JOIN LATERAL jsonb_array_elements(
        COALESCE(latest_snapshots.payload->'uniqueTournamentSeasons', '[]'::jsonb)
    ) AS ut_item
    CROSS JOIN LATERAL jsonb_array_elements(
        COALESCE(ut_item->'seasons', '[]'::jsonb)
    ) AS season_item
    WHERE ut_item->'uniqueTournament'->>'id' IS NOT NULL
      AND season_item->>'id' IS NOT NULL
)
INSERT INTO team_player_statistics_season (team_id, unique_tournament_id, season_id)
SELECT DISTINCT season_rows.team_id, season_rows.unique_tournament_id, season_rows.season_id
FROM season_rows
JOIN team ON team.id = season_rows.team_id
JOIN unique_tournament ON unique_tournament.id = season_rows.unique_tournament_id
JOIN season ON season.id = season_rows.season_id
ON CONFLICT (team_id, unique_tournament_id, season_id) DO NOTHING;

WITH latest_snapshots AS (
    SELECT DISTINCT ON (context_entity_id, source_url)
        context_entity_id AS team_id,
        payload
    FROM api_payload_snapshot
    WHERE endpoint_pattern = '/api/v1/team/{team_id}/player-statistics/seasons'
      AND context_entity_type = 'team'
      AND context_entity_id IS NOT NULL
    ORDER BY context_entity_id, source_url, fetched_at DESC, id DESC
),
type_rows AS (
    SELECT
        latest_snapshots.team_id,
        ut_entry.key::bigint AS unique_tournament_id,
        season_entry.key::bigint AS season_id,
        stat_type.value #>> '{}' AS stat_type
    FROM latest_snapshots
    CROSS JOIN LATERAL jsonb_each(COALESCE(latest_snapshots.payload->'typesMap', '{}'::jsonb)) AS ut_entry
    CROSS JOIN LATERAL jsonb_each(ut_entry.value) AS season_entry
    CROSS JOIN LATERAL jsonb_array_elements(season_entry.value) AS stat_type
    WHERE stat_type.value #>> '{}' IS NOT NULL
)
INSERT INTO team_player_statistics_type (team_id, unique_tournament_id, season_id, stat_type)
SELECT DISTINCT type_rows.team_id, type_rows.unique_tournament_id, type_rows.season_id, type_rows.stat_type
FROM type_rows
JOIN team_player_statistics_season AS tps
  ON tps.team_id = type_rows.team_id
 AND tps.unique_tournament_id = type_rows.unique_tournament_id
 AND tps.season_id = type_rows.season_id
ON CONFLICT (team_id, unique_tournament_id, season_id, stat_type) DO NOTHING;
