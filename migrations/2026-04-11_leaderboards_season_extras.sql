CREATE TABLE IF NOT EXISTS season_group (
    unique_tournament_id BIGINT NOT NULL REFERENCES unique_tournament(id),
    season_id BIGINT NOT NULL REFERENCES season(id),
    tournament_id BIGINT NOT NULL,
    group_name TEXT NOT NULL,
    PRIMARY KEY (unique_tournament_id, season_id, tournament_id)
);

CREATE TABLE IF NOT EXISTS season_player_of_the_season (
    unique_tournament_id BIGINT NOT NULL REFERENCES unique_tournament(id),
    season_id BIGINT NOT NULL REFERENCES season(id),
    player_id BIGINT NOT NULL REFERENCES player(id),
    team_id BIGINT REFERENCES team(id),
    player_of_the_tournament BOOLEAN,
    statistics_id BIGINT,
    statistics_payload JSONB,
    PRIMARY KEY (unique_tournament_id, season_id)
);
