CREATE TABLE IF NOT EXISTS event_best_player_entry (
    event_id BIGINT NOT NULL REFERENCES event(id) ON DELETE CASCADE,
    bucket TEXT NOT NULL,
    ordinal INTEGER NOT NULL,
    player_id BIGINT REFERENCES player(id),
    label TEXT,
    value_text TEXT,
    value_numeric NUMERIC,
    is_player_of_the_match BOOLEAN,
    PRIMARY KEY (event_id, bucket, ordinal)
);

CREATE INDEX IF NOT EXISTS event_best_player_entry_player_idx
    ON event_best_player_entry(player_id);

CREATE TABLE IF NOT EXISTS event_player_statistics (
    event_id BIGINT NOT NULL REFERENCES event(id) ON DELETE CASCADE,
    player_id BIGINT NOT NULL REFERENCES player(id),
    team_id BIGINT REFERENCES team(id),
    position TEXT,
    rating NUMERIC,
    rating_original NUMERIC,
    rating_alternative NUMERIC,
    statistics_type TEXT,
    sport_slug TEXT,
    extra_json JSONB,
    PRIMARY KEY (event_id, player_id)
);

CREATE INDEX IF NOT EXISTS event_player_statistics_team_idx
    ON event_player_statistics(team_id);

CREATE TABLE IF NOT EXISTS event_player_stat_value (
    event_id BIGINT NOT NULL,
    player_id BIGINT NOT NULL,
    stat_name TEXT NOT NULL,
    stat_value_numeric NUMERIC,
    stat_value_text TEXT,
    stat_value_json JSONB,
    PRIMARY KEY (event_id, player_id, stat_name),
    FOREIGN KEY (event_id, player_id)
        REFERENCES event_player_statistics(event_id, player_id)
        ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS event_player_rating_breakdown_action (
    event_id BIGINT NOT NULL REFERENCES event(id) ON DELETE CASCADE,
    player_id BIGINT NOT NULL REFERENCES player(id),
    action_group TEXT NOT NULL,
    ordinal INTEGER NOT NULL,
    event_action_type TEXT,
    is_home BOOLEAN,
    keypass BOOLEAN,
    outcome BOOLEAN,
    start_x NUMERIC,
    start_y NUMERIC,
    end_x NUMERIC,
    end_y NUMERIC,
    PRIMARY KEY (event_id, player_id, action_group, ordinal)
);

CREATE INDEX IF NOT EXISTS event_player_rating_breakdown_player_idx
    ON event_player_rating_breakdown_action(player_id);
