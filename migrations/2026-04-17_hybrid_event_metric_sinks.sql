CREATE TABLE IF NOT EXISTS event_statistic (
    event_id BIGINT NOT NULL REFERENCES event(id) ON DELETE CASCADE,
    period TEXT NOT NULL,
    group_name TEXT NOT NULL,
    stat_name TEXT NOT NULL,
    home_value_numeric NUMERIC,
    home_value_text TEXT,
    home_value_json JSONB,
    away_value_numeric NUMERIC,
    away_value_text TEXT,
    away_value_json JSONB,
    compare_code TEXT,
    statistics_type TEXT,
    PRIMARY KEY (event_id, period, group_name, stat_name)
);

CREATE TABLE IF NOT EXISTS event_incident (
    event_id BIGINT NOT NULL REFERENCES event(id) ON DELETE CASCADE,
    ordinal INTEGER NOT NULL,
    incident_id BIGINT,
    incident_type TEXT,
    minute INTEGER,
    home_score_text TEXT,
    away_score_text TEXT,
    text_value TEXT,
    PRIMARY KEY (event_id, ordinal)
);

CREATE TABLE IF NOT EXISTS tennis_point_by_point (
    event_id BIGINT NOT NULL REFERENCES event(id) ON DELETE CASCADE,
    ordinal INTEGER NOT NULL,
    point_id BIGINT,
    set_number INTEGER,
    game_number INTEGER,
    server TEXT,
    home_score TEXT,
    away_score TEXT,
    PRIMARY KEY (event_id, ordinal)
);

CREATE TABLE IF NOT EXISTS tennis_power (
    event_id BIGINT NOT NULL REFERENCES event(id) ON DELETE CASCADE,
    ordinal INTEGER NOT NULL,
    set_number INTEGER,
    game_number INTEGER,
    value_numeric NUMERIC,
    value_text TEXT,
    break_occurred BOOLEAN,
    PRIMARY KEY (event_id, ordinal)
);

CREATE TABLE IF NOT EXISTS baseball_inning (
    event_id BIGINT NOT NULL REFERENCES event(id) ON DELETE CASCADE,
    ordinal INTEGER NOT NULL,
    inning INTEGER,
    home_score INTEGER,
    away_score INTEGER,
    PRIMARY KEY (event_id, ordinal)
);

CREATE TABLE IF NOT EXISTS shotmap_point (
    event_id BIGINT NOT NULL REFERENCES event(id) ON DELETE CASCADE,
    ordinal INTEGER NOT NULL,
    x NUMERIC,
    y NUMERIC,
    shot_type TEXT,
    PRIMARY KEY (event_id, ordinal)
);

CREATE TABLE IF NOT EXISTS esports_game (
    event_id BIGINT NOT NULL REFERENCES event(id) ON DELETE CASCADE,
    ordinal INTEGER NOT NULL,
    game_id BIGINT,
    status TEXT,
    map_name TEXT,
    PRIMARY KEY (event_id, ordinal)
);
