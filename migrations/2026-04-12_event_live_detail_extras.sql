CREATE TABLE IF NOT EXISTS event_comment_feed (
    event_id BIGINT PRIMARY KEY REFERENCES event(id) ON DELETE CASCADE,
    home_player_color JSONB,
    home_goalkeeper_color JSONB,
    away_player_color JSONB,
    away_goalkeeper_color JSONB
);

CREATE TABLE IF NOT EXISTS event_comment (
    event_id BIGINT NOT NULL REFERENCES event(id) ON DELETE CASCADE,
    comment_id BIGINT NOT NULL,
    sequence INTEGER,
    period_name TEXT,
    is_home BOOLEAN,
    player_id BIGINT REFERENCES player(id),
    text TEXT,
    match_time NUMERIC,
    comment_type TEXT,
    PRIMARY KEY (event_id, comment_id)
);

CREATE INDEX IF NOT EXISTS event_comment_player_idx ON event_comment(player_id);

CREATE TABLE IF NOT EXISTS event_graph (
    event_id BIGINT PRIMARY KEY REFERENCES event(id) ON DELETE CASCADE,
    period_time INTEGER,
    period_count INTEGER,
    overtime_length INTEGER
);

CREATE TABLE IF NOT EXISTS event_graph_point (
    event_id BIGINT NOT NULL REFERENCES event_graph(event_id) ON DELETE CASCADE,
    ordinal INTEGER NOT NULL,
    minute NUMERIC,
    value INTEGER,
    PRIMARY KEY (event_id, ordinal)
);

CREATE TABLE IF NOT EXISTS event_team_heatmap (
    event_id BIGINT NOT NULL REFERENCES event(id) ON DELETE CASCADE,
    team_id BIGINT NOT NULL REFERENCES team(id),
    PRIMARY KEY (event_id, team_id)
);

CREATE TABLE IF NOT EXISTS event_team_heatmap_point (
    event_id BIGINT NOT NULL,
    team_id BIGINT NOT NULL,
    point_type TEXT NOT NULL CHECK (point_type IN ('player', 'goalkeeper')),
    ordinal INTEGER NOT NULL,
    x NUMERIC,
    y NUMERIC,
    PRIMARY KEY (event_id, team_id, point_type, ordinal),
    FOREIGN KEY (event_id, team_id)
        REFERENCES event_team_heatmap(event_id, team_id)
        ON DELETE CASCADE
);
