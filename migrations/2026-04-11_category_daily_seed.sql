BEGIN;

CREATE TABLE IF NOT EXISTS category_daily_summary (
    observed_date DATE NOT NULL,
    timezone_offset_seconds INTEGER NOT NULL,
    category_id BIGINT NOT NULL REFERENCES category(id),
    total_events INTEGER,
    total_event_player_statistics INTEGER,
    total_videos INTEGER,
    PRIMARY KEY (observed_date, timezone_offset_seconds, category_id)
);

CREATE TABLE IF NOT EXISTS category_daily_unique_tournament (
    observed_date DATE NOT NULL,
    timezone_offset_seconds INTEGER NOT NULL,
    category_id BIGINT NOT NULL REFERENCES category(id),
    unique_tournament_id BIGINT NOT NULL,
    ordinal INTEGER NOT NULL,
    PRIMARY KEY (observed_date, timezone_offset_seconds, category_id, unique_tournament_id)
);

CREATE TABLE IF NOT EXISTS category_daily_team (
    observed_date DATE NOT NULL,
    timezone_offset_seconds INTEGER NOT NULL,
    category_id BIGINT NOT NULL REFERENCES category(id),
    team_id BIGINT NOT NULL,
    ordinal INTEGER NOT NULL,
    PRIMARY KEY (observed_date, timezone_offset_seconds, category_id, team_id)
);

CREATE INDEX IF NOT EXISTS idx_category_daily_unique_tournament_utid
    ON category_daily_unique_tournament(unique_tournament_id);

CREATE INDEX IF NOT EXISTS idx_category_daily_team_team_id
    ON category_daily_team(team_id);

COMMIT;
