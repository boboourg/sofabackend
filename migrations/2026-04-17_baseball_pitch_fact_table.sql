BEGIN;

CREATE TABLE IF NOT EXISTS baseball_pitch (
    event_id BIGINT NOT NULL REFERENCES event(id) ON DELETE CASCADE,
    at_bat_id BIGINT NOT NULL,
    ordinal INTEGER NOT NULL,
    pitch_id BIGINT,
    pitch_speed NUMERIC,
    pitch_type TEXT,
    pitch_zone TEXT,
    pitch_x NUMERIC,
    pitch_y NUMERIC,
    mlb_x NUMERIC,
    mlb_y NUMERIC,
    outcome TEXT,
    pitcher_id BIGINT REFERENCES player(id),
    hitter_id BIGINT REFERENCES player(id),
    PRIMARY KEY (event_id, at_bat_id, ordinal)
);

COMMIT;
