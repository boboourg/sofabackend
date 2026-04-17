DROP TABLE IF EXISTS tennis_power;

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
