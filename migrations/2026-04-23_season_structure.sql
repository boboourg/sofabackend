BEGIN;

CREATE TABLE IF NOT EXISTS season_round (
    unique_tournament_id BIGINT NOT NULL REFERENCES unique_tournament(id),
    season_id BIGINT NOT NULL REFERENCES season(id),
    round_number INTEGER NOT NULL,
    round_name TEXT,
    round_slug TEXT,
    is_current BOOLEAN NOT NULL DEFAULT FALSE,
    PRIMARY KEY (unique_tournament_id, season_id, round_number)
);

CREATE TABLE IF NOT EXISTS season_cup_tree (
    cup_tree_id BIGINT PRIMARY KEY,
    unique_tournament_id BIGINT NOT NULL REFERENCES unique_tournament(id),
    season_id BIGINT NOT NULL REFERENCES season(id),
    tournament_id BIGINT REFERENCES tournament(id),
    name TEXT,
    current_round INTEGER
);

CREATE TABLE IF NOT EXISTS season_cup_tree_round (
    cup_tree_id BIGINT NOT NULL REFERENCES season_cup_tree(cup_tree_id) ON DELETE CASCADE,
    round_order INTEGER NOT NULL,
    round_type INTEGER,
    description TEXT,
    PRIMARY KEY (cup_tree_id, round_order)
);

CREATE TABLE IF NOT EXISTS season_cup_tree_block (
    entry_id BIGINT PRIMARY KEY,
    cup_tree_id BIGINT NOT NULL REFERENCES season_cup_tree(cup_tree_id) ON DELETE CASCADE,
    round_order INTEGER NOT NULL,
    block_id BIGINT,
    block_order INTEGER,
    finished BOOLEAN,
    matches_in_round INTEGER,
    result TEXT,
    home_team_score TEXT,
    away_team_score TEXT,
    has_next_round_link BOOLEAN,
    series_start_date_timestamp BIGINT,
    automatic_progression BOOLEAN,
    event_ids_json JSONB NOT NULL DEFAULT '[]'::jsonb
);

CREATE TABLE IF NOT EXISTS season_cup_tree_participant (
    participant_id BIGINT PRIMARY KEY,
    entry_id BIGINT NOT NULL REFERENCES season_cup_tree_block(entry_id) ON DELETE CASCADE,
    team_id BIGINT REFERENCES team(id),
    order_value INTEGER,
    winner BOOLEAN
);

CREATE INDEX IF NOT EXISTS idx_season_round_ut_season
    ON season_round (unique_tournament_id, season_id);

CREATE INDEX IF NOT EXISTS idx_season_cup_tree_ut_season
    ON season_cup_tree (unique_tournament_id, season_id);

CREATE INDEX IF NOT EXISTS idx_season_cup_tree_round_cup_tree
    ON season_cup_tree_round (cup_tree_id, round_order);

CREATE INDEX IF NOT EXISTS idx_season_cup_tree_block_cup_tree
    ON season_cup_tree_block (cup_tree_id, round_order, block_order);

CREATE INDEX IF NOT EXISTS idx_season_cup_tree_participant_entry
    ON season_cup_tree_participant (entry_id);

COMMIT;
