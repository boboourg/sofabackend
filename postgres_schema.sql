BEGIN;

CREATE TABLE endpoint_registry (
    pattern TEXT PRIMARY KEY,
    path_template TEXT NOT NULL,
    query_template TEXT,
    envelope_key TEXT NOT NULL,
    target_table TEXT,
    notes TEXT
);

CREATE TABLE api_payload_snapshot (
    id BIGSERIAL PRIMARY KEY,
    endpoint_pattern TEXT NOT NULL REFERENCES endpoint_registry(pattern),
    source_url TEXT NOT NULL,
    envelope_key TEXT NOT NULL,
    context_entity_type TEXT,
    context_entity_id BIGINT,
    payload JSONB NOT NULL,
    fetched_at TIMESTAMPTZ
);

CREATE TABLE sport (
    id BIGINT PRIMARY KEY,
    slug TEXT NOT NULL UNIQUE,
    name TEXT NOT NULL
);

CREATE TABLE country (
    alpha2 TEXT PRIMARY KEY,
    alpha3 TEXT UNIQUE,
    slug TEXT UNIQUE,
    name TEXT NOT NULL
);

CREATE TABLE image_asset (
    id BIGINT PRIMARY KEY,
    md5 TEXT NOT NULL
);

CREATE TABLE category (
    id BIGINT PRIMARY KEY,
    slug TEXT NOT NULL,
    name TEXT NOT NULL,
    flag TEXT,
    alpha2 TEXT,
    priority INTEGER,
    sport_id BIGINT NOT NULL REFERENCES sport(id),
    country_alpha2 TEXT REFERENCES country(alpha2),
    field_translations JSONB
);

CREATE TABLE category_transfer_period (
    category_id BIGINT NOT NULL REFERENCES category(id),
    active_from TEXT NOT NULL,
    active_to TEXT NOT NULL,
    PRIMARY KEY (category_id, active_from, active_to)
);

CREATE TABLE category_daily_summary (
    observed_date DATE NOT NULL,
    timezone_offset_seconds INTEGER NOT NULL,
    category_id BIGINT NOT NULL REFERENCES category(id),
    total_events INTEGER,
    total_event_player_statistics INTEGER,
    total_videos INTEGER,
    PRIMARY KEY (observed_date, timezone_offset_seconds, category_id)
);

CREATE TABLE category_daily_unique_tournament (
    observed_date DATE NOT NULL,
    timezone_offset_seconds INTEGER NOT NULL,
    category_id BIGINT NOT NULL REFERENCES category(id),
    unique_tournament_id BIGINT NOT NULL,
    ordinal INTEGER NOT NULL,
    PRIMARY KEY (observed_date, timezone_offset_seconds, category_id, unique_tournament_id)
);

CREATE TABLE category_daily_team (
    observed_date DATE NOT NULL,
    timezone_offset_seconds INTEGER NOT NULL,
    category_id BIGINT NOT NULL REFERENCES category(id),
    team_id BIGINT NOT NULL,
    ordinal INTEGER NOT NULL,
    PRIMARY KEY (observed_date, timezone_offset_seconds, category_id, team_id)
);

CREATE TABLE season (
    id BIGINT PRIMARY KEY,
    name TEXT,
    year TEXT,
    editor BOOLEAN,
    season_coverage_info JSONB
);

CREATE TABLE unique_tournament (
    id BIGINT PRIMARY KEY,
    slug TEXT NOT NULL,
    name TEXT NOT NULL,
    category_id BIGINT NOT NULL REFERENCES category(id),
    country_alpha2 TEXT REFERENCES country(alpha2),
    logo_asset_id BIGINT REFERENCES image_asset(id),
    dark_logo_asset_id BIGINT REFERENCES image_asset(id),
    title_holder_team_id BIGINT,
    title_holder_titles INTEGER,
    most_titles INTEGER,
    gender TEXT,
    primary_color_hex TEXT,
    secondary_color_hex TEXT,
    start_date_timestamp BIGINT,
    end_date_timestamp BIGINT,
    tier INTEGER,
    tennis_points INTEGER,
    user_count INTEGER,
    has_rounds BOOLEAN,
    has_groups BOOLEAN,
    has_performance_graph_feature BOOLEAN,
    has_playoff_series BOOLEAN,
    has_event_player_statistics BOOLEAN,
    has_live_rating BOOLEAN,
    has_rating BOOLEAN,
    has_down_distance BOOLEAN,
    disabled_home_away_standings BOOLEAN,
    display_inverse_home_away_teams BOOLEAN,
    field_translations JSONB,
    period_length JSONB
);

CREATE TABLE unique_tournament_relation (
    unique_tournament_id BIGINT NOT NULL REFERENCES unique_tournament(id),
    related_unique_tournament_id BIGINT NOT NULL REFERENCES unique_tournament(id),
    relation_type TEXT NOT NULL CHECK (relation_type IN ('linked', 'lower_division', 'upper_division')),
    PRIMARY KEY (unique_tournament_id, related_unique_tournament_id, relation_type)
);

CREATE TABLE unique_tournament_season (
    unique_tournament_id BIGINT NOT NULL REFERENCES unique_tournament(id),
    season_id BIGINT NOT NULL REFERENCES season(id),
    PRIMARY KEY (unique_tournament_id, season_id)
);

CREATE TABLE venue (
    id BIGINT PRIMARY KEY,
    slug TEXT,
    name TEXT NOT NULL,
    capacity INTEGER,
    hidden BOOLEAN,
    country_alpha2 TEXT REFERENCES country(alpha2),
    city_name TEXT,
    stadium_name TEXT,
    stadium_capacity INTEGER,
    latitude NUMERIC,
    longitude NUMERIC,
    field_translations JSONB
);

CREATE TABLE referee (
    id BIGINT PRIMARY KEY,
    slug TEXT UNIQUE,
    name TEXT NOT NULL,
    sport_id BIGINT REFERENCES sport(id),
    country_alpha2 TEXT REFERENCES country(alpha2),
    games INTEGER,
    yellow_cards INTEGER,
    yellow_red_cards INTEGER,
    red_cards INTEGER,
    field_translations JSONB
);

CREATE TABLE tournament (
    id BIGINT PRIMARY KEY,
    slug TEXT,
    name TEXT NOT NULL,
    category_id BIGINT NOT NULL REFERENCES category(id),
    unique_tournament_id BIGINT REFERENCES unique_tournament(id),
    competition_type INTEGER,
    group_name TEXT,
    group_sign TEXT,
    is_group BOOLEAN,
    is_live BOOLEAN,
    priority INTEGER,
    field_translations JSONB
);

CREATE TABLE team (
    id BIGINT PRIMARY KEY,
    slug TEXT NOT NULL,
    name TEXT NOT NULL,
    short_name TEXT,
    full_name TEXT,
    name_code TEXT,
    sport_id BIGINT REFERENCES sport(id),
    category_id BIGINT REFERENCES category(id),
    country_alpha2 TEXT REFERENCES country(alpha2),
    manager_id BIGINT,
    venue_id BIGINT REFERENCES venue(id),
    tournament_id BIGINT REFERENCES tournament(id),
    primary_unique_tournament_id BIGINT REFERENCES unique_tournament(id),
    parent_team_id BIGINT REFERENCES team(id),
    gender TEXT,
    type INTEGER,
    class INTEGER,
    ranking INTEGER,
    national BOOLEAN,
    disabled BOOLEAN,
    foundation_date_timestamp BIGINT,
    user_count INTEGER,
    team_colors JSONB,
    field_translations JSONB,
    time_active JSONB
);

CREATE TABLE manager (
    id BIGINT PRIMARY KEY,
    slug TEXT UNIQUE,
    name TEXT NOT NULL,
    short_name TEXT,
    sport_id BIGINT REFERENCES sport(id),
    country_alpha2 TEXT REFERENCES country(alpha2),
    team_id BIGINT REFERENCES team(id),
    former_player_id BIGINT,
    nationality TEXT,
    nationality_iso2 TEXT REFERENCES country(alpha2),
    date_of_birth_timestamp BIGINT,
    deceased BOOLEAN,
    preferred_formation TEXT,
    field_translations JSONB
);

CREATE TABLE manager_performance (
    manager_id BIGINT PRIMARY KEY REFERENCES manager(id),
    total INTEGER,
    wins INTEGER,
    draws INTEGER,
    losses INTEGER,
    goals_scored INTEGER,
    goals_conceded INTEGER,
    total_points INTEGER
);

CREATE TABLE manager_team_membership (
    manager_id BIGINT NOT NULL REFERENCES manager(id),
    team_id BIGINT NOT NULL REFERENCES team(id),
    PRIMARY KEY (manager_id, team_id)
);

ALTER TABLE team
    ADD CONSTRAINT team_manager_fk
    FOREIGN KEY (manager_id) REFERENCES manager(id);

ALTER TABLE unique_tournament
    ADD CONSTRAINT unique_tournament_title_holder_team_fk
    FOREIGN KEY (title_holder_team_id) REFERENCES team(id);

CREATE TABLE unique_tournament_most_title_team (
    unique_tournament_id BIGINT NOT NULL REFERENCES unique_tournament(id),
    team_id BIGINT NOT NULL REFERENCES team(id),
    PRIMARY KEY (unique_tournament_id, team_id)
);

CREATE TABLE player (
    id BIGINT PRIMARY KEY,
    slug TEXT,
    name TEXT NOT NULL,
    short_name TEXT,
    first_name TEXT,
    last_name TEXT,
    team_id BIGINT REFERENCES team(id),
    country_alpha2 TEXT REFERENCES country(alpha2),
    manager_id BIGINT REFERENCES manager(id),
    gender TEXT,
    position TEXT,
    positions_detailed TEXT[],
    preferred_foot TEXT,
    jersey_number TEXT,
    sofascore_id TEXT,
    date_of_birth TEXT,
    date_of_birth_timestamp BIGINT,
    height INTEGER,
    weight INTEGER,
    market_value_currency TEXT,
    proposed_market_value_raw JSONB,
    rating TEXT,
    retired BOOLEAN,
    deceased BOOLEAN,
    user_count INTEGER,
    order_value INTEGER,
    field_translations JSONB
);

CREATE TABLE player_transfer_history (
    id BIGINT PRIMARY KEY,
    player_id BIGINT NOT NULL REFERENCES player(id),
    transfer_from_team_id BIGINT REFERENCES team(id),
    transfer_to_team_id BIGINT REFERENCES team(id),
    from_team_name TEXT NOT NULL,
    to_team_name TEXT NOT NULL,
    transfer_date_timestamp BIGINT NOT NULL,
    transfer_fee INTEGER NOT NULL,
    transfer_fee_description TEXT NOT NULL,
    transfer_fee_raw JSONB NOT NULL,
    type INTEGER NOT NULL
);

CREATE TABLE player_season_statistics (
    player_id BIGINT NOT NULL REFERENCES player(id),
    unique_tournament_id BIGINT NOT NULL REFERENCES unique_tournament(id),
    season_id BIGINT NOT NULL REFERENCES season(id),
    team_id BIGINT NOT NULL REFERENCES team(id),
    stat_type TEXT NOT NULL,
    statistics_id BIGINT,
    season_year TEXT,
    start_year INTEGER,
    end_year INTEGER,
    accurate_crosses INTEGER,
    accurate_crosses_percentage NUMERIC,
    accurate_long_balls INTEGER,
    accurate_long_balls_percentage NUMERIC,
    accurate_passes INTEGER,
    accurate_passes_percentage NUMERIC,
    aerial_duels_won INTEGER,
    appearances INTEGER,
    assists INTEGER,
    big_chances_created INTEGER,
    big_chances_missed INTEGER,
    blocked_shots INTEGER,
    clean_sheet INTEGER,
    count_rating INTEGER,
    dribbled_past INTEGER,
    error_lead_to_goal INTEGER,
    expected_assists NUMERIC,
    expected_goals NUMERIC,
    goals INTEGER,
    goals_assists_sum INTEGER,
    goals_conceded INTEGER,
    goals_prevented NUMERIC,
    interceptions INTEGER,
    key_passes INTEGER,
    minutes_played INTEGER,
    outfielder_blocks INTEGER,
    pass_to_assist INTEGER,
    rating NUMERIC,
    red_cards INTEGER,
    saves INTEGER,
    shots_from_inside_the_box INTEGER,
    shots_on_target INTEGER,
    successful_dribbles INTEGER,
    tackles INTEGER,
    total_cross INTEGER,
    total_long_balls INTEGER,
    total_passes INTEGER,
    total_rating NUMERIC,
    total_shots INTEGER,
    yellow_cards INTEGER,
    statistics_payload JSONB,
    PRIMARY KEY (player_id, unique_tournament_id, season_id, team_id, stat_type)
);

CREATE TABLE entity_statistics_season (
    subject_type TEXT NOT NULL CHECK (subject_type IN ('player', 'team')),
    subject_id BIGINT NOT NULL,
    unique_tournament_id BIGINT NOT NULL REFERENCES unique_tournament(id),
    season_id BIGINT NOT NULL REFERENCES season(id),
    all_time_season_id BIGINT,
    PRIMARY KEY (subject_type, subject_id, unique_tournament_id, season_id)
);

CREATE TABLE entity_statistics_type (
    subject_type TEXT NOT NULL CHECK (subject_type IN ('player', 'team')),
    subject_id BIGINT NOT NULL,
    unique_tournament_id BIGINT NOT NULL REFERENCES unique_tournament(id),
    season_id BIGINT NOT NULL REFERENCES season(id),
    stat_type TEXT NOT NULL,
    PRIMARY KEY (subject_type, subject_id, unique_tournament_id, season_id, stat_type)
);

CREATE TABLE season_statistics_type (
    subject_type TEXT NOT NULL CHECK (subject_type IN ('player', 'team')),
    unique_tournament_id BIGINT NOT NULL REFERENCES unique_tournament(id),
    season_id BIGINT NOT NULL REFERENCES season(id),
    stat_type TEXT NOT NULL,
    PRIMARY KEY (subject_type, unique_tournament_id, season_id, stat_type)
);

CREATE TABLE season_statistics_config (
    unique_tournament_id BIGINT NOT NULL REFERENCES unique_tournament(id),
    season_id BIGINT NOT NULL REFERENCES season(id),
    hide_home_and_away BOOLEAN NOT NULL,
    PRIMARY KEY (unique_tournament_id, season_id)
);

CREATE TABLE season_statistics_config_team (
    unique_tournament_id BIGINT NOT NULL,
    season_id BIGINT NOT NULL,
    team_id BIGINT NOT NULL REFERENCES team(id),
    ordinal INTEGER,
    PRIMARY KEY (unique_tournament_id, season_id, team_id),
    FOREIGN KEY (unique_tournament_id, season_id)
        REFERENCES season_statistics_config(unique_tournament_id, season_id)
);

CREATE TABLE season_statistics_nationality (
    unique_tournament_id BIGINT NOT NULL,
    season_id BIGINT NOT NULL,
    nationality_code TEXT NOT NULL,
    nationality_name TEXT NOT NULL,
    PRIMARY KEY (unique_tournament_id, season_id, nationality_code),
    FOREIGN KEY (unique_tournament_id, season_id)
        REFERENCES season_statistics_config(unique_tournament_id, season_id)
);

CREATE TABLE season_statistics_group_item (
    unique_tournament_id BIGINT NOT NULL,
    season_id BIGINT NOT NULL,
    group_scope TEXT NOT NULL CHECK (group_scope IN ('regular', 'detailed')),
    group_name TEXT NOT NULL,
    stat_field TEXT NOT NULL,
    ordinal INTEGER,
    PRIMARY KEY (unique_tournament_id, season_id, group_scope, group_name, stat_field),
    FOREIGN KEY (unique_tournament_id, season_id)
        REFERENCES season_statistics_config(unique_tournament_id, season_id)
);

CREATE TABLE season_statistics_snapshot (
    id BIGSERIAL PRIMARY KEY,
    endpoint_pattern TEXT NOT NULL REFERENCES endpoint_registry(pattern),
    unique_tournament_id BIGINT NOT NULL REFERENCES unique_tournament(id),
    season_id BIGINT NOT NULL REFERENCES season(id),
    source_url TEXT NOT NULL,
    page INTEGER,
    pages INTEGER,
    limit_value INTEGER,
    offset_value INTEGER,
    order_code TEXT,
    accumulation TEXT,
    group_code TEXT,
    fields JSONB,
    filters JSONB,
    fetched_at TIMESTAMPTZ
);

CREATE TABLE season_statistics_result (
    id BIGSERIAL PRIMARY KEY,
    snapshot_id BIGINT NOT NULL REFERENCES season_statistics_snapshot(id) ON DELETE CASCADE,
    row_number INTEGER,
    player_id BIGINT REFERENCES player(id),
    team_id BIGINT REFERENCES team(id),
    accurate_crosses INTEGER,
    accurate_crosses_percentage NUMERIC,
    accurate_final_third_passes INTEGER,
    accurate_long_balls INTEGER,
    accurate_long_balls_percentage NUMERIC,
    accurate_opposition_half_passes INTEGER,
    accurate_own_half_passes INTEGER,
    accurate_passes INTEGER,
    accurate_passes_percentage NUMERIC,
    aerial_duels_won INTEGER,
    aerial_duels_won_percentage NUMERIC,
    appearances INTEGER,
    assists NUMERIC,
    big_chances_created INTEGER,
    big_chances_missed INTEGER,
    blocked_shots NUMERIC,
    clean_sheet NUMERIC,
    clearances INTEGER,
    crosses_not_claimed INTEGER,
    dispossessed INTEGER,
    dribbled_past INTEGER,
    error_lead_to_goal INTEGER,
    error_lead_to_shot NUMERIC,
    expected_goals NUMERIC,
    fouls INTEGER,
    free_kick_goal INTEGER,
    goal_conversion_percentage NUMERIC,
    goals NUMERIC,
    goals_conceded_inside_the_box INTEGER,
    goals_conceded_outside_the_box INTEGER,
    goals_from_inside_the_box INTEGER,
    goals_from_outside_the_box NUMERIC,
    ground_duels_won INTEGER,
    ground_duels_won_percentage NUMERIC,
    headed_goals INTEGER,
    high_claims INTEGER,
    hit_woodwork NUMERIC,
    inaccurate_passes INTEGER,
    interceptions INTEGER,
    key_passes INTEGER,
    left_foot_goals INTEGER,
    matches_started INTEGER,
    minutes_played INTEGER,
    offsides INTEGER,
    outfielder_blocks INTEGER,
    own_goals INTEGER,
    pass_to_assist INTEGER,
    penalties_taken INTEGER,
    penalty_conceded INTEGER,
    penalty_conversion NUMERIC,
    penalty_faced INTEGER,
    penalty_goals INTEGER,
    penalty_save NUMERIC,
    penalty_won NUMERIC,
    possession_lost INTEGER,
    punches INTEGER,
    rating NUMERIC,
    red_cards INTEGER,
    right_foot_goals INTEGER,
    runs_out NUMERIC,
    saved_shots_from_inside_the_box NUMERIC,
    saved_shots_from_outside_the_box INTEGER,
    saves NUMERIC,
    set_piece_conversion NUMERIC,
    shot_from_set_piece INTEGER,
    shots_off_target INTEGER,
    shots_on_target INTEGER,
    successful_dribbles NUMERIC,
    successful_dribbles_percentage NUMERIC,
    successful_runs_out INTEGER,
    tackles NUMERIC,
    total_duels_won INTEGER,
    total_duels_won_percentage NUMERIC,
    total_passes INTEGER,
    total_shots INTEGER,
    was_fouled INTEGER,
    yellow_cards INTEGER,
    UNIQUE (snapshot_id, row_number)
);

CREATE TABLE top_player_snapshot (
    id BIGSERIAL PRIMARY KEY,
    endpoint_pattern TEXT NOT NULL REFERENCES endpoint_registry(pattern),
    unique_tournament_id BIGINT NOT NULL REFERENCES unique_tournament(id),
    season_id BIGINT NOT NULL REFERENCES season(id),
    source_url TEXT NOT NULL,
    statistics_type JSONB,
    fetched_at TIMESTAMPTZ
);

CREATE TABLE top_player_entry (
    snapshot_id BIGINT NOT NULL REFERENCES top_player_snapshot(id) ON DELETE CASCADE,
    metric_name TEXT NOT NULL,
    ordinal INTEGER NOT NULL,
    player_id BIGINT REFERENCES player(id),
    team_id BIGINT REFERENCES team(id),
    event_id BIGINT,
    played_enough BOOLEAN,
    statistic NUMERIC,
    statistics_id BIGINT,
    statistics_payload JSONB,
    PRIMARY KEY (snapshot_id, metric_name, ordinal)
);

CREATE TABLE top_team_snapshot (
    id BIGSERIAL PRIMARY KEY,
    endpoint_pattern TEXT NOT NULL REFERENCES endpoint_registry(pattern),
    unique_tournament_id BIGINT NOT NULL REFERENCES unique_tournament(id),
    season_id BIGINT NOT NULL REFERENCES season(id),
    source_url TEXT NOT NULL,
    fetched_at TIMESTAMPTZ
);

CREATE TABLE top_team_entry (
    snapshot_id BIGINT NOT NULL REFERENCES top_team_snapshot(id) ON DELETE CASCADE,
    metric_name TEXT NOT NULL,
    ordinal INTEGER NOT NULL,
    team_id BIGINT NOT NULL REFERENCES team(id),
    statistics_id BIGINT,
    statistics_payload JSONB,
    PRIMARY KEY (snapshot_id, metric_name, ordinal)
);

CREATE TABLE season_group (
    unique_tournament_id BIGINT NOT NULL REFERENCES unique_tournament(id),
    season_id BIGINT NOT NULL REFERENCES season(id),
    tournament_id BIGINT NOT NULL,
    group_name TEXT NOT NULL,
    PRIMARY KEY (unique_tournament_id, season_id, tournament_id)
);

CREATE TABLE season_player_of_the_season (
    unique_tournament_id BIGINT NOT NULL REFERENCES unique_tournament(id),
    season_id BIGINT NOT NULL REFERENCES season(id),
    player_id BIGINT NOT NULL REFERENCES player(id),
    team_id BIGINT REFERENCES team(id),
    player_of_the_tournament BOOLEAN,
    statistics_id BIGINT,
    statistics_payload JSONB,
    PRIMARY KEY (unique_tournament_id, season_id)
);

CREATE TABLE period (
    id BIGINT PRIMARY KEY,
    unique_tournament_id BIGINT NOT NULL REFERENCES unique_tournament(id),
    season_id BIGINT NOT NULL REFERENCES season(id),
    period_name TEXT NOT NULL,
    type TEXT NOT NULL,
    start_date_timestamp BIGINT NOT NULL,
    created_at_timestamp BIGINT NOT NULL,
    round_name TEXT,
    round_number INTEGER,
    round_slug TEXT
);

CREATE TABLE team_of_the_week (
    period_id BIGINT PRIMARY KEY REFERENCES period(id),
    formation TEXT NOT NULL
);

CREATE TABLE team_of_the_week_player (
    period_id BIGINT NOT NULL REFERENCES team_of_the_week(period_id) ON DELETE CASCADE,
    entry_id BIGINT NOT NULL,
    player_id BIGINT NOT NULL REFERENCES player(id),
    team_id BIGINT NOT NULL REFERENCES team(id),
    order_value INTEGER NOT NULL,
    rating TEXT NOT NULL,
    PRIMARY KEY (period_id, entry_id)
);

CREATE TABLE event_status (
    code INTEGER PRIMARY KEY,
    description TEXT NOT NULL,
    type TEXT NOT NULL
);

CREATE TABLE event (
    id BIGINT PRIMARY KEY,
    slug TEXT,
    custom_id TEXT,
    detail_id BIGINT,
    tournament_id BIGINT REFERENCES tournament(id),
    unique_tournament_id BIGINT REFERENCES unique_tournament(id),
    season_id BIGINT REFERENCES season(id),
    home_team_id BIGINT REFERENCES team(id),
    away_team_id BIGINT REFERENCES team(id),
    venue_id BIGINT REFERENCES venue(id),
    referee_id BIGINT REFERENCES referee(id),
    status_code INTEGER REFERENCES event_status(code),
    season_statistics_type TEXT,
    start_timestamp BIGINT,
    coverage INTEGER,
    winner_code INTEGER,
    aggregated_winner_code INTEGER,
    home_red_cards INTEGER,
    away_red_cards INTEGER,
    previous_leg_event_id BIGINT,
    cup_matches_in_round INTEGER,
    default_period_count INTEGER,
    default_period_length INTEGER,
    default_overtime_length INTEGER,
    last_period TEXT,
    correct_ai_insight BOOLEAN,
    correct_halftime_ai_insight BOOLEAN,
    feed_locked BOOLEAN,
    is_editor BOOLEAN,
    show_toto_promo BOOLEAN,
    crowdsourcing_enabled BOOLEAN,
    crowdsourcing_data_display_enabled BOOLEAN,
    final_result_only BOOLEAN,
    has_event_player_statistics BOOLEAN,
    has_event_player_heat_map BOOLEAN,
    has_global_highlights BOOLEAN,
    has_xg BOOLEAN
);

CREATE TABLE event_round_info (
    event_id BIGINT PRIMARY KEY REFERENCES event(id) ON DELETE CASCADE,
    round_number INTEGER,
    slug TEXT,
    name TEXT,
    cup_round_type INTEGER
);

CREATE TABLE event_status_time (
    event_id BIGINT PRIMARY KEY REFERENCES event(id) ON DELETE CASCADE,
    prefix TEXT,
    timestamp BIGINT,
    initial INTEGER,
    max INTEGER,
    extra INTEGER
);

CREATE TABLE event_time (
    event_id BIGINT PRIMARY KEY REFERENCES event(id) ON DELETE CASCADE,
    current_period_start_timestamp BIGINT,
    initial INTEGER,
    max INTEGER,
    extra INTEGER,
    injury_time1 INTEGER,
    injury_time2 INTEGER,
    injury_time3 INTEGER,
    injury_time4 INTEGER,
    overtime_length INTEGER,
    period_length INTEGER,
    total_period_count INTEGER
);

CREATE TABLE event_var_in_progress (
    event_id BIGINT PRIMARY KEY REFERENCES event(id) ON DELETE CASCADE,
    home_team BOOLEAN,
    away_team BOOLEAN
);

CREATE TABLE event_score (
    event_id BIGINT NOT NULL REFERENCES event(id) ON DELETE CASCADE,
    side TEXT NOT NULL CHECK (side IN ('home', 'away')),
    current INTEGER,
    display INTEGER,
    aggregated INTEGER,
    normaltime INTEGER,
    overtime INTEGER,
    penalties INTEGER,
    period1 INTEGER,
    period2 INTEGER,
    period3 INTEGER,
    period4 INTEGER,
    extra1 INTEGER,
    extra2 INTEGER,
    series INTEGER,
    PRIMARY KEY (event_id, side)
);

CREATE TABLE event_filter_value (
    event_id BIGINT NOT NULL REFERENCES event(id) ON DELETE CASCADE,
    filter_name TEXT NOT NULL,
    ordinal INTEGER NOT NULL,
    filter_value TEXT NOT NULL,
    PRIMARY KEY (event_id, filter_name, ordinal)
);

CREATE TABLE event_change_item (
    event_id BIGINT NOT NULL REFERENCES event(id) ON DELETE CASCADE,
    change_timestamp BIGINT,
    ordinal INTEGER NOT NULL,
    change_value TEXT NOT NULL,
    PRIMARY KEY (event_id, ordinal)
);

CREATE TABLE event_manager_assignment (
    event_id BIGINT NOT NULL REFERENCES event(id) ON DELETE CASCADE,
    side TEXT NOT NULL CHECK (side IN ('home', 'away')),
    manager_id BIGINT NOT NULL REFERENCES manager(id),
    PRIMARY KEY (event_id, side)
);

CREATE TABLE event_duel (
    event_id BIGINT NOT NULL REFERENCES event(id) ON DELETE CASCADE,
    duel_type TEXT NOT NULL CHECK (duel_type IN ('team', 'manager')),
    home_wins INTEGER NOT NULL,
    away_wins INTEGER NOT NULL,
    draws INTEGER NOT NULL,
    PRIMARY KEY (event_id, duel_type)
);

CREATE TABLE event_pregame_form (
    event_id BIGINT PRIMARY KEY REFERENCES event(id) ON DELETE CASCADE,
    label TEXT
);

CREATE TABLE event_pregame_form_side (
    event_id BIGINT NOT NULL REFERENCES event_pregame_form(event_id) ON DELETE CASCADE,
    side TEXT NOT NULL CHECK (side IN ('home', 'away')),
    avg_rating TEXT,
    position INTEGER,
    value TEXT,
    PRIMARY KEY (event_id, side)
);

CREATE TABLE event_pregame_form_item (
    event_id BIGINT NOT NULL,
    side TEXT NOT NULL CHECK (side IN ('home', 'away')),
    ordinal INTEGER NOT NULL,
    form_value TEXT NOT NULL,
    PRIMARY KEY (event_id, side, ordinal),
    FOREIGN KEY (event_id, side)
        REFERENCES event_pregame_form_side(event_id, side)
        ON DELETE CASCADE
);

CREATE TABLE event_vote_option (
    event_id BIGINT NOT NULL REFERENCES event(id) ON DELETE CASCADE,
    vote_type TEXT NOT NULL,
    option_name TEXT NOT NULL,
    vote_count INTEGER NOT NULL,
    PRIMARY KEY (event_id, vote_type, option_name)
);

CREATE TABLE event_comment_feed (
    event_id BIGINT PRIMARY KEY REFERENCES event(id) ON DELETE CASCADE,
    home_player_color JSONB,
    home_goalkeeper_color JSONB,
    away_player_color JSONB,
    away_goalkeeper_color JSONB
);

CREATE TABLE event_comment (
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

CREATE INDEX event_comment_player_idx ON event_comment(player_id);

CREATE TABLE event_graph (
    event_id BIGINT PRIMARY KEY REFERENCES event(id) ON DELETE CASCADE,
    period_time INTEGER,
    period_count INTEGER,
    overtime_length INTEGER
);

CREATE TABLE event_graph_point (
    event_id BIGINT NOT NULL REFERENCES event_graph(event_id) ON DELETE CASCADE,
    ordinal INTEGER NOT NULL,
    minute NUMERIC,
    value INTEGER,
    PRIMARY KEY (event_id, ordinal)
);

CREATE TABLE event_team_heatmap (
    event_id BIGINT NOT NULL REFERENCES event(id) ON DELETE CASCADE,
    team_id BIGINT NOT NULL REFERENCES team(id),
    PRIMARY KEY (event_id, team_id)
);

CREATE TABLE event_team_heatmap_point (
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

CREATE TABLE provider (
    id BIGINT PRIMARY KEY,
    slug TEXT UNIQUE,
    name TEXT,
    country TEXT,
    default_bet_slip_link TEXT,
    colors JSONB,
    odds_from_provider_id BIGINT REFERENCES provider(id),
    live_odds_from_provider_id BIGINT REFERENCES provider(id)
);

CREATE TABLE provider_configuration (
    id BIGINT PRIMARY KEY,
    campaign_id BIGINT,
    provider_id BIGINT NOT NULL REFERENCES provider(id),
    fallback_provider_id BIGINT REFERENCES provider(id),
    type TEXT,
    weight INTEGER,
    branded BOOLEAN,
    featured_odds_type TEXT,
    bet_slip_link TEXT,
    default_bet_slip_link TEXT,
    impression_cost_encrypted TEXT
);

CREATE TABLE event_market (
    id BIGINT PRIMARY KEY,
    event_id BIGINT NOT NULL REFERENCES event(id) ON DELETE CASCADE,
    provider_id BIGINT REFERENCES provider(id),
    fid BIGINT NOT NULL,
    market_id BIGINT NOT NULL,
    source_id BIGINT,
    market_group TEXT NOT NULL,
    market_name TEXT NOT NULL,
    market_period TEXT NOT NULL,
    structure_type INTEGER NOT NULL,
    choice_group TEXT,
    is_live BOOLEAN NOT NULL,
    suspended BOOLEAN NOT NULL
);

CREATE TABLE event_market_choice (
    source_id BIGINT PRIMARY KEY,
    event_market_id BIGINT NOT NULL REFERENCES event_market(id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    change_value INTEGER NOT NULL,
    fractional_value TEXT NOT NULL,
    initial_fractional_value TEXT NOT NULL
);

CREATE TABLE event_winning_odds (
    event_id BIGINT NOT NULL REFERENCES event(id) ON DELETE CASCADE,
    provider_id BIGINT NOT NULL REFERENCES provider(id),
    side TEXT NOT NULL CHECK (side IN ('home', 'away')),
    odds_id BIGINT,
    actual INTEGER,
    expected INTEGER,
    fractional_value TEXT,
    PRIMARY KEY (event_id, provider_id, side)
);

CREATE TABLE event_lineup (
    event_id BIGINT NOT NULL REFERENCES event(id) ON DELETE CASCADE,
    side TEXT NOT NULL CHECK (side IN ('home', 'away')),
    formation TEXT,
    player_color JSONB,
    goalkeeper_color JSONB,
    support_staff JSONB,
    PRIMARY KEY (event_id, side)
);

CREATE TABLE event_lineup_player (
    event_id BIGINT NOT NULL,
    side TEXT NOT NULL CHECK (side IN ('home', 'away')),
    player_id BIGINT NOT NULL REFERENCES player(id),
    team_id BIGINT REFERENCES team(id),
    position TEXT,
    substitute BOOLEAN,
    shirt_number INTEGER,
    jersey_number TEXT,
    avg_rating NUMERIC,
    PRIMARY KEY (event_id, side, player_id),
    FOREIGN KEY (event_id, side)
        REFERENCES event_lineup(event_id, side)
        ON DELETE CASCADE
);

CREATE TABLE event_lineup_missing_player (
    event_id BIGINT NOT NULL,
    side TEXT NOT NULL CHECK (side IN ('home', 'away')),
    player_id BIGINT NOT NULL REFERENCES player(id),
    description TEXT,
    expected_end_date TEXT,
    external_type INTEGER,
    reason INTEGER,
    type TEXT,
    PRIMARY KEY (event_id, side, player_id),
    FOREIGN KEY (event_id, side)
        REFERENCES event_lineup(event_id, side)
        ON DELETE CASCADE
);

CREATE TABLE standing_promotion (
    id BIGINT PRIMARY KEY,
    text TEXT NOT NULL
);

CREATE TABLE standing_tie_breaking_rule (
    id BIGINT PRIMARY KEY,
    text TEXT NOT NULL
);

CREATE TABLE standing (
    id BIGINT PRIMARY KEY,
    season_id BIGINT NOT NULL REFERENCES season(id),
    tournament_id BIGINT REFERENCES tournament(id),
    name TEXT NOT NULL,
    type TEXT NOT NULL,
    updated_at_timestamp BIGINT,
    tie_breaking_rule_id BIGINT REFERENCES standing_tie_breaking_rule(id),
    descriptions JSONB
);

CREATE TABLE standing_row (
    id BIGINT PRIMARY KEY,
    standing_id BIGINT NOT NULL REFERENCES standing(id) ON DELETE CASCADE,
    team_id BIGINT NOT NULL REFERENCES team(id),
    position INTEGER NOT NULL,
    matches INTEGER NOT NULL,
    wins INTEGER NOT NULL,
    draws INTEGER NOT NULL,
    losses INTEGER NOT NULL,
    points INTEGER NOT NULL,
    scores_for INTEGER NOT NULL,
    scores_against INTEGER NOT NULL,
    score_diff_formatted TEXT NOT NULL,
    promotion_id BIGINT REFERENCES standing_promotion(id),
    descriptions JSONB
);

CREATE TABLE tournament_team_event_snapshot (
    id BIGSERIAL PRIMARY KEY,
    endpoint_pattern TEXT NOT NULL REFERENCES endpoint_registry(pattern),
    unique_tournament_id BIGINT NOT NULL REFERENCES unique_tournament(id),
    season_id BIGINT NOT NULL REFERENCES season(id),
    scope TEXT NOT NULL CHECK (scope IN ('home', 'away', 'total')),
    source_url TEXT NOT NULL,
    fetched_at TIMESTAMPTZ
);

CREATE TABLE tournament_team_event_bucket (
    snapshot_id BIGINT NOT NULL REFERENCES tournament_team_event_snapshot(id) ON DELETE CASCADE,
    level_1_key TEXT NOT NULL,
    level_2_key TEXT NOT NULL,
    event_id BIGINT NOT NULL REFERENCES event(id),
    ordinal INTEGER,
    PRIMARY KEY (snapshot_id, level_1_key, level_2_key, event_id)
);

ALTER TABLE top_player_entry
    ADD CONSTRAINT top_player_entry_event_fk
    FOREIGN KEY (event_id) REFERENCES event(id);

CREATE INDEX idx_api_payload_snapshot_pattern ON api_payload_snapshot(endpoint_pattern);
CREATE INDEX idx_category_slug ON category(slug);
CREATE INDEX idx_category_sport_id ON category(sport_id);
CREATE INDEX idx_category_country_alpha2 ON category(country_alpha2);
CREATE INDEX idx_category_daily_unique_tournament_utid ON category_daily_unique_tournament(unique_tournament_id);
CREATE INDEX idx_category_daily_team_team_id ON category_daily_team(team_id);
CREATE INDEX idx_unique_tournament_category_id ON unique_tournament(category_id);
CREATE INDEX idx_unique_tournament_country_alpha2 ON unique_tournament(country_alpha2);
CREATE INDEX idx_unique_tournament_slug ON unique_tournament(slug);
CREATE INDEX idx_unique_tournament_season_season_id ON unique_tournament_season(season_id);
CREATE INDEX idx_tournament_slug ON tournament(slug);
CREATE INDEX idx_team_slug ON team(slug);
CREATE INDEX idx_team_category_id ON team(category_id);
CREATE INDEX idx_team_country_alpha2 ON team(country_alpha2);
CREATE INDEX idx_team_primary_unique_tournament_id ON team(primary_unique_tournament_id);
CREATE INDEX idx_player_team_id ON player(team_id);
CREATE INDEX idx_player_country_alpha2 ON player(country_alpha2);
CREATE INDEX idx_player_manager_id ON player(manager_id);
CREATE INDEX idx_player_season_statistics_player ON player_season_statistics(player_id);
CREATE INDEX idx_player_season_statistics_ut_season ON player_season_statistics(unique_tournament_id, season_id);
CREATE INDEX idx_event_tournament_id ON event(tournament_id);
CREATE INDEX idx_event_season_id ON event(season_id);
CREATE INDEX idx_event_unique_tournament_id ON event(unique_tournament_id);
CREATE INDEX idx_event_start_timestamp ON event(start_timestamp);
CREATE INDEX idx_event_home_team_id ON event(home_team_id);
CREATE INDEX idx_event_away_team_id ON event(away_team_id);
CREATE INDEX idx_event_market_event_id ON event_market(event_id);
CREATE INDEX idx_standing_season_id ON standing(season_id);
CREATE INDEX idx_standing_tournament_id ON standing(tournament_id);
CREATE INDEX idx_standing_row_standing_id ON standing_row(standing_id);
CREATE INDEX idx_standing_row_team_id ON standing_row(team_id);
CREATE INDEX idx_season_statistics_snapshot_ut_season ON season_statistics_snapshot(unique_tournament_id, season_id);
CREATE INDEX idx_season_statistics_result_snapshot ON season_statistics_result(snapshot_id);
CREATE INDEX idx_season_statistics_result_player ON season_statistics_result(player_id);
CREATE INDEX idx_season_statistics_result_team ON season_statistics_result(team_id);
CREATE INDEX idx_top_player_snapshot_ut_season ON top_player_snapshot(unique_tournament_id, season_id);
CREATE INDEX idx_top_player_entry_player ON top_player_entry(player_id);
CREATE INDEX idx_top_player_entry_team ON top_player_entry(team_id);
CREATE INDEX idx_top_team_snapshot_ut_season ON top_team_snapshot(unique_tournament_id, season_id);
CREATE INDEX idx_top_team_entry_team ON top_team_entry(team_id);
CREATE INDEX idx_tournament_team_event_snapshot_ut_season ON tournament_team_event_snapshot(unique_tournament_id, season_id);

COMMIT;
