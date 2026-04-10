# Sofascore PostgreSQL Schema Overview

## Scope

This schema is built from the local `reports/*.md` corpus, not from guessed API behavior.
Only fields that were confirmed by the saved schema reports are flattened into columns.
When a response family is clearly dynamic or map-like, it is preserved in a scoped `JSONB`
snapshot table instead of being force-flattened.

## Core relationship graph

- `sport` is the root dictionary. `category` references `sport`.
- `country` is reused by `category`, `team`, `player`, `manager`, `venue`, and `referee`.
- `category` is the parent for `unique_tournament` and for club/national `team`.
- `unique_tournament` is the stable competition identity. It has optional title-holder and
  most-title team links back to `team`.
- `season` is stored once and linked to competitions through `unique_tournament_season`.
- `tournament` is the operational season instance used by `event` and `standing`.
  It references both `category` and `unique_tournament`.
- `team` is the reusable team dictionary. It can point to its current `manager`, `venue`,
  `tournament`, `primary_unique_tournament`, parent team, `sport`, `category`, and `country`.
- `manager` is its own dictionary and also has a historical many-to-many link to `team`
  through `manager_team_membership`.
- `player` is its own dictionary and references current `team`, `country`, and optional
  `manager`.
- `event` is the match fact table. It references `tournament`, `season`, `unique_tournament`,
  `home_team`, `away_team`, `venue`, `referee`, and `event_status`.

## Event-side children

- `event_score` stores `homeScore` and `awayScore` as one normalized table with `side`.
- `event_round_info`, `event_time`, `event_status_time`, and `event_var_in_progress`
  are one-to-one children of `event`.
- `event_filter_value` and `event_change_item` preserve repeating arrays from `eventFilters`
  and `changes`.
- `event_manager_assignment` stores the explicit event-day home/away managers.
- `event_duel` stores `teamDuel` and `managerDuel`.
- `event_pregame_form`, `event_pregame_form_side`, and `event_pregame_form_item`
  preserve the `pregame-form` response.
- `event_vote_option` flattens all vote envelopes into one table with `vote_type` and
  `option_name`.
- `event_lineup`, `event_lineup_player`, and `event_lineup_missing_player`
  model `lineups`.
- `event_market`, `event_market_choice`, and `event_winning_odds`
  model `odds/.../all` and `provider/.../winning-odds`.

## Competition analytics

- `standing` belongs to a `season` and a `tournament`.
- `standing_row` belongs to one `standing` and one `team`.
- `standing_promotion` and `standing_tie_breaking_rule` are shared dictionaries.

## Season statistics family

- `season_statistics_config` is the parent record for `/statistics/info`.
- `season_statistics_config_team`, `season_statistics_nationality`, and
  `season_statistics_group_item` normalize the real payload of `/statistics/info`.
- `season_statistics_snapshot` stores the exact query variant for `/statistics?...`:
  path pattern, full URL, pagination, order, accumulation, group, fields, and filters.
- `season_statistics_result` stores the flat rows from `results[]` and references
  `player` and `team`.

## Season leaderboards

- `top_player_snapshot` stores one response from leaderboards that return `topPlayers`.
- `top_player_entry` stores every metric bucket entry, for example `rating`, `goals`,
  `expectedGoals`, `assists`, and similar arrays inside `topPlayers`.
- `top_team_snapshot` and `top_team_entry` do the same for `topTeams`.

The leaderboard payloads contain a stable outer shape but the nested `statistics` objects
change by metric, so the metric-specific statistics block stays in `JSONB`.

## Team-of-the-week and transfer history

- `period` stores `/team-of-the-week/periods`.
- `team_of_the_week` stores the chosen formation for a specific `period`.
- `team_of_the_week_player` stores the selected players and ratings.
- `player_transfer_history` stores `transferHistory[]` with explicit links to
  source and destination `team`.

## Entity-season support tables

- `entity_statistics_season` stores the `uniqueTournamentSeasons[]` relation returned by
  player/team statistics-season endpoints.
- `entity_statistics_type` stores `typesMap` rows for player/team statistics-season endpoints.
- `season_statistics_type` stores the competition-level `types[]` values from
  `/player-statistics/types` and `/team-statistics/types`.

## Provider and transport registry

- `provider` is the reusable odds provider dictionary.
- `provider_configuration` stores the region/type-specific provider wrapper returned by
  `/odds/providers/...`.
- `endpoint_registry` stores exact Sofascore path patterns and the target table family.

## Why `api_payload_snapshot` exists

The report corpus also includes envelopes such as:

- `content`
- `mediaItems`
- `averageAttributeOverviews`
- `playerAttributeOverviews`
- `shotmap`
- `highlights`
- `oddsMap`
- `winningOddsMap`
- `managedTeamMap`
- `h2hMap`
- `statisticsMap`
- `incidentsMap`
- `general`
- `head2head`

These families are real, but the local corpus confirms them mostly as document or map payloads,
not as one stable relational shape. For those cases the schema keeps a controlled system table:
`api_payload_snapshot(endpoint_pattern, envelope_key, payload)`.

That lets the future parser keep exact Sofascore payloads without inventing fake relational
columns, while still keeping the business core in 3NF.
