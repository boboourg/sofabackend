# Sofascore API / DB Integration Docs

## 1. Scope And Source Of Truth

This document is generated from the current repository implementation, not from assumptions about Sofascore.

Primary source files:

- `schema_inspector/endpoints.py`
- `schema_inspector/*_cli.py`
- `schema_inspector/*_job.py`
- `schema_inspector/*_backfill_job.py`
- `schema_inspector/*_repository.py`
- `postgres_schema.sql`

Current integration rules reflected by the code:

- Base URL: `https://www.sofascore.com`
- All HTTP calls must go through the existing transport/client layer.
- Exact endpoint templates are registered in `endpoint_registry`.
- Raw responses are persisted in `api_payload_snapshot`.
- Normalized tables are filled by parser/repository families.
- Backfill jobs dedupe mostly through `api_payload_snapshot.source_url` or `context_entity_*`.

Important implementation note:

- `endpoint_registry.target_table` is the canonical "primary target" declared by the endpoint registry.
- Some endpoints still hydrate additional dimension tables during parsing even when the primary target is `api_payload_snapshot`.

## 2. Runtime Entry Points

| Script | Module | What it does | Main selectors |
| --- | --- | --- | --- |
| `load_categories_seed.py` | `schema_inspector.categories_seed_cli` | Loads daily football category discovery seeds | `date`, timezone offset or timezone name |
| `load_bootstrap_pipeline.py` | `schema_inspector.bootstrap_pipeline_cli` | Starts the initial pipeline from categories, then hydrates competitions and event lists | date list/range, timezone, competition limit/concurrency, optional live |
| `load_competition.py` | `schema_inspector.competition_cli` | Loads competition family | `unique_tournament_id`, optional `season_id` |
| `load_event_list.py` | `schema_inspector.event_list_cli` | Loads list-style event feeds | `scheduled`, `live`, `featured`, `round` |
| `load_event_detail.py` | `schema_inspector.event_detail_cli` | Loads event detail + odds + lineups | `event_id`, repeatable `provider_id` |
| `load_standings.py` | `schema_inspector.standings_cli` | Loads standings by unique tournament or tournament | `unique_tournament_id` or `tournament_id`, `season_id`, repeatable `scope` |
| `load_standings_backfill.py` | `schema_inspector.standings_backfill_cli` | Backfills standings from PostgreSQL season seeds | limits / offsets over `event` season seeds, repeatable `scope` |
| `load_statistics.py` | `schema_inspector.statistics_cli` | Loads season statistics config and paged snapshots | `unique_tournament_id`, `season_id`, optional exact query params |
| `load_statistics_backfill.py` | `schema_inspector.statistics_backfill_cli` | Backfills statistics from PostgreSQL season seeds | limits / offsets over distinct `(unique_tournament_id, season_id)` |
| `load_entities.py` | `schema_inspector.entities_cli` | Loads team/player enrichment endpoints | `player_id`, `team_id`, request triplets |
| `load_leaderboards.py` | `schema_inspector.leaderboards_cli` | Loads seasonal leaderboard endpoints | `unique_tournament_id`, `season_id`, optional team ids / period ids / scopes |
| `load_entities_backfill.py` | `schema_inspector.entities_backfill_cli` | Backfills entities from PostgreSQL seeds already present | limits / offsets over `player`, `team`, `season_statistics_result`, `event` |
| `load_leaderboards_backfill.py` | `schema_inspector.leaderboards_backfill_cli` | Backfills leaderboards from PostgreSQL season seeds | season pairs from `event`, team ids per season from `event` |
| `load_full_backfill.py` | `schema_inspector.full_backfill_cli` | Orchestrates event detail + statistics + entities + standings + leaderboards backfills | combined limits, offsets, concurrency, provider ids |
| `load_targeted_pipeline.py` | `schema_inspector.targeted_pipeline_cli` | Fast smoke-loader for one unique tournament / season slice plus selected team, player, and event ids | `unique_tournament_id`, `season_id`, repeatable `team_id`, `player_id`, `event_id` |

Important orchestration detail:

- `load_bootstrap_pipeline.py` currently orchestrates:
  - `categories_seed`
  - `competition`
  - `event_list` scheduled feeds
  - optional `event_list` live feed
- `load_full_backfill.py` currently orchestrates:
  - `event_detail_backfill`
  - `statistics_backfill`
  - `entities_backfill`
  - `standings_backfill`
  - `leaderboards_backfill`
- It does **not** directly run:
  - competition
  - event_list

## 3. URL Pattern Grammar

### 3.1 Base Pattern

All concrete URLs are built as:

```text
https://www.sofascore.com + path_template + optional_query_string
```

Examples:

- `https://www.sofascore.com/api/v1/event/12437786`
- `https://www.sofascore.com/api/v1/unique-tournament/17/season/61644/statistics?limit=20&offset=0&order=-rating`

### 3.2 Path Parameters

| Parameter | Meaning in current code |
| --- | --- |
| `date` | Date string for scheduled events, expected as `YYYY-MM-DD` |
| `event_id` | Sofascore event id |
| `period_id` | Team-of-the-week period id |
| `player_id` | Sofascore player id |
| `provider_id` | Odds provider id, default in CLI/backfill is `1` |
| `round_number` | Tournament round number |
| `scope` | Used in standings and team-events routes; current loaders default to `total` for standings and `home, away, total` for team-events |
| `season_id` | Sofascore season id |
| `team_id` | Sofascore team id |
| `tournament_id` | Sofascore tournament id |
| `timezone_offset_seconds` | Timezone offset encoded directly into the daily categories path, e.g. `10800` for UTC+3 |
| `unique_tournament_id` | Sofascore unique tournament id |

### 3.3 Statistics Query Parameters

Only one family currently uses dynamic query strings in the endpoint registry:

`/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/statistics`

Supported query fields:

| Query param | How it is passed | How it is stored |
| --- | --- | --- |
| `limit` | `--limit` | `season_statistics_snapshot.limit_value` |
| `offset` | `--offset` | `season_statistics_snapshot.offset_value` |
| `order` | `--order` | `season_statistics_snapshot.order_code` |
| `accumulation` | `--accumulation` | `season_statistics_snapshot.accumulation` |
| `group` | `--group` | `season_statistics_snapshot.group_code` |
| `fields` | repeatable `--field`, joined by comma | `season_statistics_snapshot.fields` as JSONB |
| `filters` | repeatable `--filter`, joined by comma | `season_statistics_snapshot.filters` as parsed JSONB |

Filter parsing rule in current code:

- Raw filter expression is split as `field.operator.value`.
- Multiple filter values inside one expression are split by `~`.
- Parsed representation is stored with:
  - `expression`
  - `field`
  - `operator`
  - `raw_value`
  - `values`

## 4. Endpoint Catalog

### 4.1 Categories Seed Family

Entrypoints:

- `load_categories_seed.py`
- `load_bootstrap_pipeline.py`

Normalized write scope:

- `endpoint_registry`
- `api_payload_snapshot`
- `sport`
- `country`
- `category`
- `category_daily_summary`
- `category_daily_unique_tournament`
- `category_daily_team`

| Pattern | Path params | Envelope | Registry target | Notes |
| --- | --- | --- | --- | --- |
| `/api/v1/sport/football/{date}/{timezone_offset_seconds}/categories` | `date`, `timezone_offset_seconds` | `categories` | `category_daily_summary` | Daily discovery seed for categories plus arrays of `uniqueTournamentIds` and `teamIds` |

### 4.2 Competition Family

Entrypoint:

- `load_competition.py`

Normalized write scope:

- `endpoint_registry`
- `api_payload_snapshot`
- `image_asset`
- `sport`
- `country`
- `category`
- `team`
- `unique_tournament`
- `unique_tournament_relation`
- `unique_tournament_most_title_team`
- `season`
- `unique_tournament_season`

| Pattern | Path params | Envelope | Registry target | Notes |
| --- | --- | --- | --- | --- |
| `/api/v1/unique-tournament/{unique_tournament_id}` | `unique_tournament_id` | `uniqueTournament` | `unique_tournament` | Base tournament metadata |
| `/api/v1/unique-tournament/{unique_tournament_id}/seasons` | `unique_tournament_id` | `seasons` | `season` | Builds season links |
| `/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/info` | `unique_tournament_id`, `season_id` | `info` | `api_payload_snapshot` | Raw season info snapshot; parser also hydrates season-level metadata from payload |

### 4.3 Event List Family

Entrypoint:

- `load_event_list.py`

Normalized write scope:

- `endpoint_registry`
- `api_payload_snapshot`
- `sport`
- `country`
- `category`
- `team`
- `unique_tournament`
- `season`
- `tournament`
- `event_status`
- `event`
- `event_round_info`
- `event_status_time`
- `event_time`
- `event_var_in_progress`
- `event_score`
- `event_filter_value`
- `event_change_item`

| Pattern | Path params | Envelope | Registry target | Notes |
| --- | --- | --- | --- | --- |
| `/api/v1/sport/football/scheduled-events/{date}` | `date` | `events` | `event` | CLI subcommand `scheduled --date YYYY-MM-DD` |
| `/api/v1/sport/football/events/live` | none | `events` | `event` | CLI subcommand `live` |
| `/api/v1/unique-tournament/{unique_tournament_id}/featured-events` | `unique_tournament_id` | `featuredEvents` | `event` | CLI subcommand `featured` |
| `/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/events/round/{round_number}` | `unique_tournament_id`, `season_id`, `round_number` | `events` | `event` | CLI subcommand `round` |

### 4.4 Event Detail Family

Entrypoints:

- `load_event_detail.py`
- `load_full_backfill.py` via `EventDetailBackfillJob`

Normalized write scope:

- everything from Event List family
- `venue`
- `referee`
- `manager`
- `manager_performance`
- `manager_team_membership`
- `player`
- `event_manager_assignment`
- `event_duel`
- `event_pregame_form`
- `event_pregame_form_side`
- `event_pregame_form_item`
- `event_vote_option`
- `provider`
- `event_market`
- `event_market_choice`
- `event_winning_odds`
- `event_lineup`
- `event_lineup_player`
- `event_lineup_missing_player`

| Pattern | Path params | Envelope | Registry target | Notes |
| --- | --- | --- | --- | --- |
| `/api/v1/event/{event_id}` | `event_id` | `event` | `event` | Core event detail snapshot |
| `/api/v1/event/{event_id}/lineups` | `event_id` | `home,away` | `event_lineup` | Writes lineup header + players + missing players |
| `/api/v1/event/{event_id}/managers` | `event_id` | `homeManager,awayManager` | `event_manager_assignment` | Links event sides to managers |
| `/api/v1/event/{event_id}/h2h` | `event_id` | `teamDuel,managerDuel` | `event_duel` | Team and manager duel aggregates |
| `/api/v1/event/{event_id}/pregame-form` | `event_id` | `homeTeam,awayTeam` | `event_pregame_form` | Header plus side/form items |
| `/api/v1/event/{event_id}/votes` | `event_id` | `vote` | `event_vote_option` | Fan vote distribution |
| `/api/v1/event/{event_id}/odds/{provider_id}/all` | `event_id`, `provider_id` | `markets` | `event_market` | Full odds market list |
| `/api/v1/event/{event_id}/odds/{provider_id}/featured` | `event_id`, `provider_id` | `featured` | `event_market` | Featured odds only |
| `/api/v1/event/{event_id}/provider/{provider_id}/winning-odds` | `event_id`, `provider_id` | `home,away` | `event_winning_odds` | Winning-odds projection per side |

### 4.5 Standings Family

Entrypoint:

- `load_standings.py`
- `load_standings_backfill.py`

Normalized write scope:

- `endpoint_registry`
- `api_payload_snapshot`
- `sport`
- `country`
- `category`
- `unique_tournament`
- `tournament`
- `team`
- `standing_tie_breaking_rule`
- `standing_promotion`
- `standing`
- `standing_row`

| Pattern | Path params | Envelope | Registry target | Notes |
| --- | --- | --- | --- | --- |
| `/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/standings/{scope}` | `unique_tournament_id`, `season_id`, `scope` | `standings` | `standing` | Used when loader is called with `--unique-tournament-id` |
| `/api/v1/tournament/{tournament_id}/season/{season_id}/standings/{scope}` | `tournament_id`, `season_id`, `scope` | `standings` | `standing` | Used when loader is called with `--tournament-id` |

### 4.6 Statistics Family

Entrypoint:

- `load_statistics.py`
- `load_statistics_backfill.py`

Normalized write scope:

- `endpoint_registry`
- `api_payload_snapshot`
- `sport`
- `team`
- `player`
- `season_statistics_config`
- `season_statistics_config_team`
- `season_statistics_nationality`
- `season_statistics_group_item`
- `season_statistics_snapshot`
- `season_statistics_result`

| Pattern | Path params | Query | Envelope | Registry target | Notes |
| --- | --- | --- | --- | --- | --- |
| `/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/statistics/info` | `unique_tournament_id`, `season_id` | none | `hideHomeAndAway,teams,statisticsGroups,nationalities` | `season_statistics_config` | Configuration + teams + nationality + group definitions |
| `/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/statistics?limit={limit}&offset={offset}&order={order}&accumulation={accumulation}&group={group}&fields={fields}&filters={filters}` | `unique_tournament_id`, `season_id` | optional exact Sofascore params | `results` | `season_statistics_snapshot` | Actual URL omits absent query params |

### 4.7 Entities Family

Entrypoints:

- `load_entities.py`
- `load_entities_backfill.py`
- `load_full_backfill.py` via `EntitiesBackfillJob`

Normalized write scope:

- `endpoint_registry`
- `api_payload_snapshot`
- `sport`
- `country`
- `category`
- `category_transfer_period`
- `unique_tournament`
- `season`
- `unique_tournament_season`
- `tournament`
- `team`
- `venue`
- `manager`
- `manager_team_membership`
- `player`
- `player_transfer_history`
- `player_season_statistics`
- `entity_statistics_season`
- `entity_statistics_type`
- `season_statistics_type`

| Pattern | Path params | Envelope | Registry target | Notes |
| --- | --- | --- | --- | --- |
| `/api/v1/team/{team_id}` | `team_id` | `team` | `team` | Base team enrichment |
| `/api/v1/player/{player_id}` | `player_id` | `player` | `player` | Base player enrichment |
| `/api/v1/player/{player_id}/statistics` | `player_id` | `seasons,typesMap` | `player_season_statistics` | Normalizes per-season player summary metrics and also snapshots the raw payload |
| `/api/v1/player/{player_id}/transfer-history` | `player_id` | `transferHistory` | `player_transfer_history` | Transfer rows + team hydration |
| `/api/v1/player/{player_id}/statistics/seasons` | `player_id` | `uniqueTournamentSeasons,typesMap` | `entity_statistics_season` | Builds `entity_statistics_season` and `entity_statistics_type` |
| `/api/v1/team/{team_id}/team-statistics/seasons` | `team_id` | `uniqueTournamentSeasons,typesMap` | `entity_statistics_season` | Team season coverage + stat types |
| `/api/v1/team/{team_id}/player-statistics/seasons` | `team_id` | `uniqueTournamentSeasons,typesMap` | `season_statistics_type` | Season-level player stat types |
| `/api/v1/player/{player_id}/unique-tournament/{unique_tournament_id}/season/{season_id}/statistics/overall` | `player_id`, `unique_tournament_id`, `season_id` | `statistics,team` | `api_payload_snapshot` | Snapshot-only for current implementation; team dimension may also be hydrated |
| `/api/v1/team/{team_id}/unique-tournament/{unique_tournament_id}/season/{season_id}/statistics/overall` | `team_id`, `unique_tournament_id`, `season_id` | `statistics` | `api_payload_snapshot` | Snapshot-only for current implementation |
| `/api/v1/player/{player_id}/unique-tournament/{unique_tournament_id}/season/{season_id}/heatmap/overall` | `player_id`, `unique_tournament_id`, `season_id` | `heatmap,events` | `api_payload_snapshot` | Snapshot-only for current implementation |
| `/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/team/{team_id}/team-performance-graph-data` | `unique_tournament_id`, `season_id`, `team_id` | `graphData` | `api_payload_snapshot` | Snapshot-only for current implementation |

### 4.8 Leaderboards Family

Entrypoints:

- `load_leaderboards.py`
- `load_leaderboards_backfill.py`
- `load_full_backfill.py` via `LeaderboardsBackfillJob`

Normalized write scope:

- `endpoint_registry`
- `api_payload_snapshot`
- `sport`
- `country`
- `category`
- `unique_tournament`
- `season`
- `venue`
- `team`
- `player`
- `event_status`
- `event`
- `event_round_info`
- `event_status_time`
- `event_time`
- `event_var_in_progress`
- `event_score`
- `event_filter_value`
- `event_change_item`
- `period`
- `team_of_the_week`
- `team_of_the_week_player`
- `season_group`
- `season_player_of_the_season`
- `season_statistics_type`
- `top_player_snapshot`
- `top_player_entry`
- `top_team_snapshot`
- `top_team_entry`

| Pattern | Path params | Envelope | Registry target | Notes |
| --- | --- | --- | --- | --- |
| `/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/top-players/overall` | `unique_tournament_id`, `season_id` | `topPlayers` | `top_player_snapshot` | Main top-player leaderboard |
| `/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/top-ratings/overall` | `unique_tournament_id`, `season_id` | `topPlayers` | `top_player_snapshot` | Rating leaderboard |
| `/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/top-players-per-game/all/overall` | `unique_tournament_id`, `season_id` | `topPlayers` | `top_player_snapshot` | Per-game leaderboard |
| `/api/v1/team/{team_id}/unique-tournament/{unique_tournament_id}/season/{season_id}/top-players/overall` | `team_id`, `unique_tournament_id`, `season_id` | `topPlayers` | `top_player_snapshot` | Optional extra team-scoped pulls |
| `/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/player-of-the-season-race` | `unique_tournament_id`, `season_id` | `topPlayers,statisticsType` | `top_player_snapshot` | Race-style leaderboard |
| `/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/top-teams/overall` | `unique_tournament_id`, `season_id` | `topTeams` | `top_team_snapshot` | Team leaderboard |
| `/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/team-of-the-week/periods` | `unique_tournament_id`, `season_id` | `periods` | `period` | Period discovery |
| `/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/team-of-the-week/{period_id}` | `unique_tournament_id`, `season_id`, `period_id` | `formation,players` | `team_of_the_week` | Formation + team-of-week players |
| `/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/player-statistics/types` | `unique_tournament_id`, `season_id` | `types` | `season_statistics_type` | Player stat-type catalog |
| `/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/team-statistics/types` | `unique_tournament_id`, `season_id` | `types` | `season_statistics_type` | Team stat-type catalog |
| `/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/team-events/{scope}` | `unique_tournament_id`, `season_id`, `scope` | `tournamentTeamEvents` | `api_payload_snapshot` | Snapshot-only primary target; parser also hydrates nested `event` objects |
| `/api/v1/sport/football/trending-top-players` | none | `topPlayers` | `api_payload_snapshot` | Snapshot-only in current implementation |
| `/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/venues` | `unique_tournament_id`, `season_id` | `venues` | `venue` | Venue list per season |
| `/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/groups` | `unique_tournament_id`, `season_id` | `groups` | `season_group` | Season group to tournament mapping |
| `/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/player-of-the-season` | `unique_tournament_id`, `season_id` | `player,team,statistics,playerOfTheTournament` | `season_player_of_the_season` | One final player-of-season row per season |

## 5. Data Model Linkage

### 5.1 Registry And Raw Payload Layer

Key tables:

- `endpoint_registry`
- `api_payload_snapshot`

How they link:

- `endpoint_registry.pattern` is the canonical template key.
- `api_payload_snapshot.endpoint_pattern` references `endpoint_registry.pattern`.
- `api_payload_snapshot.source_url` stores the exact concrete URL actually fetched.
- `context_entity_type` and `context_entity_id` are used by backfills and debugging.

This is the core traceability layer:

- template lives in `endpoint_registry`
- concrete fetch lives in `api_payload_snapshot`
- normalized rows live in family-specific tables

### 5.2 Competition / Taxonomy Layer

Main tables:

- `sport`
- `country`
- `image_asset`
- `category`
- `category_transfer_period`
- `unique_tournament`
- `unique_tournament_relation`
- `unique_tournament_most_title_team`
- `season`
- `unique_tournament_season`

Main relations:

- `category.sport_id -> sport.id`
- `category.country_alpha2 -> country.alpha2`
- `unique_tournament.category_id -> category.id`
- `unique_tournament.title_holder_team_id -> team.id`
- `unique_tournament_season` bridges tournament and season

### 5.3 Tournament / Team / Person Layer

Main tables:

- `tournament`
- `venue`
- `referee`
- `team`
- `manager`
- `manager_performance`
- `manager_team_membership`
- `player`
- `player_transfer_history`

Main relations:

- `tournament.unique_tournament_id -> unique_tournament.id`
- `team.venue_id -> venue.id`
- `team.tournament_id -> tournament.id`
- `team.primary_unique_tournament_id -> unique_tournament.id`
- `team.manager_id -> manager.id`
- `manager.team_id -> team.id`
- `player.team_id -> team.id`
- `player.manager_id -> manager.id`
- `player_transfer_history.player_id -> player.id`
- `player_transfer_history.transfer_from_team_id / transfer_to_team_id -> team.id`

### 5.4 Event Layer

Main tables:

- `event_status`
- `event`
- `event_round_info`
- `event_status_time`
- `event_time`
- `event_var_in_progress`
- `event_score`
- `event_filter_value`
- `event_change_item`

Main relations:

- `event.tournament_id -> tournament.id`
- `event.unique_tournament_id -> unique_tournament.id`
- `event.season_id -> season.id`
- `event.home_team_id / away_team_id -> team.id`
- `event.venue_id -> venue.id`
- `event.referee_id -> referee.id`
- child event tables use `event_id -> event.id`

### 5.5 Event Detail Extension Layer

Main tables:

- `event_manager_assignment`
- `event_duel`
- `event_pregame_form`
- `event_pregame_form_side`
- `event_pregame_form_item`
- `event_vote_option`
- `provider`
- `event_market`
- `event_market_choice`
- `event_winning_odds`
- `event_lineup`
- `event_lineup_player`
- `event_lineup_missing_player`

Main relations:

- all event detail tables anchor on `event.id`
- odds tables anchor on `provider.id`
- lineup and market child tables anchor on their parent header tables

### 5.6 Statistics Layer

Main tables:

- `season_statistics_config`
- `season_statistics_config_team`
- `season_statistics_nationality`
- `season_statistics_group_item`
- `season_statistics_snapshot`
- `season_statistics_result`

Main relations:

- config tables key on `(unique_tournament_id, season_id)`
- `season_statistics_snapshot.endpoint_pattern -> endpoint_registry.pattern`
- `season_statistics_result.snapshot_id -> season_statistics_snapshot.id`
- statistics rows may reference `player.id` and `team.id`

Important modeling note:

- This family is the only one that stores exact query-state columns separately from raw snapshots.

### 5.7 Entity Enrichment Layer

Main tables:

- `entity_statistics_season`
- `entity_statistics_type`
- `season_statistics_type`

Main relations:

- `entity_statistics_season` keys on `(subject_type, subject_id, unique_tournament_id, season_id)`
- `entity_statistics_type` extends that key with `stat_type`
- `season_statistics_type` is season-level and not tied to a specific player/team row

### 5.8 Leaderboards / Aggregates Layer

Main tables:

- `top_player_snapshot`
- `top_player_entry`
- `top_team_snapshot`
- `top_team_entry`
- `period`
- `team_of_the_week`
- `team_of_the_week_player`
- `season_group`
- `season_player_of_the_season`

Main relations:

- snapshot tables reference `endpoint_registry.pattern`
- top-player rows may link to `player`, `team`, and optional `event`
- top-team rows link to `team`
- `team_of_the_week.period_id -> period.id`
- `team_of_the_week_player.period_id -> team_of_the_week.period_id`
- `season_group` links a season pair to specific `tournament_id`
- `season_player_of_the_season` stores one winner row per `(unique_tournament_id, season_id)`

## 6. Backfill Seed Logic

### 6.1 Event Detail Backfill

Source:

- `event` table

Dedupe rule:

- Skip event ids that already have `/api/v1/event/{event_id}` snapshot rows with:
  - `context_entity_type = 'event'`
  - `context_entity_id = event.id`

Selection order:

- newest `event.start_timestamp` first

### 6.2 Entities Backfill

Sources:

- `player.id`
- `team.id`
- player overall and heatmap requests seeded from:
  - `season_statistics_result`
  - `season_statistics_snapshot`
- team overall and performance graph requests seeded from:
  - `event.home_team_id`
  - `event.away_team_id`
  - `event.unique_tournament_id`
  - `event.season_id`

Dedupe rule:

- build exact concrete URLs
- skip URLs already present in `api_payload_snapshot.source_url` for the matching endpoint pattern

### 6.3 Statistics Backfill

Source:

- distinct `(unique_tournament_id, season_id)` pairs from `event`

Dedupe rule:

- each season pair is skipped only when all requested concrete URLs already exist
- by default that means:
  - `/statistics/info`
  - one default statistics page request

### 6.4 Standings Backfill

Sources:

- distinct season seeds from `event`
- prefers `unique_tournament_id + season_id`
- falls back to `tournament_id + season_id` when unique tournament is absent

Dedupe rule:

- skip a seed only when all requested standings scope URLs already exist in `api_payload_snapshot`

### 6.5 Leaderboards Backfill

Sources:

- distinct `(unique_tournament_id, season_id)` pairs from `event`
- optional team list per season from `event.home_team_id` and `event.away_team_id`

Dedupe rule:

- use `/top-players/overall` as the presence check for a season pair
- if the concrete URL already exists in `api_payload_snapshot`, the season is skipped unless forced

### 6.6 Full Backfill

Execution order:

1. `EventDetailBackfillJob`
2. `StatisticsBackfillJob`
3. `EntitiesBackfillJob`
4. `StandingsBackfillJob`
5. `LeaderboardsBackfillJob`

Reason for this order:

- event detail enriches `event`, `player`, `team`, `provider`, lineups and odds
- statistics builds `season_statistics_result`, which entities use as a player-season seed source
- entities reuse `player`, `team`, `season_statistics_result`, `event`
- standings reuse season seeds from `event`
- leaderboards reuse `event`, `team`, `player`, and season pairs already present

## 7. Snapshot-Only Or Partially Normalized Endpoints

The following endpoints are currently stored primarily as raw snapshots in `api_payload_snapshot`:

- `/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/info`
- `/api/v1/player/{player_id}/unique-tournament/{unique_tournament_id}/season/{season_id}/statistics/overall`
- `/api/v1/team/{team_id}/unique-tournament/{unique_tournament_id}/season/{season_id}/statistics/overall`
- `/api/v1/player/{player_id}/unique-tournament/{unique_tournament_id}/season/{season_id}/heatmap/overall`
- `/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/team/{team_id}/team-performance-graph-data`
- `/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/team-events/{scope}`
- `/api/v1/sport/football/trending-top-players`

Current nuance:

- some of these endpoints still hydrate dimensions discovered inside payloads
- but they do not yet have a dedicated fully normalized fact table in the current repositories

## 8. Remaining Empty Tables Need Seed Discovery, Not New Families

The main structural parser/repository gaps are now closed for:

- `provider_configuration`
- `tournament_team_event_snapshot`
- `tournament_team_event_bucket`
- `season_statistics_*`
- `standing*`

If any of these tables are still empty after a run, that now usually means one of two things:

- the relevant backfill entry point has not been executed yet
- the sampled live payloads did not expose the optional JSON branches needed for that table

In practice, the hardest tables to guarantee with just a small limited run are usually:

- `provider_configuration`
- `manager_performance`
- `period`
- `team_of_the_week`
- `team_of_the_week_player`
- `season_player_of_the_season`

## 9. Practical Reading Order

If you want to trace one endpoint end-to-end in code, use this order:

1. `schema_inspector/endpoints.py`
2. matching `*_cli.py`
3. matching `*_job.py`
4. matching parser
5. matching repository
6. `postgres_schema.sql`

That path shows:

- exact URL template
- runtime arguments
- parser normalization logic
- actual INSERT/UPSERT targets
- final DB shape
