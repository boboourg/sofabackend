# Next Chat Context

This file is the current handoff source of truth for the next chat.
It supersedes the older `CHAT_HANDOFF.md`.

## Environment

- Repo: `C:\Users\bobur\Desktop\sofascore`
- OS: Windows
- Shell: PowerShell
- Python: project venv at `.\.venv311\Scripts\python.exe`
- PostgreSQL: local, reachable at `localhost:5433`
- DB DSN: already configured in local `.env`
- Date of this handoff: `2026-04-12`

## Main Goal

Build and run a proxy-first async football data pipeline for Sofascore into a normalized PostgreSQL schema, then expose the ingested payloads through a local Swagger/API mirror using exact Sofascore path patterns.

The user wants:

1. fast mass loading
2. top tournaments first
3. then broader backfill for the rest
4. no fake architecture claims
5. practical commands over theory

## Architecture Truth

These points must stay true in the next chat:

1. All outbound HTTP must go through the existing transport/client layer.
2. Proxy-first mode is required.
3. The ETL architecture is async and should stay async.
4. Endpoint path templates must remain exact Sofascore-style paths.
5. Transport uses `curl_cffi` impersonation, retry/backoff, and challenge detection.
6. There is no legitimate basis to claim a custom TLS spoofing engine, CAPTCHA bypass, or anti-detect bypass stack.
7. The correct truthful wording is: proxy-only transport plus `curl_cffi` impersonation (`chrome110`) plus challenge detection/retries.

## Proxy Status

- The project is configured for residential Smartproxy usage through `.env`.
- There are multiple proxy endpoints in rotation.
- Do not paste proxy usernames/passwords into new handoff files or commit them.
- If a future chat needs proxy info, say they are already configured in `.env`.

## What Exists Now

Implemented parser / ETL families:

1. `categories_seed`
2. `competition`
3. `event_list`
4. `event_detail`
5. `statistics`
6. `standings`
7. `entities`
8. `leaderboards`
9. local API / local Swagger support
10. default-tournaments worker pipeline

Main wrapper scripts currently present:

- `load_bootstrap_pipeline.py`
- `load_default_tournaments_pipeline.py`
- `load_full_backfill.py`
- `load_entities_backfill.py`
- `load_targeted_pipeline.py`
- `load_slice_pipeline.py`
- `serve_local_api.py`
- `setup_postgres.py`

## Important Endpoint Coverage

Key exact endpoints currently implemented or recently extended:

### Bootstrap / discovery

- `/api/v1/sport/football/{date}/{timezone_offset_seconds}/categories`
- `/api/v1/config/default-unique-tournaments/{country_code}/{sport_slug}`

### Competition

- `/api/v1/unique-tournament/{unique_tournament_id}`
- `/api/v1/unique-tournament/{unique_tournament_id}/seasons`
- `/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/info`

### Entities / enrichment

- `/api/v1/team/{team_id}`
- `/api/v1/player/{player_id}`
- `/api/v1/player/{player_id}/statistics`
- `/api/v1/player/{player_id}/statistics/seasons`
- `/api/v1/player/{player_id}/transfer-history`
- `/api/v1/team/{team_id}/team-statistics/seasons`
- `/api/v1/team/{team_id}/player-statistics/seasons`
- `/api/v1/player/{player_id}/unique-tournament/{unique_tournament_id}/season/{season_id}/statistics/overall`
- `/api/v1/team/{team_id}/unique-tournament/{unique_tournament_id}/season/{season_id}/statistics/overall`
- `/api/v1/player/{player_id}/unique-tournament/{unique_tournament_id}/season/{season_id}/heatmap/overall`
- `/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/team/{team_id}/team-performance-graph-data`

### Event detail

- `/api/v1/event/{event_id}`
- `/api/v1/event/{event_id}/lineups`
- `/api/v1/event/{event_id}/managers`
- `/api/v1/event/{event_id}/h2h`
- `/api/v1/event/{event_id}/pregame-form`
- `/api/v1/event/{event_id}/votes`
- `/api/v1/event/{event_id}/comments`
- `/api/v1/event/{event_id}/graph`
- `/api/v1/event/{event_id}/heatmap/{team_id}`
- odds routes already wired through event detail

### Statistics / standings / leaderboards

- season statistics routes
- season standings routes
- top players / top ratings / top teams
- venues / groups / player-of-the-season
- team-of-the-week
- player statistics types / team statistics types
- team events

## Important Data Model Notes

These points were a major source of confusion and should be preserved:

1. `player_season_statistics` is the table for `/api/v1/player/{player_id}/statistics`.
   That is where per-player season totals like `total_shots`, `expected_goals`, `assists`, `minutes_played`, etc. belong.

2. `season_statistics_result` is a different table.
   It stores rows from `/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/statistics`.
   Many fields can legitimately be `NULL` there depending on the specific query, field set, and result type.

3. If the user asks where league/season ids are linked:
   - `unique_tournament`
   - `season`
   - `unique_tournament_season`
   are the key tables.

4. The schema has around 71 tables.
   That is normal for this project because it is intentionally normalized.

## Recent Schema / Migration State

Known migration files in `migrations/`:

- `2026-04-11_category_daily_seed.sql`
- `2026-04-11_drop_player_event_slug_unique.sql`
- `2026-04-11_drop_venue_slug_unique.sql`
- `2026-04-11_leaderboards_season_extras.sql`
- `2026-04-11_player_season_statistics.sql`
- `2026-04-11_season_statistics_result_extended.sql`
- `2026-04-12_event_live_detail_extras.sql`

If a future chat sees missing tables or relation errors, first safe command to suggest is:

```powershell
.\.venv311\Scripts\python.exe setup_postgres.py --skip-create-database
```

## Important Fixes Already Made

### 1. Proxy starvation / cooldown handling

Earlier long runs could fail with:

- `Proxy-only mode is enabled, but no proxy is currently available.`

This was improved by making transport wait for the next proxy to leave cooldown instead of failing too eagerly.

### 2. Optional 404 handling

Several Sofascore endpoints are genuinely optional and should not be treated as hard failures in every case.

Examples:

- `/api/v1/player/{player_id}/statistics`
- `/api/v1/player/{player_id}/transfer-history`
- some standings/statistics combinations
- event comments/graph/heatmap for not-started matches

### 3. Team performance graph local API bug

There was a local API mismatch for:

- `/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/team/{team_id}/team-performance-graph-data`

Root cause:

- local route matching inferred `season` context instead of `team` context

Fix:

- `schema_inspector/local_api_server.py` was updated so team routes use team context first

### 4. Event live detail extras

Recently added storage + parsing for:

- event comments
- event graph
- event team heatmap

These are only available for suitable live/finished events, not for every pre-match event.

## Local API / Swagger Notes

The local API mirrors what has already been ingested, not what exists live upstream.

That means this error is expected in some cases:

```json
{
  "error": "Requested path exists in the local API contract, but no ingested payload matched this path/query yet."
}
```

It means:

1. the route is registered correctly
2. but the DB has no matching snapshot yet for that exact path/query/context

Correct local API launch command:

```powershell
.\.venv311\Scripts\python.exe serve_local_api.py --host 127.0.0.1 --port 8000
```

Important route reminder:

- correct heatmap route is `/api/v1/event/{event_id}/heatmap/{team_id}`
- wrong route is `/api/v1/event/{event_id}/graph/heatmap/{team_id}`

## Proven / Useful Commands

### A. Fast top-tournaments baseline

This was just added and validated for CLI/help/tests:

```powershell
.\.venv311\Scripts\python.exe load_default_tournaments_pipeline.py --country-code UA --sport-slug football --tournament-concurrency 3 --seasons-per-tournament 2 --event-concurrency 3 --timeout 20 --log-level INFO
```

For one tournament only:

```powershell
.\.venv311\Scripts\python.exe load_default_tournaments_pipeline.py --country-code UA --sport-slug football --unique-tournament-id 17 --seasons-per-tournament 1 --tournament-concurrency 1 --event-concurrency 2 --timeout 20 --log-level INFO
```

What this pipeline does:

1. reads the curated tournament list from `UA/football`
2. runs tournament workers
3. loads tournament metadata and seasons
4. processes latest `N` seasons
5. loads statistics, standings, leaderboards
6. loads discovered round events and event details where discoverable
7. hydrates teams, players, player stats, transfer history, overall stats, heatmaps, team performance graph

Important limitation:

It is a fast baseline, not a magical full historical event discovery engine.
If the DB does not yet know old `event_id` values, this pipeline cannot invent all historical matches by itself.
For wide event history, date bootstrap is still needed.

### B. Broad date bootstrap

This command was used successfully in the project:

```powershell
.\.venv311\Scripts\python.exe load_bootstrap_pipeline.py --date-from 2026-04-08 --date-to 2026-04-14 --timezone-offset-seconds 10800 --competition-concurrency 3 --include-live --timeout 20
```

One real observed large result from the user:

```text
bootstrap_pipeline dates=7 categories=996 discovered_unique_tournaments=2035 competition_succeeded=2028/2035 scheduled_events=2867 live_events=556
```

This means bootstrap is already proven to discover a large volume of tournament ids and event seeds.

### C. Full backfill

The user has been running:

```powershell
.\.venv311\Scripts\python.exe load_full_backfill.py --event-concurrency 5 --statistics-concurrency 3 --standings-concurrency 3 --leaderboards-concurrency 3 --standings-scope total --standings-scope home --standings-scope away --timeout 20 --log-level DEBUG
```

What `load_full_backfill.py` does:

1. backfills event details
2. backfills statistics
3. backfills entities
4. backfills standings
5. backfills leaderboards

### D. Entities-only deep backfill

```powershell
.\.venv311\Scripts\python.exe load_entities_backfill.py --timeout 30 --log-level INFO
```

This can take a very long time if run across all players.

### E. One targeted tournament/team/player test

This targeted command is known-good:

```powershell
.\.venv311\Scripts\python.exe load_targeted_pipeline.py --unique-tournament-id 17 --season-id 76986 --team-id 35 --player-id 288205 --timeout 20
```

One observed result:

```text
targeted_pipeline competition_snapshots=3 statistics_results=521 standings=3 leaderboard_snapshots=44 entity_player_stats=56 event_details=0
```

### F. One slice with event details + manager verification

A slice run for league/team/manager/player succeeded and produced:

```text
slice_pipeline competition_snapshots=3 statistics_results=521 standings=3 leaderboard_snapshots=44 round_jobs=2 team_events=2 event_detail_succeeded=2/2 discovered_players=29 entity_player_stats=842 found_managers=1
```

### G. Event-detail batch for one tournament

Useful for filling event extras for one competition:

```powershell
.\.venv311\Scripts\python.exe load_event_detail_batch.py --all-events --unique-tournament-id 17 --concurrency 3 --timeout 20 --log-level INFO
```

## Recommended Order For The Next Big Fill

If the next chat needs a practical loading plan, the current best order is:

1. ensure schema/migrations are applied
2. run `load_default_tournaments_pipeline.py` for fast top-tournament baseline
3. run `load_bootstrap_pipeline.py` for broad football date discovery
4. run `load_full_backfill.py` for deeper enrichment
5. optionally run `load_entities_backfill.py` if player/team enrichment is still thin

## Known Normal Upstream Behaviors

The next chat should not misdiagnose these as automatic bugs:

1. Some Sofascore endpoints really return `404`.
2. `/statistics/info` is not guaranteed on every competition/season.
3. `/player/{id}/statistics` is not guaranteed for every player id.
4. `/player/{id}/transfer-history` is not guaranteed for every player id.
5. event comments/graph/heatmap are not guaranteed for pre-match events.
6. the local API can know a route pattern while still having no payload snapshot for it yet.

## Key Files Touched Recently

Recently important files include:

- `schema_inspector/default_tournaments_parser.py`
- `schema_inspector/default_tournaments_pipeline_cli.py`
- `load_default_tournaments_pipeline.py`
- `schema_inspector/competition_job.py`
- `schema_inspector/local_api_server.py`
- `schema_inspector/local_swagger_builder.py`
- `schema_inspector/event_detail_parser.py`
- `schema_inspector/event_detail_repository.py`
- `schema_inspector/event_detail_backfill_job.py`
- `schema_inspector/event_detail_backfill_cli.py`
- `postgres_schema.sql`

## Useful Reports / Source Notes

The `reports/` folder contains saved Sofascore endpoint payload notes that informed recent work.
Especially relevant:

- `reports\www_sofascore_com_api_v1_config_default_unique_tournaments_ua_football.md`
- `reports\www_sofascore_com_api_v1_player_288205_statistics.md`
- event comments / graph / heatmap report files for one match

## Validation Status

Known earlier full-suite milestone:

- after the event live detail extras work, full test suite reached `82 tests OK`

Latest validation done in this chat:

1. `py_compile` passed for the new default-tournaments pipeline files
2. new tests passed:
   - `tests.test_default_tournaments_parser`
   - `tests.test_default_tournaments_pipeline`
3. touched legacy tests also passed:
   - `tests.test_competition_parser`
   - `tests.test_entities_parser`
   - `tests.test_event_detail_backfill`

The latest full suite was not rerun in this chat.

## Open Risks / Likely Next Improvements

These are the most realistic next tasks if something breaks or needs speed:

1. run the new top-tournaments pipeline on real volume and inspect DB row growth
2. if manager slug collisions resurface, inspect manager uniqueness constraints
3. if entities are too slow, shard or batch entity ingestion more aggressively
4. if broader history is needed, rely on date bootstrap because top-tournaments baseline alone does not discover every old match

## One-Line Summary For The Next Chat

Continue from the existing repo, do not start from scratch, keep the proxy-first async architecture, treat many upstream `404` as normal/optional, use `player_season_statistics` for player totals, and use the new `load_default_tournaments_pipeline.py` as the fast top-tournament baseline before wider bootstrap/backfill.
