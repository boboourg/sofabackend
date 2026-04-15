# Next Chat Context

This file is the current handoff source of truth for the next chat.
It supersedes the older `NEXT_CHAT_CONTEXT.md` content from `2026-04-12`.

## Environment

- Repo: `D:\sofascore`
- OS: Windows
- Shell: PowerShell
- Python: `.\.venv311\Scripts\python.exe`
- PostgreSQL: local, reachable at `localhost:5433`
- DB / proxy credentials: already configured in local `.env`
- Date of this handoff: `2026-04-16`
- Remote: `origin -> https://github.com/boboourg/sofabackend.git`

## Why This Chat Happened

The project has moved from "football ETL + local mirror" into two parallel tracks:

1. make the local API / Swagger multi-sport instead of football-only
2. map Sofascore endpoint families sport-by-sport, especially for handball

The immediate trigger was a user-supplied `__NEXT_DATA__` dump from `/handball` and the question:

- does `__NEXT_DATA__` actually reveal handball endpoint families?
- do we need to inspect every page's `__NEXT_DATA__`?

Current answer:

- `__NEXT_DATA__` is useful for feature-family hints and route seeds
- it is not a direct `api/v1` registry
- we should inspect page classes, not random pages one by one

## Current Goal

Keep building an honest, proxy-first, async Sofascore ingestion and local mirror stack, but now with a clearer multi-sport endpoint model.

Near-term objective:

1. keep the local API / Swagger aligned with real ingested payload families
2. use probe scripts plus `__NEXT_DATA__` hints to classify sports into endpoint archetypes
3. determine the correct handball family without inventing fake sport-specific routes

## Architecture Truth

These points must stay true in the next chat:

1. All outbound HTTP goes through the existing transport/client layer.
2. Proxy-first mode is required.
3. The ETL architecture is async and should stay async.
4. Endpoint path templates should remain exact Sofascore-style paths.
5. Transport uses `curl_cffi` impersonation, retry/backoff, and challenge detection.
6. We should not claim custom TLS spoofing, CAPTCHA bypass, or anti-detect magic.
7. Correct wording: proxy-only transport plus `curl_cffi` impersonation and challenge-aware retries.

## Current Code State

Tracked modifications already present before this handoff commit:

- `local_swagger/index.html`
- `schema_inspector/current_year_pipeline_cli.py`
- `schema_inspector/endpoints.py`
- `schema_inspector/entities_backfill_job.py`
- `schema_inspector/event_detail_backfill_job.py`
- `schema_inspector/local_api_server.py`
- `schema_inspector/local_swagger_builder.py`

Untracked files already present before this handoff commit:

- `local_swagger/multisport.openapi.json`
- `probe_multisport_reports.py`
- `probe_next_data_feature_families.py`

## What Changed In Code

### 1. Multi-sport endpoint registry

`schema_inspector/endpoints.py` now has an explicit local multi-sport registry instead of assuming football everywhere.

Important additions:

- `LOCAL_API_SUPPORTED_SPORTS = ("football", "basketball", "tennis")`
- sport-aware local leaderboard assembly
- `local_api_endpoints(...)` is now the main source for the local API / OpenAPI route inventory

### 2. Local API server is now multi-sport oriented

`schema_inspector/local_api_server.py` now describes itself as a multi-sport local mirror and builds routes from `local_api_endpoints()`.

Meaning:

- local server route registration is no longer football-branded
- Swagger and API server share the same registry source

### 3. Local Swagger builder is now multi-sport

`schema_inspector/local_swagger_builder.py` now generates:

- title: `Sofascore Local Multi-Sport API`
- output file: `local_swagger/multisport.openapi.json`
- tag groups across categories / competition / event list / event detail / standings / statistics / entities / leaderboards

### 4. Current-year pipeline now propagates tournament scope into deeper stages

`schema_inspector/current_year_pipeline_cli.py` now passes the discovered / selected tournament ids into:

- `event_detail_backfill_job.run(... unique_tournament_ids=...)`
- `entities_backfill_job.run(... unique_tournament_ids=...)`

This matters because current-year runs are now bounded to the selected tournaments instead of accidentally widening again during event-detail and entities backfill.

### 5. Event-detail backfill now accepts tournament-id batches

`schema_inspector/event_detail_backfill_job.py` now supports:

- `unique_tournament_ids: Iterable[int] | None`

and filters candidates with:

- `e.unique_tournament_id = ANY($3)`

### 6. Entities backfill now accepts tournament-id batches

`schema_inspector/entities_backfill_job.py` now supports:

- `unique_tournament_ids: tuple[int, ...] | None`

and threads that filter through:

- player seed loading
- team seed loading
- player overall request loading
- team overall request loading

This keeps entity hydration aligned with the tournament slice chosen by discovery.

## Current Code Excerpts

These are the live code touchpoints that matter most in the next chat.

### `probe_multisport_reports.py`

The broad sport probe starts from a fixed discovery family:

```python
initial_urls = [
    f"https://www.sofascore.com/api/v1/sport/{sport}/categories/all",
    f"https://www.sofascore.com/api/v1/sport/{sport}/{date}/{tz_offset}/categories",
    f"https://www.sofascore.com/api/v1/sport/{sport}/scheduled-tournaments/{date}/page/1",
    f"https://www.sofascore.com/api/v1/sport/{sport}/scheduled-events/{date}",
    f"https://www.sofascore.com/api/v1/sport/{sport}/events/live",
    f"https://www.sofascore.com/api/v1/sport/{sport}/trending-top-players",
    f"https://www.sofascore.com/api/v1/config/default-unique-tournaments/{country_code}/{sport}",
]
```

It then expands into category / tournament / season / event / entity families and writes markdown + JSON summaries.

### `probe_next_data_feature_families.py`

The targeted probe uses `__NEXT_DATA__` only as a hint source, then verifies URLs empirically:

```python
target_sports = args.sport or ["ice-hockey", "baseball", "cricket", "mma"]
```

and probes the generic event family first:

```python
generic = [
    f"https://www.sofascore.com/api/v1/event/{context.event_id}/meta",
    f"https://www.sofascore.com/api/v1/event/{context.event_id}/statistics",
    f"https://api.sofascore.com/api/v1/event/{context.event_id}/statistics",
    f"https://www.sofascore.com/api/v1/event/{context.event_id}/lineups",
    f"https://www.sofascore.com/api/v1/event/{context.event_id}/incidents",
    f"https://www.sofascore.com/api/v1/event/{context.event_id}/best-players",
    f"https://www.sofascore.com/api/v1/event/{context.event_id}/h2h",
    f"https://www.sofascore.com/api/v1/event/{context.event_id}/pregame-form",
    f"https://www.sofascore.com/api/v1/event/{context.event_id}/graph",
    f"https://www.sofascore.com/api/v1/event/{context.event_id}/graph/win-probability",
    f"https://www.sofascore.com/api/v1/event/{context.event_id}/votes",
    f"https://www.sofascore.com/api/v1/event/{context.event_id}/comments",
    f"https://www.sofascore.com/api/v1/event/{context.event_id}/official-tweets",
    f"https://www.sofascore.com/api/v1/event/{context.event_id}/highlights",
]
```

### `schema_inspector/current_year_pipeline_cli.py`

The important current fix is that tournament scope is now preserved:

```python
event_detail_result = await event_detail_backfill_job.run(
    limit=args.event_detail_limit,
    only_missing=not args.all_event_detail,
    unique_tournament_ids=selected_unique_tournament_ids or None,
    start_timestamp_from=start_timestamp_from,
    start_timestamp_to=start_timestamp_to,
    provider_ids=provider_ids,
    concurrency=max(args.event_detail_concurrency, 1),
    timeout=args.timeout,
)

entities_result = await entities_backfill_job.run(
    player_limit=args.entity_player_limit,
    team_limit=args.entity_team_limit,
    player_request_limit=args.entity_player_request_limit,
    team_request_limit=args.entity_team_request_limit,
    only_missing=not args.all_entities,
    unique_tournament_ids=selected_unique_tournament_ids or None,
    event_timestamp_from=start_timestamp_from,
    event_timestamp_to=start_timestamp_to,
    timeout=args.timeout,
)
```

### `schema_inspector/event_detail_backfill_job.py`

Key candidate filter:

```sql
AND ($3::bigint[] IS NULL OR e.unique_tournament_id = ANY($3))
```

### `schema_inspector/entities_backfill_job.py`

Key seed filter pattern:

```sql
AND ($3::bigint[] IS NULL OR e.unique_tournament_id = ANY($3))
```

This now appears in the event-based entity seed loaders and is the main reason the current-year slice behaves correctly.

## Current Theory: Sofascore Is An ID Graph

Working model:

1. sport discovery
   - `sport/{sport}/categories/all`
   - `sport/{sport}/{date}/{tz}/categories`
   - `sport/{sport}/scheduled-tournaments/{date}/page/{page}`
   - `sport/{sport}/scheduled-events/{date}`
   - `sport/{sport}/events/live`

2. category -> unique tournament -> season
   - `category/{categoryId}/unique-tournaments`
   - `unique-tournament/{id}`
   - `unique-tournament/{id}/seasons`
   - `unique-tournament/{id}/season/{seasonId}/info`

3. event list
   - `unique-tournament/{id}/scheduled-events/{date}`
   - `unique-tournament/{id}/season/{seasonId}/events/round/{round}`
   - `sport/{sport}/scheduled-events/{date}`

4. event detail
   - `event/{id}`
   - `meta`
   - `statistics`
   - `lineups`
   - `incidents`
   - `h2h`
   - `pregame-form`
   - `votes`
   - optional families like `graph`, `comments`, `official-tweets`, `highlights`

5. entities / profiles
   - `team/{teamId}`
   - `player/{playerId}`
   - `manager/{managerId}`
   - player/team season statistics and history

## Core Theory About `__NEXT_DATA__`

`__NEXT_DATA__` is useful, but only in a bounded way.

### What it is good for

- route seeds like `/handball`, `/handball/tournament/...`, `/handball/match/...#id:{eventId}`
- tab and feature labels
- sport-specific stat names
- hints that some UI block probably maps to `statistics`, `lineups`, `incidents`, `shotmap`, or another family

### What it is not good for

- it is not a direct endpoint registry
- it does not guarantee a dedicated `api/v1/...` route exists for every UI label

### Important rule

UI feature names do not always map to dedicated endpoints.

Already proven examples:

- hockey `play-by-play` is really `event/{id}/incidents`
- hockey `event map` / shot heatmap is really `shotmap`
- baseball `batting order` and `starting pitchers` live inside `lineups`

### Correct inspection order

Do not inspect every random page.

Inspect page classes:

1. sport landing page
2. tournament page
3. event page
4. player page
5. team page

## Probe Scripts And Outputs

### Broad probe

- Script: `probe_multisport_reports.py`
- Output folder: `reports/multisport_probe_2026_04_15`

Purpose:

- discover sport slugs from `sport/10800/event-count`
- probe discovery / competition / event / entity families
- write schema reports via `inspect_api.py`

### Targeted `__NEXT_DATA__` probe

- Script: `probe_next_data_feature_families.py`
- Output folder: `reports/next_data_feature_probe_2026_04_15`

Purpose:

- start from UI feature hints
- probe generic event family first
- only then probe sport-specific guesses
- use 200/404 outcomes to kill fake theories early

## Empirical Findings Already Proved

Primary source summary:

- `reports/next_data_feature_probe_2026_04_15/probe_summary.md`

### ice-hockey

Confirmed `200`:

- `meta`
- `statistics`
- `lineups`
- `incidents`
- `best-players`
- `h2h`
- `pregame-form`
- `votes`
- `comments`
- `official-tweets`
- `highlights`
- `api.sofascore.com/.../shotmap`
- `shotmap/{teamId}`

Failed `404`:

- `play-by-play`
- `event-map`
- `penalties`
- `player/{playerId}/shotmap`

Conclusion:

- hockey play-by-play / penalties are represented by `incidents`
- hockey event-map / shot heatmap is represented by `shotmap`

Supporting evidence:

- `reports/next_data_feature_probe_2026_04_15/www_sofascore_com_api_v1_event_14201603_incidents.md`
- `reports/next_data_feature_probe_2026_04_15/api_sofascore_com_api_v1_event_14201603_shotmap.md`

### baseball

Confirmed `200`:

- `meta`
- `statistics`
- `lineups`
- `h2h`
- `pregame-form`
- `votes`
- `comments`
- `official-tweets`
- `highlights`

Failed `404`:

- `incidents`
- `best-players`
- `graph`
- `plays`
- `all-plays`
- `batting-order`
- `starting-pitchers`
- `shotmap/{teamId}`

Conclusion:

- baseball lineup payload already carries batting-order / pitcher context
- do not invent dedicated `batting-order` / `starting-pitchers` endpoints just because the UI has those words

Supporting evidence:

- `reports/next_data_feature_probe_2026_04_15/www_sofascore_com_api_v1_event_15507996_lineups.md`

### cricket

Confirmed `200`:

- `meta`
- `lineups`
- `incidents`
- `h2h`
- `pregame-form`
- `votes`
- `highlights`

Failed `404` in current probe:

- `statistics`
- `best-players`
- `graph`
- `comments`
- `official-tweets`
- `runs-per-over`
- `runs-per-over/graph`
- `overs`

Conclusion:

- cricket looks thinner than football-style event families
- innings / scorecard progression probably exists elsewhere or is embedded differently

### mma

Confirmed `200`:

- `meta`
- `statistics`
- `h2h`
- `votes`

Failed `404` in current probe:

- `lineups`
- `incidents`
- `best-players`
- `pregame-form`
- `comments`
- `official-tweets`
- `highlights`

Conclusion:

- MMA is a thin event surface compared with team sports

## Sport Archetype Theory

Current matrix:

- rich team-sport generic:
  - football
  - basketball

- rich map / incident team sport:
  - ice-hockey
  - baseball

- mixed team sport:
  - handball
  - volleyball
  - futsal
  - minifootball
  - floorball
  - waterpolo

- racket / bracket sport:
  - tennis
  - badminton
  - table-tennis
  - snooker
  - darts

- thin / unusual:
  - cricket
  - esports
  - mma

This archetype model is documented in:

- `reports/SPORT_ENDPOINT_PATTERN_MATRIX_2026_04_15.md`

## Handball: Current Best Theory

The current working assumption is:

- handball is closer to a football-like generic team-sport family
- the valuable specialization is likely inside `statistics`
- there is no evidence yet that handball needs a tennis-like special endpoint family

### Handball hints extracted from `/handball` `__NEXT_DATA__`

Roster / roles:

- `goalkeeper`
- `back`
- `winger`
- `pivot`
- `centre_back`

Stat-family hints:

- `sevenMetersScored`
- `7mSaves`
- `6mSaves`
- `9mSaves`
- `goalkeeperEfficiency`
- `breakthroughGoals`
- `breakthroughSaves`
- `fastbreakGoals`
- `fastbreakSaves`
- `twoMinutePenalties`
- `2minPenalty`
- `technicalFaults`

### Handball discovery family to test first

```text
/api/v1/sport/handball/categories/all
/api/v1/sport/handball/{date}/{tz}/categories
/api/v1/sport/handball/scheduled-tournaments/{date}/page/1
/api/v1/sport/handball/scheduled-events/{date}
/api/v1/sport/handball/events/live
/api/v1/config/default-unique-tournaments/UA/handball
```

### Handball competition / season family to test next

```text
/api/v1/category/{categoryId}/unique-tournaments
/api/v1/unique-tournament/{id}
/api/v1/unique-tournament/{id}/seasons
/api/v1/unique-tournament/{id}/season/{seasonId}/info
/api/v1/unique-tournament/{id}/featured-events
/api/v1/unique-tournament/{id}/scheduled-events/{date}
/api/v1/unique-tournament/{id}/season/{seasonId}/standings/total
/api/v1/unique-tournament/{id}/season/{seasonId}/statistics/info
/api/v1/unique-tournament/{id}/season/{seasonId}/statistics?... 
```

### Handball event family to test next

Core:

```text
/api/v1/event/{eventId}
/api/v1/event/{eventId}/meta
/api/v1/event/{eventId}/statistics
/api/v1/event/{eventId}/lineups
/api/v1/event/{eventId}/incidents
/api/v1/event/{eventId}/best-players
/api/v1/event/{eventId}/h2h
/api/v1/event/{eventId}/pregame-form
/api/v1/event/{eventId}/votes
```

Optional:

```text
/api/v1/event/{eventId}/graph
/api/v1/event/{eventId}/graph/win-probability
/api/v1/event/{eventId}/comments
/api/v1/event/{eventId}/official-tweets
/api/v1/event/{eventId}/highlights
/api/v1/event/{eventId}/sport-video-highlights/country/UA/extended
/api/v1/event/{eventId}/live-action-widget
```

### Handball entity family to test after that

```text
/api/v1/team/{teamId}
/api/v1/team/{teamId}/team-statistics/seasons
/api/v1/team/{teamId}/player-statistics/seasons
/api/v1/team/{teamId}/events/last/0
/api/v1/player/{playerId}
/api/v1/player/{playerId}/statistics
/api/v1/player/{playerId}/statistics/seasons
/api/v1/player/{playerId}/events/last/0
/api/v1/manager/{managerId}
/api/v1/manager/{managerId}/career-history
```

## What To Ask For In The Next Chat

If someone pastes handball responses, ask for:

1. a list of URLs with `200`
2. a list of URLs with `404`
3. raw or summarized JSON from:
   - `event/{eventId}/statistics`
   - `unique-tournament/{id}/season/{seasonId}/statistics/info`
   - `player/{playerId}/statistics`

That is enough to decide whether handball is:

- pure generic team-sport
- generic team-sport with custom stat vocabulary
- or hiding an extra dedicated family

## Files To Open First In The Next Chat

1. `NEXT_CHAT_CONTEXT.md`
2. `reports/SPORT_ENDPOINT_PATTERN_MATRIX_2026_04_15.md`
3. `reports/next_data_feature_probe_2026_04_15/probe_summary.md`
4. `probe_multisport_reports.py`
5. `probe_next_data_feature_families.py`
6. `schema_inspector/endpoints.py`
7. `schema_inspector/local_api_server.py`
8. `schema_inspector/local_swagger_builder.py`
9. `schema_inspector/current_year_pipeline_cli.py`
10. `schema_inspector/event_detail_backfill_job.py`
11. `schema_inspector/entities_backfill_job.py`

## Useful Commands From Laptop

Pull latest code:

```powershell
git -C D:\sofascore pull --ff-only origin main
```

Generate local multi-sport Swagger:

```powershell
.\.venv311\Scripts\python.exe -m schema_inspector.local_swagger_builder
```

Serve local multi-sport API:

```powershell
.\.venv311\Scripts\python.exe serve_local_api.py --host 127.0.0.1 --port 8000
```

Run broad multisport probe:

```powershell
.\.venv311\Scripts\python.exe probe_multisport_reports.py --limit-sports 6
```

Run targeted handball feature probe after adapting the script or adding `--sport handball`:

```powershell
.\.venv311\Scripts\python.exe probe_next_data_feature_families.py --sport handball
```

Run current-year bounded ingestion:

```powershell
.\.venv311\Scripts\python.exe -m schema_inspector.current_year_pipeline_cli --sport-slug football --timezone-name Europe/Kiev --log-level INFO
```

## Final Reminder

Do not treat UI wording as proof of a dedicated endpoint.

The fastest correct workflow is:

1. probe discovery family
2. probe generic event family
3. inspect `statistics`, `lineups`, `incidents`
4. only then guess sport-specific routes

That rule already prevented false theories for hockey and baseball, and it should be applied to handball next.
