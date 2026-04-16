# Sofascore Hybrid ETL Implementation Plan
> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox syntax.

**Goal:** Build a production-grade hybrid ETL platform for the Sofascore API across 13 sports, using PostgreSQL as the durable source of truth and Redis as the control plane for queueing, leases, dedupe, live-state, retries, and shared transport health. The new backbone must support batch discovery, live event refresh, replayable raw snapshots, parser versioning, and capability-aware planning without creating 13 isolated scripts.

**Architecture:** Production hybrid ETL with one shared proxy-first fetch layer, one planner/orchestrator, one parser registry, raw-first ingestion into PostgreSQL, Redis Streams-based job orchestration, and staged rollout starting with football, basketball, and tennis.

**Tech Stack:** Python 3.11, asyncpg, PostgreSQL, Redis Streams, curl_cffi-based transport, current `schema_inspector` transport/runtime stack, pytest/unittest test suite, existing local API and Swagger surfaces.

## Non-Negotiable Invariants

- [ ] Every outbound Sofascore request must go through the shared transport in `schema_inspector/runtime.py`, `schema_inspector/proxy.py`, `schema_inspector/challenge.py`, `schema_inspector/transport.py`, `schema_inspector/fetch.py`, and `schema_inspector/sofascore_client.py`.
- [ ] Proxy-first transport remains mandatory. No direct `requests.get()` or ad-hoc network helpers are allowed in jobs, parsers, workers, CLIs, or tests.
- [ ] Browser-like impersonation and challenge-aware behavior remain enabled through the existing transport stack. We preserve the current behavior instead of inventing a separate "TLS bypass" subsystem.
- [ ] PostgreSQL is the durable source of truth. Redis is the operational control plane, not the long-term data store.
- [ ] Raw snapshots and normalized parsing are separate stages. Parser replays must work without new network requests.
- [ ] Unsupported routes must become capability observations, not endless retry loops.
- [ ] The first production slice must fully support `football`, `basketball`, and `tennis` before we roll out the remaining 10 sports.

## Supported Sports Scope

- [ ] `football`
- [ ] `tennis`
- [ ] `basketball`
- [ ] `volleyball`
- [ ] `baseball`
- [ ] `american-football`
- [ ] `handball`
- [ ] `table-tennis`
- [ ] `ice-hockey`
- [ ] `rugby`
- [ ] `cricket`
- [ ] `futsal`
- [ ] `esports`

## Current Codebase Anchors

Keep and extend these files instead of replacing them blindly:

- [ ] `schema_inspector/runtime.py`
- [ ] `schema_inspector/proxy.py`
- [ ] `schema_inspector/challenge.py`
- [ ] `schema_inspector/transport.py`
- [ ] `schema_inspector/fetch.py`
- [ ] `schema_inspector/sofascore_client.py`
- [ ] `schema_inspector/sport_profiles.py`
- [ ] `schema_inspector/endpoints.py`
- [ ] `schema_inspector/local_api_server.py`
- [ ] `schema_inspector/local_swagger_builder.py`
- [ ] Existing parser/job/repository families remain available during migration:
  - `categories_seed_*`
  - `competition_*`
  - `event_list_*`
  - `event_detail_*`
  - `entities_*`
  - `leaderboards_*`
  - `standings_*`
  - `statistics_*`

## Phase 0 - Freeze Current Truth and Define the Migration Boundary

**Objective:** Stabilize what already works so we can build the new backbone beside it.

**Modify / create**

- [ ] `schema_inspector/sport_profiles.py`
- [ ] `schema_inspector/endpoints.py`
- [ ] `docs/superpowers/plans/2026-04-16-sofascore-hybrid-etl-implementation-plan.md`
- [ ] `reports/API_DOCS_FOUNDATION_13_SPORTS_2026_04_16.md`
- [ ] `reports/SPORT_XHR_PATTERN_WALKTHROUGH_2026_04_16.md`
- [ ] `reports/COLLECTOR_ARTIFACTS_ANALYSIS_2026_04_16.md`

**Tasks**

- [ ] Freeze the current transport contract and explicitly document that all new jobs must use it unchanged.
- [ ] Freeze the list of 13 supported sports and confirm the target slugs in code.
- [ ] Promote the current reports to canonical input references for the implementation phases.
- [ ] Mark the existing domain-specific loader families as "legacy-compatible but not the final orchestration surface".
- [ ] Add comments or docstrings where needed to make the migration boundary obvious to future contributors.

**Verification**

- [ ] `.\.venv311\Scripts\python.exe -m pytest tests\test_sport_profiles.py tests\test_local_api_server.py tests\test_local_swagger_builder.py`

**Exit criteria**

- [ ] The repository has one approved migration plan and the team can point to a single implementation path.

**Commit command**

- [ ] `git add docs/superpowers/plans/2026-04-16-sofascore-hybrid-etl-implementation-plan.md schema_inspector/sport_profiles.py schema_inspector/endpoints.py reports/*.md`
- [ ] `git commit -m "docs: define hybrid etl implementation plan"`

## Phase 1 - Extend Registry and Sport Profile Coverage to All 13 Sports

**Objective:** Turn the current football/basketball/tennis-heavy registry into the canonical contract surface for all target sports.

**Modify / create**

- [ ] `schema_inspector/sport_profiles.py`
- [ ] `schema_inspector/endpoints.py`
- [ ] `schema_inspector/__init__.py`
- [ ] `tests/test_sport_profiles.py`
- [ ] `tests/test_local_api_server.py`
- [ ] `tests/test_local_swagger_builder.py`
- [ ] `tests/test_schema_inspector.py`

**Tasks**

- [ ] Add explicit `SportProfile` definitions for all 13 sports instead of relying on the generic fallback for most of them.
- [ ] Encode archetype-specific defaults:
  - `overall` widgets for football-like sports
  - `regularSeason` widgets for basketball/baseball/ice-hockey/american-football candidates
  - tennis/table-tennis thin profiles
  - cricket/esports thin/unusual profiles
- [ ] Expand endpoint families in `schema_inspector/endpoints.py` to include:
  - `live-categories`
  - `live-tournaments`
  - `finished-upcoming-tournaments`
  - `calendar months-with-events`
  - `season rounds`
  - `events/last`
  - `events/next`
  - event `weather`
  - event `graph/sequence`
  - sport/category odds branches
  - sport-specific routes for tennis, baseball, hockey, esports
- [ ] Keep `LOCAL_API_SUPPORTED_SPORTS` as the initial pilot trio until the new pipeline proves stable.
- [ ] Ensure endpoint registry entries expose stable `pattern`, `envelope_key`, and notes for planner/parser use.

**Verification**

- [ ] `.\.venv311\Scripts\python.exe -m pytest tests\test_sport_profiles.py tests\test_local_api_server.py tests\test_local_swagger_builder.py tests\test_schema_inspector.py`

**Exit criteria**

- [ ] Every target sport resolves to an explicit profile with known discovery and season/event behavior.
- [ ] Endpoint registry can describe the required route families for the 13-sport program.

**Commit command**

- [ ] `git add schema_inspector/sport_profiles.py schema_inspector/endpoints.py tests/test_sport_profiles.py tests/test_local_api_server.py tests/test_local_swagger_builder.py tests/test_schema_inspector.py`
- [ ] `git commit -m "feat: expand sport registry for 13-sport hybrid etl"`

## Phase 2 - Add Control-Plane Data Model in PostgreSQL

**Objective:** Create the raw, audit, capability, and head-pointer tables required for replayable hybrid ETL.

**Modify / create**

- [ ] `schema_inspector/db_setup_cli.py`
- [ ] `schema_inspector/schema.py`
- [ ] `schema_inspector/storage/__init__.py`
- [ ] `schema_inspector/storage/raw_repository.py`
- [ ] `schema_inspector/storage/job_repository.py`
- [ ] `schema_inspector/storage/capability_repository.py`
- [ ] `schema_inspector/storage/live_state_repository.py`
- [ ] `tests/test_db_setup_cli.py`
- [ ] `tests/test_storage_raw_repository.py`
- [ ] `tests/test_storage_job_repository.py`
- [ ] `tests/test_storage_capability_repository.py`

**Tasks**

- [ ] Add DDL for:
  - `api_request_log`
  - `api_payload_snapshot`
  - `api_snapshot_head`
  - `etl_job_run`
  - `etl_job_effect`
  - `etl_replay_log`
  - `endpoint_capability_observation`
  - `endpoint_capability_rollup`
  - `event_live_state_history`
  - `event_terminal_state`
- [ ] Keep the current normalized tables intact while adding the new control-plane tables.
- [ ] Implement repository modules dedicated to raw snapshots, job runs, and capability rollups instead of mixing them into domain-specific repositories.
- [ ] Make snapshot writes append-only and head updates idempotent.
- [ ] Ensure repository interfaces accept `trace_id`, `job_id`, `endpoint_pattern`, `context ids`, `payload_hash`, `http_status`, and parser/runtime metadata.

**Verification**

- [ ] `.\.venv311\Scripts\python.exe -m pytest tests\test_db_setup_cli.py tests\test_storage_raw_repository.py tests\test_storage_job_repository.py tests\test_storage_capability_repository.py`

**Exit criteria**

- [ ] PostgreSQL can store raw snapshots, run history, live history, and capability observations without disrupting current domain ingestion tables.

**Commit command**

- [ ] `git add schema_inspector/db_setup_cli.py schema_inspector/schema.py schema_inspector/storage tests/test_db_setup_cli.py tests/test_storage_raw_repository.py tests/test_storage_job_repository.py tests/test_storage_capability_repository.py`
- [ ] `git commit -m "feat: add hybrid etl control-plane storage"`

## Phase 3 - Introduce Redis Streams, Delayed Scheduler, Locks, and Shared Proxy State

**Objective:** Build the operational layer required for HA orchestration and live tracking.

**Modify / create**

- [ ] `schema_inspector/queue/__init__.py`
- [ ] `schema_inspector/queue/streams.py`
- [ ] `schema_inspector/queue/delayed.py`
- [ ] `schema_inspector/queue/leases.py`
- [ ] `schema_inspector/queue/dedupe.py`
- [ ] `schema_inspector/queue/live_state.py`
- [ ] `schema_inspector/queue/proxy_state.py`
- [ ] `schema_inspector/queue/ratelimit_state.py`
- [ ] `tests/test_queue_streams.py`
- [ ] `tests/test_queue_leases.py`
- [ ] `tests/test_queue_dedupe.py`
- [ ] `tests/test_queue_proxy_state.py`

**Tasks**

- [ ] Implement Redis Streams queues:
  - `stream:etl:discovery`
  - `stream:etl:hydrate`
  - `stream:etl:normalize`
  - `stream:etl:live_hot`
  - `stream:etl:live_warm`
  - `stream:etl:maintenance`
  - `stream:etl:dlq`
- [ ] Implement delayed job scheduling with `zset:etl:delayed`.
- [ ] Implement lease/lock helpers for jobs, live events, normalize runs, and entity hydration.
- [ ] Implement job dedupe and freshness dedupe helpers.
- [ ] Implement shared proxy and rate-limit state so all workers see the same cooldown information.
- [ ] Implement Redis-side live event state and hot/warm/cold indexes.

**Verification**

- [ ] `.\.venv311\Scripts\python.exe -m pytest tests\test_queue_streams.py tests\test_queue_leases.py tests\test_queue_dedupe.py tests\test_queue_proxy_state.py`

**Exit criteria**

- [ ] Redis can coordinate jobs, retries, leases, and live windows without storing business truth.

**Commit command**

- [ ] `git add schema_inspector/queue tests/test_queue_streams.py tests/test_queue_leases.py tests/test_queue_dedupe.py tests/test_queue_proxy_state.py`
- [ ] `git commit -m "feat: add redis control plane for hybrid etl"`

## Phase 4 - Build the Shared Fetch Executor on Top of the Existing Transport

**Objective:** Turn the current transport and fetch helpers into a single execution path for all new jobs.

**Modify / create**

- [ ] `schema_inspector/fetch_executor.py`
- [ ] `schema_inspector/fetch_models.py`
- [ ] `schema_inspector/fetch_classifier.py`
- [ ] `schema_inspector/fetch.py`
- [ ] `schema_inspector/transport.py`
- [ ] `schema_inspector/runtime.py`
- [ ] `schema_inspector/storage/raw_repository.py`
- [ ] `tests/test_fetch_executor.py`
- [ ] `tests/test_fetch_classifier.py`
- [ ] `tests/test_transport_retry_policy.py`

**Tasks**

- [ ] Introduce a `FetchTask` model containing:
  - `trace_id`
  - `job_id`
  - `sport_slug`
  - `endpoint_pattern`
  - `source_url`
  - `timeout_profile`
  - `context ids`
  - `expected_content_type`
  - `fetch_reason`
- [ ] Introduce a `FetchOutcomeEnvelope` model containing:
  - `http_status`
  - `classification`
  - `proxy_id`
  - `challenge_reason`
  - `snapshot_id`
  - `payload_hash`
  - `payload_root_keys`
  - `retry_recommended`
  - `capability_signal`
- [ ] Implement classifier outcomes:
  - `success_json`
  - `success_empty_json`
  - `soft_error_json`
  - `not_found`
  - `access_denied`
  - `rate_limited`
  - `challenge_detected`
  - `network_error`
  - `decode_error`
  - `unexpected_content`
- [ ] Preserve the existing proxy-first `curl_cffi` behavior from `schema_inspector/transport.py`.
- [ ] Write every request attempt to `api_request_log` and every meaningful response body to `api_payload_snapshot`.
- [ ] Keep transport retry separate from orchestrator/job retry.

**Verification**

- [ ] `.\.venv311\Scripts\python.exe -m pytest tests\test_fetch_executor.py tests\test_fetch_classifier.py tests\test_transport_retry_policy.py`

**Exit criteria**

- [ ] Every new orchestrated job can execute network fetches through one shared, audited, proxy-aware path.

**Commit command**

- [ ] `git add schema_inspector/fetch_executor.py schema_inspector/fetch_models.py schema_inspector/fetch_classifier.py schema_inspector/fetch.py schema_inspector/transport.py schema_inspector/runtime.py schema_inspector/storage/raw_repository.py tests/test_fetch_executor.py tests/test_fetch_classifier.py tests/test_transport_retry_policy.py`
- [ ] `git commit -m "feat: add shared fetch executor for hybrid etl"`

## Phase 5 - Add Job Envelope, Planner, and Worker Backbone

**Objective:** Replace ad-hoc domain entrypoints with one orchestration model that can schedule discovery, hydration, normalize, live, and maintenance work.

**Modify / create**

- [ ] `schema_inspector/jobs/__init__.py`
- [ ] `schema_inspector/jobs/envelope.py`
- [ ] `schema_inspector/jobs/types.py`
- [ ] `schema_inspector/planner/__init__.py`
- [ ] `schema_inspector/planner/planner.py`
- [ ] `schema_inspector/planner/rules.py`
- [ ] `schema_inspector/planner/live.py`
- [ ] `schema_inspector/workers/__init__.py`
- [ ] `schema_inspector/workers/discovery_worker.py`
- [ ] `schema_inspector/workers/hydrate_worker.py`
- [ ] `schema_inspector/workers/normalize_worker.py`
- [ ] `schema_inspector/workers/live_worker.py`
- [ ] `schema_inspector/workers/maintenance_worker.py`
- [ ] `tests/test_job_envelope.py`
- [ ] `tests/test_planner.py`
- [ ] `tests/test_live_planner.py`

**Tasks**

- [ ] Introduce the canonical `JobEnvelope` with:
  - `job_id`
  - `job_type`
  - `sport_slug`
  - `entity_type`
  - `entity_id`
  - `scope`
  - `params`
  - `priority`
  - `scheduled_at`
  - `attempt`
  - `parent_job_id`
  - `trace_id`
  - `capability_hint`
  - `idempotency_key`
- [ ] Encode the agreed job types:
  - `discover_sport_surface`
  - `expand_category`
  - `sync_unique_tournament`
  - `sync_season_index`
  - `sync_season_surface`
  - `sync_season_widget`
  - `discover_tournament_events`
  - `discover_event_surface`
  - `hydrate_event_root`
  - `hydrate_event_edge`
  - `hydrate_entity_profile`
  - `hydrate_entity_season`
  - `hydrate_special_route`
  - `track_live_event`
  - `refresh_live_event`
  - `finalize_event`
  - `normalize_snapshot`
  - `reconcile_capability`
  - `replay_failed_job`
- [ ] Implement planner rules that combine:
  - `SportProfile`
  - endpoint capability rollup
  - freshness checks
  - queue pressure
  - transport health
  - event live status
- [ ] Implement priority lanes:
  - P0 live hot and finalize
  - P1 new event core and near-start scheduled events
  - P2 season widgets and entity hydration
  - P3 cold backfill and revalidation
- [ ] Keep current `*_job.py` files functional while introducing worker entrypoints for the new queue-based pipeline.

**Verification**

- [ ] `.\.venv311\Scripts\python.exe -m pytest tests\test_job_envelope.py tests\test_planner.py tests\test_live_planner.py`

**Exit criteria**

- [ ] One planner can expand and schedule work for discovery, event hydration, normalize, and live maintenance without touching parsers or HTTP directly.

**Commit command**

- [ ] `git add schema_inspector/jobs schema_inspector/planner schema_inspector/workers tests/test_job_envelope.py tests/test_planner.py tests/test_live_planner.py`
- [ ] `git commit -m "feat: add planner and worker backbone"`

## Phase 6 - Introduce Parser Registry and Normalize Workers

**Objective:** Split raw ingestion from normalized parsing and make parser evolution replayable.

**Modify / create**

- [ ] `schema_inspector/parsers/__init__.py`
- [ ] `schema_inspector/parsers/base.py`
- [ ] `schema_inspector/parsers/registry.py`
- [ ] `schema_inspector/parsers/classifier.py`
- [ ] `schema_inspector/parsers/entities.py`
- [ ] `schema_inspector/parsers/relationships.py`
- [ ] `schema_inspector/parsers/families/sport_categories.py`
- [ ] `schema_inspector/parsers/families/category_unique_tournaments.py`
- [ ] `schema_inspector/parsers/families/unique_tournament.py`
- [ ] `schema_inspector/parsers/families/season_info.py`
- [ ] `schema_inspector/parsers/families/season_standings.py`
- [ ] `schema_inspector/parsers/families/season_widgets.py`
- [ ] `schema_inspector/parsers/families/event_root.py`
- [ ] `schema_inspector/parsers/families/event_statistics.py`
- [ ] `schema_inspector/parsers/families/event_lineups.py`
- [ ] `schema_inspector/parsers/families/event_incidents.py`
- [ ] `schema_inspector/parsers/families/event_graph.py`
- [ ] `schema_inspector/parsers/families/entity_profiles.py`
- [ ] `schema_inspector/parsers/families/entity_season_statistics.py`
- [ ] `schema_inspector/parsers/sports/football.py`
- [ ] `schema_inspector/parsers/sports/basketball.py`
- [ ] `schema_inspector/parsers/sports/tennis.py`
- [ ] `schema_inspector/parsers/special/tennis_point_by_point.py`
- [ ] `schema_inspector/parsers/special/tennis_power.py`
- [ ] `schema_inspector/normalizers/__init__.py`
- [ ] `schema_inspector/normalizers/worker.py`
- [ ] `tests/test_parser_registry.py`
- [ ] `tests/test_parsers_event_root.py`
- [ ] `tests/test_parsers_event_statistics.py`
- [ ] `tests/test_parsers_event_lineups.py`
- [ ] `tests/test_parsers_tennis_special.py`

**Tasks**

- [ ] Implement snapshot classification based on `endpoint_pattern`, `sport_slug`, and observed root keys.
- [ ] Add a generic entity extractor for:
  - `sport`
  - `category`
  - `uniqueTournament`
  - `tournament`
  - `season`
  - `event`
  - `team`
  - `player`
  - `manager`
  - `venue`
- [ ] Return structured `ParseResult` objects with:
  - parser family
  - parser version
  - parse status
  - entity upserts
  - relation upserts
  - warnings
  - unsupported fragments
- [ ] Allow `parsed`, `parsed_empty`, `partially_parsed`, `soft_error_payload`, `unsupported_shape`, and `failed`.
- [ ] Keep the current domain-specific parser modules during transition, but make the new registry the long-term path.
- [ ] Ensure normalize workers read from raw snapshots and write to normalized tables without making network calls.

**Verification**

- [ ] `.\.venv311\Scripts\python.exe -m pytest tests\test_parser_registry.py tests\test_parsers_event_root.py tests\test_parsers_event_statistics.py tests\test_parsers_event_lineups.py tests\test_parsers_tennis_special.py`

**Exit criteria**

- [ ] Raw snapshots are replayable through versioned parsers and normalized writes no longer depend on direct fetch-time parsing.

**Commit command**

- [ ] `git add schema_inspector/parsers schema_inspector/normalizers tests/test_parser_registry.py tests/test_parsers_event_root.py tests/test_parsers_event_statistics.py tests/test_parsers_event_lineups.py tests/test_parsers_tennis_special.py`
- [ ] `git commit -m "feat: add parser registry and normalize workers"`

## Phase 7 - Pilot the New Backbone on Football, Basketball, and Tennis

**Objective:** Prove the new hybrid architecture on the three sports that cover rich generic, regular-season, and special-route behavior.

**Modify / create**

- [ ] `schema_inspector/pipeline/pilot_orchestrator.py`
- [ ] `schema_inspector/pipeline/pilot_cli.py`
- [ ] `tests/test_hybrid_pipeline_football.py`
- [ ] `tests/test_hybrid_pipeline_basketball.py`
- [ ] `tests/test_hybrid_pipeline_tennis.py`
- [ ] `tests/test_replay_pipeline.py`
- [ ] `tests/test_capability_rollup.py`

**Tasks**

- [ ] Run the new planner/fetch/normalize flow for `football`.
- [ ] Run the new planner/fetch/normalize flow for `basketball`.
- [ ] Run the new planner/fetch/normalize flow for `tennis`, including `point-by-point` and `tennis-power`.
- [ ] Validate:
  - discovery
  - event core
  - team/player/manager hydration
  - season widgets where relevant
  - live refresh behavior
  - post-finish finalization
- [ ] Compare pilot outputs against current legacy loaders and local API expectations.
- [ ] Populate initial capability rollups from observed outcomes rather than assumptions alone.

**Verification**

- [ ] `.\.venv311\Scripts\python.exe -m pytest tests\test_hybrid_pipeline_football.py tests\test_hybrid_pipeline_basketball.py tests\test_hybrid_pipeline_tennis.py tests\test_replay_pipeline.py tests\test_capability_rollup.py`

**Exit criteria**

- [ ] The new hybrid backbone can ingest and replay the pilot trio without depending on the legacy end-to-end loader path.

**Commit command**

- [ ] `git add schema_inspector/pipeline tests/test_hybrid_pipeline_football.py tests/test_hybrid_pipeline_basketball.py tests/test_hybrid_pipeline_tennis.py tests/test_replay_pipeline.py tests/test_capability_rollup.py`
- [ ] `git commit -m "feat: pilot hybrid etl on football basketball and tennis"`

## Phase 8 - Expand by Archetype Instead of by One-Off Scripts

**Objective:** Roll the remaining 10 sports into the backbone using profiles, adapters, and capability rollups instead of separate ETL scripts.

**Modify / create**

- [ ] `schema_inspector/parsers/sports/handball.py`
- [ ] `schema_inspector/parsers/sports/volleyball.py`
- [ ] `schema_inspector/parsers/sports/baseball.py`
- [ ] `schema_inspector/parsers/sports/ice_hockey.py`
- [ ] `schema_inspector/parsers/sports/american_football.py`
- [ ] `schema_inspector/parsers/sports/rugby.py`
- [ ] `schema_inspector/parsers/sports/cricket.py`
- [ ] `schema_inspector/parsers/sports/table_tennis.py`
- [ ] `schema_inspector/parsers/sports/futsal.py`
- [ ] `schema_inspector/parsers/sports/esports.py`
- [ ] `schema_inspector/parsers/special/baseball_innings.py`
- [ ] `schema_inspector/parsers/special/baseball_pitches.py`
- [ ] `schema_inspector/parsers/special/shotmap.py`
- [ ] `schema_inspector/parsers/special/esports_games.py`
- [ ] `tests/test_hybrid_pipeline_handball.py`
- [ ] `tests/test_hybrid_pipeline_baseball.py`
- [ ] `tests/test_hybrid_pipeline_ice_hockey.py`
- [ ] `tests/test_hybrid_pipeline_esports.py`

**Tasks**

- [ ] Expand football-like adapters to `handball`, `volleyball`, `rugby`, and `futsal`.
- [ ] Expand regular-season team adapters to `baseball`, `ice-hockey`, and `american-football`.
- [ ] Expand thin/special adapters to `cricket`, `table-tennis`, and `esports`.
- [ ] Add special route parsing only where the route family truly differs from the generic event model.
- [ ] Keep capability rollups sport-specific so unsupported routes stop spawning noise.
- [ ] Verify each archetype with one deep reference sport and one or more delta sports.

**Verification**

- [ ] `.\.venv311\Scripts\python.exe -m pytest tests\test_hybrid_pipeline_handball.py tests\test_hybrid_pipeline_baseball.py tests\test_hybrid_pipeline_ice_hockey.py tests\test_hybrid_pipeline_esports.py`

**Exit criteria**

- [ ] All 13 sports run through the same orchestrator/fetch/parser backbone with sport-specific adapters instead of separate scripts.

**Commit command**

- [ ] `git add schema_inspector/parsers/sports schema_inspector/parsers/special tests/test_hybrid_pipeline_handball.py tests/test_hybrid_pipeline_baseball.py tests/test_hybrid_pipeline_ice_hockey.py tests/test_hybrid_pipeline_esports.py`
- [ ] `git commit -m "feat: expand hybrid etl across remaining sport archetypes"`

## Phase 9 - Cut Over, Harden Operations, and Retire Legacy Entry Points

**Objective:** Make the new hybrid ETL the production path and reduce the old loader sprawl to compatibility wrappers or archived modules.

**Modify / create**

- [ ] `schema_inspector/cli.py`
- [ ] `schema_inspector/bootstrap_pipeline_cli.py`
- [ ] `schema_inspector/full_backfill_cli.py`
- [ ] `schema_inspector/current_year_pipeline_cli.py`
- [ ] `schema_inspector/targeted_pipeline_cli.py`
- [ ] `schema_inspector/ops/__init__.py`
- [ ] `schema_inspector/ops/health.py`
- [ ] `schema_inspector/ops/recovery.py`
- [ ] `schema_inspector/ops/metrics.py`
- [ ] `tests/test_ops_recovery.py`
- [ ] `tests/test_live_recovery.py`

**Tasks**

- [ ] Add production entry points for:
  - orchestrator
  - worker processes
  - normalize workers
  - maintenance/recovery jobs
- [ ] Convert old CLIs into wrappers that either call the new backbone or are clearly marked as legacy/admin-only.
- [ ] Implement recovery jobs for:
  - replaying stale stream entries
  - rebuilding delayed queues
  - rebuilding Redis live state from PostgreSQL
  - replaying DLQ windows
  - reconciling proxy cooldown state
- [ ] Add metrics and health reporting for queue lag, live load, challenge rate, snapshot ingest, normalize throughput, and DLQ size.
- [ ] Document degraded-mode behavior when source access gets tighter or proxy pressure rises.

**Verification**

- [ ] `.\.venv311\Scripts\python.exe -m pytest tests\test_ops_recovery.py tests\test_live_recovery.py tests\test_local_api_server.py tests\test_local_swagger_builder.py`

**Exit criteria**

- [ ] The hybrid orchestrator is the primary production path and the old script surface no longer defines architecture by accident.

**Commit command**

- [ ] `git add schema_inspector/cli.py schema_inspector/bootstrap_pipeline_cli.py schema_inspector/full_backfill_cli.py schema_inspector/current_year_pipeline_cli.py schema_inspector/targeted_pipeline_cli.py schema_inspector/ops tests/test_ops_recovery.py tests/test_live_recovery.py`
- [ ] `git commit -m "refactor: cut over to hybrid etl control plane"`

## Reliability and Testing Gates

- [ ] Unit tests must exist for planner rules, fetch classification, queue helpers, and parser family logic.
- [ ] Replay tests must prove that raw snapshots can be normalized again without network fetches.
- [ ] Failure injection must explicitly cover:
  - `403`
  - `429`
  - timeout
  - challenge-like HTML
  - empty but valid JSON payloads
  - soft error JSON payloads
  - Redis restart
  - worker crash while holding a lease
- [ ] Live-state tests must cover:
  - scheduled -> warm
  - warm -> hot
  - hot -> finished
  - finished -> finalize
- [ ] Capability rollup tests must prove repeated unsupported routes stop causing noisy retries.

## Suggested Execution Order for Option 2 (Inline Execution)

- [ ] Execute Phase 1 and Phase 2 in one implementation wave.
- [ ] Pause for review and schema sanity-check.
- [ ] Execute Phase 3, Phase 4, and the data models for Phase 5.
- [ ] Pause for queue/fetch integration review.
- [ ] Execute Phase 5 worker flow and Phase 6 parser registry for the pilot trio.
- [ ] Pause for end-to-end pilot validation.
- [ ] Execute Phase 7 pilot runs.
- [ ] Expand through Phase 8 only after the pilot trio is stable.
- [ ] Execute Phase 9 only when the new path demonstrably replaces the legacy critical path.

## Self-Review Checklist

- [ ] The plan does not create 13 isolated ETL scripts.
- [ ] The plan preserves the existing proxy-first transport behavior.
- [ ] The plan separates raw ingestion from normalized parsing.
- [ ] The plan gives PostgreSQL and Redis clear, non-overlapping roles.
- [ ] The plan contains exact file paths for new and modified modules.
- [ ] The plan starts with the current codebase instead of pretending we are greenfield.
- [ ] The plan uses football, basketball, and tennis as the proof set before expanding further.
- [ ] The plan keeps recovery, retries, and capability observations first-class from the start.

## When Implementing This Plan

- [ ] Prefer small vertical slices over a giant refactor.
- [ ] Keep old loaders available until the new slice is verified.
- [ ] Use feature flags or pilot-only entrypoints where needed.
- [ ] Do not merge a phase without its verification commands and a short written comparison against the legacy behavior it is replacing.
