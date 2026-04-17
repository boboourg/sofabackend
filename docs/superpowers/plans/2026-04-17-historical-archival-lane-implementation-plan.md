# Historical Archival Lane Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a safe historical date-range control plane on separate streams so the project can backfill all 13 sports without disturbing the working operational contour.

**Architecture:** Reuse the existing hybrid discovery and hydrate workers, but wire them to dedicated archival Redis Streams and a new historical planner with a persisted per-sport date cursor. Keep the operational contour untouched and expose archival streams through the same operational monitoring endpoint.

**Tech Stack:** Python 3.11, asyncio, asyncpg, redis-py, Redis Streams, PostgreSQL, pytest

---

## File Structure

- Create: `D:\sofascore\schema_inspector\services\historical_planner.py`
- Modify: `D:\sofascore\schema_inspector\queue\streams.py`
- Modify: `D:\sofascore\schema_inspector\services\service_app.py`
- Modify: `D:\sofascore\schema_inspector\cli.py`
- Modify: `D:\sofascore\schema_inspector\local_api_server.py`
- Modify: `D:\sofascore\tests\test_hybrid_cli.py`
- Modify: `D:\sofascore\tests\test_local_api_server.py`
- Create: `D:\sofascore\tests\test_historical_planner.py`

## Task 1: Add archival stream constants and planner tests

- [ ] Write failing tests for archival stream constants and historical planner cursor progression.
- [ ] Run the targeted tests and watch them fail.
- [ ] Implement archival stream names and the historical planner/cursor store.
- [ ] Re-run the targeted tests and confirm green.

## Task 2: Add archival service wiring and CLI entrypoints

- [ ] Write failing CLI/service tests for `historical-planner-daemon`, `worker-historical-discovery`, `worker-historical-hydrate`, and `worker-historical-maintenance`.
- [ ] Run the targeted tests and watch them fail.
- [ ] Implement service wiring in `service_app.py` and CLI dispatch in `cli.py`.
- [ ] Re-run the targeted tests and confirm green.

## Task 3: Expose archival streams in queue monitoring

- [ ] Write a failing local API test that expects archival streams in `/ops/queues/summary`.
- [ ] Run the targeted test and watch it fail.
- [ ] Implement archival queue-group exposure in `local_api_server.py`.
- [ ] Re-run the targeted tests and confirm green.

## Task 4: Verify end-to-end safety

- [ ] Run targeted pytest files for planner, CLI, and local API.
- [ ] Run the full pytest suite.
- [ ] Commit with the historical-lane implementation and the docs.
