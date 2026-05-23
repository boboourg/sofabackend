# Historical Bootstrap Stream Split (Phase 4.7.7)

**Date:** 2026-05-23

## Problem

Bootstrap publishes (pending → events_loaded transitions on `tournament_season_upstream_catalog`) share `stream:etl:historical_tournament` + `cg:historical_tournament` with cursor walks (full archive runs). They lose the FIFO race.

Measured on prod (2026-05-23, HEAD `641c8f7`):

- `cg:historical_tournament` — 4 workers, ~7000 jobs/24h processed, of which only **40 = bootstrap-mode successes** (events_loaded transitions). Bootstrap effective rate: **0.5%** of throughput.
- catalog state: pending=19251, events_loaded=26058, fully_processed=60. At 1.7 events_loaded/hour drain rate → **~470 days** to drain pending.
- Cursor walks dominate worker time: each full archive job is minutes-to-tens-of-minutes (competition + season + statistics + leaderboards + event_list pagination across 7+ rounds + season_last × 8 pages). Bootstrap publishes (lightweight event-list only) physically cannot reach workers because they always sit in the tail behind heavier cursor jobs the planner keeps publishing.
- `_publish_bootstrap_batch` (Phase 3.7a, 2026-05-22) already publishes pending jobs into the shared stream — but selective backpressure cannot rescue throughput when consumers are saturated by another job class in the same FIFO.

## Goal

Drain catalog `pending` rows at a measurable rate (target: ≥500/hour, ≥100× current) by isolating bootstrap workload onto a dedicated stream + consumer group + worker pool. Establish observability for bootstrap publish/claim/done so future regressions are immediately visible.

## Scope

This slice adds a second tournament-archive lane targeted only at bootstrap-mode catalog drain:

- new Redis stream `stream:etl:historical_bootstrap`
- new consumer group `cg:historical_bootstrap`
- new worker class `HistoricalBootstrapWorkerService` (consumer of the new stream)
- new CLI subcommand `worker-historical-bootstrap`
- planner wiring: `HistoricalTournamentPlannerDaemon` accepts a `bootstrap_stream` kwarg; `_publish_bootstrap_batch` publishes to `bootstrap_stream` instead of `self.stream`
- structured logging on planner publish ticks and worker claim/done/fail
- new systemd unit template `sofascore-historical-bootstrap@.service`

## Non-Goals

- **No XTRIM** of legacy `stream:etl:historical_tournament` backlog. `cg:historical_tournament` already advanced past those messages (last-delivered-id is well ahead of head). Trimming would reduce disk/memory but does not change throughput. Out of scope here — a separate maintenance task if disk pressure appears.
- **No change to bootstrap-mode dispatch logic in `HistoricalTournamentWorker`**. The full archive worker keeps reading both stream classes via `cg:historical_tournament`. Only the dedicated bootstrap worker gets the new stream.
- **No change to selective backpressure semantics**. Cursor publishes still pause on enrichment lag; bootstrap publishes still bypass. Both rules apply to whichever stream the planner targets.
- **No `max_bootstrap_jobs_per_tick` retuning**. Current default (20) stays; we expect the new worker pool to keep up. Operator can dial via `SOFASCORE_MAX_BOOTSTRAP_JOBS_PER_TICK` if needed.
- **No catalog-state index optimization** (`bootstrap_state='pending'` selector seq-scans 45k rows in ~580ms). In-budget for 10-second tick interval; future optimization if it becomes a bottleneck.
- **No change to operational `hydrate` / live lanes**. Independent crises.

## Architecture

### Streams and groups

Add to `schema_inspector/queue/streams.py`:

```python
STREAM_HISTORICAL_BOOTSTRAP = "stream:etl:historical_bootstrap"
GROUP_HISTORICAL_BOOTSTRAP = "cg:historical_bootstrap"
```

Add to `HISTORICAL_CONSUMER_GROUPS` tuple so `recover-live-state` and ops surface auto-create the group.

### Planner change

`HistoricalTournamentPlannerDaemon.__init__` gets a new kwarg `bootstrap_stream: str = STREAM_HISTORICAL_TOURNAMENT` (default keeps backwards-compat for tests that don't wire the new stream). `_publish_bootstrap_batch` publishes to `self.bootstrap_stream` rather than `self.stream`.

When `ServiceApp.build_historical_tournament_planner` constructs the daemon, it passes `bootstrap_stream=STREAM_HISTORICAL_BOOTSTRAP`.

### Worker

New file `schema_inspector/workers/historical_bootstrap_worker.py`. `HistoricalBootstrapWorkerService` is a thin shell around `HistoricalTournamentWorker.handle` (the existing handler already inspects catalog state and dispatches in `bootstrap_mode=True` when state is `pending` — no new business logic needed). The difference is **what stream and group it reads from**.

Implementation: instantiate `HistoricalTournamentWorker` with `stream=STREAM_HISTORICAL_BOOTSTRAP`, `group=GROUP_HISTORICAL_BOOTSTRAP`. No subclass needed — just a builder/factory function. The worker's `enrichment_stream` stays unchanged so the (rare) case of a bootstrap job that flips to full mode mid-flight still works.

Safety: bootstrap jobs SHOULD always run in bootstrap mode (catalog state=pending → bootstrap_mode=True at handle time). If the catalog row no longer says `pending` (race: planner published, then another worker advanced state), the existing worker falls through to full archive mode — idempotent, just slower than expected. No corruption risk.

### CLI

Add subcommand `worker-historical-bootstrap` to `schema_inspector/cli.py`. Mirrors `worker-historical-tournament` argument shape (`--consumer-name X`, `--block-ms 5000`).

### Systemd

New unit template at `ops/systemd/sofascore-historical-bootstrap@.service` — copy of `sofascore-historical-tournament@.service` with `ExecStart` pointing to the new subcommand.

### Observability — structured logging

Today the planner emits only `Cursor publishes paused: lag=X>threshold`. There is no log when a bootstrap publish happens, no log when a bootstrap worker claims/completes a job. We add:

**Planner (`historical_tournament_planner.py`)**:
- `bootstrap_tick: sport=X published=N selector_returned=M cursor_paused=bool` (one line per tick per target, INFO)
- `bootstrap_publish: ut=X season=Y stream=...bootstrap job_id=Z` (one line per publish, DEBUG — high volume)

**Worker (`historical_bootstrap_worker.py`)**:
- `bootstrap_claim: msg_id=X consumer=Y ut=Z season=W` (per claim, INFO)
- `bootstrap_job_done: ut=X season=Y duration_ms=Z events_loaded=N` (per success, INFO)
- `bootstrap_job_failed: ut=X season=Y exc=...` (per failure, WARNING with trace)

A 30-second `journalctl -u 'sofascore-historical-bootstrap@*' | grep bootstrap_job_done | wc -l` gives instant throughput readout.

## Implementation slices

Each slice is one PR-sized commit. Tests added in the same commit as the production code (TDD: failing test first, then code).

1. **Stream constants** — `streams.py` + add to `HISTORICAL_CONSUMER_GROUPS`. 1 test (constants exist + included in group tuple).
2. **Planner accepts `bootstrap_stream` kwarg** — `historical_tournament_planner.py` + structured logging. 2-3 tests (default routes to legacy stream; explicit kwarg routes to new stream; tick emits `bootstrap_tick` log).
3. **Worker factory** — new `historical_bootstrap_worker.py` with `build_historical_bootstrap_worker(orchestrator, queue, consumer, ...)` factory. 2 tests (factory builds `HistoricalTournamentWorker` with correct stream/group; structured logs emitted on claim/done).
4. **CLI subcommand** — `cli.py` `worker-historical-bootstrap`. 1 test (subparser registered, handler resolves correctly).
5. **ServiceApp wiring** — `service_app.py` `build_historical_bootstrap_worker(consumer_name)`; pass `bootstrap_stream=STREAM_HISTORICAL_BOOTSTRAP` into planner factory. 1-2 tests (factory present; planner passes bootstrap_stream through).
6. **Systemd unit** — `ops/systemd/sofascore-historical-bootstrap@.service`. No test (config file).

Total: ~6 commits, ~10-12 unit tests, ~300-400 lines new code (mostly thin glue + logging).

## Deploy plan

1. Push commits to `main`, server `git pull --ff-only`.
2. `sudo systemctl daemon-reload` (new unit template).
3. `sudo systemctl enable --now sofascore-historical-bootstrap@1 sofascore-historical-bootstrap@2 sofascore-historical-bootstrap@3` (start with 3 workers; can scale 5+ once we measure per-job duration).
4. `sudo systemctl restart sofascore-historical-tournament-planner` (picks up new bootstrap_stream wiring).
5. Verify within 60s: `redis-cli XINFO GROUPS stream:etl:historical_bootstrap` shows the group with 3 consumers, lag dropping.
6. Verify within 5 min: `journalctl -u 'sofascore-historical-bootstrap@*' | grep bootstrap_job_done | wc -l` ≥ 10.
7. Verify within 60 min: `SELECT count(*) FROM tournament_season_upstream_catalog WHERE bootstrap_state='events_loaded' AND events_loaded_at > NOW() - INTERVAL '1 hour'` ≥ 50 (currently: ~1.7/hour).

## Rollback plan

`sudo systemctl stop sofascore-historical-bootstrap@*` + `sudo systemctl disable ...`. Planner keeps publishing to the new stream but nothing consumes — backlog accumulates harmlessly. Then revert the planner commit (or set env `SOFASCORE_MAX_BOOTSTRAP_JOBS_PER_TICK=0` to halt bootstrap publishes entirely while keeping the daemon up).

Catalog state is not corrupted by the rollback: pending rows stay pending, the eventual cursor walk picks them up the old way.

## Risks

- **Per-job duration unknown for bootstrap-mode runs at scale.** Today we have ~40 bootstrap successes/day — too few to characterise p95 duration. Mitigation: start with 3 workers, measure for 1 hour, then scale to 5-10 if drain rate is below target.
- **DB pool pressure.** Pool defaults are 3/10 per process (Phase 4.7.6). 3 bootstrap workers × 10 max = +30 connections to PG. Current PG state: 335/500 used. Room for +30, but worth watching after deploy.
- **Sofascore rate limiting / proxy capacity.** Bootstrap jobs do event-list fetches. More workers → more concurrent fetches. Today's pace is so low that the new workers will visibly bump request volume. If 429s appear, scale down or rely on existing retry/proxy rotation.
- **Selective backpressure interaction.** Cursor publishes pause when `STREAM_HISTORICAL_ENRICHMENT` lag > 10000. Bootstrap publishes bypass that gate (Phase 3.7a). The new dedicated workers also bypass — they should, since bootstrap jobs do not produce enrichment fan-out. No change needed but worth verifying via a unit test that the bypass survives the stream-split refactor.

## Open questions (for review)

1. Should the worker also expose a `--bootstrap-only` flag that hard-rejects any job whose catalog state is no longer `pending` (defensive — refuse to do full archive on the bootstrap lane)? **Default position: no, the existing fall-through is safer and idempotent. Add only if observed misuse.**
2. Should we emit a separate Prometheus counter for `bootstrap_job_done` (in addition to the log line) so `/ops/metrics` exposes throughput? **Default position: defer to a follow-up; structured logs are sufficient for the first iteration.**
