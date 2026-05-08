# P3.2 — Sportradar Coverage Matrix → Endpoint Pre-Filter (Design Doc)

**Status**: Draft v2 (post user-review 2026-05-08, ready for B)
**Date**: 2026-05-08
**Authors**: ETL team
**Supersedes**: nothing
**Related**: P0.2 (HEAD probe + soft-error scoping), P0.3 (env-driven thresholds), P1 (capacity tuning)

**Revision log**:
- v1 (initial): 15 gate-able endpoints, generic integration.
- v2 (this): Phase-1 hard-gate scoped to **11 endpoints** with reliable UT context;
  team-level paths moved to fail-open (no canonical UT); top-ratings/overall held in
  dry-run observation; planner-first integration; explicit `GateContextResolver`;
  rollback procedure expanded to include planner + workers.

---

## 1. Problem statement

Production HTTP success rate is **~91%**, with ~6,000 GET 404/hour and ~4,800 HEAD 404/hour. A material fraction of these 4xx are **structurally doomed**: Sofascore returns 404 because the requested endpoint has no underlying coverage from the upstream provider (Sportradar). Examples:

- `/event/{e}/lineups` for a 4th-tier amateur league → Sportradar `Lineups=0` → Sofascore endpoint structurally returns 404.
- `/season/{s}/top-players/overall` for a low-coverage cup → Sportradar `Leaders=0` → 404.

The **HEAD probe** (P0.2) reduced body-GET volume but still pays a HEAD round-trip per attempt. We can do better by **pre-filtering at planner level** based on Sportradar's published coverage matrix.

This document specifies the design for a **per-attribute Sportradar coverage gate** that classifies endpoint requests as `block` or `allow` based on:
1. Reconciled Sofascore `unique_tournament_id` ↔ Sportradar `sr:competition:N` mapping
2. Per-attribute coverage values (`Lineups=0`, `Leaders=2`, etc.)
3. The Sofascore endpoint → Sportradar attribute mapping (validated via OpenAPI spec — see `tools/sportradar_openapi_audit.py`)

## 2. Goals

- **Reduce 4xx volume** by ~3,500/hour (estimated from current 5,711 GET 404/hour × ~60% structurally doomed).
- **Increase HTTP success rate** from 91% to ≥95%.
- **Zero data loss**: no false-positive blocking that prevents fetching data we currently receive.
- **Reversible**: kill-switch and dry-run modes for safe rollout and instant rollback.
- **Observable**: structured metrics + logs for every gate decision.
- **Low maintenance**: monthly `sportradar_coverage` sync via systemd timer.

## 3. Non-goals

- Not extending to non-football sports in Phase 1 (basketball/tennis/ice-hockey deferred to Phase 2).
- Not replacing the existing per-event `endpoint_state_matrix` (status-based polling cadence). The two systems are **orthogonal**: `endpoint_state_matrix` decides *when* to fetch (per status), this gate decides *whether* to fetch (per league coverage).
- Not replacing the HEAD probe — it remains as defence-in-depth for endpoints **not** in this gate.
- Not solving 7-day historical lag (P1 territory).
- Not solving live `tier_3` plateau (P1 territory).

## 4. Scope

### 4.1 Sport scope: football-only in Phase 1

The Sportradar coverage matrix is **per-sport**. We have one xlsx file (`footballmatrix.xlsx`, 1,269 competitions). Other sports require separate matrices we don't have yet.

**Phase 1 scope (this doc)**:
- Football only (`sport_id = 1`).
- All other sports: gate is bypassed (fail-open, no behavior change).
- Implementation must respect this: gate code reads `event.sport_slug` (or upstream context) and short-circuits to fail-open when not football.

**Phase 2 (deferred)**:
- Basketball (`sport_id = 2`), Ice-hockey (`sport_id = 4`), Tennis (`sport_id = 5`), Cricket (`sport_id = 62`), American Football, Baseball.
- Each requires its own xlsx + reconciliation pass.
- Code structure designed to be sport-pluggable from day 1 (see Section 6).

### 4.2 Endpoint scope: 11 hard-gate + 1 dry-run-observation path_templates

Per `tools/sportradar_openapi_audit.py` v2 (after Phase-1 policy carve-outs):

```
TOTAL unique path_templates in prod registry:    170
TOTAL distinct patterns (incl. #phase variants): 244

  gate_able (hard_gate=YES, Phase 3 live):  11  ← Phase 1 active block
  dry_run_observation (Phase 3 dry-run only): 1  ← top-ratings/overall
  fail_open_override (policy):                 6  ← 3 original + 3 team-level (no UT context)
  sofascore_only (no SR equiv):               21
  odds providers:                              2
  unmapped (default fail-open):              129
  ─────────────────────────────────────────────
                                             170
```

**Critical terminology**:
- `path_template` = canonical Sofascore route (e.g. `/api/v1/event/{event_id}/lineups`).
- `pattern` = `path_template` + optional `#phase=...` qualifier (e.g. `#phase=inprogress`).
- **Gate decisions are at `path_template` level**: all `#phase` variants of one route share the same SR check. The 74 `#phase` variants in the registry don't add new gate decisions — they're per-status polling cadence registrations.

The **11 hard-gate path_templates** (Phase 3 active block):

| Pattern | required_attr | Context source | Reason |
|---------|---------------|----------------|--------|
| `/api/v1/event/{event_id}/h2h` | Head2Head | event_id → ut_id via DB | Maps to SR `/competitors/.../versus/.../summaries` |
| `/api/v1/event/{event_id}/incidents` | Basic Play by Play | event_id → ut_id via DB | Maps to SR `/sport_events/.../timeline` |
| `/api/v1/event/{event_id}/lineups` | Lineups | event_id → ut_id via DB | Maps to SR `/sport_events/.../lineups` |
| `/api/v1/event/{event_id}/statistics` | Basic Statistics | event_id → ut_id via DB | Maps to SR `/sport_events/.../summary` |
| `/api/v1/unique-tournament/.../season/.../player-of-the-season` | Leaders | path params | Maps to SR `/seasons/.../leaders` |
| `/api/v1/unique-tournament/.../season/.../top-players/overall` | Leaders | path params | Same |
| `/api/v1/unique-tournament/.../season/.../top-players/regularSeason` | Leaders | path params | Same |
| `/api/v1/unique-tournament/.../season/.../top-players-per-game/all/overall` | Leaders | path params | Same |
| `/api/v1/unique-tournament/.../season/.../top-players-per-game/all/regularSeason` | Leaders | path params | Same |
| `/api/v1/unique-tournament/.../season/.../top-teams/overall` | Leaders | path params | Same |
| `/api/v1/unique-tournament/.../season/.../top-teams/regularSeason` | Leaders | path params | Same |

The **1 dry-run-observation path_template** (Phase 3 logs would-block, no actual block; promote to live in Phase 4 after 30-day analysis):

| Pattern | required_attr | Reason for dry-run-only |
|---------|---------------|--------------------------|
| `/api/v1/unique-tournament/.../season/.../top-ratings/overall` | Leaders | Insufficient probe history; observe ≥30 days before graduating |

The **6 fail-open-override path_templates** (mapped to SR but **never** hard-gated):

| Pattern | Reason |
|---------|--------|
| `/api/v1/event/{event_id}` | Root event endpoint — always-useful, never gate |
| `/api/v1/player/{player_id}` | Player profile — not a Squads endpoint |
| `/api/v1/team/{team_id}/players` | No season context in Sofascore endpoint |
| `/api/v1/team/{team_id}` | **No canonical UT context** — team plays in multiple tournaments; Competitor Profile = 100% covered anyway → gate would be no-op. See §6.3 GateContextResolver for Phase 2 plan. |
| `/api/v1/team/{team_id}/events/last/{page}` | **No canonical UT context** — Schedules = 100% covered, gate would be no-op |
| `/api/v1/team/{team_id}/events/next/{page}` | **No canonical UT context** — same |

**Non-gated total** (158 path_templates): 6 fail-open-override + 21 sofascore_only + 2 odds + 129 unmapped. All bypassed by gate at runtime.

## 5. Architecture overview

```
┌─ Planner-level gate (PRIMARY) ─────────────────────────────────────┐
│  PilotOrchestrator.plan_*_widgets():                               │
│    for candidate in planned_jobs:                                  │
│        ctx = resolver.resolve(candidate)                           │
│        if not gate.should_fetch(candidate, ctx):                   │
│            metric.inc(result="block", layer="planner")             │
│            continue   # do NOT publish to stream                   │
│        publish(candidate)                                          │
└────────────────────────────────────────────────────────────────────┘
                          │
                          ▼
                Redis Streams (skipped jobs not enqueued)
                          │
                          ▼
┌─ Worker-level gate (DEFENSE-IN-DEPTH) ─────────────────────────────┐
│  ResourceRefreshWorker.handle(entry):                              │
│    task = decode(entry)                                            │
│    ctx = resolver.resolve(task)                                    │
│    if not gate.should_fetch(task, ctx):                            │
│        metric.inc(result="block", layer="worker")                  │
│        ack_skipped()                                               │
│        return                                                      │
│    fetch_executor.execute(task)                                    │
└────────────────────────────────────────────────────────────────────┘
                          │
                          ▼
                    fetch_executor (HEAD probe + GET)
```

**Why two layers**: Planner-level gate is primary — saves stream space, avoids
backpressure on already-doomed jobs. Worker-level is defense-in-depth — catches
jobs that were enqueued before kill-switch flip, or before policy change took
effect across all planner instances.

## 5.5 Integration points (locked)

**Primary integration: planner-level**

Hooks into all job-publishing call sites:
- `schema_inspector/services/planner_daemon.py` — scheduled refresh planning
- `schema_inspector/services/live_discovery_planner.py` — live event discovery
- `schema_inspector/pipeline/pilot_orchestrator.py` — orchestrator's plan_event_widgets, plan_season_widgets
- `schema_inspector/services/leaderboards_backfill.py` — backfill CLI publishing

Each call site, before `queue.publish(stream, job)`, passes the candidate through:

```python
ctx = await self._gate_resolver.resolve(job)
if not self._sportradar_gate.should_fetch(job, ctx):
    self._metrics.gate_blocked.inc(layer="planner", attr=ctx.required_attr or "?")
    continue  # skip publish
self._metrics.gate_allowed.inc(layer="planner")
queue.publish(stream, job)
```

**Secondary integration: worker-level (defense-in-depth)**

Hooks into worker entry points:
- `schema_inspector/workers/resource_refresh_worker.py` — main suspect (hot path for season-level gate)
- `schema_inspector/workers/discovery_worker.py` — for hydrate fan-out

Worker re-checks the same gate. If gate blocks: `ack` the message, write `etl_job_run`
status `succeeded_skipped_by_gate` (NOT `failed`), increment metric.

**NOT hooked** (no need): live_worker_service (live tier 1/2/3) — those don't fetch
gate-able endpoints directly (they fetch via orchestrator which is already covered).

## 6. Data model

### 6.1 New table: `sportradar_coverage`

```sql
-- migrations/2026-05-08_sportradar_coverage.sql
CREATE TABLE IF NOT EXISTS sportradar_coverage (
  sr_competition_id BIGINT PRIMARY KEY,
  sr_competition_name TEXT NOT NULL,
  sr_category_name TEXT NOT NULL,             -- for name-match reconciliation
  sport_slug TEXT NOT NULL DEFAULT 'football',-- pluggable for Phase 2

  -- Reconciliation result
  unique_tournament_id BIGINT REFERENCES unique_tournament(id),
  reconciliation_method TEXT NOT NULL CHECK (
    reconciliation_method IN ('blind_id', 'name_match', 'manual', 'unmatched')
  ),
  reconciliation_confidence NUMERIC(3,2) NOT NULL CHECK (
    reconciliation_confidence >= 0 AND reconciliation_confidence <= 1
  ),

  -- Coverage data (source of truth for gate decisions)
  coverage_attrs JSONB NOT NULL,              -- {"Live": 2, "Lineups": 0, ...}
  tier INTEGER,                               -- info-only, NEVER read by gate
  stage_coverage JSONB,                       -- per-simple-tournament booleans (future)
  season_id BIGINT,                           -- SR's active-season id (cross-check)
  season_start_date TIMESTAMPTZ,
  last_changed TIMESTAMPTZ NOT NULL,          -- incremental sync watermark
  synced_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX sportradar_coverage_ut_idx
  ON sportradar_coverage(unique_tournament_id)
  WHERE unique_tournament_id IS NOT NULL;

CREATE INDEX sportradar_coverage_method_idx
  ON sportradar_coverage(reconciliation_method, reconciliation_confidence);

CREATE INDEX sportradar_coverage_last_changed_idx
  ON sportradar_coverage(last_changed);

-- Optional materialized view for fast gate lookup, refreshed by sync job
-- (skipped in v1; benchmark first whether the index above is enough)
```

### 6.2 Reconciliation pipeline

Per the v2 audit + earlier 100-sample probe results:
- **Blind ID match accuracy for football top-tier**: 100% (12/12 of UEFA/PL/LaLiga/etc. exactly match)
- **Blind ID match accuracy overall**: ~97% (3 known collisions for Saudi/Bahrain cups)
- **Name match recovery for collisions**: feasible via fuzzy name + country triplet

**Pipeline**:

```python
async def reconcile_sportradar_to_sofascore() -> ReconcileReport:
    for sr_row in sportradar_competitions:  # from xlsx + API
        # Phase 1: blind ID match
        ut = SELECT * FROM unique_tournament
             WHERE id = sr_row.id
             AND category JOIN sport WHERE sport.slug = sr_row.sport_slug
             AND fuzz_ratio(sr_row.name, ut.name) > 0.5  # sanity check
        if ut found and name_similarity > 0.9:
            method = 'blind_id'
            confidence = 1.0

        # Phase 2: name match (if blind failed)
        elif ut := find_by_name_country(sr_row.name, sr_row.category, sport):
            method = 'name_match'
            confidence = name_similarity_score  # 0.7-0.95

        # Phase 3: manual override
        elif sr_row.id in MANUAL_OVERRIDES:
            ut = MANUAL_OVERRIDES[sr_row.id]
            method = 'manual'
            confidence = 1.0

        # Phase 4: unmatched
        else:
            ut = None
            method = 'unmatched'
            confidence = 0.0

        UPSERT sportradar_coverage VALUES (...)
```

**Manual override config**: `config/sportradar_id_overrides.yaml`

```yaml
overrides:
  football:
    # Sofascore_id: Sportradar_id  # comment
    2058: 2110  # Sof "King's Cup" (Saudi) → SR "Kings Cup" (Saudi Arabia)
    2119: 2296  # Sof "Saudi Super Cup" → SR "Super Cup" (Saudi Arabia)
    2736: 2462  # Sof "King's Cup" (Bahrain) → SR "King of Bahrain Cup"
```

### 6.3 GateContextResolver

A standalone component responsible for extracting **gate-decision context** from
a `FetchTask` (or planner-level job candidate). The gate **never** queries the DB
directly for context; it operates only on a `GateContext` object returned by this
resolver.

**Why standalone**: separates concerns, testable in isolation, single place to
extend when adding new context dimensions (e.g., venue_id, season_phase).

```python
# schema_inspector/services/gate_context_resolver.py

@dataclass(frozen=True)
class GateContext:
    """Resolved context used by SportradarCoverageGate to make decisions."""
    sport_slug: str | None        # "football", "basketball", ...
    is_editor: bool               # True = Sofascore manual; False = automated
    unique_tournament_id: int | None
    season_id: int | None
    event_id: int | None

    has_full_context: bool        # False when any required field is missing
    missing_reason: str = ""      # human-readable for logs


class GateContextResolver:
    """Extracts GateContext from a FetchTask.

    Path-template-driven extraction:
    - /event/{event_id}/...                        → DB lookup on event for sport, ut_id, is_editor
    - /unique-tournament/{ut}/season/{s}/...       → ut_id + season_id from path params; sport via UT JOIN
    - /team/{id}/... and /player/{id}/...          → no canonical UT context → returns has_full_context=False
    - /sport/{slug}/...                            → sport_slug from path; no event/ut → has_full_context=False
    - any other / unmatched                        → has_full_context=False

    Caching: short-lived in-memory LRU (~10K entries, ~5 min TTL) to avoid
    repeated event lookups for the same event_id during high-volume cycles.
    """

    def __init__(self, db: Database, cache_size: int = 10_000):
        self._db = db
        self._event_cache: dict[int, tuple[str, int | None, bool]] = {}  # bounded by LRU

    async def resolve(self, task: FetchTask) -> GateContext:
        path = task.endpoint.path_template
        params = task.path_params

        # Case 1: event-level — need DB lookup
        if "/event/{event_id}/" in path or path == "/api/v1/event/{event_id}":
            event_id = params.get("event_id")
            if event_id is None:
                return GateContext(None, False, None, None, None, False,
                                   missing_reason="event_id not in task params")
            sport, ut_id, is_editor = await self._lookup_event(int(event_id))
            if sport is None:
                return GateContext(None, False, None, None, event_id, False,
                                   missing_reason=f"event {event_id} not found in DB")
            return GateContext(sport, is_editor, ut_id, None, event_id, True)

        # Case 2: season-level — UT and season in path; sport via UT lookup
        if "/unique-tournament/{unique_tournament_id}/season/{season_id}/" in path:
            ut_id = int(params.get("unique_tournament_id", 0)) or None
            season_id = int(params.get("season_id", 0)) or None
            if ut_id is None:
                return GateContext(None, False, None, None, None, False,
                                   missing_reason="ut_id not in path params")
            sport = await self._lookup_sport_for_ut(ut_id)
            return GateContext(sport, False, ut_id, season_id, None, sport is not None)

        # Case 3: team-level / player-level / sport-listings — no canonical UT
        # Phase 1 policy: fail-open. Phase 2 may add resolver heuristics
        # (e.g. team.primary_unique_tournament_id) — out of scope for now.
        return GateContext(None, False, None, None, None, False,
                           missing_reason=f"no canonical UT context for {path}")

    async def _lookup_event(self, event_id: int) -> tuple[str | None, int | None, bool]:
        """Returns (sport_slug, unique_tournament_id, is_editor)."""
        # Cache hit
        if event_id in self._event_cache:
            return self._event_cache[event_id]
        row = await self._db.fetchrow("""
            SELECT s.slug AS sport_slug, ut.id AS ut_id, ev.is_editor
            FROM event ev
            JOIN tournament t ON t.id = ev.tournament_id
            LEFT JOIN unique_tournament ut ON ut.id = t.unique_tournament_id
            JOIN category c ON c.id = COALESCE(ut.category_id, t.category_id)
            JOIN sport s ON s.id = c.sport_id
            WHERE ev.id = $1
        """, event_id)
        if row is None:
            result = (None, None, False)
        else:
            result = (row["sport_slug"], row["ut_id"], bool(row["is_editor"]))
        self._event_cache[event_id] = result
        # Bounded cache eviction (simple FIFO)
        if len(self._event_cache) > 10_000:
            for k in list(self._event_cache.keys())[:1000]:
                self._event_cache.pop(k, None)
        return result

    async def _lookup_sport_for_ut(self, ut_id: int) -> str | None:
        row = await self._db.fetchrow("""
            SELECT s.slug FROM unique_tournament ut
            JOIN category c ON c.id = ut.category_id
            JOIN sport s ON s.id = c.sport_id
            WHERE ut.id = $1
        """, ut_id)
        return row["slug"] if row else None
```

**Critical invariant**: when `ctx.has_full_context = False`, gate **always allows**
the fetch (fail-open). This is enforced at gate decision tree step 4 (see §7).

**Performance budget**: gate decision must complete in **<5ms p99** including
context resolution. Cache hits are ~0.01ms; cache misses do one indexed Postgres
lookup (~1-3ms). At 100k decisions/hour, the cache hit rate should exceed 99%
(events repeat across endpoints).

## 7. Gate decision tree (full fail-open conditions)

In **priority order** (top check wins). Note: BYPASS_ATTRS check moved BEFORE
DB lookup and dry-run check — operator overrides should short-circuit early.

| Order | Condition | Result | Rationale |
|------:|-----------|--------|-----------|
| 1 | `SCHEMA_INSPECTOR_SPORTRADAR_GATE_ENABLED=false` | **allow** | Kill-switch — instant disable without code rollback |
| 2 | `ctx.has_full_context == False` | **allow + log** | Resolver could not extract sport/ut/is_editor (e.g. team or player endpoint). See §6.3. |
| 3 | `ctx.sport_slug != "football"` | **allow** | Out of scope (Phase 1) |
| 4 | `ctx.is_editor == True` | **allow** | Sofascore manual data (not from Sportradar) |
| 5 | `path_template not in ENDPOINT_TO_COVERAGE_REQUIREMENT` | **allow** | No gating rule defined for this endpoint |
| 6 | `required_attr in BYPASS_ATTRS` env list | **allow + log** | Per-attr operator override; short-circuits BEFORE any DB lookup |
| 7 | `path_template in DRYRUN_OBSERVATION_ONLY` (e.g. top-ratings/overall) | **continue evaluation, force dry-run** | Endpoint not graduated yet — evaluate fully but never actually block |
| 8 | `unique_tournament_id` lookup yields no row in `sportradar_coverage` | **allow** | Coverage data missing — assume fetch needed |
| 9 | `reconciliation_confidence < MIN_CONFIDENCE` (default 0.9) | **allow** | Low-confidence mapping — too risky to block |
| 10 | `coverage_attrs[attr]` is `NULL` | **allow** | Data missing for this specific attr |
| 11 | `coverage_attrs[attr] >= min_value` (default 1) | **allow** | Coverage exists, fetch normally |
| 12 | global `DRY_RUN=true` OR step 7 forced dry-run | **allow + log** | Predict-only mode (don't actually block) |
| 13 | All else | **block** | Real coverage gap → skip fetch, emit metric |

**Pseudocode**:

```python
def should_fetch(self, task: FetchTask, ctx: GateContext) -> GateDecision:
    # 1. Kill-switch
    if not self.enabled:
        return GateDecision.allow("kill_switch_disabled")

    # 2. Missing context
    if not ctx.has_full_context:
        return GateDecision.allow_log(f"missing_context: {ctx.missing_reason}")

    # 3. Sport scope
    if ctx.sport_slug != "football":
        return GateDecision.allow("sport_out_of_scope")

    # 4. is_editor source
    if ctx.is_editor:
        return GateDecision.allow("is_editor_true")

    # 5. Endpoint not in mapping
    rule = ENDPOINT_TO_COVERAGE_REQUIREMENT.get(task.path_template)
    if rule is None:
        return GateDecision.allow("path_template_not_mapped")

    required_attr, min_value = rule

    # 6. BYPASS_ATTRS check (operator override) — BEFORE DB lookup
    if required_attr in self.bypass_attrs:
        return GateDecision.allow_log(f"bypass_attr: {required_attr}")

    # 7. DRYRUN_OBSERVATION_ONLY policy
    force_dry_run = task.path_template in self.dryrun_only_endpoints

    # 8. Coverage row lookup
    row = await self._lookup_coverage(ctx.unique_tournament_id)
    if row is None:
        return GateDecision.allow("coverage_missing")

    # 9. Low-confidence reconciliation
    if row.reconciliation_confidence < self.min_confidence:
        return GateDecision.allow("low_confidence")

    # 10. attr value missing
    coverage_value = row.coverage_attrs.get(required_attr)
    if coverage_value is None:
        return GateDecision.allow("attr_value_null")

    # 11. Coverage adequate
    if coverage_value >= min_value:
        return GateDecision.allow("coverage_adequate")

    # 12. Global or per-endpoint dry-run
    if self.dry_run or force_dry_run:
        return GateDecision.allow_log_would_block(
            attr=required_attr, value=coverage_value, min=min_value
        )

    # 13. BLOCK
    return GateDecision.block(
        attr=required_attr, value=coverage_value, min=min_value
    )
```

## 8. Kill-switch + env vars

| Env var | Default | Meaning |
|---------|---------|---------|
| `SCHEMA_INSPECTOR_SPORTRADAR_GATE_ENABLED` | `false` (initially) | Master kill-switch. `false` = always fail-open. Flip to `true` after dry-run validation. |
| `SCHEMA_INSPECTOR_SPORTRADAR_GATE_DRY_RUN` | `true` (initially) | When `true`: gate evaluates and logs decisions but does NOT block. Used for pre-rollout validation. |
| `SCHEMA_INSPECTOR_SPORTRADAR_GATE_MIN_CONFIDENCE` | `0.9` | Reconciliation confidence threshold. Below this → fail open. |
| `SCHEMA_INSPECTOR_SPORTRADAR_GATE_BYPASS_ATTRS` | (empty) | Comma-separated attr names to skip (e.g. `Lineups,Leaders`). Checked BEFORE DB lookup. |
| `SCHEMA_INSPECTOR_SPORTRADAR_GATE_DRYRUN_ONLY_ENDPOINTS` | `/api/v1/unique-tournament/.../top-ratings/overall` | Comma-separated path_templates kept in dry-run mode regardless of global DRY_RUN. Default contains the 1 dry-run-observation endpoint. |
| `SCHEMA_INSPECTOR_SPORTRADAR_GATE_BYPASS_SPORTS` | (empty) | Comma-separated sport slugs (overrides sport_slug check). Reserved for emergency use. |

**Recommended deploy sequence** (also see Section 11):

```
Phase 0: ENABLED=false, DRY_RUN=true              (no-op, deploy code only)
Phase 1: ENABLED=true,  DRY_RUN=true              (predict, don't block)
Phase 2: ENABLED=true,  DRY_RUN=false, BYPASS_ATTRS=all-except-Lineups  (block Lineups only)
Phase 3: ENABLED=true,  DRY_RUN=false, BYPASS_ATTRS=""  (full gate)
Phase 4: ENABLED=true,  DRY_RUN=false                                   (steady state)

Rollback at any phase: ENABLED=false. Restart sofascore-api. Done in <60 seconds.
```

## 9. Dry-run mode

When `DRY_RUN=true` AND gate would block:
- Worker continues fetch as normal (no behavior change for live workload).
- Structured log line emitted to stdout (collected by journald → ELK/etc.):

```json
{
  "event": "sportradar_gate_dryrun",
  "would_block": true,
  "path_template": "/api/v1/event/{event_id}/lineups",
  "event_id": 15706263,
  "ut_id": 1900,
  "sport_slug": "football",
  "is_editor": false,
  "required_attr": "Lineups",
  "coverage_value": 0,
  "min_value": 1,
  "reconciliation_method": "blind_id",
  "reconciliation_confidence": 1.0,
  "ts": "2026-05-08T18:23:45.123Z"
}
```

After 24h dry-run, **analyze logs** to validate:

1. **Predicted-block volume** ≈ expected reduction (~3,500/hour)
2. **Predicted-block accuracy**: cross-reference with `api_request_log` — for each `would_block=true` event, did the actual request return 4xx? Target accuracy: ≥95%.
3. **False positives**: cases where we'd block but actual response was 200. Investigate per-case (likely matrix lag or wrong reconciliation).

## 10. Metrics

Exposed via existing `/ops/metrics` Prometheus endpoint.

| Metric | Type | Labels | Purpose |
|--------|------|--------|---------|
| `sportradar_gate_decisions_total` | counter | `result, attr, sport, dry_run` | Per-decision count: result ∈ {block, allow, fail_open, dry_run_would_block} |
| `sportradar_gate_lookup_latency_ms` | histogram | (none) | DB lookup time for `sportradar_coverage` |
| `sportradar_gate_block_rate_per_minute` | gauge | (none) | Live block rate (for alerting) |
| `sportradar_coverage_missing_lookup_total` | counter | `ut_id` | UTs not in `sportradar_coverage` (sync gap signal) |
| `sportradar_coverage_low_confidence_total` | counter | `method` | Reconciliation < 0.9 (manual review backlog) |
| `sportradar_coverage_synced_at_seconds` | gauge | (none) | Time since last sync (alert if > 35 days) |

**Alerts** (suggested, not auto-deployed):
- `block_rate_per_minute > 1000` for 15 min → warn (gate too aggressive)
- `coverage_synced_at_seconds > 35 days` → warn (sync timer broken)
- `gate_lookup_latency_ms p99 > 50ms` for 5 min → warn (DB issue)

## 11. Verification plan on prod

### Phase 0: Deploy (no behavior change)
- Code merged + deployed via `git push origin main && ssh sofascore-prod 'cd /opt/sofascore && git pull --ff-only origin main && sudo systemctl restart sofascore-api sofascore-resource-refresh@*'`
- `.env`: `SCHEMA_INSPECTOR_SPORTRADAR_GATE_ENABLED=false` (kill-switch)
- Smoke check: `/ops/health` still healthy, no new errors in logs.
- **Exit criteria**: 1 hour with 0 regressions.

### Phase 1: First sync run + dry-run
- Run `python -m schema_inspector.cli sync-sportradar-coverage --package Soccer` (one-shot, manual).
- Verify `sportradar_coverage` table populated with ~1,269 rows, ≥85% with `reconciliation_method='blind_id'`.
- Set `.env`: `SCHEMA_INSPECTOR_SPORTRADAR_GATE_ENABLED=true, DRY_RUN=true`
- Restart `sofascore-api` + 10 × `sofascore-resource-refresh@N`.
- Wait 24 hours.
- Analyze logs:
  - Total `would_block` count.
  - Sample 100 random `would_block` events, cross-check `api_request_log`: did actual request 4xx?
  - **Exit criteria**: ≥95% predicted-block accuracy AND projected reduction matches expectation (within ±25%).

### Phase 2: Single-attr live gate (Lineups)
- `.env`: `DRY_RUN=false, BYPASS_ATTRS="Leaders,Head2Head,Schedules,Competitor Profile,Basic Play by Play,Basic Statistics"` (block Lineups only)
- Rolling restart of api + workers.
- Monitor 4 hours:
  - HTTP success rate trend (target: +1-2 pp).
  - 4xx rate on `/lineups` (target: -50%).
  - Zero new failed jobs.
- **Exit criteria**: stable, no anomalies. **Rollback criterion**: any unexpected drop in 200 responses.

### Phase 3: Full gate (11 hard-block endpoints, 1 dry-run-observation)
- `.env`:
  - `BYPASS_ATTRS=""` (block all 11 endpoints per matrix)
  - `DRYRUN_ONLY_ENDPOINTS="/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/top-ratings/overall"` (keep top-ratings in dry-run regardless)
- Rolling restart of planner + affected workers (see rollback recipe below for service list).
- Monitor 24 hours.
- **Exit criteria**: HTTP success rate ≥95% AND 4xx volume reduced ≥50% AND zero data-loss anomalies in sample audits.

### Phase 4: Steady state + top-ratings graduation evaluation (T+30 days)
- Enable Prometheus alerts.
- Schedule monthly `sportradar-sync.timer`.
- Document in CLAUDE.md.
- **Top-ratings graduation review** (after 30 days of dry-run logs):
  - Sample 1000 `would_block=true` events for `top-ratings/overall`
  - Cross-check with `api_request_log`: did the actual response 4xx?
  - If predicted-block accuracy ≥95% → remove from `DRYRUN_ONLY_ENDPOINTS`
  - If <95% → keep in dry-run, investigate mapping or coverage staleness
  - Decision recorded as separate ACK (Phase 4 → Phase 5: top-ratings live)

### Rollback (any phase)

The gate runs in **planner** (PilotOrchestrator + planner_daemon) and **workers**
(resource_refresh_worker + discovery_worker). Restart **all** affected services:

```bash
ssh sofascore-prod '
  # 1. Flip kill-switch to false
  sed -i "s/SCHEMA_INSPECTOR_SPORTRADAR_GATE_ENABLED=true/SCHEMA_INSPECTOR_SPORTRADAR_GATE_ENABLED=false/" /opt/sofascore/.env

  # 2. Restart planner-level services (gate runs here as primary)
  sudo systemctl restart \
    sofascore-resource-planner.service \
    sofascore-live-discovery-planner.service \
    sofascore-historical-planner.service \
    sofascore-historical-tournament-planner.service

  # 3. Restart worker-level services (gate runs here as defense-in-depth)
  for i in 1 2 3 4 5 6 7 8 9 10; do
    sudo systemctl restart sofascore-resource-refresh@$i.service
  done
  for i in 1 2 3 4 5 6 7 8; do
    sudo systemctl restart sofascore-hydrate@$i.service
  done

  # 4. Restart API (so /ops/health reflects kill-switch state)
  sudo systemctl restart sofascore-api.service
'
```

ETA: <90 seconds total (parallel-able). No code revert needed — env-driven.

**Partial rollback** (revert ONE attribute, keep rest live):
```bash
# e.g. observed false-positives on Lineups but not Leaders:
sed -i 's/SCHEMA_INSPECTOR_SPORTRADAR_GATE_BYPASS_ATTRS=""/SCHEMA_INSPECTOR_SPORTRADAR_GATE_BYPASS_ATTRS="Lineups"/' /opt/sofascore/.env
# Restart same set as above.
```

## 12. Tests strategy

### Unit tests (target: ~15-20 cases)

`tests/test_sportradar_coverage_gate.py`:
- Kill-switch disabled → always allow
- Sport != football → allow
- is_editor=true → allow
- path_template not in mapping → allow
- ut_id not in coverage table → allow
- confidence < threshold → allow
- coverage_attrs[attr] is NULL → allow
- coverage_attrs[attr] >= min_value → allow
- coverage_attrs[attr] < min_value AND dry_run=true → allow + emit log
- coverage_attrs[attr] < min_value AND dry_run=false → block
- BYPASS_ATTRS includes attr → allow
- Tuple-valued required_attr (multiple options) → use most-inclusive

`tests/test_sportradar_sync.py`:
- Blind ID match populates row with confidence=1.0
- Name match populates row with confidence in [0.7, 0.95]
- Cross-sport collision rejected
- Manual override populates with confidence=1.0
- Unmatched populates with method='unmatched'
- Sync is idempotent (re-running on same data → 0 changes)
- Incremental sync (filter by `last_changed > prev_synced_at`)

### Integration tests
- Wire gate into PilotOrchestrator → ensure block decision propagates to job_skipped status.
- Wire gate into ResourceRefreshWorker.

### Manual test (no automation)
- Enable dry-run in dev, simulate fetch for known low-coverage league → verify log emitted with correct fields.

## 13. Sync CLI

`schema_inspector/services/sportradar_sync.py`:

```python
async def sync_sportradar_coverage(
    *,
    package: str = "Soccer",
    incremental: bool = True,
    db: Database,
) -> SyncReport:
    """
    1. Fetch from Sportradar coverage matrix API (cached locally as xlsx).
    2. For each row: try reconciliation phases.
    3. UPSERT into sportradar_coverage.
    4. Return report (counts per method, errors).
    """
```

CLI subcommand:
```bash
python -m schema_inspector.cli sync-sportradar-coverage \
    --package Soccer \
    [--incremental | --full] \
    [--dry-run]   # don't write to DB, print report only
```

Systemd timer `ops/systemd/sofascore-sportradar-sync.timer`:
```ini
[Timer]
OnCalendar=monthly
RandomizedDelaySec=2h
Persistent=true

[Install]
WantedBy=timers.target
```

## 14. Risks + mitigations

| Risk | Probability | Impact | Mitigation |
|------|------------|--------|------------|
| **False positive blocking** (block endpoint that would return 200) | Medium | Silent data loss | Confidence ≥0.9 threshold + dry-run validation + 24h log analysis before live block |
| **Sportradar matrix stale** (1+ months) | Low | Outdated decisions | Monthly sync timer + alert on `synced_at > 35 days` |
| **Reconciliation mistakes** (wrong UT mapping) | Low (3% known) | Wrong gate applied | Manual override config + `unmatched` row stays unblocked |
| **Database lookup latency** under load | Low | Worker slowdown | Indexed PK + small table (~1.3k rows) + optional in-memory cache |
| **Gate logic bug** | Medium | Mass over-blocking | Kill-switch env var → instant disable |
| **Sync API downtime** | Low | Stale data | Sync retries + last-good cache stays valid |
| **Phase 2 sport expansion breaks Phase 1** | Medium | Cross-sport regression | Sport_slug check in gate (Phase 1 explicit football-only) + per-sport config tables |

## 15. Open questions for review

1. **Confidence threshold**: 0.9 hard-locked or env-overridable per the user's earlier decision? → Locked answer: **env-overridable, default 0.9**.
2. **Manual override storage**: YAML config file vs DB table? → Locked: **YAML file (`config/sportradar_id_overrides.yaml`)** for git-trackable manual decisions.
3. **Per-attr bypass via env**: locked feature for Phase 2 staged rollout.
4. **Tier column**: keep for analytics, never used by gate.
5. **Materialized view for fast lookup**: deferred — benchmark indexed table first.

## 16. Implementation steps (high-level for B)

When user ACKs A and approves moving to B:

1. **DB migration** — `migrations/2026-05-08_sportradar_coverage.sql` (1 hour)
2. **GateContextResolver** — `schema_inspector/services/gate_context_resolver.py` + tests (3 hours) ← NEW component per §6.3
3. **Sync CLI** — `schema_inspector/services/sportradar_sync.py` + cli subcommand (4-6 hours)
4. **Reconciliation logic** — blind_id + name_match + manual_override (2-3 hours)
5. **Gate module** — `schema_inspector/services/sportradar_coverage_gate.py` (3 hours, includes BYPASS_ATTRS short-circuit and DRYRUN_ONLY_ENDPOINTS support)
6. **Planner-level wiring** — PilotOrchestrator + planner_daemon + live_discovery_planner + leaderboards_backfill (3 hours) ← PRIMARY integration
7. **Worker-level wiring** — resource_refresh_worker + discovery_worker (1 hour) ← DEFENSE-IN-DEPTH
8. **Metrics exporter** — Prometheus counters + histograms (1 hour)
9. **Tests** — unit (~20 cases incl. resolver and BYPASS_ATTRS short-circuit) + integration (3 cases) (5 hours)
10. **Manual override config + first sync** (1 hour)
11. **Systemd timer + ops/systemd unit for monthly sync** (30 min)
12. **CLAUDE.md update** documenting the gate + env vars (30 min)
13. **Phase 0 deploy + verification** (kill-switch=false, 1 hour active monitoring)
14. **Phase 1 dry-run + 24h analysis** (passive 24h)
15. **Phase 2 Lineups-only live + 4h monitor** (active) — pick attr with highest 4xx ROI
16. **Phase 3 full gate + 24h monitor** (passive 24h, top-ratings stays in dry-run)
17. **Phase 4 (T+30 days)**: top-ratings graduation review (separate ACK)

**Total active engineering**: ~3 days (was ~2-3, +1 day for GateContextResolver as separate testable component).
**Total wall-clock to Phase 3 steady state**: ~3-4 days.
**Total wall-clock to Phase 5 full graduation**: ~30+ days (top-ratings observation window).

## 17. Acceptance criteria for B completion

### Phase 3 acceptance (initial milestone)

- [ ] DB migration applied to prod, `sportradar_coverage` populated with 1,269 rows, ≥85% blind_id confidence.
- [ ] Manual override config committed (`config/sportradar_id_overrides.yaml`), includes 3 known collisions (Saudi Kings Cup, Saudi Super Cup, Bahrain Kings Cup).
- [ ] `GateContextResolver` tested in isolation — covers event-level, season-level, team/player (no-context), unknown path cases.
- [ ] Sync CLI idempotent + monthly timer scheduled.
- [ ] Gate code merged with **20 unit tests** passing (incl. BYPASS_ATTRS short-circuit, DRYRUN_ONLY_ENDPOINTS, missing-context fail-open).
- [ ] Planner-level integration tested: `should_fetch=False` jobs are NOT published to streams.
- [ ] Worker-level defense-in-depth tested: jobs already in queue are correctly skipped if gate says block.
- [ ] Phase 1 dry-run: ≥95% predicted-block accuracy on 100-event random sample.
- [ ] Phase 3 (11 hard-block endpoints live + top-ratings dry-run): HTTP success rate ≥95% (was 91%), 4xx reduced ≥50%.
- [ ] Zero regressions in operational job throughput vs pre-deploy baseline.
- [ ] Kill-switch verified working (smoke test in Phase 0).
- [ ] Documentation in CLAUDE.md updated.

### Phase 5 acceptance (top-ratings graduation, T+30 days, separate ACK)

- [ ] 30 days of `would_block=true` log analysis for `top-ratings/overall`.
- [ ] Predicted-block accuracy ≥95% on 1000-event sample.
- [ ] Cross-check against `api_request_log`: zero data-loss anomalies.
- [ ] Remove `top-ratings/overall` from `DRYRUN_ONLY_ENDPOINTS` env var.
- [ ] Restart planner + workers. 24h monitor under live block.
- [ ] If criteria not met: keep dry-run, document in revision log, evaluate again at T+60 days.

---

## Appendix A: Endpoint mapping origin

The `ENDPOINT_TO_COVERAGE_REQUIREMENT` dict is generated by `tools/sportradar_openapi_audit.py` (read-only) which:

1. Parses Sportradar OpenAPI spec (`Soccer Extended v4`) — 56 endpoints, 164 schemas.
2. Loads coverage matrix xlsx (1,269 football competitions).
3. Queries prod `endpoint_registry` (170 unique path_templates, 244 patterns).
4. Cross-references via `SR_TO_SOFASCORE` mapping table (built from empirical Chrome probes + parser code review).
5. Outputs Section 0 final table + Section 4 ready-to-paste Python literal.

This makes the mapping **derivable, not hardcoded**: re-running the audit after Sportradar adds new endpoints (or we discover new Sofascore paths) regenerates the gate dict automatically.

## Appendix B: References

- `tools/sportradar_openapi_audit.py` — audit utility (v2)
- `.cache/sportradar_endpoint_audit.md` — full audit report
- `C:\Users\bobur\Downloads\openapi.yaml` — Sportradar Soccer Extended v4 spec
- `C:\Users\bobur\Downloads\footballmatrix.xlsx` — coverage matrix export
- Prior plans: `2026-04-21-etl-prod-hardening.md`, `2026-04-22-live-terminal-and-self-requeue-fix.md`
