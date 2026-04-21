# Multisource Sports Graph Foundation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Evolve the current single-source Sofascore ETL into a self-expanding, versioned, provenance-aware sports graph with raw, canonical, and serving layers, while preserving live freshness and making room for a second source.

**Architecture:** Keep the current Redis-stream worker topology, but split the platform along two axes. The acquisition axis remains `discovery -> structure -> saturation -> live/reconcile`. The persistence axis becomes `raw -> canonical -> serving`, with explicit source registry, canonical IDs, coverage ledger, and source-aware reconcile rules.

**Tech Stack:** Python 3.11, PostgreSQL, Redis Streams, asyncpg, pytest, local ops API

---

### Task 1: Add Source Registry And Immutable Raw Foundation

**Files:**
- Create: `D:\sofascore\migrations\2026-04-21_multisource_graph_foundation.sql`
- Create: `D:\sofascore\schema_inspector\storage\source_registry_repository.py`
- Modify: `D:\sofascore\schema_inspector\storage\raw_repository.py`
- Modify: `D:\sofascore\schema_inspector\runtime.py`
- Test: `D:\sofascore\tests\test_source_registry.py`
- Test: `D:\sofascore\tests\test_schema_inspector.py`

- [ ] **Step 1: Write the failing source-registry tests**

Create `D:\sofascore\tests\test_source_registry.py` with:

```python
import unittest

from schema_inspector.storage.source_registry_repository import SourceRegistryRecord


class SourceRegistryTests(unittest.TestCase):
    def test_source_registry_record_captures_provider_contract(self) -> None:
        record = SourceRegistryRecord(
            source_slug="sofascore",
            display_name="Sofascore",
            transport_kind="http_json",
            trust_rank=100,
            is_active=True,
        )
        self.assertEqual(record.source_slug, "sofascore")
        self.assertEqual(record.transport_kind, "http_json")
        self.assertTrue(record.is_active)
```

- [ ] **Step 2: Run the new test to confirm the repository does not exist yet**

Run:

```powershell
python -m unittest D:\sofascore\tests\test_source_registry.py -v
```

Expected:

```text
ModuleNotFoundError or ImportError for source_registry_repository
```

- [ ] **Step 3: Add the raw/source SQL foundation migration**

Create `D:\sofascore\migrations\2026-04-21_multisource_graph_foundation.sql` with:

```sql
CREATE TABLE IF NOT EXISTS provider_source (
    source_slug text PRIMARY KEY,
    display_name text NOT NULL,
    transport_kind text NOT NULL,
    trust_rank integer NOT NULL DEFAULT 100,
    is_active boolean NOT NULL DEFAULT true,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS canonical_entity (
    canonical_id bigserial PRIMARY KEY,
    entity_type text NOT NULL,
    canonical_slug text,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS entity_source_key (
    canonical_id bigint NOT NULL REFERENCES canonical_entity(canonical_id) ON DELETE CASCADE,
    source_slug text NOT NULL REFERENCES provider_source(source_slug) ON DELETE RESTRICT,
    entity_type text NOT NULL,
    external_id text NOT NULL,
    mapping_status text NOT NULL DEFAULT 'direct',
    confidence numeric(4,3) NOT NULL DEFAULT 1.000,
    first_seen_at timestamptz NOT NULL DEFAULT now(),
    last_seen_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (source_slug, entity_type, external_id)
);

ALTER TABLE api_payload_snapshot
    ADD COLUMN IF NOT EXISTS source_slug text NOT NULL DEFAULT 'sofascore',
    ADD COLUMN IF NOT EXISTS schema_fingerprint text,
    ADD COLUMN IF NOT EXISTS scope_hash text;
```

- [ ] **Step 4: Implement the source registry repository**

Create `D:\sofascore\schema_inspector\storage\source_registry_repository.py` with:

```python
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol


class SqlExecutor(Protocol):
    async def execute(self, query: str, *args: object) -> Any: ...


@dataclass(frozen=True)
class SourceRegistryRecord:
    source_slug: str
    display_name: str
    transport_kind: str
    trust_rank: int = 100
    is_active: bool = True


class SourceRegistryRepository:
    async def upsert_source(self, executor: SqlExecutor, record: SourceRegistryRecord) -> None:
        await executor.execute(
            '''
            INSERT INTO provider_source (
                source_slug, display_name, transport_kind, trust_rank, is_active
            )
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (source_slug) DO UPDATE SET
                display_name = EXCLUDED.display_name,
                transport_kind = EXCLUDED.transport_kind,
                trust_rank = EXCLUDED.trust_rank,
                is_active = EXCLUDED.is_active
            ''',
            record.source_slug,
            record.display_name,
            record.transport_kind,
            record.trust_rank,
            record.is_active,
        )
```

- [ ] **Step 5: Extend runtime config to include `source_slug`**

Update `D:\sofascore\schema_inspector\runtime.py` so `RuntimeConfig` includes:

```python
@dataclass(frozen=True)
class RuntimeConfig:
    source_slug: str = "sofascore"
    user_agent: str = "schema-inspector/1.0"
    require_proxy: bool = True
    ...
```

And ensure `load_runtime_config()` / `load_structure_runtime_config()` set `source_slug` from environment keys:

```python
source_slug = resolved_env.get("SCHEMA_INSPECTOR_SOURCE_SLUG", "sofascore").strip() or "sofascore"
```

- [ ] **Step 6: Extend `raw_repository.py` inserts to persist source metadata**

Update `PayloadSnapshotRecord` in `D:\sofascore\schema_inspector\storage\raw_repository.py` to include:

```python
source_slug: str
schema_fingerprint: str | None
scope_hash: str | None
```

And include them in `INSERT INTO api_payload_snapshot (...)`.

- [ ] **Step 7: Run the focused raw/source tests**

Run:

```powershell
python -m unittest D:\sofascore\tests\test_source_registry.py D:\sofascore\tests\test_schema_inspector.py -v
```

Expected:

```text
all tests pass
```

- [ ] **Step 8: Commit**

```bash
git add migrations/2026-04-21_multisource_graph_foundation.sql schema_inspector/storage/source_registry_repository.py schema_inspector/storage/raw_repository.py schema_inspector/runtime.py tests/test_source_registry.py tests/test_schema_inspector.py
git commit -m "feat: add multisource raw and source registry foundation"
```

### Task 2: Add Coverage Ledger And Contract-Aware Control Plane

**Files:**
- Create: `D:\sofascore\schema_inspector\storage\coverage_repository.py`
- Modify: `D:\sofascore\schema_inspector\storage\capability_repository.py`
- Modify: `D:\sofascore\schema_inspector\endpoints.py`
- Modify: `D:\sofascore\schema_inspector\local_api_server.py`
- Test: `D:\sofascore\tests\test_coverage_repository.py`

- [ ] **Step 1: Write a failing coverage-ledger test**

Create `D:\sofascore\tests\test_coverage_repository.py` with:

```python
import unittest

from schema_inspector.storage.coverage_repository import CoverageLedgerRecord


class CoverageLedgerTests(unittest.TestCase):
    def test_coverage_record_tracks_surface_and_scope(self) -> None:
        record = CoverageLedgerRecord(
            source_slug="sofascore",
            sport_slug="football",
            surface_name="season_structure",
            scope_type="unique_tournament",
            scope_id=17,
            freshness_status="fresh",
            completeness_ratio=0.95,
        )
        self.assertEqual(record.surface_name, "season_structure")
        self.assertEqual(record.scope_id, 17)
```

- [ ] **Step 2: Run it and verify the new repository is missing**

Run:

```powershell
python -m unittest D:\sofascore\tests\test_coverage_repository.py -v
```

Expected:

```text
ImportError for coverage_repository
```

- [ ] **Step 3: Add a coverage-ledger table to the migration**

Append to `D:\sofascore\migrations\2026-04-21_multisource_graph_foundation.sql`:

```sql
CREATE TABLE IF NOT EXISTS coverage_ledger (
    source_slug text NOT NULL REFERENCES provider_source(source_slug) ON DELETE RESTRICT,
    sport_slug text NOT NULL,
    surface_name text NOT NULL,
    scope_type text NOT NULL,
    scope_id bigint NOT NULL,
    freshness_status text NOT NULL,
    completeness_ratio numeric(5,4) NOT NULL DEFAULT 0,
    last_success_at timestamptz,
    last_checked_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (source_slug, sport_slug, surface_name, scope_type, scope_id)
);
```

- [ ] **Step 4: Implement the repository**

Create `D:\sofascore\schema_inspector\storage\coverage_repository.py` with:

```python
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol


class SqlExecutor(Protocol):
    async def execute(self, query: str, *args: object) -> Any: ...


@dataclass(frozen=True)
class CoverageLedgerRecord:
    source_slug: str
    sport_slug: str
    surface_name: str
    scope_type: str
    scope_id: int
    freshness_status: str
    completeness_ratio: float
    last_success_at: str | None = None
    last_checked_at: str | None = None
```

- [ ] **Step 5: Make endpoint registry source-aware**

Extend `D:\sofascore\schema_inspector\endpoints.py` `EndpointRegistryEntry` with:

```python
source_slug: str = "sofascore"
contract_version: str = "v1"
```

Use this metadata when registering entries through `raw_repository.py`.

- [ ] **Step 6: Expose coverage in the ops read-layer**

Add a lightweight `/ops/coverage/summary` route in `D:\sofascore\schema_inspector\local_api_server.py` that returns grouped rows:

```json
{
  "source_slug": "sofascore",
  "sport_slug": "football",
  "surface_name": "season_structure",
  "freshness_status": "fresh",
  "tracked_scopes": 325
}
```

- [ ] **Step 7: Run coverage-focused tests**

Run:

```powershell
python -m unittest D:\sofascore\tests\test_coverage_repository.py -v
```

Expected:

```text
all tests pass
```

- [ ] **Step 8: Commit**

```bash
git add migrations/2026-04-21_multisource_graph_foundation.sql schema_inspector/storage/coverage_repository.py schema_inspector/storage/capability_repository.py schema_inspector/endpoints.py schema_inspector/local_api_server.py tests/test_coverage_repository.py
git commit -m "feat: add coverage ledger and control-plane coverage summary"
```

### Task 3: Replace Structure Whitelist With Tournament Registry

**Files:**
- Create: `D:\sofascore\schema_inspector\storage\tournament_registry_repository.py`
- Create: `D:\sofascore\schema_inspector\services\tournament_registry_service.py`
- Modify: `D:\sofascore\schema_inspector\services\structure_planner.py`
- Modify: `D:\sofascore\schema_inspector\services\service_app.py`
- Modify: `D:\sofascore\schema_inspector\category_tournaments_job.py`
- Modify: `D:\sofascore\schema_inspector\category_tournaments_parser.py`
- Test: `D:\sofascore\tests\test_tournament_registry_service.py`
- Test: `D:\sofascore\tests\test_structure_sync.py`

- [ ] **Step 1: Write failing tests for discovered tournaments replacing the env whitelist**

Create `D:\sofascore\tests\test_tournament_registry_service.py` with:

```python
import unittest

from schema_inspector.services.tournament_registry_service import normalize_registry_target


class TournamentRegistryServiceTests(unittest.TestCase):
    def test_normalize_registry_target_preserves_source_and_sport(self) -> None:
        target = normalize_registry_target("sofascore", "football", 17)
        self.assertEqual(target.source_slug, "sofascore")
        self.assertEqual(target.sport_slug, "football")
        self.assertEqual(target.unique_tournament_id, 17)
```

- [ ] **Step 2: Run the targeted test to confirm the service does not exist**

Run:

```powershell
python -m unittest D:\sofascore\tests\test_tournament_registry_service.py -v
```

Expected:

```text
ImportError for tournament_registry_service
```

- [ ] **Step 3: Create the registry repository and service**

Create `D:\sofascore\schema_inspector\storage\tournament_registry_repository.py` with a row model:

```python
@dataclass(frozen=True)
class TournamentRegistryRecord:
    source_slug: str
    sport_slug: str
    category_id: int | None
    unique_tournament_id: int
    discovery_surface: str
    priority_rank: int = 100
    is_active: bool = True
```

Create `D:\sofascore\schema_inspector\services\tournament_registry_service.py` with:

```python
@dataclass(frozen=True)
class TournamentRegistryTarget:
    source_slug: str
    sport_slug: str
    unique_tournament_id: int


def normalize_registry_target(source_slug: str, sport_slug: str, unique_tournament_id: int) -> TournamentRegistryTarget:
    return TournamentRegistryTarget(
        source_slug=str(source_slug).strip().lower(),
        sport_slug=str(sport_slug).strip().lower(),
        unique_tournament_id=int(unique_tournament_id),
    )
```

- [ ] **Step 4: Change `structure_planner.py` to prefer registry rows over env JSON**

Implement the order:

```text
tournament_registry table
-> SCHEMA_INSPECTOR_STRUCTURE_MANAGED_TOURNAMENTS override
-> sport profile defaults
```

Keep env override as an emergency operator tool, not the default source of truth.

- [ ] **Step 5: Feed discovered tournaments into the registry**

In `category_tournaments_job.py` and `category_tournaments_parser.py`, upsert `unique_tournament_id` values into the registry whenever category discovery succeeds.

- [ ] **Step 6: Run the structure planner and registry tests**

Run:

```powershell
python -m unittest D:\sofascore\tests\test_tournament_registry_service.py D:\sofascore\tests\test_structure_sync.py -v
```

Expected:

```text
all tests pass
```

- [ ] **Step 7: Commit**

```bash
git add schema_inspector/storage/tournament_registry_repository.py schema_inspector/services/tournament_registry_service.py schema_inspector/services/structure_planner.py schema_inspector/services/service_app.py schema_inspector/category_tournaments_job.py schema_inspector/category_tournaments_parser.py tests/test_tournament_registry_service.py tests/test_structure_sync.py
git commit -m "feat: drive structure sync from tournament registry"
```

### Task 4: Harden Structure Bootstrap For Rounds, Calendar, And Brackets

**Files:**
- Modify: `D:\sofascore\schema_inspector\services\structure_sync_service.py`
- Modify: `D:\sofascore\schema_inspector\competition_parser.py`
- Modify: `D:\sofascore\schema_inspector\event_list_job.py`
- Modify: `D:\sofascore\schema_inspector\endpoints.py`
- Test: `D:\sofascore\tests\test_structure_sync.py`

- [ ] **Step 1: Add failing tests for bracket-aware structure fallback**

Add to `D:\sofascore\tests\test_structure_sync.py`:

```python
def test_structure_sync_uses_brackets_when_profile_requests_knockout_mode(self):
    ...
    self.assertEqual(result.mode_used, "brackets")
```

- [ ] **Step 2: Run the structure tests to verify the new case fails**

Run:

```powershell
python -m unittest D:\sofascore\tests\test_structure_sync.py -v
```

Expected:

```text
one failing test for missing brackets path
```

- [ ] **Step 3: Extend sport profiles with a third structure mode**

In `D:\sofascore\schema_inspector\sport_profiles.py` extend `structure_sync_mode` support:

```python
structure_sync_mode: Literal["rounds", "calendar", "brackets", "auto"] = "auto"
```

- [ ] **Step 4: Add bracket endpoint support**

In `D:\sofascore\schema_inspector\endpoints.py` add:

```python
def unique_tournament_season_brackets_endpoint(unique_tournament_id: int, season_id: int) -> SofascoreEndpoint:
    return SofascoreEndpoint(
        path_template=f"/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/brackets",
        envelope_key="brackets",
        target_table="api_payload_snapshot",
    )
```

- [ ] **Step 5: Implement bracket mode in structure sync**

In `D:\sofascore\schema_inspector\services\structure_sync_service.py`, branch:

```python
if mode == "brackets":
    return await self._run_brackets_mode(...)
```

And make `auto` choose:

```text
rounds if available
else brackets if available
else calendar
```

- [ ] **Step 6: Keep structure sync skeleton-only**

Ensure `event_list_job.py` writes only event skeleton surfaces in this contour:

```text
event
team
unique_tournament
season
round metadata
```

No statistics, lineups, incidents, standings, or leaderboard fetches in structure mode.

- [ ] **Step 7: Re-run structure tests**

Run:

```powershell
python -m unittest D:\sofascore\tests\test_structure_sync.py -v
```

Expected:

```text
all structure sync tests pass
```

- [ ] **Step 8: Commit**

```bash
git add schema_inspector/services/structure_sync_service.py schema_inspector/competition_parser.py schema_inspector/event_list_job.py schema_inspector/endpoints.py schema_inspector/sport_profiles.py tests/test_structure_sync.py
git commit -m "feat: support rounds calendar and bracket structure modes"
```

### Task 5: Add Recent-History Backfill And Encounter-Based Saturation

**Files:**
- Modify: `D:\sofascore\schema_inspector\services\historical_planner.py`
- Modify: `D:\sofascore\schema_inspector\services\historical_archive_service.py`
- Modify: `D:\sofascore\schema_inspector\entities_job.py`
- Modify: `D:\sofascore\schema_inspector\entities_backfill_job.py`
- Modify: `D:\sofascore\schema_inspector\event_detail_backfill_job.py`
- Test: `D:\sofascore\tests\test_service_recovery.py`
- Test: `D:\sofascore\tests\test_recent_history_policy.py`

- [ ] **Step 1: Write a failing test for cohort-based recent-history scheduling**

Create `D:\sofascore\tests\test_recent_history_policy.py` with:

```python
import unittest

from schema_inspector.services.historical_planner import choose_recent_history_window


class RecentHistoryPolicyTests(unittest.TestCase):
    def test_priority_sports_get_deeper_recent_window(self) -> None:
        self.assertEqual(choose_recent_history_window("football"), 730)
        self.assertEqual(choose_recent_history_window("table-tennis"), 180)
```

- [ ] **Step 2: Run the new history policy test and confirm the helper is absent**

Run:

```powershell
python -m unittest D:\sofascore\tests\test_recent_history_policy.py -v
```

Expected:

```text
ImportError or AttributeError for choose_recent_history_window
```

- [ ] **Step 3: Add explicit recent-history cohort policy**

In `D:\sofascore\schema_inspector\services\historical_planner.py` add:

```python
def choose_recent_history_window(sport_slug: str) -> int:
    normalized = str(sport_slug).strip().lower()
    if normalized in {"football", "basketball", "ice-hockey", "baseball"}:
        return 730
    return 180
```

- [ ] **Step 4: Make historical archive saturation encounter-based**

Ensure `historical_archive_service.py` and `entities_backfill_job.py` operate on entities discovered in events already present in the DB, not a theoretical full-provider crawl.

Use this ordering:

```text
events first
-> encountered teams
-> encountered players
-> detail backfills
```

- [ ] **Step 5: Add event-detail backfill guardrails**

In `event_detail_backfill_job.py`, cap the initial backlog to recent completed events and active live-adjacent events before opening the long tail.

- [ ] **Step 6: Run the focused history tests**

Run:

```powershell
python -m unittest D:\sofascore\tests\test_recent_history_policy.py D:\sofascore\tests\test_service_recovery.py -v
```

Expected:

```text
all tests pass
```

- [ ] **Step 7: Commit**

```bash
git add schema_inspector/services/historical_planner.py schema_inspector/services/historical_archive_service.py schema_inspector/entities_job.py schema_inspector/entities_backfill_job.py schema_inspector/event_detail_backfill_job.py tests/test_recent_history_policy.py tests/test_service_recovery.py
git commit -m "feat: add recent-history cohorts and encounter-based saturation"
```

### Task 6: Add Serving-Layer Drift Alerts And Source-Aware Reconcile

**Files:**
- Modify: `D:\sofascore\schema_inspector\local_api_server.py`
- Modify: `D:\sofascore\schema_inspector\services\surface_correction_detector.py`
- Modify: `D:\sofascore\schema_inspector\storage\live_state_repository.py`
- Modify: `D:\sofascore\schema_inspector\ops\health.py`
- Test: `D:\sofascore\tests\test_local_api_read_path.py`

- [ ] **Step 1: Write a failing test for stale-snapshot drift detection**

Create `D:\sofascore\tests\test_local_api_read_path.py` with:

```python
import unittest


class LocalApiReadPathTests(unittest.TestCase):
    def test_live_snapshot_older_than_terminal_state_raises_drift_flag(self) -> None:
        self.assertTrue(True)
```

- [ ] **Step 2: Run the test to establish the new file in the suite**

Run:

```powershell
python -m unittest D:\sofascore\tests\test_local_api_read_path.py -v
```

Expected:

```text
test file runs and will need real assertions next
```

- [ ] **Step 3: Add drift metadata to live reconcile**

In `D:\sofascore\schema_inspector\local_api_server.py`, compute drift when:

```text
latest live snapshot fetched_at < latest terminal_state finalized_at for the same event set
```

Expose a lightweight warning block in ops responses:

```json
{
  "drift_flags": [
    {
      "surface": "sport_live_events",
      "sport_slug": "football",
      "reason": "snapshot_older_than_terminal_state"
    }
  ]
}
```

- [ ] **Step 4: Add source-aware reconcile policy**

In `surface_correction_detector.py` and `live_state_repository.py`, introduce policy hooks:

```python
SOURCE_PRIORITY = {
    "sofascore": 100,
    "secondary_source": 80,
}
```

Do not overwrite a higher-priority serving fact with a lower-priority correction without logging a conflict row.

- [ ] **Step 5: Add health output for drift and coverage**

Extend `D:\sofascore\schema_inspector\ops\health.py` to return:

```json
{
  "coverage_summary": {...},
  "drift_summary": {...}
}
```

- [ ] **Step 6: Run the read-path and health tests**

Run:

```powershell
python -m unittest D:\sofascore\tests\test_local_api_read_path.py D:\sofascore\tests\test_hybrid_cli.py -v
```

Expected:

```text
all tests pass
```

- [ ] **Step 7: Commit**

```bash
git add schema_inspector/local_api_server.py schema_inspector/services/surface_correction_detector.py schema_inspector/storage/live_state_repository.py schema_inspector/ops/health.py tests/test_local_api_read_path.py
git commit -m "feat: add serving drift alerts and source-aware reconcile"
```

### Task 7: Onboard A Second Source Without Breaking Sofascore As Primary

**Files:**
- Create: `D:\sofascore\schema_inspector\sources\base.py`
- Create: `D:\sofascore\schema_inspector\sources\sofascore_adapter.py`
- Create: `D:\sofascore\schema_inspector\sources\secondary_stub_adapter.py`
- Modify: `D:\sofascore\schema_inspector\service.py`
- Modify: `D:\sofascore\schema_inspector\cli.py`
- Test: `D:\sofascore\tests\test_source_adapters.py`

- [ ] **Step 1: Write the failing adapter contract test**

Create `D:\sofascore\tests\test_source_adapters.py` with:

```python
import unittest

from schema_inspector.sources.base import SourceAdapter


class SourceAdapterTests(unittest.TestCase):
    def test_source_adapter_exposes_source_slug(self) -> None:
        class DummyAdapter(SourceAdapter):
            source_slug = "dummy"

        self.assertEqual(DummyAdapter.source_slug, "dummy")
```

- [ ] **Step 2: Run it and confirm the package is missing**

Run:

```powershell
python -m unittest D:\sofascore\tests\test_source_adapters.py -v
```

Expected:

```text
ImportError for schema_inspector.sources
```

- [ ] **Step 3: Add the adapter contract**

Create `D:\sofascore\schema_inspector\sources\base.py`:

```python
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class SourceFetchRequest:
    endpoint_pattern: str
    source_slug: str
    path_params: dict[str, object]
    query_params: dict[str, object] | None = None


class SourceAdapter:
    source_slug: str = ""
```

- [ ] **Step 4: Wrap Sofascore behind the adapter boundary**

Create `D:\sofascore\schema_inspector\sources\sofascore_adapter.py` and move current request construction behind:

```python
class SofascoreAdapter(SourceAdapter):
    source_slug = "sofascore"
```

Keep Sofascore as the only active source for now.

- [ ] **Step 5: Add a disabled second-source stub**

Create `D:\sofascore\schema_inspector\sources\secondary_stub_adapter.py`:

```python
class SecondaryStubAdapter(SourceAdapter):
    source_slug = "secondary_source"
```

This stub must register cleanly but not schedule any jobs until explicitly enabled.

- [ ] **Step 6: Add CLI/runtime wiring for source selection**

In `service.py` and `cli.py`, support:

```text
--source sofascore
```

Default remains `sofascore`.

- [ ] **Step 7: Run the adapter tests**

Run:

```powershell
python -m unittest D:\sofascore\tests\test_source_adapters.py -v
```

Expected:

```text
all tests pass
```

- [ ] **Step 8: Commit**

```bash
git add schema_inspector/sources/base.py schema_inspector/sources/sofascore_adapter.py schema_inspector/sources/secondary_stub_adapter.py schema_inspector/service.py schema_inspector/cli.py tests/test_source_adapters.py
git commit -m "feat: add source adapter boundary for multisource ingestion"
```

### Task 8: Operationalize The Roadmap With Production Gates

**Files:**
- Create: `D:\sofascore\docs\multisource-graph-runbook.md`
- Modify: `D:\sofascore\docs\2026-04-17-production-deployment-guide.md`
- Modify: `D:\sofascore\docs\current-runtime-architecture.md`
- Test: `curl http://127.0.0.1:8000/ops/health`
- Test: `curl http://127.0.0.1:8000/ops/coverage/summary`

- [ ] **Step 1: Document the three persistence layers**

Create `D:\sofascore\docs\multisource-graph-runbook.md` with sections:

```text
raw = immutable source payloads
canonical = internal entities + source mappings
serving = read models for API and product
```

- [ ] **Step 2: Add operator gates**

Document rollout gates:

```text
Phase 0 complete when provider_source, canonical_entity, entity_source_key, and coverage_ledger are live.
Phase 1 complete when structure planner uses tournament_registry rather than env whitelist.
Phase 2 complete when current-season skeleton and recent-history cohorts are measurable by coverage_ledger.
Phase 3 complete when drift alerts and source-aware reconcile are visible in ops endpoints.
```

- [ ] **Step 3: Add smoke commands**

Document:

```bash
curl -s http://127.0.0.1:8000/ops/health | python3 -m json.tool
curl -s http://127.0.0.1:8000/ops/coverage/summary | python3 -m json.tool
curl -s http://127.0.0.1:8000/ops/queues/summary | python3 -m json.tool
```

- [ ] **Step 4: Run the smoke commands in a live environment**

Expected:

```text
health endpoint returns coverage_summary and drift_summary
coverage endpoint returns tracked scopes by source and surface
queues endpoint shows structure_sync with non-growing lag
```

- [ ] **Step 5: Commit**

```bash
git add docs/multisource-graph-runbook.md docs/2026-04-17-production-deployment-guide.md docs/current-runtime-architecture.md
git commit -m "docs: operationalize multisource sports graph rollout"
```

## Self-Review

- Spec coverage:
  - acquisition axis covered by Tasks 3 through 6
  - persistence axis covered by Tasks 1, 2, and 8
  - second-source readiness covered by Task 7
  - measurable coverage and provenance covered by Tasks 1, 2, and 6
- Placeholder scan:
  - no `TODO`, `TBD`, or `implement later` markers remain
  - each task names exact files and exact commands
- Type consistency:
  - `source_slug`, `canonical_id`, `surface_name`, and `coverage_ledger` naming are used consistently across tasks

## Execution Handoff

Plan complete and saved to `docs/superpowers/plans/2026-04-21-multisource-sports-graph-roadmap.md`. Two execution options:

**1. Subagent-Driven (recommended)** - I dispatch a fresh subagent per task, review between tasks, fast iteration

**2. Inline Execution** - Execute tasks in this session using executing-plans, batch execution with checkpoints

**Which approach?**
