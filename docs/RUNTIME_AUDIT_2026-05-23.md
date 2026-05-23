# Runtime Audit — 2026-05-23

Цель: быстро понять, почему raw snapshot -> normalized DB -> read API ощущается
непоследовательным, и что ставить в работу для 24/7 backend.

## TL;DR

Критичные finding'и из `docs/PERFORMANCE_AUDIT_2026-05-20.md` в текущей ветке
частично уже закрыты:

| Area | 2026-05-20 finding | Current branch status |
|---|---|---|
| Hydrate duplicate work | `HydrateWorker` had no in-flight lock | Fixed: `hydrate:inflight:{event_id}` in `workers/hydrate_worker.py` |
| Live bootstrap lock leak | `App.run_event` could leak `live:hydrate_lock` | Fixed: explicit `try/finally` release in `cli.py` |
| Redis lock release race | `release_hydrate_lock` used GET -> CHECK -> DELETE | Fixed: Lua owner-checked release in `live_bootstrap.py` |
| Hot DB indexes | Missing `event.custom_id`, live-window, odds choice indexes | Fixed in `migrations/2026-05-20_perf_phase_1_indexes.sql` |
| Snapshot ops endpoint | `/ops/snapshots/summary?detail=true` ran heavy counts freely | Fixed: guarded path in `local_api_server.py` |
| Normalize lock order | Parallel persist could acquire rows in different order | Mostly fixed: broad sort discipline in `storage/normalize_repository.py` |
| Human TTL matrix | General matrix had blank Bobur columns | Fixed in `docs/endpoint-ttl-matrix.md` for football event endpoints |

The highest remaining risks are not "parser code is broken". They are
coordination and control-plane risks:

1. `scheduled_events_synthesizer.py` still uses OFFSET pagination in season,
   team, and player event list paths. Deep pages can be slow under API load.
2. `live-discovery` still appears to be a large transaction / single-instance
   path. If it stalls, live state population stalls.
3. Historical workers still need a live-first write governor before they can
   run aggressively without pressuring live.
4. Some endpoints are intentionally raw-only. That is fine for passthrough, but
   not fine if mobile screens need indexed SQL reads from those payload fields.
5. Non-football TTL and endpoint matrices are not configured yet.

## Raw Snapshot -> Normalize Contract

Current intended sequence:

1. Fetch upstream endpoint and write `api_payload_snapshot`.
2. Move latest pointer in `api_snapshot_head`.
3. Classify snapshot by endpoint pattern.
4. Parse through `ParserRegistry`.
5. Persist normalized rows in deterministic PK order.
6. Invalidate/read through API cache and overlay live deltas where needed.

Practical rule: if an endpoint is marked `(raw)`, it stops at step 2 and the
API can only serve it as raw passthrough. Do not expect fast typed SQL reads
from it until a parser + table + migration exist.

## Matrix Status

Football event endpoint TTLs are currently runtime-enforced by
`schema_inspector/endpoint_ttl_policy.py`.

The general matrix has now been synced for:

- football core event endpoints,
- football odds/meta endpoints,
- football per-player event endpoints.

Still missing:

- tennis/basketball/baseball/etc. per-status TTLs,
- product decisions for raw-only football endpoints that should become typed
  normalized tables,
- resource endpoint Bobur Always values where the current refresh interval is
  still inherited from code defaults.

## Next Work

Recommended order:

1. Add capability-aware fetch gating before negative cache:
   skip `/managers`, `/official-tweets`, heatmaps, shotmaps, highlights when
   root payload flags say the upstream has no data. This saves proxy budget and
   reduces tail latency.
2. Replace OFFSET with keyset pagination for season/team/player historical list
   endpoints, keeping OFFSET compatibility for shallow pages.
3. Split live-discovery persist into smaller stages and then run
   `live-discovery@1..3`.
4. Add a historical write governor tied to live SLO signals before increasing
   historical backfill pressure.
5. Decide which raw-only endpoints should be normalized. Start with
   `/event/{eid}/average-positions` and player heatmap/shotmap only if mobile
   needs those fields in indexed views.

## Prod Verification Checklist

Use these before changing runtime knobs:

```bash
curl -s http://127.0.0.1:8000/ops/health | jq
curl -s http://127.0.0.1:8000/ops/queues/summary | jq '.streams[] | select(.lag > 100)'
curl -s http://127.0.0.1:8000/ops/snapshots/summary | jq
curl -s http://127.0.0.1:8000/ops/backfill-priorities | jq
curl -s http://127.0.0.1:8000/ws/health | jq
```

For DB index deployment, apply `migrations/2026-05-20_perf_phase_1_indexes.sql`
one `CREATE INDEX CONCURRENTLY` statement per session with:

```bash
PGOPTIONS='-c lock_timeout=0 -c statement_timeout=0'
```

