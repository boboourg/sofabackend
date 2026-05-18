# Backfill priorities

Operator-tunable per-sport / per-UT weights for the historical-tournament
planner. Lets you say "drain football first" or "pause cricket while
EURO is hot".

## Quick start

1. Copy the example to the real path:

   ```bash
   sudo cp /opt/sofascore/config/backfill_priorities.example.yaml \
           /opt/sofascore/config/backfill_priorities.yaml
   sudo chown sofascore:sofascore /opt/sofascore/config/backfill_priorities.yaml
   ```

2. Edit weights:

   ```bash
   sudo nano /opt/sofascore/config/backfill_priorities.yaml
   ```

3. Reload without restart:

   ```bash
   sudo systemctl kill -s HUP sofascore-historical-tournament-planner.service
   # or
   /opt/sofascore/deploy/run_service.sh backfill-priorities reload
   ```

4. Inspect the current state:

   ```bash
   curl -s http://127.0.0.1:8000/ops/backfill-priorities | jq
   ```

## Schema

```yaml
sport_weights:
  <sport_slug>: <non-negative number>   # 0 = pause this sport
ut_boost:
  - ut_id: <int>
    multiplier: <non-negative number>   # multiplied onto sport_weights[<sport>]
sport_concurrency_caps:
  <sport_slug>: <non-negative int>      # null = unbounded
```

Missing sections default to "uniform / unbounded". Validation is strict —
a typo (negative weight, non-numeric value, missing `ut_id`) keeps the
previous config and logs a `WARNING` so SIGHUP never silently pauses a
sport.

## How the planner uses it

1. Pull the list of pending UTs from `backfill_cursor` (every tick).
2. For each candidate `(ut_id, sport_slug)`, compute
   `weight = sport_weights[sport_slug] * ut_boost.get(ut_id, 1.0)`.
   When `sport_weights[sport_slug]` is 0 or missing, the candidate is
   skipped entirely.
3. `random.choices(weights=…)` picks the next UT to schedule —
   ratios match the configured weights over many ticks.
4. The per-sport concurrency cap clamps how many in-flight
   `historical_tournament` jobs can exist simultaneously for that
   sport, preventing one sport from saturating the worker pool.

## Common recipes

### Drain football fast (burst)

```yaml
sport_weights:
  football: 50
  # everything else implicitly 0 (paused)
ut_boost:
  - { ut_id: 17, multiplier: 5.0 }   # Premier League
  - { ut_id: 1,  multiplier: 10.0 }  # EURO
sport_concurrency_caps:
  football: 100
```

### Equal priority across mainline sports

```yaml
sport_weights:
  football: 1
  basketball: 1
  tennis: 1
  ice-hockey: 1
  baseball: 1
```

### Pause one specific sport

```yaml
sport_weights:
  football: 1
  cricket: 0     # paused
```

## Where to find ut_id

```sql
SELECT id, name, category_id, tier
FROM unique_tournament
WHERE name ILIKE '%premier%';
```

or via REST:

```bash
curl -s "http://127.0.0.1:8000/api/v1/config/unique-tournaments?sport=football"
```

## Operational notes

- The planner reads the file **at startup** and on **SIGHUP**. A
  malformed file at startup raises and the unit refuses to start
  (visible in `journalctl -u sofascore-historical-tournament-planner`).
- A malformed file on SIGHUP keeps the previous-good config and logs
  `WARNING ... ConfigValidationError`.
- Weight changes are NOT retroactive — already-published jobs remain
  in their stream. Selection bias kicks in on the *next* planner tick.
- `sport_weights: { foo: 0 }` only pauses **publishing**. Already-
  in-flight jobs for that sport continue to completion.
