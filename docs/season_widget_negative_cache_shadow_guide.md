# Season Widget Negative Cache Shadow Guide

This guide defines how we interpret the first 7 days of `SCHEMA_INSPECTOR_NEGATIVE_CACHE_MODE=shadow`.

## Scope

- Endpoints: season widgets such as `player-of-the-season`, `top-players/*`, `top-teams/*`
- Signals come from `endpoint_availability_log`
- Shadow means:
  - the gate emits `decision='shadow_suppress'`
  - the fetch still happens
  - state is not mutated by the shadow event itself

## Rollout Decision Table

### `shadow_suppress_total`

Purpose: sample-size and coverage check. If this number is too small, the week of shadow is not representative.

- Green: `>= 10,000` shadow suppressions in 7 days
- Yellow: `1,000 .. 9,999`
- Red: `< 1,000`

Interpretation:
- Green means the shadow run touched enough noisy traffic to predict enforce behavior.
- Yellow means shadow may still be under-sampling the worst offenders.
- Red means do not switch to enforce yet.

### `revival_total`

Purpose: detect false negatives, meaning keys we would have suppressed but that later showed real data.

We use:

- `revival_candidate_total`: count of `200` probes where `classification_before IN ('c_probation', 'b_structural')`
- `real_revival_total`: a revival candidate with a second `200` for the same key within 24 hours
- `transient_revival_total`: a revival candidate without confirmation

Primary rollout threshold is:

`real_revival_rate = real_revival_total / NULLIF(shadow_suppress_total, 0)`

- Green: `<= 0.05%`
- Yellow: `> 0.05% and <= 0.20%`
- Red: `> 0.20%`

Why these numbers:
- Phase 1 found zero observed `404 -> 200` cases in the top-noise sample.
- We therefore treat real revivals as exceptional, not routine.
- `0.05%` means at most `5` confirmed revivals per `10,000` would-be suppressions.
- Above `0.20%`, the gate is no longer “rarely wrong”; it is meaningfully eating live data.

### `probe_latency_p95`

Purpose: make sure shadow instrumentation is not materially slowing widget fetches.

- Green: `<= 2,000 ms`
- Yellow: `> 2,000 ms and <= 5,000 ms`
- Red: `> 5,000 ms`

Interpretation:
- Shadow should be mostly bookkeeping.
- A red p95 means the extra lookup/logging is hurting the contour before we even enforce suppression.

### `404_volume_on_widget_endpoints`

Purpose: validate traffic shape and later measure enforce benefit.

During shadow this should remain close to the pre-shadow baseline because we still probe.

- Green: within `+-15%` of the 7-day pre-shadow daily baseline
- Yellow: deviation of `>15% and <=30%`
- Red: deviation of `>30%`

Interpretation:
- Green means shadow is observational only.
- A red increase suggests we accidentally introduced extra probes or duplicate paths.

## Distinguishing Real Revival vs Network Flap

Do not treat every isolated `200` after prior `404` as a real revival.

Classification rules:

- Real revival:
  - first `200` arrives for a key with `classification_before IN ('c_probation', 'b_structural')`
  - and a second `200` arrives for the same `(unique_tournament_id, season_id, endpoint_pattern)` within 24 hours
- Transient revival:
  - only one `200`
  - or the next probe for the same key returns `404`

Why:
- A single `200` can be an upstream flap, empty placeholder, or inconsistent widget rollout.
- Two successes on the same key are a much better proxy for actual data availability.

## Manual Kill Switch Rules

Switch back to `off` immediately if any of these happen:

- `real_revival_rate > 0.20%`
- `probe_latency_p95 > 5,000 ms` for two consecutive daily checks
- widget `404` volume increases by more than `30%` vs pre-shadow baseline

No migration rollback is needed. Only set:

`SCHEMA_INSPECTOR_NEGATIVE_CACHE_MODE=off`

and restart the affected services.

## Daily SQL Dashboard Query

Run this once per day during the first shadow week.

```sql
WITH recent AS (
    SELECT
        observed_at,
        unique_tournament_id,
        season_id,
        endpoint_pattern,
        decision,
        http_status,
        probe_latency_ms,
        classification_before
    FROM endpoint_availability_log
    WHERE observed_at >= now() - interval '7 days'
      AND endpoint_pattern LIKE '/api/v1/unique-tournament/%/season/%'
      AND (
        endpoint_pattern LIKE '%/player-of-the-season'
        OR endpoint_pattern LIKE '%/top-players/%'
        OR endpoint_pattern LIKE '%/top-teams/%'
      )
),
shadow AS (
    SELECT COUNT(*)::BIGINT AS shadow_suppress_total
    FROM recent
    WHERE decision = 'shadow_suppress'
),
probes AS (
    SELECT *
    FROM recent
    WHERE decision IN ('probe', 'bypass_probe')
),
probe_stats AS (
    SELECT
        COUNT(*)::BIGINT AS probe_total,
        COUNT(*) FILTER (WHERE http_status = 404)::BIGINT AS probe_404_total,
        COUNT(*) FILTER (WHERE http_status = 200)::BIGINT AS probe_200_total,
        percentile_disc(0.95) WITHIN GROUP (ORDER BY probe_latency_ms) AS probe_latency_p95_ms
    FROM probes
),
revival_candidates AS (
    SELECT
        p.*,
        EXISTS (
            SELECT 1
            FROM probes p2
            WHERE p2.unique_tournament_id = p.unique_tournament_id
              AND COALESCE(p2.season_id, -1) = COALESCE(p.season_id, -1)
              AND p2.endpoint_pattern = p.endpoint_pattern
              AND p2.http_status = 200
              AND p2.observed_at > p.observed_at
              AND p2.observed_at <= p.observed_at + interval '24 hours'
        ) AS has_confirming_200
    FROM probes p
    WHERE p.http_status = 200
      AND p.classification_before IN ('c_probation', 'b_structural')
),
revivals AS (
    SELECT
        COUNT(*)::BIGINT AS revival_candidate_total,
        COUNT(*) FILTER (WHERE has_confirming_200)::BIGINT AS real_revival_total,
        COUNT(*) FILTER (WHERE NOT has_confirming_200)::BIGINT AS transient_revival_total
    FROM revival_candidates
)
SELECT
    shadow.shadow_suppress_total,
    probe_stats.probe_total,
    probe_stats.probe_404_total,
    probe_stats.probe_200_total,
    probe_stats.probe_latency_p95_ms,
    revivals.revival_candidate_total,
    revivals.real_revival_total,
    revivals.transient_revival_total,
    ROUND(
        100.0 * revivals.real_revival_total / NULLIF(shadow.shadow_suppress_total, 0),
        4
    ) AS real_revival_rate_pct
FROM shadow
CROSS JOIN probe_stats
CROSS JOIN revivals;
```

## Endpoint-Level Drilldown SQL

Use this when the headline query goes yellow or red.

```sql
SELECT
    endpoint_pattern,
    COUNT(*) FILTER (WHERE decision = 'shadow_suppress') AS shadow_suppress_total,
    COUNT(*) FILTER (WHERE decision IN ('probe', 'bypass_probe') AND http_status = 404) AS probe_404_total,
    COUNT(*) FILTER (
        WHERE decision IN ('probe', 'bypass_probe')
          AND http_status = 200
          AND classification_before IN ('c_probation', 'b_structural')
    ) AS revival_candidate_total
FROM endpoint_availability_log
WHERE observed_at >= now() - interval '7 days'
  AND endpoint_pattern LIKE '/api/v1/unique-tournament/%/season/%'
GROUP BY endpoint_pattern
ORDER BY shadow_suppress_total DESC, probe_404_total DESC;
```

## Decision Rule

At the end of the first shadow week:

- switch to `enforce` only if all of these are true:
  - `shadow_suppress_total >= 10,000`
  - `real_revival_rate <= 0.05%`
  - `probe_latency_p95 <= 2,000 ms`
  - widget `404` volume stayed within `+-15%` of pre-shadow baseline

- stay in `shadow` for one more week if any metric is yellow and none are red

- switch to `off` and investigate if any metric is red
