#!/usr/bin/env bash
#
# capability_probe_refresh.sh — re-probe expiring league_endpoint_capability
# cohorts and re-prime the Redis registry cache.
#
# Background (2026-05-28): the League Capabilities Probe (Phase 4.2) measures
# which Sofascore endpoints are actually published per
# (unique_tournament_id, season_id, status_type) cohort by sampling N recent
# events. Verdicts (allowed/disabled/unknown) are written to
# league_endpoint_capability with a 14-day TTL and consulted by
# match_center_policy when SOFASCORE_LEAGUE_CAPABILITIES_ENABLED=1.
#
# Without a refresh loop those verdicts freeze after 14 days (expires_at
# elapses → state treated as stale). This driver, run daily by
# sofascore-capability-probe.timer, keeps the FOOTBALL cohorts fresh:
#
#   1. Selects football (ut, season, status) cohorts already tracked in
#      league_endpoint_capability whose earliest endpoint expires within
#      REFRESH_HORIZON_DAYS (default 4 — re-probe a bit before the 14d TTL
#      lapses so there's no stale window).
#   2. Re-probes each cohort (samples SAMPLES events per endpoint).
#   3. Primes the Redis registry cache so workers see fresh verdicts.
#
# Scope is intentionally bounded to *already-tracked* football cohorts:
# new leagues enter tracking via the manual batch probe or the
# orchestrator's inline probe-on-miss; this loop only keeps known cohorts
# from going stale, so it can never run unbounded.
#
# Idempotent + safe to run repeatedly. Honours SOFASCORE_PLANNER_SPORT_SLUGS
# implicitly (we hard-filter to football in the SQL below — the registry is
# football-only while the mobile app is football-only).

set -uo pipefail
cd /opt/sofascore

REFRESH_HORIZON_DAYS="${CAPABILITY_REFRESH_HORIZON_DAYS:-4}"
SAMPLES="${CAPABILITY_PROBE_SAMPLES:-5}"
MAX_COHORTS="${CAPABILITY_REFRESH_MAX_COHORTS:-60}"   # runaway backstop
SLEEP_BETWEEN="${CAPABILITY_REFRESH_SLEEP_SECONDS:-2}"
LOG_TAG="capability_probe_refresh"

PSQL=(psql "${SOFASCORE_DATABASE_URL}" -t -A -F$'\t')

echo "[$(date -Iseconds)] ${LOG_TAG}: start horizon=${REFRESH_HORIZON_DAYS}d samples=${SAMPLES} max=${MAX_COHORTS}"

# Select football cohorts whose *earliest* endpoint expiry is within the
# horizon. GROUP BY collapses the per-endpoint rows to one probe call per
# (ut, season, status) — the probe CLI re-probes every endpoint for that
# cohort in a single invocation.
COHORTS=$("${PSQL[@]}" <<SQL
SELECT lec.unique_tournament_id, lec.season_id, lec.status_type
FROM league_endpoint_capability lec
WHERE lec.source = 'probe'
  AND lec.expires_at < now() + interval '${REFRESH_HORIZON_DAYS} days'
  AND EXISTS (
    SELECT 1 FROM tournament t
    JOIN category c ON c.id = t.category_id
    JOIN sport sp ON sp.id = c.sport_id
    WHERE t.unique_tournament_id = lec.unique_tournament_id
      AND sp.slug = 'football'
  )
GROUP BY lec.unique_tournament_id, lec.season_id, lec.status_type
ORDER BY MIN(lec.expires_at)
LIMIT ${MAX_COHORTS}
SQL
)

if [ -z "${COHORTS}" ]; then
  echo "[$(date -Iseconds)] ${LOG_TAG}: no football cohorts expiring within ${REFRESH_HORIZON_DAYS}d — priming redis only"
else
  COUNT=$(echo "${COHORTS}" | grep -c .)
  echo "[$(date -Iseconds)] ${LOG_TAG}: ${COUNT} cohort(s) to re-probe"
  OK=0
  FAIL=0
  while IFS=$'\t' read -r ut season status; do
    [ -z "${ut}" ] && continue
    if python -m schema_inspector.cli league-capability probe \
        --unique-tournament-id "${ut}" --season-id "${season}" \
        --status-type "${status}" --samples "${SAMPLES}" >/dev/null 2>&1; then
      OK=$((OK + 1))
    else
      FAIL=$((FAIL + 1))
      echo "[$(date -Iseconds)] ${LOG_TAG}: probe FAILED ut=${ut} season=${season} status=${status}"
    fi
    sleep "${SLEEP_BETWEEN}"
  done <<< "${COHORTS}"
  echo "[$(date -Iseconds)] ${LOG_TAG}: probe done ok=${OK} fail=${FAIL}"
fi

# Always re-prime Redis so workers pick up whatever is freshest in PG.
if python -m schema_inspector.cli league-capability prime-redis >/dev/null 2>&1; then
  echo "[$(date -Iseconds)] ${LOG_TAG}: prime-redis ok"
else
  echo "[$(date -Iseconds)] ${LOG_TAG}: prime-redis FAILED"
  exit 1
fi

echo "[$(date -Iseconds)] ${LOG_TAG}: done"
