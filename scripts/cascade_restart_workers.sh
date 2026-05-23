#!/bin/bash
# Phase 4.7.6 Track 3 (2026-05-23) — staggered worker restart.
#
# Three Phase 4.8 production flips all hit the same wall: 73 worker
# processes simultaneously calling systemctl restart created a connection
# storm against Postgres (asyncpg pools initialising in parallel × ~20
# connections each = 1460 reserved slots at peak vs 500 max_connections).
# Workers timed out on their first DB acquire and never recovered.
#
# This script restarts the fleet in small groups with sleeps between
# them, so the connection budget is spread over 3-5 minutes instead of
# 30 seconds. Combined with Track 2 (pgbouncer transaction-mode pool,
# already in front) and Track 1 (smaller per-process pool 3/10), this
# is the third leg that keeps connection draw under the ceiling at all
# times.
#
# Usage on prod:
#
#   sudo bash /opt/sofascore/scripts/cascade_restart_workers.sh
#
# Dry-run (prints what would be done, doesn't touch services):
#
#   bash /opt/sofascore/scripts/cascade_restart_workers.sh --dry-run
#
# Estimated wallclock: ~3 minutes. The expensive groups are live-tier-3
# (9 services) and historical-hydrate (30 services); those are the
# steady-state DB consumers.

set -euo pipefail

DRY_RUN="${DRY_RUN:-0}"
if [[ "${1:-}" == "--dry-run" ]]; then
    DRY_RUN=1
fi

# How long to wait between groups. With pgbouncer in front, 3s is more
# than enough for transaction-pool connections to settle.
INTER_GROUP_SLEEP=3
# Historical-hydrate has 30 services — give the cluster a bit more
# breathing room between those batches.
INTER_HISTORICAL_SLEEP=5

restart_group() {
    local label="$1"
    shift
    local services=("$@")
    if [[ ${#services[@]} -eq 0 ]]; then
        return
    fi
    echo
    echo "=== [${label}] restarting ${#services[@]} services ==="
    printf '  %s\n' "${services[@]}"
    if [[ "${DRY_RUN}" == "1" ]]; then
        echo "  (dry-run — no action)"
    else
        sudo systemctl restart "${services[@]}"
    fi
    echo "  → ${label} done"
}

sleep_between() {
    local seconds="${1:-${INTER_GROUP_SLEEP}}"
    if [[ "${DRY_RUN}" == "1" ]]; then
        echo "  (dry-run: would sleep ${seconds}s)"
    else
        sleep "${seconds}"
    fi
}

echo "Cascade restart starting at $(date -Iseconds)"
echo "dry_run=${DRY_RUN}  inter_group_sleep=${INTER_GROUP_SLEEP}s"

# Group 1: API. Single service, fast.
restart_group "api" sofascore-api
sleep_between

# Group 2: operational hydrate, in batches of 3.
restart_group "hydrate-A" sofascore-hydrate@1 sofascore-hydrate@2 sofascore-hydrate@3
sleep_between
restart_group "hydrate-B" sofascore-hydrate@4 sofascore-hydrate@5 sofascore-hydrate@6
sleep_between
restart_group "hydrate-C" sofascore-hydrate@7 sofascore-hydrate@8 sofascore-hydrate@9
sleep_between

# Group 3: live-tier-1 (5 workers).
restart_group "live-tier-1" \
    sofascore-live-tier-1@1 sofascore-live-tier-1@2 \
    sofascore-live-tier-1@3 sofascore-live-tier-1@4 sofascore-live-tier-1@5
sleep_between

# Group 4: live-tier-2 (5 workers).
restart_group "live-tier-2" \
    sofascore-live-tier-2@1 sofascore-live-tier-2@2 \
    sofascore-live-tier-2@3 sofascore-live-tier-2@4 sofascore-live-tier-2@5
sleep_between

# Group 5: live-tier-3 (9 workers, batched).
restart_group "live-tier-3-A" \
    sofascore-live-tier-3@1 sofascore-live-tier-3@2 sofascore-live-tier-3@3
sleep_between
restart_group "live-tier-3-B" \
    sofascore-live-tier-3@4 sofascore-live-tier-3@5 sofascore-live-tier-3@6
sleep_between
restart_group "live-tier-3-C" \
    sofascore-live-tier-3@7 sofascore-live-tier-3@8 sofascore-live-tier-3@9
sleep_between

# Group 6: live-hot + live-warm.
restart_group "live-hot-warm" \
    sofascore-live-hot@1 \
    sofascore-live-warm@1 sofascore-live-warm@2 sofascore-live-warm@3
sleep_between

# Group 7: historical-hydrate (30 workers, in batches of 5).
for batch_start in 1 6 11 16 21 26; do
    batch_end=$((batch_start + 4))
    services=()
    for i in $(seq "$batch_start" "$batch_end"); do
        services+=("sofascore-historical-hydrate@${i}")
    done
    restart_group "historical-hydrate-${batch_start}..${batch_end}" "${services[@]}"
    sleep_between "${INTER_HISTORICAL_SLEEP}"
done

# Group 8: planners (5). Restarted last so they observe the freshly-
# warmed worker pool when they start scheduling.
restart_group "planners" \
    sofascore-final-sync-planner \
    sofascore-planner \
    sofascore-historical-planner \
    sofascore-historical-tournament-planner \
    sofascore-live-discovery-planner

echo
echo "Cascade restart complete at $(date -Iseconds)"

# Quick health sanity.
if [[ "${DRY_RUN}" == "0" ]]; then
    echo
    echo "=== Post-restart health probe ==="
    if curl -fs http://127.0.0.1:8000/ops/health > /dev/null; then
        echo "api OK"
    else
        echo "api FAIL"
    fi
fi
