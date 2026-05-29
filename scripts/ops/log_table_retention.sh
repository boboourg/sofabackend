#!/usr/bin/env bash
#
# log_table_retention.sh — bound the disposable audit-log tables that grow
# unbounded and have no built-in retention.
#
# Background (2026-05-29 audit): three pure audit/diagnostic logs were the
# dominant disk-pressure drivers (disk hit 99% / 7.8 GB free):
#
#   event_endpoint_availability_log   22 GB   (~4 GB/day regrowth, ~680 rows/min)
#   endpoint_capability_observation   12 GB   (inline-rollup firebreak path)
#   api_request_log                    7.4 GB
#
# They were TRUNCATEd once for immediate relief (+42 GB, disk → 89%), but
# they regrow fast, so this timer keeps them bounded going forward. They are
# disposable diagnostics (confirmed): no incoming FKs, the active capability
# system is league_endpoint_capability (separate), live serving doesn't read
# them.
#
# Strategy: age-based batched DELETE (keep RETENTION_DAYS, default 2). Batched
# in small chunks so each statement's WAL footprint stays tiny — critical
# given the disk-full history (a single huge DELETE could spike WAL and
# refill the disk). No VACUUM FULL (lock + rewrite risk on a loaded box, per
# the 2026-05-25 VACUUM-OOM lesson); the freed tuples go to the freelist and
# are reused by the steady insert stream, so the file stays bounded.

set -uo pipefail
cd /opt/sofascore

RETENTION_DAYS="${LOG_RETENTION_DAYS:-2}"
BATCH="${LOG_RETENTION_BATCH:-50000}"
STMT_TIMEOUT_MS="${LOG_RETENTION_STMT_TIMEOUT_MS:-600000}"

PSQL=(psql "${SOFASCORE_DATABASE_URL}" -v ON_ERROR_STOP=1 -t -A)

# table:timestamp_column
TARGETS=(
  "event_endpoint_availability_log:observed_at"
  "endpoint_capability_observation:observed_at"
  "api_request_log:started_at"
)

echo "[$(date -Iseconds)] log_table_retention: start keep=${RETENTION_DAYS}d batch=${BATCH}"

for entry in "${TARGETS[@]}"; do
  table="${entry%%:*}"
  ts_col="${entry##*:}"
  total=0
  # Loop batched deletes until a batch removes 0 rows (nothing older left).
  while : ; do
    deleted=$("${PSQL[@]}" <<SQL
BEGIN;
SET LOCAL statement_timeout = '${STMT_TIMEOUT_MS}';
WITH doomed AS (
    SELECT ctid FROM ${table}
    WHERE ${ts_col} < now() - interval '${RETENTION_DAYS} days'
    LIMIT ${BATCH}
)
DELETE FROM ${table} t USING doomed d WHERE t.ctid = d.ctid;
COMMIT;
SQL
)
    # psql prints "DELETE N" for the last command; extract N
    n=$(printf '%s\n' "${deleted}" | grep -oE 'DELETE [0-9]+' | awk '{print $2}' | tail -1)
    n="${n:-0}"
    total=$((total + n))
    [ "${n}" -lt "${BATCH}" ] && break
    sleep 1   # let WAL / checkpoint breathe between batches
  done
  echo "[$(date -Iseconds)] log_table_retention: ${table} pruned=${total} (older than ${RETENTION_DAYS}d)"
done

echo "[$(date -Iseconds)] log_table_retention: done"
