#!/usr/bin/env bash
set -euo pipefail

cd /opt/sofascore
source .venv/bin/activate
set -a
source .env
set +a

LOCK_FILE="${SOFASCORE_MOBILE_PROXY_WATCHDOG_LOCK_FILE:-/var/lock/sofascore-mobile-proxy-watchdog.lock}"
STATE_DIR="${SOFASCORE_MOBILE_PROXY_WATCHDOG_STATE_DIR:-/var/lib/sofascore/mobile-proxy-watchdog}"
THRESHOLD="${SOFASCORE_MOBILE_PROXY_403_THRESHOLD:-3}"
WINDOW_SECONDS="${SOFASCORE_MOBILE_PROXY_WINDOW_SECONDS:-120}"
ROTATE_COOLDOWN_SECONDS="${SOFASCORE_MOBILE_PROXY_ROTATE_COOLDOWN_SECONDS:-600}"
RECOVERY_WAIT_SECONDS="${SOFASCORE_MOBILE_PROXY_RECOVERY_WAIT_SECONDS:-120}"
SMOKE_TIMEOUT_SECONDS="${SOFASCORE_MOBILE_PROXY_SMOKE_TIMEOUT_SECONDS:-25}"
SMOKE_URL="${SOFASCORE_MOBILE_PROXY_SMOKE_URL:-https://www.sofascore.com/api/v1/event/14149626}"
ROTATE_URL="${SOFASCORE_MOBILE_PROXY_ROTATE_URL:-}"
ENABLED="${SOFASCORE_MOBILE_PROXY_WATCHDOG_ENABLED:-1}"
DATABASE_URL_VALUE="${SOFASCORE_DATABASE_URL:-${DATABASE_URL:-}}"

mkdir -p "$STATE_DIR" "$(dirname "$LOCK_FILE")"
exec 9>"$LOCK_FILE"
if ! flock -n 9; then
  logger -t sofascore-mobile-proxy-watchdog "skip: another watchdog run is already active"
  exit 0
fi

log() {
  local message="$1"
  logger -t sofascore-mobile-proxy-watchdog "$message"
  printf '%s\n' "$message"
}

enabled_normalized="$(printf '%s' "$ENABLED" | tr '[:upper:]' '[:lower:]')"
if [[ "$enabled_normalized" != "1" && "$enabled_normalized" != "true" && "$enabled_normalized" != "yes" && "$enabled_normalized" != "on" ]]; then
  log "skip: watchdog disabled via SOFASCORE_MOBILE_PROXY_WATCHDOG_ENABLED=$ENABLED"
  exit 0
fi

if [[ -z "$DATABASE_URL_VALUE" ]]; then
  log "skip: SOFASCORE_DATABASE_URL is not set"
  exit 0
fi

if [[ -z "$ROTATE_URL" ]]; then
  log "skip: SOFASCORE_MOBILE_PROXY_ROTATE_URL is not set"
  exit 0
fi

current_historical_403s="$(
  psql "$DATABASE_URL_VALUE" -Atqc "
    SELECT COUNT(*)
    FROM api_request_log
    WHERE started_at > now() - interval '${WINDOW_SECONDS} seconds'
      AND http_status = 403
      AND job_type IN (
        'event_detail_backfill',
        'historical_enrichment',
        'historical_hydrate',
        'historical_tournament',
        'historical_discovery'
      );
  " 2>/dev/null || printf '0'
)"
current_historical_403s="${current_historical_403s//$'\r'/}"

if ! [[ "$current_historical_403s" =~ ^[0-9]+$ ]]; then
  log "skip: could not parse historical 403 count: $current_historical_403s"
  exit 0
fi

if (( current_historical_403s < THRESHOLD )); then
  log "healthy: historical_403s=$current_historical_403s threshold=$THRESHOLD window_seconds=$WINDOW_SECONDS"
  exit 0
fi

mapfile -t active_units < <(
  systemctl list-units \
    'sofascore-historical-hydrate@*' \
    'sofascore-historical-enrichment@*' \
    'sofascore-historical-discovery@*' \
    'sofascore-historical-tournament@*' \
    --state=active \
    --plain \
    --no-legend 2>/dev/null | awk '{print $1}'
)

if (( ${#active_units[@]} == 0 )); then
  log "triggered: historical_403s=$current_historical_403s but no active historical http workers found"
  exit 0
fi

printf '%s\n' "${active_units[@]}" > "$STATE_DIR/managed-units.txt"
log "triggered: historical_403s=$current_historical_403s active_units=${#active_units[@]}"

stop_units() {
  if (( ${#active_units[@]} > 0 )); then
    systemctl stop "${active_units[@]}"
  fi
}

start_units() {
  if [[ -f "$STATE_DIR/managed-units.txt" ]]; then
    mapfile -t recorded_units < "$STATE_DIR/managed-units.txt"
    if (( ${#recorded_units[@]} > 0 )); then
      systemctl start "${recorded_units[@]}"
    fi
  fi
}

now_epoch="$(date +%s)"
last_rotate_epoch="0"
if [[ -f "$STATE_DIR/last-rotate-epoch.txt" ]]; then
  last_rotate_epoch="$(tr -d '\r\n' < "$STATE_DIR/last-rotate-epoch.txt")"
fi
if ! [[ "$last_rotate_epoch" =~ ^[0-9]+$ ]]; then
  last_rotate_epoch="0"
fi

cooldown_remaining=$(( ROTATE_COOLDOWN_SECONDS - (now_epoch - last_rotate_epoch) ))
if (( cooldown_remaining > 0 )); then
  log "cooldown: stopping active units but skipping rotate for another ${cooldown_remaining}s"
  stop_units
  exit 0
fi

stop_units
log "rotate: calling MacroDroid trigger"
curl -fsS --max-time 15 "$ROTATE_URL" >/dev/null
printf '%s\n' "$now_epoch" > "$STATE_DIR/last-rotate-epoch.txt"

log "rotate: waiting ${RECOVERY_WAIT_SECONDS}s for mobile proxy recovery"
sleep "$RECOVERY_WAIT_SECONDS"

set +e
.venv/bin/python scripts/ops/mobile_proxy_tls_smoke.py \
  --url "$SMOKE_URL" \
  --timeout "$SMOKE_TIMEOUT_SECONDS"
smoke_exit=$?
set -e

if (( smoke_exit == 0 )); then
  log "recovery: TLS smoke passed, starting previously active historical units"
  start_units
  exit 0
fi

log "recovery: TLS smoke failed, leaving historical units stopped"
exit 1
