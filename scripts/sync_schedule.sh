#!/usr/bin/env bash
set -euo pipefail

cd /opt/sofascore

PYTHON_BIN="/opt/sofascore/.venv/bin/python"
if [[ ! -x "$PYTHON_BIN" ]]; then
  PYTHON_BIN="/opt/sofascore/venv/bin/python"
fi

SPORTS=(
  football
  tennis
  basketball
  volleyball
  baseball
  american-football
  handball
  table-tennis
  ice-hockey
  rugby
  cricket
  futsal
  esports
)

PAUSE_SECONDS="${SCHEDULE_SYNC_PAUSE_SECONDS:-2}"
FAR_HORIZON_DAYS="${SCHEDULE_SYNC_FAR_HORIZON_DAYS:-30}"
MODE="${1:-near}"
DATE="${2:-}"

usage() {
  cat <<'EOF'
Usage:
  sync_schedule.sh near
  sync_schedule.sh horizon
  sync_schedule.sh date YYYY-MM-DD
  sync_schedule.sh range YYYY-MM-DD YYYY-MM-DD

Modes:
  near     Today and tomorrow for all 13 sports.
  horizon  From tomorrow (+1) through +30 days for all 13 sports.
  date     One explicit date for all 13 sports.
  range    Inclusive date range for all 13 sports.
EOF
}

run_scheduled_tournaments() {
  local sport="$1"
  local observed_date="$2"
  local page=1

  while true; do
    echo "scheduled_tournaments sport=${sport} date=${observed_date} page=${page}"
    local output
    output="$("$PYTHON_BIN" -m schema_inspector.scheduled_tournaments_cli \
      --sport-slug "$sport" \
      --date "$observed_date" \
      --page "$page")"
    echo "$output"

    if [[ "$output" == *"has_next_page=True"* ]]; then
      page=$((page + 1))
      sleep "$PAUSE_SECONDS"
      continue
    fi

    break
  done
}

run_scheduled_events() {
  local sport="$1"
  local observed_date="$2"

  echo "scheduled_events sport=${sport} date=${observed_date}"
  "$PYTHON_BIN" -m schema_inspector.event_list_cli \
    --sport-slug "$sport" \
    scheduled \
    --date "$observed_date"
}

run_for_sport_date() {
  local sport="$1"
  local observed_date="$2"

  run_scheduled_tournaments "$sport" "$observed_date"
  sleep "$PAUSE_SECONDS"
  run_scheduled_events "$sport" "$observed_date"
  sleep "$PAUSE_SECONDS"
}

run_for_date() {
  local observed_date="$1"
  for sport in "${SPORTS[@]}"; do
    run_for_sport_date "$sport" "$observed_date"
  done
}

date_plus_days() {
  local offset="$1"
  date -d "$offset day" +%F
}

case "$MODE" in
  near)
    run_for_date "$(date +%F)"
    run_for_date "$(date_plus_days +1)"
    ;;
  horizon)
    for offset in $(seq 1 "$FAR_HORIZON_DAYS"); do
      run_for_date "$(date_plus_days +"$offset")"
    done
    ;;
  date)
    if [[ -z "$DATE" ]]; then
      usage >&2
      exit 2
    fi
    run_for_date "$DATE"
    ;;
  range)
    END_DATE="${3:-}"
    if [[ -z "$DATE" || -z "$END_DATE" ]]; then
      usage >&2
      exit 2
    fi

    current="$DATE"
    while [[ "$current" < "$END_DATE" || "$current" == "$END_DATE" ]]; do
      run_for_date "$current"
      current="$(date -d "$current +1 day" +%F)"
    done
    ;;
  *)
    usage >&2
    exit 2
    ;;
esac
