"""Run the WS delta normalizer over the full socket_archive.jsonl and
report:
  * how many event/odds deltas normalise cleanly (no None, no exception)
  * which payload keys are still unmapped (would silently drop)
  * basic shape stats on the produced bundles
"""
from __future__ import annotations
import json
import sys
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from schema_inspector.ws_delta_normalizer import (  # noqa: E402
    normalize_event_delta,
    normalize_odds_delta,
)

PATH = Path(r"D:\projects\sofabackend\sofabackend\sofaws\socket_archive.jsonl")

KNOWN_EVENT_KEYS = {
    "id", "changes.changeTimestamp", "winnerCode", "lastPeriod",
    "firstToServe", "statusDescription", "currentPeriodStartTimestamp",
    "statusTime", "varInProgress", "cardsCode",
}
KNOWN_EVENT_PREFIXES = (
    "homeScore.", "awayScore.", "status.", "time.", "eventState.",
)

count_event = 0
count_odds = 0
count_event_normalized = 0
count_odds_normalized = 0
count_event_id_only = 0
exceptions: Counter = Counter()
unmapped_keys: Counter = Counter()

# Track derived stats
sides_per_delta: Counter = Counter()
fields_per_delta: Counter = Counter()

with PATH.open("r", encoding="utf-8") as f:
    for line_no, line in enumerate(f, 1):
        try:
            entry = json.loads(line)
        except json.JSONDecodeError:
            continue

        payload = entry.get("payload") or {}
        type_ = entry.get("type")

        if type_ == "event":
            count_event += 1
            # Track unmapped keys
            for k in payload.keys():
                if k in KNOWN_EVENT_KEYS:
                    continue
                if any(k.startswith(p) for p in KNOWN_EVENT_PREFIXES):
                    continue
                unmapped_keys[k] += 1
            try:
                delta = normalize_event_delta(payload)
            except Exception as e:
                exceptions[type(e).__name__] += 1
                continue
            if delta is None:
                continue
            count_event_normalized += 1
            n_sides = len(delta.event_score_rows)
            sides_per_delta[n_sides] += 1
            n_fields = (
                int(delta.change_timestamp is not None)
                + n_sides
                + len(delta.event_fields)
                + len(delta.event_status_fields)
                + len(delta.event_time_fields)
                + len(delta.event_status_time_fields)
            )
            fields_per_delta[n_fields] += 1
            if n_fields == 0:
                # id-only deltas (no state, only event_id)
                count_event_id_only += 1
        elif type_ == "odds":
            count_odds += 1
            try:
                bundle = normalize_odds_delta(payload)
            except Exception as e:
                exceptions[type(e).__name__] += 1
                continue
            if bundle is None:
                continue
            count_odds_normalized += 1

        if line_no % 200000 == 0:
            print(f"... {line_no} lines processed", file=sys.stderr)

print(f"\n=== EVENT-TYPE NORMALIZATION ===")
print(f"total: {count_event}")
print(f"normalised: {count_event_normalized} ({100*count_event_normalized/max(count_event,1):.1f}%)")
print(f"id-only (no fields): {count_event_id_only}")
print(f"\nexceptions: {dict(exceptions)}")
print(f"\nfields-per-delta distribution (top 10):")
for n, c in fields_per_delta.most_common(10):
    print(f"  {n:3d} fields  -> {c}")

print(f"\nsides-per-delta:")
for s, c in sorted(sides_per_delta.items()):
    print(f"  {s} side(s)  -> {c}")

print(f"\n=== ODDS-TYPE NORMALIZATION ===")
print(f"total: {count_odds}")
print(f"normalised: {count_odds_normalized} ({100*count_odds_normalized/max(count_odds,1):.1f}%)")

print(f"\n=== UNMAPPED KEYS (NOT in known set or known prefix) ===")
if unmapped_keys:
    for k, c in unmapped_keys.most_common(25):
        print(f"  {k:50s} {c}")
else:
    print(f"  (none — every event key in {count_event} deltas mapped)")
