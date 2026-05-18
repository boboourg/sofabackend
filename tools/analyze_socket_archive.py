"""Read socket_archive.jsonl and produce a profile:
  * counts per (sport, type)
  * unique keys per type (event vs odds)
  * top-frequency events (which event_ids get the most updates)
  * timing histogram (updates per second)
  * end-to-end coverage estimate (how many distinct events / how often)
"""
from __future__ import annotations
import json
import sys
from collections import Counter, defaultdict
from pathlib import Path


PATH = Path(r"D:\projects\sofabackend\sofabackend\sofaws\socket_archive.jsonl")

count_total = 0
count_by_sport_type: Counter = Counter()
keys_event: Counter = Counter()
keys_odds: Counter = Counter()
event_id_freq: Counter = Counter()  # for event-type only
odds_id_freq: Counter = Counter()   # for odds-type only
ts_per_second: Counter = Counter()
distinct_sports: set = set()
distinct_event_ids: set = set()
distinct_odds_ids: set = set()

# Status code mappings encountered
status_codes_seen: Counter = Counter()
status_descriptions_seen: Counter = Counter()

# Sample one full payload of each unique key combination
sample_event_payloads_by_keyshape: dict[frozenset, dict] = {}
sample_odds_payloads_by_keyshape: dict[frozenset, dict] = {}

with PATH.open("r", encoding="utf-8") as f:
    for line in f:
        count_total += 1
        try:
            entry = json.loads(line)
        except json.JSONDecodeError:
            continue
        sport = entry.get("sport", "?")
        type_ = entry.get("type", "?")
        payload = entry.get("payload") or {}
        count_by_sport_type[(sport, type_)] += 1
        distinct_sports.add(sport)
        # second-bucket
        ts = entry.get("pushed_at", "")
        if ts:
            ts_per_second[ts[:19]] += 1
        # keys analysis
        keys = frozenset(payload.keys())
        if type_ == "event":
            for k in keys:
                keys_event[k] += 1
            eid = payload.get("id")
            if eid is not None:
                event_id_freq[eid] += 1
                distinct_event_ids.add(eid)
            if "status.code" in payload:
                status_codes_seen[payload["status.code"]] += 1
            if "status.description" in payload:
                status_descriptions_seen[payload["status.description"]] += 1
            if keys not in sample_event_payloads_by_keyshape:
                sample_event_payloads_by_keyshape[keys] = payload
        elif type_ == "odds":
            for k in keys:
                keys_odds[k] += 1
            oid = payload.get("id")
            if oid is not None:
                odds_id_freq[oid] += 1
                distinct_odds_ids.add(oid)
            if keys not in sample_odds_payloads_by_keyshape:
                sample_odds_payloads_by_keyshape[keys] = payload

        if count_total % 100000 == 0:
            print(f"... processed {count_total} lines", file=sys.stderr)

print(f"\n=== TOTALS ===")
print(f"lines: {count_total}")
print(f"distinct sports: {len(distinct_sports)} -> {sorted(distinct_sports)}")
print(f"distinct event ids: {len(distinct_event_ids)}")
print(f"distinct odds ids: {len(distinct_odds_ids)}")

print(f"\n=== COUNTS BY (sport, type) ===")
for (sport, type_), n in sorted(count_by_sport_type.items(), key=lambda x: -x[1]):
    print(f"  {sport:20s} {type_:8s} {n}")

print(f"\n=== EVENT-TYPE KEYS (top 40) ===")
for k, n in keys_event.most_common(40):
    print(f"  {k:50s} {n}")

print(f"\n=== ODDS-TYPE KEYS (top 30) ===")
for k, n in keys_odds.most_common(30):
    print(f"  {k:50s} {n}")

# Time distribution
secs = sorted(ts_per_second)
print(f"\n=== TIME ===")
print(f"first sec: {secs[0] if secs else '?'}")
print(f"last sec: {secs[-1] if secs else '?'}")
print(f"distinct seconds: {len(secs)}")
duration_secs = len(secs)
if secs:
    print(f"avg msgs/sec across the archive: {count_total / max(duration_secs, 1):.1f}")

# Per-sport rate
print(f"\n=== PEAK BURST (top 10 busiest seconds) ===")
for sec, n in ts_per_second.most_common(10):
    print(f"  {sec}  {n} msgs")

# Frequency of updates per event
print(f"\n=== EVENT UPDATE FREQ (top 10 events with most deltas) ===")
for eid, n in event_id_freq.most_common(10):
    print(f"  event_id={eid}  {n} deltas")

# Sample distinct payload shapes (event type)
print(f"\n=== DISTINCT EVENT PAYLOAD SHAPES: {len(sample_event_payloads_by_keyshape)} ===")
print(f"top 15 by keyset:")
shape_counts: Counter = Counter()
with PATH.open("r", encoding="utf-8") as f:
    for line in f:
        try:
            entry = json.loads(line)
        except json.JSONDecodeError:
            continue
        if entry.get("type") != "event":
            continue
        keys = frozenset((entry.get("payload") or {}).keys())
        shape_counts[keys] += 1
for shape, n in shape_counts.most_common(15):
    sorted_keys = sorted(shape)
    short_keys = ", ".join(sorted_keys[:8])
    if len(sorted_keys) > 8:
        short_keys += f", ... ({len(sorted_keys)} total)"
    print(f"  {n:8d} -- {short_keys}")

# Status code mapping
print(f"\n=== STATUS CODES SEEN IN EVENT DELTAS ===")
for code, n in sorted(status_codes_seen.items(), key=lambda x: -x[1])[:25]:
    print(f"  {code:4} ({n} occurrences)")

print(f"\n=== STATUS DESCRIPTIONS ===")
for desc, n in sorted(status_descriptions_seen.items(), key=lambda x: -x[1])[:25]:
    print(f"  {desc:25s} {n}")
