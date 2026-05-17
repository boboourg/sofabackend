import json
import time
import urllib.request

t0 = time.time()
with urllib.request.urlopen("http://127.0.0.1:8000/api/v1/sport/football/events/live", timeout=20) as r:
    data = json.loads(r.read())
elapsed = (time.time() - t0) * 1000
events = data.get("events", [])
print(f"elapsed: {elapsed:.0f}ms, events: {len(events)}")
events_with_time = [e for e in events if "time" in e]
print(f"events with time block: {len(events_with_time)}")
if events_with_time:
    sample = events_with_time[0]["time"]
    print("sample time block:")
    print(json.dumps(sample, indent=2))
events_with_injury = [e for e in events if e.get("time", {}).get("injuryTime1") is not None]
print(f"events with injuryTime1: {len(events_with_injury)}")
