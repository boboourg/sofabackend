import json
import time
import urllib.request

for eid in [15235568, 14023956, 16191130]:
    print(f"\n--- /event/{eid} ---")
    t0 = time.time()
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:8000/api/v1/event/{eid}", timeout=20) as r:
            data = json.loads(r.read())
        elapsed = (time.time() - t0) * 1000
        e = data.get("event", {})
        home = e.get("homeTeam", {}).get("name")
        away = e.get("awayTeam", {}).get("name")
        print(f"  elapsed: {elapsed:.0f}ms")
        print(f"  id={e.get('id')} {home} vs {away}")
        print(f"  status: {e.get('status')}")
        has_changes = "changes" in e
        print(f"  has changes: {has_changes}")
        if has_changes:
            print(f"  changes: {e['changes']}")
    except Exception as exc:
        print(f"  FAIL: {exc}")
