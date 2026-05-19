# Coverage Gap 2015-2019 — Investigation & Plan

**Date**: 2026-05-19
**Status**: Investigated, documented, partial mitigation in place (Item 1+4)
**Owner**: TBD

## TL;DR

Period 2015-2019 has 3-17K finished events per year, vs 70-220K for adjacent years (2014, 2020+). The gap exists because the **strict cat-priority backfill barrier** holds cat=19+ UTs (EPL, La Liga, Bundesliga, UCL classic, EURO history) behind cat=20 (FIFA WC, EURO, Olympics) indefinitely. Cat=19 cursors have **never advanced** for most UTs.

Items 1 and 4 (2026-05-19) decoupled `/rounds` and `/cuptrees` catalog fetching from the cursor walk via dedicated resource refresh scopes — this fills the *metadata* gap for historical cup-style seasons. But the **full event/lineup/statistics backfill** for 2015-2019 EPL/La Liga/Bundesliga/UCL seasons is still blocked by the strict barrier.

## Data

| Year | events | finished | Status |
|---:|---:|---:|---|
| 2026 | 295,088 | 219,843 | current |
| 2025 | 238,511 | 226,289 | healthy |
| 2024 | 91,832 | 86,274 | healthy |
| 2023 | 174,516 | 163,617 | healthy |
| 2022 | 193,136 | 182,319 | healthy (FIFA WC) |
| 2021 | 159,224 | 149,245 | healthy |
| 2020 | 92,820 | 78,334 | healthy |
| **2019** | **17,869** | **17,177** | **gap** |
| **2018** | **6,611** | **6,290** | **gap** |
| **2017** | **7,283** | **7,176** | **gap** |
| **2016** | **3,241** | **3,183** | **gap** |
| **2015** | **4,522** | **4,406** | **gap** |
| 2014 | 97,803 | 87,237 | healthy (FIFA WC bump) |
| 2013 | 23,040 | 21,788 | partial |
| 2012-2010 | 3-14K | similar | thin |

## Sport-level coverage 2015-2019

| Sport | UTs with 2015-2019 data | Total active UTs |
|---|---:|---:|
| football | 166 | 5,463 (3%) |
| esports | 138 | 974 (14%) |
| basketball | 59 | 919 (6%) |
| volleyball | 48 | 366 (13%) |
| tennis | 40 | 1,162 (3%) |
| handball | 38 | 403 (9%) |
| table-tennis | 32 | 447 (7%) |
| rugby | 30 | 203 (15%) |
| cricket | 23 | 532 (4%) |
| futsal | 16 | 709 (2%) |
| ice-hockey | 16 | 114 (14%) |
| baseball | 15 | 83 (18%) |
| american-football | 0 | 23 (0%) |

3-18% coverage for the gap period across all sports.

## Root Cause

The historical tournament planner uses
`TournamentRegistryRepository.select_pending_cursors_by_top_category`
which implements a **strict cat-priority barrier**:

```sql
WITH top_cat AS (
    SELECT MAX(c.priority) FROM tournament_registry tr
    JOIN unique_tournament ut ON ...
    JOIN category c ON ...
    WHERE tr.is_active AND tr.historical_enabled
      AND tr.next_season_backfill_id > 0
)
SELECT ... FROM pending p, top_cat
WHERE p.category_priority = top_cat.top_cat_priority
```

So the planner emits jobs **only for the highest pending priority bucket**. Cat=20 (World — FIFA WC, EURO, Olympics, etc., 51 UTs with all of history) takes weeks/months to drain.

**Result**: cat=19 (Europe — UCL, EURO history, Bundesliga, La Liga, Premier League historical), cat=18-7, and cat=0 (everything else) **never get cursor walks** until cat=20 drains.

Cursor states confirm:

```
UT | name | cat | next_season_backfill_id | last_advance_at
---+------+-----+-------------------------+------------------
1  | EURO | 19  | 56953  | NULL (never advanced)
7  | UCL  | 19  | 76953  | NULL
11 | WC Qual UEFA | 19 | 69427 | NULL
...
```

Most cat=19+ UTs have `backfill_last_advance_at = NULL`.

## What Items 1 + 4 Fixed

* Item 1 — `/rounds` historical scope: UCL/EURO/etc. round catalogs land in `season_round` regardless of cursor walk. Unlocks Phase 4 routing for these UTs when their cursor eventually advances.
* Item 4 — `/cuptrees` historical scope: bracket structure lands in `season_cup_tree`. Frontend bracket pages work for historical cups.

Combined, frontend Group/Round/Bracket pages for historical UCL, EURO, EPL (which has rounds) now serve correctly because the metadata is decoupled. But **event-level data** (per-match lineups, statistics, incidents, etc.) for 2015-2019 still requires the cursor walk that's blocked.

## Proposed Fixes (Future Work)

### Option A — Soft Barrier (Quota-based Priority)

Replace strict `WHERE category_priority = MAX` with a quota distribution:

```python
# Planner-side allocation per tick:
cat=20: 60% of publish slots
cat=19: 20%
cat=18-10: 15%
cat=0: 5%
```

* **Pros**: Architecturally clean, automatically distributes work, no operator intervention needed.
* **Cons**: cat=20 drains slower (still acceptable — it's already 10+ advances/hour). Requires planner SQL rewrite + extensive test coverage.
* **Estimated effort**: 1-2 days.

### Option B — Operator Burst CLI

Add `python -m schema_inspector.cli backfill-cursor force-advance --ut <UT> --back-to-year <YYYY>` that:

1. Sets `tournament_registry.next_season_backfill_id` to the newest season older than `back-to-year`
2. Publishes the historical_tournament job directly to the stream, **bypassing the planner's barrier**
3. Worker processes that one season; advance gate naturally moves cursor backwards
4. Operator can re-run for next season manually

* **Pros**: Surgical, minimal architectural change, immediate unblock for specific UTs.
* **Cons**: Manual per-UT operation. For 5,300 UTs × ~10 historical seasons each = a lot of clicks. Practical only for top 10-20 UTs.
* **Estimated effort**: 2-4 hours.

### Option C — Dedicated Historical Lane

Separate stream `stream:etl:historical_backfill_burst` with:

* Its own planner that reads cat=19+ UTs ignoring barrier
* Small budget (5-10 jobs/tick) so it doesn't compete with main historical_tournament lane
* Worker consumes from both streams

* **Pros**: Decoupling like Item 1+4, runs in parallel without slowing cat=20 drain.
* **Cons**: New stream + planner + worker = more infrastructure to monitor.
* **Estimated effort**: 1-2 days.

### Option D — Time-based Override

Once a week (or on demand), planner switches to "historical sweep" mode for 1 hour: ignores barrier, walks all UTs ordered by `last_advance_at NULLS FIRST`. Then reverts to normal.

* **Pros**: Self-healing, periodic.
* **Cons**: Implementation complexity (mode switching), risk of edge cases.
* **Estimated effort**: 1 day.

## Recommendation

**Short-term (this session)**: Items 1 + 4 fill the metadata gap (rounds + cuptrees) for cup-style historical seasons. Frontend group/bracket pages work.

**Medium-term (next sprint)**: **Option A (soft barrier)** — proper architectural fix. Provides continuous historical drain alongside cat=20 progress. Test coverage will validate that strict-barrier semantics aren't broken at boundaries.

**Short-term operator workaround**: **Option B (burst CLI)** lets operator manually unblock top 20 UTs (EPL, La Liga, Bundesliga, UCL, etc.) until Option A lands.

## Tracking

* Item 1 (rounds historical scope) — DONE 2026-05-19 (commit `69298bb`)
* Item 4 (cuptrees historical scope) — DONE 2026-05-19 (commit `381bf00`)
* Soft barrier (Option A) — DEFERRED
* Burst CLI (Option B) — OPTIONAL, may not be needed if Option A lands

## Verification

Once Option A or B lands, verify via:

```sql
SELECT s.year, COUNT(*) FILTER (WHERE e.status_code = 100) AS finished
FROM event e JOIN season s ON s.id = e.season_id
WHERE s.year ~ '^[0-9]+$'
GROUP BY s.year
ORDER BY s.year::int DESC LIMIT 20;
```

Expected: 2015-2019 each grows from ~3-17K to 30K+ finished events.
