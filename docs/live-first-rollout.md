# Live-First Rollout

## Principle

The live application comes first.

Historical completeness is important, but it must not take priority over live freshness and database stability.

## Stage 1: Safe Recovery Profile

Allowed runtime profile:

- planner enabled
- live discovery planner enabled
- discovery enabled
- live discovery enabled
- hydrate workers enabled
- live hot enabled
- live warm enabled
- historical discovery enabled with defer-on-backpressure
- historical hydrate enabled
- historical tournament disabled
- historical enrichment disabled

## Stage 2: Controlled Acceleration

If Stage 1 is stable:

- add one operational hydrate worker at a time
- observe for 15 to 20 minutes
- keep historical bulk disabled

Rollback immediately if:

- new timeout signature appears
- hydrate lag resumes sustained growth
- live hot degrades

## Stage 3: Historical Re-enable Order

Only after both `hydrate` and `historical_hydrate` keep draining safely:

1. enable `historicaltournament`
2. wait 15 to 20 minutes
3. enable `historicalenrichment1`
4. wait 15 to 20 minutes
5. enable `historicalenrichment2`

Do not enable all historical stages at once.

## Promotion Gates

Promotion requires:

- `hydrate lag` decreasing for at least 30 minutes
- `historical_hydrate lag` decreasing for at least 30 minutes
- no new `TimeoutError`
- no increase in `failed`

## What Not To Do

- do not jump straight from degraded recovery to full historical bulk
- do not add many hydrate workers at once
- do not treat "system survived" as proof that live freshness is healthy
