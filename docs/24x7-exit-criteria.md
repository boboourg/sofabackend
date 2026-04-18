# 24x7 Exit Criteria

## The Bar

The system is only 24x7-ready when all of the following are true:

- no manual `tmux` babysitting is required for normal runtime
- supervised restart exists and is the default runtime control plane
- current architecture documentation matches the real system
- a one-command readiness check exists
- live queues stay fresh under normal daily workload
- historical workers can run without destabilizing live
- hydrate and historical-hydrate logs do not show recurring timeout signatures

## Mandatory Proof Window

Before calling the system 24x7-ready, require at least one full day where:

- scheduled traffic is active
- live refresh traffic is active
- services remain supervised and restartable
- no new `TimeoutError` appears
- `failed` does not trend upward
- `stream:etl:hydrate` does not show sustained growth
- `stream:etl:live_hot` does not show sustained growth

## Required Operational Capabilities

- systemd units or equivalent supervised services
- reproducible deployment steps
- readiness gate script
- operator runbooks for live readiness and rollback
- current source-of-truth runtime architecture docs

## Non-Goals

These do not by themselves prove 24x7 readiness:

- a passing test suite only
- a single clean smoke test only
- a temporary drain of backlog only
- one successful manual intervention window only

## Final Sign-Off Question

The right question is:

- "will the system keep serving the app correctly without us watching it?"

If the answer is still "probably, as long as we keep checking logs", the system is not yet 24x7-ready.
