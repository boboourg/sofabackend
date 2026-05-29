# Redis runtime config (prod)

The prod Redis runs as a **manually-launched Docker container** (`my-redis`,
`RestartPolicy=unless-stopped`) — there is no compose file or systemd unit
managing it. It was last recreated during the 2026-05-26 disk-full incident.

## Required settings (anti-OOM)

The 2026-05-26 incident root cause included Redis growing unbounded until the
host/cgroup OOM-killed it, leaving containerd in a stuck state. To make Redis
**fail-safe** (shed regenerable cache, then refuse writes) *before* the Docker
16 GiB cgroup hard-kills it:

```
maxmemory          12gb
maxmemory-policy   volatile-ttl
```

- `12gb` sits below the Docker `--memory 16g` cgroup limit (≈5× the observed
  ~2.2 GB working-set peak), so Redis hits its own soft ceiling first.
- `volatile-ttl` evicts only keys **with a TTL** — the regenerable caches
  (`api:resp:*`, dedupe, freshness, lease, capability cache). Streams
  (`stream:etl:*`) and live-state ZSETs (`zset:live:*`) have **no TTL**, so
  they are **never evicted** — no data loss, no consumer-group corruption.
  When nothing evictable remains it falls back to refusing writes (recoverable;
  workers retry) rather than a host OOM-kill (catastrophic).

## Applied at runtime 2026-05-29

```
docker exec my-redis redis-cli -a <pw> CONFIG SET maxmemory 12gb
docker exec my-redis redis-cli -a <pw> CONFIG SET maxmemory-policy volatile-ttl
```

`CONFIG SET` is **runtime-only** — the container was started with CLI args and
no config file, so `CONFIG REWRITE` does not persist. The setting survives as
long as the container runs (which, being `unless-stopped`, is effectively
indefinite).

## On the NEXT container recreate — bake the flags in

Add `--maxmemory 12gb --maxmemory-policy volatile-ttl` to the `docker run`:

```
docker run -d --name my-redis --restart unless-stopped --memory 16g \
  -p 127.0.0.1:6379:6379 -v redis_data:/data \
  redis:8 redis-server \
    --requirepass <pw> --protected-mode yes --bind 0.0.0.0 \
    --appendonly no --save "" \
    --maxmemory 12gb --maxmemory-policy volatile-ttl
```

(`--appendonly no --save ""` keep the post-incident no-persistence posture —
the working set is rebuildable from Postgres + a few minutes of live polling.)
