# Housekeeping Env Compatibility Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make housekeeping honor the env names currently used in production without breaking the canonical config names already used by the codebase.

**Architecture:** Keep the existing `HousekeepingConfig` shape and parsing entrypoint, but teach `from_env()` to accept a small compatibility layer of legacy/operational aliases for interval, batch size, and zombie max age. Protect the change with focused unit tests in the existing housekeeping test module and document the alias behavior in the housekeeping module docstring so rollout is predictable.

**Tech Stack:** Python 3.11, unittest, existing housekeeping service/config parsing.

---

### Task 1: Add Regression Tests For Production Env Aliases

**Files:**
- Modify: `D:/sofascore/tests/test_housekeeping.py`

- [ ] **Step 1: Add a focused failing test for the env aliases used in production**

```python
def test_housekeeping_config_accepts_operational_env_aliases(self) -> None:
    from schema_inspector.services.housekeeping import HousekeepingConfig

    config = HousekeepingConfig.from_env(
        {
            "SOFASCORE_HOUSEKEEPING_ENABLED": "true",
            "SOFASCORE_HOUSEKEEPING_DRY_RUN": "true",
            "SOFASCORE_HOUSEKEEPING_INTERVAL_SECONDS": "60",
            "SOFASCORE_HOUSEKEEPING_BATCH_SIZE": "20000",
            "SOFASCORE_HOUSEKEEPING_ZOMBIE_MAX_AGE_MINUTES": "120",
        }
    )

    self.assertTrue(config.enabled)
    self.assertTrue(config.dry_run)
    self.assertEqual(config.interval_s, 60.0)
    self.assertEqual(config.batch_size, 20_000)
    self.assertEqual(config.zombie_max_age_minutes, 120)
```

- [ ] **Step 2: Run the targeted test and verify it fails for the expected reason**

Run: `python -m unittest tests.test_housekeeping.HousekeepingConfigParsingTests.test_housekeeping_config_accepts_operational_env_aliases -v`

Expected: FAIL because `interval_s` and `batch_size` still come from defaults.

---

### Task 2: Implement Alias-Aware Parsing In HousekeepingConfig

**Files:**
- Modify: `D:/sofascore/schema_inspector/services/housekeeping.py`

- [ ] **Step 1: Add alias constants alongside the canonical env names**

```python
_ENV_INTERVAL_ALIASES = (_ENV_PREFIX + "HOUSEKEEPING_INTERVAL_SECONDS",)
_ENV_BATCH_SIZE_ALIASES = (_ENV_PREFIX + "HOUSEKEEPING_BATCH_SIZE",)
_ENV_ZOMBIE_MAX_AGE_ALIASES = (_ENV_PREFIX + "HOUSEKEEPING_ZOMBIE_MAX_AGE_MINUTES",)
```

- [ ] **Step 2: Update `HousekeepingConfig.from_env()` to prefer canonical names but fall back to aliases**

```python
interval_s=_env_float_any(env, (_ENV_INTERVAL_S, *_ENV_INTERVAL_ALIASES), 300.0),
batch_size=_env_int_any(env, (_ENV_BATCH_SIZE, *_ENV_BATCH_SIZE_ALIASES), 5_000, minimum=1),
zombie_max_age_minutes=_env_int_any(
    env,
    (_ENV_ZOMBIE_MAX_AGE_MIN, *_ENV_ZOMBIE_MAX_AGE_ALIASES),
    120,
    minimum=1,
),
```

- [ ] **Step 3: Add tiny shared helpers for “first present env wins” parsing**

```python
def _env_first(env: dict[str, str], names: tuple[str, ...]) -> str | None:
    for name in names:
        value = env.get(name)
        if value is not None:
            return value
    return None
```

Then wrap the existing scalar parsers so behavior stays identical once a value is selected.

---

### Task 3: Document And Verify

**Files:**
- Modify: `D:/sofascore/schema_inspector/services/housekeeping.py`
- Modify: `D:/sofascore/tests/test_housekeeping.py`

- [ ] **Step 1: Update the module docstring to note the supported operational aliases**

```python
Every numeric knob is driven by environment variables. The canonical names
remain the source of truth, but `HousekeepingConfig.from_env()` also accepts
the operational aliases currently used in deployed `.env` files for interval,
batch size, and zombie max age.
```

- [ ] **Step 2: Run the focused housekeeping test module**

Run: `python -m unittest tests.test_housekeeping -v`

Expected: PASS.

- [ ] **Step 3: Run the higher-level health tests that depend on housekeeping flags**

Run: `python -m unittest tests.test_ops_health tests.test_hybrid_cli -v`

Expected: PASS.
