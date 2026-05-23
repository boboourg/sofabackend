"""Phase 4.7.6 Track 1 Step 1 (2026-05-23): cut asyncpg per-process pool
defaults from min=20/max=50 down to min=3/max=10.

Three failed Phase 4.8 flips (with progressively better code: 12-query →
1-query batch → Redis-only-after-warm) all hit the same wall: 73 worker
processes × asyncpg pool min_size=20 = ~1460 reserved connections vs
PostgreSQL's effective max_connections (~100 cluster-wide). Any concurrent
DB activity at worker startup caused connection storm → 30 s
statement-timeouts → throughput collapse.

The fix has three parallel tracks (see task #124). This commit is Track
1 Step 1: shrink the per-process pool footprint. With pgbouncer in front
(Track 2, infra) and a Redis-only worker hot path (Track 1 Step 2,
upcoming), workers don't need 20 reserved connections — they need just
enough to handle their own writes (snapshot persist, job_run insert,
event_terminal_state upsert). 3 minimum and 10 maximum is enough for
the actual work each worker process does.

Result of the new defaults:

  73 workers × min=3 = 219 connections requested (vs 1460 before)
  73 workers × max=10 = 730 cap (vs 3650 before)

Combined with pgbouncer's transaction-mode multiplexing (Track 2),
that becomes ≤25 simultaneous real Postgres connections at any time.
Operator can still tune via SOFASCORE_PG_MIN_SIZE / SOFASCORE_PG_MAX_SIZE
env if a specific deployment needs different sizing.
"""

from __future__ import annotations

import unittest


class DefaultPoolSizingTests(unittest.TestCase):
    """The dataclass default and the load_database_config default must
    both produce the new conservative numbers — they're set in two
    places and Phase 4.8 flips proved that one without the other is a
    trap."""

    def test_database_config_default_min_size_is_three(self) -> None:
        from schema_inspector.db import DatabaseConfig

        config = DatabaseConfig(dsn="postgresql://stub/db")
        self.assertEqual(
            config.min_size, 3,
            "Phase 4.7.6: per-process pool min_size must be 3 (was 20). "
            "Higher defaults overwhelm Postgres max_connections across "
            "the 73-worker fleet.",
        )

    def test_database_config_default_max_size_is_ten(self) -> None:
        from schema_inspector.db import DatabaseConfig

        config = DatabaseConfig(dsn="postgresql://stub/db")
        self.assertEqual(
            config.max_size, 10,
            "Phase 4.7.6: per-process pool max_size must be 10 (was 50).",
        )

    def test_load_database_config_default_min_size_is_three(self) -> None:
        """The CLI / worker entrypoints all go through load_database_config,
        not the raw dataclass. Both must agree."""
        from schema_inspector.db import load_database_config

        config = load_database_config(
            env={"SOFASCORE_DATABASE_URL": "postgresql://stub/db"},
        )
        self.assertEqual(config.min_size, 3)

    def test_load_database_config_default_max_size_is_ten(self) -> None:
        from schema_inspector.db import load_database_config

        config = load_database_config(
            env={"SOFASCORE_DATABASE_URL": "postgresql://stub/db"},
        )
        self.assertEqual(config.max_size, 10)

    def test_env_var_overrides_min_size_default(self) -> None:
        """Operator escape hatch: a specific deployment can still pin a
        larger pool through env, the new conservative defaults are only
        the *default*."""
        from schema_inspector.db import load_database_config

        config = load_database_config(
            env={
                "SOFASCORE_DATABASE_URL": "postgresql://stub/db",
                "SOFASCORE_PG_MIN_SIZE": "15",
            },
        )
        self.assertEqual(config.min_size, 15)

    def test_env_var_overrides_max_size_default(self) -> None:
        from schema_inspector.db import load_database_config

        config = load_database_config(
            env={
                "SOFASCORE_DATABASE_URL": "postgresql://stub/db",
                "SOFASCORE_PG_MAX_SIZE": "40",
            },
        )
        self.assertEqual(config.max_size, 40)


if __name__ == "__main__":
    unittest.main()
