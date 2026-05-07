"""Smart-404 store for ``/team-of-the-week/{period_id}`` (D13.3).

Probes against Sofascore upstream show that only ~5% of football seasons
support Team-of-the-Week (178 / 3,314 active football seasons globally).
Without a coarse-grained gate, ``PeriodOfRegistryFootballResolver`` would
fan out (ut, season) pairs that always 404 — wasting one upstream request
per period_id per planner tick.

The :class:`ResourceNegativeCache` already absorbs per-target 404s, but
its key is the full (endpoint_pattern, path_params) tuple — so a 404 on
period A only blocks period A; the next planner tick still publishes
periods B, C, … from the same season.

This store keeps a coarser, per-(ut, season) blacklist:

* Worker hook (``ResourceRefreshWorker``) calls :meth:`mark_no` whenever
  the ToTW endpoint returns a hard 404 (or soft-error envelope).
* Resolver (:class:`PeriodOfRegistryFootballResolver`) calls
  :meth:`is_blacklisted` and skips every period_id for blacklisted
  seasons.

Storage shape: a single Redis hash, ``hash:resource_refresh:totw_season_status``,
keyed by ``"{ut_id}:{season_id}"`` with payload ``"no"`` (negative) or
``"yes"`` (explicit positive — currently only set by ops tooling for the
re-enable path; the worker never writes ``"yes"`` to avoid masking
transient 200→404 flips).

The hash itself does not have a TTL; instead we attach a per-field stamp
``"<state>:<unix_ts>"`` and treat entries older than ``BLACKLIST_TTL``
seconds as expired (default: 30 days). This lets us re-evaluate stale
seasons after schedule rotations without losing history.

Configuration::

    SCHEMA_INSPECTOR_TOTW_BLACKLIST_TTL_SECONDS=2592000   # 30 days
"""

from __future__ import annotations

import logging
import os
import time
from typing import Any

logger = logging.getLogger(__name__)

HASH_KEY = "hash:resource_refresh:totw_season_status"
DEFAULT_BLACKLIST_TTL_SECONDS = 30 * 24 * 3600  # 30 days
TTL_ENV_KEY = "SCHEMA_INSPECTOR_TOTW_BLACKLIST_TTL_SECONDS"

STATE_NO = "no"
STATE_YES = "yes"


class ToTW404Store:
    """Per-(ut, season) blacklist for Team-of-the-Week support."""

    def __init__(
        self,
        backend: Any,
        *,
        hash_key: str = HASH_KEY,
        ttl_seconds: int | None = None,
        env: dict[str, str] | None = None,
        now_factory=None,
    ) -> None:
        self.backend = backend
        self.hash_key = hash_key
        self._ttl_override = ttl_seconds
        self._env = env if env is not None else dict(os.environ)
        self._now_factory = now_factory or time.time

    @property
    def ttl_seconds(self) -> int:
        if self._ttl_override is not None:
            return int(self._ttl_override)
        raw = self._env.get(TTL_ENV_KEY)
        if raw in (None, ""):
            return DEFAULT_BLACKLIST_TTL_SECONDS
        try:
            value = int(raw)
        except (TypeError, ValueError):
            return DEFAULT_BLACKLIST_TTL_SECONDS
        return value if value > 0 else DEFAULT_BLACKLIST_TTL_SECONDS

    def is_blacklisted(self, ut_id: int, season_id: int) -> bool:
        """True iff the season has been recently marked as not-supporting ToTW."""

        if self.backend is None:
            return False
        field = self._field(ut_id, season_id)
        try:
            raw = self.backend.hget(self.hash_key, field)
        except Exception as exc:  # pragma: no cover - fail-open
            logger.warning("ToTW404Store.is_blacklisted failed (fail-open): %s", exc)
            return False
        state, stamped_at = _decode_value(raw)
        if state != STATE_NO:
            return False
        if stamped_at is None:
            return True  # legacy entry without timestamp - treat as blacklisted
        if (self._now_factory() - stamped_at) > self.ttl_seconds:
            return False
        return True

    def mark_no(self, ut_id: int, season_id: int) -> None:
        self._mark(ut_id, season_id, STATE_NO)

    def mark_yes(self, ut_id: int, season_id: int) -> None:
        self._mark(ut_id, season_id, STATE_YES)

    def clear(self, ut_id: int, season_id: int) -> None:
        if self.backend is None:
            return
        field = self._field(ut_id, season_id)
        try:
            delete = getattr(self.backend, "hdel", None)
            if callable(delete):
                delete(self.hash_key, field)
        except Exception as exc:  # pragma: no cover
            logger.warning("ToTW404Store.clear failed: %s", exc)

    # ------------------------------------------------------------------

    def _mark(self, ut_id: int, season_id: int, state: str) -> None:
        if self.backend is None:
            return
        field = self._field(ut_id, season_id)
        value = f"{state}:{int(self._now_factory())}"
        try:
            try:
                self.backend.hset(self.hash_key, mapping={field: value})
            except TypeError:
                self.backend.hset(self.hash_key, {field: value})
        except Exception as exc:  # pragma: no cover - best-effort
            logger.warning("ToTW404Store._mark failed: %s", exc)

    @staticmethod
    def _field(ut_id: int, season_id: int) -> str:
        return f"{int(ut_id)}:{int(season_id)}"


def _decode_value(raw: object) -> tuple[str | None, float | None]:
    if raw in (None, "", b""):
        return None, None
    if isinstance(raw, bytes):
        text = raw.decode("utf-8", errors="ignore")
    else:
        text = str(raw)
    if ":" in text:
        state, _, ts_text = text.partition(":")
        try:
            return state, float(ts_text)
        except ValueError:
            return state or None, None
    return text or None, None


__all__ = [
    "ToTW404Store",
    "HASH_KEY",
    "DEFAULT_BLACKLIST_TTL_SECONDS",
    "TTL_ENV_KEY",
    "STATE_NO",
    "STATE_YES",
]
