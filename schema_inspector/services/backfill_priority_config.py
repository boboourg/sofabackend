"""Operator-facing backfill priorities: per-sport weights, per-UT
boosts, per-sport concurrency caps.

Loaded from a YAML file (default
``/opt/sofascore/config/backfill_priorities.yaml``) at planner startup
and on SIGHUP. Validation is strict — any malformed value raises
:class:`ConfigValidationError` so callers can keep the previous good
config instead of silently dropping a sport.
"""
from __future__ import annotations

import logging
import random
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable, Mapping, Sequence, TypeVar

logger = logging.getLogger(__name__)


class ConfigValidationError(ValueError):
    """Raised when the YAML config has a value that cannot be safely
    interpreted (negative weight, missing ut_id, non-numeric value).

    SIGHUP handlers catch this and keep the previous-good config so a
    typo never silently pauses a sport.
    """


@dataclass(frozen=True)
class BackfillPriorityConfig:
    """Parsed + validated snapshot of ``backfill_priorities.yaml``.

    Immutable on purpose: the planner publishes jobs using a single
    pinned snapshot per tick, so concurrent reloads cannot interleave
    with selection logic.
    """

    sport_weights: Mapping[str, float] = field(default_factory=dict)
    ut_boost: Mapping[int, float] = field(default_factory=dict)
    sport_concurrency_caps: Mapping[str, int] = field(default_factory=dict)

    @classmethod
    def load(cls, path: Path) -> "BackfillPriorityConfig":
        """Read + validate the YAML at ``path``. Missing file yields an
        empty config (uniform weights = default planner behaviour)."""
        if not path.exists():
            logger.info("backfill priority config missing at %s — using defaults", path)
            return cls()

        try:
            import yaml  # type: ignore
        except ImportError as exc:
            raise ConfigValidationError(
                "PyYAML is required to load backfill priorities"
            ) from exc

        try:
            raw_text = path.read_text(encoding="utf-8")
            raw = yaml.safe_load(raw_text)
        except yaml.YAMLError as exc:
            raise ConfigValidationError(f"YAML parse error in {path}: {exc}") from exc

        if raw is None:
            return cls()
        if not isinstance(raw, dict):
            raise ConfigValidationError(
                f"Top-level YAML must be a mapping, got {type(raw).__name__}"
            )

        return cls(
            sport_weights=_parse_sport_weights(raw.get("sport_weights")),
            ut_boost=_parse_ut_boost(raw.get("ut_boost")),
            sport_concurrency_caps=_parse_caps(raw.get("sport_concurrency_caps")),
        )

    # ──── lookup helpers ─────────────────────────────────────────────

    def weight_for_ut(self, *, ut_id: int, sport_slug: str) -> float:
        """Final weight for one (ut_id, sport_slug) candidate.

        ``sport_weight * ut_multiplier`` (or just ``sport_weight`` when
        no boost). Returns ``0.0`` when the sport itself has zero or
        missing weight — ``weighted_select`` then skips this candidate."""
        sport_weight = float(self.sport_weights.get(sport_slug, 0.0))
        if sport_weight <= 0.0:
            return 0.0
        multiplier = self.ut_boost.get(int(ut_id))
        if multiplier is None:
            return sport_weight
        return sport_weight * float(multiplier)

    def cap_for_sport(self, sport_slug: str) -> int | None:
        """Concurrency cap for ``sport_slug`` (None = unbounded)."""
        cap = self.sport_concurrency_caps.get(sport_slug)
        return int(cap) if cap is not None else None


# ──── YAML parsing helpers ───────────────────────────────────────────


def _parse_sport_weights(raw: object) -> Mapping[str, float]:
    if raw is None:
        return {}
    if not isinstance(raw, dict):
        raise ConfigValidationError(
            f"sport_weights must be a mapping, got {type(raw).__name__}"
        )
    result: dict[str, float] = {}
    for sport, value in raw.items():
        if not isinstance(sport, str) or not sport:
            raise ConfigValidationError(f"sport_weights key must be a non-empty string: {sport!r}")
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise ConfigValidationError(
                f"sport_weights[{sport!r}] must be a number, got {value!r}"
            )
        if value < 0:
            raise ConfigValidationError(
                f"sport_weights[{sport!r}] must be >= 0, got {value}"
            )
        result[sport] = float(value)
    return result


def _parse_ut_boost(raw: object) -> Mapping[int, float]:
    if raw is None:
        return {}
    if not isinstance(raw, list):
        raise ConfigValidationError(
            f"ut_boost must be a list of {{ut_id, multiplier}} mappings, "
            f"got {type(raw).__name__}"
        )
    result: dict[int, float] = {}
    for i, entry in enumerate(raw):
        if not isinstance(entry, dict):
            raise ConfigValidationError(f"ut_boost[{i}] must be a mapping")
        if "ut_id" not in entry:
            raise ConfigValidationError(f"ut_boost[{i}] missing required key 'ut_id'")
        if "multiplier" not in entry:
            raise ConfigValidationError(
                f"ut_boost[{i}] missing required key 'multiplier'"
            )
        ut_id = entry["ut_id"]
        multiplier = entry["multiplier"]
        if not isinstance(ut_id, int) or isinstance(ut_id, bool):
            raise ConfigValidationError(
                f"ut_boost[{i}].ut_id must be int, got {ut_id!r}"
            )
        if isinstance(multiplier, bool) or not isinstance(multiplier, (int, float)):
            raise ConfigValidationError(
                f"ut_boost[{i}].multiplier must be a number, got {multiplier!r}"
            )
        if multiplier < 0:
            raise ConfigValidationError(
                f"ut_boost[{i}].multiplier must be >= 0, got {multiplier}"
            )
        result[int(ut_id)] = float(multiplier)
    return result


def _parse_caps(raw: object) -> Mapping[str, int]:
    if raw is None:
        return {}
    if not isinstance(raw, dict):
        raise ConfigValidationError(
            f"sport_concurrency_caps must be a mapping, got {type(raw).__name__}"
        )
    result: dict[str, int] = {}
    for sport, value in raw.items():
        if not isinstance(sport, str) or not sport:
            raise ConfigValidationError(
                f"sport_concurrency_caps key must be a non-empty string: {sport!r}"
            )
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise ConfigValidationError(
                f"sport_concurrency_caps[{sport!r}] must be a non-negative int, got {value!r}"
            )
        result[sport] = int(value)
    return result


# ──── Weighted selection ─────────────────────────────────────────────


T = TypeVar("T")


def weighted_select(
    candidates: Sequence[tuple[int, str]],
    config: BackfillPriorityConfig,
    *,
    seed: int | None = None,
) -> tuple[int, str] | None:
    """Pick one ``(ut_id, sport_slug)`` candidate proportionally to its
    weight per :meth:`BackfillPriorityConfig.weight_for_ut`.

    ``seed`` is optional and exists so tests are deterministic. Returns
    ``None`` when every candidate has zero weight (all sports paused or
    list empty).
    """
    weights: list[float] = []
    eligible: list[tuple[int, str]] = []
    for ut_id, sport_slug in candidates:
        w = config.weight_for_ut(ut_id=ut_id, sport_slug=sport_slug)
        if w > 0.0:
            weights.append(w)
            eligible.append((ut_id, sport_slug))
    if not eligible:
        return None
    rng = random.Random(seed) if seed is not None else random
    [picked] = rng.choices(eligible, weights=weights, k=1)
    return picked
