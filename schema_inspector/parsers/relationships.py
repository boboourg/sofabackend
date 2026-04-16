"""Relationship helpers for normalized parser output."""

from __future__ import annotations

from collections import defaultdict
from typing import Mapping


def build_relation_map(items: Mapping[str, list[Mapping[str, object]]]) -> dict[str, tuple[Mapping[str, object], ...]]:
    return {key: tuple(value) for key, value in items.items() if value}


def mutable_relation_map() -> defaultdict[str, list[Mapping[str, object]]]:
    return defaultdict(list)
