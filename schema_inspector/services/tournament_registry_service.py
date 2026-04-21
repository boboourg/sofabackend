"""Helpers for converting category discovery output into registry rows/targets."""

from __future__ import annotations

from ..category_tournaments_parser import CategoryTournamentsBundle
from ..storage.tournament_registry_repository import TournamentRegistryRecord, TournamentRegistryTarget


def normalize_registry_target(
    source_slug: str,
    sport_slug: str,
    unique_tournament_id: int | str,
) -> TournamentRegistryTarget:
    return TournamentRegistryTarget(
        source_slug=str(source_slug).strip().lower(),
        sport_slug=str(sport_slug).strip().lower(),
        unique_tournament_id=int(unique_tournament_id),
    )


def records_from_category_tournaments_bundle(
    bundle: CategoryTournamentsBundle,
    *,
    source_slug: str | None = None,
    sport_slug: str,
    discovery_surface: str | None = None,
) -> tuple[TournamentRegistryRecord, ...]:
    normalized_source = str(source_slug or bundle.source_slug or "sofascore").strip().lower()
    normalized_sport = str(sport_slug).strip().lower()
    resolved_surface = str(discovery_surface or bundle.discovery_surface or "category_unique_tournaments").strip().lower()
    active_ids = set(int(item) for item in bundle.active_unique_tournament_ids)
    category_ids_by_tournament = {
        int(item.id): int(item.category_id)
        for item in bundle.competition_bundle.unique_tournaments
    }
    records: list[TournamentRegistryRecord] = []
    for priority_rank, unique_tournament_id in enumerate(bundle.unique_tournament_ids, start=1):
        category_id = category_ids_by_tournament.get(int(unique_tournament_id))
        if category_id is None:
            continue
        records.append(
            TournamentRegistryRecord(
                source_slug=normalized_source,
                sport_slug=normalized_sport,
                category_id=category_id,
                unique_tournament_id=int(unique_tournament_id),
                discovery_surface=resolved_surface,
                priority_rank=priority_rank,
                is_active=int(unique_tournament_id) in active_ids,
            )
        )
    return tuple(records)


__all__ = [
    "TournamentRegistryRecord",
    "TournamentRegistryTarget",
    "normalize_registry_target",
    "records_from_category_tournaments_bundle",
]
