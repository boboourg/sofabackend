from __future__ import annotations

import unittest

from schema_inspector.category_tournaments_parser import CategoryTournamentsBundle
from schema_inspector.competition_parser import CompetitionBundle, UniqueTournamentRecord


class TournamentRegistryServiceTests(unittest.TestCase):
    def test_normalize_registry_target_preserves_source_and_sport(self) -> None:
        from schema_inspector.services.tournament_registry_service import normalize_registry_target

        target = normalize_registry_target("Sofascore", "Tennis", "2407")

        self.assertEqual(target.source_slug, "sofascore")
        self.assertEqual(target.sport_slug, "tennis")
        self.assertEqual(target.unique_tournament_id, 2407)

    def test_registry_record_generation_from_category_discovery_bundle(self) -> None:
        from schema_inspector.services.tournament_registry_service import records_from_category_tournaments_bundle

        bundle = CategoryTournamentsBundle(
            competition_bundle=CompetitionBundle(
                registry_entries=(),
                payload_snapshots=(),
                image_assets=(),
                sports=(),
                countries=(),
                categories=(),
                teams=(),
                unique_tournaments=(
                    UniqueTournamentRecord(
                        id=2407,
                        slug="barcelona",
                        name="Barcelona",
                        category_id=3,
                    ),
                    UniqueTournamentRecord(
                        id=2423,
                        slug="barcelona-doubles",
                        name="Barcelona, Doubles",
                        category_id=3,
                    ),
                ),
                unique_tournament_relations=(),
                unique_tournament_most_title_teams=(),
                seasons=(),
                unique_tournament_seasons=(),
            ),
            category_ids=(3,),
            unique_tournament_ids=(2407, 2423),
            active_unique_tournament_ids=(2407,),
            group_names=("ATP",),
        )

        records = records_from_category_tournaments_bundle(
            bundle,
            source_slug="sofascore",
            sport_slug="tennis",
            discovery_surface="category_unique_tournaments",
        )

        self.assertEqual(
            [(record.unique_tournament_id, record.priority_rank, record.is_active) for record in records],
            [(2407, 1, True), (2423, 2, False)],
        )
        self.assertTrue(all(record.source_slug == "sofascore" for record in records))
        self.assertTrue(all(record.sport_slug == "tennis" for record in records))
        self.assertTrue(all(record.category_id == 3 for record in records))
        self.assertTrue(all(record.discovery_surface == "category_unique_tournaments" for record in records))


if __name__ == "__main__":
    unittest.main()
