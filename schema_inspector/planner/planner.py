"""Capability-aware planner for the hybrid ETL backbone."""

from __future__ import annotations

from dataclasses import dataclass, field

from ..jobs.types import JOB_HYDRATE_EVENT_ROOT
from .rules import edge_jobs_for_event, season_widget_jobs


@dataclass
class Planner:
    capability_rollup: dict[str, str] = field(default_factory=dict)

    def expand(self, job) -> tuple[object, ...]:
        if job.job_type == JOB_HYDRATE_EVENT_ROOT:
            return edge_jobs_for_event(job, self.capability_rollup)
        return ()

    def plan_season_widgets(
        self,
        sport_slug: str,
        *,
        unique_tournament_id: int,
        season_id: int,
    ) -> tuple[object, ...]:
        return season_widget_jobs(
            sport_slug=sport_slug,
            unique_tournament_id=unique_tournament_id,
            season_id=season_id,
        )
