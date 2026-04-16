"""Thin worker wrapper for hydration jobs."""

from __future__ import annotations


class HydrateWorker:
    def __init__(self, planner) -> None:
        self.planner = planner

    def handle(self, job):
        return self.planner.expand(job)
