"""Thin worker wrapper for discovery jobs."""

from __future__ import annotations


class DiscoveryWorker:
    def __init__(self, planner) -> None:
        self.planner = planner

    def handle(self, job):
        return self.planner.expand(job)
