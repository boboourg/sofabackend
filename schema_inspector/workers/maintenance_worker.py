"""Thin worker wrapper for maintenance and reconciliation jobs."""

from __future__ import annotations


class MaintenanceWorker:
    def __init__(self, handler) -> None:
        self.handler = handler

    def handle(self, item):
        return self.handler(item)
