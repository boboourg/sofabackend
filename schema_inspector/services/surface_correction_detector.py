"""Detects critical surface changes that require force rehydration."""

from __future__ import annotations

from dataclasses import dataclass

from ..event_list_parser import EventListBundle


@dataclass(frozen=True)
class SurfaceEventState:
    event_id: int
    status_code: int | None
    winner_code: int | None
    aggregated_winner_code: int | None
    home_score: int | None
    away_score: int | None
    var_home: bool | None
    var_away: bool | None
    change_timestamp: int | None
    changes: tuple[str, ...]


@dataclass(frozen=True)
class SurfaceCorrection:
    event_id: int
    reason: str


class SurfaceCorrectionDetector:
    def detect(
        self,
        *,
        bundle: EventListBundle,
        previous_states: dict[int, SurfaceEventState],
    ) -> tuple[SurfaceCorrection, ...]:
        current_states = self._states_from_bundle(bundle)
        corrections: list[SurfaceCorrection] = []
        for event_id, current in sorted(current_states.items()):
            previous = previous_states.get(event_id)
            if previous is None:
                continue
            reason = self._detect_reason(previous=previous, current=current)
            if reason is not None:
                corrections.append(SurfaceCorrection(event_id=event_id, reason=reason))
        return tuple(corrections)

    def _detect_reason(
        self,
        *,
        previous: SurfaceEventState,
        current: SurfaceEventState,
    ) -> str | None:
        if previous.home_score != current.home_score or previous.away_score != current.away_score:
            return "score_changed"
        if previous.status_code != current.status_code:
            return "status_changed"
        if previous.winner_code != current.winner_code or previous.aggregated_winner_code != current.aggregated_winner_code:
            return "winner_changed"
        if previous.change_timestamp != current.change_timestamp or previous.changes != current.changes:
            return "change_log_changed"
        if previous.var_home != current.var_home or previous.var_away != current.var_away:
            return "var_state_changed"
        return None

    def _states_from_bundle(self, bundle: EventListBundle) -> dict[int, SurfaceEventState]:
        states: dict[int, dict[str, object]] = {}
        for event in bundle.events:
            states[event.id] = {
                "event_id": int(event.id),
                "status_code": event.status_code,
                "winner_code": event.winner_code,
                "aggregated_winner_code": event.aggregated_winner_code,
                "home_score": None,
                "away_score": None,
                "var_home": None,
                "var_away": None,
                "change_timestamp": None,
                "changes": (),
            }
        for score in bundle.event_scores:
            state = states.setdefault(
                int(score.event_id),
                {
                    "event_id": int(score.event_id),
                    "status_code": None,
                    "winner_code": None,
                    "aggregated_winner_code": None,
                    "home_score": None,
                    "away_score": None,
                    "var_home": None,
                    "var_away": None,
                    "change_timestamp": None,
                    "changes": (),
                },
            )
            if str(score.side) == "home":
                state["home_score"] = score.current
            elif str(score.side) == "away":
                state["away_score"] = score.current
        for item in bundle.event_var_in_progress_items:
            state = states.setdefault(
                int(item.event_id),
                {
                    "event_id": int(item.event_id),
                    "status_code": None,
                    "winner_code": None,
                    "aggregated_winner_code": None,
                    "home_score": None,
                    "away_score": None,
                    "var_home": None,
                    "var_away": None,
                    "change_timestamp": None,
                    "changes": (),
                },
            )
            state["var_home"] = item.home_team
            state["var_away"] = item.away_team
        change_groups: dict[int, list[tuple[int, str, int | None]]] = {}
        for item in bundle.event_change_items:
            change_groups.setdefault(int(item.event_id), []).append((int(item.ordinal), str(item.change_value), item.change_timestamp))
        for event_id, values in change_groups.items():
            state = states.setdefault(
                int(event_id),
                {
                    "event_id": int(event_id),
                    "status_code": None,
                    "winner_code": None,
                    "aggregated_winner_code": None,
                    "home_score": None,
                    "away_score": None,
                    "var_home": None,
                    "var_away": None,
                    "change_timestamp": None,
                    "changes": (),
                },
            )
            ordered = sorted(values, key=lambda item: item[0])
            state["change_timestamp"] = next((timestamp for _, _, timestamp in ordered if timestamp is not None), None)
            state["changes"] = tuple(change_value for _, change_value, _ in ordered)
        return {event_id: SurfaceEventState(**state) for event_id, state in states.items()}
