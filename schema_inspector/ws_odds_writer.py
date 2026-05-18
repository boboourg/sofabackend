"""Apply a NormalizedOddsDelta from the WS feed to event_market_choice.

Mapping (resolved via the proven layout in
``/api/v1/event/{id}/odds/1/all`` snapshots and a 6-day archive):

  WS subject  ``odds.{sport}.1``      (always marketId=1 = "Full time")
  WS payload  ``id``                  = ``event_market.id`` PK
  WS payload  ``choice{N}.fractionalValue`` / ``initialFractionalValue``
              N=1..3 is a positional index into the market's choices
              array. The actual stored ``event_market_choice.name``
              depends on the market group:
                * ``1X2``      → choices order is ['1', 'X', '2']
                * ``Home/Away``→ choices order is ['1', '2']
                * anything else is unknown layout → drop silently.

Per the agreed strategy (see commit log):
  * unknown offer_id (market not yet ingested via polling) → drop
  * latest only — UPDATE, no history table
  * always invalidate event_payload_cache rows tied to the affected
    event after a successful UPDATE so the persistent payload cache
    rebuilds with fresh odds on the next read.

The writer uses ``COALESCE`` on ``initial_fractional_value`` so a delta
that only carries ``fractionalValue`` keeps the previously-stored
initial value untouched.
"""
from __future__ import annotations

import logging
from typing import Any

from .ws_delta_normalizer import NormalizedOddsDelta

logger = logging.getLogger(__name__)


# Choice ordering by market_group, taken from /event/{id}/odds/1/all
# snapshots and confirmed by the event_market_choice population in
# production (78775 rows for Home/Away name='1', 78766 for name='2',
# zero for name='X' modulo one outlier; 58416 / 58151 / 58406 for
# 1X2 name in {'1','X','2'}).
_CHOICE_LAYOUT_BY_MARKET_GROUP: dict[str, tuple[str, ...]] = {
    "1X2": ("1", "X", "2"),
    "Home/Away": ("1", "2"),
}


def resolve_choice_name(market_group: str | None, choice_index: int) -> str | None:
    """Map the WS ``choiceN`` index (1-based) for a given market_group
    to the ``event_market_choice.name`` it corresponds to. Returns
    None when the layout is unknown or the index is out of range —
    the writer drops such updates silently."""
    if market_group is None:
        return None
    layout = _CHOICE_LAYOUT_BY_MARKET_GROUP.get(market_group)
    if layout is None:
        return None
    if choice_index < 1 or choice_index > len(layout):
        return None
    return layout[choice_index - 1]


async def apply_odds_delta(connection: Any, bundle: NormalizedOddsDelta) -> None:
    """Apply one WS odds delta to event_market_choice + invalidate cache.

    No-op when the bundle's offer_id has no matching event_market row
    (the polling layer is the authoritative source for new markets).
    """
    if bundle.offer_id is None or not bundle.choices:
        return

    market_row = await connection.fetchrow(
        """
        SELECT market_group, event_id
        FROM event_market
        WHERE id = $1
        """,
        bundle.offer_id,
    )
    if market_row is None:
        return  # unknown market — drop silently

    market_group = market_row["market_group"]
    event_id = market_row["event_id"]

    applied_any = False
    for choice_index, fields in sorted(bundle.choices.items()):
        name = resolve_choice_name(market_group, choice_index)
        if name is None:
            continue
        fractional = fields.get("fractionalValue")
        initial = fields.get("initialFractionalValue")
        if fractional is None and initial is None:
            continue
        # COALESCE in the UPDATE means we only overwrite columns whose
        # WS field is present; the others stay at their last value.
        await connection.execute(
            """
            UPDATE event_market_choice
            SET fractional_value = COALESCE($3, fractional_value),
                initial_fractional_value = COALESCE($4, initial_fractional_value)
            WHERE event_market_id = $1
              AND name = $2
            """,
            bundle.offer_id,
            name,
            fractional,
            initial,
        )
        applied_any = True

    if applied_any and event_id is not None:
        # Invalidate persistent payload cache so the odds-bearing
        # endpoints (e.g. /event/{id}/odds/{provider}/all served via
        # event_payload_cache) rebuild with the fresh values on the
        # next read.
        await connection.execute(
            """
            DELETE FROM event_payload_cache
            WHERE context_entity_type = 'event'
              AND context_entity_id = $1
            """,
            event_id,
        )
