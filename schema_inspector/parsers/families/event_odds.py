"""Family parser for `/event/{id}/odds/*` payloads."""

from __future__ import annotations

import re
from typing import Any, Mapping

from ..base import PARSE_STATUS_PARSED, PARSE_STATUS_PARSED_EMPTY, ParseResult, RawSnapshot


_ODDS_URL_PATTERN = re.compile(r"/event/(?P<event_id>\d+)/odds/(?P<provider_id>\d+)/(?:all|featured)")


class EventOddsParser:
    parser_family = "event_odds"
    parser_version = "v1"

    def parse(self, snapshot: RawSnapshot) -> ParseResult:
        payload = _as_mapping(snapshot.payload) or {}
        event_id = snapshot.context_event_id or snapshot.context_entity_id
        provider_id = _extract_provider_id(snapshot)
        if event_id is None or provider_id is None:
            return ParseResult.empty(
                snapshot=snapshot,
                parser_family=self.parser_family,
                parser_version=self.parser_version,
                status=PARSE_STATUS_PARSED_EMPTY,
            )

        provider_rows: dict[int, Mapping[str, object]] = {}
        provider_configuration_rows: dict[int, Mapping[str, object]] = {}
        market_rows: dict[int, Mapping[str, object]] = {}
        choice_rows: dict[int, Mapping[str, object]] = {}

        _collect_provider_payloads(
            provider_rows=provider_rows,
            provider_configuration_rows=provider_configuration_rows,
            payload=payload,
            default_provider_id=provider_id,
        )

        if snapshot.endpoint_pattern.endswith("/featured"):
            featured = _as_mapping(payload.get("featured")) or {}
            market_payloads = [item for item in featured.values() if isinstance(item, Mapping)]
        else:
            market_payloads = [item for item in payload.get("markets", ()) if isinstance(item, Mapping)]

        for market_payload in market_payloads:
            market_id = _as_int(market_payload.get("id"))
            fid = _as_int(market_payload.get("fid"))
            market_source_id = _as_int(market_payload.get("sourceId"))
            market_key = _as_int(market_payload.get("marketId"))
            market_group = _as_str(market_payload.get("marketGroup"))
            market_name = _as_str(market_payload.get("marketName"))
            market_period = _as_str(market_payload.get("marketPeriod"))
            structure_type = _as_int(market_payload.get("structureType"))
            is_live = _as_bool(market_payload.get("isLive"))
            suspended = _as_bool(market_payload.get("suspended"))
            if (
                market_id is None
                or fid is None
                or market_key is None
                or market_group is None
                or market_name is None
                or market_period is None
                or structure_type is None
                or is_live is None
                or suspended is None
            ):
                continue
            market_rows[market_id] = {
                "id": market_id,
                "event_id": event_id,
                "provider_id": provider_id,
                "fid": fid,
                "market_id": market_key,
                "source_id": market_source_id,
                "market_group": market_group,
                "market_name": market_name,
                "market_period": market_period,
                "structure_type": structure_type,
                "choice_group": _as_str(market_payload.get("choiceGroup")),
                "is_live": is_live,
                "suspended": suspended,
            }
            for choice_payload in market_payload.get("choices", ()):
                if not isinstance(choice_payload, Mapping):
                    continue
                source_id = _as_int(choice_payload.get("sourceId"))
                name = _as_str(choice_payload.get("name"))
                change_value = _as_int(choice_payload.get("change"))
                fractional_value = _as_scalar_text(choice_payload.get("fractionalValue"))
                initial_fractional_value = _as_scalar_text(choice_payload.get("initialFractionalValue"))
                if (
                    source_id is None
                    or name is None
                    or change_value is None
                    or fractional_value is None
                    or initial_fractional_value is None
                ):
                    continue
                choice_rows[source_id] = {
                    "source_id": source_id,
                    "event_market_id": market_id,
                    "name": name,
                    "change_value": change_value,
                    "fractional_value": fractional_value,
                    "initial_fractional_value": initial_fractional_value,
                }

        metric_rows: dict[str, tuple[Mapping[str, object], ...]] = {}
        if provider_rows:
            metric_rows["provider"] = tuple(provider_rows[key] for key in sorted(provider_rows))
        if provider_configuration_rows:
            metric_rows["provider_configuration"] = tuple(
                provider_configuration_rows[key] for key in sorted(provider_configuration_rows)
            )
        if market_rows:
            metric_rows["event_market"] = tuple(market_rows[key] for key in sorted(market_rows))
        if choice_rows:
            metric_rows["event_market_choice"] = tuple(choice_rows[key] for key in sorted(choice_rows))

        return ParseResult(
            snapshot_id=snapshot.snapshot_id,
            parser_family=self.parser_family,
            parser_version=self.parser_version,
            status=PARSE_STATUS_PARSED if metric_rows else PARSE_STATUS_PARSED_EMPTY,
            metric_rows=metric_rows,
            observed_root_keys=snapshot.observed_root_keys,
        )


def _collect_provider_payloads(
    *,
    provider_rows: dict[int, Mapping[str, object]],
    provider_configuration_rows: dict[int, Mapping[str, object]],
    payload: Mapping[str, Any],
    default_provider_id: int | None,
) -> None:
    provider_payload = _as_mapping(payload.get("provider"))
    if provider_payload is not None:
        provider_id = _as_int(provider_payload.get("id"))
        if provider_id is not None:
            provider_rows[provider_id] = _provider_row(provider_payload, provider_id)

    for key in ("providerConfiguration", "providerConfig"):
        provider_configuration = _as_mapping(payload.get(key))
        if provider_configuration is not None:
            _ingest_provider_configuration(
                provider_rows=provider_rows,
                provider_configuration_rows=provider_configuration_rows,
                payload=provider_configuration,
                default_provider_id=default_provider_id,
            )

    for key in ("providerConfigurations", "providerConfigs"):
        for provider_configuration in payload.get(key, ()):
            if not isinstance(provider_configuration, Mapping):
                continue
            _ingest_provider_configuration(
                provider_rows=provider_rows,
                provider_configuration_rows=provider_configuration_rows,
                payload=provider_configuration,
                default_provider_id=default_provider_id,
            )


def _ingest_provider_configuration(
    *,
    provider_rows: dict[int, Mapping[str, object]],
    provider_configuration_rows: dict[int, Mapping[str, object]],
    payload: Mapping[str, Any],
    default_provider_id: int | None,
) -> None:
    configuration_id = _as_int(payload.get("id"))
    if configuration_id is None:
        return
    provider_id = None
    provider_payload = _as_mapping(payload.get("provider"))
    if provider_payload is not None:
        provider_id = _as_int(provider_payload.get("id"))
        if provider_id is not None:
            provider_rows[provider_id] = _provider_row(provider_payload, provider_id)
    if provider_id is None:
        provider_id = _as_int(payload.get("providerId"))
    if provider_id is None:
        provider_id = default_provider_id
    if provider_id is None:
        return

    fallback_provider_payload = _as_mapping(payload.get("fallbackProvider"))
    fallback_provider_id = None
    if fallback_provider_payload is not None:
        fallback_provider_id = _as_int(fallback_provider_payload.get("id"))
        if fallback_provider_id is not None:
            provider_rows[fallback_provider_id] = _provider_row(fallback_provider_payload, fallback_provider_id)
    if fallback_provider_id is None:
        fallback_provider_id = _as_int(payload.get("fallbackProviderId"))

    provider_configuration_rows[configuration_id] = {
        "id": configuration_id,
        "provider_id": provider_id,
        "campaign_id": _as_int(payload.get("campaignId")),
        "fallback_provider_id": fallback_provider_id,
        "type": _as_str(payload.get("type")),
        "weight": _as_int(payload.get("weight")),
        "branded": _as_bool(payload.get("branded")),
        "featured_odds_type": _as_str(payload.get("featuredOddsType")),
        "bet_slip_link": _as_str(payload.get("betSlipLink")),
        "default_bet_slip_link": _as_str(payload.get("defaultBetSlipLink")),
        "impression_cost_encrypted": _as_str(payload.get("impressionCostEncrypted")),
    }


def _provider_row(payload: Mapping[str, Any], provider_id: int) -> Mapping[str, object]:
    odds_from_provider = _as_mapping(payload.get("oddsFromProvider"))
    live_odds_from_provider = _as_mapping(payload.get("liveOddsFromProvider"))
    return {
        "id": provider_id,
        "slug": _as_str(payload.get("slug")),
        "name": _as_str(payload.get("name")),
        "country": _as_str(payload.get("country")),
        "default_bet_slip_link": _as_str(payload.get("defaultBetSlipLink")),
        "colors": _as_mapping(payload.get("colors")),
        "odds_from_provider_id": _as_int(odds_from_provider.get("id")) if odds_from_provider is not None else None,
        "live_odds_from_provider_id": (
            _as_int(live_odds_from_provider.get("id")) if live_odds_from_provider is not None else None
        ),
    }


def _extract_provider_id(snapshot: RawSnapshot) -> int | None:
    for url in (snapshot.resolved_url, snapshot.source_url):
        if not isinstance(url, str):
            continue
        match = _ODDS_URL_PATTERN.search(url)
        if match is None:
            continue
        return _as_int(match.group("provider_id"))
    return None


def _as_mapping(value: object) -> Mapping[str, Any] | None:
    return value if isinstance(value, Mapping) else None


def _as_str(value: object) -> str | None:
    return value if isinstance(value, str) else None


def _as_scalar_text(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        return value
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return str(value)
    return None


def _as_bool(value: object) -> bool | None:
    return value if isinstance(value, bool) else None


def _as_int(value: object) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float) and value.is_integer():
        return int(value)
    if isinstance(value, str):
        stripped = value.strip()
        if stripped.isdigit() or (stripped.startswith("-") and stripped[1:].isdigit()):
            return int(stripped)
    return None
