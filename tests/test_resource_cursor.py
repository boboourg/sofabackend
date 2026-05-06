from __future__ import annotations

import unittest

from schema_inspector.queue.resource_cursor import (
    DEFAULT_HASH_KEY,
    ResourceCursorStore,
    build_cursor_field,
)


class _FakeRedis:
    """Minimal Redis hash backend; supports hget / hset(mapping=...)."""

    def __init__(self) -> None:
        self.store: dict[str, dict[str, str]] = {}
        self.hget_calls: list[tuple[str, str]] = []
        self.hset_calls: list[tuple[str, dict[str, str]]] = []

    def hget(self, key: str, field: str) -> str | None:
        self.hget_calls.append((key, field))
        return self.store.get(key, {}).get(field)

    def hset(self, key: str, mapping: dict[str, str]) -> int:
        self.hset_calls.append((key, dict(mapping)))
        bucket = self.store.setdefault(key, {})
        bucket.update(mapping)
        return len(mapping)


class BuildCursorFieldTests(unittest.TestCase):
    def test_field_includes_pattern_and_path_params(self) -> None:
        field = build_cursor_field(
            "/api/v1/team/{team_id}/players",
            {"team_id": 42},
        )
        # Pattern is preserved verbatim; path_params are JSON-encoded.
        self.assertTrue(field.startswith("/api/v1/team/{team_id}/players|"))
        self.assertIn('"team_id":42', field)

    def test_pages_produce_distinct_fields(self) -> None:
        page0 = build_cursor_field(
            "/api/v1/team/{team_id}/events/last/{page}",
            {"team_id": 42, "page": 0},
        )
        page1 = build_cursor_field(
            "/api/v1/team/{team_id}/events/last/{page}",
            {"team_id": 42, "page": 1},
        )
        self.assertNotEqual(page0, page1)

    def test_field_is_stable_across_dict_iteration_order(self) -> None:
        # Defensive: build_cursor_field MUST sort keys before serialising,
        # otherwise two equivalent dicts produced in different orders would
        # yield different cursor fields.
        a = build_cursor_field(
            "/api/v1/team/{team_id}/events/last/{page}",
            {"team_id": 42, "page": 0},
        )
        b = build_cursor_field(
            "/api/v1/team/{team_id}/events/last/{page}",
            {"page": 0, "team_id": 42},
        )
        self.assertEqual(a, b)


class ResourceCursorStoreTests(unittest.TestCase):
    def test_load_returns_zero_when_field_absent(self) -> None:
        backend = _FakeRedis()
        store = ResourceCursorStore(backend)
        result = store.load_last_refresh_ms(
            endpoint_pattern="/api/v1/team/{team_id}/players",
            path_params={"team_id": 42},
        )
        self.assertEqual(result, 0)
        self.assertEqual(backend.hget_calls[0][0], DEFAULT_HASH_KEY)

    def test_save_then_load_roundtrip(self) -> None:
        backend = _FakeRedis()
        store = ResourceCursorStore(backend)
        store.save_last_refresh_ms(
            endpoint_pattern="/api/v1/team/{team_id}/players",
            path_params={"team_id": 42},
            when_ms=1_700_000_000_000,
        )
        loaded = store.load_last_refresh_ms(
            endpoint_pattern="/api/v1/team/{team_id}/players",
            path_params={"team_id": 42},
        )
        self.assertEqual(loaded, 1_700_000_000_000)

    def test_load_handles_garbage_values(self) -> None:
        backend = _FakeRedis()
        backend.store[DEFAULT_HASH_KEY] = {
            build_cursor_field(
                "/api/v1/team/{team_id}/players", {"team_id": 1}
            ): "not-a-number"
        }
        store = ResourceCursorStore(backend)
        result = store.load_last_refresh_ms(
            endpoint_pattern="/api/v1/team/{team_id}/players",
            path_params={"team_id": 1},
        )
        self.assertEqual(result, 0)

    def test_save_falls_back_to_positional_hset_when_mapping_kwarg_unsupported(self) -> None:
        # Older redis-py shims accept hset(name, dict) instead of hset(name, mapping=dict).
        # ResourceCursorStore must handle TypeError from the keyword form.
        class _PositionalOnlyRedis:
            def __init__(self) -> None:
                self.calls: list[tuple] = []

            def hset(self, key, mapping=None, /):
                if mapping is not None:
                    raise TypeError("mapping kwarg not supported")
                # signature accepts only positional dict; never reached via kwarg
                return 0

        # Use a fully-positional shim that explicitly rejects mapping= kwargs.
        class _StrictBackend:
            def __init__(self) -> None:
                self.store: dict[str, dict[str, str]] = {}

            def hget(self, key, field):
                return self.store.get(key, {}).get(field)

            def hset(self, key, *args, **kwargs):
                if "mapping" in kwargs:
                    raise TypeError("mapping kwarg not supported on this backend")
                if args and isinstance(args[0], dict):
                    bucket = self.store.setdefault(key, {})
                    bucket.update(args[0])
                    return len(args[0])
                raise TypeError("unexpected hset call shape")

        backend = _StrictBackend()
        store = ResourceCursorStore(backend)
        store.save_last_refresh_ms(
            endpoint_pattern="/api/v1/team/{team_id}/players",
            path_params={"team_id": 42},
            when_ms=999,
        )
        loaded = store.load_last_refresh_ms(
            endpoint_pattern="/api/v1/team/{team_id}/players",
            path_params={"team_id": 42},
        )
        self.assertEqual(loaded, 999)


if __name__ == "__main__":
    unittest.main()
