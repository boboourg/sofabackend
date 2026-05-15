"""Tests for the admin UI (Admin-0 + Admin-1, 2026-05-16)."""

from __future__ import annotations

import base64
import os
import unittest
from contextlib import contextmanager
from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Any

from fastapi import FastAPI
from fastapi.testclient import TestClient

from schema_inspector.admin.auth import admin_password_from_env
from schema_inspector.admin.router import build_admin_router


_ADMIN_PASSWORD = "test-super-secret-pw"


# ---------------------------------------------------------------------------
# Fakes
# ---------------------------------------------------------------------------


class _FakeConnectionLease:
    def __init__(self, store: "_FakeStore") -> None:
        self._store = store

    async def fetch(self, sql: str, *params):
        return self._store.fetch(sql, params)

    async def execute(self, sql: str, *params):
        return self._store.execute(sql, params)

    async def close(self) -> None:
        return None


class _FakeStore:
    def __init__(self, rows: list[dict[str, Any]] | None = None) -> None:
        self.rows: list[dict[str, Any]] = list(rows or [])
        self.executed: list[tuple[str, tuple[Any, ...]]] = []

    def fetch(self, sql: str, params: tuple[Any, ...]) -> list[dict[str, Any]]:
        # Crude SQL routing: enough to satisfy router's two read shapes
        # (no-filter LIST + id/name filter LIST).
        rows = list(self.rows)
        if " WHERE o.unique_tournament_id = $1" in sql:
            (ut_id,) = params
            rows = [r for r in rows if r["unique_tournament_id"] == ut_id]
        elif " WHERE ut.name ILIKE $1 OR cat.name ILIKE $1" in sql:
            (pattern,) = params
            needle = pattern.strip("%").lower()
            rows = [
                r
                for r in rows
                if needle in (r.get("ut_name") or "").lower()
                or needle in (r.get("category_name") or "").lower()
            ]
        return rows

    def execute(self, sql: str, params: tuple[Any, ...]):
        self.executed.append((sql.strip(), tuple(params)))
        if sql.strip().startswith("INSERT INTO live_tier_override"):
            ut, tier, reason, created_by = params
            # Reject duplicate INSERT to match the PK constraint behaviour.
            if any(r["unique_tournament_id"] == ut for r in self.rows):
                raise RuntimeError(f"duplicate key value: ut={ut}")
            self.rows.append(
                {
                    "unique_tournament_id": ut,
                    "override_tier": tier,
                    "reason": reason,
                    "created_by": created_by,
                    "created_at": datetime.now(timezone.utc),
                    "ut_name": f"UT-{ut}",
                    "category_name": "Test Cat",
                    "sport_slug": "football",
                }
            )
            return "INSERT 0 1"
        if sql.strip().startswith("UPDATE live_tier_override"):
            ut, tier, reason, created_by = params
            for row in self.rows:
                if row["unique_tournament_id"] == ut:
                    row["override_tier"] = tier
                    row["reason"] = reason
                    if created_by is not None:
                        row["created_by"] = created_by
                    return "UPDATE 1"
            return "UPDATE 0"
        if sql.strip().startswith("DELETE FROM live_tier_override"):
            (ut,) = params
            before = len(self.rows)
            self.rows = [r for r in self.rows if r["unique_tournament_id"] != ut]
            return f"DELETE {before - len(self.rows)}"
        raise RuntimeError(f"unhandled SQL in fake store: {sql!r}")


class _FakeLocalApiApplication:
    def __init__(self, store: _FakeStore) -> None:
        self._store = store

    async def _connect(self) -> _FakeConnectionLease:
        return _FakeConnectionLease(self._store)

    async def run_in_runtime(self, coro):
        # In tests we are already inside an event loop — no need to defer.
        return await coro


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


@contextmanager
def _admin_env(password: str | None):
    """Set or clear SOFASCORE_ADMIN_PASSWORD just for the test body."""
    previous = os.environ.get("SOFASCORE_ADMIN_PASSWORD")
    try:
        if password is None:
            os.environ.pop("SOFASCORE_ADMIN_PASSWORD", None)
        else:
            os.environ["SOFASCORE_ADMIN_PASSWORD"] = password
        yield
    finally:
        if previous is None:
            os.environ.pop("SOFASCORE_ADMIN_PASSWORD", None)
        else:
            os.environ["SOFASCORE_ADMIN_PASSWORD"] = previous


def _basic_auth_header(username: str, password: str) -> dict[str, str]:
    token = base64.b64encode(f"{username}:{password}".encode("ascii")).decode("ascii")
    return {"Authorization": f"Basic {token}"}


def _make_app(store: _FakeStore) -> FastAPI:
    app = FastAPI()
    router = build_admin_router(_FakeLocalApiApplication(store))
    assert router is not None, "router must mount when password is set"
    app.include_router(router)
    return app


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class AdminFactoryDisabledTests(unittest.TestCase):
    def test_no_password_returns_none(self) -> None:
        with _admin_env(None):
            self.assertIsNone(admin_password_from_env())
            self.assertIsNone(build_admin_router(_FakeLocalApiApplication(_FakeStore())))

    def test_empty_password_treated_as_disabled(self) -> None:
        with _admin_env("   "):
            self.assertIsNone(admin_password_from_env())
            self.assertIsNone(build_admin_router(_FakeLocalApiApplication(_FakeStore())))


class AdminAuthTests(unittest.TestCase):
    def test_missing_credentials_returns_401(self) -> None:
        with _admin_env(_ADMIN_PASSWORD):
            app = _make_app(_FakeStore())
            with TestClient(app) as client:
                response = client.get("/admin/tier-overrides")
                self.assertEqual(response.status_code, 401)
                self.assertIn("WWW-Authenticate", response.headers)
                self.assertIn("Basic", response.headers["WWW-Authenticate"])

    def test_wrong_password_returns_401(self) -> None:
        with _admin_env(_ADMIN_PASSWORD):
            app = _make_app(_FakeStore())
            with TestClient(app) as client:
                response = client.get(
                    "/admin/tier-overrides",
                    headers=_basic_auth_header("admin", "wrong"),
                )
                self.assertEqual(response.status_code, 401)

    def test_wrong_username_returns_401(self) -> None:
        with _admin_env(_ADMIN_PASSWORD):
            app = _make_app(_FakeStore())
            with TestClient(app) as client:
                response = client.get(
                    "/admin/tier-overrides",
                    headers=_basic_auth_header("operator", _ADMIN_PASSWORD),
                )
                self.assertEqual(response.status_code, 401)

    def test_valid_credentials_render_page(self) -> None:
        with _admin_env(_ADMIN_PASSWORD):
            app = _make_app(_FakeStore())
            with TestClient(app) as client:
                response = client.get(
                    "/admin/tier-overrides",
                    headers=_basic_auth_header("admin", _ADMIN_PASSWORD),
                )
                self.assertEqual(response.status_code, 200)
                self.assertIn("Sofascore Admin", response.text)
                self.assertIn("Live tier overrides", response.text)


class TierOverridesCrudTests(unittest.TestCase):
    def _store_with_two_rows(self) -> _FakeStore:
        return _FakeStore(
            rows=[
                {
                    "unique_tournament_id": 17,
                    "override_tier": "tier_1",
                    "reason": "Premier League",
                    "created_at": datetime(2026, 5, 16, 10, 0, tzinfo=timezone.utc),
                    "created_by": "bobur",
                    "ut_name": "Premier League",
                    "category_name": "England",
                    "sport_slug": "football",
                },
                {
                    "unique_tournament_id": 8,
                    "override_tier": "tier_2",
                    "reason": "La Liga",
                    "created_at": datetime(2026, 5, 16, 9, 0, tzinfo=timezone.utc),
                    "created_by": "bobur",
                    "ut_name": "La Liga",
                    "category_name": "Spain",
                    "sport_slug": "football",
                },
            ]
        )

    def test_root_redirects_to_tier_overrides(self) -> None:
        with _admin_env(_ADMIN_PASSWORD):
            app = _make_app(self._store_with_two_rows())
            with TestClient(app) as client:
                response = client.get(
                    "/admin",
                    headers=_basic_auth_header("admin", _ADMIN_PASSWORD),
                    follow_redirects=False,
                )
                self.assertEqual(response.status_code, 303)
                self.assertEqual(response.headers["location"], "/admin/tier-overrides")

    def test_list_renders_all_rows(self) -> None:
        with _admin_env(_ADMIN_PASSWORD):
            app = _make_app(self._store_with_two_rows())
            with TestClient(app) as client:
                response = client.get(
                    "/admin/tier-overrides",
                    headers=_basic_auth_header("admin", _ADMIN_PASSWORD),
                )
                self.assertEqual(response.status_code, 200)
                self.assertIn("Premier League", response.text)
                self.assertIn("La Liga", response.text)
                self.assertIn("tier_1", response.text)
                self.assertIn("tier_2", response.text)

    def test_list_with_numeric_query_filters_by_ut_id(self) -> None:
        with _admin_env(_ADMIN_PASSWORD):
            app = _make_app(self._store_with_two_rows())
            with TestClient(app) as client:
                response = client.get(
                    "/admin/tier-overrides?q=17",
                    headers=_basic_auth_header("admin", _ADMIN_PASSWORD),
                )
                self.assertEqual(response.status_code, 200)
                self.assertIn("Premier League", response.text)
                self.assertNotIn("La Liga", response.text)

    def test_list_with_text_query_filters_by_name(self) -> None:
        with _admin_env(_ADMIN_PASSWORD):
            app = _make_app(self._store_with_two_rows())
            with TestClient(app) as client:
                response = client.get(
                    "/admin/tier-overrides?q=spain",
                    headers=_basic_auth_header("admin", _ADMIN_PASSWORD),
                )
                self.assertEqual(response.status_code, 200)
                self.assertIn("La Liga", response.text)
                self.assertNotIn("Premier League", response.text)

    def test_new_form_renders_blank(self) -> None:
        with _admin_env(_ADMIN_PASSWORD):
            app = _make_app(_FakeStore())
            with TestClient(app) as client:
                response = client.get(
                    "/admin/tier-overrides/new",
                    headers=_basic_auth_header("admin", _ADMIN_PASSWORD),
                )
                self.assertEqual(response.status_code, 200)
                self.assertIn("Новый tier override", response.text)
                # PK input editable in create mode
                self.assertNotIn('name="unique_tournament_id" value="" readonly', response.text)

    def test_create_inserts_row_and_redirects(self) -> None:
        with _admin_env(_ADMIN_PASSWORD):
            store = _FakeStore()
            app = _make_app(store)
            with TestClient(app) as client:
                response = client.post(
                    "/admin/tier-overrides/new",
                    headers=_basic_auth_header("admin", _ADMIN_PASSWORD),
                    data={
                        "unique_tournament_id": "215",
                        "override_tier": "tier_2",
                        "reason": "Bundesliga test",
                        "created_by": "bobur",
                    },
                    follow_redirects=False,
                )
                self.assertEqual(response.status_code, 303)
                self.assertIn("/admin/tier-overrides", response.headers["location"])
                self.assertIn("flash=Override+created", response.headers["location"])
                self.assertEqual(len(store.rows), 1)
                self.assertEqual(store.rows[0]["unique_tournament_id"], 215)
                self.assertEqual(store.rows[0]["override_tier"], "tier_2")
                self.assertEqual(store.rows[0]["reason"], "Bundesliga test")

    def test_create_invalid_tier_rerenders_form_with_flash(self) -> None:
        with _admin_env(_ADMIN_PASSWORD):
            store = _FakeStore()
            app = _make_app(store)
            with TestClient(app) as client:
                response = client.post(
                    "/admin/tier-overrides/new",
                    headers=_basic_auth_header("admin", _ADMIN_PASSWORD),
                    data={
                        "unique_tournament_id": "215",
                        "override_tier": "tier_99",
                        "reason": "test",
                    },
                    follow_redirects=False,
                )
                self.assertEqual(response.status_code, 200)
                self.assertIn("Не удалось", response.text)
                self.assertEqual(len(store.rows), 0)

    def test_create_duplicate_pk_rerenders_with_flash(self) -> None:
        with _admin_env(_ADMIN_PASSWORD):
            store = self._store_with_two_rows()
            app = _make_app(store)
            with TestClient(app) as client:
                response = client.post(
                    "/admin/tier-overrides/new",
                    headers=_basic_auth_header("admin", _ADMIN_PASSWORD),
                    data={
                        "unique_tournament_id": "17",  # already exists
                        "override_tier": "tier_3",
                    },
                    follow_redirects=False,
                )
                self.assertEqual(response.status_code, 200)
                self.assertIn("Не удалось", response.text)
                # Original row not mutated by the failed INSERT.
                premier = next(r for r in store.rows if r["unique_tournament_id"] == 17)
                self.assertEqual(premier["override_tier"], "tier_1")

    def test_edit_form_prefills_existing_row(self) -> None:
        with _admin_env(_ADMIN_PASSWORD):
            store = self._store_with_two_rows()
            app = _make_app(store)
            with TestClient(app) as client:
                response = client.get(
                    "/admin/tier-overrides/17/edit",
                    headers=_basic_auth_header("admin", _ADMIN_PASSWORD),
                )
                self.assertEqual(response.status_code, 200)
                self.assertIn("Premier League", response.text)
                self.assertIn('value="17"', response.text)
                self.assertIn("readonly", response.text)

    def test_edit_missing_id_redirects_back_with_flash(self) -> None:
        with _admin_env(_ADMIN_PASSWORD):
            app = _make_app(_FakeStore())
            with TestClient(app) as client:
                response = client.get(
                    "/admin/tier-overrides/99999/edit",
                    headers=_basic_auth_header("admin", _ADMIN_PASSWORD),
                    follow_redirects=False,
                )
                self.assertEqual(response.status_code, 303)
                self.assertIn("not+found", response.headers["location"])

    def test_edit_submit_updates_row(self) -> None:
        with _admin_env(_ADMIN_PASSWORD):
            store = self._store_with_two_rows()
            app = _make_app(store)
            with TestClient(app) as client:
                response = client.post(
                    "/admin/tier-overrides/17/edit",
                    headers=_basic_auth_header("admin", _ADMIN_PASSWORD),
                    data={
                        "override_tier": "tier_3",
                        "reason": "Demoted via admin UI",
                        "created_by": "bobur",
                    },
                    follow_redirects=False,
                )
                self.assertEqual(response.status_code, 303)
                premier = next(r for r in store.rows if r["unique_tournament_id"] == 17)
                self.assertEqual(premier["override_tier"], "tier_3")
                self.assertEqual(premier["reason"], "Demoted via admin UI")

    def test_delete_removes_row(self) -> None:
        with _admin_env(_ADMIN_PASSWORD):
            store = self._store_with_two_rows()
            app = _make_app(store)
            with TestClient(app) as client:
                response = client.post(
                    "/admin/tier-overrides/17/delete",
                    headers=_basic_auth_header("admin", _ADMIN_PASSWORD),
                    follow_redirects=False,
                )
                self.assertEqual(response.status_code, 303)
                self.assertIn("deleted", response.headers["location"])
                self.assertEqual(len(store.rows), 1)
                self.assertEqual(store.rows[0]["unique_tournament_id"], 8)

    def test_delete_non_existing_redirects_without_error(self) -> None:
        # Fake store treats DELETE on a missing PK as "DELETE 0" — same
        # as Postgres. The handler should still redirect with a success
        # flash because semantically "the row is not there anymore" is
        # the desired post-condition.
        with _admin_env(_ADMIN_PASSWORD):
            store = _FakeStore()
            app = _make_app(store)
            with TestClient(app) as client:
                response = client.post(
                    "/admin/tier-overrides/99999/delete",
                    headers=_basic_auth_header("admin", _ADMIN_PASSWORD),
                    follow_redirects=False,
                )
                self.assertEqual(response.status_code, 303)
                self.assertIn("deleted", response.headers["location"])


if __name__ == "__main__":
    unittest.main()
