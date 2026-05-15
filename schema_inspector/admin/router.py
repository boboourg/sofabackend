"""FastAPI router for the admin UI (Admin-0 + Admin-1).

The router is built by ``build_admin_router(local_api_application)`` and
mounted onto the main FastAPI app from ``local_api_server.create_asgi_app``
*before* the catch-all ``/{path:path}`` GET handler so the admin routes
take precedence. When ``SOFASCORE_ADMIN_PASSWORD`` is unset the factory
returns ``None`` and the admin is silently absent.

DB access goes through ``local_api_application.run_in_runtime(...)`` so
the admin reuses the same asyncpg pool as the public API instead of
spinning up a parallel pool on a different event loop.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from fastapi import APIRouter, Depends, Form, Query, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from .auth import admin_password_from_env, require_admin

logger = logging.getLogger(__name__)

_TEMPLATES_DIR = Path(__file__).resolve().parent / "templates"
_ALLOWED_TIERS = ("tier_1", "tier_2", "tier_3")


def build_admin_router(local_api_application: Any) -> APIRouter | None:
    """Construct the admin router, or return ``None`` to disable mounting.

    The admin is disabled (returns ``None``) when:
      * ``SOFASCORE_ADMIN_PASSWORD`` is not set in the environment, or
      * Jinja2 is unavailable (the templating import would have failed
        above; we get here only if it's importable).

    A returned router has ``/admin`` prefix and protects every handler
    with HTTP Basic via the ``require_admin`` dependency.
    """
    if admin_password_from_env() is None:
        logger.info(
            "admin UI disabled: SOFASCORE_ADMIN_PASSWORD not configured; "
            "set it in .env to enable /admin routes."
        )
        return None

    templates = Jinja2Templates(directory=str(_TEMPLATES_DIR))
    router = APIRouter(prefix="/admin", tags=["admin"])

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _render(
        request: Request,
        template_name: str,
        context: dict[str, Any] | None = None,
    ) -> HTMLResponse:
        ctx = dict(context or {})
        ctx.setdefault("now_iso", datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"))
        ctx.setdefault("flash", None)
        ctx.setdefault("flash_kind", "success")
        ctx.setdefault("active", None)
        # Modern Starlette signature: TemplateResponse(request, name, context).
        # Passing request positionally avoids the deprecation warning AND
        # the "unhashable dict in cache key" crash seen when the old
        # (name, context) form is used with the newer Jinja2Templates impl.
        return templates.TemplateResponse(request, template_name, ctx)

    async def _fetch_overrides(query: str | None) -> list[dict[str, Any]]:
        """Return tier_override rows joined with UT / category / sport.

        ``query``:
          * digits-only → match on ``unique_tournament_id``
          * non-digit  → ILIKE search across ut.name / category.name
          * None / "" → list every override (cap at 500 for safety)
        """
        normalized = (query or "").strip()
        sql_base = """
            SELECT
                o.unique_tournament_id,
                o.override_tier,
                o.reason,
                o.created_at,
                o.created_by,
                ut.name AS ut_name,
                cat.name AS category_name,
                s.slug AS sport_slug
            FROM live_tier_override AS o
            LEFT JOIN unique_tournament AS ut
                ON ut.id = o.unique_tournament_id
            LEFT JOIN category AS cat
                ON cat.id = ut.category_id
            LEFT JOIN sport AS s
                ON s.id = cat.sport_id
        """
        params: list[Any] = []
        where = ""
        if normalized:
            if normalized.isdigit():
                where = " WHERE o.unique_tournament_id = $1"
                params = [int(normalized)]
            else:
                where = " WHERE ut.name ILIKE $1 OR cat.name ILIKE $1"
                params = [f"%{normalized}%"]
        sql = sql_base + where + " ORDER BY o.created_at DESC NULLS LAST, o.unique_tournament_id LIMIT 500"

        async def _fetch():
            connection_lease = await local_api_application._connect()
            try:
                return await connection_lease.fetch(sql, *params)
            finally:
                await connection_lease.close()

        rows = await local_api_application.run_in_runtime(_fetch())
        return [_serialize_row(row) for row in rows]

    async def _fetch_override(unique_tournament_id: int) -> dict[str, Any] | None:
        rows = await _fetch_overrides(query=str(int(unique_tournament_id)))
        return rows[0] if rows else None

    async def _upsert_override(
        *,
        unique_tournament_id: int,
        override_tier: str,
        reason: str | None,
        created_by: str | None,
        is_create: bool,
    ) -> None:
        """INSERT or UPDATE in ``live_tier_override``.

        Separate code paths so the audit log is unambiguous: a row that
        already exists must be UPDATEd (not silently overwritten via
        ``ON CONFLICT``) so the caller can detect "tried to create but
        already exists" → flashing a useful error to the operator.
        """
        if override_tier not in _ALLOWED_TIERS:
            raise ValueError(f"override_tier must be one of {_ALLOWED_TIERS}")
        clean_reason = (reason or "").strip() or None
        clean_created_by = (created_by or "").strip() or None

        if is_create:
            sql = """
                INSERT INTO live_tier_override (
                    unique_tournament_id, override_tier, reason, created_by
                ) VALUES ($1, $2, $3, $4)
            """
            params: tuple = (
                int(unique_tournament_id),
                override_tier,
                clean_reason,
                clean_created_by,
            )
        else:
            sql = """
                UPDATE live_tier_override
                   SET override_tier = $2,
                       reason = $3,
                       created_by = COALESCE($4, created_by)
                 WHERE unique_tournament_id = $1
            """
            params = (
                int(unique_tournament_id),
                override_tier,
                clean_reason,
                clean_created_by,
            )

        async def _run():
            connection_lease = await local_api_application._connect()
            try:
                return await connection_lease.execute(sql, *params)
            finally:
                await connection_lease.close()

        await local_api_application.run_in_runtime(_run())

    async def _delete_override(unique_tournament_id: int) -> None:
        async def _run():
            connection_lease = await local_api_application._connect()
            try:
                return await connection_lease.execute(
                    "DELETE FROM live_tier_override WHERE unique_tournament_id = $1",
                    int(unique_tournament_id),
                )
            finally:
                await connection_lease.close()

        await local_api_application.run_in_runtime(_run())

    # ------------------------------------------------------------------
    # Routes
    # ------------------------------------------------------------------

    @router.get("", include_in_schema=False)
    async def root_redirect(_user: str = Depends(require_admin)):
        del _user
        return RedirectResponse(url="/admin/tier-overrides", status_code=303)

    @router.get("/tier-overrides", response_class=HTMLResponse, include_in_schema=False)
    async def list_overrides(
        request: Request,
        q: str | None = Query(default=None),
        flash: str | None = Query(default=None),
        flash_kind: str = Query(default="success"),
        _user: str = Depends(require_admin),
    ):
        del _user
        try:
            rows = await _fetch_overrides(q)
            err: str | None = None
        except Exception as exc:  # noqa: BLE001 — surface DB issue in UI
            logger.exception("admin list_overrides failed")
            rows = []
            err = f"DB error: {exc!s}"
        return _render(
            request,
            "tier_overrides_list.html",
            {
                "rows": rows,
                "query": q,
                "active": "tier_overrides",
                "flash": err or flash,
                "flash_kind": "danger" if err else flash_kind,
            },
        )

    @router.get("/tier-overrides/new", response_class=HTMLResponse, include_in_schema=False)
    async def new_override_form(request: Request, _user: str = Depends(require_admin)):
        del _user
        return _render(
            request,
            "tier_overrides_form.html",
            {
                "mode": "create",
                "row": {},
                "active": "tier_overrides",
            },
        )

    @router.post("/tier-overrides/new", response_class=HTMLResponse, include_in_schema=False)
    async def new_override_submit(
        request: Request,
        unique_tournament_id: int = Form(...),
        override_tier: str = Form(...),
        reason: str | None = Form(default=None),
        created_by: str | None = Form(default=None),
        _user: str = Depends(require_admin),
    ):
        del _user
        try:
            await _upsert_override(
                unique_tournament_id=unique_tournament_id,
                override_tier=override_tier,
                reason=reason,
                created_by=created_by,
                is_create=True,
            )
        except Exception as exc:  # noqa: BLE001 — re-render with flash on error
            logger.warning(
                "admin new_override_submit failed for ut=%s: %s",
                unique_tournament_id,
                exc,
            )
            return _render(
                request,
                "tier_overrides_form.html",
                {
                    "mode": "create",
                    "row": {
                        "unique_tournament_id": unique_tournament_id,
                        "override_tier": override_tier,
                        "reason": reason,
                        "created_by": created_by,
                    },
                    "active": "tier_overrides",
                    "flash": f"Не удалось создать override: {exc!s}",
                    "flash_kind": "danger",
                },
            )
        return RedirectResponse(
            url=(
                f"/admin/tier-overrides?flash=Override+created+for+UT+{unique_tournament_id}"
                f"&flash_kind=success"
            ),
            status_code=303,
        )

    @router.get(
        "/tier-overrides/{unique_tournament_id}/edit",
        response_class=HTMLResponse,
        include_in_schema=False,
    )
    async def edit_override_form(
        request: Request,
        unique_tournament_id: int,
        _user: str = Depends(require_admin),
    ):
        del _user
        row = await _fetch_override(unique_tournament_id)
        if row is None:
            return RedirectResponse(
                url=(
                    f"/admin/tier-overrides?flash=Override+for+UT+{unique_tournament_id}+not+found"
                    f"&flash_kind=danger"
                ),
                status_code=303,
            )
        return _render(
            request,
            "tier_overrides_form.html",
            {"mode": "edit", "row": row, "active": "tier_overrides"},
        )

    @router.post(
        "/tier-overrides/{unique_tournament_id}/edit",
        response_class=HTMLResponse,
        include_in_schema=False,
    )
    async def edit_override_submit(
        request: Request,
        unique_tournament_id: int,
        override_tier: str = Form(...),
        reason: str | None = Form(default=None),
        created_by: str | None = Form(default=None),
        _user: str = Depends(require_admin),
    ):
        del _user
        try:
            await _upsert_override(
                unique_tournament_id=unique_tournament_id,
                override_tier=override_tier,
                reason=reason,
                created_by=created_by,
                is_create=False,
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "admin edit_override_submit failed for ut=%s: %s",
                unique_tournament_id,
                exc,
            )
            row = await _fetch_override(unique_tournament_id) or {
                "unique_tournament_id": unique_tournament_id,
                "override_tier": override_tier,
                "reason": reason,
                "created_by": created_by,
            }
            return _render(
                request,
                "tier_overrides_form.html",
                {
                    "mode": "edit",
                    "row": row,
                    "active": "tier_overrides",
                    "flash": f"Не удалось сохранить: {exc!s}",
                    "flash_kind": "danger",
                },
            )
        return RedirectResponse(
            url=(
                f"/admin/tier-overrides?flash=Override+for+UT+{unique_tournament_id}+updated"
                f"&flash_kind=success"
            ),
            status_code=303,
        )

    @router.post(
        "/tier-overrides/{unique_tournament_id}/delete",
        response_class=HTMLResponse,
        include_in_schema=False,
    )
    async def delete_override(
        unique_tournament_id: int,
        _user: str = Depends(require_admin),
    ):
        del _user
        try:
            await _delete_override(unique_tournament_id)
            flash = f"Override+for+UT+{unique_tournament_id}+deleted"
            flash_kind = "success"
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "admin delete_override failed for ut=%s: %s",
                unique_tournament_id,
                exc,
            )
            flash = f"Delete+failed:+{exc!s}".replace(" ", "+")
            flash_kind = "danger"
        return RedirectResponse(
            url=f"/admin/tier-overrides?flash={flash}&flash_kind={flash_kind}",
            status_code=303,
        )

    return router


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _serialize_row(row: Any) -> dict[str, Any]:
    """asyncpg ``Record`` → plain dict with a stringified timestamp."""
    data = dict(row)
    created_at = data.get("created_at")
    if isinstance(created_at, datetime):
        data["created_at_iso"] = created_at.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    else:
        data["created_at_iso"] = None
    return data
