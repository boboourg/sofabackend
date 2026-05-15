"""HTTP Basic auth for the admin UI.

Password comes from the ``SOFASCORE_ADMIN_PASSWORD`` environment
variable; absence of the var disables the admin entirely (the router
factory returns ``None`` so ``local_api_server.py`` knows to skip
mounting). Username is hardcoded to ``"admin"`` because the project is
single-user — adding a user table is an Admin-Phase-2 problem.

We compare via ``secrets.compare_digest`` to avoid timing leaks and we
*never* log the password (even at DEBUG level). On a missed-auth the
browser receives ``WWW-Authenticate: Basic realm="..."`` so it pops a
native credentials prompt on the next request.
"""

from __future__ import annotations

import logging
import os
import secrets

from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBasic, HTTPBasicCredentials

logger = logging.getLogger(__name__)

_ENV_PASSWORD = "SOFASCORE_ADMIN_PASSWORD"
_ADMIN_USERNAME = "admin"
_REALM = "Sofascore Admin"

_basic = HTTPBasic(realm=_REALM, auto_error=False)


def admin_password_from_env() -> str | None:
    """Return the configured admin password, or ``None`` if unset.

    Treats empty string and whitespace-only values as unset so a stray
    ``SOFASCORE_ADMIN_PASSWORD=`` in the env file does not silently
    open the admin to anonymous access.
    """
    raw = os.environ.get(_ENV_PASSWORD)
    if raw is None:
        return None
    cleaned = raw.strip()
    if not cleaned:
        return None
    return cleaned


def require_admin(credentials: HTTPBasicCredentials | None = Depends(_basic)) -> str:
    """FastAPI dependency: return the authenticated username or 401.

    Raises ``HTTPException(401)`` with a ``WWW-Authenticate`` header so
    the browser prompts for credentials on the next request rather than
    just showing a blank 401 page.
    """
    expected_password = admin_password_from_env()
    if expected_password is None:
        # Belt-and-braces: router factory should have prevented mount,
        # but if it didn't, refuse access loudly rather than letting
        # someone in unauthenticated.
        logger.error(
            "admin route reached without %s configured; denying access",
            _ENV_PASSWORD,
        )
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Admin disabled: SOFASCORE_ADMIN_PASSWORD not configured.",
        )
    if credentials is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Authentication required.",
            headers={"WWW-Authenticate": f'Basic realm="{_REALM}"'},
        )
    username_ok = secrets.compare_digest(credentials.username or "", _ADMIN_USERNAME)
    password_ok = secrets.compare_digest(credentials.password or "", expected_password)
    if not (username_ok and password_ok):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid credentials.",
            headers={"WWW-Authenticate": f'Basic realm="{_REALM}"'},
        )
    return _ADMIN_USERNAME
