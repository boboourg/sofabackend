"""Sofascore admin UI (FastAPI + HTMX + Jinja2).

The admin is a single-user internal tool gated by HTTP Basic auth.
Disabled by default; set ``SOFASCORE_ADMIN_PASSWORD=<long-secret>`` in
``.env`` to enable. Routes are mounted under ``/admin`` from
``local_api_server.py`` only when both the password and Jinja2 are
available — otherwise the router silently no-ops, keeping the public
API surface untouched.
"""

from .router import build_admin_router  # noqa: F401
