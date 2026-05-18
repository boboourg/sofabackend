"""Verify that AsyncSession kwargs include a usable CA bundle path so
HTTPS requests via curl_cffi don't fail with ErrCode 77
(CURLE_SSL_CACERT_BADFILE) after the 0.7+ upgrade dropped the
package-bundled cacert.pem.

Reference incident: 2026-05-18 — /api/v1/event/{event_id} fell to
47.6 % transport_err with the cacert.pem-not-found error after we
bumped curl_cffi from 0.5.10 to 0.15.0 for WS ws_connect support.
"""
from __future__ import annotations
import os
import unittest

import certifi

from schema_inspector.runtime import RuntimeConfig, TlsPolicy
from schema_inspector.transport import InspectorTransport


class SessionVerifyDefaultsToCertifiTests(unittest.TestCase):
    def test_session_kwargs_sets_verify_to_certifi_when_policy_unset(self) -> None:
        """When ``tls_policy.verify`` is ``None`` (the default), the
        transport must explicitly pass certifi's bundle path to
        ``AsyncSession`` so libcurl finds a CA file inside the venv."""
        transport = InspectorTransport(RuntimeConfig())
        kwargs = transport._session_kwargs(proxy_url=None)
        self.assertIn("verify", kwargs)
        self.assertEqual(kwargs["verify"], certifi.where())
        # certifi.where() must point to a file that actually exists.
        self.assertTrue(os.path.isfile(kwargs["verify"]))

    def test_explicit_policy_verify_overrides_certifi_default(self) -> None:
        """Operator-set ``verify`` (str path or ``False``) must take
        precedence over the certifi fallback."""
        custom_path = "/etc/ssl/certs/ca-certificates.crt"
        transport = InspectorTransport(
            RuntimeConfig(tls_policy=TlsPolicy(verify=custom_path))
        )
        kwargs = transport._session_kwargs(proxy_url=None)
        self.assertEqual(kwargs["verify"], custom_path)

    def test_verify_false_disables_check(self) -> None:
        """``verify=False`` (sometimes needed for self-signed dev) must
        propagate, not get clobbered by the certifi default."""
        transport = InspectorTransport(
            RuntimeConfig(tls_policy=TlsPolicy(verify=False))
        )
        kwargs = transport._session_kwargs(proxy_url=None)
        self.assertEqual(kwargs["verify"], False)


if __name__ == "__main__":
    unittest.main()
