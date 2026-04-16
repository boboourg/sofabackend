from __future__ import annotations

import unittest

from schema_inspector.runtime import load_runtime_config
from schema_inspector.transport import InspectorTransport


class TransportRetryPolicyTests(unittest.TestCase):
    def test_proxy_cooldown_decisions_follow_retry_policy_and_challenge_reason(self) -> None:
        transport = InspectorTransport(load_runtime_config(env={}, proxy_urls=["http://proxy-1.local:8080"]))

        self.assertTrue(transport._should_cooldown_proxy(429, None))
        self.assertTrue(transport._should_cooldown_proxy(403, "access_denied"))
        self.assertTrue(transport._should_cooldown_proxy(503, None))
        self.assertFalse(transport._should_cooldown_proxy(404, None))
        self.assertFalse(transport._should_cooldown_proxy(200, None))


if __name__ == "__main__":
    unittest.main()
