from __future__ import annotations

import json
import unittest
from unittest.mock import patch
from pathlib import Path

from schema_inspector.fetch import FetchJsonError, fetch_json
from schema_inspector.report import report_filename
from schema_inspector.runtime import RuntimeConfig, TransportResult, TransportAttempt, load_runtime_config
from schema_inspector.schema import infer_schema
from schema_inspector.service import inspect_url_to_markdown
from schema_inspector.sofascore_client import (
    SofascoreAccessDeniedError,
    SofascoreClient,
    SofascoreRateLimitError,
)
from schema_inspector.transport import InspectorTransport


class SchemaInspectorTests(unittest.IsolatedAsyncioTestCase):
    FIXTURES_DIR = Path(__file__).resolve().parent / "fixtures"
    OUTPUT_DIR = Path(__file__).resolve().parent / "_output"

    def test_infer_schema_detects_nested_entities(self) -> None:
        payload = {
            "player": {
                "id": 7,
                "name": "Ada",
                "team": {"id": 99, "name": "United"},
            },
            "events": [
                {"eventId": 1, "score": 3.2},
                {"eventId": 2, "score": None},
            ],
        }

        root = infer_schema(payload)
        object_paths = {node.path for node in root.collect_object_nodes()}

        self.assertIn("root", object_paths)
        self.assertIn("root.player", object_paths)
        self.assertIn("root.player.team", object_paths)
        self.assertIn("root.events[]", object_paths)
        self.assertEqual(root.children["player"].candidate_keys(), ["id"])
        self.assertEqual(root.children["events"].item_summary.candidate_keys(), ["eventId"])

    async def test_fetch_and_report_from_file_url(self) -> None:
        json_path = self.FIXTURES_DIR / "sample_response.json"
        file_url = json_path.resolve().as_uri()
        result = await fetch_json(file_url)
        self.assertEqual(result.status_code, 200)
        self.assertEqual(len(result.attempts), 1)
        self.assertIsNone(result.challenge_reason)

        report_path = await inspect_url_to_markdown(file_url, output_dir=self.OUTPUT_DIR)
        markdown = report_path.read_text(encoding="utf-8")

        self.assertTrue(report_path.exists())
        self.assertIn("Entity: `player_team`", markdown)
        self.assertIn("Entity: `player`", markdown)
        self.assertIn("Candidate Primary Keys: `id`", markdown)
        self.assertIn("## Network Attempts", markdown)
        report_path.unlink(missing_ok=True)

    def test_report_filename_is_stable(self) -> None:
        filename = report_filename("https://api.example.com/v1/player/123?lang=en&scope=full")
        self.assertEqual(filename, "api_example_com_v1_player_123_lang_en_scope_full.md")

    def test_report_filename_is_truncated_for_long_urls(self) -> None:
        long_url = (
            "https://www.sofascore.com/api/v1/unique-tournament/17/season/76986/statistics"
            "?limit=20&order=-rating&accumulation=per90"
            "&fields=goals%2CsuccessfulDribblesPercentage%2CblockedShots%2CgoalsFromOutsideTheBox"
            "&filters=appearances.GT.4%2Cposition.in.G~D~M~F%2Cteam.in.42~40~60~50"
        )

        filename = report_filename(long_url)

        self.assertTrue(filename.endswith(".md"))
        self.assertLessEqual(len(filename), 196)

    def test_load_runtime_config_reads_proxy_and_retry_settings(self) -> None:
        config = load_runtime_config(
            env={
                "SCHEMA_INSPECTOR_PROXY_URL": "http://proxy-1.local:8080",
                "SCHEMA_INSPECTOR_PROXY_URLS": "http://proxy-2.local:8080,http://proxy-3.local:8080",
                "SCHEMA_INSPECTOR_MAX_ATTEMPTS": "5",
                "SCHEMA_INSPECTOR_BACKOFF_SECONDS": "2.5",
                "SCHEMA_INSPECTOR_USER_AGENT": "custom-agent",
            }
        )

        self.assertEqual(config.user_agent, "custom-agent")
        self.assertEqual(config.retry_policy.max_attempts, 5)
        self.assertEqual(config.retry_policy.backoff_seconds, 2.5)
        self.assertEqual(len(config.proxy_endpoints), 3)
        self.assertEqual(config.proxy_endpoints[0].url, "http://proxy-1.local:8080")

    async def test_transport_retries_with_next_proxy_on_retryable_status(self) -> None:
        current_time = [0.0]

        def fake_clock() -> float:
            return current_time[0]

        async def fake_sleep(delay: float) -> None:
            current_time[0] += delay

        config = load_runtime_config(
            env={},
            proxy_urls=["http://proxy-1.local:8080", "http://proxy-2.local:8080"],
            max_attempts=2,
        )
        transport = InspectorTransport(config, sleeper=fake_sleep, clock=fake_clock)

        responses = [
            TransportResult(
                resolved_url="https://example.test/api",
                status_code=429,
                headers={"Content-Type": "application/json"},
                body_bytes=b'{"error":"slow down"}',
                attempts=(),
                final_proxy_name=None,
                challenge_reason="rate_limited",
            ),
            TransportResult(
                resolved_url="https://example.test/api",
                status_code=200,
                headers={"Content-Type": "application/json"},
                body_bytes=b'{"ok": true}',
                attempts=(),
                final_proxy_name=None,
                challenge_reason=None,
            ),
        ]
        observed_proxy_urls = []

        def fake_execute(url, headers, timeout, proxy_url):
            del url, headers, timeout
            observed_proxy_urls.append(proxy_url)
            item = responses.pop(0)
            from schema_inspector.transport import _RawResponse

            return _RawResponse(
                resolved_url=item.resolved_url,
                status_code=item.status_code,
                headers=item.headers,
                body_bytes=item.body_bytes,
            )

        with patch.object(transport, "_execute_once", side_effect=fake_execute):
            result = await transport.fetch("https://example.test/api", headers=None, timeout=10.0)

        self.assertEqual(result.status_code, 200)
        self.assertEqual(len(result.attempts), 2)
        self.assertEqual(observed_proxy_urls, ["http://proxy-1.local:8080", "http://proxy-2.local:8080"])

    async def test_fetch_json_raises_on_challenge_response(self) -> None:
        config = RuntimeConfig()
        with patch(
            "schema_inspector.fetch.SofascoreClient.get_json",
            side_effect=SofascoreAccessDeniedError("Access denied by upstream: status=403, proxy=direct, challenge=bot_challenge"),
        ):
            with self.assertRaises(FetchJsonError):
                await fetch_json("https://example.test/protected", runtime_config=config)

    async def test_sofascore_client_returns_json_payload(self) -> None:
        config = RuntimeConfig()
        mocked_transport_result = TransportResult(
            resolved_url="https://example.test/api",
            status_code=200,
            headers={"Content-Type": "application/json"},
            body_bytes=b'{"ok": true, "items": [1, 2]}',
            attempts=(TransportAttempt(1, "proxy_1", 200, None, None),),
            final_proxy_name="proxy_1",
            challenge_reason=None,
        )

        with patch("schema_inspector.sofascore_client.InspectorTransport.fetch", return_value=mocked_transport_result):
            client = SofascoreClient(config)
            result = await client.get_json("https://example.test/api")

        self.assertEqual(result.payload, {"ok": True, "items": [1, 2]})
        self.assertEqual(result.final_proxy_name, "proxy_1")
        self.assertEqual(result.status_code, 200)

    async def test_sofascore_client_raises_rate_limit_error_on_429(self) -> None:
        config = RuntimeConfig()
        mocked_transport_result = TransportResult(
            resolved_url="https://example.test/api",
            status_code=429,
            headers={"Content-Type": "application/json"},
            body_bytes=b'{"error": "slow down"}',
            attempts=(TransportAttempt(1, "proxy_1", 429, None, "rate_limited"),),
            final_proxy_name="proxy_1",
            challenge_reason="rate_limited",
        )

        with patch("schema_inspector.sofascore_client.InspectorTransport.fetch", return_value=mocked_transport_result):
            client = SofascoreClient(config)
            with self.assertRaises(SofascoreRateLimitError):
                await client.get_json("https://example.test/api")

    async def test_sofascore_client_raises_access_denied_error_on_403(self) -> None:
        config = RuntimeConfig()
        mocked_transport_result = TransportResult(
            resolved_url="https://example.test/protected",
            status_code=403,
            headers={"Content-Type": "text/html"},
            body_bytes=b"<html>verify you are human</html>",
            attempts=(TransportAttempt(1, "proxy_1", 403, None, "bot_challenge"),),
            final_proxy_name="proxy_1",
            challenge_reason="bot_challenge",
        )

        with patch("schema_inspector.sofascore_client.InspectorTransport.fetch", return_value=mocked_transport_result):
            client = SofascoreClient(config)
            with self.assertRaises(SofascoreAccessDeniedError):
                await client.get_json("https://example.test/protected")


if __name__ == "__main__":
    unittest.main()
