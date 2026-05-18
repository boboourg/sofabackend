"""Tests for the WS fanout publisher — pushes deltas from the
consumer's dispatch path onto Redis Pub/Sub channels that the
mirror WS server subscribes to.

Pure async unit tests against a fake redis client. The IO side
(actual redis.publish) is tested indirectly through the smoke
test in production.
"""
from __future__ import annotations
import json
import unittest
from typing import Any


class FakeRedis:
    def __init__(self) -> None:
        self.published: list[tuple[str, str]] = []

    async def publish(self, channel: str, message: str) -> int:
        self.published.append((channel, message))
        return 1  # one subscriber assumed


class FanoutPublisherTests(unittest.IsolatedAsyncioTestCase):
    async def test_event_delta_publishes_to_sport_channel(self) -> None:
        from schema_inspector.ws_fanout_publisher import RedisFanoutPublisher

        redis = FakeRedis()
        pub = RedisFanoutPublisher(redis)
        await pub.publish_event(
            subject="sport.football",
            payload={"id": 15171570, "homeScore.current": 2},
        )

        # Channel scheme: ws:fanout:sport:<slug>
        channels = [ch for ch, _ in redis.published]
        self.assertIn("ws:fanout:sport:football", channels)

    async def test_event_delta_also_fanouts_to_event_channel(self) -> None:
        """If the delta carries an event_id, we ALSO publish to
        ws:fanout:event:<id> so a client subscribed to a single match
        receives the update without needing to subscribe to the whole
        sport stream."""
        from schema_inspector.ws_fanout_publisher import RedisFanoutPublisher

        redis = FakeRedis()
        pub = RedisFanoutPublisher(redis)
        await pub.publish_event(
            subject="sport.football",
            payload={"id": 15171570, "homeScore.current": 2},
        )

        channels = [ch for ch, _ in redis.published]
        self.assertIn("ws:fanout:event:15171570", channels)

    async def test_event_message_carries_subject_and_payload(self) -> None:
        from schema_inspector.ws_fanout_publisher import RedisFanoutPublisher

        redis = FakeRedis()
        pub = RedisFanoutPublisher(redis)
        await pub.publish_event(
            subject="sport.football",
            payload={"id": 1, "homeScore.current": 2},
        )
        msg_payload = json.loads(redis.published[0][1])
        # Format: {"subject": "sport.football", "payload": {...}}
        self.assertEqual(msg_payload["subject"], "sport.football")
        self.assertEqual(msg_payload["payload"], {"id": 1, "homeScore.current": 2})

    async def test_odds_delta_publishes_to_sport_odds_channel(self) -> None:
        from schema_inspector.ws_fanout_publisher import RedisFanoutPublisher

        redis = FakeRedis()
        pub = RedisFanoutPublisher(redis)
        await pub.publish_odds(
            subject="odds.football.1",
            payload={"id": 310428361, "choice1.fractionalValue": "5/6"},
        )
        channels = [ch for ch, _ in redis.published]
        # odds → ws:fanout:odds:<sport>:<market_id>
        self.assertIn("ws:fanout:odds:football:1", channels)

    async def test_odds_publishes_to_event_when_event_id_resolved(self) -> None:
        """When the caller already resolved offer_id → event_id, the
        publisher can also fanout to the event channel."""
        from schema_inspector.ws_fanout_publisher import RedisFanoutPublisher

        redis = FakeRedis()
        pub = RedisFanoutPublisher(redis)
        await pub.publish_odds(
            subject="odds.football.1",
            payload={"id": 310428361, "choice1.fractionalValue": "5/6"},
            event_id=16195558,
        )
        channels = [ch for ch, _ in redis.published]
        self.assertIn("ws:fanout:event:16195558", channels)

    async def test_publisher_swallows_redis_errors(self) -> None:
        """Best-effort: a redis hiccup should not crash the dispatch
        loop. The publisher logs and moves on."""
        from schema_inspector.ws_fanout_publisher import RedisFanoutPublisher

        class FailingRedis:
            async def publish(self, *_a: Any, **_kw: Any) -> int:
                raise ConnectionError("nope")

        pub = RedisFanoutPublisher(FailingRedis())
        # Must not raise
        await pub.publish_event(subject="sport.football", payload={"id": 1})

    async def test_payload_without_event_id_skips_event_channel(self) -> None:
        from schema_inspector.ws_fanout_publisher import RedisFanoutPublisher

        redis = FakeRedis()
        pub = RedisFanoutPublisher(redis)
        await pub.publish_event(
            subject="sport.football",
            payload={"homeScore.current": 1},  # no id
        )
        channels = [ch for ch, _ in redis.published]
        # Sport channel still receives it; event channel is not built
        self.assertEqual(channels, ["ws:fanout:sport:football"])

    async def test_publisher_works_without_redis(self) -> None:
        """When redis is None (e.g. WS server disabled), the publisher
        is a no-op — the consumer still works."""
        from schema_inspector.ws_fanout_publisher import RedisFanoutPublisher

        pub = RedisFanoutPublisher(None)
        await pub.publish_event(subject="sport.football", payload={"id": 1})
        # Just verify no crash.


if __name__ == "__main__":
    unittest.main()
