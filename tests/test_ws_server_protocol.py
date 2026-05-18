"""Tests for the mirror WS server's protocol & subscription manager.

The server speaks a NATS-subset (mirror of Sofascore's wire format):
  Client → Server:
    CONNECT {json}\\r\\n
    SUB <subject> <sid>\\r\\n
    UNSUB <sid>\\r\\n
    PING\\r\\n  / PONG\\r\\n
  Server → Client:
    INFO {json}\\r\\n   (sent on connect)
    MSG <subject> <sid> <bytes>\\r\\n<payload>\\r\\n
    PING\\r\\n  / PONG\\r\\n
"""
from __future__ import annotations
import json
import unittest


class ParseClientFrameTests(unittest.TestCase):
    def test_connect_returns_no_op_marker(self) -> None:
        from schema_inspector.ws_server_protocol import parse_client_frames

        msgs, _ = parse_client_frames('CONNECT {"verbose":false}\r\n')
        self.assertEqual(len(msgs), 1)
        self.assertEqual(msgs[0][0], "CONNECT")

    def test_sub_parses_subject_and_sid(self) -> None:
        from schema_inspector.ws_server_protocol import parse_client_frames

        msgs, _ = parse_client_frames("SUB sport.football 1\r\n")
        self.assertEqual(msgs, [("SUB", ("sport.football", 1))])

    def test_unsub_parses_sid(self) -> None:
        from schema_inspector.ws_server_protocol import parse_client_frames

        msgs, _ = parse_client_frames("UNSUB 1\r\n")
        self.assertEqual(msgs, [("UNSUB", 1)])

    def test_ping_pong(self) -> None:
        from schema_inspector.ws_server_protocol import parse_client_frames

        msgs, _ = parse_client_frames("PING\r\nPONG\r\n")
        self.assertEqual([m[0] for m in msgs], ["PING", "PONG"])

    def test_partial_sub_left_in_leftover(self) -> None:
        from schema_inspector.ws_server_protocol import parse_client_frames

        msgs, leftover = parse_client_frames("SUB sport.foot")
        self.assertEqual(msgs, [])
        self.assertEqual(leftover, "SUB sport.foot")


class SubscriptionManagerTests(unittest.TestCase):
    def test_subscribe_then_get_matching_subjects(self) -> None:
        from schema_inspector.ws_server_protocol import SubscriptionManager

        sm = SubscriptionManager()
        sm.subscribe("client-A", subject="sport.football", sid=1)
        sm.subscribe("client-B", subject="sport.tennis", sid=1)
        sm.subscribe("client-A", subject="event.16167494", sid=2)

        # Lookup channels we need to listen to (server-side)
        channels = sm.channels_to_listen()
        self.assertIn("ws:fanout:sport:football", channels)
        self.assertIn("ws:fanout:sport:tennis", channels)
        self.assertIn("ws:fanout:event:16167494", channels)

    def test_dispatch_matches_only_subscribed_clients(self) -> None:
        from schema_inspector.ws_server_protocol import SubscriptionManager

        sm = SubscriptionManager()
        sm.subscribe("A", subject="sport.football", sid=1)
        sm.subscribe("B", subject="sport.tennis", sid=1)

        # Server received a fanout message on sport.football channel.
        matches = sm.matches_for(channel="ws:fanout:sport:football")
        # Returns list of (client_id, sid, subject) tuples for clients
        # subscribed to this channel.
        client_ids = [m[0] for m in matches]
        self.assertEqual(client_ids, ["A"])

    def test_unsubscribe_removes_sid(self) -> None:
        from schema_inspector.ws_server_protocol import SubscriptionManager

        sm = SubscriptionManager()
        sm.subscribe("A", subject="sport.football", sid=1)
        sm.unsubscribe("A", sid=1)
        self.assertEqual(sm.channels_to_listen(), set())
        self.assertEqual(sm.matches_for("ws:fanout:sport:football"), [])

    def test_disconnect_removes_all_subscriptions(self) -> None:
        from schema_inspector.ws_server_protocol import SubscriptionManager

        sm = SubscriptionManager()
        sm.subscribe("A", subject="sport.football", sid=1)
        sm.subscribe("A", subject="event.123", sid=2)
        sm.subscribe("B", subject="sport.football", sid=1)
        sm.disconnect("A")
        # B's subscription survives
        matches = sm.matches_for("ws:fanout:sport:football")
        self.assertEqual([m[0] for m in matches], ["B"])

    def test_multiple_subs_same_subject_different_clients(self) -> None:
        from schema_inspector.ws_server_protocol import SubscriptionManager

        sm = SubscriptionManager()
        sm.subscribe("A", subject="sport.football", sid=1)
        sm.subscribe("B", subject="sport.football", sid=2)
        sm.subscribe("C", subject="sport.football", sid=99)

        matches = sm.matches_for("ws:fanout:sport:football")
        ids = sorted([m[0] for m in matches])
        self.assertEqual(ids, ["A", "B", "C"])

    def test_subject_to_channel_event(self) -> None:
        from schema_inspector.ws_server_protocol import subject_to_channel

        self.assertEqual(subject_to_channel("event.16167494"), "ws:fanout:event:16167494")

    def test_subject_to_channel_sport(self) -> None:
        from schema_inspector.ws_server_protocol import subject_to_channel

        self.assertEqual(subject_to_channel("sport.football"), "ws:fanout:sport:football")

    def test_subject_to_channel_odds(self) -> None:
        from schema_inspector.ws_server_protocol import subject_to_channel

        self.assertEqual(subject_to_channel("odds.football.1"), "ws:fanout:odds:football:1")


class FormatServerFrameTests(unittest.TestCase):
    def test_format_msg_frame(self) -> None:
        from schema_inspector.ws_server_protocol import format_msg_frame

        payload = '{"id":1}'
        frame = format_msg_frame(subject="sport.football", sid=1, payload=payload)
        # MSG <subject> <sid> <bytes>\r\n<payload>\r\n
        expected = f"MSG sport.football 1 {len(payload)}\r\n{payload}\r\n"
        self.assertEqual(frame, expected)

    def test_format_info_frame(self) -> None:
        from schema_inspector.ws_server_protocol import format_info_frame

        frame = format_info_frame(server_id="mirror-1", version="0.1")
        # INFO <json>\r\n
        self.assertTrue(frame.startswith("INFO "))
        self.assertTrue(frame.endswith("\r\n"))
        body = json.loads(frame[len("INFO "):-2])
        self.assertEqual(body["server_id"], "mirror-1")
        self.assertEqual(body["version"], "0.1")


if __name__ == "__main__":
    unittest.main()
