"""Tests for the NATS-over-WebSocket frame parser.

The parser converts the raw text stream we receive from
wss://ws.sofascore.com:9222 into discrete (kind, payload) tuples
which the consumer dispatches downstream.

The protocol is plain text NATS:
  PING\r\n / PONG\r\n
  +OK\r\n
  -ERR <msg>\r\n
  INFO <json>\r\n
  MSG <subject> <sid> <byte-count>\r\n<payload>\r\n
"""
from __future__ import annotations
import unittest


class ParseNatsFramesTests(unittest.TestCase):
    def test_empty_buffer_returns_no_messages(self) -> None:
        from schema_inspector.ws_nats_parser import parse_nats_frames

        msgs, leftover = parse_nats_frames("")
        self.assertEqual(msgs, [])
        self.assertEqual(leftover, "")

    def test_ping_message(self) -> None:
        from schema_inspector.ws_nats_parser import parse_nats_frames

        msgs, leftover = parse_nats_frames("PING\r\n")
        self.assertEqual(msgs, [("PING", None)])
        self.assertEqual(leftover, "")

    def test_pong_message(self) -> None:
        from schema_inspector.ws_nats_parser import parse_nats_frames

        msgs, leftover = parse_nats_frames("PONG\r\n")
        self.assertEqual(msgs, [("PONG", None)])

    def test_ok_acknowledgement(self) -> None:
        from schema_inspector.ws_nats_parser import parse_nats_frames

        msgs, leftover = parse_nats_frames("+OK\r\n")
        self.assertEqual(msgs, [("OK", None)])

    def test_err_message_carries_full_header(self) -> None:
        from schema_inspector.ws_nats_parser import parse_nats_frames

        msgs, leftover = parse_nats_frames("-ERR 'Unknown protocol'\r\n")
        self.assertEqual(msgs[0][0], "ERR")
        self.assertIn("Unknown protocol", msgs[0][1])

    def test_info_message_carries_json_string(self) -> None:
        from schema_inspector.ws_nats_parser import parse_nats_frames

        msgs, leftover = parse_nats_frames('INFO {"server_id":"abc"}\r\n')
        self.assertEqual(msgs[0][0], "INFO")
        self.assertEqual(msgs[0][1], '{"server_id":"abc"}')

    def test_msg_frame_with_payload(self) -> None:
        from schema_inspector.ws_nats_parser import parse_nats_frames

        # Frame: MSG sport.football 1 23\r\n{"id":1,"homeScore":1}\r\n
        payload = '{"id":1,"homeScore":1}'
        frame = f"MSG sport.football 1 {len(payload)}\r\n{payload}\r\n"
        msgs, leftover = parse_nats_frames(frame)
        self.assertEqual(len(msgs), 1)
        kind, (subject, sid, data_str) = msgs[0]
        self.assertEqual(kind, "MSG")
        self.assertEqual(subject, "sport.football")
        self.assertEqual(sid, 1)
        self.assertEqual(data_str, payload)
        self.assertEqual(leftover, "")

    def test_multiple_frames_in_one_buffer(self) -> None:
        from schema_inspector.ws_nats_parser import parse_nats_frames

        payload1 = '{"id":1}'
        payload2 = '{"id":2}'
        buffer = (
            f"PING\r\n"
            f"MSG sport.football 1 {len(payload1)}\r\n{payload1}\r\n"
            f"MSG sport.tennis 3 {len(payload2)}\r\n{payload2}\r\n"
        )
        msgs, leftover = parse_nats_frames(buffer)
        self.assertEqual([m[0] for m in msgs], ["PING", "MSG", "MSG"])
        self.assertEqual(leftover, "")

    def test_partial_msg_payload_kept_in_leftover(self) -> None:
        """If the buffer contains only half a MSG payload, the parser
        must return what it can and keep the rest for the next chunk."""
        from schema_inspector.ws_nats_parser import parse_nats_frames

        payload = '{"id":1,"homeScore":1}'
        # Truncate buffer mid-payload (10 bytes of 22).
        truncated = f"MSG sport.football 1 {len(payload)}\r\n{payload[:10]}"
        msgs, leftover = parse_nats_frames(truncated)
        self.assertEqual(msgs, [])
        # Leftover keeps the whole pending frame.
        self.assertEqual(leftover, truncated)

    def test_partial_header_kept_in_leftover(self) -> None:
        """A buffer that has only "MSG sport.foo 1 " (no \r\n) is held."""
        from schema_inspector.ws_nats_parser import parse_nats_frames

        truncated = "MSG sport.foo 1 "
        msgs, leftover = parse_nats_frames(truncated)
        self.assertEqual(msgs, [])
        self.assertEqual(leftover, truncated)

    def test_malformed_msg_header_skipped(self) -> None:
        """If the MSG header is malformed (e.g. missing byte count), we
        skip past it and keep parsing the rest of the buffer rather
        than getting stuck in an infinite loop."""
        from schema_inspector.ws_nats_parser import parse_nats_frames

        # Missing payload size.
        buffer = "MSG sport.football 1\r\nPING\r\n"
        msgs, leftover = parse_nats_frames(buffer)
        # The PING after the broken MSG must still parse.
        self.assertEqual(msgs, [("PING", None)])
        self.assertEqual(leftover, "")

    def test_partial_msg_with_trailing_pong(self) -> None:
        """Real-life chunking: first chunk only contains the MSG header
        + partial payload; the next chunk completes the payload and
        adds a PONG. The parser must hold the partial payload, return
        nothing on chunk 1, then on chunk 2 return MSG + PONG."""
        from schema_inspector.ws_nats_parser import parse_nats_frames

        payload = '{"id":1,"homeScore":2}'
        chunk1 = f"MSG sport.football 1 {len(payload)}\r\n{payload[:10]}"
        msgs1, leftover1 = parse_nats_frames(chunk1)
        self.assertEqual(msgs1, [])

        chunk2 = leftover1 + payload[10:] + "\r\nPONG\r\n"
        msgs2, leftover2 = parse_nats_frames(chunk2)
        self.assertEqual([m[0] for m in msgs2], ["MSG", "PONG"])
        self.assertEqual(leftover2, "")


class BuildSubscribeCommandsTests(unittest.TestCase):
    def test_sport_event_and_odds_for_13_sports(self) -> None:
        from schema_inspector.ws_nats_parser import build_subscribe_commands

        cmds = build_subscribe_commands(
            sports=["football", "tennis"],
            event_sid_offset=1,
            odds_sid_offset=101,
        )
        # Expect SUB sport.football 1, SUB odds.football.1 101,
        # SUB sport.tennis 2, SUB odds.tennis.1 102.
        self.assertEqual(len(cmds), 4)
        self.assertIn(b"SUB sport.football 1\r\n", cmds)
        self.assertIn(b"SUB odds.football.1 101\r\n", cmds)
        self.assertIn(b"SUB sport.tennis 2\r\n", cmds)
        self.assertIn(b"SUB odds.tennis.1 102\r\n", cmds)

    def test_sport_only_skips_odds(self) -> None:
        """Some deployments will run a football-only consumer with no
        odds subscriptions to drop fanout load."""
        from schema_inspector.ws_nats_parser import build_subscribe_commands

        cmds = build_subscribe_commands(
            sports=["football"], include_odds=False, event_sid_offset=1,
        )
        self.assertEqual(cmds, [b"SUB sport.football 1\r\n"])


class SubjectToSportTests(unittest.TestCase):
    def test_resolve_sport_from_event_subject(self) -> None:
        from schema_inspector.ws_nats_parser import subject_to_sport

        self.assertEqual(subject_to_sport("sport.football"), "football")
        self.assertEqual(subject_to_sport("sport.ice-hockey"), "ice-hockey")

    def test_resolve_sport_from_odds_subject(self) -> None:
        from schema_inspector.ws_nats_parser import subject_to_sport

        self.assertEqual(subject_to_sport("odds.football.1"), "football")

    def test_unknown_subject(self) -> None:
        from schema_inspector.ws_nats_parser import subject_to_sport

        self.assertIsNone(subject_to_sport("noise.foo.bar"))

    def test_msg_kind_from_subject(self) -> None:
        from schema_inspector.ws_nats_parser import subject_to_msg_type

        self.assertEqual(subject_to_msg_type("sport.football"), "event")
        self.assertEqual(subject_to_msg_type("odds.football.1"), "odds")
        self.assertIsNone(subject_to_msg_type("noise"))


if __name__ == "__main__":
    unittest.main()
