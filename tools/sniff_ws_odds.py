"""Read-only sniff: connect to ws.sofascore.com, capture a handful of
odds-type pushes, and check whether their `id` matches event_market.id
in our database. Drives the schema decision for the odds writer.
"""
from __future__ import annotations
import asyncio
import json
import os
import re
import sys

sys.path.insert(0, "/opt/sofascore")
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import asyncpg  # noqa: E402
from curl_cffi.requests import AsyncSession  # type: ignore  # noqa: E402

from schema_inspector.ws_nats_parser import (  # noqa: E402
    build_subscribe_commands,
    parse_nats_frames,
)

URI = "wss://ws.sofascore.com:9222/"
CONNECT = (
    'CONNECT {"protocol":1,"version":"3.1.0","lang":"nats.ws",'
    '"verbose":false,"pedantic":false,"user":"none","pass":"none",'
    '"headers":true,"no_responders":true}\r\n'
)
HEADERS = {
    "Origin": "https://www.sofascore.com",
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
}
TARGET_ODDS = 30


def env_url() -> str:
    with open("/opt/sofascore/.env") as f:
        for line in f:
            m = re.search(r"postgresql://\S+", line)
            if m:
                return m.group(0).strip()
    raise RuntimeError("no DB url")


async def main() -> None:
    odds_msgs: list[dict] = []

    async with AsyncSession(impersonate="chrome120") as session:
        ws = await session.ws_connect(URI, headers=HEADERS)
        await ws.send(CONNECT.encode())
        for cmd in build_subscribe_commands(("football", "tennis", "basketball"), include_odds=True):
            await ws.send(cmd)
            await asyncio.sleep(0.05)

        buffer = ""
        deadline = asyncio.get_event_loop().time() + 45
        while len(odds_msgs) < TARGET_ODDS and asyncio.get_event_loop().time() < deadline:
            chunk, _ = await ws.recv()
            if isinstance(chunk, bytes):
                chunk = chunk.decode("utf-8", errors="replace")
            buffer += chunk
            frames, buffer = parse_nats_frames(buffer)
            for kind, payload in frames:
                if kind == "PING":
                    await ws.send(b"PONG\r\n")
                if kind == "MSG":
                    subject, sid, data_str = payload
                    if subject.startswith("odds."):
                        try:
                            odds_msgs.append({"subject": subject, "data": json.loads(data_str)})
                        except json.JSONDecodeError:
                            pass

        await ws.close()

    if not odds_msgs:
        print("no odds messages captured (low live volume?)")
        return

    # Take first 10 unique offer ids and look them up in event_market.
    offer_ids: list[int] = []
    for m in odds_msgs:
        oid = m["data"].get("id")
        if isinstance(oid, int) and oid not in offer_ids:
            offer_ids.append(oid)
        if len(offer_ids) >= 10:
            break

    print(f"captured {len(odds_msgs)} odds messages, {len(offer_ids)} unique offer ids")
    print("first 5 raw messages:")
    for m in odds_msgs[:5]:
        print(f"  subject={m['subject']}  payload={m['data']}")

    conn = await asyncpg.connect(env_url())
    try:
        rows = await conn.fetch(
            """
            SELECT id, event_id, fid, market_id, market_name, market_period,
                   is_live, suspended
            FROM event_market
            WHERE id = ANY($1::bigint[])
            """,
            offer_ids,
        )
        found = {int(r["id"]) for r in rows}
        print(f"\n=== event_market lookup by id ===")
        print(f"WS offer ids tried: {offer_ids}")
        print(f"matched by event_market.id: {len(found)}/{len(offer_ids)}")
        for r in rows[:10]:
            print(f"  market_id={r['id']} event_id={r['event_id']} fid={r['fid']} "
                  f"name={r['market_name']!r} period={r['market_period']!r} "
                  f"live={r['is_live']} susp={r['suspended']}")

        if found:
            sample_id = next(iter(found))
            choices = await conn.fetch(
                """
                SELECT source_id, name, fractional_value, initial_fractional_value
                FROM event_market_choice
                WHERE event_market_id = $1
                ORDER BY source_id
                """,
                sample_id,
            )
            print(f"\n=== choices for sample event_market_id={sample_id} ===")
            for c in choices:
                print(f"  choice source_id={c['source_id']} name={c['name']!r} "
                      f"frac={c['fractional_value']!r} init={c['initial_fractional_value']!r}")

        # Also try by fid in case `id` is the fid.
        rows_by_fid = await conn.fetch(
            "SELECT id, event_id, fid, market_name FROM event_market WHERE fid = ANY($1::bigint[])",
            offer_ids,
        )
        print(f"\n=== event_market lookup by fid ===")
        print(f"matched by event_market.fid: {len(rows_by_fid)}/{len(offer_ids)}")
        for r in rows_by_fid[:5]:
            print(f"  market_id={r['id']} fid={r['fid']} event_id={r['event_id']}")

    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
