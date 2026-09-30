from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime

import websockets

from newswave.clock import ReplayClock
from newswave.config import Settings
from newswave.news.stream import NewsStream, parse_news_message

RAW = {"T": "n", "id": 24918784, "headline": "Corsair Gaming Announces Q3 Results", "summary": "s",
       "author": "Benzinga Newsdesk", "created_at": "2026-01-02T15:00:00Z",
       "updated_at": "2026-01-02T15:00:01.250Z", "url": "https://x/y", "content": "<p>c</p>",
       "symbols": ["CRSR"], "source": "benzinga"}
NOW = datetime(2026, 1, 2, 15, 0, 2, tzinfo=UTC)


def test_parse_news_message():
    e = parse_news_message(RAW, NOW)
    assert (e.article_id, e.symbols, e.source, e.url) == ("24918784", ("CRSR",), "benzinga", "https://x/y")
    assert e.received_at == "2026-01-02T15:00:02.000Z" and e.created_at == "2026-01-02T15:00:00.000Z"
    assert e.updated_at == "2026-01-02T15:00:01.250Z" and e.content == "<p>c</p>"
    assert not hasattr(e, "author")


def test_parse_missing_optionals():
    e = parse_news_message({"id": 1, "created_at": "2026-01-02T15:00:00Z", "symbols": None}, NOW)
    assert e.symbols == () and e.headline == "" and e.updated_at == e.created_at


async def test_stream_end_to_end_ignores_control_and_bad_frames():
    subs = []

    async def handler(ws):
        await ws.send(json.dumps([{"T": "success", "msg": "connected"}]))
        async for raw in ws:
            m = json.loads(raw)
            if m["action"] == "auth":
                await ws.send(json.dumps([{"T": "success", "msg": "authenticated"}]))
            else:
                subs.append(m)
                await ws.send(json.dumps([{"T": "subscription", "news": ["*"]}, {"T": "n", "id": 1},  # bad: no created_at
                                          RAW, {**RAW, "id": 2}]))

    srv = await websockets.serve(handler, "127.0.0.1", 0)
    got = []

    async def on_news(e):
        got.append(e)

    ns = NewsStream(Settings(), ReplayClock(NOW), on_news, url=f"ws://127.0.0.1:{srv.sockets[0].getsockname()[1]}")
    task = asyncio.create_task(ns.run())
    try:
        for _ in range(250):
            if len(got) >= 2:
                break
            await asyncio.sleep(0.02)
        assert [e.article_id for e in got] == ["24918784", "2"]
        assert subs[0] == {"action": "subscribe", "news": ["*"]}
        assert got[0].received_at == "2026-01-02T15:00:02.000Z"
    finally:
        ns.stop()
        await asyncio.wait_for(task, 5)
        srv.close()


def test_news_socket_uses_a_long_stale_threshold_not_the_60s_default():
    from newswave.news.stream import NEWS_STALE_AFTER_S
    ns = NewsStream(Settings(), ReplayClock(NOW), lambda e: None)
    assert ns.client.stale_after_s == NEWS_STALE_AFTER_S >= 1800 > 60


async def test_malformed_news_frame_lands_in_system_events(tmp_db):
    ns = NewsStream(Settings(), ReplayClock(NOW), lambda e: None, db=tmp_db)
    await ns._on_message({"T": "n", "id": 77, "headline": "no created_at"})
    [ev] = tmp_db.query("SELECT * FROM system_events")
    assert (ev["level"], ev["component"], ev["event"]) == ("ERROR", "news_stream", "MALFORMED_NEWS_FRAME")
    assert json.loads(ev["data_json"])["id"] == "77"
