from __future__ import annotations

import asyncio
import json

import pytest
import websockets

from newswave.wsclient import AlpacaWSClient


class Server:
    """Minimal Alpaca-like server. Records every client frame per connection."""

    def __init__(self, auth_error: int | None = None, silent: bool = False):
        self.conns: list[list[dict]] = []
        self.sockets: list = []
        self.auth_error, self.silent = auth_error, silent

    async def handler(self, ws):
        log: list[dict] = []
        self.conns.append(log)
        self.sockets.append(ws)
        await ws.send(json.dumps([{"T": "success", "msg": "connected"}]))
        async for raw in ws:
            msg = json.loads(raw)
            log.append(msg)
            if msg["action"] == "auth":
                if self.auth_error:
                    await ws.send(json.dumps([{"T": "error", "code": self.auth_error, "msg": "x"}]))
                else:
                    await ws.send(json.dumps([{"T": "success", "msg": "authenticated"}]))
            elif msg["action"] == "subscribe" and not self.silent:
                await ws.send(json.dumps([{"T": "t", "S": "AAPL", "p": 1.0}, {"T": "t", "S": "MSFT", "p": 2.0}]))


async def until(pred, timeout=5.0):
    end = asyncio.get_running_loop().time() + timeout
    while not pred():
        if asyncio.get_running_loop().time() > end:
            raise AssertionError("timeout")
        await asyncio.sleep(0.02)


async def start(server, **kw):
    srv = await websockets.serve(server.handler, "127.0.0.1", 0)
    port = srv.sockets[0].getsockname()[1]
    got: list[dict] = []
    c = AlpacaWSClient(f"ws://127.0.0.1:{port}", "K", "S", got.append, backoff_min_s=0.05,
                       backoff_max_s=0.2, **kw)
    return srv, c, got, asyncio.create_task(c.run())


async def test_auth_subscribe_deliver_and_resubscribe_after_drop():
    s = Server()
    srv, c, got, task = await start(s)
    try:
        c.subscribe(trades=["AAPL"], bars=["AAPL"])  # queued before connect
        await until(lambda: len(got) >= 2)
        assert s.conns[0][0] == {"action": "auth", "key": "K", "secret": "S"}
        assert s.conns[0][1] == {"action": "subscribe", "bars": ["AAPL"], "trades": ["AAPL"]}
        assert {m["S"] for m in got} == {"AAPL", "MSFT"}  # control frames not forwarded, lists unpacked

        c.subscribe(trades=["NVDA"])  # live
        await until(lambda: any(m.get("trades") == ["NVDA"] for m in s.conns[0]))
        c.unsubscribe(trades=["AAPL"])
        await until(lambda: any(m["action"] == "unsubscribe" for m in s.conns[0]))

        await s.sockets[0].close()  # server drop
        await until(lambda: len(s.conns) == 2 and len(s.conns[1]) >= 2)
        assert s.conns[1][1] == {"action": "subscribe", "bars": ["AAPL"], "trades": ["NVDA"]}
        assert c.connects == 2
    finally:
        c.stop()
        await asyncio.wait_for(task, 5)
        srv.close()


async def test_auth_failure_backs_off_not_tight_loop():
    s = Server(auth_error=402)
    srv, c, got, task = await start(s, hard_backoff_s=0.5)
    try:
        await asyncio.sleep(0.4)
        assert len(s.conns) == 1  # would be ~8 with plain 0.05-0.2s backoff
        assert not c.ready
    finally:
        c.stop()
        await asyncio.wait_for(task, 5)
        srv.close()


async def test_staleness_watchdog_reconnects():
    s = Server(silent=True)
    srv, c, got, task = await start(s, stale_after_s=0.3)
    try:
        c.subscribe(trades=["AAPL"])
        await until(lambda: len(s.conns) >= 2 and any(m.get("action") == "subscribe" for m in s.conns[1]))
        assert s.conns[1][-1]["action"] == "subscribe"
    finally:
        c.stop()
        await asyncio.wait_for(task, 5)
        srv.close()


async def test_async_callback_and_callback_error_isolated():
    s = Server()
    srv = await websockets.serve(s.handler, "127.0.0.1", 0)
    port = srv.sockets[0].getsockname()[1]
    seen = []

    async def cb(m):
        seen.append(m)
        raise RuntimeError("boom")

    c = AlpacaWSClient(f"ws://127.0.0.1:{port}", "K", "S", cb)
    task = asyncio.create_task(c.run())
    try:
        c.subscribe(trades=["AAPL"])
        await until(lambda: len(seen) == 2)  # second msg still delivered after first raised
    finally:
        c.stop()
        await asyncio.wait_for(task, 5)
        srv.close()


# ------------------------------------------------------------------ review fixes
async def test_error_frames_reach_on_error_and_session_survives_soft_errors():
    errs = []

    async def handler(ws):
        await ws.send(json.dumps([{"T": "success", "msg": "connected"}]))
        async for raw in ws:
            m = json.loads(raw)
            if m["action"] == "auth":
                await ws.send(json.dumps([{"T": "success", "msg": "authenticated"}]))
            else:
                await ws.send(json.dumps([{"T": "error", "code": 405, "msg": "symbol limit exceeded"},
                                          {"T": "t", "S": "AAPL", "p": 1.0}]))

    srv = await websockets.serve(handler, "127.0.0.1", 0)
    got: list[dict] = []
    c = AlpacaWSClient(f"ws://127.0.0.1:{srv.sockets[0].getsockname()[1]}", "K", "S", got.append,
                       on_error=lambda code, msg: errs.append((code, msg)))
    task = asyncio.create_task(c.run())
    try:
        c.subscribe(trades=["AAPL"])
        await until(lambda: errs and got)           # the data frame after the error is still delivered
        assert errs == [(405, "symbol limit exceeded")] and c.connects == 1 and c.ready
    finally:
        c.stop()
        await asyncio.wait_for(task, 5)
        srv.close()


async def test_on_reconnect_only_after_reconnect_and_statuses_resubscribed_separately():
    s = Server()
    calls = []
    srv, c, got, task = await start(s, on_reconnect=lambda: calls.append(c.connects))
    try:
        c.subscribe(trades=["AAPL"], bars=["AAPL"], statuses=["AAPL"])
        await until(lambda: len(got) >= 2)
        assert calls == []
        await s.sockets[0].close()
        await until(lambda: calls and len(s.conns) == 2 and len(s.conns[1]) >= 3)
        assert calls == [2]
        assert s.conns[1][1] == {"action": "subscribe", "bars": ["AAPL"], "trades": ["AAPL"]}
        assert s.conns[1][2] == {"action": "subscribe", "statuses": ["AAPL"]}
    finally:
        c.stop()
        await asyncio.wait_for(task, 5)
        srv.close()
