"""Generic Alpaca websocket client: auth, subscriptions that survive reconnects, backoff, watchdog.

Alpaca frames are JSON arrays of dicts, e.g. [{"T":"success","msg":"connected"}].
`on_message(dict)` (sync or async) receives every data frame; control frames
(success/error) are handled here and not forwarded.
"""
from __future__ import annotations

import asyncio
import inspect
import json
import logging
import random
import time
from collections import deque
from typing import Any, Awaitable, Callable

import websockets

from .clock import Clock, RealClock

log = logging.getLogger("newswave.ws")

# Alpaca error codes that must back off hard (never tight-loop): auth failed, connection limit, auth timeout.
HARD_ERROR_CODES = {401, 402, 404, 406}


class _Reconnect(Exception):
    def __init__(self, reason: str, hard: bool = False) -> None:
        super().__init__(reason)
        self.hard = hard


class AlpacaWSClient:
    def __init__(self, url: str, key: str, secret: str,
                 on_message: Callable[[dict], Awaitable[None] | None],
                 clock: Clock | None = None, *,
                 ping_interval: float = 20, stale_after_s: float = 60,
                 backoff_min_s: float = 1, backoff_max_s: float = 60,
                 stable_after_s: float = 60, hard_backoff_s: float = 60,
                 auth_timeout_s: float = 10,
                 on_error: Callable[[int, str], Awaitable[None] | None] | None = None,
                 on_reconnect: Callable[[], Awaitable[None] | None] | None = None) -> None:
        self.url, self._key, self._secret = url, key, secret
        self.on_message = on_message
        self.clock = clock or RealClock()
        self.ping_interval, self.stale_after_s = ping_interval, stale_after_s
        self.backoff_min_s, self.backoff_max_s = backoff_min_s, backoff_max_s
        self.stable_after_s, self.hard_backoff_s = stable_after_s, hard_backoff_s
        self.auth_timeout_s = auth_timeout_s
        self.on_error, self.on_reconnect = on_error, on_reconnect
        self._bg: set[asyncio.Task] = set()
        self.subscriptions: dict[str, set[str]] = {}
        self.ready = False          # connected AND authenticated
        self.connects = 0           # successful authentications so far
        self.last_message_at = None  # UTC datetime of last frame
        self._outbox: deque[dict] = deque()
        self._wake = asyncio.Event()
        self._stop = asyncio.Event()
        self._ws: Any = None

    # ---- public API -------------------------------------------------
    def subscribe(self, **channels: list[str]) -> None:
        self._change("subscribe", channels)

    def unsubscribe(self, **channels: list[str]) -> None:
        self._change("unsubscribe", channels)

    def stop(self) -> None:
        self._stop.set()
        self._wake.set()
        ws = self._ws
        if ws is not None:
            try:
                asyncio.get_running_loop().create_task(ws.close())
            except RuntimeError:
                pass

    async def run(self) -> None:
        attempt = 0
        while not self._stop.is_set():
            started = time.monotonic()
            hard = False
            try:
                await self._session()
            except _Reconnect as e:
                hard = e.hard
                log.warning("ws reconnect: %s", e)
            except (websockets.ConnectionClosed, OSError, asyncio.TimeoutError) as e:
                log.warning("ws dropped: %r", e)
            except Exception:
                log.exception("ws session crashed")
            finally:
                self.ready = False
                self._ws = None
            if self._stop.is_set():
                break
            if time.monotonic() - started >= self.stable_after_s:
                attempt = 0
            delay = min(self.backoff_max_s, self.backoff_min_s * 2 ** attempt)
            delay *= 0.5 + random.random() / 2  # jitter
            if hard:
                delay = max(delay, self.hard_backoff_s)
            attempt += 1
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=delay)
            except asyncio.TimeoutError:
                pass

    # ---- internals --------------------------------------------------
    def _change(self, action: str, channels: dict[str, list[str]]) -> None:
        clean = {k: sorted(set(v)) for k, v in channels.items() if v}
        for k, v in clean.items():
            cur = self.subscriptions.setdefault(k, set())
            if action == "subscribe":
                cur.update(v)
            else:
                cur.difference_update(v)
            if not cur:
                del self.subscriptions[k]
        if clean:
            self._outbox.append({"action": action, **clean})
            self._wake.set()

    @staticmethod
    def _frames(raw: str | bytes) -> list[dict]:
        try:
            data = json.loads(raw)
        except ValueError:
            log.warning("ws non-JSON frame dropped")
            return []
        items = data if isinstance(data, list) else [data]
        return [m for m in items if isinstance(m, dict)]

    async def _deliver(self, msg: dict) -> None:
        try:
            r = self.on_message(msg)
            if inspect.isawaitable(r):
                await r
        except Exception:
            log.exception("on_message failed")

    def _spawn(self, fn: Callable, *args: Any) -> None:
        """Run an optional user callback without ever blocking the receive loop or killing the session."""
        try:
            r = fn(*args)
            if inspect.isawaitable(r):
                async def _run() -> None:
                    try:
                        await r
                    except Exception:
                        log.exception("ws callback failed")
                t = asyncio.get_running_loop().create_task(_run())
                self._bg.add(t)
                t.add_done_callback(self._bg.discard)
        except Exception:
            log.exception("ws callback failed")

    def _control(self, msg: dict) -> str | None:
        """Returns 'authenticated' on auth success; raises on errors; None otherwise."""
        t = msg.get("T")
        if t == "success":
            return "authenticated" if msg.get("msg") == "authenticated" else None
        if t == "error":
            code = msg.get("code")
            log.error("alpaca ws error code=%s msg=%s", code, msg.get("msg"))
            if self.on_error is not None:
                self._spawn(self.on_error, code, str(msg.get("msg") or ""))
            if code in HARD_ERROR_CODES:
                raise _Reconnect(f"alpaca error {code}", hard=True)
        return None

    async def _session(self) -> None:
        async with websockets.connect(self.url, ping_interval=self.ping_interval,
                                      ping_timeout=self.ping_interval, open_timeout=15) as ws:
            self._ws = ws
            await self._authenticate(ws)
            self.ready = True
            self.connects += 1
            self._outbox.clear()  # full state below supersedes any queued commands
            if self.subscriptions:
                # `statuses` goes in its own request: a feed that rejects it (409/410) must not block trades/bars
                main = {k: sorted(v) for k, v in self.subscriptions.items() if k != "statuses"}
                if main:
                    await ws.send(json.dumps({"action": "subscribe", **main}))
                if "statuses" in self.subscriptions:
                    await ws.send(json.dumps({"action": "subscribe",
                                              "statuses": sorted(self.subscriptions["statuses"])}))
            if self.connects > 1 and self.on_reconnect is not None:
                self._spawn(self.on_reconnect)
            self._wake.clear()
            writer = asyncio.create_task(self._writer(ws))
            try:
                while not self._stop.is_set():
                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=self.stale_after_s)
                    except asyncio.TimeoutError:
                        raise _Reconnect(f"stale: no message for {self.stale_after_s}s") from None
                    self.last_message_at = self.clock.now()
                    for msg in self._frames(raw):
                        if msg.get("T") in ("success", "error"):
                            self._control(msg)
                        else:
                            await self._deliver(msg)
            finally:
                writer.cancel()

    async def _authenticate(self, ws: Any) -> None:
        await ws.send(json.dumps({"action": "auth", "key": self._key, "secret": self._secret}))
        deadline = time.monotonic() + self.auth_timeout_s
        while True:
            left = deadline - time.monotonic()
            if left <= 0:
                raise _Reconnect("auth timeout", hard=True)
            try:
                raw = await asyncio.wait_for(ws.recv(), timeout=left)
            except asyncio.TimeoutError:
                raise _Reconnect("auth timeout", hard=True) from None
            for msg in self._frames(raw):
                if self._control(msg) == "authenticated":
                    return

    async def _writer(self, ws: Any) -> None:
        while True:
            await self._wake.wait()
            self._wake.clear()
            while self._outbox:
                await ws.send(json.dumps(self._outbox.popleft()))
