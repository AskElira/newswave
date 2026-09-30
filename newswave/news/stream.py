"""Alpaca news websocket -> NewsEvent (CONTRACT §1)."""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Awaitable, Callable

from ..clock import Clock, RealClock, parse_iso, utc_iso
from ..config import Settings
from ..db import Database
from ..models import NewsEvent
from ..timeline import system_event
from ..wsclient import AlpacaWSClient

log = logging.getLogger("newswave.news")
NEWS_URL = "wss://stream.data.alpaca.markets/v1beta1/news"
# A quiet news tape is normal; a 60 s silence timeout would reconnect every minute and can lose stories in the
# gap. Liveness comes from websocket pings (ping_interval/ping_timeout); this only catches a wedged socket.
NEWS_STALE_AFTER_S = 1800


def _ts(s: Any) -> str:
    return utc_iso(parse_iso(str(s)))


def parse_news_message(msg: dict, received_at: datetime) -> NewsEvent:
    created = _ts(msg["created_at"])
    return NewsEvent(
        article_id=str(msg["id"]), received_at=utc_iso(received_at), created_at=created,
        updated_at=_ts(msg["updated_at"]) if msg.get("updated_at") else created,
        headline=msg.get("headline") or "", summary=msg.get("summary") or "",
        content=msg.get("content") or "",
        symbols=tuple(s for s in (msg.get("symbols") or []) if isinstance(s, str)),
        source=msg.get("source") or "", url=msg.get("url") or "")  # author deliberately ignored


class NewsStream:
    def __init__(self, settings: Settings, clock: Clock | None,
                 on_news: Callable[[NewsEvent], Awaitable[None]], url: str = NEWS_URL, *,
                 db: Database | None = None, stale_after_s: float = NEWS_STALE_AFTER_S) -> None:
        self.clock, self.on_news, self.db = clock or RealClock(), on_news, db
        self.client = AlpacaWSClient(url, settings.alpaca_api_key, settings.alpaca_secret_key,
                                     self._on_message, self.clock, stale_after_s=stale_after_s)
        self.client.subscribe(news=["*"])

    async def _on_message(self, msg: dict) -> None:
        if msg.get("T") != "n":
            log.info("news ws control/other frame: %s", {k: v for k, v in msg.items() if k != "key"})
            return
        received = self.clock.now()  # stamp before any parsing
        try:
            ev = parse_news_message(msg, received)
        except (KeyError, ValueError, TypeError) as e:
            log.exception("bad news message dropped")
            if self.db is not None:
                try:
                    system_event(self.db, self.clock, "ERROR", "news_stream", "MALFORMED_NEWS_FRAME",
                                 f"{type(e).__name__}: {e}"[:200], id=str(msg.get("id"))[:64],
                                 keys=sorted(str(k) for k in msg)[:20])
                except Exception:
                    log.exception("could not record malformed news frame")
            return
        await self.on_news(ev)  # errors are isolated by the ws client

    async def run(self) -> None:
        await self.client.run()

    def stop(self) -> None:
        self.client.stop()
