"""Historical IEX data (REST) + pure baseline / warm-up helpers (CONTRACT §8)."""
from __future__ import annotations

import logging
import time
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Any, Callable

from alpaca.data.enums import DataFeed
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest, StockLatestTradeRequest
from alpaca.data.timeframe import TimeFrame, TimeFrameUnit

from ..clock import CLOSE_T, OPEN_T, Clock, RealClock, parse_iso, to_et, utc_iso
from ..models import Bar, Trade

log = logging.getLogger("newswave.baseline")
MAX_TRIES = 4


def _status(e: Exception) -> int | None:
    s = getattr(e, "status_code", None)
    if s is None:
        s = getattr(getattr(e, "response", None), "status_code", None)
    return s if isinstance(s, int) else None


def _is_permission(e: Exception) -> bool:
    msg = str(e).lower()
    return _status(e) == 403 or "subscription" in msg or "not permit" in msg


def _transient(e: Exception) -> bool:
    s = _status(e)
    return s == 429 or (s is not None and s >= 500) or (s is None and isinstance(e, (OSError, TimeoutError)))


def _bar(symbol: str, b: Any, minutes: int) -> Bar:
    return Bar(symbol, utc_iso(b.timestamp), float(b.open), float(b.high), float(b.low),
               float(b.close), float(b.volume), minutes)


class HistoricalData:
    def __init__(self, key: str, secret: str, client: Any = None, clock: Clock | None = None,
                 sleep: Callable[[float], None] = time.sleep, backoff_s: float = 1.0) -> None:
        self.client = client or StockHistoricalDataClient(key, secret)
        self.clock, self._sleep, self._backoff = clock or RealClock(), sleep, backoff_s

    def _retry(self, fn: Callable[[], Any]) -> Any:
        for n in range(MAX_TRIES):
            try:
                return fn()
            except Exception as e:
                if not _transient(e) or n == MAX_TRIES - 1:
                    raise
                delay = self._backoff * 2 ** n
                log.warning("historical data retry %d in %.1fs: %s", n + 1, delay, _status(e) or type(e).__name__)
                self._sleep(delay)

    def _bars(self, symbol: str, tf: TimeFrame, minutes: int, start: datetime, end: datetime | None,
              feed: str) -> list[Bar]:
        req = StockBarsRequest(symbol_or_symbols=symbol, timeframe=tf, start=start, end=end,
                               feed=DataFeed(feed))
        resp = self._retry(lambda: self.client.get_stock_bars(req))
        return [_bar(symbol, b, minutes) for b in resp.data.get(symbol, [])]

    def bars_5m(self, symbol: str, start: datetime, end: datetime | None = None,
                feed: str = "iex") -> list[Bar]:
        return self._bars(symbol, TimeFrame(5, TimeFrameUnit.Minute), 5, start, end, feed)

    def bars_1m(self, symbol: str, start: datetime, end: datetime | None = None,
                feed: str = "iex") -> list[Bar]:
        return self._bars(symbol, TimeFrame(1, TimeFrameUnit.Minute), 1, start, end, feed)

    def daily_bars(self, symbol: str, days: int = 20, feed: str = "sip") -> tuple[list[Bar], str]:
        """Last `days` daily bars and the feed actually used (sip falls back to iex on a permission error)."""
        now = self.clock.now()
        start = now - timedelta(days=days * 2 + 6)
        tf = TimeFrame(1, TimeFrameUnit.Day)
        if feed == "sip":
            try:  # free plans may not query SIP data newer than 15 min
                return self._bars(symbol, tf, 1440, start, now - timedelta(minutes=16), "sip")[-days:], "sip"
            except Exception as e:
                if not _is_permission(e):
                    raise
                log.warning("sip daily bars not permitted, falling back to iex")
        return self._bars(symbol, tf, 1440, start, None, "iex")[-days:], "iex"

    def latest_trade(self, symbol: str, feed: str = "iex") -> Trade | None:
        req = StockLatestTradeRequest(symbol_or_symbols=symbol, feed=DataFeed(feed))
        t = self._retry(lambda: self.client.get_stock_latest_trade(req)).get(symbol)
        return None if t is None else Trade(symbol, utc_iso(t.timestamp), float(t.price), float(t.size))


def _regular(bars: list[Bar]) -> dict:
    """{ET date: {HH:MM slot: volume}} for regular-session bars only."""
    out: dict = defaultdict(dict)
    for b in bars:
        e = to_et(parse_iso(b.start))
        if OPEN_T <= e.time() < CLOSE_T and e.weekday() < 5:
            out[e.date()][e.strftime("%H:%M")] = b.volume
    return out


def build_baseline(bars_5m: list[Bar], days: int = 20, min_days: int = 5) -> dict[str, float] | None:
    by_day = _regular(bars_5m)
    if len(by_day) < min_days:
        return None
    used = sorted(by_day)[-days:]
    acc: dict[str, float] = defaultdict(float)
    for d in used:
        for slot, v in by_day[d].items():
            acc[slot] += v
    # a session present in the data with no bar in a slot traded 0 shares there: divide by ALL sessions,
    # otherwise thin symbols (IEX prints rarely) get an inflated baseline that hides real RVOL
    return {slot: v / len(used) for slot, v in sorted(acc.items())}


def warmup_bars(bars_5m: list[Bar], sessions: int = 3) -> list[Bar]:
    keep = set(sorted(_regular(bars_5m))[-sessions:])
    return sorted((b for b in bars_5m
                   if (e := to_et(parse_iso(b.start))).date() in keep and OPEN_T <= e.time() < CLOSE_T),
                  key=lambda b: b.start)
