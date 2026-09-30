"""IEX market-data websocket: trades + 1m bars (+ statuses if the plan allows). CONTRACT §8.

Message shapes (Alpaca market-data v2): trade {"T":"t","S","p","s","t"}, bar {"T":"b","S","o","h","l",
"c","v","t"}, status {"T":"s","S","sc","sm","rc","rm","t","z"}, confirmation {"T":"subscription",
"trades":[...],"bars":[...],"statuses":[...]}.
"""
from __future__ import annotations

import inspect
import logging
from typing import Any, Callable

from ..clock import Clock, parse_iso, utc_iso
from ..config import Settings
from ..models import Bar, Trade
from ..wsclient import AlpacaWSClient

log = logging.getLogger("newswave.market.stream")

IEX_URL = "wss://stream.data.alpaca.markets/v2/iex"
# Trading status codes (docs.alpaca.markets "Trading Status Messages & Codes"), per tape:
#   Tape A/B (CTA): 2 = halt, 3 = resume.  Tape C/O (UTP): H = halt, P = volatility pause, Q/T = resume.
CTA_HALT, CTA_RESUME = {"2"}, {"3"}
UTP_HALT, UTP_RESUME = {"H", "P"}, {"Q", "T"}
HALT_CODES = CTA_HALT | UTP_HALT
RESUME_CODES = CTA_RESUME | UTP_RESUME

# Trade conditions that do NOT update the last sale / high / low (docs.alpaca.markets market-data-faq table:
# "High/Low x, Open/Close x"): B W average price, C cash, G bunched sold, H price variation, I ODD LOT, M/Q/9
# official open/close/corrected close, N next day, P prior reference, R seller, V/7 contingent, 4 derivatively
# priced, Z out of sequence, U extended hours (sold out of sequence), T Form T (extended hours).
INELIGIBLE_CONDITIONS = frozenset("BWCGHIMQ9NPRV74ZUT")


def trade_eligible(m: dict) -> bool:
    """True when the print may update the last sale price. No condition flags = a regular print."""
    c = m.get("c")
    if c is None:
        return True
    codes = [c] if isinstance(c, str) else c
    return not any(str(x).strip().upper() in INELIGIBLE_CONDITIONS for x in codes)


def halt_state(code: str | None, tape: str | None = None) -> str | None:
    """'halt', 'resume' or None (informational code) for a status code, honouring the tape when known."""
    if code is None:
        return None
    if tape in ("A", "B"):
        halts, resumes = CTA_HALT, CTA_RESUME
    elif tape in ("C", "O"):
        halts, resumes = UTP_HALT, UTP_RESUME
    else:
        halts, resumes = HALT_CODES, RESUME_CODES
    return "halt" if code in halts else "resume" if code in resumes else None


def _iso(s: str) -> str:
    return utc_iso(parse_iso(s))


def parse_trade(m: dict) -> Trade | None:
    """None for malformed prints AND for prints not eligible to update the last sale (see INELIGIBLE_CONDITIONS)."""
    if not trade_eligible(m):
        return None
    try:
        return Trade(m["S"], _iso(m["t"]), float(m["p"]), float(m.get("s", 0)))
    except (KeyError, TypeError, ValueError):
        return None


def parse_bar(m: dict) -> Bar | None:
    try:
        return Bar(m["S"], _iso(m["t"]), float(m["o"]), float(m["h"]), float(m["l"]),
                   float(m["c"]), float(m["v"]), 1)
    except (KeyError, TypeError, ValueError):
        return None


def parse_status(m: dict) -> dict | None:
    try:
        code = m.get("sc")
        state = halt_state(code, m.get("z"))
        return {"symbol": m["S"], "ts": _iso(m["t"]), "status_code": code,
                "status_message": m.get("sm"), "reason_code": m.get("rc"),
                "reason_message": m.get("rm"), "halted": state == "halt", "resumed": state == "resume"}
    except (KeyError, TypeError, ValueError):
        return None


class MarketStream:
    """`subscribed` = symbols the server CONFIRMED (trades channel). Between a request and its confirmation it
    holds the optimistic target; any error frame or confirmation snaps it back to the server's word."""

    RETRY_AFTER_LIMIT_S = 30

    def __init__(self, settings: Settings, clock: Clock, on_trade: Callable[[Trade], Any],
                 on_bar_1m: Callable[[Bar], Any], on_status: Callable[[dict], Any],
                 url: str = IEX_URL, *, on_error: Callable[[int, str], Any] | None = None,
                 on_reconnect: Callable[[], Any] | None = None, **ws_kw: Any) -> None:
        self.on_trade, self.on_bar_1m, self.on_status = on_trade, on_bar_1m, on_status
        self.on_error, self.on_reconnect = on_error, on_reconnect
        self.clock = clock
        self.subscribed: set[str] = set()
        self.confirmed: dict[str, list[str]] = {}
        self.statuses_enabled = True
        self.dropped_trades = 0                 # prints filtered by condition (not eligible for last sale)
        self.dropped_by_condition: dict[str, int] = {}
        self._requested: set[str] = set()
        self._pending = 0                       # subscribe/unsubscribe requests not yet confirmed
        self._statuses_requested = False
        self._blocked_until = None
        self.client = AlpacaWSClient(url, settings.alpaca_api_key, settings.alpaca_secret_key,
                                     self._on_message, clock, on_error=self._on_ws_error,
                                     on_reconnect=self._on_ws_reconnect, **ws_kw)

    async def run(self) -> None:
        await self.client.run()

    def stop(self) -> None:
        self.client.stop()

    def apply(self, desired: set[str]) -> tuple[set[str], set[str]]:
        """Unsubscribe evicted symbols FIRST (never exceed the cap), then subscribe. `statuses` is its own request."""
        desired = set(desired)
        blocked = self._blocked_until is not None and self.clock.now() < self._blocked_until
        add = set() if blocked else desired - self._requested
        drop = self._requested - desired
        if drop:
            self.client.unsubscribe(trades=sorted(drop), bars=sorted(drop))
            self._pending += 1
            if self.statuses_enabled:
                self.client.unsubscribe(statuses=sorted(drop))
                self._pending += 1
        if add:
            self.client.subscribe(trades=sorted(add), bars=sorted(add))
            self._pending += 1
            if self.statuses_enabled:
                self._statuses_requested = True
                self.client.subscribe(statuses=sorted(add))
                self._pending += 1
        self._requested = (self._requested - drop) | add
        self.subscribed = set(self._requested)
        return add, drop

    async def _call(self, fn: Callable, *arg: Any) -> None:
        try:
            r = fn(*arg)
            if inspect.isawaitable(r):
                await r
        except Exception:
            log.exception("market callback failed")

    def _on_ws_error(self, code: int, msg: str) -> Any:
        if code not in (405, 409, 410):
            return None
        log.error("market stream subscription error %s: %s", code, msg)
        self._pending = 0
        if code in (409, 410) and self.statuses_enabled and self._statuses_requested:
            self._disable_statuses()
        if code == 405:  # symbol limit: back off retries, fall back to what the server really holds
            from datetime import timedelta
            self._blocked_until = self.clock.now() + timedelta(seconds=self.RETRY_AFTER_LIMIT_S)
        self._requested = set(self.confirmed.get("trades", ()))
        self.subscribed = set(self._requested)
        for ch in list(self.client.subscriptions):  # a reconnect must not replay the rejected symbols
            cur = self.client.subscriptions[ch]
            cur &= self._requested
            if not cur:
                del self.client.subscriptions[ch]
        if self.on_error is not None:
            return self._call(self.on_error, code, msg)
        return None

    def _on_ws_reconnect(self) -> Any:
        subs = self.client.subscriptions  # the client just resubscribed: one request (+ one for statuses)
        self._pending = int(any(k != "statuses" for k in subs)) + int("statuses" in subs)
        if self.on_reconnect is not None:
            return self._call(self.on_reconnect)
        return None

    def _disable_statuses(self) -> None:
        log.warning("server did not accept the statuses channel; halts will not be streamed")
        self.statuses_enabled = False
        stale = sorted(self.client.subscriptions.get("statuses", ()))
        if stale:
            self.client.unsubscribe(statuses=stale)

    def _handle_confirmation(self, m: dict) -> None:
        self.confirmed = {k: list(m.get(k) or []) for k in ("trades", "bars", "statuses")}
        self._pending = max(0, self._pending - 1)
        if self._pending:
            return  # more requests in flight: the last confirmation carries the final state
        self._requested = set(self.confirmed["trades"])
        self.subscribed = set(self._requested)
        if (self.statuses_enabled and self._statuses_requested and self.confirmed["trades"]
                and not self.confirmed["statuses"]):
            self._disable_statuses()

    async def _on_message(self, m: dict) -> None:
        t = m.get("T")
        if t == "t":
            if (x := parse_trade(m)) is not None:
                await self._call(self.on_trade, x)
            elif not trade_eligible(m):
                self.dropped_trades += 1
                for c in ([m["c"]] if isinstance(m["c"], str) else m["c"]):
                    if str(c).strip().upper() in INELIGIBLE_CONDITIONS:
                        self.dropped_by_condition[str(c)] = self.dropped_by_condition.get(str(c), 0) + 1
        elif t == "b":
            if (x := parse_bar(m)) is not None:
                await self._call(self.on_bar_1m, x)
        elif t == "s":
            if (s := parse_status(m)) is not None:
                await self._call(self.on_status, s)
        elif t == "subscription":
            self._handle_confirmation(m)
