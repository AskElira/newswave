"""Broker protocol + AlpacaPaperBroker (the ONLY TradingClient in the codebase) + SimBroker.

No alpaca object leaves this module: everything is converted to BrokerOrder/BrokerPosition/
BrokerAccount/BrokerAsset. Order side is the plain string "buy" | "sell".

Submit safety (SPEC 31): a timeout / connection error / 5xx / unparseable reply on submit is NEVER
treated as "failed". The order is looked up by client_order_id first; it is re-submitted only when
the broker positively says it does not exist (and client_order_id uniqueness makes even that safe).
"""
from __future__ import annotations

import asyncio
import logging
from dataclasses import asdict, dataclass, field
from datetime import UTC, date, datetime, timedelta
from typing import Any, Awaitable, Callable, Protocol

from ..clock import ET, Clock, parse_iso, session_bounds, utc_iso
from ..models import Side, Trade

log = logging.getLogger("newswave.broker")

OPEN_STATUSES = frozenset({"new", "accepted", "pending_new", "partially_filled", "pending_cancel",
                           "pending_replace", "held", "accepted_for_bidding", "pending_review",
                           "calculated", "stopped", "suspended"})
TERMINAL_STATUSES = frozenset({"filled", "canceled", "expired", "rejected", "replaced", "done_for_day"})


def is_terminal(status: str) -> bool:
    return status in TERMINAL_STATUSES


# ---------------------------------------------------------------- plain data
@dataclass
class BrokerOrder:
    client_order_id: str
    broker_order_id: str
    status: str  # lower-case Alpaca status string
    qty: int
    filled_qty: int = 0
    filled_avg_price: float | None = None
    submitted_at: str | None = None  # ISO UTC 'Z'
    filled_at: str | None = None
    raw: dict = field(default_factory=dict)
    symbol: str = ""
    side: str = ""  # "buy" | "sell"
    order_type: str = ""  # market | limit | stop
    limit_price: float | None = None
    stop_price: float | None = None


@dataclass
class BrokerPosition:
    symbol: str
    qty: int  # absolute
    side: Side
    avg_entry_price: float
    unrealized_pl: float = 0.0


@dataclass
class BrokerAccount:
    equity: float
    buying_power: float
    daytrade_count: int = 0
    pattern_day_trader: bool = False
    cash: float = 0.0
    trading_blocked: bool = False


@dataclass
class BrokerAsset:
    symbol: str
    name: str = ""
    exchange: str = ""
    asset_class: str = "us_equity"
    tradable: bool = True
    shortable: bool = True
    easy_to_borrow: bool = True
    status: str = "active"


@dataclass
class BrokerCalendar:
    """One trading day: session open/close as UTC datetimes (early closes included)."""
    date: date
    open: datetime
    close: datetime


@dataclass
class BrokerClock:
    timestamp: datetime
    is_open: bool
    next_open: datetime
    next_close: datetime


class BrokerError(Exception):
    def __init__(self, message: str, status_code: int | None = None) -> None:
        super().__init__(message)
        self.status_code = status_code


class OrderRejected(BrokerError):
    """Definite: the broker refused the request (4xx). Nothing was placed."""


class NotFound(BrokerError):
    """Definite 404."""


class BrokerUnavailable(BrokerError):
    """Transient failures exhausted. For submits this is only raised when lookups proved nothing was placed."""


class OrderOutcomeUnknown(BrokerError):
    """A submit whose outcome could not be established either way. Caller must NOT assume failure."""


class Broker(Protocol):
    kind: str  # "alpaca" | "sim"

    async def get_account(self) -> BrokerAccount: ...
    async def get_positions(self) -> list[BrokerPosition]: ...
    async def get_open_orders(self) -> list[BrokerOrder]: ...
    async def get_order_by_client_id(self, client_order_id: str) -> BrokerOrder | None: ...
    async def submit_limit(self, symbol: str, side: str, qty: int, limit_price: float,
                           client_order_id: str) -> BrokerOrder: ...
    async def submit_market(self, symbol: str, side: str, qty: int, client_order_id: str) -> BrokerOrder: ...
    async def submit_stop(self, symbol: str, side: str, qty: int, stop_price: float,
                          client_order_id: str) -> BrokerOrder: ...
    async def replace_order_qty(self, broker_order_id: str, qty: int,
                                client_order_id: str | None = None) -> BrokerOrder: ...
    async def cancel_order(self, broker_order_id: str) -> None: ...
    async def close_position(self, symbol: str) -> BrokerOrder | None: ...
    async def get_asset(self, symbol: str) -> BrokerAsset: ...
    async def get_all_assets(self) -> list[BrokerAsset]: ...
    async def latest_price(self, symbol: str) -> float | None: ...
    async def get_calendar(self, d: date) -> BrokerCalendar | None: ...
    async def get_clock(self) -> BrokerClock: ...


# ---------------------------------------------------------------- alpaca conversion
def _v(x: Any) -> Any:
    return getattr(x, "value", x)


def _f(x: Any) -> float | None:
    return None if x is None or x == "" else float(x)


def _iso(dt: datetime | None) -> str | None:
    if dt is None:
        return None
    if dt.tzinfo is None:
        from datetime import UTC
        dt = dt.replace(tzinfo=UTC)
    return utc_iso(dt)


def order_from_alpaca(o: Any) -> BrokerOrder:
    raw = o.model_dump(mode="json") if hasattr(o, "model_dump") else {}
    typ = _v(getattr(o, "order_type", None)) or _v(getattr(o, "type", None)) or ""
    return BrokerOrder(
        client_order_id=str(o.client_order_id), broker_order_id=str(o.id), status=str(_v(o.status)).lower(),
        qty=int(_f(o.qty) or 0), filled_qty=int(_f(o.filled_qty) or 0),
        filled_avg_price=_f(o.filled_avg_price), submitted_at=_iso(o.submitted_at),
        filled_at=_iso(getattr(o, "filled_at", None)), raw=raw, symbol=str(o.symbol or ""),
        side=str(_v(o.side) or "").lower(), order_type=str(typ).lower(),
        limit_price=_f(getattr(o, "limit_price", None)), stop_price=_f(getattr(o, "stop_price", None)))


def position_from_alpaca(p: Any) -> BrokerPosition:
    q = _f(p.qty) or 0.0
    side = Side.SHORT if (str(_v(p.side)).lower() == "short" or q < 0) else Side.LONG
    return BrokerPosition(symbol=str(p.symbol), qty=int(abs(q)), side=side,
                          avg_entry_price=_f(p.avg_entry_price) or 0.0,
                          unrealized_pl=_f(getattr(p, "unrealized_pl", None)) or 0.0)


def account_from_alpaca(a: Any) -> BrokerAccount:
    return BrokerAccount(equity=_f(a.equity) or 0.0, buying_power=_f(a.buying_power) or 0.0,
                         daytrade_count=int(getattr(a, "daytrade_count", 0) or 0),
                         pattern_day_trader=bool(getattr(a, "pattern_day_trader", False)),
                         cash=_f(getattr(a, "cash", None)) or 0.0,
                         trading_blocked=bool(getattr(a, "trading_blocked", False)))


def asset_from_alpaca(a: Any) -> BrokerAsset:
    return BrokerAsset(symbol=str(a.symbol), name=str(getattr(a, "name", "") or ""),
                       exchange=str(_v(a.exchange)), asset_class=str(_v(a.asset_class)),
                       tradable=bool(a.tradable), shortable=bool(a.shortable),
                       easy_to_borrow=bool(a.easy_to_borrow), status=str(_v(a.status)).lower())


def _classify(e: BaseException) -> str:
    """'reject' (definite 4xx) | 'notfound' | 'transient' (429/5xx/timeout/connection/unparseable)."""
    from alpaca.common.exceptions import APIError
    if isinstance(e, APIError):
        sc = e.status_code
        if sc == 404:
            return "notfound"
        if sc is None or sc == 429 or sc >= 500:
            return "transient"
        return "reject"
    return "transient"  # TimeoutError, OSError/requests errors, pydantic errors on a reply, ...


def _status(e: BaseException) -> int | None:
    return getattr(e, "status_code", None)


def _same_order(o: BrokerOrder, req: Any) -> bool:
    return (o.symbol.upper() == str(req.symbol).upper() and o.side == str(_v(req.side)).lower()
            and o.qty == int(req.qty))


def _is_dup(e: BaseException) -> bool:
    return _status(e) == 422 and "client_order_id" in str(e).lower()


class AlpacaPaperBroker:
    kind = "alpaca"

    def __init__(self, key: str, secret: str, *, client: Any = None, timeout_s: float = 15.0,
                 max_attempts: int = 4, backoff_s: float = 0.5,
                 sleep: Callable[[float], Awaitable[None]] | None = None) -> None:
        if client is None:
            from alpaca.trading.client import TradingClient
            client = TradingClient(key, secret, paper=True)
        self._c = client
        self.timeout_s, self.max_attempts, self.backoff_s = timeout_s, max_attempts, backoff_s
        self._sleep = sleep or asyncio.sleep

    # -- plumbing
    async def _call(self, fn: Callable, *args: Any) -> Any:
        return await asyncio.wait_for(asyncio.to_thread(fn, *args), self.timeout_s)

    async def _retry(self, fn: Callable, *args: Any) -> Any:
        last: BaseException | None = None
        for i in range(self.max_attempts):
            try:
                return await self._call(fn, *args)
            except Exception as e:  # noqa: BLE001
                kind = _classify(e)
                if kind == "notfound":
                    raise NotFound(str(e), 404) from e
                if kind == "reject":
                    raise OrderRejected(str(e), _status(e)) from e
                last = e
                log.warning("broker transient error (attempt %d): %s", i + 1, type(e).__name__)
                if i + 1 < self.max_attempts:
                    await self._sleep(self.backoff_s * 2 ** i)
        raise BrokerUnavailable(f"{type(last).__name__}: {last}") from last

    async def _lookup(self, cid: str) -> tuple[str, BrokerOrder | None]:
        """('found', order) | ('absent', None) | ('unknown', None)."""
        try:
            o = await self._retry(self._c.get_order_by_client_id, cid)
            return "found", order_from_alpaca(o)
        except NotFound:
            return "absent", None
        except BrokerError:
            return "unknown", None

    async def _submit(self, req: Any, cid: str) -> BrokerOrder:
        for i in range(self.max_attempts):
            try:
                return order_from_alpaca(await self._call(self._c.submit_order, req))
            except Exception as e:  # noqa: BLE001
                kind = _classify(e)
                if kind == "reject" and not _is_dup(e):
                    raise OrderRejected(str(e), _status(e)) from e
                # ambiguous (or duplicate id): ALWAYS look it up before any retry, and never re-submit
                # while the lookup itself is failing
                state, o = await self._lookup(cid)
                log.warning("submit %s ambiguous (%s); lookup=%s", cid, type(e).__name__, state)
                if state == "found":
                    if not _same_order(o, req):  # an id clash (DB wipe, bug) must never be adopted as ours
                        raise OrderRejected(
                            f"client_order_id {cid} already belongs to a different order ({o.symbol} {o.side} "  # type: ignore[union-attr]
                            f"{o.qty}, not {req.symbol} {_v(req.side)} {req.qty}); refusing to adopt it", 422)  # type: ignore[union-attr]
                    return o  # type: ignore[return-value]
                if state == "unknown":
                    raise OrderOutcomeUnknown(f"could not establish outcome of {cid}") from e
                if kind == "reject":  # 'duplicate' yet absent: contradictory, refuse
                    raise OrderRejected(str(e), _status(e)) from e
                if i + 1 < self.max_attempts:  # broker positively has no such order: safe to re-send
                    await self._sleep(self.backoff_s * 2 ** i)
        raise BrokerUnavailable(f"submit {cid} failed; lookups show nothing was placed")

    # -- protocol
    async def get_account(self) -> BrokerAccount:
        return account_from_alpaca(await self._retry(self._c.get_account))

    async def get_positions(self) -> list[BrokerPosition]:
        return [position_from_alpaca(p) for p in await self._retry(self._c.get_all_positions)]

    async def get_open_orders(self) -> list[BrokerOrder]:
        from alpaca.trading.enums import QueryOrderStatus
        from alpaca.trading.requests import GetOrdersRequest
        req = GetOrdersRequest(status=QueryOrderStatus.OPEN, limit=500, nested=False)
        return [order_from_alpaca(o) for o in await self._retry(self._c.get_orders, req)]

    async def get_order_by_client_id(self, client_order_id: str) -> BrokerOrder | None:
        try:
            return order_from_alpaca(await self._retry(self._c.get_order_by_client_id, client_order_id))
        except NotFound:
            return None

    async def submit_limit(self, symbol: str, side: str, qty: int, limit_price: float,
                           client_order_id: str) -> BrokerOrder:
        from alpaca.trading.requests import LimitOrderRequest
        req = LimitOrderRequest(symbol=symbol, qty=_whole(qty), side=_side(side), time_in_force=_tif(),
                                limit_price=round(limit_price, 2), client_order_id=client_order_id,
                                extended_hours=False)
        return await self._submit(req, client_order_id)

    async def submit_market(self, symbol: str, side: str, qty: int, client_order_id: str) -> BrokerOrder:
        from alpaca.trading.requests import MarketOrderRequest
        req = MarketOrderRequest(symbol=symbol, qty=_whole(qty), side=_side(side), time_in_force=_tif(),
                                 client_order_id=client_order_id, extended_hours=False)
        return await self._submit(req, client_order_id)

    async def submit_stop(self, symbol: str, side: str, qty: int, stop_price: float,
                          client_order_id: str) -> BrokerOrder:
        from alpaca.trading.requests import StopOrderRequest
        req = StopOrderRequest(symbol=symbol, qty=_whole(qty), side=_side(side), time_in_force=_tif(),
                               stop_price=round(stop_price, 2), client_order_id=client_order_id,
                               extended_hours=False)
        return await self._submit(req, client_order_id)

    async def replace_order_qty(self, broker_order_id: str, qty: int,
                                client_order_id: str | None = None) -> BrokerOrder:
        from alpaca.trading.requests import ReplaceOrderRequest
        req = ReplaceOrderRequest(qty=_whole(qty), client_order_id=client_order_id)
        return order_from_alpaca(await self._retry(self._c.replace_order_by_id, broker_order_id, req))

    async def cancel_order(self, broker_order_id: str) -> None:
        await self._retry(self._c.cancel_order_by_id, broker_order_id)

    async def close_position(self, symbol: str) -> BrokerOrder | None:
        try:
            return order_from_alpaca(await self._retry(self._c.close_position, symbol))
        except NotFound:
            return None

    async def get_asset(self, symbol: str) -> BrokerAsset:
        return asset_from_alpaca(await self._retry(self._c.get_asset, symbol))

    async def get_all_assets(self) -> list[BrokerAsset]:
        from alpaca.trading.enums import AssetClass, AssetStatus
        from alpaca.trading.requests import GetAssetsRequest
        req = GetAssetsRequest(status=AssetStatus.ACTIVE, asset_class=AssetClass.US_EQUITY)
        return [asset_from_alpaca(a) for a in await self._retry(self._c.get_all_assets, req)]

    async def latest_price(self, symbol: str) -> float | None:
        return None  # the trading API has no quotes; market data comes from the IEX stream

    async def get_calendar(self, d: date) -> BrokerCalendar | None:
        from alpaca.trading.requests import GetCalendarRequest
        rows = await self._retry(self._c.get_calendar, GetCalendarRequest(start=d, end=d))
        for r in rows:  # alpaca returns naive ET wall times
            if r.date == d:
                return BrokerCalendar(d, r.open.replace(tzinfo=ET).astimezone(UTC),
                                      r.close.replace(tzinfo=ET).astimezone(UTC))
        return None  # weekend / holiday

    async def get_clock(self) -> BrokerClock:
        c = await self._retry(self._c.get_clock)
        def aware(x: datetime) -> datetime:
            return (x if x.tzinfo else x.replace(tzinfo=ET)).astimezone(UTC)
        return BrokerClock(aware(c.timestamp), bool(c.is_open), aware(c.next_open), aware(c.next_close))


def _whole(qty: int) -> int:
    if int(qty) != qty or qty <= 0:
        raise ValueError(f"whole positive share qty required, got {qty!r}")
    return int(qty)


def _side(side: str) -> Any:
    from alpaca.trading.enums import OrderSide
    return {"buy": OrderSide.BUY, "sell": OrderSide.SELL}[side]


def _tif() -> Any:
    from alpaca.trading.enums import TimeInForce
    return TimeInForce.DAY


# ---------------------------------------------------------------- SimBroker
@dataclass
class _SimOrder:
    cid: str
    bid: str
    symbol: str
    side: str
    type: str
    qty: int
    limit: float | None
    stop: float | None
    submitted_at: str
    status: str = "new"
    filled_qty: int = 0
    filled_avg: float | None = None
    filled_at: str | None = None
    settle_left: int = 0  # lookups until a pending_cancel / pending_replace becomes terminal


class _Seq:
    """Order-id counter with a readable / restorable state (itertools.count can't be copied in 3.14)."""

    def __init__(self, last: int = 0) -> None:
        self.last = last

    def __next__(self) -> int:
        self.last += 1
        return self.last


class SimBroker:
    """In-memory broker (OBSERVE mode, replay, tests). 1x buying power, TIF day, whole shares.

    Fills: marketable limit / market fill at the last print if one exists, else at the next print;
    resting limit fills at the first print through the limit; stops fill when a print crosses them,
    AT THE PRINT PRICE (gap-through realism). Like Alpaca, an order that reduces a position while
    other open orders already reserve those shares is refused (403): exits must resize/cancel the
    stop first.
    """
    kind = "sim"

    def __init__(self, clock: Clock, starting_equity: float = 6000.0) -> None:
        self.clock = clock
        self.cash = float(starting_equity)
        self.partial_cap: int | None = None  # test knob: max shares a LIMIT order fills per print
        self.settle_lag = 0  # test knob: cancel/replace stay pending_* for this many lookups (Alpaca is async)
        self.assets: dict[str, BrokerAsset] = {}
        self._orders: dict[str, _SimOrder] = {}
        self._by_bid: dict[str, _SimOrder] = {}
        self._pos: dict[str, list[float]] = {}  # symbol -> [signed qty, avg price]
        self._last: dict[str, float] = {}
        self._n = _Seq()
        self.on_change: Callable[[], None] | None = None  # daemon persists state here (OBSERVE restart safety)

    # -- persistence (positions + orders survive a process restart)
    def _changed(self) -> None:
        if self.on_change is not None:
            self.on_change()

    def snapshot(self, keep_terminal: int = 500) -> dict:
        orders = list(self._orders.values())
        terminal = [o for o in orders if o.status not in OPEN_STATUSES][-keep_terminal:]
        keep = {id(o) for o in terminal} | {id(o) for o in orders if o.status in OPEN_STATUSES}
        return {"cash": self.cash, "n": self._n.last, "last": dict(self._last),
                "pos": {s: list(v) for s, v in self._pos.items()},
                "orders": [asdict(o) for o in orders if id(o) in keep]}

    def restore(self, state: dict) -> None:
        self.cash = float(state["cash"])
        self._last = {k: float(v) for k, v in state.get("last", {}).items()}
        self._pos = {k: [float(v[0]), float(v[1])] for k, v in state.get("pos", {}).items()}
        self._orders = {d["cid"]: _SimOrder(**d) for d in state.get("orders", [])}
        self._by_bid = {o.bid: o for o in self._orders.values()}
        self._n = _Seq(int(state.get("n", 0)))

    # -- feed
    async def on_trade(self, t: Trade) -> None:
        self._last[t.symbol] = t.price
        for o in [o for o in self._orders.values() if o.symbol == t.symbol and o.status in OPEN_STATUSES]:
            self._maybe_fill(o, t.price, t.ts)

    def _maybe_fill(self, o: _SimOrder, px: float, ts: str) -> None:
        if o.status not in OPEN_STATUSES:
            return
        buy = o.side == "buy"
        hit = (o.type == "market"
               or (o.type == "limit" and (px <= o.limit if buy else px >= o.limit))  # type: ignore[operator]
               or (o.type == "stop" and (px >= o.stop if buy else px <= o.stop)))  # type: ignore[operator]
        if not hit:
            return
        q = o.qty - o.filled_qty
        if self.partial_cap is not None and o.type == "limit":
            q = min(q, self.partial_cap)
        prev = o.filled_qty * (o.filled_avg or 0.0)
        o.filled_qty += q
        o.filled_avg = (prev + q * px) / o.filled_qty
        o.filled_at = ts
        o.status = "filled" if o.filled_qty == o.qty else "partially_filled"
        signed = q if buy else -q
        self.cash -= signed * px
        pos = self._pos.setdefault(o.symbol, [0.0, 0.0])
        new = pos[0] + signed
        if pos[0] == 0 or (pos[0] > 0) == (signed > 0):
            pos[1] = (abs(pos[0]) * pos[1] + q * px) / abs(new) if new else 0.0
        elif abs(signed) > abs(pos[0]):  # flipped through zero
            pos[1] = px
        pos[0] = new
        if new == 0:
            del self._pos[o.symbol]
        self._changed()

    # -- helpers
    def _snap(self, o: _SimOrder) -> BrokerOrder:
        raw = {"id": o.bid, "client_order_id": o.cid, "status": o.status, "qty": o.qty,
               "filled_qty": o.filled_qty, "symbol": o.symbol, "side": o.side, "type": o.type}
        return BrokerOrder(o.cid, o.bid, o.status, o.qty, o.filled_qty, o.filled_avg, o.submitted_at,
                           o.filled_at, raw, o.symbol, o.side, o.type, o.limit, o.stop)

    def _now(self) -> str:
        return utc_iso(self.clock.now())

    def _check_available(self, symbol: str, side: str, qty: int, ignore: _SimOrder | None = None) -> None:
        held = self._pos.get(symbol, [0.0, 0.0])[0]
        reduces = (held > 0 and side == "sell") or (held < 0 and side == "buy")
        if not reduces:
            return
        reserved = sum(o.qty - o.filled_qty for o in self._orders.values()
                       if o is not ignore and o.symbol == symbol and o.side == side and o.status in OPEN_STATUSES)
        if qty > abs(held) - reserved:
            raise OrderRejected(f"insufficient qty available for order (requested: {qty}, "
                                f"available: {abs(held) - reserved:g})", 403)

    def _new(self, symbol: str, side: str, typ: str, qty: int, limit: float | None, stop: float | None,
             cid: str) -> BrokerOrder:
        if int(qty) != qty or qty <= 0:
            raise OrderRejected("qty must be a whole positive number", 422)
        if side not in ("buy", "sell"):
            raise OrderRejected("bad side", 422)
        if cid in self._orders:
            raise OrderRejected("client_order_id must be unique", 422)
        self._check_available(symbol, side, qty)
        o = _SimOrder(cid, f"sim-{next(self._n)}", symbol, side, typ, int(qty), limit, stop, self._now())
        self._orders[cid] = o
        self._by_bid[o.bid] = o
        if typ != "stop" and symbol in self._last:  # marketable / market: fill at the last print now
            self._maybe_fill(o, self._last[symbol], self._now())
        self._changed()
        return self._snap(o)

    # -- protocol
    async def get_account(self) -> BrokerAccount:
        mv = sum(q * self._last.get(s, avg) for s, (q, avg) in self._pos.items())
        gross = sum(abs(q) * self._last.get(s, avg) for s, (q, avg) in self._pos.items())
        equity = self.cash + mv
        return BrokerAccount(equity=equity, buying_power=max(0.0, equity - gross), daytrade_count=0,
                             pattern_day_trader=False, cash=self.cash)

    async def get_positions(self) -> list[BrokerPosition]:
        return [BrokerPosition(s, int(abs(q)), Side.LONG if q > 0 else Side.SHORT, avg,
                               (self._last.get(s, avg) - avg) * q) for s, (q, avg) in self._pos.items()]

    async def get_open_orders(self) -> list[BrokerOrder]:
        return [self._snap(o) for o in self._orders.values() if o.status in OPEN_STATUSES]

    async def get_order_by_client_id(self, client_order_id: str) -> BrokerOrder | None:
        o = self._orders.get(client_order_id)
        if o and o.status in ("pending_cancel", "pending_replace"):
            o.settle_left -= 1
            if o.settle_left <= 0:
                o.status = "canceled" if o.status == "pending_cancel" else "replaced"
                self._changed()
        return self._snap(o) if o else None

    async def submit_limit(self, symbol: str, side: str, qty: int, limit_price: float,
                           client_order_id: str) -> BrokerOrder:
        return self._new(symbol, side, "limit", qty, limit_price, None, client_order_id)

    async def submit_market(self, symbol: str, side: str, qty: int, client_order_id: str) -> BrokerOrder:
        return self._new(symbol, side, "market", qty, None, None, client_order_id)

    async def submit_stop(self, symbol: str, side: str, qty: int, stop_price: float,
                          client_order_id: str) -> BrokerOrder:
        return self._new(symbol, side, "stop", qty, None, stop_price, client_order_id)

    async def replace_order_qty(self, broker_order_id: str, qty: int,
                                client_order_id: str | None = None) -> BrokerOrder:
        old = self._by_bid.get(broker_order_id)
        if old is None:
            raise NotFound("order not found", 404)
        if old.status not in OPEN_STATUSES:
            raise OrderRejected("order is not replaceable", 422)
        if qty <= old.filled_qty or int(qty) != qty:
            raise OrderRejected("qty must exceed filled qty and be whole", 422)
        cid = client_order_id or f"sim-auto-{next(self._n)}"
        if cid in self._orders:
            raise OrderRejected("client_order_id must be unique", 422)
        self._check_available(old.symbol, old.side, int(qty), ignore=old)
        old.status, old.settle_left = ("pending_replace", self.settle_lag) if self.settle_lag else ("replaced", 0)
        new = _SimOrder(cid, f"sim-{next(self._n)}", old.symbol, old.side, old.type, int(qty), old.limit,
                        old.stop, self._now(), filled_qty=old.filled_qty, filled_avg=old.filled_avg)
        self._orders[cid] = new
        self._by_bid[new.bid] = new
        self._changed()
        return self._snap(new)

    async def cancel_order(self, broker_order_id: str) -> None:
        o = self._by_bid.get(broker_order_id)
        if o is None:
            raise NotFound("order not found", 404)
        if o.status not in OPEN_STATUSES:
            raise OrderRejected("order is not cancelable", 422)
        o.status, o.settle_left = ("pending_cancel", self.settle_lag) if self.settle_lag else ("canceled", 0)
        self._changed()

    async def close_position(self, symbol: str) -> BrokerOrder | None:
        pos = self._pos.get(symbol)
        if not pos:
            return None
        for o in list(self._orders.values()):
            if o.symbol == symbol and o.status in OPEN_STATUSES:
                o.status = "canceled"
        side = "sell" if pos[0] > 0 else "buy"
        return self._new(symbol, side, "market", int(abs(pos[0])), None, None, f"sim-close-{next(self._n)}")

    async def get_asset(self, symbol: str) -> BrokerAsset:
        return self.assets.get(symbol) or BrokerAsset(symbol=symbol, name=symbol, exchange="NASDAQ")

    async def get_all_assets(self) -> list[BrokerAsset]:
        return list(self.assets.values())

    async def latest_price(self, symbol: str) -> float | None:
        return self._last.get(symbol)

    async def get_calendar(self, d: date) -> BrokerCalendar | None:
        """Weekdays 09:30-16:00 ET (no holiday / early-close knowledge)."""
        if d.weekday() >= 5:
            return None
        o, c = session_bounds(d)
        return BrokerCalendar(d, o, c)

    async def get_clock(self) -> BrokerClock:
        now = self.clock.now()
        d = now.astimezone(ET).date()
        while True:
            cal = await self.get_calendar(d)
            if cal and cal.close > now:
                break
            d += timedelta(days=1)
        is_open = cal.open <= now < cal.close
        nxt_open = cal.open
        if is_open:
            d2 = d + timedelta(days=1)
            while (c2 := await self.get_calendar(d2)) is None:
                d2 += timedelta(days=1)
            nxt_open = c2.open
        return BrokerClock(now, is_open, nxt_open, cal.close)

