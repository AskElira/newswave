"""Shared fixtures/fakes for the execution + reconcile tests (no tests in here)."""
from __future__ import annotations

import asyncio
import json
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

from alpaca.common.exceptions import APIError
from alpaca.trading.enums import OrderStatus, PositionSide
from alpaca.trading.models import Asset, Order

from newswave.clock import ReplayClock, utc_iso
from newswave.config import Settings, StrategyParams
from newswave.db import Database
from newswave.execution.broker import AlpacaPaperBroker, BrokerError, BrokerOrder, SimBroker
from newswave.execution.engine import ExecutionEngine
from newswave.market.subscriptions import SubscriptionManager
from newswave.models import Bar, EntrySignal, Side, Trade
from newswave.risk import KillSwitch, RiskEngine
from newswave.timeline import Timeline

P = StrategyParams()
T0 = datetime(2026, 1, 2, 15, 0, tzinfo=UTC)  # Fri 10:00 ET (EST)
EOD = datetime(2026, 1, 2, 20, 55, tzinfo=UTC)  # 15:55 ET
V = "v1"


@dataclass
class Env:
    db: Database
    clock: ReplayClock
    broker: object
    eng: ExecutionEngine
    subs: SubscriptionManager
    kill: KillSwitch
    tl: Timeline
    params: StrategyParams

    @property
    def sim(self) -> SimBroker:
        return self.broker if isinstance(self.broker, SimBroker) else self.broker._c.sim  # type: ignore[attr-defined]


def make_env(db: Database, clock: ReplayClock | None = None, params: StrategyParams = P, broker=None,
             version: str = V) -> Env:
    clock = clock or ReplayClock(T0)
    broker = broker or SimBroker(clock, 6000.0)
    tl = Timeline(db, clock, version)
    subs = SubscriptionManager()
    kill = KillSwitch(db, version)
    eng = ExecutionEngine(db, clock, tl, params, Settings(starting_equity=6000.0, strategy_version=version),
                          broker, RiskEngine(params), kill, version, subs)
    return Env(db, clock, broker, eng, subs, kill, tl, params)


def new_env_on(env: Env, broker=None, params: StrategyParams | None = None) -> Env:
    """A 'restarted process': fresh engine/subscriptions over the SAME db and clock."""
    return make_env(env.db, env.clock, params or env.params, broker or env.broker)


def new_setup(db: Database, symbol: str = "ABC", side: str = "LONG", article: str | None = None,
              version: str = V) -> int:
    return db.insert("setups", {
        "article_id": article or f"a-{symbol}-{uuid.uuid4().hex[:6]}", "symbol": symbol, "variant": "production",
        "is_shadow": 0, "side": side, "stage": "ENTRY_SIGNAL", "max_stage": "ENTRY_SIGNAL",
        "news_latency_s": 1.5, "ai_confidence": 0.9, "catalyst": "guidance raise", "rvol": 3.0,
        "impulse_pct": 2.5, "atr": 1.0, "created_at": utc_iso(T0), "strategy_version": version})


def sig(setup_id: int, symbol: str = "ABC", side: Side = Side.LONG, trigger: float = 50.0, lo: float = 48.0,
        hi: float = 50.5, atr: float = 1.0, signal_at: str | None = None) -> EntrySignal:
    return EntrySignal(setup_id, symbol, side, trigger, lo, hi, atr,
                       signal_at or utc_iso(T0 - timedelta(seconds=1)))


async def px(env: Env, symbol: str, price: float, size: float = 100) -> None:
    await env.eng.on_trade(Trade(symbol, utc_iso(env.clock.now()), price, size))


def bar(symbol: str, close: float, high: float | None = None, low: float | None = None) -> Bar:
    return Bar(symbol, "2026-01-02T15:00:00.000Z", close, high or close, low or close, close, 1000)


def msgs(db: Database) -> list[str]:
    return [r["message"] for r in db.query("SELECT message FROM timeline ORDER BY id")]


def events(db: Database, level: str | None = None) -> list[dict]:
    if level:
        return db.query("SELECT * FROM system_events WHERE level=? ORDER BY id", (level,))
    return db.query("SELECT * FROM system_events ORDER BY id")


def orders(db: Database, purpose: str | None = None) -> list[dict]:
    if purpose:
        return db.query("SELECT * FROM orders WHERE purpose=? ORDER BY id", (purpose,))
    return db.query("SELECT * FROM orders ORDER BY id")


def stops_open(sim: SimBroker, symbol: str = "ABC") -> list[BrokerOrder]:
    return [sim._snap(o) for o in sim._orders.values()
            if o.type == "stop" and o.symbol == symbol and o.status in ("new", "accepted", "partially_filled")]


# ---------------------------------------------------------------- fake alpaca-py TradingClient
def api_error(code: int, msg: str = "x") -> APIError:
    return APIError(json.dumps({"code": code, "message": msg}),
                    SimpleNamespace(response=SimpleNamespace(status_code=code), request=None))


class FakeTradingClient:
    """Duck-types alpaca.trading.client.TradingClient on top of a SimBroker. Orders/assets are REAL
    alpaca-py models (so field names/enums are validated); account/position are namespaces."""

    def __init__(self, sim: SimBroker) -> None:
        self.sim = sim
        self.submit_calls = 0
        self.script: list = []  # per submit call: "ok" | "timeout_before" | "timeout_after" | ("api", code)
        self.lookup_fail = 0  # next N get_order_by_client_id calls raise ConnectionError
        self.daytrade_count = 0
        self.requests: list = []
        self._ids: dict[str, str] = {}
        self._bids: dict[str, str] = {}

    def _uid(self, bid: str) -> str:
        u = self._ids.setdefault(bid, str(uuid.uuid5(uuid.NAMESPACE_OID, bid)))
        self._bids[u] = bid
        return u

    def _order(self, bo: BrokerOrder) -> Order:
        now = self.sim.clock.now()
        return Order(
            id=self._uid(bo.broker_order_id), client_order_id=bo.client_order_id, created_at=now, updated_at=now,
            submitted_at=now, filled_at=now if bo.filled_qty else None, symbol=bo.symbol, qty=str(bo.qty),
            filled_qty=str(bo.filled_qty),
            filled_avg_price=None if bo.filled_avg_price is None else str(bo.filled_avg_price),
            order_class="simple", type=bo.order_type, order_type=bo.order_type, side=bo.side,
            time_in_force="day", limit_price=None if bo.limit_price is None else str(bo.limit_price),
            stop_price=None if bo.stop_price is None else str(bo.stop_price),
            status=OrderStatus(bo.status), extended_hours=False)

    def _wrap(self, coro):
        try:
            return asyncio.run(coro)
        except BrokerError as e:
            raise api_error(e.status_code or 500, str(e)) from e

    def submit_order(self, req):
        self.submit_calls += 1
        self.requests.append(req)
        mode = self.script.pop(0) if self.script else "ok"
        if mode == "timeout_before":
            raise TimeoutError("read timed out")
        if isinstance(mode, tuple):
            raise api_error(mode[1], mode[2] if len(mode) > 2 else "x")
        side, t = req.side.value, req.type.value
        if t == "limit":
            co = self.sim.submit_limit(req.symbol, side, int(req.qty), req.limit_price, req.client_order_id)
        elif t == "market":
            co = self.sim.submit_market(req.symbol, side, int(req.qty), req.client_order_id)
        else:
            co = self.sim.submit_stop(req.symbol, side, int(req.qty), req.stop_price, req.client_order_id)
        bo = self._wrap(co)
        if mode == "timeout_after":  # the order IS live at the broker, the reply never arrived
            import requests
            raise requests.exceptions.ReadTimeout("read timed out")
        return self._order(bo)

    def get_order_by_client_id(self, cid):
        if self.lookup_fail > 0:
            self.lookup_fail -= 1
            raise ConnectionError("down")
        bo = self._wrap(self.sim.get_order_by_client_id(cid))
        if bo is None:
            raise api_error(404, "order not found")
        return self._order(bo)

    def get_orders(self, filter=None):
        assert filter is not None and filter.status.value == "open"
        return [self._order(o) for o in self._wrap(self.sim.get_open_orders())]

    def replace_order_by_id(self, order_id, order_data=None):
        self.requests.append(order_data)
        return self._order(self._wrap(self.sim.replace_order_qty(
            self._bids[str(order_id)], order_data.qty, order_data.client_order_id)))

    def cancel_order_by_id(self, order_id):
        self._wrap(self.sim.cancel_order(self._bids[str(order_id)]))

    def close_position(self, symbol):
        bo = self._wrap(self.sim.close_position(symbol))
        if bo is None:
            raise api_error(404, "position not found")
        return self._order(bo)

    def get_all_positions(self):
        return [SimpleNamespace(symbol=p.symbol, qty=str(p.qty if p.side == Side.LONG else -p.qty),
                                side=PositionSide.LONG if p.side == Side.LONG else PositionSide.SHORT,
                                avg_entry_price=str(p.avg_entry_price), unrealized_pl=str(p.unrealized_pl))
                for p in self._wrap(self.sim.get_positions())]

    def get_account(self):
        a = self._wrap(self.sim.get_account())
        return SimpleNamespace(equity=str(a.equity), buying_power=str(a.buying_power), cash=str(a.cash),
                               daytrade_count=self.daytrade_count, pattern_day_trader=False,
                               trading_blocked=False)

    def _asset(self, a) -> Asset:
        return Asset(**{"id": str(uuid.uuid4()), "class": "us_equity", "exchange": "NASDAQ", "symbol": a.symbol,
                        "name": a.name, "status": "active", "tradable": a.tradable, "marginable": True,
                        "shortable": a.shortable, "easy_to_borrow": a.easy_to_borrow, "fractionable": False})

    def get_asset(self, symbol):
        return self._asset(self._wrap(self.sim.get_asset(symbol)))

    def get_all_assets(self, req=None):
        return [self._asset(a) for a in self._wrap(self.sim.get_all_assets())]


async def _nosleep(_s: float) -> None:
    return None


def alpaca_env(db: Database, params: StrategyParams = P, clock: ReplayClock | None = None):
    clock = clock or ReplayClock(T0)
    sim = SimBroker(clock, 6000.0)
    fake = FakeTradingClient(sim)
    broker = AlpacaPaperBroker("k", "s", client=fake, sleep=_nosleep)
    return make_env(db, clock, params, broker), fake
