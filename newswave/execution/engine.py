"""ExecutionEngine: the only place orders are created (SPEC 15-21, 23, 31; CONTRACT 10).

Shape
- One asyncio.Lock serialises every public entry point (ponytail: a slow broker call delays the next
  print; per-symbol locks if that ever matters at 3 positions).
- Nothing blocks waiting for a fill. An entry / exit order is registered as *pending* and resolved by
  polling the broker by client_order_id from on_clock / on_trade (>= 0.5 s apart), so ReplayClock
  tests drive it deterministically. on_entry_signal polls once right after submitting.
- The DB `orders` table is the source of truth for what was filled: every exit leg is derived from
  orders.filled_qty (idempotent watermark), so a crash at any point is repaired by reconcile_on_boot.
  orders.raw_json = {"reason": <ExitReason|None>, "broker": <raw>}.
- Stop safety: a broker-resident STOP exists from the FIRST (partial) entry fill on. A bot exit first
  resizes (partial) or cancels (full) that stop, waits (clock-driven poll, <= STOP_WAIT_S) until the broker
  reports the old order terminal -- Alpaca's cancel/replace are asynchronous and the shares stay held until
  then -- and only THEN sends the market order. If the stop filled meanwhile, that STOP fill is the exit.
- A single odd print through the stop never cancels a resting broker stop: the bot waits HOLD_S for the
  broker stop (which elects on consolidated prints) and only then cancels + sells.
"""
from __future__ import annotations

import asyncio
import json
import math
import re
import secrets
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, time, timedelta

from ..clock import ET, ReplayClock, effective_eod, parse_iso, to_et, utc_iso
from ..config import Settings, StrategyParams
from ..db import Database
from ..exits import ManagedPosition
from ..market.subscriptions import SlotPriority, SubscriptionManager
from ..models import (Bar, EntrySignal, ExitReason, OrderPurpose, RejectReason, RiskApproval, Side, Stage,
                      Trade)
from ..risk import AccountState, KillSwitch, RiskEngine, count_day_trades, daily_loss_breached
from ..timeline import Timeline
from .broker import (Broker, BrokerError, BrokerOrder, BrokerPosition, OrderOutcomeUnknown, OrderRejected,
                     is_terminal)

POLL_S = 0.5  # min spacing of broker polls for one pending order
EXIT_TIMEOUT_S = 15  # market exit not terminal after this -> cancel it
UNKNOWN_GRACE_S = 30  # an order the broker has never heard of is declared gone only after this
RETRY_S = 3  # after a failed exit order, wait this long before the next attempt
STOP_WAIT_S = 3  # a stop's cancel/replace must be terminal within this, else the exit attempt is retried
HOLD_S = 5  # price beyond the stop but the resting broker stop has not filled: wait this long, then exit
RECONCILE_POLL_S = 0.25  # boot: spacing of the polls that wait for cancels to land
RECONCILE_WAIT_N = 12
SNAPSHOT_EVERY_S = 300
AUDIT_EVERY_S = 60  # tradable check + broker-vs-ledger position audit
# nw-{db_uid}-{setup_id}-{purpose}-{n}; the uid-less form is the legacy shape and is still recognised
_NW = re.compile(r"^nw-(?:([0-9a-f]{6})-)?(\d+)-(ENTRY|STOP|PARTIAL|EXIT)-(\d+)$")
_FINAL = ("filled", "canceled", "expired", "rejected", "replaced", "done_for_day", "reconciled", "orphan")
_LIVE_STOP = frozenset({"new", "accepted", "pending_new", "partially_filled", "held"})


def _buy(side: Side) -> str:  # entry / cover direction
    return "buy" if side == Side.LONG else "sell"


def _opp(side: Side) -> str:  # exit direction
    return "sell" if side == Side.LONG else "buy"


def _dir(side: Side) -> int:
    return 1 if side == Side.LONG else -1


def _cents(x: float, up: bool) -> float:
    v = round(x * 100, 6)
    return (math.ceil(v) if up else math.floor(v)) / 100


@dataclass
class _PendingEntry:
    signal: EntrySignal
    approval: RiskApproval
    cid: str
    submitted_at: datetime
    deadline: datetime
    broker_order_id: str | None = None
    last_poll: datetime | None = None
    cancel_sent: bool = False
    unknown: bool = False
    shell: "_Pos | None" = None  # protective stop placed while the entry is still only partly filled


@dataclass
class _Wait:
    """An exit parked while the broker settles the cancel / replace of the resting stop."""
    kind: ExitReason
    full: bool
    qty: int
    hint: float | None
    deadline: datetime
    watch: str | None  # client id of the stop order that must go terminal before the sell
    last_poll: datetime | None = None
    sell_deadline: datetime | None = None  # set by the first "insufficient qty (held)" refusal of the sell


@dataclass
class _PendingExit:
    cid: str
    kind: ExitReason
    qty: int
    submitted_at: datetime
    deadline: datetime
    broker_order_id: str | None = None
    last_poll: datetime | None = None
    cancel_sent: bool = False
    unknown: bool = False


@dataclass
class _Pos:
    position_id: int
    setup_id: int
    symbol: str
    side: Side
    mp: ManagedPosition
    version: str
    stop_cid: str | None = None
    stop_bid: str | None = None
    stop_qty: int = 0
    stop_live: bool = False
    stop_cancelling: bool = False
    dead_stops: set = field(default_factory=set)
    pending: _PendingExit | None = None
    queued: ExitReason | None = None
    realized: float = 0.0  # realised P&L of exit legs so far
    retry_after: datetime | None = None
    persisted: tuple = ()
    waiting: _Wait | None = None
    hold: datetime | None = None  # bot-side STOP parked behind the resting broker stop since this instant
    hold_poll: datetime | None = None
    stop_retry_at: datetime | None = None

    @property
    def closed(self) -> bool:
        return self.mp.closed


class ExecutionEngine:
    def __init__(self, db: Database, clock, timeline: Timeline, params: StrategyParams, settings: Settings,
                 broker: Broker, risk_engine: RiskEngine, kill_switch: KillSwitch, strategy_version: str,
                 subscriptions: SubscriptionManager) -> None:
        import asyncio
        self.db, self.clock, self.timeline, self.params, self.settings = db, clock, timeline, params, settings
        self.broker, self.risk, self.kill_switch = broker, risk_engine, kill_switch
        self.version, self.subs = strategy_version, subscriptions
        self._lock = asyncio.Lock()
        self._pos: dict[int, _Pos] = {}
        self._entries: dict[int, _PendingEntry] = {}  # setup_id -> pending
        self._last: dict[str, float] = {}
        self._halted: set[str] = set()
        self._foreign: dict[str, int] = {}
        self._seq_floor: dict[tuple[int, str], int] = {}
        self._broker_equity: float | None = None
        self._buying_power: float | None = None
        self._day: date | None = None
        self._realized_today = 0.0
        self._realized_total = 0.0
        self._sod_equity = float(settings.starting_equity)
        self._sod_day: date | None = None
        self._realized_today_all = 0.0  # account-level: every strategy version
        self._trades_today = 0
        self._kill_day_checked: date | None = None
        self._tripped_day: date | None = None
        self._next_audit: datetime | None = None
        self._next_snap: datetime | None = None
        self.uid = self._db_uid()

    def _db_uid(self) -> str:
        """Random per-database id in every client_order_id: a wiped DB restarts setup ids at 1, the broker's
        order ids must not collide with the previous DB's."""
        r = self.db.one("SELECT value FROM kv WHERE key='db_uid'")
        if r:
            return r["value"]
        # replay never reaches a real broker and must stay byte-for-byte deterministic: fixed uid there
        uid = "000000" if isinstance(self.clock, ReplayClock) else secrets.token_hex(3)
        self.db.upsert("kv", {"key": "db_uid", "value": uid, "updated_at": utc_iso(self.clock.now())}, ["key"])
        return uid

    def _cid(self, setup_id: int, purpose: object, n: int) -> str:
        return f"nw-{self.uid}-{setup_id}-{purpose}-{n}"

    # ------------------------------------------------------------------ small helpers
    def _now(self) -> datetime:
        return self.clock.now()

    def _tl(self, symbol: str | None, stage: str, msg: str, setup_id: int | None = None, **data: object) -> None:
        self.timeline.log(symbol, stage, msg, setup_id, **data)

    def _ev(self, level: str, event: str, msg: str, **data: object) -> None:
        self.timeline.system_event(level, "execution", event, msg, **data)

    def _session(self, now: datetime | None = None) -> date:
        return to_et(now or self._now()).date()

    def _roll(self, now: datetime) -> None:
        d = self._session(now)
        if d != self._day:
            self._day = d
            self._refresh_realized()

    def _refresh_realized(self) -> None:
        d = self._day or self._session()
        day0 = utc_iso(datetime.combine(d, time(0, 0), ET))
        rows = self.db.query("SELECT exit_at, pnl, strategy_version FROM trades WHERE is_shadow=0")
        mine = [r for r in rows if r["strategy_version"] == self.version]
        self._realized_total = sum(r["pnl"] or 0.0 for r in mine)  # ledger equity stays per version
        today = [r for r in mine if (r["exit_at"] or "") >= day0]
        self._realized_today = sum(r["pnl"] or 0.0 for r in today)
        self._trades_today = len(today)
        # the daily loss budget is the ACCOUNT's: a restart under a new strategy_version must not reset it
        self._realized_today_all = sum(r["pnl"] or 0.0 for r in rows if (r["exit_at"] or "") >= day0)
        if self._sod_day != d:
            day1 = utc_iso(datetime.combine(d + timedelta(days=1), time(0, 0), ET))
            first = self.db.one("SELECT ledger_equity FROM equity_snapshots WHERE ts>=? AND ts<? "
                                "ORDER BY ts, id LIMIT 1", (day0, day1))
            self._sod_equity = float(first["ledger_equity"]) if first else self._ledger_equity()
            self._sod_day = d

    def _unrealized(self) -> float:
        return sum((self._last.get(c.symbol, c.mp.entry_price) - c.mp.entry_price) * c.mp.qty_open * _dir(c.side)
                   for c in self._pos.values())

    def _legs_realized(self) -> float:
        return sum(c.realized for c in self._pos.values())

    def _ledger_equity(self) -> float:
        return (self.settings.starting_equity + self._realized_total + self._legs_realized()
                + self._unrealized())

    def _setup(self, setup_id: int) -> dict:
        return self.db.one("SELECT * FROM setups WHERE id=?", (setup_id,)) or {}

    def _advance(self, setup_id: int, stage: Stage, **fields: object) -> None:
        cur = self._setup(setup_id).get("max_stage")
        rank = Stage(cur).rank if cur else -1
        f = {"stage": str(stage), "updated_at": utc_iso(self._now()), **fields}
        if stage.rank > rank:
            f["max_stage"] = str(stage)
        self.db.update("setups", setup_id, f)

    def _reject(self, signal: EntrySignal, reason: RejectReason, detail: str = "") -> None:
        now = utc_iso(self._now())
        self.db.update("setups", signal.setup_id, {"stage": str(Stage.REJECTED), "reject_reason": str(reason),
                                                   "updated_at": now, "closed_at": now})
        self._tl(signal.symbol, "REJECTED", f"entry rejected: {reason}" + (f" ({detail})" if detail else ""),
                 signal.setup_id, reason=str(reason))

    # ------------------------------------------------------------------ order rows
    def _next_seq(self, setup_id: int, purpose: str) -> int:
        rows = self.db.query("SELECT client_order_id FROM orders WHERE client_order_id LIKE ?",
                             (f"nw-{self.uid}-{setup_id}-{purpose}-%",))
        n = self._seq_floor.get((setup_id, purpose), 0)  # ids seen at the broker but never recorded
        for r in rows:
            m = _NW.match(r["client_order_id"])
            if m:
                n = max(n, int(m.group(4)))
        return n + 1

    def _order_row(self, cid: str) -> dict | None:
        return self.db.one("SELECT * FROM orders WHERE client_order_id=?", (cid,))

    def _insert_order(self, cid: str, setup_id: int, position_id: int | None, symbol: str, side: str, typ: str,
                      qty: int, limit: float | None, stop: float | None, purpose: OrderPurpose,
                      signal_at: str | None = None, reason: ExitReason | None = None,
                      version: str | None = None) -> int:
        return self.db.insert("orders", {
            "client_order_id": cid, "broker_order_id": None, "setup_id": setup_id, "position_id": position_id,
            "symbol": symbol, "side": side, "order_type": typ, "qty": qty, "limit_price": limit,
            "stop_price": stop, "purpose": str(purpose), "status": "pending_submit", "signal_at": signal_at,
            "submitted_at": utc_iso(self._now()), "ack_at": None, "filled_at": None, "filled_qty": 0,
            "filled_avg_price": None, "error": None,
            "raw_json": json.dumps({"reason": str(reason) if reason else None, "broker": None}),
            "strategy_version": version or self.version})

    def _raw(self, row: dict, bo: BrokerOrder | None) -> str:
        try:
            cur = json.loads(row.get("raw_json") or "{}")
        except ValueError:
            cur = {}
        cur["broker"] = bo.raw if bo else cur.get("broker")
        return json.dumps(cur, default=str)

    def _ack(self, cid: str, bo: BrokerOrder) -> None:
        row = self._order_row(cid)
        if row:
            self.db.update("orders", row["id"], {"broker_order_id": bo.broker_order_id, "status": bo.status,
                                                 "ack_at": utc_iso(self._now()), "raw_json": self._raw(row, bo)})

    def _progress(self, cid: str, bo: BrokerOrder) -> tuple[int, float, dict]:
        """Write the broker's view into the orders row; return (newly filled qty, price of that slice, row)."""
        row = self._order_row(cid)
        if row is None:
            return 0, 0.0, {}
        prev_q, prev_p = int(row["filled_qty"] or 0), float(row["filled_avg_price"] or 0.0)
        delta = max(0, bo.filled_qty - prev_q)
        price = 0.0
        f: dict = {"broker_order_id": bo.broker_order_id, "status": bo.status, "raw_json": self._raw(row, bo)}
        if delta > 0:
            avg = bo.filled_avg_price or prev_p
            price = (avg * bo.filled_qty - prev_p * prev_q) / delta
            ts = bo.filled_at or utc_iso(self._now())
            f.update(filled_qty=bo.filled_qty, filled_avg_price=avg, filled_at=ts)
            self.db.insert("fills", {"order_id": row["id"], "broker_fill_id": f"{bo.broker_order_id}:{bo.filled_qty}",
                                     "ts": ts, "qty": delta, "price": price})
        self.db.update("orders", row["id"], f)
        return delta, price, row

    # ------------------------------------------------------------------ accounting / snapshots
    def account_snapshot(self) -> dict:
        unreal = self._unrealized()
        legs = self._legs_realized()
        return {"ledger_equity": self._ledger_equity(),
                "broker_equity": self._broker_equity if self._broker_equity is not None else self._ledger_equity(),
                "buying_power": self._buying_power if self._buying_power is not None else self._ledger_equity(),
                "daily_pnl": self._realized_today + legs + unreal,
                "total_pnl": self._realized_total + legs + unreal}

    async def _snapshot(self, reason: str) -> None:
        try:
            acct = await self.broker.get_account()
            self._broker_equity, self._buying_power = acct.equity, acct.buying_power
        except BrokerError as e:
            self._ev("WARNING", "SNAPSHOT_ACCOUNT", f"account fetch failed: {e}")
        now = self._now()
        ledger = self._ledger_equity()
        self.db.insert("equity_snapshots", {
            "ts": utc_iso(now), "ledger_equity": ledger, "broker_equity": self._broker_equity,
            "buying_power": self._buying_power, "reason": reason, "strategy_version": self.version})
        d = self._session(now)
        self.db.upsert("daily_stats", {
            "session_date": d.isoformat(), "strategy_version": self.version, "start_equity": self._sod_equity,
            "end_equity": ledger, "realized_pnl": self._realized_today + self._legs_realized(),
            "unrealized_pnl": self._unrealized(), "trades": self._trades_today,
            "day_trades": count_day_trades(self.db, self.version, self._sessions(now))},
            ["session_date", "strategy_version"])
        self._next_snap = now + timedelta(seconds=SNAPSHOT_EVERY_S)

    @staticmethod
    def _sessions(now: datetime, n: int = 5) -> list[date]:
        d, out = to_et(now).date(), []
        while len(out) < n:
            if d.weekday() < 5:
                out.append(d)
            d -= timedelta(days=1)
        return out

    # ------------------------------------------------------------------ views for the daemon
    def open_positions(self) -> list[dict]:
        out = []
        for c in self._pos.values():
            m, last = c.mp, self._last.get(c.symbol, c.mp.entry_price)
            u = (last - m.entry_price) * m.qty_open * _dir(c.side)
            out.append({"position_id": c.position_id, "setup_id": c.setup_id, "symbol": c.symbol,
                        "side": str(c.side), "qty_open": m.qty_open, "qty_initial": m.qty_initial,
                        "entry_price": m.entry_price, "stop_price": m.stop_price,
                        "risk_per_share": m.risk_per_share, "last_price": last, "unrealized_pnl": u,
                        "unrealized_r": (last - m.entry_price) * _dir(c.side) / m.risk_per_share,
                        "trail_price": m.trail_price, "partial_taken": m.partial_taken, "mfe_r": m.mfe_r,
                        "mae_r": m.mae_r, "opened_at": utc_iso(m.opened_at)})
        return out

    async def load_assets(self) -> list[dict]:
        return [asdict(a) for a in await self.broker.get_all_assets()]

    # ================================================================== ENTRY
    async def on_entry_signal(self, signal: EntrySignal) -> None:
        async with self._lock:
            await self._entry(signal)

    def _account_state(self, now: datetime, acct) -> AccountState:
        pend_risk = sum(p.approval.risk_dollars for p in self._entries.values())
        open_risk = sum(c.mp.qty_open * c.mp.risk_per_share for c in self._pos.values()) + pend_risk
        bp = acct.buying_power
        if self.broker.kind == "sim":  # sim cash is not reserved by resting orders; alpaca's is
            bp -= sum(p.approval.limit_price * p.approval.qty for p in self._entries.values())
        dt = (acct.daytrade_count if self.broker.kind == "alpaca"
              else count_day_trades(self.db, self.version, self._sessions(now)))
        return AccountState(ledger_equity=self._ledger_equity(), broker_equity=acct.equity, buying_power=bp,
                            open_risk_dollars=open_risk, open_positions=len(self._pos) + len(self._entries),
                            day_trades_5_sessions=dt, kill_switch_active=self.kill_switch.is_disabled(self._session(now)),
                            now=now)

    async def _entry(self, sig: EntrySignal) -> None:
        now = self._now()
        self._roll(now)
        cid = self._cid(sig.setup_id, "ENTRY", 1)
        if sig.variant != "production":
            self._ev("ERROR", "SHADOW_SIGNAL", f"shadow signal {sig.setup_id} reached execution; ignored")
            return
        if sig.setup_id in self._entries or self._order_row(cid) is not None:
            self._ev("WARNING", "DUPLICATE_SIGNAL", f"setup {sig.setup_id} already has an entry order; ignored")
            return
        if sig.side == Side.SHORT:  # re-check borrow at this very moment
            try:
                a = await self.broker.get_asset(sig.symbol)
                ok = a.tradable and a.shortable and a.easy_to_borrow
            except BrokerError as e:
                ok = False
                self._ev("WARNING", "ASSET_LOOKUP", f"{sig.symbol}: {e}")
            if not ok:
                self._reject(sig, RejectReason.SHORT_UNAVAILABLE)
                return
        try:
            acct = await self.broker.get_account()
        except BrokerError as e:
            self._ev("ERROR", "ACCOUNT", f"cannot size {sig.symbol}: {e}")
            self._reject(sig, RejectReason.ORDER_REJECTED, "account unavailable")
            return
        self._broker_equity, self._buying_power = acct.equity, acct.buying_power
        res = self.risk.evaluate(sig, self._account_state(now, acct))
        if isinstance(res, RejectReason):
            self._reject(sig, res)
            return
        ap: RiskApproval = res
        limit = round(ap.limit_price, 2)
        self._insert_order(cid, sig.setup_id, None, sig.symbol, _buy(sig.side), "limit", ap.qty, limit, None,
                           OrderPurpose.ENTRY, signal_at=sig.signal_at)
        pend = _PendingEntry(sig, ap, cid, now, now + timedelta(seconds=self.params.entry_fill_timeout_s))
        try:
            bo = await self.broker.submit_limit(sig.symbol, _buy(sig.side), ap.qty, limit, cid)
        except OrderOutcomeUnknown as e:
            pend.unknown = True
            self.db.update("orders", self._order_row(cid)["id"], {"status": "unknown", "error": str(e)})
            self._ev("CRITICAL", "ENTRY_UNKNOWN", f"{cid}: outcome unknown; polling by client id", cid=cid)
        except BrokerError as e:
            self.db.update("orders", self._order_row(cid)["id"], {"status": "rejected", "error": str(e)})
            self._ev("ERROR", "ENTRY_REJECTED", f"{cid}: {e}", cid=cid)
            self._reject(sig, RejectReason.ORDER_REJECTED, str(e))
            return
        else:
            pend.broker_order_id = bo.broker_order_id
            self._ack(cid, bo)
        self._entries[sig.setup_id] = pend
        self._tl(sig.symbol, "ENTRY", f"{'BUY' if sig.side == Side.LONG else 'SHORT'} order sent: {ap.qty} "
                 f"{sig.symbol} limit {limit:.2f} (stop {ap.stop_price:.2f}, risk ${ap.risk_dollars:.0f})",
                 sig.setup_id, qty=ap.qty, limit=limit)
        await self._poll_entry(pend, self._now())

    async def _poll_entry(self, p: _PendingEntry, now: datetime) -> None:
        p.last_poll = now
        bo: BrokerOrder | None = None
        try:
            bo = await self.broker.get_order_by_client_id(p.cid)
        except BrokerError as e:
            self._ev("WARNING", "ENTRY_POLL", f"{p.cid}: {e}")
        if bo is None:
            # never conclude "failed" from silence: wait out the grace, then give up loudly
            if now >= p.deadline + timedelta(seconds=UNKNOWN_GRACE_S):
                self._entries.pop(p.signal.setup_id, None)
                row = self._order_row(p.cid)
                if row:
                    self.db.update("orders", row["id"], {"status": "unknown"})
                self._ev("CRITICAL", "ENTRY_GONE", f"{p.cid}: broker never showed the order; reconcile will "
                         "repair any orphan fill", cid=p.cid)
                self._reject(p.signal, RejectReason.ENTRY_NOT_FILLED, "order unknown at broker")
            return
        p.unknown, p.broker_order_id = False, bo.broker_order_id
        if is_terminal(bo.status):
            await self._finish_entry(p, bo)
            return
        if bo.filled_qty > 0:
            await self._protect_partial(p, bo)
        if now >= p.deadline and not p.cancel_sent:
            p.cancel_sent = True
            try:
                await self.broker.cancel_order(bo.broker_order_id)
            except BrokerError as e:  # probably filled meanwhile; the re-lookup below tells
                self._ev("WARNING", "ENTRY_CANCEL", f"{p.cid}: {e}")
            try:
                bo2 = await self.broker.get_order_by_client_id(p.cid)
            except BrokerError:
                bo2 = None
            if bo2 is not None and is_terminal(bo2.status):
                await self._finish_entry(p, bo2)

    async def _protect_partial(self, p: _PendingEntry, bo: BrokerOrder) -> None:
        """Shares already filled must not wait for the rest of the entry to resolve: stop them now, resize as
        more fills arrive. The stop lives on a position-less shell that _open_position adopts."""
        sh, fq = p.shell, bo.filled_qty
        if sh is not None and sh.stop_live and sh.stop_qty == fq:
            return
        self._progress(p.cid, bo)
        try:
            if sh is None:
                sig, ap = p.signal, p.approval
                stop = _cents(ap.stop_price, up=(sig.side == Side.LONG))
                fp = float(bo.filled_avg_price or ap.limit_price)
                if (fp <= stop) if sig.side == Side.LONG else (fp >= stop):
                    return  # already through the stop: _open_position exits it
                mp = ManagedPosition(side=sig.side, qty_open=fq, qty_initial=fq, entry_price=fp, stop_price=stop,
                                     risk_per_share=abs(fp - stop), opened_at=self._now(), params=self.params)
                sh = p.shell = _Pos(None, sig.setup_id, sig.symbol, sig.side, mp, self.version)  # type: ignore[arg-type]
                await self._place_stop(sh, fq)
                self._tl(sig.symbol, "STOP", f"partial entry fill ({fq} of {ap.qty}): stop placed at {stop:.2f} "
                         "before the order resolves", sig.setup_id, qty=fq)
            elif sh.stop_live:
                await self._resize_stop(sh, fq)
            else:
                await self._place_stop(sh, fq)
        except BrokerError as e:  # _open_position retries once the entry resolves
            self._ev("ERROR", "PARTIAL_STOP_FAILED", f"{p.signal.symbol}: {e}", setup_id=p.signal.setup_id)

    async def _finish_entry(self, p: _PendingEntry, bo: BrokerOrder) -> None:
        self._entries.pop(p.signal.setup_id, None)
        delta, price, row = self._progress(p.cid, bo)
        sig = p.signal
        if bo.filled_qty <= 0:
            reason = RejectReason.ORDER_REJECTED if bo.status == "rejected" else RejectReason.ENTRY_NOT_FILLED
            self._reject(sig, reason, f"order {bo.status}")
            return
        await self._open_position(p, bo, row)

    async def _open_position(self, p: _PendingEntry, bo: BrokerOrder, erow: dict) -> None:
        sig, ap = p.signal, p.approval
        now = self._now()
        fp = float(bo.filled_avg_price or ap.limit_price)
        fq = bo.filled_qty
        side = sig.side
        stop = _cents(ap.stop_price, up=(side == Side.LONG))  # tighter-or-equal to the approved stop
        beyond = fp <= stop if side == Side.LONG else fp >= stop
        rps = ap.risk_per_share if beyond else abs(fp - stop)
        filled_at = bo.filled_at or utc_iso(now)
        opened = parse_iso(filled_at)
        mp = ManagedPosition(side=side, qty_open=fq, qty_initial=fq, entry_price=fp, stop_price=stop,
                             risk_per_share=rps, opened_at=opened, params=self.params)
        pid = self.db.insert("positions", {
            "setup_id": sig.setup_id, "symbol": sig.symbol, "side": str(side), "is_shadow": 0,
            "variant": "production", "qty_initial": fq, "qty_open": fq, "entry_price": fp, "stop_price": stop,
            "risk_per_share": rps, "highest_since_entry": mp.highest_since_entry,
            "lowest_since_entry": mp.lowest_since_entry, "trail_price": None, "partial_taken": 0, "mfe_r": 0.0,
            "mae_r": 0.0, "status": "OPEN", "opened_at": utc_iso(opened), "closed_at": None,
            "strategy_version": self.version})
        self.db.update("orders", erow["id"], {"position_id": pid})
        ctx = _Pos(pid, sig.setup_id, sig.symbol, side, mp, self.version)
        self._pos[pid] = ctx
        ctx.persisted = self._snap(ctx)
        self.subs.request(sig.symbol, f"pos-{pid}", SlotPriority.POSITION, 1.0, now)
        self.subs.promote(f"pos-{pid}", SlotPriority.POSITION)
        self._advance(sig.setup_id, Stage.IN_POSITION, stop_price=stop)
        verb = "BUY" if side == Side.LONG else "SHORT"
        self._tl(sig.symbol, "POSITION", f"{verb} {fq} {sig.symbol} @ {fp:.2f} (stop {stop:.2f}, risk "
                 f"${fq * rps:.0f})", sig.setup_id, qty=fq, price=fp, stop=stop, position_id=pid)
        if fq < ap.qty:
            self._tl(sig.symbol, "POSITION", f"partial entry fill: kept {fq} of {ap.qty}", sig.setup_id)
        self._last.setdefault(sig.symbol, fp)
        sh = p.shell
        if sh is not None and sh.stop_cid:  # adopt the stop placed while the entry was part-filled
            ctx.stop_cid, ctx.stop_bid, ctx.stop_qty, ctx.stop_live = sh.stop_cid, sh.stop_bid, sh.stop_qty, sh.stop_live
            ctx.dead_stops |= sh.dead_stops
            self.db.execute("UPDATE orders SET position_id=? WHERE setup_id=? AND purpose='STOP' "
                            "AND position_id IS NULL", [pid, sig.setup_id])
        if beyond:
            self._ev("ERROR", "FILL_BEYOND_STOP", f"{sig.symbol} filled {fp:.2f} beyond stop {stop:.2f}; exiting")
            mp.exit_pending = True
            await self._snapshot("entry_fill")
            await self._exit(ctx, ExitReason.STOP, fq, fp, forced=True)
            return
        try:
            if ctx.stop_cid:
                await self._sync_stop(ctx)  # the early stop may already have fired
                await self._ensure_stop(ctx)
            else:
                await self._place_stop(ctx, fq)
        except BrokerError as e:
            self._ev("CRITICAL", "STOP_FAILED", f"{sig.symbol}: no protective stop ({e}); flattening", position_id=pid)
            mp.exit_pending = True
            await self._exit(ctx, ExitReason.STATE_CORRUPT, fq, None, forced=True)
        if self._tripped_day == self._session(now) and not ctx.closed:  # kill tripped while this entry was in flight
            mp.exit_pending = True
            await self._exit(ctx, ExitReason.RISK_KILL, mp.qty_open, None, forced=True)
        await self._snapshot("entry_fill")

    # ------------------------------------------------------------------ stop management
    async def _adopt_unknown_stop(self, ctx: _Pos) -> None:
        """A stop whose submit outcome was unknown may be live at the broker: look it up by client id and adopt
        it BEFORE anything new is placed or flattened (else it survives as an orphan). BrokerError = cannot say."""
        cid = ctx.stop_cid
        if not cid or ctx.stop_live:
            return
        row = self._order_row(cid)
        if row is None or row["status"] != "unknown":
            return
        bo = await self.broker.get_order_by_client_id(cid)
        if bo is None:  # the broker positively never saw it
            self.db.update("orders", row["id"], {"status": "rejected", "error": "absent at broker"})
            return
        if ctx.position_id is None:  # pre-position shell: the fill (if any) is applied by the real position
            self.db.update("orders", row["id"], {"status": bo.status, "broker_order_id": bo.broker_order_id})
        else:
            delta, price, _ = self._progress(cid, bo)
            if delta > 0:
                await self._apply_leg(ctx, delta, price, ExitReason.STOP)
            if ctx.closed:
                return
        if bo.status in _LIVE_STOP:
            ctx.stop_bid, ctx.stop_qty, ctx.stop_live = bo.broker_order_id, bo.qty, True
            self._ev("WARNING", "STOP_ADOPTED", f"{ctx.symbol}: stop {cid} turned out to be live; adopted",
                     position_id=ctx.position_id)

    async def _place_stop(self, ctx: _Pos, qty: int) -> None:
        await self._adopt_unknown_stop(ctx)
        if ctx.stop_live:  # adopted: make it the right size instead of stacking a second stop
            await self._resize_stop(ctx, qty)
            return
        cid = self._cid(ctx.setup_id, "STOP", self._next_seq(ctx.setup_id, "STOP"))
        ctx.stop_cid, ctx.stop_bid, ctx.stop_live = cid, None, False
        self._insert_order(cid, ctx.setup_id, ctx.position_id, ctx.symbol, _opp(ctx.side), "stop", qty, None,
                           ctx.mp.stop_price, OrderPurpose.STOP, version=ctx.version)
        try:
            bo = await self.broker.submit_stop(ctx.symbol, _opp(ctx.side), qty, ctx.mp.stop_price, cid)
        except OrderOutcomeUnknown:
            self.db.update("orders", self._order_row(cid)["id"], {"status": "unknown"})
            try:
                await self._adopt_unknown_stop(ctx)
            except BrokerError:
                pass
            if ctx.stop_live:
                return
            raise
        except BrokerError as e:
            self.db.update("orders", self._order_row(cid)["id"], {"status": "rejected", "error": str(e)})
            raise
        self._ack(cid, bo)
        ctx.stop_bid, ctx.stop_qty, ctx.stop_live = bo.broker_order_id, qty, True

    async def _stop_state(self, ctx: _Pos, cid: str) -> tuple[BrokerOrder | None, bool]:
        """Look up stop order `cid`, record any fill of it. -> (order or None, lookup_failed)."""
        try:
            bo = await self.broker.get_order_by_client_id(cid)
        except BrokerError:
            return None, True
        if bo is not None:
            delta, price, _ = self._progress(cid, bo)
            if delta > 0:
                await self._apply_leg(ctx, delta, price, ExitReason.STOP)
        return bo, False

    async def _resize_stop(self, ctx: _Pos, qty: int) -> str | None:
        """Replace the stop with one for `qty`. Returns the OLD stop's client id while the broker still has it
        pending_replace (its shares stay held until then), else None."""
        if not ctx.stop_live or ctx.stop_qty == qty:
            return None
        old = ctx.stop_cid
        cid = self._cid(ctx.setup_id, "STOP", self._next_seq(ctx.setup_id, "STOP"))
        self._insert_order(cid, ctx.setup_id, ctx.position_id, ctx.symbol, _opp(ctx.side), "stop", qty, None,
                           ctx.mp.stop_price, OrderPurpose.STOP, version=ctx.version)
        try:
            bo = await self.broker.replace_order_qty(ctx.stop_bid, qty, cid)
        except BrokerError as e:
            try:  # an ambiguous replace may still have gone through
                bo = await self.broker.get_order_by_client_id(cid)
            except BrokerError:
                bo = None
            # accept the look-up only if it really is our replacement (an id clash with an old order is not)
            if bo is None or bo.qty != qty or bo.status not in _LIVE_STOP:
                self.db.update("orders", self._order_row(cid)["id"], {"status": "rejected", "error": str(e)})
                raise
        self._ack(cid, bo)
        orow = self._order_row(old)
        if orow and orow["status"] not in _FINAL:
            self.db.update("orders", orow["id"], {"status": "replaced"})
        ctx.dead_stops.add(old)
        ctx.stop_cid, ctx.stop_bid, ctx.stop_qty = cid, bo.broker_order_id, qty
        self._tl(ctx.symbol, "STOP", f"stop resized to {qty} shares", ctx.setup_id, qty=qty)
        if ctx.position_id is None:  # shell: nobody sells against it
            return None
        old_bo, _ = await self._stop_state(ctx, old)  # lookup down: go on, a held-shares refusal is retried
        return old if (old_bo is not None and not is_terminal(old_bo.status)) else None

    async def _cancel_stop(self, ctx: _Pos) -> str | None:
        """Cancel the resting stop. Returns its client id while the broker still shows it un-terminal
        (pending_cancel: shares still held), None once it is settled or gone."""
        await self._adopt_unknown_stop(ctx)
        if not ctx.stop_cid or not ctx.stop_live or ctx.closed:
            return None
        ctx.stop_cancelling = True
        cid = ctx.stop_cid
        try:
            try:
                await self.broker.cancel_order(ctx.stop_bid)  # type: ignore[arg-type]
            except BrokerError as e:  # already filled / canceled: the lookup below decides
                self._ev("WARNING", "STOP_CANCEL", f"{cid}: {e}")
            ctx.dead_stops.add(cid)
            ctx.stop_live = False
            bo, _ = await self._stop_state(ctx, cid)  # lookup down: go on, a held-shares refusal is retried
            if ctx.closed:
                return None
            return cid if (bo is not None and not is_terminal(bo.status)) else None
        finally:
            ctx.stop_cancelling = False

    async def _ensure_stop(self, ctx: _Pos) -> None:
        if ctx.closed:
            return
        if not ctx.stop_live:
            await self._place_stop(ctx, ctx.mp.qty_open)
        elif ctx.stop_qty != ctx.mp.qty_open:
            await self._resize_stop(ctx, ctx.mp.qty_open)

    async def _sync_stop(self, ctx: _Pos) -> bool:
        """Record any fill of the broker-resident stop. True if that closed the position."""
        if not ctx.stop_cid or ctx.stop_cancelling or ctx.closed or ctx.waiting is not None:
            return ctx.closed
        try:
            bo = await self.broker.get_order_by_client_id(ctx.stop_cid)
        except BrokerError:
            return False
        if bo is None:
            return False
        delta, price, _ = self._progress(ctx.stop_cid, bo)
        if delta > 0:
            await self._apply_leg(ctx, delta, price, ExitReason.STOP)
            if ctx.closed:
                return True
        if not ctx.stop_live and bo.status in _LIVE_STOP and ctx.stop_cid not in ctx.dead_stops:
            ctx.stop_bid, ctx.stop_qty, ctx.stop_live = bo.broker_order_id, bo.qty, True  # adopt (unknown submit)
        elif ctx.stop_live and is_terminal(bo.status):
            ctx.stop_live = False
            self._ev("WARNING", "STOP_GONE", f"{ctx.symbol}: stop {ctx.stop_cid} is {bo.status}; re-placing",
                     position_id=ctx.position_id)
            if ctx.pending is None and not ctx.mp.exit_pending:
                try:
                    await self._ensure_stop(ctx)
                except BrokerError as e:
                    self._ev("CRITICAL", "STOP_FAILED", f"{ctx.symbol}: unprotected ({e})", position_id=ctx.position_id)
        return ctx.closed

    async def _reprotect(self, ctx: _Pos, now: datetime) -> None:
        """A position with no live stop (a failed re-place, an unknown submit that never resolved) is retried
        every RETRY_S: it is never left unprotected for longer than that."""
        if (ctx.stop_live or ctx.closed or ctx.pending is not None or ctx.waiting is not None
                or ctx.hold is not None or ctx.mp.exit_pending
                or (ctx.stop_retry_at is not None and now < ctx.stop_retry_at)):
            return
        ctx.stop_retry_at = now + timedelta(seconds=RETRY_S)
        try:
            await self._ensure_stop(ctx)
        except BrokerError as e:
            self._ev("CRITICAL", "STOP_FAILED", f"{ctx.symbol}: still unprotected ({e}); retrying",
                     position_id=ctx.position_id)

    # ------------------------------------------------------------------ exits
    @staticmethod
    def _held(e: BaseException) -> bool:
        """Alpaca refuses a sell while another open order still holds the shares (403 insufficient qty)."""
        return isinstance(e, OrderRejected) and e.status_code == 403 and "insufficient qty" in str(e).lower()

    async def _exit(self, ctx: _Pos, kind: ExitReason, qty: int, hint: float | None = None,
                    forced: bool = False) -> None:
        mp = ctx.mp
        if ctx.closed:
            return
        now = self._now()
        full_kind = kind != ExitReason.PARTIAL_PROFIT
        if ctx.pending is not None or ctx.waiting is not None:
            if full_kind:  # run it as soon as the in-flight order resolves
                ctx.queued = kind
                mp.exit_pending = True
            return
        if not forced and ctx.retry_after and now < ctx.retry_after:
            self._abort(ctx, kind)
            return
        if forced:
            ctx.hold = None
        if await self._sync_stop(ctx):
            return
        qty = min(int(qty), mp.qty_open)
        if qty <= 0:
            self._abort(ctx, kind)
            return
        full = qty >= mp.qty_open
        if full:
            mp.exit_pending = True
        if kind == ExitReason.STOP and not forced and ctx.stop_live:
            # one print through the stop is not proof: the resting broker stop elects on consolidated prints.
            # Let it work; _check_hold escalates if it has not filled after HOLD_S.
            if ctx.hold is None:
                ctx.hold = now
                self._tl(ctx.symbol, "STOP", f"print {hint if hint is not None else self._last.get(ctx.symbol)} "
                         f"through stop {mp.stop_price:.2f}: waiting for the broker stop", ctx.setup_id)
            return
        w = _Wait(kind, full, qty, hint, now + timedelta(seconds=STOP_WAIT_S), None)
        try:
            w.watch = await (self._cancel_stop(ctx) if full else self._resize_stop(ctx, mp.qty_open - qty))
        except BrokerError as e:
            await self._fail_exit(ctx, kind, f"stop adjust failed: {e}")
            return
        if ctx.closed:
            return
        if w.watch:  # the broker just showed the old stop un-terminal: park, the clock-driven poll resumes us
            w.last_poll = now
            ctx.waiting = w
            return
        await self._advance_exit(ctx, w)

    async def _advance_exit(self, ctx: _Pos, w: _Wait) -> None:
        """Second half of an exit: once the old stop is terminal (or filled), send the market sell."""
        mp, now = ctx.mp, self._now()
        w.last_poll = now
        ctx.waiting = None
        if w.watch:
            bo, err = await self._stop_state(ctx, w.watch)
            if ctx.closed:  # the stop itself filled: that STOP fill is the exit
                return
            if err or (bo is not None and not is_terminal(bo.status)):
                if now < w.deadline:
                    if bo is not None and bo.status in _LIVE_STOP:  # the cancel did not take: ask again
                        try:
                            await self.broker.cancel_order(bo.broker_order_id)
                        except BrokerError:
                            pass
                    ctx.waiting = w
                    return
                if w.full and bo is not None and bo.status in _LIVE_STOP:  # never cancelled: it still protects us
                    ctx.stop_live = True
                    ctx.dead_stops.discard(w.watch)
                await self._fail_exit(ctx, w.kind, f"stop {w.watch} still {bo.status if bo else 'unreachable'} "
                                      f"after {STOP_WAIT_S}s")
                await self._run_queued(ctx)
                return
            w.watch = None
        qty = mp.qty_open if w.full else min(w.qty, mp.qty_open)
        if qty <= 0:
            self._abort(ctx, w.kind)
            return
        purpose = OrderPurpose.PARTIAL if w.kind == ExitReason.PARTIAL_PROFIT else OrderPurpose.EXIT
        cid = self._cid(ctx.setup_id, purpose, self._next_seq(ctx.setup_id, str(purpose)))
        self._insert_order(cid, ctx.setup_id, ctx.position_id, ctx.symbol, _opp(ctx.side), "market", qty, None,
                           None, purpose, reason=w.kind, version=ctx.version)
        pend = _PendingExit(cid, w.kind, qty, now, now + timedelta(seconds=EXIT_TIMEOUT_S))
        try:
            bo = await self.broker.submit_market(ctx.symbol, _opp(ctx.side), qty, cid)
        except OrderOutcomeUnknown as e:
            pend.unknown = True
            self.db.update("orders", self._order_row(cid)["id"], {"status": "unknown", "error": str(e)})
            self._ev("CRITICAL", "EXIT_UNKNOWN", f"{cid}: outcome unknown; polling by client id", cid=cid)
        except BrokerError as e:
            self.db.update("orders", self._order_row(cid)["id"], {"status": "rejected", "error": str(e)})
            if self._held(e):  # retryable: the shares are still reserved by the old stop
                w.sell_deadline = w.sell_deadline or now + timedelta(seconds=STOP_WAIT_S)
                if now < w.sell_deadline:
                    ctx.waiting = w
                    return
                self._ev("CRITICAL", "EXIT_SELL_FAILED", f"{ctx.symbol} {w.kind}: sell still refused after "
                         f"{STOP_WAIT_S}s ({e}); re-placing the stop", position_id=ctx.position_id)
            await self._fail_exit(ctx, w.kind, f"{cid}: {e}")
            await self._run_queued(ctx)
            return
        else:
            pend.broker_order_id = bo.broker_order_id
            self._ack(cid, bo)
        ctx.pending = pend
        await self._poll_exit(ctx, self._now())

    async def _run_queued(self, ctx: _Pos) -> None:
        if ctx.queued and not ctx.closed and ctx.pending is None and ctx.waiting is None:
            k, ctx.queued = ctx.queued, None
            await self._exit(ctx, k, ctx.mp.qty_open, forced=True)

    async def _check_hold(self, ctx: _Pos, now: datetime) -> None:
        """A bot-side STOP parked behind the resting broker stop: wait for its fill, else escalate after HOLD_S."""
        if ctx.hold is None:
            return
        m = ctx.mp
        if ctx.closed:
            ctx.hold = None
            return
        if ctx.hold_poll is None or (now - ctx.hold_poll).total_seconds() >= POLL_S:
            ctx.hold_poll = now
            if await self._sync_stop(ctx):  # the broker stop filled: that is the STOP exit
                ctx.hold = None
                return
        last = self._last.get(ctx.symbol, m.entry_price)
        beyond = last <= m.stop_price if ctx.side == Side.LONG else last >= m.stop_price
        if not beyond:
            ctx.hold = None
            m.clear_pending_exit()
            self._tl(ctx.symbol, "STOP", f"price back inside the stop ({last:.2f}); broker stop keeps watching",
                     ctx.setup_id)
            return
        if ctx.stop_live and (now - ctx.hold).total_seconds() < HOLD_S:
            return
        ctx.hold = None
        self._ev("WARNING", "STOP_BOT_EXIT", f"{ctx.symbol}: {last:.2f} beyond stop {m.stop_price:.2f} for "
                 f"{HOLD_S}s (or the broker stop is gone) and the broker stop has not filled; exiting by market",
                 position_id=ctx.position_id)
        await self._exit(ctx, ExitReason.STOP, m.qty_open, forced=True)

    def _abort(self, ctx: _Pos, kind: ExitReason) -> None:
        """An action the policy already marked as emitted, dropped before any order: re-arm it."""
        if kind == ExitReason.PARTIAL_PROFIT:
            ctx.mp.partial_taken = False
        else:
            ctx.mp.clear_pending_exit()

    async def _fail_exit(self, ctx: _Pos, kind: ExitReason, msg: str) -> None:
        ctx.pending = ctx.waiting = ctx.hold = None
        self._abort(ctx, kind)
        ctx.retry_after = self._now() + timedelta(seconds=RETRY_S)
        self._ev("ERROR", "EXIT_FAILED", f"{ctx.symbol} {kind}: {msg}; will retry", position_id=ctx.position_id)
        try:
            await self._ensure_stop(ctx)
        except BrokerError as e:
            self._ev("CRITICAL", "STOP_FAILED", f"{ctx.symbol}: unprotected ({e})", position_id=ctx.position_id)

    async def _poll_exit(self, ctx: _Pos, now: datetime) -> None:
        p = ctx.pending
        if p is None:
            return
        p.last_poll = now
        try:
            bo = await self.broker.get_order_by_client_id(p.cid)
        except BrokerError as e:
            self._ev("WARNING", "EXIT_POLL", f"{p.cid}: {e}")
            return
        if bo is None:
            if now >= p.submitted_at + timedelta(seconds=UNKNOWN_GRACE_S):
                await self._fail_exit(ctx, p.kind, f"{p.cid} unknown at broker after {UNKNOWN_GRACE_S}s")
            return
        p.unknown, p.broker_order_id = False, bo.broker_order_id
        delta, price, _ = self._progress(p.cid, bo)
        if delta > 0:
            await self._apply_leg(ctx, delta, price, p.kind)
        if ctx.closed:
            ctx.pending = None
            return
        if is_terminal(bo.status):
            ctx.pending = None
            if bo.filled_qty < p.qty:
                await self._fail_exit(ctx, p.kind, f"{p.cid} ended {bo.status} with {bo.filled_qty}/{p.qty}")
            await self._run_queued(ctx)
            return
        if now >= p.deadline and not p.cancel_sent:
            p.cancel_sent = True
            try:
                await self.broker.cancel_order(bo.broker_order_id)
            except BrokerError as e:
                self._ev("WARNING", "EXIT_CANCEL", f"{p.cid}: {e}")

    async def _apply_leg(self, ctx: _Pos, qty: int, price: float, kind: ExitReason) -> None:
        mp = ctx.mp
        if qty > mp.qty_open:
            self._ev("ERROR", "OVERFILL", f"{ctx.symbol}: exit fill {qty} > open {mp.qty_open}; clamped",
                     position_id=ctx.position_id)
            qty = mp.qty_open
        if qty <= 0:
            return
        r = (price - mp.entry_price) * _dir(ctx.side) / mp.risk_per_share
        mp.apply_exit_fill(qty, price)
        ctx.realized += qty * (price - mp.entry_price) * _dir(ctx.side)
        pct = 100 * qty / mp.qty_initial
        msg = {ExitReason.PARTIAL_PROFIT: f"{r:+.1f}R -> sold {pct:.0f}% ({qty} {ctx.symbol} @ {price:.2f})",
               ExitReason.TRAIL: f"5m close below ATR trail -> sold {qty} {ctx.symbol} @ {price:.2f}",
               ExitReason.STOP: f"stop hit -> sold {qty} {ctx.symbol} @ {price:.2f}"}.get(
            kind, f"{kind} -> sold {qty} {ctx.symbol} @ {price:.2f}")
        self._tl(ctx.symbol, "EXIT", msg, ctx.setup_id, qty=qty, price=price, reason=str(kind), r=r)
        self._persist(ctx)
        if mp.closed:
            await self._close_position(ctx)
        else:
            await self._snapshot("exit_fill")

    @staticmethod
    def _snap(ctx: _Pos) -> tuple:
        m = ctx.mp
        return (m.qty_open, m.highest_since_entry, m.lowest_since_entry, m.trail_price, m.partial_taken,
                m.mfe_r, m.mae_r)

    def _persist(self, ctx: _Pos) -> None:
        s = self._snap(ctx)
        if s == ctx.persisted:
            return
        m = ctx.mp
        self.db.update("positions", ctx.position_id, {
            "qty_open": m.qty_open, "stop_price": m.stop_price, "highest_since_entry": m.highest_since_entry,
            "lowest_since_entry": m.lowest_since_entry, "trail_price": m.trail_price,
            "partial_taken": int(m.partial_taken), "mfe_r": m.mfe_r, "mae_r": m.mae_r})
        ctx.persisted = s

    async def _close_position(self, ctx: _Pos) -> None:
        legs = self.db.query(
            "SELECT * FROM orders WHERE position_id=? AND purpose IN ('STOP','PARTIAL','EXIT') AND filled_qty>0 "
            "ORDER BY filled_at, id", (ctx.position_id,))
        m = ctx.mp
        q = sum(int(r["filled_qty"]) for r in legs)
        avg_exit = sum(r["filled_qty"] * r["filled_avg_price"] for r in legs) / q if q else m.entry_price
        pnl = sum(r["filled_qty"] * (r["filled_avg_price"] - m.entry_price) for r in legs) * _dir(ctx.side)
        last = legs[-1] if legs else None
        reason = "STATE_CORRUPT"
        if last is not None:
            try:
                reason = json.loads(last["raw_json"] or "{}").get("reason") or (
                    "STOP" if last["purpose"] == "STOP" else "EXIT")
            except ValueError:
                reason = "STOP" if last["purpose"] == "STOP" else "EXIT"
        exit_at = last["filled_at"] if last else utc_iso(self._now())
        erow = self.db.one("SELECT * FROM orders WHERE position_id=? AND purpose='ENTRY' ORDER BY id LIMIT 1",
                           (ctx.position_id,)) or {}
        lat = None
        if erow.get("signal_at") and erow.get("filled_at"):
            lat = (parse_iso(erow["filled_at"]) - parse_iso(erow["signal_at"])).total_seconds() * 1000
        s = self._setup(ctx.setup_id)
        self.db.insert("trades", {
            "setup_id": ctx.setup_id, "position_id": ctx.position_id, "symbol": ctx.symbol, "side": str(ctx.side),
            "is_shadow": 0, "variant": "production", "entry_at": utc_iso(m.opened_at), "exit_at": exit_at,
            "entry_price": m.entry_price, "avg_exit_price": avg_exit, "qty": m.qty_initial, "pnl": pnl,
            "r_multiple": pnl / (m.qty_initial * m.risk_per_share), "exit_reason": reason,
            "catalyst": s.get("catalyst"), "ai_confidence": s.get("ai_confidence"), "rvol": s.get("rvol"),
            "impulse_pct": s.get("impulse_pct"), "atr": s.get("atr"), "entry_latency_ms": lat,
            "news_latency_s": s.get("news_latency_s"), "mfe_r": m.mfe_r, "mae_r": m.mae_r,
            "strategy_version": ctx.version})
        now = utc_iso(self._now())
        self.db.update("positions", ctx.position_id, {
            "status": "CLOSED", "qty_open": 0, "closed_at": now, "highest_since_entry": m.highest_since_entry,
            "lowest_since_entry": m.lowest_since_entry, "trail_price": m.trail_price,
            "partial_taken": int(m.partial_taken), "mfe_r": m.mfe_r, "mae_r": m.mae_r})
        self._advance(ctx.setup_id, Stage.CLOSED, closed_at=now)
        self.subs.release(f"pos-{ctx.position_id}")
        self._pos.pop(ctx.position_id, None)
        self._refresh_realized()
        self._tl(ctx.symbol, "EXIT", f"position closed: {ctx.symbol} {reason} pnl ${pnl:+.2f} "
                 f"({pnl / (m.qty_initial * m.risk_per_share):+.2f}R)", ctx.setup_id, pnl=pnl, reason=reason)
        await self._snapshot("position_closed")

    # ================================================================== EVENT FEEDS
    def _by_symbol(self, symbol: str) -> list[_Pos]:
        return [c for c in self._pos.values() if c.symbol == symbol]

    async def _dispatch(self, ctx: _Pos, actions) -> None:
        for a in actions:
            await self._exit(ctx, a.kind, a.qty, a.price_hint)

    async def on_trade(self, t: Trade) -> None:
        async with self._lock:
            now = self._now()
            self._roll(now)
            self._last[t.symbol] = t.price
            if self.broker.kind == "sim":
                await self.broker.on_trade(t)  # type: ignore[attr-defined]
            await self._poll_pending(now, symbol=t.symbol)
            ts = parse_iso(t.ts)
            for ctx in self._by_symbol(t.symbol):
                if ctx.hold is not None:
                    await self._check_hold(ctx, now)
                if not ctx.closed:
                    await self._dispatch(ctx, ctx.mp.on_trade(t.price, ts))
            await self._check_kill(now)

    async def on_bar_5m(self, symbol: str, bar: Bar, indicators: dict) -> None:
        async with self._lock:
            now = self._now()
            atr = float(indicators.get("atr") or 0.0)
            for ctx in self._by_symbol(symbol):
                before = ctx.mp.trail_price
                actions = ctx.mp.on_bar_close(bar, atr, now)
                after = ctx.mp.trail_price
                if after is not None and after != before:
                    self._tl(symbol, "TRAIL", f"trail {'raised to' if before is not None else 'set at'} {after:.2f}",
                             ctx.setup_id, trail=after)
                for a in actions:
                    if a.kind == ExitReason.TRAIL:
                        self._tl(symbol, "TRAIL", f"5m close {bar.close:.2f} below ATR trail {after:.2f}",
                                 ctx.setup_id)
                self._persist(ctx)
                await self._dispatch(ctx, actions)

    async def on_opposite_news(self, symbol: str, new_side: Side) -> None:
        async with self._lock:
            for ctx in self._by_symbol(symbol):
                if ctx.side != new_side:
                    self._tl(symbol, "EXIT", f"opposite {new_side} news on open {ctx.side} -> exiting",
                             ctx.setup_id)
                    ctx.mp.exit_pending = True
                    await self._exit(ctx, ExitReason.OPPOSITE_NEWS, ctx.mp.qty_open, forced=True)

    async def on_status(self, d: dict) -> None:
        async with self._lock:
            sym = d.get("symbol")
            ctxs = self._by_symbol(sym) if sym else []
            halted = bool(d.get("halted"))
            if halted and ctxs and sym not in self._halted:
                self._ev("WARNING", "HALT_OPEN_POSITION", f"{sym} halted while a position is open",
                         symbol=sym)
                self._tl(sym, "STATUS", f"{sym} halted with an open position")
            (self._halted.add if halted else self._halted.discard)(sym)
            if d.get("tradable") is False:
                for ctx in ctxs:
                    ctx.mp.exit_pending = True
                    await self._exit(ctx, ExitReason.NON_TRADABLE, ctx.mp.qty_open, forced=True)

    async def _poll_pending(self, now: datetime, symbol: str | None = None, force: bool = False) -> None:
        for p in list(self._entries.values()):
            if symbol and p.signal.symbol != symbol:
                continue
            if force or p.last_poll is None or (now - p.last_poll).total_seconds() >= POLL_S:
                await self._poll_entry(p, now)
        for ctx in list(self._pos.values()):
            if symbol and ctx.symbol != symbol:
                continue
            pe, w = ctx.pending, ctx.waiting
            if pe is not None and (force or pe.last_poll is None or (now - pe.last_poll).total_seconds() >= POLL_S):
                await self._poll_exit(ctx, now)
            elif w is not None and (force or w.last_poll is None or (now - w.last_poll).total_seconds() >= POLL_S):
                await self._advance_exit(ctx, w)

    async def on_clock(self, now: datetime) -> None:
        async with self._lock:
            self._roll(now)
            await self._poll_pending(now)
            for ctx in list(self._pos.values()):
                if ctx.pending is None and ctx.waiting is None:
                    await self._sync_stop(ctx)
                if ctx.closed:
                    continue
                await self._check_hold(ctx, now)
                if ctx.closed:
                    continue
                await self._dispatch(ctx, ctx.mp.on_clock(now))
                await self._reprotect(ctx, now)
                self._persist(ctx)
            await self._check_kill(now)
            if now >= effective_eod(now, self.params) and (self._entries or self._pos):
                await self._flatten(ExitReason.EOD)
            if self._pos and (self._next_audit is None or now >= self._next_audit):
                self._next_audit = now + timedelta(seconds=AUDIT_EVERY_S)
                await self._audit()
            if self._next_snap is None or now >= self._next_snap:
                await self._snapshot("periodic")

    async def _audit(self) -> None:
        for ctx in list(self._pos.values()):
            try:
                a = await self.broker.get_asset(ctx.symbol)
            except BrokerError:
                a = None
            if a is not None and not a.tradable and not ctx.closed and ctx.pending is None and ctx.waiting is None:
                self._ev("ERROR", "NON_TRADABLE", f"{ctx.symbol} is no longer tradable")
                ctx.mp.exit_pending = True
                await self._exit(ctx, ExitReason.NON_TRADABLE, ctx.mp.qty_open, forced=True)
        try:
            bps = {p.symbol: p for p in await self.broker.get_positions()}
        except BrokerError:
            return
        for ctx in list(self._pos.values()):
            if ctx.pending is None and ctx.waiting is None and not ctx.mp.exit_pending and not ctx.closed:
                await self._repair(ctx, bps.get(ctx.symbol))

    # ================================================================== KILL SWITCH / FLATTEN
    async def _check_kill(self, now: datetime) -> None:
        today = self._session(now)
        if self._kill_day_checked != today:
            self._kill_day_checked = today
            if self.kill_switch.is_disabled(today):
                self._tripped_day = today
        if self._tripped_day == today:
            return
        realized, unreal = self._realized_today_all + self._legs_realized(), self._unrealized()
        if daily_loss_breached(self._sod_equity, realized, unreal, self.params.max_daily_loss_pct):
            self._tripped_day = today
            msg = (f"daily loss limit hit: realized {realized:+.2f} unrealized {unreal:+.2f} vs "
                   f"{self.params.max_daily_loss_pct}% of {self._sod_equity:.2f}")
            self.kill_switch.trip(today, now, msg)
            self._ev("CRITICAL", "KILL_SWITCH", msg)
            self._tl(None, "RISK", "KILL SWITCH: " + msg)
            await self._flatten(ExitReason.RISK_KILL)

    async def _cancel_entries(self, why: str) -> None:
        for p in list(self._entries.values()):
            if p.broker_order_id is None:
                try:
                    bo = await self.broker.get_order_by_client_id(p.cid)
                    p.broker_order_id = bo.broker_order_id if bo else None
                except BrokerError:
                    pass
            if p.broker_order_id:
                try:
                    await self.broker.cancel_order(p.broker_order_id)
                except BrokerError as e:
                    self._ev("WARNING", "ENTRY_CANCEL", f"{p.cid}: {e}")
            p.cancel_sent = True
            p.deadline = self._now()
            self._tl(p.signal.symbol, "ENTRY", f"entry order cancelled ({why})", p.signal.setup_id)
            await self._poll_entry(p, self._now())

    async def _flatten(self, reason: ExitReason) -> None:
        await self._cancel_entries(str(reason))  # may turn a partial fill into a position we then flatten
        for ctx in list(self._pos.values()):
            if ctx.hold is not None and not ctx.closed:  # a parked STOP must not delay a flatten
                await self._exit(ctx, reason, ctx.mp.qty_open, forced=True)
                continue
            busy = ctx.pending is not None or ctx.waiting is not None
            if ctx.closed or ctx.mp.exit_pending or busy:
                if busy and reason == ExitReason.RISK_KILL:
                    ctx.queued = reason
                continue
            ctx.mp.exit_pending = True
            await self._exit(ctx, reason, ctx.mp.qty_open, forced=True)

    async def flatten_all(self, reason: ExitReason) -> None:
        async with self._lock:
            await self._flatten(reason)

    # ================================================================== RECONCILIATION
    def _synthetic_exit(self, ctx: _Pos, qty: int, price: float, reason: ExitReason) -> None:
        cid = self._cid(ctx.setup_id, "EXIT", self._next_seq(ctx.setup_id, "EXIT"))
        now = utc_iso(self._now())
        oid = self._insert_order(cid, ctx.setup_id, ctx.position_id, ctx.symbol, _opp(ctx.side), "market", qty,
                                 None, None, OrderPurpose.EXIT, reason=reason, version=ctx.version)
        self.db.update("orders", oid, {"status": "reconciled", "filled_qty": qty, "filled_avg_price": price,
                                       "filled_at": now, "ack_at": now})
        self.db.insert("fills", {"order_id": oid, "broker_fill_id": "reconciled", "ts": now, "qty": qty,
                                 "price": price})

    async def _repair(self, ctx: _Pos, bp: BrokerPosition | None) -> None:
        """Make the in-memory position agree with the broker's (audit + boot)."""
        if ctx.closed:
            return
        await self._sync_stop(ctx)
        if ctx.closed:
            return
        have = bp.qty if bp is not None else 0
        if have >= ctx.mp.qty_open:
            if have > ctx.mp.qty_open:
                self._ev("WARNING", "EXTRA_SHARES", f"{ctx.symbol}: broker holds {have}, ledger {ctx.mp.qty_open}; "
                         "extra shares left untouched", position_id=ctx.position_id)
            return
        missing = ctx.mp.qty_open - have
        price = self._last.get(ctx.symbol, ctx.mp.entry_price)
        self._ev("ERROR", "STATE_CORRUPT", f"{ctx.symbol}: ledger {ctx.mp.qty_open} vs broker {have}; "
                 f"recording {missing} sold at {price:.2f} (last known)", position_id=ctx.position_id)
        if have == 0 and ctx.stop_live:
            try:
                await self.broker.cancel_order(ctx.stop_bid)  # type: ignore[arg-type]
            except BrokerError:
                pass
            ctx.stop_live = False
        self._synthetic_exit(ctx, missing, price, ExitReason.STATE_CORRUPT)
        await self._apply_leg(ctx, missing, price, ExitReason.STATE_CORRUPT)
        if not ctx.closed:
            try:
                await self._ensure_stop(ctx)
            except BrokerError as e:
                self._ev("CRITICAL", "STOP_FAILED", f"{ctx.symbol}: unprotected ({e})", position_id=ctx.position_id)

    async def _sync_db_orders(self, position_id: int) -> None:
        rows = self.db.query("SELECT client_order_id FROM orders WHERE position_id=? AND status NOT IN (%s)"
                             % ",".join("?" * len(_FINAL)), (position_id, *_FINAL))
        for r in rows:
            try:
                bo = await self.broker.get_order_by_client_id(r["client_order_id"])
            except BrokerError:
                continue
            if bo is not None:
                self._progress(r["client_order_id"], bo)

    def _rehydrate(self, row: dict) -> _Pos:
        legs = self.db.query("SELECT * FROM orders WHERE position_id=? AND purpose IN ('STOP','PARTIAL','EXIT') "
                             "AND filled_qty>0", (row["id"],))
        sold = sum(int(r["filled_qty"]) for r in legs)
        side = Side(row["side"])
        mp = ManagedPosition(
            side=side, qty_open=max(0, int(row["qty_initial"]) - sold), qty_initial=int(row["qty_initial"]),
            entry_price=row["entry_price"], stop_price=row["stop_price"], risk_per_share=row["risk_per_share"],
            opened_at=parse_iso(row["opened_at"]), params=self.params,
            highest_since_entry=row["highest_since_entry"], lowest_since_entry=row["lowest_since_entry"],
            trail_price=row["trail_price"], partial_taken=bool(row["partial_taken"]) or
            any(r["purpose"] == "PARTIAL" for r in legs), mfe_r=row["mfe_r"] or 0.0, mae_r=row["mae_r"] or 0.0)
        if mp.qty_open == 0:
            mp.closed = True
        ctx = _Pos(row["id"], row["setup_id"], row["symbol"], side, mp, row["strategy_version"])
        ctx.realized = sum(r["filled_qty"] * (r["filled_avg_price"] - mp.entry_price) for r in legs) * _dir(side)
        ctx.persisted = self._snap(ctx)
        return ctx

    def _is_stop_of(self, cid: str, setup_id: int) -> bool:
        m = _NW.match(cid)
        return bool(m and m.group(1) in (None, self.uid) and int(m.group(2)) == setup_id and m.group(3) == "STOP")

    async def _settle(self, cids: list[str]) -> dict[str, BrokerOrder | None]:
        """Poll orders (by client id) until each is terminal or gone; bounded by RECONCILE_WAIT_N polls."""
        final: dict[str, BrokerOrder | None] = {}
        todo = list(cids)
        for i in range(RECONCILE_WAIT_N):
            still = []
            for cid in todo:
                try:
                    bo = await self.broker.get_order_by_client_id(cid)
                except BrokerError:
                    still.append(cid)
                    continue
                final[cid] = bo
                if bo is not None and not is_terminal(bo.status):
                    still.append(cid)
            todo = still
            if not todo:
                break
            await asyncio.sleep(RECONCILE_POLL_S)
        for cid in todo:
            self._ev("WARNING", "RECONCILE_SETTLE_TIMEOUT", f"{cid}: not terminal after the wait", client_order_id=cid)
        return final

    async def _close_orphan(self, sym: str, bp: BrokerPosition, setup_id: int) -> None:
        """Flatten an orphan NewsWave position with a TRACKED nw- market order (never the untracked
        close_position): the order row exists before the broker sees it, so a crash here stays explainable."""
        side = "sell" if bp.side == Side.LONG else "buy"
        cid = self._cid(setup_id, "EXIT", self._next_seq(setup_id, "EXIT"))
        oid = self._insert_order(cid, setup_id, None, sym, side, "market", bp.qty, None, None, OrderPurpose.EXIT,
                                 reason=ExitReason.STATE_CORRUPT)
        try:
            bo = await self.broker.submit_market(sym, side, bp.qty, cid)
        except BrokerError as e:  # includes OrderOutcomeUnknown: a later boot's reconcile sees the tracked row
            self.db.update("orders", oid, {"status": "unknown" if isinstance(e, OrderOutcomeUnknown) else "rejected",
                                           "error": str(e)})
            self._ev("CRITICAL", "STATE_CORRUPT", f"{sym}: orphan position could NOT be flattened: {e}", symbol=sym)
            return
        self._ack(cid, bo)
        fo = (await self._settle([cid])).get(cid) or bo
        self._progress(cid, fo)
        self._ev("CRITICAL", "STATE_CORRUPT", f"{sym}: orphan NewsWave position of {bp.qty} flattened "
                 f"({cid}: {fo.status})", symbol=sym, qty=bp.qty, client_order_id=cid)
        self._tl(sym, "RECONCILE", f"orphan {sym} position flattened (STATE_CORRUPT)")

    async def reconcile_on_boot(self) -> None:
        async with self._lock:
            now = self._now()
            self._roll(now)
            self._refresh_realized()
            try:
                bps = {p.symbol: p for p in await self.broker.get_positions()}
                open_orders = await self.broker.get_open_orders()
            except BrokerError as e:
                self._ev("CRITICAL", "RECONCILE_FAILED", f"cannot read broker state: {e}")
                raise
            nw_open = {o.client_order_id: o for o in open_orders if o.client_order_id.startswith("nw-")}
            for cid_, o_ in nw_open.items():
                m_ = _NW.match(cid_)
                if m_ and m_.group(1) in (None, self.uid):
                    k_ = (int(m_.group(2)), m_.group(3))
                    self._seq_floor[k_] = max(self._seq_floor.get(k_, 0), int(m_.group(4)))
            known_cids = {r["client_order_id"] for r in self.db.query("SELECT client_order_id FROM orders")}
            unknown_syms = {o.symbol for c, o in nw_open.items() if c not in known_cids}
            orphan_setup: dict[str, int] = {}  # symbol -> setup id to book the tracked close under
            for c, o in nw_open.items():
                if c not in known_cids and (m_ := _NW.match(c)):
                    orphan_setup[o.symbol] = int(m_.group(2))
            keep: set[str] = set()
            claimed: set[str] = set()

            # 1) DB says open
            for row in self.db.query("SELECT * FROM positions WHERE status='OPEN' AND is_shadow=0"):
                await self._sync_db_orders(row["id"])
                ctx = self._rehydrate(row)
                claimed.add(row["symbol"])
                if ctx.mp.closed:  # every share was already sold per recorded fills
                    self._pos[ctx.position_id] = ctx
                    await self._close_position(ctx)
                    self._ev("WARNING", "RECONCILE_CLOSED", f"{ctx.symbol}: position closed by recorded fills",
                             position_id=ctx.position_id)
                    continue
                stops = sorted((o for c, o in nw_open.items() if self._is_stop_of(c, ctx.setup_id)),
                               key=lambda o: int(o.client_order_id.rsplit("-", 1)[1]))
                if stops and ctx.mp.qty_open:
                    s = stops[-1]
                    ctx.stop_cid, ctx.stop_bid, ctx.stop_qty, ctx.stop_live = (
                        s.client_order_id, s.broker_order_id, s.qty, True)
                    keep.add(s.client_order_id)
                    if self._order_row(s.client_order_id) is None:  # broker-only stop: record it so its fill is seen
                        self._insert_order(s.client_order_id, ctx.setup_id, ctx.position_id, ctx.symbol,
                                           _opp(ctx.side), "stop", s.qty, None, s.stop_price, OrderPurpose.STOP,
                                           version=ctx.version)
                        self._ack(s.client_order_id, s)
                    for extra in stops[:-1]:
                        try:
                            await self.broker.cancel_order(extra.broker_order_id)
                            self._ev("WARNING", "RECONCILE_DUP_STOP", f"cancelled duplicate {extra.client_order_id}")
                        except BrokerError:
                            pass
                self._pos[ctx.position_id] = ctx
                bp = bps.get(ctx.symbol)
                await self._repair(ctx, bp)
                if ctx.closed:
                    self._ev("WARNING", "RECONCILE_CLOSED", f"{ctx.symbol}: broker flat; exit recorded",
                             position_id=ctx.position_id)
                    continue
                if not ctx.stop_live or ctx.stop_qty != ctx.mp.qty_open:
                    try:
                        was = "missing" if not ctx.stop_live else "wrong qty"
                        await self._ensure_stop(ctx)
                        self._ev("WARNING", "RECONCILE_STOP", f"{ctx.symbol}: stop {was}; re-placed for "
                                 f"{ctx.mp.qty_open}", position_id=ctx.position_id)
                    except BrokerError as e:
                        self._ev("CRITICAL", "STOP_FAILED", f"{ctx.symbol}: unprotected ({e})",
                                 position_id=ctx.position_id)
                self.subs.request(ctx.symbol, f"pos-{ctx.position_id}", SlotPriority.POSITION, 1.0, now)
                self._tl(ctx.symbol, "RECONCILE", f"recovered {ctx.side} {ctx.mp.qty_open} {ctx.symbol} "
                         f"@ {ctx.mp.entry_price:.2f}", ctx.setup_id)
                self._ev("INFO", "RECONCILE_REHYDRATED", f"{ctx.symbol}: {ctx.mp.qty_open} shares",
                         position_id=ctx.position_id)

            # 2) evidence of our own orphan fills: an ENTRY row with no position whose order actually filled
            for r in self.db.query("SELECT * FROM orders WHERE purpose='ENTRY' AND position_id IS NULL AND status!='orphan' "
                                   "AND (filled_qty>0 OR status NOT IN ('rejected','canceled','expired'))"):
                try:
                    bo = await self.broker.get_order_by_client_id(r["client_order_id"])
                except BrokerError:
                    continue
                if bo is not None and bo.filled_qty > 0 and r["symbol"] not in claimed:
                    unknown_syms.add(r["symbol"])
                    orphan_setup[r["symbol"]] = int(r["setup_id"] or 0)
                    self.db.update("orders", r["id"], {"status": "orphan", "filled_qty": bo.filled_qty,
                                                       "filled_avg_price": bo.filled_avg_price})

            # 3) every other open nw- order is cancelled (entries never survive a restart). Alpaca cancels are
            #    async (pending_cancel; a fill can still land): wait for a terminal state before judging positions
            to_cancel = {c: o for c, o in nw_open.items() if c not in keep}
            for cid, o in to_cancel.items():
                try:
                    await self.broker.cancel_order(o.broker_order_id)
                except BrokerError as e:  # probably filled meanwhile; the lookup below tells
                    self._ev("ERROR", "RECONCILE_CANCEL_FAILED", f"{cid}: {e}")
            final = await self._settle(list(to_cancel))
            for cid, o in to_cancel.items():
                fo = final.get(cid)
                row = self._order_row(cid)
                m_ = _NW.match(cid)
                opened = fo is not None and fo.filled_qty > 0 and (
                    (m_ is not None and m_.group(3) == "ENTRY") or (m_ is None and o.order_type == "limit"))
                if opened and o.symbol not in claimed:  # the "cancelled" entry filled first: it is an orphan position
                    unknown_syms.add(o.symbol)
                    orphan_setup.setdefault(o.symbol, int(m_.group(2)) if m_ else int(row["setup_id"] or 0) if row else 0)
                if row:
                    self.db.update("orders", row["id"], {"status": "orphan" if opened and o.symbol not in claimed
                                                         else (fo.status if fo and is_terminal(fo.status)
                                                               else "canceled")})
                    s = self._setup(row["setup_id"])
                    if row["purpose"] == "ENTRY" and s.get("stage") == str(Stage.ENTRY_SIGNAL):
                        self.db.update("setups", row["setup_id"], {
                            "stage": str(Stage.REJECTED), "reject_reason": str(RejectReason.ENTRY_NOT_FILLED),
                            "updated_at": utc_iso(now), "closed_at": utc_iso(now)})
                self._ev("WARNING", "RECONCILE_ORDER_CANCELLED",
                         f"{cid} {'unknown to DB' if cid not in known_cids else 'not needed'}; cancelled",
                         client_order_id=cid)
            if to_cancel:  # cancels may have moved positions: decide on what the broker holds NOW
                try:
                    bps = {p.symbol: p for p in await self.broker.get_positions()}
                except BrokerError as e:
                    self._ev("CRITICAL", "RECONCILE_FAILED", f"cannot re-read positions after cancels: {e}")
                    raise

            # 4) broker positions nobody owns
            for sym, bp in bps.items():
                if sym in claimed:
                    continue
                if sym in unknown_syms:
                    await self._close_orphan(sym, bp, orphan_setup.get(sym, 0))
                else:
                    self._foreign[sym] = bp.qty
                    self._ev("WARNING", "FOREIGN_POSITION", f"{sym}: {bp.qty} shares not opened by NewsWave; "
                             "left untouched and excluded from the ledger", symbol=sym, qty=bp.qty)

            # 5) session state
            if self._pos and self.kill_switch.is_disabled(self._session(now)):
                self._tripped_day = self._kill_day_checked = self._session(now)
                await self._flatten(ExitReason.RISK_KILL)
            elif self._pos and now >= effective_eod(now, self.params):
                await self._flatten(ExitReason.EOD)
            await self._snapshot("reconcile")
