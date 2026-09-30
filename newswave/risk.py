"""Risk engine, sizing, kill switch, PDT counting (SPEC §§16-18, CONTRACT §10). Pure apart from KillSwitch/count_day_trades (db).

Equity convention: every % calculation uses min(ledger_equity, broker_equity) -- the more
conservative of our ledger and the broker's number.
Entry convention: stop and size are computed from the marketable LIMIT price (trigger +/- slippage),
i.e. the worst acceptable fill, so realised risk never exceeds the sized risk.
"""
from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import date, datetime

from .clock import effective_cutoff, parse_iso, to_et, utc_iso
from .db import Database
from .config import StrategyParams
from .models import _RISK_TOKEN, EntrySignal, RejectReason, RiskApproval, Side

_EPS = 1e-9  # floor() guard against 59.999999 from float division
PDT_EQUITY_THRESHOLD = 25_000.0
PDT_MAX_DAY_TRADES = 3


def _floor(x: float) -> int:
    return max(0, math.floor(x + _EPS))


def compute_stop(side: Side, entry: float, pullback_low: float, pullback_high: float,
                 atr: float, min_stop_atr: float) -> float:
    if atr <= 0:
        raise ValueError("atr must be > 0")
    if entry <= 0:
        raise ValueError("entry must be > 0")
    if side == Side.LONG:
        return min(pullback_low, entry - min_stop_atr * atr)
    return max(pullback_high, entry + min_stop_atr * atr)


def size_position(equity: float, entry: float, stop: float, buying_power: float,
                  params: StrategyParams) -> int:
    """Whole shares; 0 on any non-positive input or zero risk/share."""
    rps = abs(entry - stop)
    if equity <= 0 or entry <= 0 or rps <= 0 or buying_power <= 0:
        return 0
    by_risk = _floor(params.risk_per_trade_pct / 100 * equity / rps)
    by_size = _floor(params.max_position_pct / 100 * equity / entry)
    by_bp = _floor(buying_power / entry)
    return min(by_risk, by_size, by_bp)


@dataclass
class AccountState:
    ledger_equity: float
    broker_equity: float
    buying_power: float
    open_risk_dollars: float  # production positions only
    open_positions: int  # open production positions + pending entries
    day_trades_5_sessions: int
    kill_switch_active: bool
    now: datetime  # UTC


class RiskEngine:
    def __init__(self, params: StrategyParams) -> None:
        self.params = params

    def evaluate(self, signal: EntrySignal, account: AccountState) -> RiskApproval | RejectReason:
        p = self.params
        if signal.variant != "production":
            raise ValueError(f"shadow signal reached risk: variant={signal.variant!r}")
        if account.kill_switch_active:
            return RejectReason.KILL_SWITCH
        if account.now >= effective_cutoff(account.now, p):
            return RejectReason.AFTER_CUTOFF
        if account.open_positions >= p.max_concurrent_positions:
            return RejectReason.MAX_POSITIONS
        # every open / pending intraday position will become a day trade too (EOD flatten)
        if (p.pdt_mode == "auto" and account.broker_equity < PDT_EQUITY_THRESHOLD
                and account.day_trades_5_sessions + account.open_positions >= PDT_MAX_DAY_TRADES):
            return RejectReason.PDT_LIMIT

        slip = p.entry_slippage_atr * signal.atr
        limit = signal.trigger_price + slip if signal.side == Side.LONG else signal.trigger_price - slip
        stop = compute_stop(signal.side, limit, signal.pullback_low, signal.pullback_high,
                            signal.atr, p.min_stop_atr)
        rps = abs(limit - stop)
        equity = min(account.ledger_equity, account.broker_equity)

        qty = size_position(equity, limit, stop, 1e15, p)  # buying power checked below, for its own reason
        if qty == 0:
            return RejectReason.SIZE_ZERO
        if account.buying_power < limit:
            return RejectReason.BUYING_POWER
        qty = min(qty, _floor(account.buying_power / limit))

        room = p.max_concurrent_risk_pct / 100 * equity - account.open_risk_dollars
        if qty * rps > room + _EPS:
            qty = min(qty, _floor(room / rps))
            if qty == 0:
                return RejectReason.MAX_CONCURRENT_RISK

        return RiskApproval(signal=signal, qty=qty, entry_price=signal.trigger_price,
                            limit_price=limit, stop_price=stop, risk_per_share=rps,
                            risk_dollars=qty * rps, token=_RISK_TOKEN)


def daily_loss_breached(start_of_day_equity: float, realized_today: float, unrealized: float,
                        max_daily_loss_pct: float) -> bool:
    """Loss >= pct x start equity (inclusive)."""
    if start_of_day_equity <= 0:
        return False
    return -(realized_today + unrealized) >= max_daily_loss_pct / 100 * start_of_day_equity - _EPS


class KillSwitch:
    """Persisted per session in daily_stats (account-level). No reset exists: a new session_date is a new day."""

    def __init__(self, db: Database, strategy_version: str) -> None:
        self.db = db
        self.version = strategy_version

    def is_disabled(self, session_date: date) -> bool:
        """Account-level: a trip under ANY strategy version disables the whole session."""
        return self.db.one("SELECT 1 x FROM daily_stats WHERE session_date=? AND trading_disabled=1",
                           (session_date.isoformat(),)) is not None

    def trip(self, session_date: date, ts: datetime, reason: str = "") -> None:
        if self.is_disabled(session_date):
            return  # keep the first trip time
        self.db.upsert("daily_stats", {
            "session_date": session_date.isoformat(), "strategy_version": self.version,
            "trading_disabled": 1, "kill_switch_at": utc_iso(ts)},
            ["session_date", "strategy_version"])
        self.db.insert("system_events", {
            "ts": utc_iso(ts), "level": "ERROR", "component": "risk", "event": "KILL_SWITCH",
            "message": reason, "data_json": None})


def count_day_trades(db: Database, strategy_version: str, sessions: list[date]) -> int:
    """Production round trips whose entry and exit fall on the same ET date, within `sessions`."""
    wanted = set(sessions)
    n = 0
    for r in db.query("SELECT entry_at, exit_at FROM trades WHERE is_shadow=0 AND strategy_version=? "
                      "AND entry_at IS NOT NULL AND exit_at IS NOT NULL", (strategy_version,)):
        d = to_et(parse_iso(r["exit_at"])).date()
        if d in wanted and to_et(parse_iso(r["entry_at"])).date() == d:
            n += 1
    return n
