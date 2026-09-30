"""ManagedPosition: pure exit state machine shared by live execution and shadow/replay (CONTRACT §10).

Conventions:
- mfe_r / mae_r are both >= 0 (favourable / adverse excursion in R).
- An emitted full exit (STOP/TRAIL/TIME_STOP/EOD) sets `exit_pending` so later events return []
  until `apply_exit_fill` closes the position; `clear_pending_exit()` re-arms it after a failed exit order.
- PARTIAL_PROFIT is emitted once (partial_taken is set at emission).
- The stop is never changed by any method here (asserted).
"""
from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from .clock import effective_eod, parse_iso, utc_iso
from .config import StrategyParams
from .models import Bar, ExitAction, ExitReason, Side

_EPS = 1e-9


@dataclass
class Fill:
    kind: ExitReason
    qty: int
    price: float
    ts: datetime


@dataclass
class ManagedPosition:
    side: Side
    qty_open: int
    qty_initial: int
    entry_price: float
    stop_price: float
    risk_per_share: float
    opened_at: datetime
    params: StrategyParams
    highest_since_entry: float | None = None
    lowest_since_entry: float | None = None
    trail_price: float | None = None
    partial_taken: bool = False
    mfe_r: float = 0.0
    mae_r: float = 0.0
    closed: bool = False
    exit_pending: bool = False

    def __post_init__(self) -> None:
        if self.risk_per_share <= 0:
            raise ValueError("risk_per_share must be > 0")
        if self.highest_since_entry is None:
            self.highest_since_entry = self.entry_price
        if self.lowest_since_entry is None:
            self.lowest_since_entry = self.entry_price

    # -- helpers
    @property
    def _long(self) -> bool:
        return self.side == Side.LONG

    def _track(self, hi: float, lo: float) -> None:
        self.highest_since_entry = max(self.highest_since_entry, hi)
        self.lowest_since_entry = min(self.lowest_since_entry, lo)
        fav, adv = ((self.highest_since_entry - self.entry_price, self.entry_price - self.lowest_since_entry)
                    if self._long else
                    (self.entry_price - self.lowest_since_entry, self.highest_since_entry - self.entry_price))
        self.mfe_r = max(self.mfe_r, fav / self.risk_per_share)
        self.mae_r = max(self.mae_r, adv / self.risk_per_share)

    def _live(self) -> bool:
        return not (self.closed or self.exit_pending)

    def _full_exit(self, kind: ExitReason, price: float | None) -> list[ExitAction]:
        self.exit_pending = True
        return [ExitAction(kind, self.qty_open, price)]

    def _guard(self, stop_before: float) -> None:
        assert self.stop_price == stop_before, "stop must never change"

    # -- events
    def on_trade(self, price: float, ts: datetime) -> list[ExitAction]:
        if self.closed:
            return []
        s0 = self.stop_price
        self._track(price, price)
        out: list[ExitAction] = []
        if self.exit_pending:
            pass
        elif (price <= self.stop_price) if self._long else (price >= self.stop_price):
            out = self._full_exit(ExitReason.STOP, price)
        elif not self.partial_taken:
            target = self.entry_price + (1 if self._long else -1) * self.params.partial_at_r * self.risk_per_share
            if (price >= target - _EPS) if self._long else (price <= target + _EPS):
                pq = max(1, math.floor(self.qty_initial * self.params.partial_fraction + _EPS))
                self.partial_taken = True  # qty 1 (or pq covering everything): skip the sale, start trailing
                if 1 <= pq < self.qty_open:
                    out = [ExitAction(ExitReason.PARTIAL_PROFIT, pq, price)]
        self._guard(s0)
        return out

    def on_bar_close(self, bar: Bar, atr: float, ts: datetime) -> list[ExitAction]:
        if self.closed:
            return []
        s0 = self.stop_price
        if parse_iso(bar.start) >= self.opened_at:  # the entry bar's pre-entry high/low are not ours (prints cover the rest)
            self._track(bar.high, bar.low)
        out: list[ExitAction] = []
        if self.partial_taken and atr > 0:
            m = self.params.atr_trail_multiplier
            if self._long:
                new = self.highest_since_entry - m * atr
                self.trail_price = new if self.trail_price is None else max(self.trail_price, new)
            else:
                new = self.lowest_since_entry + m * atr
                self.trail_price = new if self.trail_price is None else min(self.trail_price, new)
        if self._live() and self.trail_price is not None and \
                ((bar.close < self.trail_price) if self._long else (bar.close > self.trail_price)):
            out = self._full_exit(ExitReason.TRAIL, bar.close)
        self._guard(s0)
        return out

    def on_clock(self, ts: datetime) -> list[ExitAction]:
        if not self._live():
            return []
        if ts >= effective_eod(ts, self.params):
            return self._full_exit(ExitReason.EOD, None)
        p = self.params
        if (p.time_stop_enabled and (ts - self.opened_at).total_seconds() >= p.time_stop_minutes * 60
                and self.mfe_r < p.time_stop_min_r):
            return self._full_exit(ExitReason.TIME_STOP, None)
        return []

    def clear_pending_exit(self) -> None:
        self.exit_pending = False

    def apply_exit_fill(self, qty: int, price: float) -> None:
        if qty <= 0 or qty > self.qty_open:
            raise ValueError(f"bad exit fill qty {qty} (open {self.qty_open})")
        self.qty_open -= qty
        if self.qty_open == 0:
            self.closed = True
            self.exit_pending = False

    # -- persistence
    def to_dict(self) -> dict[str, Any]:
        return {
            "side": str(self.side), "qty_open": self.qty_open, "qty_initial": self.qty_initial,
            "entry_price": self.entry_price, "stop_price": self.stop_price,
            "risk_per_share": self.risk_per_share, "opened_at": utc_iso(self.opened_at),
            "highest_since_entry": self.highest_since_entry, "lowest_since_entry": self.lowest_since_entry,
            "trail_price": self.trail_price, "partial_taken": self.partial_taken,
            "mfe_r": self.mfe_r, "mae_r": self.mae_r, "closed": self.closed,
            "exit_pending": self.exit_pending}

    @classmethod
    def from_dict(cls, d: dict[str, Any], params: StrategyParams) -> "ManagedPosition":
        d = dict(d)
        d["side"] = Side(d["side"])
        d["opened_at"] = parse_iso(d["opened_at"])
        return cls(params=params, **d)


def simulate(position: ManagedPosition, events: list[tuple]) -> tuple[list[Fill], ExitReason | None]:
    """Replay events through `position`, filling every action immediately.

    events: ("trade", price, ts) | ("bar", Bar, atr, ts) | ("clock", ts).
    Fill price: the event price. A stop print that gapped through the stop therefore fills at the
    (worse) print price, not at the stop -- deliberately pessimistic. Bar-close exits fill at bar.close;
    clock exits (no price) fill at the last seen price (entry price if none).
    Returns (fills, reason of the closing fill) -- reason is None if still open at the end.
    """
    fills: list[Fill] = []
    last = position.entry_price
    reason: ExitReason | None = None
    for ev in events:
        if position.closed:
            break
        kind = ev[0]
        if kind == "trade":
            last = ev[1]
            acts, ts = position.on_trade(ev[1], ev[2]), ev[2]
        elif kind == "bar":
            last = ev[1].close
            acts, ts = position.on_bar_close(ev[1], ev[2], ev[3]), ev[3]
        elif kind == "clock":
            acts, ts = position.on_clock(ev[1]), ev[1]
        else:
            raise ValueError(f"unknown event {kind!r}")
        for a in acts:
            px = a.price_hint if a.price_hint is not None else last
            position.apply_exit_fill(a.qty, px)
            fills.append(Fill(a.kind, a.qty, px, ts))
            if position.closed:
                reason = a.kind
    return fills, reason
