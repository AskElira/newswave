"""Shadow variants (SPEC §30, CONTRACT §9) and the hypothetical-trade book.

Nothing here ever reaches execution or risk: ShadowBook has no callback to either, it only writes
`positions` / `trades` rows with is_shadow=1 and moves the variant's `setups` row to IN_POSITION/CLOSED.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Callable

from ..clock import Clock, parse_iso, utc_iso
from ..config import StrategyParams
from ..db import Database
from ..exits import ManagedPosition
from ..models import Bar, EntrySignal, ExitReason, RejectReason, Side, Stage, Trade
from ..news.intake import Candidate
from ..risk import compute_stop, size_position
from ..timeline import Timeline

STARTING_EQUITY = 6000.0  # nominal, every shadow trade is sized on this with unlimited buying power
_UNLIMITED_BP = 1e15
_AI_REJECTS = {RejectReason.AI_NEUTRAL, RejectReason.AI_BEARISH, RejectReason.AI_LOW_CONFIDENCE,
               RejectReason.AI_NOT_MATERIAL}


@dataclass(frozen=True)
class ShadowVariant:
    name: str
    description: str
    rvol_min: float | None = None   # None = production RVOL_MIN
    shadow_rvol: bool = False       # True: threshold is params.shadow_rvol_min
    pullback_number: int = 1
    standalone: bool = False        # True: spawned INSTEAD of a production machine (news the gate refused)


SHADOW_VARIANTS: dict[str, ShadowVariant] = {v.name: v for v in (
    ShadowVariant("neutral_news", "NEUTRAL news treated as LONG", standalone=True),
    ShadowVariant("bearish_short", "BEARISH material news as SHORT while ALLOW_SHORTS=false", standalone=True),
    ShadowVariant("low_confidence", "BULLISH material news, confidence low_conf_floor to ai_min_confidence",
                  standalone=True),
    ShadowVariant("rvol_1_5", "production params with RVOL_MIN=shadow_rvol_min (only max rvol in [shadow_rvol_min, rvol_min))",
                  shadow_rvol=True),
    ShadowVariant("second_pullback", "production setup, trades the 2nd EMA9 pullback", pullback_number=2),
)}


def variant_rvol_min(v: ShadowVariant, params: StrategyParams) -> float | None:
    return params.shadow_rvol_min if v.shadow_rvol else v.rvol_min


def spawn_standalone(c: Candidate, params: StrategyParams) -> list[tuple[ShadowVariant, Side]]:
    """Shadow variants for news the gate refused (no production machine exists for it)."""
    cl = c.classification
    if cl is None or cl.error or c.reject_reason not in _AI_REJECTS:
        return []  # pre-AI rejects (closed market, too old, ...) never spawn research setups
    out: list[tuple[ShadowVariant, Side]] = []
    v = SHADOW_VARIANTS
    if cl.direction == "NEUTRAL":
        out.append((v["neutral_news"], Side.LONG))
    elif (cl.direction == "BEARISH" and cl.material and cl.confidence >= params.ai_min_confidence
          and not params.allow_shorts):
        out.append((v["bearish_short"], Side.SHORT))
    elif (cl.direction == "BULLISH" and cl.material
          and params.low_conf_floor <= cl.confidence < params.ai_min_confidence):
        out.append((v["low_confidence"], Side.LONG))
    return out


def spawn_companions(side: Side, params: StrategyParams) -> list[tuple[ShadowVariant, Side]]:
    """Shadow variants that run alongside every armed production machine."""
    out = [(SHADOW_VARIANTS["second_pullback"], side)]
    if params.rvol_min > params.shadow_rvol_min:
        out.insert(0, (SHADOW_VARIANTS["rvol_1_5"], side))
    return out


@dataclass
class _Shadow:
    setup_id: int
    position_id: int
    symbol: str
    variant: str
    mp: ManagedPosition
    legs: list[tuple[ExitReason, int, float]]
    opened_at: datetime
    last: float


class ShadowBook:
    def __init__(self, db: Database, clock: Clock, timeline: Timeline, params: StrategyParams,
                 strategy_version: str, *, on_close: Callable[[int], None] | None = None) -> None:
        self.db, self.clock, self.timeline, self.params = db, clock, timeline, params
        self.strategy_version, self.on_close = strategy_version, on_close
        self._open: dict[int, _Shadow] = {}  # setup_id -> shadow

    # ---- queries ----
    def open_setup_ids(self) -> set[int]:
        return set(self._open)

    def symbols(self) -> set[str]:
        return {s.symbol for s in self._open.values()}

    # ---- open ----
    def open(self, signal: EntrySignal, print_price: float) -> int | None:
        """Open a hypothetical position at the triggering print. Returns the positions.id, or None (SIZE_ZERO)."""
        if signal.setup_id in self._open:
            return None
        p, now = self.params, self.clock.now()
        entry = print_price
        stop = compute_stop(signal.side, entry, signal.pullback_low, signal.pullback_high,
                            signal.atr, p.min_stop_atr)
        qty = size_position(STARTING_EQUITY, entry, stop, _UNLIMITED_BP, p)
        ts = utc_iso(now)
        if qty <= 0:
            self._finish_row(signal.setup_id, {"stage": str(Stage.REJECTED), "reject_reason": str(RejectReason.SIZE_ZERO)})
            self.timeline.log(signal.symbol, Stage.REJECTED, f"shadow {signal.variant} size zero", signal.setup_id)
            self._closed(signal.setup_id)
            return None
        rps = abs(entry - stop)
        mp = ManagedPosition(signal.side, qty, qty, entry, stop, rps, parse_iso(signal.signal_at), p)
        pid = self.db.insert("positions", {
            "setup_id": signal.setup_id, "symbol": signal.symbol, "side": str(signal.side), "is_shadow": 1,
            "variant": signal.variant, "qty_initial": qty, "qty_open": qty, "entry_price": entry,
            "stop_price": stop, "risk_per_share": rps, "highest_since_entry": entry,
            "lowest_since_entry": entry, "trail_price": None, "partial_taken": 0, "mfe_r": 0.0,
            "mae_r": 0.0, "status": "OPEN", "opened_at": signal.signal_at,
            "strategy_version": self.strategy_version})
        self.db.update("setups", signal.setup_id, {
            "stage": str(Stage.IN_POSITION), "max_stage": str(Stage.IN_POSITION), "stop_price": stop,
            "updated_at": ts})
        self._open[signal.setup_id] = _Shadow(signal.setup_id, pid, signal.symbol, signal.variant, mp, [],
                                              parse_iso(signal.signal_at), entry)
        verb = "BUY" if signal.side == Side.LONG else "SELL SHORT"
        self.timeline.log(signal.symbol, Stage.IN_POSITION,
                          f"shadow {signal.variant}: hypothetical {verb} {qty} {signal.symbol} @ {entry:.2f}",
                          signal.setup_id, qty=qty, entry=entry, stop=stop, variant=signal.variant)
        return pid

    # ---- feeds ----
    def on_trade(self, t: Trade) -> None:
        ts = parse_iso(t.ts)
        for sh in [s for s in self._open.values() if s.symbol == t.symbol]:
            sh.last = t.price
            self._apply(sh, sh.mp.on_trade(t.price, ts), ts)

    def on_bar(self, bar: Bar, atr: float | None) -> None:
        now = self.clock.now()
        for sh in [s for s in self._open.values() if s.symbol == bar.symbol]:
            sh.last = bar.close
            self._apply(sh, sh.mp.on_bar_close(bar, atr or 0.0, now), now, persist=True)

    def on_clock(self, now: datetime) -> None:
        for sh in list(self._open.values()):
            self._apply(sh, sh.mp.on_clock(now), now)

    def opposite_news(self, symbol: str, new_side: Side, now: datetime) -> None:
        for sh in [s for s in self._open.values() if s.symbol == symbol and s.mp.side != new_side]:
            if sh.mp.closed:
                continue
            q = sh.mp.qty_open
            sh.mp.apply_exit_fill(q, sh.last)
            sh.legs.append((ExitReason.OPPOSITE_NEWS, q, sh.last))
            self._close(sh, now)

    # ---- internals ----
    def _apply(self, sh: _Shadow, actions: list, ts: datetime, persist: bool = False) -> None:
        for a in actions:
            px = a.price_hint if a.price_hint is not None else sh.last
            sh.mp.apply_exit_fill(a.qty, px)
            sh.legs.append((a.kind, a.qty, px))
            persist = True
            if sh.mp.closed:
                break
        if sh.mp.closed:
            self._close(sh, ts)
        elif persist:
            self._save(sh)

    def _save(self, sh: _Shadow, **extra: object) -> None:
        m = sh.mp
        self.db.update("positions", sh.position_id, {
            "qty_open": m.qty_open, "highest_since_entry": m.highest_since_entry,
            "lowest_since_entry": m.lowest_since_entry, "trail_price": m.trail_price,
            "partial_taken": int(m.partial_taken), "mfe_r": m.mfe_r, "mae_r": m.mae_r, **extra})

    def _close(self, sh: _Shadow, ts: datetime) -> None:
        m, sign = sh.mp, (1 if sh.mp.side == Side.LONG else -1)
        qty = sum(q for _, q, _ in sh.legs)
        pnl = sum(sign * (px - m.entry_price) * q for _, q, px in sh.legs)
        avg_exit = sum(px * q for _, q, px in sh.legs) / qty if qty else m.entry_price
        reason = sh.legs[-1][0]
        risk = m.risk_per_share * m.qty_initial
        self._save(sh, status="CLOSED", qty_open=0, closed_at=utc_iso(ts))
        row = self.db.one("SELECT * FROM setups WHERE id=?", (sh.setup_id,)) or {}
        self.db.insert("trades", {
            "setup_id": sh.setup_id, "position_id": sh.position_id, "symbol": sh.symbol,
            "side": str(m.side), "is_shadow": 1, "variant": sh.variant, "entry_at": utc_iso(sh.opened_at),
            "exit_at": utc_iso(ts), "entry_price": m.entry_price, "avg_exit_price": avg_exit,
            "qty": m.qty_initial, "pnl": pnl, "r_multiple": pnl / risk if risk else None,
            "exit_reason": str(reason), "catalyst": row.get("catalyst"),
            "ai_confidence": row.get("ai_confidence"), "rvol": row.get("rvol"),
            "impulse_pct": row.get("impulse_pct"), "atr": row.get("atr"), "entry_latency_ms": None,
            "news_latency_s": row.get("news_latency_s"), "mfe_r": m.mfe_r, "mae_r": m.mae_r,
            "strategy_version": self.strategy_version})
        self._finish_row(sh.setup_id, {"stage": str(Stage.CLOSED), "max_stage": str(Stage.CLOSED)})
        self.timeline.log(sh.symbol, Stage.CLOSED,
                          f"shadow {sh.variant} closed {reason}: pnl {pnl:+.2f} ({pnl / risk if risk else 0:+.2f}R)",
                          sh.setup_id, pnl=pnl, exit_reason=str(reason))
        del self._open[sh.setup_id]
        self._closed(sh.setup_id)

    def _finish_row(self, setup_id: int, fields: dict) -> None:
        ts = utc_iso(self.clock.now())
        self.db.update("setups", setup_id, {**fields, "updated_at": ts, "closed_at": ts})

    def _closed(self, setup_id: int) -> None:
        if self.on_close:
            self.on_close(setup_id)
