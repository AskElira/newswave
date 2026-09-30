"""SetupMachine: pure per-(candidate, variant) state machine (CONTRACT §9). No db, no io, no clock reads.

LONG is written once; SHORT mirrors it through the sign `s` (+1 long, -1 short): every price
comparison is done on `s * price`, so "higher" always means "more favourable".
  LONG : impulse up, pullback dips toward EMA9, pullback_high/low = max high/min low of pullback bars,
         trigger = pullback_high + buffer*ATR, entry on a print ABOVE trigger.
  SHORT: impulse down, rally up toward EMA9 (same pullback_high/low definitions, so pullback_high is the
         stop reference), trigger = pullback_low - buffer*ATR, entry on a print BELOW trigger.
Decisions where the contract is silent are marked `decision:`.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta

from ..clock import before_cutoff, parse_iso, utc_iso
from ..config import StrategyParams
from ..models import Bar, EntrySignal, RejectReason, Side, Stage, Trade

_EPS = 1e-9
_BAR = timedelta(minutes=5)


@dataclass
class Transition:
    stage: Stage
    reject_reason: RejectReason | None = None
    message: str = ""
    data: dict = field(default_factory=dict)


class SetupMachine:
    def __init__(self, setup_id: int, symbol: str, side: Side, variant: str, params: StrategyParams,
                 news_received_at: datetime, ref_price: float, *, rvol_min: float | None = None,
                 pullback_number: int = 1) -> None:
        self.setup_id, self.symbol, self.side, self.variant = setup_id, symbol, side, variant
        self.params, self.news_received_at, self.ref_price = params, news_received_at, ref_price
        self.rvol_min = params.rvol_min if rvol_min is None else rvol_min
        self.pullback_number = pullback_number
        self._s = 1 if side == Side.LONG else -1
        self.stage = Stage.WAITING_FOR_VOLUME
        self.max_stage = Stage.WAITING_FOR_VOLUME
        self.reject_reason: RejectReason | None = None
        self.signal: EntrySignal | None = None
        self.signal_price: float | None = None  # the triggering print
        # numbers
        self.rvol: float | None = None
        self.max_rvol_seen = 0.0
        self.impulse_pct: float | None = None
        self.impulse_extreme: float | None = None
        self.pullback_high: float | None = None
        self.pullback_low: float | None = None
        self.pullback_bars: int | None = None
        self.ema9_at_touch: float | None = None
        self.atr: float | None = None
        self.entry_trigger: float | None = None
        # internals
        self._n = 0                      # counted bars (bucket ends after the news)
        self._rvol_met = False
        self._ext = ref_price            # running favourable extreme since news
        self._pb: list[Bar] = []
        self._touched = False
        self._pb_done = 0                # pullbacks already broken out of and skipped (second_pullback)
        self._ignore_until: datetime | None = None  # bars that started at/before this are the breakout bar
        self._outbox: list[Transition] = []

    # ---- introspection -------------------------------------------------
    @property
    def done(self) -> bool:
        return self.stage in (Stage.REJECTED, Stage.ENTRY_SIGNAL)

    @property
    def numbers(self) -> dict:
        return {"rvol": self.rvol, "impulse_pct": self.impulse_pct, "impulse_extreme": self.impulse_extreme,
                "pullback_high": self.pullback_high, "pullback_low": self.pullback_low,
                "pullback_bars": self.pullback_bars, "ema9_at_touch": self.ema9_at_touch, "atr": self.atr,
                "entry_trigger": self.entry_trigger, "max_rvol_seen": self.max_rvol_seen}

    def drain(self) -> list[Transition]:
        """Transitions produced inside on_trade (which can only return the signal)."""
        out, self._outbox = self._outbox, []
        return out

    # ---- helpers --------------------------------------------------------
    def _go(self, stage: Stage, message: str, **extra: object) -> Transition:
        self.stage = stage
        if stage.rank > self.max_stage.rank:
            self.max_stage = stage
        return Transition(stage, None, message, {**self.numbers, **extra})

    def _info(self, message: str, **extra: object) -> Transition:
        return Transition(self.stage, None, message, {**self.numbers, **extra})

    def reject(self, reason: RejectReason, detail: str = "") -> Transition:
        self.stage, self.reject_reason = Stage.REJECTED, reason
        return Transition(Stage.REJECTED, reason, f"{self.symbol} rejected: {reason}{detail}", self.numbers)

    def _flush(self, out: list[Transition]) -> list[Transition]:
        return self.drain() + out

    def _fav(self, bar: Bar) -> float:
        return bar.high if self._s > 0 else bar.low

    def _adv(self, bar: Bar) -> float:
        return bar.low if self._s > 0 else bar.high

    def _extend(self, price: float) -> bool:
        if self._s * (price - self._ext) > 0:
            self._ext = price
            self.impulse_extreme = price
            self.impulse_pct = self._s * (price - self.ref_price) / self.ref_price * 100
            return True
        return False

    # ---- bars -------------------------------------------------------------
    def on_bar(self, bar: Bar, ind: dict, baseline_volume: float | None) -> list[Transition]:
        if self.done:
            return self._flush([])
        start = parse_iso(bar.start)
        if start + _BAR <= self.news_received_at:
            return self._flush([])  # completed before the news: never counts
        self._n += 1
        rv = bar.volume / baseline_volume if baseline_volume and baseline_volume > 0 else None
        if rv is not None and self._n <= self.params.impulse_max_bars:
            self.max_rvol_seen = max(self.max_rvol_seen, rv)  # keeps measuring after qualification (rvol_1_5 test)
        if self.stage in (Stage.WAITING_FOR_VOLUME, Stage.WAITING_FOR_IMPULSE):
            return self._flush(self._volume_impulse(bar, rv))
        atr = ind.get("atr") or ind.get("atr14")
        ema = ind.get("ema9")
        if ema is None or not atr or atr <= 0:
            # indicators not warm: the pullback can never be judged, so fail now instead of stalling to SETUP_EXPIRED
            return self._flush([self.reject(RejectReason.NO_MARKET_DATA,
                                            f" (EMA9/ATR14 not warm: ema9={ema}, atr={atr})")])
        self.atr = atr
        return self._flush(self._pullback_bar(bar, start, ema, atr))

    def _volume_impulse(self, bar: Bar, rv: float | None) -> list[Transition]:
        p, out = self.params, []
        self._extend(self._fav(bar))
        if self.impulse_pct is None:
            self.impulse_extreme, self.impulse_pct = self._ext, 0.0
        if not self._rvol_met:
            if rv is not None:
                self.rvol = rv
            if rv is not None and rv >= self.rvol_min - _EPS:
                self._rvol_met = True
                out.append(self._go(Stage.WAITING_FOR_IMPULSE, f"RVOL {rv:.1f}"))
        if self._rvol_met and self.impulse_pct >= p.min_impulse_pct - _EPS:
            self._pb, self._touched = [], False
            out.append(self._go(Stage.WAITING_FOR_PULLBACK, f"price impulse {self.impulse_pct:+.1f}%"))
        elif self._n >= p.impulse_max_bars:
            out.append(self.reject(RejectReason.NO_MOMENTUM if self._rvol_met else RejectReason.NO_VOLUME))
        return out

    def _pullback_bar(self, bar: Bar, start: datetime, ema: float, atr: float) -> list[Transition]:
        p, s = self.params, self._s
        if self._ignore_until is not None and start <= self._ignore_until:
            self._extend(self._fav(bar))  # the bar that contained the skipped breakout: impulse, not pullback
            return []
        adv = self._adv(bar)
        if s * (adv - self.ref_price) <= 0:
            return [self.reject(RejectReason.IMPULSE_LOST)]
        if s * (bar.close - ema) < -p.pullback_collapse_atr * atr:
            return [self.reject(RejectReason.PULLBACK_COLLAPSE)]
        if bar.volume <= 0:
            return [self.reject(RejectReason.VOLUME_DRIED_UP)]
        if self._extend(self._fav(bar)):  # new extreme
            if self._touched:
                return []  # breakout stage: trigger untouched, the next print decides
            self._pb, self.pullback_bars = [], None
            return [self._info(f"impulse extends {self.impulse_pct:+.1f}%")]
        self._pb.append(bar)
        n = len(self._pb)
        if self._touched:
            if n > p.pullback_max_bars:  # decision: a 5th pullback bar, touch or not, ends the setup
                return [self.reject(RejectReason.NO_PULLBACK)]
            self._set_pullback()
            return [self._info(f"breakout trigger {self.entry_trigger:.2f} (pullback extended)")]
        if s * adv <= s * ema + p.ema_touch_tolerance_atr * atr + _EPS:
            self._touched = True
            self.ema9_at_touch = ema
            self._set_pullback()
            return [self._go(Stage.WAITING_FOR_BREAKOUT, "9 EMA pullback confirmed")]
        if n > p.pullback_max_bars:
            return [self.reject(RejectReason.NO_PULLBACK)]
        self.pullback_bars = n
        return [self._info("waiting for 9 EMA")]

    def _set_pullback(self) -> None:
        self.pullback_high = max(b.high for b in self._pb)
        self.pullback_low = min(b.low for b in self._pb)
        self.pullback_bars = len(self._pb)
        buf = self.params.breakout_buffer_atr * (self.atr or 0.0)
        self.entry_trigger = self.pullback_high + buf if self._s > 0 else self.pullback_low - buf

    # ---- trades -----------------------------------------------------------
    def on_trade(self, trade: Trade) -> EntrySignal | None:
        if self.stage not in (Stage.WAITING_FOR_PULLBACK, Stage.WAITING_FOR_BREAKOUT):
            return None
        ts, px, s = parse_iso(trade.ts), trade.price, self._s
        if ts < self.news_received_at:
            return None
        if s * (px - self.ref_price) <= 0:
            self._outbox.append(self.reject(RejectReason.IMPULSE_LOST))
            return None
        if self.stage != Stage.WAITING_FOR_BREAKOUT or self.entry_trigger is None:
            return None
        if self._expired(ts) or not before_cutoff(ts, self.params.no_new_entries_after):
            return None  # on_clock rejects; a late print never signals
        if s * (px - self.entry_trigger) <= 0:
            return None
        if self.pullback_number == 2 and self._pb_done == 0:
            first = {"trigger": self.entry_trigger, "print": px}
            self._pb_done, self._pb, self._touched, self._ignore_until = 1, [], False, ts
            self.pullback_high = self.pullback_low = self.pullback_bars = None
            self.ema9_at_touch = self.entry_trigger = None
            self._outbox.append(self._go(Stage.WAITING_FOR_PULLBACK,
                                         "first pullback broke out, waiting for the 2nd", first=first))
            return None
        self.signal_price = px
        self.signal = EntrySignal(self.setup_id, self.symbol, self.side, self.entry_trigger,
                                  self.pullback_low, self.pullback_high, self.atr or 0.0,
                                  utc_iso(ts), self.variant)
        self._outbox.append(self._go(Stage.ENTRY_SIGNAL, "breakout triggered",
                                     print_price=px, trigger=self.entry_trigger))
        return self.signal

    # ---- clock ------------------------------------------------------------
    def _expired(self, now: datetime) -> bool:
        return now - self.news_received_at >= timedelta(minutes=self.params.setup_expiration_minutes)

    def on_clock(self, now: datetime) -> list[Transition]:
        if self.done:
            return self._flush([])
        if not before_cutoff(now, self.params.no_new_entries_after):
            return self._flush([self.reject(RejectReason.AFTER_CUTOFF)])
        if self._expired(now):
            return self._flush([self.reject(RejectReason.SETUP_EXPIRED)])
        return self._flush([])
