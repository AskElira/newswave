"""StrategyEngine: owns every SetupMachine, routes news/bars/trades, emits EntrySignals (CONTRACT §9).

Production signals go to `on_entry_signal` (execution takes it from there). Shadow signals go ONLY to
the ShadowBook: no code path in this module hands a shadow EntrySignal to a callback.
"""
from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Awaitable, Callable

from ..clock import Clock, parse_iso, to_et, utc_iso
from ..config import StrategyParams
from ..db import Database
from ..market.bars import FiveMinuteBuilder, bucket_start
from ..market.baseline import HistoricalData, build_baseline, warmup_bars
from ..market.indicators import SymbolIndicators
from ..market.subscriptions import SlotPriority, SubscriptionManager
from ..market.universe import AssetCache, check_market, upsert_symbol_row
from ..models import Bar, EntrySignal, RejectReason, Side, Stage, Trade
from ..news.intake import Candidate
from ..timeline import Timeline
from .setup import SetupMachine, Transition
from .shadow import ShadowBook, ShadowVariant, spawn_companions, spawn_standalone, variant_rvol_min

log = logging.getLogger("newswave.strategy")
_HIST5_KEEP = 24  # completed 5m bars remembered per symbol, replayed into machines armed later on the same symbol
_M1_KEEP = timedelta(minutes=30)
_REAL_NEWS = (None, RejectReason.CATALYST_COOLDOWN, RejectReason.AI_BEARISH)  # rejects that still are real news
_NUMBER_COLS = ("rvol", "impulse_pct", "impulse_extreme", "pullback_high", "pullback_low", "pullback_bars",
                "ema9_at_touch", "atr", "entry_trigger")


# ---------------------------------------------------------------------------- market data
@dataclass
class SymbolContext:
    ref_price: float
    last_price: float
    baseline: dict[str, float]
    warm_bars_5m: list[Bar]
    avg_dollar_volume: float
    feed_used: str
    halted: bool = False
    ref_source: str = "latest_trade"   # "1m_open" | "1m_prev_close" | "latest_trade"
    bars_1m: list[Bar] = field(default_factory=list)  # 1m bars from the news bucket start, replayed on arming


class AlpacaMarketData:
    """prepare(symbol): REST history (blocking calls run in a worker thread) -> SymbolContext or a reject reason."""

    def __init__(self, historical: HistoricalData, asset_cache: AssetCache, params: StrategyParams,
                 clock: Clock, *, db: Database | None = None) -> None:
        self.historical, self.asset_cache, self.params, self.clock, self.db = (
            historical, asset_cache, params, clock, db)

    async def prepare(self, symbol: str, news_received_at: datetime) -> SymbolContext | RejectReason:
        try:
            return await asyncio.to_thread(self._prepare, symbol, news_received_at)
        except Exception as e:
            log.warning("market data for %s failed: %s", symbol, type(e).__name__)
            return RejectReason.NO_MARKET_DATA

    async def bars_1m_since(self, symbol: str, start: datetime) -> list[Bar]:
        """REST 1m bars (iex) from `start` on; used to fill the hole after a stream reconnect."""
        return await asyncio.to_thread(self.historical.bars_1m, symbol, start, None, "iex")

    def _note(self, symbol: str, now: datetime, reason: RejectReason | None, **kw: Any) -> None:
        if self.db is not None:
            upsert_symbol_row(self.db, symbol, self.asset_cache.get(symbol), reject_reason=reason, now=now, **kw)

    @staticmethod
    def _ref(m1: list[Bar], news: datetime) -> tuple[float, str] | None:
        floor = news.replace(second=0, microsecond=0)
        for b in m1:
            if parse_iso(b.start) == floor:
                return b.open, "1m_open"
        prev = [b for b in m1 if parse_iso(b.start) < floor]
        return (prev[-1].close, "1m_prev_close") if prev else None

    def _prepare(self, symbol: str, news: datetime) -> SymbolContext | RejectReason:
        p, h = self.params, self.historical
        now = self.clock.now()
        today = to_et(now).date()
        start = now - timedelta(days=int(p.rvol_baseline_days * 1.5) + 10)  # covers N sessions over holidays
        bars = h.bars_5m(symbol, start, None, "iex")
        past = [b for b in bars if to_et(parse_iso(b.start)).date() < today]  # today is never in the baseline
        baseline = build_baseline(past, p.rvol_baseline_days, p.baseline_min_days)
        if baseline is None:
            self._note(symbol, now, RejectReason.NO_MARKET_DATA)
            return RejectReason.NO_MARKET_DATA
        # warm-up = buckets that completed before the news; post-news buckets are replayed from 1m bars
        warm = [b for b in warmup_bars(bars, p.warmup_sessions) if parse_iso(b.start) + timedelta(minutes=5) <= news]
        try:
            m1 = sorted(h.bars_1m(symbol, min(news, now) - timedelta(minutes=10), None, "iex"),
                        key=lambda b: b.start)
        except Exception as e:  # degrade to latest_trade for the reference price, no backfill
            log.warning("1m bars for %s failed: %s", symbol, type(e).__name__)
            m1 = []
        ref = self._ref(m1, news)
        back = [b for b in m1 if parse_iso(b.start) >= bucket_start(news)]
        daily, feed = h.daily_bars(symbol, p.adv_days)
        adv = sum(b.close * b.volume for b in daily) / len(daily) if daily else None
        trade = h.latest_trade(symbol)
        last = trade.price if trade else (ref[0] if ref else (warm[-1].close if warm else None))
        if ref is None and last is not None:
            ref = (last, "latest_trade")
        reason = check_market(symbol, last, adv, feed, False, p)
        self._note(symbol, now, reason, last_price=last, avg_dollar_volume=adv)
        if reason or last is None or adv is None:
            return reason or RejectReason.NO_MARKET_DATA
        return SymbolContext(ref[0], last, baseline, warm, adv, feed, False, ref[1], back)


# ---------------------------------------------------------------------------- engine
@dataclass
class _Active:
    m: SetupMachine
    shadow: bool
    in_position: bool = False            # shadow with an open hypothetical position
    release_at: datetime | None = None   # production signal: auto-release the slot if nobody claims it


class StrategyEngine:
    def __init__(self, db: Database, clock: Clock, timeline: Timeline, params: StrategyParams,
                 strategy_version: str, subscriptions: SubscriptionManager, asset_cache: AssetCache,
                 market_data: Any, *,
                 on_entry_signal: Callable[[EntrySignal], Awaitable[None]],
                 on_opposite_news: Callable[[str, Side], Awaitable[None]],
                 on_bar_5m: Callable[[str, Bar, dict], Awaitable[None]],
                 shadow_enabled: bool = True) -> None:
        self.db, self.clock, self.timeline, self.params = db, clock, timeline, params
        self.strategy_version, self.subs, self.assets, self.market_data = (
            strategy_version, subscriptions, asset_cache, market_data)
        self._on_entry, self._on_opposite, self._on_bar5 = on_entry_signal, on_opposite_news, on_bar_5m
        self.shadow_enabled = shadow_enabled and params.shadow_enabled
        self.book = ShadowBook(db, clock, timeline, params, strategy_version, on_close=self._shadow_closed)
        self._active: dict[int, _Active] = {}
        self._ind: dict[str, SymbolIndicators] = {}
        self._builders: dict[str, FiveMinuteBuilder] = {}
        self._baseline: dict[str, dict[str, float]] = {}
        self._last_bar: dict[str, str] = {}
        self._backfill: dict[str, list[Bar]] = {}
        self._m1: dict[str, dict[str, Bar]] = {}            # recent 1m bars (news-bucket trimming, gap detection)
        self._last_1m: dict[str, str] = {}
        self._hist5: dict[str, list[tuple[Bar, dict]]] = {}  # completed 5m bars + indicator snapshots

    # ---------------------------------------------------------------- daemon-facing
    def active_symbols(self) -> set[str]:
        return {a.m.symbol for a in self._active.values()}

    def stage_summary(self) -> list[dict]:
        out = []
        for sid, a in self._active.items():
            m = a.m
            out.append({"symbol": m.symbol, "setup_id": sid, "variant": m.variant, "is_shadow": a.shadow,
                        "side": str(m.side), "stage": str(Stage.IN_POSITION if a.in_position else m.stage),
                        "ref_price": m.ref_price, "news_received_at": utc_iso(m.news_received_at),
                        **{k: v for k, v in m.numbers.items()}})
        return out

    async def track(self, symbol: str) -> bool:
        """Start the 1m->5m builder + EMA/ATR for a symbol that has an open position but no armed setup
        (boot rehydration). Without this the position would never see a completed 5m bar, so the ATR trail
        (`on_bar_5m`) could not fire after a restart. Returns False when market data is unavailable."""
        if symbol in self._ind:
            return True
        ctx = await self._prepare(symbol, self.clock.now())
        if isinstance(ctx, RejectReason):
            log.warning("cannot track %s after restart: %s", symbol, ctx)
            return False
        self._ensure_symbol(symbol, ctx)
        await self._replay(symbol)
        return True

    def release_symbol_owner(self, owner_id: str) -> None:
        self.subs.release(owner_id)
        if owner_id.startswith("setup-"):
            try:
                self._active.pop(int(owner_id[6:]), None)
            except ValueError:
                pass

    def _shadow_closed(self, setup_id: int) -> None:
        self.release_symbol_owner(f"setup-{setup_id}")

    # ---------------------------------------------------------------- row / timeline helpers
    def _reject_row(self, row_id: int, symbol: str, reason: RejectReason, max_stage: Stage = Stage.CLASSIFIED) -> None:
        ts = utc_iso(self.clock.now())
        self.db.update("setups", row_id, {"stage": str(Stage.REJECTED), "max_stage": str(max_stage),
                                          "reject_reason": str(reason), "updated_at": ts, "closed_at": ts})
        row = self.db.one("SELECT is_shadow, variant FROM setups WHERE id=?", (row_id,)) or {}
        tag = f"[shadow {row.get('variant')}] " if row.get("is_shadow") else ""
        self.timeline.log(symbol, Stage.REJECTED, f"{tag}{symbol} rejected: {reason}", row_id, reason=str(reason))

    def _apply(self, a: _Active, transitions: list[Transition]) -> None:
        if not transitions:
            return
        m = a.m
        tag = f"[shadow {m.variant}] " if a.shadow else ""
        for t in transitions:
            self.timeline.log(m.symbol, t.stage, tag + t.message, m.setup_id, variant=m.variant, **t.data)
        ts = utc_iso(self.clock.now())
        fields: dict[str, Any] = {"stage": str(m.stage), "max_stage": str(m.max_stage),
                                  "reject_reason": str(m.reject_reason) if m.reject_reason else None,
                                  "updated_at": ts, **{k: m.numbers[k] for k in _NUMBER_COLS}}
        if m.stage == Stage.REJECTED:
            fields["closed_at"] = ts
        if m.signal is not None:
            fields["signal_at"] = m.signal.signal_at
        self.db.update("setups", m.setup_id, fields)
        if m.stage == Stage.REJECTED:
            self.release_symbol_owner(f"setup-{m.setup_id}")

    def _drain_evicted(self) -> None:
        for owner, _sym in self.subs.drain_evicted():
            if not owner.startswith("setup-"):
                continue
            a = self._active.get(int(owner[6:]))
            if a is not None and not a.m.done:
                self._apply(a, [a.m.reject(RejectReason.NO_SLOT)])
            elif a is not None:
                self._active.pop(int(owner[6:]), None)

    # ---------------------------------------------------------------- news
    @staticmethod
    def _news_side(c: Candidate) -> Side | None:
        if isinstance(c.gate_result, Side):
            return c.gate_result
        if c.reject_reason == RejectReason.AI_BEARISH:
            return Side.SHORT  # material, confident BEARISH news still contradicts a LONG even with shorts off
        return None

    async def on_candidate(self, c: Candidate) -> None:
        p, sym, now = self.params, c.symbol, self.clock.now()
        news_side = self._news_side(c)
        # decision: stale / pre-AI-rejected stories never contradict; cooldown-rejected ones do (real news)
        if news_side is not None and c.reject_reason in _REAL_NEWS:
            await self._contradict(sym, news_side)
        if c.reject_reason is None and isinstance(c.gate_result, Side):
            await self._production(c, c.gate_result)
        elif self.shadow_enabled and c.classification is not None:
            await self._standalone_shadows(c)

    async def _contradict(self, symbol: str, news_side: Side) -> None:
        for a in list(self._active.values()):
            if a.m.symbol == symbol and a.m.side != news_side and not a.m.done:
                self._apply(a, [a.m.reject(RejectReason.CONTRADICTORY_NEWS)])
        self.book.opposite_news(symbol, news_side, self.clock.now())
        await self._safe(self._on_opposite(symbol, news_side), "on_opposite_news")

    async def _production(self, c: Candidate, side: Side) -> None:
        p, sym, now = self.params, c.symbol, self.clock.now()
        if side == Side.SHORT and not (p.allow_shorts and self.assets.short_eligible(sym)):
            self._reject_row(c.setup_id, sym, RejectReason.SHORT_UNAVAILABLE)
            return
        conf = c.classification.confidence if c.classification else 0.0
        owner = f"setup-{c.setup_id}"
        ok = self.subs.request(sym, owner, SlotPriority.PRODUCTION, conf, now)
        self._drain_evicted()
        if not ok:
            self._reject_row(c.setup_id, sym, RejectReason.NO_SLOT)
            return
        ctx = await self._prepare(sym, c.news_received_at)
        if isinstance(ctx, RejectReason):
            self.subs.release(owner)
            self._reject_row(c.setup_id, sym, ctx)
            return
        if owner not in self.subs.owners(sym):  # evicted while we awaited market data
            self._reject_row(c.setup_id, sym, RejectReason.NO_SLOT)
            return
        self._arm(c, c.setup_id, "production", side, False, ctx)
        if self.shadow_enabled:
            for v, s in spawn_companions(side, p):
                self._spawn_shadow(c, v, s, ctx)
        await self._replay(sym)

    async def _standalone_shadows(self, c: Candidate) -> None:
        sym, now, live = c.symbol, self.clock.now(), []
        for v, side in spawn_standalone(c, self.params):
            rid = self._shadow_row(c, v, side)
            if rid is None:
                continue
            if side == Side.SHORT and not self.assets.short_eligible(sym):
                self._reject_row(rid, sym, RejectReason.SHORT_UNAVAILABLE)
                continue
            ok = self.subs.request(sym, f"setup-{rid}", SlotPriority.SHADOW,
                                   c.classification.confidence, now)  # type: ignore[union-attr]
            self._drain_evicted()
            if ok:
                live.append((v, side, rid))
            else:
                self._reject_row(rid, sym, RejectReason.NO_SLOT)
        if not live:
            return
        ctx = await self._prepare(sym, c.news_received_at)
        for v, side, rid in live:
            if isinstance(ctx, RejectReason):
                self.subs.release(f"setup-{rid}")
                self._reject_row(rid, sym, ctx)
            elif f"setup-{rid}" not in self.subs.owners(sym):
                self._reject_row(rid, sym, RejectReason.NO_SLOT)
            else:
                self._arm(c, rid, v.name, side, True, ctx, v)
        await self._replay(sym)

    async def _replay(self, sym: str) -> None:
        """Feed the REST 1m backfill through the builder once every machine of this candidate exists, so
        they see the post-news buckets in order and the first live 5m bar carries its full volume."""
        for b in self._backfill.pop(sym, []):
            await self.on_bar_1m(b)

    def _shadow_row(self, c: Candidate, v: ShadowVariant | str, side: Side) -> int | None:
        name = v if isinstance(v, str) else v.name
        if self.db.one("SELECT 1 FROM setups WHERE article_id=? AND symbol=? AND variant=? AND strategy_version=?",
                       (c.article_id, c.symbol, name, self.strategy_version)):
            return None
        cl, ts = c.classification, utc_iso(self.clock.now())
        row = {"article_id": c.article_id, "symbol": c.symbol, "variant": name, "is_shadow": 1,
               "side": str(side), "stage": str(Stage.CLASSIFIED), "max_stage": str(Stage.CLASSIFIED),
               "news_received_at": utc_iso(c.news_received_at), "news_latency_s": c.news_latency_s,
               "created_at": ts, "updated_at": ts, "strategy_version": self.strategy_version}
        if cl is not None:
            row.update({"ai_direction": str(cl.direction), "ai_confidence": cl.confidence,
                        "ai_material": int(cl.material), "catalyst": cl.catalyst})
        return self.db.insert("setups", row)

    def _spawn_shadow(self, c: Candidate, v: ShadowVariant, side: Side, ctx: SymbolContext) -> None:
        rid = self._shadow_row(c, v, side)
        if rid is None:
            return
        conf = c.classification.confidence if c.classification else 0.0
        if not self.subs.request(c.symbol, f"setup-{rid}", SlotPriority.SHADOW, conf, self.clock.now()):
            self._reject_row(rid, c.symbol, RejectReason.NO_SLOT)
            return
        self._arm(c, rid, v.name, side, True, ctx, v)

    def _arm(self, c: Candidate, row_id: int, variant: str, side: Side, shadow: bool,
             ctx: SymbolContext, v: ShadowVariant | None = None) -> None:
        sym = c.symbol
        existing = sym in self._ind and sym not in self._backfill  # tracked before this candidate and already live
        self._ensure_symbol(sym, ctx)
        m = SetupMachine(row_id, sym, side, variant, self.params, c.news_received_at, ctx.ref_price,
                         rvol_min=variant_rvol_min(v, self.params) if v else None,
                         pullback_number=v.pullback_number if v else 1)
        a = self._active[row_id] = _Active(m, shadow)
        self.db.update("setups", row_id, {"stage": str(Stage.WAITING_FOR_VOLUME),
                                          "max_stage": str(Stage.WAITING_FOR_VOLUME), "ref_price": ctx.ref_price,
                                          "ref_source": ctx.ref_source, "updated_at": utc_iso(self.clock.now())})
        tag = f"[shadow {variant}] " if shadow else ""
        self.timeline.log(sym, Stage.WAITING_FOR_VOLUME,
                          f"{tag}{sym} subscribed, armed {side}, ref {ctx.ref_price:.2f}, waiting for volume",
                          row_id, variant=variant, ref_price=ctx.ref_price, ref_source=ctx.ref_source)
        for bar, snap in (self._hist5.get(sym, []) if existing else []):  # a 2nd story on an armed symbol
            if a.m.done:
                break
            try:
                self._apply(a, self._feed(a.m, bar, snap))
            except Exception as e:
                self._fail(a, e)

    def _ensure_symbol(self, sym: str, ctx: SymbolContext) -> None:
        if sym not in self._ind:
            ind = SymbolIndicators()
            for b in sorted(ctx.warm_bars_5m, key=lambda b: b.start):
                ind.update(b)
                self._last_bar[sym] = b.start
            self._ind[sym] = ind
            self._builders[sym] = FiveMinuteBuilder(sym, self.params.bar_grace_s)
            self._backfill[sym] = list(ctx.bars_1m)
        self._baseline[sym] = ctx.baseline

    async def _prepare(self, sym: str, news: datetime) -> SymbolContext | RejectReason:
        try:
            return await self.market_data.prepare(sym, news)
        except Exception as e:
            log.warning("prepare(%s) crashed: %s", sym, type(e).__name__)
            return RejectReason.NO_MARKET_DATA

    async def _safe(self, coro: Awaitable[None], what: str) -> None:
        try:
            await coro
        except Exception as e:  # a downstream callback must never take the strategy loop down
            log.exception("%s callback failed", what)
            self.timeline.system_event("ERROR", "strategy", f"{what}_failed", f"{type(e).__name__}: {e}"[:200])

    # ---------------------------------------------------------------- market data
    def _feed(self, m: SetupMachine, bar: Bar, snap: dict) -> list[Transition]:
        """Hand one completed 5m bar to a machine. The bucket that CONTAINS the news only counts from the news
        minute on (volume, high/low from the 1m bars), and its baseline is prorated by the minutes kept / 5."""
        base = self._baseline.get(m.symbol, {}).get(to_et(parse_iso(bar.start)).strftime("%H:%M"))
        start, news = parse_iso(bar.start), m.news_received_at
        floor = news.replace(second=0, microsecond=0)
        if start < floor < start + timedelta(minutes=5) and news < start + timedelta(minutes=5):
            kept = 5 - int((floor - start).total_seconds() // 60)
            post = [b for b in self._m1.get(m.symbol, {}).values()
                    if floor <= parse_iso(b.start) < start + timedelta(minutes=5)]
            if post:
                post.sort(key=lambda b: b.start)
                bar = Bar(bar.symbol, bar.start, post[0].open, max(b.high for b in post), min(b.low for b in post),
                          post[-1].close, sum(b.volume for b in post), 5)
            else:  # no print since the news: zero volume, nothing above/below the reference
                bar = Bar(bar.symbol, bar.start, m.ref_price, m.ref_price, m.ref_price, m.ref_price, 0.0, 5)
            base = base * kept / 5 if base else base
        return m.on_bar(bar, snap, base)

    def _fail(self, a: _Active, e: Exception) -> None:
        """One machine blew up: log, reject ONLY it (INTERNAL_ERROR), free its slot. Everyone else keeps running."""
        m = a.m
        log.exception("setup %s (%s %s) failed", m.setup_id, m.symbol, m.variant)
        try:
            self.timeline.system_event("ERROR", "strategy", "setup_internal_error",
                                       f"setup {m.setup_id} {m.symbol} {m.variant}: {type(e).__name__}: {e}"[:300],
                                       setup_id=m.setup_id, variant=m.variant)
        except Exception:
            log.exception("could not record setup failure")
        if a.release_at is not None:  # a production signal already went to execution: nothing left to reject
            return
        try:
            self._apply(a, [m.reject(RejectReason.INTERNAL_ERROR, f" ({type(e).__name__})")])
        except Exception:
            log.exception("could not reject failed setup %s", m.setup_id)
            self.release_symbol_owner(f"setup-{m.setup_id}")

    async def on_bar_1m(self, b: Bar) -> None:
        bld = self._builders.get(b.symbol)
        if bld is None:
            return
        m1 = self._m1.setdefault(b.symbol, {})
        m1[b.start] = b
        if b.start > self._last_1m.get(b.symbol, ""):
            self._last_1m[b.symbol] = b.start
        cut = utc_iso(parse_iso(self._last_1m[b.symbol]) - _M1_KEEP)
        for k in [k for k in m1 if k < cut]:
            del m1[k]
        for bar in bld.on_bar(b):
            await self._process_bar5(bar)

    async def backfill_after_gap(self) -> int:
        """After a market-stream reconnect: fetch the 1m bars missed since each tracked symbol's last seen minute
        (REST, iex) and feed them through the builders. Safe to repeat: the builder replaces same-minute resends and
        drops buckets it already completed. Returns the number of 1m bars fed."""
        fed = 0
        for sym in list(self._ind):
            if sym in self._last_1m:
                since = parse_iso(self._last_1m[sym])
            elif sym in self._last_bar:
                since = parse_iso(self._last_bar[sym]) + timedelta(minutes=4)  # last minute of the last 5m bucket
            else:
                continue
            try:
                bars = await self.market_data.bars_1m_since(sym, since)
            except Exception as e:
                log.warning("gap backfill for %s failed: %s", sym, type(e).__name__)
                self.timeline.system_event("WARN", "strategy", "gap_backfill_failed",
                                           f"{sym}: {type(e).__name__}", symbol=sym)
                continue
            for b in sorted(bars, key=lambda b: b.start):
                if parse_iso(b.start) > since:
                    await self.on_bar_1m(b)
                    fed += 1
        if fed:
            self.timeline.system_event("INFO", "strategy", "gap_backfilled", f"{fed} 1m bars after reconnect")
        return fed

    async def _process_bar5(self, bar: Bar) -> None:
        sym = bar.symbol
        ind = self._ind.get(sym)
        if ind is None or bar.start <= self._last_bar.get(sym, ""):
            return  # unarmed symbol, or a bucket the warm-up already covered
        self._last_bar[sym] = bar.start
        snap = ind.update(bar)
        snap["atr"] = snap["atr14"]
        hist = self._hist5.setdefault(sym, [])
        hist.append((bar, snap))
        del hist[:-_HIST5_KEEP]
        self.db.insert("market_events", {"ts": bar.start, "symbol": sym, "kind": "BAR_5M", "data_json": json.dumps(
            {"o": bar.open, "h": bar.high, "l": bar.low, "c": bar.close, "v": bar.volume, **snap}, default=str)})
        for a in [a for a in self._active.values() if a.m.symbol == sym and not a.m.done]:
            try:
                self._apply(a, self._feed(a.m, bar, snap))
            except Exception as e:
                self._fail(a, e)
        self._guard_book(self.book.on_bar, bar, snap["atr"])
        await self._safe(self._on_bar5(sym, bar, snap), "on_bar_5m")

    def _guard_book(self, fn: Callable, *args: Any) -> None:
        try:
            fn(*args)
        except Exception as e:  # the shadow book must never stop production machines
            log.exception("shadow book failed")
            self.timeline.system_event("ERROR", "strategy", "shadow_book_error", f"{type(e).__name__}: {e}"[:200])

    async def on_trade(self, t: Trade) -> None:
        self._guard_book(self.book.on_trade, t)
        for a in [a for a in self._active.values() if a.m.symbol == t.symbol and not a.m.done]:
            try:
                sig = a.m.on_trade(t)
                self._apply(a, a.m.drain())
                if sig is not None:
                    await self._signal(a, sig)
            except Exception as e:
                self._fail(a, e)

    async def _signal(self, a: _Active, sig: EntrySignal) -> None:
        m = a.m
        if not a.shadow:
            a.release_at = self.clock.now() + timedelta(seconds=self.params.setup_slot_grace_s)
            await self._safe(self._on_entry(sig), "on_entry_signal")
            return
        # SHADOW: hypothetical book only. on_entry_signal is never reachable from here.
        if m.variant == "rvol_1_5" and not (self.params.shadow_rvol_min <= m.max_rvol_seen < self.params.rvol_min):
            # production would have traded this too (or volume never qualified): no hypothetical trade, and the
            # row must not stay at ENTRY_SIGNAL where the funnel would count it as a breakout
            self._reject_row(m.setup_id, m.symbol, RejectReason.NOT_COUNTERFACTUAL, Stage.WAITING_FOR_BREAKOUT)
            self.db.update("setups", m.setup_id, {"signal_at": None})
            self.release_symbol_owner(f"setup-{m.setup_id}")
            return
        if self.book.open(sig, m.signal_price or sig.trigger_price) is not None:
            a.in_position = True

    async def on_status(self, d: dict) -> None:
        if not d.get("halted"):
            return
        for a in [a for a in self._active.values() if a.m.symbol == d.get("symbol") and not a.m.done]:
            try:
                self._apply(a, [a.m.reject(RejectReason.HALTED)])
            except Exception as e:
                self._fail(a, e)

    async def on_clock(self, now: datetime) -> None:
        for bld in list(self._builders.values()):
            for bar in bld.flush_due(now):
                await self._process_bar5(bar)
        for a in [a for a in self._active.values() if not a.m.done]:
            try:
                self._apply(a, a.m.on_clock(now))
            except Exception as e:
                self._fail(a, e)
        self._guard_book(self.book.on_clock, now)
        for sid, a in list(self._active.items()):
            if a.release_at is not None and now >= a.release_at:
                self.release_symbol_owner(f"setup-{sid}")
        for sym in [s for s in self._ind if self.subs.priority(s) is None]:  # nobody holds it any more
            for d in (self._ind, self._builders, self._baseline, self._last_bar, self._backfill,
                      self._m1, self._last_1m, self._hist5):
                d.pop(sym, None)
