"""News intake (CONTRACT §7): cheap deterministic filters, then classify, gate, cooldown."""
from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Callable

from ..clock import Clock, before_cutoff, is_regular_session, parse_iso, utc_iso
from ..config import StrategyParams
from ..db import Database
from ..models import Classification, NewsEvent, RejectReason, Side, Stage
from ..timeline import Timeline
from .classifier import gate


@dataclass(frozen=True)
class Candidate:
    setup_id: int
    article_id: str
    symbol: str
    headline: str
    news_received_at: datetime
    news_created_at: datetime
    news_latency_s: float
    classification: Classification | None
    gate_result: Side | RejectReason | None
    reject_reason: RejectReason | None


class NewsIntake:
    def __init__(self, db: Database, clock: Clock, timeline: Timeline, params: StrategyParams,
                 strategy_version: str, asset_check: Callable[[str], RejectReason | None],
                 classifier: Any) -> None:
        self.db, self.clock, self.timeline, self.params = db, clock, timeline, params
        self.strategy_version, self.asset_check, self.classifier = strategy_version, asset_check, classifier

    def _reject(self, setup_id: int, symbol: str, reason: RejectReason, max_stage: Stage,
                **fields: Any) -> None:
        self.db.update("setups", setup_id, {
            "stage": str(Stage.REJECTED), "max_stage": str(max_stage), "reject_reason": str(reason),
            "updated_at": utc_iso(self.clock.now()), "closed_at": utc_iso(self.clock.now()), **fields})
        self.timeline.log(symbol, Stage.REJECTED, f"{symbol} rejected: {reason}", setup_id, reason=str(reason))

    def _pre_ai_reason(self, symbol: str, story_reason: RejectReason | None, now: datetime) -> RejectReason | None:
        if story_reason:
            return story_reason
        if not is_regular_session(now):
            return RejectReason.MARKET_CLOSED
        if not before_cutoff(now, self.params.no_new_entries_after):
            return RejectReason.AFTER_CUTOFF
        return self.asset_check(symbol)

    async def handle(self, event: NewsEvent) -> list[Candidate]:
        p, now = self.params, self.clock.now()
        received, created = parse_iso(event.received_at), parse_iso(event.created_at)
        latency = round((received - created).total_seconds(), 3)
        # 1. save story; duplicate article id (incl. updated_at re-sends) -> DUPLICATE_NEWS, no new row
        if self.db.one("SELECT 1 FROM news_events WHERE article_id=?", (event.article_id,)):
            self.timeline.log(None, Stage.NEWS, f"duplicate story {event.article_id} ignored: "
                              f"{event.headline[:60]}", reason=str(RejectReason.DUPLICATE_NEWS))
            self.timeline.system_event("INFO", "news", str(RejectReason.DUPLICATE_NEWS),
                                       f"article {event.article_id} seen again", updated_at=event.updated_at)
            return []
        symbols = list(dict.fromkeys(s.upper() for s in event.symbols))
        self.db.insert("news_events", {
            "article_id": event.article_id, "received_at": event.received_at, "created_at": event.created_at,
            "updated_at": event.updated_at, "headline": event.headline, "summary": event.summary,
            "content": event.content, "symbols_json": json.dumps(symbols), "source": event.source,
            "url": event.url, "latency_s": latency, "is_duplicate": 0, "strategy_version": self.strategy_version})
        # 3a. no symbols: nothing to attach a setup to -> system_event only
        if not symbols:
            self.timeline.log(None, Stage.NEWS, f"story {event.article_id} has no symbols: {event.headline[:60]}",
                              reason=str(RejectReason.NO_SYMBOLS))
            self.timeline.system_event("INFO", "news", str(RejectReason.NO_SYMBOLS),
                                       f"article {event.article_id} has no symbols")
            return []
        # 2/3. story-level rejects
        story_reason = None
        if latency > p.news_max_age_seconds:
            story_reason = RejectReason.NEWS_TOO_OLD
        elif len(symbols) > p.max_symbols_per_story:
            story_reason = RejectReason.TOO_MANY_SYMBOLS

        # 4. one setups row per article x symbol
        rows: list[tuple[str, int, RejectReason | None]] = []
        for sym in symbols:
            sid = self.db.insert("setups", {
                "article_id": event.article_id, "symbol": sym, "variant": "production", "is_shadow": 0,
                "stage": str(Stage.NEWS), "max_stage": str(Stage.NEWS), "news_received_at": event.received_at,
                "news_latency_s": latency, "created_at": utc_iso(now), "updated_at": utc_iso(now),
                "strategy_version": self.strategy_version})
            self.timeline.log(sym, Stage.NEWS, f"{sym} news received (latency {latency:.1f}s): "
                              f"{event.headline[:80]}", sid, article_id=event.article_id, latency_s=latency)
            rows.append((sym, sid, self._pre_ai_reason(sym, story_reason, received)))

        todo: list[tuple[str, int, RejectReason | None]] = []
        for sym, sid, pre in rows:
            if pre and not p.classify_outside_window:
                self._reject(sid, sym, pre, Stage.NEWS)
            elif self._budget_left() <= 0:
                self._reject(sid, sym, RejectReason.CLASSIFIER_BUDGET, Stage.NEWS)
            else:
                todo.append((sym, sid, pre))
        results = await asyncio.gather(*(self._classify(event, s) for s, _, _ in todo))

        out: list[Candidate] = []
        for (sym, sid, pre), c in zip(todo, results):
            g = gate(c, p)
            reason = pre or (g if isinstance(g, RejectReason) else None)
            if reason is None:  # 5. catalyst cooldown, only after gating
                cutoff = utc_iso(self.clock.now() - timedelta(minutes=p.catalyst_cooldown_minutes))
                if self.db.one(
                        "SELECT 1 FROM setups WHERE symbol=? AND variant='production' AND side=? AND article_id<>? "
                        "AND created_at>? AND strategy_version=?",
                        (sym, str(g), event.article_id, cutoff, self.strategy_version)):
                    reason = RejectReason.CATALYST_COOLDOWN
            ai = {"ai_direction": str(c.direction), "ai_confidence": c.confidence,
                  "ai_material": int(c.material), "catalyst": c.catalyst}
            self.timeline.log(sym, Stage.CLASSIFIED,
                              f"Claude: {c.error or f'{c.direction} {c.confidence:.2f} - {c.reason}'}", sid,
                              direction=str(c.direction), confidence=c.confidence, material=c.material)
            if reason:
                self._reject(sid, sym, reason, Stage.CLASSIFIED, **ai)
            else:
                self.db.update("setups", sid, {**ai, "side": str(g), "stage": str(Stage.CLASSIFIED),
                                               "max_stage": str(Stage.CLASSIFIED),
                                               "updated_at": utc_iso(self.clock.now())})
                self.timeline.log(sym, Stage.CLASSIFIED, f"{sym} armed {g} candidate ({c.catalyst})", sid)
            out.append(Candidate(sid, event.article_id, sym, event.headline, received, created, latency,
                                 c, g, reason))
        return out

    def _budget_left(self) -> int:
        f = getattr(self.classifier, "budget_left", None)
        return f() if f else 1

    async def _classify(self, event: NewsEvent, symbol: str) -> Classification:
        try:
            return await self.classifier.classify(event, symbol)
        except Exception as e:  # classifier must never take intake down
            from ..models import Direction
            return Classification(Direction.NEUTRAL, 0.0, False, "", "", self.params.classifier_model,
                                  error=f"classifier crashed: {type(e).__name__}: {e}"[:200])
