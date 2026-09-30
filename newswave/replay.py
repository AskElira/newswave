"""Replay: historical / fixture news + 1m bars streamed through the SAME NewsIntake -> StrategyEngine ->
ExecutionEngine(SimBroker) -> ShadowBook objects, built by `daemon.build_app` with a ReplayClock.

Fixture JSON (see tests/fixtures/make_fixtures.py):
  name, session_date, news[{id, created_at, received_at?, headline, summary, content, symbols, source, url}],
  bars_1m{SYM:[{t,o,h,l,c,v}]} (the session being replayed), history_5m{SYM:[{t,o,h,l,c,v}]} (>= 20 prior
  sessions for the RVOL baseline), classifications{"<article_id>:<SYM>": {direction,confidence,material,
  catalyst,reason}}, assets? [symbols], extra_assets? (list of BrokerAsset dicts).

Trade prints are derived from every 1m bar: open -> (low, high in bar-direction order) -> close, at
t+1s / t+20s / t+40s / t+58s; the 1m bar itself is delivered at t+60s (like the live stream).
Ticks (the daemon's 1 s ticker) run every simulated second while anything is armed / open, else every 30 s.
"""
from __future__ import annotations

import json
import shutil
import sqlite3
from dataclasses import replace
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path
from typing import Any

from .clock import CLOSE_T, ET, OPEN_T, ReplayClock, parse_iso, to_et, utc_iso
from .config import Settings
from .daemon import App, build_app
from .execution.broker import BrokerAsset, SimBroker
from .market.bars import aggregate_5m
from .models import Bar, Classification, Direction, NewsEvent, Trade
from .news.classifier import ClaudeCliClassifier

BUSY_TICK_S = 1
IDLE_TICK_S = 30
PRINT_OFFSETS_S = (1, 20, 40, 58)


# ------------------------------------------------------------------ fixture data -> engine inputs
def _bar(symbol: str, d: dict, minutes: int) -> Bar:
    return Bar(symbol, utc_iso(parse_iso(d["t"])), float(d["o"]), float(d["h"]), float(d["l"]), float(d["c"]),
               float(d["v"]), minutes)


def derive_prints(b: Bar) -> list[Trade]:
    """open -> (low/high order by bar direction) -> close."""
    t0 = parse_iso(b.start)
    path = [b.open, b.low, b.high, b.close] if b.close >= b.open else [b.open, b.high, b.low, b.close]
    size = max(1.0, float(int(b.volume / 4)))
    return [Trade(b.symbol, utc_iso(t0 + timedelta(seconds=o)), p, size) for o, p in zip(PRINT_OFFSETS_S, path)]


def build_events(fx: dict) -> list[tuple[datetime, int, str, Any]]:
    """(time, rank, kind, payload) sorted; news first, then prints, then the 1m bar that closes the minute."""
    ev: list[tuple[datetime, int, str, Any]] = []
    for n in fx.get("news", []):
        created = parse_iso(n["created_at"])
        received = parse_iso(n["received_at"]) if n.get("received_at") else created + timedelta(seconds=1)
        ev.append((received, 0, "news", NewsEvent(
            article_id=str(n["id"]), received_at=utc_iso(received), created_at=utc_iso(created),
            updated_at=utc_iso(parse_iso(n.get("updated_at") or n["created_at"])), headline=n.get("headline", ""),
            summary=n.get("summary", ""), content=n.get("content", ""),
            symbols=tuple(n.get("symbols", [])), source=n.get("source", "fixture"), url=n.get("url", ""))))
    for sym in sorted(fx.get("bars_1m", {})):
        for d in fx["bars_1m"][sym]:
            b = _bar(sym, d, 1)
            for t in derive_prints(b):
                ev.append((parse_iso(t.ts), 1, "trade", t))
            ev.append((parse_iso(b.start) + timedelta(seconds=60), 2, "bar", b))
    ev.sort(key=lambda e: (e[0], e[1]))  # stable: generation order (symbols sorted) breaks remaining ties
    return ev


class FixtureHistorical:
    """Duck-types HistoricalData, answering as of the replay clock (no look-ahead)."""

    def __init__(self, fx: dict, clock: ReplayClock) -> None:
        self.fx, self.clock = fx, clock
        self._b1 = {s: [_bar(s, d, 1) for d in v] for s, v in fx.get("bars_1m", {}).items()}
        self._h5 = {s: [_bar(s, d, 5) for d in v] for s, v in fx.get("history_5m", {}).items()}

    def bars_5m(self, symbol: str, start: datetime, end: datetime | None = None, feed: str = "iex") -> list[Bar]:
        now = self.clock.now()
        done = [b for b in aggregate_5m([x for x in self._b1.get(symbol, [])
                                         if parse_iso(x.start) + timedelta(minutes=1) <= now])
                if parse_iso(b.start) + timedelta(minutes=5) <= now]
        return sorted([b for b in [*self._h5.get(symbol, []), *done] if parse_iso(b.start) >= start],
                      key=lambda b: b.start)

    def bars_1m(self, symbol: str, start: datetime, end: datetime | None = None, feed: str = "iex") -> list[Bar]:
        now = self.clock.now()
        return [b for b in self._b1.get(symbol, [])
                if parse_iso(b.start) >= start and parse_iso(b.start) + timedelta(minutes=1) <= now]

    def daily_bars(self, symbol: str, days: int = 20, feed: str = "sip") -> tuple[list[Bar], str]:
        by_day: dict[date, list[Bar]] = {}
        for b in self._h5.get(symbol, []):
            by_day.setdefault(to_et(parse_iso(b.start)).date(), []).append(b)
        out = []
        for d in sorted(by_day)[-days:]:
            bs = sorted(by_day[d], key=lambda b: b.start)
            out.append(Bar(symbol, bs[0].start, bs[0].open, max(b.high for b in bs), min(b.low for b in bs),
                           bs[-1].close, sum(b.volume for b in bs), 1440))
        return out, "iex"  # IEX volume only: universe applies the IEX liquidity scale

    def latest_trade(self, symbol: str, feed: str = "iex") -> Trade | None:
        now, last = self.clock.now(), None
        for b in self._b1.get(symbol, []):
            for t in derive_prints(b):
                if parse_iso(t.ts) <= now:
                    last = t
        return last


# ------------------------------------------------------------------ classifiers
def _row_record(db: Any, clock: Any, settings: Settings, event: NewsEvent, symbol: str, c: Classification,
                raw: dict) -> None:
    if db is None:
        return
    db.insert("ai_classifications", {
        "article_id": event.article_id, "symbol": symbol, "model": c.model,
        "effort": settings.params.classifier_effort, "direction": str(c.direction), "confidence": c.confidence,
        "material": int(c.material), "catalyst": c.catalyst, "reason": c.reason, "raw_json": json.dumps(raw),
        "duration_ms": 0, "cost_usd_est": 0.0, "error": c.error, "created_at": utc_iso(clock.now()),
        "strategy_version": settings.strategy_version})


class _Recording:
    db: Any = None
    clock: Any = None
    settings: Settings | None = None

    def attach(self, db: Any, clock: Any, settings: Settings) -> None:
        self.db, self.clock, self.settings = db, clock, settings

    def _finish(self, event: NewsEvent, symbol: str, c: Classification, raw: dict) -> Classification:
        _row_record(self.db, self.clock, self.settings, event, symbol, c, raw)  # type: ignore[arg-type]
        return c


class FixtureClassifier(_Recording):
    """Canned classifications keyed '<article_id>:<SYMBOL>'; anything else is NEUTRAL with an error."""
    MODEL = "fixture"

    def __init__(self, canned: dict[str, dict]) -> None:
        self.canned = canned

    async def classify(self, event: NewsEvent, symbol: str) -> Classification:
        d = self.canned.get(f"{event.article_id}:{symbol}")
        if d is None:
            c = Classification(Direction.NEUTRAL, 0.0, False, "", "", self.MODEL, error="no canned classification")
            return self._finish(event, symbol, c, {})
        c = Classification(Direction(d["direction"]), float(d["confidence"]), bool(d["material"]),
                           d.get("catalyst", ""), d.get("reason", ""), self.MODEL)
        return self._finish(event, symbol, c, d)


class CachedClassifier(_Recording):
    """Reuse ai_classifications rows of the live database (read-only), else NEUTRAL with an error."""
    MODEL = "cached"

    def __init__(self, live_db_path: Path) -> None:
        self.path = Path(live_db_path)

    def _lookup(self, article_id: str, symbol: str) -> dict | None:
        if not self.path.exists():
            return None
        con = sqlite3.connect(f"file:{self.path}?mode=ro", uri=True)
        con.row_factory = sqlite3.Row
        try:
            r = con.execute("SELECT * FROM ai_classifications WHERE article_id=? AND symbol=? AND error IS NULL "
                            "ORDER BY id DESC LIMIT 1", (article_id, symbol)).fetchone()
            return dict(r) if r else None
        finally:
            con.close()

    async def classify(self, event: NewsEvent, symbol: str) -> Classification:
        r = self._lookup(event.article_id, symbol)
        if r is None:
            c = Classification(Direction.NEUTRAL, 0.0, False, "", "", self.MODEL,
                               error="no cached classification (use --classify cli to call the claude CLI)")
            return self._finish(event, symbol, c, {})
        c = Classification(Direction(r["direction"]), float(r["confidence"]), bool(r["material"]),
                           r["catalyst"] or "", r["reason"] or "", r["model"] or self.MODEL)
        return self._finish(event, symbol, c, {"cached_from": r["id"]})


# ------------------------------------------------------------------ the replay loop
def _busy(app: App) -> bool:
    return bool(app.strategy.active_symbols() or app.execution.open_positions())


async def run_events(app: App, fx: dict, clock: ReplayClock) -> None:
    events = build_events(fx)
    if not events:
        return
    sess = date.fromisoformat(fx["session_date"]) if fx.get("session_date") else to_et(events[0][0]).date()
    end = max(datetime.combine(sess, CLOSE_T, ET).astimezone(UTC) + timedelta(minutes=1), events[-1][0])
    start = datetime.combine(sess, time(9, 0), ET).astimezone(UTC)
    clock.set(min(start, events[0][0]))
    await app.startup()
    now = clock.now()
    await app.tick(now)

    async def advance(to: datetime) -> None:
        nonlocal now
        while now < to:
            step = timedelta(seconds=BUSY_TICK_S if _busy(app) else IDLE_TICK_S)
            now = min(to, now + step)
            clock.set(now)
            await app.tick(now)

    for ts, _rank, kind, payload in events:
        await advance(ts)
        clock.set(ts)
        if kind == "news":
            await app.handle_news(payload)
        elif kind == "trade":
            await app.handle_trade(payload)
        else:
            await app.handle_bar(payload)
        now = ts
    await advance(end)
    await app.tick(end)


def _assets(fx: dict) -> list[BrokerAsset]:
    syms = set(fx.get("assets") or []) | set(fx.get("bars_1m", {})) | {s for n in fx.get("news", [])
                                                                        for s in n.get("symbols", [])}
    out = {s: BrokerAsset(symbol=s, name=s, exchange="NASDAQ") for s in sorted(syms)}
    for a in fx.get("extra_assets", []):
        out[a["symbol"]] = BrokerAsset(**a)
    return list(out.values())


async def replay_fixture(fx: dict, settings: Settings, *, out_dir: Path | None = None,
                         classifier: Any = None) -> dict:
    """Replay `fx` into its own database (never the live one). Returns a small summary dict."""
    name = fx.get("name", "replay")
    root = Path(out_dir) if out_dir else settings.data_dir / "replay" / name
    if root.exists():
        shutil.rmtree(root)
    rs = replace(settings, data_dir=root, execution_mode="OBSERVE")
    clock = ReplayClock(datetime.combine(date.fromisoformat(fx["session_date"]), time(9, 0), ET).astimezone(UTC))
    sim = SimBroker(clock, settings.starting_equity)
    sim.assets = {a.symbol: a for a in _assets(fx)}
    clf = classifier or FixtureClassifier(fx.get("classifications", {}))
    app = build_app(rs, clock=clock, broker=sim, classifier=clf, historical=FixtureHistorical(fx, clock))
    try:
        await run_events(app, fx, clock)
        trades = app.db.query("SELECT symbol, variant, is_shadow, exit_reason, pnl, r_multiple FROM trades "
                              "ORDER BY id")
        return {"db": str(rs.db_path), "trades": len([t for t in trades if not t["is_shadow"]]),
                "shadow_trades": len([t for t in trades if t["is_shadow"]]),
                "pnl": round(sum(t["pnl"] or 0 for t in trades if not t["is_shadow"]), 2),
                "setups": app.db.one("SELECT COUNT(*) c FROM setups")["c"]}
    finally:
        app.close()


# ------------------------------------------------------------------ historical mode (REST) -> same path
def fetch_historical_fixture(settings: Settings, day: date, symbols: list[str]) -> dict:
    """Alpaca REST news + IEX 1m bars for `day`, plus 45 days of IEX 5m history for the RVOL baseline.
    received_at is created_at + 1 s (the REST API has no receive time)."""
    from alpaca.data.historical.news import NewsClient
    from alpaca.data.requests import NewsRequest

    from .market.baseline import HistoricalData
    o = datetime.combine(day, OPEN_T, ET).astimezone(UTC)
    c = datetime.combine(day, CLOSE_T, ET).astimezone(UTC)
    hd = HistoricalData(settings.alpaca_api_key, settings.alpaca_secret_key)
    news: list[dict] = []
    token = None
    nc = NewsClient(settings.alpaca_api_key, settings.alpaca_secret_key)
    while True:
        res = nc.get_news(NewsRequest(start=o - timedelta(hours=1), end=c, symbols=",".join(symbols), limit=50,
                                      include_content=True, page_token=token))
        for n in res.data.get("news", []):
            news.append({"id": str(n.id), "created_at": utc_iso(n.created_at), "updated_at": utc_iso(n.updated_at),
                         "headline": n.headline or "", "summary": n.summary or "", "content": n.content or "",
                         "symbols": [s for s in n.symbols if s in symbols], "source": n.source or "", "url": n.url or ""})
        token = getattr(res, "next_page_token", None)
        if not token:
            break
    news.sort(key=lambda n: (n["created_at"], n["id"]))
    fx: dict = {"name": f"hist-{day.isoformat()}", "session_date": day.isoformat(), "news": news,
                "bars_1m": {}, "history_5m": {}, "classifications": {}}
    for s in symbols:
        fx["bars_1m"][s] = [{"t": b.start, "o": b.open, "h": b.high, "l": b.low, "c": b.close, "v": b.volume}
                            for b in hd.bars_1m(s, o, c)]
        fx["history_5m"][s] = [{"t": b.start, "o": b.open, "h": b.high, "l": b.low, "c": b.close, "v": b.volume}
                               for b in hd.bars_5m(s, o - timedelta(days=45), o)]
    return fx


def make_classifier(mode: str, settings: Settings, fx: dict, canned_path: str | None) -> Any:
    if mode == "cli":
        return _CliForReplay(settings)
    if mode == "fixture":
        canned = dict(fx.get("classifications", {}))
        if canned_path:
            canned.update(json.loads(Path(canned_path).read_text(encoding="utf-8")))
        return FixtureClassifier(canned)
    return CachedClassifier(settings.db_path)  # default: cached


class _CliForReplay:
    """Opt-in: real `claude` CLI calls (uses the user's Claude plan). Built lazily against the replay db."""

    def __init__(self, settings: Settings) -> None:
        self.settings, self._inner = settings, None

    def attach(self, db: Any, clock: Any, settings: Settings) -> None:
        self._inner = ClaudeCliClassifier(settings, db, clock)

    def budget_left(self) -> int:
        return self._inner.budget_left()  # type: ignore[union-attr]

    async def classify(self, event: NewsEvent, symbol: str) -> Classification:
        return await self._inner.classify(event, symbol)  # type: ignore[union-attr]


def load_fixture(path: str | Path) -> dict:
    return json.loads(Path(path).read_text(encoding="utf-8"))
