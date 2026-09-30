"""Shared fixtures/fakes for the daemon, integration, failure and replay tests (no tests in here)."""
from __future__ import annotations

import asyncio
import json
import sys
from datetime import UTC, datetime
from pathlib import Path

import websockets

sys.path.insert(0, str(Path(__file__).resolve().parent / "fixtures"))
import make_fixtures as mf  # noqa: E402

from newswave.clock import ReplayClock  # noqa: E402
from newswave.config import Settings, StrategyParams  # noqa: E402
from newswave.daemon import App, build_app  # noqa: E402
from newswave.execution.broker import BrokerAsset, SimBroker  # noqa: E402
from newswave.replay import FixtureClassifier, FixtureHistorical, build_events  # noqa: E402

FIXTURE_JSON = Path(__file__).resolve().parent / "fixtures" / "story_nvda.json"
T_OPEN = mf.at(9, 0).astimezone(UTC)

FAKE_CLAUDE = """import json, os, sys, time
mode = os.environ.get("FAKE_MODE", "ok")
if mode == "hang":
    time.sleep(30)
if mode == "fail":
    sys.stderr.write("kaput"); sys.exit(1)
prompt = sys.stdin.read()            # the prompt arrives on stdin, never argv
if "NEUTRALNEWS" in prompt:
    out = dict(direction="NEUTRAL", confidence=0.6, material=False, catalyst="routine", reason="routine update")
else:
    out = dict(direction="BULLISH", confidence=0.9, material=True, catalyst="Guidance raise", reason="guidance raised")
print(json.dumps(dict(is_error=False, subtype="success", structured_output=out, result=json.dumps(out),
                      duration_ms=12, total_cost_usd=0.001)))
"""


def fake_claude(tmp_path: Path) -> str:
    p = tmp_path / "claude.py"          # a .py CLAUDE_BIN is run with sys.executable (portable, no shebang)
    p.write_text(FAKE_CLAUDE, encoding="utf-8")
    return str(p)


def settings(tmp_path: Path, **kw) -> Settings:
    params = kw.pop("params", StrategyParams())
    return Settings(alpaca_api_key="K", alpaca_secret_key="S", data_dir=tmp_path / "data", params=params,
                    strategy_version=kw.pop("version", "v_test"), **kw)


def sim_with_assets(clock, symbols=("NVDA", "AAA", "SPY", "QQQ")) -> SimBroker:
    sim = SimBroker(clock, 6000.0)
    sim.assets = {s: BrokerAsset(symbol=s, name=s, exchange="NASDAQ") for s in symbols}
    return sim


def make_app(tmp_path: Path, fx: dict | None = None, *, clock=None, sim=None, classifier=None, cfg=None,
             **kw) -> tuple[App, ReplayClock, SimBroker]:
    fx = fx or mf.story_fixture()
    clock = clock or ReplayClock(T_OPEN)
    sim = sim or sim_with_assets(clock)
    cfg = cfg or settings(tmp_path)
    app = build_app(cfg, clock=clock, broker=sim, classifier=classifier or FixtureClassifier(fx["classifications"]),
                    historical=FixtureHistorical(fx, clock), **kw)
    return app, clock, sim


async def until(pred, timeout: float = 5.0, what: str = "condition") -> None:
    end = asyncio.get_running_loop().time() + timeout
    while not pred():
        if asyncio.get_running_loop().time() > end:
            raise AssertionError(f"timeout waiting for {what}")
        await asyncio.sleep(0.002)


# ------------------------------------------------------------------ direct driving (no websockets)
async def play(app: App, clock: ReplayClock, fx: dict, until_ts: datetime | None = None, *,
               start_after: datetime | None = None) -> None:
    """Feed fixture events through the app's handlers (what the streams call), ticking like the daemon."""
    for ts, _rank, kind, payload in build_events(fx):
        if start_after is not None and ts <= start_after:
            continue
        if until_ts is not None and ts > until_ts:
            break
        clock.set(ts)
        await app.tick(ts)
        await {"news": app.handle_news, "trade": app.handle_trade, "bar": app.handle_bar}[kind](payload)


def rows(app: App, sql: str, *params) -> list[dict]:
    return app.db.query(sql, params)


# ------------------------------------------------------------------ fake Alpaca websocket servers
class FakeAlpacaWS:
    """Impersonates the Alpaca news / IEX stream: connect banner, auth, subscription confirmations, pushes."""

    def __init__(self) -> None:
        self.conns: list[list[dict]] = []   # client frames per connection
        self.sockets: list = []
        self.state: dict[str, set[str]] = {}
        self.url = ""
        self._srv = None

    async def handler(self, ws) -> None:
        log: list[dict] = []
        self.conns.append(log)
        self.sockets.append(ws)
        self.state = {}
        await ws.send(json.dumps([{"T": "success", "msg": "connected"}]))
        try:
            async for raw in ws:
                msg = json.loads(raw)
                log.append(msg)
                act = msg.get("action")
                if act == "auth":
                    await ws.send(json.dumps([{"T": "success", "msg": "authenticated"}]))
                elif act in ("subscribe", "unsubscribe"):
                    for ch, syms in msg.items():
                        if ch == "action":
                            continue
                        cur = self.state.setdefault(ch, set())
                        (cur.update if act == "subscribe" else cur.difference_update)(syms)
                    await ws.send(json.dumps([{"T": "subscription", **{k: sorted(v) for k, v in self.state.items()}}]))
        except websockets.ConnectionClosed:
            pass

    async def start(self) -> "FakeAlpacaWS":
        self._srv = await websockets.serve(self.handler, "127.0.0.1", 0)
        self.url = f"ws://127.0.0.1:{self._srv.sockets[0].getsockname()[1]}"
        return self

    async def close(self) -> None:
        self._srv.close()
        await self._srv.wait_closed()

    async def push(self, frames: list[dict]) -> None:
        await self.sockets[-1].send(json.dumps(frames))

    async def drop(self) -> None:
        for s in list(self.sockets):
            await s.close(code=1011)

    def frames(self, action: str) -> list[dict]:
        return [f for c in self.conns for f in c if f.get("action") == action]


def news_frame(n: dict) -> dict:
    return {"T": "n", "id": n["id"], "headline": n["headline"], "summary": n["summary"], "author": "x",
            "created_at": n["created_at"], "updated_at": n["created_at"], "url": n["url"], "content": n["content"],
            "symbols": n["symbols"], "source": n["source"]}


def trade_frame(t) -> dict:
    return {"T": "t", "S": t.symbol, "p": t.price, "s": t.size, "t": t.ts}


def bar_frame(b) -> dict:
    return {"T": "b", "S": b.symbol, "o": b.open, "h": b.high, "l": b.low, "c": b.close, "v": b.volume, "t": b.start}


def fast_ws(app: App) -> None:
    for c in (app.news_stream.client, app.market_stream.client):
        c.backoff_min_s, c.backoff_max_s = 0.05, 0.2
    app.tick_interval_s = 0.05
    app.restart_backoff_s = 0.05


async def drive_over_ws(app: App, clock: ReplayClock, fx: dict, news_srv: FakeAlpacaWS, iex_srv: FakeAlpacaWS,
                        until_ts: datetime | None = None, after_ts: datetime | None = None) -> None:
    """Push the fixture over the fake websockets, one message at a time, waiting until the app has PROCESSED
    it (counters tick after processing) before moving the replay clock on."""
    key = {"news": "news", "trade": "trades", "bar": "bars"}
    done = {k: app.counts[v] for k, v in key.items()}  # counters are cumulative across calls
    for ts, _rank, kind, payload in build_events(fx):
        if until_ts is not None and ts > until_ts:
            break
        if after_ts is not None and ts <= after_ts:
            continue
        clock.set(ts)
        if kind == "news":
            await news_srv.push([news_frame({"id": payload.article_id, "headline": payload.headline,
                                             "summary": payload.summary, "created_at": payload.created_at,
                                             "url": payload.url, "content": payload.content,
                                             "symbols": list(payload.symbols), "source": payload.source})])
        elif kind == "trade":
            await iex_srv.push([trade_frame(payload)])
        else:
            await iex_srv.push([bar_frame(payload)])
        done[kind] += 1
        await until(lambda: app.counts[key[kind]] >= done[kind], what=f"{kind} #{done[kind]} processed")
        if kind == "news":  # the real server only streams a symbol once it is subscribed
            syms = set(payload.symbols)
            await until(lambda: not app._jobs and syms <= iex_srv.state.get("trades", set())
                        or app.db.one("SELECT 1 FROM setups WHERE article_id=? AND stage='REJECTED'",
                                      (payload.article_id,)) is not None,
                        what="subscription or rejection")


async def start_servers() -> tuple[FakeAlpacaWS, FakeAlpacaWS]:
    return await FakeAlpacaWS().start(), await FakeAlpacaWS().start()



# ------------------------------------------------------------------ alpaca-shaped paper broker over a SimBroker
def alpaca_over_sim(clock, symbols=("NVDA", "AAA", "SPY", "QQQ")):
    """AlpacaPaperBroker on the execution-test FakeTradingClient (+ calendar), backed by a SimBroker the test
    must feed prints to (`sim.on_trade`)."""
    from datetime import datetime as _dt
    from types import SimpleNamespace

    from newswave.clock import ET
    from newswave.execution.broker import AlpacaPaperBroker
    from test_execution_helpers import FakeTradingClient, _nosleep

    sim = sim_with_assets(clock, symbols)

    class Client(FakeTradingClient):
        def get_calendar(self, req):
            d = req.start
            if d.weekday() >= 5:
                return []
            return [SimpleNamespace(date=d, open=_dt(d.year, d.month, d.day, 9, 30),
                                    close=_dt(d.year, d.month, d.day, 16, 0))]

    client = Client(sim)
    return AlpacaPaperBroker("k", "s", client=client, sleep=_nosleep), client, sim
