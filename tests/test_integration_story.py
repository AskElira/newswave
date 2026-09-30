"""End-to-end over LOCAL websocket servers impersonating Alpaca's news and IEX streams.

Real parts: AlpacaWSClient, NewsStream, MarketStream, NewsIntake, the `claude` CLI classifier (a fake executable),
StrategyEngine, ExecutionEngine, SimBroker, the daemon ticker + subscription applier. The clock is a ReplayClock
moved by the test, and the test waits on processed-message counters (never sleeps) before each clock move.
"""
from __future__ import annotations

import asyncio

import pytest

from newswave.clock import parse_iso
from newswave.news.classifier import ClaudeCliClassifier
from test_daemon_helpers import (T_OPEN, drive_over_ws, fake_claude, fast_ws, make_app, mf, rows, settings,
                                 start_servers, until)


@pytest.fixture
async def servers():
    news, iex = await start_servers()
    yield news, iex
    await news.close()
    await iex.close()


async def boot(tmp_path, servers, fx, monkeypatch, classify_env=None):
    news, iex = servers
    cfg = settings(tmp_path, claude_bin=fake_claude(tmp_path), classifier_timeout_s=10)
    app, clock, sim = make_app(tmp_path, fx, cfg=cfg, news_url=news.url, iex_url=iex.url)
    app.classifier = ClaudeCliClassifier(cfg, app.db, clock)
    app.intake.classifier = app.classifier
    fast_ws(app)
    task = asyncio.create_task(app.run(install_signals=False))
    await until(lambda: app.started.is_set(), what="app started")
    await until(lambda: news.conns and iex.conns and app.news_stream.client.ready and app.market_stream.client.ready,
                what="both streams authenticated")
    return app, clock, sim, task


async def shutdown(app, task):
    app.stop()
    await asyncio.wait_for(task, 10)
    app.close()


async def test_scenario_a_spec27_story(tmp_path, servers, monkeypatch):
    news, iex = servers
    fx = mf.story_fixture()
    app, clock, sim, task = await boot(tmp_path, servers, fx, monkeypatch)
    try:
        # SPY/QQQ are subscribed from boot, before any news
        await until(lambda: {"SPY", "QQQ"} <= iex.state.get("trades", set()), what="SPY/QQQ subscribed")
        first = iex.frames("subscribe")[0]
        assert {"SPY", "QQQ"} <= set(first["trades"]) and set(first["bars"]) == set(first["trades"])
        assert news.frames("subscribe")[0] == {"action": "subscribe", "news": ["*"]}
        assert "NVDA" not in iex.state.get("trades", set())

        await drive_over_ws(app, clock, fx, news, iex, until_ts=mf.at(10, 22, 30))
        # news -> classified by the fake claude CLI -> NVDA subscribed on the IEX server
        nvda_sub = [f for f in iex.frames("subscribe") if "NVDA" in f.get("trades", [])]
        assert nvda_sub and "NVDA" in nvda_sub[0]["bars"]
        cls = rows(app, "SELECT * FROM ai_classifications")
        assert len(cls) == 1 and cls[0]["direction"] == "BULLISH" and cls[0]["confidence"] == 0.9 and cls[0]["error"] is None
        assert rows(app, "SELECT max_stage FROM setups WHERE variant='production'")[0]["max_stage"] == "WAITING_FOR_BREAKOUT"

        await drive_over_ws(app, clock, fx, news, iex, after_ts=mf.at(10, 22, 30))
        for t in (mf.at(10, 41), mf.at(10, 46)):   # ticker-equivalent nudges: expiry, slot release
            clock.set(t)
            await app.tick(t)
        await until(lambda: "NVDA" not in iex.state.get("trades", set()), what="NVDA unsubscribed after close")
        assert {"SPY", "QQQ"} <= iex.state["trades"]                      # reserved symbols always subscribed
        assert any("NVDA" in f.get("trades", []) for f in iex.frames("unsubscribe"))

        # production trade: TRAIL exit, latency recorded
        tr = rows(app, "SELECT * FROM trades WHERE is_shadow=0")
        assert len(tr) == 1
        t = tr[0]
        assert (t["symbol"], t["side"], t["exit_reason"], t["qty"]) == ("NVDA", "LONG", "TRAIL", 20)
        assert t["entry_latency_ms"] is not None and t["entry_price"] == pytest.approx(101.6)
        assert t["pnl"] == pytest.approx(5.0) and t["news_latency_s"] == pytest.approx(1.0)
        assert t["catalyst"] == "Guidance raise" and t["ai_confidence"] == 0.9
        # funnel row: one per (article, symbol, variant); production reached CLOSED
        s = rows(app, "SELECT * FROM setups WHERE variant='production'")
        assert len(s) == 1 and s[0]["max_stage"] == "CLOSED" and s[0]["stage"] == "CLOSED"
        assert {r["variant"] for r in rows(app, "SELECT variant FROM setups")} == {"production", "rvol_1_5",
                                                                                    "second_pullback"}
        # equity curve
        snaps = rows(app, "SELECT reason, ledger_equity FROM equity_snapshots ORDER BY id")
        assert {"entry_fill", "position_closed"} <= {r["reason"] for r in snaps}
        assert snaps[-1]["ledger_equity"] == pytest.approx(6005.0)
        # orders: entry limit, stop (resized), partial, exit, all with broker ids
        assert [r["purpose"] for r in rows(app, "SELECT purpose FROM orders ORDER BY id")][0] == "ENTRY"
        assert rows(app, "SELECT COUNT(*) c FROM orders WHERE purpose='ENTRY'")[0]["c"] == 1
        # the timeline reads like SPEC 27
        tl = [r["message"] for r in rows(app, "SELECT message FROM timeline WHERE symbol='NVDA' ORDER BY id")]
        wanted = ["news received", "Claude: BULLISH 0.90", "subscribed, armed LONG", "RVOL 2.5",
                  "price impulse +2.0%", "waiting for 9 EMA", "9 EMA pullback confirmed", "breakout triggered",
                  "BUY 20 NVDA @ 101.60", "+1.1R -> sold 50%", "trail raised", "5m close below ATR trail",
                  "position closed"]
        i = 0
        for m in tl:
            if i < len(wanted) and wanted[i] in m:
                i += 1
        assert i == len(wanted), f"timeline missing {wanted[i]!r}: {tl}"
        ts = [r["ts"] for r in rows(app, "SELECT ts FROM timeline ORDER BY id")]
        assert ts == sorted(ts)
        assert not rows(app, "SELECT * FROM system_events WHERE level IN ('ERROR','CRITICAL')")
        # kill switch untouched, sim broker flat
        assert not rows(app, "SELECT * FROM daily_stats WHERE trading_disabled=1")
        assert (await sim.get_positions()) == []
    finally:
        await shutdown(app, task)


async def test_scenario_b_neutral_news_makes_shadow_only(tmp_path, servers, monkeypatch):
    news, iex = servers
    fx = mf.story_fixture()
    fx["news"] = [mf.story_news("a-nvda-n", "NVIDIA NEUTRALNEWS routine product update")]
    app, clock, sim, task = await boot(tmp_path, servers, fx, monkeypatch)
    try:
        await drive_over_ws(app, clock, fx, news, iex)
        for t in (mf.at(10, 41), mf.at(10, 46)):
            clock.set(t)
            await app.tick(t)
        assert rows(app, "SELECT * FROM orders") == []
        assert rows(app, "SELECT * FROM trades WHERE is_shadow=0") == []
        assert (await sim.get_positions()) == []
        prod = rows(app, "SELECT * FROM setups WHERE variant='production'")[0]
        assert prod["stage"] == "REJECTED" and prod["reject_reason"] == "AI_NEUTRAL"
        shadow = rows(app, "SELECT * FROM setups WHERE variant='neutral_news'")
        assert len(shadow) == 1 and shadow[0]["is_shadow"] == 1 and shadow[0]["ai_direction"] == "NEUTRAL"
        assert shadow[0]["max_stage"] in ("WAITING_FOR_BREAKOUT", "ENTRY_SIGNAL", "IN_POSITION", "CLOSED")
    finally:
        await shutdown(app, task)
