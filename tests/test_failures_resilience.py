"""Failure injection against the wired app: ws drops, classifier timeout, ambiguous broker submit, restarts,
kill switch, live-endpoint refusal, arm-gate refusal."""
from __future__ import annotations

import asyncio
import pytest

import newswave.daemon as daemon
from newswave.__main__ import main
from newswave.clock import ReplayClock, parse_iso
from newswave.config import StrategyParams
from newswave.daemon import arm_hash
from newswave.execution.broker import BrokerError
from newswave.news.classifier import ClaudeCliClassifier
from newswave.replay import build_events
from test_daemon_helpers import (T_OPEN, alpaca_over_sim, drive_over_ws, fake_claude, fast_ws, make_app, mf, news_frame,
                                 play, rows, settings, start_servers, trade_frame, until)
from test_execution_helpers import new_setup, sig


def events(app, event):
    return rows(app, "SELECT * FROM system_events WHERE event=?", event)


async def live_app(tmp_path, fx=None, **kw):
    news, iex = await start_servers()
    cfg = settings(tmp_path, claude_bin=fake_claude(tmp_path), classifier_timeout_s=10, **kw)
    app, clock, sim = make_app(tmp_path, fx, cfg=cfg, news_url=news.url, iex_url=iex.url)
    app.classifier = app.intake.classifier = ClaudeCliClassifier(cfg, app.db, clock)
    fast_ws(app)
    task = asyncio.create_task(app.run(install_signals=False))
    await until(lambda: app.started.is_set() and app.news_stream.client.ready and app.market_stream.client.ready,
                what="app + streams up")
    return app, clock, sim, task, news, iex


async def teardown(app, task, *servers):
    app.stop()
    await asyncio.wait_for(task, 10)
    app.close()
    for s in servers:
        await s.close()


def story(article, headline, sym="NVDA", at=(10, 0, 10)):
    return mf.story_news(article, headline, sym, mf.at(*at))


async def push_news(app, clock, news, n):
    before = app.counts["news"]
    await news.push([news_frame(n)])
    await until(lambda: app.counts["news"] > before, what=f"story {n['id']} processed")


# ------------------------------------------------------------------ websocket drops
async def test_news_ws_drop_reconnects_and_next_story_is_processed(tmp_path):
    app, clock, sim, task, news, iex = await live_app(tmp_path)
    try:
        n1 = story("a1", "NVIDIA NEUTRALNEWS routine update one")
        clock.set(parse_iso(n1["received_at"]))
        await push_news(app, clock, news, n1)
        assert len(rows(app, "SELECT * FROM news_events")) == 1
        await news.drop()
        await until(lambda: app.news_stream.client.connects >= 2 and len(news.conns) >= 2
                    and {"action": "subscribe", "news": ["*"]} in news.conns[1], what="news reconnect + resubscribe")
        n2 = story("a2", "AAA NEUTRALNEWS routine update two", "AAA", (10, 1, 10))
        clock.set(parse_iso(n2["received_at"]))
        await push_news(app, clock, news, n2)
        assert {r["article_id"] for r in rows(app, "SELECT article_id FROM news_events")} == {"a1", "a2"}
        assert len(rows(app, "SELECT * FROM ai_classifications")) == 2       # both really classified
        assert app.counts["restart:news"] == 0        # handled inside the ws client, no task crash
    finally:
        await teardown(app, task, news, iex)


async def test_iex_ws_drop_resubscribes_including_the_armed_symbol(tmp_path):
    fx = mf.story_fixture()
    app, clock, sim, task, news, iex = await live_app(tmp_path, fx)
    try:
        await drive_over_ws(app, clock, fx, news, iex, until_ts=mf.at(10, 6))
        assert "NVDA" in iex.state["trades"]
        await iex.drop()
        await until(lambda: len(iex.conns) >= 2 and app.market_stream.client.ready, what="iex reconnect")
        await until(lambda: iex.state.get("trades", set()) >= {"NVDA", "SPY", "QQQ"}, what="full resubscribe")
        sub = [f for f in iex.conns[1] if f.get("action") == "subscribe"][0]
        assert {"NVDA", "SPY", "QQQ"} <= set(sub["trades"]) and {"NVDA", "SPY", "QQQ"} <= set(sub["bars"])
        before = app.counts["trades"]                                  # and data flows again
        await iex.push([trade_frame(next(p for _, _, k, p in build_events(fx) if k == "trade"
                                         and parse_iso(p.ts) > mf.at(10, 6)))])
        await until(lambda: app.counts["trades"] > before, what="trade after reconnect")
    finally:
        await teardown(app, task, news, iex)


# ------------------------------------------------------------------ classifier failure
async def test_classifier_timeout_is_ai_error_and_never_trades(tmp_path, monkeypatch):
    monkeypatch.setenv("FAKE_MODE", "hang")
    cfg = settings(tmp_path, claude_bin=fake_claude(tmp_path), classifier_timeout_s=1)
    app, clock, sim = make_app(tmp_path, cfg=cfg)
    app.classifier = app.intake.classifier = ClaudeCliClassifier(cfg, app.db, clock)
    await app.startup()
    await play(app, clock, mf.story_fixture(), until_ts=mf.at(10, 0, 10))
    await app.handle_news(next(p for _, _, k, p in build_events(mf.story_fixture()) if k == "news"))
    s = rows(app, "SELECT * FROM setups")
    assert [(r["variant"], r["stage"], r["reject_reason"]) for r in s] == [("production", "REJECTED", "AI_ERROR")]
    c = rows(app, "SELECT * FROM ai_classifications")[0]
    assert c["direction"] == "NEUTRAL" and c["error"].startswith("timeout")
    assert rows(app, "SELECT * FROM orders") == [] and app.strategy.active_symbols() == set()
    assert app.subs.desired() == {"SPY", "QQQ"}
    app.close()


# ------------------------------------------------------------------ ambiguous broker submit
async def test_submit_timeout_that_actually_filled_makes_exactly_one_position(tmp_path, monkeypatch):
    clock = ReplayClock(T_OPEN)
    broker, client, sim = alpaca_over_sim(clock)
    cfg = settings(tmp_path)
    armed = tmp_path / ".armed"
    armed.write_text(arm_hash(cfg.params) + "\n", encoding="utf-8")
    monkeypatch.setattr(daemon, "ARMED_FILE", armed)
    fx = mf.story_fixture()
    app, clock, _ = make_app(tmp_path, fx, clock=clock, sim=broker, cfg=cfg)
    assert app.sim is None and app.broker.kind == "alpaca"
    orig = app.handle_trade

    async def feed(t):                      # the fake broker's exchange needs the prints
        await sim.on_trade(t)
        await orig(t)
    app.handle_trade = feed
    client.script = ["timeout_after"]       # the ENTRY submit: order is live at the broker, the reply never arrives
    await app.startup()
    await play(app, clock, fx, until_ts=mf.at(10, 23))
    assert client.submit_calls >= 2         # entry (ambiguous) + protective stop; NOT a re-sent entry
    entry_reqs = [r for r in client.requests if getattr(r, "client_order_id", "").endswith("ENTRY-1")]
    assert len(entry_reqs) == 1
    assert [p.qty for p in await sim.get_positions()] == [20]
    assert len(rows(app, "SELECT * FROM positions WHERE status='OPEN'")) == 1
    assert len(rows(app, "SELECT * FROM orders WHERE purpose='ENTRY'")) == 1
    assert sum(1 for o in sim._orders.values() if o.side == "buy") == 1
    app.close()


async def test_boot_aborts_loudly_when_the_broker_cannot_be_reconciled(tmp_path, monkeypatch):
    clock = ReplayClock(T_OPEN)
    broker, client, sim = alpaca_over_sim(clock)
    cfg = settings(tmp_path)
    armed = tmp_path / ".armed"
    armed.write_text(arm_hash(cfg.params) + "\n", encoding="utf-8")
    monkeypatch.setattr(daemon, "ARMED_FILE", armed)
    app, clock, _ = make_app(tmp_path, clock=clock, sim=broker, cfg=cfg)

    def down():
        raise ConnectionError("alpaca unreachable")
    client.get_all_positions = down
    with pytest.raises(BrokerError):
        await app.run(install_signals=False)
    assert events(app, "BOOT_ABORTED")[0]["level"] == "CRITICAL"
    app.close()


# ------------------------------------------------------------------ restarts
async def test_restart_mid_position_rehydrates_and_later_exits_correctly(tmp_path):
    fx = mf.story_fixture()
    app1, clock, sim1 = make_app(tmp_path, fx)
    await app1.startup()
    await play(app1, clock, fx, until_ts=mf.at(10, 23))
    assert [p.qty for p in await sim1.get_positions()] == [20]
    app1.close()                                                   # process dies with a live position + stop

    app2, _, sim2 = make_app(tmp_path, fx, clock=clock)            # new process: new SimBroker restored from kv
    try:
        await app2.startup()
        pos = app2.execution.open_positions()
        assert [(p["symbol"], p["qty_open"], p["entry_price"], p["stop_price"]) for p in pos] == [
            ("NVDA", 20, 101.6, 100.6)]
        assert rows(app2, "SELECT * FROM timeline WHERE stage='RECONCILE'")
        assert "NVDA" in app2.subs.desired() and app2.strategy.active_symbols() == set()
        assert [o.order_type for o in await sim2.get_open_orders()] == ["stop"]
        await play(app2, clock, fx, start_after=mf.at(10, 23))
        t = rows(app2, "SELECT * FROM trades WHERE is_shadow=0")
        assert len(t) == 1 and t[0]["exit_reason"] == "TRAIL" and t[0]["qty"] == 20 and t[0]["pnl"] > 0
        assert rows(app2, "SELECT COUNT(*) c FROM orders WHERE purpose='ENTRY'")[0]["c"] == 1      # no second entry
        assert await sim2.get_positions() == [] and await sim2.get_open_orders() == []
        assert "NVDA" not in app2.subs.desired()                          # slot released after the exit
        assert not [e for e in rows(app2, "SELECT * FROM system_events WHERE level IN ('ERROR','CRITICAL')")]
    finally:
        app2.close()


async def test_kill_switch_flattens_and_a_restart_the_same_session_still_refuses_entries(tmp_path):
    fx = mf.story_fixture()
    cfg = settings(tmp_path, params=StrategyParams(max_daily_loss_pct=0.2))          # 0.2% of 6000 = $12
    app1, clock, sim1 = make_app(tmp_path, fx, cfg=cfg)
    await app1.startup()
    await play(app1, clock, fx, until_ts=mf.at(10, 23))
    clock.set(mf.at(10, 23, 30))
    from newswave.models import Trade
    await app1.handle_trade(Trade("NVDA", "2026-01-02T15:23:30.000Z", 100.0, 100))   # gaps through the stop
    assert await sim1.get_positions() == []
    ds = rows(app1, "SELECT * FROM daily_stats WHERE session_date='2026-01-02'")[0]
    assert ds["trading_disabled"] == 1 and ds["kill_switch_at"]
    assert events(app1, "KILL_SWITCH") or rows(app1, "SELECT * FROM system_events WHERE event='KILL_SWITCH'")
    t = rows(app1, "SELECT * FROM trades WHERE is_shadow=0")
    assert len(t) == 1 and t[0]["pnl"] < -12
    app1.close()

    app2, _, sim2 = make_app(tmp_path, fx, clock=clock, cfg=cfg)                 # restart, SAME session
    try:
        await app2.startup()
        clock.set(mf.at(10, 30))
        sid = new_setup(app2.db, "NVDA", version="v_test")
        await app2.execution.on_entry_signal(sig(sid, "NVDA", trigger=100.0, lo=99.0, hi=100.5))
        s = rows(app2, "SELECT * FROM setups WHERE id=?", sid)[0]
        assert s["stage"] == "REJECTED" and s["reject_reason"] == "KILL_SWITCH"
        assert rows(app2, "SELECT * FROM orders WHERE setup_id=?", sid) == [] and await sim2.get_positions() == []
    finally:
        app2.close()


# ------------------------------------------------------------------ refusals
def test_live_endpoint_env_refuses_boot(tmp_path, capsys):
    env = {"TRADING_MODE": "PAPER", "ALPACA_PAPER": "true", "DATA_DIR": str(tmp_path / "d"),
           "APCA_API_BASE_URL": "https://api.alpaca.markets/v2"}
    assert main(["run"], env) == 2
    assert "live Alpaca endpoint" in capsys.readouterr().err and not (tmp_path / "d").exists()


def test_execute_without_a_matching_arm_is_refused(tmp_path, monkeypatch, capsys):
    env = {"TRADING_MODE": "PAPER", "ALPACA_PAPER": "true", "DATA_DIR": str(tmp_path / "d"),
           "ALPACA_API_KEY": "k", "ALPACA_SECRET_KEY": "s"}
    armed = tmp_path / ".armed"
    armed.write_text("0" * 64 + "\n", encoding="utf-8")                               # stale arm
    monkeypatch.setattr(daemon, "ARMED_FILE", armed)
    assert main(["run", "--execute"], env) == 3
    assert "refused" in capsys.readouterr().err.lower()
    assert not (tmp_path / "d" / "newswave.db").exists()           # nothing booted
