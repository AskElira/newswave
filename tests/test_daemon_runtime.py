"""Daemon run-time behaviour: persistence, isolation of crashes, supervision, heartbeat, calendar, shutdown."""
from __future__ import annotations

import asyncio
import json
import os
import signal
import sys
from dataclasses import asdict
from datetime import timedelta

import pytest

from newswave.clock import ReplayClock, clear_session_closes
from newswave.config import StrategyParams
from newswave.execution.broker import BrokerCalendar, SimBroker
from newswave.news.classifier import ClaudeCliClassifier
from newswave.models import NewsEvent, RejectReason, Trade
from test_daemon_helpers import (T_OPEN, fast_ws, make_app, mf, play, rows, settings, sim_with_assets,
                                 start_servers, until)


@pytest.fixture(autouse=True)
def _fresh_sessions():
    clear_session_closes()  # the session-close registry is process-global: no leaking early closes between tests
    yield
    clear_session_closes()


def events(app, event):
    return rows(app, "SELECT * FROM system_events WHERE event=?", event)


# ------------------------------------------------------------------ OBSERVE restart safety
async def test_sim_state_survives_a_restart(tmp_path):
    fx = mf.story_fixture()
    app, clock, sim = make_app(tmp_path, fx)
    await app.startup()
    await play(app, clock, fx, until_ts=mf.at(10, 23))
    pos, orders = await sim.get_positions(), await sim.get_open_orders()
    assert [(p.symbol, p.qty) for p in pos] == [("NVDA", 20)] and [o.order_type for o in orders] == ["stop"]
    cash, nxt = sim.cash, sim._n.last
    app.close()

    app2, _, sim2 = make_app(tmp_path, fx, clock=clock)          # brand-new SimBroker, same database
    try:
        assert events(app2, "SIM_RESTORED")
        assert [(p.symbol, p.qty, p.avg_entry_price) for p in await sim2.get_positions()] == [("NVDA", 20, 101.6)]
        assert [(o.client_order_id, o.qty, o.stop_price) for o in await sim2.get_open_orders()] == [
            (o.client_order_id, o.qty, o.stop_price) for o in orders]
        assert sim2.cash == pytest.approx(cash) and sim2._n.last == nxt
        # the restored broker remembers filled orders too (reconcile looks them up by client id)
        assert (await sim2.get_order_by_client_id(f"nw-{app2.execution.uid}-1-ENTRY-1")).status == "filled"
    finally:
        app2.close()


def test_sim_snapshot_roundtrip_is_json_and_prunes_old_terminal_orders():
    clock = ReplayClock()
    a = SimBroker(clock)
    for i in range(7):
        asyncio.run(a.submit_limit("A", "buy", 1, 1.0, f"c{i}"))
        asyncio.run(a.cancel_order(f"sim-{i + 1}"))
    asyncio.run(a.submit_limit("A", "buy", 1, 1.0, "open"))
    snap = json.loads(json.dumps(a.snapshot(keep_terminal=3)))
    b = SimBroker(clock)
    b.restore(snap)
    assert sorted(b._orders) == ["c4", "c5", "c6", "open"]
    asyncio.run(b.submit_limit("A", "buy", 1, 1.0, "next"))
    assert b._orders["next"].bid == "sim-9"


# ------------------------------------------------------------------ crashes never stop the loop
async def test_crashing_callbacks_do_not_kill_the_loop(tmp_path):
    news, iex = await start_servers()
    app, clock, sim = make_app(tmp_path, news_url=news.url, iex_url=iex.url)
    fast_ws(app)
    calls = {"exec": 0}
    real = app.execution.on_clock

    async def boom(now):
        raise RuntimeError("strategy exploded")

    async def counting(now):
        calls["exec"] += 1
        await real(now)

    app.strategy.on_clock = boom
    app.execution.on_clock = counting
    task = asyncio.create_task(app.run(install_signals=False))
    try:
        await until(lambda: calls["exec"] >= 5, what="execution ticking despite the strategy crashing")
        crit = [e for e in events(app, "CALLBACK_FAILED") if e["level"] == "CRITICAL"]
        assert len(crit) == 1 and json.loads(crit[0]["data_json"])["where"] == "strategy.on_clock"  # no flood
        assert app.counts["errors"] >= 5 and not task.done()
    finally:
        app.stop()
        await asyncio.wait_for(task, 10)
        app.close()
        await news.close()
        await iex.close()


async def test_a_crash_in_one_handler_stage_does_not_skip_execution(tmp_path):
    app, clock, sim = make_app(tmp_path)
    seen = []

    async def bad(t):
        raise ValueError("bad strategy")

    async def ok(t):
        seen.append(t.price)

    app.strategy.on_trade, app.execution.on_trade = bad, ok
    await app.handle_trade(Trade("NVDA", "2026-01-02T15:00:00.000Z", 100.0, 1))   # does not raise
    assert seen == [100.0] and app.counts["trades"] == 1
    assert len(events(app, "CALLBACK_FAILED")) == 1

    async def bad_intake(ev):
        raise RuntimeError("intake exploded")

    app.intake.handle = bad_intake
    from newswave.replay import build_events
    await app.handle_news(next(p for _, _, k, p in build_events(mf.story_fixture()) if k == "news"))
    assert app.counts["news"] == 1 and app.counts["errors"] == 2
    app.close()


async def test_crashed_task_is_restarted_with_backoff(tmp_path):
    news, iex = await start_servers()
    app, clock, sim = make_app(tmp_path, news_url=news.url, iex_url=iex.url)
    fast_ws(app)
    calls = []

    async def flaky():
        calls.append(1)
        if len(calls) == 1:
            raise OSError("socket exploded")
        await app._stop.wait()

    app.news_stream.run = flaky
    task = asyncio.create_task(app.run(install_signals=False))
    try:
        await until(lambda: len(calls) >= 2, what="news task restarted")
        ev = events(app, "TASK_CRASHED")
        assert len(ev) == 1 and ev[0]["level"] == "CRITICAL" and "socket exploded" in ev[0]["message"]
        assert app.counts["restart:news"] == 1
        await until(lambda: app.market_stream.client.ready, what="other tasks unaffected")
    finally:
        app.stop()
        await asyncio.wait_for(task, 10)
        app.close()
        await news.close()
        await iex.close()


# ------------------------------------------------------------------ heartbeat / positions_live / assets
async def test_heartbeat_positions_live_and_periodic_jobs(tmp_path):
    fx = mf.story_fixture()
    app, clock, sim = make_app(tmp_path, fx)
    await app.startup()
    await play(app, clock, fx, until_ts=mf.at(10, 23))
    t = mf.at(10, 23, 30)
    clock.set(t)
    await app.tick(t)
    kv = {r["key"]: r["value"] for r in rows(app, "SELECT key, value FROM kv")}
    assert kv["heartbeat_at"].startswith("2026-01-02T15:23:30")
    live = json.loads(kv["positions_live"])
    assert len(live) == 1 and live[0]["symbol"] == "NVDA" and live[0]["qty"] == 20 and live[0]["entry"] == 101.6
    assert live[0]["last"] > 0 and {"unrealized_usd", "unrealized_r"} <= set(live[0])
    n = len(events(app, "HEARTBEAT"))
    for add in (30, 61, 120):                       # < 60 s after the previous: no new heartbeat
        clock.set(t + timedelta(seconds=add))
        await app.tick()
    assert rows(app, "SELECT value FROM kv WHERE key='heartbeat_at'")[0]["value"].startswith("2026-01-02T15:24:31")
    assert len(events(app, "HEARTBEAT")) == n        # the system_event is only every 10 minutes
    clock.set(t + timedelta(seconds=601))
    await app.tick()
    assert len(events(app, "HEARTBEAT")) == n + 1
    app.close()


async def test_assets_reload_daily_at_0900_et_and_retry_on_failure(tmp_path):
    app, clock, sim = make_app(tmp_path)
    n = lambda: len(events(app, "ASSETS_LOADED"))  # noqa: E731
    clock.set(mf.at(8, 0))
    await app.tick()
    assert n() == 1 and app.assets.get("NVDA")          # nothing loaded yet -> loads straight away
    clock.set(mf.at(9, 1))
    await app.tick()
    assert n() == 2                                    # the daily 09:00 load
    clock.set(mf.at(9, 2))
    await app.tick()
    clock.set(mf.at(15, 0))
    await app.tick()
    assert n() == 2
    from newswave.execution.broker import BrokerUnavailable

    async def down():
        raise BrokerUnavailable("alpaca down")

    sim.get_all_assets = down
    nxt = mf.at(9, 0, 0, mf.SESSION + timedelta(days=3))        # Mon 2026-01-05
    clock.set(nxt - timedelta(minutes=1))
    await app.tick()
    clock.set(nxt)
    await app.tick()
    assert events(app, "ASSETS_FAILED")[0]["level"] == "ERROR" and n() == 2
    app.close()


# ------------------------------------------------------------------ market calendar
def NO_TIME_STOP(tmp_path):  # the 60-minute time stop would close the position long before the EOD checks
    return settings(tmp_path, params=StrategyParams(time_stop_enabled=False))


class HolidaySim(SimBroker):
    async def get_calendar(self, d):
        return None if d == mf.SESSION else await super().get_calendar(d)


class EarlyCloseSim(SimBroker):
    async def get_calendar(self, d):
        if d != mf.SESSION:
            return await super().get_calendar(d)
        return BrokerCalendar(d, mf.at(9, 30), mf.at(13, 0))


async def test_closed_day_keeps_streams_but_nothing_arms(tmp_path):
    fx = mf.story_fixture()
    clock = ReplayClock(T_OPEN)
    app, clock, sim = make_app(tmp_path, fx, clock=clock, sim=_with_assets(HolidaySim(clock, 6000.0)))
    await app.startup()
    await play(app, clock, fx)
    s = rows(app, "SELECT * FROM setups")
    assert [(r["variant"], r["stage"], r["reject_reason"]) for r in s] == [("production", "REJECTED", "MARKET_CLOSED")]
    assert rows(app, "SELECT * FROM orders") == [] and rows(app, "SELECT * FROM ai_classifications") == []
    assert app.subs.desired() == {"SPY", "QQQ"} and app.strategy.active_symbols() == set()
    app.apply_subscriptions()
    assert app.market_stream.subscribed == {"SPY", "QQQ"}     # streams stay wired; only the reserved pair
    app.close()


def _with_assets(sim):
    from newswave.execution.broker import BrokerAsset
    sim.assets = {s: BrokerAsset(symbol=s, name=s, exchange="NASDAQ") for s in ("NVDA", "SPY", "QQQ")}
    return sim


async def test_early_close_day_cutoff_and_flatten_before_the_real_close(tmp_path):
    fx = mf.story_fixture()
    clock = ReplayClock(T_OPEN)
    app, clock, sim = make_app(tmp_path, fx, clock=clock, sim=_with_assets(EarlyCloseSim(clock, 6000.0)),
                               cfg=NO_TIME_STOP(tmp_path))
    await app.startup()
    await play(app, clock, fx, until_ts=mf.at(10, 23))
    assert [p.qty for p in await sim.get_positions()] == [20]
    clock.set(mf.at(12, 29))
    assert app._asset_check("NVDA") is None
    clock.set(mf.at(12, 31))                                # close 13:00 - 30 min
    assert app._asset_check("NVDA") == RejectReason.AFTER_CUTOFF
    await app.tick(mf.at(12, 54, 59))
    assert await sim.get_positions() != []
    clock.set(mf.at(12, 55, 1))
    await app.tick()
    assert await sim.get_positions() == []
    t = rows(app, "SELECT * FROM trades WHERE is_shadow=0")[0]
    assert t["exit_reason"] == "EOD" and events(app, "EARLY_CLOSE")
    clock.set(mf.at(13, 1))
    assert app._asset_check("NVDA") == RejectReason.MARKET_CLOSED
    app.close()


async def test_normal_day_has_no_early_flatten(tmp_path):
    fx = mf.story_fixture()
    app, clock, sim = make_app(tmp_path, fx, cfg=NO_TIME_STOP(tmp_path))
    await app.startup()
    await play(app, clock, fx, until_ts=mf.at(10, 23))
    clock.set(mf.at(15, 54, 50))
    await app.tick()
    assert [p.qty for p in await sim.get_positions()] == [20] and not events(app, "EARLY_CLOSE")
    clock.set(mf.at(15, 55, 1))
    await app.tick()                                        # the engine's own static EOD flatten
    assert rows(app, "SELECT exit_reason FROM trades")[0]["exit_reason"] == "EOD"
    app.close()


# ------------------------------------------------------------------ shutdown
async def test_sigterm_stops_gracefully_without_flattening_and_resubscribes_open_position(tmp_path):
    fx = mf.story_fixture()
    app1, clock, sim1 = make_app(tmp_path, fx)
    await app1.startup()
    await play(app1, clock, fx, until_ts=mf.at(10, 23))
    app1.close()                                            # "crash" with a position open

    news, iex = await start_servers()
    app, clock, sim = make_app(tmp_path, fx, clock=clock, news_url=news.url, iex_url=iex.url)
    fast_ws(app)
    task = asyncio.create_task(app.run())                   # real signal handlers
    try:
        await until(lambda: app.started.is_set(), what="started")
        # boot reconciliation rehydrated the position and its slot -> NVDA is streamed again
        await until(lambda: "NVDA" in iex.state.get("trades", set()), what="rehydrated symbol subscribed")
        assert rows(app, "SELECT COUNT(*) c FROM positions WHERE status='OPEN'")[0]["c"] == 1
        signal.raise_signal(signal.SIGTERM if sys.platform != "win32" else signal.SIGINT)
        await asyncio.wait_for(task, 10)
    finally:
        if not task.done():
            app.stop()
            await task
    assert events(app, "SHUTDOWN") and json.loads(events(app, "SHUTDOWN")[0]["data_json"])["open_positions"] == 1
    assert app.news_stream.client._stop.is_set() and app.market_stream.client._stop.is_set()
    assert [p.qty for p in await sim.get_positions()] == [20]       # not flattened
    assert [o.order_type for o in await sim.get_open_orders()] == ["stop"]  # the protective stop stays
    assert rows(app, "SELECT status FROM positions")[0]["status"] == "OPEN"
    assert signal.getsignal(signal.SIGTERM) is not None
    app.close()
    await news.close()
    await iex.close()


# ------------------------------------------------------------------ review 11: wiring + classifier shutdown
async def test_iex_drop_triggers_strategy_backfill_and_ws_errors_are_logged(tmp_path, monkeypatch):
    from newswave.strategy.engine import StrategyEngine
    calls = []

    async def counting(self):
        calls.append(1)
        return 0
    monkeypatch.setattr(StrategyEngine, "backfill_after_gap", counting)
    news, iex = await start_servers()
    app, clock, sim = make_app(tmp_path, news_url=news.url, iex_url=iex.url)
    fast_ws(app)
    task = asyncio.create_task(app.run(install_signals=False))
    try:
        await until(lambda: app.market_stream.client.ready, what="iex connected")
        assert calls == []  # the first connect is not a reconnect
        await iex.drop()
        await until(lambda: calls, what="backfill_after_gap after the IEX server dropped us")
        assert len(iex.conns) >= 2
        await iex.push([{"T": "error", "code": 405, "msg": "symbol limit exceeded"}])
        await until(lambda: events(app, "MARKET_WS_ERROR"), what="ws error logged as a system_event")
        assert json.loads(events(app, "MARKET_WS_ERROR")[0]["data_json"])["code"] == 405
    finally:
        app.stop()
        await asyncio.wait_for(task, 10)
        app.close()
        await news.close()
        await iex.close()


async def test_news_stream_gets_the_db_for_malformed_frames(tmp_path):
    app, _, _ = make_app(tmp_path)
    assert app.news_stream.db is app.db
    app.close()


async def test_shutdown_kills_an_inflight_classifier_process(tmp_path):
    fake = tmp_path / "fake_claude.py"
    fake.write_text("import time\ntime.sleep(60)\n", encoding="utf-8")
    cfg = settings(tmp_path, claude_bin=str(fake))
    clf = ClaudeCliClassifier(cfg, None)
    app, _, _ = make_app(tmp_path, cfg=cfg, classifier=clf)
    task = asyncio.create_task(clf.classify(NewsEvent(article_id="1", received_at="2026-01-02T15:00:01.000Z", created_at="2026-01-02T15:00:00.000Z",
                                                         updated_at="2026-01-02T15:00:00.000Z", headline="h", summary="", content="",
                                                         symbols=("ACME",), source="benzinga", url=""), "ACME"))
    try:
        await until(lambda: clf._procs, what="fake claude started")
        (proc,) = clf._procs
        app._stop_classifier()
        c = await asyncio.wait_for(task, 5)
        assert c.error and "claude exit" in c.error
        await asyncio.wait_for(proc.wait(), 5)       # portable: reaped, no os.kill(pid, 0)
        assert proc.returncode is not None
    finally:
        task.cancel()
        app.close()


async def test_stop_file_stops_gracefully_and_is_deleted(tmp_path):
    app, _, _ = make_app(tmp_path)
    task = asyncio.create_task(app.run(install_signals=False))
    try:
        await until(lambda: app.started.is_set(), what="started")
        app.stop_file.write_text("", encoding="utf-8")
        await asyncio.wait_for(task, 2.5)
    finally:
        if not task.done():
            app.stop()
            await task
    assert not app.stop_file.exists() and events(app, "SHUTDOWN")
    app.close()


def _fake_signal_module(monkeypatch):
    calls = {}
    monkeypatch.setattr("newswave.daemon.sys.platform", "win32")
    monkeypatch.setattr("newswave.daemon.signal.SIGBREAK", 21, raising=False)
    monkeypatch.setattr("newswave.daemon.signal.signal", lambda s, h: calls.__setitem__(s, h) or "old")
    return calls


async def test_windows_signal_fallback_uses_signal_signal_not_add_signal_handler(tmp_path, monkeypatch):
    app, _, _ = make_app(tmp_path)
    loop = asyncio.get_running_loop()
    monkeypatch.setattr(loop, "add_signal_handler", lambda *a: (_ for _ in ()).throw(NotImplementedError()))
    calls = _fake_signal_module(monkeypatch)
    undo = app._install_signals(loop)
    assert set(calls) == {signal.SIGINT, 21}
    calls[21](21, None)                         # the handler marshals onto the loop
    await asyncio.wait_for(app._stop.wait(), 2)
    undo()
    assert calls[signal.SIGINT] == "old"        # restored
    app.close()
