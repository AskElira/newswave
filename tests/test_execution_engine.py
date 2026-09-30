from __future__ import annotations

import json
from dataclasses import replace
from datetime import timedelta

import pytest

from newswave.clock import utc_iso
from newswave.clock import clear_session_closes, set_session_close
from newswave.execution.broker import BrokerAsset, OrderRejected
from newswave.models import ExitReason as X
from newswave.models import EntrySignal, RiskApproval, Side, Trade
from newswave.market.subscriptions import SlotPriority
from newswave.risk import KillSwitch
from test_execution_helpers import (EOD, P, T0, alpaca_env, bar, events, make_env, msgs, new_env_on, new_setup,
                                    orders, px, sig, stops_open)


@pytest.fixture(autouse=True)
def _fresh_sessions():
    clear_session_closes()
    yield
    clear_session_closes()


async def open_long(env, symbol="ABC", price=50.0, **kw):
    """Entry on SimBroker: print at `price`, signal, fill at the print. Returns setup_id."""
    sid = new_setup(env.db, symbol)
    await px(env, symbol, price)
    await env.eng.on_entry_signal(sig(sid, symbol, **kw))
    return sid


def pos_row(db, pid=1):
    return db.one("SELECT * FROM positions WHERE id=?", (pid,))


# ------------------------------------------------------------------ the happy path, end to end
async def test_entry_partial_trail_full_lifecycle(tmp_db):
    env = make_env(tmp_db)
    sid = await open_long(env)
    # --- entry: limit order, fill at the print, stop for the FULL qty
    e = orders(tmp_db, "ENTRY")[0]
    assert e["client_order_id"] == f"nw-{env.eng.uid}-{sid}-ENTRY-1" and e["status"] == "filled"
    assert e["filled_qty"] == 28 and e["filled_avg_price"] == 50.0 and e["limit_price"] == 50.10
    st = stops_open(env.sim)[0]
    assert st.client_order_id == f"nw-{env.eng.uid}-{sid}-STOP-1" and st.qty == 28 and st.stop_price == 48.0
    p = pos_row(tmp_db)
    assert (p["qty_initial"], p["qty_open"], p["entry_price"], p["stop_price"], p["risk_per_share"]) == (
        28, 28, 50.0, 48.0, 2.0)  # risk/share from the ACTUAL fill
    assert p["status"] == "OPEN" and p["is_shadow"] == 0 and p["variant"] == "production"
    s = tmp_db.one("SELECT * FROM setups WHERE id=?", (sid,))
    assert s["stage"] == "IN_POSITION" and s["max_stage"] == "IN_POSITION"
    assert "BUY 28 ABC @ 50.00 (stop 48.00, risk $56)" in msgs(tmp_db)
    assert env.subs.priority("ABC") == SlotPriority.POSITION and env.subs.owners("ABC") == {"pos-1"}
    assert tmp_db.one("SELECT COUNT(*) n FROM fills")["n"] == 1
    snaps = tmp_db.query("SELECT * FROM equity_snapshots")
    assert snaps and snaps[-1]["ledger_equity"] == pytest.approx(6000) and snaps[-1]["broker_equity"] is not None
    assert [d["symbol"] for d in env.eng.open_positions()] == ["ABC"]
    # --- +1R (52.0): stop resized to the remaining 14 FIRST (sim refuses the sell otherwise), then sell 14
    await px(env, "ABC", 52.0)
    part = orders(tmp_db, "PARTIAL")[0]
    assert part["client_order_id"] == f"nw-{env.eng.uid}-{sid}-PARTIAL-1" and part["filled_qty"] == 14
    stops = orders(tmp_db, "STOP")
    assert [o["client_order_id"] for o in stops] == [f"nw-{env.eng.uid}-{sid}-STOP-1", f"nw-{env.eng.uid}-{sid}-STOP-2"]
    assert stops[0]["status"] == "replaced" and stops[0]["id"] < part["id"] and stops[1]["id"] < part["id"]
    assert stops_open(env.sim)[0].qty == 14
    assert pos_row(tmp_db)["qty_open"] == 14 and pos_row(tmp_db)["partial_taken"] == 1
    assert any(m.startswith("+1.0R -> sold 50%") for m in msgs(tmp_db))
    # --- trail: ATR 1 -> highest 54 - 2 = 52 ; close 53 stays, then close 51.5 < 52 exits
    await env.eng.on_bar_5m("ABC", bar("ABC", 53.0, high=54.0), {"atr": 1.0, "ema9": 52.0})
    assert pos_row(tmp_db)["trail_price"] == 52.0
    assert "trail set at 52.00" in msgs(tmp_db)
    await px(env, "ABC", 51.5)
    await env.eng.on_bar_5m("ABC", bar("ABC", 51.5, high=52.0, low=51.4), {"atr": 1.0, "ema9": 52.0})
    assert "5m close 51.50 below ATR trail 52.00" in msgs(tmp_db)
    # --- closed
    assert stops_open(env.sim) == [] and env.eng.open_positions() == []
    t = tmp_db.one("SELECT * FROM trades")
    assert t["exit_reason"] == "TRAIL" and t["qty"] == 28 and t["entry_price"] == 50.0
    assert t["pnl"] == pytest.approx(14 * 2.0 + 14 * 1.5) and t["avg_exit_price"] == pytest.approx(51.75)
    assert t["r_multiple"] == pytest.approx(49 / (28 * 2.0)) and t["is_shadow"] == 0
    assert t["catalyst"] == "guidance raise" and t["ai_confidence"] == 0.9 and t["rvol"] == 3.0
    assert t["news_latency_s"] == 1.5 and t["impulse_pct"] == 2.5 and t["atr"] == 1.0
    assert t["mfe_r"] == pytest.approx(2.0) and t["mae_r"] == pytest.approx(0.0)
    assert pos_row(tmp_db)["status"] == "CLOSED" and pos_row(tmp_db)["qty_open"] == 0
    assert tmp_db.one("SELECT stage FROM setups WHERE id=?", (sid,))["stage"] == "CLOSED"
    assert env.subs.priority("ABC") is None
    assert "position closed: ABC TRAIL pnl $+49.00 (+0.88R)" in msgs(tmp_db)
    assert env.eng.account_snapshot()["total_pnl"] == pytest.approx(49.0)
    assert env.eng.account_snapshot()["ledger_equity"] == pytest.approx(6049.0)
    assert env.eng.account_snapshot()["daily_pnl"] == pytest.approx(49.0)
    ds = tmp_db.one("SELECT * FROM daily_stats")
    assert ds["realized_pnl"] == pytest.approx(49.0) and ds["start_equity"] == 6000 and ds["trades"] == 1


async def test_latency_fields_populated(tmp_db):
    env = make_env(tmp_db)
    sid = new_setup(tmp_db)
    await px(env, "ABC", 50.0)
    await env.eng.on_entry_signal(sig(sid, signal_at=utc_iso(T0 - timedelta(milliseconds=1250))))
    e = orders(tmp_db, "ENTRY")[0]
    assert e["signal_at"] == utc_iso(T0 - timedelta(milliseconds=1250))
    assert e["submitted_at"] and e["ack_at"] and e["filled_at"] and e["broker_order_id"]
    json.loads(e["raw_json"])
    await px(env, "ABC", 47.0)  # stop
    t = tmp_db.one("SELECT * FROM trades")
    assert t["entry_latency_ms"] == pytest.approx(1250.0) and t["news_latency_s"] == 1.5


async def test_short_lifecycle_and_direction_math(tmp_db):
    env = make_env(tmp_db)
    sid = new_setup(tmp_db, side="SHORT")
    await px(env, "ABC", 50.0)
    await env.eng.on_entry_signal(sig(sid, side=Side.SHORT, lo=49.5, hi=50.5))
    p = pos_row(tmp_db)
    assert p["side"] == "SHORT" and p["qty_initial"] == 42 and p["stop_price"] == 50.65
    e = orders(tmp_db, "ENTRY")[0]
    assert e["side"] == "sell" and stops_open(env.sim)[0].side == "buy"
    assert "SHORT 42 ABC @ 50.00 (stop 50.65, risk $27)" in msgs(tmp_db)
    await px(env, "ABC", 50.7)  # buy-stop fires at the print
    t = tmp_db.one("SELECT * FROM trades")
    assert t["exit_reason"] == "STOP" and t["pnl"] == pytest.approx(42 * (50.0 - 50.7))
    assert orders(tmp_db, "EXIT") == []


# ------------------------------------------------------------------ broker-resident stop
async def test_broker_stop_fill_detected_on_the_print(tmp_db):
    env = make_env(tmp_db)
    await open_long(env)
    await px(env, "ABC", 47.9)  # sim stop fires at the print; engine must record THAT fill, not sell again
    t = tmp_db.one("SELECT * FROM trades")
    assert t["exit_reason"] == "STOP" and t["avg_exit_price"] == pytest.approx(47.9)
    assert t["pnl"] == pytest.approx(28 * -2.1)
    assert orders(tmp_db, "EXIT") == [] and orders(tmp_db, "PARTIAL") == []
    assert env.sim._pos == {} and env.eng.open_positions() == []


async def test_broker_stop_fill_detected_on_clock_poll(tmp_db):
    env = make_env(tmp_db)
    await open_long(env)
    await env.sim.on_trade(Trade("ABC", utc_iso(T0), 47.0, 1))
    assert len(env.eng.open_positions()) == 1  # engine has not seen it yet
    env.clock.advance(seconds=5)
    await env.eng.on_clock(env.clock.now())
    t = tmp_db.one("SELECT * FROM trades")
    assert t["exit_reason"] == "STOP" and t["avg_exit_price"] == 47.0
    assert tmp_db.one("SELECT qty_open, status FROM positions WHERE id=1") == {"qty_open": 0, "status": "CLOSED"}
    assert "stop hit -> sold 28 ABC @ 47.00" in msgs(tmp_db)


async def test_fill_beyond_stop_exits_immediately(tmp_db):
    env = make_env(tmp_db)
    sid = new_setup(tmp_db)
    await px(env, "ABC", 47.5)  # market already below the 48.0 stop: limit 50.10 is marketable at 47.5
    await env.eng.on_entry_signal(sig(sid))
    t = tmp_db.one("SELECT * FROM trades")
    assert t["exit_reason"] == "STOP" and t["qty"] > 0 and t["pnl"] == pytest.approx(0.0)
    assert any(e["event"] == "FILL_BEYOND_STOP" for e in events(tmp_db))
    assert stops_open(env.sim) == [] and env.sim._pos == {}


# ------------------------------------------------------------------ entry fill handling
async def test_entry_timeout_cancels_and_rejects(tmp_db):
    env = make_env(tmp_db)
    sid = new_setup(tmp_db)
    await env.eng.on_entry_signal(sig(sid))  # no print yet: the limit order just rests
    assert orders(tmp_db, "ENTRY")[0]["status"] == "new"
    env.clock.advance(seconds=5)
    await env.eng.on_clock(env.clock.now())
    assert orders(tmp_db, "ENTRY")[0]["status"] == "new" and len(env.eng._entries) == 1
    env.clock.advance(seconds=5.1)
    await env.eng.on_clock(env.clock.now())
    assert orders(tmp_db, "ENTRY")[0]["status"] == "canceled" and not env.eng._entries
    s = tmp_db.one("SELECT * FROM setups WHERE id=?", (sid,))
    assert s["stage"] == "REJECTED" and s["reject_reason"] == "ENTRY_NOT_FILLED"
    assert tmp_db.one("SELECT COUNT(*) n FROM positions")["n"] == 0 and orders(tmp_db, "STOP") == []
    assert await env.sim.get_open_orders() == []


async def test_entry_fills_late_but_inside_the_window(tmp_db):
    env = make_env(tmp_db)
    sid = new_setup(tmp_db)
    await px(env, "ABC", 51.0)  # above the 50.10 limit: rests
    await env.eng.on_entry_signal(sig(sid))
    assert len(env.eng._entries) == 1
    env.clock.advance(seconds=2)
    await px(env, "ABC", 50.05)  # print through the limit -> fills -> engine notices in the same call
    assert tmp_db.one("SELECT qty_open, entry_price FROM positions WHERE id=1") == {"qty_open": 28, "entry_price": 50.05}


async def test_partial_entry_fill_keeps_filled_qty_and_cancels_rest(tmp_db):
    env = make_env(tmp_db)
    env.sim.partial_cap = 10
    sid = new_setup(tmp_db)
    await px(env, "ABC", 50.0)
    await env.eng.on_entry_signal(sig(sid))
    assert len(env.eng._entries) == 1 and tmp_db.one("SELECT COUNT(*) n FROM positions")["n"] == 0
    env.clock.advance(seconds=10.5)
    await env.eng.on_clock(env.clock.now())
    p = pos_row(tmp_db)
    assert p["qty_initial"] == 10 and p["qty_open"] == 10
    assert stops_open(env.sim)[0].qty == 10 and [o.client_order_id for o in await env.sim.get_open_orders()] == [o.client_order_id for o in stops_open(env.sim)]
    assert orders(tmp_db, "ENTRY")[0]["status"] == "canceled" and orders(tmp_db, "ENTRY")[0]["filled_qty"] == 10
    assert any("partial entry fill: kept 10 of 28" in m for m in msgs(tmp_db))
    assert "BUY 10 ABC @ 50.00 (stop 48.00, risk $20)" in msgs(tmp_db)


async def test_entry_submit_rejected_marks_setup(tmp_db):
    env = make_env(tmp_db)

    async def boom(*a, **k):
        raise OrderRejected("insufficient buying power", 403)
    env.sim.submit_limit = boom
    sid = new_setup(tmp_db)
    await env.eng.on_entry_signal(sig(sid))
    s = tmp_db.one("SELECT * FROM setups WHERE id=?", (sid,))
    assert s["stage"] == "REJECTED" and s["reject_reason"] == "ORDER_REJECTED"
    o = orders(tmp_db, "ENTRY")[0]
    assert o["status"] == "rejected" and "insufficient" in o["error"]


async def test_duplicate_signal_ignored(tmp_db):
    env = make_env(tmp_db)
    sid = await open_long(env)
    await env.eng.on_entry_signal(sig(sid))
    assert len(orders(tmp_db, "ENTRY")) == 1 and len(env.sim._orders) == 2


async def test_risk_reject_updates_setup_and_timeline(tmp_db):
    env = make_env(tmp_db, params=replace(P, max_concurrent_positions=1))
    await open_long(env, "ABC")
    sid2 = new_setup(tmp_db, "DEF")
    await px(env, "DEF", 30.0)
    await env.eng.on_entry_signal(sig(sid2, "DEF", trigger=30.0, lo=29.0, hi=30.5))
    s = tmp_db.one("SELECT * FROM setups WHERE id=?", (sid2,))
    assert s["stage"] == "REJECTED" and s["reject_reason"] == "MAX_POSITIONS" and s["closed_at"]
    assert "entry rejected: MAX_POSITIONS" in msgs(tmp_db)
    assert orders(tmp_db, "ENTRY")[-1]["symbol"] == "ABC"  # nothing sent for DEF


async def test_pending_entries_count_against_limits(tmp_db):
    env = make_env(tmp_db, params=replace(P, max_concurrent_positions=1))
    a, b = new_setup(tmp_db, "ABC"), new_setup(tmp_db, "DEF")
    await env.eng.on_entry_signal(sig(a))  # rests (no print)
    await env.eng.on_entry_signal(sig(b, "DEF", trigger=30.0, lo=29.0, hi=30.5))
    assert tmp_db.one("SELECT reject_reason r FROM setups WHERE id=?", (b,))["r"] == "MAX_POSITIONS"


async def test_shadow_signal_never_reaches_the_broker(tmp_db):
    env = make_env(tmp_db)
    sid = new_setup(tmp_db)
    await env.eng.on_entry_signal(replace(sig(sid), variant="neutral_news"))
    assert orders(tmp_db) == [] and any(e["event"] == "SHADOW_SIGNAL" for e in events(tmp_db))


# ------------------------------------------------------------------ shorts availability
@pytest.mark.parametrize("short,etb", [(False, True), (True, False)])
async def test_short_unavailable_checked_at_signal_time(tmp_db, short, etb):
    env = make_env(tmp_db)
    env.sim.assets["ABC"] = BrokerAsset("ABC", shortable=short, easy_to_borrow=etb)
    sid = new_setup(tmp_db, side="SHORT")
    await px(env, "ABC", 50.0)
    await env.eng.on_entry_signal(sig(sid, side=Side.SHORT, lo=49.5, hi=50.5))
    s = tmp_db.one("SELECT * FROM setups WHERE id=?", (sid,))
    assert s["stage"] == "REJECTED" and s["reject_reason"] == "SHORT_UNAVAILABLE"
    assert orders(tmp_db) == []
    assert any("SHORT_UNAVAILABLE" in m for m in msgs(tmp_db))


async def test_long_does_not_need_shortable(tmp_db):
    env = make_env(tmp_db)
    env.sim.assets["ABC"] = BrokerAsset("ABC", shortable=False, easy_to_borrow=False)
    await open_long(env)
    assert len(env.eng.open_positions()) == 1


# ------------------------------------------------------------------ other exits
async def test_opposite_news_exit(tmp_db):
    env = make_env(tmp_db)
    await open_long(env)
    await env.eng.on_opposite_news("ABC", Side.LONG)  # same direction: nothing
    assert len(env.eng.open_positions()) == 1
    await env.eng.on_opposite_news("OTHER", Side.SHORT)
    assert len(env.eng.open_positions()) == 1
    await px(env, "ABC", 50.4)
    await env.eng.on_opposite_news("ABC", Side.SHORT)
    t = tmp_db.one("SELECT * FROM trades")
    assert t["exit_reason"] == "OPPOSITE_NEWS" and t["avg_exit_price"] == 50.4
    assert [o["purpose"] for o in orders(tmp_db)] == ["ENTRY", "STOP", "EXIT"]
    assert stops_open(env.sim) == []


async def test_eod_flatten_positions_and_pending_entries(tmp_db):
    env = make_env(tmp_db, params=replace(P, time_stop_enabled=False))
    await open_long(env)
    sid2 = new_setup(tmp_db, "DEF")
    await env.eng.on_entry_signal(sig(sid2, "DEF", trigger=30.0, lo=29.0, hi=30.5))  # rests
    env.clock.set(EOD - timedelta(seconds=1))
    await env.eng.on_clock(env.clock.now())
    assert len(env.eng.open_positions()) == 1  # 15:54:59
    env.clock.set(EOD)
    await env.eng.on_clock(env.clock.now())
    t = tmp_db.one("SELECT * FROM trades")
    assert t["exit_reason"] == "EOD" and len(orders(tmp_db, "EXIT")) == 1
    assert env.sim._pos == {} and await env.sim.get_open_orders() == []
    assert tmp_db.one("SELECT reject_reason r FROM setups WHERE id=?", (sid2,))["r"] == "ENTRY_NOT_FILLED"
    await env.eng.on_clock(env.clock.now())  # idempotent
    assert len(orders(tmp_db, "EXIT")) == 1


async def test_time_stop_via_clock(tmp_db):
    env = make_env(tmp_db)
    await open_long(env)
    env.clock.advance(minutes=61)
    await env.eng.on_clock(env.clock.now())
    assert tmp_db.one("SELECT exit_reason FROM trades")["exit_reason"] == "TIME_STOP"


async def test_non_tradable_exit_via_status_and_audit(tmp_db):
    env = make_env(tmp_db)
    await open_long(env)
    await env.eng.on_status({"symbol": "ABC", "halted": True})
    assert len(env.eng.open_positions()) == 1
    assert any(e["event"] == "HALT_OPEN_POSITION" for e in events(tmp_db, "WARNING"))
    await env.eng.on_status({"symbol": "ABC", "halted": False, "tradable": False})
    assert tmp_db.one("SELECT exit_reason FROM trades")["exit_reason"] == "NON_TRADABLE"
    # periodic asset check (clock) catches it too
    env2 = make_env(tmp_db, version="v2")
    sid = new_setup(tmp_db, "XYZ", version="v2")
    await px(env2, "XYZ", 50.0)
    await env2.eng.on_entry_signal(sig(sid, "XYZ"))
    env2.sim.assets["XYZ"] = BrokerAsset("XYZ", tradable=False)
    await env2.eng.on_clock(env2.clock.now())
    assert tmp_db.one("SELECT exit_reason FROM trades WHERE strategy_version='v2'")["exit_reason"] == "NON_TRADABLE"


# ------------------------------------------------------------------ failed exit order -> retry
async def test_failed_partial_is_rearmed_stop_restored_and_retried(tmp_db):
    env = make_env(tmp_db)
    await open_long(env)
    real = env.sim.submit_market
    calls = []

    async def flaky(*a, **k):
        calls.append(a)
        if len(calls) == 1:
            raise OrderRejected("boom", 422)
        return await real(*a, **k)
    env.sim.submit_market = flaky
    await px(env, "ABC", 52.0)
    assert any(e["event"] == "EXIT_FAILED" for e in events(tmp_db, "ERROR"))
    p = pos_row(tmp_db)
    assert p["qty_open"] == 28 and stops_open(env.sim)[0].qty == 28  # stop back at full size
    assert orders(tmp_db, "PARTIAL")[0]["status"] == "rejected"
    await px(env, "ABC", 52.1)  # still inside the retry back-off: nothing happens
    assert len(calls) == 1
    env.clock.advance(seconds=4)
    await px(env, "ABC", 52.2)  # next event retries (partial re-armed)
    assert len(calls) == 2 and pos_row(tmp_db)["qty_open"] == 14 and stops_open(env.sim)[0].qty == 14
    assert orders(tmp_db, "PARTIAL")[1]["client_order_id"].endswith("PARTIAL-2")


async def test_failed_full_exit_clears_pending_and_retries(tmp_db):
    env = make_env(tmp_db, params=replace(P, time_stop_enabled=False))
    await open_long(env)
    real = env.sim.submit_market
    n = []

    async def flaky(*a, **k):
        n.append(1)
        if len(n) <= 2:  # EOD exit, then the forced flatten in the same tick, both fail
            raise OrderRejected("boom", 422)
        return await real(*a, **k)
    env.sim.submit_market = flaky
    env.clock.set(EOD)
    await env.eng.on_clock(env.clock.now())  # EOD exit fails; stop re-placed
    assert len(env.eng.open_positions()) == 1 and len(stops_open(env.sim)) == 1
    env.clock.advance(seconds=5)
    await env.eng.on_clock(env.clock.now())
    assert tmp_db.one("SELECT exit_reason FROM trades")["exit_reason"] == "EOD"


async def test_exit_order_partial_fill_then_cancel_is_repaired(tmp_db):
    """A market exit that dies unfilled (canceled) must not strand the stop or the pending flag."""
    env = make_env(tmp_db)
    await open_long(env)
    real = env.sim.submit_market

    async def dead(symbol, side, qty, cid):
        o = await real(symbol, side, qty, cid)
        env.sim._orders[cid].status = "canceled"  # e.g. broker killed it
        env.sim._orders[cid].filled_qty = 0
        return env.sim._snap(env.sim._orders[cid])
    env.sim.submit_market = dead
    await env.eng.on_opposite_news("ABC", Side.SHORT)
    assert len(env.eng.open_positions()) == 1 and len(stops_open(env.sim)) == 1
    assert env.eng._pos[1].mp.exit_pending is False and env.eng._pos[1].pending is None


# ------------------------------------------------------------------ kill switch
async def test_kill_switch_trips_flattens_cancels_and_persists(tmp_db):
    params = replace(P, max_daily_loss_pct=0.5)  # $30 of 6000
    env = make_env(tmp_db, params=params)
    await open_long(env)
    sid2 = new_setup(tmp_db, "DEF")
    await env.eng.on_entry_signal(sig(sid2, "DEF", trigger=30.0, lo=29.0, hi=30.5))  # rests
    await px(env, "ABC", 49.0)  # -28 unrealized: below the line
    assert len(env.eng.open_positions()) == 1 and not env.kill.is_disabled(T0.date())
    await px(env, "ABC", 48.5)  # -42 <= -30 -> trip
    assert env.kill.is_disabled(T0.date())
    assert env.eng.open_positions() == [] and env.sim._pos == {} and await env.sim.get_open_orders() == []
    t = tmp_db.one("SELECT * FROM trades")
    assert t["exit_reason"] == "RISK_KILL" and t["pnl"] == pytest.approx(28 * -1.5)
    assert tmp_db.one("SELECT reject_reason r FROM setups WHERE id=?", (sid2,))["r"] == "ENTRY_NOT_FILLED"
    assert any(e["event"] == "KILL_SWITCH" for e in events(tmp_db, "CRITICAL"))
    assert any(m.startswith("KILL SWITCH") for m in msgs(tmp_db))
    ds = tmp_db.one("SELECT * FROM daily_stats")
    assert ds["trading_disabled"] == 1 and ds["kill_switch_at"]
    # rest of session: rejected
    sid3 = new_setup(tmp_db, "GHI")
    await px(env, "GHI", 20.0)
    await env.eng.on_entry_signal(sig(sid3, "GHI", trigger=20.0, lo=19.0, hi=20.5))
    assert tmp_db.one("SELECT reject_reason r FROM setups WHERE id=?", (sid3,))["r"] == "KILL_SWITCH"
    # a brand-new engine on the same db (process restart) stays disabled
    env2 = new_env_on(env, params=params)
    sid4 = new_setup(tmp_db, "JKL")
    await px(env2, "JKL", 20.0)
    await env2.eng.on_entry_signal(sig(sid4, "JKL", trigger=20.0, lo=19.0, hi=20.5))
    assert tmp_db.one("SELECT reject_reason r FROM setups WHERE id=?", (sid4,))["r"] == "KILL_SWITCH"
    assert orders(tmp_db, "ENTRY")[-1]["symbol"] != "JKL"
    # next session is a fresh day
    env2.clock.advance(days=3)
    assert not KillSwitch(tmp_db, "v1").is_disabled(env2.clock.now().date())


async def test_kill_switch_counts_realized_losses_too(tmp_db):
    env = make_env(tmp_db, params=replace(P, max_daily_loss_pct=0.5))
    await open_long(env)
    await px(env, "ABC", 47.0)  # stop fills at 47: -84 realized
    assert env.kill.is_disabled(T0.date())


# ------------------------------------------------------------------ forging approvals
def test_risk_approval_cannot_be_forged_outside_risk_py():
    s = EntrySignal(1, "A", Side.LONG, 50, 49, 51, 1, "2026-01-02T15:00:00.000Z")
    with pytest.raises(PermissionError):
        RiskApproval(signal=s, qty=1000, entry_price=50, limit_price=50, stop_price=49, risk_per_share=1,
                     risk_dollars=1)
    with pytest.raises(PermissionError):
        RiskApproval(s, 1, 50, 50, 49, 1, 1, token=object())
    from pathlib import Path
    src = (Path(__file__).resolve().parents[1] / "newswave" / "execution" / "engine.py").read_text(encoding="utf-8")
    assert "RiskApproval(" not in src and "_RISK_TOKEN" not in src


# ------------------------------------------------------------------ same engine over AlpacaPaperBroker + fake client
async def test_entry_over_alpaca_broker_with_submit_timeout_no_duplicate(tmp_db):
    env, fake = alpaca_env(tmp_db)
    sid = new_setup(tmp_db)
    await fake.sim.on_trade(Trade("ABC", utc_iso(T0), 50.0, 1))
    fake.script = ["timeout_after"]  # the order was accepted; the reply was lost
    await env.eng.on_entry_signal(sig(sid))
    entries = [o for o in fake.sim._orders.values() if o.cid.endswith("ENTRY-1")]
    assert len(entries) == 1 and fake.submit_calls == 2  # 1 entry (no resend) + 1 stop
    assert len(orders(tmp_db, "ENTRY")) == 1 and pos_row(tmp_db)["qty_open"] == 28
    st = [o for o in await env.broker.get_open_orders() if o.order_type == "stop"]
    assert len(st) == 1 and st[0].qty == 28


async def test_entry_unknown_outcome_is_polled_not_assumed_failed(tmp_db):
    env, fake = alpaca_env(tmp_db)
    sid = new_setup(tmp_db)
    await fake.sim.on_trade(Trade("ABC", utc_iso(T0), 50.0, 1))
    fake.script = ["timeout_after"]
    fake.lookup_fail = 99  # broker unreachable for lookups too
    await env.eng.on_entry_signal(sig(sid))
    assert any(e["event"] == "ENTRY_UNKNOWN" for e in events(tmp_db, "CRITICAL"))
    assert len(env.eng._entries) == 1 and tmp_db.one("SELECT reject_reason r FROM setups WHERE id=?", (sid,))["r"] is None
    fake.lookup_fail = 0  # connectivity returns: the next poll finds the filled order
    env.clock.advance(seconds=1)
    await env.eng.on_clock(env.clock.now())
    assert pos_row(tmp_db)["qty_open"] == 28 and fake.submit_calls == 2


async def test_alpaca_kind_uses_broker_daytrade_count_for_pdt(tmp_db):
    env, fake = alpaca_env(tmp_db)
    fake.daytrade_count = 3
    sid = new_setup(tmp_db)
    await fake.sim.on_trade(Trade("ABC", utc_iso(T0), 50.0, 1))
    await env.eng.on_entry_signal(sig(sid))
    assert tmp_db.one("SELECT reject_reason r FROM setups WHERE id=?", (sid,))["r"] == "PDT_LIMIT"
    assert fake.submit_calls == 0


async def test_load_assets_returns_plain_dicts(tmp_db):
    env = make_env(tmp_db)
    env.sim.assets["AAA"] = BrokerAsset("AAA", name="Triple A", exchange="NYSE")
    a = await env.eng.load_assets()
    assert a == [{"symbol": "AAA", "name": "Triple A", "exchange": "NYSE", "asset_class": "us_equity",
                  "tradable": True, "shortable": True, "easy_to_borrow": True, "status": "active"}]


# ------------------------------------------------------------------ in-flight exit orders
async def test_unfilled_exit_times_out_stop_restored_and_queued_exit_runs(tmp_db):
    env = make_env(tmp_db)
    await open_long(env)
    env.sim._last.clear()  # no print: the market exit order rests instead of filling
    await env.eng.on_opposite_news("ABC", Side.SHORT)
    c = env.eng._pos[1]
    assert c.pending is not None and stops_open(env.sim) == []  # stop was cancelled BEFORE the market order
    await env.eng.flatten_all(X.RISK_KILL)  # an exit is already in flight: queue, never a second order
    assert c.queued == X.RISK_KILL and len(orders(tmp_db, "EXIT")) == 1
    env.clock.advance(seconds=16)
    await env.eng.on_clock(env.clock.now())  # deadline: cancel sent
    env.clock.advance(seconds=1)
    await env.eng.on_clock(env.clock.now())  # canceled -> EXIT_FAILED (+stop restored) -> queued RISK_KILL runs
    assert any(e["event"] == "EXIT_FAILED" for e in events(tmp_db, "ERROR"))
    ex = orders(tmp_db, "EXIT")
    assert [o["status"] for o in ex] == ["canceled", "new"] and ex[1]["client_order_id"].endswith("EXIT-2")
    assert c.pending is not None and c.queued is None
    env.clock.advance(seconds=1)  # polls are >= 0.5 s apart on the injected clock
    await px(env, "ABC", 50.3)  # a print arrives: the resting order fills, engine sees it on the same call
    t = tmp_db.one("SELECT * FROM trades")
    assert t["exit_reason"] == "RISK_KILL" and t["avg_exit_price"] == 50.3


async def test_exit_submit_unknown_outcome_is_polled_never_resent(tmp_db):
    env, fake = alpaca_env(tmp_db)
    sid = new_setup(tmp_db)
    await fake.sim.on_trade(Trade("ABC", utc_iso(T0), 50.0, 1))
    await env.eng.on_entry_signal(sig(sid))
    assert pos_row(tmp_db)["qty_open"] == 28
    before = fake.submit_calls
    fake.script = ["timeout_after"]
    fake.lookup_fail = 99
    await env.eng.on_opposite_news("ABC", Side.SHORT)
    assert any(e["event"] == "EXIT_UNKNOWN" for e in events(tmp_db, "CRITICAL"))
    assert fake.submit_calls == before + 1 and env.eng._pos[1].pending.unknown
    fake.lookup_fail = 0
    env.clock.advance(seconds=1)
    await env.eng.on_clock(env.clock.now())
    assert fake.submit_calls == before + 1  # never sent a second exit
    assert tmp_db.one("SELECT exit_reason FROM trades")["exit_reason"] == "OPPOSITE_NEWS"
    assert fake.sim._pos == {}


async def test_entry_order_vanished_at_broker_is_given_up_loudly_after_grace(tmp_db):
    env, fake = alpaca_env(tmp_db)
    sid = new_setup(tmp_db)
    await fake.sim.on_trade(Trade("ABC", utc_iso(T0), 50.0, 1))
    fake.script = ["timeout_before"]  # never reached the broker
    fake.lookup_fail = 99
    await env.eng.on_entry_signal(sig(sid))
    assert len(env.eng._entries) == 1
    fake.lookup_fail = 0  # lookups now answer a definite "no such order"
    env.clock.advance(seconds=20)
    await env.eng.on_clock(env.clock.now())
    assert len(env.eng._entries) == 1  # still inside deadline + grace
    env.clock.advance(seconds=25)
    await env.eng.on_clock(env.clock.now())
    assert not env.eng._entries
    assert any(e["event"] == "ENTRY_GONE" for e in events(tmp_db, "CRITICAL"))
    assert tmp_db.one("SELECT reject_reason r FROM setups WHERE id=?", (sid,))["r"] == "ENTRY_NOT_FILLED"


# ================================================================== review fixes
async def _tick(env, seconds=1.0):
    env.clock.advance(seconds=seconds)
    await env.eng.on_clock(env.clock.now())


# ------------------------------------------------------------------ 2: async cancel / replace
async def test_full_exit_waits_for_pending_cancel_before_selling(tmp_db):
    """REGRESSION: the sell used to go out while the stop was still pending_cancel (shares held) -> 403."""
    env = make_env(tmp_db)
    env.sim.settle_lag = 2
    await open_long(env)
    await env.eng.on_opposite_news("ABC", Side.SHORT)
    assert orders(tmp_db, "EXIT") == []  # stop is pending_cancel: nothing sold yet
    assert env.sim._orders[f"nw-{env.eng.uid}-1-STOP-1"].status == "pending_cancel"
    await _tick(env)  # settles (canceled) -> sell goes out
    (e,) = orders(tmp_db, "EXIT")
    assert e["status"] == "filled" and e["filled_qty"] == 28
    assert tmp_db.one("SELECT exit_reason FROM trades")["exit_reason"] == "OPPOSITE_NEWS"
    assert not events(tmp_db, "ERROR") and env.sim._pos == {}


async def test_partial_exit_waits_for_pending_replace(tmp_db):
    env = make_env(tmp_db)
    env.sim.settle_lag = 2
    await open_long(env)
    await px(env, "ABC", 52.0)  # +1R: replace the stop to 14 (pending_replace), sell waits
    assert orders(tmp_db, "PARTIAL") == [] and pos_row(tmp_db)["qty_open"] == 28
    await _tick(env)
    assert orders(tmp_db, "PARTIAL")[0]["filled_qty"] == 14 and pos_row(tmp_db)["qty_open"] == 14
    assert not events(tmp_db, "ERROR")


async def test_stop_filling_during_the_cancel_is_the_exit(tmp_db):
    env = make_env(tmp_db)
    env.sim.settle_lag = 3
    await open_long(env)
    await env.eng.on_opposite_news("ABC", Side.SHORT)
    assert orders(tmp_db, "EXIT") == []
    await env.sim.on_trade(Trade("ABC", utc_iso(env.clock.now()), 47.0, 1))  # fills while pending_cancel
    await _tick(env)
    t = tmp_db.one("SELECT * FROM trades")
    assert t["exit_reason"] == "STOP" and t["avg_exit_price"] == 47.0
    assert orders(tmp_db, "EXIT") == [] and env.sim._pos == {} and env.eng.open_positions() == []


async def test_held_shares_refusal_of_the_sell_is_retried_not_a_failed_exit(tmp_db):
    env = make_env(tmp_db)
    await open_long(env)
    real, refusals = env.sim.submit_market, [2]

    async def flaky(*a, **k):
        if refusals[0] > 0:
            refusals[0] -= 1
            raise OrderRejected("insufficient qty available for order (requested: 28, available: 0)", 403)
        return await real(*a, **k)
    env.sim.submit_market = flaky
    await env.eng.on_opposite_news("ABC", Side.SHORT)
    assert not events(tmp_db, "ERROR") and env.eng._pos[1].waiting is not None
    await _tick(env, 1)
    await _tick(env, 1)
    assert tmp_db.one("SELECT exit_reason FROM trades")["exit_reason"] == "OPPOSITE_NEWS"
    assert not events(tmp_db, "ERROR") and not events(tmp_db, "CRITICAL")


async def test_sell_refused_past_the_window_replaces_the_stop_and_goes_critical(tmp_db):
    env = make_env(tmp_db)
    await open_long(env)

    async def refuse(*a, **k):
        raise OrderRejected("insufficient qty available for order (requested: 28, available: 0)", 403)
    env.sim.submit_market = refuse
    await env.eng.on_opposite_news("ABC", Side.SHORT)
    for _ in range(4):
        await _tick(env, 1)
    assert any(e["event"] == "EXIT_SELL_FAILED" for e in events(tmp_db, "CRITICAL"))
    assert [o.qty for o in stops_open(env.sim)] == [28]  # never left without a live stop
    assert len(env.eng.open_positions()) == 1


# ------------------------------------------------------------------ 5: one print is not a stop
async def test_single_print_through_stop_leaves_the_broker_stop_alone(tmp_db):
    env, fake = alpaca_env(tmp_db)
    sid = new_setup(tmp_db)
    await fake.sim.on_trade(Trade("ABC", utc_iso(T0), 50.0, 1))
    await env.eng.on_entry_signal(sig(sid))
    await px(env, "ABC", 47.9)  # odd print: the broker never saw it
    assert orders(tmp_db, "EXIT") == [] and len(stops_open(fake.sim)) == 1
    assert env.eng._pos[1].hold is not None
    await px(env, "ABC", 50.0)  # price back inside: disarmed, broker stop keeps watching
    assert env.eng._pos[1].hold is None and env.eng._pos[1].mp.exit_pending is False
    assert orders(tmp_db, "EXIT") == [] and len(stops_open(fake.sim)) == 1


async def test_bot_exit_after_hold_when_broker_stop_does_not_fill(tmp_db):
    env, fake = alpaca_env(tmp_db)
    sid = new_setup(tmp_db)
    await fake.sim.on_trade(Trade("ABC", utc_iso(T0), 50.0, 1))
    await env.eng.on_entry_signal(sig(sid))
    await px(env, "ABC", 47.9)  # engine sees it; the fake broker's last price stays 50 -> its stop cannot fire
    await _tick(env, 4)
    assert orders(tmp_db, "EXIT") == [] and len(stops_open(fake.sim)) == 1
    await _tick(env, 1.5)  # >= 5 s with price still beyond the stop
    assert [e["event"] for e in events(tmp_db, "WARNING") if e["event"] == "STOP_BOT_EXIT"]
    assert tmp_db.one("SELECT exit_reason FROM trades")["exit_reason"] == "STOP" and fake.sim._pos == {}
    assert stops_open(fake.sim) == []


async def test_broker_stop_fill_during_the_hold_is_recorded_not_resold(tmp_db):
    env, fake = alpaca_env(tmp_db)
    sid = new_setup(tmp_db)
    await fake.sim.on_trade(Trade("ABC", utc_iso(T0), 50.0, 1))
    await env.eng.on_entry_signal(sig(sid))
    await px(env, "ABC", 47.9)
    await fake.sim.on_trade(Trade("ABC", utc_iso(env.clock.now()), 47.8, 1))  # the consolidated print: broker stop fires
    await _tick(env, 1)
    t = tmp_db.one("SELECT * FROM trades")
    assert t["exit_reason"] == "STOP" and t["avg_exit_price"] == 47.8 and orders(tmp_db, "EXIT") == []


# ------------------------------------------------------------------ 6: unknown stop outcome
def _flaky_stop_lookups(fake, n):
    real, calls = fake.get_order_by_client_id, {"n": 0}

    def flaky(cid):
        if "-STOP-" in cid and calls["n"] < n:
            calls["n"] += 1
            raise ConnectionError("down")
        return real(cid)
    fake.get_order_by_client_id = flaky


async def test_unknown_stop_submit_is_adopted_by_client_id_not_flattened(tmp_db):
    """REGRESSION: OrderOutcomeUnknown on the stop flattened, then re-placed a NEW stop -> orphan stop."""
    env, fake = alpaca_env(tmp_db)
    sid = new_setup(tmp_db)
    await fake.sim.on_trade(Trade("ABC", utc_iso(T0), 50.0, 1))
    fake.script = ["ok", "timeout_after"]  # entry fine; stop reaches the broker, the reply is lost
    _flaky_stop_lookups(fake, 4)  # the broker layer's own lookups (4 attempts) fail -> OrderOutcomeUnknown
    await env.eng.on_entry_signal(sig(sid))
    assert orders(tmp_db, "EXIT") == [] and pos_row(tmp_db)["qty_open"] == 28
    assert len(orders(tmp_db, "STOP")) == 1 and len(stops_open(fake.sim)) == 1 and env.eng._pos[1].stop_live
    assert any(e["event"] == "STOP_ADOPTED" for e in events(tmp_db, "WARNING"))


async def test_unknown_stop_that_stays_unknown_is_adopted_then_cancelled_when_flattening(tmp_db):
    env, fake = alpaca_env(tmp_db)
    sid = new_setup(tmp_db)
    await fake.sim.on_trade(Trade("ABC", utc_iso(T0), 50.0, 1))
    fake.script = ["ok", "timeout_after"]
    _flaky_stop_lookups(fake, 8)  # submit's 4 lookups + the engine's first adopt attempt (4) fail
    await env.eng.on_entry_signal(sig(sid))
    assert any(e["event"] == "STOP_FAILED" for e in events(tmp_db, "CRITICAL"))
    assert fake.sim._pos == {} and stops_open(fake.sim) == [] and len(orders(tmp_db, "STOP")) == 1
    assert tmp_db.one("SELECT exit_reason FROM trades")["exit_reason"] == "STATE_CORRUPT"


# ------------------------------------------------------------------ 9: partial entry is protected at once
async def test_partial_entry_fill_is_stopped_immediately_and_resized_as_fills_arrive(tmp_db):
    env = make_env(tmp_db)
    env.sim.partial_cap = 10
    sid = new_setup(tmp_db)
    await px(env, "ABC", 50.0)
    await env.eng.on_entry_signal(sig(sid))
    assert len(env.eng._entries) == 1 and tmp_db.one("SELECT COUNT(*) n FROM positions")["n"] == 0
    assert [o.qty for o in stops_open(env.sim)] == [10]  # entry still working, the 10 shares are stopped
    env.clock.advance(seconds=1)
    await px(env, "ABC", 50.0)  # 10 more fill
    assert [o.qty for o in stops_open(env.sim)] == [20] and len(env.eng._entries) == 1
    env.clock.advance(seconds=10)
    await env.eng.on_clock(env.clock.now())  # entry times out, cancelled, position opens with 20
    p = pos_row(tmp_db)
    assert p["qty_open"] == 20 and [o.qty for o in stops_open(env.sim)] == [20]
    assert all(r["position_id"] == 1 for r in orders(tmp_db, "STOP"))
    assert env.eng._pos[1].stop_live and not events(tmp_db, "ERROR")
    await px(env, "ABC", 47.0)  # the adopted stop protects the position
    assert tmp_db.one("SELECT exit_reason, qty FROM trades") == {"exit_reason": "STOP", "qty": 20}


# ------------------------------------------------------------------ 3: account-level kill switch / loss budget
async def test_kill_switch_survives_a_new_strategy_version_the_same_day(tmp_db):
    """REGRESSION: trading_disabled / the loss budget were keyed by strategy_version."""
    params = replace(P, max_daily_loss_pct=0.5)
    env1 = make_env(tmp_db, params=params, version="v1.0")
    await open_long(env1)
    await px(env1, "ABC", 47.0)
    assert env1.kill.is_disabled(T0.date())
    env2 = make_env(tmp_db, env1.clock, params, env1.broker, version="v1.1")
    sid = new_setup(tmp_db, "JKL", version="v1.1")
    await px(env2, "JKL", 20.0)
    await env2.eng.on_entry_signal(sig(sid, "JKL", trigger=20.0, lo=19.0, hi=20.5))
    assert tmp_db.one("SELECT reject_reason r FROM setups WHERE id=?", (sid,))["r"] == "KILL_SWITCH"
    assert env2.kill.is_disabled(T0.date())


async def test_daily_loss_budget_is_shared_across_versions(tmp_db):
    params = replace(P, max_daily_loss_pct=2.0)  # $120 of 6000
    env1 = make_env(tmp_db, params=params, version="v1.0")
    await open_long(env1)
    await px(env1, "ABC", 47.9)  # -$58.8: under the budget
    assert not env1.kill.is_disabled(T0.date())
    env2 = make_env(tmp_db, env1.clock, params, env1.broker, version="v1.1")
    sid = new_setup(tmp_db, "DEF", version="v1.1")
    await px(env2, "DEF", 50.0)
    await env2.eng.on_entry_signal(sig(sid, "DEF"))
    await px(env2, "DEF", 47.0)  # another -$84 under v1.1: per-version neither trips (-58.8 / -84), together they do
    assert env2.kill.is_disabled(T0.date())


# ------------------------------------------------------------------ 4: early close, one central rule
async def test_early_close_1300_blocks_entries_from_1230_and_flattens_at_1255(tmp_db):
    from datetime import date, datetime, UTC
    d = T0.date()
    close = datetime(2026, 1, 2, 18, 0, tzinfo=UTC)  # 13:00 ET
    set_session_close(d, close)
    env = make_env(tmp_db, params=replace(P, time_stop_enabled=False))
    await open_long(env)  # 10:00 ET
    env.clock.set(datetime(2026, 1, 2, 17, 40, tzinfo=UTC))  # 12:40 ET
    sid = new_setup(tmp_db, "DEF")
    await px(env, "DEF", 30.0)
    await env.eng.on_entry_signal(sig(sid, "DEF", trigger=30.0, lo=29.0, hi=30.5))
    assert tmp_db.one("SELECT reject_reason r FROM setups WHERE id=?", (sid,))["r"] == "AFTER_CUTOFF"
    env.clock.set(datetime(2026, 1, 2, 17, 54, 59, tzinfo=UTC))
    await env.eng.on_clock(env.clock.now())
    assert len(env.eng.open_positions()) == 1
    env.clock.set(datetime(2026, 1, 2, 17, 55, 0, tzinfo=UTC))  # 12:55 ET
    await env.eng.on_clock(env.clock.now())
    assert env.eng.open_positions() == [] and tmp_db.one("SELECT exit_reason FROM trades")["exit_reason"] == "EOD"


# ------------------------------------------------------------------ 8: client ids
async def test_client_ids_carry_a_per_database_uid_unique_across_db_wipes(tmp_path):
    from datetime import datetime, UTC
    from newswave.db import Database

    class Wall:  # any non-replay clock draws a random uid
        def now(self):
            return T0

    uids = []
    for n in (1, 2):
        db = Database(tmp_path / f"w{n}.db")
        db.init_schema()
        env = make_env(db, Wall())
        sid = await open_long(env)
        cid = orders(db, "ENTRY")[0]["client_order_id"]
        assert cid == f"nw-{env.eng.uid}-{sid}-ENTRY-1" and len(env.eng.uid) == 6
        assert make_env(db, Wall()).eng.uid == env.eng.uid  # stable for one database
        uids.append(env.eng.uid)
        db.close()
    assert uids[0] != uids[1]
