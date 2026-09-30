"""Crash recovery: a 'restart' is a fresh ExecutionEngine over the same db (and the same broker state)."""
from __future__ import annotations

from dataclasses import replace
from datetime import timedelta

import pytest

from newswave.clock import utc_iso
from newswave.execution.broker import SimBroker
from newswave.models import OrderPurpose, Trade
from test_execution_helpers import (EOD, P, T0, bar, events, make_env, msgs, new_env_on, new_setup, orders, px,
                                    sig, stops_open)


async def open_long(env, symbol="ABC", price=50.0):
    sid = new_setup(env.db, symbol)
    await px(env, symbol, price)
    await env.eng.on_entry_signal(sig(sid, symbol))
    return sid


def ev(db, name):
    return [e for e in events(db) if e["event"] == name]


# ------------------------------------------------------------------ (a) rehydrate + missing stop re-placed
async def test_rehydrate_partial_position_and_replace_missing_stop(tmp_db):
    env1 = make_env(tmp_db)
    sid = await open_long(env1)
    await px(env1, "ABC", 52.0)  # partial
    await env1.eng.on_bar_5m("ABC", bar("ABC", 53.0, high=54.0), {"atr": 1.0, "ema9": 52.0})  # trail 52, persisted
    # crash: the broker lost our stop
    await env1.sim.cancel_order(stops_open(env1.sim)[0].broker_order_id)
    assert stops_open(env1.sim) == []

    env2 = new_env_on(env1)
    await env2.eng.reconcile_on_boot()
    (c,) = env2.eng._pos.values()
    m = c.mp
    assert (m.qty_open, m.qty_initial, m.entry_price, m.stop_price, m.risk_per_share) == (14, 28, 50.0, 48.0, 2.0)
    assert m.partial_taken and m.trail_price == 52.0 and m.highest_since_entry == 54.0 and not m.exit_pending
    (st,) = stops_open(env2.sim)
    assert st.qty == 14 and st.stop_price == 48.0 and st.client_order_id == f"nw-{env2.eng.uid}-{sid}-STOP-3"
    assert ev(tmp_db, "RECONCILE_STOP") and "recovered LONG 14 ABC @ 50.00" in msgs(tmp_db)
    assert env2.subs.priority("ABC").name == "POSITION" and env2.subs.owners("ABC") == {"pos-1"}
    assert [p["symbol"] for p in env2.eng.open_positions()] == ["ABC"]
    # and it keeps managing: the trail (52) fires on a close below it
    await px(env2, "ABC", 51.0)
    await env2.eng.on_bar_5m("ABC", bar("ABC", 51.0, high=52.0, low=50.9), {"atr": 1.0, "ema9": 52.0})
    t = tmp_db.one("SELECT * FROM trades")
    assert t["exit_reason"] == "TRAIL" and t["pnl"] == pytest.approx(14 * 2.0 + 14 * 1.0)
    assert stops_open(env2.sim) == []
    # the ledger survives the restart
    assert env2.eng.account_snapshot()["ledger_equity"] == pytest.approx(6042.0)


async def test_rehydrate_with_healthy_stop_changes_nothing(tmp_db):
    env1 = make_env(tmp_db)
    sid = await open_long(env1)
    n_orders = len(orders(tmp_db))
    env2 = new_env_on(env1)
    await env2.eng.reconcile_on_boot()
    assert len(orders(tmp_db)) == n_orders and len(env2.eng._pos) == 1
    assert not ev(tmp_db, "RECONCILE_STOP") and ev(tmp_db, "RECONCILE_REHYDRATED")
    assert env2.eng._pos[1].stop_cid == f"nw-{env2.eng.uid}-{sid}-STOP-1" and env2.eng._pos[1].stop_live
    await env2.eng.reconcile_on_boot()  # idempotent
    assert len(orders(tmp_db)) == n_orders


async def test_wrong_size_nw_stop_is_resized(tmp_db):
    env1 = make_env(tmp_db)
    sid = await open_long(env1)
    st = stops_open(env1.sim)[0]
    await env1.sim.replace_order_qty(st.broker_order_id, 9, f"nw-{sid}-STOP-2")  # crash mid-resize
    env2 = new_env_on(env1)
    await env2.eng.reconcile_on_boot()
    (s,) = stops_open(env2.sim)
    assert s.qty == 28 and s.client_order_id == f"nw-{env2.eng.uid}-{sid}-STOP-3"
    assert "wrong qty" in ev(tmp_db, "RECONCILE_STOP")[0]["message"]


async def test_unowned_stop_blocks_replacement_loudly(tmp_db):
    """A stop with a foreign id reserves the shares; we cannot add ours -> CRITICAL, never a silent gap."""
    env1 = make_env(tmp_db)
    await open_long(env1)
    st = stops_open(env1.sim)[0]
    await env1.sim.replace_order_qty(st.broker_order_id, 28, "manual-stop")
    env2 = new_env_on(env1)
    await env2.eng.reconcile_on_boot()
    assert ev(tmp_db, "STOP_FAILED")[0]["level"] == "CRITICAL" and len(env2.eng._pos) == 1


# ------------------------------------------------------------------ (b) broker flat
async def test_broker_flat_stop_filled_is_recorded_as_STOP(tmp_db):
    env1 = make_env(tmp_db)
    await open_long(env1)
    await env1.sim.on_trade(Trade("ABC", utc_iso(T0), 47.0, 1))  # stop fires while the bot is down
    assert env1.sim._pos == {}
    env2 = new_env_on(env1)
    await env2.eng.reconcile_on_boot()
    t = tmp_db.one("SELECT * FROM trades")
    assert t["exit_reason"] == "STOP" and t["avg_exit_price"] == 47.0 and t["pnl"] == pytest.approx(28 * -3.0)
    assert tmp_db.one("SELECT status, qty_open FROM positions WHERE id=1") == {"status": "CLOSED", "qty_open": 0}
    assert tmp_db.one("SELECT stage FROM setups WHERE id=1")["stage"] == "CLOSED"
    assert env2.eng._pos == {} and env2.subs.priority("ABC") is None
    assert [o["purpose"] for o in orders(tmp_db)] == ["ENTRY", "STOP"]  # nothing invented
    assert ev(tmp_db, "RECONCILE_CLOSED")
    assert env2.eng.account_snapshot()["total_pnl"] == pytest.approx(-84.0)


async def test_broker_flat_no_trace_is_STATE_CORRUPT_at_last_known_price(tmp_db):
    env1 = make_env(tmp_db)
    await open_long(env1)
    wiped = SimBroker(env1.clock, 6000.0)  # broker has no record of anything
    env2 = new_env_on(env1, broker=wiped)
    await px(env2, "ABC", 49.0)  # a print arrives before boot completes? (last known price)
    await env2.eng.reconcile_on_boot()
    t = tmp_db.one("SELECT * FROM trades")
    assert t["exit_reason"] == "STATE_CORRUPT" and t["avg_exit_price"] == 49.0
    assert t["pnl"] == pytest.approx(28 * -1.0)
    assert ev(tmp_db, "STATE_CORRUPT") and env2.eng._pos == {}
    exit_row = orders(tmp_db, "EXIT")[0]
    assert exit_row["status"] == "reconciled" and exit_row["filled_qty"] == 28


async def test_broker_flat_defaults_to_entry_price_without_any_print(tmp_db):
    env1 = make_env(tmp_db)
    await open_long(env1)
    env2 = new_env_on(env1, broker=SimBroker(env1.clock, 6000.0))
    await env2.eng.reconcile_on_boot()
    t = tmp_db.one("SELECT * FROM trades")
    assert t["exit_reason"] == "STATE_CORRUPT" and t["avg_exit_price"] == 50.0 and t["pnl"] == 0


async def test_broker_holds_fewer_shares_than_ledger(tmp_db):
    env1 = make_env(tmp_db)
    await open_long(env1)
    await env1.sim.cancel_order(stops_open(env1.sim)[0].broker_order_id)
    await env1.sim.submit_market("ABC", "sell", 5, "manual-sell")  # someone sold 5 by hand
    env2 = new_env_on(env1)
    await env2.eng.reconcile_on_boot()
    (c,) = env2.eng._pos.values()
    assert c.mp.qty_open == 23 and stops_open(env2.sim)[0].qty == 23
    leg = orders(tmp_db, "EXIT")[0]
    assert leg["status"] == "reconciled" and leg["filled_qty"] == 5
    assert ev(tmp_db, "STATE_CORRUPT")


# ------------------------------------------------------------------ (c) unknown nw- orders
async def test_unknown_nw_order_is_cancelled_and_foreign_order_left_alone(tmp_db):
    env = make_env(tmp_db)
    await env.sim.on_trade(Trade("ZZZ", utc_iso(T0), 60.0, 1))
    await env.sim.submit_limit("ZZZ", "buy", 5, 10.0, "nw-999-ENTRY-1")  # rests
    await env.sim.submit_limit("ZZZ", "buy", 5, 10.0, "my-own-order")
    await env.eng.reconcile_on_boot()
    assert (await env.sim.get_order_by_client_id("nw-999-ENTRY-1")).status == "canceled"
    assert (await env.sim.get_order_by_client_id("my-own-order")).status == "new"
    e = ev(tmp_db, "RECONCILE_ORDER_CANCELLED")
    assert len(e) == 1 and "nw-999-ENTRY-1" in e[0]["message"] and "unknown to DB" in e[0]["message"]


async def test_pending_entry_does_not_survive_a_restart(tmp_db):
    env1 = make_env(tmp_db)
    sid = new_setup(tmp_db)
    await env1.eng.on_entry_signal(sig(sid))  # resting entry, then crash
    env2 = new_env_on(env1)
    await env2.eng.reconcile_on_boot()
    assert await env2.sim.get_open_orders() == []
    assert orders(tmp_db, "ENTRY")[0]["status"] == "canceled"
    s = tmp_db.one("SELECT * FROM setups WHERE id=?", (sid,))
    assert s["stage"] == "REJECTED" and s["reject_reason"] == "ENTRY_NOT_FILLED"


# ------------------------------------------------------------------ (d) foreign positions
async def test_foreign_position_untouched_and_excluded_from_ledger(tmp_db):
    env = make_env(tmp_db)
    await env.sim.on_trade(Trade("ZZZ", utc_iso(T0), 10.0, 1))
    await env.sim.submit_market("ZZZ", "buy", 100, "manual-1")
    await env.sim.submit_stop("ZZZ", "sell", 100, 9.0, "manual-stop")
    await env.eng.reconcile_on_boot()
    assert env.sim._pos["ZZZ"][0] == 100
    assert (await env.sim.get_order_by_client_id("manual-stop")).status == "new"
    w = ev(tmp_db, "FOREIGN_POSITION")
    assert len(w) == 1 and w[0]["level"] == "WARNING" and "ZZZ" in w[0]["message"]
    assert env.eng.open_positions() == [] and tmp_db.one("SELECT COUNT(*) n FROM positions")["n"] == 0
    await env.sim.on_trade(Trade("ZZZ", utc_iso(T0), 5.0, 1))  # foreign loss does not touch the ledger
    snap = env.eng.account_snapshot()
    assert snap["ledger_equity"] == 6000 and snap["total_pnl"] == 0 and snap["daily_pnl"] == 0


async def test_foreign_position_next_to_our_own_symbol_rows(tmp_db):
    """A manual holding in a symbol we once traded (closed) is still foreign."""
    env1 = make_env(tmp_db)
    await open_long(env1)
    await px(env1, "ABC", 47.0)  # stop -> closed trade, ENTRY row filled with a position
    await env1.sim.submit_market("ABC", "buy", 3, "manual-abc")
    env2 = new_env_on(env1)
    await env2.eng.reconcile_on_boot()
    assert env1.sim._pos["ABC"][0] == 3 and ev(tmp_db, "FOREIGN_POSITION")


# ------------------------------------------------------------------ orphan nw- positions
@pytest.mark.parametrize("with_stop", [True, False])
async def test_orphan_nw_position_is_flattened(tmp_db, with_stop):
    env = make_env(tmp_db)
    sid = new_setup(tmp_db)
    await env.sim.on_trade(Trade("ABC", utc_iso(T0), 50.0, 1))
    cid = f"nw-{sid}-ENTRY-1"
    # crash right after the broker filled the entry: DB has the order row (written before submit), no position
    env.eng._insert_order(cid, sid, None, "ABC", "buy", "limit", 20, 50.1, None,
                          OrderPurpose.ENTRY)
    await env.sim.submit_limit("ABC", "buy", 20, 50.1, cid)
    if with_stop:
        await env.sim.submit_stop("ABC", "sell", 20, 48.0, f"nw-{sid}-STOP-1")  # unknown to the DB
    await env.eng.reconcile_on_boot()
    assert env.sim._pos == {} and await env.sim.get_open_orders() == []
    c = ev(tmp_db, "STATE_CORRUPT")
    assert c and c[0]["level"] == "CRITICAL" and "orphan" in c[0]["message"]
    assert tmp_db.one("SELECT COUNT(*) n FROM trades")["n"] == 0
    assert env.eng._pos == {}


# ------------------------------------------------------------------ boot timing / session state
async def test_boot_after_eod_flattens_open_positions(tmp_db):
    env1 = make_env(tmp_db, params=replace(P, time_stop_enabled=False))
    await open_long(env1)
    env1.clock.set(EOD + timedelta(minutes=2))
    env2 = new_env_on(env1)
    await env2.eng.reconcile_on_boot()
    assert tmp_db.one("SELECT exit_reason FROM trades")["exit_reason"] == "EOD"
    assert env2.sim._pos == {} and stops_open(env2.sim) == []


async def test_boot_with_kill_switch_tripped_flattens_risk_kill(tmp_db):
    params = replace(P, max_daily_loss_pct=0.5, time_stop_enabled=False)
    env1 = make_env(tmp_db, params=params)
    await open_long(env1)
    env1.kill.trip(T0.date(), T0, "test")  # tripped, yet a position is somehow still open at boot
    env2 = new_env_on(env1)
    await env2.eng.reconcile_on_boot()
    assert tmp_db.one("SELECT exit_reason FROM trades")["exit_reason"] == "RISK_KILL"


async def test_reconcile_failure_to_read_broker_is_loud(tmp_db):
    env = make_env(tmp_db)
    from newswave.execution.broker import BrokerUnavailable

    async def down():
        raise BrokerUnavailable("down")
    env.sim.get_positions = down
    with pytest.raises(BrokerUnavailable):
        await env.eng.reconcile_on_boot()
    assert ev(tmp_db, "RECONCILE_FAILED")[0]["level"] == "CRITICAL"


# ------------------------------------------------------------------ review 7: async cancels + tracked flatten
@pytest.fixture
def _fast_settle(monkeypatch):
    monkeypatch.setattr("newswave.execution.engine.RECONCILE_POLL_S", 0)


async def test_orphan_is_closed_by_a_tracked_order_after_its_stop_cancel_settles(tmp_db, _fast_settle):
    """REGRESSION: the orphan was closed with the untracked close_position right after cancelling, before the
    async cancels (pending_cancel, shares held) reached a terminal state."""
    env = make_env(tmp_db)
    env.sim.settle_lag = 2
    sid = new_setup(tmp_db)
    await env.sim.on_trade(Trade("ABC", utc_iso(T0), 50.0, 1))
    cid = f"nw-{env.eng.uid}-{sid}-ENTRY-1"
    env.eng._insert_order(cid, sid, None, "ABC", "buy", "limit", 20, 50.1, None, OrderPurpose.ENTRY)
    await env.sim.submit_limit("ABC", "buy", 20, 50.1, cid)
    await env.sim.submit_stop("ABC", "sell", 20, 48.0, f"nw-{env.eng.uid}-{sid}-STOP-1")  # unknown to the DB
    await env.eng.reconcile_on_boot()
    assert env.sim._pos == {} and await env.sim.get_open_orders() == []
    (x,) = [o for o in orders(tmp_db, "EXIT")]
    assert x["client_order_id"].startswith(f"nw-{env.eng.uid}-{sid}-EXIT-") and x["status"] == "filled"
    assert x["filled_qty"] == 20 and x["position_id"] is None
    assert (await env.sim.get_order_by_client_id(f"nw-{env.eng.uid}-{sid}-STOP-1")).status == "canceled"
    assert ev(tmp_db, "STATE_CORRUPT")[0]["level"] == "CRITICAL"


async def test_entry_that_fills_while_its_cancel_is_pending_becomes_an_orphan_and_is_flattened(tmp_db, _fast_settle):
    env = make_env(tmp_db)
    env.sim.settle_lag = 2
    await env.sim.on_trade(Trade("ZZZ", utc_iso(T0), 60.0, 1))
    await env.sim.submit_limit("ZZZ", "buy", 5, 10.0, "nw-999-ENTRY-1")  # rests, unknown to the DB
    real, fired = env.sim.get_order_by_client_id, []

    async def lookup(cid):
        if not fired:  # the market dips through the limit while the cancel is still pending_cancel
            fired.append(1)
            await env.sim.on_trade(Trade("ZZZ", utc_iso(T0), 9.0, 1))
        return await real(cid)
    env.sim.get_order_by_client_id = lookup
    await env.eng.reconcile_on_boot()
    assert env.sim._pos == {}  # re-read after the cancels saw the new position and closed it
    (x,) = orders(tmp_db, "EXIT")
    assert x["client_order_id"].startswith(f"nw-{env.eng.uid}-999-EXIT-") and x["symbol"] == "ZZZ"
    assert not ev(tmp_db, "FOREIGN_POSITION")
