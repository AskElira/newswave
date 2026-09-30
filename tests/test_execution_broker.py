from __future__ import annotations

import pytest
from alpaca.trading.enums import OrderSide, OrderType, TimeInForce

from newswave.clock import ReplayClock, utc_iso
from newswave.execution.broker import (AlpacaPaperBroker, BrokerUnavailable, NotFound, OrderOutcomeUnknown,
                                       OrderRejected, SimBroker)
from newswave.models import Side, Trade
from test_execution_helpers import FakeTradingClient, T0, _nosleep


def T(sym, price):
    return Trade(sym, utc_iso(T0), price, 100)


def sim():
    return SimBroker(ReplayClock(T0), 6000.0)


# ------------------------------------------------------------------ SimBroker
async def test_sim_marketable_limit_fills_immediately_at_last_print():
    b = sim()
    await b.on_trade(T("A", 50.0))
    o = await b.submit_limit("A", "buy", 10, 50.10, "c1")  # last 50.0 <= limit -> fills AT THE PRINT
    assert o.status == "filled" and o.filled_qty == 10 and o.filled_avg_price == 50.0
    assert (await b.get_positions())[0].qty == 10
    assert (await b.get_account()).cash == pytest.approx(6000 - 500)


async def test_sim_limit_rests_until_print_through_limit():
    b = sim()
    await b.on_trade(T("A", 51.0))
    o = await b.submit_limit("A", "buy", 10, 50.10, "c1")
    assert o.status == "new"
    await b.on_trade(T("A", 50.5))
    assert (await b.get_order_by_client_id("c1")).status == "new"
    await b.on_trade(T("A", 50.0))
    assert (await b.get_order_by_client_id("c1")).filled_avg_price == 50.0


async def test_sim_short_limit_and_buy_stop():
    b = sim()
    await b.on_trade(T("A", 50.0))
    o = await b.submit_limit("A", "sell", 10, 49.9, "s1")  # sell limit marketable when last >= limit
    assert o.status == "filled" and (await b.get_positions())[0].side == Side.SHORT
    await b.submit_stop("A", "buy", 10, 50.65, "st")
    await b.on_trade(T("A", 50.6))
    assert (await b.get_order_by_client_id("st")).status == "new"
    await b.on_trade(T("A", 51.2))  # gapped through: fills at the print, not the stop
    st = await b.get_order_by_client_id("st")
    assert st.status == "filled" and st.filled_avg_price == 51.2
    assert await b.get_positions() == []


async def test_sim_stop_gap_through_long():
    b = sim()
    await b.on_trade(T("A", 50.0))
    await b.submit_limit("A", "buy", 10, 50.1, "e")
    await b.submit_stop("A", "sell", 10, 48.0, "st")
    await b.on_trade(T("A", 48.5))
    assert (await b.get_order_by_client_id("st")).status == "new"
    await b.on_trade(T("A", 46.0))
    assert (await b.get_order_by_client_id("st")).filled_avg_price == 46.0


async def test_sim_market_waits_for_a_price_when_none_yet():
    b = sim()
    o = await b.submit_market("A", "buy", 5, "m")
    assert o.status == "new"
    await b.on_trade(T("A", 20.0))
    assert (await b.get_order_by_client_id("m")).filled_avg_price == 20.0


async def test_sim_unique_client_id_and_whole_qty():
    b = sim()
    await b.submit_limit("A", "buy", 1, 10, "dup")
    with pytest.raises(OrderRejected) as e:
        await b.submit_limit("A", "buy", 1, 10, "dup")
    assert e.value.status_code == 422
    with pytest.raises(OrderRejected):
        await b.submit_market("A", "buy", 0, "z")
    with pytest.raises(OrderRejected):
        await b.submit_market("A", "buy", 1.5, "z2")


async def test_sim_reserved_shares_refuse_a_second_sell_like_alpaca():
    b = sim()
    await b.on_trade(T("A", 50.0))
    await b.submit_limit("A", "buy", 10, 50.1, "e")
    st = await b.submit_stop("A", "sell", 10, 48.0, "st")
    with pytest.raises(OrderRejected) as e:
        await b.submit_market("A", "sell", 5, "x")
    assert e.value.status_code == 403
    r = await b.replace_order_qty(st.broker_order_id, 5, "st2")  # shrink the stop, then 5 are free
    assert r.qty == 5 and r.broker_order_id != st.broker_order_id
    assert (await b.get_order_by_client_id("st")).status == "replaced"
    assert (await b.submit_market("A", "sell", 5, "x")).status == "filled"


async def test_sim_replace_cancel_semantics():
    b = sim()
    o = await b.submit_stop("A", "sell", 10, 48.0, "st")
    with pytest.raises(OrderRejected):
        await b.replace_order_qty(o.broker_order_id, 10, "st")  # duplicate id
    await b.cancel_order(o.broker_order_id)
    assert (await b.get_order_by_client_id("st")).status == "canceled"
    with pytest.raises(OrderRejected):
        await b.cancel_order(o.broker_order_id)  # not cancelable any more
    with pytest.raises(OrderRejected):
        await b.replace_order_qty(o.broker_order_id, 5, "x")
    with pytest.raises(NotFound):
        await b.cancel_order("nope")
    assert await b.get_open_orders() == []
    assert await b.get_order_by_client_id("unknown") is None


async def test_sim_account_and_assets():
    b = sim()
    await b.on_trade(T("A", 50.0))
    await b.submit_limit("A", "buy", 10, 50.1, "e")
    await b.on_trade(T("A", 52.0))
    a = await b.get_account()
    assert a.equity == pytest.approx(6020) and a.buying_power == pytest.approx(6020 - 520)
    assert (await b.get_asset("A")).shortable is True
    assert await b.latest_price("A") == 52.0 and await b.latest_price("Z") is None


# ------------------------------------------------------------------ AlpacaPaperBroker over a fake client
def mk():
    s = sim()
    fake = FakeTradingClient(s)
    return AlpacaPaperBroker("k", "s", client=fake, sleep=_nosleep), fake, s


async def test_alpaca_request_shape_and_conversion():
    b, fake, s = mk()
    await s.on_trade(T("A", 50.0))
    o = await b.submit_limit("A", "buy", 10, 50.104, "nw-1-ENTRY-1")
    req = fake.requests[-1]
    assert req.type == OrderType.LIMIT and req.side == OrderSide.BUY and req.time_in_force == TimeInForce.DAY
    assert req.extended_hours is False and req.limit_price == 50.10 and req.client_order_id == "nw-1-ENTRY-1"
    assert o.status == "filled" and o.filled_qty == 10 and o.filled_avg_price == 50.0
    assert o.order_type == "limit" and o.side == "buy" and o.symbol == "A" and o.submitted_at.endswith("Z")
    assert isinstance(o.raw, dict) and o.raw["client_order_id"] == "nw-1-ENTRY-1"
    st = await b.submit_stop("A", "sell", 10, 48.0, "nw-1-STOP-1")
    assert fake.requests[-1].type == OrderType.STOP and st.stop_price == 48.0
    st2 = await b.replace_order_qty(st.broker_order_id, 4, "nw-1-STOP-2")
    assert fake.requests[-1].qty == 4 and fake.requests[-1].client_order_id == "nw-1-STOP-2" and st2.qty == 4
    await b.submit_market("A", "sell", 6, "nw-1-EXIT-1")
    assert fake.requests[-1].type == OrderType.MARKET
    pos = await b.get_positions()
    assert pos[0].qty == 4 and pos[0].side == Side.LONG
    assert [o.client_order_id for o in await b.get_open_orders()] == ["nw-1-STOP-2"]
    assert (await b.get_order_by_client_id("nope")) is None
    await b.cancel_order(st2.broker_order_id)
    acct = await b.get_account()
    assert acct.equity > 0 and acct.buying_power > 0 and acct.daytrade_count == 0 and acct.pattern_day_trader is False
    a = await b.get_asset("A")
    assert a.tradable and a.shortable and a.easy_to_borrow and a.exchange == "NASDAQ" and a.asset_class == "us_equity"
    assert a.status == "active"
    assert await b.get_all_assets() == []
    assert await b.latest_price("A") is None
    assert await b.close_position("A") is not None and await b.close_position("A") is None


async def test_alpaca_short_position_sign():
    b, fake, s = mk()
    await s.on_trade(T("A", 50.0))
    await b.submit_limit("A", "sell", 7, 49.0, "x")
    p = (await b.get_positions())[0]
    assert p.side == Side.SHORT and p.qty == 7


async def test_alpaca_whole_shares_only():
    b, fake, s = mk()
    with pytest.raises(ValueError):
        await b.submit_market("A", "buy", 1.5, "x")
    assert fake.submit_calls == 0


async def test_submit_timeout_that_actually_succeeded_is_not_resubmitted():
    b, fake, s = mk()
    await s.on_trade(T("A", 50.0))
    fake.script = ["timeout_after"]
    o = await b.submit_limit("A", "buy", 10, 50.1, "nw-9-ENTRY-1")
    assert fake.submit_calls == 1  # found by client_order_id, NEVER sent twice
    assert o.client_order_id == "nw-9-ENTRY-1" and o.filled_qty == 10
    assert len([x for x in s._orders.values()]) == 1


async def test_submit_timeout_before_accept_is_retried_once_lookup_says_absent():
    b, fake, s = mk()
    fake.script = ["timeout_before"]
    o = await b.submit_limit("A", "buy", 10, 50.1, "nw-9-ENTRY-1")
    assert fake.submit_calls == 2 and o.status == "new" and len(s._orders) == 1


async def test_submit_5xx_and_429_look_up_then_retry():
    b, fake, s = mk()
    fake.script = [("api", 503), ("api", 429)]
    o = await b.submit_market("A", "buy", 3, "c")
    assert fake.submit_calls == 3 and o.client_order_id == "c"


async def test_submit_unknown_outcome_when_lookup_is_also_down_never_resends():
    b, fake, s = mk()
    fake.script = ["timeout_after"]
    fake.lookup_fail = 99
    with pytest.raises(OrderOutcomeUnknown):
        await b.submit_limit("A", "buy", 10, 50.1, "nw-9-ENTRY-1")
    assert fake.submit_calls == 1 and len(s._orders) == 1


async def test_submit_definite_reject_and_exhausted_transient():
    b, fake, s = mk()
    fake.script = [("api", 403)]
    with pytest.raises(OrderRejected) as e:
        await b.submit_market("A", "buy", 3, "c")
    assert e.value.status_code == 403 and fake.submit_calls == 1  # no retry on a definite 4xx
    fake.script = ["timeout_before"] * 10
    with pytest.raises(BrokerUnavailable):
        await b.submit_market("A", "buy", 3, "c2")
    assert len(s._orders) == 0


async def test_duplicate_client_id_422_returns_the_existing_order():
    b, fake, s = mk()
    await b.submit_limit("A", "buy", 10, 50.1, "c")
    o = await b.submit_limit("A", "buy", 10, 50.1, "c")  # broker says duplicate -> we return the original
    assert o.client_order_id == "c" and len(s._orders) == 1


async def test_reads_retry_transient_then_give_up():
    b, fake, s = mk()
    fake.lookup_fail = 2
    assert await b.get_order_by_client_id("nope") is None  # 2 connection errors, then a clean 404
    fake.lookup_fail = 99
    with pytest.raises(BrokerUnavailable):
        await b.get_order_by_client_id("nope")


async def test_duplicate_client_id_of_a_different_order_is_refused_not_adopted():
    """REGRESSION (review 8): a 422 duplicate whose lookup returns another symbol/side/qty was returned as ours."""
    b, fake, s = mk()
    await s.submit_limit("ZZZ", "sell", 5, 10.0, "nw-1-ENTRY-1")  # someone else's order holds the id
    fake.script = [("api", 422, "client_order_id must be unique")]
    with pytest.raises(OrderRejected, match="different order"):
        await b.submit_limit("A", "buy", 10, 50.1, "nw-1-ENTRY-1")
    assert len(s._orders) == 1
    # same symbol/side/qty: it IS our order (a retry after a lost reply) and is returned
    await s.submit_limit("A", "buy", 10, 50.1, "mine")
    fake.script = [("api", 422, "client_order_id must be unique")]
    assert (await b.submit_limit("A", "buy", 10, 50.1, "mine")).client_order_id == "mine"


async def test_sim_cancel_and_replace_are_asynchronous_when_lagged():
    s = sim()
    s.settle_lag = 2
    await s.on_trade(T("A", 50.0))
    await s.submit_limit("A", "buy", 10, 50.1, "e")
    st = await s.submit_stop("A", "sell", 10, 48.0, "s1")
    await s.cancel_order(st.broker_order_id)
    assert (await s.get_order_by_client_id("s1")).status == "pending_cancel"  # shares still held
    with pytest.raises(OrderRejected, match="insufficient qty"):
        await s.submit_market("A", "sell", 10, "x")
    assert (await s.get_order_by_client_id("s1")).status == "canceled"
    await s.submit_market("A", "sell", 10, "x2")
