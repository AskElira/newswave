from __future__ import annotations

from dataclasses import replace
from datetime import UTC, date, datetime

import pytest

from newswave.config import StrategyParams
from newswave.models import EntrySignal, RejectReason, RiskApproval, Side
from newswave.risk import (AccountState, KillSwitch, RiskEngine, compute_stop, count_day_trades,
                           daily_loss_breached, size_position)

P = StrategyParams()
NOON = datetime(2026, 3, 4, 17, 0, tzinfo=UTC)  # 12:00 ET (EST)


def sig(side=Side.LONG, trigger=50.0, lo=49.5, hi=50.5, atr=1.0, variant="production"):
    return EntrySignal(1, "ABC", side, trigger, lo, hi, atr, "2026-03-04T17:00:00.000Z", variant)


def acct(**kw):
    base = dict(ledger_equity=6000, broker_equity=6000, buying_power=12000, open_risk_dollars=0,
                open_positions=0, day_trades_5_sessions=0, kill_switch_active=False, now=NOON)
    base.update(kw)
    return AccountState(**base)


def test_compute_stop_long_short():
    assert compute_stop(Side.LONG, 50, 49.5, 50.5, 1, 0.75) == pytest.approx(49.25)  # atr floor wins
    assert compute_stop(Side.LONG, 50, 48.0, 50.5, 1, 0.75) == 48.0  # pullback low wins
    assert compute_stop(Side.SHORT, 50, 49.5, 50.5, 1, 0.75) == pytest.approx(50.75)
    assert compute_stop(Side.SHORT, 50, 49.5, 52.0, 1, 0.75) == 52.0


@pytest.mark.parametrize("entry,atr", [(50, 0), (50, -1), (0, 1), (-5, 1)])
def test_compute_stop_raises(entry, atr):
    with pytest.raises(ValueError):
        compute_stop(Side.LONG, entry, 1, 2, atr, 0.75)


def test_size_position_caps():
    # risk: 60/2 = 30; size cap 2100/50 = 42; bp 100000/50
    assert size_position(6000, 50, 48, 100000, P) == 30
    # risk would be 80 shares, size cap 42 wins
    assert size_position(6000, 50, 49.25, 100000, P) == 42
    # buying power wins
    assert size_position(6000, 50, 49.25, 500, P) == 10
    assert size_position(6000, 50, 49.25, 49, P) == 0
    assert size_position(6000, 50, 50, 1000, P) == 0  # zero risk/share
    assert size_position(0, 50, 48, 1000, P) == 0
    # float guard: 60 / 0.6 must be 100 not 99
    assert size_position(6000, 10, 9.4, 1e9, replace(P, max_position_pct=1000)) == 100


def test_short_size_uses_abs_risk():
    assert size_position(6000, 50, 52, 100000, P) == 30


def test_approval_long():
    a = RiskEngine(P).evaluate(sig(), acct())
    assert isinstance(a, RiskApproval)
    assert a.limit_price == pytest.approx(50.1) and a.entry_price == 50.0
    assert a.stop_price == pytest.approx(49.35)  # 50.1 - .75
    assert a.risk_per_share == pytest.approx(0.75)
    # risk 80 shares, size cap floor(2100/50.1)=41
    assert a.qty == 41 and a.risk_dollars == pytest.approx(41 * 0.75)
    assert a.signal.setup_id == 1


def test_approval_short():
    a = RiskEngine(P).evaluate(sig(Side.SHORT), acct())
    assert a.limit_price == pytest.approx(49.9)
    assert a.stop_price == pytest.approx(50.65)  # max(50.5, 49.9+.75)
    assert a.qty == 42  # floor(2100/49.9)


def test_equity_is_min_of_ledger_and_broker():
    a = RiskEngine(P).evaluate(sig(lo=48.0), acct(ledger_equity=6000, broker_equity=3000))
    # equity 3000: risk 30/2.1 = 14
    assert a.qty == 14
    b = RiskEngine(P).evaluate(sig(lo=48.0), acct(ledger_equity=3000, broker_equity=6000))
    assert b.qty == 14


def test_shadow_raises():
    with pytest.raises(ValueError):
        RiskEngine(P).evaluate(sig(variant="shadow_x"), acct())


def test_kill_switch_first():
    # also after cutoff and max positions: kill switch still wins
    r = RiskEngine(P).evaluate(sig(), acct(kill_switch_active=True, open_positions=9,
                                           now=datetime(2026, 3, 4, 20, 45, tzinfo=UTC)))
    assert r == RejectReason.KILL_SWITCH


def test_cutoff_boundary():
    eng = RiskEngine(P)
    ok = datetime(2026, 3, 4, 20, 29, 59, tzinfo=UTC)  # 15:29:59 ET
    at = datetime(2026, 3, 4, 20, 30, tzinfo=UTC)  # 15:30 ET
    assert isinstance(eng.evaluate(sig(), acct(now=ok)), RiskApproval)
    assert eng.evaluate(sig(), acct(now=at)) == RejectReason.AFTER_CUTOFF
    # summer: 15:30 ET = 19:30 UTC
    assert eng.evaluate(sig(), acct(now=datetime(2026, 7, 1, 19, 30, tzinfo=UTC))) == RejectReason.AFTER_CUTOFF
    assert isinstance(eng.evaluate(sig(), acct(now=datetime(2026, 7, 1, 19, 29, tzinfo=UTC))), RiskApproval)


def test_after_cutoff_beats_max_positions():
    r = RiskEngine(P).evaluate(sig(), acct(open_positions=3, now=datetime(2026, 3, 4, 21, 0, tzinfo=UTC)))
    assert r == RejectReason.AFTER_CUTOFF


def test_max_positions():
    eng = RiskEngine(P)
    assert isinstance(eng.evaluate(sig(), acct(open_positions=2)), RiskApproval)
    assert eng.evaluate(sig(), acct(open_positions=3)) == RejectReason.MAX_POSITIONS


def test_pdt():
    eng = RiskEngine(P)
    assert isinstance(eng.evaluate(sig(), acct(day_trades_5_sessions=2)), RiskApproval)
    assert eng.evaluate(sig(), acct(day_trades_5_sessions=3)) == RejectReason.PDT_LIMIT
    # PDT above threshold or mode off never blocks
    assert isinstance(eng.evaluate(sig(), acct(day_trades_5_sessions=9, broker_equity=25000)), RiskApproval)
    assert isinstance(RiskEngine(replace(P, pdt_mode="off")).evaluate(sig(), acct(day_trades_5_sessions=9)),
                      RiskApproval)


def test_pdt_before_size_zero():
    r = RiskEngine(P).evaluate(sig(), acct(day_trades_5_sessions=3, ledger_equity=1))
    assert r == RejectReason.PDT_LIMIT


def test_size_zero():
    # equity 100: risk $1 / 0.75 = 1 share ok; equity 50 -> 0.5 -> 0
    assert RiskEngine(P).evaluate(sig(), acct(ledger_equity=50, broker_equity=50)) == RejectReason.SIZE_ZERO


def test_buying_power():
    eng = RiskEngine(P)
    assert eng.evaluate(sig(), acct(buying_power=50.0)) == RejectReason.BUYING_POWER  # 1 share = 50.1
    a = eng.evaluate(sig(), acct(buying_power=200.0))  # 3 shares
    assert a.qty == 3


def test_concurrent_risk_reduces_then_rejects():
    eng = RiskEngine(P)
    # budget 180; open 170 -> room 10 -> 10/.75 = 13 shares
    a = eng.evaluate(sig(), acct(open_risk_dollars=170))
    assert a.qty == 13 and a.risk_dollars <= 10
    # room 0.5 -> 0 shares -> reject
    assert eng.evaluate(sig(), acct(open_risk_dollars=179.5)) == RejectReason.MAX_CONCURRENT_RISK
    assert eng.evaluate(sig(), acct(open_risk_dollars=500)) == RejectReason.MAX_CONCURRENT_RISK
    # exactly fitting is allowed: room = 41 * .75 = 30.75
    assert eng.evaluate(sig(), acct(open_risk_dollars=180 - 30.75)).qty == 41


def test_approval_cannot_be_forged():
    a = RiskEngine(P).evaluate(sig(), acct())
    with pytest.raises(PermissionError):
        RiskApproval(signal=a.signal, qty=1, entry_price=1, limit_price=1, stop_price=1,
                     risk_per_share=1, risk_dollars=1)


def test_daily_loss():
    assert not daily_loss_breached(6000, -599.99, 0, 10)
    assert daily_loss_breached(6000, -600, 0, 10)  # exactly at threshold
    assert daily_loss_breached(6000, -400, -250, 10)  # realized + unrealized
    assert not daily_loss_breached(6000, 100, -200, 10)
    assert not daily_loss_breached(6000, 500, 0, 10)


def test_kill_switch_persists(tmp_db):
    d = date(2026, 3, 4)
    ks = KillSwitch(tmp_db, "v1")
    assert not ks.is_disabled(d)
    tmp_db.upsert("daily_stats", {"session_date": d.isoformat(), "strategy_version": "v1",
                                  "start_equity": 6000.0}, ["session_date", "strategy_version"])
    ks.trip(d, NOON, "loss")
    assert KillSwitch(tmp_db, "v1").is_disabled(d)
    assert KillSwitch(tmp_db, "v2").is_disabled(d)  # account-level: any version trips the whole session
    assert not ks.is_disabled(date(2026, 3, 5))
    row = tmp_db.one("SELECT * FROM daily_stats WHERE session_date=?", (d.isoformat(),))
    assert row["trading_disabled"] == 1 and row["kill_switch_at"].endswith("Z") and row["start_equity"] == 6000.0
    first = row["kill_switch_at"]
    ks.trip(d, datetime(2026, 3, 4, 18, 0, tzinfo=UTC))  # idempotent, keeps first time
    assert tmp_db.one("SELECT kill_switch_at FROM daily_stats")["kill_switch_at"] == first
    assert not hasattr(ks, "reset") and not hasattr(ks, "clear")


def _trade(db, entry, exit_, shadow=0, version="v1"):
    db.insert("trades", {"symbol": "A", "is_shadow": shadow, "entry_at": entry, "exit_at": exit_,
                         "strategy_version": version})


def test_count_day_trades(tmp_db):
    s = [date(2026, 3, 2), date(2026, 3, 3), date(2026, 3, 4)]
    _trade(tmp_db, "2026-03-02T15:00:00.000Z", "2026-03-02T16:00:00.000Z")  # day trade
    _trade(tmp_db, "2026-03-03T15:00:00.000Z", "2026-03-04T15:00:00.000Z")  # overnight: no
    _trade(tmp_db, "2026-03-04T15:00:00.000Z", "2026-03-04T16:00:00.000Z", shadow=1)  # shadow: no
    _trade(tmp_db, "2026-03-04T15:00:00.000Z", "2026-03-04T16:00:00.000Z", version="v2")  # other version
    _trade(tmp_db, "2026-02-27T15:00:00.000Z", "2026-02-27T16:00:00.000Z")  # outside sessions
    _trade(tmp_db, "2026-03-04T15:00:00.000Z", "2026-03-04T15:30:00.000Z")  # day trade
    # ET-date boundary: 23:30Z entry and 04:30Z+1 exit are 18:30 and 23:30 ET same day (EST)
    _trade(tmp_db, "2026-03-03T23:30:00.000Z", "2026-03-04T04:30:00.000Z")
    assert count_day_trades(tmp_db, "v1", s) == 3
    assert count_day_trades(tmp_db, "v1", [date(2026, 3, 4)]) == 1
    assert count_day_trades(tmp_db, "v1", []) == 0


def test_pdt_counts_open_and_pending_positions_as_future_day_trades():
    """REGRESSION (review 1): only CLOSED day trades counted, so 2 done + 1 open let a 4th entry through."""
    eng = RiskEngine(P)
    assert eng.evaluate(sig(), acct(day_trades_5_sessions=2, open_positions=1)) == RejectReason.PDT_LIMIT
    assert eng.evaluate(sig(), acct(day_trades_5_sessions=0, open_positions=3)) == RejectReason.MAX_POSITIONS
    assert eng.evaluate(sig(), acct(day_trades_5_sessions=1, open_positions=2)) == RejectReason.PDT_LIMIT
    assert isinstance(eng.evaluate(sig(), acct(day_trades_5_sessions=2, open_positions=0)), RiskApproval)
    assert isinstance(eng.evaluate(sig(), acct(day_trades_5_sessions=2, open_positions=1, broker_equity=25000)),
                      RiskApproval)


def test_early_close_moves_the_entry_cutoff_to_close_minus_30():
    from newswave.clock import clear_session_closes, set_session_close
    clear_session_closes()
    try:
        set_session_close(date(2026, 3, 4), datetime(2026, 3, 4, 18, 0, tzinfo=UTC))  # 13:00 EST close
        eng = RiskEngine(P)
        assert eng.evaluate(sig(), acct(now=datetime(2026, 3, 4, 17, 29, tzinfo=UTC))).__class__ is RiskApproval
        assert eng.evaluate(sig(), acct(now=datetime(2026, 3, 4, 17, 30, tzinfo=UTC))) == RejectReason.AFTER_CUTOFF
    finally:
        clear_session_closes()
