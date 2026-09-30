from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest

from newswave.config import StrategyParams
from newswave.exits import ManagedPosition, simulate
from newswave.models import Bar, ExitReason as X, Side

P = StrategyParams()
T0 = datetime(2026, 3, 4, 15, 0, tzinfo=UTC)  # 10:00 ET


def mk(side=Side.LONG, qty=100, entry=100.0, stop=98.0, params=P):
    return ManagedPosition(side, qty, qty, entry, stop, abs(entry - stop), T0, params)


def bar(close, high=None, low=None):
    return Bar("A", "2026-03-04T15:00:00.000Z", close, high or close, low or close, close, 1000)


def kinds(acts):
    return [(a.kind, a.qty) for a in acts]


# --- stop
def test_stop_long():
    p = mk()
    assert p.on_trade(98.01, T0) == []
    assert kinds(p.on_trade(98.0, T0)) == [(X.STOP, 100)]
    assert p.on_trade(97.0, T0) == []  # pending: never twice


def test_stop_short():
    p = mk(Side.SHORT, stop=102.0)
    assert p.on_trade(101.99, T0) == []
    assert kinds(p.on_trade(102.0, T0)) == [(X.STOP, 100)]


def test_gap_through_stop_long_and_short():
    p = mk()
    a = p.on_trade(95.0, T0)
    assert kinds(a) == [(X.STOP, 100)] and a[0].price_hint == 95.0
    s = mk(Side.SHORT, stop=102.0)
    assert kinds(s.on_trade(105.0, T0)) == [(X.STOP, 100)]


def test_stop_takes_precedence_over_partial():
    # odd params where the target sits at/below the stop side: stop wins
    p = mk(params=replace(P, partial_at_r=-1.0))
    assert kinds(p.on_trade(98.0, T0)) == [(X.STOP, 100)]


def test_stop_after_partial_uses_remaining_qty():
    p = mk()
    p.on_trade(102.0, T0)
    p.apply_exit_fill(50, 102.0)
    assert kinds(p.on_trade(98.0, T0)) == [(X.STOP, 50)]


# --- partial
def test_partial_long_once():
    p = mk()
    assert p.on_trade(101.99, T0) == []
    assert kinds(p.on_trade(102.0, T0)) == [(X.PARTIAL_PROFIT, 50)]
    assert p.partial_taken and p.on_trade(105.0, T0) == []


def test_partial_short():
    p = mk(Side.SHORT, stop=102.0)
    assert p.on_trade(98.01, T0) == []
    assert kinds(p.on_trade(98.0, T0)) == [(X.PARTIAL_PROFIT, 50)]


def test_partial_rounding_and_min_one():
    assert kinds(mk(qty=7).on_trade(102, T0)) == [(X.PARTIAL_PROFIT, 3)]
    assert kinds(mk(qty=2).on_trade(102, T0)) == [(X.PARTIAL_PROFIT, 1)]
    p = mk(qty=3, params=replace(P, partial_fraction=0.1))
    assert kinds(p.on_trade(102, T0)) == [(X.PARTIAL_PROFIT, 1)]


def test_qty_one_no_partial_starts_trailing():
    p = mk(qty=1)
    assert p.on_trade(102.0, T0) == [] and p.partial_taken
    p.on_trade(104.0, T0)
    assert p.on_bar_close(bar(103.0, 104.0, 103.0), 1.0, T0) == []  # trail 102 -> close 103 fine
    assert p.trail_price == 102.0
    assert kinds(p.on_bar_close(bar(101.9), 1.0, T0)) == [(X.TRAIL, 1)]


# --- trail
def test_full_example_r_math_and_trail():
    p = mk()  # entry 100 stop 98 R=2 100 sh
    a = p.on_trade(102.0, T0)
    assert kinds(a) == [(X.PARTIAL_PROFIT, 50)]
    p.apply_exit_fill(50, 102.0)
    assert p.qty_open == 50 and not p.closed
    p.on_trade(106.0, T0)
    assert p.mfe_r == pytest.approx(3.0) and p.mae_r == 0
    assert p.on_bar_close(bar(105.5, 106.0, 105.0), 1.0, T0) == []
    assert p.trail_price == pytest.approx(104.0)  # 106 - 2*1
    a = p.on_bar_close(bar(103.9, 105.0, 103.5), 1.0, T0)
    assert kinds(a) == [(X.TRAIL, 50)]
    p.apply_exit_fill(50, 103.9)
    assert p.closed and p.qty_open == 0
    assert p.on_trade(90, T0) == [] and p.on_clock(T0 + timedelta(hours=5)) == []


def test_close_equal_trail_does_not_exit():
    p = mk()
    p.on_trade(102, T0); p.apply_exit_fill(50, 102)
    p.on_trade(106, T0)
    assert p.on_bar_close(bar(104.0, 106, 104.0), 1.0, T0) == []


def test_trail_ratchets_never_loosens():
    p = mk()
    p.on_trade(102, T0); p.apply_exit_fill(50, 102)
    p.on_trade(106, T0)
    p.on_bar_close(bar(105.5, 106, 105), 1.0, T0)
    assert p.trail_price == 104.0
    p.on_bar_close(bar(105.0, 105.5, 104.5), 3.0, T0)  # bigger ATR would give 100: ignored
    assert p.trail_price == 104.0
    p.on_trade(108, T0)
    p.on_bar_close(bar(107, 108, 106.5), 1.0, T0)
    assert p.trail_price == 106.0


def test_one_tick_dip_through_trail_is_ignored():
    p = mk()
    p.on_trade(102, T0); p.apply_exit_fill(50, 102)
    p.on_trade(106, T0)
    p.on_bar_close(bar(105.5, 106, 105), 1.0, T0)  # trail 104
    assert p.on_trade(103.0, T0) == []  # print through trail (still above stop 98)
    assert not p.closed and not p.exit_pending
    assert p.on_bar_close(bar(105.0, 105.2, 103.0), 1.0, T0) == []  # recovered: close above trail


def test_no_trail_before_partial():
    p = mk()
    p.on_trade(101.5, T0)
    assert p.on_bar_close(bar(99.0, 101.5, 99.0), 1.0, T0) == []
    assert p.trail_price is None


def test_short_full_trail():
    p = mk(Side.SHORT, stop=102.0)
    assert kinds(p.on_trade(98.0, T0)) == [(X.PARTIAL_PROFIT, 50)]
    p.apply_exit_fill(50, 98.0)
    p.on_trade(94.0, T0)
    assert p.mfe_r == pytest.approx(3.0)
    assert p.on_bar_close(bar(94.5, 95, 94), 1.0, T0) == []
    assert p.trail_price == pytest.approx(96.0)  # 94 + 2
    p.on_bar_close(bar(94.2, 94.5, 94), 3.0, T0)
    assert p.trail_price == 96.0  # ratchet only downward
    assert p.on_trade(97.0, T0) == []  # print through trail ignored
    assert kinds(p.on_bar_close(bar(96.1, 97, 95), 1.0, T0)) == [(X.TRAIL, 50)]


# --- MFE / MAE
def test_mfe_mae_long_short():
    p = mk()
    p.on_trade(99.0, T0); p.on_trade(101.0, T0)
    assert p.mae_r == pytest.approx(0.5) and p.mfe_r == pytest.approx(0.5)
    s = mk(Side.SHORT, stop=102.0)
    s.on_trade(101.0, T0); s.on_trade(99.0, T0)
    assert s.mae_r == pytest.approx(0.5) and s.mfe_r == pytest.approx(0.5)


# --- clock
def test_time_stop_long_short():
    for p in (mk(), mk(Side.SHORT, stop=102.0)):
        assert p.on_clock(T0 + timedelta(minutes=59, seconds=59)) == []
        assert kinds(p.on_clock(T0 + timedelta(minutes=60))) == [(X.TIME_STOP, 100)]
        assert p.on_clock(T0 + timedelta(minutes=61)) == []


def test_time_stop_skipped_if_mfe_enough_or_disabled():
    p = mk()
    p.on_trade(101.0, T0)  # mfe 0.5R == min_r: not < so no time stop
    assert p.on_clock(T0 + timedelta(minutes=90)) == []
    q = mk(params=replace(P, time_stop_enabled=False))
    assert q.on_clock(T0 + timedelta(minutes=90)) == []


def test_eod():
    p = mk(params=replace(P, time_stop_enabled=False))
    assert p.on_clock(datetime(2026, 3, 4, 20, 54, 59, tzinfo=UTC)) == []  # 15:54:59 ET
    q = mk(Side.SHORT, stop=102.0)
    assert kinds(q.on_clock(datetime(2026, 3, 4, 20, 55, tzinfo=UTC))) == [(X.EOD, 100)]


def test_eod_wins_over_time_stop():
    p = mk()  # mfe 0, opened 10:00 -> both due at 15:55
    assert kinds(p.on_clock(datetime(2026, 3, 4, 20, 55, tzinfo=UTC))) == [(X.EOD, 100)]


def test_eod_after_partial_uses_remaining():
    p = mk()
    p.on_trade(102, T0); p.apply_exit_fill(50, 102)
    assert kinds(p.on_clock(datetime(2026, 3, 4, 21, 0, tzinfo=UTC))) == [(X.EOD, 50)]


# --- fills, stop invariant, pending
def test_apply_exit_fill_validation_and_clear_pending():
    p = mk()
    with pytest.raises(ValueError):
        p.apply_exit_fill(101, 100)
    with pytest.raises(ValueError):
        p.apply_exit_fill(0, 100)
    p.on_trade(97, T0)
    assert p.exit_pending
    p.clear_pending_exit()
    assert kinds(p.on_trade(97, T0)) == [(X.STOP, 100)]


def test_stop_never_changes():
    p = mk()
    for ev in range(20):
        p.on_trade(100 + ev * 0.5, T0)
        p.on_bar_close(bar(100 + ev), 1.0, T0)
        p.on_clock(T0 + timedelta(minutes=ev))
    assert p.stop_price == 98.0


# --- serialization
def test_round_trip():
    p = mk()
    p.on_trade(102, T0); p.apply_exit_fill(50, 102); p.on_trade(106, T0)
    p.on_bar_close(bar(105.5, 106, 105), 1.0, T0)
    d = p.to_dict()
    assert d["opened_at"] == "2026-03-04T15:00:00.000Z" and d["side"] == "LONG"
    q = ManagedPosition.from_dict(d, P)
    assert q == p and q.to_dict() == d
    assert kinds(q.on_bar_close(bar(103.9, 105, 103.5), 1.0, T0)) == [(X.TRAIL, 50)]
    import json
    json.dumps(d)


# --- simulate
def test_simulate_full_path_long():
    p = mk()
    ev = [("trade", 101.0, T0), ("trade", 102.0, T0 + timedelta(minutes=1)), ("trade", 106.0, T0 + timedelta(minutes=2)),
          ("bar", bar(105.5, 106, 105), 1.0, T0 + timedelta(minutes=5)),
          ("bar", bar(103.9, 105, 103.5), 1.0, T0 + timedelta(minutes=10)),
          ("trade", 50.0, T0 + timedelta(minutes=11))]  # ignored after close
    fills, reason = simulate(p, ev)
    assert [(f.kind, f.qty, f.price) for f in fills] == [(X.PARTIAL_PROFIT, 50, 102.0), (X.TRAIL, 50, 103.9)]
    assert reason == X.TRAIL and p.closed
    pnl = 50 * 2.0 + 50 * 3.9
    assert pnl == pytest.approx(295.0)


def test_simulate_gap_stop_fills_at_print():
    fills, reason = simulate(mk(), [("trade", 95.0, T0)])
    assert fills[0].price == 95.0 and reason == X.STOP
    fills, reason = simulate(mk(Side.SHORT, stop=102.0), [("trade", 104.0, T0)])
    assert fills[0].price == 104.0 and reason == X.STOP


def test_simulate_clock_uses_last_price_and_open_returns_none():
    fills, reason = simulate(mk(), [("trade", 99.0, T0), ("clock", T0 + timedelta(minutes=61))])
    assert (fills[0].kind, fills[0].price, reason) == (X.TIME_STOP, 99.0, X.TIME_STOP)
    fills, reason = simulate(mk(), [("trade", 99.5, T0)])
    assert fills == [] and reason is None
    fills, reason = simulate(mk(), [("clock", datetime(2026, 3, 4, 21, 0, tzinfo=UTC))])
    assert fills[0].price == 100.0 and reason == X.EOD  # no price seen: entry


def test_simulate_partial_only_then_eod_reason_is_eod():
    ev = [("trade", 102.0, T0), ("clock", datetime(2026, 3, 4, 21, 0, tzinfo=UTC))]
    fills, reason = simulate(mk(), ev)
    assert [f.kind for f in fills] == [X.PARTIAL_PROFIT, X.EOD] and reason == X.EOD


def test_simulate_unknown_event():
    with pytest.raises(ValueError):
        simulate(mk(), [("nope", 1)])


def test_entry_bar_does_not_feed_its_pre_entry_extremes():
    """REGRESSION (review 10): the 5m bar that contains the entry carried its PRE-entry high/low into the
    extremes, MFE/MAE and the trail."""
    opened = datetime(2026, 3, 4, 15, 3, tzinfo=UTC)  # entry inside the 15:00 bucket
    p = ManagedPosition(Side.LONG, 100, 100, 100.0, 98.0, 2.0, opened, P)
    p.on_trade(100.4, opened + timedelta(seconds=30))
    p.on_bar_close(Bar("A", "2026-03-04T15:00:00.000Z", 100.0, 110.0, 90.0, 100.5, 1000), 1.0, opened + timedelta(minutes=2))
    assert p.highest_since_entry == 100.4 and p.lowest_since_entry == 100.0
    assert p.mfe_r == pytest.approx(0.2) and p.mae_r == 0.0
    p.on_bar_close(Bar("A", "2026-03-04T15:05:00.000Z", 100.5, 101.0, 100.2, 100.8, 1000), 1.0, opened + timedelta(minutes=7))
    assert p.highest_since_entry == 101.0 and p.mfe_r == pytest.approx(0.5)


def test_early_close_moves_the_eod_flatten_to_close_minus_5():
    from newswave.clock import clear_session_closes, set_session_close
    clear_session_closes()
    try:
        set_session_close(datetime(2026, 3, 4).date(), datetime(2026, 3, 4, 18, 0, tzinfo=UTC))  # 13:00 EST close
        p = mk(params=replace(P, time_stop_enabled=False))
        assert p.on_clock(datetime(2026, 3, 4, 17, 54, 59, tzinfo=UTC)) == []
        assert kinds(p.on_clock(datetime(2026, 3, 4, 17, 55, tzinfo=UTC))) == [(X.EOD, 100)]
    finally:
        clear_session_closes()
