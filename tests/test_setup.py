"""SetupMachine: every scenario runs LONG and SHORT (SHORT = the same tape mirrored around ref_price)."""
from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from newswave.clock import utc_iso
from newswave.config import StrategyParams
from newswave.models import Bar, EntrySignal, RejectReason, Side, Stage, Trade
from newswave.strategy.setup import SetupMachine

REF = 100.0
NEWS = datetime(2026, 1, 2, 15, 2, 10, tzinfo=UTC)   # Fri 10:02:10 ET
T0 = datetime(2026, 1, 2, 15, 0, tzinfo=UTC)         # 10:00 ET bucket = index 0
SIDES = pytest.mark.parametrize("side", [Side.LONG, Side.SHORT])


def P(side, x):
    return x if side == Side.LONG else 2 * REF - x


def mk(side, news=NEWS, **kw):
    pk = kw.pop("params", StrategyParams())
    return SetupMachine(1, "NVDA", side, kw.pop("variant", "production"), pk, news, REF, **kw)


def step(m, side, i, h, l, c, v, ema=100.5, atr=1.0, base=1000.0):
    """One completed 5m bar written in LONG terms (mirrored for SHORT); returns the transitions."""
    hi, lo = (h, l) if side == Side.LONG else (P(side, l), P(side, h))
    b = Bar("NVDA", utc_iso(T0 + timedelta(minutes=5 * i)), P(side, c), hi, lo, P(side, c), v, 5)
    return m.on_bar(b, {"ema9": P(side, ema), "atr": atr}, base)


def trade(side, px, minute=21.0):
    return Trade("NVDA", utc_iso(T0 + timedelta(minutes=minute)), P(side, px), 100)


def to_pullback(m, side):
    """bar0 rvol 1.5 / impulse 1%, bar1 rvol 2.5 / impulse 2% -> WAITING_FOR_PULLBACK (impulse extreme 102)."""
    assert step(m, side, 0, 101.0, 100.0, 100.8, 1500) == []
    t = step(m, side, 1, 102.0, 100.8, 101.8, 2500)
    assert [x.stage for x in t] == [Stage.WAITING_FOR_IMPULSE, Stage.WAITING_FOR_PULLBACK]
    return t


def to_touch(m, side):
    to_pullback(m, side)
    step(m, side, 2, 101.5, 100.9, 101.0, 800)            # pullback bar 1, no touch
    t = step(m, side, 3, 101.3, 100.6, 100.9, 700)        # low 100.6 <= ema 100.5 + 0.2
    assert t[-1].stage == Stage.WAITING_FOR_BREAKOUT
    return t


def last_reject(ts):
    assert ts and ts[-1].stage == Stage.REJECTED
    return ts[-1].reject_reason


# ------------------------------------------------------------------ clean path
@SIDES
def test_clean_path_to_entry_signal(side):
    m = mk(side)
    assert m.stage == Stage.WAITING_FOR_VOLUME
    t = to_pullback(m, side)
    assert t[0].message == "RVOL 2.5" and t[1].message == "price impulse +2.0%"
    assert m.numbers["rvol"] == pytest.approx(2.5) and m.numbers["impulse_pct"] == pytest.approx(2.0)
    assert m.numbers["impulse_extreme"] == pytest.approx(P(side, 102.0))
    t = step(m, side, 2, 101.5, 100.9, 101.0, 800)
    assert m.stage == Stage.WAITING_FOR_PULLBACK and t[0].message == "waiting for 9 EMA"
    t = step(m, side, 3, 101.3, 100.6, 100.9, 700)
    assert m.stage == Stage.WAITING_FOR_BREAKOUT and t[0].message == "9 EMA pullback confirmed"
    n = m.numbers
    assert n["pullback_bars"] == 2 and n["ema9_at_touch"] == pytest.approx(P(side, 100.5)) and n["atr"] == 1.0
    if side == Side.LONG:
        assert (n["pullback_high"], n["pullback_low"]) == (101.5, 100.6)
        assert n["entry_trigger"] == pytest.approx(101.55)
    else:
        assert (n["pullback_high"], n["pullback_low"]) == (pytest.approx(99.4), pytest.approx(98.5))
        assert n["entry_trigger"] == pytest.approx(98.45)
    assert m.on_trade(trade(side, 101.54)) is None       # not through the trigger yet
    assert m.drain() == []
    sig = m.on_trade(trade(side, 101.56))
    assert isinstance(sig, EntrySignal)
    assert (sig.setup_id, sig.symbol, sig.side, sig.variant) == (1, "NVDA", side, "production")
    assert sig.trigger_price == pytest.approx(n["entry_trigger"]) and sig.atr == 1.0
    assert (sig.pullback_low, sig.pullback_high) == (n["pullback_low"], n["pullback_high"])
    assert sig.signal_at == utc_iso(T0 + timedelta(minutes=21)) and m.signal_price == P(side, 101.56)
    [tr] = m.drain()
    assert tr.stage == Stage.ENTRY_SIGNAL and tr.data["print_price"] == P(side, 101.56)
    assert m.stage == m.max_stage == Stage.ENTRY_SIGNAL and m.done
    assert m.on_trade(trade(side, 105.0, 22)) is None    # one signal per machine
    assert step(m, side, 4, 103, 101, 102, 900) == [] and m.on_clock(NEWS + timedelta(hours=2)) == []


# ------------------------------------------------------------------ volume / momentum window
@SIDES
def test_no_volume_after_window(side):
    m = mk(side)
    assert step(m, side, 0, 102.0, 100.0, 101.9, 900) == []
    assert step(m, side, 1, 103.0, 101.0, 102.9, 1200) == []
    assert last_reject(step(m, side, 2, 103.5, 102.0, 103.0, 1900)) == RejectReason.NO_VOLUME
    assert m.max_stage == Stage.WAITING_FOR_VOLUME and m.stage == Stage.REJECTED


@SIDES
def test_no_momentum_after_window(side):
    m = mk(side)
    assert [x.stage for x in step(m, side, 0, 100.5, 100.0, 100.4, 3000)] == [Stage.WAITING_FOR_IMPULSE]
    assert step(m, side, 1, 100.9, 100.2, 100.8, 3000) == []
    assert last_reject(step(m, side, 2, 101.0, 100.3, 100.9, 3000)) == RejectReason.NO_MOMENTUM
    assert m.max_stage == Stage.WAITING_FOR_IMPULSE


@SIDES
def test_rvol_met_on_bar_two_not_one(side):
    m = mk(side)
    assert step(m, side, 0, 101.0, 100.0, 100.9, 1999) == [] and m.stage == Stage.WAITING_FOR_VOLUME  # rvol 1.999
    t = step(m, side, 1, 102.0, 100.8, 101.8, 2000)                                                    # rvol 2.0 met
    assert t[0].message == "RVOL 2.0" and m.stage == Stage.WAITING_FOR_PULLBACK
    assert m.numbers["rvol"] == pytest.approx(2.0) and m.max_rvol_seen == pytest.approx(2.0)


@SIDES
def test_volume_first_then_impulse_on_later_bar(side):
    m = mk(side)
    assert [x.stage for x in step(m, side, 0, 100.5, 100.0, 100.4, 2500)] == [Stage.WAITING_FOR_IMPULSE]
    t = step(m, side, 1, 101.6, 100.3, 101.5, 400)   # volume already latched, impulse +1.6% now
    assert [x.stage for x in t] == [Stage.WAITING_FOR_PULLBACK]


@SIDES
def test_no_baseline_never_meets_volume(side):
    m = mk(side)
    for i in range(3):
        t = step(m, side, i, 103.0, 100.0, 102.0, 10**6, base=None)
    assert last_reject(t) == RejectReason.NO_VOLUME


@SIDES
def test_bars_before_news_ignored(side):
    m = mk(side)
    b = Bar("NVDA", utc_iso(T0 - timedelta(minutes=5)), 100, 110, 99, 109, 10**6, 5)  # 09:55 ends 10:00 < news
    assert m.on_bar(b, {"ema9": 100, "atr": 1}, 1000.0) == []
    assert m._n == 0 and m.max_rvol_seen == 0 and m.impulse_pct is None
    # the bucket that CONTAINS the news counts
    assert step(m, side, 0, 101.0, 100.0, 100.9, 2500) != [] and m._n == 1


# ------------------------------------------------------------------ pullback
@SIDES
def test_impulse_extends_and_resets_pullback_count(side):
    m = mk(side)
    to_pullback(m, side)
    for i in (2, 3, 4):
        assert step(m, side, i, 101.8, 101.2, 101.5, 500)[0].message == "waiting for 9 EMA"
    assert m.numbers["pullback_bars"] == 3
    t = step(m, side, 5, 103.0, 101.5, 102.9, 900)        # new high before any touch
    assert t[0].message.startswith("impulse extends") and m.stage == Stage.WAITING_FOR_PULLBACK
    assert m.numbers["impulse_extreme"] == pytest.approx(P(side, 103.0)) and m.numbers["pullback_bars"] is None
    for i in (6, 7, 8, 9):                                # four more non-touch bars are fine again
        assert step(m, side, i, 102.8, 102.0, 102.5, 500)[0].stage == Stage.WAITING_FOR_PULLBACK
    assert last_reject(step(m, side, 10, 102.8, 102.0, 102.5, 500)) == RejectReason.NO_PULLBACK


@pytest.mark.parametrize("low,touches", [(100.69, True), (100.71, False)])
@SIDES
def test_touch_tolerance_boundary(side, low, touches):
    m = mk(side)
    to_pullback(m, side)                                   # ema 100.5, atr 1 -> threshold 100.7
    t = step(m, side, 2, 101.5, low, 101.0, 800)
    assert (m.stage == Stage.WAITING_FOR_BREAKOUT) is touches
    assert t[0].message == ("9 EMA pullback confirmed" if touches else "waiting for 9 EMA")


@SIDES
def test_pullback_collapse(side):
    m = mk(side)
    to_pullback(m, side)
    # close 100.0 < ema 101.0 - 0.5; low 100.4 stays above ref so it is not IMPULSE_LOST
    assert last_reject(step(m, side, 2, 101.0, 100.4, 100.0, 800, ema=101.0)) == RejectReason.PULLBACK_COLLAPSE


@SIDES
def test_collapse_boundary_is_strict(side):
    m = mk(side)
    to_pullback(m, side)
    t = step(m, side, 2, 101.0, 100.4, 100.5, 800, ema=101.0)   # close == ema - 0.5 exactly: not a collapse
    assert t[0].stage != Stage.REJECTED


@SIDES
def test_impulse_lost(side):
    m = mk(side)
    to_pullback(m, side)
    assert last_reject(step(m, side, 2, 101.0, 100.0, 100.4, 800, ema=100.4)) == RejectReason.IMPULSE_LOST


@SIDES
def test_impulse_lost_by_print(side):
    m = mk(side)
    to_touch(m, side)
    assert m.on_trade(trade(side, 99.9)) is None
    [t] = m.drain()
    assert t.reject_reason == RejectReason.IMPULSE_LOST and m.stage == Stage.REJECTED


@SIDES
def test_volume_dried_up(side):
    m = mk(side)
    to_pullback(m, side)
    assert last_reject(step(m, side, 2, 101.5, 100.9, 101.0, 0)) == RejectReason.VOLUME_DRIED_UP


@SIDES
def test_no_pullback_after_five_non_touch_bars(side):
    m = mk(side)
    to_pullback(m, side)
    for i in range(2, 6):
        assert step(m, side, i, 101.8, 101.2, 101.5, 500)[0].stage == Stage.WAITING_FOR_PULLBACK
    assert m.numbers["pullback_bars"] == 4
    assert last_reject(step(m, side, 6, 101.8, 101.2, 101.5, 500)) == RejectReason.NO_PULLBACK
    assert m.max_stage == Stage.WAITING_FOR_PULLBACK


@SIDES
def test_touch_on_fourth_bar_is_valid(side):
    m = mk(side)
    to_pullback(m, side)
    for i in (2, 3, 4):
        step(m, side, i, 101.8, 101.2, 101.5, 500)
    assert step(m, side, 5, 101.6, 100.6, 101.0, 500)[0].stage == Stage.WAITING_FOR_BREAKOUT
    assert m.numbers["pullback_bars"] == 4


# ------------------------------------------------------------------ breakout
@SIDES
def test_trigger_updates_when_pullback_extends(side):
    m = mk(side)
    to_touch(m, side)
    old = m.numbers
    t = step(m, side, 4, 101.8, 100.2, 101.2, 600)        # deeper low, higher high, still valid
    assert t[0].stage == Stage.WAITING_FOR_BREAKOUT and "pullback extended" in t[0].message
    n = m.numbers
    assert n["pullback_bars"] == 3
    if side == Side.LONG:
        assert n["pullback_high"] == 101.8 and n["pullback_low"] == 100.2
        assert n["entry_trigger"] == pytest.approx(101.85) and old["entry_trigger"] == pytest.approx(101.55)
        assert m.on_trade(trade(side, 101.7, 26)) is None       # old trigger no longer enough
    assert m.on_trade(trade(side, 101.9, 26)) is not None


@SIDES
def test_touch_alone_never_signals(side):
    m = mk(side)
    to_pullback(m, side)
    assert m.on_trade(trade(side, 150.0, 12)) is None    # WAITING_FOR_PULLBACK: a print cannot signal
    step(m, side, 2, 101.5, 100.6, 100.9, 800)           # touch bar
    assert m.stage == Stage.WAITING_FOR_BREAKOUT and m.signal is None
    for px in (100.7, 101.0, 101.5, 101.55):
        assert m.on_trade(trade(side, px)) is None
    assert m.signal is None and m.stage == Stage.WAITING_FOR_BREAKOUT


@SIDES
def test_max_stage_never_decreases(side):
    m = mk(side)
    seen = [m.max_stage]
    to_pullback(m, side); seen.append(m.max_stage)
    step(m, side, 2, 101.5, 100.6, 100.9, 800); seen.append(m.max_stage)
    step(m, side, 3, 103.0, 101.0, 102.9, 800); seen.append(m.max_stage)   # extend while touched: stays
    m.reject(RejectReason.SETUP_EXPIRED); seen.append(m.max_stage)
    ranks = [s.rank for s in seen]
    assert ranks == sorted(ranks) and m.stage == Stage.REJECTED and m.max_stage == Stage.WAITING_FOR_BREAKOUT


# ------------------------------------------------------------------ clock
@SIDES
def test_setup_expired_at_45_minutes(side):
    m = mk(side)
    to_touch(m, side)
    assert m.on_clock(NEWS + timedelta(minutes=44, seconds=59)) == []
    assert last_reject(m.on_clock(NEWS + timedelta(minutes=45))) == RejectReason.SETUP_EXPIRED
    assert m.on_clock(NEWS + timedelta(minutes=60)) == []


@SIDES
def test_late_print_never_signals(side):
    m = mk(side)
    to_touch(m, side)
    late = Trade("NVDA", utc_iso(NEWS + timedelta(minutes=46)), P(side, 105.0), 10)
    assert m.on_trade(late) is None and m.signal is None


def test_after_cutoff():
    news = datetime(2026, 1, 2, 20, 29, tzinfo=UTC)       # 15:29 ET
    m = mk(Side.LONG, news=news)
    assert m.on_clock(datetime(2026, 1, 2, 20, 29, 59, tzinfo=UTC)) == []
    assert last_reject(m.on_clock(datetime(2026, 1, 2, 20, 30, tzinfo=UTC))) == RejectReason.AFTER_CUTOFF
    m = mk(Side.LONG, news=news)
    m.stage = Stage.WAITING_FOR_BREAKOUT
    m.entry_trigger = 100.0
    late = Trade("NVDA", "2026-01-02T20:30:01.000Z", 105.0, 10)
    assert m.on_trade(late) is None


# ------------------------------------------------------------------ second pullback shadow
@SIDES
def test_second_pullback_signals_only_on_second_breakout(side):
    m = mk(side, pullback_number=2, variant="second_pullback")
    plain = mk(side)
    for x in (m, plain):
        to_touch(x, side)
    assert plain.on_trade(trade(side, 101.6)) is not None          # first-pullback machine signals here
    assert m.on_trade(trade(side, 101.6)) is None                  # 2nd-pullback machine does not
    [t] = m.drain()
    assert t.stage == Stage.WAITING_FOR_PULLBACK and "2nd" in t.message and m.signal is None
    assert m.numbers["entry_trigger"] is None and m.numbers["pullback_high"] is None
    assert m.max_stage == Stage.WAITING_FOR_BREAKOUT
    # the bar that contains the breakout print (10:20 bucket, print at 10:21) is impulse, not a pullback bar
    assert step(m, side, 4, 103.0, 100.6, 102.9, 900) == []
    assert m.stage == Stage.WAITING_FOR_PULLBACK and m._pb == []
    t = step(m, side, 5, 102.5, 102.0, 102.2, 600, ema=101.6)      # rally... pullback bar 1, no touch (102.0 > 101.8)
    assert t[0].message == "waiting for 9 EMA"
    t = step(m, side, 6, 102.4, 101.7, 102.0, 600, ema=101.6)      # touch (101.7 <= 101.6 + 0.2)
    assert t[0].stage == Stage.WAITING_FOR_BREAKOUT
    n = m.numbers
    assert n["pullback_bars"] == 2
    if side == Side.LONG:
        assert n["pullback_high"] == 102.5 and n["pullback_low"] == 101.7
        assert n["entry_trigger"] == pytest.approx(102.55)
    assert m.on_trade(trade(side, 102.5, 33)) is None
    sig = m.on_trade(trade(side, 102.6, 34))
    assert sig is not None and sig.variant == "second_pullback"
    assert sig.trigger_price == pytest.approx(n["entry_trigger"])


@SIDES
def test_rvol_min_override_and_max_rvol_seen(side):
    m = mk(side, rvol_min=1.5, variant="rvol_1_5")
    step(m, side, 0, 101.0, 100.0, 100.8, 1500)        # rvol 1.5 meets the 1.5 gate
    t = step(m, side, 1, 102.0, 100.8, 101.8, 1700)
    assert m.stage == Stage.WAITING_FOR_PULLBACK and m.max_rvol_seen == pytest.approx(1.7)
    step(m, side, 2, 101.9, 101.2, 101.5, 2300)         # bar 3 of the window still feeds max_rvol_seen
    assert m.max_rvol_seen == pytest.approx(2.3)
    step(m, side, 3, 101.9, 101.2, 101.5, 9000)         # beyond the window: ignored
    assert m.max_rvol_seen == pytest.approx(2.3)


# ------------------------------------------------------------------ review fixes
@SIDES
def test_cold_indicators_reject_no_market_data_at_once(side):
    m = mk(side)
    to_pullback(m, side)
    b = Bar("NVDA", utc_iso(T0 + timedelta(minutes=10)), P(side, 101.0), P(side, 101.5), P(side, 100.9),
            P(side, 101.0), 800, 5)
    t = m.on_bar(b, {"ema9": None, "atr": None}, 1000.0)       # not warm: used to stall silently to SETUP_EXPIRED
    assert last_reject(t) == RejectReason.NO_MARKET_DATA
    assert "not warm" in t[-1].message and m.stage == Stage.REJECTED
