from __future__ import annotations

import pytest

from newswave.market.indicators import EMA, VWAP, SymbolIndicators, WilderATR, rvol
from newswave.models import Bar


def test_ema_sma_seed_and_hand_values():
    e = EMA(3)
    assert [e.update(x) for x in (1, 2, 3)] == [None, None, 2.0]  # SMA seed
    assert [e.update(x) for x in (4, 5, 6)] == [3.0, 4.0, 5.0]  # k = 0.5


def test_atr_hand_values():
    a = WilderATR(3)
    bars = [(10, 8, 9), (11, 9, 10), (12, 9, 11), (13, 12, 12.5), (14, 12, 13)]
    out = [a.update(*b) for b in bars]
    # TR: 2, 2, 3 (seed 7/3), then 2 -> 20/9, then 2 -> 58/27
    assert out[:2] == [None, None]
    assert out[2] == pytest.approx(7 / 3) and out[3] == pytest.approx(20 / 9) and out[4] == pytest.approx(58 / 27)


def test_atr_true_range_uses_prev_close_gap():
    a = WilderATR(2)
    a.update(10, 9, 10)            # TR 1
    assert a.update(16, 15, 15.5) == pytest.approx((1 + 6) / 2)  # gap: |16-10| = 6


def test_vwap_resets_each_et_session():
    v = VWAP()
    assert v.update("2026-01-02T14:30:00Z", 10, 10, 10, 100) == 10
    assert v.update("2026-01-02T14:35:00Z", 12, 12, 12, 100) == 11
    assert v.update("2026-01-05T14:30:00Z", 20, 20, 20, 50) == 20  # next session
    assert v.update("2026-01-05T14:35:00Z", 20, 20, 20, 0) == 20   # zero volume keeps value


def test_rvol():
    assert rvol(300, 100) == 3.0
    assert rvol(300, None) is None and rvol(300, 0) is None


def test_symbol_indicators_12_bars_vs_reference():
    bars = [Bar("X", f"2026-01-02T{14 + (30 + 5 * i) // 60}:{(30 + 5 * i) % 60:02d}:00Z",
                100 + i, 102 + i, 99 + i, 101 + i * 1.5, 1000 + i, 5) for i in range(12)]
    si, snap = SymbolIndicators(), {}
    for b in bars:
        snap = si.update(b)
    closes = [b.close for b in bars]
    ema = sum(closes[:9]) / 9
    for c in closes[9:]:
        ema += 0.2 * (c - ema)
    trs = [bars[0].high - bars[0].low] + [max(b.high - b.low, abs(b.high - p.close), abs(b.low - p.close))
                                          for p, b in zip(bars, bars[1:])]
    atr = sum(trs[:14]) / 14 if len(trs) >= 14 else None
    tp = sum((b.high + b.low + b.close) / 3 * b.volume for b in bars) / sum(b.volume for b in bars)
    assert snap["ema9"] == pytest.approx(ema)
    assert snap["ema20"] is None and snap["atr14"] is None and atr is None  # not seeded at 12 bars
    assert snap["vwap"] == pytest.approx(tp) and snap["bars"] == 12 and snap["close"] == closes[-1]
