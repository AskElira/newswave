"""Incremental indicators on completed 5m bars (CONTRACT §8). Pure; no I/O."""
from __future__ import annotations

from ..clock import parse_iso, to_et
from ..models import Bar


class EMA:
    """EMA seeded with the SMA of the first `period` values; `.value` is None until seeded."""

    def __init__(self, period: int) -> None:
        self.period, self.k = period, 2 / (period + 1)
        self.value: float | None = None
        self._seed: list[float] = []

    def update(self, x: float) -> float | None:
        if self.value is not None:
            self.value += self.k * (x - self.value)
        else:
            self._seed.append(x)
            if len(self._seed) == self.period:
                self.value = sum(self._seed) / self.period
        return self.value


class WilderATR:
    """True range uses the previous close (first bar: high-low). SMA seed, then Wilder smoothing."""

    def __init__(self, period: int = 14) -> None:
        self.period = period
        self.value: float | None = None
        self._prev_close: float | None = None
        self._seed: list[float] = []

    def update(self, high: float, low: float, close: float) -> float | None:
        pc = self._prev_close
        tr = high - low if pc is None else max(high - low, abs(high - pc), abs(low - pc))
        self._prev_close = close
        if self.value is not None:
            self.value = (self.value * (self.period - 1) + tr) / self.period
        else:
            self._seed.append(tr)
            if len(self._seed) == self.period:
                self.value = sum(self._seed) / self.period
        return self.value


class VWAP:
    """Session VWAP on typical price; resets when the ET session date changes."""

    def __init__(self) -> None:
        self.value: float | None = None
        self._day = None
        self._pv = self._v = 0.0

    def update(self, start_iso: str, high: float, low: float, close: float, volume: float) -> float | None:
        day = to_et(parse_iso(start_iso)).date()
        if day != self._day:
            self._day, self._pv, self._v, self.value = day, 0.0, 0.0, None
        self._pv += (high + low + close) / 3 * volume
        self._v += volume
        if self._v > 0:
            self.value = self._pv / self._v
        return self.value


def rvol(volume: float, baseline: float | None) -> float | None:
    if not baseline or baseline <= 0:
        return None
    return volume / baseline


class SymbolIndicators:
    def __init__(self) -> None:
        self.ema9, self.ema20 = EMA(9), EMA(20)
        self.atr14, self.vwap = WilderATR(14), VWAP()
        self.bars = 0
        self.close: float | None = None

    def update(self, bar: Bar) -> dict:
        self.ema9.update(bar.close)
        self.ema20.update(bar.close)
        self.atr14.update(bar.high, bar.low, bar.close)
        self.vwap.update(bar.start, bar.high, bar.low, bar.close, bar.volume)
        self.bars += 1
        self.close = bar.close
        return self.snapshot()

    def snapshot(self) -> dict:
        return {"ema9": self.ema9.value, "ema20": self.ema20.value, "atr14": self.atr14.value,
                "vwap": self.vwap.value, "close": self.close, "bars": self.bars}
