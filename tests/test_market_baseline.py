from __future__ import annotations

from datetime import UTC, datetime, timedelta
from types import SimpleNamespace as NS

import pytest

from newswave.clock import ReplayClock, utc_iso
from newswave.market.baseline import HistoricalData, build_baseline, warmup_bars
from newswave.models import Bar


class APIError(Exception):
    def __init__(self, status, msg=""):
        super().__init__(msg or str(status))
        self.status_code = status


def day_bars(day: datetime, vols: dict[int, float]) -> list[Bar]:
    """day = 09:30 ET open in UTC; vols = {slot index: volume}."""
    return [Bar("X", utc_iso(day + timedelta(minutes=5 * i)), 1, 1, 1, 1, v, 5) for i, v in vols.items()]


def sessions(n, start=datetime(2026, 1, 5, 14, 30, tzinfo=UTC)):
    return [start + timedelta(days=i) for i in range(n)]  # Mon..; fine for ET weekday filter up to 5


def test_build_baseline_mean_ignores_missing_slots_and_needs_min_days():
    ds = sessions(5)
    bars = []
    for i, d in enumerate(ds):
        bars += day_bars(d, {0: 100 * (i + 1), 1: 50} if i < 4 else {0: 600})  # slot 1 missing on day 5
    b = build_baseline(bars)
    assert b == {"09:30": 320.0, "09:35": 40.0}  # slot 1 missing on day 5 counts as 0: (4*50)/5
    assert build_baseline(bars[: 2 * 4], min_days=5) is None  # only 4 sessions
    # premarket bar ignored
    pre = Bar("X", utc_iso(ds[0] - timedelta(hours=2)), 1, 1, 1, 1, 9999, 5)
    assert build_baseline(bars + [pre]) == b


def test_build_baseline_uses_last_n_days():
    ds = sessions(5)
    bars = [x for i, d in enumerate(ds) for x in day_bars(d, {0: float(10 ** i)})]
    assert build_baseline(bars, days=2, min_days=2) == {"09:30": (1000 + 10000) / 2}


def test_warmup_last_sessions_sorted_regular_only():
    ds = sessions(5)
    bars = [x for d in ds for x in day_bars(d, {0: 1, 1: 1})]
    bars.append(Bar("X", utc_iso(ds[4] + timedelta(hours=8)), 1, 1, 1, 1, 1, 5))  # 17:30 ET after close
    w = warmup_bars(list(reversed(bars)), sessions=3)
    assert len(w) == 6 and w[0].start < w[-1].start and w[0].start.startswith("2026-01-07")


class FakeClient:
    def __init__(self, script):
        self.script, self.calls = list(script), []

    def get_stock_bars(self, req):
        self.calls.append(req)
        r = self.script.pop(0)
        if isinstance(r, Exception):
            raise r
        return NS(data={"X": r})

    def get_stock_latest_trade(self, req):
        self.calls.append(req)
        r = self.script.pop(0)
        if isinstance(r, Exception):
            raise r
        return r


def ab(ts, v=10):
    return NS(timestamp=ts, open=1, high=2, low=0.5, close=1.5, volume=v)


def hd(script, sleeps):
    return HistoricalData("k", "s", client=FakeClient(script), clock=ReplayClock(),
                          sleep=sleeps.append, backoff_s=1.0)


T = datetime(2026, 1, 5, 14, 30, tzinfo=UTC)


def test_bars_5m_converts_and_requests_iex():
    h = hd([[ab(T)]], [])
    out = h.bars_5m("X", T - timedelta(days=1), T)
    assert out == [Bar("X", utc_iso(T), 1.0, 2.0, 0.5, 1.5, 10.0, 5)]
    assert str(h.client.calls[0].feed).endswith("IEX") and h.client.calls[0].timeframe.amount == 5
    assert hd([[ab(T)]], []).bars_1m("X", T)[0].timeframe_min == 1


def test_429_retried_with_exponential_backoff_then_gives_up():
    sl: list[float] = []
    assert len(hd([APIError(429), APIError(503), [ab(T)]], sl).bars_5m("X", T)) == 1
    assert sl == [1.0, 2.0]
    sl2: list[float] = []
    with pytest.raises(APIError):
        hd([APIError(429)] * 4, sl2).bars_5m("X", T)
    assert sl2 == [1.0, 2.0, 4.0]  # 4 tries, 3 sleeps, never tight-loops
    with pytest.raises(APIError):  # non-transient: no retry
        hd([APIError(400), [ab(T)]], []).bars_5m("X", T)


def test_daily_sip_used_when_allowed():
    h = hd([[ab(T)] * 30], [])
    bars, feed = h.daily_bars("X", days=20, feed="sip")
    assert feed == "sip" and len(bars) == 20 and bars[0].timeframe_min == 1440
    assert h.client.calls[0].end.replace(tzinfo=UTC) < ReplayClock().now()  # end lags now for SIP


def test_daily_falls_back_to_iex_on_permission_error():
    h = hd([APIError(403, "subscription does not permit querying recent SIP data"), [ab(T)] * 3], [])
    bars, feed = h.daily_bars("X", days=20)
    assert feed == "iex" and len(bars) == 3 and len(h.client.calls) == 2
    assert str(h.client.calls[1].feed).endswith("IEX")
    with pytest.raises(APIError):  # other errors are not masked
        hd([APIError(400)], []).daily_bars("X")


def test_latest_trade():
    t = NS(timestamp=T, price=12.5, size=3)
    assert hd([{"X": t}], []).latest_trade("X").price == 12.5
    assert hd([{}], []).latest_trade("X") is None


def test_baseline_counts_a_missing_slot_as_zero_volume_for_thin_symbols():
    ds = sessions(5)
    bars = []
    for i, d in enumerate(ds):   # the 09:35 slot only ever prints on ONE of five sessions
        bars += day_bars(d, {0: 1000, 1: 500} if i == 0 else {0: 1000})
    assert build_baseline(bars) == {"09:30": 1000.0, "09:35": 100.0}   # 500/5, not 500/1
