"""Broker additions for the daemon: get_calendar / get_clock on SimBroker and AlpacaPaperBroker."""
from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from types import SimpleNamespace

from alpaca.trading.models import Clock

from newswave.clock import ReplayClock
from newswave.execution.broker import AlpacaPaperBroker, SimBroker
from test_execution_helpers import FakeTradingClient, _nosleep


def utc(*a):
    return datetime(*a, tzinfo=UTC)


async def test_sim_calendar_is_weekdays_0930_to_1600_et():
    b = SimBroker(ReplayClock(utc(2026, 1, 2, 15)))
    c = await b.get_calendar(date(2026, 1, 2))                      # Friday, EST
    assert (c.open, c.close) == (utc(2026, 1, 2, 14, 30), utc(2026, 1, 2, 21, 0))
    assert await b.get_calendar(date(2026, 1, 3)) is None            # Saturday
    c = await b.get_calendar(date(2026, 7, 1))                       # EDT
    assert (c.open, c.close) == (utc(2026, 7, 1, 13, 30), utc(2026, 7, 1, 20, 0))


async def test_sim_clock_open_closed_and_weekend_rollover():
    clk = ReplayClock(utc(2026, 1, 2, 15))
    b = SimBroker(clk)
    c = await b.get_clock()
    assert c.is_open and c.next_close == utc(2026, 1, 2, 21, 0) and c.next_open == utc(2026, 1, 5, 14, 30)
    clk.set(utc(2026, 1, 2, 14, 0))
    c = await b.get_clock()
    assert not c.is_open and c.next_open == utc(2026, 1, 2, 14, 30)
    clk.set(utc(2026, 1, 2, 22, 0))                                   # Friday after close -> Monday
    c = await b.get_clock()
    assert not c.is_open and c.next_open == utc(2026, 1, 5, 14, 30) and c.next_close == utc(2026, 1, 5, 21, 0)


class CalClient(FakeTradingClient):
    def __init__(self, sim):
        super().__init__(sim)
        self.asked = []

    def get_calendar(self, req):
        self.asked.append((req.start, req.end))
        d = req.start
        if d == date(2026, 1, 19):                                    # holiday: alpaca returns nothing
            return []
        close = (13, 0) if d == date(2026, 11, 27) else (16, 0)       # day after Thanksgiving
        return [SimpleNamespace(date=d, open=datetime(d.year, d.month, d.day, 9, 30),
                                close=datetime(d.year, d.month, d.day, *close))]

    def get_clock(self):
        return Clock(timestamp=datetime(2026, 1, 2, 10, 0), is_open=True, next_open=datetime(2026, 1, 5, 9, 30),
                     next_close=datetime(2026, 1, 2, 16, 0))


async def test_alpaca_calendar_converts_naive_et_holiday_and_early_close():
    client = CalClient(SimBroker(ReplayClock(utc(2026, 1, 2, 15))))
    b = AlpacaPaperBroker("k", "s", client=client, sleep=_nosleep)
    c = await b.get_calendar(date(2026, 1, 2))
    assert (c.open, c.close) == (utc(2026, 1, 2, 14, 30), utc(2026, 1, 2, 21, 0))
    assert client.asked == [(date(2026, 1, 2), date(2026, 1, 2))]
    assert await b.get_calendar(date(2026, 1, 19)) is None
    c = await b.get_calendar(date(2026, 11, 27))
    assert c.close == utc(2026, 11, 27, 18, 0)                        # 13:00 EST early close


async def test_alpaca_clock_is_converted_to_utc():
    b = AlpacaPaperBroker("k", "s", client=CalClient(SimBroker(ReplayClock(utc(2026, 1, 2, 15)))), sleep=_nosleep)
    c = await b.get_clock()
    assert c.is_open and c.timestamp == utc(2026, 1, 2, 15, 0) and c.next_open == utc(2026, 1, 5, 14, 30)
    assert c.next_close == utc(2026, 1, 2, 21, 0) and c.timestamp.utcoffset() == timedelta(0)
