from __future__ import annotations

from datetime import UTC, datetime, timedelta

from newswave.clock import utc_iso
from newswave.market.bars import FiveMinuteBuilder, aggregate_5m
from newswave.models import Bar

T0 = datetime(2026, 1, 2, 14, 30, tzinfo=UTC)  # 09:30 ET (EST)


def m1(minute: int, o=10.0, h=11.0, l=9.0, c=10.5, v=100.0, t0=T0) -> Bar:
    return Bar("X", utc_iso(t0 + timedelta(minutes=minute)), o, h, l, c, v, 1)


def test_completes_on_minute_4_and_aggregates():
    b = FiveMinuteBuilder("X")
    assert all(b.on_bar(m1(i)) == [] for i in range(4))
    out = b.on_bar(m1(4, o=1, h=20, l=0.5, c=12))
    assert len(out) == 1 and out[0].timeframe_min == 5 and out[0].start == "2026-01-02T14:30:00.000Z"
    assert (out[0].open, out[0].high, out[0].low, out[0].close, out[0].volume) == (10.0, 20, 0.5, 12, 500)


def test_gaps_ok_zero_bucket_emits_nothing_and_no_duplicates():
    b = FiveMinuteBuilder("X")
    b.on_bar(m1(1)); b.on_bar(m1(3))
    assert b.flush_due(T0 + timedelta(minutes=5, seconds=4)) == []  # grace not passed
    out = b.flush_due(T0 + timedelta(minutes=5, seconds=5))
    assert len(out) == 1 and out[0].volume == 200
    assert b.flush_due(T0 + timedelta(hours=1)) == []  # empty buckets / already emitted: nothing
    assert b.on_bar(m1(2)) == []  # late bar dropped


def test_late_bar_after_minute4_completion_dropped_and_next_bucket_started():
    b = FiveMinuteBuilder("X")
    for i in (0, 1, 2, 4):
        out = b.on_bar(m1(i))
    assert len(out) == 1
    assert b.on_bar(m1(3)) == []
    assert b.flush_due(T0 + timedelta(hours=1)) == []


def test_next_bucket_bar_completes_previous_when_minute4_missing():
    b = FiveMinuteBuilder("X")
    b.on_bar(m1(0)); b.on_bar(m1(2))
    out = b.on_bar(m1(5))
    assert [x.volume for x in out] == [200]


def test_dst_alignment_edt_and_est():
    for t0 in (datetime(2026, 1, 2, 14, 30, tzinfo=UTC), datetime(2026, 7, 2, 13, 30, tzinfo=UTC)):
        out = aggregate_5m([m1(i, t0=t0) for i in range(10)])  # 09:30 ET both seasons
        assert [o.start for o in out] == [utc_iso(t0), utc_iso(t0 + timedelta(minutes=5))]
    assert aggregate_5m([]) == []


def test_aggregate_with_gaps():
    out = aggregate_5m([m1(7), m1(0), m1(1), m1(11)])
    assert [(o.start[11:16], o.volume) for o in out] == [("14:30", 200), ("14:35", 100), ("14:40", 100)]
