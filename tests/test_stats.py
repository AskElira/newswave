from __future__ import annotations

import math
from datetime import UTC, datetime

import pytest

from newswave.stats import bucket, group_by, summarize

T = [{"pnl": 100, "r_multiple": 1.0, "exit_at": "2026-03-02T16:00:00.000Z"},
     {"pnl": -50, "r_multiple": -0.5, "exit_at": "2026-03-03T16:00:00.000Z"},
     {"pnl": 200, "r_multiple": 2.0, "exit_at": "2026-03-04T16:00:00.000Z"},
     {"pnl": -100, "r_multiple": -1.0, "exit_at": "2026-03-05T16:00:00.000Z"}]


def test_summary_from_pnl():
    s = summarize(T)
    assert (s["n"], s["wins"], s["losses"]) == (4, 2, 2)
    assert s["win_rate"] == 0.5 and s["avg_win"] == 150 and s["avg_loss"] == -75
    assert s["profit_factor"] == 2.0 and s["expectancy_dollars"] == 37.5
    assert s["expectancy_r"] == pytest.approx(0.375) and s["total_pnl"] == 150
    # 6000 -> 6100, 6050, 6250, 6150: max dd 100 from 6250
    assert s["max_drawdown_dollars"] == 100 and s["max_drawdown_pct"] == pytest.approx(1.6)
    assert s["sharpe"] is None


def test_drawdown_uses_exit_order_and_start_equity():
    s = summarize(list(reversed(T)), starting_equity=1000)
    # 1000 -> 1100 -> 1050 -> 1250 -> 1150: dd 100 of 1250 = 8%
    assert s["max_drawdown_dollars"] == 100 and s["max_drawdown_pct"] == pytest.approx(8.0)


def test_drawdown_initial_loss():
    s = summarize([{"pnl": -300, "r_multiple": -1}], starting_equity=6000)
    assert s["max_drawdown_dollars"] == 300 and s["max_drawdown_pct"] == pytest.approx(5.0)
    assert s["profit_factor"] == 0.0 and s["wins"] == 0


def test_no_losses_and_empty():
    s = summarize([{"pnl": 10, "r_multiple": 0.5}])
    assert s["profit_factor"] is None and s["win_rate"] == 1.0 and s["avg_loss"] == 0
    e = summarize([])
    assert e["n"] == 0 and e["win_rate"] == 0 and e["profit_factor"] is None and e["max_drawdown_dollars"] == 0


def test_zero_pnl_is_neither():
    s = summarize([{"pnl": 0, "r_multiple": 0}, {"pnl": 50, "r_multiple": 1}])
    assert s["wins"] == 1 and s["losses"] == 0 and s["win_rate"] == 0.5


def _d(day, hour=20):
    return datetime(2026, 3, day, hour, 0, tzinfo=UTC)


def test_curve_drawdown_and_sharpe():
    curve = [(_d(2), 1000.0), (_d(3), 1100.0), (_d(4), 990.0), (_d(5), 1089.0)]
    s = summarize([], curve)
    assert s["max_drawdown_dollars"] == pytest.approx(110) and s["max_drawdown_pct"] == pytest.approx(10.0)
    # returns .1, -.1, .1 -> mean 1/30, sample sd sqrt(.0133333)
    sd = math.sqrt(0.04 / 3)
    assert s["sharpe"] == pytest.approx((1 / 30) / sd * math.sqrt(252))


def test_sharpe_last_value_per_et_date_and_nones():
    # two points on the same ET date: the later one counts
    curve = [(_d(2, 15), 5.0), (_d(2, 20), 1000.0), (_d(3, 20), 1100.0), (_d(4, 20), 990.0)]
    s = summarize([], curve)
    assert s["sharpe"] == pytest.approx(summarize([], [(_d(2), 1000.0), (_d(3), 1100.0), (_d(4), 990.0)])["sharpe"])
    assert s["sharpe"] is not None
    assert summarize([], [(_d(2), 1000.0), (_d(3), 1100.0)])["sharpe"] is None  # 1 return
    assert summarize([], [(_d(2), 100.0), (_d(3), 100.0), (_d(4), 100.0)])["sharpe"] is None  # std 0


def test_group_by_and_bucket():
    g = group_by(T, lambda t: "win" if t["pnl"] > 0 else "loss")
    assert g["win"]["n"] == 2 and g["win"]["total_pnl"] == 300 and g["loss"]["total_pnl"] == -150
    assert group_by([], lambda t: 1) == {}
    e = [1, 2, 3]
    assert [bucket(v, e) for v in (0.5, 1, 1.99, 2, 3, 10, None)] == ["<1", "1-2", "1-2", "2-3", "3+", "3+", "n/a"]
    assert bucket(0.75, [0.5, 1.5]) == "0.5-1.5"
