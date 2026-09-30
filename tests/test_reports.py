from __future__ import annotations

from datetime import UTC, datetime

from newswave.clock import ReplayClock
from newswave.reports import build_monthly_report, maybe_generate_monthly, month_bounds

V = "v1"


def z(day: int, hhmm: str, month: int = 9) -> str:
    return f"2026-{month:02d}-{day:02d}T{hhmm}:00.000Z"


def add_trade(db, variant, shadow, exit_at, pnl, r, **kw):
    row = {"symbol": "AAPL", "side": "LONG", "is_shadow": shadow, "variant": variant, "entry_at": exit_at,
           "exit_at": exit_at, "entry_price": 100, "avg_exit_price": 101, "qty": 10, "pnl": pnl, "r_multiple": r,
           "catalyst": "guidance", "ai_confidence": 0.85, "rvol": 2.5, "impulse_pct": 2.5, "atr": 1.0,
           "entry_latency_ms": 300, "strategy_version": V}
    row.update(kw)
    db.insert("trades", row)


def seed_small(db):
    add_trade(db, "production", 0, z(3, "15:00"), 100, 2, catalyst="earnings", ai_confidence=0.95)
    add_trade(db, "production", 0, z(4, "15:00"), -50, -1, symbol="MSFT", rvol=1.9)
    add_trade(db, "production", 0, z(5, "15:00"), 50, 1, symbol="MSFT")
    add_trade(db, "neutral_news", 1, z(6, "15:00"), 10, 0.2)
    add_trade(db, "production", 0, z(3, "15:00", 10), 999, 9)  # other month: excluded
    db.insert("symbols", {"symbol": "AAPL", "market_cap": 3e12})
    for ts, le in [(z(31, "20:00", 8), 6000), (z(30, "20:00"), 6100)]:
        db.insert("equity_snapshots", {"ts": ts, "ledger_equity": le, "broker_equity": le, "strategy_version": V})
    db.insert("setups", {"article_id": "a", "symbol": "AAPL", "variant": "production", "stage": "REJECTED",
                         "max_stage": "CLASSIFIED", "reject_reason": "AI_NEUTRAL", "ai_direction": "NEUTRAL",
                         "created_at": z(3, "14:00"), "strategy_version": V})


def test_report_numbers_and_wording(tmp_db, tmp_path):
    seed_small(tmp_db)
    md = build_monthly_report(tmp_db, "2026-09", V, tmp_path)
    assert md == tmp_path / V / "2026-09.md" and (tmp_path / V / "2026-09.html").exists()
    t = md.read_text(encoding="utf-8")
    for line in ["| Starting equity | 6,000.00 |", "| Ending equity | 6,100.00 |", "| Return | 1.7% |", "| Trades | 3 |",
                 "| Wins | 2 |", "| Losses | 1 |", "| Win rate | 66.7% |", "| Average winner | 75.00 |",
                 "| Average loser | -50.00 |", "| Profit factor | 3.00 |", "| Expectancy ($) | 33.33 |",
                 "| Expectancy (R) | 0.67 |", "| stories | 0 |", "| reject: AI_NEUTRAL | 1 |",
                 "| earnings | 1 | 100.0% | 2.00 | 100.00 | n/a |", "| 0.9+ | 1 |", "| 0.8-0.9 | 2 |",
                 "| 2-3 | 2 |", "| <2 | 1 |", "| 11:00 | 3 |", "| 10+B | 1 |", "| n/a | 2 |"]:
        assert line in t, line
    assert "not enough data (n=3 production vs n=1 neutral_news)" in t
    assert "not enough data (n=3 first pullback vs n=0 second_pullback)" in t
    assert "Entry latency: not enough data (n=3)" in t
    assert "Profit concentration: not enough data (n=3)" in t
    assert "999" not in t  # other month never leaks in
    h = (tmp_path / V / "2026-09.html").read_text(encoding="utf-8")
    assert "<table>" in h and "<script" not in h


def test_report_conclusion_with_enough_data(tmp_db, tmp_path):
    for i in range(45):
        add_trade(tmp_db, "production", 0, z(2 + i % 20, f"15:{i:02d}"), 20, 1.0, entry_latency_ms=100 + 40 * i,
                  catalyst="earnings" if i < 36 else "fda")
        if i < 25:
            add_trade(tmp_db, "neutral_news", 1, z(2 + i % 20, f"16:{i:02d}"), -5, -0.25)
    t = build_monthly_report(tmp_db, "2026-09", V, tmp_path).read_text(encoding="utf-8")
    assert "production avg R +1.00 (win 100%, n=45) vs neutral_news avg R -0.25 (win 0%, n=25). production is higher" in t
    assert "Entry latency (split at median" in t and "fast half avg R +1.00" in t
    assert "earnings 80% (720.00), fda 20% (180.00)" in t
    assert "RVOL 1.5-2.0 (shadow rvol_1_5): not enough data (n=45 RVOL>=2 vs n=0 rvol_1_5)" in t


def test_other_version_excluded(tmp_db, tmp_path):
    add_trade(tmp_db, "production", 0, z(3, "15:00"), 100, 2, strategy_version="v0")
    t = build_monthly_report(tmp_db, "2026-09", V, tmp_path).read_text(encoding="utf-8")
    assert "| Trades | 0 |" in t


def test_month_bounds_et():
    assert month_bounds("2026-09") == ("2026-09-01T04:00:00.000Z", "2026-10-01T04:00:00.000Z")
    assert month_bounds("2026-12")[1] == "2027-01-01T05:00:00.000Z"


def test_maybe_generate_monthly_once(tmp_db, tmp_path):
    clock = ReplayClock(datetime(2026, 9, 30, 15, 0, tzinfo=UTC))
    assert maybe_generate_monthly(tmp_db, clock, V, tmp_path) is None  # no August data: nothing written, month recorded
    assert tmp_db.one("SELECT value FROM kv WHERE key='last_report_month'")["value"] == "2026-09"
    seed_small(tmp_db)
    assert maybe_generate_monthly(tmp_db, clock, V, tmp_path) is None  # same month: no-op
    clock.set(datetime(2026, 10, 1, 12, 0, tzinfo=UTC))
    p = maybe_generate_monthly(tmp_db, clock, V, tmp_path)
    assert p == tmp_path / V / "2026-09.md" and p.exists()
    p.unlink()
    assert maybe_generate_monthly(tmp_db, clock, V, tmp_path) is None and not p.exists()  # once only
    assert tmp_db.one("SELECT value FROM kv WHERE key='last_report_month'")["value"] == "2026-10"


def test_monthly_has_data_counts_production_trades_only(tmp_db, tmp_path):
    clock = ReplayClock(datetime(2026, 10, 1, 12, 0, tzinfo=UTC))
    add_trade(tmp_db, "neutral_news", 1, z(3, "15:00"), 10, 0.2)        # shadow-only September, no snapshots
    assert maybe_generate_monthly(tmp_db, clock, V, tmp_path) is None and not (tmp_path / V).exists()
    tmp_db.execute("DELETE FROM kv")
    add_trade(tmp_db, "production", 0, z(4, "15:00"), 10, 0.2)
    assert maybe_generate_monthly(tmp_db, clock, V, tmp_path) is not None
