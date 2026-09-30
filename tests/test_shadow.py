from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from newswave.clock import ReplayClock, utc_iso
from newswave.config import StrategyParams
from newswave.models import Bar, Classification, Direction, EntrySignal, RejectReason, Side, Trade
from newswave.news.intake import Candidate
from newswave.strategy import shadow as shadow_mod
from newswave.strategy.shadow import SHADOW_VARIANTS, ShadowBook, spawn_companions, spawn_standalone
from newswave.timeline import Timeline

T = datetime(2026, 1, 2, 15, 21, tzinfo=UTC)   # 10:21 ET
P = StrategyParams()


def cand(direction, conf, material=True, reject=RejectReason.AI_NEUTRAL, error=None, **kw):
    cl = Classification(Direction(direction), conf, material, "cat", "r", "m", error)
    return Candidate(1, "a1", "NVDA", "h", T, T, 1.0, cl, kw.get("gate", reject), reject)


# ------------------------------------------------------------------ which candidates spawn which variant
@pytest.mark.parametrize("c,params,expect", [
    (cand("NEUTRAL", 0.3, material=False), P, [("neutral_news", Side.LONG)]),
    (cand("NEUTRAL", 0.95, material=True), P, [("neutral_news", Side.LONG)]),
    (cand("BEARISH", 0.9, reject=RejectReason.AI_BEARISH), P, [("bearish_short", Side.SHORT)]),
    (cand("BEARISH", 0.75, reject=RejectReason.AI_BEARISH), P, [("bearish_short", Side.SHORT)]),
    (cand("BEARISH", 0.74, reject=RejectReason.AI_LOW_CONFIDENCE), P, []),
    (cand("BEARISH", 0.9, material=False, reject=RejectReason.AI_NOT_MATERIAL), P, []),
    (cand("BEARISH", 0.9, reject=RejectReason.AI_BEARISH), StrategyParams(allow_shorts=True), []),
    (cand("BULLISH", 0.6, reject=RejectReason.AI_LOW_CONFIDENCE), P, [("low_confidence", Side.LONG)]),
    (cand("BULLISH", 0.5, reject=RejectReason.AI_LOW_CONFIDENCE), P, [("low_confidence", Side.LONG)]),
    (cand("BULLISH", 0.49, reject=RejectReason.AI_LOW_CONFIDENCE), P, []),
    (cand("BULLISH", 0.6, material=False, reject=RejectReason.AI_NOT_MATERIAL), P, []),
    (cand("BULLISH", 0.92, reject=None, gate=Side.LONG), P, []),                      # production handles it
    (cand("NEUTRAL", 0.5, reject=RejectReason.MARKET_CLOSED), P, []),                 # pre-AI reject
    (cand("NEUTRAL", 0.5, reject=RejectReason.NEWS_TOO_OLD), P, []),
    (cand("NEUTRAL", 0.0, reject=RejectReason.AI_ERROR, error="boom"), P, []),
])
def test_spawn_standalone(c, params, expect):
    assert [(v.name, s) for v, s in spawn_standalone(c, params)] == expect


def test_spawn_companions_and_variant_table():
    assert [(v.name, s) for v, s in spawn_companions(Side.SHORT, P)] == [
        ("rvol_1_5", Side.SHORT), ("second_pullback", Side.SHORT)]
    assert [v.name for v, _ in spawn_companions(Side.LONG, StrategyParams(rvol_min=1.5))] == ["second_pullback"]
    assert set(SHADOW_VARIANTS) == {"neutral_news", "bearish_short", "low_confidence", "rvol_1_5",
                                    "second_pullback"}
    assert SHADOW_VARIANTS["rvol_1_5"].shadow_rvol and SHADOW_VARIANTS["second_pullback"].pullback_number == 2


def test_shadow_module_cannot_reach_execution():
    src = Path(shadow_mod.__file__).read_text(encoding="utf-8")
    assert "newswave.execution" not in src and "on_entry_signal" not in src and "RiskEngine" not in src


# ------------------------------------------------------------------ the hypothetical book
@pytest.fixture
def env(tmp_db):
    clock = ReplayClock(T)
    closed: list[int] = []
    book = ShadowBook(tmp_db, clock, Timeline(tmp_db, clock, "v"), P, "v", on_close=closed.append)
    return tmp_db, clock, book, closed


def setup_row(db, variant="rvol_1_5", side="LONG"):
    return db.insert("setups", {
        "article_id": "a1", "symbol": "NVDA", "variant": variant, "is_shadow": 1, "side": side,
        "stage": "ENTRY_SIGNAL", "max_stage": "ENTRY_SIGNAL", "catalyst": "Guidance raise",
        "ai_confidence": 0.8, "rvol": 1.7, "impulse_pct": 2.0, "atr": 1.0, "news_latency_s": 1.5,
        "strategy_version": "v", "created_at": "x", "updated_at": "x"})


def sig(sid, side=Side.LONG, atr=1.0, variant="rvol_1_5", lo=100.6, hi=101.5):
    return EntrySignal(sid, "NVDA", side, 101.55, lo, hi, atr, utc_iso(T), variant)


def tr(px, m=0):
    return Trade("NVDA", utc_iso(T + timedelta(minutes=m)), px, 100)


def test_open_partial_trail_close_writes_trade_row(env):
    db, clock, book, closed = env
    sid = setup_row(db)
    pid = book.open(sig(sid), 101.6)
    pos = db.one("SELECT * FROM positions WHERE id=?", (pid,))
    # stop = min(100.6, 101.6 - 0.75) = 100.6; rps 1.0; qty = min(60 by risk, 20 by 35% size)
    assert (pos["is_shadow"], pos["variant"], pos["qty_initial"], pos["stop_price"], pos["status"]) == (1, "rvol_1_5", 20, 100.6, "OPEN")
    assert db.one("SELECT stage FROM setups WHERE id=?", (sid,))["stage"] == "IN_POSITION"
    book.on_trade(tr(102.0, 1))
    assert db.one("SELECT qty_open FROM positions WHERE id=?", (pid,))["qty_open"] == 20
    book.on_trade(tr(102.6, 2))                                   # +1R: half off
    assert db.one("SELECT qty_open, partial_taken FROM positions WHERE id=?", (pid,)) == {"qty_open": 10, "partial_taken": 1}
    clock.advance(minutes=5)
    book.on_bar(Bar("NVDA", utc_iso(T), 102, 104.0, 102, 103.5, 500, 5), 1.0)      # trail = 104 - 2 = 102
    assert closed == [] and db.one("SELECT trail_price FROM positions")["trail_price"] == pytest.approx(102.0)
    book.on_bar(Bar("NVDA", utc_iso(T), 103, 103, 101.5, 101.9, 500, 5), 1.0)      # close 101.9 < trail
    t = db.one("SELECT * FROM trades")
    assert (t["is_shadow"], t["variant"], t["exit_reason"], t["qty"], t["symbol"], t["side"]) == (1, "rvol_1_5", "TRAIL", 20, "NVDA", "LONG")
    assert t["pnl"] == pytest.approx(10 * 1.0 + 10 * 0.3) and t["r_multiple"] == pytest.approx(13 / 20)
    assert (t["catalyst"], t["ai_confidence"], t["rvol"], t["impulse_pct"], t["atr"], t["news_latency_s"]) == (
        "Guidance raise", 0.8, 1.7, 2.0, 1.0, 1.5)
    assert t["mfe_r"] == pytest.approx(2.4) and t["mae_r"] == pytest.approx(0.1) and t["avg_exit_price"] == pytest.approx((10 * 102.6 + 10 * 101.9) / 20)
    s = db.one("SELECT stage, max_stage, closed_at FROM setups WHERE id=?", (sid,))
    assert s["stage"] == "CLOSED" and s["max_stage"] == "CLOSED" and s["closed_at"]
    assert db.one("SELECT status, qty_open FROM positions")["status"] == "CLOSED" and closed == [sid]
    assert book.open_setup_ids() == set()


def test_stop_out(env):
    db, _, book, closed = env
    sid = setup_row(db)
    book.open(sig(sid), 101.6)
    book.on_trade(tr(100.5, 1))
    t = db.one("SELECT * FROM trades")
    assert t["exit_reason"] == "STOP" and t["pnl"] == pytest.approx(-1.1 * 20) and t["r_multiple"] == pytest.approx(-1.1)
    assert closed == [sid]


def test_short_mirror(env):
    db, _, book, _ = env
    sid = setup_row(db, "bearish_short", "SHORT")
    # short: stop = max(pullback_high 99.4, 98.4 + 0.75) = 99.4 ; rps 1.0
    book.open(sig(sid, Side.SHORT, variant="bearish_short", lo=98.5, hi=99.4), 98.4)
    book.on_trade(tr(99.5, 1))
    t = db.one("SELECT * FROM trades")
    assert (t["side"], t["exit_reason"]) == ("SHORT", "STOP") and t["pnl"] == pytest.approx(-1.1 * t["qty"])


def test_eod_clock_exit_at_last_price(env):
    db, clock, book, closed = env
    sid = setup_row(db)
    book.open(sig(sid), 101.6)
    book.on_trade(tr(102.2, 1))                                   # mfe 0.6R: the 60-minute time stop stays quiet
    book.on_trade(tr(101.9, 2))
    book.on_clock(datetime(2026, 1, 2, 20, 54, tzinfo=UTC))
    assert closed == []
    book.on_clock(datetime(2026, 1, 2, 20, 55, tzinfo=UTC))       # 15:55 ET
    t = db.one("SELECT * FROM trades")
    assert t["exit_reason"] == "EOD" and t["avg_exit_price"] == pytest.approx(101.9)


def test_size_zero_rejects_row_without_position(env):
    db, _, book, closed = env
    sid = setup_row(db)
    assert book.open(sig(sid, atr=200.0), 101.6) is None          # rps 150 -> floor(60/150) = 0
    s = db.one("SELECT stage, reject_reason, max_stage FROM setups WHERE id=?", (sid,))
    assert (s["stage"], s["reject_reason"], s["max_stage"]) == ("REJECTED", "SIZE_ZERO", "ENTRY_SIGNAL")
    assert db.query("SELECT * FROM positions") == [] and closed == [sid]


def test_opposite_news_closes_shadow_at_last_price(env):
    db, clock, book, _ = env
    sid = setup_row(db)
    book.open(sig(sid), 101.6)
    book.on_trade(tr(101.8, 1))
    book.opposite_news("NVDA", Side.LONG, clock.now())            # same side: nothing
    assert book.open_setup_ids() == {sid}
    book.opposite_news("NVDA", Side.SHORT, clock.now())
    t = db.one("SELECT * FROM trades")
    assert t["exit_reason"] == "OPPOSITE_NEWS" and t["avg_exit_price"] == pytest.approx(101.8)


def test_open_twice_is_ignored(env):
    db, _, book, _ = env
    sid = setup_row(db)
    assert book.open(sig(sid), 101.6) is not None and book.open(sig(sid), 101.7) is None
    assert len(db.query("SELECT * FROM positions")) == 1


# ------------------------------------------------------------------ review fixes: versioned shadow numbers
def test_low_conf_floor_and_shadow_rvol_min_come_from_params():
    c = cand("BULLISH", 0.6, reject=RejectReason.AI_LOW_CONFIDENCE)
    assert [v.name for v, _ in spawn_standalone(c, StrategyParams(low_conf_floor=0.7))] == []
    assert [v.name for v, _ in spawn_standalone(c, StrategyParams(low_conf_floor=0.55))] == ["low_confidence"]
    assert [v.name for v, _ in spawn_companions(Side.LONG, StrategyParams(shadow_rvol_min=2.0))] == ["second_pullback"]
    assert shadow_mod.variant_rvol_min(SHADOW_VARIANTS["rvol_1_5"], StrategyParams(shadow_rvol_min=1.7)) == 1.7
    assert shadow_mod.variant_rvol_min(SHADOW_VARIANTS["second_pullback"], P) is None
