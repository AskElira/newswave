"""Replay: a fixture streamed through the SAME build_app engine with a ReplayClock and SimBroker."""
from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime

import pytest

from newswave.__main__ import main
from newswave.clock import ReplayClock
from newswave.config import Settings
from newswave.db import Database
from newswave.models import Bar, Direction, NewsEvent
from newswave.replay import (CachedClassifier, FixtureClassifier, FixtureHistorical, build_events, derive_prints,
                             load_fixture, make_classifier, replay_fixture)
from test_daemon_helpers import FIXTURE_JSON, mf


def run(fx, tmp_path, name="r", **kw):
    s = Settings(data_dir=tmp_path / "data", strategy_version="v_replay")
    out = asyncio.run(replay_fixture(fx, s, out_dir=tmp_path / name, **kw))
    return out, Database(tmp_path / name / "newswave.db")


def dump(db, table):
    return json.dumps(db.query(f"SELECT * FROM {table} ORDER BY 1"), sort_keys=True)


def test_committed_fixture_matches_the_generator():
    assert load_fixture(FIXTURE_JSON) == json.loads(json.dumps(mf.story_fixture()))
    fx = load_fixture(FIXTURE_JSON)
    sessions = {b["t"][:10] for b in fx["history_5m"]["NVDA"]}
    assert len(sessions) >= 20 and max(sessions) < fx["session_date"]


def test_replay_produces_the_expected_trade(tmp_path):
    out, db = run(load_fixture(FIXTURE_JSON), tmp_path)
    assert out["trades"] == 1 and out["pnl"] == pytest.approx(5.0)
    t = db.query("SELECT * FROM trades WHERE is_shadow=0")
    assert len(t) == 1
    t = t[0]
    assert (t["symbol"], t["side"], t["qty"], t["exit_reason"]) == ("NVDA", "LONG", 20, "TRAIL")
    assert t["entry_price"] == pytest.approx(101.6) and t["avg_exit_price"] == pytest.approx(101.85)
    assert t["entry_at"] == "2026-01-02T15:22:40.000Z" and t["exit_at"] == "2026-01-02T15:40:00.000Z"
    assert t["rvol"] == pytest.approx(2.5) and t["impulse_pct"] == pytest.approx(2.0)
    assert t["r_multiple"] == pytest.approx(0.25) and t["mfe_r"] == pytest.approx(2.9)
    s = db.one("SELECT * FROM setups WHERE variant='production'")
    assert s["max_stage"] == "CLOSED" and s["article_id"] == "a-nvda-1"
    assert db.one("SELECT COUNT(*) c FROM equity_snapshots")["c"] >= 3
    assert not db.query("SELECT * FROM system_events WHERE level IN ('ERROR','CRITICAL')")
    assert db.one("SELECT COUNT(*) c FROM ai_classifications WHERE model='fixture'")["c"] == 1
    # its own database: the live data dir was never created
    assert not (tmp_path / "data" / "newswave.db").exists()
    db.close()


def test_replay_is_deterministic_byte_for_byte(tmp_path):
    fx = load_fixture(FIXTURE_JSON)
    _, a = run(fx, tmp_path, "a")
    _, b = run(fx, tmp_path, "b")
    for table in ("trades", "orders", "fills", "positions", "setups", "timeline", "equity_snapshots",
                  "market_events", "ai_classifications", "daily_stats"):
        assert dump(a, table) == dump(b, table), table
    assert dump(a, "trades") != "[]"
    a.close()
    b.close()


def test_replaying_over_an_existing_replay_db_starts_fresh(tmp_path):
    fx = load_fixture(FIXTURE_JSON)
    run(fx, tmp_path, "same")[1].close()
    _, db = run(fx, tmp_path, "same")
    assert db.one("SELECT COUNT(*) c FROM trades WHERE is_shadow=0")["c"] == 1
    db.close()


def test_neutral_news_replays_as_shadow_only(tmp_path):
    fx = load_fixture(FIXTURE_JSON)
    fx["classifications"] = {"a-nvda-1:NVDA": {"direction": "NEUTRAL", "confidence": 0.6, "material": False,
                                               "catalyst": "routine", "reason": "routine"}}
    out, db = run(fx, tmp_path)
    assert out["trades"] == 0 and db.query("SELECT * FROM orders") == []
    sh = db.query("SELECT * FROM trades WHERE is_shadow=1")
    assert sh and {t["variant"] for t in sh} == {"neutral_news"}
    assert db.one("SELECT reject_reason FROM setups WHERE variant='production'")["reject_reason"] == "AI_NEUTRAL"
    db.close()


def test_uncanned_article_is_ai_error_no_trade(tmp_path):
    fx = load_fixture(FIXTURE_JSON)
    fx["classifications"] = {}
    out, db = run(fx, tmp_path)
    assert out["trades"] == 0
    assert db.one("SELECT reject_reason FROM setups WHERE variant='production'")["reject_reason"] == "AI_ERROR"
    db.close()


# ------------------------------------------------------------------ pieces
def test_trade_prints_follow_bar_direction():
    up = Bar("X", "2026-01-02T15:00:00.000Z", 10.0, 12.0, 9.0, 11.0, 400, 1)
    dn = Bar("X", "2026-01-02T15:00:00.000Z", 11.0, 12.0, 9.0, 10.0, 400, 1)
    assert [t.price for t in derive_prints(up)] == [10.0, 9.0, 12.0, 11.0]      # open, low, high, close
    assert [t.price for t in derive_prints(dn)] == [11.0, 12.0, 9.0, 10.0]      # open, high, low, close
    assert [t.ts[14:19] for t in derive_prints(up)] == ["00:01", "00:20", "00:40", "00:58"]
    ev = build_events({"bars_1m": {"X": [{"t": "2026-01-02T15:00:00Z", "o": 10, "h": 12, "l": 9, "c": 11, "v": 400}]},
                       "news": [{"id": "n", "created_at": "2026-01-02T15:00:00Z", "symbols": ["X"]}]})
    assert [e[2] for e in ev] == ["news", "trade", "trade", "trade", "trade", "bar"]
    assert ev[-1][0] == datetime(2026, 1, 2, 15, 1, tzinfo=UTC)                  # the bar closes the minute


def test_fixture_historical_has_no_lookahead():
    fx = mf.story_fixture()
    clock = ReplayClock(mf.at(10, 3).astimezone(UTC))
    h = FixtureHistorical(fx, clock)
    assert [b.start[11:16] for b in h.bars_1m("NVDA", mf.at(9, 0).astimezone(UTC))] == ["15:00", "15:01", "15:02"]
    assert h.latest_trade("NVDA").price == pytest.approx(100.6)                  # last print at <= 10:02:58
    today = [b for b in h.bars_5m("NVDA", mf.at(9, 0).astimezone(UTC)) if b.start.startswith("2026-01-02")]
    assert today == []                                                           # bucket 10:00 not complete yet
    clock.set(mf.at(10, 5, 1).astimezone(UTC))
    assert [b.start[11:16] for b in h.bars_5m("NVDA", mf.at(9, 0).astimezone(UTC)) if b.start.startswith("2026-01-02")] == ["15:00"]
    bars, feed = h.daily_bars("NVDA", 20)
    assert len(bars) == 20 and feed == "iex"


async def test_classifiers_fixture_and_cached(tmp_path):
    ev = NewsEvent("a1", "", "", "", "h", "", "", ("NVDA",), "s", "")
    fc = FixtureClassifier({"a1:NVDA": {"direction": "BULLISH", "confidence": 0.9, "material": True}})
    assert (await fc.classify(ev, "NVDA")).direction == Direction.BULLISH
    c = await fc.classify(ev, "AAA")
    assert c.error and c.direction == Direction.NEUTRAL
    live = Database(tmp_path / "live.db")
    live.init_schema()
    live.insert("news_events", {"article_id": "a1", "strategy_version": "v"})
    live.insert("ai_classifications", {"article_id": "a1", "symbol": "NVDA", "model": "m", "direction": "BULLISH",
                                       "confidence": 0.88, "material": 1, "catalyst": "x", "reason": "r", "error": None,
                                       "created_at": "t", "strategy_version": "v"})
    live.close()
    cc = CachedClassifier(tmp_path / "live.db")
    got = await cc.classify(ev, "NVDA")
    assert (got.direction, got.confidence, got.material, got.error) == (Direction.BULLISH, 0.88, True, None)
    miss = await cc.classify(ev, "AAA")
    assert miss.error and "cached" in miss.error and miss.direction == Direction.NEUTRAL
    assert (await CachedClassifier(tmp_path / "absent.db").classify(ev, "NVDA")).error


def test_make_classifier_defaults_to_cached_and_cli_is_opt_in(tmp_path):
    s = Settings(data_dir=tmp_path)
    assert isinstance(make_classifier("cached", s, {}, None), CachedClassifier)
    assert isinstance(make_classifier("fixture", s, {"classifications": {}}, None), FixtureClassifier)
    assert type(make_classifier("cli", s, {}, None)).__name__ == "_CliForReplay"


# ------------------------------------------------------------------ CLI
ENV = {"TRADING_MODE": "PAPER", "ALPACA_PAPER": "true"}


def test_cli_replay_fixture(tmp_path, capsys):
    env = {**ENV, "DATA_DIR": str(tmp_path / "data")}
    assert main(["replay", "--fixture", str(FIXTURE_JSON), "--out", str(tmp_path / "out")], env) == 0
    out = capsys.readouterr().out
    assert "trades: 1" in out and "pnl: 5.0" in out
    assert (tmp_path / "out" / "newswave.db").exists() and not (tmp_path / "data" / "newswave.db").exists()


def test_cli_replay_argument_errors(tmp_path, capsys):
    env = {**ENV, "DATA_DIR": str(tmp_path / "data")}
    assert main(["replay"], env) == 1
    assert "--fixture" in capsys.readouterr().err
    assert main(["replay", "--date", "2026-01-02", "--symbols", "NVDA"], env) == 1      # historical needs keys
    assert "ALPACA_API_KEY" in capsys.readouterr().err
