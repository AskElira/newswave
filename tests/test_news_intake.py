from __future__ import annotations

from datetime import UTC, datetime

import pytest

from newswave.clock import ReplayClock, parse_iso, utc_iso
from newswave.config import StrategyParams
from newswave.models import Classification, Direction, NewsEvent, RejectReason, Side
from newswave.news.intake import NewsIntake
from newswave.timeline import Timeline

M = "claude-sonnet-5-5"
OPEN = datetime(2026, 1, 2, 15, 0, 2, tzinfo=UTC)  # Fri 10:00:02 ET


class FakeClf:
    def __init__(self, c=None, budget=10):
        self.c = c or Classification(Direction.BULLISH, 0.92, True, "Guidance raise", "raised guidance", M)
        self.budget, self.calls = budget, []

    def budget_left(self):
        return self.budget

    async def classify(self, event, symbol):
        self.calls.append(symbol)
        if isinstance(self.c, Exception):
            raise self.c
        return self.c


def mk(tmp_db, clf=None, now=OPEN, asset=lambda s: None, **pk):
    clock = ReplayClock(now)
    p = StrategyParams(**pk)
    clf = clf or FakeClf()
    return NewsIntake(tmp_db, clock, Timeline(tmp_db, clock, "v"), p, "v", asset, clf), clock, clf


def story(i="1", symbols=("NVDA",), lat=1.2, now=OPEN, **kw):
    return NewsEvent(article_id=i, received_at=utc_iso(now), created_at=utc_iso(now.replace(microsecond=0) - __import__("datetime").timedelta(seconds=lat)),
                     updated_at=utc_iso(now), headline="NVDA raises guidance", summary="s", content="c",
                     symbols=symbols, source="benzinga", url="", **kw)


def setups(db):
    return db.query("SELECT * FROM setups ORDER BY id")


async def test_happy_path_long_candidate(tmp_db):
    it, _, clf = mk(tmp_db)
    [c] = await it.handle(story())
    assert c.gate_result == Side.LONG and c.reject_reason is None and c.classification.catalyst == "Guidance raise"
    assert c.news_latency_s == pytest.approx(1.2, abs=0.6) and c.symbol == "NVDA"
    [s] = setups(tmp_db)
    assert (s["stage"], s["max_stage"], s["side"], s["variant"], s["is_shadow"]) == ("CLASSIFIED", "CLASSIFIED", "LONG", "production", 0)
    assert s["ai_direction"] == "BULLISH" and s["ai_material"] == 1 and s["catalyst"] == "Guidance raise"
    assert s["id"] == c.setup_id and tmp_db.one("SELECT latency_s FROM news_events")["latency_s"] > 0
    msgs = [r["message"] for r in tmp_db.query("SELECT message FROM timeline")]
    assert any("NVDA news received (latency" in m for m in msgs) and any(m.startswith("Claude: BULLISH 0.92 - raised guidance") for m in msgs)


async def test_duplicate_article_id_no_new_rows_and_no_classify(tmp_db):
    it, _, clf = mk(tmp_db)
    await it.handle(story())
    assert await it.handle(story()) == []
    assert tmp_db.one("SELECT COUNT(*) n FROM news_events")["n"] == 1 and len(setups(tmp_db)) == 1 and len(clf.calls) == 1
    assert tmp_db.one("SELECT 1 FROM system_events WHERE event='DUPLICATE_NEWS'")


async def test_too_old_rejected_not_classified(tmp_db):
    it, _, clf = mk(tmp_db)
    assert await it.handle(story(lat=500)) == []
    [s] = setups(tmp_db)
    assert s["reject_reason"] == "NEWS_TOO_OLD" and s["stage"] == "REJECTED" and s["max_stage"] == "NEWS" and not clf.calls


async def test_no_symbols_and_too_many(tmp_db):
    it, _, clf = mk(tmp_db)
    assert await it.handle(story("a", symbols=())) == []
    assert setups(tmp_db) == [] and tmp_db.one("SELECT 1 FROM system_events WHERE event='NO_SYMBOLS'")
    assert await it.handle(story("b", symbols=("A", "B", "C", "D"))) == []
    assert [s["reject_reason"] for s in setups(tmp_db)] == ["TOO_MANY_SYMBOLS"] * 4 and not clf.calls


@pytest.mark.parametrize("now,reason", [
    (datetime(2026, 1, 2, 13, 0, 2, tzinfo=UTC), "MARKET_CLOSED"),   # 08:00 ET
    (datetime(2026, 1, 3, 15, 0, 2, tzinfo=UTC), "MARKET_CLOSED"),   # Saturday
    (datetime(2026, 1, 2, 20, 45, 2, tzinfo=UTC), "AFTER_CUTOFF"),   # 15:45 ET
])
async def test_session_windows(tmp_db, now, reason):
    it, _, clf = mk(tmp_db, now=now)
    assert await it.handle(story(now=now)) == []
    assert setups(tmp_db)[0]["reject_reason"] == reason and not clf.calls


async def test_asset_check_reject(tmp_db):
    it, _, clf = mk(tmp_db, asset=lambda s: RejectReason.OTC if s == "BAD" else None)
    out = await it.handle(story(symbols=("BAD", "NVDA")))
    assert [c.symbol for c in out] == ["NVDA"] and clf.calls == ["NVDA"]
    assert {s["symbol"]: s["reject_reason"] for s in setups(tmp_db)} == {"BAD": "OTC", "NVDA": None}


async def test_classify_outside_window_still_classifies_but_keeps_reason(tmp_db):
    now = datetime(2026, 1, 2, 13, 0, 2, tzinfo=UTC)
    it, _, clf = mk(tmp_db, now=now, classify_outside_window=True)
    [c] = await it.handle(story(now=now))
    assert clf.calls == ["NVDA"] and c.reject_reason == RejectReason.MARKET_CLOSED and c.gate_result == Side.LONG
    s = setups(tmp_db)[0]
    assert s["reject_reason"] == "MARKET_CLOSED" and s["ai_direction"] == "BULLISH" and s["side"] is None


@pytest.mark.parametrize("c,reason", [
    (Classification(Direction.NEUTRAL, 0.9, True, "c", "r", M), "AI_NEUTRAL"),
    (Classification(Direction.BEARISH, 0.9, True, "c", "r", M), "AI_BEARISH"),
    (Classification(Direction.BULLISH, 0.9, False, "c", "r", M), "AI_NOT_MATERIAL"),
    (Classification(Direction.BULLISH, 0.5, True, "c", "r", M), "AI_LOW_CONFIDENCE"),
    (Classification(Direction.NEUTRAL, 0, False, "", "", M, error="x"), "AI_ERROR"),
])
async def test_gate_rejects(tmp_db, c, reason):
    it, _, _ = mk(tmp_db, FakeClf(c))
    [cand] = await it.handle(story())
    assert cand.reject_reason == reason and cand.gate_result == reason and cand.classification is c
    s = setups(tmp_db)[0]
    assert s["reject_reason"] == reason and s["max_stage"] == "CLASSIFIED" and s["side"] is None


async def test_classifier_crash_is_ai_error(tmp_db):
    it, _, _ = mk(tmp_db, FakeClf(RuntimeError("boom")))
    [c] = await it.handle(story())
    assert c.reject_reason == RejectReason.AI_ERROR


async def test_budget_exceeded(tmp_db):
    it, _, clf = mk(tmp_db, FakeClf(budget=0))
    assert await it.handle(story()) == []
    assert setups(tmp_db)[0]["reject_reason"] == "CLASSIFIER_BUDGET" and not clf.calls


async def test_catalyst_cooldown_same_symbol_same_side(tmp_db):
    it, clock, _ = mk(tmp_db)
    [a] = await it.handle(story("1"))
    clock.advance(minutes=10)
    [b] = await it.handle(story("2", now=clock.now()))
    assert a.reject_reason is None and b.reject_reason == RejectReason.CATALYST_COOLDOWN
    assert b.gate_result == Side.LONG
    clock.advance(minutes=50)  # 60 min after the first... cooldown counts from the passing setup
    [c] = await it.handle(story("3", now=clock.now()))
    assert c.reject_reason is None  # cooled-down row (b) did not extend the window


async def test_cooldown_ignores_other_symbol_and_rejected_gate(tmp_db):
    it, clock, _ = mk(tmp_db)
    await it.handle(story("1", symbols=("AAA",)))
    [b] = await it.handle(story("2", symbols=("BBB",)))
    assert b.reject_reason is None
    it2, clock2, _ = mk(tmp_db, FakeClf(Classification(Direction.NEUTRAL, 0.9, True, "c", "r", M)))
    await it2.handle(story("3", symbols=("CCC",)))
    it3, _, _ = mk(tmp_db)
    [d] = await it3.handle(story("4", symbols=("CCC",)))
    assert d.reject_reason is None  # earlier CCC setup never passed the gate
