"""StrategyEngine with fake market data, recording callbacks and a ReplayClock (offline)."""
from __future__ import annotations

from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest

from newswave.clock import ReplayClock, utc_iso
from newswave.config import StrategyParams
from newswave.market.subscriptions import SlotPriority, SubscriptionManager
from newswave.market.universe import AssetCache
from newswave.models import (Bar, Classification, Direction, RejectReason, Side, Stage, Trade)
from newswave.news.intake import Candidate
from newswave.strategy.engine import StrategyEngine, SymbolContext
from newswave.timeline import Timeline

NEWS = datetime(2026, 1, 2, 15, 2, 10, tzinfo=UTC)   # Fri 10:02:10 ET
T0 = datetime(2026, 1, 2, 15, 0, tzinfo=UTC)         # bucket 0 = 10:00 ET


def asset(sym, shortable=True):
    return {"symbol": sym, "asset_class": "us_equity", "exchange": "NASDAQ", "tradable": True,
            "status": "active", "shortable": shortable, "easy_to_borrow": shortable}


def make_ctx(ref=100.0):
    warm = [Bar("NVDA", utc_iso(datetime(2025, 12, 31, 14, 30, tzinfo=UTC) + timedelta(minutes=5 * i)),
                100.0, 100.5, 99.5, 100.0, 1000.0, 5) for i in range(20)]   # EMA9 = 100, ATR14 = 1
    slots = {}
    for i in range(78):
        mins = 9 * 60 + 30 + 5 * i
        slots[f"{mins // 60:02d}:{mins % 60:02d}"] = 1000.0
    return SymbolContext(ref, ref, slots, warm, 50e6, "sip")


class FakeMD:
    def __init__(self, result=None):
        self.result, self.calls = result or make_ctx(), []

    async def prepare(self, symbol, news_received_at):
        self.calls.append(symbol)
        return self.result


def make(db, params=None, cap=30, md=None, assets=("NVDA", "AAA"), raise_entry=False):
    clock = ReplayClock(NEWS + timedelta(seconds=2))
    p = params or StrategyParams()
    r = SimpleNamespace(db=db, clock=clock, signals=[], opp=[], bars=[], md=md or FakeMD())
    r.subs = SubscriptionManager(cap=cap, shadow_max=p.shadow_max_slots)
    ac = AssetCache()
    ac.load([asset(s) for s in assets])
    r.assets = ac

    async def on_entry(s):
        r.signals.append(s)
        if raise_entry:
            raise RuntimeError("boom")

    async def on_opp(sym, side):
        r.opp.append((sym, side))

    async def on_bar(sym, bar, ind):
        r.bars.append((sym, bar, ind))

    r.engine = StrategyEngine(db, clock, Timeline(db, clock, "v"), p, "v", r.subs, ac, r.md,
                              on_entry_signal=on_entry, on_opposite_news=on_opp, on_bar_5m=on_bar)
    return r


def cand(db, sym="NVDA", direction=Direction.BULLISH, conf=0.92, material=True, gate=None, reject=None,
         article="a1"):
    cl = Classification(direction, conf, material, "Guidance raise", "r", "m")
    if gate is None and reject is None:
        gate = Side.LONG
    sid = db.insert("setups", {
        "article_id": article, "symbol": sym, "variant": "production", "is_shadow": 0,
        "side": str(gate) if isinstance(gate, Side) else None,
        "stage": "REJECTED" if reject else "CLASSIFIED", "max_stage": "CLASSIFIED",
        "reject_reason": str(reject) if reject else None, "news_received_at": utc_iso(NEWS),
        "ai_direction": str(direction), "ai_confidence": conf, "ai_material": int(material),
        "catalyst": "Guidance raise", "strategy_version": "v", "created_at": utc_iso(NEWS),
        "updated_at": utc_iso(NEWS)})
    return Candidate(sid, article, sym, "h", NEWS, NEWS - timedelta(seconds=1), 1.0, cl, gate, reject)


def rows(db, **where):
    q = "SELECT * FROM setups" + (" WHERE " + " AND ".join(f"{k}=?" for k in where) if where else "")
    return db.query(q + " ORDER BY id", list(where.values()))


def by_variant(db):
    return {r["variant"]: r for r in rows(db)}


async def bar5(r, i, h, l, c, v, sym="NVDA", last=True):
    """One completed 5m bar delivered as five 1m bars (high in minute 0, low in minute 1)."""
    start = T0 + timedelta(minutes=5 * i)
    for k in range(5 if last else 4):
        hi, lo = (h if k == 0 else c), (l if k == 1 else c)
        ts = start + timedelta(minutes=k)
        r.clock.set(ts + timedelta(seconds=59))
        await r.engine.on_bar_1m(Bar(sym, utc_iso(ts), c, max(hi, c), min(lo, c), c, v / 5, 1))


def trade(px, minute, sym="NVDA"):
    return Trade(sym, utc_iso(T0 + timedelta(minutes=minute)), px, 100)


async def to_breakout(r, vols=(1500, 2500, 800, 700)):
    await bar5(r, 0, 101.0, 100.0, 100.8, vols[0])
    await bar5(r, 1, 102.0, 100.8, 101.8, vols[1])
    await bar5(r, 2, 101.5, 100.9, 101.0, vols[2])
    await bar5(r, 3, 101.3, 100.6, 100.9, vols[3])


# ---------------------------------------------------------------- production flow
async def test_full_production_flow_emits_exactly_one_signal(tmp_db):
    r = make(tmp_db)
    c = cand(tmp_db)
    await r.engine.on_candidate(c)
    v = by_variant(tmp_db)
    assert set(v) == {"production", "rvol_1_5", "second_pullback"}
    assert (v["production"]["is_shadow"], v["rvol_1_5"]["is_shadow"], v["second_pullback"]["is_shadow"]) == (0, 1, 1)
    assert {x["stage"] for x in v.values()} == {"WAITING_FOR_VOLUME"} and v["production"]["ref_price"] == 100.0
    assert all(x["article_id"] == "a1" and x["symbol"] == "NVDA" and x["side"] == "LONG" for x in v.values())
    assert r.subs.owners("NVDA") == {f"setup-{x['id']}" for x in v.values()}
    assert r.engine.active_symbols() == {"NVDA"} and r.md.calls == ["NVDA"]   # one prepare for all variants

    await to_breakout(r)
    assert rows(tmp_db, variant="production")[0]["stage"] == "WAITING_FOR_BREAKOUT"
    assert r.signals == []
    await r.engine.on_trade(trade(101.54, 21))
    assert r.signals == []
    await r.engine.on_trade(trade(101.6, 21))
    await r.engine.on_trade(trade(101.9, 22))                 # one signal per machine
    assert len(r.signals) == 1
    s = r.signals[0]
    assert (s.variant, s.side, s.symbol, s.setup_id) == ("production", Side.LONG, "NVDA", c.setup_id)
    assert s.trigger_price == pytest.approx(101.549, abs=0.01) and (s.pullback_high, s.pullback_low) == (101.5, 100.6)

    prod = rows(tmp_db, variant="production")[0]
    assert (prod["stage"], prod["max_stage"], prod["reject_reason"]) == ("ENTRY_SIGNAL", "ENTRY_SIGNAL", None)
    assert prod["signal_at"] == utc_iso(T0 + timedelta(minutes=21)) and prod["entry_trigger"] == pytest.approx(s.trigger_price)
    assert prod["rvol"] == pytest.approx(2.5) and prod["impulse_pct"] == pytest.approx(2.0)
    assert prod["pullback_bars"] == 2 and prod["pullback_high"] == 101.5 and prod["ema9_at_touch"] is not None
    # funnel rows of the shadows
    v = by_variant(tmp_db)
    assert (v["second_pullback"]["stage"], v["second_pullback"]["max_stage"]) == ("WAITING_FOR_PULLBACK", "WAITING_FOR_BREAKOUT")
    # rvol_1_5 saw rvol 2.5 >= 2.0: production would have traded it too -> no hypothetical trade
    assert (v["rvol_1_5"]["stage"], v["rvol_1_5"]["reject_reason"]) == ("REJECTED", "NOT_COUNTERFACTUAL")
    assert v["rvol_1_5"]["max_stage"] == "WAITING_FOR_BREAKOUT" and tmp_db.query("SELECT * FROM positions") == []
    # 5m bars: market_events + callback with indicators
    assert len(tmp_db.query("SELECT * FROM market_events WHERE kind='BAR_5M' AND symbol='NVDA'")) == 4
    assert len(r.bars) == 4 and all(i["atr"] and i["ema9"] for _, _, i in r.bars)
    # the watch-the-bot-think log
    msgs = [m["message"] for m in tmp_db.query("SELECT message FROM timeline WHERE setup_id=?", (c.setup_id,))]
    for needle in ("subscribed", "RVOL 2.5", "price impulse +2.0%", "waiting for 9 EMA", "9 EMA pullback confirmed",
                   "breakout triggered"):
        assert any(needle in m for m in msgs), needle
    # the production signal keeps its slot for execution, then auto-releases
    assert f"setup-{c.setup_id}" in r.subs.owners("NVDA")
    await r.engine.on_clock(r.clock.now() + timedelta(seconds=130))
    assert f"setup-{c.setup_id}" not in r.subs.owners("NVDA")


async def test_execution_can_release_owner_explicitly(tmp_db):
    r = make(tmp_db)
    c = cand(tmp_db)
    await r.engine.on_candidate(c)
    r.engine.release_symbol_owner(f"setup-{c.setup_id}")
    assert f"setup-{c.setup_id}" not in r.subs.owners("NVDA")
    assert all(x["variant"] != "production" for x in r.engine.stage_summary())


async def test_stage_summary_shape(tmp_db):
    r = make(tmp_db)
    await r.engine.on_candidate(cand(tmp_db))
    await bar5(r, 0, 101.0, 100.0, 100.8, 1500)
    await bar5(r, 1, 102.0, 100.8, 101.8, 2500)
    summ = {s["variant"]: s for s in r.engine.stage_summary()}
    assert set(summ) == {"production", "rvol_1_5", "second_pullback"}
    p = summ["production"]
    assert (p["symbol"], p["stage"], p["side"], p["is_shadow"]) == ("NVDA", "WAITING_FOR_PULLBACK", "LONG", False)
    assert p["rvol"] == pytest.approx(2.5) and p["impulse_pct"] == pytest.approx(2.0) and p["setup_id"]


async def test_entry_callback_error_does_not_kill_the_loop(tmp_db):
    r = make(tmp_db, raise_entry=True)
    await r.engine.on_candidate(cand(tmp_db))
    await to_breakout(r)
    await r.engine.on_trade(trade(101.6, 21))
    assert len(r.signals) == 1
    assert tmp_db.one("SELECT event FROM system_events WHERE event='on_entry_signal_failed'")


# ---------------------------------------------------------------- shadow can never reach execution
async def test_shadow_signal_never_reaches_on_entry_signal(tmp_db):
    r = make(tmp_db)
    # rvol 1.7x: production never qualifies (NO_VOLUME), rvol_1_5 does and is a true counterfactual
    c = cand(tmp_db)
    await r.engine.on_candidate(c)
    await to_breakout(r, vols=(1700, 1700, 1700, 1700))
    prod = rows(tmp_db, variant="production")[0]
    assert (prod["stage"], prod["reject_reason"], prod["max_stage"]) == ("REJECTED", "NO_VOLUME", "WAITING_FOR_VOLUME")
    assert by_variant(tmp_db)["second_pullback"]["reject_reason"] == "NO_VOLUME"
    assert by_variant(tmp_db)["rvol_1_5"]["stage"] == "WAITING_FOR_BREAKOUT"
    await r.engine.on_trade(trade(101.6, 21))
    assert r.signals == []                                   # the shadow signal went to the ShadowBook only
    pos = tmp_db.one("SELECT * FROM positions")
    assert (pos["is_shadow"], pos["variant"], pos["symbol"], pos["status"]) == (1, "rvol_1_5", "NVDA", "OPEN")
    sh = by_variant(tmp_db)["rvol_1_5"]
    assert sh["stage"] == "IN_POSITION" and sh["max_stage"] == "IN_POSITION" and sh["signal_at"]
    assert [s["stage"] for s in r.engine.stage_summary()] == ["IN_POSITION"]
    assert r.subs.owners("NVDA") == {f"setup-{sh['id']}"}     # slot held while the hypothetical position is open
    await r.engine.on_trade(trade(100.5, 23))                 # through the stop
    t = tmp_db.one("SELECT * FROM trades")
    assert (t["is_shadow"], t["variant"], t["exit_reason"]) == (1, "rvol_1_5", "STOP") and t["pnl"] < 0
    assert t["catalyst"] == "Guidance raise" and t["rvol"] == pytest.approx(1.7) and t["ai_confidence"] == 0.92
    sh = by_variant(tmp_db)["rvol_1_5"]
    assert sh["stage"] == "CLOSED" and sh["max_stage"] == "CLOSED" and sh["closed_at"]
    assert r.subs.owners("NVDA") == set() and r.signals == [] and r.engine.active_symbols() == set()


async def test_neutral_candidate_creates_only_a_shadow_row(tmp_db):
    r = make(tmp_db)
    c = cand(tmp_db, direction=Direction.NEUTRAL, conf=0.4, material=False, reject=RejectReason.AI_NEUTRAL,
             gate=RejectReason.AI_NEUTRAL)
    await r.engine.on_candidate(c)
    v = by_variant(tmp_db)
    assert set(v) == {"production", "neutral_news"}
    assert (v["production"]["stage"], v["production"]["reject_reason"]) == ("REJECTED", "AI_NEUTRAL")
    n = v["neutral_news"]
    assert (n["is_shadow"], n["side"], n["stage"], n["ai_direction"], n["article_id"]) == (1, "LONG", "WAITING_FOR_VOLUME", "NEUTRAL", "a1")
    assert [s["variant"] for s in r.engine.stage_summary()] == ["neutral_news"]
    assert r.subs.owners("NVDA") == {f"setup-{n['id']}"}
    await to_breakout(r)
    await r.engine.on_trade(trade(101.6, 21))
    assert r.signals == []
    assert tmp_db.one("SELECT variant, is_shadow FROM positions") == {"variant": "neutral_news", "is_shadow": 1}


async def test_bearish_and_low_confidence_shadows(tmp_db):
    r = make(tmp_db)
    await r.engine.on_candidate(cand(tmp_db, direction=Direction.BEARISH, conf=0.9, reject=RejectReason.AI_BEARISH,
                                     gate=RejectReason.AI_BEARISH, article="b"))
    await r.engine.on_candidate(cand(tmp_db, sym="AAA", direction=Direction.BULLISH, conf=0.6,
                                     reject=RejectReason.AI_LOW_CONFIDENCE, gate=RejectReason.AI_LOW_CONFIDENCE, article="c"))
    v = {(x["article_id"], x["variant"]): x for x in rows(tmp_db) if x["is_shadow"]}
    assert v[("b", "bearish_short")]["side"] == "SHORT" and v[("c", "low_confidence")]["side"] == "LONG"
    assert r.signals == [] and r.engine.active_symbols() == {"NVDA", "AAA"}


async def test_bearish_shadow_needs_shortable_asset(tmp_db):
    r = make(tmp_db)
    r.assets.load([asset("NVDA", shortable=False)])
    await r.engine.on_candidate(cand(tmp_db, direction=Direction.BEARISH, conf=0.9, reject=RejectReason.AI_BEARISH,
                                     gate=RejectReason.AI_BEARISH))
    sh = by_variant(tmp_db)["bearish_short"]
    assert (sh["stage"], sh["reject_reason"]) == ("REJECTED", "SHORT_UNAVAILABLE") and r.md.calls == []


async def test_pre_ai_rejects_spawn_nothing(tmp_db):
    r = make(tmp_db)
    await r.engine.on_candidate(cand(tmp_db, direction=Direction.NEUTRAL, reject=RejectReason.MARKET_CLOSED,
                                     gate=RejectReason.AI_NEUTRAL))
    assert len(rows(tmp_db)) == 1 and r.md.calls == [] and r.engine.active_symbols() == set()


# ---------------------------------------------------------------- contradictory news
async def test_contradictory_news_rejects_active_setup_and_notifies_execution(tmp_db):
    r = make(tmp_db)
    c = cand(tmp_db)
    await r.engine.on_candidate(c)
    r.opp.clear()   # every Side-gated candidate is announced to execution, which ignores same-side news
    await bar5(r, 0, 101.0, 100.0, 100.8, 1500)
    bear = cand(tmp_db, direction=Direction.BEARISH, conf=0.9, reject=RejectReason.AI_BEARISH,
                gate=RejectReason.AI_BEARISH, article="a2")
    await r.engine.on_candidate(bear)
    assert r.opp == [("NVDA", Side.SHORT)]
    v = {(x["article_id"], x["variant"]): x for x in rows(tmp_db)}
    for var in ("production", "rvol_1_5", "second_pullback"):
        x = v[("a1", var)]
        assert (x["stage"], x["reject_reason"]) == ("REJECTED", "CONTRADICTORY_NEWS"), var
    assert v[("a1", "production")]["max_stage"] == "WAITING_FOR_VOLUME"
    assert [s["variant"] for s in r.engine.stage_summary()] == ["bearish_short"]   # only the new research setup lives
    await r.engine.on_trade(trade(150.0, 21))
    assert r.signals == []


async def test_contradictory_with_shorts_enabled_arms_the_short(tmp_db):
    r = make(tmp_db, params=StrategyParams(allow_shorts=True))
    await r.engine.on_candidate(cand(tmp_db))
    r.opp.clear()
    short = cand(tmp_db, direction=Direction.BEARISH, conf=0.9, gate=Side.SHORT, article="a2")
    await r.engine.on_candidate(short)
    assert r.opp == [("NVDA", Side.SHORT)]
    live = r.engine.stage_summary()
    assert {(s["variant"], s["side"]) for s in live} == {("production", "SHORT"), ("rvol_1_5", "SHORT"), ("second_pullback", "SHORT")}
    assert all(s["setup_id"] in {short.setup_id} or s["is_shadow"] for s in live)


async def test_same_direction_news_is_not_contradictory(tmp_db):
    r = make(tmp_db)
    await r.engine.on_candidate(cand(tmp_db))
    r.opp.clear()
    await r.engine.on_candidate(cand(tmp_db, reject=RejectReason.CATALYST_COOLDOWN, gate=Side.LONG, article="a2"))
    assert r.opp == [("NVDA", Side.LONG)] and len(r.engine.stage_summary()) == 3


async def test_stale_or_pre_ai_rejected_opposite_news_is_ignored(tmp_db):
    r = make(tmp_db)
    await r.engine.on_candidate(cand(tmp_db))
    r.opp.clear()
    await r.engine.on_candidate(cand(tmp_db, direction=Direction.BEARISH, conf=0.9, gate=Side.SHORT,
                                     reject=RejectReason.NEWS_TOO_OLD, article="old"))
    assert r.opp == [] and len(r.engine.stage_summary()) == 3


# ---------------------------------------------------------------- slots
async def test_no_slot_when_full_of_higher_priority_owners(tmp_db):
    r = make(tmp_db, cap=3)                                   # SPY + QQQ + one free slot
    assert r.subs.request("AAA", "pos-1", SlotPriority.POSITION, 1.0, NEWS)
    c = cand(tmp_db)
    await r.engine.on_candidate(c)
    x = rows(tmp_db)[0]
    assert (x["stage"], x["reject_reason"], x["max_stage"]) == ("REJECTED", "NO_SLOT", "CLASSIFIED")
    assert r.md.calls == [] and r.engine.active_symbols() == set() and len(rows(tmp_db)) == 1
    assert r.subs.owners("NVDA") == set()


async def test_production_evicts_a_shadow_setup_and_rejects_it_no_slot(tmp_db):
    r = make(tmp_db, cap=3)
    await r.engine.on_candidate(cand(tmp_db, sym="AAA", direction=Direction.NEUTRAL, reject=RejectReason.AI_NEUTRAL,
                                     gate=RejectReason.AI_NEUTRAL, article="n"))
    assert r.engine.active_symbols() == {"AAA"}
    await r.engine.on_candidate(cand(tmp_db))
    assert r.engine.active_symbols() == {"NVDA"}
    aaa = [x for x in rows(tmp_db, symbol="AAA") if x["is_shadow"]][0]
    assert (aaa["stage"], aaa["reject_reason"], aaa["max_stage"]) == ("REJECTED", "NO_SLOT", "WAITING_FOR_VOLUME")
    assert rows(tmp_db, variant="production", symbol="NVDA")[0]["stage"] == "WAITING_FOR_VOLUME"


# ---------------------------------------------------------------- rejects before arming
@pytest.mark.parametrize("reason", [RejectReason.ILLIQUID, RejectReason.NO_MARKET_DATA, RejectReason.PRICE_OUT_OF_RANGE,
                                    RejectReason.HALTED])
async def test_prepare_reject_lands_on_setups_row_and_frees_slot(tmp_db, reason):
    r = make(tmp_db, md=FakeMD(reason))
    c = cand(tmp_db)
    await r.engine.on_candidate(c)
    x = rows(tmp_db)
    assert len(x) == 1 and (x[0]["stage"], x[0]["reject_reason"]) == ("REJECTED", str(reason))
    assert r.subs.owners("NVDA") == set() and r.engine.active_symbols() == set()


async def test_prepare_crash_is_no_market_data(tmp_db):
    class Boom:
        async def prepare(self, s, n):
            raise OSError("x")
    r = make(tmp_db, md=Boom())
    await r.engine.on_candidate(cand(tmp_db))
    assert rows(tmp_db)[0]["reject_reason"] == "NO_MARKET_DATA"


async def test_short_unavailable_when_not_enabled_or_not_shortable(tmp_db):
    r = make(tmp_db, params=StrategyParams(allow_shorts=True))
    r.assets.load([asset("NVDA", shortable=False)])
    await r.engine.on_candidate(cand(tmp_db, direction=Direction.BEARISH, gate=Side.SHORT))
    assert rows(tmp_db)[0]["reject_reason"] == "SHORT_UNAVAILABLE" and r.md.calls == []
    r = make(tmp_db, params=StrategyParams(allow_shorts=False))     # defensive: a SHORT gate with shorts off
    await r.engine.on_candidate(cand(tmp_db, direction=Direction.BEARISH, gate=Side.SHORT, article="z"))
    assert rows(tmp_db, article_id="z")[0]["reject_reason"] == "SHORT_UNAVAILABLE"


async def test_short_production_full_flow(tmp_db):
    r = make(tmp_db, params=StrategyParams(allow_shorts=True))
    ctx = make_ctx()
    r.md.result = ctx
    await r.engine.on_candidate(cand(tmp_db, direction=Direction.BEARISH, gate=Side.SHORT))
    # mirror of the LONG tape around ref 100
    m = lambda p: 200 - p
    await bar5(r, 0, m(100.0), m(101.0), m(100.8), 1500)
    await bar5(r, 1, m(100.8), m(102.0), m(101.8), 2500)
    await bar5(r, 2, m(100.9), m(101.5), m(101.0), 800)
    await bar5(r, 3, m(100.6), m(101.3), m(100.9), 700)
    await r.engine.on_trade(trade(m(101.6), 21))
    assert len(r.signals) == 1 and r.signals[0].side == Side.SHORT and r.signals[0].trigger_price < 98.5


# ---------------------------------------------------------------- status / clock / bars
async def test_halt_rejects_every_active_machine(tmp_db):
    r = make(tmp_db)
    await r.engine.on_candidate(cand(tmp_db))
    await r.engine.on_status({"symbol": "NVDA", "halted": False})
    assert len(r.engine.stage_summary()) == 3
    await r.engine.on_status({"symbol": "NVDA", "halted": True})
    assert {x["reject_reason"] for x in rows(tmp_db)} == {"HALTED"} and r.engine.active_symbols() == set()
    assert r.subs.owners("NVDA") == set()


async def test_setup_expired_via_clock_and_symbol_cleanup(tmp_db):
    r = make(tmp_db)
    await r.engine.on_candidate(cand(tmp_db))
    await r.engine.on_clock(NEWS + timedelta(minutes=44))
    assert len(r.engine.stage_summary()) == 3
    await r.engine.on_clock(NEWS + timedelta(minutes=45))
    assert {x["reject_reason"] for x in rows(tmp_db)} == {"SETUP_EXPIRED"}
    assert r.subs.owners("NVDA") == set() and r.engine.active_symbols() == set()
    await bar5(r, 0, 101.0, 100.0, 100.8, 1500)             # symbol no longer armed: ignored, no crash
    assert r.bars == []


async def test_after_cutoff_via_clock(tmp_db):
    r = make(tmp_db)
    await r.engine.on_candidate(cand(tmp_db))
    await r.engine.on_clock(datetime(2026, 1, 2, 20, 30, tzinfo=UTC))   # 15:30 ET
    assert {x["reject_reason"] for x in rows(tmp_db)} == {"AFTER_CUTOFF"}


async def test_clock_flushes_a_bucket_whose_last_minute_never_arrived(tmp_db):
    r = make(tmp_db)
    await r.engine.on_candidate(cand(tmp_db))
    await bar5(r, 0, 101.0, 100.0, 100.8, 1500, last=False)
    assert r.bars == []
    await r.engine.on_clock(T0 + timedelta(minutes=5, seconds=4))
    assert r.bars == []
    await r.engine.on_clock(T0 + timedelta(minutes=5, seconds=5))
    assert len(r.bars) == 1 and r.bars[0][1].start == utc_iso(T0)


async def test_bars_already_covered_by_warmup_are_not_double_counted(tmp_db):
    r = make(tmp_db)
    await r.engine.on_candidate(cand(tmp_db))
    old = Bar("NVDA", "2025-12-31T14:30:00.000Z", 100, 100.5, 99.5, 100, 1000, 5)
    await r.engine._process_bar5(old)
    assert r.bars == []


async def test_only_bars_ending_after_the_news_count(tmp_db):
    r = make(tmp_db)
    await r.engine.on_candidate(cand(tmp_db))
    await bar5(r, -1, 110.0, 99.0, 109.0, 10**6)             # 09:55 bucket ended 10:00, before the 10:02:10 news
    assert rows(tmp_db, variant="production")[0]["stage"] == "WAITING_FOR_VOLUME"
    assert rows(tmp_db, variant="production")[0]["rvol"] is None


async def test_duplicate_candidate_does_not_duplicate_shadow_rows(tmp_db):
    r = make(tmp_db)
    c = cand(tmp_db)
    await r.engine.on_candidate(c)
    await r.engine.on_candidate(c)
    names = [x["variant"] for x in rows(tmp_db)]
    assert sorted(names) == ["production", "rvol_1_5", "second_pullback"]


async def test_shadow_disabled_arms_production_only(tmp_db):
    r = make(tmp_db)
    r.engine.shadow_enabled = False
    await r.engine.on_candidate(cand(tmp_db))
    await r.engine.on_candidate(cand(tmp_db, sym="AAA", direction=Direction.NEUTRAL, reject=RejectReason.AI_NEUTRAL,
                                     gate=RejectReason.AI_NEUTRAL, article="n"))
    assert [x["variant"] for x in rows(tmp_db)] == ["production", "production"]


# ---------------------------------------------------------------- 1m backfill
def m1(minute, v, o=100.0, h=None, l=None, c=100.0):
    ts = T0 + timedelta(minutes=minute)
    return Bar("NVDA", utc_iso(ts), o, h or max(o, c), l or min(o, c), c, v, 1)


async def test_backfilled_first_bar_has_full_volume_and_no_double_count(tmp_db):
    ctx = make_ctx()
    ctx.bars_1m = [m1(0, 100), m1(1, 100), m1(2, 100)]
    r = make(tmp_db, md=FakeMD(ctx))
    await r.engine.on_candidate(cand(tmp_db))
    for k in (2, 3, 4):                                        # minute 2 resent live (same volume) + minutes 3, 4
        ts = T0 + timedelta(minutes=k)
        await r.engine.on_bar_1m(Bar("NVDA", utc_iso(ts), 100, 100, 100, 100, 100, 1))
    [(sym, bar, _)] = r.bars
    assert bar.volume == 500 and sym == "NVDA"                  # 5 minutes x 100, minute 2 counted once


async def test_late_armed_symbol_replays_post_news_buckets_in_order(tmp_db):
    ctx = make_ctx()
    ctx.bars_1m = ([m1(k, 300, h=101.0 if k == 0 else None) for k in range(5)]
                   + [m1(5 + k, 500, o=101.8, h=102.0 if k == 0 else None, l=100.8 if k == 1 else None, c=101.8)
                      for k in range(5)]
                   + [m1(10, 50, o=101.0, c=101.0)])            # open bucket 2
    r = make(tmp_db, md=FakeMD(ctx))
    await r.engine.on_candidate(cand(tmp_db))
    assert [b.start for _, b, _ in r.bars] == [utc_iso(T0), utc_iso(T0 + timedelta(minutes=5))]
    prod = rows(tmp_db, variant="production")[0]
    assert prod["stage"] == "WAITING_FOR_PULLBACK" and prod["rvol"] == pytest.approx(2.5)
    assert r.bars[0][1].volume == 1500


async def test_ref_source_is_logged_when_armed(tmp_db):
    ctx = make_ctx()
    ctx.ref_source = "1m_open"
    r = make(tmp_db, md=FakeMD(ctx))
    c = cand(tmp_db)
    await r.engine.on_candidate(c)
    import json
    d = tmp_db.one("SELECT data_json FROM timeline WHERE setup_id=? AND message LIKE '%subscribed%'", (c.setup_id,))
    assert json.loads(d["data_json"])["ref_source"] == "1m_open"


# ---------------------------------------------------------------- review fixes
import dataclasses  # noqa: E402
import json  # noqa: E402


async def minute_bars(r, vols, highs=None, sym="NVDA", first=0, o=100.0):
    for k, v in enumerate(vols):
        ts = T0 + timedelta(minutes=first + k)
        r.clock.set(ts + timedelta(seconds=59))
        hi = (highs or {}).get(k, o)
        await r.engine.on_bar_1m(Bar(sym, utc_iso(ts), o, hi, o, o, v, 1))


async def test_news_mid_bucket_counts_only_post_news_minutes_and_prorates_baseline(tmp_db):
    r = make(tmp_db)
    c = cand(tmp_db)
    c = dataclasses.replace(c, news_received_at=T0 + timedelta(minutes=3, seconds=20))   # 10:03:20
    await r.engine.on_candidate(c)
    # before the news: 3 x 600 shares and a 103 high (would be rvol 2.0 + impulse 3% if the bucket leaked)
    await minute_bars(r, [600, 600, 600, 100, 100], highs={0: 103.0, 3: 100.4})
    m = r.engine._active[c.setup_id].m
    assert m.rvol == pytest.approx(200 / (1000 * 2 / 5))        # 200 post-news shares vs baseline x 2/5
    assert m.impulse_pct == pytest.approx(0.4) and m.stage == Stage.WAITING_FOR_VOLUME
    assert [b.volume for _, b, _ in r.bars] == [2000]           # the published 5m bar (indicators/trail) stays whole


async def test_news_in_first_minute_of_bucket_keeps_the_whole_bucket(tmp_db):
    r = make(tmp_db)
    c = dataclasses.replace(cand(tmp_db), news_received_at=T0 + timedelta(seconds=20))
    await r.engine.on_candidate(c)
    await minute_bars(r, [600, 600, 600, 100, 100])
    assert r.engine._active[c.setup_id].m.rvol == pytest.approx(2.0)


async def test_no_print_after_news_in_bucket_is_zero_volume(tmp_db):
    r = make(tmp_db)
    c = dataclasses.replace(cand(tmp_db), news_received_at=T0 + timedelta(minutes=4, seconds=50))
    await r.engine.on_candidate(c)
    await minute_bars(r, [900, 900, 900, 900])                  # minute 4 (the news minute) never arrives
    await r.engine.on_clock(T0 + timedelta(minutes=5, seconds=10))
    m = r.engine._active[c.setup_id].m
    assert m.rvol == 0.0 and m.impulse_pct == 0.0


async def test_one_machine_blowing_up_is_isolated_on_trade_and_bar(tmp_db):
    r = make(tmp_db)
    c = cand(tmp_db)
    await r.engine.on_candidate(c)
    prod = r.engine._active[c.setup_id].m
    others = [a.m for a in r.engine._active.values() if a.m is not prod]
    assert len(others) == 2
    seen = []
    for m in others:
        real = m.on_trade
        m.on_trade = lambda t, real=real: seen.append(t) or real(t)
    prod.on_trade = lambda t: 1 / 0
    await r.engine.on_trade(trade(101.0, 1))                     # must not raise
    assert len(seen) == 2                                         # other machines still ran
    row = rows(tmp_db, variant="production")[0]
    assert (row["stage"], row["reject_reason"]) == ("REJECTED", "INTERNAL_ERROR")
    assert f"setup-{c.setup_id}" not in r.subs.owners("NVDA")     # slot released
    ev = tmp_db.one("SELECT * FROM system_events WHERE event='setup_internal_error'")
    assert ev["level"] == "ERROR" and "ZeroDivisionError" in ev["message"]
    # a bar that blows up a shadow machine: production-less, but the sibling still counts it
    sh, sibling = others
    sh.on_bar = lambda *a, **k: 1 / 0
    await bar5(r, 0, 101.0, 100.0, 100.8, 1500)
    assert sum(1 for x in rows(tmp_db) if x["reject_reason"] == "INTERNAL_ERROR") == 2
    assert sibling.rvol == pytest.approx(1.5) and len(r.bars) == 1


async def test_shadow_book_error_does_not_stop_the_production_signal(tmp_db):
    r = make(tmp_db)
    await r.engine.on_candidate(cand(tmp_db))
    await to_breakout(r)

    def boom(t):
        raise RuntimeError("book down")
    r.engine.book.on_trade = boom
    await r.engine.on_trade(trade(101.6, 21))
    assert len(r.signals) == 1
    assert tmp_db.one("SELECT 1 FROM system_events WHERE event='shadow_book_error'")


class GapMD(FakeMD):
    def __init__(self, bars=(), fail=False):
        super().__init__()
        self.bars, self.fail, self.since = list(bars), fail, []

    async def bars_1m_since(self, symbol, start):
        self.since.append((symbol, start))
        if self.fail:
            raise TimeoutError("rest down")
        return list(self.bars)


async def test_backfill_after_gap_fills_missing_minutes_and_is_dedupe_safe(tmp_db):
    md = GapMD([m1(k, 100) for k in (1, 2, 3, 4)])               # REST returns minute 1 again (already seen) + the hole
    r = make(tmp_db, md=md)
    c = dataclasses.replace(cand(tmp_db), news_received_at=T0 + timedelta(seconds=5))
    await r.engine.on_candidate(c)
    await minute_bars(r, [100, 100])                             # minutes 0, 1 live, then the socket drops
    assert r.bars == []
    assert await r.engine.backfill_after_gap() == 3              # only minutes 2..4 are new
    assert md.since == [("NVDA", T0 + timedelta(minutes=1))]
    [(_, bar, _)] = r.bars
    assert bar.start == utc_iso(T0) and bar.volume == 500        # understated 200 before the fix
    assert await r.engine.backfill_after_gap() == 0 and len(r.bars) == 1   # repeat: nothing new, no double count


async def test_backfill_after_gap_survives_rest_failure(tmp_db):
    md = GapMD(fail=True)
    r = make(tmp_db, md=md)
    await r.engine.on_candidate(cand(tmp_db))
    await minute_bars(r, [100])
    assert await r.engine.backfill_after_gap() == 0
    assert tmp_db.one("SELECT 1 FROM system_events WHERE event='gap_backfill_failed'")


async def test_second_story_on_an_armed_symbol_replays_post_news_bars(tmp_db):
    r = make(tmp_db)
    await r.engine.on_candidate(cand(tmp_db))
    await to_breakout(r)                                          # bars 0..3 already completed live
    news2 = T0 + timedelta(minutes=5, seconds=10)                 # second story lands in bucket 1
    c2 = dataclasses.replace(cand(tmp_db, article="a2"), news_received_at=news2)
    await r.engine.on_candidate(c2)
    row = [x for x in rows(tmp_db, variant="production") if x["article_id"] == "a2"][0]
    assert row["stage"] in ("WAITING_FOR_PULLBACK", "WAITING_FOR_BREAKOUT")   # was stuck at WAITING_FOR_VOLUME
    assert row["rvol"] == pytest.approx(2.5) and row["impulse_pct"] == pytest.approx(2.0)
    # the a1 machine was not disturbed and bars were not double counted
    assert len(tmp_db.query("SELECT * FROM market_events WHERE kind='BAR_5M'")) == 4


async def test_ref_source_is_persisted_on_the_setup_row(tmp_db):
    ctx = make_ctx()
    ctx.ref_source = "1m_prev_close"
    r = make(tmp_db, md=FakeMD(ctx))
    await r.engine.on_candidate(cand(tmp_db))
    assert {x["ref_source"] for x in rows(tmp_db)} == {"1m_prev_close"}


async def test_shadow_timeline_lines_are_tagged_production_lines_are_not(tmp_db):
    r = make(tmp_db)
    c = cand(tmp_db)
    await r.engine.on_candidate(c)
    await to_breakout(r)
    v = by_variant(tmp_db)
    def msgs(sid):
        return [m["message"] for m in tmp_db.query("SELECT message FROM timeline WHERE setup_id=?", (sid,))]
    assert not any(m.startswith("[shadow") for m in msgs(v["production"]["id"]))
    for name in ("rvol_1_5", "second_pullback"):
        lines = msgs(v[name]["id"])
        assert lines and all(m.startswith(f"[shadow {name}] ") for m in lines)
        assert any("RVOL" in m for m in lines)
