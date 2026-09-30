from __future__ import annotations

from datetime import UTC, date, datetime, timedelta

from newswave.clock import ReplayClock, utc_iso
from newswave.config import StrategyParams
from newswave.market.universe import AssetCache
from newswave.models import Bar, RejectReason, Trade
from newswave.strategy.engine import AlpacaMarketData, SymbolContext

NOW = datetime(2026, 1, 2, 15, 2, 12, tzinfo=UTC)   # Fri 10:02:12 ET


def session(d: date, vol=1000.0, until_min=None):
    """5m regular-session bars for ET date d (EST, open = 14:30Z)."""
    start = datetime(d.year, d.month, d.day, 14, 30, tzinfo=UTC)
    n = 78 if until_min is None else until_min // 5
    return [Bar("NVDA", utc_iso(start + timedelta(minutes=5 * i)), 100, 100.5, 99.5, 100, vol, 5) for i in range(n)]


def days(n, end=date(2025, 12, 31)):
    out, d = [], end
    while len(out) < n:
        if d.weekday() < 5:
            out.append(d)
        d -= timedelta(days=1)
    return sorted(out)


def one(minute, o, c, v=100.0):
    return Bar("NVDA", utc_iso(datetime(2026, 1, 2, 15, minute, tzinfo=UTC)), o, max(o, c), min(o, c), c, v, 1)


M1 = [one(0, 100.0, 100.2), one(1, 100.2, 100.4), one(2, 100.7, 100.9), one(3, 100.9, 101.0)]
NEWS = datetime(2026, 1, 2, 15, 2, 10, tzinfo=UTC)


class FakeHist:
    def __init__(self, n_days=8, daily_vol=1_000_000.0, trade=Trade("NVDA", "x", 101.0, 10), feed="sip", boom=False, m1=None):
        self.bars = [b for d in days(n_days) for b in session(d)] + session(date(2026, 1, 2), 5000.0, until_min=35)
        self.daily = [Bar("NVDA", "d", 100, 101, 99, 100, daily_vol, 1440)] * 20
        self.trade, self.feed, self.boom, self.calls = trade, feed, boom, []
        self.m1 = m1 if m1 is not None else M1
        self.m1_boom = False

    def bars_5m(self, symbol, start, end=None, feed="iex"):
        self.calls.append(("bars_5m", feed))
        if self.boom:
            raise OSError("down")
        return list(self.bars)

    def bars_1m(self, symbol, start, end=None, feed="iex"):
        if self.m1_boom:
            raise OSError("down")
        return [b for b in self.m1 if b.start >= utc_iso(start)]

    def daily_bars(self, symbol, days=20, feed="sip"):
        return self.daily, self.feed

    def latest_trade(self, symbol, feed="iex"):
        return self.trade


def mk(tmp_db, hist=None, asset=None):
    ac = AssetCache()
    ac.load([asset or {"symbol": "NVDA", "asset_class": "us_equity", "exchange": "NASDAQ", "tradable": True,
                       "status": "active", "shortable": True, "easy_to_borrow": True, "name": "NVIDIA"}])
    return AlpacaMarketData(hist or FakeHist(), ac, StrategyParams(), ReplayClock(NOW), db=tmp_db)


async def test_prepare_builds_context_and_writes_symbols_row(tmp_db):
    ctx = await mk(tmp_db).prepare("NVDA", NEWS)
    assert isinstance(ctx, SymbolContext)
    assert ctx.ref_price == 100.7 and ctx.last_price == 101.0 and ctx.feed_used == "sip" and not ctx.halted
    assert ctx.avg_dollar_volume == 100 * 1_000_000.0
    assert ctx.baseline["10:00"] == 1000.0 and len(ctx.baseline) == 78      # today's 5000-volume bars are NOT in it
    last = max(b.start for b in ctx.warm_bars_5m)
    assert last == "2026-01-02T14:55:00.000Z"                               # 09:55 bucket; 10:00 bucket is still open
    assert len(ctx.warm_bars_5m) > 9 + 14
    row = tmp_db.one("SELECT * FROM symbols WHERE symbol='NVDA'")
    assert row["last_price"] == 101.0 and row["reject_reason"] is None and row["name"] == "NVIDIA"


async def test_too_little_history_is_no_market_data(tmp_db):
    assert await mk(tmp_db, FakeHist(n_days=4)).prepare("NVDA", NEWS) == RejectReason.NO_MARKET_DATA
    assert tmp_db.one("SELECT reject_reason FROM symbols")["reject_reason"] == "NO_MARKET_DATA"


async def test_rest_failure_is_no_market_data(tmp_db):
    assert await mk(tmp_db, FakeHist(boom=True)).prepare("NVDA", NEWS) == RejectReason.NO_MARKET_DATA


async def test_check_market_reasons_pass_through(tmp_db):
    assert await mk(tmp_db, FakeHist(daily_vol=1000.0)).prepare("NVDA", NEWS) == RejectReason.ILLIQUID
    assert await mk(tmp_db, FakeHist(trade=Trade("NVDA", "x", 2.0, 1))).prepare("NVDA", NEWS) == RejectReason.PRICE_OUT_OF_RANGE
    assert tmp_db.one("SELECT reject_reason FROM symbols")["reject_reason"] == "PRICE_OUT_OF_RANGE"


async def test_no_latest_trade_falls_back_to_last_warm_bar_close(tmp_db):
    ctx = await mk(tmp_db, FakeHist(trade=None, m1=[])).prepare("NVDA", NEWS)
    assert isinstance(ctx, SymbolContext) and ctx.ref_price == 100.0


async def test_ref_price_is_open_of_the_1m_bar_containing_the_news(tmp_db):
    ctx = await mk(tmp_db).prepare("NVDA", NEWS)
    assert ctx.ref_price == 100.7 and ctx.ref_source == "1m_open" and ctx.last_price == 101.0


async def test_ref_falls_back_to_previous_close_then_latest_trade(tmp_db):
    gap = [one(0, 100.0, 100.2), one(1, 100.2, 100.4), one(3, 100.9, 101.0)]        # 15:02 missing
    ctx = await mk(tmp_db, FakeHist(m1=gap)).prepare("NVDA", NEWS)
    assert (ctx.ref_price, ctx.ref_source) == (100.4, "1m_prev_close")
    ctx = await mk(tmp_db, FakeHist(m1=[one(3, 100.9, 101.0)])).prepare("NVDA", NEWS)
    assert (ctx.ref_price, ctx.ref_source) == (101.0, "latest_trade")
    h = FakeHist()
    h.m1_boom = True
    ctx = await mk(tmp_db, h).prepare("NVDA", NEWS)
    assert (ctx.ref_price, ctx.ref_source, ctx.bars_1m) == (101.0, "latest_trade", [])


async def test_backfill_starts_at_the_news_bucket_and_warmup_ends_before_the_news(tmp_db):
    h = FakeHist()
    h.m1 = [Bar("NVDA", "2026-01-02T14:58:00.000Z", 99, 99, 99, 99, 5, 1)] + M1
    ctx = await mk(tmp_db, h).prepare("NVDA", NEWS)
    assert [b.start for b in ctx.bars_1m][0] == "2026-01-02T15:00:00.000Z" and len(ctx.bars_1m) == 4
    assert max(b.start for b in ctx.warm_bars_5m) == "2026-01-02T14:55:00.000Z"


async def test_bars_1m_since_fetches_rest_minutes_from_the_gap_start(tmp_db):
    bars = await mk(tmp_db).bars_1m_since("NVDA", datetime(2026, 1, 2, 15, 2, tzinfo=UTC))
    assert [b.start for b in bars] == [M1[2].start, M1[3].start]


async def test_warmup_baseline_and_liquidity_numbers_come_from_params(tmp_db):
    md = AlpacaMarketData(FakeHist(n_days=8), mk(tmp_db).asset_cache, StrategyParams(baseline_min_days=9),
                          ReplayClock(NOW), db=tmp_db)
    assert await md.prepare("NVDA", NEWS) == RejectReason.NO_MARKET_DATA        # only 8 sessions, 9 required
    md = AlpacaMarketData(FakeHist(n_days=8), mk(tmp_db).asset_cache, StrategyParams(warmup_sessions=1),
                          ReplayClock(NOW), db=tmp_db)
    ctx = await md.prepare("NVDA", NEWS)
    assert len(ctx.warm_bars_5m) == 6           # 1 session = only today's pre-news buckets (default 3 gives far more)
