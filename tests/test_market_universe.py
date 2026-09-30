from __future__ import annotations

import pytest

from newswave.config import StrategyParams
from newswave.market.universe import AssetCache, check_market, upsert_symbol_row
from newswave.models import RejectReason as R

P = StrategyParams()


def asset(symbol="AAPL", **kw):
    a = dict(symbol=symbol, name="Apple", exchange="NASDAQ", asset_class="us_equity", tradable=True,
             shortable=True, easy_to_borrow=True, status="active")
    return {**a, **kw}


@pytest.fixture
def cache():
    c = AssetCache()
    c.load([asset(), asset("OTCX", exchange="OTC"), asset("CRYP", asset_class="crypto"),
            asset("NOTR", tradable=False), asset("INAC", status="inactive"),
            asset("ENUM", status="AssetStatus.ACTIVE", asset_class="AssetClass.US_EQUITY"),
            asset("HTB", easy_to_borrow=False), asset("NOSH", shortable=False)])
    return c


def test_check_static(cache):
    assert cache.check_static("AAPL") is None
    assert cache.check_static("aapl") is None
    assert cache.check_static("ENUM") is None
    assert cache.check_static("ZZZZ") == R.NOT_TRADABLE
    assert cache.check_static("CRYP") == R.NOT_US_EQUITY
    assert cache.check_static("OTCX") == R.OTC
    assert cache.check_static("NOTR") == R.NOT_TRADABLE
    assert cache.check_static("INAC") == R.NOT_TRADABLE


def test_short_eligible(cache):
    assert cache.short_eligible("AAPL")
    assert not cache.short_eligible("HTB") and not cache.short_eligible("NOSH") and not cache.short_eligible("ZZZZ")


def test_check_market_branches():
    cm = lambda **kw: check_market("X", **{**dict(last_price=50, avg_dollar_volume=6e6, feed_used="sip",
                                                   halted=False, params=P), **kw})
    assert cm() is None
    assert cm(halted=True) == R.HALTED
    assert cm(last_price=2.99) == R.PRICE_OUT_OF_RANGE and cm(last_price=500.01) == R.PRICE_OUT_OF_RANGE
    assert cm(last_price=3) is None and cm(last_price=500) is None
    assert cm(avg_dollar_volume=4.9e6) == R.ILLIQUID
    assert cm(avg_dollar_volume=1.1e5, feed_used="iex") is None  # 5e6 * 0.02 = 1e5
    assert cm(avg_dollar_volume=9e4, feed_used="iex") == R.ILLIQUID
    assert cm(avg_dollar_volume=9e4, feed_used="sip") == R.ILLIQUID
    assert cm(last_price=None) == R.NO_MARKET_DATA and cm(avg_dollar_volume=None) == R.NO_MARKET_DATA


def test_upsert_symbol_row_keeps_prior_values(tmp_db):
    upsert_symbol_row(tmp_db, "AAPL", asset(), last_price=10.0, avg_dollar_volume=1e7, now="2026-01-02T00:00:00.000Z")
    upsert_symbol_row(tmp_db, "AAPL", reject_reason=R.ILLIQUID, now="2026-01-02T01:00:00.000Z")
    r = tmp_db.one("SELECT * FROM symbols WHERE symbol='AAPL'")
    assert r["reject_reason"] == "ILLIQUID" and r["last_price"] == 10.0 and r["tradable"] == 1
    assert r["exchange"] == "NASDAQ" and r["updated_at"].startswith("2026-01-02T01")
    upsert_symbol_row(tmp_db, "AAPL", reject_reason=None)
    assert tmp_db.one("SELECT reject_reason FROM symbols")["reject_reason"] is None
