"""Asset cache + static/market universe filters (CONTRACT §7, §8)."""
from __future__ import annotations

from datetime import datetime

from ..clock import RealClock, utc_iso
from ..config import StrategyParams
from ..db import Database
from ..models import RejectReason


def _norm(v: object) -> str:
    return str(v or "").strip().lower().split(".")[-1]


class AssetCache:
    def __init__(self) -> None:
        self._a: dict[str, dict] = {}

    def load(self, assets: list[dict]) -> None:
        self._a = {str(a["symbol"]).upper(): a for a in assets}

    def get(self, symbol: str) -> dict | None:
        return self._a.get(symbol.upper())

    def check_static(self, symbol: str) -> RejectReason | None:
        a = self.get(symbol)
        if a is None:
            return RejectReason.NOT_TRADABLE
        if _norm(a.get("asset_class")) != "us_equity":
            return RejectReason.NOT_US_EQUITY
        if _norm(a.get("exchange")) == "otc":
            return RejectReason.OTC
        if not a.get("tradable") or _norm(a.get("status")) != "active":
            return RejectReason.NOT_TRADABLE
        return None

    def short_eligible(self, symbol: str) -> bool:
        a = self.get(symbol)
        return bool(a and a.get("shortable") and a.get("easy_to_borrow"))


def check_market(symbol: str, last_price: float | None, avg_dollar_volume: float | None,
                 feed_used: str, halted: bool, params: StrategyParams) -> RejectReason | None:
    if halted:
        return RejectReason.HALTED
    if last_price is None or avg_dollar_volume is None:
        return RejectReason.NO_MARKET_DATA
    if not params.min_price <= last_price <= params.max_price:
        return RejectReason.PRICE_OUT_OF_RANGE
    floor = params.min_avg_dollar_volume * (params.iex_liquidity_scale if feed_used == "iex" else 1.0)
    if avg_dollar_volume < floor:
        return RejectReason.ILLIQUID
    return None


def upsert_symbol_row(db: Database, symbol: str, asset: dict | None = None, *,
                      last_price: float | None = None, avg_dollar_volume: float | None = None,
                      market_cap: float | None = None, reject_reason: RejectReason | str | None = None,
                      now: datetime | str | None = None) -> None:
    ts = now if isinstance(now, str) else utc_iso(now or RealClock().now())
    row: dict = {"symbol": symbol.upper(), "reject_reason": str(reject_reason) if reject_reason else None,
                 "updated_at": ts}
    a = asset or {}
    for k in ("name", "exchange", "asset_class", "status"):
        if k in a:
            row[k] = a[k] if a[k] is None else str(a[k])
    for k in ("tradable", "shortable", "easy_to_borrow"):
        if k in a:
            row[k] = int(bool(a[k]))
    for k, v in (("last_price", last_price), ("avg_dollar_volume", avg_dollar_volume), ("market_cap", market_cap)):
        if v is not None:
            row[k] = v
    db.upsert("symbols", row, ["symbol"])
