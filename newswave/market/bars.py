"""1m -> 5m bar builder. Buckets are aligned to ET 09:30, 09:35, ... (CONTRACT §8).

ET offsets are whole hours, so flooring the UTC epoch to 5 minutes equals flooring ET wall time
and is DST-safe (no wall-clock arithmetic).
"""
from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta

from ..clock import parse_iso, utc_iso
from ..models import Bar

log = logging.getLogger("newswave.bars")
_B = 300  # seconds


def bucket_start(ts: datetime) -> datetime:
    return datetime.fromtimestamp(int(ts.timestamp()) // _B * _B, UTC)


def _merge(symbol: str, start: datetime, bars: list[Bar]) -> Bar:
    bars = sorted(bars, key=lambda b: b.start)
    return Bar(symbol, utc_iso(start), bars[0].open, max(b.high for b in bars),
               min(b.low for b in bars), bars[-1].close, sum(b.volume for b in bars), 5)


class FiveMinuteBuilder:
    def __init__(self, symbol: str, grace_s: int = 5) -> None:
        self.symbol, self.grace = symbol, timedelta(seconds=grace_s)
        self._open: dict[datetime, dict[str, Bar]] = {}
        self._done: set[datetime] = set()

    def on_bar(self, b: Bar) -> list[Bar]:
        ts = parse_iso(b.start)
        bk = bucket_start(ts)
        if bk in self._done:
            log.warning("late 1m bar dropped %s %s", self.symbol, b.start)
            return []
        self._open.setdefault(bk, {})[utc_iso(ts)] = b  # same-minute resend replaces
        out: list[Bar] = []
        for old in sorted(k for k in self._open if k < bk):  # a later bucket started: earlier ones are complete
            out.append(self._emit(old))
        if ts == bk + timedelta(minutes=4):
            out.append(self._emit(bk))
        return out

    def flush_due(self, now: datetime) -> list[Bar]:
        due = sorted(k for k in self._open if now >= k + timedelta(seconds=_B) + self.grace)
        return [self._emit(k) for k in due]

    def _emit(self, bk: datetime) -> Bar:
        self._done.add(bk)
        return _merge(self.symbol, bk, list(self._open.pop(bk).values()))


def aggregate_5m(bars_1m: list[Bar]) -> list[Bar]:
    groups: dict[datetime, list[Bar]] = {}
    symbol = bars_1m[0].symbol if bars_1m else ""
    for b in bars_1m:
        groups.setdefault(bucket_start(parse_iso(b.start)), []).append(b)
    return [_merge(symbol, k, v) for k, v in sorted(groups.items())]
