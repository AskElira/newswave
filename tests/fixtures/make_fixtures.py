"""Synthetic replay fixtures (deterministic). Regenerate with:  .venv/bin/python tests/fixtures/make_fixtures.py

The NVDA story (Fri 2026-01-02, all times ET): news 10:00:10, impulse bucket 10:05, first EMA9 pullback
10:15, breakout print 10:22:40 (BUY 20 @ 101.60, stop 100.60), +1R partial 10:24, ATR-trail close 10:40.
History: 21 flat prior sessions of 5m IEX bars (volume 1000 per slot, EMA9 = 100, ATR = 1).
"""
from __future__ import annotations

import json
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

ET = ZoneInfo("America/New_York")
SESSION = date(2026, 1, 2)
HERE = Path(__file__).resolve().parent


def _z(d: datetime) -> str:
    return d.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def at(hh: int, mm: int, ss: int = 0, day: date = SESSION) -> datetime:
    return datetime.combine(day, time(hh, mm, ss), ET)


def history_5m(base: float = 100.0, vol: float = 1000.0, sessions: int = 21, end: date = date(2025, 12, 31)) -> list[dict]:
    days: list[date] = []
    d = end
    while len(days) < sessions:
        if d.weekday() < 5:
            days.append(d)
        d -= timedelta(days=1)
    out = []
    for d in sorted(days):
        for i in range(78):
            out.append({"t": _z(at(9, 30, 0, d) + timedelta(minutes=5 * i)), "o": base, "h": base + 0.5,
                        "l": base - 0.5, "c": base, "v": vol})
    return out


# (minute offset from 10:00 ET) -> (open, high, low, close, volume). Buckets 0-3 replicate the strategy-engine
# unit story (impulse, pullback to EMA9); 4 is the breakout; 5-6 ride; 7 closes below the trail; 8-9 drift.
def _m(buckets: list[list[tuple[float, float, float, float]]], vols: list[float]) -> list[dict]:
    out = []
    for i, (bars, v) in enumerate(zip(buckets, vols)):
        for k, (o, h, l, c) in enumerate(bars):
            out.append({"t": _z(at(10, 0) + timedelta(minutes=5 * i + k)), "o": o, "h": h, "l": l, "c": c,
                        "v": v / 5})
    return out


def _path(o: float, closes: list[float], wick: float = 0.0) -> list[tuple[float, float, float, float]]:
    bars, prev = [], o
    for c in closes:
        bars.append((prev, max(prev, c) + wick, min(prev, c) - wick, c))
        prev = c
    return bars


def story_bars() -> list[dict]:
    b0 = _path(100.0, [100.2, 100.4, 100.6, 100.7, 100.8])
    b0[4] = (100.7, 101.0, 100.0, 100.8)            # bucket high 101.0 / low 100.0 (as in the unit story)
    b1 = _path(100.8, [101.0, 101.4, 101.7, 101.9, 101.8])
    b1[3] = (101.7, 102.0, 101.7, 101.9)            # bucket high 102.0
    b2 = _path(101.5, [101.3, 101.2, 101.1, 101.0, 101.0])
    b2[0] = (101.5, 101.5, 100.9, 101.3)            # highest pullback high 101.5, low 100.9
    b3 = _path(101.0, [101.1, 101.0, 100.8, 100.7, 100.9])
    b3[0] = (101.0, 101.3, 101.0, 101.1)
    b3[3] = (100.8, 100.8, 100.6, 100.7)            # low 100.6 touches EMA9 (~100.6)
    b4 = [(100.9, 101.2, 100.9, 101.2), (101.2, 101.5, 101.2, 101.5), (101.5, 101.6, 101.5, 101.6),
          (101.6, 102.0, 101.6, 102.0), (102.0, 102.7, 102.0, 102.7)]   # 101.6 print breaks the 101.55 trigger
    b5 = _path(102.7, [103.1, 103.5, 103.8, 104.0, 103.9])
    b6 = _path(103.9, [104.3, 104.5, 104.4, 104.3, 104.2])
    b7 = _path(104.2, [103.0, 102.0, 101.5, 101.2, 101.0])
    b8 = [(101.0, 101.1, 100.9, 101.0)] * 5
    b9 = [(101.0, 101.1, 100.9, 101.0)] * 5
    return _m([b0, b1, b2, b3, b4, b5, b6, b7, b8, b9],
              [1500, 2500, 800, 700, 1800, 1500, 1400, 2200, 500, 500])


def story_news(article: str = "a-nvda-1", headline: str = "NVIDIA raises full-year guidance", sym: str = "NVDA",
               ts: datetime | None = None) -> dict:
    created = ts or at(10, 0, 10)
    return {"id": article, "created_at": _z(created), "received_at": _z(created + timedelta(seconds=1)),
            "headline": headline, "summary": "Guidance raised well above consensus.",
            "content": "<p>Management raised the outlook.</p>", "symbols": [sym], "source": "benzinga",
            "url": "https://example.test/" + article}


def story_fixture() -> dict:
    return {"name": "story_nvda", "session_date": SESSION.isoformat(), "news": [story_news()],
            "bars_1m": {"NVDA": story_bars()}, "history_5m": {"NVDA": history_5m()},
            "classifications": {"a-nvda-1:NVDA": {"direction": "BULLISH", "confidence": 0.92, "material": True,
                                                  "catalyst": "Guidance raise", "reason": "guidance raised"}}}


if __name__ == "__main__":
    (HERE / "story_nvda.json").write_text(json.dumps(story_fixture(), sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8")
    print("wrote", HERE / "story_nvda.json")
