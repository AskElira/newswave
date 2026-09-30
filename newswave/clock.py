"""Injected clock + ET session helpers (CONTRACT §3)."""
from __future__ import annotations

from datetime import UTC, date, datetime, time, timedelta
from typing import Protocol
from zoneinfo import ZoneInfo

ET = ZoneInfo("America/New_York")
OPEN_T = time(9, 30)
CLOSE_T = time(16, 0)


class Clock(Protocol):
    def now(self) -> datetime: ...


class RealClock:
    def now(self) -> datetime:
        return datetime.now(UTC)


class ReplayClock:
    def __init__(self, start: datetime | None = None) -> None:
        self._now = _aware(start or datetime(2026, 1, 2, 14, 30, tzinfo=UTC))

    def now(self) -> datetime:
        return self._now

    def set(self, ts: datetime) -> None:
        self._now = _aware(ts)

    def advance(self, **kw: float) -> datetime:
        self._now += timedelta(**kw)
        return self._now


def _aware(ts: datetime) -> datetime:
    if ts.tzinfo is None:
        raise ValueError("naive datetime")
    return ts.astimezone(UTC)


def to_et(ts: datetime) -> datetime:
    return _aware(ts).astimezone(ET)


def session_bounds(d: date) -> tuple[datetime, datetime]:
    """Regular session open/close for ET date d, as UTC datetimes (no holiday/early-close awareness)."""
    return (datetime.combine(d, OPEN_T, ET).astimezone(UTC),
            datetime.combine(d, CLOSE_T, ET).astimezone(UTC))


def is_regular_session(ts: datetime) -> bool:
    e = to_et(ts)
    return e.weekday() < 5 and OPEN_T <= e.time() < CLOSE_T


def before_cutoff(ts: datetime, hhmm: str) -> bool:
    """True if ET wall time of ts is strictly before HH:MM."""
    h, m = hhmm.split(":")
    return to_et(ts).time() < time(int(h), int(m))


# ---- session registry: the ONE place early closes are applied (set by the daemon from the broker calendar)
NO_ENTRY_LEAD = timedelta(minutes=30)  # stop entering this long before the real close
FLATTEN_LEAD = timedelta(minutes=5)  # be flat this long before the real close
_CLOSES: dict[date, datetime] = {}


def set_session_close(d: date, close: datetime) -> None:
    _CLOSES[d] = _aware(close)
    for old in [k for k in _CLOSES if k < d - timedelta(days=7)]:
        del _CLOSES[old]


def clear_session_closes() -> None:
    _CLOSES.clear()


def session_close(ts: datetime) -> datetime:
    """Real close of ts's ET session (registered early close, else 16:00 ET)."""
    d = to_et(ts).date()
    return _CLOSES.get(d) or datetime.combine(d, CLOSE_T, ET).astimezone(UTC)


def _at(ts: datetime, hhmm: str) -> datetime:
    h, m = hhmm.split(":")
    return datetime.combine(to_et(ts).date(), time(int(h), int(m)), ET).astimezone(UTC)


def effective_cutoff(ts: datetime, params) -> datetime:
    """No new entries at/after this instant: min(NO_NEW_ENTRIES_AFTER, close - 30 min)."""
    return min(_at(ts, params.no_new_entries_after), session_close(ts) - NO_ENTRY_LEAD)


def effective_eod(ts: datetime, params) -> datetime:
    """Flatten at/after this instant: min(EOD_FLATTEN_AT, close - 5 min)."""
    return min(_at(ts, params.eod_flatten_at), session_close(ts) - FLATTEN_LEAD)


def utc_iso(ts: datetime) -> str:
    return _aware(ts).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def parse_iso(s: str) -> datetime:
    return datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(UTC)
