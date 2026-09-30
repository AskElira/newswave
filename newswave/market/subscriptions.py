"""Pure subscription slot manager (CONTRACT §8). No I/O, no clock reads (caller passes `now`)."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import IntEnum


class SlotPriority(IntEnum):
    SHADOW = 1
    PRODUCTION = 2
    POSITION = 3


@dataclass
class _Hold:
    priority: SlotPriority
    confidence: float
    at: datetime
    seq: int


class SubscriptionManager:
    def __init__(self, cap: int = 30, reserved: tuple[str, ...] = ("SPY", "QQQ"), shadow_max: int = 8) -> None:
        self.cap, self.reserved, self.shadow_max = cap, frozenset(reserved), shadow_max
        self._holds: dict[str, dict[str, _Hold]] = {}  # symbol -> owner -> hold
        self._seq = 0
        self.evicted: list[tuple[str, str]] = []  # (owner_id, symbol), caller drains and rejects NO_SLOT

    # ---- queries ----
    def priority(self, symbol: str) -> SlotPriority | None:
        h = self._holds.get(symbol)
        return max(x.priority for x in h.values()) if h else None

    def _rank(self, symbol: str) -> tuple:
        h = self._holds[symbol].values()  # lowest tuple = best victim
        return (max(x.priority for x in h), max(x.seq for x in h), max(x.confidence for x in h))

    def _slots(self, extra: str | None = None) -> int:
        return len(set(self._holds) | self.reserved | ({extra} if extra else set()))

    def _shadow_count(self) -> int:
        return sum(1 for s in self._holds if s not in self.reserved and self.priority(s) == SlotPriority.SHADOW)

    def owners(self, symbol: str) -> set[str]:
        return set(self._holds.get(symbol, {}))

    def drain_evicted(self) -> list[tuple[str, str]]:
        out, self.evicted = self.evicted, []
        return out

    def desired(self) -> set[str]:
        return set(self._holds) | set(self.reserved)

    def diff(self, current: set[str]) -> tuple[set[str], set[str]]:
        want = self.desired()
        return want - set(current), set(current) - want

    # ---- mutations ----
    def request(self, symbol: str, owner_id: str, priority: SlotPriority, confidence: float,
                now: datetime) -> bool:
        if symbol not in self._holds and symbol not in self.reserved:
            if priority == SlotPriority.SHADOW and self._shadow_count() >= self.shadow_max:
                return False
            if self._slots(symbol) > self.cap and not self._evict_for(priority):
                return False
        self._seq += 1
        self._holds.setdefault(symbol, {})[owner_id] = _Hold(priority, confidence, now, self._seq)
        return True

    def _evict_for(self, priority: SlotPriority) -> bool:
        cands = [s for s in self._holds if s not in self.reserved and self.priority(s) != SlotPriority.POSITION]
        if not cands:
            return False
        victim = min(cands, key=self._rank)
        if priority <= self.priority(victim):  # type: ignore[operator]
            return False
        for owner in self._holds.pop(victim):
            self.evicted.append((owner, victim))
        return True

    def release(self, owner_id: str) -> None:
        for s in list(self._holds):
            self._holds[s].pop(owner_id, None)
            if not self._holds[s]:
                del self._holds[s]

    def promote(self, owner_id: str, priority: SlotPriority) -> None:
        for holds in self._holds.values():
            h = holds.get(owner_id)
            if h and priority > h.priority:
                h.priority = priority
