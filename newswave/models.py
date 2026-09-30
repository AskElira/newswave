"""Shared enums and dataclasses (CONTRACT §5). Enum strings are exact; never rename."""
from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum


class Direction(StrEnum):
    BULLISH = "BULLISH"
    NEUTRAL = "NEUTRAL"
    BEARISH = "BEARISH"


class Side(StrEnum):
    LONG = "LONG"
    SHORT = "SHORT"


class Stage(StrEnum):
    NEWS = "NEWS"
    CLASSIFIED = "CLASSIFIED"
    WAITING_FOR_VOLUME = "WAITING_FOR_VOLUME"
    WAITING_FOR_IMPULSE = "WAITING_FOR_IMPULSE"
    WAITING_FOR_PULLBACK = "WAITING_FOR_PULLBACK"
    WAITING_FOR_BREAKOUT = "WAITING_FOR_BREAKOUT"
    ENTRY_SIGNAL = "ENTRY_SIGNAL"
    IN_POSITION = "IN_POSITION"
    CLOSED = "CLOSED"
    REJECTED = "REJECTED"  # terminal non-entry; rank is -1 (max_stage keeps the furthest real stage)

    @property
    def rank(self) -> int:
        return _STAGE_ORDER.get(self, -1)


_STAGE_ORDER = {s: i for i, s in enumerate(
    [Stage.NEWS, Stage.CLASSIFIED, Stage.WAITING_FOR_VOLUME, Stage.WAITING_FOR_IMPULSE,
     Stage.WAITING_FOR_PULLBACK, Stage.WAITING_FOR_BREAKOUT, Stage.ENTRY_SIGNAL,
     Stage.IN_POSITION, Stage.CLOSED])}


class RejectReason(StrEnum):
    DUPLICATE_NEWS = "DUPLICATE_NEWS"
    CATALYST_COOLDOWN = "CATALYST_COOLDOWN"
    NEWS_TOO_OLD = "NEWS_TOO_OLD"
    NO_SYMBOLS = "NO_SYMBOLS"
    TOO_MANY_SYMBOLS = "TOO_MANY_SYMBOLS"
    MARKET_CLOSED = "MARKET_CLOSED"
    AFTER_CUTOFF = "AFTER_CUTOFF"
    NOT_TRADABLE = "NOT_TRADABLE"
    OTC = "OTC"
    NOT_US_EQUITY = "NOT_US_EQUITY"
    PRICE_OUT_OF_RANGE = "PRICE_OUT_OF_RANGE"
    ILLIQUID = "ILLIQUID"
    HALTED = "HALTED"
    CLASSIFIER_BUDGET = "CLASSIFIER_BUDGET"
    AI_ERROR = "AI_ERROR"
    AI_NEUTRAL = "AI_NEUTRAL"
    AI_BEARISH = "AI_BEARISH"
    AI_NOT_MATERIAL = "AI_NOT_MATERIAL"
    AI_LOW_CONFIDENCE = "AI_LOW_CONFIDENCE"
    NO_SLOT = "NO_SLOT"
    NO_MARKET_DATA = "NO_MARKET_DATA"
    NO_VOLUME = "NO_VOLUME"
    NO_MOMENTUM = "NO_MOMENTUM"
    NO_PULLBACK = "NO_PULLBACK"
    PULLBACK_COLLAPSE = "PULLBACK_COLLAPSE"
    IMPULSE_LOST = "IMPULSE_LOST"
    VOLUME_DRIED_UP = "VOLUME_DRIED_UP"
    CONTRADICTORY_NEWS = "CONTRADICTORY_NEWS"
    SETUP_EXPIRED = "SETUP_EXPIRED"
    SHORT_UNAVAILABLE = "SHORT_UNAVAILABLE"
    RISK_LIMIT = "RISK_LIMIT"
    MAX_POSITIONS = "MAX_POSITIONS"
    MAX_CONCURRENT_RISK = "MAX_CONCURRENT_RISK"
    SIZE_ZERO = "SIZE_ZERO"
    BUYING_POWER = "BUYING_POWER"
    PDT_LIMIT = "PDT_LIMIT"
    KILL_SWITCH = "KILL_SWITCH"
    ENTRY_NOT_FILLED = "ENTRY_NOT_FILLED"
    ORDER_REJECTED = "ORDER_REJECTED"
    NOT_COUNTERFACTUAL = "NOT_COUNTERFACTUAL"
    INTERNAL_ERROR = "INTERNAL_ERROR"


class ExitReason(StrEnum):
    STOP = "STOP"
    TRAIL = "TRAIL"
    PARTIAL_PROFIT = "PARTIAL_PROFIT"
    TIME_STOP = "TIME_STOP"
    EOD = "EOD"
    RISK_KILL = "RISK_KILL"
    OPPOSITE_NEWS = "OPPOSITE_NEWS"
    NON_TRADABLE = "NON_TRADABLE"
    STATE_CORRUPT = "STATE_CORRUPT"


class OrderPurpose(StrEnum):
    ENTRY = "ENTRY"
    STOP = "STOP"
    PARTIAL = "PARTIAL"
    EXIT = "EXIT"


@dataclass(frozen=True)
class NewsEvent:
    article_id: str
    received_at: str  # ISO UTC 'Z'
    created_at: str
    updated_at: str
    headline: str
    summary: str
    content: str
    symbols: tuple[str, ...]
    source: str
    url: str


@dataclass(frozen=True)
class Classification:
    direction: Direction
    confidence: float
    material: bool
    catalyst: str
    reason: str
    model: str
    error: str | None = None


@dataclass(frozen=True)
class Bar:
    symbol: str
    start: str  # ISO UTC 'Z' bar open time
    open: float
    high: float
    low: float
    close: float
    volume: float
    timeframe_min: int = 5


@dataclass(frozen=True)
class Trade:
    """A market print."""
    symbol: str
    ts: str
    price: float
    size: float


@dataclass(frozen=True)
class EntrySignal:
    setup_id: int
    symbol: str
    side: Side
    trigger_price: float
    pullback_low: float
    pullback_high: float
    atr: float
    signal_at: str
    variant: str = "production"


# Only risk.py may import and use this token. Nothing else may construct a RiskApproval.
_RISK_TOKEN = object()


@dataclass(frozen=True)
class RiskApproval:
    """Sized, approved entry. Constructible ONLY by RiskEngine (risk.py), which imports
    `_RISK_TOKEN` from this module and passes it as `token`."""
    signal: EntrySignal
    qty: int
    entry_price: float
    limit_price: float
    stop_price: float
    risk_per_share: float
    risk_dollars: float
    token: object = None

    def __post_init__(self) -> None:
        if self.token is not _RISK_TOKEN:
            raise PermissionError("RiskApproval may only be created by RiskEngine")


@dataclass(frozen=True)
class ExitAction:
    kind: ExitReason
    qty: int
    price_hint: float | None = None
