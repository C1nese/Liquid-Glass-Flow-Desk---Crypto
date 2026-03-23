from dataclasses import dataclass, field
from typing import Any, Dict, Optional


@dataclass
class ExchangeSnapshot:
    exchange: str
    symbol: str
    last_price: Optional[float] = None
    mark_price: Optional[float] = None
    index_price: Optional[float] = None
    open_interest: Optional[float] = None
    open_interest_notional: Optional[float] = None
    funding_rate: Optional[float] = None
    volume_24h_base: Optional[float] = None
    volume_24h_notional: Optional[float] = None
    timestamp_ms: Optional[int] = None
    status: str = "ok"
    error: Optional[str] = None
    raw: Dict[str, Any] = field(default_factory=dict)

    @property
    def premium_pct(self) -> Optional[float]:
        if self.last_price is None or self.mark_price in (None, 0):
            return None
        return (self.last_price - self.mark_price) / self.mark_price * 100.0

    @property
    def funding_bps(self) -> Optional[float]:
        if self.funding_rate is None:
            return None
        return self.funding_rate * 10000.0

    def to_row(self) -> Dict[str, Any]:
        return {
            "Exchange": self.exchange,
            "Symbol": self.symbol,
            "Last": self.last_price,
            "Mark": self.mark_price,
            "Index/Oracle": self.index_price,
            "Premium %": self.premium_pct,
            "Open Interest": self.open_interest,
            "OI Notional": self.open_interest_notional,
            "Funding Rate": self.funding_rate,
            "Funding bps": self.funding_bps,
            "24h Base Volume": self.volume_24h_base,
            "24h Notional Volume": self.volume_24h_notional,
            "Timestamp": self.timestamp_ms,
            "Status": self.status,
            "Error": self.error,
        }


@dataclass
class SpotSnapshot:
    exchange: str
    symbol: str
    last_price: Optional[float] = None
    bid_price: Optional[float] = None
    ask_price: Optional[float] = None
    volume_24h_base: Optional[float] = None
    volume_24h_notional: Optional[float] = None
    timestamp_ms: Optional[int] = None
    status: str = "ok"
    error: Optional[str] = None
    raw: Dict[str, Any] = field(default_factory=dict)

    @property
    def spread_bps(self) -> Optional[float]:
        if self.bid_price in (None, 0) or self.ask_price is None or self.ask_price <= 0:
            return None
        mid = (self.bid_price + self.ask_price) * 0.5
        if mid <= 0:
            return None
        return (self.ask_price - self.bid_price) / mid * 10000.0


@dataclass
class Candle:
    timestamp_ms: int
    open: float
    high: float
    low: float
    close: float
    volume: float = 0.0


@dataclass
class OIPoint:
    timestamp_ms: int
    open_interest: Optional[float] = None
    open_interest_notional: Optional[float] = None


@dataclass
class LiquidationEvent:
    exchange: str
    symbol: str
    timestamp_ms: int
    side: str
    price: Optional[float] = None
    size: Optional[float] = None
    notional: Optional[float] = None
    source: str = "unknown"
    raw: Dict[str, Any] = field(default_factory=dict)


@dataclass
class TradeEvent:
    exchange: str
    symbol: str
    timestamp_ms: int
    side: str
    price: Optional[float] = None
    size: Optional[float] = None
    notional: Optional[float] = None
    source: str = "unknown"
    raw: Dict[str, Any] = field(default_factory=dict)


@dataclass
class OrderBookLevel:
    price: float
    size: float
    side: str


@dataclass
class OrderBookQualityPoint:
    timestamp_ms: int
    added_notional: float = 0.0
    canceled_notional: float = 0.0
    net_notional: float = 0.0
    near_added_notional: float = 0.0
    near_canceled_notional: float = 0.0
    spoof_events: int = 0
    refill_events: int = 0
    bid_wall_persistence_s: float = 0.0
    ask_wall_persistence_s: float = 0.0
    imbalance_pct: Optional[float] = None
    best_bid: Optional[float] = None
    best_ask: Optional[float] = None


@dataclass
class RecordedMarketEvent:
    timestamp_ms: int
    exchange: str
    symbol: str
    category: str
    market: str = "perp"
    side: Optional[str] = None
    price: Optional[float] = None
    size: Optional[float] = None
    notional: Optional[float] = None
    value: Optional[float] = None
    label: Optional[str] = None
    raw: Dict[str, Any] = field(default_factory=dict)
