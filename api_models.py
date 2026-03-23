from __future__ import annotations

from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class FlexibleApiModel(BaseModel):
    class Config:
        extra = "allow"
        arbitrary_types_allowed = True


class DataMetaModel(FlexibleApiModel):
    source: Optional[str] = None
    updated_at_ms: Optional[int] = None
    truth_level: Optional[str] = None
    compute_ms: Optional[float] = None


class CardModel(FlexibleApiModel):
    label: str
    value: Any = None
    sub: Optional[str] = None


class PanelMetaModel(DataMetaModel):
    stage: Optional[str] = None


class PanelModel(FlexibleApiModel):
    id: Optional[str] = None
    title: Optional[str] = None
    kind: Optional[str] = None
    note: Optional[str] = None
    limit: Optional[int] = None
    rows: Optional[List[Dict[str, Any]]] = None
    figure: Optional[Dict[str, Any]] = None
    meta: Optional[PanelMetaModel] = None


class SnapshotModel(FlexibleApiModel):
    exchange_key: Optional[str] = None
    exchange: Optional[str] = None
    symbol: Optional[str] = None
    status: Optional[str] = None
    error: Optional[str] = None
    timestamp_ms: Optional[int] = None


class OrderbookLevelModel(FlexibleApiModel):
    price: Optional[float] = None
    size: Optional[float] = None
    notional: Optional[float] = None


class OrderbookModel(FlexibleApiModel):
    bids: List[OrderbookLevelModel] = Field(default_factory=list)
    asks: List[OrderbookLevelModel] = Field(default_factory=list)


class TransportModel(FlexibleApiModel):
    snapshot_timestamp_ms: Optional[int] = None
    live_timestamp_ms: Optional[int] = None
    sample_timestamp_ms: Optional[int] = None
    trade_timestamp_ms: Optional[int] = None
    orderbook_levels: Optional[int] = None
    sync_state: Optional[str] = None


class PanelPayloadModel(FlexibleApiModel):
    coin: str
    generated_at_ms: int
    summary: Optional[Any] = None
    filters: Dict[str, Any] = Field(default_factory=dict)
    thresholds: Dict[str, Any] = Field(default_factory=dict)
    cards: List[CardModel] = Field(default_factory=list)
    panels: List[PanelModel] = Field(default_factory=list)
    data_meta: Optional[DataMetaModel] = None


class PingResponseModel(FlexibleApiModel):
    ok: bool


class CoinsResponseModel(FlexibleApiModel):
    coins: List[str] = Field(default_factory=list)
    defaults: List[str] = Field(default_factory=list)
    summary: Dict[str, Any] = Field(default_factory=dict)
    status: Dict[str, Any] = Field(default_factory=dict)
    errors: Dict[str, Any] = Field(default_factory=dict)


class LegacyStatusResponseModel(FlexibleApiModel):
    running: bool
    port: int
    url: str
    managed: bool
    status: str
    checked_at_ms: int


class OverviewPayloadModel(FlexibleApiModel):
    coin: str
    generated_at_ms: int
    snapshots: List[SnapshotModel] = Field(default_factory=list)
    headline_items: List[Dict[str, Any]] = Field(default_factory=list)
    cards: List[CardModel] = Field(default_factory=list)
    panels: List[PanelModel] = Field(default_factory=list)
    data_meta: Optional[DataMetaModel] = None


class OverviewRichPayloadModel(PanelPayloadModel):
    stage: Optional[str] = None


class MulticoinPayloadModel(PanelPayloadModel):
    watch_group: Optional[str] = None


class MonitorPayloadModel(PanelPayloadModel):
    rows: List[Dict[str, Any]] = Field(default_factory=list)
    whale_events: List[Dict[str, Any]] = Field(default_factory=list)
    flash_alerts: List[Dict[str, Any]] = Field(default_factory=list)
    exchange_sections: List[Dict[str, Any]] = Field(default_factory=list)
    signals: List[Dict[str, Any]] = Field(default_factory=list)
    oi_rows: List[Dict[str, Any]] = Field(default_factory=list)
    price_matrix_rows: List[Dict[str, Any]] = Field(default_factory=list)


class ExecutionPayloadModel(PanelPayloadModel):
    pass


class DepthPayloadModel(FlexibleApiModel):
    coin: str
    market: str
    exchange_key: str
    exchange: str
    snapshot: Dict[str, Any] = Field(default_factory=dict)
    summary: Dict[str, Any] = Field(default_factory=dict)
    transport: TransportModel = Field(default_factory=TransportModel)
    orderbook: OrderbookModel = Field(default_factory=OrderbookModel)
    trades: List[Dict[str, Any]] = Field(default_factory=list)
    recent_trades: List[Dict[str, Any]] = Field(default_factory=list)
    generated_at_ms: int
    data_meta: Optional[DataMetaModel] = None


class TapePayloadModel(PanelPayloadModel):
    items: List[Dict[str, Any]] = Field(default_factory=list)


class MarketSplitPayloadModel(PanelPayloadModel):
    rows: List[Dict[str, Any]] = Field(default_factory=list)


class LiquidationPayloadModel(PanelPayloadModel):
    events: List[Dict[str, Any]] = Field(default_factory=list)


class AlertsPayloadModel(PanelPayloadModel):
    live_alerts: List[Dict[str, Any]] = Field(default_factory=list)


class LabPayloadModel(PanelPayloadModel):
    section: Optional[str] = None


class OrderbookCenterPayloadModel(FlexibleApiModel):
    coin: str
    generated_at_ms: int
    exchange_key: str
    exchange: str
    snapshot: Dict[str, Any] = Field(default_factory=dict)
    perp_transport: Dict[str, Any] = Field(default_factory=dict)
    spot_transport: Dict[str, Any] = Field(default_factory=dict)
    perp_orderbook: OrderbookModel = Field(default_factory=OrderbookModel)
    spot_orderbook: OrderbookModel = Field(default_factory=OrderbookModel)
    perp_summary: Dict[str, Any] = Field(default_factory=dict)
    spot_summary: Dict[str, Any] = Field(default_factory=dict)
    quality_summary: Dict[str, Any] = Field(default_factory=dict)
    perp_quality: List[Dict[str, Any]] = Field(default_factory=list)
    spot_quality: List[Dict[str, Any]] = Field(default_factory=list)
    perp_events: List[Dict[str, Any]] = Field(default_factory=list)
    spot_events: List[Dict[str, Any]] = Field(default_factory=list)
    thresholds: Dict[str, Any] = Field(default_factory=dict)
    data_meta: Optional[DataMetaModel] = None


class AddressPayloadModel(FlexibleApiModel):
    status: str
    error: Optional[str] = None
    coin: str
    address: str
    lookback_hours: int
    stream_enabled: bool
    presets: List[Dict[str, Any]] = Field(default_factory=list)
    lookback_options: List[int] = Field(default_factory=list)
    bundle: Dict[str, Any] = Field(default_factory=dict)
    stream: Dict[str, Any] = Field(default_factory=dict)


class HistoryPayloadModel(FlexibleApiModel):
    coin: str
    generated_at_ms: int
    days: int
    summary: Dict[str, Any] = Field(default_factory=dict)
    filters: Dict[str, Any] = Field(default_factory=dict)
    signal_filters: Dict[str, Any] = Field(default_factory=dict)
    thresholds: Dict[str, Any] = Field(default_factory=dict)
    preferences: Dict[str, Any] = Field(default_factory=dict)
    recent_alerts: List[Dict[str, Any]] = Field(default_factory=list)
    recent_market: List[Dict[str, Any]] = Field(default_factory=list)
    recent_spot_market: List[Dict[str, Any]] = Field(default_factory=list)
    recent_events: List[Dict[str, Any]] = Field(default_factory=list)
    recent_quality: List[Dict[str, Any]] = Field(default_factory=list)
    recent_signals: List[Dict[str, Any]] = Field(default_factory=list)
    data_meta: Optional[DataMetaModel] = None


class ClearHistorySignalsPayloadModel(FlexibleApiModel):
    coin: str
    deleted: int
    days: int
    signal_kind: str
    signal_exchange: str
    exchange_keys: List[str] = Field(default_factory=list)
    cleared_at_ms: int


class DebugPayloadModel(FlexibleApiModel):
    coin: str
    generated_at_ms: int
    filters: Dict[str, Any] = Field(default_factory=dict)
    request_health: List[Dict[str, Any]] = Field(default_factory=list)
    perp_snapshots: List[Dict[str, Any]] = Field(default_factory=list)
    spot_snapshots: List[Dict[str, Any]] = Field(default_factory=list)
    data_meta: Optional[DataMetaModel] = None


class HealthPayloadModel(FlexibleApiModel):
    coin: str
    generated_at_ms: int
    filters: Dict[str, Any] = Field(default_factory=dict)
    cards: List[CardModel] = Field(default_factory=list)
    panels: List[PanelModel] = Field(default_factory=list)
    rest: List[Dict[str, Any]] = Field(default_factory=list)
    transport: List[Dict[str, Any]] = Field(default_factory=list)
    catalog: Dict[str, Any] = Field(default_factory=dict)
    history_summary: Dict[str, Any] = Field(default_factory=dict)
    data_meta: Optional[DataMetaModel] = None


class RuntimeManagerSessionModel(FlexibleApiModel):
    coin: str
    created_at_ms: int
    last_access_at_ms: int
    session_age_s: float
    cache_entries: int
    active_warm_jobs: int
    active_address_streams: int


class PrecomputeTaskModel(FlexibleApiModel):
    task: str
    status: str
    duration_ms: float
    cards: int = 0
    panels: int = 0
    revision: Optional[Any] = None
    error: Optional[str] = None


class PrecomputeCoinStatusModel(FlexibleApiModel):
    coin: str
    status: str
    started_at_ms: Optional[int] = None
    finished_at_ms: Optional[int] = None
    duration_ms: Optional[int] = None
    last_error: Optional[str] = None
    tasks: List[PrecomputeTaskModel] = Field(default_factory=list)
    runtime: Dict[str, Any] = Field(default_factory=dict)


class PrecomputeStateModel(FlexibleApiModel):
    enabled: bool
    active: bool
    interval_s: int
    workers: int
    run_count: int
    failure_count: int
    last_cycle_started_ms: Optional[int] = None
    last_cycle_finished_ms: Optional[int] = None
    last_cycle_duration_ms: Optional[int] = None
    coins: List[PrecomputeCoinStatusModel] = Field(default_factory=list)


class RuntimeManagerPayloadModel(FlexibleApiModel):
    manager_started_at_ms: int
    hot_coins: List[str] = Field(default_factory=list)
    session_count: int
    catalog_cached_at_ms: Optional[int] = None
    precompute: PrecomputeStateModel
    sessions: List[Dict[str, Any]] = Field(default_factory=list)
