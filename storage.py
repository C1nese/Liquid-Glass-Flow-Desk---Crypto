from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import threading
import time
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import pandas as pd

from models import ExchangeSnapshot, LiquidationEvent, OrderBookQualityPoint, RecordedMarketEvent, SpotSnapshot, TradeEvent


def _json_dumps(value: Any) -> str:
    return json.dumps(value or {}, ensure_ascii=True, separators=(",", ":"))


def _env_flag(name: str, default: bool) -> bool:
    raw = str(os.getenv(name, "1" if default else "0") or "").strip().lower()
    return raw in {"1", "true", "yes", "on"}


def _empty_frame(columns: List[str]) -> pd.DataFrame:
    return pd.DataFrame(columns=columns)


def _safe_float(value: Any) -> Optional[float]:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


class TerminalHistoryStore:
    def __init__(self, base_dir: Path | None = None) -> None:
        self.base_dir = base_dir or Path(__file__).with_name(".terminal_data")
        self.base_dir.mkdir(parents=True, exist_ok=True)
        self.archive_dir = self.base_dir.joinpath("archive")
        self.archive_dir.mkdir(parents=True, exist_ok=True)
        self.db_path = self.base_dir.joinpath("terminal_history.sqlite3")
        self.persist_snapshot_raw = _env_flag("LGFD_HISTORY_SNAPSHOT_RAW", False)
        self.persist_event_raw = _env_flag("LGFD_HISTORY_EVENT_RAW", False)
        self.persist_alert_payload = _env_flag("LGFD_HISTORY_ALERT_PAYLOAD", True)
        self.persist_signal_payload = _env_flag("LGFD_HISTORY_SIGNAL_PAYLOAD", True)
        self._write_lock = threading.Lock()
        self._describe_cache_lock = threading.Lock()
        self._describe_cache: Dict[str, Any] | None = None
        self._describe_cached_at = 0.0
        self._ensure_schema()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.db_path, timeout=15.0)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("PRAGMA synchronous=NORMAL")
        connection.execute("PRAGMA busy_timeout=5000")
        connection.execute("PRAGMA temp_store=MEMORY")
        connection.execute("PRAGMA wal_autocheckpoint=1000")
        connection.execute("PRAGMA journal_size_limit=67108864")
        return connection

    def _ensure_schema(self) -> None:
        def writer(connection: sqlite3.Connection) -> None:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS market_snapshots (
                    ts_ms INTEGER NOT NULL,
                    coin TEXT NOT NULL,
                    exchange_key TEXT NOT NULL,
                    exchange_name TEXT,
                    market TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    last_price REAL,
                    mark_price REAL,
                    index_price REAL,
                    funding_bps REAL,
                    open_interest REAL,
                    open_interest_notional REAL,
                    volume_24h_notional REAL,
                    status TEXT,
                    error TEXT,
                    raw_json TEXT,
                    PRIMARY KEY (ts_ms, coin, exchange_key, market, symbol)
                );

                CREATE TABLE IF NOT EXISTS alert_events (
                    ts_ms INTEGER NOT NULL,
                    coin TEXT NOT NULL,
                    exchange_name TEXT NOT NULL,
                    exchange_key TEXT,
                    symbol TEXT,
                    level TEXT,
                    alert TEXT NOT NULL,
                    action TEXT NOT NULL,
                    explanation TEXT,
                    payload_json TEXT,
                    PRIMARY KEY (ts_ms, coin, exchange_name, alert, action)
                );

                CREATE TABLE IF NOT EXISTS market_events (
                    event_key TEXT PRIMARY KEY,
                    ts_ms INTEGER NOT NULL,
                    coin TEXT NOT NULL,
                    exchange_key TEXT NOT NULL,
                    exchange_name TEXT,
                    market TEXT NOT NULL,
                    category TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    side TEXT,
                    price REAL,
                    size REAL,
                    notional REAL,
                    source TEXT,
                    raw_json TEXT
                );

                CREATE TABLE IF NOT EXISTS orderbook_quality_points (
                    point_key TEXT PRIMARY KEY,
                    ts_ms INTEGER NOT NULL,
                    coin TEXT NOT NULL,
                    exchange_key TEXT NOT NULL,
                    exchange_name TEXT,
                    market TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    added_notional REAL,
                    canceled_notional REAL,
                    net_notional REAL,
                    near_added_notional REAL,
                    near_canceled_notional REAL,
                    spoof_events INTEGER,
                    refill_events INTEGER,
                    bid_wall_persistence_s REAL,
                    ask_wall_persistence_s REAL,
                    imbalance_pct REAL,
                    best_bid REAL,
                    best_ask REAL
                );

                CREATE TABLE IF NOT EXISTS orderbook_quality_1m (
                    agg_key TEXT PRIMARY KEY,
                    bucket_ms INTEGER NOT NULL,
                    coin TEXT NOT NULL,
                    exchange_key TEXT NOT NULL,
                    exchange_name TEXT,
                    market TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    sample_count INTEGER,
                    added_notional REAL,
                    canceled_notional REAL,
                    net_notional REAL,
                    near_added_notional REAL,
                    near_canceled_notional REAL,
                    spoof_events INTEGER,
                    refill_events INTEGER,
                    bid_wall_persistence_s REAL,
                    ask_wall_persistence_s REAL,
                    imbalance_pct REAL,
                    imbalance_abs_max REAL,
                    best_bid REAL,
                    best_ask REAL
                );

                CREATE TABLE IF NOT EXISTS signal_events (
                    signal_key TEXT PRIMARY KEY,
                    ts_ms INTEGER NOT NULL,
                    coin TEXT NOT NULL,
                    exchange_key TEXT,
                    exchange_name TEXT,
                    kind TEXT NOT NULL,
                    label TEXT,
                    text TEXT NOT NULL,
                    score REAL,
                    anchor TEXT,
                    payload_json TEXT
                );

                CREATE TABLE IF NOT EXISTS oi_points (
                    point_key TEXT PRIMARY KEY,
                    ts_ms INTEGER NOT NULL,
                    coin TEXT NOT NULL,
                    exchange_key TEXT NOT NULL,
                    exchange_name TEXT,
                    symbol TEXT NOT NULL,
                    open_interest REAL,
                    open_interest_notional REAL
                );

                CREATE TABLE IF NOT EXISTS transport_state (
                    state_key TEXT PRIMARY KEY,
                    ts_ms INTEGER NOT NULL,
                    coin TEXT NOT NULL,
                    exchange_key TEXT NOT NULL,
                    market TEXT NOT NULL,
                    sync_state TEXT,
                    snapshot_timestamp_ms INTEGER,
                    trade_timestamp_ms INTEGER,
                    orderbook_levels INTEGER,
                    bootstrap_retry_at_ms INTEGER,
                    bootstrap_error TEXT
                );

                CREATE TABLE IF NOT EXISTS market_bars_1m (
                    bar_key TEXT PRIMARY KEY,
                    bucket_ms INTEGER NOT NULL,
                    coin TEXT NOT NULL,
                    exchange_key TEXT NOT NULL,
                    market TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    open REAL,
                    high REAL,
                    low REAL,
                    close REAL,
                    volume_notional REAL,
                    large_trade_notional REAL,
                    trade_count INTEGER
                );

                CREATE INDEX IF NOT EXISTS idx_market_snapshots_coin_exchange_ts
                ON market_snapshots (coin, exchange_key, ts_ms);

                CREATE INDEX IF NOT EXISTS idx_market_snapshots_market_coin_exchange_ts
                ON market_snapshots (market, coin, exchange_key, ts_ms);

                CREATE INDEX IF NOT EXISTS idx_alert_events_coin_ts
                ON alert_events (coin, ts_ms);

                CREATE INDEX IF NOT EXISTS idx_market_events_coin_category_ts
                ON market_events (coin, category, ts_ms);

                CREATE INDEX IF NOT EXISTS idx_market_events_market_coin_exchange_ts
                ON market_events (market, coin, exchange_key, ts_ms);

                CREATE INDEX IF NOT EXISTS idx_orderbook_quality_coin_exchange_ts
                ON orderbook_quality_points (coin, exchange_key, ts_ms);

                CREATE INDEX IF NOT EXISTS idx_orderbook_quality_market_coin_exchange_ts
                ON orderbook_quality_points (market, coin, exchange_key, ts_ms);

                CREATE INDEX IF NOT EXISTS idx_orderbook_quality_1m_coin_exchange_bucket
                ON orderbook_quality_1m (coin, exchange_key, market, bucket_ms);

                CREATE INDEX IF NOT EXISTS idx_orderbook_quality_1m_market_coin_exchange_bucket
                ON orderbook_quality_1m (market, coin, exchange_key, bucket_ms);

                CREATE INDEX IF NOT EXISTS idx_signal_events_coin_kind_exchange_ts
                ON signal_events (coin, kind, exchange_key, ts_ms);

                CREATE INDEX IF NOT EXISTS idx_oi_points_coin_exchange_ts
                ON oi_points (coin, exchange_key, ts_ms);

                CREATE INDEX IF NOT EXISTS idx_transport_state_coin_market_exchange_ts
                ON transport_state (coin, market, exchange_key, ts_ms);

                CREATE INDEX IF NOT EXISTS idx_transport_state_coin_exchange_market_ts
                ON transport_state (coin, exchange_key, market, ts_ms);

                CREATE INDEX IF NOT EXISTS idx_market_bars_1m_coin_exchange_market_bucket
                ON market_bars_1m (coin, exchange_key, market, bucket_ms);

                CREATE INDEX IF NOT EXISTS idx_market_bars_1m_coin_market_exchange_bucket
                ON market_bars_1m (coin, market, exchange_key, bucket_ms);
                """
            )
            return None

        self._execute_write(writer)

    def _invalidate_describe_cache(self) -> None:
        with self._describe_cache_lock:
            self._describe_cache = None
            self._describe_cached_at = 0.0

    def _execute_write(self, writer, *, retries: int = 4, initial_retry_delay_s: float = 0.05) -> Any:
        attempts = max(int(retries), 0)
        last_error: sqlite3.OperationalError | None = None
        for attempt in range(attempts + 1):
            with self._write_lock:
                try:
                    with self._connect() as connection:
                        result = writer(connection)
                    self._invalidate_describe_cache()
                    return result
                except sqlite3.OperationalError as exc:
                    if "locked" not in str(exc).lower():
                        raise
                    last_error = exc
            if attempt >= attempts:
                break
            time.sleep(min(float(initial_retry_delay_s) * (2 ** attempt), 1.0))
        if last_error is not None:
            raise last_error
        return None

    def record_snapshots(self, coin: str, snapshots: Dict[str, ExchangeSnapshot | SpotSnapshot], *, market: str = "perp") -> int:
        rows: List[tuple] = []
        for exchange_key, snapshot in snapshots.items():
            timestamp_ms = int(snapshot.timestamp_ms or int(time.time() * 1000))
            rows.append(
                (
                    timestamp_ms,
                    str(coin or "").upper(),
                    str(exchange_key),
                    snapshot.exchange,
                    str(market),
                    snapshot.symbol,
                    snapshot.last_price,
                    getattr(snapshot, "mark_price", None),
                    getattr(snapshot, "index_price", None),
                    getattr(snapshot, "funding_bps", None),
                    getattr(snapshot, "open_interest", None),
                    getattr(snapshot, "open_interest_notional", None),
                    getattr(snapshot, "volume_24h_notional", None),
                    snapshot.status,
                    snapshot.error,
                    _json_dumps(getattr(snapshot, "raw", None)) if self.persist_snapshot_raw else None,
                )
            )
        if not rows:
            return 0
        def writer(connection: sqlite3.Connection) -> int:
            connection.executemany(
                """
                INSERT OR IGNORE INTO market_snapshots (
                    ts_ms, coin, exchange_key, exchange_name, market, symbol,
                    last_price, mark_price, index_price, funding_bps, open_interest,
                    open_interest_notional, volume_24h_notional, status, error, raw_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                rows,
            )
            return int(connection.total_changes)
        return int(self._execute_write(writer) or 0)

    def record_alert_timeline(
        self,
        coin: str,
        timeline_frame: pd.DataFrame,
        *,
        symbol_map: Dict[str, str] | None = None,
        exchange_title_map: Dict[str, str] | None = None,
    ) -> int:
        column_positions = {str(column): index for index, column in enumerate(timeline_frame.columns)}
        time_index = column_positions.get("时间")
        exchange_index = column_positions.get("交易所")
        level_index = column_positions.get("等级")
        alert_index = column_positions.get("告警")
        action_index = column_positions.get("动作")
        explanation_index = column_positions.get("说明")
        if time_index is not None:
            if timeline_frame.empty:
                return 0
            exchange_key_by_title = {str(title): str(key) for key, title in (exchange_title_map or {}).items()}
            normalized_coin = str(coin or "").upper()
            symbol_lookup = symbol_map or {}
            rows: List[tuple] = []
            for row in timeline_frame.itertuples(index=False, name=None):
                timestamp_value = row[time_index]
                if timestamp_value is None or pd.isna(timestamp_value):
                    continue
                timestamp_ms = int(pd.Timestamp(timestamp_value).timestamp() * 1000)
                exchange_name = str((row[exchange_index] if exchange_index is not None else None) or "未知")
                exchange_key = exchange_key_by_title.get(exchange_name)
                symbol = symbol_lookup.get(exchange_key or "", None)
                level = row[level_index] if level_index is not None else None
                alert = row[alert_index] if alert_index is not None else None
                action = row[action_index] if action_index is not None else None
                explanation = row[explanation_index] if explanation_index is not None else None
                payload = {
                    "level": level,
                    "alert": alert,
                    "action": action,
                    "explanation": explanation,
                }
                rows.append(
                    (
                        timestamp_ms,
                        normalized_coin,
                        exchange_name,
                        exchange_key,
                        symbol,
                        level,
                        alert,
                        action,
                        explanation,
                        _json_dumps(payload) if self.persist_alert_payload else None,
                    )
                )
            if not rows:
                return 0
            def writer(connection: sqlite3.Connection) -> int:
                connection.executemany(
                    """
                    INSERT OR IGNORE INTO alert_events (
                        ts_ms, coin, exchange_name, exchange_key, symbol,
                        level, alert, action, explanation, payload_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    rows,
                )
                return int(connection.total_changes)
            return int(self._execute_write(writer) or 0)
        if timeline_frame.empty:
            return 0
        exchange_key_by_title = {str(title): str(key) for key, title in (exchange_title_map or {}).items()}
        rows: List[tuple] = []
        for row in timeline_frame.to_dict("records"):
            timestamp_value = row.get("时间")
            if timestamp_value is None or pd.isna(timestamp_value):
                continue
            timestamp_ms = int(pd.Timestamp(timestamp_value).timestamp() * 1000)
            exchange_name = str(row.get("交易所") or "未知")
            exchange_key = exchange_key_by_title.get(exchange_name)
            symbol = (symbol_map or {}).get(exchange_key or "", None)
            payload = {
                "level": row.get("等级"),
                "alert": row.get("告警"),
                "action": row.get("动作"),
                "explanation": row.get("说明"),
            }
            rows.append(
                (
                    timestamp_ms,
                    str(coin or "").upper(),
                    exchange_name,
                    exchange_key,
                    symbol,
                    row.get("等级"),
                    row.get("告警"),
                    row.get("动作"),
                    row.get("说明"),
                    _json_dumps(payload) if self.persist_alert_payload else None,
                )
            )
        if not rows:
            return 0
        def writer(connection: sqlite3.Connection) -> int:
            connection.executemany(
                """
                INSERT OR IGNORE INTO alert_events (
                    ts_ms, coin, exchange_name, exchange_key, symbol,
                    level, alert, action, explanation, payload_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                rows,
            )
            return int(connection.total_changes)
        return int(self._execute_write(writer) or 0)

    def load_snapshot_rows(
        self,
        *,
        coin: str,
        exchange_keys: Iterable[str] | None = None,
        market: str = "perp",
        since_ms: int | None = None,
        limit: int = 240,
    ) -> pd.DataFrame:
        where = ["coin = ?", "market = ?"]
        params: List[Any] = [str(coin).upper(), str(market).lower()]
        selected_keys = [str(item).lower() for item in (exchange_keys or []) if str(item)]
        if selected_keys:
            placeholders = ",".join("?" for _ in selected_keys)
            where.append(f"exchange_key IN ({placeholders})")
            params.extend(selected_keys)
        if since_ms is not None:
            where.append("ts_ms >= ?")
            params.append(int(since_ms))
        sql = (
            "SELECT ts_ms, coin, exchange_key, exchange_name, symbol, last_price, mark_price, index_price, "
            "funding_bps, open_interest, open_interest_notional, volume_24h_notional, status, error "
            "FROM market_snapshots WHERE "
            + " AND ".join(where)
            + " ORDER BY ts_ms DESC LIMIT ?"
        )
        params.append(int(limit))
        with self._connect() as connection:
            return pd.read_sql_query(sql, connection, params=params)

    def record_market_events(
        self,
        coin: str,
        events_by_exchange: Dict[str, List[TradeEvent | LiquidationEvent]],
        *,
        category: str,
        exchange_title_map: Dict[str, str] | None = None,
        market: str = "perp",
    ) -> int:
        rows: List[tuple] = []
        for exchange_key, events in events_by_exchange.items():
            exchange_name = str((exchange_title_map or {}).get(exchange_key) or exchange_key)
            for event in events or []:
                event_key = "::".join(
                    [
                        str(category),
                        str(market),
                        str(exchange_key),
                        str(event.symbol),
                        str(event.timestamp_ms),
                        str(event.side or ""),
                        f"{float(event.price or 0.0):.8f}",
                        f"{float(event.size or 0.0):.8f}",
                    ]
                )
                rows.append(
                    (
                        event_key,
                        int(event.timestamp_ms),
                        str(coin or "").upper(),
                        str(exchange_key),
                        exchange_name,
                        str(market),
                        str(category),
                        str(event.symbol),
                        event.side,
                        event.price,
                        event.size,
                        event.notional,
                        getattr(event, "source", None),
                        _json_dumps(getattr(event, "raw", None)) if self.persist_event_raw else None,
                    )
                )
        if not rows:
            return 0
        def writer(connection: sqlite3.Connection) -> int:
            connection.executemany(
                """
                INSERT OR IGNORE INTO market_events (
                    event_key, ts_ms, coin, exchange_key, exchange_name, market,
                    category, symbol, side, price, size, notional, source, raw_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                rows,
            )
            return int(connection.total_changes)
        return int(self._execute_write(writer) or 0)

    def record_recorded_events(
        self,
        coin: str,
        events_by_exchange: Dict[str, List[RecordedMarketEvent]],
    ) -> int:
        rows: List[tuple] = []
        for exchange_key, events in events_by_exchange.items():
            for event in events or []:
                unique_value = float(event.value or event.notional or 0.0)
                event_key = "::".join(
                    [
                        str(event.category or ""),
                        str(event.market or "perp"),
                        str(exchange_key),
                        str(event.symbol or ""),
                        str(event.timestamp_ms or 0),
                        str(event.side or ""),
                        f"{float(event.price or 0.0):.8f}",
                        f"{unique_value:.8f}",
                    ]
                )
                rows.append(
                    (
                        event_key,
                        int(event.timestamp_ms or 0),
                        str(coin or "").upper(),
                        str(exchange_key),
                        str(event.exchange or exchange_key),
                        str(event.market or "perp"),
                        str(event.category or "event"),
                        str(event.symbol or ""),
                        event.side,
                        event.price,
                        event.size,
                        event.notional,
                        event.label,
                        (
                            _json_dumps(
                                {
                                    "value": event.value,
                                    "label": event.label,
                                    "raw": getattr(event, "raw", None),
                                }
                            )
                            if self.persist_event_raw
                            else None
                        ),
                    )
                )
        if not rows:
            return 0
        def writer(connection: sqlite3.Connection) -> int:
            connection.executemany(
                """
                INSERT OR IGNORE INTO market_events (
                    event_key, ts_ms, coin, exchange_key, exchange_name, market,
                    category, symbol, side, price, size, notional, source, raw_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                rows,
            )
            return int(connection.total_changes)
        return int(self._execute_write(writer) or 0)

    def record_quality_points(
        self,
        coin: str,
        *,
        exchange_key: str,
        exchange_name: str,
        symbol: str,
        points: List[OrderBookQualityPoint],
        market: str = "perp",
    ) -> int:
        rows: List[tuple] = []
        for point in points or []:
            point_key = "::".join(
                [
                    str(market),
                    str(exchange_key),
                    str(symbol),
                    str(point.timestamp_ms),
                    f"{float(point.added_notional or 0.0):.4f}",
                    f"{float(point.canceled_notional or 0.0):.4f}",
                ]
            )
            rows.append(
                (
                    point_key,
                    int(point.timestamp_ms),
                    str(coin or "").upper(),
                    str(exchange_key),
                    str(exchange_name),
                    str(market),
                    str(symbol),
                    point.added_notional,
                    point.canceled_notional,
                    point.net_notional,
                    point.near_added_notional,
                    point.near_canceled_notional,
                    int(point.spoof_events or 0),
                    int(point.refill_events or 0),
                    point.bid_wall_persistence_s,
                    point.ask_wall_persistence_s,
                    point.imbalance_pct,
                    point.best_bid,
                    point.best_ask,
                )
            )
        if not rows:
            return 0
        def writer(connection: sqlite3.Connection) -> int:
            connection.executemany(
                """
                INSERT OR IGNORE INTO orderbook_quality_points (
                    point_key, ts_ms, coin, exchange_key, exchange_name, market, symbol,
                    added_notional, canceled_notional, net_notional, near_added_notional, near_canceled_notional,
                    spoof_events, refill_events, bid_wall_persistence_s, ask_wall_persistence_s,
                    imbalance_pct, best_bid, best_ask
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                rows,
            )
            return int(connection.total_changes)
        inserted = int(self._execute_write(writer) or 0)
        if inserted > 0:
            bucket_values = sorted({(int(point.timestamp_ms or 0) // 60_000) * 60_000 for point in points or [] if int(getattr(point, "timestamp_ms", 0) or 0) > 0})
            if bucket_values:
                self._refresh_quality_1m_buckets(
                    coin=str(coin or "").upper(),
                    exchange_key=str(exchange_key),
                    market=str(market),
                    symbol=str(symbol),
                    bucket_values=bucket_values,
                )
        return inserted

    def _refresh_quality_1m_buckets(
        self,
        *,
        coin: str,
        exchange_key: str,
        market: str,
        symbol: str,
        bucket_values: List[int],
    ) -> int:
        normalized_coin = str(coin or "").upper()
        normalized_exchange = str(exchange_key or "").lower()
        normalized_market = str(market or "").lower()
        normalized_symbol = str(symbol or "")
        selected_buckets = sorted({int(value) for value in (bucket_values or []) if int(value or 0) > 0})
        if not selected_buckets:
            return 0
        min_bucket = min(selected_buckets)
        max_bucket = max(selected_buckets) + 60_000
        placeholders = ",".join("?" for _ in selected_buckets)
        with self._connect() as connection:
            frame = pd.read_sql_query(
                """
                SELECT point_key, ts_ms, coin, exchange_key, exchange_name, market, symbol,
                       added_notional, canceled_notional, net_notional, near_added_notional, near_canceled_notional,
                       spoof_events, refill_events, bid_wall_persistence_s, ask_wall_persistence_s,
                       imbalance_pct, best_bid, best_ask
                FROM orderbook_quality_points
                WHERE coin = ?
                  AND exchange_key = ?
                  AND market = ?
                  AND symbol = ?
                  AND ts_ms >= ?
                  AND ts_ms < ?
                  AND ((ts_ms / 60000) * 60000) IN ("""
                + placeholders
                + """)
                ORDER BY ts_ms ASC
                """,
                connection,
                params=[
                    normalized_coin,
                    normalized_exchange,
                    normalized_market,
                    normalized_symbol,
                    int(min_bucket),
                    int(max_bucket),
                    *selected_buckets,
                ],
            )
        payload_rows: List[tuple] = []
        if not frame.empty:
            frame = frame.copy()
            frame["bucket_ms"] = (pd.to_numeric(frame["ts_ms"], errors="coerce").fillna(0).astype("int64") // 60_000) * 60_000
            for bucket_ms, bucket_frame in frame.groupby("bucket_ms", sort=False):
                latest_row = bucket_frame.sort_values("ts_ms", ascending=True).iloc[-1]
                agg_key = f"{normalized_coin}:{normalized_exchange}:{normalized_market}:{normalized_symbol}:{int(bucket_ms)}"
                payload_rows.append(
                    (
                        agg_key,
                        int(bucket_ms),
                        normalized_coin,
                        normalized_exchange,
                        str(latest_row.get("exchange_name") or normalized_exchange),
                        normalized_market,
                        normalized_symbol,
                        int(len(bucket_frame.index)),
                        float(bucket_frame["added_notional"].fillna(0.0).sum()),
                        float(bucket_frame["canceled_notional"].fillna(0.0).sum()),
                        float(bucket_frame["net_notional"].fillna(0.0).sum()),
                        float(bucket_frame["near_added_notional"].fillna(0.0).sum()),
                        float(bucket_frame["near_canceled_notional"].fillna(0.0).sum()),
                        int(bucket_frame["spoof_events"].fillna(0).sum()),
                        int(bucket_frame["refill_events"].fillna(0).sum()),
                        float(bucket_frame["bid_wall_persistence_s"].fillna(0.0).max()),
                        float(bucket_frame["ask_wall_persistence_s"].fillna(0.0).max()),
                        float(bucket_frame["imbalance_pct"].fillna(0.0).mean()),
                        float(bucket_frame["imbalance_pct"].fillna(0.0).abs().max()),
                        _safe_float(latest_row.get("best_bid")),
                        _safe_float(latest_row.get("best_ask")),
                    )
                )

        def writer(connection: sqlite3.Connection) -> int:
            deleted = connection.execute(
                """
                DELETE FROM orderbook_quality_1m
                WHERE coin = ?
                  AND exchange_key = ?
                  AND market = ?
                  AND symbol = ?
                  AND bucket_ms IN ("""
                + placeholders
                + """)
                """,
                [normalized_coin, normalized_exchange, normalized_market, normalized_symbol, *selected_buckets],
            ).rowcount or 0
            if payload_rows:
                connection.executemany(
                    """
                    INSERT OR REPLACE INTO orderbook_quality_1m (
                        agg_key, bucket_ms, coin, exchange_key, exchange_name, market, symbol,
                        sample_count, added_notional, canceled_notional, net_notional,
                        near_added_notional, near_canceled_notional, spoof_events, refill_events,
                        bid_wall_persistence_s, ask_wall_persistence_s, imbalance_pct, imbalance_abs_max,
                        best_bid, best_ask
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    payload_rows,
                )
            return int((deleted or 0) + len(payload_rows))

        return int(self._execute_write(writer) or 0)

    def load_alert_events(self, *, coin: str | None = None, since_ms: int | None = None, limit: int = 500) -> pd.DataFrame:
        where: List[str] = []
        params: List[Any] = []
        if coin:
            where.append("coin = ?")
            params.append(str(coin).upper())
        if since_ms is not None:
            where.append("ts_ms >= ?")
            params.append(int(since_ms))
        sql = "SELECT ts_ms, coin, exchange_name, exchange_key, symbol, level, alert, action, explanation FROM alert_events"
        if where:
            sql += " WHERE " + " AND ".join(where)
        sql += " ORDER BY ts_ms DESC LIMIT ?"
        params.append(int(limit))
        with self._connect() as connection:
            frame = pd.read_sql_query(sql, connection, params=params)
        if frame.empty:
            return _empty_frame(["时间", "币种", "交易所", "交易所键", "合约", "等级", "告警", "动作", "说明"])
        frame["时间"] = pd.to_datetime(frame["ts_ms"], unit="ms")
        return frame.rename(
            columns={
                "coin": "币种",
                "exchange_name": "交易所",
                "exchange_key": "交易所键",
                "symbol": "合约",
                "level": "等级",
                "alert": "告警",
                "action": "动作",
                "explanation": "说明",
            }
        )[["时间", "币种", "交易所", "交易所键", "合约", "等级", "告警", "动作", "说明"]]

    def load_market_history(
        self,
        *,
        coin: str | None = None,
        exchange_keys: Iterable[str] | None = None,
        market: str = "perp",
        since_ms: int | None = None,
        limit: int = 5000,
        include_archive: bool = False,
    ) -> pd.DataFrame:
        where = ["market = ?"]
        params: List[Any] = [market]
        if coin:
            where.append("coin = ?")
            params.append(str(coin).upper())
        selected_keys = [str(item) for item in (exchange_keys or []) if str(item)]
        if selected_keys:
            placeholders = ",".join("?" for _ in selected_keys)
            where.append(f"exchange_key IN ({placeholders})")
            params.extend(selected_keys)
        if since_ms is not None:
            where.append("ts_ms >= ?")
            params.append(int(since_ms))
        sql = (
            "SELECT ts_ms, coin, exchange_key, exchange_name, symbol, last_price, mark_price, index_price, "
            "funding_bps, open_interest, open_interest_notional, volume_24h_notional, status "
            "FROM market_snapshots WHERE "
            + " AND ".join(where)
            + " ORDER BY ts_ms DESC LIMIT ?"
        )
        params.append(int(limit))
        with self._connect() as connection:
            frame = pd.read_sql_query(sql, connection, params=params)
        if include_archive and since_ms is not None:
            archive_frame = self._load_archive_table_history(
                "market_snapshots",
                time_column="ts_ms",
                key_column=None,
                coin=coin,
                exchange_keys=exchange_keys,
                market=market,
                since_ms=since_ms,
                limit=limit,
            )
            if archive_frame is not None and not archive_frame.empty:
                frame = pd.concat([frame, archive_frame], ignore_index=True) if not frame.empty else archive_frame
                frame = frame.drop_duplicates(
                    subset=["ts_ms", "coin", "exchange_key", "market", "symbol"],
                    keep="last",
                )
                frame = frame.sort_values("ts_ms", ascending=False).head(int(limit)).reset_index(drop=True)
        if frame.empty:
            return _empty_frame(["时间", "币种", "交易所键", "交易所", "合约", "最新价", "标记价", "指数价", "Funding(bps)", "OI", "OI金额", "24h成交额", "状态"])
        frame["时间"] = pd.to_datetime(frame["ts_ms"], unit="ms")
        return frame.rename(
            columns={
                "coin": "币种",
                "exchange_key": "交易所键",
                "exchange_name": "交易所",
                "symbol": "合约",
                "last_price": "最新价",
                "mark_price": "标记价",
                "index_price": "指数价",
                "funding_bps": "Funding(bps)",
                "open_interest": "OI",
                "open_interest_notional": "OI金额",
                "volume_24h_notional": "24h成交额",
                "status": "状态",
            }
        )[["时间", "币种", "交易所键", "交易所", "合约", "最新价", "标记价", "指数价", "Funding(bps)", "OI", "OI金额", "24h成交额", "状态"]]

    def load_market_events(
        self,
        *,
        coin: str | None = None,
        category: str | None = None,
        exchange_keys: Iterable[str] | None = None,
        market: str | None = None,
        since_ms: int | None = None,
        limit: int = 3000,
        include_archive: bool = False,
    ) -> pd.DataFrame:
        where: List[str] = []
        params: List[Any] = []
        if coin:
            where.append("coin = ?")
            params.append(str(coin).upper())
        if category:
            where.append("category = ?")
            params.append(str(category))
        if market:
            where.append("market = ?")
            params.append(str(market))
        selected_keys = [str(item) for item in (exchange_keys or []) if str(item)]
        if selected_keys:
            placeholders = ",".join("?" for _ in selected_keys)
            where.append(f"exchange_key IN ({placeholders})")
            params.extend(selected_keys)
        if since_ms is not None:
            where.append("ts_ms >= ?")
            params.append(int(since_ms))
        sql = "SELECT ts_ms, coin, exchange_key, exchange_name, market, category, symbol, side, price, size, notional, source FROM market_events"
        if where:
            sql += " WHERE " + " AND ".join(where)
        sql += " ORDER BY ts_ms DESC LIMIT ?"
        params.append(int(limit))
        with self._connect() as connection:
            frame = pd.read_sql_query(sql, connection, params=params)
        if include_archive and since_ms is not None:
            archive_frame = self._load_archive_table_history(
                "market_events",
                time_column="ts_ms",
                key_column="event_key",
                coin=coin,
                exchange_keys=exchange_keys,
                market=market,
                since_ms=since_ms,
                limit=limit,
            )
            if archive_frame is not None and not archive_frame.empty:
                if category and "category" in archive_frame.columns:
                    archive_frame = archive_frame[archive_frame["category"].astype(str) == str(category)]
                frame = pd.concat([frame, archive_frame], ignore_index=True) if not frame.empty else archive_frame
                dedupe_subset = ["event_key"] if "event_key" in frame.columns else ["ts_ms", "coin", "exchange_key", "market", "category", "symbol"]
                frame = frame.drop_duplicates(subset=dedupe_subset, keep="last")
                frame = frame.sort_values("ts_ms", ascending=False).head(int(limit)).reset_index(drop=True)
        if frame.empty:
            return _empty_frame(["时间", "币种", "交易所键", "交易所", "市场", "类型", "合约", "方向", "价格", "数量", "名义金额", "来源"])
        frame["时间"] = pd.to_datetime(frame["ts_ms"], unit="ms")
        return frame.rename(
            columns={
                "coin": "币种",
                "exchange_key": "交易所键",
                "exchange_name": "交易所",
                "market": "市场",
                "category": "类型",
                "symbol": "合约",
                "side": "方向",
                "price": "价格",
                "size": "数量",
                "notional": "名义金额",
                "source": "来源",
            }
        )[["时间", "币种", "交易所键", "交易所", "市场", "类型", "合约", "方向", "价格", "数量", "名义金额", "来源"]]

    def load_quality_history(
        self,
        *,
        coin: str | None = None,
        exchange_keys: Iterable[str] | None = None,
        market: str = "perp",
        since_ms: int | None = None,
        limit: int = 2000,
        include_archive: bool = False,
    ) -> pd.DataFrame:
        where = ["market = ?"]
        params: List[Any] = [market]
        if coin:
            where.append("coin = ?")
            params.append(str(coin).upper())
        selected_keys = [str(item) for item in (exchange_keys or []) if str(item)]
        if selected_keys:
            placeholders = ",".join("?" for _ in selected_keys)
            where.append(f"exchange_key IN ({placeholders})")
            params.extend(selected_keys)
        if since_ms is not None:
            where.append("ts_ms >= ?")
            params.append(int(since_ms))
        sql = (
            "SELECT ts_ms, coin, exchange_key, exchange_name, market, symbol, added_notional, canceled_notional, net_notional, "
            "near_added_notional, near_canceled_notional, spoof_events, refill_events, bid_wall_persistence_s, ask_wall_persistence_s, "
            "imbalance_pct, best_bid, best_ask FROM orderbook_quality_points WHERE "
            + " AND ".join(where)
            + " ORDER BY ts_ms DESC LIMIT ?"
        )
        params.append(int(limit))
        with self._connect() as connection:
            frame = pd.read_sql_query(sql, connection, params=params)
        if include_archive and since_ms is not None and coin:
            archive_frame = self._load_quality_archive_history(
                coin=str(coin).upper(),
                exchange_keys=selected_keys,
                market=str(market).lower(),
                since_ms=int(since_ms),
                limit=int(limit),
            )
            if archive_frame is not None and not archive_frame.empty:
                frame = pd.concat([frame, archive_frame], ignore_index=True) if not frame.empty else archive_frame
                frame = (
                    frame.sort_values("ts_ms", ascending=False)
                    .drop_duplicates(subset=["coin", "exchange_key", "market", "symbol", "ts_ms", "added_notional", "canceled_notional"])
                    .head(int(limit))
                )
        if frame.empty:
            return _empty_frame(["时间", "币种", "交易所键", "交易所", "市场", "合约", "新增挂单额", "撤单额", "净变化", "近价新增", "近价撤单", "假挂单次数", "补单次数", "买墙持续(s)", "卖墙持续(s)", "盘口失衡(%)", "最优买价", "最优卖价"])
        frame["时间"] = pd.to_datetime(frame["ts_ms"], unit="ms")
        return frame.rename(
            columns={
                "coin": "币种",
                "exchange_key": "交易所键",
                "exchange_name": "交易所",
                "market": "市场",
                "symbol": "合约",
                "added_notional": "新增挂单额",
                "canceled_notional": "撤单额",
                "net_notional": "净变化",
                "near_added_notional": "近价新增",
                "near_canceled_notional": "近价撤单",
                "spoof_events": "假挂单次数",
                "refill_events": "补单次数",
                "bid_wall_persistence_s": "买墙持续(s)",
                "ask_wall_persistence_s": "卖墙持续(s)",
                "imbalance_pct": "盘口失衡(%)",
                "best_bid": "最优买价",
                "best_ask": "最优卖价",
            }
        )[["时间", "币种", "交易所键", "交易所", "市场", "合约", "新增挂单额", "撤单额", "净变化", "近价新增", "近价撤单", "假挂单次数", "补单次数", "买墙持续(s)", "卖墙持续(s)", "盘口失衡(%)", "最优买价", "最优卖价"]]

    def _load_quality_archive_history(
        self,
        *,
        coin: str,
        exchange_keys: List[str],
        market: str,
        since_ms: int,
        limit: int,
    ) -> pd.DataFrame:
        table_dir = self.archive_dir.joinpath("orderbook_quality_points")
        if not table_dir.exists():
            return pd.DataFrame()
        since_day = pd.to_datetime(int(since_ms), unit="ms").normalize()
        frames: List[pd.DataFrame] = []
        for path in sorted(table_dir.glob("orderbook_quality_points-*.*"), reverse=True):
            day_token = path.name.replace("orderbook_quality_points-", "").split(".", 1)[0]
            try:
                day_dt = pd.to_datetime(day_token, format="%Y-%m-%d", errors="raise")
            except Exception:
                continue
            if day_dt < since_day:
                continue
            try:
                if path.suffix.lower() == ".parquet":
                    frame = pd.read_parquet(path)
                else:
                    frame = pd.read_csv(path)
            except Exception:
                continue
            if frame.empty:
                continue
            frames.append(frame)
        if not frames:
            return pd.DataFrame()
        archive_frame = pd.concat(frames, ignore_index=True)
        archive_frame = archive_frame[archive_frame["coin"].astype(str).str.upper() == str(coin).upper()]
        archive_frame = archive_frame[archive_frame["market"].astype(str).str.lower() == str(market).lower()]
        if exchange_keys:
            normalized_keys = [str(item).lower() for item in exchange_keys]
            archive_frame = archive_frame[archive_frame["exchange_key"].astype(str).str.lower().isin(normalized_keys)]
        archive_frame = archive_frame[pd.to_numeric(archive_frame["ts_ms"], errors="coerce").fillna(0).astype("int64") >= int(since_ms)]
        if archive_frame.empty:
            return pd.DataFrame()
        return archive_frame.sort_values("ts_ms", ascending=False).head(int(limit)).reset_index(drop=True)

    def _load_archive_table_history(
        self,
        table_name: str,
        *,
        time_column: str,
        key_column: str | None = None,
        coin: str | None = None,
        exchange_keys: Iterable[str] | None = None,
        market: str | None = None,
        since_ms: int | None = None,
        limit: int = 2000,
    ) -> pd.DataFrame:
        table_dir = self.archive_dir.joinpath(table_name)
        if not table_dir.exists():
            return pd.DataFrame()
        frames: List[pd.DataFrame] = []
        if since_ms is not None:
            since_day = pd.to_datetime(int(since_ms), unit="ms").normalize()
        else:
            since_day = None
        for path in sorted(table_dir.glob(f"{table_name}-*.*"), reverse=True):
            if since_day is not None:
                day_token = path.name.replace(f"{table_name}-", "").split(".", 1)[0]
                try:
                    day_dt = pd.to_datetime(day_token, format="%Y-%m-%d", errors="raise")
                except Exception:
                    day_dt = None
                if day_dt is not None and day_dt < since_day:
                    continue
            try:
                frame = pd.read_parquet(path) if path.suffix.lower() == ".parquet" else pd.read_csv(path)
            except Exception:
                continue
            if frame.empty:
                continue
            frames.append(frame)
        if not frames:
            return pd.DataFrame()
        archive_frame = pd.concat(frames, ignore_index=True, sort=False)
        if coin and "coin" in archive_frame.columns:
            archive_frame = archive_frame[archive_frame["coin"].astype(str).str.upper() == str(coin).upper()]
        normalized_keys = [str(item).lower() for item in (exchange_keys or []) if str(item)]
        if normalized_keys and "exchange_key" in archive_frame.columns:
            archive_frame = archive_frame[archive_frame["exchange_key"].astype(str).str.lower().isin(normalized_keys)]
        if market and "market" in archive_frame.columns:
            archive_frame = archive_frame[archive_frame["market"].astype(str).str.lower() == str(market).lower()]
        if since_ms is not None and time_column in archive_frame.columns:
            archive_frame = archive_frame[
                pd.to_numeric(archive_frame[time_column], errors="coerce").fillna(0).astype("int64") >= int(since_ms)
            ]
        if archive_frame.empty:
            return pd.DataFrame()
        if key_column and key_column in archive_frame.columns:
            archive_frame = archive_frame.drop_duplicates(subset=[key_column], keep="last")
        return archive_frame.sort_values(time_column, ascending=False).head(int(limit)).reset_index(drop=True)

    def load_quality_history_1m(
        self,
        *,
        coin: str | None = None,
        exchange_keys: Iterable[str] | None = None,
        market: str = "perp",
        since_ms: int | None = None,
        limit: int = 2000,
    ) -> pd.DataFrame:
        where = ["market = ?"]
        params: List[Any] = [str(market).lower()]
        if coin:
            where.append("coin = ?")
            params.append(str(coin).upper())
        selected_keys = [str(item).lower() for item in (exchange_keys or []) if str(item)]
        if selected_keys:
            placeholders = ",".join("?" for _ in selected_keys)
            where.append(f"exchange_key IN ({placeholders})")
            params.extend(selected_keys)
        if since_ms is not None:
            where.append("bucket_ms >= ?")
            params.append(int(since_ms))
        sql = (
            "SELECT bucket_ms, coin, exchange_key, exchange_name, market, symbol, sample_count, added_notional, canceled_notional, "
            "net_notional, near_added_notional, near_canceled_notional, spoof_events, refill_events, bid_wall_persistence_s, "
            "ask_wall_persistence_s, imbalance_pct, imbalance_abs_max, best_bid, best_ask FROM orderbook_quality_1m WHERE "
            + " AND ".join(where)
            + " ORDER BY bucket_ms DESC LIMIT ?"
        )
        params.append(int(limit))
        with self._connect() as connection:
            frame = pd.read_sql_query(sql, connection, params=params)
        if frame.empty:
            return _empty_frame(["时间", "币种", "交易所键", "交易所", "市场", "合约", "样本数", "新增挂单额", "撤单额", "净变化", "近价新增", "近价撤单", "假挂单次数", "补单次数", "买墙持续(s)", "卖墙持续(s)", "盘口失衡(%)", "失衡峰值(%)", "最优买价", "最优卖价"])
        frame["时间"] = pd.to_datetime(frame["bucket_ms"], unit="ms")
        return frame.rename(
            columns={
                "coin": "币种",
                "exchange_key": "交易所键",
                "exchange_name": "交易所",
                "market": "市场",
                "symbol": "合约",
                "sample_count": "样本数",
                "added_notional": "新增挂单额",
                "canceled_notional": "撤单额",
                "net_notional": "净变化",
                "near_added_notional": "近价新增",
                "near_canceled_notional": "近价撤单",
                "spoof_events": "假挂单次数",
                "refill_events": "补单次数",
                "bid_wall_persistence_s": "买墙持续(s)",
                "ask_wall_persistence_s": "卖墙持续(s)",
                "imbalance_pct": "盘口失衡(%)",
                "imbalance_abs_max": "失衡峰值(%)",
                "best_bid": "最优买价",
                "best_ask": "最优卖价",
            }
        )[["时间", "币种", "交易所键", "交易所", "市场", "合约", "样本数", "新增挂单额", "撤单额", "净变化", "近价新增", "近价撤单", "假挂单次数", "补单次数", "买墙持续(s)", "卖墙持续(s)", "盘口失衡(%)", "失衡峰值(%)", "最优买价", "最优卖价"]]

    def rebuild_quality_1m_from_raw(self, *, since_ms: int | None = None) -> int:
        params: List[Any] = []
        sql = (
            "SELECT point_key, ts_ms, coin, exchange_key, exchange_name, market, symbol, "
            "added_notional, canceled_notional, net_notional, near_added_notional, near_canceled_notional, "
            "spoof_events, refill_events, bid_wall_persistence_s, ask_wall_persistence_s, imbalance_pct, best_bid, best_ask "
            "FROM orderbook_quality_points"
        )
        if since_ms is not None:
            sql += " WHERE ts_ms >= ?"
            params.append(int(since_ms))
        with self._connect() as connection:
            frame = pd.read_sql_query(sql, connection, params=params)
        if frame.empty:
            return 0
        frame["bucket_ms"] = (pd.to_numeric(frame["ts_ms"], errors="coerce").fillna(0).astype("int64") // 60_000) * 60_000
        payload_rows: List[tuple] = []
        grouped = frame.groupby(["coin", "exchange_key", "exchange_name", "market", "symbol", "bucket_ms"], sort=False)
        for (coin_value, exchange_key, exchange_name, market_value, symbol, bucket_ms), bucket_frame in grouped:
            latest_row = bucket_frame.sort_values("ts_ms", ascending=True).iloc[-1]
            agg_key = f"{coin_value}:{exchange_key}:{market_value}:{symbol}:{int(bucket_ms)}"
            payload_rows.append(
                (
                    agg_key,
                    int(bucket_ms),
                    str(coin_value),
                    str(exchange_key),
                    str(exchange_name),
                    str(market_value),
                    str(symbol),
                    int(len(bucket_frame.index)),
                    float(bucket_frame["added_notional"].fillna(0.0).sum()),
                    float(bucket_frame["canceled_notional"].fillna(0.0).sum()),
                    float(bucket_frame["net_notional"].fillna(0.0).sum()),
                    float(bucket_frame["near_added_notional"].fillna(0.0).sum()),
                    float(bucket_frame["near_canceled_notional"].fillna(0.0).sum()),
                    int(bucket_frame["spoof_events"].fillna(0).sum()),
                    int(bucket_frame["refill_events"].fillna(0).sum()),
                    float(bucket_frame["bid_wall_persistence_s"].fillna(0.0).max()),
                    float(bucket_frame["ask_wall_persistence_s"].fillna(0.0).max()),
                    float(bucket_frame["imbalance_pct"].fillna(0.0).mean()),
                    float(bucket_frame["imbalance_pct"].fillna(0.0).abs().max()),
                    _safe_float(latest_row.get("best_bid")),
                    _safe_float(latest_row.get("best_ask")),
                )
            )
        if not payload_rows:
            return 0
        def writer(connection: sqlite3.Connection) -> int:
            if since_ms is None:
                connection.execute("DELETE FROM orderbook_quality_1m")
            else:
                connection.execute("DELETE FROM orderbook_quality_1m WHERE bucket_ms >= ?", (int(since_ms),))
            connection.executemany(
                """
                INSERT OR REPLACE INTO orderbook_quality_1m (
                    agg_key, bucket_ms, coin, exchange_key, exchange_name, market, symbol,
                    sample_count, added_notional, canceled_notional, net_notional,
                    near_added_notional, near_canceled_notional, spoof_events, refill_events,
                    bid_wall_persistence_s, ask_wall_persistence_s, imbalance_pct, imbalance_abs_max,
                    best_bid, best_ask
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                payload_rows,
            )
            return int(len(payload_rows))
        return int(self._execute_write(writer) or 0)

    def record_signal_events(
        self,
        coin: str,
        items: Iterable[Dict[str, Any]],
        *,
        bucket_ms: int = 30_000,
    ) -> int:
        rows: List[tuple] = []
        normalized_coin = str(coin or "").upper()
        bucket_size = max(int(bucket_ms), 1)
        for item in items or []:
            if not isinstance(item, dict):
                continue
            text = str(item.get("text") or "").strip()
            if not text:
                continue
            kind = str(item.get("kind") or "signal").strip().lower() or "signal"
            label = str(item.get("label") or kind.upper()).strip() or kind.upper()
            exchange_key = str(item.get("exchange_key") or "").strip().lower()
            exchange_name = str(item.get("exchange") or item.get("exchange_name") or exchange_key or "").strip()
            score = item.get("score")
            anchor = str(item.get("anchor") or "").strip()
            timestamp_ms = int(item.get("timestamp_ms") or int(time.time() * 1000))
            bucket_timestamp_ms = (timestamp_ms // bucket_size) * bucket_size
            digest = hashlib.sha1(
                json.dumps(
                    [normalized_coin, exchange_key, kind, label, text],
                    ensure_ascii=True,
                    separators=(",", ":"),
                ).encode("utf-8")
            ).hexdigest()
            signal_key = f"{normalized_coin}::{kind}::{exchange_key or 'all'}::{bucket_timestamp_ms}::{digest}"
            payload = dict(item)
            payload["bucket_timestamp_ms"] = bucket_timestamp_ms
            rows.append(
                (
                    signal_key,
                    timestamp_ms,
                    normalized_coin,
                    exchange_key or None,
                    exchange_name or None,
                    kind,
                    label,
                    text,
                    float(score) if score not in (None, "") else None,
                    anchor or None,
                    _json_dumps(payload) if self.persist_signal_payload else None,
                )
            )
        if not rows:
            return 0
        def writer(connection: sqlite3.Connection) -> int:
            connection.executemany(
                """
                INSERT OR IGNORE INTO signal_events (
                    signal_key, ts_ms, coin, exchange_key, exchange_name,
                    kind, label, text, score, anchor, payload_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                rows,
            )
            return int(connection.total_changes)
        return int(self._execute_write(writer) or 0)

    def record_oi_points(self, coin: str, rows: Iterable[Dict[str, Any]]) -> int:
        payload_rows: List[tuple] = []
        normalized_coin = str(coin or "").upper()
        for row in rows or []:
            if not isinstance(row, dict):
                continue
            exchange_key = str(row.get("exchange_key") or "").strip().lower()
            symbol = str(row.get("symbol") or "").strip()
            ts_ms = row.get("ts_ms")
            if not exchange_key or not symbol or ts_ms in (None, ""):
                continue
            point_key = f"{normalized_coin}:{exchange_key}:{int(ts_ms)}:{symbol}"
            payload_rows.append(
                (
                    point_key,
                    int(ts_ms),
                    normalized_coin,
                    exchange_key,
                    row.get("exchange_name"),
                    symbol,
                    row.get("open_interest"),
                    row.get("open_interest_notional"),
                )
            )
        if not payload_rows:
            return 0
        def writer(connection: sqlite3.Connection) -> int:
            connection.executemany(
                """
                INSERT OR REPLACE INTO oi_points (
                    point_key, ts_ms, coin, exchange_key, exchange_name, symbol,
                    open_interest, open_interest_notional
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                payload_rows,
            )
            return int(connection.total_changes)
        return int(self._execute_write(writer) or 0)

    def load_oi_points(
        self,
        *,
        coin: str,
        exchange_keys: Iterable[str] | None = None,
        since_ms: int | None = None,
        limit: int = 2000,
        include_archive: bool = False,
    ) -> pd.DataFrame:
        where = ["coin = ?"]
        params: List[Any] = [str(coin).upper()]
        selected_keys = [str(item).lower() for item in (exchange_keys or []) if str(item)]
        if selected_keys:
            placeholders = ",".join("?" for _ in selected_keys)
            where.append(f"exchange_key IN ({placeholders})")
            params.extend(selected_keys)
        if since_ms is not None:
            where.append("ts_ms >= ?")
            params.append(int(since_ms))
        sql = "SELECT ts_ms, coin, exchange_key, exchange_name, symbol, open_interest, open_interest_notional FROM oi_points WHERE " + " AND ".join(where) + " ORDER BY ts_ms DESC LIMIT ?"
        params.append(int(limit))
        with self._connect() as connection:
            frame = pd.read_sql_query(sql, connection, params=params)
        if include_archive and since_ms is not None:
            archive_frame = self._load_archive_table_history(
                "oi_points",
                time_column="ts_ms",
                key_column="point_key",
                coin=coin,
                exchange_keys=exchange_keys,
                market=None,
                since_ms=since_ms,
                limit=limit,
            )
            if archive_frame is not None and not archive_frame.empty:
                frame = pd.concat([frame, archive_frame], ignore_index=True) if not frame.empty else archive_frame
                dedupe_subset = ["point_key"] if "point_key" in frame.columns else ["ts_ms", "coin", "exchange_key", "symbol"]
                frame = frame.drop_duplicates(subset=dedupe_subset, keep="last")
                frame = frame.sort_values("ts_ms", ascending=False).head(int(limit)).reset_index(drop=True)
        return frame

    def load_oi_points_window(
        self,
        *,
        coin: str,
        exchange_keys: Iterable[str] | None = None,
        since_ms: int | None = None,
        limit: int = 720,
        include_archive: bool = False,
        bucket_minutes: int = 5,
    ) -> pd.DataFrame:
        normalized_bucket_minutes = max(int(bucket_minutes or 1), 1)
        if normalized_bucket_minutes <= 1:
            return self.load_oi_points(
                coin=coin,
                exchange_keys=exchange_keys,
                since_ms=since_ms,
                limit=limit,
                include_archive=include_archive,
            )
        raw_limit = max(int(limit) * normalized_bucket_minutes * 4, 2400)
        frame = self.load_oi_points(
            coin=coin,
            exchange_keys=exchange_keys,
            since_ms=since_ms,
            limit=raw_limit,
            include_archive=include_archive,
        )
        if frame.empty:
            return _empty_frame(
                [
                    "bucket_ms",
                    "coin",
                    "exchange_key",
                    "exchange_name",
                    "symbol",
                    "open_interest",
                    "open_interest_notional",
                    "sample_count",
                ]
            )
        working = frame.copy()
        working["ts_ms"] = pd.to_numeric(working.get("ts_ms"), errors="coerce")
        working = working.dropna(subset=["ts_ms"])
        bucket_size_ms = normalized_bucket_minutes * 60_000
        working["bucket_ms"] = (working["ts_ms"].astype("int64") // bucket_size_ms) * bucket_size_ms
        aggregated = (
            working.sort_values("ts_ms", ascending=True)
            .groupby(["bucket_ms", "coin", "exchange_key", "exchange_name", "symbol"], as_index=False)
            .agg(
                open_interest=("open_interest", "last"),
                open_interest_notional=("open_interest_notional", "last"),
                sample_count=("ts_ms", "size"),
            )
            .sort_values("bucket_ms", ascending=False, na_position="last")
            .head(int(limit))
            .reset_index(drop=True)
        )
        return aggregated

    def record_transport_states(self, coin: str, rows: Iterable[Dict[str, Any]]) -> int:
        payload_rows: List[tuple] = []
        normalized_coin = str(coin or "").upper()
        for row in rows or []:
            if not isinstance(row, dict):
                continue
            exchange_key = str(row.get("exchange_key") or "").strip().lower()
            market = str(row.get("market") or "").strip().lower()
            ts_ms = row.get("ts_ms") or row.get("snapshot_timestamp_ms") or int(time.time() * 1000)
            if not exchange_key or not market:
                continue
            state_key = f"{normalized_coin}:{exchange_key}:{market}:{int(ts_ms)}"
            payload_rows.append(
                (
                    state_key,
                    int(ts_ms),
                    normalized_coin,
                    exchange_key,
                    market,
                    row.get("sync_state"),
                    row.get("snapshot_timestamp_ms"),
                    row.get("trade_timestamp_ms"),
                    row.get("orderbook_levels"),
                    row.get("bootstrap_retry_at_ms"),
                    row.get("bootstrap_error"),
                )
            )
        if not payload_rows:
            return 0
        def writer(connection: sqlite3.Connection) -> int:
            connection.executemany(
                """
                INSERT OR REPLACE INTO transport_state (
                    state_key, ts_ms, coin, exchange_key, market, sync_state,
                    snapshot_timestamp_ms, trade_timestamp_ms, orderbook_levels,
                    bootstrap_retry_at_ms, bootstrap_error
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                payload_rows,
            )
            return int(connection.total_changes)
        return int(self._execute_write(writer) or 0)

    def load_transport_states(
        self,
        *,
        coin: str,
        exchange_keys: Iterable[str] | None = None,
        market: str | None = None,
        since_ms: int | None = None,
        limit: int = 2000,
        include_archive: bool = False,
    ) -> pd.DataFrame:
        where = ["coin = ?"]
        params: List[Any] = [str(coin).upper()]
        selected_keys = [str(item).lower() for item in (exchange_keys or []) if str(item)]
        if selected_keys:
            placeholders = ",".join("?" for _ in selected_keys)
            where.append(f"exchange_key IN ({placeholders})")
            params.extend(selected_keys)
        if market:
            where.append("market = ?")
            params.append(str(market).lower())
        if since_ms is not None:
            where.append("ts_ms >= ?")
            params.append(int(since_ms))
        sql = "SELECT ts_ms, coin, exchange_key, market, sync_state, snapshot_timestamp_ms, trade_timestamp_ms, orderbook_levels, bootstrap_retry_at_ms, bootstrap_error FROM transport_state WHERE " + " AND ".join(where) + " ORDER BY ts_ms DESC LIMIT ?"
        params.append(int(limit))
        with self._connect() as connection:
            frame = pd.read_sql_query(sql, connection, params=params)
        if include_archive and since_ms is not None:
            archive_frame = self._load_archive_table_history(
                "transport_state",
                time_column="ts_ms",
                key_column="state_key",
                coin=coin,
                exchange_keys=exchange_keys,
                market=market,
                since_ms=since_ms,
                limit=limit,
            )
            if archive_frame is not None and not archive_frame.empty:
                frame = pd.concat([frame, archive_frame], ignore_index=True) if not frame.empty else archive_frame
                dedupe_subset = ["state_key"] if "state_key" in frame.columns else ["coin", "exchange_key", "market", "ts_ms"]
                frame = frame.drop_duplicates(subset=dedupe_subset, keep="last")
                frame = frame.sort_values("ts_ms", ascending=False).head(int(limit)).reset_index(drop=True)
        return frame

    def load_transport_states_latest(
        self,
        *,
        coin: str,
        exchange_keys: Iterable[str] | None = None,
        market: str | None = None,
        since_ms: int | None = None,
        include_archive: bool = False,
    ) -> pd.DataFrame:
        raw_limit = max(512, len([item for item in (exchange_keys or []) if str(item)]) * 48)
        frame = self.load_transport_states(
            coin=coin,
            exchange_keys=exchange_keys,
            market=market,
            since_ms=since_ms,
            limit=raw_limit,
            include_archive=include_archive,
        )
        if frame.empty:
            return frame
        working = frame.copy()
        working["ts_ms"] = pd.to_numeric(working.get("ts_ms"), errors="coerce")
        working = working.dropna(subset=["ts_ms"])
        return (
            working.sort_values("ts_ms", ascending=False, na_position="last")
            .drop_duplicates(subset=["coin", "exchange_key", "market"], keep="first")
            .reset_index(drop=True)
        )

    def record_market_bars_1m(self, coin: str, rows: Iterable[Dict[str, Any]]) -> int:
        payload_rows: List[tuple] = []
        normalized_coin = str(coin or "").upper()
        for row in rows or []:
            if not isinstance(row, dict):
                continue
            exchange_key = str(row.get("exchange_key") or "").strip().lower()
            market = str(row.get("market") or "").strip().lower()
            symbol = str(row.get("symbol") or "").strip()
            bucket_ms = row.get("bucket_ms")
            if not exchange_key or not market or not symbol or bucket_ms in (None, ""):
                continue
            bar_key = f"{normalized_coin}:{exchange_key}:{market}:{symbol}:{int(bucket_ms)}"
            payload_rows.append(
                (
                    bar_key,
                    int(bucket_ms),
                    normalized_coin,
                    exchange_key,
                    market,
                    symbol,
                    row.get("open"),
                    row.get("high"),
                    row.get("low"),
                    row.get("close"),
                    row.get("volume_notional"),
                    row.get("large_trade_notional"),
                    row.get("trade_count"),
                )
            )
        if not payload_rows:
            return 0
        def writer(connection: sqlite3.Connection) -> int:
            connection.executemany(
                """
                INSERT OR REPLACE INTO market_bars_1m (
                    bar_key, bucket_ms, coin, exchange_key, market, symbol,
                    open, high, low, close, volume_notional, large_trade_notional, trade_count
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                payload_rows,
            )
            return int(connection.total_changes)
        return int(self._execute_write(writer) or 0)

    def load_market_bars_1m(
        self,
        *,
        coin: str,
        exchange_keys: Iterable[str] | None = None,
        market: str | None = None,
        since_ms: int | None = None,
        limit: int = 2000,
        include_archive: bool = False,
    ) -> pd.DataFrame:
        where = ["coin = ?"]
        params: List[Any] = [str(coin).upper()]
        selected_keys = [str(item).lower() for item in (exchange_keys or []) if str(item)]
        if selected_keys:
            placeholders = ",".join("?" for _ in selected_keys)
            where.append(f"exchange_key IN ({placeholders})")
            params.extend(selected_keys)
        if market:
            where.append("market = ?")
            params.append(str(market).lower())
        if since_ms is not None:
            where.append("bucket_ms >= ?")
            params.append(int(since_ms))
        sql = "SELECT bucket_ms, coin, exchange_key, market, symbol, open, high, low, close, volume_notional, large_trade_notional, trade_count FROM market_bars_1m WHERE " + " AND ".join(where) + " ORDER BY bucket_ms DESC LIMIT ?"
        params.append(int(limit))
        with self._connect() as connection:
            frame = pd.read_sql_query(sql, connection, params=params)
        if include_archive and since_ms is not None:
            archive_frame = self._load_archive_table_history(
                "market_bars_1m",
                time_column="bucket_ms",
                key_column="bar_key",
                coin=coin,
                exchange_keys=exchange_keys,
                market=market,
                since_ms=since_ms,
                limit=limit,
            )
            if archive_frame is not None and not archive_frame.empty:
                frame = pd.concat([frame, archive_frame], ignore_index=True) if not frame.empty else archive_frame
                dedupe_subset = ["bar_key"] if "bar_key" in frame.columns else ["bucket_ms", "coin", "exchange_key", "market", "symbol"]
                frame = frame.drop_duplicates(subset=dedupe_subset, keep="last")
                frame = frame.sort_values("bucket_ms", ascending=False).head(int(limit)).reset_index(drop=True)
        return frame

    def load_market_bars_window(
        self,
        *,
        coin: str,
        exchange_keys: Iterable[str] | None = None,
        market: str | None = None,
        since_ms: int | None = None,
        limit: int = 1440,
        include_archive: bool = False,
        bucket_minutes: int = 5,
    ) -> pd.DataFrame:
        normalized_bucket_minutes = max(int(bucket_minutes or 1), 1)
        if normalized_bucket_minutes <= 1:
            return self.load_market_bars_1m(
                coin=coin,
                exchange_keys=exchange_keys,
                market=market,
                since_ms=since_ms,
                limit=limit,
                include_archive=include_archive,
            )
        raw_limit = max(int(limit) * normalized_bucket_minutes * 4, 4000)
        frame = self.load_market_bars_1m(
            coin=coin,
            exchange_keys=exchange_keys,
            market=market,
            since_ms=since_ms,
            limit=raw_limit,
            include_archive=include_archive,
        )
        if frame.empty:
            return _empty_frame(
                [
                    "bucket_ms",
                    "coin",
                    "exchange_key",
                    "market",
                    "symbol",
                    "open",
                    "high",
                    "low",
                    "close",
                    "volume_notional",
                    "large_trade_notional",
                    "trade_count",
                    "sample_count",
                ]
            )
        working = frame.copy()
        working["bucket_ms"] = pd.to_numeric(working.get("bucket_ms"), errors="coerce")
        working = working.dropna(subset=["bucket_ms"])
        bucket_size_ms = normalized_bucket_minutes * 60_000
        working["window_bucket_ms"] = (working["bucket_ms"].astype("int64") // bucket_size_ms) * bucket_size_ms
        aggregated = (
            working.sort_values("bucket_ms", ascending=True)
            .groupby(["window_bucket_ms", "coin", "exchange_key", "market", "symbol"], as_index=False)
            .agg(
                open=("open", "first"),
                high=("high", "max"),
                low=("low", "min"),
                close=("close", "last"),
                volume_notional=("volume_notional", "sum"),
                large_trade_notional=("large_trade_notional", "sum"),
                trade_count=("trade_count", "sum"),
                sample_count=("bucket_ms", "size"),
            )
            .rename(columns={"window_bucket_ms": "bucket_ms"})
            .sort_values("bucket_ms", ascending=False, na_position="last")
            .head(int(limit))
            .reset_index(drop=True)
        )
        return aggregated

    def load_signal_events(
        self,
        *,
        coin: str | None = None,
        kind: str | None = None,
        exchange_key: str | None = None,
        since_ms: int | None = None,
        limit: int = 500,
    ) -> pd.DataFrame:
        where: List[str] = []
        params: List[Any] = []
        if coin:
            where.append("coin = ?")
            params.append(str(coin).upper())
        if kind:
            where.append("kind = ?")
            params.append(str(kind).lower())
        if exchange_key:
            where.append("exchange_key = ?")
            params.append(str(exchange_key).lower())
        if since_ms is not None:
            where.append("ts_ms >= ?")
            params.append(int(since_ms))
        sql = "SELECT signal_key, ts_ms, coin, exchange_key, exchange_name, kind, label, text, score, anchor FROM signal_events"
        if where:
            sql += " WHERE " + " AND ".join(where)
        sql += " ORDER BY ts_ms DESC LIMIT ?"
        params.append(int(limit))
        with self._connect() as connection:
            frame = pd.read_sql_query(sql, connection, params=params)
        if frame.empty:
            return _empty_frame(["signal_key", "time", "coin", "exchange_key", "exchange", "kind", "label", "text", "score", "anchor"])
        frame["time"] = pd.to_datetime(frame["ts_ms"], unit="ms")
        return frame.rename(
            columns={
                "signal_key": "signal_key",
                "coin": "coin",
                "exchange_key": "exchange_key",
                "exchange_name": "exchange",
                "kind": "kind",
                "label": "label",
                "text": "text",
                "score": "score",
                "anchor": "anchor",
            }
        )[["signal_key", "time", "coin", "exchange_key", "exchange", "kind", "label", "text", "score", "anchor"]]

    def clear_signal_events(
        self,
        *,
        coin: str | None = None,
        kind: str | None = None,
        exchange_key: str | None = None,
        since_ms: int | None = None,
    ) -> int:
        where: List[str] = []
        params: List[Any] = []
        if coin:
            where.append("coin = ?")
            params.append(str(coin).upper())
        if kind:
            where.append("kind = ?")
            params.append(str(kind).lower())
        if exchange_key:
            where.append("exchange_key = ?")
            params.append(str(exchange_key).lower())
        if since_ms is not None:
            where.append("ts_ms >= ?")
            params.append(int(since_ms))
        sql = "DELETE FROM signal_events"
        if where:
            sql += " WHERE " + " AND ".join(where)
        def writer(connection: sqlite3.Connection) -> int:
            cursor = connection.execute(sql, params)
            return int(cursor.rowcount or 0)
        return int(self._execute_write(writer) or 0)

    def delete_signal_events_by_keys(self, signal_keys: Iterable[str]) -> int:
        selected = [str(item) for item in (signal_keys or []) if str(item)]
        if not selected:
            return 0
        placeholders = ",".join("?" for _ in selected)
        sql = f"DELETE FROM signal_events WHERE signal_key IN ({placeholders})"
        def writer(connection: sqlite3.Connection) -> int:
            cursor = connection.execute(sql, selected)
            return int(cursor.rowcount or 0)
        return int(self._execute_write(writer) or 0)

    def compact_market_events(self, *, min_trade_notional: float = 10_000.0) -> int:
        threshold = max(float(min_trade_notional or 0.0), 0.0)
        def writer(connection: sqlite3.Connection) -> int:
            cursor = connection.execute(
                """
                DELETE FROM market_events
                WHERE category = 'trade'
                  AND ABS(COALESCE(notional, 0)) < ?
                """,
                (threshold,),
            )
            return int(cursor.rowcount or 0)
        return int(self._execute_write(writer) or 0)

    def compact_orderbook_quality_points(
        self,
        *,
        min_notional: float = 50_000.0,
        min_imbalance_pct: float = 12.0,
    ) -> int:
        notional_threshold = max(float(min_notional or 0.0), 0.0)
        imbalance_threshold = max(float(min_imbalance_pct or 0.0), 0.0)
        def writer(connection: sqlite3.Connection) -> int:
            cursor = connection.execute(
                """
                DELETE FROM orderbook_quality_points
                WHERE MAX(
                    ABS(COALESCE(added_notional, 0)),
                    ABS(COALESCE(canceled_notional, 0)),
                    ABS(COALESCE(near_added_notional, 0)),
                    ABS(COALESCE(near_canceled_notional, 0))
                ) < ?
                  AND ABS(COALESCE(imbalance_pct, 0)) < ?
                  AND COALESCE(spoof_events, 0) = 0
                  AND COALESCE(refill_events, 0) = 0
                """,
                (notional_threshold, imbalance_threshold),
            )
            return int(cursor.rowcount or 0)
        return int(self._execute_write(writer) or 0)

    def compact_duplicate_orderbook_quality_events(self, *, keep_recent_ms: int = 12 * 60 * 60 * 1000) -> int:
        retention_ms = max(int(keep_recent_ms or 0), 0)
        cutoff_ms = int(time.time() * 1000) + 1 if retention_ms <= 0 else int(time.time() * 1000) - retention_ms

        def writer(connection: sqlite3.Connection) -> int:
            cursor = connection.execute(
                """
                DELETE FROM market_events
                WHERE category = 'orderbook_quality'
                  AND ts_ms < ?
                """,
                (cutoff_ms,),
            )
            return int(cursor.rowcount or 0)

        return int(self._execute_write(writer) or 0)

    def archive_quality_points_before(self, cutoff_ms: int, *, prefer_parquet: bool = True) -> List[Dict[str, Any]]:
        cutoff_value = int(cutoff_ms)
        with self._connect() as connection:
            frame = pd.read_sql_query(
                """
                SELECT point_key, ts_ms, coin, exchange_key, exchange_name, market, symbol,
                       added_notional, canceled_notional, net_notional, near_added_notional, near_canceled_notional,
                       spoof_events, refill_events, bid_wall_persistence_s, ask_wall_persistence_s,
                       imbalance_pct, best_bid, best_ask
                FROM orderbook_quality_points
                WHERE ts_ms < ?
                ORDER BY ts_ms ASC
                """,
                connection,
                params=[cutoff_value],
            )
        if frame.empty:
            return []
        frame["archive_day"] = pd.to_datetime(frame["ts_ms"], unit="ms").dt.strftime("%Y-%m-%d")
        output_dir = self.archive_dir.joinpath("orderbook_quality_points")
        output_dir.mkdir(parents=True, exist_ok=True)
        archived: List[Dict[str, Any]] = []
        for day_label, day_frame in frame.groupby("archive_day"):
            clean_frame = day_frame.drop(columns=["archive_day"]).copy()
            parquet_path = output_dir.joinpath(f"orderbook_quality_points-{day_label}.parquet")
            csv_path = output_dir.joinpath(f"orderbook_quality_points-{day_label}.csv.gz")
            existing_frame = pd.DataFrame()
            read_path: Optional[Path] = None
            if parquet_path.exists():
                try:
                    existing_frame = pd.read_parquet(parquet_path)
                    read_path = parquet_path
                except Exception:
                    existing_frame = pd.DataFrame()
            elif csv_path.exists():
                try:
                    existing_frame = pd.read_csv(csv_path)
                    read_path = csv_path
                except Exception:
                    existing_frame = pd.DataFrame()
            merged_frame = pd.concat([existing_frame, clean_frame], ignore_index=True) if not existing_frame.empty else clean_frame
            if "point_key" in merged_frame.columns:
                merged_frame = merged_frame.drop_duplicates(subset=["point_key"], keep="last")
            written_path = parquet_path if prefer_parquet else csv_path
            if prefer_parquet:
                try:
                    merged_frame.to_parquet(parquet_path, index=False)
                    if csv_path.exists():
                        try:
                            csv_path.unlink()
                        except OSError:
                            pass
                except Exception:
                    written_path = csv_path
                    merged_frame.to_csv(csv_path, index=False, compression="gzip")
            else:
                merged_frame.to_csv(csv_path, index=False, compression="gzip")
                if parquet_path.exists():
                    try:
                        parquet_path.unlink()
                    except OSError:
                        pass
            archived.append({"table": "orderbook_quality_points", "day": day_label, "rows": int(len(clean_frame.index)), "path": str(written_path), "merged": bool(read_path)})

        point_keys = frame["point_key"].astype(str).tolist()
        if point_keys:
            placeholders = ",".join("?" for _ in point_keys)
            def writer(connection: sqlite3.Connection) -> int:
                cursor = connection.execute(f"DELETE FROM orderbook_quality_points WHERE point_key IN ({placeholders})", point_keys)
                return int(cursor.rowcount or 0)
            self._execute_write(writer)
        return archived

    def vacuum(self) -> None:
        self._execute_write(lambda connection: connection.execute("VACUUM"))

    def compact_payload_blobs(self) -> Dict[str, int]:
        updated_rows: Dict[str, int] = {}
        blob_columns = {
            "market_snapshots": "raw_json",
            "market_events": "raw_json",
            "alert_events": "payload_json",
            "signal_events": "payload_json",
        }
        for table_name, column_name in blob_columns.items():
            def writer(
                connection: sqlite3.Connection,
                table_name: str = table_name,
                column_name: str = column_name,
            ) -> int:
                cursor = connection.execute(
                    f"UPDATE {table_name} SET {column_name} = NULL WHERE {column_name} IS NOT NULL AND length({column_name}) > 0"
                )
                return int(cursor.rowcount or 0)
            updated_rows[table_name] = int(self._execute_write(writer) or 0)
        return updated_rows

    def prune_before(
        self,
        cutoff_ms: int,
        *,
        include_archive: bool = False,
    ) -> Dict[str, Any]:
        deleted_rows: Dict[str, int] = {}
        time_columns = {
            "market_snapshots": "ts_ms",
            "alert_events": "ts_ms",
            "market_events": "ts_ms",
            "orderbook_quality_points": "ts_ms",
            "orderbook_quality_1m": "bucket_ms",
            "signal_events": "ts_ms",
            "oi_points": "ts_ms",
            "transport_state": "ts_ms",
            "market_bars_1m": "bucket_ms",
        }
        for table_name, time_column in time_columns.items():
            def writer(connection: sqlite3.Connection, table_name: str = table_name, time_column: str = time_column) -> int:
                cursor = connection.execute(f"DELETE FROM {table_name} WHERE {time_column} < ?", (int(cutoff_ms),))
                return int(cursor.rowcount or 0)
            deleted_rows[table_name] = int(self._execute_write(writer) or 0)
        removed_archives: List[str] = []
        if include_archive:
            cutoff_dt = pd.to_datetime(int(cutoff_ms), unit="ms")
            for path in self.archive_dir.rglob("*.*"):
                file_name = path.name
                parts = file_name.split("-")
                if len(parts) < 2:
                    continue
                day_token = "-".join(parts[-3:]).split(".", 1)[0]
                try:
                    day_dt = pd.to_datetime(day_token, format="%Y-%m-%d", errors="raise")
                except Exception:
                    continue
                if day_dt < cutoff_dt.normalize():
                    try:
                        path.unlink()
                        removed_archives.append(str(path))
                    except OSError:
                        continue
        return {"deleted_rows": deleted_rows, "removed_archives": removed_archives}

    def describe(self, *, ttl_seconds: float = 15.0) -> Dict[str, Any]:
        now = time.time()
        with self._describe_cache_lock:
            cached = (
                dict(self._describe_cache)
                if self._describe_cache is not None and now - float(self._describe_cached_at or 0.0) < max(float(ttl_seconds), 0.0)
                else None
            )
        if cached is not None:
            return cached

        def approx_rows(connection: sqlite3.Connection, table_name: str) -> int:
            try:
                value = connection.execute(f"SELECT MAX(rowid) FROM {table_name}").fetchone()[0]
                return int(value or 0)
            except Exception:
                return 0

        try:
            db_size_bytes = int(self.db_path.stat().st_size)
        except OSError:
            db_size_bytes = 0

        def measured_rows(connection: sqlite3.Connection, table_name: str) -> int:
            if db_size_bytes <= 512 * 1024 * 1024:
                try:
                    return int(connection.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0] or 0)
                except Exception:
                    pass
            return approx_rows(connection, table_name)

        def latest_ts(connection: sqlite3.Connection, table_name: str, time_column: str = "ts_ms") -> int | None:
            try:
                value = connection.execute(f"SELECT {time_column} FROM {table_name} ORDER BY rowid DESC LIMIT 1").fetchone()
                if not value:
                    return None
                item = value[0]
                return int(item) if item is not None else None
            except Exception:
                return None

        with self._connect() as connection:
            market_count = measured_rows(connection, "market_snapshots")
            alert_count = measured_rows(connection, "alert_events")
            event_count = measured_rows(connection, "market_events")
            quality_count = measured_rows(connection, "orderbook_quality_points")
            quality_agg_count = measured_rows(connection, "orderbook_quality_1m")
            duplicate_quality_event_count = int(
                connection.execute("SELECT COUNT(*) FROM market_events WHERE category = 'orderbook_quality'").fetchone()[0] or 0
            )
            signal_count = measured_rows(connection, "signal_events")
            oi_point_count = measured_rows(connection, "oi_points")
            transport_count = measured_rows(connection, "transport_state")
            bars_count = measured_rows(connection, "market_bars_1m")
            last_market_ts = latest_ts(connection, "market_snapshots")
            last_alert_ts = latest_ts(connection, "alert_events")
            last_event_ts = latest_ts(connection, "market_events")
            last_quality_ts = latest_ts(connection, "orderbook_quality_points")
            last_quality_agg_ts = latest_ts(connection, "orderbook_quality_1m", "bucket_ms")
            last_signal_ts = latest_ts(connection, "signal_events")
            last_oi_ts = latest_ts(connection, "oi_points")
            last_transport_ts = latest_ts(connection, "transport_state")
            last_bar_ts = latest_ts(connection, "market_bars_1m", "bucket_ms")
        archive_files = sorted(self.archive_dir.rglob("*.*"))
        summary = {
            "db_path": str(self.db_path),
            "db_size_bytes": db_size_bytes,
            "market_rows": int(market_count or 0),
            "alert_rows": int(alert_count or 0),
            "event_rows": int(event_count or 0),
            "quality_rows": int(quality_count or 0),
            "quality_agg_rows": int(quality_agg_count or 0),
            "duplicate_quality_event_rows": int(duplicate_quality_event_count or 0),
            "signal_rows": int(signal_count or 0),
            "oi_point_rows": int(oi_point_count or 0),
            "transport_rows": int(transport_count or 0),
            "bar_rows": int(bars_count or 0),
            "last_market_ts": int(last_market_ts) if last_market_ts else None,
            "last_alert_ts": int(last_alert_ts) if last_alert_ts else None,
            "last_event_ts": int(last_event_ts) if last_event_ts else None,
            "last_quality_ts": int(last_quality_ts) if last_quality_ts else None,
            "last_quality_agg_ts": int(last_quality_agg_ts) if last_quality_agg_ts else None,
            "last_signal_ts": int(last_signal_ts) if last_signal_ts else None,
            "last_oi_ts": int(last_oi_ts) if last_oi_ts else None,
            "last_transport_ts": int(last_transport_ts) if last_transport_ts else None,
            "last_bar_ts": int(last_bar_ts) if last_bar_ts else None,
            "archive_files": [str(path) for path in archive_files[-16:]],
            "row_count_mode": "exact_if_small_else_approx_rowid",
        }
        with self._describe_cache_lock:
            self._describe_cache = dict(summary)
            self._describe_cached_at = now
        return summary

    def archive_before(self, cutoff_ms: int, *, prefer_parquet: bool = True) -> List[Dict[str, Any]]:
        archived: List[Dict[str, Any]] = []
        time_columns = {
            "market_snapshots": "ts_ms",
            "alert_events": "ts_ms",
            "market_events": "ts_ms",
            "orderbook_quality_points": "ts_ms",
            "orderbook_quality_1m": "bucket_ms",
            "signal_events": "ts_ms",
            "oi_points": "ts_ms",
            "transport_state": "ts_ms",
            "market_bars_1m": "bucket_ms",
        }
        for table_name, time_column in time_columns.items():
            with self._connect() as connection:
                frame = pd.read_sql_query(
                    f"SELECT * FROM {table_name} WHERE {time_column} < ? ORDER BY {time_column} ASC",
                    connection,
                    params=[int(cutoff_ms)],
                )
            if frame.empty:
                continue
            frame["archive_day"] = pd.to_datetime(frame[time_column], unit="ms").dt.strftime("%Y-%m-%d")
            for day_label, day_frame in frame.groupby("archive_day"):
                output_dir = self.archive_dir.joinpath(table_name)
                output_dir.mkdir(parents=True, exist_ok=True)
                clean_frame = day_frame.drop(columns=["archive_day"]).copy()
                parquet_path = output_dir.joinpath(f"{table_name}-{day_label}.parquet")
                csv_path = output_dir.joinpath(f"{table_name}-{day_label}.csv.gz")
                existing_frame = pd.DataFrame()
                read_path: Optional[Path] = None
                if parquet_path.exists():
                    try:
                        existing_frame = pd.read_parquet(parquet_path)
                        read_path = parquet_path
                    except Exception:
                        existing_frame = pd.DataFrame()
                elif csv_path.exists():
                    try:
                        existing_frame = pd.read_csv(csv_path)
                        read_path = csv_path
                    except Exception:
                        existing_frame = pd.DataFrame()
                merged_frame = pd.concat([existing_frame, clean_frame], ignore_index=True) if not existing_frame.empty else clean_frame
                dedupe_keys = {
                    "market_events": "event_key",
                    "orderbook_quality_points": "point_key",
                    "oi_points": "point_key",
                    "transport_state": "state_key",
                    "market_bars_1m": "bar_key",
                }
                dedupe_key = dedupe_keys.get(table_name)
                if dedupe_key and dedupe_key in merged_frame.columns:
                    merged_frame = merged_frame.drop_duplicates(subset=[dedupe_key], keep="last")
                elif table_name == "market_snapshots":
                    merged_frame = merged_frame.drop_duplicates(subset=["ts_ms", "coin", "exchange_key", "market", "symbol"], keep="last")
                elif table_name == "alert_events":
                    merged_frame = merged_frame.drop_duplicates(subset=["ts_ms", "coin", "exchange_name", "alert", "action"], keep="last")
                output_path = parquet_path if prefer_parquet else csv_path
                written_path = output_path
                if prefer_parquet:
                    try:
                        merged_frame.to_parquet(output_path, index=False)
                    except Exception:
                        written_path = csv_path
                        merged_frame.to_csv(written_path, index=False, compression="gzip")
                else:
                    merged_frame.to_csv(output_path, index=False, compression="gzip")
                archived.append({"table": table_name, "day": day_label, "rows": len(clean_frame), "path": str(written_path), "merged": bool(read_path)})
            with self._connect() as connection:
                connection.execute(f"DELETE FROM {table_name} WHERE {time_column} < ?", (int(cutoff_ms),))
        return archived
