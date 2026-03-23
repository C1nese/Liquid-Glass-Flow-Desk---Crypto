from __future__ import annotations

from typing import Any, Callable, Dict, Optional

import pandas as pd


def build_workspace_source_meta(
    *,
    source_label: str,
    working_frame: pd.DataFrame,
    detail_frame: Optional[pd.DataFrame] = None,
    frame_max_timestamp_ms: Callable[[pd.DataFrame, str], Optional[int]],
    best_updated_at_ms: Callable[..., Optional[int]],
) -> Dict[str, Any]:
    normalized_source = str(source_label or "").strip().lower()
    source_key = "unknown"
    data_source = str(source_label or "").strip() or "未标记数据源"
    truth_level = "proxy"
    if normalized_source == "manager monitor base cache":
        source_key = "manager_cache"
        data_source = "Manager 监控聚合缓存"
        truth_level = "proxy"
    elif normalized_source == "legacy monitor frame cache":
        source_key = "legacy_cache"
        data_source = "Legacy 监控缓存"
        truth_level = "proxy"
    elif normalized_source == "local runtime sessions + history fallback":
        source_key = "local_runtime_fallback"
        data_source = "本地 runtime 会话 + 历史回补"
        truth_level = "rest_backfill"
    updated_at_ms = best_updated_at_ms(
        frame_max_timestamp_ms(working_frame if isinstance(working_frame, pd.DataFrame) else pd.DataFrame(), "最近时间"),
        frame_max_timestamp_ms(detail_frame if isinstance(detail_frame, pd.DataFrame) else pd.DataFrame(), "最近时间"),
    )
    if not isinstance(working_frame, pd.DataFrame) or working_frame.empty:
        truth_level = "sample_limited"
    return {
        "source_key": source_key,
        "data_source": data_source,
        "truth_level": truth_level,
        "updated_at_ms": updated_at_ms,
    }


def resolve_overview_quadrant_columns(
    frame: pd.DataFrame,
    *,
    time_window: str,
    normalize_time_window: Callable[[str], str],
) -> Dict[str, str]:
    normalized_time_window = normalize_time_window(time_window)
    columns = set(frame.columns) if isinstance(frame, pd.DataFrame) else set()
    price_column_candidates = {
        "5m": ["5m%", "30m%", "1h%", "4h%", "24h%"],
        "15m": ["30m%", "1h%", "4h%", "24h%"],
        "30m": ["30m%", "1h%", "4h%", "24h%"],
        "1h": ["1h%", "4h%", "24h%"],
        "2h": ["1h%", "4h%", "24h%"],
        "4h": ["4h%", "1h%", "24h%"],
        "24h": ["24h%", "4h%", "1h%"],
        "1d": ["24h%", "4h%", "1h%"],
        "1w": ["24h%", "4h%", "1h%"],
    }
    oi_column_candidates = {
        "5m": ["OI 1h(%)", "OI 4h(%)", "OI 24h(%)"],
        "15m": ["OI 1h(%)", "OI 4h(%)", "OI 24h(%)"],
        "30m": ["OI 1h(%)", "OI 4h(%)", "OI 24h(%)"],
        "1h": ["OI 1h(%)", "OI 4h(%)", "OI 24h(%)"],
        "2h": ["OI 1h(%)", "OI 4h(%)", "OI 24h(%)"],
        "4h": ["OI 4h(%)", "OI 1h(%)", "OI 24h(%)"],
        "24h": ["OI 24h(%)", "OI 4h(%)", "OI 1h(%)"],
        "1d": ["OI 24h(%)", "OI 4h(%)", "OI 1h(%)"],
        "1w": ["OI 24h(%)", "OI 4h(%)", "OI 1h(%)"],
    }
    price_column = next(
        (column for column in price_column_candidates.get(normalized_time_window, ["1h%", "24h%"]) if column in columns),
        "24h%",
    )
    oi_column = next(
        (column for column in oi_column_candidates.get(normalized_time_window, ["OI 1h(%)"]) if column in columns),
        "OI 1h(%)",
    )
    return {
        "price_column": price_column,
        "oi_column": oi_column,
        "price_label": price_column.replace("%", " 价格变化"),
        "oi_label": oi_column.replace("(%)", "").replace("%", "") + " 变化",
    }


def build_overview_stage_payload(
    *,
    coin: str,
    generated_at_ms: int,
    filters: Dict[str, Any],
    exchange_key: str,
    watch_group: str,
    stage_name: str,
    cards: list[Dict[str, Any]],
    panels: list[Dict[str, Any]],
    data_source: str = "",
    updated_at_ms: Any = None,
    truth_level: str = "",
    best_updated_at_ms: Callable[..., Optional[int]],
    build_data_meta: Callable[[str, Any], Dict[str, Any]],
    standardize_payload_metrics: Callable[[Dict[str, Any]], Dict[str, Any]],
) -> Dict[str, Any]:
    payload = {
        "coin": coin,
        "generated_at_ms": generated_at_ms,
        "exchange_key": exchange_key,
        "watch_group": watch_group,
        "filters": filters,
        "stage": stage_name,
        "cards": cards,
        "panels": panels,
    }
    if str(data_source or "").strip() or best_updated_at_ms(updated_at_ms):
        data_meta = build_data_meta(str(data_source or "overview rich staged cache"), updated_at_ms or generated_at_ms)
        if str(truth_level or "").strip():
            data_meta["truth_level"] = str(truth_level or "").strip()
        payload["data_meta"] = data_meta
    return standardize_payload_metrics(payload)
