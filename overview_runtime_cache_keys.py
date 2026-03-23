from __future__ import annotations

from typing import Any, Callable, Dict, List


def build_custom_token(custom_coins: List[str]) -> str:
    return ",".join([str(item or "").upper().strip() for item in list(custom_coins or []) if str(item or "").strip()]) or "-"


def build_overview_rich_full_signature(
    *,
    coin: str,
    exchange_key: str,
    watch_group: str,
    custom_coins: List[str],
    exchange_keys: List[str],
    market_scope: str,
    time_window: str,
    ratio_window: str,
    coin_scope: str,
    min_notional: float,
    oi_threshold_pct: float,
    sentiment_mode: str,
    stage: str = "full",
) -> Dict[str, Any]:
    return {
        "coin": coin,
        "exchange_key": exchange_key,
        "watch_group": watch_group,
        "custom_coins": list(custom_coins or []),
        "exchange_keys": list(exchange_keys or []),
        "market_scope": market_scope,
        "time_window": time_window,
        "ratio_window": ratio_window,
        "coin_scope": coin_scope,
        "min_notional": min_notional,
        "oi_threshold_pct": oi_threshold_pct,
        "sentiment_mode": sentiment_mode,
        "stage": stage,
    }


def build_overview_rich_lite_cache_key(
    *,
    coin: str,
    watch_group: str,
    exchange_key: str,
    custom_token: str,
    coin_scope: str,
    market_scope: str,
    time_window: str,
    ratio_window: str,
    oi_threshold_pct: float,
    sentiment_mode: str,
) -> str:
    return (
        f"overview-rich-lite::{coin}::{watch_group}::{exchange_key}::"
        f"{custom_token}::{coin_scope}::{market_scope}::{time_window}::{ratio_window}::{oi_threshold_pct}::{sentiment_mode}"
    )


def build_overview_rich_charts_a_cache_key(
    *,
    coin: str,
    watch_group: str,
    exchange_key: str,
    exchange_keys: List[str],
    custom_token: str,
    coin_scope: str,
    market_scope: str,
    time_window: str,
    ratio_window: str,
    oi_threshold_pct: float,
    sentiment_mode: str,
) -> str:
    return (
        f"overview-rich-charts-a::{coin}::{watch_group}::{exchange_key}::{','.join(exchange_keys)}::"
        f"{custom_token}::{coin_scope}::{market_scope}::{time_window}::{ratio_window}::{oi_threshold_pct}::{sentiment_mode}"
    )


def build_overview_rich_charts_b_cache_key(
    *,
    coin: str,
    exchange_key: str,
    exchange_keys: List[str],
    market_scope: str,
    time_window: str,
    ratio_window: str,
    min_notional: float,
) -> str:
    return (
        f"overview-rich-charts-b::{coin}::{exchange_key}::{','.join(exchange_keys)}::"
        f"{market_scope}::{time_window}::{ratio_window}::{min_notional}"
    )


def build_overview_rich_full_cache_key(
    *,
    coin: str,
    watch_group: str,
    exchange_key: str,
    exchange_keys: List[str],
    custom_token: str,
    coin_scope: str,
    market_scope: str,
    time_window: str,
    ratio_window: str,
    min_notional: float,
    oi_threshold_pct: float,
    sentiment_mode: str,
) -> str:
    return (
        f"overview-rich-full::{coin}::{watch_group}::{exchange_key}::{','.join(exchange_keys)}::"
        f"{custom_token}::{coin_scope}::{market_scope}::{time_window}::{ratio_window}::{min_notional}::{oi_threshold_pct}::{sentiment_mode}"
    )


def build_overview_rich_chart_seed_cache_key(
    signature: Dict[str, Any],
    *,
    cache_signature_token: Callable[[Dict[str, Any]], str],
) -> str:
    return f"overview-rich-chart-seed::{cache_signature_token({**dict(signature or {}), 'stage': 'chart-seed'})}"


def build_overview_rich_heavy_warm_cache_key(
    signature: Dict[str, Any],
    *,
    cache_signature_token: Callable[[Dict[str, Any]], str],
) -> str:
    return f"overview-rich-heavy-warm::{cache_signature_token(dict(signature or {}))}"


def build_overview_rich_charts_a_heavy_cache_key(
    signature: Dict[str, Any],
    *,
    cache_signature_token: Callable[[Dict[str, Any]], str],
) -> str:
    return f"overview-rich-charts-a-heavy::{cache_signature_token(dict(signature or {}))}"


def build_overview_rich_charts_b_heavy_cache_key(
    signature: Dict[str, Any],
    *,
    cache_signature_token: Callable[[Dict[str, Any]], str],
) -> str:
    return f"overview-rich-charts-b-heavy::{cache_signature_token(dict(signature or {}))}"
