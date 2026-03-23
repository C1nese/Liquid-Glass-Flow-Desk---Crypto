from __future__ import annotations

import time
from typing import Any, Callable, Dict, List

import pandas as pd

from analytics import (
    build_bull_bear_power_figure,
    build_contract_sentiment_truth_figure,
    build_cvd_figure,
    build_multi_coin_funding_heatmap_figure,
    build_multi_coin_oi_quadrant_bubble_figure,
    build_multi_coin_oi_ranking_figure,
    build_multicoin_long_short_ratio_figure,
    build_spot_perp_spread_history_figure,
)
from overview_runtime_cache_keys import build_overview_rich_chart_seed_cache_key


def build_overview_rich_chart_panels_a(
    context: Dict[str, Any],
    *,
    watch_group: str,
    market_scope: str,
    sentiment_mode: str,
    sync_heavy: bool,
    data_source_default: str,
    cache_signature_token: Callable[[Dict[str, Any]], str],
    cache_get_or_build: Callable[[str, int, Callable[[], Dict[str, Any]]], Dict[str, Any]],
    build_chart_seed_context: Callable[[Dict[str, Any], str, str], Dict[str, Any]],
    build_timed_figure_panel: Callable[..., Dict[str, Any]],
    build_timed_frame_panel: Callable[..., Dict[str, Any]],
    attach_panel_meta: Callable[..., List[Dict[str, Any]]],
    watch_group_label: Callable[[Any], str],
) -> List[Dict[str, Any]]:
    now_ms = int(context.get("now_ms") or time.time() * 1000)
    summary_signature = dict(context.get("signature") or {})
    data_source = str(context.get("data_source") or data_source_default)
    truth_level = str(context.get("truth_level") or "rest_backfill")
    chart_seed_context = context.get("chart_seed_context") if isinstance(context.get("chart_seed_context"), dict) else None
    if chart_seed_context is None:
        chart_seed_cache_key = build_overview_rich_chart_seed_cache_key(
            summary_signature,
            cache_signature_token=cache_signature_token,
        )
        chart_seed_context = cache_get_or_build(
            chart_seed_cache_key,
            30,
            lambda: build_chart_seed_context(
                context,
                market_scope,
                sentiment_mode,
            ),
        )
    oi_seed_frame = chart_seed_context.get("oi_seed_frame") if isinstance(chart_seed_context.get("oi_seed_frame"), pd.DataFrame) else pd.DataFrame()
    oi_quadrant_seed_frame = (
        chart_seed_context.get("oi_quadrant_seed_frame")
        if isinstance(chart_seed_context.get("oi_quadrant_seed_frame"), pd.DataFrame)
        else pd.DataFrame()
    )
    funding_seed_frame = (
        chart_seed_context.get("funding_seed_frame")
        if isinstance(chart_seed_context.get("funding_seed_frame"), pd.DataFrame)
        else pd.DataFrame()
    )
    multicoin_sentiment_frame = (
        chart_seed_context.get("sentiment_seed_frame")
        if isinstance(chart_seed_context.get("sentiment_seed_frame"), pd.DataFrame)
        else pd.DataFrame()
    )
    quadrant_meta = chart_seed_context.get("quadrant_meta") if isinstance(chart_seed_context.get("quadrant_meta"), dict) else {}
    quadrant_price_column = str(quadrant_meta.get("price_column") or "24h%")
    quadrant_oi_column = str(quadrant_meta.get("oi_column") or "OI 1h(%)")
    quadrant_price_label = str(quadrant_meta.get("price_label") or quadrant_price_column)
    quadrant_oi_label = str(quadrant_meta.get("oi_label") or quadrant_oi_column)
    panels: List[Dict[str, Any]] = []
    if market_scope != "spot":
        panels.extend(
            [
                build_timed_figure_panel(
                    "overview-oi-ranking",
                    "持仓量排行（OI Ranking）",
                    signature={**summary_signature, "panel": "oi-ranking", "rows": len(oi_seed_frame), "seed": "hot"},
                    figure_builder=lambda: build_multi_coin_oi_ranking_figure(oi_seed_frame, limit=18),
                    note="先看谁的 OI 体量最大、谁在被快速关注。",
                    data_source=data_source,
                    updated_at_ms=now_ms,
                    truth_level=truth_level,
                    ttl_seconds=45,
                    heavy=True,
                    sync_on_miss=sync_heavy,
                ),
                build_timed_figure_panel(
                    "overview-oi-quadrant",
                    "OI变化 vs 价格变化四象限",
                    signature={**summary_signature, "panel": "oi-quadrant", "rows": len(oi_quadrant_seed_frame), "seed": "hot"},
                    figure_builder=lambda: build_multi_coin_oi_quadrant_bubble_figure(
                        oi_quadrant_seed_frame,
                        limit=20,
                        price_column=quadrant_price_column,
                        oi_column=quadrant_oi_column,
                        price_label=quadrant_price_label,
                        oi_label=quadrant_oi_label,
                    ),
                    note=f"横轴 {quadrant_price_label}，纵轴 {quadrant_oi_label}。",
                    data_source=data_source,
                    updated_at_ms=now_ms,
                    truth_level=truth_level,
                    ttl_seconds=45,
                    heavy=True,
                    sync_on_miss=sync_heavy,
                ),
                build_timed_figure_panel(
                    "overview-funding-heatmap",
                    "资金费率热力图",
                    signature={**summary_signature, "panel": "funding-heatmap", "rows": len(funding_seed_frame), "seed": "hot"},
                    figure_builder=lambda: build_multi_coin_funding_heatmap_figure(funding_seed_frame, limit=18),
                    note="按当前过滤后的币组展示 Funding 极值。",
                    data_source=data_source,
                    updated_at_ms=now_ms,
                    truth_level=truth_level,
                    ttl_seconds=45,
                    heavy=True,
                    sync_on_miss=sync_heavy,
                ),
                build_timed_figure_panel(
                    "overview-multicoin-long-short",
                    "主流币多空比总览",
                    signature={**summary_signature, "panel": "multicoin-long-short", "rows": len(multicoin_sentiment_frame), "ratio_window": summary_signature.get("ratio_window")},
                    figure_builder=lambda: build_multicoin_long_short_ratio_figure(
                        multicoin_sentiment_frame,
                        ratio_window=str(summary_signature.get("ratio_window") or "15m"),
                        title=f"{watch_group_label(watch_group)} 多空比总览",
                    ),
                    note=f"覆盖币组：{watch_group_label(watch_group)}，聚合展示主流币多空比。",
                    data_source=data_source,
                    updated_at_ms=now_ms,
                    truth_level="proxy",
                    ttl_seconds=45,
                    heavy=True,
                    sync_on_miss=sync_heavy,
                ),
            ]
        )
    panels.append(
        build_timed_frame_panel(
            "overview-multicoin-sentiment",
            "多币种多空情绪板",
            signature={**summary_signature, "panel": "multicoin-sentiment", "rows": len(multicoin_sentiment_frame)},
            frame_builder=lambda: multicoin_sentiment_frame,
            note=f"币组：{watch_group_label(watch_group)} | 模式：{sentiment_mode}",
            limit=18,
            data_source=data_source,
            updated_at_ms=now_ms,
            truth_level="proxy",
            ttl_seconds=45,
        )
    )
    return attach_panel_meta(
        panels,
        data_source=data_source,
        updated_at_ms=now_ms,
    )


def build_overview_rich_chart_panels_b(
    context: Dict[str, Any],
    *,
    coin: str,
    market_scope: str,
    time_window: str,
    ratio_window: str,
    min_notional: float,
    sync_heavy: bool,
    exchange_title_map: Dict[str, str],
    exchange_order: List[str],
    default_ratio_panel_coins: List[str],
    tape_min_notional_floor: float,
    normalize_ratio_window: Callable[[str], str],
    time_window_minutes: Callable[[str], int],
    best_updated_at_ms: Callable[..., int],
    frame_max_timestamp_ms: Callable[[pd.DataFrame, str], int],
    exchange_ratio_panel_variants: Callable[..., Dict[str, Dict[str, Any]]],
    build_timed_multi_figure_panel: Callable[..., Dict[str, Any]],
    build_timed_figure_panel: Callable[..., Dict[str, Any]],
    build_timed_frame_panel: Callable[..., Dict[str, Any]],
    attach_panel_meta: Callable[..., List[Dict[str, Any]]],
    fmt_compact: Callable[[Any], str],
) -> List[Dict[str, Any]]:
    now_ms = int(context.get("now_ms") or time.time() * 1000)
    summary_signature = dict(context.get("signature") or {})
    contract_sentiment_frame = (
        context.get("contract_sentiment_frame")
        if isinstance(context.get("contract_sentiment_frame"), pd.DataFrame)
        else pd.DataFrame()
    )
    contract_sentiment_alert_frame = (
        context.get("contract_sentiment_alert_frame")
        if isinstance(context.get("contract_sentiment_alert_frame"), pd.DataFrame)
        else pd.DataFrame()
    )
    spot_perp_alert_frame = (
        context.get("spot_perp_alert_frame")
        if isinstance(context.get("spot_perp_alert_frame"), pd.DataFrame)
        else pd.DataFrame()
    )
    bull_bear_frame = context.get("bull_bear_frame") if isinstance(context.get("bull_bear_frame"), pd.DataFrame) else pd.DataFrame()
    recent_trade_frame = context.get("recent_trade_frame") if isinstance(context.get("recent_trade_frame"), pd.DataFrame) else pd.DataFrame()
    spread_history_frame = context.get("spread_history_frame") if isinstance(context.get("spread_history_frame"), pd.DataFrame) else pd.DataFrame()
    exchange_long_short_frame = (
        context.get("exchange_long_short_frame")
        if isinstance(context.get("exchange_long_short_frame"), pd.DataFrame)
        else pd.DataFrame()
    )
    selected_exchange_for_trades = str(context.get("selected_exchange_for_trades") or summary_signature.get("exchange_key") or "binance").lower().strip()
    selected_trade_market = str(context.get("selected_trade_market") or "perp")
    selected_trades = list(context.get("selected_trades") or [])
    resolved_time_window_minutes = max(15, time_window_minutes(time_window))
    normalized_ratio_window = normalize_ratio_window(ratio_window)
    exchange_title = exchange_title_map.get(selected_exchange_for_trades, selected_exchange_for_trades.title())
    selected_exchange_keys = [
        str(item or "").lower().strip()
        for item in list(summary_signature.get("exchange_keys") or exchange_order)
        if str(item or "").strip()
    ]
    ratio_panel_coins: List[str] = []
    for candidate in [coin, *default_ratio_panel_coins]:
        normalized_coin = str(candidate or "").upper().strip()
        if normalized_coin and normalized_coin not in ratio_panel_coins:
            ratio_panel_coins.append(normalized_coin)
    ratio_control_tabs = [{"key": item, "label": item} for item in ratio_panel_coins]
    ratio_variant_bundle = (
        exchange_ratio_panel_variants(
            coins=ratio_panel_coins,
            exchange_keys=selected_exchange_keys,
            ratio_window=normalized_ratio_window,
        )
        if market_scope != "spot"
        else {}
    )
    ratio_balance_variants = {
        symbol: payload.get("balance_figure")
        for symbol, payload in ratio_variant_bundle.items()
        if payload.get("balance_figure") is not None
    }
    ratio_trend_variants = {
        symbol: payload.get("trend_figure")
        for symbol, payload in ratio_variant_bundle.items()
        if payload.get("trend_figure") is not None
    }
    ratio_variant_updated_at_ms = best_updated_at_ms(
        frame_max_timestamp_ms(exchange_long_short_frame, "时间"),
        *[payload.get("updated_at_ms") for payload in ratio_variant_bundle.values()],
        now_ms,
    )
    panels: List[Dict[str, Any]] = []
    if market_scope != "spot":
        panels.extend(
            [
                build_timed_multi_figure_panel(
                    "overview-exchange-long-short-balance",
                    "交易所多空比分布",
                    signature={
                        **summary_signature,
                        "panel": "exchange-long-short-balance",
                        "rows": len(exchange_long_short_frame),
                        "ratio_window": normalized_ratio_window,
                        "ratio_tabs": ratio_panel_coins,
                    },
                    figures_builder=lambda: ratio_balance_variants,
                    note="图内可切 BTC / ETH / SOL / BNB / XRP，不改变全局币种。",
                    data_source="Binance / Bybit / Bitget / Gate / HTX 官方公开多空比接口",
                    updated_at_ms=ratio_variant_updated_at_ms,
                    truth_level="proxy",
                    heavy=True,
                    sync_on_miss=sync_heavy,
                    control_tabs=ratio_control_tabs,
                    default_variant=coin,
                ),
                build_timed_multi_figure_panel(
                    "overview-exchange-long-short-trend",
                    "交易所多空比趋势",
                    signature={
                        **summary_signature,
                        "panel": "exchange-long-short-trend",
                        "ratio_window": normalized_ratio_window,
                        "ratio_tabs": ratio_panel_coins,
                    },
                    figures_builder=lambda: ratio_trend_variants,
                    note="同一窗口口径下的交易所多空比时间序列。",
                    data_source="Binance / Bybit / Bitget / Gate / HTX 官方公开多空比接口",
                    updated_at_ms=ratio_variant_updated_at_ms,
                    truth_level="proxy",
                    heavy=True,
                    sync_on_miss=sync_heavy,
                    control_tabs=ratio_control_tabs,
                    default_variant=coin,
                ),
                build_timed_figure_panel(
                    "overview-contract-sentiment",
                    "合约情绪真值层",
                    signature={**summary_signature, "panel": "contract-sentiment", "rows": len(contract_sentiment_frame)},
                    figure_builder=lambda: build_contract_sentiment_truth_figure(contract_sentiment_frame),
                    note="Binance / Bybit 真值优先，OKX / Hyperliquid 代理参考。",
                    data_source="多交易所 REST 聚合 + 会话实时缓存",
                    updated_at_ms=now_ms,
                    truth_level="proxy",
                    heavy=True,
                    sync_on_miss=sync_heavy,
                ),
                build_timed_frame_panel(
                    "overview-contract-sentiment-table",
                    "合约情绪真值明细",
                    signature={**summary_signature, "panel": "contract-sentiment-table", "rows": len(contract_sentiment_frame)},
                    frame_builder=lambda: contract_sentiment_frame,
                    note="公开多空比、Funding、主动流与共振评分。",
                    limit=12,
                    data_source="多交易所 REST 聚合 + 会话实时缓存",
                    updated_at_ms=now_ms,
                    truth_level="proxy",
                ),
                build_timed_frame_panel(
                    "overview-contract-alerts",
                    "合约情绪告警",
                    signature={**summary_signature, "panel": "contract-alerts", "rows": len(contract_sentiment_alert_frame)},
                    frame_builder=lambda: contract_sentiment_alert_frame,
                    note="合约拥挤与共振异常。",
                    limit=12,
                    data_source="多交易所 REST 聚合 + 会话实时缓存",
                    updated_at_ms=now_ms,
                    truth_level="proxy",
                ),
                build_timed_figure_panel(
                    "overview-cvd",
                    f"{coin} CVD",
                    signature={**summary_signature, "panel": "cvd", "rows": len(selected_trades), "exchange": selected_exchange_for_trades, "market": selected_trade_market},
                    figure_builder=lambda: build_cvd_figure(selected_trades, now_ms=now_ms, window_minutes=max(15, min(120, resolved_time_window_minutes))),
                    note=f"{exchange_title} 逐笔成交流。",
                    data_source="逐笔成交 WS 实时流",
                    updated_at_ms=now_ms,
                    truth_level="ws_live",
                    heavy=True,
                    sync_on_miss=sync_heavy,
                ),
                build_timed_figure_panel(
                    "overview-bull-bear",
                    "多空力量实时面板",
                    signature={**summary_signature, "panel": "bull-bear", "rows": len(bull_bear_frame)},
                    figure_builder=lambda: build_bull_bear_power_figure(bull_bear_frame, aggregate_score=context.get("bull_bear_score")),
                    note="盘口力量 + CVD 速率 + 综合买卖力量。",
                    data_source="盘口 + 成交 WS 实时流",
                    updated_at_ms=now_ms,
                    truth_level="ws_live",
                    heavy=True,
                    sync_on_miss=sync_heavy,
                ),
            ]
        )
    if market_scope != "perp":
        panels.extend(
            [
                build_timed_figure_panel(
                    "overview-spot-perp-spread",
                    "现货-合约实时价差",
                    signature={
                        **summary_signature,
                        "panel": "spot-perp-spread",
                        "exchange_keys": summary_signature.get("exchange_keys"),
                        "rows": len(spread_history_frame),
                    },
                    figure_builder=lambda: build_spot_perp_spread_history_figure(spread_history_frame),
                    note="按交易所对比 spot/perp spread bps。",
                    data_source="Spot + Perp 实时快照",
                    updated_at_ms=now_ms,
                    truth_level="rest_backfill",
                    heavy=True,
                    sync_on_miss=sync_heavy,
                ),
                build_timed_frame_panel(
                    "overview-spot-perp-alerts",
                    "Spot-Perp 实时告警",
                    signature={**summary_signature, "panel": "spot-perp-alerts", "rows": len(spot_perp_alert_frame)},
                    frame_builder=lambda: spot_perp_alert_frame,
                    note="现货先动、合约抢跑、OI 与主动流联动。",
                    limit=12,
                    data_source="Spot + Perp 联动判定",
                    updated_at_ms=now_ms,
                    truth_level="proxy",
                ),
            ]
        )
    panels.append(
        build_timed_frame_panel(
            "overview-recent-trades",
            "最近主动成交",
            signature={**summary_signature, "panel": "recent-trades", "rows": len(recent_trade_frame), "exchange": selected_exchange_for_trades, "market": selected_trade_market},
            frame_builder=lambda: recent_trade_frame,
            note=(
                f"金额下限：{fmt_compact(max(float(min_notional or 0.0), tape_min_notional_floor))}"
                f" | 市场：{'现货' if selected_trade_market == 'spot' else '合约'}"
            ),
            limit=100,
            data_source="逐笔成交 WS 实时流",
            updated_at_ms=now_ms,
            truth_level="ws_live",
        )
    )
    return attach_panel_meta(
        panels,
        data_source="多交易所 REST 聚合 + 会话实时缓存",
        updated_at_ms=now_ms,
    )
