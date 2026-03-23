from __future__ import annotations

from typing import Any, Callable, Dict, List

import pandas as pd

from market_frame_columns import canonicalize_market_frame_columns, find_total_oi_column


def _canonicalize_known_columns(frame: pd.DataFrame) -> pd.DataFrame:
    return canonicalize_market_frame_columns(frame)


def _find_total_oi_column(frame: pd.DataFrame) -> str:
    return find_total_oi_column(frame)


def build_overview_rich_chart_seed_context(
    context: Dict[str, Any],
    *,
    coin: str,
    market_scope: str,
    sentiment_mode: str,
    resolve_overview_quadrant_columns: Callable[[pd.DataFrame, str], Dict[str, str]],
    attach_selection_column: Callable[[pd.DataFrame, str, List[str]], pd.DataFrame],
) -> Dict[str, Any]:
    now_ms = int(context.get("now_ms") or 0)
    summary_signature = dict(context.get("signature") or {})
    quadrant_meta = dict(context.get("quadrant_meta") or {})
    filtered_overview = context.get("filtered_overview") if isinstance(context.get("filtered_overview"), pd.DataFrame) else pd.DataFrame()
    multicoin_sentiment_frame = (
        context.get("multicoin_sentiment_frame")
        if isinstance(context.get("multicoin_sentiment_frame"), pd.DataFrame)
        else pd.DataFrame()
    )
    contract_sentiment_frame = (
        context.get("contract_sentiment_frame")
        if isinstance(context.get("contract_sentiment_frame"), pd.DataFrame)
        else pd.DataFrame()
    )

    prepared_overview = pd.DataFrame()
    if not filtered_overview.empty:
        prepared_overview = _canonicalize_known_columns(filtered_overview.copy())
        if "主交易所键" not in prepared_overview.columns:
            prepared_overview["主交易所键"] = [
                str(((item or {}).get("exchange") or ""))
                for item in prepared_overview.get("__selection", pd.Series([{} for _ in range(len(prepared_overview))]))
            ]
        prepared_overview = attach_selection_column(
            prepared_overview,
            coin,
            ["主交易所键", "主交易所", "偏离交易所"],
        )
        for column in ["5m%", "30m%", "1h%", "4h%", "24h%", "OI 1h(%)", "OI 4h(%)", "OI 24h(%)", "OI总额", "Funding(bp)", "L/S比", "情绪评分"]:
            if column in prepared_overview.columns:
                prepared_overview[column] = pd.to_numeric(prepared_overview[column], errors="coerce")
        total_oi_column = _find_total_oi_column(prepared_overview)
        if total_oi_column and total_oi_column in prepared_overview.columns:
            prepared_overview[total_oi_column] = pd.to_numeric(prepared_overview[total_oi_column], errors="coerce")

    oi_seed_frame = pd.DataFrame()
    oi_quadrant_seed_frame = pd.DataFrame()
    funding_seed_frame = pd.DataFrame()
    if market_scope != "spot" and not prepared_overview.empty:
        if not quadrant_meta:
            quadrant_meta = resolve_overview_quadrant_columns(
                prepared_overview,
                str(summary_signature.get("time_window") or "1h"),
            )
        total_oi_column = _find_total_oi_column(prepared_overview)
        quadrant_price_column = str(quadrant_meta.get("price_column") or "24h%")
        quadrant_oi_column = str(quadrant_meta.get("oi_column") or "OI 1h(%)")
        if total_oi_column:
            oi_seed_frame = (
                prepared_overview.dropna(subset=[total_oi_column])
                .sort_values(total_oi_column, ascending=False, na_position="last")
                .head(24)
                .reset_index(drop=True)
            )
        subset_columns = [column for column in [quadrant_price_column, quadrant_oi_column] if column in oi_seed_frame.columns]
        oi_quadrant_seed_frame = oi_seed_frame.dropna(subset=subset_columns).reset_index(drop=True) if subset_columns else pd.DataFrame()
        funding_seed_frame = (
            prepared_overview.dropna(subset=["Funding(bp)"])
            .reindex(prepared_overview["Funding(bp)"].abs().sort_values(ascending=False, na_position="last").index)
            .head(24)
            .reset_index(drop=True)
        )

    sentiment_seed_frame = pd.DataFrame()
    if not multicoin_sentiment_frame.empty:
        sentiment_seed_frame = _canonicalize_known_columns(multicoin_sentiment_frame.copy())
        if "情绪评分" in sentiment_seed_frame.columns:
            sentiment_seed_frame["情绪评分"] = pd.to_numeric(sentiment_seed_frame["情绪评分"], errors="coerce")
            sentiment_seed_frame = sentiment_seed_frame.reindex(
                sentiment_seed_frame["情绪评分"].abs().sort_values(ascending=False, na_position="last").index
            )
        if sentiment_mode == "extreme" and "情绪评分" in sentiment_seed_frame.columns:
            narrowed = sentiment_seed_frame[sentiment_seed_frame["情绪评分"].abs() >= 8.0].reset_index(drop=True)
            if not narrowed.empty:
                sentiment_seed_frame = narrowed
        sentiment_seed_frame = sentiment_seed_frame.head(18).reset_index(drop=True)

    contract_truth_seed_frame = pd.DataFrame()
    if market_scope != "spot" and not contract_sentiment_frame.empty:
        contract_truth_seed_frame = contract_sentiment_frame.copy().reset_index(drop=True)

    return {
        "now_ms": now_ms,
        "oi_seed_frame": oi_seed_frame,
        "oi_quadrant_seed_frame": oi_quadrant_seed_frame,
        "funding_seed_frame": funding_seed_frame,
        "sentiment_seed_frame": sentiment_seed_frame,
        "contract_truth_seed_frame": contract_truth_seed_frame,
        "quadrant_meta": quadrant_meta,
    }


def build_overview_rich_charts_a_context(
    light_context: Dict[str, Any],
    *,
    coin: str,
    watch_group: str,
    normalized_custom_coins: List[str],
    exchange_key: str,
    exchange_keys: List[str],
    market_scope: str,
    time_window: str,
    normalized_ratio_window: str,
    coin_scope: str,
    min_notional: float,
    oi_threshold_pct: float,
    sentiment_mode: str,
    ratio_sentiment_frame: pd.DataFrame,
    build_chart_seed_context: Callable[[Dict[str, Any]], Dict[str, Any]],
) -> Dict[str, Any]:
    now_ms = int(light_context.get("now_ms") or 0)
    filtered_overview = (
        light_context.get("filtered_overview")
        if isinstance(light_context.get("filtered_overview"), pd.DataFrame)
        else pd.DataFrame()
    )
    filtered_overview = _canonicalize_known_columns(filtered_overview.copy()) if not filtered_overview.empty else pd.DataFrame()
    ratio_sentiment_frame = _canonicalize_known_columns(ratio_sentiment_frame.copy()) if not ratio_sentiment_frame.empty else pd.DataFrame()

    multicoin_sentiment_frame = pd.DataFrame()
    if not filtered_overview.empty:
        sentiment_columns = [
            column
            for column in ["币种", "信号", "情绪评分", "置信度", "Funding(bp)", "OI 1h(%)", "24h%", "主交易所", "__selection"]
            if column in filtered_overview.columns
        ]
        multicoin_sentiment_frame = filtered_overview[sentiment_columns].copy()
        if "情绪" not in multicoin_sentiment_frame.columns and "情绪评分" in multicoin_sentiment_frame.columns:
            score_series = pd.to_numeric(multicoin_sentiment_frame["情绪评分"], errors="coerce").fillna(0.0)
            multicoin_sentiment_frame["情绪"] = [
                "偏多" if float(score) >= 4.0 else "偏空" if float(score) <= -4.0 else "中性"
                for score in score_series
            ]
        if "拥挤方向" not in multicoin_sentiment_frame.columns and "Funding(bp)" in multicoin_sentiment_frame.columns:
            funding_series = pd.to_numeric(multicoin_sentiment_frame["Funding(bp)"], errors="coerce").fillna(0.0)
            multicoin_sentiment_frame["拥挤方向"] = [
                "多头拥挤" if float(value) >= 2.5 else "空头拥挤" if float(value) <= -2.5 else "均衡"
                for value in funding_series
            ]
        if not ratio_sentiment_frame.empty and "币种" in ratio_sentiment_frame.columns:
            ratio_columns = [
                column
                for column in [
                    "币种",
                    "Binance全市场比",
                    "Bybit账户比",
                    "Bybit多头占比(%)",
                    "Bybit空头占比(%)",
                    "OKX合约账户比",
                    "OKX全网账户比",
                    "OKX大户账户比",
                    "OKX大户持仓比",
                    "OKX多头占比(%)",
                    "OKX空头占比(%)",
                    "Bitget账户比",
                    "Bitget多头占比(%)",
                    "Bitget空头占比(%)",
                    "Gate账户比",
                    "Gate多头占比(%)",
                    "Gate空头占比(%)",
                    "HTX账户比",
                    "HTX多头占比(%)",
                    "HTX空头占比(%)",
                ]
                if column in ratio_sentiment_frame.columns
            ]
            if len(ratio_columns) > 1:
                multicoin_sentiment_frame = multicoin_sentiment_frame.merge(
                    ratio_sentiment_frame[ratio_columns],
                    on="币种",
                    how="left",
                )
    if multicoin_sentiment_frame.empty:
        multicoin_sentiment_frame = ratio_sentiment_frame.copy()
        if not multicoin_sentiment_frame.empty:
            multicoin_sentiment_frame = multicoin_sentiment_frame.copy()
            multicoin_sentiment_frame["__selection"] = [
                {"coin": str(row.get("币种") or "").upper().strip()}
                for _, row in multicoin_sentiment_frame.iterrows()
            ]

    signature = {
        "coin": coin,
        "exchange_key": exchange_key,
        "watch_group": watch_group,
        "custom_coins": normalized_custom_coins,
        "exchange_keys": exchange_keys,
        "market_scope": market_scope,
        "time_window": time_window,
        "ratio_window": normalized_ratio_window,
        "coin_scope": coin_scope,
        "min_notional": min_notional,
        "oi_threshold_pct": oi_threshold_pct,
        "sentiment_mode": sentiment_mode,
        "stage": "charts-a-lite",
    }
    context = {
        "now_ms": now_ms,
        "cards": list(light_context.get("cards", [])),
        "filtered_overview": filtered_overview,
        "multicoin_sentiment_frame": multicoin_sentiment_frame,
        "contract_sentiment_frame": pd.DataFrame(),
        "data_source": light_context.get("data_source") or "multicoin REST aggregate + session cache",
        "truth_level": light_context.get("truth_level") or "rest_backfill",
        "quadrant_meta": light_context.get("quadrant_meta") or {},
        "signature": signature,
    }
    chart_seed_context = build_chart_seed_context(context)
    return {**context, "chart_seed_context": chart_seed_context}
