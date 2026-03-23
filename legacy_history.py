from __future__ import annotations

from typing import Any, Dict, List

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from market_frame_columns import canonicalize_market_frame_columns


def _canonicalize_frame(frame: pd.DataFrame) -> pd.DataFrame:
    if frame is None or frame.empty:
        return pd.DataFrame()
    return canonicalize_market_frame_columns(frame.copy())


def _subset_existing_columns(frame: pd.DataFrame, columns: List[str]) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame(columns=columns)
    normalized = _canonicalize_frame(frame)
    available_columns = [column for column in columns if column in normalized.columns]
    if not available_columns:
        return pd.DataFrame(columns=columns)
    return normalized[available_columns].copy()


def _concat_non_empty_frames(frames: List[pd.DataFrame]) -> pd.DataFrame:
    valid_frames: List[pd.DataFrame] = []
    for frame in frames:
        if frame.empty:
            continue
        cleaned = _canonicalize_frame(frame).dropna(axis=1, how="all").copy()
        if cleaned.empty or cleaned.shape[1] == 0:
            continue
        valid_frames.append(cleaned)
    if not valid_frames:
        return pd.DataFrame()
    return pd.concat(valid_frames, ignore_index=True, sort=False)


def build_history_review_payload(
    history_store: Any,
    *,
    coin: str,
    exchange_keys: List[str],
    since_ms: int,
) -> Dict[str, pd.DataFrame]:
    market_history_frame = _canonicalize_frame(
        history_store.load_market_history(
            coin=coin,
            exchange_keys=exchange_keys,
            since_ms=since_ms,
            limit=5000,
            include_archive=True,
        )
    )
    return {
        "alert_history_frame": _canonicalize_frame(history_store.load_alert_events(coin=coin, since_ms=since_ms, limit=360)),
        "market_history_frame": market_history_frame,
        "event_history_frame": _canonicalize_frame(
            history_store.load_market_events(
                coin=coin,
                exchange_keys=exchange_keys,
                since_ms=since_ms,
                market="perp",
                limit=3200,
                include_archive=True,
            )
        ),
        "quality_history_frame": _canonicalize_frame(
            history_store.load_quality_history(
                coin=coin,
                exchange_keys=exchange_keys,
                since_ms=since_ms,
                market="perp",
                limit=2400,
                include_archive=True,
            )
        ),
    }


def build_history_index_payload(history_store: Any, *, since_ms: int) -> Dict[str, pd.DataFrame]:
    perp_market_frame = _canonicalize_frame(history_store.load_market_history(since_ms=since_ms, market="perp", limit=6000, include_archive=True))
    spot_market_frame = _canonicalize_frame(history_store.load_market_history(since_ms=since_ms, market="spot", limit=4000, include_archive=True))
    perp_quality_frame = _canonicalize_frame(history_store.load_quality_history(since_ms=since_ms, market="perp", limit=4000, include_archive=True))
    spot_quality_frame = _canonicalize_frame(history_store.load_quality_history(since_ms=since_ms, market="spot", limit=2500, include_archive=True))
    market_frame = _concat_non_empty_frames(
        [
            _subset_existing_columns(perp_market_frame, ["时间", "币种", "交易所", "合约"]).assign(市场="perp")
            if not perp_market_frame.empty
            else pd.DataFrame(),
            _subset_existing_columns(spot_market_frame, ["时间", "币种", "交易所", "合约"]).assign(市场="spot")
            if not spot_market_frame.empty
            else pd.DataFrame(),
        ]
    )
    quality_frame = _concat_non_empty_frames(
        [
            _subset_existing_columns(perp_quality_frame, ["时间", "币种", "交易所", "市场", "合约"]),
            _subset_existing_columns(spot_quality_frame, ["时间", "币种", "交易所", "市场", "合约"]),
        ]
    )
    return {
        "alert_frame": _canonicalize_frame(history_store.load_alert_events(since_ms=since_ms, limit=3200)),
        "market_frame": market_frame,
        "event_frame": _canonicalize_frame(history_store.load_market_events(since_ms=since_ms, limit=5000, include_archive=True)),
        "quality_frame": quality_frame,
    }


def build_history_workbench_payload(
    history_store: Any,
    *,
    coin: str,
    exchange_keys: List[str],
    markets: List[str],
    event_categories: List[str],
    since_ms: int,
) -> Dict[str, pd.DataFrame]:
    selected_markets = [str(market) for market in markets if str(market) in {"perp", "spot"}] or ["perp"]
    market_frames: List[pd.DataFrame] = []
    quality_frames: List[pd.DataFrame] = []
    for market in selected_markets:
        market_frame = _canonicalize_frame(
            history_store.load_market_history(
                coin=coin,
                exchange_keys=exchange_keys,
                market=market,
                since_ms=since_ms,
                limit=5000,
                include_archive=True,
            )
        )
        if not market_frame.empty:
            market_frames.append(market_frame.assign(市场=market))
        quality_frame = _canonicalize_frame(
            history_store.load_quality_history(
                coin=coin,
                exchange_keys=exchange_keys,
                market=market,
                since_ms=since_ms,
                limit=3200,
                include_archive=True,
            )
        )
        if not quality_frame.empty:
            quality_frames.append(quality_frame)
    market_history_frame = _concat_non_empty_frames(market_frames)
    if not market_history_frame.empty and "时间" in market_history_frame.columns:
        market_history_frame = market_history_frame.sort_values("时间").reset_index(drop=True)
    event_history_frame = _canonicalize_frame(
        history_store.load_market_events(
            coin=coin,
            exchange_keys=exchange_keys,
            since_ms=since_ms,
            limit=4200,
            include_archive=True,
        )
    )
    if not event_history_frame.empty:
        if "市场" in event_history_frame.columns:
            event_history_frame = event_history_frame[event_history_frame["市场"].astype(str).isin(selected_markets)]
        normalized_categories = [category for category in event_categories if category != "all"]
        if normalized_categories and "类型" in event_history_frame.columns:
            event_history_frame = event_history_frame[event_history_frame["类型"].astype(str).isin(normalized_categories)]
        if "时间" in event_history_frame.columns:
            event_history_frame = event_history_frame.sort_values("时间", ascending=False).reset_index(drop=True)
    quality_history_frame = _concat_non_empty_frames(quality_frames)
    if not quality_history_frame.empty and "时间" in quality_history_frame.columns:
        quality_history_frame = quality_history_frame.sort_values("时间", ascending=False).reset_index(drop=True)
    alert_history_frame = _canonicalize_frame(history_store.load_alert_events(coin=coin, since_ms=since_ms, limit=1000))
    if not alert_history_frame.empty and exchange_keys and "交易所键" in alert_history_frame.columns:
        alert_history_frame = alert_history_frame[alert_history_frame["交易所键"].astype(str).isin(exchange_keys)].reset_index(drop=True)
    return {
        "market_history_frame": market_history_frame,
        "event_history_frame": event_history_frame,
        "quality_history_frame": quality_history_frame,
        "alert_history_frame": alert_history_frame,
    }


def build_history_workbench_figure(market_history_frame: pd.DataFrame) -> go.Figure:
    figure = make_subplots(
        rows=2,
        cols=1,
        shared_xaxes=True,
        vertical_spacing=0.08,
        specs=[[{"secondary_y": True}], [{"secondary_y": True}]],
        subplot_titles=("价格 / OI金额", "Funding / 24h成交额"),
    )
    frame = _canonicalize_frame(market_history_frame)
    if frame.empty or "时间" not in frame.columns:
        figure.update_layout(height=520, margin=dict(l=18, r=18, t=48, b=18))
        return figure
    frame = frame.sort_values("时间")
    if "市场" in frame.columns:
        legend_names = (frame["交易所"].astype(str) + " · " + frame["市场"].astype(str)).unique().tolist()
    else:
        legend_names = frame["交易所"].astype(str).unique().tolist()
    for legend_name in legend_names:
        if "市场" in frame.columns:
            exchange_name, market = legend_name.split(" · ", 1)
            subset = frame[(frame["交易所"].astype(str) == exchange_name) & (frame["市场"].astype(str) == market)]
        else:
            subset = frame[frame["交易所"].astype(str) == legend_name]
        if subset.empty:
            continue
        if "最新价" in subset.columns and subset["最新价"].notna().any():
            figure.add_trace(
                go.Scatter(x=subset["时间"], y=subset["最新价"], mode="lines", name=f"{legend_name} 价格", line=dict(width=2.2)),
                row=1,
                col=1,
                secondary_y=False,
            )
        if "OI总额" in subset.columns and subset["OI总额"].notna().any():
            figure.add_trace(
                go.Scatter(x=subset["时间"], y=subset["OI总额"], mode="lines", name=f"{legend_name} OI", line=dict(width=1.4, dash="dot"), opacity=0.72),
                row=1,
                col=1,
                secondary_y=True,
            )
        funding_column = "Funding(bp)" if "Funding(bp)" in subset.columns else "Funding(bps)" if "Funding(bps)" in subset.columns else ""
        if funding_column and subset[funding_column].notna().any():
            figure.add_trace(
                go.Scatter(x=subset["时间"], y=subset[funding_column], mode="lines", name=f"{legend_name} Funding", line=dict(width=1.7), opacity=0.88),
                row=2,
                col=1,
                secondary_y=False,
            )
        volume_column = "24h成交额" if "24h成交额" in subset.columns else "volume_24h_notional" if "volume_24h_notional" in subset.columns else ""
        if volume_column and subset[volume_column].notna().any():
            figure.add_trace(
                go.Scatter(x=subset["时间"], y=subset[volume_column], mode="lines", name=f"{legend_name} 24h成交额", line=dict(width=1.2, dash="dash"), opacity=0.48),
                row=2,
                col=1,
                secondary_y=True,
            )
    figure.update_layout(height=540, margin=dict(l=18, r=18, t=48, b=18), legend=dict(orientation="h"))
    figure.update_yaxes(title_text="价格", row=1, col=1, secondary_y=False)
    figure.update_yaxes(title_text="OI金额", row=1, col=1, secondary_y=True)
    figure.update_yaxes(title_text="Funding(bps)", row=2, col=1, secondary_y=False)
    figure.update_yaxes(title_text="24h成交额", row=2, col=1, secondary_y=True)
    return figure


def build_history_event_mix_figure(event_history_frame: pd.DataFrame) -> go.Figure:
    figure = go.Figure()
    frame = _canonicalize_frame(event_history_frame)
    if frame.empty or "交易所" not in frame.columns or "类型" not in frame.columns:
        figure.update_layout(height=320, margin=dict(l=18, r=18, t=32, b=18))
        return figure
    grouped = (
        frame.groupby(["交易所", "类型"], as_index=False)
        .agg(名义金额=("名义金额", "sum"), 事件数=("类型", "size"))
        .sort_values(["交易所", "名义金额"], ascending=[True, False])
    )
    for event_type in grouped["类型"].astype(str).unique():
        subset = grouped[grouped["类型"].astype(str) == event_type]
        figure.add_trace(
            go.Bar(
                x=subset["交易所"],
                y=subset["名义金额"],
                name=event_type,
                text=subset["事件数"],
                textposition="outside",
            )
        )
    figure.update_layout(
        barmode="stack",
        height=320,
        margin=dict(l=18, r=18, t=32, b=18),
        xaxis_title="交易所",
        yaxis_title="名义金额",
        legend=dict(orientation="h"),
    )
    return figure
