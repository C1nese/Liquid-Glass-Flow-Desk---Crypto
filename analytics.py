from __future__ import annotations

import math
import time
from typing import Any, Dict, List

import pandas as pd
import plotly.graph_objects as go
import plotly.validator_cache  # noqa: F401
from plotly.subplots import make_subplots

from market_frame_columns import canonicalize_market_frame_columns
from models import (
    Candle,
    ExchangeSnapshot,
    LiquidationEvent,
    OIPoint,
    OrderBookLevel,
    OrderBookQualityPoint,
    RecordedMarketEvent,
    SpotSnapshot,
    TradeEvent,
)


EMPTY_FRAME_COLUMNS = ["价格区间", "价格中位", "热度", "方向", "归因"]
RATIO_WINDOW_LABELS = {
    "5m": "5分钟",
    "15m": "15分钟",
    "30m": "30分钟",
    "1h": "1小时",
    "4h": "4小时",
    "1d": "1天",
    "1w": "1周",
}


def clamp01(value: float) -> float:
    return max(0.0, min(1.0, value))


def _coerce_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _coerce_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _normalize_series(values: List[float]) -> List[float]:
    if not values:
        return []
    max_value = max(values)
    if max_value <= 0:
        return [0.0 for _ in values]
    return [value / max_value for value in values]


def _parse_side_label(side: str) -> str:
    if side == "long":
        return "多头爆仓"
    if side == "short":
        return "空头爆仓"
    return side or "未知"


def summarize_orderbook(levels: List[OrderBookLevel], reference_price: float | None) -> Dict[str, float | None]:
    bid_levels = [level for level in levels if level.side == "bid" and level.size > 0]
    ask_levels = [level for level in levels if level.side == "ask" and level.size > 0]
    if not bid_levels and not ask_levels:
        return {
            "bid_size": None,
            "ask_size": None,
            "bid_notional": None,
            "ask_notional": None,
            "imbalance_pct": None,
            "spread_bps": None,
        }
    bid_size = sum(level.size for level in bid_levels)
    ask_size = sum(level.size for level in ask_levels)
    bid_notional = sum(level.price * level.size for level in bid_levels)
    ask_notional = sum(level.price * level.size for level in ask_levels)
    total_notional = bid_notional + ask_notional
    imbalance = None
    if total_notional > 0:
        imbalance = (bid_notional - ask_notional) / total_notional * 100.0
    top_bid = max((level.price for level in bid_levels), default=None)
    top_ask = min((level.price for level in ask_levels), default=None)
    spread_bps = None
    if top_bid and top_ask and reference_price:
        spread_bps = (top_ask - top_bid) / reference_price * 10000.0
    return {
        "bid_size": bid_size,
        "ask_size": ask_size,
        "bid_notional": bid_notional,
        "ask_notional": ask_notional,
        "imbalance_pct": imbalance,
        "spread_bps": spread_bps,
    }


def merge_liquidation_events(backfill: List[LiquidationEvent], session_events: List[LiquidationEvent]) -> List[LiquidationEvent]:
    merged: Dict[tuple, LiquidationEvent] = {}
    for event in backfill + session_events:
        key = (
            event.exchange,
            event.symbol,
            event.timestamp_ms,
            event.side,
            round(event.price or 0.0, 6),
            round(event.size or 0.0, 6),
        )
        merged[key] = event
    return sorted(merged.values(), key=lambda item: item.timestamp_ms)


def build_liquidation_metrics(
    events: List[LiquidationEvent],
    now_ms: int | None = None,
    window_minutes: int = 60,
) -> Dict[str, float | int | str | None]:
    if now_ms is None:
        now_ms = int(time.time() * 1000)
    cutoff_ms = now_ms - max(window_minutes, 1) * 60_000
    recent_events = [event for event in events if event.timestamp_ms >= cutoff_ms]
    total_notional = sum(event.notional or 0.0 for event in recent_events)
    long_count = sum(1 for event in recent_events if event.side == "long")
    short_count = sum(1 for event in recent_events if event.side == "short")
    long_notional = sum(event.notional or 0.0 for event in recent_events if event.side == "long")
    short_notional = sum(event.notional or 0.0 for event in recent_events if event.side == "short")
    if long_notional > short_notional:
        dominant = "多头"
    elif short_notional > long_notional:
        dominant = "空头"
    elif long_count > short_count:
        dominant = "多头"
    elif short_count > long_count:
        dominant = "空头"
    elif recent_events:
        dominant = "均衡"
    else:
        dominant = None
    return {
        "count": len(recent_events),
        "notional": total_notional,
        "long_count": long_count,
        "short_count": short_count,
        "long_notional": long_notional,
        "short_notional": short_notional,
        "dominant": dominant,
    }


def build_liquidation_frame(events: List[LiquidationEvent], limit: int = 36) -> pd.DataFrame:
    if not events:
        return pd.DataFrame(columns=["时间", "爆仓方向", "价格", "数量", "名义金额", "来源"])
    rows = []
    for event in sorted(events, key=lambda item: item.timestamp_ms, reverse=True)[:limit]:
        rows.append(
            {
                "时间": pd.to_datetime(event.timestamp_ms, unit="ms"),
                "爆仓方向": _parse_side_label(event.side),
                "价格": event.price,
                "数量": event.size,
                "名义金额": event.notional,
                "来源": event.source,
            }
        )
    return pd.DataFrame(rows)


def build_liquidation_figure(events: List[LiquidationEvent]) -> go.Figure:
    figure = go.Figure()
    if not events:
        figure.add_annotation(text="等待爆仓事件流", showarrow=False, x=0.5, y=0.5, xref="paper", yref="paper")
        figure.update_layout(height=320, margin=dict(l=12, r=12, t=24, b=12))
        return figure

    rows = []
    for event in events:
        if event.price is None:
            continue
        rows.append(
            {
                "ts": pd.to_datetime(event.timestamp_ms, unit="ms"),
                "price": event.price,
                "notional": event.notional or 0.0,
                "side": event.side,
                "label": _parse_side_label(event.side),
            }
        )
    frame = pd.DataFrame(rows)
    if frame.empty:
        figure.add_annotation(text="爆仓事件缺少价格字段", showarrow=False, x=0.5, y=0.5, xref="paper", yref="paper")
        figure.update_layout(height=320, margin=dict(l=12, r=12, t=24, b=12))
        return figure

    max_notional = max(frame["notional"].max(), 1.0)
    frame["marker_size"] = frame["notional"].apply(lambda value: 9 + 24 * math.sqrt(value / max_notional) if value > 0 else 10)
    color_map = {"long": "#ff7b7b", "short": "#5bc0ff"}
    symbol_map = {"long": "triangle-down", "short": "triangle-up"}

    for side in ("long", "short"):
        side_frame = frame[frame["side"] == side]
        if side_frame.empty:
            continue
        figure.add_trace(
            go.Scatter(
                x=side_frame["ts"],
                y=side_frame["price"],
                mode="markers",
                name=_parse_side_label(side),
                marker=dict(
                    size=side_frame["marker_size"],
                    color=color_map.get(side, "#dfe8f1"),
                    symbol=symbol_map.get(side, "circle"),
                    opacity=0.82,
                    line=dict(width=1, color="rgba(7, 17, 27, 0.85)"),
                ),
                customdata=side_frame[["notional"]],
                hovertemplate="时间 %{x}<br>价格 %{y:,.2f}<br>名义金额 %{customdata[0]:,.0f}<extra></extra>",
            )
        )

    figure.update_layout(
        height=320,
        margin=dict(l=12, r=12, t=58, b=12),
        paper_bgcolor="rgba(14, 22, 35, 0.56)",
        plot_bgcolor="rgba(255, 255, 255, 0.045)",
        font=dict(color="#f6f9ff", family="SF Pro Display, Segoe UI, sans-serif"),
        title=dict(text="Executed Liquidations", x=0.03, y=0.98, xanchor="left", font=dict(size=18, color="#f7fbff")),
        transition=dict(duration=300, easing="cubic-in-out"),
        legend=dict(orientation="h", yanchor="bottom", y=1.01, xanchor="right", x=1.0),
    )
    figure.update_xaxes(showgrid=False)
    figure.update_yaxes(showgrid=True, gridcolor="rgba(255, 255, 255, 0.08)", side="right")
    return figure


def _bucket_bounds(reference_price: float, window_pct: float) -> tuple[float, float]:
    lower = reference_price * (1.0 - window_pct / 100.0)
    upper = reference_price * (1.0 + window_pct / 100.0)
    return lower, upper


def _bucket_index(price: float, lower: float, upper: float, bucket_count: int) -> int | None:
    if bucket_count <= 0 or lower >= upper or price < lower or price > upper:
        return None
    ratio = (price - lower) / (upper - lower)
    index = min(bucket_count - 1, max(0, int(ratio * bucket_count)))
    return index


def _bucketize_market_context(
    candles: List[Candle],
    orderbook: List[OrderBookLevel],
    reference_price: float,
    window_pct: float,
    bucket_count: int,
) -> Dict[str, List[float] | float]:
    lower, upper = _bucket_bounds(reference_price, window_pct)
    bid_density = [0.0 for _ in range(bucket_count)]
    ask_density = [0.0 for _ in range(bucket_count)]
    swing_high = [0.0 for _ in range(bucket_count)]
    swing_low = [0.0 for _ in range(bucket_count)]
    bucket_width = (upper - lower) / max(bucket_count, 1)

    for level in orderbook:
        if level.size <= 0:
            continue
        index = _bucket_index(level.price, lower, upper, bucket_count)
        if index is None:
            continue
        notional = level.price * level.size
        if level.side == "bid":
            bid_density[index] += notional
        else:
            ask_density[index] += notional

    for candle in candles:
        range_size = max(candle.high - candle.low, abs(candle.close - candle.open), reference_price * 0.0005)
        wick_weight = 1.0 + range_size / max(reference_price * 0.002, 1e-9)
        high_index = _bucket_index(candle.high, lower, upper, bucket_count)
        low_index = _bucket_index(candle.low, lower, upper, bucket_count)
        if high_index is not None:
            swing_high[high_index] += wick_weight
        if low_index is not None:
            swing_low[low_index] += wick_weight

    if candles:
        lookback = candles[max(0, len(candles) - min(20, len(candles)))]
        momentum = (candles[-1].close - lookback.close) / max(lookback.close, 1e-9)
    else:
        momentum = 0.0

    mids = [lower + bucket_width * (index + 0.5) for index in range(bucket_count)]
    ranges = [
        (lower + bucket_width * index, lower + bucket_width * (index + 1))
        for index in range(bucket_count)
    ]

    return {
        "lower": lower,
        "upper": upper,
        "mids": mids,
        "ranges": ranges,
        "bid_density": _normalize_series(bid_density),
        "ask_density": _normalize_series(ask_density),
        "swing_high": _normalize_series(swing_high),
        "swing_low": _normalize_series(swing_low),
        "momentum": momentum,
    }


def build_probability_heatmap_frame(
    candles: List[Candle],
    orderbook: List[OrderBookLevel],
    snapshot: ExchangeSnapshot,
    scenario: str,
    reference_price: float | None,
    window_pct: float = 8.0,
    bucket_count: int = 28,
) -> pd.DataFrame:
    if reference_price is None or reference_price <= 0 or bucket_count <= 0:
        return pd.DataFrame(columns=EMPTY_FRAME_COLUMNS)

    context = _bucketize_market_context(candles, orderbook, reference_price, window_pct, bucket_count)
    long_crowding = clamp01((snapshot.funding_rate or 0.0) * 6000.0 + 0.18)
    short_crowding = clamp01(-(snapshot.funding_rate or 0.0) * 6000.0 + 0.18)
    up_bias = clamp01(0.5 + context["momentum"] * 8.0)
    down_bias = clamp01(0.5 - context["momentum"] * 8.0)

    oi_scale = 1.0
    if snapshot.open_interest_notional and snapshot.open_interest_notional > 0:
        oi_scale = min(1.9, 0.75 + max(0.0, math.log10(snapshot.open_interest_notional) - 6.0) * 0.22)

    raw_scores: List[float] = []
    rows: List[Dict[str, object]] = []
    max_distance = max(window_pct / 100.0, 0.0001)

    for index, price_mid in enumerate(context["mids"]):
        price_low, price_high = context["ranges"][index]
        below = price_mid < reference_price
        side_label = "下方" if below else "上方"
        same_side_book = context["bid_density"][index] if below else context["ask_density"][index]
        swing_density = context["swing_low"][index] if below else context["swing_high"][index]
        distance_ratio = abs(price_mid - reference_price) / reference_price
        distance_score = clamp01(distance_ratio / max_distance)
        near_score = 1.0 - distance_score
        thin_liquidity = 1.0 - same_side_book

        if scenario == "liquidation":
            crowding = long_crowding if below else short_crowding
            raw_score = (0.42 * swing_density + 0.23 * distance_score + 0.20 * same_side_book + 0.15 * thin_liquidity) * (0.8 + crowding * 1.3) * oi_scale
            reason = "推断下方多头爆仓风险" if below else "推断上方空头爆仓风险"
        elif scenario == "tp":
            directional_bias = up_bias if not below else down_bias
            crowding = long_crowding if not below else short_crowding
            raw_score = (0.46 * swing_density + 0.32 * same_side_book + 0.22 * near_score) * (0.75 + directional_bias * 0.9 + crowding * 0.25) * oi_scale
            reason = "推断上方多头止盈密集" if not below else "推断下方空头止盈密集"
        else:
            crowding = long_crowding if below else short_crowding
            raw_score = (0.48 * swing_density + 0.32 * thin_liquidity + 0.20 * distance_score) * (0.75 + crowding * 0.85) * oi_scale
            reason = "推断下方多头止损池" if below else "推断上方空头止损池"

        if price_low <= reference_price <= price_high:
            raw_score *= 0.35

        raw_scores.append(raw_score)
        rows.append(
            {
                "价格区间": f"{price_low:,.2f} - {price_high:,.2f}",
                "价格中位": price_mid,
                "热度": raw_score,
                "方向": side_label,
                "归因": reason,
            }
        )

    max_score = max(raw_scores, default=0.0)
    if max_score <= 0:
        return pd.DataFrame(columns=EMPTY_FRAME_COLUMNS)

    for row in rows:
        row["热度"] = row["热度"] / max_score

    return pd.DataFrame(rows)


def build_heat_zone_frame(frame: pd.DataFrame, limit: int = 6) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame(columns=["价格区间", "热度", "归因"])
    top_frame = frame.sort_values(["热度", "价格中位"], ascending=[False, True]).head(limit)
    return top_frame[["价格区间", "热度", "归因"]].reset_index(drop=True)


def build_directional_heat_zone_frames(
    frame: pd.DataFrame,
    below_limit: int = 10,
    above_limit: int = 10,
) -> Dict[str, pd.DataFrame]:
    empty = pd.DataFrame(columns=["价格区间", "热度", "归因"])
    if frame.empty:
        return {"below": empty, "above": empty}

    below = (
        frame[frame["方向"] == "下方"]
        .sort_values(["热度", "价格中位"], ascending=[False, False])
        .head(below_limit)[["价格区间", "热度", "归因"]]
        .reset_index(drop=True)
    )
    above = (
        frame[frame["方向"] == "上方"]
        .sort_values(["热度", "价格中位"], ascending=[False, True])
        .head(above_limit)[["价格区间", "热度", "归因"]]
        .reset_index(drop=True)
    )
    return {"below": below, "above": above}


def build_heatmap_figure(
    frame: pd.DataFrame,
    title: str,
    reference_price: float | None,
    colorscale: list,
    empty_text: str,
) -> go.Figure:
    figure = go.Figure()
    if frame.empty:
        figure.add_annotation(text=empty_text, showarrow=False, x=0.5, y=0.5, xref="paper", yref="paper")
        figure.update_layout(height=380, margin=dict(l=12, r=12, t=24, b=12))
        return figure

    display = frame.copy()
    display["方向标签"] = [
        "现价上方" if reference_price is not None and value >= reference_price else "现价下方"
        for value in display["价格中位"]
    ]
    display["提示"] = [
        f"{direction} | {reason}"
        for direction, reason in zip(display["方向标签"], display["归因"])
    ]
    figure.add_trace(
        go.Bar(
            x=display["热度"],
            y=display["价格区间"],
            orientation="h",
            marker=dict(
                color=display["热度"],
                colorscale=colorscale,
                cmin=0.0,
                cmax=1.0,
                line=dict(width=0),
            ),
            text=[f"{value:.2f}" for value in display["热度"]],
            textposition="outside",
            customdata=display[["价格中位", "提示"]],
            hovertemplate="%{y}<br>价格中位 %{customdata[0]:,.2f}<br>%{customdata[1]}<br>热度 %{x:.2f}<extra></extra>",
        )
    )
    figure.update_layout(
        height=380,
        margin=dict(l=12, r=12, t=58, b=12),
        paper_bgcolor="rgba(14, 22, 35, 0.56)",
        plot_bgcolor="rgba(255, 255, 255, 0.045)",
        font=dict(color="#f6f9ff", family="SF Pro Display, Segoe UI, sans-serif"),
        title=dict(text=title, x=0.03, y=0.98, xanchor="left", font=dict(size=18, color="#f7fbff")),
        transition=dict(duration=280, easing="cubic-in-out"),
        annotations=[
            dict(
                x=1.0,
                y=1.08,
                xref="paper",
                yref="paper",
                xanchor="right",
                showarrow=False,
                font=dict(size=12, color="#d3e0f2"),
                text=f"现价 {reference_price:,.2f}" if reference_price is not None else "现价参考缺失",
            )
        ],
    )
    figure.update_xaxes(showgrid=True, gridcolor="rgba(255, 255, 255, 0.08)", range=[0, 1.05], title="热度分数")
    figure.update_yaxes(showgrid=False, autorange="reversed")
    return figure


def _mbo_color(side: str, intensity: float) -> str:
    if side == "bid":
        red, green, blue = (39, 156, 255)
    else:
        red, green, blue = (216, 69, 45)
    alpha = 0.35 + 0.55 * clamp01(intensity)
    return f"rgba({red}, {green}, {blue}, {alpha:.3f})"


def build_mbo_profile_frame(
    levels: List[OrderBookLevel],
    reference_price: float | None,
    rows_per_side: int = 14,
) -> pd.DataFrame:
    if not levels or reference_price is None or reference_price <= 0:
        return pd.DataFrame(
            columns=[
                "方向",
                "价格",
                "挂单量",
                "名义金额",
                "累积名义金额",
                "距现价(bps)",
                "盘口占比",
                "队列压力",
                "吸收分数",
                "signed_notional",
                "side",
            ]
        )

    bid_levels = sorted([level for level in levels if level.side == "bid" and level.size > 0], key=lambda item: item.price, reverse=True)[:rows_per_side]
    ask_levels = sorted([level for level in levels if level.side == "ask" and level.size > 0], key=lambda item: item.price)[:rows_per_side]
    bid_total = sum(level.price * level.size for level in bid_levels)
    ask_total = sum(level.price * level.size for level in ask_levels)

    rows: List[Dict[str, float | str]] = []
    cumulative = 0.0
    for level in bid_levels:
        notional = level.price * level.size
        cumulative += notional
        distance_bps = (level.price - reference_price) / reference_price * 10000.0
        share = notional / bid_total if bid_total > 0 else 0.0
        queue_pressure = level.size / max(abs(distance_bps), 1.0)
        rows.append(
            {
                "方向": "买盘",
                "价格": level.price,
                "挂单量": level.size,
                "名义金额": notional,
                "累积名义金额": cumulative,
                "距现价(bps)": distance_bps,
                "盘口占比": share,
                "队列压力": queue_pressure,
                "吸收分数": 0.0,
                "signed_notional": notional,
                "side": "bid",
            }
        )

    cumulative = 0.0
    for level in ask_levels:
        notional = level.price * level.size
        cumulative += notional
        distance_bps = (level.price - reference_price) / reference_price * 10000.0
        share = notional / ask_total if ask_total > 0 else 0.0
        queue_pressure = level.size / max(abs(distance_bps), 1.0)
        rows.append(
            {
                "方向": "卖盘",
                "价格": level.price,
                "挂单量": level.size,
                "名义金额": notional,
                "累积名义金额": cumulative,
                "距现价(bps)": distance_bps,
                "盘口占比": share,
                "队列压力": queue_pressure,
                "吸收分数": 0.0,
                "signed_notional": -notional,
                "side": "ask",
            }
        )

    frame = pd.DataFrame(rows)
    if frame.empty:
        return frame

    pressure_norm = _normalize_series(frame["队列压力"].tolist())
    share_norm = _normalize_series(frame["盘口占比"].tolist())
    frame["吸收分数"] = [0.55 * share + 0.45 * pressure for share, pressure in zip(share_norm, pressure_norm)]
    return frame.sort_values("价格", ascending=False).reset_index(drop=True)


def build_mbo_figure(frame: pd.DataFrame, reference_price: float | None) -> go.Figure:
    figure = go.Figure()
    if frame.empty:
        figure.add_annotation(text="等待盘口深度", showarrow=False, x=0.5, y=0.5, xref="paper", yref="paper")
        figure.update_layout(height=420, margin=dict(l=12, r=12, t=24, b=12))
        return figure

    figure.add_vline(x=0, line_width=1, line_color="rgba(223, 232, 241, 0.22)")
    for side in ("bid", "ask"):
        side_frame = frame[frame["side"] == side].sort_values("价格")
        if side_frame.empty:
            continue
        figure.add_trace(
            go.Bar(
                x=side_frame["signed_notional"],
                y=side_frame["价格"],
                orientation="h",
                name="买盘梯级" if side == "bid" else "卖盘梯级",
                marker_color=[_mbo_color(side, value) for value in side_frame["吸收分数"]],
                text=[f"{value:.0%}" if value >= 0.12 else "" for value in side_frame["盘口占比"]],
                textposition="outside",
                customdata=side_frame[["挂单量", "名义金额", "盘口占比", "队列压力", "吸收分数"]],
                hovertemplate="价格 %{y:,.2f}<br>挂单量 %{customdata[0]:,.2f}<br>名义金额 %{customdata[1]:,.0f}<br>盘口占比 %{customdata[2]:.2%}<br>队列压力 %{customdata[3]:.2f}<br>吸收分数 %{customdata[4]:.2f}<extra></extra>",
            )
        )

    if reference_price is not None:
        figure.add_hline(y=reference_price, line_color="#f8d35e", line_dash="dot", line_width=1)

    figure.update_layout(
        height=420,
        barmode="overlay",
        margin=dict(l=12, r=12, t=58, b=12),
        paper_bgcolor="rgba(14, 22, 35, 0.56)",
        plot_bgcolor="rgba(255, 255, 255, 0.045)",
        font=dict(color="#f6f9ff", family="SF Pro Display, Segoe UI, sans-serif"),
        title=dict(text="Queue Pressure & Absorption", x=0.03, y=0.98, xanchor="left", font=dict(size=18, color="#f7fbff")),
        transition=dict(duration=300, easing="cubic-in-out"),
        legend=dict(orientation="h", yanchor="bottom", y=1.01, xanchor="right", x=1.0),
        annotations=[
            dict(
                x=0.5,
                y=1.08,
                xref="paper",
                yref="paper",
                showarrow=False,
                font=dict(size=12, color="#d3e0f2"),
                text="左边越长=卖盘压制更强，右边越长=买盘支撑更强；颜色越深=越可能吸收成交",
            )
        ],
    )
    figure.update_xaxes(showgrid=True, gridcolor="rgba(255, 255, 255, 0.08)", title="挂单压力（左卖盘 / 右买盘）")
    figure.update_yaxes(showgrid=False, side="right")
    return figure


def build_trade_metrics(
    events: List[TradeEvent],
    now_ms: int | None = None,
    window_minutes: int = 15,
) -> Dict[str, float | int | str | None]:
    if now_ms is None:
        now_ms = int(time.time() * 1000)
    cutoff_ms = now_ms - max(window_minutes, 1) * 60_000
    recent_events = [event for event in events if event.timestamp_ms >= cutoff_ms]
    if not recent_events:
        return {
            "count": 0,
            "buy_notional": 0.0,
            "sell_notional": 0.0,
            "delta_notional": 0.0,
            "buy_ratio": None,
            "price_change_pct": None,
            "regime": None,
        }

    buy_notional = sum(event.notional or 0.0 for event in recent_events if event.side == "buy")
    sell_notional = sum(event.notional or 0.0 for event in recent_events if event.side == "sell")
    total_notional = buy_notional + sell_notional
    buy_ratio = buy_notional / total_notional if total_notional > 0 else None
    first_price = next((event.price for event in recent_events if event.price), None)
    last_price = next((event.price for event in reversed(recent_events) if event.price), None)
    price_change_pct = None
    if first_price and last_price:
        price_change_pct = (last_price - first_price) / first_price * 100.0
    delta_notional = buy_notional - sell_notional
    regime = infer_trade_regime(delta_notional, price_change_pct)
    return {
        "count": len(recent_events),
        "buy_notional": buy_notional,
        "sell_notional": sell_notional,
        "delta_notional": delta_notional,
        "buy_ratio": buy_ratio,
        "price_change_pct": price_change_pct,
        "regime": regime,
    }


def infer_trade_regime(delta_notional: float, price_change_pct: float | None) -> str | None:
    if price_change_pct is None:
        return None
    flat = abs(price_change_pct) <= 0.08
    if delta_notional > 0 and price_change_pct > 0.08:
        return "多头主动推进"
    if delta_notional < 0 and price_change_pct < -0.08:
        return "空头主动推进"
    if delta_notional > 0 and flat:
        return "上方卖盘吸收"
    if delta_notional < 0 and flat:
        return "下方买盘吸收"
    if delta_notional > 0 and price_change_pct < -0.08:
        return "多头衰竭 / 被动承接"
    if delta_notional < 0 and price_change_pct > 0.08:
        return "空头衰竭 / 被动承接"
    return "信号混合"


def build_trade_frame(events: List[TradeEvent], limit: int = 40) -> pd.DataFrame:
    if not events:
        return pd.DataFrame(columns=["时间", "主动方向", "价格", "数量", "名义金额", "来源"])
    rows = []
    for event in sorted(events, key=lambda item: item.timestamp_ms, reverse=True)[:limit]:
        rows.append(
            {
                "时间": pd.to_datetime(event.timestamp_ms, unit="ms"),
                "主动方向": "主动买" if event.side == "buy" else "主动卖" if event.side == "sell" else event.side,
                "价格": event.price,
                "数量": event.size,
                "名义金额": event.notional,
                "来源": event.source,
            }
        )
    return pd.DataFrame(rows)


def build_cvd_figure(
    events: List[TradeEvent],
    now_ms: int | None = None,
    window_minutes: int = 30,
) -> go.Figure:
    figure = make_subplots(
        rows=2,
        cols=1,
        shared_xaxes=True,
        vertical_spacing=0.05,
        row_heights=[0.72, 0.28],
        specs=[[{"secondary_y": True}], [{"secondary_y": False}]],
    )
    if now_ms is None:
        now_ms = int(time.time() * 1000)
    cutoff_ms = now_ms - max(window_minutes, 1) * 60_000
    recent_events = [event for event in events if event.timestamp_ms >= cutoff_ms and event.price is not None]
    if not recent_events:
        figure.add_annotation(text="等待逐笔成交流", showarrow=False, x=0.5, y=0.5, xref="paper", yref="paper")
        figure.update_layout(height=360, margin=dict(l=12, r=12, t=24, b=12))
        return figure

    frame = pd.DataFrame(
        {
            "ts": pd.to_datetime([event.timestamp_ms for event in recent_events], unit="ms"),
            "price": [event.price for event in recent_events],
            "signed_notional": [
                (event.notional or 0.0) if event.side == "buy" else -(event.notional or 0.0) for event in recent_events
            ],
        }
    )
    frame["cvd"] = frame["signed_notional"].cumsum()
    flow_buckets = frame.assign(bucket=lambda item: item["ts"].dt.floor("min")).groupby("bucket", as_index=False).agg(
        delta_notional=("signed_notional", "sum"),
        last_price=("price", "last"),
    )
    price_change_pct = None
    if recent_events[0].price and recent_events[-1].price:
        price_change_pct = (recent_events[-1].price - recent_events[0].price) / recent_events[0].price * 100.0
    regime = infer_trade_regime(frame["signed_notional"].sum(), price_change_pct)

    figure.add_trace(
        go.Scatter(
            x=frame["ts"],
            y=frame["cvd"],
            mode="lines",
            name="CVD",
            line=dict(color="#67d1ff", width=2.4),
            fill="tozeroy",
            fillcolor="rgba(103, 209, 255, 0.12)",
            hovertemplate="时间 %{x}<br>CVD %{y:,.0f}<extra></extra>",
        ),
        row=1,
        col=1,
        secondary_y=False,
    )
    figure.add_trace(
        go.Scatter(
            x=frame["ts"],
            y=frame["price"],
            mode="lines",
            name="价格",
            line=dict(color="#ffd76b", width=1.7),
            hovertemplate="时间 %{x}<br>价格 %{y:,.2f}<extra></extra>",
        ),
        row=1,
        col=1,
        secondary_y=True,
    )
    figure.add_trace(
        go.Bar(
            x=flow_buckets["bucket"],
            y=flow_buckets["delta_notional"],
            marker_color=["#57b06b" if value >= 0 else "#ff7b7b" for value in flow_buckets["delta_notional"]],
            name="每分钟主动净流",
            hovertemplate="时间 %{x}<br>净主动成交 %{y:,.0f}<extra></extra>",
        ),
        row=2,
        col=1,
        secondary_y=False,
    )
    figure.add_hline(y=0, line_color="rgba(223, 232, 241, 0.22)", line_width=1, row=2, col=1)
    figure.update_layout(
        height=420,
        margin=dict(l=12, r=12, t=58, b=12),
        paper_bgcolor="rgba(14, 22, 35, 0.56)",
        plot_bgcolor="rgba(255, 255, 255, 0.045)",
        font=dict(color="#f6f9ff", family="SF Pro Display, Segoe UI, sans-serif"),
        title=dict(text=f"主动成交流与 CVD（累计主动买卖差） · {regime or '等待判断'}", x=0.03, y=0.98, xanchor="left", font=dict(size=18, color="#f7fbff")),
        transition=dict(duration=300, easing="cubic-in-out"),
        legend=dict(orientation="h", yanchor="bottom", y=1.01, xanchor="right", x=1.0),
    )
    figure.update_xaxes(showgrid=False)
    figure.update_yaxes(showgrid=True, gridcolor="rgba(255, 255, 255, 0.08)", title="CVD（累计主动买卖差）", row=1, col=1, secondary_y=False)
    figure.update_yaxes(showgrid=False, title="价格", row=1, col=1, secondary_y=True)
    figure.update_yaxes(showgrid=True, gridcolor="rgba(255, 255, 255, 0.08)", title="净主动成交", row=2, col=1, secondary_y=False)
    return figure


def build_oi_quadrant_metrics(points: List[OIPoint], candles: List[Candle]) -> Dict[str, float | str | None]:
    valid_points = []
    for point in points:
        value = point.open_interest_notional if point.open_interest_notional is not None else point.open_interest
        if value is not None:
            valid_points.append((point.timestamp_ms, value, "持仓金额" if point.open_interest_notional is not None else "持仓量"))
    if len(valid_points) < 2 or len(candles) < 2:
        return {
            "price_change_pct": None,
            "oi_change_pct": None,
            "label": None,
            "confidence": None,
            "value_label": None,
        }

    oi_lookback = min(12, len(valid_points) - 1)
    price_lookback = min(12, len(candles) - 1)
    current_oi_ts, current_oi_value, current_label = valid_points[-1]
    anchor_oi_ts, anchor_oi_value, _ = valid_points[-(oi_lookback + 1)]
    current_price = candles[-1].close
    anchor_price = candles[-(price_lookback + 1)].close

    price_change_pct = (current_price - anchor_price) / max(anchor_price, 1e-9) * 100.0
    oi_change_pct = (current_oi_value - anchor_oi_value) / max(abs(anchor_oi_value), 1e-9) * 100.0
    confidence = clamp01(min(1.0, (abs(price_change_pct) / 2.5) + (abs(oi_change_pct) / 4.0)))

    if price_change_pct >= 0 and oi_change_pct >= 0:
        label = "多头主动加仓"
    elif price_change_pct >= 0 and oi_change_pct < 0:
        label = "空头回补占优"
    elif price_change_pct < 0 and oi_change_pct >= 0:
        label = "空头主动加仓"
    else:
        label = "多头减仓 / 多头爆仓"

    return {
        "price_change_pct": price_change_pct,
        "oi_change_pct": oi_change_pct,
        "label": label,
        "confidence": confidence,
        "value_label": current_label,
        "start_ts": anchor_oi_ts,
        "end_ts": current_oi_ts,
    }


def build_oi_quadrant_figure(metrics: Dict[str, float | str | None]) -> go.Figure:
    figure = go.Figure()
    price_change_pct = metrics.get("price_change_pct")
    oi_change_pct = metrics.get("oi_change_pct")
    if price_change_pct is None or oi_change_pct is None:
        figure.add_annotation(text="等待 OI 与价格联合样本", showarrow=False, x=0.5, y=0.5, xref="paper", yref="paper")
        figure.update_layout(height=340, margin=dict(l=12, r=12, t=24, b=12))
        return figure

    axis_limit = max(1.0, abs(float(price_change_pct)) * 1.5, abs(float(oi_change_pct)) * 1.5)
    quadrants = [
        ((0, axis_limit), (0, axis_limit), "rgba(80, 194, 126, 0.16)"),
        ((0, axis_limit), (-axis_limit, 0), "rgba(244, 208, 110, 0.14)"),
        ((-axis_limit, 0), (0, axis_limit), "rgba(98, 194, 255, 0.14)"),
        ((-axis_limit, 0), (-axis_limit, 0), "rgba(255, 123, 123, 0.14)"),
    ]
    for x_range, y_range, color in quadrants:
        figure.add_shape(
            type="rect",
            x0=x_range[0],
            x1=x_range[1],
            y0=y_range[0],
            y1=y_range[1],
            fillcolor=color,
            line_width=0,
            layer="below",
        )

    figure.add_hline(y=0, line_width=1, line_color="rgba(223, 232, 241, 0.22)")
    figure.add_vline(x=0, line_width=1, line_color="rgba(223, 232, 241, 0.22)")
    figure.add_trace(
        go.Scatter(
            x=[price_change_pct],
            y=[oi_change_pct],
            mode="markers+text",
            text=[metrics.get("label") or ""],
            textposition="top center",
            marker=dict(
                size=20 + 24 * float(metrics.get("confidence") or 0.0),
                color="#f8d35e",
                line=dict(width=1.5, color="#081421"),
            ),
            hovertemplate="价格变化 %{x:.2f}%<br>OI 变化 %{y:.2f}%<extra></extra>",
            showlegend=False,
        )
    )
    figure.update_layout(
        height=340,
        margin=dict(l=12, r=12, t=58, b=12),
        paper_bgcolor="rgba(14, 22, 35, 0.56)",
        plot_bgcolor="rgba(255, 255, 255, 0.045)",
        font=dict(color="#f6f9ff", family="SF Pro Display, Segoe UI, sans-serif"),
        title=dict(text="OI Delta / Positioning Quadrants", x=0.03, y=0.98, xanchor="left", font=dict(size=18, color="#f7fbff")),
        transition=dict(duration=300, easing="cubic-in-out"),
    )
    figure.update_xaxes(
        range=[-axis_limit, axis_limit],
        title="价格变化 (%)",
        showgrid=True,
        gridcolor="rgba(255, 255, 255, 0.08)",
    )
    figure.update_yaxes(
        range=[-axis_limit, axis_limit],
        title="OI 变化 (%)",
        showgrid=True,
        gridcolor="rgba(255, 255, 255, 0.08)",
    )
    return figure


def _cluster_liquidations(
    events: List[LiquidationEvent],
    cluster_window_seconds: int = 30,
) -> List[Dict[str, object]]:
    if not events:
        return []
    ordered = sorted(events, key=lambda item: item.timestamp_ms)
    clusters: List[Dict[str, object]] = []
    current: Dict[str, object] | None = None
    window_ms = max(cluster_window_seconds, 5) * 1000

    for event in ordered:
        if current is None or event.timestamp_ms - int(current["last_ts"]) > window_ms:
            if current is not None:
                clusters.append(current)
            current = {
                "start_ts": event.timestamp_ms,
                "end_ts": event.timestamp_ms,
                "last_ts": event.timestamp_ms,
                "events": 0,
                "notional": 0.0,
                "long_count": 0,
                "short_count": 0,
                "long_notional": 0.0,
                "short_notional": 0.0,
                "exchanges": set(),
            }
        current["last_ts"] = event.timestamp_ms
        current["end_ts"] = event.timestamp_ms
        current["events"] = int(current["events"]) + 1
        current["notional"] = float(current["notional"]) + (event.notional or 0.0)
        current["exchanges"].add(event.exchange)
        if event.side == "long":
            current["long_count"] = int(current["long_count"]) + 1
            current["long_notional"] = float(current["long_notional"]) + (event.notional or 0.0)
        elif event.side == "short":
            current["short_count"] = int(current["short_count"]) + 1
            current["short_notional"] = float(current["short_notional"]) + (event.notional or 0.0)

    if current is not None:
        clusters.append(current)
    return clusters


def build_liquidation_cluster_frame(
    events: List[LiquidationEvent],
    cluster_window_seconds: int = 30,
    limit: int = 12,
) -> pd.DataFrame:
    if not events:
        return pd.DataFrame(
            columns=["开始时间", "持续秒数", "类别", "主导方向", "交易所数", "事件数", "多头爆仓额", "空头爆仓额", "总名义金额", "交易所"]
        )
    rows = []
    for cluster in reversed(_cluster_liquidations(events, cluster_window_seconds)[-limit:]):
        if float(cluster["long_notional"]) > float(cluster["short_notional"]):
            dominant = "多头爆仓"
        elif float(cluster["short_notional"]) > float(cluster["long_notional"]):
            dominant = "空头爆仓"
        else:
            dominant = "混合"
        exchanges = sorted(cluster["exchanges"])
        rows.append(
            {
                "开始时间": pd.to_datetime(cluster["start_ts"], unit="ms"),
                "持续秒数": max(1, round((int(cluster["end_ts"]) - int(cluster["start_ts"])) / 1000.0, 1)),
                "类别": "跨所联动" if len(exchanges) > 1 else "单所爆仓",
                "主导方向": dominant,
                "交易所数": len(exchanges),
                "事件数": cluster["events"],
                "多头爆仓额": cluster["long_notional"],
                "空头爆仓额": cluster["short_notional"],
                "总名义金额": cluster["notional"],
                "交易所": " / ".join(exchanges),
            }
        )
    return pd.DataFrame(rows)


def build_cross_exchange_liquidation_frame(
    events: List[LiquidationEvent],
    cluster_window_seconds: int = 30,
    limit: int = 12,
) -> pd.DataFrame:
    frame = build_liquidation_cluster_frame(events, cluster_window_seconds=cluster_window_seconds, limit=limit)
    if frame.empty:
        return pd.DataFrame(columns=["时间", "主导方向", "交易所数", "事件数", "总名义金额", "交易所"])
    cross_only = frame[frame["类别"] == "跨所联动"].copy()
    if cross_only.empty:
        return pd.DataFrame(columns=["时间", "主导方向", "交易所数", "事件数", "总名义金额", "交易所"])
    cross_only = cross_only.rename(columns={"开始时间": "时间"})[["时间", "主导方向", "交易所数", "事件数", "总名义金额", "交易所"]]
    return cross_only.reset_index(drop=True)


def build_liquidation_truth_summary(
    events: List[LiquidationEvent],
    now_ms: int | None = None,
    window_minutes: int = 60,
    cluster_window_seconds: int = 30,
) -> Dict[str, float | int | str | None]:
    metrics = build_liquidation_metrics(events, now_ms=now_ms, window_minutes=window_minutes)
    cutoff_ms = (now_ms or int(time.time() * 1000)) - max(window_minutes, 1) * 60_000
    recent_events = [event for event in events if event.timestamp_ms >= cutoff_ms]
    clusters = _cluster_liquidations(recent_events, cluster_window_seconds=cluster_window_seconds)
    cross_clusters = [cluster for cluster in clusters if len(cluster["exchanges"]) > 1]
    single_clusters = [cluster for cluster in clusters if len(cluster["exchanges"]) <= 1]
    metrics.update(
        {
            "cluster_count": len(clusters),
            "cross_cluster_count": len(cross_clusters),
            "single_cluster_count": len(single_clusters),
            "cross_cluster_notional": sum(float(cluster["notional"]) for cluster in cross_clusters),
            "single_cluster_notional": sum(float(cluster["notional"]) for cluster in single_clusters),
        }
    )
    return metrics


def build_liquidation_cluster_figure(
    events: List[LiquidationEvent],
    cluster_window_seconds: int = 30,
    limit: int = 14,
) -> go.Figure:
    figure = go.Figure()
    frame = build_liquidation_cluster_frame(events, cluster_window_seconds=cluster_window_seconds, limit=limit)
    if frame.empty:
        figure.add_annotation(text="等待 30 秒爆仓簇形成", showarrow=False, x=0.5, y=0.5, xref="paper", yref="paper")
        figure.update_layout(height=320, margin=dict(l=12, r=12, t=24, b=12))
        return figure

    frame = frame.iloc[::-1].reset_index(drop=True)
    frame["标签"] = [
        f"{category} | {direction}"
        for category, direction in zip(frame["类别"], frame["主导方向"])
    ]
    figure.add_trace(
        go.Bar(
            x=frame["总名义金额"],
            y=frame["开始时间"],
            orientation="h",
            marker=dict(
                color=["#ff9a59" if label == "跨所联动" else "#67d1ff" for label in frame["类别"]],
                line=dict(width=0),
            ),
            text=[f"{value:,.0f}" for value in frame["总名义金额"]],
            textposition="outside",
            customdata=frame[["标签", "事件数", "交易所"]],
            hovertemplate="%{y}<br>%{customdata[0]}<br>事件数 %{customdata[1]}<br>%{customdata[2]}<br>总额 %{x:,.0f}<extra></extra>",
        )
    )
    figure.update_layout(
        height=320,
        margin=dict(l=12, r=12, t=58, b=12),
        paper_bgcolor="rgba(14, 22, 35, 0.56)",
        plot_bgcolor="rgba(255, 255, 255, 0.045)",
        font=dict(color="#f6f9ff", family="SF Pro Display, Segoe UI, sans-serif"),
        title=dict(text="Liquidation Clusters (30s)", x=0.03, y=0.98, xanchor="left", font=dict(size=18, color="#f7fbff")),
        transition=dict(duration=300, easing="cubic-in-out"),
        showlegend=False,
    )
    figure.update_xaxes(showgrid=True, gridcolor="rgba(255, 255, 255, 0.08)", tickformat=".2s", title="名义金额")
    figure.update_yaxes(showgrid=False)
    return figure


def build_funding_comparison_figure(snapshots: List[ExchangeSnapshot]) -> go.Figure:
    figure = go.Figure()
    frame = pd.DataFrame(
        [
            {"交易所": snapshot.exchange, "费率(bps)": snapshot.funding_bps}
            for snapshot in snapshots
            if snapshot.funding_bps is not None
        ]
    )
    if frame.empty:
        figure.add_annotation(text="当前没有可比较的资金费率", showarrow=False, x=0.5, y=0.5, xref="paper", yref="paper")
        figure.update_layout(height=320, margin=dict(l=12, r=12, t=24, b=12))
        return figure

    colors = ["#57b06b" if value >= 0 else "#ff7b7b" for value in frame["费率(bps)"]]
    figure.add_trace(
        go.Bar(
            x=frame["交易所"],
            y=frame["费率(bps)"],
            marker_color=colors,
            text=[f"{value:.2f}" for value in frame["费率(bps)"]],
            textposition="outside",
            hovertemplate="%{x}<br>资金费率 %{y:.2f} bps<extra></extra>",
        )
    )
    figure.add_hline(y=0, line_color="rgba(223, 232, 241, 0.22)", line_width=1)
    figure.update_layout(
        height=320,
        margin=dict(l=12, r=12, t=58, b=12),
        paper_bgcolor="rgba(14, 22, 35, 0.56)",
        plot_bgcolor="rgba(255, 255, 255, 0.045)",
        font=dict(color="#f6f9ff", family="SF Pro Display, Segoe UI, sans-serif"),
        title=dict(text="Funding Rate Comparison", x=0.03, y=0.98, xanchor="left", font=dict(size=18, color="#f7fbff")),
        transition=dict(duration=300, easing="cubic-in-out"),
        showlegend=False,
    )
    figure.update_xaxes(showgrid=False)
    figure.update_yaxes(showgrid=True, gridcolor="rgba(255, 255, 255, 0.08)", title="bps")
    return figure


def build_basis_comparison_figure(snapshots: List[ExchangeSnapshot]) -> go.Figure:
    figure = go.Figure()
    frame = pd.DataFrame(
        [
            {
                "交易所": snapshot.exchange,
                "溢价(%)": snapshot.premium_pct,
                "Basis": (snapshot.last_price - snapshot.mark_price) if snapshot.last_price is not None and snapshot.mark_price is not None else None,
            }
            for snapshot in snapshots
            if snapshot.premium_pct is not None
        ]
    )
    if frame.empty:
        figure.add_annotation(text="当前没有可比较的 Basis / Premium", showarrow=False, x=0.5, y=0.5, xref="paper", yref="paper")
        figure.update_layout(height=320, margin=dict(l=12, r=12, t=24, b=12))
        return figure

    colors = ["#67d1ff" if value >= 0 else "#ff9a59" for value in frame["溢价(%)"]]
    figure.add_trace(
        go.Bar(
            x=frame["交易所"],
            y=frame["溢价(%)"],
            marker_color=colors,
            text=[f"{value:.3f}%" for value in frame["溢价(%)"]],
            textposition="outside",
            hovertemplate="%{x}<br>溢价 %{y:.3f}%<extra></extra>",
        )
    )
    figure.add_hline(y=0, line_color="rgba(223, 232, 241, 0.22)", line_width=1)
    figure.update_layout(
        height=320,
        margin=dict(l=12, r=12, t=58, b=12),
        paper_bgcolor="rgba(14, 22, 35, 0.56)",
        plot_bgcolor="rgba(255, 255, 255, 0.045)",
        font=dict(color="#f6f9ff", family="SF Pro Display, Segoe UI, sans-serif"),
        title=dict(text="Basis / Premium Snapshot", x=0.03, y=0.98, xanchor="left", font=dict(size=18, color="#f7fbff")),
        transition=dict(duration=300, easing="cubic-in-out"),
        showlegend=False,
    )
    figure.update_xaxes(showgrid=False)
    figure.update_yaxes(showgrid=True, gridcolor="rgba(255, 255, 255, 0.08)", title="%")
    return figure


def build_carry_surface_frame(rows: List[Dict[str, float | str | None]]) -> pd.DataFrame:
    columns = [
        "交易所",
        "现货锚定",
        "Basis来源",
        "Basis(%)",
        "Funding(bps)",
        "Carry倾斜(bps)",
        "年化Funding(%)",
        "24h成交额比",
        "OI金额",
        "OI份额(%)",
    ]
    if not rows:
        return pd.DataFrame(columns=columns)
    frame = pd.DataFrame(rows)
    for column in columns:
        if column not in frame.columns:
            frame[column] = None
    sort_base = pd.to_numeric(frame.get("Carry倾斜(bps)"), errors="coerce").abs()
    if sort_base.isna().all():
        sort_base = pd.to_numeric(frame.get("Funding(bps)"), errors="coerce").abs()
    frame["_sort"] = sort_base.fillna(0.0)
    return frame[columns + ["_sort"]].sort_values("_sort", ascending=False).drop(columns="_sort").reset_index(drop=True)


def build_carry_surface_figure(frame: pd.DataFrame) -> go.Figure:
    figure = go.Figure()
    metric_columns = [
        "Basis(%)",
        "Funding(bps)",
        "Carry倾斜(bps)",
        "年化Funding(%)",
        "OI份额(%)",
    ]
    available_columns = [column for column in metric_columns if column in frame.columns and frame[column].notna().any()]
    if frame.empty or not available_columns:
        figure.add_annotation(text="等待 Carry / Basis / Funding 曲面样本", showarrow=False, x=0.5, y=0.5, xref="paper", yref="paper")
        figure.update_layout(height=340, margin=dict(l=12, r=12, t=24, b=12))
        return figure

    z_values: List[List[float]] = []
    text_values: List[List[str]] = []
    for _, row in frame.iterrows():
        z_row: List[float] = []
        text_row: List[str] = []
        for column in available_columns:
            numeric_value = pd.to_numeric(row.get(column), errors="coerce")
            z_row.append(0.0 if pd.isna(numeric_value) else float(numeric_value))
            if pd.isna(numeric_value):
                text_row.append("-")
            elif column.endswith("(%)"):
                text_row.append(f"{float(numeric_value):.2f}%")
            elif column.endswith("(bps)"):
                text_row.append(f"{float(numeric_value):.2f}")
            else:
                text_row.append(f"{float(numeric_value):.1f}")
        z_values.append(z_row)
        text_values.append(text_row)

    normalized_z = []
    for row_index in range(len(z_values)):
        normalized_z.append([0.0 for _ in available_columns])
    for col_index, column in enumerate(available_columns):
        series = pd.to_numeric(frame[column], errors="coerce").fillna(0.0)
        scale = max(float(series.abs().max()), 1e-9)
        for row_index, value in enumerate(series):
            normalized_z[row_index][col_index] = float(value) / scale

    figure.add_trace(
        go.Heatmap(
            z=normalized_z,
            x=available_columns,
            y=frame["交易所"],
            text=text_values,
            texttemplate="%{text}",
            colorscale=[(0.0, "#8a2836"), (0.5, "#17324d"), (1.0, "#4dbd88")],
            zmid=0.0,
            hovertemplate="交易所 %{y}<br>维度 %{x}<br>数值 %{text}<extra></extra>",
            showscale=False,
        )
    )
    figure.update_layout(
        height=max(320, 96 + 56 * len(frame)),
        margin=dict(l=12, r=12, t=58, b=12),
        paper_bgcolor="rgba(14, 22, 35, 0.56)",
        plot_bgcolor="rgba(255, 255, 255, 0.045)",
        font=dict(color="#f6f9ff", family="SF Pro Display, Segoe UI, sans-serif"),
        title=dict(text="Carry / Basis / Funding Surface", x=0.03, y=0.98, xanchor="left", font=dict(size=18, color="#f7fbff")),
        transition=dict(duration=300, easing="cubic-in-out"),
    )
    figure.update_xaxes(showgrid=False)
    figure.update_yaxes(showgrid=False)
    return figure


def build_binance_crowd_figure(payload: Dict[str, List[dict]]) -> go.Figure:
    figure = go.Figure()
    if not payload:
        figure.add_annotation(text="等待 Binance crowd / taker 数据", showarrow=False, x=0.5, y=0.5, xref="paper", yref="paper")
        figure.update_layout(height=340, margin=dict(l=12, r=12, t=24, b=12))
        return figure

    trace_specs = [
        ("top_position", "大户持仓比", "#67d1ff"),
        ("top_account", "大户账户比", "#ffd76b"),
        ("global_account", "全市场账户比", "#ff9a59"),
        ("taker_ratio", "主动买卖比", "#6fdd8c"),
    ]
    has_trace = False
    for key, label, color in trace_specs:
        items = payload.get(key) or []
        if not items:
            continue
        frame = pd.DataFrame(items)
        if "timestamp" not in frame or "longShortRatio" not in frame and "buySellRatio" not in frame:
            continue
        value_col = "buySellRatio" if "buySellRatio" in frame else "longShortRatio"
        frame["ts"] = pd.to_datetime(frame["timestamp"], unit="ms")
        frame["value"] = pd.to_numeric(frame[value_col], errors="coerce")
        frame = frame.dropna(subset=["value"])
        if frame.empty:
            continue
        has_trace = True
        figure.add_trace(
            go.Scatter(
                x=frame["ts"],
                y=frame["value"],
                mode="lines",
                name=label,
                line=dict(color=color, width=2.1),
                hovertemplate="时间 %{x}<br>比值 %{y:.3f}<extra></extra>",
            )
        )

    if not has_trace:
        figure.add_annotation(text="Binance crowd / taker 数据为空", showarrow=False, x=0.5, y=0.5, xref="paper", yref="paper")
        figure.update_layout(height=340, margin=dict(l=12, r=12, t=24, b=12))
        return figure

    figure.add_hline(y=1.0, line_color="rgba(223, 232, 241, 0.22)", line_width=1, line_dash="dot")
    figure.update_layout(
        height=340,
        margin=dict(l=12, r=12, t=58, b=12),
        paper_bgcolor="rgba(14, 22, 35, 0.56)",
        plot_bgcolor="rgba(255, 255, 255, 0.045)",
        font=dict(color="#f6f9ff", family="SF Pro Display, Segoe UI, sans-serif"),
        title=dict(text="Binance Crowd / Taker Positioning", x=0.03, y=0.98, xanchor="left", font=dict(size=18, color="#f7fbff")),
        transition=dict(duration=300, easing="cubic-in-out"),
        legend=dict(orientation="h", yanchor="bottom", y=1.01, xanchor="right", x=1.0),
    )
    figure.update_xaxes(showgrid=False)
    figure.update_yaxes(showgrid=True, gridcolor="rgba(255, 255, 255, 0.08)", title="ratio")
    return figure


def build_open_interest_comparison_figure(snapshots: List[ExchangeSnapshot]) -> go.Figure:
    figure = go.Figure()
    frame = pd.DataFrame(
        [
            {
                "交易所": snapshot.exchange,
                "持仓金额": snapshot.open_interest_notional,
                "持仓量": snapshot.open_interest,
            }
            for snapshot in snapshots
            if snapshot.open_interest_notional is not None or snapshot.open_interest is not None
        ]
    )
    if frame.empty:
        figure.add_annotation(text="当前没有可比较的未平仓数据", showarrow=False, x=0.5, y=0.5, xref="paper", yref="paper")
        figure.update_layout(height=320, margin=dict(l=12, r=12, t=24, b=12))
        return figure

    value_col = "持仓金额" if frame["持仓金额"].notna().sum() >= max(1, len(frame) // 2) else "持仓量"
    figure.add_trace(
        go.Bar(
            x=frame["交易所"],
            y=frame[value_col],
            marker_color="#67d1ff",
            text=[f"{value:,.0f}" if pd.notna(value) else "-" for value in frame[value_col]],
            textposition="outside",
            hovertemplate="%{x}<br>" + value_col + " %{y:,.0f}<extra></extra>",
        )
    )
    figure.update_layout(
        height=320,
        margin=dict(l=12, r=12, t=58, b=12),
        paper_bgcolor="rgba(14, 22, 35, 0.56)",
        plot_bgcolor="rgba(255, 255, 255, 0.045)",
        font=dict(color="#f6f9ff", family="SF Pro Display, Segoe UI, sans-serif"),
        title=dict(text=f"{value_col}对比", x=0.03, y=0.98, xanchor="left", font=dict(size=18, color="#f7fbff")),
        transition=dict(duration=300, easing="cubic-in-out"),
        showlegend=False,
    )
    figure.update_xaxes(showgrid=False)
    figure.update_yaxes(showgrid=True, gridcolor="rgba(255, 255, 255, 0.08)", tickformat=".2s")
    return figure


def build_open_interest_frame(snapshots: List[ExchangeSnapshot]) -> pd.DataFrame:
    frame = pd.DataFrame(
        [
            {
                "交易所": snapshot.exchange,
                "持仓量": snapshot.open_interest,
                "持仓金额": snapshot.open_interest_notional,
                "资金费率(bps)": snapshot.funding_bps,
            }
            for snapshot in snapshots
            if snapshot.open_interest is not None or snapshot.open_interest_notional is not None
        ]
    )
    if frame.empty:
        return pd.DataFrame(columns=["交易所", "持仓量", "持仓金额", "资金费率(bps)", "份额(%)"])
    share_base = frame["持仓金额"] if frame["持仓金额"].notna().sum() else frame["持仓量"]
    total = share_base.fillna(0.0).sum()
    frame["份额(%)"] = share_base.fillna(0.0) / total * 100.0 if total > 0 else 0.0
    return frame.sort_values("持仓金额" if frame["持仓金额"].notna().sum() else "持仓量", ascending=False).reset_index(drop=True)


def build_spot_perp_metrics(
    spot_snapshot: SpotSnapshot | None,
    perp_snapshot: ExchangeSnapshot | None,
    spot_orderbook: List[OrderBookLevel],
    perp_orderbook: List[OrderBookLevel],
    spot_trades: List[TradeEvent],
) -> Dict[str, float | str | None]:
    if spot_snapshot is None or perp_snapshot is None:
        return {
            "basis_pct": None,
            "spot_spread_bps": None,
            "perp_spread_bps": None,
            "spot_volume_ratio": None,
            "spot_buy_ratio": None,
        }

    basis_pct = None
    if spot_snapshot.last_price not in (None, 0) and perp_snapshot.last_price is not None:
        basis_pct = (perp_snapshot.last_price - spot_snapshot.last_price) / spot_snapshot.last_price * 100.0

    spot_summary = summarize_orderbook(spot_orderbook, spot_snapshot.last_price)
    perp_summary = summarize_orderbook(perp_orderbook, perp_snapshot.last_price)
    spot_volume_ratio = None
    if spot_snapshot.volume_24h_notional not in (None, 0) and perp_snapshot.volume_24h_notional is not None:
        spot_volume_ratio = perp_snapshot.volume_24h_notional / spot_snapshot.volume_24h_notional

    trade_metrics = build_trade_metrics(spot_trades, window_minutes=15)
    return {
        "basis_pct": basis_pct,
        "spot_spread_bps": spot_snapshot.spread_bps or spot_summary.get("spread_bps"),
        "perp_spread_bps": perp_summary.get("spread_bps"),
        "spot_volume_ratio": spot_volume_ratio,
        "spot_buy_ratio": trade_metrics.get("buy_ratio"),
    }


def build_spot_perp_figure(
    spot_snapshot: SpotSnapshot | None,
    perp_snapshot: ExchangeSnapshot | None,
) -> go.Figure:
    figure = make_subplots(rows=1, cols=2, subplot_titles=("价格对比", "24h 成交额对比"))
    if spot_snapshot is None or perp_snapshot is None or spot_snapshot.status != "ok" or perp_snapshot.status != "ok":
        figure.add_annotation(text="等待现货 / 合约快照", showarrow=False, x=0.5, y=0.5, xref="paper", yref="paper")
        figure.update_layout(height=360, margin=dict(l=12, r=12, t=32, b=12))
        return figure

    figure.add_trace(
        go.Bar(
            x=["现货", "永续"],
            y=[spot_snapshot.last_price or 0.0, perp_snapshot.last_price or 0.0],
            marker_color=["#67d1ff", "#ffd76b"],
            text=[f"{spot_snapshot.last_price:,.2f}" if spot_snapshot.last_price is not None else "-", f"{perp_snapshot.last_price:,.2f}" if perp_snapshot.last_price is not None else "-"],
            textposition="outside",
            showlegend=False,
            hovertemplate="%{x}<br>价格 %{y:,.2f}<extra></extra>",
        ),
        row=1,
        col=1,
    )
    figure.add_trace(
        go.Bar(
            x=["现货", "永续"],
            y=[spot_snapshot.volume_24h_notional or 0.0, perp_snapshot.volume_24h_notional or 0.0],
            marker_color=["#5bc0ff", "#ff9a59"],
            text=[
                f"{spot_snapshot.volume_24h_notional:,.0f}" if spot_snapshot.volume_24h_notional is not None else "-",
                f"{perp_snapshot.volume_24h_notional:,.0f}" if perp_snapshot.volume_24h_notional is not None else "-",
            ],
            textposition="outside",
            showlegend=False,
            hovertemplate="%{x}<br>24h 成交额 %{y:,.0f}<extra></extra>",
        ),
        row=1,
        col=2,
    )
    figure.update_layout(
        height=360,
        margin=dict(l=12, r=12, t=68, b=12),
        paper_bgcolor="rgba(14, 22, 35, 0.56)",
        plot_bgcolor="rgba(255, 255, 255, 0.045)",
        font=dict(color="#f6f9ff", family="SF Pro Display, Segoe UI, sans-serif"),
        title=dict(text="Spot vs Perp Snapshot", x=0.03, y=0.98, xanchor="left", font=dict(size=18, color="#f7fbff")),
        transition=dict(duration=300, easing="cubic-in-out"),
    )
    figure.update_xaxes(showgrid=False)
    figure.update_yaxes(showgrid=True, gridcolor="rgba(255, 255, 255, 0.08)", tickformat=".2s", row=1, col=1)
    figure.update_yaxes(showgrid=True, gridcolor="rgba(255, 255, 255, 0.08)", tickformat=".2s", row=1, col=2)
    return figure


def build_spot_perp_flow_figure(
    spot_events: List[TradeEvent],
    perp_events: List[TradeEvent],
    now_ms: int | None = None,
    window_minutes: int = 30,
) -> go.Figure:
    figure = go.Figure()
    if now_ms is None:
        now_ms = int(time.time() * 1000)
    cutoff_ms = now_ms - max(window_minutes, 1) * 60_000

    def to_cvd_frame(events: List[TradeEvent], label: str) -> pd.DataFrame:
        recent = [event for event in events if event.timestamp_ms >= cutoff_ms]
        if not recent:
            return pd.DataFrame(columns=["ts", "cvd", "label"])
        frame = pd.DataFrame(
            {
                "ts": pd.to_datetime([event.timestamp_ms for event in recent], unit="ms"),
                "signed_notional": [
                    (event.notional or 0.0) if event.side == "buy" else -(event.notional or 0.0) for event in recent
                ],
            }
        )
        frame["cvd"] = frame["signed_notional"].cumsum()
        frame["label"] = label
        return frame

    spot_frame = to_cvd_frame(spot_events, "现货")
    perp_frame = to_cvd_frame(perp_events, "永续")
    if spot_frame.empty and perp_frame.empty:
        figure.add_annotation(text="等待现货 / 永续实时流", showarrow=False, x=0.5, y=0.5, xref="paper", yref="paper")
        figure.update_layout(height=320, margin=dict(l=12, r=12, t=24, b=12))
        return figure

    for frame, color in ((spot_frame, "#67d1ff"), (perp_frame, "#ffd76b")):
        if frame.empty:
            continue
        figure.add_trace(
            go.Scatter(
                x=frame["ts"],
                y=frame["cvd"],
                mode="lines",
                name=frame["label"].iloc[0],
                line=dict(color=color, width=2.3),
                hovertemplate="时间 %{x}<br>CVD %{y:,.0f}<extra></extra>",
            )
        )

    figure.add_hline(y=0, line_color="rgba(223, 232, 241, 0.22)", line_width=1)
    figure.update_layout(
        height=320,
        margin=dict(l=12, r=12, t=58, b=12),
        paper_bgcolor="rgba(14, 22, 35, 0.56)",
        plot_bgcolor="rgba(255, 255, 255, 0.045)",
        font=dict(color="#f6f9ff", family="SF Pro Display, Segoe UI, sans-serif"),
        title=dict(text="Spot vs Perp Real-Time Flow", x=0.03, y=0.98, xanchor="left", font=dict(size=18, color="#f7fbff")),
        transition=dict(duration=300, easing="cubic-in-out"),
        legend=dict(orientation="h", yanchor="bottom", y=1.01, xanchor="right", x=1.0),
    )
    figure.update_xaxes(showgrid=False)
    figure.update_yaxes(showgrid=True, gridcolor="rgba(255, 255, 255, 0.08)", title="CVD")
    return figure


def build_oi_multiframe_matrix_frame(metrics_by_interval: Dict[str, Dict[str, float | str | None]]) -> pd.DataFrame:
    rows = []
    score_map = {
        "多头主动加仓": 2.0,
        "空头回补占优": 1.0,
        "多头减仓 / 多头爆仓": -1.0,
        "空头主动加仓": -2.0,
    }
    for interval, metrics in metrics_by_interval.items():
        score = score_map.get(str(metrics.get("label")), 0.0) * float(metrics.get("confidence") or 0.0)
        rows.append(
            {
                "周期": interval,
                "状态": metrics.get("label"),
                "价格变化(%)": metrics.get("price_change_pct"),
                "OI变化(%)": metrics.get("oi_change_pct"),
                "置信度": (metrics.get("confidence") or 0.0) * 100.0 if metrics.get("confidence") is not None else None,
                "score": score,
            }
        )
    return pd.DataFrame(rows)


def build_oi_multiframe_matrix_figure(metrics_by_interval: Dict[str, Dict[str, float | str | None]]) -> go.Figure:
    figure = go.Figure()
    frame = build_oi_multiframe_matrix_frame(metrics_by_interval)
    if frame.empty:
        figure.add_annotation(text="等待多时间框架 OI 数据", showarrow=False, x=0.5, y=0.5, xref="paper", yref="paper")
        figure.update_layout(height=220, margin=dict(l=12, r=12, t=24, b=12))
        return figure

    figure.add_trace(
        go.Heatmap(
            z=[frame["score"].tolist()],
            x=frame["周期"].tolist(),
            y=["OI Delta Matrix"],
            text=[[f"{row['状态'] or '等待'}<br>价格 {row['价格变化(%)'] if pd.notna(row['价格变化(%)']) else '-'}%<br>OI {row['OI变化(%)'] if pd.notna(row['OI变化(%)']) else '-'}%" for _, row in frame.iterrows()]],
            hovertemplate="%{text}<extra></extra>",
            colorscale=[(0.0, "#ff7b7b"), (0.35, "#ffd76b"), (0.5, "#f6f9ff"), (0.7, "#67d1ff"), (1.0, "#57b06b")],
            zmin=-2.0,
            zmax=2.0,
            showscale=False,
        )
    )
    figure.update_layout(
        height=220,
        margin=dict(l=12, r=12, t=58, b=12),
        paper_bgcolor="rgba(14, 22, 35, 0.56)",
        plot_bgcolor="rgba(255, 255, 255, 0.045)",
        font=dict(color="#f6f9ff", family="SF Pro Display, Segoe UI, sans-serif"),
        title=dict(text="Multi-Timeframe OI Delta Matrix", x=0.03, y=0.98, xanchor="left", font=dict(size=18, color="#f7fbff")),
        transition=dict(duration=300, easing="cubic-in-out"),
    )
    figure.update_xaxes(showgrid=False)
    figure.update_yaxes(showgrid=False)
    return figure


def build_term_structure_figure(payload: Dict[str, List[dict]]) -> go.Figure:
    figure = make_subplots(specs=[[{"secondary_y": True}]])
    rows = []
    for contract_type, items in payload.items():
        if not items:
            continue
        latest = items[-1]
        rows.append(
            {
                "合约": contract_type,
                "期货价": pd.to_numeric(latest.get("futuresPrice"), errors="coerce"),
                "基差": pd.to_numeric(latest.get("basis"), errors="coerce"),
                "基差率(%)": pd.to_numeric(latest.get("basisRate"), errors="coerce") * 100.0,
                "年化基差(%)": pd.to_numeric(latest.get("annualizedBasisRate"), errors="coerce") * 100.0,
            }
        )
    frame = pd.DataFrame(rows)
    if frame.empty:
        figure.add_annotation(text="等待期限结构数据", showarrow=False, x=0.5, y=0.5, xref="paper", yref="paper")
        figure.update_layout(height=340, margin=dict(l=12, r=12, t=24, b=12))
        return figure

    figure.add_trace(
        go.Bar(
            x=frame["合约"],
            y=frame["年化基差(%)"].fillna(frame["基差率(%)"]),
            marker_color=["#67d1ff", "#ffd76b", "#ff9a59"][: len(frame)],
            text=[f"{value:.2f}%" if pd.notna(value) else "-" for value in frame["年化基差(%)"].fillna(frame["基差率(%)"])],
            textposition="outside",
            name="年化基差",
            hovertemplate="%{x}<br>年化基差 %{y:.2f}%<extra></extra>",
        ),
        secondary_y=False,
    )
    figure.add_trace(
        go.Scatter(
            x=frame["合约"],
            y=frame["期货价"],
            mode="lines+markers",
            line=dict(color="#f6f9ff", width=2),
            marker=dict(size=8, color="#f6f9ff"),
            name="期货价",
            hovertemplate="%{x}<br>期货价 %{y:,.2f}<extra></extra>",
        ),
        secondary_y=True,
    )
    figure.update_layout(
        height=340,
        margin=dict(l=12, r=12, t=58, b=12),
        paper_bgcolor="rgba(14, 22, 35, 0.56)",
        plot_bgcolor="rgba(255, 255, 255, 0.045)",
        font=dict(color="#f6f9ff", family="SF Pro Display, Segoe UI, sans-serif"),
        title=dict(text="Quarterly / Delivery Term Structure", x=0.03, y=0.98, xanchor="left", font=dict(size=18, color="#f7fbff")),
        transition=dict(duration=300, easing="cubic-in-out"),
        legend=dict(orientation="h", yanchor="bottom", y=1.01, xanchor="right", x=1.0),
    )
    figure.update_xaxes(showgrid=False)
    figure.update_yaxes(showgrid=True, gridcolor="rgba(255, 255, 255, 0.08)", title="年化基差(%)", secondary_y=False)
    figure.update_yaxes(showgrid=False, title="期货价", secondary_y=True)
    return figure


def build_term_structure_frame(payload: Dict[str, List[dict]]) -> pd.DataFrame:
    rows = []
    for contract_type, items in payload.items():
        if not items:
            continue
        latest = items[-1]
        rows.append(
            {
                "合约": contract_type,
                "指数价": pd.to_numeric(latest.get("indexPrice"), errors="coerce"),
                "期货价": pd.to_numeric(latest.get("futuresPrice"), errors="coerce"),
                "基差": pd.to_numeric(latest.get("basis"), errors="coerce"),
                "基差率(%)": pd.to_numeric(latest.get("basisRate"), errors="coerce") * 100.0,
                "年化基差(%)": pd.to_numeric(latest.get("annualizedBasisRate"), errors="coerce") * 100.0,
            }
        )
    return pd.DataFrame(rows)


def build_liquidation_waterfall_figure(
    events: List[LiquidationEvent],
    now_ms: int | None = None,
    window_minutes: int = 120,
    bucket_minutes: int = 5,
) -> go.Figure:
    figure = go.Figure()
    if now_ms is None:
        now_ms = int(time.time() * 1000)
    cutoff_ms = now_ms - max(window_minutes, 1) * 60_000
    recent_events = [event for event in events if event.timestamp_ms >= cutoff_ms]
    if not recent_events:
        figure.add_annotation(text="等待跨所爆仓样本", showarrow=False, x=0.5, y=0.5, xref="paper", yref="paper")
        figure.update_layout(height=320, margin=dict(l=12, r=12, t=24, b=12))
        return figure

    frame = pd.DataFrame(
        {
            "bucket": [pd.to_datetime(event.timestamp_ms, unit="ms").floor(f"{bucket_minutes}min") for event in recent_events],
            "signed_notional": [
                -(event.notional or 0.0) if event.side == "long" else (event.notional or 0.0) for event in recent_events
            ],
        }
    )
    waterfall = frame.groupby("bucket", as_index=False).agg(net_liq=("signed_notional", "sum"))
    figure.add_trace(
        go.Bar(
            x=waterfall["bucket"],
            y=waterfall["net_liq"],
            marker_color=["#5bc0ff" if value >= 0 else "#ff7b7b" for value in waterfall["net_liq"]],
            hovertemplate="时间 %{x}<br>净爆仓 %{y:,.0f}<extra></extra>",
        )
    )
    figure.add_hline(y=0, line_color="rgba(223, 232, 241, 0.22)", line_width=1)
    figure.update_layout(
        height=320,
        margin=dict(l=12, r=12, t=58, b=12),
        paper_bgcolor="rgba(14, 22, 35, 0.56)",
        plot_bgcolor="rgba(255, 255, 255, 0.045)",
        font=dict(color="#f6f9ff", family="SF Pro Display, Segoe UI, sans-serif"),
        title=dict(text="Liquidation Waterfall", x=0.03, y=0.98, xanchor="left", font=dict(size=18, color="#f7fbff")),
        transition=dict(duration=300, easing="cubic-in-out"),
        showlegend=False,
    )
    figure.update_xaxes(showgrid=False)
    figure.update_yaxes(showgrid=True, gridcolor="rgba(255, 255, 255, 0.08)", title="净爆仓额")
    return figure


def build_liquidation_linkage_heatmap(
    events: List[LiquidationEvent],
    now_ms: int | None = None,
    window_minutes: int = 120,
    bucket_minutes: int = 5,
) -> go.Figure:
    figure = go.Figure()
    if now_ms is None:
        now_ms = int(time.time() * 1000)
    cutoff_ms = now_ms - max(window_minutes, 1) * 60_000
    recent_events = [event for event in events if event.timestamp_ms >= cutoff_ms]
    if not recent_events:
        figure.add_annotation(text="等待联动热区样本", showarrow=False, x=0.5, y=0.5, xref="paper", yref="paper")
        figure.update_layout(height=320, margin=dict(l=12, r=12, t=24, b=12))
        return figure

    frame = pd.DataFrame(
        {
            "exchange": [event.exchange for event in recent_events],
            "bucket": [pd.to_datetime(event.timestamp_ms, unit="ms").floor(f"{bucket_minutes}min") for event in recent_events],
            "notional": [event.notional or 0.0 for event in recent_events],
        }
    )
    pivot = frame.pivot_table(index="exchange", columns="bucket", values="notional", aggfunc="sum", fill_value=0.0)
    figure.add_trace(
        go.Heatmap(
            z=pivot.values,
            x=list(pivot.columns),
            y=list(pivot.index),
            colorscale=[(0.0, "#102016"), (0.5, "#2ca7ff"), (1.0, "#ffd76b")],
            hovertemplate="交易所 %{y}<br>时间 %{x}<br>爆仓额 %{z:,.0f}<extra></extra>",
            showscale=False,
        )
    )
    figure.update_layout(
        height=320,
        margin=dict(l=12, r=12, t=58, b=12),
        paper_bgcolor="rgba(14, 22, 35, 0.56)",
        plot_bgcolor="rgba(255, 255, 255, 0.045)",
        font=dict(color="#f6f9ff", family="SF Pro Display, Segoe UI, sans-serif"),
        title=dict(text="Cross-Exchange Liquidation Linkage", x=0.03, y=0.98, xanchor="left", font=dict(size=18, color="#f7fbff")),
        transition=dict(duration=300, easing="cubic-in-out"),
    )
    figure.update_xaxes(showgrid=False)
    figure.update_yaxes(showgrid=False)
    return figure


def build_spot_perp_exchange_frame(rows: List[Dict[str, float | str | None]]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame(
            columns=[
                "交易所",
                "现货价格",
                "永续价格",
                "Basis(%)",
                "现货价差(bps)",
                "永续价差(bps)",
                "永续/现货成交额比",
                "现货主动买占比(%)",
                "现货盘口失衡(%)",
                "合约盘口失衡(%)",
                "现货24h成交额",
                "合约24h成交额",
                "合约持仓量",
                "合约持仓金额",
                "资金费率(bps)",
            ]
        )
    return pd.DataFrame(rows)


def build_spot_perp_exchange_figure(frame: pd.DataFrame) -> go.Figure:
    figure = make_subplots(specs=[[{"secondary_y": True}]])
    if frame.empty:
        figure.add_annotation(text="等待多交易所现货 / 合约对照", showarrow=False, x=0.5, y=0.5, xref="paper", yref="paper")
        figure.update_layout(height=340, margin=dict(l=12, r=12, t=24, b=12))
        return figure

    figure.add_trace(
        go.Bar(
            x=frame["交易所"],
            y=frame["Basis(%)"],
            marker_color=["#67d1ff" if value is not None and value >= 0 else "#ff9a59" for value in frame["Basis(%)"]],
            text=[f"{value:.3f}%" if pd.notna(value) else "-" for value in frame["Basis(%)"]],
            textposition="outside",
            name="Basis",
            hovertemplate="%{x}<br>Basis %{y:.3f}%<extra></extra>",
        ),
        secondary_y=False,
    )
    figure.add_trace(
        go.Scatter(
            x=frame["交易所"],
            y=frame["永续/现货成交额比"],
            mode="lines+markers",
            line=dict(color="#ffd76b", width=2.0),
            marker=dict(size=8, color="#ffd76b"),
            name="永续/现货成交额比",
            hovertemplate="%{x}<br>成交额比 %{y:.2f}x<extra></extra>",
        ),
        secondary_y=True,
    )
    figure.update_layout(
        height=340,
        margin=dict(l=12, r=12, t=58, b=12),
        paper_bgcolor="rgba(14, 22, 35, 0.56)",
        plot_bgcolor="rgba(255, 255, 255, 0.045)",
        font=dict(color="#f6f9ff", family="SF Pro Display, Segoe UI, sans-serif"),
        title=dict(text="Spot vs Perp Across Exchanges", x=0.03, y=0.98, xanchor="left", font=dict(size=18, color="#f7fbff")),
        transition=dict(duration=300, easing="cubic-in-out"),
        legend=dict(orientation="h", yanchor="bottom", y=1.01, xanchor="right", x=1.0),
    )
    figure.update_xaxes(showgrid=False)
    figure.update_yaxes(showgrid=True, gridcolor="rgba(255, 255, 255, 0.08)", title="Basis(%)", secondary_y=False)
    figure.update_yaxes(showgrid=False, title="永续/现货成交额比", secondary_y=True)
    return figure


def compute_spot_perp_lead_lag(
    spot_events: List[TradeEvent],
    perp_events: List[TradeEvent],
    now_ms: int | None = None,
    lookback_minutes: int = 15,
    bucket_seconds: int = 10,
    max_lag_buckets: int = 6,
) -> Dict[str, float | str | None]:
    if now_ms is None:
        now_ms = int(time.time() * 1000)
    cutoff_ms = now_ms - max(lookback_minutes, 1) * 60_000
    spot_recent = [event for event in spot_events if event.timestamp_ms >= cutoff_ms and event.price is not None]
    perp_recent = [event for event in perp_events if event.timestamp_ms >= cutoff_ms and event.price is not None]
    if len(spot_recent) < 6 or len(perp_recent) < 6:
        return {"leader": None, "lag_seconds": None, "correlation": None, "summary": None, "samples": 0, "confidence": 0.0}

    def build_return_series(events: List[TradeEvent]) -> pd.Series:
        frame = pd.DataFrame(
            {
                "ts": pd.to_datetime([event.timestamp_ms for event in events], unit="ms"),
                "price": [event.price for event in events],
            }
        )
        frame["bucket"] = frame["ts"].dt.floor(f"{bucket_seconds}s")
        series = frame.groupby("bucket")["price"].last().sort_index()
        series = series.resample(f"{bucket_seconds}s").last().ffill()
        return series.pct_change().fillna(0.0)

    spot_returns = build_return_series(spot_recent)
    perp_returns = build_return_series(perp_recent)
    joined = pd.concat({"spot": spot_returns, "perp": perp_returns}, axis=1).dropna()
    if len(joined) < 3:
        return {"leader": None, "lag_seconds": None, "correlation": None, "summary": None, "samples": len(joined), "confidence": 0.0}
    effective_max_lag = min(max_lag_buckets, max(1, len(joined) - 2))

    best_lag = 0
    best_corr: float | None = None
    for lag in range(-effective_max_lag, effective_max_lag + 1):
        shifted = joined["spot"].shift(lag)
        frame = pd.concat([shifted, joined["perp"]], axis=1).dropna()
        if len(frame) < 3:
            continue
        if frame.iloc[:, 0].std() == 0 or frame.iloc[:, 1].std() == 0:
            continue
        corr = frame.iloc[:, 0].corr(frame.iloc[:, 1])
        if pd.isna(corr):
            continue
        if best_corr is None or abs(corr) > abs(best_corr):
            best_corr = corr
            best_lag = lag

    if best_corr is None:
        return {"leader": None, "lag_seconds": None, "correlation": None, "summary": None, "samples": len(joined), "confidence": 0.0}

    lag_seconds = abs(best_lag) * bucket_seconds
    if abs(best_corr) < 0.18:
        summary = "基本同步"
        leader = "同步"
    elif best_lag > 0:
        summary = f"现货领先 {lag_seconds}s"
        leader = "现货"
    elif best_lag < 0:
        summary = f"永续领先 {lag_seconds}s"
        leader = "永续"
    else:
        summary = "基本同步"
        leader = "同步"
    if len(joined) < 6:
        summary = f"短样本 · {summary}"
    confidence = clamp01(abs(float(best_corr)) * 0.78 + min(1.0, len(joined) / 24.0) * 0.22)
    return {
        "leader": leader,
        "lag_seconds": lag_seconds,
        "correlation": best_corr,
        "summary": summary,
        "samples": len(joined),
        "confidence": confidence,
    }


def build_binance_ratio_breakdown_figure(payload: Dict[str, List[dict]]) -> go.Figure:
    figure = go.Figure()
    specs = [
        ("top_position", "大户持仓占比"),
        ("top_account", "大户账户占比"),
        ("global_account", "全市场账户占比"),
    ]
    rows = []
    for key, label in specs:
        items = payload.get(key) or []
        if not items:
            continue
        latest = items[-1]
        rows.append(
            {
                "维度": label,
                "多头占比": pd.to_numeric(latest.get("longAccount"), errors="coerce") * 100.0,
                "空头占比": pd.to_numeric(latest.get("shortAccount"), errors="coerce") * 100.0,
            }
        )
    frame = pd.DataFrame(rows)
    if frame.empty:
        figure.add_annotation(text="等待 Binance 多空占比数据", showarrow=False, x=0.5, y=0.5, xref="paper", yref="paper")
        figure.update_layout(height=320, margin=dict(l=12, r=12, t=24, b=12))
        return figure

    figure.add_trace(
        go.Bar(
            x=frame["维度"],
            y=frame["多头占比"],
            name="多头占比",
            marker_color="#57b06b",
            text=[f"{value:.1f}%" for value in frame["多头占比"]],
            textposition="inside",
        )
    )
    figure.add_trace(
        go.Bar(
            x=frame["维度"],
            y=frame["空头占比"],
            name="空头占比",
            marker_color="#ff7b7b",
            text=[f"{value:.1f}%" for value in frame["空头占比"]],
            textposition="inside",
        )
    )
    figure.update_layout(
        height=320,
        barmode="stack",
        margin=dict(l=12, r=12, t=58, b=12),
        paper_bgcolor="rgba(14, 22, 35, 0.56)",
        plot_bgcolor="rgba(255, 255, 255, 0.045)",
        font=dict(color="#f6f9ff", family="SF Pro Display, Segoe UI, sans-serif"),
        title=dict(text="Binance Long / Short Share Breakdown", x=0.03, y=0.98, xanchor="left", font=dict(size=18, color="#f7fbff")),
        transition=dict(duration=300, easing="cubic-in-out"),
        legend=dict(orientation="h", yanchor="bottom", y=1.01, xanchor="right", x=1.0),
    )
    figure.update_xaxes(showgrid=False)
    figure.update_yaxes(showgrid=True, gridcolor="rgba(255, 255, 255, 0.08)", title="占比(%)", range=[0, 100])
    return figure


def build_binance_crowding_alerts(
    payload: Dict[str, List[dict]],
    funding_bps: float | None,
    oi_change_pct: float | None,
    trade_metrics: Dict[str, float | int | str | None] | None = None,
) -> pd.DataFrame:
    def latest(dataset_key: str, field: str) -> float | None:
        items = payload.get(dataset_key) or []
        if not items:
            return None
        return pd.to_numeric(items[-1].get(field), errors="coerce")

    top_position = latest("top_position", "longShortRatio")
    top_account = latest("top_account", "longShortRatio")
    global_account = latest("global_account", "longShortRatio")
    taker_ratio = latest("taker_ratio", "buySellRatio")

    rows = []
    oi_change_pct = oi_change_pct or 0.0
    if top_position is not None and top_account is not None and funding_bps is not None:
        if top_position > 1.05 and top_account > 1.03 and funding_bps > 1.2 and oi_change_pct > 0.4:
            rows.append({"告警": "多头拥挤预警", "等级": "高", "解释": "大户持仓和账户都明显偏多，资金费率抬升，未平仓继续扩张。"})
        elif top_position < 0.95 and top_account < 0.97 and funding_bps < -1.2 and oi_change_pct > 0.4:
            rows.append({"告警": "空头拥挤预警", "等级": "高", "解释": "大户持仓和账户都明显偏空，资金费率走低，未平仓继续扩张。"})
        elif top_position > 1.02 and top_account > 1.01 and funding_bps > 0.6:
            rows.append({"告警": "多头偏拥挤", "等级": "中", "解释": "大户多头略占优，资金费率同步偏正，注意追多拥挤。"})
        elif top_position < 0.98 and top_account < 0.99 and funding_bps < -0.6:
            rows.append({"告警": "空头偏拥挤", "等级": "中", "解释": "大户空头略占优，资金费率同步偏负，注意追空拥挤。"})
    if top_position is not None and funding_bps is not None and oi_change_pct < -0.3:
        if top_position > 1.03 and funding_bps > 0.8:
            rows.append({"告警": "多头拥挤松动", "等级": "观察", "解释": "多头仍偏拥挤，但 OI 已开始回落，可能进入减仓或多头止盈阶段。"})
        elif top_position < 0.97 and funding_bps < -0.8:
            rows.append({"告警": "空头拥挤松动", "等级": "观察", "解释": "空头仍偏拥挤，但 OI 已开始回落，可能进入回补阶段。"})
    if top_position is not None and global_account is not None:
        if top_position > 1.0 and global_account < 1.0:
            rows.append({"告警": "大户偏多 / 散户偏空", "等级": "中", "解释": "大户持仓方向和全市场账户方向出现背离。"})
        elif top_position < 1.0 and global_account > 1.0:
            rows.append({"告警": "大户偏空 / 散户偏多", "等级": "中", "解释": "大户持仓方向和全市场账户方向出现背离。"})
    if taker_ratio is not None:
        if taker_ratio > 1.2:
            rows.append({"告警": "主动买加速", "等级": "高", "解释": "主动买卖比明显偏向买方，短线扫单正在加速。"})
        elif taker_ratio > 1.08:
            rows.append({"告警": "主动买明显占优", "等级": "中", "解释": "主动买卖比明显偏向买方，短线扫单更积极。"})
        elif taker_ratio < 0.8:
            rows.append({"告警": "主动卖加速", "等级": "高", "解释": "主动买卖比明显偏向卖方，短线抛压正在加速。"})
        elif taker_ratio < 0.92:
            rows.append({"告警": "主动卖明显占优", "等级": "中", "解释": "主动买卖比明显偏向卖方，短线抛压更主动。"})
    if trade_metrics and trade_metrics.get("regime"):
        rows.append({"告警": "流动性状态", "等级": "观察", "解释": str(trade_metrics.get("regime"))})
    if not rows:
        rows.append({"告警": "暂无拥挤告警", "等级": "低", "解释": "当前公开比率没有形成明显的极端拥挤。"})
    priority_map = {"高": 0, "中": 1, "观察": 2, "低": 3}
    frame = pd.DataFrame(rows)
    frame["_priority"] = frame["等级"].map(priority_map).fillna(9)
    return frame.sort_values(["_priority", "告警"]).drop(columns="_priority").reset_index(drop=True)


def _latest_dataset_float(payload: Dict[str, List[dict]], dataset_key: str, field: str) -> float | None:
    items = payload.get(dataset_key) or []
    if not items:
        return None
    value = pd.to_numeric(items[-1].get(field), errors="coerce")
    return None if pd.isna(value) else float(value)


def _latest_dataset_timestamp(payload: Dict[str, List[dict]], dataset_key: str) -> int | None:
    items = payload.get(dataset_key) or []
    if not items:
        return None
    value = pd.to_numeric(items[-1].get("timestamp"), errors="coerce")
    return None if pd.isna(value) else int(value)


def _ratio_window_label(value: str) -> str:
    return RATIO_WINDOW_LABELS.get(str(value or "").strip().lower(), str(value or "5m"))


def _ratio_snapshot(items: List[dict], *, ratio_window: str) -> Dict[str, float | int | None]:
    if not items:
        return {"timestamp": None, "ratio": None, "long_pct": None, "short_pct": None}
    frame = pd.DataFrame(items)
    if frame.empty or "timestamp" not in frame.columns:
        return {"timestamp": None, "ratio": None, "long_pct": None, "short_pct": None}
    frame["timestamp"] = pd.to_numeric(frame["timestamp"], errors="coerce")
    frame["longShortRatio"] = pd.to_numeric(frame.get("longShortRatio"), errors="coerce")
    if "longAccount" in frame.columns:
        frame["longAccount"] = pd.to_numeric(frame["longAccount"], errors="coerce")
    if "shortAccount" in frame.columns:
        frame["shortAccount"] = pd.to_numeric(frame["shortAccount"], errors="coerce")
    frame = frame.dropna(subset=["timestamp"]).sort_values("timestamp")
    if frame.empty:
        return {"timestamp": None, "ratio": None, "long_pct": None, "short_pct": None}
    if str(ratio_window or "").strip().lower() == "1w":
        working = frame.tail(7).copy()
        ratio_value = working["longShortRatio"].dropna().mean() if "longShortRatio" in working.columns else None
        long_value = working["longAccount"].dropna().mean() if "longAccount" in working.columns else None
        short_value = working["shortAccount"].dropna().mean() if "shortAccount" in working.columns else None
        timestamp_value = int(working["timestamp"].iloc[-1]) if not working.empty else None
    else:
        latest = frame.iloc[-1]
        ratio_value = latest.get("longShortRatio")
        long_value = latest.get("longAccount")
        short_value = latest.get("shortAccount")
        timestamp_value = int(latest.get("timestamp")) if pd.notna(latest.get("timestamp")) else None
    ratio_numeric = None if pd.isna(ratio_value) else float(ratio_value)
    long_pct = None if pd.isna(long_value) else float(long_value) * 100.0
    short_pct = None if pd.isna(short_value) else float(short_value) * 100.0
    if (long_pct is None or short_pct is None) and ratio_numeric not in (None, 0):
        computed_long = float(ratio_numeric) / (1.0 + float(ratio_numeric)) * 100.0
        long_pct = computed_long if long_pct is None else long_pct
        short_pct = 100.0 - computed_long if short_pct is None else short_pct
    if long_pct is not None and short_pct is None:
        short_pct = 100.0 - float(long_pct)
    if short_pct is not None and long_pct is None:
        long_pct = 100.0 - float(short_pct)
    return {
        "timestamp": timestamp_value,
        "ratio": ratio_numeric,
        "long_pct": long_pct,
        "short_pct": short_pct,
    }


def _ratio_previous_snapshot(items: List[dict], *, ratio_window: str) -> Dict[str, float | int | None]:
    if not items:
        return {"timestamp": None, "ratio": None, "long_pct": None, "short_pct": None}
    frame = pd.DataFrame(items)
    if frame.empty or "timestamp" not in frame.columns:
        return {"timestamp": None, "ratio": None, "long_pct": None, "short_pct": None}
    frame["timestamp"] = pd.to_numeric(frame["timestamp"], errors="coerce")
    frame["longShortRatio"] = pd.to_numeric(frame.get("longShortRatio"), errors="coerce")
    if "longAccount" in frame.columns:
        frame["longAccount"] = pd.to_numeric(frame.get("longAccount"), errors="coerce")
    if "shortAccount" in frame.columns:
        frame["shortAccount"] = pd.to_numeric(frame.get("shortAccount"), errors="coerce")
    frame = frame.dropna(subset=["timestamp"]).sort_values("timestamp")
    if len(frame) <= 1:
        return {"timestamp": None, "ratio": None, "long_pct": None, "short_pct": None}
    normalized_window = str(ratio_window or "").strip().lower()
    if normalized_window == "1w":
        working = frame.tail(14).copy()
        previous = working.iloc[:-7].tail(7).copy() if len(working) > 7 else working.iloc[:-1].copy()
        if previous.empty:
            return {"timestamp": None, "ratio": None, "long_pct": None, "short_pct": None}
        ratio_value = previous["longShortRatio"].dropna().mean() if "longShortRatio" in previous.columns else None
        long_value = previous["longAccount"].dropna().mean() if "longAccount" in previous.columns else None
        short_value = previous["shortAccount"].dropna().mean() if "shortAccount" in previous.columns else None
        timestamp_value = int(previous["timestamp"].iloc[-1]) if not previous.empty else None
    else:
        previous = frame.iloc[-2]
        ratio_value = previous.get("longShortRatio")
        long_value = previous.get("longAccount")
        short_value = previous.get("shortAccount")
        timestamp_value = int(previous.get("timestamp")) if pd.notna(previous.get("timestamp")) else None
    ratio_numeric = None if pd.isna(ratio_value) else float(ratio_value)
    long_pct = None if pd.isna(long_value) else float(long_value) * 100.0
    short_pct = None if pd.isna(short_value) else float(short_value) * 100.0
    if (long_pct is None or short_pct is None) and ratio_numeric not in (None, 0):
        computed_long = float(ratio_numeric) / (1.0 + float(ratio_numeric)) * 100.0
        long_pct = computed_long if long_pct is None else long_pct
        short_pct = 100.0 - computed_long if short_pct is None else short_pct
    if long_pct is not None and short_pct is None:
        short_pct = 100.0 - float(long_pct)
    if short_pct is not None and long_pct is None:
        long_pct = 100.0 - float(short_pct)
    return {
        "timestamp": timestamp_value,
        "ratio": ratio_numeric,
        "long_pct": long_pct,
        "short_pct": short_pct,
    }


def _contract_sentiment_resonance_score(
    ratio_value: float | None,
    funding_bps: float | None,
    active_buy_pct: float | None,
    taker_ratio: float | None,
) -> float:
    score = 0.0
    if ratio_value is not None:
        score += min(40.0, abs(float(ratio_value) - 1.0) * 260.0)
    if funding_bps is not None:
        score += min(20.0, abs(float(funding_bps)) * 6.0)
    if active_buy_pct is not None:
        score += min(20.0, abs(float(active_buy_pct) - 50.0) * 0.9)
    if taker_ratio is not None:
        score += min(20.0, abs(float(taker_ratio) - 1.0) * 90.0)
    return max(0.0, min(100.0, score))


def _contract_sentiment_label(
    ratio_value: float | None,
    funding_bps: float | None,
    active_buy_pct: float | None,
) -> str:
    if ratio_value is not None:
        if ratio_value >= 1.08 and funding_bps is not None and funding_bps > 0.4:
            return "偏多拥挤"
        if ratio_value <= 0.92 and funding_bps is not None and funding_bps < -0.4:
            return "偏空拥挤"
        if ratio_value >= 1.03:
            return "偏多"
        if ratio_value <= 0.97:
            return "偏空"
    if active_buy_pct is not None:
        if active_buy_pct >= 58.0:
            return "买方主动"
        if active_buy_pct <= 42.0:
            return "卖方主动"
    if funding_bps is not None:
        if funding_bps >= 0.8:
            return "多头付费"
        if funding_bps <= -0.8:
            return "空头付费"
    return "中性 / 样本不足"


def build_contract_sentiment_truth_frame(
    snapshots: Dict[str, ExchangeSnapshot],
    sentiment_payloads: Dict[str, Dict[str, List[dict]]],
    trade_metrics_by_exchange: Dict[str, Dict[str, float | int | str | None]],
    exchange_title_map: Dict[str, str],
    exchange_keys: List[str] | None = None,
) -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []
    for exchange_key in exchange_keys or list(exchange_title_map):
        snapshot = snapshots.get(exchange_key)
        if snapshot is None:
            continue
        exchange_name = exchange_title_map.get(exchange_key, exchange_key.title())
        payload = sentiment_payloads.get(exchange_key, {})
        trade_metrics = trade_metrics_by_exchange.get(exchange_name, {})
        active_buy_pct = None
        if trade_metrics.get("buy_ratio") is not None:
            active_buy_pct = float(trade_metrics.get("buy_ratio") or 0.0) * 100.0

        ratio_value = None
        top_position_ratio = None
        top_account_ratio = None
        global_account_ratio = None
        taker_ratio = None
        account_long_pct = None
        account_short_pct = None
        source_label = "OI / Funding / 主动流代理"
        source_confidence = "代理"
        truth_tier = "代理"
        source_timestamp_ms = snapshot.timestamp_ms

        if exchange_key == "binance":
            top_position_ratio = _latest_dataset_float(payload, "top_position", "longShortRatio")
            top_account_ratio = _latest_dataset_float(payload, "top_account", "longShortRatio")
            global_account_ratio = _latest_dataset_float(payload, "global_account", "longShortRatio")
            taker_ratio = _latest_dataset_float(payload, "taker_ratio", "buySellRatio")
            ratio_value = top_position_ratio or top_account_ratio or global_account_ratio
            account_long_pct = _latest_dataset_float(payload, "global_account", "longAccount")
            account_short_pct = _latest_dataset_float(payload, "global_account", "shortAccount")
            source_label = "Top Position + Top Account + Global Account + Taker"
            source_confidence = "高"
            truth_tier = "真值"
            source_timestamp_ms = (
                _latest_dataset_timestamp(payload, "top_position")
                or _latest_dataset_timestamp(payload, "top_account")
                or _latest_dataset_timestamp(payload, "global_account")
                or snapshot.timestamp_ms
            )
        elif exchange_key == "bybit":
            ratio_value = _latest_dataset_float(payload, "account_ratio", "longShortRatio")
            top_account_ratio = ratio_value
            account_long_pct = _latest_dataset_float(payload, "account_ratio", "longAccount")
            account_short_pct = _latest_dataset_float(payload, "account_ratio", "shortAccount")
            source_label = "Account Ratio + OI + Funding"
            source_confidence = "中高"
            truth_tier = "真值"
            source_timestamp_ms = _latest_dataset_timestamp(payload, "account_ratio") or snapshot.timestamp_ms
        elif exchange_key == "okx":
            top_position_ratio = _latest_dataset_float(payload, "top_position", "longShortRatio")
            top_account_ratio = _latest_dataset_float(payload, "top_account", "longShortRatio")
            global_account_ratio = _latest_dataset_float(payload, "global_account", "longShortRatio")
            contract_account_ratio = _latest_dataset_float(payload, "contract_account", "longShortRatio")
            ratio_value = top_position_ratio or top_account_ratio or contract_account_ratio or global_account_ratio
            account_long_pct = _latest_dataset_float(payload, "contract_account", "longAccount")
            account_short_pct = _latest_dataset_float(payload, "contract_account", "shortAccount")
            source_label = "Top Position + Top Account + Contract Account + Global Account"
            source_confidence = "高"
            truth_tier = "真值"
            source_timestamp_ms = (
                _latest_dataset_timestamp(payload, "top_position")
                or _latest_dataset_timestamp(payload, "top_account")
                or _latest_dataset_timestamp(payload, "contract_account")
                or _latest_dataset_timestamp(payload, "global_account")
                or snapshot.timestamp_ms
            )
        elif exchange_key == "hyperliquid":
            source_label = "公开全市场多空比缺失，先看 OI / Funding / 主动流"
            source_confidence = "代理"

        resonance_score = _contract_sentiment_resonance_score(ratio_value, snapshot.funding_bps, active_buy_pct, taker_ratio)

        rows.append(
            {
                "交易所": exchange_name,
                "价格": snapshot.last_price if snapshot.status == "ok" else None,
                "未平仓金额": snapshot.open_interest_notional,
                "资金费率(bps)": snapshot.funding_bps,
                "合约多空比": ratio_value,
                "大户持仓多空比": top_position_ratio,
                "大户账户多空比": top_account_ratio,
                "全市场账户多空比": global_account_ratio,
                "账户多头占比(%)": None if account_long_pct is None else account_long_pct * 100.0,
                "账户空头占比(%)": None if account_short_pct is None else account_short_pct * 100.0,
                "主动流买占比(%)": active_buy_pct,
                "主动买卖比": taker_ratio,
                "共振评分": resonance_score,
                "情绪标签": _contract_sentiment_label(ratio_value, snapshot.funding_bps, active_buy_pct),
                "真值层级": truth_tier,
                "数据口径": source_label,
                "口径置信度": source_confidence,
                "时间": source_timestamp_ms,
            }
        )

    frame = pd.DataFrame(rows)
    if frame.empty:
        return pd.DataFrame(
            columns=[
                "交易所",
                "价格",
                "未平仓金额",
                "OI份额(%)",
                "资金费率(bps)",
                "合约多空比",
                "大户持仓多空比",
                "大户账户多空比",
                "全市场账户多空比",
                "账户多头占比(%)",
                "账户空头占比(%)",
                "主动流买占比(%)",
                "主动买卖比",
                "共振评分",
                "情绪标签",
                "真值层级",
                "数据口径",
                "口径置信度",
                "时间",
            ]
        )
    total_oi = float(frame["未平仓金额"].fillna(0.0).sum())
    frame["OI份额(%)"] = frame["未平仓金额"].fillna(0.0) / total_oi * 100.0 if total_oi > 0 else 0.0
    return frame.sort_values(["共振评分", "OI份额(%)", "交易所"], ascending=[False, False, True], na_position="last").reset_index(drop=True)


def build_contract_sentiment_truth_figure(frame: pd.DataFrame) -> go.Figure:
    figure = go.Figure()
    if frame.empty:
        figure.add_annotation(text="等待合约情绪真值层样本", showarrow=False, x=0.5, y=0.5, xref="paper", yref="paper")
        figure.update_layout(height=320, margin=dict(l=12, r=12, t=24, b=12))
        return figure

    metric_specs = [
        ("合约多空比", lambda value: max(-1.0, min(1.0, (float(value) - 1.0) / 0.12)) if pd.notna(value) else None, lambda value: f"{float(value):.3f}" if pd.notna(value) else "-"),
        ("账户多头占比(%)", lambda value: max(-1.0, min(1.0, (float(value) - 50.0) / 25.0)) if pd.notna(value) else None, lambda value: f"{float(value):.1f}%" if pd.notna(value) else "-"),
        ("主动流买占比(%)", lambda value: max(-1.0, min(1.0, (float(value) - 50.0) / 25.0)) if pd.notna(value) else None, lambda value: f"{float(value):.1f}%" if pd.notna(value) else "-"),
        ("资金费率(bps)", lambda value: max(-1.0, min(1.0, float(value) / 4.0)) if pd.notna(value) else None, lambda value: f"{float(value):.2f}" if pd.notna(value) else "-"),
    ]
    y_labels = frame["交易所"].astype(str).tolist()
    x_labels = [item[0] for item in metric_specs]
    z_rows: List[List[float | None]] = []
    text_rows: List[List[str]] = []
    custom_rows: List[List[List[Any]]] = []
    for _, row in frame.iterrows():
        exchange_key = str((row.get("__selection") or {}).get("exchange") or row.get("交易所键") or row.get("交易所") or "")
        z_row: List[float | None] = []
        text_row: List[str] = []
        custom_row: List[List[Any]] = []
        for column_name, score_builder, text_builder in metric_specs:
            value = pd.to_numeric(row.get(column_name), errors="coerce")
            z_row.append(score_builder(value))
            text_row.append(text_builder(value))
            custom_row.append([exchange_key, column_name, text_builder(value)])
        z_rows.append(z_row)
        text_rows.append(text_row)
        custom_rows.append(custom_row)

    figure.add_trace(
        go.Heatmap(
            x=x_labels,
            y=y_labels,
            z=z_rows,
            text=text_rows,
            customdata=custom_rows,
            texttemplate="%{text}",
            zmin=-1.0,
            zmax=1.0,
            zmid=0.0,
            colorscale=[
                [0.0, "#d14d57"],
                [0.5, "#eef3fb"],
                [1.0, "#4bc07a"],
            ],
            hovertemplate="交易所 %{y}<br>指标 %{x}<br>数值 %{customdata[2]}<extra></extra>",
            xgap=6,
            ygap=6,
            showscale=False,
        )
    )
    figure.update_layout(
        height=max(300, 78 * len(frame)),
        margin=dict(l=12, r=12, t=58, b=12),
        paper_bgcolor="rgba(14, 22, 35, 0.56)",
        plot_bgcolor="rgba(255, 255, 255, 0.045)",
        font=dict(color="#f6f9ff", family="SF Pro Display, Segoe UI, sans-serif"),
        title=dict(text="Contract Sentiment Truth Layer", x=0.03, y=0.98, xanchor="left", font=dict(size=18, color="#f7fbff")),
        transition=dict(duration=280, easing="cubic-in-out"),
    )
    figure.update_xaxes(showgrid=False, side="top")
    figure.update_yaxes(showgrid=False)
    return figure


def build_contract_ratio_history_figure(
    sentiment_payloads: Dict[str, Dict[str, List[dict]]],
    exchange_title_map: Dict[str, str],
    *,
    coin: str | None = None,
    ratio_window: str = "5m",
) -> go.Figure:
    figure = go.Figure()
    trace_specs = [
        ("binance", "global_account", "Binance 全市场账户比", "#ffbf79"),
        ("bybit", "account_ratio", "Bybit 账户多空比", "#ffd76b"),
        ("okx", "contract_account", "OKX 合约账户比", "#74c6ff"),
        ("bitget", "account_ratio", "Bitget 账户多空比", "#67d1ff"),
        ("gate", "account_ratio", "Gate 账户多空比", "#6fdd8c"),
        ("htx", "account_ratio", "HTX 精英账户比", "#ff86c8"),
    ]
    has_trace = False
    for exchange_key, dataset_key, trace_name, color in trace_specs:
        payload = sentiment_payloads.get(exchange_key, {})
        items = payload.get(dataset_key) or []
        if not items:
            continue
        frame = pd.DataFrame(items)
        if "timestamp" not in frame.columns or "longShortRatio" not in frame.columns:
            continue
        frame["ts"] = pd.to_datetime(frame["timestamp"], unit="ms")
        frame["value"] = pd.to_numeric(frame["longShortRatio"], errors="coerce")
        frame = frame.dropna(subset=["value"]).sort_values("ts")
        if frame.empty:
            continue
        if str(ratio_window or "").strip().lower() == "1w":
            frame["value"] = frame["value"].rolling(window=7, min_periods=1).mean()
        has_trace = True
        figure.add_trace(
            go.Scatter(
                x=frame["ts"],
                y=frame["value"],
                mode="lines",
                name=trace_name,
                line=dict(color=color, width=2.1),
                hovertemplate="时间 %{x}<br>合约多空比 %{y:.3f}<extra></extra>",
            )
        )

    if not has_trace:
        figure.add_annotation(text="等待交易所合约多空比历史", showarrow=False, x=0.5, y=0.5, xref="paper", yref="paper")
        figure.update_layout(height=320, margin=dict(l=12, r=12, t=24, b=12))
        return figure

    figure.add_hline(y=1.0, line_color="rgba(223, 232, 241, 0.22)", line_width=1, line_dash="dot")
    figure.update_layout(
        height=320,
        margin=dict(l=12, r=12, t=58, b=12),
        paper_bgcolor="rgba(14, 22, 35, 0.56)",
        plot_bgcolor="rgba(255, 255, 255, 0.045)",
        font=dict(color="#f6f9ff", family="SF Pro Display, Segoe UI, sans-serif"),
        title=dict(
            text=(
                f"{str(coin or '').upper().strip()} 交易所多空比趋势 · {_ratio_window_label(ratio_window)}"
                if str(coin or "").strip()
                else f"交易所多空比趋势 · {_ratio_window_label(ratio_window)}"
            ),
            x=0.03,
            y=0.98,
            xanchor="left",
            font=dict(size=18, color="#f7fbff"),
        ),
        transition=dict(duration=280, easing="cubic-in-out"),
        legend=dict(orientation="h", yanchor="bottom", y=1.01, xanchor="right", x=1.0),
    )
    figure.update_xaxes(showgrid=False)
    figure.update_yaxes(showgrid=True, gridcolor="rgba(255, 255, 255, 0.08)", title="ratio")
    return figure


def build_exchange_long_short_ratio_frame(
    sentiment_payloads: Dict[str, Dict[str, List[dict]]],
    snapshots: Dict[str, ExchangeSnapshot],
    exchange_title_map: Dict[str, str],
    *,
    exchange_keys: List[str] | None = None,
    ratio_window: str = "5m",
) -> pd.DataFrame:
    ratio_specs = {
        "binance": ("global_account", "Binance 全市场账户比"),
        "bybit": ("account_ratio", "Bybit 账户多空比"),
        "okx": ("contract_account", "OKX 合约账户多空比"),
        "bitget": ("account_ratio", "Bitget 账户多空比"),
        "gate": ("account_ratio", "Gate 账户多空比"),
        "htx": ("account_ratio", "HTX 精英账户多空比"),
    }
    rows: List[Dict[str, Any]] = []
    for exchange_key in exchange_keys or list(ratio_specs):
        if exchange_key not in ratio_specs:
            continue
        dataset_key, source_label = ratio_specs[exchange_key]
        payload = sentiment_payloads.get(exchange_key, {})
        snapshot = _ratio_snapshot(payload.get(dataset_key) or [], ratio_window=ratio_window)
        previous_snapshot = _ratio_previous_snapshot(payload.get(dataset_key) or [], ratio_window=ratio_window)
        if snapshot.get("long_pct") is None or snapshot.get("short_pct") is None:
            continue
        long_delta_pp = None
        short_delta_pp = None
        if previous_snapshot.get("long_pct") is not None:
            long_delta_pp = float(snapshot.get("long_pct") or 0.0) - float(previous_snapshot.get("long_pct") or 0.0)
        if previous_snapshot.get("short_pct") is not None:
            short_delta_pp = float(snapshot.get("short_pct") or 0.0) - float(previous_snapshot.get("short_pct") or 0.0)
        rows.append(
            {
                "交易所": exchange_title_map.get(exchange_key, exchange_key.title()),
                "交易所键": exchange_key,
                "多头占比(%)": snapshot.get("long_pct"),
                "空头占比(%)": snapshot.get("short_pct"),
                "多空比": snapshot.get("ratio"),
                "多头变化(pp)": long_delta_pp,
                "空头变化(pp)": short_delta_pp,
                "未平仓金额": getattr(snapshots.get(exchange_key), "open_interest_notional", None),
                "时间": snapshot.get("timestamp"),
                "对比时间": previous_snapshot.get("timestamp"),
                "数据口径": source_label,
                "__selection": {"exchange": exchange_key},
            }
        )
    frame = pd.DataFrame(rows)
    if frame.empty:
        return pd.DataFrame(
            columns=[
                "交易所",
                "交易所键",
                "多头占比(%)",
                "空头占比(%)",
                "多空比",
                "多头变化(pp)",
                "空头变化(pp)",
                "未平仓金额",
                "时间",
                "对比时间",
                "数据口径",
                "__selection",
            ]
        )
    weights = pd.to_numeric(frame["未平仓金额"], errors="coerce").fillna(0.0)
    long_series = pd.to_numeric(frame["多头占比(%)"], errors="coerce").fillna(0.0)
    short_series = pd.to_numeric(frame["空头占比(%)"], errors="coerce").fillna(0.0)
    total_weight = float(weights.sum())
    if total_weight > 0:
        aggregate_long = float((long_series * weights).sum() / total_weight)
        aggregate_short = float((short_series * weights).sum() / total_weight)
    else:
        aggregate_long = float(long_series.mean())
        aggregate_short = float(short_series.mean())
    aggregate_ratio = None
    if aggregate_short > 0:
        aggregate_ratio = aggregate_long / aggregate_short
    aggregate_time = pd.to_numeric(frame["时间"], errors="coerce").dropna()
    aggregate_row = {
        "交易所": "全部",
        "交易所键": "all",
        "多头占比(%)": aggregate_long,
        "空头占比(%)": aggregate_short,
        "多空比": aggregate_ratio,
        "多头变化(pp)": None,
        "空头变化(pp)": None,
        "未平仓金额": total_weight if total_weight > 0 else None,
        "时间": int(aggregate_time.max()) if not aggregate_time.empty else None,
        "对比时间": None,
        "数据口径": "按 OI 加权聚合",
        "__selection": {"exchange": "binance"},
    }
    frame = pd.DataFrame([aggregate_row, *frame.to_dict("records")], columns=list(frame.columns))
    return frame.reset_index(drop=True)


def build_exchange_long_short_ratio_balance_figure(
    frame: pd.DataFrame,
    *,
    coin: str | None = None,
    ratio_window: str = "5m",
) -> go.Figure:
    figure = go.Figure()
    if frame.empty:
        figure.add_annotation(text="等待交易所多空比样本", showarrow=False, x=0.5, y=0.5, xref="paper", yref="paper")
        figure.update_layout(height=320, margin=dict(l=12, r=12, t=24, b=12))
        return figure
    working = frame.copy()
    working["多头占比(%)"] = pd.to_numeric(working["多头占比(%)"], errors="coerce")
    working["空头占比(%)"] = pd.to_numeric(working["空头占比(%)"], errors="coerce")
    if "多头变化(pp)" in working.columns:
        working["多头变化(pp)"] = pd.to_numeric(working["多头变化(pp)"], errors="coerce")
    if "空头变化(pp)" in working.columns:
        working["空头变化(pp)"] = pd.to_numeric(working["空头变化(pp)"], errors="coerce")
    working = working.dropna(subset=["多头占比(%)", "空头占比(%)"]).reset_index(drop=True)
    if working.empty:
        figure.add_annotation(text="等待交易所多空比样本", showarrow=False, x=0.5, y=0.5, xref="paper", yref="paper")
        figure.update_layout(height=320, margin=dict(l=12, r=12, t=24, b=12))
        return figure
    y_labels = working["交易所"].astype(str).tolist()
    customdata = working["__selection"].tolist() if "__selection" in working.columns else [{"exchange": ""} for _ in y_labels]
    figure.add_trace(
        go.Bar(
            x=working["多头占比(%)"],
            y=y_labels,
            orientation="h",
            name="多",
            marker_color="#167c2a",
            text=[f"{float(value):.2f}%" for value in working["多头占比(%)"]],
            textposition="inside",
            customdata=customdata,
            hovertemplate="交易所 %{y}<br>多头 %{x:.2f}%<extra></extra>",
        )
    )
    figure.add_trace(
        go.Bar(
            x=working["空头占比(%)"],
            y=y_labels,
            orientation="h",
            name="空",
            marker_color="#8d1b1f",
            text=[f"{float(value):.2f}%" for value in working["空头占比(%)"]],
            textposition="inside",
            customdata=customdata,
            hovertemplate="交易所 %{y}<br>空头 %{x:.2f}%<extra></extra>",
        )
    )
    delta_parts: List[str] = []
    if "多头变化(pp)" in working.columns:
        non_aggregate = working[working["交易所键"] != "all"].copy() if "交易所键" in working.columns else working.copy()
        delta_series = non_aggregate["多头变化(pp)"].dropna().abs() if not non_aggregate.empty else pd.Series(dtype=float)
        if not delta_series.empty:
            delta_parts.append(f"最近一档最大变化 {float(delta_series.max()):.3f}pp")
    figure.update_layout(
        barmode="stack",
        height=max(320, 48 * len(working) + 120),
        margin=dict(l=18, r=18, t=62, b=18),
        paper_bgcolor="rgba(14, 22, 35, 0.56)",
        plot_bgcolor="rgba(255, 255, 255, 0.045)",
        font=dict(color="#f6f9ff", family="SF Pro Display, Segoe UI, sans-serif"),
        title=dict(
            text=(
                (
                    f"{str(coin or '').upper().strip()} 交易所多空比分布 · {_ratio_window_label(ratio_window)}"
                    if str(coin or "").strip()
                    else f"交易所多空比分布 · {_ratio_window_label(ratio_window)}"
                )
                + (f"<br><sup>{' | '.join(delta_parts)}</sup>" if delta_parts else "")
            ),
            x=0.03,
            y=0.98,
            xanchor="left",
            font=dict(size=18, color="#f7fbff"),
        ),
        transition=dict(duration=280, easing="cubic-in-out"),
        legend=dict(orientation="h", yanchor="bottom", y=1.01, xanchor="right", x=1.0),
    )
    figure.update_xaxes(showgrid=False, range=[0, 100], ticksuffix="%")
    figure.update_yaxes(showgrid=False, autorange="reversed")
    return figure


def build_multicoin_long_short_ratio_figure(
    frame: pd.DataFrame,
    *,
    ratio_window: str = "15m",
    title: str = "主流币多空比总览",
) -> go.Figure:
    figure = make_subplots(
        rows=1,
        cols=2,
        column_widths=[0.54, 0.46],
        subplot_titles=("多交易所多空比", "综合多空占比"),
        specs=[[{"secondary_y": False}, {"secondary_y": False}]],
    )
    if frame.empty:
        figure.add_annotation(text="等待主流币多空比样本", showarrow=False, x=0.5, y=0.5, xref="paper", yref="paper")
        figure.update_layout(height=360, margin=dict(l=12, r=12, t=30, b=12))
        return figure
    working = canonicalize_market_frame_columns(frame.copy())
    if "币种" not in working.columns:
        figure.add_annotation(text="等待主流币多空比样本", showarrow=False, x=0.5, y=0.5, xref="paper", yref="paper")
        figure.update_layout(height=360, margin=dict(l=12, r=12, t=30, b=12))
        return figure
    for column in [
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
        "情绪评分",
    ]:
        if column in working.columns:
            working[column] = pd.to_numeric(working[column], errors="coerce")
    if "情绪评分" in working.columns:
        working = working.reindex(working["情绪评分"].abs().sort_values(ascending=False, na_position="last").index)
    working = working.head(10).reset_index(drop=True)
    if working.empty:
        figure.add_annotation(text="等待主流币多空比样本", showarrow=False, x=0.5, y=0.5, xref="paper", yref="paper")
        figure.update_layout(height=360, margin=dict(l=12, r=12, t=30, b=12))
        return figure
    labels = working["币种"].astype(str).tolist()
    ratio_specs = [
        ("Binance全市场比", "Binance 全市场比", "bar", "rgba(255, 191, 121, 0.86)"),
        ("Bybit账户比", "Bybit 账户比", "line", "#67d1ff"),
        ("OKX合约账户比", "OKX 合约账户比", "line", "#74c6ff"),
        ("OKX全网账户比", "OKX 全网账户比", "line", "#5fa3ff"),
        ("Bitget账户比", "Bitget 账户比", "line", "#8b9dff"),
        ("Gate账户比", "Gate 账户比", "line", "#6fdd8c"),
        ("HTX账户比", "HTX 账户比", "line", "#ff86c8"),
    ]
    for column, label, trace_kind, color in ratio_specs:
        if column not in working.columns or working[column].dropna().empty:
            continue
        if trace_kind == "bar":
            figure.add_trace(
                go.Bar(
                    x=labels,
                    y=working[column],
                    name=label,
                    marker_color=color,
                    text=[f"{float(value):.3f}" if pd.notna(value) else "-" for value in working[column]],
                    textposition="outside",
                    hovertemplate=f"币种 %{{x}}<br>{label} %{{y:.3f}}<extra></extra>",
                ),
                row=1,
                col=1,
            )
        else:
            figure.add_trace(
                go.Scatter(
                    x=labels,
                    y=working[column],
                    name=label,
                    mode="lines+markers",
                    line=dict(color=color, width=2.2),
                    marker=dict(size=8, color=color),
                    hovertemplate=f"币种 %{{x}}<br>{label} %{{y:.3f}}<extra></extra>",
                ),
                row=1,
                col=1,
            )
    long_sources = []
    short_sources = []
    if "Binance全市场比" in working.columns:
        ratio_series = pd.to_numeric(working["Binance全市场比"], errors="coerce")
        long_sources.append(ratio_series / (1.0 + ratio_series) * 100.0)
        short_sources.append(100.0 - (ratio_series / (1.0 + ratio_series) * 100.0))
    for long_column, short_column in [
        ("Bybit多头占比(%)", "Bybit空头占比(%)"),
        ("OKX多头占比(%)", "OKX空头占比(%)"),
        ("Bitget多头占比(%)", "Bitget空头占比(%)"),
        ("Gate多头占比(%)", "Gate空头占比(%)"),
        ("HTX多头占比(%)", "HTX空头占比(%)"),
    ]:
        if long_column in working.columns and short_column in working.columns:
            long_sources.append(pd.to_numeric(working[long_column], errors="coerce"))
            short_sources.append(pd.to_numeric(working[short_column], errors="coerce"))
    if long_sources:
        long_frame = pd.concat(long_sources, axis=1)
        short_frame = pd.concat(short_sources, axis=1)
        working["综合多头占比(%)"] = long_frame.mean(axis=1, skipna=True)
        working["综合空头占比(%)"] = short_frame.mean(axis=1, skipna=True)
        figure.add_trace(
            go.Bar(
                x=labels,
                y=working["综合多头占比(%)"],
                name="综合多头",
                marker_color="#167c2a",
                text=[f"{float(value):.1f}%" if pd.notna(value) else "-" for value in working["综合多头占比(%)"]],
                textposition="inside",
                hovertemplate="币种 %{x}<br>综合多头 %{y:.2f}%<extra></extra>",
            ),
            row=1,
            col=2,
        )
        figure.add_trace(
            go.Bar(
                x=labels,
                y=working["综合空头占比(%)"],
                name="综合空头",
                marker_color="#8d1b1f",
                text=[f"{float(value):.1f}%" if pd.notna(value) else "-" for value in working["综合空头占比(%)"]],
                textposition="inside",
                hovertemplate="币种 %{x}<br>综合空头 %{y:.2f}%<extra></extra>",
            ),
            row=1,
            col=2,
        )
    figure.add_hline(y=1.0, line_color="rgba(223, 232, 241, 0.22)", line_width=1, line_dash="dot", row=1, col=1)
    figure.update_layout(
        height=max(360, 120 + len(working) * 22),
        margin=dict(l=12, r=12, t=66, b=16),
        paper_bgcolor="rgba(14, 22, 35, 0.56)",
        plot_bgcolor="rgba(255, 255, 255, 0.045)",
        font=dict(color="#f6f9ff", family="SF Pro Display, Segoe UI, sans-serif"),
        title=dict(
            text=f"{title} · {_ratio_window_label(ratio_window)}",
            x=0.03,
            y=0.98,
            xanchor="left",
            font=dict(size=18, color="#f7fbff"),
        ),
        transition=dict(duration=280, easing="cubic-in-out"),
        legend=dict(orientation="h", yanchor="bottom", y=1.01, xanchor="right", x=1.0),
        barmode="stack",
    )
    figure.update_xaxes(showgrid=False, tickangle=-18, row=1, col=1)
    figure.update_xaxes(showgrid=False, tickangle=-18, row=1, col=2)
    figure.update_yaxes(showgrid=True, gridcolor="rgba(255, 255, 255, 0.08)", title="ratio", row=1, col=1)
    figure.update_yaxes(showgrid=False, range=[0, 100], ticksuffix="%", row=1, col=2)
    return figure


def build_event_heatmap_frame(
    events_by_exchange: Dict[str, List[TradeEvent] | List[LiquidationEvent]],
    reference_price: float | None,
    *,
    now_ms: int | None = None,
    window_minutes: int = 60,
    window_pct: float = 8.0,
    bucket_count: int = 24,
    min_notional: float = 0.0,
    mode: str = "trade",
) -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []
    if now_ms is None:
        now_ms = int(time.time() * 1000)
    cutoff_ms = now_ms - max(window_minutes, 1) * 60_000
    if reference_price is None or reference_price <= 0:
        candidate_prices = [
            float(getattr(event, "price"))
            for items in events_by_exchange.values()
            for event in items
            if getattr(event, "price", None) is not None
        ]
        reference_price = float(pd.Series(candidate_prices).median()) if candidate_prices else None
    if reference_price is None or reference_price <= 0:
        return pd.DataFrame(columns=["交易所", "价格带", "价格中位", "净名义金额", "总名义金额", "事件数", "主导方向"])

    lower_bound = reference_price * (1.0 - max(window_pct, 0.5) / 100.0)
    upper_bound = reference_price * (1.0 + max(window_pct, 0.5) / 100.0)
    if upper_bound <= lower_bound:
        return pd.DataFrame(columns=["交易所", "价格带", "价格中位", "净名义金额", "总名义金额", "事件数", "主导方向"])
    bucket_size = (upper_bound - lower_bound) / max(int(bucket_count), 1)
    aggregates: Dict[tuple, Dict[str, Any]] = {}
    positive_sides = {"buy"} if mode == "trade" else {"short"}
    negative_sides = {"sell"} if mode == "trade" else {"long"}
    positive_label = "主动买" if mode == "trade" else "空头清算"
    negative_label = "主动卖" if mode == "trade" else "多头清算"

    for exchange_name, items in events_by_exchange.items():
        for event in items:
            event_ts = int(getattr(event, "timestamp_ms", 0) or 0)
            event_price = pd.to_numeric(getattr(event, "price", None), errors="coerce")
            event_notional = pd.to_numeric(getattr(event, "notional", None), errors="coerce")
            event_side = str(getattr(event, "side", "") or "").lower()
            if event_ts < cutoff_ms or pd.isna(event_price) or pd.isna(event_notional):
                continue
            if float(event_notional) < float(min_notional):
                continue
            if float(event_price) < lower_bound or float(event_price) > upper_bound:
                continue
            if event_side not in positive_sides and event_side not in negative_sides:
                continue
            bucket_index = min(max(int((float(event_price) - lower_bound) / bucket_size), 0), max(int(bucket_count) - 1, 0))
            bucket_low = lower_bound + bucket_size * bucket_index
            bucket_high = bucket_low + bucket_size
            bucket_mid = (bucket_low + bucket_high) * 0.5
            if reference_price >= 1000:
                bucket_label = f"{bucket_mid:,.0f}"
            elif reference_price >= 10:
                bucket_label = f"{bucket_mid:,.2f}"
            else:
                bucket_label = f"{bucket_mid:,.4f}"
            side_sign = 1.0 if event_side in positive_sides else -1.0 if event_side in negative_sides else 0.0
            aggregate_key = (exchange_name, bucket_label, bucket_mid)
            aggregate = aggregates.setdefault(
                aggregate_key,
                {
                    "交易所": exchange_name,
                    "价格带": bucket_label,
                    "价格中位": bucket_mid,
                    "净名义金额": 0.0,
                    "总名义金额": 0.0,
                    "事件数": 0,
                    "买侧名义金额": 0.0,
                    "卖侧名义金额": 0.0,
                },
            )
            aggregate["净名义金额"] += float(event_notional) * side_sign
            aggregate["总名义金额"] += float(event_notional)
            aggregate["事件数"] += 1
            if side_sign >= 0:
                aggregate["买侧名义金额"] += float(event_notional)
            else:
                aggregate["卖侧名义金额"] += float(event_notional)

    for aggregate in aggregates.values():
        aggregate["主导方向"] = positive_label if aggregate["买侧名义金额"] >= aggregate["卖侧名义金额"] else negative_label
        rows.append(aggregate)
    frame = pd.DataFrame(rows)
    if frame.empty:
        return pd.DataFrame(columns=["交易所", "价格带", "价格中位", "净名义金额", "总名义金额", "事件数", "主导方向"])
    return frame.sort_values(["交易所", "价格中位"]).reset_index(drop=True)


def build_event_heatmap_figure(
    frame: pd.DataFrame,
    *,
    title: str,
    positive_label: str,
    negative_label: str,
) -> go.Figure:
    figure = go.Figure()
    if frame.empty:
        figure.add_annotation(text="当前没有足够的热力样本", showarrow=False, x=0.5, y=0.5, xref="paper", yref="paper")
        figure.update_layout(height=320, margin=dict(l=12, r=12, t=24, b=12))
        return figure

    x_order = (
        frame[["价格带", "价格中位"]]
        .drop_duplicates()
        .sort_values("价格中位")["价格带"]
        .astype(str)
        .tolist()
    )
    y_order = frame["交易所"].astype(str).drop_duplicates().tolist()
    pivot_net = frame.pivot(index="交易所", columns="价格带", values="净名义金额").reindex(index=y_order, columns=x_order).fillna(0.0)
    pivot_total = frame.pivot(index="交易所", columns="价格带", values="总名义金额").reindex(index=y_order, columns=x_order).fillna(0.0)
    pivot_count = frame.pivot(index="交易所", columns="价格带", values="事件数").reindex(index=y_order, columns=x_order).fillna(0)
    customdata = [
        [
            [float(pivot_total.iloc[row_index, col_index]), int(pivot_count.iloc[row_index, col_index])]
            for col_index in range(len(x_order))
        ]
        for row_index in range(len(y_order))
    ]
    figure.add_trace(
        go.Heatmap(
            x=x_order,
            y=y_order,
            z=pivot_net.values,
            customdata=customdata,
            colorscale=[
                [0.0, "#d14d57"],
                [0.5, "#eef3fb"],
                [1.0, "#4bc07a"],
            ],
            zmid=0.0,
            xgap=3,
            ygap=6,
            colorbar=dict(title="净名义"),
            hovertemplate=(
                "交易所 %{y}<br>价格带 %{x}<br>"
                + f"{positive_label}/{negative_label} 净名义 %{{z:,.0f}}<br>"
                + "总名义 %{customdata[0]:,.0f}<br>事件数 %{customdata[1]}<extra></extra>"
            ),
        )
    )
    figure.update_layout(
        height=max(320, 84 * len(y_order)),
        margin=dict(l=12, r=12, t=58, b=12),
        paper_bgcolor="rgba(14, 22, 35, 0.56)",
        plot_bgcolor="rgba(255, 255, 255, 0.045)",
        font=dict(color="#f6f9ff", family="SF Pro Display, Segoe UI, sans-serif"),
        title=dict(text=title, x=0.03, y=0.98, xanchor="left", font=dict(size=18, color="#f7fbff")),
        transition=dict(duration=280, easing="cubic-in-out"),
    )
    figure.update_xaxes(showgrid=False)
    figure.update_yaxes(showgrid=False)
    return figure


def build_contract_sentiment_alert_frame(contract_sentiment_frame: pd.DataFrame) -> pd.DataFrame:
    if contract_sentiment_frame.empty:
        return pd.DataFrame(columns=["交易所", "等级", "告警", "解释", "数据口径"])

    rows: List[Dict[str, str]] = []
    for _, row in contract_sentiment_frame.iterrows():
        exchange = str(row.get("交易所") or "")
        ratio_value = pd.to_numeric(row.get("合约多空比"), errors="coerce")
        funding_bps = pd.to_numeric(row.get("资金费率(bps)"), errors="coerce")
        active_buy_pct = pd.to_numeric(row.get("主动流买占比(%)"), errors="coerce")
        taker_ratio = pd.to_numeric(row.get("主动买卖比"), errors="coerce")
        resonance_score = pd.to_numeric(row.get("共振评分"), errors="coerce")
        confidence = str(row.get("口径置信度") or "代理")
        truth_tier = str(row.get("真值层级") or "代理")
        source_label = str(row.get("数据口径") or "")
        row_alert_count = 0

        def add(level: str, title: str, detail: str) -> None:
            nonlocal row_alert_count
            rows.append(
                {
                    "交易所": exchange,
                    "等级": level,
                    "告警": title,
                    "解释": detail,
                    "数据口径": source_label,
                }
            )
            row_alert_count += 1

        if not pd.isna(ratio_value):
            if float(ratio_value) >= 1.08:
                add("高", "合约偏多拥挤", f"{exchange} 合约多空比 {float(ratio_value):.3f}，公开多空持仓明显偏多。")
            elif float(ratio_value) <= 0.92:
                add("高", "合约偏空拥挤", f"{exchange} 合约多空比 {float(ratio_value):.3f}，公开多空持仓明显偏空。")
            elif float(ratio_value) >= 1.03:
                add("中", "合约轻度偏多", f"{exchange} 合约多空比 {float(ratio_value):.3f}，合约端多头略占优。")
            elif float(ratio_value) <= 0.97:
                add("中", "合约轻度偏空", f"{exchange} 合约多空比 {float(ratio_value):.3f}，合约端空头略占优。")

        if not pd.isna(active_buy_pct):
            if float(active_buy_pct) >= 62.0:
                add("中", "主动买显著占优", f"{exchange} 主动流买占比 {float(active_buy_pct):.1f}%，短线更像扫单上行。")
            elif float(active_buy_pct) <= 38.0:
                add("中", "主动卖显著占优", f"{exchange} 主动流买占比 {float(active_buy_pct):.1f}%，短线更像主动抛压。")

        if not pd.isna(taker_ratio):
            if float(taker_ratio) >= 1.18:
                add("中", "主动买卖比偏多", f"{exchange} 主动买卖比 {float(taker_ratio):.2f}，买方吃单明显占优。")
            elif float(taker_ratio) <= 0.84:
                add("中", "主动买卖比偏空", f"{exchange} 主动买卖比 {float(taker_ratio):.2f}，卖方吃单明显占优。")

        if not pd.isna(ratio_value) and not pd.isna(funding_bps):
            if float(ratio_value) >= 1.05 and float(funding_bps) >= 0.8:
                add("高", "多头拥挤共振", f"{exchange} 多空比和 funding 同时偏多，追多拥挤风险抬升。")
            elif float(ratio_value) <= 0.95 and float(funding_bps) <= -0.8:
                add("高", "空头拥挤共振", f"{exchange} 多空比和 funding 同时偏空，追空拥挤风险抬升。")

        if not pd.isna(resonance_score):
            if float(resonance_score) >= 78.0:
                add(
                    "高",
                    "情绪共振极值",
                    f"{exchange} 共振评分 {float(resonance_score):.1f}，{truth_tier}层样本里多空比、Funding 和主动流同时偏斜。",
                )
            elif float(resonance_score) >= 58.0 and row_alert_count == 0:
                add(
                    "中",
                    "情绪共振偏强",
                    f"{exchange} 共振评分 {float(resonance_score):.1f}，当前已出现较明显的单边拥挤迹象。",
                )

        if confidence == "代理" and row_alert_count == 0:
            add("观察", "代理情绪观察", f"{exchange} 当前没有公开多空比真值，先用 OI / Funding / 主动流代理观察。")

    if not rows:
        return pd.DataFrame([{"交易所": "全市场", "等级": "低", "告警": "暂无合约情绪告警", "解释": "当前多空比、Funding 和主动流没有形成明显极端。", "数据口径": "公开 / 代理混合"}])

    priority_map = {"高": 0, "中": 1, "观察": 2, "低": 3}
    frame = pd.DataFrame(rows)
    frame["_priority"] = frame["等级"].map(priority_map).fillna(9)
    return frame.sort_values(["_priority", "交易所", "告警"]).drop(columns="_priority").reset_index(drop=True)


def build_spot_perp_alert_frame(
    spot_exchange_frame: pd.DataFrame,
    lead_lag_frame: pd.DataFrame,
    oi_metrics_by_exchange: Dict[str, Dict[str, float | str | None]],
    trade_metrics_by_exchange: Dict[str, Dict[str, float | int | str | None]],
    liquidation_metrics_by_exchange: Dict[str, Dict[str, float | int | str | None]],
    crowd_payload: Dict[str, List[dict]] | None = None,
) -> pd.DataFrame:
    rows: List[Dict[str, str | float | None]] = []
    exchange_col = "交易所" if "交易所" in spot_exchange_frame.columns else None
    spot_records = spot_exchange_frame.to_dict("records") if not spot_exchange_frame.empty else []
    exchange_names = [str(item.get("交易所") or "") for item in spot_records if item.get("交易所")]
    lead_lookup = {}
    if not lead_lag_frame.empty and "交易所" in lead_lag_frame.columns:
        lead_lookup = {
            str(row.get("交易所") or ""): row
            for row in lead_lag_frame.to_dict("records")
            if row.get("交易所")
        }
    crowd_payload = crowd_payload or {}

    def latest(dataset_key: str, field: str) -> float | None:
        items = crowd_payload.get(dataset_key) or []
        if not items:
            return None
        value = pd.to_numeric(items[-1].get(field), errors="coerce")
        return None if pd.isna(value) else float(value)

    def add_alert(exchange: str, level: str, title: str, detail: str) -> None:
        rows.append({"交易所": exchange, "等级": level, "告警": title, "解释": detail})

    for row in spot_records:
        exchange = str(row.get("交易所") or "")
        lead = lead_lookup.get(exchange, {})
        oi_metrics = oi_metrics_by_exchange.get(exchange, {})
        trade_metrics = trade_metrics_by_exchange.get(exchange, {})
        liq_metrics = liquidation_metrics_by_exchange.get(exchange, {})

        leader = str(lead.get("领先方") or "")
        lag_seconds = pd.to_numeric(lead.get("领先秒数"), errors="coerce")
        corr = pd.to_numeric(lead.get("相关性"), errors="coerce")
        basis_pct = pd.to_numeric(row.get("Basis(%)"), errors="coerce")
        volume_ratio = pd.to_numeric(row.get("永续/现货成交额比"), errors="coerce")
        spot_buy_ratio_pct = pd.to_numeric(row.get("现货主动买占比(%)"), errors="coerce")
        oi_change_pct = pd.to_numeric(oi_metrics.get("oi_change_pct"), errors="coerce")
        trade_buy_ratio = pd.to_numeric(trade_metrics.get("buy_ratio"), errors="coerce")
        trade_regime = str(trade_metrics.get("regime") or "")
        liq_count = int(liq_metrics.get("count") or 0)
        liq_long_count = int(liq_metrics.get("long_count") or 0)
        liq_short_count = int(liq_metrics.get("short_count") or 0)

        if leader == "现货" and not pd.isna(lag_seconds) and lag_seconds >= 1 and not pd.isna(corr) and corr >= 0.35:
            add_alert(
                exchange,
                "高" if (pd.isna(trade_buy_ratio) or 0.45 <= trade_buy_ratio <= 0.58) else "中",
                "现货先拉 / 合约没跟",
                f"{exchange} 现货领先 {int(lag_seconds)}s，相关性 {corr:.2f}；当前更像现货在带方向，合约确认还不充分。",
            )
        elif leader == "永续" and not pd.isna(lag_seconds) and lag_seconds >= 1 and not pd.isna(corr) and corr >= 0.35:
            add_alert(
                exchange,
                "中",
                "合约先跑 / 现货未确认",
                f"{exchange} 永续领先 {int(lag_seconds)}s，相关性 {corr:.2f}；当前更像杠杆资金先动，现货确认偏慢。",
            )

        weak_flow = (
            (not pd.isna(trade_buy_ratio) and 0.45 <= trade_buy_ratio <= 0.55)
            or "吸收" in trade_regime
            or "衰竭" in trade_regime
        )
        if not pd.isna(oi_change_pct) and oi_change_pct >= 0.35 and weak_flow:
            add_alert(
                exchange,
                "高",
                "OI 上升但主动买卖转弱",
                f"{exchange} 当前 OI 继续抬升，但主动成交进入 {trade_regime or '弱推进'}，更像加仓扩张、推动力在变弱。",
            )

        if not pd.isna(basis_pct) and not pd.isna(volume_ratio):
            if basis_pct > 0.08 and volume_ratio >= 3.0:
                add_alert(
                    exchange,
                    "观察",
                    "永续溢价偏高",
                    f"{exchange} 永续相对现货维持正溢价，且永续/现货成交额比 {volume_ratio:.2f}x，说明杠杆盘更活跃。",
                )
            elif basis_pct < -0.08 and volume_ratio >= 3.0:
                add_alert(
                    exchange,
                    "观察",
                    "永续贴水偏深",
                    f"{exchange} 永续相对现货偏贴水，且永续/现货成交额比 {volume_ratio:.2f}x，短线偏向合约端主导。",
                )

        if exchange == "Binance":
            top_position = latest("top_position", "longShortRatio")
            top_account = latest("top_account", "longShortRatio")
            if top_position is not None and top_account is not None and liq_count >= 2:
                if top_position > 1.03 and top_account > 1.02 and liq_long_count > liq_short_count:
                    add_alert(
                        exchange,
                        "高",
                        "账户拥挤 + 多头爆仓联动",
                        "Binance 大户持仓和账户都偏多，同时已发生爆仓偏向多头，说明拥挤方向正在被清算。",
                    )
                elif top_position < 0.97 and top_account < 0.98 and liq_short_count > liq_long_count:
                    add_alert(
                        exchange,
                        "高",
                        "账户拥挤 + 空头爆仓联动",
                        "Binance 大户持仓和账户都偏空，同时已发生爆仓偏向空头，说明拥挤方向正在被清算。",
                    )

        if not pd.isna(spot_buy_ratio_pct) and spot_buy_ratio_pct >= 58 and trade_regime in {"上方卖盘吸收", "多头衰竭 / 被动承接"}:
            add_alert(
                exchange,
                "中",
                "现货主动买强 / 上方吸收",
                f"{exchange} 现货主动买占比 {spot_buy_ratio_pct:.1f}%，但合约端显示 {trade_regime}，上方可能有被动卖盘吸收。",
            )

    if rows:
        frame = pd.DataFrame(rows, columns=["交易所", "等级", "告警", "解释"])
    else:
        frame = pd.DataFrame(
            [{"交易所": exchange, "等级": "低", "告警": "暂无强实时告警", "解释": "当前 spot-perp、OI、主动买卖和爆仓没有形成明显共振。"} for exchange in exchange_names],
            columns=["交易所", "等级", "告警", "解释"],
        )

    if frame.empty and not exchange_names:
        return pd.DataFrame(columns=["交易所", "等级", "告警", "解释"])

    present_exchanges = set(frame["交易所"].tolist()) if "交易所" in frame.columns else set()
    for exchange in exchange_names:
        if exchange not in present_exchanges:
            frame = pd.concat(
                [
                    frame,
                    pd.DataFrame(
                        [{"交易所": exchange, "等级": "低", "告警": "暂无强实时告警", "解释": "当前 spot-perp、OI、主动买卖和爆仓没有形成明显共振。"}]
                    ),
                ],
                ignore_index=True,
            )

    priority_map = {"高": 0, "中": 1, "观察": 2, "低": 3}
    frame["_priority"] = frame["等级"].map(priority_map).fillna(9)
    return frame.sort_values(["_priority", "交易所", "告警"]).drop(columns="_priority").reset_index(drop=True)


def normalize_alert_level(level: str) -> str:
    mapping = {"高": "强", "中": "中", "观察": "弱", "低": "弱", "强": "强", "弱": "弱"}
    return mapping.get(level, level or "弱")


def evolve_alert_engine(
    raw_alert_frame: pd.DataFrame,
    state: Dict[str, Dict[str, object]] | None,
    timeline: List[Dict[str, object]] | None,
    now_ms: int | None = None,
    confirm_after: int = 3,
    cooldown_minutes: int = 0,
) -> tuple[pd.DataFrame, pd.DataFrame, Dict[str, Dict[str, object]], List[Dict[str, object]]]:
    now_ms = now_ms or int(time.time() * 1000)
    state = dict(state or {})
    timeline = list(timeline or [])
    seen_keys = set()
    confirmed_rows: List[Dict[str, object]] = []
    cooldown_ms = max(0, int(cooldown_minutes)) * 60_000

    frame = raw_alert_frame.copy()
    if not frame.empty:
        frame["等级"] = frame["等级"].apply(lambda value: normalize_alert_level(str(value)))
    else:
        frame = pd.DataFrame(columns=["交易所", "等级", "告警", "解释"])

    for row in frame.to_dict("records"):
        exchange = str(row.get("交易所") or "未知")
        title = str(row.get("告警") or "未命名告警")
        key = f"{exchange}::{title}"
        seen_keys.add(key)
        previous = dict(state.get(key, {}))
        streak = int(previous.get("streak", 0)) + 1
        active = bool(previous.get("active", False))
        current = {
            "交易所": exchange,
            "等级": row.get("等级") or "弱",
            "告警": title,
            "解释": row.get("解释") or "",
            "streak": streak,
            "active": active,
            "last_seen": now_ms,
            "last_triggered_ms": previous.get("last_triggered_ms"),
        }
        if streak >= max(confirm_after, 1):
            confirmed_rows.append(
                {
                    "交易所": exchange,
                    "等级": current["等级"],
                    "告警": title,
                    "解释": current["解释"],
                    "连续触发": streak,
                    "状态": "已确认",
                }
            )
            if not active:
                previous_trigger_ms = int(previous.get("last_triggered_ms") or 0)
                if cooldown_ms <= 0 or previous_trigger_ms <= 0 or now_ms - previous_trigger_ms >= cooldown_ms:
                    timeline.append(
                        {
                            "时间": pd.to_datetime(now_ms, unit="ms"),
                            "交易所": exchange,
                            "等级": current["等级"],
                            "告警": title,
                            "动作": "触发",
                            "说明": current["解释"],
                        }
                    )
                    current["last_triggered_ms"] = now_ms
                else:
                    current["last_triggered_ms"] = previous_trigger_ms
            current["active"] = True
        state[key] = current

    for key, previous in list(state.items()):
        if key in seen_keys:
            continue
        if previous.get("active"):
            timeline.append(
                {
                    "时间": pd.to_datetime(now_ms, unit="ms"),
                    "交易所": previous.get("交易所"),
                    "等级": previous.get("等级"),
                    "告警": previous.get("告警"),
                    "动作": "解除",
                    "说明": "本轮未继续满足条件，告警解除。",
                }
            )
        previous["active"] = False
        previous["streak"] = 0
        state[key] = previous

    pending_rows = []
    for previous in state.values():
        if previous.get("active"):
            continue
        streak = int(previous.get("streak", 0) or 0)
        if 0 < streak < max(confirm_after, 1):
            pending_rows.append(
                {
                    "交易所": previous.get("交易所"),
                    "等级": previous.get("等级"),
                    "告警": previous.get("告警"),
                    "解释": previous.get("解释"),
                    "连续触发": streak,
                    "状态": f"待确认 {streak}/{confirm_after}",
                }
            )

    confirmed_frame = pd.DataFrame(confirmed_rows + pending_rows)
    if confirmed_frame.empty:
        confirmed_frame = pd.DataFrame(columns=["交易所", "等级", "告警", "解释", "连续触发", "状态"])
    priority_map = {"强": 0, "中": 1, "弱": 2}
    confirmed_frame["_priority"] = confirmed_frame["等级"].map(priority_map).fillna(9)
    confirmed_frame = confirmed_frame.sort_values(["_priority", "交易所", "告警"]).drop(columns="_priority").reset_index(drop=True)

    timeline = timeline[-240:]
    timeline_frame = pd.DataFrame(timeline)
    if timeline_frame.empty:
        timeline_frame = pd.DataFrame(columns=["时间", "交易所", "等级", "告警", "动作", "说明"])
    else:
        timeline_frame = timeline_frame.sort_values("时间", ascending=False).reset_index(drop=True)
    return confirmed_frame, timeline_frame, state, timeline


def build_alert_timeline_figure(timeline_frame: pd.DataFrame, limit: int = 36) -> go.Figure:
    figure = go.Figure()
    if timeline_frame.empty:
        figure.add_annotation(text="等待告警连续确认", showarrow=False, x=0.5, y=0.5, xref="paper", yref="paper")
        figure.update_layout(height=300, margin=dict(l=12, r=12, t=24, b=12))
        return figure

    frame = timeline_frame.head(limit).sort_values("时间").copy()
    color_map = {"强": "#ff8d7a", "中": "#ffd76b", "弱": "#67d1ff"}
    symbol_map = {"触发": "diamond", "解除": "circle-open"}
    for level in ("强", "中", "弱"):
        level_frame = frame[frame["等级"] == level]
        if level_frame.empty:
            continue
        figure.add_trace(
            go.Scatter(
                x=level_frame["时间"],
                y=level_frame["交易所"],
                mode="markers",
                name=level,
                marker=dict(
                    size=[14 if action == "触发" else 10 for action in level_frame["动作"]],
                    color=color_map.get(level, "#dfe8f1"),
                    symbol=[symbol_map.get(action, "circle") for action in level_frame["动作"]],
                    line=dict(width=1, color="rgba(8, 16, 28, 0.78)"),
                ),
                customdata=level_frame[["告警", "动作", "说明"]],
                hovertemplate="%{x}<br>%{y}<br>%{customdata[0]} | %{customdata[1]}<br>%{customdata[2]}<extra></extra>",
            )
        )
    figure.update_layout(
        height=300,
        margin=dict(l=12, r=12, t=58, b=12),
        paper_bgcolor="rgba(14, 22, 35, 0.56)",
        plot_bgcolor="rgba(255, 255, 255, 0.045)",
        font=dict(color="#f6f9ff", family="SF Pro Display, Segoe UI, sans-serif"),
        title=dict(text="Alert Timeline", x=0.03, y=0.98, xanchor="left", font=dict(size=18, color="#f7fbff")),
        transition=dict(duration=280, easing="cubic-in-out"),
        legend=dict(orientation="h", yanchor="bottom", y=1.01, xanchor="right", x=1.0),
    )
    figure.update_xaxes(showgrid=False)
    figure.update_yaxes(showgrid=True, gridcolor="rgba(255, 255, 255, 0.08)")
    return figure


def build_orderbook_quality_frame(history: List[OrderBookQualityPoint], limit: int = 24) -> pd.DataFrame:
    if not history:
        return pd.DataFrame(
            columns=[
                "时间",
                "新增挂单额",
                "撤单额",
                "净变化",
                "近价新增",
                "近价撤单",
                "假挂单次数",
                "补单次数",
                "买墙持续(s)",
                "卖墙持续(s)",
                "盘口失衡(%)",
            ]
        )
    rows = []
    for point in history[-limit:]:
        rows.append(
            {
                "时间": pd.to_datetime(point.timestamp_ms, unit="ms"),
                "新增挂单额": point.added_notional,
                "撤单额": point.canceled_notional,
                "净变化": point.net_notional,
                "近价新增": point.near_added_notional,
                "近价撤单": point.near_canceled_notional,
                "假挂单次数": point.spoof_events,
                "补单次数": point.refill_events,
                "买墙持续(s)": point.bid_wall_persistence_s,
                "卖墙持续(s)": point.ask_wall_persistence_s,
                "盘口失衡(%)": point.imbalance_pct,
            }
        )
    return pd.DataFrame(rows[::-1])


def build_orderbook_quality_figure(history: List[OrderBookQualityPoint], title: str = "Orderbook Quality / Cancel Speed") -> go.Figure:
    figure = make_subplots(
        rows=2,
        cols=1,
        shared_xaxes=True,
        vertical_spacing=0.08,
        row_heights=[0.62, 0.38],
        specs=[[{"secondary_y": True}], [{"secondary_y": True}]],
    )
    if not history:
        figure.add_annotation(text="等待本地盘口质量样本", showarrow=False, x=0.5, y=0.5, xref="paper", yref="paper")
        figure.update_layout(height=380, margin=dict(l=12, r=12, t=24, b=12))
        return figure

    frame = build_orderbook_quality_frame(history, limit=180).sort_values("时间")
    figure.add_trace(
        go.Bar(
            x=frame["时间"],
            y=frame["新增挂单额"],
            name="新增挂单",
            marker_color="rgba(103, 209, 255, 0.72)",
            hovertemplate="%{x}<br>新增挂单额 %{y:,.0f}<extra></extra>",
        ),
        row=1,
        col=1,
    )
    figure.add_trace(
        go.Bar(
            x=frame["时间"],
            y=-frame["撤单额"],
            name="撤单",
            marker_color="rgba(255, 154, 89, 0.72)",
            hovertemplate="%{x}<br>撤单额 %{customdata:,.0f}<extra></extra>",
            customdata=frame["撤单额"],
        ),
        row=1,
        col=1,
    )
    figure.add_trace(
        go.Scatter(
            x=frame["时间"],
            y=frame["盘口失衡(%)"],
            mode="lines",
            name="盘口失衡",
            line=dict(color="#f8d35e", width=2.1),
            hovertemplate="%{x}<br>盘口失衡 %{y:.2f}%<extra></extra>",
        ),
        row=1,
        col=1,
        secondary_y=True,
    )
    figure.add_trace(
        go.Scatter(
            x=frame["时间"],
            y=frame["假挂单次数"],
            mode="lines+markers",
            name="假挂单",
            line=dict(color="#ff7b7b", width=1.8),
            hovertemplate="%{x}<br>假挂单次数 %{y}<extra></extra>",
        ),
        row=2,
        col=1,
    )
    figure.add_trace(
        go.Scatter(
            x=frame["时间"],
            y=frame["补单次数"],
            mode="lines+markers",
            name="补单",
            line=dict(color="#6fdd8c", width=1.8),
            hovertemplate="%{x}<br>补单次数 %{y}<extra></extra>",
        ),
        row=2,
        col=1,
    )
    figure.add_trace(
        go.Scatter(
            x=frame["时间"],
            y=frame["买墙持续(s)"] - frame["卖墙持续(s)"],
            mode="lines",
            name="墙体持续差",
            line=dict(color="#d8e3f2", width=1.5, dash="dot"),
            hovertemplate="%{x}<br>买卖墙持续差 %{y:.1f}s<extra></extra>",
        ),
        row=2,
        col=1,
        secondary_y=True,
    )
    figure.update_layout(
        height=400,
        barmode="relative",
        margin=dict(l=12, r=12, t=58, b=12),
        paper_bgcolor="rgba(14, 22, 35, 0.56)",
        plot_bgcolor="rgba(255, 255, 255, 0.045)",
        font=dict(color="#f6f9ff", family="SF Pro Display, Segoe UI, sans-serif"),
        title=dict(text=title, x=0.03, y=0.98, xanchor="left", font=dict(size=18, color="#f7fbff")),
        transition=dict(duration=280, easing="cubic-in-out"),
        legend=dict(orientation="h", yanchor="bottom", y=1.01, xanchor="right", x=1.0),
    )
    figure.update_xaxes(showgrid=False, row=1, col=1)
    figure.update_xaxes(showgrid=False, row=2, col=1)
    figure.update_yaxes(showgrid=True, gridcolor="rgba(255, 255, 255, 0.08)", tickformat=".2s", row=1, col=1)
    figure.update_yaxes(showgrid=False, secondary_y=True, row=1, col=1, title="盘口失衡(%)")
    figure.update_yaxes(showgrid=True, gridcolor="rgba(255, 255, 255, 0.08)", row=2, col=1, title="事件数")
    figure.update_yaxes(showgrid=False, secondary_y=True, row=2, col=1, title="持续差(s)")
    return figure


def build_composite_signal(
    snapshot: ExchangeSnapshot | None,
    oi_quadrant: Dict[str, float | str | None],
    trade_metrics: Dict[str, float | int | str | None],
    crowd_position_ratio: float | None = None,
    crowd_account_ratio: float | None = None,
    global_ratio: float | None = None,
) -> Dict[str, object]:
    funding_bps = snapshot.funding_bps if snapshot is not None else None
    price_change_pct = pd.to_numeric(oi_quadrant.get("price_change_pct"), errors="coerce")
    oi_change_pct = pd.to_numeric(oi_quadrant.get("oi_change_pct"), errors="coerce")
    delta_notional = pd.to_numeric(trade_metrics.get("delta_notional"), errors="coerce")
    buy_ratio = pd.to_numeric(trade_metrics.get("buy_ratio"), errors="coerce")
    crowd_bias = 0.0
    crowd_fields = [value for value in (crowd_position_ratio, crowd_account_ratio, global_ratio) if value is not None]
    if crowd_fields:
        crowd_bias = sum(value - 1.0 for value in crowd_fields) / len(crowd_fields) * 60.0

    contributions: List[Dict[str, object]] = []
    if not pd.isna(price_change_pct):
        contributions.append({"因子": "价格", "得分": max(-18.0, min(18.0, float(price_change_pct) * 8.0))})
    if not pd.isna(oi_change_pct):
        contributions.append({"因子": "OI Delta", "得分": max(-22.0, min(22.0, float(oi_change_pct) * 16.0))})
    if not pd.isna(delta_notional):
        contributions.append({"因子": "CVD / 主动买卖", "得分": max(-24.0, min(24.0, float(delta_notional) / 1_000_000.0))})
    if funding_bps is not None:
        contributions.append({"因子": "Funding", "得分": max(-16.0, min(16.0, float(funding_bps) * 4.0))})
    if crowd_fields:
        contributions.append({"因子": "Crowd", "得分": max(-20.0, min(20.0, crowd_bias))})

    score = sum(float(item["得分"]) for item in contributions)
    label = "信号混合"
    if score >= 30:
        label = "偏多推进"
    elif score <= -30:
        label = "偏空推进"
    else:
        regime = str(trade_metrics.get("regime") or "")
        if ("吸收" in regime or "衰竭" in regime) and not pd.isna(oi_change_pct) and abs(float(oi_change_pct)) >= 0.3:
            label = "拥挤但衰竭"
        elif "吸收" in regime:
            label = "吸收中"

    confidence = min(1.0, max(0.2, len(contributions) / 5.0))
    return {
        "score": score,
        "label": label,
        "confidence": confidence,
        "contributions": contributions,
    }


def build_composite_signal_figure(signal: Dict[str, object]) -> go.Figure:
    figure = make_subplots(rows=1, cols=2, subplot_titles=("总分", "因子贡献"), column_widths=[0.44, 0.56], specs=[[{"type": "indicator"}, {"type": "bar"}]])
    score = float(signal.get("score") or 0.0)
    contributions = pd.DataFrame(signal.get("contributions") or [], columns=["因子", "得分"])
    figure.add_trace(
        go.Indicator(
            mode="gauge+number",
            value=score,
            number=dict(suffix=" pt", font=dict(color="#f7fbff", size=28)),
            gauge=dict(
                axis=dict(range=[-100, 100], tickcolor="#dce8f6"),
                bar=dict(color="#f8d35e"),
                steps=[
                    dict(range=[-100, -30], color="rgba(255, 123, 123, 0.32)"),
                    dict(range=[-30, 30], color="rgba(103, 209, 255, 0.18)"),
                    dict(range=[30, 100], color="rgba(111, 221, 140, 0.28)"),
                ],
            ),
            title=dict(text=str(signal.get("label") or "Composite")),
        ),
        row=1,
        col=1,
    )
    if not contributions.empty:
        contributions = contributions.sort_values("得分")
        figure.add_trace(
            go.Bar(
                x=contributions["得分"],
                y=contributions["因子"],
                orientation="h",
                marker_color=["#ff8d7a" if value < 0 else "#6fdd8c" for value in contributions["得分"]],
                text=[f"{value:.1f}" for value in contributions["得分"]],
                textposition="outside",
                hovertemplate="%{y}<br>贡献 %{x:.1f}<extra></extra>",
                showlegend=False,
            ),
            row=1,
            col=2,
        )
    figure.update_layout(
        height=320,
        margin=dict(l=12, r=12, t=58, b=12),
        paper_bgcolor="rgba(14, 22, 35, 0.56)",
        plot_bgcolor="rgba(255, 255, 255, 0.045)",
        font=dict(color="#f6f9ff", family="SF Pro Display, Segoe UI, sans-serif"),
        title=dict(text="Composite Positioning Signal", x=0.03, y=0.98, xanchor="left", font=dict(size=18, color="#f7fbff")),
        transition=dict(duration=280, easing="cubic-in-out"),
    )
    figure.update_xaxes(showgrid=True, gridcolor="rgba(255, 255, 255, 0.08)", range=[-30, 30], row=1, col=2)
    figure.update_yaxes(showgrid=False, row=1, col=2)
    return figure


def build_recorded_event_frame(events: List[RecordedMarketEvent], limit: int = 120) -> pd.DataFrame:
    if not events:
        return pd.DataFrame(columns=["时间", "市场", "类型", "方向", "价格", "数量", "名义金额", "数值", "标签"])
    rows = []
    for event in sorted(events, key=lambda item: item.timestamp_ms, reverse=True)[:limit]:
        rows.append(
            {
                "时间": pd.to_datetime(event.timestamp_ms, unit="ms"),
                "市场": event.market,
                "类型": event.category,
                "方向": event.side,
                "价格": event.price,
                "数量": event.size,
                "名义金额": event.notional,
                "数值": event.value,
                "标签": event.label,
            }
        )
    return pd.DataFrame(rows)


def build_replay_figure(
    events: List[RecordedMarketEvent],
    window_start_ms: int,
    window_end_ms: int,
    progress_ratio: float = 1.0,
) -> go.Figure:
    figure = make_subplots(
        rows=2,
        cols=1,
        shared_xaxes=True,
        vertical_spacing=0.08,
        row_heights=[0.7, 0.3],
        specs=[[{"secondary_y": False}], [{"secondary_y": True}]],
    )
    relevant = [event for event in events if window_start_ms <= event.timestamp_ms <= window_end_ms]
    if not relevant:
        figure.add_annotation(text="当前回放窗口里还没有录制事件", showarrow=False, x=0.5, y=0.5, xref="paper", yref="paper")
        figure.update_layout(height=380, margin=dict(l=12, r=12, t=24, b=12))
        return figure

    end_ms = int(window_start_ms + (window_end_ms - window_start_ms) * max(0.0, min(1.0, progress_ratio)))
    active = [event for event in relevant if event.timestamp_ms <= end_ms]
    trade_rows = [
        {
            "时间": pd.to_datetime(event.timestamp_ms, unit="ms"),
            "价格": event.price,
            "市场": event.market,
            "方向": event.side,
        }
        for event in active
        if event.category == "trade" and event.price is not None
    ]
    if trade_rows:
        trade_frame = pd.DataFrame(trade_rows)
        for market, color in (("perp", "#ffd76b"), ("spot", "#67d1ff")):
            market_frame = trade_frame[trade_frame["市场"] == market]
            if market_frame.empty:
                continue
            figure.add_trace(
                go.Scatter(
                    x=market_frame["时间"],
                    y=market_frame["价格"],
                    mode="lines",
                    name=f"{market} 价格",
                    line=dict(color=color, width=2.0),
                    hovertemplate="%{x}<br>价格 %{y:,.2f}<extra></extra>",
                ),
                row=1,
                col=1,
            )

    liquidation_rows = [
        {
            "时间": pd.to_datetime(event.timestamp_ms, unit="ms"),
            "价格": event.price,
            "方向": event.side,
            "名义金额": event.notional,
        }
        for event in active
        if event.category == "liquidation" and event.price is not None
    ]
    if liquidation_rows:
        liq_frame = pd.DataFrame(liquidation_rows)
        for side, color, symbol in (("long", "#ff7b7b", "triangle-down"), ("short", "#5bc0ff", "triangle-up")):
            side_frame = liq_frame[liq_frame["方向"] == side]
            if side_frame.empty:
                continue
            figure.add_trace(
                go.Scatter(
                    x=side_frame["时间"],
                    y=side_frame["价格"],
                    mode="markers",
                    name="多头爆仓" if side == "long" else "空头爆仓",
                    marker=dict(color=color, symbol=symbol, size=12, line=dict(width=1, color="rgba(8, 16, 26, 0.88)")),
                    customdata=side_frame[["名义金额"]],
                    hovertemplate="%{x}<br>价格 %{y:,.2f}<br>爆仓额 %{customdata[0]:,.0f}<extra></extra>",
                ),
                row=1,
                col=1,
            )

    quality_rows = [
        {
            "时间": pd.to_datetime(event.timestamp_ms, unit="ms"),
            "净变化": event.value or 0.0,
            "市场": event.market,
            "假挂单": float((event.raw or {}).get("spoof_events", 0)),
        }
        for event in active
        if event.category == "orderbook_quality"
    ]
    if quality_rows:
        quality_frame = pd.DataFrame(quality_rows)
        figure.add_trace(
            go.Bar(
                x=quality_frame["时间"],
                y=quality_frame["净变化"],
                name="簿面净变化",
                marker_color=["rgba(111, 221, 140, 0.62)" if value >= 0 else "rgba(255, 141, 122, 0.68)" for value in quality_frame["净变化"]],
                hovertemplate="%{x}<br>净变化 %{y:,.0f}<extra></extra>",
            ),
            row=2,
            col=1,
        )
        figure.add_trace(
            go.Scatter(
                x=quality_frame["时间"],
                y=quality_frame["假挂单"],
                mode="lines",
                name="假挂单",
                line=dict(color="#f8d35e", width=1.8),
                hovertemplate="%{x}<br>假挂单 %{y}<extra></extra>",
            ),
            row=2,
            col=1,
            secondary_y=True,
        )

    cursor_time = pd.to_datetime(end_ms, unit="ms")
    figure.add_vline(x=cursor_time, line_dash="dot", line_color="#f8d35e", line_width=1.2)
    figure.update_layout(
        height=400,
        margin=dict(l=12, r=12, t=58, b=12),
        paper_bgcolor="rgba(14, 22, 35, 0.56)",
        plot_bgcolor="rgba(255, 255, 255, 0.045)",
        font=dict(color="#f6f9ff", family="SF Pro Display, Segoe UI, sans-serif"),
        title=dict(text="Session Replay", x=0.03, y=0.98, xanchor="left", font=dict(size=18, color="#f7fbff")),
        transition=dict(duration=280, easing="cubic-in-out"),
        legend=dict(orientation="h", yanchor="bottom", y=1.01, xanchor="right", x=1.0),
    )
    figure.update_xaxes(showgrid=False)
    figure.update_yaxes(showgrid=True, gridcolor="rgba(255, 255, 255, 0.08)", row=1, col=1, title="价格")
    figure.update_yaxes(showgrid=True, gridcolor="rgba(255, 255, 255, 0.08)", row=2, col=1, tickformat=".2s", title="净变化")
    figure.update_yaxes(showgrid=False, row=2, col=1, secondary_y=True, title="假挂单")
    return figure


def build_hyperliquid_predicted_funding_frame(
    payload: List[list],
    *,
    selected_coin: str | None = None,
    limit: int = 72,
) -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []
    focus_coin = str(selected_coin or "").strip().upper()
    for item in payload:
        if not isinstance(item, (list, tuple)) or len(item) != 2:
            continue
        coin, venues = item
        coin_text = str(coin or "").upper()
        if focus_coin and coin_text != focus_coin:
            continue
        if not isinstance(venues, list):
            continue
        for venue_item in venues:
            if not isinstance(venue_item, (list, tuple)) or len(venue_item) != 2:
                continue
            venue, meta = venue_item
            if not isinstance(meta, dict):
                continue
            funding_rate = pd.to_numeric(meta.get("fundingRate"), errors="coerce")
            funding_interval_hours = pd.to_numeric(meta.get("fundingIntervalHours"), errors="coerce")
            if pd.isna(funding_rate):
                continue
            rate_value = float(funding_rate)
            interval_value = float(funding_interval_hours) if not pd.isna(funding_interval_hours) and float(funding_interval_hours) > 0 else 8.0
            rows.append(
                {
                    "币种": coin_text,
                    "市场": str(venue),
                    "预测费率(bps)": rate_value * 10000.0,
                    "8h等价费率(bps)": rate_value * (8.0 / interval_value) * 10000.0,
                    "年化费率(%)": rate_value * (24.0 / interval_value) * 365.0 * 100.0,
                    "下次结算时间": pd.to_datetime(int(meta.get("nextFundingTime") or 0), unit="ms"),
                    "结算间隔(h)": interval_value,
                }
            )
    frame = pd.DataFrame(rows)
    if frame.empty:
        return pd.DataFrame(columns=["币种", "市场", "预测费率(bps)", "8h等价费率(bps)", "年化费率(%)", "下次结算时间", "结算间隔(h)"])
    frame["_focus"] = frame["币种"].eq(focus_coin).astype(int) if focus_coin else 0
    frame["_sort"] = frame["8h等价费率(bps)"].abs()
    frame = frame.sort_values(["_focus", "_sort", "币种", "市场"], ascending=[False, False, True, True]).drop(columns=["_focus", "_sort"])
    return frame.head(limit).reset_index(drop=True)


def build_hyperliquid_predicted_funding_figure(frame: pd.DataFrame) -> go.Figure:
    figure = go.Figure()
    if frame.empty:
        figure.add_annotation(text="等待 predictedFundings 样本", showarrow=False, x=0.5, y=0.5, xref="paper", yref="paper")
        figure.update_layout(height=360, margin=dict(l=12, r=12, t=24, b=12))
        return figure
    pivot = frame.pivot_table(index="币种", columns="市场", values="8h等价费率(bps)", aggfunc="mean").sort_index()
    figure.add_trace(
        go.Heatmap(
            z=pivot.values,
            x=list(pivot.columns),
            y=list(pivot.index),
            colorscale="RdBu",
            zmid=0.0,
            colorbar=dict(title="8h bps"),
            hovertemplate="币种 %{y}<br>市场 %{x}<br>8h等价 %{z:.2f} bps<extra></extra>",
        )
    )
    figure.update_layout(
        height=380,
        margin=dict(l=12, r=12, t=58, b=12),
        paper_bgcolor="rgba(14, 22, 35, 0.56)",
        plot_bgcolor="rgba(255, 255, 255, 0.045)",
        font=dict(color="#f6f9ff", family="SF Pro Display, Segoe UI, sans-serif"),
        title=dict(text="Hyperliquid Predicted Funding Surface", x=0.03, y=0.98, xanchor="left", font=dict(size=18, color="#f7fbff")),
    )
    figure.update_xaxes(showgrid=False)
    figure.update_yaxes(showgrid=False)
    return figure


def build_cross_exchange_spread_frame(snapshots: List[ExchangeSnapshot]) -> pd.DataFrame:
    valid = [snapshot for snapshot in snapshots if snapshot.status == "ok" and snapshot.last_price is not None]
    if not valid:
        return pd.DataFrame(columns=["交易所", "价格", "偏离中位(bps)", "Mark价", "Funding(bps)", "OI金额", "24h成交额"])
    median_price = pd.Series([snapshot.last_price for snapshot in valid]).median()
    rows = []
    for snapshot in valid:
        deviation_bps = None
        if median_price not in (None, 0):
            deviation_bps = (float(snapshot.last_price or 0.0) - float(median_price)) / float(median_price) * 10000.0
        rows.append(
            {
                "交易所": snapshot.exchange,
                "价格": snapshot.last_price,
                "偏离中位(bps)": deviation_bps,
                "Mark价": snapshot.mark_price,
                "Funding(bps)": snapshot.funding_bps,
                "OI金额": snapshot.open_interest_notional,
                "24h成交额": snapshot.volume_24h_notional,
            }
        )
    return pd.DataFrame(rows).sort_values("偏离中位(bps)", ascending=False, na_position="last").reset_index(drop=True)


def build_cross_exchange_spread_figure(frame: pd.DataFrame) -> go.Figure:
    figure = go.Figure()
    if frame.empty:
        figure.add_annotation(text="等待跨所价差样本", showarrow=False, x=0.5, y=0.5, xref="paper", yref="paper")
        figure.update_layout(height=300, margin=dict(l=12, r=12, t=24, b=12))
        return figure
    colors = ["#67d1ff" if value >= 0 else "#ff9a59" for value in frame["偏离中位(bps)"].fillna(0.0)]
    figure.add_trace(
        go.Bar(
            x=frame["交易所"],
            y=frame["偏离中位(bps)"],
            marker_color=colors,
            text=[f"{value:.2f}" if pd.notna(value) else "-" for value in frame["偏离中位(bps)"]],
            textposition="outside",
            hovertemplate="%{x}<br>偏离中位 %{y:.2f} bps<extra></extra>",
        )
    )
    figure.add_hline(y=0, line_dash="dot", line_color="rgba(255,255,255,0.28)")
    figure.update_layout(
        height=320,
        margin=dict(l=12, r=12, t=58, b=12),
        paper_bgcolor="rgba(14, 22, 35, 0.56)",
        plot_bgcolor="rgba(255, 255, 255, 0.045)",
        font=dict(color="#f6f9ff", family="SF Pro Display, Segoe UI, sans-serif"),
        title=dict(text="Cross-Exchange Real-Time Spread Deviation", x=0.03, y=0.98, xanchor="left", font=dict(size=18, color="#f7fbff")),
    )
    figure.update_xaxes(showgrid=False)
    figure.update_yaxes(showgrid=True, gridcolor="rgba(255, 255, 255, 0.08)", title="相对中位偏离 (bps)")
    return figure


def build_funding_arb_frame(snapshots: List[ExchangeSnapshot], limit: int = 10) -> pd.DataFrame:
    valid = [snapshot for snapshot in snapshots if snapshot.status == "ok" and snapshot.funding_bps is not None]
    rows: List[Dict[str, Any]] = []
    for index, left in enumerate(valid):
        for right in valid[index + 1 :]:
            diff = float(left.funding_bps or 0.0) - float(right.funding_bps or 0.0)
            if diff == 0:
                continue
            short_leg = left.exchange if diff > 0 else right.exchange
            long_leg = right.exchange if diff > 0 else left.exchange
            rows.append(
                {
                    "做多腿": long_leg,
                    "做空腿": short_leg,
                    "费率差(bps)": abs(diff),
                    "年化差(%)": abs(diff) / 10000.0 * 3.0 * 365.0 * 100.0,
                    "价格差(%)": None
                    if left.last_price in (None, 0) or right.last_price is None
                    else abs(float(left.last_price) - float(right.last_price)) / float(left.last_price) * 100.0,
                    "提示": f"多 {long_leg} / 空 {short_leg}",
                }
            )
    frame = pd.DataFrame(rows)
    if frame.empty:
        return pd.DataFrame(columns=["做多腿", "做空腿", "费率差(bps)", "年化差(%)", "价格差(%)", "提示"])
    return frame.sort_values("费率差(bps)", ascending=False).head(limit).reset_index(drop=True)


def build_funding_arb_figure(frame: pd.DataFrame) -> go.Figure:
    figure = go.Figure()
    if frame.empty:
        figure.add_annotation(text="等待跨所资金费率差样本", showarrow=False, x=0.5, y=0.5, xref="paper", yref="paper")
        figure.update_layout(height=300, margin=dict(l=12, r=12, t=24, b=12))
        return figure
    labels = [f"{row['做多腿']} / {row['做空腿']}" for _, row in frame.iterrows()]
    figure.add_trace(
        go.Bar(
            x=frame["费率差(bps)"],
            y=labels,
            orientation="h",
            marker_color="#6fdd8c",
            text=[f"{value:.2f}" for value in frame["费率差(bps)"]],
            textposition="outside",
            hovertemplate="%{y}<br>费率差 %{x:.2f} bps<extra></extra>",
        )
    )
    figure.update_layout(
        height=max(300, 48 * len(frame)),
        margin=dict(l=12, r=12, t=58, b=12),
        paper_bgcolor="rgba(14, 22, 35, 0.56)",
        plot_bgcolor="rgba(255, 255, 255, 0.045)",
        font=dict(color="#f6f9ff", family="SF Pro Display, Segoe UI, sans-serif"),
        title=dict(text="Funding Arbitrage Spread Opportunities", x=0.03, y=0.98, xanchor="left", font=dict(size=18, color="#f7fbff")),
    )
    figure.update_xaxes(showgrid=True, gridcolor="rgba(255, 255, 255, 0.08)", title="费率差 (bps)")
    figure.update_yaxes(showgrid=False)
    return figure


def build_exchange_share_frame(snapshots: List[ExchangeSnapshot]) -> pd.DataFrame:
    valid = [snapshot for snapshot in snapshots if snapshot.status == "ok"]
    if not valid:
        return pd.DataFrame(columns=["交易所", "价格", "OI金额", "OI份额(%)", "24h成交额", "成交份额(%)", "Funding(bps)"])
    total_oi = sum(float(snapshot.open_interest_notional or 0.0) for snapshot in valid)
    total_volume = sum(float(snapshot.volume_24h_notional or 0.0) for snapshot in valid)
    rows = []
    for snapshot in valid:
        oi_notional = float(snapshot.open_interest_notional or 0.0)
        volume = float(snapshot.volume_24h_notional or 0.0)
        rows.append(
            {
                "交易所": snapshot.exchange,
                "价格": snapshot.last_price,
                "OI金额": snapshot.open_interest_notional,
                "OI份额(%)": oi_notional / total_oi * 100.0 if total_oi > 0 else None,
                "24h成交额": snapshot.volume_24h_notional,
                "成交份额(%)": volume / total_volume * 100.0 if total_volume > 0 else None,
                "Funding(bps)": snapshot.funding_bps,
            }
        )
    return pd.DataFrame(rows).sort_values("OI份额(%)", ascending=False, na_position="last").reset_index(drop=True)


def build_exchange_share_figure(frame: pd.DataFrame) -> go.Figure:
    figure = go.Figure()
    if frame.empty:
        figure.add_annotation(text="等待交易所份额样本", showarrow=False, x=0.5, y=0.5, xref="paper", yref="paper")
        figure.update_layout(height=300, margin=dict(l=12, r=12, t=24, b=12))
        return figure
    figure.add_trace(
        go.Bar(
            x=frame["交易所"],
            y=frame["OI份额(%)"],
            name="OI份额",
            marker_color="rgba(103, 209, 255, 0.78)",
            hovertemplate="%{x}<br>OI份额 %{y:.2f}%<extra></extra>",
        )
    )
    figure.add_trace(
        go.Bar(
            x=frame["交易所"],
            y=frame["成交份额(%)"],
            name="成交份额",
            marker_color="rgba(248, 211, 94, 0.74)",
            hovertemplate="%{x}<br>成交份额 %{y:.2f}%<extra></extra>",
        )
    )
    figure.update_layout(
        height=340,
        barmode="group",
        margin=dict(l=12, r=12, t=58, b=12),
        paper_bgcolor="rgba(14, 22, 35, 0.56)",
        plot_bgcolor="rgba(255, 255, 255, 0.045)",
        font=dict(color="#f6f9ff", family="SF Pro Display, Segoe UI, sans-serif"),
        title=dict(text="OI / Volume Share Dynamics", x=0.03, y=0.98, xanchor="left", font=dict(size=18, color="#f7fbff")),
        legend=dict(orientation="h", yanchor="bottom", y=1.01, xanchor="right", x=1.0),
    )
    figure.update_xaxes(showgrid=False)
    figure.update_yaxes(showgrid=True, gridcolor="rgba(255, 255, 255, 0.08)", title="份额 (%)")
    return figure


def build_large_trade_frame(
    trades_by_exchange: Dict[str, List[TradeEvent]],
    *,
    min_notional: float = 250_000.0,
    limit: int = 80,
) -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []
    for exchange_name, events in trades_by_exchange.items():
        for event in events:
            notional = float(event.notional or 0.0)
            if notional < max(min_notional, 0.0):
                continue
            rows.append(
                {
                    "时间": pd.to_datetime(event.timestamp_ms, unit="ms"),
                    "交易所": exchange_name,
                    "方向": "主动买" if event.side == "buy" else "主动卖" if event.side == "sell" else event.side,
                    "价格": event.price,
                    "数量": event.size,
                    "名义金额": event.notional,
                    "侧向": event.side,
                }
            )
    frame = pd.DataFrame(rows)
    if frame.empty:
        return pd.DataFrame(columns=["时间", "交易所", "方向", "价格", "数量", "名义金额", "侧向"])
    return frame.sort_values(["时间", "名义金额"], ascending=[False, False]).head(limit).reset_index(drop=True)


def build_large_trade_figure(frame: pd.DataFrame) -> go.Figure:
    figure = go.Figure()
    if frame.empty:
        figure.add_annotation(text="等待跨所大单流样本", showarrow=False, x=0.5, y=0.5, xref="paper", yref="paper")
        figure.update_layout(height=320, margin=dict(l=12, r=12, t=24, b=12))
        return figure
    max_notional = max(float(frame["名义金额"].max() or 0.0), 1.0)
    for exchange_name in frame["交易所"].dropna().unique():
        exchange_frame = frame[frame["交易所"] == exchange_name]
        figure.add_trace(
            go.Scatter(
                x=exchange_frame["时间"],
                y=exchange_frame["价格"],
                mode="markers",
                name=str(exchange_name),
                marker=dict(
                    size=[8.0 + 26.0 * math.sqrt(max(float(value or 0.0), 0.0) / max_notional) for value in exchange_frame["名义金额"]],
                    color=["#6fdd8c" if side == "buy" else "#ff8d7a" for side in exchange_frame["侧向"]],
                    opacity=0.78,
                    line=dict(width=1, color="rgba(8, 16, 26, 0.82)"),
                ),
                customdata=exchange_frame[["方向", "名义金额"]],
                hovertemplate="%{x}<br>价格 %{y:,.4f}<br>%{customdata[0]}<br>名义金额 %{customdata[1]:,.0f}<extra></extra>",
            )
        )
    figure.update_layout(
        height=360,
        margin=dict(l=12, r=12, t=58, b=12),
        paper_bgcolor="rgba(14, 22, 35, 0.56)",
        plot_bgcolor="rgba(255, 255, 255, 0.045)",
        font=dict(color="#f6f9ff", family="SF Pro Display, Segoe UI, sans-serif"),
        title=dict(text="Aggregated Whale Prints", x=0.03, y=0.98, xanchor="left", font=dict(size=18, color="#f7fbff")),
        legend=dict(orientation="h", yanchor="bottom", y=1.01, xanchor="right", x=1.0),
    )
    figure.update_xaxes(showgrid=False)
    figure.update_yaxes(showgrid=True, gridcolor="rgba(255, 255, 255, 0.08)", title="价格")
    return figure


def build_multifactor_sentiment_frame(
    snapshots_by_exchange: Dict[str, ExchangeSnapshot],
    oi_metrics_by_exchange: Dict[str, Dict[str, float | str | None]],
    trade_metrics_by_exchange: Dict[str, Dict[str, float | int | str | None]],
    *,
    crowd_position_ratio: float | None = None,
    crowd_account_ratio: float | None = None,
    global_ratio: float | None = None,
) -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []
    crowd_bias = 0.0
    crowd_fields = [value for value in (crowd_position_ratio, crowd_account_ratio, global_ratio) if value is not None]
    if crowd_fields:
        crowd_bias = sum(float(value) - 1.0 for value in crowd_fields) / len(crowd_fields) * 28.0
    for exchange_key, snapshot in snapshots_by_exchange.items():
        if snapshot.status != "ok":
            continue
        exchange_name = snapshot.exchange
        oi_metrics = oi_metrics_by_exchange.get(exchange_name, {})
        trade_metrics = trade_metrics_by_exchange.get(exchange_name, {})
        price_score = pd.to_numeric(oi_metrics.get("price_change_pct"), errors="coerce")
        oi_score = pd.to_numeric(oi_metrics.get("oi_change_pct"), errors="coerce")
        buy_ratio = pd.to_numeric(trade_metrics.get("buy_ratio"), errors="coerce")
        flow_score = pd.to_numeric(trade_metrics.get("delta_notional"), errors="coerce")
        price_component = 0.0 if pd.isna(price_score) else max(-16.0, min(16.0, float(price_score) * 5.0))
        oi_component = 0.0 if pd.isna(oi_score) else max(-20.0, min(20.0, float(oi_score) * 14.0))
        flow_component = 0.0
        if not pd.isna(flow_score):
            flow_component += max(-18.0, min(18.0, float(flow_score) / 1_500_000.0))
        if not pd.isna(buy_ratio):
            flow_component += max(-12.0, min(12.0, (float(buy_ratio) - 0.5) * 80.0))
        funding_component = 0.0 if snapshot.funding_bps is None else max(-18.0, min(18.0, float(snapshot.funding_bps) * 3.0))
        crowd_component = crowd_bias if exchange_key == "binance" else 0.0
        total_score = price_component + oi_component + flow_component + funding_component + crowd_component
        label = "中性"
        if total_score >= 18:
            label = "偏多"
        elif total_score <= -18:
            label = "偏空"
        rows.append(
            {
                "交易所": exchange_name,
                "价格": snapshot.last_price,
                "总分": total_score,
                "情绪": label,
                "价格因子": price_component,
                "OI因子": oi_component,
                "流量因子": flow_component,
                "Funding因子": funding_component,
                "Crowd因子": crowd_component,
            }
        )
    frame = pd.DataFrame(rows)
    if frame.empty:
        return pd.DataFrame(columns=["交易所", "价格", "总分", "情绪", "价格因子", "OI因子", "流量因子", "Funding因子", "Crowd因子"])
    return frame.sort_values("总分", ascending=False).reset_index(drop=True)


def build_multifactor_sentiment_figure(frame: pd.DataFrame) -> go.Figure:
    figure = go.Figure()
    if frame.empty:
        figure.add_annotation(text="等待多因子情绪样本", showarrow=False, x=0.5, y=0.5, xref="paper", yref="paper")
        figure.update_layout(height=320, margin=dict(l=12, r=12, t=24, b=12))
        return figure
    component_cols = ["价格因子", "OI因子", "流量因子", "Funding因子", "Crowd因子"]
    heat = frame.set_index("交易所")[component_cols]
    figure.add_trace(
        go.Heatmap(
            z=heat.values,
            x=component_cols,
            y=list(heat.index),
            colorscale="RdBu",
            zmid=0.0,
            colorbar=dict(title="得分"),
            hovertemplate="交易所 %{y}<br>因子 %{x}<br>得分 %{z:.1f}<extra></extra>",
        )
    )
    figure.update_layout(
        height=360,
        margin=dict(l=12, r=12, t=58, b=12),
        paper_bgcolor="rgba(14, 22, 35, 0.56)",
        plot_bgcolor="rgba(255, 255, 255, 0.045)",
        font=dict(color="#f6f9ff", family="SF Pro Display, Segoe UI, sans-serif"),
        title=dict(text="Multi-Factor Sentiment Matrix", x=0.03, y=0.98, xanchor="left", font=dict(size=18, color="#f7fbff")),
    )
    figure.update_xaxes(showgrid=False)
    figure.update_yaxes(showgrid=False)
    return figure


def build_wall_absorption_frame(
    history: List[OrderBookQualityPoint],
    trade_metrics: Dict[str, float | int | str | None],
) -> pd.DataFrame:
    if not history:
        return pd.DataFrame(columns=["信号", "等级", "参考值", "解释"])
    recent = history[-min(6, len(history)) :]
    near_added = sum(float(point.near_added_notional or 0.0) for point in recent)
    near_canceled = sum(float(point.near_canceled_notional or 0.0) for point in recent)
    spoof_events = sum(int(point.spoof_events or 0) for point in recent)
    bid_wall = sum(float(point.bid_wall_persistence_s or 0.0) for point in recent)
    ask_wall = sum(float(point.ask_wall_persistence_s or 0.0) for point in recent)
    buy_ratio = pd.to_numeric(trade_metrics.get("buy_ratio"), errors="coerce")
    regime = str(trade_metrics.get("regime") or "")
    rows: List[Dict[str, Any]] = []
    cancel_ratio = near_canceled / max(near_added, 1.0)
    if cancel_ratio >= 1.35:
        rows.append({"信号": "墙体消失", "等级": "强" if cancel_ratio >= 1.8 else "中", "参考值": f"{cancel_ratio:.2f}x", "解释": "近价撤单明显快于新增挂单，挂单墙正在快速回撤。"})
    if ("吸收" in regime or "衰竭" in regime) and not pd.isna(buy_ratio):
        rows.append({"信号": "吸收 / 衰竭", "等级": "强" if abs(float(buy_ratio) - 0.5) >= 0.08 else "中", "参考值": f"主动买占比 {float(buy_ratio) * 100.0:.1f}%", "解释": f"逐笔成交当前显示 `{regime}`，更像被动盘在承接或拦截。"})
    if spoof_events >= 2:
        rows.append({"信号": "假挂单活跃", "等级": "中" if spoof_events < 4 else "强", "参考值": str(spoof_events), "解释": "最近样本里反复出现近价挂单快速出现又撤走的行为。"})
    if bid_wall > ask_wall * 1.3:
        rows.append({"信号": "买墙更持久", "等级": "观察", "参考值": f"{bid_wall - ask_wall:.1f}s", "解释": "下方买墙持续时间更长，短线更容易形成被动承接。"})
    elif ask_wall > bid_wall * 1.3:
        rows.append({"信号": "卖墙更持久", "等级": "观察", "参考值": f"{ask_wall - bid_wall:.1f}s", "解释": "上方卖墙持续时间更长，短线更容易被压制。"})
    frame = pd.DataFrame(rows)
    if frame.empty:
        return pd.DataFrame([{"信号": "暂无强信号", "等级": "低", "参考值": "-", "解释": "当前墙体撤单、吸收和假挂单没有形成明显共振。"}])
    return frame


def build_vpin_frame(trades: List[TradeEvent], bucket_count: int = 24) -> pd.DataFrame:
    if len(trades) < 8:
        return pd.DataFrame(columns=["时间", "Bucket成交额", "买卖失衡", "VPIN"])
    ordered = sorted([event for event in trades if event.notional is not None], key=lambda item: item.timestamp_ms)
    total_notional = sum(float(event.notional or 0.0) for event in ordered)
    if total_notional <= 0:
        return pd.DataFrame(columns=["时间", "Bucket成交额", "买卖失衡", "VPIN"])
    bucket_target = max(total_notional / max(bucket_count, 1), 1.0)
    rows: List[Dict[str, Any]] = []
    bucket_notional = 0.0
    signed_notional = 0.0
    bucket_end_ts = ordered[0].timestamp_ms
    for event in ordered:
        notional = float(event.notional or 0.0)
        bucket_notional += notional
        signed_notional += notional if event.side == "buy" else -notional
        bucket_end_ts = event.timestamp_ms
        if bucket_notional < bucket_target:
            continue
        imbalance = abs(signed_notional) / max(bucket_notional, 1.0)
        rows.append({"时间": pd.to_datetime(bucket_end_ts, unit="ms"), "Bucket成交额": bucket_notional, "买卖失衡": imbalance})
        bucket_notional = 0.0
        signed_notional = 0.0
    frame = pd.DataFrame(rows)
    if frame.empty:
        return pd.DataFrame(columns=["时间", "Bucket成交额", "买卖失衡", "VPIN"])
    frame["VPIN"] = frame["买卖失衡"].rolling(min_periods=1, window=min(8, len(frame))).mean()
    return frame


def build_vpin_figure(frame: pd.DataFrame) -> go.Figure:
    figure = go.Figure()
    if frame.empty:
        figure.add_annotation(text="等待 VPIN 样本", showarrow=False, x=0.5, y=0.5, xref="paper", yref="paper")
        figure.update_layout(height=300, margin=dict(l=12, r=12, t=24, b=12))
        return figure
    figure.add_trace(go.Bar(x=frame["时间"], y=frame["买卖失衡"], name="桶失衡", marker_color="rgba(103, 209, 255, 0.58)", hovertemplate="%{x}<br>失衡 %{y:.3f}<extra></extra>"))
    figure.add_trace(go.Scatter(x=frame["时间"], y=frame["VPIN"], mode="lines+markers", name="VPIN", line=dict(color="#f8d35e", width=2.2), hovertemplate="%{x}<br>VPIN %{y:.3f}<extra></extra>"))
    figure.update_layout(
        height=320,
        margin=dict(l=12, r=12, t=58, b=12),
        paper_bgcolor="rgba(14, 22, 35, 0.56)",
        plot_bgcolor="rgba(255, 255, 255, 0.045)",
        font=dict(color="#f6f9ff", family="SF Pro Display, Segoe UI, sans-serif"),
        title=dict(text="Approximate VPIN / Order Flow Toxicity", x=0.03, y=0.98, xanchor="left", font=dict(size=18, color="#f7fbff")),
        legend=dict(orientation="h", yanchor="bottom", y=1.01, xanchor="right", x=1.0),
    )
    figure.update_xaxes(showgrid=False)
    figure.update_yaxes(showgrid=True, gridcolor="rgba(255, 255, 255, 0.08)", title="毒性")
    return figure


def build_microstructure_anomaly_frame(
    book_summary: Dict[str, float | None],
    quality_history: List[OrderBookQualityPoint],
    trade_metrics: Dict[str, float | int | str | None],
) -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []
    spread_bps = pd.to_numeric(book_summary.get("spread_bps"), errors="coerce")
    imbalance_pct = pd.to_numeric(book_summary.get("imbalance_pct"), errors="coerce")
    near_cancel = sum(float(point.near_canceled_notional or 0.0) for point in quality_history[-4:])
    near_add = sum(float(point.near_added_notional or 0.0) for point in quality_history[-4:])
    spoof_events = sum(int(point.spoof_events or 0) for point in quality_history[-4:])
    buy_ratio = pd.to_numeric(trade_metrics.get("buy_ratio"), errors="coerce")
    if not pd.isna(spread_bps) and float(spread_bps) >= 4.0:
        rows.append({"异常": "价差突扩", "等级": "强" if float(spread_bps) >= 8.0 else "中", "指标": f"{float(spread_bps):.2f} bps", "解释": "盘口最优价差明显放宽，做市保护或流动性抽离在增强。"})
    if not pd.isna(imbalance_pct) and abs(float(imbalance_pct)) >= 24.0:
        rows.append({"异常": "盘口失衡极值", "等级": "中", "指标": f"{float(imbalance_pct):+.1f}%", "解释": "买卖盘名义金额失衡已经达到极值，短线更容易出现跳价。"})
    if near_cancel > max(near_add, 1.0) * 1.5:
        rows.append({"异常": "深度异常回撤", "等级": "强", "指标": f"{near_cancel / max(near_add, 1.0):.2f}x", "解释": "近价撤单速度显著快于补单速度，深度支撑正在塌陷。"})
    if spoof_events >= 3:
        rows.append({"异常": "假挂单密集", "等级": "中", "指标": str(spoof_events), "解释": "近价区域重复出现快速挂撤单，短线微结构可信度下降。"})
    if not pd.isna(buy_ratio) and (float(buy_ratio) >= 0.62 or float(buy_ratio) <= 0.38):
        rows.append({"异常": "主动流偏斜", "等级": "观察", "指标": f"{float(buy_ratio) * 100.0:.1f}%", "解释": "主动买卖比明显偏向单边，可能放大短时冲击成本。"})
    frame = pd.DataFrame(rows)
    if frame.empty:
        return pd.DataFrame([{"异常": "暂无明显微结构异常", "等级": "低", "指标": "-", "解释": "当前价差、失衡和撤单速度还没有形成共振。"}])
    return frame


def _detect_pattern_at(candles: List[Candle], index: int) -> List[Dict[str, Any]]:
    if index <= 0 or index >= len(candles):
        return []
    current = candles[index]
    previous = candles[index - 1]
    rows: List[Dict[str, Any]] = []
    body = abs(current.close - current.open)
    candle_range = max(current.high - current.low, 1e-9)
    upper_wick = current.high - max(current.open, current.close)
    lower_wick = min(current.open, current.close) - current.low
    if body > 0:
        if lower_wick >= body * 2.2 and upper_wick <= body * 0.9:
            rows.append({"时间": pd.to_datetime(current.timestamp_ms, unit="ms"), "形态": "Pin Bar", "方向": "看多", "强度": min(1.0, lower_wick / candle_range), "索引": index, "说明": "下影线明显更长，回落后被快速承接。"})
        if upper_wick >= body * 2.2 and lower_wick <= body * 0.9:
            rows.append({"时间": pd.to_datetime(current.timestamp_ms, unit="ms"), "形态": "Pin Bar", "方向": "看空", "强度": min(1.0, upper_wick / candle_range), "索引": index, "说明": "上影线明显更长，冲高后被快速压回。"})
    bullish_engulf = previous.close < previous.open and current.close > current.open and current.close >= previous.open and current.open <= previous.close
    bearish_engulf = previous.close > previous.open and current.close < current.open and current.open >= previous.close and current.close <= previous.open
    if bullish_engulf:
        rows.append({"时间": pd.to_datetime(current.timestamp_ms, unit="ms"), "形态": "吞没", "方向": "看多", "强度": min(1.0, abs(current.close - current.open) / max(abs(previous.close - previous.open), 1e-9)), "索引": index, "说明": "本根 K 线实体反包上一根阴线。"})
    if bearish_engulf:
        rows.append({"时间": pd.to_datetime(current.timestamp_ms, unit="ms"), "形态": "吞没", "方向": "看空", "强度": min(1.0, abs(current.close - current.open) / max(abs(previous.close - previous.open), 1e-9)), "索引": index, "说明": "本根 K 线实体反包上一根阳线。"})
    if index >= 14:
        closes = pd.Series([item.close for item in candles[max(0, index - 20) : index + 1]])
        delta = closes.diff()
        gain = delta.clip(lower=0.0).rolling(14).mean()
        loss = (-delta.clip(upper=0.0)).rolling(14).mean()
        rs = gain / loss.replace(0.0, pd.NA)
        rsi = (100.0 - 100.0 / (1.0 + rs)).fillna(50.0)
        if len(rsi) >= 3:
            price_change = current.close - candles[index - 2].close
            rsi_change = float(rsi.iloc[-1] - rsi.iloc[-3])
            if price_change < 0 and rsi_change > 4.0:
                rows.append({"时间": pd.to_datetime(current.timestamp_ms, unit="ms"), "形态": "背离", "方向": "看多", "强度": min(1.0, abs(rsi_change) / 12.0), "索引": index, "说明": "价格继续下探，但 RSI 反而抬升。"})
            elif price_change > 0 and rsi_change < -4.0:
                rows.append({"时间": pd.to_datetime(current.timestamp_ms, unit="ms"), "形态": "背离", "方向": "看空", "强度": min(1.0, abs(rsi_change) / 12.0), "索引": index, "说明": "价格继续抬升，但 RSI 反而转弱。"})
    return rows


def build_candlestick_pattern_frame(candles: List[Candle], limit: int = 10) -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []
    for index in range(1, len(candles)):
        rows.extend(_detect_pattern_at(candles, index))
    frame = pd.DataFrame(rows)
    if frame.empty:
        return pd.DataFrame(columns=["时间", "形态", "方向", "强度", "说明", "索引"])
    return frame.sort_values("时间", ascending=False).head(limit).reset_index(drop=True)


def build_signal_backtest_frame(candles: List[Candle], horizon_bars: int = 6) -> pd.DataFrame:
    if len(candles) < max(20, horizon_bars + 4):
        return pd.DataFrame(columns=["信号", "方向", "样本数", "胜率(%)", "平均收益(%)", "中位收益(%)", "平均MFE(%)", "平均MAE(%)"])
    signals: List[Dict[str, Any]] = []
    for index in range(1, len(candles) - horizon_bars):
        signals.extend(_detect_pattern_at(candles, index))
    if not signals:
        return pd.DataFrame(columns=["信号", "方向", "样本数", "胜率(%)", "平均收益(%)", "中位收益(%)", "平均MFE(%)", "平均MAE(%)"])
    result_rows: List[Dict[str, Any]] = []
    grouped: Dict[tuple, List[Dict[str, float]]] = {}
    for signal in signals:
        signal_index = int(signal.get("索引") or 0)
        if signal_index + horizon_bars >= len(candles):
            continue
        entry_price = candles[signal_index].close
        exit_price = candles[signal_index + horizon_bars].close
        if entry_price in (None, 0):
            continue
        raw_return = (exit_price - entry_price) / entry_price * 100.0
        direction = 1.0 if signal.get("方向") == "看多" else -1.0
        realized = raw_return * direction
        window = candles[signal_index + 1 : signal_index + horizon_bars + 1]
        highs = [float(item.high) for item in window if item.high is not None]
        lows = [float(item.low) for item in window if item.low is not None]
        favorable = None
        adverse = None
        if highs and lows:
            if direction > 0:
                favorable = (max(highs) - entry_price) / entry_price * 100.0
                adverse = (min(lows) - entry_price) / entry_price * 100.0
            else:
                favorable = (entry_price - min(lows)) / entry_price * 100.0
                adverse = (entry_price - max(highs)) / entry_price * 100.0
        grouped.setdefault((str(signal.get("形态")), str(signal.get("方向"))), []).append(
            {
                "return_pct": realized,
                "mfe_pct": favorable if favorable is not None else 0.0,
                "mae_pct": adverse if adverse is not None else 0.0,
            }
        )
    for (pattern, direction), samples in grouped.items():
        sample_frame = pd.DataFrame(samples)
        series = sample_frame["return_pct"]
        if series.empty:
            continue
        result_rows.append(
            {
                "信号": pattern,
                "方向": direction,
                "样本数": int(len(series)),
                "胜率(%)": float((series > 0).mean() * 100.0),
                "平均收益(%)": float(series.mean()),
                "中位收益(%)": float(series.median()),
                "平均MFE(%)": float(sample_frame["mfe_pct"].mean()) if "mfe_pct" in sample_frame.columns else None,
                "平均MAE(%)": float(sample_frame["mae_pct"].mean()) if "mae_pct" in sample_frame.columns else None,
            }
        )
    frame = pd.DataFrame(result_rows)
    if frame.empty:
        return pd.DataFrame(columns=["信号", "方向", "样本数", "胜率(%)", "平均收益(%)", "中位收益(%)", "平均MFE(%)", "平均MAE(%)"])
    return frame.sort_values(["胜率(%)", "样本数"], ascending=[False, False]).reset_index(drop=True)


def build_liquidation_density_frame(
    watchlist_rows: List[Dict[str, Any]],
    mids: Dict[str, str],
    *,
    selected_coin: str,
    window_pct: float = 12.0,
    bucket_count: int = 24,
) -> pd.DataFrame:
    coin = str(selected_coin or "").strip().upper()
    current_mid = pd.to_numeric(mids.get(coin), errors="coerce")
    if pd.isna(current_mid) or float(current_mid) <= 0:
        return pd.DataFrame(columns=["价格中位", "距离现价(%)", "方向", "爆仓密度", "地址数"])
    rows: List[Dict[str, Any]] = []
    for item in watchlist_rows:
        if str(item.get("coin") or "").upper() != coin:
            continue
        liq_price = pd.to_numeric(item.get("liquidation_price"), errors="coerce")
        position_value = pd.to_numeric(item.get("position_value"), errors="coerce")
        if pd.isna(liq_price) or float(liq_price) <= 0:
            continue
        distance_pct = (float(liq_price) - float(current_mid)) / float(current_mid) * 100.0
        if abs(distance_pct) > max(window_pct, 0.5):
            continue
        rows.append({"liquidation_price": float(liq_price), "distance_pct": distance_pct, "direction": "多头清算带" if str(item.get("side") or "") == "long" else "空头清算带", "weight": float(position_value) if not pd.isna(position_value) else 1.0, "address": item.get("address")})
    if not rows:
        return pd.DataFrame(columns=["价格中位", "距离现价(%)", "方向", "爆仓密度", "地址数"])
    frame = pd.DataFrame(rows)
    bounds = pd.interval_range(start=-window_pct, end=window_pct, periods=max(bucket_count, 4))
    frame["bucket"] = pd.cut(frame["distance_pct"], bounds)
    grouped = frame.groupby(["bucket", "direction"], dropna=True).agg({"weight": "sum", "address": pd.Series.nunique}).reset_index()
    grouped["bucket_mid"] = grouped["bucket"].apply(lambda interval: (float(interval.left) + float(interval.right)) * 0.5)
    grouped["价格中位"] = float(current_mid) * (1.0 + grouped["bucket_mid"] / 100.0)
    grouped["距离现价(%)"] = grouped["bucket_mid"]
    grouped["爆仓密度"] = grouped["weight"]
    grouped["地址数"] = grouped["address"]
    grouped["方向"] = grouped["direction"]
    result = grouped[["价格中位", "距离现价(%)", "方向", "爆仓密度", "地址数"]]
    return result.sort_values(["方向", "距离现价(%)"]).reset_index(drop=True)


def build_liquidation_density_figure(frame: pd.DataFrame, reference_price: float | None) -> go.Figure:
    figure = go.Figure()
    if frame.empty:
        figure.add_annotation(text="等待公开地址清算密度样本", showarrow=False, x=0.5, y=0.5, xref="paper", yref="paper")
        figure.update_layout(height=320, margin=dict(l=12, r=12, t=24, b=12))
        return figure
    for direction, color in (("多头清算带", "#5bc0ff"), ("空头清算带", "#ff8d7a")):
        direction_frame = frame[frame["方向"] == direction]
        if direction_frame.empty:
            continue
        figure.add_trace(go.Bar(x=direction_frame["价格中位"], y=direction_frame["爆仓密度"], name=direction, marker_color=color, hovertemplate="价格 %{x:,.4f}<br>密度 %{y:,.0f}<extra></extra>"))
    if reference_price not in (None, 0):
        figure.add_vline(x=float(reference_price), line_dash="dot", line_color="#f8d35e", line_width=1.2)
    figure.update_layout(
        height=340,
        barmode="group",
        margin=dict(l=12, r=12, t=58, b=12),
        paper_bgcolor="rgba(14, 22, 35, 0.56)",
        plot_bgcolor="rgba(255, 255, 255, 0.045)",
        font=dict(color="#f6f9ff", family="SF Pro Display, Segoe UI, sans-serif"),
        title=dict(text="Observed Liquidation Density Heatmap", x=0.03, y=0.98, xanchor="left", font=dict(size=18, color="#f7fbff")),
        legend=dict(orientation="h", yanchor="bottom", y=1.01, xanchor="right", x=1.0),
    )
    figure.update_xaxes(showgrid=False, title="价格")
    figure.update_yaxes(showgrid=True, gridcolor="rgba(255, 255, 255, 0.08)", title="密度")
    return figure


def _simulate_impact_bps(
    levels: List[OrderBookLevel],
    reference_price: float | None,
    *,
    aggress_side: str,
    quote_notional: float,
) -> Dict[str, float | None]:
    if reference_price in (None, 0) or quote_notional <= 0:
        return {"impact_bps": None, "fill_ratio": None}
    book_side = "ask" if aggress_side == "buy" else "bid"
    side_levels = [level for level in levels if level.side == book_side and level.price > 0 and level.size > 0]
    side_levels.sort(key=lambda item: item.price, reverse=(book_side == "bid"))
    if not side_levels:
        return {"impact_bps": None, "fill_ratio": 0.0}

    remaining_quote = float(quote_notional)
    spent_quote = 0.0
    filled_size = 0.0
    for level in side_levels:
        level_quote = float(level.price) * float(level.size)
        take_quote = min(level_quote, remaining_quote)
        if take_quote <= 0:
            continue
        take_size = take_quote / float(level.price)
        spent_quote += take_quote
        filled_size += take_size
        remaining_quote -= take_quote
        if remaining_quote <= 1e-9:
            break

    filled_quote = float(quote_notional) - max(remaining_quote, 0.0)
    fill_ratio = filled_quote / max(float(quote_notional), 1e-9)
    if filled_size <= 0 or spent_quote <= 0:
        return {"impact_bps": None, "fill_ratio": fill_ratio}
    average_price = spent_quote / filled_size
    if aggress_side == "buy":
        impact_bps = (average_price - float(reference_price)) / float(reference_price) * 10000.0
    else:
        impact_bps = (float(reference_price) - average_price) / float(reference_price) * 10000.0
    return {"impact_bps": max(impact_bps, 0.0), "fill_ratio": fill_ratio}


def _impact_cost_summary(
    levels: List[OrderBookLevel],
    reference_price: float | None,
    notionals: List[float] | None = None,
) -> Dict[str, float | None]:
    notionals = notionals or [5_000.0, 50_000.0, 250_000.0]
    summary: Dict[str, float | None] = {}
    for quote_notional in notionals:
        buy_metrics = _simulate_impact_bps(levels, reference_price, aggress_side="buy", quote_notional=quote_notional)
        sell_metrics = _simulate_impact_bps(levels, reference_price, aggress_side="sell", quote_notional=quote_notional)
        bucket_label = f"{int(quote_notional / 1000)}k"
        impact_values = [value for value in (buy_metrics.get("impact_bps"), sell_metrics.get("impact_bps")) if value is not None]
        fill_values = [value for value in (buy_metrics.get("fill_ratio"), sell_metrics.get("fill_ratio")) if value is not None]
        summary[f"{bucket_label}冲击(bps)"] = max(impact_values) if impact_values else None
        summary[f"{bucket_label}填充率(%)"] = min(fill_values) * 100.0 if fill_values else None
    return summary


def build_spot_flow_reference_frame(
    spot_snapshots: Dict[str, SpotSnapshot],
    spot_orderbooks: Dict[str, List[OrderBookLevel]],
    spot_trades_by_exchange: Dict[str, List[TradeEvent]],
    quality_history_by_exchange: Dict[str, List[OrderBookQualityPoint]] | None,
    exchange_title_map: Dict[str, str],
    *,
    exchange_keys: List[str] | None = None,
    now_ms: int | None = None,
    window_minutes: int = 15,
) -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []
    for exchange_key in exchange_keys or list(spot_snapshots):
        snapshot = spot_snapshots.get(exchange_key)
        if snapshot is None or snapshot.status != "ok":
            continue
        exchange_name = exchange_title_map.get(exchange_key, exchange_key.title())
        trade_metrics = build_trade_metrics(spot_trades_by_exchange.get(exchange_key, []), now_ms=now_ms, window_minutes=window_minutes)
        orderbook = spot_orderbooks.get(exchange_key, [])
        book_summary = summarize_orderbook(orderbook, snapshot.last_price)
        impact_summary = _impact_cost_summary(orderbook, snapshot.last_price)
        recent_quality = (quality_history_by_exchange or {}).get(exchange_key, [])[-6:]
        first_bid = next((point.best_bid for point in recent_quality if point.best_bid is not None), None)
        first_ask = next((point.best_ask for point in recent_quality if point.best_ask is not None), None)
        last_bid = next((point.best_bid for point in reversed(recent_quality) if point.best_bid is not None), None)
        last_ask = next((point.best_ask for point in reversed(recent_quality) if point.best_ask is not None), None)
        quote_drift_bps = None
        quote_range_bps = None
        if snapshot.last_price not in (None, 0):
            mid_points = [
                (float(point.best_bid) + float(point.best_ask)) * 0.5
                for point in recent_quality
                if point.best_bid is not None and point.best_ask is not None
            ]
            if first_bid is not None and first_ask is not None and last_bid is not None and last_ask is not None:
                first_mid = (float(first_bid) + float(first_ask)) * 0.5
                last_mid = (float(last_bid) + float(last_ask)) * 0.5
                quote_drift_bps = (last_mid - first_mid) / max(float(snapshot.last_price or 0.0), 1e-9) * 10000.0
            if mid_points:
                quote_range_bps = (max(mid_points) - min(mid_points)) / max(float(snapshot.last_price or 0.0), 1e-9) * 10000.0
        rows.append(
            {
                "交易所": exchange_name,
                "现货价格": snapshot.last_price,
                "24h成交额": snapshot.volume_24h_notional,
                f"{window_minutes}m净主动额": trade_metrics.get("delta_notional"),
                "主动买占比(%)": None if trade_metrics.get("buy_ratio") is None else float(trade_metrics.get("buy_ratio") or 0.0) * 100.0,
                "主动状态": trade_metrics.get("regime") or "样本不足",
                "价差(bps)": snapshot.spread_bps if snapshot.spread_bps is not None else book_summary.get("spread_bps"),
                "盘口失衡(%)": book_summary.get("imbalance_pct"),
                "可见深度": (book_summary.get("bid_notional") or 0.0) + (book_summary.get("ask_notional") or 0.0),
                "报价漂移(bps)": quote_drift_bps,
                "报价波动(bps)": quote_range_bps,
                "50k冲击(bps)": impact_summary.get("50k冲击(bps)"),
                "250k冲击(bps)": impact_summary.get("250k冲击(bps)"),
                "50k填充率(%)": impact_summary.get("50k填充率(%)"),
                "250k填充率(%)": impact_summary.get("250k填充率(%)"),
                "时间": snapshot.timestamp_ms,
            }
        )
    frame = pd.DataFrame(rows)
    if frame.empty:
        return pd.DataFrame(
            columns=[
                "交易所",
                "现货价格",
                "24h成交额",
                f"{window_minutes}m净主动额",
                "主动买占比(%)",
                "主动状态",
                "价差(bps)",
                "盘口失衡(%)",
                "可见深度",
                "报价漂移(bps)",
                "报价波动(bps)",
                "50k冲击(bps)",
                "250k冲击(bps)",
                "50k填充率(%)",
                "250k填充率(%)",
                "时间",
            ]
        )
    sort_column = f"{window_minutes}m净主动额"
    frame["_priority"] = pd.to_numeric(frame[sort_column], errors="coerce").abs().fillna(0.0)
    return frame.sort_values(["_priority", "交易所"], ascending=[False, True]).drop(columns="_priority").reset_index(drop=True)


def build_spot_flow_reference_figure(frame: pd.DataFrame, *, window_minutes: int = 15) -> go.Figure:
    figure = make_subplots(specs=[[{"secondary_y": True}]])
    if frame.empty:
        figure.add_annotation(text="等待现货净主动流样本", showarrow=False, x=0.5, y=0.5, xref="paper", yref="paper")
        figure.update_layout(height=320, margin=dict(l=12, r=12, t=24, b=12))
        return figure
    delta_column = f"{window_minutes}m净主动额"
    figure.add_trace(
        go.Bar(
            x=frame["交易所"],
            y=frame[delta_column],
            name="净主动额",
            marker_color=["#57b06b" if value >= 0 else "#ff7b7b" for value in frame[delta_column].fillna(0.0)],
            hovertemplate="%{x}<br>净主动额 %{y:,.0f}<extra></extra>",
        ),
        secondary_y=False,
    )
    figure.add_trace(
        go.Scatter(
            x=frame["交易所"],
            y=frame["主动买占比(%)"],
            mode="lines+markers",
            name="主动买占比",
            line=dict(color="#67d1ff", width=2.2),
            marker=dict(size=8, color="#67d1ff"),
            hovertemplate="%{x}<br>主动买占比 %{y:.1f}%<extra></extra>",
        ),
        secondary_y=True,
    )
    figure.update_layout(
        height=340,
        margin=dict(l=12, r=12, t=58, b=12),
        paper_bgcolor="rgba(14, 22, 35, 0.56)",
        plot_bgcolor="rgba(255, 255, 255, 0.045)",
        font=dict(color="#f6f9ff", family="SF Pro Display, Segoe UI, sans-serif"),
        title=dict(text="Spot Net Aggressive Flow", x=0.03, y=0.98, xanchor="left", font=dict(size=18, color="#f7fbff")),
        legend=dict(orientation="h", yanchor="bottom", y=1.01, xanchor="right", x=1.0),
    )
    figure.update_xaxes(showgrid=False)
    figure.update_yaxes(showgrid=True, gridcolor="rgba(255, 255, 255, 0.08)", title="净主动额", secondary_y=False)
    figure.update_yaxes(showgrid=False, title="主动买占比(%)", range=[0, 100], secondary_y=True)
    return figure


def build_execution_quality_frame(
    snapshots_by_exchange: Dict[str, ExchangeSnapshot | SpotSnapshot],
    orderbooks_by_exchange: Dict[str, List[OrderBookLevel]],
    quality_history_by_exchange: Dict[str, List[OrderBookQualityPoint]],
    exchange_title_map: Dict[str, str],
    *,
    exchange_keys: List[str] | None = None,
    market_label: str = "现货",
) -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []
    for exchange_key in exchange_keys or list(snapshots_by_exchange):
        snapshot = snapshots_by_exchange.get(exchange_key)
        if snapshot is None or getattr(snapshot, "status", "ok") != "ok":
            continue
        reference_price = getattr(snapshot, "last_price", None)
        orderbook = orderbooks_by_exchange.get(exchange_key, [])
        book_summary = summarize_orderbook(orderbook, reference_price)
        impact_summary = _impact_cost_summary(orderbook, reference_price)
        recent_history = quality_history_by_exchange.get(exchange_key, [])[-6:]
        near_added = sum(float(point.near_added_notional or 0.0) for point in recent_history)
        near_canceled = sum(float(point.near_canceled_notional or 0.0) for point in recent_history)
        quote_midpoints = [
            (float(point.best_bid) + float(point.best_ask)) * 0.5
            for point in recent_history
            if point.best_bid is not None and point.best_ask is not None
        ]
        quote_drift_bps = None
        quote_range_bps = None
        if quote_midpoints and reference_price not in (None, 0):
            quote_drift_bps = (quote_midpoints[-1] - quote_midpoints[0]) / max(float(reference_price or 0.0), 1e-9) * 10000.0
            quote_range_bps = (max(quote_midpoints) - min(quote_midpoints)) / max(float(reference_price or 0.0), 1e-9) * 10000.0
        rows.append(
            {
                "交易所": exchange_title_map.get(exchange_key, exchange_key.title()),
                "市场": market_label,
                "价格": reference_price,
                "价差(bps)": book_summary.get("spread_bps"),
                "盘口失衡(%)": book_summary.get("imbalance_pct"),
                "可见深度": (book_summary.get("bid_notional") or 0.0) + (book_summary.get("ask_notional") or 0.0),
                "50k冲击(bps)": impact_summary.get("50k冲击(bps)"),
                "250k冲击(bps)": impact_summary.get("250k冲击(bps)"),
                "50k填充率(%)": impact_summary.get("50k填充率(%)"),
                "250k填充率(%)": impact_summary.get("250k填充率(%)"),
                "近价新增额": near_added,
                "近价撤单额": near_canceled,
                "近价净变化": near_added - near_canceled if recent_history else None,
                "近价撤补比": near_canceled / max(near_added, 1.0) if recent_history else None,
                "补单次数": sum(int(point.refill_events or 0) for point in recent_history),
                "假挂单次数": sum(int(point.spoof_events or 0) for point in recent_history),
                "报价漂移(bps)": quote_drift_bps,
                "报价波动(bps)": quote_range_bps,
                "时间": getattr(snapshot, "timestamp_ms", None),
            }
        )
    frame = pd.DataFrame(rows)
    if frame.empty:
        return pd.DataFrame(
            columns=[
                "交易所",
                "市场",
                "价格",
                "价差(bps)",
                "盘口失衡(%)",
                "可见深度",
                "50k冲击(bps)",
                "250k冲击(bps)",
                "50k填充率(%)",
                "250k填充率(%)",
                "近价新增额",
                "近价撤单额",
                "近价净变化",
                "近价撤补比",
                "补单次数",
                "假挂单次数",
                "报价漂移(bps)",
                "报价波动(bps)",
                "时间",
            ]
        )
    return frame.sort_values(["50k冲击(bps)", "交易所"], ascending=[True, True], na_position="last").reset_index(drop=True)


def build_execution_quality_figure(frame: pd.DataFrame, *, title: str) -> go.Figure:
    figure = make_subplots(specs=[[{"secondary_y": True}]])
    if frame.empty:
        figure.add_annotation(text="等待执行质量样本", showarrow=False, x=0.5, y=0.5, xref="paper", yref="paper")
        figure.update_layout(height=320, margin=dict(l=12, r=12, t=24, b=12))
        return figure
    figure.add_trace(
        go.Bar(
            x=frame["交易所"],
            y=frame["50k冲击(bps)"],
            name="50k冲击",
            marker_color="rgba(103, 209, 255, 0.78)",
            hovertemplate="%{x}<br>50k 冲击 %{y:.2f} bps<extra></extra>",
        ),
        secondary_y=False,
    )
    figure.add_trace(
        go.Bar(
            x=frame["交易所"],
            y=frame["250k冲击(bps)"],
            name="250k冲击",
            marker_color="rgba(255, 154, 89, 0.78)",
            hovertemplate="%{x}<br>250k 冲击 %{y:.2f} bps<extra></extra>",
        ),
        secondary_y=False,
    )
    figure.add_trace(
        go.Scatter(
            x=frame["交易所"],
            y=frame["价差(bps)"],
            mode="lines+markers",
            name="价差",
            line=dict(color="#f8d35e", width=2.1),
            marker=dict(size=8, color="#f8d35e"),
            hovertemplate="%{x}<br>价差 %{y:.2f} bps<extra></extra>",
        ),
        secondary_y=True,
    )
    figure.update_layout(
        height=340,
        barmode="group",
        margin=dict(l=12, r=12, t=58, b=12),
        paper_bgcolor="rgba(14, 22, 35, 0.56)",
        plot_bgcolor="rgba(255, 255, 255, 0.045)",
        font=dict(color="#f6f9ff", family="SF Pro Display, Segoe UI, sans-serif"),
        title=dict(text=title, x=0.03, y=0.98, xanchor="left", font=dict(size=18, color="#f7fbff")),
        legend=dict(orientation="h", yanchor="bottom", y=1.01, xanchor="right", x=1.0),
    )
    figure.update_xaxes(showgrid=False)
    figure.update_yaxes(showgrid=True, gridcolor="rgba(255, 255, 255, 0.08)", title="冲击成本 (bps)", secondary_y=False)
    figure.update_yaxes(showgrid=False, title="价差 (bps)", secondary_y=True)
    return figure


def build_execution_route_frame(
    spot_execution_frame: pd.DataFrame,
    perp_execution_frame: pd.DataFrame,
    decision_frame: pd.DataFrame,
) -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []
    decision_lookup = {
        str(row.get("交易所") or ""): row
        for row in decision_frame.to_dict("records")
    } if not decision_frame.empty else {}
    for source_frame, market_label in ((spot_execution_frame, "现货"), (perp_execution_frame, "合约")):
        if source_frame.empty:
            continue
        for row in source_frame.to_dict("records"):
            exchange_name = str(row.get("交易所") or "")
            decision_row = decision_lookup.get(exchange_name, {})
            spread_bps = _coerce_float(row.get("价差(bps)"))
            impact_50k = _coerce_float(row.get("50k冲击(bps)"))
            impact_250k = _coerce_float(row.get("250k冲击(bps)"))
            fill_50k = _coerce_float(row.get("50k填充率(%)"))
            fill_250k = _coerce_float(row.get("250k填充率(%)"))
            visible_depth = _coerce_float(row.get("可见深度"))
            cost_score = 0.0
            for value, weight in ((spread_bps, 1.0), (impact_50k, 1.3), (impact_250k, 0.6)):
                if value is not None:
                    cost_score += float(value) * weight
            if fill_50k is not None:
                cost_score -= max(0.0, float(fill_50k) - 60.0) * 0.08
            if fill_250k is not None:
                cost_score -= max(0.0, float(fill_250k) - 45.0) * 0.05
            if visible_depth is not None:
                cost_score -= min(18.0, math.log10(max(float(visible_depth), 1.0)) * 2.6)
            execution_hint = "观察"
            decision_label = str(decision_row.get("主导判断") or "")
            if market_label == "现货" and decision_label in {"现货先动", "现货主导"}:
                execution_hint = "优先现货"
            elif market_label == "合约" and decision_label in {"合约追价", "合约抢跑", "偏多拥挤", "偏空拥挤"}:
                execution_hint = "优先合约"
            elif cost_score <= 6.0:
                execution_hint = "可优先"
            elif cost_score >= 18.0:
                execution_hint = "谨慎"
            rows.append(
                {
                    "路由": f"{exchange_name} | {market_label}",
                    "交易所": exchange_name,
                    "市场": market_label,
                    "执行评分": round(max(0.0, 100.0 - max(0.0, cost_score) * 4.0), 1),
                    "价差(bps)": spread_bps,
                    "50k冲击(bps)": impact_50k,
                    "250k冲击(bps)": impact_250k,
                    "50k填充率(%)": fill_50k,
                    "250k填充率(%)": fill_250k,
                    "可见深度": visible_depth,
                    "执行建议": execution_hint,
                    "关联判断": decision_label or "等待样本",
                }
            )
    frame = pd.DataFrame(rows)
    if frame.empty:
        return pd.DataFrame(
            columns=[
                "路由",
                "交易所",
                "市场",
                "执行评分",
                "价差(bps)",
                "50k冲击(bps)",
                "250k冲击(bps)",
                "50k填充率(%)",
                "250k填充率(%)",
                "可见深度",
                "执行建议",
                "关联判断",
            ]
        )
    return frame.sort_values(["执行评分", "50k冲击(bps)"], ascending=[False, True], na_position="last").reset_index(drop=True)


def build_execution_route_figure(frame: pd.DataFrame, *, title: str = "Execution Route Matrix") -> go.Figure:
    figure = go.Figure()
    if frame.empty:
        figure.add_annotation(text="等待路由矩阵样本", showarrow=False, x=0.5, y=0.5, xref="paper", yref="paper")
        figure.update_layout(height=320, margin=dict(l=12, r=12, t=24, b=12))
        return figure
    colors = []
    for row in frame.to_dict("records"):
        hint = str(row.get("执行建议") or "")
        if hint == "优先现货":
            colors.append("#67d1ff")
        elif hint == "优先合约":
            colors.append("#ffd76b")
        elif hint == "谨慎":
            colors.append("#ff8d7a")
        else:
            colors.append("#6fdd8c")
    figure.add_trace(
        go.Bar(
            x=frame["路由"],
            y=frame["执行评分"],
            marker_color=colors,
            text=[f"{float(value):.1f}" if pd.notna(value) else "-" for value in frame["执行评分"]],
            textposition="outside",
            customdata=frame[["交易所", "市场", "执行建议", "50k冲击(bps)", "价差(bps)"]].to_numpy(),
            hovertemplate=(
                "%{customdata[0]} | %{customdata[1]}<br>"
                + "执行评分 %{y:.1f}<br>"
                + "执行建议 %{customdata[2]}<br>"
                + "50k 冲击 %{customdata[3]:.2f} bps<br>"
                + "价差 %{customdata[4]:.2f} bps<extra></extra>"
            ),
        )
    )
    figure.update_layout(
        height=max(340, 26 * len(frame) + 120),
        margin=dict(l=16, r=16, t=58, b=20),
        paper_bgcolor="rgba(14, 22, 35, 0.56)",
        plot_bgcolor="rgba(255, 255, 255, 0.045)",
        font=dict(color="#f6f9ff", family="SF Pro Display, Segoe UI, sans-serif"),
        title=dict(text=title, x=0.03, y=0.98, xanchor="left", font=dict(size=18, color="#f7fbff")),
    )
    figure.update_xaxes(showgrid=False, tickangle=-18)
    figure.update_yaxes(showgrid=True, gridcolor="rgba(255, 255, 255, 0.08)", title="执行评分")
    return figure


def _bar_window_metrics(frame: pd.DataFrame, minutes: int) -> Dict[str, Any]:
    if frame.empty or "bucket_ms" not in frame.columns:
        return {
            "open": None,
            "close": None,
            "high": None,
            "low": None,
            "price_change_pct": None,
            "range_pct": None,
            "volume_notional": None,
            "large_trade_share_pct": None,
            "trade_count": None,
            "sample_count": 0,
        }
    working = frame.copy()
    working["bucket_ms"] = pd.to_numeric(working.get("bucket_ms"), errors="coerce")
    working = working.dropna(subset=["bucket_ms"]).sort_values("bucket_ms", ascending=True)
    if working.empty:
        return {
            "open": None,
            "close": None,
            "high": None,
            "low": None,
            "price_change_pct": None,
            "range_pct": None,
            "volume_notional": None,
            "large_trade_share_pct": None,
            "trade_count": None,
            "sample_count": 0,
        }
    latest_ms = int(working["bucket_ms"].iloc[-1])
    cutoff_ms = latest_ms - max(int(minutes or 1), 1) * 60_000
    window = working[working["bucket_ms"].astype("int64") >= cutoff_ms].copy()
    if window.empty:
        window = working.tail(max(2, int(minutes or 1))).copy()
    open_price = _coerce_float(window.iloc[0].get("open"))
    if open_price in (None, 0):
        open_price = _coerce_float(window.iloc[0].get("close"))
    close_price = _coerce_float(window.iloc[-1].get("close"))
    if close_price in (None, 0):
        close_price = _coerce_float(window.iloc[-1].get("open"))
    high_value = _coerce_float(pd.to_numeric(window.get("high"), errors="coerce").max())
    low_value = _coerce_float(pd.to_numeric(window.get("low"), errors="coerce").min())
    price_change_pct = None
    if open_price not in (None, 0) and close_price is not None:
        price_change_pct = (float(close_price) - float(open_price)) / max(float(open_price), 1e-9) * 100.0
    range_pct = None
    if open_price not in (None, 0) and high_value is not None and low_value is not None:
        range_pct = (float(high_value) - float(low_value)) / max(float(open_price), 1e-9) * 100.0
    volume_notional = pd.to_numeric(window.get("volume_notional"), errors="coerce").fillna(0.0).sum()
    large_trade_notional = pd.to_numeric(window.get("large_trade_notional"), errors="coerce").fillna(0.0).sum()
    trade_count = pd.to_numeric(window.get("trade_count"), errors="coerce").fillna(0.0).sum()
    large_trade_share_pct = None
    if float(volume_notional or 0.0) > 0:
        large_trade_share_pct = float(large_trade_notional) / float(volume_notional) * 100.0
    return {
        "open": open_price,
        "close": close_price,
        "high": high_value,
        "low": low_value,
        "price_change_pct": price_change_pct,
        "range_pct": range_pct,
        "volume_notional": float(volume_notional) if volume_notional else None,
        "large_trade_share_pct": large_trade_share_pct,
        "trade_count": int(trade_count) if trade_count else None,
        "sample_count": int(len(window.index)),
    }


def _oi_window_metrics(frame: pd.DataFrame, minutes: int) -> Dict[str, Any]:
    if frame.empty:
        return {"oi_change_pct": None, "latest_oi_notional": None, "sample_count": 0}
    working = frame.copy()
    time_column = "bucket_ms" if "bucket_ms" in working.columns else "ts_ms"
    working[time_column] = pd.to_numeric(working.get(time_column), errors="coerce")
    working["open_interest_notional"] = pd.to_numeric(working.get("open_interest_notional"), errors="coerce")
    working = working.dropna(subset=[time_column, "open_interest_notional"]).sort_values(time_column, ascending=True)
    if working.empty:
        return {"oi_change_pct": None, "latest_oi_notional": None, "sample_count": 0}
    latest_ts = int(working[time_column].iloc[-1])
    cutoff_ms = latest_ts - max(int(minutes or 1), 1) * 60_000
    latest_value = _coerce_float(working.iloc[-1].get("open_interest_notional"))
    reference_slice = working[working[time_column].astype("int64") <= cutoff_ms]
    reference_row = reference_slice.iloc[-1] if not reference_slice.empty else working.iloc[0]
    reference_value = _coerce_float(reference_row.get("open_interest_notional"))
    oi_change_pct = None
    if latest_value not in (None, 0) and reference_value not in (None, 0):
        oi_change_pct = (float(latest_value) - float(reference_value)) / max(float(reference_value), 1e-9) * 100.0
    return {
        "oi_change_pct": oi_change_pct,
        "latest_oi_notional": latest_value,
        "sample_count": int(len(working.index)),
    }


def build_multi_timeframe_resonance_frame(
    spot_bars: pd.DataFrame,
    perp_bars: pd.DataFrame,
    oi_frame: pd.DataFrame,
    *,
    basis_pct: float | None = None,
    avg_funding_bps: float | None = None,
    lead_summary: str | None = None,
    timeframes: List[tuple[str, int]] | None = None,
) -> pd.DataFrame:
    periods = timeframes or [("5m", 5), ("15m", 15), ("1h", 60), ("4h", 240)]
    rows: List[Dict[str, Any]] = []
    for label, minutes in periods:
        spot_metrics = _bar_window_metrics(spot_bars, minutes)
        perp_metrics = _bar_window_metrics(perp_bars, minutes)
        oi_metrics = _oi_window_metrics(oi_frame, minutes)
        spot_change = _coerce_float(spot_metrics.get("price_change_pct"))
        perp_change = _coerce_float(perp_metrics.get("price_change_pct"))
        oi_change = _coerce_float(oi_metrics.get("oi_change_pct"))
        spot_volume = _coerce_float(spot_metrics.get("volume_notional"))
        perp_volume = _coerce_float(perp_metrics.get("volume_notional"))
        spot_share_pct = None
        if spot_volume is not None or perp_volume is not None:
            total_volume = float(spot_volume or 0.0) + float(perp_volume or 0.0)
            if total_volume > 0:
                spot_share_pct = float(spot_volume or 0.0) / total_volume * 100.0
        large_trade_share_pct = _coerce_float(perp_metrics.get("large_trade_share_pct"))
        alignment_score = 0.42
        same_direction = False
        if spot_change is not None and perp_change is not None:
            active_move = max(abs(float(spot_change)), abs(float(perp_change)))
            same_direction = active_move < 0.12 or (float(spot_change) >= 0 and float(perp_change) >= 0) or (float(spot_change) <= 0 and float(perp_change) <= 0)
            spread = abs(float(spot_change) - float(perp_change))
            alignment_score = 1.0 - clamp01(spread / max(active_move, 0.35))
            if same_direction and active_move < 0.18:
                alignment_score = max(alignment_score, 0.48)
        spot_support = 0.22 if spot_share_pct is None else clamp01((float(spot_share_pct) - 16.0) / 22.0)
        oi_support = 0.36
        if oi_change is not None and perp_change is not None and abs(float(perp_change)) >= 0.10:
            if float(oi_change) == 0:
                oi_support = 0.44
            elif (float(oi_change) > 0 and float(perp_change) > 0) or (float(oi_change) < 0 and float(perp_change) < 0):
                oi_support = 0.50 + 0.50 * clamp01(abs(float(oi_change)) / 1.6)
            else:
                oi_support = max(0.0, 0.34 - 0.24 * clamp01(abs(float(oi_change)) / 1.2))
        flow_support = 0.26 if large_trade_share_pct is None else clamp01(float(large_trade_share_pct) / 22.0)
        move_support = clamp01(max(abs(float(spot_change or 0.0)), abs(float(perp_change or 0.0))) / 1.2)
        basis_support = 1.0 - clamp01(abs(float(basis_pct or 0.0)) / 0.35)
        resonance_score = (
            0.30 * alignment_score
            + 0.20 * spot_support
            + 0.18 * oi_support
            + 0.14 * flow_support
            + 0.12 * move_support
            + 0.06 * basis_support
        ) * 100.0
        if avg_funding_bps is not None and abs(float(avg_funding_bps)) >= 5.0:
            resonance_score -= 4.0 + clamp01((abs(float(avg_funding_bps)) - 5.0) / 4.0) * 6.0
        resonance_score = max(0.0, min(100.0, resonance_score))
        if max(abs(float(spot_change or 0.0)), abs(float(perp_change or 0.0))) < 0.18 and abs(float(oi_change or 0.0)) < 0.25:
            conclusion = "蓄势 / 等待"
            structure = "波动与 OI 都还不大，先等 15m/1h 继续展开。"
        elif not same_direction:
            conclusion = "现货 / 合约背离"
            structure = "现货和合约方向不同步，先防假突破。"
        elif oi_change is not None and perp_change is not None and abs(float(oi_change)) >= 0.25 and (
            (float(oi_change) > 0 and float(perp_change) > 0) or (float(oi_change) < 0 and float(perp_change) < 0)
        ) and resonance_score >= 74.0:
            conclusion = "强共振"
            structure = "现货与合约同向，OI 也在配合扩张。"
        elif spot_change is not None and perp_change is not None and abs(float(spot_change)) + 0.05 < abs(float(perp_change)):
            conclusion = "合约抢跑"
            structure = "合约波动明显快于现货，最好等现货补确认。"
        elif resonance_score >= 62.0:
            conclusion = "现货确认"
            structure = "现货跟随、量能和结构都在配合。"
        else:
            conclusion = "弱共振"
            structure = "结构有配合，但延续性还不够扎实。"
        if avg_funding_bps is not None and abs(float(avg_funding_bps)) >= 5.0 and conclusion not in {"蓄势 / 等待", "现货 / 合约背离"}:
            structure = f"{structure} Funding 偏热，追单风险更高。"
        if lead_summary and label == "15m":
            structure = f"{structure} Lead/Lag: {lead_summary}"
        rows.append(
            {
                "周期": label,
                "现货变化(%)": spot_change,
                "合约变化(%)": perp_change,
                "OI变化(%)": oi_change,
                "现货成交占比(%)": spot_share_pct,
                "大单占比(%)": large_trade_share_pct,
                "共振分": round(float(resonance_score), 1),
                "共振结论": conclusion,
                "结构提示": structure,
            }
        )
    frame = pd.DataFrame(rows)
    if frame.empty:
        return pd.DataFrame(
            columns=["周期", "现货变化(%)", "合约变化(%)", "OI变化(%)", "现货成交占比(%)", "大单占比(%)", "共振分", "共振结论", "结构提示"]
        )
    return frame.reset_index(drop=True)


def build_multi_timeframe_resonance_figure(frame: pd.DataFrame, *, title: str = "Multi-Timeframe Resonance") -> go.Figure:
    figure = go.Figure()
    if frame.empty:
        figure.add_annotation(text="等待多周期共振样本", showarrow=False, x=0.5, y=0.5, xref="paper", yref="paper")
        figure.update_layout(height=320, margin=dict(l=12, r=12, t=24, b=12))
        return figure
    metric_specs = [
        ("现货变化(%)", lambda value: max(-1.0, min(1.0, float(value) / 2.0)) if pd.notna(value) else None, lambda value: f"{float(value):+.2f}%" if pd.notna(value) else "-"),
        ("合约变化(%)", lambda value: max(-1.0, min(1.0, float(value) / 2.0)) if pd.notna(value) else None, lambda value: f"{float(value):+.2f}%" if pd.notna(value) else "-"),
        ("OI变化(%)", lambda value: max(-1.0, min(1.0, float(value) / 3.0)) if pd.notna(value) else None, lambda value: f"{float(value):+.2f}%" if pd.notna(value) else "-"),
        ("现货成交占比(%)", lambda value: max(-1.0, min(1.0, (float(value) - 28.0) / 18.0)) if pd.notna(value) else None, lambda value: f"{float(value):.1f}%" if pd.notna(value) else "-"),
        ("大单占比(%)", lambda value: max(-1.0, min(1.0, (float(value) - 12.0) / 12.0)) if pd.notna(value) else None, lambda value: f"{float(value):.1f}%" if pd.notna(value) else "-"),
        ("共振分", lambda value: max(-1.0, min(1.0, (float(value) - 50.0) / 50.0)) if pd.notna(value) else None, lambda value: f"{float(value):.1f}" if pd.notna(value) else "-"),
    ]
    z_rows: List[List[float | None]] = []
    text_rows: List[List[str]] = []
    for _, row in frame.iterrows():
        z_row: List[float | None] = []
        text_row: List[str] = []
        for column_name, score_builder, text_builder in metric_specs:
            value = pd.to_numeric(row.get(column_name), errors="coerce")
            z_row.append(score_builder(value))
            text_row.append(text_builder(value))
        z_rows.append(z_row)
        text_rows.append(text_row)
    figure.add_trace(
        go.Heatmap(
            x=[item[0] for item in metric_specs],
            y=frame["周期"],
            z=z_rows,
            text=text_rows,
            texttemplate="%{text}",
            zmin=-1.0,
            zmax=1.0,
            zmid=0.0,
            colorscale=[[0.0, "#d14d57"], [0.5, "#eef3fb"], [1.0, "#4bc07a"]],
            xgap=6,
            ygap=6,
            showscale=False,
            hovertemplate="周期 %{y}<br>指标 %{x}<br>数值 %{text}<extra></extra>",
        )
    )
    figure.update_layout(
        height=max(320, 82 * len(frame)),
        margin=dict(l=12, r=12, t=58, b=12),
        paper_bgcolor="rgba(14, 22, 35, 0.56)",
        plot_bgcolor="rgba(255, 255, 255, 0.045)",
        font=dict(color="#f6f9ff", family="SF Pro Display, Segoe UI, sans-serif"),
        title=dict(text=title, x=0.03, y=0.98, xanchor="left", font=dict(size=18, color="#f7fbff")),
    )
    figure.update_xaxes(showgrid=False, tickangle=-12)
    figure.update_yaxes(showgrid=False, autorange="reversed")
    return figure


def build_breakout_quality_frame(
    resonance_frame: pd.DataFrame,
    *,
    spot_metrics: Dict[str, Any] | None = None,
    perp_metrics: Dict[str, Any] | None = None,
    best_route_row: Dict[str, Any] | None = None,
    thresholds: Dict[str, float] | None = None,
    lead_summary: str | None = None,
) -> pd.DataFrame:
    columns = ["维度", "当前值", "结论", "动作"]
    if resonance_frame.empty:
        return pd.DataFrame(columns=columns)
    resonance_lookup = {
        str(row.get("周期") or ""): dict(row)
        for row in resonance_frame.to_dict("records")
    }
    default_thresholds = {
        "spot_confirm_min": 62.0,
        "perp_crowding_caution": 62.0,
        "perp_crowding_block": 72.0,
        "funding_caution": 4.0,
        "funding_block": 5.5,
    }
    threshold_map = {**default_thresholds, **(thresholds or {})}

    def _row_value(period: str, key: str) -> float | None:
        return _coerce_float((resonance_lookup.get(period) or {}).get(key))

    weighted_move = _weighted_average(
        [
            (_row_value("5m", "合约变化(%)"), 0.35),
            (_row_value("15m", "合约变化(%)"), 0.40),
            (_row_value("1h", "合约变化(%)"), 0.20),
            (_row_value("4h", "合约变化(%)"), 0.05),
        ]
    )
    avg_resonance = _weighted_average(
        [
            (_row_value("5m", "共振分"), 0.22),
            (_row_value("15m", "共振分"), 0.33),
            (_row_value("1h", "共振分"), 0.30),
            (_row_value("4h", "共振分"), 0.15),
        ]
    )
    short_resonance = _weighted_average(
        [
            (_row_value("5m", "共振分"), 0.45),
            (_row_value("15m", "共振分"), 0.40),
            (_row_value("1h", "共振分"), 0.15),
        ]
    )
    perp_direction_values = [_row_value(period, "合约变化(%)") for period in ("5m", "15m", "1h")]
    active_direction_values = [float(value) for value in perp_direction_values if value is not None and abs(float(value)) >= 0.12]
    consistency_score = 42.0
    if len(active_direction_values) >= 2:
        same_direction = all(value > 0 for value in active_direction_values) or all(value < 0 for value in active_direction_values)
        consistency_score = 82.0 if same_direction else 34.0
    elif len(active_direction_values) == 1:
        consistency_score = 58.0
    if avg_resonance is not None:
        consistency_score = max(0.0, min(100.0, consistency_score + (float(avg_resonance) - 55.0) * 0.18))
    direction_label = "震荡 / 假动作"
    direction_word = "突破"
    if weighted_move is not None and float(weighted_move) >= 0.22:
        direction_label = "上破尝试"
        direction_word = "上破"
    elif weighted_move is not None and float(weighted_move) <= -0.22:
        direction_label = "下破尝试"
        direction_word = "下破"

    spot_metrics = spot_metrics or {}
    perp_metrics = perp_metrics or {}
    best_route_row = best_route_row or {}
    spot_score = _coerce_float(spot_metrics.get("confirmation_score"))
    spot_status = str(spot_metrics.get("status") or "样本不足")
    crowding_score = _coerce_float(perp_metrics.get("crowding_score"))
    avg_funding_bps = _coerce_float(perp_metrics.get("avg_funding_bps"))
    route_score = _coerce_float(best_route_row.get("执行评分") or best_route_row.get("现货评分") or best_route_row.get("合约评分"))
    route_market = str(best_route_row.get("更优市场") or best_route_row.get("市场") or "-")
    route_hint = str(best_route_row.get("执行建议") or best_route_row.get("关联判断") or "等待样本")

    leverage_quality = 100.0
    if crowding_score is not None:
        caution = float(threshold_map["perp_crowding_caution"])
        block = float(threshold_map["perp_crowding_block"])
        if float(crowding_score) > caution:
            leverage_quality -= min(42.0, (float(crowding_score) - caution) * 1.5)
        if float(crowding_score) >= block:
            leverage_quality -= 12.0
    if avg_funding_bps is not None:
        leverage_quality -= max(0.0, abs(float(avg_funding_bps)) - float(threshold_map["funding_caution"])) * 7.0
    leverage_quality = max(0.0, min(100.0, leverage_quality))

    quality_score = (
        0.34 * float(avg_resonance or 0.0)
        + 0.22 * float(spot_score or 0.0)
        + 0.18 * float(consistency_score)
        + 0.16 * float(route_score or 0.0)
        + 0.10 * float(leverage_quality)
    )
    if direction_label == "震荡 / 假动作":
        quality_score = min(float(quality_score), 56.0)
    if spot_score is not None and float(spot_score) < float(threshold_map["spot_confirm_min"]) - 10.0 and crowding_score is not None and float(crowding_score) >= float(threshold_map["perp_crowding_caution"]):
        quality_score -= 10.0
    if short_resonance is not None and avg_resonance is not None and float(short_resonance) >= float(avg_resonance) + 12.0:
        quality_score -= 4.0
    quality_score = max(0.0, min(100.0, quality_score))

    if direction_label == "震荡 / 假动作":
        summary_label = "震荡假动作"
        summary_action = "等待 15m / 1h 共振继续抬升，再考虑开单。"
    elif quality_score >= 78.0 and (spot_score or 0.0) >= float(threshold_map["spot_confirm_min"]) and leverage_quality >= 50.0:
        summary_label = f"A级{direction_word}"
        summary_action = "可以按主导方向择优执行，但依然优先低成本市场。"
    elif quality_score >= 68.0 and (spot_score or 0.0) >= float(threshold_map["spot_confirm_min"]) - 8.0:
        summary_label = f"B级{direction_word}"
        summary_action = "可以轻仓顺势，优先等回踩 / 反抽后的二次确认。"
    elif crowding_score is not None and float(crowding_score) >= float(threshold_map["perp_crowding_block"]):
        summary_label = f"高拥挤{direction_word}"
        summary_action = "不建议追单，只看回撤后的确认点。"
    elif (spot_score or 0.0) < float(threshold_map["spot_confirm_min"]) - 12.0:
        summary_label = f"低质量{direction_word}"
        summary_action = "先等现货确认和 15m / 1h 共振修复。"
    else:
        summary_label = f"待确认{direction_word}"
        summary_action = "继续观察量能、OI 和执行成本是否继续配合。"

    rows = [
        {
            "维度": "方向结构",
            "当前值": "-" if weighted_move is None else f"{float(weighted_move):+.2f}%",
            "结论": direction_label,
            "动作": lead_summary or "优先结合 15m / 1h 的共振和现货确认一起看。",
        },
        {
            "维度": "多周期共振",
            "当前值": "-" if avg_resonance is None else f"{float(avg_resonance):.1f}",
            "结论": f"短线 {('-' if short_resonance is None else f'{float(short_resonance):.1f}')} / 一致性 {float(consistency_score):.1f}",
            "动作": "5m、15m、1h 越同向，突破延续性越高。",
        },
        {
            "维度": "现货确认",
            "当前值": "-" if spot_score is None else f"{float(spot_score):.1f}",
            "结论": spot_status,
            "动作": "现货分数越接近或高于阈值，越适合顺势而不是纯追杠杆。"},
        {
            "维度": "杠杆温度",
            "当前值": (
                "-"
                if crowding_score is None and avg_funding_bps is None
                else f"拥挤 {('-' if crowding_score is None else f'{float(crowding_score):.1f}')} / Funding {('-' if avg_funding_bps is None else f'{float(avg_funding_bps):+.2f}bps')}"
            ),
            "结论": "杠杆健康" if leverage_quality >= 58.0 else "杠杆偏热",
            "动作": "拥挤和 Funding 同时抬高时，不要把冲刺段当成低风险突破。",
        },
        {
            "维度": "执行成本",
            "当前值": "-" if route_score is None else f"{float(route_score):.1f}",
            "结论": f"{route_market} | {route_hint}",
            "动作": "优先执行评分更高、冲击更低的市场，不用和最差深度硬碰。",
        },
        {
            "维度": "突破质量",
            "当前值": f"{float(quality_score):.1f}",
            "结论": summary_label,
            "动作": summary_action,
        },
    ]
    return pd.DataFrame(rows, columns=columns)


def _classify_crowding_trio(
    ratio_value: float | None,
    oi_change_pct: float | None,
    active_buy_pct: float | None,
    funding_bps: float | None,
) -> str:
    if ratio_value is not None and oi_change_pct is not None and active_buy_pct is not None:
        if ratio_value >= 1.05 and oi_change_pct >= 0.35 and active_buy_pct >= 54.0:
            return "多头拥挤推进"
        if ratio_value <= 0.95 and oi_change_pct >= 0.35 and active_buy_pct <= 46.0:
            return "空头拥挤推进"
        if ratio_value >= 1.04 and oi_change_pct < 0:
            return "多头拥挤松动"
        if ratio_value <= 0.96 and oi_change_pct < 0:
            return "空头拥挤松动"
    if active_buy_pct is not None:
        if active_buy_pct >= 58.0:
            return "主动买主导"
        if active_buy_pct <= 42.0:
            return "主动卖主导"
    if funding_bps is not None:
        if funding_bps >= 0.8:
            return "多头付费"
        if funding_bps <= -0.8:
            return "空头付费"
    return "中性 / 样本不足"


def build_perp_crowding_trio_frame(
    contract_sentiment_frame: pd.DataFrame,
    oi_metrics_by_exchange: Dict[str, Dict[str, float | str | None]],
) -> pd.DataFrame:
    if contract_sentiment_frame.empty:
        return pd.DataFrame(
            columns=["交易所", "合约多空比", "OI变化(%)", "主动流买占比(%)", "资金费率(bps)", "拥挤状态", "口径置信度", "时间"]
        )
    rows: List[Dict[str, Any]] = []
    for row in contract_sentiment_frame.to_dict("records"):
        exchange = str(row.get("交易所") or "")
        oi_metrics = oi_metrics_by_exchange.get(exchange, {})
        ratio_value = pd.to_numeric(row.get("合约多空比"), errors="coerce")
        oi_change_pct = pd.to_numeric(oi_metrics.get("oi_change_pct"), errors="coerce")
        active_buy_pct = pd.to_numeric(row.get("主动流买占比(%)"), errors="coerce")
        funding_bps = pd.to_numeric(row.get("资金费率(bps)"), errors="coerce")
        ratio_float = None if pd.isna(ratio_value) else float(ratio_value)
        oi_float = None if pd.isna(oi_change_pct) else float(oi_change_pct)
        active_float = None if pd.isna(active_buy_pct) else float(active_buy_pct)
        funding_float = None if pd.isna(funding_bps) else float(funding_bps)
        rows.append(
            {
                "交易所": exchange,
                "合约多空比": ratio_float,
                "OI变化(%)": oi_float,
                "主动流买占比(%)": active_float,
                "资金费率(bps)": funding_float,
                "拥挤状态": _classify_crowding_trio(ratio_float, oi_float, active_float, funding_float),
                "口径置信度": row.get("口径置信度"),
                "时间": row.get("时间"),
            }
        )
    frame = pd.DataFrame(rows)
    frame["_priority"] = (
        (pd.to_numeric(frame["合约多空比"], errors="coerce").sub(1.0).abs().fillna(0.0) * 100.0)
        + pd.to_numeric(frame["OI变化(%)"], errors="coerce").abs().fillna(0.0) * 2.0
        + pd.to_numeric(frame["主动流买占比(%)"], errors="coerce").sub(50.0).abs().fillna(0.0)
    )
    return frame.sort_values(["_priority", "交易所"], ascending=[False, True]).drop(columns="_priority").reset_index(drop=True)


def build_perp_crowding_trio_figure(frame: pd.DataFrame) -> go.Figure:
    figure = go.Figure()
    if frame.empty:
        figure.add_annotation(text="等待合约拥挤度三件套样本", showarrow=False, x=0.5, y=0.5, xref="paper", yref="paper")
        figure.update_layout(height=320, margin=dict(l=12, r=12, t=24, b=12))
        return figure
    metric_specs = [
        ("合约多空比", lambda value: max(-1.0, min(1.0, (float(value) - 1.0) / 0.12)) if pd.notna(value) else None, lambda value: f"{float(value):.3f}" if pd.notna(value) else "-"),
        ("OI变化(%)", lambda value: max(-1.0, min(1.0, float(value) / 1.0)) if pd.notna(value) else None, lambda value: f"{float(value):+.2f}%" if pd.notna(value) else "-"),
        ("主动流买占比(%)", lambda value: max(-1.0, min(1.0, (float(value) - 50.0) / 15.0)) if pd.notna(value) else None, lambda value: f"{float(value):.1f}%" if pd.notna(value) else "-"),
        ("资金费率(bps)", lambda value: max(-1.0, min(1.0, float(value) / 3.0)) if pd.notna(value) else None, lambda value: f"{float(value):+.2f}" if pd.notna(value) else "-"),
    ]
    z_rows: List[List[float | None]] = []
    text_rows: List[List[str]] = []
    for _, row in frame.iterrows():
        z_row: List[float | None] = []
        text_row: List[str] = []
        for column_name, score_builder, text_builder in metric_specs:
            value = pd.to_numeric(row.get(column_name), errors="coerce")
            z_row.append(score_builder(value))
            text_row.append(text_builder(value))
        z_rows.append(z_row)
        text_rows.append(text_row)
    figure.add_trace(
        go.Heatmap(
            x=[item[0] for item in metric_specs],
            y=frame["交易所"],
            z=z_rows,
            text=text_rows,
            texttemplate="%{text}",
            zmin=-1.0,
            zmax=1.0,
            zmid=0.0,
            colorscale=[[0.0, "#d14d57"], [0.5, "#eef3fb"], [1.0, "#4bc07a"]],
            xgap=6,
            ygap=6,
            showscale=False,
            hovertemplate="交易所 %{y}<br>指标 %{x}<br>数值 %{text}<extra></extra>",
        )
    )
    figure.update_layout(
        height=max(320, 78 * len(frame)),
        margin=dict(l=12, r=12, t=58, b=12),
        paper_bgcolor="rgba(14, 22, 35, 0.56)",
        plot_bgcolor="rgba(255, 255, 255, 0.045)",
        font=dict(color="#f6f9ff", family="SF Pro Display, Segoe UI, sans-serif"),
        title=dict(text="Perp Crowding Trio", x=0.03, y=0.98, xanchor="left", font=dict(size=18, color="#f7fbff")),
    )
    figure.update_xaxes(showgrid=False, side="top")
    figure.update_yaxes(showgrid=False)
    return figure


def _extract_next_funding_time_ms(snapshot: ExchangeSnapshot) -> int | None:
    exchange_name = str(snapshot.exchange or "").lower()
    raw = snapshot.raw or {}
    if "binance" in exchange_name:
        return _coerce_int((raw.get("premium_index") or {}).get("nextFundingTime")) if isinstance(raw, dict) else None
    if "bybit" in exchange_name:
        return _coerce_int(raw.get("nextFundingTime")) if isinstance(raw, dict) else None
    if "okx" in exchange_name:
        return _coerce_int((raw.get("funding_rate") or {}).get("nextFundingTime")) if isinstance(raw, dict) else None
    return None


def _extract_next_funding_rate_bps(snapshot: ExchangeSnapshot) -> float | None:
    exchange_name = str(snapshot.exchange or "").lower()
    raw = snapshot.raw or {}
    if "okx" in exchange_name:
        next_rate = _coerce_float((raw.get("funding_rate") or {}).get("nextFundingRate")) if isinstance(raw, dict) else None
        return None if next_rate is None else next_rate * 10000.0
    return snapshot.funding_bps


def _extract_funding_band_bps(snapshot: ExchangeSnapshot) -> float | None:
    exchange_name = str(snapshot.exchange or "").lower()
    raw = snapshot.raw or {}
    if "bybit" in exchange_name:
        cap = _coerce_float(raw.get("fundingCap")) if isinstance(raw, dict) else None
        return None if cap is None else abs(cap) * 2.0 * 10000.0
    if "okx" in exchange_name:
        funding_raw = raw.get("funding_rate") if isinstance(raw, dict) else {}
        max_rate = _coerce_float((funding_raw or {}).get("maxFundingRate"))
        min_rate = _coerce_float((funding_raw or {}).get("minFundingRate"))
        if max_rate is not None and min_rate is not None:
            return abs(max_rate - min_rate) * 10000.0
    return None


def _extract_basis_rate_pct(snapshot: ExchangeSnapshot, basis_pct: float | None) -> float | None:
    raw = snapshot.raw or {}
    exchange_name = str(snapshot.exchange or "").lower()
    if "bybit" in exchange_name and isinstance(raw, dict):
        bybit_basis_rate = _coerce_float(raw.get("basisRate"))
        if bybit_basis_rate is not None:
            return bybit_basis_rate * 100.0
    return basis_pct


def _classify_carry_state(
    funding_bps: float | None,
    premium_pct: float | None,
    basis_pct: float | None,
    next_funding_bps: float | None,
) -> str:
    if funding_bps is not None and basis_pct is not None:
        if funding_bps >= 0.8 and basis_pct >= 0.05:
            return "正 Carry / 空永续多现货"
        if funding_bps <= -0.8 and basis_pct <= -0.05:
            return "反向 Carry / 多永续空现货"
    if funding_bps is not None and premium_pct is not None and funding_bps * premium_pct < 0:
        return "Funding / Premium 背离"
    if next_funding_bps is not None and funding_bps is not None and next_funding_bps * funding_bps < 0:
        return "费率方向切换"
    if basis_pct is not None:
        if basis_pct >= 0.08:
            return "基差偏正 / 观察空永续"
        if basis_pct <= -0.08:
            return "基差偏负 / 观察多永续"
    return "中性 / 等待扩张"


def _classify_funding_regime(funding_bps: float | None, premium_pct: float | None, basis_pct: float | None) -> str:
    if funding_bps is not None and premium_pct is not None:
        if funding_bps >= 0.8 and premium_pct >= 0.03:
            return "正溢价 / 多头付费"
        if funding_bps <= -0.8 and premium_pct <= -0.03:
            return "负溢价 / 空头付费"
        if funding_bps * premium_pct < 0:
            return "Funding / Premium 背离"
    if basis_pct is not None:
        if basis_pct >= 0.08:
            return "Carry 偏多"
        if basis_pct <= -0.08:
            return "Carry 偏空"
    return "中性 / 等待扩张"


def build_funding_regime_frame(
    snapshots_by_key: Dict[str, ExchangeSnapshot],
    spot_snapshots: Dict[str, SpotSnapshot],
    exchange_title_map: Dict[str, str],
    *,
    exchange_keys: List[str] | None = None,
) -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []
    for exchange_key in exchange_keys or list(snapshots_by_key):
        snapshot = snapshots_by_key.get(exchange_key)
        if snapshot is None or snapshot.status != "ok":
            continue
        spot_snapshot = spot_snapshots.get(exchange_key)
        basis_pct = None
        if spot_snapshot is not None and spot_snapshot.status == "ok" and spot_snapshot.last_price not in (None, 0) and snapshot.last_price is not None:
            basis_pct = (snapshot.last_price - spot_snapshot.last_price) / spot_snapshot.last_price * 100.0
        funding_bps = snapshot.funding_bps
        premium_pct = snapshot.premium_pct
        next_funding_bps = _extract_next_funding_rate_bps(snapshot)
        basis_rate_pct = _extract_basis_rate_pct(snapshot, basis_pct)
        next_funding_time_ms = _extract_next_funding_time_ms(snapshot)
        minutes_to_funding = None
        if next_funding_time_ms is not None and snapshot.timestamp_ms is not None:
            minutes_to_funding = max(0.0, (float(next_funding_time_ms) - float(snapshot.timestamp_ms)) / 60000.0)
        rows.append(
            {
                "交易所": exchange_title_map.get(exchange_key, exchange_key.title()),
                "价格": snapshot.last_price,
                "Premium(%)": premium_pct,
                "Basis(%)": basis_pct,
                "基差率(%)": basis_rate_pct,
                "Funding(bps)": funding_bps,
                "下一次Funding(bps)": next_funding_bps,
                "Funding带宽(bps)": _extract_funding_band_bps(snapshot),
                "状态": _classify_funding_regime(funding_bps, premium_pct, basis_pct),
                "Carry状态": _classify_carry_state(funding_bps, premium_pct, basis_pct, next_funding_bps),
                "时间": snapshot.timestamp_ms,
                "下次Funding时间": next_funding_time_ms,
                "距下次Funding(min)": minutes_to_funding,
            }
        )
    frame = pd.DataFrame(rows)
    if frame.empty:
        return pd.DataFrame(
            columns=["交易所", "价格", "Premium(%)", "Basis(%)", "基差率(%)", "Funding(bps)", "下一次Funding(bps)", "Funding带宽(bps)", "状态", "Carry状态", "时间", "下次Funding时间", "距下次Funding(min)"]
        )
    frame["_priority"] = (
        pd.to_numeric(frame["Funding(bps)"], errors="coerce").abs().fillna(0.0)
        + pd.to_numeric(frame["Premium(%)"], errors="coerce").abs().fillna(0.0) * 20.0
        + pd.to_numeric(frame["Basis(%)"], errors="coerce").abs().fillna(0.0) * 14.0
    )
    return frame.sort_values(["_priority", "交易所"], ascending=[False, True]).drop(columns="_priority").reset_index(drop=True)


def build_funding_regime_figure(frame: pd.DataFrame) -> go.Figure:
    figure = go.Figure()
    if frame.empty:
        figure.add_annotation(text="等待 Funding / Premium 状态机样本", showarrow=False, x=0.5, y=0.5, xref="paper", yref="paper")
        figure.update_layout(height=320, margin=dict(l=12, r=12, t=24, b=12))
        return figure
    next_funding_values = pd.to_numeric(frame["下一次Funding(bps)"], errors="coerce").fillna(0.0)
    figure.add_trace(
        go.Scatter(
            x=frame["Premium(%)"],
            y=frame["Funding(bps)"],
            mode="markers+text",
            text=frame["交易所"],
            textposition="top center",
            marker=dict(
                size=[14.0 for _ in range(len(frame))],
                color=pd.to_numeric(frame["Basis(%)"], errors="coerce").fillna(0.0),
                colorscale="RdBu",
                cmin=-0.25,
                cmax=0.25,
                colorbar=dict(title="Basis(%)"),
                line=dict(width=1, color="rgba(8, 16, 26, 0.85)"),
            ),
            customdata=[[state, value] for state, value in zip(frame["状态"], next_funding_values)],
            hovertemplate="交易所 %{text}<br>Premium %{x:.3f}%<br>Funding %{y:.2f} bps<br>%{customdata[0]}<br>下一次 Funding %{customdata[1]:.2f} bps<extra></extra>",
        )
    )
    figure.add_hline(y=0.0, line_color="rgba(223, 232, 241, 0.22)", line_width=1, line_dash="dot")
    figure.add_vline(x=0.0, line_color="rgba(223, 232, 241, 0.22)", line_width=1, line_dash="dot")
    figure.update_layout(
        height=340,
        margin=dict(l=12, r=12, t=58, b=12),
        paper_bgcolor="rgba(14, 22, 35, 0.56)",
        plot_bgcolor="rgba(255, 255, 255, 0.045)",
        font=dict(color="#f6f9ff", family="SF Pro Display, Segoe UI, sans-serif"),
        title=dict(text="Funding / Premium Regime", x=0.03, y=0.98, xanchor="left", font=dict(size=18, color="#f7fbff")),
    )
    figure.update_xaxes(showgrid=True, gridcolor="rgba(255, 255, 255, 0.08)", title="Premium (%)")
    figure.update_yaxes(showgrid=True, gridcolor="rgba(255, 255, 255, 0.08)", title="Funding (bps)")
    return figure


def build_liquidation_truth_inference_frame(
    events_by_exchange: Dict[str, List[LiquidationEvent]],
    inferred_heatmap_frame: pd.DataFrame,
    *,
    now_ms: int | None = None,
    window_minutes: int = 60,
) -> pd.DataFrame:
    def liquidation_coverage_profile(exchange_name: str) -> Dict[str, str]:
        normalized = str(exchange_name or "").lower()
        if "bybit" in normalized:
            return {"coverage": "高", "tape": "WS全量", "note": "allLiquidation / 500ms"}
        if "binance" in normalized:
            return {"coverage": "中", "tape": "WS抽样", "note": "每symbol 1000ms 最新强平"}
        if "okx" in normalized:
            return {"coverage": "中低", "tape": "WS抽样", "note": "每合约每秒最多一笔"}
        if "hyperliquid" in normalized:
            return {"coverage": "低", "tape": "公开真值缺失", "note": "当前更适合地址/热力推断"}
        return {"coverage": "未知", "tape": "未知", "note": "-"}

    rows: List[Dict[str, Any]] = []
    inferred_heatmap_frame = inferred_heatmap_frame if not inferred_heatmap_frame.empty else pd.DataFrame(columns=["交易所", "净名义金额", "总名义金额"])
    all_exchanges = sorted(set(events_by_exchange) | set(inferred_heatmap_frame.get("交易所", pd.Series(dtype=str)).astype(str).tolist()))
    for exchange_name in all_exchanges:
        truth_metrics = build_liquidation_metrics(events_by_exchange.get(exchange_name, []), now_ms=now_ms, window_minutes=window_minutes)
        exchange_frame = inferred_heatmap_frame[inferred_heatmap_frame["交易所"] == exchange_name]
        inferred_total = float(exchange_frame["总名义金额"].sum()) if not exchange_frame.empty else 0.0
        positive_bands = int((pd.to_numeric(exchange_frame.get("净名义金额"), errors="coerce") > 0).sum()) if not exchange_frame.empty else 0
        negative_bands = int((pd.to_numeric(exchange_frame.get("净名义金额"), errors="coerce") < 0).sum()) if not exchange_frame.empty else 0
        inferred_bias = "空头清算" if positive_bands > negative_bands else "多头清算" if negative_bands > positive_bands else "均衡"
        count_value = int(truth_metrics.get("count") or 0)
        coverage_profile = liquidation_coverage_profile(exchange_name)
        coverage_value = str(coverage_profile.get("coverage") or "未知")
        confidence_value = "高" if count_value >= 4 and coverage_value == "高" else "中" if count_value >= 1 and coverage_value in {"高", "中", "中低"} else "低"
        rows.append(
            {
                "交易所": exchange_name,
                "真实清算额": truth_metrics.get("notional"),
                "真实事件数": count_value,
                "真实主导": truth_metrics.get("dominant"),
                "推断热力额": inferred_total,
                "推断价带数": len(exchange_frame),
                "推断主导": inferred_bias,
                "覆盖等级": coverage_value,
                "Tape口径": coverage_profile.get("tape"),
                "采样说明": coverage_profile.get("note"),
                "真值置信度": confidence_value,
            }
        )
    frame = pd.DataFrame(rows)
    if frame.empty:
        return pd.DataFrame(columns=["交易所", "真实清算额", "真实事件数", "真实主导", "推断热力额", "推断价带数", "推断主导", "覆盖等级", "Tape口径", "采样说明", "真值置信度"])
    return frame.sort_values(["真实清算额", "推断热力额"], ascending=[False, False], na_position="last").reset_index(drop=True)


def build_hyperliquid_spot_perp_context_frame(
    spot_meta_payload: Dict[str, Any],
    perp_snapshot: ExchangeSnapshot | None,
    *,
    selected_coin: str,
    address_bundle: Dict[str, Any] | None = None,
    oi_cap_payload: Dict[str, Any] | None = None,
) -> pd.DataFrame:
    selected_coin = str(selected_coin or "").strip().upper()
    meta = spot_meta_payload.get("meta") or {}
    contexts = spot_meta_payload.get("contexts") or []
    tokens = (meta.get("tokens") or []) if isinstance(meta, dict) else []
    universe = (meta.get("universe") or []) if isinstance(meta, dict) else []
    token_map = {
        int(item.get("index")): item
        for item in tokens if isinstance(item, dict) and item.get("index") is not None
    }
    spot_balance = None
    spot_entry_ntl = None
    if isinstance(address_bundle, dict):
        for balance_row in (address_bundle.get("spot_state") or {}).get("balances", []) or []:
            if str(balance_row.get("coin") or "").upper() == selected_coin:
                spot_balance = _coerce_float(balance_row.get("total"))
                spot_entry_ntl = _coerce_float(balance_row.get("entryNtl"))
                break
    perp_position_value = None
    if isinstance(address_bundle, dict):
        for position in address_bundle.get("positions") or []:
            if str(position.get("coin") or "").upper() == selected_coin:
                perp_position_value = _coerce_float(position.get("position_value"))
                break

    oi_cap = None
    total_oi_cap = None
    if isinstance(oi_cap_payload, dict):
        total_oi_cap = _coerce_float(oi_cap_payload.get("totalOiCap"))
        coin_cap_items = oi_cap_payload.get("coinToOiCap") or []
        for item in coin_cap_items if isinstance(coin_cap_items, list) else []:
            if isinstance(item, (list, tuple)) and len(item) >= 2 and str(item[0]).upper() == selected_coin:
                oi_cap = _coerce_float(item[1])
                break

    rows: List[Dict[str, Any]] = []
    if perp_snapshot is not None and perp_snapshot.status == "ok":
        current_oi = _coerce_float(perp_snapshot.open_interest)
        oi_cap_util_pct = None
        if current_oi is not None and oi_cap not in (None, 0):
            oi_cap_util_pct = current_oi / max(float(oi_cap or 0.0), 1e-9) * 100.0
        rows.append(
            {
                "市场": "合约",
                "标的": selected_coin,
                "价格": perp_snapshot.last_price,
                "24h成交额": perp_snapshot.volume_24h_notional,
                "Funding(bps)": perp_snapshot.funding_bps,
                "Premium(%)": perp_snapshot.premium_pct,
                "未平仓金额": perp_snapshot.open_interest_notional,
                "地址余额": None,
                "地址成本": None,
                "地址持仓值": perp_position_value,
                "OI上限占用(%)": oi_cap_util_pct,
                "OI上限": oi_cap,
                "全局OI上限": total_oi_cap,
                "时间": perp_snapshot.timestamp_ms,
            }
        )

    for index, pair in enumerate(universe if isinstance(universe, list) else []):
        if not isinstance(pair, dict):
            continue
        token_ids = pair.get("tokens") or []
        if not isinstance(token_ids, list) or len(token_ids) < 2:
            continue
        base_token = token_map.get(int(token_ids[0]), {})
        quote_token = token_map.get(int(token_ids[1]), {})
        base_name = str(base_token.get("name") or "").upper()
        quote_name = str(quote_token.get("name") or "").upper()
        if base_name != selected_coin:
            continue
        context = contexts[index] if index < len(contexts) and isinstance(contexts[index], dict) else {}
        spot_price = _coerce_float(context.get("midPx")) or _coerce_float(context.get("markPx"))
        balance_value = spot_balance * spot_price if spot_balance is not None and spot_price is not None else None
        rows.append(
            {
                "市场": "现货",
                "标的": str(pair.get("name") or f"{base_name}/{quote_name}"),
                "价格": spot_price,
                "24h成交额": _coerce_float(context.get("dayNtlVlm")),
                "Funding(bps)": None,
                "Premium(%)": None,
                "未平仓金额": None,
                "地址余额": spot_balance,
                "地址成本": spot_entry_ntl,
                "地址持仓值": balance_value,
                "OI上限占用(%)": None,
                "OI上限": None,
                "全局OI上限": None,
                "时间": None,
            }
        )

    frame = pd.DataFrame(rows)
    if frame.empty:
        return pd.DataFrame(
            columns=["市场", "标的", "价格", "24h成交额", "Funding(bps)", "Premium(%)", "未平仓金额", "地址余额", "地址成本", "地址持仓值", "OI上限占用(%)", "OI上限", "全局OI上限", "时间"]
        )
    market_order = {"合约": 0, "现货": 1}
    frame["_priority"] = frame["市场"].map(market_order).fillna(9)
    return frame.sort_values(["_priority", "24h成交额", "标的"], ascending=[True, False, True], na_position="last").drop(columns="_priority").reset_index(drop=True)


def build_hyperliquid_spot_perp_context_figure(frame: pd.DataFrame) -> go.Figure:
    figure = make_subplots(specs=[[{"secondary_y": True}]])
    if frame.empty:
        figure.add_annotation(text="等待 Hyperliquid spot / perp 一体样本", showarrow=False, x=0.5, y=0.5, xref="paper", yref="paper")
        figure.update_layout(height=320, margin=dict(l=12, r=12, t=24, b=12))
        return figure
    working = frame.copy()
    working["24h成交额"] = pd.to_numeric(working["24h成交额"], errors="coerce")
    working["价格"] = pd.to_numeric(working["价格"], errors="coerce")
    color_map = {"合约": "rgba(255, 148, 112, 0.78)", "现货": "rgba(103, 209, 255, 0.78)"}
    figure.add_trace(
        go.Bar(
            x=working["标的"],
            y=working["24h成交额"],
            marker_color=[color_map.get(str(value), "rgba(255,255,255,0.45)") for value in working["市场"]],
            text=working["市场"],
            textposition="outside",
            name="24h成交额",
            hovertemplate="%{x}<br>%{text}<br>24h成交额 %{y:,.0f}<extra></extra>",
        ),
        secondary_y=False,
    )
    figure.add_trace(
        go.Scatter(
            x=working["标的"],
            y=working["价格"],
            mode="lines+markers",
            line=dict(color="#f7fbff", width=2.1),
            marker=dict(size=8, color="#f7fbff"),
            name="价格",
            hovertemplate="%{x}<br>价格 %{y:,.4f}<extra></extra>",
        ),
        secondary_y=True,
    )
    figure.update_layout(
        height=340,
        margin=dict(l=12, r=12, t=58, b=12),
        paper_bgcolor="rgba(14, 22, 35, 0.56)",
        plot_bgcolor="rgba(255, 255, 255, 0.045)",
        font=dict(color="#f6f9ff", family="SF Pro Display, Segoe UI, sans-serif"),
        title=dict(text="Hyperliquid Spot / Perp Unified Context", x=0.03, y=0.98, xanchor="left", font=dict(size=18, color="#f7fbff")),
        legend=dict(orientation="h", yanchor="bottom", y=1.01, xanchor="right", x=1.0),
    )
    figure.update_xaxes(showgrid=False)
    figure.update_yaxes(showgrid=True, gridcolor="rgba(255, 255, 255, 0.08)", title="24h成交额", secondary_y=False)
    figure.update_yaxes(showgrid=False, title="价格", secondary_y=True)
    return figure


def build_liquidation_truth_inference_figure(frame: pd.DataFrame) -> go.Figure:
    figure = go.Figure()
    if frame.empty:
        figure.add_annotation(text="等待真实清算 / 推断热力样本", showarrow=False, x=0.5, y=0.5, xref="paper", yref="paper")
        figure.update_layout(height=320, margin=dict(l=12, r=12, t=24, b=12))
        return figure
    figure.add_trace(
        go.Bar(
            x=frame["交易所"],
            y=frame["真实清算额"],
            name="真实清算额",
            marker_color="rgba(255, 123, 123, 0.78)",
            hovertemplate="%{x}<br>真实清算额 %{y:,.0f}<extra></extra>",
        )
    )
    figure.add_trace(
        go.Bar(
            x=frame["交易所"],
            y=frame["推断热力额"],
            name="推断热力额",
            marker_color="rgba(103, 209, 255, 0.74)",
            hovertemplate="%{x}<br>推断热力额 %{y:,.0f}<extra></extra>",
        )
    )
    figure.update_layout(
        height=340,
        barmode="group",
        margin=dict(l=12, r=12, t=58, b=12),
        paper_bgcolor="rgba(14, 22, 35, 0.56)",
        plot_bgcolor="rgba(255, 255, 255, 0.045)",
        font=dict(color="#f6f9ff", family="SF Pro Display, Segoe UI, sans-serif"),
        title=dict(text="True Liquidation Tape vs Inferred Heat", x=0.03, y=0.98, xanchor="left", font=dict(size=18, color="#f7fbff")),
        legend=dict(orientation="h", yanchor="bottom", y=1.01, xanchor="right", x=1.0),
    )
    figure.update_xaxes(showgrid=False)
    figure.update_yaxes(showgrid=True, gridcolor="rgba(255, 255, 255, 0.08)", title="名义金额")
    return figure


def build_risk_buffer_frame(
    snapshots_by_key: Dict[str, ExchangeSnapshot],
    exchange_title_map: Dict[str, str],
    *,
    exchange_keys: List[str] | None = None,
    bybit_insurance_value: float | None = None,
) -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []
    for exchange_key in exchange_keys or list(snapshots_by_key):
        snapshot = snapshots_by_key.get(exchange_key)
        if snapshot is None or snapshot.status != "ok":
            continue
        oi_notional = float(snapshot.open_interest_notional or 0.0)
        volume_notional = float(snapshot.volume_24h_notional or 0.0)
        oi_turnover_ratio = oi_notional / volume_notional if volume_notional > 0 else None
        raw = snapshot.raw or {}
        impact_width_bps = None
        if exchange_key == "hyperliquid":
            impact_prices = (raw.get("asset_context") or {}).get("impactPxs") if isinstance(raw, dict) else None
            if isinstance(impact_prices, list) and len(impact_prices) >= 2 and snapshot.last_price not in (None, 0):
                bid_px = _coerce_float(impact_prices[0])
                ask_px = _coerce_float(impact_prices[1])
                if bid_px is not None and ask_px is not None:
                    impact_width_bps = (ask_px - bid_px) / max(float(snapshot.last_price or 0.0), 1e-9) * 10000.0
        funding_band_bps = _extract_funding_band_bps(snapshot)
        risk_score = 0.0
        if oi_turnover_ratio is not None:
            risk_score += min(1.5, oi_turnover_ratio * 2.2)
        if snapshot.funding_bps is not None:
            risk_score += min(1.2, abs(float(snapshot.funding_bps)) / 1.4)
        if snapshot.premium_pct is not None:
            risk_score += min(1.1, abs(float(snapshot.premium_pct)) / 0.06)
        if impact_width_bps is not None:
            risk_score += min(0.8, abs(float(impact_width_bps)) / 2.0)
        risk_label = "正常"
        if risk_score >= 2.8:
            risk_label = "高压"
        elif risk_score >= 1.8:
            risk_label = "偏紧"
        rows.append(
            {
                "交易所": exchange_title_map.get(exchange_key, exchange_key.title()),
                "OI/24h成交比": oi_turnover_ratio,
                "Premium(%)": snapshot.premium_pct,
                "Funding(bps)": snapshot.funding_bps,
                "Funding带宽(bps)": funding_band_bps,
                "下次Funding时间": _extract_next_funding_time_ms(snapshot),
                "保险池(USD)": bybit_insurance_value if exchange_key == "bybit" else None,
                "冲击价差(bps)": impact_width_bps,
                "风险标签": risk_label,
            }
        )
    frame = pd.DataFrame(rows)
    if frame.empty:
        return pd.DataFrame(columns=["交易所", "OI/24h成交比", "Premium(%)", "Funding(bps)", "Funding带宽(bps)", "下次Funding时间", "保险池(USD)", "冲击价差(bps)", "风险标签"])
    return frame.sort_values(["OI/24h成交比", "交易所"], ascending=[False, True], na_position="last").reset_index(drop=True)


def build_risk_buffer_figure(frame: pd.DataFrame) -> go.Figure:
    figure = go.Figure()
    if frame.empty:
        figure.add_annotation(text="等待风险缓冲层样本", showarrow=False, x=0.5, y=0.5, xref="paper", yref="paper")
        figure.update_layout(height=320, margin=dict(l=12, r=12, t=24, b=12))
        return figure
    metric_specs = [
        ("OI/24h成交比", lambda value: max(-1.0, min(1.0, float(value) / 1.0)) if pd.notna(value) else None, lambda value: f"{float(value):.2f}x" if pd.notna(value) else "-"),
        ("Premium(%)", lambda value: max(-1.0, min(1.0, float(value) / 0.15)) if pd.notna(value) else None, lambda value: f"{float(value):+.3f}%" if pd.notna(value) else "-"),
        ("Funding(bps)", lambda value: max(-1.0, min(1.0, float(value) / 3.0)) if pd.notna(value) else None, lambda value: f"{float(value):+.2f}" if pd.notna(value) else "-"),
        ("冲击价差(bps)", lambda value: max(-1.0, min(1.0, float(value) / 2.0)) if pd.notna(value) else None, lambda value: f"{float(value):.2f}" if pd.notna(value) else "-"),
    ]
    z_rows: List[List[float | None]] = []
    text_rows: List[List[str]] = []
    for _, row in frame.iterrows():
        z_row: List[float | None] = []
        text_row: List[str] = []
        for column_name, score_builder, text_builder in metric_specs:
            value = pd.to_numeric(row.get(column_name), errors="coerce")
            z_row.append(score_builder(value))
            text_row.append(text_builder(value))
        z_rows.append(z_row)
        text_rows.append(text_row)
    figure.add_trace(
        go.Heatmap(
            x=[item[0] for item in metric_specs],
            y=frame["交易所"],
            z=z_rows,
            text=text_rows,
            texttemplate="%{text}",
            zmin=-1.0,
            zmax=1.0,
            zmid=0.0,
            colorscale=[[0.0, "#4bc07a"], [0.5, "#eef3fb"], [1.0, "#d14d57"]],
            xgap=6,
            ygap=6,
            showscale=False,
            hovertemplate="交易所 %{y}<br>指标 %{x}<br>数值 %{text}<extra></extra>",
        )
    )
    figure.update_layout(
        height=max(320, 78 * len(frame)),
        margin=dict(l=12, r=12, t=58, b=12),
        paper_bgcolor="rgba(14, 22, 35, 0.56)",
        plot_bgcolor="rgba(255, 255, 255, 0.045)",
        font=dict(color="#f6f9ff", family="SF Pro Display, Segoe UI, sans-serif"),
        title=dict(text="Risk Buffer Layer", x=0.03, y=0.98, xanchor="left", font=dict(size=18, color="#f7fbff")),
    )
    figure.update_xaxes(showgrid=False, side="top")
    figure.update_yaxes(showgrid=False)
    return figure


def build_exchange_share_dynamics_frame(
    perp_snapshots_by_key: Dict[str, ExchangeSnapshot],
    spot_snapshots_by_key: Dict[str, SpotSnapshot],
    exchange_title_map: Dict[str, str],
    *,
    exchange_keys: List[str] | None = None,
    previous_records: List[Dict[str, Any]] | None = None,
) -> pd.DataFrame:
    exchange_keys = exchange_keys or list(perp_snapshots_by_key)
    current_rows: List[Dict[str, Any]] = []
    total_oi = sum(float(perp_snapshots_by_key.get(key).open_interest_notional or 0.0) for key in exchange_keys if perp_snapshots_by_key.get(key) is not None and perp_snapshots_by_key.get(key).status == "ok")
    total_perp_volume = sum(float(perp_snapshots_by_key.get(key).volume_24h_notional or 0.0) for key in exchange_keys if perp_snapshots_by_key.get(key) is not None and perp_snapshots_by_key.get(key).status == "ok")
    total_spot_volume = sum(float(spot_snapshots_by_key.get(key).volume_24h_notional or 0.0) for key in exchange_keys if spot_snapshots_by_key.get(key) is not None and spot_snapshots_by_key.get(key).status == "ok")
    previous_lookup = {str(item.get("交易所") or ""): item for item in (previous_records or []) if item.get("交易所")}
    for exchange_key in exchange_keys:
        perp_snapshot = perp_snapshots_by_key.get(exchange_key)
        spot_snapshot = spot_snapshots_by_key.get(exchange_key)
        exchange_name = exchange_title_map.get(exchange_key, exchange_key.title())
        oi_share = None
        perp_volume_share = None
        spot_volume_share = None
        if perp_snapshot is not None and perp_snapshot.status == "ok":
            oi_share = float(perp_snapshot.open_interest_notional or 0.0) / total_oi * 100.0 if total_oi > 0 else None
            perp_volume_share = float(perp_snapshot.volume_24h_notional or 0.0) / total_perp_volume * 100.0 if total_perp_volume > 0 else None
        if spot_snapshot is not None and spot_snapshot.status == "ok":
            spot_volume_share = float(spot_snapshot.volume_24h_notional or 0.0) / total_spot_volume * 100.0 if total_spot_volume > 0 else None
        previous_row = previous_lookup.get(exchange_name, {})
        current_rows.append(
            {
                "交易所": exchange_name,
                "OI份额(%)": oi_share,
                "合约成交份额(%)": perp_volume_share,
                "现货成交份额(%)": spot_volume_share,
                "OI份额Δ(%)": None if oi_share is None or previous_row.get("OI份额(%)") is None else oi_share - float(previous_row.get("OI份额(%)") or 0.0),
                "合约成交份额Δ(%)": None if perp_volume_share is None or previous_row.get("合约成交份额(%)") is None else perp_volume_share - float(previous_row.get("合约成交份额(%)") or 0.0),
                "现货成交份额Δ(%)": None if spot_volume_share is None or previous_row.get("现货成交份额(%)") is None else spot_volume_share - float(previous_row.get("现货成交份额(%)") or 0.0),
            }
        )
    frame = pd.DataFrame(current_rows)
    if frame.empty:
        return pd.DataFrame(columns=["交易所", "OI份额(%)", "合约成交份额(%)", "现货成交份额(%)", "OI份额Δ(%)", "合约成交份额Δ(%)", "现货成交份额Δ(%)"])
    return frame.sort_values("OI份额(%)", ascending=False, na_position="last").reset_index(drop=True)


def build_exchange_share_dynamics_figure(frame: pd.DataFrame) -> go.Figure:
    figure = go.Figure()
    if frame.empty:
        figure.add_annotation(text="等待跨所份额动态样本", showarrow=False, x=0.5, y=0.5, xref="paper", yref="paper")
        figure.update_layout(height=320, margin=dict(l=12, r=12, t=24, b=12))
        return figure
    figure.add_trace(
        go.Bar(
            x=frame["交易所"],
            y=frame["OI份额(%)"],
            name="OI份额",
            marker_color="rgba(103, 209, 255, 0.78)",
            hovertemplate="%{x}<br>OI份额 %{y:.2f}%<extra></extra>",
        )
    )
    figure.add_trace(
        go.Bar(
            x=frame["交易所"],
            y=frame["合约成交份额(%)"],
            name="合约成交份额",
            marker_color="rgba(248, 211, 94, 0.74)",
            hovertemplate="%{x}<br>合约成交份额 %{y:.2f}%<extra></extra>",
        )
    )
    figure.add_trace(
        go.Bar(
            x=frame["交易所"],
            y=frame["现货成交份额(%)"],
            name="现货成交份额",
            marker_color="rgba(111, 221, 140, 0.74)",
            hovertemplate="%{x}<br>现货成交份额 %{y:.2f}%<extra></extra>",
        )
    )
    figure.update_layout(
        height=340,
        barmode="group",
        margin=dict(l=12, r=12, t=58, b=12),
        paper_bgcolor="rgba(14, 22, 35, 0.56)",
        plot_bgcolor="rgba(255, 255, 255, 0.045)",
        font=dict(color="#f6f9ff", family="SF Pro Display, Segoe UI, sans-serif"),
        title=dict(text="Exchange Share Dynamics", x=0.03, y=0.98, xanchor="left", font=dict(size=18, color="#f7fbff")),
        legend=dict(orientation="h", yanchor="bottom", y=1.01, xanchor="right", x=1.0),
    )
    figure.update_xaxes(showgrid=False)
    figure.update_yaxes(showgrid=True, gridcolor="rgba(255, 255, 255, 0.08)", title="份额 (%)")
    return figure


def _multi_coin_signal_color(label: Any, score: Any = None) -> str:
    text = str(label or "").strip()
    score_value = _coerce_float(score)
    if "偏多" in text or "现货领先" in text or "空头回补" in text:
        return "#2dd4bf"
    if "偏空" in text or "流动性塌陷" in text or "多头减仓" in text:
        return "#ff6b6b"
    if score_value is not None:
        if score_value >= 6.0:
            return "#34d399"
        if score_value <= -6.0:
            return "#fb7185"
    return "#67d1ff"


def _compact_axis_text(value: Any) -> str:
    numeric = _coerce_float(value)
    if numeric is None:
        return "-"
    absolute = abs(numeric)
    if absolute >= 1_000_000_000:
        return f"{numeric / 1_000_000_000:.2f}B"
    if absolute >= 1_000_000:
        return f"{numeric / 1_000_000:.2f}M"
    if absolute >= 1_000:
        return f"{numeric / 1_000:.2f}K"
    return f"{numeric:.2f}"


def build_multi_coin_oi_ranking_figure(frame: pd.DataFrame, *, limit: int = 18) -> go.Figure:
    figure = go.Figure()
    if frame.empty:
        figure.add_annotation(text="等待多币种 OI 排行样本", showarrow=False, x=0.5, y=0.5, xref="paper", yref="paper")
        figure.update_layout(height=340, margin=dict(l=12, r=12, t=24, b=12))
        return figure
    working = canonicalize_market_frame_columns(frame.copy())
    if "OI总额" not in working.columns or "币种" not in working.columns:
        figure.add_annotation(text="等待多币种 OI 排行样本", showarrow=False, x=0.5, y=0.5, xref="paper", yref="paper")
        figure.update_layout(height=340, margin=dict(l=12, r=12, t=24, b=12))
        return figure
    working["OI总额"] = pd.to_numeric(working["OI总额"], errors="coerce")
    working = working.dropna(subset=["OI总额"]).sort_values("OI总额", ascending=False).head(limit)
    if working.empty:
        figure.add_annotation(text="等待多币种 OI 排行样本", showarrow=False, x=0.5, y=0.5, xref="paper", yref="paper")
        figure.update_layout(height=340, margin=dict(l=12, r=12, t=24, b=12))
        return figure
    colors = [_multi_coin_signal_color(row.get("信号"), row.get("情绪评分")) for _, row in working.iterrows()]
    working["_selection_exchange"] = working["主交易所键"] if "主交易所键" in working.columns else ""
    if "信号" not in working.columns:
        working["信号"] = ""
    customdata = working[["币种", "_selection_exchange", "信号"]].to_numpy()
    figure.add_trace(
        go.Bar(
            x=working["OI总额"],
            y=working["币种"],
            orientation="h",
            marker_color=colors,
            text=[_compact_axis_text(value) for value in working["OI总额"]],
            textposition="outside",
            hovertemplate="%{y}<br>OI总额 %{x:,.0f}<br>信号 %{customdata[2]}<extra></extra>" if customdata is not None else "%{y}<br>OI总额 %{x:,.0f}<extra></extra>",
            customdata=customdata,
        )
    )
    figure.update_layout(
        height=max(360, 28 * len(working) + 110),
        margin=dict(l=16, r=22, t=58, b=12),
        paper_bgcolor="rgba(14, 22, 35, 0.56)",
        plot_bgcolor="rgba(255, 255, 255, 0.045)",
        font=dict(color="#f6f9ff", family="SF Pro Display, Segoe UI, sans-serif"),
        title=dict(text="持仓量排行 (OI Ranking) · 颜色 = 合成信号", x=0.03, y=0.98, xanchor="left", font=dict(size=18, color="#f7fbff")),
        transition=dict(duration=280, easing="cubic-in-out"),
    )
    figure.update_xaxes(showgrid=True, gridcolor="rgba(255, 255, 255, 0.08)", title="OI 总额")
    figure.update_yaxes(showgrid=False, autorange="reversed")
    return figure


def build_multi_coin_oi_quadrant_bubble_figure(
    frame: pd.DataFrame,
    *,
    limit: int = 20,
    price_column: str = "24h%",
    oi_column: str = "OI 1h(%)",
    price_label: Optional[str] = None,
    oi_label: Optional[str] = None,
) -> go.Figure:
    figure = go.Figure()
    resolved_price_label = str(price_label or price_column or "价格变化")
    resolved_oi_label = str(oi_label or oi_column or "OI变化")
    if frame.empty:
        figure.add_annotation(text="等待 OI 变化 vs 价格变化样本", showarrow=False, x=0.5, y=0.5, xref="paper", yref="paper")
        figure.update_layout(height=360, margin=dict(l=12, r=12, t=24, b=12))
        return figure
    working = canonicalize_market_frame_columns(frame.copy())
    required_columns = {"币种", price_column, oi_column, "OI总额"}
    if not required_columns.issubset(set(working.columns)):
        figure.add_annotation(text="等待 OI 变化 vs 价格变化样本", showarrow=False, x=0.5, y=0.5, xref="paper", yref="paper")
        figure.update_layout(height=360, margin=dict(l=12, r=12, t=24, b=12))
        return figure
    working[price_column] = pd.to_numeric(working[price_column], errors="coerce")
    working[oi_column] = pd.to_numeric(working[oi_column], errors="coerce")
    working["OI总额"] = pd.to_numeric(working["OI总额"], errors="coerce")
    working = working.dropna(subset=[price_column, oi_column, "OI总额"]).sort_values("OI总额", ascending=False).head(limit)
    if working.empty:
        figure.add_annotation(text="等待 OI 变化 vs 价格变化样本", showarrow=False, x=0.5, y=0.5, xref="paper", yref="paper")
        figure.update_layout(height=360, margin=dict(l=12, r=12, t=24, b=12))
        return figure
    size_series = working["OI总额"].clip(lower=0)
    size_max = float(size_series.max() or 1.0)
    marker_sizes = [18.0 + 34.0 * math.sqrt(float(value) / max(size_max, 1.0)) for value in size_series]
    colors = [_multi_coin_signal_color(row.get("信号"), row.get("情绪评分")) for _, row in working.iterrows()]
    working["_selection_exchange"] = working["主交易所键"] if "主交易所键" in working.columns else ""
    if "信号" not in working.columns:
        working["信号"] = ""
    if "Lead/Lag" not in working.columns:
        working["Lead/Lag"] = ""
    figure.add_hline(y=0.0, line_width=1, line_dash="dot", line_color="rgba(223, 232, 241, 0.28)")
    figure.add_vline(x=0.0, line_width=1, line_dash="dot", line_color="rgba(223, 232, 241, 0.28)")
    figure.add_trace(
        go.Scatter(
            x=working[price_column],
            y=working[oi_column],
            mode="markers+text",
            text=working["币种"],
            textposition="top center",
            marker=dict(size=marker_sizes, color=colors, line=dict(width=1.1, color="rgba(8, 16, 26, 0.9)"), opacity=0.88),
            customdata=working[["币种", "_selection_exchange", "OI总额", "信号", "Lead/Lag"]].to_numpy(),
            hovertemplate=(
                "币种 %{text}<br>"
                + f"{resolved_price_label} %{{x:.2f}}%<br>"
                + f"{resolved_oi_label} %{{y:.2f}}%<br>"
                + "OI总额 %{customdata[2]:,.0f}<br>信号 %{customdata[3]}<br>Lead/Lag %{customdata[4]}<extra></extra>"
            ),
            showlegend=False,
        )
    )
    figure.add_annotation(text="空头加仓", xref="paper", yref="paper", x=0.18, y=0.95, showarrow=False, font=dict(color="#ff7b7b", size=12))
    figure.add_annotation(text="多头加仓", xref="paper", yref="paper", x=0.87, y=0.95, showarrow=False, font=dict(color="#4bc07a", size=12))
    figure.add_annotation(text="多头减仓", xref="paper", yref="paper", x=0.18, y=0.08, showarrow=False, font=dict(color="#f8d35e", size=12))
    figure.add_annotation(text="空头回补", xref="paper", yref="paper", x=0.87, y=0.08, showarrow=False, font=dict(color="#67d1ff", size=12))
    figure.update_layout(
        height=380,
        margin=dict(l=16, r=16, t=58, b=16),
        paper_bgcolor="rgba(14, 22, 35, 0.56)",
        plot_bgcolor="rgba(255, 255, 255, 0.045)",
        font=dict(color="#f6f9ff", family="SF Pro Display, Segoe UI, sans-serif"),
        title=dict(text=f"{resolved_oi_label} vs {resolved_price_label} 气泡图 · 四象限分析", x=0.03, y=0.98, xanchor="left", font=dict(size=18, color="#f7fbff")),
    )
    figure.update_xaxes(showgrid=True, gridcolor="rgba(255, 255, 255, 0.08)", title=f"{resolved_price_label} (%)")
    figure.update_yaxes(showgrid=True, gridcolor="rgba(255, 255, 255, 0.08)", title=f"{resolved_oi_label} (%)")
    return figure


def build_multi_coin_funding_heatmap_figure(frame: pd.DataFrame, *, limit: int = 18) -> go.Figure:
    figure = go.Figure()
    if frame.empty or "Funding(bp)" not in frame.columns:
        figure.add_annotation(text="等待 Funding 热力样本", showarrow=False, x=0.5, y=0.5, xref="paper", yref="paper")
        figure.update_layout(height=340, margin=dict(l=12, r=12, t=24, b=12))
        return figure
    working = frame.copy()
    working["Funding(bp)"] = pd.to_numeric(working["Funding(bp)"], errors="coerce")
    working = working.dropna(subset=["Funding(bp)"])
    working = working.reindex(working["Funding(bp)"].abs().sort_values(ascending=False).index).head(limit)
    if working.empty:
        figure.add_annotation(text="等待 Funding 热力样本", showarrow=False, x=0.5, y=0.5, xref="paper", yref="paper")
        figure.update_layout(height=340, margin=dict(l=12, r=12, t=24, b=12))
        return figure
    colors = ["#ef4444" if float(value) >= 0 else "#2dd4bf" for value in working["Funding(bp)"]]
    working["_selection_exchange"] = working["主交易所键"] if "主交易所键" in working.columns else ""
    if "L/S比" not in working.columns:
        working["L/S比"] = ""
    customdata = working[["币种", "_selection_exchange", "L/S比"]].to_numpy()
    figure.add_trace(
        go.Bar(
            x=working["Funding(bp)"],
            y=working["币种"],
            orientation="h",
            marker_color=colors,
            text=[f"{float(value):+.2f}bps" for value in working["Funding(bp)"]],
            textposition="outside",
            hovertemplate="%{y}<br>Funding %{x:.2f} bps<br>L/S %{customdata[2]}<extra></extra>",
            customdata=customdata,
        )
    )
    figure.add_vline(x=0.0, line_width=1, line_dash="dot", line_color="rgba(223, 232, 241, 0.28)")
    figure.update_layout(
        height=max(360, 28 * len(working) + 110),
        margin=dict(l=16, r=22, t=58, b=12),
        paper_bgcolor="rgba(14, 22, 35, 0.56)",
        plot_bgcolor="rgba(255, 255, 255, 0.045)",
        font=dict(color="#f6f9ff", family="SF Pro Display, Segoe UI, sans-serif"),
        title=dict(text="资金费率热力图 (Funding Rate Heatmap)", x=0.03, y=0.98, xanchor="left", font=dict(size=18, color="#f7fbff")),
    )
    figure.update_xaxes(showgrid=True, gridcolor="rgba(255, 255, 255, 0.08)", title="Funding (bps)")
    figure.update_yaxes(showgrid=False, autorange="reversed")
    return figure


def build_multi_timeframe_price_heatmap_figure(frame: pd.DataFrame) -> go.Figure:
    figure = go.Figure()
    required_columns = ["币种", "5m%", "30m%", "1h%", "4h%"]
    if frame.empty or any(column not in frame.columns for column in required_columns):
        figure.add_annotation(text="等待多周期价格切片样本", showarrow=False, x=0.5, y=0.5, xref="paper", yref="paper")
        figure.update_layout(height=360, margin=dict(l=12, r=12, t=24, b=12))
        return figure
    working = frame.copy()
    for column in required_columns[1:]:
        working[column] = pd.to_numeric(working[column], errors="coerce")
    working = working.dropna(subset=required_columns[1:], how="all").reset_index(drop=True)
    if working.empty:
        figure.add_annotation(text="等待多周期价格切片样本", showarrow=False, x=0.5, y=0.5, xref="paper", yref="paper")
        figure.update_layout(height=360, margin=dict(l=12, r=12, t=24, b=12))
        return figure
    working["_selection_exchange"] = working["主交易所键"] if "主交易所键" in working.columns else ""
    matrix_columns = required_columns[1:]
    z_values = working[matrix_columns].fillna(0.0).to_numpy()
    max_abs = float(pd.DataFrame(z_values).abs().max().max() or 1.0)
    max_abs = max(1.0, min(max_abs, 12.0))
    customdata = []
    for _, row in working.iterrows():
        selection_exchange = row.get("_selection_exchange") or ""
        customdata.append(
            [
                {"coin": row.get("币种"), "exchange": selection_exchange, "window": column, "value": row.get(column)}
                for column in matrix_columns
            ]
        )
    figure.add_trace(
        go.Heatmap(
            z=z_values,
            x=matrix_columns,
            y=working["币种"],
            customdata=customdata,
            colorscale=[
                [0.0, "#0f766e"],
                [0.45, "#123b5a"],
                [0.5, "#152238"],
                [0.55, "#5a2d38"],
                [1.0, "#dc2626"],
            ],
            zmid=0.0,
            zmin=-max_abs,
            zmax=max_abs,
            hovertemplate="币种 %{y}<br>周期 %{x}<br>涨跌幅 %{z:.2f}%<extra></extra>",
            text=[[f"{float(value):+.2f}%" for value in row] for row in z_values],
            texttemplate="%{text}",
            textfont={"color": "#f8fbff", "size": 12},
            showscale=True,
            colorbar=dict(title="Price %"),
        )
    )
    figure.update_layout(
        height=max(360, 30 * len(working) + 120),
        margin=dict(l=16, r=16, t=58, b=16),
        paper_bgcolor="rgba(14, 22, 35, 0.56)",
        plot_bgcolor="rgba(255, 255, 255, 0.045)",
        font=dict(color="#f6f9ff", family="SF Pro Display, Segoe UI, sans-serif"),
        title=dict(text="Price Action Matrix · 5m / 30m / 1h / 4h", x=0.03, y=0.98, xanchor="left", font=dict(size=18, color="#f7fbff")),
    )
    figure.update_xaxes(showgrid=False, side="top")
    figure.update_yaxes(showgrid=False, autorange="reversed")
    return figure


def build_spot_perp_spread_history_figure(frame: pd.DataFrame) -> go.Figure:
    figure = go.Figure()
    if frame.empty:
        figure.add_annotation(text="等待现货 / 合约价差历史样本", showarrow=False, x=0.5, y=0.5, xref="paper", yref="paper")
        figure.update_layout(height=340, margin=dict(l=12, r=12, t=24, b=12))
        return figure
    working = frame.copy()
    working["时间"] = pd.to_datetime(working["时间"], unit="ms", errors="coerce")
    working["Spread(bps)"] = pd.to_numeric(working["Spread(bps)"], errors="coerce")
    working = working.dropna(subset=["时间", "Spread(bps)"])
    if working.empty:
        figure.add_annotation(text="等待现货 / 合约价差历史样本", showarrow=False, x=0.5, y=0.5, xref="paper", yref="paper")
        figure.update_layout(height=340, margin=dict(l=12, r=12, t=24, b=12))
        return figure
    palette = ["#f8b400", "#3b82f6", "#2dd4bf", "#ff7b7b"]
    for index, (exchange, group) in enumerate(working.groupby("交易所", sort=False)):
        figure.add_trace(
            go.Scatter(
                x=group["时间"],
                y=group["Spread(bps)"],
                mode="lines+markers",
                name=str(exchange),
                line=dict(width=2.0, color=palette[index % len(palette)]),
                marker=dict(size=5),
                hovertemplate="%{x}<br>%{y:.2f} bps<extra>%{fullData.name}</extra>",
            )
        )
    figure.add_hline(y=0.0, line_width=1, line_dash="dot", line_color="rgba(223, 232, 241, 0.28)")
    figure.update_layout(
        height=360,
        margin=dict(l=16, r=16, t=58, b=12),
        paper_bgcolor="rgba(14, 22, 35, 0.56)",
        plot_bgcolor="rgba(255, 255, 255, 0.045)",
        font=dict(color="#f6f9ff", family="SF Pro Display, Segoe UI, sans-serif"),
        title=dict(text="现货 · 合约实时价差 Spot-Perp Spread", x=0.03, y=0.98, xanchor="left", font=dict(size=18, color="#f7fbff")),
        legend=dict(orientation="h", yanchor="bottom", y=1.01, xanchor="right", x=1.0),
    )
    figure.update_xaxes(showgrid=False, title="时间")
    figure.update_yaxes(showgrid=True, gridcolor="rgba(255, 255, 255, 0.08)", title="Spread (bps)")
    return figure


def build_bull_bear_power_figure(exchange_frame: pd.DataFrame, aggregate_score: float | None = None) -> go.Figure:
    figure = make_subplots(
        rows=1,
        cols=3,
        specs=[[{"type": "bar"}, {"type": "bar"}, {"type": "indicator"}]],
        column_widths=[0.36, 0.32, 0.32],
        subplot_titles=("盘口力量", "CVD速率", "买卖力量"),
    )
    if exchange_frame.empty:
        figure.add_annotation(text="等待多空力量样本", showarrow=False, x=0.5, y=0.5, xref="paper", yref="paper")
        figure.update_layout(height=360, margin=dict(l=12, r=12, t=24, b=12))
        return figure
    working = exchange_frame.copy()
    working["盘口力量"] = pd.to_numeric(working["盘口力量"], errors="coerce")
    working["CVD速率(K)"] = pd.to_numeric(working["CVD速率(K)"], errors="coerce")
    working = working.fillna(0.0)
    figure.add_trace(
        go.Bar(
            x=working["交易所"],
            y=working["盘口力量"],
            marker_color=["#2dd4bf" if value >= 0 else "#ff6b6b" for value in working["盘口力量"]],
            name="盘口力量",
            hovertemplate="%{x}<br>盘口力量 %{y:.1f}<extra></extra>",
        ),
        row=1,
        col=1,
    )
    figure.add_trace(
        go.Bar(
            x=working["交易所"],
            y=working["CVD速率(K)"],
            marker_color=["#34d399" if value >= 0 else "#fb7185" for value in working["CVD速率(K)"]],
            name="CVD速率",
            hovertemplate="%{x}<br>CVD速率 %{y:.2f}K<extra></extra>",
        ),
        row=1,
        col=2,
    )
    score_value = max(-100.0, min(100.0, float(aggregate_score or 0.0)))
    figure.add_trace(
        go.Indicator(
            mode="gauge+number+delta",
            value=score_value,
            delta={"reference": 0.0, "increasing": {"color": "#2dd4bf"}, "decreasing": {"color": "#ff6b6b"}},
            number={"font": {"size": 26, "color": "#f7fbff"}},
            gauge={
                "axis": {"range": [-100, 100], "tickcolor": "#dce8f6"},
                "bar": {"color": "#2dd4bf" if score_value >= 0 else "#ff6b6b"},
                "steps": [
                    {"range": [-100, -20], "color": "rgba(209, 77, 87, 0.24)"},
                    {"range": [-20, 20], "color": "rgba(255, 255, 255, 0.08)"},
                    {"range": [20, 100], "color": "rgba(75, 192, 122, 0.24)"},
                ],
            },
            title={"text": "买卖力量"},
        ),
        row=1,
        col=3,
    )
    figure.update_layout(
        height=380,
        margin=dict(l=16, r=16, t=58, b=12),
        paper_bgcolor="rgba(14, 22, 35, 0.56)",
        plot_bgcolor="rgba(255, 255, 255, 0.045)",
        font=dict(color="#f6f9ff", family="SF Pro Display, Segoe UI, sans-serif"),
        title=dict(text="多空力量实时面板 · Bull/Bear Power", x=0.03, y=0.98, xanchor="left", font=dict(size=18, color="#f7fbff")),
        showlegend=False,
    )
    figure.update_yaxes(showgrid=True, gridcolor="rgba(255, 255, 255, 0.08)", row=1, col=1)
    figure.update_yaxes(showgrid=True, gridcolor="rgba(255, 255, 255, 0.08)", row=1, col=2)
    return figure


def _weighted_average(rows: List[tuple[float, float]]) -> float | None:
    valid_rows = [(float(value), max(float(weight), 0.0)) for value, weight in rows if value is not None and weight is not None]
    total_weight = sum(weight for _, weight in valid_rows)
    if total_weight <= 0:
        return None
    return sum(value * weight for value, weight in valid_rows) / total_weight


def build_spot_reference_frame(
    snapshot_rows: List[Dict[str, Any]],
    *,
    lead_lag_rows: List[Dict[str, Any]] | None = None,
) -> pd.DataFrame:
    lead_lag_lookup = {
        str(row.get("交易所") or "").strip(): dict(row)
        for row in (lead_lag_rows or [])
        if str(row.get("交易所") or "").strip()
    }
    rows: List[Dict[str, Any]] = []
    for item in snapshot_rows or []:
        exchange = str(item.get("exchange") or item.get("交易所") or "").strip()
        exchange_key = str(item.get("exchange_key") or "").strip().lower()
        spot_price = _coerce_float(item.get("spot_reference_price"))
        perp_price = _coerce_float(item.get("last_price"))
        if exchange_key in {"", "hyperliquid"} or spot_price in (None, 0):
            continue
        spot_volume = _coerce_float(item.get("spot_volume_24h_notional"))
        basis_pct = _coerce_float(item.get("premium_pct"))
        if basis_pct is None and perp_price not in (None, 0):
            basis_pct = (float(perp_price) - float(spot_price)) / max(float(spot_price), 1e-9) * 100.0
        spot_orderbook = item.get("spot_orderbook") or {}
        spot_imbalance = _coerce_float(item.get("spot_orderbook_imbalance_pct"))
        if spot_imbalance is None:
            spot_imbalance = _coerce_float(spot_orderbook.get("imbalance_pct"))
        lead_lag_row = lead_lag_lookup.get(exchange, {})
        leader = str(lead_lag_row.get("领先方") or lead_lag_row.get("leader") or "同步").strip() or "同步"
        lead_summary = str(lead_lag_row.get("摘要") or lead_lag_row.get("summary") or "等待更多样本").strip() or "等待更多样本"
        volume_score = clamp01((math.log10(max(float(spot_volume or 0.0), 1.0)) - 5.0) / 3.0)
        basis_penalty = clamp01(abs(float(basis_pct or 0.0)) / 0.35)
        imbalance_score = clamp01(abs(float(spot_imbalance or 0.0)) / 18.0)
        lead_bonus = 0.18 if "现货" in leader else 0.1 if "同步" in leader else 0.02
        score = max(0.0, min(100.0, (0.50 * volume_score + 0.24 * imbalance_score + 0.16 * (1.0 - basis_penalty) + lead_bonus) * 100.0))
        if abs(float(basis_pct or 0.0)) >= 0.25:
            conclusion = "合约偏离过大"
        elif "永续" in leader or "合约" in leader:
            conclusion = "更多是合约驱动"
        elif score >= 68:
            conclusion = "现货确认较强"
        elif score >= 52:
            conclusion = "现货有跟随"
        else:
            conclusion = "现货确认偏弱"
        rows.append(
            {
                "交易所": exchange,
                "现货参考价": spot_price,
                "24h现货成交额": spot_volume,
                "合约最新价": perp_price,
                "基差(%)": basis_pct,
                "现货盘口失衡(%)": spot_imbalance,
                "Lead/Lag": lead_summary,
                "现货确认分": round(score, 1),
                "现货结论": conclusion,
            }
        )
    frame = pd.DataFrame(rows)
    if frame.empty:
        return pd.DataFrame(columns=["交易所", "现货参考价", "24h现货成交额", "合约最新价", "基差(%)", "现货盘口失衡(%)", "Lead/Lag", "现货确认分", "现货结论"])
    return frame.sort_values(["现货确认分", "24h现货成交额"], ascending=[False, False], na_position="last").reset_index(drop=True)


def compute_spot_reference_metrics(frame: pd.DataFrame) -> Dict[str, Any]:
    if frame.empty:
        return {
            "anchor_price": None,
            "confirmation_score": None,
            "dispersion_pct": None,
            "best_exchange": None,
            "status": "样本不足",
            "exchange_count": 0,
        }
    working = frame.copy()
    working["现货参考价"] = pd.to_numeric(working["现货参考价"], errors="coerce")
    working["24h现货成交额"] = pd.to_numeric(working["24h现货成交额"], errors="coerce").fillna(0.0)
    working["现货确认分"] = pd.to_numeric(working["现货确认分"], errors="coerce")
    anchor_price = _weighted_average(list(zip(working["现货参考价"].dropna().tolist(), working.loc[working["现货参考价"].notna(), "24h现货成交额"].tolist())))
    confirmation_score = _weighted_average(list(zip(working["现货确认分"].dropna().tolist(), working.loc[working["现货确认分"].notna(), "24h现货成交额"].replace(0.0, 1.0).tolist())))
    valid_prices = working["现货参考价"].dropna()
    dispersion_pct = None
    if not valid_prices.empty and anchor_price not in (None, 0):
        dispersion_pct = (float(valid_prices.max()) - float(valid_prices.min())) / max(float(anchor_price), 1e-9) * 100.0
    best_exchange = str(working.iloc[0].get("交易所") or "") if not working.empty else None
    if confirmation_score is None:
        status = "样本不足"
    elif confirmation_score >= 68 and (dispersion_pct is None or dispersion_pct <= 0.18):
        status = "现货确认强"
    elif confirmation_score >= 52:
        status = "现货部分确认"
    else:
        status = "现货确认弱"
    return {
        "anchor_price": anchor_price,
        "confirmation_score": confirmation_score,
        "dispersion_pct": dispersion_pct,
        "best_exchange": best_exchange,
        "status": status,
        "exchange_count": int(len(working)),
    }


def build_perp_reference_frame(snapshot_rows: List[Dict[str, Any]]) -> pd.DataFrame:
    total_oi = sum(max(float(_coerce_float(item.get("open_interest_notional")) or 0.0), 0.0) for item in snapshot_rows or [])
    rows: List[Dict[str, Any]] = []
    for item in snapshot_rows or []:
        exchange = str(item.get("exchange") or item.get("交易所") or "").strip()
        exchange_key = str(item.get("exchange_key") or "").strip().lower()
        last_price = _coerce_float(item.get("last_price"))
        if exchange_key == "" or last_price in (None, 0):
            continue
        mark_price = _coerce_float(item.get("mark_price"))
        funding_bps = _coerce_float(item.get("funding_bps"))
        oi_notional = _coerce_float(item.get("open_interest_notional"))
        premium_pct = _coerce_float(item.get("premium_pct"))
        orderbook = item.get("orderbook") or {}
        imbalance_pct = _coerce_float(orderbook.get("imbalance_pct"))
        if imbalance_pct is None:
            imbalance_pct = _coerce_float(item.get("orderbook_imbalance_pct"))
        oi_share = (float(oi_notional or 0.0) / max(total_oi, 1e-9) * 100.0) if total_oi > 0 else None
        funding_score = clamp01(abs(float(funding_bps or 0.0)) / 6.0)
        premium_score = clamp01(abs(float(premium_pct or 0.0)) / 0.35)
        oi_score = clamp01(float(oi_share or 0.0) / 45.0)
        imbalance_score = clamp01(abs(float(imbalance_pct or 0.0)) / 20.0)
        crowding_score = max(0.0, min(100.0, (0.36 * funding_score + 0.28 * premium_score + 0.22 * oi_score + 0.14 * imbalance_score) * 100.0))
        directional_bias = (float(premium_pct or 0.0) * 0.7) + (float(funding_bps or 0.0) * 0.12) + (float(imbalance_pct or 0.0) * 0.03)
        if crowding_score >= 66 and directional_bias >= 0.2:
            state = "上行拥挤"
        elif crowding_score >= 66 and directional_bias <= -0.2:
            state = "下行拥挤"
        elif crowding_score >= 50:
            state = "杠杆偏热"
        else:
            state = "相对中性"
        rows.append(
            {
                "交易所": exchange,
                "合约最新价": last_price,
                "标记价": mark_price,
                "Funding(bp)": funding_bps,
                "OI金额": oi_notional,
                "OI占比(%)": oi_share,
                "盘口失衡(%)": imbalance_pct,
                "溢价(%)": premium_pct,
                "合约拥挤分": round(crowding_score, 1),
                "合约状态": state,
            }
        )
    frame = pd.DataFrame(rows)
    if frame.empty:
        return pd.DataFrame(columns=["交易所", "合约最新价", "标记价", "Funding(bp)", "OI金额", "OI占比(%)", "盘口失衡(%)", "溢价(%)", "合约拥挤分", "合约状态"])
    return frame.sort_values(["合约拥挤分", "OI金额"], ascending=[False, False], na_position="last").reset_index(drop=True)


def compute_perp_reference_metrics(frame: pd.DataFrame) -> Dict[str, Any]:
    if frame.empty:
        return {
            "crowding_score": None,
            "avg_funding_bps": None,
            "total_oi_notional": None,
            "dominant_state": "样本不足",
            "bias": "中性",
            "exchange_count": 0,
        }
    working = frame.copy()
    working["合约拥挤分"] = pd.to_numeric(working["合约拥挤分"], errors="coerce")
    working["Funding(bp)"] = pd.to_numeric(working["Funding(bp)"], errors="coerce")
    working["OI金额"] = pd.to_numeric(working["OI金额"], errors="coerce").fillna(0.0)
    working["溢价(%)"] = pd.to_numeric(working["溢价(%)"], errors="coerce")
    crowding_score = _weighted_average(list(zip(working["合约拥挤分"].dropna().tolist(), working.loc[working["合约拥挤分"].notna(), "OI金额"].replace(0.0, 1.0).tolist())))
    avg_funding_bps = _weighted_average(list(zip(working["Funding(bp)"].dropna().tolist(), working.loc[working["Funding(bp)"].notna(), "OI金额"].replace(0.0, 1.0).tolist())))
    total_oi_notional = float(working["OI金额"].sum()) if "OI金额" in working.columns else None
    bias_signal = _weighted_average(
        [
            (
                (float(row.get("溢价(%)") or 0.0) * 0.8) + (float(row.get("Funding(bp)") or 0.0) * 0.08),
                max(float(row.get("OI金额") or 0.0), 1.0),
            )
            for row in working.to_dict("records")
        ]
    )
    if bias_signal is None or abs(float(bias_signal)) < 0.08:
        bias = "中性"
    elif bias_signal > 0:
        bias = "偏多"
    else:
        bias = "偏空"
    dominant_state = str(working.iloc[0].get("合约状态") or "样本不足")
    return {
        "crowding_score": crowding_score,
        "avg_funding_bps": avg_funding_bps,
        "total_oi_notional": total_oi_notional,
        "dominant_state": dominant_state,
        "bias": bias,
        "exchange_count": int(len(working)),
    }


def build_market_conclusion_frame(spot_metrics: Dict[str, Any], perp_metrics: Dict[str, Any]) -> pd.DataFrame:
    spot_score = _coerce_float((spot_metrics or {}).get("confirmation_score"))
    perp_score = _coerce_float((perp_metrics or {}).get("crowding_score"))
    anchor_price = _coerce_float((spot_metrics or {}).get("anchor_price"))
    avg_funding_bps = _coerce_float((perp_metrics or {}).get("avg_funding_bps"))
    bias = str((perp_metrics or {}).get("bias") or "中性")
    spot_status = str((spot_metrics or {}).get("status") or "样本不足")
    dispersion_pct = _coerce_float((spot_metrics or {}).get("dispersion_pct"))

    if spot_score is None and perp_score is None:
        driver = "样本不足"
        verdict = "现货与合约样本都不足，先看实时流继续热起来。"
        suggestion = "等待更多现货成交、合约 Funding/OI 与盘口样本。"
        risk = "当前结论可信度低"
        confidence = 20.0
    elif (spot_score or 0.0) >= 62.0 and (perp_score or 0.0) < 58.0:
        driver = "现货主导"
        verdict = "更像现货先行推动，合约还没到明显拥挤。"
        suggestion = "优先看现货多所是否继续确认，再决定是否放大仓位。"
        risk = "合约确认不足，追高容易遇到回吐"
        confidence = 74.0
    elif (spot_score or 0.0) >= 62.0 and (perp_score or 0.0) >= 58.0:
        driver = "现货+合约共振"
        verdict = "现货确认与合约活跃同时出现，趋势延续概率更高。"
        suggestion = "顺势优先，但要盯 funding / premium 是否继续扩张。"
        risk = "共振末端容易转成 squeeze 后回落"
        confidence = 82.0
    elif (spot_score or 0.0) < 55.0 and (perp_score or 0.0) >= 62.0:
        driver = "合约主导"
        verdict = "更像杠杆资金在推，现货确认还不够。"
        suggestion = "先看现货是否补确认，不要把合约拉升直接当真突破。"
        risk = "假突破 / 挤压后回落"
        confidence = 76.0
    else:
        driver = "混合驱动"
        verdict = "现货和合约都不算特别强，更像混合或震荡结构。"
        suggestion = "优先轻仓、快进快出，等待更明确的主导方。"
        risk = "噪音较大，信号容易反复"
        confidence = 58.0

    if dispersion_pct is not None and dispersion_pct > 0.20:
        confidence = max(32.0, confidence - 14.0)
        risk = f"{risk}；多所现货锚点分歧偏大"
    if avg_funding_bps is not None and abs(avg_funding_bps) >= 5.0 and driver != "样本不足":
        risk = f"{risk}；Funding 已处在极端区"

    row = {
        "市场驱动": driver,
        "置信度": round(confidence, 1),
        "现货锚点": anchor_price,
        "现货确认分": round(float(spot_score), 1) if spot_score is not None else None,
        "现货状态": spot_status,
        "合约拥挤分": round(float(perp_score), 1) if perp_score is not None else None,
        "合约偏向": bias,
        "综合判断": verdict,
        "执行建议": suggestion,
        "风险提示": risk,
    }
    return pd.DataFrame([row])


