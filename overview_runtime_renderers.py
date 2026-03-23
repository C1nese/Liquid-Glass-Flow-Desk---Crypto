from __future__ import annotations

from typing import Any, Callable, Dict, List

import pandas as pd

from market_frame_columns import canonicalize_market_frame_columns


def build_overview_summary_content(
    overview_frame: pd.DataFrame,
    *,
    watch_group: str,
    time_window: str,
    oi_threshold_pct: float,
    updated_at_ms: int,
    data_source: str,
    filter_overview_frame_by_oi_threshold: Callable[[pd.DataFrame, float], pd.DataFrame],
    resolve_overview_quadrant_columns: Callable[[pd.DataFrame], Dict[str, str]],
    fmt_compact: Callable[[Any], str],
    panel_from_frame: Callable[..., Dict[str, Any]],
    attach_panel_meta: Callable[..., List[Dict[str, Any]]],
    watch_group_label: Callable[[Any], str],
) -> Dict[str, Any]:
    working_overview = (
        canonicalize_market_frame_columns(overview_frame.copy())
        if isinstance(overview_frame, pd.DataFrame)
        else pd.DataFrame()
    )
    for column, default in {
        "币种": "",
        "价格": None,
        "24h%": None,
        "OI总额": None,
        "OI 1h(%)": None,
        "OI变化额": None,
        "Funding(bp)": None,
        "24h爆仓样本额": None,
        "Lead/Lag": "",
        "信号": "-",
        "状态": "正常",
    }.items():
        if column not in working_overview.columns:
            working_overview[column] = default
    filtered_overview = filter_overview_frame_by_oi_threshold(working_overview, oi_threshold_pct)
    quadrant_source = filtered_overview.copy() if not filtered_overview.empty else working_overview.copy()
    quadrant_meta = resolve_overview_quadrant_columns(quadrant_source)
    price_column = str(quadrant_meta.get("price_column") or "24h%")
    oi_column = str(quadrant_meta.get("oi_column") or "OI 1h(%)")
    if quadrant_source.empty:
        quadrant_total = 0
        quadrant_counts = {"long_add": 0, "short_add": 0, "short_cover": 0, "long_reduce": 0}
        avg_oi_velocity = None
    else:
        price_series = pd.to_numeric(quadrant_source.get(price_column), errors="coerce")
        oi_series = pd.to_numeric(quadrant_source.get(oi_column), errors="coerce")
        valid_mask = price_series.notna() & oi_series.notna()
        price_series = price_series[valid_mask]
        oi_series = oi_series[valid_mask]
        quadrant_total = int(valid_mask.sum())
        quadrant_counts = {
            "long_add": int(((price_series >= 0) & (oi_series >= 0)).sum()),
            "short_add": int(((price_series < 0) & (oi_series >= 0)).sum()),
            "short_cover": int(((price_series >= 0) & (oi_series < 0)).sum()),
            "long_reduce": int(((price_series < 0) & (oi_series < 0)).sum()),
        }
        oi_rate_values = [
            abs(float(delta)) / 60.0
            for delta in pd.to_numeric(quadrant_source.get("OI变化额"), errors="coerce").dropna().tolist()
        ]
        avg_oi_velocity = float(pd.Series(oi_rate_values).mean()) if oi_rate_values else None

    oi_board = (
        filtered_overview.assign(_rank=pd.to_numeric(filtered_overview["OI 1h(%)"], errors="coerce").abs())
        .sort_values("_rank", ascending=False, na_position="last")
        .drop(columns="_rank")
        .head(8)
        .reset_index(drop=True)
        if not filtered_overview.empty and "OI 1h(%)" in filtered_overview.columns
        else pd.DataFrame()
    )
    funding_board = (
        filtered_overview.assign(_rank=pd.to_numeric(filtered_overview["Funding(bp)"], errors="coerce").abs())
        .sort_values("_rank", ascending=False, na_position="last")
        .drop(columns="_rank")
        .head(8)
        .reset_index(drop=True)
        if not filtered_overview.empty and "Funding(bp)" in filtered_overview.columns
        else pd.DataFrame()
    )
    spot_leader_board = (
        filtered_overview[filtered_overview["Lead/Lag"].astype(str).str.contains("现货领先", na=False)]
        .head(8)
        .reset_index(drop=True)
        if not filtered_overview.empty and "Lead/Lag" in filtered_overview.columns
        else pd.DataFrame()
    )
    watchlist_frame = (
        working_overview[["币种", "价格", "OI总额", "Funding(bp)", "24h爆仓样本额", "状态"]].copy()
        if not working_overview.empty
        else pd.DataFrame()
    )

    cards = [
        {"label": "多头加仓", "value": f"{quadrant_counts['long_add'] / quadrant_total * 100:.1f}%" if quadrant_total else "-", "sub": f"{quadrant_counts['long_add']} / {quadrant_total} · {price_column} × {oi_column}"},
        {"label": "空头加仓", "value": f"{quadrant_counts['short_add'] / quadrant_total * 100:.1f}%" if quadrant_total else "-", "sub": f"{quadrant_counts['short_add']} / {quadrant_total} · {price_column} × {oi_column}"},
        {"label": "空头回补", "value": f"{quadrant_counts['short_cover'] / quadrant_total * 100:.1f}%" if quadrant_total else "-", "sub": f"{quadrant_counts['short_cover']} / {quadrant_total} · {price_column} × {oi_column}"},
        {"label": "多头减仓", "value": f"{quadrant_counts['long_reduce'] / quadrant_total * 100:.1f}%" if quadrant_total else "-", "sub": f"{quadrant_counts['long_reduce']} / {quadrant_total} · {price_column} × {oi_column}"},
        {"label": "平均OI速率/min", "value": fmt_compact(avg_oi_velocity), "sub": f"按 {oi_column} 对应变化额估算"},
    ]

    def build_spot_board_frame() -> pd.DataFrame:
        output_columns = [column for column in ["币种", quadrant_price_column, quadrant_oi_column, "Lead/Lag", "信号", "__selection"] if column in filtered_overview.columns]
        if filtered_overview.empty:
            return pd.DataFrame(columns=[column for column in output_columns if column != "__selection"])
        leadlag_rows = (
            filtered_overview[filtered_overview["Lead/Lag"].astype(str).str.contains("现货领先", na=False)]
            .reset_index(drop=True)
            if "Lead/Lag" in filtered_overview.columns
            else pd.DataFrame()
        )
        if not leadlag_rows.empty:
            return leadlag_rows[output_columns].head(8)
        fallback = filtered_overview.copy()
        fallback["Lead/Lag"] = "现货热度回退"
        if "信号" not in fallback.columns:
            fallback["信号"] = "待现货领先样本补充"
        if "Spot/OI" in fallback.columns:
            fallback["_spot_rank"] = pd.to_numeric(fallback["Spot/OI"], errors="coerce").abs().fillna(0.0)
        elif "24h成交额" in fallback.columns:
            fallback["_spot_rank"] = pd.to_numeric(fallback["24h成交额"], errors="coerce").fillna(0.0)
        else:
            fallback["_spot_rank"] = pd.to_numeric(fallback.get(quadrant_price_column), errors="coerce").abs().fillna(0.0)
        return fallback.sort_values("_spot_rank", ascending=False, na_position="last").head(8).reset_index(drop=True)[output_columns]

    def build_spot_board_frame() -> pd.DataFrame:
        output_columns = [column for column in ["币种", quadrant_price_column, quadrant_oi_column, "Lead/Lag", "信号", "__selection"] if column in filtered_overview.columns]
        if filtered_overview.empty:
            return pd.DataFrame(columns=[column for column in output_columns if column != "__selection"])
        leadlag_rows = (
            filtered_overview[filtered_overview["Lead/Lag"].astype(str).str.contains("现货领先", na=False)]
            .reset_index(drop=True)
            if "Lead/Lag" in filtered_overview.columns
            else pd.DataFrame()
        )
        if not leadlag_rows.empty:
            return leadlag_rows[output_columns].head(8)
        fallback = filtered_overview.copy()
        fallback["Lead/Lag"] = "现货热度回退"
        if "信号" not in fallback.columns:
            fallback["信号"] = "待现货领先样本补充"
        if "Spot/OI" in fallback.columns:
            fallback["_spot_rank"] = pd.to_numeric(fallback["Spot/OI"], errors="coerce").abs().fillna(0.0)
        elif "24h成交额" in fallback.columns:
            fallback["_spot_rank"] = pd.to_numeric(fallback["24h成交额"], errors="coerce").fillna(0.0)
        else:
            fallback["_spot_rank"] = pd.to_numeric(fallback.get(quadrant_price_column), errors="coerce").abs().fillna(0.0)
        return fallback.sort_values("_spot_rank", ascending=False, na_position="last").head(8).reset_index(drop=True)[output_columns]

    panels = [
        panel_from_frame("overview-market-table", "全市场总览表", filtered_overview, note="价格、OI、Funding、多空比、Lead/Lag 与主结论"),
        panel_from_frame("overview-oi-board", "OI 激增榜", oi_board[["币种", "OI 1h(%)", "OI总额", "信号", "Lead/Lag"]] if not oi_board.empty else oi_board, note="按 OI 1h 绝对变化排序"),
        panel_from_frame("overview-funding-board", "Funding 极值榜", funding_board[["币种", "Funding(bp)", "24h%", "信号"]] if not funding_board.empty else funding_board, note="按 Funding 极值排序"),
        panel_from_frame("overview-spot-board", "现货带动榜", spot_leader_board[["币种", "24h%", "OI 1h(%)", "Lead/Lag", "信号"]] if not spot_leader_board.empty else spot_leader_board, note="筛出 Lead/Lag 为现货领先的币种"),
        panel_from_frame("overview-watchlist", "多币种轮巡", watchlist_frame, note=f"币组：{watch_group_label(watch_group)}"),
    ]
    panels = attach_panel_meta(panels, data_source=data_source, updated_at_ms=updated_at_ms)
    return {
        "cards": cards,
        "panels": panels,
        "filtered_overview": filtered_overview,
        "overview_frame": working_overview,
        "quadrant_meta": quadrant_meta,
    }


def build_overview_lite_panels(
    *,
    filtered_overview: pd.DataFrame,
    reference_layers: Dict[str, Any],
    summary_signature: Dict[str, Any],
    data_source: str,
    truth_level: str,
    quadrant_meta: Dict[str, Any],
    watch_group: str,
    market_scope: str,
    now_ms: int,
    watch_group_label: Callable[[Any], str],
    build_timed_frame_panel: Callable[..., Dict[str, Any]],
    build_board_frame: Callable[..., pd.DataFrame],
    build_ai_market_summary_frame: Callable[..., pd.DataFrame],
    attach_panel_meta: Callable[..., List[Dict[str, Any]]],
) -> List[Dict[str, Any]]:
    filtered_overview = (
        canonicalize_market_frame_columns(filtered_overview.copy())
        if isinstance(filtered_overview, pd.DataFrame)
        else pd.DataFrame()
    )
    quadrant_price_column = str(quadrant_meta.get("price_column") or "24h%")
    quadrant_oi_column = str(quadrant_meta.get("oi_column") or "OI 1h(%)")
    quadrant_price_label = str(quadrant_meta.get("price_label") or quadrant_price_column)
    quadrant_oi_label = str(quadrant_meta.get("oi_label") or quadrant_oi_column)
    watchlist_columns = [column for column in ["币种", "价格", "OI总额", "Funding(bp)", "24h爆仓样本额", "状态", "__selection"] if column in filtered_overview.columns]
    panels = [
        build_timed_frame_panel(
            "overview-ai-copilot",
            "AI 市场副驾驶",
            signature={**summary_signature, "panel": "ai-copilot", "rows": len(filtered_overview)},
            frame_builder=lambda: build_ai_market_summary_frame(
                reference_layers=reference_layers,
                focus_frame=filtered_overview,
                market_scope=market_scope,
            ),
            note="把现货确认、合约拥挤、市场主导和当前焦点浓缩成几条可执行结论，优先回答现在为什么值得看。",
            limit=6,
            data_source=f"{data_source} + 现货/合约参考层",
            updated_at_ms=now_ms,
            truth_level="proxy",
        ),
        build_timed_frame_panel(
            "overview-market-table",
            "全市场总览表",
            signature={**summary_signature, "panel": "market-table", "rows": len(filtered_overview)},
            frame_builder=lambda: filtered_overview,
            note=f"币组：{watch_group_label(watch_group)} · 范围：{summary_signature.get('coin_scope') or 'majors'}",
            limit=18,
            data_source=data_source,
            updated_at_ms=now_ms,
            truth_level=truth_level,
        ),
        build_timed_frame_panel(
            "overview-oi-board",
            "OI 激增榜",
            signature={**summary_signature, "panel": "oi-board", "rows": len(filtered_overview)},
            frame_builder=lambda: build_board_frame(
                filtered_overview,
                quadrant_oi_column,
                ["币种", quadrant_oi_column, "OI总额", "信号", "Lead/Lag"],
                limit=8,
            ),
            note=f"按 {quadrant_oi_label} 绝对变化排序。",
            limit=8,
            data_source=data_source,
            updated_at_ms=now_ms,
            truth_level=truth_level,
        ),
        build_timed_frame_panel(
            "overview-funding-board",
            "Funding 极值榜",
            signature={**summary_signature, "panel": "funding-board", "rows": len(filtered_overview)},
            frame_builder=lambda: build_board_frame(
                filtered_overview,
                "Funding(bp)",
                ["币种", "Funding(bp)", quadrant_price_column, "信号"],
                limit=8,
            ),
            note=f"按 Funding 极值排序，参考 {quadrant_price_label}。",
            limit=8,
            data_source=data_source,
            updated_at_ms=now_ms,
            truth_level=truth_level,
        ),
    ]
    if market_scope != "perp":
        panels.append(
            build_timed_frame_panel(
                "overview-spot-board",
                "现货带动榜",
                signature={**summary_signature, "panel": "spot-board", "rows": len(filtered_overview)},
                frame_builder=lambda: filtered_overview[
                    filtered_overview["Lead/Lag"].astype(str).str.contains("现货领先", na=False)
                ].reset_index(drop=True)[[column for column in ["币种", quadrant_price_column, quadrant_oi_column, "Lead/Lag", "信号", "__selection"] if column in filtered_overview.columns]],
                note=f"筛出 Lead/Lag 为现货领先的币种，参考 {quadrant_price_label} / {quadrant_oi_label}。",
                limit=8,
                data_source=data_source,
                updated_at_ms=now_ms,
                truth_level=truth_level,
            )
        )
    panels.append(
        build_timed_frame_panel(
            "overview-watchlist",
            "多币种轮巡",
            signature={**summary_signature, "panel": "watchlist", "rows": len(filtered_overview)},
            frame_builder=lambda: filtered_overview[watchlist_columns].copy() if watchlist_columns else pd.DataFrame(),
            note=f"当前过滤后币组：{watch_group_label(watch_group)}。",
            limit=18,
            data_source=data_source,
            updated_at_ms=now_ms,
            truth_level=truth_level,
        )
    )
    def build_spot_board_frame() -> pd.DataFrame:
        output_columns = [column for column in ["币种", quadrant_price_column, quadrant_oi_column, "Lead/Lag", "信号", "__selection"] if column in filtered_overview.columns]
        if filtered_overview.empty:
            return pd.DataFrame(columns=[column for column in output_columns if column != "__selection"])
        leadlag_rows = (
            filtered_overview[filtered_overview["Lead/Lag"].astype(str).str.contains("现货领先", na=False)]
            .reset_index(drop=True)
            if "Lead/Lag" in filtered_overview.columns
            else pd.DataFrame()
        )
        if not leadlag_rows.empty:
            return leadlag_rows[output_columns].head(8)
        fallback = filtered_overview.copy()
        fallback["Lead/Lag"] = "现货热度回退"
        if "信号" not in fallback.columns:
            fallback["信号"] = "待现货领先样本补充"
        if "Spot/OI" in fallback.columns:
            fallback["_spot_rank"] = pd.to_numeric(fallback["Spot/OI"], errors="coerce").abs().fillna(0.0)
        elif "24h成交额" in fallback.columns:
            fallback["_spot_rank"] = pd.to_numeric(fallback["24h成交额"], errors="coerce").fillna(0.0)
        else:
            fallback["_spot_rank"] = pd.to_numeric(fallback.get(quadrant_price_column), errors="coerce").abs().fillna(0.0)
        return fallback.sort_values("_spot_rank", ascending=False, na_position="last").head(8).reset_index(drop=True)[output_columns]
    if market_scope != "perp":
        patched_panels: List[Dict[str, Any]] = []
        for panel in panels:
            if str(panel.get("id") or "") == "overview-spot-board":
                patched_panels.append(
                    build_timed_frame_panel(
                        "overview-spot-board",
                        "现货带动榜",
                        signature={**summary_signature, "panel": "spot-board", "rows": len(filtered_overview), "fallback_version": "spot-heat-v1"},
                        frame_builder=build_spot_board_frame,
                        note=f"筛出 Lead/Lag 为现货领先的币种，缺样本时按现货热度回退 | 参考 {quadrant_price_label} / {quadrant_oi_label}",
                        limit=8,
                        data_source=data_source,
                        updated_at_ms=now_ms,
                        truth_level=truth_level,
                    )
                )
                continue
            patched_panels.append(panel)
        panels = patched_panels
    return attach_panel_meta(
        panels,
        data_source=data_source,
        updated_at_ms=now_ms,
    )
