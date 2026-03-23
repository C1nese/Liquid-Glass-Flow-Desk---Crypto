from __future__ import annotations

from typing import Any, Callable, Dict

import pandas as pd
import streamlit as st


def render_hyperliquid_address_panel(
    bundle: Dict[str, Any],
    *,
    address: str,
    lookback_hours: int,
    current_coin: str,
    key_scope: str,
    render_section: Callable[[str, str], None],
    build_hyperliquid_position_frame: Callable[[Dict[str, Any]], pd.DataFrame],
    build_hyperliquid_fill_frame: Callable[[Dict[str, Any]], pd.DataFrame],
    build_hyperliquid_funding_frame: Callable[[Dict[str, Any]], pd.DataFrame],
    build_hyperliquid_user_event_frame: Callable[[Dict[str, Any]], pd.DataFrame],
    build_hyperliquid_vault_equity_frame: Callable[[Dict[str, Any]], pd.DataFrame],
    build_hyperliquid_portfolio_frame: Callable[[Dict[str, Any]], pd.DataFrame],
    build_hyperliquid_vault_detail_frame: Callable[[Dict[str, Any]], pd.DataFrame],
    format_latency: Callable[[Any], str],
    fmt_compact: Callable[[Any], str],
) -> None:
    render_section(
        "Hyperliquid 地址模式",
        "输入地址后，这里会拉取该地址的永续账户摘要、持仓、近端成交和 funding 轨迹；如果开启实时地址模式，还会额外订阅 userFills / userFundings / userEvents / clearinghouseState / activeAssetData。",
    )
    if bundle.get("status") != "ok":
        st.warning(f"地址模式暂时不可用: {bundle.get('error') or '加载失败'}")
        return
    positions_frame = build_hyperliquid_position_frame(bundle)
    fills_frame = build_hyperliquid_fill_frame(bundle)
    funding_frame = build_hyperliquid_funding_frame(bundle)
    event_frame = build_hyperliquid_user_event_frame(bundle)
    vault_equity_frame = build_hyperliquid_vault_equity_frame(bundle)
    portfolio_frame = build_hyperliquid_portfolio_frame(bundle)
    vault_detail_frame = build_hyperliquid_vault_detail_frame(bundle)
    funding_net = float(funding_frame["金额"].fillna(0.0).sum()) if not funding_frame.empty and "金额" in funding_frame.columns else 0.0
    stream_status = str(bundle.get("stream_status") or "未连接")
    stream_connected = bool(bundle.get("connected"))
    stream_latency = format_latency(bundle.get("last_message_ms"))
    role_label = str(bundle.get("role") or "未知")
    address_row = st.columns(6)
    address_row[0].metric("账户权益", fmt_compact(bundle.get("account_value")))
    address_row[1].metric("可提余额", fmt_compact(bundle.get("withdrawable")))
    address_row[2].metric("保证金占用", fmt_compact(bundle.get("total_margin_used")))
    address_row[3].metric("持仓数", str(len(positions_frame)))
    address_row[4].metric(f"近 {lookback_hours}h Funding", fmt_compact(funding_net))
    address_row[5].metric("实时流", "在线" if stream_connected else stream_status)
    st.caption(
        f"地址 `{address}` | 角色 `{role_label}` | 当前关注 `{current_coin or '全部仓位'}`"
        f" | 最近成交 {len(fills_frame)} 笔 | 最近 funding 记录 {len(funding_frame)} 条 | {stream_latency}"
    )
    if bundle.get("error"):
        st.caption(f"部分字段降级: {bundle.get('error')}")
    active_asset = bundle.get("active_asset") or {}
    active_items = []
    if isinstance(active_asset, dict):
        for field_key, value in active_asset.items():
            if isinstance(value, (dict, list)):
                continue
            active_items.append({"字段": field_key, "数值": value})
    active_frame = pd.DataFrame(active_items)
    top_left, top_right = st.columns([1.45, 1.55], gap="large")
    with top_left:
        if positions_frame.empty:
            st.info("当前地址在所选币种下没有公开可见仓位。")
        else:
            st.dataframe(
                positions_frame,
                width="stretch",
                hide_index=True,
                column_config={
                    "仓位": st.column_config.NumberColumn(format="%.4f"),
                    "开仓价": st.column_config.NumberColumn(format="%.4f"),
                    "标记价": st.column_config.NumberColumn(format="%.4f"),
                    "清算价": st.column_config.NumberColumn(format="%.4f"),
                    "杠杆": st.column_config.NumberColumn(format="%.2f"),
                    "仓位价值": st.column_config.NumberColumn(format="%.2f"),
                    "未实现PnL": st.column_config.NumberColumn(format="%.2f"),
                    "ROE(%)": st.column_config.NumberColumn(format="%.2f"),
                },
            )
    with top_right:
        if active_frame.empty:
            st.info("当前币种没有额外的 activeAssetData 明细。")
        else:
            st.dataframe(active_frame, width="stretch", hide_index=True)
    middle_left, middle_right = st.columns(2, gap="large")
    with middle_left:
        if portfolio_frame.empty:
            st.info("当前没有可展示的组合窗口统计。")
        else:
            st.dataframe(
                portfolio_frame,
                width="stretch",
                hide_index=True,
                column_config={
                    "账户权益": st.column_config.NumberColumn(format="%.2f"),
                    "PnL": st.column_config.NumberColumn(format="%.2f"),
                    "成交量": st.column_config.NumberColumn(format="%.2f"),
                },
            )
    with middle_right:
        if vault_detail_frame.empty and vault_equity_frame.empty:
            st.info("当前地址没有额外的金库明细。")
        else:
            if not vault_detail_frame.empty:
                st.dataframe(vault_detail_frame, width="stretch", hide_index=True)
            if not vault_equity_frame.empty:
                st.dataframe(
                    vault_equity_frame,
                    width="stretch",
                    hide_index=True,
                    column_config={"权益": st.column_config.NumberColumn(format="%.2f")},
                )
    bottom_left, bottom_right = st.columns(2, gap="large")
    with bottom_left:
        if fills_frame.empty:
            st.info("当前窗口里还没有可展示的地址成交。")
        else:
            st.dataframe(
                fills_frame,
                width="stretch",
                hide_index=True,
                column_config={
                    "价格": st.column_config.NumberColumn(format="%.4f"),
                    "数量": st.column_config.NumberColumn(format="%.4f"),
                    "名义金额": st.column_config.NumberColumn(format="%.2f"),
                    "已实现PnL": st.column_config.NumberColumn(format="%.2f"),
                    "手续费": st.column_config.NumberColumn(format="%.4f"),
                },
            )
    with bottom_right:
        if funding_frame.empty:
            st.info("当前窗口里还没有可展示的 funding 记录。")
        else:
            st.dataframe(
                funding_frame,
                width="stretch",
                hide_index=True,
                column_config={"金额": st.column_config.NumberColumn(format="%.4f")},
            )
    if not event_frame.empty:
        st.dataframe(event_frame, width="stretch", hide_index=True)
