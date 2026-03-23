from __future__ import annotations

import base64
import ctypes
from dataclasses import asdict
from html import escape as html_escape
import math
import os
import time
import json
import threading
from ctypes import wintypes
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, List, Tuple

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import requests
import streamlit as st
import streamlit.components.v1 as components

from analytics import (
    build_basis_comparison_figure,
    build_binance_crowd_figure,
    build_binance_crowding_alerts,
    build_binance_ratio_breakdown_figure,
    build_carry_surface_figure,
    build_carry_surface_frame,
    build_cross_exchange_liquidation_frame,
    build_cross_exchange_spread_figure,
    build_cross_exchange_spread_frame,
    build_cvd_figure,
    build_composite_signal,
    build_composite_signal_figure,
    build_contract_ratio_history_figure,
    build_contract_sentiment_alert_frame,
    build_contract_sentiment_truth_figure,
    build_contract_sentiment_truth_frame,
    build_directional_heat_zone_frames,
    build_event_heatmap_figure,
    build_event_heatmap_frame,
    build_exchange_share_figure,
    build_exchange_share_dynamics_figure,
    build_exchange_share_dynamics_frame,
    build_exchange_share_frame,
    build_execution_quality_figure,
    build_execution_quality_frame,
    build_funding_arb_figure,
    build_funding_arb_frame,
    build_funding_regime_figure,
    build_funding_regime_frame,
    build_heat_zone_frame,
    build_heatmap_figure,
    build_hyperliquid_predicted_funding_figure,
    build_hyperliquid_predicted_funding_frame,
    build_hyperliquid_spot_perp_context_figure,
    build_hyperliquid_spot_perp_context_frame,
    build_alert_timeline_figure,
    build_large_trade_figure,
    build_large_trade_frame,
    build_liquidation_cluster_figure,
    build_liquidation_cluster_frame,
    build_liquidation_density_figure,
    build_liquidation_density_frame,
    build_liquidation_figure,
    build_liquidation_frame,
    build_liquidation_linkage_heatmap,
    build_liquidation_metrics,
    build_liquidation_truth_inference_figure,
    build_liquidation_truth_inference_frame,
    build_liquidation_truth_summary,
    build_liquidation_waterfall_figure,
    build_funding_comparison_figure,
    build_mbo_figure,
    build_mbo_profile_frame,
    build_microstructure_anomaly_frame,
    build_multifactor_sentiment_figure,
    build_multifactor_sentiment_frame,
    build_open_interest_comparison_figure,
    build_open_interest_frame,
    build_oi_multiframe_matrix_figure,
    build_oi_multiframe_matrix_frame,
    build_oi_quadrant_figure,
    build_oi_quadrant_metrics,
    build_orderbook_quality_figure,
    build_orderbook_quality_frame,
    build_probability_heatmap_frame,
    build_perp_crowding_trio_figure,
    build_perp_crowding_trio_frame,
    build_recorded_event_frame,
    build_replay_figure,
    build_risk_buffer_figure,
    build_risk_buffer_frame,
    build_signal_backtest_frame,
    build_spot_flow_reference_figure,
    build_spot_flow_reference_frame,
    build_spot_perp_exchange_figure,
    build_spot_perp_alert_frame,
    build_spot_perp_exchange_frame,
    build_spot_perp_figure,
    build_spot_perp_flow_figure,
    build_spot_perp_metrics,
    build_candlestick_pattern_frame,
    build_term_structure_figure,
    build_term_structure_frame,
    build_trade_frame,
    build_trade_metrics,
    build_vpin_figure,
    build_vpin_frame,
    build_wall_absorption_frame,
    compute_spot_perp_lead_lag,
    evolve_alert_engine,
    merge_liquidation_events,
    summarize_orderbook,
)
from exchanges import (
    EXCHANGE_ORDER,
    SPOT_EXCHANGE_ORDER,
    SUPPORTED_INTERVALS,
    default_spot_symbols,
    default_symbols,
    fetch_all_snapshots,
    fetch_binance_basis_curve,
    fetch_binance_trader_sentiment,
    fetch_bybit_insurance_pool,
    fetch_binance_spot_orderbook,
    fetch_binance_spot_snapshot,
    fetch_binance_spot_trades,
    fetch_bybit_trader_sentiment,
    fetch_exchange_candles,
    fetch_exchange_coin_catalog,
    fetch_exchange_liquidations,
    fetch_exchange_oi_history,
    fetch_exchange_orderbook,
    fetch_exchange_trades,
    fetch_hyperliquid_address_mode,
    fetch_hyperliquid_all_mids,
    fetch_hyperliquid_perps_at_open_interest_cap,
    fetch_hyperliquid_predicted_fundings,
    fetch_hyperliquid_spot_meta_and_asset_contexts,
    fetch_spot_orderbook,
    fetch_spot_snapshot,
    fetch_spot_trades,
    interval_to_millis,
    is_valid_onchain_address,
    describe_exchange_request_health,
)
from models import Candle, ExchangeSnapshot, LiquidationEvent, OIPoint, OrderBookLevel, SpotSnapshot, TradeEvent
from realtime import LiveTerminalService
from realtime import HyperliquidAddressStreamService
from storage import TerminalHistoryStore
from legacy_address import render_hyperliquid_address_panel as render_hyperliquid_address_panel_view
from legacy_health import build_exchange_health_frame
from legacy_history import (
    build_history_event_mix_figure,
    build_history_index_payload,
    build_history_review_payload,
    build_history_workbench_figure,
    build_history_workbench_payload,
)


POPULAR_COINS = ["BTC", "ETH", "SOL", "XRP", "BNB", "DOGE", "ADA", "SUI", "AVAX", "LINK", "LTC", "HYPE", "TAO", "PEPE", "PENDLE", "WIF", "TRUMP", "FARTCOIN"]
EXCHANGE_TITLES = {"bybit": "Bybit", "binance": "Binance", "okx": "OKX", "hyperliquid": "Hyperliquid"}
BID_PALETTE = ["#dff8ff", "#bdefff", "#92ddff", "#5fc0ff", "#279cff", "#1768d3"]
ASK_PALETTE = ["#fff1db", "#ffd9ad", "#ffbe80", "#ff9a59", "#ff6938", "#d8452d"]
CARD_STATUS = {"ok": "正常", "error": "异常"}
LIQUIDATION_COLORSCALE = [(0.0, "#081a2b"), (0.25, "#12466f"), (0.55, "#2ca7ff"), (1.0, "#ffd76b")]
TP_COLORSCALE = [(0.0, "#102016"), (0.28, "#1f5d32"), (0.6, "#57b06b"), (1.0, "#f1ff9b")]
STOP_COLORSCALE = [(0.0, "#260f14"), (0.25, "#5f1d28"), (0.58, "#d45454"), (1.0, "#ffd7b1")]
PLOTLY_CONFIG = {
    "displaylogo": False,
    "responsive": True,
    "scrollZoom": True,
}
PERFORMANCE_PROFILES = {
    "标准": {
        "history_size": 720,
        "liquidation_history_size": 600,
        "trade_history_size": 1200,
        "record_history_size": 2400,
        "orderbook_limit": 80,
    },
    "轻量": {
        "history_size": 240,
        "liquidation_history_size": 240,
        "trade_history_size": 420,
        "record_history_size": 720,
        "orderbook_limit": 60,
    },
}
LOCAL_UI_PREFERENCES_PATH = Path(__file__).with_name(".terminal_ui_preferences.json")
LOCAL_UI_SECRET_PREFIX = "dpapi:v1:"
LOCAL_UI_SECRET_FIELDS = ("telegram_bot_token", "telegram_chat_id")
LOCAL_UI_SECRET_ENV_OVERRIDES = {
    "telegram_bot_token": "LGFD_TELEGRAM_BOT_TOKEN",
    "telegram_chat_id": "LGFD_TELEGRAM_CHAT_ID",
}
LOCAL_UI_DPAPI_ENTROPY = b"LiquidGlassFlowDesk::ui-preferences"
HYPERLIQUID_ADDRESS_LOOKBACK_OPTIONS = [24, 72, 168]
LIQUIDATION_ARCHIVE_WINDOW_OPTIONS = ["最近 30 分钟", "最近 4 小时", "今天", "全部本地缓存"]
HYPERLIQUID_ADDRESS_PRESETS = {
    "手动输入": "",
    "官方示例主账户": "0x8c967e73e6b15087c42a10d344cff4c96d877f1d",
    "官方示例金库 Leader": "0x677d831aef5328190852e24f13c46cac05f984e7",
    "官方示例金库 Follower": "0x005844b2ffb2e122cf4244be7dbcb4f84924907c",
    "官方示例金库地址": "0xdfc24b077bc1425ad1dea75bcb6f8158e10df303",
}
HYPERLIQUID_WATCHLIST_OPTIONS = [label for label in HYPERLIQUID_ADDRESS_PRESETS if label != "手动输入"]
LAB_VIEW_OPTIONS = ["总览", "Hyperliquid", "跨所聚合", "策略层", "跨币种联动", "通知与持久化"]
ARCHIVE_RETENTION_DAYS_OPTIONS = [1, 3, 7, 14, 30]
ALERT_REVIEW_WINDOW_OPTIONS = [30, 60, 240, 1440]
ALERT_LEVEL_OPTIONS = ["弱", "中", "强"]
DEFAULT_CROSS_COIN_POOL = ["BTC", "ETH", "SOL"]
TOP_ORDERBOOK_SCOPE_OPTIONS = ["当前交易所", "四所聚合"]
TOP_ORDERBOOK_MARKET_OPTIONS = ["合约", "现货", "合并"]
HEADLINE_KIND_LABELS = {"oi": "OI", "liquidation": "爆仓", "trade": "异动", "large": "大单", "alert": "告警"}

if os.name == "nt":
    class _DpapiBlob(ctypes.Structure):
        _fields_ = [("cbData", wintypes.DWORD), ("pbData", ctypes.POINTER(ctypes.c_byte))]


    _CRYPT32 = ctypes.windll.crypt32
    _KERNEL32 = ctypes.windll.kernel32
else:
    _DpapiBlob = None
    _CRYPT32 = None
    _KERNEL32 = None


st.set_page_config(page_title="多交易所流动性终端", layout="wide")
st.markdown(
    """
    <style>
    :root {
        --glass-bg: rgba(255, 255, 255, 0.09);
        --glass-strong: rgba(255, 255, 255, 0.16);
        --glass-border: rgba(255, 255, 255, 0.18);
        --glass-shadow: 0 22px 52px rgba(6, 11, 21, 0.28);
        --text-main: #f8fbff;
        --text-soft: #d3e0f2;
        --text-muted: #aebfd8;
        --line-soft: rgba(255, 255, 255, 0.16);
        --sidebar-control-bg: rgba(20, 31, 49, 0.94);
        --sidebar-control-bg-soft: rgba(24, 38, 60, 0.96);
        --sidebar-control-border: rgba(183, 206, 238, 0.18);
        --sidebar-control-border-strong: rgba(183, 206, 238, 0.30);
        --sidebar-control-focus: rgba(122, 178, 255, 0.46);
        --sidebar-text-main: #f7fbff;
        --sidebar-text-soft: #deebfb;
        --sidebar-text-muted: #adc0da;
    }
    html, body, [class*="css"] {
        font-family: "SF Pro Display", "Segoe UI", sans-serif;
    }
    .stApp {
        background:
            radial-gradient(circle at 8% 14%, rgba(148, 195, 255, 0.34), transparent 26%),
            radial-gradient(circle at 90% 10%, rgba(255, 204, 158, 0.22), transparent 24%),
            radial-gradient(circle at 78% 82%, rgba(134, 234, 221, 0.16), transparent 20%),
            linear-gradient(140deg, #0f1828 0%, #122038 42%, #101925 100%);
        color: var(--text-main);
        background-attachment: fixed;
    }
    .stApp::before {
        content: "";
        position: fixed;
        inset: 0;
        background:
            linear-gradient(180deg, rgba(255, 255, 255, 0.06), transparent 24%),
            radial-gradient(circle at 50% 0%, rgba(255, 255, 255, 0.10), transparent 34%);
        pointer-events: none;
        z-index: 0;
    }
    header[data-testid="stHeader"] {
        background: rgba(10, 16, 27, 0.24);
        border-bottom: 1px solid rgba(255, 255, 255, 0.08);
        backdrop-filter: blur(20px);
    }
    .block-container {
        position: relative;
        z-index: 1;
        padding-top: 1.2rem;
        padding-bottom: 2rem;
        max-width: 1520px;
    }
    section[data-testid="stSidebar"] {
        background: linear-gradient(180deg, rgba(14, 24, 40, 0.96), rgba(11, 18, 31, 0.92));
        border-right: 1px solid rgba(255, 255, 255, 0.12);
        backdrop-filter: blur(30px);
    }
    section[data-testid="stSidebar"] > div {
        background: transparent;
    }
    section[data-testid="stSidebar"] .stMarkdown,
    section[data-testid="stSidebar"] .stCaption,
    section[data-testid="stSidebar"] label,
    section[data-testid="stSidebar"] span,
    section[data-testid="stSidebar"] p,
    section[data-testid="stSidebar"] h1,
    section[data-testid="stSidebar"] h2,
    section[data-testid="stSidebar"] h3,
    section[data-testid="stSidebar"] [data-testid="stWidgetLabel"] * {
        color: #dce7f7 !important;
    }
    section[data-testid="stSidebar"] [data-testid="stWidgetLabel"] p {
        font-weight: 680;
        letter-spacing: 0.01em;
        color: var(--sidebar-text-main) !important;
    }
    section[data-testid="stSidebar"] .stCaption {
        color: var(--sidebar-text-muted) !important;
        line-height: 1.55;
    }
    section[data-testid="stSidebar"] .stSubheader,
    section[data-testid="stSidebar"] h2,
    section[data-testid="stSidebar"] h3 {
        color: var(--sidebar-text-main) !important;
        letter-spacing: 0.01em;
    }
    section[data-testid="stSidebar"] hr {
        border-color: rgba(255, 255, 255, 0.10);
        margin: 0.85rem 0 1rem;
    }
    .hero-shell {
        position: relative;
        overflow: hidden;
        padding: 30px 32px 24px;
        margin-bottom: 1.1rem;
        border-radius: 34px;
        border: 1px solid rgba(255, 255, 255, 0.18);
        background: linear-gradient(135deg, rgba(255, 255, 255, 0.16), rgba(255, 255, 255, 0.07));
        box-shadow: var(--glass-shadow);
        backdrop-filter: blur(34px);
        transition: transform 260ms cubic-bezier(0.22, 1, 0.36, 1), box-shadow 320ms ease, border-color 240ms ease;
    }
    .hero-shell::before {
        content: "";
        position: absolute;
        top: -120px;
        right: -60px;
        width: 280px;
        height: 280px;
        background: radial-gradient(circle, rgba(255, 255, 255, 0.26), transparent 60%);
        pointer-events: none;
    }
    .hero-shell::after {
        content: "";
        position: absolute;
        left: -40px;
        bottom: -90px;
        width: 220px;
        height: 220px;
        background: radial-gradient(circle, rgba(132, 191, 255, 0.26), transparent 64%);
        pointer-events: none;
    }
    .hero-kicker {
        color: #d7e7ff;
        font-size: 0.77rem;
        text-transform: uppercase;
        letter-spacing: 0.2em;
        margin-bottom: 0.62rem;
    }
    .hero-title {
        color: #ffffff;
        font-size: 2.28rem;
        font-weight: 720;
        line-height: 1.05;
        margin-bottom: 0.48rem;
        text-shadow: 0 10px 28px rgba(5, 10, 18, 0.22);
    }
    .hero-sub {
        max-width: 920px;
        color: #e1ebf8;
        font-size: 1.03rem;
        line-height: 1.65;
    }
    .helper-bar {
        display: flex;
        flex-wrap: wrap;
        gap: 0.62rem;
        margin-top: 1rem;
    }
    .helper-pill {
        padding: 0.48rem 0.92rem;
        border-radius: 999px;
        border: 1px solid rgba(255, 255, 255, 0.14);
        background: rgba(255, 255, 255, 0.10);
        color: #f5f9ff;
        font-size: 0.87rem;
        box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.14);
        backdrop-filter: blur(18px);
        transition: background 220ms ease, border-color 220ms ease, transform 220ms ease;
    }
    .headline-marquee-shell {
        display: flex;
        align-items: center;
        gap: 0.9rem;
        padding: 0.86rem 1rem;
        margin: -0.18rem 0 0.9rem;
        border-radius: 24px;
        border: 1px solid rgba(255, 255, 255, 0.15);
        background: linear-gradient(135deg, rgba(255, 255, 255, 0.11), rgba(255, 255, 255, 0.05));
        box-shadow: var(--glass-shadow);
        backdrop-filter: blur(24px);
        overflow: hidden;
    }
    .headline-marquee-label {
        flex: 0 0 auto;
        padding: 0.46rem 0.8rem;
        border-radius: 999px;
        background: rgba(255, 129, 98, 0.2);
        border: 1px solid rgba(255, 169, 142, 0.32);
        color: #fff4ef;
        font-size: 0.76rem;
        font-weight: 700;
        letter-spacing: 0.08em;
        text-transform: uppercase;
        white-space: nowrap;
    }
    .headline-marquee-viewport {
        position: relative;
        flex: 1 1 auto;
        min-width: 0;
        overflow: hidden;
        mask-image: linear-gradient(90deg, transparent 0%, rgba(0, 0, 0, 1) 6%, rgba(0, 0, 0, 1) 94%, transparent 100%);
    }
    .headline-marquee-track {
        display: flex;
        align-items: center;
        width: max-content;
        animation: headline-marquee-scroll 38s linear infinite;
        will-change: transform;
    }
    .headline-marquee-group {
        display: flex;
        align-items: center;
        gap: 0.72rem;
        padding-right: 0.72rem;
        white-space: nowrap;
    }
    .headline-marquee-chip {
        display: inline-flex;
        align-items: center;
        gap: 0.62rem;
        padding: 0.42rem 0.54rem 0.42rem 0.46rem;
        border-radius: 999px;
        background: rgba(10, 19, 31, 0.4);
        border: 1px solid rgba(255, 255, 255, 0.14);
        color: #eff5ff;
        font-size: 0.9rem;
        line-height: 1.25;
        box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.08);
    }
    .headline-marquee-tag,
    .headline-marquee-text {
        text-decoration: none !important;
    }
    .headline-marquee-tag {
        display: inline-flex;
        align-items: center;
        justify-content: center;
        min-width: 3.2rem;
        padding: 0.34rem 0.72rem;
        border-radius: 999px;
        font-size: 0.75rem;
        font-weight: 700;
        letter-spacing: 0.05em;
        text-transform: uppercase;
        border: 1px solid transparent;
        white-space: nowrap;
    }
    .headline-marquee-text {
        color: #eff5ff !important;
        white-space: nowrap;
        padding-right: 0.24rem;
    }
    .headline-tag-oi {
        background: rgba(112, 196, 255, 0.18);
        border-color: rgba(112, 196, 255, 0.34);
        color: #dff3ff !important;
    }
    .headline-tag-liquidation {
        background: rgba(255, 126, 111, 0.18);
        border-color: rgba(255, 126, 111, 0.34);
        color: #ffe6df !important;
    }
    .headline-tag-trade {
        background: rgba(255, 198, 120, 0.18);
        border-color: rgba(255, 198, 120, 0.34);
        color: #fff0cf !important;
    }
    .headline-tag-large {
        background: rgba(120, 233, 188, 0.18);
        border-color: rgba(120, 233, 188, 0.34);
        color: #e3fff4 !important;
    }
    .headline-tag-alert {
        background: rgba(198, 159, 255, 0.18);
        border-color: rgba(198, 159, 255, 0.34);
        color: #f0e6ff !important;
    }
    .headline-marquee-tag:hover,
    .headline-marquee-text:hover {
        filter: brightness(1.06);
    }
    .section-anchor {
        position: relative;
        top: -82px;
        visibility: hidden;
        height: 0;
    }
    @keyframes headline-marquee-scroll {
        from {
            transform: translateX(0);
        }
        to {
            transform: translateX(-50%);
        }
    }
    @media (prefers-reduced-motion: reduce) {
        .headline-marquee-track {
            animation: none;
        }
        .headline-marquee-viewport {
            overflow-x: auto;
            mask-image: none;
        }
    }
    .glass-section {
        margin: 1.1rem 0 0.72rem;
        padding: 1rem 1.08rem;
        border-radius: 26px;
        border: 1px solid rgba(255, 255, 255, 0.13);
        background: linear-gradient(135deg, rgba(255, 255, 255, 0.10), rgba(255, 255, 255, 0.045));
        box-shadow: var(--glass-shadow);
        backdrop-filter: blur(26px);
        transition: transform 240ms cubic-bezier(0.22, 1, 0.36, 1), box-shadow 320ms ease, border-color 240ms ease;
    }
    .glass-kicker {
        color: #bfd5f2;
        font-size: 0.72rem;
        text-transform: uppercase;
        letter-spacing: 0.18em;
        margin-bottom: 0.32rem;
    }
    .glass-title {
        color: #ffffff;
        font-size: 1.18rem;
        font-weight: 680;
        margin-bottom: 0.24rem;
        letter-spacing: 0.01em;
    }
    .glass-title::after {
        content: "";
        display: block;
        width: 54px;
        height: 1px;
        margin-top: 0.62rem;
        background: linear-gradient(90deg, rgba(255, 255, 255, 0.82), rgba(255, 255, 255, 0.12));
    }
    .glass-sub {
        color: #dce8f6;
        font-size: 0.95rem;
        line-height: 1.55;
        margin-top: 0.62rem;
    }
    .status-strip {
        margin: 0.28rem 0 1rem;
        padding: 0.82rem 1.04rem;
        border-radius: 20px;
        border: 1px solid rgba(255, 255, 255, 0.12);
        background: rgba(255, 255, 255, 0.08);
        box-shadow: var(--glass-shadow);
        backdrop-filter: blur(26px);
        color: #eef5ff;
        font-size: 0.95rem;
        font-weight: 540;
        letter-spacing: 0.01em;
    }
    .stMarkdown p, .stCaption, label, [data-testid="stWidgetLabel"] p {
        color: #dce8f6 !important;
    }
    div[data-testid="stMetric"],
    div[data-testid="stPlotlyChart"],
    div[data-testid="stDataFrame"],
    details[data-testid="stExpander"],
    .glass-section,
    .status-strip,
    .hero-shell {
        transition: transform 240ms cubic-bezier(0.22, 1, 0.36, 1), box-shadow 320ms ease, border-color 240ms ease, background 260ms ease;
    }
    div[data-testid="stMetric"] {
        background: linear-gradient(145deg, rgba(255, 255, 255, 0.15), rgba(255, 255, 255, 0.07));
        border: 1px solid rgba(255, 255, 255, 0.16);
        border-radius: 24px;
        padding: 1rem 1.04rem;
        box-shadow: var(--glass-shadow);
        backdrop-filter: blur(28px);
    }
    div[data-testid="stMetric"]:hover,
    div[data-testid="stPlotlyChart"]:hover,
    div[data-testid="stDataFrame"]:hover,
    details[data-testid="stExpander"]:hover,
    .glass-section:hover,
    .hero-shell:hover {
        transform: translateY(-1px);
        border-color: rgba(255, 255, 255, 0.22);
        box-shadow: 0 26px 58px rgba(6, 12, 22, 0.30);
    }
    div[data-testid="stMetricLabel"] * {
        color: #d7e6f8 !important;
        font-weight: 600;
        letter-spacing: 0.01em;
    }
    div[data-testid="stMetricValue"] {
        color: #ffffff;
        text-shadow: 0 8px 22px rgba(7, 12, 21, 0.16);
    }
    div[data-testid="stMetricDelta"] {
        color: #d9ecff;
    }
    .stTabs [data-baseweb="tab-list"] {
        gap: 0.42rem;
        padding: 0.4rem;
        margin-bottom: 1rem;
        border-radius: 999px;
        border: 1px solid rgba(255, 255, 255, 0.12);
        background: rgba(255, 255, 255, 0.07);
        box-shadow: var(--glass-shadow);
        backdrop-filter: blur(24px);
    }
    .stTabs [data-baseweb="tab"] {
        height: 2.75rem;
        border-radius: 999px;
        color: #d3e0f2;
        background: transparent;
        font-weight: 600;
        transition: background 240ms ease, color 220ms ease, transform 220ms ease;
    }
    .stTabs [data-baseweb="tab"]:hover {
        background: rgba(255, 255, 255, 0.08);
        color: #f8fbff;
    }
    .stTabs [aria-selected="true"] {
        background: linear-gradient(135deg, rgba(255, 255, 255, 0.22), rgba(255, 255, 255, 0.10));
        color: #ffffff !important;
        box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.18);
    }
    div[data-baseweb="select"] > div,
    .stTextInput input {
        background: rgba(255, 255, 255, 0.10) !important;
        border: 1px solid rgba(255, 255, 255, 0.14) !important;
        border-radius: 16px !important;
        color: #f8fbff !important;
        backdrop-filter: blur(18px);
        box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.10);
    }
    section[data-testid="stSidebar"] div[data-baseweb="select"] > div,
    section[data-testid="stSidebar"] div[data-baseweb="input"] > div,
    section[data-testid="stSidebar"] .stTextInput input,
    section[data-testid="stSidebar"] .stTextArea textarea {
        background: linear-gradient(180deg, var(--sidebar-control-bg-soft), var(--sidebar-control-bg)) !important;
        border: 1px solid var(--sidebar-control-border) !important;
        border-radius: 16px !important;
        color: var(--sidebar-text-main) !important;
        box-shadow:
            inset 0 1px 0 rgba(255, 255, 255, 0.06),
            0 10px 24px rgba(4, 9, 18, 0.16) !important;
        transition: border-color 180ms ease, box-shadow 180ms ease, background 180ms ease;
    }
    section[data-testid="stSidebar"] div[data-baseweb="select"] > div:hover,
    section[data-testid="stSidebar"] div[data-baseweb="input"] > div:hover,
    section[data-testid="stSidebar"] .stTextInput input:hover,
    section[data-testid="stSidebar"] .stTextArea textarea:hover {
        border-color: var(--sidebar-control-border-strong) !important;
    }
    section[data-testid="stSidebar"] div[data-baseweb="select"] > div:focus-within,
    section[data-testid="stSidebar"] div[data-baseweb="input"] > div:focus-within,
    section[data-testid="stSidebar"] .stTextInput input:focus,
    section[data-testid="stSidebar"] .stTextArea textarea:focus {
        border-color: var(--sidebar-control-focus) !important;
        box-shadow:
            inset 0 1px 0 rgba(255, 255, 255, 0.08),
            0 0 0 1px rgba(122, 178, 255, 0.16),
            0 12px 26px rgba(4, 9, 18, 0.18) !important;
        outline: none !important;
    }
    section[data-testid="stSidebar"] div[data-baseweb="select"] input,
    section[data-testid="stSidebar"] div[data-baseweb="select"] span,
    section[data-testid="stSidebar"] div[data-baseweb="input"] input,
    section[data-testid="stSidebar"] .stTextInput input,
    section[data-testid="stSidebar"] .stTextArea textarea {
        color: var(--sidebar-text-main) !important;
        -webkit-text-fill-color: var(--sidebar-text-main) !important;
        caret-color: #ffffff !important;
        font-weight: 600;
    }
    section[data-testid="stSidebar"] .stTextInput input::placeholder,
    section[data-testid="stSidebar"] .stTextArea textarea::placeholder,
    section[data-testid="stSidebar"] div[data-baseweb="select"] input::placeholder {
        color: var(--sidebar-text-muted) !important;
        -webkit-text-fill-color: var(--sidebar-text-muted) !important;
        opacity: 1 !important;
    }
    section[data-testid="stSidebar"] input:disabled,
    section[data-testid="stSidebar"] textarea:disabled {
        color: var(--sidebar-text-soft) !important;
        -webkit-text-fill-color: var(--sidebar-text-soft) !important;
        opacity: 1 !important;
        background: linear-gradient(180deg, rgba(28, 42, 64, 0.90), rgba(21, 32, 51, 0.92)) !important;
        cursor: not-allowed;
    }
    section[data-testid="stSidebar"] input:-webkit-autofill,
    section[data-testid="stSidebar"] input:-webkit-autofill:hover,
    section[data-testid="stSidebar"] input:-webkit-autofill:focus,
    section[data-testid="stSidebar"] textarea:-webkit-autofill,
    section[data-testid="stSidebar"] textarea:-webkit-autofill:hover,
    section[data-testid="stSidebar"] textarea:-webkit-autofill:focus {
        -webkit-text-fill-color: var(--sidebar-text-main) !important;
        box-shadow: 0 0 0 1000px var(--sidebar-control-bg) inset !important;
        transition: background-color 99999s ease-in-out 0s;
    }
    section[data-testid="stSidebar"] div[data-baseweb="select"] svg,
    section[data-testid="stSidebar"] div[data-baseweb="input"] svg,
    section[data-testid="stSidebar"] [data-testid="stTextInputRootElement"] svg {
        color: var(--sidebar-text-soft) !important;
        fill: currentColor !important;
    }
    section[data-testid="stSidebar"] [data-baseweb="tag"] {
        background: rgba(255, 111, 98, 0.18) !important;
        border: 1px solid rgba(255, 140, 124, 0.36) !important;
        color: #fff7f5 !important;
    }
    section[data-testid="stSidebar"] [data-baseweb="tag"] * {
        color: #fff7f5 !important;
        fill: currentColor !important;
    }
    section[data-testid="stSidebar"] [data-testid="stCheckbox"] label {
        color: var(--sidebar-text-soft) !important;
        font-weight: 600;
    }
    section[data-testid="stSidebar"] [data-testid="stCheckbox"] div[role="checkbox"] {
        background: linear-gradient(180deg, rgba(30, 43, 64, 0.92), rgba(22, 33, 50, 0.96)) !important;
        border: 1px solid var(--sidebar-control-border) !important;
        border-radius: 8px !important;
        box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.05);
    }
    section[data-testid="stSidebar"] [data-testid="stCheckbox"] div[role="checkbox"][aria-checked="true"] {
        background: linear-gradient(180deg, rgba(255, 114, 98, 0.94), rgba(255, 136, 112, 0.92)) !important;
        border-color: rgba(255, 160, 142, 0.58) !important;
    }
    section[data-testid="stSidebar"] [data-testid="stCheckbox"] svg {
        color: #ffffff !important;
        fill: currentColor !important;
    }
    section[data-testid="stSidebar"] .stButton > button {
        width: 100%;
        min-height: 2.7rem;
        border-radius: 999px;
        border: 1px solid rgba(183, 206, 238, 0.16);
        background: linear-gradient(180deg, rgba(36, 49, 72, 0.90), rgba(25, 35, 53, 0.96));
        color: var(--sidebar-text-main);
        font-weight: 680;
        letter-spacing: 0.01em;
    }
    section[data-testid="stSidebar"] .stButton > button:hover {
        border-color: rgba(183, 206, 238, 0.26);
        background: linear-gradient(180deg, rgba(44, 58, 84, 0.94), rgba(31, 43, 63, 0.98));
        box-shadow: 0 14px 28px rgba(4, 9, 18, 0.18);
    }
    .stButton > button {
        border-radius: 999px;
        border: 1px solid rgba(255, 255, 255, 0.15);
        background: linear-gradient(135deg, rgba(255, 255, 255, 0.17), rgba(255, 255, 255, 0.08));
        color: #ffffff;
        box-shadow: var(--glass-shadow);
        backdrop-filter: blur(16px);
        transition: transform 220ms ease, background 220ms ease, border-color 220ms ease, box-shadow 260ms ease;
    }
    .stButton > button:hover {
        transform: translateY(-1px);
        border-color: rgba(255, 255, 255, 0.24);
        background: linear-gradient(135deg, rgba(255, 255, 255, 0.24), rgba(255, 255, 255, 0.12));
    }
    .stButton > button[kind="primary"] {
        border-color: rgba(255, 176, 154, 0.42);
        background: linear-gradient(135deg, rgba(255, 112, 94, 0.95), rgba(255, 142, 112, 0.88));
        color: #ffffff;
        box-shadow: 0 18px 34px rgba(255, 104, 92, 0.24);
        font-weight: 700;
    }
    .stButton > button[kind="secondary"] {
        color: #eef5ff;
    }
    .mode-bar-label {
        margin: 0.32rem 0 0.48rem;
        color: #eef5ff;
        font-size: 0.88rem;
        font-weight: 620;
        letter-spacing: 0.02em;
    }
    div[data-testid="stDataFrame"],
    div[data-testid="stPlotlyChart"],
    details[data-testid="stExpander"] {
        border-radius: 24px;
        overflow: hidden;
        border: 1px solid rgba(255, 255, 255, 0.12);
        box-shadow: var(--glass-shadow);
        background: rgba(255, 255, 255, 0.05);
        backdrop-filter: blur(24px);
    }
    div[data-testid="stDataFrame"] [role="columnheader"],
    div[data-testid="stDataFrame"] [role="gridcell"],
    div[data-testid="stDataFrame"] [data-testid="StyledDataFrameCell"] {
        color: #eef5ff !important;
    }
    div[data-testid="stAlert"] {
        border-radius: 20px;
        border: 1px solid rgba(255, 255, 255, 0.13);
        background: rgba(255, 255, 255, 0.08);
        backdrop-filter: blur(22px);
        color: #f6fbff;
    }
    code {
        background: rgba(255, 255, 255, 0.10);
        border-radius: 10px;
        padding: 0.18rem 0.38rem;
        color: #f5f9ff;
    }
    </style>
    """,
    unsafe_allow_html=True,
)


def _serialize_model_list(items: List[Any]) -> List[Dict[str, Any]]:
    return [asdict(item) for item in list(items or [])]


def _restore_model_list(payload: List[Dict[str, Any]], model_type: Any) -> List[Any]:
    restored: List[Any] = []
    for item in payload or []:
        if isinstance(item, model_type):
            restored.append(item)
        elif isinstance(item, dict):
            try:
                restored.append(model_type(**item))
            except TypeError:
                continue
    return restored


def _load_candles_payload_cached(exchange_key: str, symbol: str, interval: str, limit: int, timeout: int) -> List[Dict[str, Any]]:
    try:
        return _serialize_model_list(fetch_exchange_candles(exchange_key, symbol, interval, limit, timeout=timeout))
    except Exception:
        return []


def load_candles_cached(exchange_key: str, symbol: str, interval: str, limit: int, timeout: int) -> List[Candle]:
    return get_local_ttl_result(
        f"candles::{exchange_key}::{symbol}::{interval}::{int(limit)}::{int(timeout)}",
        15,
        lambda: _restore_model_list(_load_candles_payload_cached(exchange_key, symbol, interval, limit, timeout), Candle),
    )


def _load_orderbook_payload_cached(exchange_key: str, symbol: str, limit: int, timeout: int) -> List[Dict[str, Any]]:
    try:
        return _serialize_model_list(fetch_exchange_orderbook(exchange_key, symbol, limit, timeout=timeout))
    except Exception:
        return []


def load_orderbook_cached(exchange_key: str, symbol: str, limit: int, timeout: int) -> List[OrderBookLevel]:
    return get_local_ttl_result(
        f"perp-orderbook::{exchange_key}::{symbol}::{int(limit)}::{int(timeout)}",
        5,
        lambda: _restore_model_list(_load_orderbook_payload_cached(exchange_key, symbol, limit, timeout), OrderBookLevel),
    )


def _load_oi_backfill_payload_cached(exchange_key: str, symbol: str, interval: str, limit: int, timeout: int) -> List[Dict[str, Any]]:
    try:
        return _serialize_model_list(fetch_exchange_oi_history(exchange_key, symbol, interval, limit, timeout=timeout))
    except Exception:
        return []


def load_oi_backfill_cached(exchange_key: str, symbol: str, interval: str, limit: int, timeout: int) -> List[OIPoint]:
    return get_local_ttl_result(
        f"oi::{exchange_key}::{symbol}::{interval}::{int(limit)}::{int(timeout)}",
        90,
        lambda: _restore_model_list(_load_oi_backfill_payload_cached(exchange_key, symbol, interval, limit, timeout), OIPoint),
    )


def _load_liquidations_payload_cached(exchange_key: str, symbol: str, limit: int, timeout: int) -> List[Dict[str, Any]]:
    try:
        return _serialize_model_list(fetch_exchange_liquidations(exchange_key, symbol, limit, timeout=timeout))
    except Exception:
        return []


def load_liquidations_cached(exchange_key: str, symbol: str, limit: int, timeout: int) -> List[LiquidationEvent]:
    return get_local_ttl_result(
        f"liq::{exchange_key}::{symbol}::{int(limit)}::{int(timeout)}",
        10,
        lambda: _restore_model_list(_load_liquidations_payload_cached(exchange_key, symbol, limit, timeout), LiquidationEvent),
    )


def _load_exchange_trades_payload_cached(exchange_key: str, symbol: str, limit: int, timeout: int) -> List[Dict[str, Any]]:
    try:
        return _serialize_model_list(fetch_exchange_trades(exchange_key, symbol, limit, timeout=timeout))
    except Exception:
        return []


def load_exchange_trades_cached(exchange_key: str, symbol: str, limit: int, timeout: int) -> List[TradeEvent]:
    return get_local_ttl_result(
        f"perp-trades::{exchange_key}::{symbol}::{int(limit)}::{int(timeout)}",
        5,
        lambda: _restore_model_list(_load_exchange_trades_payload_cached(exchange_key, symbol, limit, timeout), TradeEvent),
    )


@st.cache_data(ttl=120, show_spinner=False)
def load_binance_crowd_cached(symbol: str, period: str, timeout: int):
    try:
        return fetch_binance_trader_sentiment(symbol, period, 60, timeout=timeout)
    except Exception:
        return {}


@st.cache_data(ttl=120, show_spinner=False)
def load_bybit_crowd_cached(symbol: str, period: str, timeout: int):
    try:
        return fetch_bybit_trader_sentiment(symbol, period, 60, timeout=timeout)
    except Exception:
        return {}


@st.cache_data(ttl=900, show_spinner=False)
def load_bybit_insurance_cached(coin: str, timeout: int):
    try:
        return fetch_bybit_insurance_pool(coin, timeout=timeout)
    except Exception:
        return {}


@st.cache_data(ttl=120, show_spinner=False)
def load_binance_basis_curve_cached(pair: str, period: str, timeout: int):
    try:
        return fetch_binance_basis_curve(pair, period, 60, timeout=timeout)
    except Exception:
        return {}


def _load_binance_spot_snapshot_payload_cached(symbol: str, timeout: int) -> Dict[str, Any]:
    try:
        return asdict(fetch_binance_spot_snapshot(symbol, timeout=timeout))
    except Exception as exc:
        return asdict(SpotSnapshot(exchange="Binance Spot", symbol=symbol, status="error", error=str(exc)))


def load_binance_spot_snapshot_cached(symbol: str, timeout: int) -> SpotSnapshot:
    return get_local_ttl_result(
        f"binance-spot-snapshot::{symbol}::{int(timeout)}",
        5,
        lambda: SpotSnapshot(**_load_binance_spot_snapshot_payload_cached(symbol, timeout)),
    )


def _load_binance_spot_orderbook_payload_cached(symbol: str, limit: int, timeout: int) -> List[Dict[str, Any]]:
    try:
        return _serialize_model_list(fetch_binance_spot_orderbook(symbol, limit, timeout=timeout))
    except Exception:
        return []


def load_binance_spot_orderbook_cached(symbol: str, limit: int, timeout: int) -> List[OrderBookLevel]:
    return get_local_ttl_result(
        f"binance-spot-orderbook::{symbol}::{int(limit)}::{int(timeout)}",
        5,
        lambda: _restore_model_list(_load_binance_spot_orderbook_payload_cached(symbol, limit, timeout), OrderBookLevel),
    )


def _load_binance_spot_trades_payload_cached(symbol: str, limit: int, timeout: int) -> List[Dict[str, Any]]:
    try:
        return _serialize_model_list(fetch_binance_spot_trades(symbol, limit, timeout=timeout))
    except Exception:
        return []


def load_binance_spot_trades_cached(symbol: str, limit: int, timeout: int) -> List[TradeEvent]:
    return get_local_ttl_result(
        f"binance-spot-trades::{symbol}::{int(limit)}::{int(timeout)}",
        5,
        lambda: _restore_model_list(_load_binance_spot_trades_payload_cached(symbol, limit, timeout), TradeEvent),
    )


def _load_spot_snapshot_payload_cached(exchange_key: str, symbol: str, timeout: int) -> Dict[str, Any]:
    try:
        return asdict(fetch_spot_snapshot(exchange_key, symbol, timeout=timeout))
    except Exception as exc:
        return asdict(SpotSnapshot(exchange=f"{EXCHANGE_TITLES.get(exchange_key, exchange_key.title())} Spot", symbol=symbol, status="error", error=str(exc)))


def load_spot_snapshot_cached(exchange_key: str, symbol: str, timeout: int) -> SpotSnapshot:
    return get_local_ttl_result(
        f"spot-snapshot::{exchange_key}::{symbol}::{int(timeout)}",
        5,
        lambda: SpotSnapshot(**_load_spot_snapshot_payload_cached(exchange_key, symbol, timeout)),
    )


def _load_spot_orderbook_payload_cached(exchange_key: str, symbol: str, limit: int, timeout: int) -> List[Dict[str, Any]]:
    try:
        return _serialize_model_list(fetch_spot_orderbook(exchange_key, symbol, limit, timeout=timeout))
    except Exception:
        return []


def load_spot_orderbook_cached(exchange_key: str, symbol: str, limit: int, timeout: int) -> List[OrderBookLevel]:
    return get_local_ttl_result(
        f"spot-orderbook::{exchange_key}::{symbol}::{int(limit)}::{int(timeout)}",
        5,
        lambda: _restore_model_list(_load_spot_orderbook_payload_cached(exchange_key, symbol, limit, timeout), OrderBookLevel),
    )


def _load_spot_trades_payload_cached(exchange_key: str, symbol: str, limit: int, timeout: int) -> List[Dict[str, Any]]:
    try:
        return _serialize_model_list(fetch_spot_trades(exchange_key, symbol, limit, timeout=timeout))
    except Exception:
        return []


def load_spot_trades_cached(exchange_key: str, symbol: str, limit: int, timeout: int) -> List[TradeEvent]:
    return get_local_ttl_result(
        f"spot-trades::{exchange_key}::{symbol}::{int(limit)}::{int(timeout)}",
        5,
        lambda: _restore_model_list(_load_spot_trades_payload_cached(exchange_key, symbol, limit, timeout), TradeEvent),
    )


@st.cache_data(ttl=12, show_spinner=False)
def load_hyperliquid_address_mode_cached(address: str, coin: str, lookback_hours: int, timeout: int) -> Dict[str, Any]:
    try:
        return fetch_hyperliquid_address_mode(address, coin, lookback_hours, timeout=timeout)
    except Exception as exc:
        return {
            "status": "error",
            "error": str(exc),
            "address": address,
            "coin": coin,
            "positions": [],
            "fills": [],
            "funding": [],
            "active_asset": {},
            "spot_state": {},
        }


@st.cache_data(ttl=30, show_spinner=False)
def load_hyperliquid_predicted_fundings_cached(timeout: int) -> List[list]:
    try:
        return fetch_hyperliquid_predicted_fundings(timeout=timeout)
    except Exception:
        return []


@st.cache_data(ttl=12, show_spinner=False)
def load_hyperliquid_all_mids_cached(timeout: int) -> Dict[str, str]:
    try:
        return fetch_hyperliquid_all_mids(timeout=timeout)
    except Exception:
        return {}


@st.cache_data(ttl=20, show_spinner=False)
def load_hyperliquid_spot_meta_and_asset_contexts_cached(timeout: int) -> Dict[str, Any]:
    try:
        return fetch_hyperliquid_spot_meta_and_asset_contexts(timeout=timeout)
    except Exception:
        return {"meta": {}, "contexts": []}


@st.cache_data(ttl=20, show_spinner=False)
def load_hyperliquid_perps_at_oi_cap_cached(timeout: int) -> Dict[str, Any]:
    try:
        return fetch_hyperliquid_perps_at_open_interest_cap(timeout=timeout)
    except Exception:
        return {}


@st.cache_data(ttl=120, show_spinner=False)
def load_exchange_coin_catalog_cached(timeout: int) -> Dict[str, Any]:
    try:
        return fetch_exchange_coin_catalog(timeout=timeout)
    except Exception:
        return {"coins": [], "availability": {}, "summary": {}, "status": {}, "errors": {}}


def merge_liquidation_event_groups(*groups: List[LiquidationEvent]) -> List[LiquidationEvent]:
    merged: List[LiquidationEvent] = []
    for group in groups:
        merged = merge_liquidation_events(merged, list(group or []))
    return merged


def _start_of_local_day_ms() -> int:
    return int(pd.Timestamp.now().normalize().timestamp() * 1000)


def resolve_liquidation_archive_window(window_label: str, now_ms: int) -> Tuple[int | None, str]:
    if window_label == "最近 30 分钟":
        return now_ms - 30 * 60_000, "最近 30 分钟"
    if window_label == "最近 4 小时":
        return now_ms - 4 * 3_600_000, "最近 4 小时"
    if window_label == "今天":
        return _start_of_local_day_ms(), "今天"
    return None, "全部本地缓存"


def payload_float(value: Any) -> float | None:
    parsed = pd.to_numeric(value, errors="coerce")
    return None if pd.isna(parsed) else float(parsed)


def build_liquidation_exchange_totals_frame(events: List[LiquidationEvent]) -> pd.DataFrame:
    if not events:
        return pd.DataFrame(columns=["交易所", "事件数", "总名义金额", "多头爆仓额", "空头爆仓额", "最近时间"])
    frame = pd.DataFrame(
        {
            "交易所": [event.exchange for event in events],
            "方向": [event.side for event in events],
            "名义金额": [event.notional or 0.0 for event in events],
            "时间": [pd.to_datetime(event.timestamp_ms, unit="ms") for event in events],
        }
    )
    grouped = frame.groupby("交易所", as_index=False).agg(
        事件数=("交易所", "size"),
        总名义金额=("名义金额", "sum"),
        最近时间=("时间", "max"),
    )
    long_notional = frame[frame["方向"] == "long"].groupby("交易所")["名义金额"].sum()
    short_notional = frame[frame["方向"] == "short"].groupby("交易所")["名义金额"].sum()
    grouped["多头爆仓额"] = grouped["交易所"].map(long_notional).fillna(0.0)
    grouped["空头爆仓额"] = grouped["交易所"].map(short_notional).fillna(0.0)
    return grouped.sort_values("总名义金额", ascending=False).reset_index(drop=True)


def build_carry_surface_rows(
    snapshot_by_key: Dict[str, ExchangeSnapshot],
    spot_snapshot_map: Dict[str, SpotSnapshot],
    exchange_keys: List[str],
) -> List[Dict[str, float | str | None]]:
    anchor_candidates = [
        snapshot.last_price
        for exchange_key, snapshot in spot_snapshot_map.items()
        if exchange_key in SPOT_EXCHANGE_ORDER and snapshot.status == "ok" and snapshot.last_price not in (None, 0)
    ]
    anchor_spot_price = float(pd.Series(anchor_candidates).median()) if anchor_candidates else None
    total_oi_notional = sum(
        snapshot_by_key[exchange_key].open_interest_notional or 0.0
        for exchange_key in exchange_keys
        if exchange_key in snapshot_by_key and snapshot_by_key[exchange_key].open_interest_notional is not None
    )
    rows: List[Dict[str, float | str | None]] = []
    for exchange_key in exchange_keys:
        snapshot = snapshot_by_key.get(exchange_key)
        if snapshot is None:
            continue
        spot_snapshot = spot_snapshot_map.get(exchange_key)
        has_same_venue_spot = (
            spot_snapshot is not None
            and spot_snapshot.status == "ok"
            and spot_snapshot.last_price not in (None, 0)
        )
        spot_reference_price = float(spot_snapshot.last_price) if has_same_venue_spot else anchor_spot_price
        basis_pct = None
        if snapshot.last_price not in (None, 0) and spot_reference_price not in (None, 0):
            basis_pct = (float(snapshot.last_price) - float(spot_reference_price)) / float(spot_reference_price) * 100.0
        funding_bps = snapshot.funding_bps
        basis_bps = basis_pct * 100.0 if basis_pct is not None else None
        if basis_bps is not None and funding_bps is not None:
            carry_tilt_bps = basis_bps + funding_bps
        elif basis_bps is not None:
            carry_tilt_bps = basis_bps
        else:
            carry_tilt_bps = funding_bps
        annualized_funding_pct = snapshot.funding_rate * 3.0 * 365.0 * 100.0 if snapshot.funding_rate is not None else None
        volume_ratio = None
        if (
            has_same_venue_spot
            and spot_snapshot is not None
            and spot_snapshot.volume_24h_notional not in (None, 0)
            and snapshot.volume_24h_notional is not None
        ):
            volume_ratio = snapshot.volume_24h_notional / spot_snapshot.volume_24h_notional
        oi_notional = snapshot.open_interest_notional
        oi_share_pct = oi_notional / total_oi_notional * 100.0 if total_oi_notional > 0 and oi_notional is not None else None
        rows.append(
            {
                "交易所": snapshot.exchange,
                "现货锚定": EXCHANGE_TITLES.get(exchange_key, exchange_key.title()) if has_same_venue_spot else "全市场现货中位",
                "Basis来源": "同交易所现货" if has_same_venue_spot else "跨所现货锚定",
                "Basis(%)": basis_pct,
                "Funding(bps)": funding_bps,
                "Carry倾斜(bps)": carry_tilt_bps,
                "年化Funding(%)": annualized_funding_pct,
                "24h成交额比": volume_ratio,
                "OI金额": oi_notional,
                "OI份额(%)": oi_share_pct,
            }
        )
    return rows


def build_hyperliquid_position_frame(bundle: Dict[str, Any]) -> pd.DataFrame:
    positions = bundle.get("positions") or []
    if not positions:
        return pd.DataFrame(columns=["币种", "方向", "仓位", "开仓价", "标记价", "清算价", "杠杆", "仓位价值", "未实现PnL", "ROE(%)"])
    rows = []
    for position in positions:
        side = str(position.get("side") or "")
        rows.append(
            {
                "币种": position.get("coin"),
                "方向": "多头" if side == "long" else "空头" if side == "short" else "空仓",
                "仓位": position.get("size"),
                "开仓价": position.get("entry_price"),
                "标记价": position.get("mark_price"),
                "清算价": position.get("liquidation_price"),
                "杠杆": position.get("leverage"),
                "仓位价值": position.get("position_value"),
                "未实现PnL": position.get("unrealized_pnl"),
                "ROE(%)": None if position.get("return_on_equity") is None else float(position.get("return_on_equity")) * 100.0,
            }
        )
    return pd.DataFrame(rows)


def build_hyperliquid_fill_frame(bundle: Dict[str, Any]) -> pd.DataFrame:
    fills = bundle.get("fills") or []
    if not fills:
        return pd.DataFrame(columns=["时间", "币种", "方向", "价格", "数量", "名义金额", "已实现PnL", "手续费"])
    rows = []
    for fill in fills:
        rows.append(
            {
                "时间": pd.to_datetime(int(fill.get("time") or 0), unit="ms"),
                "币种": fill.get("coin"),
                "方向": fill.get("direction"),
                "价格": fill.get("price"),
                "数量": fill.get("size"),
                "名义金额": fill.get("notional"),
                "已实现PnL": fill.get("closed_pnl"),
                "手续费": fill.get("fee"),
            }
        )
    return pd.DataFrame(rows)


def build_hyperliquid_funding_frame(bundle: Dict[str, Any]) -> pd.DataFrame:
    funding_records = bundle.get("funding") or []
    if not funding_records:
        return pd.DataFrame(columns=["时间", "币种", "方向", "金额", "类型"])
    rows = []
    for item in funding_records:
        rows.append(
            {
                "时间": pd.to_datetime(int(item.get("time") or 0), unit="ms"),
                "币种": item.get("coin"),
                "方向": "收入" if str(item.get("direction") or "") == "received" else "支出",
                "金额": item.get("amount"),
                "类型": item.get("type"),
            }
        )
    return pd.DataFrame(rows)


def build_hyperliquid_user_event_frame(bundle: Dict[str, Any]) -> pd.DataFrame:
    events = bundle.get("user_events") or []
    if not events:
        return pd.DataFrame(columns=["时间", "类别", "摘要"])
    rows = []
    for item in events:
        payload = item.get("payload") or {}
        summary = ""
        if isinstance(payload, dict):
            if "coin" in payload:
                summary = str(payload.get("coin"))
            elif "liquidated_user" in payload:
                summary = f"liquidated {payload.get('liquidated_user')}"
            else:
                summary = ", ".join(list(payload.keys())[:3])
        rows.append(
            {
                "时间": pd.to_datetime(int(item.get("time") or 0), unit="ms"),
                "类别": item.get("category"),
                "摘要": summary,
            }
        )
    return pd.DataFrame(rows)


def build_hyperliquid_vault_equity_frame(bundle: Dict[str, Any]) -> pd.DataFrame:
    items = bundle.get("vault_equities") or []
    if not items:
        return pd.DataFrame(columns=["金库地址", "权益"])
    rows = []
    for item in items:
        if not isinstance(item, dict):
            continue
        rows.append(
            {
                "金库地址": item.get("vaultAddress") or item.get("vault_address"),
                "权益": payload_float(item.get("equity")),
            }
        )
    return pd.DataFrame(rows)


def build_hyperliquid_portfolio_frame(bundle: Dict[str, Any]) -> pd.DataFrame:
    portfolio = bundle.get("portfolio") or []
    if not portfolio:
        return pd.DataFrame(columns=["周期", "账户权益", "PnL", "成交量"])
    rows = []
    for item in portfolio:
        if not isinstance(item, (list, tuple)) or len(item) != 2:
            continue
        window_key, payload = item
        if not isinstance(payload, dict):
            continue
        account_value_history = payload.get("accountValueHistory") or []
        pnl_history = payload.get("pnlHistory") or []
        latest_account_value = payload_float(account_value_history[-1][1]) if account_value_history else None
        latest_pnl = payload_float(pnl_history[-1][1]) if pnl_history else None
        rows.append(
            {
                "周期": window_key,
                "账户权益": latest_account_value,
                "PnL": latest_pnl,
                "成交量": payload_float(payload.get("vlm")),
            }
        )
    return pd.DataFrame(rows)


def build_hyperliquid_vault_detail_frame(bundle: Dict[str, Any]) -> pd.DataFrame:
    vault_details = bundle.get("vault_details") or {}
    if not isinstance(vault_details, dict) or not vault_details:
        return pd.DataFrame(columns=["字段", "数值"])
    rows = []
    for key, value in vault_details.items():
        if isinstance(value, (dict, list)):
            continue
        rows.append({"字段": key, "数值": value})
    return pd.DataFrame(rows)


def merge_hyperliquid_address_bundle(base_bundle: Dict[str, Any], live_bundle: Dict[str, Any] | None) -> Dict[str, Any]:
    if not live_bundle:
        merged = dict(base_bundle)
        merged["stream_status"] = "未连接"
        merged["connected"] = False
        merged["last_message_ms"] = None
        merged.setdefault("user_events", [])
        return merged
    merged = dict(base_bundle)
    for key in (
        "account_value",
        "total_margin_used",
        "withdrawable",
        "total_notional_position",
        "active_asset",
        "spot_state",
        "raw_state",
        "role",
        "portfolio",
        "vault_equities",
        "vault_details",
        "stream_status",
        "connected",
        "last_message_ms",
        "timestamp_ms",
        "error",
        "user_events",
    ):
        live_value = live_bundle.get(key)
        if live_value not in (None, [], {}, ""):
            merged[key] = live_value
    if live_bundle.get("positions"):
        merged["positions"] = list(live_bundle.get("positions") or [])
    if live_bundle.get("fills"):
        merged["fills"] = list(live_bundle.get("fills") or [])
    if live_bundle.get("funding"):
        merged["funding"] = list(live_bundle.get("funding") or [])
    return merged


def render_carry_surface_panel(frame: pd.DataFrame, *, key_scope: str) -> None:
    render_section(
        "跨所 Carry / Basis / Funding 曲面",
        "把 Basis、Funding、简化 Carry 倾斜和 OI 份额放在一张矩阵里。Carry 倾斜 = Basis(bps) + Funding(bps)，这里只作为横向比较分数，不等同可实现套利收益。",
    )
    if len(frame) <= 1:
        st.info("当前样本仍偏聚焦。把 `交易所范围` 切到 `全部交易所`，这张曲面会更完整。")
    surface_left, surface_right = st.columns([1.55, 1.35], gap="large")
    with surface_left:
        st.plotly_chart(
            build_carry_surface_figure(frame),
            key=chart_key("carry-surface", key_scope),
            config=PLOTLY_CONFIG,
        )
    with surface_right:
        st.dataframe(
            frame,
            width="stretch",
            hide_index=True,
            column_config={
                "Basis(%)": st.column_config.NumberColumn(format="%.3f"),
                "Funding(bps)": st.column_config.NumberColumn(format="%.2f"),
                "Carry倾斜(bps)": st.column_config.NumberColumn(format="%.2f"),
                "年化Funding(%)": st.column_config.NumberColumn(format="%.2f"),
                "24h成交额比": st.column_config.NumberColumn(format="%.2f"),
                "OI金额": st.column_config.NumberColumn(format="%.2f"),
                "OI份额(%)": st.column_config.ProgressColumn(format="%.2f", min_value=0.0, max_value=100.0),
            },
        )


def render_hyperliquid_address_panel(
    bundle: Dict[str, Any],
    *,
    address: str,
    lookback_hours: int,
    current_coin: str,
    key_scope: str,
) -> None:
    render_hyperliquid_address_panel_view(
        bundle,
        address=address,
        lookback_hours=lookback_hours,
        current_coin=current_coin,
        key_scope=key_scope,
        render_section=render_section,
        build_hyperliquid_position_frame=build_hyperliquid_position_frame,
        build_hyperliquid_fill_frame=build_hyperliquid_fill_frame,
        build_hyperliquid_funding_frame=build_hyperliquid_funding_frame,
        build_hyperliquid_user_event_frame=build_hyperliquid_user_event_frame,
        build_hyperliquid_vault_equity_frame=build_hyperliquid_vault_equity_frame,
        build_hyperliquid_portfolio_frame=build_hyperliquid_portfolio_frame,
        build_hyperliquid_vault_detail_frame=build_hyperliquid_vault_detail_frame,
        format_latency=_format_latency,
        fmt_compact=fmt_compact,
    )


def resolve_history_store() -> TerminalHistoryStore:
    store = st.session_state.get("_terminal_history_store")
    if isinstance(store, TerminalHistoryStore):
        return store
    store = TerminalHistoryStore()
    st.session_state["_terminal_history_store"] = store
    return store


def alert_level_rank(level: str | None) -> int:
    mapping = {"低": 0, "观察": 0, "弱": 1, "中": 2, "强": 3}
    return mapping.get(str(level or ""), 0)


def parse_hyperliquid_address_pool_text(text: str) -> tuple[List[Dict[str, str]], List[str]]:
    rows: List[Dict[str, str]] = []
    errors: List[str] = []
    for line_no, raw_line in enumerate(str(text or "").splitlines(), start=1):
        line = raw_line.strip()
        if not line:
            continue
        parts = [part.strip() for part in line.split("|")]
        if len(parts) == 1:
            label = f"自定义地址 {line_no}"
            address = parts[0]
            group = "自定义"
        elif len(parts) == 2:
            label, address = parts
            group = "自定义"
        else:
            label, address, group = parts[:3]
        normalized_address = str(address or "").strip().lower()
        if not is_valid_onchain_address(normalized_address):
            errors.append(f"第 {line_no} 行地址无效: {address}")
            continue
        rows.append(
            {
                "label": label or f"自定义地址 {line_no}",
                "address": normalized_address,
                "group": group or "自定义",
                "source": "custom",
            }
        )
    return rows, errors


def normalize_address_pool_entries(entries: Any) -> List[Dict[str, str]]:
    normalized: List[Dict[str, str]] = []
    seen_addresses = set()
    if not isinstance(entries, list):
        return normalized
    for index, item in enumerate(entries, start=1):
        if not isinstance(item, dict):
            continue
        address = str(item.get("address") or "").strip().lower()
        if not is_valid_onchain_address(address) or address in seen_addresses:
            continue
        seen_addresses.add(address)
        normalized.append(
            {
                "label": str(item.get("label") or f"自定义地址 {index}").strip() or f"自定义地址 {index}",
                "address": address,
                "group": str(item.get("group") or "自定义").strip() or "自定义",
                "source": str(item.get("source") or "custom"),
            }
        )
    return normalized


def merge_address_pool_entries(existing: List[Dict[str, str]], imported: List[Dict[str, str]]) -> List[Dict[str, str]]:
    merged: Dict[str, Dict[str, str]] = {}
    for item in normalize_address_pool_entries(existing) + normalize_address_pool_entries(imported):
        merged[str(item.get("address"))] = item
    return list(merged.values())


def build_watch_option_catalog(custom_entries: List[Dict[str, str]]) -> List[Dict[str, str]]:
    catalog: List[Dict[str, str]] = []
    for label, address in HYPERLIQUID_ADDRESS_PRESETS.items():
        if label == "手动输入":
            continue
        catalog.append({"option": label, "label": label, "address": address, "group": "官方示例", "source": "preset"})
    for item in normalize_address_pool_entries(custom_entries):
        catalog.append(
            {
                "option": f"{item['group']} / {item['label']}",
                "label": item["label"],
                "address": item["address"],
                "group": item["group"],
                "source": item.get("source") or "custom",
            }
        )
    return catalog


def build_hyperliquid_watchlist_specs(
    current_address: str,
    selected_labels: List[str],
    *,
    option_catalog: List[Dict[str, str]] | None = None,
) -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []
    seen_addresses = set()
    catalog_by_option = {
        str(item.get("option") or ""): item
        for item in (option_catalog or [])
        if isinstance(item, dict)
    }
    for label in selected_labels:
        meta = catalog_by_option.get(str(label), {})
        address = str(meta.get("address") or HYPERLIQUID_ADDRESS_PRESETS.get(label) or "").strip().lower()
        if not address or address in seen_addresses:
            continue
        seen_addresses.add(address)
        rows.append(
            {
                "label": str(meta.get("label") or label),
                "address": address,
                "group": str(meta.get("group") or "默认"),
                "source": str(meta.get("source") or "preset"),
            }
        )
    normalized_current = str(current_address or "").strip().lower()
    if is_valid_onchain_address(normalized_current) and normalized_current not in seen_addresses:
        rows.insert(0, {"label": "当前地址", "address": normalized_current, "group": "当前输入", "source": "current"})
    return rows


def load_hyperliquid_watchlist_bundles(
    specs: List[Dict[str, str]],
    *,
    coin: str,
    lookback_hours: int,
    timeout: int,
    current_address: str,
    current_bundle: Dict[str, Any] | None = None,
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    normalized_current = str(current_address or "").strip().lower()
    for spec in specs:
        address = str(spec.get("address") or "").strip().lower()
        if not is_valid_onchain_address(address):
            continue
        if current_bundle and address == normalized_current:
            bundle = dict(current_bundle)
        else:
            bundle = load_hyperliquid_address_mode_cached(address, coin, lookback_hours, timeout)
        bundle["label"] = spec.get("label") or address
        bundle["address"] = address
        bundle["group"] = spec.get("group") or "默认"
        bundle["source"] = spec.get("source") or "preset"
        rows.append(bundle)
    return rows


def build_hyperliquid_watchlist_leaderboard_frame(bundles: List[Dict[str, Any]]) -> pd.DataFrame:
    rows = []
    for bundle in bundles:
        portfolio_frame = build_hyperliquid_portfolio_frame(bundle)
        funding_frame = build_hyperliquid_funding_frame(bundle)
        position_frame = build_hyperliquid_position_frame(bundle)
        preferred_portfolio = pd.DataFrame()
        if not portfolio_frame.empty and "周期" in portfolio_frame.columns:
            preferred_portfolio = portfolio_frame[
                portfolio_frame["周期"].astype(str).str.contains("day|1d|24h", case=False, na=False)
            ]
        selected_portfolio_row = (
            preferred_portfolio.iloc[0].to_dict()
            if not preferred_portfolio.empty
            else portfolio_frame.iloc[0].to_dict()
            if not portfolio_frame.empty
            else {}
        )
        rows.append(
            {
                "标签": bundle.get("label"),
                "地址": bundle.get("address"),
                "分组": bundle.get("group"),
                "来源": bundle.get("source"),
                "角色": bundle.get("role") or "user",
                "账户权益": payload_float(bundle.get("account_value")),
                "24hPnL": payload_float(selected_portfolio_row.get("PnL")),
                "成交量": payload_float(selected_portfolio_row.get("成交量")),
                "持仓数": len(position_frame),
                "持仓价值": position_frame["仓位价值"].fillna(0.0).sum() if not position_frame.empty and "仓位价值" in position_frame.columns else 0.0,
                "Funding净额": funding_frame["金额"].fillna(0.0).sum() if not funding_frame.empty and "金额" in funding_frame.columns else 0.0,
                "清算价样本": int(position_frame["清算价"].notna().sum()) if not position_frame.empty and "清算价" in position_frame.columns else 0,
            }
        )
    frame = pd.DataFrame(rows)
    if frame.empty:
        return pd.DataFrame(columns=["标签", "地址", "分组", "来源", "角色", "账户权益", "24hPnL", "成交量", "持仓数", "持仓价值", "Funding净额", "清算价样本"])
    return frame.sort_values(["24hPnL", "账户权益", "持仓价值"], ascending=[False, False, False], na_position="last").reset_index(drop=True)


def build_hyperliquid_watchlist_position_rows(bundles: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for bundle in bundles:
        for position in bundle.get("positions") or []:
            if not isinstance(position, dict):
                continue
            rows.append(
                {
                    **position,
                    "address": bundle.get("address"),
                    "label": bundle.get("label"),
                    "group": bundle.get("group"),
                }
            )
    return rows


def build_hyperliquid_watch_group_frame(bundles: List[Dict[str, Any]]) -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []
    for bundle in bundles:
        group = str(bundle.get("group") or "默认")
        positions_frame = build_hyperliquid_position_frame(bundle)
        funding_frame = build_hyperliquid_funding_frame(bundle)
        rows.append(
            {
                "分组": group,
                "标签": bundle.get("label"),
                "账户权益": payload_float(bundle.get("account_value")),
                "持仓价值": positions_frame["仓位价值"].fillna(0.0).sum() if not positions_frame.empty and "仓位价值" in positions_frame.columns else 0.0,
                "Funding净额": funding_frame["金额"].fillna(0.0).sum() if not funding_frame.empty and "金额" in funding_frame.columns else 0.0,
                "地址数": 1,
            }
        )
    frame = pd.DataFrame(rows)
    if frame.empty:
        return pd.DataFrame(columns=["分组", "地址数", "账户权益", "持仓价值", "Funding净额"])
    grouped = (
        frame.groupby("分组", as_index=False)[["地址数", "账户权益", "持仓价值", "Funding净额"]]
        .sum(min_count=1)
        .sort_values("账户权益", ascending=False, na_position="last")
        .reset_index(drop=True)
    )
    return grouped


def _alert_bias_from_text(title: str, explanation: str) -> int | None:
    text = f"{title} {explanation}".lower()
    bullish_keywords = ["偏多", "现货领先", "空头回补", "看多", "买墙", "多头推进", "吸收", "多头衰竭"]
    bearish_keywords = ["偏空", "流动性塌陷", "看空", "卖墙", "空头推进", "上方卖盘", "多头拥挤", "多头衰竭"]
    bullish_score = sum(1 for keyword in bullish_keywords if keyword in text)
    bearish_score = sum(1 for keyword in bearish_keywords if keyword in text)
    if bullish_score > bearish_score:
        return 1
    if bearish_score > bullish_score:
        return -1
    return None


def build_alert_review_frame(
    alert_history_frame: pd.DataFrame,
    market_history_frame: pd.DataFrame,
    *,
    review_window_minutes: int,
) -> pd.DataFrame:
    if alert_history_frame.empty or market_history_frame.empty:
        return pd.DataFrame(columns=["时间", "交易所", "告警", "预期方向", "窗口收益(%)", "顺风区间(%)", "逆风区间(%)", "命中"])
    alerts = alert_history_frame[alert_history_frame["动作"] == "触发"].copy()
    if alerts.empty:
        return pd.DataFrame(columns=["时间", "交易所", "告警", "预期方向", "窗口收益(%)", "顺风区间(%)", "逆风区间(%)", "命中"])
    market = market_history_frame.copy()
    market["ts_ms"] = (pd.to_datetime(market["时间"]).astype("int64") // 10**6).astype(int)
    alerts["ts_ms"] = (pd.to_datetime(alerts["时间"]).astype("int64") // 10**6).astype(int)
    rows = []
    window_ms = max(1, int(review_window_minutes)) * 60_000
    for row in alerts.to_dict("records"):
        exchange_key = str(row.get("交易所键") or "")
        title = str(row.get("告警") or "")
        explanation = str(row.get("说明") or "")
        bias = _alert_bias_from_text(title, explanation)
        if bias is None:
            continue
        exchange_market = market[market["交易所键"].astype(str) == exchange_key] if exchange_key else market[market["交易所"].astype(str) == str(row.get("交易所") or "")]
        if exchange_market.empty:
            continue
        start_ts = int(row["ts_ms"])
        entry_rows = exchange_market[exchange_market["ts_ms"] >= start_ts].sort_values("ts_ms")
        path_rows = exchange_market[
            (exchange_market["ts_ms"] >= start_ts) & (exchange_market["ts_ms"] <= start_ts + window_ms)
        ].sort_values("ts_ms")
        exit_rows = exchange_market[exchange_market["ts_ms"] >= start_ts + window_ms].sort_values("ts_ms")
        if entry_rows.empty or exit_rows.empty or path_rows.empty:
            continue
        entry_price = payload_float(entry_rows.iloc[0].get("最新价"))
        exit_price = payload_float(exit_rows.iloc[0].get("最新价"))
        if entry_price in (None, 0) or exit_price is None:
            continue
        raw_return_pct = (float(exit_price) - float(entry_price)) / float(entry_price) * 100.0
        path_prices = pd.to_numeric(path_rows["最新价"], errors="coerce").dropna()
        if path_prices.empty:
            continue
        if bias > 0:
            favorable_pct = float((path_prices.max() - float(entry_price)) / float(entry_price) * 100.0)
            adverse_pct = float((path_prices.min() - float(entry_price)) / float(entry_price) * 100.0)
        else:
            favorable_pct = float((float(entry_price) - path_prices.min()) / float(entry_price) * 100.0)
            adverse_pct = float((float(entry_price) - path_prices.max()) / float(entry_price) * 100.0)
        hit = raw_return_pct * bias > 0
        rows.append(
            {
                "时间": row.get("时间"),
                "交易所": row.get("交易所"),
                "告警": title,
                "预期方向": "看多" if bias > 0 else "看空",
                "窗口收益(%)": raw_return_pct,
                "顺风区间(%)": favorable_pct,
                "逆风区间(%)": adverse_pct,
                "命中": "是" if hit else "否",
            }
        )
    frame = pd.DataFrame(rows)
    if frame.empty:
        return pd.DataFrame(columns=["时间", "交易所", "告警", "预期方向", "窗口收益(%)", "顺风区间(%)", "逆风区间(%)", "命中"])
    return frame.sort_values("时间", ascending=False).reset_index(drop=True)


def build_alert_review_summary_frame(alert_review_frame: pd.DataFrame) -> pd.DataFrame:
    if alert_review_frame.empty:
        return pd.DataFrame(columns=["告警", "样本数", "命中率(%)", "平均收益(%)", "中位收益(%)", "平均顺风(%)", "平均逆风(%)"])
    grouped = (
        alert_review_frame.groupby("告警", as_index=False)
        .agg(
            样本数=("命中", "size"),
            命中率=("命中", lambda series: (series == "是").mean() * 100.0),
            平均收益=("窗口收益(%)", "mean"),
            中位收益=("窗口收益(%)", "median"),
            平均顺风=("顺风区间(%)", "mean"),
            平均逆风=("逆风区间(%)", "mean"),
        )
        .sort_values(["命中率", "样本数"], ascending=[False, False])
        .reset_index(drop=True)
    )
    return grouped.rename(
        columns={
            "命中率": "命中率(%)",
            "平均收益": "平均收益(%)",
            "中位收益": "中位收益(%)",
            "平均顺风": "平均顺风(%)",
            "平均逆风": "平均逆风(%)",
        }
    )


def build_alert_review_window_matrix(
    alert_history_frame: pd.DataFrame,
    market_history_frame: pd.DataFrame,
    *,
    review_windows: List[int],
) -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []
    unique_windows = sorted({max(5, int(value)) for value in review_windows if int(value) > 0})
    for review_window in unique_windows:
        review_frame = build_alert_review_frame(
            alert_history_frame,
            market_history_frame,
            review_window_minutes=review_window,
        )
        if review_frame.empty:
            continue
        grouped = (
            review_frame.groupby(["告警", "预期方向"], as_index=False)
            .agg(
                样本数=("命中", "size"),
                命中率=("命中", lambda series: (series == "是").mean() * 100.0),
                平均收益=("窗口收益(%)", "mean"),
                中位收益=("窗口收益(%)", "median"),
                平均顺风=("顺风区间(%)", "mean"),
                平均逆风=("逆风区间(%)", "mean"),
            )
            .sort_values(["命中率", "样本数"], ascending=[False, False])
        )
        for item in grouped.to_dict("records"):
            rows.append(
                {
                    "告警": item.get("告警"),
                    "预期方向": item.get("预期方向"),
                    "窗口(min)": review_window,
                    "样本数": item.get("样本数"),
                    "命中率(%)": item.get("命中率"),
                    "平均收益(%)": item.get("平均收益"),
                    "中位收益(%)": item.get("中位收益"),
                    "平均顺风(%)": item.get("平均顺风"),
                    "平均逆风(%)": item.get("平均逆风"),
                }
            )
    frame = pd.DataFrame(rows)
    if frame.empty:
        return pd.DataFrame(columns=["告警", "预期方向", "窗口(min)", "样本数", "命中率(%)", "平均收益(%)", "中位收益(%)", "平均顺风(%)", "平均逆风(%)"])
    return frame.sort_values(["命中率(%)", "样本数", "平均收益(%)"], ascending=[False, False, False]).reset_index(drop=True)


def build_alert_best_window_frame(alert_window_frame: pd.DataFrame) -> pd.DataFrame:
    if alert_window_frame.empty:
        return pd.DataFrame(columns=["告警", "预期方向", "最佳窗口(min)", "样本数", "最佳命中率(%)", "平均收益(%)", "中位收益(%)", "平均顺风(%)", "平均逆风(%)"])
    best_rows = (
        alert_window_frame.sort_values(
            ["命中率(%)", "平均收益(%)", "样本数", "窗口(min)"],
            ascending=[False, False, False, True],
        )
        .groupby(["告警", "预期方向"], as_index=False)
        .first()
    )
    return best_rows.rename(columns={"窗口(min)": "最佳窗口(min)", "命中率(%)": "最佳命中率(%)"})


def build_history_event_summary_frame(event_history_frame: pd.DataFrame) -> pd.DataFrame:
    if event_history_frame.empty:
        return pd.DataFrame(columns=["类型", "交易所", "事件数", "名义金额", "最近时间"])
    frame = event_history_frame.copy()
    grouped = (
        frame.groupby(["类型", "交易所"], as_index=False)
        .agg(
            事件数=("合约", "size"),
            名义金额=("名义金额", "sum"),
            最近时间=("时间", "max"),
        )
        .sort_values(["事件数", "名义金额"], ascending=[False, False], na_position="last")
        .reset_index(drop=True)
    )
    return grouped


def build_quality_summary_frame(quality_history_frame: pd.DataFrame) -> pd.DataFrame:
    if quality_history_frame.empty:
        return pd.DataFrame(columns=["交易所", "市场", "样本数", "净变化", "假挂单次数", "补单次数", "最近盘口失衡(%)"])
    sorted_frame = quality_history_frame.sort_values("时间")
    latest = (
        sorted_frame.groupby(["交易所", "市场"], as_index=False)
        .last()
        .rename(columns={"盘口失衡(%)": "最近盘口失衡(%)"})
    )
    totals = (
        quality_history_frame.groupby(["交易所", "市场"], as_index=False)
        .agg(
            样本数=("合约", "size"),
            净变化=("净变化", "sum"),
            假挂单次数=("假挂单次数", "sum"),
            补单次数=("补单次数", "sum"),
        )
    )
    return totals.merge(
        latest[["交易所", "市场", "最近盘口失衡(%)"]],
        on=["交易所", "市场"],
        how="left",
    ).sort_values(["样本数", "假挂单次数"], ascending=[False, False], na_position="last").reset_index(drop=True)


def build_alert_catalog_index_frame(alert_history_frame: pd.DataFrame) -> pd.DataFrame:
    if alert_history_frame.empty:
        return pd.DataFrame(columns=["币种", "交易所", "等级", "告警", "触发数", "确认数", "最近时间"])
    frame = alert_history_frame.copy()
    grouped = (
        frame.groupby(["币种", "交易所", "等级", "告警"], as_index=False)
        .agg(
            触发数=("动作", lambda series: int((series.astype(str) == "触发").sum())),
            确认数=("动作", lambda series: int((series.astype(str) == "已确认").sum())),
            最近时间=("时间", "max"),
        )
        .sort_values(["触发数", "确认数", "最近时间"], ascending=[False, False, False], na_position="last")
        .reset_index(drop=True)
    )
    return grouped


def build_market_coverage_index_frame(
    market_history_frame: pd.DataFrame,
    event_history_frame: pd.DataFrame,
    quality_history_frame: pd.DataFrame,
) -> pd.DataFrame:
    if market_history_frame.empty and event_history_frame.empty and quality_history_frame.empty:
        return pd.DataFrame(columns=["币种", "交易所", "市场", "快照数", "事件数", "盘口质量点", "最近快照", "最近事件", "最近质量"])
    snapshot_frame = pd.DataFrame(columns=["币种", "交易所", "市场", "快照数", "最近快照"])
    if not market_history_frame.empty:
        snapshot_frame = (
            market_history_frame.groupby(["币种", "交易所", "市场"], as_index=False)
            .agg(
                快照数=("合约", "size"),
                最近快照=("时间", "max"),
            )
        )
    event_frame = pd.DataFrame(columns=["币种", "交易所", "市场", "事件数", "最近事件"])
    if not event_history_frame.empty:
        event_frame = (
            event_history_frame.groupby(["币种", "交易所", "市场"], as_index=False)
            .agg(
                事件数=("合约", "size"),
                最近事件=("时间", "max"),
            )
        )
    quality_frame = pd.DataFrame(columns=["币种", "交易所", "市场", "盘口质量点", "最近质量"])
    if not quality_history_frame.empty:
        quality_frame = (
            quality_history_frame.groupby(["币种", "交易所", "市场"], as_index=False)
            .agg(
                盘口质量点=("合约", "size"),
                最近质量=("时间", "max"),
            )
        )
    merged = snapshot_frame.merge(event_frame, on=["币种", "交易所", "市场"], how="outer")
    merged = merged.merge(quality_frame, on=["币种", "交易所", "市场"], how="outer")
    for column in ("快照数", "事件数", "盘口质量点"):
        if column in merged.columns:
            merged[column] = merged[column].fillna(0).astype(int)
    return merged.sort_values(["快照数", "事件数", "盘口质量点"], ascending=[False, False, False], na_position="last").reset_index(drop=True)


def build_event_catalog_index_frame(event_history_frame: pd.DataFrame) -> pd.DataFrame:
    if event_history_frame.empty:
        return pd.DataFrame(columns=["币种", "交易所", "类型", "事件数", "名义金额", "最近时间"])
    frame = event_history_frame.copy()
    grouped = (
        frame.groupby(["币种", "交易所", "类型"], as_index=False)
        .agg(
            事件数=("合约", "size"),
            名义金额=("名义金额", "sum"),
            最近时间=("时间", "max"),
        )
        .sort_values(["事件数", "名义金额"], ascending=[False, False], na_position="last")
        .reset_index(drop=True)
    )
    return grouped


def _normalize_for_heatmap(series: pd.Series, *, log_scale: bool = False) -> pd.Series:
    numeric = pd.to_numeric(series, errors="coerce")
    if log_scale:
        numeric = numeric.apply(lambda value: None if pd.isna(value) or float(value) < 0 else float(value))
        numeric = numeric.apply(lambda value: None if value is None else math.log1p(value))
    valid = numeric.dropna()
    if valid.empty:
        return pd.Series([None] * len(series), index=series.index, dtype="float64")
    if len(valid) == 1:
        return pd.Series([0.0 if pd.notna(value) else None for value in numeric], index=series.index, dtype="float64")
    std = float(valid.std(ddof=0))
    if std <= 1e-9:
        return pd.Series([0.0 if pd.notna(value) else None for value in numeric], index=series.index, dtype="float64")
    normalized = (numeric - float(valid.mean())) / std
    return normalized.clip(-2.5, 2.5)


def build_cross_coin_linkage_frame(multi_coin_frame: pd.DataFrame, *, base_coin: str) -> pd.DataFrame:
    if multi_coin_frame.empty:
        return pd.DataFrame(columns=["币种", "价格", "OI 1h(%)", "Funding(bps)", "24h爆仓样本额", "置信度", "Funding偏离(bps)", "OI动量偏离(%)", "联动标签"])
    frame = multi_coin_frame.copy()
    for column in ("价格", "OI 1h(%)", "Funding(bps)", "24h爆仓样本额", "现货/合约成交比", "置信度"):
        if column in frame.columns:
            frame[column] = pd.to_numeric(frame[column], errors="coerce")
    base_row = frame[frame["币种"].astype(str).str.upper() == str(base_coin or "").upper()]
    base_funding = payload_float(base_row.iloc[0].get("Funding(bps)")) if not base_row.empty else None
    base_oi_change = payload_float(base_row.iloc[0].get("OI 1h(%)")) if not base_row.empty else None
    liq_median = float(frame["24h爆仓样本额"].fillna(0.0).median()) if "24h爆仓样本额" in frame.columns else 0.0
    labels: List[str] = []
    for row in frame.to_dict("records"):
        oi_change = payload_float(row.get("OI 1h(%)"))
        funding_bps = payload_float(row.get("Funding(bps)"))
        liq_notional = payload_float(row.get("24h爆仓样本额"))
        spot_perp_ratio = payload_float(row.get("现货/合约成交比"))
        if liq_notional is not None and liq_notional > max(liq_median * 1.45, 1.0):
            labels.append("爆仓传导")
        elif oi_change is not None and oi_change >= 3.0 and funding_bps is not None and funding_bps >= 8.0:
            labels.append("多头拥挤")
        elif oi_change is not None and oi_change <= -3.0 and funding_bps is not None and funding_bps <= -8.0:
            labels.append("空头挤压")
        elif spot_perp_ratio is not None and spot_perp_ratio >= 1.05:
            labels.append("现货推动")
        else:
            labels.append("中性")
    frame["Funding偏离(bps)"] = [
        None if base_funding is None or payload_float(value) is None else float(payload_float(value) or 0.0) - base_funding
        for value in frame.get("Funding(bps)", pd.Series(dtype="float64"))
    ]
    frame["OI动量偏离(%)"] = [
        None if base_oi_change is None or payload_float(value) is None else float(payload_float(value) or 0.0) - base_oi_change
        for value in frame.get("OI 1h(%)", pd.Series(dtype="float64"))
    ]
    frame["联动标签"] = labels
    preferred_columns = [
        "币种",
        "价格",
        "OI 1h(%)",
        "Funding(bps)",
        "24h爆仓样本额",
        "置信度",
        "Funding偏离(bps)",
        "OI动量偏离(%)",
        "联动标签",
    ]
    return frame[[column for column in preferred_columns if column in frame.columns]]


def build_cross_coin_linkage_heatmap_figure(multi_coin_frame: pd.DataFrame) -> go.Figure:
    figure = go.Figure()
    if multi_coin_frame.empty or "币种" not in multi_coin_frame.columns:
        figure.add_annotation(text="等待跨币种联动样本", showarrow=False, x=0.5, y=0.5, xref="paper", yref="paper")
        figure.update_layout(height=320, margin=dict(l=12, r=12, t=24, b=12))
        return figure
    frame = multi_coin_frame.copy()
    metrics = [
        ("OI 1h(%)", False),
        ("OI 24h(%)", False),
        ("Funding(bps)", False),
        ("24h爆仓样本额", True),
        ("现货/合约成交比", False),
        ("置信度", False),
    ]
    heatmap_rows: List[pd.Series] = []
    metric_labels: List[str] = []
    for column, log_scale in metrics:
        if column not in frame.columns:
            continue
        metric_labels.append(column)
        heatmap_rows.append(_normalize_for_heatmap(frame[column], log_scale=log_scale))
    if not heatmap_rows:
        figure.add_annotation(text="等待跨币种联动样本", showarrow=False, x=0.5, y=0.5, xref="paper", yref="paper")
        figure.update_layout(height=320, margin=dict(l=12, r=12, t=24, b=12))
        return figure
    heatmap_frame = pd.DataFrame(heatmap_rows, index=metric_labels, columns=frame["币种"].astype(str))
    figure.add_trace(
        go.Heatmap(
            z=heatmap_frame.values,
            x=list(heatmap_frame.columns),
            y=list(heatmap_frame.index),
            colorscale="RdBu",
            zmid=0.0,
            colorbar=dict(title="相对强度"),
            hovertemplate="币种 %{x}<br>指标 %{y}<br>相对强度 %{z:.2f}<extra></extra>",
        )
    )
    figure.update_layout(
        height=340,
        margin=dict(l=12, r=12, t=58, b=12),
        paper_bgcolor="rgba(14, 22, 35, 0.56)",
        plot_bgcolor="rgba(255, 255, 255, 0.045)",
        font=dict(color="#f6f9ff", family="SF Pro Display, Segoe UI, sans-serif"),
        title=dict(text="Cross-Coin Linkage Heatmap", x=0.03, y=0.98, xanchor="left", font=dict(size=18, color="#f7fbff")),
    )
    figure.update_xaxes(showgrid=False)
    figure.update_yaxes(showgrid=False)
    return figure


def build_cross_coin_positioning_figure(multi_coin_frame: pd.DataFrame) -> go.Figure:
    figure = go.Figure()
    if multi_coin_frame.empty or "币种" not in multi_coin_frame.columns:
        figure.add_annotation(text="等待跨币种定位样本", showarrow=False, x=0.5, y=0.5, xref="paper", yref="paper")
        figure.update_layout(height=320, margin=dict(l=12, r=12, t=24, b=12))
        return figure
    frame = multi_coin_frame.copy()
    frame["Funding(bps)"] = pd.to_numeric(frame.get("Funding(bps)"), errors="coerce")
    frame["OI 1h(%)"] = pd.to_numeric(frame.get("OI 1h(%)"), errors="coerce")
    frame["24h爆仓样本额"] = pd.to_numeric(frame.get("24h爆仓样本额"), errors="coerce").fillna(0.0)
    frame["价格"] = pd.to_numeric(frame.get("价格"), errors="coerce")
    frame["置信度"] = pd.to_numeric(frame.get("置信度"), errors="coerce")
    if frame[["Funding(bps)", "OI 1h(%)"]].dropna(how="all").empty:
        figure.add_annotation(text="等待跨币种定位样本", showarrow=False, x=0.5, y=0.5, xref="paper", yref="paper")
        figure.update_layout(height=320, margin=dict(l=12, r=12, t=24, b=12))
        return figure
    max_liq = float(frame["24h爆仓样本额"].max()) if not frame.empty else 0.0
    size_values = [
        18.0 + (float(value) / max_liq * 26.0 if max_liq > 0 else 6.0)
        for value in frame["24h爆仓样本额"].fillna(0.0)
    ]
    customdata = frame[["24h爆仓样本额", "价格"]].fillna(0.0).to_numpy()
    figure.add_trace(
        go.Scatter(
            x=frame["Funding(bps)"],
            y=frame["OI 1h(%)"],
            mode="markers+text",
            text=frame["币种"],
            textposition="top center",
            marker=dict(
                size=size_values,
                color=frame["置信度"],
                colorscale="Viridis",
                cmin=0.0,
                cmax=100.0,
                line=dict(width=1, color="rgba(255,255,255,0.35)"),
                colorbar=dict(title="置信度"),
                opacity=0.85,
            ),
            customdata=customdata,
            hovertemplate="币种 %{text}<br>Funding %{x:.2f} bps<br>OI 1h %{y:.2f}%<br>24h爆仓样本额 %{customdata[0]:,.0f}<br>价格 %{customdata[1]:,.2f}<extra></extra>",
        )
    )
    figure.add_hline(y=0, line_dash="dot", line_color="rgba(255,255,255,0.28)")
    figure.add_vline(x=0, line_dash="dot", line_color="rgba(255,255,255,0.28)")
    figure.update_layout(
        height=340,
        margin=dict(l=12, r=12, t=58, b=12),
        paper_bgcolor="rgba(14, 22, 35, 0.56)",
        plot_bgcolor="rgba(255, 255, 255, 0.045)",
        font=dict(color="#f6f9ff", family="SF Pro Display, Segoe UI, sans-serif"),
        title=dict(text="Cross-Coin Positioning Map", x=0.03, y=0.98, xanchor="left", font=dict(size=18, color="#f7fbff")),
        xaxis_title="Funding (bps)",
        yaxis_title="OI 1h (%)",
    )
    figure.update_xaxes(showgrid=True, gridcolor="rgba(255,255,255,0.08)")
    figure.update_yaxes(showgrid=True, gridcolor="rgba(255,255,255,0.08)")
    return figure


def collect_new_alert_notifications(timeline_frame: pd.DataFrame, *, base_coin: str, limit: int = 6) -> List[Dict[str, Any]]:
    if timeline_frame.empty:
        return []
    seen = st.session_state.get("_alert_notification_seen_keys")
    seen_keys = set(seen) if isinstance(seen, list) else set()
    rows: List[Dict[str, Any]] = []
    ordered = timeline_frame.sort_values("时间")
    for item in ordered.to_dict("records"):
        if str(item.get("动作") or "") != "触发":
            continue
        timestamp = pd.to_datetime(item.get("时间"))
        timestamp_text = timestamp.isoformat()
        key = f"{timestamp_text}::{item.get('交易所')}::{item.get('告警')}::{item.get('动作')}"
        if key in seen_keys:
            continue
        seen_keys.add(key)
        rows.append(
            {
                "timestamp_ms": int(timestamp.timestamp() * 1000),
                "exchange": item.get("交易所"),
                "level": item.get("等级"),
                "alert": item.get("告警"),
                "title": f"{base_coin} | {item.get('交易所')} | {item.get('等级')}",
                "body": f"{item.get('告警')} | {item.get('说明')}",
            }
        )
    st.session_state["_alert_notification_seen_keys"] = list(seen_keys)[-500:]
    return rows[-limit:]


def route_alert_notifications(
    messages: List[Dict[str, Any]],
    *,
    channel: str,
    min_level: str,
    cooldown_minutes: int,
) -> List[Dict[str, Any]]:
    if not messages:
        return []
    routed: List[Dict[str, Any]] = []
    min_rank = alert_level_rank(min_level)
    cooldown_ms = max(0, int(cooldown_minutes)) * 60_000
    state_key = f"_alert_channel_last_sent::{channel}"
    last_sent = st.session_state.get(state_key)
    last_sent_map = dict(last_sent) if isinstance(last_sent, dict) else {}
    for item in messages:
        level_rank = alert_level_rank(str(item.get("level") or ""))
        if level_rank < min_rank:
            continue
        dedupe_key = f"{item.get('exchange')}::{item.get('alert')}"
        timestamp_ms = int(item.get("timestamp_ms") or 0)
        previous_ts = int(last_sent_map.get(dedupe_key) or 0)
        if cooldown_ms > 0 and previous_ts > 0 and timestamp_ms - previous_ts < cooldown_ms:
            continue
        last_sent_map[dedupe_key] = timestamp_ms
        routed.append(item)
    st.session_state[state_key] = last_sent_map
    return routed


def emit_browser_notifications(messages: List[Dict[str, str]], *, enable_sound: bool) -> None:
    if not messages:
        return
    script_payload = json.dumps(messages, ensure_ascii=True)
    sound_flag = "true" if enable_sound else "false"
    components.html(
        f"""
        <script>
        const messages = {script_payload};
        const enableSound = {sound_flag};
        const beep = () => {{
          if (!enableSound || !window.AudioContext) return;
          const ctx = new AudioContext();
          const oscillator = ctx.createOscillator();
          const gain = ctx.createGain();
          oscillator.type = 'sine';
          oscillator.frequency.value = 880;
          gain.gain.value = 0.03;
          oscillator.connect(gain);
          gain.connect(ctx.destination);
          oscillator.start();
          oscillator.stop(ctx.currentTime + 0.12);
        }};
        const notify = async () => {{
          if (!('Notification' in window)) return;
          if (Notification.permission === 'default') {{
            try {{ await Notification.requestPermission(); }} catch (e) {{}}
          }}
          if (Notification.permission !== 'granted') return;
          for (const item of messages) {{
            new Notification(item.title || 'Alert', {{ body: item.body || '' }});
          }}
          beep();
        }};
        notify();
        </script>
        """,
        height=0,
        width=0,
    )


def emit_telegram_notifications(messages: List[Dict[str, str]], *, token: str, chat_id: str, timeout: int) -> List[str]:
    if not messages or not token or not chat_id:
        return []
    errors: List[str] = []
    endpoint = f"https://api.telegram.org/bot{token}/sendMessage"
    for item in messages:
        try:
            response = requests.post(
                endpoint,
                json={"chat_id": chat_id, "text": f"{item.get('title')}\n{item.get('body')}", "disable_web_page_preview": True},
                timeout=timeout,
            )
            response.raise_for_status()
        except Exception as exc:
            errors.append(str(exc))
    return errors


def maybe_run_auto_archive(store: TerminalHistoryStore, *, enabled: bool, retention_days: int) -> List[Dict[str, Any]]:
    if not enabled:
        return []
    now_s = time.time()
    last_run = float(st.session_state.get("_history_archive_last_run", 0.0) or 0.0)
    if now_s - last_run < 900:
        return list(st.session_state.get("_history_archive_last_result") or [])
    cutoff_ms = int((pd.Timestamp.now() - pd.Timedelta(days=max(1, retention_days))).timestamp() * 1000)
    try:
        result = store.archive_before(cutoff_ms, prefer_parquet=True)
    except Exception:
        result = []
    st.session_state["_history_archive_last_run"] = now_s
    st.session_state["_history_archive_last_result"] = result
    return result


def _is_timestamp_fresh(timestamp_ms: int | None, max_age_ms: int) -> bool:
    if timestamp_ms is None:
        return False
    return abs(int(time.time() * 1000) - int(timestamp_ms)) <= max_age_ms


def _latest_trade_timestamp(events: List[TradeEvent]) -> int | None:
    if not events:
        return None
    return max((event.timestamp_ms for event in events if event.timestamp_ms is not None), default=None)


def _use_live_snapshot(snapshot: SpotSnapshot | None, max_age_ms: int = 20_000) -> bool:
    return snapshot is not None and snapshot.status == "ok" and _is_timestamp_fresh(snapshot.timestamp_ms, max_age_ms)


def _use_live_orderbook(orderbook: List[OrderBookLevel] | None, reference_timestamp_ms: int | None, max_age_ms: int = 20_000) -> bool:
    return bool(orderbook) and _is_timestamp_fresh(reference_timestamp_ms, max_age_ms)


def _use_live_trades(trades: List[TradeEvent] | None, max_age_ms: int = 20_000) -> bool:
    if not trades:
        return False
    return _is_timestamp_fresh(_latest_trade_timestamp(trades), max_age_ms)


def service_profile(mode: str) -> Dict[str, int]:
    return dict(PERFORMANCE_PROFILES.get(mode, PERFORMANCE_PROFILES["标准"]))


def build_refresh_profile(
    base_refresh_seconds: int,
    *,
    active_view: str,
    overview_mode: str | None = None,
    lab_mode: str | None = None,
) -> Dict[str, int]:
    base_refresh = max(1, int(base_refresh_seconds))
    profile = {
        "fast_ttl": max(2, base_refresh),
        "balanced_ttl": max(4, base_refresh),
        "heavy_ttl": max(12, base_refresh * 3),
        "stale_ttl": max(120, base_refresh * 30),
        "prefetch_age": max(60, base_refresh * 15),
    }
    if active_view == "首页总览" and overview_mode == "完整":
        profile.update(
            {
                "balanced_ttl": max(6, base_refresh * 2),
                "heavy_ttl": max(16, base_refresh * 4),
                "stale_ttl": max(150, base_refresh * 36),
                "prefetch_age": max(75, base_refresh * 18),
            }
        )
    elif active_view == "增强实验室":
        profile.update(
            {
                "balanced_ttl": max(8, base_refresh * 2),
                "heavy_ttl": max(20, base_refresh * 5),
                "stale_ttl": max(180, base_refresh * 45),
                "prefetch_age": max(90, base_refresh * 20),
            }
        )
        if lab_mode == "通知与持久化":
            profile["heavy_ttl"] = max(profile["heavy_ttl"], 24)
    elif active_view in {"交易台深度页", "盘口中心"}:
        profile.update(
            {
                "balanced_ttl": max(4, base_refresh),
                "heavy_ttl": max(10, base_refresh * 2),
                "prefetch_age": max(45, base_refresh * 12),
            }
        )
    return profile


def _latency_ms(timestamp_ms: int | None) -> int | None:
    if timestamp_ms is None:
        return None
    return max(0, int(time.time() * 1000) - int(timestamp_ms))


def _format_latency(timestamp_ms: int | None) -> str:
    latency_ms = _latency_ms(timestamp_ms)
    if latency_ms is None:
        return "延迟 -"
    if latency_ms < 1_000:
        return f"延迟 {latency_ms} ms"
    if latency_ms < 60_000:
        return f"延迟 {latency_ms / 1_000.0:.1f} s"
    return f"延迟 {latency_ms / 60_000.0:.1f} m"


def _confidence_label(score: float | None) -> str:
    value = 0.0 if score is None else float(score)
    if value >= 0.72:
        return "高置信"
    if value >= 0.42:
        return "中置信"
    return "低置信"


def _sync_status_label(sync_state: str | None) -> str:
    mapping = {
        "synced": "序列同步正常",
        "bootstrapping": "快照回补中",
        "degraded": "序列待修复",
        "waiting": "等待首包",
    }
    return mapping.get(str(sync_state or ""), str(sync_state or ""))


def _build_state_caption(
    source: str,
    timestamp_ms: int | None,
    *,
    sync_state: str | None = None,
    sample_count: int | None = None,
    min_samples: int | None = None,
    confidence: str | None = None,
) -> str:
    parts = [source]
    if sync_state:
        parts.append(_sync_status_label(sync_state))
    parts.append(_format_latency(timestamp_ms))
    if sample_count is not None and min_samples is not None and sample_count < min_samples:
        parts.append("样本不足")
    if confidence:
        parts.append(confidence)
    return " | ".join(parts)


def _join_caption_parts(*parts: str | None) -> str:
    return " · ".join(str(part) for part in parts if part)


def _riskmap_confidence_score(
    snapshot: ExchangeSnapshot,
    candles: List[Candle],
    orderbook: List[OrderBookLevel],
    sync_state: str | None,
) -> float:
    score = 0.0
    if len(candles) >= 120:
        score += 0.34
    elif len(candles) >= 60:
        score += 0.24
    elif len(candles) >= 24:
        score += 0.12
    if len(orderbook) >= 120:
        score += 0.34
    elif len(orderbook) >= 60:
        score += 0.24
    elif len(orderbook) >= 24:
        score += 0.12
    if snapshot.open_interest_notional is not None or snapshot.open_interest is not None:
        score += 0.2
    if sync_state == "synced":
        score += 0.12
    elif sync_state == "bootstrapping":
        score += 0.05
    return min(1.0, score)


def _orderbook_quality_confidence_score(history: List[Any], sync_state: str | None) -> float:
    score = 0.0
    if sync_state == "synced":
        score += 0.45
    elif sync_state == "bootstrapping":
        score += 0.18
    sample_count = len(history)
    if sample_count >= 24:
        score += 0.35
    elif sample_count >= 12:
        score += 0.24
    elif sample_count >= 6:
        score += 0.12
    if history:
        latest = history[-1]
        if getattr(latest, "best_bid", None) is not None and getattr(latest, "best_ask", None) is not None:
            score += 0.12
    return min(1.0, score)


def _oi_source_label(backfill: List[OIPoint], session_points: List[OIPoint]) -> str:
    if backfill and session_points:
        return "会话采样 + REST回补"
    if session_points:
        return "会话采样"
    if backfill:
        return "REST回补"
    return "样本不足"


def _derived_cache_store() -> Dict[str, Dict[str, Any]]:
    return st.session_state.setdefault("_derived_result_cache", {})


_STALE_RESULT_CACHE: Dict[str, Dict[str, Any]] = {}
_STALE_RESULT_LOCK = threading.Lock()
_LOCAL_TTL_CACHE: Dict[str, Dict[str, Any]] = {}
_LOCAL_TTL_CACHE_LOCK = threading.Lock()
_LOCAL_TTL_LAST_PRUNE_AT = 0.0
_HISTORY_WRITE_PENDING: Dict[str, Callable[[], None]] = {}
_HISTORY_WRITE_LOCK = threading.Lock()
_HISTORY_WRITE_STARTED = False


def _history_write_worker() -> None:
    while True:
        time.sleep(2.0)
        with _HISTORY_WRITE_LOCK:
            pending_callbacks = list(_HISTORY_WRITE_PENDING.values())
            _HISTORY_WRITE_PENDING.clear()
        for callback in pending_callbacks:
            try:
                callback()
            except Exception:
                continue


def ensure_history_write_worker() -> None:
    global _HISTORY_WRITE_STARTED
    with _HISTORY_WRITE_LOCK:
        if _HISTORY_WRITE_STARTED:
            return
        _HISTORY_WRITE_STARTED = True
    thread = threading.Thread(target=_history_write_worker, daemon=True, name="history-write-worker")
    thread.start()


def schedule_history_write(write_key: str, callback: Callable[[], None]) -> None:
    ensure_history_write_worker()
    with _HISTORY_WRITE_LOCK:
        _HISTORY_WRITE_PENDING[write_key] = callback


def clear_local_runtime_caches() -> None:
    cache = _derived_cache_store()
    cache.clear()
    with _STALE_RESULT_LOCK:
        _STALE_RESULT_CACHE.clear()
    with _LOCAL_TTL_CACHE_LOCK:
        _LOCAL_TTL_CACHE.clear()


def _schedule_stale_result_refresh(
    cache_key: str,
    signature: Tuple[Any, ...],
    builder: Callable[[], Any],
) -> None:
    def _runner() -> None:
        try:
            value = builder()
            with _STALE_RESULT_LOCK:
                entry = _STALE_RESULT_CACHE.get(cache_key, {})
                entry.update(
                    {
                        "signature": signature,
                        "created_at": time.time(),
                        "value": value,
                        "refreshing": False,
                    }
                )
                _STALE_RESULT_CACHE[cache_key] = entry
        except Exception:
            with _STALE_RESULT_LOCK:
                entry = _STALE_RESULT_CACHE.get(cache_key, {})
                entry["refreshing"] = False
                _STALE_RESULT_CACHE[cache_key] = entry

    with _STALE_RESULT_LOCK:
        entry = _STALE_RESULT_CACHE.setdefault(cache_key, {})
        if entry.get("refreshing") and entry.get("signature") == signature:
            return
        entry["refreshing"] = True
        entry["signature"] = signature
    thread = threading.Thread(target=_runner, daemon=True, name=f"stale-refresh::{cache_key}")
    thread.start()


def get_stale_while_revalidate_result(
    cache_key: str,
    signature: Tuple[Any, ...],
    ttl_seconds: int,
    stale_ttl_seconds: int,
    builder: Callable[[], Any],
):
    now = time.time()
    with _STALE_RESULT_LOCK:
        entry = _STALE_RESULT_CACHE.get(cache_key)
    if entry and entry.get("signature") == signature:
        age_seconds = now - float(entry.get("created_at", 0.0))
        if age_seconds <= ttl_seconds:
            return entry.get("value")
        if age_seconds <= stale_ttl_seconds:
            _schedule_stale_result_refresh(cache_key, signature, builder)
            return entry.get("value")
    value = builder()
    with _STALE_RESULT_LOCK:
        _STALE_RESULT_CACHE[cache_key] = {
            "signature": signature,
            "created_at": now,
            "value": value,
            "refreshing": False,
        }
    return value


def prefetch_stale_result(
    cache_key: str,
    signature: Tuple[Any, ...],
    builder: Callable[[], Any],
    *,
    max_age_seconds: int = 60,
) -> None:
    now = time.time()
    with _STALE_RESULT_LOCK:
        entry = _STALE_RESULT_CACHE.get(cache_key)
    if entry and entry.get("signature") == signature and now - float(entry.get("created_at", 0.0)) <= max_age_seconds:
        return
    _schedule_stale_result_refresh(cache_key, signature, builder)


def get_cached_derived_result(
    cache_key: str,
    signature: Tuple[Any, ...],
    ttl_seconds: int,
    builder: Callable[[], Any],
):
    cache = _derived_cache_store()
    now = time.time()
    entry = cache.get(cache_key)
    if entry and entry.get("signature") == signature and now - float(entry.get("created_at", 0.0)) <= ttl_seconds:
        return entry.get("value")
    value = builder()
    cache[cache_key] = {"signature": signature, "created_at": now, "value": value}
    return value


def get_local_ttl_result(cache_key: str, ttl_seconds: int, builder: Callable[[], Any]):
    global _LOCAL_TTL_LAST_PRUNE_AT
    now = time.time()
    with _LOCAL_TTL_CACHE_LOCK:
        if _LOCAL_TTL_CACHE and (
            now - _LOCAL_TTL_LAST_PRUNE_AT >= 60.0 or len(_LOCAL_TTL_CACHE) >= 2048
        ):
            cutoff = now - max(float(ttl_seconds or 0), 1.0)
            stale_keys = [
                key
                for key, value in _LOCAL_TTL_CACHE.items()
                if cutoff > float(value.get("created_at", 0.0))
            ]
            for key in stale_keys:
                _LOCAL_TTL_CACHE.pop(key, None)
            _LOCAL_TTL_LAST_PRUNE_AT = now
        entry = _LOCAL_TTL_CACHE.get(cache_key)
        if entry and now - float(entry.get("created_at", 0.0)) <= float(ttl_seconds):
            return entry.get("value")
    value = builder()
    with _LOCAL_TTL_CACHE_LOCK:
        _LOCAL_TTL_CACHE[cache_key] = {"created_at": now, "value": value}
    return value


def _subset_existing_columns(frame: pd.DataFrame, columns: List[str]) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame(columns=columns)
    available_columns = [column for column in columns if column in frame.columns]
    if not available_columns:
        return pd.DataFrame(columns=columns)
    return frame[available_columns].copy()


def _concat_non_empty_frames(frames: List[pd.DataFrame]) -> pd.DataFrame:
    valid_frames: List[pd.DataFrame] = []
    for frame in frames:
        if frame.empty:
            continue
        cleaned = frame.dropna(axis=1, how="all").copy()
        if cleaned.empty or cleaned.shape[1] == 0:
            continue
        valid_frames.append(cleaned)
    if not valid_frames:
        return pd.DataFrame()
    return pd.concat(valid_frames, ignore_index=True, sort=False)


def _confidence_score_from_label(label: Any) -> float:
    label_text = str(label or "").strip()
    if label_text == "高置信":
        return 0.82
    if label_text == "中置信":
        return 0.56
    if label_text == "低置信":
        return 0.24
    return 0.0


def build_spot_perp_decision_frame(
    spot_exchange_frame: pd.DataFrame,
    lead_lag_frame: pd.DataFrame,
    contract_sentiment_frame: pd.DataFrame,
    perp_crowding_trio_frame: pd.DataFrame,
    funding_regime_frame: pd.DataFrame,
) -> pd.DataFrame:
    exchange_names = sorted(
        set(spot_exchange_frame.get("交易所", pd.Series(dtype=str)).dropna().astype(str))
        | set(lead_lag_frame.get("交易所", pd.Series(dtype=str)).dropna().astype(str))
        | set(contract_sentiment_frame.get("交易所", pd.Series(dtype=str)).dropna().astype(str))
        | set(perp_crowding_trio_frame.get("交易所", pd.Series(dtype=str)).dropna().astype(str))
        | set(funding_regime_frame.get("交易所", pd.Series(dtype=str)).dropna().astype(str))
    )
    rows: List[Dict[str, Any]] = []
    for exchange_name in exchange_names:
        spot_row = (
            spot_exchange_frame[spot_exchange_frame["交易所"].astype(str) == exchange_name].iloc[0].to_dict()
            if not spot_exchange_frame.empty and "交易所" in spot_exchange_frame.columns and exchange_name in set(spot_exchange_frame["交易所"].astype(str))
            else {}
        )
        lead_row = (
            lead_lag_frame[lead_lag_frame["交易所"].astype(str) == exchange_name].iloc[0].to_dict()
            if not lead_lag_frame.empty and "交易所" in lead_lag_frame.columns and exchange_name in set(lead_lag_frame["交易所"].astype(str))
            else {}
        )
        sentiment_row = (
            contract_sentiment_frame[contract_sentiment_frame["交易所"].astype(str) == exchange_name].iloc[0].to_dict()
            if not contract_sentiment_frame.empty and "交易所" in contract_sentiment_frame.columns and exchange_name in set(contract_sentiment_frame["交易所"].astype(str))
            else {}
        )
        crowd_row = (
            perp_crowding_trio_frame[perp_crowding_trio_frame["交易所"].astype(str) == exchange_name].iloc[0].to_dict()
            if not perp_crowding_trio_frame.empty and "交易所" in perp_crowding_trio_frame.columns and exchange_name in set(perp_crowding_trio_frame["交易所"].astype(str))
            else {}
        )
        funding_row = (
            funding_regime_frame[funding_regime_frame["交易所"].astype(str) == exchange_name].iloc[0].to_dict()
            if not funding_regime_frame.empty and "交易所" in funding_regime_frame.columns and exchange_name in set(funding_regime_frame["交易所"].astype(str))
            else {}
        )
        spot_buy_ratio = payload_float(spot_row.get("现货主动买占比(%)"))
        basis_pct = payload_float(spot_row.get("Basis(%)") or funding_row.get("Basis(%)"))
        volume_ratio = payload_float(spot_row.get("永续/现货成交额比"))
        lead_summary = str(lead_row.get("提示") or "").strip()
        lead_seconds = payload_float(lead_row.get("领先秒数"))
        lead_correlation = payload_float(lead_row.get("相关性"))
        lead_confidence = _confidence_score_from_label(lead_row.get("置信度"))
        contract_ratio = payload_float(sentiment_row.get("合约多空比") or crowd_row.get("合约多空比"))
        oi_change_pct = payload_float(crowd_row.get("OI变化(%)"))
        taker_buy_ratio = payload_float(crowd_row.get("主动流买占比(%)") or sentiment_row.get("主动流买占比(%)"))
        funding_bps = payload_float(funding_row.get("Funding(bps)") or sentiment_row.get("资金费率(bps)"))

        regime = "混合观察"
        action = "等待更清晰的现货/合约联动"
        if "现货领先" in lead_summary:
            regime = "现货先动"
            action = "盯合约是否跟价放量"
        elif "合约领先" in lead_summary or "永续领先" in lead_summary:
            regime = "合约抢跑"
            action = "防现货不跟的回撤"
        elif volume_ratio is not None and volume_ratio >= 1.6 and taker_buy_ratio is not None and abs(taker_buy_ratio - 50.0) >= 8.0:
            regime = "合约追价"
            action = "结合 Basis / Funding 判断是否挤仓"
        elif spot_buy_ratio is not None and abs(spot_buy_ratio - 50.0) >= 8.0 and (volume_ratio is None or volume_ratio < 1.35):
            regime = "现货主导"
            action = "优先看现货扫单持续性"
        elif contract_ratio is not None and oi_change_pct is not None and funding_bps is not None and contract_ratio > 1.08 and oi_change_pct > 0 and funding_bps > 0:
            regime = "偏多拥挤"
            action = "防 short squeeze 后衰竭"
        elif contract_ratio is not None and oi_change_pct is not None and funding_bps is not None and contract_ratio < 0.92 and oi_change_pct > 0 and funding_bps < 0:
            regime = "偏空拥挤"
            action = "防 long flush 扩大"

        confidence_score = 0.0
        confidence_score += 0.34 * lead_confidence
        confidence_score += 0.12 if spot_buy_ratio is not None else 0.0
        confidence_score += 0.12 if taker_buy_ratio is not None else 0.0
        confidence_score += 0.12 if oi_change_pct is not None else 0.0
        confidence_score += 0.1 if funding_bps is not None else 0.0
        confidence_score += 0.08 if basis_pct is not None else 0.0
        confidence_score += 0.06 if volume_ratio is not None else 0.0
        if lead_correlation is not None and abs(float(lead_correlation)) >= 0.28:
            confidence_score += 0.08
        confidence_score = min(1.0, confidence_score)
        explanation_parts = []
        if spot_buy_ratio is not None:
            explanation_parts.append(f"现货主动买 {spot_buy_ratio:.1f}%")
        if taker_buy_ratio is not None:
            explanation_parts.append(f"合约主动买 {taker_buy_ratio:.1f}%")
        if oi_change_pct is not None:
            explanation_parts.append(f"OI变化 {oi_change_pct:.2f}%")
        if funding_bps is not None:
            explanation_parts.append(f"Funding {funding_bps:.2f}bps")
        if lead_summary:
            explanation_parts.append(lead_summary)
        rows.append(
            {
                "交易所": exchange_name,
                "主导判断": regime,
                "建议动作": action,
                "Lead/Lag": lead_summary or "样本不足",
                "领先秒数": lead_seconds,
                "相关性": lead_correlation,
                "现货主动买占比(%)": spot_buy_ratio,
                "合约主动买占比(%)": taker_buy_ratio,
                "合约多空比": contract_ratio,
                "OI变化(%)": oi_change_pct,
                "Funding(bps)": funding_bps,
                "Basis(%)": basis_pct,
                "永续/现货成交额比": volume_ratio,
                "置信度": _confidence_label(confidence_score),
                "说明": " | ".join(explanation_parts[:5]) if explanation_parts else "样本不足",
            }
        )
    frame = pd.DataFrame(rows)
    if frame.empty:
        return pd.DataFrame(columns=["交易所", "主导判断", "建议动作", "Lead/Lag", "领先秒数", "相关性", "现货主动买占比(%)", "合约主动买占比(%)", "合约多空比", "OI变化(%)", "Funding(bps)", "Basis(%)", "永续/现货成交额比", "置信度", "说明"])
    return frame.sort_values(by=["置信度", "交易所"], ascending=[False, True], na_position="last").reset_index(drop=True)


def build_unified_signal_frame(
    decision_frame: pd.DataFrame,
    sentiment_alert_frame: pd.DataFrame,
    confirmed_alert_frame: pd.DataFrame,
    share_dynamics_frame: pd.DataFrame,
    risk_buffer_frame: pd.DataFrame,
) -> pd.DataFrame:
    exchange_names = sorted(
        set(decision_frame.get("交易所", pd.Series(dtype=str)).dropna().astype(str))
        | set(share_dynamics_frame.get("交易所", pd.Series(dtype=str)).dropna().astype(str))
        | set(risk_buffer_frame.get("交易所", pd.Series(dtype=str)).dropna().astype(str))
    )
    rows: List[Dict[str, Any]] = []
    for exchange_name in exchange_names:
        decision_row = (
            decision_frame[decision_frame["交易所"].astype(str) == exchange_name].iloc[0].to_dict()
            if not decision_frame.empty and "交易所" in decision_frame.columns and exchange_name in set(decision_frame["交易所"].astype(str))
            else {}
        )
        share_row = (
            share_dynamics_frame[share_dynamics_frame["交易所"].astype(str) == exchange_name].iloc[0].to_dict()
            if not share_dynamics_frame.empty and "交易所" in share_dynamics_frame.columns and exchange_name in set(share_dynamics_frame["交易所"].astype(str))
            else {}
        )
        risk_row = (
            risk_buffer_frame[risk_buffer_frame["交易所"].astype(str) == exchange_name].iloc[0].to_dict()
            if not risk_buffer_frame.empty and "交易所" in risk_buffer_frame.columns and exchange_name in set(risk_buffer_frame["交易所"].astype(str))
            else {}
        )
        strong_alerts = 0
        if not confirmed_alert_frame.empty and "交易所" in confirmed_alert_frame.columns:
            strong_alerts = int(
                confirmed_alert_frame[
                    (confirmed_alert_frame["交易所"].astype(str) == exchange_name)
                    & (confirmed_alert_frame["等级"].astype(str) == "强")
                ].shape[0]
            )
        sentiment_alerts = 0
        if not sentiment_alert_frame.empty and "交易所" in sentiment_alert_frame.columns:
            sentiment_alerts = int(sentiment_alert_frame[sentiment_alert_frame["交易所"].astype(str) == exchange_name].shape[0])
        confidence_score = _confidence_score_from_label(decision_row.get("置信度"))
        oi_share_delta = payload_float(share_row.get("OI份额Δ(%)"))
        perp_share_delta = payload_float(share_row.get("合约成交份额Δ(%)"))
        spot_share_delta = payload_float(share_row.get("现货成交份额Δ(%)"))
        risk_ratio = payload_float(risk_row.get("OI/24h成交比"))
        regime = str(decision_row.get("主导判断") or "混合观察")
        direction = "中性"
        if regime in {"现货先动", "现货主导", "偏多拥挤"}:
            direction = "偏多"
        elif regime in {"合约抢跑", "偏空拥挤"}:
            direction = "偏空"
        if payload_float(decision_row.get("合约多空比")) is not None:
            ratio_value = float(payload_float(decision_row.get("合约多空比")) or 1.0)
            if ratio_value > 1.06:
                direction = "偏多"
            elif ratio_value < 0.94:
                direction = "偏空"
        total_score = 45.0 * confidence_score
        total_score += min(18.0, abs(float(oi_share_delta or 0.0)) * 3.2)
        total_score += min(12.0, abs(float(perp_share_delta or 0.0)) * 2.2)
        total_score += min(10.0, abs(float(spot_share_delta or 0.0)) * 2.0)
        total_score += min(10.0, strong_alerts * 4.0 + sentiment_alerts * 1.5)
        total_score += min(5.0, abs(float(risk_ratio or 0.0) - 1.0) * 0.8 if risk_ratio is not None else 0.0)
        total_score = min(100.0, total_score)
        execution_hint = "等待"
        if regime in {"现货先动", "现货主导"}:
            execution_hint = "优先现货"
        elif regime in {"合约追价", "合约抢跑", "偏多拥挤", "偏空拥挤"}:
            execution_hint = "优先合约"
        reason_parts = []
        if regime and regime != "混合观察":
            reason_parts.append(regime)
        if oi_share_delta is not None and abs(oi_share_delta) >= 0.3:
            reason_parts.append(f"OI份额Δ {oi_share_delta:.2f}%")
        if perp_share_delta is not None and abs(perp_share_delta) >= 0.3:
            reason_parts.append(f"合约份额Δ {perp_share_delta:.2f}%")
        if strong_alerts:
            reason_parts.append(f"强告警 {strong_alerts}")
        rows.append(
            {
                "交易所": exchange_name,
                "统一评分": round(total_score, 1),
                "方向": direction,
                "主判断": regime,
                "执行偏好": execution_hint,
                "置信度": decision_row.get("置信度") or "低置信",
                "强告警": strong_alerts,
                "情绪告警": sentiment_alerts,
                "OI份额Δ(%)": oi_share_delta,
                "合约成交份额Δ(%)": perp_share_delta,
                "现货成交份额Δ(%)": spot_share_delta,
                "风险比": risk_ratio,
                "原因": " | ".join(reason_parts[:4]) if reason_parts else "等待更清晰样本",
            }
        )
    frame = pd.DataFrame(rows)
    if frame.empty:
        return pd.DataFrame(columns=["交易所", "统一评分", "方向", "主判断", "执行偏好", "置信度", "强告警", "情绪告警", "OI份额Δ(%)", "合约成交份额Δ(%)", "现货成交份额Δ(%)", "风险比", "原因"])
    return frame.sort_values(["统一评分", "交易所"], ascending=[False, True]).reset_index(drop=True)


def build_unified_signal_figure(frame: pd.DataFrame) -> go.Figure:
    figure = go.Figure()
    if frame.empty:
        figure.add_annotation(text="等待统一评分样本", showarrow=False, x=0.5, y=0.5, xref="paper", yref="paper")
        figure.update_layout(height=300, margin=dict(l=12, r=12, t=24, b=12))
        return figure
    colors = []
    for _, row in frame.iterrows():
        direction = str(row.get("方向") or "")
        if direction == "偏多":
            colors.append("#6fdd8c")
        elif direction == "偏空":
            colors.append("#ff9a59")
        else:
            colors.append("#67d1ff")
    figure.add_trace(
        go.Bar(
            x=frame["交易所"],
            y=frame["统一评分"],
            marker_color=colors,
            text=[f"{value:.1f}" for value in frame["统一评分"].fillna(0.0)],
            textposition="outside",
            customdata=frame[["主判断", "执行偏好", "原因"]],
            hovertemplate="%{x}<br>统一评分 %{y:.1f}<br>%{customdata[0]}<br>%{customdata[1]}<br>%{customdata[2]}<extra></extra>",
        )
    )
    figure.update_layout(
        height=320,
        margin=dict(l=12, r=12, t=58, b=12),
        paper_bgcolor="rgba(14, 22, 35, 0.56)",
        plot_bgcolor="rgba(255, 255, 255, 0.045)",
        font=dict(color="#f6f9ff", family="SF Pro Display, Segoe UI, sans-serif"),
        title=dict(text="Unified Signal Score", x=0.03, y=0.98, xanchor="left", font=dict(size=18, color="#f7fbff")),
    )
    figure.update_xaxes(showgrid=False)
    figure.update_yaxes(showgrid=True, gridcolor="rgba(255, 255, 255, 0.08)", title="统一评分")
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
            spread_bps = payload_float(row.get("价差(bps)"))
            impact_50k = payload_float(row.get("50k冲击(bps)"))
            impact_250k = payload_float(row.get("250k冲击(bps)"))
            fill_50k = payload_float(row.get("50k填充率(%)"))
            fill_250k = payload_float(row.get("250k填充率(%)"))
            visible_depth = payload_float(row.get("可见深度"))
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
        return pd.DataFrame(columns=["路由", "交易所", "市场", "执行评分", "价差(bps)", "50k冲击(bps)", "250k冲击(bps)", "50k填充率(%)", "250k填充率(%)", "可见深度", "执行建议", "关联判断"])
    return frame.sort_values(["执行评分", "50k冲击(bps)"], ascending=[False, True], na_position="last").reset_index(drop=True)


def build_alert_closure_frame(
    alert_history_frame: pd.DataFrame,
    market_history_frame: pd.DataFrame,
    event_history_frame: pd.DataFrame,
    quality_history_frame: pd.DataFrame,
    *,
    review_window_minutes: int,
    limit: int = 24,
) -> pd.DataFrame:
    if alert_history_frame.empty or market_history_frame.empty:
        return pd.DataFrame(columns=["时间", "交易所", "告警", "触发价格", "触发Funding", "触发OI", "5m事件额", "5m事件数", "近价撤补比", "顺风区间(%)", "逆风区间(%)", "窗口收益(%)", "命中"])
    alert_review_frame = build_alert_review_frame(
        alert_history_frame,
        market_history_frame,
        review_window_minutes=review_window_minutes,
    )
    alerts = alert_history_frame[alert_history_frame["动作"] == "触发"].copy()
    if alerts.empty:
        return pd.DataFrame(columns=["时间", "交易所", "告警", "触发价格", "触发Funding", "触发OI", "5m事件额", "5m事件数", "近价撤补比", "顺风区间(%)", "逆风区间(%)", "窗口收益(%)", "命中"])
    alerts["ts_ms"] = (pd.to_datetime(alerts["时间"]).astype("int64") // 10**6).astype(int)
    market = market_history_frame.copy()
    market["ts_ms"] = (pd.to_datetime(market["时间"]).astype("int64") // 10**6).astype(int)
    events = event_history_frame.copy()
    if not events.empty:
        events["ts_ms"] = (pd.to_datetime(events["时间"]).astype("int64") // 10**6).astype(int)
    quality = quality_history_frame.copy()
    if not quality.empty:
        quality["ts_ms"] = (pd.to_datetime(quality["时间"]).astype("int64") // 10**6).astype(int)
    review_lookup = {
        (str(row.get("时间")), str(row.get("交易所")), str(row.get("告警"))): row
        for row in alert_review_frame.to_dict("records")
    } if not alert_review_frame.empty else {}
    rows: List[Dict[str, Any]] = []
    for row in alerts.sort_values("ts_ms", ascending=False).head(limit).to_dict("records"):
        exchange_key = str(row.get("交易所键") or "")
        exchange_name = str(row.get("交易所") or "")
        title = str(row.get("告警") or "")
        alert_ts = int(row.get("ts_ms") or 0)
        if exchange_key and "交易所键" in market.columns:
            market_slice = market[market["交易所键"].astype(str) == exchange_key].copy()
        else:
            market_slice = market[market["交易所"].astype(str) == exchange_name].copy()
        if market_slice.empty:
            continue
        trigger_market = market_slice.iloc[(market_slice["ts_ms"] - alert_ts).abs().argsort()[:1]].iloc[0].to_dict()
        event_notional = None
        event_count = 0
        if not events.empty:
            event_slice = events
            if exchange_key and "交易所键" in events.columns:
                event_slice = event_slice[event_slice["交易所键"].astype(str) == exchange_key]
            else:
                event_slice = event_slice[event_slice["交易所"].astype(str) == exchange_name]
            event_slice = event_slice[(event_slice["ts_ms"] >= alert_ts) & (event_slice["ts_ms"] <= alert_ts + 5 * 60_000)]
            event_count = int(len(event_slice))
            if "名义金额" in event_slice.columns and not event_slice.empty:
                event_notional = float(pd.to_numeric(event_slice["名义金额"], errors="coerce").fillna(0.0).sum())
        cancel_refill_ratio = None
        if not quality.empty:
            quality_slice = quality
            if exchange_key and "交易所键" in quality.columns:
                quality_slice = quality_slice[quality_slice["交易所键"].astype(str) == exchange_key]
            else:
                quality_slice = quality_slice[quality_slice["交易所"].astype(str) == exchange_name]
            quality_slice = quality_slice[(quality_slice["ts_ms"] >= alert_ts - 2 * 60_000) & (quality_slice["ts_ms"] <= alert_ts + 2 * 60_000)]
            if not quality_slice.empty:
                added_sum = float(pd.to_numeric(quality_slice["近价新增"], errors="coerce").fillna(0.0).sum()) if "近价新增" in quality_slice.columns else 0.0
                canceled_sum = float(pd.to_numeric(quality_slice["近价撤单"], errors="coerce").fillna(0.0).sum()) if "近价撤单" in quality_slice.columns else 0.0
                cancel_refill_ratio = canceled_sum / max(added_sum, 1.0)
        review_row = review_lookup.get((str(row.get("时间")), exchange_name, title), {})
        rows.append(
            {
                "时间": row.get("时间"),
                "交易所": exchange_name,
                "告警": title,
                "触发价格": payload_float(trigger_market.get("最新价")),
                "触发Funding": payload_float(trigger_market.get("Funding(bps)")),
                "触发OI": payload_float(trigger_market.get("OI金额")),
                "5m事件额": event_notional,
                "5m事件数": event_count,
                "近价撤补比": cancel_refill_ratio,
                "顺风区间(%)": payload_float(review_row.get("顺风区间(%)")),
                "逆风区间(%)": payload_float(review_row.get("逆风区间(%)")),
                "窗口收益(%)": payload_float(review_row.get("窗口收益(%)")),
                "命中": review_row.get("命中") or "-",
            }
        )
    frame = pd.DataFrame(rows)
    if frame.empty:
        return pd.DataFrame(columns=["时间", "交易所", "告警", "触发价格", "触发Funding", "触发OI", "5m事件额", "5m事件数", "近价撤补比", "顺风区间(%)", "逆风区间(%)", "窗口收益(%)", "命中"])
    return frame.reset_index(drop=True)


def build_cross_exchange_propagation_frame(
    decision_frame: pd.DataFrame,
    share_dynamics_frame: pd.DataFrame,
    spread_frame: pd.DataFrame,
    funding_arb_frame: pd.DataFrame,
    cluster_frame: pd.DataFrame,
) -> pd.DataFrame:
    exchange_names = sorted(
        set(decision_frame.get("交易所", pd.Series(dtype=str)).dropna().astype(str))
        | set(share_dynamics_frame.get("交易所", pd.Series(dtype=str)).dropna().astype(str))
        | set(spread_frame.get("交易所", pd.Series(dtype=str)).dropna().astype(str))
    )
    latest_cluster_exchanges: List[str] = []
    latest_cluster_label = ""
    if not cluster_frame.empty and "交易所" in cluster_frame.columns:
        latest_cluster = cluster_frame.iloc[0].to_dict()
        latest_cluster_label = str(latest_cluster.get("主导方向") or "")
        latest_cluster_exchanges = [part.strip() for part in str(latest_cluster.get("交易所") or "").split("/") if part.strip()]
    arb_text = ""
    if not funding_arb_frame.empty:
        arb_row = funding_arb_frame.iloc[0].to_dict()
        arb_text = str(arb_row.get("提示") or "")
    rows: List[Dict[str, Any]] = []
    for exchange_name in exchange_names:
        decision_row = (
            decision_frame[decision_frame["交易所"].astype(str) == exchange_name].iloc[0].to_dict()
            if not decision_frame.empty and exchange_name in set(decision_frame["交易所"].astype(str))
            else {}
        )
        share_row = (
            share_dynamics_frame[share_dynamics_frame["交易所"].astype(str) == exchange_name].iloc[0].to_dict()
            if not share_dynamics_frame.empty and exchange_name in set(share_dynamics_frame["交易所"].astype(str))
            else {}
        )
        spread_row = (
            spread_frame[spread_frame["交易所"].astype(str) == exchange_name].iloc[0].to_dict()
            if not spread_frame.empty and exchange_name in set(spread_frame["交易所"].astype(str))
            else {}
        )
        regime = str(decision_row.get("主导判断") or "混合观察")
        oi_delta = payload_float(share_row.get("OI份额Δ(%)"))
        perp_delta = payload_float(share_row.get("合约成交份额Δ(%)"))
        spot_delta = payload_float(share_row.get("现货成交份额Δ(%)"))
        price_dev_bps = payload_float(spread_row.get("偏离中位(bps)"))
        confidence_score = _confidence_score_from_label(decision_row.get("置信度"))
        propagation_score = 40.0 * confidence_score
        propagation_score += min(18.0, abs(float(oi_delta or 0.0)) * 3.5)
        propagation_score += min(16.0, abs(float(perp_delta or 0.0)) * 2.8)
        propagation_score += min(14.0, abs(float(spot_delta or 0.0)) * 2.8)
        propagation_score += min(10.0, abs(float(price_dev_bps or 0.0)) * 0.15)
        if exchange_name in latest_cluster_exchanges:
            propagation_score += 12.0
        propagation_score = min(100.0, propagation_score)
        role = "观察"
        if regime in {"现货先动", "现货主导"} and (spot_delta or 0.0) >= 0:
            role = "现货源头"
        elif regime in {"合约追价", "合约抢跑"} and (perp_delta or 0.0) >= 0:
            role = "合约放大"
        elif exchange_name in latest_cluster_exchanges:
            role = "爆仓传导"
        path_parts = []
        if regime and regime != "混合观察":
            path_parts.append(regime)
        if oi_delta is not None and abs(oi_delta) >= 0.2:
            path_parts.append(f"OI份额Δ {oi_delta:.2f}%")
        if price_dev_bps is not None and abs(price_dev_bps) >= 3.0:
            path_parts.append(f"价差偏离 {price_dev_bps:.1f}bps")
        if exchange_name in latest_cluster_exchanges and latest_cluster_label:
            path_parts.append(latest_cluster_label)
        if arb_text and exchange_name in arb_text:
            path_parts.append("费率腿")
        rows.append(
            {
                "交易所": exchange_name,
                "传导评分": round(propagation_score, 1),
                "角色": role,
                "主判断": regime,
                "现货份额Δ(%)": spot_delta,
                "合约份额Δ(%)": perp_delta,
                "OI份额Δ(%)": oi_delta,
                "价差偏离(bps)": price_dev_bps,
                "路径说明": " | ".join(path_parts[:4]) if path_parts else "等待更清晰的跨所样本",
            }
        )
    frame = pd.DataFrame(rows)
    if frame.empty:
        return pd.DataFrame(columns=["交易所", "传导评分", "角色", "主判断", "现货份额Δ(%)", "合约份额Δ(%)", "OI份额Δ(%)", "价差偏离(bps)", "路径说明"])
    return frame.sort_values(["传导评分", "交易所"], ascending=[False, True]).reset_index(drop=True)


def build_cross_exchange_propagation_figure(frame: pd.DataFrame) -> go.Figure:
    figure = go.Figure()
    if frame.empty:
        figure.add_annotation(text="等待跨所传导样本", showarrow=False, x=0.5, y=0.5, xref="paper", yref="paper")
        figure.update_layout(height=300, margin=dict(l=12, r=12, t=24, b=12))
        return figure
    color_map = {"现货源头": "#6fdd8c", "合约放大": "#67d1ff", "爆仓传导": "#ff9a59", "观察": "#f8d35e"}
    figure.add_trace(
        go.Bar(
            x=frame["传导评分"],
            y=frame["交易所"],
            orientation="h",
            marker_color=[color_map.get(str(role), "#67d1ff") for role in frame["角色"]],
            text=frame["角色"],
            textposition="outside",
            customdata=frame[["主判断", "路径说明"]],
            hovertemplate="%{y}<br>传导评分 %{x:.1f}<br>%{customdata[0]}<br>%{customdata[1]}<extra></extra>",
        )
    )
    figure.update_layout(
        height=max(300, 52 * len(frame)),
        margin=dict(l=12, r=12, t=58, b=12),
        paper_bgcolor="rgba(14, 22, 35, 0.56)",
        plot_bgcolor="rgba(255, 255, 255, 0.045)",
        font=dict(color="#f6f9ff", family="SF Pro Display, Segoe UI, sans-serif"),
        title=dict(text="Cross-Exchange Propagation Path", x=0.03, y=0.98, xanchor="left", font=dict(size=18, color="#f7fbff")),
    )
    figure.update_xaxes(showgrid=True, gridcolor="rgba(255, 255, 255, 0.08)", title="传导评分")
    figure.update_yaxes(showgrid=False, autorange="reversed")
    return figure


def build_hyperliquid_watch_behavior_frame(
    bundles: List[Dict[str, Any]],
    predicted_funding_frame: pd.DataFrame,
    *,
    current_coin: str,
) -> pd.DataFrame:
    predicted_map: Dict[str, float | None] = {}
    if not predicted_funding_frame.empty and {"币种", "预测费率(bps)"} <= set(predicted_funding_frame.columns):
        predicted_map = {
            str(row.get("币种") or "").strip().upper(): payload_float(row.get("预测费率(bps)"))
            for row in predicted_funding_frame.to_dict("records")
        }
    selected_coin = str(current_coin or "").strip().upper()
    rows: List[Dict[str, Any]] = []
    for bundle in bundles:
        positions_frame = build_hyperliquid_position_frame(bundle)
        raw_fills_frame = build_hyperliquid_fill_frame(bundle)
        raw_funding_frame = build_hyperliquid_funding_frame(bundle)
        fills_frame = raw_fills_frame.sort_values("时间") if not raw_fills_frame.empty else pd.DataFrame()
        funding_frame = raw_funding_frame.sort_values("时间") if not raw_funding_frame.empty else pd.DataFrame()
        filtered_positions = (
            positions_frame[positions_frame["币种"].astype(str).str.upper() == selected_coin].copy()
            if selected_coin and not positions_frame.empty and "币种" in positions_frame.columns
            else positions_frame.copy()
        )
        current_exposure = filtered_positions["仓位价值"].fillna(0.0).sum() if not filtered_positions.empty and "仓位价值" in filtered_positions.columns else 0.0
        total_exposure = positions_frame["仓位价值"].fillna(0.0).sum() if not positions_frame.empty and "仓位价值" in positions_frame.columns else 0.0
        funding_net = funding_frame["金额"].fillna(0.0).sum() if not funding_frame.empty and "金额" in funding_frame.columns else 0.0
        latest_fill = fills_frame.iloc[-1].to_dict() if not fills_frame.empty else {}
        latest_funding = funding_frame.iloc[-1].to_dict() if not funding_frame.empty else {}
        liq_distances: List[float] = []
        if not filtered_positions.empty:
            for _, row in filtered_positions.iterrows():
                mark_price = payload_float(row.get("标记价"))
                liq_price = payload_float(row.get("清算价"))
                if mark_price not in (None, 0) and liq_price is not None:
                    liq_distances.append(abs(float(mark_price) - float(liq_price)) / abs(float(mark_price)) * 100.0)
        nearest_liq_distance = min(liq_distances) if liq_distances else None
        latest_fill_notional = payload_float(latest_fill.get("名义金额"))
        behavior_label = "低活跃"
        if latest_fill_notional is not None and latest_fill_notional >= 100_000:
            behavior_label = "刚有大额成交"
        elif nearest_liq_distance is not None and nearest_liq_distance <= 8.0:
            behavior_label = "靠近清算带"
        elif abs(float(funding_net or 0.0)) >= 200.0:
            behavior_label = "Funding 敏感"
        elif current_exposure > 0:
            behavior_label = "持仓观察"
        rows.append(
            {
                "标签": bundle.get("label"),
                "分组": bundle.get("group"),
                "角色": bundle.get("role") or "user",
                "账户权益": payload_float(bundle.get("account_value")),
                "当前币种持仓值": current_exposure if current_exposure > 0 else None,
                "总持仓值": total_exposure if total_exposure > 0 else None,
                "Funding净额": funding_net if funding_net != 0 else None,
                "最新成交方向": latest_fill.get("方向"),
                "最新成交时间": latest_fill.get("时间"),
                "最新成交额": latest_fill_notional,
                "最近Funding": payload_float(latest_funding.get("金额")),
                "预测费率(bps)": predicted_map.get(selected_coin),
                "最近清算距离(%)": nearest_liq_distance,
                "实时流": "在线" if bool(bundle.get("connected")) else str(bundle.get("stream_status") or "未连接"),
                "行为标签": behavior_label,
            }
        )
    frame = pd.DataFrame(rows)
    if frame.empty:
        return pd.DataFrame(columns=["标签", "分组", "角色", "账户权益", "当前币种持仓值", "总持仓值", "Funding净额", "最新成交方向", "最新成交时间", "最新成交额", "最近Funding", "预测费率(bps)", "最近清算距离(%)", "实时流", "行为标签"])
    return frame.sort_values(["当前币种持仓值", "总持仓值", "账户权益"], ascending=[False, False, False], na_position="last").reset_index(drop=True)


def build_hyperliquid_watch_risk_frame(behavior_frame: pd.DataFrame) -> pd.DataFrame:
    if behavior_frame.empty or "分组" not in behavior_frame.columns:
        return pd.DataFrame(columns=["分组", "地址数", "在线率(%)", "账户权益", "当前币种持仓值", "总持仓值", "Funding净额", "预测费率(bps)", "最近清算距离(%)"])
    frame = behavior_frame.copy()
    frame["在线率(%)"] = [100.0 if "在线" in str(value or "") else 0.0 for value in frame.get("实时流", pd.Series(dtype=object))]
    grouped = (
        frame.groupby("分组", as_index=False)
        .agg(
            地址数=("标签", "size"),
            在线率_pct=("在线率(%)", "mean"),
            账户权益=("账户权益", "sum"),
            当前币种持仓值=("当前币种持仓值", "sum"),
            总持仓值=("总持仓值", "sum"),
            Funding净额=("Funding净额", "sum"),
            预测费率_bps=("预测费率(bps)", "mean"),
            最近清算距离_pct=("最近清算距离(%)", "min"),
        )
        .sort_values(["当前币种持仓值", "总持仓值", "账户权益"], ascending=[False, False, False], na_position="last")
        .reset_index(drop=True)
    )
    return grouped.rename(
        columns={
            "在线率_pct": "在线率(%)",
            "预测费率_bps": "预测费率(bps)",
            "最近清算距离_pct": "最近清算距离(%)",
        }
    )


def load_spot_market_maps(
    service: LiveTerminalService,
    spot_symbol_map: Dict[str, str],
    depth_limit: int,
    request_timeout: int,
    exchange_keys: List[str] | None = None,
) -> Tuple[Dict[str, SpotSnapshot], Dict[str, List[OrderBookLevel]], Dict[str, List[TradeEvent]]]:
    spot_snapshot_map: Dict[str, SpotSnapshot] = {}
    spot_orderbook_map: Dict[str, List[OrderBookLevel]] = {}
    spot_trades_map: Dict[str, List[TradeEvent]] = {}
    for exchange_key in exchange_keys or list(SPOT_EXCHANGE_ORDER):
        spot_exchange_symbol = str(spot_symbol_map.get(exchange_key) or "").strip().upper()
        if not spot_exchange_symbol:
            continue
        live_spot_snapshot = service.get_spot_snapshot(exchange_key)
        live_spot_orderbook = service.get_spot_orderbook(exchange_key)
        live_spot_trades = service.get_spot_trade_history(exchange_key)
        if exchange_key == "binance":
            fallback_snapshot = load_binance_spot_snapshot_cached(spot_exchange_symbol, request_timeout)
        else:
            fallback_snapshot = load_spot_snapshot_cached(exchange_key, spot_exchange_symbol, request_timeout)
        chosen_snapshot = live_spot_snapshot if _use_live_snapshot(live_spot_snapshot) else fallback_snapshot
        snapshot_timestamp_ms = chosen_snapshot.timestamp_ms or (live_spot_snapshot.timestamp_ms if live_spot_snapshot is not None else None)
        chosen_orderbook = (
            limit_orderbook_levels(live_spot_orderbook, depth_limit)
            if _use_live_orderbook(live_spot_orderbook, snapshot_timestamp_ms)
            else load_effective_spot_orderbook(service, exchange_key, spot_exchange_symbol, chosen_snapshot, depth_limit, request_timeout)
        )
        if _use_live_trades(live_spot_trades):
            chosen_trades = live_spot_trades
        elif exchange_key == "binance":
            chosen_trades = load_binance_spot_trades_cached(spot_exchange_symbol, 120, request_timeout)
        else:
            chosen_trades = load_spot_trades_cached(exchange_key, spot_exchange_symbol, 160, request_timeout)
        spot_snapshot_map[exchange_key] = chosen_snapshot
        spot_orderbook_map[exchange_key] = chosen_orderbook
        spot_trades_map[exchange_key] = chosen_trades
    return spot_snapshot_map, spot_orderbook_map, spot_trades_map


def limit_orderbook_levels(levels: List[OrderBookLevel], per_side_limit: int) -> List[OrderBookLevel]:
    limit = max(1, int(per_side_limit))
    bid_levels = [level for level in levels if level.side == "bid"][:limit]
    ask_levels = [level for level in levels if level.side == "ask"][:limit]
    return bid_levels + ask_levels


def load_effective_perp_orderbook(
    service: LiveTerminalService,
    exchange_key: str,
    symbol: str,
    snapshot: ExchangeSnapshot,
    depth_limit: int,
    request_timeout: int,
) -> List[OrderBookLevel]:
    live_orderbook = service.get_orderbook(exchange_key)
    if _use_live_orderbook(live_orderbook, snapshot.timestamp_ms):
        return limit_orderbook_levels(live_orderbook, depth_limit)
    return limit_orderbook_levels(load_orderbook_cached(exchange_key, symbol, depth_limit, request_timeout), depth_limit)


def load_effective_spot_orderbook(
    service: LiveTerminalService,
    exchange_key: str,
    symbol: str,
    snapshot: SpotSnapshot,
    depth_limit: int,
    request_timeout: int,
) -> List[OrderBookLevel]:
    live_orderbook = service.get_spot_orderbook(exchange_key)
    if _use_live_orderbook(live_orderbook, snapshot.timestamp_ms):
        return limit_orderbook_levels(live_orderbook, depth_limit)
    if exchange_key == "binance":
        return limit_orderbook_levels(load_binance_spot_orderbook_cached(symbol, min(depth_limit, 200), request_timeout), depth_limit)
    return limit_orderbook_levels(load_spot_orderbook_cached(exchange_key, symbol, min(depth_limit, 200), request_timeout), depth_limit)


def combine_orderbook_summaries(summaries: List[Dict[str, float | None]]) -> Dict[str, float | None]:
    if not summaries:
        return {
            "bid_size": None,
            "ask_size": None,
            "bid_notional": None,
            "ask_notional": None,
            "imbalance_pct": None,
            "spread_bps": None,
        }
    bid_size = sum(float(item.get("bid_size") or 0.0) for item in summaries)
    ask_size = sum(float(item.get("ask_size") or 0.0) for item in summaries)
    bid_notional = sum(float(item.get("bid_notional") or 0.0) for item in summaries)
    ask_notional = sum(float(item.get("ask_notional") or 0.0) for item in summaries)
    total_notional = bid_notional + ask_notional
    imbalance_pct = None if total_notional <= 0 else (bid_notional - ask_notional) / total_notional * 100.0
    spread_candidates = [payload_float(item.get("spread_bps")) for item in summaries if payload_float(item.get("spread_bps")) is not None]
    spread_bps = float(pd.Series(spread_candidates).mean()) if spread_candidates else None
    return {
        "bid_size": bid_size if bid_size > 0 else None,
        "ask_size": ask_size if ask_size > 0 else None,
        "bid_notional": bid_notional if bid_notional > 0 else None,
        "ask_notional": ask_notional if ask_notional > 0 else None,
        "imbalance_pct": imbalance_pct,
        "spread_bps": spread_bps,
    }


def build_top_orderbook_summary(
    service: LiveTerminalService,
    snapshot_by_key: Dict[str, ExchangeSnapshot],
    symbol_map: Dict[str, str],
    spot_symbol_map: Dict[str, str],
    *,
    selected_exchange: str,
    depth_limit: int,
    request_timeout: int,
    scope_mode: str,
    market_mode: str,
) -> Dict[str, Any]:
    perp_exchange_keys = (
        [selected_exchange]
        if scope_mode == "当前交易所" and symbol_map.get(selected_exchange)
        else [exchange_key for exchange_key in EXCHANGE_ORDER if symbol_map.get(exchange_key)]
    )
    spot_exchange_keys = (
        [selected_exchange]
        if scope_mode == "当前交易所" and spot_symbol_map.get(selected_exchange)
        else [exchange_key for exchange_key in SPOT_EXCHANGE_ORDER if spot_symbol_map.get(exchange_key)]
        if scope_mode == "四所聚合"
        else []
    )
    summaries: List[Dict[str, float | None]] = []
    actual_bid_levels = 0
    actual_ask_levels = 0
    requested_bid_levels = 0
    requested_ask_levels = 0
    included_labels: List[str] = []
    if market_mode in ("合约", "合并"):
        for exchange_key in perp_exchange_keys:
            snapshot = snapshot_by_key.get(exchange_key)
            if snapshot is None or snapshot.status != "ok":
                continue
            requested_bid_levels += int(depth_limit)
            requested_ask_levels += int(depth_limit)
            reference_price = snapshot.last_price or snapshot.mark_price
            orderbook = load_effective_perp_orderbook(
                service,
                exchange_key,
                symbol_map[exchange_key],
                snapshot,
                depth_limit,
                request_timeout,
            )
            if not orderbook:
                continue
            actual_bid_levels += len([level for level in orderbook if level.side == "bid"])
            actual_ask_levels += len([level for level in orderbook if level.side == "ask"])
            summaries.append(summarize_orderbook(orderbook, reference_price))
        if perp_exchange_keys:
            included_labels.append("合约")
    if market_mode in ("现货", "合并"):
        for exchange_key in spot_exchange_keys:
            live_snapshot = service.get_spot_snapshot(exchange_key)
            snapshot = live_snapshot if _use_live_snapshot(live_snapshot) else (
                load_binance_spot_snapshot_cached(spot_symbol_map[exchange_key], request_timeout)
                if exchange_key == "binance"
                else load_spot_snapshot_cached(exchange_key, spot_symbol_map[exchange_key], request_timeout)
            )
            if snapshot is None or snapshot.status != "ok":
                continue
            requested_bid_levels += int(depth_limit)
            requested_ask_levels += int(depth_limit)
            reference_price = snapshot.last_price or snapshot.bid_price or snapshot.ask_price
            orderbook = load_effective_spot_orderbook(
                service,
                exchange_key,
                spot_symbol_map[exchange_key],
                snapshot,
                depth_limit,
                request_timeout,
            )
            if not orderbook:
                continue
            actual_bid_levels += len([level for level in orderbook if level.side == "bid"])
            actual_ask_levels += len([level for level in orderbook if level.side == "ask"])
            summaries.append(summarize_orderbook(orderbook, reference_price))
        if spot_exchange_keys:
            included_labels.append("现货")
    scope_label = f"{EXCHANGE_TITLES.get(selected_exchange, selected_exchange)} 当前所" if scope_mode == "当前交易所" else "跨所聚合"
    market_label = " + ".join(included_labels) if included_labels else market_mode
    note = ""
    if market_mode == "现货" and scope_mode == "当前交易所" and not spot_symbol_map.get(selected_exchange):
        note = "当前主图交易所没有现货盘口接口"
    if market_mode == "合并" and scope_mode == "当前交易所" and not spot_symbol_map.get(selected_exchange):
        note = "当前主图交易所仅统计合约盘口"
    return {
        "summary": combine_orderbook_summaries(summaries),
        "scope_label": scope_label,
        "market_label": market_label,
        "actual_bid_levels": actual_bid_levels,
        "actual_ask_levels": actual_ask_levels,
        "requested_bid_levels": requested_bid_levels,
        "requested_ask_levels": requested_ask_levels,
        "note": note,
    }


def load_perp_reference_maps(
    service: LiveTerminalService,
    symbol_map: Dict[str, str],
    snapshot_by_key: Dict[str, ExchangeSnapshot],
    depth_limit: int,
    request_timeout: int,
    exchange_keys: List[str] | None = None,
) -> Tuple[Dict[str, List[OrderBookLevel]], Dict[str, List[TradeEvent]]]:
    perp_orderbook_map: Dict[str, List[OrderBookLevel]] = {}
    perp_trades_map: Dict[str, List[TradeEvent]] = {}
    for exchange_key in exchange_keys or list(EXCHANGE_ORDER):
        symbol = str(symbol_map.get(exchange_key) or "").strip().upper()
        snapshot = snapshot_by_key.get(exchange_key)
        if not symbol or snapshot is None:
            continue
        perp_orderbook_map[exchange_key] = load_effective_perp_orderbook(
            service,
            exchange_key,
            symbol,
            snapshot,
            depth_limit,
            request_timeout,
        )
        live_trades = service.get_trade_history(exchange_key)
        perp_trades_map[exchange_key] = (
            live_trades
            if _use_live_trades(live_trades)
            else load_exchange_trades_cached(exchange_key, symbol, 160, request_timeout)
        )
    return perp_orderbook_map, perp_trades_map


def load_single_perp_reference(
    service: LiveTerminalService,
    exchange_key: str,
    symbol: str,
    snapshot: ExchangeSnapshot,
    depth_limit: int,
    request_timeout: int,
) -> Tuple[List[OrderBookLevel], List[TradeEvent]]:
    if not str(symbol or "").strip():
        return [], []
    orderbook = load_effective_perp_orderbook(service, exchange_key, symbol, snapshot, depth_limit, request_timeout)
    live_trades = service.get_trade_history(exchange_key)
    trades = live_trades if _use_live_trades(live_trades) else load_exchange_trades_cached(exchange_key, symbol, 160, request_timeout)
    return orderbook, trades


def load_single_spot_market(
    service: LiveTerminalService,
    exchange_key: str,
    symbol: str,
    depth_limit: int,
    request_timeout: int,
) -> Tuple[SpotSnapshot, List[OrderBookLevel], List[TradeEvent]]:
    if not str(symbol or "").strip():
        return SpotSnapshot(exchange=EXCHANGE_TITLES.get(exchange_key, exchange_key.title()), symbol="", status="error", error="未上架此币"), [], []
    live_snapshot = service.get_spot_snapshot(exchange_key)
    if exchange_key == "binance":
        fallback_snapshot = load_binance_spot_snapshot_cached(symbol, request_timeout)
    else:
        fallback_snapshot = load_spot_snapshot_cached(exchange_key, symbol, request_timeout)
    chosen_snapshot = live_snapshot if _use_live_snapshot(live_snapshot) else fallback_snapshot
    live_orderbook = service.get_spot_orderbook(exchange_key)
    snapshot_timestamp_ms = chosen_snapshot.timestamp_ms or (live_snapshot.timestamp_ms if live_snapshot is not None else None)
    chosen_orderbook = (
        limit_orderbook_levels(live_orderbook, depth_limit)
        if _use_live_orderbook(live_orderbook, snapshot_timestamp_ms)
        else load_effective_spot_orderbook(service, exchange_key, symbol, chosen_snapshot, depth_limit, request_timeout)
    )
    live_trades = service.get_spot_trade_history(exchange_key)
    if _use_live_trades(live_trades):
        chosen_trades = live_trades
    elif exchange_key == "binance":
        chosen_trades = load_binance_spot_trades_cached(symbol, 120, request_timeout)
    else:
        chosen_trades = load_spot_trades_cached(exchange_key, symbol, 160, request_timeout)
    return chosen_snapshot, chosen_orderbook, chosen_trades


def _oi_value(point: OIPoint | None) -> float | None:
    if point is None:
        return None
    return point.open_interest_notional if point.open_interest_notional is not None else point.open_interest


def compute_oi_change_pct(points: List[OIPoint], hours_back: int) -> float | None:
    if not points:
        return None
    ordered = [point for point in points if _oi_value(point) not in (None, 0)]
    if len(ordered) < 2:
        return None
    latest = ordered[-1]
    latest_value = _oi_value(latest)
    if latest_value in (None, 0):
        return None
    cutoff_ms = latest.timestamp_ms - max(hours_back, 1) * 3_600_000
    reference = next((point for point in reversed(ordered[:-1]) if point.timestamp_ms <= cutoff_ms), ordered[0])
    reference_value = _oi_value(reference)
    if reference_value in (None, 0):
        return None
    return (float(latest_value) - float(reference_value)) / float(reference_value) * 100.0


@st.cache_data(ttl=60, show_spinner=False)
def load_market_overview_row_cached(coin: str, timeout: int, liquidation_sample_limit: int) -> Dict[str, object]:
    coin = coin.upper().strip()
    catalog_payload = load_exchange_coin_catalog_cached(timeout)
    available_perp_keys, available_spot_keys = resolve_coin_market_availability(
        coin,
        catalog_payload.get("availability") or {},
        catalog_payload.get("status") or {},
    )
    symbol_map, spot_symbols = filter_symbol_maps_for_coin(default_symbols(coin), default_spot_symbols(coin), available_perp_keys, available_spot_keys)
    snapshots = fetch_all_snapshots(symbol_map, timeout=timeout)
    snapshot_map = {key: snapshot for key, snapshot in zip(EXCHANGE_ORDER, snapshots)}
    ok_snapshots = [snapshot for snapshot in snapshots if snapshot.status == "ok"]
    binance_perp_snapshot = snapshot_map.get("binance", ExchangeSnapshot(exchange="Binance", symbol=symbol_map["binance"], status="error"))
    binance_spot_snapshot = (
        load_binance_spot_snapshot_cached(spot_symbols["binance"], timeout)
        if spot_symbols.get("binance")
        else SpotSnapshot(exchange="Binance", symbol="", status="error", error="未上架此币")
    )
    binance_oi_history = load_oi_backfill_cached("binance", symbol_map["binance"], "1h", 30, timeout) if symbol_map.get("binance") else []
    binance_trades = load_exchange_trades_cached("binance", symbol_map["binance"], 180, timeout) if symbol_map.get("binance") else []
    binance_spot_trades = load_binance_spot_trades_cached(spot_symbols["binance"], 180, timeout) if spot_symbols.get("binance") else []
    binance_candles = load_candles_cached("binance", symbol_map["binance"], "1h", 40, timeout) if symbol_map.get("binance") else []
    crowd_payload = load_binance_crowd_cached(symbol_map["binance"], "1h", timeout) if symbol_map.get("binance") else {}
    bybit_crowd_payload = load_bybit_crowd_cached(symbol_map["bybit"], "1h", timeout) if symbol_map.get("bybit") else {}
    binance_liquidations = load_liquidations_cached("binance", symbol_map["binance"], liquidation_sample_limit, timeout) if symbol_map.get("binance") else []
    bybit_liquidations = load_liquidations_cached("bybit", symbol_map["bybit"], liquidation_sample_limit, timeout) if symbol_map.get("bybit") else []
    aggregate_liquidations = merge_liquidation_events(binance_liquidations, bybit_liquidations)
    liquidation_sample_notional = sum(event.notional or 0.0 for event in aggregate_liquidations)
    lead_lag = compute_spot_perp_lead_lag(
        binance_spot_trades,
        binance_trades,
        now_ms=int(time.time() * 1000),
        lookback_minutes=10,
        bucket_seconds=1,
        max_lag_buckets=3,
    )
    oi_quadrant = build_oi_quadrant_metrics(binance_oi_history, binance_candles)
    trade_metrics = build_trade_metrics(binance_trades, window_minutes=30)
    top_position_ratio = latest_ratio(crowd_payload, "top_position", "longShortRatio")
    top_account_ratio = latest_ratio(crowd_payload, "top_account", "longShortRatio")
    global_ratio = latest_ratio(crowd_payload, "global_account", "longShortRatio")
    bybit_ratio = latest_ratio(bybit_crowd_payload, "account_ratio", "longShortRatio")
    overview_ratio_values = [value for value in (global_ratio, top_position_ratio, bybit_ratio) if value is not None]
    overview_contract_ratio = payload_float(pd.Series(overview_ratio_values).median()) if overview_ratio_values else None
    composite_signal = build_composite_signal(
        binance_perp_snapshot if binance_perp_snapshot.status == "ok" else None,
        oi_quadrant,
        trade_metrics,
        top_position_ratio,
        top_account_ratio,
        global_ratio,
    )
    current_price = pd.Series([snapshot.last_price for snapshot in ok_snapshots if snapshot.last_price is not None]).median()
    total_oi_notional = sum(snapshot.open_interest_notional or 0.0 for snapshot in ok_snapshots)
    funding_values = [snapshot.funding_bps for snapshot in ok_snapshots if snapshot.funding_bps is not None]
    avg_funding_bps = sum(funding_values) / len(funding_values) if funding_values else None
    spot_perp_volume_ratio = None
    if (
        binance_spot_snapshot.status == "ok"
        and binance_spot_snapshot.volume_24h_notional not in (None, 0)
        and binance_perp_snapshot.volume_24h_notional is not None
    ):
        spot_perp_volume_ratio = binance_perp_snapshot.volume_24h_notional / binance_spot_snapshot.volume_24h_notional
    return {
        "币种": coin,
        "价格": current_price,
        "OI": total_oi_notional if total_oi_notional > 0 else (binance_perp_snapshot.open_interest_notional or binance_perp_snapshot.open_interest),
        "OI 1h(%)": compute_oi_change_pct(binance_oi_history, 1),
        "OI 24h(%)": compute_oi_change_pct(binance_oi_history, 24),
        "Funding(bps)": avg_funding_bps if avg_funding_bps is not None else binance_perp_snapshot.funding_bps,
        "24h爆仓样本额": liquidation_sample_notional,
        "多空比": overview_contract_ratio,
        "现货/合约成交比": spot_perp_volume_ratio,
        "Lead/Lag": lead_lag.get("summary") or "样本不足",
        "主结论": composite_signal.get("label") or "信号混合",
        "置信度": float(composite_signal.get("confidence") or 0.0) * 100.0,
        "主图交易所数": len(ok_snapshots),
        "Binance OI": binance_perp_snapshot.open_interest_notional,
        "Binance OI 1h(%)": oi_quadrant.get("oi_change_pct"),
    }


@st.cache_data(ttl=60, show_spinner=False)
def load_market_overview_frame_cached(coins: Tuple[str, ...], timeout: int, liquidation_sample_limit: int) -> pd.DataFrame:
    rows = []
    for coin in coins:
        try:
            rows.append(load_market_overview_row_cached(coin, timeout, liquidation_sample_limit))
        except Exception:
            rows.append({"币种": coin, "价格": None, "主结论": "加载失败"})
    frame = pd.DataFrame(rows)
    if frame.empty:
        return pd.DataFrame(columns=["币种", "价格", "OI", "OI 1h(%)", "OI 24h(%)", "Funding(bps)", "24h爆仓样本额", "多空比", "现货/合约成交比", "Lead/Lag", "主结论", "置信度"])
    sort_col = "OI"
    if sort_col not in frame.columns:
        return frame
    return frame.sort_values(sort_col, ascending=False, na_position="last").reset_index(drop=True)


def build_market_conclusion_lines(
    coin: str,
    composite_signal: Dict[str, object],
    lead_lag_summary: str | None,
    strongest_alerts: pd.DataFrame,
    liquidation_truth: Dict[str, object],
) -> List[str]:
    lines = [
        f"{coin}: 当前更像 `{composite_signal.get('label') or '信号混合'}`，合成分数 {float(composite_signal.get('score') or 0.0):+.1f}。",
    ]
    if lead_lag_summary:
        lines.append(f"短线驱动: `{lead_lag_summary}`。")
    if not strongest_alerts.empty:
        first_alert = strongest_alerts.iloc[0]
        lines.append(f"最高优先级告警: `{first_alert['告警']}`。")
    dominant = liquidation_truth.get("dominant")
    if dominant:
        lines.append(
            f"爆仓真值: 最近 {liquidation_window_minutes}m `{dominant}` 主导，30 秒爆仓簇 {int(liquidation_truth.get('cluster_count') or 0)} 个。"
        )
    return lines


def build_movers_frame(source: pd.DataFrame, column: str, limit: int = 5, ascending: bool = False, title_filter: str | None = None) -> pd.DataFrame:
    if source.empty or column not in source.columns:
        return pd.DataFrame()
    frame = source.copy()
    if title_filter and "主结论" in frame.columns:
        frame = frame[frame["主结论"].astype(str).str.contains(title_filter, na=False)]
    frame = frame.dropna(subset=[column])
    if frame.empty:
        return pd.DataFrame()
    return frame.sort_values(column, ascending=ascending).head(limit).reset_index(drop=True)


def fmt_price(value) -> str:
    return "-" if value is None else f"{value:,.2f}"


def fmt_compact(value) -> str:
    if value is None:
        return "-"
    value = float(value)
    abs_value = abs(value)
    if abs_value >= 1_000_000_000:
        return f"{value / 1_000_000_000:.2f}B"
    if abs_value >= 1_000_000:
        return f"{value / 1_000_000:.2f}M"
    if abs_value >= 1_000:
        return f"{value / 1_000:.2f}K"
    return f"{value:.2f}"


def fmt_signed_compact(value) -> str:
    if value is None:
        return "-"
    value = float(value)
    sign = "+" if value >= 0 else "-"
    return f"{sign}{fmt_compact(abs(value))}"


def fmt_bps(value) -> str:
    return "-" if value is None else f"{value:.2f} bps"


def fmt_pct(value) -> str:
    return "-" if value is None else f"{value:.2f}%"


def fmt_rate(value) -> str:
    return "-" if value is None else f"{value:+.6f}"


def format_display_timestamp_ms(value: Any) -> str:
    if value in (None, "", 0):
        return "-"
    try:
        return datetime.fromtimestamp(int(value) / 1000.0).astimezone().strftime("%m-%d %H:%M:%S")
    except (OSError, OverflowError, TypeError, ValueError):
        return "-"


def format_share_baseline_age(ts_ms: int | None, now_ms: int) -> str:
    if not ts_ms:
        return "本轮样本"
    delta_seconds = max(0.0, (float(now_ms) - float(ts_ms)) / 1000.0)
    if delta_seconds < 60.0:
        return f"对比 {delta_seconds:.0f}s 前"
    if delta_seconds < 3600.0:
        return f"对比 {delta_seconds / 60.0:.1f}m 前"
    return f"对比 {delta_seconds / 3600.0:.1f}h 前"


def _estimate_oi_delta_notional(current_notional: float | None, oi_change_pct: float | None) -> float | None:
    if current_notional in (None, 0) or oi_change_pct is None:
        return None
    denominator = 1.0 + float(oi_change_pct) / 100.0
    if abs(denominator) < 1e-9:
        return None
    anchor_notional = float(current_notional) / denominator
    return float(current_notional) - anchor_notional


def build_oi_brief_lines(
    base_coin: str,
    snapshot_by_key: Dict[str, ExchangeSnapshot],
    oi_metrics_by_exchange: Dict[str, Dict[str, float | str | None]],
    *,
    exchange_keys: List[str],
) -> List[str]:
    lines: List[str] = []
    for exchange_key in exchange_keys:
        snapshot = snapshot_by_key.get(exchange_key)
        if snapshot is None or snapshot.status != "ok":
            continue
        exchange_name = EXCHANGE_TITLES.get(exchange_key, exchange_key.title())
        metrics = oi_metrics_by_exchange.get(exchange_name) or {}
        oi_change_pct = payload_float(metrics.get("oi_change_pct"))
        current_notional = payload_float(snapshot.open_interest_notional)
        if oi_change_pct is None or current_notional is None:
            continue
        delta_notional = _estimate_oi_delta_notional(current_notional, oi_change_pct)
        if abs(float(oi_change_pct)) < 0.05:
            direction = "持平"
        else:
            direction = "增加" if float(oi_change_pct) > 0 else "减少"
        current_label = f"${fmt_compact(current_notional)}"
        delta_label = f"${fmt_signed_compact(delta_notional)}" if delta_notional is not None else "-"
        lines.append(
            f"{exchange_name} {base_coin} OI {direction} {float(oi_change_pct):+.1f}% | {current_label}（变化 {delta_label}）"
        )
    return lines


def build_liquidation_brief_lines(
    base_coin: str,
    liquidation_metrics_by_exchange: Dict[str, Dict[str, float | int | str | None]],
    *,
    exchange_keys: List[str],
) -> List[str]:
    lines: List[str] = []
    for exchange_key in exchange_keys:
        exchange_name = EXCHANGE_TITLES.get(exchange_key, exchange_key.title())
        metrics = liquidation_metrics_by_exchange.get(exchange_name) or {}
        total_notional = payload_float(metrics.get("notional"))
        long_notional = payload_float(metrics.get("long_notional")) or 0.0
        short_notional = payload_float(metrics.get("short_notional")) or 0.0
        if total_notional is None or total_notional <= 0:
            continue
        if long_notional > short_notional:
            dominant_label = "多头爆仓更重"
        elif short_notional > long_notional:
            dominant_label = "空头爆仓更重"
        else:
            dominant_label = "多空爆仓均衡"
        lines.append(
            f"{exchange_name} {base_coin} 爆仓主导 {dominant_label} | 总额 ${fmt_compact(total_notional)}（多头 ${fmt_compact(long_notional)} / 空头 ${fmt_compact(short_notional)}）"
        )
    return lines


def build_trade_impulse_brief_lines(
    base_coin: str,
    trade_metrics_by_exchange: Dict[str, Dict[str, float | int | str | None]],
    *,
    exchange_keys: List[str],
    window_minutes: int,
) -> List[str]:
    lines: List[str] = []
    for exchange_key in exchange_keys:
        exchange_name = EXCHANGE_TITLES.get(exchange_key, exchange_key.title())
        metrics = trade_metrics_by_exchange.get(exchange_name) or {}
        price_change_pct = payload_float(metrics.get("price_change_pct"))
        delta_notional = payload_float(metrics.get("delta_notional"))
        regime = str(metrics.get("regime") or "").strip()
        if price_change_pct is None and delta_notional is None:
            continue
        if price_change_pct is not None and price_change_pct >= 0.25:
            lines.append(
                f"{exchange_name} {base_coin} 上涨异动 {price_change_pct:+.2f}% | {window_minutes}m 主动差额 ${fmt_signed_compact(delta_notional)} · {regime or '多头推进'}"
            )
        elif price_change_pct is not None and price_change_pct <= -0.25:
            lines.append(
                f"{exchange_name} {base_coin} 砸盘异动 {price_change_pct:+.2f}% | {window_minutes}m 主动差额 ${fmt_signed_compact(delta_notional)} · {regime or '空头推进'}"
            )
        elif delta_notional is not None and abs(delta_notional) >= 1_000_000.0:
            lines.append(
                f"{exchange_name} {base_coin} {'买盘' if float(delta_notional) > 0 else '卖盘'}异动 | {window_minutes}m 主动差额 ${fmt_signed_compact(delta_notional)} · {regime or '流量放大'}"
            )
    return lines


def build_large_trade_brief_lines(large_trade_frame: pd.DataFrame, *, limit: int = 3) -> List[str]:
    if large_trade_frame.empty:
        return []
    frame = large_trade_frame.copy()
    if "名义金额" in frame.columns:
        frame = frame.sort_values(["名义金额", "时间"], ascending=[False, False], na_position="last")
    lines: List[str] = []
    for _, row in frame.head(limit).iterrows():
        exchange_name = str(row.get("交易所") or "未知交易所")
        direction = str(row.get("方向") or "大单")
        notional = payload_float(row.get("名义金额"))
        price = payload_float(row.get("价格"))
        if notional is None or notional <= 0:
            continue
        lines.append(
            f"{exchange_name} 大单 {direction} ${fmt_compact(notional)} @ {fmt_price(price)}"
        )
    return lines


def build_alert_brief_lines(
    confirmed_alert_frame: pd.DataFrame,
    *,
    exchange_keys: List[str],
    limit: int = 3,
) -> List[str]:
    if confirmed_alert_frame.empty or "告警" not in confirmed_alert_frame.columns:
        return []
    allowed_names = {EXCHANGE_TITLES.get(key, key.title()) for key in exchange_keys}
    frame = confirmed_alert_frame.copy()
    if "交易所" in frame.columns:
        frame = frame[frame["交易所"].astype(str).isin(allowed_names)]
    if "状态" in frame.columns:
        frame = frame[frame["状态"].astype(str) == "已确认"]
    if frame.empty:
        return []
    priority_map = {"强": 0, "中": 1, "弱": 2}
    if "等级" in frame.columns:
        frame = frame.assign(_priority=frame["等级"].map(priority_map).fillna(9))
        frame = frame.sort_values(["_priority", "交易所", "告警"], na_position="last")
    lines: List[str] = []
    for _, row in frame.head(limit).iterrows():
        exchange_name = str(row.get("交易所") or "未知交易所")
        alert_title = str(row.get("告警") or "告警")
        alert_level = str(row.get("等级") or "弱")
        lines.append(f"{exchange_name} 告警 {alert_title}（{alert_level}）")
    return lines


def _headline_items_from_lines(
    kind: str,
    lines: List[str],
    *,
    section_target: str,
    overview_target: str,
) -> List[Dict[str, str]]:
    items: List[Dict[str, str]] = []
    for line in lines:
        text = str(line or "").strip()
        if not text:
            continue
        items.append(
            {
                "kind": kind,
                "label": text,
                "tag_target": section_target,
                "body_target": overview_target or section_target,
            }
        )
    return items


def build_home_headline_items(
    base_coin: str,
    snapshot_by_key: Dict[str, ExchangeSnapshot],
    oi_metrics_by_exchange: Dict[str, Dict[str, float | str | None]],
    liquidation_metrics_by_exchange: Dict[str, Dict[str, float | int | str | None]],
    trade_metrics_by_exchange: Dict[str, Dict[str, float | int | str | None]],
    *,
    exchange_keys: List[str],
    liquidation_window_minutes: int,
    large_trade_frame: pd.DataFrame | None = None,
    confirmed_alert_frame: pd.DataFrame | None = None,
    kind_targets: Dict[str, str] | None = None,
    limit: int = 12,
) -> List[Dict[str, str]]:
    kind_targets = dict(kind_targets or {})
    overview_target = str(kind_targets.get("overview") or "section-home-overview")
    items: List[Dict[str, str]] = []
    items.extend(
        _headline_items_from_lines(
            "oi",
            build_oi_brief_lines(
                base_coin,
                snapshot_by_key,
                oi_metrics_by_exchange,
                exchange_keys=exchange_keys,
            ),
            section_target=str(kind_targets.get("oi") or overview_target),
            overview_target=overview_target,
        )
    )
    items.extend(
        _headline_items_from_lines(
            "liquidation",
            build_liquidation_brief_lines(
                base_coin,
                liquidation_metrics_by_exchange,
                exchange_keys=exchange_keys,
            ),
            section_target=str(kind_targets.get("liquidation") or overview_target),
            overview_target=overview_target,
        )
    )
    items.extend(
        _headline_items_from_lines(
            "trade",
            build_trade_impulse_brief_lines(
                base_coin,
                trade_metrics_by_exchange,
                exchange_keys=exchange_keys,
                window_minutes=liquidation_window_minutes,
            ),
            section_target=str(kind_targets.get("trade") or overview_target),
            overview_target=overview_target,
        )
    )
    if confirmed_alert_frame is not None:
        items.extend(
            _headline_items_from_lines(
                "alert",
                build_alert_brief_lines(confirmed_alert_frame, exchange_keys=exchange_keys),
                section_target=str(kind_targets.get("alert") or overview_target),
                overview_target=overview_target,
            )
        )
    if large_trade_frame is not None:
        items.extend(
            _headline_items_from_lines(
                "large",
                build_large_trade_brief_lines(large_trade_frame),
                section_target=str(kind_targets.get("large") or overview_target),
                overview_target=overview_target,
            )
        )
    normalized_items: List[Dict[str, str]] = []
    seen = set()
    for item in items:
        text = str(item.get("label") or "").strip()
        if not text or text in seen:
            continue
        seen.add(text)
        normalized_items.append(item)
        if len(normalized_items) >= max(limit, 1):
            break
    return normalized_items


def _normalize_headline_href(anchor_id: str | None) -> str:
    anchor = str(anchor_id or "").strip()
    return f"#{anchor}" if anchor else ""


def render_home_headline_marquee(container, items: List[Dict[str, str]]) -> None:
    normalized_items = [item for item in items if str(item.get("label") or "").strip()]
    if not normalized_items:
        container.empty()
        return
    chips_html = ""
    for item in normalized_items:
        kind = str(item.get("kind") or "trade").strip().lower()
        label = str(item.get("label") or "").strip()
        tag_text = HEADLINE_KIND_LABELS.get(kind, kind.upper())
        tag_href = _normalize_headline_href(item.get("tag_target"))
        body_href = _normalize_headline_href(item.get("body_target")) or tag_href
        tag_html = (
            f"<a class='headline-marquee-tag headline-tag-{kind}' href='{html_escape(tag_href)}'>{html_escape(tag_text)}</a>"
            if tag_href
            else f"<span class='headline-marquee-tag headline-tag-{kind}'>{html_escape(tag_text)}</span>"
        )
        body_html = (
            f"<a class='headline-marquee-text' href='{html_escape(body_href)}'>{html_escape(label)}</a>"
            if body_href
            else f"<span class='headline-marquee-text'>{html_escape(label)}</span>"
        )
        chips_html += f"<span class='headline-marquee-chip headline-chip-{kind}'>{tag_html}{body_html}</span>"
    container.markdown(
        f"""
        <div class="headline-marquee-shell">
            <div class="headline-marquee-label">实时摘要</div>
            <div class="headline-marquee-viewport">
                <div class="headline-marquee-track">
                    <div class="headline-marquee-group">{chips_html}</div>
                    <div class="headline-marquee-group" aria-hidden="true">{chips_html}</div>
                </div>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def crowd_period_for_interval(interval: str) -> str:
    mapping = {
        "1m": "5m",
        "3m": "5m",
        "5m": "5m",
        "15m": "15m",
        "30m": "30m",
        "1h": "1h",
        "4h": "4h",
        "1d": "1d",
    }
    return mapping.get(interval, "5m")


def chart_key(*parts: object) -> str:
    return "::".join(str(part) for part in parts)


def latest_ratio(payload: Dict[str, List[dict]], dataset_key: str, field_name: str) -> float | None:
    items = payload.get(dataset_key) or []
    if not items:
        return None
    try:
        value = float(items[-1].get(field_name))
        return None if pd.isna(value) else value
    except (TypeError, ValueError, AttributeError):
        return None


def latest_payload_float(payload: Dict[str, List[dict]], dataset_key: str, field_name: str) -> float | None:
    items = payload.get(dataset_key) or []
    if not items:
        return None
    value = pd.to_numeric(items[-1].get(field_name), errors="coerce")
    return None if pd.isna(value) else float(value)


def _render_section_anchor_html(anchor_id: str | None = None, anchor_aliases: List[str] | None = None) -> str:
    anchors = [str(anchor_id or "").strip(), *[str(item).strip() for item in (anchor_aliases or [])]]
    unique_anchors: List[str] = []
    seen = set()
    for anchor in anchors:
        if not anchor or anchor in seen:
            continue
        seen.add(anchor)
        unique_anchors.append(anchor)
    return "".join(f"<div id='{html_escape(anchor)}' class='section-anchor'></div>" for anchor in unique_anchors)


def render_section(
    title: str,
    subtitle: str = "",
    kicker: str = "Desk",
    *,
    anchor_id: str | None = None,
    anchor_aliases: List[str] | None = None,
) -> None:
    subtitle_html = f"<div class='glass-sub'>{subtitle}</div>" if subtitle else ""
    st.markdown(
        f"{_render_section_anchor_html(anchor_id, anchor_aliases)}"
        f"<div class='glass-section'><div class='glass-kicker'>{kicker}</div><div class='glass-title'>{title}</div>{subtitle_html}</div>",
        unsafe_allow_html=True,
    )


def render_choice_bar(label: str, options: List[str], key: str, default: str | None = None) -> str:
    if not options:
        return ""
    selected_state_key = f"{key}::selected"
    current = st.session_state.get(selected_state_key, default or options[0])
    if current not in options:
        current = default or options[0]
    st.markdown(f"<div class='mode-bar-label'>{label}</div>", unsafe_allow_html=True)
    widths = [max(1.0, min(2.6, 0.8 + 0.1 * len(option))) for option in options]
    for column, option in zip(st.columns(widths), options):
        with column:
            if st.button(
                option,
                key=f"{key}::button::{option}",
                width="stretch",
                type="primary" if option == current else "secondary",
            ):
                current = option
                st.session_state[selected_state_key] = option
    st.session_state[selected_state_key] = current
    return current


def pick_option(options: List[Any], preferred: Any, fallback: Any = None) -> Any:
    if preferred in options:
        return preferred
    preferred_text = None if preferred is None else str(preferred)
    if preferred_text is not None:
        for option in options:
            if str(option) == preferred_text:
                return option
    if fallback in options:
        return fallback
    fallback_text = None if fallback is None else str(fallback)
    if fallback_text is not None:
        for option in options:
            if str(option) == fallback_text:
                return option
    return options[0] if options else fallback


def pick_option_index(options: List[Any], preferred: Any, fallback: Any = None) -> int:
    if not options:
        return 0
    return options.index(pick_option(options, preferred, fallback))


def pick_option_list(options: List[Any], preferred: Any, fallback: List[Any] | None = None, limit: int | None = None) -> List[Any]:
    values = preferred if isinstance(preferred, list) else fallback or []
    picked: List[Any] = []
    for value in values:
        resolved = pick_option(options, value, None)
        if resolved in options and resolved not in picked:
            picked.append(resolved)
    if not picked:
        for value in fallback or []:
            resolved = pick_option(options, value, None)
            if resolved in options and resolved not in picked:
                picked.append(resolved)
    if limit is not None:
        picked = picked[:limit]
    return picked


def order_coin_options(options: List[str]) -> List[str]:
    cleaned = [str(option or "").strip().upper() for option in options if str(option or "").strip()]
    unique_options = list(dict.fromkeys(cleaned))
    pinned = [coin for coin in POPULAR_COINS if coin in unique_options]
    extras = sorted([coin for coin in unique_options if coin not in pinned])
    return pinned + extras


def build_catalog_summary_text(
    summary: Dict[str, Dict[str, Any]],
    catalog_status: Dict[str, Dict[str, str]],
) -> str:
    parts: List[str] = []
    for exchange_key in EXCHANGE_ORDER:
        exchange_name = EXCHANGE_TITLES[exchange_key]
        summary_row = summary.get(exchange_key) or {}
        status_row = catalog_status.get(exchange_key) or {}
        perp_status = str(status_row.get("perp") or "")
        spot_status = str(status_row.get("spot") or "")
        perp_label = "合约目录受限" if perp_status == "error" else f"合约 {int(summary_row.get('perp') or 0)}"
        if exchange_key == "hyperliquid":
            spot_label = "现货未接入"
        else:
            spot_label = "现货目录受限" if spot_status == "error" else f"现货 {int(summary_row.get('spot') or 0)}"
        parts.append(f"{exchange_name} {perp_label} / {spot_label}")
    return " | ".join(parts)


def build_coin_availability_caption(
    base_coin: str,
    availability: Dict[str, Dict[str, Dict[str, bool]]],
    catalog_status: Dict[str, Dict[str, str]] | None = None,
) -> str:
    normalized_coin = str(base_coin or "").strip().upper()
    if not normalized_coin:
        return ""
    availability_row = availability.get(normalized_coin) or {}
    catalog_status = catalog_status or {}
    catalog_errors = [
        EXCHANGE_TITLES[exchange_key]
        for exchange_key in EXCHANGE_ORDER
        if "error" in {str((catalog_status.get(exchange_key) or {}).get("perp") or ""), str((catalog_status.get(exchange_key) or {}).get("spot") or "")}
    ]
    if not availability_row:
        if catalog_errors:
            return f"{normalized_coin}: {', '.join(catalog_errors)} 目录受限，已按默认符号继续尝试真实行情，不再直接判成未上架。"
        return f"{normalized_coin}: 目录里还没命中，仍然可以手动输入并用下面的合约映射框继续尝试。"
    parts: List[str] = []
    missing: List[str] = []
    for exchange_key in EXCHANGE_ORDER:
        exchange_row = availability_row.get(exchange_key) or {}
        status_row = catalog_status.get(exchange_key) or {}
        market_labels: List[str] = []
        if exchange_row.get("perp"):
            market_labels.append("合约")
        if exchange_row.get("spot"):
            market_labels.append("现货")
        if market_labels:
            parts.append(f"{EXCHANGE_TITLES[exchange_key]} {'/'.join(market_labels)}")
        elif "error" in {str(status_row.get("perp") or ""), str(status_row.get("spot") or "")}:
            parts.append(f"{EXCHANGE_TITLES[exchange_key]} 目录受限(按默认符号继续尝试)")
        else:
            missing.append(EXCHANGE_TITLES[exchange_key])
    caption = " | ".join(parts) if parts else f"{normalized_coin}: 当前目录还没发现可用市场。"
    if missing:
        caption += " | 缺少: " + " / ".join(missing)
    return caption


def resolve_coin_market_availability(
    base_coin: str,
    availability: Dict[str, Dict[str, Dict[str, bool]]],
    catalog_status: Dict[str, Dict[str, str]] | None = None,
) -> Tuple[List[str], List[str]]:
    normalized_coin = str(base_coin or "").strip().upper()
    availability_row = availability.get(normalized_coin) or {}
    catalog_status = catalog_status or {}
    perp_keys = [
        exchange_key
        for exchange_key in EXCHANGE_ORDER
        if (availability_row.get(exchange_key) or {}).get("perp") or str((catalog_status.get(exchange_key) or {}).get("perp") or "") == "error"
    ]
    spot_keys = [
        exchange_key
        for exchange_key in SPOT_EXCHANGE_ORDER
        if (availability_row.get(exchange_key) or {}).get("spot") or str((catalog_status.get(exchange_key) or {}).get("spot") or "") == "error"
    ]
    return perp_keys, spot_keys


def filter_symbol_maps_for_coin(
    symbol_map: Dict[str, str],
    spot_symbol_map: Dict[str, str],
    available_perp_keys: List[str],
    available_spot_keys: List[str],
) -> Tuple[Dict[str, str], Dict[str, str]]:
    filtered_symbol_map = {
        exchange_key: str(symbol_map.get(exchange_key) or "").strip().upper() if exchange_key in available_perp_keys else ""
        for exchange_key in EXCHANGE_ORDER
    }
    filtered_spot_symbol_map = {
        exchange_key: str(spot_symbol_map.get(exchange_key) or "").strip().upper() if exchange_key in available_spot_keys else ""
        for exchange_key in SPOT_EXCHANGE_ORDER
    }
    return filtered_symbol_map, filtered_spot_symbol_map


def resolve_effective_exchange(selected_exchange: str, available_perp_keys: List[str]) -> Tuple[str, str]:
    preferred_exchange = str(selected_exchange or "").strip().lower()
    if preferred_exchange in available_perp_keys:
        return preferred_exchange, ""
    if available_perp_keys:
        fallback_exchange = available_perp_keys[0]
        return (
            fallback_exchange,
            f"当前币种未在 {EXCHANGE_TITLES.get(preferred_exchange, preferred_exchange.title() or preferred_exchange)} 的合约目录中命中，已自动切到 {EXCHANGE_TITLES.get(fallback_exchange, fallback_exchange)}。",
        )
    return preferred_exchange or EXCHANGE_ORDER[0], f"{str(selected_exchange or '').upper() or '当前币种'} 在已扫描目录里还没有可用合约市场。"

def _read_local_ui_preferences_raw() -> Dict[str, Any]:
    try:
        loaded = json.loads(LOCAL_UI_PREFERENCES_PATH.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return {}
    return loaded if isinstance(loaded, dict) else {}


def _write_local_ui_preferences_raw(preferences: Dict[str, Any]) -> bool:
    try:
        temp_path = LOCAL_UI_PREFERENCES_PATH.with_suffix(".tmp")
        temp_path.write_text(json.dumps(preferences, ensure_ascii=True, indent=2), encoding="utf-8")
        temp_path.replace(LOCAL_UI_PREFERENCES_PATH)
    except OSError:
        return False
    return True


def _dpapi_blob_from_bytes(data: bytes) -> tuple[Any, Any]:
    if _DpapiBlob is None or not data:
        return _DpapiBlob(0, ctypes.POINTER(ctypes.c_byte)()) if _DpapiBlob is not None else None, None
    buffer = ctypes.create_string_buffer(data)
    blob = _DpapiBlob(len(data), ctypes.cast(buffer, ctypes.POINTER(ctypes.c_byte)))
    return blob, buffer


def _dpapi_encrypt_bytes(data: bytes) -> bytes:
    if os.name != "nt" or not data or _DpapiBlob is None or _CRYPT32 is None or _KERNEL32 is None:
        return data
    input_blob, input_buffer = _dpapi_blob_from_bytes(data)
    entropy_blob, entropy_buffer = _dpapi_blob_from_bytes(LOCAL_UI_DPAPI_ENTROPY)
    output_blob = _DpapiBlob()
    if not _CRYPT32.CryptProtectData(
        ctypes.byref(input_blob),
        "Liquid Glass Flow Desk",
        ctypes.byref(entropy_blob) if entropy_blob is not None else None,
        None,
        None,
        0,
        ctypes.byref(output_blob),
    ):
        raise ctypes.WinError()
    try:
        return ctypes.string_at(output_blob.pbData, output_blob.cbData)
    finally:
        _KERNEL32.LocalFree(ctypes.cast(output_blob.pbData, ctypes.c_void_p))


def _dpapi_decrypt_bytes(data: bytes) -> bytes:
    if os.name != "nt" or not data or _DpapiBlob is None or _CRYPT32 is None or _KERNEL32 is None:
        return data
    input_blob, input_buffer = _dpapi_blob_from_bytes(data)
    entropy_blob, entropy_buffer = _dpapi_blob_from_bytes(LOCAL_UI_DPAPI_ENTROPY)
    output_blob = _DpapiBlob()
    if not _CRYPT32.CryptUnprotectData(
        ctypes.byref(input_blob),
        None,
        ctypes.byref(entropy_blob) if entropy_blob is not None else None,
        None,
        None,
        0,
        ctypes.byref(output_blob),
    ):
        raise ctypes.WinError()
    try:
        return ctypes.string_at(output_blob.pbData, output_blob.cbData)
    finally:
        _KERNEL32.LocalFree(ctypes.cast(output_blob.pbData, ctypes.c_void_p))


def _encrypt_local_ui_secret(value: Any) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    if raw.startswith(LOCAL_UI_SECRET_PREFIX):
        return raw
    try:
        encrypted = _dpapi_encrypt_bytes(raw.encode("utf-8"))
    except Exception:
        return raw
    return LOCAL_UI_SECRET_PREFIX + base64.urlsafe_b64encode(encrypted).decode("ascii")


def _decrypt_local_ui_secret(value: Any) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    if not raw.startswith(LOCAL_UI_SECRET_PREFIX):
        return raw
    payload = raw[len(LOCAL_UI_SECRET_PREFIX):]
    try:
        encrypted = base64.urlsafe_b64decode(payload.encode("ascii"))
        return _dpapi_decrypt_bytes(encrypted).decode("utf-8")
    except Exception:
        return ""


def _hydrate_local_ui_preferences(raw_preferences: Dict[str, Any]) -> tuple[Dict[str, Any], bool]:
    normalized = dict(raw_preferences or {})
    needs_migration = False
    for field in LOCAL_UI_SECRET_FIELDS:
        raw_value = raw_preferences.get(field)
        normalized[field] = _decrypt_local_ui_secret(raw_value)
        if str(raw_value or "").strip() and not str(raw_value).startswith(LOCAL_UI_SECRET_PREFIX):
            needs_migration = True
    for field, env_name in LOCAL_UI_SECRET_ENV_OVERRIDES.items():
        env_value = str(os.getenv(env_name) or "").strip()
        if env_value:
            normalized[field] = env_value
    return normalized, needs_migration


def _serialize_local_ui_preferences(preferences: Dict[str, Any]) -> Dict[str, Any]:
    normalized = dict(preferences or {})
    for field in LOCAL_UI_SECRET_FIELDS:
        normalized[field] = _encrypt_local_ui_secret(normalized.get(field))
    return normalized


def load_local_ui_preferences() -> Dict[str, Any]:
    cached = st.session_state.get("_local_ui_preferences")
    if isinstance(cached, dict):
        return cached
    raw_preferences = _read_local_ui_preferences_raw()
    preferences, needs_migration = _hydrate_local_ui_preferences(raw_preferences)
    if needs_migration:
        _write_local_ui_preferences_raw(_serialize_local_ui_preferences(preferences))
    st.session_state["_local_ui_preferences"] = preferences
    return preferences


def save_local_ui_preferences(preferences: Dict[str, Any]) -> None:
    current = load_local_ui_preferences()
    normalized = dict(preferences)
    if current == normalized:
        return
    serialized = _serialize_local_ui_preferences(normalized)
    if not _write_local_ui_preferences_raw(serialized):
        return
    st.session_state["_local_ui_preferences"] = normalized


def resolve_exchange_scope(
    selected_exchange: str,
    scope_mode: str,
    available_spot_keys: List[str] | None = None,
    available_perp_keys: List[str] | None = None,
) -> Tuple[str, List[str], List[str], List[str]]:
    resolved_spot_keys = list(available_spot_keys or [key for key in SPOT_EXCHANGE_ORDER])
    resolved_perp_keys = list(available_perp_keys or [key for key in EXCHANGE_ORDER])
    spot_focus_exchange = selected_exchange if selected_exchange in resolved_spot_keys else (resolved_spot_keys[0] if resolved_spot_keys else "binance")
    if scope_mode == "当前交易所优先":
        spot_keys = [spot_focus_exchange] if resolved_spot_keys and spot_focus_exchange in resolved_spot_keys else []
        perp_dashboard_keys = [selected_exchange] if selected_exchange in resolved_perp_keys else resolved_perp_keys[:1]
    else:
        spot_keys = resolved_spot_keys
        perp_dashboard_keys = resolved_perp_keys
    perp_reference_keys: List[str] = []
    for exchange_key in perp_dashboard_keys + spot_keys:
        if exchange_key in EXCHANGE_ORDER and exchange_key not in perp_reference_keys:
            perp_reference_keys.append(exchange_key)
    return spot_focus_exchange, spot_keys, perp_dashboard_keys, perp_reference_keys

def status_caption(snapshot: ExchangeSnapshot) -> str:
    return f"{snapshot.exchange}: 正常" if snapshot.status == "ok" else f"{snapshot.exchange}: {snapshot.error or '异常'}"


def resolve_service(
    symbol_map: Dict[str, str],
    timeout: int,
    sample_seconds: int,
    spot_symbol_map: Dict[str, str],
    performance_settings: Dict[str, int],
    force_restart: bool,
) -> LiveTerminalService:
    service_key = (
        tuple(sorted(symbol_map.items())),
        timeout,
        sample_seconds,
        tuple(sorted(spot_symbol_map.items())),
        tuple(sorted(performance_settings.items())),
    )
    current_key = st.session_state.get("live_service_key")
    service = st.session_state.get("live_service")
    if force_restart and service is not None:
        service.stop()
        service = None
        current_key = None
    if service is None or current_key != service_key:
        if service is not None:
            service.stop()
        service = LiveTerminalService(
            symbol_map,
            timeout=timeout,
            sample_seconds=sample_seconds,
            spot_symbol=spot_symbol_map.get("binance", "BTCUSDT"),
            spot_symbol_map=spot_symbol_map,
            **performance_settings,
        )
        st.session_state["live_service"] = service
        st.session_state["live_service_key"] = service_key
    return service


def resolve_hyperliquid_user_stream(
    address: str,
    coin: str,
    timeout: int,
    lookback_hours: int,
    force_restart: bool,
) -> HyperliquidAddressStreamService | None:
    normalized_address = str(address or "").strip().lower()
    if not is_valid_onchain_address(normalized_address):
        existing = st.session_state.get("hyperliquid_user_stream")
        if existing is not None:
            try:
                existing.stop()
            except Exception:
                pass
            st.session_state.pop("hyperliquid_user_stream", None)
            st.session_state.pop("hyperliquid_user_stream_key", None)
        return None

    stream_key = (normalized_address, str(coin or "").strip().upper(), timeout, int(lookback_hours))
    current_key = st.session_state.get("hyperliquid_user_stream_key")
    stream = st.session_state.get("hyperliquid_user_stream")
    if force_restart and stream is not None:
        stream.stop()
        stream = None
        current_key = None
    if stream is None or current_key != stream_key:
        if stream is not None:
            stream.stop()
        stream = HyperliquidAddressStreamService(
            normalized_address,
            str(coin or "").strip().upper(),
            timeout=timeout,
            lookback_hours=int(lookback_hours),
        )
        st.session_state["hyperliquid_user_stream"] = stream
        st.session_state["hyperliquid_user_stream_key"] = stream_key
    return stream


def build_snapshot_frame(snapshots: List[ExchangeSnapshot]) -> pd.DataFrame:
    rows = []
    for snapshot in snapshots:
        rows.append({
            "交易所": snapshot.exchange,
            "合约": snapshot.symbol,
            "最新价": snapshot.last_price,
            "参考价": snapshot.mark_price,
            "现货参考": snapshot.index_price,
            "价格偏离(%)": snapshot.premium_pct,
            "持仓量": snapshot.open_interest,
            "持仓金额": snapshot.open_interest_notional,
            "费率(bps)": snapshot.funding_bps,
            "24h 成交额": snapshot.volume_24h_notional,
            "时间": format_display_timestamp_ms(snapshot.timestamp_ms),
            "状态": CARD_STATUS.get(snapshot.status, snapshot.status),
            "异常信息": snapshot.error,
        })
    return pd.DataFrame(rows)


def build_spot_dashboard_frame(
    spot_snapshot_map: Dict[str, SpotSnapshot],
    spot_summary_map: Dict[str, Dict[str, float | None]],
    lead_lag_frame: pd.DataFrame,
    exchange_keys: List[str] | None = None,
) -> pd.DataFrame:
    lead_lag_lookup = {}
    if not lead_lag_frame.empty and "交易所" in lead_lag_frame.columns:
        lead_lag_lookup = {str(row["交易所"]): row for _, row in lead_lag_frame.iterrows()}
    rows = []
    for exchange_key in exchange_keys or list(SPOT_EXCHANGE_ORDER):
        exchange_name = EXCHANGE_TITLES[exchange_key]
        snapshot = spot_snapshot_map.get(exchange_key)
        summary = spot_summary_map.get(exchange_key, {})
        lead_row = lead_lag_lookup.get(exchange_name, {})
        rows.append(
            {
                "交易所": exchange_name,
                "现货价格": None if snapshot is None or snapshot.status != "ok" else snapshot.last_price,
                "买一": None if snapshot is None or snapshot.status != "ok" else snapshot.bid_price,
                "卖一": None if snapshot is None or snapshot.status != "ok" else snapshot.ask_price,
                "现货价差(bps)": None if snapshot is None else snapshot.spread_bps,
                "盘口失衡(%)": summary.get("imbalance_pct"),
                "买盘挂单金额": summary.get("bid_notional"),
                "卖盘挂单金额": summary.get("ask_notional"),
                "24h成交额": None if snapshot is None or snapshot.status != "ok" else snapshot.volume_24h_notional,
                "短线提示": lead_row.get("提示") or "样本不足",
            }
        )
    return pd.DataFrame(rows)


def build_perp_dashboard_frame(
    snapshot_by_key: Dict[str, ExchangeSnapshot],
    perp_summary_map: Dict[str, Dict[str, float | None]],
    oi_metrics_by_exchange: Dict[str, Dict[str, float | str | None]],
    contract_ratio_map: Dict[str, float | None] | None = None,
    exchange_keys: List[str] | None = None,
) -> pd.DataFrame:
    rows = []
    for exchange_key in exchange_keys or list(EXCHANGE_ORDER):
        exchange_name = EXCHANGE_TITLES[exchange_key]
        snapshot = snapshot_by_key[exchange_key]
        summary = perp_summary_map.get(exchange_key, {})
        oi_metrics = oi_metrics_by_exchange.get(exchange_name, {})
        rows.append(
            {
                "交易所": exchange_name,
                "合约价格": snapshot.last_price if snapshot.status == "ok" else None,
                "资金费率(bps)": snapshot.funding_bps,
                "未平仓量": snapshot.open_interest,
                "未平仓金额": snapshot.open_interest_notional,
                "OI变化(%)": oi_metrics.get("oi_change_pct"),
                "价格变化(%)": oi_metrics.get("price_change_pct"),
                "加减仓状态": oi_metrics.get("label"),
                "合约价差(bps)": summary.get("spread_bps"),
                "盘口失衡(%)": summary.get("imbalance_pct"),
                "24h成交额": snapshot.volume_24h_notional,
                "合约多空比": None if contract_ratio_map is None else contract_ratio_map.get(exchange_key),
            }
        )
    frame = pd.DataFrame(rows)
    if frame.empty:
        return frame
    frame["_ratio_distance"] = (pd.to_numeric(frame.get("合约多空比"), errors="coerce") - 1.0).abs()
    frame["_oi_rank"] = pd.to_numeric(frame.get("未平仓金额"), errors="coerce")
    return (
        frame.sort_values(["_ratio_distance", "_oi_rank", "交易所"], ascending=[False, False, True], na_position="last")
        .drop(columns=["_ratio_distance", "_oi_rank"])
        .reset_index(drop=True)
    )


def rgb_from_hex(color_hex: str) -> Tuple[int, int, int]:
    color_hex = color_hex.lstrip("#")
    return int(color_hex[0:2], 16), int(color_hex[2:4], 16), int(color_hex[4:6], 16)


def rgba_from_hex(color_hex: str, alpha: float) -> str:
    red, green, blue = rgb_from_hex(color_hex)
    return f"rgba({red}, {green}, {blue}, {alpha:.3f})"


def palette_color(side: str, intensity: float) -> str:
    palette = BID_PALETTE if side == "bid" else ASK_PALETTE
    index = int(round(max(0.0, min(1.0, intensity)) * (len(palette) - 1)))
    return palette[index]

def aggregate_heat_bars(levels: List[OrderBookLevel], reference_price: float | None, window_pct: float, buckets_per_side: int, bars_per_side: int) -> List[Dict[str, float]]:
    if not levels:
        return []
    if reference_price is None or reference_price <= 0:
        bids = [level.price for level in levels if level.side == "bid"]
        asks = [level.price for level in levels if level.side == "ask"]
        reference_price = (max(bids) + min(asks)) * 0.5 if bids and asks else (max(bids) if bids else min(asks) if asks else None)
    if reference_price is None or reference_price <= 0:
        return []
    lower_bound = reference_price * (1.0 - window_pct / 100.0)
    upper_bound = reference_price * (1.0 + window_pct / 100.0)

    def bucketize(side: str, lower: float, upper: float) -> List[Dict[str, float]]:
        side_levels = [level for level in levels if level.side == side and lower <= level.price <= upper and level.size > 0]
        if not side_levels:
            return []
        step = max((upper - lower) / max(buckets_per_side, 1), reference_price * 0.00015)
        buckets: Dict[int, Dict[str, float]] = {}
        for level in side_levels:
            idx = max(0, min(buckets_per_side - 1, int((level.price - lower) / step)))
            bucket = buckets.setdefault(idx, {"side": side, "price_low": lower + idx * step, "price_high": lower + (idx + 1) * step, "size": 0.0})
            bucket["size"] += level.size
        ranked = sorted(buckets.values(), key=lambda item: item["size"], reverse=True)[:bars_per_side]
        ranked.sort(key=lambda item: item["price_high"], reverse=(side == "bid"))
        return ranked

    all_bars = bucketize("bid", lower_bound, reference_price) + bucketize("ask", reference_price, upper_bound)
    if not all_bars:
        return []
    max_size = max(bar["size"] for bar in all_bars)
    for bar in all_bars:
        bar["mid_price"] = (bar["price_low"] + bar["price_high"]) * 0.5
        bar["distance_pct"] = abs(bar["mid_price"] - reference_price) / reference_price * 100.0
        bar["intensity"] = 0.0 if max_size <= 0 else bar["size"] / max_size
    return all_bars


def merge_oi_points(backfill: List[OIPoint], session_points: List[OIPoint]) -> List[OIPoint]:
    merged: Dict[int, OIPoint] = {}
    for point in backfill + session_points:
        existing = merged.get(point.timestamp_ms)
        if existing is None:
            merged[point.timestamp_ms] = OIPoint(point.timestamp_ms, point.open_interest, point.open_interest_notional)
        else:
            if existing.open_interest is None and point.open_interest is not None:
                existing.open_interest = point.open_interest
            if existing.open_interest_notional is None and point.open_interest_notional is not None:
                existing.open_interest_notional = point.open_interest_notional
    return sorted(merged.values(), key=lambda item: item.timestamp_ms)


def build_terminal_chart(candles: List[Candle], heat_bars: List[Dict[str, float]], snapshot: ExchangeSnapshot, interval: str) -> go.Figure:
    figure = make_subplots(rows=2, cols=1, shared_xaxes=True, vertical_spacing=0.04, row_heights=[0.78, 0.22])
    if not candles:
        figure.add_annotation(text="没有可用 K 线数据", showarrow=False, x=0.5, y=0.5, xref="paper", yref="paper")
        return figure
    frame = pd.DataFrame({
        "ts": pd.to_datetime([item.timestamp_ms for item in candles], unit="ms"),
        "open": [item.open for item in candles],
        "high": [item.high for item in candles],
        "low": [item.low for item in candles],
        "close": [item.close for item in candles],
        "volume": [item.volume for item in candles],
    })
    up_color, down_color = "#1dc796", "#ff6868"
    figure.add_trace(go.Candlestick(x=frame["ts"], open=frame["open"], high=frame["high"], low=frame["low"], close=frame["close"], increasing_line_color=up_color, increasing_fillcolor=up_color, decreasing_line_color=down_color, decreasing_fillcolor=down_color, name="K线"), row=1, col=1)
    volume_colors = [up_color if close_price >= open_price else down_color for open_price, close_price in zip(frame["open"], frame["close"])]
    figure.add_trace(go.Bar(x=frame["ts"], y=frame["volume"], marker_color=volume_colors, name="成交量", opacity=0.55), row=2, col=1)
    x_start_ms = candles[0].timestamp_ms
    x_end_ms = candles[-1].timestamp_ms + interval_to_millis(interval)
    span_ms = max(x_end_ms - x_start_ms, interval_to_millis(interval) * 20)
    for bar in heat_bars:
        intensity = bar["intensity"]
        x0_ms = int(x_end_ms - span_ms * (0.28 + 0.72 * intensity))
        color_hex = palette_color(str(bar["side"]), intensity)
        figure.add_shape(type="rect", x0=pd.to_datetime(x0_ms, unit="ms"), x1=pd.to_datetime(x_end_ms, unit="ms"), y0=bar["price_low"], y1=bar["price_high"], fillcolor=rgba_from_hex(color_hex, 0.24 + 0.34 * intensity), line_width=0, layer="below", row=1, col=1)
    last_price = snapshot.last_price or candles[-1].close
    figure.add_hline(y=last_price, line_color="#f8d35e", line_dash="dot", line_width=1, row=1, col=1)
    if snapshot.mark_price is not None:
        figure.add_hline(y=snapshot.mark_price, line_color="#8fd3ff", line_dash="dash", line_width=1, row=1, col=1)
    figure.update_layout(
        height=760,
        margin=dict(l=12, r=12, t=62, b=12),
        paper_bgcolor="rgba(14, 22, 35, 0.56)",
        plot_bgcolor="rgba(255, 255, 255, 0.045)",
        font=dict(color="#f6f9ff", family="SF Pro Display, Segoe UI, sans-serif"),
        title=dict(text="Price Structure & Liquidity", x=0.02, y=0.98, xanchor="left", font=dict(size=20, color="#f8fbff")),
        transition=dict(duration=320, easing="cubic-in-out"),
        xaxis_rangeslider_visible=False,
    )
    figure.update_xaxes(showgrid=False, zeroline=False)
    figure.update_yaxes(showgrid=True, gridcolor="rgba(255, 255, 255, 0.08)", side="right")
    return figure


def build_oi_figure(points: List[OIPoint]) -> Tuple[go.Figure, str]:
    figure = make_subplots(
        rows=2,
        cols=1,
        shared_xaxes=True,
        vertical_spacing=0.06,
        row_heights=[0.68, 0.32],
    )
    if not points:
        figure.add_annotation(text="等待 OI 历史采样", showarrow=False, x=0.5, y=0.5, xref="paper", yref="paper")
        figure.update_layout(height=340, margin=dict(l=12, r=12, t=24, b=12))
        return figure, "持仓历史"
    frame = pd.DataFrame({"ts": pd.to_datetime([item.timestamp_ms for item in points], unit="ms"), "oi": [item.open_interest for item in points], "oi_notional": [item.open_interest_notional for item in points]}).sort_values("ts")
    use_notional = frame["oi_notional"].notna().sum() >= max(3, len(frame) // 4)
    value_col = "oi_notional" if use_notional else "oi"
    label = "持仓金额变化" if use_notional else "持仓量变化"
    frame = frame.dropna(subset=[value_col])
    if frame.empty:
        figure.add_annotation(text="等待 OI 历史采样", showarrow=False, x=0.5, y=0.5, xref="paper", yref="paper")
    else:
        frame["delta"] = frame[value_col].diff()
        frame["delta_pct"] = frame[value_col].pct_change() * 100.0
        delta_colors = ["#67d1ff" if pd.isna(value) or value >= 0 else "#ff9a59" for value in frame["delta"]]
        figure.add_trace(
            go.Scatter(
                x=frame["ts"],
                y=frame[value_col],
                mode="lines",
                line=dict(color="#62c2ff", width=2.4),
                fill="tozeroy",
                fillcolor="rgba(98, 194, 255, 0.16)",
                name=label,
                hovertemplate="时间 %{x}<br>" + label + " %{y:,.0f}<extra></extra>",
            ),
            row=1,
            col=1,
        )
        figure.add_trace(
            go.Bar(
                x=frame["ts"],
                y=frame["delta"].fillna(0.0),
                marker_color=delta_colors,
                name="OI Delta",
                hovertemplate="时间 %{x}<br>Delta %{y:,.0f}<extra></extra>",
            ),
            row=2,
            col=1,
        )
        latest_value = frame[value_col].iloc[-1]
        latest_delta = frame["delta"].iloc[-1] if len(frame) > 1 else None
        latest_delta_pct = frame["delta_pct"].iloc[-1] if len(frame) > 1 else None
        figure.add_annotation(
            text=f"最新 {label} {latest_value:,.0f}<br>单步Δ {latest_delta:+,.0f}" + (f" ({latest_delta_pct:+.2f}%)" if latest_delta_pct is not None and not pd.isna(latest_delta_pct) else ""),
            showarrow=False,
            x=0.99,
            y=0.98,
            xref="paper",
            yref="paper",
            xanchor="right",
            align="right",
            font=dict(size=12, color="#dcecff"),
            bgcolor="rgba(8, 20, 34, 0.55)",
            bordercolor="rgba(255, 255, 255, 0.12)",
            borderwidth=1,
        )
        figure.add_hline(y=0, line_color="rgba(223, 232, 241, 0.22)", line_width=1, row=2, col=1)
    figure.update_layout(
        height=340,
        margin=dict(l=12, r=12, t=56, b=10),
        paper_bgcolor="rgba(14, 22, 35, 0.56)",
        plot_bgcolor="rgba(255, 255, 255, 0.045)",
        font=dict(color="#f6f9ff", family="SF Pro Display, Segoe UI, sans-serif"),
        title=dict(text=label, x=0.03, y=0.98, xanchor="left", font=dict(size=17, color="#f3f8ff")),
        transition=dict(duration=280, easing="cubic-in-out"),
        showlegend=False,
    )
    figure.update_xaxes(showgrid=False, row=1, col=1)
    figure.update_xaxes(showgrid=False, row=2, col=1)
    figure.update_yaxes(showgrid=True, gridcolor="rgba(255, 255, 255, 0.08)", tickformat=".2s", row=1, col=1)
    figure.update_yaxes(showgrid=True, gridcolor="rgba(255, 255, 255, 0.08)", tickformat=".2s", row=2, col=1)
    return figure, label


def build_heat_frame(heat_bars: List[Dict[str, float]]) -> pd.DataFrame:
    return pd.DataFrame([
        {"方向": "下方买盘墙" if bar["side"] == "bid" else "上方卖盘墙", "价格区间": f"{bar['price_low']:,.2f} - {bar['price_high']:,.2f}", "挂单量": bar["size"], "热度": bar["intensity"], "离现价": bar["distance_pct"]}
        for bar in heat_bars
    ])


def render_directional_zone_tables(
    below_frame: pd.DataFrame,
    above_frame: pd.DataFrame,
    below_title: str,
    above_title: str,
) -> None:
    columns = st.columns(2, gap="large")
    specs = (
        (columns[0], below_title, below_frame),
        (columns[1], above_title, above_frame),
    )
    for column, title, frame in specs:
        with column:
            st.caption(title)
            if frame.empty:
                st.info("当前没有可展示的热区。")
            else:
                st.dataframe(
                    frame,
                    width="stretch",
                    hide_index=True,
                    column_config={
                        "热度": st.column_config.ProgressColumn(format="%.2f", min_value=0.0, max_value=1.0),
                    },
                )


def ensure_alert_engine_buffers(scope_key: str) -> tuple[Dict[str, Dict[str, object]], List[Dict[str, object]]]:
    state_key = f"alert_engine_state::{scope_key}"
    timeline_key = f"alert_engine_timeline::{scope_key}"
    if state_key not in st.session_state:
        st.session_state[state_key] = {}
    if timeline_key not in st.session_state:
        st.session_state[timeline_key] = []
    return st.session_state[state_key], st.session_state[timeline_key]

LEGACY_QUERY_VIEW_ALIASES = {
    "overview": "首页总览",
    "home": "首页总览",
    "首页总览": "首页总览",
    "depth": "交易台深度页",
    "desk": "交易台深度页",
    "trading-desk": "交易台深度页",
    "交易台深度页": "交易台深度页",
    "alerts": "告警中心",
    "alert-center": "告警中心",
    "告警中心": "告警中心",
    "liquidations": "爆仓中心",
    "liquidation-center": "爆仓中心",
    "爆仓中心": "爆仓中心",
    "orderbook": "盘口中心",
    "book": "盘口中心",
    "盘口中心": "盘口中心",
    "lab": "增强实验室",
    "enhanced-lab": "增强实验室",
    "增强实验室": "增强实验室",
    "debug": "接口调试",
    "接口调试": "接口调试",
}
LEGACY_QUERY_LAB_ALIASES = {
    "overview": "总览",
    "总览": "总览",
    "hyper": "Hyperliquid",
    "hyperliquid": "Hyperliquid",
    "跨所聚合": "跨所聚合",
    "aggregate": "跨所聚合",
    "aggregation": "跨所聚合",
    "strategy": "策略层",
    "策略层": "策略层",
    "cross-coin": "跨币种联动",
    "crosscoin": "跨币种联动",
    "跨币种联动": "跨币种联动",
    "history": "通知与持久化",
    "storage": "通知与持久化",
    "notify": "通知与持久化",
    "通知与持久化": "通知与持久化",
}
LEGACY_QUERY_OVERVIEW_MODE_ALIASES = {
    "light": "轻量",
    "lite": "轻量",
    "轻量": "轻量",
    "full": "完整",
    "complete": "完整",
    "完整": "完整",
}
LEGACY_QUERY_DESK_MODE_ALIASES = {
    "core": "核心",
    "核心": "核心",
    "full": "完整",
    "complete": "完整",
    "完整": "完整",
}
LEGACY_QUERY_DEEP_MARKET_ALIASES = {
    "all": "综合深度",
    "mixed": "综合深度",
    "综合深度": "综合深度",
    "spot": "现货深度",
    "现货深度": "现货深度",
    "perp": "合约深度",
    "futures": "合约深度",
    "合约深度": "合约深度",
}
LEGACY_QUERY_SCOPE_ALIASES = {
    "focused": "当前交易所优先",
    "current": "当前交易所优先",
    "current-first": "当前交易所优先",
    "当前交易所优先": "当前交易所优先",
    "all": "全部交易所",
    "cross": "全部交易所",
    "全部交易所": "全部交易所",
}


def load_streamlit_query_params() -> Dict[str, str]:
    params: Dict[str, str] = {}
    query_params = getattr(st, "query_params", None)
    if query_params is not None:
        try:
            items = list(query_params.items())
        except Exception:
            items = []
        for key, value in items:
            if isinstance(value, list):
                params[str(key)] = "" if not value else str(value[-1])
            else:
                params[str(key)] = str(value)
        return params
    try:
        legacy_params = st.experimental_get_query_params()
    except Exception:
        legacy_params = {}
    for key, value in legacy_params.items():
        if isinstance(value, list):
            params[str(key)] = "" if not value else str(value[-1])
        else:
            params[str(key)] = str(value)
    return params


def _normalize_query_choice(value: object, mapping: Dict[str, str]) -> str | None:
    text = str(value or "").strip()
    if not text:
        return None
    if text in mapping.values():
        return text
    return mapping.get(text.lower())


def _query_flag(value: object) -> bool | None:
    text = str(value or "").strip().lower()
    if not text:
        return None
    if text in {"1", "true", "yes", "on"}:
        return True
    if text in {"0", "false", "no", "off"}:
        return False
    return None


def build_query_ui_overrides(coin_catalog_options: List[str]) -> Dict[str, Any]:
    params = load_streamlit_query_params()
    overrides: Dict[str, Any] = {}
    coin = str(params.get("coin") or params.get("base_coin") or "").strip().upper()
    if coin:
        overrides["custom_coin"] = coin
        if coin in coin_catalog_options:
            overrides["catalog_coin"] = coin
            overrides["preset_coin"] = coin
    exchange = str(params.get("exchange") or params.get("selected_exchange") or "").strip().lower()
    if exchange in EXCHANGE_ORDER:
        overrides["selected_exchange"] = exchange
    interval = str(params.get("interval") or "").strip()
    if interval in SUPPORTED_INTERVALS:
        overrides["interval"] = interval
    feature = str(params.get("feature") or params.get("legacy_feature") or "").strip().lower()
    active_view = _normalize_query_choice(params.get("view"), LEGACY_QUERY_VIEW_ALIASES)
    lab_mode = _normalize_query_choice(params.get("lab_mode"), LEGACY_QUERY_LAB_ALIASES)
    overview_mode = _normalize_query_choice(params.get("overview_mode"), LEGACY_QUERY_OVERVIEW_MODE_ALIASES)
    desk_mode = _normalize_query_choice(params.get("desk_mode"), LEGACY_QUERY_DESK_MODE_ALIASES)
    deep_market_mode = _normalize_query_choice(params.get("deep_market_mode"), LEGACY_QUERY_DEEP_MARKET_ALIASES)
    exchange_scope_mode = _normalize_query_choice(params.get("exchange_scope"), LEGACY_QUERY_SCOPE_ALIASES)
    feature_defaults = {
        "home": {"active_view": "首页总览", "overview_mode": "完整"},
        "overview": {"active_view": "首页总览", "overview_mode": "完整"},
        "depth": {"active_view": "交易台深度页", "desk_mode": "完整"},
        "alerts": {"active_view": "告警中心", "exchange_scope_mode": "全部交易所"},
        "liquidations": {"active_view": "爆仓中心", "exchange_scope_mode": "全部交易所"},
        "orderbook": {"active_view": "盘口中心"},
        "lab": {"active_view": "增强实验室", "lab_mode": "总览", "exchange_scope_mode": "全部交易所"},
        "lab-hyper": {"active_view": "增强实验室", "lab_mode": "Hyperliquid", "exchange_scope_mode": "全部交易所"},
        "lab-aggregate": {"active_view": "增强实验室", "lab_mode": "跨所聚合", "exchange_scope_mode": "全部交易所"},
        "lab-strategy": {"active_view": "增强实验室", "lab_mode": "策略层", "exchange_scope_mode": "全部交易所"},
        "lab-cross-coin": {"active_view": "增强实验室", "lab_mode": "跨币种联动", "exchange_scope_mode": "全部交易所"},
        "lab-history": {"active_view": "增强实验室", "lab_mode": "通知与持久化", "exchange_scope_mode": "全部交易所"},
        "debug": {"active_view": "接口调试"},
        "address": {
            "active_view": "首页总览",
            "overview_mode": "完整",
            "hyperliquid_address_source": "官方示例主账户",
            "hyperliquid_realtime_enabled": True,
        },
    }
    if feature in feature_defaults:
        overrides.update(feature_defaults[feature])
    if active_view:
        overrides["active_view"] = active_view
    if lab_mode:
        overrides["lab_mode"] = lab_mode
    if overview_mode:
        overrides["overview_mode"] = overview_mode
    if desk_mode:
        overrides["desk_mode"] = desk_mode
    if deep_market_mode:
        overrides["deep_market_mode"] = deep_market_mode
    if exchange_scope_mode:
        overrides["exchange_scope_mode"] = exchange_scope_mode
    address_source = str(params.get("address_source") or params.get("address_preset") or "").strip()
    if address_source in HYPERLIQUID_ADDRESS_PRESETS:
        overrides["hyperliquid_address_source"] = address_source
    address = str(params.get("address") or "").strip()
    if address:
        overrides["hyperliquid_address"] = address
        overrides.setdefault("hyperliquid_address_source", "手动输入")
    lookback_raw = params.get("address_lookback") or params.get("hyperliquid_address_lookback_hours")
    if lookback_raw:
        try:
            lookback_hours = int(lookback_raw)
        except (TypeError, ValueError):
            lookback_hours = None
        if lookback_hours in HYPERLIQUID_ADDRESS_LOOKBACK_OPTIONS:
            overrides["hyperliquid_address_lookback_hours"] = lookback_hours
    realtime_enabled = _query_flag(params.get("address_stream") or params.get("hyperliquid_realtime_enabled"))
    if realtime_enabled is not None:
        overrides["hyperliquid_realtime_enabled"] = realtime_enabled
    return overrides


def apply_query_choice_state(
    base_coin: str,
    selected_exchange: str,
    interval: str,
    selected_symbol: str,
    query_overrides: Dict[str, Any],
) -> None:
    if not query_overrides:
        return

    def set_choice(choice_key: str, widget_key: str) -> None:
        value = query_overrides.get(choice_key)
        if value is not None:
            st.session_state[f"{widget_key}::selected"] = value

    set_choice("active_view", chart_key("active-view", base_coin, selected_exchange, interval))
    set_choice("exchange_scope_mode", chart_key("exchange-scope", base_coin))
    set_choice("overview_mode", chart_key("overview-mode", base_coin, selected_exchange, interval))
    set_choice("desk_mode", chart_key("desk-mode", base_coin, selected_exchange, selected_symbol, interval))
    set_choice("deep_market_mode", chart_key("desk-market-mode", base_coin, selected_exchange, selected_symbol, interval))
    set_choice("lab_mode", chart_key("lab-mode", base_coin, selected_exchange, interval))


stored_ui_preferences = load_local_ui_preferences()
ui_preferences = dict(stored_ui_preferences)
try:
    coin_catalog_timeout = max(5, min(20, int(ui_preferences.get("request_timeout") or 10)))
except (TypeError, ValueError):
    coin_catalog_timeout = 10
coin_catalog_payload = load_exchange_coin_catalog_cached(coin_catalog_timeout)
coin_market_availability = coin_catalog_payload.get("availability") or {}
coin_catalog_status = coin_catalog_payload.get("status") or {}
coin_catalog_options = order_coin_options(list(coin_catalog_payload.get("coins") or []))
if not coin_catalog_options:
    coin_catalog_options = order_coin_options(POPULAR_COINS)
coin_catalog_summary = coin_catalog_payload.get("summary") or {}
query_ui_overrides = build_query_ui_overrides(coin_catalog_options)
ui_preferences = {**stored_ui_preferences, **query_ui_overrides}
address_pool_import_errors: List[str] = []
with st.sidebar.form("sidebar-controls", clear_on_submit=False):
    preset_coin = st.selectbox(
        "全所币种搜索",
        coin_catalog_options,
        index=pick_option_index(
            coin_catalog_options,
            ui_preferences.get("catalog_coin") or ui_preferences.get("preset_coin"),
            POPULAR_COINS[0],
        ),
    )
    custom_coin = st.text_input(
        "自定义币种",
        value=str(ui_preferences.get("custom_coin") or ""),
        placeholder="目录里没有就手动输入，比如 PEPE / TAO / FARTCOIN",
    )
    base_coin = custom_coin.strip().upper() if custom_coin.strip() else preset_coin
    st.caption("下拉框现在是全交易所币种目录，展开后可以直接键盘搜索；手动输入会覆盖下拉选择。")
    catalog_summary_text = build_catalog_summary_text(coin_catalog_summary, coin_catalog_status)
    st.caption("目录覆盖: " + catalog_summary_text)
    st.caption(build_coin_availability_caption(base_coin, coin_catalog_payload.get("availability") or {}, coin_catalog_status))

    st.header("终端参数")
    st.caption(f"当前基础币种: {base_coin}")
    overview_coin_options = list(dict.fromkeys([base_coin] + coin_catalog_options))
    default_overview_coins = pick_option_list(
        overview_coin_options,
        ui_preferences.get("overview_coins"),
        fallback=[base_coin],
        limit=12,
    ) or [base_coin]
    overview_coins = st.multiselect("首页币种池", overview_coin_options, default=default_overview_coins, max_selections=12)
    selected_exchange = st.selectbox(
        "主图交易所",
        list(EXCHANGE_ORDER),
        index=pick_option_index(list(EXCHANGE_ORDER), ui_preferences.get("selected_exchange"), "binance"),
        format_func=lambda key: EXCHANGE_TITLES[key],
    )
    interval = st.selectbox(
        "K线周期",
        list(SUPPORTED_INTERVALS),
        index=pick_option_index(list(SUPPORTED_INTERVALS), ui_preferences.get("interval"), "5m"),
    )
    candle_limit = st.slider("K线数量", 120, 480, 240, 30)
    depth_limit = st.slider("盘口深度", 50, 400, 160, 10)
    top_orderbook_scope_mode = st.selectbox(
        "顶部挂单统计",
        TOP_ORDERBOOK_SCOPE_OPTIONS,
        index=pick_option_index(TOP_ORDERBOOK_SCOPE_OPTIONS, ui_preferences.get("top_orderbook_scope_mode"), "当前交易所"),
    )
    top_orderbook_market_mode = st.selectbox(
        "顶部挂单市场",
        TOP_ORDERBOOK_MARKET_OPTIONS,
        index=pick_option_index(TOP_ORDERBOOK_MARKET_OPTIONS, ui_preferences.get("top_orderbook_market_mode"), "合约"),
    )
    heat_window_pct = st.slider("挂单热力窗口 (%)", 2.0, 12.0, 5.0, 0.5)
    heat_buckets = st.slider("挂单热力分桶", 10, 36, 22, 2)
    heat_bars_per_side = st.slider("每侧热力条数", 4, 16, 8, 1)
    risk_heat_window_pct = st.slider("风险热图窗口 (%)", 4.0, 18.0, 8.0, 0.5)
    risk_heat_buckets = st.slider("风险热图分桶", 16, 40, 28, 2)
    liquidation_limit = st.slider("爆仓回看条数", 20, 160, 60, 10)
    liquidation_window_minutes = st.slider("爆仓统计分钟", 15, 240, 60, 15)
    mbo_rows = st.slider("MBO 档位", 8, 24, 14, 2)
    refresh_seconds = st.slider("界面刷新秒数", 1, 10, 4, 1)
    sample_seconds = st.slider("持仓采样秒数", 5, 60, 15, 5)
    request_timeout = st.slider("请求超时秒数", 5, 20, 10, 1)
    performance_mode = st.selectbox(
        "性能模式",
        ["标准", "轻量"],
        index=pick_option_index(["标准", "轻量"], ui_preferences.get("performance_mode"), "标准"),
    )
    st.caption("轻量模式会缩短实时缓冲历史，减少深度页、盘口质量和回放占用。")
    st.caption("测试视角会自动记住到本地，下次打开会恢复最近一次选择。")
    st.markdown("---")

    st.subheader("Hyperliquid 地址模式")
    hyperliquid_address_source = st.selectbox(
        "地址来源",
        list(HYPERLIQUID_ADDRESS_PRESETS),
        index=pick_option_index(list(HYPERLIQUID_ADDRESS_PRESETS), ui_preferences.get("hyperliquid_address_source"), "手动输入"),
    )
    default_preset_address = HYPERLIQUID_ADDRESS_PRESETS.get(hyperliquid_address_source, "")
    hyperliquid_manual_address = st.text_input(
        "地址",
        value=str(ui_preferences.get("hyperliquid_address") or ""),
        placeholder="0x 开头的钱包地址",
        disabled=hyperliquid_address_source != "手动输入",
    )
    hyperliquid_address = hyperliquid_manual_address.strip() if hyperliquid_address_source == "手动输入" else default_preset_address
    hyperliquid_address_lookback_hours = st.selectbox(
        "地址回看窗口",
        HYPERLIQUID_ADDRESS_LOOKBACK_OPTIONS,
        index=pick_option_index(HYPERLIQUID_ADDRESS_LOOKBACK_OPTIONS, ui_preferences.get("hyperliquid_address_lookback_hours"), 24),
        format_func=lambda value: f"{int(value)} 小时" if int(value) < 168 else "7 天",
    )
    hyperliquid_realtime_enabled = st.checkbox(
        "启用地址实时流",
        value=bool(ui_preferences.get("hyperliquid_realtime_enabled", True)),
    )
    if hyperliquid_address.strip() and not is_valid_onchain_address(hyperliquid_address.strip()):
        st.caption("地址格式需要是 `0x` 开头的 42 位十六进制地址。")
    elif hyperliquid_address_source != "手动输入":
        st.caption("这里用的是官方文档里的公开示例地址，方便你没准备地址时直接测试。")
    custom_hyperliquid_pool_entries = normalize_address_pool_entries(ui_preferences.get("custom_hyperliquid_pool_entries"))
    address_pool_import_text = st.text_area(
        "导入自定义地址池",
        value="",
        placeholder="每行一个，格式：标签|地址|分组\n例如：鲸鱼A|0xabc...|鲸鱼\nVault观察|0xdef...|Vault",
        height=96,
    )
    address_option_catalog = build_watch_option_catalog(custom_hyperliquid_pool_entries)
    address_watch_options = [item["option"] for item in address_option_catalog]
    address_watch_groups = list(dict.fromkeys([str(item.get("group") or "默认") for item in address_option_catalog] + ["当前输入"]))
    selected_watch_groups = st.multiselect(
        "地址分组过滤",
        address_watch_groups,
        default=pick_option_list(
            address_watch_groups,
            ui_preferences.get("hyperliquid_watch_groups"),
            fallback=address_watch_groups,
            limit=len(address_watch_groups),
        ),
    )
    hyperliquid_watch_labels = st.multiselect(
        "观察地址池",
        address_watch_options,
        default=pick_option_list(
            address_watch_options,
            ui_preferences.get("hyperliquid_watch_labels"),
            fallback=address_watch_options[: min(3, len(address_watch_options))],
            limit=len(address_watch_options),
        ),
    )
    st.caption(f"当前可选观察地址 {len(address_watch_options)} 个，分组 {len(address_watch_groups)} 个。当前地址如果有效，也会自动加入。")
    st.markdown("---")

    st.subheader("通知 / 历史")
    alert_confirm_after = st.slider(
        "告警确认次数",
        1,
        6,
        int(ui_preferences.get("alert_confirm_after") or 3),
        1,
    )
    alert_cooldown_minutes = st.slider(
        "告警冷却分钟",
        0,
        120,
        int(ui_preferences.get("alert_cooldown_minutes") or 15),
        5,
    )
    browser_min_level = st.selectbox(
        "桌面通知最低等级",
        ALERT_LEVEL_OPTIONS,
        index=pick_option_index(ALERT_LEVEL_OPTIONS, ui_preferences.get("browser_min_level"), "中"),
    )
    telegram_min_level = st.selectbox(
        "Telegram 最低等级",
        ALERT_LEVEL_OPTIONS,
        index=pick_option_index(ALERT_LEVEL_OPTIONS, ui_preferences.get("telegram_min_level"), "强"),
    )
    history_sqlite_enabled = st.checkbox(
        "启用 SQLite 历史库",
        value=bool(ui_preferences.get("history_sqlite_enabled", True)),
    )
    auto_archive_enabled = st.checkbox(
        "启用自动 Parquet 归档",
        value=bool(ui_preferences.get("auto_archive_enabled", True)),
    )
    archive_retention_days = st.selectbox(
        "归档保留天数",
        ARCHIVE_RETENTION_DAYS_OPTIONS,
        index=pick_option_index(ARCHIVE_RETENTION_DAYS_OPTIONS, ui_preferences.get("archive_retention_days"), 7),
        format_func=lambda value: f"{int(value)} 天",
    )
    browser_notify_enabled = st.checkbox(
        "浏览器桌面通知",
        value=bool(ui_preferences.get("browser_notify_enabled", False)),
    )
    browser_sound_enabled = st.checkbox(
        "桌面通知音效",
        value=bool(ui_preferences.get("browser_sound_enabled", False)),
    )
    telegram_push_enabled = st.checkbox(
        "Telegram 推送",
        value=bool(ui_preferences.get("telegram_push_enabled", False)),
    )
    telegram_bot_token = st.text_input(
        "Telegram Bot Token",
        value=str(ui_preferences.get("telegram_bot_token") or ""),
        type="password",
    )
    telegram_chat_id = st.text_input(
        "Telegram Chat ID",
        value=str(ui_preferences.get("telegram_chat_id") or ""),
        placeholder="例如 123456789 或 -100xxxx",
    )
    alert_review_window_minutes = st.selectbox(
        "告警复盘窗口",
        ALERT_REVIEW_WINDOW_OPTIONS,
        index=pick_option_index(ALERT_REVIEW_WINDOW_OPTIONS, ui_preferences.get("alert_review_window_minutes"), 60),
        format_func=lambda value: f"{int(value)} 分钟" if int(value) < 1440 else "1 天",
    )
    st.caption("通知配置保存在本地；Telegram Token / Chat ID 现在会在本地偏好文件里加密落盘。也可以用环境变量 `LGFD_TELEGRAM_BOT_TOKEN` / `LGFD_TELEGRAM_CHAT_ID` 覆盖。")
    st.markdown("---")

    st.subheader("跨币种联动")
    cross_coin_pool_options = list(dict.fromkeys([base_coin] + coin_catalog_options))
    default_cross_coin_pool = pick_option_list(
        cross_coin_pool_options,
        ui_preferences.get("cross_coin_pool"),
        fallback=list(dict.fromkeys([base_coin] + DEFAULT_CROSS_COIN_POOL)),
        limit=6,
    ) or list(dict.fromkeys([base_coin] + DEFAULT_CROSS_COIN_POOL))
    cross_coin_pool = st.multiselect(
        "联动币种池",
        cross_coin_pool_options,
        default=default_cross_coin_pool,
        max_selections=6,
    )
    st.markdown("---")

    st.subheader("合约映射")
    base_defaults = default_symbols(base_coin)
    if st.session_state.get("symbol_base_coin") != base_coin:
        for key in ("bybit", "binance", "okx", "hyperliquid"):
            st.session_state[f"symbol_{key}"] = base_defaults[key]
        st.session_state["symbol_base_coin"] = base_coin
    bybit_symbol = st.text_input("Bybit 合约", key="symbol_bybit")
    binance_symbol = st.text_input("Binance 合约", key="symbol_binance")
    okx_symbol = st.text_input("OKX 合约", key="symbol_okx")
    hyper_symbol = st.text_input("Hyperliquid 币种", key="symbol_hyperliquid")
    st.caption("参数会在点击下方按钮后统一提交，避免每次拖动都整页重算。")
    primary_action_cols = st.columns(3)
    apply_sidebar = primary_action_cols[0].form_submit_button("应用参数", use_container_width=True)
    import_address_pool = primary_action_cols[1].form_submit_button("导入地址池", use_container_width=True)
    clear_custom_address_pool = primary_action_cols[2].form_submit_button("清空地址池", use_container_width=True)
    secondary_action_cols = st.columns(3)
    restore_defaults = secondary_action_cols[0].form_submit_button("恢复默认合约", use_container_width=True)
    clear_cache = secondary_action_cols[1].form_submit_button("清空缓存", use_container_width=True)
    restart_feed = secondary_action_cols[2].form_submit_button("重连实时流", use_container_width=True)

if import_address_pool:
    imported_entries, address_pool_import_errors = parse_hyperliquid_address_pool_text(address_pool_import_text)
    if imported_entries:
        custom_hyperliquid_pool_entries = merge_address_pool_entries(custom_hyperliquid_pool_entries, imported_entries)
if clear_custom_address_pool:
    custom_hyperliquid_pool_entries = []
if address_pool_import_errors:
    st.sidebar.caption("导入时忽略了这些行: " + " | ".join(address_pool_import_errors[:4]))

if not overview_coins:
    overview_coins = [base_coin]
if not cross_coin_pool:
    cross_coin_pool = list(dict.fromkeys([base_coin] + DEFAULT_CROSS_COIN_POOL))
save_local_ui_preferences(
    {
        **stored_ui_preferences,
        "catalog_coin": preset_coin,
        "preset_coin": preset_coin,
        "custom_coin": custom_coin.strip().upper(),
        "overview_coins": overview_coins,
        "selected_exchange": selected_exchange,
        "interval": interval,
        "top_orderbook_scope_mode": top_orderbook_scope_mode,
        "top_orderbook_market_mode": top_orderbook_market_mode,
        "performance_mode": performance_mode,
        "hyperliquid_address_source": hyperliquid_address_source,
        "hyperliquid_address": hyperliquid_manual_address.strip(),
        "hyperliquid_address_lookback_hours": int(hyperliquid_address_lookback_hours),
        "hyperliquid_realtime_enabled": bool(hyperliquid_realtime_enabled),
        "hyperliquid_watch_labels": hyperliquid_watch_labels,
        "hyperliquid_watch_groups": selected_watch_groups,
        "custom_hyperliquid_pool_entries": custom_hyperliquid_pool_entries,
        "alert_confirm_after": int(alert_confirm_after),
        "alert_cooldown_minutes": int(alert_cooldown_minutes),
        "browser_min_level": browser_min_level,
        "telegram_min_level": telegram_min_level,
        "history_sqlite_enabled": bool(history_sqlite_enabled),
        "auto_archive_enabled": bool(auto_archive_enabled),
        "archive_retention_days": int(archive_retention_days),
        "browser_notify_enabled": bool(browser_notify_enabled),
        "browser_sound_enabled": bool(browser_sound_enabled),
        "telegram_push_enabled": bool(telegram_push_enabled),
        "telegram_bot_token": telegram_bot_token.strip(),
        "telegram_chat_id": telegram_chat_id.strip(),
        "alert_review_window_minutes": int(alert_review_window_minutes),
        "cross_coin_pool": cross_coin_pool,
    }
)
stored_ui_preferences = load_local_ui_preferences()
ui_preferences = {**stored_ui_preferences, **query_ui_overrides}

if restore_defaults:
    for key in ("bybit", "binance", "okx", "hyperliquid"):
        st.session_state[f"symbol_{key}"] = base_defaults[key]
    st.rerun()
if clear_cache:
    st.cache_data.clear()
    clear_local_runtime_caches()
    st.rerun()
if import_address_pool or clear_custom_address_pool:
    st.rerun()

symbol_map = {
    "bybit": bybit_symbol.strip().upper(),
    "binance": binance_symbol.strip().upper(),
    "okx": okx_symbol.strip().upper(),
    "hyperliquid": hyper_symbol.strip().upper(),
}
spot_symbol_map = default_spot_symbols(base_coin)
available_perp_keys, available_spot_keys = resolve_coin_market_availability(base_coin, coin_market_availability, coin_catalog_status)
requested_exchange = selected_exchange
selected_exchange, selected_exchange_note = resolve_effective_exchange(requested_exchange, available_perp_keys)
symbol_map, spot_symbol_map = filter_symbol_maps_for_coin(symbol_map, spot_symbol_map, available_perp_keys, available_spot_keys)
spot_symbol = next((spot_symbol_map[key] for key in SPOT_EXCHANGE_ORDER if spot_symbol_map.get(key)), default_spot_symbols(base_coin)["binance"])
performance_settings = service_profile(performance_mode)
service = resolve_service(
    symbol_map,
    request_timeout,
    sample_seconds,
    spot_symbol_map,
    performance_settings,
    restart_feed,
)
service.ensure_orderbook_limit(max(int(performance_settings.get("orderbook_limit", 80)), int(depth_limit)))
history_store = resolve_history_store()

st.markdown(
    f"""
    <div class="hero-shell">
        <div class="hero-kicker">Liquid Glass Flow Desk</div>
        <div class="hero-title">{base_coin} 多交易所流动性终端</div>
        <div class="hero-sub">更通透的玻璃风工作台，把挂单、已发生爆仓、未来 Liquidation / TP / Stop 风险热图和 MBO 风格盘口画像放在一张桌面里。</div>
        <div class="helper-bar">
            <div class="helper-pill">主图 {EXCHANGE_TITLES[selected_exchange]}</div>
            <div class="helper-pill">周期 {interval}</div>
            <div class="helper-pill">刷新 {refresh_seconds}s</div>
            <div class="helper-pill">爆仓窗口 {liquidation_window_minutes}m</div>
            <div class="helper-pill">风险热图 {risk_heat_window_pct:.1f}%</div>
        </div>
    </div>
    """,
    unsafe_allow_html=True,
)
if selected_exchange_note:
    st.caption(selected_exchange_note)


@st.fragment(run_every=refresh_seconds)
def render_terminal() -> None:
    snapshots = service.current_snapshots()
    ok_snapshots = [snapshot for snapshot in snapshots if snapshot.status == "ok"]
    headline_placeholder = st.empty()
    status_text = " · ".join(status_caption(snapshot) for snapshot in snapshots)
    st.markdown(f"<div class='status-strip'>{status_text}</div>", unsafe_allow_html=True)
    if not ok_snapshots:
        st.error("当前没有交易所返回可用数据，请检查合约名或网络。")
        return

    snapshot_by_key = dict(zip(EXCHANGE_ORDER, snapshots))
    selected_snapshot = snapshot_by_key[selected_exchange]
    selected_symbol = symbol_map[selected_exchange]
    binance_perp_snapshot = snapshot_by_key["binance"]
    apply_query_choice_state(base_coin, selected_exchange, interval, selected_symbol, query_ui_overrides)
    view_options = ["首页总览", "交易台深度页", "告警中心", "爆仓中心", "盘口中心", "增强实验室", "接口调试"]
    active_view = render_choice_bar(
        "工作台视图",
        view_options,
        chart_key("active-view", base_coin, selected_exchange, interval),
        default=view_options[0],
    )
    exchange_scope_mode = render_choice_bar(
        "交易所范围",
        ["当前交易所优先", "全部交易所"],
        chart_key("exchange-scope", base_coin),
        default=pick_option(["当前交易所优先", "全部交易所"], ui_preferences.get("exchange_scope_mode"), "当前交易所优先"),
    )
    st.caption("当前交易所优先 = 只重点加载当前合约所和当前现货焦点，速度更快；全部交易所 = 深度页和中心页都会保留跨所横向比较、联动和测试视角。")
    spot_focus_exchange, scope_spot_keys, scope_perp_keys, scope_perp_reference_keys = resolve_exchange_scope(
        selected_exchange,
        exchange_scope_mode,
        available_spot_keys=available_spot_keys,
        available_perp_keys=available_perp_keys,
    )
    load_spot_keys = scope_spot_keys
    load_perp_reference_keys = scope_perp_reference_keys
    scope_snapshots = [snapshot_by_key[exchange_key] for exchange_key in scope_perp_keys if exchange_key in snapshot_by_key]
    scope_ok_snapshots = [snapshot for snapshot in scope_snapshots if snapshot.status == "ok"]
    overview_mode = None
    liquidation_archive_window = pick_option(
        LIQUIDATION_ARCHIVE_WINDOW_OPTIONS,
        ui_preferences.get("liquidation_archive_window"),
        "最近 4 小时",
    )
    if active_view == "首页总览":
        overview_mode = render_choice_bar(
            "首页模式",
            ["轻量", "完整"],
            chart_key("overview-mode", base_coin, selected_exchange, interval),
            default=pick_option(["轻量", "完整"], ui_preferences.get("overview_mode"), "轻量"),
        )
        st.caption("首页轻量模式只保留总览板、异动榜和全市场对比；完整模式才会继续加载现货/合约、告警、期限结构这些更重的模块。")
    desk_mode = "核心"
    deep_market_mode = "综合深度"
    if active_view == "交易台深度页":
        desk_mode = render_choice_bar(
            "深度页模式",
            ["核心", "完整"],
            chart_key("desk-mode", base_coin, selected_exchange, selected_symbol, interval),
            default=pick_option(["核心", "完整"], ui_preferences.get("desk_mode"), "核心"),
        )
        st.caption("核心模式先加载主图、OI、CVD、合成信号和爆仓真值；完整模式再继续加载止盈/止损、MBO、盘口质量和回放。")
        deep_market_mode = render_choice_bar(
            "深度市场视角",
            ["综合深度", "现货深度", "合约深度"],
            chart_key("desk-market-mode", base_coin, selected_exchange, selected_symbol, interval),
            default=pick_option(["综合深度", "现货深度", "合约深度"], ui_preferences.get("deep_market_mode"), "综合深度"),
        )
        st.caption("综合深度先看现货/合约联动，再往下看合约主图；现货深度只保留现货盘口和主动买卖；合约深度则聚焦 OI、爆仓和风险区。")
        st.caption("交易所范围会直接作用到深度页：`当前交易所优先` 更快；`全部交易所` 会补齐现货/合约横向对照，但主图、风险图和回放仍以当前选中的合约所为主。")
    if active_view == "爆仓中心":
        liquidation_archive_window = render_choice_bar(
            "爆仓归档窗口",
            LIQUIDATION_ARCHIVE_WINDOW_OPTIONS,
            chart_key("liq-archive-window", base_coin, selected_exchange, interval),
            default=pick_option(LIQUIDATION_ARCHIVE_WINDOW_OPTIONS, ui_preferences.get("liquidation_archive_window"), "最近 4 小时"),
        )
        st.caption("这里会把 REST 回补、会话内实时流和本地归档一起合并；你可以切到 30 分钟、4 小时、今天或全部本地缓存。")
    lab_mode = "总览"
    if active_view == "增强实验室":
        lab_mode = render_choice_bar(
            "增强实验室视图",
            LAB_VIEW_OPTIONS,
            chart_key("lab-mode", base_coin, selected_exchange, interval),
            default=pick_option(LAB_VIEW_OPTIONS, ui_preferences.get("lab_mode"), LAB_VIEW_OPTIONS[0]),
        )
        st.caption("这里把 Hyperliquid 独有 API、多交易所聚合、策略层信号、通知与持久化集中到一个独立试验台里，不改你原来的主工作流。")
    refresh_profile = build_refresh_profile(
        refresh_seconds,
        active_view=active_view,
        overview_mode=overview_mode,
        lab_mode=lab_mode,
    )
    fast_ttl = int(refresh_profile["fast_ttl"])
    balanced_ttl = int(refresh_profile["balanced_ttl"])
    heavy_ttl = int(refresh_profile["heavy_ttl"])
    stale_ttl = int(refresh_profile["stale_ttl"])
    prefetch_age = int(refresh_profile["prefetch_age"])
    save_local_ui_preferences(
        {
            "catalog_coin": preset_coin,
            "preset_coin": preset_coin,
            "custom_coin": custom_coin.strip().upper(),
            "overview_coins": overview_coins,
            "selected_exchange": requested_exchange,
            "interval": interval,
            "top_orderbook_scope_mode": top_orderbook_scope_mode,
            "top_orderbook_market_mode": top_orderbook_market_mode,
            "performance_mode": performance_mode,
            "hyperliquid_address_source": hyperliquid_address_source,
            "hyperliquid_address": hyperliquid_manual_address.strip(),
            "hyperliquid_address_lookback_hours": int(hyperliquid_address_lookback_hours),
            "hyperliquid_realtime_enabled": bool(hyperliquid_realtime_enabled),
            "hyperliquid_watch_labels": hyperliquid_watch_labels,
            "custom_hyperliquid_pool_entries": custom_hyperliquid_pool_entries,
            "alert_confirm_after": int(alert_confirm_after),
            "alert_cooldown_minutes": int(alert_cooldown_minutes),
            "browser_min_level": browser_min_level,
            "telegram_min_level": telegram_min_level,
            "active_view": active_view,
            "lab_mode": lab_mode,
            "exchange_scope_mode": exchange_scope_mode,
            "liquidation_archive_window": liquidation_archive_window,
            "overview_mode": overview_mode or pick_option(["轻量", "完整"], ui_preferences.get("overview_mode"), "轻量"),
            "desk_mode": desk_mode if active_view == "交易台深度页" else pick_option(["核心", "完整"], ui_preferences.get("desk_mode"), "核心"),
            "deep_market_mode": (
                deep_market_mode
                if active_view == "交易台深度页"
                else pick_option(["综合深度", "现货深度", "合约深度"], ui_preferences.get("deep_market_mode"), "综合深度")
            ),
            "history_sqlite_enabled": bool(history_sqlite_enabled),
            "auto_archive_enabled": bool(auto_archive_enabled),
            "archive_retention_days": int(archive_retention_days),
            "browser_notify_enabled": bool(browser_notify_enabled),
            "browser_sound_enabled": bool(browser_sound_enabled),
            "telegram_push_enabled": bool(telegram_push_enabled),
            "telegram_bot_token": telegram_bot_token.strip(),
            "telegram_chat_id": telegram_chat_id.strip(),
            "alert_review_window_minutes": int(alert_review_window_minutes),
            "cross_coin_pool": cross_coin_pool,
        }
    )

    market_reference_price = pd.Series([snapshot.mark_price for snapshot in ok_snapshots if snapshot.mark_price is not None]).median()
    total_oi_notional = sum(snapshot.open_interest_notional or 0.0 for snapshot in ok_snapshots)
    funding_series = pd.Series([snapshot.funding_bps for snapshot in ok_snapshots if snapshot.funding_bps is not None])
    average_holding_cost = funding_series.mean() if not funding_series.empty else None
    top_signature_parts: List[Tuple[str, str, int, int]] = []
    perp_top_keys = [selected_exchange] if top_orderbook_scope_mode == "当前交易所" else list(available_perp_keys)
    if top_orderbook_market_mode in ("合约", "合并"):
        for exchange_key in perp_top_keys:
            snapshot = snapshot_by_key.get(exchange_key)
            top_signature_parts.append(
                (
                    "perp",
                    exchange_key,
                    int(snapshot.timestamp_ms or 0) if snapshot is not None else 0,
                    len(service.get_orderbook(exchange_key)),
                )
            )
    if top_orderbook_market_mode in ("现货", "合并"):
        spot_top_keys = (
            [selected_exchange]
            if top_orderbook_scope_mode == "当前交易所" and selected_exchange in available_spot_keys
            else list(available_spot_keys)
            if top_orderbook_scope_mode == "四所聚合"
            else []
        )
        for exchange_key in spot_top_keys:
            live_spot_snapshot = service.get_spot_snapshot(exchange_key)
            top_signature_parts.append(
                (
                    "spot",
                    exchange_key,
                    int(live_spot_snapshot.timestamp_ms or 0) if live_spot_snapshot is not None else 0,
                    len(service.get_spot_orderbook(exchange_key)),
                )
            )
    top_orderbook_payload = get_cached_derived_result(
        f"top-orderbook::{base_coin}::{selected_exchange}",
        (
            top_orderbook_scope_mode,
            top_orderbook_market_mode,
            int(depth_limit),
            int(request_timeout),
            tuple(top_signature_parts),
        ),
        ttl_seconds=max(2, refresh_seconds),
        builder=lambda: build_top_orderbook_summary(
            service,
            snapshot_by_key,
            symbol_map,
            spot_symbol_map,
            selected_exchange=selected_exchange,
            depth_limit=depth_limit,
            request_timeout=request_timeout,
            scope_mode=top_orderbook_scope_mode,
            market_mode=top_orderbook_market_mode,
        ),
    )
    quick_book_summary = dict(top_orderbook_payload.get("summary") or {})
    top_orderbook_scope_label = str(top_orderbook_payload.get("scope_label") or "")
    top_orderbook_market_label = str(top_orderbook_payload.get("market_label") or top_orderbook_market_mode)
    top_orderbook_note = str(top_orderbook_payload.get("note") or "")
    actual_bid_levels = int(top_orderbook_payload.get("actual_bid_levels") or 0)
    actual_ask_levels = int(top_orderbook_payload.get("actual_ask_levels") or 0)
    requested_bid_levels = int(top_orderbook_payload.get("requested_bid_levels") or 0)
    requested_ask_levels = int(top_orderbook_payload.get("requested_ask_levels") or 0)
    row_top = st.columns(5)
    row_top[0].metric("在线交易所", str(len(ok_snapshots)))
    row_top[1].metric("市场参考价", fmt_price(market_reference_price))
    row_top[2].metric("持仓总金额", fmt_compact(total_oi_notional))
    row_top[3].metric("主图资金费率", fmt_rate(selected_snapshot.funding_rate))
    row_top[4].metric("四所平均费率", fmt_bps(average_holding_cost))
    row_bottom = st.columns(6)
    row_bottom[0].metric("买盘挂单金额", fmt_compact(quick_book_summary.get("bid_notional")))
    row_bottom[1].metric("卖盘挂单金额", fmt_compact(quick_book_summary.get("ask_notional")))
    row_bottom[2].metric("买盘挂单数量", fmt_compact(quick_book_summary.get("bid_size")))
    row_bottom[3].metric("卖盘挂单数量", fmt_compact(quick_book_summary.get("ask_size")))
    row_bottom[4].metric("买盘档数", f"{actual_bid_levels} / {requested_bid_levels}" if requested_bid_levels > 0 else "-")
    row_bottom[5].metric("卖盘档数", f"{actual_ask_levels} / {requested_ask_levels}" if requested_ask_levels > 0 else "-")
    st.caption(
        f"顶部挂单卡片口径: `{top_orderbook_scope_label}` | `{top_orderbook_market_label}` | 每侧目标 `{int(depth_limit)}` 档。"
        " 挂单金额 = 当前可见盘口深度内的价格 x 数量汇总；挂单数量 = 币数量或合约张数汇总，不是价格。"
    )
    if top_orderbook_note:
        st.caption(top_orderbook_note)
    cards = st.columns(len(EXCHANGE_ORDER))
    for col, exchange_key in zip(cards, EXCHANGE_ORDER):
        snapshot = snapshot_by_key[exchange_key]
        col.metric(snapshot.exchange, fmt_price(snapshot.last_price), delta=f"OI {fmt_compact(snapshot.open_interest_notional)}")
    if history_sqlite_enabled:
        schedule_history_write(
            f"snapshots::{base_coin}::perp",
            lambda store=history_store, coin=base_coin, snapshots=dict(snapshot_by_key): store.record_snapshots(coin, snapshots),
        )
    archive_events = maybe_run_auto_archive(
        history_store,
        enabled=bool(history_sqlite_enabled and auto_archive_enabled),
        retention_days=int(archive_retention_days),
    )

    if active_view == "首页总览" and overview_mode == "轻量":
        overview_signature = (tuple(dict.fromkeys(overview_coins)), int(request_timeout), int(liquidation_limit))
        overview_frame = get_stale_while_revalidate_result(
            f"home-overview::{base_coin}",
            overview_signature,
            ttl_seconds=45,
            stale_ttl_seconds=300,
            builder=lambda: load_market_overview_frame_cached(tuple(dict.fromkeys(overview_coins)), request_timeout, liquidation_limit),
        )
        if overview_frame is None:
            overview_frame = pd.DataFrame()
        selected_overview_frame = overview_frame[overview_frame["币种"] == base_coin] if not overview_frame.empty and "币种" in overview_frame.columns else pd.DataFrame()
        selected_overview_row = selected_overview_frame.iloc[0].to_dict() if not selected_overview_frame.empty else {}
        light_live_trade_events = service.get_trade_history(selected_exchange)
        light_trade_events = (
            light_live_trade_events
            if _use_live_trades(light_live_trade_events)
            else load_exchange_trades_cached(selected_exchange, selected_symbol, 120, request_timeout)
        )
        light_session_liquidations = service.get_liquidation_history(selected_exchange)
        light_liquidation_events = merge_liquidation_events(
            load_liquidations_cached(selected_exchange, selected_symbol, liquidation_limit, request_timeout),
            light_session_liquidations,
        )
        light_selected_name = EXCHANGE_TITLES.get(selected_exchange, selected_exchange.title())
        light_trade_metrics_by_exchange = {
            light_selected_name: build_trade_metrics(
                light_trade_events,
                selected_snapshot.timestamp_ms or int(time.time() * 1000),
                liquidation_window_minutes,
            )
        }
        light_liquidation_metrics_by_exchange = {
            light_selected_name: build_liquidation_metrics(
                light_liquidation_events,
                selected_snapshot.timestamp_ms or int(time.time() * 1000),
                liquidation_window_minutes,
            )
        }
        light_oi_metrics_by_exchange = {
            light_selected_name: {
                "oi_change_pct": payload_float(selected_overview_row.get("OI 1h(%)")),
            }
        }
        light_large_trade_frame = build_large_trade_frame(
            {light_selected_name: light_trade_events},
            min_notional=max(75_000.0, float(selected_snapshot.last_price or 0.0) * 1.5),
            limit=6,
        )
        render_home_headline_marquee(
            headline_placeholder,
            build_home_headline_items(
                base_coin,
                snapshot_by_key,
                light_oi_metrics_by_exchange,
                light_liquidation_metrics_by_exchange,
                light_trade_metrics_by_exchange,
                exchange_keys=[selected_exchange],
                liquidation_window_minutes=liquidation_window_minutes,
                large_trade_frame=light_large_trade_frame,
                confirmed_alert_frame=pd.DataFrame(),
                kind_targets={
                    "overview": "section-home-exchange-compare",
                    "oi": "section-home-oi",
                    "liquidation": "section-home-liquidation",
                    "trade": "section-home-movers",
                    "large": "section-home-large-perp",
                    "alert": "section-home-movers",
                },
                limit=8,
            ),
        )
        render_section(
            "竞品级首页",
            "轻量模式先看主结论、全市场总览板和异动榜，需要更细的盘口和联动再切去深度页或中心页。",
            anchor_id="section-home-hero",
        )
        conclusion_cards = st.columns(4)
        conclusion_cards[0].metric("当前主结论", str(selected_overview_row.get("主结论") or "信号混合"))
        conclusion_cards[1].metric("短线驱动", str(selected_overview_row.get("Lead/Lag") or "样本不足"))
        conclusion_cards[2].metric("OI 1h", fmt_pct(selected_overview_row.get("OI 1h(%)")))
        conclusion_cards[3].metric("24h爆仓样本", fmt_compact(selected_overview_row.get("24h爆仓样本额")))
        home_lines = [
            f"{base_coin}: 当前更像 `{selected_overview_row.get('主结论') or '信号混合'}`。",
            f"短线驱动: `{selected_overview_row.get('Lead/Lag') or '样本不足'}`。",
            f"Funding {fmt_bps(selected_overview_row.get('Funding(bps)'))}，多空比 {selected_overview_row.get('多空比') if selected_overview_row.get('多空比') is not None else '-'}。",
        ]
        for line in home_lines:
            st.markdown(f"- {line}")

        render_section(
            "全市场总览板",
            "按币种把价格、OI、OI 1h/24h、Funding、爆仓样本、多空比、现货/合约成交比和 Lead/Lag 放在一张榜单里。",
            anchor_id="section-home-overview",
            anchor_aliases=["section-home-oi"],
        )
        if overview_frame.empty:
            st.info("当前首页币种池还没有可展示的数据。")
        else:
            st.dataframe(
                overview_frame,
                width="stretch",
                hide_index=True,
                column_config={
                    "价格": st.column_config.NumberColumn(format="%.2f"),
                    "OI": st.column_config.NumberColumn(format="%.2f"),
                    "OI 1h(%)": st.column_config.NumberColumn(format="%.2f"),
                    "OI 24h(%)": st.column_config.NumberColumn(format="%.2f"),
                    "Funding(bps)": st.column_config.NumberColumn(format="%.2f"),
                    "24h爆仓样本额": st.column_config.NumberColumn(format="%.2f"),
                    "多空比": st.column_config.NumberColumn(format="%.3f"),
                    "现货/合约成交比": st.column_config.NumberColumn(format="%.2f"),
                    "置信度": st.column_config.ProgressColumn(format="%.1f", min_value=0.0, max_value=100.0),
                },
            )

        render_section(
            "异动榜",
            "把 OI 激增、爆仓放大、Funding 极值、合约情绪极值、现货先动、拥挤但衰竭 这六类异动拆开看。",
            anchor_id="section-home-movers",
            anchor_aliases=["section-home-liquidation", "section-home-large-perp"],
        )
        oi_movers = build_movers_frame(overview_frame, "OI 1h(%)", limit=5, ascending=False)
        liq_movers = build_movers_frame(overview_frame, "24h爆仓样本额", limit=5, ascending=False)
        funding_movers = (
            overview_frame.assign(_abs_funding=overview_frame["Funding(bps)"].abs())
            .sort_values("_abs_funding", ascending=False)
            .drop(columns="_abs_funding")
            .head(5)
            .reset_index(drop=True)
            if not overview_frame.empty and "Funding(bps)" in overview_frame.columns
            else pd.DataFrame()
        )
        spot_leaders = (
            overview_frame[overview_frame["Lead/Lag"].astype(str).str.contains("现货领先", na=False)].head(5).reset_index(drop=True)
            if not overview_frame.empty
            else pd.DataFrame()
        )
        crowd_movers = (
            overview_frame.assign(_ratio_distance=(pd.to_numeric(overview_frame["多空比"], errors="coerce") - 1.0).abs())
            .sort_values("_ratio_distance", ascending=False)
            .drop(columns="_ratio_distance")
            .head(5)
            .reset_index(drop=True)
            if not overview_frame.empty and "多空比" in overview_frame.columns
            else pd.DataFrame()
        )
        exhausted_movers = build_movers_frame(overview_frame, "置信度", limit=5, ascending=False, title_filter="拥挤但衰竭")
        mover_specs = [
            ("OI 激增榜", oi_movers, "OI 1h(%)"),
            ("爆仓榜", liq_movers, "24h爆仓样本额"),
            ("Funding 极值榜", funding_movers, "Funding(bps)"),
            ("合约情绪极值榜", crowd_movers, "多空比"),
            ("现货带动榜", spot_leaders, "Lead/Lag"),
            ("拥挤但衰竭榜", exhausted_movers, "置信度"),
        ]
        mover_columns = st.columns(len(mover_specs), gap="medium")
        for column, (title, frame, focus_column) in zip(mover_columns, mover_specs):
            with column:
                st.caption(title)
                if frame.empty:
                    st.info("暂无")
                else:
                    st.dataframe(frame[["币种", "主结论", focus_column]], width="stretch", hide_index=True)

        render_section(
            "全市场对比" if exchange_scope_mode == "全部交易所" else "当前交易所对比",
            "横向比较价格、持仓、费率和成交额。",
            anchor_id="section-home-exchange-compare",
        )
        st.dataframe(build_snapshot_frame(scope_snapshots if exchange_scope_mode == "当前交易所优先" else snapshots), width="stretch", hide_index=True)
        render_section("持仓总量 / 未平仓对比", "公开 API 下，这里的未平仓就是合约 OI；现货市场不存在 OI 概念。")
        oi_left, oi_right = st.columns([1.6, 1.35], gap="large")
        with oi_left:
            st.plotly_chart(
                build_open_interest_comparison_figure(scope_ok_snapshots if exchange_scope_mode == "当前交易所优先" else ok_snapshots),
                key=chart_key("oi-compare-lite", base_coin, interval),
                config=PLOTLY_CONFIG,
            )
        with oi_right:
            oi_frame = build_open_interest_frame(scope_ok_snapshots if exchange_scope_mode == "当前交易所优先" else ok_snapshots)
            if oi_frame.empty:
                st.info("当前没有可展示的未平仓分布。")
            else:
                st.dataframe(
                    oi_frame,
                    width="stretch",
                    hide_index=True,
                    column_config={
                        "持仓量": st.column_config.NumberColumn(format="%.4f"),
                        "持仓金额": st.column_config.NumberColumn(format="%.2f"),
                        "资金费率(bps)": st.column_config.NumberColumn(format="%.2f"),
                        "份额(%)": st.column_config.ProgressColumn(format="%.2f", min_value=0.0, max_value=100.0),
                    },
                )
        st.caption("更重的 Spot-Perp、告警、期限结构、盘口质量和爆仓联动已经留在其他页面；如果你要把它们放回首页，可以把首页模式切到 `完整`。")
        return

    if active_view == "爆仓中心":
        now_ms = selected_snapshot.timestamp_ms or int(time.time() * 1000)
        archive_since_ms, archive_window_label = resolve_liquidation_archive_window(liquidation_archive_window, now_ms)
        archive_limit = max(800, liquidation_limit * 40)
        session_liquidations = service.get_liquidation_history(selected_exchange)
        rest_liquidations = load_liquidations_cached(selected_exchange, selected_symbol, liquidation_limit, request_timeout)
        persisted_liquidations = service.get_persisted_liquidations(
            selected_exchange,
            selected_symbol,
            since_ms=archive_since_ms,
            limit=archive_limit,
        )
        persisted_summary = service.get_persisted_liquidation_summary(
            selected_exchange,
            selected_symbol,
            since_ms=archive_since_ms,
            limit=archive_limit,
        )
        liquidation_events = merge_liquidation_event_groups(persisted_liquidations, rest_liquidations, session_liquidations)
        if archive_since_ms is not None:
            liquidation_events = [event for event in liquidation_events if event.timestamp_ms >= archive_since_ms]
        selected_liq_signature = (
            selected_exchange,
            selected_symbol,
            len(liquidation_events),
            liquidation_events[-1].timestamp_ms if liquidation_events else 0,
            liquidation_window_minutes,
            persisted_summary.get("count"),
            persisted_summary.get("last_timestamp_ms"),
            archive_window_label,
        )
        selected_liq_bundle = get_cached_derived_result(
            f"selected-liq::{selected_exchange}::{selected_symbol}::{liquidation_window_minutes}::{archive_window_label}",
            selected_liq_signature,
            ttl_seconds=max(4, refresh_seconds),
            builder=lambda: {
                "truth": build_liquidation_truth_summary(
                    liquidation_events,
                    now_ms=now_ms,
                    window_minutes=liquidation_window_minutes,
                    cluster_window_seconds=30,
                ),
                "clusters": build_liquidation_cluster_frame(liquidation_events, cluster_window_seconds=30, limit=14),
            },
        )
        selected_liquidation_truth = selected_liq_bundle["truth"]
        cross_liq_scope_keys = scope_perp_keys if exchange_scope_mode == "当前交易所优先" else list(EXCHANGE_ORDER)
        cross_exchange_liquidations: List[LiquidationEvent] = []
        for exchange_key in cross_liq_scope_keys:
            exchange_symbol = symbol_map[exchange_key]
            exchange_events = merge_liquidation_event_groups(
                service.get_persisted_liquidations(
                    exchange_key,
                    exchange_symbol,
                    since_ms=archive_since_ms,
                    limit=archive_limit,
                ),
                load_liquidations_cached(exchange_key, exchange_symbol, liquidation_limit, request_timeout),
                service.get_liquidation_history(exchange_key),
            )
            if archive_since_ms is not None:
                exchange_events = [event for event in exchange_events if event.timestamp_ms >= archive_since_ms]
            cross_exchange_liquidations.extend(exchange_events)
        cross_exchange_liquidations = sorted(cross_exchange_liquidations, key=lambda item: item.timestamp_ms)
        cross_exchange_cluster_frame = build_liquidation_cluster_frame(cross_exchange_liquidations, cluster_window_seconds=30, limit=16)
        cross_exchange_linkage_frame = build_cross_exchange_liquidation_frame(cross_exchange_liquidations, cluster_window_seconds=30, limit=12)
        cross_exchange_totals_frame = build_liquidation_exchange_totals_frame(cross_exchange_liquidations)
        render_section("爆仓中心 2.0", "把 REST 回补、会话内实时爆仓流和本地归档合并后，再看单所簇、跨所簇、瀑布和传导。")
        st.caption(
            f"当前归档窗口: `{archive_window_label}` | 当前交易所本地归档 {int(persisted_summary.get('count') or 0)} 条"
            f" | 归档目录 `{persisted_summary.get('path') or '-'}`"
        )
        if exchange_scope_mode == "当前交易所优先":
            st.info("当前是 `当前交易所优先` 模式，爆仓中心的跨所联动样本已收敛到当前交易所。要看完整跨所传导，请切到 `全部交易所`。")
        liq_center_row = st.columns(6)
        liq_center_row[0].metric("多头爆仓额", fmt_compact(selected_liquidation_truth.get("long_notional")))
        liq_center_row[1].metric("空头爆仓额", fmt_compact(selected_liquidation_truth.get("short_notional")))
        liq_center_row[2].metric("单所爆仓簇", str(int(selected_liquidation_truth.get("single_cluster_count") or 0)))
        liq_center_row[3].metric("跨所联动簇", str(int(selected_liquidation_truth.get("cross_cluster_count") or 0)))
        liq_center_row[4].metric("本地归档样本", str(int(persisted_summary.get("count") or 0)))
        liq_center_row[5].metric("跨所爆仓样本额", fmt_compact(selected_liquidation_truth.get("cross_cluster_notional")))
        liq_center_top_left, liq_center_top_right = st.columns([1.65, 1.35], gap="large")
        with liq_center_top_left:
            st.plotly_chart(
                build_liquidation_figure(liquidation_events),
                key=chart_key("liq-center-truth", base_coin, selected_exchange, selected_symbol, interval),
                config=PLOTLY_CONFIG,
            )
        with liq_center_top_right:
            st.plotly_chart(
                build_liquidation_cluster_figure(liquidation_events, cluster_window_seconds=30, limit=14),
                key=chart_key("liq-center-cluster", base_coin, selected_exchange, selected_symbol, interval),
                config=PLOTLY_CONFIG,
            )
        liq_center_bottom_left, liq_center_bottom_right = st.columns([1.6, 1.4], gap="large")
        with liq_center_bottom_left:
            st.plotly_chart(
                build_liquidation_waterfall_figure(cross_exchange_liquidations, now_ms, 120, 5),
                key=chart_key("liq-center-waterfall", base_coin, interval),
                config=PLOTLY_CONFIG,
            )
        with liq_center_bottom_right:
            st.plotly_chart(
                build_liquidation_linkage_heatmap(cross_exchange_liquidations, now_ms, 120, 5),
                key=chart_key("liq-center-linkage", base_coin, interval),
                config=PLOTLY_CONFIG,
            )
        long_liq_frame = build_liquidation_frame([event for event in liquidation_events if event.side == "long"], limit=10)
        short_liq_frame = build_liquidation_frame([event for event in liquidation_events if event.side == "short"], limit=10)
        liq_table_left, liq_table_right = st.columns(2, gap="large")
        with liq_table_left:
            st.caption("多头爆仓真值 · Top 10")
            st.dataframe(long_liq_frame, width="stretch", hide_index=True)
        with liq_table_right:
            st.caption("空头爆仓真值 · Top 10")
            st.dataframe(short_liq_frame, width="stretch", hide_index=True)
        liq_archive_left, liq_archive_right = st.columns([1.15, 1.35], gap="large")
        with liq_archive_left:
            if cross_exchange_linkage_frame.empty:
                st.info("当前窗口里还没有形成跨所联动簇。")
            else:
                st.dataframe(
                    cross_exchange_linkage_frame,
                    width="stretch",
                    hide_index=True,
                    column_config={
                        "交易所数": st.column_config.NumberColumn(format="%d"),
                        "事件数": st.column_config.NumberColumn(format="%d"),
                        "总名义金额": st.column_config.NumberColumn(format="%.2f"),
                    },
                )
        with liq_archive_right:
            if cross_exchange_totals_frame.empty:
                st.info("当前窗口里还没有归档可用的交易所汇总。")
            else:
                st.dataframe(
                    cross_exchange_totals_frame,
                    width="stretch",
                    hide_index=True,
                    column_config={
                        "事件数": st.column_config.NumberColumn(format="%d"),
                        "总名义金额": st.column_config.NumberColumn(format="%.2f"),
                        "多头爆仓额": st.column_config.NumberColumn(format="%.2f"),
                        "空头爆仓额": st.column_config.NumberColumn(format="%.2f"),
                    },
                )
        st.dataframe(
            cross_exchange_cluster_frame,
            width="stretch",
            hide_index=True,
            column_config={
                "持续秒数": st.column_config.NumberColumn(format="%.1f"),
                "多头爆仓额": st.column_config.NumberColumn(format="%.2f"),
                "空头爆仓额": st.column_config.NumberColumn(format="%.2f"),
                "总名义金额": st.column_config.NumberColumn(format="%.2f"),
            },
        )
        return

    if active_view == "盘口中心":
        live_orderbook = service.get_orderbook(selected_exchange)
        orderbook = (
            live_orderbook
            if _use_live_orderbook(live_orderbook, selected_snapshot.timestamp_ms)
            else load_orderbook_cached(selected_exchange, selected_symbol, depth_limit, request_timeout)
        )
        reference_price = selected_snapshot.last_price or selected_snapshot.mark_price or (orderbook[0].price if orderbook else None)
        mbo_frame = build_mbo_profile_frame(orderbook, reference_price, mbo_rows)
        perp_quality_history = service.get_orderbook_quality_history(selected_exchange)
        quality_summary_frame = build_orderbook_quality_frame(perp_quality_history, limit=24)
        spot_quality_history = service.get_spot_orderbook_quality_history(selected_exchange) if spot_symbol_map.get(selected_exchange) else []
        render_section("盘口中心", "把撤单速度、假挂单、补单、墙体寿命和近价流动性塌陷风险单独拎出来看。")
        latest_quality = quality_summary_frame.iloc[0].to_dict() if not quality_summary_frame.empty else {}
        near_added = float(latest_quality.get("近价新增") or 0.0)
        near_canceled = float(latest_quality.get("近价撤单") or 0.0)
        collapse_ratio = near_canceled / max(near_added, 1.0)
        if collapse_ratio >= 1.45:
            collapse_label = "高"
        elif collapse_ratio >= 1.1:
            collapse_label = "中"
        else:
            collapse_label = "低"
        book_center_row = st.columns(5)
        book_center_row[0].metric("新增挂单额", fmt_compact(latest_quality.get("新增挂单额")))
        book_center_row[1].metric("撤单额", fmt_compact(latest_quality.get("撤单额")))
        book_center_row[2].metric("假挂单次数", str(int(latest_quality.get("假挂单次数") or 0)))
        book_center_row[3].metric("补单次数", str(int(latest_quality.get("补单次数") or 0)))
        book_center_row[4].metric("近价流动性塌陷", collapse_label, delta=f"{collapse_ratio:.2f}x")
        book_center_left, book_center_right = st.columns([1.7, 1.3], gap="large")
        with book_center_left:
            st.plotly_chart(
                build_orderbook_quality_figure(perp_quality_history, title=f"{selected_snapshot.exchange} Perp Liquidity Quality"),
                key=chart_key("orderbook-center-perp", base_coin, selected_exchange, selected_symbol, interval),
                config=PLOTLY_CONFIG,
            )
        with book_center_right:
            st.plotly_chart(
                build_mbo_figure(mbo_frame, reference_price),
                key=chart_key("orderbook-center-mbo", base_coin, selected_exchange, selected_symbol, interval),
                config=PLOTLY_CONFIG,
            )
        if spot_symbol_map.get(selected_exchange):
            st.plotly_chart(
                build_orderbook_quality_figure(spot_quality_history, title=f"{selected_snapshot.exchange} Spot Liquidity Quality"),
                key=chart_key("orderbook-center-spot", base_coin, selected_exchange, selected_symbol, interval),
                config=PLOTLY_CONFIG,
            )
        st.dataframe(
            quality_summary_frame,
            width="stretch",
            hide_index=True,
            column_config={
                "新增挂单额": st.column_config.NumberColumn(format="%.2f"),
                "撤单额": st.column_config.NumberColumn(format="%.2f"),
                "净变化": st.column_config.NumberColumn(format="%.2f"),
                "近价新增": st.column_config.NumberColumn(format="%.2f"),
                "近价撤单": st.column_config.NumberColumn(format="%.2f"),
                "买墙持续(s)": st.column_config.NumberColumn(format="%.1f"),
                "卖墙持续(s)": st.column_config.NumberColumn(format="%.1f"),
                "盘口失衡(%)": st.column_config.NumberColumn(format="%.2f"),
            },
        )
        return

    if active_view == "告警中心":
        now_ms = selected_snapshot.timestamp_ms or int(time.time() * 1000)
        crowd_payload = load_binance_crowd_cached(symbol_map["binance"], crowd_period_for_interval(interval), request_timeout)
        alert_exchange_keys = scope_spot_keys
        if exchange_scope_mode == "全部交易所":
            spot_snapshot_map, spot_orderbook_map, spot_trades_map = load_spot_market_maps(service, spot_symbol_map, depth_limit, request_timeout)
            perp_orderbook_map, perp_trades_map = load_perp_reference_maps(service, symbol_map, snapshot_by_key, depth_limit, request_timeout)
        else:
            spot_snapshot_map = {}
            spot_orderbook_map = {}
            spot_trades_map = {}
            perp_orderbook_map = {}
            perp_trades_map = {}
            for exchange_key in alert_exchange_keys:
                if not spot_symbol_map.get(exchange_key) or not symbol_map.get(exchange_key):
                    continue
                spot_snapshot_map[exchange_key], spot_orderbook_map[exchange_key], spot_trades_map[exchange_key] = load_single_spot_market(
                    service,
                    exchange_key,
                    spot_symbol_map[exchange_key],
                    depth_limit,
                    request_timeout,
                )
                perp_orderbook_map[exchange_key], perp_trades_map[exchange_key] = load_single_perp_reference(
                    service,
                    exchange_key,
                    symbol_map[exchange_key],
                    snapshot_by_key[exchange_key],
                    depth_limit,
                    request_timeout,
                )
        if history_sqlite_enabled and spot_snapshot_map:
            schedule_history_write(
                f"snapshots::{base_coin}::spot",
                lambda store=history_store, coin=base_coin, snapshots={key: value for key, value in spot_snapshot_map.items() if key in SPOT_EXCHANGE_ORDER}: store.record_snapshots(
                    coin,
                    snapshots,
                    market="spot",
                ),
            )
        alert_input_signature = (
            base_coin,
            interval,
            liquidation_window_minutes,
            tuple(
                (
                    exchange_key,
                    snapshot_by_key[exchange_key].timestamp_ms or 0,
                    spot_snapshot_map[exchange_key].timestamp_ms or 0,
                    len(spot_trades_map[exchange_key]),
                    _latest_trade_timestamp(spot_trades_map[exchange_key]) or 0,
                    len(perp_trades_map[exchange_key]),
                    _latest_trade_timestamp(perp_trades_map[exchange_key]) or 0,
                    len(service.get_oi_history(exchange_key)),
                )
                for exchange_key in alert_exchange_keys
            ),
        )
        alert_inputs = get_cached_derived_result(
            f"alert-inputs::{base_coin}::{interval}",
            alert_input_signature,
            ttl_seconds=max(4, refresh_seconds),
            builder=lambda: {
                "spot_exchange_rows": [
                    {
                        "交易所": EXCHANGE_TITLES[exchange_key],
                        "现货价格": spot_snapshot_map[exchange_key].last_price if spot_snapshot_map[exchange_key].status == "ok" else None,
                        "永续价格": snapshot_by_key[exchange_key].last_price if snapshot_by_key[exchange_key].status == "ok" else None,
                        "Basis(%)": exchange_metrics.get("basis_pct"),
                        "现货价差(bps)": exchange_metrics.get("spot_spread_bps"),
                        "永续价差(bps)": exchange_metrics.get("perp_spread_bps"),
                        "永续/现货成交额比": exchange_metrics.get("spot_volume_ratio"),
                        "现货主动买占比(%)": (exchange_metrics.get("spot_buy_ratio") or 0.0) * 100.0 if exchange_metrics.get("spot_buy_ratio") is not None else None,
                        "现货盘口失衡(%)": summarize_orderbook(
                            spot_orderbook_map[exchange_key],
                            spot_snapshot_map[exchange_key].last_price if spot_snapshot_map[exchange_key].status == "ok" else None,
                        ).get("imbalance_pct"),
                        "合约盘口失衡(%)": summarize_orderbook(
                            perp_orderbook_map[exchange_key],
                            snapshot_by_key[exchange_key].last_price,
                        ).get("imbalance_pct"),
                        "现货24h成交额": spot_snapshot_map[exchange_key].volume_24h_notional if spot_snapshot_map[exchange_key].status == "ok" else None,
                        "合约24h成交额": snapshot_by_key[exchange_key].volume_24h_notional,
                        "合约持仓量": snapshot_by_key[exchange_key].open_interest,
                        "合约持仓金额": snapshot_by_key[exchange_key].open_interest_notional,
                        "资金费率(bps)": snapshot_by_key[exchange_key].funding_bps,
                    }
                    for exchange_key in alert_exchange_keys
                    for exchange_metrics in [
                        build_spot_perp_metrics(
                            spot_snapshot_map[exchange_key],
                            snapshot_by_key[exchange_key],
                            spot_orderbook_map[exchange_key],
                            perp_orderbook_map[exchange_key],
                            spot_trades_map[exchange_key],
                        )
                    ]
                ],
                "lead_lag_rows": [
                    {
                        "交易所": EXCHANGE_TITLES[exchange_key],
                        "领先方": lead_lag_metrics.get("leader"),
                        "领先秒数": lead_lag_metrics.get("lag_seconds"),
                        "相关性": lead_lag_metrics.get("correlation"),
                        "提示": lead_lag_metrics.get("summary") or "样本不足",
                    }
                    for exchange_key in alert_exchange_keys
                    for lead_lag_metrics in [
                        compute_spot_perp_lead_lag(
                            spot_trades_map[exchange_key],
                            perp_trades_map[exchange_key],
                            now_ms=now_ms,
                            lookback_minutes=max(3, min(liquidation_window_minutes, 10)),
                            bucket_seconds=1,
                            max_lag_buckets=3,
                        )
                    ]
                ],
                "oi_metrics_by_exchange": {
                    EXCHANGE_TITLES[exchange_key]: build_oi_quadrant_metrics(
                        merge_oi_points(
                            load_oi_backfill_cached(exchange_key, symbol_map[exchange_key], interval, max(60, candle_limit // 2), request_timeout),
                            service.get_oi_history(exchange_key),
                        ),
                        load_candles_cached(exchange_key, symbol_map[exchange_key], interval, candle_limit, request_timeout),
                    )
                    for exchange_key in alert_exchange_keys
                },
                "trade_metrics_by_exchange": {
                    EXCHANGE_TITLES[exchange_key]: build_trade_metrics(perp_trades_map[exchange_key], now_ms, liquidation_window_minutes)
                    for exchange_key in alert_exchange_keys
                },
                "liquidation_metrics_by_exchange": {
                    EXCHANGE_TITLES[exchange_key]: build_liquidation_metrics(
                        merge_liquidation_events(
                            load_liquidations_cached(exchange_key, symbol_map[exchange_key], liquidation_limit, request_timeout),
                            service.get_liquidation_history(exchange_key),
                        ),
                        now_ms,
                        liquidation_window_minutes,
                    )
                    for exchange_key in alert_exchange_keys
                },
            },
        )
        spot_exchange_frame = build_spot_perp_exchange_frame(alert_inputs["spot_exchange_rows"])
        lead_lag_frame = pd.DataFrame(alert_inputs["lead_lag_rows"])
        spot_perp_alert_frame = build_spot_perp_alert_frame(
            spot_exchange_frame,
            lead_lag_frame,
            alert_inputs["oi_metrics_by_exchange"],
            alert_inputs["trade_metrics_by_exchange"],
            alert_inputs["liquidation_metrics_by_exchange"],
            crowd_payload,
        )
        alert_scope_key = f"{base_coin}::{interval}::{liquidation_window_minutes}"
        alert_state, alert_timeline = ensure_alert_engine_buffers(alert_scope_key)
        confirmed_alert_frame, alert_timeline_frame, next_alert_state, next_alert_timeline = evolve_alert_engine(
            spot_perp_alert_frame,
            alert_state,
            alert_timeline,
            now_ms=now_ms,
            confirm_after=int(alert_confirm_after),
            cooldown_minutes=int(alert_cooldown_minutes),
        )
        st.session_state[f"alert_engine_state::{alert_scope_key}"] = next_alert_state
        st.session_state[f"alert_engine_timeline::{alert_scope_key}"] = next_alert_timeline
        if history_sqlite_enabled:
            schedule_history_write(
                f"alerts::{base_coin}::center",
                lambda store=history_store, coin=base_coin, frame=alert_timeline_frame.copy(), symbols=dict(symbol_map): store.record_alert_timeline(
                    coin,
                    frame,
                    symbol_map=symbols,
                    exchange_title_map=EXCHANGE_TITLES,
                ),
            )
        alert_center_notifications = collect_new_alert_notifications(alert_timeline_frame, base_coin=base_coin)
        if browser_notify_enabled:
            routed_browser = route_alert_notifications(
                alert_center_notifications,
                channel="browser",
                min_level=browser_min_level,
                cooldown_minutes=int(alert_cooldown_minutes),
            )
            if routed_browser:
                emit_browser_notifications(routed_browser, enable_sound=browser_sound_enabled)
        if telegram_push_enabled and telegram_bot_token.strip() and telegram_chat_id.strip():
            routed_telegram = route_alert_notifications(
                alert_center_notifications,
                channel="telegram",
                min_level=telegram_min_level,
                cooldown_minutes=int(alert_cooldown_minutes),
            )
            if routed_telegram:
                emit_telegram_notifications(
                    routed_telegram,
                    token=telegram_bot_token.strip(),
                    chat_id=telegram_chat_id.strip(),
                    timeout=request_timeout,
                )
        render_section("告警中心", "集中看连续确认后的强/中/弱告警，支持筛选、静音和一键切到复盘窗口。")
        st.caption(
            f"当前规则: 连续确认 `{int(alert_confirm_after)}` 次 | 冷却 `{int(alert_cooldown_minutes)}` 分钟"
            f" | 桌面通知 `>={browser_min_level}` | Telegram `>={telegram_min_level}`"
        )
        if exchange_scope_mode == "当前交易所优先":
            st.info("当前是 `当前交易所优先` 模式，告警中心只保留当前现货焦点和对应合约。要看三所联动告警，请切到 `全部交易所`。")
        alert_exchange_options = sorted(confirmed_alert_frame["交易所"].dropna().unique().tolist()) if not confirmed_alert_frame.empty else []
        alert_title_options = sorted(confirmed_alert_frame["告警"].dropna().unique().tolist()) if not confirmed_alert_frame.empty else []
        muted_alerts = st.multiselect(
            "静音告警",
            alert_title_options,
            default=st.session_state.get(chart_key("muted-alerts", base_coin, interval), []),
            key=chart_key("muted-alerts", base_coin, interval),
        )
        alert_exchange_filter = st.multiselect(
            "交易所筛选",
            alert_exchange_options,
            default=alert_exchange_options,
            key=chart_key("alert-filter-exchanges", base_coin, interval),
        )
        only_strong_alerts = st.checkbox("只看强告警", value=False, key=chart_key("only-strong", base_coin, interval))
        alert_center_frame = confirmed_alert_frame.copy()
        if muted_alerts:
            alert_center_frame = alert_center_frame[~alert_center_frame["告警"].isin(muted_alerts)]
        if alert_exchange_filter:
            alert_center_frame = alert_center_frame[alert_center_frame["交易所"].isin(alert_exchange_filter)]
        if only_strong_alerts:
            alert_center_frame = alert_center_frame[alert_center_frame["等级"] == "强"]
        alert_center_left, alert_center_right = st.columns([1.55, 1.45], gap="large")
        with alert_center_left:
            st.plotly_chart(
                build_alert_timeline_figure(alert_timeline_frame),
                key=chart_key("alert-center-timeline", base_coin, interval),
                config=PLOTLY_CONFIG,
            )
        with alert_center_right:
            st.dataframe(
                alert_center_frame,
                width="stretch",
                hide_index=True,
                column_config={
                    "交易所": st.column_config.TextColumn(),
                    "等级": st.column_config.TextColumn(),
                    "告警": st.column_config.TextColumn(),
                    "解释": st.column_config.TextColumn(width="large"),
                    "连续触发": st.column_config.NumberColumn(format="%d"),
                    "状态": st.column_config.TextColumn(),
                },
            )
        if history_sqlite_enabled:
            alert_history_summary = history_store.describe()
            alert_now_ms = selected_snapshot.timestamp_ms or int(time.time() * 1000)
            alert_closure_since_ms = alert_now_ms - max(int(alert_review_window_minutes), 60) * 20 * 60_000
            alert_closure_payload = get_cached_derived_result(
                f"alert-closure::{base_coin}",
                (
                    int(alert_closure_since_ms),
                    int(alert_history_summary.get("last_market_ts") or 0),
                    int(alert_history_summary.get("last_alert_ts") or 0),
                    int(alert_history_summary.get("last_event_ts") or 0),
                    int(alert_history_summary.get("last_quality_ts") or 0),
                    tuple(scope_perp_keys),
                ),
                ttl_seconds=max(20, refresh_seconds * 5),
                builder=lambda: build_history_workbench_payload(
                    history_store,
                    coin=base_coin,
                    exchange_keys=scope_perp_keys,
                    markets=["perp", "spot"],
                    event_categories=["all"],
                    since_ms=alert_closure_since_ms,
                ),
            ) or {}
            alert_closure_frame = build_alert_closure_frame(
                alert_closure_payload.get("alert_history_frame", pd.DataFrame()),
                alert_closure_payload.get("market_history_frame", pd.DataFrame()),
                alert_closure_payload.get("event_history_frame", pd.DataFrame()),
                alert_closure_payload.get("quality_history_frame", pd.DataFrame()),
                review_window_minutes=max(int(alert_review_window_minutes), 60),
                limit=18,
            )
            render_section("告警闭环板", "同一张表里回看告警触发时的价格、Funding、OI、事件流和复盘结果，减少‘告警响了但过后说不清’。")
            if alert_closure_frame.empty:
                st.info("当前历史样本还不足以形成告警闭环板。")
            else:
                st.dataframe(
                    alert_closure_frame,
                    width="stretch",
                    hide_index=True,
                    column_config={
                        "触发价格": st.column_config.NumberColumn(format="%.2f"),
                        "触发Funding": st.column_config.NumberColumn(format="%.2f"),
                        "触发OI": st.column_config.NumberColumn(format="%.2f"),
                        "5m事件额": st.column_config.NumberColumn(format="%.2f"),
                        "5m事件数": st.column_config.NumberColumn(format="%d"),
                        "近价撤补比": st.column_config.NumberColumn(format="%.2f"),
                        "顺风区间(%)": st.column_config.NumberColumn(format="%.3f"),
                        "逆风区间(%)": st.column_config.NumberColumn(format="%.3f"),
                        "窗口收益(%)": st.column_config.NumberColumn(format="%.3f"),
                    },
                )
        alert_jump_options = ["不跳转"] + [f"{row['交易所']} | {row['告警']}" for _, row in alert_center_frame.iterrows()]
        alert_jump = st.selectbox("告警跳转到复盘", alert_jump_options, key=chart_key("alert-jump", base_coin, interval))
        if st.button("跳到最近 2 分钟复盘窗口", key=chart_key("alert-jump-button", base_coin, interval)):
            st.session_state[chart_key("replay-mode", base_coin, selected_exchange, selected_symbol, interval)] = "最近 2 分钟"
            st.session_state[chart_key("replay-progress", base_coin, selected_exchange, selected_symbol, interval)] = 100
            if alert_jump != "不跳转":
                st.info(f"已把复盘窗口切到最近 2 分钟，可回到 `交易台深度页` 直接查看 `{alert_jump}`。")
        return

    if active_view == "接口调试":
        live_trade_events = service.get_trade_history(selected_exchange)
        trade_events = (
            live_trade_events
            if _use_live_trades(live_trade_events)
            else load_exchange_trades_cached(selected_exchange, selected_symbol, 160, request_timeout)
        )
        session_liquidations = service.get_liquidation_history(selected_exchange)
        liquidation_events = merge_liquidation_events(
            load_liquidations_cached(selected_exchange, selected_symbol, liquidation_limit, request_timeout),
            session_liquidations,
        )
        spot_snapshot_map, spot_orderbook_map, spot_trades_map = load_spot_market_maps(service, spot_symbol_map, depth_limit, request_timeout)
        exchange_health_frame = build_exchange_health_frame(
            describe_exchange_request_health(),
            service=service,
            snapshot_by_key=snapshot_by_key,
            spot_snapshot_map=spot_snapshot_map,
            available_perp_keys=available_perp_keys,
            available_spot_keys=available_spot_keys,
            catalog_status=coin_catalog_status,
        )
        render_section("接口调试", "查看原始接口返回，定位网络或符号问题。")
        st.write(f"当前主图交易所: `{selected_snapshot.exchange}` | 合约: `{selected_symbol}`")
        st.dataframe(
            exchange_health_frame,
            width="stretch",
            hide_index=True,
            column_config={
                "连续失败": st.column_config.NumberColumn(format="%d"),
                "冷却剩余(s)": st.column_config.NumberColumn(format="%d"),
            },
        )
        spot_debug = " | ".join(
            f"{EXCHANGE_TITLES[key]} Spot 笔数 `{len(spot_trades_map[key])}` / 档位 `{len(spot_orderbook_map[key])}`"
            for key in spot_snapshot_map
        )
        st.write(
            f"会话内 OI 点数: `{len(service.get_oi_history(selected_exchange))}` | 会话内成交笔数: `{len(trade_events)}` | 会话内爆仓事件数: `{len(session_liquidations)}` | 合并后爆仓事件数: `{len(liquidation_events)}` | {spot_debug}"
        )
        for exchange_key in spot_snapshot_map:
            debug_spot_snapshot = spot_snapshot_map[exchange_key]
            with st.expander(f"{debug_spot_snapshot.exchange} | {debug_spot_snapshot.symbol} | {CARD_STATUS.get(debug_spot_snapshot.status, debug_spot_snapshot.status)}"):
                if debug_spot_snapshot.error:
                    st.error(debug_spot_snapshot.error)
                st.json(debug_spot_snapshot.raw)
        for snapshot in snapshots:
            with st.expander(f"{snapshot.exchange} | {snapshot.symbol} | {CARD_STATUS.get(snapshot.status, snapshot.status)}"):
                if snapshot.error:
                    st.error(snapshot.error)
                st.json(snapshot.raw)
        return

    candles = load_candles_cached(selected_exchange, selected_symbol, interval, candle_limit, request_timeout)
    live_orderbook = service.get_orderbook(selected_exchange)
    orderbook = (
        live_orderbook
        if _use_live_orderbook(live_orderbook, selected_snapshot.timestamp_ms)
        else load_orderbook_cached(selected_exchange, selected_symbol, depth_limit, request_timeout)
    )
    backfill_oi = load_oi_backfill_cached(selected_exchange, selected_symbol, interval, max(60, candle_limit // 2), request_timeout)
    session_oi_points = service.get_oi_history(selected_exchange)
    merged_oi = merge_oi_points(backfill_oi, session_oi_points)
    live_trade_events = service.get_trade_history(selected_exchange)
    trade_events = (
        live_trade_events
        if _use_live_trades(live_trade_events)
        else load_exchange_trades_cached(selected_exchange, selected_symbol, 160, request_timeout)
    )
    reference_price = selected_snapshot.last_price or selected_snapshot.mark_price or (candles[-1].close if candles else None)
    heat_bars = aggregate_heat_bars(orderbook, reference_price, heat_window_pct, heat_buckets, heat_bars_per_side)
    book_summary = summarize_orderbook(orderbook, reference_price)
    session_liquidations = service.get_liquidation_history(selected_exchange)
    rest_liquidations = load_liquidations_cached(selected_exchange, selected_symbol, liquidation_limit, request_timeout)
    liquidation_events = merge_liquidation_events(rest_liquidations, session_liquidations)
    liquidation_metrics = build_liquidation_metrics(liquidation_events, selected_snapshot.timestamp_ms or int(time.time() * 1000), liquidation_window_minutes)
    trade_metrics = build_trade_metrics(trade_events, selected_snapshot.timestamp_ms or int(time.time() * 1000), liquidation_window_minutes)
    oi_quadrant = build_oi_quadrant_metrics(merged_oi, candles)
    need_home_full = active_view == "首页总览" and overview_mode == "完整"
    need_lab_view = active_view == "增强实验室"
    need_deep_view = active_view == "交易台深度页"
    need_multi_exchange_context = need_home_full or need_lab_view or (need_deep_view and exchange_scope_mode == "全部交易所")
    selected_transport_health = service.get_transport_health(selected_exchange)
    perp_quality_history = service.get_orderbook_quality_history(selected_exchange)
    quality_summary_frame = build_orderbook_quality_frame(perp_quality_history, limit=24)
    spot_quality_history = service.get_spot_orderbook_quality_history(selected_exchange) if spot_symbol_map.get(selected_exchange) else []
    perp_recorded_events = service.get_recorded_events(selected_exchange)
    spot_recorded_events = service.get_spot_recorded_events(selected_exchange) if spot_symbol_map.get(selected_exchange) else []

    riskmap_bundle = {"liquidation": pd.DataFrame(), "tp": pd.DataFrame(), "stop": pd.DataFrame()}
    if need_deep_view:
        riskmap_signature = (
            selected_exchange,
            selected_symbol,
            interval,
            len(candles),
            candles[-1].timestamp_ms if candles else 0,
            len(orderbook),
            selected_snapshot.timestamp_ms or 0,
            round(reference_price or 0.0, 2),
            risk_heat_window_pct,
            risk_heat_buckets,
        )
        riskmap_bundle = get_cached_derived_result(
            f"riskmaps::{selected_exchange}::{selected_symbol}::{interval}",
            riskmap_signature,
            ttl_seconds=balanced_ttl,
            builder=lambda: {
                "liquidation": build_probability_heatmap_frame(candles, orderbook, selected_snapshot, "liquidation", reference_price, risk_heat_window_pct, risk_heat_buckets),
                "tp": build_probability_heatmap_frame(candles, orderbook, selected_snapshot, "tp", reference_price, risk_heat_window_pct, risk_heat_buckets),
                "stop": build_probability_heatmap_frame(candles, orderbook, selected_snapshot, "stop", reference_price, risk_heat_window_pct, risk_heat_buckets),
            },
        )
    liquidation_heat_frame = riskmap_bundle["liquidation"]
    tp_heat_frame = riskmap_bundle["tp"]
    stop_heat_frame = riskmap_bundle["stop"]
    mbo_frame = build_mbo_profile_frame(orderbook, reference_price, mbo_rows) if need_deep_view else pd.DataFrame()
    crowd_payload = (
        load_binance_crowd_cached(symbol_map["binance"], crowd_period_for_interval(interval), request_timeout)
        if need_home_full or need_lab_view or selected_exchange == "binance"
        else {}
    )
    bybit_crowd_payload = (
        load_bybit_crowd_cached(symbol_map["bybit"], crowd_period_for_interval(interval), request_timeout)
        if need_home_full or need_lab_view or selected_exchange == "bybit"
        else {}
    )
    spot_snapshot_map, spot_orderbook_map, spot_trades_map = load_spot_market_maps(
        service,
        spot_symbol_map,
        depth_limit,
        request_timeout,
        exchange_keys=load_spot_keys,
    )
    perp_orderbook_map, perp_trades_map = load_perp_reference_maps(
        service,
        symbol_map,
        snapshot_by_key,
        depth_limit,
        request_timeout,
        exchange_keys=load_perp_reference_keys,
    )
    exchange_health_frame = build_exchange_health_frame(
        describe_exchange_request_health(),
        service=service,
        snapshot_by_key=snapshot_by_key,
        spot_snapshot_map=spot_snapshot_map,
        available_perp_keys=available_perp_keys,
        available_spot_keys=available_spot_keys,
        catalog_status=coin_catalog_status,
    )
    home_spot_exchange = "binance" if "binance" in spot_snapshot_map else spot_focus_exchange
    spot_snapshot = spot_snapshot_map[home_spot_exchange]
    spot_orderbook = spot_orderbook_map[home_spot_exchange]
    spot_trades = spot_trades_map[home_spot_exchange]
    home_perp_exchange = "binance" if "binance" in perp_orderbook_map else selected_exchange
    home_perp_snapshot = snapshot_by_key[home_perp_exchange]
    home_perp_orderbook = perp_orderbook_map[home_perp_exchange]
    basis_payload = (
        load_binance_basis_curve_cached(spot_symbol, crowd_period_for_interval(interval), request_timeout)
        if need_home_full
        else {}
    )
    if basis_payload and not basis_payload.get("PERPETUAL") and spot_snapshot.status == "ok" and spot_snapshot.last_price not in (None, 0) and binance_perp_snapshot.last_price is not None:
        perp_basis = binance_perp_snapshot.last_price - spot_snapshot.last_price
        perp_basis_rate = perp_basis / spot_snapshot.last_price
        basis_payload = {
            **basis_payload,
            "PERPETUAL": [
                {
                    "indexPrice": str(spot_snapshot.last_price),
                    "contractType": "PERPETUAL",
                    "basisRate": str(perp_basis_rate),
                    "futuresPrice": str(binance_perp_snapshot.last_price),
                    "annualizedBasisRate": str(perp_basis_rate),
                    "basis": str(perp_basis),
                    "pair": spot_symbol,
                    "timestamp": selected_snapshot.timestamp_ms or int(time.time() * 1000),
                }
            ],
        }
    live_selected_oi_history = session_oi_points
    oi_matrix_signature = (
        selected_exchange,
        selected_symbol,
        interval,
        len(live_selected_oi_history),
        live_selected_oi_history[-1].timestamp_ms if live_selected_oi_history else 0,
        request_timeout,
    )
    oi_matrix_metrics = (
        get_stale_while_revalidate_result(
            f"oi-matrix::{selected_exchange}::{selected_symbol}::{interval}",
            oi_matrix_signature,
            ttl_seconds=heavy_ttl,
            stale_ttl_seconds=stale_ttl,
            builder=lambda: {
                matrix_interval: build_oi_quadrant_metrics(
                    merge_oi_points(
                        load_oi_backfill_cached(selected_exchange, selected_symbol, matrix_interval, 120, request_timeout),
                        live_selected_oi_history,
                    ),
                    load_candles_cached(selected_exchange, selected_symbol, matrix_interval, 180, request_timeout),
                )
                for matrix_interval in ("5m", "15m", "1h", "4h")
            },
        )
        if active_view == "首页总览" and overview_mode == "完整"
        else {}
    )
    now_ms = selected_snapshot.timestamp_ms or int(time.time() * 1000)
    overview_signature = (tuple(dict.fromkeys(overview_coins)), int(request_timeout), int(liquidation_limit))
    overview_builder = lambda: load_market_overview_frame_cached(tuple(dict.fromkeys(overview_coins)), request_timeout, liquidation_limit)
    if need_home_full:
        overview_frame = get_stale_while_revalidate_result(
            f"home-overview-full::{base_coin}",
            overview_signature,
            ttl_seconds=heavy_ttl,
            stale_ttl_seconds=stale_ttl,
            builder=overview_builder,
        )
        if overview_frame is None:
            overview_frame = pd.DataFrame()
    else:
        overview_frame = pd.DataFrame()
        if overview_coins:
            prefetch_stale_result(
                f"home-overview-full::{base_coin}",
                overview_signature,
                overview_builder,
                max_age_seconds=prefetch_age,
            )
    spot_metrics = build_spot_perp_metrics(spot_snapshot, home_perp_snapshot, spot_orderbook, home_perp_orderbook, spot_trades)
    spot_summary = summarize_orderbook(spot_orderbook, spot_snapshot.last_price if spot_snapshot.status == "ok" else None)
    binance_perp_summary = summarize_orderbook(home_perp_orderbook, home_perp_snapshot.last_price)
    spot_summary_map = {
        exchange_key: summarize_orderbook(
            spot_orderbook_map[exchange_key],
            spot_snapshot_map[exchange_key].last_price if spot_snapshot_map[exchange_key].status == "ok" else None,
        )
        for exchange_key in load_spot_keys
    }
    perp_summary_map = {
        exchange_key: summarize_orderbook(
            perp_orderbook_map[exchange_key],
            snapshot_by_key[exchange_key].last_price,
        )
        for exchange_key in load_perp_reference_keys
        if exchange_key in perp_orderbook_map and exchange_key in snapshot_by_key
    }
    binance_candles = candles if selected_exchange == "binance" else []
    binance_merged_oi = merged_oi if selected_exchange == "binance" else []
    if need_home_full and selected_exchange != "binance":
        binance_candles = load_candles_cached("binance", symbol_map["binance"], interval, candle_limit, request_timeout)
        binance_merged_oi = merge_oi_points(
            load_oi_backfill_cached("binance", symbol_map["binance"], interval, max(60, candle_limit // 2), request_timeout),
            service.get_oi_history("binance"),
        )
    binance_oi_quadrant = (
        oi_quadrant
        if selected_exchange == "binance"
        else build_oi_quadrant_metrics(binance_merged_oi, binance_candles)
        if binance_candles and binance_merged_oi
        else {}
    )
    crowd_position_ratio = latest_ratio(crowd_payload, "top_position", "longShortRatio")
    crowd_account_ratio = latest_ratio(crowd_payload, "top_account", "longShortRatio")
    global_ratio = latest_ratio(crowd_payload, "global_account", "longShortRatio")
    taker_ratio = latest_ratio(crowd_payload, "taker_ratio", "buySellRatio")
    top_position_long_share = latest_payload_float(crowd_payload, "top_position", "longAccount")
    top_position_short_share = latest_payload_float(crowd_payload, "top_position", "shortAccount")
    top_account_long_share = latest_payload_float(crowd_payload, "top_account", "longAccount")
    top_account_short_share = latest_payload_float(crowd_payload, "top_account", "shortAccount")
    global_account_long_share = latest_payload_float(crowd_payload, "global_account", "longAccount")
    global_account_short_share = latest_payload_float(crowd_payload, "global_account", "shortAccount")
    bybit_contract_ratio = latest_ratio(bybit_crowd_payload, "account_ratio", "longShortRatio")
    bybit_account_long_share = latest_payload_float(bybit_crowd_payload, "account_ratio", "longAccount")
    bybit_account_short_share = latest_payload_float(bybit_crowd_payload, "account_ratio", "shortAccount")
    contract_ratio_by_exchange = {
        "binance": crowd_position_ratio if crowd_position_ratio is not None else crowd_account_ratio if crowd_account_ratio is not None else global_ratio,
        "bybit": bybit_contract_ratio,
        "okx": None,
        "hyperliquid": None,
    }
    contract_sentiment_payloads = {
        "binance": crowd_payload,
        "bybit": bybit_crowd_payload,
        "okx": {},
        "hyperliquid": {},
    }
    binance_trades = (
        trade_events
        if selected_exchange == "binance"
        else service.get_trade_history("binance")
        if _use_live_trades(service.get_trade_history("binance"))
        else load_exchange_trades_cached("binance", symbol_map["binance"], 160, request_timeout)
    )
    composite_signal = build_composite_signal(
        selected_snapshot,
        oi_quadrant,
        trade_metrics,
        crowd_position_ratio if selected_exchange == "binance" else None,
        crowd_account_ratio if selected_exchange == "binance" else None,
        global_ratio if selected_exchange == "binance" else None,
    )
    crowd_alerts = pd.DataFrame(columns=["告警", "等级", "解释"])
    binance_contract_overview = pd.DataFrame(columns=["指标", "数值", "说明"])
    if need_home_full:
        crowd_alerts = build_binance_crowding_alerts(
            crowd_payload,
            binance_perp_snapshot.funding_bps,
            binance_oi_quadrant.get("oi_change_pct"),
            build_trade_metrics(binance_trades, now_ms, liquidation_window_minutes),
        )
        binance_contract_overview = pd.DataFrame(
            [
                {"指标": "当前未平仓量(OI)", "数值": binance_perp_snapshot.open_interest, "说明": "Binance 合约当前未平仓张数 / 币数口径"},
                {"指标": "当前未平仓金额", "数值": binance_perp_snapshot.open_interest_notional, "说明": "按公开 OI 与现价估算的合约未平仓金额"},
                {"指标": "OI Delta", "数值": binance_oi_quadrant.get("oi_change_pct"), "说明": "当前周期 OI 变化百分比"},
                {"指标": "大户持仓多空比", "数值": crowd_position_ratio, "说明": "Top Trader Position Ratio"},
                {"指标": "大户账户多空比", "数值": crowd_account_ratio, "说明": "Top Trader Account Ratio"},
                {"指标": "全市场账户多空比", "数值": global_ratio, "说明": "Global Long/Short Account Ratio"},
                {"指标": "主动买卖比", "数值": taker_ratio, "说明": "Taker buy/sell ratio"},
                {"指标": "大户持仓多头占比(%)", "数值": None if top_position_long_share is None else top_position_long_share * 100.0, "说明": "大户持仓口径的多头占比"},
                {"指标": "大户持仓空头占比(%)", "数值": None if top_position_short_share is None else top_position_short_share * 100.0, "说明": "大户持仓口径的空头占比"},
                {"指标": "大户账户多头占比(%)", "数值": None if top_account_long_share is None else top_account_long_share * 100.0, "说明": "大户账户口径的多头占比"},
                {"指标": "大户账户空头占比(%)", "数值": None if top_account_short_share is None else top_account_short_share * 100.0, "说明": "大户账户口径的空头占比"},
                {"指标": "全市场账户多头占比(%)", "数值": None if global_account_long_share is None else global_account_long_share * 100.0, "说明": "全市场账户口径的多头占比"},
                {"指标": "全市场账户空头占比(%)", "数值": None if global_account_short_share is None else global_account_short_share * 100.0, "说明": "全市场账户口径的空头占比"},
            ]
        )
    spot_exchange_rows: List[Dict[str, float | str | None]] = []
    lead_lag_rows: List[Dict[str, float | str | None]] = []
    spot_exchange_frame = pd.DataFrame()
    lead_lag_frame = pd.DataFrame(columns=["交易所", "领先方", "领先秒数", "相关性", "置信度", "样本桶数", "提示"])
    directional_liquidation_zones = build_directional_heat_zone_frames(liquidation_heat_frame, below_limit=10, above_limit=10)
    directional_tp_zones = build_directional_heat_zone_frames(tp_heat_frame, below_limit=10, above_limit=10)
    directional_stop_zones = build_directional_heat_zone_frames(stop_heat_frame, below_limit=10, above_limit=10)
    oi_metrics_by_exchange: Dict[str, Dict[str, float | str | None]] = {}
    trade_metrics_by_exchange: Dict[str, Dict[str, float | int | str | None]] = {}
    liquidation_metrics_by_exchange: Dict[str, Dict[str, float | int | str | None]] = {}
    trade_events_by_exchange: Dict[str, List[TradeEvent]] = {selected_exchange: list(perp_trades_map.get(selected_exchange, trade_events))}
    liquidation_events_by_exchange: Dict[str, List[LiquidationEvent]] = {selected_exchange: list(liquidation_events)}
    spot_perp_alert_frame = pd.DataFrame()
    confirmed_alert_frame = pd.DataFrame()
    alert_timeline_frame = pd.DataFrame()
    new_alert_notifications: List[Dict[str, str]] = []
    cross_exchange_session_liqs: List[LiquidationEvent] = []
    cross_exchange_cluster_frame = pd.DataFrame()
    if need_multi_exchange_context:
        for exchange_key in load_spot_keys:
            exchange_spot_snapshot = spot_snapshot_map[exchange_key]
            exchange_perp_snapshot = snapshot_by_key[exchange_key]
            exchange_metrics = build_spot_perp_metrics(
                exchange_spot_snapshot,
                exchange_perp_snapshot,
                spot_orderbook_map[exchange_key],
                perp_orderbook_map[exchange_key],
                spot_trades_map[exchange_key],
            )
            spot_exchange_rows.append(
                {
                    "交易所": EXCHANGE_TITLES[exchange_key],
                    "现货价格": exchange_spot_snapshot.last_price if exchange_spot_snapshot.status == "ok" else None,
                    "永续价格": exchange_perp_snapshot.last_price if exchange_perp_snapshot.status == "ok" else None,
                    "Basis(%)": exchange_metrics.get("basis_pct"),
                    "现货价差(bps)": exchange_metrics.get("spot_spread_bps"),
                    "永续价差(bps)": exchange_metrics.get("perp_spread_bps"),
                    "永续/现货成交额比": exchange_metrics.get("spot_volume_ratio"),
                    "现货主动买占比(%)": (exchange_metrics.get("spot_buy_ratio") or 0.0) * 100.0 if exchange_metrics.get("spot_buy_ratio") is not None else None,
                    "现货盘口失衡(%)": spot_summary_map.get(exchange_key, {}).get("imbalance_pct"),
                    "合约盘口失衡(%)": perp_summary_map.get(exchange_key, {}).get("imbalance_pct"),
                    "现货24h成交额": exchange_spot_snapshot.volume_24h_notional if exchange_spot_snapshot.status == "ok" else None,
                    "合约24h成交额": exchange_perp_snapshot.volume_24h_notional,
                    "合约持仓量": exchange_perp_snapshot.open_interest,
                    "合约持仓金额": exchange_perp_snapshot.open_interest_notional,
                    "资金费率(bps)": exchange_perp_snapshot.funding_bps,
                }
            )
            lead_lag_metrics = compute_spot_perp_lead_lag(
                spot_trades_map[exchange_key],
                perp_trades_map[exchange_key],
                now_ms=now_ms,
                lookback_minutes=max(3, min(liquidation_window_minutes, 10)),
                bucket_seconds=1,
                max_lag_buckets=3,
            )
            lead_lag_rows.append(
                {
                    "交易所": EXCHANGE_TITLES[exchange_key],
                    "领先方": lead_lag_metrics.get("leader"),
                    "领先秒数": lead_lag_metrics.get("lag_seconds"),
                    "相关性": lead_lag_metrics.get("correlation"),
                    "置信度": _confidence_label(lead_lag_metrics.get("confidence")),
                    "样本桶数": lead_lag_metrics.get("samples"),
                    "提示": lead_lag_metrics.get("summary") or "样本不足",
                }
            )
        for exchange_key in load_perp_reference_keys:
            if exchange_key == selected_exchange:
                exchange_candles = candles
                exchange_oi = merged_oi
                exchange_liquidations = liquidation_events
            elif exchange_key == "binance":
                exchange_candles = binance_candles
                exchange_oi = binance_merged_oi
                exchange_liquidations = merge_liquidation_events(
                    load_liquidations_cached(exchange_key, symbol_map[exchange_key], liquidation_limit, request_timeout),
                    service.get_liquidation_history(exchange_key),
                )
            else:
                exchange_candles = load_candles_cached(exchange_key, symbol_map[exchange_key], interval, candle_limit, request_timeout)
                exchange_oi = merge_oi_points(
                    load_oi_backfill_cached(exchange_key, symbol_map[exchange_key], interval, max(60, candle_limit // 2), request_timeout),
                    service.get_oi_history(exchange_key),
                )
                exchange_liquidations = merge_liquidation_events(
                    load_liquidations_cached(exchange_key, symbol_map[exchange_key], liquidation_limit, request_timeout),
                    service.get_liquidation_history(exchange_key),
                )
            display_name = EXCHANGE_TITLES[exchange_key]
            oi_metrics_by_exchange[display_name] = build_oi_quadrant_metrics(exchange_oi, exchange_candles)
            trade_metrics_by_exchange[display_name] = build_trade_metrics(perp_trades_map[exchange_key], now_ms, liquidation_window_minutes)
            liquidation_metrics_by_exchange[display_name] = build_liquidation_metrics(exchange_liquidations, now_ms, liquidation_window_minutes)
            trade_events_by_exchange[exchange_key] = list(perp_trades_map.get(exchange_key, []))
            liquidation_events_by_exchange[exchange_key] = list(exchange_liquidations)
        spot_exchange_frame = build_spot_perp_exchange_frame(spot_exchange_rows)
        lead_lag_frame = pd.DataFrame(lead_lag_rows)
        spot_perp_alert_frame = build_spot_perp_alert_frame(
            spot_exchange_frame,
            lead_lag_frame,
            oi_metrics_by_exchange,
            trade_metrics_by_exchange,
            liquidation_metrics_by_exchange,
            crowd_payload,
        )
        alert_scope_key = f"{base_coin}::{interval}::{liquidation_window_minutes}"
        alert_state, alert_timeline = ensure_alert_engine_buffers(alert_scope_key)
        confirmed_alert_frame, alert_timeline_frame, next_alert_state, next_alert_timeline = evolve_alert_engine(
            spot_perp_alert_frame,
            alert_state,
            alert_timeline,
            now_ms=now_ms,
            confirm_after=int(alert_confirm_after),
            cooldown_minutes=int(alert_cooldown_minutes),
        )
        st.session_state[f"alert_engine_state::{alert_scope_key}"] = next_alert_state
        st.session_state[f"alert_engine_timeline::{alert_scope_key}"] = next_alert_timeline
        if history_sqlite_enabled:
            schedule_history_write(
                f"alerts::{base_coin}::multi",
                lambda store=history_store, coin=base_coin, frame=alert_timeline_frame.copy(), symbols=dict(symbol_map): store.record_alert_timeline(
                    coin,
                    frame,
                    symbol_map=symbols,
                    exchange_title_map=EXCHANGE_TITLES,
                ),
            )
        new_alert_notifications = collect_new_alert_notifications(alert_timeline_frame, base_coin=base_coin)
        cross_liq_signature = tuple(
            (
                exchange_key,
                len(service.get_liquidation_history(exchange_key)),
                service.get_liquidation_history(exchange_key)[-1].timestamp_ms if service.get_liquidation_history(exchange_key) else 0,
            )
            for exchange_key in available_perp_keys
        )
        cross_exchange_session_liqs = get_cached_derived_result(
            f"cross-liq::{base_coin}::{interval}",
            cross_liq_signature,
            ttl_seconds=balanced_ttl,
            builder=lambda: [event for exchange_key in available_perp_keys for event in service.get_liquidation_history(exchange_key)],
        )
    selected_display_name = EXCHANGE_TITLES[selected_exchange]
    if selected_display_name not in oi_metrics_by_exchange:
        oi_metrics_by_exchange[selected_display_name] = oi_quadrant
    if selected_display_name not in trade_metrics_by_exchange:
        trade_metrics_by_exchange[selected_display_name] = trade_metrics
    if selected_display_name not in liquidation_metrics_by_exchange:
        liquidation_metrics_by_exchange[selected_display_name] = liquidation_metrics
    trade_events_by_exchange.setdefault(selected_exchange, list(trade_events))
    liquidation_events_by_exchange.setdefault(selected_exchange, list(liquidation_events))
    contract_sentiment_frame = build_contract_sentiment_truth_frame(
        snapshot_by_key,
        contract_sentiment_payloads,
        trade_metrics_by_exchange,
        EXCHANGE_TITLES,
        exchange_keys=scope_perp_keys if exchange_scope_mode == "当前交易所优先" else list(EXCHANGE_ORDER),
    )
    contract_sentiment_alert_frame = build_contract_sentiment_alert_frame(contract_sentiment_frame) if need_home_full else pd.DataFrame()
    deep_spot_dashboard_frame = (
        build_spot_dashboard_frame(spot_snapshot_map, spot_summary_map, lead_lag_frame, exchange_keys=scope_spot_keys)
        if need_deep_view and exchange_scope_mode == "全部交易所"
        else pd.DataFrame()
    )
    deep_perp_dashboard_frame = (
        build_perp_dashboard_frame(snapshot_by_key, perp_summary_map, oi_metrics_by_exchange, contract_ratio_by_exchange, exchange_keys=scope_perp_keys)
        if need_deep_view and exchange_scope_mode == "全部交易所"
        else pd.DataFrame()
    )
    carry_surface_exchange_keys = scope_perp_keys if exchange_scope_mode == "当前交易所优先" else list(EXCHANGE_ORDER)
    carry_surface_frame = build_carry_surface_frame(
        build_carry_surface_rows(snapshot_by_key, spot_snapshot_map, carry_surface_exchange_keys)
    )
    spot_reference_exchange_keys = list(scope_spot_keys)
    perp_reference_exchange_keys = list(carry_surface_exchange_keys)
    spot_quality_history_by_exchange = {
        exchange_key: service.get_spot_orderbook_quality_history(exchange_key)
        for exchange_key in spot_reference_exchange_keys
        if spot_symbol_map.get(exchange_key)
    }
    perp_quality_history_by_exchange = {
        exchange_key: service.get_orderbook_quality_history(exchange_key)
        for exchange_key in perp_reference_exchange_keys
        if symbol_map.get(exchange_key)
    }
    spot_reference_window_minutes = max(10, min(20, liquidation_window_minutes))
    bybit_insurance_payload = load_bybit_insurance_cached(base_coin, request_timeout) if need_home_full and "bybit" in perp_reference_exchange_keys else {}
    bybit_insurance_value = payload_float(bybit_insurance_payload.get("total_value")) if isinstance(bybit_insurance_payload, dict) else None
    spot_reference_signature = (
        tuple(
            (
                exchange_key,
                int(spot_snapshot_map.get(exchange_key).timestamp_ms or 0) if spot_snapshot_map.get(exchange_key) is not None else 0,
                len(spot_orderbook_map.get(exchange_key, [])),
                len(spot_trades_map.get(exchange_key, [])),
                _latest_trade_timestamp(spot_trades_map.get(exchange_key, [])) or 0,
                len(spot_quality_history_by_exchange.get(exchange_key, [])),
                spot_quality_history_by_exchange.get(exchange_key)[-1].timestamp_ms if spot_quality_history_by_exchange.get(exchange_key) else 0,
            )
            for exchange_key in spot_reference_exchange_keys
        ),
        int(spot_reference_window_minutes),
    )
    spot_reference_builder = lambda: {
        "flow_frame": build_spot_flow_reference_frame(
            spot_snapshot_map,
            spot_orderbook_map,
            spot_trades_map,
            spot_quality_history_by_exchange,
            EXCHANGE_TITLES,
            exchange_keys=spot_reference_exchange_keys,
            now_ms=now_ms,
            window_minutes=spot_reference_window_minutes,
        ),
        "execution_frame": build_execution_quality_frame(
            spot_snapshot_map,
            spot_orderbook_map,
            spot_quality_history_by_exchange,
            EXCHANGE_TITLES,
            exchange_keys=spot_reference_exchange_keys,
            market_label="现货",
        ),
    }
    perp_reference_signature = (
        tuple(
            (
                exchange_key,
                int(snapshot_by_key.get(exchange_key).timestamp_ms or 0) if snapshot_by_key.get(exchange_key) is not None else 0,
                len(perp_orderbook_map.get(exchange_key, [])),
                len(perp_quality_history_by_exchange.get(exchange_key, [])),
                perp_quality_history_by_exchange.get(exchange_key)[-1].timestamp_ms if perp_quality_history_by_exchange.get(exchange_key) else 0,
                len(trade_events_by_exchange.get(exchange_key, [])),
                _latest_trade_timestamp(trade_events_by_exchange.get(exchange_key, [])) or 0,
            )
            for exchange_key in perp_reference_exchange_keys
        ),
        int(bybit_insurance_payload.get("updated_time_ms") or 0) if isinstance(bybit_insurance_payload, dict) else 0,
    )
    perp_reference_builder = lambda: {
        "crowding_frame": build_perp_crowding_trio_frame(contract_sentiment_frame, oi_metrics_by_exchange),
        "funding_frame": build_funding_regime_frame(
            snapshot_by_key,
            spot_snapshot_map,
            EXCHANGE_TITLES,
            exchange_keys=perp_reference_exchange_keys,
        ),
        "execution_frame": build_execution_quality_frame(
            snapshot_by_key,
            perp_orderbook_map,
            perp_quality_history_by_exchange,
            EXCHANGE_TITLES,
            exchange_keys=perp_reference_exchange_keys,
            market_label="合约",
        ),
        "risk_buffer_frame": build_risk_buffer_frame(
            snapshot_by_key,
            EXCHANGE_TITLES,
            exchange_keys=perp_reference_exchange_keys,
            bybit_insurance_value=bybit_insurance_value,
        ),
    }
    if need_home_full:
        spot_reference_payload = get_stale_while_revalidate_result(
            f"spot-reference::{base_coin}::{exchange_scope_mode}",
            spot_reference_signature,
            ttl_seconds=balanced_ttl,
            stale_ttl_seconds=stale_ttl,
            builder=spot_reference_builder,
        ) or {}
        perp_reference_payload = get_stale_while_revalidate_result(
            f"perp-reference::{base_coin}::{exchange_scope_mode}",
            perp_reference_signature,
            ttl_seconds=balanced_ttl,
            stale_ttl_seconds=stale_ttl,
            builder=perp_reference_builder,
        ) or {}
    else:
        spot_reference_payload = {}
        perp_reference_payload = {}
        prefetch_stale_result(
            f"spot-reference::{base_coin}::{exchange_scope_mode}",
            spot_reference_signature,
            spot_reference_builder,
            max_age_seconds=prefetch_age,
        )
        prefetch_stale_result(
            f"perp-reference::{base_coin}::{exchange_scope_mode}",
            perp_reference_signature,
            perp_reference_builder,
            max_age_seconds=prefetch_age,
        )
    spot_flow_reference_frame = spot_reference_payload.get("flow_frame", pd.DataFrame())
    spot_execution_frame = spot_reference_payload.get("execution_frame", pd.DataFrame())
    perp_crowding_trio_frame = perp_reference_payload.get("crowding_frame", pd.DataFrame())
    funding_regime_frame = perp_reference_payload.get("funding_frame", pd.DataFrame())
    perp_execution_frame = perp_reference_payload.get("execution_frame", pd.DataFrame())
    risk_buffer_frame = perp_reference_payload.get("risk_buffer_frame", pd.DataFrame())
    share_baseline_key = chart_key("share-dynamics-baseline", base_coin, exchange_scope_mode)
    share_baseline_entry = st.session_state.get(share_baseline_key, {})
    share_baseline_records = share_baseline_entry.get("records") if isinstance(share_baseline_entry, dict) else None
    share_baseline_ts = int(share_baseline_entry.get("ts_ms") or 0) if isinstance(share_baseline_entry, dict) else 0
    share_dynamics_frame = (
        build_exchange_share_dynamics_frame(
            snapshot_by_key,
            spot_snapshot_map,
            EXCHANGE_TITLES,
            exchange_keys=perp_reference_exchange_keys,
            previous_records=share_baseline_records,
        )
        if need_home_full
        else pd.DataFrame()
    )
    share_baseline_label = format_share_baseline_age(share_baseline_ts if share_baseline_records else None, now_ms)
    if need_home_full and (not share_baseline_entry or now_ms - share_baseline_ts >= max(60_000, refresh_seconds * 15 * 1000)):
        st.session_state[share_baseline_key] = {
            "ts_ms": now_ms,
            "records": share_dynamics_frame[
                ["交易所", "OI份额(%)", "合约成交份额(%)", "现货成交份额(%)"]
            ].to_dict("records"),
        }
    hyperliquid_address_value = hyperliquid_address.strip().lower()
    hyperliquid_address_bundle: Dict[str, Any] = {}
    hyperliquid_user_stream = None
    if (
        (not hyperliquid_realtime_enabled)
        or (not hyperliquid_address_value)
        or (not is_valid_onchain_address(hyperliquid_address_value))
        or (active_view not in {"首页总览", "交易台深度页"})
    ):
        resolve_hyperliquid_user_stream("", symbol_map["hyperliquid"], request_timeout, int(hyperliquid_address_lookback_hours), restart_feed)
    if (
        hyperliquid_address_value
        and is_valid_onchain_address(hyperliquid_address_value)
        and active_view in {"首页总览", "交易台深度页"}
    ):
        base_hyperliquid_address_bundle = load_hyperliquid_address_mode_cached(
            hyperliquid_address_value,
            symbol_map["hyperliquid"],
            int(hyperliquid_address_lookback_hours),
            request_timeout,
        )
        if hyperliquid_realtime_enabled:
            hyperliquid_user_stream = resolve_hyperliquid_user_stream(
                hyperliquid_address_value,
                symbol_map["hyperliquid"],
                request_timeout,
                int(hyperliquid_address_lookback_hours),
                restart_feed,
            )
        live_hyperliquid_bundle = hyperliquid_user_stream.snapshot() if hyperliquid_user_stream is not None else None
        hyperliquid_address_bundle = merge_hyperliquid_address_bundle(base_hyperliquid_address_bundle, live_hyperliquid_bundle)
    hyperliquid_spot_meta_payload: Dict[str, Any] = {}
    hyperliquid_oi_cap_payload: Dict[str, Any] = {}
    hyperliquid_spot_perp_context_frame = pd.DataFrame()
    if active_view == "增强实验室" or (active_view == "首页总览" and overview_mode == "完整"):
        hyperliquid_spot_meta_payload = load_hyperliquid_spot_meta_and_asset_contexts_cached(request_timeout)
        hyperliquid_oi_cap_payload = load_hyperliquid_perps_at_oi_cap_cached(request_timeout)
        hyperliquid_spot_perp_context_frame = build_hyperliquid_spot_perp_context_frame(
            hyperliquid_spot_meta_payload,
            snapshot_by_key.get("hyperliquid"),
            selected_coin=symbol_map["hyperliquid"],
            address_bundle=hyperliquid_address_bundle if hyperliquid_address_bundle else None,
            oi_cap_payload=hyperliquid_oi_cap_payload,
        )
    compare_coins = tuple(dict.fromkeys(cross_coin_pool))[:6]
    multi_coin_signature = (compare_coins, int(request_timeout), int(liquidation_limit))
    if active_view != "增强实验室" and compare_coins:
        prefetch_stale_result(
            f"lab-multi-coin::{base_coin}",
            multi_coin_signature,
            builder=lambda: load_market_overview_frame_cached(compare_coins, request_timeout, liquidation_limit),
            max_age_seconds=prefetch_age,
        )
    watchlist_specs = build_hyperliquid_watchlist_specs(
        hyperliquid_address_value,
        hyperliquid_watch_labels,
        option_catalog=address_option_catalog,
    )
    if selected_watch_groups:
        watchlist_specs = [
            spec
            for spec in watchlist_specs
            if str(spec.get("group") or "默认") in selected_watch_groups
        ]
    watch_signature = (
        tuple(
            (
                str(spec.get("label") or ""),
                str(spec.get("address") or ""),
                str(spec.get("group") or ""),
                str(spec.get("source") or ""),
            )
            for spec in watchlist_specs
        ),
        symbol_map["hyperliquid"],
        int(hyperliquid_address_lookback_hours),
        int(request_timeout),
        str(hyperliquid_address_value or ""),
        int(hyperliquid_user_stream.last_snapshot_ms or 0) if hyperliquid_user_stream is not None else 0,
    )
    if active_view != "增强实验室" and watchlist_specs:
        prefetch_stale_result(
            f"lab-watchlist::{base_coin}",
            watch_signature,
            builder=lambda: load_hyperliquid_watchlist_bundles(
                watchlist_specs,
                coin=symbol_map["hyperliquid"],
                lookback_hours=int(hyperliquid_address_lookback_hours),
                timeout=request_timeout,
                current_address=hyperliquid_address_value,
                current_bundle=hyperliquid_address_bundle if hyperliquid_address_bundle else None,
            ),
            max_age_seconds=prefetch_age,
        )
    prefetched_history_summary: Dict[str, Any] = {}
    history_index_days_prefetch = max(1, min(int(archive_retention_days), 7))
    history_index_since_ms_prefetch = now_ms - history_index_days_prefetch * 24 * 60 * 60 * 1000
    if history_sqlite_enabled and active_view != "增强实验室":
        prefetched_history_summary = history_store.describe()
        prefetch_stale_result(
            f"lab-history-index::{base_coin}",
            (
                int(history_index_since_ms_prefetch),
                int(prefetched_history_summary.get("last_market_ts") or 0),
                int(prefetched_history_summary.get("last_alert_ts") or 0),
                int(prefetched_history_summary.get("last_event_ts") or 0),
                int(prefetched_history_summary.get("last_quality_ts") or 0),
            ),
            builder=lambda: build_history_index_payload(history_store, since_ms=history_index_since_ms_prefetch),
            max_age_seconds=max(prefetch_age, 90),
        )
    selected_liquidation_truth = build_liquidation_truth_summary(
        liquidation_events,
        now_ms=now_ms,
        window_minutes=liquidation_window_minutes,
        cluster_window_seconds=30,
    )
    selected_liquidation_clusters = build_liquidation_cluster_frame(liquidation_events, cluster_window_seconds=30, limit=14)
    if need_home_full:
        cross_exchange_cluster_frame = get_cached_derived_result(
            f"cross-liq-clusters::{base_coin}::{interval}",
            cross_liq_signature + (len(cross_exchange_session_liqs),),
            ttl_seconds=balanced_ttl,
            builder=lambda: build_liquidation_cluster_frame(cross_exchange_session_liqs, cluster_window_seconds=30, limit=16),
        )
    replay_events = sorted(perp_recorded_events + spot_recorded_events, key=lambda item: item.timestamp_ms)
    selected_lead_lag_summary = None
    if need_home_full and not lead_lag_frame.empty and "交易所" in lead_lag_frame.columns:
        selected_lead_row = lead_lag_frame[lead_lag_frame["交易所"] == EXCHANGE_TITLES.get(selected_exchange, selected_exchange.title())]
        if not selected_lead_row.empty:
            selected_lead_lag_summary = str(selected_lead_row.iloc[0].get("提示") or "")
    strongest_alerts = confirmed_alert_frame[confirmed_alert_frame["状态"] == "已确认"] if not confirmed_alert_frame.empty else confirmed_alert_frame
    home_conclusions = (
        build_market_conclusion_lines(base_coin, composite_signal, selected_lead_lag_summary, strongest_alerts, selected_liquidation_truth)
        if need_home_full
        else []
    )
    selected_snapshot_state = _build_state_caption(
        "快照 实时聚合" if selected_transport_health.get("live_timestamp_ms") is not None else "快照 REST回补",
        selected_transport_health.get("snapshot_timestamp_ms"),
    )
    selected_orderbook_state = _build_state_caption(
        f"盘口 {'WS实时' if _use_live_orderbook(live_orderbook, selected_snapshot.timestamp_ms) else 'REST回补'}",
        selected_transport_health.get("snapshot_timestamp_ms"),
        sync_state=str(selected_transport_health.get("sync_state") or ""),
        sample_count=len(orderbook),
        min_samples=24,
    )
    selected_trade_state = _build_state_caption(
        f"成交 {'WS实时' if _use_live_trades(live_trade_events) else 'REST回补'}",
        _latest_trade_timestamp(trade_events),
        sample_count=len(trade_events),
        min_samples=12,
    )
    selected_oi_state = _build_state_caption(
        f"OI {_oi_source_label(backfill_oi, session_oi_points)}",
        merged_oi[-1].timestamp_ms if merged_oi else None,
        sample_count=len(merged_oi),
        min_samples=8,
        confidence=_confidence_label(oi_quadrant.get("confidence")),
    )
    selected_risk_confidence = _confidence_label(
        _riskmap_confidence_score(selected_snapshot, candles, orderbook, str(selected_transport_health.get("sync_state") or ""))
    )
    selected_quality_confidence = _confidence_label(
        _orderbook_quality_confidence_score(perp_quality_history, str(selected_transport_health.get("sync_state") or ""))
    )
    selected_replay_confidence = _confidence_label(min(1.0, len(replay_events) / 36.0 if replay_events else 0.0))
    spot_perp_decision_frame = pd.DataFrame()
    propagation_spread_frame = pd.DataFrame()
    propagation_funding_arb_frame = pd.DataFrame()
    unified_signal_frame = pd.DataFrame()
    execution_route_frame = pd.DataFrame()
    propagation_frame = pd.DataFrame()
    if need_home_full:
        spot_perp_decision_frame = build_spot_perp_decision_frame(
            spot_exchange_frame,
            lead_lag_frame,
            contract_sentiment_frame,
            perp_crowding_trio_frame,
            funding_regime_frame,
        )
        propagation_snapshots = [snapshot_by_key[key] for key in perp_reference_exchange_keys if key in snapshot_by_key and snapshot_by_key[key].status == "ok"]
        propagation_spread_frame = build_cross_exchange_spread_frame(propagation_snapshots) if propagation_snapshots else pd.DataFrame()
        propagation_funding_arb_frame = build_funding_arb_frame(propagation_snapshots, limit=10) if propagation_snapshots else pd.DataFrame()
        unified_signal_frame = build_unified_signal_frame(
            spot_perp_decision_frame,
            contract_sentiment_alert_frame,
            confirmed_alert_frame,
            share_dynamics_frame,
            risk_buffer_frame,
        )
        execution_route_frame = build_execution_route_frame(
            spot_execution_frame,
            perp_execution_frame,
            spot_perp_decision_frame,
        )
        propagation_frame = build_cross_exchange_propagation_frame(
            spot_perp_decision_frame,
            share_dynamics_frame,
            propagation_spread_frame,
            propagation_funding_arb_frame,
            cross_exchange_cluster_frame,
        )
    if history_sqlite_enabled:
        schedule_history_write(
            f"events::{base_coin}::trade",
            lambda store=history_store, coin=base_coin, events={key: list(value) for key, value in trade_events_by_exchange.items() if value}: store.record_market_events(
                coin,
                events,
                category="trade",
                exchange_title_map=EXCHANGE_TITLES,
                market="perp",
            ),
        )
        schedule_history_write(
            f"events::{base_coin}::liquidation",
            lambda store=history_store, coin=base_coin, events={key: list(value) for key, value in liquidation_events_by_exchange.items() if value}: store.record_market_events(
                coin,
                events,
                category="liquidation",
                exchange_title_map=EXCHANGE_TITLES,
                market="perp",
            ),
        )
        schedule_history_write(
            f"quality::{base_coin}::{selected_exchange}::perp",
            lambda store=history_store, coin=base_coin, exchange_key=selected_exchange, exchange_name=selected_snapshot.exchange, symbol=selected_symbol, points=list(perp_quality_history[-48:]): store.record_quality_points(
                coin,
                exchange_key=exchange_key,
                exchange_name=exchange_name,
                symbol=symbol,
                points=points,
                market="perp",
            ),
        )
        if spot_symbol_map.get(selected_exchange) and spot_quality_history:
            schedule_history_write(
                f"quality::{base_coin}::{selected_exchange}::spot",
                lambda store=history_store, coin=base_coin, exchange_key=selected_exchange, exchange_name=f"{selected_snapshot.exchange} Spot", symbol=spot_symbol_map.get(selected_exchange, spot_symbol), points=list(spot_quality_history[-48:]): store.record_quality_points(
                    coin,
                    exchange_key=exchange_key,
                    exchange_name=exchange_name,
                    symbol=symbol,
                    points=points,
                    market="spot",
                ),
            )
    browser_notifications = route_alert_notifications(
        new_alert_notifications,
        channel="browser",
        min_level=browser_min_level,
        cooldown_minutes=int(alert_cooldown_minutes),
    )
    telegram_notifications = route_alert_notifications(
        new_alert_notifications,
        channel="telegram",
        min_level=telegram_min_level,
        cooldown_minutes=int(alert_cooldown_minutes),
    )
    if browser_notifications and browser_notify_enabled:
        emit_browser_notifications(browser_notifications, enable_sound=browser_sound_enabled)
    telegram_errors: List[str] = []
    if telegram_notifications and telegram_push_enabled and telegram_bot_token.strip() and telegram_chat_id.strip():
        telegram_errors = emit_telegram_notifications(
            telegram_notifications,
            token=telegram_bot_token.strip(),
            chat_id=telegram_chat_id.strip(),
            timeout=request_timeout,
        )
    if active_view == "增强实验室":
        lab_snapshot_keys = scope_perp_keys if exchange_scope_mode == "当前交易所优先" else list(EXCHANGE_ORDER)
        lab_need_hyper = lab_mode in ("总览", "Hyperliquid")
        lab_need_agg = lab_mode in ("总览", "跨所聚合")
        lab_need_strategy = lab_mode in ("总览", "策略层")
        lab_need_cross_coin = lab_mode in ("总览", "跨币种联动")
        lab_need_persist = lab_mode in ("总览", "通知与持久化")
        lab_need_review = lab_need_strategy or lab_need_persist
        lab_need_market_history = lab_need_review or lab_need_agg

        lab_snapshots: List[ExchangeSnapshot] = []
        predicted_funding_frame = pd.DataFrame()
        all_mids: Dict[str, str] = {}
        watchlist_bundles: List[Dict[str, Any]] = []
        watchlist_leaderboard_frame = pd.DataFrame()
        watch_group_frame = pd.DataFrame()
        watch_behavior_frame = pd.DataFrame()
        watch_risk_frame = pd.DataFrame()
        liquidation_density_frame = pd.DataFrame()
        vault_watch_frame = pd.DataFrame()
        chain_trade_frame = pd.DataFrame()
        spread_frame = pd.DataFrame()
        funding_arb_frame = pd.DataFrame()
        share_frame = pd.DataFrame()
        oi_weighted_funding_bps = None
        max_real_time_spread_bps = None
        max_funding_gap_bps = None
        aggregate_large_trade_frame = pd.DataFrame()
        multifactor_frame = pd.DataFrame()
        wall_absorption_frame = pd.DataFrame()
        vpin_frame = pd.DataFrame()
        microstructure_frame = pd.DataFrame()
        pattern_frame = pd.DataFrame()
        backtest_frame = pd.DataFrame()
        multi_coin_frame = pd.DataFrame()
        cross_coin_linkage_frame = pd.DataFrame()
        history_summary = (
            prefetched_history_summary
            if prefetched_history_summary
            else history_store.describe()
            if history_sqlite_enabled and (lab_need_review or lab_need_persist or lab_need_agg)
            else {}
        )
        review_since_ms = now_ms - max(int(alert_review_window_minutes), 60) * 20 * 60_000
        alert_history_frame = pd.DataFrame()
        market_history_frame = pd.DataFrame()
        event_history_frame = pd.DataFrame()
        quality_history_frame = pd.DataFrame()
        alert_review_frame = pd.DataFrame()
        alert_review_summary_frame = pd.DataFrame()
        alert_window_matrix_frame = pd.DataFrame()
        alert_best_window_frame = pd.DataFrame()
        event_summary_frame = pd.DataFrame()
        quality_summary_frame = pd.DataFrame()
        history_index_days = history_index_days_prefetch
        history_index_since_ms = history_index_since_ms_prefetch
        alert_catalog_index_frame = pd.DataFrame()
        market_coverage_index_frame = pd.DataFrame()
        event_catalog_index_frame = pd.DataFrame()
        exchange_share_delta_frame = pd.DataFrame()

        if lab_need_hyper or lab_need_agg:
            lab_snapshots = [snapshot_by_key[key] for key in lab_snapshot_keys if key in snapshot_by_key and snapshot_by_key[key].status == "ok"]

        if lab_need_hyper:
            predicted_funding_payload = load_hyperliquid_predicted_fundings_cached(request_timeout)
            predicted_funding_frame = build_hyperliquid_predicted_funding_frame(
                predicted_funding_payload,
                selected_coin=symbol_map["hyperliquid"],
            )
            all_mids = load_hyperliquid_all_mids_cached(request_timeout)
            watchlist_bundles = get_stale_while_revalidate_result(
                f"lab-watchlist::{base_coin}",
                watch_signature,
                ttl_seconds=heavy_ttl,
                stale_ttl_seconds=stale_ttl,
                builder=lambda: load_hyperliquid_watchlist_bundles(
                    watchlist_specs,
                    coin=symbol_map["hyperliquid"],
                    lookback_hours=int(hyperliquid_address_lookback_hours),
                    timeout=request_timeout,
                    current_address=hyperliquid_address_value,
                    current_bundle=hyperliquid_address_bundle if hyperliquid_address_bundle else None,
                ),
            ) or []
            watchlist_leaderboard_frame = build_hyperliquid_watchlist_leaderboard_frame(watchlist_bundles)
            watch_group_frame = build_hyperliquid_watch_group_frame(watchlist_bundles)
            watch_behavior_frame = build_hyperliquid_watch_behavior_frame(
                watchlist_bundles,
                predicted_funding_frame,
                current_coin=symbol_map["hyperliquid"],
            )
            watch_risk_frame = build_hyperliquid_watch_risk_frame(watch_behavior_frame)
            watchlist_positions = build_hyperliquid_watchlist_position_rows(watchlist_bundles)
            liquidation_density_frame = build_liquidation_density_frame(
                watchlist_positions,
                all_mids,
                selected_coin=symbol_map["hyperliquid"],
                window_pct=max(6.0, risk_heat_window_pct),
                bucket_count=max(12, risk_heat_buckets),
            )
            vault_watch_rows: List[Dict[str, Any]] = []
            for bundle in watchlist_bundles:
                detail_frame = build_hyperliquid_vault_detail_frame(bundle)
                if detail_frame.empty:
                    continue
                detail_frame = detail_frame.copy()
                detail_frame.insert(0, "标签", bundle.get("label"))
                detail_frame.insert(1, "地址", bundle.get("address"))
                detail_frame.insert(2, "分组", bundle.get("group"))
                vault_watch_rows.extend(detail_frame.to_dict("records"))
            vault_watch_frame = pd.DataFrame(vault_watch_rows)
            hyperliquid_live_trades = service.get_trade_history("hyperliquid")
            hyperliquid_public_trades = (
                hyperliquid_live_trades
                if _use_live_trades(hyperliquid_live_trades)
                else load_exchange_trades_cached("hyperliquid", symbol_map["hyperliquid"], 160, request_timeout)
            )
            chain_trade_frame = build_large_trade_frame(
                {"Hyperliquid": hyperliquid_public_trades},
                min_notional=max(25_000.0, float(selected_snapshot.last_price or 0.0) * 0.6),
                limit=36,
            )

        if lab_need_agg:
            agg_payload = get_stale_while_revalidate_result(
                f"lab-agg::{base_coin}",
                (
                    tuple(
                        (
                            key,
                            int(snapshot_by_key[key].timestamp_ms or 0) if key in snapshot_by_key else 0,
                            len(perp_trades_map.get(key, [])),
                            perp_trades_map.get(key)[-1].timestamp_ms if perp_trades_map.get(key) else 0,
                        )
                        for key in lab_snapshot_keys
                    ),
                    len(cross_exchange_session_liqs),
                    cross_exchange_session_liqs[-1].timestamp_ms if cross_exchange_session_liqs else 0,
                ),
                ttl_seconds=balanced_ttl,
                stale_ttl_seconds=stale_ttl,
                builder=lambda: {
                    "spread_frame": build_cross_exchange_spread_frame(lab_snapshots),
                    "funding_arb_frame": build_funding_arb_frame(lab_snapshots),
                    "share_frame": build_exchange_share_frame(lab_snapshots),
                    "aggregate_large_trade_frame": build_large_trade_frame(
                        {EXCHANGE_TITLES[key]: perp_trades_map.get(key, []) for key in lab_snapshot_keys},
                        min_notional=max(75_000.0, float(selected_snapshot.last_price or 0.0) * 1.5),
                        limit=60,
                    ),
                },
            ) or {}
            spread_frame = agg_payload.get("spread_frame", pd.DataFrame())
            funding_arb_frame = agg_payload.get("funding_arb_frame", pd.DataFrame())
            share_frame = agg_payload.get("share_frame", pd.DataFrame())
            aggregate_large_trade_frame = agg_payload.get("aggregate_large_trade_frame", pd.DataFrame())
            oi_weighted_funding_bps = (
                float((share_frame["OI金额"].fillna(0.0) * share_frame["Funding(bps)"].fillna(0.0)).sum()) / float(share_frame["OI金额"].fillna(0.0).sum())
                if not share_frame.empty and float(share_frame["OI金额"].fillna(0.0).sum()) > 0
                else None
            )
            max_real_time_spread_bps = (
                float(spread_frame["偏离中位(bps)"].max() - spread_frame["偏离中位(bps)"].min())
                if not spread_frame.empty and "偏离中位(bps)" in spread_frame.columns and spread_frame["偏离中位(bps)"].notna().any()
                else None
            )
            max_funding_gap_bps = funding_arb_frame["费率差(bps)"].max() if not funding_arb_frame.empty else None

        if lab_need_cross_coin or lab_need_agg:
            multi_coin_frame = get_stale_while_revalidate_result(
                f"lab-multi-coin::{base_coin}",
                multi_coin_signature,
                ttl_seconds=max(60, heavy_ttl),
                stale_ttl_seconds=max(600, stale_ttl),
                builder=lambda: load_market_overview_frame_cached(compare_coins, request_timeout, liquidation_limit),
            )
            if multi_coin_frame is None:
                multi_coin_frame = pd.DataFrame()
            cross_coin_linkage_frame = build_cross_coin_linkage_frame(multi_coin_frame, base_coin=base_coin)

        if lab_need_strategy:
            strategy_payload = get_stale_while_revalidate_result(
                f"lab-strategy::{base_coin}",
                (
                    tuple((key, int(snapshot_by_key[key].timestamp_ms or 0)) for key in lab_snapshot_keys if key in snapshot_by_key),
                    len(perp_quality_history),
                    perp_quality_history[-1].timestamp_ms if perp_quality_history else 0,
                    len(trade_events),
                    trade_events[-1].timestamp_ms if trade_events else 0,
                    len(candles),
                    candles[-1].timestamp_ms if candles else 0,
                    float(crowd_position_ratio or 0.0),
                    float(crowd_account_ratio or 0.0),
                    float(global_ratio or 0.0),
                ),
                ttl_seconds=balanced_ttl,
                stale_ttl_seconds=stale_ttl,
                builder=lambda: {
                    "multifactor_frame": build_multifactor_sentiment_frame(
                        {key: snapshot_by_key[key] for key in lab_snapshot_keys if key in snapshot_by_key},
                        oi_metrics_by_exchange,
                        trade_metrics_by_exchange,
                        crowd_position_ratio=crowd_position_ratio,
                        crowd_account_ratio=crowd_account_ratio,
                        global_ratio=global_ratio,
                    ),
                    "wall_absorption_frame": build_wall_absorption_frame(perp_quality_history, trade_metrics),
                    "vpin_frame": build_vpin_frame(trade_events, bucket_count=24),
                    "microstructure_frame": build_microstructure_anomaly_frame(book_summary, perp_quality_history, trade_metrics),
                    "pattern_frame": build_candlestick_pattern_frame(candles, limit=12),
                    "backtest_frame": build_signal_backtest_frame(candles, horizon_bars=max(3, min(12, candle_limit // 40))),
                },
            ) or {}
            multifactor_frame = strategy_payload.get("multifactor_frame", pd.DataFrame())
            wall_absorption_frame = strategy_payload.get("wall_absorption_frame", pd.DataFrame())
            vpin_frame = strategy_payload.get("vpin_frame", pd.DataFrame())
            microstructure_frame = strategy_payload.get("microstructure_frame", pd.DataFrame())
            pattern_frame = strategy_payload.get("pattern_frame", pd.DataFrame())
            backtest_frame = strategy_payload.get("backtest_frame", pd.DataFrame())

        if history_sqlite_enabled and lab_need_market_history:
            history_payload = get_stale_while_revalidate_result(
                f"lab-history-review::{base_coin}",
                (
                    tuple(lab_snapshot_keys),
                    int(review_since_ms),
                    int(history_summary.get("last_market_ts") or 0),
                    int(history_summary.get("last_alert_ts") or 0),
                    int(history_summary.get("last_event_ts") or 0),
                    int(history_summary.get("last_quality_ts") or 0),
                ),
                ttl_seconds=max(12, heavy_ttl),
                stale_ttl_seconds=max(stale_ttl, 150),
                builder=lambda: build_history_review_payload(
                    history_store,
                    coin=base_coin,
                    exchange_keys=lab_snapshot_keys,
                    since_ms=review_since_ms,
                ),
            ) or {}
            alert_history_frame = history_payload.get("alert_history_frame", pd.DataFrame())
            market_history_frame = history_payload.get("market_history_frame", pd.DataFrame())
            if not market_history_frame.empty and "市场" not in market_history_frame.columns:
                market_history_frame.insert(4, "市场", "perp")
            event_history_frame = history_payload.get("event_history_frame", pd.DataFrame())
            quality_history_frame = history_payload.get("quality_history_frame", pd.DataFrame())

        if lab_need_review:
            alert_review_frame = build_alert_review_frame(
                alert_history_frame,
                market_history_frame,
                review_window_minutes=int(alert_review_window_minutes),
            )
            alert_review_summary_frame = build_alert_review_summary_frame(alert_review_frame)
            alert_window_matrix_frame = build_alert_review_window_matrix(
                alert_history_frame,
                market_history_frame,
                review_windows=[
                    max(10, int(alert_review_window_minutes) // 2),
                    int(alert_review_window_minutes),
                    max(30, int(alert_review_window_minutes) * 2),
                ],
            )
            alert_best_window_frame = build_alert_best_window_frame(alert_window_matrix_frame)
            event_summary_frame = build_history_event_summary_frame(event_history_frame)
            quality_summary_frame = build_quality_summary_frame(quality_history_frame)

        if history_sqlite_enabled and lab_need_persist:
            index_signature = (
                int(history_index_since_ms),
                int(history_summary.get("last_market_ts") or 0),
                int(history_summary.get("last_alert_ts") or 0),
                int(history_summary.get("last_event_ts") or 0),
                int(history_summary.get("last_quality_ts") or 0),
            )
            index_payload = get_stale_while_revalidate_result(
                f"lab-history-index::{base_coin}",
                index_signature,
                ttl_seconds=max(20, heavy_ttl),
                stale_ttl_seconds=max(180, stale_ttl),
                builder=lambda: build_history_index_payload(history_store, since_ms=history_index_since_ms),
            ) or {}
            history_index_alert_frame = index_payload.get("alert_frame", pd.DataFrame())
            history_index_market_frame = index_payload.get("market_frame", pd.DataFrame())
            history_index_event_frame = index_payload.get("event_frame", pd.DataFrame())
            history_index_quality_frame = index_payload.get("quality_frame", pd.DataFrame())
            alert_catalog_index_frame = build_alert_catalog_index_frame(history_index_alert_frame)
            market_coverage_index_frame = build_market_coverage_index_frame(
                history_index_market_frame,
                history_index_event_frame,
                history_index_quality_frame,
            )
            event_catalog_index_frame = build_event_catalog_index_frame(history_index_event_frame)

        exchange_share_delta_frame = share_frame.copy()
        if history_sqlite_enabled and not market_history_frame.empty and not exchange_share_delta_frame.empty:
            sorted_history = market_history_frame.sort_values("时间")
            if "交易所键" in sorted_history.columns and "OI金额" in sorted_history.columns:
                earliest = sorted_history.groupby("交易所键", as_index=False).first()
                start_total_oi = float(earliest["OI金额"].fillna(0.0).sum())
                share_delta_map = {}
                for _, row in earliest.iterrows():
                    if start_total_oi <= 0:
                        break
                    share_delta_map[str(row["交易所键"])] = float(row["OI金额"] or 0.0) / start_total_oi * 100.0
                exchange_key_from_name = {snapshot.exchange: key for key, snapshot in snapshot_by_key.items()}
                exchange_share_delta_frame["OI份额变化(%)"] = [
                    payload_float(current) - share_delta_map.get(exchange_key_from_name.get(str(exchange_name), ""), 0.0)
                    if payload_float(current) is not None and exchange_key_from_name.get(str(exchange_name), "") in share_delta_map
                    else None
                    for exchange_name, current in zip(exchange_share_delta_frame["交易所"], exchange_share_delta_frame["OI份额(%)"])
                ]
        render_section("增强实验室", "把 Hyperliquid 独有 API、跨所聚合、策略层与通知/持久化集中在一页里，尽量不改动你原来的首页和深度页主逻辑。")
        if exchange_scope_mode == "当前交易所优先":
            st.info("当前是 `当前交易所优先` 模式。增强实验室里的跨所价差、份额和套利面板会跟着收敛；想看完整 4 所聚合，请切到 `全部交易所`。")
        lab_top = st.columns(6)
        lab_top[0].metric("实验室模式", lab_mode)
        lab_top[1].metric("公开观察地址", str(len(watchlist_bundles)))
        lab_top[2].metric("OI加权费率", fmt_bps(oi_weighted_funding_bps))
        lab_top[3].metric("最大跨所价差", fmt_bps(max_real_time_spread_bps))
        lab_top[4].metric("最大费率差", fmt_bps(max_funding_gap_bps))
        lab_top[5].metric("历史库", "启用" if history_sqlite_enabled else "关闭")

        if lab_mode in ("总览", "Hyperliquid"):
            render_section("Hyperliquid 独有 API", "鲸鱼账户追踪、predictedFundings、Vault 监控、链上逐笔和公开清算密度都放在这一层。")
            hyper_row_left, hyper_row_right = st.columns([1.45, 1.15], gap="large")
            with hyper_row_left:
                st.plotly_chart(
                    build_hyperliquid_predicted_funding_figure(predicted_funding_frame),
                    key=chart_key("lab-hl-predicted-funding", base_coin, interval, lab_mode),
                    config=PLOTLY_CONFIG,
                )
            with hyper_row_right:
                if predicted_funding_frame.empty:
                    st.info("当前没有 predictedFundings 样本。")
                else:
                    st.dataframe(
                        predicted_funding_frame,
                        width="stretch",
                        hide_index=True,
                        column_config={
                            "预测费率(bps)": st.column_config.NumberColumn(format="%.3f"),
                            "8h等价费率(bps)": st.column_config.NumberColumn(format="%.3f"),
                            "年化费率(%)": st.column_config.NumberColumn(format="%.2f"),
                            "结算间隔(h)": st.column_config.NumberColumn(format="%.1f"),
                        },
                    )
            render_section("Hyperliquid Spot / Perp 一体层", "把链上现货、永续、地址余额和 OI 上限占用放在同一层，避免只看单腿误判。")
            hyper_context_display_frame = hyperliquid_spot_perp_context_frame.copy()
            if not hyper_context_display_frame.empty and "时间" in hyper_context_display_frame.columns:
                hyper_context_display_frame["时间"] = [format_display_timestamp_ms(value) for value in hyper_context_display_frame["时间"]]
            hyper_context_cards = st.columns(4)
            hyper_context_cards[0].metric("现货腿数", str(int(hyperliquid_spot_perp_context_frame["市场"].astype(str).eq("现货").sum()) if not hyperliquid_spot_perp_context_frame.empty else 0))
            hyper_context_cards[1].metric("合约腿数", str(int(hyperliquid_spot_perp_context_frame["市场"].astype(str).eq("合约").sum()) if not hyperliquid_spot_perp_context_frame.empty else 0))
            hyper_context_cards[2].metric(
                "地址现货余额",
                fmt_compact(
                    payload_float(
                        hyperliquid_spot_perp_context_frame.loc[
                            hyperliquid_spot_perp_context_frame["市场"].astype(str).eq("现货"),
                            "地址余额",
                        ].dropna().sum()
                    )
                    if not hyperliquid_spot_perp_context_frame.empty
                    else None
                ),
            )
            hyper_context_cards[3].metric(
                "OI 上限占用",
                fmt_pct(
                    hyperliquid_spot_perp_context_frame.loc[
                        hyperliquid_spot_perp_context_frame["市场"].astype(str).eq("合约"),
                        "OI上限占用(%)",
                    ].max()
                    if not hyperliquid_spot_perp_context_frame.empty
                    else None
                ),
            )
            hyper_context_left, hyper_context_right = st.columns([1.2, 1.05], gap="large")
            with hyper_context_left:
                st.plotly_chart(
                    build_hyperliquid_spot_perp_context_figure(hyperliquid_spot_perp_context_frame),
                    key=chart_key("lab-hl-spot-perp-context", base_coin, interval, lab_mode),
                    config=PLOTLY_CONFIG,
                )
            with hyper_context_right:
                if hyper_context_display_frame.empty:
                    st.info("当前还没有足够的 Hyperliquid spot / perp 一体样本。")
                else:
                    st.dataframe(
                        hyper_context_display_frame,
                        width="stretch",
                        hide_index=True,
                        column_config={
                            "价格": st.column_config.NumberColumn(format="%.4f"),
                            "24h成交额": st.column_config.NumberColumn(format="%.2f"),
                            "Funding(bps)": st.column_config.NumberColumn(format="%.2f"),
                            "Premium(%)": st.column_config.NumberColumn(format="%.3f"),
                            "未平仓金额": st.column_config.NumberColumn(format="%.2f"),
                            "地址余额": st.column_config.NumberColumn(format="%.4f"),
                            "地址成本": st.column_config.NumberColumn(format="%.2f"),
                            "地址持仓值": st.column_config.NumberColumn(format="%.2f"),
                            "OI上限占用(%)": st.column_config.NumberColumn(format="%.2f"),
                            "OI上限": st.column_config.NumberColumn(format="%.2f"),
                            "全局OI上限": st.column_config.NumberColumn(format="%.2f"),
                        },
                    )
            watch_left, watch_right = st.columns([1.15, 1.25], gap="large")
            with watch_left:
                if watchlist_leaderboard_frame.empty:
                    st.info("当前没有可展示的公开观察地址。")
                else:
                    st.dataframe(
                        watchlist_leaderboard_frame,
                        width="stretch",
                        hide_index=True,
                        column_config={
                            "账户权益": st.column_config.NumberColumn(format="%.2f"),
                            "24hPnL": st.column_config.NumberColumn(format="%.2f"),
                            "成交量": st.column_config.NumberColumn(format="%.2f"),
                            "持仓价值": st.column_config.NumberColumn(format="%.2f"),
                            "Funding净额": st.column_config.NumberColumn(format="%.2f"),
                        },
                    )
                    st.caption("这里先做成 `公开观察地址榜`。它使用当前地址 + 官方公开示例地址，不冒充全网官方 leaderboard。")
            with watch_right:
                st.plotly_chart(
                    build_liquidation_density_figure(
                        liquidation_density_frame,
                        payload_float(all_mids.get(symbol_map["hyperliquid"])) if all_mids else selected_snapshot.last_price,
                    ),
                    key=chart_key("lab-hl-liq-density", base_coin, interval, lab_mode),
                    config=PLOTLY_CONFIG,
                )
            group_left, group_right = st.columns([1.0, 1.4], gap="large")
            with group_left:
                if watch_group_frame.empty:
                    st.info("当前筛选分组下没有可聚合的观察地址。")
                else:
                    st.dataframe(
                        watch_group_frame,
                        width="stretch",
                        hide_index=True,
                        column_config={
                            "地址数": st.column_config.NumberColumn(format="%d"),
                            "账户权益": st.column_config.NumberColumn(format="%.2f"),
                            "持仓价值": st.column_config.NumberColumn(format="%.2f"),
                            "Funding净额": st.column_config.NumberColumn(format="%.2f"),
                        },
                    )
                    st.caption("地址池现在支持 `标签 / 分组 / 来源`，这一栏方便你按鲸鱼、Vault、做市地址去看聚合暴露。")
            with group_right:
                if vault_watch_frame.empty:
                    st.info("当前观察地址里没有可展示的 Vault 明细。")
                else:
                    st.dataframe(
                        vault_watch_frame,
                        width="stretch",
                        hide_index=True,
                    )
            behavior_left, behavior_right = st.columns([1.2, 1.0], gap="large")
            with behavior_left:
                if watch_behavior_frame.empty:
                    st.info("当前还没有足够的地址行为样本。")
                else:
                    behavior_display_frame = watch_behavior_frame.copy()
                    if "最新成交时间" in behavior_display_frame.columns:
                        behavior_display_frame["最新成交时间"] = [
                            "-" if pd.isna(value) else pd.to_datetime(value).strftime("%m-%d %H:%M:%S")
                            for value in behavior_display_frame["最新成交时间"]
                        ]
                    st.dataframe(
                        behavior_display_frame,
                        width="stretch",
                        hide_index=True,
                        column_config={
                            "账户权益": st.column_config.NumberColumn(format="%.2f"),
                            "当前币种持仓值": st.column_config.NumberColumn(format="%.2f"),
                            "总持仓值": st.column_config.NumberColumn(format="%.2f"),
                            "Funding净额": st.column_config.NumberColumn(format="%.2f"),
                            "最新成交额": st.column_config.NumberColumn(format="%.2f"),
                            "最近Funding": st.column_config.NumberColumn(format="%.2f"),
                            "预测费率(bps)": st.column_config.NumberColumn(format="%.3f"),
                            "最近清算距离(%)": st.column_config.NumberColumn(format="%.2f"),
                        },
                    )
            with behavior_right:
                if watch_risk_frame.empty:
                    st.info("当前分组下还没有足够的风险聚合样本。")
                else:
                    st.dataframe(
                        watch_risk_frame,
                        width="stretch",
                        hide_index=True,
                        column_config={
                            "地址数": st.column_config.NumberColumn(format="%d"),
                            "在线率(%)": st.column_config.NumberColumn(format="%.1f"),
                            "账户权益": st.column_config.NumberColumn(format="%.2f"),
                            "当前币种持仓值": st.column_config.NumberColumn(format="%.2f"),
                            "总持仓值": st.column_config.NumberColumn(format="%.2f"),
                            "Funding净额": st.column_config.NumberColumn(format="%.2f"),
                            "预测费率(bps)": st.column_config.NumberColumn(format="%.3f"),
                            "最近清算距离(%)": st.column_config.NumberColumn(format="%.2f"),
                        },
                    )
            chain_left, chain_right = st.columns([1.25, 1.15], gap="large")
            with chain_left:
                st.plotly_chart(
                    build_large_trade_figure(chain_trade_frame),
                    key=chart_key("lab-hl-chain-trades", base_coin, interval, lab_mode),
                    config=PLOTLY_CONFIG,
                )
            with chain_right:
                if chain_trade_frame.empty:
                    st.info("当前会话里还没有足够的 Hyperliquid 链上大单。")
                else:
                    st.dataframe(
                        chain_trade_frame.drop(columns=["侧向"], errors="ignore"),
                        width="stretch",
                        hide_index=True,
                        column_config={
                            "价格": st.column_config.NumberColumn(format="%.4f"),
                            "数量": st.column_config.NumberColumn(format="%.4f"),
                            "名义金额": st.column_config.NumberColumn(format="%.2f"),
                        },
                    )
            if hyperliquid_address_bundle:
                render_hyperliquid_address_panel(
                    hyperliquid_address_bundle,
                    address=hyperliquid_address_value,
                    lookback_hours=int(hyperliquid_address_lookback_hours),
                    current_coin=symbol_map["hyperliquid"],
                    key_scope=chart_key("lab-hyper-address", base_coin, interval),
                )

        if lab_mode in ("总览", "跨所聚合"):
            render_section("多交易所聚合增强", "价差、OI 权重、资金费率套利、聚合爆仓、大单流和份额动态统一收在这一层。")
            agg_row = st.columns(5)
            agg_row[0].metric("跨所实时价差", fmt_bps(max_real_time_spread_bps))
            agg_row[1].metric("聚合 OI", fmt_compact(share_frame["OI金额"].sum() if not share_frame.empty else None))
            agg_row[2].metric("聚合爆仓事件", str(len(cross_exchange_session_liqs)))
            agg_row[3].metric("大单笔数", str(len(aggregate_large_trade_frame)))
            agg_row[4].metric("多币种样本", str(len(multi_coin_frame)))
            spread_left, spread_right = st.columns([1.2, 1.2], gap="large")
            with spread_left:
                st.plotly_chart(
                    build_cross_exchange_spread_figure(spread_frame),
                    key=chart_key("lab-spread", base_coin, interval, lab_mode),
                    config=PLOTLY_CONFIG,
                )
            with spread_right:
                st.plotly_chart(
                    build_funding_arb_figure(funding_arb_frame),
                    key=chart_key("lab-funding-arb", base_coin, interval, lab_mode),
                    config=PLOTLY_CONFIG,
                )
            share_left, share_right = st.columns([1.1, 1.3], gap="large")
            with share_left:
                st.plotly_chart(
                    build_exchange_share_figure(share_frame),
                    key=chart_key("lab-share-dynamics", base_coin, interval, lab_mode),
                    config=PLOTLY_CONFIG,
                )
            with share_right:
                if exchange_share_delta_frame.empty:
                    st.info("当前没有可展示的交易所份额样本。")
                else:
                    st.dataframe(
                        exchange_share_delta_frame,
                        width="stretch",
                        hide_index=True,
                        column_config={
                            "价格": st.column_config.NumberColumn(format="%.2f"),
                            "OI金额": st.column_config.NumberColumn(format="%.2f"),
                            "OI份额(%)": st.column_config.ProgressColumn(format="%.2f", min_value=0.0, max_value=100.0),
                            "24h成交额": st.column_config.NumberColumn(format="%.2f"),
                            "成交份额(%)": st.column_config.ProgressColumn(format="%.2f", min_value=0.0, max_value=100.0),
                            "Funding(bps)": st.column_config.NumberColumn(format="%.2f"),
                            "OI份额变化(%)": st.column_config.NumberColumn(format="%.2f"),
                        },
                    )
            liq_left, liq_right = st.columns([1.2, 1.2], gap="large")
            with liq_left:
                st.plotly_chart(
                    build_liquidation_waterfall_figure(cross_exchange_session_liqs, now_ms, 120, 5),
                    key=chart_key("lab-agg-liq-waterfall", base_coin, interval, lab_mode),
                    config=PLOTLY_CONFIG,
                )
            with liq_right:
                if aggregate_large_trade_frame.empty:
                    st.info("当前还没有足够的跨所大单聚合样本。")
                else:
                    st.plotly_chart(
                        build_large_trade_figure(aggregate_large_trade_frame),
                        key=chart_key("lab-agg-whales", base_coin, interval, lab_mode),
                        config=PLOTLY_CONFIG,
                    )
            stream_left, stream_right = st.columns([1.1, 1.3], gap="large")
            with stream_left:
                st.dataframe(
                    build_liquidation_frame(cross_exchange_session_liqs, limit=24),
                    width="stretch",
                    hide_index=True,
                )
            with stream_right:
                st.dataframe(
                    aggregate_large_trade_frame.drop(columns=["侧向"], errors="ignore"),
                    width="stretch",
                    hide_index=True,
                    column_config={
                        "价格": st.column_config.NumberColumn(format="%.4f"),
                        "数量": st.column_config.NumberColumn(format="%.4f"),
                        "名义金额": st.column_config.NumberColumn(format="%.2f"),
                    },
                )
            st.dataframe(
                multi_coin_frame,
                width="stretch",
                hide_index=True,
                column_config={
                    "价格": st.column_config.NumberColumn(format="%.2f"),
                    "OI": st.column_config.NumberColumn(format="%.2f"),
                    "OI 1h(%)": st.column_config.NumberColumn(format="%.2f"),
                    "Funding(bps)": st.column_config.NumberColumn(format="%.2f"),
                    "24h爆仓样本额": st.column_config.NumberColumn(format="%.2f"),
                    "置信度": st.column_config.ProgressColumn(format="%.1f", min_value=0.0, max_value=100.0),
                },
            )

        if lab_mode in ("总览", "跨币种联动"):
            render_section("跨币种联动板", "把 BTC / ETH / SOL 等联动币种放进同一张热力图和定位图里，优先看谁先扩 OI、谁先拉 Funding、谁先进入爆仓传导。")
            cross_coin_valid_frame = cross_coin_linkage_frame if not cross_coin_linkage_frame.empty else multi_coin_frame
            oi_leader = (
                cross_coin_valid_frame.sort_values("OI 1h(%)", ascending=False, na_position="last").iloc[0]
                if not cross_coin_valid_frame.empty and "OI 1h(%)" in cross_coin_valid_frame.columns
                else {}
            )
            funding_leader = (
                cross_coin_valid_frame.reindex(
                    cross_coin_valid_frame["Funding(bps)"].abs().sort_values(ascending=False, na_position="last").index
                ).iloc[0]
                if not cross_coin_valid_frame.empty and "Funding(bps)" in cross_coin_valid_frame.columns
                else {}
            )
            liquidation_leader = (
                cross_coin_valid_frame.sort_values("24h爆仓样本额", ascending=False, na_position="last").iloc[0]
                if not cross_coin_valid_frame.empty and "24h爆仓样本额" in cross_coin_valid_frame.columns
                else {}
            )
            cross_coin_row = st.columns(5)
            cross_coin_row[0].metric("基准币种", base_coin)
            cross_coin_row[1].metric(
                "OI领动",
                str(oi_leader.get("币种") or "-"),
                fmt_pct(oi_leader.get("OI 1h(%)")) if isinstance(oi_leader, pd.Series) else None,
            )
            cross_coin_row[2].metric(
                "Funding极值",
                str(funding_leader.get("币种") or "-"),
                fmt_bps(funding_leader.get("Funding(bps)")) if isinstance(funding_leader, pd.Series) else None,
            )
            cross_coin_row[3].metric(
                "爆仓压力",
                str(liquidation_leader.get("币种") or "-"),
                fmt_compact(liquidation_leader.get("24h爆仓样本额")) if isinstance(liquidation_leader, pd.Series) else None,
            )
            cross_coin_row[4].metric("联动币种数", str(len(cross_coin_valid_frame)))
            linkage_left, linkage_right = st.columns([1.15, 1.2], gap="large")
            with linkage_left:
                st.plotly_chart(
                    build_cross_coin_linkage_heatmap_figure(cross_coin_valid_frame),
                    key=chart_key("lab-cross-coin-heatmap", base_coin, interval, lab_mode),
                    config=PLOTLY_CONFIG,
                )
            with linkage_right:
                st.plotly_chart(
                    build_cross_coin_positioning_figure(cross_coin_valid_frame),
                    key=chart_key("lab-cross-coin-positioning", base_coin, interval, lab_mode),
                    config=PLOTLY_CONFIG,
                )
            st.dataframe(
                cross_coin_linkage_frame,
                width="stretch",
                hide_index=True,
                column_config={
                    "价格": st.column_config.NumberColumn(format="%.2f"),
                    "OI 1h(%)": st.column_config.NumberColumn(format="%.2f"),
                    "Funding(bps)": st.column_config.NumberColumn(format="%.2f"),
                    "24h爆仓样本额": st.column_config.NumberColumn(format="%.2f"),
                    "置信度": st.column_config.ProgressColumn(format="%.1f", min_value=0.0, max_value=100.0),
                    "Funding偏离(bps)": st.column_config.NumberColumn(format="%.2f"),
                    "OI动量偏离(%)": st.column_config.NumberColumn(format="%.2f"),
                },
            )

        if lab_mode in ("总览", "策略层"):
            render_section("信号增强 · 策略层", "多因子情绪、墙体消失 + 吸收、VPIN、微结构异常、K 线形态和轻量回测都在这里。")
            strategy_row = st.columns(5)
            strategy_row[0].metric("当前合成信号", str(composite_signal.get("label") or "-"))
            strategy_row[1].metric("情绪总分", fmt_compact(multifactor_frame["总分"].mean() if not multifactor_frame.empty else None))
            strategy_row[2].metric("VPIN", f"{float(vpin_frame['VPIN'].iloc[-1]):.3f}" if not vpin_frame.empty else "-")
            strategy_row[3].metric("形态样本", str(len(pattern_frame)))
            strategy_row[4].metric("回测样本", str(int(backtest_frame["样本数"].sum()) if not backtest_frame.empty else 0))
            signal_left, signal_right = st.columns([1.2, 1.2], gap="large")
            with signal_left:
                st.plotly_chart(
                    build_multifactor_sentiment_figure(multifactor_frame),
                    key=chart_key("lab-sentiment", base_coin, interval, lab_mode),
                    config=PLOTLY_CONFIG,
                )
            with signal_right:
                st.plotly_chart(
                    build_vpin_figure(vpin_frame),
                    key=chart_key("lab-vpin", base_coin, interval, lab_mode),
                    config=PLOTLY_CONFIG,
                )
            signal_table_left, signal_table_right = st.columns([1.15, 1.25], gap="large")
            with signal_table_left:
                st.dataframe(
                    wall_absorption_frame,
                    width="stretch",
                    hide_index=True,
                )
                st.dataframe(
                    microstructure_frame,
                    width="stretch",
                    hide_index=True,
                )
            with signal_table_right:
                st.dataframe(
                    multifactor_frame,
                    width="stretch",
                    hide_index=True,
                    column_config={
                        "价格": st.column_config.NumberColumn(format="%.2f"),
                        "总分": st.column_config.NumberColumn(format="%.2f"),
                        "价格因子": st.column_config.NumberColumn(format="%.2f"),
                        "OI因子": st.column_config.NumberColumn(format="%.2f"),
                        "流量因子": st.column_config.NumberColumn(format="%.2f"),
                        "Funding因子": st.column_config.NumberColumn(format="%.2f"),
                        "Crowd因子": st.column_config.NumberColumn(format="%.2f"),
                    },
                )
            pattern_left, pattern_right = st.columns([1.0, 1.35], gap="large")
            with pattern_left:
                st.dataframe(
                    pattern_frame.drop(columns=["索引"], errors="ignore"),
                    width="stretch",
                    hide_index=True,
                    column_config={"强度": st.column_config.ProgressColumn(format="%.2f", min_value=0.0, max_value=1.0)},
                )
            with pattern_right:
                st.dataframe(
                    backtest_frame,
                    width="stretch",
                    hide_index=True,
                    column_config={
                        "样本数": st.column_config.NumberColumn(format="%d"),
                        "胜率(%)": st.column_config.NumberColumn(format="%.2f"),
                        "平均收益(%)": st.column_config.NumberColumn(format="%.3f"),
                        "中位收益(%)": st.column_config.NumberColumn(format="%.3f"),
                        "平均MFE(%)": st.column_config.NumberColumn(format="%.3f"),
                        "平均MAE(%)": st.column_config.NumberColumn(format="%.3f"),
                    },
                )
            review_left, review_right = st.columns([1.05, 1.35], gap="large")
            with review_left:
                if alert_review_summary_frame.empty and alert_best_window_frame.empty:
                    st.info("历史库样本还不够，暂时无法算出告警最佳持有窗口。")
                else:
                    if not alert_review_summary_frame.empty:
                        st.dataframe(
                            alert_review_summary_frame,
                            width="stretch",
                            hide_index=True,
                            column_config={
                                "样本数": st.column_config.NumberColumn(format="%d"),
                                "命中率(%)": st.column_config.NumberColumn(format="%.2f"),
                                "平均收益(%)": st.column_config.NumberColumn(format="%.3f"),
                                "中位收益(%)": st.column_config.NumberColumn(format="%.3f"),
                                "平均顺风(%)": st.column_config.NumberColumn(format="%.3f"),
                                "平均逆风(%)": st.column_config.NumberColumn(format="%.3f"),
                            },
                        )
                    if not alert_best_window_frame.empty:
                        st.dataframe(
                            alert_best_window_frame,
                            width="stretch",
                            hide_index=True,
                            column_config={
                                "最佳窗口(min)": st.column_config.NumberColumn(format="%d"),
                                "样本数": st.column_config.NumberColumn(format="%d"),
                                "最佳命中率(%)": st.column_config.NumberColumn(format="%.2f"),
                                "平均收益(%)": st.column_config.NumberColumn(format="%.3f"),
                                "中位收益(%)": st.column_config.NumberColumn(format="%.3f"),
                                "平均顺风(%)": st.column_config.NumberColumn(format="%.3f"),
                                "平均逆风(%)": st.column_config.NumberColumn(format="%.3f"),
                            },
                        )
            with review_right:
                if alert_window_matrix_frame.empty:
                    st.info("当前还没有足够的告警触发样本来做多窗口复盘。")
                else:
                    st.dataframe(
                        alert_window_matrix_frame,
                        width="stretch",
                        hide_index=True,
                        column_config={
                            "窗口(min)": st.column_config.NumberColumn(format="%d"),
                            "样本数": st.column_config.NumberColumn(format="%d"),
                            "命中率(%)": st.column_config.NumberColumn(format="%.2f"),
                            "平均收益(%)": st.column_config.NumberColumn(format="%.3f"),
                            "中位收益(%)": st.column_config.NumberColumn(format="%.3f"),
                            "平均顺风(%)": st.column_config.NumberColumn(format="%.3f"),
                            "平均逆风(%)": st.column_config.NumberColumn(format="%.3f"),
                        },
                    )

        if lab_mode in ("总览", "通知与持久化"):
            render_section("推送 / 通知 / 体验 / 历史", "浏览器桌面通知、Telegram、SQLite 历史、自动 Parquet 归档和告警复盘都在这一层。")
            history_row = st.columns(6)
            history_row[0].metric("市场快照", str(int(history_summary.get("market_rows") or 0)))
            history_row[1].metric("告警事件", str(int(history_summary.get("alert_rows") or 0)))
            history_row[2].metric("成交/爆仓事件", str(int(history_summary.get("event_rows") or 0)))
            history_row[3].metric("盘口质量点", str(int(history_summary.get("quality_rows") or 0)))
            history_row[4].metric("归档文件", str(len(history_summary.get("archive_files") or [])))
            history_row[5].metric(
                "告警命中率",
                f"{(alert_review_frame['命中'].eq('是').mean() * 100.0):.1f}%"
                if not alert_review_frame.empty and "命中" in alert_review_frame.columns
                else "-",
            )
            st.caption(
                "通知规则: "
                + f"确认 {int(alert_confirm_after)} 次 | 冷却 {int(alert_cooldown_minutes)} 分钟 | "
                + f"桌面 {browser_min_level} 及以上 ({'开' if browser_notify_enabled else '关'}, 本轮 {len(browser_notifications)}) | "
                + f"Telegram {telegram_min_level} 及以上 ({'开' if telegram_push_enabled else '关'}, 本轮 {len(telegram_notifications)})"
            )
            if telegram_errors:
                st.warning("Telegram 推送这轮有失败: " + " | ".join(telegram_errors[:3]))
            if archive_events:
                st.caption("本轮自动归档: " + " | ".join(f"{item['table']} {item['day']} {item['rows']}行" for item in archive_events[:4]))
            st.dataframe(
                exchange_health_frame,
                width="stretch",
                hide_index=True,
                column_config={
                    "连续失败": st.column_config.NumberColumn(format="%d"),
                    "冷却剩余(s)": st.column_config.NumberColumn(format="%d"),
                },
            )
            persist_left, persist_right = st.columns([1.0, 1.3], gap="large")
            with persist_left:
                st.dataframe(
                    alert_history_frame,
                    width="stretch",
                    hide_index=True,
                )
                if not alert_history_frame.empty:
                    st.download_button(
                        "下载告警历史 CSV",
                        data=alert_history_frame.to_csv(index=False).encode("utf-8"),
                        file_name=f"{base_coin.lower()}-alert-history.csv",
                        mime="text/csv",
                    )
                if not event_summary_frame.empty:
                    st.dataframe(
                        event_summary_frame,
                        width="stretch",
                        hide_index=True,
                        column_config={
                            "事件数": st.column_config.NumberColumn(format="%d"),
                            "名义金额": st.column_config.NumberColumn(format="%.2f"),
                        },
                    )
            with persist_right:
                st.dataframe(
                    alert_review_frame,
                    width="stretch",
                    hide_index=True,
                    column_config={
                        "窗口收益(%)": st.column_config.NumberColumn(format="%.3f"),
                        "顺风区间(%)": st.column_config.NumberColumn(format="%.3f"),
                        "逆风区间(%)": st.column_config.NumberColumn(format="%.3f"),
                    },
                )
                if not quality_summary_frame.empty:
                    st.dataframe(
                        quality_summary_frame,
                        width="stretch",
                        hide_index=True,
                        column_config={
                            "样本数": st.column_config.NumberColumn(format="%d"),
                            "净变化": st.column_config.NumberColumn(format="%.2f"),
                            "假挂单次数": st.column_config.NumberColumn(format="%d"),
                            "补单次数": st.column_config.NumberColumn(format="%d"),
                            "最近盘口失衡(%)": st.column_config.NumberColumn(format="%.2f"),
                        },
                    )
                if not market_history_frame.empty:
                    st.download_button(
                        "下载市场历史 CSV",
                        data=market_history_frame.to_csv(index=False).encode("utf-8"),
                        file_name=f"{base_coin.lower()}-market-history.csv",
                        mime="text/csv",
                    )
            detail_left, detail_right = st.columns([1.1, 1.1], gap="large")
            with detail_left:
                st.dataframe(
                    event_history_frame,
                    width="stretch",
                    hide_index=True,
                    column_config={
                        "价格": st.column_config.NumberColumn(format="%.4f"),
                        "数量": st.column_config.NumberColumn(format="%.4f"),
                        "名义金额": st.column_config.NumberColumn(format="%.2f"),
                    },
                )
                if not event_history_frame.empty:
                    st.download_button(
                        "下载事件流 CSV",
                        data=event_history_frame.to_csv(index=False).encode("utf-8"),
                        file_name=f"{base_coin.lower()}-event-history.csv",
                        mime="text/csv",
                    )
            with detail_right:
                st.dataframe(
                    quality_history_frame,
                    width="stretch",
                    hide_index=True,
                    column_config={
                        "新增挂单额": st.column_config.NumberColumn(format="%.2f"),
                        "撤单额": st.column_config.NumberColumn(format="%.2f"),
                        "净变化": st.column_config.NumberColumn(format="%.2f"),
                        "近价新增": st.column_config.NumberColumn(format="%.2f"),
                        "近价撤单": st.column_config.NumberColumn(format="%.2f"),
                        "盘口失衡(%)": st.column_config.NumberColumn(format="%.2f"),
                    },
                )
                if not quality_history_frame.empty:
                    st.download_button(
                        "下载盘口质量 CSV",
                        data=quality_history_frame.to_csv(index=False).encode("utf-8"),
                        file_name=f"{base_coin.lower()}-quality-history.csv",
                        mime="text/csv",
                    )
            if history_sqlite_enabled:
                history_workbench_window = st.selectbox(
                    "复盘窗口",
                    ["1 小时", "4 小时", "12 小时", "24 小时", "72 小时"],
                    index=1,
                    key=chart_key("history-workbench-window", base_coin, lab_mode),
                )
                history_workbench_market = st.selectbox(
                    "复盘市场",
                    ["合约", "现货", "合并"],
                    index=0,
                    key=chart_key("history-workbench-market", base_coin, lab_mode),
                )
                history_workbench_event = st.selectbox(
                    "事件类型",
                    ["全部事件", "成交", "爆仓"],
                    index=0,
                    key=chart_key("history-workbench-event", base_coin, lab_mode),
                )
                history_exchange_options = [EXCHANGE_TITLES[key] for key in EXCHANGE_ORDER]
                default_history_exchange_titles = [EXCHANGE_TITLES[key] for key in scope_perp_keys] if scope_perp_keys else [EXCHANGE_TITLES[selected_exchange]]
                selected_history_exchange_titles = st.multiselect(
                    "复盘交易所",
                    history_exchange_options,
                    default=default_history_exchange_titles,
                    key=chart_key("history-workbench-exchanges", base_coin, lab_mode),
                )
                history_exchange_map = {value: key for key, value in EXCHANGE_TITLES.items()}
                history_workbench_exchange_keys = [history_exchange_map[title] for title in selected_history_exchange_titles if title in history_exchange_map]
                history_window_ms_map = {
                    "1 小时": 1 * 60 * 60 * 1000,
                    "4 小时": 4 * 60 * 60 * 1000,
                    "12 小时": 12 * 60 * 60 * 1000,
                    "24 小时": 24 * 60 * 60 * 1000,
                    "72 小时": 72 * 60 * 60 * 1000,
                }
                history_market_selection = {
                    "合约": ["perp"],
                    "现货": ["spot"],
                    "合并": ["perp", "spot"],
                }.get(history_workbench_market, ["perp"])
                history_event_selection = {
                    "全部事件": ["all"],
                    "成交": ["trade"],
                    "爆仓": ["liquidation"],
                }.get(history_workbench_event, ["all"])
                history_workbench_since_ms = now_ms - history_window_ms_map.get(history_workbench_window, 4 * 60 * 60 * 1000)
                history_workbench_payload = get_cached_derived_result(
                    f"history-workbench::{base_coin}",
                    (
                        int(history_workbench_since_ms),
                        tuple(history_workbench_exchange_keys),
                        tuple(history_market_selection),
                        tuple(history_event_selection),
                        int(history_summary.get("last_market_ts") or 0),
                        int(history_summary.get("last_event_ts") or 0),
                        int(history_summary.get("last_quality_ts") or 0),
                        int(history_summary.get("last_alert_ts") or 0),
                    ),
                    ttl_seconds=max(20, heavy_ttl),
                    builder=lambda: build_history_workbench_payload(
                        history_store,
                        coin=base_coin,
                        exchange_keys=history_workbench_exchange_keys,
                        markets=history_market_selection,
                        event_categories=history_event_selection,
                        since_ms=history_workbench_since_ms,
                    ),
                ) or {}
                history_workbench_market_frame = history_workbench_payload.get("market_history_frame", pd.DataFrame())
                history_workbench_event_frame = history_workbench_payload.get("event_history_frame", pd.DataFrame())
                history_workbench_quality_frame = history_workbench_payload.get("quality_history_frame", pd.DataFrame())
                history_workbench_alert_frame = history_workbench_payload.get("alert_history_frame", pd.DataFrame())
                render_section("历史复盘工作台", "按窗口、市场、事件类型和交易所切片，直接回看价格、OI、Funding、事件流和盘口质量，不用手工翻 SQLite。")
                workbench_cards = st.columns(4)
                workbench_cards[0].metric("市场样本", str(len(history_workbench_market_frame)))
                workbench_cards[1].metric("事件样本", str(len(history_workbench_event_frame)))
                workbench_cards[2].metric("盘口质量点", str(len(history_workbench_quality_frame)))
                workbench_cards[3].metric("告警样本", str(len(history_workbench_alert_frame)))
                workbench_left, workbench_right = st.columns([1.4, 1.0], gap="large")
                with workbench_left:
                    st.plotly_chart(
                        build_history_workbench_figure(history_workbench_market_frame),
                        key=chart_key("history-workbench-market-figure", base_coin, lab_mode),
                        config=PLOTLY_CONFIG,
                    )
                with workbench_right:
                    st.plotly_chart(
                        build_history_event_mix_figure(history_workbench_event_frame),
                        key=chart_key("history-workbench-event-figure", base_coin, lab_mode),
                        config=PLOTLY_CONFIG,
                    )
                workbench_detail_left, workbench_detail_right = st.columns([1.0, 1.0], gap="large")
                with workbench_detail_left:
                    st.dataframe(
                        history_workbench_event_frame.head(40),
                        width="stretch",
                        hide_index=True,
                        column_config={
                            "价格": st.column_config.NumberColumn(format="%.4f"),
                            "数量": st.column_config.NumberColumn(format="%.4f"),
                            "名义金额": st.column_config.NumberColumn(format="%.2f"),
                        },
                    )
                with workbench_detail_right:
                    st.dataframe(
                        history_workbench_quality_frame.head(40),
                        width="stretch",
                        hide_index=True,
                        column_config={
                            "新增挂单额": st.column_config.NumberColumn(format="%.2f"),
                            "撤单额": st.column_config.NumberColumn(format="%.2f"),
                            "净变化": st.column_config.NumberColumn(format="%.2f"),
                            "近价新增": st.column_config.NumberColumn(format="%.2f"),
                            "近价撤单": st.column_config.NumberColumn(format="%.2f"),
                            "盘口失衡(%)": st.column_config.NumberColumn(format="%.2f"),
                        },
                    )
            index_left, index_right = st.columns([1.0, 1.2], gap="large")
            with index_left:
                st.caption(f"历史索引页: 最近 {history_index_days} 天")
                st.dataframe(
                    alert_catalog_index_frame,
                    width="stretch",
                    hide_index=True,
                    column_config={
                        "触发数": st.column_config.NumberColumn(format="%d"),
                        "确认数": st.column_config.NumberColumn(format="%d"),
                    },
                )
                st.dataframe(
                    event_catalog_index_frame,
                    width="stretch",
                    hide_index=True,
                    column_config={
                        "事件数": st.column_config.NumberColumn(format="%d"),
                        "名义金额": st.column_config.NumberColumn(format="%.2f"),
                    },
                )
            with index_right:
                st.dataframe(
                    market_coverage_index_frame,
                    width="stretch",
                    hide_index=True,
                    column_config={
                        "快照数": st.column_config.NumberColumn(format="%d"),
                        "事件数": st.column_config.NumberColumn(format="%d"),
                        "盘口质量点": st.column_config.NumberColumn(format="%d"),
                    },
                )
            if history_sqlite_enabled:
                st.caption(
                    f"SQLite: `{history_summary.get('db_path')}` | 最近市场样本 {_format_latency(history_summary.get('last_market_ts'))} | "
                    f"最近告警样本 {_format_latency(history_summary.get('last_alert_ts'))} | 最近事件样本 {_format_latency(history_summary.get('last_event_ts'))} | "
                    f"最近盘口质量样本 {_format_latency(history_summary.get('last_quality_ts'))}"
                )
                archive_file_frame = pd.DataFrame({"归档文件": history_summary.get("archive_files") or []})
                if not archive_file_frame.empty:
                    st.dataframe(archive_file_frame, width="stretch", hide_index=True)
        return
    if active_view == "交易台深度页":
        spot_focus_exchange = selected_exchange if selected_exchange in available_spot_keys else (available_spot_keys[0] if available_spot_keys else selected_exchange)
        spot_focus_exchange_name = EXCHANGE_TITLES[spot_focus_exchange]
        spot_focus_symbol = spot_symbol_map.get(spot_focus_exchange, spot_symbol_map.get("binance", spot_symbol))
        spot_focus_snapshot = spot_snapshot_map.get(spot_focus_exchange) or SpotSnapshot(exchange=spot_focus_exchange_name, symbol=spot_focus_symbol, status="error", error="未上架此币")
        spot_focus_orderbook = spot_orderbook_map.get(spot_focus_exchange, [])
        spot_focus_trades = spot_trades_map.get(spot_focus_exchange, [])
        spot_focus_summary = spot_summary_map.get(spot_focus_exchange, {})
        spot_focus_reference_price = (
            spot_focus_snapshot.last_price
            or spot_focus_snapshot.bid_price
            or spot_focus_snapshot.ask_price
            or (spot_focus_orderbook[0].price if spot_focus_orderbook else None)
        )
        spot_focus_heat_bars = aggregate_heat_bars(
            spot_focus_orderbook,
            spot_focus_reference_price,
            heat_window_pct,
            heat_buckets,
            heat_bars_per_side,
        )
        spot_focus_heat_frame = build_heat_frame(spot_focus_heat_bars)
        spot_focus_mbo_frame = build_mbo_profile_frame(spot_focus_orderbook, spot_focus_reference_price, mbo_rows)
        spot_focus_trade_metrics = build_trade_metrics(spot_focus_trades, now_ms, liquidation_window_minutes)
        spot_focus_quality_history = service.get_spot_orderbook_quality_history(spot_focus_exchange)
        spot_focus_quality_frame = build_orderbook_quality_frame(spot_focus_quality_history, limit=24)
        spot_focus_recorded_events = service.get_spot_recorded_events(spot_focus_exchange)
        spot_focus_perp_snapshot = snapshot_by_key.get(spot_focus_exchange, selected_snapshot)
        spot_focus_perp_metrics = build_spot_perp_metrics(
            spot_focus_snapshot,
            spot_focus_perp_snapshot,
            spot_focus_orderbook,
            perp_orderbook_map.get(spot_focus_exchange, []),
            spot_focus_trades,
        )
        spot_focus_transport_health = service.get_transport_health(spot_focus_exchange, spot=True)
        live_focus_spot_snapshot = service.get_spot_snapshot(spot_focus_exchange)
        live_focus_spot_orderbook = service.get_spot_orderbook(spot_focus_exchange)
        live_focus_spot_trades = service.get_spot_trade_history(spot_focus_exchange)
        spot_focus_lead_lag_metrics = compute_spot_perp_lead_lag(
            spot_focus_trades,
            perp_trades_map[spot_focus_exchange],
            now_ms=now_ms,
            lookback_minutes=max(3, min(liquidation_window_minutes, 10)),
            bucket_seconds=1,
            max_lag_buckets=3,
        )
        spot_focus_lead_lag_summary = str(spot_focus_lead_lag_metrics.get("summary") or "样本不足")
        spot_focus_lead_lag_confidence = _confidence_label(spot_focus_lead_lag_metrics.get("confidence"))
        spot_focus_snapshot_state = _build_state_caption(
            "快照 WS实时" if _use_live_snapshot(live_focus_spot_snapshot) else "快照 REST回补",
            spot_focus_transport_health.get("snapshot_timestamp_ms"),
        )
        spot_focus_orderbook_state = _build_state_caption(
            f"盘口 {'WS实时' if _use_live_orderbook(live_focus_spot_orderbook, spot_focus_snapshot.timestamp_ms) else 'REST回补'}",
            spot_focus_transport_health.get("snapshot_timestamp_ms"),
            sync_state=str(spot_focus_transport_health.get("sync_state") or ""),
            sample_count=len(spot_focus_orderbook),
            min_samples=16,
        )
        spot_focus_trade_state = _build_state_caption(
            f"成交 {'WS实时' if _use_live_trades(live_focus_spot_trades) else 'REST回补'}",
            _latest_trade_timestamp(spot_focus_trades),
            sample_count=len(spot_focus_trades),
            min_samples=12,
        )
        spot_focus_quality_confidence = _confidence_label(
            _orderbook_quality_confidence_score(spot_focus_quality_history, str(spot_focus_transport_health.get("sync_state") or ""))
        )
        spot_focus_replay_confidence = _confidence_label(min(1.0, len(spot_focus_recorded_events) / 24.0 if spot_focus_recorded_events else 0.0))
        spot_view_notice = (
            f"{selected_snapshot.exchange} 当前没有接入现货公共流，现货深度自动切到 {spot_focus_exchange_name} Spot。"
            if not spot_symbol_map.get(selected_exchange) and spot_symbol_map.get(spot_focus_exchange)
            else "当前币种在已接入交易所没有现货市场，本页会以合约和可用横向对照为主。"
            if not available_spot_keys
            else ""
        )
        spot_depth_frame = pd.DataFrame(
            [
                {
                    "交易所": spot_focus_exchange_name,
                    "现货价格": None if spot_focus_snapshot.status != "ok" else spot_focus_snapshot.last_price,
                    "买一": None if spot_focus_snapshot.status != "ok" else spot_focus_snapshot.bid_price,
                    "卖一": None if spot_focus_snapshot.status != "ok" else spot_focus_snapshot.ask_price,
                    "现货价差(bps)": spot_focus_snapshot.spread_bps,
                    "盘口失衡(%)": spot_focus_summary.get("imbalance_pct"),
                    "买盘挂单金额": spot_focus_summary.get("bid_notional"),
                    "卖盘挂单金额": spot_focus_summary.get("ask_notional"),
                    "24h成交额": None if spot_focus_snapshot.status != "ok" else spot_focus_snapshot.volume_24h_notional,
                    "主动买占比(%)": None if spot_focus_trade_metrics.get("buy_ratio") is None else float(spot_focus_trade_metrics.get("buy_ratio") or 0.0) * 100.0,
                    "短线提示": spot_focus_lead_lag_summary,
                }
            ]
        )
        if exchange_scope_mode == "全部交易所":
            render_section("全交易所联动范围", "当前深度页已经切到全部交易所模式。主图仍跟随当前所，下面这层专门保留现货 / 合约横向对照，方便测试跨所联动。")
            st.caption("这里的表只负责横向比较；真正的主图、风险图、回放、盘口质量仍然围绕当前选中的合约交易所展开。")
            deep_scope_row = st.columns(4)
            deep_scope_row[0].metric("现货在线", str(int(deep_spot_dashboard_frame["现货价格"].notna().sum()) if not deep_spot_dashboard_frame.empty else 0))
            deep_scope_row[1].metric("合约在线", str(int(deep_perp_dashboard_frame["合约价格"].notna().sum()) if not deep_perp_dashboard_frame.empty else 0))
            deep_scope_row[2].metric("现货最紧价差", fmt_bps(deep_spot_dashboard_frame["现货价差(bps)"].min() if not deep_spot_dashboard_frame.empty else None))
            deep_scope_row[3].metric("合约最大 OI 变化", fmt_pct(deep_perp_dashboard_frame["OI变化(%)"].abs().max() if not deep_perp_dashboard_frame.empty else None))
            deep_scope_left, deep_scope_right = st.columns([1.18, 1.22], gap="large")
            with deep_scope_left:
                st.dataframe(
                    deep_spot_dashboard_frame,
                    width="stretch",
                    hide_index=True,
                    column_config={
                        "现货价格": st.column_config.NumberColumn(format="%.2f"),
                        "买一": st.column_config.NumberColumn(format="%.2f"),
                        "卖一": st.column_config.NumberColumn(format="%.2f"),
                        "现货价差(bps)": st.column_config.NumberColumn(format="%.2f"),
                        "盘口失衡(%)": st.column_config.NumberColumn(format="%.2f"),
                        "买盘挂单金额": st.column_config.NumberColumn(format="%.2f"),
                        "卖盘挂单金额": st.column_config.NumberColumn(format="%.2f"),
                        "24h成交额": st.column_config.NumberColumn(format="%.2f"),
                    },
                )
            with deep_scope_right:
                st.dataframe(
                    deep_perp_dashboard_frame,
                    width="stretch",
                    hide_index=True,
                    column_config={
                        "合约价格": st.column_config.NumberColumn(format="%.2f"),
                        "资金费率(bps)": st.column_config.NumberColumn(format="%.2f"),
                        "未平仓量": st.column_config.NumberColumn(format="%.4f"),
                        "未平仓金额": st.column_config.NumberColumn(format="%.2f"),
                        "合约多空比": st.column_config.NumberColumn(format="%.3f"),
                        "OI变化(%)": st.column_config.NumberColumn(format="%.2f"),
                        "价格变化(%)": st.column_config.NumberColumn(format="%.2f"),
                        "合约价差(bps)": st.column_config.NumberColumn(format="%.2f"),
                        "盘口失衡(%)": st.column_config.NumberColumn(format="%.2f"),
                        "24h成交额": st.column_config.NumberColumn(format="%.2f"),
                    },
                )
                st.caption("这里的合约横向表也已经按 `多空比偏离程度` 优先排序，方便先抓到最拥挤的那一所。")
            if lead_lag_rows:
                lead_cards = st.columns(max(1, len(lead_lag_rows)))
                for column, row in zip(lead_cards, lead_lag_rows):
                    column.metric(
                        f"{row['交易所']} lead/lag",
                        str(row.get("提示") or "样本不足"),
                        delta="-" if row.get("相关性") is None or pd.isna(row.get("相关性")) else f"相关性 {float(row['相关性']):.2f}",
                    )

        render_carry_surface_panel(
            carry_surface_frame,
            key_scope=chart_key("carry-deep", base_coin, exchange_scope_mode, selected_exchange, interval),
        )

        if hyperliquid_address_bundle:
            render_hyperliquid_address_panel(
                hyperliquid_address_bundle,
                address=hyperliquid_address_value,
                lookback_hours=int(hyperliquid_address_lookback_hours),
                current_coin=symbol_map["hyperliquid"],
                key_scope=chart_key("hyper-address-deep", base_coin, selected_exchange, interval),
            )

        if deep_market_mode == "现货深度":
            render_section(
                f"{spot_focus_exchange_name} Spot {spot_focus_symbol}",
                "只看现货价格、盘口、主动买卖和现货簿面质量。现货没有 OI / 未平仓，所以不会混入合约字段。",
            )
            st.caption(
                _join_caption_parts(
                    spot_focus_snapshot_state,
                    spot_focus_orderbook_state,
                    spot_focus_trade_state,
                    f"Lead/Lag {spot_focus_lead_lag_confidence}",
                )
            )
            if spot_view_notice:
                st.info(spot_view_notice)
            spot_row_top = st.columns(6)
            spot_row_top[0].metric("现货价格", fmt_price(spot_focus_snapshot.last_price))
            spot_row_top[1].metric("买一", fmt_price(spot_focus_snapshot.bid_price))
            spot_row_top[2].metric("卖一", fmt_price(spot_focus_snapshot.ask_price))
            spot_row_top[3].metric("现货价差", fmt_bps(spot_focus_snapshot.spread_bps))
            spot_row_top[4].metric("24h成交额", fmt_compact(spot_focus_snapshot.volume_24h_notional))
            spot_row_top[5].metric("Lead/Lag", spot_focus_lead_lag_summary)
            spot_row_bottom = st.columns(4)
            spot_row_bottom[0].metric("买盘挂单金额", fmt_compact(spot_focus_summary.get("bid_notional")))
            spot_row_bottom[1].metric("卖盘挂单金额", fmt_compact(spot_focus_summary.get("ask_notional")))
            spot_row_bottom[2].metric("盘口失衡", fmt_pct(spot_focus_summary.get("imbalance_pct")))
            spot_row_bottom[3].metric(
                f"近{liquidation_window_minutes}m 主动买占比",
                fmt_pct(float(spot_focus_trade_metrics.get("buy_ratio") or 0.0) * 100.0 if spot_focus_trade_metrics.get("buy_ratio") is not None else None),
            )

            render_section("现货主动买卖 / CVD / 盘口剖面", "左边看现货主动净流和 CVD，右边看现货本地盘口剖面和队列压力。")
            st.caption(
                _join_caption_parts(
                    spot_focus_trade_state,
                    spot_focus_orderbook_state,
                    f"Lead/Lag {spot_focus_lead_lag_confidence}",
                    "样本不足" if int(spot_focus_lead_lag_metrics.get("samples") or 0) < 8 else None,
                )
            )
            spot_left, spot_right = st.columns([2.0, 1.4], gap="large")
            with spot_left:
                st.plotly_chart(
                    build_cvd_figure(spot_focus_trades, spot_focus_snapshot.timestamp_ms or now_ms, max(30, liquidation_window_minutes)),
                    key=chart_key("spot-depth-cvd", base_coin, spot_focus_exchange, spot_focus_symbol, interval),
                    config=PLOTLY_CONFIG,
                )
            with spot_right:
                st.plotly_chart(
                    build_mbo_figure(spot_focus_mbo_frame, spot_focus_reference_price),
                    key=chart_key("spot-depth-mbo", base_coin, spot_focus_exchange, spot_focus_symbol, interval),
                    config=PLOTLY_CONFIG,
                )

            render_section("现货盘口热区 / Recent Trades", "热区表直接看哪一带挂单更厚，右边放最近的现货逐笔成交。")
            spot_heat_col, spot_trades_col = st.columns([1.2, 1.1], gap="large")
            with spot_heat_col:
                if spot_focus_heat_frame.empty:
                    st.info("当前现货盘口深度不足，暂时没有可绘制的热力条。")
                else:
                    st.dataframe(
                        spot_focus_heat_frame,
                        width="stretch",
                        hide_index=True,
                        column_config={
                            "挂单量": st.column_config.NumberColumn(format="%.2f"),
                            "热度": st.column_config.ProgressColumn(format="%.2f", min_value=0.0, max_value=1.0),
                            "离现价": st.column_config.NumberColumn(format="%.2f%%"),
                        },
                    )
            with spot_trades_col:
                spot_trade_frame = build_trade_frame(spot_focus_trades, limit=24)
                if spot_trade_frame.empty:
                    st.info("当前会话里还没有足够的现货逐笔成交样本。")
                else:
                    st.dataframe(
                        spot_trade_frame,
                        width="stretch",
                        hide_index=True,
                        column_config={
                            "价格": st.column_config.NumberColumn(format="%.2f"),
                            "数量": st.column_config.NumberColumn(format="%.4f"),
                            "名义金额": st.column_config.NumberColumn(format="%.2f"),
                        },
                    )
            st.dataframe(
                spot_depth_frame,
                width="stretch",
                hide_index=True,
                column_config={
                    "现货价格": st.column_config.NumberColumn(format="%.2f"),
                    "买一": st.column_config.NumberColumn(format="%.2f"),
                    "卖一": st.column_config.NumberColumn(format="%.2f"),
                    "现货价差(bps)": st.column_config.NumberColumn(format="%.2f"),
                    "盘口失衡(%)": st.column_config.NumberColumn(format="%.2f"),
                    "买盘挂单金额": st.column_config.NumberColumn(format="%.2f"),
                    "卖盘挂单金额": st.column_config.NumberColumn(format="%.2f"),
                    "24h成交额": st.column_config.NumberColumn(format="%.2f"),
                    "主动买占比(%)": st.column_config.NumberColumn(format="%.2f"),
                },
            )

            if desk_mode == "核心":
                st.info("当前是核心模式。现货盘口质量和现货事件回放已经延后；需要时把深度页模式切到 `完整`。")
                return

            render_section("现货盘口质量 / 撤单速度 / 假挂单 / 补单", "这里专门看现货簿面的新增、撤单、净变化和补单，不再混入合约簿面。")
            st.caption(
                _join_caption_parts(
                    spot_focus_orderbook_state,
                    f"簿面质量 {spot_focus_quality_confidence}",
                )
            )
            spot_quality_latest = spot_focus_quality_frame.iloc[0].to_dict() if not spot_focus_quality_frame.empty else {}
            spot_quality_row = st.columns(5)
            spot_quality_row[0].metric("新增挂单额", fmt_compact(spot_quality_latest.get("新增挂单额")))
            spot_quality_row[1].metric("撤单额", fmt_compact(spot_quality_latest.get("撤单额")))
            spot_quality_row[2].metric("近价撤单", fmt_compact(spot_quality_latest.get("近价撤单")))
            spot_quality_row[3].metric("假挂单次数", str(int(spot_quality_latest.get("假挂单次数") or 0)))
            spot_quality_row[4].metric("补单次数", str(int(spot_quality_latest.get("补单次数") or 0)))
            spot_quality_left, spot_quality_right = st.columns([1.85, 1.15], gap="large")
            with spot_quality_left:
                st.plotly_chart(
                    build_orderbook_quality_figure(spot_focus_quality_history, title=f"{spot_focus_exchange_name} Spot Orderbook Quality"),
                    key=chart_key("spot-depth-quality", base_coin, spot_focus_exchange, spot_focus_symbol, interval),
                    config=PLOTLY_CONFIG,
                )
            with spot_quality_right:
                if spot_focus_quality_frame.empty:
                    st.info("等待现货本地订单簿增量样本。")
                else:
                    st.dataframe(
                        spot_focus_quality_frame,
                        width="stretch",
                        hide_index=True,
                        column_config={
                            "新增挂单额": st.column_config.NumberColumn(format="%.2f"),
                            "撤单额": st.column_config.NumberColumn(format="%.2f"),
                            "净变化": st.column_config.NumberColumn(format="%.2f"),
                            "近价新增": st.column_config.NumberColumn(format="%.2f"),
                            "近价撤单": st.column_config.NumberColumn(format="%.2f"),
                            "买墙持续(s)": st.column_config.NumberColumn(format="%.1f"),
                            "卖墙持续(s)": st.column_config.NumberColumn(format="%.1f"),
                            "盘口失衡(%)": st.column_config.NumberColumn(format="%.2f"),
                        },
                    )

            render_section("现货事件录制 / 复盘回放", "当前会自动录制会话内的现货成交和现货簿面质量事件。这里先给最近 2 分钟的交互式回放。")
            st.caption(
                _join_caption_parts(
                    spot_focus_trade_state,
                    f"回放 {spot_focus_replay_confidence}",
                    f"录制事件 {len(spot_focus_recorded_events)} 条",
                )
            )
            spot_replay_speed = st.selectbox(
                "现货回放速度",
                ["1x", "5x", "20x"],
                index=1,
                key=chart_key("spot-replay-speed", base_coin, spot_focus_exchange, spot_focus_symbol, interval),
            )
            spot_replay_progress = st.slider(
                "现货回放进度",
                min_value=0,
                max_value=100,
                value=100,
                step=5,
                key=chart_key("spot-replay-progress", base_coin, spot_focus_exchange, spot_focus_symbol, interval),
            )
            spot_replay_start_ms = max(0, now_ms - 120_000)
            spot_replay_left, spot_replay_right = st.columns([1.9, 1.1], gap="large")
            with spot_replay_left:
                st.plotly_chart(
                    build_replay_figure(
                        spot_focus_recorded_events,
                        spot_replay_start_ms,
                        now_ms,
                        progress_ratio=spot_replay_progress / 100.0,
                    ),
                    key=chart_key("spot-replay-figure", base_coin, spot_focus_exchange, spot_focus_symbol, interval),
                    config=PLOTLY_CONFIG,
                )
            with spot_replay_right:
                spot_replay_frame = build_recorded_event_frame(
                    [
                        event
                        for event in spot_focus_recorded_events
                        if spot_replay_start_ms <= event.timestamp_ms <= now_ms
                    ],
                    limit=36,
                )
                if spot_replay_frame.empty:
                    st.info("当前现货回放窗口里还没有录制到事件。")
                else:
                    st.dataframe(
                        spot_replay_frame,
                        width="stretch",
                        hide_index=True,
                        column_config={
                            "价格": st.column_config.NumberColumn(format="%.2f"),
                            "数量": st.column_config.NumberColumn(format="%.4f"),
                            "名义金额": st.column_config.NumberColumn(format="%.2f"),
                            "数值": st.column_config.NumberColumn(format="%.2f"),
                        },
                    )
                spot_speed_factor = int(spot_replay_speed.replace("x", ""))
                st.caption(
                    f"当前速度 {spot_replay_speed}。这里只回放最近 2 分钟的现货事件，方便快速复盘现货先动还是盘口先变。"
                    f" 当前进度约等于 {(120.0 * (spot_replay_progress / 100.0) / max(spot_speed_factor, 1)):.1f}s。"
                )
            return

        if deep_market_mode == "综合深度":
            render_section("现货 / 合约联动总览", "综合深度先给你一层 spot-perp 联动摘要，再往下看合约主图、OI、爆仓和风险区。")
            st.caption(
                _join_caption_parts(
                    f"现货侧 {spot_focus_snapshot_state}",
                    spot_focus_trade_state,
                    f"Lead/Lag {spot_focus_lead_lag_confidence}",
                    f"合约侧 {selected_snapshot_state}",
                    selected_oi_state,
                )
            )
            if spot_view_notice:
                st.info(spot_view_notice)
            combined_row = st.columns(6)
            combined_row[0].metric(f"{spot_focus_exchange_name} 现货价", fmt_price(spot_focus_snapshot.last_price))
            combined_row[1].metric(f"{spot_focus_exchange_name} 永续价", fmt_price(spot_focus_perp_snapshot.last_price))
            combined_row[2].metric("Basis", fmt_pct(spot_focus_perp_metrics.get("basis_pct")))
            combined_row[3].metric("Lead/Lag", spot_focus_lead_lag_summary)
            combined_row[4].metric(
                "现货主动买占比",
                fmt_pct(float(spot_focus_perp_metrics.get("spot_buy_ratio") or 0.0) * 100.0 if spot_focus_perp_metrics.get("spot_buy_ratio") is not None else None),
            )
            combined_row[5].metric("合约加减仓", str(oi_quadrant.get("label") or "-"))
            combined_left, combined_right = st.columns([1.2, 1.2], gap="large")
            with combined_left:
                st.dataframe(
                    spot_depth_frame,
                    width="stretch",
                    hide_index=True,
                    column_config={
                        "现货价格": st.column_config.NumberColumn(format="%.2f"),
                        "买一": st.column_config.NumberColumn(format="%.2f"),
                        "卖一": st.column_config.NumberColumn(format="%.2f"),
                        "现货价差(bps)": st.column_config.NumberColumn(format="%.2f"),
                        "盘口失衡(%)": st.column_config.NumberColumn(format="%.2f"),
                        "买盘挂单金额": st.column_config.NumberColumn(format="%.2f"),
                        "卖盘挂单金额": st.column_config.NumberColumn(format="%.2f"),
                        "24h成交额": st.column_config.NumberColumn(format="%.2f"),
                        "主动买占比(%)": st.column_config.NumberColumn(format="%.2f"),
                    },
                )
            with combined_right:
                st.dataframe(
                    pd.DataFrame(
                        [
                            {
                                "交易所": selected_snapshot.exchange,
                                "合约价格": selected_snapshot.last_price if selected_snapshot.status == "ok" else None,
                                "资金费率(bps)": selected_snapshot.funding_bps,
                                "未平仓量": selected_snapshot.open_interest,
                                "未平仓金额": selected_snapshot.open_interest_notional,
                                "OI变化(%)": oi_quadrant.get("oi_change_pct"),
                                "价格变化(%)": oi_quadrant.get("price_change_pct"),
                                "加减仓状态": oi_quadrant.get("label"),
                                "合约价差(bps)": book_summary.get("spread_bps"),
                                "盘口失衡(%)": book_summary.get("imbalance_pct"),
                                "24h成交额": selected_snapshot.volume_24h_notional,
                            }
                        ]
                    ),
                    width="stretch",
                    hide_index=True,
                    column_config={
                        "合约价格": st.column_config.NumberColumn(format="%.2f"),
                        "资金费率(bps)": st.column_config.NumberColumn(format="%.2f"),
                        "未平仓量": st.column_config.NumberColumn(format="%.4f"),
                        "未平仓金额": st.column_config.NumberColumn(format="%.2f"),
                        "OI变化(%)": st.column_config.NumberColumn(format="%.2f"),
                        "价格变化(%)": st.column_config.NumberColumn(format="%.2f"),
                        "合约价差(bps)": st.column_config.NumberColumn(format="%.2f"),
                        "盘口失衡(%)": st.column_config.NumberColumn(format="%.2f"),
                        "24h成交额": st.column_config.NumberColumn(format="%.2f"),
                    },
                )

        depth_title = f"{selected_snapshot.exchange} {selected_symbol}"
        depth_intro = (
            "合约深度只看 K 线、OI、爆仓真值和风险区，不再混入现货盘口。"
            if deep_market_mode == "合约深度"
            else "综合深度下面仍以合约主图为主，现货联动已经在上方摘要里单独展开。"
        )
        render_section(depth_title, depth_intro)
        st.caption(
            _join_caption_parts(
                selected_snapshot_state,
                selected_orderbook_state,
                selected_trade_state,
                selected_oi_state,
            )
        )
        left, right = st.columns([3.1, 1.35], gap="large")
        with left:
            st.plotly_chart(
                build_terminal_chart(candles, heat_bars, selected_snapshot, interval),
                key=chart_key("terminal", base_coin, selected_exchange, selected_symbol, interval),
                config=PLOTLY_CONFIG,
            )
        with right:
            oi_figure, oi_label = build_oi_figure(merged_oi)
            st.plotly_chart(
                oi_figure,
                key=chart_key("oi", base_coin, selected_exchange, selected_symbol, interval),
                config=PLOTLY_CONFIG,
            )
            st.caption(oi_label)
            heat_frame = build_heat_frame(heat_bars)
            if heat_frame.empty:
                st.info("当前盘口深度不足，暂时没有可绘制的热力条。")
            else:
                st.dataframe(heat_frame, width="stretch", hide_index=True, column_config={"挂单量": st.column_config.NumberColumn(format="%.2f"), "热度": st.column_config.ProgressColumn(format="%.2f", min_value=0.0, max_value=1.0), "离现价": st.column_config.NumberColumn(format="%.2f%%")})

        render_section("OI Delta / 加减仓四象限", "结合价格变化和 OI 变化，快速看当前更像主动加仓、回补还是减仓。")
        st.caption(selected_oi_state)
        quad_left, quad_right = st.columns([1.45, 1.35], gap="large")
        with quad_left:
            st.plotly_chart(
                build_oi_quadrant_figure(oi_quadrant),
                key=chart_key("quadrant", base_coin, selected_exchange, selected_symbol, interval),
                config=PLOTLY_CONFIG,
            )
        with quad_right:
            quad_metrics = st.columns(2)
            quad_metrics[0].metric("价格变化", fmt_pct(oi_quadrant.get("price_change_pct")))
            quad_metrics[1].metric("OI 变化", fmt_pct(oi_quadrant.get("oi_change_pct")))
            quad_metrics = st.columns(2)
            quad_metrics[0].metric("推断状态", oi_quadrant.get("label") or "-")
            quad_metrics[1].metric("置信度", fmt_pct((oi_quadrant.get("confidence") or 0.0) * 100 if oi_quadrant.get("confidence") is not None else None))
            st.caption(f"口径: {oi_quadrant.get('value_label') or '等待 OI 数据'}。这是基于公开字段的高质量推断，不是交易所直接返回的真值。")

        render_section("主动买卖 / CVD / 吸收与衰竭", "逐笔成交流看谁在主动扫单，CVD 看净主动方向，价格与主动成交背离时用来识别吸收与衰竭。")
        st.caption(selected_trade_state)
        trade_row = st.columns(4)
        trade_row[0].metric(f"近{liquidation_window_minutes}m 主动买额", fmt_compact(trade_metrics.get("buy_notional")))
        trade_row[1].metric(f"近{liquidation_window_minutes}m 主动卖额", fmt_compact(trade_metrics.get("sell_notional")))
        trade_row[2].metric("主动买占比", fmt_pct((trade_metrics.get("buy_ratio") or 0.0) * 100 if trade_metrics.get("buy_ratio") is not None else None))
        trade_row[3].metric("流动性状态", trade_metrics.get("regime") or "-")
        trade_left, trade_right = st.columns([2.1, 1.35], gap="large")
        with trade_left:
            st.plotly_chart(
                build_cvd_figure(trade_events, selected_snapshot.timestamp_ms or int(time.time() * 1000), max(30, liquidation_window_minutes)),
                key=chart_key("cvd", base_coin, selected_exchange, selected_symbol, interval),
                config=PLOTLY_CONFIG,
            )
        with trade_right:
            trade_frame = build_trade_frame(trade_events, limit=24)
            if trade_frame.empty:
                st.info("当前会话里还没有足够的逐笔成交样本。")
            else:
                st.dataframe(
                    trade_frame,
                    width="stretch",
                    hide_index=True,
                    column_config={
                        "价格": st.column_config.NumberColumn(format="%.2f"),
                        "数量": st.column_config.NumberColumn(format="%.4f"),
                        "名义金额": st.column_config.NumberColumn(format="%.2f"),
                    },
                )

        render_section("OI + CVD + Funding + Crowd 合成信号", "把价格、OI Delta、主动买卖、Funding 和 crowd 合成一个总分，减少单指标误判。")
        st.caption(
            _join_caption_parts(
                selected_oi_state,
                selected_trade_state,
                f"合成信号 {_confidence_label(composite_signal.get('confidence'))}",
            )
        )
        composite_left, composite_right = st.columns([1.75, 1.2], gap="large")
        with composite_left:
            st.plotly_chart(
                build_composite_signal_figure(composite_signal),
                key=chart_key("composite-signal", base_coin, selected_exchange, selected_symbol, interval),
                config=PLOTLY_CONFIG,
            )
        with composite_right:
            signal_row = st.columns(3)
            signal_row[0].metric("总分", f"{float(composite_signal.get('score') or 0.0):+.1f}")
            signal_row[1].metric("状态", str(composite_signal.get("label") or "-"))
            signal_row[2].metric("置信度", fmt_pct(float(composite_signal.get("confidence") or 0.0) * 100.0))
            signal_contrib_frame = pd.DataFrame(composite_signal.get("contributions") or [])
            if signal_contrib_frame.empty:
                st.info("当前因子样本还不够，暂时无法给出合成分数。")
            else:
                st.dataframe(
                    signal_contrib_frame,
                    width="stretch",
                    hide_index=True,
                    column_config={"得分": st.column_config.NumberColumn(format="%.1f")},
                )
            st.caption("解释口径: `偏多推进 / 偏空推进` 更像趋势延续；`拥挤但衰竭 / 吸收中` 更像短线需要防冲高回落或杀跌回拉。")

        render_section(
            "已发生爆仓真值",
            "这里显示的是交易所公开接口已经确认发生的爆仓事件，不是模型推断区。",
            anchor_id="section-home-liquidation",
        )
        st.caption(
            _join_caption_parts(
                selected_trade_state,
                f"录制事件 {selected_replay_confidence}",
            )
        )
        flow = st.columns(4)
        flow[0].metric("盘口失衡", fmt_pct(book_summary.get("imbalance_pct")))
        flow[1].metric(f"近{liquidation_window_minutes}m 爆仓额", fmt_compact(liquidation_metrics.get("notional")))
        flow[2].metric(f"近{liquidation_window_minutes}m 爆仓单数", str(liquidation_metrics.get("count", 0)))
        flow[3].metric("最小价差", fmt_bps(book_summary.get("spread_bps")))
        st.caption(f"主导爆仓方向: {liquidation_metrics.get('dominant') or '暂无'}。接口不支持时会退化为会话内实时爆仓流。")
        liq_left, liq_right = st.columns([2.1, 1.35], gap="large")
        with liq_left:
            st.plotly_chart(
                build_liquidation_figure(liquidation_events),
                key=chart_key("liq", base_coin, selected_exchange, selected_symbol, interval),
                config=PLOTLY_CONFIG,
            )
        with liq_right:
            liq_frame = build_liquidation_frame(liquidation_events, limit=24)
            if liq_frame.empty:
                st.info("当前没有可展示的爆仓事件。")
            else:
                st.dataframe(liq_frame, width="stretch", hide_index=True, column_config={"价格": st.column_config.NumberColumn(format="%.2f"), "数量": st.column_config.NumberColumn(format="%.4f"), "名义金额": st.column_config.NumberColumn(format="%.2f")})
        truth_row = st.columns(6)
        truth_row[0].metric("多头爆仓额", fmt_compact(selected_liquidation_truth.get("long_notional")))
        truth_row[1].metric("空头爆仓额", fmt_compact(selected_liquidation_truth.get("short_notional")))
        truth_row[2].metric("多头爆仓单数", str(int(selected_liquidation_truth.get("long_count") or 0)))
        truth_row[3].metric("空头爆仓单数", str(int(selected_liquidation_truth.get("short_count") or 0)))
        truth_row[4].metric("30s 爆仓簇", str(int(selected_liquidation_truth.get("cluster_count") or 0)))
        truth_row[5].metric("跨所联动簇", str(int(selected_liquidation_truth.get("cross_cluster_count") or 0)))
        truth_cluster_left, truth_cluster_right = st.columns([1.65, 1.35], gap="large")
        with truth_cluster_left:
            st.plotly_chart(
                build_liquidation_cluster_figure(liquidation_events, cluster_window_seconds=30, limit=12),
                key=chart_key("liq-cluster", base_coin, selected_exchange, selected_symbol, interval),
                config=PLOTLY_CONFIG,
            )
        with truth_cluster_right:
            if selected_liquidation_clusters.empty:
                st.info("当前窗口里还没有形成 30 秒连续爆仓簇。")
            else:
                st.dataframe(
                    selected_liquidation_clusters,
                    width="stretch",
                    hide_index=True,
                    column_config={
                        "持续秒数": st.column_config.NumberColumn(format="%.1f"),
                        "多头爆仓额": st.column_config.NumberColumn(format="%.2f"),
                        "空头爆仓额": st.column_config.NumberColumn(format="%.2f"),
                        "总名义金额": st.column_config.NumberColumn(format="%.2f"),
                    },
                )
        st.caption("这里的 `多头爆仓 / 空头爆仓` 来自交易所已发生事件真值；`单所爆仓 / 跨所联动` 是按 30 秒窗口把这些真值簇聚类后的结果。")

        render_section("推断爆仓区", "这张图基于 Orderbook + Candles + OI + Funding 推断未来更容易发生爆仓的价带，不是交易所清算引擎真值。")
        st.caption(
            _join_caption_parts(
                selected_orderbook_state,
                selected_oi_state,
                f"推断置信度 {selected_risk_confidence}",
            )
        )
        st.plotly_chart(
            build_heatmap_figure(liquidation_heat_frame, "推断爆仓区", reference_price, LIQUIDATION_COLORSCALE, "数据不足，暂时无法推断潜在爆仓区。"),
            key=chart_key("riskmap", "liquidation", base_coin, selected_exchange, selected_symbol, interval),
            config=PLOTLY_CONFIG,
        )
        st.caption("左表 = 现价下方更像多头爆仓带；右表 = 现价上方更像空头爆仓带。")
        render_directional_zone_tables(
            directional_liquidation_zones["below"],
            directional_liquidation_zones["above"],
            "现价下方多头爆仓带 · Top 10",
            "现价上方空头爆仓带 · Top 10",
        )

        if desk_mode == "核心":
            st.info("当前是核心模式。推断止盈/止损、MBO、盘口质量和回放已经延后；需要时把深度页模式切到 `完整`。")
            return

        render_section("推断止盈区", "推断哪一带更可能出现公开仓位的止盈兑现，不是真实挂单簿里的 TP 真值。")
        st.caption(
            _join_caption_parts(
                selected_orderbook_state,
                selected_oi_state,
                f"推断置信度 {selected_risk_confidence}",
            )
        )
        st.plotly_chart(
            build_heatmap_figure(tp_heat_frame, "推断止盈区", reference_price, TP_COLORSCALE, "数据不足，暂时无法推断潜在止盈区。"),
            key=chart_key("riskmap", "tp", base_coin, selected_exchange, selected_symbol, interval),
            config=PLOTLY_CONFIG,
        )
        st.caption("左表 = 下方空头止盈；右表 = 上方多头止盈。")
        render_directional_zone_tables(
            directional_tp_zones["below"],
            directional_tp_zones["above"],
            "下方空头止盈 · Top 10",
            "上方多头止盈 · Top 10",
        )

        render_section("推断止损区", "推断止损池只是一种公开数据推断。这里会把多头止损和空头止损拆开，不再混排。")
        st.caption(
            _join_caption_parts(
                selected_orderbook_state,
                selected_oi_state,
                f"推断置信度 {selected_risk_confidence}",
            )
        )
        st.plotly_chart(
            build_heatmap_figure(stop_heat_frame, "推断止损区", reference_price, STOP_COLORSCALE, "数据不足，暂时无法推断潜在止损区。"),
            key=chart_key("riskmap", "stop", base_coin, selected_exchange, selected_symbol, interval),
            config=PLOTLY_CONFIG,
        )
        st.caption("左表 = 现价下方多头止损池；右表 = 现价上方空头止损池。")
        render_directional_zone_tables(
            directional_stop_zones["below"],
            directional_stop_zones["above"],
            "多头止损池 · 现价下方 Top 10",
            "空头止损池 · 现价上方 Top 10",
        )

        render_section("MBO Profile", "当前数据源仍然是公开 L2 聚合盘口，不是真正逐订单 ID 的私有级别 MBO。")
        st.caption(selected_orderbook_state)
        mbo_left, mbo_right = st.columns([2.1, 1.35], gap="large")
        with mbo_left:
            st.plotly_chart(
                build_mbo_figure(mbo_frame, reference_price),
                key=chart_key("mbo", base_coin, selected_exchange, selected_symbol, interval),
                config=PLOTLY_CONFIG,
            )
        with mbo_right:
            if mbo_frame.empty:
                st.info("当前盘口深度不足，暂时没有可展示的 MBO 梯级。")
            else:
                st.dataframe(mbo_frame[["方向", "价格", "挂单量", "名义金额", "盘口占比", "队列压力", "吸收分数"]], width="stretch", hide_index=True, column_config={"价格": st.column_config.NumberColumn(format="%.2f"), "挂单量": st.column_config.NumberColumn(format="%.4f"), "名义金额": st.column_config.NumberColumn(format="%.2f"), "盘口占比": st.column_config.ProgressColumn(format="%.2f", min_value=0.0, max_value=1.0), "吸收分数": st.column_config.ProgressColumn(format="%.2f", min_value=0.0, max_value=1.0)})

        render_section("盘口质量 / 撤单速度 / 假挂单 / 补单", "把新增挂单、撤单、净变化、墙体持续时间、假挂单和大单被吃后是否快速补单放到一起看。")
        st.caption(
            _join_caption_parts(
                selected_orderbook_state,
                f"簿面质量 {selected_quality_confidence}",
            )
        )
        quality_row = st.columns(5)
        latest_quality = quality_summary_frame.iloc[0].to_dict() if not quality_summary_frame.empty else {}
        quality_row[0].metric("新增挂单额", fmt_compact(latest_quality.get("新增挂单额")))
        quality_row[1].metric("撤单额", fmt_compact(latest_quality.get("撤单额")))
        quality_row[2].metric("近价撤单", fmt_compact(latest_quality.get("近价撤单")))
        quality_row[3].metric("假挂单次数", str(int(latest_quality.get("假挂单次数") or 0)))
        quality_row[4].metric("补单次数", str(int(latest_quality.get("补单次数") or 0)))
        quality_left, quality_right = st.columns([1.85, 1.15], gap="large")
        with quality_left:
            st.plotly_chart(
                build_orderbook_quality_figure(perp_quality_history, title=f"{selected_snapshot.exchange} Perp Orderbook Quality"),
                key=chart_key("book-quality", base_coin, selected_exchange, selected_symbol, interval),
                config=PLOTLY_CONFIG,
            )
        with quality_right:
            if quality_summary_frame.empty:
                st.info("等待本地订单簿增量样本。")
            else:
                st.dataframe(
                    quality_summary_frame,
                    width="stretch",
                    hide_index=True,
                    column_config={
                        "新增挂单额": st.column_config.NumberColumn(format="%.2f"),
                        "撤单额": st.column_config.NumberColumn(format="%.2f"),
                        "净变化": st.column_config.NumberColumn(format="%.2f"),
                        "近价新增": st.column_config.NumberColumn(format="%.2f"),
                        "近价撤单": st.column_config.NumberColumn(format="%.2f"),
                        "买墙持续(s)": st.column_config.NumberColumn(format="%.1f"),
                        "卖墙持续(s)": st.column_config.NumberColumn(format="%.1f"),
                        "盘口失衡(%)": st.column_config.NumberColumn(format="%.2f"),
                    },
                )
            if spot_symbol_map.get(selected_exchange):
                spot_quality_frame = build_orderbook_quality_frame(spot_quality_history, limit=8)
                if not spot_quality_frame.empty:
                    st.caption(f"{selected_snapshot.exchange} Spot 也在持续统计簿面质量，下面这张是最近 {len(spot_quality_frame)} 条现货样本。")
                    st.dataframe(
                        spot_quality_frame,
                        width="stretch",
                        hide_index=True,
                        column_config={
                            "新增挂单额": st.column_config.NumberColumn(format="%.2f"),
                            "撤单额": st.column_config.NumberColumn(format="%.2f"),
                            "净变化": st.column_config.NumberColumn(format="%.2f"),
                        },
                    )

        render_section("事件录制 / 复盘回放", "当前会自动录制会话内的成交、爆仓、OI 和簿面质量事件。可按最近两分钟或某个爆仓簇做 1x / 5x / 20x 回放。")
        st.caption(
            _join_caption_parts(
                selected_trade_state,
                f"回放 {selected_replay_confidence}",
                f"录制事件 {len(replay_events)} 条",
            )
        )
        replay_mode_options = ["最近 2 分钟"] + [
            f"{row['开始时间']} | {row['类别']} | {row['主导方向']} | {fmt_compact(row['总名义金额'])}"
            for _, row in selected_liquidation_clusters.iterrows()
        ]
        replay_mode = st.selectbox(
            "回放窗口",
            replay_mode_options,
            key=chart_key("replay-mode", base_coin, selected_exchange, selected_symbol, interval),
        )
        replay_speed = st.selectbox(
            "回放速度",
            ["1x", "5x", "20x"],
            index=1,
            key=chart_key("replay-speed", base_coin, selected_exchange, selected_symbol, interval),
        )
        speed_factor = int(replay_speed.replace("x", ""))
        if replay_mode == "最近 2 分钟":
            replay_start_ms = max(0, now_ms - 120_000)
            replay_end_ms = now_ms
        else:
            cluster_index = replay_mode_options.index(replay_mode) - 1
            cluster_row = selected_liquidation_clusters.iloc[cluster_index]
            cluster_start_ms = int(pd.Timestamp(cluster_row["开始时间"]).timestamp() * 1000)
            duration_ms = int(float(cluster_row["持续秒数"]) * 1000)
            replay_start_ms = max(0, cluster_start_ms - 120_000)
            replay_end_ms = cluster_start_ms + duration_ms + 120_000
        replay_progress = st.slider(
            "回放进度",
            min_value=0,
            max_value=100,
            value=100,
            step=5,
            key=chart_key("replay-progress", base_coin, selected_exchange, selected_symbol, interval),
        )
        replay_left, replay_right = st.columns([1.9, 1.1], gap="large")
        with replay_left:
            st.plotly_chart(
                build_replay_figure(replay_events, replay_start_ms, replay_end_ms, progress_ratio=replay_progress / 100.0),
                key=chart_key("replay-figure", base_coin, selected_exchange, selected_symbol, interval),
                config=PLOTLY_CONFIG,
            )
        with replay_right:
            replay_frame = build_recorded_event_frame(
                [
                    event
                    for event in replay_events
                    if replay_start_ms <= event.timestamp_ms <= replay_end_ms
                ],
                limit=36,
            )
            if replay_frame.empty:
                st.info("当前回放窗口里还没有录制到事件。")
            else:
                st.dataframe(
                    replay_frame,
                    width="stretch",
                    hide_index=True,
                    column_config={
                        "价格": st.column_config.NumberColumn(format="%.2f"),
                        "数量": st.column_config.NumberColumn(format="%.4f"),
                        "名义金额": st.column_config.NumberColumn(format="%.2f"),
                        "数值": st.column_config.NumberColumn(format="%.2f"),
                    },
                )
            replay_window_seconds = max(1.0, (replay_end_ms - replay_start_ms) / 1000.0)
            virtual_seconds = replay_window_seconds * (replay_progress / 100.0) / max(speed_factor, 1)
            st.caption(f"当前速度 {replay_speed}，窗口总长约 {replay_window_seconds:.0f}s，按当前进度相当于回放到 {virtual_seconds:.1f}s。这里先做交互式时间游标回放，方便你快速定位某次爆仓前后 2 分钟。")

    if active_view == "首页总览" and overview_mode == "完整":
        headline_exchange_keys = scope_perp_keys if exchange_scope_mode == "当前交易所优先" else list(EXCHANGE_ORDER)
        headline_large_trade_frame = build_large_trade_frame(
            {EXCHANGE_TITLES[key]: trade_events_by_exchange.get(key, []) for key in headline_exchange_keys},
            min_notional=max(75_000.0, float(selected_snapshot.last_price or 0.0) * 1.5),
            limit=10,
        )
        render_home_headline_marquee(
            headline_placeholder,
            build_home_headline_items(
                base_coin,
                snapshot_by_key,
                oi_metrics_by_exchange,
                liquidation_metrics_by_exchange,
                trade_metrics_by_exchange,
                exchange_keys=headline_exchange_keys,
                liquidation_window_minutes=liquidation_window_minutes,
                large_trade_frame=headline_large_trade_frame,
                confirmed_alert_frame=confirmed_alert_frame,
                kind_targets={
                    "overview": "section-home-exchange-compare",
                    "oi": "section-home-oi",
                    "liquidation": "section-home-liquidation",
                    "trade": "section-home-movers",
                    "large": "section-home-large-perp",
                    "alert": "section-home-movers",
                },
                limit=12,
            ),
        )
        render_section(
            "竞品级首页",
            "先看主结论、全市场总览板和异动榜，再决定要不要切到单币深度页。",
            anchor_id="section-home-hero",
        )
        conclusion_cards = st.columns(4)
        conclusion_cards[0].metric("当前主结论", str(composite_signal.get("label") or "-"))
        conclusion_cards[1].metric("短线驱动", selected_lead_lag_summary or "样本不足")
        conclusion_cards[2].metric("最高风险", strongest_alerts.iloc[0]["告警"] if not strongest_alerts.empty else "暂无强告警")
        conclusion_cards[3].metric("爆仓主导", str(selected_liquidation_truth.get("dominant") or "暂无"))
        for line in home_conclusions:
            st.markdown(f"- {line}")

        render_section("交易所健康 / 自动熔断", "把目录状态、REST 受限、WS 在线情况和自动冷却都集中放在这一层，云端部署时更容易看出到底是哪一段在降级。")
        health_cards = st.columns(4)
        health_cards[0].metric("目录受限所数", str(int(exchange_health_frame["目录"].astype(str).str.contains("目录受限", na=False).sum()) if not exchange_health_frame.empty else 0))
        health_cards[1].metric("REST 冷却所数", str(int((pd.to_numeric(exchange_health_frame["冷却剩余(s)"], errors="coerce").fillna(0) > 0).sum()) if not exchange_health_frame.empty else 0))
        health_cards[2].metric("WS 待修复", str(int(exchange_health_frame["说明"].astype(str).str.contains("WS待修复", na=False).sum()) if not exchange_health_frame.empty else 0))
        health_cards[3].metric("最近成功", exchange_health_frame["最近成功"].iloc[0] if not exchange_health_frame.empty else "-")
        st.dataframe(
            exchange_health_frame,
            width="stretch",
            hide_index=True,
            column_config={
                "连续失败": st.column_config.NumberColumn(format="%d"),
                "冷却剩余(s)": st.column_config.NumberColumn(format="%d"),
            },
        )

        render_section("统一评分层", "把现货/合约决策、情绪告警、份额变化和风险缓冲汇成一张总控板，先给一个主判断，再展开看原因。")
        score_cards = st.columns(4)
        score_cards[0].metric("最高评分", f"{float(unified_signal_frame.iloc[0]['统一评分']):.1f}" if not unified_signal_frame.empty else "-")
        score_cards[1].metric("最强方向", str(unified_signal_frame.iloc[0]["方向"]) if not unified_signal_frame.empty else "-")
        score_cards[2].metric("优先执行", str(unified_signal_frame.iloc[0]["执行偏好"]) if not unified_signal_frame.empty else "-")
        score_cards[3].metric("高分样本", str(int((pd.to_numeric(unified_signal_frame["统一评分"], errors='coerce').fillna(0.0) >= 70.0).sum()) if not unified_signal_frame.empty else 0))
        score_left, score_right = st.columns([1.0, 1.2], gap="large")
        with score_left:
            st.plotly_chart(
                build_unified_signal_figure(unified_signal_frame),
                key=chart_key("unified-signal-score", base_coin, interval, exchange_scope_mode),
                config=PLOTLY_CONFIG,
            )
        with score_right:
            st.dataframe(
                unified_signal_frame,
                width="stretch",
                hide_index=True,
                column_config={
                    "统一评分": st.column_config.NumberColumn(format="%.1f"),
                    "OI份额Δ(%)": st.column_config.NumberColumn(format="%.2f"),
                    "合约成交份额Δ(%)": st.column_config.NumberColumn(format="%.2f"),
                    "现货成交份额Δ(%)": st.column_config.NumberColumn(format="%.2f"),
                    "风险比": st.column_config.NumberColumn(format="%.2f"),
                },
            )

        render_section(
            "全市场总览板",
            "按币种把价格、OI、OI 1h/24h、Funding、爆仓样本、多空比、现货/合约成交比和 Lead/Lag 放在一张榜单里。",
            anchor_id="section-home-overview",
        )
        if overview_frame.empty:
            st.info("当前首页币种池还没有可展示的数据。")
        else:
            st.dataframe(
                overview_frame,
                width="stretch",
                hide_index=True,
                column_config={
                    "价格": st.column_config.NumberColumn(format="%.2f"),
                    "OI": st.column_config.NumberColumn(format="%.2f"),
                    "OI 1h(%)": st.column_config.NumberColumn(format="%.2f"),
                    "OI 24h(%)": st.column_config.NumberColumn(format="%.2f"),
                    "Funding(bps)": st.column_config.NumberColumn(format="%.2f"),
                    "24h爆仓样本额": st.column_config.NumberColumn(format="%.2f"),
                    "多空比": st.column_config.NumberColumn(format="%.3f"),
                    "现货/合约成交比": st.column_config.NumberColumn(format="%.2f"),
                    "置信度": st.column_config.ProgressColumn(format="%.1f", min_value=0.0, max_value=100.0),
                },
            )
            st.caption("这里的 `24h爆仓样本额` 目前基于公开接口可回拉到的爆仓样本，不等于全市场完整 24h 真值。")

        render_section(
            "异动榜",
            "把 OI 激增、爆仓放大、Funding 极值、合约情绪极值、现货先动、拥挤但衰竭 这六类异动拆开看。",
            anchor_id="section-home-movers",
        )
        oi_movers = build_movers_frame(overview_frame, "OI 1h(%)", limit=5, ascending=False)
        liq_movers = build_movers_frame(overview_frame, "24h爆仓样本额", limit=5, ascending=False)
        funding_movers = (
            overview_frame.assign(_abs_funding=overview_frame["Funding(bps)"].abs())
            .sort_values("_abs_funding", ascending=False)
            .drop(columns="_abs_funding")
            .head(5)
            .reset_index(drop=True)
            if not overview_frame.empty and "Funding(bps)" in overview_frame.columns
            else pd.DataFrame()
        )
        spot_leaders = (
            overview_frame[overview_frame["Lead/Lag"].astype(str).str.contains("现货领先", na=False)].head(5).reset_index(drop=True)
            if not overview_frame.empty
            else pd.DataFrame()
        )
        crowd_movers = (
            overview_frame.assign(_ratio_distance=(pd.to_numeric(overview_frame["多空比"], errors="coerce") - 1.0).abs())
            .sort_values("_ratio_distance", ascending=False)
            .drop(columns="_ratio_distance")
            .head(5)
            .reset_index(drop=True)
            if not overview_frame.empty and "多空比" in overview_frame.columns
            else pd.DataFrame()
        )
        exhausted_movers = build_movers_frame(overview_frame, "置信度", limit=5, ascending=False, title_filter="拥挤但衰竭")
        mover_specs = [
            ("OI 激增榜", oi_movers, "OI 1h(%)"),
            ("爆仓榜", liq_movers, "24h爆仓样本额"),
            ("Funding 极值榜", funding_movers, "Funding(bps)"),
            ("合约情绪极值榜", crowd_movers, "多空比"),
            ("现货带动榜", spot_leaders, "Lead/Lag"),
            ("拥挤但衰竭榜", exhausted_movers, "置信度"),
        ]
        mover_columns = st.columns(len(mover_specs), gap="medium")
        for column, (title, frame, focus_column) in zip(mover_columns, mover_specs):
            with column:
                st.caption(title)
                if frame.empty:
                    st.info("暂无")
                else:
                    display_columns = ["币种", "主结论", focus_column]
                    st.dataframe(frame[display_columns], width="stretch", hide_index=True)

        render_section(
            "全市场对比" if exchange_scope_mode == "全部交易所" else "当前交易所对比",
            "横向比较价格、持仓、费率和成交额。",
            anchor_id="section-home-exchange-compare",
        )
        st.dataframe(build_snapshot_frame(scope_snapshots if exchange_scope_mode == "当前交易所优先" else snapshots), width="stretch", hide_index=True)
        market_board_mode = render_choice_bar(
            "首页看板视角",
            ["综合看板", "现货看板", "合约看板"],
            chart_key("home-board-mode", base_coin, interval),
            default="综合看板",
        )
        st.caption("综合看板 = 现货和合约一起看；现货看板 = 只看现货流动性和成交；合约看板 = 只看 OI、Funding、爆仓和加减仓。")
        spot_dashboard_frame = build_spot_dashboard_frame(spot_snapshot_map, spot_summary_map, lead_lag_frame, exchange_keys=scope_spot_keys)
        perp_dashboard_frame = build_perp_dashboard_frame(snapshot_by_key, perp_summary_map, oi_metrics_by_exchange, contract_ratio_by_exchange, exchange_keys=scope_perp_keys)

        if market_board_mode in ("综合看板", "合约看板"):
            render_section(
                "持仓总量 / 未平仓对比",
                "公开 API 下，这里的未平仓就是合约 OI；现货市场不存在 OI 概念。",
                anchor_id="section-home-oi",
            )
            oi_left, oi_right = st.columns([1.6, 1.35], gap="large")
            with oi_left:
                st.plotly_chart(
                    build_open_interest_comparison_figure(scope_ok_snapshots if exchange_scope_mode == "当前交易所优先" else ok_snapshots),
                    key=chart_key("oi-compare", base_coin, interval, market_board_mode),
                    config=PLOTLY_CONFIG,
                )
            with oi_right:
                oi_frame = build_open_interest_frame(scope_ok_snapshots if exchange_scope_mode == "当前交易所优先" else ok_snapshots)
                if oi_frame.empty:
                    st.info("当前没有可展示的未平仓分布。")
                else:
                    st.dataframe(
                        oi_frame,
                        width="stretch",
                        hide_index=True,
                        column_config={
                            "持仓量": st.column_config.NumberColumn(format="%.4f"),
                            "持仓金额": st.column_config.NumberColumn(format="%.2f"),
                            "资金费率(bps)": st.column_config.NumberColumn(format="%.2f"),
                            "份额(%)": st.column_config.ProgressColumn(format="%.2f", min_value=0.0, max_value=100.0),
                        },
                    )
            oi_brief_lines = build_oi_brief_lines(
                base_coin,
                snapshot_by_key,
                oi_metrics_by_exchange,
                exchange_keys=scope_perp_keys if exchange_scope_mode == "当前交易所优先" else list(EXCHANGE_ORDER),
            )
            if oi_brief_lines:
                st.caption("OI 快报: " + " · ".join(oi_brief_lines))

        if market_board_mode == "现货看板":
            render_section(
                "三所现货看板" if exchange_scope_mode == "全部交易所" else "当前现货看板",
                "把 Binance / Bybit / OKX 的现货价格、价差、盘口和成交额单独拿出来看，不混入 OI 和 Funding。"
                if exchange_scope_mode == "全部交易所"
                else "只看当前现货焦点的价格、价差、盘口和成交额，不混入 OI 和 Funding。",
            )
            spot_cards = st.columns(4)
            spot_cards[0].metric("在线现货所", str(int(spot_dashboard_frame["现货价格"].notna().sum()) if not spot_dashboard_frame.empty else 0))
            spot_cards[1].metric("现货24h总成交", fmt_compact(spot_dashboard_frame["24h成交额"].sum() if not spot_dashboard_frame.empty else None))
            spot_cards[2].metric("最紧现货价差", fmt_bps(spot_dashboard_frame["现货价差(bps)"].min() if not spot_dashboard_frame.empty else None))
            spot_cards[3].metric("最大盘口失衡", fmt_pct(spot_dashboard_frame["盘口失衡(%)"].abs().max() if not spot_dashboard_frame.empty else None))
            st.dataframe(
                spot_dashboard_frame,
                width="stretch",
                hide_index=True,
                column_config={
                    "现货价格": st.column_config.NumberColumn(format="%.2f"),
                    "买一": st.column_config.NumberColumn(format="%.2f"),
                    "卖一": st.column_config.NumberColumn(format="%.2f"),
                    "现货价差(bps)": st.column_config.NumberColumn(format="%.2f"),
                    "盘口失衡(%)": st.column_config.NumberColumn(format="%.2f"),
                    "买盘挂单金额": st.column_config.NumberColumn(format="%.2f"),
                    "卖盘挂单金额": st.column_config.NumberColumn(format="%.2f"),
                    "24h成交额": st.column_config.NumberColumn(format="%.2f"),
                },
            )
            st.caption("现货看板不显示 OI、Funding、多空人数，因为这些口径本来就属于合约公开接口，不属于现货。")

        if market_board_mode == "合约看板":
            render_section(
                "三所合约看板" if exchange_scope_mode == "全部交易所" else "当前合约看板",
                "把合约价格、Funding、未平仓、OI 变化、加减仓状态和盘口压力单独放在一张板里。"
                if exchange_scope_mode == "全部交易所"
                else "只看当前合约交易所的价格、Funding、未平仓、OI 变化和盘口压力。",
            )
            perp_cards = st.columns(4)
            perp_cards[0].metric("在线合约所", str(int(perp_dashboard_frame["合约价格"].notna().sum()) if not perp_dashboard_frame.empty else 0))
            perp_cards[1].metric("未平仓总额", fmt_compact(perp_dashboard_frame["未平仓金额"].sum() if not perp_dashboard_frame.empty else None))
            perp_cards[2].metric("平均资金费率", fmt_bps(perp_dashboard_frame["资金费率(bps)"].mean() if not perp_dashboard_frame.empty else None))
            perp_cards[3].metric("最大 OI 变化", fmt_pct(perp_dashboard_frame["OI变化(%)"].abs().max() if not perp_dashboard_frame.empty else None))
            st.dataframe(
                perp_dashboard_frame,
                width="stretch",
                hide_index=True,
                column_config={
                    "合约价格": st.column_config.NumberColumn(format="%.2f"),
                    "资金费率(bps)": st.column_config.NumberColumn(format="%.2f"),
                    "未平仓量": st.column_config.NumberColumn(format="%.4f"),
                    "未平仓金额": st.column_config.NumberColumn(format="%.2f"),
                    "合约多空比": st.column_config.NumberColumn(format="%.3f"),
                    "OI变化(%)": st.column_config.NumberColumn(format="%.2f"),
                    "价格变化(%)": st.column_config.NumberColumn(format="%.2f"),
                    "合约价差(bps)": st.column_config.NumberColumn(format="%.2f"),
                    "盘口失衡(%)": st.column_config.NumberColumn(format="%.2f"),
                    "24h成交额": st.column_config.NumberColumn(format="%.2f"),
                },
            )
            st.caption("合约看板当前默认优先把 `多空比偏离 1.0 更明显` 的交易所排前面；如果多空比缺失，再回到未平仓金额排序。")

        if market_board_mode == "综合看板":
            combined_focus_label = EXCHANGE_TITLES.get(home_spot_exchange, home_spot_exchange.title())
            render_section(
                f"{combined_focus_label} 现货 vs 合约实时对照",
                "把当前焦点现货和对应永续放到一个面板里，直接看价格、Basis、挂单和实时流谁先动。"
                if exchange_scope_mode == "当前交易所优先"
                else "把 Binance 现货和 Binance 永续放到一个面板里，直接看价格、Basis、挂单和实时流谁先动。",
            )
            binance_lead_lag = lead_lag_frame[lead_lag_frame["交易所"] == EXCHANGE_TITLES.get(home_spot_exchange, home_spot_exchange.title())]
            binance_lead_row = binance_lead_lag.iloc[0].to_dict() if not binance_lead_lag.empty else {}
            spot_row = st.columns(6)
            spot_row[0].metric("现货价格", fmt_price(spot_snapshot.last_price if spot_snapshot.status == "ok" else None))
            spot_row[1].metric("永续价格", fmt_price(home_perp_snapshot.last_price))
            spot_row[2].metric("现货-永续 Basis", fmt_pct(spot_metrics.get("basis_pct")))
            spot_row[3].metric("永续/现货成交额比", "-" if spot_metrics.get("spot_volume_ratio") is None else f"{float(spot_metrics['spot_volume_ratio']):.2f}x")
            spot_row[4].metric("现货主动买占比", fmt_pct((spot_metrics.get("spot_buy_ratio") or 0.0) * 100 if spot_metrics.get("spot_buy_ratio") is not None else None))
            spot_row[5].metric("Lead / Lag", str(binance_lead_row.get("提示") or "样本不足"))
            spot_row = st.columns(4)
            spot_row[0].metric("现货价差", fmt_bps(spot_metrics.get("spot_spread_bps")))
            spot_row[1].metric("永续价差", fmt_bps(spot_metrics.get("perp_spread_bps")))
            spot_row[2].metric(f"{home_perp_snapshot.exchange} 合约 OI", fmt_compact(home_perp_snapshot.open_interest_notional))
            spot_row[3].metric("相关性", "-" if pd.isna(binance_lead_row.get("相关性")) else f"{float(binance_lead_row['相关性']):.2f}")
            spot_left, spot_right = st.columns([1.8, 1.2], gap="large")
            with spot_left:
                st.plotly_chart(
                    build_spot_perp_figure(spot_snapshot, home_perp_snapshot),
                    key=chart_key("spot-perp", base_coin, interval),
                    config=PLOTLY_CONFIG,
                )
            with spot_right:
                st.dataframe(
                    pd.DataFrame(
                        [
                            {"市场": f"{combined_focus_label} Spot", "买盘金额": spot_summary.get("bid_notional"), "卖盘金额": spot_summary.get("ask_notional"), "盘口失衡(%)": spot_summary.get("imbalance_pct"), "24h 成交额": spot_snapshot.volume_24h_notional if spot_snapshot.status == "ok" else None, "未平仓(OI)": None, "多空比": None},
                            {"市场": f"{home_perp_snapshot.exchange} Perp", "买盘金额": binance_perp_summary.get("bid_notional"), "卖盘金额": binance_perp_summary.get("ask_notional"), "盘口失衡(%)": binance_perp_summary.get("imbalance_pct"), "24h 成交额": home_perp_snapshot.volume_24h_notional, "未平仓(OI)": home_perp_snapshot.open_interest_notional, "多空比": contract_ratio_by_exchange.get(home_perp_exchange)},
                        ]
                    ),
                    width="stretch",
                    hide_index=True,
                    column_config={
                        "买盘金额": st.column_config.NumberColumn(format="%.2f"),
                        "卖盘金额": st.column_config.NumberColumn(format="%.2f"),
                        "盘口失衡(%)": st.column_config.NumberColumn(format="%.2f"),
                        "24h 成交额": st.column_config.NumberColumn(format="%.2f"),
                        "未平仓(OI)": st.column_config.NumberColumn(format="%.2f"),
                        "多空比": st.column_config.NumberColumn(format="%.3f"),
                    },
                )
                st.caption("现货没有未平仓量、资金费率和公开多空人数；这些口径只存在于合约公开接口里。")

            render_section("现货 vs 合约实时流", "现货和永续放在同一时间轴上看主动流向，判断现在是现货带动，还是合约先跑。")
            st.plotly_chart(
                build_spot_perp_flow_figure(spot_trades, perp_trades_map[home_perp_exchange], now_ms, max(30, liquidation_window_minutes)),
                key=chart_key("spot-perp-flow", base_coin, interval),
                config=PLOTLY_CONFIG,
            )
            st.caption("这块会优先使用当前焦点现货和对应合约的实时成交流，lead/lag 会跟着刷新。")

            render_section("现货 vs 合约横向对照", "把当前范围里的现货和对应永续放在一起比较：看哪一所现货更强、哪一所合约更拥挤、哪一所挂单更失衡。")
            exchange_left, exchange_right = st.columns([1.7, 1.25], gap="large")
            with exchange_left:
                st.plotly_chart(
                    build_spot_perp_exchange_figure(spot_exchange_frame),
                    key=chart_key("spot-perp-exchange", base_coin, interval),
                    config=PLOTLY_CONFIG,
                )
            with exchange_right:
                st.dataframe(
                    spot_exchange_frame,
                    width="stretch",
                    hide_index=True,
                    column_config={
                        "现货价格": st.column_config.NumberColumn(format="%.2f"),
                        "永续价格": st.column_config.NumberColumn(format="%.2f"),
                        "Basis(%)": st.column_config.NumberColumn(format="%.3f"),
                        "现货价差(bps)": st.column_config.NumberColumn(format="%.2f"),
                        "永续价差(bps)": st.column_config.NumberColumn(format="%.2f"),
                        "永续/现货成交额比": st.column_config.NumberColumn(format="%.2f"),
                        "现货主动买占比(%)": st.column_config.NumberColumn(format="%.2f"),
                        "现货盘口失衡(%)": st.column_config.NumberColumn(format="%.2f"),
                        "合约盘口失衡(%)": st.column_config.NumberColumn(format="%.2f"),
                        "现货24h成交额": st.column_config.NumberColumn(format="%.2f"),
                        "合约24h成交额": st.column_config.NumberColumn(format="%.2f"),
                        "合约持仓量": st.column_config.NumberColumn(format="%.4f"),
                        "合约持仓金额": st.column_config.NumberColumn(format="%.2f"),
                        "资金费率(bps)": st.column_config.NumberColumn(format="%.2f"),
                    },
                )
            lead_cards = st.columns(max(1, len(lead_lag_rows)))
            for column, row in zip(lead_cards, lead_lag_rows):
                column.metric(
                    f"{row['交易所']} lead/lag",
                    str(row.get("提示") or "样本不足"),
                    delta="-" if row.get("相关性") is None or pd.isna(row.get("相关性")) else f"相关性 {float(row['相关性']):.2f}",
                )
            st.caption("Lead / Lag 用秒级分桶收益率去估算谁先动。它更适合看短线先后手，不是严格的撮合级因果判定。")

            render_section("Spot-Perp 实时告警", "告警现在加入了连续 3 次确认、强/中/弱分级和时间线，不会再一闪一闪。")
            active_alert_frame = confirmed_alert_frame[confirmed_alert_frame["状态"] == "已确认"] if not confirmed_alert_frame.empty else confirmed_alert_frame
            alert_counts = active_alert_frame["等级"].value_counts() if active_alert_frame is not None and not active_alert_frame.empty else pd.Series(dtype=int)
            pending_count = 0 if confirmed_alert_frame.empty else int((confirmed_alert_frame["状态"].astype(str).str.contains("待确认")).sum())
            alert_cards = st.columns(4)
            alert_cards[0].metric("强告警", str(int(alert_counts.get("强", 0))))
            alert_cards[1].metric("中告警", str(int(alert_counts.get("中", 0))))
            alert_cards[2].metric("弱告警", str(int(alert_counts.get("弱", 0))))
            alert_cards[3].metric("待确认", str(pending_count))
            alert_left, alert_right = st.columns([1.7, 1.3], gap="large")
            with alert_left:
                st.plotly_chart(
                    build_alert_timeline_figure(alert_timeline_frame),
                    key=chart_key("alert-timeline", base_coin, interval),
                    config=PLOTLY_CONFIG,
                )
            with alert_right:
                st.dataframe(
                    confirmed_alert_frame,
                    width="stretch",
                    hide_index=True,
                    column_config={
                        "交易所": st.column_config.TextColumn(),
                        "等级": st.column_config.TextColumn(),
                        "告警": st.column_config.TextColumn(),
                        "解释": st.column_config.TextColumn(width="large"),
                        "连续触发": st.column_config.NumberColumn(format="%d"),
                        "状态": st.column_config.TextColumn(),
                    },
                )
            with st.expander("查看原始即时触发", expanded=False):
                st.dataframe(
                    spot_perp_alert_frame,
                    width="stretch",
                    hide_index=True,
                    column_config={
                        "交易所": st.column_config.TextColumn(),
                        "等级": st.column_config.TextColumn(),
                        "告警": st.column_config.TextColumn(),
                        "解释": st.column_config.TextColumn(width="large"),
                    },
                )

        if market_board_mode in ("现货看板", "综合看板"):
            spot_reference_price = payload_float(
                pd.Series(
                [
                    spot_snapshot_map[exchange_key].last_price
                    for exchange_key in scope_spot_keys
                    if exchange_key in spot_snapshot_map and spot_snapshot_map[exchange_key].status == "ok" and spot_snapshot_map[exchange_key].last_price is not None
                ]
                ).median()
            )
            spot_large_trade_threshold = max(25_000.0, float(spot_reference_price if spot_reference_price is not None else selected_snapshot.last_price or 0.0) * 0.6)
            spot_large_trade_payload = get_cached_derived_result(
                f"spot-large-heatmap::{base_coin}::{market_board_mode}",
                (
                    tuple(
                        (
                            exchange_key,
                            len(spot_trades_map.get(exchange_key, [])),
                            _latest_trade_timestamp(spot_trades_map.get(exchange_key)) or 0,
                        )
                        for exchange_key in scope_spot_keys
                    ),
                    round(float(spot_reference_price or 0.0), 4),
                    int(now_ms),
                    int(liquidation_window_minutes),
                    round(float(risk_heat_window_pct), 2),
                    int(risk_heat_buckets),
                ),
                ttl_seconds=balanced_ttl,
                builder=lambda: {
                    "heatmap_frame": build_event_heatmap_frame(
                        {EXCHANGE_TITLES[key]: spot_trades_map.get(key, []) for key in scope_spot_keys},
                        spot_reference_price,
                        now_ms=int(time.time() * 1000),
                        window_minutes=max(10, liquidation_window_minutes),
                        window_pct=max(4.0, risk_heat_window_pct),
                        bucket_count=max(12, risk_heat_buckets),
                        min_notional=spot_large_trade_threshold,
                        mode="trade",
                    ),
                    "large_trade_frame": build_large_trade_frame(
                        {EXCHANGE_TITLES[key]: spot_trades_map.get(key, []) for key in scope_spot_keys},
                        min_notional=spot_large_trade_threshold,
                        limit=30,
                    ),
                },
            )
            spot_large_trade_heatmap_frame = spot_large_trade_payload.get("heatmap_frame", pd.DataFrame())
            spot_large_trade_frame = spot_large_trade_payload.get("large_trade_frame", pd.DataFrame())
            render_section("现货大额热力图", "把现货大额成交按价格带聚合；绿色更偏主动买，红色更偏主动卖，和合约热力图区分开看。")
            spot_heat_row = st.columns(3)
            spot_heat_row[0].metric("现货大额样本额", fmt_compact(spot_large_trade_heatmap_frame["总名义金额"].sum() if not spot_large_trade_heatmap_frame.empty else None))
            spot_heat_row[1].metric("主动买价带", str(int((spot_large_trade_heatmap_frame["净名义金额"] > 0).sum()) if not spot_large_trade_heatmap_frame.empty else 0))
            spot_heat_row[2].metric("主动卖价带", str(int((spot_large_trade_heatmap_frame["净名义金额"] < 0).sum()) if not spot_large_trade_heatmap_frame.empty else 0))
            spot_heat_left, spot_heat_right = st.columns([1.55, 1.15], gap="large")
            with spot_heat_left:
                st.plotly_chart(
                    build_event_heatmap_figure(
                        spot_large_trade_heatmap_frame,
                        title="Spot Large Trade Heatmap",
                        positive_label="主动买",
                        negative_label="主动卖",
                    ),
                    key=chart_key("spot-large-heatmap", base_coin, interval, market_board_mode),
                    config=PLOTLY_CONFIG,
                )
            with spot_heat_right:
                if spot_large_trade_frame.empty:
                    st.info("当前现货大额成交样本还不够。")
                else:
                    st.dataframe(
                        spot_large_trade_frame.drop(columns=["侧向"], errors="ignore"),
                        width="stretch",
                        hide_index=True,
                        column_config={
                            "价格": st.column_config.NumberColumn(format="%.4f"),
                            "数量": st.column_config.NumberColumn(format="%.4f"),
                            "名义金额": st.column_config.NumberColumn(format="%.2f"),
                        },
                    )
            st.caption(f"现货大额热力图当前按最近 {max(10, liquidation_window_minutes)} 分钟、价格窗口 {max(4.0, risk_heat_window_pct):.1f}% 聚合。")

            spot_flow_display_frame = spot_flow_reference_frame.copy()
            if not spot_flow_display_frame.empty and "时间" in spot_flow_display_frame.columns:
                spot_flow_display_frame["时间"] = [format_display_timestamp_ms(value) for value in spot_flow_display_frame["时间"]]
            spot_execution_display_frame = spot_execution_frame.copy()
            if not spot_execution_display_frame.empty and "时间" in spot_execution_display_frame.columns:
                spot_execution_display_frame["时间"] = [format_display_timestamp_ms(value) for value in spot_execution_display_frame["时间"]]
            render_section("现货净主动流 / 执行质量参考层", "现货不做多空持仓真值，所以这里重点看净主动流、冲击成本、填充率和撤补速度。")
            spot_reference_cards = st.columns(4)
            strongest_spot_buy = (
                spot_flow_reference_frame.sort_values("主动买占比(%)", ascending=False).iloc[0]
                if not spot_flow_reference_frame.empty and "主动买占比(%)" in spot_flow_reference_frame.columns
                else {}
            )
            strongest_spot_sell = (
                spot_flow_reference_frame.sort_values("主动买占比(%)", ascending=True).iloc[0]
                if not spot_flow_reference_frame.empty and "主动买占比(%)" in spot_flow_reference_frame.columns
                else {}
            )
            best_execution_row = (
                spot_execution_frame.sort_values("50k冲击(bps)", ascending=True, na_position="last").iloc[0]
                if not spot_execution_frame.empty and "50k冲击(bps)" in spot_execution_frame.columns
                else {}
            )
            spot_reference_cards[0].metric("现货在线交易所", str(int(spot_flow_reference_frame["交易所"].nunique()) if not spot_flow_reference_frame.empty else 0))
            spot_reference_cards[1].metric(
                "主动买最强",
                str(strongest_spot_buy.get("交易所") or "-") if isinstance(strongest_spot_buy, pd.Series) else "-",
                "-" if not isinstance(strongest_spot_buy, pd.Series) else fmt_pct(strongest_spot_buy.get("主动买占比(%)")),
            )
            spot_reference_cards[2].metric(
                "主动卖最强",
                str(strongest_spot_sell.get("交易所") or "-") if isinstance(strongest_spot_sell, pd.Series) else "-",
                "-" if not isinstance(strongest_spot_sell, pd.Series) else fmt_pct(strongest_spot_sell.get("主动买占比(%)")),
            )
            spot_reference_cards[3].metric(
                "执行最紧",
                str(best_execution_row.get("交易所") or "-") if isinstance(best_execution_row, pd.Series) else "-",
                "-" if not isinstance(best_execution_row, pd.Series) else fmt_bps(best_execution_row.get("50k冲击(bps)")),
            )
            spot_reference_left, spot_reference_right = st.columns([1.35, 1.15], gap="large")
            with spot_reference_left:
                st.plotly_chart(
                    build_spot_flow_reference_figure(spot_flow_reference_frame, window_minutes=spot_reference_window_minutes),
                    key=chart_key("spot-flow-reference", base_coin, interval, exchange_scope_mode),
                    config=PLOTLY_CONFIG,
                )
            with spot_reference_right:
                st.plotly_chart(
                    build_execution_quality_figure(spot_execution_frame, title="Spot Execution Quality"),
                    key=chart_key("spot-execution-quality", base_coin, interval, exchange_scope_mode),
                    config=PLOTLY_CONFIG,
                )
            if spot_flow_display_frame.empty:
                st.info("当前现货参考层还没有足够的逐笔和盘口样本。")
            else:
                st.dataframe(
                    spot_flow_display_frame,
                    width="stretch",
                    hide_index=True,
                    column_config={
                        "现货价格": st.column_config.NumberColumn(format="%.4f"),
                        "24h成交额": st.column_config.NumberColumn(format="%.2f"),
                        f"{spot_reference_window_minutes}m净主动额": st.column_config.NumberColumn(format="%.2f"),
                        "主动买占比(%)": st.column_config.NumberColumn(format="%.2f"),
                        "价差(bps)": st.column_config.NumberColumn(format="%.2f"),
                        "盘口失衡(%)": st.column_config.NumberColumn(format="%.2f"),
                        "可见深度": st.column_config.NumberColumn(format="%.2f"),
                        "报价漂移(bps)": st.column_config.NumberColumn(format="%.2f"),
                        "报价波动(bps)": st.column_config.NumberColumn(format="%.2f"),
                        "50k冲击(bps)": st.column_config.NumberColumn(format="%.2f"),
                        "250k冲击(bps)": st.column_config.NumberColumn(format="%.2f"),
                        "50k填充率(%)": st.column_config.NumberColumn(format="%.1f"),
                        "250k填充率(%)": st.column_config.NumberColumn(format="%.1f"),
                    },
                )
            with st.expander("查看现货执行质量明细", expanded=False):
                if spot_execution_display_frame.empty:
                    st.info("当前现货执行质量明细还没有足够的盘口样本。")
                else:
                    st.dataframe(
                        spot_execution_display_frame,
                        width="stretch",
                        hide_index=True,
                        column_config={
                            "价格": st.column_config.NumberColumn(format="%.4f"),
                            "价差(bps)": st.column_config.NumberColumn(format="%.2f"),
                            "盘口失衡(%)": st.column_config.NumberColumn(format="%.2f"),
                            "可见深度": st.column_config.NumberColumn(format="%.2f"),
                            "50k冲击(bps)": st.column_config.NumberColumn(format="%.2f"),
                            "250k冲击(bps)": st.column_config.NumberColumn(format="%.2f"),
                            "50k填充率(%)": st.column_config.NumberColumn(format="%.1f"),
                            "250k填充率(%)": st.column_config.NumberColumn(format="%.1f"),
                            "近价新增额": st.column_config.NumberColumn(format="%.2f"),
                            "近价撤单额": st.column_config.NumberColumn(format="%.2f"),
                            "近价净变化": st.column_config.NumberColumn(format="%.2f"),
                            "近价撤补比": st.column_config.NumberColumn(format="%.2f"),
                            "报价漂移(bps)": st.column_config.NumberColumn(format="%.2f"),
                            "报价波动(bps)": st.column_config.NumberColumn(format="%.2f"),
                        },
                    )
            st.caption("现货参考层 = 净主动流 + 执行质量，不会把合约的 OI、Funding、多空人数硬套到现货上。")

        if market_board_mode == "现货看板":
            st.caption("现货看板到这里为止；OI、Funding、爆仓真值、加减仓和多空账户比这些是合约字段，已经留给 `合约看板` 和 `综合看板`。")
            return

        render_section("多时间框架 OI Delta / 加减仓矩阵", "同一个交易所，按 5m / 15m / 1h / 4h 四个时间框架同时看加仓减仓状态。")
        matrix_left, matrix_right = st.columns([1.7, 1.2], gap="large")
        with matrix_left:
            st.plotly_chart(
                build_oi_multiframe_matrix_figure(oi_matrix_metrics),
                key=chart_key("oi-matrix", base_coin, selected_exchange, interval),
                config=PLOTLY_CONFIG,
            )
        with matrix_right:
            matrix_frame = build_oi_multiframe_matrix_frame(oi_matrix_metrics)
            st.dataframe(
                matrix_frame,
                width="stretch",
                hide_index=True,
                column_config={
                    "价格变化(%)": st.column_config.NumberColumn(format="%.2f"),
                    "OI变化(%)": st.column_config.NumberColumn(format="%.2f"),
                    "置信度": st.column_config.ProgressColumn(format="%.2f", min_value=0.0, max_value=100.0),
                },
            )

        render_section("季度 / 交割合约期限结构", "当前先接 Binance basis 曲线，把永续、当季、次季放在一张结构图里。")
        term_left, term_right = st.columns([1.7, 1.2], gap="large")
        with term_left:
            st.plotly_chart(
                build_term_structure_figure(basis_payload),
                key=chart_key("term-structure", base_coin, interval),
                config=PLOTLY_CONFIG,
            )
        with term_right:
            term_frame = build_term_structure_frame(basis_payload)
            if term_frame.empty:
                st.info("当前没有可展示的期限结构数据。")
            else:
                st.dataframe(
                    term_frame,
                    width="stretch",
                    hide_index=True,
                    column_config={
                        "指数价": st.column_config.NumberColumn(format="%.2f"),
                        "期货价": st.column_config.NumberColumn(format="%.2f"),
                        "基差": st.column_config.NumberColumn(format="%.2f"),
                        "基差率(%)": st.column_config.NumberColumn(format="%.3f"),
                        "年化基差(%)": st.column_config.NumberColumn(format="%.3f"),
                    },
                )

        render_section("资金费率 / Basis", "把跨交易所 funding 和当前 perpetual premium 放到一张比较面板里。")
        compare_left, compare_right = st.columns(2, gap="large")
        with compare_left:
            st.plotly_chart(
                build_funding_comparison_figure(ok_snapshots),
                key=chart_key("funding-compare", base_coin, selected_symbol, interval),
                config=PLOTLY_CONFIG,
            )
        with compare_right:
            st.plotly_chart(
                build_basis_comparison_figure(ok_snapshots),
                key=chart_key("basis-compare", base_coin, selected_symbol, interval),
                config=PLOTLY_CONFIG,
            )

        render_carry_surface_panel(
            carry_surface_frame,
            key_scope=chart_key("carry-home", base_coin, exchange_scope_mode, interval),
        )

        render_section("Hyperliquid Spot / Perp 一体层", "把 Hyperliquid 现货、永续、地址库存和 OI 上限占用放到同一层，方便直接看链上现货和合约是否同向。")
        hyperliquid_context_display_frame = hyperliquid_spot_perp_context_frame.copy()
        if not hyperliquid_context_display_frame.empty and "时间" in hyperliquid_context_display_frame.columns:
            hyperliquid_context_display_frame["时间"] = [format_display_timestamp_ms(value) for value in hyperliquid_context_display_frame["时间"]]
        hyper_unified_cards = st.columns(4)
        hyper_unified_cards[0].metric("现货腿数", str(int(hyperliquid_spot_perp_context_frame["市场"].astype(str).eq("现货").sum()) if not hyperliquid_spot_perp_context_frame.empty else 0))
        hyper_unified_cards[1].metric("永续腿数", str(int(hyperliquid_spot_perp_context_frame["市场"].astype(str).eq("合约").sum()) if not hyperliquid_spot_perp_context_frame.empty else 0))
        hyper_unified_cards[2].metric(
            "地址持仓值",
            fmt_compact(
                payload_float(
                    hyperliquid_spot_perp_context_frame["地址持仓值"].dropna().sum()
                    if not hyperliquid_spot_perp_context_frame.empty and "地址持仓值" in hyperliquid_spot_perp_context_frame.columns
                    else None
                )
            ),
        )
        hyper_unified_cards[3].metric(
            "最高 OI 上限占用",
            fmt_pct(
                hyperliquid_spot_perp_context_frame.loc[
                    hyperliquid_spot_perp_context_frame["市场"].astype(str).eq("合约"),
                    "OI上限占用(%)",
                ].max()
                if not hyperliquid_spot_perp_context_frame.empty
                else None
            ),
        )
        hyper_unified_left, hyper_unified_right = st.columns([1.18, 1.02], gap="large")
        with hyper_unified_left:
            st.plotly_chart(
                build_hyperliquid_spot_perp_context_figure(hyperliquid_spot_perp_context_frame),
                key=chart_key("home-hl-spot-perp-context", base_coin, interval, exchange_scope_mode),
                config=PLOTLY_CONFIG,
            )
        with hyper_unified_right:
            if hyperliquid_context_display_frame.empty:
                st.info("当前没有足够的 Hyperliquid spot / perp 一体样本。")
            else:
                st.dataframe(
                    hyperliquid_context_display_frame,
                    width="stretch",
                    hide_index=True,
                    column_config={
                        "价格": st.column_config.NumberColumn(format="%.4f"),
                        "24h成交额": st.column_config.NumberColumn(format="%.2f"),
                        "Funding(bps)": st.column_config.NumberColumn(format="%.2f"),
                        "Premium(%)": st.column_config.NumberColumn(format="%.3f"),
                        "未平仓金额": st.column_config.NumberColumn(format="%.2f"),
                        "地址余额": st.column_config.NumberColumn(format="%.4f"),
                        "地址成本": st.column_config.NumberColumn(format="%.2f"),
                        "地址持仓值": st.column_config.NumberColumn(format="%.2f"),
                        "OI上限占用(%)": st.column_config.NumberColumn(format="%.2f"),
                        "OI上限": st.column_config.NumberColumn(format="%.2f"),
                        "全局OI上限": st.column_config.NumberColumn(format="%.2f"),
                    },
                )

        contract_sentiment_display_frame = contract_sentiment_frame.copy()
        if not contract_sentiment_display_frame.empty and "时间" in contract_sentiment_display_frame.columns:
            contract_sentiment_display_frame["时间"] = [format_display_timestamp_ms(value) for value in contract_sentiment_display_frame["时间"]]
        render_section("合约情绪真值层", "公开多空比当前只对 Binance / Bybit 可用；OKX / Hyperliquid 先展示 OI、Funding 和主动流代理，不和现货口径混在一起。")
        sentiment_row = st.columns(5)
        available_ratio_frame = contract_sentiment_frame.dropna(subset=["合约多空比"]) if not contract_sentiment_frame.empty and "合约多空比" in contract_sentiment_frame.columns else pd.DataFrame()
        strongest_long_row = (
            available_ratio_frame.sort_values("合约多空比", ascending=False).iloc[0]
            if not available_ratio_frame.empty
            else {}
        )
        strongest_short_row = (
            available_ratio_frame.sort_values("合约多空比", ascending=True).iloc[0]
            if not available_ratio_frame.empty
            else {}
        )
        active_flow_row = (
            contract_sentiment_frame.reindex(
                contract_sentiment_frame["主动流买占比(%)"].sub(50.0).abs().sort_values(ascending=False, na_position="last").index
            ).iloc[0]
            if not contract_sentiment_frame.empty and "主动流买占比(%)" in contract_sentiment_frame.columns
            else {}
        )
        strongest_resonance_row = (
            contract_sentiment_frame.sort_values("共振评分", ascending=False, na_position="last").iloc[0]
            if not contract_sentiment_frame.empty and "共振评分" in contract_sentiment_frame.columns
            else {}
        )
        sentiment_row[0].metric("公开多空比所数", str(int(contract_sentiment_frame["合约多空比"].notna().sum()) if not contract_sentiment_frame.empty else 0))
        sentiment_row[1].metric(
            "偏多最强",
            str(strongest_long_row.get("交易所") or "-") if isinstance(strongest_long_row, pd.Series) else "-",
            "-" if not isinstance(strongest_long_row, pd.Series) or strongest_long_row.get("合约多空比") is None else f"{float(strongest_long_row.get('合约多空比')):.3f}",
        )
        sentiment_row[2].metric(
            "偏空最强",
            str(strongest_short_row.get("交易所") or "-") if isinstance(strongest_short_row, pd.Series) else "-",
            "-" if not isinstance(strongest_short_row, pd.Series) or strongest_short_row.get("合约多空比") is None else f"{float(strongest_short_row.get('合约多空比')):.3f}",
        )
        sentiment_row[3].metric(
            "主动流最偏单边",
            str(active_flow_row.get("交易所") or "-") if isinstance(active_flow_row, pd.Series) else "-",
            "-" if not isinstance(active_flow_row, pd.Series) else fmt_pct(active_flow_row.get("主动流买占比(%)")),
        )
        sentiment_row[4].metric(
            "最强共振",
            str(strongest_resonance_row.get("交易所") or "-") if isinstance(strongest_resonance_row, pd.Series) else "-",
            "-"
            if not isinstance(strongest_resonance_row, pd.Series)
            else (
                f"{float(strongest_resonance_row.get('共振评分') or 0.0):.1f} · {str(strongest_resonance_row.get('真值层级') or '-')}"
            ),
        )
        sentiment_left, sentiment_right = st.columns([1.05, 1.15], gap="large")
        with sentiment_left:
            st.plotly_chart(
                build_contract_sentiment_truth_figure(contract_sentiment_frame),
                key=chart_key("contract-sentiment-truth", base_coin, interval, exchange_scope_mode),
                config=PLOTLY_CONFIG,
            )
        with sentiment_right:
            st.plotly_chart(
                build_contract_ratio_history_figure(contract_sentiment_payloads, EXCHANGE_TITLES),
                key=chart_key("contract-sentiment-history", base_coin, interval, exchange_scope_mode),
                config=PLOTLY_CONFIG,
            )
        if contract_sentiment_display_frame.empty:
            st.info("当前合约情绪真值层还没有足够的公开样本。")
        else:
            st.dataframe(
                contract_sentiment_display_frame,
                width="stretch",
                hide_index=True,
                column_config={
                    "价格": st.column_config.NumberColumn(format="%.2f"),
                    "未平仓金额": st.column_config.NumberColumn(format="%.2f"),
                    "OI份额(%)": st.column_config.ProgressColumn(format="%.2f", min_value=0.0, max_value=100.0),
                    "资金费率(bps)": st.column_config.NumberColumn(format="%.2f"),
                    "合约多空比": st.column_config.NumberColumn(format="%.3f"),
                    "大户持仓多空比": st.column_config.NumberColumn(format="%.3f"),
                    "大户账户多空比": st.column_config.NumberColumn(format="%.3f"),
                    "全市场账户多空比": st.column_config.NumberColumn(format="%.3f"),
                    "账户多头占比(%)": st.column_config.NumberColumn(format="%.2f"),
                    "账户空头占比(%)": st.column_config.NumberColumn(format="%.2f"),
                    "主动流买占比(%)": st.column_config.NumberColumn(format="%.2f"),
                    "主动买卖比": st.column_config.NumberColumn(format="%.2f"),
                    "共振评分": st.column_config.NumberColumn(format="%.1f"),
                    "真值层级": st.column_config.TextColumn(),
                },
            )
        sentiment_alert_left, sentiment_alert_right = st.columns([1.2, 1.0], gap="large")
        with sentiment_alert_left:
            sentiment_alert_counts = contract_sentiment_alert_frame["等级"].value_counts() if not contract_sentiment_alert_frame.empty else pd.Series(dtype=int)
            alert_cards = st.columns(3)
            alert_cards[0].metric("高优先级", str(int(sentiment_alert_counts.get("高", 0))))
            alert_cards[1].metric("中优先级", str(int(sentiment_alert_counts.get("中", 0))))
            alert_cards[2].metric("观察", str(int(sentiment_alert_counts.get("观察", 0))))
            st.dataframe(
                contract_sentiment_alert_frame,
                width="stretch",
                hide_index=True,
                column_config={
                    "交易所": st.column_config.TextColumn(),
                    "等级": st.column_config.TextColumn(),
                    "告警": st.column_config.TextColumn(),
                    "解释": st.column_config.TextColumn(width="large"),
                    "数据口径": st.column_config.TextColumn(width="medium"),
                },
            )
        with sentiment_alert_right:
            st.caption("这一栏现在会同时扫描 Binance + Bybit 的公开多空比；OKX / Hyperliquid 仍然只做代理观察，不会冒充真值。")
            st.markdown(
                "- `高`：多空比与 funding 同向共振，或者多空比极值明显。\n"
                "- `中`：单边主动流、主动买卖比或共振评分明显偏斜。\n"
                "- `观察`：当前只有代理口径，先做风险提示，不当成真拥挤。"
            )

        perp_crowding_display_frame = perp_crowding_trio_frame.copy()
        if not perp_crowding_display_frame.empty and "时间" in perp_crowding_display_frame.columns:
            perp_crowding_display_frame["时间"] = [format_display_timestamp_ms(value) for value in perp_crowding_display_frame["时间"]]
        funding_regime_display_frame = funding_regime_frame.copy()
        if not funding_regime_display_frame.empty:
            if "时间" in funding_regime_display_frame.columns:
                funding_regime_display_frame["时间"] = [format_display_timestamp_ms(value) for value in funding_regime_display_frame["时间"]]
            if "下次Funding时间" in funding_regime_display_frame.columns:
                funding_regime_display_frame["下次Funding时间"] = [format_display_timestamp_ms(value) for value in funding_regime_display_frame["下次Funding时间"]]
        perp_execution_display_frame = perp_execution_frame.copy()
        if not perp_execution_display_frame.empty and "时间" in perp_execution_display_frame.columns:
            perp_execution_display_frame["时间"] = [format_display_timestamp_ms(value) for value in perp_execution_display_frame["时间"]]
        risk_buffer_display_frame = risk_buffer_frame.copy()
        if not risk_buffer_display_frame.empty and "下次Funding时间" in risk_buffer_display_frame.columns:
            risk_buffer_display_frame["下次Funding时间"] = [format_display_timestamp_ms(value) for value in risk_buffer_display_frame["下次Funding时间"]]

        render_section("合约拥挤度三件套 / Funding 状态机", "把公开多空比、OI 变化、主动流、Funding、Premium 和 Basis 放在同一层，分清是拥挤推进还是只是费率噪音。")
        crowding_cards = st.columns(4)
        strongest_crowding = (
            perp_crowding_trio_frame.iloc[0]
            if not perp_crowding_trio_frame.empty
            else {}
        )
        strongest_regime = (
            funding_regime_frame.iloc[0]
            if not funding_regime_frame.empty
            else {}
        )
        best_perp_execution = (
            perp_execution_frame.sort_values("50k冲击(bps)", ascending=True, na_position="last").iloc[0]
            if not perp_execution_frame.empty and "50k冲击(bps)" in perp_execution_frame.columns
            else {}
        )
        crowding_cards[0].metric("拥挤样本所数", str(int(perp_crowding_trio_frame["交易所"].nunique()) if not perp_crowding_trio_frame.empty else 0))
        crowding_cards[1].metric(
            "最拥挤交易所",
            str(strongest_crowding.get("交易所") or "-") if isinstance(strongest_crowding, pd.Series) else "-",
            "-" if not isinstance(strongest_crowding, pd.Series) else str(strongest_crowding.get("拥挤状态") or "-"),
        )
        crowding_cards[2].metric(
            "Funding 最极端",
            str(strongest_regime.get("交易所") or "-") if isinstance(strongest_regime, pd.Series) else "-",
            "-" if not isinstance(strongest_regime, pd.Series) else fmt_bps(strongest_regime.get("Funding(bps)")),
        )
        crowding_cards[3].metric(
            "合约执行最紧",
            str(best_perp_execution.get("交易所") or "-") if isinstance(best_perp_execution, pd.Series) else "-",
            "-" if not isinstance(best_perp_execution, pd.Series) else fmt_bps(best_perp_execution.get("50k冲击(bps)")),
        )
        crowding_left, crowding_right = st.columns([1.18, 1.12], gap="large")
        with crowding_left:
            st.plotly_chart(
                build_perp_crowding_trio_figure(perp_crowding_trio_frame),
                key=chart_key("perp-crowding-trio", base_coin, interval, exchange_scope_mode),
                config=PLOTLY_CONFIG,
            )
        with crowding_right:
            st.plotly_chart(
                build_funding_regime_figure(funding_regime_frame),
                key=chart_key("funding-regime", base_coin, interval, exchange_scope_mode),
                config=PLOTLY_CONFIG,
            )
        if perp_crowding_display_frame.empty:
            st.info("当前合约拥挤度三件套还没有足够的公开样本。")
        else:
            st.dataframe(
                perp_crowding_display_frame,
                width="stretch",
                hide_index=True,
                column_config={
                    "合约多空比": st.column_config.NumberColumn(format="%.3f"),
                    "OI变化(%)": st.column_config.NumberColumn(format="%.2f"),
                    "主动流买占比(%)": st.column_config.NumberColumn(format="%.2f"),
                    "资金费率(bps)": st.column_config.NumberColumn(format="%.2f"),
                },
            )
        with st.expander("查看 Funding / Premium / 合约执行 明细", expanded=False):
            funding_detail_left, funding_detail_right = st.columns([1.15, 1.0], gap="large")
            with funding_detail_left:
                if funding_regime_display_frame.empty:
                    st.info("当前 Funding / Premium 状态机还没有样本。")
                else:
                    st.dataframe(
                        funding_regime_display_frame,
                        width="stretch",
                        hide_index=True,
                        column_config={
                            "价格": st.column_config.NumberColumn(format="%.2f"),
                            "Premium(%)": st.column_config.NumberColumn(format="%.3f"),
                            "Basis(%)": st.column_config.NumberColumn(format="%.3f"),
                            "基差率(%)": st.column_config.NumberColumn(format="%.3f"),
                            "Funding(bps)": st.column_config.NumberColumn(format="%.2f"),
                            "下一次Funding(bps)": st.column_config.NumberColumn(format="%.2f"),
                            "Funding带宽(bps)": st.column_config.NumberColumn(format="%.2f"),
                            "Carry状态": st.column_config.TextColumn(),
                            "距下次Funding(min)": st.column_config.NumberColumn(format="%.1f"),
                        },
                    )
            with funding_detail_right:
                if perp_execution_display_frame.empty:
                    st.info("当前合约执行质量明细还没有足够的盘口样本。")
                else:
                    st.dataframe(
                        perp_execution_display_frame,
                        width="stretch",
                        hide_index=True,
                        column_config={
                            "价格": st.column_config.NumberColumn(format="%.2f"),
                            "价差(bps)": st.column_config.NumberColumn(format="%.2f"),
                            "盘口失衡(%)": st.column_config.NumberColumn(format="%.2f"),
                            "可见深度": st.column_config.NumberColumn(format="%.2f"),
                            "50k冲击(bps)": st.column_config.NumberColumn(format="%.2f"),
                            "250k冲击(bps)": st.column_config.NumberColumn(format="%.2f"),
                            "50k填充率(%)": st.column_config.NumberColumn(format="%.1f"),
                            "250k填充率(%)": st.column_config.NumberColumn(format="%.1f"),
                            "近价新增额": st.column_config.NumberColumn(format="%.2f"),
                            "近价撤单额": st.column_config.NumberColumn(format="%.2f"),
                            "近价净变化": st.column_config.NumberColumn(format="%.2f"),
                            "近价撤补比": st.column_config.NumberColumn(format="%.2f"),
                            "报价漂移(bps)": st.column_config.NumberColumn(format="%.2f"),
                            "报价波动(bps)": st.column_config.NumberColumn(format="%.2f"),
                        },
                    )

        render_section("现货先动 / 合约追价 决策板", "把现货主动流、Lead/Lag、合约主动流、多空比、OI 和 Funding 汇成一张判断板，先看谁在主导，再决定要不要追。")
        decision_cards = st.columns(4)
        decision_cards[0].metric("高置信判断", str(int(spot_perp_decision_frame["置信度"].astype(str).eq("高置信").sum()) if not spot_perp_decision_frame.empty else 0))
        decision_cards[1].metric("现货先动", str(int(spot_perp_decision_frame["主导判断"].astype(str).eq("现货先动").sum()) if not spot_perp_decision_frame.empty else 0))
        decision_cards[2].metric("合约追价/抢跑", str(int(spot_perp_decision_frame["主导判断"].astype(str).isin(["合约追价", "合约抢跑"]).sum()) if not spot_perp_decision_frame.empty else 0))
        decision_cards[3].metric(
            "最清晰样本",
            str(spot_perp_decision_frame.iloc[0]["交易所"]) if not spot_perp_decision_frame.empty else "-",
            str(spot_perp_decision_frame.iloc[0]["主导判断"]) if not spot_perp_decision_frame.empty else "-",
        )
        if spot_perp_decision_frame.empty:
            st.info("当前样本还不足以形成稳定的现货 / 合约决策板。")
        else:
            st.dataframe(
                spot_perp_decision_frame,
                width="stretch",
                hide_index=True,
                column_config={
                    "领先秒数": st.column_config.NumberColumn(format="%.2f"),
                    "相关性": st.column_config.NumberColumn(format="%.2f"),
                    "现货主动买占比(%)": st.column_config.NumberColumn(format="%.2f"),
                    "合约主动买占比(%)": st.column_config.NumberColumn(format="%.2f"),
                    "合约多空比": st.column_config.NumberColumn(format="%.3f"),
                    "OI变化(%)": st.column_config.NumberColumn(format="%.2f"),
                    "Funding(bps)": st.column_config.NumberColumn(format="%.2f"),
                    "Basis(%)": st.column_config.NumberColumn(format="%.3f"),
                    "永续/现货成交额比": st.column_config.NumberColumn(format="%.2f"),
                },
            )
            st.caption("这块是决策参考层，不是交易建议。它更适合帮你先分清：现在是现货先动、合约跟价，还是合约自己在抢跑。")

        render_section("执行层 / 路由矩阵", "把现货和合约放到同一张执行表里，直接比较去哪一所、走哪个市场更省滑点。")
        execution_cards = st.columns(4)
        execution_cards[0].metric("最佳路由", str(execution_route_frame.iloc[0]["路由"]) if not execution_route_frame.empty else "-")
        execution_cards[1].metric("最高执行评分", f"{float(execution_route_frame.iloc[0]['执行评分']):.1f}" if not execution_route_frame.empty else "-")
        execution_cards[2].metric("优先现货", str(int(execution_route_frame["执行建议"].astype(str).eq("优先现货").sum()) if not execution_route_frame.empty else 0))
        execution_cards[3].metric("优先合约", str(int(execution_route_frame["执行建议"].astype(str).eq("优先合约").sum()) if not execution_route_frame.empty else 0))
        execution_left, execution_right = st.columns([1.0, 1.2], gap="large")
        with execution_left:
            execution_figure_frame = execution_route_frame.copy()
            if not execution_figure_frame.empty and "路由" in execution_figure_frame.columns:
                execution_figure_frame["交易所"] = execution_figure_frame["路由"]
            st.plotly_chart(
                build_execution_quality_figure(execution_figure_frame, title="Execution Route Matrix"),
                key=chart_key("execution-route-matrix", base_coin, interval, exchange_scope_mode),
                config=PLOTLY_CONFIG,
            )
        with execution_right:
            st.dataframe(
                execution_route_frame,
                width="stretch",
                hide_index=True,
                column_config={
                    "执行评分": st.column_config.NumberColumn(format="%.1f"),
                    "价差(bps)": st.column_config.NumberColumn(format="%.2f"),
                    "50k冲击(bps)": st.column_config.NumberColumn(format="%.2f"),
                    "250k冲击(bps)": st.column_config.NumberColumn(format="%.2f"),
                    "50k填充率(%)": st.column_config.NumberColumn(format="%.1f"),
                    "250k填充率(%)": st.column_config.NumberColumn(format="%.1f"),
                    "可见深度": st.column_config.NumberColumn(format="%.2f"),
                },
            )

        render_section("跨所传导路径板", "不只看谁先动，还看份额、价差和爆仓簇怎么在交易所之间扩散。")
        propagation_cards = st.columns(4)
        propagation_cards[0].metric("主导路径", str(propagation_frame.iloc[0]["交易所"]) if not propagation_frame.empty else "-")
        propagation_cards[1].metric("主导角色", str(propagation_frame.iloc[0]["角色"]) if not propagation_frame.empty else "-")
        propagation_cards[2].metric("最高传导评分", f"{float(propagation_frame.iloc[0]['传导评分']):.1f}" if not propagation_frame.empty else "-")
        propagation_cards[3].metric("爆仓传导所数", str(int(propagation_frame['角色'].astype(str).eq('爆仓传导').sum()) if not propagation_frame.empty else 0))
        propagation_left, propagation_right = st.columns([1.0, 1.2], gap="large")
        with propagation_left:
            st.plotly_chart(
                build_cross_exchange_propagation_figure(propagation_frame),
                key=chart_key("cross-exchange-propagation", base_coin, interval, exchange_scope_mode),
                config=PLOTLY_CONFIG,
            )
        with propagation_right:
            st.dataframe(
                propagation_frame,
                width="stretch",
                hide_index=True,
                column_config={
                    "传导评分": st.column_config.NumberColumn(format="%.1f"),
                    "现货份额Δ(%)": st.column_config.NumberColumn(format="%.2f"),
                    "合约份额Δ(%)": st.column_config.NumberColumn(format="%.2f"),
                    "OI份额Δ(%)": st.column_config.NumberColumn(format="%.2f"),
                    "价差偏离(bps)": st.column_config.NumberColumn(format="%.2f"),
                },
            )

        perp_reference_price = payload_float(
            pd.Series(
            [
                snapshot_by_key[exchange_key].last_price
                for exchange_key in scope_perp_keys
                if exchange_key in snapshot_by_key and snapshot_by_key[exchange_key].status == "ok" and snapshot_by_key[exchange_key].last_price is not None
            ]
            ).median()
        )
        perp_large_trade_threshold = max(75_000.0, float(perp_reference_price if perp_reference_price is not None else selected_snapshot.last_price or 0.0) * 1.5)
        contract_heatmap_payload = get_cached_derived_result(
            f"perp-heatmap::{base_coin}::{exchange_scope_mode}",
            (
                tuple(
                    (
                        exchange_key,
                        len(trade_events_by_exchange.get(exchange_key, [])),
                        _latest_trade_timestamp(trade_events_by_exchange.get(exchange_key, [])) or 0,
                        len(liquidation_events_by_exchange.get(exchange_key, [])),
                        liquidation_events_by_exchange.get(exchange_key)[-1].timestamp_ms if liquidation_events_by_exchange.get(exchange_key) else 0,
                    )
                    for exchange_key in scope_perp_keys
                ),
                round(float(perp_reference_price or 0.0), 4),
                int(now_ms),
                int(liquidation_window_minutes),
                round(float(risk_heat_window_pct), 2),
                int(risk_heat_buckets),
            ),
            ttl_seconds=balanced_ttl,
            builder=lambda: {
                "trade_heatmap_frame": build_event_heatmap_frame(
                    {EXCHANGE_TITLES[key]: trade_events_by_exchange.get(key, []) for key in scope_perp_keys},
                    perp_reference_price,
                    now_ms=int(time.time() * 1000),
                    window_minutes=max(10, liquidation_window_minutes),
                    window_pct=max(4.0, risk_heat_window_pct),
                    bucket_count=max(12, risk_heat_buckets),
                    min_notional=perp_large_trade_threshold,
                    mode="trade",
                ),
                "liquidation_heatmap_frame": build_event_heatmap_frame(
                    {EXCHANGE_TITLES[key]: liquidation_events_by_exchange.get(key, []) for key in scope_perp_keys},
                    perp_reference_price,
                    now_ms=int(time.time() * 1000),
                    window_minutes=max(10, liquidation_window_minutes),
                    window_pct=max(4.0, risk_heat_window_pct),
                    bucket_count=max(12, risk_heat_buckets),
                    min_notional=max(20_000.0, perp_large_trade_threshold * 0.3),
                    mode="liquidation",
                ),
                "large_trade_frame": build_large_trade_frame(
                    {EXCHANGE_TITLES[key]: trade_events_by_exchange.get(key, []) for key in scope_perp_keys},
                    min_notional=perp_large_trade_threshold,
                    limit=36,
                ),
            },
        )
        perp_large_trade_heatmap_frame = contract_heatmap_payload.get("trade_heatmap_frame", pd.DataFrame())
        perp_liquidation_heatmap_frame = contract_heatmap_payload.get("liquidation_heatmap_frame", pd.DataFrame())
        perp_large_trade_frame = contract_heatmap_payload.get("large_trade_frame", pd.DataFrame())
        render_section(
            "合约大额热力图 / 清算热力图 2.0",
            "把合约大额主动成交和真实清算流分开画；绿色更偏 short squeeze / 主动买，红色更偏 long flush / 主动卖。",
            anchor_id="section-home-large-perp",
        )
        contract_heat_row = st.columns(4)
        contract_heat_row[0].metric("合约大额样本额", fmt_compact(perp_large_trade_heatmap_frame["总名义金额"].sum() if not perp_large_trade_heatmap_frame.empty else None))
        contract_heat_row[1].metric("清算样本额", fmt_compact(perp_liquidation_heatmap_frame["总名义金额"].sum() if not perp_liquidation_heatmap_frame.empty else None))
        contract_heat_row[2].metric("主动买价带", str(int((perp_large_trade_heatmap_frame["净名义金额"] > 0).sum()) if not perp_large_trade_heatmap_frame.empty else 0))
        contract_heat_row[3].metric("多头清算价带", str(int((perp_liquidation_heatmap_frame["净名义金额"] < 0).sum()) if not perp_liquidation_heatmap_frame.empty else 0))
        contract_heat_left, contract_heat_right = st.columns(2, gap="large")
        with contract_heat_left:
            st.plotly_chart(
                build_event_heatmap_figure(
                    perp_large_trade_heatmap_frame,
                    title="Perp Large Trade Heatmap",
                    positive_label="主动买",
                    negative_label="主动卖",
                ),
                key=chart_key("perp-large-heatmap", base_coin, interval, exchange_scope_mode),
                config=PLOTLY_CONFIG,
            )
        with contract_heat_right:
            st.plotly_chart(
                build_event_heatmap_figure(
                    perp_liquidation_heatmap_frame,
                    title="Perp Liquidation Heatmap 2.0",
                    positive_label="空头清算",
                    negative_label="多头清算",
                ),
                key=chart_key("perp-liq-heatmap", base_coin, interval, exchange_scope_mode),
                config=PLOTLY_CONFIG,
            )
        if not perp_large_trade_frame.empty:
            st.dataframe(
                perp_large_trade_frame.drop(columns=["侧向"], errors="ignore"),
                width="stretch",
                hide_index=True,
                column_config={
                    "价格": st.column_config.NumberColumn(format="%.4f"),
                    "数量": st.column_config.NumberColumn(format="%.4f"),
                    "名义金额": st.column_config.NumberColumn(format="%.2f"),
                },
            )
        st.caption(f"合约热力图当前按最近 {max(10, liquidation_window_minutes)} 分钟、价格窗口 {max(4.0, risk_heat_window_pct):.1f}% 聚合；现货和合约已完全分开计算。")

        liquidation_truth_inference_frame = build_liquidation_truth_inference_frame(
            {EXCHANGE_TITLES[key]: liquidation_events_by_exchange.get(key, []) for key in scope_perp_keys},
            perp_liquidation_heatmap_frame,
            now_ms=now_ms,
            window_minutes=max(10, liquidation_window_minutes),
        )
        render_section("真实清算流 / 推断爆仓带", "真实清算流只统计已发生事件；推断爆仓带是按价格带聚合的高风险区域，两者分开看才不会混淆真值和推断。")
        liq_truth_cards = st.columns(6)
        liq_truth_cards[0].metric("真实清算样本额", fmt_compact(liquidation_truth_inference_frame["真实清算额"].sum() if not liquidation_truth_inference_frame.empty else None))
        liq_truth_cards[1].metric("推断热力总额", fmt_compact(liquidation_truth_inference_frame["推断热力额"].sum() if not liquidation_truth_inference_frame.empty else None))
        liq_truth_cards[2].metric("真实事件数", str(int(liquidation_truth_inference_frame["真实事件数"].sum()) if not liquidation_truth_inference_frame.empty else 0))
        liq_truth_cards[3].metric("推断价带数", str(int(liquidation_truth_inference_frame["推断价带数"].sum()) if not liquidation_truth_inference_frame.empty else 0))
        liq_truth_cards[4].metric(
            "高覆盖所数",
            str(int(liquidation_truth_inference_frame["覆盖等级"].astype(str).eq("高").sum()) if not liquidation_truth_inference_frame.empty else 0),
        )
        liq_truth_cards[5].metric(
            "中高置信所数",
            str(
                int(
                    liquidation_truth_inference_frame["真值置信度"].astype(str).isin(["高", "中"]).sum()
                )
                if not liquidation_truth_inference_frame.empty
                else 0
            ),
        )
        liq_truth_left, liq_truth_right = st.columns([1.2, 1.0], gap="large")
        with liq_truth_left:
            st.plotly_chart(
                build_liquidation_truth_inference_figure(liquidation_truth_inference_frame),
                key=chart_key("liquidation-truth-inference", base_coin, interval, exchange_scope_mode),
                config=PLOTLY_CONFIG,
            )
        with liq_truth_right:
            if liquidation_truth_inference_frame.empty:
                st.info("当前还没有足够的真实清算和推断热力样本。")
            else:
                st.dataframe(
                    liquidation_truth_inference_frame,
                    width="stretch",
                    hide_index=True,
                    column_config={
                        "真实清算额": st.column_config.NumberColumn(format="%.2f"),
                        "真实事件数": st.column_config.NumberColumn(format="%d"),
                        "推断热力额": st.column_config.NumberColumn(format="%.2f"),
                        "推断价带数": st.column_config.NumberColumn(format="%d"),
                        "覆盖等级": st.column_config.TextColumn(),
                        "Tape口径": st.column_config.TextColumn(),
                        "采样说明": st.column_config.TextColumn(width="medium"),
                        "真值置信度": st.column_config.TextColumn(),
                    },
                )
        st.caption("真实清算来自公开 liquidation 流；推断爆仓带来自价格带热力聚合。前者看已发生，后者看潜在高风险区；不同交易所的 tape 覆盖等级并不相同。")

        render_section("风险缓冲 / 跨所份额动态", "风险缓冲层看 Funding、Premium、OI/成交比、保险池和冲击价差；份额动态层看 OI、合约成交、现货成交三条份额线谁在扩张。")
        risk_share_left, risk_share_right = st.columns([1.05, 1.15], gap="large")
        with risk_share_left:
            st.plotly_chart(
                build_risk_buffer_figure(risk_buffer_frame),
                key=chart_key("risk-buffer", base_coin, interval, exchange_scope_mode),
                config=PLOTLY_CONFIG,
            )
        with risk_share_right:
            st.plotly_chart(
                build_exchange_share_dynamics_figure(share_dynamics_frame),
                key=chart_key("share-dynamics", base_coin, interval, exchange_scope_mode),
                config=PLOTLY_CONFIG,
            )
        risk_buffer_cards = st.columns(4)
        highest_risk_row = (
            risk_buffer_frame.sort_values("OI/24h成交比", ascending=False, na_position="last").iloc[0]
            if not risk_buffer_frame.empty
            else {}
        )
        highest_share_row = (
            share_dynamics_frame.sort_values("OI份额(%)", ascending=False, na_position="last").iloc[0]
            if not share_dynamics_frame.empty
            else {}
        )
        risk_buffer_cards[0].metric(
            "Bybit 保险池",
            fmt_compact(bybit_insurance_value),
            delta="-" if not isinstance(bybit_insurance_payload, dict) else format_display_timestamp_ms(bybit_insurance_payload.get("updated_time_ms")),
        )
        risk_buffer_cards[1].metric(
            "最高 OI/成交比",
            str(highest_risk_row.get("交易所") or "-") if isinstance(highest_risk_row, pd.Series) else "-",
            "-" if not isinstance(highest_risk_row, pd.Series) else f"{float(highest_risk_row.get('OI/24h成交比') or 0.0):.2f}x",
        )
        risk_buffer_cards[2].metric(
            "最大 OI 份额",
            str(highest_share_row.get("交易所") or "-") if isinstance(highest_share_row, pd.Series) else "-",
            "-" if not isinstance(highest_share_row, pd.Series) else fmt_pct(highest_share_row.get("OI份额(%)")),
        )
        risk_buffer_cards[3].metric("份额基准", share_baseline_label)
        risk_share_detail_left, risk_share_detail_right = st.columns([1.0, 1.2], gap="large")
        with risk_share_detail_left:
            if risk_buffer_display_frame.empty:
                st.info("当前风险缓冲层还没有足够的公开样本。")
            else:
                st.dataframe(
                    risk_buffer_display_frame,
                    width="stretch",
                    hide_index=True,
                    column_config={
                        "OI/24h成交比": st.column_config.NumberColumn(format="%.2f"),
                        "Premium(%)": st.column_config.NumberColumn(format="%.3f"),
                        "Funding(bps)": st.column_config.NumberColumn(format="%.2f"),
                        "Funding带宽(bps)": st.column_config.NumberColumn(format="%.2f"),
                        "保险池(USD)": st.column_config.NumberColumn(format="%.2f"),
                        "冲击价差(bps)": st.column_config.NumberColumn(format="%.2f"),
                    },
                )
        with risk_share_detail_right:
            if share_dynamics_frame.empty:
                st.info("当前份额动态层还没有足够的快照样本。")
            else:
                st.dataframe(
                    share_dynamics_frame,
                    width="stretch",
                    hide_index=True,
                    column_config={
                        "OI份额(%)": st.column_config.ProgressColumn(format="%.2f", min_value=0.0, max_value=100.0),
                        "合约成交份额(%)": st.column_config.ProgressColumn(format="%.2f", min_value=0.0, max_value=100.0),
                        "现货成交份额(%)": st.column_config.ProgressColumn(format="%.2f", min_value=0.0, max_value=100.0),
                        "OI份额Δ(%)": st.column_config.NumberColumn(format="%.2f"),
                        "合约成交份额Δ(%)": st.column_config.NumberColumn(format="%.2f"),
                        "现货成交份额Δ(%)": st.column_config.NumberColumn(format="%.2f"),
                    },
                )
        st.caption("份额动态默认对比最近一轮基准样本；如果你一直开着页面，它会逐步更像一个实时的份额变化板。")

        if hyperliquid_address_bundle:
            render_hyperliquid_address_panel(
                hyperliquid_address_bundle,
                address=hyperliquid_address_value,
                lookback_hours=int(hyperliquid_address_lookback_hours),
                current_coin=symbol_map["hyperliquid"],
                key_scope=chart_key("hyper-address-home", base_coin, interval),
            )

        render_section("Binance 持仓量变化 / 多空比 / 账户占比", "把 Binance 的未平仓变化、大户持仓多空比、账户占比和主动买卖比放在一起看。这里的“人数”口径用账户占比代理，不是全市场真实人数。")
        crowd_row = st.columns(6)
        crowd_row[0].metric("当前 OI", fmt_compact(binance_perp_snapshot.open_interest))
        crowd_row[1].metric("当前未平仓金额", fmt_compact(binance_perp_snapshot.open_interest_notional))
        crowd_row[2].metric("Binance OI 变化", fmt_pct(binance_oi_quadrant.get("oi_change_pct")))
        crowd_row[3].metric("大户持仓比", "-" if crowd_position_ratio is None else f"{crowd_position_ratio:.3f}")
        crowd_row[4].metric("大户账户比", "-" if crowd_account_ratio is None else f"{crowd_account_ratio:.3f}")
        crowd_row[5].metric("全市场账户比", "-" if global_ratio is None else f"{global_ratio:.3f}")
        crowd_row = st.columns(4)
        crowd_row[0].metric("主动买卖比", "-" if taker_ratio is None else f"{taker_ratio:.3f}")
        crowd_row[1].metric("大户持仓多头占比", fmt_pct(top_position_long_share * 100.0 if top_position_long_share is not None else None))
        crowd_row[2].metric("大户账户多头占比", fmt_pct(top_account_long_share * 100.0 if top_account_long_share is not None else None))
        crowd_row[3].metric("全市场多头账户占比", fmt_pct(global_account_long_share * 100.0 if global_account_long_share is not None else None))
        crowd_top_left, crowd_top_right = st.columns([1.55, 1.25], gap="large")
        with crowd_top_left:
            binance_oi_figure, binance_oi_label = build_oi_figure(binance_merged_oi)
            st.plotly_chart(
                binance_oi_figure,
                key=chart_key("binance-oi", base_coin, symbol_map["binance"], interval),
                config=PLOTLY_CONFIG,
            )
            st.caption(f"{binance_oi_label}，下方 Delta 柱表示最近一步是加仓还是减仓。")
        with crowd_top_right:
            st.plotly_chart(
                build_binance_ratio_breakdown_figure(crowd_payload),
                key=chart_key("binance-ratios", base_coin, symbol_map["binance"], interval),
                config=PLOTLY_CONFIG,
            )
        crowd_bottom_left, crowd_bottom_right = st.columns([1.55, 1.25], gap="large")
        with crowd_bottom_left:
            st.plotly_chart(
                build_binance_crowd_figure(crowd_payload),
                key=chart_key("crowd", base_coin, symbol_map["binance"], interval),
                config=PLOTLY_CONFIG,
            )
        with crowd_bottom_right:
            st.dataframe(
                binance_contract_overview,
                width="stretch",
                hide_index=True,
                column_config={
                    "指标": st.column_config.TextColumn(),
                    "数值": st.column_config.NumberColumn(format="%.3f"),
                    "说明": st.column_config.TextColumn(width="large"),
                },
            )
            st.dataframe(
                crowd_alerts,
                width="stretch",
                hide_index=True,
                column_config={
                    "告警": st.column_config.TextColumn(),
                    "等级": st.column_config.TextColumn(),
                    "解释": st.column_config.TextColumn(width="large"),
                },
            )
        st.caption("账户占比 = 公开 API 提供的 longAccount / shortAccount 口径，只能作为“多空人数倾向”的代理值，不是全市场真实人数统计；现货市场没有 OI、多空人数、多空账户比这些公开字段。")

        render_section("爆仓瀑布 / 跨所联动热区", "把净爆仓强度和交易所间同步扩散放在一起看，更容易看出是单所事件还是跨所传导。")
        cluster_row = st.columns(4)
        cluster_row[0].metric("会话爆仓簇", str(len(cross_exchange_cluster_frame)))
        cluster_row[1].metric("跨所联动簇", str(int((cross_exchange_cluster_frame["类别"] == "跨所联动").sum()) if not cross_exchange_cluster_frame.empty else 0))
        cluster_row[2].metric("单所爆仓簇", str(int((cross_exchange_cluster_frame["类别"] == "单所爆仓").sum()) if not cross_exchange_cluster_frame.empty else 0))
        cluster_row[3].metric("跨所爆仓额", fmt_compact(cross_exchange_cluster_frame.loc[cross_exchange_cluster_frame["类别"] == "跨所联动", "总名义金额"].sum() if not cross_exchange_cluster_frame.empty else 0.0))
        liq_compare_left, liq_compare_right = st.columns(2, gap="large")
        with liq_compare_left:
            st.plotly_chart(
                build_liquidation_waterfall_figure(cross_exchange_session_liqs, selected_snapshot.timestamp_ms or int(time.time() * 1000), 120, 5),
                key=chart_key("liq-waterfall", base_coin, interval),
                config=PLOTLY_CONFIG,
            )
        with liq_compare_right:
            st.plotly_chart(
                build_liquidation_linkage_heatmap(cross_exchange_session_liqs, selected_snapshot.timestamp_ms or int(time.time() * 1000), 120, 5),
                key=chart_key("liq-linkage", base_coin, interval),
                config=PLOTLY_CONFIG,
            )
        if cross_exchange_cluster_frame.empty:
            st.info("当前会话里还没有形成跨交易所爆仓联动簇。")
        else:
            st.dataframe(
                cross_exchange_cluster_frame,
                width="stretch",
                hide_index=True,
                column_config={
                    "持续秒数": st.column_config.NumberColumn(format="%.1f"),
                    "多头爆仓额": st.column_config.NumberColumn(format="%.2f"),
                    "空头爆仓额": st.column_config.NumberColumn(format="%.2f"),
                    "总名义金额": st.column_config.NumberColumn(format="%.2f"),
                },
            )

render_terminal()
