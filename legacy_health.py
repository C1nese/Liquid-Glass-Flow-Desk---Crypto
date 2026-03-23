from __future__ import annotations

import time
from datetime import datetime
from typing import Any, Dict, List

import pandas as pd

from exchanges import EXCHANGE_ORDER, SPOT_EXCHANGE_ORDER


EXCHANGE_TITLES = {"bybit": "Bybit", "binance": "Binance", "okx": "OKX", "hyperliquid": "Hyperliquid"}


def payload_float(value: Any) -> float | None:
    parsed = pd.to_numeric(value, errors="coerce")
    return None if pd.isna(parsed) else float(parsed)


def format_display_timestamp_ms(value: Any) -> str:
    if value in (None, "", 0):
        return "-"
    try:
        return datetime.fromtimestamp(int(value) / 1000.0).astimezone().strftime("%m-%d %H:%M:%S")
    except (OSError, OverflowError, TypeError, ValueError):
        return "-"


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


def _join_caption_parts(*parts: str | None) -> str:
    return " · ".join(str(part) for part in parts if part)


def _transport_health_state_label(health: Dict[str, Any], *, available: bool, market: str) -> str:
    if not available:
        return "未上架/未命中"
    sync_state = str(health.get("sync_state") or health.get("stream_status") or "").strip().lower()
    if market == "spot" and health.get("status") == "unsupported":
        return "未接入"
    if sync_state in {"synced", "live", "proxy"}:
        ts_ms = int(payload_float(health.get("last_message_ms")) or 0) or int(payload_float(health.get("snapshot_timestamp_ms")) or 0) or int(payload_float(health.get("last_snapshot_ms")) or 0)
        prefix = "代理在线" if sync_state == "proxy" else "在线"
        return _join_caption_parts(prefix, _format_latency(ts_ms))
    if sync_state == "bootstrapping":
        return "回补中"
    if sync_state in {"connecting", "reconnecting"}:
        return "重连中"
    if sync_state == "degraded":
        return "待修复"
    error_text = str(health.get("error") or health.get("bootstrap_error") or "").strip()
    if error_text:
        return error_text[:48]
    ts_ms = int(payload_float(health.get("last_message_ms")) or 0) or int(payload_float(health.get("snapshot_timestamp_ms")) or 0) or int(payload_float(health.get("last_snapshot_ms")) or 0)
    if ts_ms:
        return _format_latency(ts_ms)
    return "待采样"


def _request_health_state_label(row: Dict[str, Any], *, available: bool, current_status: str | None = None) -> str:
    if not available:
        return "未上架/未命中"
    cooldown_seconds = int(row.get("cooldown_remaining_s") or 0)
    if cooldown_seconds > 0:
        return f"冷却 {cooldown_seconds}s"
    if int(row.get("consecutive_failures") or 0) > 0 and str(row.get("status") or "") == "error":
        status_code = row.get("last_status_code")
        if status_code:
            return f"异常 {status_code}"
        error_kind = str(row.get("error_kind") or "").strip()
        return f"异常 {error_kind or '失败'}"
    if int(payload_float(row.get("last_success_ms")) or 0):
        return "正常"
    if current_status == "ok":
        return "正常"
    return "待采样"


def build_exchange_health_frame(
    request_health_rows: List[Dict[str, Any]],
    *,
    service: Any,
    snapshot_by_key: Dict[str, Any],
    spot_snapshot_map: Dict[str, Any],
    available_perp_keys: List[str],
    available_spot_keys: List[str],
    catalog_status: Dict[str, Dict[str, str]],
) -> pd.DataFrame:
    request_index = {
        (str(row.get("exchange_key") or ""), str(row.get("market") or "")): dict(row)
        for row in request_health_rows
    }
    rows: List[Dict[str, Any]] = []
    for exchange_key in EXCHANGE_ORDER:
        exchange_title = EXCHANGE_TITLES[exchange_key]
        perp_snapshot = snapshot_by_key.get(exchange_key)
        spot_snapshot = spot_snapshot_map.get(exchange_key)
        perp_row = request_index.get((exchange_key, "perp"), {})
        spot_row = request_index.get((exchange_key, "spot"), {})
        perp_available = exchange_key in available_perp_keys or str((catalog_status.get(exchange_key) or {}).get("perp") or "") == "error"
        spot_available = exchange_key in available_spot_keys or str((catalog_status.get(exchange_key) or {}).get("spot") or "") == "error"
        catalog_parts: List[str] = []
        perp_catalog_status = str((catalog_status.get(exchange_key) or {}).get("perp") or "")
        spot_catalog_status = str((catalog_status.get(exchange_key) or {}).get("spot") or "")
        catalog_parts.append("合约目录受限" if perp_catalog_status == "error" else "合约可用" if exchange_key in available_perp_keys else "合约未命中")
        if exchange_key == "hyperliquid":
            catalog_parts.append("现货未接入")
        else:
            catalog_parts.append("现货目录受限" if spot_catalog_status == "error" else "现货可用" if exchange_key in available_spot_keys else "现货未命中")
        perp_transport = service.get_transport_health(exchange_key)
        spot_transport = service.get_transport_health(exchange_key, spot=True) if exchange_key in SPOT_EXCHANGE_ORDER else {"status": "unsupported"}
        latest_success_ms = max(
            int(payload_float(perp_row.get("last_success_ms")) or 0),
            int(payload_float(spot_row.get("last_success_ms")) or 0),
        ) or None
        latest_error_items = [
            (
                int(payload_float(perp_row.get("last_error_ms")) or 0),
                str(perp_row.get("last_error") or "").strip(),
            ),
            (
                int(payload_float(spot_row.get("last_error_ms")) or 0),
                str(spot_row.get("last_error") or "").strip(),
            ),
        ]
        latest_error_ms, latest_error_text = max(latest_error_items, key=lambda item: item[0])
        notes: List[str] = []
        if any(int(row.get("cooldown_remaining_s") or 0) > 0 for row in (perp_row, spot_row)):
            notes.append("自动冷却中")
        if "error" in {perp_catalog_status, spot_catalog_status}:
            notes.append("目录受限时仍按默认符号继续尝试")
        if str(perp_row.get("error_kind") or "") in {"legal", "forbidden", "rate_limit"} or str(spot_row.get("error_kind") or "") in {"legal", "forbidden", "rate_limit"}:
            notes.append("云端/风控受限")
        if str(perp_transport.get("sync_state") or "") in {"degraded", "proxy"} or str(spot_transport.get("sync_state") or "") in {"degraded", "proxy"}:
            notes.append("WS代理/待修复")
        rows.append(
            {
                "交易所": exchange_title,
                "目录": " | ".join(catalog_parts),
                "合约REST": _request_health_state_label(perp_row, available=perp_available, current_status=getattr(perp_snapshot, "status", None)),
                "现货REST": "未接入" if exchange_key == "hyperliquid" else _request_health_state_label(spot_row, available=spot_available, current_status=getattr(spot_snapshot, "status", None)),
                "合约WS": _transport_health_state_label(perp_transport, available=perp_available, market="perp"),
                "现货WS": "未接入" if exchange_key == "hyperliquid" else _transport_health_state_label(spot_transport, available=spot_available, market="spot"),
                "连续失败": max(int(perp_row.get("consecutive_failures") or 0), int(spot_row.get("consecutive_failures") or 0)),
                "冷却剩余(s)": max(int(perp_row.get("cooldown_remaining_s") or 0), int(spot_row.get("cooldown_remaining_s") or 0)),
                "最近成功": format_display_timestamp_ms(latest_success_ms),
                "最近错误": "-" if not latest_error_text else f"{format_display_timestamp_ms(latest_error_ms)} | {latest_error_text[:48]}",
                "说明": " / ".join(notes) if notes else "正常采样",
            }
        )
    frame = pd.DataFrame(rows)
    if frame.empty:
        return pd.DataFrame(columns=["交易所", "目录", "合约REST", "现货REST", "合约WS", "现货WS", "连续失败", "冷却剩余(s)", "最近成功", "最近错误", "说明"])
    return frame
