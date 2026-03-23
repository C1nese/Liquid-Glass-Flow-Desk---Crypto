from __future__ import annotations

from typing import Dict, Iterable, List

import pandas as pd


def _normalize_column_token(value: object) -> str:
    return (
        str(value or "")
        .strip()
        .lower()
        .replace(" ", "")
        .replace("_", "")
    )


CANONICAL_COLUMN_ALIASES: Dict[str, List[str]] = {
    "时间": ["时间", "timestamp", "timestamp_ms", "ts", "ts_ms", "time"],
    "币种": ["币种", "\u752f\u4f7a\ue752", "symbol", "coin", "asset", "标的"],
    "市场": ["市场", "market"],
    "合约": ["合约", "contract", "instrument", "instId", "symbol_name"],
    "最新价": ["最新价", "last_price", "close", "latest_price"],
    "标记价": ["标记价", "mark_price"],
    "指数价": ["指数价", "index_price"],
    "信号": ["信号", "\u6dc7\u2033\u5f7f", "signal"],
    "情绪评分": ["情绪评分", "\u93af\u546f\u534e\u7487\u52eb\u578e", "sentiment_score"],
    "置信度": ["置信度", "\u7f03\ue1bb\u4fca\u6434?", "confidence", "truth_level_score"],
    "情绪": ["情绪", "\u93af\u546f\u534e", "sentiment"],
    "拥挤方向": ["拥挤方向", "\u93b7\u30e6\u5c0b\u93c2\u7470\u609c", "crowding_direction"],
    "交易所": ["交易所", "exchange", "主交易所", "\u6d93\u8bb3\u6c26\u93c4\u64b4\u588d"],
    "交易所键": ["交易所键", "exchange_key", "主交易所键", "\u6d93\u8bb3\u6c26\u93c4\u64b4\u588d\u95bf?"],
    "主交易所": ["主交易所", "\u6d93\u8bb3\u6c26\u93c4\u64b4\u588d", "primary_exchange"],
    "主交易所键": ["主交易所键", "\u6d93\u8bb3\u6c26\u93c4\u64b4\u588d\u95bf?", "primary_exchange_key"],
    "偏离交易所": ["偏离交易所", "\u934b\u5fd5\ue787\u6d5c\u3086\u69d7\u93b5\u20ac", "divergence_exchange"],
    "OI总额": ["OI总额", "OI金额", "\u93ac\u5a5a\ue582", "OI Total", "total_oi_notional", "oi_notional_usdt", "open_interest_notional", "未平仓金额"],
    "24h成交额": ["24h成交额", "volume_24h_notional", "perp_volume_24h_notional", "spot_volume_24h_notional"],
    "Funding(bp)": ["Funding(bp)", "Funding(bps)", "funding_bps", "perp_funding_bps"],
    "L/S比": ["L/S比", "L/S姣?", "ls_ratio"],
    "价格": ["价格", "\u6d60\u950b\u7278", "price", "last_price", "reference_price"],
    "状态": ["状态", "\u9418\u8235\u20ac?", "status"],
    "类型": ["类型", "event_kind", "kind", "category"],
    "动作": ["动作", "action"],
    "等级": ["等级", "level", "severity"],
    "说明": ["说明", "explanation", "detail", "message"],
    "名义金额": ["名义金额", "notional", "amount_notional"],
    "24h爆仓样本额": ["24h爆仓样本额", "\u9416\u55d5\u7ca8\u93cd\u950b\u6e70\u68f0?", "liquidation_notional_24h"],
    "事件数": ["事件数", "event_count", "count"],
    "总名义金额": ["总名义金额", "total_notional"],
    "多头爆仓额": ["多头爆仓额", "long_notional"],
    "空头爆仓额": ["空头爆仓额", "short_notional"],
    "最近时间": ["最近时间", "last_timestamp_ms", "latest_timestamp_ms"],
    "本地归档样本": ["本地归档样本", "archive_sample_count"],
    "归档目录": ["归档目录", "archive_path", "path"],
}


def _resolved_column_lookup(frame: pd.DataFrame) -> Dict[str, str]:
    lookup: Dict[str, str] = {}
    for column in frame.columns:
        text = str(column)
        lookup.setdefault(_normalize_column_token(text), text)
    return lookup


def _fallback_column(frame: pd.DataFrame, canonical: str) -> str:
    columns = [str(column) for column in frame.columns]
    normalized_lookup = _resolved_column_lookup(frame)
    if canonical == "OI总额":
        for column in columns:
            token = _normalize_column_token(column)
            if "oi" in token and all(excluded not in token for excluded in ("1h", "4h", "24h", "pct", "%", "share", "delta")):
                return column
    if canonical == "币种":
        for token in ("symbol", "coin", "asset", "标的"):
            matched = normalized_lookup.get(_normalize_column_token(token))
            if matched:
                return matched
    if canonical == "交易所":
        for token in ("exchange", "交易所"):
            matched = normalized_lookup.get(_normalize_column_token(token))
            if matched:
                return matched
    if canonical == "交易所键":
        for token in ("exchange_key", "交易所键", "主交易所键"):
            matched = normalized_lookup.get(_normalize_column_token(token))
            if matched:
                return matched
    if canonical == "24h成交额":
        for token in ("volume_24h_notional", "24h成交额", "perp_volume_24h_notional", "spot_volume_24h_notional"):
            matched = normalized_lookup.get(_normalize_column_token(token))
            if matched:
                return matched
    if canonical == "Funding(bp)":
        for token in ("funding_bps", "funding(bp)", "funding(bps)", "perp_funding_bps"):
            matched = normalized_lookup.get(_normalize_column_token(token))
            if matched:
                return matched
    return ""


def find_market_column(frame: pd.DataFrame, canonical: str) -> str:
    columns = _resolved_column_lookup(frame)
    direct = columns.get(_normalize_column_token(canonical))
    if direct:
        return direct
    aliases = CANONICAL_COLUMN_ALIASES.get(canonical) or []
    for alias in aliases:
        matched = columns.get(_normalize_column_token(alias))
        if matched:
            return matched
    return _fallback_column(frame, canonical)


def find_total_oi_column(frame: pd.DataFrame) -> str:
    return find_market_column(frame, "OI总额")


def canonicalize_market_frame_columns(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return frame
    rename_map: Dict[str, str] = {}
    current_columns = [str(column) for column in frame.columns]
    for canonical in CANONICAL_COLUMN_ALIASES:
        if canonical in current_columns:
            continue
        matched = find_market_column(frame, canonical)
        if matched and matched != canonical and matched not in rename_map:
            rename_map[matched] = canonical
    return frame.rename(columns=rename_map) if rename_map else frame


def canonicalize_frame_list(frames: Iterable[pd.DataFrame]) -> List[pd.DataFrame]:
    return [canonicalize_market_frame_columns(frame.copy()) if isinstance(frame, pd.DataFrame) else pd.DataFrame() for frame in frames]
