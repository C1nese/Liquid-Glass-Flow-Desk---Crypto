from __future__ import annotations

from typing import Iterable

from market_runtime import EXCHANGE_ORDER, MarketRuntimeSession


def assert_true(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


def panel_meta_source_set(panels: Iterable[dict], limit: int = 4) -> set[str]:
    values: set[str] = set()
    for panel in list(panels)[:limit]:
        meta = panel.get("meta") if isinstance(panel, dict) else {}
        source = str((meta or {}).get("source") or "").strip()
        if source:
            values.add(source)
    return values


def main() -> None:
    session = MarketRuntimeSession("BTC", timeout=5)
    try:
        stages = {}
        for stage in ["lite-tables", "charts-a", "full"]:
            payload = session.overview_rich_payload(
                exchange_key="binance",
                watch_group="hot",
                exchange_keys=list(EXCHANGE_ORDER),
                market_scope="merged",
                time_window="1h",
                ratio_window="15m",
                coin_scope="all",
                stage=stage,
            )
            stages[stage] = payload

        lite_source = str((stages["lite-tables"].get("data_meta") or {}).get("source") or "").strip()
        charts_a_source = str((stages["charts-a"].get("data_meta") or {}).get("source") or "").strip()
        full_source = str((stages["full"].get("data_meta") or {}).get("source") or "").strip()
        assert_true(bool(lite_source), "lite-tables 缺少 data_meta.source")
        assert_true(lite_source == charts_a_source == full_source, f"overview-rich 分阶段来源不一致: {lite_source} / {charts_a_source} / {full_source}")

        lite_truth = str((stages["lite-tables"].get("data_meta") or {}).get("truth_level") or "").strip()
        full_truth = str((stages["full"].get("data_meta") or {}).get("truth_level") or "").strip()
        assert_true(bool(lite_truth), "lite-tables 缺少 data_meta.truth_level")
        assert_true(lite_truth == full_truth, f"overview-rich 轻量与 full 真值等级不一致: {lite_truth} / {full_truth}")

        light_4h = session._build_overview_rich_light_context(
            exchange_key="binance",
            watch_group="hot",
            ratio_window="15m",
            custom_coins=None,
            exchange_keys=list(EXCHANGE_ORDER),
            market_scope="merged",
            time_window="4h",
            coin_scope="all",
            min_notional=0.0,
            oi_threshold_pct=0.0,
            sentiment_mode="all",
        )
        quadrant_meta = dict(light_4h.get("quadrant_meta") or {})
        assert_true(quadrant_meta.get("price_column") == "4h%", f"4h 窗口 price_column 异常: {quadrant_meta}")
        assert_true(quadrant_meta.get("oi_column") == "OI 4h(%)", f"4h 窗口 oi_column 异常: {quadrant_meta}")

        lite_panels = session._build_overview_rich_lite_panels(light_4h, watch_group="hot", market_scope="merged")
        note_map = {str(panel.get("id") or ""): str(panel.get("note") or "") for panel in lite_panels}
        assert_true("4h" in note_map.get("overview-oi-board", ""), f"overview-oi-board 未跟随 4h 窗口: {note_map.get('overview-oi-board')}")
        assert_true("4h" in note_map.get("overview-funding-board", ""), f"overview-funding-board 未跟随 4h 窗口: {note_map.get('overview-funding-board')}")

        panel_sources = panel_meta_source_set(stages["lite-tables"].get("panels") or [])
        assert_true(any("本地 runtime 会话 + 历史回补" in item or "Manager" in item or "Legacy" in item for item in panel_sources), f"lite-tables 面板来源异常: {panel_sources}")

        print("OK overview-rich source lock regression")
        print("SOURCE", lite_source)
        print("TRUTH", lite_truth)
        print("QUADRANT", quadrant_meta)
    finally:
        try:
            session.service.stop()
        except Exception:
            pass


if __name__ == "__main__":
    main()
