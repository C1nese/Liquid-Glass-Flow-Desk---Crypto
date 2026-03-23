from __future__ import annotations

from market_runtime import EXCHANGE_ORDER, MarketRuntimeSession


def assert_true(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


def main() -> None:
    session = MarketRuntimeSession("BTC", timeout=5)
    try:
        payload = session.execution_payload(
            exchange_keys=list(EXCHANGE_ORDER),
            market_scope="merged",
            time_window="1h",
            _sync_on_miss=True,
        )

        data_meta = payload.get("data_meta") or {}
        assert_true(str(data_meta.get("source") or "").strip() != "", "execution 缺少 data_meta.source")
        assert_true(str(data_meta.get("truth_level") or "").strip() == "mixed_reference", f"execution truth_level 异常: {data_meta}")

        cards = list(payload.get("cards") or [])
        labels = {str(item.get("label") or "") for item in cards}
        assert_true("15m共振" in labels, f"execution cards 缺少 15m共振: {labels}")
        assert_true("突破质量" in labels, f"execution cards 缺少 突破质量: {labels}")

        resonance_rows = list(payload.get("resonance_rows") or [])
        breakout_rows = list(payload.get("breakout_quality_rows") or [])
        assert_true(len(resonance_rows) >= 1, "execution resonance_rows 为空")
        assert_true(len(breakout_rows) >= 1, "execution breakout_quality_rows 为空")

        resonance_columns = set(resonance_rows[0].keys())
        breakout_columns = set(breakout_rows[0].keys())
        assert_true({"周期", "共振分", "共振结论"}.issubset(resonance_columns), f"resonance_rows 字段异常: {resonance_columns}")
        assert_true({"维度", "当前值", "结论", "动作"}.issubset(breakout_columns), f"breakout_quality_rows 字段异常: {breakout_columns}")

        panel_ids = [str(panel.get("id") or "") for panel in list(payload.get("panels") or [])]
        assert_true("execution-resonance-figure" in panel_ids, f"execution 缺少 resonance panel: {panel_ids[:12]}")
        assert_true("execution-breakout-quality" in panel_ids, f"execution 缺少 breakout panel: {panel_ids[:12]}")

        print("OK execution regression")
        print("TRUTH", data_meta.get("truth_level"))
        print("RESONANCE_ROWS", len(resonance_rows))
        print("BREAKOUT_ROWS", len(breakout_rows))
    finally:
        try:
            session.service.stop()
        except Exception:
            pass


if __name__ == "__main__":
    main()
