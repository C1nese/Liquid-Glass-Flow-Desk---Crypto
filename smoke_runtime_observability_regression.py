from __future__ import annotations

from market_runtime import EXCHANGE_ORDER, MarketRuntimeSession


def assert_true(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


def assert_runtime_meta(data_meta: dict, label: str) -> None:
    runtime = data_meta.get("runtime") if isinstance(data_meta, dict) else {}
    assert_true(isinstance(runtime, dict), f"{label} runtime 缺失: {data_meta}")
    assert_true(str(data_meta.get("runtime_summary") or "").strip() != "", f"{label} runtime_summary 缺失: {data_meta}")
    assert_true(str(data_meta.get("runtime_compact_summary") or "").strip() != "", f"{label} runtime_compact_summary 缺失: {data_meta}")
    cache = runtime.get("cache") if isinstance(runtime, dict) else {}
    assert_true(isinstance(cache, dict), f"{label} runtime.cache 缺失: {runtime}")
    for key in ["hit", "miss", "build", "wait"]:
        assert_true(key in cache, f"{label} runtime.cache 缺少 {key}: {cache}")


def main() -> None:
    session = MarketRuntimeSession("BTC", timeout=5)
    try:
        overview_payload = session.overview_rich_payload(
            exchange_key="binance",
            watch_group="hot",
            exchange_keys=list(EXCHANGE_ORDER),
            market_scope="merged",
            time_window="1h",
            ratio_window="15m",
            coin_scope="all",
            stage="lite-tables",
        )
        assert_runtime_meta(dict(overview_payload.get("data_meta") or {}), "overview-rich")

        execution_payload = session.execution_payload(
            exchange_keys=list(EXCHANGE_ORDER),
            market_scope="merged",
            time_window="1h",
            _sync_on_miss=True,
        )
        assert_runtime_meta(dict(execution_payload.get("data_meta") or {}), "execution")

        print("OK runtime observability regression")
        print("OVERVIEW", overview_payload.get("data_meta", {}).get("runtime_compact_summary"))
        print("EXECUTION", execution_payload.get("data_meta", {}).get("runtime_compact_summary"))
    finally:
        try:
            session.service.stop()
        except Exception:
            pass


if __name__ == "__main__":
    main()
