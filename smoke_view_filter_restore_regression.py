from __future__ import annotations

from pathlib import Path


def assert_true(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


def main() -> None:
    content = Path("web/app_shared.js").read_text(encoding="utf-8")

    assert_true("function sanitizeViewFilterSnapshot" in content, "缺少视图筛选净化函数")
    assert_true('if (String(viewKey || "") === "info-board" && sanitized.marketScope === "spot") {' in content, "信息榜 spot -> merged 保护缺失")
    assert_true('sanitized.selectedExchangeKeys = sanitized.selectedExchangeKeys.filter((exchangeKey) => marketSupported(exchangeKey, "spot"));' in content, "spot 市场交易所筛选净化缺失")
    assert_true("const snapshot = sanitizeViewFilterSnapshot(viewKey, rawSnapshot);" in content, "保存筛选时未做净化")
    assert_true("const sanitizedSnapshot = sanitizeViewFilterSnapshot(viewKey, snapshot);" in content, "恢复筛选时未做净化")
    assert_true("state.viewFilters = {" in content and "saveViewFilters(state.viewFilters);" in content, "净化后未回写本地存储")

    print("OK view filter restore regression")


if __name__ == "__main__":
    main()
