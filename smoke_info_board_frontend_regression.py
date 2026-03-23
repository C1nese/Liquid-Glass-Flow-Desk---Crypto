from __future__ import annotations

from pathlib import Path


def assert_true(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


def main() -> None:
    content = Path("web/app_views.js").read_text(encoding="utf-8")

    assert_true('function resolveInfoBoardMarketScope' in content, "缺少 resolveInfoBoardMarketScope")
    assert_true('if (normalized === "perp") {' in content and 'return "perp";' in content, "perp 口径未被保留")
    assert_true('return "merged";' in content, "信息榜默认 merged 回退丢失")
    assert_true('const effectiveMarketScope = resolveInfoBoardMarketScope(state.marketScope);' in content, "loadInfoBoard 未使用 effectiveMarketScope")
    assert_true('market_scope: effectiveMarketScope,' in content, "loadInfoBoard buildUrl 仍未使用 effectiveMarketScope")
    assert_true('lockedSourceSummary(dataMeta)' in content, "信息榜/首页状态栏未显示锁定来源摘要")
    assert_true('attachLockedSourceToCards' in content, "顶部卡片未注入锁定来源摘要")

    print("OK info-board frontend regression")


if __name__ == "__main__":
    main()
