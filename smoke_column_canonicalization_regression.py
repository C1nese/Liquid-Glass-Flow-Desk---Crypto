from __future__ import annotations

import pandas as pd

from analytics import (
    build_multi_coin_oi_quadrant_bubble_figure,
    build_multi_coin_oi_ranking_figure,
    build_multicoin_long_short_ratio_figure,
)
from market_frame_columns import canonicalize_market_frame_columns


def main() -> None:
    legacy_oi_frame = pd.DataFrame(
        [
            {
                "甯佺": "BTC",
                "OI鎬婚": 1_250_000_000,
                "淇″彿": "偏多",
                "鎯呯华璇勫垎": 7.5,
                "涓讳氦鏄撴墍閿?": "binance",
                "Lead/Lag": "现货领先",
                "1h%": 2.4,
                "OI 1h(%)": 4.8,
            },
            {
                "甯佺": "ETH",
                "OI鎬婚": 860_000_000,
                "淇″彿": "中性",
                "鎯呯华璇勫垎": 1.8,
                "涓讳氦鏄撴墍閿?": "bybit",
                "Lead/Lag": "同步",
                "1h%": -0.8,
                "OI 1h(%)": 1.2,
            },
        ]
    )
    normalized = canonicalize_market_frame_columns(legacy_oi_frame.copy())
    assert {"币种", "OI总额", "信号", "情绪评分", "交易所键"}.issubset(set(normalized.columns)), normalized.columns.tolist()

    oi_ranking = build_multi_coin_oi_ranking_figure(legacy_oi_frame, limit=10)
    assert len(oi_ranking.data) == 1, "OI ranking figure should render one trace"

    oi_quadrant = build_multi_coin_oi_quadrant_bubble_figure(
        legacy_oi_frame,
        limit=10,
        price_column="1h%",
        oi_column="OI 1h(%)",
        price_label="1h 价格变化",
        oi_label="OI 1h 变化",
    )
    assert len(oi_quadrant.data) == 1, "OI quadrant figure should render one trace"

    legacy_ratio_frame = pd.DataFrame(
        [
            {
                "甯佺": "BTC",
                "Binance全市场比": 1.18,
                "Bybit账户比": 1.09,
                "Bybit多头占比(%)": 54.0,
                "Bybit空头占比(%)": 46.0,
                "OKX合约账户比": 1.05,
                "Bitget账户比": 1.02,
                "Gate账户比": 1.01,
                "HTX账户比": 0.99,
                "鎯呯华璇勫垎": 6.0,
            },
            {
                "甯佺": "ETH",
                "Binance全市场比": 0.94,
                "Bybit账户比": 0.97,
                "Bybit多头占比(%)": 49.0,
                "Bybit空头占比(%)": 51.0,
                "OKX合约账户比": 0.96,
                "Bitget账户比": 0.98,
                "Gate账户比": 0.95,
                "HTX账户比": 0.97,
                "鎯呯华璇勫垎": -3.0,
            },
        ]
    )
    ratio_figure = build_multicoin_long_short_ratio_figure(legacy_ratio_frame, ratio_window="15m")
    assert len(ratio_figure.data) >= 2, "Long-short figure should render traces"

    print("OK column canonicalization regression")
    print("COLUMNS", sorted(normalized.columns.tolist())[:8])
    print("OI_TRACES", len(oi_ranking.data), len(oi_quadrant.data))
    print("RATIO_TRACES", len(ratio_figure.data))


if __name__ == "__main__":
    main()
