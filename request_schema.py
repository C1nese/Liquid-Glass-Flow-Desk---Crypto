from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable, Optional, Sequence


def _allowed_values(allowed: Iterable[str] | dict[str, Any]) -> list[str]:
    if isinstance(allowed, dict):
        values = list(allowed.keys())
    else:
        values = list(allowed)
    normalized: list[str] = []
    for value in values:
        token = str(value or "").strip().lower()
        if token and token not in normalized:
            normalized.append(token)
    return normalized


def normalize_choice(
    value: Any,
    *,
    allowed: Iterable[str] | dict[str, Any],
    default: str,
    aliases: Optional[dict[str, str]] = None,
) -> str:
    allowed_values = _allowed_values(allowed)
    normalized_default = str(default or "").strip().lower()
    normalized_value = str(value or normalized_default).strip().lower()
    if aliases:
        normalized_value = aliases.get(normalized_value, normalized_value)
    if normalized_value in allowed_values:
        return normalized_value
    return normalized_default if normalized_default in allowed_values else (allowed_values[0] if allowed_values else normalized_default)


def normalize_csv_choices(
    value: Any,
    *,
    allowed: Sequence[str] | dict[str, Any],
    default: Optional[Sequence[str]] = None,
) -> list[str]:
    allowed_values = _allowed_values(allowed)
    if isinstance(value, (list, tuple, set)):
        candidates = value
    else:
        candidates = str(value or "").replace("|", ",").split(",")
    selected: list[str] = []
    for item in candidates:
        token = str(item or "").strip().lower()
        if token in allowed_values and token not in selected:
            selected.append(token)
    if selected:
        return selected
    if default is not None:
        fallback = []
        for item in default:
            token = str(item or "").strip().lower()
            if token in allowed_values and token not in fallback:
                fallback.append(token)
        if fallback:
            return fallback
    return list(allowed_values)


def normalize_non_negative_float(value: Any, *, default: float = 0.0) -> float:
    try:
        normalized = float(value if value not in (None, "") else default)
    except (TypeError, ValueError):
        normalized = float(default)
    return max(float(normalized), 0.0)


def normalize_optional_int(
    value: Any,
    *,
    minimum: Optional[int] = None,
    maximum: Optional[int] = None,
) -> Optional[int]:
    if value is None or value == "":
        return None
    try:
        normalized = int(value)
    except (TypeError, ValueError):
        return None
    if minimum is not None:
        normalized = max(int(minimum), normalized)
    if maximum is not None:
        normalized = min(int(maximum), normalized)
    return normalized


def normalize_int(
    value: Any,
    *,
    default: int,
    minimum: Optional[int] = None,
    maximum: Optional[int] = None,
) -> int:
    try:
        normalized = int(value if value not in (None, "") else default)
    except (TypeError, ValueError):
        normalized = int(default)
    if minimum is not None:
        normalized = max(int(minimum), normalized)
    if maximum is not None:
        normalized = min(int(maximum), normalized)
    return normalized


@dataclass(frozen=True)
class LiquidationRequestSchema:
    exchange_key: str
    exchange_keys: list[str]
    archive_window: str
    window_minutes: Optional[int]
    limit: int
    min_notional: float
    direction: str
    scope: str
    time_window: str


def normalize_liquidation_request(
    *,
    exchange_key: Any,
    exchange_keys: Any,
    archive_window: Any,
    window_minutes: Any,
    limit: Any,
    min_notional: Any,
    direction: Any,
    scope: Any,
    time_window: Any,
    allowed_exchanges: Sequence[str],
    allowed_archive_windows: Sequence[str],
    allowed_time_windows: Sequence[str] | dict[str, Any],
    default_exchange: str = "binance",
) -> LiquidationRequestSchema:
    normalized_exchange_keys = normalize_csv_choices(exchange_keys, allowed=allowed_exchanges, default=allowed_exchanges)
    normalized_exchange = normalize_choice(
        exchange_key,
        allowed=allowed_exchanges,
        default=normalized_exchange_keys[0] if normalized_exchange_keys else default_exchange,
    )
    if normalized_exchange_keys and normalized_exchange not in normalized_exchange_keys:
        normalized_exchange = normalized_exchange_keys[0]
    return LiquidationRequestSchema(
        exchange_key=normalized_exchange,
        exchange_keys=normalized_exchange_keys,
        archive_window=normalize_choice(archive_window, allowed=allowed_archive_windows, default="4h"),
        window_minutes=normalize_optional_int(window_minutes, minimum=5, maximum=720),
        limit=normalize_int(limit, default=120, minimum=20, maximum=300),
        min_notional=normalize_non_negative_float(min_notional),
        direction=normalize_choice(direction, allowed=("all", "long", "short"), default="all"),
        scope=normalize_choice(scope, allowed=("all", "single", "cross"), default="all"),
        time_window=normalize_choice(
            time_window,
            allowed=allowed_time_windows,
            default="1h",
            aliases={"24h": "24h", "1d": "1d"},
        ),
    )
