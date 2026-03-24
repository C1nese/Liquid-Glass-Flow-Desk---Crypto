from __future__ import annotations

from dataclasses import replace
import copy
from typing import Any, Dict, List, Optional, Tuple
import re
import time
import threading

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from models import Candle, ExchangeSnapshot, LiquidationEvent, OIPoint, OrderBookLevel, SpotSnapshot, TradeEvent


DEFAULT_TIMEOUT = 10
EXCHANGE_ORDER = ("binance", "bybit", "okx", "hyperliquid")
SPOT_EXCHANGE_ORDER = ("binance", "bybit", "okx")
SUPPORTED_INTERVALS = ("1m", "3m", "5m", "15m", "30m", "1h", "4h", "1d")

BYBIT_CANDLE_INTERVALS = {
    "1m": "1",
    "3m": "3",
    "5m": "5",
    "15m": "15",
    "30m": "30",
    "1h": "60",
    "4h": "240",
    "1d": "D",
}
BINANCE_CANDLE_INTERVALS = {
    "1m": "1m",
    "3m": "3m",
    "5m": "5m",
    "15m": "15m",
    "30m": "30m",
    "1h": "1h",
    "4h": "4h",
    "1d": "1d",
}
OKX_CANDLE_INTERVALS = {
    "1m": "1m",
    "3m": "3m",
    "5m": "5m",
    "15m": "15m",
    "30m": "30m",
    "1h": "1H",
    "4h": "4H",
    "1d": "1Dutc",
}
HYPERLIQUID_CANDLE_INTERVALS = {
    "1m": "1m",
    "3m": "3m",
    "5m": "5m",
    "15m": "15m",
    "30m": "30m",
    "1h": "1h",
    "4h": "4h",
    "1d": "1d",
}
BINANCE_OI_INTERVALS = {
    "1m": "5m",
    "3m": "5m",
    "5m": "5m",
    "15m": "15m",
    "30m": "30m",
    "1h": "1h",
    "4h": "4h",
    "1d": "1d",
}
BYBIT_OI_INTERVALS = {
    "1m": "5min",
    "3m": "5min",
    "5m": "5min",
    "15m": "15min",
    "30m": "30min",
    "1h": "1h",
    "4h": "4h",
    "1d": "1d",
}
BYBIT_RATIO_INTERVALS = {
    "1m": "5min",
    "3m": "5min",
    "5m": "5min",
    "15m": "15min",
    "30m": "30min",
    "1h": "1h",
    "4h": "4h",
    "1d": "1d",
}
BITGET_RATIO_INTERVALS = {
    "1m": "5m",
    "3m": "5m",
    "5m": "5m",
    "15m": "15m",
    "30m": "30m",
    "1h": "1h",
    "4h": "4h",
    "1d": "1Dutc",
}
GATE_RATIO_INTERVALS = {
    "1m": "1m",
    "3m": "5m",
    "5m": "5m",
    "15m": "15m",
    "30m": "30m",
    "1h": "1h",
    "4h": "4h",
    "1d": "1d",
    "1w": "7d",
}
HTX_RATIO_INTERVALS = {
    "1m": "5min",
    "3m": "5min",
    "5m": "5min",
    "15m": "15min",
    "30m": "30min",
    "1h": "60min",
    "4h": "4hour",
    "1d": "1day",
}
OKX_RATIO_INTERVALS = {
    "1m": "5m",
    "3m": "5m",
    "5m": "5m",
    "15m": "15m",
    "30m": "30m",
    "1h": "1H",
    "4h": "4H",
    "1d": "1D",
}
OKX_GLOBAL_RATIO_INTERVALS = {
    "1m": "5m",
    "3m": "5m",
    "5m": "5m",
    "15m": "5m",
    "30m": "5m",
    "1h": "1H",
    "4h": "1H",
    "1d": "1D",
}

EXCHANGE_TITLE_MAP = {
    "binance": "Binance",
    "bybit": "Bybit",
    "bitget": "Bitget",
    "gate": "Gate",
    "htx": "HTX",
    "okx": "OKX",
    "hyperliquid": "Hyperliquid",
}

_REQUEST_HEALTH_LOCK = threading.Lock()
_REQUEST_HEALTH: Dict[str, Dict[str, Any]] = {}
_THREAD_LOCAL_CLIENTS = threading.local()
_FETCH_CACHE_LOCK = threading.Lock()
_FETCH_CACHE: Dict[Tuple[Any, ...], Tuple[float, Any]] = {}
_FETCH_CACHE_TTLS = {
    "all_snapshots": 2.0,
    "exchange_snapshot": 2.0,
    "exchange_candles": 8.0,
    "exchange_orderbook": 2.0,
    "exchange_oi_history": 10.0,
    "exchange_liquidations": 3.0,
    "exchange_trades": 2.0,
    "spot_snapshot": 2.0,
    "spot_candles": 8.0,
    "spot_orderbook": 2.0,
    "spot_trades": 2.0,
}
_REQUEST_COOLDOWN_SECONDS = {
    "legal": 180,
    "forbidden": 90,
    "rate_limit": 45,
    "timeout": 20,
    "transport": 25,
    "server": 18,
    "http": 30,
    "other": 15,
}
_RATE_LIMIT_RECOVERY_GRACE_SECONDS = 90
_BINANCE_PROBE_INTERVAL_SECONDS = 12
_BINANCE_RAMP_UP_SECONDS = 90
_BINANCE_RAMP_MIN_INTERVAL_MS = {
    "probe": 12_000,
    "light": 4_000,
    "normal": 12_000,
    "heavy": 45_000,
}


def safe_float(value: Optional[object]) -> Optional[float]:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def safe_int(value: Optional[object]) -> Optional[int]:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def normalize_onchain_address(value: Optional[object]) -> str:
    return str(value or "").strip().lower()


def is_valid_onchain_address(value: Optional[object]) -> bool:
    text = normalize_onchain_address(value)
    if len(text) != 42 or not text.startswith("0x"):
        return False
    return all(char in "0123456789abcdef" for char in text[2:])


def interval_to_millis(interval: str) -> int:
    mapping = {
        "1m": 60_000,
        "3m": 180_000,
        "5m": 300_000,
        "15m": 900_000,
        "30m": 1_800_000,
        "1h": 3_600_000,
        "4h": 14_400_000,
        "1d": 86_400_000,
    }
    return mapping.get(interval, 300_000)


def _clone_cached_payload(value: Any) -> Any:
    try:
        return copy.deepcopy(value)
    except Exception:
        return value


def _fetch_cache_get(cache_key: Tuple[Any, ...], ttl_seconds: float) -> Any:
    if ttl_seconds <= 0:
        return None
    now = time.time()
    with _FETCH_CACHE_LOCK:
        record = _FETCH_CACHE.get(cache_key)
        if not record:
            return None
        expires_at, payload = record
        if expires_at <= now:
            _FETCH_CACHE.pop(cache_key, None)
            return None
        return _clone_cached_payload(payload)


def _fetch_cache_set(cache_key: Tuple[Any, ...], ttl_seconds: float, payload: Any) -> Any:
    stored_payload = _clone_cached_payload(payload)
    with _FETCH_CACHE_LOCK:
        _FETCH_CACHE[cache_key] = (time.time() + max(float(ttl_seconds), 0.0), stored_payload)
    return _clone_cached_payload(stored_payload)


def _cached_fetch(kind: str, cache_key: Tuple[Any, ...], builder) -> Any:
    ttl_seconds = float(_FETCH_CACHE_TTLS.get(kind, 0.0) or 0.0)
    cached = _fetch_cache_get(cache_key, ttl_seconds)
    if cached is not None:
        return cached
    payload = builder()
    return _fetch_cache_set(cache_key, ttl_seconds, payload)


def _thread_local_client_store(store_name: str) -> Dict[Tuple[int, str], BaseClient]:
    store = getattr(_THREAD_LOCAL_CLIENTS, store_name, None)
    if not isinstance(store, dict):
        store = {}
        setattr(_THREAD_LOCAL_CLIENTS, store_name, store)
    return store


BITGET_CANDLE_INTERVALS = {
    "1m": "1m",
    "3m": "3m",
    "5m": "5m",
    "15m": "15m",
    "30m": "30m",
    "1h": "1H",
    "4h": "4H",
    "1d": "1D",
}


def default_symbols(coin: str) -> Dict[str, str]:
    coin = coin.upper().strip()
    return {
        "bybit": f"{coin}USDT",
        "binance": f"{coin}USDT",
        "okx": f"{coin}-USDT-SWAP",
        "hyperliquid": coin,
        "bitget": f"{coin}USDT",
    }


def default_spot_symbol(coin: str) -> str:
    return f"{coin.upper().strip()}USDT"


def default_spot_symbols(coin: str) -> Dict[str, str]:
    coin = coin.upper().strip()
    return {
        "binance": f"{coin}USDT",
        "bybit": f"{coin}USDT",
        "okx": f"{coin}-USDT",
    }


def _normalize_coin_code(value: Optional[object]) -> str:
    return str(value or "").strip().upper()


def _collect_bybit_instruments(client: "BaseClient", category: str) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    cursor = ""
    for _ in range(8):
        params = {"category": category, "limit": 1000}
        if cursor:
            params["cursor"] = cursor
        payload = client._request("GET", "/v5/market/instruments-info", params=params, track_health=False)
        result = payload.get("result", {}) if isinstance(payload, dict) else {}
        batch = result.get("list", []) if isinstance(result, dict) else []
        if isinstance(batch, list):
            rows.extend(item for item in batch if isinstance(item, dict))
        next_cursor = ""
        if isinstance(result, dict):
            next_cursor = str(result.get("nextPageCursor") or "").strip()
        if not next_cursor or next_cursor == cursor:
            break
        cursor = next_cursor
    return rows


def fetch_exchange_coin_catalog(timeout: int = DEFAULT_TIMEOUT) -> Dict[str, Any]:
    availability: Dict[str, Dict[str, Dict[str, bool]]] = {}
    status: Dict[str, Dict[str, str]] = {
        exchange_key: {"perp": "unknown", "spot": "unknown"}
        for exchange_key in EXCHANGE_ORDER
    }
    errors: Dict[str, Dict[str, str]] = {
        exchange_key: {"perp": "", "spot": ""}
        for exchange_key in EXCHANGE_ORDER
    }
    status["hyperliquid"]["spot"] = "unsupported"

    def register(exchange_key: str, market: str, coins: List[str]) -> None:
        unique_coins = sorted({_normalize_coin_code(item) for item in coins if _normalize_coin_code(item)})
        for coin in unique_coins:
            coin_entry = availability.setdefault(
                coin,
                {
                    key: {"perp": False, "spot": False}
                    for key in EXCHANGE_ORDER
                },
            )
            coin_entry.setdefault(exchange_key, {"perp": False, "spot": False})[market] = True

    client = None
    try:
        client = BinanceClient(timeout)
        binance_perp_payload = client._request("GET", "/fapi/v1/exchangeInfo", track_health=False)
        register(
            "binance",
            "perp",
            [
                item.get("baseAsset")
                for item in binance_perp_payload.get("symbols", [])
                if str(item.get("quoteAsset") or "").upper() == "USDT"
                and str(item.get("contractType") or "").upper() == "PERPETUAL"
                and str(item.get("status") or "").upper() == "TRADING"
            ],
        )
        status["binance"]["perp"] = "ok"
    except Exception as exc:
        status["binance"]["perp"] = "error"
        errors["binance"]["perp"] = str(exc)
    finally:
        try:
            client.close()
        except Exception:
            pass

    client = None
    try:
        client = BinanceSpotClient(timeout)
        binance_spot_payload = client._public_request("GET", "/api/v3/exchangeInfo", track_health=False)
        register(
            "binance",
            "spot",
            [
                item.get("baseAsset")
                for item in binance_spot_payload.get("symbols", [])
                if str(item.get("quoteAsset") or "").upper() == "USDT"
                and str(item.get("status") or "").upper() == "TRADING"
            ],
        )
        status["binance"]["spot"] = "ok"
    except Exception as exc:
        status["binance"]["spot"] = "error"
        errors["binance"]["spot"] = str(exc)
    finally:
        try:
            client.close()
        except Exception:
            pass

    client = None
    try:
        client = BybitClient(timeout)
        bybit_linear_rows = _collect_bybit_instruments(client, "linear")
        register(
            "bybit",
            "perp",
            [
                item.get("baseCoin")
                for item in bybit_linear_rows
                if str(item.get("quoteCoin") or "").upper() == "USDT"
                and str(item.get("status") or "").lower() in {"trading", "settling"}
            ],
        )
        status["bybit"]["perp"] = "ok"
    except Exception as exc:
        status["bybit"]["perp"] = "error"
        errors["bybit"]["perp"] = str(exc)
    finally:
        try:
            client.close()
        except Exception:
            pass

    client = None
    try:
        client = BybitSpotClient(timeout)
        bybit_spot_rows = _collect_bybit_instruments(client, "spot")
        register(
            "bybit",
            "spot",
            [
                item.get("baseCoin")
                for item in bybit_spot_rows
                if str(item.get("quoteCoin") or "").upper() == "USDT"
                and str(item.get("status") or "").lower() in {"trading", "settling"}
            ],
        )
        status["bybit"]["spot"] = "ok"
    except Exception as exc:
        status["bybit"]["spot"] = "error"
        errors["bybit"]["spot"] = str(exc)
    finally:
        try:
            client.close()
        except Exception:
            pass

    client = None
    try:
        client = OkxClient(timeout)
        okx_perp_payload = client._request("GET", "/api/v5/public/instruments", params={"instType": "SWAP"}, track_health=False)
        register(
            "okx",
            "perp",
            [
                str(item.get("instId") or "").split("-")[0]
                for item in okx_perp_payload.get("data", [])
                if "-USDT" in str(item.get("instId") or "").upper()
                and str(item.get("state") or "").lower() == "live"
            ],
        )
        status["okx"]["perp"] = "ok"
    except Exception as exc:
        status["okx"]["perp"] = "error"
        errors["okx"]["perp"] = str(exc)
    finally:
        try:
            client.close()
        except Exception:
            pass

    client = None
    try:
        client = OkxSpotClient(timeout)
        okx_spot_payload = client._request("GET", "/api/v5/public/instruments", params={"instType": "SPOT"}, track_health=False)
        register(
            "okx",
            "spot",
            [
                item.get("baseCcy") or str(item.get("instId") or "").split("-")[0]
                for item in okx_spot_payload.get("data", [])
                if str(item.get("quoteCcy") or "").upper() == "USDT"
                and str(item.get("state") or "").lower() == "live"
            ],
        )
        status["okx"]["spot"] = "ok"
    except Exception as exc:
        status["okx"]["spot"] = "error"
        errors["okx"]["spot"] = str(exc)
    finally:
        try:
            client.close()
        except Exception:
            pass

    client = None
    try:
        client = HyperliquidClient(timeout)
        hyper_meta_payload = client._request("POST", "/info", json={"type": "meta"}, track_health=False)
        universe = hyper_meta_payload.get("universe", []) if isinstance(hyper_meta_payload, dict) else []
        register(
            "hyperliquid",
            "perp",
            [item.get("name") for item in universe if isinstance(item, dict)],
        )
        status["hyperliquid"]["perp"] = "ok"
    except Exception as exc:
        status["hyperliquid"]["perp"] = "error"
        errors["hyperliquid"]["perp"] = str(exc)
    finally:
        try:
            client.close()
        except Exception:
            pass

    coins = sorted(availability)
    summary = {
        exchange_key: {
            "perp": sum(1 for coin in coins if availability.get(coin, {}).get(exchange_key, {}).get("perp")),
            "spot": sum(1 for coin in coins if availability.get(coin, {}).get(exchange_key, {}).get("spot")),
        }
        for exchange_key in EXCHANGE_ORDER
    }
    return {"coins": coins, "availability": availability, "summary": summary, "status": status, "errors": errors}


def compute_notional(price: Optional[float], size: Optional[float]) -> Optional[float]:
    if price is None or size is None:
        return None
    return price * size


def normalize_depth_limit(exchange_key: str, limit: int) -> int:
    if exchange_key == "binance":
        supported_limits = [5, 10, 20, 50, 100, 500, 1000]
        return min(supported_limits, key=lambda candidate: (abs(candidate - limit), candidate))
    return limit


def normalize_liquidation_side(value: Optional[object]) -> str:
    text = str(value or "").strip().lower()
    if text in {"long", "longs", "sell"}:
        return "long"
    if text in {"short", "shorts", "buy"}:
        return "short"
    return text or "unknown"


def normalize_liquidation_position_side(value: Optional[object]) -> str:
    text = str(value or "").strip().lower()
    if text in {"long", "longs", "buy"}:
        return "long"
    if text in {"short", "shorts", "sell"}:
        return "short"
    return text or "unknown"


def normalize_liquidation_side_for_exchange(exchange_key: Optional[object], value: Optional[object]) -> str:
    normalized_exchange = str(exchange_key or "").strip().lower()
    if normalized_exchange in {"bybit", "bitget"}:
        return normalize_liquidation_position_side(value)
    return normalize_liquidation_side(value)


def normalize_trade_side(value: Optional[object]) -> str:
    text = str(value or "").strip().lower()
    if text in {"buy", "bid", "b"}:
        return "buy"
    if text in {"sell", "ask", "a", "s"}:
        return "sell"
    return text or "unknown"


def _request_health_key(exchange_name: str) -> Tuple[str, str, str]:
    normalized_name = str(exchange_name or "").strip()
    lower_name = normalized_name.lower()
    market = "spot" if "spot" in lower_name else "perp"
    if "binance" in lower_name:
        exchange_key = "binance"
    elif "bybit" in lower_name:
        exchange_key = "bybit"
    elif "okx" in lower_name:
        exchange_key = "okx"
    elif "hyperliquid" in lower_name:
        exchange_key = "hyperliquid"
    else:
        exchange_key = lower_name.replace(" ", "-") or "unknown"
    display_name = EXCHANGE_TITLE_MAP.get(exchange_key, normalized_name or exchange_key.title())
    return exchange_key, market, display_name


def _request_health_defaults(exchange_name: str) -> Dict[str, Any]:
    exchange_key, market, display_name = _request_health_key(exchange_name)
    return {
        "exchange_key": exchange_key,
        "exchange_name": display_name,
        "market": market,
        "last_success_ms": None,
        "last_attempt_ms": None,
        "last_error_ms": None,
        "last_error": None,
        "last_status_code": None,
        "error_kind": None,
        "consecutive_failures": 0,
        "cooldown_until_ms": None,
        "status": "idle",
        "recovery_state": "normal",
        "recovery_probe_after_ms": None,
        "recovery_ramp_until_ms": None,
        "recovery_last_request_ms": None,
        "recovery_success_streak": 0,
    }


def _supports_probe_ramp(exchange_name: str) -> bool:
    exchange_key, _, _ = _request_health_key(exchange_name)
    return exchange_key == "binance"


def _sync_recovery_state_locked(exchange_name: str, entry: Dict[str, Any], now_ms: int) -> str:
    if not _supports_probe_ramp(exchange_name):
        entry["recovery_state"] = "normal"
        entry["recovery_probe_after_ms"] = None
        entry["recovery_ramp_until_ms"] = None
        entry["recovery_success_streak"] = 0
        return "normal"
    state = str(entry.get("recovery_state") or "normal")
    cooldown_until_ms = int(entry.get("cooldown_until_ms") or 0)
    if cooldown_until_ms and cooldown_until_ms > now_ms:
        entry["recovery_state"] = "cooldown"
        if not entry.get("recovery_probe_after_ms"):
            entry["recovery_probe_after_ms"] = cooldown_until_ms
        return "cooldown"
    if state == "cooldown":
        entry["recovery_state"] = "probe"
        entry["recovery_probe_after_ms"] = max(int(entry.get("recovery_probe_after_ms") or 0), now_ms)
        state = "probe"
    if state == "ramp_up":
        ramp_until_ms = int(entry.get("recovery_ramp_until_ms") or 0)
        success_streak = int(entry.get("recovery_success_streak") or 0)
        if ramp_until_ms and now_ms >= ramp_until_ms and success_streak >= 2:
            entry["recovery_state"] = "normal"
            entry["recovery_probe_after_ms"] = None
            entry["recovery_ramp_until_ms"] = None
            entry["recovery_success_streak"] = 0
            state = "normal"
    return str(entry.get("recovery_state") or "normal")


def _reserve_request_slot(exchange_name: str, request_tier: str) -> None:
    exchange_key, market, display_name = _request_health_key(exchange_name)
    cache_key = f"{exchange_key}:{market}"
    now_ms = int(time.time() * 1000)
    with _REQUEST_HEALTH_LOCK:
        entry = dict(_REQUEST_HEALTH.get(cache_key) or _request_health_defaults(exchange_name))
        cooldown_until_ms = int(entry.get("cooldown_until_ms") or 0)
        if cooldown_until_ms and cooldown_until_ms > now_ms:
            remaining_seconds = max(1, int((cooldown_until_ms - now_ms + 999) // 1000))
            raise RuntimeError(f"{display_name} REST 冷却中，{remaining_seconds}s 后自动重试")

        state = _sync_recovery_state_locked(exchange_name, entry, now_ms)
        last_request_ms = int(entry.get("recovery_last_request_ms") or 0)

        if state == "probe":
            probe_after_ms = int(entry.get("recovery_probe_after_ms") or 0)
            if probe_after_ms and now_ms < probe_after_ms:
                remaining_seconds = max(1, int((probe_after_ms - now_ms + 999) // 1000))
                raise RuntimeError(f"{display_name} REST 恢复探测将在 {remaining_seconds}s 后开始")
            if request_tier != "probe":
                raise RuntimeError(f"{display_name} REST 探测恢复中，等待轻量探测完成")
            min_gap_ms = int(_BINANCE_RAMP_MIN_INTERVAL_MS.get("probe", 12_000))
            if last_request_ms and now_ms - last_request_ms < min_gap_ms:
                remaining_seconds = max(1, int((min_gap_ms - (now_ms - last_request_ms) + 999) // 1000))
                raise RuntimeError(f"{display_name} REST 探测节流中，{remaining_seconds}s 后重试")
        elif state == "ramp_up":
            if request_tier == "heavy":
                raise RuntimeError(f"{display_name} REST 恢复爬坡中，暂不执行重型回补")
            min_gap_ms = int(_BINANCE_RAMP_MIN_INTERVAL_MS.get(request_tier, _BINANCE_RAMP_MIN_INTERVAL_MS["normal"]))
            if last_request_ms and now_ms - last_request_ms < min_gap_ms:
                remaining_seconds = max(1, int((min_gap_ms - (now_ms - last_request_ms) + 999) // 1000))
                raise RuntimeError(f"{display_name} REST 恢复爬坡中，{remaining_seconds}s 后重试")

        entry["recovery_last_request_ms"] = now_ms
        _REQUEST_HEALTH[cache_key] = entry


def _classify_request_error(exc: Exception) -> Tuple[Optional[int], str]:
    status_code = None
    if isinstance(exc, requests.HTTPError) and exc.response is not None:
        status_code = int(exc.response.status_code)
    elif getattr(exc, "response", None) is not None and getattr(exc.response, "status_code", None) is not None:
        try:
            status_code = int(exc.response.status_code)
        except (TypeError, ValueError):
            status_code = None
    if status_code == 451:
        return status_code, "legal"
    if status_code == 403:
        return status_code, "forbidden"
    if status_code in (418, 429):
        return status_code, "rate_limit"
    if status_code is not None and status_code >= 500:
        return status_code, "server"
    if status_code is not None:
        return status_code, "http"
    if isinstance(exc, requests.Timeout):
        return None, "timeout"
    if isinstance(exc, requests.RequestException):
        return None, "transport"
    return None, "other"


def _extract_remote_cooldown_until_ms(exc: Exception) -> Optional[int]:
    response = getattr(exc, "response", None)
    text = ""
    headers = {}
    if response is not None:
        try:
            text = response.text or ""
        except Exception:
            text = ""
        headers = getattr(response, "headers", {}) or {}
    match = re.search(r"banned until (\d+)", text, flags=re.IGNORECASE)
    if match:
        try:
            return int(match.group(1))
        except (TypeError, ValueError):
            return None
    retry_after = headers.get("Retry-After") if isinstance(headers, dict) else None
    if retry_after:
        try:
            return int(time.time() * 1000) + int(float(retry_after) * 1000)
        except (TypeError, ValueError):
            return None
    return None


def _current_cooldown_remaining_ms(exchange_name: str) -> int:
    exchange_key, market, _ = _request_health_key(exchange_name)
    cache_key = f"{exchange_key}:{market}"
    now_ms = int(time.time() * 1000)
    with _REQUEST_HEALTH_LOCK:
        entry = _REQUEST_HEALTH.get(cache_key)
        cooldown_until_ms = int(entry.get("cooldown_until_ms") or 0) if entry else 0
    return max(0, cooldown_until_ms - now_ms)


def _record_request_success(exchange_name: str, request_tier: str = "normal") -> None:
    exchange_key, market, display_name = _request_health_key(exchange_name)
    cache_key = f"{exchange_key}:{market}"
    now_ms = int(time.time() * 1000)
    with _REQUEST_HEALTH_LOCK:
        entry = dict(_REQUEST_HEALTH.get(cache_key) or _request_health_defaults(exchange_name))
        state = _sync_recovery_state_locked(exchange_name, entry, now_ms)
        recovery_state = "normal"
        recovery_probe_after_ms = None
        recovery_ramp_until_ms = None
        recovery_success_streak = 0
        status = "ok"
        if _supports_probe_ramp(exchange_name):
            if state in {"cooldown", "probe"} and request_tier == "probe":
                recovery_state = "ramp_up"
                recovery_ramp_until_ms = now_ms + _BINANCE_RAMP_UP_SECONDS * 1000
                recovery_success_streak = 1
                status = "ramp_up"
            elif state == "ramp_up":
                ramp_until_ms = int(entry.get("recovery_ramp_until_ms") or (now_ms + _BINANCE_RAMP_UP_SECONDS * 1000))
                recovery_success_streak = int(entry.get("recovery_success_streak") or 0) + 1
                if ramp_until_ms > now_ms:
                    recovery_state = "ramp_up"
                    recovery_ramp_until_ms = ramp_until_ms
                    status = "ramp_up"
        entry.update(
            {
                "exchange_key": exchange_key,
                "exchange_name": display_name,
                "market": market,
                "last_success_ms": now_ms,
                "last_attempt_ms": now_ms,
                "last_error_ms": None,
                "last_error": None,
                "last_status_code": None,
                "error_kind": None,
                "consecutive_failures": 0,
                "cooldown_until_ms": None,
                "status": status,
                "recovery_state": recovery_state,
                "recovery_probe_after_ms": recovery_probe_after_ms,
                "recovery_ramp_until_ms": recovery_ramp_until_ms,
                "recovery_success_streak": recovery_success_streak,
            }
        )
        _REQUEST_HEALTH[cache_key] = entry


def _record_request_failure(exchange_name: str, exc: Exception) -> None:
    exchange_key, market, display_name = _request_health_key(exchange_name)
    cache_key = f"{exchange_key}:{market}"
    now_ms = int(time.time() * 1000)
    status_code, error_kind = _classify_request_error(exc)
    cooldown_seconds = int(_REQUEST_COOLDOWN_SECONDS.get(error_kind, _REQUEST_COOLDOWN_SECONDS["other"]))
    remote_cooldown_until_ms = _extract_remote_cooldown_until_ms(exc)
    with _REQUEST_HEALTH_LOCK:
        entry = dict(_REQUEST_HEALTH.get(cache_key) or _request_health_defaults(exchange_name))
        consecutive_failures = int(entry.get("consecutive_failures") or 0) + 1
        should_cooldown = error_kind in {"legal", "forbidden", "rate_limit"} or consecutive_failures >= 2
        local_cooldown_until_ms = now_ms + cooldown_seconds * 1000 if should_cooldown else None
        cooldown_until_ms = local_cooldown_until_ms
        if should_cooldown and remote_cooldown_until_ms:
            cooldown_until_ms = max(int(local_cooldown_until_ms or 0), int(remote_cooldown_until_ms))
        if cooldown_until_ms and error_kind == "rate_limit":
            cooldown_until_ms = int(cooldown_until_ms) + _RATE_LIMIT_RECOVERY_GRACE_SECONDS * 1000
        entry.update(
            {
                "exchange_key": exchange_key,
                "exchange_name": display_name,
                "market": market,
                "last_attempt_ms": now_ms,
                "last_error_ms": now_ms,
                "last_error": str(exc),
                "last_status_code": status_code,
                "error_kind": error_kind,
                "consecutive_failures": consecutive_failures,
                "cooldown_until_ms": cooldown_until_ms,
                "status": "error",
                "recovery_state": "cooldown" if should_cooldown and _supports_probe_ramp(exchange_name) else entry.get("recovery_state") or "normal",
                "recovery_probe_after_ms": cooldown_until_ms if should_cooldown and _supports_probe_ramp(exchange_name) else entry.get("recovery_probe_after_ms"),
                "recovery_ramp_until_ms": None if should_cooldown else entry.get("recovery_ramp_until_ms"),
                "recovery_success_streak": 0 if should_cooldown else int(entry.get("recovery_success_streak") or 0),
            }
        )
        _REQUEST_HEALTH[cache_key] = entry


def describe_exchange_request_health() -> List[Dict[str, Any]]:
    now_ms = int(time.time() * 1000)
    rows: List[Dict[str, Any]] = []
    market_matrix = {
        "binance": ("perp", "spot"),
        "bybit": ("perp", "spot"),
        "okx": ("perp", "spot"),
        "hyperliquid": ("perp",),
    }
    with _REQUEST_HEALTH_LOCK:
        snapshot = {key: dict(value) for key, value in _REQUEST_HEALTH.items()}
    for exchange_key in EXCHANGE_ORDER:
        for market in market_matrix.get(exchange_key, ("perp",)):
            cache_key = f"{exchange_key}:{market}"
            entry = snapshot.get(cache_key, {})
            display_name = EXCHANGE_TITLE_MAP.get(exchange_key, exchange_key.title())
            cooldown_until_ms = int(entry.get("cooldown_until_ms") or 0)
            effective_status = str(entry.get("status") or "idle")
            recovery_state = str(entry.get("recovery_state") or "normal")
            if cooldown_until_ms and cooldown_until_ms > now_ms:
                if str(entry.get("error_kind") or "").lower() == "rate_limit":
                    effective_status = "cooldown"
                elif effective_status == "error":
                    effective_status = "backoff"
            elif recovery_state == "probe":
                effective_status = "probe"
            elif recovery_state == "ramp_up":
                effective_status = "ramp_up"
            rows.append(
                {
                    "exchange_key": exchange_key,
                    "exchange_name": display_name,
                    "market": market,
                    "last_success_ms": entry.get("last_success_ms"),
                    "last_attempt_ms": entry.get("last_attempt_ms"),
                    "last_error_ms": entry.get("last_error_ms"),
                    "last_error": entry.get("last_error"),
                    "last_status_code": entry.get("last_status_code"),
                    "error_kind": entry.get("error_kind"),
                    "consecutive_failures": int(entry.get("consecutive_failures") or 0),
                    "cooldown_until_ms": cooldown_until_ms or None,
                    "cooldown_remaining_s": max(0, int((cooldown_until_ms - now_ms + 999) // 1000)) if cooldown_until_ms else 0,
                    "status": effective_status,
                    "recovery_state": recovery_state,
                    "recovery_probe_after_ms": entry.get("recovery_probe_after_ms"),
                    "recovery_ramp_until_ms": entry.get("recovery_ramp_until_ms"),
                }
            )
    return rows


class BaseClient:
    exchange_name = "Unknown"
    base_url = ""

    def __init__(self, timeout: int = DEFAULT_TIMEOUT) -> None:
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "exchange-liquidity-gui/2.0"})
        retry = Retry(
            total=2,
            connect=2,
            read=2,
            backoff_factor=0.35,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=("GET", "POST"),
        )
        adapter = HTTPAdapter(max_retries=retry)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)

    def _request(self, method: str, path: str, *, params=None, json=None, track_health: bool = True, request_tier: str = "normal", base_url_override: Optional[str] = None):
        if track_health:
            _reserve_request_slot(self.exchange_name, request_tier)
        try:
            response = self.session.request(
                method,
                (base_url_override or self.base_url) + path,
                params=params,
                json=json,
                timeout=self.timeout,
            )
            response.raise_for_status()
            payload = response.json()
        except Exception as exc:
            if track_health:
                _record_request_failure(self.exchange_name, exc)
            raise
        if track_health:
            _record_request_success(self.exchange_name, request_tier=request_tier)
        return payload

    def fetch(self, symbol: str) -> ExchangeSnapshot:
        raise NotImplementedError

    def fetch_candles(self, symbol: str, interval: str, limit: int) -> List[Candle]:
        raise NotImplementedError

    def fetch_orderbook(self, symbol: str, limit: int) -> List[OrderBookLevel]:
        raise NotImplementedError

    def fetch_open_interest_history(self, symbol: str, interval: str, limit: int) -> List[OIPoint]:
        return []

    def fetch_liquidations(self, symbol: str, limit: int) -> List[LiquidationEvent]:
        return []

    def fetch_trades(self, symbol: str, limit: int) -> List[TradeEvent]:
        return []

    def close(self) -> None:
        try:
            self.session.close()
        except Exception:
            pass

    def _error(self, symbol: str, exc: Exception) -> ExchangeSnapshot:
        return ExchangeSnapshot(
            exchange=self.exchange_name,
            symbol=symbol,
            status="error",
            error=str(exc),
        )


class BybitClient(BaseClient):
    exchange_name = "Bybit"
    base_url = "https://api.bybit.com"

    def fetch(self, symbol: str) -> ExchangeSnapshot:
        try:
            payload = self._request(
                "GET",
                "/v5/market/tickers",
                params={"category": "linear", "symbol": symbol},
            )
            items = payload.get("result", {}).get("list", [])
            if not items:
                raise ValueError("empty ticker response")
            item = items[0]
            return ExchangeSnapshot(
                exchange=self.exchange_name,
                symbol=symbol,
                last_price=safe_float(item.get("lastPrice")),
                mark_price=safe_float(item.get("markPrice")),
                index_price=safe_float(item.get("indexPrice")),
                open_interest=safe_float(item.get("openInterest")),
                open_interest_notional=safe_float(item.get("openInterestValue")),
                funding_rate=safe_float(item.get("fundingRate")),
                volume_24h_base=safe_float(item.get("volume24h")),
                volume_24h_notional=safe_float(item.get("turnover24h")),
                timestamp_ms=safe_int(payload.get("time")),
                raw=item,
            )
        except Exception as exc:
            return self._error(symbol, exc)

    def fetch_candles(self, symbol: str, interval: str, limit: int) -> List[Candle]:
        payload = self._request(
            "GET",
            "/v5/market/kline",
            params={
                "category": "linear",
                "symbol": symbol,
                "interval": BYBIT_CANDLE_INTERVALS.get(interval, "5"),
                "limit": max(10, min(limit, 1000)),
            },
        )
        items = payload.get("result", {}).get("list", [])
        candles: List[Candle] = []
        for row in reversed(items):
            candles.append(
                Candle(
                    timestamp_ms=safe_int(row[0]) or 0,
                    open=safe_float(row[1]) or 0.0,
                    high=safe_float(row[2]) or 0.0,
                    low=safe_float(row[3]) or 0.0,
                    close=safe_float(row[4]) or 0.0,
                    volume=safe_float(row[5]) or 0.0,
                )
            )
        return candles

    def fetch_orderbook(self, symbol: str, limit: int) -> List[OrderBookLevel]:
        payload = self._request(
            "GET",
            "/v5/market/orderbook",
            params={"category": "linear", "symbol": symbol, "limit": max(1, min(limit, 200))},
        )
        result = payload.get("result", {})
        levels: List[OrderBookLevel] = []
        for price, size in result.get("b", []):
            levels.append(OrderBookLevel(price=safe_float(price) or 0.0, size=safe_float(size) or 0.0, side="bid"))
        for price, size in result.get("a", []):
            levels.append(OrderBookLevel(price=safe_float(price) or 0.0, size=safe_float(size) or 0.0, side="ask"))
        return levels

    def fetch_open_interest_history(self, symbol: str, interval: str, limit: int) -> List[OIPoint]:
        payload = self._request(
            "GET",
            "/v5/market/open-interest",
            params={
                "category": "linear",
                "symbol": symbol,
                "intervalTime": BYBIT_OI_INTERVALS.get(interval, "5min"),
                "limit": max(10, min(limit, 200)),
            },
        )
        items = payload.get("result", {}).get("list", [])
        points: List[OIPoint] = []
        for item in reversed(items):
            points.append(
                OIPoint(
                    timestamp_ms=safe_int(item.get("timestamp")) or 0,
                    open_interest=safe_float(item.get("openInterest")),
                    open_interest_notional=None,
                )
            )
        return points

    def fetch_trades(self, symbol: str, limit: int) -> List[TradeEvent]:
        payload = self._request(
            "GET",
            "/v5/market/recent-trade",
            params={"category": "linear", "symbol": symbol, "limit": max(10, min(limit, 1000))},
        )
        items = payload.get("result", {}).get("list", [])
        events: List[TradeEvent] = []
        for item in items:
            price = safe_float(item.get("price"))
            size = safe_float(item.get("size"))
            events.append(
                TradeEvent(
                    exchange=self.exchange_name,
                    symbol=symbol,
                    timestamp_ms=safe_int(item.get("time")) or 0,
                    side=normalize_trade_side(item.get("side")),
                    price=price,
                    size=size,
                    notional=compute_notional(price, size),
                    source="rest",
                    raw=item,
                )
            )
        return events


class BitgetPublicClient(BaseClient):
    exchange_name = "Bitget"
    base_url = "https://api.bitget.com"

    def __init__(self, timeout: int = DEFAULT_TIMEOUT) -> None:
        super().__init__(timeout)
        self._all_tickers_cache: Dict[str, Any] = {"rows": [], "fetched_at_ms": 0}

    def _fetch_all_tickers(self) -> List[Dict[str, Any]]:
        now_ms = int(time.time() * 1000)
        cached_rows = self._all_tickers_cache.get("rows")
        fetched_at_ms = int(self._all_tickers_cache.get("fetched_at_ms") or 0)
        if isinstance(cached_rows, list) and now_ms - fetched_at_ms <= 20_000:
            return [dict(item) for item in cached_rows if isinstance(item, dict)]
        payload = self._request(
            "GET",
            "/api/v2/mix/market/tickers",
            params={"productType": "USDT-FUTURES"},
            request_tier="light",
        )
        rows = payload.get("data", []) if isinstance(payload, dict) else []
        normalized_rows = [dict(item) for item in rows if isinstance(item, dict)]
        self._all_tickers_cache = {"rows": normalized_rows, "fetched_at_ms": now_ms}
        return [dict(item) for item in normalized_rows]

    def fetch(self, symbol: str) -> ExchangeSnapshot:
        try:
            normalized_symbol = str(symbol or "").strip().upper()
            item = next(
                (row for row in self._fetch_all_tickers() if str(row.get("symbol") or "").strip().upper() == normalized_symbol),
                None,
            )
            if not isinstance(item, dict):
                raise ValueError(f"symbol {normalized_symbol} not found in Bitget futures tickers")
            mark_price = safe_float(item.get("markPrice"))
            last_price = safe_float(item.get("lastPr"))
            open_interest = safe_float(item.get("holdingAmount"))
            open_interest_notional = None
            if open_interest is not None and mark_price is not None:
                open_interest_notional = open_interest * mark_price
            volume_24h_base = safe_float(item.get("baseVolume"))
            volume_24h_notional = safe_float(item.get("usdtVolume")) or safe_float(item.get("quoteVolume"))
            return ExchangeSnapshot(
                exchange=self.exchange_name,
                symbol=normalized_symbol,
                last_price=last_price,
                mark_price=mark_price,
                index_price=safe_float(item.get("indexPrice")),
                open_interest=open_interest,
                open_interest_notional=open_interest_notional,
                funding_rate=safe_float(item.get("fundingRate")),
                volume_24h_base=volume_24h_base,
                volume_24h_notional=volume_24h_notional,
                timestamp_ms=safe_int(item.get("ts")) or int(time.time() * 1000),
                raw=item,
            )
        except Exception as exc:
            return self._error(symbol, exc)

    def fetch_candles(self, symbol: str, interval: str, limit: int) -> List[Candle]:
        payload = self._request(
            "GET",
            "/api/v2/mix/market/candles",
            params={
                "symbol": str(symbol or "").strip().upper(),
                "productType": "USDT-FUTURES",
                "granularity": BITGET_CANDLE_INTERVALS.get(interval, "5m"),
                "limit": max(10, min(limit, 200)),
            },
            request_tier="heavy",
        )
        items = payload.get("data", []) if isinstance(payload, dict) else []
        candles: List[Candle] = []
        for row in items:
            if not isinstance(row, (list, tuple)) or len(row) < 6:
                continue
            candles.append(
                Candle(
                    timestamp_ms=safe_int(row[0]) or 0,
                    open=safe_float(row[1]) or 0.0,
                    high=safe_float(row[2]) or 0.0,
                    low=safe_float(row[3]) or 0.0,
                    close=safe_float(row[4]) or 0.0,
                    volume=safe_float(row[5]) or 0.0,
                )
            )
        return candles

    def fetch_orderbook(self, symbol: str, limit: int) -> List[OrderBookLevel]:
        payload = self._request(
            "GET",
            "/api/v2/mix/market/merge-depth",
            params={
                "symbol": str(symbol or "").strip().upper(),
                "productType": "USDT-FUTURES",
                "limit": max(5, min(limit, 200)),
            },
            request_tier="normal",
        )
        data = payload.get("data", {}) if isinstance(payload, dict) else {}
        levels: List[OrderBookLevel] = []
        for price, size, *_ in data.get("bids", []) or []:
            levels.append(OrderBookLevel(price=safe_float(price) or 0.0, size=safe_float(size) or 0.0, side="bid"))
        for price, size, *_ in data.get("asks", []) or []:
            levels.append(OrderBookLevel(price=safe_float(price) or 0.0, size=safe_float(size) or 0.0, side="ask"))
        return levels

    def fetch_open_interest_history(self, symbol: str, interval: str, limit: int) -> List[OIPoint]:
        snapshot = self.fetch(symbol)
        open_interest = snapshot.open_interest
        open_interest_notional = snapshot.open_interest_notional
        timestamp_ms = snapshot.timestamp_ms or int(time.time() * 1000)
        if open_interest is None and open_interest_notional is None:
            return []
        return [
            OIPoint(
                timestamp_ms=timestamp_ms,
                open_interest=open_interest,
                open_interest_notional=open_interest_notional,
            )
        ]


class GatePublicClient(BaseClient):
    exchange_name = "Gate"
    base_url = "https://api.gateio.ws/api/v4"


class HtxPublicClient(BaseClient):
    exchange_name = "HTX"
    base_url = "https://api.hbdm.com"


class BinanceClient(BaseClient):
    exchange_name = "Binance"
    base_url = "https://fapi.binance.com"

    def __init__(self, timeout: int = DEFAULT_TIMEOUT) -> None:
        super().__init__(timeout)
        self._snapshot_cache: Dict[str, ExchangeSnapshot] = {}
        self._open_interest_cache: Dict[str, Dict[str, Any]] = {}
        self._open_interest_ttl_ms = 45_000

    def _fallback_snapshot(self, symbol: str, exc: Exception) -> ExchangeSnapshot:
        previous = self._snapshot_cache.get(symbol)
        if previous is None:
            return self._error(symbol, exc)
        snapshot = replace(previous)
        snapshot.status = "proxy" if "冷却" in str(exc) or "恢复" in str(exc) else "degraded"
        snapshot.error = str(exc)
        return snapshot

    def _maybe_refresh_open_interest(self, symbol: str) -> Optional[Dict[str, Any]]:
        cached = self._open_interest_cache.get(symbol)
        now_ms = int(time.time() * 1000)
        if cached and now_ms - int(cached.get("fetched_at_ms") or 0) <= self._open_interest_ttl_ms:
            return dict(cached.get("payload") or {})
        try:
            payload = self._request(
                "GET",
                "/fapi/v1/openInterest",
                params={"symbol": symbol},
                request_tier="light",
            )
        except Exception:
            if cached:
                return dict(cached.get("payload") or {})
            return None
        self._open_interest_cache[symbol] = {
            "payload": dict(payload or {}),
            "fetched_at_ms": now_ms,
        }
        return dict(payload or {})

    def fetch(self, symbol: str) -> ExchangeSnapshot:
        try:
            previous = self._snapshot_cache.get(symbol)
            premium = self._request("GET", "/fapi/v1/premiumIndex", params={"symbol": symbol}, request_tier="probe")
            open_interest_payload = self._maybe_refresh_open_interest(symbol)
            mark_price = safe_float(premium.get("markPrice"))
            open_interest = safe_float((open_interest_payload or {}).get("openInterest"))
            open_interest_notional = None
            if open_interest is not None and mark_price is not None:
                open_interest_notional = open_interest * mark_price
            elif previous is not None:
                open_interest_notional = previous.open_interest_notional

            snapshot = ExchangeSnapshot(
                exchange=self.exchange_name,
                symbol=symbol,
                last_price=(previous.last_price if previous is not None else None) or mark_price,
                mark_price=mark_price,
                index_price=safe_float(premium.get("indexPrice")) or (previous.index_price if previous is not None else None),
                open_interest=open_interest if open_interest is not None else (previous.open_interest if previous is not None else None),
                open_interest_notional=open_interest_notional,
                funding_rate=safe_float(premium.get("lastFundingRate")) if safe_float(premium.get("lastFundingRate")) is not None else (previous.funding_rate if previous is not None else None),
                volume_24h_base=previous.volume_24h_base if previous is not None else None,
                volume_24h_notional=previous.volume_24h_notional if previous is not None else None,
                timestamp_ms=safe_int(premium.get("time")) or (previous.timestamp_ms if previous is not None else None) or int(time.time() * 1000),
                raw={
                    "premium_index": premium,
                    "open_interest": open_interest_payload,
                },
            )
            self._snapshot_cache[symbol] = replace(snapshot)
            return snapshot
        except Exception as exc:
            return self._fallback_snapshot(symbol, exc)

    def fetch_candles(self, symbol: str, interval: str, limit: int) -> List[Candle]:
        payload = self._request(
            "GET",
            "/fapi/v1/klines",
            params={
                "symbol": symbol,
                "interval": BINANCE_CANDLE_INTERVALS.get(interval, "5m"),
                "limit": max(10, min(limit, 1500)),
            },
            request_tier="heavy",
        )
        candles: List[Candle] = []
        for row in payload:
            candles.append(
                Candle(
                    timestamp_ms=safe_int(row[0]) or 0,
                    open=safe_float(row[1]) or 0.0,
                    high=safe_float(row[2]) or 0.0,
                    low=safe_float(row[3]) or 0.0,
                    close=safe_float(row[4]) or 0.0,
                    volume=safe_float(row[5]) or 0.0,
                )
            )
        return candles

    def fetch_orderbook(self, symbol: str, limit: int) -> List[OrderBookLevel]:
        normalized_limit = normalize_depth_limit("binance", max(5, min(limit, 1000)))
        payload = self._request(
            "GET",
            "/fapi/v1/depth",
            params={"symbol": symbol, "limit": normalized_limit},
            request_tier="normal",
        )
        levels: List[OrderBookLevel] = []
        for price, size in payload.get("bids", []):
            levels.append(OrderBookLevel(price=safe_float(price) or 0.0, size=safe_float(size) or 0.0, side="bid"))
        for price, size in payload.get("asks", []):
            levels.append(OrderBookLevel(price=safe_float(price) or 0.0, size=safe_float(size) or 0.0, side="ask"))
        return levels

    def fetch_open_interest_history(self, symbol: str, interval: str, limit: int) -> List[OIPoint]:
        payload = self._request(
            "GET",
            "/futures/data/openInterestHist",
            params={
                "symbol": symbol,
                "period": BINANCE_OI_INTERVALS.get(interval, "5m"),
                "limit": max(10, min(limit, 500)),
            },
            request_tier="heavy",
        )
        points: List[OIPoint] = []
        for item in payload:
            points.append(
                OIPoint(
                    timestamp_ms=safe_int(item.get("timestamp")) or 0,
                    open_interest=safe_float(item.get("sumOpenInterest")),
                    open_interest_notional=safe_float(item.get("sumOpenInterestValue")),
                )
            )
        return points

    def fetch_trades(self, symbol: str, limit: int) -> List[TradeEvent]:
        payload = self._request(
            "GET",
            "/fapi/v1/aggTrades",
            params={"symbol": symbol, "limit": max(10, min(limit, 1000))},
            request_tier="normal",
        )
        events: List[TradeEvent] = []
        for item in payload:
            price = safe_float(item.get("p"))
            size = safe_float(item.get("q"))
            events.append(
                TradeEvent(
                    exchange=self.exchange_name,
                    symbol=symbol,
                    timestamp_ms=safe_int(item.get("T")) or 0,
                    side="sell" if bool(item.get("m")) else "buy",
                    price=price,
                    size=size,
                    notional=compute_notional(price, size),
                    source="rest",
                    raw=item,
                )
            )
        return events

    def fetch_liquidations(self, symbol: str, limit: int) -> List[LiquidationEvent]:
        _reserve_request_slot(self.exchange_name, "heavy")
        try:
            response = self.session.request(
                "GET",
                self.base_url + "/fapi/v1/allForceOrders",
                params={"symbol": symbol, "limit": max(10, min(limit, 100))},
                timeout=self.timeout,
            )
            if response.status_code == 400 and "out of maintenance" in response.text.lower():
                _record_request_success(self.exchange_name, request_tier="heavy")
                return []
            response.raise_for_status()
            payload = response.json()
        except Exception as exc:
            _record_request_failure(self.exchange_name, exc)
            raise
        _record_request_success(self.exchange_name, request_tier="heavy")
        items = payload if isinstance(payload, list) else payload.get("data", [])
        events: List[LiquidationEvent] = []
        for item in items:
            price = safe_float(item.get("avgPrice")) or safe_float(item.get("averagePrice")) or safe_float(item.get("price"))
            size = safe_float(item.get("executedQty")) or safe_float(item.get("origQty"))
            notional = compute_notional(price, size) or safe_float(item.get("cumQuote"))
            events.append(
                LiquidationEvent(
                    exchange=self.exchange_name,
                    symbol=symbol,
                    timestamp_ms=safe_int(item.get("time")) or safe_int(item.get("updatedTime")) or 0,
                    side=normalize_liquidation_side(item.get("side")),
                    price=price,
                    size=size,
                    notional=notional,
                    source="rest",
                    raw=item,
                )
            )
        return events


class BinanceSpotClient(BaseClient):
    exchange_name = "Binance Spot"
    base_url = "https://api.binance.com"

    def __init__(self, timeout: int = DEFAULT_TIMEOUT) -> None:
        super().__init__(timeout)
        self.public_base_url = "https://data-api.binance.vision"
        self._snapshot_cache: Dict[str, SpotSnapshot] = {}
        self._stats_cache: Dict[str, Dict[str, Any]] = {}
        self._stats_ttl_ms = 60_000

    def _public_request(self, method: str, path: str, *, params=None, json=None, track_health: bool = True, request_tier: str = "normal"):
        return self._request(
            method,
            path,
            params=params,
            json=json,
            track_health=track_health,
            request_tier=request_tier,
            base_url_override=self.public_base_url,
        )

    def _fallback_snapshot(self, symbol: str, exc: Exception) -> SpotSnapshot:
        previous = self._snapshot_cache.get(symbol)
        if previous is None:
            return SpotSnapshot(exchange=self.exchange_name, symbol=symbol, status="error", error=str(exc))
        snapshot = replace(previous)
        snapshot.status = "proxy" if "冷却" in str(exc) or "恢复" in str(exc) else "degraded"
        snapshot.error = str(exc)
        return snapshot

    def _maybe_refresh_stats(self, symbol: str) -> Optional[Dict[str, Any]]:
        cached = self._stats_cache.get(symbol)
        now_ms = int(time.time() * 1000)
        if cached and now_ms - int(cached.get("fetched_at_ms") or 0) <= self._stats_ttl_ms:
            return dict(cached.get("payload") or {})
        try:
            payload = self._public_request(
                "GET",
                "/api/v3/ticker/24hr",
                params={"symbol": symbol},
                request_tier="light",
            )
        except Exception:
            if cached:
                return dict(cached.get("payload") or {})
            return None
        self._stats_cache[symbol] = {
            "payload": dict(payload or {}),
            "fetched_at_ms": now_ms,
        }
        return dict(payload or {})

    def fetch(self, symbol: str) -> SpotSnapshot:
        try:
            previous = self._snapshot_cache.get(symbol)
            book = self._public_request("GET", "/api/v3/ticker/bookTicker", params={"symbol": symbol}, request_tier="probe")
            stats = self._maybe_refresh_stats(symbol)
            bid_price = safe_float(book.get("bidPrice"))
            ask_price = safe_float(book.get("askPrice"))
            midpoint = None
            if bid_price is not None and ask_price is not None:
                midpoint = (bid_price + ask_price) / 2.0
            snapshot = SpotSnapshot(
                exchange=self.exchange_name,
                symbol=symbol,
                last_price=safe_float((stats or {}).get("lastPrice")) or midpoint or (previous.last_price if previous is not None else None),
                bid_price=bid_price if bid_price is not None else (previous.bid_price if previous is not None else None),
                ask_price=ask_price if ask_price is not None else (previous.ask_price if previous is not None else None),
                volume_24h_base=safe_float((stats or {}).get("volume")) if safe_float((stats or {}).get("volume")) is not None else (previous.volume_24h_base if previous is not None else None),
                volume_24h_notional=safe_float((stats or {}).get("quoteVolume")) if safe_float((stats or {}).get("quoteVolume")) is not None else (previous.volume_24h_notional if previous is not None else None),
                timestamp_ms=safe_int((stats or {}).get("closeTime")) or (previous.timestamp_ms if previous is not None else None) or int(time.time() * 1000),
                raw={"ticker_24h": stats, "book_ticker": book},
            )
            self._snapshot_cache[symbol] = replace(snapshot)
            return snapshot
        except Exception as exc:
            return self._fallback_snapshot(symbol, exc)

    def fetch_candles(self, symbol: str, interval: str, limit: int) -> List[Candle]:
        payload = self._public_request(
            "GET",
            "/api/v3/klines",
            params={
                "symbol": symbol,
                "interval": BINANCE_CANDLE_INTERVALS.get(interval, "5m"),
                "limit": max(10, min(limit, 1500)),
            },
            request_tier="heavy",
        )
        candles: List[Candle] = []
        for row in payload:
            candles.append(
                Candle(
                    timestamp_ms=safe_int(row[0]) or 0,
                    open=safe_float(row[1]) or 0.0,
                    high=safe_float(row[2]) or 0.0,
                    low=safe_float(row[3]) or 0.0,
                    close=safe_float(row[4]) or 0.0,
                    volume=safe_float(row[5]) or 0.0,
                )
            )
        return candles

    def fetch_orderbook(self, symbol: str, limit: int) -> List[OrderBookLevel]:
        normalized_limit = normalize_depth_limit("binance", max(5, min(limit, 1000)))
        payload = self._public_request(
            "GET",
            "/api/v3/depth",
            params={"symbol": symbol, "limit": normalized_limit},
            request_tier="normal",
        )
        levels: List[OrderBookLevel] = []
        for price, size in payload.get("bids", []):
            levels.append(OrderBookLevel(price=safe_float(price) or 0.0, size=safe_float(size) or 0.0, side="bid"))
        for price, size in payload.get("asks", []):
            levels.append(OrderBookLevel(price=safe_float(price) or 0.0, size=safe_float(size) or 0.0, side="ask"))
        return levels

    def fetch_trades(self, symbol: str, limit: int) -> List[TradeEvent]:
        payload = self._public_request(
            "GET",
            "/api/v3/trades",
            params={"symbol": symbol, "limit": max(10, min(limit, 1000))},
            request_tier="normal",
        )
        events: List[TradeEvent] = []
        for item in payload:
            price = safe_float(item.get("price"))
            size = safe_float(item.get("qty"))
            events.append(
                TradeEvent(
                    exchange=self.exchange_name,
                    symbol=symbol,
                    timestamp_ms=safe_int(item.get("time")) or 0,
                    side="sell" if bool(item.get("isBuyerMaker")) else "buy",
                    price=price,
                    size=size,
                    notional=compute_notional(price, size),
                    source="rest",
                    raw=item,
                )
            )
        return events


class BybitSpotClient(BaseClient):
    exchange_name = "Bybit Spot"
    base_url = "https://api.bybit.com"

    def fetch(self, symbol: str) -> SpotSnapshot:
        try:
            payload = self._request("GET", "/v5/market/tickers", params={"category": "spot", "symbol": symbol})
            items = payload.get("result", {}).get("list", [])
            if not items:
                raise ValueError("empty ticker response")
            item = items[0]
            return SpotSnapshot(
                exchange=self.exchange_name,
                symbol=symbol,
                last_price=safe_float(item.get("lastPrice")),
                bid_price=safe_float(item.get("bid1Price")),
                ask_price=safe_float(item.get("ask1Price")),
                volume_24h_base=safe_float(item.get("volume24h")),
                volume_24h_notional=safe_float(item.get("turnover24h")),
                timestamp_ms=safe_int(payload.get("time")),
                raw=item,
            )
        except Exception as exc:
            return SpotSnapshot(exchange=self.exchange_name, symbol=symbol, status="error", error=str(exc))

    def fetch_candles(self, symbol: str, interval: str, limit: int) -> List[Candle]:
        payload = self._request(
            "GET",
            "/v5/market/kline",
            params={
                "category": "spot",
                "symbol": symbol,
                "interval": BYBIT_CANDLE_INTERVALS.get(interval, "5"),
                "limit": max(10, min(limit, 1000)),
            },
        )
        items = payload.get("result", {}).get("list", [])
        candles: List[Candle] = []
        for row in reversed(items):
            candles.append(
                Candle(
                    timestamp_ms=safe_int(row[0]) or 0,
                    open=safe_float(row[1]) or 0.0,
                    high=safe_float(row[2]) or 0.0,
                    low=safe_float(row[3]) or 0.0,
                    close=safe_float(row[4]) or 0.0,
                    volume=safe_float(row[5]) or 0.0,
                )
            )
        return candles

    def fetch_orderbook(self, symbol: str, limit: int) -> List[OrderBookLevel]:
        payload = self._request(
            "GET",
            "/v5/market/orderbook",
            params={"category": "spot", "symbol": symbol, "limit": max(1, min(limit, 200))},
        )
        result = payload.get("result", {})
        levels: List[OrderBookLevel] = []
        for price, size in result.get("b", []):
            levels.append(OrderBookLevel(price=safe_float(price) or 0.0, size=safe_float(size) or 0.0, side="bid"))
        for price, size in result.get("a", []):
            levels.append(OrderBookLevel(price=safe_float(price) or 0.0, size=safe_float(size) or 0.0, side="ask"))
        return levels

    def fetch_trades(self, symbol: str, limit: int) -> List[TradeEvent]:
        payload = self._request(
            "GET",
            "/v5/market/recent-trade",
            params={"category": "spot", "symbol": symbol, "limit": max(10, min(limit, 1000))},
        )
        items = payload.get("result", {}).get("list", [])
        events: List[TradeEvent] = []
        for item in items:
            price = safe_float(item.get("price"))
            size = safe_float(item.get("size"))
            events.append(
                TradeEvent(
                    exchange=self.exchange_name,
                    symbol=symbol,
                    timestamp_ms=safe_int(item.get("time")) or 0,
                    side=normalize_trade_side(item.get("side")),
                    price=price,
                    size=size,
                    notional=compute_notional(price, size),
                    source="rest",
                    raw=item,
                )
            )
        return events


class OkxSpotClient(BaseClient):
    exchange_name = "OKX Spot"
    base_url = "https://www.okx.com"

    def fetch(self, symbol: str) -> SpotSnapshot:
        try:
            payload = self._request("GET", "/api/v5/market/ticker", params={"instId": symbol})
            item = (payload.get("data") or [{}])[0]
            return SpotSnapshot(
                exchange=self.exchange_name,
                symbol=symbol,
                last_price=safe_float(item.get("last")),
                bid_price=safe_float(item.get("bidPx")),
                ask_price=safe_float(item.get("askPx")),
                volume_24h_base=safe_float(item.get("vol24h")),
                volume_24h_notional=safe_float(item.get("volCcy24h")),
                timestamp_ms=safe_int(item.get("ts")),
                raw=item,
            )
        except Exception as exc:
            return SpotSnapshot(exchange=self.exchange_name, symbol=symbol, status="error", error=str(exc))

    def fetch_candles(self, symbol: str, interval: str, limit: int) -> List[Candle]:
        payload = self._request(
            "GET",
            "/api/v5/market/candles",
            params={
                "instId": symbol,
                "bar": OKX_CANDLE_INTERVALS.get(interval, "5m"),
                "limit": max(10, min(limit, 300)),
            },
        )
        items = payload.get("data", [])
        candles: List[Candle] = []
        for row in reversed(items):
            candles.append(
                Candle(
                    timestamp_ms=safe_int(row[0]) or 0,
                    open=safe_float(row[1]) or 0.0,
                    high=safe_float(row[2]) or 0.0,
                    low=safe_float(row[3]) or 0.0,
                    close=safe_float(row[4]) or 0.0,
                    volume=safe_float(row[5]) or 0.0,
                )
            )
        return candles

    def fetch_orderbook(self, symbol: str, limit: int) -> List[OrderBookLevel]:
        payload = self._request(
            "GET",
            "/api/v5/market/books",
            params={"instId": symbol, "sz": max(1, min(limit, 400))},
        )
        item = (payload.get("data") or [{}])[0]
        levels: List[OrderBookLevel] = []
        for price, size, *_ in item.get("bids", []):
            levels.append(OrderBookLevel(price=safe_float(price) or 0.0, size=safe_float(size) or 0.0, side="bid"))
        for price, size, *_ in item.get("asks", []):
            levels.append(OrderBookLevel(price=safe_float(price) or 0.0, size=safe_float(size) or 0.0, side="ask"))
        return levels

    def fetch_trades(self, symbol: str, limit: int) -> List[TradeEvent]:
        payload = self._request(
            "GET",
            "/api/v5/market/trades",
            params={"instId": symbol, "limit": max(10, min(limit, 100))},
        )
        items = payload.get("data", [])
        events: List[TradeEvent] = []
        for item in items:
            price = safe_float(item.get("px"))
            size = safe_float(item.get("sz"))
            events.append(
                TradeEvent(
                    exchange=self.exchange_name,
                    symbol=symbol,
                    timestamp_ms=safe_int(item.get("ts")) or 0,
                    side=normalize_trade_side(item.get("side")),
                    price=price,
                    size=size,
                    notional=compute_notional(price, size),
                    source="rest",
                    raw=item,
                )
            )
        return events


class OkxClient(BaseClient):
    exchange_name = "OKX"
    base_url = "https://www.okx.com"

    def fetch(self, symbol: str) -> ExchangeSnapshot:
        try:
            ticker_payload = self._request("GET", "/api/v5/market/ticker", params={"instId": symbol})
            mark_payload = self._request(
                "GET",
                "/api/v5/public/mark-price",
                params={"instType": "SWAP", "instId": symbol},
            )
            oi_payload = self._request(
                "GET",
                "/api/v5/public/open-interest",
                params={"instType": "SWAP", "instId": symbol},
            )
            funding_payload = self._request(
                "GET",
                "/api/v5/public/funding-rate",
                params={"instId": symbol},
            )

            ticker = (ticker_payload.get("data") or [{}])[0]
            mark = (mark_payload.get("data") or [{}])[0]
            oi_item = (oi_payload.get("data") or [{}])[0]
            funding = (funding_payload.get("data") or [{}])[0]
            index_item: Dict[str, Any] = {}
            volume_candles: List[Any] = []
            index_symbol = "-".join(str(symbol or "").split("-")[:2])
            if index_symbol:
                try:
                    index_payload = self._request("GET", "/api/v5/market/index-tickers", params={"instId": index_symbol})
                    index_item = (index_payload.get("data") or [{}])[0]
                except Exception:
                    index_item = {}
            try:
                volume_payload = self._request(
                    "GET",
                    "/api/v5/market/candles",
                    params={"instId": symbol, "bar": "1H", "limit": 24},
                )
                volume_candles = volume_payload.get("data", []) or []
            except Exception:
                volume_candles = []

            open_interest = safe_float(oi_item.get("oi"))
            open_interest_notional = safe_float(oi_item.get("oiUsd"))
            mark_price = safe_float(mark.get("markPx"))
            last_price = safe_float(ticker.get("last"))
            index_price = safe_float(index_item.get("idxPx"))
            if open_interest_notional is None and open_interest is not None and mark_price is not None:
                open_interest_notional = open_interest * mark_price
            # OKX derivatives expose vol24h in contracts and volCcy24h in base currency.
            volume_24h_base = safe_float(ticker.get("volCcy24h"))
            volume_reference_price = last_price or mark_price or index_price
            volume_24h_notional = None
            quote_candle_notional = sum(
                safe_float(row[7]) or 0.0
                for row in volume_candles
                if isinstance(row, list) and len(row) >= 8
            )
            if quote_candle_notional > 0:
                volume_24h_notional = quote_candle_notional
            elif volume_24h_base is not None and volume_reference_price is not None:
                volume_24h_notional = volume_24h_base * volume_reference_price

            return ExchangeSnapshot(
                exchange=self.exchange_name,
                symbol=symbol,
                last_price=last_price,
                mark_price=mark_price,
                index_price=index_price,
                open_interest=open_interest,
                open_interest_notional=open_interest_notional,
                funding_rate=safe_float(funding.get("fundingRate")),
                volume_24h_base=volume_24h_base,
                volume_24h_notional=volume_24h_notional,
                timestamp_ms=safe_int(ticker.get("ts")),
                raw={
                    "ticker": ticker,
                    "mark_price": mark,
                    "index_ticker": index_item,
                    "volume_candles": volume_candles,
                    "open_interest": oi_item,
                    "funding_rate": funding,
                },
            )
        except Exception as exc:
            return self._error(symbol, exc)

    def fetch_candles(self, symbol: str, interval: str, limit: int) -> List[Candle]:
        payload = self._request(
            "GET",
            "/api/v5/market/candles",
            params={
                "instId": symbol,
                "bar": OKX_CANDLE_INTERVALS.get(interval, "5m"),
                "limit": max(10, min(limit, 300)),
            },
        )
        items = payload.get("data", [])
        candles: List[Candle] = []
        for row in reversed(items):
            candles.append(
                Candle(
                    timestamp_ms=safe_int(row[0]) or 0,
                    open=safe_float(row[1]) or 0.0,
                    high=safe_float(row[2]) or 0.0,
                    low=safe_float(row[3]) or 0.0,
                    close=safe_float(row[4]) or 0.0,
                    volume=safe_float(row[5]) or 0.0,
                )
            )
        return candles

    def fetch_orderbook(self, symbol: str, limit: int) -> List[OrderBookLevel]:
        payload = self._request(
            "GET",
            "/api/v5/market/books",
            params={"instId": symbol, "sz": max(1, min(limit, 400))},
        )
        data = (payload.get("data") or [{}])[0]
        levels: List[OrderBookLevel] = []
        for price, size, *_ in data.get("bids", []):
            levels.append(OrderBookLevel(price=safe_float(price) or 0.0, size=safe_float(size) or 0.0, side="bid"))
        for price, size, *_ in data.get("asks", []):
            levels.append(OrderBookLevel(price=safe_float(price) or 0.0, size=safe_float(size) or 0.0, side="ask"))
        return levels

    def fetch_trades(self, symbol: str, limit: int) -> List[TradeEvent]:
        payload = self._request(
            "GET",
            "/api/v5/market/trades",
            params={"instId": symbol, "limit": max(10, min(limit, 100))},
        )
        items = payload.get("data", [])
        events: List[TradeEvent] = []
        for item in items:
            price = safe_float(item.get("px"))
            size = safe_float(item.get("sz"))
            events.append(
                TradeEvent(
                    exchange=self.exchange_name,
                    symbol=symbol,
                    timestamp_ms=safe_int(item.get("ts")) or 0,
                    side=normalize_trade_side(item.get("side")),
                    price=price,
                    size=size,
                    notional=compute_notional(price, size),
                    source="rest",
                    raw=item,
                )
            )
        return events


class HyperliquidClient(BaseClient):
    exchange_name = "Hyperliquid"
    base_url = "https://api.hyperliquid.xyz"

    def fetch(self, symbol: str) -> ExchangeSnapshot:
        try:
            payload = self._request("POST", "/info", json={"type": "metaAndAssetCtxs"})
            if not isinstance(payload, list) or len(payload) != 2:
                raise ValueError("unexpected metaAndAssetCtxs response")

            meta = payload[0]
            asset_contexts = payload[1]
            universe = meta.get("universe", [])

            asset_index = None
            for index, asset in enumerate(universe):
                if asset.get("name") == symbol:
                    asset_index = index
                    break

            if asset_index is None:
                raise ValueError(f"symbol {symbol} not found in Hyperliquid universe")

            ctx = asset_contexts[asset_index]
            mark_price = safe_float(ctx.get("markPx"))
            open_interest = safe_float(ctx.get("openInterest"))
            open_interest_notional = None
            if open_interest is not None and mark_price is not None:
                open_interest_notional = open_interest * mark_price

            last_price = safe_float(ctx.get("midPx")) or mark_price

            return ExchangeSnapshot(
                exchange=self.exchange_name,
                symbol=symbol,
                last_price=last_price,
                mark_price=mark_price,
                index_price=safe_float(ctx.get("oraclePx")),
                open_interest=open_interest,
                open_interest_notional=open_interest_notional,
                funding_rate=safe_float(ctx.get("funding")),
                volume_24h_base=safe_float(ctx.get("dayBaseVlm")),
                volume_24h_notional=safe_float(ctx.get("dayNtlVlm")),
                timestamp_ms=int(time.time() * 1000),
                raw={"meta": meta, "asset_context": ctx},
            )
        except Exception as exc:
            return self._error(symbol, exc)

    def fetch_candles(self, symbol: str, interval: str, limit: int) -> List[Candle]:
        interval_ms = interval_to_millis(interval)
        end_time = int(time.time() * 1000)
        start_time = max(0, end_time - interval_ms * (limit + 10))
        payload = self._request(
            "POST",
            "/info",
            json={
                "type": "candleSnapshot",
                "req": {
                    "coin": symbol,
                    "interval": HYPERLIQUID_CANDLE_INTERVALS.get(interval, "5m"),
                    "startTime": start_time,
                    "endTime": end_time,
                },
            },
        )
        candles: List[Candle] = []
        for row in payload[-limit:]:
            candles.append(
                Candle(
                    timestamp_ms=safe_int(row.get("t")) or 0,
                    open=safe_float(row.get("o")) or 0.0,
                    high=safe_float(row.get("h")) or 0.0,
                    low=safe_float(row.get("l")) or 0.0,
                    close=safe_float(row.get("c")) or 0.0,
                    volume=safe_float(row.get("v")) or 0.0,
                )
            )
        return candles

    def fetch_orderbook(self, symbol: str, limit: int) -> List[OrderBookLevel]:
        payload = self._request("POST", "/info", json={"type": "l2Book", "coin": symbol})
        levels: List[OrderBookLevel] = []
        book_levels = payload.get("levels", [[], []])
        for row in book_levels[0][:limit]:
            levels.append(OrderBookLevel(price=safe_float(row.get("px")) or 0.0, size=safe_float(row.get("sz")) or 0.0, side="bid"))
        for row in book_levels[1][:limit]:
            levels.append(OrderBookLevel(price=safe_float(row.get("px")) or 0.0, size=safe_float(row.get("sz")) or 0.0, side="ask"))
        return levels

    def fetch_clearinghouse_state(self, address: str) -> Dict[str, Any]:
        normalized_address = normalize_onchain_address(address)
        if not is_valid_onchain_address(normalized_address):
            raise ValueError("invalid Hyperliquid address")
        payload = self._request(
            "POST",
            "/info",
            json={"type": "clearinghouseState", "user": normalized_address},
        )
        return payload if isinstance(payload, dict) else {}

    def fetch_user_funding(self, address: str, start_time_ms: int, end_time_ms: Optional[int] = None) -> List[dict]:
        normalized_address = normalize_onchain_address(address)
        if not is_valid_onchain_address(normalized_address):
            raise ValueError("invalid Hyperliquid address")
        payload = self._request(
            "POST",
            "/info",
            json={
                "type": "userFunding",
                "user": normalized_address,
                "startTime": int(start_time_ms),
                "endTime": int(end_time_ms or int(time.time() * 1000)),
            },
        )
        return payload if isinstance(payload, list) else []

    def fetch_user_fills(self, address: str) -> List[dict]:
        normalized_address = normalize_onchain_address(address)
        if not is_valid_onchain_address(normalized_address):
            raise ValueError("invalid Hyperliquid address")
        payload = self._request(
            "POST",
            "/info",
            json={"type": "userFills", "user": normalized_address},
        )
        return payload if isinstance(payload, list) else []

    def fetch_active_asset_data(self, address: str, symbol: str) -> Dict[str, Any]:
        normalized_address = normalize_onchain_address(address)
        if not is_valid_onchain_address(normalized_address):
            raise ValueError("invalid Hyperliquid address")
        payload = self._request(
            "POST",
            "/info",
            json={"type": "activeAssetData", "user": normalized_address, "coin": symbol.upper().strip()},
        )
        return payload if isinstance(payload, dict) else {}

    def fetch_user_role(self, address: str) -> Dict[str, Any]:
        normalized_address = normalize_onchain_address(address)
        if not is_valid_onchain_address(normalized_address):
            raise ValueError("invalid Hyperliquid address")
        payload = self._request(
            "POST",
            "/info",
            json={"type": "userRole", "user": normalized_address},
        )
        return payload if isinstance(payload, dict) else {}

    def fetch_portfolio(self, address: str) -> List[dict]:
        normalized_address = normalize_onchain_address(address)
        if not is_valid_onchain_address(normalized_address):
            raise ValueError("invalid Hyperliquid address")
        payload = self._request(
            "POST",
            "/info",
            json={"type": "portfolio", "user": normalized_address},
        )
        return payload if isinstance(payload, list) else []

    def fetch_user_vault_equities(self, address: str) -> List[dict]:
        normalized_address = normalize_onchain_address(address)
        if not is_valid_onchain_address(normalized_address):
            raise ValueError("invalid Hyperliquid address")
        payload = self._request(
            "POST",
            "/info",
            json={"type": "userVaultEquities", "user": normalized_address},
        )
        return payload if isinstance(payload, list) else []

    def fetch_vault_details(self, vault_address: str, user: Optional[str] = None) -> Dict[str, Any]:
        normalized_vault_address = normalize_onchain_address(vault_address)
        if not is_valid_onchain_address(normalized_vault_address):
            raise ValueError("invalid Hyperliquid vault address")
        body: Dict[str, Any] = {"type": "vaultDetails", "vaultAddress": normalized_vault_address}
        if user:
            normalized_user = normalize_onchain_address(user)
            if is_valid_onchain_address(normalized_user):
                body["user"] = normalized_user
        payload = self._request("POST", "/info", json=body)
        return payload if isinstance(payload, dict) else {}

    def fetch_spot_meta_and_asset_contexts(self) -> Dict[str, Any]:
        payload = self._request("POST", "/info", json={"type": "spotMetaAndAssetCtxs"})
        if isinstance(payload, list) and len(payload) == 2:
            meta = payload[0] if isinstance(payload[0], dict) else {}
            contexts = payload[1] if isinstance(payload[1], list) else []
            return {"meta": meta, "contexts": contexts}
        return {"meta": {}, "contexts": []}

    def fetch_spot_clearinghouse_state(self, address: str) -> Dict[str, Any]:
        normalized_address = normalize_onchain_address(address)
        if not is_valid_onchain_address(normalized_address):
            raise ValueError("invalid Hyperliquid address")
        payload = self._request(
            "POST",
            "/info",
            json={"type": "spotClearinghouseState", "user": normalized_address},
        )
        return payload if isinstance(payload, dict) else {}

    def fetch_perps_at_open_interest_cap(self, dex: Optional[str] = None) -> Dict[str, Any]:
        body: Dict[str, Any] = {"type": "perpsAtOpenInterestCap"}
        if dex:
            body["dex"] = str(dex)
        payload = self._request("POST", "/info", json=body)
        return payload if isinstance(payload, dict) else {}


def _parse_hyperliquid_fill(item: dict) -> Dict[str, Any]:
    fill = item.get("fill") if isinstance(item.get("fill"), dict) else item
    price = safe_float(fill.get("px")) or safe_float(fill.get("price"))
    size = safe_float(fill.get("sz")) or safe_float(fill.get("size"))
    return {
        "time": safe_int(fill.get("time")) or 0,
        "coin": str(fill.get("coin") or ""),
        "direction": str(fill.get("dir") or fill.get("side") or ""),
        "side": normalize_trade_side(fill.get("side")),
        "price": price,
        "size": size,
        "notional": compute_notional(price, size),
        "closed_pnl": safe_float(fill.get("closedPnl")),
        "fee": safe_float(fill.get("fee")),
        "fee_token": str(fill.get("feeToken") or ""),
        "start_position": safe_float(fill.get("startPosition")),
        "hash": str(fill.get("hash") or ""),
        "raw": fill,
    }


def _parse_hyperliquid_funding(item: dict) -> Dict[str, Any]:
    delta = item.get("delta") if isinstance(item.get("delta"), dict) else {}
    if not delta and isinstance(item.get("ledgerUpdate"), dict):
        delta = item.get("ledgerUpdate") or {}
    amount = (
        safe_float(item.get("usdc"))
        or safe_float(item.get("amount"))
        or safe_float(delta.get("usdc"))
        or safe_float(delta.get("amount"))
        or safe_float(delta.get("delta"))
    )
    return {
        "time": safe_int(item.get("time")) or 0,
        "coin": str(item.get("coin") or delta.get("coin") or ""),
        "amount": amount,
        "type": str(item.get("type") or item.get("kind") or delta.get("type") or "funding"),
        "direction": "received" if amount is not None and amount >= 0 else "paid",
        "raw": item,
    }


def _parse_hyperliquid_position(item: dict) -> Dict[str, Any]:
    position = item.get("position") if isinstance(item.get("position"), dict) else item
    size = safe_float(position.get("szi"))
    if size is None:
        signed_size = safe_float(position.get("sz")) or safe_float(position.get("size"))
        size = abs(signed_size) if signed_size is not None else None
    raw_signed_size = safe_float(position.get("szi")) or safe_float(position.get("signedSz")) or safe_float(position.get("sz"))
    side = "flat"
    if raw_signed_size is not None:
        if raw_signed_size > 0:
            side = "long"
        elif raw_signed_size < 0:
            side = "short"
    entry_price = safe_float(position.get("entryPx"))
    mark_price = safe_float(position.get("markPx"))
    reference_price = mark_price if mark_price is not None else entry_price
    notional = abs(raw_signed_size or 0.0) * reference_price if raw_signed_size is not None and reference_price is not None else None
    leverage_value = None
    leverage = position.get("leverage")
    if isinstance(leverage, dict):
        leverage_value = safe_float(leverage.get("value"))
    else:
        leverage_value = safe_float(leverage)
    return {
        "coin": str(position.get("coin") or item.get("coin") or ""),
        "side": side,
        "size": abs(raw_signed_size) if raw_signed_size is not None else size,
        "signed_size": raw_signed_size,
        "entry_price": entry_price,
        "mark_price": mark_price,
        "liquidation_price": safe_float(position.get("liquidationPx")),
        "leverage": leverage_value,
        "max_leverage": safe_float(position.get("maxLeverage")),
        "margin_used": safe_float(position.get("marginUsed")),
        "position_value": safe_float(position.get("positionValue")) or notional,
        "unrealized_pnl": safe_float(position.get("unrealizedPnl")),
        "return_on_equity": safe_float(position.get("returnOnEquity")),
        "cum_funding": safe_float(position.get("cumFunding")),
        "raw": position,
    }


def fetch_hyperliquid_address_mode(
    address: str,
    coin: str,
    lookback_hours: int,
    timeout: int = DEFAULT_TIMEOUT,
) -> Dict[str, Any]:
    normalized_address = normalize_onchain_address(address)
    if not is_valid_onchain_address(normalized_address):
        return {
            "status": "error",
            "error": "地址格式无效",
            "address": normalized_address,
            "positions": [],
            "fills": [],
            "funding": [],
            "active_asset": {},
            "spot_state": {},
        }

    client = HyperliquidClient(timeout=timeout)
    selected_coin = str(coin or "").strip().upper()
    now_ms = int(time.time() * 1000)
    start_time_ms = now_ms - max(1, int(lookback_hours)) * 3_600_000
    errors: List[str] = []

    try:
        try:
            state = client.fetch_clearinghouse_state(normalized_address)
        except Exception as exc:
            return {
                "status": "error",
                "error": str(exc),
                "address": normalized_address,
                "positions": [],
                "fills": [],
                "funding": [],
                "active_asset": {},
                "spot_state": {},
            }

        raw_positions = state.get("assetPositions") or []
        positions = [_parse_hyperliquid_position(item) for item in raw_positions if isinstance(item, dict)]
        if selected_coin:
            positions = [item for item in positions if str(item.get("coin") or "").upper() == selected_coin]
        positions = sorted(positions, key=lambda item: abs(float(item.get("position_value") or 0.0)), reverse=True)

        try:
            raw_funding = client.fetch_user_funding(normalized_address, start_time_ms, now_ms)
        except Exception as exc:
            raw_funding = []
            errors.append(f"funding: {exc}")
        funding_records = [_parse_hyperliquid_funding(item) for item in raw_funding if isinstance(item, dict)]
        if selected_coin:
            funding_records = [item for item in funding_records if not item.get("coin") or str(item.get("coin")).upper() == selected_coin]
        funding_records = [item for item in funding_records if int(item.get("time") or 0) >= start_time_ms]
        funding_records.sort(key=lambda item: int(item.get("time") or 0), reverse=True)

        try:
            raw_fills = client.fetch_user_fills(normalized_address)
        except Exception as exc:
            raw_fills = []
            errors.append(f"fills: {exc}")
        fills = [_parse_hyperliquid_fill(item) for item in raw_fills if isinstance(item, dict)]
        if selected_coin:
            fills = [item for item in fills if str(item.get("coin") or "").upper() == selected_coin]
        fills = [item for item in fills if int(item.get("time") or 0) >= start_time_ms]
        fills.sort(key=lambda item: int(item.get("time") or 0), reverse=True)

        active_asset: Dict[str, Any] = {}
        if selected_coin:
            try:
                active_asset = client.fetch_active_asset_data(normalized_address, selected_coin)
            except Exception as exc:
                errors.append(f"activeAssetData: {exc}")

        spot_state: Dict[str, Any] = {}
        try:
            spot_state = client.fetch_spot_clearinghouse_state(normalized_address)
        except Exception as exc:
            errors.append(f"spotClearinghouseState: {exc}")

        role_payload: Dict[str, Any] = {}
        try:
            role_payload = client.fetch_user_role(normalized_address)
        except Exception as exc:
            errors.append(f"userRole: {exc}")

        portfolio: List[dict] = []
        try:
            portfolio = client.fetch_portfolio(normalized_address)
        except Exception as exc:
            errors.append(f"portfolio: {exc}")

        vault_equities: List[dict] = []
        try:
            vault_equities = client.fetch_user_vault_equities(normalized_address)
        except Exception as exc:
            errors.append(f"userVaultEquities: {exc}")

        vault_details: Dict[str, Any] = {}
        role_text = str(role_payload.get("role") or role_payload.get("type") or role_payload.get("userRole") or "")
        if role_text.lower() == "vault":
            try:
                vault_details = client.fetch_vault_details(normalized_address)
            except Exception as exc:
                errors.append(f"vaultDetails: {exc}")

        margin_summary = state.get("marginSummary") or {}
        cross_margin_summary = state.get("crossMarginSummary") or {}
        return {
            "status": "ok",
            "error": " | ".join(errors) if errors else None,
            "address": normalized_address,
            "coin": selected_coin,
            "timestamp_ms": safe_int(state.get("time")) or now_ms,
            "account_value": safe_float(margin_summary.get("accountValue")),
            "total_notional_position": safe_float(margin_summary.get("totalNtlPos")),
            "total_raw_usd": safe_float(margin_summary.get("totalRawUsd")),
            "total_margin_used": safe_float(margin_summary.get("totalMarginUsed")),
            "withdrawable": safe_float(state.get("withdrawable")),
            "cross_account_value": safe_float(cross_margin_summary.get("accountValue")),
            "cross_margin_used": safe_float(cross_margin_summary.get("totalMarginUsed")),
            "positions": positions,
            "fills": fills[:80],
            "funding": funding_records[:80],
            "active_asset": active_asset,
            "spot_state": spot_state,
            "role": role_text or None,
            "role_payload": role_payload,
            "portfolio": portfolio,
            "vault_equities": vault_equities,
            "vault_details": vault_details,
            "raw_state": state,
        }
    finally:
        client.close()


def fetch_hyperliquid_predicted_fundings(timeout: int = DEFAULT_TIMEOUT) -> List[list]:
    client = HyperliquidClient(timeout=timeout)
    try:
        payload = client._request("POST", "/info", json={"type": "predictedFundings"})
        return payload if isinstance(payload, list) else []
    finally:
        client.close()


def fetch_hyperliquid_all_mids(timeout: int = DEFAULT_TIMEOUT) -> Dict[str, str]:
    client = HyperliquidClient(timeout=timeout)
    try:
        payload = client._request("POST", "/info", json={"type": "allMids"})
        return payload if isinstance(payload, dict) else {}
    finally:
        client.close()


def fetch_hyperliquid_spot_meta_and_asset_contexts(timeout: int = DEFAULT_TIMEOUT) -> Dict[str, Any]:
    client = HyperliquidClient(timeout=timeout)
    try:
        return client.fetch_spot_meta_and_asset_contexts()
    finally:
        client.close()


def fetch_hyperliquid_perps_at_open_interest_cap(timeout: int = DEFAULT_TIMEOUT, dex: Optional[str] = None) -> Dict[str, Any]:
    client = HyperliquidClient(timeout=timeout)
    try:
        return client.fetch_perps_at_open_interest_cap(dex=dex)
    finally:
        client.close()


def fetch_bybit_insurance_pool(coin: str, timeout: int = DEFAULT_TIMEOUT) -> Dict[str, Any]:
    client = BybitClient(timeout=timeout)
    base_coin = _normalize_coin_code(coin)
    try:
        try:
            payload = client._request("GET", "/v5/market/insurance", params={"coin": base_coin} if base_coin else None)
        except Exception:
            payload = {}
        result = payload.get("result", {}) if isinstance(payload, dict) else {}
        items = result.get("list", []) if isinstance(result, dict) else []
        normalized_rows: List[Dict[str, Any]] = []
        total_value = 0.0
        total_balance = 0.0
        for item in items if isinstance(items, list) else []:
            if not isinstance(item, dict):
                continue
            value = safe_float(item.get("value")) or 0.0
            balance = safe_float(item.get("balance")) or 0.0
            total_value += value
            total_balance += balance
            normalized_rows.append(
                {
                    "coin": str(item.get("coin") or base_coin),
                    "symbols": str(item.get("symbols") or ""),
                    "balance": balance,
                    "value": value,
                }
            )
        return {
            "coin": base_coin,
            "updated_time_ms": safe_int(result.get("updatedTime")) if isinstance(result, dict) else None,
            "total_value": total_value if normalized_rows else None,
            "total_balance": total_balance if normalized_rows else None,
            "rows": normalized_rows,
        }
    finally:
        client.close()


def build_clients(timeout: int = DEFAULT_TIMEOUT) -> Dict[str, BaseClient]:
    return {
        "bybit": BybitClient(timeout=timeout),
        "binance": BinanceClient(timeout=timeout),
        "okx": OkxClient(timeout=timeout),
        "hyperliquid": HyperliquidClient(timeout=timeout),
        "bitget": BitgetPublicClient(timeout=timeout),
    }


def _get_shared_perp_clients(timeout: int = DEFAULT_TIMEOUT) -> Dict[str, BaseClient]:
    store = _thread_local_client_store("perp_clients")
    client_factories = {
        "bybit": BybitClient,
        "binance": BinanceClient,
        "okx": OkxClient,
        "hyperliquid": HyperliquidClient,
        "bitget": BitgetPublicClient,
    }
    clients: Dict[str, BaseClient] = {}
    for key, factory in client_factories.items():
        cache_key = (int(timeout), key)
        client = store.get(cache_key)
        if client is None:
            client = factory(timeout=timeout)
            store[cache_key] = client
        clients[key] = client
    return clients


def close_clients(clients: Dict[str, BaseClient]) -> None:
    for client in (clients or {}).values():
        if hasattr(client, "close"):
            try:
                client.close()
            except Exception:
                continue


def fetch_all_snapshots(symbol_map: Dict[str, str], timeout: int = DEFAULT_TIMEOUT) -> List[ExchangeSnapshot]:
    normalized_symbol_map = {key: str(symbol_map.get(key) or "").strip().upper() for key in EXCHANGE_ORDER}
    cache_key = ("all_snapshots", int(timeout), tuple((key, normalized_symbol_map.get(key, "")) for key in EXCHANGE_ORDER))

    def _builder() -> List[ExchangeSnapshot]:
        clients = _get_shared_perp_clients(timeout=timeout)
        snapshots: List[ExchangeSnapshot] = []
        for key in EXCHANGE_ORDER:
            symbol = normalized_symbol_map.get(key, "")
            if not symbol:
                snapshots.append(
                    ExchangeSnapshot(
                        exchange=clients[key].exchange_name,
                        symbol="",
                        status="error",
                        error="未上架此币",
                    )
                )
                continue
            snapshots.append(clients[key].fetch(symbol))
        return snapshots

    return _cached_fetch("all_snapshots", cache_key, _builder)


def fetch_exchange_snapshot(exchange_key: str, symbol: str, timeout: int = DEFAULT_TIMEOUT) -> ExchangeSnapshot:
    normalized_symbol = str(symbol or "").strip().upper()
    clients = _get_shared_perp_clients(timeout=timeout)
    if not normalized_symbol:
        return ExchangeSnapshot(exchange=clients[exchange_key].exchange_name, symbol="", status="error", error="未上架此币")
    return _cached_fetch(
        "exchange_snapshot",
        ("exchange_snapshot", exchange_key, normalized_symbol, int(timeout)),
        lambda: clients[exchange_key].fetch(normalized_symbol),
    )


def fetch_exchange_candles(exchange_key: str, symbol: str, interval: str, limit: int, timeout: int = DEFAULT_TIMEOUT) -> List[Candle]:
    normalized_symbol = str(symbol or "").strip().upper()
    if not normalized_symbol:
        return []
    clients = _get_shared_perp_clients(timeout=timeout)
    return _cached_fetch(
        "exchange_candles",
        ("exchange_candles", exchange_key, normalized_symbol, str(interval), int(limit), int(timeout)),
        lambda: clients[exchange_key].fetch_candles(normalized_symbol, interval, limit),
    )


def fetch_exchange_orderbook(exchange_key: str, symbol: str, limit: int, timeout: int = DEFAULT_TIMEOUT) -> List[OrderBookLevel]:
    normalized_symbol = str(symbol or "").strip().upper()
    if not normalized_symbol:
        return []
    clients = _get_shared_perp_clients(timeout=timeout)
    return _cached_fetch(
        "exchange_orderbook",
        ("exchange_orderbook", exchange_key, normalized_symbol, int(limit), int(timeout)),
        lambda: clients[exchange_key].fetch_orderbook(normalized_symbol, limit),
    )


def fetch_exchange_oi_history(exchange_key: str, symbol: str, interval: str, limit: int, timeout: int = DEFAULT_TIMEOUT) -> List[OIPoint]:
    normalized_symbol = str(symbol or "").strip().upper()
    if not normalized_symbol:
        return []
    clients = _get_shared_perp_clients(timeout=timeout)
    return _cached_fetch(
        "exchange_oi_history",
        ("exchange_oi_history", exchange_key, normalized_symbol, str(interval), int(limit), int(timeout)),
        lambda: clients[exchange_key].fetch_open_interest_history(normalized_symbol, interval, limit),
    )


def fetch_exchange_liquidations(exchange_key: str, symbol: str, limit: int, timeout: int = DEFAULT_TIMEOUT) -> List[LiquidationEvent]:
    normalized_symbol = str(symbol or "").strip().upper()
    if not normalized_symbol:
        return []
    clients = _get_shared_perp_clients(timeout=timeout)
    return _cached_fetch(
        "exchange_liquidations",
        ("exchange_liquidations", exchange_key, normalized_symbol, int(limit), int(timeout)),
        lambda: clients[exchange_key].fetch_liquidations(normalized_symbol, limit),
    )


def fetch_exchange_trades(exchange_key: str, symbol: str, limit: int, timeout: int = DEFAULT_TIMEOUT) -> List[TradeEvent]:
    normalized_symbol = str(symbol or "").strip().upper()
    if not normalized_symbol:
        return []
    clients = _get_shared_perp_clients(timeout=timeout)
    return _cached_fetch(
        "exchange_trades",
        ("exchange_trades", exchange_key, normalized_symbol, int(limit), int(timeout)),
        lambda: clients[exchange_key].fetch_trades(normalized_symbol, limit),
    )


def fetch_bitget_all_futures_tickers(timeout: int = DEFAULT_TIMEOUT) -> List[Dict[str, Any]]:
    client = BitgetPublicClient(timeout=timeout)
    try:
        return client._fetch_all_tickers()
    finally:
        client.close()


def fetch_binance_trader_sentiment(symbol: str, period: str, limit: int, timeout: int = DEFAULT_TIMEOUT) -> Dict[str, List[dict]]:
    client = BinanceClient(timeout=timeout)
    try:
        params = {"symbol": symbol, "period": period, "limit": max(10, min(limit, 500))}
        datasets: Dict[str, List[dict]] = {}
        endpoint_map = {
            "taker_ratio": "/futures/data/takerlongshortRatio",
            "top_position": "/futures/data/topLongShortPositionRatio",
            "top_account": "/futures/data/topLongShortAccountRatio",
            "global_account": "/futures/data/globalLongShortAccountRatio",
        }
        for dataset_key, path in endpoint_map.items():
            try:
                payload = client._request("GET", path, params=params)
                datasets[dataset_key] = payload if isinstance(payload, list) else []
            except Exception:
                datasets[dataset_key] = []
        return datasets
    finally:
        client.close()


def fetch_bybit_trader_sentiment(symbol: str, period: str, limit: int, timeout: int = DEFAULT_TIMEOUT) -> Dict[str, List[dict]]:
    client = BybitClient(timeout=timeout)
    try:
        try:
            payload = client._request(
                "GET",
                "/v5/market/account-ratio",
                params={
                    "category": "linear",
                    "symbol": symbol,
                    "period": BYBIT_RATIO_INTERVALS.get(period, "1h"),
                    "limit": max(10, min(limit, 500)),
                },
            )
            items = payload.get("result", {}).get("list", [])
        except Exception:
            items = []

        normalized: List[dict] = []
        for item in reversed(items):
            long_share = safe_float(item.get("buyRatio"))
            short_share = safe_float(item.get("sellRatio"))
            ratio = None
            if long_share is not None and short_share not in (None, 0):
                ratio = long_share / short_share
            normalized.append(
                {
                    "symbol": symbol,
                    "timestamp": safe_int(item.get("timestamp")),
                    "longShortRatio": ratio,
                    "longAccount": long_share,
                    "shortAccount": short_share,
                    "buyRatio": long_share,
                    "sellRatio": short_share,
                }
            )
        return {"account_ratio": normalized}
    finally:
        client.close()


def fetch_bitget_trader_sentiment(symbol: str, period: str, limit: int, timeout: int = DEFAULT_TIMEOUT) -> Dict[str, List[dict]]:
    client = BitgetPublicClient(timeout=timeout)
    try:
        normalized_symbol = str(symbol or "").upper().replace("-", "").replace("_", "").strip()
        normalized_period = str(period or "").strip().lower()
        request_period = "1Dutc" if normalized_period == "1w" else BITGET_RATIO_INTERVALS.get(period, "15m")
        non_global_request_period = "1d" if request_period == "1Dutc" else request_period
        request_limit = 14 if str(period or "").strip().lower() == "1w" else max(10, min(limit, 500))
        datasets: Dict[str, List[dict]] = {"global_account": [], "account_ratio": [], "top_position": []}
        endpoint_specs = {
            "global_account": ("/api/v2/mix/market/long-short", "longRatio", "shortRatio", "longShortRatio", request_period),
            "account_ratio": (
                "/api/v2/mix/market/account-long-short",
                "longAccountRatio",
                "shortAccountRatio",
                "longShortAccountRatio",
                non_global_request_period,
            ),
            "top_position": (
                "/api/v2/mix/market/position-long-short",
                "longPositionRatio",
                "shortPositionRatio",
                "longShortPositionRatio",
                non_global_request_period,
            ),
        }
        for dataset_key, (path, long_key, short_key, ratio_key, dataset_period) in endpoint_specs.items():
            try:
                payload = client._request(
                    "GET",
                    path,
                    params={"symbol": normalized_symbol, "period": dataset_period, "limit": request_limit},
                )
                items = payload.get("data") if isinstance(payload, dict) else []
            except Exception:
                items = []
            normalized_items: List[dict] = []
            for item in reversed(items or []):
                long_share = safe_float(item.get(long_key))
                short_share = safe_float(item.get(short_key))
                ratio = safe_float(item.get(ratio_key))
                if ratio is None and long_share is not None and short_share not in (None, 0):
                    ratio = long_share / short_share
                normalized_items.append(
                    {
                        "symbol": normalized_symbol,
                        "timestamp": safe_int(item.get("ts")),
                        "longShortRatio": ratio,
                        "longAccount": long_share,
                        "shortAccount": short_share,
                    }
                )
            datasets[dataset_key] = normalized_items
        return datasets
    finally:
        client.close()


def fetch_gate_trader_sentiment(symbol: str, period: str, limit: int, timeout: int = DEFAULT_TIMEOUT) -> Dict[str, List[dict]]:
    client = GatePublicClient(timeout=timeout)
    try:
        normalized_symbol = str(symbol or "").upper().replace("-", "_").replace("USDT", "_USDT").replace("__", "_").strip("_")
        if not normalized_symbol.endswith("_USDT"):
            normalized_symbol = f"{normalized_symbol.replace('_', '')}_USDT"
        request_interval = GATE_RATIO_INTERVALS.get(period, "15m")
        request_limit = 14 if str(period or "").strip().lower() == "1w" else max(10, min(limit, 200))
        try:
            payload = client._request(
                "GET",
                "/futures/usdt/contract_stats",
                params={"contract": normalized_symbol, "interval": request_interval, "limit": request_limit},
            )
            items = payload if isinstance(payload, list) else []
        except Exception:
            items = []

        def _derive_share(ratio_value: Optional[float]) -> tuple[Optional[float], Optional[float]]:
            if ratio_value in (None, 0):
                return None, None
            long_share = float(ratio_value) / (1.0 + float(ratio_value))
            short_share = 1.0 - long_share
            return long_share, short_share

        account_ratio: List[dict] = []
        taker_ratio: List[dict] = []
        top_account: List[dict] = []
        top_position: List[dict] = []
        for item in items or []:
            timestamp = safe_int(item.get("time"))
            if timestamp is not None and timestamp < 1_000_000_000_000:
                timestamp *= 1000
            account_ratio_value = safe_float(item.get("lsr_account"))
            taker_ratio_value = safe_float(item.get("lsr_taker"))
            top_account_value = safe_float(item.get("top_lsr_account"))
            top_position_value = safe_float(item.get("top_lsr_size"))
            long_share, short_share = _derive_share(account_ratio_value)
            account_ratio.append(
                {
                    "symbol": normalized_symbol,
                    "timestamp": timestamp,
                    "longShortRatio": account_ratio_value,
                    "longAccount": long_share,
                    "shortAccount": short_share,
                }
            )
            taker_ratio.append(
                {
                    "symbol": normalized_symbol,
                    "timestamp": timestamp,
                    "longShortRatio": taker_ratio_value,
                }
            )
            top_account.append(
                {
                    "symbol": normalized_symbol,
                    "timestamp": timestamp,
                    "longShortRatio": top_account_value,
                }
            )
            top_position.append(
                {
                    "symbol": normalized_symbol,
                    "timestamp": timestamp,
                    "longShortRatio": top_position_value,
                }
            )
        return {
            "account_ratio": account_ratio,
            "taker_ratio": taker_ratio,
            "top_account": top_account,
            "top_position": top_position,
        }
    finally:
        client.close()


def fetch_htx_trader_sentiment(symbol: str, period: str, limit: int, timeout: int = DEFAULT_TIMEOUT) -> Dict[str, List[dict]]:
    client = HtxPublicClient(timeout=timeout)
    try:
        normalized_symbol = str(symbol or "").upper().replace("-", "").replace("_", "").replace("USDT", "").strip()
        if not normalized_symbol:
            return {"account_ratio": [], "top_position": []}
        request_period = HTX_RATIO_INTERVALS.get(str(period or "").strip().lower(), "15min")
        request_limit = 14 if str(period or "").strip().lower() == "1w" else max(10, min(limit, 60))

        def _request_ratio(path: str) -> List[dict]:
            try:
                payload = client._request(
                    "GET",
                    path,
                    params={"symbol": normalized_symbol, "period": request_period},
                )
                items = (((payload or {}).get("data") or {}).get("list") or [])
            except Exception:
                items = []
            normalized_items: List[dict] = []
            for item in list(items or [])[-request_limit:]:
                buy_ratio = safe_float(item.get("buy_ratio"))
                sell_ratio = safe_float(item.get("sell_ratio"))
                long_share = buy_ratio
                short_share = sell_ratio
                ratio = None
                if long_share is not None and short_share not in (None, 0):
                    ratio = long_share / short_share
                normalized_items.append(
                    {
                        "symbol": normalized_symbol,
                        "timestamp": safe_int(item.get("ts")),
                        "longShortRatio": ratio,
                        "longAccount": long_share,
                        "shortAccount": short_share,
                        "buyRatio": buy_ratio,
                        "sellRatio": sell_ratio,
                        "lockedRatio": safe_float(item.get("locked_ratio")),
                    }
                )
            return normalized_items

        return {
            "account_ratio": _request_ratio("/api/v1/contract_elite_account_ratio"),
            "top_position": _request_ratio("/api/v1/contract_elite_position_ratio"),
        }
    finally:
        client.close()


def fetch_okx_trader_sentiment(symbol: str, period: str, limit: int, timeout: int = DEFAULT_TIMEOUT) -> Dict[str, List[dict]]:
    client = OkxClient(timeout=timeout)
    try:
        normalized_symbol = str(symbol or "").upper().strip()
        if not normalized_symbol:
            return {
                "global_account": [],
                "contract_account": [],
                "top_account": [],
                "top_position": [],
                "oi_volume": [],
            }
        ccy = str(normalized_symbol.split("-", 1)[0] or "").upper().strip()
        normalized_period = str(period or "").strip().lower()
        account_period = "1D" if normalized_period == "1w" else OKX_RATIO_INTERVALS.get(normalized_period, "15m")
        global_period = "1D" if normalized_period == "1w" else OKX_GLOBAL_RATIO_INTERVALS.get(normalized_period, "5m")
        request_limit = 14 if normalized_period == "1w" else max(10, min(limit, 120))

        def _normalize_ratio_items(items: Any) -> List[dict]:
            normalized_items: List[dict] = []
            raw_items = list(items) if isinstance(items, list) else []
            for row in reversed(raw_items):
                if not isinstance(row, (list, tuple)) or len(row) < 2:
                    continue
                timestamp = safe_int(row[0])
                ratio = safe_float(row[1])
                long_share = None
                short_share = None
                if ratio not in (None, 0):
                    long_share = float(ratio) / (1.0 + float(ratio))
                    short_share = 1.0 - long_share
                normalized_items.append(
                    {
                        "symbol": normalized_symbol,
                        "timestamp": timestamp,
                        "longShortRatio": ratio,
                        "longAccount": long_share,
                        "shortAccount": short_share,
                    }
                )
            return normalized_items[-request_limit:]

        def _normalize_oi_volume_items(items: Any) -> List[dict]:
            normalized_items: List[dict] = []
            raw_items = list(items) if isinstance(items, list) else []
            for row in reversed(raw_items):
                if not isinstance(row, (list, tuple)) or len(row) < 3:
                    continue
                normalized_items.append(
                    {
                        "symbol": normalized_symbol,
                        "timestamp": safe_int(row[0]),
                        "openInterest": safe_float(row[1]),
                        "volume": safe_float(row[2]),
                    }
                )
            return normalized_items[-request_limit:]

        datasets: Dict[str, List[dict]] = {
            "global_account": [],
            "contract_account": [],
            "top_account": [],
            "top_position": [],
            "oi_volume": [],
        }
        endpoint_specs: Dict[str, Tuple[str, Dict[str, Any], str]] = {
            "global_account": (
                "/api/v5/rubik/stat/contracts/long-short-account-ratio",
                {"ccy": ccy, "period": global_period},
                "ratio",
            ),
            "contract_account": (
                "/api/v5/rubik/stat/contracts/long-short-account-ratio-contract",
                {"instId": normalized_symbol, "period": account_period},
                "ratio",
            ),
            "top_account": (
                "/api/v5/rubik/stat/contracts/long-short-account-ratio-contract-top-trader",
                {"instId": normalized_symbol, "period": account_period},
                "ratio",
            ),
            "top_position": (
                "/api/v5/rubik/stat/contracts/long-short-position-ratio-contract-top-trader",
                {"instId": normalized_symbol, "period": account_period},
                "ratio",
            ),
            "oi_volume": (
                "/api/v5/rubik/stat/contracts/open-interest-volume",
                {"ccy": ccy, "period": global_period},
                "oi_volume",
            ),
        }
        for dataset_key, (path, params, kind) in endpoint_specs.items():
            try:
                payload = client._request("GET", path, params=params)
                items = payload.get("data") if isinstance(payload, dict) else []
            except Exception:
                items = []
            datasets[dataset_key] = _normalize_oi_volume_items(items) if kind == "oi_volume" else _normalize_ratio_items(items)
        return datasets
    finally:
        client.close()


def fetch_binance_futures_orderbook_snapshot(symbol: str, limit: int, timeout: int = DEFAULT_TIMEOUT) -> dict:
    client = BinanceClient(timeout=timeout)
    try:
        return client._request(
            "GET",
            "/fapi/v1/depth",
            params={"symbol": symbol, "limit": normalize_depth_limit("binance", max(5, min(limit, 1000)))},
        )
    finally:
        client.close()


def fetch_binance_spot_orderbook_snapshot(symbol: str, limit: int, timeout: int = DEFAULT_TIMEOUT) -> dict:
    client = BinanceSpotClient(timeout=timeout)
    try:
        return client._public_request(
            "GET",
            "/api/v3/depth",
            params={"symbol": symbol, "limit": normalize_depth_limit("binance", max(5, min(limit, 1000)))},
        )
    finally:
        client.close()


def fetch_binance_spot_snapshot(symbol: str, timeout: int = DEFAULT_TIMEOUT) -> SpotSnapshot:
    client = BinanceSpotClient(timeout=timeout)
    try:
        return client.fetch(symbol)
    finally:
        client.close()


def fetch_binance_spot_orderbook(symbol: str, limit: int, timeout: int = DEFAULT_TIMEOUT) -> List[OrderBookLevel]:
    client = BinanceSpotClient(timeout=timeout)
    try:
        return client.fetch_orderbook(symbol, limit)
    finally:
        client.close()


def fetch_binance_spot_trades(symbol: str, limit: int, timeout: int = DEFAULT_TIMEOUT) -> List[TradeEvent]:
    client = BinanceSpotClient(timeout=timeout)
    try:
        return client.fetch_trades(symbol, limit)
    finally:
        client.close()


def fetch_binance_basis_curve(pair: str, period: str, limit: int, timeout: int = DEFAULT_TIMEOUT) -> Dict[str, List[dict]]:
    client = BinanceClient(timeout=timeout)
    try:
        datasets: Dict[str, List[dict]] = {}
        for contract_type in ("PERPETUAL", "CURRENT_QUARTER", "NEXT_QUARTER"):
            try:
                payload = client._request(
                    "GET",
                    "/futures/data/basis",
                    params={
                        "pair": pair,
                        "contractType": contract_type,
                        "period": period,
                        "limit": max(10, min(limit, 200)),
                    },
                )
                datasets[contract_type] = payload if isinstance(payload, list) else []
            except Exception:
                datasets[contract_type] = []
        return datasets
    finally:
        client.close()


def build_spot_clients(timeout: int = DEFAULT_TIMEOUT) -> Dict[str, BaseClient]:
    return {
        "binance": BinanceSpotClient(timeout=timeout),
        "bybit": BybitSpotClient(timeout=timeout),
        "okx": OkxSpotClient(timeout=timeout),
    }


def _get_shared_spot_clients(timeout: int = DEFAULT_TIMEOUT) -> Dict[str, BaseClient]:
    store = _thread_local_client_store("spot_clients")
    client_factories = {
        "binance": BinanceSpotClient,
        "bybit": BybitSpotClient,
        "okx": OkxSpotClient,
    }
    clients: Dict[str, BaseClient] = {}
    for key, factory in client_factories.items():
        cache_key = (int(timeout), key)
        client = store.get(cache_key)
        if client is None:
            client = factory(timeout=timeout)
            store[cache_key] = client
        clients[key] = client
    return clients


def fetch_spot_snapshot(exchange_key: str, symbol: str, timeout: int = DEFAULT_TIMEOUT) -> SpotSnapshot:
    normalized_symbol = str(symbol or "").strip().upper()
    clients = _get_shared_spot_clients(timeout=timeout)
    if not normalized_symbol:
        return SpotSnapshot(exchange=clients[exchange_key].exchange_name, symbol="", status="error", error="未上架此币")
    return _cached_fetch(
        "spot_snapshot",
        ("spot_snapshot", exchange_key, normalized_symbol, int(timeout)),
        lambda: clients[exchange_key].fetch(normalized_symbol),
    )


def fetch_spot_candles(exchange_key: str, symbol: str, interval: str, limit: int, timeout: int = DEFAULT_TIMEOUT) -> List[Candle]:
    normalized_symbol = str(symbol or "").strip().upper()
    if not normalized_symbol:
        return []
    clients = _get_shared_spot_clients(timeout=timeout)
    client = clients[exchange_key]
    return _cached_fetch(
        "spot_candles",
        ("spot_candles", exchange_key, normalized_symbol, str(interval), int(limit), int(timeout)),
        lambda: client.fetch_candles(normalized_symbol, interval, limit),  # type: ignore[attr-defined]
    )


def fetch_spot_orderbook(exchange_key: str, symbol: str, limit: int, timeout: int = DEFAULT_TIMEOUT) -> List[OrderBookLevel]:
    normalized_symbol = str(symbol or "").strip().upper()
    if not normalized_symbol:
        return []
    clients = _get_shared_spot_clients(timeout=timeout)
    return _cached_fetch(
        "spot_orderbook",
        ("spot_orderbook", exchange_key, normalized_symbol, int(limit), int(timeout)),
        lambda: clients[exchange_key].fetch_orderbook(normalized_symbol, limit),
    )


def fetch_spot_trades(exchange_key: str, symbol: str, limit: int, timeout: int = DEFAULT_TIMEOUT) -> List[TradeEvent]:
    normalized_symbol = str(symbol or "").strip().upper()
    if not normalized_symbol:
        return []
    clients = _get_shared_spot_clients(timeout=timeout)
    client = clients[exchange_key]
    return _cached_fetch(
        "spot_trades",
        ("spot_trades", exchange_key, normalized_symbol, int(limit), int(timeout)),
        lambda: client.fetch_trades(normalized_symbol, limit),  # type: ignore[attr-defined]
    )


def snapshots_to_rows(snapshots: List[ExchangeSnapshot]):
    return [snapshot.to_row() for snapshot in snapshots]

