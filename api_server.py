from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager, suppress
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import threading
import time
from typing import Any, Callable, Dict, Optional

import requests
from fastapi import Body, FastAPI, HTTPException, Query, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse, RedirectResponse, Response, StreamingResponse
from fastapi.staticfiles import StaticFiles

from exchanges import EXCHANGE_ORDER, SPOT_EXCHANGE_ORDER
from request_schema import normalize_csv_choices, normalize_liquidation_request


BASE_DIR = Path(__file__).resolve().parent
WEB_DIR = BASE_DIR / "web"
LEGACY_STREAMLIT_PORT = int(os.getenv("LEGACY_STREAMLIT_PORT", "8501"))
LEGACY_STREAMLIT_URL = f"http://127.0.0.1:{LEGACY_STREAMLIT_PORT}/?embed=true"
LEGACY_AUTOSTART = str(os.getenv("LEGACY_STREAMLIT_AUTOSTART", "0")).strip().lower() in {"1", "true", "yes", "on"}
PRECOMPUTE_INTERVAL_SECONDS = int(os.getenv("LGFD_PRECOMPUTE_INTERVAL_SECONDS", "30"))
PRECOMPUTE_WORKERS = int(os.getenv("LGFD_PRECOMPUTE_WORKERS", "2"))
PRECOMPUTE_ENABLED = str(os.getenv("LGFD_ENABLE_PRECOMPUTE_WORKER", "1")).strip().lower() in {"1", "true", "yes", "on"}
PRECOMPUTE_BOOT_DELAY_SECONDS = float(os.getenv("LGFD_PRECOMPUTE_BOOT_DELAY_SECONDS", "45"))
HOT_RUNTIME_COINS = os.getenv("LGFD_HOT_RUNTIME_COINS", "BTC,ETH,SOL,XRP")
FANOUT_SOURCE_POLL_SECONDS = max(float(os.getenv("LGFD_FANOUT_SOURCE_POLL_SECONDS", "3.0") or 3.0), 0.5)
LIQ_MAP_EXCHANGE_KEYS = ["binance", "bybit", "okx", "hyperliquid", "bitget"]
TIME_WINDOW_OPTIONS: Dict[str, int] = {"5m": 5, "15m": 15, "30m": 30, "1h": 60, "2h": 120, "4h": 240, "24h": 1440, "1d": 1440, "1w": 10080}
LIQUIDATION_ARCHIVE_WINDOWS = (
    {"key": "30m", "label": "最近30分钟"},
    {"key": "4h", "label": "最近4小时"},
    {"key": "today", "label": "今天"},
    {"key": "all", "label": "全部本地缓存"},
)
_runtime_manager_lock = threading.Lock()
_runtime_manager_instance: Optional[Any] = None
_runtime_manager_listener_registered = False
_legacy_process_lock = threading.Lock()
_legacy_process: Optional[subprocess.Popen] = None
_market_runtime_module: Optional[Any] = None
_market_runtime_module_lock = threading.Lock()


def _get_market_runtime_module() -> Any:
    global _market_runtime_module
    module = _market_runtime_module
    if module is not None:
        return module
    with _market_runtime_module_lock:
        module = _market_runtime_module
        if module is None:
            import market_runtime as runtime_module

            module = runtime_module
            _market_runtime_module = module
    return module


def _load_custom_alert_rules_from_runtime(payload: Dict[str, Any]) -> list[Dict[str, Any]]:
    runtime_module = _get_market_runtime_module()
    return list(runtime_module._load_custom_alert_rules(payload))


def _load_ui_preferences_snapshot_from_runtime() -> Dict[str, Any]:
    runtime_module = _get_market_runtime_module()
    return dict(runtime_module._load_ui_preferences_snapshot())


def _save_ui_preferences_snapshot_from_runtime(payload: Dict[str, Any]) -> Dict[str, Any]:
    runtime_module = _get_market_runtime_module()
    return dict(runtime_module._save_ui_preferences_snapshot(payload))


def _build_runtime_manager() -> Any:
    runtime_module = _get_market_runtime_module()
    return runtime_module.MarketRuntimeManager(
        hot_coins=HOT_RUNTIME_COINS,
        precompute_interval_seconds=PRECOMPUTE_INTERVAL_SECONDS,
        precompute_workers=PRECOMPUTE_WORKERS,
        enable_precompute_worker=PRECOMPUTE_ENABLED,
        autostart_hot_sessions=False,
        autostart_precompute_worker=False,
    )


def get_runtime_manager() -> Any:
    global _runtime_manager_instance
    instance = _runtime_manager_instance
    if instance is not None:
        return instance
    with _runtime_manager_lock:
        instance = _runtime_manager_instance
        if instance is None:
            instance = _build_runtime_manager()
            _runtime_manager_instance = instance
    return instance


class _LazyRuntimeManagerProxy:
    def __getattr__(self, item: str) -> Any:
        manager = get_runtime_manager()
        _ensure_runtime_manager_listener_registered(manager)
        return getattr(manager, item)

    def shutdown(self) -> None:
        global _runtime_manager_instance, _runtime_manager_listener_registered
        with _runtime_manager_lock:
            instance = _runtime_manager_instance
            _runtime_manager_instance = None
            _runtime_manager_listener_registered = False
        if instance is not None:
            instance.shutdown()


runtime_manager = _LazyRuntimeManagerProxy()


def _payload_revision(payload: Dict[str, Any]) -> str:
    return json.dumps(
        {
            "generated_at_ms": payload.get("generated_at_ms"),
            "updated_at_ms": (payload.get("data_meta") or {}).get("updated_at_ms"),
            "summary": payload.get("summary"),
            "counts": {
                "cards": len(payload.get("cards", []) or []),
                "panels": len(payload.get("panels", []) or []),
                "events": len(payload.get("events", []) or []),
                "alerts": len(payload.get("alerts", []) or []),
                "rows": len(payload.get("rows", []) or []),
                "signals": len(payload.get("signals", []) or []),
                "whales": len(payload.get("whale_events", []) or []),
            },
            "error": payload.get("error"),
        },
        sort_keys=True,
        default=_json_default,
    )


class PayloadFanoutHub:
    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._topics: Dict[str, Dict[str, Any]] = {}
        self._stopped = False
        self._notify_lock = threading.Lock()
        self._topic_source_keys: Dict[str, set[str]] = {}
        self._topic_wakeups: Dict[str, threading.Event] = {}

    @staticmethod
    def _normalize_client_interval(interval_seconds: float) -> float:
        return max(float(interval_seconds or 0.0), 0.25)

    @staticmethod
    def _new_subscriber_state(interval_seconds: float) -> Dict[str, Any]:
        return {
            "interval_seconds": PayloadFanoutHub._normalize_client_interval(interval_seconds),
            "last_sent_at": 0.0,
            "last_delivered_revision": None,
            "pending_payload": None,
            "pending_revision": None,
        }

    @staticmethod
    def _flush_subscriber_state(queue: asyncio.Queue, state: Dict[str, Any], *, now: float) -> bool:
        pending_revision = state.get("pending_revision")
        pending_payload = state.get("pending_payload")
        if pending_revision in (None, "") or pending_payload is None:
            return False
        interval_seconds = PayloadFanoutHub._normalize_client_interval(state.get("interval_seconds") or 0.0)
        last_sent_at = float(state.get("last_sent_at") or 0.0)
        if last_sent_at and now - last_sent_at < interval_seconds:
            return False
        PayloadFanoutHub._offer(queue, pending_payload)
        state["last_sent_at"] = now
        state["last_delivered_revision"] = pending_revision
        state["pending_payload"] = None
        state["pending_revision"] = None
        return True

    @staticmethod
    def _mark_pending_for_all(topic: Dict[str, Any], *, payload: Dict[str, Any], revision: str) -> None:
        subscribers = topic.get("subscribers") or {}
        for state in subscribers.values():
            if state.get("last_delivered_revision") == revision:
                continue
            state["pending_payload"] = payload
            state["pending_revision"] = revision

    @staticmethod
    def _flush_due_subscribers(topic: Dict[str, Any], *, now: float) -> None:
        subscribers = topic.get("subscribers") or {}
        for queue, state in list(subscribers.items()):
            PayloadFanoutHub._flush_subscriber_state(queue, state, now=now)

    @staticmethod
    def _next_pending_due_seconds(topic: Dict[str, Any], *, now: float) -> Optional[float]:
        subscribers = topic.get("subscribers") or {}
        due_values: list[float] = []
        for state in subscribers.values():
            if state.get("pending_revision") in (None, "") or state.get("pending_payload") is None:
                continue
            interval_seconds = PayloadFanoutHub._normalize_client_interval(state.get("interval_seconds") or 0.0)
            last_sent_at = float(state.get("last_sent_at") or 0.0)
            remaining = 0.0 if last_sent_at <= 0 else max(0.0, interval_seconds - (now - last_sent_at))
            due_values.append(remaining)
        if not due_values:
            return None
        return min(due_values)

    async def subscribe(
        self,
        topic_key: str,
        *,
        builder: Callable[[], Dict[str, Any]],
        interval_seconds: float,
        revision_getter: Optional[Callable[[], Any]] = None,
        source_keys: Optional[list[str]] = None,
    ) -> asyncio.Queue:
        queue: asyncio.Queue = asyncio.Queue(maxsize=1)
        normalized_source_keys = {str(item) for item in (source_keys or []) if str(item)}
        async with self._lock:
            if self._stopped:
                raise RuntimeError("fanout hub stopped")
            topic = self._topics.get(topic_key)
            if topic is None:
                topic = {
                    "builder": builder,
                    "subscribers": {},
                    "task": None,
                    "latest_payload": None,
                    "latest_revision": None,
                    "latest_source_revision": None,
                    "revision_getter": revision_getter,
                    "wakeup": threading.Event(),
                }
                self._topics[topic_key] = topic
            else:
                topic["builder"] = builder
                topic["revision_getter"] = revision_getter
            subscriber_state = self._new_subscriber_state(interval_seconds)
            topic["subscribers"][queue] = subscriber_state
            wakeup = topic.get("wakeup")
            if wakeup is None:
                wakeup = threading.Event()
                topic["wakeup"] = wakeup
            with self._notify_lock:
                self._topic_source_keys[topic_key] = normalized_source_keys
                self._topic_wakeups[topic_key] = wakeup
            latest_payload = topic.get("latest_payload")
            if latest_payload is not None:
                subscriber_state["pending_payload"] = latest_payload
                subscriber_state["pending_revision"] = topic.get("latest_revision")
                self._flush_subscriber_state(queue, subscriber_state, now=time.monotonic())
            task = topic.get("task")
            if task is None or task.done():
                topic["task"] = asyncio.create_task(self._run_topic(topic_key), name=f"fanout::{topic_key[:80]}")
            if wakeup is not None:
                wakeup.set()
        return queue

    async def unsubscribe(self, topic_key: str, queue: asyncio.Queue) -> None:
        task = None
        async with self._lock:
            topic = self._topics.get(topic_key)
            if topic is None:
                return
            subscribers = topic.get("subscribers") or {}
            subscribers.pop(queue, None)
            if not topic["subscribers"]:
                task = topic.get("task")
                self._topics.pop(topic_key, None)
                with self._notify_lock:
                    self._topic_source_keys.pop(topic_key, None)
                    self._topic_wakeups.pop(topic_key, None)
        if task is not None:
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task

    async def stop(self) -> None:
        async with self._lock:
            self._stopped = True
            tasks = [topic.get("task") for topic in self._topics.values() if topic.get("task") is not None]
            self._topics.clear()
        with self._notify_lock:
            self._topic_source_keys.clear()
            self._topic_wakeups.clear()
        for task in tasks:
            task.cancel()
        for task in tasks:
            with suppress(asyncio.CancelledError):
                await task

    def notify_source_update(self, source_key: str) -> None:
        normalized = str(source_key or "")
        if not normalized:
            return
        with self._notify_lock:
            wakeups = [
                wakeup
                for topic_key, source_keys in self._topic_source_keys.items()
                if normalized in source_keys
                for wakeup in [self._topic_wakeups.get(topic_key)]
                if wakeup is not None
            ]
        for wakeup in wakeups:
            wakeup.set()

    async def _run_topic(self, topic_key: str) -> None:
        try:
            while True:
                async with self._lock:
                    topic = self._topics.get(topic_key)
                    if topic is None or self._stopped:
                        return
                    subscribers = list(topic.get("subscribers") or [])
                    if not subscribers:
                        self._topics.pop(topic_key, None)
                        with self._notify_lock:
                            self._topic_source_keys.pop(topic_key, None)
                            self._topic_wakeups.pop(topic_key, None)
                        return
                    builder = topic["builder"]
                    revision_getter = topic.get("revision_getter")
                    previous_revision = topic.get("latest_revision")
                    previous_source_revision = topic.get("latest_source_revision")
                    latest_payload_exists = topic.get("latest_payload") is not None
                    wakeup = topic.get("wakeup")
                    next_due = self._next_pending_due_seconds(topic, now=time.monotonic())
                wait_timeout = FANOUT_SOURCE_POLL_SECONDS if next_due is None else max(0.0, min(FANOUT_SOURCE_POLL_SECONDS, next_due))
                woke_by_signal = False
                if wakeup is not None and wait_timeout > 0:
                    woke_by_signal = await asyncio.to_thread(wakeup.wait, wait_timeout)
                    wakeup.clear()
                current_source_revision = None
                if revision_getter is not None:
                    try:
                        current_source_revision = revision_getter()
                    except Exception:
                        current_source_revision = None
                should_build = not latest_payload_exists
                if revision_getter is None:
                    should_build = should_build or woke_by_signal
                elif current_source_revision is None:
                    should_build = should_build or woke_by_signal
                elif current_source_revision != previous_source_revision:
                    should_build = True
                elif woke_by_signal:
                    should_build = True
                if not should_build:
                    async with self._lock:
                        topic = self._topics.get(topic_key)
                        if topic is None:
                            return
                        self._flush_due_subscribers(topic, now=time.monotonic())
                        next_due = self._next_pending_due_seconds(topic, now=time.monotonic())
                        wakeup = topic.get("wakeup")
                    if next_due is not None and next_due > 0:
                        if wakeup is not None:
                            await asyncio.to_thread(wakeup.wait, next_due)
                            wakeup.clear()
                        else:
                            await asyncio.sleep(next_due)
                    continue
                try:
                    payload = await asyncio.to_thread(builder)
                except Exception as exc:
                    payload = {"error": str(exc), "captured_at_ms": int(time.time() * 1000)}
                revision = _payload_revision(payload)
                async with self._lock:
                    topic = self._topics.get(topic_key)
                    if topic is None:
                        return
                    topic["latest_payload"] = payload
                    changed = revision != previous_revision
                    topic["latest_revision"] = revision
                    topic["latest_source_revision"] = current_source_revision if current_source_revision is not None else revision
                    if changed:
                        self._mark_pending_for_all(topic, payload=payload, revision=revision)
                    self._flush_due_subscribers(topic, now=time.monotonic())
                    next_due = self._next_pending_due_seconds(topic, now=time.monotonic())
                    wakeup = topic.get("wakeup")
                if next_due is not None and next_due > 0:
                    if wakeup is not None:
                        await asyncio.to_thread(wakeup.wait, next_due)
                        wakeup.clear()
                    else:
                        await asyncio.sleep(next_due)
        finally:
            async with self._lock:
                topic = self._topics.get(topic_key)
                if topic is not None and topic.get("task") is asyncio.current_task():
                    topic["task"] = None
                elif topic is None:
                    with self._notify_lock:
                        self._topic_source_keys.pop(topic_key, None)
                        self._topic_wakeups.pop(topic_key, None)

    @staticmethod
    def _offer(queue: asyncio.Queue, payload: Dict[str, Any]) -> None:
        while queue.full():
            try:
                queue.get_nowait()
            except asyncio.QueueEmpty:
                break
        try:
            queue.put_nowait(payload)
        except asyncio.QueueFull:
            pass


payload_fanout_hub = PayloadFanoutHub()


def _ensure_runtime_manager_listener_registered(manager: Optional[Any] = None) -> None:
    global _runtime_manager_listener_registered
    if _runtime_manager_listener_registered:
        return
    if manager is None:
        manager = get_runtime_manager()
    with _runtime_manager_lock:
        if _runtime_manager_listener_registered:
            return
        manager.register_cache_update_listener(payload_fanout_hub.notify_source_update)
        _runtime_manager_listener_registered = True


def _parse_exchange_keys(raw: str) -> list[str]:
    return normalize_csv_choices(raw, allowed=EXCHANGE_ORDER, default=EXCHANGE_ORDER)


def _parse_liquidation_map_exchange_keys(raw: Any) -> list[str]:
    return normalize_csv_choices(raw, allowed=LIQ_MAP_EXCHANGE_KEYS, default=LIQ_MAP_EXCHANGE_KEYS)


def _normalize_liquidation_request_params(
    *,
    exchange: str,
    exchange_keys: Any,
    archive_window: str,
    window_minutes: Optional[int],
    limit: int,
    min_notional: float,
    direction: str,
    scope: str,
    time_window: str,
) -> Any:
    return normalize_liquidation_request(
        exchange_key=exchange,
        exchange_keys=exchange_keys,
        archive_window=archive_window,
        window_minutes=window_minutes,
        limit=limit,
        min_notional=min_notional,
        direction=direction,
        scope=scope,
        time_window=time_window,
        allowed_exchanges=EXCHANGE_ORDER,
        allowed_archive_windows=LIQUIDATION_ARCHIVE_WINDOWS,
        allowed_time_windows=TIME_WINDOW_OPTIONS,
        default_exchange="binance",
    )


def _sanitize_ui_preferences(preferences: Dict[str, Any]) -> Dict[str, Any]:
    sanitized = dict(preferences or {})
    token = str(sanitized.pop("telegram_bot_token", "") or "").strip()
    chat_id = str(sanitized.pop("telegram_chat_id", "") or "").strip()
    sanitized["telegram_configured"] = bool(token and chat_id)
    rules = _load_custom_alert_rules_from_runtime({"custom_alert_rules": sanitized.get("custom_alert_rules")})
    sanitized["custom_alert_rules"] = rules
    sanitized["custom_alert_rule_count"] = len(rules)
    return sanitized


def _send_telegram_messages(messages: list[dict[str, str]], *, token: str, chat_id: str, timeout: int = 10) -> list[str]:
    if not messages or not token or not chat_id:
        return []
    errors: list[str] = []
    endpoint = f"https://api.telegram.org/bot{token}/sendMessage"
    for item in messages:
        try:
            response = requests.post(
                endpoint,
                json={
                    "chat_id": chat_id,
                    "text": f"{item.get('title') or 'Alert'}\n{item.get('body') or ''}".strip(),
                    "disable_web_page_preview": True,
                },
                timeout=timeout,
            )
            response.raise_for_status()
        except Exception as exc:
            errors.append(str(exc))
    return errors


def _json_default(value: Any) -> Any:
    if isinstance(value, Path):
        return str(value)
    if hasattr(value, "isoformat"):
        try:
            return value.isoformat()
        except Exception:
            pass
    if hasattr(value, "item"):
        try:
            return value.item()
        except Exception:
            pass
    return str(value)


def _topic_key(namespace: str, **params: Any) -> str:
    normalized = {key: value for key, value in params.items() if value not in (None, "")}
    return f"{namespace}::{json.dumps(normalized, sort_keys=True, ensure_ascii=False, default=_json_default)}"


def _overview_revision_getter(
    coin: str,
    *,
    exchange_keys: list[str],
    market_scope: str,
    time_window: str,
    min_notional: float = 0.0,
    custom_min_notional: float = 0.0,
    oi_threshold_pct: float = 0.0,
    anomaly_type: str = "all",
    whale_window: str = "5m",
    oi_window: str = "1h",
) -> Callable[[], Any]:
    normalized_coin = str(coin or "BTC").upper().strip() or "BTC"
    return lambda: runtime_manager.get_runtime(normalized_coin).overview_payload_cache_revision(
        exchange_keys=exchange_keys,
        market_scope=market_scope,
        time_window=time_window,
        min_notional=min_notional,
        custom_min_notional=custom_min_notional,
        oi_threshold_pct=oi_threshold_pct,
        anomaly_type=anomaly_type,
        whale_window=whale_window,
        oi_window=oi_window,
    )


def _overview_source_keys(
    coin: str,
    *,
    exchange_keys: list[str],
    market_scope: str,
    time_window: str,
    min_notional: float = 0.0,
    custom_min_notional: float = 0.0,
    oi_threshold_pct: float = 0.0,
    anomaly_type: str = "all",
    whale_window: str = "5m",
    oi_window: str = "1h",
) -> list[str]:
    normalized_coin = str(coin or "BTC").upper().strip() or "BTC"
    return [
        runtime_manager.get_runtime(normalized_coin).overview_payload_cache_key(
            exchange_keys=exchange_keys,
            market_scope=market_scope,
            time_window=time_window,
            min_notional=min_notional,
            custom_min_notional=custom_min_notional,
            oi_threshold_pct=oi_threshold_pct,
            anomaly_type=anomaly_type,
            whale_window=whale_window,
            oi_window=oi_window,
        )
    ]


def _alerts_revision_getter(
    coin: str,
    *,
    exchange_keys: list[str],
    market_scope: str,
    time_window: str,
    top_n: int = 20,
    ai_event_title: str = "",
    ai_event_time_ms: int = 0,
) -> Callable[[], Any]:
    normalized_coin = str(coin or "BTC").upper().strip() or "BTC"
    return lambda: runtime_manager.get_runtime(normalized_coin).alerts_payload_cache_revision(
        exchange_keys=exchange_keys,
        market_scope=market_scope,
        time_window=time_window,
        top_n=top_n,
        ai_event_title=ai_event_title,
        ai_event_time_ms=ai_event_time_ms,
    )


def _alerts_source_keys(
    coin: str,
    *,
    exchange_keys: list[str],
    market_scope: str,
    time_window: str,
    top_n: int = 20,
    ai_event_title: str = "",
    ai_event_time_ms: int = 0,
) -> list[str]:
    normalized_coin = str(coin or "BTC").upper().strip() or "BTC"
    return [
        runtime_manager.get_runtime(normalized_coin).alerts_payload_cache_key(
            exchange_keys=exchange_keys,
            market_scope=market_scope,
            time_window=time_window,
            top_n=top_n,
            ai_event_title=ai_event_title,
            ai_event_time_ms=ai_event_time_ms,
        )
    ]


def _monitor_revision_getter(
    coin: str,
    *,
    watch_group: str,
    coin_scope: str,
    exchange_keys: list[str],
    market_scope: str,
    time_window: str,
    min_notional: float,
    custom_min_notional: float,
    oi_threshold_pct: float,
    sentiment_mode: str,
    side: str,
    sort_by: str,
    aggregate_mode: str,
    top_n: int = 20,
) -> Callable[[], Any]:
    normalized_coin = str(coin or "BTC").upper().strip() or "BTC"
    return lambda: runtime_manager.get_runtime(normalized_coin).monitor_payload_cache_revision(
        watch_group=watch_group,
        coin_scope=coin_scope,
        exchange_keys=exchange_keys,
        market_scope=market_scope,
        time_window=time_window,
        top_n=top_n,
        min_notional=min_notional,
        custom_min_notional=custom_min_notional,
        oi_threshold_pct=oi_threshold_pct,
        sentiment_mode=sentiment_mode,
        side=side,
        sort_by=sort_by,
        aggregate_mode=aggregate_mode,
    )


def _monitor_source_keys(
    coin: str,
    *,
    watch_group: str,
    coin_scope: str,
    exchange_keys: list[str],
    market_scope: str,
    time_window: str,
    min_notional: float,
    custom_min_notional: float,
    oi_threshold_pct: float,
    sentiment_mode: str,
    side: str,
    sort_by: str,
    aggregate_mode: str,
    top_n: int = 20,
) -> list[str]:
    normalized_coin = str(coin or "BTC").upper().strip() or "BTC"
    return [
        runtime_manager.get_runtime(normalized_coin).monitor_payload_cache_key(
            watch_group=watch_group,
            coin_scope=coin_scope,
            exchange_keys=exchange_keys,
            market_scope=market_scope,
            time_window=time_window,
            top_n=top_n,
            min_notional=min_notional,
            custom_min_notional=custom_min_notional,
            oi_threshold_pct=oi_threshold_pct,
            sentiment_mode=sentiment_mode,
            side=side,
            sort_by=sort_by,
            aggregate_mode=aggregate_mode,
        )
    ]


def _depth_revision_getter(coin: str, *, exchange: str, market: str, levels: int) -> Callable[[], Any]:
    normalized_coin = str(coin or "BTC").upper().strip() or "BTC"
    normalized_exchange = str(exchange or "binance").lower().strip()
    normalized_market = "spot" if market == "spot" else "perp"
    normalized_levels = max(5, min(int(levels), 80))
    return lambda: runtime_manager.get_runtime(normalized_coin).depth_payload_source_revision(
        normalized_exchange,
        market=normalized_market,
    ) or runtime_manager.get_runtime(normalized_coin).depth_payload_cache_revision(
        normalized_exchange,
        market=normalized_market,
        levels=normalized_levels,
    )


def _depth_source_keys(coin: str, *, exchange: str, market: str) -> list[str]:
    normalized_coin = str(coin or "BTC").upper().strip() or "BTC"
    normalized_exchange = str(exchange or "binance").lower().strip()
    normalized_market = "spot" if market == "spot" else "perp"
    return [f"runtime::{normalized_coin}::depth::{normalized_exchange}::{normalized_market}"]


def _liquidation_revision_getter(
    coin: str,
    *,
    exchange_keys: list[str],
    map_exchange_keys: Optional[list[str]] = None,
    window_minutes: Optional[int] = None,
    limit: int = 120,
    exchange_key: str = "binance",
    archive_window: str = "4h",
    min_notional: float = 0.0,
    direction: str = "all",
    scope: str = "all",
    time_window: str = "1h",
) -> Callable[[], Any]:
    normalized_coin = str(coin or "BTC").upper().strip() or "BTC"
    request = _normalize_liquidation_request_params(
        exchange=exchange_key,
        exchange_keys=exchange_keys,
        archive_window=archive_window,
        window_minutes=window_minutes,
        limit=limit,
        min_notional=min_notional,
        direction=direction,
        scope=scope,
        time_window=time_window,
    )
    return lambda: runtime_manager.get_runtime(normalized_coin).liquidation_payload_source_revision(
        exchange_keys=request.exchange_keys,
        map_exchange_keys=map_exchange_keys,
    ) or runtime_manager.get_runtime(normalized_coin).liquidation_payload_cache_revision(
        window_minutes=request.window_minutes,
        limit=request.limit,
        exchange_key=request.exchange_key,
        archive_window=request.archive_window,
        min_notional=request.min_notional,
        direction=request.direction,
        scope=request.scope,
        exchange_keys=request.exchange_keys,
        time_window=request.time_window,
        map_exchange_keys=map_exchange_keys,
    )


def _liquidation_source_keys(coin: str, *, exchange_keys: list[str], map_exchange_keys: Optional[list[str]] = None) -> list[str]:
    normalized_coin = str(coin or "BTC").upper().strip() or "BTC"
    merged_exchange_keys = list(dict.fromkeys([*(exchange_keys or []), *(map_exchange_keys or [])]))
    return [f"runtime::{normalized_coin}::liquidations::{str(exchange_name).lower().strip()}::perp" for exchange_name in merged_exchange_keys]


async def _bootstrap_runtime_services() -> None:
    try:
        await asyncio.sleep(0)
        await asyncio.sleep(max(PRECOMPUTE_BOOT_DELAY_SECONDS, 0.0))
        if LEGACY_AUTOSTART:
            await asyncio.to_thread(_start_legacy_streamlit)

        def _start_runtime_background_stack() -> None:
            manager = get_runtime_manager()
            _ensure_runtime_manager_listener_registered(manager)
            manager.start_background_workers()

        await asyncio.to_thread(_start_runtime_background_stack)
    except asyncio.CancelledError:
        raise
    except Exception:
        return


async def _stream_payloads(
    builder: Callable[[], Dict[str, Any]],
    *,
    topic_key: str,
    interval_seconds: float = 1.0,
    event_name: str = "update",
    revision_getter: Optional[Callable[[], Any]] = None,
    source_keys: Optional[list[str]] = None,
) -> Any:
    queue = await payload_fanout_hub.subscribe(
        topic_key,
        builder=builder,
        interval_seconds=interval_seconds,
        revision_getter=revision_getter,
        source_keys=source_keys,
    )
    try:
        while True:
            payload = await queue.get()
            yield f"event: {event_name}\ndata: {json.dumps(payload, ensure_ascii=False, default=_json_default)}\n\n"
    finally:
        await payload_fanout_hub.unsubscribe(topic_key, queue)


def _is_port_open(port: int) -> bool:
    try:
        with socket.create_connection(("127.0.0.1", port), timeout=0.6):
            return True
    except OSError:
        return False


def _legacy_process_running() -> bool:
    return _legacy_process is not None and _legacy_process.poll() is None


def _start_legacy_streamlit() -> None:
    global _legacy_process
    with _legacy_process_lock:
        if _is_port_open(LEGACY_STREAMLIT_PORT) or _legacy_process_running():
            return
        creationflags = getattr(subprocess, "CREATE_NO_WINDOW", 0)
        _legacy_process = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "streamlit",
                "run",
                str(BASE_DIR / "app.py"),
                "--server.port",
                str(LEGACY_STREAMLIT_PORT),
                "--server.address",
                "127.0.0.1",
                "--server.headless",
                "true",
                "--server.enableCORS",
                "false",
                "--server.enableXsrfProtection",
                "false",
            ],
            cwd=str(BASE_DIR),
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            creationflags=creationflags,
        )


def _stop_legacy_streamlit() -> None:
    global _legacy_process
    with _legacy_process_lock:
        if _legacy_process is None:
            return
        if _legacy_process.poll() is None:
            try:
                _legacy_process.terminate()
            except Exception:
                pass
        _legacy_process = None


def _legacy_status_payload() -> Dict[str, Any]:
    running = _is_port_open(LEGACY_STREAMLIT_PORT)
    return {
        "running": running,
        "port": LEGACY_STREAMLIT_PORT,
        "url": LEGACY_STREAMLIT_URL,
        "managed": _legacy_process_running(),
        "status": "ready" if running else "starting",
        "checked_at_ms": int(time.time() * 1000),
    }

@asynccontextmanager
async def lifespan(app: FastAPI):
    bootstrap_task = asyncio.create_task(_bootstrap_runtime_services(), name="bootstrap-runtime-services")
    app.state.bootstrap_task = bootstrap_task
    try:
        yield
    finally:
        bootstrap = getattr(app.state, "bootstrap_task", None)
        if bootstrap is not None and not bootstrap.done():
            bootstrap.cancel()
            with suppress(asyncio.CancelledError):
                await bootstrap
        await payload_fanout_hub.stop()
        await asyncio.to_thread(_stop_legacy_streamlit)
        await asyncio.to_thread(runtime_manager.shutdown)


app = FastAPI(
    title="Liquid Glass Flow Desk API",
    version="2.0.0",
    docs_url="/api/docs",
    redoc_url="/api/redoc",
    lifespan=lifespan,
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class NoCacheStaticFiles(StaticFiles):
    async def get_response(self, path: str, scope):  # type: ignore[override]
        response = await super().get_response(path, scope)
        if response.status_code < 400:
            response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
            response.headers["Pragma"] = "no-cache"
            response.headers["Expires"] = "0"
        return response


if WEB_DIR.exists():
    app.mount("/static", NoCacheStaticFiles(directory=str(WEB_DIR)), name="static")

@app.get("/api/ping")
def ping() -> Dict[str, Any]:
    return {"ok": True}


@app.get("/api/coins")
def get_coins() -> Dict[str, Any]:
    catalog = runtime_manager.get_catalog(fast=True)
    coins = list(catalog.get("coins", []))
    default_pool = [coin for coin in ["BTC", "ETH", "SOL", "XRP", "BNB", "DOGE", "ADA", "PEPE"] if coin in coins]
    return {
        "coins": coins,
        "defaults": default_pool or coins[:12],
        "summary": catalog.get("summary", {}),
        "status": catalog.get("status", {}),
        "errors": catalog.get("errors", {}),
    }


@app.get("/api/legacy-status")
def get_legacy_status() -> Dict[str, Any]:
    _start_legacy_streamlit()
    return _legacy_status_payload()


@app.get("/api/overview")
def get_overview(
    coin: str = Query("BTC", min_length=1, max_length=20),
    exchange_keys: str = Query(""),
    market_scope: str = Query("merged"),
    time_window: str = Query("1h"),
    min_notional: float = Query(0.0, ge=0.0),
    custom_min_notional: float = Query(0.0, ge=0.0),
    oi_threshold_pct: float = Query(0.0, ge=0.0),
    anomaly_type: str = Query("all"),
    whale_window: str = Query("5m"),
    oi_window: str = Query("1h"),
) -> Dict[str, Any]:
    return runtime_manager.get_runtime(coin).overview_payload(
        exchange_keys=_parse_exchange_keys(exchange_keys),
        market_scope=market_scope,
        time_window=time_window,
        min_notional=min_notional,
        custom_min_notional=custom_min_notional,
        oi_threshold_pct=oi_threshold_pct,
        anomaly_type=anomaly_type,
        whale_window=whale_window,
        oi_window=oi_window,
        _sync_on_miss=True,
    )


@app.get("/api/overview-rich")
def get_overview_rich(
    coin: str = Query("BTC", min_length=1, max_length=20),
    exchange: str = Query("binance"),
    min_notional: float = Query(0.0, ge=0.0),
    oi_threshold_pct: float = Query(0.0, ge=0.0),
    sentiment_mode: str = Query("all"),
    watch_group: str = Query("board"),
    exchange_keys: str = Query(""),
    market_scope: str = Query("merged"),
    time_window: str = Query("1h"),
    ratio_window: str = Query("15m"),
    coin_scope: str = Query("majors"),
    custom_coins: str = Query(""),
    stage: str = Query("full"),
) -> Dict[str, Any]:
    return runtime_manager.get_runtime(coin).overview_rich_payload(
        exchange_key=exchange,
        min_notional=min_notional,
        oi_threshold_pct=oi_threshold_pct,
        sentiment_mode=sentiment_mode,
        watch_group=watch_group,
        exchange_keys=_parse_exchange_keys(exchange_keys),
        market_scope=market_scope,
        time_window=time_window,
        ratio_window=ratio_window,
        coin_scope=coin_scope,
        custom_coins=custom_coins,
        stage=stage,
    )


@app.get("/api/multicoin")
def get_multicoin(
    coin: str = Query("BTC", min_length=1, max_length=20),
    watch_group: str = Query("board"),
    coin_scope: str = Query("majors"),
    exchange_keys: str = Query(""),
    market_scope: str = Query("merged"),
    time_window: str = Query("1h"),
    top_n: int = Query(20, ge=1, le=100),
    min_notional: float = Query(0.0, ge=0.0),
    oi_threshold_pct: float = Query(0.0, ge=0.0),
    sentiment_mode: str = Query("all"),
) -> Dict[str, Any]:
    return runtime_manager.get_runtime(coin).multicoin_payload(
        watch_group=watch_group,
        coin_scope=coin_scope,
        exchange_keys=_parse_exchange_keys(exchange_keys),
        market_scope=market_scope,
        time_window=time_window,
        top_n=top_n,
        min_notional=min_notional,
        oi_threshold_pct=oi_threshold_pct,
        sentiment_mode=sentiment_mode,
        _sync_on_miss=True,
    )


@app.get("/api/monitor")
def get_monitor(
    coin: str = Query("BTC", min_length=1, max_length=20),
    watch_group: str = Query("board"),
    coin_scope: str = Query("majors"),
    exchange_keys: str = Query(""),
    market_scope: str = Query("merged"),
    time_window: str = Query("1h"),
    top_n: int = Query(20, ge=1, le=100),
    min_notional: float = Query(0.0, ge=0.0),
    custom_min_notional: float = Query(0.0, ge=0.0),
    oi_threshold_pct: float = Query(0.0, ge=0.0),
    sentiment_mode: str = Query("all"),
    side: str = Query("all"),
    sort_by: str = Query("signal_desc"),
    aggregate_mode: str = Query("trade"),
    anomaly_type: str = Query("all"),
    whale_window: str = Query("5m"),
    oi_window: str = Query("1h"),
) -> Dict[str, Any]:
    return runtime_manager.get_runtime(coin).monitor_payload(
        watch_group=watch_group,
        coin_scope=coin_scope,
        exchange_keys=_parse_exchange_keys(exchange_keys),
        market_scope=market_scope,
        time_window=time_window,
        top_n=top_n,
        min_notional=min_notional,
        custom_min_notional=custom_min_notional,
        oi_threshold_pct=oi_threshold_pct,
        sentiment_mode=sentiment_mode,
        side=side,
        sort_by=sort_by,
        aggregate_mode=aggregate_mode,
        anomaly_type=anomaly_type,
        whale_window=whale_window,
        oi_window=oi_window,
        _sync_on_miss=True,
    )


@app.get("/api/monitor/exchange-board")
def get_monitor_exchange_board(
    coin: str = Query("BTC", min_length=1, max_length=20),
    watch_group: str = Query("board"),
    coin_scope: str = Query("majors"),
    exchange_keys: str = Query(""),
    market_scope: str = Query("merged"),
    time_window: str = Query("1h"),
    top_n: int = Query(20, ge=1, le=100),
    min_notional: float = Query(0.0, ge=0.0),
    custom_min_notional: float = Query(0.0, ge=0.0),
    oi_threshold_pct: float = Query(0.0, ge=0.0),
    sentiment_mode: str = Query("all"),
    side: str = Query("all"),
    sort_by: str = Query("signal_desc"),
    aggregate_mode: str = Query("trade"),
    anomaly_type: str = Query("all"),
    whale_window: str = Query("5m"),
    oi_window: str = Query("1h"),
) -> Dict[str, Any]:
    return runtime_manager.get_runtime(coin).monitor_exchange_board_payload(
        watch_group=watch_group,
        coin_scope=coin_scope,
        exchange_keys=_parse_exchange_keys(exchange_keys),
        market_scope=market_scope,
        time_window=time_window,
        top_n=top_n,
        min_notional=min_notional,
        custom_min_notional=custom_min_notional,
        oi_threshold_pct=oi_threshold_pct,
        sentiment_mode=sentiment_mode,
        side=side,
        sort_by=sort_by,
        aggregate_mode=aggregate_mode,
        anomaly_type=anomaly_type,
        whale_window=whale_window,
        oi_window=oi_window,
    )


@app.get("/api/monitor/whales")
def get_monitor_whales(
    coin: str = Query("BTC", min_length=1, max_length=20),
    watch_group: str = Query("board"),
    coin_scope: str = Query("majors"),
    exchange_keys: str = Query(""),
    market_scope: str = Query("merged"),
    time_window: str = Query("1h"),
    top_n: int = Query(20, ge=1, le=100),
    min_notional: float = Query(0.0, ge=0.0),
    custom_min_notional: float = Query(0.0, ge=0.0),
    oi_threshold_pct: float = Query(0.0, ge=0.0),
    sentiment_mode: str = Query("all"),
    side: str = Query("all"),
    sort_by: str = Query("notional_desc"),
    aggregate_mode: str = Query("trade"),
    anomaly_type: str = Query("all"),
    whale_window: str = Query("5m"),
    oi_window: str = Query("1h"),
) -> Dict[str, Any]:
    return runtime_manager.get_runtime(coin).monitor_whales_payload(
        watch_group=watch_group,
        coin_scope=coin_scope,
        exchange_keys=_parse_exchange_keys(exchange_keys),
        market_scope=market_scope,
        time_window=time_window,
        top_n=top_n,
        min_notional=min_notional,
        custom_min_notional=custom_min_notional,
        oi_threshold_pct=oi_threshold_pct,
        sentiment_mode=sentiment_mode,
        side=side,
        sort_by=sort_by,
        aggregate_mode=aggregate_mode,
        anomaly_type=anomaly_type,
        whale_window=whale_window,
        oi_window=oi_window,
    )


@app.get("/api/monitor/price-matrix")
def get_monitor_price_matrix(
    coin: str = Query("BTC", min_length=1, max_length=20),
    watch_group: str = Query("board"),
    coin_scope: str = Query("majors"),
    exchange_keys: str = Query(""),
    market_scope: str = Query("merged"),
    time_window: str = Query("1h"),
    top_n: int = Query(20, ge=1, le=100),
    min_notional: float = Query(0.0, ge=0.0),
    custom_min_notional: float = Query(0.0, ge=0.0),
    oi_threshold_pct: float = Query(0.0, ge=0.0),
    sentiment_mode: str = Query("all"),
    side: str = Query("all"),
    sort_by: str = Query("signal_desc"),
    aggregate_mode: str = Query("trade"),
    anomaly_type: str = Query("all"),
    whale_window: str = Query("5m"),
    oi_window: str = Query("1h"),
) -> Dict[str, Any]:
    return runtime_manager.get_runtime(coin).monitor_price_matrix_payload(
        watch_group=watch_group,
        coin_scope=coin_scope,
        exchange_keys=_parse_exchange_keys(exchange_keys),
        market_scope=market_scope,
        time_window=time_window,
        top_n=top_n,
        min_notional=min_notional,
        custom_min_notional=custom_min_notional,
        oi_threshold_pct=oi_threshold_pct,
        sentiment_mode=sentiment_mode,
        side=side,
        sort_by=sort_by,
        aggregate_mode=aggregate_mode,
        anomaly_type=anomaly_type,
        whale_window=whale_window,
        oi_window=oi_window,
    )


@app.get("/api/monitor/oi-leaderboard")
def get_monitor_oi_leaderboard(
    coin: str = Query("BTC", min_length=1, max_length=20),
    watch_group: str = Query("board"),
    coin_scope: str = Query("majors"),
    exchange_keys: str = Query(""),
    market_scope: str = Query("merged"),
    time_window: str = Query("1h"),
    top_n: int = Query(20, ge=1, le=100),
    min_notional: float = Query(0.0, ge=0.0),
    custom_min_notional: float = Query(0.0, ge=0.0),
    oi_threshold_pct: float = Query(0.0, ge=0.0),
    sentiment_mode: str = Query("all"),
    side: str = Query("all"),
    sort_by: str = Query("oi_desc"),
    aggregate_mode: str = Query("trade"),
    anomaly_type: str = Query("all"),
    whale_window: str = Query("5m"),
    oi_window: str = Query("1h"),
) -> Dict[str, Any]:
    return runtime_manager.get_runtime(coin).monitor_oi_leaderboard_payload(
        watch_group=watch_group,
        coin_scope=coin_scope,
        exchange_keys=_parse_exchange_keys(exchange_keys),
        market_scope=market_scope,
        time_window=time_window,
        top_n=top_n,
        min_notional=min_notional,
        custom_min_notional=custom_min_notional,
        oi_threshold_pct=oi_threshold_pct,
        sentiment_mode=sentiment_mode,
        side=side,
        sort_by=sort_by,
        aggregate_mode=aggregate_mode,
        anomaly_type=anomaly_type,
        whale_window=whale_window,
        oi_window=oi_window,
    )


@app.get("/api/monitor/signals")
def get_monitor_signals(
    coin: str = Query("BTC", min_length=1, max_length=20),
    watch_group: str = Query("board"),
    coin_scope: str = Query("majors"),
    exchange_keys: str = Query(""),
    market_scope: str = Query("merged"),
    time_window: str = Query("1h"),
    top_n: int = Query(20, ge=1, le=100),
    min_notional: float = Query(0.0, ge=0.0),
    custom_min_notional: float = Query(0.0, ge=0.0),
    oi_threshold_pct: float = Query(0.0, ge=0.0),
    sentiment_mode: str = Query("all"),
    side: str = Query("all"),
    sort_by: str = Query("signal_desc"),
    aggregate_mode: str = Query("trade"),
    anomaly_type: str = Query("all"),
    whale_window: str = Query("5m"),
    oi_window: str = Query("1h"),
) -> Dict[str, Any]:
    return runtime_manager.get_runtime(coin).monitor_signals_payload(
        watch_group=watch_group,
        coin_scope=coin_scope,
        exchange_keys=_parse_exchange_keys(exchange_keys),
        market_scope=market_scope,
        time_window=time_window,
        top_n=top_n,
        min_notional=min_notional,
        custom_min_notional=custom_min_notional,
        oi_threshold_pct=oi_threshold_pct,
        sentiment_mode=sentiment_mode,
        side=side,
        sort_by=sort_by,
        aggregate_mode=aggregate_mode,
        anomaly_type=anomaly_type,
        whale_window=whale_window,
        oi_window=oi_window,
    )


@app.get("/api/execution")
def get_execution(
    coin: str = Query("BTC", min_length=1, max_length=20),
    exchange_keys: str = Query(""),
    market_scope: str = Query("merged"),
    time_window: str = Query("1h"),
    entry_spot_confirm_min: float = Query(62.0, ge=0.0, le=100.0),
    entry_spot_dispersion_max: float = Query(0.22, ge=0.0, le=10.0),
    entry_spot_cost_max: float = Query(16.0, ge=0.0, le=500.0),
    entry_perp_crowding_caution: float = Query(62.0, ge=0.0, le=100.0),
    entry_perp_crowding_block: float = Query(72.0, ge=0.0, le=100.0),
    entry_funding_caution: float = Query(4.0, ge=0.0, le=50.0),
    entry_funding_block: float = Query(5.5, ge=0.0, le=50.0),
) -> Dict[str, Any]:
    return runtime_manager.get_runtime(coin).execution_payload(
        exchange_keys=_parse_exchange_keys(exchange_keys),
        market_scope=market_scope,
        time_window=time_window,
        entry_spot_confirm_min=entry_spot_confirm_min,
        entry_spot_dispersion_max=entry_spot_dispersion_max,
        entry_spot_cost_max=entry_spot_cost_max,
        entry_perp_crowding_caution=entry_perp_crowding_caution,
        entry_perp_crowding_block=entry_perp_crowding_block,
        entry_funding_caution=entry_funding_caution,
        entry_funding_block=entry_funding_block,
        _sync_on_miss=True,
    )


@app.get("/api/depth")
def get_depth(
    coin: str = Query("BTC", min_length=1, max_length=20),
    exchange: str = Query("binance"),
    market: str = Query("perp"),
    levels: int = Query(24, ge=5, le=80),
) -> Dict[str, Any]:
    normalized_exchange = str(exchange or "binance").lower().strip()
    if normalized_exchange not in EXCHANGE_ORDER:
        raise HTTPException(status_code=404, detail="unknown exchange")
    normalized_market = "spot" if market == "spot" else "perp"
    if normalized_market == "spot" and normalized_exchange not in SPOT_EXCHANGE_ORDER:
        raise HTTPException(status_code=404, detail="spot unsupported on this exchange")
    return runtime_manager.get_runtime(coin).depth_payload(normalized_exchange, market=normalized_market, levels=levels)


@app.get("/api/tape")
def get_tape(
    coin: str = Query("BTC", min_length=1, max_length=20),
    limit: int = Query(120, ge=20, le=300),
    min_notional: float = Query(0.0, ge=0.0),
    event_kind: str = Query("all"),
    exchange_keys: str = Query(""),
    market_scope: str = Query("merged"),
    time_window: str = Query("1h"),
    anomaly_type: str = Query("all"),
    whale_window: str = Query("5m"),
    oi_window: str = Query("1h"),
) -> Dict[str, Any]:
    return runtime_manager.get_runtime(coin).tape_payload(
        limit=limit,
        min_notional=min_notional,
        event_kind=event_kind,
        exchange_keys=_parse_exchange_keys(exchange_keys),
        market_scope=market_scope,
        time_window=time_window,
        anomaly_type=anomaly_type,
        whale_window=whale_window,
        oi_window=oi_window,
    )


@app.get("/api/market-split")
def get_market_split(
    coin: str = Query("BTC", min_length=1, max_length=20),
    exchange_keys: str = Query(""),
    market_scope: str = Query("merged"),
) -> Dict[str, Any]:
    return runtime_manager.get_runtime(coin).market_split_payload(
        exchange_keys=_parse_exchange_keys(exchange_keys),
        market_scope=market_scope,
    )


@app.get("/api/liquidations")
def get_liquidations(
    coin: str = Query("BTC", min_length=1, max_length=20),
    exchange: str = Query("binance"),
    archive_window: str = Query("4h"),
    window_minutes: Optional[int] = Query(None, ge=5, le=720),
    limit: int = Query(120, ge=20, le=300),
    min_notional: float = Query(0.0, ge=0.0),
    direction: str = Query("all"),
    scope: str = Query("all"),
    exchange_keys: str = Query(""),
    time_window: str = Query("1h"),
    coins: str = Query(""),
    map_exchange_keys: str = Query(""),
    oi_rank_window: str = Query("1h"),
    visual_model: str = Query("realized"),
    visual_window: str = Query("24h"),
    visual_coin: str = Query(""),
    visual_exchange: str = Query("all"),
) -> Dict[str, Any]:
    request = _normalize_liquidation_request_params(
        exchange=exchange,
        exchange_keys=exchange_keys,
        archive_window=archive_window,
        window_minutes=window_minutes,
        limit=limit,
        min_notional=min_notional,
        direction=direction,
        scope=scope,
        time_window=time_window,
    )
    return runtime_manager.get_runtime(coin).liquidation_payload(
        window_minutes=request.window_minutes,
        limit=request.limit,
        exchange_key=request.exchange_key,
        archive_window=request.archive_window,
        min_notional=request.min_notional,
        direction=request.direction,
        scope=request.scope,
        exchange_keys=request.exchange_keys,
        time_window=request.time_window,
        coins=coins,
        map_exchange_keys=_parse_liquidation_map_exchange_keys(map_exchange_keys),
        oi_rank_window=oi_rank_window,
        visual_model=visual_model,
        visual_window=visual_window,
        visual_coin=visual_coin,
        visual_exchange=visual_exchange,
    )


@app.get("/api/alerts")
def get_alerts(
    coin: str = Query("BTC", min_length=1, max_length=20),
    watch_group: str = Query("board"),
    coin_scope: str = Query("board"),
    exchange_keys: str = Query(""),
    market_scope: str = Query("merged"),
    time_window: str = Query("1h"),
    top_n: int = Query(20, ge=1, le=100),
    min_notional: float = Query(0.0, ge=0.0),
    custom_min_notional: float = Query(0.0, ge=0.0),
    oi_threshold_pct: float = Query(0.0, ge=0.0),
    sentiment_mode: str = Query("all"),
    side: str = Query("all"),
    sort_by: str = Query("signal_desc"),
    aggregate_mode: str = Query("trade"),
    anomaly_type: str = Query("all"),
    whale_window: str = Query("5m"),
    oi_window: str = Query("1h"),
    ai_event_title: str = Query(""),
    ai_event_time_ms: int = Query(0, ge=0),
) -> Dict[str, Any]:
    return runtime_manager.get_runtime(coin).alerts_payload(
        watch_group=watch_group,
        coin_scope=coin_scope,
        exchange_keys=_parse_exchange_keys(exchange_keys),
        market_scope=market_scope,
        time_window=time_window,
        top_n=top_n,
        min_notional=min_notional,
        custom_min_notional=custom_min_notional,
        oi_threshold_pct=oi_threshold_pct,
        sentiment_mode=sentiment_mode,
        side=side,
        sort_by=sort_by,
        aggregate_mode=aggregate_mode,
        anomaly_type=anomaly_type,
        whale_window=whale_window,
        oi_window=oi_window,
        ai_event_title=ai_event_title,
        ai_event_time_ms=ai_event_time_ms,
        _sync_on_miss=True,
    )


@app.get("/api/ui-preferences")
def get_ui_preferences() -> Dict[str, Any]:
    return {"preferences": _sanitize_ui_preferences(_load_ui_preferences_snapshot_from_runtime())}


@app.post("/api/ui-preferences")
def save_ui_preferences(payload: Dict[str, Any] = Body(default_factory=dict)) -> Dict[str, Any]:
    saved = _save_ui_preferences_snapshot_from_runtime(dict(payload or {}))
    return {"ok": True, "preferences": _sanitize_ui_preferences(saved)}


@app.post("/api/notify/telegram")
def notify_telegram(payload: Dict[str, Any] = Body(default_factory=dict)) -> Dict[str, Any]:
    preferences = _load_ui_preferences_snapshot_from_runtime()
    token = str(payload.get("telegram_bot_token") or preferences.get("telegram_bot_token") or "").strip()
    chat_id = str(payload.get("telegram_chat_id") or preferences.get("telegram_chat_id") or "").strip()
    messages = payload.get("messages") if isinstance(payload.get("messages"), list) else []
    normalized_messages = [
        {"title": str(item.get("title") or "Alert"), "body": str(item.get("body") or "")}
        for item in messages
        if isinstance(item, dict)
    ]
    errors = _send_telegram_messages(normalized_messages, token=token, chat_id=chat_id)
    return {"ok": not errors, "sent": max(0, len(normalized_messages) - len(errors)), "errors": errors}


@app.get("/api/lab")
def get_lab(
    coin: str = Query("BTC", min_length=1, max_length=20),
    section: str = Query("overview"),
    exchange: str = Query("binance"),
    min_notional: float = Query(0.0, ge=0.0),
    oi_threshold_pct: float = Query(0.0, ge=0.0),
    sentiment_mode: str = Query("all"),
    watch_group: str = Query("majors"),
    exchange_keys: str = Query(""),
    market_scope: str = Query("merged"),
    time_window: str = Query("1h"),
    coin_scope: str = Query("majors"),
) -> Dict[str, Any]:
    return runtime_manager.get_runtime(coin).lab_payload(
        section=section,
        exchange_key=exchange,
        min_notional=min_notional,
        oi_threshold_pct=oi_threshold_pct,
        sentiment_mode=sentiment_mode,
        watch_group=watch_group,
        exchange_keys=_parse_exchange_keys(exchange_keys),
        market_scope=market_scope,
        time_window=time_window,
        coin_scope=coin_scope,
    )


@app.get("/api/lab/strategy-multicoin.csv")
def export_lab_strategy_multicoin_csv(
    coin: str = Query("BTC", min_length=1, max_length=20),
    oi_threshold_pct: float = Query(0.0, ge=0.0),
    sentiment_mode: str = Query("all"),
    watch_group: str = Query("majors"),
) -> Response:
    frame = runtime_manager.get_runtime(coin).strategy_multicoin_export_frame(
        oi_threshold_pct=oi_threshold_pct,
        sentiment_mode=sentiment_mode,
        watch_group=watch_group,
    )
    csv_text = frame.to_csv(index=False)
    filename = f"{coin.upper()}_multicoin_sentiment.csv"
    return Response(
        content=csv_text.encode("utf-8-sig"),
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename=\"{filename}\"'},
    )


@app.get("/api/orderbook-center")
def get_orderbook_center(
    coin: str = Query("BTC", min_length=1, max_length=20),
    exchange: str = Query("binance"),
    min_notional: float = Query(0.0, ge=0.0),
    event_kind: str = Query("all"),
) -> Dict[str, Any]:
    return runtime_manager.get_runtime(coin).orderbook_center_payload(exchange, min_notional=min_notional, event_kind=event_kind)


@app.get("/api/address-mode")
def get_address_mode(
    coin: str = Query("BTC", min_length=1, max_length=20),
    address: str = Query("", min_length=0, max_length=120),
    lookback_hours: int = Query(24, ge=1, le=168),
    stream: bool = Query(True),
) -> Dict[str, Any]:
    return runtime_manager.get_runtime(coin).address_payload(address, lookback_hours=lookback_hours, enable_stream=stream)


@app.get("/api/history")
def get_history(
    coin: str = Query("BTC", min_length=1, max_length=20),
    days: int = Query(3, ge=1, le=7),
    signal_kind: str = Query("all"),
    signal_exchange: str = Query("all"),
    event_kind: str = Query("all"),
    min_notional: float = Query(0.0, ge=0.0),
    oi_threshold_pct: float = Query(0.0, ge=0.0),
    exchange_keys: str = Query(""),
    market_scope: str = Query("merged"),
) -> Dict[str, Any]:
    return runtime_manager.get_runtime(coin).history_payload(
        days=days,
        signal_kind=signal_kind,
        signal_exchange=signal_exchange,
        event_kind=event_kind,
        min_notional=min_notional,
        oi_threshold_pct=oi_threshold_pct,
        exchange_keys=_parse_exchange_keys(exchange_keys),
        market_scope=market_scope,
    )


@app.get("/api/history/signals.csv")
def export_history_signals_csv(
    coin: str = Query("BTC", min_length=1, max_length=20),
    days: int = Query(3, ge=1, le=7),
    signal_kind: str = Query("all"),
    signal_exchange: str = Query("all"),
    min_notional: float = Query(0.0, ge=0.0),
    oi_threshold_pct: float = Query(0.0, ge=0.0),
    exchange_keys: str = Query(""),
) -> Response:
    frame = runtime_manager.get_runtime(coin).history_signals_export_frame(
        days=days,
        signal_kind=signal_kind,
        signal_exchange=signal_exchange,
        min_notional=min_notional,
        oi_threshold_pct=oi_threshold_pct,
        exchange_keys=_parse_exchange_keys(exchange_keys),
    )
    csv_text = frame.to_csv(index=False)
    filename = f"{coin.upper()}_signals_{days}d.csv"
    return Response(
        content=csv_text.encode("utf-8-sig"),
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@app.post("/api/history/signals/clear")
def clear_history_signals(
    coin: str = Query("BTC", min_length=1, max_length=20),
    days: int = Query(3, ge=1, le=7),
    signal_kind: str = Query("all"),
    signal_exchange: str = Query("all"),
    min_notional: float = Query(0.0, ge=0.0),
    oi_threshold_pct: float = Query(0.0, ge=0.0),
    exchange_keys: str = Query(""),
) -> Dict[str, Any]:
    return runtime_manager.get_runtime(coin).clear_history_signals(
        days=days,
        signal_kind=signal_kind,
        signal_exchange=signal_exchange,
        min_notional=min_notional,
        oi_threshold_pct=oi_threshold_pct,
        exchange_keys=_parse_exchange_keys(exchange_keys),
    )


@app.get("/api/debug")
def get_debug(
    coin: str = Query("BTC", min_length=1, max_length=20),
    exchange_keys: str = Query(""),
    market_scope: str = Query("merged"),
) -> Dict[str, Any]:
    return runtime_manager.get_runtime(coin).debug_payload(
        exchange_keys=_parse_exchange_keys(exchange_keys),
        market_scope=market_scope,
    )


@app.get("/api/health")
def get_health(
    coin: str = Query("BTC", min_length=1, max_length=20),
    exchange_keys: str = Query(""),
    market_scope: str = Query("merged"),
) -> Dict[str, Any]:
    return runtime_manager.get_runtime(coin).health_payload(
        exchange_keys=_parse_exchange_keys(exchange_keys),
        market_scope=market_scope,
    )


@app.get("/api/runtime-manager")
def get_runtime_manager_status() -> Dict[str, Any]:
    return runtime_manager.runtime_manager_payload()


@app.post("/api/runtime-manager/precompute-now")
def post_runtime_manager_precompute_now() -> Dict[str, Any]:
    runtime_manager.warm_hot_runtimes_now()
    return runtime_manager.runtime_manager_payload()


@app.get("/api/stream/depth")
async def stream_depth(
    coin: str = Query("BTC", min_length=1, max_length=20),
    exchange: str = Query("binance"),
    market: str = Query("perp"),
    levels: int = Query(24, ge=5, le=80),
    interval_ms: int = Query(900, ge=250, le=5000),
) -> StreamingResponse:
    normalized_exchange = str(exchange or "binance").lower().strip()
    if normalized_exchange not in EXCHANGE_ORDER:
        raise HTTPException(status_code=404, detail="unknown exchange")
    normalized_market = "spot" if market == "spot" else "perp"
    if normalized_market == "spot" and normalized_exchange not in SPOT_EXCHANGE_ORDER:
        raise HTTPException(status_code=404, detail="spot unsupported on this exchange")
    return StreamingResponse(
        _stream_payloads(
            lambda: runtime_manager.get_runtime(coin).depth_payload(normalized_exchange, market=normalized_market, levels=levels),
            topic_key=_topic_key("depth", coin=coin, exchange=normalized_exchange, market=normalized_market, levels=levels),
            interval_seconds=interval_ms / 1000.0,
            event_name="depth",
            revision_getter=_depth_revision_getter(coin, exchange=normalized_exchange, market=normalized_market, levels=levels),
            source_keys=_depth_source_keys(coin, exchange=normalized_exchange, market=normalized_market),
        ),
        media_type="text/event-stream",
    )


@app.get("/api/stream/liquidations")
async def stream_liquidations(
    coin: str = Query("BTC", min_length=1, max_length=20),
    exchange: str = Query("binance"),
    archive_window: str = Query("4h"),
    window_minutes: Optional[int] = Query(None, ge=5, le=720),
    limit: int = Query(120, ge=20, le=300),
    min_notional: float = Query(0.0, ge=0.0),
    direction: str = Query("all"),
    scope: str = Query("all"),
    exchange_keys: str = Query(""),
    time_window: str = Query("1h"),
    coins: str = Query(""),
    map_exchange_keys: str = Query(""),
    visual_model: str = Query("realized"),
    visual_window: str = Query("24h"),
    visual_coin: str = Query(""),
    visual_exchange: str = Query("all"),
    interval_ms: int = Query(1200, ge=250, le=5000),
) -> StreamingResponse:
    request = _normalize_liquidation_request_params(
        exchange=exchange,
        exchange_keys=exchange_keys,
        archive_window=archive_window,
        window_minutes=window_minutes,
        limit=limit,
        min_notional=min_notional,
        direction=direction,
        scope=scope,
        time_window=time_window,
    )
    normalized_map_exchange_keys = _parse_liquidation_map_exchange_keys(map_exchange_keys)
    return StreamingResponse(
        _stream_payloads(
            lambda: runtime_manager.get_runtime(coin).liquidation_payload(
                window_minutes=request.window_minutes,
                limit=request.limit,
                exchange_key=request.exchange_key,
                archive_window=request.archive_window,
                min_notional=request.min_notional,
                direction=request.direction,
                scope=request.scope,
                exchange_keys=request.exchange_keys,
                time_window=request.time_window,
                coins=coins,
                map_exchange_keys=normalized_map_exchange_keys,
                visual_model=visual_model,
                visual_window=visual_window,
                visual_coin=visual_coin,
                visual_exchange=visual_exchange,
            ),
            topic_key=_topic_key(
                "liquidations",
                coin=coin,
                exchange=request.exchange_key,
                archive_window=request.archive_window,
                window_minutes=request.window_minutes,
                limit=request.limit,
                min_notional=request.min_notional,
                direction=request.direction,
                scope=request.scope,
                exchange_keys=request.exchange_keys,
                time_window=request.time_window,
                coins=coins,
                map_exchange_keys=normalized_map_exchange_keys,
                visual_model=visual_model,
                visual_window=visual_window,
                visual_coin=visual_coin,
                visual_exchange=visual_exchange,
            ),
            interval_seconds=interval_ms / 1000.0,
            event_name="liquidations",
            revision_getter=_liquidation_revision_getter(
                coin,
                exchange_keys=request.exchange_keys,
                window_minutes=request.window_minutes,
                limit=request.limit,
                exchange_key=request.exchange_key,
                archive_window=request.archive_window,
                min_notional=request.min_notional,
                direction=request.direction,
                scope=request.scope,
                time_window=request.time_window,
                map_exchange_keys=normalized_map_exchange_keys,
            ),
            source_keys=_liquidation_source_keys(coin, exchange_keys=request.exchange_keys, map_exchange_keys=normalized_map_exchange_keys),
        ),
        media_type="text/event-stream",
    )


@app.get("/api/stream/alerts")
async def stream_alerts(
    coin: str = Query("BTC", min_length=1, max_length=20),
    watch_group: str = Query("board"),
    coin_scope: str = Query("board"),
    exchange_keys: str = Query(""),
    market_scope: str = Query("merged"),
    time_window: str = Query("1h"),
    top_n: int = Query(20, ge=1, le=100),
    min_notional: float = Query(0.0, ge=0.0),
    custom_min_notional: float = Query(0.0, ge=0.0),
    oi_threshold_pct: float = Query(0.0, ge=0.0),
    sentiment_mode: str = Query("all"),
    side: str = Query("all"),
    sort_by: str = Query("signal_desc"),
    aggregate_mode: str = Query("trade"),
    interval_ms: int = Query(1200, ge=250, le=5000),
) -> StreamingResponse:
    normalized_exchange_keys = _parse_exchange_keys(exchange_keys)
    return StreamingResponse(
        _stream_payloads(
            lambda: runtime_manager.get_runtime(coin).alerts_payload(
                watch_group=watch_group,
                coin_scope=coin_scope,
                exchange_keys=normalized_exchange_keys,
                market_scope=market_scope,
                time_window=time_window,
                top_n=top_n,
                min_notional=min_notional,
                custom_min_notional=custom_min_notional,
                oi_threshold_pct=oi_threshold_pct,
                sentiment_mode=sentiment_mode,
                side=side,
                sort_by=sort_by,
                aggregate_mode=aggregate_mode,
                ai_event_title="",
                ai_event_time_ms=0,
            ),
            topic_key=_topic_key(
                "alerts",
                coin=coin,
                watch_group=watch_group,
                coin_scope=coin_scope,
                exchange_keys=normalized_exchange_keys,
                market_scope=market_scope,
                time_window=time_window,
                top_n=top_n,
                min_notional=min_notional,
                custom_min_notional=custom_min_notional,
                oi_threshold_pct=oi_threshold_pct,
                sentiment_mode=sentiment_mode,
                side=side,
                sort_by=sort_by,
                aggregate_mode=aggregate_mode,
            ),
            interval_seconds=interval_ms / 1000.0,
            event_name="alerts",
            revision_getter=_alerts_revision_getter(
                coin,
                exchange_keys=normalized_exchange_keys,
                market_scope=market_scope,
                time_window=time_window,
            ),
            source_keys=_alerts_source_keys(
                coin,
                exchange_keys=normalized_exchange_keys,
                market_scope=market_scope,
                time_window=time_window,
            ),
        ),
        media_type="text/event-stream",
    )


@app.get("/api/stream/monitor/exchange-board")
async def stream_monitor_exchange_board(
    coin: str = Query("BTC", min_length=1, max_length=20),
    watch_group: str = Query("board"),
    coin_scope: str = Query("majors"),
    exchange_keys: str = Query(""),
    market_scope: str = Query("merged"),
    time_window: str = Query("1h"),
    min_notional: float = Query(0.0, ge=0.0),
    custom_min_notional: float = Query(0.0, ge=0.0),
    oi_threshold_pct: float = Query(0.0, ge=0.0),
    sentiment_mode: str = Query("all"),
    side: str = Query("all"),
    sort_by: str = Query("signal_desc"),
    aggregate_mode: str = Query("trade"),
    interval_ms: int = Query(2500, ge=500, le=8000),
) -> StreamingResponse:
    normalized_exchange_keys = _parse_exchange_keys(exchange_keys)
    return StreamingResponse(
        _stream_payloads(
            lambda: runtime_manager.get_runtime(coin).monitor_exchange_board_payload(
                watch_group=watch_group,
                coin_scope=coin_scope,
                exchange_keys=normalized_exchange_keys,
                market_scope=market_scope,
                time_window=time_window,
                min_notional=min_notional,
                custom_min_notional=custom_min_notional,
                oi_threshold_pct=oi_threshold_pct,
                sentiment_mode=sentiment_mode,
                side=side,
                sort_by=sort_by,
                aggregate_mode=aggregate_mode,
            ),
            topic_key=_topic_key(
                "monitor-exchange-board",
                coin=coin,
                watch_group=watch_group,
                coin_scope=coin_scope,
                exchange_keys=normalized_exchange_keys,
                market_scope=market_scope,
                time_window=time_window,
                min_notional=min_notional,
                custom_min_notional=custom_min_notional,
                oi_threshold_pct=oi_threshold_pct,
                sentiment_mode=sentiment_mode,
                side=side,
                sort_by=sort_by,
                aggregate_mode=aggregate_mode,
            ),
            interval_seconds=interval_ms / 1000.0,
            event_name="monitor_exchange_board",
            revision_getter=_monitor_revision_getter(
                coin,
                watch_group=watch_group,
                coin_scope=coin_scope,
                exchange_keys=normalized_exchange_keys,
                market_scope=market_scope,
                time_window=time_window,
                min_notional=min_notional,
                custom_min_notional=custom_min_notional,
                oi_threshold_pct=oi_threshold_pct,
                sentiment_mode=sentiment_mode,
                side=side,
                sort_by=sort_by,
                aggregate_mode=aggregate_mode,
            ),
            source_keys=_monitor_source_keys(
                coin,
                watch_group=watch_group,
                coin_scope=coin_scope,
                exchange_keys=normalized_exchange_keys,
                market_scope=market_scope,
                time_window=time_window,
                min_notional=min_notional,
                custom_min_notional=custom_min_notional,
                oi_threshold_pct=oi_threshold_pct,
                sentiment_mode=sentiment_mode,
                side=side,
                sort_by=sort_by,
                aggregate_mode=aggregate_mode,
            ),
        ),
        media_type="text/event-stream",
    )


@app.get("/api/stream/monitor/whales")
async def stream_monitor_whales(
    coin: str = Query("BTC", min_length=1, max_length=20),
    watch_group: str = Query("board"),
    coin_scope: str = Query("majors"),
    exchange_keys: str = Query(""),
    market_scope: str = Query("merged"),
    time_window: str = Query("1h"),
    min_notional: float = Query(0.0, ge=0.0),
    custom_min_notional: float = Query(0.0, ge=0.0),
    oi_threshold_pct: float = Query(0.0, ge=0.0),
    sentiment_mode: str = Query("all"),
    side: str = Query("all"),
    sort_by: str = Query("notional_desc"),
    aggregate_mode: str = Query("trade"),
    interval_ms: int = Query(1200, ge=500, le=5000),
) -> StreamingResponse:
    normalized_exchange_keys = _parse_exchange_keys(exchange_keys)
    return StreamingResponse(
        _stream_payloads(
            lambda: runtime_manager.get_runtime(coin).monitor_whales_payload(
                watch_group=watch_group,
                coin_scope=coin_scope,
                exchange_keys=normalized_exchange_keys,
                market_scope=market_scope,
                time_window=time_window,
                min_notional=min_notional,
                custom_min_notional=custom_min_notional,
                oi_threshold_pct=oi_threshold_pct,
                sentiment_mode=sentiment_mode,
                side=side,
                sort_by=sort_by,
                aggregate_mode=aggregate_mode,
            ),
            topic_key=_topic_key(
                "monitor-whales",
                coin=coin,
                watch_group=watch_group,
                coin_scope=coin_scope,
                exchange_keys=normalized_exchange_keys,
                market_scope=market_scope,
                time_window=time_window,
                min_notional=min_notional,
                custom_min_notional=custom_min_notional,
                oi_threshold_pct=oi_threshold_pct,
                sentiment_mode=sentiment_mode,
                side=side,
                sort_by=sort_by,
                aggregate_mode=aggregate_mode,
            ),
            interval_seconds=interval_ms / 1000.0,
            event_name="monitor_whales",
            revision_getter=_monitor_revision_getter(
                coin,
                watch_group=watch_group,
                coin_scope=coin_scope,
                exchange_keys=normalized_exchange_keys,
                market_scope=market_scope,
                time_window=time_window,
                min_notional=min_notional,
                custom_min_notional=custom_min_notional,
                oi_threshold_pct=oi_threshold_pct,
                sentiment_mode=sentiment_mode,
                side=side,
                sort_by=sort_by,
                aggregate_mode=aggregate_mode,
            ),
            source_keys=_monitor_source_keys(
                coin,
                watch_group=watch_group,
                coin_scope=coin_scope,
                exchange_keys=normalized_exchange_keys,
                market_scope=market_scope,
                time_window=time_window,
                min_notional=min_notional,
                custom_min_notional=custom_min_notional,
                oi_threshold_pct=oi_threshold_pct,
                sentiment_mode=sentiment_mode,
                side=side,
                sort_by=sort_by,
                aggregate_mode=aggregate_mode,
            ),
        ),
        media_type="text/event-stream",
    )


@app.get("/api/stream/monitor/signals")
async def stream_monitor_signals(
    coin: str = Query("BTC", min_length=1, max_length=20),
    watch_group: str = Query("board"),
    coin_scope: str = Query("majors"),
    exchange_keys: str = Query(""),
    market_scope: str = Query("merged"),
    time_window: str = Query("1h"),
    min_notional: float = Query(0.0, ge=0.0),
    custom_min_notional: float = Query(0.0, ge=0.0),
    oi_threshold_pct: float = Query(0.0, ge=0.0),
    sentiment_mode: str = Query("all"),
    side: str = Query("all"),
    sort_by: str = Query("signal_desc"),
    aggregate_mode: str = Query("trade"),
    interval_ms: int = Query(1800, ge=500, le=6000),
) -> StreamingResponse:
    normalized_exchange_keys = _parse_exchange_keys(exchange_keys)
    return StreamingResponse(
        _stream_payloads(
            lambda: runtime_manager.get_runtime(coin).monitor_signals_payload(
                watch_group=watch_group,
                coin_scope=coin_scope,
                exchange_keys=normalized_exchange_keys,
                market_scope=market_scope,
                time_window=time_window,
                min_notional=min_notional,
                custom_min_notional=custom_min_notional,
                oi_threshold_pct=oi_threshold_pct,
                sentiment_mode=sentiment_mode,
                side=side,
                sort_by=sort_by,
                aggregate_mode=aggregate_mode,
            ),
            topic_key=_topic_key(
                "monitor-signals",
                coin=coin,
                watch_group=watch_group,
                coin_scope=coin_scope,
                exchange_keys=normalized_exchange_keys,
                market_scope=market_scope,
                time_window=time_window,
                min_notional=min_notional,
                custom_min_notional=custom_min_notional,
                oi_threshold_pct=oi_threshold_pct,
                sentiment_mode=sentiment_mode,
                side=side,
                sort_by=sort_by,
                aggregate_mode=aggregate_mode,
            ),
            interval_seconds=interval_ms / 1000.0,
            event_name="monitor_signals",
            revision_getter=_monitor_revision_getter(
                coin,
                watch_group=watch_group,
                coin_scope=coin_scope,
                exchange_keys=normalized_exchange_keys,
                market_scope=market_scope,
                time_window=time_window,
                min_notional=min_notional,
                custom_min_notional=custom_min_notional,
                oi_threshold_pct=oi_threshold_pct,
                sentiment_mode=sentiment_mode,
                side=side,
                sort_by=sort_by,
                aggregate_mode=aggregate_mode,
            ),
            source_keys=_monitor_source_keys(
                coin,
                watch_group=watch_group,
                coin_scope=coin_scope,
                exchange_keys=normalized_exchange_keys,
                market_scope=market_scope,
                time_window=time_window,
                min_notional=min_notional,
                custom_min_notional=custom_min_notional,
                oi_threshold_pct=oi_threshold_pct,
                sentiment_mode=sentiment_mode,
                side=side,
                sort_by=sort_by,
                aggregate_mode=aggregate_mode,
            ),
        ),
        media_type="text/event-stream",
    )


async def _run_socket_loop(
    websocket: WebSocket,
    builder: Callable[[], Dict[str, Any]],
    *,
    topic_key: str,
    interval_seconds: float = 1.0,
    revision_getter: Optional[Callable[[], Any]] = None,
    source_keys: Optional[list[str]] = None,
) -> None:
    await websocket.accept()
    queue = await payload_fanout_hub.subscribe(
        topic_key,
        builder=builder,
        interval_seconds=interval_seconds,
        revision_getter=revision_getter,
        source_keys=source_keys,
    )
    try:
        while True:
            payload = await queue.get()
            await websocket.send_json(payload)
    except WebSocketDisconnect:
        return
    except Exception as exc:
        try:
            await websocket.send_json({"error": str(exc), "captured_at_ms": int(time.time() * 1000)})
        except Exception:
            pass
        try:
            await websocket.close()
        except Exception:
            pass
    finally:
        await payload_fanout_hub.unsubscribe(topic_key, queue)


@app.websocket("/ws/overview")
async def overview_socket(
    websocket: WebSocket,
    coin: str = "BTC",
    exchange_keys: str = "",
    market_scope: str = "merged",
    time_window: str = "1h",
    min_notional: float = 0.0,
    custom_min_notional: float = 0.0,
    oi_threshold_pct: float = 0.0,
    anomaly_type: str = "all",
    whale_window: str = "5m",
    oi_window: str = "1h",
) -> None:
    normalized_exchange_keys = _parse_exchange_keys(exchange_keys)
    await _run_socket_loop(
        websocket,
        lambda: runtime_manager.get_runtime(coin).overview_payload(
            exchange_keys=normalized_exchange_keys,
            market_scope=market_scope,
            time_window=time_window,
            min_notional=min_notional,
            custom_min_notional=custom_min_notional,
            oi_threshold_pct=oi_threshold_pct,
            anomaly_type=anomaly_type,
            whale_window=whale_window,
            oi_window=oi_window,
            _sync_on_miss=True,
        ),
        topic_key=_topic_key(
            "overview",
            coin=coin,
            exchange_keys=normalized_exchange_keys,
            market_scope=market_scope,
            time_window=time_window,
            min_notional=min_notional,
            custom_min_notional=custom_min_notional,
            oi_threshold_pct=oi_threshold_pct,
            anomaly_type=anomaly_type,
            whale_window=whale_window,
            oi_window=oi_window,
        ),
        interval_seconds=1.5,
        revision_getter=_overview_revision_getter(
            coin,
            exchange_keys=normalized_exchange_keys,
            market_scope=market_scope,
            time_window=time_window,
            min_notional=min_notional,
            custom_min_notional=custom_min_notional,
            oi_threshold_pct=oi_threshold_pct,
            anomaly_type=anomaly_type,
            whale_window=whale_window,
            oi_window=oi_window,
        ),
        source_keys=_overview_source_keys(
            coin,
            exchange_keys=normalized_exchange_keys,
            market_scope=market_scope,
            time_window=time_window,
            min_notional=min_notional,
            custom_min_notional=custom_min_notional,
            oi_threshold_pct=oi_threshold_pct,
            anomaly_type=anomaly_type,
            whale_window=whale_window,
            oi_window=oi_window,
        ),
    )


@app.websocket("/ws/depth")
async def depth_socket(
    websocket: WebSocket,
    coin: str = "BTC",
    exchange: str = "binance",
    market: str = "perp",
    levels: int = 24,
) -> None:
    normalized_exchange = str(exchange or "binance").lower().strip()
    if normalized_exchange not in EXCHANGE_ORDER:
        await websocket.accept()
        await websocket.send_json({"error": "unknown exchange"})
        await websocket.close()
        return
    normalized_market = "spot" if market == "spot" else "perp"
    if normalized_market == "spot" and normalized_exchange not in SPOT_EXCHANGE_ORDER:
        await websocket.accept()
        await websocket.send_json({"error": "spot unsupported on this exchange"})
        await websocket.close()
        return
    normalized_levels = max(5, min(int(levels), 80))
    await _run_socket_loop(
        websocket,
        lambda: runtime_manager.get_runtime(coin).depth_payload(normalized_exchange, market=normalized_market, levels=normalized_levels),
        topic_key=_topic_key("depth", coin=coin, exchange=normalized_exchange, market=normalized_market, levels=normalized_levels),
        interval_seconds=0.9,
        revision_getter=_depth_revision_getter(coin, exchange=normalized_exchange, market=normalized_market, levels=normalized_levels),
        source_keys=_depth_source_keys(coin, exchange=normalized_exchange, market=normalized_market),
    )


@app.websocket("/ws/liquidations")
async def liquidations_socket(
    websocket: WebSocket,
    coin: str = "BTC",
    exchange: str = "binance",
    archive_window: str = "4h",
    window_minutes: Optional[int] = None,
    limit: int = 120,
    min_notional: float = 0.0,
    direction: str = "all",
    scope: str = "all",
    exchange_keys: str = "",
    time_window: str = "1h",
    coins: str = "",
    map_exchange_keys: str = "",
) -> None:
    request = _normalize_liquidation_request_params(
        exchange=exchange,
        exchange_keys=exchange_keys,
        archive_window=archive_window,
        window_minutes=window_minutes,
        limit=limit,
        min_notional=min_notional,
        direction=direction,
        scope=scope,
        time_window=time_window,
    )
    normalized_map_exchange_keys = _parse_liquidation_map_exchange_keys(map_exchange_keys)
    await _run_socket_loop(
        websocket,
        lambda: runtime_manager.get_runtime(coin).liquidation_payload(
            window_minutes=request.window_minutes,
            limit=request.limit,
            exchange_key=request.exchange_key,
            archive_window=request.archive_window,
            min_notional=request.min_notional,
            direction=request.direction,
            scope=request.scope,
            exchange_keys=request.exchange_keys,
            time_window=request.time_window,
            coins=coins,
            map_exchange_keys=normalized_map_exchange_keys,
        ),
        topic_key=_topic_key(
            "liquidations",
            coin=coin,
            exchange=request.exchange_key,
            archive_window=request.archive_window,
            window_minutes=request.window_minutes,
            limit=request.limit,
            min_notional=request.min_notional,
            direction=request.direction,
            scope=request.scope,
            exchange_keys=request.exchange_keys,
            time_window=request.time_window,
            coins=coins,
            map_exchange_keys=normalized_map_exchange_keys,
        ),
        interval_seconds=1.2,
        revision_getter=_liquidation_revision_getter(
            coin,
            exchange_keys=request.exchange_keys,
            window_minutes=request.window_minutes,
            limit=request.limit,
            exchange_key=request.exchange_key,
            archive_window=request.archive_window,
            min_notional=request.min_notional,
            direction=request.direction,
            scope=request.scope,
            time_window=request.time_window,
            map_exchange_keys=normalized_map_exchange_keys,
        ),
        source_keys=_liquidation_source_keys(coin, exchange_keys=request.exchange_keys, map_exchange_keys=normalized_map_exchange_keys),
    )


@app.websocket("/ws/alerts")
async def alerts_socket(
    websocket: WebSocket,
    coin: str = "BTC",
    watch_group: str = "board",
    coin_scope: str = "board",
    exchange_keys: str = "",
    market_scope: str = "merged",
    time_window: str = "1h",
    top_n: int = 20,
    min_notional: float = 0.0,
    custom_min_notional: float = 0.0,
    oi_threshold_pct: float = 0.0,
    sentiment_mode: str = "all",
    side: str = "all",
    sort_by: str = "signal_desc",
    aggregate_mode: str = "trade",
    ai_event_title: str = "",
    ai_event_time_ms: int = 0,
) -> None:
    normalized_exchange_keys = _parse_exchange_keys(exchange_keys)
    await _run_socket_loop(
        websocket,
        lambda: runtime_manager.get_runtime(coin).alerts_payload(
            watch_group=watch_group,
            coin_scope=coin_scope,
            exchange_keys=normalized_exchange_keys,
            market_scope=market_scope,
            time_window=time_window,
            top_n=top_n,
            min_notional=min_notional,
            custom_min_notional=custom_min_notional,
            oi_threshold_pct=oi_threshold_pct,
            sentiment_mode=sentiment_mode,
            side=side,
            sort_by=sort_by,
            aggregate_mode=aggregate_mode,
            ai_event_title=ai_event_title,
            ai_event_time_ms=ai_event_time_ms,
        ),
        topic_key=_topic_key(
            "alerts",
            coin=coin,
            watch_group=watch_group,
            coin_scope=coin_scope,
            exchange_keys=normalized_exchange_keys,
            market_scope=market_scope,
            time_window=time_window,
            top_n=top_n,
            min_notional=min_notional,
            custom_min_notional=custom_min_notional,
            oi_threshold_pct=oi_threshold_pct,
            sentiment_mode=sentiment_mode,
            side=side,
            sort_by=sort_by,
            aggregate_mode=aggregate_mode,
            ai_event_title=ai_event_title,
            ai_event_time_ms=ai_event_time_ms,
        ),
        interval_seconds=1.5,
        revision_getter=_alerts_revision_getter(
            coin,
            exchange_keys=normalized_exchange_keys,
            market_scope=market_scope,
            time_window=time_window,
            ai_event_title=ai_event_title,
            ai_event_time_ms=ai_event_time_ms,
        ),
        source_keys=_alerts_source_keys(
            coin,
            exchange_keys=normalized_exchange_keys,
            market_scope=market_scope,
            time_window=time_window,
            ai_event_title=ai_event_title,
            ai_event_time_ms=ai_event_time_ms,
        ),
    )


@app.websocket("/ws/monitor/whales")
async def monitor_whales_socket(
    websocket: WebSocket,
    coin: str = "BTC",
    watch_group: str = "board",
    coin_scope: str = "majors",
    exchange_keys: str = "",
    market_scope: str = "merged",
    time_window: str = "1h",
    min_notional: float = 0.0,
    custom_min_notional: float = 0.0,
    oi_threshold_pct: float = 0.0,
    sentiment_mode: str = "all",
    side: str = "all",
    sort_by: str = "notional_desc",
    aggregate_mode: str = "trade",
) -> None:
    normalized_exchange_keys = _parse_exchange_keys(exchange_keys)
    await _run_socket_loop(
        websocket,
        lambda: runtime_manager.get_runtime(coin).monitor_whales_payload(
            watch_group=watch_group,
            coin_scope=coin_scope,
            exchange_keys=normalized_exchange_keys,
            market_scope=market_scope,
            time_window=time_window,
            min_notional=min_notional,
            custom_min_notional=custom_min_notional,
            oi_threshold_pct=oi_threshold_pct,
            sentiment_mode=sentiment_mode,
            side=side,
            sort_by=sort_by,
            aggregate_mode=aggregate_mode,
        ),
        topic_key=_topic_key(
            "monitor-whales",
            coin=coin,
            watch_group=watch_group,
            coin_scope=coin_scope,
            exchange_keys=normalized_exchange_keys,
            market_scope=market_scope,
            time_window=time_window,
            min_notional=min_notional,
            custom_min_notional=custom_min_notional,
            oi_threshold_pct=oi_threshold_pct,
            sentiment_mode=sentiment_mode,
            side=side,
            sort_by=sort_by,
            aggregate_mode=aggregate_mode,
        ),
        interval_seconds=2.0,
        revision_getter=_monitor_revision_getter(
            coin,
            watch_group=watch_group,
            coin_scope=coin_scope,
            exchange_keys=normalized_exchange_keys,
            market_scope=market_scope,
            time_window=time_window,
            min_notional=min_notional,
            custom_min_notional=custom_min_notional,
            oi_threshold_pct=oi_threshold_pct,
            sentiment_mode=sentiment_mode,
            side=side,
            sort_by=sort_by,
            aggregate_mode=aggregate_mode,
        ),
        source_keys=_monitor_source_keys(
            coin,
            watch_group=watch_group,
            coin_scope=coin_scope,
            exchange_keys=normalized_exchange_keys,
            market_scope=market_scope,
            time_window=time_window,
            min_notional=min_notional,
            custom_min_notional=custom_min_notional,
            oi_threshold_pct=oi_threshold_pct,
            sentiment_mode=sentiment_mode,
            side=side,
            sort_by=sort_by,
            aggregate_mode=aggregate_mode,
        ),
    )


@app.websocket("/ws/monitor/signals")
async def monitor_signals_socket(
    websocket: WebSocket,
    coin: str = "BTC",
    watch_group: str = "board",
    coin_scope: str = "majors",
    exchange_keys: str = "",
    market_scope: str = "merged",
    time_window: str = "1h",
    min_notional: float = 0.0,
    custom_min_notional: float = 0.0,
    oi_threshold_pct: float = 0.0,
    sentiment_mode: str = "all",
    side: str = "all",
    sort_by: str = "signal_desc",
    aggregate_mode: str = "trade",
) -> None:
    normalized_exchange_keys = _parse_exchange_keys(exchange_keys)
    await _run_socket_loop(
        websocket,
        lambda: runtime_manager.get_runtime(coin).monitor_signals_payload(
            watch_group=watch_group,
            coin_scope=coin_scope,
            exchange_keys=normalized_exchange_keys,
            market_scope=market_scope,
            time_window=time_window,
            min_notional=min_notional,
            custom_min_notional=custom_min_notional,
            oi_threshold_pct=oi_threshold_pct,
            sentiment_mode=sentiment_mode,
            side=side,
            sort_by=sort_by,
            aggregate_mode=aggregate_mode,
        ),
        topic_key=_topic_key(
            "monitor-signals",
            coin=coin,
            watch_group=watch_group,
            coin_scope=coin_scope,
            exchange_keys=normalized_exchange_keys,
            market_scope=market_scope,
            time_window=time_window,
            min_notional=min_notional,
            custom_min_notional=custom_min_notional,
            oi_threshold_pct=oi_threshold_pct,
            sentiment_mode=sentiment_mode,
            side=side,
            sort_by=sort_by,
            aggregate_mode=aggregate_mode,
        ),
        interval_seconds=2.0,
        revision_getter=_monitor_revision_getter(
            coin,
            watch_group=watch_group,
            coin_scope=coin_scope,
            exchange_keys=normalized_exchange_keys,
            market_scope=market_scope,
            time_window=time_window,
            min_notional=min_notional,
            custom_min_notional=custom_min_notional,
            oi_threshold_pct=oi_threshold_pct,
            sentiment_mode=sentiment_mode,
            side=side,
            sort_by=sort_by,
            aggregate_mode=aggregate_mode,
        ),
        source_keys=_monitor_source_keys(
            coin,
            watch_group=watch_group,
            coin_scope=coin_scope,
            exchange_keys=normalized_exchange_keys,
            market_scope=market_scope,
            time_window=time_window,
            min_notional=min_notional,
            custom_min_notional=custom_min_notional,
            oi_threshold_pct=oi_threshold_pct,
            sentiment_mode=sentiment_mode,
            side=side,
            sort_by=sort_by,
            aggregate_mode=aggregate_mode,
        ),
    )


@app.get("/legacy")
def open_legacy() -> RedirectResponse:
    _start_legacy_streamlit()
    return RedirectResponse(url=LEGACY_STREAMLIT_URL)


@app.get("/")
def index():
    index_path = WEB_DIR / "index.html"
    if not index_path.exists():
        return JSONResponse(
            {
                "message": "Frontend assets missing.",
                "hint": "Expected web/index.html beside api_server.py",
            },
            status_code=503,
        )
    response = FileResponse(index_path)
    response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"
    return response
