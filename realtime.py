from __future__ import annotations

from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import replace
from itertools import count
import json
from pathlib import Path
import re
import threading
import time
from typing import Any, Deque, Dict, List, Optional, Tuple

import websocket

from exchanges import (
    EXCHANGE_ORDER,
    SPOT_EXCHANGE_ORDER,
    build_clients,
    build_spot_clients,
    compute_notional,
    fetch_binance_futures_orderbook_snapshot,
    fetch_binance_spot_orderbook_snapshot,
    fetch_hyperliquid_address_mode,
    is_valid_onchain_address,
    normalize_liquidation_side,
    normalize_liquidation_side_for_exchange,
    normalize_onchain_address,
    normalize_trade_side,
    safe_float,
    safe_int,
)
from models import (
    ExchangeSnapshot,
    LiquidationEvent,
    OIPoint,
    OrderBookLevel,
    OrderBookQualityPoint,
    RecordedMarketEvent,
    SpotSnapshot,
    TradeEvent,
)

PERP_EXCHANGE_ORDER: Tuple[str, ...] = tuple(dict.fromkeys((*EXCHANGE_ORDER, "bitget")))


def _normalize_symbol_token(value: Optional[object]) -> str:
    return str(value or "").strip().upper()


def _okx_underlying_from_symbol(symbol: Optional[object]) -> str:
    parts = [part for part in str(symbol or "").strip().upper().split("-") if part]
    if len(parts) >= 2:
        return "-".join(parts[:2])
    return ""


def _extract_okx_liquidation_rows(payload_data: object) -> List[Dict[str, object]]:
    roots: List[Dict[str, object]] = []
    if isinstance(payload_data, list):
        roots = [item for item in payload_data if isinstance(item, dict)]
    elif isinstance(payload_data, dict):
        roots = [payload_data]
    rows: List[Dict[str, object]] = []
    for root in roots:
        details = root.get("details")
        if isinstance(details, list) and details:
            for detail in details:
                if not isinstance(detail, dict):
                    continue
                merged = dict(root)
                merged.update(detail)
                rows.append(merged)
        else:
            rows.append(dict(root))
    return rows


class LocalLiquidationArchive:
    def __init__(self, base_dir: Optional[Path] = None, retention_hours: int = 24 * 14) -> None:
        self.base_dir = base_dir or Path(__file__).with_name(".terminal_data").joinpath("liquidations")
        self.retention_hours = max(retention_hours, 24)
        self._last_prune_ms: Dict[str, int] = {}
        self.base_dir.mkdir(parents=True, exist_ok=True)

    @staticmethod
    def _safe_symbol(symbol: str) -> str:
        return "".join(char if char.isalnum() or char in {"-", "_"} else "_" for char in str(symbol or "").upper()) or "UNKNOWN"

    @staticmethod
    def _event_key(event: LiquidationEvent) -> tuple:
        return (
            event.exchange,
            event.symbol,
            event.timestamp_ms,
            event.side,
            round(event.price or 0.0, 6),
            round(event.size or 0.0, 6),
        )

    def _symbol_dir(self, exchange_key: str, symbol: str) -> Path:
        return self.base_dir / exchange_key / self._safe_symbol(symbol)

    def _event_path(self, exchange_key: str, event: LiquidationEvent) -> Path:
        day_key = time.strftime("%Y-%m-%d", time.gmtime(max(int(event.timestamp_ms), 0) / 1000.0))
        return self._symbol_dir(exchange_key, event.symbol) / f"{day_key}.jsonl"

    def _maybe_prune(self, symbol_dir: Path, now_ms: int) -> None:
        cache_key = str(symbol_dir)
        last_prune_ms = self._last_prune_ms.get(cache_key, 0)
        if now_ms - last_prune_ms < 30 * 60_000:
            return
        self._last_prune_ms[cache_key] = now_ms
        cutoff_ms = now_ms - self.retention_hours * 3_600_000
        try:
            for path in symbol_dir.glob("*.jsonl"):
                try:
                    if int(path.stat().st_mtime * 1000) < cutoff_ms:
                        path.unlink(missing_ok=True)
                except OSError:
                    continue
        except OSError:
            return

    def append(self, exchange_key: str, event: LiquidationEvent) -> None:
        path = self._event_path(exchange_key, event)
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            record = {
                "exchange": event.exchange,
                "symbol": event.symbol,
                "timestamp_ms": int(event.timestamp_ms),
                "side": event.side,
                "price": event.price,
                "size": event.size,
                "notional": event.notional,
                "source": event.source,
                "side_semantics": "liquidated_position",
                "side_schema_version": 2,
            }
            with path.open("a", encoding="utf-8") as handle:
                handle.write(json.dumps(record, ensure_ascii=True) + "\n")
        except OSError:
            return
        self._maybe_prune(path.parent, int(time.time() * 1000))

    def load(
        self,
        exchange_key: str,
        symbol: str,
        *,
        since_ms: Optional[int] = None,
        limit: int = 4000,
    ) -> List[LiquidationEvent]:
        symbol_dir = self._symbol_dir(exchange_key, symbol)
        if not symbol_dir.exists():
            return []
        events: List[LiquidationEvent] = []
        seen = set()
        paths = sorted(symbol_dir.glob("*.jsonl"))
        for path in paths:
            try:
                lines = path.read_text(encoding="utf-8").splitlines()
            except OSError:
                continue
            for line in lines:
                if not line.strip():
                    continue
                try:
                    payload = json.loads(line)
                except json.JSONDecodeError:
                    continue
                timestamp_ms = safe_int(payload.get("timestamp_ms")) or 0
                if since_ms is not None and timestamp_ms < since_ms:
                    continue
                side = normalize_liquidation_side(payload.get("side"))
                side_schema_version = safe_int(payload.get("side_schema_version")) or 0
                side_semantics = str(payload.get("side_semantics") or "").strip().lower()
                if (
                    str(exchange_key or "").strip().lower() == "bybit"
                    and str(payload.get("source") or "").strip().lower() == "ws"
                    and side_schema_version < 2
                    and side_semantics != "liquidated_position"
                ):
                    if side == "long":
                        side = "short"
                    elif side == "short":
                        side = "long"
                event = LiquidationEvent(
                    exchange=str(payload.get("exchange") or exchange_key.title()),
                    symbol=str(payload.get("symbol") or symbol),
                    timestamp_ms=timestamp_ms,
                    side=side,
                    price=safe_float(payload.get("price")),
                    size=safe_float(payload.get("size")),
                    notional=safe_float(payload.get("notional")),
                    source=str(payload.get("source") or "persisted"),
                )
                event_key = self._event_key(event)
                if event_key in seen:
                    continue
                seen.add(event_key)
                events.append(event)
        events.sort(key=lambda item: item.timestamp_ms)
        if limit > 0 and len(events) > limit:
            return events[-limit:]
        return events

    def describe(
        self,
        exchange_key: str,
        symbol: str,
        *,
        since_ms: Optional[int] = None,
        limit: int = 4000,
    ) -> Dict[str, Any]:
        events = self.load(exchange_key, symbol, since_ms=since_ms, limit=limit)
        symbol_dir = self._symbol_dir(exchange_key, symbol)
        return {
            "count": len(events),
            "first_timestamp_ms": events[0].timestamp_ms if events else None,
            "last_timestamp_ms": events[-1].timestamp_ms if events else None,
            "path": str(symbol_dir),
        }


class HyperliquidAddressStreamService:
    def __init__(
        self,
        address: str,
        coin: str,
        timeout: int = 10,
        lookback_hours: int = 24,
        fill_history_size: int = 500,
        funding_history_size: int = 240,
        event_history_size: int = 500,
    ) -> None:
        normalized_address = normalize_onchain_address(address)
        if not is_valid_onchain_address(normalized_address):
            raise ValueError("invalid Hyperliquid address")
        self.address = normalized_address
        self.coin = str(coin or "").strip().upper()
        self.timeout = timeout
        self.lookback_hours = max(1, int(lookback_hours))
        self.fill_history_size = max(100, fill_history_size)
        self.funding_history_size = max(60, funding_history_size)
        self.event_history_size = max(120, event_history_size)
        self.lock = threading.Lock()
        self.stop_event = threading.Event()
        self.ws_app: websocket.WebSocketApp | None = None
        self.thread: threading.Thread | None = None
        self.fills: Deque[Dict[str, Any]] = deque(maxlen=self.fill_history_size)
        self.fundings: Deque[Dict[str, Any]] = deque(maxlen=self.funding_history_size)
        self.user_events: Deque[Dict[str, Any]] = deque(maxlen=self.event_history_size)
        self.positions: List[Dict[str, Any]] = []
        self.active_asset: Dict[str, Any] = {}
        self.raw_state: Dict[str, Any] = {}
        self.role: str | None = None
        self.portfolio: List[dict] = []
        self.vault_equities: List[dict] = []
        self.vault_details: Dict[str, Any] = {}
        self.account_value: float | None = None
        self.total_margin_used: float | None = None
        self.withdrawable: float | None = None
        self.total_notional_position: float | None = None
        self.connected = False
        self.stream_status = "bootstrapping"
        self.error: str | None = None
        self.last_message_ms: int | None = None
        self.last_snapshot_ms: int | None = None
        self._hydrate_from_rest()
        self._start_thread()

    @staticmethod
    def _fill_key(item: Dict[str, Any]) -> tuple:
        return (
            str(item.get("coin") or ""),
            int(item.get("time") or 0),
            str(item.get("hash") or ""),
            round(float(item.get("price") or 0.0), 6),
            round(float(item.get("size") or 0.0), 6),
        )

    @staticmethod
    def _funding_key(item: Dict[str, Any]) -> tuple:
        return (
            str(item.get("coin") or ""),
            int(item.get("time") or 0),
            round(float(item.get("amount") or 0.0), 6),
            str(item.get("type") or ""),
        )

    @staticmethod
    def _parse_fill(item: Dict[str, Any]) -> Dict[str, Any]:
        price = safe_float(item.get("px")) or safe_float(item.get("price"))
        size = safe_float(item.get("sz")) or safe_float(item.get("size"))
        return {
            "time": safe_int(item.get("time")) or 0,
            "coin": str(item.get("coin") or ""),
            "direction": str(item.get("dir") or item.get("direction") or item.get("side") or ""),
            "side": normalize_trade_side(item.get("side")),
            "price": price,
            "size": size,
            "notional": compute_notional(price, size),
            "closed_pnl": safe_float(item.get("closedPnl")) or safe_float(item.get("closed_pnl")),
            "fee": safe_float(item.get("fee")),
            "fee_token": str(item.get("feeToken") or item.get("fee_token") or ""),
            "start_position": safe_float(item.get("startPosition")) or safe_float(item.get("start_position")),
            "hash": str(item.get("hash") or ""),
            "liquidation": item.get("liquidation"),
            "raw": item,
        }

    @staticmethod
    def _parse_funding(item: Dict[str, Any]) -> Dict[str, Any]:
        amount = safe_float(item.get("usdc")) or safe_float(item.get("amount"))
        return {
            "time": safe_int(item.get("time")) or 0,
            "coin": str(item.get("coin") or ""),
            "amount": amount,
            "type": str(item.get("type") or "funding"),
            "direction": "received" if amount is not None and amount >= 0 else "paid",
            "funding_rate": safe_float(item.get("fundingRate")) or safe_float(item.get("funding_rate")),
            "size": safe_float(item.get("szi")) or safe_float(item.get("size")),
            "raw": item,
        }

    @staticmethod
    def _parse_position(item: Dict[str, Any]) -> Dict[str, Any]:
        position = item.get("position") if isinstance(item.get("position"), dict) else item
        signed_size = safe_float(position.get("szi")) or safe_float(position.get("sz")) or safe_float(position.get("signedSz"))
        entry_price = safe_float(position.get("entryPx"))
        mark_price = safe_float(position.get("markPx"))
        role_side = "flat"
        if signed_size is not None:
            if signed_size > 0:
                role_side = "long"
            elif signed_size < 0:
                role_side = "short"
        return {
            "coin": str(position.get("coin") or item.get("coin") or ""),
            "side": role_side,
            "size": abs(signed_size) if signed_size is not None else None,
            "signed_size": signed_size,
            "entry_price": entry_price,
            "mark_price": mark_price,
            "liquidation_price": safe_float(position.get("liquidationPx")),
            "leverage": safe_float((position.get("leverage") or {}).get("value") if isinstance(position.get("leverage"), dict) else position.get("leverage")),
            "position_value": safe_float(position.get("positionValue")),
            "unrealized_pnl": safe_float(position.get("unrealizedPnl")),
            "return_on_equity": safe_float(position.get("returnOnEquity")),
            "raw": position,
        }

    def _hydrate_from_rest(self) -> None:
        try:
            bundle = fetch_hyperliquid_address_mode(self.address, self.coin, self.lookback_hours, timeout=self.timeout)
        except Exception as exc:
            with self.lock:
                self.stream_status = "error"
                self.error = str(exc)
            return
        with self.lock:
            self.account_value = safe_float(bundle.get("account_value"))
            self.total_margin_used = safe_float(bundle.get("total_margin_used"))
            self.withdrawable = safe_float(bundle.get("withdrawable"))
            self.total_notional_position = safe_float(bundle.get("total_notional_position"))
            self.active_asset = dict(bundle.get("active_asset") or {})
            self.raw_state = dict(bundle.get("raw_state") or {})
            self.role = str(bundle.get("role") or "") or None
            self.portfolio = list(bundle.get("portfolio") or [])
            self.vault_equities = list(bundle.get("vault_equities") or [])
            self.vault_details = dict(bundle.get("vault_details") or {})
            self.last_snapshot_ms = safe_int(bundle.get("timestamp_ms")) or int(time.time() * 1000)
            self.error = str(bundle.get("error") or "") or None
            self._replace_positions_locked(list(bundle.get("positions") or []))
            self._replace_fills_locked(list(bundle.get("fills") or []))
            self._replace_fundings_locked(list(bundle.get("funding") or []))

    def _start_thread(self) -> None:
        self.thread = threading.Thread(target=self._run_worker, name=f"ws-hyper-user-{self.address[:10]}", daemon=True)
        self.thread.start()

    def stop(self) -> None:
        self.stop_event.set()
        if self.ws_app is not None:
            try:
                self.ws_app.close()
            except Exception:
                pass

    def snapshot(self) -> Dict[str, Any]:
        with self.lock:
            return {
                "status": "ok" if self.stream_status != "error" else "error",
                "address": self.address,
                "coin": self.coin,
                "account_value": self.account_value,
                "total_margin_used": self.total_margin_used,
                "withdrawable": self.withdrawable,
                "total_notional_position": self.total_notional_position,
                "positions": list(self.positions),
                "fills": list(self.fills),
                "funding": list(self.fundings),
                "user_events": list(self.user_events),
                "active_asset": dict(self.active_asset),
                "raw_state": dict(self.raw_state),
                "role": self.role,
                "portfolio": list(self.portfolio),
                "vault_equities": list(self.vault_equities),
                "vault_details": dict(self.vault_details),
                "stream_status": self.stream_status,
                "connected": self.connected,
                "last_message_ms": self.last_message_ms,
                "timestamp_ms": self.last_snapshot_ms or self.last_message_ms,
                "error": self.error,
            }

    def get_transport_health(self) -> Dict[str, Any]:
        with self.lock:
            return {
                "connected": self.connected,
                "stream_status": self.stream_status,
                "last_message_ms": self.last_message_ms,
                "last_snapshot_ms": self.last_snapshot_ms,
                "fill_count": len(self.fills),
                "funding_count": len(self.fundings),
                "event_count": len(self.user_events),
                "error": self.error,
            }

    def _replace_positions_locked(self, positions: List[Dict[str, Any]]) -> None:
        selected = positions
        if self.coin:
            selected = [item for item in positions if str(item.get("coin") or "").upper() == self.coin]
        selected = sorted(selected, key=lambda item: abs(float(item.get("position_value") or 0.0)), reverse=True)
        self.positions = selected

    def _append_fill_locked(self, fill: Dict[str, Any]) -> None:
        if self.coin and str(fill.get("coin") or "").upper() != self.coin:
            return
        key = self._fill_key(fill)
        if any(self._fill_key(existing) == key for existing in self.fills):
            return
        self.fills.append(fill)

    def _replace_fills_locked(self, fills: List[Dict[str, Any]]) -> None:
        self.fills.clear()
        for item in sorted(fills, key=lambda value: int(value.get("time") or 0)):
            self._append_fill_locked(item)

    def _append_funding_locked(self, funding: Dict[str, Any]) -> None:
        if self.coin and str(funding.get("coin") or "").upper() != self.coin:
            return
        key = self._funding_key(funding)
        if any(self._funding_key(existing) == key for existing in self.fundings):
            return
        self.fundings.append(funding)

    def _replace_fundings_locked(self, funding_items: List[Dict[str, Any]]) -> None:
        self.fundings.clear()
        for item in sorted(funding_items, key=lambda value: int(value.get("time") or 0)):
            self._append_funding_locked(item)

    def _append_user_event_locked(self, category: str, payload: Dict[str, Any]) -> None:
        event = {
            "time": int(payload.get("time") or payload.get("timestamp_ms") or int(time.time() * 1000)),
            "category": category,
            "payload": payload,
        }
        event_key = (event["time"], category, json.dumps(payload, sort_keys=True, ensure_ascii=True))
        if any(
            (existing.get("time"), existing.get("category"), json.dumps(existing.get("payload"), sort_keys=True, ensure_ascii=True)) == event_key
            for existing in self.user_events
        ):
            return
        self.user_events.append(event)

    def _run_worker(self) -> None:
        while not self.stop_event.is_set():
            with self.lock:
                self.stream_status = "connecting"
            app = websocket.WebSocketApp(
                "wss://api.hyperliquid.xyz/ws",
                on_open=self._on_open,
                on_message=self._on_message,
                on_error=self._on_error,
                on_close=self._on_close,
            )
            self.ws_app = app
            try:
                app.run_forever(ping_interval=20, ping_timeout=10)
            except Exception as exc:
                self._on_error(app, exc)
            if self.stop_event.wait(3):
                return

    def _on_open(self, ws: websocket.WebSocketApp) -> None:
        subscriptions = [
            {"type": "clearinghouseState", "user": self.address},
            {"type": "userFills", "user": self.address, "aggregateByTime": True},
            {"type": "userFundings", "user": self.address},
            {"type": "userEvents", "user": self.address},
            {"type": "webData3", "user": self.address},
        ]
        if self.coin:
            subscriptions.append({"type": "activeAssetData", "user": self.address, "coin": self.coin})
        for subscription in subscriptions:
            ws.send(json.dumps({"method": "subscribe", "subscription": subscription}))
        with self.lock:
            self.connected = True
            self.stream_status = "live"
            self.error = None

    def _on_message(self, ws: websocket.WebSocketApp, message: str) -> None:
        try:
            payload = json.loads(message)
        except json.JSONDecodeError:
            return
        channel = str(payload.get("channel") or "")
        data = payload.get("data")
        with self.lock:
            self.connected = True
            self.stream_status = "live"
            self.error = None
            self.last_message_ms = int(time.time() * 1000)
        if channel == "subscriptionResponse":
            return
        if channel == "userFills":
            self._handle_user_fills(data)
            return
        if channel == "userFundings":
            self._handle_user_fundings(data)
            return
        if channel == "userEvents":
            self._handle_user_events(data)
            return
        if channel == "clearinghouseState":
            self._handle_state(data)
            return
        if channel == "activeAssetData":
            self._handle_active_asset_data(data)
            return
        if channel == "webData3":
            self._handle_web_data(data)

    def _on_error(self, ws: websocket.WebSocketApp, error: Exception) -> None:
        with self.lock:
            self.connected = False
            self.stream_status = "degraded"
            self.error = str(error)

    def _on_close(self, ws: websocket.WebSocketApp, close_status_code: object, close_message: object) -> None:
        with self.lock:
            self.connected = False
            if not self.stop_event.is_set() and self.stream_status != "error":
                self.stream_status = "reconnecting"

    def _handle_user_fills(self, data: Any) -> None:
        if not isinstance(data, dict):
            return
        fills = data.get("fills") or []
        if isinstance(fills, dict):
            fills = [fills]
        parsed = [self._parse_fill(item) for item in fills if isinstance(item, dict)]
        with self.lock:
            if bool(data.get("isSnapshot")):
                self._replace_fills_locked(parsed)
            else:
                for item in parsed:
                    self._append_fill_locked(item)

    def _handle_user_fundings(self, data: Any) -> None:
        if not isinstance(data, dict):
            return
        funding_items = data.get("fundings") or data.get("fundingPayments") or []
        if not funding_items and all(key in data for key in ("time", "coin")):
            funding_items = [data]
        if isinstance(funding_items, dict):
            funding_items = [funding_items]
        parsed = [self._parse_funding(item) for item in funding_items if isinstance(item, dict)]
        with self.lock:
            if bool(data.get("isSnapshot")):
                self._replace_fundings_locked(parsed)
            else:
                for item in parsed:
                    self._append_funding_locked(item)

    def _handle_user_events(self, data: Any) -> None:
        if not isinstance(data, dict):
            return
        with self.lock:
            if isinstance(data.get("fills"), list):
                for item in data.get("fills") or []:
                    if isinstance(item, dict):
                        parsed_fill = self._parse_fill(item)
                        self._append_fill_locked(parsed_fill)
                        self._append_user_event_locked("fills", parsed_fill)
            if isinstance(data.get("funding"), dict):
                parsed_funding = self._parse_funding(data.get("funding") or {})
                self._append_funding_locked(parsed_funding)
                self._append_user_event_locked("funding", parsed_funding)
            if isinstance(data.get("liquidation"), dict):
                payload = dict(data.get("liquidation") or {})
                payload["time"] = payload.get("time") or int(time.time() * 1000)
                self._append_user_event_locked("liquidation", payload)
            if isinstance(data.get("nonUserCancel"), list):
                for item in data.get("nonUserCancel") or []:
                    if isinstance(item, dict):
                        payload = dict(item)
                        payload["time"] = payload.get("time") or int(time.time() * 1000)
                        self._append_user_event_locked("nonUserCancel", payload)

    def _handle_state(self, data: Any) -> None:
        if not isinstance(data, dict):
            return
        positions = [self._parse_position(item) for item in data.get("assetPositions") or [] if isinstance(item, dict)]
        margin_summary = data.get("marginSummary") or {}
        with self.lock:
            self.account_value = safe_float(margin_summary.get("accountValue"))
            self.total_margin_used = safe_float(margin_summary.get("totalMarginUsed"))
            self.total_notional_position = safe_float(margin_summary.get("totalNtlPos"))
            self.withdrawable = safe_float(data.get("withdrawable"))
            self.raw_state = dict(data)
            self.last_snapshot_ms = safe_int(data.get("time")) or int(time.time() * 1000)
            self._replace_positions_locked(positions)

    def _handle_active_asset_data(self, data: Any) -> None:
        if not isinstance(data, dict):
            return
        with self.lock:
            self.active_asset = dict(data)

    def _handle_web_data(self, data: Any) -> None:
        if not isinstance(data, dict):
            return
        user_state = data.get("userState") if isinstance(data.get("userState"), dict) else {}
        perp_states = data.get("perpDexStates") if isinstance(data.get("perpDexStates"), list) else []
        leading_vaults: List[dict] = []
        total_vault_equity = 0.0
        for state in perp_states:
            if not isinstance(state, dict):
                continue
            for item in state.get("leadingVaults") or []:
                if isinstance(item, dict):
                    leading_vaults.append(item)
            total_vault_equity += float(safe_float(state.get("totalVaultEquity")) or 0.0)
        with self.lock:
            if not self.role:
                self.role = "vault" if bool(user_state.get("isVault")) else self.role
            if leading_vaults:
                self.vault_details.setdefault("leadingVaults", leading_vaults)
            if total_vault_equity > 0:
                self.vault_details.setdefault("totalVaultEquity", total_vault_equity)


class SharedRealtimeHub:
    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.stop_event = threading.Event()
        self.sample_wakeup = threading.Event()
        self.services: List["LiveTerminalService"] = []
        self.threads: Dict[str, threading.Thread] = {}
        self.ws_apps: Dict[str, websocket.WebSocketApp] = {}
        self.clients = build_clients(timeout=10)
        self.spot_clients = build_spot_clients(timeout=10)
        self.sample_seconds = 15
        self._worker_targets: Dict[str, Tuple[str, ...]] = {}
        self._control_message_ids = count(1)

    def register_service(self, service: "LiveTerminalService") -> None:
        with self.lock:
            if service not in self.services:
                self.services.append(service)
            self._ensure_workers_locked()
            self._refresh_worker_targets_locked()
        self.sample_wakeup.set()

    def unregister_service(self, service: "LiveTerminalService") -> None:
        with self.lock:
            self.services = [item for item in self.services if item is not service]
            self._refresh_worker_targets_locked()
        self.sample_wakeup.set()

    def stop(self) -> None:
        self.stop_event.set()
        self.sample_wakeup.set()
        with self.lock:
            apps = list(self.ws_apps.values())
        for app in apps:
            try:
                app.close()
            except Exception:
                pass

    def _refresh_worker_targets_locked(self) -> None:
        restart_keys: set[str] = set()
        for exchange_key in PERP_EXCHANGE_ORDER:
            targets = tuple(sorted({symbol for _, symbol in self._collect_targets_locked(exchange_key, spot=False)}))
            worker_key = exchange_key
            previous = self._worker_targets.get(worker_key, ())
            if targets != previous:
                current_app = self.ws_apps.get(worker_key)
                if current_app is not None and not targets:
                    restart_keys.add(worker_key)
                elif current_app is not None and self._supports_incremental_target_updates(exchange_key):
                    added = sorted(set(targets) - set(previous))
                    removed = sorted(set(previous) - set(targets))
                    try:
                        if removed:
                            self._send_subscription_delta(exchange_key, current_app, removed, spot=False, action="unsubscribe")
                        if added:
                            self._send_subscription_delta(exchange_key, current_app, added, spot=False, action="subscribe")
                    except Exception:
                        restart_keys.add(worker_key)
                else:
                    restart_keys.add(worker_key)
                self._worker_targets[worker_key] = targets
        for exchange_key in SPOT_EXCHANGE_ORDER:
            targets = tuple(sorted({symbol for _, symbol in self._collect_targets_locked(exchange_key, spot=True)}))
            worker_key = f"spot::{exchange_key}"
            previous = self._worker_targets.get(worker_key, ())
            if targets != previous:
                current_app = self.ws_apps.get(worker_key)
                if current_app is not None and not targets:
                    restart_keys.add(worker_key)
                elif current_app is not None and self._supports_incremental_target_updates(exchange_key):
                    added = sorted(set(targets) - set(previous))
                    removed = sorted(set(previous) - set(targets))
                    try:
                        if removed:
                            self._send_subscription_delta(exchange_key, current_app, removed, spot=True, action="unsubscribe")
                        if added:
                            self._send_subscription_delta(exchange_key, current_app, added, spot=True, action="subscribe")
                    except Exception:
                        restart_keys.add(worker_key)
                else:
                    restart_keys.add(worker_key)
                self._worker_targets[worker_key] = targets
        for restart_key in restart_keys:
            app = self.ws_apps.get(restart_key)
            if app is None:
                continue
            try:
                app.close()
            except Exception:
                continue

    def _ensure_workers_locked(self) -> None:
        if "sampler" not in self.threads or not self.threads["sampler"].is_alive():
            thread = threading.Thread(
                target=self._run_sampler,
                name="shared-sampler",
                daemon=True,
            )
            self.threads["sampler"] = thread
            thread.start()
        for exchange_key in PERP_EXCHANGE_ORDER:
            if exchange_key not in self.threads or not self.threads[exchange_key].is_alive():
                thread = threading.Thread(
                    target=self._run_perp_worker,
                    args=(exchange_key,),
                    name=f"shared-ws-{exchange_key}",
                    daemon=True,
                )
                self.threads[exchange_key] = thread
                thread.start()
        for exchange_key in SPOT_EXCHANGE_ORDER:
            worker_key = f"spot::{exchange_key}"
            if worker_key not in self.threads or not self.threads[worker_key].is_alive():
                thread = threading.Thread(
                    target=self._run_spot_worker,
                    args=(exchange_key,),
                    name=f"shared-ws-spot-{exchange_key}",
                    daemon=True,
                )
                self.threads[worker_key] = thread
                thread.start()

    def _collect_targets_locked(self, exchange_key: str, *, spot: bool = False) -> List[Tuple["LiveTerminalService", str]]:
        active_services: List["LiveTerminalService"] = []
        targets: List[Tuple["LiveTerminalService", str]] = []
        for service in self.services:
            if getattr(service, "stop_event", None) is not None and service.stop_event.is_set():
                continue
            active_services.append(service)
            symbol = service.spot_symbol_map.get(exchange_key) if spot else service.symbol_map.get(exchange_key)
            if symbol:
                targets.append((service, str(symbol)))
        self.services = active_services
        return targets

    def _collect_sample_targets_locked(self) -> Tuple[Dict[Tuple[str, str], List["LiveTerminalService"]], Dict[Tuple[str, str], List["LiveTerminalService"]]]:
        perp_targets: Dict[Tuple[str, str], List["LiveTerminalService"]] = {}
        spot_targets: Dict[Tuple[str, str], List["LiveTerminalService"]] = {}
        active_services: List["LiveTerminalService"] = []
        for service in self.services:
            if getattr(service, "stop_event", None) is not None and service.stop_event.is_set():
                continue
            active_services.append(service)
            for exchange_key, symbol in service.symbol_map.items():
                if symbol:
                    perp_targets.setdefault((exchange_key, str(symbol)), []).append(service)
            for exchange_key, symbol in service.spot_symbol_map.items():
                if symbol:
                    spot_targets.setdefault((exchange_key, str(symbol)), []).append(service)
        self.services = active_services
        return perp_targets, spot_targets

    def _build_ws_url(self, exchange_key: str, symbols: List[str], *, spot: bool = False) -> str:
        if exchange_key == "binance":
            return "wss://stream.binance.com:9443/ws" if spot else "wss://fstream.binance.com/ws"
        if exchange_key == "bybit":
            return "wss://stream.bybit.com/v5/public/spot" if spot else "wss://stream.bybit.com/v5/public/linear"
        if exchange_key == "okx":
            return "wss://ws.okx.com:8443/ws/v5/public"
        if exchange_key == "bitget":
            return "wss://ws.bitget.com/v3/ws/public"
        return "wss://api.hyperliquid.xyz/ws"

    @staticmethod
    def _binance_stream_params(symbols: List[str], *, spot: bool = False) -> List[str]:
        lowered = [str(symbol or "").strip().lower() for symbol in symbols if str(symbol or "").strip()]
        params: List[str] = []
        for symbol in lowered:
            if spot:
                params.extend([f"{symbol}@ticker", f"{symbol}@aggTrade", f"{symbol}@depth@100ms"])
            else:
                params.extend(
                    [
                        f"{symbol}@markPrice@1s",
                        f"{symbol}@ticker",
                        f"{symbol}@forceOrder",
                        f"{symbol}@aggTrade",
                        f"{symbol}@depth@100ms",
                    ]
                )
        return params

    @staticmethod
    def _bitget_public_args(symbols: List[str], *, include_liquidation: bool = True) -> List[Dict[str, str]]:
        args: List[Dict[str, str]] = []
        for symbol in symbols:
            normalized_symbol = str(symbol or "").strip().upper()
            if not normalized_symbol:
                continue
            args.extend(
                [
                    {"instType": "usdt-futures", "topic": "ticker", "symbol": normalized_symbol},
                    {"instType": "usdt-futures", "topic": "books50", "symbol": normalized_symbol},
                    {"instType": "usdt-futures", "topic": "publicTrade", "symbol": normalized_symbol},
                ]
            )
        if include_liquidation:
            args.append({"instType": "usdt-futures", "topic": "liquidation"})
        return args

    def _send_subscriptions(self, exchange_key: str, ws: websocket.WebSocketApp, symbols: List[str], *, spot: bool = False) -> None:
        if exchange_key == "binance":
            ws.send(json.dumps({"method": "SET_PROPERTY", "params": ["combined", True], "id": next(self._control_message_ids)}))
            ws.send(
                json.dumps(
                    {
                        "method": "SUBSCRIBE",
                        "params": self._binance_stream_params(symbols, spot=spot),
                        "id": next(self._control_message_ids),
                    }
                )
            )
            return
        if exchange_key == "bybit":
            args: List[str] = []
            for symbol in symbols:
                args.append(f"tickers.{symbol}")
                args.append(f"orderbook.50.{symbol}")
                args.append(f"publicTrade.{symbol}")
                if not spot:
                    args.append(f"allLiquidation.{symbol}")
            ws.send(json.dumps({"op": "subscribe", "args": args}))
            return
        if exchange_key == "okx":
            args: List[Dict[str, str]] = []
            for symbol in symbols:
                args.append({"channel": "tickers", "instId": symbol})
                args.append({"channel": "books5", "instId": symbol})
                args.append({"channel": "trades", "instId": symbol})
                if not spot:
                    args.append({"channel": "mark-price", "instId": symbol})
            ws.send(json.dumps({"op": "subscribe", "args": args}))
            return
        if exchange_key == "bitget" and not spot:
            ws.send(json.dumps({"op": "subscribe", "args": self._bitget_public_args(symbols, include_liquidation=True)}))
            return
        if exchange_key == "hyperliquid":
            ws.send(json.dumps({"method": "subscribe", "subscription": {"type": "allMids"}}))
            for symbol in symbols:
                ws.send(json.dumps({"method": "subscribe", "subscription": {"type": "l2Book", "coin": symbol}}))
                ws.send(json.dumps({"method": "subscribe", "subscription": {"type": "trades", "coin": symbol}}))

    @staticmethod
    def _supports_incremental_target_updates(exchange_key: str) -> bool:
        return str(exchange_key or "").strip().lower() in {"binance", "bybit", "okx", "hyperliquid", "bitget"}

    def _send_subscription_delta(
        self,
        exchange_key: str,
        ws: websocket.WebSocketApp,
        symbols: List[str],
        *,
        spot: bool = False,
        action: str = "subscribe",
    ) -> None:
        normalized_action = "unsubscribe" if str(action or "").strip().lower() == "unsubscribe" else "subscribe"
        if not symbols:
            return
        if exchange_key == "binance":
            ws.send(
                json.dumps(
                    {
                        "method": "UNSUBSCRIBE" if normalized_action == "unsubscribe" else "SUBSCRIBE",
                        "params": self._binance_stream_params(symbols, spot=spot),
                        "id": next(self._control_message_ids),
                    }
                )
            )
            return
        if exchange_key == "bybit":
            args: List[str] = []
            for symbol in symbols:
                args.append(f"tickers.{symbol}")
                args.append(f"orderbook.50.{symbol}")
                args.append(f"publicTrade.{symbol}")
                if not spot:
                    args.append(f"allLiquidation.{symbol}")
            ws.send(json.dumps({"op": normalized_action, "args": args}))
            return
        if exchange_key == "okx":
            args: List[Dict[str, str]] = []
            for symbol in symbols:
                args.append({"channel": "tickers", "instId": symbol})
                args.append({"channel": "books5", "instId": symbol})
                args.append({"channel": "trades", "instId": symbol})
                if not spot:
                    args.append({"channel": "mark-price", "instId": symbol})
            ws.send(json.dumps({"op": normalized_action, "args": args}))
            return
        if exchange_key == "bitget" and not spot:
            ws.send(json.dumps({"op": normalized_action, "args": self._bitget_public_args(symbols, include_liquidation=False)}))
            return
        if exchange_key == "hyperliquid" and not spot:
            if normalized_action == "subscribe":
                ws.send(json.dumps({"method": "subscribe", "subscription": {"type": "allMids"}}))
            for symbol in symbols:
                ws.send(json.dumps({"method": normalized_action, "subscription": {"type": "l2Book", "coin": symbol}}))
                ws.send(json.dumps({"method": normalized_action, "subscription": {"type": "trades", "coin": symbol}}))
            return

    def _matching_targets(self, exchange_key: str, payload: Dict[str, object], targets: List[Tuple["LiveTerminalService", str]]) -> List[Tuple["LiveTerminalService", str]]:
        if not targets:
            return []
        if exchange_key == "hyperliquid" and str(payload.get("channel") or "") == "allMids":
            return targets
        symbol: Optional[str] = None
        if exchange_key == "binance":
            data = payload.get("data") or {}
            if isinstance(data, dict):
                symbol = str(data.get("s") or "").strip() or None
        elif exchange_key == "bybit":
            topic = str(payload.get("topic") or "")
            if "." in topic:
                symbol = topic.split(".")[-1].strip() or None
        elif exchange_key == "okx":
            arg = payload.get("arg") or {}
            if isinstance(arg, dict):
                symbol = str(arg.get("instId") or "").strip() or None
        elif exchange_key == "hyperliquid":
            data = payload.get("data") or {}
            if isinstance(data, dict):
                symbol = str(data.get("coin") or "").strip() or None
            elif isinstance(data, list) and data and isinstance(data[0], dict):
                symbol = str(data[0].get("coin") or "").strip() or None
        elif exchange_key == "bitget":
            arg = payload.get("arg") or {}
            if isinstance(arg, dict):
                topic = str(arg.get("topic") or "").strip().lower()
                if topic == "liquidation":
                    return targets
                symbol = str(arg.get("symbol") or "").strip() or None
        if not symbol:
            return targets
        normalized = symbol.upper()
        return [(service, target_symbol) for service, target_symbol in targets if str(target_symbol).upper() == normalized]

    def _build_snapshot_event(
        self,
        exchange_key: str,
        payload: Dict[str, object],
        symbol: str,
        *,
        spot: bool = False,
    ) -> Dict[str, object] | None:
        normalized_symbol = str(symbol or "").strip()
        if not normalized_symbol:
            return None
        now_ms = int(time.time() * 1000)
        values: Dict[str, object] | None = None

        if exchange_key == "binance":
            stream = str(payload.get("stream") or "").lower()
            data = payload.get("data") or {}
            if not isinstance(data, dict):
                return None
            event_ts_ms = safe_int(data.get("E")) or now_ms
            if not spot and "markprice" in stream:
                values = {
                    "mark_price": safe_float(data.get("p")),
                    "index_price": safe_float(data.get("i")),
                    "funding_rate": safe_float(data.get("r")),
                    "timestamp_ms": event_ts_ms,
                }
            elif "@ticker" in stream:
                values = {
                    "last_price": safe_float(data.get("c")),
                    "volume_24h_base": safe_float(data.get("v")),
                    "volume_24h_notional": safe_float(data.get("q")),
                    "timestamp_ms": event_ts_ms,
                }
        elif exchange_key == "bybit":
            if payload.get("success") is not None:
                return None
            topic = str(payload.get("topic") or "")
            data = payload.get("data") or {}
            if not isinstance(data, dict):
                return None
            event_ts_ms = safe_int(payload.get("ts")) or now_ms
            if topic.startswith("tickers"):
                if spot:
                    values = {
                        "last_price": safe_float(data.get("lastPrice")),
                        "bid_price": safe_float(data.get("bid1Price")),
                        "ask_price": safe_float(data.get("ask1Price")),
                        "volume_24h_base": safe_float(data.get("volume24h")),
                        "volume_24h_notional": safe_float(data.get("turnover24h")),
                        "timestamp_ms": event_ts_ms,
                    }
                else:
                    values = {
                        "last_price": safe_float(data.get("lastPrice")),
                        "mark_price": safe_float(data.get("markPrice")),
                        "index_price": safe_float(data.get("indexPrice")),
                        "open_interest": safe_float(data.get("openInterest")),
                        "open_interest_notional": safe_float(data.get("openInterestValue")),
                        "funding_rate": safe_float(data.get("fundingRate")),
                        "volume_24h_base": safe_float(data.get("volume24h")),
                        "volume_24h_notional": safe_float(data.get("turnover24h")),
                        "timestamp_ms": event_ts_ms,
                    }
        elif exchange_key == "okx":
            if payload.get("event"):
                return None
            arg = payload.get("arg") or {}
            if not isinstance(arg, dict):
                return None
            channel = str(arg.get("channel") or "")
            data_list = payload.get("data") or [{}]
            data = data_list[0] if isinstance(data_list, list) and data_list else {}
            if not isinstance(data, dict):
                return None
            event_ts_ms = safe_int(data.get("ts")) or now_ms
            if channel == "tickers":
                if spot:
                    values = {
                        "last_price": safe_float(data.get("last")),
                        "bid_price": safe_float(data.get("bidPx")),
                        "ask_price": safe_float(data.get("askPx")),
                        "volume_24h_base": safe_float(data.get("vol24h")),
                        "volume_24h_notional": safe_float(data.get("volCcy24h")),
                        "timestamp_ms": event_ts_ms,
                    }
                else:
                    last_price = safe_float(data.get("last"))
                    base_volume = safe_float(data.get("volCcy24h"))
                    notional_volume = None
                    if base_volume is not None and last_price is not None:
                        notional_volume = base_volume * last_price
                    values = {
                        "last_price": last_price,
                        "volume_24h_base": base_volume,
                        "volume_24h_notional": notional_volume,
                        "timestamp_ms": event_ts_ms,
                    }
            elif not spot and channel == "mark-price":
                values = {
                    "mark_price": safe_float(data.get("markPx")),
                    "timestamp_ms": event_ts_ms,
                }
        elif exchange_key == "hyperliquid" and not spot:
            channel = str(payload.get("channel") or "")
            if channel == "allMids":
                data = payload.get("data") or {}
                mids = data.get("mids") if isinstance(data, dict) else {}
                if isinstance(mids, dict):
                    values = {
                        "last_price": safe_float(mids.get(normalized_symbol)),
                        "timestamp_ms": now_ms,
                    }
        elif exchange_key == "bitget" and not spot:
            arg = payload.get("arg") or {}
            if isinstance(arg, dict) and str(arg.get("topic") or "").strip().lower() in {"ticker", "tickers"}:
                data_list = payload.get("data") or []
                data = data_list[0] if isinstance(data_list, list) and data_list else {}
                if isinstance(data, dict):
                    price = safe_float(data.get("lastPr")) or safe_float(data.get("last"))
                    open_interest = safe_float(data.get("holdingAmount"))
                    open_interest_notional = safe_float(data.get("holdingAmountUsdt"))
                    if open_interest_notional is None and open_interest is not None and price is not None:
                        open_interest_notional = open_interest * price
                    values = {
                        "last_price": price,
                        "mark_price": safe_float(data.get("markPrice")),
                        "index_price": safe_float(data.get("indexPrice")),
                        "open_interest": open_interest,
                        "open_interest_notional": open_interest_notional,
                        "funding_rate": safe_float(data.get("fundingRate")),
                        "volume_24h_base": safe_float(data.get("baseVolume")),
                        "volume_24h_notional": safe_float(data.get("usdtVolume")) or safe_float(data.get("quoteVolume")),
                        "timestamp_ms": safe_int(data.get("ts")) or safe_int(payload.get("ts")) or now_ms,
                    }

        if not values:
            return None
        if not any(value is not None for key, value in values.items() if key != "timestamp_ms"):
            return None
        return {
            "kind": "snapshot_update",
            "exchange_key": exchange_key,
            "spot": bool(spot),
            "symbol": normalized_symbol,
            "values": values,
        }

    def _build_structured_event(
        self,
        exchange_key: str,
        payload: Dict[str, object],
        symbol: str,
        *,
        spot: bool = False,
    ) -> Dict[str, object] | None:
        snapshot_event = self._build_snapshot_event(exchange_key, payload, symbol, spot=spot)
        if snapshot_event is not None:
            return snapshot_event

        normalized_symbol = str(symbol or "").strip()
        if not normalized_symbol:
            return None
        now_ms = int(time.time() * 1000)

        if not spot and exchange_key == "binance":
            stream = str(payload.get("stream") or "").lower()
            data = payload.get("data") or {}
            if not isinstance(data, dict):
                return None
            if "@forceorder" in stream:
                order = data.get("o") or {}
                if not isinstance(order, dict):
                    return None
                price = safe_float(order.get("ap")) or safe_float(order.get("p"))
                size = safe_float(order.get("z")) or safe_float(order.get("q"))
                return {
                    "kind": "liquidation_event",
                    "exchange_key": exchange_key,
                    "spot": False,
                    "symbol": str(order.get("s") or normalized_symbol),
                    "event": {
                        "timestamp_ms": safe_int(data.get("E")) or safe_int(order.get("T")) or now_ms,
                        "side": normalize_liquidation_side(order.get("S")),
                        "price": price,
                        "size": size,
                        "notional": compute_notional(price, size),
                        "source": "ws",
                        "raw": order,
                    },
                }
            if "@aggtrade" in stream:
                price = safe_float(data.get("p"))
                size = safe_float(data.get("q"))
                return {
                    "kind": "trade_event",
                    "exchange_key": exchange_key,
                    "spot": False,
                    "symbol": str(data.get("s") or normalized_symbol),
                    "event": {
                        "timestamp_ms": safe_int(data.get("E")) or safe_int(data.get("T")) or now_ms,
                        "side": "sell" if bool(data.get("m")) else "buy",
                        "price": price,
                        "size": size,
                        "notional": compute_notional(price, size),
                        "source": "ws",
                        "raw": data,
                    },
                }
            if "@depth@" in stream:
                event = {
                    "U": safe_int(data.get("U")),
                    "u": safe_int(data.get("u")),
                    "pu": safe_int(data.get("pu")),
                    "bids": [(safe_float(price), safe_float(size)) for price, size in data.get("b", [])],
                    "asks": [(safe_float(price), safe_float(size)) for price, size in data.get("a", [])],
                }
                if event["U"] is None or event["u"] is None:
                    return None
                return {
                    "kind": "binance_depth_event",
                    "exchange_key": exchange_key,
                    "spot": False,
                    "symbol": normalized_symbol,
                    "event": event,
                }
            return None

        if spot and exchange_key == "binance":
            stream = str(payload.get("stream") or "").lower()
            data = payload.get("data") or {}
            if not isinstance(data, dict):
                return None
            if "@aggtrade" in stream:
                price = safe_float(data.get("p"))
                size = safe_float(data.get("q"))
                return {
                    "kind": "trade_event",
                    "exchange_key": exchange_key,
                    "spot": True,
                    "symbol": str(data.get("s") or normalized_symbol),
                    "event": {
                        "timestamp_ms": safe_int(data.get("E")) or safe_int(data.get("T")) or now_ms,
                        "side": "sell" if bool(data.get("m")) else "buy",
                        "price": price,
                        "size": size,
                        "notional": compute_notional(price, size),
                        "source": "ws",
                        "raw": data,
                    },
                }
            if "@depth@" in stream:
                event = {
                    "U": safe_int(data.get("U")),
                    "u": safe_int(data.get("u")),
                    "bids": [(safe_float(price), safe_float(size)) for price, size in data.get("b", [])],
                    "asks": [(safe_float(price), safe_float(size)) for price, size in data.get("a", [])],
                }
                if event["U"] is None or event["u"] is None:
                    return None
                return {
                    "kind": "binance_depth_event",
                    "exchange_key": exchange_key,
                    "spot": True,
                    "symbol": normalized_symbol,
                    "event": event,
                }
            return None

        if exchange_key == "bybit":
            topic = str(payload.get("topic") or "")
            if payload.get("success") is not None:
                return None
            if topic.startswith("allLiquidation") and not spot:
                items = payload.get("data") or []
                if isinstance(items, dict):
                    items = [items]
                events = []
                for item in items:
                    if not isinstance(item, dict):
                        continue
                    price = safe_float(item.get("price")) or safe_float(item.get("p"))
                    size = safe_float(item.get("size")) or safe_float(item.get("v"))
                    events.append(
                        {
                            "timestamp_ms": safe_int(item.get("updatedTime")) or safe_int(item.get("T")) or now_ms,
                            "side": normalize_liquidation_side_for_exchange("bybit", item.get("side") or item.get("S")),
                            "price": price,
                            "size": size,
                            "notional": compute_notional(price, size),
                            "source": "ws",
                            "raw": item,
                        }
                    )
                if events:
                    return {
                        "kind": "liquidation_batch",
                        "exchange_key": exchange_key,
                        "spot": False,
                        "symbol": normalized_symbol,
                        "events": events,
                    }
                return None
            if topic.startswith("publicTrade"):
                items = payload.get("data") or []
                if isinstance(items, dict):
                    items = [items]
                events = []
                for item in items:
                    if not isinstance(item, dict):
                        continue
                    price = safe_float(item.get("p")) or safe_float(item.get("price"))
                    size = safe_float(item.get("v")) or safe_float(item.get("size"))
                    events.append(
                        {
                            "symbol": str(item.get("s") or normalized_symbol),
                            "timestamp_ms": safe_int(item.get("T")) or safe_int(item.get("time")) or now_ms,
                            "side": normalize_trade_side(item.get("S") or item.get("side")),
                            "price": price,
                            "size": size,
                            "notional": compute_notional(price, size),
                            "source": "ws",
                            "raw": item,
                        }
                    )
                if events:
                    return {
                        "kind": "trade_batch",
                        "exchange_key": exchange_key,
                        "spot": bool(spot),
                        "symbol": normalized_symbol,
                        "events": events,
                    }
                return None
            if topic.startswith("orderbook"):
                data = payload.get("data") or {}
                if not isinstance(data, dict):
                    return None
                return {
                    "kind": "orderbook_update",
                    "exchange_key": exchange_key,
                    "spot": bool(spot),
                    "symbol": normalized_symbol,
                    "mode": "snapshot" if payload.get("type") == "snapshot" else "delta",
                    "bids": [(safe_float(price), safe_float(size)) for price, size in data.get("b", [])],
                    "asks": [(safe_float(price), safe_float(size)) for price, size in data.get("a", [])],
                }
            return None

        if exchange_key == "okx":
            if payload.get("event"):
                return None
            arg = payload.get("arg") or {}
            if not isinstance(arg, dict):
                return None
            channel = str(arg.get("channel") or "")
            data_list = payload.get("data") or []
            if channel == "books5":
                data = data_list[0] if isinstance(data_list, list) and data_list else {}
                if not isinstance(data, dict):
                    return None
                return {
                    "kind": "orderbook_update",
                    "exchange_key": exchange_key,
                    "spot": bool(spot),
                    "symbol": normalized_symbol,
                    "mode": "snapshot",
                    "bids": [(safe_float(row[0]), safe_float(row[1])) for row in data.get("bids", [])],
                    "asks": [(safe_float(row[0]), safe_float(row[1])) for row in data.get("asks", [])],
                }
            if channel == "trades":
                events = []
                for item in data_list if isinstance(data_list, list) else []:
                    if not isinstance(item, dict):
                        continue
                    price = safe_float(item.get("px"))
                    size = safe_float(item.get("sz"))
                    events.append(
                        {
                            "symbol": str(item.get("instId") or normalized_symbol),
                            "timestamp_ms": safe_int(item.get("ts")) or now_ms,
                            "side": normalize_trade_side(item.get("side")),
                            "price": price,
                            "size": size,
                            "notional": compute_notional(price, size),
                            "source": "ws",
                            "raw": item,
                        }
                    )
                if events:
                    return {
                        "kind": "trade_batch",
                        "exchange_key": exchange_key,
                        "spot": bool(spot),
                        "symbol": normalized_symbol,
                        "events": events,
                    }
            if channel == "liquidation-orders" and not spot:
                events = []
                for item in _extract_okx_liquidation_rows(data_list):
                    price = safe_float(item.get("bkPx")) or safe_float(item.get("px")) or safe_float(item.get("price"))
                    size = safe_float(item.get("sz")) or safe_float(item.get("size"))
                    event_symbol = str(item.get("instId") or arg.get("instId") or normalized_symbol)
                    events.append(
                        {
                            "symbol": event_symbol,
                            "timestamp_ms": safe_int(item.get("ts")) or now_ms,
                            "side": normalize_liquidation_side(item.get("posSide") or item.get("side")),
                            "price": price,
                            "size": size,
                            "notional": compute_notional(price, size),
                            "source": "ws",
                            "raw": item,
                        }
                    )
                if events:
                    return {
                        "kind": "liquidation_batch",
                        "exchange_key": exchange_key,
                        "spot": False,
                        "symbol": normalized_symbol,
                        "events": events,
                    }
            return None

        if exchange_key == "hyperliquid" and not spot:
            channel = str(payload.get("channel") or "")
            if channel == "l2Book":
                data = payload.get("data") or {}
                if not isinstance(data, dict):
                    return None
                levels = data.get("levels", [[], []])
                return {
                    "kind": "orderbook_update",
                    "exchange_key": exchange_key,
                    "spot": False,
                    "symbol": normalized_symbol,
                    "mode": "snapshot",
                    "bids": [(safe_float(item.get("px")), safe_float(item.get("sz"))) for item in levels[0]],
                    "asks": [(safe_float(item.get("px")), safe_float(item.get("sz"))) for item in levels[1]],
                }
            if channel == "trades":
                items = payload.get("data") or []
                events = []
                for item in items if isinstance(items, list) else []:
                    if not isinstance(item, dict):
                        continue
                    price = safe_float(item.get("px"))
                    size = safe_float(item.get("sz"))
                    events.append(
                        {
                            "symbol": str(item.get("coin") or normalized_symbol),
                            "timestamp_ms": safe_int(item.get("time")) or now_ms,
                            "side": normalize_trade_side(item.get("side")),
                            "price": price,
                            "size": size,
                            "notional": compute_notional(price, size),
                            "source": "ws",
                            "raw": item,
                        }
                    )
                if events:
                    return {
                        "kind": "trade_batch",
                        "exchange_key": exchange_key,
                        "spot": False,
                        "symbol": normalized_symbol,
                        "events": events,
                    }
            return None

        if exchange_key == "bitget" and not spot:
            arg = payload.get("arg") or {}
            if not isinstance(arg, dict):
                return None
            topic = str(arg.get("topic") or "").strip().lower()
            data_list = payload.get("data") or []
            if topic in {"books50", "books"}:
                data = data_list[0] if isinstance(data_list, list) and data_list else {}
                if not isinstance(data, dict):
                    return None
                return {
                    "kind": "orderbook_update",
                    "exchange_key": exchange_key,
                    "spot": False,
                    "symbol": normalized_symbol,
                    "mode": "snapshot",
                    "bids": [(safe_float(item[0]), safe_float(item[1])) for item in data.get("bids", []) if isinstance(item, (list, tuple)) and len(item) >= 2],
                    "asks": [(safe_float(item[0]), safe_float(item[1])) for item in data.get("asks", []) if isinstance(item, (list, tuple)) and len(item) >= 2],
                }
            if topic in {"publictrade", "trade", "trades"}:
                events = []
                for item in data_list if isinstance(data_list, list) else []:
                    if not isinstance(item, dict):
                        continue
                    event_symbol = str(item.get("symbol") or arg.get("symbol") or normalized_symbol).strip().upper()
                    if event_symbol != normalized_symbol.upper():
                        continue
                    price = safe_float(item.get("price"))
                    size = safe_float(item.get("size"))
                    events.append(
                        {
                            "symbol": event_symbol,
                            "timestamp_ms": safe_int(item.get("ts")) or safe_int(item.get("timestamp")) or now_ms,
                            "side": normalize_trade_side(item.get("side")),
                            "price": price,
                            "size": size,
                            "notional": compute_notional(price, size),
                            "source": "ws",
                            "raw": item,
                        }
                    )
                if events:
                    return {
                        "kind": "trade_batch",
                        "exchange_key": exchange_key,
                        "spot": False,
                        "symbol": normalized_symbol,
                        "events": events,
                    }
                return None
            if topic == "liquidation":
                events = []
                for item in data_list if isinstance(data_list, list) else []:
                    if not isinstance(item, dict):
                        continue
                    event_symbol = str(item.get("symbol") or item.get("instId") or "").strip().upper()
                    if event_symbol != normalized_symbol.upper():
                        continue
                    price = safe_float(item.get("price"))
                    notional = safe_float(item.get("amount")) or safe_float(item.get("size")) or safe_float(item.get("notional"))
                    size = safe_float(item.get("sz")) or safe_float(item.get("size"))
                    if size is None and price not in (None, 0) and notional is not None:
                        size = float(notional) / float(price)
                    events.append(
                        {
                            "symbol": event_symbol,
                            "timestamp_ms": safe_int(item.get("ts")) or safe_int(item.get("timestamp")) or now_ms,
                            "side": normalize_liquidation_side_for_exchange("bitget", item.get("side") or item.get("holdSide") or item.get("positionSide")),
                            "price": price,
                            "size": size,
                            "notional": notional or compute_notional(price, size),
                            "source": "ws",
                            "raw": item,
                        }
                    )
                if events:
                    return {
                        "kind": "liquidation_batch",
                        "exchange_key": exchange_key,
                        "spot": False,
                        "symbol": normalized_symbol,
                        "events": events,
                    }
                return None

        return None

    def _dispatch_payload(self, exchange_key: str, payload: Dict[str, object], *, spot: bool = False) -> None:
        with self.lock:
            targets = list(self._collect_targets_locked(exchange_key, spot=spot))
        matched = self._matching_targets(exchange_key, payload, targets)
        if not matched:
            return

        grouped_targets: Dict[str, List["LiveTerminalService"]] = {}
        for service, symbol in matched:
            normalized_symbol = str(symbol or "").strip()
            if not normalized_symbol:
                continue
            grouped_targets.setdefault(normalized_symbol, []).append(service)

        for symbol, services in grouped_targets.items():
            structured_event = self._build_structured_event(exchange_key, payload, symbol, spot=spot)
            for service in services:
                try:
                    if structured_event is not None:
                        service._apply_shared_structured_event(structured_event)
                    elif spot:
                        service._on_spot_message_payload(exchange_key, symbol, payload)
                    else:
                        service._on_message_payload(exchange_key, symbol, payload)
                except Exception:
                    continue

    def _dispatch_message(self, exchange_key: str, raw_message: str, *, spot: bool = False) -> None:
        try:
            payload = json.loads(raw_message)
        except json.JSONDecodeError:
            return
        self._dispatch_payload(exchange_key, payload, spot=spot)

    def _run_perp_worker(self, exchange_key: str) -> None:
        worker_key = exchange_key
        while not self.stop_event.is_set():
            with self.lock:
                targets = self._collect_targets_locked(exchange_key, spot=False)
            symbols = sorted({symbol for _, symbol in targets})
            if not symbols:
                if self.stop_event.wait(2):
                    return
                continue
            ws_url = self._build_ws_url(exchange_key, symbols, spot=False)
            app = websocket.WebSocketApp(
                ws_url,
                on_open=lambda ws, key=exchange_key, current=list(symbols): self._send_subscriptions(key, ws, current, spot=False),
                on_message=lambda ws, message, key=exchange_key: self._dispatch_message(key, message, spot=False),
            )
            with self.lock:
                self.ws_apps[worker_key] = app
            try:
                app.run_forever(ping_interval=20, ping_timeout=10)
            except Exception:
                pass
            with self.lock:
                current = self.ws_apps.get(worker_key)
                if current is app:
                    self.ws_apps.pop(worker_key, None)
            if self.stop_event.wait(2):
                return

    def _run_sampler(self) -> None:
        while not self.stop_event.is_set():
            try:
                self._sample_once_shared()
            except Exception:
                pass
            self.sample_wakeup.clear()
            if self.sample_wakeup.wait(self.sample_seconds):
                self.sample_wakeup.clear()
                if self.stop_event.is_set():
                    return

    def _sample_spot_snapshot_once(self, exchange_key: str, symbol: str) -> SpotSnapshot:
        client = self.spot_clients.get(exchange_key)
        if client is None:
            return SpotSnapshot(exchange=str(exchange_key or "").upper(), symbol=symbol, status="error", error="missing client")
        try:
            return client.fetch(symbol)
        except Exception as exc:
            return SpotSnapshot(
                exchange=client.exchange_name,
                symbol=symbol,
                status="error",
                error=str(exc),
            )

    def _sample_perp_snapshot_once(self, exchange_key: str, symbol: str) -> ExchangeSnapshot:
        client = self.clients.get(exchange_key)
        if client is None:
            return ExchangeSnapshot(exchange=str(exchange_key or "").upper(), symbol=symbol, status="error", error="missing client")
        try:
            return client.fetch(symbol)
        except Exception as exc:
            return ExchangeSnapshot(
                exchange=client.exchange_name,
                symbol=symbol,
                status="error",
                error=str(exc),
            )

    def _sample_once_shared(self) -> None:
        with self.lock:
            perp_targets, spot_targets = self._collect_sample_targets_locked()
        fetch_specs: List[Tuple[str, str, str, List["LiveTerminalService"]]] = []
        for (exchange_key, symbol), services in spot_targets.items():
            fetch_specs.append(("spot", exchange_key, symbol, services))
        for (exchange_key, symbol), services in perp_targets.items():
            fetch_specs.append(("perp", exchange_key, symbol, services))
        if not fetch_specs:
            return
        max_workers = min(max(len(fetch_specs), 1), 8)
        with ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="shared-sampler") as executor:
            future_map = {}
            for market, exchange_key, symbol, services in fetch_specs:
                if market == "spot":
                    future = executor.submit(self._sample_spot_snapshot_once, exchange_key, symbol)
                else:
                    future = executor.submit(self._sample_perp_snapshot_once, exchange_key, symbol)
                future_map[future] = (market, exchange_key, services)
            for future in as_completed(future_map):
                market, exchange_key, services = future_map[future]
                try:
                    snapshot = future.result()
                except Exception as exc:
                    if market == "spot":
                        client = self.spot_clients.get(exchange_key)
                        snapshot = SpotSnapshot(
                            exchange=client.exchange_name if client is not None else str(exchange_key or "").upper(),
                            symbol="",
                            status="error",
                            error=str(exc),
                        )
                    else:
                        client = self.clients.get(exchange_key)
                        snapshot = ExchangeSnapshot(
                            exchange=client.exchange_name if client is not None else str(exchange_key or "").upper(),
                            symbol="",
                            status="error",
                            error=str(exc),
                        )
                for service in services:
                    try:
                        if market == "spot":
                            service._apply_shared_spot_snapshot(exchange_key, snapshot)
                        else:
                            service._apply_shared_sampled_snapshot(exchange_key, snapshot)
                    except Exception:
                        continue

    def _run_spot_worker(self, exchange_key: str) -> None:
        worker_key = f"spot::{exchange_key}"
        while not self.stop_event.is_set():
            with self.lock:
                targets = self._collect_targets_locked(exchange_key, spot=True)
            symbols = sorted({symbol for _, symbol in targets})
            if not symbols:
                if self.stop_event.wait(2):
                    return
                continue
            ws_url = self._build_ws_url(exchange_key, symbols, spot=True)
            app = websocket.WebSocketApp(
                ws_url,
                on_open=lambda ws, key=exchange_key, current=list(symbols): self._send_subscriptions(key, ws, current, spot=True),
                on_message=lambda ws, message, key=exchange_key: self._dispatch_message(key, message, spot=True),
            )
            with self.lock:
                self.ws_apps[worker_key] = app
            try:
                app.run_forever(ping_interval=20, ping_timeout=10)
            except Exception:
                pass
            with self.lock:
                current = self.ws_apps.get(worker_key)
                if current is app:
                    self.ws_apps.pop(worker_key, None)
            if self.stop_event.wait(2):
                return


class LiveTerminalService:
    def __init__(
        self,
        symbol_map: Dict[str, str],
        timeout: int = 10,
        sample_seconds: int = 15,
        history_size: int = 720,
        liquidation_history_size: int = 600,
        trade_history_size: int = 1200,
        record_history_size: int = 2400,
        orderbook_limit: int = 80,
        spot_symbol: str = "BTCUSDT",
        spot_symbol_map: Dict[str, str] | None = None,
        shared_hub: SharedRealtimeHub | None = None,
        source_update_callback: Optional[Callable[[str, str, str], None]] = None,
        autostart: bool = True,
    ) -> None:
        self.symbol_map = dict(symbol_map)
        self.spot_symbol_map = dict(spot_symbol_map or {"binance": spot_symbol})
        self.shared_hub = shared_hub
        self.source_update_callback = source_update_callback
        self.timeout = timeout
        self.sample_seconds = max(sample_seconds, 5)
        self.history_size = max(history_size, 120)
        self.liquidation_history_size = max(liquidation_history_size, 120)
        self.trade_history_size = max(trade_history_size, 240)
        self.record_history_size = max(record_history_size, 480)
        self.orderbook_limit = max(orderbook_limit, 20)
        if self.shared_hub is not None:
            self.clients = self.shared_hub.clients
            self.spot_clients = self.shared_hub.spot_clients
        else:
            self.clients = build_clients(timeout=timeout)
            self.spot_clients = build_spot_clients(timeout=timeout)
        self.liquidation_archive = LocalLiquidationArchive()
        self.lock = threading.Lock()
        self.stop_event = threading.Event()
        self.live_by_exchange: Dict[str, ExchangeSnapshot] = {}
        self.sampled_by_exchange: Dict[str, ExchangeSnapshot] = {}
        self.oi_history: Dict[str, Deque[OIPoint]] = {
            key: deque(maxlen=self.history_size) for key in PERP_EXCHANGE_ORDER
        }
        self.liquidation_history: Dict[str, Deque[LiquidationEvent]] = {
            key: deque(maxlen=self.liquidation_history_size) for key in PERP_EXCHANGE_ORDER
        }
        self.trade_history: Dict[str, Deque[TradeEvent]] = {
            key: deque(maxlen=self.trade_history_size) for key in PERP_EXCHANGE_ORDER
        }
        self.orderbook_quality_history: Dict[str, Deque[OrderBookQualityPoint]] = {
            key: deque(maxlen=self.history_size) for key in PERP_EXCHANGE_ORDER
        }
        self.recorded_events: Dict[str, Deque[RecordedMarketEvent]] = {
            key: deque(maxlen=self.record_history_size) for key in PERP_EXCHANGE_ORDER
        }
        self.orderbooks: Dict[str, List[OrderBookLevel]] = {key: [] for key in PERP_EXCHANGE_ORDER}
        self.orderbook_state: Dict[str, Dict[str, Dict[float, float]]] = {
            key: {"bid": {}, "ask": {}} for key in PERP_EXCHANGE_ORDER
        }
        self.orderbook_quality_state: Dict[str, Dict[str, object]] = {
            key: self._initial_book_quality_state() for key in PERP_EXCHANGE_ORDER
        }
        self.binance_depth_buffer: Deque[Dict[str, object]] = deque(maxlen=400)
        self.binance_depth_last_u: int | None = None
        self.binance_depth_synced = False
        self.binance_depth_bootstrapping = False
        self.binance_depth_proxy_active = False
        self.binance_depth_bootstrap_retry_at_ms = 0
        self.binance_depth_last_bootstrap_error: str | None = None
        self.binance_depth_last_bootstrap_attempt_ms: int | None = None
        self.ws_apps: Dict[str, websocket.WebSocketApp] = {}
        self.threads: List[threading.Thread] = []
        self.spot_snapshots: Dict[str, SpotSnapshot | None] = {
            key: None for key in SPOT_EXCHANGE_ORDER
        }
        self.spot_orderbooks: Dict[str, List[OrderBookLevel]] = {
            key: [] for key in SPOT_EXCHANGE_ORDER
        }
        self.spot_trade_histories: Dict[str, Deque[TradeEvent]] = {
            key: deque(maxlen=self.trade_history_size) for key in SPOT_EXCHANGE_ORDER
        }
        self.spot_orderbook_quality_history: Dict[str, Deque[OrderBookQualityPoint]] = {
            key: deque(maxlen=self.history_size) for key in SPOT_EXCHANGE_ORDER
        }
        self.spot_recorded_events: Dict[str, Deque[RecordedMarketEvent]] = {
            key: deque(maxlen=self.record_history_size) for key in SPOT_EXCHANGE_ORDER
        }
        self.spot_orderbook_state: Dict[str, Dict[str, Dict[float, float]]] = {
            key: {"bid": {}, "ask": {}} for key in SPOT_EXCHANGE_ORDER
        }
        self.spot_orderbook_quality_state: Dict[str, Dict[str, object]] = {
            key: self._initial_book_quality_state() for key in SPOT_EXCHANGE_ORDER
        }
        self.binance_spot_depth_buffer: Deque[Dict[str, object]] = deque(maxlen=400)
        self.binance_spot_depth_last_u: int | None = None
        self.binance_spot_depth_synced = False
        self.binance_spot_depth_bootstrapping = False
        self._start_lock = threading.Lock()
        self._starting = False
        self._started = False

        if autostart:
            self.ensure_started(background=False)

    def _emit_source_update(self, exchange_key: str, market: str, channel: str) -> None:
        callback = self.source_update_callback
        if callback is None:
            return
        try:
            callback(str(exchange_key or "").lower().strip(), "spot" if market == "spot" else "perp", str(channel or "").lower().strip())
        except Exception:
            pass

    def ensure_started(self, *, background: bool = True) -> None:
        with self._start_lock:
            if self._started or self._starting or self.stop_event.is_set():
                return
            self._starting = True
        if background:
            thread = threading.Thread(target=self._bootstrap_runtime, name="runtime-bootstrap", daemon=True)
            thread.start()
            return
        self._bootstrap_runtime()

    def _bootstrap_runtime(self) -> None:
        success = False
        try:
            if self.stop_event.is_set():
                return
            if self.shared_hub is None:
                self._sample_once()
            if self.stop_event.is_set():
                return
            self._start_threads()
            success = True
        finally:
            with self._start_lock:
                self._starting = False
                if success:
                    self._started = True

    def _start_threads(self) -> None:
        if self.shared_hub is not None:
            self.shared_hub.register_service(self)
            return

        sampler = threading.Thread(target=self._run_sampler, name="oi-sampler", daemon=True)
        sampler.start()
        self.threads.append(sampler)

        for exchange_key in PERP_EXCHANGE_ORDER:
            if not self.symbol_map.get(exchange_key):
                continue
            worker = threading.Thread(
                target=self._run_ws_worker,
                args=(exchange_key,),
                name=f"ws-{exchange_key}",
                daemon=True,
            )
            worker.start()
            self.threads.append(worker)

        for exchange_key in SPOT_EXCHANGE_ORDER:
            if not self.spot_symbol_map.get(exchange_key):
                continue
            spot_worker = threading.Thread(
                target=self._run_spot_ws_worker,
                args=(exchange_key,),
                name=f"ws-spot-{exchange_key}",
                daemon=True,
            )
            spot_worker.start()
            self.threads.append(spot_worker)

    def stop(self) -> None:
        self.stop_event.set()
        if self.shared_hub is not None:
            try:
                self.shared_hub.unregister_service(self)
            except Exception:
                pass
        for ws in list(self.ws_apps.values()):
            try:
                ws.close()
            except Exception:
                pass

    def current_snapshots(self) -> List[ExchangeSnapshot]:
        snapshots: List[ExchangeSnapshot] = []
        with self.lock:
            for exchange_key in PERP_EXCHANGE_ORDER:
                live = self.live_by_exchange.get(exchange_key)
                sampled = self.sampled_by_exchange.get(exchange_key)
                exchange_name = self.clients[exchange_key].exchange_name
                symbol = self.symbol_map.get(exchange_key, "")

                if not symbol:
                    snapshots.append(
                        ExchangeSnapshot(
                            exchange=exchange_name,
                            symbol="",
                            status="error",
                            error="未上架此币",
                        )
                    )
                    continue

                if sampled is not None and (sampled.status == "ok" or live is None):
                    merged = replace(sampled)
                elif live is not None:
                    merged = replace(live)
                else:
                    merged = ExchangeSnapshot(
                        exchange=exchange_name,
                        symbol=symbol,
                        status="error",
                        error="waiting for data",
                    )

                if live is not None:
                    for field_name in (
                        "last_price",
                        "mark_price",
                        "index_price",
                        "open_interest",
                        "open_interest_notional",
                        "funding_rate",
                        "volume_24h_base",
                        "volume_24h_notional",
                        "timestamp_ms",
                    ):
                        live_value = getattr(live, field_name)
                        if live_value is not None:
                            setattr(merged, field_name, live_value)
                    if live.status == "ok":
                        merged.status = "ok"
                        merged.error = None

                snapshots.append(merged)
        return snapshots

    def get_oi_history(self, exchange_key: str) -> List[OIPoint]:
        with self.lock:
            return list(self.oi_history.get(exchange_key, []))

    def _expected_symbol(self, exchange_key: str, *, spot: bool = False) -> str:
        mapping = self.spot_symbol_map if spot else self.symbol_map
        return _normalize_symbol_token(mapping.get(exchange_key, ""))

    def _matches_expected_symbol(self, exchange_key: str, symbol: Optional[object], *, spot: bool = False) -> bool:
        expected = self._expected_symbol(exchange_key, spot=spot)
        actual = _normalize_symbol_token(symbol)
        if not expected or not actual:
            return True
        return actual == expected

    def get_liquidation_history(self, exchange_key: str) -> List[LiquidationEvent]:
        with self.lock:
            return [
                event
                for event in self.liquidation_history.get(exchange_key, [])
                if self._matches_expected_symbol(exchange_key, getattr(event, "symbol", None))
            ]

    def get_persisted_liquidations(
        self,
        exchange_key: str,
        symbol: str,
        *,
        since_ms: Optional[int] = None,
        limit: int = 4000,
    ) -> List[LiquidationEvent]:
        with self.lock:
            return self.liquidation_archive.load(exchange_key, symbol, since_ms=since_ms, limit=limit)

    def get_persisted_liquidation_summary(
        self,
        exchange_key: str,
        symbol: str,
        *,
        since_ms: Optional[int] = None,
        limit: int = 4000,
    ) -> Dict[str, Any]:
        with self.lock:
            return self.liquidation_archive.describe(exchange_key, symbol, since_ms=since_ms, limit=limit)

    def get_trade_history(self, exchange_key: str) -> List[TradeEvent]:
        with self.lock:
            return [
                event
                for event in self.trade_history.get(exchange_key, [])
                if self._matches_expected_symbol(exchange_key, getattr(event, "symbol", None))
            ]

    def get_orderbook_quality_history(self, exchange_key: str) -> List[OrderBookQualityPoint]:
        with self.lock:
            return list(self.orderbook_quality_history.get(exchange_key, []))

    def get_recorded_events(self, exchange_key: str) -> List[RecordedMarketEvent]:
        with self.lock:
            return [
                event
                for event in self.recorded_events.get(exchange_key, [])
                if self._matches_expected_symbol(exchange_key, getattr(event, "symbol", None))
            ]

    def get_orderbook(self, exchange_key: str) -> List[OrderBookLevel]:
        with self.lock:
            return list(self.orderbooks.get(exchange_key, []))

    def ensure_orderbook_limit(self, limit: int) -> None:
        target_limit = max(int(limit), 20)
        with self.lock:
            if target_limit == self.orderbook_limit:
                return
            self.orderbook_limit = target_limit
            for exchange_key in PERP_EXCHANGE_ORDER:
                self._refresh_orderbook_levels_locked(exchange_key)
            for exchange_key in SPOT_EXCHANGE_ORDER:
                self._refresh_spot_orderbook_locked(exchange_key)

    def get_spot_snapshot(self, exchange_key: str = "binance") -> SpotSnapshot | None:
        with self.lock:
            snapshot = self.spot_snapshots.get(exchange_key)
            return replace(snapshot) if snapshot is not None else None

    def get_spot_orderbook(self, exchange_key: str = "binance") -> List[OrderBookLevel]:
        with self.lock:
            return list(self.spot_orderbooks.get(exchange_key, []))

    def get_spot_trade_history(self, exchange_key: str = "binance") -> List[TradeEvent]:
        with self.lock:
            return [
                event
                for event in self.spot_trade_histories.get(exchange_key, [])
                if self._matches_expected_symbol(exchange_key, getattr(event, "symbol", None), spot=True)
            ]

    def get_spot_orderbook_quality_history(self, exchange_key: str = "binance") -> List[OrderBookQualityPoint]:
        with self.lock:
            return list(self.spot_orderbook_quality_history.get(exchange_key, []))

    def get_spot_recorded_events(self, exchange_key: str = "binance") -> List[RecordedMarketEvent]:
        with self.lock:
            return [
                event
                for event in self.spot_recorded_events.get(exchange_key, [])
                if self._matches_expected_symbol(exchange_key, getattr(event, "symbol", None), spot=True)
            ]

    def get_transport_health(self, exchange_key: str, *, spot: bool = False) -> Dict[str, object]:
        with self.lock:
            if spot:
                if not self.spot_symbol_map.get(exchange_key):
                    return {
                        "snapshot_timestamp_ms": None,
                        "trade_timestamp_ms": None,
                        "orderbook_levels": 0,
                        "sync_state": "unsupported",
                    }
                snapshot = self.spot_snapshots.get(exchange_key)
                orderbook = self.spot_orderbooks.get(exchange_key, [])
                trades = self.spot_trade_histories.get(exchange_key, [])
                if exchange_key == "binance":
                    sync_state = (
                        "synced"
                        if self.binance_spot_depth_synced
                        else "bootstrapping"
                        if self.binance_spot_depth_bootstrapping
                        else "degraded"
                    )
                else:
                    sync_state = "synced" if orderbook else "waiting"
                return {
                    "snapshot_timestamp_ms": snapshot.timestamp_ms if snapshot is not None else None,
                    "trade_timestamp_ms": max((event.timestamp_ms for event in trades), default=None),
                    "orderbook_levels": len(orderbook),
                    "sync_state": sync_state,
                }

            live = self.live_by_exchange.get(exchange_key)
            sampled = self.sampled_by_exchange.get(exchange_key)
            orderbook = self.orderbooks.get(exchange_key, [])
            trades = self.trade_history.get(exchange_key, [])
            if not self.symbol_map.get(exchange_key):
                return {
                    "snapshot_timestamp_ms": None,
                    "live_timestamp_ms": None,
                    "sample_timestamp_ms": None,
                    "trade_timestamp_ms": None,
                    "orderbook_levels": 0,
                    "sync_state": "unsupported",
                }
            if exchange_key == "binance":
                sync_state = (
                    "synced"
                    if self.binance_depth_synced
                    else "proxy"
                    if self.binance_depth_proxy_active and orderbook
                    else "bootstrapping"
                    if self.binance_depth_bootstrapping
                    else "degraded"
                )
            else:
                sync_state = "synced" if orderbook else "waiting"
            snapshot_timestamp_ms = None
            if live is not None and live.timestamp_ms is not None:
                snapshot_timestamp_ms = live.timestamp_ms
            elif sampled is not None:
                snapshot_timestamp_ms = sampled.timestamp_ms
            return {
                "snapshot_timestamp_ms": snapshot_timestamp_ms,
                "live_timestamp_ms": live.timestamp_ms if live is not None else None,
                "sample_timestamp_ms": sampled.timestamp_ms if sampled is not None else None,
                "trade_timestamp_ms": max((event.timestamp_ms for event in trades), default=None),
                "orderbook_levels": len(orderbook),
                "sync_state": sync_state,
                "bootstrap_retry_at_ms": (self.binance_depth_bootstrap_retry_at_ms or None) if exchange_key == "binance" else None,
                "bootstrap_error": self.binance_depth_last_bootstrap_error if exchange_key == "binance" else None,
            }

    @staticmethod
    def _initial_book_quality_state() -> Dict[str, object]:
        return {
            "wall_registry": {"bid": {}, "ask": {}},
            "refill_watch": {"bid": None, "ask": None},
        }

    @staticmethod
    def _bootstrap_retry_ms_from_error(error: Exception) -> int:
        message = str(error or "")
        response = getattr(error, "response", None)
        if response is not None:
            try:
                response_text = response.text or ""
            except Exception:
                response_text = ""
            if response_text:
                message = f"{message} {response_text}".strip()
        now_ms = int(time.time() * 1000)
        banned_match = re.search(r"banned until (\d+)", message, flags=re.IGNORECASE)
        if banned_match:
            try:
                banned_until_ms = int(banned_match.group(1))
                return max(5_000, banned_until_ms - now_ms)
            except (TypeError, ValueError):
                pass
        cooldown_match = re.search(r"(\d+)s 后自动重试", message)
        if cooldown_match:
            try:
                return max(5_000, int(cooldown_match.group(1)) * 1000)
            except (TypeError, ValueError):
                pass
        if "418" in message or "429" in message or "too many requests" in message.lower():
            return 60_000
        return 12_000

    def _apply_binance_depth_proxy_locked(self, event: Dict[str, object], *, reset: bool = False) -> None:
        if reset or not self.orderbooks["binance"]:
            self._replace_orderbook_locked("binance", event["bids"], event["asks"])
        elif self.binance_depth_last_u is not None and int(event["U"]) > self.binance_depth_last_u + 1:
            self._replace_orderbook_locked("binance", event["bids"], event["asks"])
        elif self.binance_depth_last_u is None or int(event["u"]) > self.binance_depth_last_u:
            self._apply_orderbook_delta_locked("binance", event["bids"], event["asks"])
        self.binance_depth_last_u = int(event["u"])
        self.binance_depth_proxy_active = bool(self.orderbooks["binance"])

    def _run_sampler(self) -> None:
        while not self.stop_event.is_set():
            self._sample_once()
            if self.stop_event.wait(self.sample_seconds):
                return

    def _sample_once(self) -> None:
        for exchange_key in SPOT_EXCHANGE_ORDER:
            symbol = self.spot_symbol_map.get(exchange_key)
            if not symbol:
                continue
            try:
                spot_snapshot = self.spot_clients[exchange_key].fetch(symbol)
            except Exception as exc:
                spot_snapshot = SpotSnapshot(
                    exchange=self.spot_clients[exchange_key].exchange_name,
                    symbol=symbol,
                    status="error",
                    error=str(exc),
                )
            with self.lock:
                self._apply_spot_snapshot_locked(exchange_key, spot_snapshot)

        for exchange_key in PERP_EXCHANGE_ORDER:
            symbol = self.symbol_map.get(exchange_key)
            if not symbol:
                continue
            try:
                snapshot = self.clients[exchange_key].fetch(symbol)
            except Exception as exc:
                snapshot = ExchangeSnapshot(
                    exchange=self.clients[exchange_key].exchange_name,
                    symbol=symbol,
                    status="error",
                    error=str(exc),
                )

            with self.lock:
                self._apply_sampled_snapshot_locked(exchange_key, snapshot)

    def _apply_shared_spot_snapshot(self, exchange_key: str, snapshot: SpotSnapshot) -> None:
        with self.lock:
            self._apply_spot_snapshot_locked(exchange_key, snapshot)

    def _apply_shared_sampled_snapshot(self, exchange_key: str, snapshot: ExchangeSnapshot) -> None:
        with self.lock:
            self._apply_sampled_snapshot_locked(exchange_key, snapshot)

    def _apply_spot_snapshot_locked(self, exchange_key: str, spot_snapshot: SpotSnapshot) -> None:
        previous_spot_snapshot = self.spot_snapshots.get(exchange_key)
        if (
            spot_snapshot.status == "error"
            and previous_spot_snapshot is not None
            and (
                previous_spot_snapshot.last_price is not None
                or previous_spot_snapshot.bid_price is not None
                or previous_spot_snapshot.ask_price is not None
                or previous_spot_snapshot.volume_24h_notional is not None
            )
        ):
            preserved_snapshot = replace(previous_spot_snapshot)
            preserved_snapshot.status = previous_spot_snapshot.status or "ok"
            preserved_snapshot.error = spot_snapshot.error
            self.spot_snapshots[exchange_key] = preserved_snapshot
        else:
            self.spot_snapshots[exchange_key] = replace(spot_snapshot)
        if self.spot_orderbooks.get(exchange_key):
            self._sync_spot_best_prices_locked(exchange_key)

    def _apply_sampled_snapshot_locked(self, exchange_key: str, snapshot: ExchangeSnapshot) -> None:
        self.sampled_by_exchange[exchange_key] = replace(snapshot)
        if snapshot.status == "ok":
            self._append_oi_point_locked(exchange_key, snapshot)
            self.live_by_exchange.setdefault(exchange_key, replace(snapshot))

    def _append_oi_point_locked(self, exchange_key: str, snapshot: ExchangeSnapshot) -> None:
        if snapshot.open_interest is None and snapshot.open_interest_notional is None:
            return
        history = self.oi_history[exchange_key]
        previous_point = history[-1] if history else None
        point = OIPoint(
            timestamp_ms=snapshot.timestamp_ms or int(time.time() * 1000),
            open_interest=snapshot.open_interest,
            open_interest_notional=snapshot.open_interest_notional,
        )
        if history and abs(history[-1].timestamp_ms - point.timestamp_ms) <= 1_000:
            history[-1] = point
        else:
            history.append(point)
        previous_oi = previous_point.open_interest if previous_point is not None else None
        current_oi = point.open_interest
        delta_oi = (
            float(current_oi) - float(previous_oi)
            if current_oi is not None and previous_oi is not None
            else None
        )
        previous_notional = previous_point.open_interest_notional if previous_point is not None else None
        current_notional = point.open_interest_notional
        delta_notional = (
            float(current_notional) - float(previous_notional)
            if current_notional is not None and previous_notional is not None
            else None
        )
        reference_price = snapshot.last_price or snapshot.mark_price or snapshot.index_price
        if delta_notional is None and delta_oi is not None and reference_price not in (None, 0):
            delta_notional = float(delta_oi) * float(reference_price)
        size_delta = abs(float(delta_oi)) if delta_oi is not None else None
        if size_delta is None and delta_notional is not None and reference_price not in (None, 0):
            size_delta = abs(float(delta_notional)) / float(reference_price)
        direction = None
        if delta_notional is not None:
            if float(delta_notional) > 0:
                direction = "buy"
            elif float(delta_notional) < 0:
                direction = "sell"
        elif delta_oi is not None:
            if float(delta_oi) > 0:
                direction = "buy"
            elif float(delta_oi) < 0:
                direction = "sell"
        self._append_recorded_event_locked(
            exchange_key,
            RecordedMarketEvent(
                timestamp_ms=point.timestamp_ms,
                exchange=snapshot.exchange,
                symbol=snapshot.symbol,
                category="oi",
                market="perp",
                side=direction,
                price=reference_price,
                size=size_delta,
                notional=abs(float(delta_notional)) if delta_notional is not None else None,
                value=point.open_interest_notional if point.open_interest_notional is not None else point.open_interest,
                label="OI update",
                raw={
                    "open_interest": point.open_interest,
                    "open_interest_notional": point.open_interest_notional,
                    "delta_open_interest": delta_oi,
                    "delta_open_interest_notional": delta_notional,
                },
            ),
        )

    def _append_liquidation_event_locked(self, exchange_key: str, event: LiquidationEvent) -> None:
        if not self._matches_expected_symbol(exchange_key, event.symbol):
            return
        history = self.liquidation_history[exchange_key]
        event_id = (
            event.timestamp_ms,
            event.side,
            round(event.price or 0.0, 6),
            round(event.size or 0.0, 6),
        )
        if history:
            last = history[-1]
            last_id = (
                last.timestamp_ms,
                last.side,
                round(last.price or 0.0, 6),
                round(last.size or 0.0, 6),
            )
            if last_id == event_id:
                return
        history.append(event)
        self.liquidation_archive.append(exchange_key, event)
        self._append_recorded_event_locked(
            exchange_key,
            RecordedMarketEvent(
                timestamp_ms=event.timestamp_ms,
                exchange=event.exchange,
                symbol=event.symbol,
                category="liquidation",
                market="perp",
                side=event.side,
                price=event.price,
                size=event.size,
                notional=event.notional,
                label=event.source,
                raw=event.raw,
            ),
        )
        self._emit_source_update(exchange_key, "perp", "liquidations")

    def _append_trade_event_locked(self, exchange_key: str, event: TradeEvent) -> None:
        if not self._matches_expected_symbol(exchange_key, event.symbol):
            return
        history = self.trade_history[exchange_key]
        event_id = (
            event.timestamp_ms,
            event.side,
            round(event.price or 0.0, 6),
            round(event.size or 0.0, 6),
        )
        if history:
            last = history[-1]
            last_id = (
                last.timestamp_ms,
                last.side,
                round(last.price or 0.0, 6),
                round(last.size or 0.0, 6),
            )
            if last_id == event_id:
                return
        history.append(event)
        self._append_recorded_event_locked(
            exchange_key,
            RecordedMarketEvent(
                timestamp_ms=event.timestamp_ms,
                exchange=event.exchange,
                symbol=event.symbol,
                category="trade",
                market="perp",
                side=event.side,
                price=event.price,
                size=event.size,
                notional=event.notional,
                label=event.source,
                raw=event.raw,
            ),
        )
        self._emit_source_update(exchange_key, "perp", "trades")

    def _append_recorded_event_locked(self, exchange_key: str, event: RecordedMarketEvent, *, spot: bool = False) -> None:
        if not self._matches_expected_symbol(exchange_key, event.symbol, spot=spot):
            return
        history = self.spot_recorded_events[exchange_key] if spot else self.recorded_events[exchange_key]
        event_id = (
            event.timestamp_ms,
            event.category,
            event.market,
            event.side,
            round(event.price or 0.0, 6),
            round(event.value or event.notional or 0.0, 6),
        )
        if history:
            last = history[-1]
            last_id = (
                last.timestamp_ms,
                last.category,
                last.market,
                last.side,
                round(last.price or 0.0, 6),
                round(last.value or last.notional or 0.0, 6),
            )
            if last_id == event_id:
                return
        history.append(event)

    @staticmethod
    def _compute_book_imbalance(bid_state: Dict[float, float], ask_state: Dict[float, float]) -> float | None:
        bid_notional = sum(price * size for price, size in bid_state.items() if size > 0)
        ask_notional = sum(price * size for price, size in ask_state.items() if size > 0)
        total_notional = bid_notional + ask_notional
        if total_notional <= 0:
            return None
        return (bid_notional - ask_notional) / total_notional * 100.0

    @staticmethod
    def _best_prices_from_state(side_state: Dict[str, Dict[float, float]]) -> tuple[float | None, float | None]:
        bid_prices = [price for price, size in side_state["bid"].items() if size > 0]
        ask_prices = [price for price, size in side_state["ask"].items() if size > 0]
        best_bid = max(bid_prices) if bid_prices else None
        best_ask = min(ask_prices) if ask_prices else None
        return best_bid, best_ask

    @staticmethod
    def _reference_price_from_state(side_state: Dict[str, Dict[float, float]]) -> float | None:
        best_bid, best_ask = LiveTerminalService._best_prices_from_state(side_state)
        if best_bid is not None and best_ask is not None:
            return (best_bid + best_ask) * 0.5
        return best_bid if best_bid is not None else best_ask

    def _record_orderbook_quality_locked(
        self,
        exchange_key: str,
        previous_bid_state: Dict[float, float],
        previous_ask_state: Dict[float, float],
        *,
        spot: bool = False,
    ) -> None:
        side_state = self.spot_orderbook_state[exchange_key] if spot else self.orderbook_state[exchange_key]
        quality_history = self.spot_orderbook_quality_history[exchange_key] if spot else self.orderbook_quality_history[exchange_key]
        quality_state = self.spot_orderbook_quality_state[exchange_key] if spot else self.orderbook_quality_state[exchange_key]
        exchange_name = self.spot_clients[exchange_key].exchange_name if spot else self.clients[exchange_key].exchange_name
        symbol = self.spot_symbol_map.get(exchange_key, "") if spot else self.symbol_map.get(exchange_key, "")
        now_ms = int(time.time() * 1000)
        reference_price = self._reference_price_from_state(side_state)
        best_bid, best_ask = self._best_prices_from_state(side_state)
        added_notional = 0.0
        canceled_notional = 0.0
        near_added_notional = 0.0
        near_canceled_notional = 0.0
        spoof_events = 0
        refill_events = 0
        persistence_by_side = {"bid": 0.0, "ask": 0.0}
        near_ratio = 0.0016
        spoof_window_ms = 8_000
        refill_window_ms = 3_000

        for side in ("bid", "ask"):
            previous_state = previous_bid_state if side == "bid" else previous_ask_state
            current_state = side_state[side]
            prices = set(previous_state) | set(current_state)
            for price in prices:
                previous_size = previous_state.get(price, 0.0)
                current_size = current_state.get(price, 0.0)
                delta_size = current_size - previous_size
                if abs(delta_size) <= 1e-12:
                    continue
                delta_notional = abs(delta_size) * price
                is_near = reference_price is not None and reference_price > 0 and abs(price - reference_price) / reference_price <= near_ratio
                if delta_size > 0:
                    added_notional += delta_notional
                    if is_near:
                        near_added_notional += delta_notional
                else:
                    canceled_notional += delta_notional
                    if is_near:
                        near_canceled_notional += delta_notional

            wall_registry = quality_state["wall_registry"][side]
            current_levels = [(price, size, price * size) for price, size in current_state.items() if size > 0]
            notionals = sorted((notional for _, _, notional in current_levels), reverse=True)
            threshold = notionals[min(2, len(notionals) - 1)] if notionals else 0.0
            if threshold <= 0 and notionals:
                threshold = notionals[0]
            notable_prices = set()
            for price, size, notional in current_levels:
                if notional < threshold or threshold <= 0:
                    continue
                notable_prices.add(price)
                if price not in wall_registry:
                    wall_registry[price] = {
                        "first_seen": now_ms,
                        "last_seen": now_ms,
                        "peak_notional": notional,
                    }
                else:
                    wall_registry[price]["last_seen"] = now_ms
                    wall_registry[price]["peak_notional"] = max(float(wall_registry[price]["peak_notional"]), notional)

            expired_prices = []
            for price, metadata in wall_registry.items():
                if price in notable_prices:
                    continue
                age_ms = now_ms - int(metadata["first_seen"])
                if age_ms <= spoof_window_ms:
                    spoof_events += 1
                expired_prices.append(price)
            for price in expired_prices:
                wall_registry.pop(price, None)

            live_ages = [max(0.0, (now_ms - int(metadata["first_seen"])) / 1000.0) for metadata in wall_registry.values()]
            persistence_by_side[side] = sum(live_ages) / len(live_ages) if live_ages else 0.0

            previous_best_price = max(previous_state) if side == "bid" and previous_state else min(previous_state) if previous_state else None
            current_best_price = best_bid if side == "bid" else best_ask
            previous_best_size = previous_state.get(previous_best_price, 0.0) if previous_best_price is not None else 0.0
            current_best_size = current_state.get(current_best_price, 0.0) if current_best_price is not None else 0.0
            refill_watch = quality_state["refill_watch"][side]
            if previous_best_price is not None and previous_best_size > 0:
                same_zone = (
                    current_best_price is not None
                    and abs(current_best_price - previous_best_price) / previous_best_price <= 0.0008
                )
                if same_zone and current_best_size < previous_best_size * 0.65:
                    quality_state["refill_watch"][side] = {
                        "timestamp_ms": now_ms,
                        "price": current_best_price,
                        "target_size": previous_best_size,
                    }
                    refill_watch = quality_state["refill_watch"][side]
            if refill_watch is not None:
                if now_ms - int(refill_watch["timestamp_ms"]) > refill_window_ms:
                    quality_state["refill_watch"][side] = None
                elif (
                    current_best_price is not None
                    and abs(current_best_price - float(refill_watch["price"])) / max(float(refill_watch["price"]), 1e-9) <= 0.0008
                    and current_best_size >= float(refill_watch["target_size"]) * 0.85
                ):
                    refill_events += 1
                    quality_state["refill_watch"][side] = None

        quality_point = OrderBookQualityPoint(
            timestamp_ms=now_ms,
            added_notional=added_notional,
            canceled_notional=canceled_notional,
            net_notional=added_notional - canceled_notional,
            near_added_notional=near_added_notional,
            near_canceled_notional=near_canceled_notional,
            spoof_events=spoof_events,
            refill_events=refill_events,
            bid_wall_persistence_s=persistence_by_side["bid"],
            ask_wall_persistence_s=persistence_by_side["ask"],
            imbalance_pct=self._compute_book_imbalance(side_state["bid"], side_state["ask"]),
            best_bid=best_bid,
            best_ask=best_ask,
        )
        bucket_ms = 1_000
        if quality_history and int(quality_history[-1].timestamp_ms // bucket_ms) == int(quality_point.timestamp_ms // bucket_ms):
            previous = quality_history[-1]
            quality_history[-1] = OrderBookQualityPoint(
                timestamp_ms=quality_point.timestamp_ms,
                added_notional=previous.added_notional + quality_point.added_notional,
                canceled_notional=previous.canceled_notional + quality_point.canceled_notional,
                net_notional=previous.net_notional + quality_point.net_notional,
                near_added_notional=previous.near_added_notional + quality_point.near_added_notional,
                near_canceled_notional=previous.near_canceled_notional + quality_point.near_canceled_notional,
                spoof_events=previous.spoof_events + quality_point.spoof_events,
                refill_events=previous.refill_events + quality_point.refill_events,
                bid_wall_persistence_s=quality_point.bid_wall_persistence_s,
                ask_wall_persistence_s=quality_point.ask_wall_persistence_s,
                imbalance_pct=quality_point.imbalance_pct,
                best_bid=quality_point.best_bid,
                best_ask=quality_point.best_ask,
            )
        else:
            quality_history.append(quality_point)

        self._append_recorded_event_locked(
            exchange_key,
            RecordedMarketEvent(
                timestamp_ms=quality_point.timestamp_ms,
                exchange=exchange_name,
                symbol=symbol,
                category="orderbook_quality",
                market="spot" if spot else "perp",
                side="buy" if float(quality_point.net_notional or 0.0) > 0 else "sell" if float(quality_point.net_notional or 0.0) < 0 else None,
                price=((quality_point.best_bid or 0.0) + (quality_point.best_ask or 0.0)) * 0.5 if quality_point.best_bid and quality_point.best_ask else quality_point.best_bid or quality_point.best_ask,
                size=(
                    (abs(float(quality_point.added_notional or 0.0)) + abs(float(quality_point.canceled_notional or 0.0)))
                    / (((quality_point.best_bid or 0.0) + (quality_point.best_ask or 0.0)) * 0.5)
                    if quality_point.best_bid and quality_point.best_ask and ((quality_point.best_bid or 0.0) + (quality_point.best_ask or 0.0)) > 0
                    else None
                ),
                notional=abs(float(quality_point.added_notional or 0.0)) + abs(float(quality_point.canceled_notional or 0.0)),
                value=quality_point.net_notional,
                label="quality",
                raw={
                    "added_notional": quality_point.added_notional,
                    "canceled_notional": quality_point.canceled_notional,
                    "spoof_events": quality_point.spoof_events,
                    "refill_events": quality_point.refill_events,
                    "imbalance_pct": quality_point.imbalance_pct,
                },
            ),
            spot=spot,
        )

    def _replace_orderbook_locked(self, exchange_key: str, bids, asks) -> None:
        bid_state = self.orderbook_state[exchange_key]["bid"]
        ask_state = self.orderbook_state[exchange_key]["ask"]
        previous_bid_state = dict(bid_state)
        previous_ask_state = dict(ask_state)
        bid_state.clear()
        ask_state.clear()
        for price, size in bids:
            if price is not None and size is not None and size > 0:
                bid_state[price] = size
        for price, size in asks:
            if price is not None and size is not None and size > 0:
                ask_state[price] = size
        self._refresh_orderbook_levels_locked(exchange_key)
        self._record_orderbook_quality_locked(exchange_key, previous_bid_state, previous_ask_state)

    def _apply_orderbook_delta_locked(self, exchange_key: str, bids, asks) -> None:
        bid_state = self.orderbook_state[exchange_key]["bid"]
        ask_state = self.orderbook_state[exchange_key]["ask"]
        previous_bid_state = dict(bid_state)
        previous_ask_state = dict(ask_state)
        for price, size in bids:
            if price is None or size is None:
                continue
            if size <= 0:
                bid_state.pop(price, None)
            else:
                bid_state[price] = size
        for price, size in asks:
            if price is None or size is None:
                continue
            if size <= 0:
                ask_state.pop(price, None)
            else:
                ask_state[price] = size
        self._refresh_orderbook_levels_locked(exchange_key)
        self._record_orderbook_quality_locked(exchange_key, previous_bid_state, previous_ask_state)

    def _refresh_orderbook_levels_locked(self, exchange_key: str) -> None:
        bid_levels = sorted(
            self.orderbook_state[exchange_key]["bid"].items(),
            key=lambda item: item[0],
            reverse=True,
        )[: self.orderbook_limit]
        ask_levels = sorted(
            self.orderbook_state[exchange_key]["ask"].items(),
            key=lambda item: item[0],
        )[: self.orderbook_limit]
        self.orderbooks[exchange_key] = [
            OrderBookLevel(price=price, size=size, side="bid") for price, size in bid_levels
        ] + [
            OrderBookLevel(price=price, size=size, side="ask") for price, size in ask_levels
        ]
        self._emit_source_update(exchange_key, "perp", "depth")

    def _replace_spot_orderbook_locked(self, exchange_key: str, bids, asks) -> None:
        previous_bid_state = dict(self.spot_orderbook_state[exchange_key]["bid"])
        previous_ask_state = dict(self.spot_orderbook_state[exchange_key]["ask"])
        self.spot_orderbook_state[exchange_key]["bid"].clear()
        self.spot_orderbook_state[exchange_key]["ask"].clear()
        for price, size in bids:
            if price is not None and size is not None and size > 0:
                self.spot_orderbook_state[exchange_key]["bid"][price] = size
        for price, size in asks:
            if price is not None and size is not None and size > 0:
                self.spot_orderbook_state[exchange_key]["ask"][price] = size
        self._refresh_spot_orderbook_locked(exchange_key)
        self._record_orderbook_quality_locked(exchange_key, previous_bid_state, previous_ask_state, spot=True)

    def _apply_spot_orderbook_delta_locked(self, exchange_key: str, bids, asks) -> None:
        previous_bid_state = dict(self.spot_orderbook_state[exchange_key]["bid"])
        previous_ask_state = dict(self.spot_orderbook_state[exchange_key]["ask"])
        for price, size in bids:
            if price is None or size is None:
                continue
            if size <= 0:
                self.spot_orderbook_state[exchange_key]["bid"].pop(price, None)
            else:
                self.spot_orderbook_state[exchange_key]["bid"][price] = size
        for price, size in asks:
            if price is None or size is None:
                continue
            if size <= 0:
                self.spot_orderbook_state[exchange_key]["ask"].pop(price, None)
            else:
                self.spot_orderbook_state[exchange_key]["ask"][price] = size
        self._refresh_spot_orderbook_locked(exchange_key)
        self._record_orderbook_quality_locked(exchange_key, previous_bid_state, previous_ask_state, spot=True)

    def _refresh_spot_orderbook_locked(self, exchange_key: str) -> None:
        bid_levels = sorted(
            self.spot_orderbook_state[exchange_key]["bid"].items(),
            key=lambda item: item[0],
            reverse=True,
        )[: self.orderbook_limit]
        ask_levels = sorted(
            self.spot_orderbook_state[exchange_key]["ask"].items(),
            key=lambda item: item[0],
        )[: self.orderbook_limit]
        self.spot_orderbooks[exchange_key] = [OrderBookLevel(price=price, size=size, side="bid") for price, size in bid_levels] + [
            OrderBookLevel(price=price, size=size, side="ask") for price, size in ask_levels
        ]
        self._sync_spot_best_prices_locked(exchange_key)
        self._emit_source_update(exchange_key, "spot", "trades")

    def _sync_spot_best_prices_locked(self, exchange_key: str) -> None:
        top_bid = next((level.price for level in self.spot_orderbooks[exchange_key] if level.side == "bid"), None)
        top_ask = next((level.price for level in self.spot_orderbooks[exchange_key] if level.side == "ask"), None)
        if self.spot_snapshots.get(exchange_key) is None:
            self.spot_snapshots[exchange_key] = SpotSnapshot(
                exchange=self.spot_clients[exchange_key].exchange_name,
                symbol=self.spot_symbol_map.get(exchange_key, ""),
            )
        self.spot_snapshots[exchange_key].bid_price = top_bid
        self.spot_snapshots[exchange_key].ask_price = top_ask

    def _append_spot_trade_event_locked(self, exchange_key: str, event: TradeEvent) -> None:
        if not self._matches_expected_symbol(exchange_key, event.symbol, spot=True):
            return
        event_id = (
            event.timestamp_ms,
            event.side,
            round(event.price or 0.0, 6),
            round(event.size or 0.0, 6),
        )
        history = self.spot_trade_histories[exchange_key]
        if history:
            last = history[-1]
            last_id = (
                last.timestamp_ms,
                last.side,
                round(last.price or 0.0, 6),
                round(last.size or 0.0, 6),
            )
            if last_id == event_id:
                return
        history.append(event)
        self._append_recorded_event_locked(
            exchange_key,
            RecordedMarketEvent(
                timestamp_ms=event.timestamp_ms,
                exchange=event.exchange,
                symbol=event.symbol,
                category="trade",
                market="spot",
                side=event.side,
                price=event.price,
                size=event.size,
                notional=event.notional,
                label=event.source,
                raw=event.raw,
            ),
            spot=True,
        )
        self._emit_source_update(exchange_key, "spot", "depth")

    def _run_ws_worker(self, exchange_key: str) -> None:
        while not self.stop_event.is_set():
            symbol = self.symbol_map.get(exchange_key)
            if not symbol:
                return
            ws_url = self._build_ws_url(exchange_key, symbol)
            app = websocket.WebSocketApp(
                ws_url,
                on_open=lambda ws, key=exchange_key, sym=symbol: self._on_open(key, sym, ws),
                on_message=lambda ws, message, key=exchange_key, sym=symbol: self._on_message(key, sym, message),
                on_error=lambda ws, error, key=exchange_key, sym=symbol: self._on_error(key, sym, error),
            )
            self.ws_apps[exchange_key] = app
            try:
                app.run_forever(ping_interval=20, ping_timeout=10)
            except Exception as exc:
                self._on_error(exchange_key, symbol, exc)
            if self.stop_event.wait(3):
                return

    def _run_spot_ws_worker(self, exchange_key: str) -> None:
        while not self.stop_event.is_set():
            symbol = self.spot_symbol_map.get(exchange_key)
            if not symbol:
                return
            ws_url = self._build_spot_ws_url(exchange_key, symbol)
            app = websocket.WebSocketApp(
                ws_url,
                on_open=lambda ws, key=exchange_key, sym=symbol: self._on_spot_open(key, sym, ws),
                on_message=lambda ws, message, key=exchange_key, sym=symbol: self._on_spot_message(key, sym, message),
                on_error=lambda ws, error, key=exchange_key, sym=symbol: self._on_spot_error(key, sym, error),
            )
            self.ws_apps[f"spot_{exchange_key}"] = app
            try:
                app.run_forever(ping_interval=20, ping_timeout=10)
            except Exception as exc:
                self._on_spot_error(exchange_key, symbol, exc)
            if self.stop_event.wait(3):
                return

    def _build_ws_url(self, exchange_key: str, symbol: str) -> str:
        if exchange_key == "bybit":
            return "wss://stream.bybit.com/v5/public/linear"
        if exchange_key == "binance":
            lower = symbol.lower()
            return (
                "wss://fstream.binance.com/stream?streams="
                f"{lower}@markPrice@1s/{lower}@ticker/{lower}@forceOrder/{lower}@aggTrade/{lower}@depth@100ms"
            )
        if exchange_key == "okx":
            return "wss://ws.okx.com:8443/ws/v5/public"
        if exchange_key == "bitget":
            return "wss://ws.bitget.com/v3/ws/public"
        return "wss://api.hyperliquid.xyz/ws"

    def _build_spot_ws_url(self, exchange_key: str, symbol: str) -> str:
        if exchange_key == "binance":
            lower = symbol.lower()
            return f"wss://stream.binance.com:9443/stream?streams={lower}@ticker/{lower}@aggTrade/{lower}@depth@100ms"
        if exchange_key == "bybit":
            return "wss://stream.bybit.com/v5/public/spot"
        return "wss://ws.okx.com:8443/ws/v5/public"

    def _on_open(self, exchange_key: str, symbol: str, ws: websocket.WebSocketApp) -> None:
        if exchange_key == "bybit":
            ws.send(
                json.dumps(
                    {
                        "op": "subscribe",
                        "args": [
                            f"tickers.{symbol}",
                            f"allLiquidation.{symbol}",
                            f"orderbook.50.{symbol}",
                            f"publicTrade.{symbol}",
                        ],
                    }
                )
            )
        elif exchange_key == "okx":
            okx_underlying = _okx_underlying_from_symbol(symbol)
            ws.send(
                json.dumps(
                    {
                        "op": "subscribe",
                        "args": [
                            {"channel": "tickers", "instId": symbol},
                            {"channel": "mark-price", "instId": symbol},
                            {"channel": "books5", "instId": symbol},
                            {"channel": "trades", "instId": symbol},
                            {"channel": "liquidation-orders", "instType": "SWAP", "uly": okx_underlying} if okx_underlying else {"channel": "liquidation-orders", "instId": symbol},
                        ],
                    }
                )
            )
        elif exchange_key == "bitget":
            ws.send(json.dumps({"op": "subscribe", "args": SharedRealtimeHub._bitget_public_args([symbol], include_liquidation=True)}))
        elif exchange_key == "hyperliquid":
            ws.send(json.dumps({"method": "subscribe", "subscription": {"type": "allMids"}}))
            ws.send(json.dumps({"method": "subscribe", "subscription": {"type": "l2Book", "coin": symbol}}))
            ws.send(json.dumps({"method": "subscribe", "subscription": {"type": "trades", "coin": symbol}}))

    def _on_spot_open(self, exchange_key: str, symbol: str, ws: websocket.WebSocketApp) -> None:
        if exchange_key == "bybit":
            ws.send(
                json.dumps(
                    {
                        "op": "subscribe",
                        "args": [
                            f"tickers.{symbol}",
                            f"orderbook.50.{symbol}",
                            f"publicTrade.{symbol}",
                        ],
                    }
                )
            )
        elif exchange_key == "okx":
            ws.send(
                json.dumps(
                    {
                        "op": "subscribe",
                        "args": [
                            {"channel": "tickers", "instId": symbol},
                            {"channel": "books5", "instId": symbol},
                            {"channel": "trades", "instId": symbol},
                        ],
                    }
                )
            )

    def _on_message(self, exchange_key: str, symbol: str, message: str) -> None:
        try:
            payload = json.loads(message)
        except json.JSONDecodeError:
            return
        self._on_message_payload(exchange_key, symbol, payload)

    def _apply_shared_structured_event(self, event: Dict[str, object]) -> None:
        kind = str(event.get("kind") or "")
        exchange_key = str(event.get("exchange_key") or "").strip().lower()
        symbol = str(event.get("symbol") or "").strip()
        if not exchange_key or not symbol:
            return
        if kind == "snapshot_update":
            values = event.get("values") or {}
            if not isinstance(values, dict):
                return
            if bool(event.get("spot")):
                self._update_spot_snapshot(exchange_key, symbol, **values)
            else:
                self._update_live_snapshot(exchange_key, symbol, **values)
            return
        if kind == "trade_event":
            payload = event.get("event") or {}
            if not isinstance(payload, dict):
                return
            with self.lock:
                trade_event = TradeEvent(
                    exchange=self.spot_clients[exchange_key].exchange_name if bool(event.get("spot")) else self.clients[exchange_key].exchange_name,
                    symbol=str(payload.get("symbol") or symbol),
                    timestamp_ms=safe_int(payload.get("timestamp_ms")) or int(time.time() * 1000),
                    side=normalize_trade_side(payload.get("side")),
                    price=safe_float(payload.get("price")),
                    size=safe_float(payload.get("size")),
                    notional=safe_float(payload.get("notional")),
                    source=str(payload.get("source") or "ws"),
                    raw=payload.get("raw") if isinstance(payload.get("raw"), dict) else {},
                )
                if bool(event.get("spot")):
                    self._append_spot_trade_event_locked(exchange_key, trade_event)
                else:
                    self._append_trade_event_locked(exchange_key, trade_event)
            return
        if kind == "trade_batch":
            items = event.get("events") or []
            if not isinstance(items, list):
                return
            with self.lock:
                exchange_name = self.spot_clients[exchange_key].exchange_name if bool(event.get("spot")) else self.clients[exchange_key].exchange_name
                for payload in items:
                    if not isinstance(payload, dict):
                        continue
                    trade_event = TradeEvent(
                        exchange=exchange_name,
                        symbol=str(payload.get("symbol") or symbol),
                        timestamp_ms=safe_int(payload.get("timestamp_ms")) or int(time.time() * 1000),
                        side=normalize_trade_side(payload.get("side")),
                        price=safe_float(payload.get("price")),
                        size=safe_float(payload.get("size")),
                        notional=safe_float(payload.get("notional")),
                        source=str(payload.get("source") or "ws"),
                        raw=payload.get("raw") if isinstance(payload.get("raw"), dict) else {},
                    )
                    if bool(event.get("spot")):
                        self._append_spot_trade_event_locked(exchange_key, trade_event)
                    else:
                        self._append_trade_event_locked(exchange_key, trade_event)
            return
        if kind == "liquidation_event":
            payload = event.get("event") or {}
            if not isinstance(payload, dict):
                return
            with self.lock:
                liquidation_event = LiquidationEvent(
                    exchange=self.clients[exchange_key].exchange_name,
                    symbol=str(payload.get("symbol") or symbol),
                    timestamp_ms=safe_int(payload.get("timestamp_ms")) or int(time.time() * 1000),
                    side=normalize_liquidation_side(payload.get("side")),
                    price=safe_float(payload.get("price")),
                    size=safe_float(payload.get("size")),
                    notional=safe_float(payload.get("notional")),
                    source=str(payload.get("source") or "ws"),
                    raw=payload.get("raw") if isinstance(payload.get("raw"), dict) else {},
                )
                self._append_liquidation_event_locked(exchange_key, liquidation_event)
            return
        if kind == "liquidation_batch":
            items = event.get("events") or []
            if not isinstance(items, list):
                return
            with self.lock:
                exchange_name = self.clients[exchange_key].exchange_name
                for payload in items:
                    if not isinstance(payload, dict):
                        continue
                    liquidation_event = LiquidationEvent(
                        exchange=exchange_name,
                        symbol=str(payload.get("symbol") or symbol),
                        timestamp_ms=safe_int(payload.get("timestamp_ms")) or int(time.time() * 1000),
                        side=normalize_liquidation_side(payload.get("side")),
                        price=safe_float(payload.get("price")),
                        size=safe_float(payload.get("size")),
                        notional=safe_float(payload.get("notional")),
                        source=str(payload.get("source") or "ws"),
                        raw=payload.get("raw") if isinstance(payload.get("raw"), dict) else {},
                    )
                    self._append_liquidation_event_locked(exchange_key, liquidation_event)
            return
        if kind == "orderbook_update":
            bids = event.get("bids") or []
            asks = event.get("asks") or []
            if not isinstance(bids, list) or not isinstance(asks, list):
                return
            with self.lock:
                if bool(event.get("spot")):
                    if str(event.get("mode") or "") == "snapshot" or not self.spot_orderbooks.get(exchange_key):
                        self._replace_spot_orderbook_locked(exchange_key, bids, asks)
                    else:
                        self._apply_spot_orderbook_delta_locked(exchange_key, bids, asks)
                else:
                    if str(event.get("mode") or "") == "snapshot" or not self.orderbooks.get(exchange_key):
                        self._replace_orderbook_locked(exchange_key, bids, asks)
                    else:
                        self._apply_orderbook_delta_locked(exchange_key, bids, asks)
            return
        if kind == "binance_depth_event":
            payload = event.get("event") or {}
            if not isinstance(payload, dict):
                return
            if bool(event.get("spot")):
                self._apply_binance_spot_depth_event(exchange_key, symbol, payload)
            else:
                self._apply_binance_depth_event(symbol, payload)

    def _on_message_payload(self, exchange_key: str, symbol: str, payload: Dict[str, object]) -> None:
        if exchange_key == "bybit":
            topic = str(payload.get("topic", ""))
            if topic.startswith("allLiquidation"):
                self._handle_bybit_liquidation(symbol, payload)
            elif topic.startswith("orderbook"):
                self._handle_bybit_orderbook(payload)
            elif topic.startswith("publicTrade"):
                self._handle_bybit_trade(symbol, payload)
            else:
                self._handle_bybit_message(symbol, payload)
        elif exchange_key == "binance":
            self._handle_binance_message(symbol, payload)
        elif exchange_key == "okx":
            self._handle_okx_message(symbol, payload)
        elif exchange_key == "hyperliquid":
            self._handle_hyperliquid_message(symbol, payload)
        elif exchange_key == "bitget":
            self._handle_bitget_message(symbol, payload)

    def _on_spot_message(self, exchange_key: str, symbol: str, message: str) -> None:
        try:
            payload = json.loads(message)
        except json.JSONDecodeError:
            return
        self._on_spot_message_payload(exchange_key, symbol, payload)

    def _on_spot_message_payload(self, exchange_key: str, symbol: str, payload: Dict[str, object]) -> None:
        self._handle_spot_message(exchange_key, symbol, payload)

    def _on_error(self, exchange_key: str, symbol: str, error: Exception) -> None:
        with self.lock:
            previous = self.live_by_exchange.get(exchange_key)
            snapshot = replace(previous) if previous is not None else ExchangeSnapshot(
                exchange=self.clients[exchange_key].exchange_name,
                symbol=symbol,
            )
            snapshot.status = "error"
            snapshot.error = str(error)
            self.live_by_exchange[exchange_key] = snapshot

    def _on_spot_error(self, exchange_key: str, symbol: str, error: Exception) -> None:
        with self.lock:
            previous = self.spot_snapshots.get(exchange_key)
            snapshot = replace(previous) if previous is not None else SpotSnapshot(
                exchange=self.spot_clients[exchange_key].exchange_name,
                symbol=symbol,
            )
            snapshot.status = "error"
            snapshot.error = str(error)
            self.spot_snapshots[exchange_key] = snapshot

    def _update_live_snapshot(self, exchange_key: str, symbol: str, **values) -> None:
        with self.lock:
            previous = self.live_by_exchange.get(exchange_key)
            snapshot = replace(previous) if previous is not None else ExchangeSnapshot(
                exchange=self.clients[exchange_key].exchange_name,
                symbol=symbol,
            )
            snapshot.exchange = self.clients[exchange_key].exchange_name
            snapshot.symbol = symbol
            snapshot.status = "ok"
            snapshot.error = None
            for key, value in values.items():
                if value is not None:
                    setattr(snapshot, key, value)
            self.live_by_exchange[exchange_key] = snapshot
        self._emit_source_update(exchange_key, "perp", "depth")

    def _update_spot_snapshot(self, exchange_key: str, symbol: str, **values) -> None:
        with self.lock:
            previous = self.spot_snapshots.get(exchange_key)
            snapshot = replace(previous) if previous is not None else SpotSnapshot(
                exchange=self.spot_clients[exchange_key].exchange_name,
                symbol=symbol,
            )
            snapshot.exchange = self.spot_clients[exchange_key].exchange_name
            snapshot.symbol = symbol
            snapshot.status = "ok"
            snapshot.error = None
            for key, value in values.items():
                if value is not None:
                    setattr(snapshot, key, value)
            self.spot_snapshots[exchange_key] = snapshot
        self._emit_source_update(exchange_key, "spot", "depth")

    def _apply_binance_depth_event(self, symbol: str, event: Dict[str, object]) -> None:
        if event.get("U") is None or event.get("u") is None:
            return
        should_bootstrap = False
        with self.lock:
            now_ms = int(time.time() * 1000)
            self.binance_depth_buffer.append(event)
            if not self.binance_depth_synced:
                self._apply_binance_depth_proxy_locked(event)
                if not self.binance_depth_bootstrapping and now_ms >= self.binance_depth_bootstrap_retry_at_ms:
                    self.binance_depth_bootstrapping = True
                    self.binance_depth_last_bootstrap_attempt_ms = now_ms
                    should_bootstrap = True
            else:
                if self.binance_depth_last_u is not None and int(event["U"]) > self.binance_depth_last_u + 1:
                    self.binance_depth_synced = False
                    self.binance_depth_buffer.clear()
                    self.binance_depth_buffer.append(event)
                    self._apply_binance_depth_proxy_locked(event, reset=True)
                    if not self.binance_depth_bootstrapping and now_ms >= self.binance_depth_bootstrap_retry_at_ms:
                        self.binance_depth_bootstrapping = True
                        self.binance_depth_last_bootstrap_attempt_ms = now_ms
                        should_bootstrap = True
                elif self.binance_depth_last_u is None or int(event["u"]) > self.binance_depth_last_u:
                    self._apply_orderbook_delta_locked("binance", event["bids"], event["asks"])
                    self.binance_depth_last_u = int(event["u"])
        if should_bootstrap:
            self._bootstrap_binance_depth(symbol)

    def _apply_binance_spot_depth_event(self, exchange_key: str, symbol: str, event: Dict[str, object]) -> None:
        if event.get("U") is None or event.get("u") is None:
            return
        should_bootstrap = False
        with self.lock:
            if not self.binance_spot_depth_synced:
                self.binance_spot_depth_buffer.append(event)
                if not self.binance_spot_depth_bootstrapping:
                    self.binance_spot_depth_bootstrapping = True
                    should_bootstrap = True
            else:
                if self.binance_spot_depth_last_u is not None and int(event["U"]) > self.binance_spot_depth_last_u + 1:
                    self.binance_spot_depth_synced = False
                    self.binance_spot_depth_last_u = None
                    self.binance_spot_depth_buffer.clear()
                    self.binance_spot_depth_buffer.append(event)
                    if not self.binance_spot_depth_bootstrapping:
                        self.binance_spot_depth_bootstrapping = True
                        should_bootstrap = True
                elif self.binance_spot_depth_last_u is None or int(event["u"]) > self.binance_spot_depth_last_u:
                    self._apply_spot_orderbook_delta_locked(exchange_key, event["bids"], event["asks"])
                    self.binance_spot_depth_last_u = int(event["u"])
        if should_bootstrap:
            self._bootstrap_spot_depth(exchange_key, symbol)

    def _handle_bybit_message(self, symbol: str, payload: Dict[str, object]) -> None:
        if payload.get("success") is not None:
            return
        data = payload.get("data") or {}
        if not isinstance(data, dict):
            return
        self._update_live_snapshot(
            "bybit",
            symbol,
            last_price=safe_float(data.get("lastPrice")),
            mark_price=safe_float(data.get("markPrice")),
            index_price=safe_float(data.get("indexPrice")),
            open_interest=safe_float(data.get("openInterest")),
            open_interest_notional=safe_float(data.get("openInterestValue")),
            funding_rate=safe_float(data.get("fundingRate")),
            volume_24h_base=safe_float(data.get("volume24h")),
            volume_24h_notional=safe_float(data.get("turnover24h")),
            timestamp_ms=safe_int(payload.get("ts")) or int(time.time() * 1000),
        )

    def _handle_bybit_liquidation(self, symbol: str, payload: Dict[str, object]) -> None:
        items = payload.get("data") or []
        if isinstance(items, dict):
            items = [items]
        with self.lock:
            for item in items:
                side = normalize_liquidation_side_for_exchange("bybit", item.get("side") or item.get("S"))
                price = safe_float(item.get("price")) or safe_float(item.get("p"))
                size = safe_float(item.get("size")) or safe_float(item.get("v"))
                event = LiquidationEvent(
                    exchange=self.clients["bybit"].exchange_name,
                    symbol=symbol,
                    timestamp_ms=safe_int(item.get("updatedTime")) or safe_int(item.get("T")) or int(time.time() * 1000),
                    side=side,
                    price=price,
                    size=size,
                    notional=compute_notional(price, size),
                    source="ws",
                    raw=item,
                )
                self._append_liquidation_event_locked("bybit", event)

    def _handle_bybit_orderbook(self, payload: Dict[str, object]) -> None:
        data = payload.get("data") or {}
        if not isinstance(data, dict):
            return
        bids = [(safe_float(price), safe_float(size)) for price, size in data.get("b", [])]
        asks = [(safe_float(price), safe_float(size)) for price, size in data.get("a", [])]
        with self.lock:
            if payload.get("type") == "snapshot" or not self.orderbooks["bybit"]:
                self._replace_orderbook_locked("bybit", bids, asks)
            else:
                self._apply_orderbook_delta_locked("bybit", bids, asks)

    def _handle_bybit_trade(self, symbol: str, payload: Dict[str, object]) -> None:
        items = payload.get("data") or []
        if isinstance(items, dict):
            items = [items]
        with self.lock:
            for item in items:
                price = safe_float(item.get("p")) or safe_float(item.get("price"))
                size = safe_float(item.get("v")) or safe_float(item.get("size"))
                event = TradeEvent(
                    exchange=self.clients["bybit"].exchange_name,
                    symbol=item.get("s") or symbol,
                    timestamp_ms=safe_int(item.get("T")) or safe_int(item.get("time")) or int(time.time() * 1000),
                    side=normalize_trade_side(item.get("S") or item.get("side")),
                    price=price,
                    size=size,
                    notional=compute_notional(price, size),
                    source="ws",
                    raw=item,
                )
                self._append_trade_event_locked("bybit", event)

    def _handle_binance_message(self, symbol: str, payload: Dict[str, object]) -> None:
        stream = str(payload.get("stream", ""))
        data = payload.get("data") or {}
        if "@forceorder" in stream.lower():
            order = data.get("o") or {}
            price = safe_float(order.get("ap")) or safe_float(order.get("p"))
            size = safe_float(order.get("z")) or safe_float(order.get("q"))
            event = LiquidationEvent(
                exchange=self.clients["binance"].exchange_name,
                symbol=order.get("s") or symbol,
                timestamp_ms=safe_int(data.get("E")) or safe_int(order.get("T")) or int(time.time() * 1000),
                side=normalize_liquidation_side(order.get("S")),
                price=price,
                size=size,
                notional=compute_notional(price, size),
                source="ws",
                raw=order,
            )
            with self.lock:
                self._append_liquidation_event_locked("binance", event)
            return

        if "@aggtrade" in stream.lower():
            price = safe_float(data.get("p"))
            size = safe_float(data.get("q"))
            event = TradeEvent(
                exchange=self.clients["binance"].exchange_name,
                symbol=data.get("s") or symbol,
                timestamp_ms=safe_int(data.get("E")) or safe_int(data.get("T")) or int(time.time() * 1000),
                side="sell" if bool(data.get("m")) else "buy",
                price=price,
                size=size,
                notional=compute_notional(price, size),
                source="ws",
                raw=data,
            )
            with self.lock:
                self._append_trade_event_locked("binance", event)
            return

        if "@depth@" in stream.lower():
            self._handle_binance_depth(symbol, data)
            return

        if "markprice" in stream.lower():
            self._update_live_snapshot(
                "binance",
                symbol,
                mark_price=safe_float(data.get("p")),
                index_price=safe_float(data.get("i")),
                funding_rate=safe_float(data.get("r")),
                timestamp_ms=safe_int(data.get("E")) or int(time.time() * 1000),
            )
        elif "@ticker" in stream.lower():
            self._update_live_snapshot(
                "binance",
                symbol,
                last_price=safe_float(data.get("c")),
                volume_24h_base=safe_float(data.get("v")),
                volume_24h_notional=safe_float(data.get("q")),
                timestamp_ms=safe_int(data.get("E")) or int(time.time() * 1000),
            )

    def _handle_binance_depth(self, symbol: str, data: Dict[str, object]) -> None:
        event = {
            "U": safe_int(data.get("U")),
            "u": safe_int(data.get("u")),
            "pu": safe_int(data.get("pu")),
            "bids": [(safe_float(price), safe_float(size)) for price, size in data.get("b", [])],
            "asks": [(safe_float(price), safe_float(size)) for price, size in data.get("a", [])],
        }
        self._apply_binance_depth_event(symbol, event)

    def _bootstrap_binance_depth(self, symbol: str) -> None:
        try:
            snapshot = fetch_binance_futures_orderbook_snapshot(symbol, 1000, timeout=self.timeout)
            last_update_id = safe_int(snapshot.get("lastUpdateId"))
            if last_update_id is None:
                raise ValueError("binance depth snapshot missing lastUpdateId")

            bids = [(safe_float(price), safe_float(size)) for price, size in snapshot.get("bids", [])]
            asks = [(safe_float(price), safe_float(size)) for price, size in snapshot.get("asks", [])]
            with self.lock:
                buffered_events = list(self.binance_depth_buffer)
                self.binance_depth_buffer.clear()
                self._replace_orderbook_locked("binance", bids, asks)
                previous_u = last_update_id
                for event in sorted(buffered_events, key=lambda item: (int(item["U"]), int(item["u"]))):
                    if int(event["u"]) <= last_update_id:
                        continue
                    if int(event["U"]) > previous_u + 1:
                        self.binance_depth_synced = False
                        self.binance_depth_last_u = previous_u
                        self.binance_depth_bootstrapping = False
                        self.binance_depth_proxy_active = bool(self.orderbooks["binance"])
                        self.binance_depth_bootstrap_retry_at_ms = int(time.time() * 1000) + 5_000
                        self.binance_depth_last_bootstrap_error = "binance depth bootstrap gap detected"
                        return
                    self._apply_orderbook_delta_locked("binance", event["bids"], event["asks"])
                    previous_u = int(event["u"])

                self.binance_depth_last_u = previous_u
                self.binance_depth_synced = True
                self.binance_depth_bootstrapping = False
                self.binance_depth_proxy_active = False
                self.binance_depth_bootstrap_retry_at_ms = 0
                self.binance_depth_last_bootstrap_error = None
        except Exception as exc:
            retry_ms = self._bootstrap_retry_ms_from_error(exc)
            with self.lock:
                self.binance_depth_synced = False
                self.binance_depth_bootstrapping = False
                self.binance_depth_bootstrap_retry_at_ms = int(time.time() * 1000) + retry_ms
                self.binance_depth_last_bootstrap_error = str(exc)
                latest_event = self.binance_depth_buffer[-1] if self.binance_depth_buffer else None
                if isinstance(latest_event, dict):
                    self._apply_binance_depth_proxy_locked(latest_event, reset=not self.orderbooks["binance"])

    def _handle_okx_message(self, symbol: str, payload: Dict[str, object]) -> None:
        if payload.get("event"):
            return
        arg = payload.get("arg") or {}
        data_list = payload.get("data") or [{}]
        data = data_list[0]
        channel = arg.get("channel")
        if channel == "tickers":
            last_price = safe_float(data.get("last"))
            base_volume = safe_float(data.get("volCcy24h"))
            notional_volume = None
            if base_volume is not None and last_price is not None:
                notional_volume = base_volume * last_price
            self._update_live_snapshot(
                "okx",
                symbol,
                last_price=last_price,
                volume_24h_base=base_volume,
                volume_24h_notional=notional_volume,
                timestamp_ms=safe_int(data.get("ts")) or int(time.time() * 1000),
            )
        elif channel == "mark-price":
            self._update_live_snapshot(
                "okx",
                symbol,
                mark_price=safe_float(data.get("markPx")),
                timestamp_ms=safe_int(data.get("ts")) or int(time.time() * 1000),
            )
        elif channel == "books5":
            bids = [(safe_float(row[0]), safe_float(row[1])) for row in data.get("bids", [])]
            asks = [(safe_float(row[0]), safe_float(row[1])) for row in data.get("asks", [])]
            with self.lock:
                self._replace_orderbook_locked("okx", bids, asks)
        elif channel == "trades":
            with self.lock:
                for item in data_list:
                    price = safe_float(item.get("px"))
                    size = safe_float(item.get("sz"))
                    event = TradeEvent(
                        exchange=self.clients["okx"].exchange_name,
                        symbol=item.get("instId") or symbol,
                        timestamp_ms=safe_int(item.get("ts")) or int(time.time() * 1000),
                        side=normalize_trade_side(item.get("side")),
                        price=price,
                        size=size,
                        notional=compute_notional(price, size),
                        source="ws",
                        raw=item,
                    )
                    self._append_trade_event_locked("okx", event)
        elif channel == "liquidation-orders":
            with self.lock:
                for item in _extract_okx_liquidation_rows(data_list):
                    price = safe_float(item.get("bkPx")) or safe_float(item.get("px")) or safe_float(item.get("price"))
                    size = safe_float(item.get("sz")) or safe_float(item.get("size"))
                    event = LiquidationEvent(
                        exchange=self.clients["okx"].exchange_name,
                        symbol=item.get("instId") or symbol,
                        timestamp_ms=safe_int(item.get("ts")) or int(time.time() * 1000),
                        side=normalize_liquidation_side(item.get("posSide") or item.get("side")),
                        price=price,
                        size=size,
                        notional=compute_notional(price, size),
                        source="ws",
                        raw=item,
                    )
                    self._append_liquidation_event_locked("okx", event)

    def _handle_hyperliquid_message(self, symbol: str, payload: Dict[str, object]) -> None:
        channel = payload.get("channel")
        if channel == "allMids":
            mids = (payload.get("data") or {}).get("mids") or {}
            self._update_live_snapshot(
                "hyperliquid",
                symbol,
                last_price=safe_float(mids.get(symbol)),
                timestamp_ms=int(time.time() * 1000),
            )
        elif channel == "l2Book":
            data = payload.get("data") or {}
            levels = data.get("levels", [[], []])
            bids = [(safe_float(item.get("px")), safe_float(item.get("sz"))) for item in levels[0]]
            asks = [(safe_float(item.get("px")), safe_float(item.get("sz"))) for item in levels[1]]
            with self.lock:
                self._replace_orderbook_locked("hyperliquid", bids, asks)
        elif channel == "trades":
            items = payload.get("data") or []
            with self.lock:
                for item in items:
                    price = safe_float(item.get("px"))
                    size = safe_float(item.get("sz"))
                    event = TradeEvent(
                        exchange=self.clients["hyperliquid"].exchange_name,
                        symbol=item.get("coin") or symbol,
                        timestamp_ms=safe_int(item.get("time")) or int(time.time() * 1000),
                        side=normalize_trade_side(item.get("side")),
                        price=price,
                        size=size,
                        notional=compute_notional(price, size),
                        source="ws",
                        raw=item,
                    )
                    self._append_trade_event_locked("hyperliquid", event)

    def _handle_bitget_message(self, symbol: str, payload: Dict[str, object]) -> None:
        arg = payload.get("arg") or {}
        if not isinstance(arg, dict):
            return
        topic = str(arg.get("topic") or "").strip().lower()
        if topic in {"ticker", "tickers"}:
            data_list = payload.get("data") or []
            data = data_list[0] if isinstance(data_list, list) and data_list else {}
            if not isinstance(data, dict):
                return
            price = safe_float(data.get("lastPr")) or safe_float(data.get("last"))
            open_interest = safe_float(data.get("holdingAmount"))
            open_interest_notional = safe_float(data.get("holdingAmountUsdt"))
            if open_interest_notional is None and open_interest is not None and price is not None:
                open_interest_notional = open_interest * price
            self._update_live_snapshot(
                "bitget",
                str(arg.get("symbol") or symbol),
                last_price=price,
                mark_price=safe_float(data.get("markPrice")),
                index_price=safe_float(data.get("indexPrice")),
                open_interest=open_interest,
                open_interest_notional=open_interest_notional,
                funding_rate=safe_float(data.get("fundingRate")),
                volume_24h_base=safe_float(data.get("baseVolume")),
                volume_24h_notional=safe_float(data.get("usdtVolume")) or safe_float(data.get("quoteVolume")),
                timestamp_ms=safe_int(data.get("ts")) or safe_int(payload.get("ts")) or int(time.time() * 1000),
            )
            return
        if topic in {"books50", "books"}:
            data_list = payload.get("data") or []
            data = data_list[0] if isinstance(data_list, list) and data_list else {}
            if not isinstance(data, dict):
                return
            bids = [(safe_float(item[0]), safe_float(item[1])) for item in data.get("bids", []) if isinstance(item, (list, tuple)) and len(item) >= 2]
            asks = [(safe_float(item[0]), safe_float(item[1])) for item in data.get("asks", []) if isinstance(item, (list, tuple)) and len(item) >= 2]
            with self.lock:
                self._replace_orderbook_locked("bitget", bids, asks)
            return
        if topic in {"publictrade", "trade", "trades"}:
            items = payload.get("data") or []
            with self.lock:
                for item in items if isinstance(items, list) else []:
                    if not isinstance(item, dict):
                        continue
                    price = safe_float(item.get("price"))
                    size = safe_float(item.get("size"))
                    event = TradeEvent(
                        exchange=self.clients["bitget"].exchange_name,
                        symbol=item.get("symbol") or arg.get("symbol") or symbol,
                        timestamp_ms=safe_int(item.get("ts")) or safe_int(item.get("timestamp")) or int(time.time() * 1000),
                        side=normalize_trade_side(item.get("side")),
                        price=price,
                        size=size,
                        notional=compute_notional(price, size),
                        source="ws",
                        raw=item,
                    )
                    self._append_trade_event_locked("bitget", event)
            return
        if topic == "liquidation":
            items = payload.get("data") or []
            target_symbol = str(symbol or "").strip().upper()
            with self.lock:
                for item in items if isinstance(items, list) else []:
                    if not isinstance(item, dict):
                        continue
                    event_symbol = str(item.get("symbol") or item.get("instId") or "").strip().upper()
                    if target_symbol and event_symbol and event_symbol != target_symbol:
                        continue
                    price = safe_float(item.get("price"))
                    notional = safe_float(item.get("amount")) or safe_float(item.get("size")) or safe_float(item.get("notional"))
                    size = safe_float(item.get("sz")) or safe_float(item.get("size"))
                    if size is None and price not in (None, 0) and notional is not None:
                        size = float(notional) / float(price)
                    event = LiquidationEvent(
                        exchange=self.clients["bitget"].exchange_name,
                        symbol=event_symbol or target_symbol or symbol,
                        timestamp_ms=safe_int(item.get("ts")) or safe_int(item.get("timestamp")) or int(time.time() * 1000),
                        side=normalize_liquidation_side_for_exchange("bitget", item.get("side") or item.get("holdSide") or item.get("positionSide")),
                        price=price,
                        size=size,
                        notional=notional or compute_notional(price, size),
                        source="ws",
                        raw=item,
                    )
                    self._append_liquidation_event_locked("bitget", event)

    def _handle_spot_message(self, exchange_key: str, symbol: str, payload: Dict[str, object]) -> None:
        if exchange_key == "binance":
            stream = str(payload.get("stream", ""))
            data = payload.get("data") or {}
            if "@aggtrade" in stream.lower():
                price = safe_float(data.get("p"))
                size = safe_float(data.get("q"))
                event = TradeEvent(
                    exchange=self.spot_clients["binance"].exchange_name,
                    symbol=data.get("s") or symbol,
                    timestamp_ms=safe_int(data.get("E")) or safe_int(data.get("T")) or int(time.time() * 1000),
                    side="sell" if bool(data.get("m")) else "buy",
                    price=price,
                    size=size,
                    notional=compute_notional(price, size),
                    source="ws",
                    raw=data,
                )
                with self.lock:
                    self._append_spot_trade_event_locked("binance", event)
                return
            if "@depth@" in stream.lower():
                self._handle_spot_depth("binance", symbol, data)
                return
            if "@ticker" in stream.lower():
                self._update_spot_snapshot(
                    "binance",
                    symbol,
                    last_price=safe_float(data.get("c")),
                    volume_24h_base=safe_float(data.get("v")),
                    volume_24h_notional=safe_float(data.get("q")),
                    timestamp_ms=safe_int(data.get("E")) or int(time.time() * 1000),
                )
            return

        if exchange_key == "bybit":
            topic = str(payload.get("topic", ""))
            if payload.get("success") is not None:
                return
            if topic.startswith("orderbook"):
                self._handle_bybit_spot_orderbook(payload)
            elif topic.startswith("publicTrade"):
                self._handle_bybit_spot_trade(symbol, payload)
            elif topic.startswith("tickers"):
                self._handle_bybit_spot_ticker(symbol, payload)
            return

        if exchange_key == "okx":
            self._handle_okx_spot_message(symbol, payload)

    def _handle_spot_depth(self, exchange_key: str, symbol: str, data: Dict[str, object]) -> None:
        event = {
            "U": safe_int(data.get("U")),
            "u": safe_int(data.get("u")),
            "bids": [(safe_float(price), safe_float(size)) for price, size in data.get("b", [])],
            "asks": [(safe_float(price), safe_float(size)) for price, size in data.get("a", [])],
        }
        self._apply_binance_spot_depth_event(exchange_key, symbol, event)

    def _bootstrap_spot_depth(self, exchange_key: str, symbol: str) -> None:
        try:
            snapshot = fetch_binance_spot_orderbook_snapshot(symbol, 1000, timeout=self.timeout)
            last_update_id = safe_int(snapshot.get("lastUpdateId"))
            if last_update_id is None:
                raise ValueError("binance spot depth snapshot missing lastUpdateId")
            bids = [(safe_float(price), safe_float(size)) for price, size in snapshot.get("bids", [])]
            asks = [(safe_float(price), safe_float(size)) for price, size in snapshot.get("asks", [])]
            with self.lock:
                buffered_events = list(self.binance_spot_depth_buffer)
                self.binance_spot_depth_buffer.clear()
                self._replace_spot_orderbook_locked(exchange_key, bids, asks)
                previous_u = last_update_id
                for event in sorted(buffered_events, key=lambda item: (int(item["U"]), int(item["u"]))):
                    if int(event["u"]) <= last_update_id:
                        continue
                    if int(event["U"]) > previous_u + 1:
                        self.binance_spot_depth_synced = False
                        self.binance_spot_depth_last_u = None
                        self.binance_spot_depth_bootstrapping = False
                        return
                    self._apply_spot_orderbook_delta_locked(exchange_key, event["bids"], event["asks"])
                    previous_u = int(event["u"])
                self.binance_spot_depth_last_u = previous_u
                self.binance_spot_depth_synced = True
                self.binance_spot_depth_bootstrapping = False
        except Exception as exc:
            with self.lock:
                self.binance_spot_depth_synced = False
                self.binance_spot_depth_last_u = None
                self.binance_spot_depth_bootstrapping = False
            self._on_spot_error(exchange_key, symbol, exc)

    def _handle_bybit_spot_ticker(self, symbol: str, payload: Dict[str, object]) -> None:
        data = payload.get("data") or {}
        if not isinstance(data, dict):
            return
        self._update_spot_snapshot(
            "bybit",
            symbol,
            last_price=safe_float(data.get("lastPrice")),
            bid_price=safe_float(data.get("bid1Price")),
            ask_price=safe_float(data.get("ask1Price")),
            volume_24h_base=safe_float(data.get("volume24h")),
            volume_24h_notional=safe_float(data.get("turnover24h")),
            timestamp_ms=safe_int(payload.get("ts")) or int(time.time() * 1000),
        )

    def _handle_bybit_spot_orderbook(self, payload: Dict[str, object]) -> None:
        data = payload.get("data") or {}
        if not isinstance(data, dict):
            return
        bids = [(safe_float(price), safe_float(size)) for price, size in data.get("b", [])]
        asks = [(safe_float(price), safe_float(size)) for price, size in data.get("a", [])]
        with self.lock:
            if payload.get("type") == "snapshot" or not self.spot_orderbooks["bybit"]:
                self._replace_spot_orderbook_locked("bybit", bids, asks)
            else:
                self._apply_spot_orderbook_delta_locked("bybit", bids, asks)

    def _handle_bybit_spot_trade(self, symbol: str, payload: Dict[str, object]) -> None:
        items = payload.get("data") or []
        if isinstance(items, dict):
            items = [items]
        with self.lock:
            for item in items:
                price = safe_float(item.get("p")) or safe_float(item.get("price"))
                size = safe_float(item.get("v")) or safe_float(item.get("size"))
                event = TradeEvent(
                    exchange=self.spot_clients["bybit"].exchange_name,
                    symbol=item.get("s") or symbol,
                    timestamp_ms=safe_int(item.get("T")) or safe_int(item.get("time")) or int(time.time() * 1000),
                    side=normalize_trade_side(item.get("S") or item.get("side")),
                    price=price,
                    size=size,
                    notional=compute_notional(price, size),
                    source="ws",
                    raw=item,
                )
                self._append_spot_trade_event_locked("bybit", event)

    def _handle_okx_spot_message(self, symbol: str, payload: Dict[str, object]) -> None:
        if payload.get("event"):
            return
        arg = payload.get("arg") or {}
        data_list = payload.get("data") or [{}]
        data = data_list[0]
        channel = arg.get("channel")
        if channel == "tickers":
            self._update_spot_snapshot(
                "okx",
                symbol,
                last_price=safe_float(data.get("last")),
                bid_price=safe_float(data.get("bidPx")),
                ask_price=safe_float(data.get("askPx")),
                volume_24h_base=safe_float(data.get("vol24h")),
                volume_24h_notional=safe_float(data.get("volCcy24h")),
                timestamp_ms=safe_int(data.get("ts")) or int(time.time() * 1000),
            )
        elif channel == "books5":
            bids = [(safe_float(row[0]), safe_float(row[1])) for row in data.get("bids", [])]
            asks = [(safe_float(row[0]), safe_float(row[1])) for row in data.get("asks", [])]
            with self.lock:
                self._replace_spot_orderbook_locked("okx", bids, asks)
        elif channel == "trades":
            with self.lock:
                for item in data_list:
                    price = safe_float(item.get("px"))
                    size = safe_float(item.get("sz"))
                    event = TradeEvent(
                        exchange=self.spot_clients["okx"].exchange_name,
                        symbol=item.get("instId") or symbol,
                        timestamp_ms=safe_int(item.get("ts")) or int(time.time() * 1000),
                        side=normalize_trade_side(item.get("side")),
                        price=price,
                        size=size,
                        notional=compute_notional(price, size),
                        source="ws",
                        raw=item,
                    )
                    self._append_spot_trade_event_locked("okx", event)
