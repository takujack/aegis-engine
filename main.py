#!/usr/env python3
# -*- coding: utf-8 -*-
"""
AEGIS ULTIMATE PRO — Trend-Capture & Zero-Cooldown Variant.
- Scaled for ~13.2M JPY BTC price level (Stop: 15,000 JPY, TP: 35,000 JPY).
- Zero cooldown for maximum opportunity capture.
- Robust Startup & Position Aggregation.
"""
from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import logging
import os
import signal
import sqlite3
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, ROUND_DOWN
from typing import Any, Optional
from urllib.parse import urlencode

import aiohttp

LOG = logging.getLogger("aegis_ultimate_pro")
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)


class SafetyStop(RuntimeError):
    """Fail closed when exchange state is ambiguous or a safety invariant breaks."""


def D(value: Any) -> Decimal:
    return Decimal(str(value))


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def utc_day() -> str:
    return utc_now().date().isoformat()


def floor_step(value: Decimal, step: Decimal) -> Decimal:
    if step <= 0:
        raise ValueError("step must be positive")
    if value <= 0:
        return Decimal("0")
    return (value / step).to_integral_value(rounding=ROUND_DOWN) * step


@dataclass(frozen=True)
class Config:
    api_base: str = "https://api.bitflyer.com"
    product_code: str = "FX_BTC_JPY"
    mode: str = "PAPER"
    api_key: str = ""
    api_secret: str = ""
    enable_live_orders: bool = False

    starting_equity_jpy: Decimal = Decimal("10000")
    risk_fraction: Decimal = Decimal("0.025")
    daily_loss_fraction: Decimal = Decimal("0.30")
    max_spread_bps: Decimal = Decimal("15")
    max_board_fetch_seconds: Decimal = Decimal("3")
    loop_seconds: float = 2.0
    cooldown_seconds: float = 0.0
    stop_distance_jpy: Decimal = Decimal("15000")
    take_profit_distance_jpy: Decimal = Decimal("35000")
    cost_buffer_jpy_per_btc: Decimal = Decimal("100")
    min_size: Decimal = Decimal("0.001")
    size_step: Decimal = Decimal("0.001")
    max_position_size: Decimal = Decimal("0.05")
    max_consecutive_losses: int = 10
    min_available_collateral_jpy: Decimal = Decimal("500")
    order_poll_seconds: float = 0.3
    order_timeout_seconds: float = 12.0
    request_timeout_seconds: float = 5.0
    max_consecutive_errors: int = 8
    db_path: str = "aegis_ultimate_pro.sqlite3"
    signal_window_size: int = 2

    @classmethod
    def from_env(cls) -> "Config":
        d = cls()
        return cls(
            api_base=os.getenv("AEGIS_API_BASE", d.api_base),
            product_code=os.getenv("AEGIS_PRODUCT_CODE", d.product_code),
            mode=os.getenv("AEGIS_MODE", d.mode).strip().upper(),
            api_key=os.getenv("BF_API_KEY", ""),
            api_secret=os.getenv("BF_API_SECRET", ""),
            enable_live_orders=os.getenv("AEGIS_ENABLE_LIVE_ORDERS", "") == "YES_I_ACCEPT_RISK",
            starting_equity_jpy=D(os.getenv("AEGIS_STARTING_EQUITY_JPY", str(d.starting_equity_jpy))),
            risk_fraction=D(os.getenv("AEGIS_RISK_FRACTION", str(d.risk_fraction))),
            daily_loss_fraction=D(os.getenv("AEGIS_DAILY_LOSS_FRACTION", str(d.daily_loss_fraction))),
            max_spread_bps=D(os.getenv("AEGIS_MAX_SPREAD_BPS", str(d.max_spread_bps))),
            max_board_fetch_seconds=D(os.getenv("AEGIS_MAX_BOARD_FETCH_SECONDS", str(d.max_board_fetch_seconds))),
            loop_seconds=float(os.getenv("AEGIS_LOOP_SECONDS", str(d.loop_seconds))),
            cooldown_seconds=float(os.getenv("AEGIS_COOLDOWN_SECONDS", str(d.cooldown_seconds))),
            stop_distance_jpy=D(os.getenv("AEGIS_STOP_DISTANCE_JPY", str(d.stop_distance_jpy))),
            take_profit_distance_jpy=D(os.getenv("AEGIS_TAKE_PROFIT_DISTANCE_JPY", str(d.take_profit_distance_jpy))),
            cost_buffer_jpy_per_btc=D(os.getenv("AEGIS_COST_BUFFER_JPY_PER_BTC", str(d.cost_buffer_jpy_per_btc))),
            min_size=D(os.getenv("AEGIS_MIN_SIZE", str(d.min_size))),
            size_step=D(os.getenv("AEGIS_SIZE_STEP", str(d.size_step))),
            max_position_size=D(os.getenv("AEGIS_MAX_POSITION_SIZE", str(d.max_position_size))),
            max_consecutive_losses=int(os.getenv("AEGIS_MAX_CONSECUTIVE_LOSSES", str(d.max_consecutive_losses))),
            min_available_collateral_jpy=D(os.getenv("AEGIS_MIN_AVAILABLE_COLLATERAL_JPY", str(d.min_available_collateral_jpy))),
            order_poll_seconds=float(os.getenv("AEGIS_ORDER_POLL_SECONDS", str(d.order_poll_seconds))),
            order_timeout_seconds=float(os.getenv("AEGIS_ORDER_TIMEOUT_SECONDS", str(d.order_timeout_seconds))),
            request_timeout_seconds=float(os.getenv("AEGIS_REQUEST_TIMEOUT_SECONDS", str(d.request_timeout_seconds))),
            max_consecutive_errors=int(os.getenv("AEGIS_MAX_CONSECUTIVE_ERRORS", str(d.max_consecutive_errors))),
            db_path=os.getenv("AEGIS_DB_PATH", d.db_path),
            signal_window_size=int(os.getenv("AEGIS_SIGNAL_WINDOW_SIZE", str(d.signal_window_size))),
        )

    def validate(self) -> None:
        if self.product_code != "FX_BTC_JPY":
            raise SafetyStop("This build is restricted to FX_BTC_JPY.")
        if self.mode not in {"PAPER", "LIVE"}:
            raise SafetyStop("AEGIS_MODE must be PAPER or LIVE.")
        if self.starting_equity_jpy <= 0:
            raise SafetyStop("starting_equity_jpy must be positive.")
        if not (Decimal("0") < self.risk_fraction <= Decimal("0.05")):
            raise SafetyStop("risk_fraction must be in (0, 0.05].")
        if not (Decimal("0") < self.daily_loss_fraction <= Decimal("0.40")):
            raise SafetyStop("daily_loss_fraction must be in (0, 0.40].")
        if self.stop_distance_jpy <= 0 or self.take_profit_distance_jpy <= 0:
            raise SafetyStop("Stop and take-profit distances must be positive.")
        if self.cost_buffer_jpy_per_btc < 0:
            raise SafetyStop("Cost buffer cannot be negative.")
        if self.min_size <= 0 or self.size_step <= 0 or self.max_position_size < self.min_size:
            raise SafetyStop("Invalid order size configuration.")
        if self.max_consecutive_losses < 1:
            raise SafetyStop("max_consecutive_losses must be >= 1.")
        if self.mode == "LIVE":
            if not self.enable_live_orders:
                raise SafetyStop("LIVE requires AEGIS_ENABLE_LIVE_ORDERS=YES_I_ACCEPT_RISK.")
            if not self.api_key or not self.api_secret:
                raise SafetyStop("LIVE API credentials missing.")


class StateStore:
    def __init__(self, path: str):
        self.db = sqlite3.connect(path, timeout=10)
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=FULL")
        self.db.execute("""CREATE TABLE IF NOT EXISTS events(
            id INTEGER PRIMARY KEY AUTOINCREMENT, ts_utc TEXT NOT NULL,
            event TEXT NOT NULL, details TEXT NOT NULL)""")
        self.db.execute("""CREATE TABLE IF NOT EXISTS state(
            key TEXT PRIMARY KEY, value TEXT NOT NULL)""")
        self.db.execute("""CREATE TABLE IF NOT EXISTS paper_trades(
            id INTEGER PRIMARY KEY AUTOINCREMENT, ts_utc TEXT NOT NULL,
            closed_ts_utc TEXT, side TEXT NOT NULL, size TEXT NOT NULL,
            entry_price TEXT NOT NULL, exit_price TEXT, status TEXT NOT NULL, pnl TEXT)""")
        cols = {r[1] for r in self.db.execute("PRAGMA table_info(paper_trades)")}
        if "closed_ts_utc" not in cols:
            self.db.execute("ALTER TABLE paper_trades ADD COLUMN closed_ts_utc TEXT")
        self.db.commit()

    def event(self, name: str, details: dict[str, Any], level: str = "INFO") -> None:
        self.db.execute(
            "INSERT INTO events(ts_utc,event,details) VALUES(?,?,?)",
            (utc_now().isoformat(), name,
             json.dumps({"level": level, **details}, ensure_ascii=False, default=str)),
        )
        self.db.commit()

    def get(self, key: str, default: Any = None) -> Any:
        row = self.db.execute("SELECT value FROM state WHERE key=?", (key,)).fetchone()
        return json.loads(row[0]) if row else default

    def set(self, key: str, value: Any) -> None:
        self.db.execute(
            "INSERT INTO state(key,value) VALUES(?,?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, json.dumps(value, ensure_ascii=False, default=str)),
        )
        self.db.commit()

    def clear_ambiguous_flags(self) -> None:
        self.set("live_order_ambiguous", False)
        self.set("live_order_intent", None)

    def close(self) -> None:
        self.db.close()


class BitflyerClient:
    def __init__(self, session: aiohttp.ClientSession, cfg: Config):
        self.session, self.cfg = session, cfg

    async def request(self, method: str, path: str, *,
                      params: Optional[dict[str, Any]] = None,
                      payload: Optional[dict[str, Any]] = None,
                      private: bool = False) -> Any:
        query = urlencode(sorted((params or {}).items()))
        signed_path = path + (("?" + query) if query else "")
        body = json.dumps(payload, separators=(",", ":"), ensure_ascii=False) if payload is not None else ""
        headers: dict[str, str] = {}
        if private:
            timestamp = str(time.time())
            message = timestamp + method.upper() + signed_path + body
            signature = hmac.new(self.cfg.api_secret.encode(), message.encode(), hashlib.sha256).hexdigest()
            headers = {"ACCESS-KEY": self.cfg.api_key, "ACCESS-TIMESTAMP": timestamp,
                       "ACCESS-SIGN": signature, "Content-Type": "application/json"}
        url = self.cfg.api_base.rstrip("/") + signed_path

        max_retries = 4
        for attempt in range(max_retries):
            try:
                async with self.session.request(
                    method.upper(), url, data=body if payload is not None else None,
                    headers=headers, timeout=aiohttp.ClientTimeout(total=self.cfg.request_timeout_seconds),
                ) as resp:
                    raw = await resp.text()
                    if resp.status == 429:
                        if attempt < max_retries - 1:
                            wait_time = 10.0 * (attempt + 1)
                            LOG.warning("HTTP 429 Rate Limit. Waiting %.1fs...", wait_time)
                            await asyncio.sleep(wait_time)
                            continue
                        else:
                            raise SafetyStop(f"HTTP 429 Rate Limit exceeded on {path}")
                    if resp.status >= 400:
                        if method.upper() == "POST" and path.endswith("/sendchildorder"):
                            raise SafetyStop(
                                f"Order POST HTTP {resp.status}; reconcile before retry: {raw[:250]}"
                            )
                        raise SafetyStop(f"HTTP {resp.status} on {method} {path}: {raw[:250]}")
                    try:
                        return json.loads(raw)
                    except json.JSONDecodeError as exc:
                        raise SafetyStop(f"Invalid JSON from {path}") from exc
            except SafetyStop:
                raise
            except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                if attempt < max_retries - 1:
                    wait_time = 3.0 * (attempt + 1)
                    LOG.warning("Transport failure on %s %s. Retrying in %.1fs...", method, path, wait_time)
                    await asyncio.sleep(wait_time)
                    continue
                raise SafetyStop(f"Transport failure on {method} {path}: {exc}") from exc

    async def board(self) -> tuple[dict[str, Any], float]:
        t0 = time.monotonic()
        data = await self.request("GET", "/v1/board", params={"product_code": self.cfg.product_code})
        return data, time.monotonic() - t0

    async def collateral(self) -> dict[str, Any]:
        return await self.request("GET", "/v1/me/getcollateral", private=True)

    async def positions(self) -> list[dict[str, Any]]:
        data = await self.request("GET", "/v1/me/getpositions",
                                  params={"product_code": self.cfg.product_code}, private=True)
        if not isinstance(data, list):
            raise SafetyStop("Unexpected positions response.")
        return data

    async def active_orders(self) -> list[dict[str, Any]]:
        data = await self.request("GET", "/v1/me/getchildorders", params={
            "product_code": self.cfg.product_code, "child_order_state": "ACTIVE", "count": 100
        }, private=True)
        if not isinstance(data, list):
            raise SafetyStop("Unexpected active-orders response.")
        return data

    async def order_by_acceptance_id(self, acceptance_id: str) -> list[dict[str, Any]]:
        data = await self.request("GET", "/v1/me/getchildorders", params={
            "product_code": self.cfg.product_code,
            "child_order_acceptance_id": acceptance_id, "count": 100
        }, private=True)
        if not isinstance(data, list):
            raise SafetyStop("Unexpected child-orders response.")
        return data

    async def executions_by_acceptance_id(self, acceptance_id: str) -> list[dict[str, Any]]:
        data = await self.request("GET", "/v1/me/getexecutions", params={
            "product_code": self.cfg.product_code,
            "child_order_acceptance_id": acceptance_id, "count": 100
        }, private=True)
        if not isinstance(data, list):
            raise SafetyStop("Unexpected executions response.")
        return data

    async def send_market(self, side: str, size: Decimal) -> dict[str, Any]:
        payload = {"product_code": self.cfg.product_code, "child_order_type": "MARKET",
                   "side": side, "size": float(size)}
        return await self.request("POST", "/v1/me/sendchildorder", private=True, payload=payload)


def parse_board(raw: dict[str, Any]) -> dict[str, Decimal]:
    bids, asks = raw.get("bids") or [], raw.get("asks") or []
    if not bids or not asks:
        raise SafetyStop("Order book has no bids or asks.")
    bid, ask = D(bids[0]["price"]), D(asks[0]["price"])
    if bid <= 0 or ask < bid:
        raise SafetyStop(f"Invalid market quotes bid={bid}, ask={ask}")
    bid_depth = sum((D(x.get("size", 0)) for x in bids[:5]), Decimal("0"))
    ask_depth = sum((D(x.get("size", 0)) for x in asks[:5]), Decimal("0"))
    total = bid_depth + ask_depth
    imbalance = (bid_depth - ask_depth) / total if total > 0 else Decimal("0")
    mid = (bid + ask) / Decimal("2")
    spread_bps = (ask - bid) / mid * Decimal("10000")
    total_volume = bid_depth + ask_depth
    return {"bid": bid, "ask": ask, "mid": mid, "spread_bps": spread_bps, "imbalance": imbalance, "volume": total_volume}


class AegisUltimatePro:
    def __init__(self, cfg: Config, store: StateStore, client: BitflyerClient):
        self.cfg, self.store, self.client = cfg, store, client
        self.running = True
        self.signal_window: list[Decimal] = []
        self.recent_mids: list[Decimal] = []
        self.recent_volumes: list[Decimal] = []
        self.consecutive_losses = int(store.get("consecutive_losses", 0))
        self.live_block_new_entries = False

    async def reconcile_on_startup(self) -> None:
        if self.cfg.mode != "LIVE":
            return
        LOG.info("Performing startup exchange reconciliation check...")
        try:
            active_orders = await self.client.active_orders()
            positions = await self.client.positions()
            real_positions = [p for p in positions if D(p.get("size", 0)) > 0]

            if not active_orders and not real_positions:
                LOG.info("No active orders or positions found. Clearing flags.")
                self.store.clear_ambiguous_flags()
            else:
                if self.store.get("live_order_ambiguous", False):
                    raise SafetyStop("Ambiguous prior LIVE order with active exposure. Reconcile manually.")
        except SafetyStop:
            raise
        except Exception as e:
            LOG.warning("Startup reconciliation check warning: %s", e)

    @staticmethod
    def _rolling_std(values: list[Decimal]) -> Decimal:
        if len(values) < 2:
            return Decimal("0")
        floats = [float(v) for v in values]
        mean_val = sum(floats) / len(floats)
        variance = sum((x - mean_val) ** 2 for x in floats) / len(floats)
        return Decimal(str(variance ** 0.5))

    def signal(self, book: dict[str, Decimal]) -> Optional[str]:
        if book["spread_bps"] > self.cfg.max_spread_bps:
            return None

        self.signal_window.append(book["imbalance"])
        self.signal_window = self.signal_window[-self.cfg.signal_window_size:]
        self.recent_mids.append(book["mid"])
        self.recent_mids = self.recent_mids[-15:]
        self.recent_volumes.append(book["volume"])
        self.recent_volumes = self.recent_volumes[-15:]

        if len(self.signal_window) < self.cfg.signal_window_size:
            return None
        avg_imb = sum(self.signal_window, Decimal("0")) / Decimal(len(self.signal_window))

        if len(self.recent_mids) < 5:
            return None
        momentum = (self.recent_mids[-1] - self.recent_mids[-5]) / self.recent_mids[-5]

        imb_std = self._rolling_std(self.signal_window) or Decimal("0.05")
        dynamic_th = max(Decimal("0.06"), imb_std * Decimal("1.2"))

        vol_spike = False
        if len(self.recent_volumes) >= 5:
            avg_vol = sum(self.recent_volumes[-5:], Decimal("0")) / Decimal(5)
            if self.recent_volumes[-1] > avg_vol * Decimal("1.4"):
                vol_spike = True

        strong_buy = (
            avg_imb >= dynamic_th and
            momentum > Decimal("0.00010") and
            (vol_spike or avg_imb >= dynamic_th * Decimal("1.2"))
        )
        strong_sell = (
            avg_imb <= -dynamic_th and
            momentum < Decimal("-0.00010") and
            (vol_spike or avg_imb <= -dynamic_th * Decimal("1.2"))
        )

        if strong_buy:
            return "BUY"
        if strong_sell:
            return "SELL"
        return None

    def size_for_equity(self, equity: Decimal, mid_price: Decimal) -> Decimal:
        if equity <= 0 or mid_price <= 0:
            return Decimal("0")
        
        max_allowable = floor_step((equity * Decimal("0.9")) / (mid_price / Decimal("2")), self.cfg.size_step)
        if max_allowable < self.cfg.min_size:
            if equity < (mid_price * self.cfg.min_size / Decimal("2")):
                return Decimal("0")
            return self.cfg.min_size
        return min(max_allowable, self.cfg.max_position_size)

    async def wait_order_terminal(self, acceptance_id: str) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        deadline = time.monotonic() + self.cfg.order_timeout_seconds
        while time.monotonic() < deadline:
            orders = await self.client.order_by_acceptance_id(acceptance_id)
            executions = await self.client.executions_by_acceptance_id(acceptance_id)
            states = {str(x.get("child_order_state", "")).upper() for x in orders}
            if states and states.issubset({"COMPLETED", "CANCELED", "EXPIRED", "REJECTED"}):
                return orders, executions
            await asyncio.sleep(self.cfg.order_poll_seconds)
        self.store.set("live_order_ambiguous", True)
        self.store.set("live_order_acceptance_id", acceptance_id)
        raise SafetyStop(f"Order {acceptance_id} timed out; halted.")

    @staticmethod
    def execution_summary(executions: list[dict[str, Any]]) -> tuple[Decimal, Decimal]:
        qty = Decimal("0")
        notional = Decimal("0")
        for e in executions:
            q = D(e.get("size", 0))
            p = D(e.get("price", 0))
            if q > 0 and p > 0:
                qty += q
                notional += q * p
        return qty, (notional / qty if qty else Decimal("0"))

    async def submit_live(self, side: str, size: Decimal, purpose: str) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        if side not in {"BUY", "SELL"}:
            raise SafetyStop("Invalid order side.")
        size = min(floor_step(size, self.cfg.size_step), self.cfg.max_position_size)
        if size < self.cfg.min_size:
            raise SafetyStop(f"Calculated size {size} below min_size.")
        intent = {"side": side, "size": str(size), "purpose": purpose,
                  "status": "SUBMITTING", "ts": utc_now().isoformat()}
        self.store.set("live_order_intent", intent)
        
        try:
            response = await self.client.send_market(side, size)
        except Exception as exc:
            await asyncio.sleep(1.5)
            active = await self.client.active_orders()
            pos = await self.client.positions()
            if not active and not any(D(p.get("size", 0)) > 0 for p in pos):
                self.store.set("live_order_intent", None)
                raise SafetyStop("Order submission failed cleanly.") from exc

            self.store.set("live_order_ambiguous", True)
            raise SafetyStop("Order submission outcome unknown; reconcile manually.") from exc

        acceptance_id = response.get("child_order_acceptance_id") if isinstance(response, dict) else None
        if not acceptance_id:
            self.store.set("live_order_ambiguous", True)
            raise SafetyStop("No acceptance ID; manual reconciliation required.")
        orders, executions = await self.wait_order_terminal(str(acceptance_id))
        states = {str(x.get("child_order_state", "")).upper() for x in orders}
        filled, avg_price = self.execution_summary(executions)
        if "REJECTED" in states or filled <= 0 or filled + self.cfg.size_step / 2 < size:
            self.store.set("live_order_ambiguous", True)
            raise SafetyStop("Rejected/partial fill; stopped for manual reconciliation.")
        self.store.set("live_order_intent", {
            **intent, "status": "TERMINAL", "acceptance_id": acceptance_id,
            "filled_size": str(filled), "average_price": str(avg_price),
        })
        return orders, executions

    async def live_cycle(self, book: dict[str, Decimal]) -> None:
        active = await self.client.active_orders()
        if active:
            raise SafetyStop("Active exchange order found; reconcile before continuing.")
        
        positions_raw = await self.client.positions()
        positions = [p for p in positions_raw if D(p.get("size", 0)) > 0]

        net_side: Optional[str] = None
        total_size = Decimal("0")
        weighted_price_sum = Decimal("0")

        if positions:
            sides = {str(p.get("side", "")).upper() for p in positions}
            if len(sides) > 1:
                raise SafetyStop("Conflicting position sides found on exchange.")
            net_side = sides.pop()
            
            for p in positions:
                p_size = D(p.get("size", 0))
                p_price = D(p.get("price", 0))
                total_size += p_size
                weighted_price_sum += p_size * p_price
            
            avg_entry_price = weighted_price_sum / total_size if total_size > 0 else Decimal("0")
        else:
            avg_entry_price = Decimal("0")

        collateral = await self.client.collateral()
        collateral_jpy = D(collateral.get("collateral", 0))
        open_pnl = D(collateral.get("open_position_pnl", 0))
        required = D(collateral.get("require_collateral", 0))
        equity = collateral_jpy + open_pnl
        if equity <= 0:
            raise SafetyStop("Exchange-reported equity is non-positive.")
        available = collateral_jpy - required
        if available < self.cfg.min_available_collateral_jpy:
            raise SafetyStop("Available collateral is below configured minimum.")

        day_key = "live_day:" + utc_day()
        baseline = self.store.get(day_key)
        if baseline is None:
            baseline = str(equity)
            self.store.set(day_key, baseline)
        daily_limit = D(baseline) * self.cfg.daily_loss_fraction
        daily_loss_hit = equity <= D(baseline) - daily_limit

        if net_side and total_size > 0:
            move = book["bid"] - avg_entry_price if net_side == "BUY" else avg_entry_price - book["ask"]
            reason = "daily_loss" if daily_loss_hit else (
                "stop" if move <= -self.cfg.stop_distance_jpy else (
                    "take_profit" if move >= self.cfg.take_profit_distance_jpy else None))
            if reason:
                close_side = "SELL" if net_side == "BUY" else "BUY"
                latest_raw = await self.client.positions()
                latest_pos = [p for p in latest_raw if D(p.get("size", 0)) > 0]
                latest_size = sum((D(p.get("size", 0)) for p in latest_pos), Decimal("0"))
                
                if latest_size <= 0:
                    raise SafetyStop("Position closed before exit execution.")
                
                await self.submit_live(close_side, latest_size, "exit:" + reason)
                after = [x for x in await self.client.positions() if D(x.get("size", 0)) > 0]
                if after:
                    raise SafetyStop("Exposure remains after exit; manual reconciliation required.")
                self.store.event("LIVE_EXIT_VERIFIED", {
                    "reason": reason, "total_closed_size": str(latest_size),
                }, "WARNING")
                self.live_block_new_entries = reason == "daily_loss"
            return

        if daily_loss_hit:
            self.live_block_new_entries = True
            return
        if self.live_block_new_entries:
            return
        if self.consecutive_losses >= self.cfg.max_consecutive_losses:
            raise SafetyStop("Maximum consecutive losses reached.")
            
        side = self.signal(book)
        if not side:
            return
        size = self.size_for_equity(equity, book["mid"])
        if size < self.cfg.min_size:
            return
        if await self.client.active_orders() or await self.client.positions():
            raise SafetyStop("Exchange state changed before entry.")
        await self.submit_live(side, size, "entry")
        after = [p for p in await self.client.positions() if D(p.get("size", 0)) > 0]
        if not after or any(str(p.get("side", "")).upper() != side for p in after):
            raise SafetyStop("Entry does not reconcile with exchange positions.")

    async def paper_cycle(self, book: dict[str, Decimal]) -> None:
        pass

    async def run(self) -> None:
        errors = 0
        self.store.event("START", {"mode": self.cfg.mode, "product": self.cfg.product_code})
        LOG.info("Aegis Ultimate Pro started in %s mode (Trend-Capture & Zero-Cooldown).", self.cfg.mode)
        
        await self.reconcile_on_startup()

        while self.running:
            t0 = time.monotonic()
            try:
                raw, latency = await self.client.board()
                if latency > float(self.cfg.max_board_fetch_seconds):
                    raise SafetyStop(f"Board fetch latency too high: {latency:.3f}s")
                book = parse_board(raw)
                if self.cfg.mode == "LIVE":
                    await self.live_cycle(book)
                errors = 0
            except asyncio.CancelledError:
                raise
            except SafetyStop as exc:
                self.store.event("SAFETY_STOP", {"error": str(exc)}, "ERROR")
                LOG.error("SAFETY STOP: %s", exc)
                self.running = False
            except Exception as exc:
                errors += 1
                LOG.exception("Cycle failed (%s/%s)", errors, self.cfg.max_consecutive_errors)
                if errors >= self.cfg.max_consecutive_errors:
                    self.running = False
                else:
                    await asyncio.sleep(min(2.0 * errors, 10.0))
            elapsed = time.monotonic() - t0
            if self.running and elapsed < self.cfg.loop_seconds:
                await asyncio.sleep(self.cfg.loop_seconds - elapsed)

        self.store.event("ENGINE_STOP", {"mode": self.cfg.mode})
        LOG.info("Aegis stopped.")


async def async_main() -> int:
    cfg = Config.from_env()
    cfg.validate()
    store = StateStore(cfg.db_path)
    timeout = aiohttp.ClientTimeout(total=cfg.request_timeout_seconds)
    try:
        async with aiohttp.ClientSession(timeout=timeout) as session:
            client = BitflyerClient(session, cfg)
            engine = AegisUltimatePro(cfg, store, client)
            loop = asyncio.get_running_loop()
            for sig in (signal.SIGINT, signal.SIGTERM):
                try:
                    loop.add_signal_handler(sig, setattr, engine, "running", False)
                except (NotImplementedError, RuntimeError):
                    pass
            await engine.run()
    finally:
        store.close()
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(asyncio.run(async_main()))
    except SafetyStop as exc:
        LOG.error("Aegis refused to start/continue: %s", exc)
        raise SystemExit(2)
