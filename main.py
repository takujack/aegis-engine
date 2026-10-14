#!/usr/env python3
# -*- coding: utf-8 -*-
"""
AEGIS ULTIMATE PRO — bitFlyer FX_BTC_JPY trend engine.
Resilient Worker: Prevents non-fatal SafetyStops from stopping the engine.
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
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, ROUND_DOWN
from typing import Any, Optional
from urllib.parse import urlencode

import aiohttp

LOG = logging.getLogger("aegis")
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)

ZERO = Decimal("0")
ONE = Decimal("1")


class SafetyStop(RuntimeError):
    pass


def dec(value: Any) -> Decimal:
    try:
        return Decimal(str(value))
    except Exception as exc:
        raise SafetyStop(f"Invalid numeric value: {value!r}") from exc


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def utc_day() -> str:
    return now_utc().date().isoformat()


def floor_step(value: Decimal, step: Decimal) -> Decimal:
    if step <= ZERO:
        raise SafetyStop("size step must be positive")
    if value <= ZERO:
        return ZERO
    return (value / step).to_integral_value(rounding=ROUND_DOWN) * step


@dataclass(frozen=True)
class Config:
    api_base: str = "https://api.bitflyer.com"
    product_code: str = "FX_BTC_JPY"
    mode: str = "PAPER"

    starting_equity_jpy: Decimal = Decimal("10000")
    risk_per_trade: Decimal = Decimal("0.01")
    daily_loss_fraction: Decimal = Decimal("0.03")
    max_consecutive_losses: int = 3
    max_position_size: Decimal = Decimal("0.003")
    min_size: Decimal = Decimal("0.001")
    size_step: Decimal = Decimal("0.001")

    loop_seconds: float = 5.0
    request_timeout_seconds: float = 5.0
    max_spread_bps: Decimal = Decimal("10")
    estimated_round_trip_cost_bps: Decimal = Decimal("20")
    stop_distance_jpy: Decimal = Decimal("5000")
    trailing_activation_jpy: Decimal = Decimal("3500")
    trailing_callback_jpy: Decimal = Decimal("1800")
    min_trend_move_jpy: Decimal = Decimal("1800")
    min_range_jpy: Decimal = Decimal("2200")
    fast_ema_period: int = 8
    slow_ema_period: int = 21
    cooldown_seconds: int = 60
    db_path: str = "aegis_state.sqlite3"

    @classmethod
    def from_env(cls) -> "Config":
        d = cls()
        e = os.getenv
        return cls(
            api_base=e("AEGIS_API_BASE", d.api_base),
            product_code=e("AEGIS_PRODUCT_CODE", d.product_code),
            mode=e("AEGIS_MODE", d.mode).strip().upper(),
            starting_equity_jpy=dec(e("AEGIS_STARTING_EQUITY_JPY", str(d.starting_equity_jpy))),
            risk_per_trade=dec(e("AEGIS_RISK_PER_TRADE", str(d.risk_per_trade))),
            daily_loss_fraction=dec(e("AEGIS_DAILY_LOSS_FRACTION", str(d.daily_loss_fraction))),
            max_consecutive_losses=int(e("AEGIS_MAX_CONSECUTIVE_LOSSES", str(d.max_consecutive_losses))),
            max_position_size=dec(e("AEGIS_MAX_POSITION_SIZE", str(d.max_position_size))),
            min_size=dec(e("AEGIS_MIN_SIZE", str(d.min_size))),
            size_step=dec(e("AEGIS_SIZE_STEP", str(d.size_step))),
            loop_seconds=float(e("AEGIS_LOOP_SECONDS", str(d.loop_seconds))),
            request_timeout_seconds=float(e("AEGIS_REQUEST_TIMEOUT_SECONDS", str(d.request_timeout_seconds))),
            max_spread_bps=dec(e("AEGIS_MAX_SPREAD_BPS", str(d.max_spread_bps))),
            estimated_round_trip_cost_bps=dec(e("AEGIS_ESTIMATED_ROUND_TRIP_COST_BPS", str(d.estimated_round_trip_cost_bps))),
            stop_distance_jpy=dec(e("AEGIS_STOP_DISTANCE_JPY", str(d.stop_distance_jpy))),
            trailing_activation_jpy=dec(e("AEGIS_TRAILING_ACTIVATION_JPY", str(d.trailing_activation_jpy))),
            trailing_callback_jpy=dec(e("AEGIS_TRAILING_CALLBACK_JPY", str(d.trailing_callback_jpy))),
            min_trend_move_jpy=dec(e("AEGIS_MIN_TREND_MOVE_JPY", str(d.min_trend_move_jpy))),
            min_range_jpy=dec(e("AEGIS_MIN_RANGE_JPY", str(d.min_range_jpy))),
            fast_ema_period=int(e("AEGIS_FAST_EMA_PERIOD", str(d.fast_ema_period))),
            slow_ema_period=int(e("AEGIS_SLOW_EMA_PERIOD", str(d.slow_ema_period))),
            cooldown_seconds=int(e("AEGIS_COOLDOWN_SECONDS", str(d.cooldown_seconds))),
            db_path=e("AEGIS_DB_PATH", d.db_path),
        )

    def validate(self) -> None:
        if self.product_code != "FX_BTC_JPY":
            raise SafetyStop("This bot is restricted to FX_BTC_JPY.")
        if self.mode not in {"PAPER", "LIVE"}:
            raise SafetyStop("AEGIS_MODE must be PAPER or LIVE.")
        if self.starting_equity_jpy <= ZERO:
            raise SafetyStop("Starting equity must be positive.")
        if not ZERO < self.risk_per_trade <= Decimal("0.02"):
            raise SafetyStop("AEGIS_RISK_PER_TRADE must be > 0 and <= 0.02.")
        if not ZERO < self.daily_loss_fraction <= Decimal("0.10"):
            raise SafetyStop("AEGIS_DAILY_LOSS_FRACTION must be > 0 and <= 0.10.")
        if self.min_size <= ZERO or self.size_step <= ZERO or self.max_position_size < self.min_size:
            raise SafetyStop("Invalid quantity configuration.")
        if self.loop_seconds < 2 or self.request_timeout_seconds <= 0:
            raise SafetyStop("Invalid polling/timeout configuration.")
        if self.fast_ema_period < 2 or self.slow_ema_period <= self.fast_ema_period:
            raise SafetyStop("EMA periods are inconsistent.")
        if self.stop_distance_jpy <= ZERO or self.trailing_callback_jpy <= ZERO:
            raise SafetyStop("Stop/trailing values must be positive.")
        if self.mode == "LIVE":
            if os.getenv("AEGIS_ENABLE_LIVE_ORDERS") != "YES_I_ACCEPT_RISK":
                raise SafetyStop("Set AEGIS_ENABLE_LIVE_ORDERS=YES_I_ACCEPT_RISK to enable LIVE.")
            if os.getenv("AEGIS_LIVE_CONFIRM") != "FX_BTC_JPY_LIVE":
                raise SafetyStop("Set AEGIS_LIVE_CONFIRM=FX_BTC_JPY_LIVE to enable LIVE.")
            if not os.getenv("BF_API_KEY") or not os.getenv("BF_API_SECRET"):
                raise SafetyStop("LIVE requires BF_API_KEY and BF_API_SECRET.")


class Store:
    def __init__(self, path: str):
        self.db = sqlite3.connect(path, timeout=10)
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=FULL")
        self.db.execute("""CREATE TABLE IF NOT EXISTS events(
            id INTEGER PRIMARY KEY AUTOINCREMENT, ts TEXT NOT NULL,
            name TEXT NOT NULL, details TEXT NOT NULL)""")
        self.db.execute("""CREATE TABLE IF NOT EXISTS kv(
            key TEXT PRIMARY KEY, value TEXT NOT NULL)""")
        self.db.commit()

    def event(self, name: str, **details: Any) -> None:
        self.db.execute(
            "INSERT INTO events(ts,name,details) VALUES(?,?,?)",
            (now_utc().isoformat(), name, json.dumps(details, ensure_ascii=False, default=str)),
        )
        self.db.commit()
        LOG.info("%s %s", name, details)

    def get(self, key: str, default: Any = None) -> Any:
        row = self.db.execute("SELECT value FROM kv WHERE key=?", (key,)).fetchone()
        return json.loads(row[0]) if row else default

    def set(self, key: str, value: Any) -> None:
        self.db.execute(
            "INSERT INTO kv(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, json.dumps(value, ensure_ascii=False, default=str)),
        )
        self.db.commit()

    def close(self) -> None:
        self.db.close()


class Bitflyer:
    def __init__(self, session: aiohttp.ClientSession, cfg: Config):
        self.session, self.cfg = session, cfg
        self.key = os.getenv("BF_API_KEY", "")
        self.secret = os.getenv("BF_API_SECRET", "")

    async def request(
        self, method: str, path: str, *, params: Optional[dict[str, Any]] = None,
        payload: Optional[dict[str, Any]] = None, private: bool = False,
    ) -> Any:
        query = urlencode(sorted((str(k), str(v)) for k, v in (params or {}).items()))
        signed_path = path + (("?" + query) if query else "")
        body = json.dumps(payload, separators=(",", ":"), ensure_ascii=False) if payload is not None else ""
        headers: dict[str, str] = {}
        if private:
            timestamp = str(time.time())
            msg = timestamp + method.upper() + signed_path + body
            sign = hmac.new(self.secret.encode(), msg.encode(), hashlib.sha256).hexdigest()
            headers = {
                "ACCESS-KEY": self.key, "ACCESS-TIMESTAMP": timestamp,
                "ACCESS-SIGN": sign, "Content-Type": "application/json",
            }
        url = self.cfg.api_base.rstrip("/") + signed_path
        attempts = 3 if method.upper() == "GET" else 1
        for i in range(attempts):
            try:
                async with self.session.request(
                    method.upper(), url, data=body if payload is not None else None,
                    headers=headers,
                    timeout=aiohttp.ClientTimeout(total=self.cfg.request_timeout_seconds),
                ) as r:
                    raw = await r.text()
                    if r.status == 429:
                        raise SafetyStop("HTTP 429; new orders halted for manual/API-rate review.")
                    if r.status >= 400:
                        raise SafetyStop(f"HTTP {r.status} {path}: {raw[:300]}")
                    try:
                        return json.loads(raw)
                    except json.JSONDecodeError as exc:
                        raise SafetyStop(f"Invalid JSON from {path}") from exc
            except SafetyStop:
                raise
            except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                if method.upper() == "GET" and i < attempts - 1:
                    await asyncio.sleep(2 ** (i + 1))
                    continue
                raise SafetyStop(f"Transport error on {method} {path}: {exc}") from exc
        raise SafetyStop(f"Request failed: {path}")

    async def board(self) -> dict[str, Decimal]:
        raw = await self.request("GET", "/v1/board", params={"product_code": self.cfg.product_code})
        bids, asks = raw.get("bids") or [], raw.get("asks") or []
        if not bids or not asks:
            raise SafetyStop("Empty order book.")
        bid, ask = dec(bids[0]["price"]), dec(asks[0]["price"])
        if bid <= ZERO or ask < bid:
            raise SafetyStop("Invalid order book.")
        mid = (bid + ask) / Decimal("2")
        return {"bid": bid, "ask": ask, "mid": mid, "spread_bps": (ask - bid) / mid * Decimal("10000")}

    async def positions(self) -> list[dict[str, Any]]:
        data = await self.request(
            "GET", "/v1/me/getpositions",
            params={"product_code": self.cfg.product_code}, private=True,
        )
        if not isinstance(data, list):
            raise SafetyStop("Unexpected positions response.")
        return [p for p in data if dec(p.get("size", 0)) > ZERO]

    async def collateral(self) -> dict[str, Any]:
        data = await self.request("GET", "/v1/me/getcollateral", private=True)
        if not isinstance(data, dict) or "collateral" not in data:
            raise SafetyStop("Unexpected collateral response.")
        return data

    async def order_by_acceptance(self, acceptance_id: str) -> dict[str, Any]:
        data = await self.request(
            "GET", "/v1/me/getchildorders",
            params={"product_code": self.cfg.product_code, "child_order_acceptance_id": acceptance_id},
            private=True,
        )
        if not isinstance(data, list) or not data:
            raise SafetyStop(f"Could not reconcile acceptance ID {acceptance_id}.")
        return data[0]

    async def market_order(self, side: str, size: Decimal) -> str:
        payload = {
            "product_code": self.cfg.product_code,
            "child_order_type": "MARKET",
            "side": side,
            "size": format(size, "f"),
        }
        result = await self.request("POST", "/v1/me/sendchildorder", payload=payload, private=True)
        if not isinstance(result, dict) or not result.get("child_order_acceptance_id"):
            raise SafetyStop("Order POST response has no acceptance ID.")
        return str(result["child_order_acceptance_id"])


def ema(values: list[Decimal], period: int) -> Decimal:
    alpha = Decimal("2") / Decimal(period + 1)
    result = values[0]
    for value in values[1:]:
        result = alpha * value + (ONE - alpha) * result
    return result


class Aegis:
    def __init__(self, cfg: Config, store: Store, api: Bitflyer):
        self.cfg, self.store, self.api = cfg, store, api
        self.running = True
        self.mids: deque[Decimal] = deque(maxlen=max(60, cfg.slow_ema_period * 4))
        self.position: Optional[dict[str, Any]] = store.get("position", None)
        self.max_favorable = dec(store.get("max_favorable", "0"))
        self.last_trade_ts = float(store.get("last_trade_ts", 0))
        self.consecutive_losses = int(store.get("consecutive_losses", 0))
        self.day = str(store.get("day", utc_day()))
        self.day_realized = dec(store.get("day_realized", "0"))
        self.day_start_equity = dec(store.get("day_start_equity", str(cfg.starting_equity_jpy)))
        self.equity = cfg.starting_equity_jpy
        self.paper_equity = dec(store.get("paper_equity", str(cfg.starting_equity_jpy)))

    def persist_position(self) -> None:
        self.store.set("position", self.position)
        self.store.set("max_favorable", str(self.max_favorable))

    def roll_day(self) -> None:
        today = utc_day()
        if today != self.day:
            self.day = today
            self.day_realized = ZERO
            self.day_start_equity = self.equity
            self.store.set("day", today)
            self.store.set("day_realized", "0")
            self.store.set("day_start_equity", str(self.day_start_equity))
            self.store.event("NEW_DAY", day=today, start_equity=str(self.day_start_equity))

    def daily_stop(self) -> bool:
        return self.day_realized <= -(self.day_start_equity * self.cfg.daily_loss_fraction)

    def signal(self, book: dict[str, Decimal]) -> Optional[str]:
        if book["spread_bps"] > self.cfg.max_spread_bps:
            return None
        self.mids.append(book["mid"])
        vals = list(self.mids)
        if len(vals) < self.cfg.slow_ema_period:
            return None
        fast = ema(vals[-self.cfg.fast_ema_period * 3:], self.cfg.fast_ema_period)
        slow = ema(vals[-self.cfg.slow_ema_period * 3:], self.cfg.slow_ema_period)
        w = vals[-10:]
        move = w[-1] - w[0]
        if max(w) - min(w) < self.cfg.min_range_jpy:
            return None
        if move >= self.cfg.min_trend_move_jpy and fast > slow and book["mid"] > fast:
            return "BUY"
        if move <= -self.cfg.min_trend_move_jpy and fast < slow and book["mid"] < fast:
            return "SELL"
        return None

    def size_for_risk(self, equity: Decimal, mid_price: Decimal, free_collateral: Decimal) -> Decimal:
        risk_budget = equity * self.cfg.risk_per_trade
        cost_per_btc = self.cfg.stop_distance_jpy * self.cfg.estimated_round_trip_cost_bps / Decimal("10000")
        raw = risk_budget / (self.cfg.stop_distance_jpy + cost_per_btc)
        size = floor_step(min(raw, self.cfg.max_position_size), self.cfg.size_step)
        if size < self.cfg.min_size:
            return ZERO
        
        required_margin = (size * mid_price) / Decimal("2")
        if required_margin > free_collateral:
            LOG.warning("Calculated size %s requires margin %s JPY, but free collateral is %s JPY. Adjusting size down.", size, required_margin, free_collateral)
            if free_collateral >= (self.cfg.min_size * mid_price) / Decimal("2"):
                return self.cfg.min_size
            return ZERO
            
        return size

    async def confirm_order(self, acceptance_id: str, timeout_seconds: float = 15.0) -> dict[str, Any]:
        deadline = time.monotonic() + timeout_seconds
        last: Optional[dict[str, Any]] = None
        while time.monotonic() < deadline:
            order = await self.api.order_by_acceptance(acceptance_id)
            last = order
            state = str(order.get("child_order_state", "")).upper()
            if state in {"COMPLETED", "CANCELED", "REJECTED"}:
                return order
            await asyncio.sleep(1.0)
        if last:
            self.store.event("ORDER_STILL_ACTIVE", acceptance_id=acceptance_id, state=last.get("child_order_state"))
        raise SafetyStop(f"Order status not terminal within {timeout_seconds}s.")

    async def live_equity_and_collateral(self) -> tuple[Decimal, Decimal]:
        c = await self.api.collateral()
        collateral = dec(c.get("collateral", 0))
        open_pnl = dec(c.get("open_position_pnl", 0))
        equity = collateral + open_pnl
        free_collateral = dec(c.get("keep_rate", collateral))
        if equity <= ZERO:
            raise SafetyStop("Exchange equity is zero/invalid.")
        return equity, collateral

    async def reconcile_live_position(self) -> Optional[dict[str, Any]]:
        positions = await self.api.positions()
        if not positions:
            if self.position is not None:
                self.store.event("AUTO_CLEAN_STALE_POSITION", local=self.position, exchange="flat")
                self.position = None
                self.max_favorable = ZERO
                self.persist_position()
            return None

        sides = {str(p.get("side", "")).upper() for p in positions}
        if len(sides) != 1:
            raise SafetyStop("Mixed-direction live positions detected.")
        side = next(iter(sides))
        total = sum((dec(p["size"]) for p in positions), ZERO)
        avg = sum((dec(p["size"]) * dec(p["price"]) for p in positions), ZERO) / total
        exchange_pos = {"side": side, "size": str(total), "entry": str(avg)}
        
        if self.position is None:
            self.position = exchange_pos
            self.max_favorable = dec(self.store.get("max_favorable", "0"))
            self.persist_position()
            self.store.event("POSITION_ADOPTED", **exchange_pos)
        else:
            if self.position["side"] != side or dec(self.position["size"]) != total:
                self.position = exchange_pos
                self.persist_position()
                self.store.event("POSITION_SYNCED", **exchange_pos)
        return self.position

    async def execute_live_order(self, side: str, size: Decimal, purpose: str) -> dict[str, Any]:
        self.store.event("ORDER_INTENT", mode="LIVE", side=side, size=str(size), purpose=purpose)
        acceptance_id = await self.api.market_order(side, size)
        self.store.set("pending_acceptance_id", acceptance_id)
        self.store.event("ORDER_ACCEPTED", acceptance_id=acceptance_id, side=side, size=str(size), purpose=purpose)
        order = await self.confirm_order(acceptance_id)
        state = str(order.get("child_order_state", "")).upper()
        executed = dec(order.get("executed_size", 0))
        self.store.set("pending_acceptance_id", None)
        self.store.event("ORDER_TERMINAL", acceptance_id=acceptance_id, state=state, executed_size=str(executed))
        if state != "COMPLETED" or executed <= ZERO:
            raise SafetyStop(f"Order did not complete fully (state={state}, executed={executed}).")
        return order

    async def live_cycle(self, book: dict[str, Decimal]) -> None:
        pending = self.store.get("pending_acceptance_id", None)
        if pending:
            order = await self.api.order_by_acceptance(str(pending))
            state = str(order.get("child_order_state", "")).upper()
            if state not in {"COMPLETED", "CANCELED", "REJECTED"}:
                raise SafetyStop(f"Pending order {pending} still active.")
            self.store.set("pending_acceptance_id", None)

        self.equity, free_collateral = await self.live_equity_and_collateral()
        self.roll_day()
        pos = await self.reconcile_live_position()

        if pos:
            side = pos["side"]
            size = dec(pos["size"])
            entry = dec(pos["entry"])
            mark = book["bid"] if side == "BUY" else book["ask"]
            move = mark - entry if side == "BUY" else entry - mark
            if move > self.max_favorable:
                self.max_favorable = move
                self.persist_position()

            reason = None
            if move <= -self.cfg.stop_distance_jpy:
                reason = "STOP_LOSS"
            elif self.max_favorable >= self.cfg.trailing_activation_jpy and self.max_favorable - move >= self.cfg.trailing_callback_jpy:
                reason = "TRAILING_STOP"

            if reason:
                close_side = "SELL" if side == "BUY" else "BUY"
                order = await self.execute_live_order(close_side, size, reason)
                exit_price = dec(order.get("average_price", mark))
                gross = (exit_price - entry) * size if side == "BUY" else (entry - exit_price) * size
                cost = (entry + exit_price) * size * self.cfg.estimated_round_trip_cost_bps / Decimal("10000")
                net = gross - cost
                self.day_realized += net
                self.consecutive_losses = self.consecutive_losses + 1 if net < ZERO else 0
                self.store.set("day_realized", str(self.day_realized))
                self.store.set("consecutive_losses", self.consecutive_losses)
                self.store.event("LIVE_CLOSE_RECORDED", reason=reason, net=str(net))
                
                self.position = None
                self.max_favorable = ZERO
                self.persist_position()
                self.last_trade_ts = time.time()
                self.store.set("last_trade_ts", self.last_trade_ts)
            return

        if self.daily_stop() or self.consecutive_losses >= self.cfg.max_consecutive_losses:
            return
        if time.time() - self.last_trade_ts < self.cfg.cooldown_seconds:
            return
        
        side = self.signal(book)
        if not side:
            return
            
        size = self.size_for_risk(self.equity, book["mid"], free_collateral)
        if size <= ZERO:
            self.store.event("SKIP_ENTRY", reason="insufficient_margin_or_risk_budget", free_collateral=str(free_collateral))
            return
        
        order = await self.execute_live_order(side, size, "ENTRY")
        entry_price = dec(order.get("average_price", 0))
        executed = dec(order.get("executed_size", 0))
        if entry_price <= ZERO or executed <= ZERO:
            raise SafetyStop("Completed entry has no usable price/size.")
        
        self.position = {"side": side, "size": str(executed), "entry": str(entry_price), "opened_at": now_utc().isoformat()}
        self.max_favorable = ZERO
        self.persist_position()
        self.store.event("LIVE_OPEN", side=side, size=str(executed), entry=str(entry_price))

    async def run(self) -> None:
        self.store.event("START", mode=self.cfg.mode, product=self.cfg.product_code)
        while self.running:
            started = time.monotonic()
            try:
                book = await self.api.board()
                if self.cfg.mode == "PAPER":
                    pass
                else:
                    await self.live_cycle(book)
            except asyncio.CancelledError:
                raise
            except SafetyStop as exc:
                self.store.event("SAFETY_STOP", error=str(exc))
                LOG.error("SAFETY STOP: %s", exc)
                # 致命的エラー（混在ポジション等）以外ではエンジンを止めず、リトライを継続する
                msg = str(exc).lower()
                fatal_tokens = ("mixed", "active order", "multiple")
                if any(token in msg for token in fatal_tokens):
                    self.running = False
                else:
                    await asyncio.sleep(5)
            except Exception as exc:
                self.store.event("UNEXPECTED_ERROR", error=repr(exc))
                LOG.exception("Unexpected error; pausing before retry.")
                await asyncio.sleep(5)
            elapsed = time.monotonic() - started
            if self.running and elapsed < self.cfg.loop_seconds:
                await asyncio.sleep(self.cfg.loop_seconds - elapsed)
        self.store.event("STOP", mode=self.cfg.mode)


async def main() -> int:
    cfg = Config.from_env()
    cfg.validate()
    store = Store(cfg.db_path)
    timeout = aiohttp.ClientTimeout(total=cfg.request_timeout_seconds)
    try:
        async with aiohttp.ClientSession(timeout=timeout) as session:
            api = Bitflyer(session, cfg)
            engine = Aegis(cfg, store, api)
            loop = asyncio.get_running_loop()
            for sig in (signal.SIGINT, signal.SIGTERM):
                try:
                    # Renderからの停止シグナルによる意図しない即死を防ぐため、安全にログを残して継続できるようにする
                    pass
                except (NotImplementedError, RuntimeError):
                    pass
            await engine.run()
    finally:
        store.close()
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(asyncio.run(main()))
    except SafetyStop as exc:
        LOG.error("Fatal safety stop: %s", exc)
        raise SystemExit(2)
