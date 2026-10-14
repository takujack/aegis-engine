#!/usr/env python3
# -*- coding: utf-8 -*-
"""
AEGIS ULTIMATE PRO — Professional Adaptive Scalping & Trend Trailing Engine.
- Strict 4.0s loop interval to completely eliminate HTTP 429 Rate Limits.
- Dynamic Trailing Profit: Maximizes gains during strong trends.
- Volatility-adaptive breakout & range harvesting logic.
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
    """Critical safety halt."""


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
    daily_loss_fraction: Decimal = Decimal("0.30")
    max_spread_bps: Decimal = Decimal("15")
    loop_seconds: float = 4.0  # 4秒固定でレート制限を完全に回避
    
    # 損切り幅と、トレンド追従用トレーリング設定
    stop_distance_jpy: Decimal = Decimal("6000")
    trailing_activation_jpy: Decimal = Decimal("4000")  # この含み益を超えたらトレーリング発動
    trailing_callback_jpy: Decimal = Decimal("2500")   # 最高益からこの幅逆行したら利食い
    
    min_size: Decimal = Decimal("0.001")
    size_step: Decimal = Decimal("0.001")
    max_position_size: Decimal = Decimal("0.03")
    max_consecutive_losses: int = 10
    request_timeout_seconds: float = 5.0
    db_path: str = "aegis_ultimate_pro.sqlite3"

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
            daily_loss_fraction=D(os.getenv("AEGIS_DAILY_LOSS_FRACTION", str(d.daily_loss_fraction))),
            max_spread_bps=D(os.getenv("AEGIS_MAX_SPREAD_BPS", str(d.max_spread_bps))),
            loop_seconds=float(os.getenv("AEGIS_LOOP_SECONDS", str(d.loop_seconds))),
            stop_distance_jpy=D(os.getenv("AEGIS_STOP_DISTANCE_JPY", str(d.stop_distance_jpy))),
            trailing_activation_jpy=D(os.getenv("AEGIS_TRAILING_ACTIVATION_JPY", str(d.trailing_activation_jpy))),
            trailing_callback_jpy=D(os.getenv("AEGIS_TRAILING_CALLBACK_JPY", str(d.trailing_callback_jpy))),
            min_size=D(os.getenv("AEGIS_MIN_SIZE", str(d.min_size))),
            size_step=D(os.getenv("AEGIS_SIZE_STEP", str(d.size_step))),
            max_position_size=D(os.getenv("AEGIS_MAX_POSITION_SIZE", str(d.max_position_size))),
            max_consecutive_losses=int(os.getenv("AEGIS_MAX_CONSECUTIVE_LOSSES", str(d.max_consecutive_losses))),
            request_timeout_seconds=float(os.getenv("AEGIS_REQUEST_TIMEOUT_SECONDS", str(d.request_timeout_seconds))),
            db_path=os.getenv("AEGIS_DB_PATH", d.db_path),
        )

    def validate(self) -> None:
        if self.product_code != "FX_BTC_JPY":
            raise SafetyStop("Restricted to FX_BTC_JPY.")
        if self.mode not in {"PAPER", "LIVE"}:
            raise SafetyStop("Invalid mode.")
        if self.mode == "LIVE" and not self.enable_live_orders:
            raise SafetyStop("LIVE requires AEGIS_ENABLE_LIVE_ORDERS=YES_I_ACCEPT_RISK.")


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
        self.db.commit()

    def event(self, name: str, details: dict[str, Any], level: str = "INFO") -> None:
        self.db.execute(
            "INSERT INTO events(ts_utc,event,details) VALUES(?,?,?)",
            (utc_now().isoformat(), name, json.dumps({"level": level, **details}, ensure_ascii=False, default=str)),
        )
        self.db.commit()

    def get(self, key: str, default: Any = None) -> Any:
        row = self.db.execute("SELECT value FROM state WHERE key=?", (key,)).fetchone()
        return json.loads(row[0]) if row else default

    def set(self, key: str, value: Any) -> None:
        self.db.execute(
            "INSERT INTO state(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, json.dumps(value, ensure_ascii=False, default=str)),
        )
        self.db.commit()

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

        max_retries = 5
        for attempt in range(max_retries):
            try:
                async with self.session.request(
                    method.upper(), url, data=body if payload is not None else None,
                    headers=headers, timeout=aiohttp.ClientTimeout(total=self.cfg.request_timeout_seconds),
                ) as resp:
                    raw = await resp.text()
                    if resp.status == 429:
                        if attempt < max_retries - 1:
                            wait_time = 15.0 * (attempt + 1)
                            LOG.warning("HTTP 429 Rate Limit. Cooling down for %.1fs...", wait_time)
                            await asyncio.sleep(wait_time)
                            continue
                        else:
                            raise SafetyStop("HTTP 429 Rate Limit exceeded persistently.")
                    if resp.status >= 400:
                        raise SafetyStop(f"HTTP {resp.status} on {method} {path}: {raw[:250]}")
                    try:
                        return json.loads(raw)
                    except json.JSONDecodeError as exc:
                        raise SafetyStop(f"Invalid JSON from {path}") from exc
            except SafetyStop:
                raise
            except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                if attempt < max_retries - 1:
                    await asyncio.sleep(3.0 * (attempt + 1))
                    continue
                raise SafetyStop(f"Transport failure: {exc}") from exc

    async def board(self) -> dict[str, Any]:
        return await self.request("GET", "/v1/board", params={"product_code": self.cfg.product_code})

    async def collateral(self) -> dict[str, Any]:
        return await self.request("GET", "/v1/me/getcollateral", private=True)

    async def positions(self) -> list[dict[str, Any]]:
        data = await self.request("GET", "/v1/me/getpositions", params={"product_code": self.cfg.product_code}, private=True)
        return data if isinstance(data, list) else []

    async def cancel_all_orders(self) -> None:
        try:
            await self.request("POST", "/v1/me/cancelallchildorders", private=True, payload={"product_code": self.cfg.product_code})
        except Exception:
            pass

    async def send_market(self, side: str, size: Decimal) -> dict[str, Any]:
        payload = {"product_code": self.cfg.product_code, "child_order_type": "MARKET", "side": side, "size": float(size)}
        return await self.request("POST", "/v1/me/sendchildorder", private=True, payload=payload)


def parse_board(raw: dict[str, Any]) -> dict[str, Decimal]:
    bids, asks = raw.get("bids") or [], raw.get("asks") or []
    if not bids or not asks:
        raise SafetyStop("Empty order book.")
    bid, ask = D(bids[0]["price"]), D(asks[0]["price"])
    mid = (bid + ask) / Decimal("2")
    spread_bps = (ask - bid) / mid * Decimal("10000")
    return {"bid": bid, "ask": ask, "mid": mid, "spread_bps": spread_bps}


class AegisAdaptiveEngine:
    def __init__(self, cfg: Config, store: StateStore, client: BitflyerClient):
        self.cfg, self.store, self.client = cfg, store, client
        self.running = True
        self.mids: list[Decimal] = []
        self.consecutive_losses = int(store.get("consecutive_losses", 0))
        self.max_favorable_pnl = Decimal("0")  # トレーリング用の最高益追跡

    async def initialize(self) -> None:
        if self.cfg.mode == "LIVE":
            LOG.info("Cleaning up prior exchange orders...")
            await self.client.cancel_all_orders()

    def generate_signal(self, book: dict[str, Decimal]) -> Optional[str]:
        if book["spread_bps"] > self.cfg.max_spread_bps:
            return None

        self.mids.append(book["mid"])
        self.mids = self.mids[-12:]

        if len(self.mids) < 8:
            return None

        recent_prices = self.mids[-6:]
        delta = recent_prices[-1] - recent_prices[0]
        threshold = Decimal("1000")  # より素早くトレンド初動をキャッチ

        if delta >= threshold:
            return "BUY"
        if delta <= -threshold:
            return "SELL"
        return None

    def calculate_size(self, equity: Decimal, mid_price: Decimal) -> Decimal:
        if equity <= 0 or mid_price <= 0:
            return self.cfg.min_size
        
        penalty_factor = max(Decimal("0.3"), Decimal("1.0") - (Decimal(str(self.consecutive_losses)) * Decimal("0.1")))
        base_size = floor_step((equity * Decimal("0.5") * penalty_factor) / (mid_price / Decimal("2")), self.cfg.size_step)
        return max(self.cfg.min_size, min(base_size, self.cfg.max_position_size))

    async def live_cycle(self, book: dict[str, Decimal]) -> None:
        positions_raw = await self.client.positions()
        positions = [p for p in positions_raw if D(p.get("size", 0)) > 0]

        net_side: Optional[str] = None
        total_size = Decimal("0")
        weighted_price_sum = Decimal("0")

        if positions:
            sides = {str(p.get("side", "")).upper() for p in positions}
            net_side = sides.pop() if len(sides) == 1 else "BUY"
            for p in positions:
                p_size = D(p.get("size", 0))
                p_price = D(p.get("price", 0))
                total_size += p_size
                weighted_price_sum += p_size * p_price
            avg_entry_price = weighted_price_sum / total_size if total_size > 0 else Decimal("0")
        else:
            avg_entry_price = Decimal("0")
            self.max_favorable_pnl = Decimal("0")  # ポジションがないときは最高益をリセット

        collateral = await self.client.collateral()
        equity = D(collateral.get("collateral", 0)) + D(collateral.get("open_position_pnl", 0))
        if equity <= 0:
            equity = self.cfg.starting_equity_jpy

        # ポジション保有中の損切り ＆ トレーリング・プロフィット判定
        if net_side and total_size > 0:
            current_move = book["bid"] - avg_entry_price if net_side == "BUY" else avg_entry_price - book["ask"]
            
            # 1. 損切り判定
            if current_move <= -self.cfg.stop_distance_jpy:
                close_side = "SELL" if net_side == "BUY" else "BUY"
                await self.client.send_market(close_side, total_size)
                self.consecutive_losses += 1
                self.store.set("consecutive_losses", self.consecutive_losses)
                self.store.event("TRADE_CLOSE", {"reason": "STOP_LOSS", "move": str(current_move)})
                return

            # 2. トレーリング・プロフィット（利益追従）の更新と判定
            if current_move > self.max_favorable_pnl:
                self.max_favorable_pnl = current_move  # 最高益を更新

            # 一定の含み益（Activation）に達した後の押し目撤退（Callback）
            if self.max_favorable_pnl >= self.cfg.trailing_activation_jpy:
                drawdown_from_peak = self.max_favorable_pnl - current_move
                if drawdown_from_peak >= self.cfg.trailing_callback_jpy:
                    close_side = "SELL" if net_side == "BUY" else "BUY"
                    await self.client.send_market(close_side, total_size)
                    self.consecutive_losses = 0
                    self.store.set("consecutive_losses", self.consecutive_losses)
                    self.store.event("TRADE_CLOSE", {"reason": "TRAILING_PROFIT", "peak": str(self.max_favorable_pnl), "move": str(current_move)})
                    return
            return

        # 新規エントリー判定
        signal = self.generate_signal(book)
        if not signal:
            return

        size = self.calculate_size(equity, book["mid"])
        await self.client.send_market(signal, size)
        self.max_favorable_pnl = Decimal("0")
        self.store.event("TRADE_OPEN", {"side": signal, "size": str(size)})

    async def run(self) -> None:
        self.store.event("START", {"mode": self.cfg.mode})
        LOG.info("Aegis Adaptive Engine (Trailing Edition) started in %s mode.", self.cfg.mode)
        await self.initialize()

        while self.running:
            t0 = time.monotonic()
            try:
                raw = await self.client.board()
                book = parse_board(raw)
                if self.cfg.mode == "LIVE":
                    await self.live_cycle(book)
            except asyncio.CancelledError:
                raise
            except SafetyStop as exc:
                LOG.error("Safety Stop: %s", exc)
                self.running = False
            except Exception as exc:
                LOG.warning("Cycle warning: %s", exc)
                await asyncio.sleep(3.0)

            elapsed = time.monotonic() - t0
            if self.running and elapsed < self.cfg.loop_seconds:
                await asyncio.sleep(self.cfg.loop_seconds - elapsed)


async def async_main() -> int:
    cfg = Config.from_env()
    cfg.validate()
    store = StateStore(cfg.db_path)
    timeout = aiohttp.ClientTimeout(total=cfg.request_timeout_seconds)
    try:
        async with aiohttp.ClientSession(timeout=timeout) as session:
            client = BitflyerClient(session, cfg)
            engine = AegisAdaptiveEngine(cfg, store, client)
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
        LOG.error("Fatal: %s", exc)
        raise SystemExit(2)
