#!/usr/init/env python3
# -*- coding: utf-8 -*-
"""
AEGIS ULTIMATE PRO — Multi-Factor Aggressive Integrated Variant.
- Self-Healing & Resilient Version for 24/7 Render Operations.
- Fixed StateStore helper binding for clean startup reconciliation.
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
    risk_fraction: Decimal = Decimal("0.025")                # アグレッシブ設定（2.5%）
    daily_loss_fraction: Decimal = Decimal("0.12")           # 日次制限12%
    max_spread_bps: Decimal = Decimal("12")                  # スプレッド許容
    max_board_fetch_seconds: Decimal = Decimal("3")
    loop_seconds: float = 4.0                                # 安全な4.0秒間隔
    cooldown_seconds: float = 3.0
    stop_distance_jpy: Decimal = Decimal("180")
    take_profit_distance_jpy: Decimal = Decimal("320")
    cost_buffer_jpy_per_btc: Decimal = Decimal("80")
    min_size: Decimal = Decimal("0.001")
    size_step: Decimal = Decimal("0.001")
    max_position_size: Decimal = Decimal("0.5")
    max_consecutive_losses: int = 5
    min_available_collateral_jpy: Decimal = Decimal("800")
    order_poll_seconds: float = 0.4
    order_timeout_seconds: float = 12.0
    request_timeout_seconds: float = 5.0
    max_consecutive_errors: int = 8
    db_path: str = "aegis_ultimate_pro.sqlite3"
    signal_window_size: int = 4

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
        if not (Decimal("0") < self.daily_loss_fraction <= Decimal("0.25")):
            raise SafetyStop("daily_loss_fraction must be in (0, 0.25].")
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
        """不確定状態フラグをクリアする"""
        self.set("live_order_ambiguous", False)
        self.set("live_order_intent", None)

    def open_paper_trade(self) -> Optional[dict[str, Any]]:
        row = self.db.execute(
            "SELECT id,ts_utc,side,size,entry_price FROM paper_trades "
            "WHERE status='OPEN' ORDER BY id DESC LIMIT 1"
        ).fetchone()
        if not row:
            return None
        return {"id": row[0], "ts_utc": row[1], "side": row[2],
                "size": D(row[3]), "entry_price": D(row[4])}

    def create_paper_trade(self, side: str, size: Decimal, price: Decimal) -> int:
        cur = self.db.execute(
            "INSERT INTO paper_trades(ts_utc,side,size,entry_price,status) VALUES(?,?,?,?,?)",
            (utc_now().isoformat(), side, str(size), str(price), "OPEN"),
        )
        self.db.commit()
        return int(cur.lastrowid)

    def close_paper_trade(self, trade_id: int, price: Decimal, pnl: Decimal) -> None:
        cur = self.db.execute(
            "UPDATE paper_trades SET exit_price=?,pnl=?,status='CLOSED',closed_ts_utc=? "
            "WHERE id=? AND status='OPEN'",
            (str(price), str(pnl), utc_now().isoformat(), trade_id),
        )
        self.db.commit()
        if cur.rowcount != 1:
            raise SafetyStop("Paper trade state conflict while closing.")

    def paper_daily_pnl(self, day: str) -> Decimal:
        rows = self.db.execute(
            "SELECT pnl FROM paper_trades WHERE status='CLOSED' AND substr(closed_ts_utc,1,10)=?",
            (day,),
        ).fetchall()
        return sum((D(r[0]) for r in rows if r[0] is not None), Decimal("0"))

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
                            wait_time = 15.0 * (attempt + 1)
                            LOG.warning("HTTP 42
