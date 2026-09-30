"""SQLite layer (CONTRACT §4). Modules write their own SQL through the generic helpers.

Portability: queries use `?` placeholders, translated in `_sql()` only. The DDL below uses
AUTOINCREMENT (SQLite); a Postgres port swaps that one keyword and `_sql()`.
"""
from __future__ import annotations

import json
import sqlite3
import threading
from pathlib import Path
from typing import Any, Iterable, Sequence

from .clock import RealClock, utc_iso
from .config import StrategyParams, params_hash

SCHEMA = """
CREATE TABLE IF NOT EXISTS news_events(
  article_id TEXT PRIMARY KEY, received_at TEXT, created_at TEXT, updated_at TEXT,
  headline TEXT, summary TEXT, content TEXT, symbols_json TEXT, source TEXT, url TEXT,
  latency_s REAL, is_duplicate INTEGER DEFAULT 0, strategy_version TEXT);
CREATE TABLE IF NOT EXISTS ai_classifications(
  id INTEGER PRIMARY KEY AUTOINCREMENT, article_id TEXT REFERENCES news_events(article_id),
  symbol TEXT, model TEXT, effort TEXT, direction TEXT, confidence REAL, material INTEGER,
  catalyst TEXT, reason TEXT, raw_json TEXT, duration_ms INTEGER, cost_usd_est REAL,
  error TEXT, created_at TEXT, strategy_version TEXT);
CREATE INDEX IF NOT EXISTS ix_ai_article ON ai_classifications(article_id);
CREATE TABLE IF NOT EXISTS symbols(
  symbol TEXT PRIMARY KEY, name TEXT, exchange TEXT, asset_class TEXT, tradable INTEGER,
  shortable INTEGER, easy_to_borrow INTEGER, status TEXT, last_price REAL,
  avg_dollar_volume REAL, market_cap REAL, reject_reason TEXT, updated_at TEXT);
CREATE TABLE IF NOT EXISTS market_events(
  id INTEGER PRIMARY KEY AUTOINCREMENT, ts TEXT, symbol TEXT, kind TEXT, data_json TEXT);
CREATE INDEX IF NOT EXISTS ix_market_sym_ts ON market_events(symbol, ts);
CREATE TABLE IF NOT EXISTS setups(
  id INTEGER PRIMARY KEY AUTOINCREMENT, article_id TEXT, symbol TEXT, variant TEXT,
  is_shadow INTEGER DEFAULT 0, side TEXT, stage TEXT, max_stage TEXT, reject_reason TEXT,
  news_received_at TEXT, news_latency_s REAL, ai_direction TEXT, ai_confidence REAL,
  ai_material INTEGER, catalyst TEXT, ref_price REAL, rvol REAL, impulse_pct REAL,
  impulse_extreme REAL, pullback_high REAL, pullback_low REAL, pullback_bars INTEGER,
  ema9_at_touch REAL, atr REAL, entry_trigger REAL, stop_price REAL, signal_at TEXT,
  created_at TEXT, updated_at TEXT, closed_at TEXT, strategy_version TEXT, ref_source TEXT,
  UNIQUE(article_id, symbol, variant, strategy_version));
CREATE INDEX IF NOT EXISTS ix_setups_sym ON setups(symbol, created_at);
CREATE INDEX IF NOT EXISTS ix_setups_stage ON setups(stage);
CREATE TABLE IF NOT EXISTS positions(
  id INTEGER PRIMARY KEY AUTOINCREMENT, setup_id INTEGER REFERENCES setups(id), symbol TEXT,
  side TEXT, is_shadow INTEGER DEFAULT 0, variant TEXT, qty_initial INTEGER, qty_open INTEGER,
  entry_price REAL, stop_price REAL, risk_per_share REAL, highest_since_entry REAL,
  lowest_since_entry REAL, trail_price REAL, partial_taken INTEGER DEFAULT 0, mfe_r REAL,
  mae_r REAL, status TEXT, opened_at TEXT, closed_at TEXT, strategy_version TEXT);
CREATE INDEX IF NOT EXISTS ix_positions_status ON positions(status);
CREATE TABLE IF NOT EXISTS orders(
  id INTEGER PRIMARY KEY AUTOINCREMENT, client_order_id TEXT UNIQUE, broker_order_id TEXT,
  setup_id INTEGER, position_id INTEGER, symbol TEXT, side TEXT, order_type TEXT, qty INTEGER,
  limit_price REAL, stop_price REAL, purpose TEXT, status TEXT, signal_at TEXT,
  submitted_at TEXT, ack_at TEXT, filled_at TEXT, filled_qty INTEGER, filled_avg_price REAL,
  error TEXT, raw_json TEXT, strategy_version TEXT);
CREATE INDEX IF NOT EXISTS ix_orders_broker ON orders(broker_order_id);
CREATE TABLE IF NOT EXISTS fills(
  id INTEGER PRIMARY KEY AUTOINCREMENT, order_id INTEGER REFERENCES orders(id),
  broker_fill_id TEXT, ts TEXT, qty REAL, price REAL);
CREATE TABLE IF NOT EXISTS trades(
  id INTEGER PRIMARY KEY AUTOINCREMENT, setup_id INTEGER, position_id INTEGER, symbol TEXT,
  side TEXT, is_shadow INTEGER DEFAULT 0, variant TEXT, entry_at TEXT, exit_at TEXT,
  entry_price REAL, avg_exit_price REAL, qty INTEGER, pnl REAL, r_multiple REAL,
  exit_reason TEXT, catalyst TEXT, ai_confidence REAL, rvol REAL, impulse_pct REAL, atr REAL,
  entry_latency_ms REAL, news_latency_s REAL, mfe_r REAL, mae_r REAL, strategy_version TEXT);
CREATE INDEX IF NOT EXISTS ix_trades_exit ON trades(exit_at);
CREATE TABLE IF NOT EXISTS daily_stats(
  session_date TEXT, strategy_version TEXT, start_equity REAL, end_equity REAL,
  realized_pnl REAL, unrealized_pnl REAL, trades INTEGER, day_trades INTEGER,
  kill_switch_at TEXT, trading_disabled INTEGER DEFAULT 0,
  PRIMARY KEY(session_date, strategy_version));
CREATE TABLE IF NOT EXISTS system_events(
  id INTEGER PRIMARY KEY AUTOINCREMENT, ts TEXT, level TEXT, component TEXT, event TEXT,
  message TEXT, data_json TEXT);
CREATE TABLE IF NOT EXISTS timeline(
  id INTEGER PRIMARY KEY AUTOINCREMENT, ts TEXT, symbol TEXT, setup_id INTEGER,
  stage TEXT, message TEXT, data_json TEXT, strategy_version TEXT);
CREATE INDEX IF NOT EXISTS ix_timeline_ts ON timeline(ts);
CREATE TABLE IF NOT EXISTS equity_snapshots(
  id INTEGER PRIMARY KEY AUTOINCREMENT, ts TEXT, ledger_equity REAL, broker_equity REAL,
  buying_power REAL, reason TEXT, strategy_version TEXT);
CREATE TABLE IF NOT EXISTS strategy_versions(
  version TEXT PRIMARY KEY, params_json TEXT, params_hash TEXT, created_at TEXT, notes TEXT);
CREATE TABLE IF NOT EXISTS kv(key TEXT PRIMARY KEY, value TEXT, updated_at TEXT);
"""


MIGRATIONS = [("setups", "ref_source", "TEXT")]


class StrategyVersionMismatch(RuntimeError):
    """Params changed under an existing STRATEGY_VERSION: bump STRATEGY_VERSION."""


class Database:
    def __init__(self, path: str | Path) -> None:
        self.path = str(path)
        if self.path != ":memory:":
            Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(self.path, check_same_thread=False, isolation_level=None)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._conn.execute("PRAGMA busy_timeout=5000")

    @staticmethod
    def _sql(sql: str) -> str:
        """The one place placeholders are translated (sqlite uses `?` natively)."""
        return sql

    def init_schema(self) -> None:
        with self._lock:
            self._conn.executescript(SCHEMA)
            for table, col, decl in MIGRATIONS:  # additive migrations for databases created by older schemas
                have = {r["name"] for r in self._conn.execute(f"PRAGMA table_info({table})")}
                if col not in have:
                    self._conn.execute(f"ALTER TABLE {table} ADD COLUMN {col} {decl}")

    def execute(self, sql: str, params: Sequence[Any] = ()) -> sqlite3.Cursor:
        with self._lock:
            return self._conn.execute(self._sql(sql), tuple(params))

    def query(self, sql: str, params: Sequence[Any] = ()) -> list[dict]:
        return [dict(r) for r in self.execute(sql, params).fetchall()]

    def one(self, sql: str, params: Sequence[Any] = ()) -> dict | None:
        r = self.execute(sql, params).fetchone()
        return dict(r) if r else None

    def insert(self, table: str, row: dict) -> int:
        cols = list(row)
        sql = f"INSERT INTO {table}({','.join(cols)}) VALUES({','.join('?' * len(cols))})"
        return int(self.execute(sql, [row[c] for c in cols]).lastrowid or 0)

    def update(self, table: str, id: Any, fields: dict, pk: str = "id") -> None:
        if not fields:
            return
        sets = ",".join(f"{c}=?" for c in fields)
        self.execute(f"UPDATE {table} SET {sets} WHERE {pk}=?", [*fields.values(), id])

    def upsert(self, table: str, row: dict, key_cols: Iterable[str]) -> None:
        keys = list(key_cols)
        cols = list(row)
        upd = [c for c in cols if c not in keys]
        action = "DO UPDATE SET " + ",".join(f"{c}=excluded.{c}" for c in upd) if upd else "DO NOTHING"
        sql = (f"INSERT INTO {table}({','.join(cols)}) VALUES({','.join('?' * len(cols))}) "
               f"ON CONFLICT({','.join(keys)}) {action}")
        self.execute(sql, [row[c] for c in cols])

    def close(self) -> None:
        with self._lock:
            self._conn.close()


def register_strategy_version(db: Database, version: str, params: StrategyParams,
                              notes: str = "", now: str | None = None) -> str:
    """CONTRACT §11 boot rule. Returns the params hash; raises on hash mismatch."""
    h = params_hash(params)
    row = db.one("SELECT params_hash FROM strategy_versions WHERE version=?", (version,))
    if row is None:
        from dataclasses import asdict
        db.insert("strategy_versions", {
            "version": version, "params_json": json.dumps(asdict(params), sort_keys=True),
            "params_hash": h, "created_at": now or utc_iso(RealClock().now()), "notes": notes})
    elif row["params_hash"] != h:
        raise StrategyVersionMismatch(
            f"params changed for {version}: bump STRATEGY_VERSION (stored {row['params_hash'][:12]}, now {h[:12]})")
    return h
