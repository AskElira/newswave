from __future__ import annotations

import dataclasses
import json

import pytest

from newswave.config import StrategyParams
from newswave.db import Database, StrategyVersionMismatch, register_strategy_version
from newswave.timeline import Timeline, system_event

TABLES = {"news_events", "ai_classifications", "symbols", "market_events", "setups", "orders", "fills",
          "positions", "trades", "daily_stats", "system_events", "timeline", "equity_snapshots",
          "strategy_versions", "kv"}


def test_schema_idempotent_and_wal(tmp_db):
    tmp_db.init_schema()
    names = {r["name"] for r in tmp_db.query("SELECT name FROM sqlite_master WHERE type='table'")}
    assert TABLES <= names
    assert tmp_db.one("PRAGMA journal_mode")["journal_mode"] == "wal"
    assert tmp_db.one("PRAGMA foreign_keys")["foreign_keys"] == 1


def test_insert_update_upsert_query(tmp_db):
    i = tmp_db.insert("setups", {"article_id": "a", "symbol": "X", "variant": "production",
                                 "stage": "NEWS", "strategy_version": "v"})
    tmp_db.update("setups", i, {"stage": "CLASSIFIED", "rvol": 2.5})
    assert tmp_db.one("SELECT stage, rvol FROM setups WHERE id=?", (i,)) == {"stage": "CLASSIFIED", "rvol": 2.5}
    tmp_db.upsert("kv", {"key": "k", "value": "1", "updated_at": "t"}, ["key"])
    tmp_db.upsert("kv", {"key": "k", "value": "2", "updated_at": "t"}, ["key"])
    assert tmp_db.query("SELECT value FROM kv") == [{"value": "2"}]
    assert tmp_db.one("SELECT 1 FROM kv WHERE key=?", ("nope",)) is None


def test_foreign_key_enforced(tmp_db):
    import sqlite3
    with pytest.raises(sqlite3.IntegrityError):
        tmp_db.insert("fills", {"order_id": 999, "qty": 1, "price": 1})


def test_strategy_version_rule(tmp_db):
    p = StrategyParams()
    h = register_strategy_version(tmp_db, "v1", p)
    assert register_strategy_version(tmp_db, "v1", p) == h
    with pytest.raises(StrategyVersionMismatch):
        register_strategy_version(tmp_db, "v1", dataclasses.replace(p, rvol_min=3.0))
    register_strategy_version(tmp_db, "v1.1", dataclasses.replace(p, rvol_min=3.0))
    assert len(tmp_db.query("SELECT * FROM strategy_versions")) == 2


def test_timeline_and_system_event(tmp_db, replay_clock, caplog):
    tl = Timeline(tmp_db, replay_clock, "v1")
    with caplog.at_level("INFO", logger="newswave"):
        tl.log("NVDA", "NEWS", "news received", setup_id=None, rvol=3.4)
        system_event(tmp_db, replay_clock, "warning", "ws", "drop", "dropped", n=1)
    row = tmp_db.one("SELECT * FROM timeline")
    assert row["symbol"] == "NVDA" and row["ts"].endswith("Z") and json.loads(row["data_json"]) == {"rvol": 3.4}
    assert tmp_db.one("SELECT level FROM system_events")["level"] == "WARNING"
    assert any(json.loads(r.message)["kind"] == "timeline" for r in caplog.records if r.name == "newswave")


def test_setups_ref_source_column_and_migration_of_an_old_database(tmp_path):
    import sqlite3
    path = tmp_path / "old.db"
    from newswave.db import SCHEMA
    old = SCHEMA.replace(" ref_source TEXT,", "")   # the schema as it was before the column existed
    assert old != SCHEMA
    c = sqlite3.connect(path)
    c.executescript(old)
    c.execute("INSERT INTO setups(article_id, symbol, variant, strategy_version) VALUES('a','X','production','v')")
    c.commit()
    c.close()
    db = Database(path)
    db.init_schema()
    db.init_schema()            # idempotent
    assert db.one("SELECT ref_source FROM setups")["ref_source"] is None
    db.update("setups", 1, {"ref_source": "1m_open"})
    assert db.one("SELECT ref_source FROM setups")["ref_source"] == "1m_open"
    fresh = Database(":memory:")
    fresh.init_schema()
    assert "ref_source" in {r["name"] for r in fresh.query("PRAGMA table_info(setups)")}
