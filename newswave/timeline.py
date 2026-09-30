"""The 'watch the bot think' log (spec 27): timeline table + structured JSON log line."""
from __future__ import annotations

import json
import logging

from .clock import Clock, utc_iso
from .db import Database

log = logging.getLogger("newswave")


def _dump(data: dict) -> str:
    return json.dumps(data, default=str, sort_keys=True)


class Timeline:
    def __init__(self, db: Database, clock: Clock, strategy_version: str) -> None:
        self.db, self.clock, self.strategy_version = db, clock, strategy_version

    def log(self, symbol: str | None, stage: str, message: str,
            setup_id: int | None = None, **data: object) -> int:
        ts = utc_iso(self.clock.now())
        rid = self.db.insert("timeline", {
            "ts": ts, "symbol": symbol, "setup_id": setup_id, "stage": str(stage),
            "message": message, "data_json": _dump(data), "strategy_version": self.strategy_version})
        log.info(json.dumps({"ts": ts, "kind": "timeline", "symbol": symbol, "setup_id": setup_id,
                             "stage": str(stage), "message": message, "data": data},
                            default=str, sort_keys=True))
        return rid

    def system_event(self, level: str, component: str, event: str, message: str, **data: object) -> int:
        return system_event(self.db, self.clock, level, component, event, message, **data)


def system_event(db: Database, clock: Clock, level: str, component: str, event: str,
                 message: str, **data: object) -> int:
    ts = utc_iso(clock.now())
    rid = db.insert("system_events", {"ts": ts, "level": level.upper(), "component": component,
                                      "event": event, "message": message, "data_json": _dump(data)})
    log.log(getattr(logging, level.upper(), logging.INFO), json.dumps(
        {"ts": ts, "kind": "system", "component": component, "event": event,
         "message": message, "data": data}, default=str, sort_keys=True))
    return rid
