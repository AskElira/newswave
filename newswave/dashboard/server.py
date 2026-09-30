"""Local read-only dashboard (SPEC 26/27): stdlib HTTP server + pure query functions.

Query functions take any object with `.query(sql, params)` / `.one(sql, params)` (a `Database`
or the read-only `_RO` below) and return JSON-able dicts. Bound to loopback only.
"""
from __future__ import annotations

import json
import sqlite3
from datetime import date, datetime, time, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

from ..clock import ET, UTC, Clock, RealClock, parse_iso, to_et, utc_iso
from ..models import Stage
from ..stats import summarize

STARTING_EQUITY = 6000.0
INDEX = Path(__file__).with_name("index.html")
LOOPBACK = ("127.0.0.1", "localhost", "::1")
SHADOW_VARIANTS = ("neutral_news", "bearish_short", "low_confidence", "rvol_1_5", "second_pullback")
ACTIVE_EXCLUDED = (Stage.REJECTED.value, Stage.CLOSED.value)


class _RO:
    """Read-only SQLite connection (own process; WAL lets it read while the daemon writes)."""

    def __init__(self, path: str | Path) -> None:
        uri = f"{Path(path).resolve().as_uri()}?mode=ro"
        self._c = sqlite3.connect(uri, uri=True, timeout=5)
        self._c.row_factory = sqlite3.Row

    def query(self, sql: str, params=()) -> list[dict]:
        return [dict(r) for r in self._c.execute(sql, tuple(params)).fetchall()]

    def one(self, sql: str, params=()) -> dict | None:
        r = self._c.execute(sql, tuple(params)).fetchone()
        return dict(r) if r else None

    def close(self) -> None:
        self._c.close()


# ---------------------------------------------------------------- helpers
def day_bounds(d: date) -> tuple[str, str]:
    """[start, end) of an ET calendar day as UTC ISO strings (same format the daemon stores)."""
    a = datetime.combine(d, time(0), ET)
    return utc_iso(a), utc_iso(datetime.combine(d + timedelta(days=1), time(0), ET))


def today_et(now: datetime) -> date:
    return to_et(now).date()


def resolve_version(db, version: str | None) -> str | None:
    if version:
        return version
    r = db.one("SELECT version FROM strategy_versions ORDER BY created_at DESC LIMIT 1")
    return r["version"] if r else None


def _et_hms(ts: str | None) -> str | None:
    return to_et(parse_iso(ts)).strftime("%H:%M:%S") if ts else None


def _trade_curve(trades: list[dict], start: float = STARTING_EQUITY) -> list[tuple[str, float]]:
    eq, out = start, []
    for t in sorted(trades, key=lambda t: t.get("exit_at") or ""):
        eq += float(t["pnl"])
        out.append((t["exit_at"], eq))
    return out


def _stats(trades: list[dict], curve: list[tuple[str, float]] | None = None) -> dict:
    """stats.summarize with a curve (given, else cumulative-pnl) so Sharpe/drawdown are populated."""
    c = curve if curve and len(curve) > 1 else _trade_curve(trades)
    if c:
        c = [(parse_iso(t), v) for t, v in c if t]
    return summarize(trades, c or None, STARTING_EQUITY)


def _prod_trades(db, version: str, start: str | None = None, end: str | None = None) -> list[dict]:
    sql, p = "SELECT * FROM trades WHERE is_shadow=0 AND strategy_version=?", [version]
    if start:
        sql += " AND exit_at>=? AND exit_at<?"
        p += [start, end]
    return db.query(sql + " ORDER BY exit_at", p)


def _curve(db, version: str, start: str | None = None, end: str | None = None) -> list[tuple[str, float]]:
    sql, p = "SELECT ts, ledger_equity FROM equity_snapshots WHERE strategy_version=? AND ledger_equity IS NOT NULL", [version]
    if start:
        sql += " AND ts>=? AND ts<?"
        p += [start, end]
    return [(r["ts"], r["ledger_equity"]) for r in db.query(sql + " ORDER BY ts", p)]


# ---------------------------------------------------------------- queries
def summary(db, now: datetime, version: str | None = None, starting_equity: float = STARTING_EQUITY) -> dict:
    v = resolve_version(db, version)
    d = today_et(now)
    a, b = day_bounds(d)
    snap = db.one("SELECT * FROM equity_snapshots WHERE strategy_version=? ORDER BY ts DESC, id DESC LIMIT 1", (v,))
    realized = sum(t["pnl"] for t in _prod_trades(db, v, a, b))
    ds = db.one("SELECT * FROM daily_stats WHERE session_date=? AND strategy_version=?", (d.isoformat(), v))
    live = _live_positions(db)
    if live is not None:  # the daemon's own mark-to-market beats the (rarely written) daily_stats column
        unreal = sum(float(x.get("unrealized_usd") or 0.0) for x in live)
    else:
        unreal = ds["unrealized_pnl"] if ds else None
    ledger = snap["ledger_equity"] if snap else None
    total = (ledger - starting_equity) if ledger is not None else sum(t["pnl"] for t in _prod_trades(db, v))
    mode = db.one("SELECT value FROM kv WHERE key='execution_mode'")
    return {
        "strategy_version": v, "session_date": d.isoformat(), "starting_equity": starting_equity,
        "ledger_equity": ledger, "broker_equity": snap["broker_equity"] if snap else None,
        "equity_ts": snap["ts"] if snap else None,
        "daily_realized": realized, "daily_unrealized": unreal, "daily_pnl": realized + (unreal or 0.0),
        "total_pnl": total,
        "kill_switch": {"active": bool(ds and (ds["trading_disabled"] or ds["kill_switch_at"])),
                        "at": ds["kill_switch_at"] if ds else None},
        "execution_mode": mode["value"] if mode else None,
    }


def _live_positions(db) -> list[dict] | None:
    """kv.positions_live (written by the daemon heartbeat): [{symbol, qty, entry, last, unrealized_usd, unrealized_r}]."""
    r = db.one("SELECT value FROM kv WHERE key='positions_live'")
    if not r or not r["value"]:
        return None
    try:
        rows = json.loads(r["value"])
    except ValueError:
        return None
    return [x for x in rows if isinstance(x, dict)] if isinstance(rows, list) else None


def positions(db, version: str | None = None) -> dict:
    v = resolve_version(db, version)
    rows = db.query("SELECT * FROM positions WHERE strategy_version=? AND closed_at IS NULL "
                    "AND COALESCE(qty_open,0)>0 ORDER BY opened_at", (v,))
    live = {x.get("symbol"): x for x in _live_positions(db) or []}
    for r in rows:
        if not r["is_shadow"] and r["symbol"] in live:
            x = live[r["symbol"]]
            r.update(last_price=x.get("last"), unrealized_usd=x.get("unrealized_usd"),
                     unrealized_r=x.get("unrealized_r"))
    return {"production": [r for r in rows if not r["is_shadow"]],
            "shadow": [r for r in rows if r["is_shadow"]]}


def news_today(db, now: datetime, version: str | None = None) -> dict:
    v = resolve_version(db, version)
    a, b = day_bounds(today_et(now))
    latest: dict = {}
    for r in db.query("SELECT * FROM ai_classifications WHERE strategy_version=? AND created_at>=? AND created_at<? "
                      "AND error IS NULL AND direction IS NOT NULL ORDER BY id", (v, a, b)):
        latest[(r["article_id"], r["symbol"])] = r
    counts = {"BULLISH": 0, "NEUTRAL": 0, "BEARISH": 0}
    material = 0
    for r in latest.values():
        counts[r["direction"]] = counts.get(r["direction"], 0) + 1
        material += 1 if r["material"] else 0
    stories = []
    for n in db.query("SELECT * FROM news_events WHERE strategy_version=? AND received_at>=? AND received_at<? "
                      "ORDER BY received_at DESC LIMIT 50", (v, a, b)):
        sets = db.query("SELECT symbol, reject_reason FROM setups WHERE article_id=? AND variant='production' "
                        "AND strategy_version=?", (n["article_id"], v))
        if not sets:
            sets = [{"symbol": None, "reject_reason": "DUPLICATE_NEWS" if n["is_duplicate"] else None}]
        for s in sets:
            c = latest.get((n["article_id"], s["symbol"]))
            stories.append({"article_id": n["article_id"], "ts": n["received_at"], "time_et": _et_hms(n["received_at"]),
                            "symbol": s["symbol"], "headline": n["headline"],
                            "direction": c["direction"] if c else None,
                            "confidence": c["confidence"] if c else None,
                            "catalyst": c["catalyst"] if c else None,
                            "reject_reason": s["reject_reason"]})
    return {"counts": counts, "material": material, "classified": len(latest), "stories": stories[:50]}


def setups_active(db, version: str | None = None) -> list[dict]:
    v = resolve_version(db, version)
    return db.query(
        "SELECT id, symbol, variant, is_shadow, side, stage, max_stage, ai_direction, ai_confidence, catalyst, "
        "ref_price, rvol, impulse_pct, pullback_bars, atr, entry_trigger, stop_price, news_received_at, updated_at "
        "FROM setups WHERE strategy_version=? AND stage NOT IN (?, ?) ORDER BY is_shadow, news_received_at DESC",
        (v, *ACTIVE_EXCLUDED))


def _rank(stage: str | None) -> int:
    try:
        return Stage(stage).rank if stage else -1
    except ValueError:
        return -1


def funnel_counts(db, version: str, start: str, end: str, variant: str = "production") -> dict:
    """One variant's funnel for setups created in [start, end). Counts are cumulative 'reached stage'."""
    rows = db.query("SELECT stage, max_stage, reject_reason, ai_direction, ai_material FROM setups "
                    "WHERE strategy_version=? AND variant=? AND created_at>=? AND created_at<?",
                    (version, variant, start, end))
    ranks = [max(_rank(r["max_stage"]), _rank(r["stage"])) for r in rows]
    at = lambda st: sum(1 for k in ranks if k >= Stage(st).rank)  # noqa: E731
    if variant == "production":
        stories = db.one("SELECT COUNT(*) c FROM news_events WHERE strategy_version=? AND is_duplicate=0 "
                         "AND received_at>=? AND received_at<?", (version, start, end))["c"]
    else:
        stories = len(rows)
    rejects: dict[str, int] = {}
    for r in rows:
        if r["reject_reason"]:
            rejects[r["reject_reason"]] = rejects.get(r["reject_reason"], 0) + 1
    closed = db.one("SELECT COUNT(*) c FROM trades WHERE strategy_version=? AND variant=? AND exit_at>=? AND exit_at<?",
                    (version, variant, start, end))["c"]
    return {
        "stories": stories, "setups": len(rows),
        "classified": sum(1 for r, k in zip(rows, ranks) if r["ai_direction"] or k >= Stage.CLASSIFIED.rank),
        "bullish": sum(1 for r in rows if r["ai_direction"] == "BULLISH"),
        "material": sum(1 for r in rows if r["ai_direction"] == "BULLISH" and r["ai_material"]),
        "gate_passed": at("WAITING_FOR_VOLUME"), "had_volume": at("WAITING_FOR_IMPULSE"),
        "had_momentum": at("WAITING_FOR_PULLBACK"), "pullback": at("WAITING_FOR_BREAKOUT"),
        "breakout_signals": at("ENTRY_SIGNAL"), "entries": at("IN_POSITION"), "closed_trades": closed,
        "rejects": dict(sorted(rejects.items(), key=lambda kv: -kv[1])),
    }


def funnel(db, now: datetime, version: str | None = None, days: int = 30) -> dict:
    v = resolve_version(db, version)
    days = max(1, min(int(days), 3650))
    start, end = utc_iso(now - timedelta(days=days)), utc_iso(now + timedelta(seconds=1))
    shadow_names = [r["variant"] for r in db.query(
        "SELECT DISTINCT variant FROM setups WHERE strategy_version=? AND is_shadow=1", (v,))]
    return {"days": days, "production": funnel_counts(db, v, start, end),
            "shadow": {s: funnel_counts(db, v, start, end, s) for s in sorted(set(shadow_names) | set(SHADOW_VARIANTS))}}


def trades(db, version: str | None = None, limit: int = 50) -> list[dict]:
    v = resolve_version(db, version)
    limit = max(1, min(int(limit), 500))
    rows = db.query("SELECT * FROM trades WHERE strategy_version=? ORDER BY exit_at DESC, id DESC LIMIT ?", (v, limit))
    for r in rows:
        r["is_shadow"] = bool(r["is_shadow"])
        r["exit_time_et"] = to_et(parse_iso(r["exit_at"])).strftime("%m-%d %H:%M:%S") if r["exit_at"] else None
    return rows


def stats(db, now: datetime, version: str | None = None) -> dict:
    v = resolve_version(db, version)
    a, b = day_bounds(today_et(now))
    prod = _prod_trades(db, v)
    out = {"production": {"all_time": _stats(prod, _curve(db, v)), "today": _stats(_prod_trades(db, v, a, b))},
           "shadow": {}}
    for r in db.query("SELECT DISTINCT variant FROM trades WHERE strategy_version=? AND is_shadow=1 ORDER BY variant", (v,)):
        rows = db.query("SELECT * FROM trades WHERE strategy_version=? AND is_shadow=1 AND variant=? ORDER BY exit_at",
                        (v, r["variant"]))
        out["shadow"][r["variant"]] = _stats(rows)
    return out


def timeline(db, version: str | None = None, since_id: int = 0, symbol: str | None = None, limit: int = 300) -> list[dict]:
    """Oldest-first page of rows with id > since_id. With since_id=0 returns the newest `limit` rows."""
    v = resolve_version(db, version)
    limit = max(1, min(int(limit), 500))
    sql, p = "SELECT * FROM timeline WHERE strategy_version=? AND id>?", [v, int(since_id)]
    if symbol:
        sql += " AND symbol=?"
        p.append(symbol.upper())
    if since_id:
        rows = db.query(sql + " ORDER BY id LIMIT ?", p + [limit])
    else:
        rows = db.query(sql + " ORDER BY id DESC LIMIT ?", p + [limit])[::-1]
    for r in rows:
        r["time_et"] = _et_hms(r["ts"])
        r["line"] = f"{r['time_et']} {r['symbol'] + ' ' if r['symbol'] else ''}{r['message']}"
        r.pop("data_json", None)
    return rows


def equity(db, version: str | None = None, limit: int = 2000) -> list[dict]:
    v = resolve_version(db, version)
    rows = db.query("SELECT ts, ledger_equity, broker_equity FROM equity_snapshots WHERE strategy_version=? "
                    "ORDER BY ts DESC LIMIT ?", (v, limit))[::-1]
    return rows


# ---------------------------------------------------------------- HTTP
def route(path: str, qs: dict, db, now: datetime, version: str | None):
    q1 = lambda k, d=None: qs.get(k, [d])[0]  # noqa: E731
    if path == "/api/summary":
        return summary(db, now, version)
    if path == "/api/positions":
        return positions(db, version)
    if path == "/api/news_today":
        return news_today(db, now, version)
    if path == "/api/setups":
        return {"setups": setups_active(db, version)}
    if path == "/api/funnel":
        return funnel(db, now, version, int(q1("days", 30)))
    if path == "/api/trades":
        return {"trades": trades(db, version, int(q1("limit", 50)))}
    if path == "/api/stats":
        return stats(db, now, version)
    if path == "/api/timeline":
        return {"rows": timeline(db, version, int(q1("since_id", 0)), q1("symbol"), int(q1("limit", 300)))}
    if path == "/api/equity":
        return {"points": equity(db, version)}
    return None


def make_server(db_path: str | Path, host: str = "127.0.0.1", port: int = 8765,
                strategy_version: str | None = None, clock: Clock | None = None) -> ThreadingHTTPServer:
    if host not in LOOPBACK:
        raise ValueError("dashboard binds to loopback only")
    clk = clock or RealClock()

    class Handler(BaseHTTPRequestHandler):
        def _send(self, code: int, body: bytes, ctype: str) -> None:
            self.send_response(code)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.send_header("X-Content-Type-Options", "nosniff")
            self.end_headers()
            self.wfile.write(body)

        def _json(self, code: int, obj) -> None:
            self._send(code, json.dumps(obj, default=str).encode(), "application/json")

        def do_GET(self) -> None:  # noqa: N802
            port = self.server.server_address[1]  # DNS rebinding: only our own loopback names are served
            if self.headers.get("Host") not in (f"127.0.0.1:{port}", f"localhost:{port}"):
                return self._json(403, {"error": "forbidden host"})
            u = urlparse(self.path)
            if u.path in ("/", "/index.html"):
                return self._send(200, INDEX.read_bytes(), "text/html; charset=utf-8")
            if not u.path.startswith("/api/"):
                return self._json(404, {"error": "not found"})
            try:
                db = _RO(db_path)
                try:
                    out = route(u.path, parse_qs(u.query), db, clk.now(), strategy_version)
                finally:
                    db.close()
            except (ValueError, TypeError):
                return self._json(400, {"error": "bad request"})
            except sqlite3.Error as e:  # generic message: never leak paths/env
                return self._json(503, {"error": "database unavailable", "kind": type(e).__name__})
            if out is None:
                return self._json(404, {"error": "not found"})
            self._json(200, out)

        def log_message(self, *a) -> None:  # quiet
            pass

    return ThreadingHTTPServer((host, port), Handler)


def serve(db_path: str | Path, host: str = "127.0.0.1", port: int = 8765,
          strategy_version: str | None = None) -> None:
    srv = make_server(db_path, host, port, strategy_version)
    print(f"NewsWave dashboard on http://{host}:{srv.server_address[1]}/")
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        srv.server_close()
