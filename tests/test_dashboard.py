from __future__ import annotations

import json
import threading
import urllib.request
from datetime import UTC, datetime
from pathlib import Path

import pytest

from newswave.clock import ReplayClock
from newswave.dashboard import server as S

V = "v1"
NOW = datetime(2026, 9, 30, 15, 0, tzinfo=UTC)  # 11:00 ET
XSS = "<script>alert(1)</script>"


def z(day: int, hhmm: str, month: int = 9) -> str:
    return f"2026-{month:02d}-{day:02d}T{hhmm}:00.000Z"


def seed(db) -> None:
    db.insert("strategy_versions", {"version": V, "params_json": "{}", "params_hash": "h", "created_at": z(1, "00:00")})
    news = [("n0", z(1, "14:00", 8), "old"), ("n1", z(30, "14:03"), XSS), ("n2", z(30, "14:10"), "msft flat"),
            ("n3", z(30, "14:20"), "tsla bad"), ("n4", z(30, "14:30"), "nvda up")]
    for a, ts, h in news:
        db.insert("news_events", {"article_id": a, "received_at": ts, "headline": h, "is_duplicate": 0, "strategy_version": V})
    for a, sym, d, c, m, cat in [("n1", "AAPL", "BULLISH", 0.9, 1, "guidance"), ("n2", "MSFT", "NEUTRAL", 0.6, 0, "other"),
                                 ("n3", "TSLA", "BEARISH", 0.8, 1, "recall"), ("n4", "NVDA", "BULLISH", 0.85, 1, "contract")]:
        db.insert("ai_classifications", {"article_id": a, "symbol": sym, "direction": d, "confidence": c, "material": m,
                                         "catalyst": cat, "created_at": z(30, "14:31"), "strategy_version": V})

    def setup(a, sym, variant, shadow, stage, mx, rej=None, d="BULLISH", mat=1):
        return db.insert("setups", {"article_id": a, "symbol": sym, "variant": variant, "is_shadow": shadow, "side": "LONG",
                                    "stage": stage, "max_stage": mx, "reject_reason": rej, "ai_direction": d, "ai_material": mat,
                                    "created_at": z(30, "14:05"), "news_received_at": z(30, "14:05"), "strategy_version": V})
    setup("n1", "AAPL", "production", 0, "CLOSED", "CLOSED")
    setup("n2", "MSFT", "production", 0, "REJECTED", "CLASSIFIED", "AI_NEUTRAL", "NEUTRAL", 0)
    setup("n3", "TSLA", "production", 0, "REJECTED", "CLASSIFIED", "AI_BEARISH", "BEARISH")
    setup("n4", "NVDA", "production", 0, "WAITING_FOR_BREAKOUT", "WAITING_FOR_BREAKOUT")
    setup("n2", "MSFT", "neutral_news", 1, "CLOSED", "IN_POSITION", None, "NEUTRAL", 0)

    def trade(variant, shadow, exit_at, pnl, r, sym="AAPL"):
        db.insert("trades", {"symbol": sym, "side": "LONG", "is_shadow": shadow, "variant": variant, "entry_at": exit_at,
                             "exit_at": exit_at, "entry_price": 10, "avg_exit_price": 11, "qty": 10, "pnl": pnl,
                             "r_multiple": r, "strategy_version": V})
    trade("production", 0, z(30, "14:00"), 100, 2)
    trade("production", 0, z(30, "14:30"), -50, -1)
    trade("production", 0, z(30, "14:50"), 50, 1)
    trade("production", 0, z(10, "15:00"), -30, -0.5)
    trade("neutral_news", 1, z(30, "14:40"), 10, 0.2, "MSFT")
    for ts, le, be in [(z(31, "20:00", 8), 6000, 6000), (z(10, "15:00"), 5970, 5970), (z(30, "14:00"), 6070, 6071)]:
        db.insert("equity_snapshots", {"ts": ts, "ledger_equity": le, "broker_equity": be, "strategy_version": V})
    db.insert("daily_stats", {"session_date": "2026-09-30", "strategy_version": V, "unrealized_pnl": 5.0, "trading_disabled": 0})
    db.insert("positions", {"symbol": "NVDA", "side": "LONG", "is_shadow": 0, "variant": "production", "qty_open": 5,
                            "status": "OPEN", "opened_at": z(30, "14:40"), "strategy_version": V})
    db.insert("positions", {"symbol": "MSFT", "side": "LONG", "is_shadow": 1, "variant": "neutral_news", "qty_open": 5,
                            "status": "OPEN", "opened_at": z(30, "14:41"), "strategy_version": V})
    db.insert("positions", {"symbol": "OLD", "side": "LONG", "is_shadow": 0, "variant": "production", "qty_open": 0,
                            "status": "CLOSED", "opened_at": z(29, "14:41"), "closed_at": z(29, "15:41"), "strategy_version": V})
    for ts, sym, msg in [("14:03:01", "AAPL", "news received"), ("14:03:02", "AAPL", "Claude: BULLISH 0.92"),
                         ("14:04:00", "MSFT", "news received"), ("14:05:00", "AAPL", "subscribed")]:
        db.insert("timeline", {"ts": z(30, ts[:5]).replace("00.000", ts[6:] + ".000"), "symbol": sym, "stage": "NEWS",
                               "message": msg, "data_json": "{}", "strategy_version": V})


@pytest.fixture
def db(tmp_db):
    seed(tmp_db)
    return tmp_db


def test_summary(db):
    s = S.summary(db, NOW, V)
    assert s["ledger_equity"] == 6070 and s["broker_equity"] == 6071
    assert s["daily_realized"] == 100 and s["daily_unrealized"] == 5 and s["daily_pnl"] == 105
    assert s["total_pnl"] == 70 and s["kill_switch"]["active"] is False and s["execution_mode"] is None
    db.execute("UPDATE daily_stats SET trading_disabled=1, kill_switch_at=?", (z(30, "14:45"),))
    db.insert("kv", {"key": "execution_mode", "value": "PAPER"})
    s = S.summary(db, NOW, None)  # version resolved from strategy_versions
    assert s["kill_switch"]["active"] and s["execution_mode"] == "PAPER" and s["strategy_version"] == V


def test_positions_setups_news(db):
    p = S.positions(db, V)
    assert [r["symbol"] for r in p["production"]] == ["NVDA"] and [r["symbol"] for r in p["shadow"]] == ["MSFT"]
    assert [(r["symbol"], r["stage"]) for r in S.setups_active(db, V) if not r["is_shadow"]] == [("NVDA", "WAITING_FOR_BREAKOUT")]
    n = S.news_today(db, NOW, V)
    assert n["counts"] == {"BULLISH": 2, "NEUTRAL": 1, "BEARISH": 1} and n["material"] == 3
    assert len(n["stories"]) == 4 and n["stories"][0]["symbol"] == "NVDA"
    by = {r["symbol"]: r for r in n["stories"]}
    assert by["MSFT"]["reject_reason"] == "AI_NEUTRAL" and by["AAPL"]["headline"] == XSS and by["AAPL"]["time_et"] == "10:03:00"
    assert by["TSLA"]["direction"] == "BEARISH" and by["AAPL"]["catalyst"] == "guidance"


def test_funnel(db):
    f = S.funnel(db, NOW, V, 30)
    p = f["production"]
    assert {k: p[k] for k in ("stories", "setups", "classified", "bullish", "material", "gate_passed", "had_volume",
                              "had_momentum", "pullback", "breakout_signals", "entries", "closed_trades")} == dict(
        stories=4, setups=4, classified=4, bullish=2, material=2, gate_passed=2, had_volume=2, had_momentum=2,
        pullback=2, breakout_signals=1, entries=1, closed_trades=4)
    assert p["rejects"] == {"AI_NEUTRAL": 1, "AI_BEARISH": 1}
    nn = f["shadow"]["neutral_news"]
    assert (nn["setups"], nn["entries"], nn["closed_trades"]) == (1, 1, 1)
    assert f["shadow"]["rvol_1_5"]["setups"] == 0


def test_trades_and_stats(db):
    t = S.trades(db, V, 3)
    assert len(t) == 3 and t[0]["exit_at"] >= t[1]["exit_at"] and any(r["is_shadow"] for r in S.trades(db, V))
    st = S.stats(db, NOW, V)
    today, allt = st["production"]["today"], st["production"]["all_time"]
    assert today["n"] == 3 and today["wins"] == 2 and today["losses"] == 1
    assert today["avg_win"] == 75 and today["avg_loss"] == -50 and today["profit_factor"] == 3
    assert today["expectancy_dollars"] == pytest.approx(100 / 3) and today["expectancy_r"] == pytest.approx(2 / 3)
    assert allt["n"] == 4 and allt["total_pnl"] == 70 and allt["max_drawdown_dollars"] == 30
    sh = st["shadow"]["neutral_news"]
    assert sh["n"] == 1 and sh["win_rate"] == 1.0 and "sharpe" in sh and "production" not in st["shadow"]


def test_timeline_paging_and_equity(db):
    rows = S.timeline(db, V)
    assert [r["line"] for r in rows][:2] == ["10:03:01 AAPL news received", "10:03:02 AAPL Claude: BULLISH 0.92"]
    assert [r["symbol"] for r in rows] == ["AAPL", "AAPL", "MSFT", "AAPL"]
    nxt = S.timeline(db, V, since_id=rows[1]["id"])
    assert [r["message"] for r in nxt] == ["news received", "subscribed"]
    assert [r["message"] for r in S.timeline(db, V, symbol="msft")] == ["news received"]
    assert S.timeline(db, V, since_id=rows[-1]["id"]) == []
    assert [p["ledger_equity"] for p in S.equity(db, V)] == [6000, 5970, 6070]


def test_server_endpoints_and_xss(db):
    with pytest.raises(ValueError):
        S.make_server(db.path, host="0.0.0.0", port=0)
    srv = S.make_server(db.path, port=0, strategy_version=V, clock=ReplayClock(NOW))
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    base = f"http://127.0.0.1:{srv.server_address[1]}"
    try:
        for p in ["summary", "positions", "news_today", "setups", "funnel?days=7", "trades?limit=5", "stats",
                  "timeline?since_id=0&symbol=AAPL", "equity"]:
            with urllib.request.urlopen(f"{base}/api/{p}") as r:
                assert r.status == 200 and r.headers["Content-Type"] == "application/json"
                json.loads(r.read())
        with urllib.request.urlopen(f"{base}/api/news_today") as r:
            body = r.read().decode()
        assert json.loads(body)["stories"][-1]["headline"] == XSS  # served as JSON data, never markup
        with urllib.request.urlopen(f"{base}/") as r:
            assert r.status == 200 and b"NewsWave" in r.read()
        with pytest.raises(urllib.error.HTTPError) as e:
            urllib.request.urlopen(f"{base}/api/nope")
        assert e.value.code == 404
        with pytest.raises(urllib.error.HTTPError) as e:
            urllib.request.urlopen(f"{base}/api/funnel?days=abc")
        assert e.value.code == 400
    finally:
        srv.shutdown()
        srv.server_close()


def test_index_never_uses_innerhtml():
    src = (Path(S.__file__).parent / "index.html").read_text(encoding="utf-8")
    for bad in ("innerHTML", "outerHTML", "insertAdjacentHTML", "document.write", "http://", "https://cdn"):
        assert bad not in src.replace('http://www.w3.org/2000/svg', "")
    assert "textContent" in src and "setInterval(tick,2000)" in src


# ------------------------------------------------------------------ review fixes
def test_positions_live_feeds_unrealized_and_position_rows(db):
    live = [{"symbol": "NVDA", "side": "LONG", "qty": 10, "entry": 100, "last": 98.75, "unrealized_usd": -12.5,
             "unrealized_r": -0.4}]
    db.insert("kv", {"key": "positions_live", "value": json.dumps(live)})
    s = S.summary(db, NOW, V)
    assert s["daily_unrealized"] == -12.5 and s["daily_pnl"] == 100 - 12.5   # daily_stats' 5 is not used
    row = S.positions(db, V)["production"][0]
    assert (row["last_price"], row["unrealized_usd"], row["unrealized_r"]) == (98.75, -12.5, -0.4)
    db.execute("UPDATE kv SET value='[]' WHERE key='positions_live'")          # flat: live wins with 0
    assert S.summary(db, NOW, V)["daily_unrealized"] == 0
    db.execute("UPDATE kv SET value='not json' WHERE key='positions_live'")    # garbage: fall back to daily_stats
    assert S.summary(db, NOW, V)["daily_unrealized"] == 5


def test_limit_is_clamped_to_500(db):
    for i in range(600):
        db.insert("timeline", {"ts": z(30, "14:00"), "symbol": "X", "message": f"m{i}", "strategy_version": V})
    assert len(S.timeline(db, V, limit=10**9)) == 500
    assert S.timeline(db, V, limit=-5) and len(S.timeline(db, V, limit=-5)) == 1     # floor of 1
    for i in range(520):
        db.insert("trades", {"symbol": "X", "is_shadow": 1, "variant": "neutral_news", "exit_at": z(1, "14:00", 9),
                             "pnl": 0, "strategy_version": V})
    assert len(S.trades(db, V, 10**6)) == 500


def test_host_header_must_be_loopback_name_with_our_port(db):
    import http.client
    srv = S.make_server(db.path, port=0, strategy_version=V, clock=ReplayClock(NOW))
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    port = srv.server_address[1]
    try:
        def get(host):
            c = http.client.HTTPConnection("127.0.0.1", port)
            c.putrequest("GET", "/api/summary", skip_host=True)
            if host is not None:
                c.putheader("Host", host)
            c.endheaders()
            r = c.getresponse()
            r.read()
            return r.status
        assert get(f"127.0.0.1:{port}") == 200 and get(f"localhost:{port}") == 200
        assert get(f"evil.example:{port}") == 403           # DNS rebinding
        assert get("127.0.0.1") == 403 and get(f"127.0.0.1:{port + 1}") == 403 and get(None) == 403
    finally:
        srv.shutdown()
        srv.server_close()
