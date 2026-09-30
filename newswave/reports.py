"""Monthly report (SPEC 29): markdown + html from the database, per strategy version."""
from __future__ import annotations

import html
from datetime import date, datetime, time
from pathlib import Path

from .clock import ET, UTC, Clock, parse_iso, to_et, utc_iso
from .dashboard.server import STARTING_EQUITY, _stats, funnel_counts
from .stats import bucket, group_by

MIN_N = 20  # below this on either side of a comparison we draw no conclusion

BUCKETS: dict[str, list[float]] = {
    "rvol": [2, 3, 5], "impulse_pct": [1.5, 2, 3, 5], "atr_pct": [0.5, 1, 2],
    "latency_ms": [250, 500, 1000, 2000], "confidence": [0.75, 0.8, 0.9], "mcap_b": [2, 10],
}


def _labels(edges: list[float]) -> list[str]:
    e = sorted(edges)
    return [f"<{e[0]:g}"] + [f"{a:g}-{b:g}" for a, b in zip(e, e[1:])] + [f"{e[-1]:g}+"]


def month_bounds(month: str) -> tuple[str, str]:
    y, m = int(month[:4]), int(month[5:7])
    ny, nm = (y + 1, 1) if m == 12 else (y, m + 1)
    return (utc_iso(datetime.combine(date(y, m, 1), time(0), ET)),
            utc_iso(datetime.combine(date(ny, nm, 1), time(0), ET)))


def prev_month(d: date) -> str:
    return f"{d.year - 1}-12" if d.month == 1 else f"{d.year}-{d.month - 1:02d}"


def _f(x, d=2, pct=False) -> str:
    if x is None:
        return "n/a"
    return f"{x * 100:.1f}%" if pct else f"{x:,.{d}f}"


def _row(name: str, s: dict) -> list[str]:
    return [name, str(s["n"]), _f(s["win_rate"], pct=True), _f(s["expectancy_r"]), _f(s["total_pnl"]), _f(s["profit_factor"])]


HEAD = ["", "Trades", "Win rate", "Avg R", "Total P&L", "PF"]


def _breakdown(title: str, trades: list[dict], key_fn, order: list[str] | None = None) -> dict:
    g = group_by(trades, key_fn)
    keys = [k for k in (order or []) if k in g] + sorted((k for k in g if k not in (order or []) and k != "n/a"),
                                                          key=lambda k: -g[k]["total_pnl"])
    if "n/a" in g:
        keys.append("n/a")
    return {"title": title, "head": [title.split(" (")[0]] + HEAD[1:], "rows": [_row(k, g[k]) for k in keys]}


def _compare(label: str, a_name: str, a: list[dict], b_name: str, b: list[dict]) -> str:
    if len(a) < MIN_N or len(b) < MIN_N:
        return f"{label}: not enough data (n={len(a)} {a_name} vs n={len(b)} {b_name}); need at least {MIN_N} each side."
    sa, sb = _stats(a), _stats(b)
    hi = a_name if sa["expectancy_r"] > sb["expectancy_r"] else b_name
    return (f"{label}: {a_name} avg R {sa['expectancy_r']:+.2f} (win {sa['win_rate']*100:.0f}%, n={sa['n']}) vs "
            f"{b_name} avg R {sb['expectancy_r']:+.2f} (win {sb['win_rate']*100:.0f}%, n={sb['n']}). "
            f"{hi} is higher; descriptive only, not significance-tested.")


def _latency_answer(prod: list[dict]) -> str:
    L = [t for t in prod if t["entry_latency_ms"] is not None]
    if len(L) < 2 * MIN_N:
        return f"Entry latency: not enough data (n={len(L)}); need at least {2 * MIN_N} trades to split fast vs slow."
    L.sort(key=lambda t: t["entry_latency_ms"])
    h = len(L) // 2
    fast, slow = L[:h], L[h:]
    return _compare(f"Entry latency (split at median {L[h]['entry_latency_ms']:.0f} ms)", "fast half", fast, "slow half", slow)


def _profit_answer(prod: list[dict]) -> str:
    if len(prod) < MIN_N:
        return f"Profit concentration: not enough data (n={len(prod)}); need at least {MIN_N} trades."
    total = sum(t["pnl"] for t in prod)
    if total <= 0:
        return f"Profit concentration: no net profit this month (total {total:,.2f}), share undefined."
    by: dict[str, float] = {}
    for t in prod:
        by[t["catalyst"] or "n/a"] = by.get(t["catalyst"] or "n/a", 0.0) + t["pnl"]
    top = sorted(by.items(), key=lambda kv: -kv[1])
    parts = ", ".join(f"{k} {v / total * 100:.0f}% ({v:,.2f})" for k, v in top[:5])
    return f"Profit concentration: share of total profit {total:,.2f} by catalyst: {parts}."


def gather(db, month: str, version: str) -> dict:
    a, b = month_bounds(month)
    q = "SELECT * FROM trades WHERE strategy_version=? AND exit_at>=? AND exit_at<? AND is_shadow=%d ORDER BY exit_at"
    prod = db.query(q % 0, (version, a, b))
    shadow = db.query(q % 1, (version, a, b))
    snaps = db.query("SELECT ts, ledger_equity FROM equity_snapshots WHERE strategy_version=? AND ledger_equity IS NOT NULL "
                     "AND ts>=? AND ts<? ORDER BY ts", (version, a, b))
    before = db.one("SELECT ledger_equity FROM equity_snapshots WHERE strategy_version=? AND ledger_equity IS NOT NULL "
                    "AND ts<? ORDER BY ts DESC LIMIT 1", (version, a))
    start = before["ledger_equity"] if before else STARTING_EQUITY
    end = snaps[-1]["ledger_equity"] if snaps else start + sum(t["pnl"] for t in prod)
    curve = [(a, start)] + [(r["ts"], r["ledger_equity"]) for r in snaps]
    return {"a": a, "b": b, "prod": prod, "shadow": shadow, "start": start, "end": end, "curve": curve}


def build_sections(db, month: str, version: str) -> tuple[list[dict], dict]:
    g = gather(db, month, version)
    prod = g["prod"]
    s = _stats(prod, g["curve"])
    for t in prod:
        t["_conf"] = bucket(t["ai_confidence"], BUCKETS["confidence"])
        t["_atr"] = t["atr"] / t["entry_price"] * 100 if t["atr"] and t["entry_price"] else None
    caps = {r["symbol"]: r["market_cap"] for r in db.query("SELECT symbol, market_cap FROM symbols")}

    def cap(t):
        c = caps.get(t["symbol"])
        return "n/a" if c is None else bucket(c / 1e9, BUCKETS["mcap_b"]) + "B"

    def tod(t):
        e = to_et(parse_iso(t["entry_at"])) if t["entry_at"] else None
        return "n/a" if e is None else f"{e.hour:02d}:{30 * (e.minute // 30):02d}"

    fn = {k: (lambda t, k=k: bucket(t[k], BUCKETS[k])) for k in ("rvol", "impulse_pct")}
    tables = [
        _breakdown("Catalyst", prod, lambda t: t["catalyst"] or "n/a"),
        _breakdown("Confidence", prod, lambda t: t["_conf"], _labels(BUCKETS["confidence"])),
        _breakdown("Ticker", prod, lambda t: t["symbol"]),
        _breakdown("Market cap", prod, cap),
        _breakdown("Time of day (ET, 30 min)", prod, tod, sorted({tod(t) for t in prod})),
        _breakdown("RVOL", prod, fn["rvol"], _labels(BUCKETS["rvol"])),
        _breakdown("Initial move % (impulse)", prod, fn["impulse_pct"], _labels(BUCKETS["impulse_pct"])),
        _breakdown("ATR (% of price)", prod, lambda t: bucket(t["_atr"], BUCKETS["atr_pct"]), _labels(BUCKETS["atr_pct"])),
        _breakdown("Entry latency (ms)", prod, lambda t: bucket(t["entry_latency_ms"], BUCKETS["latency_ms"]),
                   _labels(BUCKETS["latency_ms"])),
    ]
    sh = lambda v: [t for t in g["shadow"] if t["variant"] == v]  # noqa: E731
    answers = [
        _compare("BULLISH (production) vs NEUTRAL news (shadow neutral_news)", "production", prod, "neutral_news", sh("neutral_news")),
        _compare("RVOL>=2 (production) vs RVOL 1.5-2.0 (shadow rvol_1_5)", "RVOL>=2",
                 [t for t in prod if (t["rvol"] or 0) >= 2], "rvol_1_5", sh("rvol_1_5")),
        _compare("First EMA9 pullback (production) vs second pullback (shadow second_pullback)", "first pullback", prod,
                 "second_pullback", sh("second_pullback")),
        _latency_answer(prod),
        _profit_answer(prod),
    ]
    fun = funnel_counts(db, version, g["a"], g["b"])
    fr = [[k, str(fun[k])] for k in ("stories", "classified", "bullish", "material", "gate_passed", "had_volume",
                                      "had_momentum", "pullback", "breakout_signals", "entries", "closed_trades")]
    fr += [[f"reject: {k}", str(v)] for k, v in fun["rejects"].items()]
    ret = g["end"] / g["start"] - 1 if g["start"] else None
    summ = [["Starting equity", _f(g["start"])], ["Ending equity", _f(g["end"])], ["Return", _f(ret, pct=True)],
            ["Trades", str(s["n"])], ["Wins", str(s["wins"])], ["Losses", str(s["losses"])],
            ["Win rate", _f(s["win_rate"], pct=True)], ["Average winner", _f(s["avg_win"])],
            ["Average loser", _f(s["avg_loss"])], ["Profit factor", _f(s["profit_factor"])],
            ["Expectancy ($)", _f(s["expectancy_dollars"])], ["Expectancy (R)", _f(s["expectancy_r"])],
            ["Sharpe", _f(s["sharpe"])], ["Max drawdown", f"{_f(s['max_drawdown_dollars'])} ({_f(s['max_drawdown_pct'])}%)"]]
    sections = [{"title": "Summary", "head": ["Metric", "Value"], "rows": summ},
                {"title": "Funnel", "head": ["Stage", "Count"], "rows": fr}]
    sections += tables
    sections.append({"title": "Research questions", "notes": answers})
    return sections, {"stats": s, "start": g["start"], "end": g["end"]}


def render_md(title: str, sections: list[dict]) -> str:
    out = [f"# {title}", ""]
    for sec in sections:
        out += [f"## {sec['title']}", ""]
        if "head" in sec:
            out += ["| " + " | ".join(sec["head"]) + " |", "|" + "---|" * len(sec["head"])]
            out += ["| " + " | ".join(r) + " |" for r in sec["rows"]] or ["| (no trades) |" + " |" * (len(sec["head"]) - 1)]
        for n in sec.get("notes", []):
            out.append(f"- {n}")
        out.append("")
    return "\n".join(out)


def render_html(title: str, sections: list[dict]) -> str:
    e = html.escape
    p = [f"<!doctype html><meta charset=utf-8><title>{e(title)}</title>"
         "<style>body{font:14px system-ui;max-width:900px;margin:2em auto;padding:0 1em}"
         "table{border-collapse:collapse;margin:0 0 1em}td,th{border:1px solid #8884;padding:3px 8px;text-align:left}"
         "@media(prefers-color-scheme:dark){body{background:#111;color:#ddd}}</style>", f"<h1>{e(title)}</h1>"]
    for sec in sections:
        p.append(f"<h2>{e(sec['title'])}</h2>")
        if "head" in sec:
            p.append("<table><tr>" + "".join(f"<th>{e(h)}</th>" for h in sec["head"]) + "</tr>"
                     + "".join("<tr>" + "".join(f"<td>{e(c)}</td>" for c in r) + "</tr>" for r in sec["rows"]) + "</table>")
        if sec.get("notes"):
            p.append("<ul>" + "".join(f"<li>{e(n)}</li>" for n in sec["notes"]) + "</ul>")
    return "\n".join(p)


def build_monthly_report(db, month: str, strategy_version: str, out_dir: str | Path) -> Path:
    """Write reports/<version>/<YYYY-MM>.md and .html under out_dir; returns the .md path."""
    sections, _ = build_sections(db, month, strategy_version)
    title = f"NewsWave monthly report {month} ({strategy_version})"
    d = Path(out_dir) / strategy_version
    d.mkdir(parents=True, exist_ok=True)
    md = d / f"{month}.md"
    md.write_text(render_md(title, sections), encoding="utf-8")
    (d / f"{month}.html").write_text(render_html(title, sections), encoding="utf-8")
    return md


def maybe_generate_monthly(db, clock: Clock, strategy_version: str, out_dir: str | Path) -> Path | None:
    """On month rollover generate last month's report once (kv last_report_month = current month)."""
    now = to_et(clock.now())
    cur = f"{now.year}-{now.month:02d}"
    row = db.one("SELECT value FROM kv WHERE key='last_report_month'")
    if row and row["value"] == cur:
        return None
    prev = prev_month(now.date())
    a, b = month_bounds(prev)
    has = db.one("SELECT (SELECT COUNT(*) FROM trades WHERE is_shadow=0 AND strategy_version=? AND exit_at>=? AND exit_at<?) "
                 "+ (SELECT COUNT(*) FROM equity_snapshots WHERE strategy_version=? AND ts>=? AND ts<?) c",
                 (strategy_version, a, b, strategy_version, a, b))["c"]
    path = build_monthly_report(db, prev, strategy_version, out_dir) if has else None
    db.upsert("kv", {"key": "last_report_month", "value": cur, "updated_at": utc_iso(clock.now())}, ["key"])
    return path
