"""Pure performance statistics for trades (dicts with pnl, r_multiple, exit_at) and equity curves."""
from __future__ import annotations

import math
import statistics
from datetime import datetime
from typing import Any, Callable

from .clock import parse_iso, to_et


def _dt(x: Any) -> datetime:
    return parse_iso(x) if isinstance(x, str) else x


def _drawdown(values: list[float]) -> tuple[float, float]:
    """(max drawdown dollars, max drawdown pct 0-100) from a running peak."""
    peak, dd, dd_pct = values[0], 0.0, 0.0
    for v in values:
        peak = max(peak, v)
        if peak - v > dd:
            dd = peak - v
        if peak > 0:
            dd_pct = max(dd_pct, (peak - v) / peak * 100)
    return dd, dd_pct


def _sharpe(curve: list[tuple[datetime, float]]) -> float | None:
    last: dict = {}
    for ts, eq in sorted(curve, key=lambda x: _dt(x[0])):
        last[to_et(_dt(ts)).date()] = eq  # last value per ET date
    vals = [last[d] for d in sorted(last)]
    rets = [b / a - 1 for a, b in zip(vals, vals[1:]) if a]
    if len(rets) < 2:  # sample std needs >= 2 daily returns
        return None
    sd = statistics.stdev(rets)
    return None if sd == 0 else statistics.fmean(rets) / sd * math.sqrt(252)


def summarize(trades: list[dict], equity_curve: list[tuple[datetime, float]] | None = None,
              starting_equity: float = 6000.0) -> dict:
    """avg_loss is negative; win_rate is a 0-1 fraction; max_drawdown_pct is 0-100.
    A trade with pnl == 0 is neither win nor loss. Drawdown uses the equity curve if given,
    else cumulative pnl (ordered by exit_at) on `starting_equity`."""
    pnls = [float(t["pnl"]) for t in trades]
    wins = [x for x in pnls if x > 0]
    losses = [x for x in pnls if x < 0]
    rs = [float(t["r_multiple"]) for t in trades if t.get("r_multiple") is not None]
    n = len(pnls)
    if equity_curve:
        eqs = [v for _, v in sorted(equity_curve, key=lambda x: _dt(x[0]))]
    else:
        ordered = sorted(trades, key=lambda t: t.get("exit_at") or "")
        eqs, run = [starting_equity], starting_equity
        for t in ordered:
            run += float(t["pnl"])
            eqs.append(run)
    dd, dd_pct = _drawdown(eqs)
    return {
        "n": n, "wins": len(wins), "losses": len(losses),
        "win_rate": len(wins) / n if n else 0.0,
        "avg_win": statistics.fmean(wins) if wins else 0.0,
        "avg_loss": statistics.fmean(losses) if losses else 0.0,
        "profit_factor": sum(wins) / -sum(losses) if losses else None,
        "expectancy_dollars": sum(pnls) / n if n else 0.0,
        "expectancy_r": statistics.fmean(rs) if rs else 0.0,
        "total_pnl": sum(pnls),
        "max_drawdown_dollars": dd, "max_drawdown_pct": dd_pct,
        "sharpe": _sharpe(equity_curve) if equity_curve else None,
    }


def group_by(trades: list[dict], key_fn: Callable[[dict], Any]) -> dict[str, dict]:
    groups: dict[str, list[dict]] = {}
    for t in trades:
        groups.setdefault(str(key_fn(t)), []).append(t)
    return {k: summarize(v) for k, v in groups.items()}


def bucket(value: float | None, edges: list[float]) -> str:
    """edges [1,2,3] -> '<1', '1-2', '2-3', '3+' (lower edge inclusive)."""
    if value is None:
        return "n/a"
    e = sorted(edges)
    if value < e[0]:
        return f"<{e[0]:g}"
    for lo, hi in zip(e, e[1:]):
        if value < hi:
            return f"{lo:g}-{hi:g}"
    return f"{e[-1]:g}+"
