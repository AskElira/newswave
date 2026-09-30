from __future__ import annotations

import dataclasses
from datetime import UTC, date, datetime

from newswave.clock import (ReplayClock, before_cutoff, is_regular_session, session_bounds, to_et,
                            utc_iso)
from newswave.models import _RISK_TOKEN, EntrySignal, RiskApproval, Side, Stage

import pytest

from newswave.config import (SYSTEM_PROMPT, LiveTradingRefused, StrategyParams, load_settings,
                             params_hash)

OK = {"TRADING_MODE": "PAPER", "ALPACA_PAPER": "true"}


def test_defaults():
    s = load_settings(OK)
    p = s.params
    assert (p.rvol_min, p.min_impulse_pct, p.no_new_entries_after, p.allow_shorts) == (2.0, 1.5, "15:30", False)
    assert s.execution_mode == "OBSERVE" and s.starting_equity == 6000 and s.strategy_version == "newswave_v1.0"
    assert p.system_prompt_sha256 and "<news>" in SYSTEM_PROMPT


def test_env_override_and_types():
    p = load_settings({**OK, "RVOL_MIN": "1.5", "ALLOW_SHORTS": "true", "MAX_SYMBOLS_PER_STORY": "2",
                       "NO_NEW_ENTRIES_AFTER": "15:00"}).params
    assert (p.rvol_min, p.allow_shorts, p.max_symbols_per_story, p.no_new_entries_after) == (1.5, True, 2, "15:00")


def test_bad_value_raises():
    with pytest.raises(ValueError, match="RVOL_MIN"):
        load_settings({**OK, "RVOL_MIN": "abc"})


def test_hash_stable_and_sensitive():
    a, b = StrategyParams(), StrategyParams()
    assert params_hash(a) == params_hash(b) == a.params_hash()
    assert params_hash(dataclasses.replace(a, rvol_min=2.5)) != params_hash(a)


def test_non_strategy_settings_not_in_hash():
    h1 = load_settings(OK).params.params_hash()
    h2 = load_settings({**OK, "DASHBOARD_PORT": "9000", "STARTING_EQUITY": "7000",
                        "STRATEGY_VERSION": "x", "EXECUTION_MODE": "PAPER"}).params.params_hash()
    assert h1 == h2


def test_bad_execution_mode():
    with pytest.raises(ValueError):
        load_settings({**OK, "EXECUTION_MODE": "LIVE"})


def test_refuses_live():
    with pytest.raises(LiveTradingRefused):
        load_settings({"TRADING_MODE": "PAPER"})


def test_stage_rank_order():
    assert Stage.NEWS.rank < Stage.ENTRY_SIGNAL.rank < Stage.CLOSED.rank and Stage.REJECTED.rank == -1


def _sig():
    return EntrySignal(1, "X", Side.LONG, 10, 9, 10, 0.5, "t")


def test_risk_approval_needs_token():
    with pytest.raises(PermissionError):
        RiskApproval(_sig(), 1, 10, 10.1, 9, 1, 1)
    RiskApproval(_sig(), 1, 10, 10.1, 9, 1, 1, token=_RISK_TOKEN)


def test_clock_helpers():
    a, b = session_bounds(date(2026, 1, 5))  # EST
    assert (a.hour, b.hour) == (14, 21) and utc_iso(a) == "2026-01-05T14:30:00.000Z"
    assert is_regular_session(a) and not is_regular_session(b)
    assert not is_regular_session(datetime(2026, 1, 3, 15, 0, tzinfo=UTC))  # Saturday
    assert before_cutoff(datetime(2026, 7, 6, 19, 29, tzinfo=UTC), "15:30")  # EDT 15:29
    assert not before_cutoff(datetime(2026, 7, 6, 19, 30, tzinfo=UTC), "15:30")
    assert to_et(a).hour == 9


def test_replay_clock():
    c = ReplayClock(datetime(2026, 1, 5, 14, 30, tzinfo=UTC))
    c.advance(minutes=5)
    assert c.now().minute == 35
    with pytest.raises(ValueError):
        c.set(datetime(2026, 1, 1))


def test_review13_params_are_versioned_and_percent_params_are_floats():
    p = load_settings(OK).params
    assert (p.iex_liquidity_scale, p.warmup_sessions, p.baseline_min_days, p.low_conf_floor,
            p.shadow_rvol_min) == (0.02, 3, 5, 0.5, 1.5)
    assert params_hash(StrategyParams()) != params_hash(StrategyParams(iex_liquidity_scale=0.03))
    for name in ("max_position_pct", "risk_per_trade_pct", "max_daily_loss_pct", "max_concurrent_risk_pct",
                 "min_impulse_pct", "min_price", "max_price", "min_avg_dollar_volume"):
        assert isinstance(getattr(p, name), float), name
    q = load_settings({**OK, "MAX_POSITION_PCT": "12.5", "MAX_DAILY_LOSS_PCT": "7.5", "MAX_CONCURRENT_RISK_PCT": "2.5",
                       "SHADOW_RVOL_MIN": "1.6", "WARMUP_SESSIONS": "4"}).params
    assert (q.max_position_pct, q.max_daily_loss_pct, q.max_concurrent_risk_pct) == (12.5, 7.5, 2.5)
    assert q.shadow_rvol_min == 1.6 and q.warmup_sessions == 4


def test_session_registry_effective_cutoff_and_eod():
    from newswave.clock import clear_session_closes, effective_cutoff, effective_eod, set_session_close
    p = StrategyParams()
    clear_session_closes()
    try:
        normal = datetime(2026, 3, 4, 17, 0, tzinfo=UTC)
        assert to_et(effective_cutoff(normal, p)).strftime("%H:%M") == "15:30"  # unknown close = 16:00
        assert to_et(effective_eod(normal, p)).strftime("%H:%M") == "15:55"
        set_session_close(date(2026, 3, 4), datetime(2026, 3, 4, 18, 0, tzinfo=UTC))  # 13:00 EST
        assert to_et(effective_cutoff(normal, p)).strftime("%H:%M") == "12:30"
        assert to_et(effective_eod(normal, p)).strftime("%H:%M") == "12:55"
        assert to_et(effective_cutoff(datetime(2026, 3, 5, 17, 0, tzinfo=UTC), p)).strftime("%H:%M") == "15:30"
    finally:
        clear_session_closes()
