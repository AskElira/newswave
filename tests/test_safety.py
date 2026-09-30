from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path

import pytest

from newswave.config import LiveTradingRefused, load_settings

OK = {"TRADING_MODE": "PAPER", "ALPACA_PAPER": "true"}
PKG = Path(__file__).resolve().parents[1] / "newswave"


@pytest.mark.parametrize("env", [
    {"ALPACA_PAPER": "true"},
    {"TRADING_MODE": "LIVE", "ALPACA_PAPER": "true"},
    {"TRADING_MODE": "paper", "ALPACA_PAPER": "true"},
    {"TRADING_MODE": "PAPER", "ALPACA_PAPER": "false"},
    {"TRADING_MODE": "PAPER"},
    {**OK, "ALPACA_BASE_URL": "https://api.alpaca.markets"},
    {**OK, "APCA_API_BASE_URL": "https://api.alpaca.markets/v2"},
    {**OK, "ALPACA_ENDPOINT": "https://evil.example.com"},
    {**OK, "SOMETHING_ELSE": "https://api.alpaca.markets"},
])
def test_refuses(env):
    with pytest.raises(LiveTradingRefused):
        load_settings(env)


@pytest.mark.parametrize("extra", [{}, {"ALPACA_BASE_URL": "https://paper-api.alpaca.markets"},
                                   {"APCA_API_BASE_URL": "https://paper-api.alpaca.markets/v2"}])
def test_paper_ok(extra):
    load_settings({**OK, **extra})


def test_cli_exit_codes(tmp_path):
    def run(**env):
        import os
        e = {k: v for k, v in os.environ.items() if not k.startswith(("ALPACA_", "APCA_", "TRADING_"))}
        e.update(env)
        return subprocess.run([sys.executable, "-m", "newswave", "check"], capture_output=True, text=True,
                              env=e, cwd=tmp_path)
    assert run(TRADING_MODE="PAPER", ALPACA_PAPER="true", ALPACA_API_KEY="SECRETKEY123").returncode == 0
    assert "SECRETKEY123" not in run(TRADING_MODE="PAPER", ALPACA_PAPER="true",
                                     ALPACA_API_KEY="SECRETKEY123").stdout
    assert run(TRADING_MODE="PAPER", ALPACA_PAPER="true",
               ALPACA_BASE_URL="https://api.alpaca.markets").returncode == 2


def _sources():
    return [(p, p.read_text(encoding="utf-8")) for p in PKG.rglob("*.py")]


def test_package_grep():
    bad = []
    for p, txt in _sources():
        if re.search(r"paper\s*=\s*False", txt):
            bad.append(f"{p}: paper=False")
        if '"https://api.alpaca.markets' in txt or "'https://api.alpaca.markets" in txt:
            bad.append(f"{p}: live URL literal")
        if re.search(r"^\s*(import anthropic|from anthropic)\b", txt, re.M):
            bad.append(f"{p}: anthropic SDK")
        for m in re.finditer(r"TradingClient\(([^)]*)\)", txt):
            if not re.search(r"\bpaper=True\b", m.group(1)):
                bad.append(f"{p}: TradingClient without literal paper=True")
    assert not bad, bad
