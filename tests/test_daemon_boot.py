"""Boot sequence, broker selection, arm gate (CONTRACT §11) and the CLI entry points."""
from __future__ import annotations

import json
import logging
from dataclasses import replace
from pathlib import Path

import pytest

import newswave.daemon as daemon
from newswave.__main__ import main
from newswave.clock import ReplayClock
from newswave.config import StrategyParams
from newswave.daemon import ArmRefused, arm, arm_hash, build_app, check_armed, setup_logging
from newswave.db import Database, StrategyVersionMismatch
from newswave.execution.broker import AlpacaPaperBroker, SimBroker
from test_daemon_helpers import settings

OK_ENV = {"TRADING_MODE": "PAPER", "ALPACA_PAPER": "true"}


@pytest.fixture(autouse=True)
def _clean_logging():
    yield
    root = logging.getLogger("newswave")
    for h in list(root.handlers):
        if getattr(h, "_nw", False):
            root.removeHandler(h)
            h.close()


def kv(db, key):
    r = db.one("SELECT value FROM kv WHERE key=?", (key,))
    return r["value"] if r else None


# ------------------------------------------------------------------ boot
def test_boot_records_mode_version_hash_and_no_secrets(tmp_path):
    cfg = replace(settings(tmp_path), alpaca_api_key="SECRETKEY123", alpaca_secret_key="SECRETSECRET456")
    app = build_app(cfg, clock=ReplayClock())
    try:
        assert isinstance(app.broker, SimBroker) and app.sim is app.broker
        assert kv(app.db, "execution_mode") == "OBSERVE" and kv(app.db, "boot_at")
        ev = app.db.one("SELECT * FROM system_events WHERE event='BOOT'")
        d = json.loads(ev["data_json"])
        assert d["strategy_version"] == "v_test" and d["params_hash"] == cfg.params.params_hash()
        assert d["execution_mode"] == "OBSERVE"
        dump = json.dumps(app.db.query("SELECT * FROM system_events")) + json.dumps(app.db.query("SELECT * FROM kv"))
        assert "SECRETKEY123" not in dump and "SECRETSECRET456" not in dump
        assert app.db.one("SELECT 1 FROM strategy_versions WHERE version='v_test'")
        # keys present + OBSERVE: assets/calendar come from a READ-ONLY alpaca paper client, orders stay on the sim
        assert app.info_broker.kind == "alpaca" and app.broker.kind == "sim"
    finally:
        app.close()


def test_observe_without_keys_uses_sim_for_everything(tmp_path):
    cfg = replace(settings(tmp_path), alpaca_api_key="", alpaca_secret_key="")
    with pytest.raises(ValueError, match="ALPACA_API_KEY"):   # the real historical client needs keys
        build_app(cfg, clock=ReplayClock())
    app = build_app(cfg, clock=ReplayClock(), historical=object())
    try:
        assert app.info_broker is app.broker
    finally:
        app.close()


def test_changed_params_under_same_version_refuse_to_boot(tmp_path):
    build_app(settings(tmp_path), clock=ReplayClock()).close()
    with pytest.raises(StrategyVersionMismatch):
        build_app(settings(tmp_path, params=StrategyParams(rvol_min=3.0)), clock=ReplayClock())
    build_app(settings(tmp_path, params=StrategyParams(rvol_min=3.0), version="v_test2"), clock=ReplayClock()).close()


# ------------------------------------------------------------------ arm gate
def fake_pkg(tmp_path) -> Path:
    p = tmp_path / "pkg"
    (p / "sub").mkdir(parents=True)
    (p / "a.py").write_text("x = 1\n", encoding="utf-8")
    (p / "sub" / "b.py").write_text("y = 2\n", encoding="utf-8")
    (p / "notes.txt").write_text("not code", encoding="utf-8")
    return p


def test_arm_hash_ignores_non_python_and_depends_on_content_path_and_params(tmp_path):
    pkg, prm = fake_pkg(tmp_path), StrategyParams()
    h = arm_hash(prm, pkg)
    (pkg / "notes.txt").write_text("changed", encoding="utf-8")
    assert arm_hash(prm, pkg) == h
    (pkg / "a.py").write_text("x = 2\n", encoding="utf-8")
    assert arm_hash(prm, pkg) != h
    (pkg / "a.py").write_text("x = 1\n", encoding="utf-8")
    assert arm_hash(prm, pkg) == h
    (pkg / "sub" / "b.py").rename(pkg / "sub" / "c.py")
    assert arm_hash(prm, pkg) != h
    (pkg / "sub" / "c.py").rename(pkg / "sub" / "b.py")
    assert arm_hash(StrategyParams(rvol_min=3.0), pkg) != h


def test_armed_file_match_allows_one_byte_or_params_change_refuses(tmp_path):
    pkg, prm, f = fake_pkg(tmp_path), StrategyParams(), tmp_path / ".armed"
    with pytest.raises(ArmRefused, match="not armed"):
        check_armed(prm, pkg, f)
    f.write_text(arm_hash(prm, pkg) + "\n", encoding="utf-8")
    check_armed(prm, pkg, f)  # match: allowed
    (pkg / "a.py").write_text("x = 1 \n", encoding="utf-8")  # one byte
    with pytest.raises(ArmRefused, match="changed"):
        check_armed(prm, pkg, f)
    (pkg / "a.py").write_text("x = 1\n", encoding="utf-8")
    check_armed(prm, pkg, f)
    with pytest.raises(ArmRefused, match="changed"):
        check_armed(StrategyParams(rvol_min=2.5), pkg, f)  # params changed


def test_arm_writes_only_on_green(tmp_path):
    pkg, prm, f = fake_pkg(tmp_path), StrategyParams(), tmp_path / ".armed"
    assert arm(prm, runner=lambda: 1, pkg_dir=pkg, armed_file=f) == 1
    assert not f.exists()
    assert arm(prm, runner=lambda: 0, pkg_dir=pkg, armed_file=f) == 0
    assert f.read_text(encoding="utf-8").strip() == arm_hash(prm, pkg)


def test_paper_mode_needs_a_valid_armed_file(tmp_path, monkeypatch):
    armed = tmp_path / ".armed"
    monkeypatch.setattr(daemon, "ARMED_FILE", armed)
    cfg = replace(settings(tmp_path), execution_mode="PAPER")
    with pytest.raises(ArmRefused):
        build_app(cfg, clock=ReplayClock())
    assert not cfg.db_path.exists()  # refused before anything was booted
    armed.write_text(arm_hash(cfg.params) + "\n", encoding="utf-8")  # the REAL package hash
    app = build_app(cfg, clock=ReplayClock())
    try:
        assert isinstance(app.broker, AlpacaPaperBroker) and app.info_broker is app.broker
        assert kv(app.db, "execution_mode") == "PAPER"
    finally:
        app.close()
    armed.write_text("deadbeef\n", encoding="utf-8")
    with pytest.raises(ArmRefused):
        build_app(cfg, clock=ReplayClock())
    # even an injected alpaca broker goes through the gate
    with pytest.raises(ArmRefused):
        build_app(cfg, clock=ReplayClock(), broker=AlpacaPaperBroker("k", "s", client=object()))


def test_paper_mode_without_keys_refuses(tmp_path, monkeypatch):
    armed = tmp_path / ".armed"
    monkeypatch.setattr(daemon, "ARMED_FILE", armed)
    cfg = replace(settings(tmp_path), execution_mode="PAPER", alpaca_api_key="")
    armed.write_text(arm_hash(cfg.params) + "\n", encoding="utf-8")
    with pytest.raises(ValueError, match="ALPACA_API_KEY"):
        build_app(cfg, clock=ReplayClock())


# ------------------------------------------------------------------ CLI
def env_for(tmp_path, **extra):
    return {**OK_ENV, "DATA_DIR": str(tmp_path / "data"), "ALPACA_API_KEY": "k", "ALPACA_SECRET_KEY": "s", **extra}


def test_cli_refuses_live_endpoint_before_touching_anything(tmp_path, capsys):
    env = env_for(tmp_path, ALPACA_BASE_URL="https://api.alpaca.markets")
    for argv in (["run"], ["run", "--execute"], ["arm"], ["replay", "--fixture", "x"], ["report", "--month", "2026-01"]):
        assert main(argv, env) == 2
    assert "REFUSED" in capsys.readouterr().err
    assert not (tmp_path / "data").exists()


def test_cli_run_execute_refused_when_not_armed(tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(daemon, "ARMED_FILE", tmp_path / "missing")
    assert main(["run", "--execute"], env_for(tmp_path)) == 3
    assert "Run `python -m newswave arm`" in capsys.readouterr().err
    assert main(["run"], env_for(tmp_path, EXECUTION_MODE="PAPER")) == 3


def test_cli_run_observe_boots_and_runs(tmp_path, monkeypatch):
    ran = []

    async def fake_run(self, install_signals=True):
        ran.append(self.settings.execution_mode)

    monkeypatch.setattr(daemon.App, "run", fake_run)
    assert main(["run"], env_for(tmp_path)) == 0
    assert ran == ["OBSERVE"]
    assert (tmp_path / "data" / "logs" / "newswave.log").exists()
    db = Database(tmp_path / "data" / "newswave.db")
    assert kv(db, "execution_mode") == "OBSERVE"
    db.close()


def test_cli_run_version_mismatch_exits_nonzero(tmp_path, capsys):
    build_app(settings(tmp_path, version="newswave_v1.0"), clock=ReplayClock()).close()
    assert main(["run"], env_for(tmp_path, RVOL_MIN="3.5")) == 1
    assert "bump STRATEGY_VERSION" in capsys.readouterr().err


def test_cli_dashboard_and_report(tmp_path, monkeypatch, capsys):
    build_app(settings(tmp_path, version="newswave_v1.0"), clock=ReplayClock()).close()
    calls = []
    import newswave.dashboard.server as srv
    monkeypatch.setattr(srv, "serve", lambda *a, **k: calls.append((a, k)))
    assert main(["dashboard", "--port", "9123"], env_for(tmp_path)) == 0
    assert calls[0][1]["port"] == 9123 and str(calls[0][0][0]).endswith("newswave.db")
    assert main(["report", "--month", "2026-01"], env_for(tmp_path)) == 0
    out = capsys.readouterr().out.strip().splitlines()[-1]
    assert Path(out).exists() and out.endswith("2026-01.md")
    assert main(["report", "--month", "2026-01"], env_for(tmp_path / "nowhere")) == 1


def test_cli_arm_red_writes_nothing(tmp_path, monkeypatch):
    monkeypatch.setattr(daemon, "ARMED_FILE", tmp_path / ".armed")
    monkeypatch.setattr(daemon.subprocess, "run", lambda *a, **k: type("R", (), {"returncode": 1})())
    assert main(["arm"], env_for(tmp_path)) == 1
    assert not (tmp_path / ".armed").exists()
    monkeypatch.setattr(daemon.subprocess, "run", lambda *a, **k: type("R", (), {"returncode": 0})())
    assert main(["arm"], env_for(tmp_path)) == 0
    assert (tmp_path / ".armed").read_text(encoding="utf-8").strip() == arm_hash(StrategyParams())


# ------------------------------------------------------------------ logging
def test_structured_json_logs_to_stdout_and_rotating_file(tmp_path, capsys):
    setup_logging(tmp_path)
    setup_logging(tmp_path)  # idempotent: no duplicate handlers
    app = build_app(settings(tmp_path), clock=ReplayClock())
    try:
        app.timeline.log("NVDA", "NEWS", "hello", None, k=1)
        logging.getLogger("newswave.test").warning("plain message")
    finally:
        app.close()
    lines = (tmp_path / "logs" / "newswave.log").read_text(encoding="utf-8").strip().splitlines()
    parsed = [json.loads(x) for x in lines]  # every line is JSON
    assert any(p.get("kind") == "timeline" and p["message"] == "hello" for p in parsed)
    assert any(p.get("kind") == "log" and p["message"] == "plain message" for p in parsed)
    assert sum(1 for p in parsed if p.get("event") == "BOOT") == 1
    assert [json.loads(x) for x in capsys.readouterr().out.strip().splitlines()]
