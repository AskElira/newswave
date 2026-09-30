"""Windows-port guards that must hold on every platform (Windows branches are driven by monkeypatching)."""
from __future__ import annotations

import io
import re
import shutil
import subprocess
import sys
import tomllib
from pathlib import Path

import pytest

from newswave.config import StrategyParams
from newswave.daemon import arm

ROOT = Path(__file__).resolve().parents[1]
SECRET_NAME = re.compile(r"(^|/)(\.env($|\.(?!example$))|\.armed$|.*\.db(-.*)?$|.*\.(pem|key)$)|(^|/)(\.venv|data)/")


def test_tzdata_is_a_dependency_and_new_york_resolves():
    deps = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))["project"]["dependencies"]
    assert "tzdata" in deps
    from newswave.clock import ET
    assert ET.key == "America/New_York"


def test_arm_runs_pytest_through_this_interpreter(tmp_path, monkeypatch):
    seen = {}

    def fake_run(argv, **kw):
        seen["argv"] = argv
        return subprocess.CompletedProcess(argv, 0)
    monkeypatch.setattr("newswave.daemon.subprocess.run", fake_run)
    assert arm(StrategyParams(), pkg_dir=ROOT / "newswave", armed_file=tmp_path / ".armed") == 0
    assert seen["argv"][:3] == [sys.executable, "-m", "pytest"]


def test_reconfigure_makes_a_cp1252_console_survive_non_ascii(monkeypatch):
    from newswave import __main__ as M
    raw = io.BytesIO()
    out = io.TextIOWrapper(raw, encoding="cp1252", errors="strict")
    monkeypatch.setattr(sys, "stdout", out)
    monkeypatch.setattr(sys, "stderr", out)
    monkeypatch.setenv("TRADING_MODE", "PAPER")
    M.main(["check"], env={"TRADING_MODE": "PAPER", "ALPACA_PAPER": "true"})
    out.write("headline — 北京\n")          # would raise UnicodeEncodeError before the reconfigure
    out.flush()
    assert b"headline" in raw.getvalue()


def test_tracked_files_and_bundle_contain_no_secrets():
    if not (ROOT / ".git").exists() or not shutil.which("git"):
        pytest.skip("no git checkout (running from an unpacked bundle)")
    names = subprocess.run(["git", "-C", str(ROOT), "ls-files"], capture_output=True, text=True, check=True).stdout.split()
    assert names and not [n for n in names if SECRET_NAME.search(n)], [n for n in names if SECRET_NAME.search(n)]


def test_bundle_script_asserts_no_env_and_uses_git_archive():
    sh = (ROOT / "scripts" / "make_windows_bundle.sh").read_text(encoding="utf-8")
    assert "git archive" in sh and "windows-port" in sh and ".env" in sh and "newswave-windows.zip" in sh
