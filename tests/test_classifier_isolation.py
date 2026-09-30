from __future__ import annotations

import subprocess
import sys


def test_classifier_imports_no_trading_code():
    code = ("import sys, newswave.news.classifier;"
            "bad=[m for m in sys.modules if m.startswith(('newswave.execution','newswave.risk','alpaca.trading'))];"
            "print(bad); sys.exit(1 if bad else 0)")
    r = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True)
    assert r.returncode == 0, r.stdout + r.stderr
