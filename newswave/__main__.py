"""CLI: check | run [--execute] | arm | dashboard [--port] | report --month | replay."""
from __future__ import annotations

import argparse
import asyncio
import sys
from datetime import date
from pathlib import Path
from typing import Mapping

from .config import LiveTradingRefused, Settings, load_settings
from .db import StrategyVersionMismatch

EXIT_REFUSED, EXIT_ARM, EXIT_FAIL = 2, 3, 1


def _check(s: Settings) -> int:
    print("paper guard: OK (PAPER only)")
    print(f"strategy_version: {s.strategy_version}")
    print(f"params_hash: {s.params.params_hash()}")
    print(f"execution_mode: {s.execution_mode}")
    print(f"alpaca keys: {'set' if s.alpaca_api_key and s.alpaca_secret_key else 'NOT set'}")
    return 0


def _run(s: Settings, execute: bool) -> int:
    from dataclasses import replace

    from .daemon import ArmRefused, build_app, setup_logging
    if execute:
        s = replace(s, execution_mode="PAPER")
    setup_logging(s.data_dir)
    try:
        app = build_app(s)
    except ArmRefused as e:
        print(f"REFUSED: {e}", file=sys.stderr)
        return EXIT_ARM
    except StrategyVersionMismatch as e:
        print(f"REFUSED: {e}", file=sys.stderr)
        return EXIT_FAIL
    except ValueError as e:
        print(f"REFUSED: {e}", file=sys.stderr)
        return EXIT_FAIL
    try:
        asyncio.run(app.run())
    except Exception as e:  # noqa: BLE001  (already recorded as a CRITICAL system_event where possible)
        print(f"FAILED: {type(e).__name__}: {e}", file=sys.stderr)
        return EXIT_FAIL
    finally:
        app.close()
    return 0


def _replay(s: Settings, a: argparse.Namespace) -> int:
    from dataclasses import replace

    from .replay import fetch_historical_fixture, load_fixture, make_classifier, replay_fixture
    if a.fixture:
        fx = load_fixture(a.fixture)
    elif a.date and a.symbols:
        if not (s.alpaca_api_key and s.alpaca_secret_key):
            print("historical replay needs ALPACA_API_KEY / ALPACA_SECRET_KEY (paper keys, read-only data)",
                  file=sys.stderr)
            return EXIT_FAIL
        fx = fetch_historical_fixture(s, date.fromisoformat(a.date), [x.strip().upper() for x in a.symbols.split(",")])
    else:
        print("replay needs --fixture PATH or --date YYYY-MM-DD --symbols A,B", file=sys.stderr)
        return EXIT_FAIL
    mode = a.classify if not a.fixture or a.classify != "cached" else "fixture"
    clf = make_classifier(mode, s, fx, a.canned)
    out = replace(s, execution_mode="OBSERVE")
    summary = asyncio.run(replay_fixture(fx, out, out_dir=Path(a.out) if a.out else None, classifier=clf))
    for k, v in summary.items():
        print(f"{k}: {v}")
    return 0


def main(argv: list[str] | None = None, env: Mapping[str, str] | None = None) -> int:
    for stream in (sys.stdout, sys.stderr):  # cp1252 console: never crash on a non-ASCII character
        if hasattr(stream, "reconfigure"):
            stream.reconfigure(errors="replace")
    p = argparse.ArgumentParser(prog="newswave")
    sub = p.add_subparsers(dest="cmd", required=True)
    sub.add_parser("check", help="validate settings + paper guard")
    r = sub.add_parser("run", help="run the daemon (OBSERVE unless --execute / EXECUTION_MODE=PAPER)")
    r.add_argument("--execute", action="store_true", help="paper execution (needs a valid `arm`)")
    sub.add_parser("arm", help="run the full test suite; on green write .armed")
    d = sub.add_parser("dashboard", help="local dashboard on 127.0.0.1")
    d.add_argument("--port", type=int, default=None)
    rp = sub.add_parser("report", help="write the monthly report")
    rp.add_argument("--month", required=True, help="YYYY-MM")
    rl = sub.add_parser("replay", help="stream a fixture / a historical day through the real engine (SimBroker)")
    rl.add_argument("--fixture")
    rl.add_argument("--date")
    rl.add_argument("--symbols")
    rl.add_argument("--classify", choices=["cached", "fixture", "cli"], default="cached",
                    help="historical news: cached ai_classifications rows (default) | fixture | cli (uses your "
                         "Claude plan). With --fixture the canned classifications are used.")
    rl.add_argument("--canned", help="JSON file of extra canned classifications")
    rl.add_argument("--out", help="directory for the replay database (default DATA_DIR/replay/<name>)")
    a = p.parse_args(argv)
    try:
        s = load_settings(env)
    except LiveTradingRefused as e:
        print(f"REFUSED: {e}", file=sys.stderr)
        return EXIT_REFUSED
    except ValueError as e:
        print(f"BAD SETTINGS: {e}", file=sys.stderr)
        return EXIT_FAIL
    if a.cmd == "check":
        return _check(s)
    if a.cmd == "run":
        return _run(s, a.execute)
    if a.cmd == "arm":
        from .daemon import arm
        return arm(s.params)
    if a.cmd == "dashboard":
        from .dashboard.server import serve
        serve(s.db_path, port=a.port or s.dashboard_port, strategy_version=s.strategy_version)
        return 0
    if a.cmd == "report":
        from .db import Database
        from .reports import build_monthly_report
        if not s.db_path.exists():
            print(f"no database at {s.db_path}", file=sys.stderr)
            return EXIT_FAIL
        db = Database(s.db_path)
        try:
            path = build_monthly_report(db, a.month, s.strategy_version, s.data_dir / "reports")
        finally:
            db.close()
        print(path)
        return 0
    return _replay(s, a)


if __name__ == "__main__":
    raise SystemExit(main())
