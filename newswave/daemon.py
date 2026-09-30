"""Daemon: wires every component into one process (CONTRACT §1, §11).

`build_app()` is the single factory used by `run`, `replay` and the tests, so all three drive the EXACT same
NewsIntake -> StrategyEngine -> ExecutionEngine objects. `App.tick()` and the `handle_*` methods are the only
entry points the streams (or a replay loop) call.

The SPEC §18 kill switch lives in ExecutionEngine; nothing here can re-enable trading.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import logging.handlers
import signal
import subprocess
import sys
import time
from collections import Counter
from dataclasses import asdict
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any, Awaitable, Callable

from .clock import Clock, RealClock, effective_cutoff, effective_eod, set_session_close, to_et, utc_iso
from .config import Settings, StrategyParams, params_hash
from .db import Database, StrategyVersionMismatch, register_strategy_version
from .execution.broker import (AlpacaPaperBroker, BrokerCalendar, BrokerError, SimBroker)
from .execution.engine import ExecutionEngine
from .market.baseline import HistoricalData
from .market.stream import IEX_URL, MarketStream
from .market.subscriptions import SubscriptionManager
from .market.universe import AssetCache
from .models import Bar, ExitReason, NewsEvent, RejectReason, Trade
from .news.classifier import ClaudeCliClassifier
from .news.intake import NewsIntake
from .news.stream import NEWS_URL, NewsStream
from .reports import maybe_generate_monthly
from .risk import KillSwitch, RiskEngine
from .strategy.engine import AlpacaMarketData, StrategyEngine
from .timeline import Timeline, system_event

log = logging.getLogger("newswave.daemon")

PKG_DIR = Path(__file__).resolve().parent
REPO_ROOT = PKG_DIR.parent
ARMED_FILE = REPO_ROOT / ".armed"
SIM_KEY = "sim_broker_state"
HEARTBEAT_S = 60
HEARTBEAT_EVENT_S = 600
ASSETS_RETRY_S = 60
STOP_FILE = "STOP"
STOP_POLL_S = 1.0
_MISSING = object()


class ArmRefused(RuntimeError):
    """PAPER execution requested but `.armed` does not match the current code + params."""


# ------------------------------------------------------------------ arm gate (CONTRACT §11)
def arm_hash(params: StrategyParams, pkg_dir: Path | None = None) -> str:
    """sha256 over every newswave/**/*.py (sorted by path, path included) + the params hash."""
    root = Path(pkg_dir or PKG_DIR)
    h = hashlib.sha256()
    for p in sorted(root.rglob("*.py")):
        h.update(p.relative_to(root).as_posix().encode() + b"\0" + p.read_bytes() + b"\0")
    h.update(params_hash(params).encode())
    return h.hexdigest()


def check_armed(params: StrategyParams, pkg_dir: Path | None = None, armed_file: Path | None = None) -> None:
    f = Path(armed_file or ARMED_FILE)
    want = arm_hash(params, pkg_dir)
    have = f.read_text(encoding="utf-8").strip() if f.exists() else ""
    if have != want:
        why = "not armed" if not have else "code or params changed since `arm`"
        raise ArmRefused(f"paper execution refused: {why}. Run `python -m newswave arm` (full test suite) first.")


def arm(params: StrategyParams, runner: Callable[[], int] | None = None, pkg_dir: Path | None = None,
        armed_file: Path | None = None) -> int:
    """Run the full test suite; write `.armed` only on green. Returns the process exit code."""
    def default() -> int:
        return subprocess.run([sys.executable, "-m", "pytest", "-q"], cwd=REPO_ROOT).returncode
    rc = (runner or default)()
    if rc != 0:
        print(f"arm: test suite is RED (exit {rc}); .armed not written", file=sys.stderr)
        return 1
    f = Path(armed_file or ARMED_FILE)
    f.write_text(arm_hash(params, pkg_dir) + "\n", encoding="utf-8")
    print(f"armed: {f}")
    return 0


# ------------------------------------------------------------------ logging
class JsonFormatter(logging.Formatter):
    """Lines that are already JSON (timeline / system events) pass through; the rest are wrapped."""

    def format(self, r: logging.LogRecord) -> str:
        msg = r.getMessage()
        if msg.startswith("{") and msg.endswith("}") and not r.exc_info:
            return msg
        d = {"ts": utc_iso(datetime.fromtimestamp(r.created, tz=UTC)), "kind": "log",
             "level": r.levelname, "logger": r.name, "message": msg}
        if r.exc_info:
            d["exc"] = self.formatException(r.exc_info)
        return json.dumps(d, default=str)


def setup_logging(data_dir: Path, level: int = logging.INFO) -> None:
    """Structured JSON to stdout + <data_dir>/logs/newswave.log (rotating). Idempotent."""
    root = logging.getLogger("newswave")
    for h in list(root.handlers):
        if getattr(h, "_nw", False):
            root.removeHandler(h)
            h.close()
    logs = Path(data_dir) / "logs"
    logs.mkdir(parents=True, exist_ok=True)
    fmt = JsonFormatter()
    for stream in (sys.stdout, sys.stderr):  # a cp1252 console must not crash on a non-ASCII headline
        if hasattr(stream, "reconfigure"):
            stream.reconfigure(errors="replace")
    for h in (logging.StreamHandler(sys.stdout),
              logging.handlers.RotatingFileHandler(logs / "newswave.log", maxBytes=5_000_000, backupCount=5,
                                                   encoding="utf-8")):
        h._nw = True  # type: ignore[attr-defined]
        h.setFormatter(fmt)
        root.addHandler(h)
    root.setLevel(level)


# ------------------------------------------------------------------ kv helpers
def kv_set(db: Database, clock: Clock, key: str, value: str) -> None:
    db.upsert("kv", {"key": key, "value": value, "updated_at": utc_iso(clock.now())}, ["key"])


def kv_get(db: Database, key: str) -> str | None:
    r = db.one("SELECT value FROM kv WHERE key=?", (key,))
    return r["value"] if r else None


# ------------------------------------------------------------------ the app
class App:
    def __init__(self, settings: Settings, db: Database, clock: Clock, broker: Any, info_broker: Any,
                 classifier: Any, historical: Any, news_url: str | None, iex_url: str | None) -> None:
        p, v = settings.params, settings.strategy_version
        self.settings, self.db, self.clock, self.broker, self.info_broker = settings, db, clock, broker, info_broker
        self.sim: SimBroker | None = broker if isinstance(broker, SimBroker) else None
        self.counts: Counter[str] = Counter()
        self.timeline = Timeline(db, clock, v)
        self.subs = SubscriptionManager(cap=30, reserved=("SPY", "QQQ"), shadow_max=p.shadow_max_slots)
        self.assets = AssetCache()
        self.kill = KillSwitch(db, v)
        self.execution = ExecutionEngine(db, clock, self.timeline, p, settings, broker, RiskEngine(p), self.kill,
                                         v, self.subs)
        if hasattr(classifier, "attach"):
            classifier.attach(db, clock, settings)
        self.classifier = classifier
        self.market_data = AlpacaMarketData(historical, self.assets, p, clock, db=db)
        self.strategy = StrategyEngine(
            db, clock, self.timeline, p, v, self.subs, self.assets, self.market_data,
            on_entry_signal=self.execution.on_entry_signal, on_opposite_news=self.execution.on_opposite_news,
            on_bar_5m=self.execution.on_bar_5m, shadow_enabled=p.shadow_enabled)
        self.intake = NewsIntake(db, clock, self.timeline, p, v, self._asset_check, classifier)
        self.news_stream = NewsStream(settings, clock, self._news_from_stream, url=news_url or NEWS_URL, db=db)
        self.market_stream = MarketStream(settings, clock, self.handle_trade, self.handle_bar, self.handle_status,
                                          url=iex_url or IEX_URL, on_error=self._ws_error,
                                          on_reconnect=self.strategy.backfill_after_gap)  # refill bars lost in a gap
        self.tick_interval_s = 1.0
        self.restart_backoff_s = 1.0
        self.started = asyncio.Event()
        self._stop = asyncio.Event()
        self._tasks: list[asyncio.Task] = []
        self._jobs: set[asyncio.Task] = set()
        self._cal: dict[date, BrokerCalendar | None] = {}
        self._cal_fail_at: float = 0.0
        self._assets_day: date | None = None
        self._assets_ok = False
        self._assets_try_at: datetime | None = None
        self._monthly_day: date | None = None
        self._next_hb: datetime | None = None
        self._next_hb_event: datetime | None = None
        self._err_at: dict[str, float] = {}
        if self.sim is not None:
            self._restore_sim()
            self.sim.on_change = self._persist_sim

    # ---------------------------------------------------------------- sim persistence (OBSERVE restart safety)
    def _persist_sim(self) -> None:
        kv_set(self.db, self.clock, SIM_KEY, json.dumps(self.sim.snapshot()))  # type: ignore[union-attr]

    def _restore_sim(self) -> None:
        raw = kv_get(self.db, SIM_KEY)
        if raw:
            self.sim.restore(json.loads(raw))  # type: ignore[union-attr]
            system_event(self.db, self.clock, "INFO", "daemon", "SIM_RESTORED",
                         f"observation broker restored: {len(self.sim._pos)} positions, "  # type: ignore[union-attr]
                         f"{len(self.sim._orders)} orders")  # type: ignore[union-attr]

    # ---------------------------------------------------------------- failure isolation
    def _ev(self, level: str, event: str, msg: str, **data: object) -> None:
        system_event(self.db, self.clock, level, "daemon", event, msg, **data)

    def _ws_error(self, code: int, msg: str) -> None:
        """Alpaca market-data stream error frame (subscription refused, auth, limit...): never silent."""
        self._ev("WARNING", "MARKET_WS_ERROR", f"IEX stream error {code}: {msg}"[:300], code=code)

    async def _guard(self, name: str, aw: Awaitable[Any]) -> Any:
        """Run one piece of the pipeline; an exception is logged (CRITICAL system_event) and swallowed so
        the loop / the other pieces keep running (open positions must never go unmanaged)."""
        try:
            return await aw
        except Exception as e:  # noqa: BLE001
            self._crash(name, e)
            return None

    def _crash(self, name: str, e: BaseException) -> None:
        self.counts["errors"] += 1
        log.exception("%s failed", name)
        now = time.monotonic()
        if now - self._err_at.get(name, -1e9) >= 30:  # one event per component per 30 s, no flood
            self._err_at[name] = now
            try:
                self._ev("CRITICAL", "CALLBACK_FAILED", f"{name}: {type(e).__name__}: {e}"[:300], where=name)
            except Exception:  # the db itself failing must not kill the loop either
                log.exception("could not record failure of %s", name)

    def _spawn(self, name: str, coro: Awaitable[Any]) -> None:
        t = asyncio.ensure_future(self._guard(name, coro))
        self._jobs.add(t)
        t.add_done_callback(self._jobs.discard)

    # ---------------------------------------------------------------- calendar / universe gate
    def _asset_check(self, symbol: str) -> RejectReason | None:
        """NewsIntake's universe hook: market calendar (holidays, early closes) + static asset filter."""
        now = self.clock.now()
        cal = self._cal.get(to_et(now).date(), _MISSING)
        if cal is None:
            return RejectReason.MARKET_CLOSED
        if cal is not _MISSING:
            if not cal.open <= now < cal.close:  # type: ignore[union-attr]
                return RejectReason.MARKET_CLOSED
            if now >= effective_cutoff(now, self.settings.params):
                return RejectReason.AFTER_CUTOFF
        return self.assets.check_static(symbol)

    async def _refresh_calendar(self, now: datetime) -> None:
        d = to_et(now).date()
        if d in self._cal or time.monotonic() - self._cal_fail_at < 60:
            return
        try:
            self._cal[d] = await self.info_broker.get_calendar(d)
        except BrokerError as e:
            self._cal_fail_at = time.monotonic()
            log.warning("calendar unavailable (%s); falling back to weekday 09:30-16:00", e)
            return
        cal = self._cal[d]
        if cal is not None:
            # the ONE early-close rule lives in clock.py: risk, exits, execution and intake all read it
            set_session_close(d, cal.close)
            p = self.settings.params
            if to_et(cal.close).strftime("%H:%M") < "16:00":
                self._ev("WARNING", "EARLY_CLOSE", f"early close at {to_et(cal.close):%H:%M} ET: no entries from "
                         f"{to_et(effective_cutoff(now, p)):%H:%M}, flat by {to_et(effective_eod(now, p)):%H:%M}")
        for old in [k for k in self._cal if k < d - timedelta(days=2)]:
            del self._cal[old]

    # ---------------------------------------------------------------- stream / replay entry points
    async def _news_from_stream(self, ev: NewsEvent) -> None:
        self._spawn("news", self.handle_news(ev))  # a slow classification never blocks the ws reader

    async def handle_news(self, ev: NewsEvent) -> None:
        try:
            for c in await self.intake.handle(ev):
                await self._guard("strategy.on_candidate", self.strategy.on_candidate(c))
        except Exception as e:  # noqa: BLE001
            self._crash("intake", e)
        finally:
            self.counts["news"] += 1  # counters tick AFTER processing: tests wait on them

    async def handle_trade(self, t: Trade) -> None:
        """Order matters: the sim sees the print first so an entry signal raised by this very print fills at it.
        Each stage is guarded on its own: a strategy crash must never stop execution managing positions."""
        try:
            if self.sim is not None:
                await self._guard("sim.on_trade", self.sim.on_trade(t))
            await self._guard("strategy.on_trade", self.strategy.on_trade(t))
            await self._guard("execution.on_trade", self.execution.on_trade(t))
        finally:
            self.counts["trades"] += 1

    async def handle_bar(self, b: Bar) -> None:
        try:
            await self._guard("strategy.on_bar_1m", self.strategy.on_bar_1m(b))
        finally:
            self.counts["bars"] += 1

    async def handle_status(self, d: dict) -> None:
        try:
            await self._guard("strategy.on_status", self.strategy.on_status(d))
            await self._guard("execution.on_status", self.execution.on_status(d))
        finally:
            self.counts["statuses"] += 1

    # ---------------------------------------------------------------- ticker
    async def tick(self, now: datetime | None = None) -> None:
        now = now or self.clock.now()
        self.counts["ticks"] += 1
        await self._guard("calendar", self._refresh_calendar(now))
        await self._guard("strategy.on_clock", self.strategy.on_clock(now))
        await self._guard("execution.on_clock", self.execution.on_clock(now))
        await self._guard("periodic", self._periodic(now))

    async def _periodic(self, now: datetime) -> None:
        d = to_et(now).date()
        if self._next_hb is None or now >= self._next_hb:
            self._next_hb = now + timedelta(seconds=HEARTBEAT_S)
            self._heartbeat(now)
        # assets: daily at 09:00 ET, and retry while a load has not succeeded
        due_daily = self._assets_day != d and to_et(now).strftime("%H:%M") >= "09:00"
        retry = not self._assets_ok and (self._assets_try_at is None or now >= self._assets_try_at)
        if due_daily or retry:
            await self._load_assets(now)
        if self._monthly_day != d:
            self._monthly_day = d
            maybe_generate_monthly(self.db, self.clock, self.settings.strategy_version,
                                   self.settings.data_dir / "reports")

    def _heartbeat(self, now: datetime) -> None:
        kv_set(self.db, self.clock, "heartbeat_at", utc_iso(now))
        live = [{"symbol": x["symbol"], "side": x["side"], "qty": x["qty_open"], "entry": x["entry_price"],
                 "last": x["last_price"], "unrealized_usd": round(x["unrealized_pnl"], 2),
                 "unrealized_r": round(x["unrealized_r"], 3), "stop": x["stop_price"], "trail": x["trail_price"]}
                for x in self.execution.open_positions()]
        kv_set(self.db, self.clock, "positions_live", json.dumps(live))
        if self._next_hb_event is None or now >= self._next_hb_event:
            self._next_hb_event = now + timedelta(seconds=HEARTBEAT_EVENT_S)
            self._ev("INFO", "HEARTBEAT", "alive", positions=len(live), counts=dict(self.counts),
                     armed=sorted(self.strategy.active_symbols()))

    async def _load_assets(self, now: datetime | None = None) -> None:
        now = now or self.clock.now()
        self._assets_try_at = now + timedelta(seconds=ASSETS_RETRY_S)
        try:
            if self.info_broker is self.broker:
                rows = await self.execution.load_assets()
            else:
                rows = [asdict(a) for a in await self.info_broker.get_all_assets()]
        except BrokerError as e:
            self._assets_ok = False
            self._ev("ERROR", "ASSETS_FAILED", f"asset list unavailable, nothing can arm until it loads: {e}")
            return
        self.assets.load(rows)
        self._assets_ok = True
        # a pre-09:00 load (boot) does not count as today's 09:00 refresh
        self._assets_day = to_et(now).date() if to_et(now).strftime("%H:%M") >= "09:00" else None
        self._ev("INFO", "ASSETS_LOADED", f"{len(rows)} assets")

    # ---------------------------------------------------------------- subscriptions
    def apply_subscriptions(self) -> bool:
        want = self.subs.desired()
        if want == self.market_stream.subscribed:
            return False
        self.market_stream.apply(want)
        return True

    # ---------------------------------------------------------------- lifecycle
    async def startup(self) -> None:
        """Assets, calendar, then boot reconciliation (sim state was restored in __init__, before this)."""
        now = self.clock.now()
        await self._guard("assets", self._load_assets(now))
        await self._guard("calendar", self._refresh_calendar(now))
        try:
            await self.execution.reconcile_on_boot()
        except BrokerError as e:
            self._ev("CRITICAL", "BOOT_ABORTED", f"cannot reconcile with the broker: {e}")
            raise
        for pos in self.execution.open_positions():  # rehydrated positions still need their 5m bars (ATR trail)
            await self._guard("track", self._track(pos["symbol"]))
        self.apply_subscriptions()

    async def _track(self, symbol: str) -> None:
        if not await self.strategy.track(symbol):
            self._ev("ERROR", "NO_BAR_FEED", f"{symbol}: open position has no 5m bar feed after restart; the ATR "
                     "trail cannot fire (hard stop, time stop and EOD still protect it)", symbol=symbol)

    def stop(self) -> None:
        self._stop.set()

    async def _sleep(self, s: float) -> None:
        try:
            await asyncio.wait_for(self._stop.wait(), timeout=s)
        except asyncio.TimeoutError:
            pass

    async def _ticker(self) -> None:
        while not self._stop.is_set():
            t0 = time.monotonic()
            await self.tick()
            await self._sleep(max(0.0, self.tick_interval_s - (time.monotonic() - t0)))

    async def _sub_loop(self) -> None:
        while not self._stop.is_set():
            self.apply_subscriptions()
            await self._sleep(0.25)  # debounce: changes are pushed to the ws within 250 ms

    async def _supervise(self, name: str, factory: Callable[[], Awaitable[Any]]) -> None:
        """Restart a crashed / exited task with exponential backoff; it never stays dead."""
        delay = self.restart_backoff_s
        while not self._stop.is_set():
            t0 = time.monotonic()
            try:
                await factory()
                if self._stop.is_set():
                    return
                err = "returned unexpectedly"
            except asyncio.CancelledError:
                raise
            except Exception as e:  # noqa: BLE001
                log.exception("task %s crashed", name)
                err = f"{type(e).__name__}: {e}"
            self.counts[f"restart:{name}"] += 1
            try:
                self._ev("CRITICAL", "TASK_CRASHED", f"{name}: {err}"[:300], task=name, restart_in_s=delay)
            except Exception:  # noqa: BLE001
                log.exception("could not record crash of %s", name)
            if time.monotonic() - t0 > 60:
                delay = self.restart_backoff_s
            await self._sleep(delay)
            delay = min(delay * 2, 30.0)

    async def run(self, install_signals: bool = True) -> None:
        loop = asyncio.get_running_loop()
        restore = self._install_signals(loop) if install_signals else (lambda: None)
        try:
            await self.startup()
            self._tasks.append(asyncio.create_task(self._watch_stop_file()))
            for name, fn in (("news", self.news_stream.run), ("market", self.market_stream.run),
                             ("ticker", self._ticker), ("subscriptions", self._sub_loop)):
                self._tasks.append(asyncio.create_task(self._supervise(name, fn)))
            self.started.set()
            await self._stop.wait()
        finally:
            self._stop.set()
            self.news_stream.stop()
            self.market_stream.stop()
            pending = [*self._tasks, *self._jobs]
            if pending:
                _, still = await asyncio.wait(pending, timeout=3)
                for t in still:
                    t.cancel()
                await asyncio.gather(*still, return_exceptions=True)
            self._stop_classifier()
            restore()
            self.stop_file.unlink(missing_ok=True)  # consumed: the next start must not stop at once
            # graceful: streams closed, positions are NOT flattened (broker-resident stops protect them)
            try:
                self._ev("INFO", "SHUTDOWN", "stopped", open_positions=len(self.execution.open_positions()))
            except Exception:  # noqa: BLE001
                log.exception("could not record shutdown")

    @property
    def stop_file(self) -> Path:
        return Path(self.settings.data_dir) / STOP_FILE

    async def _watch_stop_file(self) -> None:
        """`<DATA_DIR>/STOP` = graceful stop within ~1 s (works everywhere; the only clean stop on Windows)."""
        while not self._stop.is_set():
            if self.stop_file.exists():
                log.info("stop file found: stopping")
                self.stop()
                return
            await self._sleep(STOP_POLL_S)

    def _install_signals(self, loop: asyncio.AbstractEventLoop) -> Callable[[], None]:
        """POSIX: loop handlers for SIGTERM/SIGINT. Windows has no add_signal_handler: signal.signal for
        SIGINT/SIGBREAK, marshalled onto the loop. Returns the undo."""
        if sys.platform == "win32":
            sigs = (signal.SIGINT, signal.SIGBREAK)
            old = {s: signal.signal(s, lambda *_: loop.call_soon_threadsafe(self.stop)) for s in sigs}
            return lambda: [signal.signal(s, h) for s, h in old.items()] and None
        sigs = (signal.SIGTERM, signal.SIGINT)
        for s in sigs:
            loop.add_signal_handler(s, self.stop)
        return lambda: [loop.remove_signal_handler(s) for s in sigs] and None

    def _stop_classifier(self) -> None:
        """SIGTERM: a cancelled classify job does NOT kill its `claude` child, so end any in-flight one here."""
        hook = getattr(self.classifier, "shutdown", None)
        if callable(hook):
            try:
                hook()
            except Exception:  # noqa: BLE001
                log.exception("classifier shutdown hook failed")

    def close(self) -> None:
        self.db.close()


# ------------------------------------------------------------------ factory
def build_app(settings: Settings, *, clock: Clock | None = None, broker: Any = None, classifier: Any = None,
              historical: Any = None, news_url: str | None = None, iex_url: str | None = None) -> App:
    """Boot (CONTRACT §11) + wiring. Raises StrategyVersionMismatch / ArmRefused / ValueError, never half-boots."""
    clock = clock or RealClock()
    mode = settings.execution_mode
    if broker is None:
        if mode == "PAPER":
            check_armed(settings.params)
            if not (settings.alpaca_api_key and settings.alpaca_secret_key):
                raise ValueError("PAPER execution needs ALPACA_API_KEY and ALPACA_SECRET_KEY")
            broker = AlpacaPaperBroker(settings.alpaca_api_key, settings.alpaca_secret_key)
        else:
            broker = SimBroker(clock, settings.starting_equity)
        info = broker
        if broker.kind == "sim" and settings.alpaca_api_key and settings.alpaca_secret_key:
            info = AlpacaPaperBroker(settings.alpaca_api_key, settings.alpaca_secret_key)  # read-only: assets + calendar
    else:
        if broker.kind == "alpaca":
            check_armed(settings.params)  # no path may spend (paper) money without the gate
        info = broker
    db = Database(settings.db_path)
    try:
        db.init_schema()
        now = utc_iso(clock.now())
        h = register_strategy_version(db, settings.strategy_version, settings.params, now=now)
        effective = "PAPER" if broker.kind == "alpaca" else "OBSERVE"
        db.upsert("kv", {"key": "execution_mode", "value": effective, "updated_at": now}, ["key"])
        db.upsert("kv", {"key": "boot_at", "value": now, "updated_at": now}, ["key"])
        system_event(db, clock, "INFO", "daemon", "BOOT", f"{settings.strategy_version} booting in {effective}",
                     strategy_version=settings.strategy_version, params_hash=h, execution_mode=effective,
                     broker=broker.kind)
        if historical is None:
            if not (settings.alpaca_api_key and settings.alpaca_secret_key):
                raise ValueError("ALPACA_API_KEY and ALPACA_SECRET_KEY are required (market data + news streams)")
            historical = HistoricalData(settings.alpaca_api_key, settings.alpaca_secret_key, clock=clock)
        if classifier is None:
            classifier = ClaudeCliClassifier(settings, db, clock)
        return App(settings, db, clock, broker, info, classifier, historical, news_url, iex_url)
    except BaseException:
        db.close()
        raise

