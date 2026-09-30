from __future__ import annotations

import asyncio
import json

import websockets

from newswave.clock import RealClock
from newswave.config import Settings
from newswave.market.stream import MarketStream, parse_bar, parse_status, parse_trade
from newswave.models import Bar, Trade

TRADE = {"T": "t", "S": "AAPL", "i": 1, "x": "V", "p": 187.5, "s": 100, "t": "2026-01-02T14:31:02.123456789Z"}
BAR = {"T": "b", "S": "AAPL", "o": 1, "h": 2, "l": 0.5, "c": 1.5, "v": 1234, "t": "2026-01-02T14:31:00Z"}
STATUS = {"T": "s", "S": "AAPL", "sc": "H", "sm": "Trading Halt", "rc": "T12", "rm": "News pending",
          "t": "2026-01-02T14:32:00Z", "z": "C"}


def test_parse_trade_bar_status():
    assert parse_trade(TRADE) == Trade("AAPL", "2026-01-02T14:31:02.123Z", 187.5, 100.0)
    assert parse_bar(BAR) == Bar("AAPL", "2026-01-02T14:31:00.000Z", 1.0, 2.0, 0.5, 1.5, 1234.0, 1)
    s = parse_status(STATUS)
    assert s["symbol"] == "AAPL" and s["halted"] is True and s["reason_code"] == "T12"
    assert parse_status({**STATUS, "sc": "T"})["halted"] is False
    assert parse_trade({"T": "t"}) is None and parse_bar({"T": "b", "S": "X"}) is None
    assert parse_status({"T": "s"}) is None


class Srv:
    def __init__(self, statuses_ok=True):
        self.log: list[dict] = []
        self.statuses_ok, self.ws = statuses_ok, None

    async def handler(self, ws):
        self.ws = ws
        await ws.send(json.dumps([{"T": "success", "msg": "connected"}]))
        trades: set = set(); bars: set = set(); st: set = set()
        async for raw in ws:
            m = json.loads(raw)
            self.log.append(m)
            if m["action"] == "auth":
                await ws.send(json.dumps([{"T": "success", "msg": "authenticated"}]))
                continue
            for name, s in (("trades", trades), ("bars", bars), ("statuses", st)):
                if m["action"] == "subscribe" and (name != "statuses" or self.statuses_ok):
                    s.update(m.get(name, []))
                elif m["action"] == "unsubscribe":
                    s.difference_update(m.get(name, []))
            conf = {"T": "subscription", "trades": sorted(trades), "quotes": [], "bars": sorted(bars),
                    "statuses": sorted(st)}
            await ws.send(json.dumps([conf]))
            if m["action"] == "subscribe":
                await ws.send(json.dumps([TRADE, BAR, STATUS]))


async def until(pred, timeout=5.0):
    end = asyncio.get_running_loop().time() + timeout
    while not pred():
        assert asyncio.get_running_loop().time() < end, "timeout"
        await asyncio.sleep(0.02)


async def run(statuses_ok):
    srv = Srv(statuses_ok)
    server = await websockets.serve(srv.handler, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    got = {"t": [], "b": [], "s": []}

    async def on_trade(x):  # async callbacks supported
        got["t"].append(x)

    ms = MarketStream(Settings(alpaca_api_key="K", alpaca_secret_key="S"), RealClock(), on_trade,
                      got["b"].append, got["s"].append, url=f"ws://127.0.0.1:{port}",
                      backoff_min_s=0.05, backoff_max_s=0.2)
    return srv, server, ms, got, asyncio.create_task(ms.run())


async def test_integration_subscribe_dispatch_unsubscribe():
    srv, server, ms, got, task = await run(True)
    try:
        assert ms.apply({"SPY", "AAPL"}) == ({"SPY", "AAPL"}, set())
        await until(lambda: got["t"] and got["b"] and got["s"])
        assert got["t"][0].price == 187.5 and got["b"][0].timeframe_min == 1 and got["s"][0]["halted"]
        subs = [m for m in srv.log if m["action"] == "subscribe"]
        assert subs[0] == {"action": "subscribe", "bars": ["AAPL", "SPY"], "trades": ["AAPL", "SPY"]}
        assert subs[1] == {"action": "subscribe", "statuses": ["AAPL", "SPY"]}  # statuses never rides with trades
        await until(lambda: ms.confirmed.get("trades") == ["AAPL", "SPY"])
        assert ms.statuses_enabled
        assert ms.apply({"SPY"}) == (set(), {"AAPL"})
        await until(lambda: ms.confirmed.get("trades") == ["SPY"])
        assert ms.apply({"SPY"}) == (set(), set())  # idempotent
    finally:
        ms.stop(); await asyncio.wait_for(task, 5); server.close()


async def test_statuses_rejected_logged_once_and_not_requested_again():
    srv, server, ms, got, task = await run(False)
    try:
        ms.apply({"SPY"})
        await until(lambda: not ms.statuses_enabled)
        ms.apply({"SPY", "MSFT"})
        await until(lambda: ms.confirmed.get("trades") == ["MSFT", "SPY"])
        subs = [m for m in srv.log if m["action"] == "subscribe"]
        assert "statuses" not in subs[0] and "statuses" in subs[1] and "statuses" not in subs[2]
        assert "statuses" not in ms.client.subscriptions
    finally:
        ms.stop(); await asyncio.wait_for(task, 5); server.close()


async def test_callback_error_isolated():
    calls = []
    ms = MarketStream(Settings(), RealClock(), lambda x: 1 / 0, calls.append, calls.append)
    await ms._on_message(TRADE)  # raises inside callback, swallowed
    await ms._on_message(BAR)
    await ms._on_message({"T": "t"})  # malformed ignored
    assert len(calls) == 1


# ------------------------------------------------------------------ review fixes
import pytest  # noqa: E402

from newswave.clock import ReplayClock  # noqa: E402
from newswave.market.stream import INELIGIBLE_CONDITIONS, halt_state, trade_eligible  # noqa: E402

T_NOW = __import__("datetime").datetime(2026, 1, 2, 15, 0, tzinfo=__import__("datetime").UTC)


def _ms(**kw):
    return MarketStream(Settings(), ReplayClock(T_NOW), lambda x: None, lambda x: None, lambda x: None, **kw)


def _outbox(ms):
    out = list(ms.client._outbox)
    ms.client._outbox.clear()
    return out


@pytest.mark.parametrize("code,tape,want", [
    ("2", "A", "halt"), ("2", "B", "halt"), ("3", "A", "resume"), ("3", "B", "resume"),
    ("H", "C", "halt"), ("P", "C", "halt"), ("P", "O", "halt"), ("Q", "C", "resume"), ("T", "O", "resume"),
    ("2", "C", None), ("H", "A", None),                       # a code only means what its own tape says
    ("5", "A", None), ("E", "B", None), ("F", "A", None), ("C", "B", None), ("7", "A", None),  # CTA info codes
    ("2", None, "halt"), ("H", None, "halt"), ("3", None, "resume"), ("T", None, "resume"), (None, "A", None)])
def test_halt_state_per_tape(code, tape, want):
    assert halt_state(code, tape) == want


def test_parse_status_flags_halt_and_resume_for_each_tape():
    base = {"T": "s", "S": "AAPL", "t": "2026-01-02T14:32:00Z"}
    a = parse_status({**base, "sc": "2", "z": "A"})
    assert a["halted"] and not a["resumed"]
    b = parse_status({**base, "sc": "3", "z": "B"})
    assert b["resumed"] and not b["halted"]
    assert parse_status({**base, "sc": "P", "z": "C"})["halted"]
    assert parse_status({**base, "sc": "Q", "z": "C"})["resumed"]
    assert not parse_status({**base, "sc": "E", "z": "A"})["halted"]


@pytest.mark.parametrize("code", sorted(INELIGIBLE_CONDITIONS))
def test_ineligible_condition_print_is_dropped(code):
    assert parse_trade({**TRADE, "c": ["@", code]}) is None
    assert not trade_eligible({**TRADE, "c": [code]})


@pytest.mark.parametrize("c", [None, [], [" "], ["@"], ["@", "F"], ["@", "E"], ["O"], ["6"], "@"])
def test_regular_print_is_kept(c):
    m = TRADE if c is None else {**TRADE, "c": c}
    assert parse_trade(m) == Trade("AAPL", "2026-01-02T14:31:02.123Z", 187.5, 100.0)


async def test_market_stream_drops_and_counts_ineligible_prints():
    got = []
    ms = MarketStream(Settings(), ReplayClock(T_NOW), got.append, lambda x: None, lambda x: None)
    await ms._on_message({**TRADE, "s": 1, "c": ["@", "I"]})       # 1-share odd lot
    await ms._on_message({**TRADE, "c": ["@", "T"]})               # extended hours
    await ms._on_message({**TRADE, "c": ["@", "Z"]})               # out of sequence
    await ms._on_message({**TRADE, "c": ["@"]})
    assert len(got) == 1 and ms.dropped_trades == 3
    assert ms.dropped_by_condition == {"I": 1, "T": 1, "Z": 1}


def test_apply_unsubscribes_before_subscribing_and_statuses_is_separate():
    ms = _ms()
    ms.apply({"SPY", "A"})
    _outbox(ms)
    ms.apply({"SPY", "B"})
    acts = [(m["action"], tuple(sorted(k for k in m if k != "action"))) for m in _outbox(ms)]
    assert acts == [("unsubscribe", ("bars", "trades")), ("unsubscribe", ("statuses",)),
                    ("subscribe", ("bars", "trades")), ("subscribe", ("statuses",))]


def test_subscribed_follows_confirmation_frames_not_the_request():
    ms = _ms()
    ms.apply({"SPY", "AAPL"})                                      # 2 requests: trades+bars, statuses
    assert ms.subscribed == {"SPY", "AAPL"}                        # optimistic until the server answers
    ms._handle_confirmation({"T": "subscription", "trades": ["SPY"], "bars": ["SPY"], "statuses": []})
    assert ms.subscribed == {"SPY", "AAPL"}                        # one more answer in flight
    ms._handle_confirmation({"T": "subscription", "trades": ["SPY"], "bars": ["SPY"], "statuses": ["SPY"]})
    assert ms.subscribed == {"SPY"} and ms.statuses_enabled        # the server only took SPY


async def test_limit_405_reverts_calls_back_and_backs_off_retries():
    errs = []
    ms = _ms(on_error=lambda c, m: errs.append((c, m)))
    ms.apply({"SPY", "AAPL"})
    await ms._on_ws_error(405, "symbol limit exceeded")
    assert errs == [(405, "symbol limit exceeded")] and ms.subscribed == set()
    assert ms.client.subscriptions == {}                           # a reconnect must not replay the rejected set
    _outbox(ms)
    assert ms.apply({"SPY", "AAPL"}) == (set(), set()) and _outbox(ms) == []   # backing off
    ms.clock.advance(seconds=MarketStream.RETRY_AFTER_LIMIT_S + 1)
    assert ms.apply({"SPY", "AAPL"}) == ({"SPY", "AAPL"}, set())


@pytest.mark.parametrize("code", [409, 410])
async def test_statuses_rejected_by_error_frame_disables_statuses_only(code):
    errs = []
    ms = _ms(on_error=lambda c, m: errs.append(c))
    ms.apply({"SPY"})
    await ms._on_ws_error(code, "insufficient subscription")
    assert not ms.statuses_enabled and errs == [code]
    assert "statuses" not in ms.client.subscriptions
    _outbox(ms)
    ms.apply({"SPY", "QQQ"})
    assert all("statuses" not in m for m in _outbox(ms))


async def test_on_reconnect_fires_after_resubscribe():
    srv, server, ms, got, task = await run(True)
    calls = []
    ms.on_reconnect = lambda: calls.append(len(srv.log))
    try:
        ms.apply({"SPY"})
        await until(lambda: ms.confirmed.get("trades") == ["SPY"])
        assert calls == []                                          # first connect is not a reconnect
        await srv.ws.close()
        await until(lambda: calls)
        assert ms.client.connects == 2 and calls == [calls[0]]
        await until(lambda: [m["action"] for m in srv.log].count("subscribe") >= 3)  # main + statuses again
    finally:
        ms.stop(); await asyncio.wait_for(task, 5); server.close()
