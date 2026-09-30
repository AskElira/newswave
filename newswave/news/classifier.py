"""Claude CLI news classifier (CONTRACT §6). No API SDK; the CLI's own login does the auth.

Isolation: this module must never import execution, risk, or alpaca.trading
(tests/test_classifier_isolation.py proves it).
"""
from __future__ import annotations

import argparse
import asyncio
import html
import json
import logging
import os
import re
import shutil
import sys
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Mapping

from ..clock import Clock, RealClock, to_et, utc_iso
from ..config import SYSTEM_PROMPT, Settings, StrategyParams
from ..db import Database
from ..models import Classification, Direction, NewsEvent, RejectReason, Side

log = logging.getLogger("newswave.classifier")

SCHEMA = {
    "type": "object",
    "properties": {
        "direction": {"type": "string", "enum": ["BULLISH", "NEUTRAL", "BEARISH"]},
        "confidence": {"type": "number"},
        "material": {"type": "boolean"},
        "catalyst": {"type": "string"},
        "reason": {"type": "string"},
    },
    "required": ["direction", "confidence", "material", "catalyst", "reason"],
    "additionalProperties": False,
}
_SCRUB_PREFIXES = ("ALPACA_", "APCA_")
_SCRUB_KEYS = ("ANTHROPIC_API_KEY", "ANTHROPIC_AUTH_TOKEN")


@dataclass(frozen=True)
class CliResult:
    classification: Classification
    duration_ms: int | None = None
    cost_usd: float | None = None


# ---- pure helpers --------------------------------------------------------
_NEWS_TAG = re.compile(r"<\s*/?\s*news\s*>", re.I)


def _clean(text: str) -> str:
    text = html.unescape(re.sub(r"<[^>]+>", " ", text or ""))
    text = re.sub(r"\s+", " ", text).strip()
    while True:  # data must not close its own tag; removal can re-form one ("<<news>news>"), so loop to a fixpoint
        nxt = _NEWS_TAG.sub("", text)
        if nxt == text:
            return text
        text = nxt


def build_user_prompt(symbol: str, event: NewsEvent, content_chars: int) -> str:
    return (f"ticker: {symbol}\n<news>\nheadline: {_clean(event.headline)}\n"
            f"summary: {_clean(event.summary)}\n"
            f"content: {_clean(event.content)[:content_chars]}\n</news>")


def resolve_bin(settings: Settings) -> str | None:
    return settings.claude_bin or shutil.which("claude")


def refused_shim(binary: str) -> str | None:
    """Windows: cmd.exe re-parses the args of a .cmd/.bat shim (BatBadBut). Refuse it outright."""
    if sys.platform == "win32" and binary.lower().endswith((".cmd", ".bat")):
        return (f"refusing {binary}: a .cmd/.bat shim is unsafe on Windows; "
                "set CLAUDE_BIN to claude.exe from the native installer")
    return None


def build_argv(settings: Settings) -> list[str]:
    """argv only holds constants; the user prompt (untrusted news text) goes to the child's STDIN."""
    p = settings.params
    binary = resolve_bin(settings) or "claude"
    # `.py` = test seam (fake claude): run it with this interpreter so no shebang/exec bit is needed
    head = [sys.executable, binary] if binary.endswith(".py") else [binary]
    return [*head, "-p",
            "--model", p.classifier_model, "--effort", p.classifier_effort,
            "--system-prompt", SYSTEM_PROMPT,
            "--tools", "", "--setting-sources", "", "--strict-mcp-config",
            "--no-session-persistence", "--output-format", "json",
            "--json-schema", json.dumps(SCHEMA, separators=(",", ":"))]


def scrubbed_env(environ: Mapping[str, str]) -> dict[str, str]:
    return {k: v for k, v in environ.items()
            if not k.startswith(_SCRUB_PREFIXES) and k not in _SCRUB_KEYS}


def _err(model: str, msg: str) -> Classification:
    return Classification(Direction.NEUTRAL, 0.0, False, "", "", model, error=msg)


def parse_cli_output_ex(stdout: str, model: str) -> CliResult:
    try:
        env = json.loads(stdout)
        if not isinstance(env, dict):
            return CliResult(_err(model, "cli output not an object"))
        dur = env.get("duration_ms") if isinstance(env.get("duration_ms"), (int, float)) else None
        cost = env.get("total_cost_usd")
        if not isinstance(cost, (int, float)):
            usage = env.get("modelUsage")
            cost = next((v["costUSD"] for v in usage.values() if isinstance(v, dict)
                         and isinstance(v.get("costUSD"), (int, float))), None) if isinstance(usage, dict) else None
        meta = {"duration_ms": int(dur) if dur is not None else None,
                "cost_usd": float(cost) if cost is not None else None}
        if env.get("is_error"):
            return CliResult(_err(model, f"cli is_error: {str(env.get('result'))[:200]}"), **meta)
        out = env.get("structured_output")
        if not isinstance(out, dict):
            out = json.loads(env.get("result"))
        if not isinstance(out, dict):
            return CliResult(_err(model, "classification not an object"), **meta)
        conf = out["confidence"]
        if isinstance(conf, bool) or not isinstance(conf, (int, float)) or not 0 <= conf <= 1:
            return CliResult(_err(model, f"bad confidence {conf!r}"), **meta)
        if not isinstance(out["material"], bool):
            return CliResult(_err(model, "material not boolean"), **meta)
        if not isinstance(out["catalyst"], str) or not isinstance(out["reason"], str):
            return CliResult(_err(model, "catalyst/reason not strings"), **meta)
        direction = Direction(out["direction"])
        reason = " ".join(out["reason"].split()[:12])
        return CliResult(Classification(direction, float(conf), out["material"],
                                        out["catalyst"].strip(), reason, model), **meta)
    except (ValueError, KeyError, TypeError) as e:  # JSONDecodeError is a ValueError
        return CliResult(_err(model, f"unparseable cli output: {type(e).__name__}: {e}"[:300]))


def parse_cli_output(stdout: str, model: str) -> Classification:
    return parse_cli_output_ex(stdout, model).classification


def gate(c: Classification, params: StrategyParams) -> Side | RejectReason:
    if c.error:
        return RejectReason.AI_ERROR
    if c.direction == Direction.NEUTRAL:
        return RejectReason.AI_NEUTRAL
    if not c.material:
        return RejectReason.AI_NOT_MATERIAL
    if c.confidence < params.ai_min_confidence:
        return RejectReason.AI_LOW_CONFIDENCE
    if c.direction == Direction.BULLISH:
        return Side.LONG
    return Side.SHORT if params.allow_shorts else RejectReason.AI_BEARISH


# ---- the subprocess classifier ------------------------------------------
class ClaudeCliClassifier:
    def __init__(self, settings: Settings, db: Database | None, clock: Clock | None = None) -> None:
        self.settings, self.db, self.clock = settings, db, clock or RealClock()
        self._sem = asyncio.Semaphore(max(1, settings.classifier_concurrency))
        self.cwd = settings.data_dir / "claude_cwd"
        self._procs: set = set()

    def shutdown(self) -> None:
        for p in list(self._procs):
            if p.returncode is None:
                try:
                    p.kill()
                except ProcessLookupError:
                    pass
        self._procs.clear()

    # daily counter, keyed by ET date, persisted in kv
    def _key(self) -> str:
        return f"classifier_calls:{to_et(self.clock.now()).date().isoformat()}"

    def calls_today(self) -> int:
        if self.db is None:
            return 0
        row = self.db.one("SELECT value FROM kv WHERE key=?", (self._key(),))
        return int(row["value"]) if row else 0

    def budget_left(self) -> int:
        return max(0, self.settings.classifier_max_calls_per_day - self.calls_today())

    def _count_call(self) -> None:
        if self.db is not None:
            self.db.upsert("kv", {"key": self._key(), "value": str(self.calls_today() + 1),
                                  "updated_at": utc_iso(self.clock.now())}, ["key"])

    def _record(self, event: NewsEvent, symbol: str, c: Classification, raw: str,
                duration_ms: int | None, cost: float | None) -> None:
        if self.db is None:
            return
        p = self.settings.params
        self.db.insert("ai_classifications", {
            "article_id": event.article_id, "symbol": symbol, "model": c.model,
            "effort": p.classifier_effort, "direction": str(c.direction), "confidence": c.confidence,
            "material": int(c.material), "catalyst": c.catalyst, "reason": c.reason,
            "raw_json": raw, "duration_ms": duration_ms, "cost_usd_est": cost, "error": c.error,
            "created_at": utc_iso(self.clock.now()), "strategy_version": self.settings.strategy_version})

    async def classify(self, event: NewsEvent, symbol: str) -> Classification:
        model = self.settings.params.classifier_model
        binary = resolve_bin(self.settings)
        if not binary:
            c = _err(model, "claude CLI not found")
            self._record(event, symbol, c, "", None, None)
            return c
        bad = refused_shim(binary)
        if bad:
            c = _err(model, bad)
            self._record(event, symbol, c, "", None, None)
            return c
        argv = build_argv(self.settings)
        prompt = build_user_prompt(symbol, event, self.settings.params.classifier_content_chars).encode("utf-8")
        self.cwd.mkdir(parents=True, exist_ok=True)
        self._count_call()
        raw, res = "", None
        async with self._sem:
            proc = None
            try:
                proc = await asyncio.create_subprocess_exec(
                    *argv, cwd=self.cwd, env=scrubbed_env(os.environ), stdin=asyncio.subprocess.PIPE,
                    stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
                self._procs.add(proc)
                try:
                    out, errb = await asyncio.wait_for(proc.communicate(prompt), self.settings.classifier_timeout_s)
                except asyncio.CancelledError:
                    if proc.returncode is None:
                        proc.kill()
                    raise
                raw = out.decode("utf-8", "replace")
                if proc.returncode != 0:
                    res = CliResult(_err(model, f"claude exit {proc.returncode}: "
                                                f"{errb.decode('utf-8', 'replace')[:200].strip()}"))
            except asyncio.TimeoutError:
                if proc and proc.returncode is None:
                    proc.kill()
                    await proc.wait()
                res = CliResult(_err(model, f"timeout after {self.settings.classifier_timeout_s}s"))
            except OSError as e:
                res = CliResult(_err(model, f"cannot run claude: {e}"))
            finally:
                self._procs.discard(proc)
        res = res or parse_cli_output_ex(raw, model)
        self._record(event, symbol, res.classification, raw, res.duration_ms, res.cost_usd)
        return res.classification


def main(argv: list[str] | None = None) -> None:
    """Manual smoke: makes ONE real CLI call. `python -m newswave.news.classifier --symbol NVDA --headline ...`"""
    ap = argparse.ArgumentParser(description=main.__doc__)
    ap.add_argument("--symbol", required=True)
    ap.add_argument("--headline", required=True)
    ap.add_argument("--summary", default="")
    ap.add_argument("--content", default="")
    a = ap.parse_args(argv)
    settings = Settings(data_dir=Path(os.environ.get("DATA_DIR", "./data")),
                        claude_bin=os.environ.get("CLAUDE_BIN", ""))
    ev = NewsEvent("smoke", "", "", "", a.headline, a.summary, a.content, (a.symbol,), "smoke", "")
    c = asyncio.run(ClaudeCliClassifier(settings, None).classify(ev, a.symbol))
    print(json.dumps(asdict(c), indent=2))


if __name__ == "__main__":
    main()
