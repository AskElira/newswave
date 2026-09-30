# NewsWave

A news-momentum paper-trading bot where Claude only classifies the news and deterministic Python makes every trading decision.

Story: https://jellycharts.com/newswave

## How it works

```
NEWS -> VOLUME -> MOMENTUM -> FIRST 9 EMA PULLBACK -> BREAKOUT -> ENTRY
```

1. **News.** Alpaca's news stream delivers headlines. Cheap deterministic filters run first (dedupe, catalyst cooldown, freshness).
2. **Claude's one job.** The headline is passed to the local `claude` CLI, which returns a structured classification of the news (is it a material catalyst, and in which direction). That is all it does. It never sizes, enters, exits or sees an order.
3. **Volume, momentum, pullback, breakout.** Deterministic code watches the stock's 1-minute bars: volume surge, momentum, the first pullback to the 9 EMA, then a breakout. A state machine tracks each setup.
4. **Entry.** Risk sizing, stops and exits are plain Python in `newswave/risk.py`, `newswave/exits.py` and `newswave/execution/`.

**Shadow signals.** Setups that fail a filter are still tracked as shadow signals, so you can measure what each filter costs or saves.

**The funnel log.** Every stage a symbol passes or fails is written to a `timeline` table with a reason, so you can see exactly why something did or did not trade.

## Safety guarantees

- **Paper-only lock.** Boot refuses unless `TRADING_MODE=PAPER`, `ALPACA_PAPER=true` and any Alpaca URL is the paper host. The only `TradingClient` is built with a literal `paper=True`, and a test greps the package for violations.
- **Arm gate.** `EXECUTION_MODE=PAPER` orders are refused unless `python -m newswave arm` (which runs the full test suite) wrote an `.armed` file matching the current code and parameters.
- **Kill switch.** Once tripped, no flag re-enables it; a restart the same session stays disabled.
- **Classifier isolation.** The classifier never sees brokerage keys or order code (its environment is scrubbed and a test walks its import graph).
- **Input on stdin.** News text goes to `claude -p` on stdin, never on the command line.

## Requirements

- Python 3.12 and [uv](https://docs.astral.sh/uv/)
- A free [Alpaca](https://alpaca.markets) paper account (paper keys only)
- [Claude Code](https://claude.com/claude-code) installed and logged in. The classifier runs on your Claude subscription through `claude -p`; there is no API key.

## Quickstart (macOS / Linux)

```
git clone https://github.com/AskElira/newswave.git
cd newswave
uv venv --python 3.12 .venv
uv pip install --python .venv/bin/python -e '.[dev]'
cp .env.example .env        # fill in ALPACA_API_KEY and ALPACA_SECRET_KEY (paper)
.venv/bin/python -m newswave check
.venv/bin/python -m newswave run
```

Windows: see [`deploy/windows/SETUP.md`](deploy/windows/SETUP.md). A launchd template is in `deploy/com.newswave.daemon.plist`; it is never loaded for you.

## Commands

Run as `.venv/bin/python -m newswave <command>`.

| command | what it does |
| --- | --- |
| `check` | validate settings and the paper guard; never prints secrets |
| `run` | the daemon in OBSERVE mode |
| `arm` | run the full test suite; on green write `.armed` |
| `run --execute` | send paper orders to Alpaca (needs a valid `arm`) |
| `dashboard [--port N]` | local dashboard on 127.0.0.1 (default 8765) |
| `report --month YYYY-MM` | write the monthly report (md + html) under `DATA_DIR/reports/` |
| `replay --fixture tests/fixtures/story_nvda.json` | stream a recorded story through the real engine with a simulated broker |

## Modes

- **OBSERVE** (default): real news and market streams, real classification, the real engine, but orders go to an in-memory simulated broker. Nothing reaches Alpaca.
- **PAPER**: orders go to your Alpaca paper account. Refused unless `arm` passed for the current code and parameters. Re-run `arm` after any change.

Typical path: `pytest`, then `run` for several sessions while watching `dashboard`, then `arm`, then `run --execute`.

## Configuration

Secrets and modes come from `.env` (see `.env.example`). Every strategy parameter lives in `StrategyParams` in `newswave/config.py`. Changing one requires a new `STRATEGY_VERSION`; the daemon refuses to boot if the parameters changed under an existing version, so results from different versions stay separate.

## Testing

```
.venv/bin/pytest -q
```

Tests block non-localhost sockets, scrub credential environment variables and never call the real `claude` CLI.

## Known limits

- The pattern day trader rule applies to margin accounts under $25,000.
- Position size is capped at 35% of equity.
- News must be under 120 seconds old, so pre-market news is often dropped.
- Not proven on live money. It is a paper-trading experiment.

## Built with Claude Code

Built in one day by Claude Sonnet 5.5 agents directed from a single spec. The spec is kept in [`docs/SPEC.md`](docs/SPEC.md) and the resulting design decisions in [`docs/CONTRACT.md`](docs/CONTRACT.md).

Want to backtest your own ideas on futures and stocks, and code strategies with your Claude subscription? Try Jelly Charts: https://jellycharts.com

## Disclaimer

This is educational software for paper trading only. It is not financial advice. It comes with no warranty (see [LICENSE](LICENSE)). Trading involves risk of loss; do not point it at real money.
