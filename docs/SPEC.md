# NewsWave — Original specification (the prompt)

This is the author's original prompt, kept verbatim as the record of what was asked for.

> Source of truth for WHAT to build. `docs/CONTRACT.md` is the source of truth for HOW
> (layout, names, interfaces, and decisions the spec leaves open). If they conflict, CONTRACT wins.
>
> **AMENDMENT 2026-09-30 (author): "use auth cli sonnet 5.5 not api".** The classifier calls the
> local `claude` CLI (Claude Code, headless `-p`, the user's own Claude login) with model
> `claude-sonnet-5-5` at effort `medium`. It does NOT use the Anthropic API or `ANTHROPIC_API_KEY`.
> Section 1 "AI" and section 2 "ANTHROPIC_API_KEY" below are superseded by CONTRACT §6.

PROJECT: NewsWave

GOAL
Build an autonomous PAPER-TRADING system that starts with $6,000 and runs
for six months.

The strategy trades short-term momentum caused by material company news.

Core philosophy:

NEWS -> VOLUME -> MOMENTUM -> FIRST 9 EMA PULLBACK -> BREAKOUT -> ENTRY

Do not attempt to beat Bloomberg to the initial headline.

We are deliberately entering AFTER the news has caused an observable
market reaction.

The AI does NOT decide entries, stops, sizing, or exits.

The AI has one job:
CLASSIFY THE NEWS.

All trading logic must be deterministic Python.

## 1. TECHNOLOGY

Python 3.12+

Alpaca:
- Paper Trading API
- Real-Time News WebSocket
- IEX real-time stock WebSocket
- Historical IEX data where available

AI:
- Claude Sonnet 5.5
- medium reasoning/effort
- one classification call per unique news event
- no web searches
- no tool calls
- no extended research

Storage:
- SQLite initially
- design database layer so PostgreSQL can replace it

Runtime:
- long-running daemon/service
- reconnect automatically
- survive crashes/restarts
- persist all positions and state

No OpenBB dependency is required for v1.

## 2. SECRETS / AUTH

Never hard-code credentials.

Use environment variables:

ALPACA_API_KEY=
ALPACA_SECRET_KEY=
ANTHROPIC_API_KEY=   (superseded: classifier uses the claude CLI login, see amendment)

ALPACA_PAPER=true

If Claude Code itself uses another officially supported auth mechanism,
leave that separate from runtime trading authentication.

Do NOT scrape, extract, print, commit, or log Claude/Anthropic auth tokens.

Provide .env.example and .gitignore and prevent .env from entering git.

## 3. STARTING ACCOUNT

starting_equity = $6,000

PAPER TRADING ONLY.

There must be absolutely no code path capable of switching to live
trading accidentally.

Require: TRADING_MODE=PAPER

Application should refuse to boot if a live Alpaca endpoint is supplied.

## 4. NEWS INGESTION

Connect continuously to: wss://stream.data.alpaca.markets/v1beta1/news

Subscribe to: news: ["*"]

For every story save: article_id, received_at, created_at, updated_at, headline,
summary, content, symbols, source, url

Deduplicate stories using Alpaca article ID.

Also implement ticker-level duplicate protection because Benzinga may
publish several updates about the same event.

If essentially the same catalyst appears repeatedly, do not create a new
trade setup every time.

Default duplicate/catalyst cooldown: 60 minutes.

## 5. CLAUDE'S ONLY JOB

Claude receives: ticker, headline, summary, optionally truncated article content

Claude should NOT see charts.
Claude should NOT determine position size.
Claude should NOT determine entries.
Claude should NOT research the company.
Claude should NOT produce essays.

We want an extremely blunt assessment of the IMMEDIATE market impact of
the new information.

SYSTEM PROMPT (use verbatim):

You classify breaking stock-market news for a short-term momentum system.

Determine the likely immediate directional implication of THIS NEWS,
not the long-term quality of the company.

Return BULLISH, NEUTRAL, or BEARISH.

Be conservative.

If the news is ambiguous, routine, promotional, already expected,
non-material, or you cannot clearly determine the direction:
NEUTRAL.

Do not invent information.
Do not research anything.
Do not predict price targets.
Do not explain extensively.

Examples of potentially bullish catalysts:
earnings beat
guidance raise
major contract
FDA approval
material acquisition premium
major strategic transaction
unexpectedly strong operating results

Examples of potentially bearish catalysts:
earnings miss
guidance cut
dilution/offering
regulatory failure
material lawsuit/adverse ruling
unexpected executive departure
major contract loss

Routine corporate announcements should normally be NEUTRAL.

Judge the likely immediate 0-60 minute reaction.

OUTPUT JSON ONLY.

OUTPUT SCHEMA:

{
  "direction": "BULLISH | NEUTRAL | BEARISH",
  "confidence": 0.00-1.00,
  "material": true | false,
  "catalyst": "short category",
  "reason": "maximum 12 words"
}

## 6. AI GATING

LONG candidate: direction == BULLISH AND material == true AND confidence >= 0.75

Anything else: DO NOTHING. NEUTRAL: DO NOTHING. BEARISH: DO NOTHING initially.

Implement optional: ALLOW_SHORTS=false

If ALLOW_SHORTS=true: BEARISH + material + confidence >= 0.75 can create a SHORT
candidate only if Alpaca reports that the asset is tradable and shortable and its
current borrow status permits the short. Otherwise: DO NOTHING.

## 7. SYMBOL MANAGEMENT

The free Alpaca feed supports only 30 simultaneous stock subscriptions.

Do NOT permanently select 30 stocks. Use dynamic subscriptions.

Reserve: SPY, QQQ. Remaining 28 slots are event-driven.

When qualifying news arrives:
1. subscribe ticker
2. begin collecting live IEX trades/quotes/bars
3. keep ticker armed while setup is active
4. unsubscribe after setup expires / trade closes
5. reuse slot for future news

Implement an LRU/event-priority subscription manager.

If capacity is full, prioritize: open positions, armed trade setups, newest
high-confidence catalysts.

## 8. UNIVERSE FILTER

Before considering entry: US equity only, tradable == true

Initial defaults: price >= $3, price <= $500

Avoid: OTC, obviously illiquid stocks, halted securities

Require reasonable liquidity.

Store the exact reason whenever a ticker is rejected.

## 9. NEWS FRESHNESS

Do not chase old stories. Measure: received_at - created_at. Record news latency.

Default: NEWS_MAX_AGE_SECONDS = 120

If already older than threshold: NO TRADE. Still record it for analysis.

## 10. NEWS DOES NOT CAUSE AN INSTANT BUY

This is extremely important. BULLISH NEWS != BUY. Bullish news only ARMS the ticker.
The market must confirm the news.

## 11. VOLUME CONFIRMATION

Use IEX data consistently.

Because IEX represents only part of market volume, do not compare
IEX volume to SIP/full-market volume. Compare IEX against historical IEX baselines.

Calculate relative volume. Initial parameter: RVOL_MIN = 2.0

The post-news 5-minute volume must be >= 2x the normal IEX volume for
that symbol/time period.

Also require meaningful price expansion. Initial: MIN_IMPULSE_PCT = 1.5%

A bullish news candidate must show price appreciation >= 1.5% AND RVOL >= 2.0
before looking for a pullback. Make these parameters configurable.

## 12. CHART

Primary strategy timeframe: 5 MINUTE

Indicators: EMA 9, ATR 14

Optional logging: EMA 20, VWAP — but EMA20/VWAP must NOT gate trades in v1.

Do not contaminate the initial experiment with too many indicators.

## 13. SETUP

For LONG:
A. bullish qualifying news arrives
B. stock produces upward impulse
C. volume confirms
D. wait for FIRST meaningful pullback toward 9 EMA
E. pullback must not completely destroy the impulse
F. define: pullback_high, pullback_low
G. ENTRY: buy when price breaks above pullback_high after the 9 EMA pullback.

We want continuation. We are NOT buying merely because price touched the EMA.

        NEWS
          ↓
       ████
      ██████
     ███████      initial impulse
        ███
         ██
          ▒
          ▒    <- 9 EMA pullback
          ▒
          █
         ███   <- breaks pullback high
        ████
          ↑
        ENTRY

## 14. PULLBACK RULES

pullback begins after confirmed impulse

pullback length: 1-4 completed 5-minute bars

Price must approach/touch the EMA9. Allow tolerance: EMA_TOUCH_TOLERANCE_ATR = 0.20

Reject if price collapses significantly through EMA9.

Reject if:
- volume disappears completely
- price loses the entire news impulse
- setup takes too long
- new contradictory news arrives

SETUP_EXPIRATION_MINUTES = 45

## 15. ENTRY

Entry trigger LONG: current price > pullback_high

Optionally require small breakout buffer: BREAKOUT_BUFFER_ATR = 0.05

entry_trigger = pullback_high + ATR * 0.05

Use an appropriate Alpaca order.

Log: signal timestamp, order timestamp, ack timestamp, fill timestamp
(important: we want to measure latency).

## 16. INITIAL STOP

Use pullback structure + ATR. Long protective stop should be based around pullback_low.

technical_stop = pullback_low

Do not allow absurdly tiny stops. MIN_STOP_ATR = 0.75. Minimum stop distance: ATR * 0.75

Final stop distance should respect both market structure and minimum ATR distance.

Do not widen stop after entry.

## 17. POSITION SIZE

Position sizing is deterministic. Do NOT ask Claude how many shares to buy.

RISK_PER_TRADE_PCT = 1.0%  (for $6,000: initial risk ~= $60/trade)

shares = risk_dollars / abs(entry - stop)

Round safely according to Alpaca requirements.

MAX_POSITION_PCT = 35% (configurable). Never exceed available buying power.

## 18. DAILY RISK

Absolute hard kill switch: MAX_DAILY_LOSS_PCT = 10% (~$600 on $6,000).

IMPORTANT: 10% is a catastrophe ceiling, NOT a risk target. Normal individual trade
risk remains approximately 1%.

Also: MAX_CONCURRENT_POSITIONS = 3, MAX_CONCURRENT_RISK_PCT = 3%

If daily realized + unrealized loss reaches 10%:
cancel entries, cancel outstanding opening orders, close open positions,
disable trading until next session.

Nothing can override this automatically.

## 19. PROFIT MANAGEMENT

We are trying to RIDE the news wave. Do not use a tiny fixed profit target.

At +1R: take 50% off.

For remaining 50%: activate ATR trailing exit.

trail = highest_price_since_entry - (ATR * ATR_TRAIL_MULTIPLIER); default ATR_TRAIL_MULTIPLIER = 2.0

Exit remainder when a completed 5-minute candle CLOSES below the ATR trailing level.
Do not exit from a one-tick dip through trail. Make multiplier configurable.

## 20. FAILED MOMENTUM EXIT

Immediately exit if: hard stop reached; material opposite-direction news appears;
symbol becomes non-tradable; system determines position state is corrupted.

Optional time stop: if trade cannot produce +0.5R within 60 minutes, close it. Configurable.

## 21. END OF DAY

Version 1 is intraday only. No overnight positions.

Do not initiate new positions after 15:30 ET. Close all remaining positions before market close.

Record exit reason: STOP, TRAIL, PARTIAL_PROFIT, TIME_STOP, EOD, RISK_KILL, OPPOSITE_NEWS

## 22. SHORTS

Build short architecture but default it OFF. ALLOW_SHORTS=false

When enabled: BEARISH catalyst + volume confirmation + downward impulse + first rally
back toward EMA9 + break below pullback low = SHORT candidate.

Only submit if Alpaca currently reports stock is shortable and acceptable borrow
availability. Mirror long strategy. Do NOT assume every stock can be shorted.

## 23. PAPER ACCOUNT

Run the strategy against Alpaca PAPER API only. Initial simulated equity: $6,000

Record account equity after every event/order/fill. Produce equity curve.

## 24. DATABASE

Tables: news_events, ai_classifications, symbols, market_events, setups, orders, fills,
positions, trades, daily_stats, system_events

Every rejected trade needs a reason. Examples: AI_NEUTRAL, AI_LOW_CONFIDENCE, NEWS_TOO_OLD,
NO_VOLUME, NO_MOMENTUM, NO_PULLBACK, SETUP_EXPIRED, ILLIQUID, RISK_LIMIT, DUPLICATE_NEWS,
NO_MARKET_DATA, SHORT_UNAVAILABLE

## 25. EXTREMELY IMPORTANT: LOG THE FUNNEL

We eventually need to know:

10,000 news stories -> 1,800 bullish -> 900 material -> 400 had sufficient volume
-> 250 had momentum -> 130 produced EMA pullback -> 92 entries -> results

Do not only save completed trades. Save EVERY stage of the decision process.
Otherwise we cannot improve the strategy scientifically.

## 26. DASHBOARD

Simple local web dashboard. Display: account equity, daily P&L, total P&L, open positions,
news today (Bullish / Neutral / Bearish), armed symbols, current setup stage (NEWS,
WAITING_FOR_VOLUME, WAITING_FOR_IMPULSE, WAITING_FOR_PULLBACK, WAITING_FOR_BREAKOUT,
IN_POSITION), recent trades, win rate, average win, average loss, profit factor,
expectancy, max drawdown, Sharpe, number of trades.

## 27. LIVE EVENT VIEW

I want to be able to watch the bot think. Example:

14:03:01 NVDA news received
14:03:02 Claude: BULLISH 0.92 - guidance raise
14:03:02 NVDA subscribed
14:08:00 RVOL 3.4
14:08:00 price impulse +3.1%
14:13:00 waiting for 9 EMA
14:18:00 9 EMA pullback confirmed
14:18:23 breakout triggered
14:18:24 BUY 17 NVDA @ ...
14:31:00 +1R -> sold 50%
14:46:00 trail raised
15:02:00 5m close below ATR trail
15:02:01 position closed

That timeline must be persisted.

## 28. SIX-MONTH EXPERIMENT

Duration: 6 calendar months. Do not optimize parameters based on individual losing trades.
The first version needs to remain substantially unchanged long enough to produce
meaningful data. Bug fixes are allowed.

Any strategy parameter modification must: 1. be timestamped 2. create a new strategy
version 3. preserve old results separately. Example: newswave_v1.0, newswave_v1.1

## 29. MONTHLY REPORT

Automatically generate monthly: starting equity, ending equity, return, trades, wins,
losses, win rate, average winner, average loser, profit factor, expectancy, Sharpe,
max drawdown.

Results by: catalyst type, confidence bucket, ticker, market cap if available, time of
day, RVOL, initial move %, ATR, entry latency.

Also answer: Did BULLISH news outperform NEUTRAL news? Did RVOL >=2 outperform lower RVOL?
Did first EMA9 pullbacks work? How much did latency matter? Were biggest profits
concentrated in specific catalyst types?

## 30. SHADOW SIGNALS

Very important. Record setups that WOULD have been traded if a filter were different.
For example: neutral news, bearish news, RVOL 1.5-2.0, second EMA9 pullbacks, short signals.
Do not execute them. Track their hypothetical outcomes separately. This gives us research
data without changing the production strategy.

## 31. RELIABILITY

WebSocket reconnects, heartbeats, exponential backoff, API rate-limit handling, duplicate
prevention, order reconciliation, position reconciliation, crash recovery, structured logging.

On startup: fetch Alpaca positions, fetch open orders, compare against database, repair
state safely. Never assume an order failed merely because a response timed out.

## 32. TESTING

Unit tests for: news classification parser, deduplication, volume calculation, EMA, ATR,
pullback detection, breakout detection, position sizing, daily loss kill switch, trailing
stop, short eligibility, subscription manager.

Integration tests using mocked Alpaca streams.

Replay tests where historical news + bars are streamed through the exact same strategy engine.

## 33. SAFETY RULE

Claude must NEVER have access to Alpaca order submission. Architecture must physically
separate NewsClassifier from ExecutionEngine.

Alpaca News -> NewsClassifier -> structured JSON -> StrategyEngine -> RiskEngine
-> ExecutionEngine -> Alpaca Paper Trading

The LLM cannot bypass RiskEngine.

## 34. BUILD ORDER

1 skeleton + config + database · 2 Alpaca news stream · 3 Claude classifier ·
4 dynamic IEX subscriptions · 5 bar builder + EMA9 + ATR + RVOL · 6 setup state machine ·
7 risk engine · 8 paper execution · 9 dashboard · 10 replay tests + failure tests ·
11 observation-only mode for several market sessions · 12 enable $6,000 paper trading

DO NOT enable paper execution until automated tests pass.
