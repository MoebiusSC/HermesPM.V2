# HermesPM.V2 — parallel paper experiment (3.0.0)

Independent successor to MoebiusSC/HermesPM at e1a4bc81b7cfc0540217e94cbb624465ade8ed04. Public market data only; no live trading routes or private trading keys. Never mount the original database in this project.

## Changes
- Target 5-second polling, concurrent collection (8 workers), no overlapping cycles. Every wallet still passes the conversion/split/merge guard before its trades are processed. Sell orders are executed before buys within each portfolio.
- One quote request per asset per cycle shared across the independent portfolios; metadata cache retained. HTTP timeout 4 seconds, host cooldown for 429/5xx, bounded 80 requests / 10 seconds across research and polling; failed reads retain cursors. Cycle errors increase polling interval up to 60 seconds. Five seconds is a target, not an execution guarantee.
- Dashboard reports detection P50/P95, execution-after-detection P95, cycle P95, overruns and throttling. Signal/fill samples are bounded to the latest 1,000; transport counters and cycle samples reset on process restart.
- Candidate review reads up to 300 closed positions and current open-position P&L, uses net positive weeks rather than weeks with any winner, and labels partial histories. Ten candidates per hourly round, two research workers. This is not a full equity/return reconstruction.
- Filtered/adaptive wallet cost budgets capped at 10% initial capital (100 USDC with the default 1,000); existing market/event/total caps retained. Reference retains original sizing. Source copying remains 1% of shares, not normalized source portfolio weights.
- Fully exited events, including partial exit history, supply adaptive evidence. At least 14 days and 20 closed events required; uncertain evidence retains the previous weight. Positive allocation increase requires positive approximate lower confidence bound of event returns; this heuristic is not proof of edge.
- New optimized buys pause when existing positions have incomplete valuation. Repetitive signals after exhausted exposure remain recorded without creating redundant orders.
- Health endpoint returns 503 if the polling cycle stops completing. Fresh-data errors remain separately visible; health is not a profitability or upstream availability guarantee.
- Independent run configuration and comparison cohort. Optional SEED_WALLETS_JSON seeds original public wallet addresses once; no trades, balances, inventory or historical performance are imported. New baseline snapshots prevent retroactive copying.
- Dates labelled as published dates, not guaranteed resolution timestamps; confirmed payouts remain separately identified.

## Run
Python 3.12, standard library only.

```
export HERMES_PM_KEY='set-a-private-dashboard-password'
export DB_PATH='/data/hermes_pm_v2.sqlite3'
export POLL_SECONDS=5
python app.py
```

Railway: deploy Dockerfile, one replica, persistent volume at `/data`, sleeping off, healthcheck `/health`. `REQUIRE_PERSISTENT_VOLUME=1` refuses startup without an actual mount. `START_CASH=1000` initializes each of three independent virtual portfolios. Do not add their balances together as one strategy.

`SEED_WALLETS_JSON`: optional JSON array of public `{address,label}` records; initial cohort matches the original to aid comparison. Discovery continues; existing members are not automatically rotated in this release. Compare returns over common calendar windows; separate deployments have different initial inventory and cannot isolate the effect of speed alone.

## Validation
```
python -m unittest discover -s tests -v
```
35 tests cover paper conservation, deduplication, sell reservation, partial exits, fees, settlement, neg-risk guards, auth, backups, candidate net weeks/open losses, adaptive evidence, health, cooldown and telemetry.

## Research backlog and limitations
WebSocket books, target-position normalization, fragment aggregation, source-independent correlated exposure limits, candidate replacement experiments and off-volume backup delivery are not implemented in 3.0.0. Do not interpret this release as completion of every proposed research idea. No maker queue, market impact, atomic multileg or live execution simulation. Standard neg-risk directional markets require canonical event identification; conversions/augmented/multileg strategies remain blocked.

Daily SQLite backups retain the last three on the same volume, with authenticated download at `/api/backup`. These do not protect against volume loss. Never commit secrets or database files. Retained signal/fill history should be monitored against disk capacity.

A shorter poll interval reduces only polling wait; provider indexing delay and changing prices remain. Ten wallets at 5 seconds require a baseline of ~20 trades requests per 10 seconds, plus activity, pagination, quotes and research. Rate limits and CPU/latency metrics must be monitored. There is no promised return.
