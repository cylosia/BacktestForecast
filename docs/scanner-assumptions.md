# Scanner and Forecast Assumptions

## Scanner modes
- Basic scanner is intended for Pro and Premium tiers.
- Advanced scanner is intended for Premium only.
- Basic mode is narrower by design: smaller symbol/strategy/rule-set limits and a reduced strategy list.

## Ranking
- Ranking is deterministic.
- Final score uses three inputs:
  - current backtest performance
  - recency-weighted historical performance from prior comparable scanner outcomes
  - a small forecast-alignment adjustment based on historical analogs
- No opaque ML model is used in MVP ranking.

## Historical aggregation
- Comparable history is keyed by symbol + strategy type + rule-set hash.
- Recent outcomes are weighted more heavily than older ones using exponential decay.
- Historical weighting is confidence-scaled so a tiny sample does not dominate ranking.

## Forecasting
- Forecasting is a bounded expected-range estimate built from historical analog windows on daily bars.
- The forecast describes historical analog outcomes, not certainty or advice.
- Forecast alignment is deliberately a small ranking component compared with realized backtest performance.

## Persistence
- Scanner jobs are immutable snapshots of the submitted request.
- Recommendations store the input snapshot, summary metrics, warnings, forecast payload, ranking breakdown, trades, and equity curve.
- Daily refresh creates a new child job rather than mutating the original manual job.
