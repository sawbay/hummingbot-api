# Statistical Arbitrage Controller

The `StatArbController` implements a statistical arbitrage strategy between two cointegrated assets.

## Overview
It monitors the price relationship (spread) between a "dominant" pair and a "hedge" pair and trades when the spread deviates significantly from its historical mean.

## Logic
- **Cointegration Analysis:** Performs linear regression over a `lookback_period` to calculate alpha, beta, and the spread.
- **Z-Score Signal:** Generates trading signals when the Z-score of the spread exceeds the `entry_threshold`.
  - **Positive Z-score:** Long Dominant / Short Hedge.
  - **Negative Z-score:** Short Dominant / Long Hedge.
- **Execution:**
  - Uses `PositionExecutor` to open pairs of trades simultaneously.
  - Implements a quoter that places limit orders at a `quoter_spread` from the market.
- **Risk Management:**
  - Global take-profit and stop-loss for the entire pair.
  - Imbalance scaling to ensure the hedge ratio is maintained.
  - Cooldown and refresh timers for the quoter.

## Best For
Trading pairs that are historically correlated or cointegrated (e.g., SOL and a SOL-ecosystem meme coin, or BTC and ETH).
