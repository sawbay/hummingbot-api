# Grid Strike Controller

The `GridStrikeController` implements a grid trading strategy within a specific price range. It places multiple buy or sell orders (depending on the side configured) at different price levels.

## Overview
It uses the `GridExecutor` to manage a set of grid levels between a `start_price` and an `end_price`.

## Logic
- **Boundaries:** User defines a price range (`start_price` to `end_price`).
- **Direction:** Can be configured to be either `BUY` or `SELL` dominant.
- **Execution:**
  - Creates a `GridExecutor` when the mid-price is within the specified bounds.
  - Manages multiple open orders based on `max_open_orders` and `order_frequency`.
  - Supports `TripleBarrierConfig` for each level (take profit, stop loss, time limit).
- **Imbalance Management:** Tracks filled vs. canceled orders and maintains the grid state.

## Best For
Trading in range-bound markets where price is expected to oscillate within a known interval.
