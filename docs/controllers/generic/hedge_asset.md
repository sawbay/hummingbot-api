# Hedge Asset Controller

The `HedgeAssetController` maintains a hedge on a perpetual exchange for a spot asset held on another exchange.

## Overview
It monitors the spot balance of a specific asset and adjusts a short position on a perpetual exchange to maintain a target hedge ratio.

## Logic
- **Monitoring:** Tracks the balance of `asset_to_hedge` on a spot connector.
- **Calculation:**
  - `Target Hedge Size = Spot Balance * Hedge Ratio`.
  - `Gap = Target Hedge Size - Current Perpetual Position`.
- **Execution:**
  - Adjusts the hedge if the gap exceeds `min_notional_size`.
  - Uses `OrderExecutor` with market orders for immediate rebalancing.
  - Implements a `cooldown_time` to prevent frequent small adjustments.
- **Management:** Automatically sets leverage and position mode on the perpetual connector.

## Best For
Minimizing market exposure for spot holdings (e.g., yield farming or long-term storage) by hedging price risk.
