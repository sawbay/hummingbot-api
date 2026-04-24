# XEMM Multiple Levels Controller

The `XEMMMultipleLevelsController` implements a Cross-Exchange Market Making (XEMM) strategy with multiple profitability levels.

## Overview
It makes a market on one exchange (maker) and hedges on another exchange (taker) whenever a fill occurs, ensuring a specific target profitability.

## Logic
- **Dual Connector:** Requires a `maker_connector` (usually a lower volume/higher spread exchange) and a `taker_connector` (usually a higher volume/lower spread exchange).
- **Profitability Levels:** Users define multiple levels, each with a `target_profitability` and `amount`.
- **Hedging:** Uses `XEMMExecutor` to monitor the maker order and immediately execute a taker order when a fill is detected.
- **Imbalance Management:** Tracks the number of buy vs. sell fills and restricts further maker orders if the imbalance exceeds `max_executors_imbalance`.
- **Dynamic Pricing:** Maker prices are calculated based on taker prices plus/minus the target profitability.

## Best For
Cross-exchange arbitrage and market making where you want to provide liquidity on one exchange while guaranteed a profit by hedging on another.
