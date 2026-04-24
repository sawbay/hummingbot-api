# PMM V1 Controller

The `PMMV1Controller` replicates the core logic of the legacy Hummingbot `pure_market_making` strategy.

## Overview
It provides a familiar interface and behavior for users transitioning from the legacy Hummingbot scripts to the new Strategy V2 architecture.

## Logic
- **Multi-Level Support:** Defines multiple buy and sell levels using list-based spread and amount configurations.
- **Legacy Inventory Skew:** Implements the exact same inventory skew calculation as the legacy strategy.
- **Order Refresh:** Uses standard timing-based refresh with a configurable tolerance percentage.
- **Price Bands:** Supports static `price_ceiling` and `price_floor` to restrict trading within safe price ranges.
- **Filled Order Delay:** Includes a delay after an order is filled before the next order for that level is placed.

## Best For
Users who want the exact behavior of the classic Hummingbot Pure Market Making strategy within the Strategy V2 framework.
