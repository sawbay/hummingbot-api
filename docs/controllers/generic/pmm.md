# PMM Controller

The `PMMController` is the standard Pure Market Making implementation in Strategy V2.

## Overview
It provides a robust foundation for market making with multiple levels, inventory skew, and basic risk management.

## Logic
- **Level Configuration:** Users provide comma-separated lists for `buy_spreads`, `sell_spreads`, and optionally `buy_amounts_pct`/`sell_amounts_pct`.
- **Inventory Skewing:** Dynamically adjusts order sizes based on `target_base_pct`. It will decrease buy sizes and increase sell sizes if the base asset inventory is above target.
- **Refresh Logic:** Periodically refreshes orders based on `executor_refresh_time`.
- **Global Risk:** Monitors unrealized PnL and can execute a global stop-loss or take-profit by closing the entire position.
- **Inventory Bounds:** Stops placing buy/sell orders if inventory falls outside the `min_base_pct` or `max_base_pct` range.

## Best For
General market making on any pair where the goal is to capture the spread while keeping inventory relatively balanced.
